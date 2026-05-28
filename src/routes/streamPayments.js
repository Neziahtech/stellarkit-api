const express = require("express");
const router = express.Router();
const { server } = require("../config/stellar");
const { validateAccountId } = require("../utils/validators");

/** Payment operation types streamed by this endpoint. */
const PAYMENT_TYPES = new Set(["payment", "create_account", "path_payment_strict_send", "path_payment_strict_receive"]);

/**
 * Formats a raw Horizon operation record into a clean payment event payload.
 *
 * @param {Object} op - Raw Horizon operation record
 * @returns {Object} Formatted payment event
 */
function formatPaymentEvent(op) {
  switch (op.type) {
    case "payment":
      return {
        type: op.type,
        id: op.id,
        createdAt: op.created_at,
        transactionHash: op.transaction_hash,
        amount: op.amount,
        assetCode: op.asset_code || "XLM",
        assetIssuer: op.asset_issuer || null,
        sender: op.from,
        receiver: op.to,
      };

    case "create_account":
      return {
        type: op.type,
        id: op.id,
        createdAt: op.created_at,
        transactionHash: op.transaction_hash,
        amount: op.starting_balance,
        assetCode: "XLM",
        assetIssuer: null,
        sender: op.funder,
        receiver: op.account,
      };

    case "path_payment_strict_send":
      return {
        type: op.type,
        id: op.id,
        createdAt: op.created_at,
        transactionHash: op.transaction_hash,
        amount: op.amount,
        assetCode: op.asset_code || "XLM",
        assetIssuer: op.asset_issuer || null,
        sender: op.from,
        receiver: op.to,
      };

    case "path_payment_strict_receive":
      return {
        type: op.type,
        id: op.id,
        createdAt: op.created_at,
        transactionHash: op.transaction_hash,
        amount: op.amount,
        assetCode: op.asset_code || "XLM",
        assetIssuer: op.asset_issuer || null,
        sender: op.from,
        receiver: op.to,
      };

    default:
      return null;
  }
}

/**
 * Writes a Server-Sent Event frame to the response.
 *
 * @param {import("express").Response} res - Express response object
 * @param {string} event - SSE event name
 * @param {Object} data - Payload to JSON-serialize
 */
function sendSSE(res, event, data) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

/**
 * GET /stream/payments/:id
 *
 * Opens a Server-Sent Events stream that emits real-time payment operations
 * for the given Stellar account. Only payment-type operations are forwarded;
 * all other operation types are silently dropped.
 *
 * SSE events emitted:
 *   - "connected"  — sent once on open, confirms the account being watched
 *   - "payment"    — emitted for each incoming payment operation
 *   - "error"      — emitted when the Horizon stream encounters an error
 *
 * Each "payment" event data shape:
 * {
 *   type:            string,   // "payment" | "create_account" | "path_payment_strict_send" | "path_payment_strict_receive"
 *   id:              string,
 *   createdAt:       string,   // ISO 8601
 *   transactionHash: string,
 *   amount:          string,
 *   assetCode:       string,   // "XLM" for native
 *   assetIssuer:     string|null,
 *   sender:          string,   // Stellar public key
 *   receiver:        string,   // Stellar public key
 * }
 *
 * @param {string} id - Stellar account public key (G...)
 *
 * @example
 * GET /stream/payments/GAAZI4TCR3TY5OJHCTJC2A4QSY6CJWJH5IAJTGKIN2ER7LBNVKOCCWN
 */
router.get("/:id", (req, res, next) => {
  const { id } = req.params;

  // Validate account ID before opening the stream
  try {
    validateAccountId(id);
  } catch (err) {
    return next(err);
  }

  // ── SSE headers ────────────────────────────────────────────────────────────
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no"); // disable nginx buffering
  res.flushHeaders();

  // Send an initial "connected" event so the client knows the stream is live
  sendSSE(res, "connected", { accountId: id, message: "Streaming payment operations" });

  let isClosed = false;
  let closeHorizonStream;

  // ── Horizon operations stream ──────────────────────────────────────────────
  try {
    closeHorizonStream = server
      .operations()
      .forAccount(id)
      .cursor("now")       // only receive new operations going forward
      .stream({
        onmessage: (op) => {
          if (isClosed) return;

          // Drop non-payment operation types
          if (!PAYMENT_TYPES.has(op.type)) return;

          const payload = formatPaymentEvent(op);
          if (!payload) return;

          try {
            sendSSE(res, "payment", payload);
          } catch (writeErr) {
            // Client disconnected mid-write; clean up
            cleanup();
          }
        },
        onerror: (err) => {
          if (isClosed) return;
          console.error(`[SSE /stream/payments/${id}] Horizon stream error:`, err);
          try {
            sendSSE(res, "error", { message: "Horizon stream error. Reconnect to resume." });
          } catch (_) {
            // ignore write errors on a broken connection
          }
          cleanup();
        },
      });
  } catch (err) {
    console.error(`[SSE /stream/payments/${id}] Failed to start Horizon stream:`, err);
    try {
      sendSSE(res, "error", { message: "Failed to start payment stream." });
    } catch (_) {
      // ignore
    }
    res.end();
    return;
  }

  // ── Clean disconnect ───────────────────────────────────────────────────────
  function cleanup() {
    if (isClosed) return;
    isClosed = true;
    console.log(`[SSE /stream/payments/${id}] Client disconnected. Closing Horizon stream.`);
    if (typeof closeHorizonStream === "function") {
      try {
        closeHorizonStream();
      } catch (err) {
        console.error(`[SSE /stream/payments/${id}] Error closing Horizon stream:`, err);
      }
    }
    if (!res.writableEnded) {
      res.end();
    }
  }

  // Triggered when the HTTP client closes the connection
  req.on("close", cleanup);
  req.on("aborted", cleanup);
});

module.exports = router;
