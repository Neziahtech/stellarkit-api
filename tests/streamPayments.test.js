const http = require("http");
const app = require("../src/index");
const { server: stellarServer } = require("../src/config/stellar");

const VALID_ACCOUNT = "GAAZI4TCR3TY5OJHCTJC2A4QSY6CJWJH5IAJTGKIN2ER7LBNVKOCCWN";
const INVALID_ACCOUNT = "NOT_A_VALID_KEY";

/**
 * Opens an SSE connection and collects raw text chunks until the connection
 * is closed by the caller or the server ends the stream.
 *
 * @param {number} port - Local server port
 * @param {string} path - Request path
 * @param {Function} onChunk - Called with each raw text chunk received
 * @param {Function} onEnd   - Called when the connection closes
 * @returns {{ close: Function }} Object with a close() method to abort early
 */
function openSSE(port, path, onChunk, onEnd) {
  const req = http.request(
    { hostname: "127.0.0.1", port, path, method: "GET" },
    (res) => {
      res.setEncoding("utf8");
      res.on("data", onChunk);
      res.on("end", onEnd);
    }
  );
  req.on("error", onEnd);
  req.end();
  return { close: () => req.destroy(), res: req };
}

/**
 * Parses SSE text into an array of { event, data } objects.
 *
 * @param {string} raw - Raw SSE text
 * @returns {Array<{ event: string, data: Object }>}
 */
function parseSSEEvents(raw) {
  return raw
    .split("\n\n")
    .filter(Boolean)
    .map((block) => {
      const lines = block.split("\n");
      const event = lines.find((l) => l.startsWith("event:"))?.slice(7).trim();
      const dataLine = lines.find((l) => l.startsWith("data:"))?.slice(6).trim();
      let data = null;
      try { data = JSON.parse(dataLine); } catch (_) { /* ignore */ }
      return { event, data };
    })
    .filter((e) => e.event && e.data);
}

describe("GET /stream/payments/:id — SSE endpoint", () => {
  let server;
  let port;

  beforeAll((done) => {
    server = http.createServer(app);
    server.listen(0, "127.0.0.1", () => {
      port = server.address().port;
      done();
    });
  });

  afterAll((done) => {
    server.close(done);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  // ── Headers ────────────────────────────────────────────────────────────────
  it("responds with SSE headers", (done) => {
    const mockClose = jest.fn();
    jest.spyOn(stellarServer, "operations").mockReturnValue({
      forAccount: () => ({
        cursor: () => ({
          stream: jest.fn().mockReturnValue(mockClose),
        }),
      }),
    });

    const req = http.request(
      { hostname: "127.0.0.1", port, path: `/stream/payments/${VALID_ACCOUNT}`, method: "GET" },
      (res) => {
        expect(res.statusCode).toBe(200);
        expect(res.headers["content-type"]).toMatch(/text\/event-stream/);
        expect(res.headers["cache-control"]).toMatch(/no-cache/);
        req.destroy();
        done();
      }
    );
    req.on("error", () => done());
    req.end();
  });

  // ── Connected event ────────────────────────────────────────────────────────
  it("emits a connected event immediately on open", (done) => {
    const mockClose = jest.fn();
    jest.spyOn(stellarServer, "operations").mockReturnValue({
      forAccount: () => ({
        cursor: () => ({
          stream: jest.fn().mockReturnValue(mockClose),
        }),
      }),
    });

    let buffer = "";
    const { close } = openSSE(
      port,
      `/stream/payments/${VALID_ACCOUNT}`,
      (chunk) => {
        buffer += chunk;
        const events = parseSSEEvents(buffer);
        const connected = events.find((e) => e.event === "connected");
        if (connected) {
          expect(connected.data.accountId).toBe(VALID_ACCOUNT);
          expect(connected.data.message).toBeDefined();
          close();
          done();
        }
      },
      () => {}
    );
  });

  // ── Payment event forwarding ───────────────────────────────────────────────
  it("emits a payment event for incoming payment operations", (done) => {
    const mockClose = jest.fn();
    const fakePayment = {
      type: "payment",
      id: "op-001",
      created_at: "2026-05-27T10:00:00Z",
      transaction_hash: "abc123",
      amount: "50.0000000",
      asset_type: "credit_alphanum4",
      asset_code: "USDC",
      asset_issuer: "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
      from: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
      to: VALID_ACCOUNT,
    };

    jest.spyOn(stellarServer, "operations").mockReturnValue({
      forAccount: () => ({
        cursor: () => ({
          stream: jest.fn().mockImplementation(({ onmessage }) => {
            setTimeout(() => onmessage(fakePayment), 20);
            return mockClose;
          }),
        }),
      }),
    });

    let buffer = "";
    const { close } = openSSE(
      port,
      `/stream/payments/${VALID_ACCOUNT}`,
      (chunk) => {
        buffer += chunk;
        const events = parseSSEEvents(buffer);
        const paymentEvent = events.find((e) => e.event === "payment");
        if (paymentEvent) {
          expect(paymentEvent.data).toMatchObject({
            type: "payment",
            id: "op-001",
            createdAt: "2026-05-27T10:00:00Z",
            transactionHash: "abc123",
            amount: "50.0000000",
            assetCode: "USDC",
            assetIssuer: "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
            sender: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
            receiver: VALID_ACCOUNT,
          });
          close();
          done();
        }
      },
      () => {}
    );
  });

  // ── create_account forwarding ──────────────────────────────────────────────
  it("emits a payment event for create_account operations", (done) => {
    const mockClose = jest.fn();
    const fakeCreateAccount = {
      type: "create_account",
      id: "op-002",
      created_at: "2026-05-27T11:00:00Z",
      transaction_hash: "def456",
      starting_balance: "10.0000000",
      funder: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
      account: VALID_ACCOUNT,
    };

    jest.spyOn(stellarServer, "operations").mockReturnValue({
      forAccount: () => ({
        cursor: () => ({
          stream: jest.fn().mockImplementation(({ onmessage }) => {
            setTimeout(() => onmessage(fakeCreateAccount), 20);
            return mockClose;
          }),
        }),
      }),
    });

    let buffer = "";
    const { close } = openSSE(
      port,
      `/stream/payments/${VALID_ACCOUNT}`,
      (chunk) => {
        buffer += chunk;
        const events = parseSSEEvents(buffer);
        const paymentEvent = events.find((e) => e.event === "payment");
        if (paymentEvent) {
          expect(paymentEvent.data).toMatchObject({
            type: "create_account",
            amount: "10.0000000",
            assetCode: "XLM",
            assetIssuer: null,
            sender: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
            receiver: VALID_ACCOUNT,
          });
          close();
          done();
        }
      },
      () => {}
    );
  });

  // ── Non-payment filtering ──────────────────────────────────────────────────
  it("does not emit events for non-payment operation types", (done) => {
    const mockClose = jest.fn();
    const nonPaymentOp = {
      type: "change_trust",
      id: "op-003",
      created_at: "2026-05-27T12:00:00Z",
      transaction_hash: "ghi789",
    };
    const paymentOp = {
      type: "payment",
      id: "op-004",
      created_at: "2026-05-27T12:01:00Z",
      transaction_hash: "jkl012",
      amount: "5.0000000",
      asset_code: "XLM",
      asset_issuer: null,
      from: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
      to: VALID_ACCOUNT,
    };

    jest.spyOn(stellarServer, "operations").mockReturnValue({
      forAccount: () => ({
        cursor: () => ({
          stream: jest.fn().mockImplementation(({ onmessage }) => {
            setTimeout(() => {
              onmessage(nonPaymentOp); // should be dropped
              onmessage(paymentOp);   // should be forwarded
            }, 20);
            return mockClose;
          }),
        }),
      }),
    });

    let buffer = "";
    const { close } = openSSE(
      port,
      `/stream/payments/${VALID_ACCOUNT}`,
      (chunk) => {
        buffer += chunk;
        const events = parseSSEEvents(buffer);
        const paymentEvents = events.filter((e) => e.event === "payment");
        if (paymentEvents.length > 0) {
          // Only the actual payment should have been forwarded
          expect(paymentEvents).toHaveLength(1);
          expect(paymentEvents[0].data.id).toBe("op-004");
          close();
          done();
        }
      },
      () => {}
    );
  });

  // ── Payload shape ──────────────────────────────────────────────────────────
  it("payment event includes amount, assetCode, assetIssuer, sender, receiver", (done) => {
    const mockClose = jest.fn();
    const fakePayment = {
      type: "payment",
      id: "op-005",
      created_at: "2026-05-27T13:00:00Z",
      transaction_hash: "mno345",
      amount: "100.0000000",
      asset_type: "native",
      asset_code: undefined,
      asset_issuer: undefined,
      from: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
      to: VALID_ACCOUNT,
    };

    jest.spyOn(stellarServer, "operations").mockReturnValue({
      forAccount: () => ({
        cursor: () => ({
          stream: jest.fn().mockImplementation(({ onmessage }) => {
            setTimeout(() => onmessage(fakePayment), 20);
            return mockClose;
          }),
        }),
      }),
    });

    let buffer = "";
    const { close } = openSSE(
      port,
      `/stream/payments/${VALID_ACCOUNT}`,
      (chunk) => {
        buffer += chunk;
        const events = parseSSEEvents(buffer);
        const paymentEvent = events.find((e) => e.event === "payment");
        if (paymentEvent) {
          const d = paymentEvent.data;
          expect(d).toHaveProperty("amount");
          expect(d).toHaveProperty("assetCode");
          expect(d).toHaveProperty("assetIssuer");
          expect(d).toHaveProperty("sender");
          expect(d).toHaveProperty("receiver");
          // XLM native should default correctly
          expect(d.assetCode).toBe("XLM");
          expect(d.assetIssuer).toBeNull();
          close();
          done();
        }
      },
      () => {}
    );
  });

  // ── Clean disconnect ───────────────────────────────────────────────────────
  it("calls the Horizon stream close function when client disconnects", (done) => {
    const mockClose = jest.fn();
    jest.spyOn(stellarServer, "operations").mockReturnValue({
      forAccount: () => ({
        cursor: () => ({
          stream: jest.fn().mockReturnValue(mockClose),
        }),
      }),
    });

    const { close } = openSSE(
      port,
      `/stream/payments/${VALID_ACCOUNT}`,
      (chunk) => {
        // Close as soon as we get the connected event
        if (chunk.includes("connected")) {
          close();
        }
      },
      () => {
        // After disconnect, give the server a tick to run cleanup
        setTimeout(() => {
          expect(mockClose).toHaveBeenCalledTimes(1);
          done();
        }, 50);
      }
    );
  });

  // ── Validation ─────────────────────────────────────────────────────────────
  it("returns 400 for an invalid account ID", (done) => {
    const req = http.request(
      { hostname: "127.0.0.1", port, path: `/stream/payments/${INVALID_ACCOUNT}`, method: "GET" },
      (res) => {
        let body = "";
        res.setEncoding("utf8");
        res.on("data", (c) => { body += c; });
        res.on("end", () => {
          expect(res.statusCode).toBe(400);
          const parsed = JSON.parse(body);
          expect(parsed.success).toBe(false);
          expect(parsed.error.type).toBe("ValidationError");
          done();
        });
      }
    );
    req.on("error", done);
    req.end();
  });

  // ── Horizon error forwarding ───────────────────────────────────────────────
  it("emits an error SSE event when the Horizon stream errors", (done) => {
    const mockClose = jest.fn();
    jest.spyOn(stellarServer, "operations").mockReturnValue({
      forAccount: () => ({
        cursor: () => ({
          stream: jest.fn().mockImplementation(({ onerror }) => {
            setTimeout(() => onerror(new Error("Horizon unavailable")), 20);
            return mockClose;
          }),
        }),
      }),
    });

    let buffer = "";
    openSSE(
      port,
      `/stream/payments/${VALID_ACCOUNT}`,
      (chunk) => { buffer += chunk; },
      () => {
        const events = parseSSEEvents(buffer);
        const errorEvent = events.find((e) => e.event === "error");
        expect(errorEvent).toBeDefined();
        expect(errorEvent.data.message).toBeDefined();
        done();
      }
    );
  });
});
