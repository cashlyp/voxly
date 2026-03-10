const { createVonageEventWebhookHandler } = require("./webhookRoutes");

function createMockResponse() {
  const res = {
    statusCode: 200,
    body: null,
  };
  res.status = jest.fn((code) => {
    res.statusCode = code;
    return res;
  });
  res.send = jest.fn((body) => {
    res.body = body;
    return res;
  });
  return res;
}

function buildCtx(overrides = {}) {
  const callConfigurations = new Map();
  const callDirections = new Map();
  const activeCalls = new Map();
  return {
    requireValidVonageWebhook: jest.fn(() => true),
    getVonageCallPayload: jest.fn((req, payload = {}) => ({
      direction: payload.direction || req?.query?.direction || null,
      uuid: payload.uuid || req?.query?.uuid || null,
    })),
    getVonageDtmfDigits: jest.fn((payload = {}) => String(payload?.dtmf || "")),
    shouldProcessProviderEvent: null,
    shouldProcessProviderEventAsync: jest.fn(async () => true),
    resolveVonageCallSid: jest.fn(async () => "call-123"),
    isOutboundVonageDirection: jest.fn((value = "") =>
      String(value || "").toLowerCase().startsWith("outbound"),
    ),
    buildVonageInboundCallSid: jest.fn((uuid) => `vonage-in-${uuid}`),
    ensureCallSetup: jest.fn(),
    rememberVonageCallMapping: jest.fn(),
    handleExternalDtmfInput: jest.fn(async () => {}),
    recordCallStatus: jest.fn(async () => {}),
    handleCallEnd: jest.fn(async () => {}),
    clearVonageCallMappings: jest.fn(),
    getCallConfigurations: jest.fn(() => callConfigurations),
    getCallDirections: jest.fn(() => callDirections),
    getActiveCalls: jest.fn(() => activeCalls),
    ...overrides,
  };
}

describe("createVonageEventWebhookHandler", () => {
  test("dedupes duplicate Vonage callbacks by uuid/status/timestamp", async () => {
    const seen = new Set();
    const ctx = buildCtx({
      shouldProcessProviderEventAsync: jest.fn(async (_source, payload) => {
        const key = `${payload?.uuid || "na"}:${payload?.status || "na"}:${payload?.timestamp || "na"}`;
        if (seen.has(key)) {
          return false;
        }
        seen.add(key);
        return true;
      }),
    });
    const handler = createVonageEventWebhookHandler(ctx);
    const payload = {
      uuid: "vonage-u1",
      status: "ringing",
      timestamp: "2026-03-10T12:00:00.000Z",
    };

    const req1 = { path: "/ve", body: payload, query: {} };
    const res1 = createMockResponse();
    await handler(req1, res1);

    const req2 = { path: "/ve", body: payload, query: {} };
    const res2 = createMockResponse();
    await handler(req2, res2);

    expect(res1.statusCode).toBe(200);
    expect(res1.body).toBe("OK");
    expect(res2.statusCode).toBe(200);
    expect(res2.body).toBe("OK");
    expect(ctx.recordCallStatus).toHaveBeenCalledTimes(1);
    expect(ctx.recordCallStatus).toHaveBeenCalledWith(
      "call-123",
      "ringing",
      "call_ringing",
      expect.objectContaining({
        raw_status: "ringing",
        event_timestamp: "2026-03-10T12:00:00.000Z",
      }),
    );
  });

  test("passes canonical status data even when callbacks arrive out of order", async () => {
    const ctx = buildCtx();
    const handler = createVonageEventWebhookHandler(ctx);

    const answeredReq = {
      path: "/ve",
      query: {},
      body: {
        uuid: "vonage-u2",
        status: "answered",
        timestamp: "2026-03-10T12:00:03.000Z",
      },
    };
    const ringingReq = {
      path: "/ve",
      query: {},
      body: {
        uuid: "vonage-u2",
        status: "ringing",
        timestamp: "2026-03-10T12:00:01.000Z",
      },
    };

    const answeredRes = createMockResponse();
    await handler(answeredReq, answeredRes);
    const ringingRes = createMockResponse();
    await handler(ringingReq, ringingRes);

    expect(answeredRes.statusCode).toBe(200);
    expect(ringingRes.statusCode).toBe(200);
    expect(ctx.recordCallStatus).toHaveBeenNthCalledWith(
      1,
      "call-123",
      "answered",
      "call_answered",
      expect.objectContaining({
        raw_status: "answered",
        event_timestamp: "2026-03-10T12:00:03.000Z",
      }),
    );
    expect(ctx.recordCallStatus).toHaveBeenNthCalledWith(
      2,
      "call-123",
      "ringing",
      "call_ringing",
      expect.objectContaining({
        raw_status: "ringing",
        event_timestamp: "2026-03-10T12:00:01.000Z",
      }),
    );
  });

  test("returns OK and skips malformed or unknown-status payloads", async () => {
    const ctx = buildCtx();
    const handler = createVonageEventWebhookHandler(ctx);

    const malformedReq = { path: "/ve", body: {}, query: {} };
    const malformedRes = createMockResponse();
    await handler(malformedReq, malformedRes);

    const unknownStatusReq = {
      path: "/ve",
      body: { uuid: "vonage-u3", status: "mystery_state" },
      query: {},
    };
    const unknownStatusRes = createMockResponse();
    await handler(unknownStatusReq, unknownStatusRes);

    expect(malformedRes.statusCode).toBe(200);
    expect(malformedRes.body).toBe("OK");
    expect(unknownStatusRes.statusCode).toBe(200);
    expect(unknownStatusRes.body).toBe("OK");
    expect(ctx.recordCallStatus).toHaveBeenCalledTimes(0);
  });
});
