const { registerStatusRoutes } = require("../controllers/statusRoutes");

function createAppStub() {
  const routes = {};
  return {
    routes,
    get(path, ...handlers) {
      routes[`GET ${path}`] = handlers[handlers.length - 1];
    },
  };
}

function createResMock() {
  const res = {
    statusCode: 200,
    body: null,
  };
  res.status = jest.fn((code) => {
    res.statusCode = code;
    return res;
  });
  res.json = jest.fn((body) => {
    res.body = body;
    return res;
  });
  return res;
}

function createCtx(overrides = {}) {
  return {
    requireOutboundAuthorization: (_req, _res, next) => next(),
    isSafeId: () => true,
    normalizeCallRecordForApi: (row) => row,
    buildDigitSummary: () => ({ summary: "", count: 0 }),
    webhookService: {
      healthCheck: jest.fn().mockResolvedValue({ status: "healthy" }),
      getCallStatusStats: jest.fn().mockReturnValue({ total_tracked_calls: 0 }),
    },
    config: {
      apiAuth: {
        hmacSecret: "secret",
      },
      keypadGuard: {
        enabled: true,
      },
      openRouter: {
        alerting: {},
        slo: {},
      },
    },
    verifyHmacSignature: () => ({ ok: false }),
    hasAdminToken: () => false,
    refreshInboundDefaultScript: jest.fn().mockResolvedValue(),
    getInboundHealthContext: () => ({
      inboundDefaultSummary: {},
      inboundEnvSummary: {},
    }),
    supportedProviders: ["twilio"],
    providerHealth: new Map(),
    getProviderReadiness: () => ({ twilio: true }),
    isProviderDegraded: () => false,
    pruneExpiredKeypadProviderOverrides: jest.fn(),
    keypadProviderOverrides: new Map(),
    callConfigurations: new Map(),
    functionEngine: {
      getBusinessAnalysis: () => ({ availableTemplates: [] }),
    },
    callFunctionSystems: new Map(),
    db: null,
    ...overrides,
  };
}

describe("statusRoutes health behavior", () => {
  test("GET /health returns 200 degraded payload when db is corrupt", async () => {
    const corruptError = new Error("SQLITE_CORRUPT: database disk image is malformed");
    corruptError.code = "SQLITE_CORRUPT";
    const db = {
      healthCheck: jest.fn().mockRejectedValue(corruptError),
    };
    const app = createAppStub();
    registerStatusRoutes(app, createCtx({ db }));

    const handler = app.routes["GET /health"];
    const req = {
      path: "/health",
      headers: {},
      query: {},
    };
    const res = createResMock();

    await handler(req, res);

    expect(res.status).not.toHaveBeenCalled();
    expect(res.json).toHaveBeenCalledWith(
      expect.objectContaining({
        status: "degraded",
        code: "database_corrupt",
        public: true,
        readiness: "degraded",
      }),
    );
  });

  test("GET /ready returns 503 when readiness checks hit corrupt db", async () => {
    const corruptError = new Error("SQLITE_CORRUPT: database disk image is malformed");
    corruptError.code = "SQLITE_CORRUPT";
    const db = {
      getCallsWithTranscripts: jest.fn().mockRejectedValue(corruptError),
    };
    const app = createAppStub();
    registerStatusRoutes(
      app,
      createCtx({
        db,
        hasAdminToken: () => true,
      }),
    );

    const handler = app.routes["GET /ready"];
    const req = {
      path: "/ready",
      headers: {},
      query: {},
    };
    const res = createResMock();

    await handler(req, res);

    expect(res.status).toHaveBeenCalledWith(503);
    expect(res.body).toEqual(
      expect.objectContaining({
        status: "degraded",
        code: "database_corrupt",
      }),
    );
  });

  test("GET /ready returns 401 for unauthorized callers", async () => {
    const app = createAppStub();
    registerStatusRoutes(
      app,
      createCtx({
        db: {},
        hasAdminToken: () => false,
        verifyHmacSignature: () => ({ ok: false }),
      }),
    );

    const handler = app.routes["GET /ready"];
    const req = {
      path: "/ready",
      headers: {},
      query: {},
    };
    const res = createResMock();

    await handler(req, res);

    expect(res.status).toHaveBeenCalledWith(401);
    expect(res.body).toEqual(
      expect.objectContaining({
        status: "unauthorized",
      }),
    );
  });
});
