"use strict";

const request = require("supertest");
const {
  computeInitDataHash,
  createMiniAppSessionToken,
} = require("./miniappAuth");

function buildInitDataRaw({ botToken, userId, authDate, queryId = "QID_TEST_123" }) {
  const params = new URLSearchParams();
  params.set("auth_date", String(authDate));
  params.set("query_id", String(queryId));
  params.set(
    "user",
    JSON.stringify({
      id: userId,
      username: "miniapp_admin",
      first_name: "Mini",
    }),
  );
  const hash = computeInitDataHash(params.toString(), botToken);
  params.set("hash", hash);
  return params.toString();
}

describe("miniapp route auth flow", () => {
  const originalEnv = { ...process.env };
  const BOT_TOKEN = "123456:TEST_TOKEN";
  const STALE_TELEGRAM_BOT_TOKEN = "123456:STALE_TOKEN";
  const SESSION_SECRET = "test-miniapp-session-secret";
  const ADMIN_USER_ID = "7770001";
  let app;

  beforeAll(() => {
    process.env.NODE_ENV = "test";
    process.env.TELEGRAM_BOT_TOKEN = STALE_TELEGRAM_BOT_TOKEN;
    process.env.BOT_TOKEN = BOT_TOKEN;
    process.env.MINI_APP_SESSION_SECRET = SESSION_SECRET;
    process.env.TELEGRAM_ADMIN_USER_IDS = ADMIN_USER_ID;
    process.env.CORS_ORIGINS = "https://voxly-miniapp.vercel.app";
    process.env.MINI_APP_URL = "https://voxly-miniapp.vercel.app";
    process.env.TWILIO_ACCOUNT_SID = "AC11111111111111111111111111111111";
    process.env.TWILIO_AUTH_TOKEN = "test_twilio_auth_token";
    process.env.FROM_NUMBER = "+15550001111";
    process.env.MINI_APP_ACTION_RATE_PER_USER = "2";
    process.env.MINI_APP_ACTION_RATE_GLOBAL = "100";

    jest.resetModules();
    ({ app } = require("../app"));
  });

  afterAll(() => {
    process.env = originalEnv;
  });

  test("POST /miniapp/session returns 400 when init data is missing", async () => {
    const response = await request(app)
      .post("/miniapp/session")
      .send({});

    expect(response.status).toBe(400);
    expect(response.body?.code).toBe("miniapp_missing_init_data");
  });

  test("POST /miniapp/session returns 403 for non-admin Telegram user", async () => {
    const initDataRaw = buildInitDataRaw({
      botToken: BOT_TOKEN,
      userId: 123456789,
      authDate: Math.floor(Date.now() / 1000),
    });

    const response = await request(app)
      .post("/miniapp/session")
      .set("x-telegram-init-data", initDataRaw)
      .send({ init_data_raw: initDataRaw });

    expect(response.status).toBe(403);
    expect(response.body?.code).toBe("miniapp_admin_required");
  });

  test("POST /miniapp/session returns token for admin Telegram user", async () => {
    const initDataRaw = buildInitDataRaw({
      botToken: BOT_TOKEN,
      userId: Number(ADMIN_USER_ID),
      authDate: Math.floor(Date.now() / 1000),
    });

    const response = await request(app)
      .post("/miniapp/session")
      .set("Authorization", `tma ${initDataRaw}`)
      .send({ init_data_raw: initDataRaw });

    expect(response.status).toBe(200);
    expect(response.body?.success).toBe(true);
    expect(typeof response.body?.token).toBe("string");
    expect(response.body?.session?.telegram_id).toBe(ADMIN_USER_ID);
  });

  test("POST /miniapp/session flags replay_detected for repeated init data payload", async () => {
    const initDataRaw = buildInitDataRaw({
      botToken: BOT_TOKEN,
      userId: Number(ADMIN_USER_ID),
      authDate: Math.floor(Date.now() / 1000),
      queryId: `QID_REPLAY_${Date.now()}`,
    });

    const first = await request(app)
      .post("/miniapp/session")
      .set("Authorization", `tma ${initDataRaw}`)
      .send({ init_data_raw: initDataRaw });
    const second = await request(app)
      .post("/miniapp/session")
      .set("Authorization", `tma ${initDataRaw}`)
      .send({ init_data_raw: initDataRaw });

    expect(first.status).toBe(200);
    expect(first.body?.replay_detected).toBe(false);
    expect(second.status).toBe(200);
    expect(second.body?.replay_detected).toBe(true);
  });

  test("GET /miniapp/bootstrap returns 401 without token", async () => {
    const response = await request(app).get("/miniapp/bootstrap");

    expect(response.status).toBe(401);
    expect(response.body?.code).toBe("miniapp_auth_required");
  });

  test("GET /miniapp/bootstrap returns 403 when dashboard capability is missing", async () => {
    const token = createMiniAppSessionToken(
      {
        jti: "test-jti-1",
        sub: "tg:admin-cap-test",
        telegram_id: "admin-cap-test",
        role: "admin",
        caps: ["provider_manage"],
      },
      SESSION_SECRET,
      { nowSeconds: Math.floor(Date.now() / 1000), ttlSeconds: 600 },
    );

    const response = await request(app)
      .get("/miniapp/bootstrap")
      .set("Authorization", `Bearer ${token}`);

    expect(response.status).toBe(403);
    expect(response.body?.code).toBe("miniapp_capability_denied");
  });

  test("POST /miniapp/logout revokes the active token for follow-up requests", async () => {
    const token = createMiniAppSessionToken(
      {
        jti: "test-jti-logout",
        sub: "tg:admin-logout",
        telegram_id: "admin-logout",
        role: "admin",
        caps: ["dashboard_view"],
      },
      SESSION_SECRET,
      { nowSeconds: Math.floor(Date.now() / 1000), ttlSeconds: 600 },
    );

    const logoutResponse = await request(app)
      .post("/miniapp/logout")
      .set("Authorization", `Bearer ${token}`)
      .send({});

    expect(logoutResponse.status).toBe(200);
    expect(logoutResponse.body?.success).toBe(true);
    expect(logoutResponse.body?.revoked).toBe(true);

    const bootstrapAfterLogout = await request(app)
      .get("/miniapp/bootstrap")
      .set("Authorization", `Bearer ${token}`);

    expect(bootstrapAfterLogout.status).toBe(401);
    expect(bootstrapAfterLogout.body?.code).toBe("miniapp_token_revoked");
  });

  test("POST /miniapp/action validates missing/unsupported actions", async () => {
    const token = createMiniAppSessionToken(
      {
        jti: "test-jti-2",
        sub: "tg:admin-action-validate",
        telegram_id: "admin-action-validate",
        role: "admin",
        caps: ["provider_manage", "dashboard_view"],
      },
      SESSION_SECRET,
      { nowSeconds: Math.floor(Date.now() / 1000), ttlSeconds: 600 },
    );

    const missingAction = await request(app)
      .post("/miniapp/action")
      .set("Authorization", `Bearer ${token}`)
      .send({ payload: {} });

    expect(missingAction.status).toBe(400);
    expect(missingAction.body?.code).toBe("miniapp_action_required");

    const unsupportedAction = await request(app)
      .post("/miniapp/action")
      .set("Authorization", `Bearer ${token}`)
      .send({ action: "unknown.action", payload: {} });

    expect(unsupportedAction.status).toBe(400);
    expect(unsupportedAction.body?.code).toBe("miniapp_action_invalid");
  });

  test("POST /miniapp/action enforces per-user miniapp action rate limits", async () => {
    const token = createMiniAppSessionToken(
      {
        jti: "test-jti-3",
        sub: "tg:admin-action-rate",
        telegram_id: "admin-action-rate",
        role: "admin",
        caps: ["provider_manage", "dashboard_view"],
      },
      SESSION_SECRET,
      { nowSeconds: Math.floor(Date.now() / 1000), ttlSeconds: 600 },
    );

    const first = await request(app)
      .post("/miniapp/action")
      .set("Authorization", `Bearer ${token}`)
      .send({ action: "unknown.action", payload: {} });
    const second = await request(app)
      .post("/miniapp/action")
      .set("Authorization", `Bearer ${token}`)
      .send({ action: "unknown.action", payload: {} });
    const third = await request(app)
      .post("/miniapp/action")
      .set("Authorization", `Bearer ${token}`)
      .send({ action: "unknown.action", payload: {} });

    expect(first.status).toBe(400);
    expect(second.status).toBe(400);
    expect(third.status).toBe(429);
    expect(third.body?.code).toBe("miniapp_action_per_user_rate_limited");
  });
});
