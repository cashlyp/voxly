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
  const ADMIN_API_TOKEN = "test-miniapp-admin-api-token";
  let app;

  beforeAll(() => {
    process.env.NODE_ENV = "test";
    delete process.env.API_SECRET;
    process.env.ADMIN_API_TOKEN = ADMIN_API_TOKEN;
    process.env.TELEGRAM_BOT_TOKEN = STALE_TELEGRAM_BOT_TOKEN;
    process.env.BOT_TOKEN = BOT_TOKEN;
    process.env.MINI_APP_SESSION_SECRET = SESSION_SECRET;
    process.env.TELEGRAM_ADMIN_USER_IDS = ADMIN_USER_ID;
    process.env.CORS_ORIGINS = "https://voxly-miniapp.vercel.app";
    process.env.MINI_APP_URL = "https://voxly-miniapp.vercel.app";
    process.env.TWILIO_ACCOUNT_SID = "AC11111111111111111111111111111111";
    process.env.TWILIO_AUTH_TOKEN = "test_twilio_auth_token";
    process.env.FROM_NUMBER = "+15550001111";
    process.env.MINI_APP_ACTION_RATE_PER_USER = "100";
    process.env.MINI_APP_ACTION_RATE_GLOBAL = "1000";

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

  test("GET /miniapp/bootstrap rejects tokens for unauthorized Telegram ids", async () => {
    const token = createMiniAppSessionToken(
      {
        jti: "test-jti-unauthorized-id",
        sub: "tg:unauthorized-user",
        telegram_id: "unauthorized-user",
        role: "admin",
        caps: ["dashboard_view", "users_manage", "provider_manage"],
      },
      SESSION_SECRET,
      { nowSeconds: Math.floor(Date.now() / 1000), ttlSeconds: 600 },
    );

    const response = await request(app)
      .get("/miniapp/bootstrap")
      .set("Authorization", `Bearer ${token}`);

    expect(response.status).toBe(403);
    expect(response.body?.code).toBe("miniapp_admin_required");
  });

  test("GET /miniapp/bootstrap refreshes role capabilities from server policy", async () => {
    const token = createMiniAppSessionToken(
      {
        jti: "test-jti-cap-refresh",
        sub: `tg:${ADMIN_USER_ID}`,
        telegram_id: ADMIN_USER_ID,
        role: "viewer",
        caps: [],
      },
      SESSION_SECRET,
      { nowSeconds: Math.floor(Date.now() / 1000), ttlSeconds: 600 },
    );

    const response = await request(app)
      .get("/miniapp/bootstrap")
      .set("Authorization", `Bearer ${token}`);

    expect(response.status).toBe(200);
    expect(response.body?.success).toBe(true);
    const caps = Array.isArray(response.body?.session?.caps)
      ? response.body.session.caps
      : [];
    expect(caps).toContain("dashboard_view");
    expect(caps).toContain("users_manage");
  });

  test("POST /miniapp/logout revokes the active token for follow-up requests", async () => {
    const token = createMiniAppSessionToken(
      {
        jti: "test-jti-logout",
        sub: `tg:${ADMIN_USER_ID}`,
        telegram_id: ADMIN_USER_ID,
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
        sub: `tg:${ADMIN_USER_ID}`,
        telegram_id: ADMIN_USER_ID,
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

  test("GET /miniapp/bootstrap returns extended dashboard payload shape", async () => {
    const token = createMiniAppSessionToken(
      {
        jti: "test-jti-bootstrap-shape",
        sub: `tg:${ADMIN_USER_ID}`,
        telegram_id: ADMIN_USER_ID,
        role: "admin",
        caps: ["dashboard_view"],
      },
      SESSION_SECRET,
      { nowSeconds: Math.floor(Date.now() / 1000), ttlSeconds: 600 },
    );

    const response = await request(app)
      .get("/miniapp/bootstrap")
      .set("Authorization", `Bearer ${token}`);

    expect(response.status).toBe(200);
    expect(response.body?.success).toBe(true);
    expect(response.body?.dashboard).toBeTruthy();
    expect(response.body?.dashboard).toHaveProperty("provider");
    expect(response.body?.dashboard).toHaveProperty("call_stats");
    expect(response.body?.dashboard).toHaveProperty("ops");
    expect(response.body?.dashboard).toHaveProperty("call_logs");
    expect(response.body?.dashboard).toHaveProperty("call_scripts");
    expect(response.body?.dashboard).toHaveProperty("voice_runtime");
    expect(response.body?.dashboard).toHaveProperty("users");
    expect(response.body?.dashboard).toHaveProperty("audit");
    expect(response.body?.dashboard).toHaveProperty("incidents");
  });

  test("POST /miniapp/action supports calls.list for dashboard viewers", async () => {
    const token = createMiniAppSessionToken(
      {
        jti: "test-jti-calls-list",
        sub: `tg:${ADMIN_USER_ID}`,
        telegram_id: ADMIN_USER_ID,
        role: "admin",
        caps: ["dashboard_view"],
      },
      SESSION_SECRET,
      { nowSeconds: Math.floor(Date.now() / 1000), ttlSeconds: 600 },
    );

    const response = await request(app)
      .post("/miniapp/action")
      .set("Authorization", `Bearer ${token}`)
      .send({
        action: "calls.list",
        payload: { limit: 5, offset: 0 },
      });

    expect(response.status).toBe(200);
    expect(response.body?.success).toBe(true);
    expect(response.body?.data).toBeTruthy();
    expect(Array.isArray(response.body?.data?.rows)).toBe(true);
  });

  test("POST /miniapp/action routes callscript.update via local bridge (no loopback dependency)", async () => {
    const token = createMiniAppSessionToken(
      {
        jti: "test-jti-callscript-update",
        sub: `tg:${ADMIN_USER_ID}`,
        telegram_id: ADMIN_USER_ID,
        role: "admin",
        caps: ["caller_flags_manage"],
      },
      SESSION_SECRET,
      { nowSeconds: Math.floor(Date.now() / 1000), ttlSeconds: 600 },
    );

    const response = await request(app)
      .post("/miniapp/action")
      .set("Authorization", `Bearer ${token}`)
      .send({
        action: "callscript.update",
        payload: {
          id: 999999,
          description: "Updated via miniapp action bridge fallback path",
          prompt: "Bridge update prompt.",
          first_message: "Bridge update first message.",
          default_profile: "friendship",
          objective_tags: "general_outreach",
        },
      });

    expect(response.status).toBe(404);
    expect(response.body?.code).toBe("miniapp_action_failed");
    expect(String(response.body?.error || "").toLowerCase()).toContain("script not found");
  });

  test("POST /miniapp/action enforces capability for runtime.status", async () => {
    const viewerTelegramId = "miniapp-runtime-viewer";
    const roleSeed = await request(app)
      .post("/admin/miniapp/users/role")
      .set("x-admin-token", ADMIN_API_TOKEN)
      .send({
        telegram_id: viewerTelegramId,
        role: "viewer",
        reason: "test runtime capability guard",
      });
    expect(roleSeed.status).toBe(200);
    expect(roleSeed.body?.success).toBe(true);

    const token = createMiniAppSessionToken(
      {
        jti: "test-jti-runtime-capability",
        sub: `tg:${viewerTelegramId}`,
        telegram_id: viewerTelegramId,
        role: "admin",
        caps: ["provider_manage"],
      },
      SESSION_SECRET,
      { nowSeconds: Math.floor(Date.now() / 1000), ttlSeconds: 600 },
    );

    const response = await request(app)
      .post("/miniapp/action")
      .set("Authorization", `Bearer ${token}`)
      .send({
        action: "runtime.status",
        payload: {},
      });

    expect(response.status).toBe(403);
    expect(response.body?.code).toBe("miniapp_capability_denied");
  });

  test("POST /miniapp/action enforces capabilities for users role management", async () => {
    const viewerTelegramId = "miniapp-viewer-cap-test";
    const roleSeed = await request(app)
      .post("/admin/miniapp/users/role")
      .set("x-admin-token", ADMIN_API_TOKEN)
      .send({
        telegram_id: viewerTelegramId,
        role: "viewer",
        reason: "test capability guard",
      });
    expect(roleSeed.status).toBe(200);
    expect(roleSeed.body?.success).toBe(true);

    const token = createMiniAppSessionToken(
      {
        jti: "test-jti-users-capability",
        sub: `tg:${viewerTelegramId}`,
        telegram_id: viewerTelegramId,
        role: "admin",
        caps: ["dashboard_view", "users_manage"],
      },
      SESSION_SECRET,
      { nowSeconds: Math.floor(Date.now() / 1000), ttlSeconds: 600 },
    );

    const response = await request(app)
      .post("/miniapp/action")
      .set("Authorization", `Bearer ${token}`)
      .send({ action: "users.list", payload: {} });

    expect(response.status).toBe(403);
    expect(response.body?.code).toBe("miniapp_capability_denied");
  });

  test("POST /miniapp/action validates provider preflight and users role payloads", async () => {
    const token = createMiniAppSessionToken(
      {
        jti: "test-jti-action-payloads",
        sub: `tg:${ADMIN_USER_ID}`,
        telegram_id: ADMIN_USER_ID,
        role: "admin",
        caps: ["provider_manage", "users_manage", "dashboard_view"],
      },
      SESSION_SECRET,
      { nowSeconds: Math.floor(Date.now() / 1000), ttlSeconds: 600 },
    );

    const invalidPreflight = await request(app)
      .post("/miniapp/action")
      .set("Authorization", `Bearer ${token}`)
      .send({ action: "provider.preflight", payload: { channel: "sms" } });

    expect(invalidPreflight.status).toBe(400);
    expect(invalidPreflight.body?.code).toBe("miniapp_action_invalid");
    expect(String(invalidPreflight.body?.error || "").toLowerCase()).toContain("required");

    const invalidRoleUpdate = await request(app)
      .post("/miniapp/action")
      .set("Authorization", `Bearer ${token}`)
      .send({
        action: "users.role.set",
        payload: { telegram_id: "12345", role: "superadmin", reason: "invalid-role-test" },
      });

    expect(invalidRoleUpdate.status).toBe(400);
    expect(invalidRoleUpdate.body?.code).toBe("miniapp_action_invalid");
    expect(String(invalidRoleUpdate.body?.error || "").toLowerCase()).toContain("role");

    const missingReasonRoleUpdate = await request(app)
      .post("/miniapp/action")
      .set("Authorization", `Bearer ${token}`)
      .send({
        action: "users.role.set",
        payload: { telegram_id: "12345", role: "viewer" },
      });

    expect(missingReasonRoleUpdate.status).toBe(400);
    expect(missingReasonRoleUpdate.body?.code).toBe("miniapp_action_invalid");
    expect(String(missingReasonRoleUpdate.body?.error || "").toLowerCase()).toContain("reason");

    const invalidScriptListFlow = await request(app)
      .post("/miniapp/action")
      .set("Authorization", `Bearer ${token}`)
      .send({
        action: "callscript.list",
        payload: { flow_type: "totally_invalid_flow" },
      });

    expect(invalidScriptListFlow.status).toBe(400);
    expect(invalidScriptListFlow.body?.code).toBe("miniapp_action_invalid");
    expect(String(invalidScriptListFlow.body?.error || "").toLowerCase()).toContain("flow_type");

    const invalidScriptReviewDecision = await request(app)
      .post("/miniapp/action")
      .set("Authorization", `Bearer ${token}`)
      .send({
        action: "callscript.review",
        payload: { id: 10, decision: "shipit" },
      });

    expect(invalidScriptReviewDecision.status).toBe(400);
    expect(invalidScriptReviewDecision.body?.code).toBe("miniapp_action_invalid");
    expect(String(invalidScriptReviewDecision.body?.error || "").toLowerCase()).toContain("decision");
  });

  test("POST /miniapp/action keeps unsupported action validation stable across retries", async () => {
    const token = createMiniAppSessionToken(
      {
        jti: "test-jti-3",
        sub: `tg:${ADMIN_USER_ID}`,
        telegram_id: ADMIN_USER_ID,
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
    expect(first.body?.code).toBe("miniapp_action_invalid");
    expect(second.status).toBe(400);
    expect(second.body?.code).toBe("miniapp_action_invalid");
    expect(third.status).toBe(400);
    expect(third.body?.code).toBe("miniapp_action_invalid");
  });
});
