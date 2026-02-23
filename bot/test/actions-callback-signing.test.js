"use strict";

const assert = require("assert");
process.env.ADMIN_TELEGRAM_ID = process.env.ADMIN_TELEGRAM_ID || "1";
process.env.ADMIN_TELEGRAM_USERNAME =
  process.env.ADMIN_TELEGRAM_USERNAME || "admin";
process.env.API_URL = process.env.API_URL || "http://localhost:3000";
process.env.BOT_TOKEN =
  process.env.BOT_TOKEN || "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi";
process.env.API_SECRET = process.env.API_SECRET || "test-secret";
const {
  buildCallbackData,
  parseCallbackData,
  validateCallback,
} = require("../utils/actions");

function createCtx(token = "1234abcd") {
  return {
    session: {
      currentOp: {
        id: "1234abcd-1234-1234-1234-1234567890ab",
        token,
        startedAt: Date.now(),
      },
    },
  };
}

function testShortActionUsesRegularSignedPayload() {
  const ctx = createCtx();
  const payload = buildCallbackData(ctx, "MENU");
  assert.ok(payload.startsWith("cb|"));
  const parsed = parseCallbackData(payload);
  assert.strictEqual(parsed.action, "MENU");
  assert.strictEqual(parsed.valid, true);
}

function testLongActionUsesAliasSignedPayload() {
  const ctx = createCtx();
  const longAction =
    "call-script-main:1234abcd:prompt:select:very-long-option-token-value";
  const payload = buildCallbackData(ctx, longAction);
  assert.ok(payload.startsWith("cbk|"));

  const parsed = parseCallbackData(payload);
  assert.strictEqual(parsed.action, longAction);
  assert.strictEqual(parsed.valid, true);

  const validation = validateCallback(ctx, payload);
  assert.strictEqual(validation.status, "ok");
  assert.strictEqual(validation.action, longAction);
}

function testAliasSignedPayloadRespectsOperationToken() {
  const sourceCtx = createCtx("1234abcd");
  const longAction =
    "sms-script-select:1234abcd:prompt:select:another-long-option-token";
  const payload = buildCallbackData(sourceCtx, longAction);
  assert.ok(payload.startsWith("cbk|"));

  const staleCtx = createCtx("deadbeef");
  const validation = validateCallback(staleCtx, payload);
  assert.strictEqual(validation.status, "stale");
}

testShortActionUsesRegularSignedPayload();
testLongActionUsesAliasSignedPayload();
testAliasSignedPayloadRespectsOperationToken();
