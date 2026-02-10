#!/usr/bin/env node
"use strict";

/**
 * Lightweight provider parity harness.
 *
 * This does not replace end-to-end telephony tests. It validates that the
 * provider-specific webhook handlers produce expected control responses in-process.
 */

process.env.NODE_ENV = process.env.NODE_ENV || "development";
process.env.TWILIO_WEBHOOK_VALIDATION =
  process.env.TWILIO_WEBHOOK_VALIDATION || "off";
process.env.VONAGE_WEBHOOK_VALIDATION =
  process.env.VONAGE_WEBHOOK_VALIDATION || "off";
process.env.ADMIN_API_TOKEN =
  process.env.ADMIN_API_TOKEN || "test_admin_token";
process.env.TWILIO_ACCOUNT_SID =
  process.env.TWILIO_ACCOUNT_SID || "AC00000000000000000000000000000000";
process.env.TWILIO_AUTH_TOKEN =
  process.env.TWILIO_AUTH_TOKEN || "test_auth_token";
process.env.FROM_NUMBER = process.env.FROM_NUMBER || "+10000000000";

const request = require("supertest");
const { app } = require("../app");

function safeReadEndpointUri(body) {
  if (!Array.isArray(body) || !body.length) return "";
  const first = body[0];
  const endpoint = first?.endpoint;
  if (!Array.isArray(endpoint) || !endpoint.length) return "";
  return String(endpoint[0]?.uri || "");
}

function safeReadEndpointContentType(body) {
  if (!Array.isArray(body) || !body.length) return "";
  const first = body[0];
  const endpoint = first?.endpoint;
  if (!Array.isArray(endpoint) || !endpoint.length) return "";
  return String(endpoint[0]?.["content-type"] || "");
}

function safeReadConnectEventUrl(body) {
  if (!Array.isArray(body) || !body.length) return "";
  const first = body[0];
  const eventUrl = first?.eventUrl || first?.event_url;
  if (Array.isArray(eventUrl) && eventUrl.length) {
    return String(eventUrl[0] || "");
  }
  if (typeof eventUrl === "string") {
    return eventUrl;
  }
  return "";
}

function safeReadConnectEventMethod(body) {
  if (!Array.isArray(body) || !body.length) return "";
  const first = body[0];
  return String(first?.eventMethod || first?.event_method || "");
}

function assertSupportedVonageContentType(contentType) {
  const normalized = String(contentType || "").trim().toLowerCase();
  if (!normalized) {
    throw new Error("Vonage NCCO websocket endpoint missing content-type");
  }
  if (!normalized.startsWith("audio/l16") && !normalized.startsWith("audio/pcmu")) {
    throw new Error(`Unexpected Vonage websocket content-type: ${contentType}`);
  }
}

async function runCheck(name, fn, options = {}) {
  const startedAt = Date.now();
  try {
    const detail = await fn();
    return {
      name,
      ok: true,
      critical: options.critical !== false,
      ms: Date.now() - startedAt,
      detail: detail || "",
    };
  } catch (error) {
    return {
      name,
      ok: false,
      critical: options.critical !== false,
      ms: Date.now() - startedAt,
      detail: error?.message || String(error),
    };
  }
}

async function checkVonageInboundAnswer() {
  const inboundUuid = "inbound-uuid-001";
  const res = await request(app)
    .get("/webhook/vonage/answer")
    .query({
      uuid: inboundUuid,
      from: "+14155550123",
      to: "+14155550999",
    })
    .expect(200);
  const wsUri = safeReadEndpointUri(res.body);
  const wsContentType = safeReadEndpointContentType(res.body);
  const eventUri = safeReadConnectEventUrl(res.body);
  const eventMethod = safeReadConnectEventMethod(res.body);
  if (!wsUri) {
    throw new Error("No websocket URI returned for Vonage inbound answer");
  }
  assertSupportedVonageContentType(wsContentType);
  if (!wsUri.includes("callSid=vonage-in-inbound-uuid-001")) {
    throw new Error(`Unexpected inbound callSid in wsUri: ${wsUri}`);
  }
  if (!wsUri.includes("direction=inbound")) {
    throw new Error(`Inbound direction missing in wsUri: ${wsUri}`);
  }
  if (!eventUri) {
    throw new Error("No event URL returned for Vonage inbound answer");
  }
  if (!eventUri.includes("/webhook/vonage/event")) {
    throw new Error(`Unexpected inbound event URL: ${eventUri}`);
  }
  if (!eventUri.includes("callSid=vonage-in-inbound-uuid-001")) {
    throw new Error(`Inbound callSid missing in event URL: ${eventUri}`);
  }
  if (String(eventMethod || "").toUpperCase() !== "POST") {
    throw new Error(`Unexpected inbound event method: ${eventMethod || "none"}`);
  }
  return `wsUri=${wsUri}, eventUri=${eventUri}, content-type=${wsContentType}`;
}

async function checkVonageOutboundAnswer() {
  const res = await request(app)
    .get("/webhook/vonage/answer")
    .query({
      callSid: "test-outbound-call-001",
      uuid: "outbound-uuid-001",
    })
    .expect(200);
  const wsUri = safeReadEndpointUri(res.body);
  const wsContentType = safeReadEndpointContentType(res.body);
  const eventUri = safeReadConnectEventUrl(res.body);
  const eventMethod = safeReadConnectEventMethod(res.body);
  if (!wsUri) {
    throw new Error("No websocket URI returned for Vonage outbound answer");
  }
  assertSupportedVonageContentType(wsContentType);
  if (!wsUri.includes("callSid=test-outbound-call-001")) {
    throw new Error(`Outbound callSid mismatch in wsUri: ${wsUri}`);
  }
  if (!wsUri.includes("direction=outbound")) {
    throw new Error(`Outbound direction missing in wsUri: ${wsUri}`);
  }
  if (!eventUri) {
    throw new Error("No event URL returned for Vonage outbound answer");
  }
  if (!eventUri.includes("/webhook/vonage/event")) {
    throw new Error(`Unexpected outbound event URL: ${eventUri}`);
  }
  if (!eventUri.includes("callSid=test-outbound-call-001")) {
    throw new Error(`Outbound callSid missing in event URL: ${eventUri}`);
  }
  if (String(eventMethod || "").toUpperCase() !== "POST") {
    throw new Error(`Unexpected outbound event method: ${eventMethod || "none"}`);
  }
  return `wsUri=${wsUri}, eventUri=${eventUri}, content-type=${wsContentType}`;
}

async function checkTwilioIncomingTwiml() {
  const res = await request(app)
    .post("/incoming")
    .type("form")
    .send({
      Direction: "outbound-api",
      From: "+14155550123",
      To: "+14155550999",
    })
    .expect(200);
  const body = String(res.text || "");
  if (!body.includes("<Connect>") || !body.includes("<Stream")) {
    throw new Error("Twilio /incoming did not return stream TwiML");
  }
  return "returned streaming TwiML";
}

async function checkVonageDtmfWebhook() {
  const res = await request(app)
    .post("/webhook/vonage/event")
    .send({
      callSid: "test-vonage-dtmf-001",
      uuid: "test-vonage-dtmf-uuid-001",
      event: "dtmf",
      dtmf: {
        digits: "1234",
      },
    })
    .expect(200);
  const body = String(res.text || "");
  if (body.trim().toUpperCase() !== "OK") {
    throw new Error(`Unexpected Vonage DTMF webhook response body: ${body}`);
  }
  return "accepted DTMF webhook";
}

async function checkProviderOverrideAdminEndpoints() {
  const adminToken = process.env.ADMIN_API_TOKEN;
  const listRes = await request(app)
    .get("/admin/provider/keypad-overrides")
    .set("x-admin-token", adminToken)
    .expect(200);
  if (!listRes.body?.success) {
    throw new Error("Failed to list keypad overrides via admin endpoint");
  }

  const clearRes = await request(app)
    .post("/admin/provider/keypad-overrides/clear")
    .set("x-admin-token", adminToken)
    .send({ all: true })
    .expect(200);
  if (!clearRes.body?.success) {
    throw new Error("Failed to clear keypad overrides via admin endpoint");
  }
  if (!Array.isArray(clearRes.body?.overrides)) {
    throw new Error("Override clear response missing overrides list");
  }
  return "admin override endpoints OK";
}

async function main() {
  const checks = [];
  checks.push(
    await runCheck("vonage_inbound_answer_bootstrap", checkVonageInboundAnswer),
  );
  checks.push(
    await runCheck(
      "vonage_outbound_answer_callsid",
      checkVonageOutboundAnswer,
    ),
  );
  checks.push(
    await runCheck("twilio_incoming_stream_twiml", checkTwilioIncomingTwiml),
  );
  checks.push(
    await runCheck("vonage_dtmf_webhook_acceptance", checkVonageDtmfWebhook),
  );
  checks.push(
    await runCheck(
      "provider_keypad_override_admin_endpoints",
      checkProviderOverrideAdminEndpoints,
    ),
  );

  const lines = [];
  lines.push("Provider Parity Harness");
  lines.push("=======================");
  checks.forEach((item) => {
    const status = item.ok ? "PASS" : "FAIL";
    lines.push(
      `${status} ${item.name} (${item.ms}ms)${item.detail ? ` :: ${item.detail}` : ""}`,
    );
  });

  const failedCritical = checks.filter((item) => !item.ok && item.critical);
  lines.push("");
  lines.push(
    `Summary: ${checks.length - failedCritical.length}/${checks.length} critical checks passed`,
  );
  console.log(lines.join("\n"));

  if (failedCritical.length) {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error("Provider parity harness failed:", error);
  process.exit(1);
});
