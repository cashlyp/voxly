"use strict";

const crypto = require("crypto");

class MiniAppAuthError extends Error {
  constructor(message, code = "miniapp_auth_invalid", status = 401) {
    super(message);
    this.name = "MiniAppAuthError";
    this.code = code;
    this.status = status;
  }
}

function base64UrlEncode(value) {
  return Buffer.from(String(value), "utf8")
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function base64UrlDecode(value) {
  const source = String(value || "").replace(/-/g, "+").replace(/_/g, "/");
  const pad = source.length % 4;
  const padded = pad ? source + "=".repeat(4 - pad) : source;
  return Buffer.from(padded, "base64").toString("utf8");
}

function safeJsonParse(value, fallback = null) {
  if (!value) return fallback;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function parseInitData(initDataRaw) {
  const raw = String(initDataRaw || "").trim();
  if (!raw) {
    throw new MiniAppAuthError(
      "Missing Telegram init data",
      "miniapp_missing_init_data",
      400,
    );
  }

  const params = new URLSearchParams(raw);
  const entries = {};
  for (const [key, value] of params.entries()) {
    entries[key] = value;
  }

  const authDate = Number(entries.auth_date);
  const queryId = entries.query_id || null;
  const hash = entries.hash || null;
  const user = safeJsonParse(entries.user, null);
  const chat = safeJsonParse(entries.chat, null);
  const receiver = safeJsonParse(entries.receiver, null);

  return {
    raw,
    params,
    entries,
    authDate,
    queryId,
    hash,
    user,
    chat,
    receiver,
  };
}

function buildDataCheckString(input) {
  const params =
    input instanceof URLSearchParams
      ? input
      : new URLSearchParams(String(input || ""));
  const pairs = [];
  for (const [key, value] of params.entries()) {
    // Telegram hash verification excludes only `hash`.
    if (key === "hash") continue;
    pairs.push([key, value]);
  }
  pairs.sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0));
  return pairs.map(([key, value]) => `${key}=${value}`).join("\n");
}

function computeInitDataHash(initDataRaw, botToken) {
  const params = new URLSearchParams(String(initDataRaw || ""));
  const dataCheckString = buildDataCheckString(params);
  const secret = crypto
    .createHmac("sha256", "WebAppData")
    .update(String(botToken || ""))
    .digest();
  return crypto
    .createHmac("sha256", secret)
    .update(dataCheckString)
    .digest("hex");
}

function compareHash(leftHex, rightHex) {
  const left = Buffer.from(String(leftHex || ""), "hex");
  const right = Buffer.from(String(rightHex || ""), "hex");
  if (!left.length || left.length !== right.length) {
    return false;
  }
  return crypto.timingSafeEqual(left, right);
}

function validateInitData(initDataRaw, botToken, options = {}) {
  const parsed = parseInitData(initDataRaw);
  const token = String(botToken || "").trim();
  if (!token) {
    throw new MiniAppAuthError(
      "Mini App bot token is not configured",
      "miniapp_missing_bot_token",
      500,
    );
  }
  if (!parsed.hash) {
    throw new MiniAppAuthError(
      "Missing Telegram init data hash",
      "miniapp_missing_hash",
      401,
    );
  }
  if (!Number.isFinite(parsed.authDate) || parsed.authDate <= 0) {
    throw new MiniAppAuthError(
      "Invalid Telegram auth date",
      "miniapp_invalid_auth_date",
      401,
    );
  }

  const expectedHash = computeInitDataHash(parsed.raw, token);
  if (!compareHash(parsed.hash, expectedHash)) {
    throw new MiniAppAuthError(
      "Telegram init data signature is invalid",
      "miniapp_invalid_signature",
      401,
    );
  }

  const nowSeconds = Number.isFinite(Number(options.nowSeconds))
    ? Number(options.nowSeconds)
    : Math.floor(Date.now() / 1000);
  const maxAgeSeconds = Number.isFinite(Number(options.maxAgeSeconds))
    ? Number(options.maxAgeSeconds)
    : 300;
  const ageSeconds = nowSeconds - parsed.authDate;
  if (ageSeconds < -30) {
    throw new MiniAppAuthError(
      "Telegram auth date is in the future",
      "miniapp_auth_date_future",
      401,
    );
  }
  if (maxAgeSeconds > 0 && ageSeconds > maxAgeSeconds) {
    throw new MiniAppAuthError(
      "Telegram init data is expired",
      "miniapp_init_data_expired",
      401,
    );
  }

  return {
    ...parsed,
    nowSeconds,
    ageSeconds,
  };
}

function signToken(payload, secret) {
  const header = { alg: "HS256", typ: "JWT" };
  const encodedHeader = base64UrlEncode(JSON.stringify(header));
  const encodedPayload = base64UrlEncode(JSON.stringify(payload));
  const signature = crypto
    .createHmac("sha256", String(secret || ""))
    .update(`${encodedHeader}.${encodedPayload}`)
    .digest("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
  return `${encodedHeader}.${encodedPayload}.${signature}`;
}

function createMiniAppSessionToken(claims, secret, options = {}) {
  const tokenSecret = String(secret || "").trim();
  if (!tokenSecret) {
    throw new MiniAppAuthError(
      "Mini App session secret is not configured",
      "miniapp_missing_session_secret",
      500,
    );
  }
  const nowSeconds = Number.isFinite(Number(options.nowSeconds))
    ? Number(options.nowSeconds)
    : Math.floor(Date.now() / 1000);
  const ttlSeconds = Number.isFinite(Number(options.ttlSeconds))
    ? Number(options.ttlSeconds)
    : 900;
  const payload = {
    ...claims,
    iat: nowSeconds,
    nbf: nowSeconds,
    exp: nowSeconds + Math.max(60, ttlSeconds),
  };
  return signToken(payload, tokenSecret);
}

function verifyMiniAppSessionToken(token, secret, options = {}) {
  const value = String(token || "").trim();
  const tokenSecret = String(secret || "").trim();
  if (!value || !tokenSecret) {
    throw new MiniAppAuthError(
      "Invalid Mini App session token",
      "miniapp_invalid_token",
      401,
    );
  }
  const parts = value.split(".");
  if (parts.length !== 3) {
    throw new MiniAppAuthError(
      "Malformed Mini App session token",
      "miniapp_malformed_token",
      401,
    );
  }
  const [encodedHeader, encodedPayload, encodedSignature] = parts;
  const expectedSignature = crypto
    .createHmac("sha256", tokenSecret)
    .update(`${encodedHeader}.${encodedPayload}`)
    .digest();
  const providedSignature = Buffer.from(
    encodedSignature.replace(/-/g, "+").replace(/_/g, "/"),
    "base64",
  );
  if (
    !providedSignature.length ||
    providedSignature.length !== expectedSignature.length ||
    !crypto.timingSafeEqual(expectedSignature, providedSignature)
  ) {
    throw new MiniAppAuthError(
      "Mini App session signature is invalid",
      "miniapp_invalid_token_signature",
      401,
    );
  }

  const header = safeJsonParse(base64UrlDecode(encodedHeader), null);
  const payload = safeJsonParse(base64UrlDecode(encodedPayload), null);
  if (!header || !payload || String(header.alg || "").toUpperCase() !== "HS256") {
    throw new MiniAppAuthError(
      "Mini App session token is invalid",
      "miniapp_invalid_token_payload",
      401,
    );
  }

  const nowSeconds = Number.isFinite(Number(options.nowSeconds))
    ? Number(options.nowSeconds)
    : Math.floor(Date.now() / 1000);
  const skewSeconds = Number.isFinite(Number(options.skewSeconds))
    ? Number(options.skewSeconds)
    : 30;

  if (Number.isFinite(Number(payload.nbf)) && nowSeconds + skewSeconds < Number(payload.nbf)) {
    throw new MiniAppAuthError(
      "Mini App session token is not active",
      "miniapp_token_not_active",
      401,
    );
  }
  if (Number.isFinite(Number(payload.exp)) && nowSeconds - skewSeconds > Number(payload.exp)) {
    throw new MiniAppAuthError(
      "Mini App session token expired",
      "miniapp_token_expired",
      401,
    );
  }

  return payload;
}

module.exports = {
  MiniAppAuthError,
  parseInitData,
  buildDataCheckString,
  computeInitDataHash,
  validateInitData,
  createMiniAppSessionToken,
  verifyMiniAppSessionToken,
};
