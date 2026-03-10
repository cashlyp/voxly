"use strict";

const crypto = require("crypto");
const { sanitizeVoiceOutputText } = require("../utils/voiceOutputGuard");
const {
  getProfileRuntimeContract,
  normalizeProfileType,
  listProfileTypes,
} = require("../functions/profileRegistry");

const KNOWN_CALL_PROVIDERS = new Set(["twilio", "vonage", "aws"]);
const URL_REGEX = /\bhttps?:\/\/[^\s]+/i;
const MARKDOWN_TOKEN_REGEX = /[`*_~#>[\]]/;
const NON_SPOKEN_TOKEN_REGEX = /(^|\s)(lol|lmao|rofl|brb|ttyl)(\s|$)/i;

function normalizeProvider(value = "") {
  const normalized = String(value || "").trim().toLowerCase();
  return KNOWN_CALL_PROVIDERS.has(normalized) ? normalized : "";
}

function normalizeProfile(value = "") {
  return normalizeProfileType(String(value || "").trim().toLowerCase());
}

function normalizeList(values = []) {
  if (!Array.isArray(values)) return [];
  return values.map((value) => String(value || "").trim()).filter(Boolean);
}

function buildCanaryPrompt(profile = "general", runtimeContract = null) {
  const safeFallback = String(runtimeContract?.safeFallback || "").trim();
  const fallbackLine = safeFallback
    ? `If uncertain, use this fallback: "${safeFallback}".`
    : "If uncertain, keep the response safe, concise, and professional.";
  return [
    `Synthetic canary validation call for profile ${profile}.`,
    "Objective: verify first greeting quality, response continuity, and call stability.",
    "Use natural spoken wording. Keep it concise and non-robotic.",
    fallbackLine,
  ].join(" ");
}

function buildCanaryFirstMessage(profile = "general", runtimeContract = null) {
  const fallback = String(runtimeContract?.defaultFirstMessage || "").trim();
  if (fallback) {
    return fallback;
  }
  return `Hello, this is a quick ${profile} quality canary call.`;
}

function buildCanaryIdempotencyKey({
  provider = "unknown",
  profile = "general",
  windowMs = 300000,
  nowMs = Date.now(),
} = {}) {
  const normalizedWindowMs = Number.isFinite(Number(windowMs))
    ? Math.max(60000, Math.floor(Number(windowMs)))
    : 300000;
  const bucket = Math.floor(nowMs / normalizedWindowMs);
  return `canary:${provider}:${profile}:${bucket}`;
}

function parseJson(value) {
  if (!value || typeof value !== "string") return null;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function initProviderCounter(providers = []) {
  const acc = {};
  providers.forEach((provider) => {
    const key = normalizeProvider(provider);
    if (!key) return;
    acc[key] = 0;
  });
  return acc;
}

function countQuestionMarks(value = "") {
  const matches = String(value || "").match(/\?/g);
  return Array.isArray(matches) ? matches.length : 0;
}

function clampNumber(value, min, max, fallback) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  if (parsed < min) return min;
  if (parsed > max) return max;
  return parsed;
}

function normalizeCanaryQualityConfig(rawConfig = {}) {
  return {
    enabled: rawConfig?.enabled !== false,
    failClosed: rawConfig?.failClosed === true,
    minScore: clampNumber(rawConfig?.minScore, 0, 100, 70),
    minFirstMessageChars: clampNumber(rawConfig?.minFirstMessageChars, 8, 80, 18),
    maxPromptChars: clampNumber(rawConfig?.maxPromptChars, 280, 4000, 1200),
    maxSentenceWords: clampNumber(rawConfig?.maxSentenceWords, 10, 80, 30),
  };
}

function evaluateCanaryConversationQuality({
  profile = "general",
  prompt = "",
  firstMessage = "",
  runtimeContract = null,
  qualityConfig = {},
} = {}) {
  const cfg = normalizeCanaryQualityConfig(qualityConfig);
  if (cfg.enabled !== true) {
    return {
      enabled: false,
      blocked: false,
      status: "disabled",
      score: 100,
      findings: [],
      adjustments: [],
      firstMessage: String(firstMessage || "").trim(),
      prompt: String(prompt || "").trim(),
      first_message: String(firstMessage || "").trim(),
    };
  }

  const responseConstraints = runtimeContract?.responseConstraints || {};
  const questionLimit = clampNumber(responseConstraints?.maxQuestions, 0, 3, 1);
  const maxChars = clampNumber(responseConstraints?.maxChars, 80, 500, 260);
  const fallbackFirstMessage =
    String(runtimeContract?.defaultFirstMessage || "").trim() ||
    `Hello, this is a quick ${profile} quality canary call.`;
  const fallbackPrompt = buildCanaryPrompt(profile, runtimeContract);
  const firstInput = String(firstMessage || "").trim();
  const promptInput = String(prompt || "").trim();

  const sanitizedFirst = sanitizeVoiceOutputText(firstInput || fallbackFirstMessage, {
    fallbackText: fallbackFirstMessage,
    maxChars,
  });
  const sanitizedPrompt = sanitizeVoiceOutputText(promptInput || fallbackPrompt, {
    fallbackText: fallbackPrompt,
    maxChars: cfg.maxPromptChars,
  });

  let score = 100;
  const findings = [];
  const adjustments = [];
  const addFinding = (severity, code, message, penalty = 0) => {
    findings.push({ severity, code, message });
    if (Number.isFinite(penalty) && penalty > 0) {
      score = Math.max(0, score - penalty);
    }
  };
  const addAdjustment = (field, reason) => {
    adjustments.push({ field, reason });
  };

  if (!firstInput) {
    addFinding(
      "blocker",
      "missing_first_message",
      "First message is missing and fallback text was injected.",
      28,
    );
  }

  if (firstInput && firstInput.length < cfg.minFirstMessageChars) {
    addFinding(
      "warning",
      "first_message_too_short",
      `First message should be at least ${cfg.minFirstMessageChars} chars for natural delivery.`,
      14,
    );
  }

  if (sanitizedFirst.changed) {
    addAdjustment("first_message", "voice_sanitized");
    addFinding(
      "warning",
      "first_message_sanitized",
      "First message contained non-spoken or noisy content and was normalized.",
      8,
    );
  }

  if (sanitizedPrompt.changed) {
    addAdjustment("prompt", "voice_sanitized");
    addFinding(
      "warning",
      "prompt_sanitized",
      "Prompt contained non-spoken or noisy content and was normalized.",
      4,
    );
  }

  const firstQuestionCount = countQuestionMarks(sanitizedFirst.text);
  if (firstQuestionCount > questionLimit) {
    addFinding(
      "blocker",
      "too_many_questions",
      `First message includes ${firstQuestionCount} direct questions but limit is ${questionLimit}.`,
      20,
    );
  }

  const sentenceWordCounts = sanitizedFirst.text
    .split(/[.!?]/)
    .map((sentence) =>
      sentence
        .trim()
        .split(/\s+/)
        .filter(Boolean).length,
    )
    .filter((count) => count > 0);
  const maxSentenceWords = sentenceWordCounts.length ? Math.max(...sentenceWordCounts) : 0;
  if (maxSentenceWords > cfg.maxSentenceWords) {
    addFinding(
      "warning",
      "sentence_too_dense",
      `Detected dense sentence (${maxSentenceWords} words). Keep line pacing below ${cfg.maxSentenceWords} words.`,
      12,
    );
  }

  if (URL_REGEX.test(firstInput) || URL_REGEX.test(promptInput)) {
    addFinding(
      "warning",
      "contains_url",
      "URL-like content was detected and normalized for spoken delivery.",
      8,
    );
  }

  if (MARKDOWN_TOKEN_REGEX.test(firstInput) || MARKDOWN_TOKEN_REGEX.test(promptInput)) {
    addFinding(
      "warning",
      "contains_markdown",
      "Markdown tokens were detected and normalized for spoken delivery.",
      6,
    );
  }

  if (NON_SPOKEN_TOKEN_REGEX.test(firstInput)) {
    addFinding(
      "warning",
      "non_spoken_slang",
      "Detected non-spoken chat slang in first message.",
      10,
    );
  }

  const blockerCodes = findings
    .filter((entry) => entry.severity === "blocker")
    .map((entry) => entry.code);
  const blocked = cfg.failClosed === true && (blockerCodes.length > 0 || score < cfg.minScore);
  const status = blocked
    ? "blocked"
    : findings.length
      ? "warn"
      : "pass";

  return {
    enabled: true,
    blocked,
    status,
    score,
    findings,
    adjustments,
    blocker_codes: blockerCodes,
    firstMessage: sanitizedFirst.text,
    prompt: sanitizedPrompt.text,
    first_message: sanitizedFirst.text,
  };
}

async function runCallCanarySweep(options = {}) {
  const {
    config = {},
    placeOutboundCall,
    getProviderReadiness,
    runWithTimeout,
    recordProviderError,
    recordProviderFlowMetric,
  } = options;
  const canaryConfig = config.callCanary || {};
  const enabled = canaryConfig.enabled === true;
  if (!enabled) {
    return { ok: true, skipped: true, reason: "disabled" };
  }
  if (typeof placeOutboundCall !== "function") {
    return { ok: false, skipped: true, reason: "missing_place_outbound_call" };
  }
  if (typeof getProviderReadiness !== "function") {
    return { ok: false, skipped: true, reason: "missing_provider_readiness" };
  }

  const readiness = getProviderReadiness() || {};
  const configuredProviders = normalizeList(canaryConfig.providers).map(normalizeProvider);
  const providerCandidates = (configuredProviders.length
    ? configuredProviders
    : ["twilio", "vonage"])
    .filter(Boolean)
    .filter((provider, index, arr) => arr.indexOf(provider) === index)
    .filter((provider) => readiness?.[provider] === true);
  if (!providerCandidates.length) {
    return { ok: true, skipped: true, reason: "no_ready_provider" };
  }

  const knownProfiles = new Set(listProfileTypes().map((value) => String(value || "").trim().toLowerCase()));
  const configuredProfiles = normalizeList(canaryConfig.profiles)
    .map(normalizeProfile)
    .filter(Boolean)
    .filter((profile, index, arr) => arr.indexOf(profile) === index)
    .filter((profile) => knownProfiles.has(profile));
  const profileCandidates = configuredProfiles.length
    ? configuredProfiles
    : ["creator", "friendship"].filter((profile) => knownProfiles.has(profile));
  if (!profileCandidates.length) {
    return { ok: true, skipped: true, reason: "no_profile_candidates" };
  }

  const maxCallsPerRun = Number.isFinite(Number(canaryConfig.maxCallsPerRun))
    ? Math.max(1, Math.min(12, Math.floor(Number(canaryConfig.maxCallsPerRun))))
    : 2;
  const dryRun = canaryConfig.dryRun !== false;
  const timeoutMs = Number.isFinite(Number(canaryConfig.timeoutMs))
    ? Math.max(10000, Math.floor(Number(canaryConfig.timeoutMs)))
    : 60000;
  const targetNumber = String(canaryConfig.targetNumber || "").trim();
  const canPlaceLiveCalls = !dryRun && /^\+[1-9]\d{6,14}$/.test(targetNumber);
  const userChatId = String(canaryConfig.userChatId || "").trim() || null;
  const idempotencyWindowMs = Number.isFinite(Number(canaryConfig.idempotencyWindowMs))
    ? Math.max(60000, Math.floor(Number(canaryConfig.idempotencyWindowMs)))
    : 300000;
  const qualityConfig = normalizeCanaryQualityConfig(canaryConfig.quality || {});

  const runId = crypto.randomUUID();
  const startedAt = Date.now();
  const attempts = [];
  let totalAttempted = 0;
  let successCount = 0;
  let failureCount = 0;
  const providerStats = initProviderCounter(providerCandidates);
  const qualityStats = {
    enabled: qualityConfig.enabled === true,
    fail_closed: qualityConfig.failClosed === true,
    min_score: qualityConfig.minScore,
    checked: 0,
    blocked: 0,
    adjusted: 0,
    warned: 0,
  };

  for (const provider of providerCandidates) {
    for (const profile of profileCandidates) {
      if (totalAttempted >= maxCallsPerRun) break;
      totalAttempted += 1;
      providerStats[provider] = (providerStats[provider] || 0) + 1;
      const runtimeContract = getProfileRuntimeContract(profile);
      const callPayload = {
        number: targetNumber || "+10000000000",
        prompt: buildCanaryPrompt(profile, runtimeContract),
        first_message: buildCanaryFirstMessage(profile, runtimeContract),
        call_profile: profile,
        conversation_profile: profile,
        profile_lock: true,
        user_chat_id: userChatId,
        preferred_provider: provider,
        idempotency_key: buildCanaryIdempotencyKey({
          provider,
          profile,
          windowMs: idempotencyWindowMs,
          nowMs: startedAt,
        }),
      };
      let quality = null;
      if (qualityConfig.enabled === true) {
        quality = evaluateCanaryConversationQuality({
          profile,
          prompt: callPayload.prompt,
          firstMessage: callPayload.first_message,
          runtimeContract,
          qualityConfig,
        });
        qualityStats.checked += 1;
        if (Array.isArray(quality?.adjustments) && quality.adjustments.length > 0) {
          qualityStats.adjusted += 1;
          callPayload.prompt = quality.prompt;
          callPayload.first_message = quality.first_message;
          recordProviderFlowMetric?.("call_canary_quality_adjusted", {
            channel: "call",
            flow: "synthetic_canary",
            provider,
            profile,
            run_id: runId,
            quality_score: quality.score,
            adjustments: quality.adjustments.map((item) => item.reason).join(","),
          });
        }
        if (Array.isArray(quality?.findings) && quality.findings.length > 0) {
          qualityStats.warned += 1;
        }
      }

      if (quality?.blocked === true) {
        failureCount += 1;
        qualityStats.blocked += 1;
        attempts.push({
          provider,
          profile,
          mode: canPlaceLiveCalls ? "live" : "dry_run",
          status: "quality_blocked",
          quality: {
            status: quality.status,
            score: quality.score,
            blocker_codes: quality.blocker_codes,
          },
        });
        recordProviderFlowMetric?.("call_canary_quality_blocked", {
          channel: "call",
          flow: "synthetic_canary",
          provider,
          profile,
          run_id: runId,
          quality_score: quality.score,
          blocker_codes: quality.blocker_codes,
        });
        continue;
      }

      if (!canPlaceLiveCalls) {
        attempts.push({
          provider,
          profile,
          mode: "dry_run",
          status: "planned",
          quality:
            qualityConfig.enabled === true
              ? {
                status: quality?.status || "pass",
                score: Number(quality?.score || 100),
              }
              : null,
        });
        recordProviderFlowMetric?.("call_canary_planned", {
          channel: "call",
          flow: "synthetic_canary",
          provider,
          profile,
          run_id: runId,
          dry_run: true,
        });
        continue;
      }

      try {
        const runner = () => placeOutboundCall(callPayload);
        const result = typeof runWithTimeout === "function"
          ? await runWithTimeout(runner(), timeoutMs, "call_canary_outbound", "call_canary_timeout")
          : await runner();
        successCount += 1;
        attempts.push({
          provider,
          profile,
          mode: "live",
          status: "ok",
          call_id: result?.callId || null,
          call_status: result?.callStatus || null,
          quality:
            qualityConfig.enabled === true
              ? {
                status: quality?.status || "pass",
                score: Number(quality?.score || 100),
              }
              : null,
        });
        recordProviderFlowMetric?.("call_canary_success", {
          channel: "call",
          flow: "synthetic_canary",
          provider,
          profile,
          run_id: runId,
          call_sid: result?.callId || null,
        });
      } catch (error) {
        failureCount += 1;
        attempts.push({
          provider,
          profile,
          mode: "live",
          status: "failed",
          error: String(error?.message || error || "call_canary_failed"),
        });
        if (typeof recordProviderError === "function") {
          recordProviderError(provider, error);
        }
        recordProviderFlowMetric?.("call_canary_failed", {
          channel: "call",
          flow: "synthetic_canary",
          provider,
          profile,
          run_id: runId,
          reason: String(error?.message || error || "call_canary_failed"),
        });
      }
    }
    if (totalAttempted >= maxCallsPerRun) break;
  }

  return {
    ok: failureCount === 0,
    skipped: false,
    run_id: runId,
    dry_run: !canPlaceLiveCalls,
    live_enabled: canPlaceLiveCalls,
    attempted: totalAttempted,
    success: successCount,
    failed: failureCount,
    providers: providerStats,
    profiles: profileCandidates,
    quality: qualityStats,
    attempts,
    duration_ms: Math.max(0, Date.now() - startedAt),
  };
}

async function evaluateCallCanarySloGuardrail(options = {}) {
  const {
    db,
    config = {},
    onBreach,
    supportedProviders = ["twilio", "vonage", "aws"],
  } = options;
  const canaryConfig = config.callCanary || {};
  const sloConfig = canaryConfig.slo || {};
  if (!sloConfig.enabled || !db?.db) {
    return { ok: true, skipped: true, reason: "disabled" };
  }

  const windowMs = Number.isFinite(Number(sloConfig.windowMs))
    ? Math.max(120000, Math.floor(Number(sloConfig.windowMs)))
    : 900000;
  const minSamples = Number.isFinite(Number(sloConfig.minSamples))
    ? Math.max(1, Math.floor(Number(sloConfig.minSamples)))
    : 3;
  const maxTimeoutEvents = Number.isFinite(Number(sloConfig.maxTimeoutEvents))
    ? Math.max(1, Math.floor(Number(sloConfig.maxTimeoutEvents)))
    : 2;
  const maxStallEvents = Number.isFinite(Number(sloConfig.maxStallEvents))
    ? Math.max(1, Math.floor(Number(sloConfig.maxStallEvents))
      )
    : 2;
  const windowMinutes = Math.max(1, Math.ceil(windowMs / 60000));

  const callRows = await new Promise((resolve) => {
    db.db.all(
      `
        SELECT data
        FROM call_states
        WHERE state = 'call_created'
          AND timestamp >= datetime('now', ?)
      `,
      [`-${windowMinutes} minutes`],
      (err, rows) => {
        if (err) {
          resolve([]);
          return;
        }
        resolve(Array.isArray(rows) ? rows : []);
      },
    );
  });
  const callSamples = {};
  normalizeList(supportedProviders).forEach((provider) => {
    const key = normalizeProvider(provider);
    if (!key) return;
    callSamples[key] = 0;
  });
  for (const row of callRows) {
    const parsed = parseJson(row?.data);
    const provider = normalizeProvider(parsed?.provider);
    if (!provider) continue;
    callSamples[provider] = (callSamples[provider] || 0) + 1;
  }

  const flowRows = await new Promise((resolve) => {
    db.db.all(
      `
        SELECT status, details
        FROM service_health_logs
        WHERE service_name = 'provider_flow'
          AND status IN ('stream_timeout', 'stream_stalled_no_media', 'stream_stalled_stt')
          AND timestamp >= datetime('now', ?)
      `,
      [`-${windowMinutes} minutes`],
      (err, rows) => {
        if (err) {
          resolve([]);
          return;
        }
        resolve(Array.isArray(rows) ? rows : []);
      },
    );
  });

  const events = {};
  Object.keys(callSamples).forEach((provider) => {
    events[provider] = { timeout: 0, no_media: 0, stt_stall: 0 };
  });
  for (const row of flowRows) {
    const details = parseJson(row?.details);
    const provider = normalizeProvider(details?.provider);
    if (!provider) continue;
    if (!events[provider]) {
      events[provider] = { timeout: 0, no_media: 0, stt_stall: 0 };
    }
    const status = String(row?.status || "").trim().toLowerCase();
    if (status === "stream_timeout") {
      events[provider].timeout += 1;
    } else if (status === "stream_stalled_no_media") {
      events[provider].no_media += 1;
    } else if (status === "stream_stalled_stt") {
      events[provider].stt_stall += 1;
    }
  }

  const breaches = [];
  for (const provider of Object.keys(events)) {
    const sampleCount = Number(callSamples[provider] || 0);
    if (sampleCount < minSamples) continue;
    const timeoutCount = Number(events[provider].timeout || 0);
    const stallCount =
      Number(events[provider].no_media || 0) +
      Number(events[provider].stt_stall || 0);
    if (timeoutCount < maxTimeoutEvents && stallCount < maxStallEvents) {
      continue;
    }
    const breach = {
      provider,
      sample_count: sampleCount,
      timeout_events: timeoutCount,
      no_media_events: Number(events[provider].no_media || 0),
      stt_stall_events: Number(events[provider].stt_stall || 0),
      max_timeout_events: maxTimeoutEvents,
      max_stall_events: maxStallEvents,
      min_samples: minSamples,
      window_ms: windowMs,
    };
    breaches.push(breach);
    if (typeof onBreach === "function") {
      await Promise.resolve(onBreach(provider, breach));
    }
  }

  return {
    ok: true,
    skipped: false,
    window_ms: windowMs,
    providers: Object.keys(events).length,
    samples: callSamples,
    events,
    breaches,
  };
}

module.exports = {
  normalizeCanaryQualityConfig,
  evaluateCanaryConversationQuality,
  runCallCanarySweep,
  evaluateCallCanarySloGuardrail,
};
