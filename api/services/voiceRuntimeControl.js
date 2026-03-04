"use strict";

const VOICE_RUNTIME_CONTROL_SETTING_KEY = "voice_runtime_controls_v1";
const CLEAR_TOKENS = new Set([
  "",
  "default",
  "inherit",
  "config",
  "none",
  "null",
  "clear",
]);
const VOICE_RUNTIME_MODES = new Set(["legacy", "hybrid", "voice_agent"]);

function clampCanaryPercent(value, fallback = 0) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(0, Math.min(100, Math.floor(parsed)));
}

function normalizeVoiceRuntimeMode(value) {
  const mode = String(value || "legacy")
    .trim()
    .toLowerCase();
  if (VOICE_RUNTIME_MODES.has(mode)) {
    return mode;
  }
  return "legacy";
}

function parseVoiceRuntimeModeOverride(value) {
  if (value === undefined) return undefined;
  const normalized = String(value ?? "")
    .trim()
    .toLowerCase();
  if (CLEAR_TOKENS.has(normalized)) return null;
  if (VOICE_RUNTIME_MODES.has(normalized)) return normalized;
  return undefined;
}

function parseVoiceRuntimeCanaryOverride(value) {
  if (value === undefined) return undefined;
  const normalized = String(value ?? "")
    .trim()
    .toLowerCase();
  if (CLEAR_TOKENS.has(normalized)) return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return undefined;
  return clampCanaryPercent(parsed, 0);
}

function parseEpochMs(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 0;
  return Math.max(0, Math.floor(parsed));
}

function parseBooleanFlag(value, fallback = false) {
  if (value === undefined || value === null || value === "") return fallback;
  if (value === true || value === 1) return true;
  const normalized = String(value).trim().toLowerCase();
  return ["1", "true", "yes", "on"].includes(normalized);
}

function normalizeVoiceAgentAutoCanaryConfig(raw = {}) {
  return {
    enabled: raw.enabled === true,
    intervalMs: Math.max(5000, Number(raw.intervalMs) || 60000),
    windowMs: Math.max(10000, Number(raw.windowMs) || 300000),
    cooldownMs: Math.max(10000, Number(raw.cooldownMs) || 180000),
    minSamples: Math.max(1, Number(raw.minSamples) || 8),
    minPercent: clampCanaryPercent(raw.minPercent, 5),
    maxPercent: clampCanaryPercent(raw.maxPercent, 50),
    stepUpPercent: Math.max(1, clampCanaryPercent(raw.stepUpPercent, 5)),
    stepDownPercent: Math.max(1, clampCanaryPercent(raw.stepDownPercent, 10)),
    maxErrorRate: Math.max(
      0,
      Math.min(1, Number.isFinite(Number(raw.maxErrorRate)) ? Number(raw.maxErrorRate) : 0.2),
    ),
    maxFallbackRate: Math.max(
      0,
      Math.min(
        1,
        Number.isFinite(Number(raw.maxFallbackRate)) ? Number(raw.maxFallbackRate) : 0.25,
      ),
    ),
    failClosedOnBreach: raw.failClosedOnBreach !== false,
  };
}

function sanitizePersistedVoiceRuntimeControls(raw = {}, nowMs = Date.now()) {
  const parsedMode = parseVoiceRuntimeModeOverride(
    raw.mode_override !== undefined ? raw.mode_override : raw.modeOverride,
  );
  const parsedCanary = parseVoiceRuntimeCanaryOverride(
    raw.canary_percent_override !== undefined
      ? raw.canary_percent_override
      : raw.canaryPercentOverride,
  );
  const parsedCanarySource = String(
    raw.canary_percent_override_source || raw.canaryPercentOverrideSource || "",
  )
    .trim()
    .toLowerCase();
  const canaryPercentOverrideSource = parsedCanarySource || null;
  const forcedLegacyRaw =
    raw.forced_legacy_until_ms !== undefined
      ? raw.forced_legacy_until_ms
      : raw.forcedLegacyUntilMs;
  const autoCooldownRaw =
    raw.auto_canary_cooldown_until_ms !== undefined
      ? raw.auto_canary_cooldown_until_ms
      : raw.autoCanaryCooldownUntilMs;
  const autoLastEvalRaw =
    raw.auto_canary_last_eval_at_ms !== undefined
      ? raw.auto_canary_last_eval_at_ms
      : raw.autoCanaryLastEvalAtMs;
  const updatedAtRaw = raw.updated_at || raw.updatedAt || null;
  const forcedLegacyUntilMs = parseEpochMs(forcedLegacyRaw);
  const autoCanaryCooldownUntilMs = parseEpochMs(autoCooldownRaw);
  const autoCanaryLastEvalAtMs = parseEpochMs(autoLastEvalRaw);

  return {
    modeOverride: parsedMode === undefined ? null : parsedMode,
    canaryPercentOverride: parsedCanary === undefined ? null : parsedCanary,
    canaryPercentOverrideSource:
      parsedCanary === undefined || parsedCanary === null
        ? null
        : canaryPercentOverrideSource,
    forcedLegacyUntilMs:
      forcedLegacyUntilMs > nowMs ? forcedLegacyUntilMs : 0,
    autoCanaryCooldownUntilMs:
      autoCanaryCooldownUntilMs > nowMs ? autoCanaryCooldownUntilMs : 0,
    autoCanaryLastEvalAtMs: autoCanaryLastEvalAtMs > 0 ? autoCanaryLastEvalAtMs : 0,
    updatedAt:
      updatedAtRaw && !Number.isNaN(new Date(updatedAtRaw).getTime())
        ? new Date(updatedAtRaw).toISOString()
        : null,
  };
}

function buildPersistedVoiceRuntimeControlsPayload(state = {}, nowMs = Date.now()) {
  const modeOverride =
    parseVoiceRuntimeModeOverride(state.modeOverride) === undefined
      ? null
      : parseVoiceRuntimeModeOverride(state.modeOverride);
  const canaryParsed = parseVoiceRuntimeCanaryOverride(state.canaryPercentOverride);
  const canaryPercentOverride =
    canaryParsed === undefined || canaryParsed === null ? null : canaryParsed;
  const source =
    canaryPercentOverride === null
      ? null
      : String(state.canaryPercentOverrideSource || "").trim().toLowerCase() || null;
  const forcedLegacyUntilMs = parseEpochMs(state.forcedLegacyUntilMs);
  const autoCanaryCooldownUntilMs = parseEpochMs(state.autoCanaryCooldownUntilMs);
  const autoCanaryLastEvalAtMs = parseEpochMs(state.autoCanaryLastEvalAtMs);
  return {
    version: 1,
    updated_at: new Date(nowMs).toISOString(),
    mode_override: modeOverride,
    canary_percent_override: canaryPercentOverride,
    canary_percent_override_source: source,
    forced_legacy_until_ms:
      forcedLegacyUntilMs > nowMs ? forcedLegacyUntilMs : 0,
    auto_canary_cooldown_until_ms:
      autoCanaryCooldownUntilMs > nowMs ? autoCanaryCooldownUntilMs : 0,
    auto_canary_last_eval_at_ms: autoCanaryLastEvalAtMs > 0 ? autoCanaryLastEvalAtMs : 0,
  };
}

function shouldFallbackVoiceAgentOnDtmf(runtimeMode) {
  return normalizeVoiceRuntimeMode(runtimeMode) === "voice_agent";
}

function pruneVoiceAgentAutoCanaryEvents(
  events = [],
  windowMs = 300000,
  nowMs = Date.now(),
  maxEvents = 2000,
) {
  if (!Array.isArray(events) || events.length === 0) return 0;
  const keepAfter = nowMs - Math.max(1000, Number(windowMs) || 300000);
  let writeIndex = 0;
  for (let index = 0; index < events.length; index += 1) {
    const item = events[index];
    const at = Number(item?.at || 0);
    if (!Number.isFinite(at) || at < keepAfter) continue;
    events[writeIndex] = item;
    writeIndex += 1;
  }
  events.length = writeIndex;
  if (events.length > maxEvents) {
    events.splice(0, events.length - maxEvents);
  }
  return events.length;
}

function summarizeVoiceAgentAutoCanaryEvents(
  events = [],
  windowMs = 300000,
  nowMs = Date.now(),
  lastEvalAtMs = 0,
) {
  const keepAfter = nowMs - Math.max(1000, Number(windowMs) || 300000);
  let selected = 0;
  let selectedSinceLastEval = 0;
  let runtimeErrors = 0;
  let setupFailures = 0;
  let fallbackRuntime = 0;
  let fallbackNoAudio = 0;
  let fallbackDtmf = 0;
  let fallbackOther = 0;

  for (const item of events) {
    const at = Number(item?.at || 0);
    if (!Number.isFinite(at) || at < keepAfter) continue;
    const kind = String(item?.kind || "").trim().toLowerCase();
    if (kind === "selected") {
      selected += 1;
      if (at > Number(lastEvalAtMs || 0)) {
        selectedSinceLastEval += 1;
      }
      continue;
    }
    if (kind === "runtime_error") {
      runtimeErrors += 1;
      continue;
    }
    if (kind === "settings_failure") {
      setupFailures += 1;
      continue;
    }
    if (kind === "fallback_runtime") {
      fallbackRuntime += 1;
      continue;
    }
    if (kind === "fallback_no_audio") {
      fallbackNoAudio += 1;
      continue;
    }
    if (kind === "fallback_dtmf") {
      fallbackDtmf += 1;
      continue;
    }
    if (kind.startsWith("fallback")) {
      fallbackOther += 1;
    }
  }

  const totalFailuresForErrorRate = runtimeErrors + setupFailures;
  const totalFallbackForSlo = fallbackRuntime + fallbackNoAudio + fallbackOther;
  const errorRate = selected > 0 ? totalFailuresForErrorRate / selected : 0;
  const fallbackRate = selected > 0 ? totalFallbackForSlo / selected : 0;
  return {
    selected,
    selectedSinceLastEval,
    runtimeErrors,
    setupFailures,
    fallbackRuntime,
    fallbackNoAudio,
    fallbackDtmf,
    fallbackOther,
    totalFailuresForErrorRate,
    totalFallbackForSlo,
    errorRate,
    fallbackRate,
  };
}

function evaluateVoiceAgentAutoCanaryDecision(input = {}) {
  const cfg = normalizeVoiceAgentAutoCanaryConfig(input.config || {});
  const nowMs = Number(input.nowMs) || Date.now();
  const mode = normalizeVoiceRuntimeMode(input.mode);
  const manualCanaryOverride = input.manualCanaryOverride === true;
  const currentCanaryPercent = clampCanaryPercent(input.currentCanaryPercent, 0);
  const configuredCanaryPercent = clampCanaryPercent(input.configuredCanaryPercent, 0);
  const targetCanaryPercent = Math.min(
    cfg.maxPercent,
    Math.max(0, configuredCanaryPercent),
  );
  const summary = input.summary && typeof input.summary === "object" ? input.summary : {};
  const selected = Math.max(0, Number(summary.selected) || 0);
  const selectedSinceLastEval = Math.max(
    0,
    Number(summary.selectedSinceLastEval) || 0,
  );
  const errorRate = Number.isFinite(Number(summary.errorRate))
    ? Number(summary.errorRate)
    : 0;
  const fallbackRate = Number.isFinite(Number(summary.fallbackRate))
    ? Number(summary.fallbackRate)
    : 0;
  const circuitOpen = input.circuitOpen === true;
  const cooldownUntilMs = parseEpochMs(input.cooldownUntilMs);
  const base = {
    enabled: cfg.enabled,
    mode,
    manualCanaryOverride,
    currentCanaryPercent,
    configuredCanaryPercent,
    targetCanaryPercent,
    cooldownUntilMs,
    selected,
    selectedSinceLastEval,
    errorRate,
    fallbackRate,
    circuitOpen,
  };

  if (!cfg.enabled) return { action: "noop", reason: "disabled", ...base };
  if (mode !== "hybrid") return { action: "noop", reason: "mode_not_hybrid", ...base };
  if (manualCanaryOverride) {
    return { action: "noop", reason: "manual_override_active", ...base };
  }
  if (targetCanaryPercent <= 0) {
    return { action: "noop", reason: "target_zero", ...base };
  }

  if (currentCanaryPercent <= 0 && cooldownUntilMs <= nowMs) {
    const bootstrapCanary = Math.min(
      targetCanaryPercent,
      Math.max(1, cfg.minPercent),
    );
    if (bootstrapCanary > 0) {
      return {
        action: "set_canary",
        reason: "bootstrap",
        nextCanaryPercent: bootstrapCanary,
        nextCooldownUntilMs: cooldownUntilMs,
        ...base,
      };
    }
  }

  const breachedBySlo =
    selected >= cfg.minSamples &&
    (errorRate > cfg.maxErrorRate || fallbackRate > cfg.maxFallbackRate);
  if (circuitOpen || breachedBySlo) {
    const nextCanaryPercent = cfg.failClosedOnBreach
      ? 0
      : Math.max(0, currentCanaryPercent - cfg.stepDownPercent);
    const nextCooldownUntilMs = nowMs + cfg.cooldownMs;
    if (
      nextCanaryPercent !== currentCanaryPercent ||
      nextCooldownUntilMs > cooldownUntilMs
    ) {
      return {
        action: "set_canary",
        reason: circuitOpen ? "circuit_open" : "slo_breach",
        nextCanaryPercent,
        nextCooldownUntilMs,
        ...base,
      };
    }
    return {
      action: "noop",
      reason: "breach_already_applied",
      ...base,
    };
  }

  if (cooldownUntilMs > nowMs) {
    return { action: "noop", reason: "cooldown_active", ...base };
  }
  if (selected < cfg.minSamples) {
    return { action: "noop", reason: "insufficient_samples", ...base };
  }
  if (selectedSinceLastEval <= 0) {
    return { action: "noop", reason: "no_new_samples", ...base };
  }
  if (currentCanaryPercent >= targetCanaryPercent) {
    return { action: "noop", reason: "at_or_above_target", ...base };
  }

  const nextCanaryPercent = Math.min(
    targetCanaryPercent,
    currentCanaryPercent + cfg.stepUpPercent,
  );
  return {
    action: "set_canary",
    reason: "healthy_step_up",
    nextCanaryPercent,
    nextCooldownUntilMs: cooldownUntilMs,
    ...base,
  };
}

function applyVoiceRuntimeControlMutation(input = {}) {
  const body = input.body && typeof input.body === "object" ? input.body : {};
  const nowMs = Number(input.nowMs) || Date.now();
  const circuitCooldownMs = Math.max(
    5000,
    Number(input.circuitCooldownMs) || 180000,
  );
  const stateInput = input.state && typeof input.state === "object" ? input.state : {};
  const state = {
    modeOverride: stateInput.modeOverride ?? null,
    canaryPercentOverride:
      stateInput.canaryPercentOverride ?? null,
    canaryPercentOverrideSource:
      stateInput.canaryPercentOverrideSource || null,
    forcedLegacyUntilMs: parseEpochMs(stateInput.forcedLegacyUntilMs),
    autoCanaryCooldownUntilMs: parseEpochMs(stateInput.autoCanaryCooldownUntilMs),
    autoCanaryLastEvalAtMs: parseEpochMs(stateInput.autoCanaryLastEvalAtMs),
    runtimeOverrideUpdatedAtMs: parseEpochMs(stateInput.runtimeOverrideUpdatedAtMs),
  };
  const actions = [];
  const applied = {};
  let overrideChanged = false;

  const modeValue =
    body.mode !== undefined
      ? body.mode
      : body.mode_override !== undefined
        ? body.mode_override
        : body.runtime_mode;
  if (modeValue !== undefined) {
    const nextMode = parseVoiceRuntimeModeOverride(modeValue);
    if (nextMode === undefined) {
      return {
        ok: false,
        error:
          "Invalid mode override. Use legacy, hybrid, voice_agent, or clear/default.",
      };
    }
    state.modeOverride = nextMode;
    overrideChanged = true;
    actions.push("mode_override_updated");
    applied.mode_override = state.modeOverride;
  }

  const canaryValue =
    body.canary_percent !== undefined
      ? body.canary_percent
      : body.canaryPercent !== undefined
        ? body.canaryPercent
        : body.canary_percent_override;
  if (canaryValue !== undefined) {
    const nextCanary = parseVoiceRuntimeCanaryOverride(canaryValue);
    if (nextCanary === undefined) {
      return {
        ok: false,
        error:
          "Invalid canary percent override. Use a number between 0 and 100, or clear/default.",
      };
    }
    state.canaryPercentOverride = nextCanary;
    state.canaryPercentOverrideSource = nextCanary === null ? null : "manual";
    overrideChanged = true;
    actions.push("canary_override_updated");
    applied.canary_percent_override = state.canaryPercentOverride;
    applied.canary_percent_override_source = state.canaryPercentOverrideSource;
  }

  const forceLegacyValue =
    body.force_legacy_for_ms !== undefined
      ? body.force_legacy_for_ms
      : body.forceLegacyForMs;
  if (forceLegacyValue !== undefined) {
    const parsed = Number(forceLegacyValue);
    if (!Number.isFinite(parsed) || parsed < 0) {
      return {
        ok: false,
        error: "force_legacy_for_ms must be a non-negative number",
      };
    }
    const boundedMs = Math.min(Math.floor(parsed), 24 * 60 * 60 * 1000);
    state.forcedLegacyUntilMs = boundedMs > 0 ? nowMs + boundedMs : 0;
    actions.push("force_legacy_window_updated");
    applied.force_legacy_for_ms = boundedMs;
  }

  const openCircuitRequested =
    parseBooleanFlag(body.open_circuit, false) ||
    parseBooleanFlag(body.openCircuit, false);
  if (openCircuitRequested) {
    const forcedUntil = nowMs + circuitCooldownMs;
    if (forcedUntil > state.forcedLegacyUntilMs) {
      state.forcedLegacyUntilMs = forcedUntil;
    }
    actions.push("circuit_opened");
    applied.circuit_opened_for_ms = circuitCooldownMs;
  }

  const resetCircuitRequested =
    parseBooleanFlag(body.reset_circuit, false) ||
    parseBooleanFlag(body.resetCircuit, false);
  if (resetCircuitRequested) {
    state.forcedLegacyUntilMs = 0;
    state.autoCanaryCooldownUntilMs = 0;
    state.autoCanaryLastEvalAtMs = 0;
    actions.push("circuit_reset");
    applied.circuit_reset = true;
  }

  if (overrideChanged) {
    state.runtimeOverrideUpdatedAtMs = nowMs;
  }

  return {
    ok: true,
    state,
    actions,
    applied,
    overrideChanged,
    resetCircuitRequested,
  };
}

module.exports = {
  VOICE_RUNTIME_CONTROL_SETTING_KEY,
  clampCanaryPercent,
  normalizeVoiceRuntimeMode,
  parseVoiceRuntimeModeOverride,
  parseVoiceRuntimeCanaryOverride,
  normalizeVoiceAgentAutoCanaryConfig,
  sanitizePersistedVoiceRuntimeControls,
  buildPersistedVoiceRuntimeControlsPayload,
  shouldFallbackVoiceAgentOnDtmf,
  pruneVoiceAgentAutoCanaryEvents,
  summarizeVoiceAgentAutoCanaryEvents,
  evaluateVoiceAgentAutoCanaryDecision,
  applyVoiceRuntimeControlMutation,
};
