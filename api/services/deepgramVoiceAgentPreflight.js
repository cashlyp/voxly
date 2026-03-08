"use strict";

const VOICE_AGENT_THINK_MODELS_ENDPOINT =
  "https://agent.deepgram.com/v1/agent/settings/think/models";
const VOICE_AGENT_SPEAK_PROBE_ENDPOINT = "https://api.deepgram.com/v1/speak";
const ACTIVE_VOICE_AGENT_MODES = new Set(["hybrid", "voice_agent"]);
const THINK_PROVIDER_ALIASES = Object.freeze({
  openai: "open_ai",
  "open-ai": "open_ai",
  "open ai": "open_ai",
  open_ai: "open_ai",
});

function normalizeText(value, fallback = "") {
  if (value === undefined || value === null) return fallback;
  const text = String(value).trim();
  return text || fallback;
}

function normalizeMode(mode) {
  const normalized = normalizeText(mode, "legacy").toLowerCase();
  return ACTIVE_VOICE_AGENT_MODES.has(normalized) ? normalized : "legacy";
}

function normalizeThinkProvider(provider, fallback = "open_ai") {
  const normalized = normalizeText(provider, fallback).toLowerCase();
  return THINK_PROVIDER_ALIASES[normalized] || normalized;
}

function shouldRunDeepgramVoiceAgentPreflight(options = {}) {
  const enabled = options.enabled === true;
  if (!enabled) return false;
  return normalizeMode(options.mode) !== "legacy";
}

function createPreflightError(code, message, extra = {}) {
  const error = new Error(message);
  error.code = code;
  if (extra && typeof extra === "object") {
    Object.assign(error, extra);
  }
  return error;
}

function normalizeThinkModelsResponse(payload) {
  const rows = Array.isArray(payload?.models)
    ? payload.models
    : Array.isArray(payload?.result?.models)
      ? payload.result.models
      : Array.isArray(payload)
        ? payload
        : [];
  return rows
    .map((entry) => {
      const providerSource =
        typeof entry?.provider === "string"
          ? entry.provider
          : entry?.provider?.type || entry?.provider?.id;
      const provider = normalizeThinkProvider(providerSource, "");
      const id = normalizeText(entry?.id || entry?.model || entry?.name);
      if (!provider || !id) return null;
      return {
        provider,
        id,
        name: normalizeText(entry?.name),
      };
    })
    .filter(Boolean);
}

function normalizeModelIdentifier(modelId = "") {
  return normalizeText(modelId).toLowerCase();
}

function modelMatchesTarget(modelId = "", targetModel = "") {
  const normalizedCandidate = normalizeModelIdentifier(modelId);
  const normalizedTarget = normalizeModelIdentifier(targetModel);
  if (!normalizedCandidate || !normalizedTarget) return false;
  if (normalizedCandidate === normalizedTarget) return true;
  return (
    normalizedCandidate.endsWith(`/${normalizedTarget}`) ||
    normalizedCandidate.endsWith(`:${normalizedTarget}`)
  );
}

function evaluateThinkModelCompatibility(models = [], options = {}) {
  const provider = normalizeThinkProvider(options.provider, "open_ai");
  const model = normalizeText(options.model, "gpt-4o-mini");
  const providerModels = models
    .filter((entry) => entry.provider === provider)
    .map((entry) => entry.id);
  const isSupported = providerModels.some((candidate) =>
    modelMatchesTarget(candidate, model),
  );

  return {
    provider,
    model,
    isSupported,
    providerModelCount: providerModels.length,
    providerModelsSample: providerModels.slice(0, 15),
  };
}

function normalizeVoiceAgentAudioConfig(options = {}) {
  const inputEncoding = normalizeText(options.inputEncoding, "mulaw").toLowerCase();
  const inputSampleRate = Math.max(8000, Number(options.inputSampleRate) || 8000);
  const outputEncoding = normalizeText(options.outputEncoding, "mulaw").toLowerCase();
  const outputSampleRate = Math.max(8000, Number(options.outputSampleRate) || 8000);
  const outputContainer = normalizeText(options.outputContainer, "none").toLowerCase();
  return {
    inputEncoding,
    inputSampleRate,
    outputEncoding,
    outputSampleRate,
    outputContainer,
  };
}

function validateVoiceAgentConfig(options = {}) {
  const listenModel = normalizeText(options.listenModel, "nova-2");
  const speakModel = normalizeText(options.speakModel, "aura-2-andromeda-en");
  if (!listenModel) {
    throw createPreflightError(
      "voice_agent_preflight_missing_listen_model",
      "Voice Agent listen model is required for startup preflight",
    );
  }
  if (!speakModel) {
    throw createPreflightError(
      "voice_agent_preflight_missing_speak_model",
      "Voice Agent speak model is required for startup preflight",
    );
  }

  const audio = normalizeVoiceAgentAudioConfig(options);
  const telephonyProfile =
    audio.inputEncoding === "mulaw" &&
    audio.outputEncoding === "mulaw" &&
    audio.inputSampleRate === 8000 &&
    audio.outputSampleRate === 8000;
  const warnings = [];
  if (!telephonyProfile) {
    warnings.push("non_mulaw_8k_audio_profile");
  }

  const syntheticTurnText = normalizeText(
    options.syntheticTurnText,
    "Hello, this is a quick voice quality probe.",
  );
  if (!syntheticTurnText || syntheticTurnText.length < 8) {
    throw createPreflightError(
      "voice_agent_preflight_invalid_synthetic_turn",
      "Voice Agent synthetic turn text is too short",
      {
        minLength: 8,
      },
    );
  }
  if (syntheticTurnText.length > 260) {
    throw createPreflightError(
      "voice_agent_preflight_invalid_synthetic_turn",
      "Voice Agent synthetic turn text is too long",
      {
        maxLength: 260,
      },
    );
  }

  return {
    listenModel,
    speakModel,
    audio,
    telephonyProfile,
    warnings,
    syntheticTurnText,
  };
}

function buildSpeakProbeUrl(options = {}) {
  const endpoint = normalizeText(
    options.endpoint,
    VOICE_AGENT_SPEAK_PROBE_ENDPOINT,
  );
  const speakModel = normalizeText(options.speakModel, "aura-2-andromeda-en");
  const audio = normalizeVoiceAgentAudioConfig(options);
  const params = new URLSearchParams();
  params.set("model", speakModel);
  params.set("encoding", audio.outputEncoding);
  params.set("sample_rate", String(audio.outputSampleRate));
  params.set("container", audio.outputContainer || "none");
  return `${endpoint}?${params.toString()}`;
}

async function executeDeepgramVoiceAgentSpeakProbe(options = {}) {
  const {
    fetchImpl,
    apiKey,
    speakModel,
    outputEncoding = "mulaw",
    outputSampleRate = 8000,
    outputContainer = "none",
    syntheticTurnText = "Hello, this is a quick voice quality probe.",
    timeoutMs = 5000,
  } = options;

  if (typeof fetchImpl !== "function") {
    throw createPreflightError(
      "voice_agent_preflight_fetch_missing",
      "Voice Agent preflight fetch implementation is unavailable",
    );
  }
  const token = normalizeText(apiKey);
  if (!token) {
    throw createPreflightError(
      "voice_agent_preflight_missing_api_key",
      "Deepgram API key is required for Voice Agent preflight",
    );
  }

  const safeTimeoutMs = Math.max(1500, Number(timeoutMs) || 5000);
  const controller =
    typeof AbortController !== "undefined" ? new AbortController() : null;
  const timeoutHandle = setTimeout(() => {
    if (controller) {
      try {
        controller.abort();
      } catch {}
    }
  }, safeTimeoutMs);
  if (typeof timeoutHandle.unref === "function") {
    timeoutHandle.unref();
  }

  const probeUrl = buildSpeakProbeUrl({
    speakModel,
    outputEncoding,
    outputSampleRate,
    outputContainer,
    endpoint: options.endpoint,
  });

  let response = null;
  try {
    response = await fetchImpl(probeUrl, {
      method: "POST",
      headers: {
        Authorization: `Token ${token}`,
        Accept: "audio/*",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        text: normalizeText(syntheticTurnText, "Hello from voice agent startup."),
      }),
      ...(controller ? { signal: controller.signal } : {}),
    });
  } catch (error) {
    const isAbort =
      error?.name === "AbortError" ||
      error?.type === "aborted" ||
      error?.code === "ABORT_ERR";
    if (isAbort) {
      throw createPreflightError(
        "voice_agent_preflight_speak_probe_timeout",
        `Voice Agent speak probe timed out after ${safeTimeoutMs}ms`,
        { timeoutMs: safeTimeoutMs },
      );
    }
    throw createPreflightError(
      "voice_agent_preflight_speak_probe_network_error",
      `Voice Agent speak probe request failed: ${error?.message || "network_error"}`,
      { cause: error?.message || null },
    );
  } finally {
    clearTimeout(timeoutHandle);
  }

  if (!response.ok) {
    throw createPreflightError(
      "voice_agent_preflight_speak_probe_http_error",
      `Voice Agent speak probe failed (${response.status} ${response.statusText || "error"})`,
      {
        httpStatus: Number(response.status) || null,
      },
    );
  }

  let bytes = 0;
  try {
    const body = await response.arrayBuffer();
    bytes = Buffer.from(body || new ArrayBuffer(0)).length;
  } catch {
    bytes = 0;
  }
  if (bytes <= 0) {
    throw createPreflightError(
      "voice_agent_preflight_speak_probe_empty_audio",
      "Voice Agent speak probe returned empty audio",
    );
  }

  return {
    ok: true,
    model: normalizeText(speakModel, "aura-2-andromeda-en"),
    bytes,
    outputEncoding: normalizeText(outputEncoding, "mulaw").toLowerCase(),
    outputSampleRate: Math.max(8000, Number(outputSampleRate) || 8000),
    outputContainer: normalizeText(outputContainer, "none").toLowerCase(),
  };
}

async function executeDeepgramVoiceAgentThinkPreflight(options = {}) {
  const {
    fetchImpl,
    apiKey,
    enabled = false,
    mode = "legacy",
    thinkProvider = "open_ai",
    thinkModel = "gpt-4o-mini",
    timeoutMs = 8000,
    endpoint = VOICE_AGENT_THINK_MODELS_ENDPOINT,
  } = options;

  const normalizedMode = normalizeMode(mode);
  if (!shouldRunDeepgramVoiceAgentPreflight({ enabled, mode: normalizedMode })) {
    return {
      ok: true,
      skipped: true,
      reason: "voice_agent_runtime_not_active",
      mode: normalizedMode,
      enabled: enabled === true,
    };
  }

  if (typeof fetchImpl !== "function") {
    throw createPreflightError(
      "voice_agent_preflight_fetch_missing",
      "Voice Agent preflight fetch implementation is unavailable",
    );
  }

  const token = normalizeText(apiKey);
  if (!token) {
    throw createPreflightError(
      "voice_agent_preflight_missing_api_key",
      "Deepgram API key is required for Voice Agent preflight",
    );
  }

  const safeTimeoutMs = Math.max(2000, Number(timeoutMs) || 8000);
  const controller =
    typeof AbortController !== "undefined" ? new AbortController() : null;
  const timeoutHandle = setTimeout(() => {
    if (controller) {
      try {
        controller.abort();
      } catch {}
    }
  }, safeTimeoutMs);
  if (typeof timeoutHandle.unref === "function") {
    timeoutHandle.unref();
  }

  let response = null;
  try {
    response = await fetchImpl(endpoint, {
      method: "GET",
      headers: {
        Authorization: `Token ${token}`,
        Accept: "application/json",
      },
      ...(controller ? { signal: controller.signal } : {}),
    });
  } catch (error) {
    const isAbort =
      error?.name === "AbortError" ||
      error?.type === "aborted" ||
      error?.code === "ABORT_ERR";
    if (isAbort) {
      throw createPreflightError(
        "voice_agent_preflight_timeout",
        `Voice Agent preflight timed out after ${safeTimeoutMs}ms`,
        { timeoutMs: safeTimeoutMs },
      );
    }
    throw createPreflightError(
      "voice_agent_preflight_network_error",
      `Voice Agent preflight request failed: ${error?.message || "network_error"}`,
      { cause: error?.message || null },
    );
  } finally {
    clearTimeout(timeoutHandle);
  }

  let payload = null;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }

  if (!response.ok) {
    throw createPreflightError(
      "voice_agent_preflight_http_error",
      `Voice Agent preflight request failed (${response.status} ${response.statusText || "error"})`,
      {
        httpStatus: Number(response.status) || null,
      },
    );
  }

  const models = normalizeThinkModelsResponse(payload);
  if (!models.length) {
    throw createPreflightError(
      "voice_agent_preflight_empty_model_catalog",
      "Voice Agent preflight returned an empty think model catalog",
    );
  }

  const compatibility = evaluateThinkModelCompatibility(models, {
    provider: thinkProvider,
    model: thinkModel,
  });

  if (!compatibility.isSupported) {
    throw createPreflightError(
      "voice_agent_preflight_model_unsupported",
      `Voice Agent think model "${compatibility.model}" is not available for provider "${compatibility.provider}"`,
      {
        provider: compatibility.provider,
        model: compatibility.model,
        providerModelCount: compatibility.providerModelCount,
        providerModelsSample: compatibility.providerModelsSample,
      },
    );
  }

  return {
    ok: true,
    skipped: false,
    mode: normalizedMode,
    provider: compatibility.provider,
    model: compatibility.model,
    providerModelCount: compatibility.providerModelCount,
    providerModelsSample: compatibility.providerModelsSample,
    catalogSize: models.length,
  };
}

async function executeDeepgramVoiceAgentRuntimePreflight(options = {}) {
  const thinkResult = await executeDeepgramVoiceAgentThinkPreflight(options);
  if (thinkResult.skipped) {
    return {
      ...thinkResult,
      configCheck: {
        skipped: true,
      },
      speakProbe: {
        skipped: true,
      },
      syntheticTurn: {
        skipped: true,
      },
    };
  }

  const configCheck = validateVoiceAgentConfig(options);
  const syntheticTurn = {
    ok: true,
    textLength: configCheck.syntheticTurnText.length,
  };

  let speakProbe = {
    ok: true,
    skipped: true,
    reason: "tts_probe_disabled",
  };
  if (options.ttsProbeEnabled !== false) {
    speakProbe = await executeDeepgramVoiceAgentSpeakProbe({
      fetchImpl: options.fetchImpl,
      apiKey: options.apiKey,
      speakModel: configCheck.speakModel,
      outputEncoding: configCheck.audio.outputEncoding,
      outputSampleRate: configCheck.audio.outputSampleRate,
      outputContainer: configCheck.audio.outputContainer,
      syntheticTurnText: configCheck.syntheticTurnText,
      timeoutMs: options.ttsProbeTimeoutMs || 5000,
      endpoint: options.speakProbeEndpoint,
    });
  }

  return {
    ...thinkResult,
    listenModel: configCheck.listenModel,
    speakModel: configCheck.speakModel,
    audio: configCheck.audio,
    configWarnings: configCheck.warnings,
    syntheticTurn,
    speakProbe,
  };
}

module.exports = {
  VOICE_AGENT_THINK_MODELS_ENDPOINT,
  VOICE_AGENT_SPEAK_PROBE_ENDPOINT,
  shouldRunDeepgramVoiceAgentPreflight,
  normalizeThinkModelsResponse,
  evaluateThinkModelCompatibility,
  executeDeepgramVoiceAgentSpeakProbe,
  executeDeepgramVoiceAgentThinkPreflight,
  executeDeepgramVoiceAgentRuntimePreflight,
};
