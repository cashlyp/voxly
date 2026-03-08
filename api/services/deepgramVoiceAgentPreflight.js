"use strict";

const VOICE_AGENT_THINK_MODELS_ENDPOINT =
  "https://agent.deepgram.com/v1/agent/settings/think/models";
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

module.exports = {
  VOICE_AGENT_THINK_MODELS_ENDPOINT,
  shouldRunDeepgramVoiceAgentPreflight,
  normalizeThinkModelsResponse,
  evaluateThinkModelCompatibility,
  executeDeepgramVoiceAgentThinkPreflight,
};
