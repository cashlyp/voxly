"use strict";

const crypto = require("crypto");

const DEEPGRAM_MODELS_ENDPOINT = "https://api.deepgram.com/v1/models";
const LEGACY_TWILIO_VOICE_ALIASES = new Set(["alice", "man", "woman"]);

const CURATED_DEEPGRAM_VOICE_MODELS = Object.freeze([
  {
    id: "aura-2-andromeda-en",
    label: "Andromeda",
    gender: "female",
    style: "balanced",
    priority: 100,
  },
  {
    id: "aura-2-helena-en",
    label: "Helena",
    gender: "female",
    style: "warm",
    priority: 96,
  },
  {
    id: "aura-2-thalia-en",
    label: "Thalia",
    gender: "female",
    style: "clear",
    priority: 94,
  },
  {
    id: "aura-2-arcas-en",
    label: "Arcas",
    gender: "male",
    style: "grounded",
    priority: 92,
  },
  {
    id: "aura-2-aries-en",
    label: "Aries",
    gender: "male",
    style: "confident",
    priority: 90,
  },
  {
    id: "aura-asteria-en",
    label: "Asteria",
    gender: "female",
    style: "bright",
    priority: 80,
  },
]);

const DEFAULT_PROFILE_VOICE_POOLS = Object.freeze({
  general: ["aura-2-andromeda-en", "aura-2-helena-en", "aura-2-arcas-en"],
  sales: ["aura-2-aries-en", "aura-2-andromeda-en", "aura-2-helena-en"],
  support: ["aura-2-helena-en", "aura-2-arcas-en", "aura-2-thalia-en"],
  collections: ["aura-2-arcas-en", "aura-2-aries-en", "aura-2-thalia-en"],
  verification: ["aura-2-thalia-en", "aura-2-arcas-en", "aura-2-helena-en"],
  identity_verification: [
    "aura-2-thalia-en",
    "aura-2-arcas-en",
    "aura-2-helena-en",
  ],
  dating: ["aura-2-helena-en", "aura-2-arcas-en", "aura-2-andromeda-en"],
  fan: ["aura-2-helena-en", "aura-2-andromeda-en", "aura-2-arcas-en"],
  celebrity: ["aura-2-thalia-en", "aura-2-arcas-en", "aura-2-helena-en"],
  creator: ["aura-2-aries-en", "aura-2-andromeda-en", "aura-2-thalia-en"],
  friendship: ["aura-2-helena-en", "aura-2-andromeda-en", "aura-2-arcas-en"],
  networking: ["aura-2-arcas-en", "aura-2-aries-en", "aura-2-thalia-en"],
  community: ["aura-2-helena-en", "aura-2-andromeda-en", "aura-2-thalia-en"],
  marketplace_seller: [
    "aura-2-arcas-en",
    "aura-2-andromeda-en",
    "aura-2-aries-en",
  ],
  real_estate_agent: [
    "aura-2-arcas-en",
    "aura-2-andromeda-en",
    "aura-2-thalia-en",
  ],
});
const FEMALE_VOICE_MODEL_IDS = new Set(
  CURATED_DEEPGRAM_VOICE_MODELS.filter((entry) => entry.gender === "female").map(
    (entry) => entry.id,
  ),
);

function normalizeVoiceProfileKey(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^\w-]+/g, "_");
}

function normalizeVoiceModelId(value) {
  const normalized = String(value || "").trim();
  return normalized || "";
}

function isLegacyVoiceAlias(value) {
  const model = normalizeVoiceModelId(value);
  if (!model) return false;
  if (model.startsWith("Polly.")) return true;
  return LEGACY_TWILIO_VOICE_ALIASES.has(model.toLowerCase());
}

function uniq(values = []) {
  return Array.from(new Set(values.filter(Boolean)));
}

function parseVoiceModelPool(rawValue) {
  if (Array.isArray(rawValue)) {
    return uniq(rawValue.map(normalizeVoiceModelId).filter(Boolean));
  }
  const source = String(rawValue || "").trim();
  if (!source) return [];
  return uniq(
    source
      .split(/[|,;]/)
      .map((entry) => normalizeVoiceModelId(entry))
      .filter(Boolean),
  );
}

function computeStableVoicePoolIndex(seed, poolSize) {
  if (!Number.isFinite(poolSize) || poolSize <= 1) return 0;
  const digest = crypto
    .createHash("sha256")
    .update(String(seed || "voice-pool"))
    .digest();
  const hashValue = digest.readUInt32BE(0);
  return hashValue % poolSize;
}

function resolveMappedPool(profileKey, profileVoiceMap = {}) {
  if (!profileKey || !profileVoiceMap || typeof profileVoiceMap !== "object") {
    return [];
  }
  const exactPool = parseVoiceModelPool(profileVoiceMap[profileKey]);
  if (exactPool.length) return exactPool;
  return parseVoiceModelPool(profileVoiceMap.general);
}

function resolveDefaultProfilePool(profileKey, fallbackModel) {
  const fallback = normalizeVoiceModelId(fallbackModel) || "aura-2-andromeda-en";
  const key = normalizeVoiceProfileKey(profileKey) || "general";
  const basePool = Array.isArray(DEFAULT_PROFILE_VOICE_POOLS[key])
    ? DEFAULT_PROFILE_VOICE_POOLS[key]
    : DEFAULT_PROFILE_VOICE_POOLS.general;
  return uniq([...(basePool || []), fallback]);
}

function selectDeepgramVoiceModel(options = {}) {
  const explicitModel = normalizeVoiceModelId(options.candidateModel);
  if (explicitModel && !isLegacyVoiceAlias(explicitModel)) {
    return {
      model: explicitModel,
      source: "explicit",
      profileKey: normalizeVoiceProfileKey(options.conversationProfile) || "general",
      pool: [explicitModel],
    };
  }

  const profileKey =
    normalizeVoiceProfileKey(options.conversationProfile) || "general";
  const fallbackModel =
    normalizeVoiceModelId(options.fallbackSpeakModel) || "aura-2-andromeda-en";

  const mappedPool = resolveMappedPool(profileKey, options.profileVoiceMap);
  const pool = mappedPool.length
    ? mappedPool
    : resolveDefaultProfilePool(profileKey, fallbackModel);
  const preferDatingFemaleFirst =
    profileKey === "dating" &&
    mappedPool.length === 0 &&
    options.datingFemaleFirst !== false;
  if (preferDatingFemaleFirst) {
    const preferredFemale = pool.find((id) => FEMALE_VOICE_MODEL_IDS.has(id));
    if (preferredFemale) {
      return {
        model: preferredFemale,
        source: "dating_female_first",
        profileKey,
        pool,
        selectedIndex: pool.indexOf(preferredFemale),
      };
    }
  }

  const selectionSeed =
    normalizeVoiceModelId(options.seed) ||
    normalizeVoiceModelId(options.callSid) ||
    normalizeVoiceModelId(options.customerName) ||
    profileKey;
  const selectedIndex = computeStableVoicePoolIndex(selectionSeed, pool.length);
  const selectedModel = pool[selectedIndex] || fallbackModel;

  return {
    model: selectedModel,
    source: mappedPool.length ? "mapped_pool" : "default_pool",
    profileKey,
    pool,
    selectedIndex,
  };
}

function normalizeRemoteTtsEntry(entry) {
  if (!entry) return null;
  if (typeof entry === "string") {
    const id = normalizeVoiceModelId(entry);
    return id ? { id, label: id, description: "" } : null;
  }
  const id = normalizeVoiceModelId(
    entry.id || entry.model || entry.name || entry.slug,
  );
  if (!id) return null;
  return {
    id,
    label: String(entry.name || entry.label || id).trim() || id,
    description: String(entry.description || "").trim(),
  };
}

async function fetchDeepgramTtsModels(options = {}) {
  const fetchImpl = options.fetchImpl;
  const apiKey = normalizeVoiceModelId(options.apiKey);
  const timeoutMs = Math.max(
    1500,
    Number(options.timeoutMs || 3500),
  );
  if (!fetchImpl || typeof fetchImpl !== "function") {
    return { ok: false, error: "missing_fetch_impl", models: [] };
  }
  if (!apiKey) {
    return { ok: false, error: "missing_api_key", models: [] };
  }

  const controller =
    typeof AbortController !== "undefined" ? new AbortController() : null;
  const timeout = setTimeout(() => {
    if (controller) controller.abort();
  }, timeoutMs);

  try {
    const response = await fetchImpl(DEEPGRAM_MODELS_ENDPOINT, {
      method: "GET",
      headers: {
        Authorization: `Token ${apiKey}`,
      },
      signal: controller ? controller.signal : undefined,
    });
    if (!response?.ok) {
      return {
        ok: false,
        error: `http_${response?.status || "unknown"}`,
        models: [],
      };
    }
    const payload = await response.json().catch(() => null);
    const remoteEntries = Array.isArray(payload?.tts) ? payload.tts : [];
    const models = uniq(
      remoteEntries
        .map((entry) => normalizeRemoteTtsEntry(entry))
        .filter(Boolean)
        .map((entry) => entry.id),
    ).map((id) => {
      const match = remoteEntries
        .map((entry) => normalizeRemoteTtsEntry(entry))
        .find((entry) => entry?.id === id);
      return {
        id,
        label: match?.label || id,
        description: match?.description || "",
      };
    });
    return { ok: true, models };
  } catch (error) {
    return {
      ok: false,
      error: error?.name === "AbortError" ? "timeout" : "request_failed",
      details: error?.message || String(error || "unknown_error"),
      models: [],
    };
  } finally {
    clearTimeout(timeout);
  }
}

function buildDeepgramVoiceModelCatalog(options = {}) {
  const remoteModels = Array.isArray(options.remoteModels) ? options.remoteModels : [];
  const byId = new Map();

  CURATED_DEEPGRAM_VOICE_MODELS.forEach((entry) => {
    byId.set(entry.id, {
      id: entry.id,
      label: entry.label,
      description: "",
      gender: entry.gender || "unknown",
      style: entry.style || "balanced",
      source: "curated",
      priority: Number(entry.priority) || 0,
    });
  });

  remoteModels.forEach((entry) => {
    const normalized = normalizeRemoteTtsEntry(entry);
    if (!normalized) return;
    const existing = byId.get(normalized.id) || {};
    byId.set(normalized.id, {
      id: normalized.id,
      label: normalized.label || existing.label || normalized.id,
      description: normalized.description || existing.description || "",
      gender: existing.gender || "unknown",
      style: existing.style || "balanced",
      source: existing.id ? "curated+deepgram" : "deepgram",
      priority: Number(existing.priority) || 0,
    });
  });

  const models = Array.from(byId.values()).sort((a, b) => {
    const rankDelta = Number(b.priority || 0) - Number(a.priority || 0);
    if (rankDelta !== 0) return rankDelta;
    return a.id.localeCompare(b.id);
  });

  return {
    models,
    recommendedByFlow: DEFAULT_PROFILE_VOICE_POOLS,
  };
}

module.exports = {
  CURATED_DEEPGRAM_VOICE_MODELS,
  DEFAULT_PROFILE_VOICE_POOLS,
  normalizeVoiceProfileKey,
  normalizeVoiceModelId,
  parseVoiceModelPool,
  selectDeepgramVoiceModel,
  fetchDeepgramTtsModels,
  buildDeepgramVoiceModelCatalog,
};
