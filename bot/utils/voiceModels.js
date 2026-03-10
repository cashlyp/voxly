"use strict";

const { InlineKeyboard } = require("grammy");
const config = require("../config");
const httpClient = require("./httpClient");
const { upsertMenuMessage, dismissMenuMessage, escapeMarkdown } = require("./ui");
const {
  buildCallbackData,
  matchesCallbackPrefix,
  parseCallbackData,
} = require("./actions");

const scriptsApiBase = config.scriptsApiUrl.replace(/\/+$/, "");

const FALLBACK_VOICE_MODELS = Object.freeze([
  { id: "aura-2-andromeda-en", gender: "female", style: "balanced" },
  { id: "aura-2-helena-en", gender: "female", style: "warm" },
  { id: "aura-2-thalia-en", gender: "female", style: "clear" },
  { id: "aura-2-arcas-en", gender: "male", style: "grounded" },
  { id: "aura-2-aries-en", gender: "male", style: "confident" },
  { id: "aura-asteria-en", gender: "female", style: "bright" },
]);

const VOICE_MODEL_CACHE_TTL_MS = 60 * 1000;
const catalogCache = {
  expiresAt: 0,
  payload: null,
};

function normalizeVoiceModelEntry(entry) {
  if (!entry || typeof entry !== "object") return null;
  const id = String(entry.id || entry.model || "").trim();
  if (!id) return null;
  return {
    id,
    label: String(entry.label || entry.name || id).trim() || id,
    description: String(entry.description || "").trim(),
    gender: String(entry.gender || "unknown").trim().toLowerCase(),
    style: String(entry.style || "balanced").trim().toLowerCase(),
  };
}

function buildFallbackCatalog() {
  return {
    success: true,
    source: "bot_fallback",
    error: null,
    models: FALLBACK_VOICE_MODELS.map((entry) => ({
      id: entry.id,
      label: entry.id,
      description: "",
      gender: entry.gender,
      style: entry.style,
    })),
    recommended_by_flow: {},
    defaults: {
      runtime_voice_model: config.defaultVoiceModel,
    },
  };
}

function normalizeCatalogPayload(payload) {
  if (!payload || typeof payload !== "object") {
    return buildFallbackCatalog();
  }
  const normalizedModels = Array.isArray(payload.models)
    ? payload.models.map(normalizeVoiceModelEntry).filter(Boolean)
    : [];
  if (!normalizedModels.length) {
    return buildFallbackCatalog();
  }
  return {
    success: payload.success !== false,
    source: String(payload.source || "api").trim() || "api",
    error: payload.error || null,
    models: normalizedModels,
    recommended_by_flow:
      payload.recommended_by_flow && typeof payload.recommended_by_flow === "object"
        ? payload.recommended_by_flow
        : {},
    defaults:
      payload.defaults && typeof payload.defaults === "object"
        ? payload.defaults
        : { runtime_voice_model: config.defaultVoiceModel },
  };
}

async function fetchVoiceModelCatalog(ctx, options = {}) {
  const forceRefresh = options.forceRefresh === true;
  const now = Date.now();
  if (!forceRefresh && catalogCache.payload && catalogCache.expiresAt > now) {
    return catalogCache.payload;
  }

  try {
    const response = await httpClient.get(ctx, `${scriptsApiBase}/admin/voice-models`, {
      timeout: 10000,
      headers: {
        "x-admin-token": config.admin.apiToken,
      },
      params: forceRefresh ? { refresh: 1 } : undefined,
    });
    const payload = normalizeCatalogPayload(response?.data || {});
    catalogCache.payload = payload;
    catalogCache.expiresAt = now + VOICE_MODEL_CACHE_TTL_MS;
    return payload;
  } catch (error) {
    console.warn("Failed to fetch voice model catalog:", error?.message || error);
    const fallback = buildFallbackCatalog();
    catalogCache.payload = fallback;
    catalogCache.expiresAt = now + 20 * 1000;
    return fallback;
  }
}

function formatVoiceLabel(entry) {
  const gender = String(entry.gender || "").toLowerCase();
  const icon = gender === "male" ? "♂️" : gender === "female" ? "♀️" : "🎙️";
  return `${icon} ${entry.id}`;
}

function formatVoiceCatalogLines(models = []) {
  return models.map((entry) => {
    const parts = [entry.id];
    if (entry.gender && entry.gender !== "unknown") {
      parts.push(entry.gender);
    }
    if (entry.style && entry.style !== "balanced") {
      parts.push(entry.style);
    }
    return `• ${parts.join(" · ")}`;
  });
}

async function askVoiceModelWithPagination(
  conversation,
  ctx,
  {
    prompt = "🎙️ Select voice model",
    models = [],
    topOptions = [],
    bottomOptions = [],
    prefix = "voice-model",
    pageSize = 8,
    ensureActive,
  } = {},
) {
  const safeModels = Array.isArray(models)
    ? models.filter((entry) => entry && entry.id)
    : [];
  const normalizedPageSize = Math.max(1, Math.floor(Number(pageSize) || 8));
  const safeEnsureActive =
    typeof ensureActive === "function" ? ensureActive : () => {};
  const opToken = String(ctx.session?.currentOp?.token || "").trim();
  let page = 0;
  let activeFilter = "";
  let menuMessage = null;

  const buildKeyboard = ({
    top = [],
    pageModels = [],
    bottom = [],
    hasPrev = false,
    hasNext = false,
    hasFilter = false,
  }) => {
    const keyboard = new InlineKeyboard();
    const addRow = (items = []) => {
      const valid = items.filter(Boolean);
      if (!valid.length) return;
      valid.forEach((item, index) => {
        keyboard.text(
          item.label,
          buildCallbackData(ctx, `${prefix}:${item.id}`),
        );
        if (index < valid.length - 1) {
          // keep on same row
        }
      });
      keyboard.row();
    };

    for (let index = 0; index < top.length; index += 2) {
      addRow(top.slice(index, index + 2));
    }

    pageModels.forEach((entry) => {
      addRow([{ id: `model:${entry.id}`, label: formatVoiceLabel(entry) }]);
    });

    addRow([
      hasPrev ? { id: "__nav_prev__", label: "⬅️ Previous" } : null,
      { id: "__search__", label: "🔎 Search" },
      hasNext ? { id: "__nav_next__", label: "Next ➡️" } : null,
    ]);

    if (hasFilter) {
      addRow([{ id: "__clear_search__", label: "✖ Clear Filter" }]);
    }

    for (let index = 0; index < bottom.length; index += 2) {
      addRow(bottom.slice(index, index + 2));
    }
    return keyboard;
  };

  while (true) {
    const normalizedFilter = activeFilter.trim().toLowerCase();
    const filteredModels = normalizedFilter
      ? safeModels.filter((entry) =>
          String(entry.id || "")
            .toLowerCase()
            .includes(normalizedFilter),
        )
      : safeModels;
    const totalPages = Math.max(
      1,
      Math.ceil(filteredModels.length / normalizedPageSize),
    );
    if (page > totalPages - 1) {
      page = totalPages - 1;
    }
    if (page < 0) {
      page = 0;
    }

    const start = page * normalizedPageSize;
    const pageModels = filteredModels.slice(start, start + normalizedPageSize);
    const safeTopOptions = Array.isArray(topOptions) ? topOptions : [];
    const safeBottomOptions = Array.isArray(bottomOptions) ? bottomOptions : [];
    const keyboard = buildKeyboard({
      top: safeTopOptions,
      pageModels,
      bottom: safeBottomOptions,
      hasPrev: totalPages > 1 && page > 0,
      hasNext: totalPages > 1 && page < totalPages - 1,
      hasFilter: Boolean(normalizedFilter),
    });
    const pageHint =
      totalPages > 1
        ? `\n_Page ${page + 1}/${totalPages} • Models ${start + 1}-${start + pageModels.length} of ${filteredModels.length}_`
        : "";
    const filterHint = normalizedFilter
      ? `\n_Filter: ${escapeMarkdown(normalizedFilter)}_`
      : "";
    menuMessage = await upsertMenuMessage(ctx, menuMessage, `${prompt}${filterHint}${pageHint}`, {
      parse_mode: "Markdown",
      reply_markup: keyboard,
    });
    const selectionCtx = await conversation.waitFor(
      "callback_query:data",
      (callbackCtx) => {
        const callbackData = callbackCtx?.callbackQuery?.data;
        if (!callbackData) return false;
        if (!matchesCallbackPrefix(callbackData, prefix)) return false;
        const parsed = parseCallbackData(callbackData);
        if (parsed?.signed && opToken && parsed.token && parsed.token !== opToken) {
          return false;
        }
        return true;
      },
    );
    safeEnsureActive();
    await selectionCtx.answerCallbackQuery();

    const selectedData = selectionCtx?.callbackQuery?.data || "";
    const action = parseCallbackData(selectedData).action || selectedData;
    const parts = action.split(":");
    const prefixSegments = String(prefix || "voice-model")
      .split(":")
      .filter(Boolean).length;
    const selectedId = parts.slice(prefixSegments).join(":");

    if (selectedId === "__nav_prev__") {
      page -= 1;
      continue;
    }
    if (selectedId === "__nav_next__") {
      page += 1;
      continue;
    }
    if (selectedId === "__clear_search__") {
      activeFilter = "";
      page = 0;
      continue;
    }
    if (selectedId === "__search__") {
      await ctx.reply(
        "🔎 Enter a model id filter (example: `helena`). Type `clear` to reset or `cancel` to abort.",
        { parse_mode: "Markdown" },
      );
      const update = await conversation.wait();
      safeEnsureActive();
      const input = String(update?.message?.text || "").trim();
      if (!input) {
        continue;
      }
      const lowered = input.toLowerCase();
      if (lowered === "cancel") {
        await dismissMenuMessage(ctx, menuMessage);
        return null;
      }
      if (lowered === "clear") {
        activeFilter = "";
        page = 0;
        continue;
      }
      activeFilter = lowered;
      page = 0;
      const matched = safeModels.some((entry) =>
        String(entry.id || "").toLowerCase().includes(activeFilter),
      );
      if (!matched) {
        await ctx.reply(
          `⚠️ No models matched "${escapeMarkdown(input)}". Try another filter.`,
          { parse_mode: "Markdown" },
        );
      }
      continue;
    }

    const lookup = [
      ...safeTopOptions,
      ...safeBottomOptions,
      ...pageModels.map((entry) => ({
        id: `model:${entry.id}`,
        label: formatVoiceLabel(entry),
      })),
    ].find((entry) => entry.id === selectedId);
    if (!lookup) {
      await dismissMenuMessage(ctx, menuMessage);
      return null;
    }
    await dismissMenuMessage(ctx, menuMessage);
    return lookup;
  }
}

module.exports = {
  fetchVoiceModelCatalog,
  formatVoiceLabel,
  formatVoiceCatalogLines,
  askVoiceModelWithPagination,
};
