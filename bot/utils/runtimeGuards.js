"use strict";

function hasActiveConversation(ctx) {
  try {
    if (!ctx?.conversation || typeof ctx.conversation.active !== "function") {
      return false;
    }
    const active = ctx.conversation.active();
    if (Array.isArray(active)) {
      return active.length > 0;
    }
    if (typeof active === "number") {
      return active > 0;
    }
    if (active && typeof active === "object") {
      return Object.values(active).some((count) => Number(count) > 0);
    }
    return Boolean(active);
  } catch (_) {
    return false;
  }
}

function isSafeCallIdentifier(value) {
  return /^[A-Za-z0-9_-]{6,80}$/.test(String(value || "").trim());
}

module.exports = {
  hasActiveConversation,
  isSafeCallIdentifier,
};
