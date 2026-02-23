"use strict";

function hasActiveConversation(ctx) {
  try {
    if (!ctx?.conversation || typeof ctx.conversation.active !== "function") {
      return false;
    }
    const active = ctx.conversation.active();
    return Array.isArray(active) && active.length > 0;
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
