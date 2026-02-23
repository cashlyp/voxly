"use strict";

function hasConversationSessionState(ctx) {
  const conversationState = ctx?.session?.conversation;
  if (!conversationState || typeof conversationState !== "object") {
    return false;
  }
  const entries = Object.values(conversationState);
  if (entries.length === 0) {
    return false;
  }
  return entries.some((entry) => {
    if (Array.isArray(entry)) {
      return entry.length > 0;
    }
    if (entry && typeof entry === "object") {
      return Object.keys(entry).length > 0;
    }
    return Boolean(entry);
  });
}

function hasActiveConversation(ctx) {
  const hasInteractiveSession = Boolean(
    ctx?.session?.currentOp?.id ||
      ctx?.session?.flow?.name ||
      hasConversationSessionState(ctx),
  );
  try {
    if (!ctx?.conversation || typeof ctx.conversation.active !== "function") {
      return hasInteractiveSession;
    }
    const active = ctx.conversation.active();
    if (active && typeof active.then === "function") {
      // Conservatively treat unresolved conversation state as active to avoid
      // misrouting free-text input while a conversation is resuming.
      return true;
    }
    if (Array.isArray(active)) {
      return active.length > 0;
    }
    if (typeof active === "number") {
      return active > 0;
    }
    if (active && typeof active === "object") {
      const values = Object.values(active);
      if (values.length === 0) {
        return hasInteractiveSession;
      }
      return values.some((count) => Number(count) > 0);
    }
    return Boolean(active || hasInteractiveSession);
  } catch (_) {
    return hasInteractiveSession;
  }
}

function isSafeCallIdentifier(value) {
  return /^[A-Za-z0-9_-]{6,80}$/.test(String(value || "").trim());
}

module.exports = {
  hasActiveConversation,
  isSafeCallIdentifier,
  __testables: {
    hasConversationSessionState,
  },
};
