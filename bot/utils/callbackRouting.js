const OP_ID_SEGMENT_PATTERN = /^[0-9a-zA-Z-]{8,}$/;

function parseCallbackAction(action) {
  if (!action || !action.includes(":")) {
    return null;
  }
  const parts = action.split(":");
  const prefix = parts[0];
  if (parts.length >= 3 && OP_ID_SEGMENT_PATTERN.test(parts[1])) {
    return { prefix, opId: parts[1], value: parts.slice(2).join(":") };
  }
  return { prefix, opId: null, value: parts.slice(1).join(":") };
}

function resolveConversationFromPrefix(prefix, currentCommand = null) {
  if (!prefix) return null;
  if (prefix === "call-script-fallback") return "call-conversation";
  if (prefix.startsWith("call-script-")) return "scripts-conversation";
  if (prefix === "call-script") return "call-conversation";
  if (prefix.startsWith("sms-script-")) return "scripts-conversation";
  if (prefix.startsWith("inbound-default")) return "scripts-conversation";
  if (prefix === "sms-script") return "sms-conversation";
  if (prefix === "confirm") {
    if (typeof currentCommand === "string") {
      if (currentCommand.startsWith("email")) return "email-conversation";
      if (currentCommand.startsWith("scripts")) return "scripts-conversation";
    }
    return "scripts-conversation";
  }
  if (prefix.startsWith("script-")) {
    return "scripts-conversation";
  }
  if (prefix.startsWith("email-template-")) {
    return "email-templates-conversation";
  }
  if (prefix.startsWith("bulk-email-")) return "bulk-email-conversation";
  if (prefix.startsWith("email-")) return "email-conversation";
  if (prefix.startsWith("bulk-sms-")) return "bulk-sms-conversation";
  if (prefix.startsWith("sms-")) return "sms-conversation";
  if (prefix.startsWith("persona-")) return "persona-conversation";
  if (
    [
      "persona",
      "purpose",
      "tone",
      "urgency",
      "tech",
      "call-config",
      "call-voice",
    ].includes(prefix)
  ) {
    return "call-conversation";
  }
  return null;
}

function matchesExpiredConversation({
  expiredConversation,
  parsedCallback,
  signedToken,
}) {
  if (!expiredConversation) {
    return false;
  }
  if (
    expiredConversation.opId &&
    parsedCallback?.opId &&
    expiredConversation.opId === parsedCallback.opId
  ) {
    return true;
  }
  if (
    expiredConversation.token &&
    signedToken &&
    expiredConversation.token === signedToken
  ) {
    return true;
  }
  return false;
}

function isConversationCallbackStale(parsedCallback, currentOpId) {
  return (
    !parsedCallback?.opId || !currentOpId || parsedCallback.opId !== currentOpId
  );
}

function buildStaleConversationKey(conversationTarget, opId) {
  return `stale_conversation:${conversationTarget}:${opId || "unknown"}`;
}

module.exports = {
  parseCallbackAction,
  resolveConversationFromPrefix,
  matchesExpiredConversation,
  isConversationCallbackStale,
  buildStaleConversationKey,
};
