const {
  parseCallbackAction,
  matchesExpiredConversation,
} = require("./callbackRouting");

const NOTICE_TTL_MS = 60 * 60 * 1000;

async function handleInvalidCallback({
  ctx,
  rawAction,
  validation,
  parseCallbackData,
  isDuplicateAction,
  safeAnswerCallback,
  clearCallbackMessageMarkup,
  clearMenuMessages,
  isProviderAction,
  handleProviderCallbackAction,
  reopenFreshMenu,
  providerHomeAction,
}) {
  const staleAction = validation.action || rawAction || "";
  const signed = parseCallbackData(rawAction);
  const parsedCallback = parseCallbackAction(staleAction);
  const expiredConversation = ctx.session?.meta?.expiredConversation || null;

  if (
    matchesExpiredConversation({
      expiredConversation,
      parsedCallback,
      signedToken: signed?.token,
    })
  ) {
    const staleNoticeKey = `expired_conversation:${
      expiredConversation.opId || expiredConversation.token || "unknown"
    }`;
    const firstExpiredNotice =
      !expiredConversation.noticeSent &&
      !isDuplicateAction(ctx, staleNoticeKey, NOTICE_TTL_MS);

    await safeAnswerCallback(ctx, {
      text: firstExpiredNotice
        ? "⌛ This menu expired. Use /menu to start again."
        : "⌛ Session expired. Use /menu to start again.",
      show_alert: false,
    });
    await clearCallbackMessageMarkup(ctx);

    if (firstExpiredNotice) {
      ctx.session.meta.expiredConversation = {
        ...expiredConversation,
        noticeSent: true,
      };
    }

    return { handled: true, metricStatus: "expired_callback" };
  }

  const staleMenuKey = `stale_menu:${validation.status}:${
    ctx.callbackQuery?.message?.message_id || "unknown"
  }`;
  const firstStaleNotice = !isDuplicateAction(ctx, staleMenuKey, NOTICE_TTL_MS);
  const shouldReopenProvider = isProviderAction(staleAction);
  const message =
    validation.status === "expired"
      ? "⌛ This menu expired. Opening the latest view…"
      : "⚠️ This menu is no longer active.";

  await safeAnswerCallback(ctx, { text: message, show_alert: false });
  await clearCallbackMessageMarkup(ctx);

  if (firstStaleNotice) {
    await clearMenuMessages(ctx);
    if (shouldReopenProvider) {
      await handleProviderCallbackAction(ctx, providerHomeAction);
    } else {
      await reopenFreshMenu(ctx, staleAction);
    }
  }

  return {
    handled: true,
    metricStatus: validation.status,
    metricExtra: { reason: validation.reason },
  };
}

module.exports = {
  handleInvalidCallback,
  NOTICE_TTL_MS,
};
