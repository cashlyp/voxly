let grammyPkg;
try {
  grammyPkg = require("grammy");
} catch (error) {
  console.error(
    '❌ Missing dependency "grammy". Run `npm ci --omit=dev` in /bot before starting PM2.',
  );
  throw error;
}
const { Bot, session, InlineKeyboard } = grammyPkg;

let conversationsPkg;
try {
  conversationsPkg = require("@grammyjs/conversations");
} catch (error) {
  console.error(
    '❌ Missing dependency "@grammyjs/conversations". Run `npm ci --omit=dev` in /bot before starting PM2.',
  );
  throw error;
}
const { conversations, createConversation } = conversationsPkg;
const axios = require("axios");
const httpClient = require("./utils/httpClient");
const config = require("./config");
const { attachHmacAuth } = require("./utils/apiAuth");
const {
  clearMenuMessages,
  getLatestMenuMessageId,
  isLatestMenuExpired,
  renderMenu,
} = require("./utils/ui");
const {
  buildCallbackData,
  validateCallback,
  parseCallbackData,
  isDuplicateAction,
  startActionMetric,
  finishActionMetric,
} = require("./utils/actions");
const {
  getConversationRecoveryTarget,
  buildCallbackReplayQueue,
  recoverConversationFromCallback,
} = require("./utils/conversationRecovery");
const {
  parseScriptDesignerCallbackAction,
  isScriptDesignerAction,
} = require("./utils/scriptDesignerCallbacks");
const {
  normalizeReply,
  logCommandError,
  escapeMarkdown,
  toHtmlSafeText,
  isEntityParseError,
} = require("./utils/ui");
const {
  hasActiveConversation,
  isSafeCallIdentifier,
} = require("./utils/runtimeGuards");
const {
  validateCallbackDataSize,
  validateCallSid,
} = require("./utils/inputValidator");
const {
  getAccessProfile,
  getCapabilityForCommand,
  getCapabilityForAction,
  requireCapability,
} = require("./utils/capabilities");

const apiOrigins = new Set();
try {
  apiOrigins.add(new URL(config.apiUrl).origin);
} catch (_) {}
try {
  apiOrigins.add(new URL(config.scriptsApiUrl).origin);
} catch (_) {}

attachHmacAuth(axios, {
  secret: config.apiAuth?.hmacSecret,
  allowedOrigins: apiOrigins,
  defaultBaseUrl: config.apiUrl,
});
const {
  initialSessionState,
  ensureSession,
  cancelActiveFlow,
  startOperation,
  resetSession,
  OperationCancelledError,
  getCurrentOpId,
} = require("./utils/sessionState");

// Bot initialization
const token = config.botToken;
const bot = new Bot(token);

// Initialize conversations with error handling wrapper
function wrapConversation(handler, name) {
  return createConversation(async (conversation, ctx) => {
    try {
      await handler(conversation, ctx);
    } catch (error) {
      if (error instanceof OperationCancelledError) {
        console.log(`Conversation ${name} cancelled: ${error.message}`);
        return;
      }
      console.error(`Conversation error in ${name}:`, error);
      const fallback =
        "❌ An error occurred during the conversation. Please try again.";
      const message = error?.userMessage || fallback;
      await ctx.reply(message);
    }
  }, name);
}

// IMPORTANT: Add session middleware BEFORE conversations
bot.use(session({ initial: initialSessionState }));

// Ensure every update touches a session object
bot.use(async (ctx, next) => {
  ensureSession(ctx);
  return next();
});

// Drop interactive updates without a user context to avoid downstream crashes
// in handlers that require ctx.from.id.
bot.use(async (ctx, next) => {
  const isInteractive = Boolean(ctx.message || ctx.callbackQuery);
  if (isInteractive && !ctx.from?.id) {
    return;
  }
  return next();
});

// Initialize conversations middleware AFTER session and BEFORE middleware
// that may need ctx.conversation helpers (for example slash-command resets).
bot.use(conversations());

// When a new slash command arrives, cancel any active flow first
bot.use(async (ctx, next) => {
  const text = ctx.message?.text || ctx.callbackQuery?.data;
  if (text && text.startsWith("/")) {
    const command = text.split(" ")[0].toLowerCase();
    await cancelActiveFlow(ctx, `command:${command}`);
    await clearMenuMessages(ctx);
    ctx.session.lastCommand = command;
    ctx.session.currentOp = null;
  }
  return next();
});

// Capability gating for slash commands
bot.use(async (ctx, next) => {
  const text = ctx.message?.text;
  if (!text || !text.startsWith("/")) {
    return next();
  }
  const command = text.split(" ")[0].slice(1).toLowerCase();
  const capability = getCapabilityForCommand(command);
  const access = await getAccessProfile(ctx);
  await syncChatCommands(ctx, access);
  if (capability) {
    const allowed = await requireCapability(ctx, capability, {
      actionLabel: `/${command}`,
      profile: access,
    });
    if (!allowed) {
      return;
    }
  }
  return next();
});

// Metrics for slash commands
bot.use(async (ctx, next) => {
  const command = ctx.message?.text?.startsWith("/")
    ? ctx.message.text.split(" ")[0].toLowerCase()
    : null;
  if (!command) {
    return next();
  }
  const metric = startActionMetric(ctx, `command:${command}`);
  try {
    const result = await next();
    finishActionMetric(metric, "ok");
    return result;
  } catch (error) {
    finishActionMetric(metric, "error", {
      error: error?.message || String(error),
    });
    throw error;
  }
});
// Normalize command replies to HTML formatting
bot.use(async (ctx, next) => {
  const isCommand = Boolean(
    ctx.message?.text?.startsWith("/") ||
    ctx.callbackQuery?.data ||
    ctx.session?.lastCommand,
  );
  if (!isCommand) {
    return next();
  }
  const originalReply = ctx.reply.bind(ctx);
  ctx.reply = async (text, options = {}) => {
    const normalized = normalizeReply(text, options);
    try {
      return await originalReply(normalized.text, normalized.options);
    } catch (error) {
      if (!isEntityParseError(error)) {
        throw error;
      }
      const fallbackOptions = {
        ...normalized.options,
        parse_mode: "HTML",
      };
      return originalReply(
        toHtmlSafeText(String(normalized.text || "")),
        fallbackOptions,
      );
    }
  };
  return next();
});

// Shared command wrapper for consistent error handling
bot.use(async (ctx, next) => {
  const isCommand = Boolean(
    ctx.message?.text?.startsWith("/") ||
    ctx.callbackQuery?.data ||
    ctx.session?.lastCommand,
  );
  if (!isCommand) {
    return next();
  }
  try {
    return await next();
  } catch (error) {
    logCommandError(ctx, error);
    try {
      const fallback =
        "⚠️ Sorry, something went wrong while handling that command. Please try again.";
      const message = error?.userMessage || fallback;
      await ctx.reply(message);
    } catch (replyError) {
      console.error("Failed to send command fallback:", replyError);
    }
  }
});

// Operator/alert inline actions
bot.callbackQuery(/^alert:/, async (ctx) => {
  const data = ctx.callbackQuery.data || "";

  // Validate callback data size to prevent DoS/injection attacks
  const sizeValidation = validateCallbackDataSize(data);
  if (sizeValidation !== true) {
    console.warn(`Invalid callback data size: ${sizeValidation}`);
    await safeAnswerCallbackQuery(ctx, {
      text: "Invalid action payload.",
      show_alert: false,
    });
    return;
  }

  const parts = data.split(":");
  if (parts.length < 3) {
    await safeAnswerCallbackQuery(ctx, {
      text: "Invalid action payload.",
      show_alert: false,
    });
    return;
  }
  const dedupeKey = `alert:${data}|${ctx.callbackQuery?.message?.message_id || ""}`;
  if (isDuplicateAction(ctx, dedupeKey)) {
    await safeAnswerCallbackQuery(ctx, {
      text: "Already processed.",
      show_alert: false,
    });
    return;
  }
  const action = parts[1];
  const callSid = parts.slice(2).join(":").trim();

  // Validate call SID format
  const sidValidation = validateCallSid(callSid);
  if (sidValidation !== true) {
    console.warn(`Invalid call SID: ${sidValidation}`);
    await safeAnswerCallbackQuery(ctx, {
      text: "Invalid call id.",
      show_alert: false,
    });
    return;
  }

  try {
    const allowed = await requireCapability(ctx, "call", {
      actionLabel: "Call controls",
    });
    if (!allowed) {
      await safeAnswerCallbackQuery(ctx, {
        text: "Access required.",
        show_alert: false,
      });
      return;
    }
    switch (action) {
      case "mute":
        await httpClient.post(
          ctx,
          `${API_BASE}/api/calls/${encodeURIComponent(callSid)}/operator`,
          { action: "mute_alerts" },
          { timeout: 8000 },
        );
        await safeAnswerCallbackQuery(ctx, {
          text: "🔕 Alerts muted for this call",
          show_alert: false,
        });
        break;
      case "retry":
        await httpClient.post(
          ctx,
          `${API_BASE}/api/calls/${encodeURIComponent(callSid)}/operator`,
          { action: "clarify", text: "Let me retry that step." },
          { timeout: 8000 },
        );
        await safeAnswerCallbackQuery(ctx, {
          text: "🔄 Retry requested",
          show_alert: false,
        });
        break;
      case "transfer":
        await httpClient.post(
          ctx,
          `${API_BASE}/api/calls/${encodeURIComponent(callSid)}/operator`,
          { action: "transfer" },
          { timeout: 8000 },
        );
        await safeAnswerCallbackQuery(ctx, {
          text: "📞 Transfer request noted",
          show_alert: false,
        });
        break;
      default:
        await safeAnswerCallbackQuery(ctx, {
          text: "Action not supported yet",
          show_alert: false,
        });
        break;
    }
  } catch (error) {
    const opId = getCurrentOpId(ctx);
    const userId = ctx.from?.id || "unknown";
    console.error(
      `Operator action error [opId=${opId}] [user=${userId}]:`,
      error?.message || error,
    );
    await safeAnswerCallbackQuery(ctx, {
      text: "⚠️ Failed to execute action",
      show_alert: false,
    });
  }
});

// Live call console actions (proxy to API webhook handler)
bot.callbackQuery(/^lc:/, async (ctx) => {
  const data = String(ctx.callbackQuery?.data || "");
  const dedupeKey = `lc:${data}|${ctx.callbackQuery?.message?.message_id || ""}`;
  if (isDuplicateAction(ctx, dedupeKey)) {
    await safeAnswerCallbackQuery(ctx, {
      text: "Already processed.",
      show_alert: false,
    });
    return;
  }
  try {
    const allowed = await requireCapability(ctx, "calllog_view", {
      actionLabel: "Live call console",
    });
    if (!allowed) {
      await safeAnswerCallbackQuery(ctx, {
        text: "Access required.",
        show_alert: false,
      });
      return;
    }
    await safeAnswerCallbackQuery(ctx);
    await httpClient.post(
      ctx,
      `${config.apiUrl}/webhook/telegram`,
      ctx.update,
      { timeout: 8000 },
    );
    return;
  } catch (error) {
    console.error("Live call action proxy error:", error?.message || error);
    await safeAnswerCallbackQuery(ctx, {
      text: "⚠️ Failed to process action",
      show_alert: false,
    });
  }
});

// Transcript actions from realtime status cards
bot.callbackQuery(/^(tr|rca):/, async (ctx) => {
  const data = String(ctx.callbackQuery?.data || "");
  const dedupeKey = `transcript:${data}|${ctx.callbackQuery?.message?.message_id || ""}`;
  if (isDuplicateAction(ctx, dedupeKey)) {
    await safeAnswerCallbackQuery(ctx, {
      text: "Already processed.",
      show_alert: false,
    });
    return;
  }
  const [prefix, callSid] = data.split(":");
  if (!callSid || !isSafeCallIdentifier(callSid)) {
    await safeAnswerCallbackQuery(ctx, {
      text: "Missing or invalid call id",
      show_alert: false,
    });
    return;
  }
  try {
    const allowed = await requireCapability(ctx, "calllog_view", {
      actionLabel: "Call transcript",
    });
    if (!allowed) {
      await safeAnswerCallbackQuery(ctx, {
        text: "Access required.",
        show_alert: false,
      });
      return;
    }

    await safeAnswerCallbackQuery(ctx, {
      text:
        prefix === "tr"
          ? "Loading transcript..."
          : "Loading transcript audio...",
      show_alert: false,
    });

    if (prefix === "tr") {
      await sendFullTranscriptFromApi(ctx, callSid);
      return;
    }
    await sendTranscriptAudioFromApi(ctx, callSid);
  } catch (error) {
    console.error("Transcript callback error:", error?.message || error);
    await safeAnswerCallbackQuery(ctx, {
      text: "⚠️ Failed to load transcript",
      show_alert: false,
    });
    await ctx.reply("⚠️ Failed to load transcript.");
  }
});

// Global error handler
bot.catch(async (err) => {
  const errorMessage = `Error while handling update ${err.ctx.update.update_id}:
    ${err.error.message}
    Stack: ${err.error.stack}`;
  console.error(errorMessage);

  try {
    await err.ctx.reply(
      "❌ An error occurred. Please try again or contact support.",
    );
  } catch (replyError) {
    console.error("Failed to send error message:", replyError);
  }
});

async function validateTemplatesApiConnectivity() {
  const healthUrl = new URL("/health", config.scriptsApiUrl).toString();
  try {
    const response = await httpClient.get(null, healthUrl, { timeout: 5000 });
    const contentType = response.headers?.["content-type"] || "";
    if (!contentType.includes("application/json")) {
      throw new Error(
        `healthcheck returned ${contentType || "unknown"} content`,
      );
    }
    const status = String(response.data?.status || "").toLowerCase();
    if (status && status !== "healthy") {
      throw new Error(`service reported status "${status}"`);
    }
    console.log(`✅ Templates API reachable (${healthUrl})`);
  } catch (error) {
    let reason;
    if (error.response) {
      const status = error.response.status;
      const statusText = error.response.statusText || "";
      reason = `HTTP ${status} ${statusText}`;
    } else if (error.request) {
      reason = "no response received";
    } else {
      reason = error.message;
    }
    throw new Error(`Unable to reach Templates API at ${healthUrl}: ${reason}`);
  }
}

// Import dependencies
const { getUser, expireInactiveUsers, closeDb } = require("./db/db");
const { callFlow, registerCallCommand } = require("./commands/call");
const {
  smsFlow,
  bulkSmsFlow,
  scheduleSmsFlow,
  smsStatusFlow,
  smsConversationFlow,
  recentSmsFlow,
  smsStatsFlow,
  bulkSmsStatusFlow,
  renderSmsMenu,
  renderBulkSmsMenu,
  sendRecentSms,
  sendBulkSmsList,
  sendBulkSmsStats,
  registerSmsCommands,
  getSmsStats,
} = require("./commands/sms");
const {
  emailFlow,
  bulkEmailFlow,
  emailTemplatesFlow,
  renderEmailMenu,
  renderBulkEmailMenu,
  emailStatusFlow,
  bulkEmailStatusFlow,
  bulkEmailHistoryFlow,
  bulkEmailStatsFlow,
  sendBulkEmailHistory,
  sendBulkEmailStats,
  emailHistoryFlow,
  registerEmailCommands,
  sendEmailStatusCard,
  sendEmailTimeline,
  sendBulkStatusCard,
} = require("./commands/email");
const { scriptsFlow, registerScriptsCommand } = require("./commands/scripts");
const { personaFlow, registerPersonaCommand } = require("./commands/persona");
const {
  renderCalllogMenu,
  calllogRecentFlow,
  calllogSearchFlow,
  calllogDetailsFlow,
  calllogEventsFlow,
  registerCalllogCommand,
} = require("./commands/calllog");
const {
  registerProviderCommand,
  handleProviderSwitch,
  renderProviderMenu,
} = require("./commands/provider");
const {
  addUserFlow,
  promoteFlow,
  removeUserFlow,
  registerUserListCommand,
  renderUsersMenu,
  sendUsersList,
} = require("./commands/users");
const {
  registerCallerFlagsCommand,
  renderCallerFlagsMenu,
  sendCallerFlagsList,
  callerFlagAllowFlow,
  callerFlagBlockFlow,
  callerFlagSpamFlow,
} = require("./commands/callerFlags");
const { registerHelpCommand, handleHelp } = require("./commands/help");
const { registerMenuCommand, handleMenu } = require("./commands/menu");
const { registerGuideCommand, handleGuide } = require("./commands/guide");
const {
  registerApiCommands,
  handleStatusCommand,
  handleHealthCommand,
} = require("./commands/api");

// Register conversations with error handling
bot.use(wrapConversation(callFlow, "call-conversation"));
bot.use(wrapConversation(addUserFlow, "adduser-conversation"));
bot.use(wrapConversation(promoteFlow, "promote-conversation"));
bot.use(wrapConversation(removeUserFlow, "remove-conversation"));
bot.use(wrapConversation(scheduleSmsFlow, "schedule-sms-conversation"));
bot.use(wrapConversation(smsFlow, "sms-conversation"));
bot.use(wrapConversation(smsStatusFlow, "sms-status-conversation"));
bot.use(wrapConversation(smsConversationFlow, "sms-thread-conversation"));
bot.use(wrapConversation(recentSmsFlow, "sms-recent-conversation"));
bot.use(wrapConversation(smsStatsFlow, "sms-stats-conversation"));
bot.use(wrapConversation(bulkSmsFlow, "bulk-sms-conversation"));
bot.use(wrapConversation(bulkSmsStatusFlow, "bulk-sms-status-conversation"));
bot.use(wrapConversation(emailFlow, "email-conversation"));
bot.use(wrapConversation(emailStatusFlow, "email-status-conversation"));
bot.use(wrapConversation(emailTemplatesFlow, "email-templates-conversation"));
bot.use(wrapConversation(bulkEmailFlow, "bulk-email-conversation"));
bot.use(
  wrapConversation(bulkEmailStatusFlow, "bulk-email-status-conversation"),
);
bot.use(
  wrapConversation(bulkEmailHistoryFlow, "bulk-email-history-conversation"),
);
bot.use(wrapConversation(bulkEmailStatsFlow, "bulk-email-stats-conversation"));
bot.use(wrapConversation(calllogRecentFlow, "calllog-recent-conversation"));
bot.use(wrapConversation(calllogSearchFlow, "calllog-search-conversation"));
bot.use(wrapConversation(calllogDetailsFlow, "calllog-details-conversation"));
bot.use(wrapConversation(calllogEventsFlow, "calllog-events-conversation"));
bot.use(wrapConversation(scriptsFlow, "scripts-conversation"));
bot.use(wrapConversation(personaFlow, "persona-conversation"));
bot.use(wrapConversation(callerFlagAllowFlow, "callerflag-allow-conversation"));
bot.use(wrapConversation(callerFlagBlockFlow, "callerflag-block-conversation"));
bot.use(wrapConversation(callerFlagSpamFlow, "callerflag-spam-conversation"));

// Register command handlers
registerCallCommand(bot);
registerSmsCommands(bot);
registerEmailCommands(bot);
registerScriptsCommand(bot);
registerUserListCommand(bot);
registerPersonaCommand(bot);
registerCalllogCommand(bot);
registerCallerFlagsCommand(bot);

// Register non-conversation commands
registerHelpCommand(bot);
registerMenuCommand(bot);
registerGuideCommand(bot);
registerApiCommands(bot);
registerProviderCommand(bot);
const API_BASE = config.apiUrl;

async function safeAnswerCallbackQuery(ctx, options = {}) {
  if (!ctx?.callbackQuery) {
    return;
  }
  try {
    await ctx.answerCallbackQuery(options);
  } catch (_) {
    // Ignore stale/already-answered callback query errors.
  }
}

function buildCallbackStateSnapshot(ctx) {
  let activeConversations = null;
  try {
    if (ctx?.conversation && typeof ctx.conversation.active === "function") {
      activeConversations = ctx.conversation.active();
    }
  } catch (_) {
    activeConversations = null;
  }
  return {
    op_id: ctx.session?.currentOp?.id || null,
    op_token: ctx.session?.currentOp?.token || null,
    op_command: ctx.session?.currentOp?.command || null,
    flow_name: ctx.session?.flow?.name || null,
    flow_step: ctx.session?.flow?.step || null,
    active_conversations: activeConversations,
  };
}

function getRequesterChatId(ctx) {
  const chatId =
    ctx.callbackQuery?.message?.chat?.id || ctx.chat?.id || ctx.from?.id || "";
  return String(chatId || "").trim();
}

function normalizeOpToken(opId) {
  const raw = String(opId || "").trim();
  if (!raw) return null;
  return raw.replace(/-/g, "").slice(0, 8) || null;
}

function hasMatchingConversationOpToken(ctx, recoveryTarget) {
  const callbackOpToken = normalizeOpToken(recoveryTarget?.parsed?.opId);
  const currentOpToken = ctx.session?.currentOp?.token || null;
  return Boolean(
    callbackOpToken && currentOpToken && callbackOpToken === currentOpToken,
  );
}

function isConversationTargetActive(ctx, conversationTarget) {
  if (!conversationTarget) {
    return false;
  }
  try {
    if (!ctx?.conversation || typeof ctx.conversation.active !== "function") {
      return false;
    }
    const scoped = ctx.conversation.active(conversationTarget);
    if (scoped && typeof scoped.then === "function") {
      // If active-state probing is async in this runtime, avoid stale routing by
      // treating the conversation as active.
      return true;
    }
    if (typeof scoped === "number") {
      return scoped > 0;
    }
    if (typeof scoped === "boolean") {
      return scoped;
    }
    const active = ctx.conversation.active();
    if (active && typeof active.then === "function") {
      return true;
    }
    if (Array.isArray(active)) {
      return active.includes(conversationTarget);
    }
    if (active && typeof active === "object") {
      if (Object.keys(active).length === 0) {
        return Boolean(ctx.session?.currentOp?.id);
      }
      return Number(active[conversationTarget] || 0) > 0;
    }
    if (typeof active === "number") {
      return active > 0;
    }
    return false;
  } catch (_) {
    return false;
  }
}

function isScriptDesignerRecoveryPrefix(prefix) {
  const value = String(prefix || "");
  return (
    value.startsWith("call-script-") ||
    value.startsWith("sms-script-") ||
    value.startsWith("script-") ||
    value.startsWith("inbound-default") ||
    value.startsWith("email-template-")
  );
}

const SCRIPT_RECOVERY_GUARD_WINDOW_MS = 30 * 1000;
const SCRIPT_RECOVERY_GUARD_MAX_ATTEMPTS = 2;

function reserveScriptRecoveryAttempt(ctx, action, messageId = null) {
  ensureSession(ctx);
  ctx.session.meta = ctx.session.meta || {};
  const key = `${String(action || "")}|${String(messageId || "")}`;
  const now = Date.now();
  const attempts =
    ctx.session.meta.scriptRecoveryAttempts &&
    typeof ctx.session.meta.scriptRecoveryAttempts === "object"
      ? ctx.session.meta.scriptRecoveryAttempts
      : {};

  Object.entries(attempts).forEach(([attemptKey, value]) => {
    const lastAt = Number(value?.lastAt || 0);
    if (!lastAt || now - lastAt > SCRIPT_RECOVERY_GUARD_WINDOW_MS) {
      delete attempts[attemptKey];
    }
  });

  const previous = attempts[key];
  if (
    !previous ||
    !Number.isFinite(Number(previous.firstAt)) ||
    now - Number(previous.firstAt) > SCRIPT_RECOVERY_GUARD_WINDOW_MS
  ) {
    attempts[key] = { count: 1, firstAt: now, lastAt: now };
  } else {
    attempts[key] = {
      count: Number(previous.count || 0) + 1,
      firstAt: Number(previous.firstAt),
      lastAt: now,
    };
  }
  ctx.session.meta.scriptRecoveryAttempts = attempts;
  const count = Number(attempts[key]?.count || 0);
  return {
    key,
    count,
    allowed: count <= SCRIPT_RECOVERY_GUARD_MAX_ATTEMPTS,
  };
}

function clearScriptRecoveryAttempt(ctx, key) {
  if (!key || !ctx?.session?.meta?.scriptRecoveryAttempts) {
    return;
  }
  delete ctx.session.meta.scriptRecoveryAttempts[key];
}

async function handleScriptDesignerCallbackRouting(
  ctx,
  action,
  recoveryTarget,
  {
    isMenuExemptAction = false,
    finishMetric,
  } = {},
) {
  if (!isScriptDesignerAction(action) || !recoveryTarget) {
    return false;
  }

  const hasActiveOp = Boolean(ctx.session?.currentOp?.id);
  const hasMatchingOpToken = hasMatchingConversationOpToken(ctx, recoveryTarget);
  const hasActiveRecoveryConversation = isConversationTargetActive(
    ctx,
    recoveryTarget.conversationTarget,
  );

  if (hasActiveOp && hasMatchingOpToken) {
    finishMetric?.("ignored", { reason: "active_op_router_leak" });
    return true;
  }

  if (isMenuExemptAction && hasActiveRecoveryConversation && !hasMatchingOpToken) {
    finishMetric?.("ignored", {
      reason: "menu_exempt_conversation_listener",
    });
    return true;
  }

  if (!hasActiveOp) {
    if (hasActiveRecoveryConversation) {
      finishMetric?.("ignored", {
        reason: "active_conversation_router_leak",
      });
      return true;
    }

    const recoveryPrefix = String(recoveryTarget?.parsed?.prefix || "");
    if (isScriptDesignerRecoveryPrefix(recoveryPrefix)) {
      const replayQueue = buildCallbackReplayQueue(action);
      const expiredMessage = recoveryPrefix.startsWith("email-template-")
        ? "⚠️ Template menu is stale. Reopen with /email."
        : "⚠️ Script Designer menu is stale. Reopen with /scripts.";
      if (!replayQueue.length) {
        await ctx.reply(expiredMessage).catch(() => {});
        finishMetric?.("stale", { reason: "replay_not_supported" });
        return true;
      }
      const recoveryAttempt = reserveScriptRecoveryAttempt(
        ctx,
        action,
        ctx.callbackQuery?.message?.message_id || null,
      );
      if (!recoveryAttempt.allowed) {
        await ctx.reply(expiredMessage).catch(() => {});
        finishMetric?.("stale", {
          reason: "recovery_loop_guard",
          attempts: recoveryAttempt.count,
        });
        return true;
      }
      const replayState =
        replayQueue.length > 0
          ? {
              pendingCallbackReplay: {
                actions: replayQueue,
                createdAt: Date.now(),
                sourceAction: action,
              },
            }
          : null;
      const lastCommand = String(ctx.session?.lastCommand || "").toLowerCase();
      const recoveryTargets = [recoveryTarget.conversationTarget];
      if (
        recoveryPrefix.startsWith("email-template-") &&
        lastCommand === "scripts"
      ) {
        recoveryTargets.unshift("scripts-conversation");
      } else if (
        recoveryPrefix.startsWith("call-script-") ||
        recoveryPrefix.startsWith("sms-script-") ||
        recoveryPrefix.startsWith("script-") ||
        recoveryPrefix.startsWith("inbound-default")
      ) {
        recoveryTargets.push("scripts-conversation");
      }
      const uniqueRecoveryTargets = Array.from(
        new Set(recoveryTargets.filter(Boolean)),
      );
      for (const conversationTarget of uniqueRecoveryTargets) {
        try {
          console.log(
            JSON.stringify({
              type: "callback_query_recovery_attempt",
              callback_data: action,
              conversation_target: conversationTarget,
              replay_count: replayQueue.length,
              recovery_attempt: recoveryAttempt.count,
              user_id: ctx.from?.id || null,
              chat_id: ctx.chat?.id || ctx.callbackQuery?.message?.chat?.id || null,
            }),
          );
          const recovered = await recoverConversationFromCallback(
            ctx,
            action,
            conversationTarget,
            {
              cancelActiveFlow,
              resetSession,
              clearMenuMessages,
            },
            {
              notify: false,
              sessionMeta: replayState,
            },
          );
          if (recovered) {
            clearScriptRecoveryAttempt(ctx, recoveryAttempt.key);
            finishMetric?.("ok", {
              reason: "recovered_missing_active_op",
              replay_count: replayQueue.length,
              recovery_target: conversationTarget,
            });
            return true;
          }
        } catch (recoveryError) {
          console.warn(
            `Conversation recovery failed for ${action} -> ${conversationTarget}:`,
            recoveryError?.message || recoveryError,
          );
        }
      }
      if (hasActiveConversation(ctx)) {
        finishMetric?.("ignored", {
          reason: "active_conversation_recovery_failed",
        });
        return true;
      }
      await ctx.reply(expiredMessage).catch(() => {});
      finishMetric?.("stale", { reason: "missing_active_op" });
      return true;
    }

    await clearMenuMessages(ctx);
    await handleMenu(ctx);
    finishMetric?.("stale", { reason: "missing_active_op" });
    return true;
  }

  finishMetric?.("stale", { reason: "op_token_mismatch" });
  return true;
}

function splitTelegramMessage(text, maxLength = 3900) {
  const source = String(text || "");
  if (source.length <= maxLength) return [source];
  const chunks = [];
  let remaining = source;
  while (remaining.length > maxLength) {
    let splitAt = remaining.lastIndexOf("\n", maxLength);
    if (splitAt < maxLength * 0.6) {
      splitAt = maxLength;
    }
    chunks.push(remaining.slice(0, splitAt).trim());
    remaining = remaining.slice(splitAt).trim();
  }
  if (remaining) {
    chunks.push(remaining);
  }
  return chunks;
}

function buildTranscriptText(call, transcripts) {
  const lines = ["📄 Full Transcript", ""];
  if (call?.phone_number) {
    lines.push(`📞 Phone: ${call.phone_number}`);
  }
  const duration = Number(call?.duration);
  if (Number.isFinite(duration) && duration > 0) {
    const minutes = Math.floor(duration / 60);
    const seconds = duration % 60;
    lines.push(`⏱️ Duration: ${minutes}:${String(seconds).padStart(2, "0")}`);
  }
  if (call?.created_at) {
    lines.push(`🕐 Started: ${new Date(call.created_at).toLocaleString()}`);
  }
  lines.push(`💬 Messages: ${transcripts.length}`);
  lines.push("");
  lines.push("Conversation:");
  lines.push("─────────────────────────");

  for (const entry of transcripts) {
    const speaker = entry?.speaker === "user" ? "🧑 User" : "🤖 AI";
    const message = String(entry?.message || "").trim();
    if (!message) continue;
    lines.push(`${speaker}: ${message}`);
    lines.push("");
  }

  return lines.join("\n").trim();
}

async function sendFullTranscriptFromApi(ctx, callSid) {
  const requesterChatId = getRequesterChatId(ctx);
  const url = `${API_BASE}/api/calls/${encodeURIComponent(callSid)}`;
  try {
    const response = await httpClient.get(ctx, url, {
      timeout: 15000,
      headers: requesterChatId
        ? { "x-telegram-chat-id": requesterChatId }
        : undefined,
    });
    const payload = response?.data || {};
    const call = payload?.call || payload;
    const transcripts = Array.isArray(payload?.transcripts)
      ? payload.transcripts
      : [];

    if (
      call?.user_chat_id &&
      requesterChatId &&
      String(call.user_chat_id) !== requesterChatId
    ) {
      await ctx.reply("❌ Not authorized for this call.");
      return;
    }

    if (!transcripts.length) {
      await ctx.reply("📄 Transcript is not available yet.");
      return;
    }

    const message = buildTranscriptText(call, transcripts);
    const chunks = splitTelegramMessage(message, 3900);
    const replyToMessageId = ctx.callbackQuery?.message?.message_id;

    for (let index = 0; index < chunks.length; index += 1) {
      const chunk = chunks[index];
      if (!chunk) continue;
      const options =
        index === 0 && replyToMessageId
          ? { reply_to_message_id: replyToMessageId }
          : undefined;
      await ctx.reply(chunk, options);
    }
  } catch (error) {
    const status = Number(error?.response?.status || 0);
    if (status === 403) {
      await ctx.reply("❌ Not authorized for this call.");
      return;
    }
    if (status === 404) {
      await ctx.reply("📄 Transcript not found for this call.");
      return;
    }
    if (status >= 500) {
      await ctx.reply("⚠️ Transcript service is temporarily unavailable.");
      return;
    }
    await ctx.reply(
      httpClient.getUserMessage(error, "Failed to fetch transcript."),
    );
  }
}

async function sendTranscriptAudioFromApi(ctx, callSid) {
  const requesterChatId = getRequesterChatId(ctx);
  const url = `${API_BASE}/api/calls/${encodeURIComponent(callSid)}/transcript/audio`;
  try {
    const response = await httpClient.get(ctx, url, {
      timeout: 15000,
      headers: requesterChatId
        ? { "x-telegram-chat-id": requesterChatId }
        : undefined,
    });
    const status = Number(response?.status || 0);
    const payload = response?.data || {};

    if (
      status === 202 ||
      String(payload?.status || "").toLowerCase() === "pending"
    ) {
      await ctx.reply(
        payload?.message || "🎧 Transcript audio is not available yet.",
      );
      return;
    }

    const audioUrl = String(payload?.audio_url || "").trim();
    if (status !== 200 || !audioUrl) {
      await ctx.reply("🎧 Transcript audio is not available yet.");
      return;
    }

    await ctx.replyWithAudio(audioUrl, {
      caption: payload?.caption || "🎧 Transcript audio",
      reply_to_message_id: ctx.callbackQuery?.message?.message_id || undefined,
    });
  } catch (error) {
    const status = Number(error?.response?.status || 0);
    if (status === 403) {
      await ctx.reply("❌ Not authorized for this call.");
      return;
    }
    if (status === 404) {
      await ctx.reply("🎧 Transcript audio not found for this call.");
      return;
    }
    if (status >= 500) {
      await ctx.reply(
        "⚠️ Transcript audio service is temporarily unavailable.",
      );
      return;
    }
    await ctx.reply(
      httpClient.getUserMessage(error, "Failed to fetch transcript audio."),
    );
  }
}

// Start command handler
bot.command("start", async (ctx) => {
  try {
    expireInactiveUsers();

    const access = await getAccessProfile(ctx);
    const isOwner = access.isAdmin;
    const safeUsername = escapeMarkdown(`@${ctx.from?.username || "none"}`);
    const safeRole = access.user?.role
      ? escapeMarkdown(String(access.user.role))
      : "Guest";
    await syncChatCommands(ctx, access);

    const userStats = access.user
      ? `👤 *User Information*
• ID: \`${ctx.from.id}\`
• Username: ${safeUsername}
• Role: ${safeRole}
• Joined: ${new Date(access.user.timestamp).toLocaleDateString()}`
      : `👤 *Guest Access*
• ID: \`${ctx.from.id}\`
• Username: ${safeUsername}
• Role: Guest`;

    const welcomeText = access.user
      ? isOwner
        ? "🛡️ *Welcome, Administrator!*\n\nYou have full access to all bot features."
        : "👋 *Welcome to Voicednut Bot!*\n\nYou can make voice calls using AI agents."
      : "⚠️ *Limited Access*\n\nYou can explore menus, but execution requires approval.";

    const kb = new InlineKeyboard()
      // PRIMARY: Call, SMS, Email, Call Log (2x2 grid)
      .text(access.user ? "📞 Call" : "🔒 Call", buildCallbackData(ctx, "CALL"))
      .text(access.user ? "💬 SMS" : "🔒 SMS", buildCallbackData(ctx, "SMS"))
      .row()
      .text(
        access.user ? "📧 Email" : "🔒 Email",
        buildCallbackData(ctx, "EMAIL"),
      )
      .text(
        access.user ? "📜 Call Log" : "🔒 Call Log",
        buildCallbackData(ctx, "CALLLOG"),
      )
      .row()
      // UTILITIES: Guide, Help, Menu, Health (2x2 grid)
      .text("📚 Guide", buildCallbackData(ctx, "GUIDE"))
      .text("ℹ️ Help", buildCallbackData(ctx, "HELP"))
      .row()
      .text("📋 Menu", buildCallbackData(ctx, "MENU"));

    if (access.user) {
      kb.text("🏥 Health", buildCallbackData(ctx, "HEALTH"));
    }

    kb.row();

    // ADMIN TOOLS: SMS Sender, Mailer, Users, Caller Flags, Scripts, Provider, Status (admin-only)
    if (isOwner) {
      kb.text("📤 SMS Sender", buildCallbackData(ctx, "BULK_SMS"))
        .text("📧 Mailer", buildCallbackData(ctx, "BULK_EMAIL"))
        .row()
        .text("👥 Users", buildCallbackData(ctx, "USERS"))
        .text("📵 Caller Flags", buildCallbackData(ctx, "CALLER_FLAGS"))
        .row()
        .text("🧰 Scripts", buildCallbackData(ctx, "SCRIPTS"))
        .text("☎️ Provider", buildCallbackData(ctx, "PROVIDER_STATUS"))
        .row()
        .text("🔍 Status", buildCallbackData(ctx, "STATUS"))
        .row();
    }

    // REQUEST ACCESS: For guests (admin-only)
    if (!access.user) {
      const adminUsername = (config.admin.username || "").replace(/^@/, "");
      if (adminUsername) {
        kb.url("📱 Request Access", `https://t.me/${adminUsername}`);
      }
    }

    const message = `${welcomeText}\n\n${userStats}\n\nTip: SMS and Email actions are grouped under /sms and /email.\n\nUse the buttons below or type /help for available commands.`;
    await renderMenu(ctx, message, kb, { parseMode: "Markdown" });
  } catch (error) {
    console.error("Start command error:", error);
    await ctx.reply(
      "❌ An error occurred. Please try again or contact support.",
    );
  }
});

// Enhanced callback query handler
bot.on("callback_query:data", async (ctx) => {
  const rawAction = String(ctx.callbackQuery?.data || "");
  const metric = startActionMetric(ctx, "callback", { raw_action: rawAction });
  const callbackMeta = {
    user_id: ctx.from?.id || null,
    chat_id: ctx.callbackQuery?.message?.chat?.id || ctx.chat?.id || null,
    message_id: ctx.callbackQuery?.message?.message_id || null,
    state: buildCallbackStateSnapshot(ctx),
  };
  const finishMetric = (status, extra = {}) => {
    finishActionMetric(metric, status, extra);
  };
  try {
    console.log(
      JSON.stringify({
        type: "callback_query_received",
        callback_data: rawAction,
        ...callbackMeta,
      }),
    );
    if (!rawAction) {
      await safeAnswerCallbackQuery(ctx, {
        text: "⚠️ That option is unavailable.",
        show_alert: false,
      });
      finishMetric("invalid");
      return;
    }
    if (
      rawAction &&
      (rawAction.startsWith("lc:") ||
        rawAction.startsWith("tr:") ||
        rawAction.startsWith("rca:"))
    ) {
      finishMetric("skipped");
      return;
    }
    const menuExemptPrefixes = [
      "alert:",
      "lc:",
      "tr:",
      "rca:",
      "call-script-", // Call Script Designer menus
      "sms-script-", // SMS Script Designer menus
      "inbound-default", // Inbound default submenus
      "script-", // General script menus (business, persona, draft, etc.)
      "email-template-", // Email template menus (covers all email sub-menus)
      "persona-", // Persona selection menus
    ];
    const parsedRawCallback = parseCallbackData(rawAction);
    const parsedRawAction = parsedRawCallback.action || rawAction;
    const matchesMenuExemptPrefix = (candidate) =>
      menuExemptPrefixes.some((prefix) =>
        String(candidate || "").startsWith(prefix),
      );
    const isMenuExempt =
      matchesMenuExemptPrefix(rawAction) ||
      matchesMenuExemptPrefix(parsedRawAction);
    const validation = isMenuExempt
      ? {
          status: "ok",
          action: parsedRawAction,
        }
      : validateCallback(ctx, rawAction);

    if (isMenuExempt && validation.status === "ok") {
      console.log(
        JSON.stringify({
          type: "callback_query_exempt",
          callback_data: rawAction,
          action: validation.action || parsedRawAction,
          reason: "conversation_menu",
          ...callbackMeta,
        }),
      );
    }

    if (validation.status !== "ok") {
      console.warn(
        JSON.stringify({
          type: "callback_query_rejected",
          callback_data: rawAction,
          action: validation.action || null,
          status: validation.status,
          reason: validation.reason || null,
          ...callbackMeta,
        }),
      );
      const message =
        validation.status === "expired"
          ? "⌛ This menu expired. Opening the latest view…"
          : "⚠️ This menu is no longer active.";
      const staleTarget = getConversationRecoveryTarget(validation.action);
      const rejectedScriptAction = parseScriptDesignerCallbackAction(
        validation.action || rawAction,
      );
      const hasActiveConversationOp = Boolean(
        staleTarget &&
        (ctx.session?.currentOp?.id ||
          isConversationTargetActive(ctx, staleTarget.conversationTarget)),
      );
      await safeAnswerCallbackQuery(ctx, { text: message, show_alert: false });
      // Keep stale conversation-scoped callbacks side-effect free.
      // Falling back to the global admin menu here can unexpectedly
      // kick users out of Script Designer/SMS/Email template flows.
      if (!hasActiveConversationOp && !staleTarget) {
        if (rejectedScriptAction.isScriptDesigner) {
          await ctx
            .reply("⚠️ Script Designer menu is stale. Reopen with /scripts.")
            .catch(() => {});
        } else {
          await clearMenuMessages(ctx);
          await handleMenu(ctx);
        }
      }
      finishMetric(validation.status, { reason: validation.reason });
      return;
    }

    const scriptActionDetails = parseScriptDesignerCallbackAction(
      validation.action,
    );
    if (scriptActionDetails.isScriptDesigner && !scriptActionDetails.valid) {
      console.warn(
        JSON.stringify({
          type: "script_designer_callback_rejected",
          callback_data: rawAction,
          action: validation.action,
          reason: scriptActionDetails.reason,
          ...callbackMeta,
        }),
      );
      await safeAnswerCallbackQuery(ctx, {
        text: "⚠️ Invalid script menu action.",
        show_alert: false,
      });
      finishMetric("invalid", {
        reason: `script_designer_${scriptActionDetails.reason}`,
      });
      return;
    }

    const action =
      scriptActionDetails.isScriptDesigner && scriptActionDetails.valid
        ? scriptActionDetails.normalizedAction
        : validation.action;
    const recoveryTarget = getConversationRecoveryTarget(action);
    const isConversationAction = Boolean(recoveryTarget);
    const actionKey = `${action}|${ctx.callbackQuery?.message?.message_id || ""}`;
    if (isDuplicateAction(ctx, actionKey)) {
      await safeAnswerCallbackQuery(ctx, {
        text: "Already processed.",
        show_alert: false,
      });
      finishMetric("duplicate");
      return;
    }

    // Answer callback query immediately to prevent timeout
    await safeAnswerCallbackQuery(ctx);
    console.log(
      JSON.stringify({
        type: "callback_query_routed",
        callback_data: rawAction,
        action,
        ...callbackMeta,
      }),
    );

    await getAccessProfile(ctx);
    const requiredCapability = getCapabilityForAction(action);
    if (requiredCapability) {
      const allowed = await requireCapability(ctx, requiredCapability, {
        actionLabel: action,
      });
      if (!allowed) {
        finishMetric("forbidden");
        return;
      }
    }

    const isMenuExemptAction = matchesMenuExemptPrefix(action);
    const isSessionBoundAction = /^[A-Za-z0-9_-]+:[0-9a-fA-F-]{8,}(?::|$)/.test(
      action,
    );
    const menuMessageId = ctx.callbackQuery?.message?.message_id;
    const menuChatId = ctx.callbackQuery?.message?.chat?.id;
    const latestMenuId = getLatestMenuMessageId(ctx, menuChatId);
    const hasMatchingConversationOp =
      isConversationAction &&
      ctx.session?.currentOp?.id &&
      hasMatchingConversationOpToken(ctx, recoveryTarget);
    if (
      !isMenuExemptAction &&
      isSessionBoundAction &&
      isLatestMenuExpired(ctx, menuChatId)
    ) {
      // If a stale callback arrives while a matching conversation op is still active,
      // keep the user in the current step instead of force-restarting the flow.
      if (hasMatchingConversationOp) {
        finishMetric("expired", { reason: "active_conversation" });
        return;
      }
      await safeAnswerCallbackQuery(ctx, {
        text: "⌛ This menu expired. Opening the latest view…",
        show_alert: false,
      });
      await clearMenuMessages(ctx);
      await handleMenu(ctx);
      finishMetric("expired");
      return;
    }
    if (
      !isMenuExemptAction &&
      isSessionBoundAction &&
      menuMessageId &&
      latestMenuId &&
      menuMessageId !== latestMenuId
    ) {
      if (hasMatchingConversationOp) {
        finishMetric("stale", { reason: "active_conversation" });
        return;
      }
      await safeAnswerCallbackQuery(ctx, {
        text: "⚠️ That button is out of date. Opening the latest view…",
        show_alert: false,
      });
      await clearMenuMessages(ctx);
      await handleMenu(ctx);
      finishMetric("stale");
      return;
    }

    if (action.startsWith("CALL_DETAILS:")) {
      const detailsKey = action.split(":")[1];
      const detailsMessage = ctx.session?.callDetailsCache?.[detailsKey];
      if (!detailsMessage) {
        await ctx.reply("ℹ️ Details are no longer available for this call.");
        finishMetric("not_found");
        return;
      }
      await ctx.reply(detailsMessage);
      finishMetric("ok");
      return;
    }

    if (action.startsWith("PROVIDER_SET:")) {
      const parts = action.split(":");
      const hasChannel = parts.length >= 3;
      const channel = hasChannel ? parts[1] : "call";
      const provider = hasChannel ? parts.slice(2).join(":") : parts[1];
      await cancelActiveFlow(ctx, `callback:${action}`);
      resetSession(ctx);
      await handleProviderSwitch(
        ctx,
        provider?.toLowerCase(),
        channel?.toLowerCase(),
      );
      finishMetric("ok");
      return;
    }

    if (action.startsWith("PROVIDER_CHANNEL:")) {
      const [, channel] = action.split(":");
      await cancelActiveFlow(ctx, `callback:${action}`);
      resetSession(ctx);
      await renderProviderMenu(ctx, {
        forceRefresh: true,
        channel: channel?.toLowerCase(),
      });
      finishMetric("ok");
      return;
    }

    if (action.startsWith("PROVIDER_STATUS:")) {
      const [, channel] = action.split(":");
      await cancelActiveFlow(ctx, `callback:${action}`);
      resetSession(ctx);
      await renderProviderMenu(ctx, {
        forceRefresh: true,
        channel: channel?.toLowerCase(),
      });
      finishMetric("ok");
      return;
    }

    if (action.startsWith("EMAIL_STATUS:")) {
      const [, messageId] = action.split(":");
      await cancelActiveFlow(ctx, `callback:${action}`);
      resetSession(ctx);
      if (!messageId) {
        await ctx.reply("❌ Missing email message id.");
        finishMetric("invalid");
        return;
      }
      await sendEmailStatusCard(ctx, messageId);
      finishMetric("ok");
      return;
    }

    if (action.startsWith("EMAIL_TIMELINE:")) {
      const [, messageId] = action.split(":");
      await cancelActiveFlow(ctx, `callback:${action}`);
      resetSession(ctx);
      if (!messageId) {
        await ctx.reply("❌ Missing email message id.");
        finishMetric("invalid");
        return;
      }
      await sendEmailTimeline(ctx, messageId);
      finishMetric("ok");
      return;
    }

    if (action.startsWith("EMAIL_BULK:")) {
      const [, jobId] = action.split(":");
      await cancelActiveFlow(ctx, `callback:${action}`);
      resetSession(ctx);
      if (!jobId) {
        await ctx.reply("❌ Missing bulk job id.");
        finishMetric("invalid");
        return;
      }
      await sendBulkStatusCard(ctx, jobId);
      finishMetric("ok");
      return;
    }

    if (recoveryTarget) {
      const scriptDesignerHandled = await handleScriptDesignerCallbackRouting(
        ctx,
        action,
        recoveryTarget,
        {
          isMenuExemptAction,
          finishMetric,
        },
      );
      if (scriptDesignerHandled) {
        return;
      }

      const hasActiveOp = Boolean(ctx.session?.currentOp?.id);
      const hasMatchingOpToken = hasMatchingConversationOpToken(
        ctx,
        recoveryTarget,
      );
      const hasActiveRecoveryConversation = isConversationTargetActive(
        ctx,
        recoveryTarget.conversationTarget,
      );

      // Conversation-scoped callback reached the global router while the same op is active.
      // Keep current flow state and avoid noisy forced restarts.
      if (hasActiveOp && hasMatchingOpToken) {
        finishMetric("ignored", { reason: "active_op_router_leak" });
        return;
      }

      // For menu-exempt conversation actions (script designer, persona, etc.) without
      // operation tokens, let the active conversation handle the callback even if the
      // operation token doesn't match. This preserves restart-safe callback behavior where
      // callbacks intentionally don't bind to operation tokens.
      if (
        isMenuExemptAction &&
        hasActiveRecoveryConversation &&
        !hasMatchingOpToken
      ) {
        finishMetric("ignored", {
          reason: "menu_exempt_conversation_listener",
        });
        return;
      }

      // No active op: route back to a fresh menu instead of trying to resurrect stale flow state.
      if (!hasActiveOp) {
        if (hasActiveRecoveryConversation) {
          finishMetric("ignored", {
            reason: "active_conversation_router_leak",
          });
          return;
        }
        await clearMenuMessages(ctx);
        await handleMenu(ctx);
        finishMetric("stale", { reason: "missing_active_op" });
        return;
      }

      // Active op exists but token mismatched: stale button from another op, ignore safely.
      finishMetric("stale", { reason: "op_token_mismatch" });
      return;
    }

    // Guard against future conversation callback prefixes that are not yet in
    // the explicit recovery map. If the callback is session-bound and token-matched,
    // let the active conversation own it and keep the global router side-effect free.
    if (isSessionBoundAction) {
      const callbackOpToken = normalizeOpToken(action.split(":")[1]);
      const currentOpToken = ctx.session?.currentOp?.token || null;
      if (
        callbackOpToken &&
        currentOpToken &&
        callbackOpToken === currentOpToken
      ) {
        finishMetric("ignored", { reason: "session_bound_router_leak" });
        return;
      }
    }

    // Handle conversation actions
    const conversations = {
      CALL: "call-conversation",
      ADDUSER: "adduser-conversation",
      PROMOTE: "promote-conversation",
      REMOVE: "remove-conversation",
      SMS_SEND: "sms-conversation",
      SMS_SCHEDULE: "schedule-sms-conversation",
      SMS_STATUS: "sms-status-conversation",
      SMS_CONVO: "sms-thread-conversation",
      SMS_RECENT: "sms-recent-conversation",
      SMS_STATS: "sms-stats-conversation",
      BULK_SMS_SEND: "bulk-sms-conversation",
      BULK_SMS_STATUS: "bulk-sms-status-conversation",
      EMAIL_SEND: "email-conversation",
      EMAIL_STATUS: "email-status-conversation",
      EMAIL_TEMPLATES: "email-templates-conversation",
      BULK_EMAIL_SEND: "bulk-email-conversation",
      BULK_EMAIL_STATUS: "bulk-email-status-conversation",
      BULK_EMAIL_LIST: "bulk-email-history-conversation",
      BULK_EMAIL_STATS: "bulk-email-stats-conversation",
      CALLLOG_RECENT: "calllog-recent-conversation",
      CALLLOG_SEARCH: "calllog-search-conversation",
      CALLLOG_DETAILS: "calllog-details-conversation",
      CALLLOG_EVENTS: "calllog-events-conversation",
      SCRIPTS: "scripts-conversation",
      PERSONA: "persona-conversation",
      CALLER_FLAGS_ALLOW: "callerflag-allow-conversation",
      CALLER_FLAGS_BLOCK: "callerflag-block-conversation",
      CALLER_FLAGS_SPAM: "callerflag-spam-conversation",
    };

    if (conversations[action]) {
      console.log(`Starting conversation: ${conversations[action]}`);
      await cancelActiveFlow(ctx, `callback:${action}`);
      await clearMenuMessages(ctx);
      startOperation(ctx, action.toLowerCase());
      const conversationLabels = {
        CALLLOG_RECENT: "call log (recent)",
        CALLLOG_SEARCH: "call log (search)",
        CALLLOG_DETAILS: "call details lookup",
        CALLLOG_EVENTS: "call event lookup",
        BULK_EMAIL_LIST: "bulk email history",
        BULK_EMAIL_STATS: "bulk email stats",
        SMS_STATUS: "SMS status",
        SMS_CONVO: "SMS conversation",
        SMS_RECENT: "recent SMS",
        SMS_STATS: "SMS stats",
        CALLER_FLAGS_ALLOW: "caller allowlist",
        CALLER_FLAGS_BLOCK: "caller blocklist",
        CALLER_FLAGS_SPAM: "spam flag",
      };
      const label =
        conversationLabels[action] || action.toLowerCase().replace(/_/g, " ");
      await ctx.reply(`Starting ${label}...`);
      await ctx.conversation.enter(conversations[action]);
      finishMetric("ok");
      return;
    }

    // Handle direct command actions
    await cancelActiveFlow(ctx, `callback:${action}`);
    resetSession(ctx);
    await clearMenuMessages(ctx);

    switch (action) {
      case "HELP":
        await handleHelp(ctx);
        finishMetric("ok");
        break;

      case "USERS":
        try {
          await renderUsersMenu(ctx);
          finishMetric("ok");
        } catch (usersError) {
          console.error("Users callback error:", usersError);
          await ctx.reply("❌ Error displaying users list. Please try again.");
          finishMetric("error", {
            error: usersError?.message || String(usersError),
          });
        }
        break;

      case "USERS_LIST":
        try {
          await sendUsersList(ctx);
          finishMetric("ok");
        } catch (usersError) {
          console.error("Users list callback error:", usersError);
          await ctx.reply("❌ Error displaying users list. Please try again.");
          finishMetric("error", {
            error: usersError?.message || String(usersError),
          });
        }
        break;

      case "CALLER_FLAGS":
        try {
          await renderCallerFlagsMenu(ctx);
          finishMetric("ok");
        } catch (flagsError) {
          console.error("Caller flags menu error:", flagsError);
          await ctx.reply(
            "❌ Error displaying caller flags menu. Please try again.",
          );
          finishMetric("error", {
            error: flagsError?.message || String(flagsError),
          });
        }
        break;

      case "CALLER_FLAGS_LIST":
        try {
          await sendCallerFlagsList(ctx);
          finishMetric("ok");
        } catch (flagsError) {
          console.error("Caller flags list error:", flagsError);
          await ctx.reply("❌ Error fetching caller flags. Please try again.");
          finishMetric("error", {
            error: flagsError?.message || String(flagsError),
          });
        }
        break;

      case "GUIDE":
        await handleGuide(ctx);
        finishMetric("ok");
        break;

      case "MENU":
        await handleMenu(ctx);
        finishMetric("ok");
        break;

      case "MENU_EXIT":
        await ctx.reply(
          "✅ Menu closed. Use /menu or /start to open it again.",
        );
        finishMetric("ok");
        break;

      case "HEALTH":
        await handleHealthCommand(ctx);
        finishMetric("ok");
        break;

      case "STATUS":
        await handleStatusCommand(ctx);
        finishMetric("ok");
        break;

      case "PROVIDER_STATUS":
        await renderProviderMenu(ctx, { forceRefresh: true });
        finishMetric("ok");
        break;

      case "PROVIDER:HOME":
        await renderProviderMenu(ctx, { forceRefresh: true });
        finishMetric("ok");
        break;

      case "REQUEST_ACCESS": {
        const adminUsername = (config.admin.username || "").replace(/^@/, "");
        if (!adminUsername) {
          await ctx.reply(
            "ℹ️ Access requests are enabled, but no admin username is configured.",
          );
          finishMetric("ok");
          break;
        }
        const accessKb = new InlineKeyboard()
          .url("📩 Request Access", `https://t.me/${adminUsername}`)
          .row()
          .text("⬅️ Main Menu", buildCallbackData(ctx, "MENU"));
        await ctx.reply("📩 Contact the admin to request access:", {
          reply_markup: accessKb,
        });
        finishMetric("ok");
        break;
      }

      case "CALLLOG":
        await renderCalllogMenu(ctx);
        finishMetric("ok");
        break;

      case "SMS":
        await renderSmsMenu(ctx);
        finishMetric("ok");
        break;

      case "EMAIL":
        await renderEmailMenu(ctx);
        finishMetric("ok");
        break;

      case "BULK_SMS":
        await renderBulkSmsMenu(ctx);
        finishMetric("ok");
        break;

      case "BULK_EMAIL":
        await renderBulkEmailMenu(ctx);
        finishMetric("ok");
        break;

      case "SCHEDULE_SMS":
        await renderSmsMenu(ctx);
        finishMetric("ok");
        break;

      case "BULK_SMS_LIST":
        await sendBulkSmsList(ctx);
        finishMetric("ok");
        break;

      case "BULK_SMS_STATS":
        await sendBulkSmsStats(ctx);
        finishMetric("ok");
        break;

      case "EMAIL_HISTORY":
        await emailHistoryFlow(ctx);
        finishMetric("ok");
        break;

      case "RECENT_SMS":
        await sendRecentSms(ctx, 10);
        finishMetric("ok");
        break;

      default:
        if (action.includes(":")) {
          console.log(`Stale callback action: ${action}`);
          await ctx.reply(
            "⚠️ That menu is no longer active. Use /menu to start again.",
          );
          finishMetric("stale");
        } else {
          console.log(`Unknown callback action: ${action}`);
          await ctx.reply("❌ Unknown action. Please try again.");
          finishMetric("unknown");
        }
    }
  } catch (error) {
    console.error("Callback query error:", error);
    await safeAnswerCallbackQuery(ctx, {
      text: "⚠️ Failed to process action",
      show_alert: false,
    });
    let isGuest = false;
    try {
      const access = await getAccessProfile(ctx);
      isGuest = !access?.user;
    } catch (_) {
      isGuest = false;
    }
    const fallback = isGuest
      ? "🔒 This option is not available in guest mode. Use Request Access to unlock actions."
      : "❌ An error occurred processing your request. Please try again.";
    const message = isGuest ? fallback : error?.userMessage || fallback;
    await ctx.reply(message);
    finishMetric("error", { error: error?.message || String(error) });
  }
});

const TELEGRAM_COMMANDS = [
  { command: "start", description: "Start or restart the bot" },
  { command: "help", description: "Show available commands" },
  { command: "menu", description: "Show quick action menu" },
  { command: "guide", description: "Show detailed usage guide" },
  { command: "health", description: "Check bot and API health" },
  { command: "call", description: "Start outbound voice call" },
  { command: "calllog", description: "Call history and search" },
  { command: "sms", description: "Open SMS center" },
  { command: "email", description: "Open Email center" },
  { command: "smssender", description: "Bulk SMS center (admin only)" },
  { command: "mailer", description: "Bulk email center (admin only)" },
  { command: "scripts", description: "Manage call & SMS scripts (admin only)" },
  { command: "persona", description: "Manage personas (admin only)" },
  { command: "provider", description: "Manage call provider (admin only)" },
  { command: "callerflags", description: "Manage caller flags (admin only)" },
  { command: "users", description: "Manage users (admin only)" },
  { command: "status", description: "System status (admin only)" },
];

const TELEGRAM_COMMANDS_GUEST = [
  { command: "start", description: "Start or restart the bot" },
  { command: "help", description: "Learn how the bot works" },
  { command: "menu", description: "Browse the feature menu" },
  { command: "guide", description: "View the user guide" },
];

const TELEGRAM_COMMANDS_USER = [
  { command: "start", description: "Start or restart the bot" },
  { command: "help", description: "Show available commands" },
  { command: "menu", description: "Show quick action menu" },
  { command: "guide", description: "Show detailed usage guide" },
  { command: "health", description: "Check bot and API health" },
  { command: "call", description: "Start outbound voice call" },
  { command: "calllog", description: "Call history and search" },
  { command: "sms", description: "Open SMS center" },
  { command: "email", description: "Open Email center" },
];
const COMMAND_SYNC_DEBOUNCE_MS = 60 * 1000;
const COMMAND_SYNC_RETENTION_MS = 10 * 60 * 1000;
const COMMAND_SYNC_MAX_CHATS = 5000;
const commandSyncState = new Map();

function buildCommandsFingerprint(commands = []) {
  return commands
    .map((item) => `${item.command}:${item.description}`)
    .join("|");
}

function pruneCommandSyncState(now = Date.now()) {
  for (const [chatId, state] of commandSyncState.entries()) {
    if (
      !state ||
      now - Number(state.updatedAt || 0) > COMMAND_SYNC_RETENTION_MS
    ) {
      commandSyncState.delete(chatId);
    }
  }
  if (commandSyncState.size <= COMMAND_SYNC_MAX_CHATS) {
    return;
  }
  const overflow = commandSyncState.size - COMMAND_SYNC_MAX_CHATS;
  const oldest = Array.from(commandSyncState.entries())
    .sort((a, b) => Number(a[1]?.updatedAt || 0) - Number(b[1]?.updatedAt || 0))
    .slice(0, overflow);
  for (const [chatId] of oldest) {
    commandSyncState.delete(chatId);
  }
}

async function syncChatCommands(ctx, access) {
  if (!ctx.chat || ctx.chat.type !== "private") {
    return;
  }
  const now = Date.now();
  pruneCommandSyncState(now);
  const chatId = String(ctx.chat.id);
  const commands = access.user
    ? access.isAdmin
      ? TELEGRAM_COMMANDS
      : TELEGRAM_COMMANDS_USER
    : TELEGRAM_COMMANDS_GUEST;
  const fingerprint = buildCommandsFingerprint(commands);
  const cached = commandSyncState.get(chatId);
  if (
    cached &&
    cached.fingerprint === fingerprint &&
    now - cached.updatedAt < COMMAND_SYNC_DEBOUNCE_MS
  ) {
    return;
  }
  try {
    await bot.api.setMyCommands(commands, {
      scope: { type: "chat", chat_id: ctx.chat.id },
    });
    commandSyncState.set(chatId, {
      fingerprint,
      updatedAt: now,
    });
  } catch (error) {
    console.warn("Failed to sync chat commands:", error?.message || error);
  }
}

// Handle unknown commands and text messages
bot.on("message:text", async (ctx) => {
  const text = ctx.message.text;

  // Skip if it's a command that's handled elsewhere
  if (text.startsWith("/")) {
    return;
  }

  // Let active conversation flows control their own prompts/replies.
  if (hasActiveConversation(ctx)) {
    return;
  }
  // Ignore ad-hoc text while an operation is still live to prevent
  // command fallback messages from interrupting in-flight conversations.
  if (ctx.session?.currentOp?.id || ctx.session?.flow?.name) {
    console.log(
      JSON.stringify({
        type: "message_text_deferred",
        reason: "active_operation_or_flow",
        user_id: ctx.from?.id || null,
        chat_id: ctx.chat?.id || null,
        op_id: ctx.session?.currentOp?.id || null,
        op_command: ctx.session?.currentOp?.command || null,
        flow_name: ctx.session?.flow?.name || null,
      }),
    );
    return;
  }

  await ctx.reply(
    "👋 Use the current buttons. You can also use /help or /menu.",
  );
});

async function bootstrap() {
  try {
    await validateTemplatesApiConnectivity();
  } catch (error) {
    console.error(`❌ ${error.message}`);
    process.exit(1);
  }

  console.log("🚀 Starting Voice Call Bot...");
  try {
    await bot.api.setMyCommands(TELEGRAM_COMMANDS_GUEST);
    console.log("✅ Telegram commands registered");
    await bot.start();
    console.log("✅ Voice Call Bot is running!");
    console.log("🔄 Polling for updates...");
  } catch (error) {
    console.error("❌ Failed to start bot:", error);
    process.exit(1);
  }
}

let isShuttingDown = false;
const FORCE_SHUTDOWN_MS = 15000;
const GRACEFUL_SHUTDOWN_TIMEOUT_MS = 10000;
let pendingQueryCount = 0;

/**
 * Register a pending database query
 * Call the returned function when the query completes
 */
function trackPendingQuery() {
  pendingQueryCount++;
  return () => {
    pendingQueryCount = Math.max(0, pendingQueryCount - 1);
  };
}

async function shutdown(signal, exitCode = 0) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  console.log(`🛑 Shutting down bot (${signal})...`);

  const forceTimer = setTimeout(() => {
    console.error(
      `⚠️ Forced shutdown after ${FORCE_SHUTDOWN_MS}ms (${signal}). ` +
        `Pending queries: ${pendingQueryCount}`,
    );
    process.exit(exitCode || 1);
  }, FORCE_SHUTDOWN_MS);

  if (typeof forceTimer.unref === "function") {
    forceTimer.unref();
  }

  try {
    // Step 1: Stop accepting new updates
    console.log("📍 Stopping bot polling...");
    await bot.stop();
    console.log("✅ Bot polling stopped");
  } catch (error) {
    console.error("Bot stop error:", error?.message || error);
  }

  try {
    // Step 2: Wait for pending database queries to drain
    console.log("📍 Draining pending queries...");
    const drainStartTime = Date.now();
    const maxDrainTime = GRACEFUL_SHUTDOWN_TIMEOUT_MS;

    while (
      pendingQueryCount > 0 &&
      Date.now() - drainStartTime < maxDrainTime
    ) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    if (pendingQueryCount > 0) {
      console.warn(
        `⚠️ Shutdown timeout: ${pendingQueryCount} pending queries still running ` +
          `after ${maxDrainTime}ms`,
      );
    } else {
      console.log(`✅ All pending queries drained`);
    }
  } catch (error) {
    console.error("Query drain error:", error?.message || error);
  }

  try {
    // Step 3: Close database connections
    console.log("📍 Closing database...");
    await closeDb();
    console.log("✅ Database closed");
  } catch (error) {
    console.error("Database shutdown error:", error?.message || error);
  } finally {
    clearTimeout(forceTimer);
    console.log(`✅ Shutdown complete`);
  }
  process.exit(exitCode);
}

process.on("SIGINT", () => {
  void shutdown("SIGINT");
});

process.on("SIGTERM", () => {
  void shutdown("SIGTERM");
});

process.on("unhandledRejection", (reason) => {
  console.error("Unhandled promise rejection in bot:", reason);
  void shutdown("unhandledRejection", 1);
});

process.on("uncaughtException", (error) => {
  console.error("Uncaught exception in bot:", error);
  void shutdown("uncaughtException", 1);
});

bootstrap();
