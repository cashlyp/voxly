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
  isDuplicateAction,
  startActionMetric,
  finishActionMetric,
} = require("./utils/actions");
const { normalizeReply, logCommandError } = require("./utils/ui");
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

// When a new slash command arrives, cancel any active flow first
bot.use(async (ctx, next) => {
  const text = ctx.message?.text || ctx.callbackQuery?.data;
  if (text && text.startsWith("/")) {
    const command = text.split(" ")[0].toLowerCase();
    if (command !== "/cancel") {
      await cancelActiveFlow(ctx, `command:${command}`);
      await clearMenuMessages(ctx);
    }
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
  ctx.reply = (text, options = {}) => {
    const normalized = normalizeReply(text, options);
    return originalReply(normalized.text, normalized.options);
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
  const parts = data.split(":");
  if (parts.length < 3) return;
  const action = parts[1];
  const callSid = parts[2];

  try {
    const allowed = await requireCapability(ctx, "call", {
      actionLabel: "Call controls",
    });
    if (!allowed) {
      await ctx.answerCallbackQuery({
        text: "Access required.",
        show_alert: false,
      });
      return;
    }
    switch (action) {
      case "mute":
        await httpClient.post(
          ctx,
          `${API_BASE}/api/calls/${callSid}/operator`,
          { action: "mute_alerts" },
          { timeout: 8000 },
        );
        await ctx.answerCallbackQuery({
          text: "🔕 Alerts muted for this call",
          show_alert: false,
        });
        break;
      case "retry":
        await httpClient.post(
          ctx,
          `${API_BASE}/api/calls/${callSid}/operator`,
          { action: "clarify", text: "Let me retry that step." },
          { timeout: 8000 },
        );
        await ctx.answerCallbackQuery({
          text: "🔄 Retry requested",
          show_alert: false,
        });
        break;
      case "transfer":
        await httpClient.post(
          ctx,
          `${API_BASE}/api/calls/${callSid}/operator`,
          { action: "transfer" },
          { timeout: 8000 },
        );
        await ctx.answerCallbackQuery({
          text: "📞 Transfer request noted",
          show_alert: false,
        });
        break;
      default:
        await ctx.answerCallbackQuery({
          text: "Action not supported yet",
          show_alert: false,
        });
        break;
    }
  } catch (error) {
    console.error("Operator action error:", error?.message || error);
    await ctx.answerCallbackQuery({
      text: "⚠️ Failed to execute action",
      show_alert: false,
    });
  }
});

// Live call console actions (proxy to API webhook handler)
bot.callbackQuery(/^lc:/, async (ctx) => {
  try {
    const allowed = await requireCapability(ctx, "calllog_view", {
      actionLabel: "Live call console",
    });
    if (!allowed) {
      await ctx.answerCallbackQuery({
        text: "Access required.",
        show_alert: false,
      });
      return;
    }
    await ctx.answerCallbackQuery();
    await httpClient.post(
      ctx,
      `${config.apiUrl}/webhook/telegram`,
      ctx.update,
      { timeout: 8000 },
    );
    return;
  } catch (error) {
    console.error("Live call action proxy error:", error?.message || error);
    await ctx.answerCallbackQuery({
      text: "⚠️ Failed to process action",
      show_alert: false,
    });
  }
});

// Initialize conversations middleware AFTER session
bot.use(conversations());

// Global error handler
bot.catch((err) => {
  const errorMessage = `Error while handling update ${err.ctx.update.update_id}:
    ${err.error.message}
    Stack: ${err.error.stack}`;
  console.error(errorMessage);

  try {
    err.ctx.reply("❌ An error occurred. Please try again or contact support.");
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
    if (response.data?.status && response.data.status !== "healthy") {
      throw new Error(`service reported status "${response.data.status}"`);
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
const { getUser, expireInactiveUsers } = require("./db/db");
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

function parseCallbackAction(action) {
  if (!action || !action.includes(":")) {
    return null;
  }
  const parts = action.split(":");
  const prefix = parts[0];
  if (parts.length >= 3 && /^[0-9a-fA-F-]{8,}$/.test(parts[1])) {
    return { prefix, opId: parts[1], value: parts.slice(2).join(":") };
  }
  return { prefix, opId: null, value: parts.slice(1).join(":") };
}

function resolveConversationFromPrefix(prefix) {
  if (!prefix) return null;
  if (prefix.startsWith("call-script-")) return "scripts-conversation";
  if (prefix === "call-script") return "call-conversation";
  if (prefix.startsWith("sms-script-")) return "scripts-conversation";
  if (prefix === "sms-script") return "sms-conversation";
  if (prefix.startsWith("script-") || prefix === "confirm")
    return "scripts-conversation";
  if (prefix.startsWith("email-template-"))
    return "email-templates-conversation";
  if (prefix.startsWith("bulk-email-")) return "bulk-email-conversation";
  if (prefix.startsWith("email-")) return "email-conversation";
  if (prefix.startsWith("bulk-sms-")) return "bulk-sms-conversation";
  if (prefix.startsWith("sms-")) return "sms-conversation";
  if (prefix.startsWith("persona-")) return "persona-conversation";
  if (
    ["persona", "purpose", "tone", "urgency", "tech", "call-config"].includes(
      prefix,
    )
  ) {
    return "call-conversation";
  }
  return null;
}

// Start command handler
bot.command("start", async (ctx) => {
  try {
    expireInactiveUsers();

    const access = await getAccessProfile(ctx);
    const isOwner = access.isAdmin;
    await syncChatCommands(ctx, access);

    const userStats = access.user
      ? `👤 *User Information*
• ID: \`${ctx.from.id}\`
• Username: @${ctx.from.username || "none"}
• Role: ${access.user.role}
• Joined: ${new Date(access.user.timestamp).toLocaleDateString()}`
      : `👤 *Guest Access*
• ID: \`${ctx.from.id}\`
• Username: @${ctx.from.username || "none"}
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
  const rawAction = ctx.callbackQuery.data;
  const metric = startActionMetric(ctx, "callback", { raw_action: rawAction });
  const finishMetric = (status, extra = {}) => {
    finishActionMetric(metric, status, extra);
  };
  try {
    if (rawAction && rawAction.startsWith("lc:")) {
      finishMetric("skipped");
      return;
    }
    const menuExemptPrefixes = ["alert:", "lc:"];
    const isMenuExempt = menuExemptPrefixes.some((prefix) =>
      rawAction.startsWith(prefix),
    );
    const validation = isMenuExempt
      ? { status: "ok", action: rawAction }
      : validateCallback(ctx, rawAction);
    if (validation.status !== "ok") {
      const message =
        validation.status === "expired"
          ? "⌛ This menu expired. Opening the latest view…"
          : "⚠️ This menu is no longer active.";
      await ctx.answerCallbackQuery({ text: message, show_alert: false });
      await clearMenuMessages(ctx);
      await handleMenu(ctx);
      finishMetric(validation.status, { reason: validation.reason });
      return;
    }

    const action = validation.action;
    const actionKey = `${action}|${ctx.callbackQuery?.message?.message_id || ""}`;
    if (isDuplicateAction(ctx, actionKey)) {
      await ctx.answerCallbackQuery({
        text: "Already processed.",
        show_alert: false,
      });
      finishMetric("duplicate");
      return;
    }

    // Answer callback query immediately to prevent timeout
    await ctx.answerCallbackQuery();
    console.log(`Callback query received: ${action} from user ${ctx.from.id}`);

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

    const isMenuExemptAction = menuExemptPrefixes.some((prefix) =>
      action.startsWith(prefix),
    );
    const menuMessageId = ctx.callbackQuery?.message?.message_id;
    const menuChatId = ctx.callbackQuery?.message?.chat?.id;
    const latestMenuId = getLatestMenuMessageId(ctx, menuChatId);
    if (!isMenuExemptAction && isLatestMenuExpired(ctx, menuChatId)) {
      await clearMenuMessages(ctx);
      await handleMenu(ctx);
      finishMetric("expired");
      return;
    }
    if (
      !isMenuExemptAction &&
      menuMessageId &&
      latestMenuId &&
      menuMessageId !== latestMenuId
    ) {
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
      const [, provider] = action.split(":");
      await cancelActiveFlow(ctx, `callback:${action}`);
      resetSession(ctx);
      await handleProviderSwitch(ctx, provider?.toLowerCase());
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

    const parsedCallback = parseCallbackAction(action);
    if (parsedCallback) {
      const conversationTarget = resolveConversationFromPrefix(
        parsedCallback.prefix,
      );
      if (conversationTarget) {
        const currentOpId = ctx.session?.currentOp?.id;
        if (
          !parsedCallback.opId ||
          !currentOpId ||
          parsedCallback.opId !== currentOpId
        ) {
          await cancelActiveFlow(ctx, `stale_callback:${action}`);
          resetSession(ctx);
          await ctx.reply("↩️ Reopening the menu so you can continue.");
          await ctx.conversation.enter(conversationTarget);
          finishMetric("stale");
        }
        finishMetric("routed");
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
    const fallback =
      "❌ An error occurred processing your request. Please try again.";
    const message = error?.userMessage || fallback;
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

async function syncChatCommands(ctx, access) {
  if (!ctx.chat || ctx.chat.type !== "private") {
    return;
  }
  const commands = access.user
    ? access.isAdmin
      ? TELEGRAM_COMMANDS
      : TELEGRAM_COMMANDS_USER
    : TELEGRAM_COMMANDS_GUEST;
  try {
    await bot.api.setMyCommands(commands, {
      scope: { type: "chat", chat_id: ctx.chat.id },
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

  // For non-command messages outside conversations
  if (!ctx.conversation) {
    await ctx.reply(
      "👋 Use /help to see available commands or /menu for quick actions.",
    );
  }
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
    await bot.api.setMyCommands(TELEGRAM_COMMANDS);
    console.log("✅ Telegram commands registered");
    await bot.start();
    console.log("✅ Voice Call Bot is running!");
    console.log("🔄 Polling for updates...");
  } catch (error) {
    console.error("❌ Failed to start bot:", error);
    process.exit(1);
  }
}

bootstrap();
