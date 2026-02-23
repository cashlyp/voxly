const { randomUUID } = require("crypto");

class OperationCancelledError extends Error {
  constructor(reason = "Operation cancelled") {
    super(reason);
    this.name = "OperationCancelledError";
  }
}

class FlowContext {
  constructor(name, ttlMs = 10 * 60 * 1000, seed = {}) {
    this.name = name;
    this.ttlMs = ttlMs;
    const created =
      typeof seed.createdAt === "number" ? seed.createdAt : Date.now();
    const updated =
      typeof seed.updatedAt === "number" ? seed.updatedAt : created;
    this.createdAt = created;
    this.updatedAt = updated;
    this.step = seed.step || null;
    this.state = seed.state || {};
  }

  get expired() {
    return Date.now() - this.updatedAt > this.ttlMs;
  }

  touch(step = null) {
    this.updatedAt = Date.now();
    if (step) {
      this.step = step;
    }
  }

  reset(name = this.name) {
    this.name = name;
    const now = Date.now();
    this.createdAt = now;
    this.updatedAt = now;
    this.step = null;
    this.state = {};
  }
}

const initialSessionState = () => ({
  currentOp: null,
  lastCommand: null,
  pendingControllers: [],
  meta: {},
  flow: null,
  errors: [],
  menuMessages: [],
  actionHistory: {},
});

function ensureSession(ctx) {
  if (!ctx.session || typeof ctx.session !== "object") {
    ctx.session = initialSessionState();
  } else {
    ctx.session.currentOp = ctx.session.currentOp || null;
    ctx.session.pendingControllers = Array.isArray(
      ctx.session.pendingControllers,
    )
      ? ctx.session.pendingControllers
      : [];
    ctx.session.meta = ctx.session.meta || {};
    ctx.session.flow = ctx.session.flow || null;
    ctx.session.errors = Array.isArray(ctx.session.errors)
      ? ctx.session.errors
      : [];
    ctx.session.menuMessages = Array.isArray(ctx.session.menuMessages)
      ? ctx.session.menuMessages
      : [];
    ctx.session.actionHistory =
      ctx.session.actionHistory && typeof ctx.session.actionHistory === "object"
        ? ctx.session.actionHistory
        : {};
    if (
      ctx.session.currentOp &&
      ctx.session.currentOp.id &&
      !ctx.session.currentOp.token
    ) {
      ctx.session.currentOp.token = ctx.session.currentOp.id
        .replace(/-/g, "")
        .slice(0, 8);
    }
  }
}

function generateOpId() {
  if (typeof randomUUID === "function") {
    return randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function startOperation(ctx, command, metadata = {}) {
  ensureSession(ctx);
  const safeCommand = String(command || "").trim();
  const currentOp = ctx.session?.currentOp;
  if (
    currentOp &&
    currentOp.id &&
    safeCommand &&
    String(currentOp.command || "") === safeCommand
  ) {
    currentOp.startedAt = Date.now();
    if (metadata && Object.keys(metadata).length > 0) {
      currentOp.metadata = { ...(currentOp.metadata || {}), ...metadata };
    }
    ctx.session.currentOp = currentOp;
    ctx.session.lastCommand = safeCommand;
    return currentOp.id;
  }

  const opId = generateOpId();
  const opToken = opId.replace(/-/g, "").slice(0, 8);
  ctx.session.currentOp = {
    id: opId,
    token: opToken,
    command: safeCommand,
    metadata,
    startedAt: Date.now(),
  };
  ctx.session.lastCommand = safeCommand;
  return opId;
}

function getCurrentOpId(ctx) {
  return ctx.session?.currentOp?.id || null;
}

function isOperationActive(ctx, opId) {
  return Boolean(opId && ctx.session?.currentOp?.id === opId);
}

function registerAbortController(ctx, controller) {
  ensureSession(ctx);
  ctx.session.pendingControllers.push(controller);
  const release = () => {
    ctx.session.pendingControllers = ctx.session.pendingControllers.filter(
      (item) => item !== controller,
    );
  };
  return release;
}

async function cancelActiveFlow(ctx, reason = "reset") {
  ensureSession(ctx);
  if (ctx.session.pendingControllers.length > 0) {
    ctx.session.pendingControllers.forEach((controller) => {
      try {
        controller.abort(reason);
      } catch (error) {
        console.warn("Abort controller error:", error.message);
      }
    });
    ctx.session.pendingControllers = [];
  }

  if (ctx.conversation && typeof ctx.conversation.exit === "function") {
    try {
      await ctx.conversation.exit();
    } catch (error) {
      if (!/no conversation/i.test(error.message)) {
        console.warn("Conversation exit warning:", error.message);
      }
    }
  }

  ctx.session.currentOp = null;
  ctx.session.meta = {};
  ctx.session.flow = null;
}

function resetSession(ctx) {
  ensureSession(ctx);
  ctx.session.currentOp = null;
  ctx.session.lastCommand = null;
  ctx.session.meta = {};
  ctx.session.pendingControllers = [];
  ctx.session.flow = null;
  ctx.session.errors = [];
}

/**
 * Clean up expired flows from a session
 * @param {Object} ctx - Telegram context
 * @returns {Number} - Number of flows cleaned up
 */
function cleanupExpiredFlows(ctx) {
  if (!ctx?.session?.flow) {
    return 0;
  }

  if (ctx.session.flow.expired) {
    console.log(
      `Cleanup: Expired flow ${ctx.session.flow.name} for user ${ctx.from?.id}`,
    );
    ctx.session.flow = null;
    return 1;
  }
  return 0;
}

/**
 * Clean up abandoned abort controllers
 * Removes controllers that are no longer functional
 * @param {Object} ctx - Telegram context
 * @returns {Number} - Number of controllers cleaned up
 */
function cleanupAbandonedControllers(ctx) {
  if (
    !ctx?.session?.pendingControllers ||
    ctx.session.pendingControllers.length === 0
  ) {
    return 0;
  }

  let cleaned = 0;
  ctx.session.pendingControllers = ctx.session.pendingControllers.filter(
    (controller) => {
      // Check if controller is still valid (has abort method)
      const isValid = controller && typeof controller.abort === "function";
      if (!isValid) {
        cleaned++;
      }
      return isValid;
    },
  );

  if (cleaned > 0) {
    console.log(
      `Cleanup: Removed ${cleaned} abandoned controllers for user ${ctx.from?.id}`,
    );
  }
  return cleaned;
}

/**
 * Dump session memory stats for monitoring
 * @param {Object} ctx - Telegram context
 * @returns {Object} - Stats object
 */
function getSessionMemoryStats(ctx) {
  if (!ctx?.session) {
    return { totalSessions: 0, sessionsWithFlow: 0 };
  }

  const stats = {
    hasCurrentOp: Boolean(ctx.session.currentOp),
    controllersCount: (ctx.session.pendingControllers || []).length,
    hasFlow: Boolean(ctx.session.flow),
    flowExpired: ctx.session.flow?.expired || null,
    menuMessagesCount: (ctx.session.menuMessages || []).length,
    errorsCount: (ctx.session.errors || []).length,
  };

  return stats;
}

function ensureOperationActive(ctx, opId) {
  if (!isOperationActive(ctx, opId)) {
    throw new OperationCancelledError();
  }
}

function ensureFlow(ctx, name, options = {}) {
  ensureSession(ctx);
  const ttlMs =
    typeof options.ttlMs === "number" && options.ttlMs > 0
      ? options.ttlMs
      : 10 * 60 * 1000;
  const step = options.step || null;

  let flow = ctx.session.flow;

  const needsRehydrate =
    flow &&
    (typeof flow !== "object" ||
      typeof flow.touch !== "function" ||
      typeof flow.reset !== "function" ||
      typeof flow.expired !== "boolean"); // calling getter converts to boolean

  if (!flow || flow.name !== name || needsRehydrate) {
    const seed = flow && typeof flow === "object" ? flow : {};
    flow = new FlowContext(name, ttlMs, seed);
  } else if (typeof flow.ttlMs !== "number" || flow.ttlMs !== ttlMs) {
    flow.ttlMs = ttlMs;
  }

  if (flow.expired) {
    flow.reset(name);
  }

  flow.touch(step);
  ctx.session.flow = flow;
  return flow;
}

async function safeReset(ctx, reason = "reset", options = {}) {
  const {
    message = "⚠️ Session expired. Restarting call setup...",
    menuHint = "📋 Use /menu to start again.",
    notify = true,
  } = options;

  ensureSession(ctx);
  await cancelActiveFlow(ctx, reason);
  resetSession(ctx);

  if (!notify) {
    return;
  }

  const lines = [];
  if (message) {
    lines.push(message);
  }
  if (menuHint) {
    lines.push(menuHint);
  }

  if (lines.length > 0) {
    try {
      await ctx.reply(lines.join("\n"));
    } catch (error) {
      console.warn("safeReset reply failed:", error.message);
    }
  }
}

function isSlashCommandInput(text) {
  if (typeof text !== "string") {
    return false;
  }
  const trimmed = text.trim();
  return trimmed.startsWith("/") && trimmed.length > 1;
}

async function guardAgainstCommandInterrupt(
  ctx,
  text,
  reason = "command_interrupt",
) {
  if (!isSlashCommandInput(text)) {
    return;
  }
  await safeReset(ctx, reason, { notify: false });
  throw new OperationCancelledError(
    "Conversation interrupted by slash command",
  );
}

async function waitForConversationText(conversation, ctx, options = {}) {
  const {
    ensureActive,
    allowEmpty = false,
    guardCommands = true,
    invalidMessage = "⚠️ Please send a text response to continue.",
    emptyMessage = "⚠️ Please send a non-empty response to continue.",
    timeoutMs = 30 * 60 * 1000, // Default 30 minutes for interactive conversations
    timeoutMessage = "⏱️ Response timeout. Starting over...",
  } = options;

  const startTime = Date.now();

  async function waitWithTimeout() {
    while (true) {
      // Check timeout before each iteration
      if (timeoutMs > 0 && Date.now() - startTime > timeoutMs) {
        throw new OperationCancelledError(
          `Conversation timeout after ${timeoutMs}ms`,
        );
      }

      // Create a timeout promise for this iteration
      const remainingMs =
        timeoutMs > 0
          ? Math.max(0, timeoutMs - (Date.now() - startTime))
          : timeoutMs;
      let update;

      if (timeoutMs > 0 && remainingMs <= 0) {
        throw new OperationCancelledError(
          `Conversation timeout after ${timeoutMs}ms`,
        );
      }

      try {
        if (timeoutMs > 0) {
          // Use Promise.race with a timeout and clear timer eagerly.
          let timeoutHandle = null;
          try {
            update = await Promise.race([
              conversation.wait(),
              new Promise((_, reject) => {
                timeoutHandle = setTimeout(
                  () => reject(new OperationCancelledError("wait() timeout")),
                  remainingMs,
                );
              }),
            ]);
          } finally {
            if (timeoutHandle) {
              clearTimeout(timeoutHandle);
            }
          }
        } else {
          update = await conversation.wait();
        }
      } catch (error) {
        if (
          error instanceof OperationCancelledError ||
          error?.message?.includes("timeout")
        ) {
          throw new OperationCancelledError(
            `Conversation timeout after ${timeoutMs}ms`,
          );
        }
        throw error;
      }

      if (typeof ensureActive === "function") {
        ensureActive();
      }

      const rawText = update?.message?.text;
      if (typeof rawText !== "string") {
        if (
          update?.callbackQuery?.id &&
          typeof update.answerCallbackQuery === "function"
        ) {
          try {
            await update.answerCallbackQuery({
              text: "Please reply in chat with text.",
              show_alert: false,
            });
          } catch (_) {
            // ignore callback answer failures
          }
        }
        if (invalidMessage) {
          await ctx.reply(invalidMessage);
        }
        continue;
      }

      const text = rawText.trim();
      if (!text && !allowEmpty) {
        if (emptyMessage) {
          await ctx.reply(emptyMessage);
        }
        continue;
      }

      if (guardCommands && text) {
        await guardAgainstCommandInterrupt(ctx, text);
      }

      return { update, text };
    }
  }

  try {
    return await waitWithTimeout();
  } catch (error) {
    if (
      error instanceof OperationCancelledError &&
      error.message.includes("timeout")
    ) {
      if (timeoutMessage) {
        try {
          await ctx.reply(timeoutMessage);
        } catch (_) {
          // Ignore reply errors after timeout
        }
      }
      throw error;
    }
    throw error;
  }
}

module.exports = {
  initialSessionState,
  startOperation,
  cancelActiveFlow,
  getCurrentOpId,
  isOperationActive,
  registerAbortController,
  resetSession,
  ensureSession,
  ensureOperationActive,
  ensureFlow,
  safeReset,
  guardAgainstCommandInterrupt,
  waitForConversationText,
  isSlashCommandInput,
  FlowContext,
  OperationCancelledError,
  cleanupExpiredFlows,
  cleanupAbandonedControllers,
  getSessionMemoryStats,
};
