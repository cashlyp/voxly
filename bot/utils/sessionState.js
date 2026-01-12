const { randomUUID } = require('crypto');

class OperationCancelledError extends Error {
  constructor(reason = 'Operation cancelled') {
    super(reason);
    this.name = 'OperationCancelledError';
  }
}

class FlowContext {
  constructor(name, ttlMs = 10 * 60 * 1000, seed = {}) {
    this.name = name;
    this.ttlMs = ttlMs;
    const created = typeof seed.createdAt === 'number' ? seed.createdAt : Date.now();
    const updated = typeof seed.updatedAt === 'number' ? seed.updatedAt : created;
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
  errors: []
});

function ensureSession(ctx) {
  if (!ctx.session || typeof ctx.session !== 'object') {
    ctx.session = initialSessionState();
  } else {
    ctx.session.currentOp = ctx.session.currentOp || null;
    ctx.session.pendingControllers = ctx.session.pendingControllers || [];
    ctx.session.meta = ctx.session.meta || {};
    ctx.session.flow = ctx.session.flow || null;
    ctx.session.errors = Array.isArray(ctx.session.errors) ? ctx.session.errors : [];
  }
}

function generateOpId() {
  if (typeof randomUUID === 'function') {
    return randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function startOperation(ctx, command, metadata = {}) {
  ensureSession(ctx);
  const opId = generateOpId();
  ctx.session.currentOp = {
    id: opId,
    command,
    metadata,
    startedAt: Date.now()
  };
  ctx.session.lastCommand = command;
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
    ctx.session.pendingControllers = ctx.session.pendingControllers.filter((item) => item !== controller);
  };
  return release;
}

async function cancelActiveFlow(ctx, reason = 'reset') {
  ensureSession(ctx);
  if (ctx.session.pendingControllers.length > 0) {
    ctx.session.pendingControllers.forEach((controller) => {
      try {
        controller.abort(reason);
      } catch (error) {
        console.warn('Abort controller error:', error.message);
      }
    });
    ctx.session.pendingControllers = [];
  }

  if (ctx.conversation && typeof ctx.conversation.exit === 'function') {
    try {
      await ctx.conversation.exit();
    } catch (error) {
      if (!/no conversation/i.test(error.message)) {
        console.warn('Conversation exit warning:', error.message);
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

function ensureOperationActive(ctx, opId) {
  if (!isOperationActive(ctx, opId)) {
    throw new OperationCancelledError();
  }
}

function ensureFlow(ctx, name, options = {}) {
  ensureSession(ctx);
  const ttlMs = typeof options.ttlMs === 'number' && options.ttlMs > 0 ? options.ttlMs : 10 * 60 * 1000;
  const step = options.step || null;

  let flow = ctx.session.flow;

  const needsRehydrate =
    flow &&
    (typeof flow !== 'object' ||
      typeof flow.touch !== 'function' ||
      typeof flow.reset !== 'function' ||
      typeof flow.expired !== 'boolean'); // calling getter converts to boolean

  if (!flow || flow.name !== name || needsRehydrate) {
    const seed = flow && typeof flow === 'object' ? flow : {};
    flow = new FlowContext(name, ttlMs, seed);
  } else if (typeof flow.ttlMs !== 'number' || flow.ttlMs !== ttlMs) {
    flow.ttlMs = ttlMs;
  }

  if (flow.expired) {
    flow.reset(name);
  }

  flow.touch(step);
  ctx.session.flow = flow;
  return flow;
}

async function safeReset(ctx, reason = 'reset', options = {}) {
  const {
    message = '⚠️ Session expired. Restarting call setup...',
    menuHint = '📋 Use /menu to start again.',
    notify = true
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
      await ctx.reply(lines.join('\n'));
    } catch (error) {
      console.warn('safeReset reply failed:', error.message);
    }
  }
}

function isSlashCommandInput(text) {
  if (typeof text !== 'string') {
    return false;
  }
  const trimmed = text.trim();
  return trimmed.startsWith('/') && trimmed.length > 1;
}

async function guardAgainstCommandInterrupt(ctx, text, reason = 'command_interrupt') {
  if (!isSlashCommandInput(text)) {
    return;
  }
  await safeReset(ctx, reason, { notify: false });
  throw new OperationCancelledError('Conversation interrupted by slash command');
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
  isSlashCommandInput,
  FlowContext,
  OperationCancelledError
};
