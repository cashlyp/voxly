require('colors');
const EventEmitter = require('events');
const crypto = require('crypto');
const OpenAI = require('openai');
const PersonalityEngine = require('../functions/PersonalityEngine');
const config = require('../config');

function estimateTokenCount(value = '') {
  return Math.ceil(String(value || '').length / 4);
}

function parseJsonSafe(value, fallback = null) {
  if (value == null) return fallback;
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch (_) {
    return fallback;
  }
}

function stableHash(value) {
  const body = typeof value === 'string' ? value : JSON.stringify(value || {});
  return crypto.createHash('sha256').update(body).digest('hex');
}

class EnhancedGptService extends EventEmitter {
  constructor(customPrompt = null, customFirstMessage = null, options = {}) {
    super();
    
    // Initialize OpenRouter-compatible OpenAI client
    if (!config.openRouter.apiKey) {
      throw new Error('OPENROUTER_API_KEY is not set. Please configure it to enable GPT responses.');
    }

    this.openai = new OpenAI({
      baseURL: "https://openrouter.ai/api/v1",
      apiKey: config.openRouter.apiKey,
      defaultHeaders: {
        "HTTP-Referer": config.openRouter.siteUrl,
        "X-Title": config.openRouter.siteName || "Adaptive Voice AI",
      }
    });
    
    this.model = config.openRouter.model;
    this.backupModel = config.openRouter.backupModel || null;
    this.maxTokens = config.openRouter.maxTokens || 160;
    this.fillerText = 'One moment, checking now.';
    this.stallTimeoutMs = 2000;
    this.responseTimeoutMs = config.openRouter.responseTimeoutMs || 25000;
    this.streamIdleTimeoutMs = config.openRouter.streamIdleTimeoutMs || 8000;
    this.toolExecutionTimeoutMs = Number(config.openRouter.toolExecutionTimeoutMs || 12000);
    this.toolRetryLimit = Number(config.openRouter.toolRetryLimit || 1);
    this.toolBudgetPerInteraction = Number(config.openRouter.toolBudgetPerInteraction || 4);
    this.toolIdempotencyTtlMs = Number(config.openRouter.toolIdempotencyTtlMs || 120000);
    this.strictToolSchemas = config.openRouter.strictToolSchemas !== false;
    this.toolIdempotency = new Map();
    this.toolBudget = new Map();
    this.toolRegistry = new Map();
    this.toolCircuitStates = new Map();
    this.toolExecutionLocks = new Map();
    this.latencyHistory = [];
    this.maxLatencySamples = 8;
    this.contextTokenBudget = Number(config.openRouter.contextTokenBudget || 3500);
    this.summaryMaxChars = Number(config.openRouter.summaryMaxChars || 1200);
    this.recentTurns = Number(config.openRouter.recentTurns || 10);
    this.memoryFactLimit = Number(config.openRouter.memoryFactLimit || 12);
    this.memoryFactMaxAgeDays = Number(config.openRouter.memoryFactMaxAgeDays || 14);
    this.summaryMinTurns = Number(config.openRouter.memorySummaryMinTurns || 10);
    this.summaryRollupBatch = Number(config.openRouter.memorySummaryRollupBatch || 6);
    this.maxToolLoops = Number(config.openRouter.maxToolLoops || 6);
    this.toolCircuitConfig = {
      enabled: config.openRouter?.toolCircuitBreaker?.enabled !== false,
      failureThreshold: Number(config.openRouter?.toolCircuitBreaker?.failureThreshold || 4),
      windowMs: Number(config.openRouter?.toolCircuitBreaker?.windowMs || 120000),
      cooldownMs: Number(config.openRouter?.toolCircuitBreaker?.cooldownMs || 90000)
    };
    this.personaConsistencyThreshold = Number(config.openRouter.personaConsistencyThreshold || 0.55);
    this.slo = {
      responseRttMs: Number(config.openRouter?.slo?.responseRttMs || 7000),
      ttfbMs: Number(config.openRouter?.slo?.ttfbMs || 2000),
      toolFailureRate: Number(config.openRouter?.slo?.toolFailureRate || 0.3)
    };
    this.brevityHint = 'Keep spoken replies concise: max 2 sentences, ~200 characters, and avoid rambling.';
    this.executionContext = {
      traceId: String(options.traceId || ''),
      requestId: String(options.requestId || ''),
      channel: String(options.channel || 'voice'),
      provider: String(options.provider || ''),
      startedAt: new Date().toISOString()
    };
    this.db = options.db || null;
    this.webhookService = options.webhookService || null;
    this.memoryLoaded = false;
    this.sessionSummary = '';
    this.summaryTurns = 0;
    this.longTermFacts = [];
    this.factKeys = new Set();
    this.rollupCursor = 0;
    this.toolExecutionStats = {
      total: 0,
      failed: 0
    };
    
    // Initialize Personality Engine
    this.personalityEngine = new PersonalityEngine();
    
    // Dynamic function system
    this.dynamicTools = [];
    this.availableFunctions = {};
    
    const defaultPrompt = 'You are an intelligent AI assistant capable of adapting to different business contexts and customer needs. Be professional, helpful, and responsive to customer communication styles. You must add a \'•\' symbol every 5 to 10 words at natural pauses where your response can be split for text to speech.';
    const defaultFirstMessage = 'Hello! How can I assist you today?';

    // Use custom prompt if provided, otherwise use default
    this.baseSystemPrompt = customPrompt || defaultPrompt;
    this.personalityPrompt = this.baseSystemPrompt;
    this.currentProfileName = 'general';
    this.currentProfilePrompt = '';
    this.callProfiles = {
      general: {
        name: 'general',
        prompt: 'Call profile: general assistance. Be courteous, concise, and stick to verifiable information.'
      },
      sales: {
        name: 'sales',
        prompt: 'Call profile: sales. Build quick rapport, discover the need, offer a clear next step/CTA, avoid guarantees or aggressive claims, keep answers short.'
      },
      support: {
        name: 'support',
        prompt: 'Call profile: support. Clarify the issue, confirm device/account if relevant, give step-by-step actions, avoid speculation, summarize next steps briefly.'
      },
      collections: {
        name: 'collections',
        prompt: 'Call profile: collections. Be firm yet respectful. Verify identity, state balance and due date calmly, offer payment options or to schedule, avoid threats or legal advice.'
      },
      verification: {
        name: 'verification',
        prompt: 'Call profile: verification. Purpose is identity/OTP/security checks. Never read or share codes or passwords. Prefer keypad entry; if spoken, accept only digits, acknowledge without repeating, and keep responses brief.'
      }
    };
    this.systemPrompt = this.composeSystemPrompt();
    const firstMessage = customFirstMessage || defaultFirstMessage;

    this.currentPhase = 'greeting';
    this.phaseWindows = {
      greeting: [],
      verification: [],
      resolution: [],
      closing: [],
      general: []
    };
    this.maxPerPhase = 8;
    this.metadataMessages = [];

    this.userContext = [
      { 'role': 'system', 'content': this.systemPrompt },
      { 'role': 'assistant', 'content': firstMessage },
    ];
    this.addToPhaseWindow({ role: 'assistant', content: firstMessage });
    
    this.partialResponseIndex = 0;
    this.conversationHistory = []; // Track full conversation for personality analysis

    // Store prompts for debugging/logging
    this.systemPrompt = this.composeSystemPrompt();
    this.firstMessage = firstMessage;
    this.isCustomConfiguration = !!(customPrompt || customFirstMessage);

    // Personality tracking
    this.personalityChanges = [];
    this.lastPersonalityUpdate = null;

    console.log('🎭 Enhanced GPT Service initialized with adaptive capabilities'.green);
    if (this.isCustomConfiguration) {
      console.log(`Custom prompt preview: ${this.baseSystemPrompt.substring(0, 100)}...`.cyan);
    }
  }

  getSanitizedTools() {
    if (!Array.isArray(this.dynamicTools) || this.dynamicTools.length === 0) {
      return [];
    }

    const sanitized = [];
    for (const tool of this.dynamicTools) {
      if (!tool || tool.type !== 'function' || !tool.function) {
        continue;
      }
      const { name, description, parameters } = tool.function;
      if (!name) {
        continue;
      }
      sanitized.push({
        type: 'function',
        function: {
          name,
          description,
          parameters
        }
      });
    }
    return sanitized;
  }

  composeSystemPrompt(basePrompt = null) {
    const personalityBlock = basePrompt || this.personalityPrompt || this.baseSystemPrompt;
    const personaDslPrompt = this.personalityEngine.getPersonaDslPrompt();
    const toneDirective = this.personalityEngine.buildAdaptiveToneDirective(this.personalityEngine.lastAnalysis || {});
    return [
      personalityBlock,
      this.currentProfilePrompt,
      personaDslPrompt,
      toneDirective,
      this.brevityHint
    ].filter(Boolean).join('\n');
  }

  setPhase(phaseName = 'greeting') {
    const normalized = String(phaseName || 'greeting').toLowerCase().trim();
    const allowed = ['greeting', 'verification', 'resolution', 'closing'];
    this.currentPhase = allowed.includes(normalized) ? normalized : 'greeting';
  }

  autoUpdatePhase(role, text, interactionCount) {
    if (role !== 'user') return;
    const body = String(text || '').toLowerCase();
    if (body.match(/code|otp|verify|verification|password|pin|passcode/)) {
      this.setPhase('verification');
      return;
    }
    if (interactionCount > 6 && body.match(/thank|thanks|bye|goodbye|that.s all|done/)) {
      this.setPhase('closing');
      return;
    }
    if (interactionCount >= 2 && this.currentPhase === 'greeting') {
      this.setPhase('resolution');
      return;
    }
  }

  setCallProfile(profileName = 'general') {
    const key = String(profileName || 'general').toLowerCase().trim();
    const profile = this.callProfiles[key] || this.callProfiles.general;
    this.currentProfileName = profile.name;
    this.currentProfilePrompt = profile.prompt;
    this.personalityEngine.setPersonaContext({ domain: profile.name });
    this.systemPrompt = this.composeSystemPrompt();
    this.updateSystemPromptWithPersonality(this.personalityPrompt);
    console.log(`💪 Call profile set: ${this.currentProfileName}`.blue);
  }

  // Set dynamic functions for this conversation
  setDynamicFunctions(tools, implementations) {
    const normalizedTools = Array.isArray(tools) ? tools : [];
    const normalizedImplementations = implementations && typeof implementations === 'object'
      ? implementations
      : {};
    this.toolRegistry.clear();
    this.dynamicTools = [];
    this.availableFunctions = {};

    for (const tool of normalizedTools) {
      const registered = this.registerToolDefinition(tool);
      if (!registered.ok) {
        this.logEvent('tool_registration_skipped', { reason: registered.error });
        continue;
      }
      const name = registered.tool.function.name;
      const implementation = normalizedImplementations[name];
      if (typeof implementation !== 'function') {
        this.logEvent('tool_registration_skipped', { tool: name, reason: 'missing_implementation' });
        continue;
      }
      this.dynamicTools.push(registered.tool);
      this.availableFunctions[name] = implementation;
      this.toolRegistry.set(name, registered.meta);
    }

    this.logEvent('tool_registry_loaded', {
      total_tools: this.dynamicTools.length,
      tools: Object.keys(this.availableFunctions)
    });
  }

  registerToolDefinition(tool) {
    if (!tool || tool.type !== 'function' || !tool.function?.name) {
      return { ok: false, error: 'invalid_tool_shape' };
    }
    const fn = { ...tool.function };
    fn.parameters = fn.parameters && typeof fn.parameters === 'object'
      ? fn.parameters
      : { type: 'object', properties: {}, required: [] };
    if (!fn.parameters.type) {
      fn.parameters.type = 'object';
    }
    if (!fn.parameters.properties || typeof fn.parameters.properties !== 'object') {
      fn.parameters.properties = {};
    }
    if (!Array.isArray(fn.parameters.required)) {
      fn.parameters.required = [];
    }
    const permission = String(fn.permission || fn.permissions || this.inferToolPermission(fn.name)).toLowerCase();
    const sideEffect = fn.side_effect === true || permission !== 'read' || this.isLikelySideEffectTool(fn.name);
    const fallback = fn.fallback_function || this.getDefaultToolFallback(fn.name);
    return {
      ok: true,
      tool: {
        type: 'function',
        function: {
          name: fn.name,
          description: fn.description,
          parameters: fn.parameters,
          say: fn.say,
          returns: fn.returns
        }
      },
      meta: {
        name: fn.name,
        permission,
        sideEffect,
        fallback,
        schema: fn.parameters
      }
    };
  }

  inferToolPermission(toolName = '') {
    return this.isLikelySideEffectTool(toolName) ? 'write' : 'read';
  }

  isLikelySideEffectTool(toolName = '') {
    const name = String(toolName || '').toLowerCase();
    return [
      'route_to_agent',
      'transfercall',
      'transfer_call',
      'placeorder',
      'place_order',
      'scheduleappointment',
      'schedule_appointment',
      'collect_digits',
      'collect_multiple_digits'
    ].includes(name);
  }

  getDefaultToolFallback(toolName = '') {
    const name = String(toolName || '').toLowerCase();
    if (name === 'transfercall' || name === 'transfer_call') {
      return 'route_to_agent';
    }
    return null;
  }

  // Add the callSid to the chat context
  setCallSid(callSid) {
    this.callSid = callSid;
    if (!this.executionContext.traceId && callSid) {
      this.executionContext.traceId = `call:${callSid}`;
    }
    this.memoryLoaded = false;
    this.metadataMessages = this.metadataMessages.filter((message) => {
      const content = String(message?.content || '');
      return !content.startsWith('callSid: ');
    });
    this.metadataMessages.push({ role: 'system', content: `callSid: ${callSid}` });
  }

  setCallIntent(intentLine = '') {
    const line = String(intentLine || '').trim();
    if (!line) return;
    this.metadataMessages.push({ role: 'system', content: line });
  }

  setCustomerName(customerName) {
    if (!customerName) return;
    this.metadataMessages.push({ role: 'system', content: `customerName: ${customerName}` });
  }

  setExecutionContext(context = {}) {
    if (!context || typeof context !== 'object') return;
    this.executionContext = {
      ...this.executionContext,
      ...context
    };
  }

  setTraceId(traceId) {
    if (!traceId) return;
    this.executionContext.traceId = String(traceId);
  }

  setPersonaContext(context = {}) {
    this.personalityEngine.setPersonaContext(context);
    this.systemPrompt = this.composeSystemPrompt(this.personalityPrompt);
    this.updateSystemPromptWithPersonality(this.personalityPrompt);
  }

  logEvent(eventName, payload = {}) {
    const traceId = this.executionContext.traceId || this.callSid || 'n/a';
    const body = {
      trace_id: traceId,
      call_sid: this.callSid || null,
      event: eventName,
      ...payload
    };
    console.log('gpt_event', body);
  }

  async logSloIfDegraded(ttfb, rtt) {
    const degraded = [];
    if (Number.isFinite(Number(ttfb)) && Number(ttfb) > this.slo.ttfbMs) {
      degraded.push(`ttfb>${this.slo.ttfbMs}`);
    }
    if (Number.isFinite(Number(rtt)) && Number(rtt) > this.slo.responseRttMs) {
      degraded.push(`rtt>${this.slo.responseRttMs}`);
    }
    const failureRate = this.toolExecutionStats.total > 0
      ? this.toolExecutionStats.failed / this.toolExecutionStats.total
      : 0;
    if (failureRate > this.slo.toolFailureRate) {
      degraded.push(`tool_failure_rate>${this.slo.toolFailureRate}`);
    }
    if (!degraded.length || !this.db?.logServiceHealth) return;
    try {
      await this.db.logServiceHealth('gpt_slo', 'degraded', {
        call_sid: this.callSid || null,
        trace_id: this.executionContext.traceId || null,
        degraded_checks: degraded,
        ttfb_ms: Number.isFinite(Number(ttfb)) ? Number(ttfb) : null,
        rtt_ms: Number.isFinite(Number(rtt)) ? Number(rtt) : null,
        tool_failure_rate: Number(failureRate.toFixed(3))
      });
    } catch (_) {
      // no-op
    }
  }

  async ensureMemoryLoaded() {
    if (this.memoryLoaded || !this.callSid || !this.db) {
      return;
    }
    this.memoryLoaded = true;
    try {
      const memory = await this.db.getCallMemory?.(this.callSid);
      if (memory?.summary) {
        this.sessionSummary = String(memory.summary || '');
      }
      if (Number.isFinite(Number(memory?.summary_turns))) {
        this.summaryTurns = Number(memory.summary_turns);
      }
      const facts = await this.db.listCallMemoryFacts?.(
        this.callSid,
        this.memoryFactLimit,
        this.memoryFactMaxAgeDays
      );
      if (Array.isArray(facts)) {
        for (const fact of facts) {
          const factKey = String(fact.fact_key || '');
          if (!factKey || this.factKeys.has(factKey)) continue;
          this.factKeys.add(factKey);
          this.longTermFacts.push({
            key: factKey,
            text: String(fact.fact_text || ''),
            confidence: Number(fact.confidence || 0.5),
            source: fact.source || 'persisted'
          });
        }
      }
    } catch (error) {
      this.logEvent('memory_load_failed', { error: error?.message || 'unknown' });
    }
  }

  extractLongTermFacts(role, text) {
    if (role !== 'user') return [];
    const raw = String(text || '').trim();
    if (!raw) return [];
    const candidates = [];
    const patterns = [
      { regex: /\bmy name is ([a-z][a-z .'-]{1,40})/i, key: 'customer_name', confidence: 0.9 },
      { regex: /\bi prefer ([a-z0-9 ,.'-]{2,80})/i, key: 'preference', confidence: 0.8 },
      { regex: /\bmy (?:account|card) ends with (\d{2,6})\b/i, key: 'account_tail', confidence: 0.85 },
      { regex: /\bthe issue is ([a-z0-9 ,.'-]{3,120})/i, key: 'issue_summary', confidence: 0.75 },
      { regex: /\bI need help with ([a-z0-9 ,.'-]{3,120})/i, key: 'need_summary', confidence: 0.7 }
    ];
    for (const pattern of patterns) {
      const match = raw.match(pattern.regex);
      if (!match || !match[1]) continue;
      const value = String(match[1]).trim();
      if (!value) continue;
      const factText = `${pattern.key}: ${value}`;
      const factKey = stableHash(`${pattern.key}:${value}`).slice(0, 24);
      candidates.push({
        key: factKey,
        text: factText,
        confidence: pattern.confidence,
        source: 'derived'
      });
    }
    return candidates;
  }

  async persistMemoryState() {
    if (!this.db || !this.callSid) return;
    try {
      await this.db.upsertCallMemory?.(this.callSid, {
        summary: this.sessionSummary,
        summary_turns: this.summaryTurns,
        metadata: {
          trace_id: this.executionContext.traceId || null,
          last_phase: this.currentPhase,
          updated_at: new Date().toISOString()
        }
      });
      const facts = this.longTermFacts.slice(0, this.memoryFactLimit);
      for (const fact of facts) {
        await this.db.upsertCallMemoryFact?.({
          call_sid: this.callSid,
          fact_key: fact.key,
          fact_text: fact.text,
          confidence: fact.confidence,
          source: fact.source || 'derived'
        });
      }
    } catch (error) {
      this.logEvent('memory_persist_failed', { error: error?.message || 'unknown' });
    }
  }

  buildSummarySnippet(entries = []) {
    const lines = [];
    for (const entry of entries) {
      const role = entry.role === 'assistant' ? 'agent' : 'caller';
      const content = String(entry.content || '').replace(/\s+/g, ' ').trim();
      if (!content) continue;
      lines.push(`${role}: ${content.slice(0, 180)}`);
    }
    return lines.join(' | ');
  }

  maybeRollupSummary() {
    if (this.conversationHistory.length < this.summaryMinTurns) {
      return;
    }
    const pending = this.conversationHistory.slice(this.rollupCursor);
    if (pending.length < this.summaryRollupBatch) {
      return;
    }
    const chunk = pending.slice(0, this.summaryRollupBatch);
    this.rollupCursor += chunk.length;
    const snippet = this.buildSummarySnippet(chunk);
    if (!snippet) return;
    const parts = [this.sessionSummary, snippet].filter(Boolean);
    this.sessionSummary = parts.join(' || ').slice(-this.summaryMaxChars);
    this.summaryTurns += chunk.length;
  }

  // Get current personality and adaptation info
  getPersonalityInfo() {
    const personality = this.personalityEngine.getCurrentPersonality();
    const report = this.personalityEngine.getAdaptationReport();
    
    return {
      ...personality,
      adaptationReport: report,
      personalityChanges: this.personalityChanges
    };
  }

  validateFunctionArgs(args) {
    if (args && typeof args === 'object') {
      return args;
    }
    const raw = String(args || '');
    try {
      return JSON.parse(raw);
    } catch (error) {
      console.log('Warning: Double function arguments returned by OpenRouter:', raw);
      if (raw.indexOf('{') !== raw.lastIndexOf('{')) {
        const start = raw.indexOf('{');
        const end = raw.lastIndexOf('}');
        if (start !== -1 && end !== -1 && end > start) {
          try {
            return JSON.parse(raw.substring(start, end + 1));
          } catch (innerError) {
            this.logEvent('tool_args_parse_failed', {
              reason: innerError?.message || 'invalid_json_recovery',
              sample: raw.slice(0, 160)
            });
          }
        }
      }
    }
    return {};
  }

  hashValue(value) {
    const body = typeof value === 'string' ? value : JSON.stringify(value || {});
    return crypto.createHash('sha256').update(body).digest('hex').slice(0, 16);
  }

  cleanupToolState(now = Date.now()) {
    for (const [key, entry] of this.toolIdempotency.entries()) {
      if (!entry?.at || now - entry.at > this.toolIdempotencyTtlMs) {
        this.toolIdempotency.delete(key);
      }
    }
    for (const [key, entry] of this.toolBudget.entries()) {
      if (!entry?.at || now - entry.at > this.toolIdempotencyTtlMs) {
        this.toolBudget.delete(key);
      }
    }
  }

  reserveToolBudget(interactionCount) {
    const now = Date.now();
    this.cleanupToolState(now);
    const budgetKey = `${this.callSid || 'no_call'}:${interactionCount}`;
    const current = this.toolBudget.get(budgetKey) || { count: 0, at: now };
    if (current.count >= this.toolBudgetPerInteraction) {
      return { allowed: false, budgetKey, remaining: 0 };
    }
    current.count += 1;
    current.at = now;
    this.toolBudget.set(budgetKey, current);
    return {
      allowed: true,
      budgetKey,
      remaining: Math.max(0, this.toolBudgetPerInteraction - current.count)
    };
  }

  isRetryableToolError(error) {
    const msg = String(error?.message || '').toLowerCase();
    return msg.includes('timeout')
      || msg.includes('etimedout')
      || msg.includes('econnreset')
      || msg.includes('socket hang up');
  }

  isRetryableModelError(error) {
    const status = Number(error?.status || error?.statusCode || 0);
    if (status === 408 || status === 409 || status === 425 || status === 429) {
      return true;
    }
    if (status >= 500) {
      return true;
    }
    const code = String(error?.code || '').toLowerCase();
    if (
      code === 'etimedout'
      || code === 'econnreset'
      || code === 'econnrefused'
      || code === 'epipe'
      || code === 'und_err_connect_timeout'
    ) {
      return true;
    }
    const msg = String(error?.message || '').toLowerCase();
    return (
      msg.includes('timeout')
      || msg.includes('temporarily unavailable')
      || msg.includes('rate limit')
      || msg.includes('overloaded')
      || msg.includes('socket hang up')
    );
  }

  withToolTimeout(executor, timeoutMs, label = 'tool_timeout') {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(label)), timeoutMs);
      Promise.resolve()
        .then(() => executor())
        .then((result) => {
          clearTimeout(timer);
          resolve(result);
        })
        .catch((error) => {
          clearTimeout(timer);
          reject(error);
        });
    });
  }

  getToolExecutionPolicy(toolName = '', registryEntry = null) {
    const lowerName = String(toolName || '').toLowerCase();
    const provider = String(this.executionContext?.provider || '').toLowerCase();
    const isSideEffect = registryEntry?.sideEffect === true || this.isLikelySideEffectTool(lowerName);
    const policy = {
      class: 'read',
      timeoutMs: Math.max(2000, Number(this.toolExecutionTimeoutMs) || 12000),
      retryLimit: Math.max(0, Number(this.toolRetryLimit) || 0),
      backoffBaseMs: 120,
      backoffMaxMs: 1200,
      jitterMs: 80
    };

    if (lowerName === 'collect_digits' || lowerName === 'collect_multiple_digits') {
      policy.class = 'capture';
      policy.retryLimit = 0;
      policy.timeoutMs = Math.min(policy.timeoutMs, 7000);
      policy.backoffBaseMs = 0;
      policy.backoffMaxMs = 0;
      policy.jitterMs = 0;
    } else if (isSideEffect) {
      policy.class = 'side_effect';
      policy.retryLimit = 0;
      policy.timeoutMs = Math.max(policy.timeoutMs, 12000);
      policy.backoffBaseMs = 200;
      policy.backoffMaxMs = 1500;
      policy.jitterMs = 100;
    } else {
      policy.class = 'read';
      policy.retryLimit = Math.max(1, policy.retryLimit);
      policy.timeoutMs = Math.min(policy.timeoutMs, 9000);
    }

    if (provider === 'aws') {
      policy.timeoutMs = Math.min(20000, policy.timeoutMs + 2000);
      policy.backoffBaseMs += 100;
    } else if (provider === 'vonage') {
      policy.timeoutMs = Math.min(18000, policy.timeoutMs + 1000);
      policy.jitterMs += 20;
    } else if (provider === 'twilio') {
      policy.timeoutMs = Math.min(16000, policy.timeoutMs + 500);
    }

    return policy;
  }

  sleep(ms = 0) {
    if (!Number.isFinite(Number(ms)) || Number(ms) <= 0) {
      return Promise.resolve();
    }
    return new Promise((resolve) => setTimeout(resolve, Number(ms)));
  }

  getRetryDelayMs(policy = {}, attempt = 1) {
    const base = Math.max(0, Number(policy.backoffBaseMs) || 0);
    if (!base || attempt <= 1) return base;
    const maxMs = Math.max(base, Number(policy.backoffMaxMs) || base);
    const jitter = Math.max(0, Number(policy.jitterMs) || 0);
    const pow = Math.min(6, Math.max(0, attempt - 1));
    const candidate = Math.min(maxMs, base * (2 ** pow));
    const signedJitter = jitter ? (Math.random() * jitter * 2) - jitter : 0;
    return Math.max(0, Math.round(candidate + signedJitter));
  }

  async tryGetPersistedIdempotencyResult(idempotencyKey, maxWaitMs = 1500) {
    if (!idempotencyKey) return null;
    if (!this.db?.getGptToolIdempotency) return null;
    const startedAt = Date.now();
    const waitMs = Math.max(0, Number(maxWaitMs) || 0);
    while (Date.now() - startedAt <= waitMs) {
      const existing = await this.db.getGptToolIdempotency(idempotencyKey).catch(() => null);
      if (!existing) {
        return null;
      }
      if (existing.status === 'ok' || existing.status === 'failed') {
        return existing;
      }
      await this.sleep(120);
    }
    return this.db.getGptToolIdempotency(idempotencyKey).catch(() => null);
  }

  buildToolPlan(functionName, rawArgs, interactionCount, toolCallId) {
    const parsedArgs = this.validateFunctionArgs(rawArgs);
    const validatedArgs = parsedArgs && typeof parsedArgs === 'object' && !Array.isArray(parsedArgs)
      ? parsedArgs
      : {};
    const callSid = this.callSid || validatedArgs.callSid || validatedArgs.call_sid || 'unknown';
    const stepId = validatedArgs.stepId
      || validatedArgs.step_id
      || validatedArgs.plan_id
      || validatedArgs.profile
      || functionName
      || 'tool';
    const attemptId = validatedArgs.attemptId
      || validatedArgs.attempt_id
      || validatedArgs.retry
      || validatedArgs.retries
      || 1;
    const inputHash = this.hashValue(validatedArgs);
    const idempotencyKey = `tool:${callSid}:${stepId}:${attemptId}:${inputHash}`;
    const normalizedToolCallId = toolCallId
      || `tool_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
    return {
      functionName,
      validatedArgs,
      rawArgs,
      interactionCount,
      toolCallId: normalizedToolCallId,
      idempotencyKey,
      metadata: {
        callSid,
        stepId: String(stepId),
        attemptId: String(attemptId),
        inputHash
      }
    };
  }

  validateBySchema(schema = {}, value = {}, path = 'root') {
    const errors = [];
    const target = value && typeof value === 'object' ? value : {};
    const normalizedSchema = schema && typeof schema === 'object' ? schema : {};
    const properties = normalizedSchema.properties && typeof normalizedSchema.properties === 'object'
      ? normalizedSchema.properties
      : {};
    const required = Array.isArray(normalizedSchema.required) ? normalizedSchema.required : [];

    for (const key of required) {
      if (target[key] === undefined || target[key] === null || target[key] === '') {
        errors.push(`${path}.${key} is required`);
      }
    }

    for (const [key, propertySchema] of Object.entries(properties)) {
      const nextPath = `${path}.${key}`;
      const fieldValue = target[key];
      if (fieldValue === undefined || fieldValue === null) {
        continue;
      }
      const expectedType = propertySchema?.type;
      if (expectedType === 'string' && typeof fieldValue !== 'string') {
        errors.push(`${nextPath} must be string`);
      } else if (expectedType === 'integer' && !Number.isInteger(fieldValue)) {
        errors.push(`${nextPath} must be integer`);
      } else if (expectedType === 'number' && typeof fieldValue !== 'number') {
        errors.push(`${nextPath} must be number`);
      } else if (expectedType === 'boolean' && typeof fieldValue !== 'boolean') {
        errors.push(`${nextPath} must be boolean`);
      } else if (expectedType === 'array' && !Array.isArray(fieldValue)) {
        errors.push(`${nextPath} must be array`);
      } else if (expectedType === 'object' && (typeof fieldValue !== 'object' || Array.isArray(fieldValue))) {
        errors.push(`${nextPath} must be object`);
      }

      if (Array.isArray(propertySchema?.enum) && !propertySchema.enum.includes(fieldValue)) {
        errors.push(`${nextPath} must be one of: ${propertySchema.enum.join(', ')}`);
      }
      if (Number.isFinite(Number(propertySchema?.minimum)) && Number(fieldValue) < Number(propertySchema.minimum)) {
        errors.push(`${nextPath} must be >= ${propertySchema.minimum}`);
      }
      if (Number.isFinite(Number(propertySchema?.maximum)) && Number(fieldValue) > Number(propertySchema.maximum)) {
        errors.push(`${nextPath} must be <= ${propertySchema.maximum}`);
      }
    }

    return {
      ok: errors.length === 0,
      errors
    };
  }

  getToolCircuitState(toolName) {
    if (!this.toolCircuitStates.has(toolName)) {
      this.toolCircuitStates.set(toolName, { failures: [], openUntil: 0 });
    }
    return this.toolCircuitStates.get(toolName);
  }

  pruneToolCircuitFailures(state, now = Date.now()) {
    const windowMs = Math.max(1000, Number(this.toolCircuitConfig.windowMs || 120000));
    state.failures = state.failures.filter((timestamp) => now - timestamp <= windowMs);
  }

  isToolCircuitOpen(toolName) {
    if (this.toolCircuitConfig.enabled === false) return false;
    const state = this.getToolCircuitState(toolName);
    return Number(state.openUntil || 0) > Date.now();
  }

  markToolFailure(toolName) {
    if (this.toolCircuitConfig.enabled === false) return;
    const now = Date.now();
    const state = this.getToolCircuitState(toolName);
    state.failures.push(now);
    this.pruneToolCircuitFailures(state, now);
    if (state.failures.length >= Math.max(1, Number(this.toolCircuitConfig.failureThreshold || 4))) {
      state.openUntil = now + Math.max(1000, Number(this.toolCircuitConfig.cooldownMs || 90000));
      this.logEvent('tool_circuit_opened', {
        tool: toolName,
        failures: state.failures.length,
        cooldown_ms: Number(this.toolCircuitConfig.cooldownMs || 90000)
      });
      this.db?.logServiceHealth?.('gpt_tool_circuit', 'open', {
        call_sid: this.callSid || null,
        tool: toolName,
        failures: state.failures.length,
        trace_id: this.executionContext.traceId || null
      }).catch(() => {});
    }
  }

  markToolSuccess(toolName) {
    if (this.toolCircuitConfig.enabled === false) return;
    const state = this.getToolCircuitState(toolName);
    state.failures = [];
    if (state.openUntil) {
      state.openUntil = 0;
      this.logEvent('tool_circuit_closed', { tool: toolName });
      this.db?.logServiceHealth?.('gpt_tool_circuit', 'closed', {
        call_sid: this.callSid || null,
        tool: toolName,
        trace_id: this.executionContext.traceId || null
      }).catch(() => {});
    }
  }

  validateToolPlan(plan) {
    if (!plan || !plan.functionName) {
      return { ok: false, error: 'invalid_tool_plan' };
    }
    const registryEntry = this.toolRegistry.get(plan.functionName);
    if (!this.availableFunctions[plan.functionName] || !registryEntry) {
      return { ok: false, error: `Function ${plan.functionName} not available` };
    }
    const args = { ...(plan.validatedArgs || {}) };
    if (this.strictToolSchemas) {
      const schemaValidation = this.validateBySchema(registryEntry.schema || {}, args, plan.functionName);
      if (!schemaValidation.ok) {
        return {
          ok: false,
          error: `tool_schema_validation_failed: ${schemaValidation.errors.join('; ')}`
        };
      }
    }
    if (plan.functionName === 'collect_digits') {
      if (Number.isFinite(args.min_digits) && args.min_digits < 1) {
        args.min_digits = 1;
      }
      if (Number.isFinite(args.max_digits) && Number.isFinite(args.min_digits) && args.max_digits < args.min_digits) {
        args.max_digits = args.min_digits;
      }
    }
    return {
      ok: true,
      plan: {
        ...plan,
        registry: registryEntry,
        validatedArgs: args
      }
    };
  }

  async executeToolPlan(plan) {
    const registryEntry = plan.registry || this.toolRegistry.get(plan.functionName) || {};
    const existingLock = this.toolExecutionLocks.get(plan.idempotencyKey);
    if (existingLock) {
      try {
        await existingLock;
      } catch (_) {
        // ignore prior execution errors and continue with normal duplicate checks
      }
    }

    let releaseLock = () => {};
    const lockPromise = new Promise((resolve) => {
      releaseLock = resolve;
    });
    this.toolExecutionLocks.set(plan.idempotencyKey, lockPromise);

    try {
      const now = Date.now();
      let idempotencyReserved = false;
      const duplicate = this.toolIdempotency.get(plan.idempotencyKey);
      if (duplicate && now - duplicate.at < this.toolIdempotencyTtlMs) {
        this.toolExecutionStats.total += 1;
        await this.db?.addGptToolAudit?.({
          call_sid: this.callSid || null,
          trace_id: this.executionContext.traceId || null,
          tool_name: plan.functionName,
          idempotency_key: plan.idempotencyKey,
          input_hash: plan.metadata?.inputHash || null,
          status: 'cached',
          response_payload: parseJsonSafe(duplicate.responseText, { raw: duplicate.responseText }),
          metadata: { duplicate: true, source: 'memory' }
        }).catch(() => {});
        return {
          responseText: duplicate.responseText,
          metadata: {
            ...plan.metadata,
            idempotencyKey: plan.idempotencyKey,
            duplicate: true,
            cached: true
          }
        };
      }

      if (registryEntry.sideEffect && this.db?.reserveGptToolIdempotency) {
        const reserve = await this.db.reserveGptToolIdempotency({
          idempotency_key: plan.idempotencyKey,
          call_sid: this.callSid || null,
          trace_id: this.executionContext.traceId || null,
          tool_name: plan.functionName,
          input_hash: plan.metadata?.inputHash || null
        }).catch(() => ({ reserved: false }));

        if (!reserve?.reserved) {
          const persisted = await this.tryGetPersistedIdempotencyResult(plan.idempotencyKey, 1600);
          const payload = parseJsonSafe(persisted?.response_payload, null);
          if (persisted?.status === 'ok') {
            const responseText = typeof payload === 'string'
              ? payload
              : payload
                ? JSON.stringify(payload)
                : JSON.stringify({ status: 'ok' });
            this.toolIdempotency.set(plan.idempotencyKey, { at: Date.now(), responseText });
            this.toolExecutionStats.total += 1;
            await this.db?.addGptToolAudit?.({
              call_sid: this.callSid || null,
              trace_id: this.executionContext.traceId || null,
              tool_name: plan.functionName,
              idempotency_key: plan.idempotencyKey,
              input_hash: plan.metadata?.inputHash || null,
              status: 'cached',
              response_payload: parseJsonSafe(responseText, { raw: responseText }),
              metadata: { duplicate: true, source: 'idempotency_store' }
            }).catch(() => {});
            return {
              responseText,
              metadata: {
                ...plan.metadata,
                idempotencyKey: plan.idempotencyKey,
                duplicate: true,
                cached: true,
                source: 'idempotency_store'
              }
            };
          }
          if (persisted?.status === 'failed') {
            this.toolExecutionStats.total += 1;
            this.toolExecutionStats.failed += 1;
            return {
              responseText: JSON.stringify({
                error: 'tool_idempotency_failed',
                message: persisted.error_message || 'Previous execution for this idempotency key failed.'
              }),
              metadata: {
                ...plan.metadata,
                idempotencyKey: plan.idempotencyKey,
                failed: true,
                duplicate: true,
                source: 'idempotency_store'
              }
            };
          }
          this.toolExecutionStats.total += 1;
          this.toolExecutionStats.failed += 1;
          return {
            responseText: JSON.stringify({
              error: 'tool_in_progress',
              message: 'This action is already being processed. Please wait.'
            }),
            metadata: {
              ...plan.metadata,
              idempotencyKey: plan.idempotencyKey,
              in_progress: true,
              failed: true
            }
          };
        }
        idempotencyReserved = true;
      } else if (registryEntry.sideEffect && this.db?.getGptToolAuditByIdempotency) {
        const persistedDuplicate = await this.db.getGptToolAuditByIdempotency(plan.idempotencyKey).catch(() => null);
        if (persistedDuplicate?.response_payload != null) {
          const parsed = parseJsonSafe(persistedDuplicate.response_payload, null);
          const responseText = typeof parsed === 'string'
            ? parsed
            : parsed
              ? JSON.stringify(parsed)
              : String(persistedDuplicate.response_payload);
          this.toolIdempotency.set(plan.idempotencyKey, { at: Date.now(), responseText });
          this.toolExecutionStats.total += 1;
          return {
            responseText,
            metadata: {
              ...plan.metadata,
              idempotencyKey: plan.idempotencyKey,
              duplicate: true,
              cached: true,
              source: 'db_audit'
            }
          };
        }
      }

      const budget = this.reserveToolBudget(plan.interactionCount);
      if (!budget.allowed) {
        if (idempotencyReserved) {
          await this.db?.completeGptToolIdempotency?.({
            idempotency_key: plan.idempotencyKey,
            call_sid: this.callSid || null,
            trace_id: this.executionContext.traceId || null,
            tool_name: plan.functionName,
            input_hash: plan.metadata?.inputHash || null,
            status: 'failed',
            error_message: 'tool_budget_exceeded'
          }).catch(() => {});
        }
        this.toolExecutionStats.total += 1;
        this.toolExecutionStats.failed += 1;
        return {
          responseText: JSON.stringify({ error: 'tool_budget_exceeded', message: 'Tool execution budget exceeded for this interaction.' }),
          metadata: {
            ...plan.metadata,
            idempotencyKey: plan.idempotencyKey,
            budget_exceeded: true,
            budget_remaining: 0
          }
        };
      }

      let selectedToolName = plan.functionName;
      if (this.isToolCircuitOpen(selectedToolName)) {
        const fallbackName = registryEntry.fallback;
        if (fallbackName && this.availableFunctions[fallbackName]) {
          selectedToolName = fallbackName;
          this.logEvent('tool_failover_applied', {
            tool: plan.functionName,
            fallback_tool: fallbackName
          });
        } else {
          if (idempotencyReserved) {
            await this.db?.completeGptToolIdempotency?.({
              idempotency_key: plan.idempotencyKey,
              call_sid: this.callSid || null,
              trace_id: this.executionContext.traceId || null,
              tool_name: selectedToolName,
              input_hash: plan.metadata?.inputHash || null,
              status: 'failed',
              error_message: 'tool_circuit_open'
            }).catch(() => {});
          }
          this.toolExecutionStats.total += 1;
          this.toolExecutionStats.failed += 1;
          await this.db?.addGptToolAudit?.({
            call_sid: this.callSid || null,
            trace_id: this.executionContext.traceId || null,
            tool_name: selectedToolName,
            idempotency_key: plan.idempotencyKey,
            input_hash: plan.metadata?.inputHash || null,
            request_payload: plan.validatedArgs,
            response_payload: null,
            status: 'failed',
            error_message: 'tool_circuit_open',
            metadata: {
              budget_remaining: budget.remaining
            }
          }).catch(() => {});
          return {
            responseText: JSON.stringify({ error: 'tool_circuit_open', message: `Tool ${selectedToolName} temporarily unavailable.` }),
            metadata: {
              ...plan.metadata,
              idempotencyKey: plan.idempotencyKey,
              failed: true,
              error: 'tool_circuit_open',
              budget_remaining: budget.remaining
            }
          };
        }
      }

      const functionToCall = this.availableFunctions[selectedToolName];
      const policy = this.getToolExecutionPolicy(selectedToolName, registryEntry);
      const maxAttempts = Math.max(1, Number(policy.retryLimit || 0) + 1);
      let lastError = null;
      for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
          const startedAt = Date.now();
          const result = await this.withToolTimeout(
            () => functionToCall(plan.validatedArgs),
            policy.timeoutMs,
            `${selectedToolName}_timeout`
          );
          const responseText = typeof result === 'string' ? result : JSON.stringify(result);
          const durationMs = Date.now() - startedAt;
          const metadata = {
            ...plan.metadata,
            idempotencyKey: plan.idempotencyKey,
            tool: selectedToolName,
            attempt,
            timeout_ms: policy.timeoutMs,
            retry_limit: Number(policy.retryLimit || 0),
            budget_remaining: budget.remaining,
            duration_ms: durationMs,
            policy_class: policy.class,
            provider: this.executionContext.provider || null
          };
          this.toolIdempotency.set(plan.idempotencyKey, { at: Date.now(), responseText });
          this.toolExecutionStats.total += 1;
          this.markToolSuccess(selectedToolName);
          if (registryEntry.sideEffect) {
            await this.db?.completeGptToolIdempotency?.({
              idempotency_key: plan.idempotencyKey,
              call_sid: this.callSid || null,
              trace_id: this.executionContext.traceId || null,
              tool_name: selectedToolName,
              input_hash: plan.metadata?.inputHash || null,
              status: 'ok',
              response_payload: parseJsonSafe(responseText, { raw: responseText })
            }).catch(() => {});
          }
          await this.db?.addGptToolAudit?.({
            call_sid: this.callSid || null,
            trace_id: this.executionContext.traceId || null,
            tool_name: selectedToolName,
            idempotency_key: plan.idempotencyKey,
            input_hash: plan.metadata?.inputHash || null,
            request_payload: plan.validatedArgs,
            response_payload: parseJsonSafe(responseText, { raw: responseText }),
            status: 'ok',
            duration_ms: durationMs,
            metadata
          }).catch(() => {});
          return { responseText, metadata };
        } catch (error) {
          lastError = error;
          if (attempt >= maxAttempts || !this.isRetryableToolError(error)) {
            break;
          }
          await this.sleep(this.getRetryDelayMs(policy, attempt));
        }
      }

      this.toolExecutionStats.total += 1;
      this.toolExecutionStats.failed += 1;
      this.markToolFailure(selectedToolName);
      if (registryEntry.sideEffect) {
        await this.db?.completeGptToolIdempotency?.({
          idempotency_key: plan.idempotencyKey,
          call_sid: this.callSid || null,
          trace_id: this.executionContext.traceId || null,
          tool_name: selectedToolName,
          input_hash: plan.metadata?.inputHash || null,
          status: 'failed',
          error_message: lastError?.message || 'unknown_error'
        }).catch(() => {});
      }
      await this.db?.addGptToolAudit?.({
        call_sid: this.callSid || null,
        trace_id: this.executionContext.traceId || null,
        tool_name: selectedToolName,
        idempotency_key: plan.idempotencyKey,
        input_hash: plan.metadata?.inputHash || null,
        request_payload: plan.validatedArgs,
        response_payload: null,
        status: 'failed',
        error_message: lastError?.message || 'unknown_error',
        metadata: {
          retry_limit: Number(policy.retryLimit || 0),
          timeout_ms: policy.timeoutMs,
          policy_class: policy.class,
          provider: this.executionContext.provider || null
        }
      }).catch(() => {});
      return {
        responseText: JSON.stringify({ error: 'Function execution failed', details: lastError?.message || 'unknown_error' }),
        metadata: {
          ...plan.metadata,
          idempotencyKey: plan.idempotencyKey,
          tool: selectedToolName,
          timeout_ms: policy.timeoutMs,
          retry_limit: Number(policy.retryLimit || 0),
          budget_remaining: budget.remaining,
          failed: true,
          error: lastError?.message || 'unknown_error',
          policy_class: policy.class
        }
      };
    } finally {
      releaseLock();
      if (this.toolExecutionLocks.get(plan.idempotencyKey) === lockPromise) {
        this.toolExecutionLocks.delete(plan.idempotencyKey);
      }
    }
  }

  updateUserContext(name, role, text) {
    let entry;
    if (role === 'tool') {
      entry = { role: 'tool', content: text, tool_call_id: name };
    } else if (name !== 'user') {
      entry = { role, name, content: text };
    } else {
      entry = { role, content: text };
    }

    this.userContext.push(entry);
    this.addToPhaseWindow(entry);
  }

  applyPersonaConsistency(text = '') {
    const raw = String(text || '');
    if (!raw) {
      return { text: raw, score: 1, corrected: false, issues: [] };
    }
    const check = this.personalityEngine.evaluateConsistency(raw);
    if (Number(check.score) >= this.personaConsistencyThreshold) {
      return { text: raw, score: check.score, corrected: false, issues: check.issues || [] };
    }
    const corrected = this.personalityEngine.correctResponseDrift(raw);
    const post = this.personalityEngine.evaluateConsistency(corrected);
    return {
      text: corrected,
      score: post.score,
      corrected: corrected !== raw,
      issues: check.issues || []
    };
  }

  estimateMessagesTokenCount(messages = []) {
    return messages.reduce((sum, msg) => {
      if (!msg) return sum;
      const content = msg.content == null ? '' : msg.content;
      if (Array.isArray(content)) {
        return sum + content.reduce((inner, part) => inner + estimateTokenCount(part?.text || ''), 0);
      }
      return sum + estimateTokenCount(content);
    }, 0);
  }

  // Enhanced completion method with dynamic functions and personality adaptation
  async completion(text, interactionCount, role = 'user', name = 'user', options = {}) {
    const normalizedOptions = options && typeof options === 'object' ? options : {};
    const toolDepth = Number.isFinite(Number(normalizedOptions.toolDepth))
      ? Number(normalizedOptions.toolDepth)
      : 0;
    const allowTools = normalizedOptions.allowTools !== false;

    // Normalize non-string inputs (e.g., function payload objects)
    if (typeof text === 'object') {
      try {
        text = JSON.stringify(text);
      } catch (_) {
        text = String(text);
      }
    }

    if (!text || String(text).trim().length === 0) {
      return;
    }
    if (!this.openai?.chat?.completions) {
      throw new Error('OpenRouter client not initialized');
    }
    await this.ensureMemoryLoaded();

    // Store conversation for personality analysis
    this.conversationHistory.push({
      role: role,
      content: text,
      timestamp: new Date().toISOString(),
      interactionCount: interactionCount
    });

    this.autoUpdatePhase(role, text, interactionCount);

    // Analyze customer message and adapt personality if needed
    if (role === 'user') {
      console.log(`Analyzing message for adaptation...`.blue);
      
      const adaptation = this.personalityEngine.adaptPersonality(text, this.conversationHistory);
      const extractedFacts = this.extractLongTermFacts(role, text);
      if (extractedFacts.length) {
        for (const fact of extractedFacts) {
          if (this.factKeys.has(fact.key)) continue;
          this.factKeys.add(fact.key);
          this.longTermFacts.push(fact);
        }
        this.longTermFacts = this.longTermFacts
          .sort((a, b) => Number(b.confidence || 0) - Number(a.confidence || 0))
          .slice(0, this.memoryFactLimit);
      }
      
      if (adaptation.personalityChanged) {
        console.log(`🎭 Personality: ${adaptation.previousPersonality} → ${adaptation.currentPersonality}`.magenta);
        
        // Update system prompt with new personality
        this.updateSystemPromptWithPersonality(adaptation.adaptedPrompt);
        
        // Log personality change
        this.personalityChanges.push({
          from: adaptation.previousPersonality,
          to: adaptation.currentPersonality,
          trigger: adaptation.analysis,
          timestamp: new Date().toISOString(),
          interactionCount: interactionCount
        });

        this.lastPersonalityUpdate = adaptation;
        
        // Emit personality change event
        this.emit('personalityChanged', {
          from: adaptation.previousPersonality,
          to: adaptation.currentPersonality,
          reason: adaptation.analysis,
          adaptedPrompt: adaptation.adaptedPrompt
        });
      }

      // Always refresh prompt layering so tone/urgency adjustments apply in real time.
      this.updateSystemPromptWithPersonality(adaptation.adaptedPrompt);
      this.setPersonaContext({
        urgency: adaptation.analysis?.urgency || adaptation.context?.urgencyLevel || 'normal'
      });

      console.log(`🎯 Current: ${adaptation.currentPersonality} | Mood: ${adaptation.context.customerMood}`.cyan);
      this.logEvent('persona_adapted', {
        personality: adaptation.currentPersonality,
        mood: adaptation.context.customerMood,
        urgency: adaptation.context.urgencyLevel,
        confusion: adaptation.analysis?.confusionScore
      });
    }

    this.updateUserContext(name, role, text);
    this.maybeRollupSummary();

    // Use sanitized tools for the model (strip custom fields like "say"/"returns")
    const toolsToUse = allowTools ? this.getSanitizedTools() : [];
    if (!allowTools && this.dynamicTools.length) {
      this.logEvent('tool_calls_temporarily_disabled', {
        reason: 'tool_loop_limit',
        tool_depth: toolDepth,
        max_tool_loops: this.maxToolLoops
      });
    }
    const adaptiveMaxTokens = this.getAdaptiveMaxTokens();
    const messages = this.buildModelMessages();
    this.logEvent('completion_started', {
      interaction_count: interactionCount,
      role,
      phase: this.currentPhase,
      tool_depth: toolDepth,
      tools_enabled: allowTools,
      model: this.model,
      backup_model: this.backupModel || null,
      context_messages: messages.length,
      context_tokens_est: this.estimateMessagesTokenCount(messages),
      tools_available: toolsToUse.length
    });

    // Send completion request with current personality-adapted context and dynamic tools
    let stream;
    let currentModel = this.model;
    const startedAt = Date.now();
    let firstChunkAt = null;
    let stallTimer = null;
    let responseTimer = null;
    let idleTimer = null;
    let controller = null;
    let fillerSent = false;
    const clearTimers = () => {
      if (stallTimer) {
        clearTimeout(stallTimer);
        stallTimer = null;
      }
      if (responseTimer) {
        clearTimeout(responseTimer);
        responseTimer = null;
      }
      if (idleTimer) {
        clearTimeout(idleTimer);
        idleTimer = null;
      }
    };
    const handleFailure = (err) => {
      clearTimers();
      console.error('GPT completion error:', err);
      this.logEvent('completion_failed', {
        error: err?.message || 'unknown_error',
        model: currentModel
      });
      this.emit('gpterror', err);

      const fallbackResponse = 'I am having trouble replying right now • please give me a moment or try again.';
      const fallbackReply = {
        partialResponseIndex: this.partialResponseIndex,
        partialResponse: fallbackResponse,
        personalityInfo: this.personalityEngine.getCurrentPersonality(),
        adaptationHistory: this.personalityChanges.slice(-3),
        functionsAvailable: Object.keys(this.availableFunctions).length
      };

      this.emit('gptreply', fallbackReply, interactionCount);
      this.partialResponseIndex++;

      this.conversationHistory.push({
        role: 'assistant',
        content: fallbackResponse,
        timestamp: new Date().toISOString(),
        interactionCount: interactionCount,
        personality: this.personalityEngine.currentPersonality,
        functionsUsed: []
      });

      this.userContext.push({ role: 'assistant', content: fallbackResponse });
      this.addToPhaseWindow({ role: 'assistant', content: fallbackResponse });

      const finishedAt = Date.now();
      const ttfb = firstChunkAt ? (firstChunkAt - startedAt) : null;
      const rtt = finishedAt - startedAt;
      this.recordLatency(ttfb, rtt);
    };

    const maxAttempts = this.backupModel ? 3 : 2;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        stallTimer = setTimeout(() => {
          if (!firstChunkAt && !fillerSent) {
            fillerSent = true;
            this.emit('stall', this.fillerText);
          }
        }, this.stallTimeoutMs);

        controller = new AbortController();
        responseTimer = setTimeout(() => {
          controller.abort(new Error('gpt_response_timeout'));
        }, this.responseTimeoutMs);

        const effectiveMaxTokens = interactionCount > 0
          ? Math.min(adaptiveMaxTokens, Math.floor(this.maxTokens * 0.6))
          : adaptiveMaxTokens;

        stream = await this.openai.chat.completions.create({
          model: currentModel,
          messages,
          tools: toolsToUse,
          max_tokens: effectiveMaxTokens,
          stream: true,
          signal: controller.signal,
        });
        idleTimer = setTimeout(() => {
          controller.abort(new Error('gpt_stream_idle'));
        }, this.streamIdleTimeoutMs);
        break; // success
      } catch (err) {
        const retriable = this.isRetryableModelError(err);
        const canFallback = Boolean(
          retriable
          && this.backupModel
          && this.backupModel !== currentModel
          && currentModel === this.model
        );
        if (canFallback) {
          this.logEvent('model_failover_selected', {
            from_model: currentModel,
            to_model: this.backupModel,
            attempt,
            error: err?.message || 'unknown_error'
          });
          currentModel = this.backupModel;
        }
        clearTimers();
        if (!retriable || attempt >= maxAttempts) {
          handleFailure(err);
          return;
        }
        const backoffMs = Math.min(1500, (attempt * 250) + Math.floor(Math.random() * 120));
        await this.sleep(backoffMs);
      }
    }

    let completeResponse = '';
    let partialResponse = '';
    const toolCallBuffers = new Map();
    const toolInvocations = [];
    let toolCallHandled = false;
    let finishReason = '';
    let streamError = null;

    function collectToolInformation(deltas) {
      const toolCalls = Array.isArray(deltas?.tool_calls) ? deltas.tool_calls : [];
      for (const toolDelta of toolCalls) {
        const index = Number.isFinite(Number(toolDelta?.index)) ? Number(toolDelta.index) : 0;
        const entry = toolCallBuffers.get(index) || { id: '', name: '', args: '' };
        if (toolDelta?.id) {
          entry.id = toolDelta.id;
        }
        if (toolDelta?.function?.name) {
          entry.name = toolDelta.function.name;
        }
        if (toolDelta?.function?.arguments) {
          entry.args += toolDelta.function.arguments;
        }
        toolCallBuffers.set(index, entry);
      }
    }

    try {
      for await (const chunk of stream) {
        const choice = chunk?.choices?.[0];
        if (!choice) {
          continue;
        }
        if (!firstChunkAt) {
          firstChunkAt = Date.now();
          if (stallTimer) clearTimeout(stallTimer);
        }
        if (idleTimer) {
          clearTimeout(idleTimer);
        }
        idleTimer = setTimeout(() => {
          controller?.abort(new Error('gpt_stream_idle'));
        }, this.streamIdleTimeoutMs);
        const deltas = choice.delta || {};
        const content = deltas.content || '';
        finishReason = choice.finish_reason;

        if (deltas.tool_calls) {
          collectToolInformation(deltas);
        }

        if (finishReason === 'tool_calls') {
          toolCallHandled = true;
          const plannedCalls = [...toolCallBuffers.entries()]
            .sort((a, b) => a[0] - b[0])
            .map(([, value]) => value)
            .filter((item) => item?.name);
          if (!plannedCalls.length) {
            console.error('❌ Tool call requested without a function name.'.red);
            continue;
          }

          this.logEvent('tool_planner_created', {
            tools: plannedCalls.map((item) => item.name)
          });

          for (const planned of plannedCalls) {
            const toolPlan = this.buildToolPlan(
              planned.name,
              planned.args,
              interactionCount,
              planned.id
            );
            const validation = this.validateToolPlan(toolPlan);
            const effectivePlan = validation.ok ? validation.plan : toolPlan;
            const toolArgs = planned.args && String(planned.args).trim().length > 0
              ? String(planned.args)
              : JSON.stringify(effectivePlan.validatedArgs || {});

            const toolCallMessage = {
              role: 'assistant',
              content: null,
              tool_calls: [
                {
                  id: effectivePlan.toolCallId,
                  type: 'function',
                  function: {
                    name: planned.name,
                    arguments: toolArgs
                  }
                }
              ]
            };
            this.userContext.push(toolCallMessage);
            this.addToPhaseWindow(toolCallMessage);

            const toolData = this.dynamicTools.find(tool => tool.function.name === planned.name);
            const say = toolData?.function?.say || 'One moment please...';
            this.emit('gptreply', {
              partialResponseIndex: null,
              partialResponse: say,
              personalityInfo: this.personalityEngine.getCurrentPersonality()
            }, interactionCount);

            let responseText;
            if (!validation.ok) {
              responseText = JSON.stringify({
                error: 'tool_plan_rejected',
                details: validation.error || 'invalid_tool_plan'
              });
              this.logEvent('tool_plan_rejected', {
                tool: planned.name,
                error: validation.error || 'invalid_tool_plan'
              });
            } else {
              const execution = await this.executeToolPlan(effectivePlan);
              responseText = execution.responseText;
              toolInvocations.push({
                tool: planned.name,
                metadata: execution.metadata || {}
              });
              console.log(`🔧 Executed dynamic function: ${planned.name}`.green);
              console.log('🧰 Tool envelope:', execution.metadata);
            }

            if (planned.name === 'collect_digits' || planned.name === 'collect_multiple_digits') {
              this.updateUserContext(effectivePlan.toolCallId, 'tool', responseText);
              await this.persistMemoryState();
              return;
            }
            const nextToolDepth = toolDepth + 1;
            const canUseMoreTools = nextToolDepth < this.maxToolLoops;
            if (!canUseMoreTools) {
              this.logEvent('tool_loop_limit_reached', {
                max_tool_loops: this.maxToolLoops,
                tool_depth: nextToolDepth,
                tool: planned.name
              });
            }
            try {
              await this.completion(
                responseText,
                interactionCount,
                'tool',
                effectivePlan.toolCallId,
                {
                  toolDepth: nextToolDepth,
                  allowTools: canUseMoreTools
                }
              );
            } catch (nestedError) {
              this.logEvent('tool_synthesis_failed', {
                tool: planned.name,
                error: nestedError?.message || 'unknown_error'
              });
              handleFailure(nestedError);
              return;
            }
          }
          return;
        } else {
          completeResponse += content;
          partialResponse += content;

          if (content.trim().slice(-1) === '•' || finishReason === 'stop') {
            if (!partialResponse.trim()) {
              continue;
            }
            const consistency = this.applyPersonaConsistency(partialResponse);
            const gptReply = { 
              partialResponseIndex: this.partialResponseIndex,
              partialResponse: consistency.text,
              personalityInfo: this.personalityEngine.getCurrentPersonality(),
              adaptationHistory: this.personalityChanges.slice(-3), // Last 3 changes
              functionsAvailable: Object.keys(this.availableFunctions).length,
              personaConsistency: {
                score: consistency.score,
                corrected: consistency.corrected,
                issues: consistency.issues
              }
            };

            this.emit('gptreply', gptReply, interactionCount);
            this.partialResponseIndex++;
            partialResponse = '';
          }
        }
      }
    } catch (err) {
      streamError = err;
    } finally {
      clearTimers();
    }

    if (streamError) {
      handleFailure(streamError);
      return;
    }

    if (toolCallHandled) {
      await this.persistMemoryState();
      return;
    }

    if (partialResponse.trim()) {
      const consistency = this.applyPersonaConsistency(partialResponse);
      this.emit('gptreply', {
        partialResponseIndex: this.partialResponseIndex,
        partialResponse: consistency.text,
        personalityInfo: this.personalityEngine.getCurrentPersonality(),
        adaptationHistory: this.personalityChanges.slice(-3),
        functionsAvailable: Object.keys(this.availableFunctions).length,
        personaConsistency: {
          score: consistency.score,
          corrected: consistency.corrected,
          issues: consistency.issues
        }
      }, interactionCount);
      this.partialResponseIndex++;
      partialResponse = '';
    }

    if (!String(completeResponse || '').trim()) {
      handleFailure(new Error('gpt_empty_response'));
      return;
    }

    // Store AI response in conversation history
    const correctedComplete = this.applyPersonaConsistency(completeResponse);
    this.conversationHistory.push({
      role: 'assistant',
      content: correctedComplete.text,
      timestamp: new Date().toISOString(),
      interactionCount: interactionCount,
      personality: this.personalityEngine.currentPersonality,
      functionsUsed: toolInvocations.map((item) => item.tool)
    });

    this.userContext.push({'role': 'assistant', 'content': correctedComplete.text});
    this.addToPhaseWindow({ role: 'assistant', content: correctedComplete.text });
    
    console.log(`🧠 Context: ${this.userContext.length} | Personality: ${this.personalityEngine.currentPersonality} | Functions: ${Object.keys(this.availableFunctions).length}`.green);

    // Record latency metrics
    const finishedAt = Date.now();
    const ttfb = firstChunkAt ? (firstChunkAt - startedAt) : null;
    const rtt = finishedAt - startedAt;
    this.recordLatency(ttfb, rtt);
    console.log(`Latency | model: ${currentModel} | ttfb: ${ttfb}ms | rtt: ${rtt}ms`);
    await this.logSloIfDegraded(ttfb, rtt);
    await this.persistMemoryState();
    this.logEvent('completion_finished', {
      model: currentModel,
      ttfb_ms: ttfb,
      rtt_ms: rtt,
      tools_invoked: toolInvocations.map((item) => item.tool),
      persona_consistency_score: correctedComplete.score
    });
  }

  // Update system prompt with new personality
  updateSystemPromptWithPersonality(adaptedPrompt) {
    this.personalityPrompt = adaptedPrompt || this.personalityPrompt || this.baseSystemPrompt;
    this.systemPrompt = this.composeSystemPrompt(this.personalityPrompt);

    // Replace the first system message with the adapted prompt
    const systemMessageIndex = this.userContext.findIndex(msg => msg.role === 'system' && msg.content !== `callSid: ${this.callSid}`);
    
    if (systemMessageIndex !== -1) {
      this.userContext[systemMessageIndex].content = this.systemPrompt;
      console.log(`📝 System prompt updated for new personality`.green);
    } else {
      // If no system message found, add one at the beginning
      this.userContext.unshift({ 'role': 'system', 'content': this.systemPrompt });
    }
  }

  recordLatency(ttfb, rtt) {
    const entry = {
      ttfb: typeof ttfb === 'number' ? ttfb : null,
      rtt: typeof rtt === 'number' ? rtt : null
    };
    this.latencyHistory.push(entry);
    if (this.latencyHistory.length > this.maxLatencySamples) {
      this.latencyHistory.shift();
    }
  }

  getAdaptiveMaxTokens() {
    if (!this.latencyHistory.length) return this.maxTokens;
    const recent = this.latencyHistory.slice(-this.maxLatencySamples);
    const rtts = recent.map(r => r.rtt).filter(Boolean);
    if (!rtts.length) return this.maxTokens;
    const avg = rtts.reduce((a, b) => a + b, 0) / rtts.length;

    if (avg > 4500) {
      return Math.max(60, Math.floor(this.maxTokens * 0.5));
    }
    if (avg > 3000) {
      return Math.max(80, Math.floor(this.maxTokens * 0.7));
    }
    return this.maxTokens;
  }

  addToPhaseWindow(entry) {
    const phase = this.currentPhase || 'greeting';
    const store = this.phaseWindows[phase] || (this.phaseWindows[phase] = []);
    store.push(entry);
    if (store.length > this.maxPerPhase) {
      store.shift();
    }

    // Keep a small general window as a backstop
    this.phaseWindows.general.push(entry);
    if (this.phaseWindows.general.length > this.maxPerPhase) {
      this.phaseWindows.general.shift();
    }
  }

  buildModelMessages() {
    const messages = [{ role: 'system', content: this.systemPrompt }];
    if (this.metadataMessages.length) {
      messages.push(...this.metadataMessages.slice(-4));
    }

    if (this.sessionSummary) {
      messages.push({
        role: 'system',
        content: `Session summary (${this.summaryTurns} turns): ${this.sessionSummary}`
      });
    }

    if (this.longTermFacts.length) {
      const factLines = this.longTermFacts
        .slice(0, this.memoryFactLimit)
        .map((fact) => `- ${fact.text}`)
        .join('\n');
      messages.push({
        role: 'system',
        content: `Long-term facts:\n${factLines}`
      });
    }

    const phaseEntries = (this.phaseWindows[this.currentPhase] || []).slice(-this.maxPerPhase);
    const generalBackstop = this.phaseWindows.general.slice(-Math.max(3, this.recentTurns));
    const combined = [...phaseEntries, ...generalBackstop]
      .filter((entry) => entry && entry.role && (entry.content != null || Array.isArray(entry.tool_calls)));
    const deduped = [];
    const seen = new Set();
    for (const entry of combined) {
      const key = stableHash({
        role: entry.role,
        name: entry.name || '',
        tool_call_id: entry.tool_call_id || '',
        content: entry.content == null ? null : String(entry.content),
        tool_calls: Array.isArray(entry.tool_calls) ? entry.tool_calls : null
      });
      if (seen.has(key)) continue;
      seen.add(key);
      deduped.push(entry);
    }
    const recent = deduped.slice(-Math.max(4, this.recentTurns));
    recent.forEach((entry) => messages.push(entry));

    let tokenCount = this.estimateMessagesTokenCount(messages);
    while (tokenCount > this.contextTokenBudget && recent.length > 4) {
      const removed = recent.shift();
      const compact = this.buildSummarySnippet([removed]);
      if (compact) {
        this.sessionSummary = [this.sessionSummary, compact].filter(Boolean).join(' || ').slice(-this.summaryMaxChars);
      }
      const stableMessages = messages.filter((msg) => msg.role === 'system');
      stableMessages.push(...recent);
      messages.length = 0;
      messages.push(...stableMessages);
      tokenCount = this.estimateMessagesTokenCount(messages);
    }

    return messages;
  }

  // Get comprehensive conversation analysis
  getConversationAnalysis() {
    const personalityReport = this.personalityEngine.getAdaptationReport();
    
    return {
      totalInteractions: this.conversationHistory.length,
      personalityChanges: this.personalityChanges.length,
      currentPersonality: this.personalityEngine.currentPersonality,
      personalityHistory: this.personalityChanges,
      conversationFlow: this.conversationHistory.slice(-10), // Last 10 messages
      adaptationReport: personalityReport,
      contextLength: this.userContext.length,
      functionsAvailable: Object.keys(this.availableFunctions).length,
      dynamicTools: this.dynamicTools.map(tool => tool.function.name),
      memory: {
        summaryTurns: this.summaryTurns,
        summaryLength: this.sessionSummary.length,
        longTermFacts: this.longTermFacts.slice(0, this.memoryFactLimit)
      },
      observability: {
        traceId: this.executionContext.traceId || null,
        toolExecutionStats: this.toolExecutionStats
      }
    };
  }

  // Method to force personality switch (for testing or manual override)
  forcePersonalitySwitch(personalityName, reason = 'manual_override') {
    if (this.personalityEngine.personalities[personalityName]) {
      const oldPersonality = this.personalityEngine.currentPersonality;
      this.personalityEngine.currentPersonality = personalityName;
      
      const adaptedPrompt = this.personalityEngine.generateAdaptedPrompt();
      this.updateSystemPromptWithPersonality(adaptedPrompt);
      
      this.personalityChanges.push({
        from: oldPersonality,
        to: personalityName,
        trigger: { reason: reason },
        timestamp: new Date().toISOString(),
        manual: true
      });

      console.log(`🎭 Manually switched personality: ${oldPersonality} → ${personalityName}`.yellow);
      
      return {
        success: true,
        from: oldPersonality,
        to: personalityName,
        adaptedPrompt: adaptedPrompt
      };
    } else {
      console.log(`❌ Unknown personality: ${personalityName}`.red);
      return { success: false, error: 'Unknown personality' };
    }
  }

  // Add new dynamic function at runtime
  addDynamicFunction(toolDefinition, implementation) {
    const registered = this.registerToolDefinition(toolDefinition);
    if (!registered.ok || typeof implementation !== 'function') {
      return;
    }
    this.dynamicTools.push(registered.tool);
    this.availableFunctions[registered.tool.function.name] = implementation;
    this.toolRegistry.set(registered.tool.function.name, registered.meta);
    
    console.log(`🔧 Added dynamic function: ${registered.tool.function.name}`.green);
  }

  // Remove dynamic function
  removeDynamicFunction(functionName) {
    this.dynamicTools = this.dynamicTools.filter(tool => tool.function.name !== functionName);
    delete this.availableFunctions[functionName];
    this.toolRegistry.delete(functionName);
    
    console.log(`🔧 Removed dynamic function: ${functionName}`.yellow);
  }

  // Get function usage statistics
  getFunctionUsageStats() {
    const functionCalls = {};
    let totalFunctionCalls = 0;

    this.conversationHistory.forEach(msg => {
      if (msg.functionsUsed && msg.functionsUsed.length > 0) {
        msg.functionsUsed.forEach(funcName => {
          functionCalls[funcName] = (functionCalls[funcName] || 0) + 1;
          totalFunctionCalls++;
        });
      }
    });

    return {
      totalCalls: totalFunctionCalls,
      functionBreakdown: functionCalls,
      availableFunctions: Object.keys(this.availableFunctions),
      utilizationRate: this.conversationHistory.length > 0 ? 
        (totalFunctionCalls / this.conversationHistory.length * 100).toFixed(1) : 0
    };
  }

  // Reset for new conversation
  reset() {
    this.personalityEngine.reset();
    this.conversationHistory = [];
    this.personalityChanges = [];
    this.partialResponseIndex = 0;
    this.toolIdempotency.clear();
    this.toolBudget.clear();
    this.toolRegistry.clear();
    this.toolCircuitStates.clear();
    this.toolExecutionLocks.clear();
    this.sessionSummary = '';
    this.summaryTurns = 0;
    this.longTermFacts = [];
    this.factKeys.clear();
    this.rollupCursor = 0;
    this.memoryLoaded = false;
    this.toolExecutionStats = { total: 0, failed: 0 };
    this.metadataMessages = [];
    this.phaseWindows = {
      greeting: [],
      verification: [],
      resolution: [],
      closing: [],
      general: []
    };
    this.currentPhase = 'greeting';
    this.personalityPrompt = this.baseSystemPrompt;
    this.systemPrompt = this.composeSystemPrompt(this.baseSystemPrompt);
    
    // Reset user context but keep the base system prompt and first message
    this.userContext = [
      { 'role': 'system', 'content': this.systemPrompt },
      { 'role': 'assistant', 'content': this.firstMessage },
    ];
    this.addToPhaseWindow({ role: 'assistant', content: this.firstMessage });

    if (this.callSid) {
      this.metadataMessages.push({ role: 'system', content: `callSid: ${this.callSid}` });
    }

    console.log('🔄 Enhanced GPT Service reset for new conversation'.blue);
  }

  // Get current configuration with comprehensive info
  getConfiguration() {
    const functionStats = this.getFunctionUsageStats();
    
    return {
      isCustomConfiguration: this.isCustomConfiguration,
      systemPrompt: this.systemPrompt,
      firstMessage: this.firstMessage,
      contextLength: this.userContext.length,
      personalityEngine: this.getPersonalityInfo(),
      conversationAnalysis: this.getConversationAnalysis(),
      functionSystem: {
        dynamicFunctions: this.dynamicTools.length,
        availableFunctions: Object.keys(this.availableFunctions),
        registeredTools: [...this.toolRegistry.keys()],
        usageStats: functionStats
      },
      memory: {
        summary_turns: this.summaryTurns,
        summary_length: this.sessionSummary.length,
        long_term_facts: this.longTermFacts.length,
        token_budget: this.contextTokenBudget
      },
      observability: {
        trace_id: this.executionContext.traceId || null,
        request_id: this.executionContext.requestId || null,
        tool_exec_total: this.toolExecutionStats.total,
        tool_exec_failed: this.toolExecutionStats.failed
      }
    };
  }

  // Test dynamic function (for debugging)
  async testDynamicFunction(functionName, args) {
    if (!this.availableFunctions[functionName]) {
      return { success: false, error: `Function ${functionName} not found` };
    }

    try {
      const result = await this.availableFunctions[functionName](args);
      console.log(`🧪 Test result for ${functionName}:`, result);
      return { success: true, result: result };
    } catch (error) {
      console.error(`❌ Test failed for ${functionName}:`, error);
      return { success: false, error: error.message };
    }
  }

  // Get adaptation effectiveness score
  getAdaptationEffectiveness() {
    if (this.conversationHistory.length === 0) return 0;

    const userInteractions = this.conversationHistory.filter(msg => msg.role === 'user').length;
    const adaptations = this.personalityChanges.length;
    
    // Base effectiveness on adaptation frequency relative to conversation length
    const adaptationRate = userInteractions > 0 ? adaptations / userInteractions : 0;
    
    // Optimal range is 0.1-0.3 adaptations per user message
    let effectiveness;
    if (adaptationRate < 0.05) {
      effectiveness = 'under_adaptive'; // Too few adaptations
    } else if (adaptationRate > 0.5) {
      effectiveness = 'over_adaptive'; // Too many adaptations
    } else {
      effectiveness = 'well_adaptive'; // Good balance
    }
    
    return {
      score: Math.min(100, adaptationRate * 300), // Scale to 0-100
      rating: effectiveness,
      adaptations: adaptations,
      userInteractions: userInteractions,
      rate: (adaptationRate * 100).toFixed(1) + '%'
    };
  }

  // Export conversation data for analysis
  exportConversationData() {
    return {
      metadata: {
        callSid: this.callSid,
        traceId: this.executionContext.traceId || null,
        startTime: this.conversationHistory[0]?.timestamp,
        endTime: this.conversationHistory[this.conversationHistory.length - 1]?.timestamp,
        totalInteractions: this.conversationHistory.length,
        isCustomConfiguration: this.isCustomConfiguration
      },
      conversationFlow: this.conversationHistory,
      personalityAdaptations: this.personalityChanges,
      functionUsage: this.getFunctionUsageStats(),
      adaptationEffectiveness: this.getAdaptationEffectiveness(),
      finalState: {
        personality: this.personalityEngine.currentPersonality,
        contextLength: this.userContext.length,
        availableFunctions: Object.keys(this.availableFunctions),
        summaryTurns: this.summaryTurns,
        longTermFacts: this.longTermFacts.slice(0, this.memoryFactLimit)
      }
    };
  }
}

module.exports = { EnhancedGptService };
