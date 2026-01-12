require('colors');
const EventEmitter = require('events');
const OpenAI = require('openai');
const PersonalityEngine = require('../functions/PersonalityEngine');
const config = require('../config');

class EnhancedGptService extends EventEmitter {
  constructor(customPrompt = null, customFirstMessage = null) {
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
    this.latencyHistory = [];
    this.maxLatencySamples = 8;
    this.brevityHint = 'Keep spoken replies concise: max 2 sentences, ~200 characters, and avoid rambling.';
    
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
    return [
      personalityBlock,
      this.currentProfilePrompt,
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
    this.systemPrompt = this.composeSystemPrompt();
    this.updateSystemPromptWithPersonality(this.personalityPrompt);
    console.log(`💪 Call profile set: ${this.currentProfileName}`.blue);
  }

  // Set dynamic functions for this conversation
  setDynamicFunctions(tools, implementations) {
    this.dynamicTools = tools;
    this.availableFunctions = implementations;
    
    console.log(`🔧 Loaded ${tools.length} dynamic functions: ${Object.keys(implementations).join(', ')}`.blue);
  }

  // Add the callSid to the chat context
  setCallSid(callSid) {
    this.callSid = callSid;
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
    try {
      return JSON.parse(args);
    } catch (error) {
      console.log('Warning: Double function arguments returned by OpenRouter:', args);
      if (args.indexOf('{') != args.lastIndexOf('{')) {
        const start = args.indexOf('{');
        const end = args.lastIndexOf('}');
        if (start !== -1 && end !== -1 && end > start) {
          return JSON.parse(args.substring(start, end + 1));
        }
      }
    }
    return {};
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

  // Enhanced completion method with dynamic functions and personality adaptation
  async completion(text, interactionCount, role = 'user', name = 'user') {
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

      console.log(`🎯 Current: ${adaptation.currentPersonality} | Mood: ${adaptation.context.customerMood}`.cyan);
    }

    this.updateUserContext(name, role, text);

    // Use sanitized tools for the model (strip custom fields like "say"/"returns")
    const toolsToUse = this.getSanitizedTools();
    const adaptiveMaxTokens = this.getAdaptiveMaxTokens();
    const messages = this.buildModelMessages();

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

    const maxAttempts = 2;
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
        const retriable = (err?.status && err.status >= 500) || err?.code === 502;
        const canFallback = this.backupModel && currentModel === this.model;
        if (attempt >= maxAttempts && !canFallback) {
          handleFailure(err);
          return;
        }
        if (canFallback && retriable) {
          currentModel = this.backupModel;
        }
        clearTimers();
        await new Promise((resolve) => setTimeout(resolve, 400));
      }
    }

    let completeResponse = '';
    let partialResponse = '';
    let functionName = '';
    let functionArgs = '';
    let functionCallId = '';
    let finishReason = '';
    let streamError = null;

    function collectToolInformation(deltas) {
      let name = deltas.tool_calls[0]?.function?.name || '';
      if (name != '') {
        functionName = name;
      }
      const callId = deltas.tool_calls[0]?.id;
      if (callId) {
        functionCallId = callId;
      }
      let args = deltas.tool_calls[0]?.function?.arguments || '';
      if (args != '') {
        functionArgs += args;
      }
    }

    try {
      for await (const chunk of stream) {
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
        let content = chunk.choices[0]?.delta?.content || '';
        let deltas = chunk.choices[0].delta;
        finishReason = chunk.choices[0].finish_reason;

        if (deltas.tool_calls) {
          collectToolInformation(deltas);
        }

        if (finishReason === 'tool_calls') {
          if (!functionName) {
            console.error('❌ Tool call requested without a function name.'.red);
            continue;
          }

          const functionToCall = this.availableFunctions[functionName];
          const validatedArgs = this.validateFunctionArgs(functionArgs);
          const toolCallId = functionCallId || `tool_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
          const toolArgs = functionArgs && String(functionArgs).trim().length > 0
            ? String(functionArgs)
            : JSON.stringify(validatedArgs || {});

          // Record the assistant tool call so the next tool message is valid
          const toolCallMessage = {
            role: 'assistant',
            content: null,
            tool_calls: [
              {
                id: toolCallId,
                type: 'function',
                function: {
                  name: functionName,
                  arguments: toolArgs
                }
              }
            ]
          };
          this.userContext.push(toolCallMessage);
          this.addToPhaseWindow(toolCallMessage);

          // Find the corresponding tool data for the "say" message
          const toolData = this.dynamicTools.find(tool => tool.function.name === functionName);
          const say = toolData?.function?.say || 'One moment please...';

          // Emit the function call response with personality context
          this.emit('gptreply', {
            partialResponseIndex: null,
            partialResponse: say,
            personalityInfo: this.personalityEngine.getCurrentPersonality()
          }, interactionCount);

          let functionResponse;
          try {
            if (!functionToCall) {
              throw new Error(`Function ${functionName} not available`);
            }
            functionResponse = await functionToCall(validatedArgs);
            console.log(`🔧 Executed dynamic function: ${functionName}`.green);
          } catch (functionError) {
            console.error(`❌ Error executing function ${functionName}:`, functionError);
            functionResponse = JSON.stringify({ error: 'Function execution failed', details: functionError.message });
          }

          const responseText = typeof functionResponse === 'string'
            ? functionResponse
            : JSON.stringify(functionResponse);

          // For digit collection, wait for user input instead of continuing immediately
          if (functionName === 'collect_digits') {
            this.updateUserContext(toolCallId, 'tool', responseText);
            return;
          }

          // Continue completion with function response
          await this.completion(responseText, interactionCount, 'tool', toolCallId);
        } else {
          completeResponse += content;
          partialResponse += content;

          if (content.trim().slice(-1) === '•' || finishReason === 'stop') {
            const gptReply = { 
              partialResponseIndex: this.partialResponseIndex,
              partialResponse,
              personalityInfo: this.personalityEngine.getCurrentPersonality(),
              adaptationHistory: this.personalityChanges.slice(-3), // Last 3 changes
              functionsAvailable: Object.keys(this.availableFunctions).length
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

    // Store AI response in conversation history
    this.conversationHistory.push({
      role: 'assistant',
      content: completeResponse,
      timestamp: new Date().toISOString(),
      interactionCount: interactionCount,
      personality: this.personalityEngine.currentPersonality,
      functionsUsed: functionName ? [functionName] : []
    });

    this.userContext.push({'role': 'assistant', 'content': completeResponse});
    this.addToPhaseWindow({ role: 'assistant', content: completeResponse });
    
    console.log(`🧠 Context: ${this.userContext.length} | Personality: ${this.personalityEngine.currentPersonality} | Functions: ${Object.keys(this.availableFunctions).length}`.green);

    // Record latency metrics
    const finishedAt = Date.now();
    const ttfb = firstChunkAt ? (firstChunkAt - startedAt) : null;
    const rtt = finishedAt - startedAt;
    this.recordLatency(ttfb, rtt);
    console.log(`Latency | model: ${currentModel} | ttfb: ${ttfb}ms | rtt: ${rtt}ms`);
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
    const messages = [];
    messages.push({ role: 'system', content: this.systemPrompt });
    if (this.metadataMessages.length) {
      messages.push(...this.metadataMessages);
    }

    const phaseEntries = (this.phaseWindows[this.currentPhase] || []).slice(-this.maxPerPhase);
    const generalBackstop = this.phaseWindows.general.slice(-3);
    const combined = [...phaseEntries, ...generalBackstop];

    for (const entry of combined) {
      messages.push(entry);
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
      dynamicTools: this.dynamicTools.map(tool => tool.function.name)
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
    this.dynamicTools.push(toolDefinition);
    this.availableFunctions[toolDefinition.function.name] = implementation;
    
    console.log(`🔧 Added dynamic function: ${toolDefinition.function.name}`.green);
  }

  // Remove dynamic function
  removeDynamicFunction(functionName) {
    this.dynamicTools = this.dynamicTools.filter(tool => tool.function.name !== functionName);
    delete this.availableFunctions[functionName];
    
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
    
    // Reset user context but keep the base system prompt and first message
    this.userContext = [
      { 'role': 'system', 'content': this.baseSystemPrompt },
      { 'role': 'assistant', 'content': this.firstMessage },
    ];
    
    if (this.callSid) {
      this.userContext.push({ 'role': 'system', 'content': `callSid: ${this.callSid}` });
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
        usageStats: functionStats
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
        availableFunctions: Object.keys(this.availableFunctions)
      }
    };
  }
}

module.exports = { EnhancedGptService };
