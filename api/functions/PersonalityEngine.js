class PersonalityEngine {
  constructor() {
    this.currentPersonality = 'default';
    this.personalityHistory = [];
    this.lastAnalysis = null;
    this.conversationContext = {
      customerMood: 'neutral',
      communicationStyle: 'unknown',
      urgencyLevel: 'normal',
      techSavviness: 'unknown',
      sentimentScore: 0,
      confusionScore: 0,
      responsePatterns: [],
      keywordTriggers: new Set()
    };
    this.personaContext = {
      domain: 'general',
      channel: 'voice',
      urgency: 'normal'
    };
    this.personaDsl = {
      base: 'Use natural spoken language. Keep responses concise, actionable, and easy to follow.',
      domains: {
        general: 'Domain: general assistance. Stay practical and avoid speculative claims.',
        sales: 'Domain: sales. Focus on value, discovery, and a single clear next step.',
        support: 'Domain: support. Prioritize diagnosis, clear steps, and confirmation of progress.',
        collections: 'Domain: collections. Be firm but respectful, and keep commitments explicit.',
        verification: 'Domain: verification. Prioritize identity confirmation and sensitive-data safety.'
      },
      channels: {
        voice: 'Channel: voice. Prefer short utterances with clear pauses.',
        sms: 'Channel: sms. Keep text compact and skimmable.',
        email: 'Channel: email. Use structured and slightly more formal prose.'
      },
      urgency: {
        low: 'Urgency: low. Be patient and informative.',
        normal: 'Urgency: normal. Balance speed with clarity.',
        high: 'Urgency: high. Be direct, decisive, and focused on immediate next steps.'
      }
    };
    
    // Define personality profiles
    this.personalities = {
      default: {
        name: 'Professional Helper',
        tone: 'professional',
        pace: 'moderate',
        formality: 'medium',
        enthusiasm: 'moderate',
        patience: 'high',
        verbosity: 'balanced'
      },
      
      efficient: {
        name: 'Quick & Direct',
        tone: 'business-like',
        pace: 'fast',
        formality: 'low',
        enthusiasm: 'low',
        patience: 'medium',
        verbosity: 'concise'
      },
      
      patient_teacher: {
        name: 'Patient Educator',
        tone: 'warm',
        pace: 'slow',
        formality: 'medium',
        enthusiasm: 'moderate',
        patience: 'very_high',
        verbosity: 'detailed'
      },
      
      enthusiastic_seller: {
        name: 'Energetic Closer',
        tone: 'excited',
        pace: 'moderate',
        formality: 'low',
        enthusiasm: 'high',
        patience: 'medium',
        verbosity: 'persuasive'
      },
      
      technical_expert: {
        name: 'Tech Specialist',
        tone: 'knowledgeable',
        pace: 'moderate',
        formality: 'high',
        enthusiasm: 'moderate',
        patience: 'high',
        verbosity: 'technical'
      },
      
      friendly_casual: {
        name: 'Casual Friend',
        tone: 'relaxed',
        pace: 'moderate',
        formality: 'very_low',
        enthusiasm: 'moderate',
        patience: 'high',
        verbosity: 'conversational'
      },
      
      crisis_manager: {
        name: 'Problem Solver',
        tone: 'calm',
        pace: 'slow',
        formality: 'high',
        enthusiasm: 'low',
        patience: 'very_high',
        verbosity: 'solution_focused'
      }
    };

    // Personality switching triggers
    this.triggers = {
      // Customer mood indicators
      frustrated: ['frustrated', 'angry', 'annoyed', 'upset', 'problem', 'issue', 'wrong', 'terrible', 'awful'],
      confused: ['confused', 'don\'t understand', 'what do you mean', 'unclear', 'explain', 'how does', 'what is'],
      hurried: ['quickly', 'fast', 'in a hurry', 'no time', 'brief', 'short', 'quick'],
      technical: ['specifications', 'technical', 'features', 'compatibility', 'processor', 'memory', 'bandwidth'],
      casual: ['hey', 'yo', 'sup', 'cool', 'awesome', 'dude', 'yeah', 'nah'],
      price_sensitive: ['cheap', 'expensive', 'cost', 'price', 'budget', 'affordable', 'deal', 'discount']
    };

    // Response analysis patterns
    this.responsePatterns = {
      short_responses: /^.{1,10}$/,
      long_responses: /^.{50,}$/,
      questions: /\?/g,
      technical_terms: /\b(specification|feature|compatibility|performance|technical|processor|memory|storage)\b/gi,
      emotional_words: /\b(love|hate|frustrated|excited|disappointed|happy|angry|confused)\b/gi,
      urgency_words: /\b(now|immediately|asap|urgent|quickly|fast|hurry)\b/gi
    };
  }

  // Main method to analyze customer input and adapt personality
  adaptPersonality(customerMessage, conversationHistory = []) {
    // Analyze current message
    const analysis = this.analyzeCustomerMessage(customerMessage);
    this.lastAnalysis = analysis;
    
    // Update conversation context
    this.updateConversationContext(analysis, conversationHistory);
    
    // Determine best personality
    const recommendedPersonality = this.selectOptimalPersonality();
    const previousPersonality = this.currentPersonality;
    
    // Switch personality if needed
    if (recommendedPersonality !== this.currentPersonality) {
      this.switchPersonality(recommendedPersonality);
    }

    // Generate adapted prompt
    const adaptedPrompt = this.generateAdaptedPrompt();
    
    return {
      personalityChanged: recommendedPersonality !== previousPersonality,
      previousPersonality,
      currentPersonality: recommendedPersonality,
      adaptedPrompt: adaptedPrompt,
      analysis: analysis,
      context: this.conversationContext,
      toneDirective: this.buildAdaptiveToneDirective(analysis),
      personaDslPrompt: this.getPersonaDslPrompt()
    };
  }

  analyzeCustomerMessage(message) {
    const safeMessage = String(message || '');
    const analysis = {
      mood: 'neutral',
      urgency: 'normal',
      techLevel: 'basic',
      communicationStyle: 'formal',
      messageLength: safeMessage.length,
      keywords: [],
      emotions: [],
      questionCount: (safeMessage.match(/\?/g) || []).length,
      sentimentScore: 0,
      confusionScore: 0
    };

    const lowerMessage = safeMessage.toLowerCase();

    // Analyze mood
    if (this.containsWords(lowerMessage, this.triggers.frustrated)) {
      analysis.mood = 'frustrated';
    } else if (this.containsWords(lowerMessage, this.triggers.confused)) {
      analysis.mood = 'confused';
    } else if (this.containsWords(lowerMessage, this.triggers.casual)) {
      analysis.mood = 'casual';
    }

    // Analyze urgency
    if (this.containsWords(lowerMessage, this.triggers.hurried)) {
      analysis.urgency = 'high';
    }

    // Analyze technical level
    if (this.containsWords(lowerMessage, this.triggers.technical)) {
      analysis.techLevel = 'advanced';
    }

    // Analyze communication style
    if (safeMessage.length < 20) {
      analysis.communicationStyle = 'brief';
    } else if (safeMessage.length > 100) {
      analysis.communicationStyle = 'detailed';
    }

    // Extract keywords
    for (const [category, words] of Object.entries(this.triggers)) {
      const foundWords = words.filter(word => lowerMessage.includes(word));
      if (foundWords.length > 0) {
        analysis.keywords.push({ category, words: foundWords });
      }
    }

    const positiveLexicon = ['thanks', 'thank', 'great', 'good', 'perfect', 'awesome', 'excellent', 'helpful'];
    const negativeLexicon = ['bad', 'terrible', 'awful', 'frustrated', 'angry', 'hate', 'useless', 'broken'];
    const positiveHits = positiveLexicon.filter((word) => lowerMessage.includes(word)).length;
    const negativeHits = negativeLexicon.filter((word) => lowerMessage.includes(word)).length;
    analysis.sentimentScore = Math.max(-1, Math.min(1, (positiveHits - negativeHits) / 3));
    const confusionSignals = ['confused', "don't understand", 'what do you mean', 'unclear', 'not sure', 'explain'];
    const confusionHits = confusionSignals.filter((word) => lowerMessage.includes(word)).length;
    analysis.confusionScore = Math.max(0, Math.min(1, (confusionHits + analysis.questionCount) / 4));

    return analysis;
  }

  updateConversationContext(analysis, conversationHistory) {
    // Update mood tracking
    this.conversationContext.customerMood = analysis.mood;
    
    // Update communication patterns
    this.conversationContext.responsePatterns.push({
      length: analysis.messageLength,
      mood: analysis.mood,
      urgency: analysis.urgency,
      sentimentScore: analysis.sentimentScore,
      confusionScore: analysis.confusionScore,
      timestamp: new Date().toISOString()
    });

    // Keep only last 10 patterns
    if (this.conversationContext.responsePatterns.length > 10) {
      this.conversationContext.responsePatterns = this.conversationContext.responsePatterns.slice(-10);
    }

    // Update keyword triggers
    analysis.keywords.forEach(keyword => {
      keyword.words.forEach(word => {
        this.conversationContext.keywordTriggers.add(word);
      });
    });

    // Analyze conversation trends
    const recentPatterns = this.conversationContext.responsePatterns.slice(-5);
    const avgLength = recentPatterns.reduce((sum, p) => sum + p.length, 0) / recentPatterns.length;
    
    if (avgLength < 20) {
      this.conversationContext.communicationStyle = 'brief';
    } else if (avgLength > 80) {
      this.conversationContext.communicationStyle = 'detailed';
    } else {
      this.conversationContext.communicationStyle = 'moderate';
    }

    // Check urgency level
    const urgentResponses = recentPatterns.filter(p => p.urgency === 'high').length;
    if (urgentResponses >= 2) {
      this.conversationContext.urgencyLevel = 'high';
    } else if (urgentResponses === 0) {
      this.conversationContext.urgencyLevel = 'normal';
    }

    const sentimentAvg = recentPatterns.reduce((sum, p) => sum + Number(p.sentimentScore || 0), 0) / recentPatterns.length;
    const confusionAvg = recentPatterns.reduce((sum, p) => sum + Number(p.confusionScore || 0), 0) / recentPatterns.length;
    this.conversationContext.sentimentScore = Number.isFinite(sentimentAvg) ? Number(sentimentAvg.toFixed(2)) : 0;
    this.conversationContext.confusionScore = Number.isFinite(confusionAvg) ? Number(confusionAvg.toFixed(2)) : 0;
  }

  selectOptimalPersonality() {
    const context = this.conversationContext;
    
    // Rule-based personality selection
    
    // Crisis situations - customer is frustrated or has problems
    if (context.customerMood === 'frustrated') {
      return 'crisis_manager';
    }
    
    // Customer is confused - needs patient explanation
    if (context.customerMood === 'confused') {
      return 'patient_teacher';
    }
    
    // Customer is in a hurry - be efficient
    if (context.urgencyLevel === 'high') {
      return 'efficient';
    }
    
    // Technical discussion detected
    if (this.conversationContext.keywordTriggers.has('technical') || 
        this.conversationContext.keywordTriggers.has('specifications')) {
      return 'technical_expert';
    }
    
    // Casual conversation style
    if (context.customerMood === 'casual' && context.communicationStyle === 'brief') {
      return 'friendly_casual';
    }
    
    // Price-focused conversation
    if (this.conversationContext.keywordTriggers.has('price') || 
        this.conversationContext.keywordTriggers.has('budget')) {
      return 'enthusiastic_seller';
    }
    
    // Default personality
    return 'default';
  }

  switchPersonality(newPersonality) {
    if (this.personalities[newPersonality]) {
      this.personalityHistory.push({
        from: this.currentPersonality,
        to: newPersonality,
        timestamp: new Date().toISOString(),
        context: { ...this.conversationContext }
      });
      
      this.currentPersonality = newPersonality;
      console.log(`🎭 Personality switched to: ${this.personalities[newPersonality].name}`.cyan);
    }
  }

  generateAdaptedPrompt() {
    const personality = this.personalities[this.currentPersonality];
    const context = this.conversationContext;
    const latestAnalysis = this.lastAnalysis || {};
    
    let basePrompt = `You are a ${personality.name} AI sales representative. `;
    
    // Add personality-specific instructions
    switch (this.currentPersonality) {
      case 'efficient':
        basePrompt += `Be direct, concise, and time-conscious. Get to the point quickly without unnecessary small talk. `;
        break;
        
      case 'patient_teacher':
        basePrompt += `Take time to explain things clearly and thoroughly. Be patient with questions and break down complex information into simple steps. `;
        break;
        
      case 'enthusiastic_seller':
        basePrompt += `Be energetic and persuasive. Focus on benefits, value propositions, and creating excitement about the product. `;
        break;
        
      case 'technical_expert':
        basePrompt += `Provide detailed technical information. Use proper technical terminology and focus on specifications, features, and compatibility. `;
        break;
        
      case 'friendly_casual':
        basePrompt += `Use a relaxed, conversational tone. Be approachable and personable, like talking to a friend. `;
        break;
        
      case 'crisis_manager':
        basePrompt += `Stay calm and solution-focused. Acknowledge concerns professionally and work systematically to resolve issues. `;
        break;
        
      default:
        basePrompt += `Maintain a professional, helpful demeanor while being adaptable to the customer's needs. `;
    }

    // Add context-specific adaptations
    if (context.urgencyLevel === 'high') {
      basePrompt += `The customer seems to be in a hurry, so be more concise and direct. `;
    }

    if (context.customerMood === 'frustrated') {
      basePrompt += `The customer seems frustrated, so be extra patient and focus on solving their problem. `;
    }
    if ((context.confusionScore || 0) >= 0.5 || latestAnalysis.mood === 'confused') {
      basePrompt += `Use shorter steps, check understanding, and avoid jargon unless asked. `;
    }
    if ((context.sentimentScore || 0) < -0.3) {
      basePrompt += `Acknowledge friction early and emphasize resolution. `;
    }

    // Add conversation style guidance
    basePrompt += `Adapt your response length to match the customer's communication style: `;
    if (context.communicationStyle === 'brief') {
      basePrompt += `they prefer short, to-the-point responses. `;
    } else if (context.communicationStyle === 'detailed') {
      basePrompt += `they appreciate thorough, detailed explanations. `;
    }

    // Add persona DSL layers (domain/channel/urgency)
    const personaDslPrompt = this.getPersonaDslPrompt();
    if (personaDslPrompt) {
      basePrompt += `${personaDslPrompt} `;
    }

    // Add adaptive tone controller directive
    const toneDirective = this.buildAdaptiveToneDirective(latestAnalysis);
    if (toneDirective) {
      basePrompt += `${toneDirective} `;
    }

    // Add the standard ending
    basePrompt += `Always end responses with a "•" symbol every 5-10 words for natural speech pauses.`;

    return basePrompt;
  }

  // Utility method to check if message contains specific words
  containsWords(message, words) {
    return words.some(word => message.includes(word.toLowerCase()));
  }

  setPersonaContext(context = {}) {
    if (!context || typeof context !== 'object') return;
    const next = { ...this.personaContext };
    if (context.domain) {
      next.domain = String(context.domain).toLowerCase().trim();
    }
    if (context.channel) {
      next.channel = String(context.channel).toLowerCase().trim();
    }
    if (context.urgency) {
      next.urgency = String(context.urgency).toLowerCase().trim();
    }
    this.personaContext = next;
  }

  getPersonaDslPrompt() {
    const domain = this.personaDsl.domains[this.personaContext.domain] || this.personaDsl.domains.general;
    const channel = this.personaDsl.channels[this.personaContext.channel] || this.personaDsl.channels.voice;
    const urgency = this.personaDsl.urgency[this.personaContext.urgency] || this.personaDsl.urgency.normal;
    return [this.personaDsl.base, domain, channel, urgency].join(' ');
  }

  buildAdaptiveToneDirective(analysis = {}) {
    const mood = analysis.mood || this.conversationContext.customerMood;
    const urgency = analysis.urgency || this.conversationContext.urgencyLevel;
    const confusion = Number(analysis.confusionScore ?? this.conversationContext.confusionScore ?? 0);

    if (mood === 'frustrated') {
      return 'Tone controller: respond calmly, validate concern, and give one clear action at a time.';
    }
    if (confusion >= 0.5) {
      return 'Tone controller: simplify wording, avoid assumptions, and confirm understanding in plain language.';
    }
    if (urgency === 'high') {
      return 'Tone controller: prioritize immediate next action and avoid long explanations.';
    }
    if (mood === 'casual') {
      return 'Tone controller: stay friendly and concise while preserving professional boundaries.';
    }
    return 'Tone controller: keep balanced, clear, and efficient responses.';
  }

  evaluateConsistency(text = '') {
    const content = String(text || '').trim();
    if (!content) {
      return { score: 1, issues: [] };
    }
    const issues = [];
    const maxLen = this.conversationContext.urgencyLevel === 'high' ? 220 : 420;
    if (content.length > maxLen) {
      issues.push('too_verbose');
    }
    const exclamationCount = (content.match(/!/g) || []).length;
    if (this.currentPersonality === 'crisis_manager' && exclamationCount > 1) {
      issues.push('over_excited_for_crisis_tone');
    }
    if (this.currentPersonality === 'efficient' && content.split(/\s+/).length > 60) {
      issues.push('too_wordy_for_efficient_tone');
    }
    if (this.currentPersonality === 'patient_teacher' && content.split(/[.!?]/).filter(Boolean).length < 2) {
      issues.push('insufficient_explanation');
    }
    const score = Math.max(0, 1 - (issues.length * 0.25));
    return { score, issues };
  }

  correctResponseDrift(text = '') {
    const raw = String(text || '');
    if (!raw) return raw;
    let corrected = raw.replace(/!{2,}/g, '!');
    if (this.conversationContext.urgencyLevel === 'high' && corrected.length > 220) {
      corrected = `${corrected.slice(0, 217).trimEnd()}...`;
    }
    if (this.currentPersonality === 'crisis_manager' && !/^I understand|^I hear you/i.test(corrected) && this.conversationContext.customerMood === 'frustrated') {
      corrected = `I understand the urgency. ${corrected}`.trim();
    }
    return corrected;
  }

  // Get current personality info
  getCurrentPersonality() {
    return {
      name: this.currentPersonality,
      profile: this.personalities[this.currentPersonality],
      personaContext: this.personaContext,
      context: this.conversationContext,
      history: this.personalityHistory
    };
  }

  // Reset personality engine for new conversation
  reset() {
    this.currentPersonality = 'default';
    this.personalityHistory = [];
    this.conversationContext = {
      customerMood: 'neutral',
      communicationStyle: 'unknown',
      urgencyLevel: 'normal',
      techSavviness: 'unknown',
      sentimentScore: 0,
      confusionScore: 0,
      responsePatterns: [],
      keywordTriggers: new Set()
    };
    this.personaContext = {
      domain: 'general',
      channel: 'voice',
      urgency: 'normal'
    };
    this.lastAnalysis = null;
  }

  // Get personality adaptation report
  getAdaptationReport() {
    return {
      currentPersonality: this.personalities[this.currentPersonality].name,
      totalSwitches: this.personalityHistory.length,
      adaptationHistory: this.personalityHistory,
      conversationInsights: {
        dominantMood: this.conversationContext.customerMood,
        communicationStyle: this.conversationContext.communicationStyle,
        urgencyLevel: this.conversationContext.urgencyLevel,
        keyTopics: Array.from(this.conversationContext.keywordTriggers).slice(0, 10)
      }
    };
  }
}

module.exports = PersonalityEngine;
