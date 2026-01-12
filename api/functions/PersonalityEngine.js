class PersonalityEngine {
  constructor() {
    this.currentPersonality = 'default';
    this.personalityHistory = [];
    this.conversationContext = {
      customerMood: 'neutral',
      communicationStyle: 'unknown',
      urgencyLevel: 'normal',
      techSavviness: 'unknown',
      responsePatterns: [],
      keywordTriggers: new Set()
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
    
    // Update conversation context
    this.updateConversationContext(analysis, conversationHistory);
    
    // Determine best personality
    const recommendedPersonality = this.selectOptimalPersonality();
    
    // Switch personality if needed
    if (recommendedPersonality !== this.currentPersonality) {
      this.switchPersonality(recommendedPersonality);
    }

    // Generate adapted prompt
    const adaptedPrompt = this.generateAdaptedPrompt();
    
    return {
      personalityChanged: recommendedPersonality !== this.currentPersonality,
      previousPersonality: this.currentPersonality,
      currentPersonality: recommendedPersonality,
      adaptedPrompt: adaptedPrompt,
      analysis: analysis,
      context: this.conversationContext
    };
  }

  analyzeCustomerMessage(message) {
    const analysis = {
      mood: 'neutral',
      urgency: 'normal',
      techLevel: 'basic',
      communicationStyle: 'formal',
      messageLength: message.length,
      keywords: [],
      emotions: [],
      questionCount: (message.match(/\?/g) || []).length
    };

    const lowerMessage = message.toLowerCase();

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
    if (message.length < 20) {
      analysis.communicationStyle = 'brief';
    } else if (message.length > 100) {
      analysis.communicationStyle = 'detailed';
    }

    // Extract keywords
    for (const [category, words] of Object.entries(this.triggers)) {
      const foundWords = words.filter(word => lowerMessage.includes(word));
      if (foundWords.length > 0) {
        analysis.keywords.push({ category, words: foundWords });
      }
    }

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
    }
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

    // Add conversation style guidance
    basePrompt += `Adapt your response length to match the customer's communication style: `;
    if (context.communicationStyle === 'brief') {
      basePrompt += `they prefer short, to-the-point responses. `;
    } else if (context.communicationStyle === 'detailed') {
      basePrompt += `they appreciate thorough, detailed explanations. `;
    }

    // Add the standard ending
    basePrompt += `Always end responses with a "•" symbol every 5-10 words for natural speech pauses.`;

    return basePrompt;
  }

  // Utility method to check if message contains specific words
  containsWords(message, words) {
    return words.some(word => message.includes(word.toLowerCase()));
  }

  // Get current personality info
  getCurrentPersonality() {
    return {
      name: this.currentPersonality,
      profile: this.personalities[this.currentPersonality],
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
      responsePatterns: [],
      keywordTriggers: new Set()
    };
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