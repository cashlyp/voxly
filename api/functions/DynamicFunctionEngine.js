const fs = require('fs');
const path = require('path');

class DynamicFunctionEngine {
  constructor() {
    this.functionRegistry = new Map();
    this.businessContext = null;
    this.customFunctions = new Map();
    this.functionTemplates = new Map();
    this.initializeCoreTemplates();
  }

  // Initialize core function templates that can adapt to any business
  initializeCoreTemplates() {
    this.functionTemplates.set('inventory_check', {
      name: 'checkInventory',
      description: 'Check inventory/availability of products or services',
      parameters: {
        type: 'object',
        properties: {
          item: { type: 'string', description: 'Product or service to check' },
          variant: { type: 'string', description: 'Specific variant, model, or type' },
          location: { type: 'string', description: 'Location or store (if applicable)' }
        },
        required: ['item']
      },
      implementation: this.createInventoryFunction.bind(this)
    });

    this.functionTemplates.set('pricing_check', {
      name: 'checkPrice',
      description: 'Get pricing information for products or services',
      parameters: {
        type: 'object',
        properties: {
          item: { type: 'string', description: 'Product or service name' },
          variant: { type: 'string', description: 'Specific variant or package' },
          quantity: { type: 'integer', description: 'Quantity for bulk pricing' }
        },
        required: ['item']
      },
      implementation: this.createPricingFunction.bind(this)
    });

    this.functionTemplates.set('booking_scheduling', {
      name: 'scheduleAppointment',
      description: 'Schedule appointments or bookings',
      parameters: {
        type: 'object',
        properties: {
          service: { type: 'string', description: 'Service or appointment type' },
          date: { type: 'string', description: 'Preferred date' },
          time: { type: 'string', description: 'Preferred time' },
          duration: { type: 'integer', description: 'Duration in minutes' }
        },
        required: ['service', 'date', 'time']
      },
      implementation: this.createSchedulingFunction.bind(this)
    });

    this.functionTemplates.set('order_placement', {
      name: 'placeOrder',
      description: 'Place orders for products or services',
      parameters: {
        type: 'object',
        properties: {
          items: { type: 'array', items: { type: 'object' }, description: 'Array of items to order' },
          customerInfo: { type: 'object', description: 'Customer information' },
          paymentMethod: { type: 'string', description: 'Payment method' }
        },
        required: ['items']
      },
      implementation: this.createOrderFunction.bind(this)
    });

    this.functionTemplates.set('information_lookup', {
      name: 'lookupInformation',
      description: 'Look up detailed information about products, services, or policies',
      parameters: {
        type: 'object',
        properties: {
          topic: { type: 'string', description: 'What to look up' },
          category: { type: 'string', description: 'Category or type of information' },
          details: { type: 'string', description: 'Specific details requested' }
        },
        required: ['topic']
      },
      implementation: this.createLookupFunction.bind(this)
    });

    this.functionTemplates.set('customer_support', {
      name: 'handleSupport',
      description: 'Handle customer support requests and issues',
      parameters: {
        type: 'object',
        properties: {
          issue: { type: 'string', description: 'Description of the issue' },
          category: { type: 'string', description: 'Issue category (technical, billing, etc.)' },
          urgency: { type: 'string', enum: ['low', 'medium', 'high'], description: 'Issue urgency' }
        },
        required: ['issue']
      },
      implementation: this.createSupportFunction.bind(this)
    });

    this.functionTemplates.set('lead_qualification', {
      name: 'qualifyLead',
      description: 'Qualify potential customers and gather requirements',
      parameters: {
        type: 'object',
        properties: {
          budget: { type: 'string', description: 'Budget range' },
          timeline: { type: 'string', description: 'When they need the solution' },
          requirements: { type: 'array', items: { type: 'string' }, description: 'List of requirements' }
        },
        required: ['budget']
      },
      implementation: this.createLeadQualificationFunction.bind(this)
    });
  }

  // Analyze business context from prompt and generate appropriate functions
  generateFunctionsFromPrompt(prompt, firstMessage) {
    const analysis = this.analyzeBusinessContext(prompt, firstMessage);
    this.businessContext = analysis;
    
    console.log(`Detected business context: ${analysis.industry} - ${analysis.businessType}`.cyan);
    console.log(`📋 Suggested functions: ${analysis.suggestedFunctions.join(', ')}`.blue);

    const functions = [];
    const functionImplementations = {};

    // Generate functions based on detected context
    analysis.suggestedFunctions.forEach(functionType => {
      if (this.functionTemplates.has(functionType)) {
        const template = this.functionTemplates.get(functionType);
        const adaptedFunction = this.adaptFunctionToContext(template, analysis);
        
        functions.push(adaptedFunction.manifest);
        functionImplementations[adaptedFunction.name] = adaptedFunction.implementation;
      }
    });

    // Always include transfer function
    functions.push(this.createTransferFunction());
    functionImplementations['transferCall'] = this.getTransferImplementation();

    return {
      functions,
      implementations: functionImplementations,
      context: analysis
    };
  }

  // Analyze business context from prompt
  analyzeBusinessContext(prompt, firstMessage) {
    const combinedText = `${prompt} ${firstMessage}`.toLowerCase();
    
    const analysis = {
      industry: 'general',
      businessType: 'sales',
      products: [],
      services: [],
      suggestedFunctions: [],
      keyTerms: [],
      customerActions: []
    };

    // Industry detection
    const industryPatterns = {
      'retail': ['store', 'shop', 'buy', 'purchase', 'product', 'sale', 'discount'],
      'healthcare': ['appointment', 'doctor', 'medical', 'health', 'clinic', 'patient'],
      'real_estate': ['property', 'house', 'apartment', 'rent', 'mortgage', 'real estate'],
      'automotive': ['car', 'vehicle', 'auto', 'dealership', 'lease', 'finance'],
      'technology': ['software', 'app', 'tech', 'digital', 'platform', 'system'],
      'finance': ['loan', 'investment', 'insurance', 'bank', 'credit', 'financial'],
      'education': ['course', 'training', 'learn', 'education', 'school', 'certification'],
      'food_service': ['restaurant', 'food', 'delivery', 'menu', 'order', 'reservation']
    };

    for (const [industry, keywords] of Object.entries(industryPatterns)) {
      const matches = keywords.filter(keyword => combinedText.includes(keyword));
      if (matches.length >= 2) {
        analysis.industry = industry;
        analysis.keyTerms.push(...matches);
        break;
      }
    }

    // Business type detection
    if (combinedText.includes('appointment') || combinedText.includes('schedule') || combinedText.includes('booking')) {
      analysis.businessType = 'appointment_based';
      analysis.suggestedFunctions.push('booking_scheduling');
    }
    
    if (combinedText.includes('sell') || combinedText.includes('buy') || combinedText.includes('purchase')) {
      analysis.businessType = 'sales';
      analysis.suggestedFunctions.push('inventory_check', 'pricing_check', 'order_placement');
    }
    
    if (combinedText.includes('support') || combinedText.includes('help') || combinedText.includes('issue')) {
      analysis.suggestedFunctions.push('customer_support');
    }

    // Always include information lookup for flexibility
    analysis.suggestedFunctions.push('information_lookup');

    // Add lead qualification for sales contexts
    if (analysis.businessType === 'sales') {
      analysis.suggestedFunctions.push('lead_qualification');
    }

    // Extract products/services mentioned
    const productPatterns = [
      /selling\s+([a-zA-Z\s]+?)(?:\.|,|$)/g,
      /offering\s+([a-zA-Z\s]+?)(?:\.|,|$)/g,
      /about\s+([a-zA-Z\s]+?)(?:\.|,|$)/g
    ];

    productPatterns.forEach(pattern => {
      let match;
      while ((match = pattern.exec(combinedText)) !== null) {
        if (match[1] && match[1].trim().length > 2) {
          analysis.products.push(match[1].trim());
        }
      }
    });

    return analysis;
  }

  // Adapt function template to specific business context
  adaptFunctionToContext(template, context) {
    const adaptedFunction = { ...template };
    
    // Customize function name and description based on context
    switch (context.industry) {
      case 'healthcare':
        if (template.name === 'checkInventory') {
          adaptedFunction.name = 'checkAvailability';
          adaptedFunction.description = 'Check appointment availability or service capacity';
        }
        break;
      
      case 'real_estate':
        if (template.name === 'checkInventory') {
          adaptedFunction.name = 'checkProperties';
          adaptedFunction.description = 'Check available properties matching criteria';
        }
        break;
      
      case 'automotive':
        if (template.name === 'checkInventory') {
          adaptedFunction.description = 'Check vehicle inventory and availability';
        }
        break;
    }

    // Create the function manifest
    const manifest = {
      type: 'function',
      function: {
        name: adaptedFunction.name,
        say: this.generateSayMessage(adaptedFunction.name, context),
        description: adaptedFunction.description,
        parameters: adaptedFunction.parameters,
        returns: this.generateReturnSchema(adaptedFunction.name)
      }
    };

    return {
      name: adaptedFunction.name,
      manifest,
      implementation: adaptedFunction.implementation(context)
    };
  }

  // Generate appropriate "say" messages based on context
  generateSayMessage(functionName, context) {
    const messages = {
      'checkInventory': 'Let me check what we have available for you.',
      'checkPrice': 'Let me get you the current pricing information.',
      'scheduleAppointment': 'Let me check our schedule and book that for you.',
      'placeOrder': 'Perfect! Let me process that order for you.',
      'lookupInformation': 'Let me look up those details for you.',
      'handleSupport': 'I\'ll help you resolve that issue right away.',
      'qualifyLead': 'Let me gather some information to better assist you.',
      'checkAvailability': 'Let me check our availability for you.',
      'checkProperties': 'Let me search our property listings.'
    };

    return messages[functionName] || 'One moment please, let me help you with that.';
  }

  // Generate return schema based on function type
  generateReturnSchema(functionName) {
    const schemas = {
      'checkInventory': {
        type: 'object',
        properties: {
          available: { type: 'boolean', description: 'Whether item is available' },
          quantity: { type: 'integer', description: 'Available quantity' },
          locations: { type: 'array', description: 'Available locations' }
        }
      },
      'checkPrice': {
        type: 'object',
        properties: {
          price: { type: 'number', description: 'Price of the item' },
          currency: { type: 'string', description: 'Currency code' },
          discounts: { type: 'array', description: 'Available discounts' }
        }
      },
      'placeOrder': {
        type: 'object',
        properties: {
          orderId: { type: 'string', description: 'Order confirmation ID' },
          total: { type: 'number', description: 'Total order amount' },
          deliveryDate: { type: 'string', description: 'Expected delivery date' }
        }
      }
    };

    return schemas[functionName] || {
      type: 'object',
      properties: {
        result: { type: 'string', description: 'Function execution result' },
        success: { type: 'boolean', description: 'Whether operation was successful' }
      }
    };
  }

  // Implementation generators for different function types
  createInventoryFunction(context) {
    return async function(functionArgs) {
      console.log(`GPT -> called ${this.name || 'checkInventory'} function`);
      
      const { item, variant, location } = functionArgs;
      
      // Dynamic inventory logic based on context
      const mockInventory = {
        'retail': () => Math.floor(Math.random() * 50) + 1,
        'automotive': () => Math.floor(Math.random() * 10) + 1,
        'real_estate': () => Math.floor(Math.random() * 5) + 1,
        'default': () => Math.floor(Math.random() * 100) + 1
      };

      const quantity = mockInventory[context.industry] ? 
        mockInventory[context.industry]() : 
        mockInventory.default();

      return JSON.stringify({
        available: quantity > 0,
        quantity: quantity,
        item: item,
        variant: variant || 'standard',
        location: location || 'main location'
      });
    };
  }

  createPricingFunction(context) {
    return async function(functionArgs) {
      console.log(`GPT -> called ${this.name || 'checkPrice'} function`);
      
      const { item, variant, quantity = 1 } = functionArgs;
      
      // Dynamic pricing based on context
      const basePrices = {
        'retail': () => Math.floor(Math.random() * 500) + 50,
        'automotive': () => Math.floor(Math.random() * 50000) + 15000,
        'real_estate': () => Math.floor(Math.random() * 500000) + 200000,
        'healthcare': () => Math.floor(Math.random() * 300) + 100,
        'default': () => Math.floor(Math.random() * 1000) + 100
      };

      const basePrice = basePrices[context.industry] ? 
        basePrices[context.industry]() : 
        basePrices.default();

      const totalPrice = basePrice * quantity;
      
      return JSON.stringify({
        price: totalPrice,
        basePrice: basePrice,
        quantity: quantity,
        currency: 'USD',
        item: item,
        variant: variant || 'standard'
      });
    };
  }

  createSchedulingFunction(context) {
    return async function(functionArgs) {
      console.log(`GPT -> called ${this.name || 'scheduleAppointment'} function`);
      
      const { service, date, time, duration = 30 } = functionArgs;
      
      // Generate confirmation ID
      const confirmationId = `${context.industry.toUpperCase()}-${Date.now().toString().slice(-6)}`;
      
      return JSON.stringify({
        confirmed: true,
        confirmationId: confirmationId,
        service: service,
        scheduledDate: date,
        scheduledTime: time,
        duration: duration,
        location: context.industry === 'healthcare' ? 'Main Clinic' : 'Main Office'
      });
    };
  }

  createOrderFunction(context) {
    return async function(functionArgs) {
      console.log(`GPT -> called ${this.name || 'placeOrder'} function`);
      
      const { items, customerInfo, paymentMethod = 'card' } = functionArgs;
      
      const orderId = `ORD-${Date.now().toString().slice(-8).toUpperCase()}`;
      const total = Math.floor(Math.random() * 1000) + 100; // Mock total
      
      return JSON.stringify({
        success: true,
        orderId: orderId,
        total: total,
        currency: 'USD',
        items: items,
        paymentMethod: paymentMethod,
        estimatedDelivery: context.industry === 'food_service' ? '30-45 minutes' : '3-5 business days'
      });
    };
  }

  createLookupFunction(context) {
    return async function(functionArgs) {
      console.log(`GPT -> called ${this.name || 'lookupInformation'} function`);
      
      const { topic, category, details } = functionArgs;
      
      // Context-specific information responses
      const responses = {
        'healthcare': `Based on your inquiry about ${topic}, here are the details: Our medical services include comprehensive care with qualified professionals. Please consult with our staff for specific medical advice.`,
        'automotive': `Regarding ${topic}, here's what you need to know: Our vehicles come with comprehensive warranties and financing options. All models include standard safety features and optional upgrades.`,
        'real_estate': `About ${topic}: Our properties feature modern amenities and are located in desirable neighborhoods. We offer various financing options and can arrange property tours.`,
        'default': `Here's the information about ${topic}: We provide comprehensive services with competitive pricing and excellent customer support. Contact us for detailed specifications.`
      };

      const response = responses[context.industry] || responses.default;
      
      return JSON.stringify({
        information: response,
        topic: topic,
        category: category || 'general',
        additionalResources: ['Contact our specialist', 'Schedule a consultation', 'View detailed brochure']
      });
    };
  }

  createSupportFunction(context) {
    return async function(functionArgs) {
      console.log(`GPT -> called ${this.name || 'handleSupport'} function`);
      
      const { issue, category = 'general', urgency = 'medium' } = functionArgs;
      
      const ticketId = `SUP-${Date.now().toString().slice(-6)}`;
      
      return JSON.stringify({
        ticketId: ticketId,
        issue: issue,
        category: category,
        urgency: urgency,
        status: 'acknowledged',
        nextSteps: urgency === 'high' ? 
          'Escalating to senior specialist immediately' : 
          'We will resolve this within 24 hours',
        estimatedResolution: urgency === 'high' ? '1 hour' : '24 hours'
      });
    };
  }

  createLeadQualificationFunction(context) {
    return async function(functionArgs) {
      console.log(`GPT -> called ${this.name || 'qualifyLead'} function`);
      
      const { budget, timeline, requirements = [] } = functionArgs;
      
      // Calculate qualification score based on inputs
      let score = 0;
      if (budget && !budget.toLowerCase().includes('low')) score += 30;
      if (timeline && timeline.toLowerCase().includes('soon')) score += 20;
      if (requirements.length > 0) score += 25;
      score += 25; // Base score for engagement
      
      const qualification = score >= 70 ? 'high' : score >= 40 ? 'medium' : 'low';
      
      return JSON.stringify({
        qualificationScore: score,
        qualification: qualification,
        budget: budget,
        timeline: timeline,
        requirements: requirements,
        recommendedNextStep: qualification === 'high' ? 
          'Schedule immediate consultation' : 
          'Provide detailed information package'
      });
    };
  }

  createTransferFunction() {
    return {
      type: 'function',
      function: {
        name: 'transferCall',
        say: 'One moment while I transfer your call to a specialist.',
        description: 'Transfer the customer to a live agent when needed.',
        parameters: {
          type: 'object',
          properties: {
            callSid: { type: 'string', description: 'The unique identifier for the active phone call.' },
            reason: { type: 'string', description: 'Reason for transfer' }
          },
          required: ['callSid']
        },
        returns: {
          type: 'object',
          properties: {
            status: { type: 'string', description: 'Whether or not the customer call was successfully transferred' }
          }
        }
      }
    };
  }

  getTransferImplementation() {
    const transferCall = require('./transferCall');
    return transferCall;
  }

  // Save generated functions to files
  saveGeneratedFunctions(functions, implementations, outputDir = './functions') {
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }

    // Save function manifest
    const manifestPath = path.join(outputDir, 'function-manifest.js');
    const manifestContent = `// Auto-generated function manifest
const tools = ${JSON.stringify(functions, null, 2)};

module.exports = tools;`;
    
    fs.writeFileSync(manifestPath, manifestContent);

    // Save individual function implementations
    Object.entries(implementations).forEach(([name, implementation]) => {
      const functionPath = path.join(outputDir, `${name}.js`);
      const functionContent = `// Auto-generated function: ${name}
${implementation.toString()}

module.exports = ${name};`;
      
      fs.writeFileSync(functionPath, functionContent);
    });

    console.log(`✅ Generated ${Object.keys(implementations).length} functions saved to ${outputDir}`.green);
  }

  // Main method to generate complete function system
  generateAdaptiveFunctionSystem(prompt, firstMessage, outputDir) {
    console.log('🚀 Generating adaptive function system...'.blue);
    
    const result = this.generateFunctionsFromPrompt(prompt, firstMessage);
    
    if (outputDir) {
      this.saveGeneratedFunctions(result.functions, result.implementations, outputDir);
    }
    
    console.log(`✅ Generated ${result.functions.length} adaptive functions for ${result.context.industry} industry`.green);
    
    return result;
  }

  // Get business analysis report
  getBusinessAnalysis() {
    return {
      detectedContext: this.businessContext,
      availableTemplates: Array.from(this.functionTemplates.keys()),
      generatedFunctions: Array.from(this.functionRegistry.keys())
    };
  }

  getSecureInputHints(context = null) {
    const resolvedContext = context || this.businessContext || {};
    const industry = resolvedContext.industry || 'general';
    const businessLabel =
      resolvedContext.businessDisplayName ||
      resolvedContext.companyName ||
      resolvedContext.brand ||
      'our team';

    const hints = {};

    if (industry === 'finance' || resolvedContext.businessType === 'banking') {
      hints.OTP = `It is the 6-digit security code sent to protect the ${businessLabel} account.`;
      hints.PIN = `Use the PIN you set when opening your ${businessLabel} profile.`;
      hints.CARD_LAST4 = `Only the last four digits of the ${businessLabel} card are required.`;
    } else if (industry === 'healthcare') {
      hints.OTP = `This code confirms access to your ${businessLabel} health portal.`;
      hints.PIN = `Use the clinic PIN associated with your ${businessLabel} file.`;
    } else if (industry === 'real_estate' || industry === 'automotive') {
      hints.OTP = `It verifies your ${businessLabel} inquiry; check the text we just sent.`;
      hints.PIN = `This is the application PIN tied to your ${businessLabel} request.`;
    } else {
      hints.OTP = `This keeps your ${businessLabel} experience secure; enter the code we texted.`;
      hints.PIN = `Use the short PIN you chose with ${businessLabel}.`;
    }

    return hints;
  }
}

module.exports = DynamicFunctionEngine;
