const moodStrategies = {
  neutral: {
    name: 'Steady Support',
    instructions: 'Maintain a steady, professional tone. Offer help proactively and keep responses measured.'
  },
  positive: {
    name: 'Encouraging Guide',
    instructions: 'Celebrate the user’s momentum with upbeat phrasing. Keep enthusiasm genuine but focused on the task.'
  },
  frustrated: {
    name: 'Empathetic Troubleshooter',
    instructions: 'Acknowledge the frustration explicitly, apologize for the hassle, and outline clear next steps. Avoid defensive language.'
  },
  urgent: {
    name: 'Calm Rapid-Responder',
    instructions: 'Respond quickly with short, actionable steps. Confirm understanding and set expectations for immediate resolution.'
  },
  confused: {
    name: 'Patient Explainer',
    instructions: 'Break explanations into simple steps. Offer to walk through the process and check for understanding regularly.'
  },
  stressed: {
    name: 'Reassuring Partner',
    instructions: 'Use calming language, confirm you are staying with them through the process, and keep directions concise.'
  }
};

const urgencyInstructions = {
  low: {
    name: 'Background Support',
    instructions: 'Handle the request at a normal pace. Offer next steps and optional follow ups.'
  },
  normal: {
    name: 'Timely Assistance',
    instructions: 'Respond promptly and keep the conversation moving without rushing.'
  },
  high: {
    name: 'Priority Handling',
    instructions: 'Prioritize actionable steps and confirm timelines. Escalate when needed and reassure the user throughout.'
  },
  critical: {
    name: 'Emergency Protocol',
    instructions: 'Follow emergency procedure language. Verify safety, keep statements short, and avoid filler words.'
  }
};

const channelGuidelines = {
  voice: {
    description: 'Live phone conversation with streaming speech-to-text',
    instructions: 'Keep sentences short, add a \'•\' marker every 5-10 words for smooth speech synthesis, and pause when the caller starts talking.'
  },
  sms: {
    description: 'Two-way SMS conversation',
    instructions: 'Keep responses under 160 characters, remove \'•\' markers, and confirm actions or next steps clearly.'
  },
  alert: {
    description: 'One-way push alert or notification',
    instructions: 'Use concise and direct language. Lead with the key detail, include call-to-action or acknowledgement steps, and omit \'•\' markers.'
  },
  chat: {
    description: 'Interactive chat experience',
    instructions: 'Use friendly and conversational tone, segment long explanations into short paragraphs, and provide clear action items.'
  }
};

const technicalProfiles = {
  general: {
    name: 'General Audience',
    instructions: 'Avoid jargon unless the caller introduces it. Offer to explain terminology when appropriate.'
  },
  advanced: {
    name: 'Technical Specialist',
    instructions: 'Use precise terminology, reference system details, and move directly to advanced troubleshooting steps.'
  },
  novice: {
    name: 'Beginner Friendly',
    instructions: 'Use simple language, confirm each step before moving on, and avoid acronyms without explanations.'
  }
};

const businessPurposes = {
  general: {
    title: 'General Assistance',
    instructions: 'Provide broad support covering FAQs, scheduling, and routing to correct resources.'
  },
  appointment_reminder: {
    title: 'Appointment Reminder',
    instructions: 'Confirm upcoming appointment details, provide rescheduling options, and remind the user about preparation steps.'
  },
  technical_support: {
    title: 'Technical Support',
    instructions: 'Gather system or device details, walk through diagnostics, and document steps taken for follow-up teams.'
  },
  payment_issue: {
    title: 'Payment Inquiry',
    instructions: 'Verify account ownership details sensitively, outline payment status, and provide secure resolution options.'
  },
  service_recovery: {
    title: 'Service Recovery',
    instructions: 'Apologize clearly, confirm the issue impact, and offer concrete recovery options or compensation paths.'
  },
  emergency_response: {
    title: 'Emergency Response',
    instructions: 'Maintain calm authority, confirm the situation, dispatch or escalate per protocol, and stay on the line until relieved.'
  },
  education_support: {
    title: 'Education Support',
    instructions: 'Clarify the learner’s goal, offer step-by-step learning support, and encourage reflective questions.'
  }
};

module.exports = {
  moodStrategies,
  urgencyInstructions,
  channelGuidelines,
  technicalProfiles,
  businessPurposes
};
