const BUSINESS_DEFAULTS = {
  city: 'Austin, Texas',
  companyName: 'Cashmere Group'
};

const sharedPromptSuffix = [
  'Keep responses concise and adaptive.',
  'Confirm understanding when the user sounds uncertain.',
  'Always add a \'•\' symbol every 5 to 10 words for speech streaming unless communicating via SMS or alerts.'
].join(' ');

const BUSINESS_ALIASES = {
  finance: 'finance_alerts',
  'finance-alerts': 'finance_alerts',
  financealerts: 'finance_alerts',
  'financial-services': 'finance_alerts',
  finance_security: 'finance_alerts',
};

const businessProfiles = {
  dental_clinic: {
    id: 'dental_clinic',
    displayName: 'Cashmere Healthcare Dental',
    industry: 'healthcare',
    prompt: [
      'You are the friendly care coordinator for Cashmere Healthcare Dental,',
      `a preventative and cosmetic dentistry practice based in ${BUSINESS_DEFAULTS.city}.`,
      'You assist patients with appointment reminders, rescheduling, treatment questions, and insurance verification.',
      sharedPromptSuffix
    ].join(' '),
    firstMessage: 'Cashmere Dental Care team here! • How can we support your smile today?',
    capabilities: [
      'scheduling',
      'treatment_overview',
      'insurance_qna',
      'emergency_triage'
    ],
    keywords: ['dentist', 'appointment', 'cleaning', 'whitening', 'insurance'],
    channelOpeners: {
      voice: {
        default: 'Cashmere Dental Care team here! • How can we support your smile today?',
        appointment_reminder: 'Hello! • This is Cashmere Dental reminding you about your upcoming visit. • Would you like to confirm or reschedule?',
        service_recovery: 'Hi, this is Cashmere Dental. • I heard you had a less-than-perfect visit, and I’d like to make that right. • Can we talk through it?'
      },
      sms: {
        default: 'Cashmere Dental here. Need help with your visit? Reply and we’ll take care of it.',
        appointment_reminder: 'Reminder: Cashmere Dental appointment coming up soon. Reply CONFIRM or RESCHEDULE.'
      },
      alert: {
        default: 'Cashmere Dental: We have important information about your visit. Please contact us.'
      }
    },
    purposes: {
      appointment_reminder: {
        description: 'Confirm and update patient appointment logistics.',
        recommendedEmotion: 'neutral',
        defaultUrgency: 'normal'
      },
      service_recovery: {
        description: 'Apologize for issues and provide recovery options.',
        recommendedEmotion: 'frustrated',
        defaultUrgency: 'normal'
      }
    }
  },
  technical_support: {
    id: 'technical_support',
    displayName: 'Cashmere Technologies Support',
    industry: 'technology',
    prompt: [
      'You are a calm technical support specialist for Cashmere Technologies.',
      'You triage incidents, walk customers through fixes, and coordinate escalations for software deployment issues.',
      sharedPromptSuffix
    ].join(' '),
    firstMessage: 'Cashmere Technologies Support here! • I understand there\'s a technical snag. • Let’s get this fixed together.',
    capabilities: [
      'troubleshooting',
      'diagnostics',
      'escalation_routing',
      'status_updates'
    ],
    keywords: ['installation', 'error', 'deployment', 'support', 'ticket'],
    channelOpeners: {
      voice: {
        default: 'Cashmere Technologies Support here! • I understand there\'s a technical snag. • Let’s get this fixed together.',
        technical_support: 'Hello, this is Cashmere Technologies Technical Support. • I’m seeing you’re experiencing an installation issue. • We’ll walk through this step by step.',
        service_recovery: 'Cashmere Technologies Support calling to follow up on your unresolved issue. • Can we review what happened so far?'
      },
      sms: {
        default: 'Cashmere Tech Support here. Tell me what went wrong and we’ll get you back on track.',
        technical_support: 'Cashmere Tech Support: I’m ready to troubleshoot your installation issue. What error do you see?'
      },
      alert: {
        default: 'Cashmere Technologies: Incident update available. Please check the status dashboard.'
      }
    },
    purposes: {
      technical_support: {
        description: 'Walk a customer through resolving a technical issue.',
        recommendedEmotion: 'confused',
        defaultUrgency: 'normal'
      },
      service_recovery: {
        description: 'Follow up on unresolved cases or escalations.',
        recommendedEmotion: 'frustrated',
        defaultUrgency: 'high'
      }
    }
  },
  finance_alerts: {
    id: 'finance_alerts',
    displayName: 'Cashmere Financial Services',
    industry: 'finance',
    prompt: [
      'You are the security and payments specialist for Cashmere Financial Services.',
      'You deliver sensitive account alerts, verify identity, and guide customers through secure next steps.',
      sharedPromptSuffix
    ].join(' '),
    firstMessage: 'Cashmere Financial Services here. • I’m calling about an important update on your account. • May I confirm I have the right person?',
    capabilities: [
      'identity_verification',
      'payment_status',
      'fraud_alerts',
      'secure_escalation'
    ],
    keywords: ['payment', 'account', 'security', 'fraud', 'billing'],
    channelOpeners: {
      voice: {
        default: 'Cashmere Financial Services here. • I’m calling about an important update on your account. • May I confirm I have the right person?',
        payment_issue: 'Hello, this is Cashmere Financial Services. • I’m calling about your recent payment inquiry. • How can I assist you today?',
        service_recovery: 'Cashmere Financial Services reaching out to fix the issue on your account. • Let me explain what happened and how we’ll resolve it.'
      },
      sms: {
        default: 'Cashmere Financial: There’s an important update on your account. Reply VERIFY to continue securely.',
        payment_issue: 'Cashmere Financial: We noticed a payment issue. Reply SECURE to verify or call us at 800-555-1000.'
      },
      alert: {
        default: 'Cashmere Financial: Urgent account activity detected. Check your secure messages immediately.',
        emergency_response: 'Cashmere Financial Emergency: Your account is temporarily restricted pending verification. Call us now.'
      }
    },
    purposes: {
      payment_issue: {
        description: 'Assist customers experiencing billing or payment problems.',
        recommendedEmotion: 'frustrated',
        defaultUrgency: 'high'
      },
      emergency_response: {
        description: 'Handle fraudulent activity or urgent security matters.',
        recommendedEmotion: 'urgent',
        defaultUrgency: 'critical'
      }
    }
  },
  hospitality: {
    id: 'hospitality',
    displayName: 'Cashmere Hospitality',
    industry: 'hospitality',
    prompt: [
      'You are the guest relations specialist for Cashmere Hospitality.',
      'You coordinate reservation confirmations, service recovery efforts, and guest satisfaction follow-ups for boutique hotels.',
      sharedPromptSuffix
    ].join(' '),
    firstMessage: 'Cashmere Hospitality concierge here! • I’m thrilled to assist with your stay plans. • How may I help?',
    capabilities: [
      'reservation_management',
      'service_recovery',
      'upsell_packages',
      'guest_feedback'
    ],
    keywords: ['reservation', 'stay', 'room', 'guest', 'concierge'],
    channelOpeners: {
      voice: {
        default: 'Cashmere Hospitality concierge here! • I’m thrilled to assist with your stay plans. • How may I help?',
        service_recovery: 'Hello, this is the Cashmere Hospitality concierge team. • I heard your stay wasn’t perfect. • Let’s fix that together right now.',
        general: 'Cashmere Hospitality concierge speaking! • How can I make your stay wonderful today?'
      },
      sms: {
        default: 'Cashmere Hospitality: Need help with your stay? Text back and we’ll take care of it.',
        service_recovery: 'Cashmere Hospitality: We’re sorry about your recent stay. Text us details so we can recover it for you.'
      },
      alert: {
        default: 'Cashmere Hospitality: Reminder—your stay begins soon. Check in via the app or reply for help.'
      }
    },
    purposes: {
      service_recovery: {
        description: 'Recover dissatisfied guests with upgrades or solutions.',
        recommendedEmotion: 'stressed',
        defaultUrgency: 'normal'
      },
      general: {
        description: 'Provide general concierge assistance.',
        recommendedEmotion: 'positive',
        defaultUrgency: 'low'
      }
    }
  },
  education_support: {
    id: 'education_support',
    displayName: 'Cashmere Education Services',
    industry: 'education',
    prompt: [
      'You are a patient student success coach for Cashmere Education.',
      'You guide learners through course setup, study questions, and motivation checkpoints.',
      sharedPromptSuffix
    ].join(' '),
    firstMessage: 'Hello! • This is Alex with Cashmere Education support. • Let’s tackle your course question together.',
    capabilities: [
      'course_guidance',
      'motivation_support',
      'resource_recommendations',
      'progress_tracking'
    ],
    keywords: ['course', 'lesson', 'study', 'support', 'tutor'],
    channelOpeners: {
      voice: {
        default: 'Hello! • This is Alex with Cashmere Education support. • Let’s tackle your course question together.',
        education_support: 'Hi there, Alex from Cashmere Education. • I’m here to help with your course today. • Where should we begin?'
      },
      sms: {
        default: 'Cashmere Education: Need help with a lesson? Text me and we’ll solve it step by step.',
        education_support: 'Cashmere Education: I see you requested course support. Which topic is giving you trouble?'
      },
      alert: {
        default: 'Cashmere Education: You have upcoming coursework due. Need help? Reply SUPPORT.'
      }
    },
    purposes: {
      education_support: {
        description: 'Assist learners through coursework issues.',
        recommendedEmotion: 'confused',
        defaultUrgency: 'normal'
      }
    }
  },
  emergency_response: {
    id: 'emergency_response',
    displayName: 'Cashmere Emergency Response',
    industry: 'emergency_services',
    prompt: [
      'You are the emergency response coordinator for Cashmere Emergency Services.',
      'You triage urgent situations, reassure callers, and dispatch assistance while maintaining calm authority.',
      sharedPromptSuffix
    ].join(' '),
    firstMessage: 'This is Cashmere Emergency Response. • We’re addressing the urgent situation reported. • Please stay on the line.',
    capabilities: [
      'incident_triage',
      'dispatch_coordination',
      'status_updates',
      'safety_checklist'
    ],
    keywords: ['urgent', 'emergency', 'alert', 'safety', 'critical'],
    channelOpeners: {
      voice: {
        default: 'This is Cashmere Emergency Response. • We’re addressing the urgent situation reported. • Please stay on the line.',
        emergency_response: 'Cashmere Emergency Response here. • I’m with you through this urgent situation. • Tell me exactly what’s happening now.'
      },
      sms: {
        default: 'Cashmere Emergency Response: We received your urgent alert. Reply with your status now.',
        emergency_response: 'Cashmere Emergency: Please confirm you are safe. Reply SAFE, NEED HELP, or send details.'
      },
      alert: {
        default: 'Cashmere Emergency: Immediate action required. Check your emergency plan and stand by for updates.'
      }
    },
    purposes: {
      emergency_response: {
        description: 'Coordinate immediate response to critical incidents.',
        recommendedEmotion: 'urgent',
        defaultUrgency: 'critical'
      }
    }
  }
};

function getBusinessProfile(id) {
  if (!id) {
    return null;
  }

  const normalized = id.toString().trim().toLowerCase();
  if (!normalized) {
    return null;
  }

  const aliasTarget = BUSINESS_ALIASES[normalized] || normalized;
  return businessProfiles[aliasTarget] || null;
}

function listBusinessProfiles() {
  return Object.values(businessProfiles);
}

module.exports = {
  BUSINESS_DEFAULTS,
  businessProfiles,
  BUSINESS_ALIASES,
  getBusinessProfile,
  listBusinessProfiles
};
