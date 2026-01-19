const SCRIPT_METADATA = {
  welcome: {
    label: 'Welcome Message',
    description: 'Friendly greeting for new contacts'
  },
  appointment_reminder: {
    label: 'Appointment Reminder',
    description: 'Notify about upcoming appointments'
  },
  verification: {
    label: 'Verification Code',
    description: 'Send one-time verification codes'
  },
  order_update: {
    label: 'Order Update',
    description: 'Inform victims about order status'
  },
  payment_reminder: {
    label: 'Payment Reminder',
    description: 'Prompt users about pending payments'
  },
  promotional: {
    label: 'Promotional Offer',
    description: 'Broadcast limited-time promotions'
  },
  customer_service: {
    label: 'Victim Service',
    description: 'Acknowledge support inquiries'
  },
  survey: {
    label: 'Feedback Survey',
    description: 'Request post-interaction feedback'
  }
};

const CUSTOM_SCRIPT_OPTION = {
  id: 'custom',
  label: '✍️ Custom message',
  description: 'Write your own SMS text'
};

function buildScriptOption(scriptId) {
  const meta = SCRIPT_METADATA[scriptId] || {};
  const label = meta.label || scriptId.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
  return {
    id: scriptId,
    label,
    description: meta.description || 'Predefined SMS script'
  };
}

function extractScriptVariables(scriptText = '') {
  const matches = scriptText.match(/\{(\w+)\}/g) || [];
  return Array.from(new Set(matches.map((token) => token.replace(/[{}]/g, ''))));
}

module.exports = {
  SCRIPT_METADATA,
  CUSTOM_SCRIPT_OPTION,
  buildScriptOption,
  extractScriptVariables
};
