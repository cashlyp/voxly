const TEMPLATE_METADATA = {
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
    description: 'Inform customers about order status'
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
    label: 'Customer Service',
    description: 'Acknowledge support inquiries'
  },
  survey: {
    label: 'Feedback Survey',
    description: 'Request post-interaction feedback'
  }
};

const CUSTOM_TEMPLATE_OPTION = {
  id: 'custom',
  label: '✍️ Custom message',
  description: 'Write your own SMS text'
};

function buildTemplateOption(templateId) {
  const meta = TEMPLATE_METADATA[templateId] || {};
  const label = meta.label || templateId.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
  return {
    id: templateId,
    label,
    description: meta.description || 'Predefined SMS template'
  };
}

function extractTemplateVariables(templateText = '') {
  const matches = templateText.match(/\{(\w+)\}/g) || [];
  return Array.from(new Set(matches.map((token) => token.replace(/[{}]/g, ''))));
}

module.exports = {
  TEMPLATE_METADATA,
  CUSTOM_TEMPLATE_OPTION,
  buildTemplateOption,
  extractTemplateVariables
};
