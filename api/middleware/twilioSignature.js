const twilio = require('twilio');
const config = require('../config');

function resolveHostFallback(req) {
  return config.server?.hostname
    || req?.headers?.['x-forwarded-host']
    || req?.headers?.host
    || '';
}

function getTwilioWebhookUrl(req, resolveHost = resolveHostFallback) {
  const host = resolveHost(req);
  if (!host) {
    return null;
  }
  return `https://${host}${req.originalUrl}`;
}

function validateTwilioRequest(req, options = {}) {
  const signature = req.headers['x-twilio-signature'];
  const authToken = config.twilio.authToken;
  const url = getTwilioWebhookUrl(req, options.resolveHost || resolveHostFallback);
  if (!signature || !authToken || !url) {
    return false;
  }
  const params = String(req.method || '').toUpperCase() === 'GET' ? (req.query || {}) : (req.body || {});
  return twilio.validateRequest(authToken, signature, url, params);
}

function warnOnInvalidTwilioSignature(req, label = '', options = {}) {
  const valid = validateTwilioRequest(req, options);
  if (!valid) {
    const path = label || req.originalUrl || req.path || 'unknown';
    console.warn(`⚠️ Twilio signature invalid for ${path}`);
  }
  return valid;
}

function requireValidTwilioSignature(req, res, label = '', options = {}) {
  const mode = String(config.twilio?.webhookValidation || 'warn').toLowerCase();
  if (mode === 'off') return true;
  const valid = validateTwilioRequest(req, options);
  if (valid) return true;
  const path = label || req.originalUrl || req.path || 'unknown';
  console.warn(`⚠️ Twilio signature invalid for ${path}`);
  if (mode === 'strict') {
    res.status(403).send('Forbidden');
    return false;
  }
  return true;
}

module.exports = {
  validateTwilioRequest,
  warnOnInvalidTwilioSignature,
  requireValidTwilioSignature
};
