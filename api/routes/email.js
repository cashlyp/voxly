const crypto = require('crypto');
const axios = require('axios');
const config = require('../config');

let SignatureV4;
let HttpRequest;
let Sha256;
try {
  ({ SignatureV4 } = require('@aws-sdk/signature-v4'));
  ({ HttpRequest } = require('@aws-sdk/protocol-http'));
  ({ Sha256 } = require('@aws-sdk/hash-node'));
} catch (err) {
  SignatureV4 = null;
  HttpRequest = null;
  Sha256 = null;
}

function stableStringify(value) {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    const items = value.map((item) => (item === undefined ? 'null' : stableStringify(item)));
    return `[${items.join(',')}]`;
  }
  const keys = Object.keys(value).filter((key) => value[key] !== undefined).sort();
  const entries = keys.map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`);
  return `{${entries.join(',')}}`;
}

function normalizeEmail(value) {
  return String(value || '').trim().toLowerCase();
}

function isValidEmail(value) {
  const email = normalizeEmail(value);
  if (!email || !email.includes('@')) return false;
  const parts = email.split('@');
  if (parts.length !== 2) return false;
  if (!parts[0] || !parts[1]) return false;
  return true;
}

function getDomain(email) {
  const normalized = normalizeEmail(email);
  const parts = normalized.split('@');
  return parts.length === 2 ? parts[1] : '';
}

function redactEmailForLog(value = '') {
  const email = normalizeEmail(value);
  if (!email || !email.includes('@')) return '[redacted-email]';
  const [local, domain] = email.split('@');
  const localTail = local ? local.slice(-2) : '**';
  return `***${localTail}@${domain || 'redacted'}`;
}

function previewForLog(value = '', max = 120) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  if (!text) return '[empty]';
  return text.length > max ? `${text.slice(0, max - 3)}...` : text;
}

function getNestedValue(obj, path) {
  if (!obj || !path) return undefined;
  return path.split('.').reduce((acc, key) => {
    if (acc && Object.prototype.hasOwnProperty.call(acc, key)) {
      return acc[key];
    }
    return undefined;
  }, obj);
}

function extractTemplateVariables(text) {
  if (!text) return [];
  const matches = text.match(/{{\s*([\w.-]+)\s*}}/g) || [];
  const vars = new Set();
  matches.forEach((match) => {
    const cleaned = match.replace(/{{|}}/g, '').trim();
    if (cleaned) {
      vars.add(cleaned);
    }
  });
  return Array.from(vars);
}

function renderTemplateString(text, variables) {
  if (!text) return text;
  return text.replace(/{{\s*([\w.-]+)\s*}}/g, (_, key) => {
    const value = getNestedValue(variables, key);
    if (value === undefined || value === null) {
      return '';
    }
    return String(value);
  });
}

function hashPayload(payload) {
  return crypto.createHash('sha256').update(stableStringify(payload)).digest('hex');
}

function safeParseJson(value) {
  if (!value) return null;
  try {
    return JSON.parse(value);
  } catch (_) {
    return null;
  }
}

const SUPPORTED_EMAIL_PROVIDERS = ['sendgrid', 'mailgun', 'ses'];
const RETRYABLE_NETWORK_CODES = new Set([
  'ECONNRESET',
  'ETIMEDOUT',
  'ECONNREFUSED',
  'EAI_AGAIN',
  'ECONNABORTED',
]);

function normalizeProviderName(value) {
  return String(value || '').trim().toLowerCase();
}

function mapProviderError(provider, err) {
  const status = Number(err?.response?.status || err?.status || err?.statusCode);
  const code = String(err?.code || err?.name || '').toUpperCase();
  const retryable =
    status === 429 ||
    (status >= 500 && status < 600) ||
    RETRYABLE_NETWORK_CODES.has(code);
  const wrapped = new Error(
    err?.response?.data?.error_description ||
      err?.response?.data?.error ||
      err?.message ||
      'email_provider_error',
  );
  wrapped.code = err?.code === 'email_provider_timeout' ? 'email_provider_timeout' : 'email_provider_error';
  wrapped.statusCode = Number.isFinite(status) ? status : null;
  wrapped.status = wrapped.statusCode;
  wrapped.retryable = retryable;
  wrapped.provider = normalizeProviderName(provider) || 'unknown';
  wrapped.providerCode = code || null;
  wrapped.cause = err;
  return wrapped;
}

class ProviderAdapter {
  constructor(providerName = 'unknown') {
    this.providerName = providerName;
  }

  mapError(err) {
    return mapProviderError(this.providerName, err);
  }

  async sendEmail() {
    throw new Error('ProviderAdapter.sendEmail not implemented');
  }
}

class SendGridAdapter extends ProviderAdapter {
  constructor(options = {}) {
    super('sendgrid');
    this.apiKey = options.apiKey;
    this.baseUrl = options.baseUrl || 'https://api.sendgrid.com/v3';
    this.requestTimeoutMs = Number(options.requestTimeoutMs) || 15000;
  }

  async sendEmail(message) {
    if (!this.apiKey) {
      throw new Error('SendGrid API key is not configured');
    }
    const url = `${this.baseUrl}/mail/send`;
    const payload = {
      personalizations: [
        {
          to: [{ email: message.to }],
          ...(message.subject ? { subject: message.subject } : {}),
        },
      ],
      from: { email: message.from },
      subject: message.subject,
      content: [],
      headers: message.headers || {},
      custom_args: message.messageId ? { message_id: message.messageId } : undefined,
      ...(message.replyTo ? { reply_to: { email: message.replyTo } } : {})
    };
    if (message.text) {
      payload.content.push({ type: 'text/plain', value: message.text });
    }
    if (message.html) {
      payload.content.push({ type: 'text/html', value: message.html });
    }
    try {
      const response = await axios.post(url, payload, {
        timeout: this.requestTimeoutMs,
        headers: {
          Authorization: `Bearer ${this.apiKey}`,
          'Content-Type': 'application/json'
        }
      });
      const providerMessageId = response.headers?.['x-message-id'] || null;
      return { providerMessageId, response: response.data };
    } catch (err) {
      throw this.mapError(err);
    }
  }
}

class MailgunAdapter extends ProviderAdapter {
  constructor(options = {}) {
    super('mailgun');
    this.apiKey = options.apiKey;
    this.domain = options.domain;
    this.baseUrl = options.baseUrl || 'https://api.mailgun.net/v3';
    this.requestTimeoutMs = Number(options.requestTimeoutMs) || 15000;
  }

  async sendEmail(message) {
    if (!this.apiKey || !this.domain) {
      throw new Error('Mailgun API key or domain is not configured');
    }
    const url = `${this.baseUrl}/${this.domain}/messages`;
    const params = new URLSearchParams();
    params.append('from', message.from);
    params.append('to', message.to);
    if (message.subject) {
      params.append('subject', message.subject);
    }
    if (message.text) {
      params.append('text', message.text);
    }
    if (message.html) {
      params.append('html', message.html);
    }
    if (message.replyTo) {
      params.append('h:Reply-To', message.replyTo);
    }
    if (message.messageId) {
      params.append('v:message_id', message.messageId);
    }
    if (message.headers) {
      Object.entries(message.headers).forEach(([key, value]) => {
        params.append(`h:${key}`, value);
      });
    }
    try {
      const response = await axios.post(url, params, {
        timeout: this.requestTimeoutMs,
        auth: {
          username: 'api',
          password: this.apiKey
        },
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded'
        }
      });
      const providerMessageId = response.data?.id || null;
      return { providerMessageId, response: response.data };
    } catch (err) {
      throw this.mapError(err);
    }
  }
}

class SesAdapter extends ProviderAdapter {
  constructor(options = {}) {
    super('ses');
    this.region = options.region;
    this.accessKeyId = options.accessKeyId;
    this.secretAccessKey = options.secretAccessKey;
    this.sessionToken = options.sessionToken;
    this.requestTimeoutMs = Number(options.requestTimeoutMs) || 15000;
  }

  async sendEmail(message) {
    if (!SignatureV4 || !HttpRequest || !Sha256) {
      throw new Error('SES adapter requires AWS SDK signing helpers');
    }
    if (!this.region || !this.accessKeyId || !this.secretAccessKey) {
      throw new Error('SES credentials or region missing');
    }

    const host = `email.${this.region}.amazonaws.com`;
    const url = `https://${host}/v2/email/outbound-emails`;
    const body = {
      FromEmailAddress: message.from,
      Destination: {
        ToAddresses: [message.to]
      },
      Content: {
        Simple: {
          Subject: { Data: message.subject || '' },
          Body: {
            ...(message.text ? { Text: { Data: message.text } } : {}),
            ...(message.html ? { Html: { Data: message.html } } : {})
          }
        }
      },
      ...(message.headers ? { EmailTags: Object.entries(message.headers).map(([Name, Value]) => ({ Name, Value: String(Value) })) } : {})
    };
    if (message.messageId) {
      body.EmailTags = body.EmailTags || [];
      body.EmailTags.push({ Name: 'message_id', Value: message.messageId });
    }

    const request = new HttpRequest({
      protocol: 'https:',
      hostname: host,
      method: 'POST',
      path: '/v2/email/outbound-emails',
      headers: {
        'Content-Type': 'application/json',
        host
      },
      body: JSON.stringify(body)
    });

    const signer = new SignatureV4({
      credentials: {
        accessKeyId: this.accessKeyId,
        secretAccessKey: this.secretAccessKey,
        sessionToken: this.sessionToken
      },
      region: this.region,
      service: 'ses',
      sha256: Sha256
    });

    const signed = await signer.sign(request);
    try {
      const response = await axios.post(url, body, {
        timeout: this.requestTimeoutMs,
        headers: signed.headers,
      });
      const providerMessageId = response.data?.MessageId || null;
      return { providerMessageId, response: response.data };
    } catch (err) {
      throw this.mapError(err);
    }
  }
}

class EmailService {
  constructor({
    db,
    logger = console,
    config: cfg = config,
    getActiveProvider = null,
    providerResolver = null,
  }) {
    this.db = db;
    this.logger = logger;
    this.config = cfg?.email || {};
    this.getActiveProvider =
      typeof getActiveProvider === 'function'
        ? getActiveProvider
        : typeof providerResolver === 'function'
          ? providerResolver
          : () => this.config.provider || 'sendgrid';
    this.adapters = new Map();
    this.processing = false;
    this.rateBuckets = new Map();
    this.requestTimeoutMs = Number(this.config.requestTimeoutMs) || 15000;
    this.dlqAlertThreshold = Number(this.config.dlqAlertThreshold) || 25;
    this.maxSubjectChars = Number(this.config.maxSubjectChars) || 200;
    this.maxBodyChars = Number(this.config.maxBodyChars) || 200000;
    this.maxBulkRecipients = Number(this.config.maxBulkRecipients) || 500;
  }

  resolveProvider(providerOverride = null) {
    const resolved = normalizeProviderName(
      providerOverride || this.getActiveProvider?.() || this.config.provider || 'sendgrid',
    );
    if (!SUPPORTED_EMAIL_PROVIDERS.includes(resolved)) {
      const error = new Error(`Unsupported email provider: ${resolved || providerOverride}`);
      error.code = 'validation_error';
      throw error;
    }
    return resolved;
  }

  getProviderReadiness() {
    return {
      sendgrid: !!this.config?.sendgrid?.apiKey,
      mailgun: !!(this.config?.mailgun?.apiKey && this.config?.mailgun?.domain),
      ses: !!(
        this.config?.ses?.region &&
        this.config?.ses?.accessKeyId &&
        this.config?.ses?.secretAccessKey
      ),
    };
  }

  getAdapter(provider) {
    const resolved = this.resolveProvider(provider);
    if (this.adapters.has(resolved)) {
      return this.adapters.get(resolved);
    }
    let adapter = null;
    if (resolved === 'sendgrid') {
      adapter = new SendGridAdapter({
        ...(this.config.sendgrid || {}),
        requestTimeoutMs: this.requestTimeoutMs,
      });
    } else if (resolved === 'mailgun') {
      adapter = new MailgunAdapter({
        ...(this.config.mailgun || {}),
        requestTimeoutMs: this.requestTimeoutMs,
      });
    } else if (resolved === 'ses') {
      adapter = new SesAdapter({
        ...(this.config.ses || {}),
        requestTimeoutMs: this.requestTimeoutMs,
      });
    } else {
      throw new Error(`Unsupported email provider: ${resolved}`);
    }
    this.adapters.set(resolved, adapter);
    return adapter;
  }

  getVerifiedDomains() {
    return Array.isArray(this.config.verifiedDomains) ? this.config.verifiedDomains : [];
  }

  isVerifiedSender(fromEmail) {
    const domains = this.getVerifiedDomains();
    if (!domains.length) return true;
    const domain = getDomain(fromEmail);
    return domains.includes(domain);
  }

  getDefaultFrom() {
    return this.config.defaultFrom || '';
  }

  buildHeaders(payload) {
    const headers = { ...(payload.headers || {}) };
    if (payload.is_marketing && this.config.unsubscribeUrl) {
      let url = this.config.unsubscribeUrl;
      try {
        const built = new URL(this.config.unsubscribeUrl);
        if (payload.to_email || payload.to) {
          built.searchParams.set('email', payload.to_email || payload.to);
        }
        if (payload.message_id) {
          built.searchParams.set('message_id', payload.message_id);
        }
        url = built.toString();
      } catch {
        // keep provided URL as-is
      }
      headers['List-Unsubscribe'] = `<${url}>`;
      headers['List-Unsubscribe-Post'] = 'List-Unsubscribe=One-Click';
    }
    return headers;
  }

  validateVariables(script, variables) {
    const required = new Set();
    extractTemplateVariables(script.subject).forEach((v) => required.add(v));
    extractTemplateVariables(script.html).forEach((v) => required.add(v));
    extractTemplateVariables(script.text).forEach((v) => required.add(v));
    const missing = [];
    required.forEach((key) => {
      const value = getNestedValue(variables, key);
      if (value === undefined || value === null) {
        missing.push(key);
      }
    });
    return missing;
  }

  renderTemplate(script, variables) {
    return {
      subject: renderTemplateString(script.subject, variables),
      html: renderTemplateString(script.html, variables),
      text: renderTemplateString(script.text, variables)
    };
  }

  async resolveTemplate(payload) {
    if (payload.script_id) {
      const template = await this.db.getEmailTemplate(payload.script_id);
      if (!template) {
        throw new Error(`Script ${payload.script_id} not found`);
      }
      return {
        subject: template.subject || payload.subject || '',
        html: template.html || payload.html || '',
        text: template.text || payload.text || '',
        script_id: payload.script_id
      };
    }
    return {
      subject: payload.subject || '',
      html: payload.html || '',
      text: payload.text || '',
      script_id: null
    };
  }

  async enqueueEmail(payload, options = {}) {
    const idempotencyKey = options.idempotencyKey;
    const to = normalizeEmail(payload.to);
    const from = normalizeEmail(payload.from || this.getDefaultFrom());
    if (!isValidEmail(to)) {
      throw new Error('Invalid recipient email');
    }
    if (!isValidEmail(from)) {
      throw new Error('Invalid sender email');
    }
    if (!this.isVerifiedSender(from)) {
      throw new Error('Sender domain not verified');
    }

    const script = await this.resolveTemplate(payload);
    const variables = payload.variables || {};
    const missing = this.validateVariables(script, variables);
    if (missing.length) {
      const error = new Error(`Missing script variables: ${missing.join(', ')}`);
      error.code = 'missing_variables';
      error.missing = missing;
      throw error;
    }

    const rendered = this.renderTemplate(script, variables);
    if (rendered.subject && rendered.subject.length > this.maxSubjectChars) {
      const error = new Error(
        `Subject exceeds ${this.maxSubjectChars} characters`,
      );
      error.code = 'validation_error';
      throw error;
    }
    if (rendered.text && rendered.text.length > this.maxBodyChars) {
      const error = new Error(
        `Text body exceeds ${this.maxBodyChars} characters`,
      );
      error.code = 'validation_error';
      throw error;
    }
    if (rendered.html && rendered.html.length > this.maxBodyChars) {
      const error = new Error(
        `HTML body exceeds ${this.maxBodyChars} characters`,
      );
      error.code = 'validation_error';
      throw error;
    }

    let scheduledAt = null;
    if (payload.send_at) {
      const parsed = new Date(payload.send_at);
      if (Number.isNaN(parsed.getTime())) {
        const error = new Error('send_at must be a valid ISO timestamp');
        error.code = 'validation_error';
        throw error;
      }
      if (parsed.getTime() <= Date.now()) {
        const error = new Error('send_at must be in the future');
        error.code = 'validation_error';
        throw error;
      }
      scheduledAt = parsed.toISOString();
    }

    const requestHash = hashPayload({
      to,
      from,
      subject: rendered.subject,
      script_id: script.script_id,
      variables,
      html: rendered.html,
      text: rendered.text,
      send_at: scheduledAt
    });
    const resolvedProvider = this.resolveProvider(payload.provider);

    if (idempotencyKey) {
      const existing = await this.db.getEmailIdempotency(idempotencyKey);
      if (existing?.message_id) {
        if (existing.request_hash && existing.request_hash !== requestHash) {
          const error = new Error('Idempotency key reuse with different payload');
          error.code = 'idempotency_conflict';
          throw error;
        }
        return { message_id: existing.message_id, deduped: true };
      }
    }

    const suppressed = await this.db.isEmailSuppressed(to);
    const status = suppressed ? 'suppressed' : 'queued';
    const messageId = `email_${crypto.randomUUID()}`;
    const metadata = { ...(payload.metadata || {}), is_marketing: !!payload.is_marketing };

    await this.db.saveEmailMessage({
      message_id: messageId,
      to_email: to,
      from_email: from,
      subject: rendered.subject,
      html: rendered.html,
      text: rendered.text,
      template_id: script.script_id,
      variables_json: JSON.stringify(variables),
      variables_hash: hashPayload(variables),
      metadata_json: JSON.stringify(metadata),
      status,
      provider: resolvedProvider,
      tenant_id: payload.tenant_id || null,
      bulk_job_id: payload.bulk_job_id || null,
      scheduled_at: scheduledAt,
      max_retries: payload.max_retries || this.config.maxRetries || 5
    });

    if (idempotencyKey) {
      await this.db.saveEmailIdempotency(idempotencyKey, messageId, payload.bulk_job_id || null, requestHash);
    }

    if (suppressed) {
      await this.db.updateEmailMessageStatus(messageId, {
        status: 'suppressed',
        failure_reason: suppressed.reason || 'suppressed',
        suppressed_reason: suppressed.reason || 'suppressed',
        failed_at: new Date().toISOString()
      });
      await this.db.addEmailEvent(messageId, 'suppressed', { reason: suppressed.reason, source: suppressed.source });
      await this.db.incrementEmailMetric('suppressed');
      return { message_id: messageId, suppressed: true };
    }

    await this.db.addEmailEvent(messageId, 'queued', { scheduled_at: scheduledAt });
    await this.db.incrementEmailMetric('queued');
    return { message_id: messageId };
  }

  async enqueueBulk(payload, options = {}) {
    const idempotencyKey = options.idempotencyKey;
    const recipients = Array.isArray(payload.recipients) ? payload.recipients : [];
    if (!recipients.length) {
      throw new Error('Recipients list is required');
    }
    if (recipients.length > this.maxBulkRecipients) {
      const error = new Error(
        `Recipients exceed maximum of ${this.maxBulkRecipients}`,
      );
      error.code = 'validation_error';
      throw error;
    }

    const requestHash = hashPayload({
      recipients: recipients.map((r) => normalizeEmail(r.email)),
      script_id: payload.script_id,
      subject: payload.subject,
      variables: payload.variables || {},
      send_at: payload.send_at || null
    });

    if (idempotencyKey) {
      const existing = await this.db.getEmailIdempotency(idempotencyKey);
      if (existing?.bulk_job_id) {
        if (existing.request_hash && existing.request_hash !== requestHash) {
          const error = new Error('Idempotency key reuse with different payload');
          error.code = 'idempotency_conflict';
          throw error;
        }
        return { bulk_job_id: existing.bulk_job_id, deduped: true };
      }
    }

    const jobId = `bulk_${crypto.randomUUID()}`;
    await this.db.createEmailBulkJob({
      job_id: jobId,
      status: 'queued',
      total: recipients.length,
      queued: 0,
      tenant_id: payload.tenant_id || null,
      template_id: payload.script_id || null
    });

    if (idempotencyKey) {
      await this.db.saveEmailIdempotency(idempotencyKey, null, jobId, requestHash);
    }

    let queued = 0;
    let failed = 0;
    let suppressed = 0;
    for (const recipient of recipients) {
      const recipientKey = idempotencyKey
        ? `${idempotencyKey}:${crypto.createHash('sha1').update(normalizeEmail(recipient.email)).digest('hex')}`
        : null;
      try {
        const result = await this.enqueueEmail({
          ...payload,
          to: recipient.email,
          variables: { ...(payload.variables || {}), ...(recipient.variables || {}) },
          metadata: { ...(payload.metadata || {}), ...(recipient.metadata || {}) },
          bulk_job_id: jobId
        }, { idempotencyKey: recipientKey });
        if (result.deduped) {
          const existing = await this.db.getEmailMessage(result.message_id);
          if (existing?.status === 'suppressed') {
            suppressed += 1;
          } else if (existing?.status === 'failed') {
            failed += 1;
          } else {
            queued += 1;
          }
        } else if (result.suppressed) {
          suppressed += 1;
        } else {
          queued += 1;
        }
      } catch (err) {
        failed += 1;
        this.logger.warn('⚠️ Bulk email enqueue failed:', {
          email: redactEmailForLog(recipient.email),
          error: previewForLog(err.message)
        });
      }
    }

    const status = queued > 0 ? 'queued' : 'completed';
    const completedAt = queued > 0 ? null : new Date().toISOString();
    await this.db.updateEmailBulkJob(jobId, {
      queued,
      failed,
      suppressed,
      status,
      completed_at: completedAt
    });

    return { bulk_job_id: jobId };
  }

  async processQueue({ limit = 10 } = {}) {
    if (this.processing) return;
    this.processing = true;
    try {
      const messages = await this.db.getPendingEmailMessages(limit);
      for (const message of messages) {
        await this.processMessage(message);
      }
    } catch (err) {
      this.logger.error('Email queue processing error:', err.message);
    } finally {
      this.processing = false;
    }
  }

  checkRateLimit(key, limit) {
    if (!limit || limit <= 0) return { allowed: true };
    const now = Date.now();
    const windowMs = 60000;
    const bucket = this.rateBuckets.get(key) || [];
    const filtered = bucket.filter((ts) => now - ts < windowMs);
    if (filtered.length >= limit) {
      const earliest = filtered[0];
      const retryAfterMs = Math.max(0, windowMs - (now - earliest));
      this.rateBuckets.set(key, filtered);
      return { allowed: false, retryAfterMs };
    }
    filtered.push(now);
    this.rateBuckets.set(key, filtered);
    return { allowed: true };
  }

  async checkWarmupLimit() {
    const warmup = this.config.warmup || {};
    if (!warmup.enabled || !warmup.maxPerDay) return { allowed: true };
    const count = await this.db.getEmailMetricCount('sent');
    if (count >= warmup.maxPerDay) {
      const now = new Date();
      const nextDay = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1);
      return { allowed: false, retryAfterMs: nextDay - now };
    }
    return { allowed: true };
  }

  async processMessage(message) {
    const messageId = message.message_id;
    const provider = this.resolveProvider(message.provider);
    const now = new Date();
    const nowIso = now.toISOString();

    if (message.status === 'sending') {
      return;
    }

    const suppressed = await this.db.isEmailSuppressed(message.to_email);
    if (suppressed) {
      await this.db.updateEmailMessageStatus(messageId, {
        status: 'suppressed',
        failure_reason: suppressed.reason || 'suppressed',
        suppressed_reason: suppressed.reason || 'suppressed',
        failed_at: nowIso
      });
      await this.db.addEmailEvent(messageId, 'suppressed', { reason: suppressed.reason, source: suppressed.source });
      await this.db.incrementEmailMetric('suppressed');
      await this.updateBulkCounters(message.bulk_job_id, message.status, 'suppressed');
      return;
    }

    const warmup = await this.checkWarmupLimit();
    if (!warmup.allowed) {
      const retryAt = new Date(Date.now() + warmup.retryAfterMs).toISOString();
      await this.db.updateEmailMessageStatus(messageId, {
        status: 'queued',
        next_attempt_at: retryAt
      });
      await this.db.addEmailEvent(messageId, 'throttled', { reason: 'warmup', retry_at: retryAt });
      return;
    }

    const perProvider = this.config.rateLimits?.perProviderPerMinute;
    const perTenant = this.config.rateLimits?.perTenantPerMinute;
    const perDomain = this.config.rateLimits?.perDomainPerMinute;
    const tenantId = message.tenant_id || 'default';
    const domain = getDomain(message.to_email);

    const providerLimit = this.checkRateLimit(`provider:${provider}`, perProvider);
    const tenantLimit = this.checkRateLimit(`tenant:${tenantId}`, perTenant);
    const domainLimit = this.checkRateLimit(`domain:${domain}`, perDomain);

    const blocked = [providerLimit, tenantLimit, domainLimit].find((limit) => !limit.allowed);
    if (blocked) {
      const retryAt = new Date(Date.now() + blocked.retryAfterMs).toISOString();
      await this.db.updateEmailMessageStatus(messageId, {
        status: 'queued',
        next_attempt_at: retryAt
      });
      await this.db.addEmailEvent(messageId, 'throttled', { retry_at: retryAt });
      return;
    }

    await this.db.updateEmailMessageStatus(messageId, {
      status: 'sending',
      last_attempt_at: nowIso
    });
    await this.db.addEmailEvent(messageId, 'sending', { provider });

    const metadata = safeParseJson(message.metadata_json) || {};
    const headers = this.buildHeaders({
      ...message,
      to: message.to_email,
      message_id: message.message_id,
      is_marketing: metadata.is_marketing
    });

    try {
      const adapter = this.getAdapter(provider);
      const result = await adapter.sendEmail({
        to: message.to_email,
        from: message.from_email,
        subject: message.subject,
        html: message.html,
        text: message.text,
        headers,
        replyTo: null,
        messageId: message.message_id
      });
      await this.db.updateEmailMessageStatus(messageId, {
        status: 'sent',
        provider_message_id: result.providerMessageId,
        provider_response: result.response ? JSON.stringify(result.response) : null,
        sent_at: nowIso
      });
      await this.db.addEmailEvent(messageId, 'sent', { provider_message_id: result.providerMessageId }, provider);
      await this.db.incrementEmailMetric('sent');
      await this.updateBulkCounters(message.bulk_job_id, message.status, 'sent');
    } catch (err) {
      await this.handleSendFailure(message, err);
    }
  }

  classifyError(err) {
    const status = Number(err?.statusCode || err?.status || err?.response?.status);
    const code = String(err?.code || err?.providerCode || '').toUpperCase();
    const message = err?.response?.data?.error || err?.message || 'send_failed';
    if (err?.retryable === true) {
      return { permanent: false, reason: 'provider_error', statusCode: status || null };
    }
    if (err?.code === 'email_provider_timeout') {
      return { permanent: false, reason: 'provider_timeout', statusCode: status || null };
    }
    if (status === 429) return { permanent: false, reason: 'rate_limited', statusCode: status };
    if (status && status >= 500) return { permanent: false, reason: 'provider_error', statusCode: status };
    if (status && status >= 400) return { permanent: true, reason: 'invalid_request', statusCode: status };
    if (code && RETRYABLE_NETWORK_CODES.has(code)) {
      return { permanent: false, reason: 'network_error', statusCode: status };
    }
    return { permanent: true, reason: message, statusCode: status };
  }

  async handleSendFailure(message, err) {
    const messageId = message.message_id;
    const classification = this.classifyError(err);
    const retryCount = Number(message.retry_count || 0) + 1;
    const maxRetries = Number(message.max_retries || this.config.maxRetries || 5);

    if (!classification.permanent && retryCount <= maxRetries) {
      const baseDelay = 30000;
      const backoff = Math.min(3600000, baseDelay * Math.pow(2, retryCount - 1));
      const jitter = Math.floor(Math.random() * 5000);
      const nextAttempt = new Date(Date.now() + backoff + jitter).toISOString();
      await this.db.updateEmailMessageStatus(messageId, {
        status: 'retry',
        retry_count: retryCount,
        next_attempt_at: nextAttempt,
        failure_reason: classification.reason
      });
      await this.db.addEmailEvent(messageId, 'retry_scheduled', { retry_at: nextAttempt, reason: classification.reason });
      return;
    }

    await this.db.updateEmailMessageStatus(messageId, {
      status: 'failed',
      failure_reason: classification.reason,
      failed_at: new Date().toISOString()
    });
    await this.db.addEmailEvent(messageId, 'failed', { reason: classification.reason, status: classification.statusCode });
    await this.db.incrementEmailMetric('failed');
    await this.db.insertEmailDlq(messageId, classification.reason, {
      provider: message.provider,
      to: message.to_email
    });
    this.db
      ?.logServiceHealth?.('email_queue', 'message_dead_lettered', {
        message_id: messageId,
        reason: classification.reason,
        status_code: classification.statusCode || null,
        retry_count: retryCount,
        max_retries: maxRetries,
        at: new Date().toISOString(),
      })
      .catch(() => {});
    const openDlq = await this.db.countOpenEmailDlq().catch(() => null);
    if (openDlq !== null && openDlq >= this.dlqAlertThreshold) {
      this.db
        ?.logServiceHealth?.('email_queue', 'dlq_alert_threshold', {
          open_dlq: openDlq,
          alert_threshold: this.dlqAlertThreshold,
          at: new Date().toISOString(),
        })
        .catch(() => {});
    }
    await this.updateBulkCounters(message.bulk_job_id, message.status, 'failed');
  }

  async updateBulkCounters(jobId, previousStatus, nextStatus) {
    if (!jobId) return;
    const job = await this.db.getEmailBulkJob(jobId);
    if (!job) return;
    const updates = {};
    const decrement = (field) => {
      updates[field] = Math.max(0, Number(job[field] || 0) - 1);
    };
    const increment = (field) => {
      updates[field] = Number(job[field] || 0) + 1;
    };

    const statusMap = {
      queued: 'queued',
      retry: 'queued',
      sending: 'sending',
      sent: 'sent',
      failed: 'failed',
      delivered: 'delivered',
      bounced: 'bounced',
      complained: 'complained',
      suppressed: 'suppressed'
    };

    if (statusMap[previousStatus]) {
      decrement(statusMap[previousStatus]);
    }
    if (statusMap[nextStatus]) {
      increment(statusMap[nextStatus]);
    }
    const remaining = (updates.queued ?? job.queued) + (updates.sending ?? job.sending) + (updates.sent ?? job.sent);
    if (remaining <= 0) {
      updates.status = 'completed';
      updates.completed_at = new Date().toISOString();
    } else {
      updates.status = 'sending';
    }
    await this.db.updateEmailBulkJob(jobId, updates);
  }

  async handleProviderEvent(payload) {
    const events = this.normalizeProviderEvents(payload);
    for (const event of events) {
      let messageId = event.message_id;
      let message = null;
      if (messageId) {
        message = await this.db.getEmailMessage(messageId);
      }
      if (!message && event.provider_message_id) {
        message = await this.db.getEmailMessageByProviderId(event.provider_message_id);
        messageId = message?.message_id;
      }
      if (!messageId || !message) continue;

      const statusMap = {
        delivered: { status: 'delivered', metric: 'delivered' },
        bounced: { status: 'bounced', metric: 'bounced', suppress: 'bounce' },
        complained: { status: 'complained', metric: 'complained', suppress: 'complaint' },
        failed: { status: 'failed', metric: 'failed' }
      };
      const statusInfo = statusMap[event.type];
      if (!statusInfo) continue;

      await this.db.updateEmailMessageStatus(messageId, {
        status: statusInfo.status,
        failure_reason: event.reason || null,
        delivered_at: statusInfo.status === 'delivered' ? new Date().toISOString() : null,
        failed_at: statusInfo.status !== 'delivered' ? new Date().toISOString() : null
      });
      await this.db.addEmailEvent(messageId, event.type, { reason: event.reason, provider: event.provider }, event.provider);
      await this.db.incrementEmailMetric(statusInfo.metric);
      await this.updateBulkCounters(message.bulk_job_id, message.status, statusInfo.status);

      if (statusInfo.suppress) {
        await this.db.setEmailSuppression(message.to_email, statusInfo.suppress, event.provider);
      }
    }
    return { processed: events.length };
  }

  normalizeProviderEvents(payload) {
    const provider = String(payload.provider || '').toLowerCase();
    if (!provider && Array.isArray(payload)) {
      return this.normalizeProviderEvents({ provider: 'sendgrid', events: payload });
    }
    const events = [];
    if (provider === 'sendgrid' && Array.isArray(payload.events)) {
      payload.events.forEach((event) => {
        const eventType = String(event.event || '').toLowerCase();
        const typeMap = {
          delivered: 'delivered',
          bounce: 'bounced',
          dropped: 'failed',
          spamreport: 'complained',
          unsubscribe: 'complained'
        };
        const mapped = typeMap[eventType];
        if (!mapped) return;
        const customArgs = event.custom_args || event.unique_args || {};
        events.push({
          message_id: event.message_id || customArgs.message_id || null,
          provider_message_id: event.sg_message_id || event.message_id,
          type: mapped,
          provider: 'sendgrid',
          reason: event.reason || event.response
        });
      });
      return events;
    }

    if (provider === 'mailgun') {
      const eventData = payload['event-data'] || payload.eventData || payload;
      const eventType = String(eventData.event || payload.event || '').toLowerCase();
      const typeMap = {
        delivered: 'delivered',
        failed: 'failed',
        bounced: 'bounced',
        complained: 'complained',
        unsubscribed: 'complained'
      };
      const mapped = typeMap[eventType];
      if (mapped) {
        const userVars = eventData['user-variables'] || {};
        events.push({
          message_id: payload.message_id || userVars.message_id || null,
          provider_message_id: eventData.message?.headers?.['message-id'],
          type: mapped,
          provider: 'mailgun',
          reason: eventData.reason || eventData['delivery-status']?.message
        });
      }
      return events;
    }

    if (payload.message_id && payload.event_type) {
      events.push({
        message_id: payload.message_id,
        provider_message_id: payload.provider_message_id,
        type: payload.event_type,
        provider: payload.provider || 'custom',
        reason: payload.reason
      });
    }
    return events;
  }

  async previewScript(payload) {
    const script = await this.resolveTemplate(payload);
    const variables = payload.variables || {};
    const missing = this.validateVariables(script, variables);
    if (missing.length) {
      return { ok: false, missing };
    }
    const rendered = this.renderTemplate(script, variables);
    return { ok: true, subject: rendered.subject, html: rendered.html, text: rendered.text };
  }
}

module.exports = {
  EmailService
};
