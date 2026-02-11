const EventEmitter = require('events');
const crypto = require('crypto');
const config = require('../config');
const { PinpointClient, SendMessagesCommand } = require('@aws-sdk/client-pinpoint');
const { Vonage } = require('@vonage/server-sdk');

const SUPPORTED_SMS_PROVIDERS = ['twilio', 'aws', 'vonage'];
const RETRYABLE_NETWORK_CODES = new Set([
    'ECONNRESET',
    'ECONNABORTED',
    'ETIMEDOUT',
    'ECONNREFUSED',
    'EAI_AGAIN',
    'ENOTFOUND',
]);

const GSM7_BASIC_CHARS = new Set([
    '@', '£', '$', '¥', 'è', 'é', 'ù', 'ì', 'ò', 'Ç', '\n', 'Ø', 'ø', '\r', 'Å', 'å',
    'Δ', '_', 'Φ', 'Γ', 'Λ', 'Ω', 'Π', 'Ψ', 'Σ', 'Θ', 'Ξ', 'Æ', 'æ', 'ß', 'É', ' ',
    '!', '"', '#', '¤', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/',
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', ':', ';', '<', '=', '>', '?',
    '¡', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
    'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'Ä', 'Ö', 'Ñ', 'Ü', '§',
    '¿', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
    'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'ä', 'ö', 'ñ', 'ü', 'à'
]);
const GSM7_EXT_CHARS = new Set(['^', '{', '}', '\\', '[', '~', ']', '|', '€']);

function getSmsSegmentInfo(text) {
    const value = String(text || '');
    if (!value) {
        return { encoding: 'gsm-7', length: 0, units: 0, per_segment: 160, segments: 0 };
    }

    let units = 0;
    let isGsm7 = true;
    for (const ch of value) {
        if (GSM7_BASIC_CHARS.has(ch)) {
            units += 1;
            continue;
        }
        if (GSM7_EXT_CHARS.has(ch)) {
            units += 2;
            continue;
        }
        isGsm7 = false;
        break;
    }

    if (!isGsm7) {
        const length = value.length;
        const perSegment = length <= 70 ? 70 : 67;
        const segments = Math.ceil(length / perSegment);
        return { encoding: 'ucs-2', length, units: length, per_segment: perSegment, segments };
    }

    const perSegment = units <= 160 ? 160 : 153;
    const segments = Math.ceil(units / perSegment);
    return { encoding: 'gsm-7', length: value.length, units, per_segment: perSegment, segments };
}

function isValidE164(number) {
    return /^\+[1-9]\d{1,14}$/.test(String(number || '').trim());
}

function redactPhoneForLog(value = '') {
    const digits = String(value || '').replace(/\D/g, '');
    if (!digits) return 'unknown';
    return `***${digits.slice(-4)}`;
}

function redactBodyForLog(value = '') {
    const text = String(value || '').replace(/\s+/g, ' ').trim();
    if (!text) return '[empty len=0]';
    const digest = crypto.createHash('sha256').update(text).digest('hex').slice(0, 12);
    return `[len=${text.length} sha=${digest}]`;
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeProviderName(value) {
    return String(value || '')
        .trim()
        .toLowerCase();
}

function sanitizeErrorMessage(value) {
    const text = String(value || '').trim();
    if (!text) return 'provider_error';
    return text.length > 180 ? `${text.slice(0, 177)}...` : text;
}

function createSmsProviderError(provider, options = {}) {
    const {
        message = 'SMS provider request failed',
        code = 'sms_provider_error',
        status = null,
        statusCode = null,
        retryable = false,
        providerCode = null,
        cause = null,
    } = options;
    const err = new Error(sanitizeErrorMessage(message));
    err.code = code;
    err.status = Number.isFinite(Number(status)) ? Number(status) : null;
    err.statusCode = Number.isFinite(Number(statusCode))
        ? Number(statusCode)
        : err.status;
    err.retryable = retryable === true;
    err.provider = normalizeProviderName(provider) || 'unknown';
    if (providerCode !== null && providerCode !== undefined && providerCode !== '') {
        err.providerCode = providerCode;
    }
    if (cause) {
        err.cause = cause;
    }
    return err;
}

class TwilioSmsAdapter {
    constructor(options = {}) {
        this.provider = 'twilio';
        this.client = options.client;
        this.defaultFrom = options.defaultFrom || null;
    }

    resolveFromNumber(from) {
        return String(from || this.defaultFrom || '').trim();
    }

    isConfigured() {
        return !!(this.client && this.defaultFrom);
    }

    mapError(error) {
        if (error?.code === 'sms_provider_timeout') {
            return error;
        }
        const status = Number(error?.status || error?.statusCode || error?.response?.status);
        const code = String(error?.code || '').toUpperCase();
        const twilioCode = Number(error?.code);
        const retryable =
            status === 429 ||
            (status >= 500 && status < 600) ||
            RETRYABLE_NETWORK_CODES.has(code) ||
            twilioCode === 20429 ||
            twilioCode === 30008;
        return createSmsProviderError(this.provider, {
            message: error?.message || 'Twilio SMS send failed',
            code: 'sms_provider_error',
            status,
            retryable,
            providerCode: Number.isFinite(twilioCode) ? twilioCode : null,
            cause: error,
        });
    }

    async sendSms(payload, options = {}) {
        const { withTimeout, providerTimeoutMs } = options;
        if (!this.client) {
            throw createSmsProviderError(this.provider, {
                message: 'Twilio client is not configured',
                code: 'sms_config_error',
                retryable: false,
            });
        }
        const requestPayload = {
            body: payload.body,
            to: payload.to,
            from: payload.from,
        };
        if (payload.statusCallback) {
            requestPayload.statusCallback = payload.statusCallback;
        }
        if (payload.mediaUrl) {
            requestPayload.mediaUrl = payload.mediaUrl;
        }
        try {
            const result = await withTimeout(
                this.client.messages.create(requestPayload),
                providerTimeoutMs,
                'sms_provider_timeout',
            );
            return {
                provider: this.provider,
                messageSid: result?.sid || null,
                status: result?.status || 'queued',
                from: requestPayload.from,
                response: result || null,
            };
        } catch (error) {
            throw this.mapError(error);
        }
    }
}

class AwsPinpointSmsAdapter {
    constructor(options = {}) {
        this.provider = 'aws';
        this.applicationId = options.applicationId || null;
        this.defaultFrom = options.defaultFrom || null;
        this.region = options.region || null;
        this.client = this.region ? new PinpointClient({ region: this.region }) : null;
    }

    resolveFromNumber(from) {
        return String(from || this.defaultFrom || '').trim();
    }

    isConfigured() {
        return !!(this.client && this.applicationId && this.defaultFrom);
    }

    mapError(error) {
        if (error?.code === 'sms_provider_timeout') {
            return error;
        }
        const status = Number(error?.status || error?.statusCode || error?.response?.status);
        const sdkCode = String(error?.name || error?.Code || error?.code || '').toUpperCase();
        const retryable =
            status === 429 ||
            (status >= 500 && status < 600) ||
            RETRYABLE_NETWORK_CODES.has(sdkCode) ||
            sdkCode.includes('THROTT') ||
            sdkCode.includes('TOOMANYREQUESTS');
        return createSmsProviderError(this.provider, {
            message: error?.message || 'AWS Pinpoint SMS send failed',
            code: 'sms_provider_error',
            status,
            retryable,
            providerCode: sdkCode || null,
            cause: error,
        });
    }

    async sendSms(payload, options = {}) {
        const { withTimeout, providerTimeoutMs } = options;
        if (!this.client || !this.applicationId) {
            throw createSmsProviderError(this.provider, {
                message: 'AWS Pinpoint is not configured',
                code: 'sms_config_error',
                retryable: false,
            });
        }
        if (payload.mediaUrl) {
            throw createSmsProviderError(this.provider, {
                message: 'Media messages are not supported by AWS Pinpoint adapter',
                code: 'sms_validation_failed',
                retryable: false,
            });
        }
        const command = new SendMessagesCommand({
            ApplicationId: this.applicationId,
            MessageRequest: {
                Addresses: {
                    [payload.to]: {
                        ChannelType: 'SMS',
                    },
                },
                MessageConfiguration: {
                    SMSMessage: {
                        Body: payload.body,
                        OriginationNumber: payload.from,
                        MessageType: 'TRANSACTIONAL',
                    },
                },
                TraceId: payload.idempotencyKey || undefined,
            },
        });
        try {
            const response = await withTimeout(
                this.client.send(command),
                providerTimeoutMs,
                'sms_provider_timeout',
            );
            const result = response?.MessageResponse?.Result?.[payload.to] || {};
            const statusCode = Number(result?.StatusCode);
            if (Number.isFinite(statusCode) && statusCode >= 400) {
                const retryable = statusCode === 429 || statusCode >= 500;
                throw createSmsProviderError(this.provider, {
                    message: result?.StatusMessage || 'AWS Pinpoint rejected SMS',
                    code: 'sms_provider_error',
                    status: statusCode,
                    retryable,
                    providerCode: result?.DeliveryStatus || null,
                });
            }
            return {
                provider: this.provider,
                messageSid: result?.MessageId || null,
                status: 'accepted',
                from: payload.from,
                response: response || null,
            };
        } catch (error) {
            if (error?.provider === this.provider) {
                throw error;
            }
            throw this.mapError(error);
        }
    }
}

class VonageSmsAdapter {
    constructor(options = {}) {
        this.provider = 'vonage';
        this.defaultFrom = options.defaultFrom || null;
        this.apiKey = options.apiKey || null;
        this.apiSecret = options.apiSecret || null;
        this.client =
            this.apiKey && this.apiSecret
                ? new Vonage({
                    apiKey: this.apiKey,
                    apiSecret: this.apiSecret,
                })
                : null;
    }

    resolveFromNumber(from) {
        return String(from || this.defaultFrom || '').trim();
    }

    isConfigured() {
        return !!(this.client && this.defaultFrom);
    }

    mapError(error) {
        if (error?.code === 'sms_provider_timeout') {
            return error;
        }
        const status = Number(error?.status || error?.statusCode || error?.response?.status);
        const providerCode = String(error?.providerCode || error?.code || '').toUpperCase();
        const retryable =
            status === 429 ||
            (status >= 500 && status < 600) ||
            RETRYABLE_NETWORK_CODES.has(providerCode) ||
            ['1', '5', '6', 'THROTTLED', 'TOOMANYREQUESTS'].includes(providerCode);
        return createSmsProviderError(this.provider, {
            message: error?.message || 'Vonage SMS send failed',
            code: 'sms_provider_error',
            status,
            retryable,
            providerCode: providerCode || null,
            cause: error,
        });
    }

    async sendSms(payload, options = {}) {
        const { withTimeout, providerTimeoutMs } = options;
        if (!this.client) {
            throw createSmsProviderError(this.provider, {
                message: 'Vonage SMS adapter is not configured',
                code: 'sms_config_error',
                retryable: false,
            });
        }
        if (payload.mediaUrl) {
            throw createSmsProviderError(this.provider, {
                message: 'Media messages are not supported by Vonage SMS adapter',
                code: 'sms_validation_failed',
                retryable: false,
            });
        }
        try {
            const response = await withTimeout(
                this.client.sms.send({
                    to: payload.to,
                    from: payload.from,
                    text: payload.body,
                }),
                providerTimeoutMs,
                'sms_provider_timeout',
            );
            const message = Array.isArray(response?.messages) ? response.messages[0] : null;
            const vonageStatus = Number(message?.status);
            if (Number.isFinite(vonageStatus) && vonageStatus !== 0) {
                const retryable = vonageStatus === 1;
                throw createSmsProviderError(this.provider, {
                    message: message?.['error-text'] || 'Vonage SMS rejected',
                    code: 'sms_provider_error',
                    status: retryable ? 429 : 400,
                    retryable,
                    providerCode: String(vonageStatus),
                });
            }
            return {
                provider: this.provider,
                messageSid: message?.['message-id'] || null,
                status: 'accepted',
                from: payload.from,
                response: response || null,
            };
        } catch (error) {
            if (error?.provider === this.provider) {
                throw error;
            }
            throw this.mapError(error);
        }
    }
}

class EnhancedSmsService extends EventEmitter {
    constructor(options = {}) {
        super();
        this.db = options.db || null;
        this.logger = options.logger || console;
        this.getActiveProvider =
            typeof options.getActiveProvider === 'function'
                ? options.getActiveProvider
                : () => config.sms?.provider || 'twilio';
        this.twilio = require('twilio')(
            config.twilio.accountSid,
            config.twilio.authToken
        );
        this.smsAdapters = new Map();
        this.openai = new(require('openai'))({
            baseURL: "https://openrouter.ai/api/v1",
            apiKey: config.openRouter.apiKey,
            defaultHeaders: {
                "HTTP-Referer": config.openRouter.siteUrl,
                "X-Title": config.openRouter.siteName || "SMS AI Assistant",
            }
        });
        this.model = config.openRouter.model || "meta-llama/llama-3.1-8b-instruct:free";

        // SMS conversation tracking
        this.activeConversations = new Map();
        this.messageQueue = new Map(); // Queue for outbound messages
        this.optOutCache = new Map();
        this.idempotencyCache = new Map();
        this.lastSendAt = new Map();
        this.scheduledProcessing = false;
        this.defaultQuietHours = { start: 9, end: 20 };
        this.defaultMaxRetries = 2;
        this.defaultRetryDelayMs = 2000;
        this.defaultMinIntervalMs = 2000;
        this.defaultProviderTimeoutMs = Number(config.sms?.providerTimeoutMs) || 15000;
        this.defaultAiTimeoutMs = Number(config.sms?.aiTimeoutMs) || 12000;
        this.maxMessageChars = Number(config.sms?.maxMessageChars) || 1600;
    }

    setDb(db) {
        this.db = db;
    }

    normalizePhone(phone) {
        return String(phone || '').trim();
    }

    normalizeBody(body) {
        return String(body || '').trim();
    }

    hashBody(body) {
        const text = this.normalizeBody(body);
        if (!text) return '';
        return crypto.createHash('sha256').update(text).digest('hex');
    }

    async withTimeout(promise, timeoutMs, timeoutCode = 'sms_timeout') {
        const safeTimeout = Number(timeoutMs);
        if (!Number.isFinite(safeTimeout) || safeTimeout <= 0) {
            return promise;
        }
        let settled = false;
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                if (settled) return;
                settled = true;
                const timeoutError = new Error(`Operation timed out after ${safeTimeout}ms`);
                timeoutError.code = timeoutCode;
                reject(timeoutError);
            }, safeTimeout);

            Promise.resolve(promise)
                .then((result) => {
                    if (settled) return;
                    settled = true;
                    clearTimeout(timer);
                    resolve(result);
                })
                .catch((error) => {
                    if (settled) return;
                    settled = true;
                    clearTimeout(timer);
                    reject(error);
                });
        });
    }

    computeRetryDelayMs(baseDelayMs, attempt) {
        const base = Math.max(250, Number(baseDelayMs) || this.defaultRetryDelayMs);
        const exp = Math.min(30000, base * Math.pow(2, Math.max(0, Number(attempt) - 1)));
        const jitter = Math.floor(Math.random() * 750);
        return exp + jitter;
    }

    resolveProvider(providerOverride = null) {
        const provider = normalizeProviderName(providerOverride || this.getActiveProvider?.());
        if (!SUPPORTED_SMS_PROVIDERS.includes(provider)) {
            throw createSmsProviderError(provider || 'unknown', {
                message: `Unsupported SMS provider "${provider || providerOverride}"`,
                code: 'sms_validation_failed',
                retryable: false,
            });
        }
        return provider;
    }

    getProviderReadiness() {
        return {
            twilio: !!(
                config.twilio?.accountSid &&
                config.twilio?.authToken &&
                config.twilio?.fromNumber
            ),
            aws: !!(
                config.aws?.pinpoint?.applicationId &&
                config.aws?.pinpoint?.originationNumber &&
                config.aws?.pinpoint?.region
            ),
            vonage: !!(
                config.vonage?.apiKey &&
                config.vonage?.apiSecret &&
                config.vonage?.sms?.fromNumber
            ),
        };
    }

    getAdapter(provider) {
        const resolved = this.resolveProvider(provider);
        if (this.smsAdapters.has(resolved)) {
            return this.smsAdapters.get(resolved);
        }
        let adapter = null;
        if (resolved === 'twilio') {
            adapter = new TwilioSmsAdapter({
                client: this.twilio,
                defaultFrom: config.twilio?.fromNumber,
            });
        } else if (resolved === 'aws') {
            adapter = new AwsPinpointSmsAdapter({
                applicationId: config.aws?.pinpoint?.applicationId,
                defaultFrom: config.aws?.pinpoint?.originationNumber,
                region: config.aws?.pinpoint?.region || config.aws?.region,
            });
        } else if (resolved === 'vonage') {
            adapter = new VonageSmsAdapter({
                apiKey: config.vonage?.apiKey,
                apiSecret: config.vonage?.apiSecret,
                defaultFrom: config.vonage?.sms?.fromNumber,
            });
        }
        if (!adapter) {
            throw createSmsProviderError(resolved, {
                message: `Unsupported SMS provider: ${resolved}`,
                code: 'sms_config_error',
                retryable: false,
            });
        }
        this.smsAdapters.set(resolved, adapter);
        return adapter;
    }

    isRetryableSmsError(error) {
        if (typeof error?.retryable === 'boolean') {
            return error.retryable;
        }
        const status = Number(error?.status || error?.statusCode || error?.response?.status);
        if (status === 429) return true;
        if (status >= 500 && status < 600) return true;
        const code = String(error?.code || '').toUpperCase();
        if (code === 'SMS_PROVIDER_TIMEOUT' || code === 'SMS_TIMEOUT') {
            return true;
        }
        if (RETRYABLE_NETWORK_CODES.has(code)) {
            return true;
        }
        const twilioCode = Number(error?.code);
        if (twilioCode === 20429 || twilioCode === 30008) {
            return true;
        }
        return false;
    }

    matchesOptOut(text = '') {
        const body = text.trim().toLowerCase();
        return ['stop', 'unsubscribe', 'cancel', 'quit', 'end'].includes(body);
    }

    matchesOptIn(text = '') {
        const body = text.trim().toLowerCase();
        return ['start', 'unstop', 'subscribe', 'yes'].includes(body);
    }

    async isOptedOut(phone) {
        const key = this.normalizePhone(phone);
        if (this.optOutCache.has(key)) {
            return this.optOutCache.get(key) === true;
        }
        if (this.db?.isSmsOptedOut) {
            try {
                const optedOut = await this.db.isSmsOptedOut(key);
                this.optOutCache.set(key, optedOut);
                return optedOut;
            } catch {
                return false;
            }
        }
        return false;
    }

    async setOptOut(phone, reason = null) {
        const key = this.normalizePhone(phone);
        this.optOutCache.set(key, true);
        if (this.db?.setSmsOptOut) {
            await this.db.setSmsOptOut(key, reason);
        }
    }

    async clearOptOut(phone) {
        const key = this.normalizePhone(phone);
        this.optOutCache.set(key, false);
        if (this.db?.clearSmsOptOut) {
            await this.db.clearSmsOptOut(key);
        }
    }

    isWithinQuietHours(date = new Date(), quietHours = null) {
        const hours = quietHours || this.defaultQuietHours;
        const hour = date.getHours();
        return hour < hours.start || hour >= hours.end;
    }

    nextAllowedTime(date = new Date(), quietHours = null) {
        const hours = quietHours || this.defaultQuietHours;
        const next = new Date(date);
        if (next.getHours() >= hours.end) {
            next.setDate(next.getDate() + 1);
        }
        next.setHours(hours.start, 0, 0, 0);
        return next;
    }

    // Send individual SMS
    async sendSMS(to, message, from = null, options = {}) {
        const normalizedTo = this.normalizePhone(to);
        const body = this.normalizeBody(message);
        const bodyHash = this.hashBody(body);
        const {
            idempotencyKey = null,
            allowQuietHours = true,
            quietHours = null,
            maxRetries = this.defaultMaxRetries,
            retryDelayMs = this.defaultRetryDelayMs,
            minIntervalMs = this.defaultMinIntervalMs,
            mediaUrl = null,
            providerTimeoutMs = this.defaultProviderTimeoutMs,
            userChatId = null,
            provider = null,
        } = options;

        if (!normalizedTo) {
            const error = new Error('No destination number provided');
            error.code = 'sms_validation_failed';
            throw error;
        }
        if (!isValidE164(normalizedTo)) {
            const error = new Error('Destination phone number must be in E.164 format');
            error.code = 'sms_validation_failed';
            throw error;
        }
        if (!body) {
            const error = new Error('No message body provided');
            error.code = 'sms_validation_failed';
            throw error;
        }
        if (body.length > this.maxMessageChars) {
            const error = new Error(`Message body exceeds ${this.maxMessageChars} characters`);
            error.code = 'sms_validation_failed';
            throw error;
        }

        const segmentInfo = getSmsSegmentInfo(body);
        const resolvedProvider = this.resolveProvider(provider);
        const providerReadiness = this.getProviderReadiness();
        if (!providerReadiness[resolvedProvider]) {
            throw createSmsProviderError(resolvedProvider, {
                message: `SMS provider ${resolvedProvider} is not configured`,
                code: 'sms_config_error',
                retryable: false,
            });
        }
        const adapter = this.getAdapter(resolvedProvider);
        if (!adapter.isConfigured()) {
            throw createSmsProviderError(resolvedProvider, {
                message: `SMS provider ${resolvedProvider} is not ready`,
                code: 'sms_config_error',
                retryable: false,
            });
        }
        const fromNumber = adapter.resolveFromNumber(from);

        if (!fromNumber) {
            throw createSmsProviderError(resolvedProvider, {
                message: 'No source number configured for selected SMS provider',
                code: 'sms_config_error',
                retryable: false,
            });
        }

        if (await this.isOptedOut(normalizedTo)) {
            return { success: false, suppressed: true, reason: 'opted_out', segment_info: segmentInfo };
        }

        if (allowQuietHours && this.isWithinQuietHours(new Date(), quietHours)) {
            const scheduledTime = this.nextAllowedTime(new Date(), quietHours);
            await this.scheduleSMS(normalizedTo, body, scheduledTime, {
                reason: 'quiet_hours',
                from: fromNumber,
                userChatId,
                idempotencyKey: idempotencyKey ? `${idempotencyKey}:quiet_hours` : null,
                smsOptions: {
                    allowQuietHours: false,
                    maxRetries,
                    retryDelayMs,
                    minIntervalMs,
                    mediaUrl,
                    provider: resolvedProvider,
                },
            });
            return { success: true, scheduled: true, scheduled_time: scheduledTime.toISOString(), segment_info: segmentInfo };
        }

        const lastSend = this.lastSendAt.get(normalizedTo) || 0;
        if (Number(minIntervalMs) > 0 && Date.now() - lastSend < minIntervalMs) {
            const scheduledTime = new Date(Date.now() + Number(minIntervalMs));
            await this.scheduleSMS(normalizedTo, body, scheduledTime, {
                reason: 'rate_limit',
                from: fromNumber,
                userChatId,
                idempotencyKey: idempotencyKey ? `${idempotencyKey}:rate_limit` : null,
                smsOptions: {
                    allowQuietHours: false,
                    maxRetries,
                    retryDelayMs,
                    minIntervalMs: 0,
                    mediaUrl,
                    provider: resolvedProvider,
                },
            });
            return { success: true, scheduled: true, scheduled_time: scheduledTime.toISOString(), segment_info: segmentInfo };
        }

        if (idempotencyKey) {
            const cached = this.idempotencyCache.get(idempotencyKey);
            if (cached?.messageSid) {
                if (
                    (cached.toNumber && cached.toNumber !== normalizedTo) ||
                    (cached.bodyHash && cached.bodyHash !== bodyHash)
                ) {
                    const conflict = new Error('Idempotency key reuse with different payload');
                    conflict.code = 'idempotency_conflict';
                    throw conflict;
                }
                return {
                    success: true,
                    idempotent: true,
                    message_sid: cached.messageSid,
                    segment_info: segmentInfo,
                };
            }
            if (this.db?.getSmsIdempotency) {
                const existing = await this.db.getSmsIdempotency(idempotencyKey);
                if (existing?.message_sid) {
                    if (
                        (existing.to_number && existing.to_number !== normalizedTo) ||
                        (existing.body_hash && existing.body_hash !== bodyHash)
                    ) {
                        const conflict = new Error('Idempotency key reuse with different payload');
                        conflict.code = 'idempotency_conflict';
                        throw conflict;
                    }
                    this.idempotencyCache.set(idempotencyKey, {
                        messageSid: existing.message_sid,
                        toNumber: existing.to_number || normalizedTo,
                        bodyHash: existing.body_hash || bodyHash,
                    });
                    return {
                        success: true,
                        idempotent: true,
                        message_sid: existing.message_sid,
                        segment_info: segmentInfo,
                    };
                }
            }
        }

        const payload = {
            body,
            from: fromNumber,
            to: normalizedTo,
            statusCallback: config.server.hostname
                ? `https://${config.server.hostname}/webhook/sms-status`
                : undefined,
            idempotencyKey: idempotencyKey || undefined,
        };
        if (mediaUrl) {
            const mediaList = Array.isArray(mediaUrl) ? mediaUrl : [mediaUrl];
            payload.mediaUrl = mediaList.filter(Boolean).slice(0, 5);
        }

        const maxAttempts = Math.max(1, Number(maxRetries || 0) + 1);
        let attempt = 0;
        let lastError = null;
        while (attempt < maxAttempts) {
            attempt += 1;
            try {
                console.log('sms_send_attempt', {
                    provider: resolvedProvider,
                    to: redactPhoneForLog(normalizedTo),
                    from: redactPhoneForLog(fromNumber),
                    idempotency_key: idempotencyKey ? 'present' : 'absent',
                    attempt,
                    max_attempts: maxAttempts,
                    body_preview: redactBodyForLog(body),
                });
                const smsMessage = await adapter.sendSms(
                    payload,
                    {
                        withTimeout: this.withTimeout.bind(this),
                        providerTimeoutMs,
                    },
                );
                const messageSid = smsMessage?.messageSid || null;
                if (!messageSid) {
                    throw createSmsProviderError(resolvedProvider, {
                        message: 'SMS provider did not return message id',
                        code: 'sms_provider_error',
                        retryable: false,
                    });
                }
                this.lastSendAt.set(normalizedTo, Date.now());
                if (idempotencyKey) {
                    this.idempotencyCache.set(idempotencyKey, {
                        messageSid,
                        toNumber: normalizedTo,
                        bodyHash,
                        provider: resolvedProvider,
                    });
                    if (this.db?.saveSmsIdempotency) {
                        await this.db.saveSmsIdempotency(
                            idempotencyKey,
                            messageSid,
                            normalizedTo,
                            bodyHash,
                        );
                    }
                }
                console.log('sms_send_success', {
                    provider: resolvedProvider,
                    to: redactPhoneForLog(normalizedTo),
                    message_sid: messageSid,
                    attempt,
                    status: smsMessage?.status || 'queued',
                });
                return {
                    success: true,
                    message_sid: messageSid,
                    provider: resolvedProvider,
                    to: normalizedTo,
                    from: smsMessage?.from || fromNumber,
                    body,
                    status: smsMessage?.status,
                    segment_info: segmentInfo
                };
            } catch (error) {
                lastError = error;
                const retryable = this.isRetryableSmsError(error);
                if (!retryable || attempt >= maxAttempts) {
                    break;
                }
                const delay = this.computeRetryDelayMs(retryDelayMs, attempt);
                console.warn('sms_send_retry_scheduled', {
                    provider: resolvedProvider,
                    to: redactPhoneForLog(normalizedTo),
                    attempt,
                    max_attempts: maxAttempts,
                    retry_in_ms: delay,
                    reason: sanitizeErrorMessage(error?.message || 'send_failed'),
                });
                await sleep(delay);
            }
        }
        throw lastError || new Error('SMS send failed');
    }

    // Send bulk SMS
    async sendBulkSMS(recipients, message, options = {}) {
        const results = [];
        const {
            delay = 1000,
            batchSize = 10,
            from = null,
            smsOptions = {},
            validateNumbers = true,
            idempotencyKey = null,
            durable = false,
            userChatId = null,
        } = options;

        const body = this.normalizeBody(message);
        if (!body) {
            throw new Error('No message body provided');
        }
        if (body.length > this.maxMessageChars) {
            throw new Error(`Message body exceeds ${this.maxMessageChars} characters`);
        }

        const segmentInfo = getSmsSegmentInfo(body);

        console.log('sms_bulk_start', {
            recipients: recipients.length,
            durable: durable === true,
            body_preview: redactBodyForLog(body),
        });

        if (durable && this.db?.createCallJob) {
            let queued = 0;
            let invalid = 0;
            for (const recipient of recipients) {
                const normalizedRecipient = this.normalizePhone(recipient);
                if (validateNumbers && !isValidE164(normalizedRecipient)) {
                    invalid += 1;
                    results.push({
                        recipient: normalizedRecipient,
                        success: false,
                        error: 'invalid_phone_format',
                        segment_info: segmentInfo,
                    });
                    continue;
                }
                const recipientHash = crypto
                    .createHash('sha1')
                    .update(normalizedRecipient)
                    .digest('hex')
                    .slice(0, 16);
                const recipientIdempotencyKey = idempotencyKey
                    ? `${idempotencyKey}:${recipientHash}`
                    : null;
                try {
                    const schedule = await this.scheduleSMS(
                        normalizedRecipient,
                        body,
                        new Date(),
                        {
                            reason: 'bulk_durable',
                            from,
                            userChatId,
                            idempotencyKey: recipientIdempotencyKey,
                            smsOptions: { ...smsOptions, allowQuietHours: false, minIntervalMs: 0 },
                        },
                    );
                    queued += 1;
                    results.push({
                        recipient: normalizedRecipient,
                        success: true,
                        queued: true,
                        schedule_id: schedule?.schedule_id || null,
                        segment_info: segmentInfo,
                    });
                } catch (scheduleError) {
                    results.push({
                        recipient: normalizedRecipient,
                        success: false,
                        error: scheduleError?.message || 'schedule_failed',
                        segment_info: segmentInfo,
                    });
                }
            }
            return {
                total: recipients.length,
                successful: queued,
                failed: recipients.length - queued,
                scheduled: queued,
                suppressed: 0,
                invalid,
                durable: true,
                segment_info: segmentInfo,
                results,
            };
        }

        // Process in batches to avoid rate limiting
        for (let i = 0; i < recipients.length; i += batchSize) {
            const batch = recipients.slice(i, i + batchSize);
            const batchPromises = batch.map(async (recipient) => {
                const normalizedRecipient = this.normalizePhone(recipient);
                if (validateNumbers && !isValidE164(normalizedRecipient)) {
                    return {
                        recipient: normalizedRecipient,
                        success: false,
                        error: 'invalid_phone_format',
                        segment_info: segmentInfo
                    };
                }
                try {
                    const recipientHash = crypto
                        .createHash('sha1')
                        .update(normalizedRecipient)
                        .digest('hex')
                        .slice(0, 16);
                    const recipientIdempotencyKey = idempotencyKey
                        ? `${idempotencyKey}:${recipientHash}`
                        : null;
                    const result = await this.sendSMS(normalizedRecipient, body, from, {
                        ...smsOptions,
                        userChatId,
                        idempotencyKey: recipientIdempotencyKey || smsOptions.idempotencyKey || null,
                    });
                    return { ...result,
                        recipient: normalizedRecipient,
                        success: result.success === true,
                        segment_info: result.segment_info || segmentInfo
                    };
                } catch (error) {
                    return {
                        recipient: normalizedRecipient,
                        success: false,
                        error: error.message,
                        segment_info: segmentInfo
                    };
                }
            });

            const batchResults = await Promise.allSettled(batchPromises);
            results.push(
                ...batchResults.map((entry, index) => {
                    if (entry.status === 'fulfilled') {
                        return entry.value;
                    }
                    return {
                        recipient: this.normalizePhone(batch[index]),
                        success: false,
                        error: entry.reason?.message || 'send_failed',
                        segment_info: segmentInfo,
                    };
                }),
            );

            // Add delay between batches
            if (i + batchSize < recipients.length) {
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }

        const successful = results.filter(r => r.success).length;
        const failed = results.length - successful;
        const scheduled = results.filter(r => r.scheduled).length;
        const suppressed = results.filter(r => r.suppressed).length;
        const invalid = results.filter(r => r.error === 'invalid_phone_format').length;

        console.log(`📊 Bulk SMS completed: ${successful} sent, ${failed} failed`);

        return {
            total: recipients.length,
            successful,
            failed,
            scheduled,
            suppressed,
            invalid,
            segment_info: segmentInfo,
            results
        };
    }

    // AI-powered SMS conversation
    async handleIncomingSMS(from, body, messageSid) {
        try {
            console.log('sms_incoming', {
                from: redactPhoneForLog(from),
                message_sid: messageSid || null,
                body_preview: redactBodyForLog(body),
            });
            const normalizedFrom = this.normalizePhone(from);
            const normalizedBody = this.normalizeBody(body);

            if (this.matchesOptOut(normalizedBody)) {
                await this.setOptOut(normalizedFrom, 'user_opt_out');
                const confirm = "You’re unsubscribed. Reply START to re-enable SMS.";
                await this.sendSMS(normalizedFrom, confirm, null, { allowQuietHours: false });
                return { success: true, opted_out: true };
            }

            if (this.matchesOptIn(normalizedBody)) {
                await this.clearOptOut(normalizedFrom);
                const confirm = "You’re re-subscribed. Reply HELP for options.";
                await this.sendSMS(normalizedFrom, confirm, null, { allowQuietHours: false });
                return { success: true, opted_in: true };
            }

            // Get or create conversation context
            let conversation = this.activeConversations.get(normalizedFrom);
            if (!conversation) {
                conversation = {
                    phone: normalizedFrom,
                    messages: [],
                    context: `You are a helpful SMS assistant. Keep responses concise (under 160 chars when possible). Be friendly and professional.`,
                    created_at: new Date(),
                    last_activity: new Date()
                };
                this.activeConversations.set(normalizedFrom, conversation);
            }

            // Add incoming message to conversation
            conversation.messages.push({
                role: 'user',
                content: normalizedBody,
                timestamp: new Date(),
                message_sid: messageSid
            });
            conversation.last_activity = new Date();

            // Generate AI response
            const aiResponse = await this.generateAIResponse(conversation);

            // Send response SMS
            const smsResult = await this.sendSMS(normalizedFrom, aiResponse);

            // Add AI response to conversation
            conversation.messages.push({
                role: 'assistant',
                content: aiResponse,
                timestamp: new Date(),
                message_sid: smsResult.message_sid
            });

            // Emit events for tracking
            this.emit('conversation_updated', {
                phone: from,
                conversation: conversation,
                ai_response: aiResponse
            });

            return {
                success: true,
                ai_response: aiResponse,
                message_sid: smsResult.message_sid
            };

        } catch (error) {
            console.error('❌ Error handling incoming SMS:', {
                from: redactPhoneForLog(from),
                message_sid: messageSid || null,
                error: redactBodyForLog(error?.message || 'incoming_sms_failed', 120),
            });

            // Send fallback message
            try {
                await this.sendSMS(from, "Sorry, I'm experiencing technical difficulties. Please try again later.");
            } catch (fallbackError) {
                console.error('❌ Failed to send fallback message:', fallbackError);
            }

            throw error;
        }
    }

    // Generate AI response for SMS
    async generateAIResponse(conversation) {
        try {
            const messages = [{
                role: 'system',
                content: conversation.context
            }, ...conversation.messages.slice(-10) // Keep last 10 messages for context
            ];

            const completion = await this.withTimeout(
                this.openai.chat.completions.create({
                    model: this.model,
                    messages: messages,
                    max_tokens: 150,
                    temperature: 0.7
                }),
                this.defaultAiTimeoutMs,
                'sms_ai_timeout',
            );

            let response = completion.choices[0].message.content.trim();

            // Ensure response is SMS-friendly (under 1600 chars, ideally under 160)
            if (response.length > 1500) {
                response = response.substring(0, 1500) + "...";
            }

            return response;

        } catch (error) {
            console.error('❌ AI response generation error:', {
                error: redactBodyForLog(error?.message || 'ai_response_failed', 120),
            });
            return "I apologize, but I'm having trouble processing your request right now. Please try again later.";
        }
    }

    // Get conversation history
    getConversation(phone) {
        return this.activeConversations.get(phone) || null;
    }

    // Get active conversations summary
    getActiveConversations() {
        const conversations = [];
        for (const [phone, conversation] of this.activeConversations.entries()) {
            conversations.push({
                phone,
                message_count: conversation.messages.length,
                created_at: conversation.created_at,
                last_activity: conversation.last_activity
            });
        }
        return conversations;
    }

    // Clean up old conversations
    cleanupOldConversations(maxAgeHours = 24) {
        const cutoff = new Date(Date.now() - (maxAgeHours * 60 * 60 * 1000));
        let cleanedCount = 0;

        for (const [phone, conversation] of this.activeConversations.entries()) {
            if (conversation.last_activity < cutoff) {
                this.activeConversations.delete(phone);
                cleanedCount++;
            }
        }

        if (cleanedCount > 0) {
            console.log(`🧹 Cleaned up ${cleanedCount} old SMS conversations`);
        }

        return cleanedCount;
    }

    // Schedule SMS for later sending
    async scheduleSMS(to, message, scheduledTime, options = {}) {
        const normalizedTo = this.normalizePhone(to);
        const body = this.normalizeBody(message);
        if (!isValidE164(normalizedTo)) {
            throw new Error('Invalid phone number format');
        }
        if (!body) {
            throw new Error('No message body provided');
        }
        if (body.length > this.maxMessageChars) {
            throw new Error(`Message body exceeds ${this.maxMessageChars} characters`);
        }
        const parsedSchedule = new Date(scheduledTime);
        if (Number.isNaN(parsedSchedule.getTime())) {
            throw new Error('Invalid scheduled time');
        }

        const fromNumber = options.from || null;
        const smsOptions = { ...(options.smsOptions || {}) };
        const idempotencyKey = options.idempotencyKey || null;
        const userChatId = options.userChatId || null;

        if (this.db?.createCallJob) {
            const payload = {
                to: normalizedTo,
                message: body,
                from: fromNumber,
                user_chat_id: userChatId,
                idempotency_key: idempotencyKey,
                sms_options: {
                    ...smsOptions,
                    allowQuietHours: false,
                    minIntervalMs: 0,
                },
            };
            const jobId = await this.db.createCallJob(
                'sms_scheduled_send',
                payload,
                parsedSchedule.toISOString(),
            );
            console.log('sms_scheduled_job_created', {
                schedule_id: `sms_job_${jobId}`,
                to: redactPhoneForLog(normalizedTo),
                scheduled_at: parsedSchedule.toISOString(),
                reason: options.reason || 'scheduled',
            });
            return {
                schedule_id: `sms_job_${jobId}`,
                scheduled_time: parsedSchedule.toISOString(),
                status: 'scheduled',
                durable: true,
            };
        }

        const scheduleData = {
            to: normalizedTo,
            message: body,
            scheduledTime: parsedSchedule,
            created_at: new Date(),
            options,
            status: 'scheduled'
        };

        // In a real implementation, this would be stored in database
        // For now, we'll use a simple Map
        const scheduleId = `sched_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        this.messageQueue.set(scheduleId, scheduleData);

        console.log('sms_scheduled_in_memory', {
            schedule_id: scheduleId,
            to: redactPhoneForLog(normalizedTo),
            scheduled_at: parsedSchedule.toISOString(),
            reason: options.reason || 'scheduled',
        });

        return {
            schedule_id: scheduleId,
            scheduled_time: parsedSchedule.toISOString(),
            status: 'scheduled'
        };
    }

    // Process scheduled messages
    async processScheduledMessages() {
        if (this.scheduledProcessing) {
            return 0;
        }
        this.scheduledProcessing = true;
        const now = new Date();
        const toSend = [];

        try {
            for (const [scheduleId, scheduleData] of this.messageQueue.entries()) {
                if (scheduleData.status === 'scheduled' && scheduleData.scheduledTime <= now) {
                    toSend.push({
                        scheduleId,
                        scheduleData
                    });
                }
            }

            for (const {
                    scheduleId,
                    scheduleData
                } of toSend) {
                try {
                    const result = await this.sendSMS(
                        scheduleData.to,
                        scheduleData.message,
                        scheduleData?.options?.from || null,
                        {
                            ...(scheduleData?.options?.smsOptions || {}),
                            allowQuietHours: false,
                            minIntervalMs: 0,
                            idempotencyKey: scheduleData?.options?.idempotencyKey || null,
                        },
                    );
                    scheduleData.status = 'sent';
                    scheduleData.sent_at = new Date();
                    scheduleData.message_sid = result.message_sid;

                    console.log('sms_scheduled_sent', {
                        schedule_id: scheduleId,
                        message_sid: result.message_sid || null,
                    });
                } catch (error) {
                    console.error('❌ Failed to send scheduled SMS', {
                        schedule_id: scheduleId,
                        error: redactBodyForLog(error?.message || 'scheduled_send_failed', 120),
                    });
                    scheduleData.status = 'failed';
                    scheduleData.error = error.message;
                }
            }

            return toSend.length;
        } finally {
            this.scheduledProcessing = false;
        }
    }

    // SMS scripts system
    getScript(scriptName, variables = {}) {
        const scripts = {
            welcome: "Welcome to our service! We're excited to have you aboard. Reply HELP for assistance or STOP to unsubscribe.",
            appointment_reminder: "Reminder: You have an appointment on {date} at {time}. Reply CONFIRM to confirm or RESCHEDULE to change.",
            verification: "Your verification code is: {code}. This code will expire in 10 minutes. Do not share this code with anyone.",
            order_update: "Order #{order_id} update: {status}. Track your order at {tracking_url}",
            payment_reminder: "Payment reminder: Your payment of {amount} is due on {due_date}. Pay now: {payment_url}",
            promotional: "🎉 Special offer just for you! {offer_text} Use code {promo_code}. Valid until {expiry_date}. Reply STOP to opt out.",
            customer_service: "Thanks for contacting us! We've received your message and will respond within 24 hours. For urgent matters, call {phone}.",
            survey: "How was your experience with us? Rate us 1-5 stars by replying with a number. Your feedback helps us improve!"
        };

        let script = scripts[scriptName];
        if (!script) {
            throw new Error(`Script '${scriptName}' not found`);
        }

        // Replace variables
        for (const [key, value] of Object.entries(variables)) {
            script = script.replace(new RegExp(`{${key}}`, 'g'), value);
        }

        return script;
    }

    // Get service statistics
    getStatistics() {
        const activeConversations = this.activeConversations.size;
        const scheduledMessages = Array.from(this.messageQueue.values())
            .filter(msg => msg.status === 'scheduled').length;

        return {
            active_conversations: activeConversations,
            scheduled_messages: scheduledMessages,
            total_conversations_today: activeConversations, // Would be from DB in real implementation
            message_queue_size: this.messageQueue.size
        };
    }
}

module.exports = { EnhancedSmsService };
