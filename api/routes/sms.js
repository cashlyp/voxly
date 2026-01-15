const EventEmitter = require('events');
const axios = require('axios');
const crypto = require('crypto');
const config = require('../config');

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

class EnhancedSmsService extends EventEmitter {
    constructor(options = {}) {
        super();
        this.db = options.db || null;
        this.twilio = require('twilio')(
            config.twilio.accountSid,
            config.twilio.authToken
        );
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
        this.defaultQuietHours = { start: 9, end: 20 };
        this.defaultMaxRetries = 2;
        this.defaultRetryDelayMs = 2000;
        this.defaultMinIntervalMs = 2000;
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

    isRetryableSmsError(error) {
        const code = error?.code || error?.status || error?.statusCode;
        const retryableCodes = new Set([30005, 30007, 30008, 21614, 21617]);
        if (retryableCodes.has(code)) return true;
        const msg = String(error?.message || '').toLowerCase();
        return msg.includes('timeout') || msg.includes('rate') || msg.includes('temporarily');
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
        try {
            const fromNumber = from || config.twilio.fromNumber;
            const normalizedTo = this.normalizePhone(to);
            const body = this.normalizeBody(message);
            const {
                idempotencyKey = null,
                allowQuietHours = true,
                quietHours = null,
                maxRetries = this.defaultMaxRetries,
                retryDelayMs = this.defaultRetryDelayMs,
                minIntervalMs = this.defaultMinIntervalMs,
                mediaUrl = null
            } = options;

            if (!fromNumber) {
                throw new Error('No FROM_NUMBER configured for SMS');
            }

            if (!normalizedTo) {
                throw new Error('No destination number provided');
            }

            if (!body) {
                throw new Error('No message body provided');
            }

            const segmentInfo = getSmsSegmentInfo(body);

            if (await this.isOptedOut(normalizedTo)) {
                return { success: false, suppressed: true, reason: 'opted_out', segment_info: segmentInfo };
            }

            if (allowQuietHours && this.isWithinQuietHours(new Date(), quietHours)) {
                const scheduledTime = this.nextAllowedTime(new Date(), quietHours);
                await this.scheduleSMS(normalizedTo, body, scheduledTime, { reason: 'quiet_hours' });
                return { success: true, scheduled: true, scheduled_time: scheduledTime.toISOString(), segment_info: segmentInfo };
            }

            const lastSend = this.lastSendAt.get(normalizedTo) || 0;
            if (Date.now() - lastSend < minIntervalMs) {
                const scheduledTime = new Date(Date.now() + minIntervalMs);
                await this.scheduleSMS(normalizedTo, body, scheduledTime, { reason: 'rate_limit' });
                return { success: true, scheduled: true, scheduled_time: scheduledTime.toISOString(), segment_info: segmentInfo };
            }

            if (idempotencyKey) {
                if (this.idempotencyCache.has(idempotencyKey)) {
                    return { success: true, idempotent: true, message_sid: this.idempotencyCache.get(idempotencyKey), segment_info: segmentInfo };
                }
                if (this.db?.getSmsIdempotency) {
                    const existing = await this.db.getSmsIdempotency(idempotencyKey);
                    if (existing?.message_sid) {
                        this.idempotencyCache.set(idempotencyKey, existing.message_sid);
                        return { success: true, idempotent: true, message_sid: existing.message_sid, segment_info: segmentInfo };
                    }
                }
            }

            console.log(`📱 Sending SMS to ${normalizedTo}: ${body.substring(0, 50)}...`);

            const payload = {
                body,
                from: fromNumber,
                to: normalizedTo,
                statusCallback: config.server.hostname
                    ? `https://${config.server.hostname}/webhook/sms-status`
                    : undefined
            };
            if (mediaUrl) {
                payload.mediaUrl = Array.isArray(mediaUrl) ? mediaUrl : [mediaUrl];
            }

            const smsMessage = await this.twilio.messages.create(payload);

            console.log(`✅ SMS sent successfully: ${smsMessage.sid}`);
            this.lastSendAt.set(normalizedTo, Date.now());
            if (idempotencyKey) {
                this.idempotencyCache.set(idempotencyKey, smsMessage.sid);
                if (this.db?.saveSmsIdempotency) {
                    await this.db.saveSmsIdempotency(idempotencyKey, smsMessage.sid, normalizedTo, this.hashBody(body));
                }
            }
            return {
                success: true,
                message_sid: smsMessage.sid,
                to: normalizedTo,
                from: fromNumber,
                body,
                status: smsMessage.status,
                segment_info: segmentInfo
            };
        } catch (error) {
            console.error('❌ SMS sending error:', error);
            const attempts = Number.isFinite(options?.attempts) ? options.attempts : 0;
            if (attempts < (options?.maxRetries ?? this.defaultMaxRetries) && this.isRetryableSmsError(error)) {
                const delay = (options?.retryDelayMs ?? this.defaultRetryDelayMs) * Math.pow(2, attempts);
                setTimeout(() => {
                    this.sendSMS(to, message, from, { ...options, attempts: attempts + 1 }).catch(() => {});
                }, delay);
                return { success: false, queued_retry: true, retry_in_ms: delay };
            }
            throw error;
        }
    }

    // Send bulk SMS
    async sendBulkSMS(recipients, message, options = {}) {
        const results = [];
        const {
            delay = 1000,
            batchSize = 10,
            from = null,
            smsOptions = {},
            validateNumbers = true
        } = options;

        const segmentInfo = getSmsSegmentInfo(message);

        console.log(`📱 Sending bulk SMS to ${recipients.length} recipients`);

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
                    const result = await this.sendSMS(normalizedRecipient, message, from, smsOptions);
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
            results.push(...batchResults.map(r => r.value));

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
            console.log(`📨 Incoming SMS from ${from}: ${body}`);
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
            console.error('❌ Error handling incoming SMS:', error);

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

            const completion = await this.openai.chat.completions.create({
                model: this.model,
                messages: messages,
                max_tokens: 150,
                temperature: 0.7
            });

            let response = completion.choices[0].message.content.trim();

            // Ensure response is SMS-friendly (under 1600 chars, ideally under 160)
            if (response.length > 1500) {
                response = response.substring(0, 1500) + "...";
            }

            return response;

        } catch (error) {
            console.error('❌ AI response generation error:', error);
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
        const scheduleData = {
            to,
            message,
            scheduledTime: new Date(scheduledTime),
            created_at: new Date(),
            options,
            status: 'scheduled'
        };

        // In a real implementation, this would be stored in database
        // For now, we'll use a simple Map
        const scheduleId = `sched_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        this.messageQueue.set(scheduleId, scheduleData);

        console.log(`🗓️ SMS scheduled for ${scheduledTime}: ${scheduleId}`);

        return {
            schedule_id: scheduleId,
            scheduled_time: scheduledTime,
            status: 'scheduled'
        };
    }

    // Process scheduled messages
    async processScheduledMessages() {
        const now = new Date();
        const toSend = [];

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
                const result = await this.sendSMS(scheduleData.to, scheduleData.message);
                scheduleData.status = 'sent';
                scheduleData.sent_at = new Date();
                scheduleData.message_sid = result.message_sid;

                console.log(`📱 Scheduled SMS sent: ${scheduleId}`);
            } catch (error) {
                console.error(`❌ Failed to send scheduled SMS ${scheduleId}:`, error);
                scheduleData.status = 'failed';
                scheduleData.error = error.message;
            }
        }

        return toSend.length;
    }

    // SMS templates system
    getTemplate(templateName, variables = {}) {
        const templates = {
            welcome: "Welcome to our service! We're excited to have you aboard. Reply HELP for assistance or STOP to unsubscribe.",
            appointment_reminder: "Reminder: You have an appointment on {date} at {time}. Reply CONFIRM to confirm or RESCHEDULE to change.",
            verification: "Your verification code is: {code}. This code will expire in 10 minutes. Do not share this code with anyone.",
            order_update: "Order #{order_id} update: {status}. Track your order at {tracking_url}",
            payment_reminder: "Payment reminder: Your payment of {amount} is due on {due_date}. Pay now: {payment_url}",
            promotional: "🎉 Special offer just for you! {offer_text} Use code {promo_code}. Valid until {expiry_date}. Reply STOP to opt out.",
            customer_service: "Thanks for contacting us! We've received your message and will respond within 24 hours. For urgent matters, call {phone}.",
            survey: "How was your experience with us? Rate us 1-5 stars by replying with a number. Your feedback helps us improve!"
        };

        let template = templates[templateName];
        if (!template) {
            throw new Error(`Template '${templateName}' not found`);
        }

        // Replace variables
        for (const [key, value] of Object.entries(variables)) {
            template = template.replace(new RegExp(`{${key}}`, 'g'), value);
        }

        return template;
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
