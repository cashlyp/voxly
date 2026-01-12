const EventEmitter = require('events');
const axios = require('axios');
const config = require('../config');

class EnhancedSmsService extends EventEmitter {
    constructor() {
        super();
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
    }

    // Send individual SMS
    async sendSMS(to, message, from = null) {
        try {
            const fromNumber = from || config.twilio.fromNumber;

            if (!fromNumber) {
                throw new Error('No FROM_NUMBER configured for SMS');
            }

            console.log(`📱 Sending SMS to ${to}: ${message.substring(0, 50)}...`);

            const smsMessage = await this.twilio.messages.create({
                body: message,
                from: fromNumber,
                to: to,
                statusCallback: config.server.hostname
                    ? `https://${config.server.hostname}/webhook/sms-status`
                    : undefined
            });

            console.log(`✅ SMS sent successfully: ${smsMessage.sid}`);
            return {
                success: true,
                message_sid: smsMessage.sid,
                to: to,
                from: fromNumber,
                body: message,
                status: smsMessage.status
            };
        } catch (error) {
            console.error('❌ SMS sending error:', error);
            throw error;
        }
    }

    // Send bulk SMS
    async sendBulkSMS(recipients, message, options = {}) {
        const results = [];
        const {
            delay = 1000, batchSize = 10
        } = options;

        console.log(`📱 Sending bulk SMS to ${recipients.length} recipients`);

        // Process in batches to avoid rate limiting
        for (let i = 0; i < recipients.length; i += batchSize) {
            const batch = recipients.slice(i, i + batchSize);
            const batchPromises = batch.map(async (recipient) => {
                try {
                    const result = await this.sendSMS(recipient, message);
                    return { ...result,
                        recipient,
                        success: true
                    };
                } catch (error) {
                    return {
                        recipient,
                        success: false,
                        error: error.message
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

        console.log(`📊 Bulk SMS completed: ${successful} sent, ${failed} failed`);

        return {
            total: recipients.length,
            successful,
            failed,
            results
        };
    }

    // AI-powered SMS conversation
    async handleIncomingSMS(from, body, messageSid) {
        try {
            console.log(`📨 Incoming SMS from ${from}: ${body}`);

            // Get or create conversation context
            let conversation = this.activeConversations.get(from);
            if (!conversation) {
                conversation = {
                    phone: from,
                    messages: [],
                    context: `You are a helpful SMS assistant. Keep responses concise (under 160 chars when possible). Be friendly and professional.`,
                    created_at: new Date(),
                    last_activity: new Date()
                };
                this.activeConversations.set(from, conversation);
            }

            // Add incoming message to conversation
            conversation.messages.push({
                role: 'user',
                content: body,
                timestamp: new Date(),
                message_sid: messageSid
            });
            conversation.last_activity = new Date();

            // Generate AI response
            const aiResponse = await this.generateAIResponse(conversation);

            // Send response SMS
            const smsResult = await this.sendSMS(from, aiResponse);

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
