const config = require('../config');
const axios = require('axios');
const { getUser, isAdmin } = require('../db/db');
const { escapeMarkdown, buildLine } = require('../utils/messageStyle');

module.exports = (bot) => {
    // API test command (enhanced)
    bot.command('testapi', async (ctx) => {
        try {
            // Check if user is authorized and is admin
            const user = await new Promise(r => getUser(ctx.from.id, r));
            if (!user) {
                return ctx.reply('❌ You are not authorized to use this bot.');
            }

            const adminStatus = await new Promise(r => isAdmin(ctx.from.id, r));
            if (!adminStatus) {
                return ctx.reply('❌ This command is for administrators only.');
            }

            await ctx.reply('🧪 Testing API connection...');

            console.log('Testing API connection to:', config.apiUrl);
            const startTime = Date.now();
            const response = await axios.get(`${config.apiUrl}/health`, {
                timeout: 10000,
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json'
                }
            });
            const responseTime = Date.now() - startTime;
            
            const health = response.data;
            console.log('API Health Response:', health);
            
            const apiStatusLabel = escapeMarkdown(health.status || 'healthy');
            let message = `✅ *API Status: ${apiStatusLabel}*\n\n`;
            message += `${buildLine('🔗', 'URL', escapeMarkdown(config.apiUrl))}\n`;
            message += `${buildLine('⚡', 'Response Time', `${responseTime}ms`)}\n`;
            message += `${buildLine('📊', 'Active Calls', health.active_calls || 0)}\n`;
            
            // Handle different response structures
            if (health.services) {
                const db = health.services.database;
                const webhook = health.services.webhook_service;

                message += `${buildLine('🗄️', 'Database', db?.connected ? '✅ Connected' : '❌ Disconnected')}\n`;
                if (db?.recent_calls !== undefined) {
                    message += `${buildLine('📋', 'Recent Calls', db.recent_calls)}\n`;
                } else {
                    message += `${buildLine('📋', 'Recent Calls', db?.recent_calls || 0)}\n`;
                }
                message += `${buildLine('📡', 'Webhook Service', escapeMarkdown(webhook?.status || 'Unknown'))}\n`;

                if (health.adaptation_engine) {
                    message += `\n${buildLine('🤖', 'Adaptation Engine', '✅ Active')}\n`;
                    message += `${buildLine('🧩', 'Function Templates', health.adaptation_engine.available_templates || 0)}\n`;
                }
            } else {
                // Fallback for simpler health responses
                message += `${buildLine('🗄️', 'Database', health.database_connected ? '✅ Connected' : '❌ Unknown')}\n`;
            }
            
            message += `${buildLine('⏰', 'Timestamp', escapeMarkdown(new Date(health.timestamp).toLocaleString()))}\n`;
            
            // Add enhanced features info if available
            if (health.enhanced_features) {
            message += `\n🚀 Enhanced Features: ✅ Active`;
            }
            
            await ctx.reply(message, { parse_mode: 'Markdown' });
        } catch (error) {
            console.error('API test failed:', error);
            
            let errorMessage = `❌ *API Test Failed*\n\nURL: ${escapeMarkdown(config.apiUrl)}\n`;
            
            if (error.response) {
                errorMessage += `Status: ${escapeMarkdown(String(error.response.status))} - ${escapeMarkdown(error.response.statusText)}\n`;
                errorMessage += `Error: ${escapeMarkdown(error.response.data?.error || error.message)}`;
            } else if (error.code === 'ECONNREFUSED') {
                errorMessage += `Error: Connection refused - API server may be down`;
            } else if (error.code === 'ENOTFOUND') {
                errorMessage += `Error: Host not found - Check API URL`;
            } else if (error.code === 'ETIMEDOUT') {
                errorMessage += `Error: Request timeout - API server is not responding`;
            } else {
                errorMessage += `Error: ${escapeMarkdown(error.message)}`;
            }
            
            await ctx.reply(errorMessage, { parse_mode: 'Markdown' });
        }
    });

    // Status command (admin only) - Enhanced version
    bot.command('status', async (ctx) => {
        try {
            // Check if user is admin
            const user = await new Promise(r => getUser(ctx.from.id, r));
            const adminStatus = await new Promise(r => isAdmin(ctx.from.id, r));
            
            if (!user || !adminStatus) {
                return ctx.reply('❌ This command is for administrators only.');
            }

            await ctx.reply('🔍 Checking system status...');

            const startTime = Date.now();
            const response = await axios.get(`${config.apiUrl}/health`, {
                timeout: 15000,
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json'
                }
            });
            const responseTime = Date.now() - startTime;
            
            const health = response.data;
            
            const apiHealthStatus = health.status || 'healthy';
            let message = `🔍 *System Status Report*\n\n`;
            message += `🤖 Bot: ✅ Online & Responsive\n`;
            message += `🌐 API: ${health.status === 'healthy' ? '✅' : '❌'} ${escapeMarkdown(apiHealthStatus)}\n`;
            message += `${buildLine('⚡', 'API Response Time', `${responseTime}ms`)}\n\n`;
            
            // Enhanced service status
            if (health.services) {
                message += `*🔧 Services Status:*\n`;
                
                const db = health.services.database;
                message += `${buildLine('🗄️', 'Database', db?.connected ? '✅ Connected' : '❌ Disconnected')}\n`;
                if (db?.recent_calls !== undefined) {
                    message += `${buildLine('📋', 'Recent DB Calls', db.recent_calls)}\n`;
                }

                const webhook = health.services.webhook_service;
                if (webhook) {
                    message += `${buildLine('📡', 'Webhook Service', `${webhook.status === 'running' ? '✅' : '⚠️'} ${escapeMarkdown(webhook.status)}`)}\n`;
                    if (webhook.processed_today !== undefined) {
                        message += `${buildLine('📨', 'Webhooks Today', webhook.processed_today)}\n`;
                    }
                }

                const notifications = health.services.notification_system;
                if (notifications) {
                    message += `${buildLine('🔔', 'Notifications', `${escapeMarkdown(String(notifications.success_rate || 'N/A'))} success rate`)}\n`;
                }

                message += `\n`;
            }
            
            // Call statistics
            message += `*📊 Call Statistics:*\n`;
            message += `${buildLine('📞', 'Active Calls', health.active_calls || 0)}\n`;
            message += `✨ Keeping the console lively with ${health.active_calls || 0} active connections.\n`;
            
            // Enhanced features
            if (health.adaptation_engine) {
                message += `\n*🤖 AI Features:*\n`;
                message += `${buildLine('🧠', 'Adaptation Engine', '✅ Active')}\n`;
                message += `${buildLine('🧩', 'Function Templates', health.adaptation_engine.available_templates || 0)}\n`;
                message += `${buildLine('⚙️', 'Active Systems', health.adaptation_engine.active_function_systems || 0)}\n`;
            }
            
            if (health.enhanced_features) {
                message += `${buildLine('🚀', 'Enhanced Mode', '✅ Enabled')}\n`;
            }
            
            // System health logs (if available)
            if (health.system_health && health.system_health.length > 0) {
                message += `\n*🔍 Recent Activity:*\n`;
                health.system_health.slice(0, 3).forEach(log => {
                    const status = log.status === 'error' ? '❌' : '✅';
                    message += `${status} ${escapeMarkdown(log.service_name)}: ${log.count} ${escapeMarkdown(log.status)}\n`;
                });
            }
            
            message += `\n${buildLine('⏰','Last Updated', escapeMarkdown(new Date(health.timestamp).toLocaleString()))}`;
            message += `\n${buildLine('📡','API Endpoint', escapeMarkdown(config.apiUrl))}`;
            
            await ctx.reply(message, { parse_mode: 'Markdown' });
        } catch (error) {
            console.error('Status command error:', error);
            
            let errorMessage = `❌ *System Status Check Failed*\n\n`;
            errorMessage += `🤖 Bot: ✅ Online (you're seeing this message)\n`;
            errorMessage += `🌐 API: ❌ Connection failed\n\n`;
            
            if (error.response) {
                errorMessage += `📊 API Status: ${escapeMarkdown(String(error.response.status))} - ${escapeMarkdown(error.response.statusText)}\n`;
                errorMessage += `📝 Error Details: ${escapeMarkdown(error.response.data?.error || 'Unknown API error')}\n`;
            } else if (error.code === 'ECONNREFUSED') {
                errorMessage += `📝 Error: API server connection refused\n`;
                errorMessage += `💡 Suggestion: Check if the API server is running\n`;
            } else if (error.code === 'ENOTFOUND') {
                errorMessage += `📝 Error: API server not found\n`;
                errorMessage += `💡 Suggestion: Verify API URL configuration\n`;
            } else {
                errorMessage += `📝 Error: ${escapeMarkdown(error.message)}\n`;
            }
            
            errorMessage += `\n📡 API Endpoint: ${escapeMarkdown(config.apiUrl)}`;
            
            await ctx.reply(errorMessage, { parse_mode: 'Markdown' });
        }
    });

    // Search calls
    bot.command('search', async (ctx) => {
        try {
            const user = await new Promise(r => getUser(ctx.from.id, r));
            if (!user) return ctx.reply('❌ You are not authorized.');

            const parts = ctx.message.text.split(/\s+/).slice(1);
            const query = parts.join(' ').trim();
            if (!query || query.length < 2) {
                return ctx.reply('🔍 Usage: /search <term>');
            }

            await ctx.reply(`🔍 Searching calls for “${query}”…`);
            const res = await axios.get(`${config.apiUrl}/api/calls/search`, {
                params: { q: query, limit: 10 },
                timeout: 12000
            });

            const results = res.data?.results || [];
            if (!results.length) {
                return ctx.reply('ℹ️ No matches found.');
            }

            const lines = results.slice(0, 5).map((c) => {
                const status = c.status || 'unknown';
                const when = new Date(c.created_at).toLocaleString();
                const phone = c.phone_number || 'N/A';
                const summary = c.call_summary ? `\n📝 ${c.call_summary.slice(0, 120)}${c.call_summary.length > 120 ? '…' : ''}` : '';
                return `• ${c.call_sid} (${status})\n📞 ${phone}\n🕒 ${when}${summary}`;
            });
            await ctx.reply(lines.join('\n\n'));
        } catch (error) {
            console.error('Search command error:', error?.message || error);
            await ctx.reply('❌ Search failed.');
        }
    });

    // Recent calls
    bot.command('recent', async (ctx) => {
        try {
            const user = await new Promise(r => getUser(ctx.from.id, r));
            if (!user) return ctx.reply('❌ You are not authorized.');

            const parts = ctx.message.text.split(/\s+/).slice(1);
            const limit = Math.min(parseInt(parts[0], 10) || 10, 30);
            const filter = parts[1] || '';

            const res = await axios.get(`${config.apiUrl}/api/calls/recent`, {
                params: { limit, filter },
                timeout: 10000
            });
            const calls = res.data?.calls || [];
            if (!calls.length) {
                return ctx.reply('ℹ️ No recent calls.');
            }

            const lines = calls.map((c) => {
                const status = c.status || 'unknown';
                const when = new Date(c.created_at).toLocaleString();
                const duration = c.duration ? `${Math.floor(c.duration/60)}:${String(c.duration%60).padStart(2,'0')}` : 'N/A';
                const lastMsg = c.last_message_at ? ` | 🗨️ ${new Date(c.last_message_at).toLocaleTimeString()}` : '';
                return `• ${c.call_sid} (${status})\n📞 ${c.phone_number}\n⏱️ ${duration} | 🕒 ${when}${lastMsg}`;
            });
            await ctx.reply(lines.join('\n\n'));
        } catch (error) {
            console.error('Recent command error:', error?.message || error);
            await ctx.reply('❌ Failed to fetch recent calls.');
        }
    });

    // Call latency breakdown
    bot.command('latency', async (ctx) => {
        try {
            const user = await new Promise(r => getUser(ctx.from.id, r));
            if (!user) return ctx.reply('❌ You are not authorized.');

            const parts = ctx.message.text.split(/\s+/).slice(1);
            const callSid = parts[0];
            if (!callSid) {
                return ctx.reply('⏱️ Usage: /latency <callSid>');
            }
            const res = await axios.get(`${config.apiUrl}/api/calls/${callSid}/latency`, { timeout: 8000 });
            const lat = res.data?.latency_metrics || {};
            const lines = [
                `⏱️ Latency for ${callSid}`,
                `STT: ${lat.stt_ms ?? 'N/A'} ms`,
                `GPT: ${lat.gpt_ms ?? 'N/A'} ms`,
                `TTS: ${lat.tts_ms ?? 'N/A'} ms`,
                `Duration: ${res.data?.call_duration ?? 'N/A'}s`
            ];
            await ctx.reply(lines.join('\n'));
        } catch (error) {
            console.error('Latency command error:', error?.message || error);
            await ctx.reply('❌ Failed to fetch latency.');
        }
    });

    // Version info
    bot.command('version', async (ctx) => {
        try {
            const res = await axios.get(`${config.apiUrl}/api/version`, { timeout: 6000 });
            const v = res.data;
            const message = [
                `🧭 Version: ${v.version || 'unknown'}`,
                `📦 Service: ${v.name || 'api'}`,
                `📡 Provider: ${v.provider || 'n/a'}`,
                `⏰ ${new Date(v.timestamp).toLocaleString()}`
            ].join('\n');
            await ctx.reply(message);
        } catch (error) {
            console.error('Version command error:', error?.message || error);
            await ctx.reply('❌ Failed to fetch version.');
        }
    });

    // Daily digest (lightweight)
    bot.command('digest', async (ctx) => {
        try {
            const user = await new Promise(r => getUser(ctx.from.id, r));
            if (!user) return ctx.reply('❌ You are not authorized.');

            const res = await axios.get(`${config.apiUrl}/api/analytics/notifications`, {
                params: { hours: 24, limit: 50 },
                timeout: 12000
            });
            const summary = res.data?.summary || {};
            const callsRes = await axios.get(`${config.apiUrl}/api/calls/recent`, {
                params: { limit: 10 },
                timeout: 8000
            });
            const calls = callsRes.data?.calls || [];

            const lines = [
                `📊 24h Digest`,
                `Notifications: ${summary.total_notifications ?? 0} (✅ ${summary.successful_notifications ?? 0}, ❌ ${(summary.total_notifications || 0) - (summary.successful_notifications || 0)})`,
                `Success rate: ${summary.success_rate_percent ?? 0}%`,
                `Avg delivery: ${summary.average_delivery_time_seconds ?? 'N/A'}s`,
                '',
                `Recent calls (${calls.length}):`
            ];

            calls.slice(0, 5).forEach((c) => {
                const status = c.status || 'unknown';
                const when = new Date(c.created_at).toLocaleTimeString();
                lines.push(`• ${c.call_sid} (${status}) ${when}`);
            });

            await ctx.reply(lines.join('\n'));
        } catch (error) {
            console.error('Digest command error:', error?.message || error);
            await ctx.reply('❌ Failed to fetch digest.');
        }
    });

    // Health check command (simple version for all users) - Enhanced
    bot.command(['health', 'ping'], async (ctx) => {
        try {
            const user = await new Promise(r => getUser(ctx.from.id, r));
            if (!user) {
                return ctx.reply('❌ You are not authorized to use this bot.');
            }

            const startTime = Date.now();
            
            try {
                const response = await axios.get(`${config.apiUrl}/health`, {
                    timeout: 8000,
                    headers: {
                        'Accept': 'application/json',
                        'Content-Type': 'application/json'
                    }
                });
                const responseTime = Date.now() - startTime;
                
                const health = response.data;
                
                let message = `🏥 *Health Check*\n\n`;
                message += `🤖 Bot: ✅ Responsive\n`;
                message += `🌐 API: ${health.status === 'healthy' ? '✅' : '⚠️'} ${health.status || 'responding'}\n`;
                message += `⚡ Response Time: ${responseTime}ms\n`;
                
                // Add basic stats if available
                if (health.active_calls !== undefined) {
                    message += `📞 Active Calls: ${health.active_calls}\n`;
                }
                
                // Add database status if available
                if (health.services?.database?.connected !== undefined) {
                    message += `🗄️ Database: ${health.services.database.connected ? '✅' : '❌'} ${health.services.database.connected ? 'Connected' : 'Disconnected'}\n`;
                }
                
                message += `⏰ Checked: ${new Date().toLocaleTimeString()}`;
                
                await ctx.reply(message, { parse_mode: 'Markdown' });
            } catch (apiError) {
                const responseTime = Date.now() - startTime;
                
                let message = `🏥 *Health Check*\n\n`;
                message += `🤖 Bot: ✅ Responsive\n`;
                message += `🌐 API: ❌ Connection failed\n`;
                message += `⚡ Response Time: ${responseTime}ms (timeout)\n`;
                message += `⏰ Checked: ${new Date().toLocaleTimeString()}\n\n`;
                
                if (apiError.code === 'ECONNREFUSED') {
                    message += `📝 API server appears to be down`;
                } else if (apiError.code === 'ETIMEDOUT') {
                    message += `📝 API server is not responding (timeout)`;
                } else {
                    message += `📝 ${apiError.message}`;
                }
                
                await ctx.reply(message, { parse_mode: 'Markdown' });
            }
        } catch (error) {
            console.error('Health command error:', error);
            await ctx.reply(`🏥 *Health Check*\n\n🤖 Bot: ✅ Responsive\n🌐 API: ❌ Error\n⏰ Checked: ${new Date().toLocaleTimeString()}\n\n📝 ${error.message}`, { parse_mode: 'Markdown' });
        }
    });
};
