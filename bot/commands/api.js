const config = require('../config');
const httpClient = require('../utils/httpClient');
const { getUser, isAdmin } = require('../db/db');
const { escapeHtml } = require('../utils/ui');
const { buildCallbackData } = require('../utils/actions');
const { getDeniedAuditSummary } = require('../utils/capabilities');

function buildMainMenuReplyMarkup(ctx) {
    return {
        inline_keyboard: [[{ text: '⬅️ Main Menu', callback_data: buildCallbackData(ctx, 'MENU') }]]
    };
}

function buildSpoiler(value = '') {
    return `<span class="tg-spoiler">${escapeHtml(String(value || 'unknown'))}</span>`;
}

function buildHtmlLine(icon, label, value) {
    const safeLabel = escapeHtml(String(label || ''));
    const safeValue = value === undefined || value === null ? '' : escapeHtml(String(value));
    return `${icon} <b>${safeLabel}:</b> ${safeValue}`;
}

function maskUrlsWithSpoilersHtml(text = '') {
    const source = String(text || '');
    const escaped = escapeHtml(source);
    return escaped.replace(/https?:\/\/[^\s<]+/gi, (match) => buildSpoiler(match));
}

async function replyApiError(ctx, error, fallback, options = {}) {
    const message = httpClient.getUserMessage(error, fallback);
    return ctx.reply(message, options);
}

async function handleStatusCommand(ctx) {
    try {
        const user = await new Promise(r => getUser(ctx.from.id, r));
        const adminStatus = await new Promise(r => isAdmin(ctx.from.id, r));

        if (!user || !adminStatus) {
            return ctx.reply('❌ This command is for administrators only.');
        }

        await ctx.reply('🔍 Checking system status...');

        const startTime = Date.now();
        const healthHeaders = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        };
        if (config.admin?.apiToken) {
            healthHeaders['x-admin-token'] = config.admin.apiToken;
        }
        const response = await httpClient.get(null, `${config.apiUrl}/health`, {
            timeout: 15000,
            headers: healthHeaders
        });
        const responseTime = Date.now() - startTime;

        const health = response.data;

        const apiHealthStatus = health.status || 'healthy';
        let message = `🔍 <b>System Status Report</b>\n\n`;
        message += `🤖 Bot: ✅ Online & Responsive\n`;
        message += `🌐 API: ${health.status === 'healthy' ? '✅' : '❌'} ${escapeHtml(String(apiHealthStatus))}\n`;
        message += `${buildHtmlLine('⚡', 'API Response Time', `${responseTime}ms`)}\n\n`;

        if (health.services) {
            message += `<b>🔧 Services Status:</b>\n`;

            const db = health.services.database;
            message += `${buildHtmlLine('🗄️', 'Database', db?.connected ? '✅ Connected' : '❌ Disconnected')}\n`;
            if (db?.recent_calls !== undefined) {
                message += `${buildHtmlLine('📋', 'Recent DB Calls', db.recent_calls)}\n`;
            }

            const webhook = health.services.webhook_service;
            if (webhook) {
                message += `${buildHtmlLine('📡', 'Webhook Service', `${webhook.status === 'running' ? '✅' : '⚠️'} ${webhook.status}`)}\n`;
                if (webhook.processed_today !== undefined) {
                    message += `${buildHtmlLine('📨', 'Webhooks Today', webhook.processed_today)}\n`;
                }
            }

            const notifications = health.services.notification_system;
            if (notifications) {
                message += `${buildHtmlLine('🔔', 'Notifications', `${String(notifications.success_rate || 'N/A')} success rate`)}\n`;
            }

            message += `\n`;
        }

        message += `<b>📊 Call Statistics:</b>\n`;
        message += `${buildHtmlLine('📞', 'Active Calls', health.active_calls || 0)}\n`;
        message += `✨ Keeping the console lively with ${health.active_calls || 0} active connections.\n`;

        const audit = getDeniedAuditSummary();
        if (audit.total > 0) {
            message += `${buildHtmlLine('🔒', `Access denials (${audit.windowSeconds}s)`, `${audit.total} across ${audit.users} user(s), ${audit.rateLimited} rate-limited`)}\n`;
            if (audit.recent && audit.recent.length > 0) {
                const recentLines = audit.recent.map((entry) => {
                    const suffix = entry.userId ? String(entry.userId).slice(-4) : 'unknown';
                    const who = `user#${suffix}`;
                    const actionLabel = escapeHtml(entry.actionLabel || entry.capability || 'action');
                    const role = escapeHtml(entry.role || 'unknown');
                    const when = entry.timestamp ? new Date(entry.timestamp).toLocaleTimeString() : 'recent';
                    return `• ${who} (${role}) blocked on ${actionLabel} at ${escapeHtml(when)}`;
                });
                message += `\n<b>🔐 Recent denials:</b>\n${recentLines.join('\n')}\n`;
            }
        }

        if (health.adaptation_engine) {
            message += `\n<b>🤖 AI Features:</b>\n`;
            message += `${buildHtmlLine('🧠', 'Adaptation Engine', '✅ Active')}\n`;
            message += `${buildHtmlLine('🧩', 'Function Scripts', health.adaptation_engine.available_scripts || 0)}\n`;
            message += `${buildHtmlLine('⚙️', 'Active Systems', health.adaptation_engine.active_function_systems || 0)}\n`;
        }

        if (health.inbound_defaults || health.inbound_env_defaults) {
            message += `\n<b>📥 Inbound Defaults:</b>\n`;
            const inbound = health.inbound_defaults || {};
            if (inbound.mode === 'script') {
                message += `${buildHtmlLine('📄', 'Default Script', `${inbound.name || 'Unnamed'} (${String(inbound.script_id || '')})`)}\n`;
            } else {
                message += `${buildHtmlLine('📄', 'Default Script', 'Built-in')}\n`;
            }
            const envDefaults = health.inbound_env_defaults || {};
            const envPrompt = envDefaults.prompt ? 'set' : 'unset';
            const envFirst = envDefaults.first_message ? 'set' : 'unset';
            message += `${buildHtmlLine('⚙️', 'Env Defaults', `prompt: ${envPrompt}, first_message: ${envFirst}`)}\n`;
        }

        if (health.enhanced_features) {
            message += `<b>🚀 Enhanced Mode:</b> ✅ Enabled\n`;
        }

        if (health.system_health && health.system_health.length > 0) {
            message += `\n<b>🔍 Recent Activity:</b>\n`;
            health.system_health.slice(0, 3).forEach(log => {
                const status = log.status === 'error' ? '❌' : '✅';
                message += `${status} ${escapeHtml(log.service_name)}: ${log.count} ${escapeHtml(log.status)}\n`;
            });
        }

        message += `\n${buildHtmlLine('⏰','Last Updated', new Date(health.timestamp).toLocaleString())}`;
        message += `\n📡 <b>API Endpoint:</b> ${buildSpoiler(config.apiUrl)}`;

        await ctx.reply(message, {
            parse_mode: 'HTML',
            reply_markup: buildMainMenuReplyMarkup(ctx)
        });
    } catch (error) {
        console.error('Status command error:', error);
        const message = `${maskUrlsWithSpoilersHtml(httpClient.getUserMessage(error, 'System status check failed.'))}\nAPI: ${buildSpoiler(config.apiUrl)}`;
        await ctx.reply(message, {
            parse_mode: 'HTML',
            reply_markup: buildMainMenuReplyMarkup(ctx)
        });
    }
}

async function handleHealthCommand(ctx) {
    try {
        const user = await new Promise(r => getUser(ctx.from.id, r));
        if (!user) {
            return ctx.reply('❌ You are not authorized to use this bot.');
        }

        const startTime = Date.now();

        try {
            const response = await httpClient.get(null, `${config.apiUrl}/health`, {
                timeout: 8000,
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json'
                }
            });
            const responseTime = Date.now() - startTime;

            const health = response.data;

            let message = `🏥 <b>Health Check</b>\n\n`;
            message += `🤖 Bot: ✅ Responsive\n`;
            message += `🌐 API: ${health.status === 'healthy' ? '✅' : '⚠️'} ${escapeHtml(health.status || 'responding')}\n`;
            message += `⚡ Response Time: ${responseTime}ms\n`;

            if (health.active_calls !== undefined) {
                message += `${buildHtmlLine('📞', 'Active Calls', health.active_calls)}\n`;
            }

            if (health.services?.database?.connected !== undefined) {
                message += `${buildHtmlLine('🗄️', 'Database', `${health.services.database.connected ? '✅' : '❌'} ${health.services.database.connected ? 'Connected' : 'Disconnected'}`)}\n`;
            }

            message += `${buildHtmlLine('⏰', 'Checked', new Date().toLocaleTimeString())}`;

            await ctx.reply(message, {
                parse_mode: 'HTML',
                reply_markup: buildMainMenuReplyMarkup(ctx)
            });
        } catch (apiError) {
            const message = `${maskUrlsWithSpoilersHtml(httpClient.getUserMessage(apiError, 'API unreachable.'))}\nAPI: ${buildSpoiler(config.apiUrl)}`;
            await ctx.reply(message, {
                parse_mode: 'HTML',
                reply_markup: buildMainMenuReplyMarkup(ctx)
            });
        }
    } catch (error) {
        console.error('Health command error:', error);
        await replyApiError(ctx, error, 'Health check failed.', {
            reply_markup: buildMainMenuReplyMarkup(ctx)
        });
    }
}

function registerApiCommands(bot) {
    bot.command('status', handleStatusCommand);
    bot.command(['health', 'ping'], handleHealthCommand);
}

module.exports = {
    registerApiCommands,
    handleStatusCommand,
    handleHealthCommand
};
