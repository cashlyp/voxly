const config = require('../config');
const httpClient = require('../utils/httpClient');
const { getUser, isAdmin } = require('../db/db');
const { escapeMarkdown, buildLine, sendEphemeral } = require('../utils/ui');
const { buildCallbackData } = require('../utils/actions');
const { getDeniedAuditSummary } = require('../utils/capabilities');

function buildMainMenuReplyMarkup(ctx) {
    return {
        inline_keyboard: [[{ text: '⬅️ Main Menu', callback_data: buildCallbackData(ctx, 'MENU') }]]
    };
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
            return ctx.reply('❌ Access denied. This action is available to administrators only.');
        }

        await sendEphemeral(ctx, '🔍 Checking system status...');

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
        let message = `🔍 *System Status Report*\n\n`;
        message += `🤖 Bot: ✅ Online & Responsive\n`;
        message += `🌐 API: ${health.status === 'healthy' ? '✅' : '❌'} ${escapeMarkdown(apiHealthStatus)}\n`;
        message += `${buildLine('⚡', 'API Response Time', `${responseTime}ms`)}\n\n`;

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

        message += `*📊 Call Statistics:*\n`;
        message += `${buildLine('📞', 'Active Calls', health.active_calls || 0)}\n`;
        message += `${buildLine('📈', 'Live Connections', health.active_calls || 0)}\n`;

        const audit = getDeniedAuditSummary();
        if (audit.total > 0) {
            message += `${buildLine('🔒', `Access denials (${audit.windowSeconds}s)`, `${audit.total} across ${audit.users} user(s), ${audit.rateLimited} rate-limited`)}\n`;
            if (audit.recent && audit.recent.length > 0) {
                const recentLines = audit.recent.map((entry) => {
                    const suffix = entry.userId ? String(entry.userId).slice(-4) : 'unknown';
                    const who = `user#${suffix}`;
                    const actionLabel = escapeMarkdown(entry.actionLabel || entry.capability || 'action');
                    const role = escapeMarkdown(entry.role || 'unknown');
                    const when = entry.timestamp ? new Date(entry.timestamp).toLocaleTimeString() : 'recent';
                    return `• ${who} (${role}) blocked on ${actionLabel} at ${escapeMarkdown(when)}`;
                });
                message += `\n*🔐 Recent denials:*\n${recentLines.join('\n')}\n`;
            }
        }

        if (health.adaptation_engine) {
            message += `\n*🤖 AI Features:*\n`;
            message += `${buildLine('🧠', 'Adaptation Engine', '✅ Active')}\n`;
            message += `${buildLine('🧩', 'Function Scripts', health.adaptation_engine.available_scripts || 0)}\n`;
            message += `${buildLine('⚙️', 'Active Systems', health.adaptation_engine.active_function_systems || 0)}\n`;
        }

        if (health.inbound_defaults || health.inbound_env_defaults) {
            message += `\n*📥 Inbound Defaults:*\n`;
            const inbound = health.inbound_defaults || {};
            if (inbound.mode === 'script') {
                message += `${buildLine('📄', 'Default Script', `${escapeMarkdown(inbound.name || 'Unnamed')} (${escapeMarkdown(String(inbound.script_id || ''))})`)}\n`;
            } else {
                message += `${buildLine('📄', 'Default Script', 'Built-in')}\n`;
            }
            const envDefaults = health.inbound_env_defaults || {};
            const envPrompt = envDefaults.prompt ? 'set' : 'unset';
            const envFirst = envDefaults.first_message ? 'set' : 'unset';
            message += `${buildLine('⚙️', 'Env Defaults', `prompt: ${envPrompt}, first_message: ${envFirst}`)}\n`;
        }

        if (health.enhanced_features) {
            message += `${buildLine('🚀', 'Enhanced Mode', '✅ Enabled')}\n`;
        }

        if (health.system_health && health.system_health.length > 0) {
            message += `\n*🔍 Recent Activity:*\n`;
            health.system_health.slice(0, 3).forEach(log => {
                const status = log.status === 'error' ? '❌' : '✅';
                message += `${status} ${escapeMarkdown(log.service_name)}: ${log.count} ${escapeMarkdown(log.status)}\n`;
            });
        }

        message += `\n${buildLine('⏰','Last Updated', escapeMarkdown(new Date(health.timestamp).toLocaleString()))}`;
        message += `\n${buildLine('📡','API Endpoint', escapeMarkdown(config.apiUrl))}`;

        await ctx.reply(message, {
            parse_mode: 'Markdown',
            reply_markup: buildMainMenuReplyMarkup(ctx)
        });
    } catch (error) {
        console.error('Status command error:', error);
        const message = `${httpClient.getUserMessage(error, 'System status check failed.')}\nAPI: ${config.apiUrl}`;
        await ctx.reply(message, {
            reply_markup: buildMainMenuReplyMarkup(ctx)
        });
    }
}

async function handleHealthCommand(ctx) {
    try {
        const user = await new Promise(r => getUser(ctx.from.id, r));
        if (!user) {
            return ctx.reply('❌ Access denied. Your account is not authorized for this action.');
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

            let message = `🏥 *Health Check*\n\n`;
            message += `🤖 Bot: ✅ Responsive\n`;
            message += `🌐 API: ${health.status === 'healthy' ? '✅' : '⚠️'} ${health.status || 'responding'}\n`;
            message += `⚡ Response Time: ${responseTime}ms\n`;

            if (health.active_calls !== undefined) {
                message += `📞 Active Calls: ${health.active_calls}\n`;
            }

            if (health.services?.database?.connected !== undefined) {
                message += `🗄️ Database: ${health.services.database.connected ? '✅' : '❌'} ${health.services.database.connected ? 'Connected' : 'Disconnected'}\n`;
            }

            message += `⏰ Checked: ${new Date().toLocaleTimeString()}`;

            await ctx.reply(message, {
                parse_mode: 'Markdown',
                reply_markup: buildMainMenuReplyMarkup(ctx)
            });
        } catch (apiError) {
            const message = `${httpClient.getUserMessage(apiError, 'API unreachable.')}\nAPI: ${config.apiUrl}`;
            await ctx.reply(message, {
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
