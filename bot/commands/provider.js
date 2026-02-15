const config = require('../config');
const httpClient = require('../utils/httpClient');
const { InlineKeyboard } = require('grammy');
const { getUser, isAdmin } = require('../db/db');
const { buildLine, section, escapeMarkdown, renderMenu } = require('../utils/ui');
const { buildCallbackData } = require('../utils/actions');

const ADMIN_HEADER_NAME = 'x-admin-token';
const PROVIDER_CHANNELS = Object.freeze({
    CALL: 'call',
    SMS: 'sms',
    EMAIL: 'email'
});
const CHANNEL_ORDER = [
    PROVIDER_CHANNELS.CALL,
    PROVIDER_CHANNELS.SMS,
    PROVIDER_CHANNELS.EMAIL
];
const CHANNEL_META = Object.freeze({
    [PROVIDER_CHANNELS.CALL]: {
        label: 'Call',
        emoji: '☎️',
        envKey: 'CALL_PROVIDER',
        fallbackProviders: ['twilio', 'aws', 'vonage']
    },
    [PROVIDER_CHANNELS.SMS]: {
        label: 'SMS',
        emoji: '💬',
        envKey: 'SMS_PROVIDER',
        fallbackProviders: ['twilio', 'aws', 'vonage']
    },
    [PROVIDER_CHANNELS.EMAIL]: {
        label: 'Email',
        emoji: '📧',
        envKey: 'EMAIL_PROVIDER',
        fallbackProviders: ['sendgrid', 'mailgun', 'ses']
    }
});
const STATUS_CACHE_TTL_MS = 8000;
const statusCache = {
    [PROVIDER_CHANNELS.CALL]: { value: null, fetchedAt: 0 },
    [PROVIDER_CHANNELS.SMS]: { value: null, fetchedAt: 0 },
    [PROVIDER_CHANNELS.EMAIL]: { value: null, fetchedAt: 0 }
};

function normalizeProviderChannel(value, { allowNull = false } = {}) {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (CHANNEL_META[normalized]) {
        return normalized;
    }
    return allowNull ? null : PROVIDER_CHANNELS.CALL;
}

function resetProviderStatusCache() {
    CHANNEL_ORDER.forEach((channel) => {
        statusCache[channel] = { value: null, fetchedAt: 0 };
    });
}

function normalizeProviders(status = {}, channel = PROVIDER_CHANNELS.CALL) {
    const fallbackProviders = CHANNEL_META[channel]?.fallbackProviders || CHANNEL_META[PROVIDER_CHANNELS.CALL].fallbackProviders;
    const supportedValues = Array.isArray(status.supported_providers) && status.supported_providers.length > 0
        ? status.supported_providers
        : fallbackProviders;
    const supported = Array.from(new Set(supportedValues.map((item) => String(item).toLowerCase()))).filter(Boolean);
    const active = typeof status.provider === 'string' ? status.provider.toLowerCase() : '';
    const stored = typeof status.stored_provider === 'string' && status.stored_provider.trim()
        ? status.stored_provider.toLowerCase()
        : active;
    const readiness = status.readiness && typeof status.readiness === 'object' ? status.readiness : {};
    return { supported, active, stored, readiness };
}

function extractChannelState(status = {}, channel = PROVIDER_CHANNELS.CALL) {
    const providersBlock = status?.providers;
    if (providersBlock && typeof providersBlock === 'object' && providersBlock[channel]) {
        return providersBlock[channel];
    }

    if (status?.channel === channel) {
        return {
            provider: status.provider,
            stored_provider: status.stored_provider,
            supported_providers: status.supported_providers,
            readiness: status.readiness || {}
        };
    }

    if (channel === PROVIDER_CHANNELS.SMS) {
        return {
            provider: status.sms_provider,
            stored_provider: status.sms_stored_provider,
            supported_providers: status.sms_supported_providers,
            readiness: status.sms_readiness || {}
        };
    }

    if (channel === PROVIDER_CHANNELS.EMAIL) {
        return {
            provider: status.email_provider,
            stored_provider: status.email_stored_provider,
            supported_providers: status.email_supported_providers,
            readiness: status.email_readiness || {}
        };
    }

    return {
        provider: status.provider,
        stored_provider: status.stored_provider,
        supported_providers: status.supported_providers,
        readiness: {
            twilio: Boolean(status.twilio_ready),
            aws: Boolean(status.aws_ready),
            vonage: Boolean(status.vonage_ready)
        }
    };
}

function formatReadinessSummary(supported, readiness = {}) {
    if (!Array.isArray(supported) || !supported.length) {
        return 'N/A';
    }
    return supported
        .map((provider) => `${provider.toUpperCase()}:${readiness[provider] ? '✅' : '⚠️'}`)
        .join(' • ');
}

function formatProviderHubStatus(status = {}) {
    const lines = CHANNEL_ORDER.map((channel) => {
        const state = normalizeProviders(extractChannelState(status, channel), channel);
        const label = CHANNEL_META[channel].label;
        const emoji = CHANNEL_META[channel].emoji;
        const active = state.active ? state.active.toUpperCase() : 'UNKNOWN';
        const readiness = formatReadinessSummary(state.supported, state.readiness);
        return `${emoji} *${label}:* ${active}\n• Ready: ${readiness}`;
    });

    return section('⚙️ Provider Center', [
        ...lines,
        '',
        'Choose a feature to configure providers.'
    ]);
}

function formatProviderStatus(status, channel = PROVIDER_CHANNELS.CALL) {
    const channelMeta = CHANNEL_META[channel] || CHANNEL_META[PROVIDER_CHANNELS.CALL];
    if (!status) {
        return section(`⚙️ ${channelMeta.label} Provider Settings`, ['No status data available.']);
    }

    const state = normalizeProviders(extractChannelState(status, channel), channel);
    const current = state.active || 'unknown';
    const stored = state.stored || current;

    const details = [
        buildLine('•', channelMeta.envKey, `*${current.toUpperCase()}*`),
        buildLine('•', 'Stored Default', stored.toUpperCase()),
        buildLine('•', 'Supported', state.supported.map((item) => item.toUpperCase()).join(', ') || 'N/A'),
        buildLine('•', 'Ready', formatReadinessSummary(state.supported, state.readiness))
    ];

    if (channel === PROVIDER_CHANNELS.CALL) {
        details.push(
            buildLine('•', 'Vonage DTMF Webhook', status.vonage_dtmf_ready ? '✅ Enabled' : '⚠️ Disabled'),
            buildLine('•', 'Keypad Guard', status.keypad_guard_enabled ? '✅ Enabled' : '⚠️ Disabled')
        );
    }

    return section(`⚙️ ${channelMeta.label} Provider Settings`, details);
}

function buildProviderHubKeyboard(ctx) {
    const keyboard = new InlineKeyboard()
        .text('☎️ Call', buildCallbackData(ctx, `PROVIDER_CHANNEL:${PROVIDER_CHANNELS.CALL}`))
        .text('💬 SMS', buildCallbackData(ctx, `PROVIDER_CHANNEL:${PROVIDER_CHANNELS.SMS}`))
        .row()
        .text('📧 Email', buildCallbackData(ctx, `PROVIDER_CHANNEL:${PROVIDER_CHANNELS.EMAIL}`))
        .row()
        .text('🔄 Refresh', buildCallbackData(ctx, 'PROVIDER_STATUS'))
        .row()
        .text('⬅️ Back', buildCallbackData(ctx, 'MENU'))
        .text('🚪 Exit', buildCallbackData(ctx, 'MENU_EXIT'));
    return keyboard;
}

function buildProviderKeyboard(ctx, channel, activeProvider = '', supportedProviders = []) {
    const keyboard = new InlineKeyboard();
    const fallbackProviders = CHANNEL_META[channel]?.fallbackProviders || CHANNEL_META[PROVIDER_CHANNELS.CALL].fallbackProviders;
    const providers = supportedProviders.length ? supportedProviders : fallbackProviders;
    providers.forEach((provider, index) => {
        const normalized = provider.toLowerCase();
        const isActive = normalized === activeProvider;
        const label = isActive ? `✅ ${normalized.toUpperCase()}` : normalized.toUpperCase();
        keyboard.text(label, buildCallbackData(ctx, `PROVIDER_SET:${channel}:${normalized}`));

        const shouldInsertRow = index % 2 === 1 && index < providers.length - 1;
        if (shouldInsertRow) {
            keyboard.row();
        }
    });
    keyboard
        .row()
        .text('🔄 Refresh', buildCallbackData(ctx, `PROVIDER_STATUS:${channel}`))
        .text('⬅️ Back', buildCallbackData(ctx, 'PROVIDER:HOME'))
        .row()
        .text('🚪 Exit', buildCallbackData(ctx, 'MENU_EXIT'));
    return keyboard;
}

async function fetchProviderStatus({ channel = PROVIDER_CHANNELS.CALL, force = false } = {}) {
    const resolvedChannel = normalizeProviderChannel(channel);
    const cacheEntry = statusCache[resolvedChannel];
    if (!force && cacheEntry?.value && Date.now() - cacheEntry.fetchedAt < STATUS_CACHE_TTL_MS) {
        return cacheEntry.value;
    }
    const response = await httpClient.get(null, `${config.apiUrl}/admin/provider`, {
        timeout: 10000,
        params: { channel: resolvedChannel },
        headers: {
            [ADMIN_HEADER_NAME]: config.admin.apiToken,
            'Content-Type': 'application/json',
        },
    });
    statusCache[resolvedChannel] = {
        value: response.data,
        fetchedAt: Date.now()
    };
    return response.data;
}

function formatProviderError(error, actionLabel) {
    const authMessage = httpClient.getUserMessage(error, null);
    if (authMessage && (error.response?.status === 401 || error.response?.status === 403)) {
        return `❌ Failed to ${actionLabel}: ${escapeMarkdown(authMessage)}`;
    }
    if (error.response) {
        const details = error.response.data?.details || error.response.data?.error || error.response.statusText;
        return `❌ Failed to ${actionLabel}: ${escapeMarkdown(details || 'Unknown error')}`;
    }
    if (error.request) {
        return '❌ No response from provider API. Please check the server.';
    }
    return `❌ Error: ${escapeMarkdown(error.message || 'Unknown error')}`;
}

async function updateProvider({ channel, provider }) {
    const resolvedChannel = normalizeProviderChannel(channel);
    const response = await httpClient.post(
        null,
        `${config.apiUrl}/admin/provider`,
        { provider, channel: resolvedChannel },
        {
            timeout: 15000,
            headers: {
                [ADMIN_HEADER_NAME]: config.admin.apiToken,
                'Content-Type': 'application/json',
            },
        }
    );
    resetProviderStatusCache();
    return response.data;
}

async function renderProviderHub(ctx, { status, notice, forceRefresh = false } = {}) {
    let resolvedStatus = status;
    let cachedNotice = null;
    if (!resolvedStatus) {
        try {
            resolvedStatus = await fetchProviderStatus({ channel: PROVIDER_CHANNELS.CALL, force: forceRefresh });
        } catch (error) {
            if (statusCache[PROVIDER_CHANNELS.CALL]?.value) {
                resolvedStatus = statusCache[PROVIDER_CHANNELS.CALL].value;
                cachedNotice = '⚠️ Showing cached provider status (API unavailable).';
            } else {
                throw error;
            }
        }
    }

    const keyboard = buildProviderHubKeyboard(ctx);
    let message = formatProviderHubStatus(resolvedStatus);
    const notices = [notice, cachedNotice].filter(Boolean);
    if (notices.length) {
        message = `${notices.join('\n')}\n\n${message}`;
    }
    await renderMenu(ctx, message, keyboard, { parseMode: 'Markdown' });
}

async function renderProviderChannelMenu(ctx, {
    channel = PROVIDER_CHANNELS.CALL,
    status,
    notice,
    forceRefresh = false
} = {}) {
    const resolvedChannel = normalizeProviderChannel(channel);
    let resolvedStatus = status;
    let cachedNotice = null;
    if (!resolvedStatus) {
        try {
            resolvedStatus = await fetchProviderStatus({ channel: resolvedChannel, force: forceRefresh });
        } catch (error) {
            if (statusCache[resolvedChannel]?.value) {
                resolvedStatus = statusCache[resolvedChannel].value;
                cachedNotice = '⚠️ Showing cached provider status (API unavailable).';
            } else {
                throw error;
            }
        }
    }

    const normalized = normalizeProviders(extractChannelState(resolvedStatus, resolvedChannel), resolvedChannel);
    const keyboard = buildProviderKeyboard(ctx, resolvedChannel, normalized.active, normalized.supported);

    let message = formatProviderStatus(resolvedStatus, resolvedChannel);
    const notices = [notice, cachedNotice].filter(Boolean);
    if (notices.length) {
        message = `${notices.join('\n')}\n\n${message}`;
    }
    message += '\n\nTap a provider below to switch.';
    await renderMenu(ctx, message, keyboard, { parseMode: 'Markdown' });
}

async function renderProviderMenu(ctx, { status, notice, forceRefresh = false, channel = null } = {}) {
    try {
        const { isAdminUser } = await ensureAuthorizedAdmin(ctx);
        if (!isAdminUser) {
            return;
        }

        const normalizedChannel = normalizeProviderChannel(channel, { allowNull: true });
        if (normalizedChannel) {
            await renderProviderChannelMenu(ctx, {
                channel: normalizedChannel,
                status,
                notice,
                forceRefresh
            });
            return;
        }

        await renderProviderHub(ctx, {
            status,
            notice,
            forceRefresh
        });
    } catch (error) {
        console.error('Provider status command error:', error);
        await ctx.reply(formatProviderError(error, 'fetch provider status'));
    }
}

async function ensureAuthorizedAdmin(ctx) {
    const fromId = ctx.from?.id;
    if (!fromId) {
        await ctx.reply('❌ Missing sender information.');
        return { user: null, isAdminUser: false };
    }

    const user = await new Promise((resolve) => getUser(fromId, resolve));
    if (!user) {
        await ctx.reply('❌ You are not authorized to use this bot.');
        return { user: null, isAdminUser: false };
    }

    const admin = await new Promise((resolve) => isAdmin(fromId, resolve));
    if (!admin) {
        await ctx.reply('❌ This command is for administrators only.');
        return { user, isAdminUser: false };
    }

    return { user, isAdminUser: true };
}

async function handleProviderSwitch(ctx, requestedProvider, requestedChannel = PROVIDER_CHANNELS.CALL) {
    try {
        const channel = normalizeProviderChannel(requestedChannel);
        const normalized = String(requestedProvider || '').toLowerCase();
        if (!normalized) {
            await renderProviderMenu(ctx, { forceRefresh: true, channel });
            return;
        }

        const { isAdminUser } = await ensureAuthorizedAdmin(ctx);
        if (!isAdminUser) {
            return;
        }

        const status = await fetchProviderStatus({ channel }).catch(() => null);
        const currentState = normalizeProviders(extractChannelState(status || {}, channel), channel);
        if (!normalized || !currentState.supported.includes(normalized)) {
            const usage = [
                `• /provider ${channel}`,
                ...currentState.supported.map((item) => `• /provider ${channel} ${item}`)
            ].join('\n');
            await ctx.reply(
                `❌ Unsupported ${CHANNEL_META[channel].label} provider "${escapeMarkdown(requestedProvider || '')}".\n\nUsage:\n${usage}`
            );
            return;
        }

        if (currentState.active && currentState.active === normalized) {
            await renderProviderMenu(ctx, {
                status: status || null,
                channel,
                notice: `ℹ️ ${CHANNEL_META[channel].label} provider already set to *${normalized.toUpperCase()}*.`,
            });
            return;
        }

        const result = await updateProvider({ channel, provider: normalized });
        const refreshed = await fetchProviderStatus({ channel, force: true }).catch(() => result || status || null);
        const refreshedState = normalizeProviders(extractChannelState(refreshed || {}, channel), channel);
        const activeLabel = (refreshedState.active || result?.provider || normalized).toUpperCase();
        const notice = result.changed === false
            ? `ℹ️ ${CHANNEL_META[channel].label} provider already set to *${activeLabel}*.`
            : `✅ ${CHANNEL_META[channel].label} provider set to *${activeLabel}*.`;
        await renderProviderMenu(ctx, { status: refreshed, channel, notice });
    } catch (error) {
        console.error('Provider switch command error:', error);
        await ctx.reply(formatProviderError(error, 'update provider'));
    }
}

function registerProviderCommand(bot) {
    bot.command('provider', async (ctx) => {
        const text = ctx.message?.text || '';
        const args = text.split(/\s+/).slice(1).map((item) => item.toLowerCase());
        const requestedAction = args[0] || '';
        const requestedTarget = args[1] || '';

        const { isAdminUser } = await ensureAuthorizedAdmin(ctx);
        if (!isAdminUser) {
            return;
        }

        try {
            if (!requestedAction || requestedAction === 'status' || requestedAction === 'home') {
                await renderProviderMenu(ctx, { forceRefresh: true });
                return;
            }

            const requestedChannel = normalizeProviderChannel(requestedAction, { allowNull: true });
            if (requestedChannel) {
                if (!requestedTarget || requestedTarget === 'status' || requestedTarget === 'refresh') {
                    await renderProviderMenu(ctx, {
                        forceRefresh: true,
                        channel: requestedChannel
                    });
                    return;
                }
                await handleProviderSwitch(ctx, requestedTarget, requestedChannel);
                return;
            }

            // Backward-compatible shortcut: /provider twilio (defaults to call channel)
            await handleProviderSwitch(ctx, requestedAction, PROVIDER_CHANNELS.CALL);
        } catch (error) {
            console.error('Failed to manage provider via Telegram command:', error);
            await ctx.reply(formatProviderError(error, 'update provider'));
        }
    });
}

function initializeProviderCommand(bot) {
    registerProviderCommand(bot);
}

module.exports = initializeProviderCommand;
module.exports.registerProviderCommand = registerProviderCommand;
module.exports.fetchProviderStatus = fetchProviderStatus;
module.exports.updateProvider = updateProvider;
module.exports.renderProviderMenu = renderProviderMenu;
module.exports.handleProviderSwitch = handleProviderSwitch;
module.exports.normalizeProviderChannel = normalizeProviderChannel;
module.exports.PROVIDER_CHANNELS = PROVIDER_CHANNELS;
