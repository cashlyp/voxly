const config = require('../config');
const httpClient = require('../utils/httpClient');
const { InlineKeyboard } = require('grammy');
const { getUser, isAdmin } = require('../db/db');
const { buildLine, section, escapeMarkdown, renderMenu } = require('../utils/ui');
const { buildCallbackData } = require('../utils/actions');

const ADMIN_HEADER_NAME = 'x-admin-token';
const SUPPORTED_PROVIDERS = ['twilio', 'aws', 'vonage'];
const STATUS_CACHE_TTL_MS = 8000;
const statusCache = {
    value: null,
    fetchedAt: 0
};

function normalizeProviders(status = {}) {
    const supportedValues = Array.isArray(status.supported_providers) && status.supported_providers.length > 0
        ? status.supported_providers
        : SUPPORTED_PROVIDERS;
    const supported = Array.from(new Set(supportedValues.map((item) => String(item).toLowerCase()))).filter(Boolean);
    const active = typeof status.provider === 'string' ? status.provider.toLowerCase() : '';
    return { supported, active };
}

function buildCallReadinessMap(status = {}) {
    const callReadiness = status.providers?.call?.readiness || {};
    return {
        twilio: typeof callReadiness.twilio === 'boolean' ? callReadiness.twilio : !!status.twilio_ready,
        aws: typeof callReadiness.aws === 'boolean' ? callReadiness.aws : !!status.aws_ready,
        vonage: typeof callReadiness.vonage === 'boolean' ? callReadiness.vonage : !!status.vonage_ready
    };
}

function getProviderReadiness(status = {}, channel, provider) {
    const normalizedProvider = String(provider || '').toLowerCase();
    const state = status.providers?.[channel] || {};
    const readiness = state.readiness || {};
    if (typeof readiness[normalizedProvider] === 'boolean') {
        return readiness[normalizedProvider];
    }
    if (channel === 'call') {
        const callReadiness = buildCallReadinessMap(status);
        if (typeof callReadiness[normalizedProvider] === 'boolean') {
            return callReadiness[normalizedProvider];
        }
    }
    return null;
}

function readinessBadge(value) {
    if (value === true) return '✅';
    if (value === false) return '⚠️';
    return '—';
}

function formatReadinessMatrix(status = {}) {
    const callState = status.providers?.call || {};
    const smsState = status.providers?.sms || {};
    const emailState = status.providers?.email || {};
    const providers = Array.from(new Set([
        ...(callState.supported_providers || SUPPORTED_PROVIDERS),
        ...(smsState.supported_providers || []),
        ...(emailState.supported_providers || [])
    ])).map((item) => String(item || '').toLowerCase()).filter(Boolean);
    if (!providers.length) {
        return section('🧪 Readiness Matrix', ['No provider readiness data available.']);
    }
    const lines = providers.map((provider) => {
        const callReady = readinessBadge(getProviderReadiness(status, 'call', provider));
        const smsReady = readinessBadge(getProviderReadiness(status, 'sms', provider));
        const emailReady = readinessBadge(getProviderReadiness(status, 'email', provider));
        return `• ${provider.toUpperCase()} → call ${callReady} | sms ${smsReady} | email ${emailReady}`;
    });
    return section('🧪 Readiness Matrix', lines);
}

function formatProviderStatus(status) {
    if (!status) {
        return section('⚙️ Call Provider Settings', ['No status data available.']);
    }

    const current = typeof status.provider === 'string' ? status.provider : 'unknown';
    const stored = typeof status.stored_provider === 'string' && status.stored_provider.length > 0
        ? status.stored_provider
        : current;
    const supportedValues = Array.isArray(status.supported_providers) && status.supported_providers.length > 0
        ? status.supported_providers
        : SUPPORTED_PROVIDERS;
    const smsProvider = String(status.sms_provider || status.providers?.sms?.provider || 'unknown').toUpperCase();
    const emailProvider = String(status.email_provider || status.providers?.email?.provider || 'unknown').toUpperCase();

    const details = [
        buildLine('•', `Current Provider`, `*${current.toUpperCase()}*`),
        buildLine('•', `Stored Default`, stored.toUpperCase()),
        buildLine('•', `SMS Provider`, smsProvider),
        buildLine('•', `Email Provider`, emailProvider),
        buildLine('•', `Supported Backbones`, supportedValues.join(', ').toUpperCase())
    ];
    return [
        section('⚙️ Call Provider Settings', details),
        formatReadinessMatrix(status)
    ].join('\n\n');
}

function buildProviderKeyboard(ctx, activeProvider = '', supportedProviders = []) {
    const keyboard = new InlineKeyboard();
    const providers = supportedProviders.length ? supportedProviders : SUPPORTED_PROVIDERS;
    providers.forEach((provider, index) => {
        const normalized = provider.toLowerCase();
        const isActive = normalized === activeProvider;
        const label = isActive ? `✅ ${normalized.toUpperCase()}` : normalized.toUpperCase();
        keyboard.text(label, buildCallbackData(ctx, `PROVIDER_CONFIRM:${normalized}`));

        const shouldInsertRow = index % 2 === 1 && index < providers.length - 1;
        if (shouldInsertRow) {
            keyboard.row();
        }
    });
    keyboard.row().text('🔄 Refresh', buildCallbackData(ctx, 'PROVIDER_STATUS'));
    keyboard.row().text('⬅️ Main Menu', buildCallbackData(ctx, 'MENU'));
    return keyboard;
}

function buildProviderConfirmKeyboard(ctx, provider, canApply) {
    const keyboard = new InlineKeyboard();
    if (canApply) {
        keyboard.text('✅ Apply Switch', buildCallbackData(ctx, `PROVIDER_APPLY:${provider}`));
        keyboard.row();
    }
    keyboard.text('↩️ Choose Different', buildCallbackData(ctx, 'PROVIDER_STATUS'));
    keyboard.row().text('⬅️ Main Menu', buildCallbackData(ctx, 'MENU'));
    return keyboard;
}

async function fetchProviderStatus({ force = false } = {}) {
    if (!force && statusCache.value && Date.now() - statusCache.fetchedAt < STATUS_CACHE_TTL_MS) {
        return statusCache.value;
    }
    const response = await httpClient.get(null, `${config.apiUrl}/admin/provider`, {
        timeout: 10000,
        headers: {
            [ADMIN_HEADER_NAME]: config.admin.apiToken,
            'Content-Type': 'application/json',
        },
    });
    statusCache.value = response.data;
    statusCache.fetchedAt = Date.now();
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

async function updateProvider(provider) {
    const response = await httpClient.post(
        null,
        `${config.apiUrl}/admin/provider`,
        { provider },
        {
            timeout: 15000,
            headers: {
                [ADMIN_HEADER_NAME]: config.admin.apiToken,
                'Content-Type': 'application/json',
            },
        }
    );
    return response.data;
}

async function renderProviderMenu(ctx, { status, notice, forceRefresh = false } = {}) {
    try {
        let resolvedStatus = status;
        let cachedNotice = null;
        if (!resolvedStatus) {
            try {
                resolvedStatus = await fetchProviderStatus({ force: forceRefresh });
            } catch (error) {
                if (statusCache.value) {
                    resolvedStatus = statusCache.value;
                    cachedNotice = '⚠️ Showing cached provider status (API unavailable).';
                } else {
                    throw error;
                }
            }
        }
        const { supported, active } = normalizeProviders(resolvedStatus);
        const keyboard = buildProviderKeyboard(ctx, active, supported);
        let message = formatProviderStatus(resolvedStatus);
        const notices = [notice, cachedNotice].filter(Boolean);
        if (notices.length) {
            message = `${notices.join('\n')}\n\n${message}`;
        }
        message += '\n\nTap a provider below to review readiness before switching.';
        await renderMenu(ctx, message, keyboard, { parseMode: 'Markdown' });
    } catch (error) {
        console.error('Provider status command error:', error);
        await ctx.reply(formatProviderError(error, 'fetch provider status'));
    }
}

async function renderProviderConfirm(ctx, requestedProvider) {
    try {
        const status = await fetchProviderStatus({ force: true });
        const { supported, active } = normalizeProviders(status);
        const normalized = String(requestedProvider || '').toLowerCase();
        if (!normalized || !supported.includes(normalized)) {
            const options = supported.map((item) => `• /provider ${item}`).join('\n');
            await ctx.reply(
                `❌ Unsupported provider "${escapeMarkdown(requestedProvider || '')}".\n\nUsage:\n• /provider status\n${options}`
            );
            return;
        }

        const callReady = getProviderReadiness(status, 'call', normalized);
        const smsReady = getProviderReadiness(status, 'sms', normalized);
        const emailReady = getProviderReadiness(status, 'email', normalized);
        const canApply = callReady !== false;
        const lines = [
            buildLine('•', 'Current call provider', active.toUpperCase()),
            buildLine('•', 'Selected provider', normalized.toUpperCase()),
            buildLine('•', 'Call readiness', callReady === false ? '⚠️ Not ready' : (callReady === true ? '✅ Ready' : '— Unknown')),
            buildLine('•', 'SMS readiness', smsReady === false ? '⚠️ Not ready' : (smsReady === true ? '✅ Ready' : '— Unknown')),
            buildLine('•', 'Email readiness', emailReady === false ? '⚠️ Not ready' : (emailReady === true ? '✅ Ready' : '— Unknown')),
            '',
            canApply
                ? 'Apply switch when ready.'
                : 'Switch blocked: selected call provider is not ready.'
        ];
        const message = section('🛡️ Confirm Provider Switch', lines);
        const keyboard = buildProviderConfirmKeyboard(ctx, normalized, canApply);
        await renderMenu(ctx, message, keyboard, { parseMode: 'Markdown' });
    } catch (error) {
        console.error('Provider confirm command error:', error);
        await ctx.reply(formatProviderError(error, 'load provider confirmation'));
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
        await ctx.reply('❌ Access denied. Your account is not authorized for this action.');
        return { user: null, isAdminUser: false };
    }

    const admin = await new Promise((resolve) => isAdmin(fromId, resolve));
    if (!admin) {
        await ctx.reply('❌ Access denied. This action is available to administrators only.');
        return { user, isAdminUser: false };
    }

    return { user, isAdminUser: true };
}

async function handleProviderSwitch(ctx, requestedProvider) {
    try {
        const status = await fetchProviderStatus();
        const { supported } = normalizeProviders(status);
        const normalized = String(requestedProvider || '').toLowerCase();
        if (!normalized || !supported.includes(normalized)) {
            const options = supported.map((item) => `• /provider ${item}`).join('\n');
            await ctx.reply(
                `❌ Unsupported provider "${escapeMarkdown(requestedProvider || '')}".\n\nUsage:\n• /provider status\n${options}`
            );
            return;
        }

        const result = await updateProvider(normalized);
        const refreshed = await fetchProviderStatus({ force: true });
        const activeLabel = (refreshed.provider || normalized).toUpperCase();
        const notice = result.changed === false
            ? `ℹ️ Provider already set to *${activeLabel}*.`
            : `✅ Call provider set to *${activeLabel}*.`;
        await renderProviderMenu(ctx, { status: refreshed, notice });
    } catch (error) {
        console.error('Provider switch command error:', error);
        await ctx.reply(formatProviderError(error, 'update provider'));
    }
}

function registerProviderCommand(bot) {
    bot.command('provider', async (ctx) => {
        const text = ctx.message?.text || '';
        const args = text.split(/\s+/).slice(1);
        const requestedAction = (args[0] || '').toLowerCase();

        const { isAdminUser } = await ensureAuthorizedAdmin(ctx);
        if (!isAdminUser) {
            return;
        }

        try {
            if (!requestedAction || requestedAction === 'status') {
                await renderProviderMenu(ctx, { forceRefresh: true });
                return;
            }

            await handleProviderSwitch(ctx, requestedAction);
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
module.exports.formatProviderStatus = formatProviderStatus;
module.exports.handleProviderSwitch = handleProviderSwitch;
module.exports.renderProviderMenu = renderProviderMenu;
module.exports.renderProviderConfirm = renderProviderConfirm;
module.exports.buildProviderKeyboard = buildProviderKeyboard;
module.exports.SUPPORTED_PROVIDERS = SUPPORTED_PROVIDERS;
module.exports.ADMIN_HEADER_NAME = ADMIN_HEADER_NAME;
