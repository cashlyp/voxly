const config = require('../config');
const httpClient = require('../utils/httpClient');
const { InlineKeyboard } = require('grammy');
const { getUser, isAdmin } = require('../db/db');
const { buildLine, section, escapeMarkdown, renderMenu } = require('../utils/ui');
const { buildCallbackData } = require('../utils/actions');

const ADMIN_HEADER_NAME = 'x-admin-token';
const CHANNELS = Object.freeze({
    CALL: 'call',
    SMS: 'sms',
    EMAIL: 'email',
});
const CHANNEL_ORDER = [CHANNELS.CALL, CHANNELS.SMS, CHANNELS.EMAIL];
const CHANNEL_ACTIONS = Object.freeze({
    [CHANNELS.CALL]: 'CALL',
    [CHANNELS.SMS]: 'SMS',
    [CHANNELS.EMAIL]: 'EMAIL',
});
const ACTION_TO_CHANNEL = Object.freeze({
    CALL: CHANNELS.CALL,
    SMS: CHANNELS.SMS,
    EMAIL: CHANNELS.EMAIL,
});
const CHANNEL_TITLES = Object.freeze({
    [CHANNELS.CALL]: '☎️ Call Providers',
    [CHANNELS.SMS]: '💬 SMS Providers',
    [CHANNELS.EMAIL]: '📧 Email Providers',
});
const DEFAULT_SUPPORTED_PROVIDERS = Object.freeze({
    [CHANNELS.CALL]: ['twilio', 'aws', 'vonage'],
    [CHANNELS.SMS]: ['twilio', 'aws', 'vonage'],
    [CHANNELS.EMAIL]: ['sendgrid', 'mailgun', 'ses'],
});
const PROVIDER_ACTIONS = Object.freeze({
    HOME: 'PROVIDER:HOME',
    CALL: 'PROVIDER:CALL',
    SMS: 'PROVIDER:SMS',
    EMAIL: 'PROVIDER:EMAIL',
    BACK_PREFIX: 'PROVIDER:BACK:',
    BACK_HOME: 'PROVIDER:BACK:HOME',
    STATUS_PREFIX: 'PROVIDER_STATUS:',
    SET_PREFIX: 'PROVIDER_SET:',

    // Legacy callback compatibility for older inline menus still in chat history.
    LEGACY_STATUS: 'PROVIDER_STATUS',
    LEGACY_STATUS_CHANNEL_PREFIX: 'PROVIDER_STATUS_CH:',
    LEGACY_SET_PREFIX: 'PROVIDER_SET:',
    LEGACY_SET_CHANNEL_PREFIX: 'PROVIDER_SET_CH:',
    LEGACY_OVERRIDES: 'PROVIDER_OVERRIDES',
    LEGACY_CLEAR_OVERRIDES_PREFIX: 'PROVIDER_CLEAR_OVERRIDES:',
});
const STATUS_CACHE_TTL_MS = 8000;
const statusCache = { value: null, fetchedAt: 0 };

function normalizeChannel(channel) {
    const normalized = String(channel || CHANNELS.CALL).toLowerCase().trim();
    return CHANNEL_ORDER.includes(normalized) ? normalized : CHANNELS.CALL;
}

function channelToActionSegment(channel) {
    const normalizedChannel = normalizeChannel(channel);
    return CHANNEL_ACTIONS[normalizedChannel];
}

function actionSegmentToChannel(segment) {
    return ACTION_TO_CHANNEL[String(segment || '').toUpperCase()] || null;
}

function normalizeProviderName(value) {
    return String(value || '').trim().toLowerCase();
}

function maskUserId(userId) {
    if (userId === undefined || userId === null) return 'unknown';
    const text = String(userId);
    if (text.length <= 4) return text;
    return `***${text.slice(-4)}`;
}

function redactLogText(input = '') {
    let text = String(input || '');
    if (!text) return '';
    text = text.replace(/\+?\d[\d\s().-]{6,}\d/g, '[redacted-phone]');
    text = text.replace(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi, '[redacted-email]');
    if (text.length > 180) {
        return `${text.slice(0, 180)}…`;
    }
    return text;
}

function summarizeError(error) {
    if (!error) return 'unknown_error';
    if (error.response) {
        const detail = error.response?.data?.error || error.response?.data?.message || error.response?.statusText || 'http_error';
        return `http_${error.response.status}:${redactLogText(detail)}`;
    }
    if (error.request) return 'upstream_no_response';
    return redactLogText(error.message || 'unknown_error');
}

function logProviderEvent(ctx, { action, channel, provider, result, error }) {
    const payload = {
        type: 'provider_action',
        user_id: maskUserId(ctx?.from?.id),
        action: action || 'unknown',
        channel: channel ? normalizeChannel(channel) : null,
        provider: provider ? normalizeProviderName(provider) : null,
        result: result || 'unknown',
        error: error ? summarizeError(error) : null,
    };
    console.log(JSON.stringify(payload));
}

function getChannelState(status = {}, channel = CHANNELS.CALL) {
    const normalizedChannel = normalizeChannel(channel);
    const providers = status.providers || {};
    if (providers && providers[normalizedChannel]) {
        return providers[normalizedChannel];
    }
    if (normalizedChannel === CHANNELS.CALL) {
        return status;
    }
    if (normalizedChannel === CHANNELS.SMS) {
        return {
            provider: status.sms_provider,
            stored_provider: status.sms_stored_provider,
            supported_providers: status.sms_supported_providers,
            readiness: status.sms_readiness,
        };
    }
    return {
        provider: status.email_provider,
        stored_provider: status.email_stored_provider,
        supported_providers: status.email_supported_providers,
        readiness: status.email_readiness,
    };
}

function normalizeSupportedProviders(status = {}, channel = CHANNELS.CALL) {
    const normalizedChannel = normalizeChannel(channel);
    const channelState = getChannelState(status, normalizedChannel);
    const catalog = DEFAULT_SUPPORTED_PROVIDERS[normalizedChannel] || [];
    const rawApiSupported = Array.isArray(channelState.supported_providers)
        ? channelState.supported_providers.map((item) => normalizeProviderName(item)).filter(Boolean)
        : [];
    const apiSupported = Array.from(new Set(rawApiSupported));
    let supported = apiSupported.length ? apiSupported.filter((item) => catalog.includes(item)) : [...catalog];
    if (!supported.length && catalog.length) {
        supported = [...catalog];
    }
    const active = normalizeProviderName(channelState.provider || '');
    return {
        channelState,
        supported,
        active,
    };
}

function buildProviderDashboard(status = {}) {
    const callState = getChannelState(status, CHANNELS.CALL);
    const smsState = getChannelState(status, CHANNELS.SMS);
    const emailState = getChannelState(status, CHANNELS.EMAIL);
    return section('⚙️ Provider Dashboard', [
        buildLine('•', '☎️ Call', `*${escapeMarkdown(String(callState.provider || 'unknown').toUpperCase())}*`),
        buildLine('•', '💬 SMS', `*${escapeMarkdown(String(smsState.provider || 'unknown').toUpperCase())}*`),
        buildLine('•', '📧 Email', `*${escapeMarkdown(String(emailState.provider || 'unknown').toUpperCase())}*`),
    ]);
}

function buildProviderDashboardKeyboard(ctx) {
    return new InlineKeyboard()
        .text('☎️ Call Providers', buildCallbackData(ctx, PROVIDER_ACTIONS.CALL))
        .text('💬 SMS Providers', buildCallbackData(ctx, PROVIDER_ACTIONS.SMS))
        .text('📧 Email Providers', buildCallbackData(ctx, PROVIDER_ACTIONS.EMAIL));
}

function buildReadinessLines(status = {}, channel = CHANNELS.CALL) {
    const normalizedChannel = normalizeChannel(channel);
    const state = getChannelState(status, normalizedChannel);
    const readiness = state.readiness || {};
    const entries = Object.entries(readiness);
    if (!entries.length && normalizedChannel !== CHANNELS.CALL) {
        return ['• Readiness: unknown'];
    }
    if (normalizedChannel === CHANNELS.CALL) {
        return [
            buildLine('•', 'TWILIO Ready', status.twilio_ready ? '✅' : '⚠️'),
            buildLine('•', 'AWS Ready', status.aws_ready ? '✅' : '⚠️'),
            buildLine('•', 'VONAGE Ready', status.vonage_ready ? '✅' : '⚠️'),
        ];
    }
    return entries.map(([provider, ready]) =>
        buildLine('•', `${provider.toUpperCase()} Ready`, ready ? '✅' : '⚠️')
    );
}

function buildProviderSubmenuText(status = {}, channel = CHANNELS.CALL) {
    const normalizedChannel = normalizeChannel(channel);
    const { channelState, supported, active } = normalizeSupportedProviders(status, normalizedChannel);
    const title = CHANNEL_TITLES[normalizedChannel] || 'Provider Menu';
    const storedProvider = String(channelState.stored_provider || channelState.provider || 'unknown').toUpperCase();
    const supportedText = supported.length ? supported.map((item) => item.toUpperCase()).join(', ') : '—';
    const lines = [
        buildLine('•', 'Active', `*${escapeMarkdown(String(active || 'unknown').toUpperCase())}*`),
        buildLine('•', 'Stored Default', escapeMarkdown(storedProvider)),
        buildLine('•', 'Supported', escapeMarkdown(supportedText)),
        ...buildReadinessLines(status, normalizedChannel),
    ];
    return `${section(title, lines)}\n\nTap a provider to set it as default.`;
}

function buildProviderSubmenuKeyboard(ctx, channel, supportedProviders = [], activeProvider = '') {
    const normalizedChannel = normalizeChannel(channel);
    const actionSegment = channelToActionSegment(normalizedChannel);
    const providers = supportedProviders.length
        ? supportedProviders
        : DEFAULT_SUPPORTED_PROVIDERS[normalizedChannel] || [];
    const keyboard = new InlineKeyboard();
    providers.forEach((provider, index) => {
        const normalizedProvider = normalizeProviderName(provider);
        const isActive = normalizedProvider === activeProvider;
        const label = isActive
            ? `✅ ${normalizedProvider.toUpperCase()}`
            : `Set ${normalizedProvider.toUpperCase()}`;
        keyboard.text(
            label,
            buildCallbackData(ctx, `${PROVIDER_ACTIONS.SET_PREFIX}${actionSegment}:${normalizedProvider}`),
        );
        const insertRow = index % 2 === 1 && index < providers.length - 1;
        if (insertRow) {
            keyboard.row();
        }
    });
    keyboard.row()
        .text('📊 Status', buildCallbackData(ctx, `${PROVIDER_ACTIONS.STATUS_PREFIX}${actionSegment}`))
        .text('⬅️ Back', buildCallbackData(ctx, PROVIDER_ACTIONS.BACK_HOME));
    return keyboard;
}

async function fetchProviderStatus({ force = false, channel = null } = {}) {
    const now = Date.now();
    if (!force && statusCache.value && now - statusCache.fetchedAt < STATUS_CACHE_TTL_MS) {
        return statusCache.value;
    }
    const normalizedChannel = channel ? normalizeChannel(channel) : null;
    const response = await httpClient.get(null, `${config.apiUrl}/admin/provider`, {
        timeout: 10000,
        params: normalizedChannel ? { channel: normalizedChannel } : undefined,
        headers: {
            [ADMIN_HEADER_NAME]: config.admin.apiToken,
            'Content-Type': 'application/json',
        },
    });
    statusCache.value = response.data;
    statusCache.fetchedAt = Date.now();
    return response.data;
}

async function updateProvider(provider, channel = CHANNELS.CALL) {
    const normalizedChannel = normalizeChannel(channel);
    const normalizedProvider = normalizeProviderName(provider);
    const response = await httpClient.post(
        null,
        `${config.apiUrl}/admin/provider`,
        { provider: normalizedProvider, channel: normalizedChannel },
        {
            timeout: 15000,
            headers: {
                [ADMIN_HEADER_NAME]: config.admin.apiToken,
                'Content-Type': 'application/json',
            },
        },
    );
    statusCache.value = null;
    statusCache.fetchedAt = 0;
    return response.data;
}

async function fetchKeypadOverrides() {
    const response = await httpClient.get(
        null,
        `${config.apiUrl}/admin/provider/keypad-overrides`,
        {
            timeout: 10000,
            headers: {
                [ADMIN_HEADER_NAME]: config.admin.apiToken,
                'Content-Type': 'application/json',
            },
        },
    );
    return response.data;
}

async function clearKeypadOverrides(params = {}) {
    const response = await httpClient.post(
        null,
        `${config.apiUrl}/admin/provider/keypad-overrides/clear`,
        params,
        {
            timeout: 15000,
            headers: {
                [ADMIN_HEADER_NAME]: config.admin.apiToken,
                'Content-Type': 'application/json',
            },
        },
    );
    return response.data;
}

function formatKeypadOverridesResult(payload = {}) {
    const overrides = Array.isArray(payload.overrides) ? payload.overrides : [];
    const lines = [
        '🔐 *Keypad Provider Overrides*',
        `• Total: ${overrides.length}`,
    ];
    if (!overrides.length) {
        lines.push('• None active');
        return lines.join('\n');
    }
    const preview = overrides.slice(0, 12);
    preview.forEach((item) => {
        const scope = escapeMarkdown(item.scope_key || 'unknown');
        const provider = escapeMarkdown(String(item.provider || 'twilio').toUpperCase());
        const expiresAt = item.expires_at ? escapeMarkdown(item.expires_at) : 'unknown';
        lines.push(`• ${scope} -> ${provider} (until ${expiresAt})`);
    });
    if (overrides.length > preview.length) {
        lines.push(`• ...and ${overrides.length - preview.length} more`);
    }
    return lines.join('\n');
}

async function handleProviderOverrides(ctx) {
    try {
        const payload = await fetchKeypadOverrides();
        logProviderEvent(ctx, { action: 'list_overrides', result: 'success' });
        await ctx.reply(formatKeypadOverridesResult(payload), { parse_mode: 'Markdown' });
    } catch (error) {
        logProviderEvent(ctx, { action: 'list_overrides', result: 'failure', error });
        await ctx.reply(formatProviderError(error, 'fetch keypad overrides'));
    }
}

async function handleProviderClearOverrides(ctx, args = []) {
    try {
        const rawScope = String(args[0] || '').trim();
        let payload;
        if (!rawScope || rawScope.toLowerCase() === 'all') {
            payload = await clearKeypadOverrides({ all: true });
            logProviderEvent(ctx, { action: 'clear_overrides', result: 'success' });
            await ctx.reply(
                `🧹 Cleared keypad overrides.\n• Cleared: ${payload.cleared || 0}\n• Remaining: ${payload.remaining || 0}`,
            );
            return;
        }
        payload = await clearKeypadOverrides({ scope_key: rawScope });
        logProviderEvent(ctx, { action: 'clear_overrides', result: 'success' });
        await ctx.reply(
            `🧹 Cleared keypad override scope.\n• Scope: ${escapeMarkdown(rawScope)}\n• Cleared: ${payload.cleared || 0}\n• Remaining: ${payload.remaining || 0}`,
            { parse_mode: 'Markdown' },
        );
    } catch (error) {
        logProviderEvent(ctx, { action: 'clear_overrides', result: 'failure', error });
        await ctx.reply(formatProviderError(error, 'clear keypad overrides'));
    }
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

async function ensureAuthorizedAdmin(ctx) {
    const fromId = ctx.from?.id;
    if (!fromId) {
        await ctx.reply('❌ Missing sender information.');
        return false;
    }
    const user = await new Promise((resolve) => getUser(fromId, resolve));
    if (!user) {
        await ctx.reply('❌ You are not authorized to use this bot.');
        return false;
    }
    const admin = await new Promise((resolve) => isAdmin(fromId, resolve));
    if (!admin) {
        await ctx.reply('❌ This command is for administrators only.');
        return false;
    }
    return true;
}

async function renderProviderDashboard(ctx, { status, notice, forceRefresh = false } = {}) {
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
        const notices = [notice, cachedNotice].filter(Boolean);
        let message = buildProviderDashboard(resolvedStatus);
        if (notices.length) {
            message = `${notices.join('\n')}\n\n${message}`;
        }
        message += '\n\nSelect a category to manage default providers.';
        await renderMenu(ctx, message, buildProviderDashboardKeyboard(ctx), { parseMode: 'Markdown' });
    } catch (error) {
        logProviderEvent(ctx, { action: 'render_dashboard', result: 'failure', error });
        await ctx.reply(formatProviderError(error, 'fetch provider status'));
    }
}

async function renderProviderSubmenu(ctx, channel, { status, notice, forceRefresh = false } = {}) {
    const normalizedChannel = normalizeChannel(channel);
    try {
        let resolvedStatus = status;
        let cachedNotice = null;
        if (!resolvedStatus) {
            try {
                resolvedStatus = await fetchProviderStatus({ force: forceRefresh, channel: normalizedChannel });
            } catch (error) {
                if (statusCache.value) {
                    resolvedStatus = statusCache.value;
                    cachedNotice = '⚠️ Showing cached provider status (API unavailable).';
                } else {
                    throw error;
                }
            }
        }
        const { supported, active } = normalizeSupportedProviders(resolvedStatus, normalizedChannel);
        const keyboard = buildProviderSubmenuKeyboard(ctx, normalizedChannel, supported, active);
        const notices = [notice, cachedNotice].filter(Boolean);
        let message = buildProviderSubmenuText(resolvedStatus, normalizedChannel);
        if (notices.length) {
            message = `${notices.join('\n')}\n\n${message}`;
        }
        await renderMenu(ctx, message, keyboard, { parseMode: 'Markdown' });
    } catch (error) {
        logProviderEvent(ctx, { action: 'render_submenu', channel: normalizedChannel, result: 'failure', error });
        await ctx.reply(formatProviderError(error, `fetch ${normalizedChannel.toUpperCase()} provider status`));
    }
}

async function renderProviderMenu(ctx, { status, notice, forceRefresh = false, channel = null } = {}) {
    if (!channel) {
        await renderProviderDashboard(ctx, { status, notice, forceRefresh });
        return;
    }
    await renderProviderSubmenu(ctx, channel, { status, notice, forceRefresh });
}

async function handleProviderSwitch(ctx, requestedProvider, channel = CHANNELS.CALL) {
    const normalizedChannel = normalizeChannel(channel);
    const normalizedProvider = normalizeProviderName(requestedProvider);
    if (!normalizedProvider) {
        await ctx.reply('❌ Missing provider value.');
        return;
    }
    try {
        const status = await fetchProviderStatus({ force: true, channel: normalizedChannel });
        const { supported } = normalizeSupportedProviders(status, normalizedChannel);
        if (!supported.includes(normalizedProvider)) {
            logProviderEvent(ctx, {
                action: 'set_provider',
                channel: normalizedChannel,
                provider: normalizedProvider,
                result: 'invalid_provider',
            });
            const supportedLabel = supported.length
                ? supported.map((item) => item.toUpperCase()).join(', ')
                : 'none';
            await ctx.reply(
                `❌ Unsupported ${normalizedChannel.toUpperCase()} provider "${escapeMarkdown(normalizedProvider)}".\nSupported: ${escapeMarkdown(supportedLabel)}`,
                { parse_mode: 'Markdown' },
            );
            return;
        }

        const result = await updateProvider(normalizedProvider, normalizedChannel);
        const refreshed = await fetchProviderStatus({ force: true, channel: normalizedChannel });
        const refreshedState = getChannelState(refreshed, normalizedChannel);
        const activeLabel = String(refreshedState.provider || normalizedProvider).toUpperCase();
        const channelLabel = normalizedChannel.toUpperCase();
        const notice = result.changed === false
            ? `ℹ️ ${channelLabel} provider already set to *${escapeMarkdown(activeLabel)}*.`
            : `✅ ${channelLabel} provider set to *${escapeMarkdown(activeLabel)}*.`;
        logProviderEvent(ctx, {
            action: 'set_provider',
            channel: normalizedChannel,
            provider: normalizedProvider,
            result: result.changed === false ? 'no_change' : 'success',
        });
        await renderProviderSubmenu(ctx, normalizedChannel, { status: refreshed, notice });
    } catch (error) {
        logProviderEvent(ctx, {
            action: 'set_provider',
            channel: normalizedChannel,
            provider: normalizedProvider,
            result: 'failure',
            error,
        });
        await ctx.reply(`❌ Failed to switch ${normalizedChannel.toUpperCase()} provider. Please try again.`);
    }
}

function parseProviderAction(action = '') {
    const normalized = String(action || '').trim();
    if (!normalized) return null;

    if (
        normalized === PROVIDER_ACTIONS.HOME ||
        normalized === PROVIDER_ACTIONS.BACK_HOME
    ) {
        return { type: 'home' };
    }
    if (normalized === PROVIDER_ACTIONS.CALL) {
        return { type: 'submenu', channel: CHANNELS.CALL };
    }
    if (normalized === PROVIDER_ACTIONS.SMS) {
        return { type: 'submenu', channel: CHANNELS.SMS };
    }
    if (normalized === PROVIDER_ACTIONS.EMAIL) {
        return { type: 'submenu', channel: CHANNELS.EMAIL };
    }

    if (normalized.startsWith(PROVIDER_ACTIONS.BACK_PREFIX)) {
        const scope = normalized.slice(PROVIDER_ACTIONS.BACK_PREFIX.length);
        if (scope.toUpperCase() === 'HOME') {
            return { type: 'home' };
        }
    }

    if (normalized.startsWith(PROVIDER_ACTIONS.STATUS_PREFIX)) {
        const segment = normalized.slice(PROVIDER_ACTIONS.STATUS_PREFIX.length);
        const channel = actionSegmentToChannel(segment);
        if (channel) {
            return { type: 'status', channel };
        }
    }

    if (normalized.startsWith(PROVIDER_ACTIONS.SET_PREFIX)) {
        const payload = normalized.slice(PROVIDER_ACTIONS.SET_PREFIX.length);
        const [segment, provider] = payload.split(':');
        const channel = actionSegmentToChannel(segment);
        const normalizedProvider = normalizeProviderName(provider);
        if (channel && normalizedProvider) {
            return { type: 'set', channel, provider: normalizedProvider };
        }
    }

    // Legacy callback compatibility
    if (normalized === PROVIDER_ACTIONS.LEGACY_STATUS) {
        return { type: 'home' };
    }
    if (normalized.startsWith(PROVIDER_ACTIONS.LEGACY_STATUS_CHANNEL_PREFIX)) {
        const channel = normalizeChannel(
            normalized.slice(PROVIDER_ACTIONS.LEGACY_STATUS_CHANNEL_PREFIX.length),
        );
        return { type: 'submenu', channel };
    }
    if (normalized.startsWith(PROVIDER_ACTIONS.LEGACY_SET_PREFIX)) {
        const provider = normalizeProviderName(
            normalized.slice(PROVIDER_ACTIONS.LEGACY_SET_PREFIX.length),
        );
        if (provider) {
            return { type: 'set', channel: CHANNELS.CALL, provider };
        }
    }
    if (normalized.startsWith(PROVIDER_ACTIONS.LEGACY_SET_CHANNEL_PREFIX)) {
        const payload = normalized.slice(PROVIDER_ACTIONS.LEGACY_SET_CHANNEL_PREFIX.length);
        const [channel, provider] = payload.split(':');
        const normalizedProvider = normalizeProviderName(provider);
        if (normalizedProvider) {
            return { type: 'set', channel: normalizeChannel(channel), provider: normalizedProvider };
        }
    }
    if (normalized === PROVIDER_ACTIONS.LEGACY_OVERRIDES) {
        return { type: 'overrides' };
    }
    if (normalized.startsWith(PROVIDER_ACTIONS.LEGACY_CLEAR_OVERRIDES_PREFIX)) {
        return {
            type: 'clear_overrides',
            scope: normalized.slice(PROVIDER_ACTIONS.LEGACY_CLEAR_OVERRIDES_PREFIX.length),
        };
    }

    return null;
}

function isProviderAction(action = '') {
    return Boolean(parseProviderAction(action));
}

async function handleProviderCallbackAction(ctx, action = '') {
    const parsed = parseProviderAction(action);
    if (!parsed) return false;

    if (parsed.type === 'home') {
        await renderProviderDashboard(ctx, { forceRefresh: true });
        return true;
    }
    if (parsed.type === 'submenu') {
        await renderProviderSubmenu(ctx, parsed.channel, { forceRefresh: true });
        return true;
    }
    if (parsed.type === 'status') {
        await renderProviderSubmenu(ctx, parsed.channel, { forceRefresh: true });
        return true;
    }
    if (parsed.type === 'set') {
        await handleProviderSwitch(ctx, parsed.provider, parsed.channel);
        return true;
    }
    if (parsed.type === 'overrides') {
        await handleProviderOverrides(ctx);
        return true;
    }
    if (parsed.type === 'clear_overrides') {
        await handleProviderClearOverrides(ctx, [parsed.scope || 'all']);
        return true;
    }
    return false;
}

function registerProviderCommand(bot) {
    bot.command('provider', async (ctx) => {
        const text = String(ctx.message?.text || '');
        const args = text.split(/\s+/).slice(1);
        const commandAction = String(args[0] || '').toLowerCase();
        const isAdminUser = await ensureAuthorizedAdmin(ctx);
        if (!isAdminUser) {
            return;
        }
        if (commandAction === 'overrides') {
            await handleProviderOverrides(ctx);
            return;
        }
        if (
            commandAction === 'clear-overrides' ||
            commandAction === 'clearoverride' ||
            commandAction === 'clear_overrides' ||
            commandAction === 'clear-override' ||
            commandAction === 'clear_override'
        ) {
            await handleProviderClearOverrides(ctx, args.slice(1));
            return;
        }
        await renderProviderDashboard(ctx, { forceRefresh: true });
    });
}

function initializeProviderCommand(bot) {
    registerProviderCommand(bot);
}

module.exports = initializeProviderCommand;
module.exports.registerProviderCommand = registerProviderCommand;
module.exports.fetchProviderStatus = fetchProviderStatus;
module.exports.updateProvider = updateProvider;
module.exports.fetchKeypadOverrides = fetchKeypadOverrides;
module.exports.clearKeypadOverrides = clearKeypadOverrides;
module.exports.handleProviderOverrides = handleProviderOverrides;
module.exports.handleProviderClearOverrides = handleProviderClearOverrides;
module.exports.handleProviderSwitch = handleProviderSwitch;
module.exports.handleProviderCallbackAction = handleProviderCallbackAction;
module.exports.renderProviderMenu = renderProviderMenu;
module.exports.renderProviderDashboard = renderProviderDashboard;
module.exports.renderProviderSubmenu = renderProviderSubmenu;
module.exports.buildProviderDashboardKeyboard = buildProviderDashboardKeyboard;
module.exports.buildProviderSubmenuKeyboard = buildProviderSubmenuKeyboard;
module.exports.buildProviderDashboard = buildProviderDashboard;
module.exports.buildProviderSubmenuText = buildProviderSubmenuText;
module.exports.normalizeChannel = normalizeChannel;
module.exports.parseProviderAction = parseProviderAction;
module.exports.isProviderAction = isProviderAction;
module.exports.DEFAULT_SUPPORTED_PROVIDERS = DEFAULT_SUPPORTED_PROVIDERS;
module.exports.PROVIDER_ACTIONS = PROVIDER_ACTIONS;
module.exports.ADMIN_HEADER_NAME = ADMIN_HEADER_NAME;
