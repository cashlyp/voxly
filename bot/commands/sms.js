const config = require('../config');
const axios = require('axios');
const { getUser, isAdmin } = require('../db/db');
const {
    startOperation,
    ensureOperationActive,
    registerAbortController,
    OperationCancelledError,
    guardAgainstCommandInterrupt
} = require('../utils/sessionState');
const {
    getBusinessOptions,
    MOOD_OPTIONS,
    URGENCY_OPTIONS,
    TECH_LEVEL_OPTIONS,
    askOptionWithButtons,
    getOptionLabel
} = require('../utils/persona');

const {
    buildScriptOption,
    CUSTOM_SCRIPT_OPTION,
    extractScriptVariables,
    SCRIPT_METADATA
} = require('../utils/scripts');
const { section: formatSection, buildLine, tipLine } = require('../utils/commandFormat');

async function smsAlert(ctx, text) {
    await ctx.reply(formatSection('⚠️ SMS Alert', [text]));
}

// Simple phone number validation
function isValidPhoneNumber(number) {
    const e164Regex = /^\+[1-9]\d{1,14}$/;
    return e164Regex.test(number.trim());
}

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

// SMS sending flow (UNCHANGED - already working)
async function smsFlow(conversation, ctx) {
    const opId = startOperation(ctx, 'sms');
    const ensureActive = () => ensureOperationActive(ctx, opId);
    const waitForMessage = async () => {
        const update = await conversation.wait();
        ensureActive();
        const text = update?.message?.text?.trim();
        if (text) {
            await guardAgainstCommandInterrupt(ctx, text);
        }
        return update;
    };
    const askWithGuard = async (...params) => {
        const result = await askOptionWithButtons(...params);
        ensureActive();
        return result;
    };
    const guardedGet = async (url, options = {}) => {
        const controller = new AbortController();
        const release = registerAbortController(ctx, controller);
        try {
            const response = await axios.get(url, { timeout: 20000, signal: controller.signal, ...options });
            ensureActive();
            return response;
        } finally {
            release();
        }
    };
    const guardedPost = async (url, data, options = {}) => {
        const controller = new AbortController();
        const release = registerAbortController(ctx, controller);
        try {
            const response = await axios.post(url, data, { timeout: 30000, signal: controller.signal, ...options });
            ensureActive();
            return response;
        } finally {
            release();
        }
    };

    try {
        ensureActive();
        const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
        ensureActive();
        if (!user) {
            await ctx.reply(formatSection('❌ Authorization', ['You are not authorized to use this bot.']));
            return;
        }

        const prefill = ctx.session.meta?.prefill || {};
        let number = prefill.phoneNumber || null;

        if (number) {
            await ctx.reply(formatSection('📞 Follow-up number', [
                buildLine('➡️', 'Using', number)
            ]));
            if (ctx.session.meta) {
                delete ctx.session.meta.prefill;
            }
        } else {
            await ctx.reply(formatSection('📱 Enter phone number', ['Use E.164 format, e.g., +1234567890']));
            const numMsg = await waitForMessage();
            number = numMsg?.message?.text?.trim();

            if (!number) return smsAlert(ctx, 'Please provide a phone number.');
            if (!isValidPhoneNumber(number)) {
                return smsAlert(ctx, 'Invalid phone number format. Use E.164 format: +1234567890');
            }
        }

        const businessOptions = await getBusinessOptions();
        ensureActive();

        const selectedBusiness = await askWithGuard(
            conversation,
            ctx,
            `🎭 *Select SMS persona:*
Choose the business profile for this message.`,
            businessOptions,
            {
                prefix: 'sms-persona',
                columns: 2,
                formatLabel: (option) => option.custom ? '✍️ Custom Message' : option.label
            }
        );

        if (!selectedBusiness) {
            await ctx.reply('❌ Invalid persona selection. Please try again.');
            return;
        }

        const payload = {
            to: number,
            user_chat_id: ctx.from.id.toString()
        };

        const personaSummary = [];
        let selectedPurpose = null;
        let recommendedEmotion = 'neutral';
        let recommendedUrgency = 'normal';
        let scriptSelection = null;
        let scriptName = null;
        let scriptVariables = {};
        let message = '';

        if (!selectedBusiness.custom) {
            payload.business_id = selectedBusiness.id;
            payload.channel = 'sms';

            const availablePurposes = selectedBusiness.purposes || [];
            selectedPurpose = availablePurposes.find((p) => p.id === selectedBusiness.defaultPurpose) || availablePurposes[0];

            if (availablePurposes.length > 1) {
                selectedPurpose = await askWithGuard(
                    conversation,
                    ctx,
                    `🎯 *Choose message purpose:*
This helps set tone and urgency automatically.`,
                    availablePurposes,
                    {
                        prefix: 'sms-purpose',
                        columns: 1,
                        formatLabel: (option) => `${option.emoji || '•'} ${option.label}`
                    }
                );
            }

            selectedPurpose = selectedPurpose || availablePurposes[0];
            recommendedEmotion = selectedPurpose?.defaultEmotion || 'neutral';
            recommendedUrgency = selectedPurpose?.defaultUrgency || 'normal';

            if (selectedPurpose?.id && selectedPurpose.id !== 'general') {
                payload.purpose = selectedPurpose.id;
            }

            const moodSelection = await askWithGuard(
                conversation,
                ctx,
                `🎙️ *Tone preference*
Recommended: *${getOptionLabel(MOOD_OPTIONS, recommendedEmotion)}*.`,
                MOOD_OPTIONS,
                { prefix: 'sms-tone', columns: 2 }
            );

            if (moodSelection.id !== 'auto') {
                payload.emotion = moodSelection.id;
                personaSummary.push(`Tone: ${moodSelection.label}`);
            } else {
                personaSummary.push(`Tone: ${moodSelection.label} (${getOptionLabel(MOOD_OPTIONS, recommendedEmotion)})`);
            }

            const urgencySelection = await askWithGuard(
                conversation,
                ctx,
                `⏱️ *Urgency level*
Recommended: *${getOptionLabel(URGENCY_OPTIONS, recommendedUrgency)}*.`,
                URGENCY_OPTIONS,
                { prefix: 'sms-urgency', columns: 2 }
            );

            if (urgencySelection.id !== 'auto') {
                payload.urgency = urgencySelection.id;
                personaSummary.push(`Urgency: ${urgencySelection.label}`);
            } else {
                personaSummary.push(`Urgency: ${urgencySelection.label} (${getOptionLabel(URGENCY_OPTIONS, recommendedUrgency)})`);
            }

            const techSelection = await askWithGuard(
                conversation,
                ctx,
                `🧠 *Recipient technical level:*
How comfortable is the recipient with technical details?`,
                TECH_LEVEL_OPTIONS,
                { prefix: 'sms-tech', columns: 2 }
            );

            if (techSelection.id !== 'auto') {
                payload.technical_level = techSelection.id;
                personaSummary.push(`Technical level: ${techSelection.label}`);
            } else {
                personaSummary.push(`Technical level: ${getOptionLabel(TECH_LEVEL_OPTIONS, 'general')}`);
            }

            personaSummary.unshift(`Persona: ${selectedBusiness.label}`);
            if (selectedPurpose?.label) {
                personaSummary.splice(1, 0, `Purpose: ${selectedPurpose.label}`);
            }
        }

        // Fetch available scripts
        let scriptChoices = [];
        try {
            const scriptResponse = await guardedGet(`${config.apiUrl}/api/sms/scripts`, {
                params: { include_builtins: true, detailed: true }
            });

            const builtinScripts = (scriptResponse.data.builtin || []).map((script) => ({
                id: script.name,
                label: buildScriptOption(script.name).label,
                description: buildScriptOption(script.name).description,
                content: script.content,
                is_builtin: true
            }));

            const customScripts = (scriptResponse.data.scripts || []).map((script) => ({
                id: script.name,
                label: `📝 ${script.name}`,
                description: script.description || 'Custom script',
                content: script.content,
                is_builtin: false
            }));

            scriptChoices = [...builtinScripts, ...customScripts];
        } catch (scriptError) {
            console.error('❌ Failed to fetch SMS scripts:', scriptError);
            scriptChoices = Object.keys(SCRIPT_METADATA || {})
                .map(buildScriptOption);
        }

        scriptChoices.push(CUSTOM_SCRIPT_OPTION);

        const scriptListText = scriptChoices
            .map((option) => `• ${option.label}${option.description ? ` - ${option.description}` : ''}`)
            .join('\n');

        const scriptPrompt = `📝 *Choose SMS script:*
${scriptListText}

Tap an option below to continue.`;

        scriptSelection = await askWithGuard(
            conversation,
            ctx,
            scriptPrompt,
            scriptChoices,
            { prefix: 'sms-script', columns: 1, formatLabel: (option) => option.label }
        );

        if (scriptSelection.id === 'custom') {
            await ctx.reply('💬 Enter the SMS message (max 1600 characters):');
            const msgContent = await waitForMessage();
            message = msgContent?.message?.text?.trim();

            if (!message) return smsAlert(ctx, 'Please provide a message.');
            if (message.length > 1600) {
                return ctx.reply('❌ Message too long. SMS messages must be under 1600 characters.');
            }
            personaSummary.push('Script: Custom message');
        } else {
            scriptName = scriptSelection.id;

            try {
                const scriptResponse = await guardedGet(`${config.apiUrl}/api/sms/scripts/${scriptName}`, {
                    params: { detailed: true }
                });

                const scriptPayload = scriptResponse.data.script;
                let scriptText = scriptPayload?.content || '';
                const placeholders = extractScriptVariables(scriptPayload?.content || '');

                if (placeholders.length > 0) {
                    await ctx.reply('🧩 This script includes placeholders. Provide values or type skip to leave them unchanged.');

                    for (const token of placeholders) {
                        await ctx.reply(`✏️ Enter value for *${token}* (type skip to leave as is):`, { parse_mode: 'Markdown' });
                        const valueMsg = await waitForMessage();
                        const value = valueMsg?.message?.text?.trim();

                        if (value && value.toLowerCase() !== 'skip') {
                            scriptVariables[token] = value;
                        }
                    }

                    for (const [token, value] of Object.entries(scriptVariables)) {
                        scriptText = scriptText.replace(new RegExp(`{${token}}`, 'g'), value);
                    }
                }

                message = scriptText;
                personaSummary.push(`Script: ${scriptSelection.label}`);
                if (Object.keys(scriptVariables).length > 0) {
                    personaSummary.push(`Filled variables: ${Object.keys(scriptVariables).join(', ')}`);
                }
            } catch (scriptFetchError) {
                console.error('❌ Failed to load script content:', scriptFetchError);
                await ctx.reply('⚠️ Could not load the selected script. Please type a custom message instead.');

                await ctx.reply('💬 Enter the SMS message (max 1600 characters):');
                const msgContent = await waitForMessage();
                message = msgContent?.message?.text?.trim();

                if (!message) return smsAlert(ctx, 'Please provide a message.');
                if (message.length > 1600) {
                    return ctx.reply('❌ Message too long. SMS messages must be under 1600 characters.');
                }
                personaSummary.push('Script: Custom message (fallback)');
            }
        }

        if (!message) {
            return ctx.reply('❌ Unable to generate an SMS message. Please try again.');
        }

        if (message.length > 1600) {
            return ctx.reply(`❌ Message too long (${message.length} characters). Please shorten it below 1600 characters.`);
        }

        if (scriptName) {
            payload.script_name = scriptName;
        }

        if (Object.keys(scriptVariables).length > 0) {
            payload.script_variables = scriptVariables;
        }

        let previewAction = null;
        while (true) {
            const segmentInfo = getSmsSegmentInfo(message);
            const previewLines = [
                '📱 SMS Preview',
                '',
                `📞 To: ${number}`,
                `📏 Length: ${segmentInfo.length} characters`,
                `📦 Segments: ${segmentInfo.segments} (${segmentInfo.encoding.toUpperCase()} ${segmentInfo.units}/${segmentInfo.per_segment})`,
                '',
                '💬 Message:',
                message
            ];

            if (personaSummary.length > 0) {
                previewLines.push('', 'Details:', ...personaSummary.map((line) => `• ${line}`));
            }

            previewAction = await askWithGuard(
                conversation,
                ctx,
                previewLines.join('\n'),
                [
                    { id: 'send', label: '✅ Send now' },
                    { id: 'edit', label: '✏️ Edit message' },
                    { id: 'cancel', label: '🛑 Cancel' }
                ],
                { prefix: 'sms-preview', columns: 2 }
            );

            if (!previewAction || previewAction.id === 'cancel') {
                await ctx.reply('🛑 SMS cancelled.');
                return;
            }

            if (previewAction.id === 'edit') {
                await ctx.reply('✏️ Edit the SMS message (max 1600 characters):');
                const msgContent = await waitForMessage();
                const edited = msgContent?.message?.text?.trim();
                if (!edited) {
                    await smsAlert(ctx, 'Please provide a message.');
                    continue;
                }
                if (edited.length > 1600) {
                    await ctx.reply('❌ Message too long. SMS messages must be under 1600 characters.');
                    continue;
                }
                message = edited;
                continue;
            }

            if (previewAction.id === 'send') {
                break;
            }
        }

        await ctx.reply('⏳ Sending SMS...');

        const response = await guardedPost(`${config.apiUrl}/api/sms/send`, {
            ...payload,
            message,
        }, {
            headers: { 'Content-Type': 'application/json' }
        });

        const data = response?.data || {};

        if (data.success) {
            const segmentInfo = data.segment_info || getSmsSegmentInfo(message);
            const successMsg =
                `✅ *SMS Sent Successfully!*\n\n` +
                `📱 To: ${data.to}\n` +
                `🆔 Message SID: \`${data.message_sid}\`\n` +
                `📊 Status: ${data.status}\n` +
                `📤 From: ${data.from}\n` +
                `📦 Segments: ${segmentInfo.segments} (${segmentInfo.encoding.toUpperCase()} ${segmentInfo.units}/${segmentInfo.per_segment})\n\n` +
                `🔔 You'll receive delivery notifications`;

            await ctx.reply(successMsg, { parse_mode: 'Markdown' });
        } else {
            await ctx.reply('⚠️ SMS was sent but response format unexpected. Check logs.');
        }
    } catch (error) {
        if (error instanceof OperationCancelledError || error?.name === 'AbortError' || error?.name === 'CanceledError') {
            console.log('SMS flow cancelled');
            return;
        }
        console.error('SMS send error:', error);
        if (error.response) {
            console.error('SMS send error response data:', error.response.data);
        }
        let errorMsg = '❌ *SMS Failed*\n\n';

        if (error.response) {
            const status = error.response.status;
            const errorData = error.response.data;
            if (status === 400) {
                errorMsg += `Bad Request: ${errorData?.error || 'Invalid data'}`;
            } else if (status === 500) {
                errorMsg += `Server Error: ${errorData?.error || 'Internal server error'}`;
            } else {
                errorMsg += `HTTP ${status}: ${errorData?.error || error.response.statusText}`;
            }
        } else if (error.request) {
            errorMsg += `Network Error: Cannot reach API server\nURL: ${config.apiUrl}`;
        } else {
            errorMsg += `Error: ${error.message}`;
        }

        await ctx.reply(errorMsg, { parse_mode: 'Markdown' });
    }
}

// Bulk SMS flow
async function bulkSmsFlow(conversation, ctx) {
    const opId = startOperation(ctx, 'bulk-sms');
    const ensureActive = () => ensureOperationActive(ctx, opId);
    const waitForMessage = async () => {
        const update = await conversation.wait();
        ensureActive();
        const text = update?.message?.text?.trim();
        if (text) {
            await guardAgainstCommandInterrupt(ctx, text);
        }
        return update;
    };
    const askWithGuard = async (...params) => {
        const result = await askOptionWithButtons(...params);
        ensureActive();
        return result;
    };

    try {
        ensureActive();
        const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
        ensureActive();
        if (!user) {
            await ctx.reply('❌ You are not authorized to use this bot.');
            return;
        }

        const adminStatus = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
        ensureActive();
        if (!adminStatus) {
            await ctx.reply('❌ Bulk SMS is for administrators only.');
            return;
        }

        await ctx.reply('📱 Enter phone numbers separated by commas or newlines (max 100):');

        const numbersMsg = await waitForMessage();
        const numbersText = numbersMsg?.message?.text?.trim();

        if (!numbersText) return smsAlert(ctx, 'Please provide phone numbers.');

        const numbers = numbersText
            .split(/[,\n]/)
            .map(n => n.trim())
            .filter(n => n.length > 0);

        if (numbers.length === 0) return ctx.reply('❌ No valid phone numbers found.');
        if (numbers.length > 100) return ctx.reply('❌ Maximum 100 phone numbers allowed per bulk send.');

        const invalidNumbers = numbers.filter(n => !isValidPhoneNumber(n));
        if (invalidNumbers.length > 0) {
            return ctx.reply(
                `❌ Invalid phone number format found: ${invalidNumbers.slice(0, 3).join(', ')}${invalidNumbers.length > 3 ? '...' : ''}\n\nUse E.164 format: +1234567890`
            );
        }

        await ctx.reply(`💬 Enter the message to send to ${numbers.length} recipients (max 1600 chars):`);
        const msgContent = await waitForMessage();
        let message = msgContent?.message?.text?.trim();

        if (!message) return smsAlert(ctx, 'Please provide a message.');
        if (message.length > 1600) {
            return ctx.reply('❌ Message too long. SMS messages must be under 1600 characters.');
        }

        let previewAction = null;
        while (true) {
            const segmentInfo = getSmsSegmentInfo(message);
            const previewLines = [
                '📣 Bulk SMS Preview',
                '',
                `👥 Recipients: ${numbers.length}`,
                `📱 Sample: ${numbers.slice(0, 3).join(', ')}${numbers.length > 3 ? '...' : ''}`,
                `📏 Length: ${segmentInfo.length} characters`,
                `📦 Segments: ${segmentInfo.segments} (${segmentInfo.encoding.toUpperCase()} ${segmentInfo.units}/${segmentInfo.per_segment})`,
                '',
                '💬 Message:',
                message
            ];

            previewAction = await askWithGuard(
                conversation,
                ctx,
                previewLines.join('\n'),
                [
                    { id: 'send', label: '✅ Send now' },
                    { id: 'edit', label: '✏️ Edit message' },
                    { id: 'cancel', label: '🛑 Cancel' }
                ],
                { prefix: 'bulk-sms-preview', columns: 2 }
            );

            if (!previewAction || previewAction.id === 'cancel') {
                await ctx.reply('🛑 Bulk SMS cancelled.');
                return;
            }

            if (previewAction.id === 'edit') {
                await ctx.reply('✏️ Edit the bulk SMS message (max 1600 characters):');
                const editedMsg = await waitForMessage();
                const edited = editedMsg?.message?.text?.trim();
                if (!edited) {
                    await smsAlert(ctx, 'Please provide a message.');
                    continue;
                }
                if (edited.length > 1600) {
                    await ctx.reply('❌ Message too long. SMS messages must be under 1600 characters.');
                    continue;
                }
                message = edited;
                continue;
            }

            if (previewAction.id === 'send') {
                break;
            }
        }

        await ctx.reply('⏳ Sending bulk SMS...');

        const payload = {
            recipients: numbers,
            message: message,
            user_chat_id: ctx.from.id.toString(),
            options: { delay: 1000, batchSize: 10 }
        };

        const response = await guardedPost(`${config.apiUrl}/api/sms/bulk`, payload, {
            headers: { 'Content-Type': 'application/json' }
        });

        const data = response?.data || {};

        if (data.success) {
            const result = data;
            const segmentInfo = result.segment_info || getSmsSegmentInfo(message);
            const scheduledCount = Number(result.scheduled) || 0;
            const suppressedCount = Number(result.suppressed) || 0;
            const invalidCount = Number(result.invalid) || 0;
            const hardFailed = Math.max(0, result.failed - suppressedCount - invalidCount);
            const immediateSent = Math.max(0, result.successful - scheduledCount);
            const successMsg =
                `✅ *Bulk SMS Completed!*\n\n` +
                `👥 Total Recipients: ${result.total}\n` +
                `✅ Sent now: ${immediateSent}\n` +
                `🗓️ Scheduled: ${scheduledCount}\n` +
                `🚫 Suppressed (opt-out): ${suppressedCount}\n` +
                `⚠️ Invalid numbers: ${invalidCount}\n` +
                `❌ Failed: ${hardFailed}\n` +
                `📊 Success Rate: ${Math.round((result.successful / result.total) * 100)}%\n\n` +
                `📦 Segments per SMS: ${segmentInfo.segments} (${segmentInfo.encoding.toUpperCase()} ${segmentInfo.units}/${segmentInfo.per_segment})\n\n` +
                `🔔 Individual delivery reports will follow`;

            await ctx.reply(successMsg, { parse_mode: 'Markdown' });

            if (hardFailed > 0) {
                const failedResults = result.results.filter(r => !r.success && !r.suppressed && r.error !== 'invalid_phone_format');
                if (failedResults.length <= 10 && failedResults.length > 0) {
                    let failedMsg = '❌ *Failed Numbers:*\n\n';
                    failedResults.forEach(r => {
                        failedMsg += `• ${r.recipient}: ${r.error}\n`;
                    });
                    await ctx.reply(failedMsg, { parse_mode: 'Markdown' });
                }
            }
        } else {
            await ctx.reply('⚠️ Bulk SMS completed but response format unexpected.');
        }
    } catch (error) {
        if (error instanceof OperationCancelledError || error?.name === 'AbortError' || error?.name === 'CanceledError') {
            console.log('Bulk SMS flow cancelled');
            return;
        }
        console.error('Bulk SMS error:', error);
        let errorMsg = '❌ *Bulk SMS Failed*\n\n';
        errorMsg += error.response ? `Error: ${error.response.data?.error || 'Unknown error'}` : `Error: ${error.message}`;
        await ctx.reply(errorMsg, { parse_mode: 'Markdown' });
    }
}

// Schedule SMS flow (UNCHANGED - already working)
async function scheduleSmsFlow(conversation, ctx) {
    const opId = startOperation(ctx, 'schedule-sms');
    const ensureActive = () => ensureOperationActive(ctx, opId);
    const waitForMessage = async () => {
        const update = await conversation.wait();
        ensureActive();
        const text = update?.message?.text?.trim();
        if (text) {
            await guardAgainstCommandInterrupt(ctx, text);
        }
        return update;
    };
    const guardedPost = async (url, data, options = {}) => {
        const controller = new AbortController();
        const release = registerAbortController(ctx, controller);
        try {
            const response = await axios.post(url, data, { timeout: 30000, signal: controller.signal, ...options });
            ensureActive();
            return response;
        } finally {
            release();
        }
    };

    try {
        ensureActive();
        const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
        ensureActive();
        if (!user) {
            await ctx.reply('❌ You are not authorized to use this bot.');
            return;
        }

        const prefill = ctx.session.meta?.prefill || {};
        let number = prefill.phoneNumber || null;

        if (number) {
            await ctx.reply(`📞 Using follow-up number: ${number}`);
            if (ctx.session.meta) {
                delete ctx.session.meta.prefill;
            }
        } else {
            await ctx.reply('📱 Enter phone number (E.164 format):');
            const numMsg = await waitForMessage();
            number = numMsg?.message?.text?.trim();

            if (!number || !isValidPhoneNumber(number)) {
                return ctx.reply('❌ Invalid phone number format. Use E.164 format: +1234567890');
            }
        }

        await ctx.reply('💬 Enter the message:');
        const msgContent = await waitForMessage();
        const message = msgContent?.message?.text?.trim();
        if (!message) return smsAlert(ctx, 'Please provide a message.');

        await ctx.reply('⏰ Enter schedule time (e.g., "2024-12-25 14:30" or "in 2 hours"):');
        const timeMsg = await waitForMessage();
        const timeText = timeMsg?.message?.text?.trim();
        if (!timeText) return smsAlert(ctx, 'Please provide a schedule time.');

        let scheduledTime;
        try {
            if (timeText.toLowerCase().includes('in ')) {
                const match = timeText.match(/in (\d+) (minute|minutes|hour|hours|day|days)/i);
                if (match) {
                    const amount = parseInt(match[1]);
                    const unit = match[2].toLowerCase();
                    const now = new Date();
                    if (unit.startsWith('minute')) scheduledTime = new Date(now.getTime() + amount * 60 * 1000);
                    else if (unit.startsWith('hour')) scheduledTime = new Date(now.getTime() + amount * 60 * 60 * 1000);
                    else if (unit.startsWith('day')) scheduledTime = new Date(now.getTime() + amount * 24 * 60 * 60 * 1000);
                } else throw new Error('Invalid relative time format');
            } else {
                scheduledTime = new Date(timeText);
            }

            if (isNaN(scheduledTime.getTime())) throw new Error('Invalid date');
            if (scheduledTime <= new Date()) throw new Error('Schedule time must be in the future');
        } catch {
            return ctx.reply(
                '❌ Invalid time format. Use formats like:\n• "2024-12-25 14:30"\n• "in 2 hours"\n• "in 30 minutes"'
            );
        }

        const confirmText =
            `⏰ *Schedule SMS*\n\n` +
            `📱 To: ${number}\n` +
            `💬 Message: ${message.substring(0, 100)}${message.length > 100 ? '...' : ''}\n` +
            `📅 Scheduled: ${scheduledTime.toLocaleString()}\n\n` +
            `⏳ Scheduling SMS...`;

        await ctx.reply(confirmText, { parse_mode: 'Markdown' });

        const payload = {
            to: number,
            message: message,
            scheduled_time: scheduledTime.toISOString(),
            user_chat_id: ctx.from.id.toString()
        };

        const response = await guardedPost(`${config.apiUrl}/api/sms/schedule`, payload, {
            headers: { 'Content-Type': 'application/json' }
        });

        const data = response?.data || {};

        if (data.success) {
            const successMsg =
                `✅ *SMS Scheduled Successfully!*\n\n` +
                `🆔 Schedule ID: \`${data.schedule_id}\`\n` +
                `📅 Will send: ${data.scheduled_time ? new Date(data.scheduled_time).toLocaleString() : 'unknown'}\n` +
                `📱 To: ${number}\n\n` +
                `🔔 You'll receive confirmation when sent`;

            await ctx.reply(successMsg, { parse_mode: 'Markdown' });
        }
    } catch (error) {
        if (error instanceof OperationCancelledError || error?.name === 'AbortError' || error?.name === 'CanceledError') {
            console.log('Schedule SMS flow cancelled');
            return;
        }
        console.error('Schedule SMS error:', error);
        await ctx.reply('❌ Failed to schedule SMS. Please try again.');
    }
}

// FIXED: SMS conversation viewer - now gets data from database via API
async function viewSmsConversation(ctx, phoneNumber) {
    try {
        console.log(`Fetching SMS conversation for ${phoneNumber}`);
        
        // First try to get conversation from SMS service (in-memory)
        const response = await axios.get(
            `${config.apiUrl}/api/sms/conversation/${encodeURIComponent(phoneNumber)}`,
            { timeout: 15000 }
        );

        if (response.data.success && response.data.conversation) {
            const conversation = response.data.conversation;
            const messages = conversation.messages;

            let conversationText =
                `💬 *SMS Conversation (Active)*\n\n` +
                `📱 Phone: ${conversation.phone}\n` +
                `💬 Messages: ${messages.length}\n` +
                `🕐 Started: ${new Date(conversation.created_at).toLocaleString()}\n` +
                `⏰ Last Activity: ${new Date(conversation.last_activity).toLocaleString()}\n\n` +
                `*Recent Messages:*\n` +
                `${'─'.repeat(25)}\n`;

            const recentMessages = messages.slice(-10);
            recentMessages.forEach(msg => {
                const time = new Date(msg.timestamp).toLocaleTimeString();
                const sender = msg.role === 'user' ? '👤 Victim' : '🤖 AI';
                const cleanMsg = msg.content.replace(/[*_`[\]()~>#+=|{}.!-]/g, '\\$&');
                conversationText += `\n${sender} _(${time})_\n${cleanMsg}\n`;
            });

            if (messages.length > 10) {
                conversationText += `\n_... and ${messages.length - 10} earlier messages_`;
            }

            await ctx.reply(conversationText, { parse_mode: 'Markdown' });
        } else {
            // If no active conversation, check database for stored SMS messages
            console.log('No active conversation found, checking database...');
            await viewStoredSmsConversation(ctx, phoneNumber);
        }
    } catch (error) {
        console.error('SMS conversation error:', error);
        if (error.response?.status === 404) {
            // Try database lookup as fallback
            await viewStoredSmsConversation(ctx, phoneNumber);
        } else {
            await ctx.reply('❌ Error fetching conversation. Please try again.');
        }
    }
}

// NEW: Get stored SMS conversation from database
async function viewStoredSmsConversation(ctx, phoneNumber) {
    try {
        // Call API endpoint to get stored SMS messages from database
        const response = await axios.get(
            `${config.apiUrl}/api/sms/messages/conversation/${encodeURIComponent(phoneNumber)}`,
            { timeout: 15000 }
        );

        if (response.data.success && response.data.messages.length > 0) {
            const messages = response.data.messages;
            
            let conversationText =
                `💬 *SMS Conversation History*\n\n` +
                `📱 Phone: ${phoneNumber}\n` +
                `💬 Total Messages: ${messages.length}\n` +
                `🕐 First Message: ${new Date(messages[0].created_at).toLocaleString()}\n` +
                `⏰ Last Message: ${new Date(messages[messages.length - 1].created_at).toLocaleString()}\n\n` +
                `*Recent Messages:*\n` +
                `${'─'.repeat(25)}\n`;

            // Show last 15 messages
            const recentMessages = messages.slice(-15);
            recentMessages.forEach(msg => {
                const time = new Date(msg.created_at).toLocaleTimeString();
                const direction = msg.direction === 'inbound' ? '📨 Received' : '📤 Sent';
                const cleanMsg = msg.body.replace(/[*_`[\]()~>#+=|{}.!-]/g, '\\$&');
                const status = msg.status ? ` (${msg.status})` : '';
                
                conversationText += `\n${direction}${status} _(${time})_\n${cleanMsg}\n`;
                
                // Show AI response if available
                if (msg.ai_response && msg.response_message_sid) {
                    const cleanAiMsg = msg.ai_response.replace(/[*_`[\]()~>#+=|{}.!-]/g, '\\$&');
                    conversationText += `🤖 AI Response _(${time})_\n${cleanAiMsg}\n`;
                }
            });

            if (messages.length > 15) {
                conversationText += `\n_... and ${messages.length - 15} earlier messages_`;
            }

            await ctx.reply(conversationText, { parse_mode: 'Markdown' });
        } else {
            await ctx.reply('❌ No conversation found with this phone number');
        }
    } catch (error) {
        console.error('Error fetching stored SMS conversation:', error);
        await ctx.reply('❌ No conversation found with this phone number');
    }
}

// FIXED: SMS statistics - now gets real data from database and service
async function getSmsStats(ctx) {
    try {
        console.log('Fetching SMS stats...');
        
        // Get stats from SMS service (in-memory data)
        const serviceResponse = await axios.get(`${config.apiUrl}/api/sms/stats`, { timeout: 10000 });
        
        // Get additional stats from database
        const dbStatsResponse = await axios.get(`${config.apiUrl}/api/sms/database-stats`, { timeout: 10000 });

        let statsText = `📊 *SMS Statistics*\n\n`;

        if (serviceResponse.data.success) {
            const stats = serviceResponse.data.statistics;
            const conversations = serviceResponse.data.active_conversations || [];

            statsText += 
                `**Active Service Data:**\n` +
                `💬 Active Conversations: ${stats.active_conversations || 0}\n` +
                `⏰ Scheduled Messages: ${stats.scheduled_messages || 0}\n` +
                `📋 Queue Size: ${stats.message_queue_size || 0}\n\n`;

            if (conversations.length > 0) {
                statsText += `*Recent Active Conversations:*\n`;
                conversations.slice(0, 5).forEach(conv => {
                    const lastActivity = new Date(conv.last_activity).toLocaleTimeString();
                    statsText += `• ${conv.phone} - ${conv.message_count} msgs (${lastActivity})\n`;
                });
                statsText += '\n';
            }
        }

        if (dbStatsResponse.data.success) {
            const dbStats = dbStatsResponse.data;
            statsText += 
                `**Database Statistics:**\n` +
                `📱 Total SMS Messages: ${dbStats.total_messages || 0}\n` +
                `📤 Sent Messages: ${dbStats.sent_messages || 0}\n` +
                `📨 Received Messages: ${dbStats.received_messages || 0}\n` +
                `✅ Delivered: ${dbStats.delivered_count || 0}\n` +
                `❌ Failed: ${dbStats.failed_count || 0}\n` +
                `📊 Success Rate: ${dbStats.success_rate || '0'}%\n` +
                `🔄 Bulk Operations: ${dbStats.bulk_operations || 0}\n\n`;

            if (dbStats.recent_messages && dbStats.recent_messages.length > 0) {
                statsText += `*Recent Database Messages:*\n`;
                dbStats.recent_messages.slice(0, 3).forEach(msg => {
                    const time = new Date(msg.created_at).toLocaleTimeString();
                    const direction = msg.direction === 'inbound' ? '📨' : '📤';
                    const phone = msg.to_number || msg.from_number || 'Unknown';
                    statsText += `${direction} ${phone} - ${msg.status} (${time})\n`;
                });
            }
        }

        await ctx.reply(statsText, { parse_mode: 'Markdown' });
        
    } catch (error) {
        console.error('SMS stats error:', error);
        
        // Fallback: try to get basic stats
        try {
            const basicResponse = await axios.get(`${config.apiUrl}/api/sms/database-stats`, { timeout: 5000 });
            if (basicResponse.data.success) {
                const stats = basicResponse.data.statistics;
                const basicStatsText = 
                    `📊 *Basic SMS Statistics*\n\n` +
                    `💬 Active Conversations: ${stats.active_conversations || 0}\n` +
                    `⏰ Scheduled Messages: ${stats.scheduled_messages || 0}\n` +
                    `📋 Queue Size: ${stats.message_queue_size || 0}\n\n` +
                    `_Note: Some detailed statistics are temporarily unavailable_`;
                    
                await ctx.reply(basicStatsText, { parse_mode: 'Markdown' });
            } else {
                await ctx.reply('❌ Error fetching SMS statistics. Service may be down.');
            }
        } catch (fallbackError) {
            await ctx.reply('❌ Error fetching SMS statistics. API server unreachable.');
        }
    }
}

// FIXED: SMS scripts - now properly handles the API response
// Register SMS command handlers with conversation flows
function registerSmsCommands(bot) {

    // Main SMS command
    bot.command('sms', async ctx => {
        try {
            const user = await new Promise(resolve => getUser(ctx.from.id, resolve));
            if (!user) {
                return ctx.reply('❌ You are not authorized to use this bot.');
            }
            await ctx.conversation.enter('sms-conversation');
        } catch (error) {
            console.error('SMS command error:', error);
            await ctx.reply('❌ Could not start SMS process. Please try again.');
        }
    });

    // Bulk SMS command
    bot.command('bulksms', async ctx => {
        try {
            const user = await new Promise(resolve => getUser(ctx.from.id, resolve));
            if (!user) { 
                return ctx.reply('❌ You are not authorized to use this bot.');
            }
            const adminStatus = await new Promise(resolve => isAdmin(ctx.from.id, resolve));
            if (!adminStatus) {
                return ctx.reply('❌ Bulk SMS is for administrators only.');
            }
            await ctx.conversation.enter('bulk-sms-conversation');
        } catch (error) {
            console.error('Bulk SMS command error:', error);
            await ctx.reply('❌ Could not start bulk SMS process.');
        }
    });

    // Schedule SMS command
    bot.command('schedulesms', async ctx => {
        try {
            const user = await new Promise(resolve => getUser(ctx.from.id, resolve));
            if (!user) {
                return ctx.reply('❌ You are not authorized to use this bot.');
            }
            await ctx.conversation.enter('schedule-sms-conversation');
        } catch (error) {
            console.error('Schedule SMS command error:', error);
            await ctx.reply('❌ Could not start SMS scheduling.');
        }
    });

    // FIXED: SMS conversation command
    bot.command('smsconversation', async ctx => {
        try {
            const user = await new Promise(r => getUser(ctx.from.id, r));
            if (!user) {
                return ctx.reply('❌ You are not authorized to use this bot.');
            }
            
            const adminStatus = await new Promise(r => isAdmin(ctx.from.id, r));
            if (!adminStatus) {
                return ctx.reply('❌ This command is for administrators only.');
            }

            const args = ctx.message.text.split(' ');
            if (args.length < 2) {
                return ctx.reply(
                    '📱 <b>Usage:</b> <code>/smsconversation &lt;phone_number&gt;</code>\n\n' +
                    '<b>Example:</b> <code>/smsconversation +1234567890</code>\n\n' +
                    'This will show the SMS conversation history with the specified phone number.',
                    { parse_mode: 'HTML' }
                );
            }
            
            const phoneNumber = args[1].trim();
            if (!isValidPhoneNumber(phoneNumber)) {
                return ctx.reply('❌ Invalid phone number format. Use E.164 format: +1234567890');
            }
            
            await ctx.reply(`🔍 Searching for SMS conversation with ${phoneNumber}...`);
            await viewSmsConversation(ctx, phoneNumber);
            
        } catch (error) {
            console.error('SMS conversation command error:', error);
            await ctx.reply('❌ Error viewing SMS conversation. Please check the phone number format and try again.');
        }
    });

    // FIXED: SMS statistics command
    bot.command('smsstats', async ctx => {
        try {
            const user = await new Promise(resolve => getUser(ctx.from.id, resolve));
            if (!user) { 
                return ctx.reply('❌ You are not authorized to use this bot.');
            }

            const adminStatus = await new Promise(resolve => isAdmin(ctx.from.id, resolve));
            if (!adminStatus) {
                return ctx.reply('❌ SMS statistics are for administrators only.');
            }

            await ctx.reply('📊 Fetching SMS statistics...');
            await getSmsStats(ctx);
            
        } catch (error) {
            console.error('SMS stats command error:', error);
            await ctx.reply('❌ Error fetching SMS statistics. Please try again later.');
        }
    });

    // Script designer commands are managed through /scripts (see bot/commands/scripts.js)

    // NEW: SMS delivery status check command
    bot.command('smsstatus', async ctx => {
        try {
            const user = await new Promise(r => getUser(ctx.from.id, r));
            if (!user) {
                return ctx.reply('❌ You are not authorized to use this bot.');
            }

            const args = ctx.message.text.split(' ');
            if (args.length < 2) {
                return ctx.reply(
                    '📱 <b>Usage:</b> <code>/smsstatus &lt;message_sid&gt;</code>\n\n' +
                    '<b>Example:</b> <code>/smsstatus SM1234567890abcdef</code>\n\n' +
                    'This will show the delivery status of a specific SMS message.',
                    { parse_mode: 'HTML' }
                );
            }

            const messageSid = args[1].trim();
            
            await ctx.reply(`🔍 Checking status for message: ${messageSid}...`);
            
            try {
                const response = await axios.get(`${config.apiUrl}/api/sms/status/${messageSid}`, {
                    timeout: 10000
                });
                
                if (response.data.success) {
                    const msg = response.data.message;
                    const statusText =
                        `📱 *SMS Status Report*\n\n` +
                        `🆔 **Message SID:** \`${msg.message_sid}\`\n` +
                        `📞 **To:** ${msg.to_number || 'N/A'}\n` +
                        `📤 **From:** ${msg.from_number || 'N/A'}\n` +
                        `📊 **Status:** ${msg.status}\n` +
                        `📅 **Created:** ${new Date(msg.created_at).toLocaleString()}\n` +
                        `🔄 **Updated:** ${new Date(msg.updated_at).toLocaleString()}\n` +
                        `📝 **Message:** ${msg.body.substring(0, 100)}${msg.body.length > 100 ? '...' : ''}\n`;
                        
                    if (msg.error_code || msg.error_message) {
                        statusText += `\n❌ **Error:** ${msg.error_code} - ${msg.error_message}`;
                    }
                    
                    if (msg.ai_response) {
                        statusText += `\n🤖 **AI Response:** ${msg.ai_response.substring(0, 100)}${msg.ai_response.length > 100 ? '...' : ''}`;
                    }
                    
                    await ctx.reply(statusText, { parse_mode: 'Markdown' });
                } else {
                    await ctx.reply(`❌ ${response.data.error || 'Message not found'}`);
                }
            } catch (apiError) {
                console.error('SMS status API error:', apiError);
                await ctx.reply('❌ Error checking SMS status. Message may not exist or API is unavailable.');
            }
            
        } catch (error) {
            console.error('SMS status command error:', error);
            await ctx.reply('❌ Error checking SMS status. Please try again.');
        }
    });

    // NEW: Recent SMS messages command
    bot.command('recentsms', async ctx => {
        try {
            const user = await new Promise(r => getUser(ctx.from.id, r));
            if (!user) {
                return ctx.reply('❌ You are not authorized to use this bot.');
            }

            const adminStatus = await new Promise(r => isAdmin(ctx.from.id, r));
            if (!adminStatus) {
                return ctx.reply('❌ This command is for administrators only.');
            }
            
            const args = ctx.message.text.split(' ');
            const limit = args.length > 1 ? Math.min(parseInt(args[1]) || 10, 20) : 10;
            
            await ctx.reply(`📱 Fetching last ${limit} SMS messages...`);
            
            try {
                const response = await axios.get(`${config.apiUrl}/api/sms/messages/recent`, {
                    params: { limit },
                    timeout: 10000
                });
                
                if (response.data.success && response.data.messages.length > 0) {
                    const messages = response.data.messages;
                    
                    let messagesText = `📱 *Recent SMS Messages (${messages.length})*\n\n`;
                    
                    messages.forEach((msg, index) => {
                        const time = new Date(msg.created_at).toLocaleString();
                        const direction = msg.direction === 'inbound' ? '📨' : '📤';
                        const phone = msg.to_number || msg.from_number || 'Unknown';
                        const statusIcon = msg.status === 'delivered' ? '✅' : 
                                         msg.status === 'failed' ? '❌' : 
                                         msg.status === 'pending' ? '⏳' : '❓';
                        
                        messagesText += 
                            `${index + 1}. ${direction} ${phone} ${statusIcon}\n` +
                            `   Status: ${msg.status} | ${time}\n` +
                            `   Message: ${msg.body.substring(0, 60)}${msg.body.length > 60 ? '...' : ''}\n`;
                            
                        if (msg.error_message) {
                            messagesText += `   Error: ${msg.error_message}\n`;
                        }
                        
                        messagesText += '\n';
                    });
                    
                    messagesText += `Use /smsstatus <message_sid> for detailed status info`;
                    
                    await ctx.reply(messagesText, { parse_mode: 'Markdown' });
                } else {
                    await ctx.reply('📱 No recent SMS messages found.');
                }
                
            } catch (apiError) {
                console.error('Recent SMS API error:', apiError);
                await ctx.reply('❌ Error fetching recent SMS messages. API may be unavailable.');
            }
            
        } catch (error) {
            console.error('Recent SMS command error:', error);
            await ctx.reply('❌ Error fetching recent SMS messages.');
        }
    });
}

module.exports = {
    smsFlow,
    bulkSmsFlow,
    scheduleSmsFlow,
    registerSmsCommands,
    viewSmsConversation,
    getSmsStats,
    // Export new functions
    viewStoredSmsConversation
};
