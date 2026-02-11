const axios = require('axios');
const config = require('../config');
const { withRetry } = require('../utils/httpClient');
const { getUser, isAdmin } = require('../db/db');
const { attachHmacAuth } = require('../utils/apiAuth');
const {
  startOperation,
  ensureOperationActive,
  registerAbortController,
  OperationCancelledError,
  getCurrentOpId,
  guardAgainstCommandInterrupt
} = require('../utils/sessionState');
const {
  askOptionWithButtons,
  getOptionLabel,
  MOOD_OPTIONS,
  URGENCY_OPTIONS,
  TECH_LEVEL_OPTIONS,
  invalidatePersonaCache,
  getBusinessOptions
} = require('../utils/persona');

const {
  section,
  tipLine,
  buildLine,
  escapeMarkdown,
  emphasize
} = require('../utils/ui');

const personaApi = axios.create({
  baseURL: config.apiUrl.replace(/\/+$/, ''),
  timeout: 15000,
  headers: {
    'Content-Type': 'application/json',
    ...(config.admin?.apiToken ? { 'x-admin-token': config.admin.apiToken } : {})
  }
});

let apiOrigin;
try {
  apiOrigin = new URL(config.apiUrl).origin;
} catch (_) {
  apiOrigin = null;
}
if (apiOrigin) {
  attachHmacAuth(personaApi, {
    secret: config.apiAuth?.hmacSecret,
    allowedOrigins: [apiOrigin],
    defaultBaseUrl: config.apiUrl
  });
}

const CANCEL_KEYWORDS = new Set(['cancel', 'exit', 'stop']);

function safeEnsureActiveFactory(ctx, ensureActive) {
  if (typeof ensureActive === 'function') {
    return ensureActive;
  }
  return () => ensureOperationActive(ctx, getCurrentOpId(ctx));
}

async function personaApiRequest(ctx, ensureActive, options) {
  const controller = new AbortController();
  const release = registerAbortController(ctx, controller);
  try {
    const response = await withRetry(() => personaApi.request({
      ...options,
      signal: controller.signal
    }), options.retry || {});
    ensureActive();
    return response.data;
  } finally {
    release();
  }
}

async function fetchPersonas(ctx, ensureActive) {
  return personaApiRequest(ctx, ensureActive, { method: 'get', url: '/api/personas' });
}

async function createPersona(ctx, ensureActive, payload) {
  return personaApiRequest(ctx, ensureActive, { method: 'post', url: '/api/personas', data: payload });
}

async function updatePersona(ctx, ensureActive, slug, payload) {
  return personaApiRequest(ctx, ensureActive, { method: 'put', url: `/api/personas/${encodeURIComponent(slug)}`, data: payload });
}

async function deletePersona(ctx, ensureActive, slug) {
  return personaApiRequest(ctx, ensureActive, { method: 'delete', url: `/api/personas/${encodeURIComponent(slug)}` });
}

async function fetchCallScriptsSummary(ctx, ensureActive) {
  try {
    const data = await personaApiRequest(ctx, ensureActive, { method: 'get', url: '/api/call-scripts' });
    return Array.isArray(data.scripts) ? data.scripts : [];
  } catch (error) {
    console.error('Failed to fetch call scripts for persona command:', error?.message);
    return [];
  }
}

async function fetchSmsScriptsSummary(ctx, ensureActive) {
  try {
    const data = await personaApiRequest(ctx, ensureActive, {
      method: 'get',
      url: '/api/sms/scripts',
      params: { include_builtins: true }
    });
    const custom = Array.isArray(data.scripts) ? data.scripts : [];
    const builtin = Array.isArray(data.builtin)
      ? data.builtin
      : Array.isArray(data.available_scripts)
        ? data.available_scripts.map((name) => ({ name, is_builtin: true }))
        : [];
    return [...custom, ...builtin];
  } catch (error) {
    console.error('Failed to fetch SMS scripts for persona command:', error?.message);
    return [];
  }
}

function styledSection(ctx, title, lines) {
  const content = Array.isArray(lines) ? lines : [lines];
  return ctx.reply(section(title, content));
}

function styledAlert(ctx, text, title = '⚠️ Attention') {
  return ctx.reply(section(title, [text]));
}

async function promptForText(conversation, ctx, message, options = {}) {
  const {
    required = true,
    allowSkip = false,
    defaultValue = null,
    parser = (value) => value,
    ensureActive
  } = options;

  const safeEnsureActive = safeEnsureActiveFactory(ctx, ensureActive);
  const hints = [];
  if (defaultValue) {
    hints.push(`Current: ${defaultValue}`);
  }
  if (allowSkip) {
    hints.push('Type skip to keep current value');
  }
  hints.push('Type cancel to abort');

  const promptText = hints.length ? `${message} (${hints.join(' | ')})` : message;

  while (true) {
    await styledSection(ctx, '📝 Provide Input', [promptText]);

    const update = await conversation.wait();
    safeEnsureActive();

    const text = update?.message?.text?.trim();
    if (text) {
      await guardAgainstCommandInterrupt(ctx, text);
    }
    if (!text) {
      if (required) {
        await styledAlert(ctx, 'Please provide a response or type cancel.');
        continue;
      }
      return '';
    }

    const lower = text.toLowerCase();
    if (CANCEL_KEYWORDS.has(lower)) {
      throw new OperationCancelledError('Persona flow cancelled by user');
    }

    if (allowSkip && lower === 'skip') {
      return undefined;
    }

    try {
      return parser(text);
    } catch (error) {
      await styledAlert(ctx, error.message || 'Invalid value supplied.');
    }
  }
}

async function askYesNo(conversation, ctx, question, ensureActive) {
  const choice = await askOptionWithButtons(
    conversation,
    ctx,
    question,
    [
      { id: 'yes', label: '✅ Yes' },
      { id: 'no', label: '❌ No' }
    ],
    { prefix: 'persona-confirm', columns: 2, ensureActive: safeEnsureActiveFactory(ctx, ensureActive) }
  );

  return choice.id === 'yes';
}

function filterToneOptions(options) {
  return options.filter((option) => option.id !== 'auto');
}

function mapMoodSelection(selection, fallback = null) {
  return selection?.id === 'auto' ? fallback : selection?.id || null;
}

async function chooseTone(conversation, ctx, prompt, options, ensureActive, fallback = null) {
  const choice = await askOptionWithButtons(
    conversation,
    ctx,
    prompt,
    [
      ...options.map((option) => ({
        ...option,
        id: option.id,
        label: option.label
      })),
      { id: 'skip', label: '⏭️ Skip' }
    ],
    { prefix: 'persona-tone', columns: 2, ensureActive: safeEnsureActiveFactory(ctx, ensureActive) }
  );

  if (choice.id === 'skip') {
    return undefined;
  }
  return choice.id;
}

async function createPersonaFlow(conversation, ctx, ensureActive) {
  await ctx.reply(section('🆕 Persona Studio', ['Create a new persona profile. Type cancel anytime.']));

  const slug = await promptForText(
    conversation,
    ctx,
    'Enter a unique slug (lowercase letters, numbers, hyphen, underscore):',
    {
      parser: (value) => {
        const trimmed = value.trim().toLowerCase();
        if (!/^[a-z0-9_-]{3,64}$/.test(trimmed)) {
          throw new Error('Slug must be 3-64 characters (lowercase, digits, hyphen, underscore).');
        }
        return trimmed;
      },
      ensureActive
    }
  );

  const label = await promptForText(
    conversation,
    ctx,
    'Display label for this persona:',
    {
      parser: (value) => {
        const trimmed = value.trim();
        if (!trimmed.length) {
          throw new Error('Label cannot be empty.');
        }
        return trimmed;
      },
      ensureActive
    }
  );

  const description = await promptForText(
    conversation,
    ctx,
    'Optional description (or type skip):',
    { allowSkip: true, required: false, ensureActive }
  );

  const defaultPurpose = await promptForText(
    conversation,
    ctx,
    'Default purpose keyword (e.g., general, support). Type skip to leave unset:',
    { allowSkip: true, required: false, ensureActive }
  );

  const toneOptions = filterToneOptions(MOOD_OPTIONS);
  const defaultEmotion = await chooseTone(
    conversation,
    ctx,
    'Select default tone (or choose Skip):',
    toneOptions,
    ensureActive
  );

  const urgencyOptions = URGENCY_OPTIONS.filter((option) => option.id !== 'auto');
  const defaultUrgency = await chooseTone(
    conversation,
    ctx,
    'Select default urgency (or choose Skip):',
    urgencyOptions,
    ensureActive
  );

  const techOptions = TECH_LEVEL_OPTIONS.filter((option) => option.id !== 'auto');
  const defaultTech = await chooseTone(
    conversation,
    ctx,
    'Select default technical level (or choose Skip):',
    techOptions,
    ensureActive
  );

  const callScripts = await fetchCallScriptsSummary(ctx, ensureActive);
  let callScriptId = null;
  if (callScripts.length > 0) {
    const options = callScripts.slice(0, 10).map((script) => ({
      id: script.id.toString(),
      label: `📞 ${script.name}`
    }));
    options.push({ id: 'skip', label: '⏭️ Skip' });

    const selection = await askOptionWithButtons(
      conversation,
      ctx,
      'Select a default call script (or skip):',
      options,
      { prefix: 'persona-call-script', columns: 1, ensureActive: safeEnsureActiveFactory(ctx, ensureActive) }
    );

    if (selection.id !== 'skip') {
      callScriptId = Number(selection.id);
    }
  }

  const smsScripts = await fetchSmsScriptsSummary(ctx, ensureActive);
  let smsScriptName = null;
  if (smsScripts.length > 0) {
    const options = smsScripts.slice(0, 10).map((script) => ({
      id: script.name,
      label: `${script.is_builtin ? '📦' : '📝'} ${script.name}`
    }));
    options.push({ id: 'skip', label: '⏭️ Skip' });

    const selection = await askOptionWithButtons(
      conversation,
      ctx,
      'Select a default SMS script (or skip):',
      options,
      { prefix: 'persona-sms-script', columns: 1, ensureActive: safeEnsureActiveFactory(ctx, ensureActive) }
    );

    if (selection.id !== 'skip') {
      smsScriptName = selection.id;
    }
  }

  const purposes = [
    {
      id: (defaultPurpose || 'general').toLowerCase(),
      label,
      defaultEmotion: defaultEmotion || null,
      defaultUrgency: defaultUrgency || null,
      defaultTechnicalLevel: defaultTech || null
    }
  ];

  const payload = {
    slug,
    label,
    description: description === undefined ? null : description || null,
    purposes,
    default_purpose: defaultPurpose || purposes[0].id,
    default_emotion: defaultEmotion || null,
    default_urgency: defaultUrgency || null,
    default_technical_level: defaultTech || null,
    call_script_id: callScriptId,
    sms_script_name: smsScriptName,
    created_by: ctx.from.id.toString()
  };

  try {
    const response = await createPersona(ctx, ensureActive, payload);
    await ctx.reply(section('✅ Persona Created', [
      `Persona *${response.persona.label}* is ready.`,
      'Use /persona to manage or edit it anytime.'
    ]));
    invalidatePersonaCache();
    await getBusinessOptions(true);
  } catch (error) {
    console.error('Failed to create persona:', error?.response?.data || error.message);
    const details = error.response?.data?.error || error.message;
    await styledAlert(ctx, `Failed to create persona: ${details}`);
  }
}

async function selectCustomPersona(conversation, ctx, ensureActive, personas) {
  if (!personas.length) {
    await styledSection(ctx, 'ℹ️ Persona Library', ['No custom personas available yet.']);
    return null;
  }

  const options = personas.map((persona) => ({
    id: persona.slug,
    label: `🧩 ${persona.label}`
  }));
  options.push({ id: 'cancel', label: '❌ Cancel' });

  const selection = await askOptionWithButtons(
    conversation,
    ctx,
    'Select a persona:',
    options,
    { prefix: 'persona-select', columns: 1, ensureActive: safeEnsureActiveFactory(ctx, ensureActive) }
  );

  if (selection.id === 'cancel') {
    return null;
  }

  return selection.id;
}

async function editPersonaFlow(conversation, ctx, ensureActive, persona) {
  await styledSection(ctx, '✏️ Persona Editor', [
    `Editing persona *${persona.label}* (slug: ${persona.slug}).`
  ]);

  const updates = {};
  const description = await promptForText(
    conversation,
    ctx,
    'Update description (or type skip):',
    { allowSkip: true, required: false, defaultValue: persona.description || '', ensureActive }
  );
  if (description !== undefined) {
    updates.description = description || null;
  }

  const toneOptions = filterToneOptions(MOOD_OPTIONS);
  const tonePrompt = `Select default tone (current: ${persona.default_emotion || 'none'}) or Skip:`;
  const defaultEmotion = await chooseTone(conversation, ctx, tonePrompt, toneOptions, ensureActive, persona.default_emotion);
  if (defaultEmotion !== undefined) {
    updates.default_emotion = defaultEmotion || null;
  }

  const urgencyPrompt = `Select default urgency (current: ${persona.default_urgency || 'none'}) or Skip:`;
  const defaultUrgency = await chooseTone(conversation, ctx, urgencyPrompt, URGENCY_OPTIONS.filter((option) => option.id !== 'auto'), ensureActive, persona.default_urgency);
  if (defaultUrgency !== undefined) {
    updates.default_urgency = defaultUrgency || null;
  }

  const techPrompt = `Select default technical level (current: ${persona.default_technical_level || 'none'}) or Skip:`;
  const defaultTech = await chooseTone(conversation, ctx, techPrompt, TECH_LEVEL_OPTIONS.filter((option) => option.id !== 'auto'), ensureActive, persona.default_technical_level);
  if (defaultTech !== undefined) {
    updates.default_technical_level = defaultTech || null;
  }

  const callScripts = await fetchCallScriptsSummary(ctx, ensureActive);
  if (callScripts.length > 0) {
    const options = callScripts.slice(0, 10).map((script) => ({
      id: script.id.toString(),
      label: `📞 ${script.name}`
    }));
    options.push({ id: 'skip', label: '⏭️ Skip' });
    options.push({ id: 'clear', label: '🗑️ Clear' });

    const selection = await askOptionWithButtons(
      conversation,
      ctx,
      `Select default call script (current: ${persona.call_script_id || 'none'})`,
      options,
      { prefix: 'persona-edit-call', columns: 1, ensureActive: safeEnsureActiveFactory(ctx, ensureActive) }
    );

    if (selection.id === 'clear') {
      updates.call_script_id = null;
    } else if (selection.id !== 'skip') {
      updates.call_script_id = Number(selection.id);
    }
  }

  const smsScripts = await fetchSmsScriptsSummary(ctx, ensureActive);
  if (smsScripts.length > 0) {
    const options = smsScripts.slice(0, 10).map((script) => ({
      id: script.name,
      label: `${script.is_builtin ? '📦' : '📝'} ${script.name}`
    }));
    options.push({ id: 'skip', label: '⏭️ Skip' });
    options.push({ id: 'clear', label: '🗑️ Clear' });

    const selection = await askOptionWithButtons(
      conversation,
      ctx,
      `Select default SMS script (current: ${persona.sms_script_name || 'none'})`,
      options,
      { prefix: 'persona-edit-sms', columns: 1, ensureActive: safeEnsureActiveFactory(ctx, ensureActive) }
    );

    if (selection.id === 'clear') {
      updates.sms_script_name = null;
    } else if (selection.id !== 'skip') {
      updates.sms_script_name = selection.id;
    }
  }

  if (!Object.keys(updates).length) {
    await styledAlert(ctx, 'No changes made.');
    return;
  }

  updates.updated_by = ctx.from.id.toString();

  try {
    const response = await updatePersona(ctx, ensureActive, persona.slug, updates);
    await styledSection(ctx, '✅ Persona Updated', [
      `Persona *${response.persona.label}* saved successfully.`
    ]);
    invalidatePersonaCache();
    await getBusinessOptions(true);
  } catch (error) {
    console.error('Failed to update persona:', error?.response?.data || error.message);
    const details = error.response?.data?.error || error.message;
    await styledAlert(ctx, `Failed to update persona: ${details}`);
  }
}

async function deletePersonaFlow(conversation, ctx, ensureActive, persona) {
  const confirmed = await askYesNo(
    conversation,
    ctx,
    `Delete persona *${persona.label}*?`,
    ensureActive
  );

  if (!confirmed) {
    await styledAlert(ctx, 'Deletion cancelled.');
    return;
  }

  try {
    await deletePersona(ctx, ensureActive, persona.slug);
    await styledSection(ctx, '🗑️ Persona Deleted', [
      `Persona *${persona.label}* removed from the registry.`
    ]);
    invalidatePersonaCache();
    await getBusinessOptions(true);
  } catch (error) {
    console.error('Failed to delete persona:', error?.response?.data || error.message);
    const details = error.response?.data?.error || error.message;
    await styledAlert(ctx, `Failed to delete persona: ${details}`);
  }
}

async function personaFlow(conversation, ctx) {
  const opId = startOperation(ctx, 'persona');
  const ensureActive = () => ensureOperationActive(ctx, opId);

  try {
    const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
    ensureActive();
    if (!user) {
      await styledAlert(ctx, 'You are not authorized to use this bot.');
      return;
    }

    const adminStatus = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
    ensureActive();
    if (!adminStatus) {
      await styledAlert(ctx, 'This command is for administrators only.');
      return;
    }

    await getBusinessOptions(true);

    let active = true;
    while (active) {
      const choice = await askOptionWithButtons(
        conversation,
        ctx,
        '🎭 *Persona Manager*\nWhat would you like to do?',
        [
          { id: 'list', label: '📋 List personas' },
          { id: 'create', label: '➕ Create persona' },
          { id: 'edit', label: '✏️ Edit persona' },
          { id: 'delete', label: '🗑️ Delete persona' },
          { id: 'cache', label: '🔄 Refresh cache' },
          { id: 'exit', label: '⬅️ Exit' }
        ],
        { prefix: 'persona-menu', columns: 2, ensureActive }
      );

      switch (choice.id) {
        case 'list': {
          try {
            const data = await fetchPersonas(ctx, ensureActive);
            const builtin = data.builtin || [];
            const custom = data.custom || [];

            const lines = [
              `Built-in (${builtin.length}):`,
              ...builtin.map((persona) => `• ${persona.label} (${persona.id})`)
            ];
            if (custom.length) {
              lines.push('');
              lines.push(`Custom (${custom.length}):`);
              lines.push(...custom.map((persona) => `• ${persona.label} (${persona.slug})`));
            } else {
              lines.push('');
              lines.push('No custom personas yet.');
            }

            await ctx.reply(section('🎭 Persona Profiles', lines));
            } catch (error) {
            console.error('Failed to list personas:', error?.response?.data || error.message);
            const details = error.response?.data?.error || error.message;
            await styledAlert(ctx, `Failed to list personas: ${details}`);
          }
          break;
        }
        case 'create':
          await createPersonaFlow(conversation, ctx, ensureActive);
          break;
        case 'edit': {
          try {
            const data = await fetchPersonas(ctx, ensureActive);
            const custom = data.custom || [];
            const slug = await selectCustomPersona(conversation, ctx, ensureActive, custom);
            if (!slug) {
              await styledAlert(ctx, 'Edit cancelled.');
              break;
            }
            const persona = custom.find((profile) => profile.slug === slug);
            if (!persona) {
              await styledAlert(ctx, 'Persona not found. Try refreshing the cache.');
              break;
            }
            await editPersonaFlow(conversation, ctx, ensureActive, persona);
          } catch (error) {
            if (error instanceof OperationCancelledError) {
              throw error;
            }
            console.error('Failed during persona edit:', error?.response?.data || error.message);
            const details = error.response?.data?.error || error.message;
            await styledAlert(ctx, `Failed to edit persona: ${details}`);
          }
          break;
        }
        case 'delete': {
          try {
            const data = await fetchPersonas(ctx, ensureActive);
            const custom = data.custom || [];
            const slug = await selectCustomPersona(conversation, ctx, ensureActive, custom);
            if (!slug) {
              await styledAlert(ctx, 'Deletion cancelled.');
              break;
            }
            const persona = custom.find((profile) => profile.slug === slug);
            if (!persona) {
              await styledAlert(ctx, 'Persona not found. Try refreshing the cache.');
              break;
            }
            await deletePersonaFlow(conversation, ctx, ensureActive, persona);
          } catch (error) {
            if (error instanceof OperationCancelledError) {
              throw error;
            }
            console.error('Failed during persona deletion:', error?.response?.data || error.message);
            const details = error.response?.data?.error || error.message;
            await styledAlert(ctx, `Failed to delete persona: ${details}`);
          }
          break;
        }
        case 'cache':
          invalidatePersonaCache();
          await getBusinessOptions(true);
          await styledSection(ctx, '🔄 Persona Cache', ['Persona cache refreshed.']);
          break;
        case 'exit':
          active = false;
          break;
        default:
          break;
      }
    }

    await styledSection(ctx, '🏁 Persona Manager', ['Closed — come back anytime.']);
  } catch (error) {
    if (error instanceof OperationCancelledError) {
      console.log('Persona flow cancelled:', error.message);
      return;
    }
    console.error('Persona flow error:', error);
    await styledAlert(ctx, 'An error occurred in persona manager. Please try again.');
  } finally {
    if (ctx.session?.currentOp?.id === opId) {
      ctx.session.currentOp = null;
    }
  }
}

function registerPersonaCommand(bot) {
  bot.command('persona', async (ctx) => {
    try {
      const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
      if (!user) {
        return ctx.reply('❌ You are not authorized to use this bot.');
      }

      const adminStatus = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
      if (!adminStatus) {
        return ctx.reply('❌ This command is for administrators only.');
      }

      await ctx.conversation.enter('persona-conversation');
    } catch (error) {
      console.error('Failed to start persona conversation:', error);
      await styledAlert(ctx, 'Unable to start persona manager. Please try again.');
    }
  });
}

module.exports = {
  personaFlow,
  registerPersonaCommand
};
