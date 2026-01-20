const httpClient = require('../utils/httpClient');
const { InlineKeyboard } = require('grammy');
const config = require('../config');
const {
  getUser,
  isAdmin,
  saveScriptVersion,
  listScriptVersions,
  getScriptVersion
} = require('../db/db');
const {
  startOperation,
  ensureOperationActive,
  registerAbortController,
  guardAgainstCommandInterrupt
} = require('../utils/sessionState');
const { section, buildLine, tipLine, escapeMarkdown, emphasize, activateMenuMessage, renderMenu } = require('../utils/ui');
const { buildCallbackData } = require('../utils/actions');
const { askOptionWithButtons } = require('../utils/persona');

function normalizeEmail(value) {
  return String(value || '').trim().toLowerCase();
}

function extractEmailTemplateVariables(text = '') {
  if (!text) return [];
  const matches = text.match(/{{\\s*([\\w.-]+)\\s*}}/g) || [];
  const vars = new Set();
  matches.forEach((match) => {
    const cleaned = match.replace(/{{|}}/g, '').trim();
    if (cleaned) vars.add(cleaned);
  });
  return Array.from(vars);
}

function buildRequiredVars(subject = '', html = '', text = '') {
  const required = new Set();
  extractEmailTemplateVariables(subject).forEach((v) => required.add(v));
  extractEmailTemplateVariables(html).forEach((v) => required.add(v));
  extractEmailTemplateVariables(text).forEach((v) => required.add(v));
  return Array.from(required);
}

function validateEmailTemplatePayload({ templateId, subject, html, text }) {
  const errors = [];
  const warnings = [];
  if (!templateId) {
    errors.push('Template ID is required.');
  } else if (!/^[a-zA-Z0-9_-]+$/.test(templateId)) {
    warnings.push('Template ID should use letters, numbers, underscores, or dashes.');
  }
  if (!subject) {
    errors.push('Subject is required.');
  } else if (subject.length < 3) {
    warnings.push('Subject is very short; consider a clearer subject.');
  } else if (subject.length > 140) {
    warnings.push('Subject is long; consider keeping it under 140 characters.');
  }
  if (!html && !text) {
    errors.push('Provide at least one of HTML or text.');
  }
  const requiredVars = buildRequiredVars(subject || '', html || '', text || '');
  return { errors, warnings, requiredVars };
}

function isValidEmail(value) {
  const email = normalizeEmail(value);
  if (!email || !email.includes('@')) return false;
  const parts = email.split('@');
  if (parts.length !== 2) return false;
  if (!parts[0] || !parts[1]) return false;
  return true;
}

function parseJsonInput(text) {
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch (error) {
    return null;
  }
}

function parseRecipientsInput(text) {
  const value = String(text || '').trim();
  if (!value) {
    return { recipients: [], invalid: ['(empty input)'], mode: 'list' };
  }
  if (value.startsWith('[')) {
    const parsed = parseJsonInput(value);
    if (!Array.isArray(parsed)) {
      return { recipients: [], invalid: ['JSON must be an array'], mode: 'json' };
    }
    const recipients = [];
    const invalid = [];
    parsed.forEach((entry) => {
      if (typeof entry === 'string') {
        const email = normalizeEmail(entry);
        if (isValidEmail(email)) {
          recipients.push({ email });
        } else {
          invalid.push(entry);
        }
        return;
      }
      if (entry && typeof entry === 'object') {
        const email = normalizeEmail(entry.email || entry.to);
        if (!isValidEmail(email)) {
          invalid.push(entry.email || entry.to || 'unknown');
          return;
        }
        recipients.push({
          email,
          variables: entry.variables || {},
          metadata: entry.metadata || {}
        });
        return;
      }
      invalid.push(String(entry));
    });
    return { recipients, invalid, mode: 'json' };
  }

  const rawList = value.split(/[\n,]+/).map((item) => item.trim()).filter(Boolean);
  const recipients = [];
  const invalid = [];
  rawList.forEach((entry) => {
    const email = normalizeEmail(entry.split(/\s+/)[0]);
    if (isValidEmail(email)) {
      recipients.push({ email });
    } else {
      invalid.push(entry);
    }
  });
  return { recipients, invalid, mode: 'list' };
}

async function fetchEmailTemplates(ctx) {
  const response = await guardedGet(ctx, `${config.apiUrl}/email/templates`);
  return response.data?.templates || [];
}

async function fetchEmailTemplate(ctx, templateId) {
  const response = await guardedGet(ctx, `${config.apiUrl}/email/templates/${encodeURIComponent(templateId)}`);
  return response.data?.template;
}

async function createEmailTemplate(ctx, payload) {
  const response = await guardedPost(ctx, `${config.apiUrl}/email/templates`, payload);
  return response.data?.template;
}

async function updateEmailTemplate(ctx, templateId, payload) {
  const response = await guardedPut(ctx, `${config.apiUrl}/email/templates/${encodeURIComponent(templateId)}`, payload);
  return response.data?.template;
}

async function deleteEmailTemplate(ctx, templateId) {
  await httpClient.del(ctx, `${config.apiUrl}/email/templates/${encodeURIComponent(templateId)}`, { timeout: 20000 });
}

async function downloadHtmlFromTelegram(ctx, fileId) {
  const file = await ctx.api.getFile(fileId);
  if (!file?.file_path) {
    throw new Error('Could not resolve file path.');
  }
  const url = `https://api.telegram.org/file/bot${config.botToken}/${file.file_path}`;
  const response = await httpClient.get(ctx, url, { timeout: 20000 });
  return response.data;
}

async function promptHtmlBody(conversation, ctx, ensureActive) {
  const choice = await askOptionWithButtons(
    conversation,
    ctx,
    '💡 *HTML body*\nChoose how to provide HTML.',
    [
      { id: 'skip', label: 'Skip' },
      { id: 'paste', label: 'Paste HTML' },
      { id: 'upload', label: 'Upload .html file' }
    ],
    { prefix: 'email-html-src', columns: 2, ensureActive }
  );
  if (!choice || choice.id === 'skip') {
    return null;
  }
  if (choice.id === 'paste') {
    await ctx.reply(section('🧩 HTML Body', ['Paste HTML content.']), { parse_mode: 'Markdown' });
    const update = await conversation.wait();
    ensureActive();
    return update?.message?.text?.trim() || null;
  }
  await ctx.reply(section('📎 Upload HTML', ['Send the .html file now.']), { parse_mode: 'Markdown' });
  const upload = await conversation.wait();
  ensureActive();
  const doc = upload?.message?.document;
  if (!doc?.file_id) {
    await ctx.reply('❌ No document received.');
    return null;
  }
  const filename = doc.file_name || '';
  if (!filename.toLowerCase().endsWith('.html') && doc.mime_type !== 'text/html') {
    await ctx.reply('❌ Please upload a valid .html file.');
    return null;
  }
  return downloadHtmlFromTelegram(ctx, doc.file_id);
}

async function confirmAction(conversation, ctx, prompt, ensureActive) {
  const choice = await askOptionWithButtons(
    conversation,
    ctx,
    prompt,
    [
      { id: 'yes', label: '✅ Yes' },
      { id: 'no', label: '❌ No' }
    ],
    { prefix: 'confirm', columns: 2, ensureActive }
  );
  return choice?.id === 'yes';
}

function parseRequiredVars(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value;
  try {
    return JSON.parse(value);
  } catch (_) {
    return [];
  }
}

function buildEmailTemplateSnapshot(template = {}) {
  return {
    template_id: template.template_id ?? null,
    subject: template.subject ?? null,
    html: template.html ?? null,
    text: template.text ?? null,
    required_vars: template.required_vars ?? null
  };
}

async function storeEmailTemplateVersion(template, ctx) {
  if (!template?.template_id) return;
  try {
    const payload = buildEmailTemplateSnapshot(template);
    await saveScriptVersion(template.template_id, 'email', payload, ctx.from?.id?.toString?.());
  } catch (error) {
    console.warn('Failed to store email template version:', error?.message || error);
  }
}

function formatEmailTemplateSummary(template) {
  const requiredVars = parseRequiredVars(template.required_vars);
  const varsLine = requiredVars.length ? requiredVars.join(', ') : '—';
  return section('📧 Email Template', [
    buildLine('🆔', 'ID', escapeMarkdown(template.template_id || '—')),
    buildLine('🧾', 'Subject', escapeMarkdown(template.subject || '—')),
    buildLine('🧩', 'Variables', escapeMarkdown(varsLine)),
    buildLine('📄', 'Has Text', template.text ? 'Yes' : 'No'),
    buildLine('🖼️', 'Has HTML', template.html ? 'Yes' : 'No'),
    buildLine('📅', 'Updated', formatTimestamp(template.updated_at || template.created_at))
  ]);
}

async function createEmailTemplateFlow(conversation, ctx, ensureActive) {
  await ctx.reply(section('🆕 Create Email Template', [
    'Provide a template ID (e.g., welcome_email).'
  ]), { parse_mode: 'Markdown' });
  const idMsg = await conversation.wait();
  ensureActive();
  const templateId = idMsg?.message?.text?.trim();
  if (!templateId) {
    await ctx.reply('❌ Template ID is required.');
    return;
  }

  await ctx.reply(section('🧾 Subject', ['Enter the email subject line.']), { parse_mode: 'Markdown' });
  const subjectMsg = await conversation.wait();
  ensureActive();
  const subject = subjectMsg?.message?.text?.trim();
  if (!subject) {
    await ctx.reply('❌ Subject is required.');
    return;
  }

  await ctx.reply(section('📝 Text Body', ['Enter the plain text body (or type skip).']), { parse_mode: 'Markdown' });
  const textMsg = await conversation.wait();
  ensureActive();
  let textBody = textMsg?.message?.text?.trim();
  if (textBody && textBody.toLowerCase() === 'skip') {
    textBody = null;
  }

  const htmlBody = await promptHtmlBody(conversation, ctx, ensureActive);

  const validation = validateEmailTemplatePayload({
    templateId,
    subject,
    html: htmlBody,
    text: textBody
  });
  if (validation.errors.length) {
    await ctx.reply(section('❌ Template validation failed', validation.errors), { parse_mode: 'Markdown' });
    return;
  }
  if (validation.requiredVars.length) {
    const varsLine = validation.requiredVars.slice(0, 12).join(', ');
    await ctx.reply(section('🧩 Detected variables', [varsLine]), { parse_mode: 'Markdown' });
  }
  if (validation.warnings.length) {
    await ctx.reply(section('⚠️ Template warnings', validation.warnings), { parse_mode: 'Markdown' });
    const proceed = await confirmAction(conversation, ctx, 'Continue with these warnings?', ensureActive);
    if (!proceed) {
      await ctx.reply('ℹ️ Template creation cancelled.');
      return;
    }
  }

  const template = await createEmailTemplate(ctx, {
    template_id: templateId,
    subject,
    text: textBody || undefined,
    html: htmlBody || undefined
  });

  await storeEmailTemplateVersion(template, ctx);
  await ctx.reply(formatEmailTemplateSummary(template), { parse_mode: 'Markdown' });
}

async function editEmailTemplateFlow(conversation, ctx, template, ensureActive) {
  await ctx.reply(section('✏️ Update Template', [
    'Type skip to keep the current value.'
  ]), { parse_mode: 'Markdown' });

  await ctx.reply(section('🧾 Subject', [`Current: ${template.subject || '—'}`]), { parse_mode: 'Markdown' });
  const subjectMsg = await conversation.wait();
  ensureActive();
  let subject = subjectMsg?.message?.text?.trim();
  if (subject && subject.toLowerCase() === 'skip') subject = undefined;

  await ctx.reply(section('📝 Text Body', ['Paste new text or type skip.']), { parse_mode: 'Markdown' });
  const textMsg = await conversation.wait();
  ensureActive();
  let textBody = textMsg?.message?.text?.trim();
  if (textBody && textBody.toLowerCase() === 'skip') textBody = undefined;

  const htmlBody = await promptHtmlBody(conversation, ctx, ensureActive);
  const updates = {};
  if (subject !== undefined) updates.subject = subject;
  if (textBody !== undefined) updates.text = textBody;
  if (htmlBody !== null) updates.html = htmlBody;

  if (!Object.keys(updates).length) {
    await ctx.reply('ℹ️ No changes made.');
    return;
  }

  const proposedSubject = subject !== undefined ? subject : template.subject;
  const proposedText = textBody !== undefined ? textBody : template.text;
  const proposedHtml = htmlBody !== null ? htmlBody : template.html;
  const validation = validateEmailTemplatePayload({
    templateId: template.template_id,
    subject: proposedSubject,
    html: proposedHtml,
    text: proposedText
  });
  if (validation.errors.length) {
    await ctx.reply(section('❌ Template validation failed', validation.errors), { parse_mode: 'Markdown' });
    return;
  }
  if (validation.requiredVars.length) {
    const varsLine = validation.requiredVars.slice(0, 12).join(', ');
    await ctx.reply(section('🧩 Detected variables', [varsLine]), { parse_mode: 'Markdown' });
  }
  if (validation.warnings.length) {
    await ctx.reply(section('⚠️ Template warnings', validation.warnings), { parse_mode: 'Markdown' });
    const proceed = await confirmAction(conversation, ctx, 'Continue with these warnings?', ensureActive);
    if (!proceed) {
      await ctx.reply('ℹ️ Update cancelled.');
      return;
    }
  }

  const updated = await updateEmailTemplate(ctx, template.template_id, updates);
  await storeEmailTemplateVersion(updated, ctx);
  await ctx.reply(formatEmailTemplateSummary(updated), { parse_mode: 'Markdown' });
}

async function selectEmailTemplateId(conversation, ctx, ensureActive) {
  let templates = [];
  try {
    templates = await fetchEmailTemplates(ctx);
    ensureActive();
  } catch (error) {
    await ctx.reply('⚠️ Unable to load templates. Enter the script_id manually.');
    return null;
  }
  if (!templates.length) {
    return null;
  }
  const options = templates.map((tpl) => ({
    id: tpl.template_id,
    label: `📄 ${tpl.template_id}`
  }));
  options.push({ id: 'manual', label: '✍️ Enter script_id manually' });
  const selection = await askOptionWithButtons(
    conversation,
    ctx,
    '📧 Choose a saved email template.',
    options,
    { prefix: 'email-template-pick', columns: 1, ensureActive }
  );
  if (!selection || selection.id === 'manual') {
    return null;
  }
  return selection.id;
}

async function deleteEmailTemplateFlow(conversation, ctx, template) {
  const confirmed = await askOptionWithButtons(
    conversation,
    ctx,
    `Delete template *${escapeMarkdown(template.template_id)}*?`,
    [
      { id: 'no', label: 'Cancel' },
      { id: 'yes', label: 'Delete' }
    ],
    { prefix: 'email-template-delete', columns: 2 }
  );
  if (confirmed?.id !== 'yes') {
    await ctx.reply('Deletion cancelled.');
    return;
  }
  await storeEmailTemplateVersion(template, ctx);
  await deleteEmailTemplate(ctx, template.template_id);
  await ctx.reply(`🗑️ Template *${escapeMarkdown(template.template_id)}* deleted.`, { parse_mode: 'Markdown' });
}

async function previewEmailTemplate(conversation, ctx, template, ensureActive) {
  const variables = await promptVariables(conversation, ctx, ensureActive);
  const previewResponse = await guardedPost(ctx, `${config.apiUrl}/email/preview`, {
    script_id: template.template_id,
    variables
  });
  if (!previewResponse.data?.success) {
    await ctx.reply('❌ Preview failed.');
    return;
  }
  const preview = previewResponse.data;
  await ctx.reply(section('🔍 Preview', [
    buildLine('🧾', 'Subject', escapeMarkdown(preview.subject || '—')),
    buildLine('📄', 'Text', escapeMarkdown((preview.text || '').slice(0, 140) || '—'))
  ]), { parse_mode: 'Markdown' });
}

function formatEmailVersionSummary(version) {
  const createdAt = formatTimestamp(version.created_at);
  return `#${version.version_number} • ${createdAt}${version.created_by ? ` • ${escapeMarkdown(version.created_by)}` : ''}`;
}

async function showEmailTemplateVersions(conversation, ctx, template, ensureActive) {
  const versions = await listScriptVersions(template.template_id, 'email', 8);
  ensureActive();
  if (!versions.length) {
    await ctx.reply('ℹ️ No saved versions yet. Versions are stored on edit/delete.');
    return;
  }
  const options = versions.map((version) => ({
    id: String(version.version_number),
    label: `🗂️ ${formatEmailVersionSummary(version)}`
  }));
  options.push({ id: 'back', label: '⬅️ Back' });
  const selection = await askOptionWithButtons(
    conversation,
    ctx,
    '🗂️ *Email Template Versions*\nChoose a version to restore.',
    options,
    { prefix: 'email-template-version', columns: 1, ensureActive }
  );
  if (!selection || selection.id === 'back') {
    return;
  }
  const versionNumber = Number(selection.id);
  if (Number.isNaN(versionNumber)) {
    await ctx.reply('❌ Invalid version selected.');
    return;
  }
  const version = await getScriptVersion(template.template_id, 'email', versionNumber);
  ensureActive();
  if (!version?.payload) {
    await ctx.reply('❌ Version payload not found.');
    return;
  }
  const confirmRestore = await confirmAction(
    conversation,
    ctx,
    `Restore version #${versionNumber} for *${escapeMarkdown(template.template_id)}*?`,
    ensureActive
  );
  if (!confirmRestore) {
    await ctx.reply('ℹ️ Restore cancelled.');
    return;
  }
  await storeEmailTemplateVersion(template, ctx);
  const restored = await updateEmailTemplate(ctx, template.template_id, {
    subject: version.payload.subject,
    html: version.payload.html,
    text: version.payload.text
  });
  await storeEmailTemplateVersion(restored, ctx);
  await ctx.reply(`✅ Restored template to version #${versionNumber}.`, { parse_mode: 'Markdown' });
}

async function cloneEmailTemplateFlow(conversation, ctx, template, ensureActive) {
  await ctx.reply(section('🧬 Clone Template', [
    `Enter a new template ID for the clone of ${escapeMarkdown(template.template_id)}.`
  ]), { parse_mode: 'Markdown' });
  const update = await conversation.wait();
  ensureActive();
  const newId = update?.message?.text?.trim();
  if (!newId) {
    await ctx.reply('❌ Template ID is required.');
    return;
  }
  const validation = validateEmailTemplatePayload({
    templateId: newId,
    subject: template.subject,
    html: template.html,
    text: template.text
  });
  if (validation.errors.length) {
    await ctx.reply(section('❌ Template validation failed', validation.errors), { parse_mode: 'Markdown' });
    return;
  }
  if (validation.requiredVars.length) {
    const varsLine = validation.requiredVars.slice(0, 12).join(', ');
    await ctx.reply(section('🧩 Detected variables', [varsLine]), { parse_mode: 'Markdown' });
  }
  if (validation.warnings.length) {
    await ctx.reply(section('⚠️ Template warnings', validation.warnings), { parse_mode: 'Markdown' });
  }
  const cloned = await createEmailTemplate(ctx, {
    template_id: newId,
    subject: template.subject,
    html: template.html || undefined,
    text: template.text || undefined
  });
  await storeEmailTemplateVersion(cloned, ctx);
  await ctx.reply(`✅ Template cloned as *${escapeMarkdown(cloned.template_id)}*.`, { parse_mode: 'Markdown' });
}

async function exportEmailTemplate(ctx, template) {
  const payload = {
    template_id: template.template_id,
    subject: template.subject,
    text: template.text || null,
    html: template.html || null,
    required_vars: parseRequiredVars(template.required_vars)
  };
  const text = [
    '```json',
    JSON.stringify(payload, null, 2),
    '```'
  ].join('\n');
  await ctx.reply(text, { parse_mode: 'Markdown' });
}

async function importEmailTemplateFlow(conversation, ctx, ensureActive) {
  await ctx.reply(section('📥 Import Template', [
    'Paste JSON with template_id, subject, and text/html.',
    'Example: {"template_id":"welcome","subject":"Hi {{name}}","text":"Hello {{name}}"}'
  ]), { parse_mode: 'Markdown' });
  const update = await conversation.wait();
  ensureActive();
  const raw = update?.message?.text?.trim();
  if (!raw) {
    await ctx.reply('❌ Import cancelled.');
    return;
  }
  const parsed = parseJsonInput(raw);
  if (!parsed || typeof parsed !== 'object') {
    await ctx.reply('❌ Invalid JSON.');
    return;
  }
  const templateId = String(parsed.template_id || parsed.id || '').trim();
  const subject = parsed.subject || '';
  const textBody = parsed.text || '';
  const htmlBody = parsed.html || '';
  const validation = validateEmailTemplatePayload({
    templateId,
    subject,
    html: htmlBody,
    text: textBody
  });
  if (validation.errors.length) {
    await ctx.reply(section('❌ Template validation failed', validation.errors), { parse_mode: 'Markdown' });
    return;
  }
  if (validation.requiredVars.length) {
    const varsLine = validation.requiredVars.slice(0, 12).join(', ');
    await ctx.reply(section('🧩 Detected variables', [varsLine]), { parse_mode: 'Markdown' });
  }
  if (validation.warnings.length) {
    await ctx.reply(section('⚠️ Template warnings', validation.warnings), { parse_mode: 'Markdown' });
  }
  const created = await createEmailTemplate(ctx, {
    template_id: templateId,
    subject,
    text: textBody || undefined,
    html: htmlBody || undefined
  });
  await storeEmailTemplateVersion(created, ctx);
  await ctx.reply(formatEmailTemplateSummary(created), { parse_mode: 'Markdown' });
}

async function searchEmailTemplatesFlow(conversation, ctx, ensureActive) {
  await ctx.reply(section('🔎 Search Templates', ['Enter a keyword to search.']), { parse_mode: 'Markdown' });
  const update = await conversation.wait();
  ensureActive();
  const term = update?.message?.text?.trim();
  if (!term) {
    await ctx.reply('❌ Search cancelled.');
    return;
  }
  const templates = await fetchEmailTemplates(ctx);
  const normalized = term.toLowerCase();
  const matches = templates.filter((tpl) => {
    const id = (tpl.template_id || '').toLowerCase();
    const subject = (tpl.subject || '').toLowerCase();
    return id.includes(normalized) || subject.includes(normalized);
  });
  if (!matches.length) {
    await ctx.reply('ℹ️ No templates matched your search.');
    return;
  }
  const options = matches.map((tpl) => ({
    id: tpl.template_id,
    label: `📄 ${tpl.template_id}`
  }));
  options.push({ id: 'back', label: '⬅️ Back' });
  const selection = await askOptionWithButtons(
    conversation,
    ctx,
    '📧 Search results',
    options,
    { prefix: 'email-template-search', columns: 1, ensureActive }
  );
  if (!selection || selection.id === 'back') return;
  const template = await fetchEmailTemplate(ctx, selection.id);
  if (!template) {
    await ctx.reply('❌ Template not found.');
    return;
  }
  await showEmailTemplateDetail(conversation, ctx, template, ensureActive);
}

async function showEmailTemplateDetail(conversation, ctx, template, ensureActive) {
  let viewing = true;
  while (viewing) {
    await ctx.reply(formatEmailTemplateSummary(template), { parse_mode: 'Markdown' });
    const action = await askOptionWithButtons(
      conversation,
      ctx,
      'Choose an action.',
      [
        { id: 'preview', label: '🔍 Preview' },
        { id: 'edit', label: '✏️ Edit' },
        { id: 'clone', label: '🧬 Clone' },
        { id: 'export', label: '📤 Export' },
        { id: 'versions', label: '🗂️ Versions' },
        { id: 'delete', label: '🗑️ Delete' },
        { id: 'back', label: '⬅️ Back' }
      ],
      { prefix: 'email-template-action', columns: 2, ensureActive }
    );
    switch (action.id) {
      case 'preview':
        await previewEmailTemplate(conversation, ctx, template, ensureActive);
        break;
      case 'edit':
        await editEmailTemplateFlow(conversation, ctx, template, ensureActive);
        template = await fetchEmailTemplate(ctx, template.template_id);
        break;
      case 'clone':
        await cloneEmailTemplateFlow(conversation, ctx, template, ensureActive);
        break;
      case 'export':
        await exportEmailTemplate(ctx, template);
        break;
      case 'versions':
        await showEmailTemplateVersions(conversation, ctx, template, ensureActive);
        template = await fetchEmailTemplate(ctx, template.template_id);
        break;
      case 'delete':
        await deleteEmailTemplateFlow(conversation, ctx, template);
        viewing = false;
        break;
      case 'back':
        viewing = false;
        break;
      default:
        break;
    }
  }
}

async function listEmailTemplatesFlow(conversation, ctx, ensureActive) {
  const templates = await fetchEmailTemplates(ctx);
  if (!templates.length) {
    await ctx.reply('ℹ️ No email templates found. Create one to get started.');
    return;
  }
  const options = templates.map((tpl) => ({
    id: tpl.template_id,
    label: `📄 ${tpl.template_id}`
  }));
  options.push({ id: 'back', label: '⬅️ Back' });
  const selection = await askOptionWithButtons(
    conversation,
    ctx,
    '📧 Choose a template.',
    options,
    { prefix: 'email-template-select', columns: 1, ensureActive }
  );
  if (!selection || selection.id === 'back') return;
  const template = await fetchEmailTemplate(ctx, selection.id);
  if (!template) {
    await ctx.reply('❌ Template not found.');
    return;
  }
  await showEmailTemplateDetail(conversation, ctx, template, ensureActive);
}

async function emailTemplatesFlow(conversation, ctx, options = {}) {
  const opId = options.ensureActive ? null : startOperation(ctx, 'email-templates');
  const ensureActive = typeof options.ensureActive === 'function'
    ? options.ensureActive
    : () => ensureOperationActive(ctx, opId);
  try {
    const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
    if (!user) {
      await ctx.reply(section('❌ Authorization', ['You are not authorized to use this bot.']), { parse_mode: 'Markdown' });
      return;
    }
    let open = true;
    while (open) {
      const action = await askOptionWithButtons(
        conversation,
        ctx,
        '📧 *Email Template Builder*',
        [
          { id: 'list', label: '📄 List templates' },
          { id: 'create', label: '➕ Create template' },
          { id: 'search', label: '🔎 Search templates' },
          { id: 'import', label: '📥 Import template' },
          { id: 'back', label: '⬅️ Back' }
        ],
        { prefix: 'email-template-main', columns: 1, ensureActive }
      );
      switch (action.id) {
        case 'list':
          await listEmailTemplatesFlow(conversation, ctx, ensureActive);
          break;
        case 'create':
          await createEmailTemplateFlow(conversation, ctx, ensureActive);
          break;
        case 'search':
          await searchEmailTemplatesFlow(conversation, ctx, ensureActive);
          break;
        case 'import':
          await importEmailTemplateFlow(conversation, ctx, ensureActive);
          break;
        case 'back':
          open = false;
          break;
        default:
          break;
      }
    }
  } catch (error) {
    console.error('Email template flow error:', error);
    await ctx.reply(section('❌ Email Template Error', [error.message || 'Failed to manage templates.']), { parse_mode: 'Markdown' });
  }
}

function formatTimestamp(value) {
  if (!value) return '—';
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return escapeMarkdown(String(value));
  return escapeMarkdown(dt.toLocaleString());
}

function formatEmailStatusCard(message, events) {
  const status = escapeMarkdown(message.status || 'unknown');
  const subject = escapeMarkdown(message.subject || '—');
  const toEmail = escapeMarkdown(message.to_email || '—');
  const fromEmail = escapeMarkdown(message.from_email || '—');
  const provider = escapeMarkdown(message.provider || '—');
  const messageId = escapeMarkdown(message.message_id || '—');
  const failure = message.failure_reason ? escapeMarkdown(message.failure_reason) : null;
  const scheduled = message.scheduled_at ? formatTimestamp(message.scheduled_at) : null;
  const sentAt = message.sent_at ? formatTimestamp(message.sent_at) : null;
  const deliveredAt = message.delivered_at ? formatTimestamp(message.delivered_at) : null;
  const suppressed = message.suppressed_reason ? escapeMarkdown(message.suppressed_reason) : null;

  const details = [
    buildLine('🆔', 'Message', messageId),
    buildLine('📨', 'To', toEmail),
    buildLine('📤', 'From', fromEmail),
    buildLine('🧾', 'Subject', subject),
    buildLine('📊', 'Status', status),
    buildLine('🔌', 'Provider', provider)
  ];

  if (scheduled) details.push(buildLine('🗓️', 'Scheduled', scheduled));
  if (sentAt) details.push(buildLine('🕒', 'Sent', sentAt));
  if (deliveredAt) details.push(buildLine('✅', 'Delivered', deliveredAt));
  if (suppressed) details.push(buildLine('⛔', 'Suppressed', suppressed));
  if (failure) details.push(buildLine('❌', 'Failure', failure));

  const recentEvents = (events || []).slice(-4).map((event) => {
    const meta = parseJsonInput(event.metadata) || {};
    const reason = meta.reason ? ` (${escapeMarkdown(String(meta.reason))})` : '';
    const time = formatTimestamp(event.timestamp);
    return `• ${time} — ${escapeMarkdown(event.event_type || 'event')}${reason}`;
  });

  const timelineLines = recentEvents.length ? recentEvents : ['• —'];

  return [
    emphasize('Email Status'),
    section('Details', details),
    section('Latest Events', timelineLines)
  ].join('\n\n');
}

function formatEmailTimeline(events) {
  const lines = (events || []).map((event) => {
    const meta = parseJsonInput(event.metadata) || {};
    const reason = meta.reason ? ` (${escapeMarkdown(String(meta.reason))})` : '';
    const time = formatTimestamp(event.timestamp);
    return `• ${time} — ${escapeMarkdown(event.event_type || 'event')}${reason}`;
  });
  return lines.length ? lines : ['• —'];
}

function formatBulkStatusCard(job) {
  const status = escapeMarkdown(job.status || 'unknown');
  const jobId = escapeMarkdown(job.job_id || '—');
  const total = Number(job.total || 0);
  const sent = Number(job.sent || 0);
  const failed = Number(job.failed || 0);
  const queued = Number(job.queued || 0);
  const suppressed = Number(job.suppressed || 0);
  const delivered = Number(job.delivered || 0);
  const bounced = Number(job.bounced || 0);
  const complained = Number(job.complained || 0);
  const progress = total ? Math.round(((sent + failed + suppressed) / total) * 100) : 0;

  const lines = [
    buildLine('🆔', 'Job', jobId),
    buildLine('📊', 'Status', status),
    buildLine('📨', 'Total', escapeMarkdown(String(total))),
    buildLine('⏳', 'Queued', escapeMarkdown(String(queued))),
    buildLine('✅', 'Sent', escapeMarkdown(String(sent))),
    buildLine('📬', 'Delivered', escapeMarkdown(String(delivered))),
    buildLine('❌', 'Failed', escapeMarkdown(String(failed))),
    buildLine('⛔', 'Suppressed', escapeMarkdown(String(suppressed))),
    buildLine('📉', 'Bounced', escapeMarkdown(String(bounced))),
    buildLine('⚠️', 'Complained', escapeMarkdown(String(complained))),
    buildLine('📈', 'Progress', escapeMarkdown(`${progress}%`))
  ];

  return [
    emphasize('Bulk Email'),
    section('Job Status', lines)
  ].join('\n\n');
}

async function guardedGet(ctx, url, options = {}) {
  const controller = new AbortController();
  const release = registerAbortController(ctx, controller);
  try {
    return await httpClient.get(ctx, url, { timeout: 20000, signal: controller.signal, ...options });
  } finally {
    release();
  }
}

async function guardedPost(ctx, url, data, options = {}) {
  const controller = new AbortController();
  const release = registerAbortController(ctx, controller);
  try {
    return await httpClient.post(ctx, url, data, { timeout: 30000, signal: controller.signal, ...options });
  } finally {
    release();
  }
}

async function guardedPut(ctx, url, data, options = {}) {
  const controller = new AbortController();
  const release = registerAbortController(ctx, controller);
  try {
    return await httpClient.put(ctx, url, data, { timeout: 30000, signal: controller.signal, ...options });
  } finally {
    release();
  }
}

async function sendEmailStatusCard(ctx, messageId, options = {}) {
  const response = await guardedGet(ctx, `${config.apiUrl}/email/messages/${messageId}`);
  const message = response.data?.message;
  const events = response.data?.events || [];
  if (!message) {
    await ctx.reply('❌ Email message not found.');
    return;
  }
  const text = formatEmailStatusCard(message, events);
  const keyboard = new InlineKeyboard()
    .text('🔄 Refresh', buildCallbackData(ctx, `EMAIL_STATUS:${messageId}`))
    .text('🧾 Timeline', buildCallbackData(ctx, `EMAIL_TIMELINE:${messageId}`));
  if (message.bulk_job_id) {
    keyboard.row().text('📦 Bulk Job', buildCallbackData(ctx, `EMAIL_BULK:${message.bulk_job_id}`));
  }
  const payload = { parse_mode: 'Markdown', reply_markup: keyboard };
  if (ctx.callbackQuery?.message && !options.forceReply) {
    try {
      await ctx.editMessageText(text, payload);
      await activateMenuMessage(ctx, ctx.callbackQuery.message.message_id, ctx.callbackQuery.message.chat?.id);
      return;
    } catch (error) {
      // fallback to sending a new message
    }
  }
  await renderMenu(ctx, text, keyboard, { payload });
}

async function sendEmailTimeline(ctx, messageId) {
  const response = await guardedGet(ctx, `${config.apiUrl}/email/messages/${messageId}`);
  const message = response.data?.message;
  const events = response.data?.events || [];
  if (!message) {
    await ctx.reply('❌ Email message not found.');
    return;
  }
  const timeline = formatEmailTimeline(events);
  const header = `${emphasize('Email Timeline')}\n${section('Message', [
    buildLine('🆔', 'Message', escapeMarkdown(message.message_id || '—')),
    buildLine('📊', 'Status', escapeMarkdown(message.status || 'unknown'))
  ])}`;
  const body = `${section('Events', timeline)}`;
  const text = `${header}\n\n${body}`;
  await ctx.reply(text, { parse_mode: 'Markdown' });
}

async function sendBulkStatusCard(ctx, jobId, options = {}) {
  const response = await guardedGet(ctx, `${config.apiUrl}/email/bulk/${jobId}`);
  const job = response.data?.job;
  if (!job) {
    await ctx.reply('❌ Bulk job not found.');
    return;
  }
  const text = formatBulkStatusCard(job);
  const keyboard = new InlineKeyboard()
    .text('🔄 Refresh', buildCallbackData(ctx, `EMAIL_BULK:${jobId}`));
  const payload = { parse_mode: 'Markdown', reply_markup: keyboard };
  if (ctx.callbackQuery?.message && !options.forceReply) {
    try {
      await ctx.editMessageText(text, payload);
      await activateMenuMessage(ctx, ctx.callbackQuery.message.message_id, ctx.callbackQuery.message.chat?.id);
      return;
    } catch (error) {
      // fallback to sending a new message
    }
  }
  await renderMenu(ctx, text, keyboard, { payload });
}

async function askSchedule(conversation, ctx, ensureActive) {
  const scheduleOptions = [
    { id: 'now', label: 'Send now' },
    { id: 'schedule', label: 'Schedule' },
    { id: 'cancel', label: 'Cancel' }
  ];
  const choice = await askOptionWithButtons(
    conversation,
    ctx,
    '⏱️ *Schedule this email?*',
    scheduleOptions,
    { prefix: 'email-schedule', columns: 3, ensureActive }
  );
  if (!choice || choice.id === 'cancel') {
    return { cancelled: true };
  }
  if (choice.id === 'now') {
    return { sendAt: null };
  }

  await ctx.reply(section('📅 Scheduling', [
    'Send an ISO timestamp (e.g., 2024-12-25T09:30:00Z).',
    'Type "now" to send immediately.'
  ]), { parse_mode: 'Markdown' });
  const update = await conversation.wait();
  ensureActive();
  const input = update?.message?.text?.trim();
  if (!input || input.toLowerCase() === 'now') {
    return { sendAt: null };
  }
  const parsed = new Date(input);
  if (Number.isNaN(parsed.getTime())) {
    await ctx.reply('❌ Invalid timestamp. Sending immediately instead.');
    return { sendAt: null };
  }
  return { sendAt: parsed.toISOString() };
}

async function askMarketingFlag(conversation, ctx, ensureActive) {
  const options = [
    { id: 'no', label: 'Transactional' },
    { id: 'yes', label: 'Marketing' }
  ];
  const choice = await askOptionWithButtons(
    conversation,
    ctx,
    '📣 *Is this marketing email?*',
    options,
    { prefix: 'email-marketing', columns: 2, ensureActive }
  );
  return choice?.id === 'yes';
}

async function promptVariables(conversation, ctx, ensureActive) {
  await ctx.reply(section('🧩 Template variables', [
    'Paste JSON (e.g., {"name":"Jamie","code":"123456"})',
    'Type "skip" for none.'
  ]), { parse_mode: 'Markdown' });
  const update = await conversation.wait();
  ensureActive();
  const text = update?.message?.text?.trim();
  if (!text || text.toLowerCase() === 'skip') {
    return {};
  }
  const parsed = parseJsonInput(text);
  if (!parsed || typeof parsed !== 'object') {
    await ctx.reply('❌ Invalid JSON. Using empty variables.');
    return {};
  }
  return parsed;
}

async function emailFlow(conversation, ctx) {
  const opId = startOperation(ctx, 'email');
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

  try {
    ensureActive();
    const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
    if (!user) {
      await ctx.reply(section('❌ Authorization', ['You are not authorized to use this bot.']), { parse_mode: 'Markdown' });
      return;
    }

    await ctx.reply(section('✉️ Email', [
      'Enter the recipient email address.'
    ]), { parse_mode: 'Markdown' });
    const toMsg = await waitForMessage();
    let toEmail = normalizeEmail(toMsg?.message?.text);
    if (!isValidEmail(toEmail)) {
      await ctx.reply(section('⚠️ Email Error', ['Invalid email address.']), { parse_mode: 'Markdown' });
      return;
    }

    await ctx.reply(section('📤 From Address', [
      'Optional. Type an email address or "skip" to use default.'
    ]), { parse_mode: 'Markdown' });
    const fromMsg = await waitForMessage();
    let fromEmail = normalizeEmail(fromMsg?.message?.text);
    if (!fromEmail || fromEmail.toLowerCase() === 'skip') {
      fromEmail = null;
    } else if (!isValidEmail(fromEmail)) {
      await ctx.reply(section('⚠️ Email Error', ['Invalid sender email.']), { parse_mode: 'Markdown' });
      return;
    }

    const modeOptions = [
      { id: 'script', label: 'Use script' },
      { id: 'custom', label: 'Custom content' }
    ];
    const mode = await askOptionWithButtons(
      conversation,
      ctx,
      '🧩 *Choose email mode*',
      modeOptions,
      { prefix: 'email-mode', columns: 2, ensureActive }
    );
    if (!mode) {
      await ctx.reply('❌ Email flow cancelled.');
      return;
    }

    let payload = {
      to: toEmail,
      from: fromEmail || undefined
    };

    if (mode.id === 'script') {
      let scriptId = await selectEmailTemplateId(conversation, ctx, ensureActive);
      if (!scriptId) {
        await ctx.reply(section('📄 Script', ['Enter script_id to use.']), { parse_mode: 'Markdown' });
        const scriptMsg = await waitForMessage();
        scriptId = scriptMsg?.message?.text?.trim();
      }
      if (!scriptId) {
        await ctx.reply('❌ Script ID is required.');
        return;
      }

      await ctx.reply(section('🧾 Subject override', [
        'Optional. Type a subject override or "skip".'
      ]), { parse_mode: 'Markdown' });
      const subjectMsg = await waitForMessage();
      const subjectOverride = subjectMsg?.message?.text?.trim();
      const variables = await promptVariables(conversation, ctx, ensureActive);

      const previewResponse = await guardedPost(ctx, `${config.apiUrl}/email/preview`, {
        script_id: scriptId,
        subject: subjectOverride && subjectOverride.toLowerCase() !== 'skip' ? subjectOverride : undefined,
        variables
      });

      if (!previewResponse.data?.success) {
        const missing = previewResponse.data?.missing || [];
        await ctx.reply(section('⚠️ Missing variables', [
          missing.length ? missing.join(', ') : 'Unknown script issue'
        ]), { parse_mode: 'Markdown' });
        return;
      }

      payload = {
        ...payload,
        script_id: scriptId,
        subject: subjectOverride && subjectOverride.toLowerCase() !== 'skip' ? subjectOverride : undefined,
        variables
      };
      const preview = previewResponse.data;
      await ctx.reply(section('🔍 Preview', [
        buildLine('🧾', 'Subject', escapeMarkdown(preview.subject || '—')),
        buildLine('📄', 'Text', escapeMarkdown((preview.text || '').slice(0, 140) || '—'))
      ]), { parse_mode: 'Markdown' });
    } else {
      await ctx.reply(section('🧾 Subject', ['Enter the email subject line.']), { parse_mode: 'Markdown' });
      const subjectMsg = await waitForMessage();
      const subject = subjectMsg?.message?.text?.trim();
      if (!subject) {
        await ctx.reply('❌ Subject is required.');
        return;
      }

      await ctx.reply(section('📝 Text Body', ['Enter the plain text body.']), { parse_mode: 'Markdown' });
      const textMsg = await waitForMessage();
      const textBody = textMsg?.message?.text?.trim();
      if (!textBody) {
        await ctx.reply('❌ Text body is required.');
        return;
      }

      const htmlBody = await promptHtmlBody(conversation, ctx, ensureActive);

      payload = {
        ...payload,
        subject,
        text: textBody,
        html: htmlBody || undefined
      };
    }

    payload.is_marketing = await askMarketingFlag(conversation, ctx, ensureActive);
    const schedule = await askSchedule(conversation, ctx, ensureActive);
    if (schedule.cancelled) {
      await ctx.reply('❌ Email send cancelled.');
      return;
    }
    if (schedule.sendAt) {
      payload.send_at = schedule.sendAt;
    }

    const response = await guardedPost(ctx, `${config.apiUrl}/email/send`, payload);
    const messageId = response.data?.message_id;
    if (!messageId) {
      await ctx.reply('❌ Email enqueue failed.');
      return;
    }
    await ctx.reply(section('✅ Email queued', [
      buildLine('🆔', 'Message', escapeMarkdown(messageId))
    ]), { parse_mode: 'Markdown' });
    await sendEmailStatusCard(ctx, messageId, { forceReply: true });
  } catch (error) {
    console.error('Email flow error:', error);
    await ctx.reply(section('❌ Email Error', [error.message || 'Failed to send email.']), { parse_mode: 'Markdown' });
  }
}

async function bulkEmailFlow(conversation, ctx) {
  const opId = startOperation(ctx, 'bulk-email');
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

  try {
    ensureActive();
    const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
    const admin = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
    if (!user || !admin) {
      await ctx.reply(section('❌ Authorization', ['Bulk email is for administrators only.']), { parse_mode: 'Markdown' });
      return;
    }

    await ctx.reply(section('📨 Bulk Recipients', [
      'Paste emails separated by commas or new lines.',
      'You can also paste JSON: [{"email":"a@x.com","variables":{"name":"A"}}]'
    ]), { parse_mode: 'Markdown' });
    const recipientsMsg = await waitForMessage();
    const { recipients, invalid } = parseRecipientsInput(recipientsMsg?.message?.text || '');
    if (!recipients.length) {
      await ctx.reply(section('⚠️ Recipient Error', ['No valid email addresses found.']), { parse_mode: 'Markdown' });
      return;
    }
    if (invalid.length) {
      await ctx.reply(section('⚠️ Invalid addresses', [
        `${invalid.slice(0, 5).join(', ')}${invalid.length > 5 ? '…' : ''}`
      ]), { parse_mode: 'Markdown' });
    }

    await ctx.reply(section('📤 From Address', [
      'Optional. Type an email address or "skip" to use default.'
    ]), { parse_mode: 'Markdown' });
    const fromMsg = await waitForMessage();
    let fromEmail = normalizeEmail(fromMsg?.message?.text);
    if (!fromEmail || fromEmail.toLowerCase() === 'skip') {
      fromEmail = null;
    } else if (!isValidEmail(fromEmail)) {
      await ctx.reply(section('⚠️ Email Error', ['Invalid sender email.']), { parse_mode: 'Markdown' });
      return;
    }

    const modeOptions = [
      { id: 'script', label: 'Use script' },
      { id: 'custom', label: 'Custom content' }
    ];
    const mode = await askOptionWithButtons(
      conversation,
      ctx,
      '🧩 *Choose bulk email mode*',
      modeOptions,
      { prefix: 'bulk-email-mode', columns: 2, ensureActive }
    );
    if (!mode) {
      await ctx.reply('❌ Bulk email flow cancelled.');
      return;
    }

    let payload = {
      recipients,
      from: fromEmail || undefined
    };

    if (mode.id === 'script') {
      await ctx.reply(section('📄 Script', ['Enter script_id to use.']), { parse_mode: 'Markdown' });
      const scriptMsg = await waitForMessage();
      const scriptId = scriptMsg?.message?.text?.trim();
      if (!scriptId) {
        await ctx.reply('❌ Script ID is required.');
        return;
      }

      await ctx.reply(section('🧾 Subject override', [
        'Optional. Type a subject override or "skip".'
      ]), { parse_mode: 'Markdown' });
      const subjectMsg = await waitForMessage();
      const subjectOverride = subjectMsg?.message?.text?.trim();
      const variables = await promptVariables(conversation, ctx, ensureActive);

      payload = {
        ...payload,
        script_id: scriptId,
        subject: subjectOverride && subjectOverride.toLowerCase() !== 'skip' ? subjectOverride : undefined,
        variables
      };
    } else {
      await ctx.reply(section('🧾 Subject', ['Enter the email subject line.']), { parse_mode: 'Markdown' });
      const subjectMsg = await waitForMessage();
      const subject = subjectMsg?.message?.text?.trim();
      if (!subject) {
        await ctx.reply('❌ Subject is required.');
        return;
      }

      await ctx.reply(section('📝 Text Body', ['Enter the plain text body.']), { parse_mode: 'Markdown' });
      const textMsg = await waitForMessage();
      const textBody = textMsg?.message?.text?.trim();
      if (!textBody) {
        await ctx.reply('❌ Text body is required.');
        return;
      }

      const htmlBody = await promptHtmlBody(conversation, ctx, ensureActive);

      payload = {
        ...payload,
        subject,
        text: textBody,
        html: htmlBody || undefined
      };
    }

    payload.is_marketing = await askMarketingFlag(conversation, ctx, ensureActive);
    const schedule = await askSchedule(conversation, ctx, ensureActive);
    if (schedule.cancelled) {
      await ctx.reply('❌ Bulk email cancelled.');
      return;
    }
    if (schedule.sendAt) {
      payload.send_at = schedule.sendAt;
    }

    const response = await guardedPost(ctx, `${config.apiUrl}/email/bulk`, payload);
    const jobId = response.data?.bulk_job_id;
    if (!jobId) {
      await ctx.reply('❌ Bulk job enqueue failed.');
      return;
    }
    await ctx.reply(section('✅ Bulk job queued', [
      buildLine('🆔', 'Job', escapeMarkdown(jobId)),
      buildLine('📨', 'Recipients', escapeMarkdown(String(recipients.length)))
    ]), { parse_mode: 'Markdown' });
    await sendBulkStatusCard(ctx, jobId, { forceReply: true });
  } catch (error) {
    console.error('Bulk email flow error:', error);
    await ctx.reply(section('❌ Bulk Email Error', [error.message || 'Failed to send bulk email.']), { parse_mode: 'Markdown' });
  }
}

function registerEmailCommands(bot) {
  bot.command('email', async (ctx) => {
    try {
      const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
      if (!user) {
        return ctx.reply('❌ You are not authorized to use this bot.');
      }
      await ctx.conversation.enter('email-conversation');
    } catch (error) {
      console.error('Email command error:', error);
      await ctx.reply('❌ Could not start email flow.');
    }
  });

  bot.command('bulkemail', async (ctx) => {
    try {
      const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
      if (!user) {
        return ctx.reply('❌ You are not authorized to use this bot.');
      }
      const admin = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
      if (!admin) {
        return ctx.reply('❌ Bulk email is for administrators only.');
      }
      await ctx.conversation.enter('bulk-email-conversation');
    } catch (error) {
      console.error('Bulk email command error:', error);
      await ctx.reply('❌ Could not start bulk email flow.');
    }
  });

  bot.command('emailstatus', async (ctx) => {
    try {
      const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
      if (!user) {
        return ctx.reply('❌ You are not authorized to use this bot.');
      }
      const args = ctx.message?.text?.split(' ') || [];
      if (args.length < 2) {
        return ctx.reply(
          '📧 <b>Usage:</b> <code>/emailstatus &lt;message_id&gt;</code>\n\nExample: <code>/emailstatus email_1234...</code>',
          { parse_mode: 'HTML' }
        );
      }
      const messageId = args[1].trim();
      await sendEmailStatusCard(ctx, messageId, { forceReply: true });
    } catch (error) {
      console.error('Email status command error:', error);
      await ctx.reply('❌ Failed to fetch email status.');
    }
  });

  bot.command('emailbulk', async (ctx) => {
    try {
      const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
      if (!user) {
        return ctx.reply('❌ You are not authorized to use this bot.');
      }
      const args = ctx.message?.text?.split(' ') || [];
      if (args.length < 2) {
        return ctx.reply(
          '📦 <b>Usage:</b> <code>/emailbulk &lt;bulk_job_id&gt;</code>\n\nExample: <code>/emailbulk bulk_1234...</code>',
          { parse_mode: 'HTML' }
        );
      }
      const jobId = args[1].trim();
      await sendBulkStatusCard(ctx, jobId, { forceReply: true });
    } catch (error) {
      console.error('Bulk email status command error:', error);
      await ctx.reply('❌ Failed to fetch bulk job status.');
    }
  });
}

module.exports = {
  emailFlow,
  bulkEmailFlow,
  emailTemplatesFlow,
  registerEmailCommands,
  sendEmailStatusCard,
  sendEmailTimeline,
  sendBulkStatusCard
};
