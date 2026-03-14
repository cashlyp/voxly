const httpClient = require('../utils/httpClient');
const { InlineKeyboard } = require('grammy');
const config = require('../config');
const {
  getUser,
  isAdmin
} = require('../db/db');
const {
  startOperation,
  ensureOperationActive,
  registerAbortController,
  guardAgainstCommandInterrupt
} = require('../utils/sessionState');
const {
  section,
  buildLine,
  buildTextProgressBar,
  escapeMarkdown,
  emphasize,
  activateMenuMessage,
  renderMenu,
  sendEphemeral,
  buildBackToMenuKeyboard: buildStandardBackKeyboard,
  appendBackToMenuRows,
  selectionExpiredMessage,
  cancelledMessage,
  setupStepMessage,
  upsertMenuMessage,
  dismissMenuMessage
} = require('../utils/ui');
const { buildCallbackData } = require('../utils/actions');
const { getAccessProfile } = require('../utils/capabilities');
const { askOptionWithButtons } = require('../utils/persona');

const BULK_EMAIL_HISTORY_PAGE_SIZE = 6;

function normalizeEmail(value) {
  return String(value || '').trim().toLowerCase();
}

function stripMarkdown(text = '') {
  return String(text).replace(/[*_`[\]()>#+=|{}.!-]/g, '');
}

async function safeReply(ctx, text, options = {}) {
  try {
    return await ctx.reply(text, options);
  } catch (error) {
    if (/can't parse entities/i.test(error?.message || '')) {
      const fallback = stripMarkdown(text);
      const fallbackOptions = { ...options };
      delete fallbackOptions.parse_mode;
      return await ctx.reply(fallback, fallbackOptions);
    }
    throw error;
  }
}

async function safeReplyMarkdown(ctx, text, options = {}) {
  return safeReply(ctx, text, { parse_mode: 'Markdown', ...options });
}

async function replyApiError(ctx, error, fallback) {
  const message = httpClient.getUserMessage(error, fallback);
  return safeReply(ctx, message);
}

function buildBackToMenuKeyboard(ctx, action = 'EMAIL', label = '⬅️ Back to Email Center') {
  return buildStandardBackKeyboard(ctx, {
    backAction: action,
    backLabel: label
  });
}

async function maybeSendEmailAliasTip(ctx) {
  if (!ctx.session) return;
  ctx.session.hints = ctx.session.hints || {};
  if (ctx.session.hints.emailMenuTipSent) return;
  ctx.session.hints.emailMenuTipSent = true;
  await sendEphemeral(ctx, 'ℹ️ Tip: /email is now the single entry point for all Email actions.');
}

function extractEmailTemplateVariables(text = '') {
  if (!text) return [];
  const matches = text.match(/{{\s*([\w.-]+)\s*}}/g) || [];
  const vars = new Set();
  matches.forEach((match) => {
    const cleaned = match.replace(/{{|}}/g, '').trim();
    if (cleaned) vars.add(cleaned);
  });
  return Array.from(vars);
}

function stableStringify(value) {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    const items = value.map((item) =>
      item === undefined ? 'null' : stableStringify(item)
    );
    return `[${items.join(',')}]`;
  }
  const keys = Object.keys(value)
    .filter((key) => value[key] !== undefined)
    .sort();
  const entries = keys.map(
    (key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`
  );
  return `{${entries.join(',')}}`;
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

async function submitEmailTemplateForReview(ctx, templateId) {
  const response = await guardedPost(
    ctx,
    `${config.apiUrl}/email/templates/${encodeURIComponent(templateId)}/submit-review`,
    {}
  );
  return response.data?.template;
}

async function reviewEmailTemplate(ctx, templateId, decision, note = null) {
  const response = await guardedPost(
    ctx,
    `${config.apiUrl}/email/templates/${encodeURIComponent(templateId)}/review`,
    { decision, note }
  );
  return response.data?.template;
}

async function promoteEmailTemplateLive(ctx, templateId) {
  const response = await guardedPost(
    ctx,
    `${config.apiUrl}/email/templates/${encodeURIComponent(templateId)}/promote-live`,
    {}
  );
  return response.data?.template;
}

async function listEmailTemplateApiVersions(ctx, templateId) {
  const response = await guardedGet(
    ctx,
    `${config.apiUrl}/email/templates/${encodeURIComponent(templateId)}/versions`
  );
  return response.data?.versions || [];
}

async function diffEmailTemplateApiVersions(ctx, templateId, fromVersion, toVersion) {
  const response = await guardedGet(
    ctx,
    `${config.apiUrl}/email/templates/${encodeURIComponent(templateId)}/diff`,
    {
      params: {
        from_version: fromVersion,
        to_version: toVersion
      }
    }
  );
  return response.data || {};
}

async function rollbackEmailTemplateApiVersion(ctx, templateId, version) {
  const response = await guardedPost(
    ctx,
    `${config.apiUrl}/email/templates/${encodeURIComponent(templateId)}/rollback`,
    { version }
  );
  return response.data?.template;
}

async function simulateEmailTemplateApi(ctx, templateId, variables = {}) {
  const response = await guardedPost(
    ctx,
    `${config.apiUrl}/email/templates/${encodeURIComponent(templateId)}/simulate`,
    { variables }
  );
  return response.data?.simulation || {};
}

function getEmailTemplateLifecycleState(template = {}) {
  const raw = template?.lifecycle?.lifecycle_state || template?.lifecycle_state || 'draft';
  return String(raw || 'draft').trim().toLowerCase();
}

function getEmailTemplateLifecycleBadge(template = {}) {
  const state = getEmailTemplateLifecycleState(template);
  switch (state) {
    case 'review':
      return 'In Review';
    case 'approved':
      return 'Approved';
    case 'live':
      return 'Live';
    default:
      return 'Draft';
  }
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
    await safeReplyMarkdown(ctx, section('🧩 HTML Body', ['Paste HTML content.']));
    const update = await conversation.wait();
    ensureActive();
    return update?.message?.text?.trim() || null;
  }
  await safeReplyMarkdown(ctx, section('📎 Upload HTML', ['Send the .html file now.']));
  const upload = await conversation.wait();
  ensureActive();
  const doc = upload?.message?.document;
  if (!doc?.file_id) {
    await safeReply(ctx, '❌ No document received.');
    return null;
  }
  const filename = doc.file_name || '';
  if (!filename.toLowerCase().endsWith('.html') && doc.mime_type !== 'text/html') {
    await safeReply(ctx, '❌ Please upload a valid .html file.');
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

async function promptReviewNote(conversation, ctx, prompt, ensureActive) {
  await safeReply(ctx, `${prompt}\nType "skip" for none or "cancel" to abort.`);
  const update = await conversation.wait();
  ensureActive();
  const text = update?.message?.text?.trim();
  if (!text || text.toLowerCase() === 'skip') {
    return { cancelled: false, note: null };
  }
  if (text.toLowerCase() === 'cancel') {
    return { cancelled: true, note: null };
  }
  return { cancelled: false, note: text };
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

function formatEmailTemplateSummary(template) {
  const requiredVars = parseRequiredVars(template.required_vars);
  const varsLine = requiredVars.length ? requiredVars.join(', ') : '—';
  return section('📧 Email Template', [
    buildLine('🆔', 'ID', escapeMarkdown(template.template_id || '—')),
    buildLine('🧾', 'Lifecycle', escapeMarkdown(getEmailTemplateLifecycleBadge(template))),
    buildLine('🧾', 'Subject', escapeMarkdown(template.subject || '—')),
    template?.lifecycle?.review_note
      ? buildLine('🗒️', 'Review Note', escapeMarkdown(String(template.lifecycle.review_note).slice(0, 180)))
      : null,
    buildLine('🧩', 'Variables', escapeMarkdown(varsLine)),
    buildLine('📄', 'Has Text', template.text ? 'Yes' : 'No'),
    buildLine('🖼️', 'Has HTML', template.html ? 'Yes' : 'No'),
    buildLine('📅', 'Updated', formatTimestamp(template.updated_at || template.created_at))
  ].filter(Boolean));
}

async function createEmailTemplateFlow(conversation, ctx, ensureActive) {
  await safeReplyMarkdown(ctx, section('🆕 Create Email Template', [
    'Provide a template ID (e.g., welcome_email).'
  ]));
  const idMsg = await conversation.wait();
  ensureActive();
  const templateId = idMsg?.message?.text?.trim();
  if (!templateId) {
    await safeReply(ctx, '❌ Template ID is required.');
    return;
  }

  await safeReplyMarkdown(ctx, section('🧾 Subject', ['Enter the email subject line.']));
  const subjectMsg = await conversation.wait();
  ensureActive();
  const subject = subjectMsg?.message?.text?.trim();
  if (!subject) {
    await safeReply(ctx, '❌ Subject is required.');
    return;
  }

  await safeReplyMarkdown(ctx, section('📝 Text Body', ['Enter the plain text body (or type skip).']));
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
    await safeReplyMarkdown(ctx, section('❌ Template validation failed', validation.errors));
    return;
  }
  if (validation.requiredVars.length) {
    const varsLine = validation.requiredVars.slice(0, 12).join(', ');
    await safeReplyMarkdown(ctx, section('🧩 Detected variables', [varsLine]));
  }
  if (validation.warnings.length) {
    await safeReplyMarkdown(ctx, section('⚠️ Template warnings', validation.warnings));
    const proceed = await confirmAction(conversation, ctx, 'Continue with these warnings?', ensureActive);
    if (!proceed) {
      await safeReply(ctx, 'ℹ️ Template creation cancelled.');
      return;
    }
  }

  const template = await createEmailTemplate(ctx, {
    template_id: templateId,
    subject,
    text: textBody || undefined,
    html: htmlBody || undefined
  });

  await safeReplyMarkdown(ctx, formatEmailTemplateSummary(template));
}

async function editEmailTemplateFlow(conversation, ctx, template, ensureActive) {
  await safeReplyMarkdown(ctx, section('✏️ Update Template', [
    'Type skip to keep the current value.'
  ]));

  await safeReplyMarkdown(ctx, section('🧾 Subject', [`Current: ${template.subject || '—'}`]));
  const subjectMsg = await conversation.wait();
  ensureActive();
  let subject = subjectMsg?.message?.text?.trim();
  if (subject && subject.toLowerCase() === 'skip') subject = undefined;

  await safeReplyMarkdown(ctx, section('📝 Text Body', ['Paste new text or type skip.']));
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
    await safeReply(ctx, 'ℹ️ No changes made.');
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
    await safeReplyMarkdown(ctx, section('❌ Template validation failed', validation.errors));
    return;
  }
  if (validation.requiredVars.length) {
    const varsLine = validation.requiredVars.slice(0, 12).join(', ');
    await safeReplyMarkdown(ctx, section('🧩 Detected variables', [varsLine]));
  }
  if (validation.warnings.length) {
    await safeReplyMarkdown(ctx, section('⚠️ Template warnings', validation.warnings));
    const proceed = await confirmAction(conversation, ctx, 'Continue with these warnings?', ensureActive);
    if (!proceed) {
      await safeReply(ctx, 'ℹ️ Update cancelled.');
      return;
    }
  }

  const updated = await updateEmailTemplate(ctx, template.template_id, updates);
  await safeReplyMarkdown(ctx, formatEmailTemplateSummary(updated));
}

async function selectEmailTemplateId(conversation, ctx, ensureActive) {
  let templates = [];
  try {
    templates = await fetchEmailTemplates(ctx);
    ensureActive();
  } catch (error) {
    await replyApiError(ctx, error, 'Unable to load templates. Enter the script_id manually.');
    return null;
  }
  if (!templates.length) {
    return null;
  }
  const options = templates.map((tpl) => ({
    id: tpl.template_id,
    label: `📄 ${tpl.template_id} · ${getEmailTemplateLifecycleBadge(tpl)}`
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
    await safeReply(ctx, 'Deletion cancelled.');
    return;
  }
  await deleteEmailTemplate(ctx, template.template_id);
  await safeReplyMarkdown(ctx, `🗑️ Template *${escapeMarkdown(template.template_id)}* deleted.`);
}

async function previewEmailTemplate(conversation, ctx, template, ensureActive) {
  const variables = await promptVariables(conversation, ctx, ensureActive);
  const previewResponse = await guardedPost(ctx, `${config.apiUrl}/email/preview`, {
    script_id: template.template_id,
    variables
  });
  if (!previewResponse.data?.success) {
    await safeReply(ctx, '❌ Preview failed.');
    return;
  }
  const preview = previewResponse.data;
  await safeReplyMarkdown(ctx, section('🔍 Preview', [
    buildLine('🧾', 'Subject', escapeMarkdown(preview.subject || '—')),
    buildLine('📄', 'Text', escapeMarkdown((preview.text || '').slice(0, 140) || '—'))
  ]));
}

function formatEmailVersionSummary(version) {
  const createdAt = formatTimestamp(version.created_at);
  const reason = version.reason ? ` • ${escapeMarkdown(version.reason)}` : '';
  return `v${version.version} • ${createdAt}${reason}${version.created_by ? ` • ${escapeMarkdown(version.created_by)}` : ''}`;
}

async function showEmailTemplateVersions(conversation, ctx, template, ensureActive) {
  const versions = await listEmailTemplateApiVersions(ctx, template.template_id);
  ensureActive();
  if (!versions.length) {
    await safeReply(ctx, 'ℹ️ No governance versions found yet.');
    return;
  }
  const lines = versions.map((version) => `• ${formatEmailVersionSummary(version)}`);
  await safeReplyMarkdown(ctx, `🗂️ *API Versions*\n${lines.join('\n')}`);
}

async function rollbackEmailTemplateVersionFlow(conversation, ctx, template, ensureActive) {
  const versions = await listEmailTemplateApiVersions(ctx, template.template_id);
  ensureActive();
  if (!versions.length) {
    await safeReply(ctx, 'ℹ️ No versions available for rollback.');
    return template;
  }
  const options = versions.map((version) => ({
    id: String(version.version),
    label: `🗂️ ${formatEmailVersionSummary(version)}`
  }));
  options.push({ id: 'back', label: '⬅️ Back' });
  const selection = await askOptionWithButtons(
    conversation,
    ctx,
    '🗂️ *Email Template Versions*\nChoose a version to rollback to.',
    options,
    { prefix: 'email-template-version', columns: 1, ensureActive }
  );
  if (!selection || selection.id === 'back') {
    return template;
  }
  const versionNumber = Number(selection.id);
  if (Number.isNaN(versionNumber)) {
    await safeReply(ctx, '❌ Invalid version selected.');
    return template;
  }
  const confirmRestore = await confirmAction(
    conversation,
    ctx,
    `Rollback to version #${versionNumber} for *${escapeMarkdown(template.template_id)}*?`,
    ensureActive
  );
  if (!confirmRestore) {
    await safeReply(ctx, 'ℹ️ Rollback cancelled.');
    return template;
  }
  const rolledBack = await rollbackEmailTemplateApiVersion(
    ctx,
    template.template_id,
    versionNumber
  );
  await safeReplyMarkdown(
    ctx,
    `✅ Rolled back template *${escapeMarkdown(template.template_id)}* to version #${versionNumber}.`
  );
  return rolledBack || template;
}

async function showEmailTemplateVersionDiffFlow(conversation, ctx, template, ensureActive) {
  const versions = await listEmailTemplateApiVersions(ctx, template.template_id);
  ensureActive();
  if (versions.length < 2) {
    await safeReply(ctx, 'ℹ️ At least two versions are required to compare changes.');
    return;
  }
  const options = versions.slice(0, 12).map((version) => ({
    id: String(version.version),
    label: `v${version.version}`
  }));
  const fromSelection = await askOptionWithButtons(
    conversation,
    ctx,
    'Select the *from* version.',
    [...options, { id: 'back', label: '⬅️ Back' }],
    { prefix: 'email-template-diff-from', columns: 2, ensureActive }
  );
  if (!fromSelection || fromSelection.id === 'back') return;
  const toSelection = await askOptionWithButtons(
    conversation,
    ctx,
    'Select the *to* version.',
    [...options, { id: 'back', label: '⬅️ Back' }],
    { prefix: 'email-template-diff-to', columns: 2, ensureActive }
  );
  if (!toSelection || toSelection.id === 'back') return;
  const fromVersion = Number(fromSelection.id);
  const toVersion = Number(toSelection.id);
  if (!Number.isFinite(fromVersion) || !Number.isFinite(toVersion) || fromVersion === toVersion) {
    await safeReply(ctx, '❌ Select two different versions to compare.');
    return;
  }
  const diff = await diffEmailTemplateApiVersions(
    ctx,
    template.template_id,
    fromVersion,
    toVersion
  );
  const changes = Array.isArray(diff?.changes) ? diff.changes : [];
  if (!changes.length) {
    await safeReply(ctx, `ℹ️ No differences between v${fromVersion} and v${toVersion}.`);
    return;
  }
  const lines = changes
    .slice(0, 20)
    .map((entry) => `• *${escapeMarkdown(entry.field)}*: \`${escapeMarkdown(stableStringify(entry.from))}\` → \`${escapeMarkdown(stableStringify(entry.to))}\``);
  await safeReplyMarkdown(
    ctx,
    `🧮 *Template Diff* v${fromVersion} → v${toVersion}\n${lines.join('\n')}${changes.length > 20 ? '\n… (truncated)' : ''}`
  );
}

async function simulateEmailTemplateFlow(conversation, ctx, template, ensureActive) {
  const variables = await promptVariables(conversation, ctx, ensureActive);
  const simulation = await simulateEmailTemplateApi(
    ctx,
    template.template_id,
    variables
  );
  const missing = Array.isArray(simulation.missing_variables)
    ? simulation.missing_variables
    : [];
  const renderedSubject = String(simulation.rendered_subject || '').slice(0, 180);
  const renderedText = String(simulation.rendered_text || '').slice(0, 260);
  await safeReplyMarkdown(
    ctx,
    section('🧪 Simulation', [
      buildLine('🧾', 'Lifecycle', escapeMarkdown(getEmailTemplateLifecycleBadge(template))),
      buildLine('⚠️', 'Missing Vars', escapeMarkdown(missing.length ? missing.join(', ') : 'none')),
      buildLine('🧾', 'Subject', escapeMarkdown(renderedSubject || '—')),
      buildLine('📄', 'Text', escapeMarkdown(renderedText || '—')),
    ])
  );
}

async function cloneEmailTemplateFlow(conversation, ctx, template, ensureActive) {
  await safeReplyMarkdown(ctx, section('🧬 Clone Template', [
    `Enter a new template ID for the clone of ${escapeMarkdown(template.template_id)}.`
  ]));
  const update = await conversation.wait();
  ensureActive();
  const newId = update?.message?.text?.trim();
  if (!newId) {
    await safeReply(ctx, '❌ Template ID is required.');
    return;
  }
  const validation = validateEmailTemplatePayload({
    templateId: newId,
    subject: template.subject,
    html: template.html,
    text: template.text
  });
  if (validation.errors.length) {
    await safeReplyMarkdown(ctx, section('❌ Template validation failed', validation.errors));
    return;
  }
  if (validation.requiredVars.length) {
    const varsLine = validation.requiredVars.slice(0, 12).join(', ');
    await safeReplyMarkdown(ctx, section('🧩 Detected variables', [varsLine]));
  }
  if (validation.warnings.length) {
    await safeReplyMarkdown(ctx, section('⚠️ Template warnings', validation.warnings));
  }
  const cloned = await createEmailTemplate(ctx, {
    template_id: newId,
    subject: template.subject,
    html: template.html || undefined,
    text: template.text || undefined
  });
  await safeReplyMarkdown(ctx, `✅ Template cloned as *${escapeMarkdown(cloned.template_id)}*.`);
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
  await safeReplyMarkdown(ctx, text);
}

async function importEmailTemplateFlow(conversation, ctx, ensureActive) {
  await safeReplyMarkdown(ctx, section('📥 Import Template', [
    'Paste JSON with template_id, subject, and text/html.',
    'Example: {"template_id":"welcome","subject":"Hi {{name}}","text":"Hello {{name}}"}'
  ]));
  const update = await conversation.wait();
  ensureActive();
  const raw = update?.message?.text?.trim();
  if (!raw) {
    await safeReply(ctx, '❌ Import cancelled.');
    return;
  }
  const parsed = parseJsonInput(raw);
  if (!parsed || typeof parsed !== 'object') {
    await safeReply(ctx, '❌ Invalid JSON.');
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
    await safeReplyMarkdown(ctx, section('❌ Template validation failed', validation.errors));
    return;
  }
  if (validation.requiredVars.length) {
    const varsLine = validation.requiredVars.slice(0, 12).join(', ');
    await safeReplyMarkdown(ctx, section('🧩 Detected variables', [varsLine]));
  }
  if (validation.warnings.length) {
    await safeReplyMarkdown(ctx, section('⚠️ Template warnings', validation.warnings));
  }
  const created = await createEmailTemplate(ctx, {
    template_id: templateId,
    subject,
    text: textBody || undefined,
    html: htmlBody || undefined
  });
  await safeReplyMarkdown(ctx, formatEmailTemplateSummary(created));
}

async function searchEmailTemplatesFlow(conversation, ctx, ensureActive) {
  await safeReplyMarkdown(ctx, section('🔎 Search Templates', ['Enter a keyword to search.']));
  const update = await conversation.wait();
  ensureActive();
  const term = update?.message?.text?.trim();
  if (!term) {
    await safeReply(ctx, '❌ Search cancelled.');
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
    await safeReply(ctx, 'ℹ️ No templates matched your search.');
    return;
  }
  const options = matches.map((tpl) => ({
    id: tpl.template_id,
    label: `📄 ${tpl.template_id} · ${getEmailTemplateLifecycleBadge(tpl)}`
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
    await safeReply(ctx, '❌ Template not found.');
    return;
  }
  await showEmailTemplateDetail(conversation, ctx, template, ensureActive);
}

async function showEmailTemplateDetail(conversation, ctx, template, ensureActive) {
  let summaryMessage = null;
  let viewing = true;
  try {
    while (viewing) {
      summaryMessage = await upsertMenuMessage(ctx, summaryMessage, formatEmailTemplateSummary(template), {
        parse_mode: 'Markdown'
      });
      const lifecycleState = getEmailTemplateLifecycleState(template);
      const actions = [
        { id: 'preview', label: '🔍 Preview' },
        { id: 'simulate', label: '🧪 Simulate' },
        { id: 'edit', label: '✏️ Edit' },
        { id: 'clone', label: '🧬 Clone' },
        { id: 'export', label: '📤 Export' },
        { id: 'versions', label: '🗂️ Versions' },
        { id: 'diff', label: '🧮 Diff' },
        { id: 'rollback', label: '↩️ Rollback' }
      ];
      if (lifecycleState === 'draft') {
        actions.push({ id: 'submit_review', label: '📨 Submit Review' });
      } else if (lifecycleState === 'review') {
        actions.push({ id: 'approve', label: '✅ Approve' });
        actions.push({ id: 'reject', label: '↩️ Reject' });
      } else if (lifecycleState === 'approved') {
        actions.push({ id: 'promote_live', label: '🚀 Promote Live' });
      }
      actions.push({ id: 'delete', label: '🗑️ Delete' });
      actions.push({ id: 'back', label: '⬅️ Back' });

      const action = await askOptionWithButtons(
        conversation,
        ctx,
        'Choose an action.',
        actions,
        { prefix: 'email-template-action', columns: 2, ensureActive, keepMessage: summaryMessage }
      );

      if (!action?.id) {
        await safeReply(ctx, selectionExpiredMessage(), { parse_mode: 'Markdown' });
        continue;
      }

      switch (action.id) {
        case 'preview':
          await previewEmailTemplate(conversation, ctx, template, ensureActive);
          break;
        case 'simulate':
          await simulateEmailTemplateFlow(conversation, ctx, template, ensureActive);
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
        case 'diff':
          await showEmailTemplateVersionDiffFlow(conversation, ctx, template, ensureActive);
          break;
        case 'rollback':
          template = await rollbackEmailTemplateVersionFlow(conversation, ctx, template, ensureActive);
          break;
        case 'submit_review':
          template = await submitEmailTemplateForReview(ctx, template.template_id);
          await safeReply(ctx, '✅ Template submitted for review.');
          break;
        case 'approve': {
          const approval = await promptReviewNote(
            conversation,
            ctx,
            'Optional approval note.',
            ensureActive
          );
          if (approval.cancelled) {
            await safeReply(ctx, 'Approval cancelled.');
            break;
          }
          template = await reviewEmailTemplate(
            ctx,
            template.template_id,
            'approve',
            approval.note
          );
          await safeReply(ctx, '✅ Template approved.');
          break;
        }
        case 'reject': {
          const rejection = await promptReviewNote(
            conversation,
            ctx,
            'Optional rejection note.',
            ensureActive
          );
          if (rejection.cancelled) {
            await safeReply(ctx, 'Rejection cancelled.');
            break;
          }
          template = await reviewEmailTemplate(
            ctx,
            template.template_id,
            'reject',
            rejection.note
          );
          await safeReply(ctx, '↩️ Template returned to draft.');
          break;
        }
        case 'promote_live': {
          const ok = await confirmAction(
            conversation,
            ctx,
            `Promote *${escapeMarkdown(template.template_id)}* to live?`,
            ensureActive
          );
          if (!ok) {
            await safeReply(ctx, 'Promotion cancelled.');
            break;
          }
          template = await promoteEmailTemplateLive(ctx, template.template_id);
          await safeReply(ctx, '🚀 Template promoted to live.');
          break;
        }
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
  } finally {
    if (summaryMessage) {
      await dismissMenuMessage(ctx, summaryMessage);
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
      await safeReplyMarkdown(ctx, section('❌ Authorization', ['Access denied. Your account is not authorized for this action.']));
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

      if (!action?.id) {
        await safeReply(ctx, selectionExpiredMessage(), { parse_mode: 'Markdown' });
        continue;
      }

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
    await replyApiError(ctx, error, 'Failed to manage templates.');
  }
}

function buildEmailMenuKeyboard(ctx) {
  const keyboard = new InlineKeyboard()
    .text('✉️ Send Email', buildCallbackData(ctx, 'EMAIL_SEND'))
    .text('📬 Delivery Status', buildCallbackData(ctx, 'EMAIL_STATUS'))
    .row()
    .text('🧩 Templates', buildCallbackData(ctx, 'EMAIL_TEMPLATES'))
    .text('🕒 History', buildCallbackData(ctx, 'EMAIL_HISTORY'))
    .row()
    .text('⬅️ Main Menu', buildCallbackData(ctx, 'MENU'));
  return keyboard;
}

async function renderEmailMenu(ctx) {
  const access = await getAccessProfile(ctx);
  startOperation(ctx, 'email-menu');
  const keyboard = buildEmailMenuKeyboard(ctx);
  const title = access.user ? '📧 *Email Center*' : '🔒 *Email Center (Access limited)*';
  const lines = [
    'Choose an email action below.',
    access.user ? 'Authorized access enabled.' : 'Limited access: request approval to send emails.',
    access.user ? '' : '🔒 Actions are locked without approval.'
  ].filter(Boolean);
  await renderMenu(ctx, `${title}\n${lines.join('\n')}`, keyboard, { parseMode: 'Markdown' });
}

async function emailStatusFlow(conversation, ctx) {
  const opId = startOperation(ctx, 'email-status');
  const ensureActive = () => ensureOperationActive(ctx, opId);
  try {
    const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
    ensureActive();
    if (!user) {
      await ctx.reply('❌ Access denied. Your account is not authorized for this action.');
      return;
    }
    await ctx.reply('📬 Enter the email message ID:');
    const update = await conversation.wait();
    ensureActive();
    const messageId = update?.message?.text?.trim();
    if (!messageId) {
      await ctx.reply('❌ Message ID is required.');
      return;
    }
    await sendEmailStatusCard(ctx, messageId, { forceReply: true });
  } catch (error) {
    console.error('Email status flow error:', error);
    await replyApiError(ctx, error, 'Failed to fetch email status.');
  }
}

async function emailHistoryFlow(ctx) {
  await ctx.reply('ℹ️ Email history is not yet available.');
}

function buildBulkEmailMenuKeyboard(ctx) {
  const keyboard = new InlineKeyboard()
    .text('🧪 Preflight', buildCallbackData(ctx, 'BULK_EMAIL_PRECHECK'))
    .text('📤 Send Bulk Email', buildCallbackData(ctx, 'BULK_EMAIL_SEND'))
    .row()
    .text('🧾 Job Status', buildCallbackData(ctx, 'BULK_EMAIL_STATUS'))
    .row()
    .text('🕒 History', buildCallbackData(ctx, 'BULK_EMAIL_LIST'))
    .text('📊 Stats', buildCallbackData(ctx, 'BULK_EMAIL_STATS'));
  return appendBackToMenuRows(keyboard, ctx, {
    backAction: 'EMAIL',
    backLabel: '⬅️ Back to Email Center'
  });
}

async function renderBulkEmailMenu(ctx) {
  const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
  const admin = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
  if (!user || !admin) {
    return ctx.reply('❌ Access denied. This action is available to administrators only.');
  }
  startOperation(ctx, 'bulk-email-menu');
  const keyboard = buildBulkEmailMenuKeyboard(ctx);
  const title = '📬 *Mailer*';
  const lines = [
    'Manage bulk email operations below.',
    'Run preflight before large campaigns.'
  ];
  await renderMenu(ctx, `${title}\n${lines.join('\n')}`, keyboard, { parseMode: 'Markdown' });
}

async function fetchEmailProviderStatus() {
  const response = await httpClient.get(null, `${config.apiUrl}/admin/provider`, {
    params: { channel: 'email' },
    timeout: 10000,
    headers: {
      'x-admin-token': config.admin.apiToken,
      'Content-Type': 'application/json'
    }
  });
  return response.data || {};
}

async function sendBulkEmailPreflightCard(ctx) {
  try {
    const status = await fetchEmailProviderStatus();
    const emailProvider = String(status.email_provider || status.provider || 'unknown').toLowerCase();
    const readinessMap = status.email_readiness || status.providers?.email?.readiness || {};
    const isReady = readinessMap[emailProvider] !== false;
    const lines = [
      buildLine('🔌', 'Provider', escapeMarkdown(emailProvider.toUpperCase())),
      buildLine('🛡️', 'Readiness', isReady ? '✅ Ready' : '⚠️ Check configuration'),
      buildLine('📬', 'Channel', 'Email'),
      '',
      isReady
        ? 'Run a low-volume test list first if this is your first campaign today.'
        : 'Readiness checks failed. Avoid live sends until provider setup is fixed.'
    ];
    const keyboard = new InlineKeyboard()
      .text('✅ Continue to Send', buildCallbackData(ctx, 'BULK_EMAIL_SEND'))
      .row()
      .text('🔄 Re-run Preflight', buildCallbackData(ctx, 'BULK_EMAIL_PRECHECK'))
      .text('⬅️ Back to Mailer', buildCallbackData(ctx, 'BULK_EMAIL'))
      .row()
      .text('⬅️ Main Menu', buildCallbackData(ctx, 'MENU'));
    await renderMenu(ctx, section('🧪 Bulk Email Preflight', lines), keyboard, { parseMode: 'Markdown' });
  } catch (error) {
    const keyboard = new InlineKeyboard()
      .text('✅ Continue to Send', buildCallbackData(ctx, 'BULK_EMAIL_SEND'))
      .row()
      .text('⬅️ Back to Mailer', buildCallbackData(ctx, 'BULK_EMAIL'))
      .row()
      .text('⬅️ Main Menu', buildCallbackData(ctx, 'MENU'));
    await renderMenu(
      ctx,
      section('⚠️ Bulk Email Preflight', [
        'Could not verify provider readiness right now.',
        'You can continue, but consider checking /provider first.'
      ]),
      keyboard,
      { parseMode: 'Markdown' }
    );
  }
}

async function fetchBulkEmailHistory(ctx, { limit = 10, offset = 0 } = {}) {
  const response = await httpClient.get(ctx, `${config.apiUrl}/email/bulk/history`, {
    params: { limit, offset },
    timeout: 15000
  });
  return response.data;
}

function buildBulkEmailHistoryKeyboard(ctx, page, hasNextPage) {
  const keyboard = new InlineKeyboard();
  if (page > 1) {
    keyboard.text('⬅️ Prev', buildCallbackData(ctx, `BULK_EMAIL_PAGE:${page - 1}`));
  }
  keyboard.text('🔄 Refresh', buildCallbackData(ctx, `BULK_EMAIL_PAGE:${page}`));
  if (hasNextPage) {
    keyboard.text('Next ➡️', buildCallbackData(ctx, `BULK_EMAIL_PAGE:${page + 1}`));
  }
  keyboard.row();
  keyboard.text('⬅️ Back to Mailer', buildCallbackData(ctx, 'BULK_EMAIL'));
  keyboard.row();
  keyboard.text('⬅️ Main Menu', buildCallbackData(ctx, 'MENU'));
  return keyboard;
}

async function sendBulkEmailHistory(ctx, { limit = BULK_EMAIL_HISTORY_PAGE_SIZE, offset = 0, page = null } = {}) {
  try {
    const safeLimit = Math.max(1, Math.min(Number(limit) || BULK_EMAIL_HISTORY_PAGE_SIZE, 20));
    const safePage = Number.isFinite(Number(page)) ? Math.max(1, Math.floor(Number(page))) : (Math.floor(offset / safeLimit) + 1);
    const safeOffset = Number.isFinite(Number(offset)) ? Math.max(0, Math.floor(Number(offset))) : ((safePage - 1) * safeLimit);
    const data = await fetchBulkEmailHistory(ctx, { limit: safeLimit, offset: safeOffset });
    const jobs = data?.jobs || [];
    if (!jobs.length) {
      await renderMenu(
        ctx,
        section('📦 Bulk Email History', ['No bulk email jobs found for that range.']),
        buildBackToMenuKeyboard(ctx, 'BULK_EMAIL', '⬅️ Back to Mailer'),
        { parseMode: 'Markdown' }
      );
      return;
    }
    const lines = jobs.map((job) => {
      const created = job.created_at ? new Date(job.created_at).toLocaleString() : 'N/A';
      return [
        `🆔 ${escapeMarkdown(job.job_id || 'unknown')}`,
        `📊 ${escapeMarkdown(job.status || 'unknown')}`,
        `📨 ${job.sent || 0}/${job.total || 0} sent`,
        `🕒 ${escapeMarkdown(created)}`
      ].join('\n');
    });
    const hasNextPage = jobs.length === safeLimit;
    const header = `📦 *Bulk Email History* (page ${safePage})\n\n${lines.join('\n\n')}`;
    await renderMenu(
      ctx,
      header,
      buildBulkEmailHistoryKeyboard(ctx, safePage, hasNextPage),
      { parseMode: 'Markdown' }
    );
  } catch (error) {
    await replyApiError(ctx, error, 'Failed to fetch bulk email history.');
  }
}

async function bulkEmailHistoryFlow(conversation, ctx) {
  const opId = startOperation(ctx, 'bulk-email-history');
  const ensureActive = () => ensureOperationActive(ctx, opId);
  try {
    const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
    const admin = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
    ensureActive();
    if (!user || !admin) {
      await ctx.reply('❌ Access denied. This action is available to administrators only.');
      return;
    }
    await ctx.reply('🕒 Enter page and limit (e.g., `1 10`). Limit max 50.', { parse_mode: 'Markdown' });
    const update = await conversation.wait();
    ensureActive();
    const raw = update?.message?.text?.trim() || '';
    const parts = raw.split(/\s+/).filter(Boolean);
    const page = Math.max(parseInt(parts[0], 10) || 1, 1);
    const limit = Math.min(Math.max(parseInt(parts[1], 10) || 10, 1), 50);
    const offset = (page - 1) * limit;
    await sendBulkEmailHistory(ctx, { limit, offset, page });
  } catch (error) {
    await replyApiError(ctx, error, 'Failed to fetch bulk email history.');
  }
}

async function fetchBulkEmailStats(ctx, { hours = 24 } = {}) {
  const response = await httpClient.get(ctx, `${config.apiUrl}/email/bulk/stats`, {
    params: { hours },
    timeout: 15000
  });
  return response.data;
}

async function sendBulkEmailStats(ctx, { hours = 24 } = {}) {
  try {
    const data = await fetchBulkEmailStats(ctx, { hours });
    const stats = data?.stats;
    if (!stats) {
      await renderMenu(
        ctx,
        section('📊 Bulk Email Stats', ['Stats are unavailable for the selected period.']),
        buildBackToMenuKeyboard(ctx, 'BULK_EMAIL', '⬅️ Back to Mailer'),
        { parseMode: 'Markdown' }
      );
      return;
    }
    const totalRecipients = Number(stats.total_recipients || 0);
    const sent = Number(stats.sent || 0);
    const failed = Number(stats.failed || 0);
    const delivered = Number(stats.delivered || 0);
    const bounced = Number(stats.bounced || 0);
    const complained = Number(stats.complained || 0);
    const suppressed = Number(stats.suppressed || 0);
    const processed = Math.max(0, Math.min(totalRecipients, sent + failed + suppressed));
    const completionRate = totalRecipients > 0 ? Math.round((processed / totalRecipients) * 100) : 0;
    const deliveredRate = totalRecipients > 0 ? Math.round((delivered / totalRecipients) * 100) : 0;
    const failureRate = totalRecipients > 0 ? Math.round((failed / totalRecipients) * 100) : 0;
    const lines = [
      `Jobs: ${stats.total_jobs || 0}`,
      `Recipients: ${totalRecipients}`,
      `Sent: ${sent}`,
      `Failed: ${failed}`,
      `Delivered: ${delivered}`,
      `Bounced: ${bounced}`,
      `Complaints: ${complained}`,
      `Suppressed: ${suppressed}`,
      `Progress: ${buildTextProgressBar(completionRate)}`,
      `Delivered rate: ${buildTextProgressBar(deliveredRate)}`,
      `Failure rate: ${buildTextProgressBar(failureRate)}`
    ];
    await renderMenu(
      ctx,
      `📊 *Bulk Email Stats (last ${data.hours || hours}h)*\n${lines.join('\n')}`,
      buildBackToMenuKeyboard(ctx, 'BULK_EMAIL', '⬅️ Back to Mailer'),
      { parseMode: 'Markdown' }
    );
  } catch (error) {
    await replyApiError(ctx, error, 'Failed to fetch bulk email stats.');
  }
}

async function bulkEmailStatsFlow(conversation, ctx) {
  const opId = startOperation(ctx, 'bulk-email-stats');
  const ensureActive = () => ensureOperationActive(ctx, opId);
  try {
    const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
    const admin = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
    ensureActive();
    if (!user || !admin) {
      await ctx.reply('❌ Access denied. This action is available to administrators only.');
      return;
    }
    await ctx.reply('📊 Enter timeframe in hours (e.g., 24 or 72).');
    const update = await conversation.wait();
    ensureActive();
    const hours = Math.min(Math.max(parseInt(update?.message?.text?.trim(), 10) || 24, 1), 720);
    await sendBulkEmailStats(ctx, { hours });
  } catch (error) {
    await replyApiError(ctx, error, 'Failed to fetch bulk email stats.');
  }
}

async function bulkEmailStatusFlow(conversation, ctx) {
  const opId = startOperation(ctx, 'bulk-email-status');
  const ensureActive = () => ensureOperationActive(ctx, opId);
  try {
    const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
    const admin = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
    ensureActive();
    if (!user || !admin) {
      await ctx.reply('❌ Access denied. This action is available to administrators only.');
      return;
    }
    await ctx.reply('🆔 Enter the bulk email job ID:');
    const update = await conversation.wait();
    ensureActive();
    const jobId = update?.message?.text?.trim();
    if (!jobId) {
      await ctx.reply('❌ Job ID is required.');
      return;
    }
    await sendBulkStatusCard(ctx, jobId, { forceReply: true });
  } catch (error) {
    console.error('Bulk email status flow error:', error);
    await replyApiError(ctx, error, 'Failed to fetch bulk email status.');
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
  const deliveredRate = total ? Math.round((delivered / total) * 100) : 0;
  const failedRate = total ? Math.round((failed / total) * 100) : 0;

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
    buildLine('📈', 'Progress', escapeMarkdown(buildTextProgressBar(progress))),
    buildLine('📬', 'Delivered rate', escapeMarkdown(buildTextProgressBar(deliveredRate))),
    buildLine('🚨', 'Failure rate', escapeMarkdown(buildTextProgressBar(failedRate)))
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
      await ctx.reply(section('❌ Authorization', ['Access denied. Your account is not authorized for this action.']), { parse_mode: 'Markdown' });
      return;
    }

    await ctx.reply(setupStepMessage('Email setup (Step 1/5)', [
      'Enter the recipient email address.'
    ]), { parse_mode: 'Markdown' });
    const toMsg = await waitForMessage();
    let toEmail = normalizeEmail(toMsg?.message?.text);
    if (!isValidEmail(toEmail)) {
      await ctx.reply(section('⚠️ Email Error', ['Invalid email address.']), { parse_mode: 'Markdown' });
      return;
    }

    await ctx.reply(section('📤 From Address (Step 2/5)', [
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
      '🧩 *Choose email mode (Step 3/5)*',
      modeOptions,
      { prefix: 'email-mode', columns: 2, ensureActive }
    );
    if (!mode) {
      await ctx.reply(cancelledMessage('Email flow', 'Use /email to start again.'), {
        parse_mode: 'Markdown',
        reply_markup: buildBackToMenuKeyboard(ctx, 'EMAIL')
      });
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
      await ctx.reply(cancelledMessage('Email send', 'Use /email to start again.'), {
        parse_mode: 'Markdown',
        reply_markup: buildBackToMenuKeyboard(ctx, 'EMAIL')
      });
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
    ]), {
      parse_mode: 'Markdown',
      reply_markup: buildBackToMenuKeyboard(ctx, 'EMAIL')
    });
    await sendEmailStatusCard(ctx, messageId, { forceReply: true });
  } catch (error) {
    console.error('Email flow error:', error);
    await ctx.reply(section('❌ Email Error', [error.message || 'Failed to send email.']), {
      parse_mode: 'Markdown',
      reply_markup: buildBackToMenuKeyboard(ctx, 'EMAIL')
    });
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
      await ctx.reply(section('❌ Authorization', ['Access denied. This action is available to administrators only.']), { parse_mode: 'Markdown' });
      return;
    }

    await ctx.reply(section('📨 Bulk Recipients (Step 1/5)', [
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

    await ctx.reply(section('📤 From Address (Step 2/5)', [
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
      '🧩 *Choose bulk email mode (Step 3/5)*',
      modeOptions,
      { prefix: 'bulk-email-mode', columns: 2, ensureActive }
    );
    if (!mode) {
      await ctx.reply(cancelledMessage('Bulk email flow', 'Use /mailer to start again.'), {
        parse_mode: 'Markdown',
        reply_markup: buildBackToMenuKeyboard(ctx, 'BULK_EMAIL', '⬅️ Back to Mailer')
      });
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
      await ctx.reply(cancelledMessage('Bulk email send', 'Use /mailer to start again.'), {
        parse_mode: 'Markdown',
        reply_markup: buildBackToMenuKeyboard(ctx, 'BULK_EMAIL', '⬅️ Back to Mailer')
      });
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
    ]), {
      parse_mode: 'Markdown',
      reply_markup: buildBackToMenuKeyboard(ctx, 'BULK_EMAIL', '⬅️ Back to Mailer')
    });
    await sendBulkStatusCard(ctx, jobId, { forceReply: true });
  } catch (error) {
    console.error('Bulk email flow error:', error);
    await replyApiError(ctx, error, 'Failed to send bulk email.');
  }
}

function registerEmailCommands(bot) {
  bot.command('email', async (ctx) => {
    try {
      await renderEmailMenu(ctx);
    } catch (error) {
      console.error('Email command error:', error);
      await ctx.reply('❌ Could not open email menu.');
    }
  });

  bot.command('mailer', async (ctx) => {
    try {
      await sendBulkEmailPreflightCard(ctx);
    } catch (error) {
      console.error('Bulk email command error:', error);
      await ctx.reply('❌ Could not open bulk email menu.');
    }
  });

  bot.command('emailstatus', async (ctx) => {
    try {
      const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
      if (!user) {
        return ctx.reply('❌ Access denied. Your account is not authorized for this action.');
      }
      const args = ctx.message?.text?.split(' ') || [];
      if (args.length < 2) {
        await sendEphemeral(ctx, 'ℹ️ /emailstatus is now under /email. Opening Email menu…');
        await maybeSendEmailAliasTip(ctx);
        await renderEmailMenu(ctx);
        return;
      }
      const messageId = args[1].trim();
      await sendEmailStatusCard(ctx, messageId, { forceReply: true });
    } catch (error) {
      console.error('Email status command error:', error);
      await replyApiError(ctx, error, 'Failed to fetch email status.');
    }
  });

}

module.exports = {
  emailFlow,
  bulkEmailFlow,
  emailTemplatesFlow,
  renderEmailMenu,
  renderBulkEmailMenu,
  emailStatusFlow,
  bulkEmailStatusFlow,
  bulkEmailHistoryFlow,
  bulkEmailStatsFlow,
  sendBulkEmailHistory,
  sendBulkEmailStats,
  sendBulkEmailPreflightCard,
  emailHistoryFlow,
  registerEmailCommands,
  sendEmailStatusCard,
  sendEmailTimeline,
  sendBulkStatusCard
};
