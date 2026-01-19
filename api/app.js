require('dotenv').config();
require('colors');

const express = require('express');
const fetch = require('node-fetch');
const ExpressWs = require('express-ws');
const path = require('path');
const crypto = require('crypto');
const rateLimit = require('express-rate-limit');

const { EnhancedGptService } = require('./routes/gpt');
const { StreamService } = require('./routes/stream');
const { TranscriptionService } = require('./routes/transcription');
const { TextToSpeechService } = require('./routes/tts');
const { recordingService } = require('./routes/recording');
const { EnhancedSmsService } = require('./routes/sms.js');
const { EmailService } = require('./routes/email');
const { createTwilioGatherHandler } = require('./routes/gather');
const Database = require('./db/db');
const { webhookService } = require('./routes/status');
const DynamicFunctionEngine = require('./functions/DynamicFunctionEngine');
const { createDigitCollectionService } = require('./functions/Digit');
const { formatDigitCaptureLabel } = require('./functions/Labels');
const config = require('./config');
const { AwsConnectAdapter, AwsTtsAdapter, VonageVoiceAdapter } = require('./adapters');
const { v4: uuidv4 } = require('uuid');
const apiPackage = require('./package.json');
const { WaveFile } = require('wavefile');

const twilio = require('twilio');
const VoiceResponse = twilio.twiml.VoiceResponse;

const liveConsoleAudioTickMs = Number.isFinite(Number(config.liveConsole?.audioTickMs))
  ? Number(config.liveConsole?.audioTickMs)
  : 160;
const liveConsoleUserLevelThreshold = Number.isFinite(Number(config.liveConsole?.userLevelThreshold))
  ? Number(config.liveConsole?.userLevelThreshold)
  : 0.08;
const liveConsoleUserHoldMs = Number.isFinite(Number(config.liveConsole?.userHoldMs))
  ? Number(config.liveConsole?.userHoldMs)
  : 450;

// Console helpers with clean emoji prefixes (idempotent, minimal noise)
if (!console.__emojiWrapped) {
  const baseLog = console.log.bind(console);
  const baseWarn = console.warn.bind(console);
  const baseError = console.error.bind(console);
  console.log = (...args) => baseLog('📘', ...args);
  console.warn = (...args) => baseWarn('⚠️', ...args);
  console.error = (...args) => baseError('❌', ...args);
  console.__emojiWrapped = true;
}

const HMAC_HEADER_TIMESTAMP = 'x-api-timestamp';
const HMAC_HEADER_SIGNATURE = 'x-api-signature';
const HMAC_BYPASS_PATH_PREFIXES = ['/webhook/', '/incoming', '/aws/transcripts', '/connection', '/vonage/stream', '/aws/stream'];

let db;
let digitService;
const functionEngine = new DynamicFunctionEngine();
let smsService = new EnhancedSmsService();
let emailService;

function stableStringify(value) {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    const items = value.map((item) => (item === undefined ? 'null' : stableStringify(item)));
    return `[${items.join(',')}]`;
  }
  const keys = Object.keys(value).filter((key) => value[key] !== undefined).sort();
  const entries = keys.map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`);
  return `{${entries.join(',')}}`;
}

function normalizeBodyForSignature(req) {
  const method = String(req.method || 'GET').toUpperCase();
  if (['GET', 'HEAD'].includes(method)) {
    return '';
  }
  const contentLength = Number(req.headers['content-length'] || 0);
  const hasBody = Number.isFinite(contentLength) && contentLength > 0;
  if (!req.body || Object.keys(req.body).length === 0) {
    return hasBody ? stableStringify(req.body || {}) : '';
  }
  return stableStringify(req.body);
}

function buildHmacPayload(req, timestamp) {
  const method = String(req.method || 'GET').toUpperCase();
  const path = req.originalUrl || req.url || '/';
  const body = normalizeBodyForSignature(req);
  return `${timestamp}.${method}.${path}.${body}`;
}

function clearCallEndLock(callSid) {
  if (callEndLocks.has(callSid)) {
    callEndLocks.delete(callSid);
  }
}

function clearSilenceTimer(callSid) {
  const timer = silenceTimers.get(callSid);
  if (timer) {
    clearTimeout(timer);
    silenceTimers.delete(callSid);
  }
}

function isCaptureActive(callSid) {
  if (!callSid) return false;
  const callConfig = callConfigurations.get(callSid);
  return callConfig?.digit_intent?.mode === 'dtmf' && callConfig?.digit_capture_active === true;
}

function resolveVoiceModel(callConfig) {
  const model = callConfig?.voice_model;
  if (model && typeof model === 'string' && model.trim()) {
    return model.trim();
  }
  return null;
}

function resolveTwilioSayVoice(callConfig) {
  const model = resolveVoiceModel(callConfig);
  if (!model) return null;
  const normalized = model.toLowerCase();
  if (['alice', 'man', 'woman'].includes(normalized)) {
    return model;
  }
  if (model.startsWith('Polly.')) {
    return model;
  }
  return null;
}

function resolveDeepgramVoiceModel(callConfig) {
  const model = callConfig?.voice_model;
  if (model && typeof model === 'string') {
    const normalized = model.toLowerCase();
    if (!['alice', 'man', 'woman'].includes(normalized) && !model.startsWith('Polly.')) {
      return model;
    }
  }
  return config.deepgram?.voiceModel || 'aura-asteria-en';
}

function shouldUseTwilioPlay(callConfig) {
  if (!config.deepgram?.apiKey) return false;
  if (!config.server?.hostname) return false;
  if (config.twilio?.ttsPlayEnabled === false) return false;
  return true;
}

function normalizeTwilioTtsText(text = '') {
  const cleaned = String(text || '').trim();
  if (!cleaned) return '';
  if (cleaned.length > TWILIO_TTS_MAX_CHARS) {
    return '';
  }
  return cleaned;
}

function buildTwilioTtsCacheKey(text, voiceModel) {
  return crypto
    .createHash('sha256')
    .update(`${voiceModel}::${text}`)
    .digest('hex');
}

function pruneTwilioTtsCache() {
  const now = Date.now();
  for (const [key, entry] of twilioTtsCache.entries()) {
    if (!entry || entry.expiresAt <= now) {
      twilioTtsCache.delete(key);
    }
  }
  if (twilioTtsCache.size <= TWILIO_TTS_CACHE_MAX) return;
  const entries = Array.from(twilioTtsCache.entries())
    .sort((a, b) => (a[1]?.createdAt || 0) - (b[1]?.createdAt || 0));
  const overflow = twilioTtsCache.size - TWILIO_TTS_CACHE_MAX;
  for (let i = 0; i < overflow; i += 1) {
    const entry = entries[i];
    if (entry) {
      twilioTtsCache.delete(entry[0]);
    }
  }
}

async function synthesizeTwilioTtsAudio(text, voiceModel) {
  const model = voiceModel || resolveDeepgramVoiceModel(null);
  const url = `https://api.deepgram.com/v1/speak?model=${encodeURIComponent(model)}&encoding=mulaw&sample_rate=8000&container=none`;
  const response = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Token ${config.deepgram.apiKey}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ text }),
    timeout: TWILIO_TTS_FETCH_TIMEOUT_MS
  });
  if (!response.ok) {
    const errorText = await response.text();
    console.error('Deepgram TTS error:', response.status, response.statusText, errorText);
    return null;
  }
  const arrayBuffer = await response.arrayBuffer();
  const mulawBuffer = Buffer.from(arrayBuffer);
  const wav = new WaveFile();
  wav.fromScratch(1, 8000, '8m', mulawBuffer);
  return {
    buffer: Buffer.from(wav.toBuffer()),
    contentType: 'audio/wav'
  };
}

async function getTwilioTtsAudioUrl(text, callConfig, options = {}) {
  const cleaned = normalizeTwilioTtsText(text);
  if (!cleaned) return null;
  if (!shouldUseTwilioPlay(callConfig)) return null;
  const cacheOnly = options?.cacheOnly === true;
  const voiceModel = resolveDeepgramVoiceModel(callConfig);
  const key = buildTwilioTtsCacheKey(cleaned, voiceModel);
  const now = Date.now();
  const cached = twilioTtsCache.get(key);
  if (cached && cached.expiresAt > now) {
    return `https://${config.server.hostname}/webhook/twilio-tts?key=${encodeURIComponent(key)}`;
  }
  const pending = twilioTtsPending.get(key);
  if (pending) {
    if (cacheOnly) {
      return null;
    }
    await pending;
    const refreshed = twilioTtsCache.get(key);
    if (refreshed && refreshed.expiresAt > Date.now()) {
      return `https://${config.server.hostname}/webhook/twilio-tts?key=${encodeURIComponent(key)}`;
    }
    return null;
  }
  if (cacheOnly) {
    const job = (async () => {
      try {
        const audio = await synthesizeTwilioTtsAudio(cleaned, voiceModel);
        if (!audio) return;
        twilioTtsCache.set(key, {
          ...audio,
          createdAt: Date.now(),
          expiresAt: Date.now() + TWILIO_TTS_CACHE_TTL_MS
        });
        pruneTwilioTtsCache();
      } catch (err) {
        console.error('Twilio TTS synthesis error:', err);
      }
    })();
    twilioTtsPending.set(key, job);
    job.finally(() => {
      if (twilioTtsPending.get(key) === job) {
        twilioTtsPending.delete(key);
      }
    });
    return null;
  }
  const job = (async () => {
    try {
      const audio = await synthesizeTwilioTtsAudio(cleaned, voiceModel);
      if (!audio) return;
      twilioTtsCache.set(key, {
        ...audio,
        createdAt: Date.now(),
        expiresAt: Date.now() + TWILIO_TTS_CACHE_TTL_MS
      });
      pruneTwilioTtsCache();
    } catch (err) {
      console.error('Twilio TTS synthesis error:', err);
    }
  })();
  twilioTtsPending.set(key, job);
  await job;
  twilioTtsPending.delete(key);
  const refreshed = twilioTtsCache.get(key);
  if (refreshed && refreshed.expiresAt > Date.now()) {
    return `https://${config.server.hostname}/webhook/twilio-tts?key=${encodeURIComponent(key)}`;
  }
  return null;
}

async function getTwilioTtsAudioUrlSafe(text, callConfig, timeoutMs = 1200) {
  const safeTimeoutMs = Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : 0;
  if (!safeTimeoutMs) {
    return getTwilioTtsAudioUrl(text, callConfig);
  }
  const timeoutPromise = new Promise((resolve) => {
    setTimeout(() => resolve(null), safeTimeoutMs);
  });
  try {
    return await Promise.race([
      getTwilioTtsAudioUrl(text, callConfig),
      timeoutPromise
    ]);
  } catch (error) {
    console.error('Twilio TTS timeout fallback:', error);
    return null;
  }
}

function maskDigitsForLog(input = '') {
  const digits = String(input || '').replace(/\D/g, '');
  if (!digits) return '0 digits';
  return `${digits.length} digits`;
}

function maskSmsBodyForLog(body = '') {
  const digits = String(body || '').replace(/\D/g, '');
  if (digits.length >= 2) {
    return `[${digits.length} digits]`;
  }
  const text = String(body || '').trim();
  if (!text) return '[empty]';
  return text.length > 80 ? `${text.slice(0, 77)}...` : text;
}

function queuePendingDigitAction(callSid, action = {}) {
  if (!callSid) return false;
  const callConfig = callConfigurations.get(callSid);
  if (!callConfig) return false;
  if (!Array.isArray(callConfig.pending_digit_actions)) {
    callConfig.pending_digit_actions = [];
  }
  callConfig.pending_digit_actions.push({
    type: action.type,
    text: action.text || '',
    reason: action.reason || null,
    scheduleTimeout: action.scheduleTimeout === true
  });
  callConfigurations.set(callSid, callConfig);
  return true;
}

function popPendingDigitActions(callSid) {
  const callConfig = callConfigurations.get(callSid);
  if (!callConfig || !Array.isArray(callConfig.pending_digit_actions) || !callConfig.pending_digit_actions.length) {
    return [];
  }
  const actions = callConfig.pending_digit_actions.slice(0);
  callConfig.pending_digit_actions = [];
  callConfigurations.set(callSid, callConfig);
  return actions;
}

function clearPendingDigitReprompts(callSid) {
  const callConfig = callConfigurations.get(callSid);
  if (!callConfig || !Array.isArray(callConfig.pending_digit_actions) || !callConfig.pending_digit_actions.length) {
    return;
  }
  callConfig.pending_digit_actions = callConfig.pending_digit_actions.filter((action) => action?.type !== 'reprompt');
  callConfigurations.set(callSid, callConfig);
}

async function handlePendingDigitActions(callSid, actions = [], gptService, interactionCount = 0) {
  if (!callSid || !actions.length) return false;
  for (const action of actions) {
    if (!action) continue;
    if (action.type === 'end') {
      const reason = action.reason || 'digits_collected';
      const message = action.text || CLOSING_MESSAGE;
      await speakAndEndCall(callSid, message, reason);
      return true;
    }
    if (action.type === 'reprompt' && gptService && action.text) {
      const personalityInfo = gptService?.personalityEngine?.getCurrentPersonality?.();
      gptService.emit('gptreply', {
        partialResponseIndex: null,
        partialResponse: action.text,
        personalityInfo,
        adaptationHistory: gptService?.personalityChanges?.slice(-3) || []
      }, interactionCount);
      if (digitService) {
        digitService.markDigitPrompted(callSid, gptService, interactionCount, 'dtmf', {
          allowCallEnd: true,
          prompt_text: action.text
        });
        if (action.scheduleTimeout) {
          digitService.scheduleDigitTimeout(callSid, gptService, interactionCount + 1);
        }
      }
    }
  }
  return true;
}

function scheduleSilenceTimer(callSid, timeoutMs = 30000) {
  if (!callSid) return;
  if (callEndLocks.has(callSid)) {
    return;
  }
  if (digitService?.hasExpectation(callSid) || isCaptureActive(callSid)) {
    return;
  }
  clearSilenceTimer(callSid);
  const timer = setTimeout(() => {
    if (!digitService?.hasExpectation(callSid) && !isCaptureActive(callSid)) {
      speakAndEndCall(callSid, CALL_END_MESSAGES.no_response, 'silence_timeout');
    }
  }, timeoutMs);
  silenceTimers.set(callSid, timer);
}

function getTranscriptAudioEntry(callSid) {
  const entry = transcriptAudioJobs.get(callSid);
  if (!entry) return null;
  if (Date.now() - entry.updatedAt > TRANSCRIPT_AUDIO_TTL_MS) {
    transcriptAudioJobs.delete(callSid);
    return null;
  }
  return entry;
}

async function generateTranscriptAudioBuffer(callSid) {
  if (!config.deepgram?.apiKey) {
    throw new Error('Deepgram API key not configured');
  }
  const call = await db.getCall(callSid);
  if (!call) {
    throw new Error('Call not found');
  }
  const transcripts = await db.getCallTranscripts(callSid);
  if (!Array.isArray(transcripts) || transcripts.length === 0) {
    throw new Error('Transcript not available');
  }
  const voiceModel = resolveVoiceModel(call) || config.deepgram.voiceModel || 'aura-asteria-en';
  const lines = transcripts.map((entry) => {
    const speaker = entry.speaker === 'user' ? 'User' : 'Agent';
    return `${speaker}: ${entry.message || ''}`.trim();
  });
  const text = lines.join('\n');
  const url = `https://api.deepgram.com/v1/speak?model=${encodeURIComponent(voiceModel)}&encoding=mp3`;
  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': `Token ${config.deepgram.apiKey}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ text })
  });
  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`TTS failed (${response.status}): ${errorText || response.statusText}`);
  }
  const audioBuffer = Buffer.from(await response.arrayBuffer());
  if (!audioBuffer.length) {
    throw new Error('Transcript audio empty');
  }
  return audioBuffer;
}

async function ensureTranscriptAudio(callSid) {
  const existing = getTranscriptAudioEntry(callSid);
  if (existing?.status === 'ready' || existing?.status === 'processing') {
    return existing;
  }
  const entry = {
    status: 'processing',
    buffer: null,
    error: null,
    updatedAt: Date.now()
  };
  transcriptAudioJobs.set(callSid, entry);
  generateTranscriptAudioBuffer(callSid)
    .then((buffer) => {
      entry.status = 'ready';
      entry.buffer = buffer;
      entry.updatedAt = Date.now();
    })
    .catch((error) => {
      entry.status = 'error';
      entry.error = error.message || 'Transcript audio failed';
      entry.updatedAt = Date.now();
    });
  return entry;
}

function estimateAudioLevelFromBase64(base64 = '') {
  if (!base64) return null;
  let buffer;
  try {
    buffer = Buffer.from(base64, 'base64');
  } catch (_) {
    return null;
  }
  if (!buffer.length) return null;
  const step = Math.max(1, Math.floor(buffer.length / 800));
  let sum = 0;
  let count = 0;
  for (let i = 0; i < buffer.length; i += step) {
    sum += Math.abs(buffer[i] - 128);
    count += 1;
  }
  if (!count) return null;
  const level = sum / (count * 128);
  return Math.max(0, Math.min(1, level));
}

function estimateAudioLevelFromBuffer(buffer, options = {}) {
  if (!Buffer.isBuffer(buffer) || !buffer.length) return null;
  const encoding = String(options.encoding || '').toLowerCase();
  if (['pcm', 'linear', 'linear16', 'l16'].includes(encoding)) {
    const minStep = 2;
    let step = Math.max(minStep, Math.floor(buffer.length / 800));
    if (step % 2 !== 0) {
      step += 1;
    }
    let sum = 0;
    let count = 0;
    for (let i = 0; i + 1 < buffer.length; i += step) {
      sum += Math.abs(buffer.readInt16LE(i));
      count += 1;
    }
    if (!count) return null;
    const level = sum / (count * 32768);
    return Math.max(0, Math.min(1, level));
  }
  const step = Math.max(1, Math.floor(buffer.length / 800));
  let sum = 0;
  let count = 0;
  for (let i = 0; i < buffer.length; i += step) {
    sum += Math.abs(buffer[i] - 128);
    count += 1;
  }
  if (!count) return null;
  const level = sum / (count * 128);
  return Math.max(0, Math.min(1, level));
}

function clampLevel(level) {
  if (!Number.isFinite(level)) return null;
  return Math.max(0, Math.min(1, level));
}

function shouldSampleUserAudioLevel(callSid, now = Date.now()) {
  const state = userAudioStates.get(callSid);
  if (!state) return true;
  return now - state.lastTickAt >= liveConsoleAudioTickMs;
}

function updateUserAudioLevel(callSid, level, now = Date.now()) {
  if (!callSid) return;
  const normalized = clampLevel(level);
  if (!Number.isFinite(normalized)) return;
  let state = userAudioStates.get(callSid);
  if (!state) {
    state = { lastTickAt: 0, lastAboveAt: 0, speaking: false };
  }
  if (now - state.lastTickAt < liveConsoleAudioTickMs) {
    return;
  }
  state.lastTickAt = now;
  const currentPhase = webhookService.getLiveConsolePhaseKey?.(callSid);
  if (normalized >= liveConsoleUserLevelThreshold) {
    state.speaking = true;
    state.lastAboveAt = now;
    userAudioStates.set(callSid, state);
    const nextPhase = (currentPhase === 'agent_speaking' || currentPhase === 'agent_responding')
      ? 'interrupted'
      : 'user_speaking';
    webhookService.setLiveCallPhase(callSid, nextPhase, { level: normalized, logEvent: false }).catch(() => {});
    return;
  }

  if (state.speaking) {
    if (now - state.lastAboveAt >= liveConsoleUserHoldMs) {
      state.speaking = false;
      userAudioStates.set(callSid, state);
      if (currentPhase !== 'agent_speaking' && currentPhase !== 'agent_responding') {
        webhookService.setLiveCallPhase(callSid, 'listening', { level: 0, logEvent: false }).catch(() => {});
      }
      return;
    }
    userAudioStates.set(callSid, state);
    if (currentPhase === 'user_speaking' || currentPhase === 'interrupted') {
      webhookService.setLiveCallPhase(callSid, currentPhase, { level: normalized, logEvent: false }).catch(() => {});
    }
  } else {
    userAudioStates.set(callSid, state);
  }
}

function estimateAudioLevelsFromBase64(base64 = '', options = {}) {
  if (!base64) return { durationMs: 0, levels: [], intervalMs: options.intervalMs || 160 };
  let buffer;
  try {
    buffer = Buffer.from(base64, 'base64');
  } catch (_) {
    return { durationMs: 0, levels: [], intervalMs: options.intervalMs || 160 };
  }
  const length = buffer.length;
  if (!length) return { durationMs: 0, levels: [], intervalMs: options.intervalMs || 160 };
  const durationMs = Math.round((length / 8000) * 1000);
  const intervalMs = Math.max(80, Number(options.intervalMs) || 160);
  const maxFrames = Number(options.maxFrames) || 48;
  const frames = Math.min(maxFrames, Math.max(1, Math.ceil(durationMs / intervalMs)));
  const bytesPerFrame = Math.max(1, Math.floor(length / frames));
  const levels = new Array(frames).fill(0);
  for (let frame = 0; frame < frames; frame += 1) {
    const start = frame * bytesPerFrame;
    const end = frame === frames - 1 ? length : Math.min(length, start + bytesPerFrame);
    const span = Math.max(1, end - start);
    const step = Math.max(1, Math.floor(span / 120));
    let sum = 0;
    let count = 0;
    for (let i = start; i < end; i += step) {
      sum += Math.abs(buffer[i] - 128);
      count += 1;
    }
    const level = count ? Math.max(0, Math.min(1, sum / (count * 128))) : 0;
    levels[frame] = level;
  }
  const effectiveInterval = frames ? Math.max(80, Math.floor(durationMs / frames)) : intervalMs;
  return { durationMs, levels, intervalMs: effectiveInterval };
}

function estimateAudioDurationMsFromBase64(base64 = '') {
  if (!base64) return 0;
  let buffer;
  try {
    buffer = Buffer.from(base64, 'base64');
  } catch (_) {
    return 0;
  }
  return Math.round((buffer.length / 8000) * 1000);
}

function estimateSpeechDurationMs(text = '') {
  const words = String(text || '')
    .trim()
    .split(/\s+/)
    .filter(Boolean).length;
  if (!words) return 0;
  const wordsPerMinute = 150;
  return Math.ceil((words / wordsPerMinute) * 60000);
}

function isGroupedGatherPlan(plan, callConfig = {}) {
  if (!plan) return false;
  const provider = callConfig?.provider || currentProvider;
  return provider === 'twilio'
    && ['banking', 'card'].includes(plan.group_id)
    && plan.capture_mode === 'ivr_gather'
    && callConfig?.digit_capture_active === true;
}

function startGroupedGather(callSid, callConfig, options = {}) {
  if (!callSid || !digitService?.sendTwilioGather || !digitService?.getPlan) return false;
  const plan = digitService.getPlan(callSid);
  if (!isGroupedGatherPlan(plan, callConfig)) return false;
  const expectation = digitService.getExpectation(callSid);
  if (!expectation) return false;
  if (expectation.prompted_at && options.force !== true) return false;
  const prompt = digitService.buildPlanStepPrompt
    ? digitService.buildPlanStepPrompt(expectation)
    : (expectation.prompt || digitService.buildDigitPrompt(expectation));
  if (!prompt) return false;
  const sayVoice = resolveTwilioSayVoice(callConfig);
  const sayOptions = sayVoice ? { voice: sayVoice } : null;
  const delayMs = Math.max(0, Number.isFinite(options.delayMs) ? options.delayMs : 0);
  const preamble = options.preamble || '';
  setTimeout(async () => {
    try {
      const activePlan = digitService.getPlan(callSid);
      const activeExpectation = digitService.getExpectation(callSid);
      if (!activePlan || !activeExpectation) return;
      if (!isGroupedGatherPlan(activePlan, callConfig)) return;
      if (activeExpectation.prompted_at && options.force !== true) return;
      if (activeExpectation.plan_id && activePlan.id && activeExpectation.plan_id !== activePlan.id) return;
      const usePlay = shouldUseTwilioPlay(callConfig);
      const ttsTimeoutMs = Number(config.twilio?.ttsMaxWaitMs) || 1200;
      const preambleUrl = usePlay ? await getTwilioTtsAudioUrlSafe(preamble, callConfig, ttsTimeoutMs) : null;
      const promptUrl = usePlay ? await getTwilioTtsAudioUrlSafe(prompt, callConfig, ttsTimeoutMs) : null;
      await digitService.sendTwilioGather(callSid, activeExpectation, {
        prompt,
        preamble,
        promptUrl,
        preambleUrl,
        sayOptions
      });
    } catch (err) {
      console.error('Grouped gather start error:', err);
    }
  }, delayMs);
  return true;
}

function clearSpeechTicks(callSid) {
  const timer = speechTickTimers.get(callSid);
  if (timer) {
    clearInterval(timer);
    speechTickTimers.delete(callSid);
  }
}

function scheduleSpeechTicks(callSid, phaseKey, durationMs, level = null, options = {}) {
  if (!callSid) return;
  clearSpeechTicks(callSid);
  const intervalMs = Math.max(80, Number(options.intervalMs) || 200);
  const levels = Array.isArray(options.levels) ? options.levels : null;
  const safeDuration = Math.max(0, Number(durationMs) || 0);
  if (!safeDuration || safeDuration <= intervalMs) {
    webhookService.setLiveCallPhase(callSid, phaseKey, { level, logEvent: false }).catch(() => {});
    return;
  }
  const start = Date.now();
  webhookService.setLiveCallPhase(callSid, phaseKey, { level, logEvent: false }).catch(() => {});
  const timer = setInterval(() => {
    const elapsed = Date.now() - start;
    if (elapsed >= safeDuration) {
      clearSpeechTicks(callSid);
      return;
    }
    let nextLevel = level;
    if (levels?.length) {
      const idx = Math.min(levels.length - 1, Math.floor((elapsed / safeDuration) * levels.length));
      if (Number.isFinite(levels[idx])) {
        nextLevel = levels[idx];
      }
    }
    webhookService.setLiveCallPhase(callSid, phaseKey, { level: nextLevel, logEvent: false }).catch(() => {});
  }, intervalMs);
  speechTickTimers.set(callSid, timer);
}

function scheduleSpeechTicksFromAudio(callSid, phaseKey, base64Audio = '') {
  if (!base64Audio) return;
  const { durationMs, levels, intervalMs } = estimateAudioLevelsFromBase64(base64Audio, { intervalMs: liveConsoleAudioTickMs, maxFrames: 48 });
  const fallbackLevel = estimateAudioLevelFromBase64(base64Audio);
  const startLevel = Number.isFinite(levels?.[0]) ? levels[0] : fallbackLevel;
  scheduleSpeechTicks(callSid, phaseKey, durationMs, startLevel, { levels, intervalMs });
}

async function applyInitialDigitIntent(callSid, callConfig, gptService = null, interactionCount = 0) {
  if (!digitService || !callConfig) return null;
  if (callConfig.digit_intent) {
    const existing = {
      intent: callConfig.digit_intent,
      expectation: digitService.getExpectation(callSid) || null
    };
    if (existing.intent?.mode === 'dtmf' && callConfig.digit_capture_active !== true) {
      callConfig.digit_capture_active = true;
      callConfigurations.set(callSid, callConfig);
    }
    if (existing.intent?.mode === 'dtmf' && existing.expectation) {
      try {
        await digitService.flushBufferedDigits(callSid, gptService, interactionCount, 'dtmf', { allowCallEnd: true });
      } catch (err) {
        console.error('Flush buffered digits error:', err);
      }
    }
    return existing;
  }
  const result = digitService.prepareInitialExpectation(callSid, callConfig);
  callConfig.digit_intent = result.intent;
  if (result.intent?.mode === 'dtmf') {
    callConfig.digit_capture_active = true;
  } else {
    callConfig.digit_capture_active = false;
  }
  callConfigurations.set(callSid, callConfig);
  if (result.intent?.mode === 'dtmf' && Array.isArray(result.plan_steps) && result.plan_steps.length) {
    webhookService.addLiveEvent(callSid, formatDigitCaptureLabel(result.intent, result.expectation), { force: true });
  } else if (result.intent?.mode === 'dtmf' && result.expectation) {
    webhookService.addLiveEvent(callSid, `🔢 DTMF intent detected (${result.intent.reason})`, { force: true });
  } else {
    webhookService.addLiveEvent(callSid, `🗣️ Normal call flow (${result.intent?.reason || 'no_signal'})`, { force: true });
  }
  if (result.intent?.mode === 'dtmf' && Array.isArray(result.plan_steps) && result.plan_steps.length) {
    webhookService.addLiveEvent(callSid, `🧭 Digit capture plan started (${result.intent.group_id || 'group'})`, { force: true });
    const provider = callConfig?.provider || currentProvider;
    const isGroupedPlan = ['banking', 'card'].includes(result.intent.group_id);
    const deferTwiml = provider === 'twilio' && isGroupedPlan;
    await digitService.requestDigitCollectionPlan(callSid, {
      steps: result.plan_steps,
      end_call_on_success: true,
      group_id: result.intent.group_id,
      capture_mode: 'ivr_gather',
      defer_twiml: deferTwiml
    }, gptService);
    return result;
  }
  if (result.intent?.mode === 'dtmf' && result.expectation) {
    try {
      await digitService.flushBufferedDigits(callSid, gptService, interactionCount, 'dtmf', { allowCallEnd: true });
    } catch (err) {
      console.error('Flush buffered digits error:', err);
    }
  }
  return result;
}

function resolveHost(req) {
  return config.server?.hostname
    || req?.headers?.['x-forwarded-host']
    || req?.headers?.host
    || '';
}

function buildTwilioStreamTwiml(hostname) {
  const response = new VoiceResponse();
  const connect = response.connect();
  const host = hostname || config.server.hostname;
  connect.stream({ url: `wss://${host}/connection`, track: TWILIO_STREAM_TRACK });
  return response.toString();
}

function shouldBypassHmac(req) {
  const path = req.path || '';
  if (!path) return false;
  if (req.method === 'GET' && (path === '/' || path === '/favicon.ico' || path === '/health')) {
    return true;
  }
  if (path.startsWith('/webhook/')) return true;
  return HMAC_BYPASS_PATH_PREFIXES.some((prefix) => path.startsWith(prefix));
}

function verifyHmacSignature(req) {
  const secret = config.apiAuth?.hmacSecret;
  if (!secret) {
    return { ok: true, skipped: true, reason: 'missing_secret' };
  }

  const timestampHeader = req.headers[HMAC_HEADER_TIMESTAMP];
  const signatureHeader = req.headers[HMAC_HEADER_SIGNATURE];

  if (!timestampHeader || !signatureHeader) {
    return { ok: false, reason: 'missing_headers' };
  }

  const timestamp = Number(timestampHeader);
  if (!Number.isFinite(timestamp)) {
    return { ok: false, reason: 'invalid_timestamp' };
  }

  const maxSkewMs = Number(config.apiAuth?.maxSkewMs || 300000);
  const now = Date.now();
  if (Math.abs(now - timestamp) > maxSkewMs) {
    return { ok: false, reason: 'timestamp_out_of_range' };
  }

  const payload = buildHmacPayload(req, String(timestampHeader));
  const expected = crypto.createHmac('sha256', secret).update(payload).digest('hex');

  try {
    const expectedBuf = Buffer.from(expected, 'hex');
    const providedBuf = Buffer.from(String(signatureHeader), 'hex');
    if (expectedBuf.length !== providedBuf.length) {
      return { ok: false, reason: 'invalid_signature' };
    }
    if (!crypto.timingSafeEqual(expectedBuf, providedBuf)) {
      return { ok: false, reason: 'invalid_signature' };
    }
  } catch (error) {
    return { ok: false, reason: 'invalid_signature' };
  }

  return { ok: true };
}

function getTwilioWebhookUrl(req) {
  const host = resolveHost(req);
  if (!host) {
    return null;
  }
  return `https://${host}${req.originalUrl}`;
}

function validateTwilioRequest(req) {
  const signature = req.headers['x-twilio-signature'];
  const authToken = config.twilio.authToken;
  const url = getTwilioWebhookUrl(req);
  if (!signature || !authToken || !url) {
    return false;
  }
  return twilio.validateRequest(authToken, signature, url, req.body);
}

function warnOnInvalidTwilioSignature(req, label = '') {
  const valid = validateTwilioRequest(req);
  if (!valid) {
    const path = label || req.originalUrl || req.path || 'unknown';
    console.warn(`⚠️ Twilio signature invalid for ${path}`);
  }
  return valid;
}

function selectWsProtocol(protocols) {
  if (!protocols) return false;
  if (Array.isArray(protocols) && protocols.length) return protocols[0];
  if (protocols instanceof Set) {
    const iter = protocols.values().next();
    return iter.done ? false : iter.value;
  }
  if (typeof protocols === 'string') return protocols;
  return false;
}

const app = express();
ExpressWs(app, null, {
  wsOptions: {
    handleProtocols: (protocols) => selectWsProtocol(protocols)
  }
});
// Trust the first proxy (ngrok/load balancer) so rate limiting can read X-Forwarded-For safely
app.set('trust proxy', 1);

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const apiLimiter = rateLimit({
  windowMs: config.server?.rateLimit?.windowMs || 60000,
  max: config.server?.rateLimit?.max || 300,
  standardHeaders: true,
  legacyHeaders: false,
});

app.use((req, res, next) => {
  if (shouldBypassHmac(req)) {
    return next();
  }

  const verification = verifyHmacSignature(req);
  if (!verification.ok) {
    console.warn(`⚠️ Rejected request due to invalid HMAC (${verification.reason}) ${req.method} ${req.originalUrl}`);
    return res.status(401).json({ success: false, error: 'Unauthorized' });
  }
  return next();
});

app.use((req, res, next) => {
  if (shouldBypassHmac(req)) {
    return next();
  }
  return apiLimiter(req, res, next);
});

const PORT = config.server?.port || 3000;

// Enhanced call configurations with function context
const callConfigurations = new Map();
const activeCalls = new Map();
const callFunctionSystems = new Map(); // Store generated functions per call
const callEndLocks = new Map();
const gatherEventDedupe = new Map();
const silenceTimers = new Map();
const transcriptAudioJobs = new Map();
const TRANSCRIPT_AUDIO_TTL_MS = 60 * 60 * 1000;
const twilioTtsCache = new Map();
const twilioTtsPending = new Map();
const TWILIO_TTS_CACHE_TTL_MS = Number(config.twilio?.ttsCacheTtlMs) || 10 * 60 * 1000;
const TWILIO_TTS_CACHE_MAX = Number(config.twilio?.ttsCacheMax) || 200;
const TWILIO_TTS_MAX_CHARS = Number(config.twilio?.ttsMaxChars) || 500;
const TWILIO_TTS_FETCH_TIMEOUT_MS = Number(config.twilio?.ttsFetchTimeoutMs) || 4000;
const pendingStreams = new Map(); // callSid -> timeout to detect missing websocket
const gptQueues = new Map();
const normalFlowBuffers = new Map();
const normalFlowProcessing = new Set();
const normalFlowLastInput = new Map();
const speechTickTimers = new Map();
const userAudioStates = new Map();

function enqueueGptTask(callSid, task) {
  if (!callSid || typeof task !== 'function') {
    return Promise.resolve();
  }
  const current = gptQueues.get(callSid) || Promise.resolve();
  const next = current
    .then(task)
    .catch((err) => {
      console.error('GPT queue error:', err);
    })
    .finally(() => {
      if (gptQueues.get(callSid) === next) {
        gptQueues.delete(callSid);
      }
    });
  gptQueues.set(callSid, next);
  return next;
}

function clearGptQueue(callSid) {
  if (callSid) {
    gptQueues.delete(callSid);
  }
}

function clearNormalFlowState(callSid) {
  if (!callSid) return;
  normalFlowBuffers.delete(callSid);
  normalFlowProcessing.delete(callSid);
  normalFlowLastInput.delete(callSid);
}

function shouldSkipNormalInput(callSid, text, windowMs = 2000) {
  const cleaned = String(text || '').trim();
  if (!cleaned) return true;
  const last = normalFlowLastInput.get(callSid);
  const now = Date.now();
  if (last && last.text === cleaned && now - last.at < windowMs) {
    return true;
  }
  normalFlowLastInput.set(callSid, { text: cleaned, at: now });
  return false;
}

async function processNormalFlowTranscript(callSid, text, gptService, getInteractionCount, setInteractionCount) {
  if (!callSid || !gptService) return;
  const cleaned = String(text || '').trim();
  if (!cleaned) return;
  if (shouldSkipNormalInput(callSid, cleaned)) return;

  normalFlowBuffers.set(callSid, { text: cleaned, at: Date.now() });
  if (normalFlowProcessing.has(callSid)) {
    return;
  }
  normalFlowProcessing.add(callSid);
  try {
    while (normalFlowBuffers.has(callSid)) {
      const next = normalFlowBuffers.get(callSid);
      normalFlowBuffers.delete(callSid);
      await enqueueGptTask(callSid, async () => {
        if (callEndLocks.has(callSid)) return;
        const session = activeCalls.get(callSid);
        if (session?.ending) return;
        const currentCount = typeof getInteractionCount === 'function' ? getInteractionCount() : 0;
        try {
          await gptService.completion(next.text, currentCount);
        } catch (gptError) {
          console.error('GPT completion error:', gptError);
          webhookService.addLiveEvent(callSid, '⚠️ GPT error, retrying', { force: true });
        }
        const nextCount = currentCount + 1;
        if (typeof setInteractionCount === 'function') {
          setInteractionCount(nextCount);
        }
      });
    }
  } finally {
    normalFlowProcessing.delete(callSid);
  }
}

const ALLOWED_TWILIO_STREAM_TRACKS = new Set(['inbound_track', 'outbound_track', 'both_tracks']);
const TWILIO_STREAM_TRACK = ALLOWED_TWILIO_STREAM_TRACKS.has((process.env.TWILIO_STREAM_TRACK || '').toLowerCase())
  ? process.env.TWILIO_STREAM_TRACK.toLowerCase()
  : 'inbound_track';

const CALL_END_MESSAGES = {
  success: 'Thanks, we have what we need. Goodbye.',
  failure: 'We could not verify the information provided. Thank you for your time. Goodbye.',
  no_response: 'We did not receive a response. Thank you and goodbye.',
  user_goodbye: 'Thanks for your time. Goodbye.',
  error: 'I am having trouble right now. Thank you and goodbye.'
};
const CLOSING_MESSAGE = 'Thank you—your input has been received. Your request is complete. Goodbye.';
const DIGIT_SETTINGS = {
  otpLength: 6,
  otpMaxRetries: 3,
  otpDisplayMode: 'masked',
  defaultCollectDelayMs: 1200,
  fallbackToVoiceOnFailure: true,
  showRawDigitsLive: String(process.env.SHOW_RAW_DIGITS_LIVE || 'true').toLowerCase() === 'true',
  sendRawDigitsToUser: String(process.env.SEND_RAW_DIGITS_TO_USER || 'true').toLowerCase() === 'true',
  minDtmfGapMs: 200,
  riskThresholds: {
    confirm: Number(process.env.DIGIT_RISK_CONFIRM || 0.55),
    dtmf_only: Number(process.env.DIGIT_RISK_DTMF_ONLY || 0.7),
    route_agent: Number(process.env.DIGIT_RISK_ROUTE_AGENT || 0.9)
  },
  smsFallbackEnabled: String(process.env.DIGIT_SMS_FALLBACK_ENABLED || 'true').toLowerCase() === 'true',
  smsFallbackMinRetries: Number(process.env.DIGIT_SMS_FALLBACK_MIN_RETRIES || 2),
  healthThresholds: {
    degraded: Number(process.env.DIGIT_HEALTH_DEGRADED || 30),
    overloaded: Number(process.env.DIGIT_HEALTH_OVERLOADED || 60)
  },
  circuitBreaker: {
    windowMs: Number(process.env.DIGIT_BREAKER_WINDOW_MS || 60000),
    minSamples: Number(process.env.DIGIT_BREAKER_MIN_SAMPLES || 8),
    errorRate: Number(process.env.DIGIT_BREAKER_ERROR_RATE || 0.3),
    cooldownMs: Number(process.env.DIGIT_BREAKER_COOLDOWN_MS || 60000)
  }
};

function getDigitSystemHealth() {
  const active = callConfigurations.size;
  const thresholds = DIGIT_SETTINGS.healthThresholds || {};
  const status = active >= thresholds.overloaded
    ? 'overloaded'
    : active >= thresholds.degraded
      ? 'degraded'
      : 'healthy';
  return { status, load: active };
}

// Built-in telephony function scripts to give GPT deterministic controls
const telephonyTools = [
  {
    type: 'function',
    function: {
      name: 'confirm_identity',
      description: 'Log that the caller has been identity-verified (do not include the code) and proceed to the next step.',
      parameters: {
        type: 'object',
        properties: {
          method: { type: 'string', enum: ['otp', 'pin', 'knowledge', 'other'], description: 'Verification method used.' },
          note: { type: 'string', description: 'Brief note about what was confirmed (no sensitive values).' }
        },
        required: ['method']
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'route_to_agent',
      description: 'End the call politely (no transfer) when escalation is requested.',
      parameters: {
        type: 'object',
        properties: {
          reason: { type: 'string', description: 'Short reason for the transfer.' },
          priority: { type: 'string', enum: ['low', 'normal', 'high'], description: 'Transfer priority if applicable.' }
        },
        required: ['reason']
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'collect_digits',
      description: 'Ask caller to enter digits on the keypad (e.g., OTP). Do not speak or repeat the digits.',
      parameters: {
        type: 'object',
        properties: {
          prompt: { type: 'string', description: 'Short instruction to the caller.' },
          min_digits: { type: 'integer', description: 'Minimum digits expected.', minimum: 1 },
          max_digits: { type: 'integer', description: 'Maximum digits expected.', minimum: 1 },
          profile: { type: 'string', enum: ['generic', 'verification', 'ssn', 'dob', 'routing_number', 'account_number', 'phone', 'tax_id', 'ein', 'claim_number', 'reservation_number', 'ticket_number', 'case_number', 'account', 'extension', 'zip', 'amount', 'callback_confirm', 'card_number', 'cvv', 'card_expiry'], description: 'Collection profile for downstream handling.' },
          confirmation_style: { type: 'string', enum: ['none', 'last4', 'spoken_amount'], description: 'How to confirm receipt (masked, spoken summary only).' },
          timeout_s: { type: 'integer', description: 'Timeout in seconds before reprompt.', minimum: 3 },
          max_retries: { type: 'integer', description: 'Number of retries before fallback.', minimum: 0 },
          end_call_on_success: { type: 'boolean', description: 'If false, keep the call active after digits are captured.' },
          allow_spoken_fallback: { type: 'boolean', description: 'If true, allow spoken fallback after keypad timeout.' },
          mask_for_gpt: { type: 'boolean', description: 'If true (default), mask digits before sending to GPT/transcripts.' },
          speak_confirmation: { type: 'boolean', description: 'If true, GPT can verbally confirm receipt (without echoing digits).' },
          allow_terminator: { type: 'boolean', description: 'If true, allow a terminator key (default #) to finish early.' },
          terminator_char: { type: 'string', description: 'Single key used to end entry when allow_terminator is true.' }
        },
        required: ['prompt', 'min_digits', 'max_digits']
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'collect_multiple_digits',
      description: 'Collect multiple digit profiles sequentially in a single call (e.g., card number, expiry, CVV, ZIP). Do not repeat digits.',
      parameters: {
        type: 'object',
        properties: {
          steps: {
            type: 'array',
            description: 'Ordered list of digit collection steps.',
            items: {
              type: 'object',
              properties: {
                prompt: { type: 'string', description: 'Short instruction to the caller.' },
                min_digits: { type: 'integer', description: 'Minimum digits expected.', minimum: 1 },
                max_digits: { type: 'integer', description: 'Maximum digits expected.', minimum: 1 },
                profile: { type: 'string', enum: ['generic', 'verification', 'ssn', 'dob', 'routing_number', 'account_number', 'phone', 'tax_id', 'ein', 'claim_number', 'reservation_number', 'ticket_number', 'case_number', 'account', 'extension', 'zip', 'amount', 'callback_confirm', 'card_number', 'cvv', 'card_expiry'], description: 'Collection profile for downstream handling.' },
                confirmation_style: { type: 'string', enum: ['none', 'last4', 'spoken_amount'], description: 'How to confirm receipt (masked, spoken summary only).' },
                timeout_s: { type: 'integer', description: 'Timeout in seconds before reprompt.', minimum: 3 },
                max_retries: { type: 'integer', description: 'Number of retries before fallback.', minimum: 0 },
                allow_spoken_fallback: { type: 'boolean', description: 'If true, allow spoken fallback after keypad timeout.' },
                mask_for_gpt: { type: 'boolean', description: 'If true (default), mask digits before sending to GPT/transcripts.' },
                speak_confirmation: { type: 'boolean', description: 'If true, GPT can verbally confirm receipt (without echoing digits).' },
                allow_terminator: { type: 'boolean', description: 'If true, allow a terminator key (default #) to finish early.' },
                terminator_char: { type: 'string', description: 'Single key used to end entry when allow_terminator is true.' },
                end_call_on_success: { type: 'boolean', description: 'If false, keep the call active after this step.' }
              },
              required: ['profile']
            }
          },
          end_call_on_success: { type: 'boolean', description: 'If false, keep the call active after all steps are captured.' },
          completion_message: { type: 'string', description: 'Optional message to speak after the final step when not ending the call.' }
        },
        required: ['steps']
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'play_disclosure',
      description: 'Play or read a required disclosure to the caller. Keep it concise.',
      parameters: {
        type: 'object',
        properties: {
          message: { type: 'string', description: 'Disclosure text to convey.' }
        },
        required: ['message']
      }
    }
  }
];

function buildTelephonyImplementations(callSid, gptService = null) {
  return {
    confirm_identity: async (args = {}) => {
      const payload = {
        status: 'acknowledged',
        method: args.method || 'unspecified',
        note: args.note || ''
      };
      try {
        await db.updateCallState(callSid, 'identity_confirmed', payload);
        webhookService.addLiveEvent(callSid, `✅ Identity confirmed (${payload.method})`, { force: true });
      } catch (err) {
        console.error('confirm_identity handler error:', err);
      }
      return payload;
    },
    route_to_agent: async (args = {}) => {
      const payload = {
        status: 'queued',
        reason: args.reason || 'unspecified',
        priority: args.priority || 'normal'
      };
      try {
        webhookService.addLiveEvent(callSid, `📞 Transfer requested (${payload.reason}) • ending call`, { force: true });
        await speakAndEndCall(callSid, CALL_END_MESSAGES.failure, 'transfer_requested');
      } catch (err) {
        console.error('route_to_agent handler error:', err);
      }
      return payload;
    },
    collect_digits: async (args = {}) => {
      if (!digitService) {
        return { error: 'Digit service not ready' };
      }
      return digitService.requestDigitCollection(callSid, args, gptService);
    },
    collect_multiple_digits: async (args = {}) => {
      if (!digitService) {
        return { error: 'Digit service not ready' };
      }
      return digitService.requestDigitCollectionPlan(callSid, args, gptService);
    },
    play_disclosure: async (args = {}) => {
      const payload = { message: args.message || '' };
      try {
        await db.updateCallState(callSid, 'disclosure_played', payload);
        webhookService.addLiveEvent(callSid, '📢 Disclosure played', { force: true });
      } catch (err) {
        console.error('play_disclosure handler error:', err);
      }
      return payload;
    }
  };
}

function applyTelephonyTools(gptService, callSid, baseTools = [], baseImpl = {}, options = {}) {
  const allowTransfer = options.allowTransfer !== false;
  const allowDigitCollection = options.allowDigitCollection !== false;
  const normalizedName = (tool) => String(tool?.function?.name || '').trim().toLowerCase();

  const filteredBaseTools = (Array.isArray(baseTools) ? baseTools : []).filter((tool) => {
    const name = normalizedName(tool);
    if (!name) return false;
    if (!allowTransfer && (name === 'route_to_agent' || name === 'transfercall')) return false;
    if (!allowDigitCollection && (name === 'collect_digits' || name === 'collect_multiple_digits')) return false;
    return true;
  });

  const filteredTelephonyTools = telephonyTools.filter((tool) => {
    const name = normalizedName(tool);
    if (!allowTransfer && name === 'route_to_agent') return false;
    if (!allowDigitCollection && (name === 'collect_digits' || name === 'collect_multiple_digits')) return false;
    return true;
  });

  const combinedTools = [...filteredBaseTools, ...filteredTelephonyTools];
  const combinedImpl = { ...baseImpl, ...buildTelephonyImplementations(callSid, gptService) };
  if (!allowTransfer) {
    delete combinedImpl.route_to_agent;
    delete combinedImpl.transferCall;
    delete combinedImpl.transfercall;
  }
  if (!allowDigitCollection) {
    delete combinedImpl.collect_digits;
    delete combinedImpl.collect_multiple_digits;
  }
  gptService.setDynamicFunctions(combinedTools, combinedImpl);
}

function getCallToolOptions(callConfig = {}) {
  const isDigitIntent = callConfig?.digit_intent?.mode === 'dtmf';
  return {
    allowTransfer: isDigitIntent,
    allowDigitCollection: isDigitIntent
  };
}

function configureCallTools(gptService, callSid, callConfig, functionSystem) {
  if (!gptService) return;
  const baseTools = functionSystem?.functions || [];
  const baseImpl = functionSystem?.implementations || {};
  const options = getCallToolOptions(callConfig);
  applyTelephonyTools(gptService, callSid, baseTools, baseImpl, options);
  if (!options.allowTransfer && callConfig && !callConfig.no_transfer_note_added) {
    gptService.setCallIntent('Constraint: do not transfer or escalate this call. Stay on the line and handle the customer end-to-end.');
    callConfig.no_transfer_note_added = true;
    callConfigurations.set(callSid, callConfig);
  }
}

function formatDurationForSms(seconds) {
  if (!seconds || Number.isNaN(seconds)) return '';
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  if (mins === 0) {
    return `${secs}s`;
  }
  return `${mins}m ${secs}s`;
}

function normalizeCallStatus(value) {
  return String(value || '').toLowerCase().replace(/_/g, '-');
}

function formatContactLabel(call) {
  if (call?.customer_name) return call.customer_name;
  if (call?.victim_name) return call.victim_name;
  const digits = String(call?.phone_number || call?.number || '').replace(/\D/g, '');
  if (digits.length >= 4) {
    return `the contact ending ${digits.slice(-4)}`;
  }
  return 'the contact';
}

function buildOutcomeSummary(call, status) {
  const label = formatContactLabel(call);
  switch (status) {
    case 'no-answer':
      return `${label} didn't pick up the call.`;
    case 'busy':
      return `${label}'s line was busy.`;
    case 'failed':
      return `Call failed to reach ${label}.`;
    case 'canceled':
      return `Call to ${label} was canceled.`;
    default:
      return 'Call finished.';
  }
}

function buildRecapSmsBody(call) {
  const nameValue = call.customer_name || call.victim_name;
  const name = nameValue ? ` with ${nameValue}` : '';
  const normalizedStatus = normalizeCallStatus(call.status || call.twilio_status || 'completed');
  const status = normalizedStatus.replace(/_/g, ' ');
  const duration = call.duration ? ` Duration: ${formatDurationForSms(call.duration)}.` : '';
  const rawSummary = (call.call_summary || '').replace(/\s+/g, ' ').trim();
  const summary = normalizedStatus === 'completed'
    ? (rawSummary ? rawSummary.slice(0, 180) : 'Call finished.')
    : buildOutcomeSummary(call, normalizedStatus);
  return `VoicedNut call recap${name}: ${summary} Status: ${status}.${duration}`;
}

function buildRetrySmsBody(callRecord, callState) {
  const name = callState?.customer_name || callState?.victim_name || callRecord?.customer_name || callRecord?.victim_name;
  const greeting = name ? `Hi ${name},` : 'Hi,';
  return `${greeting} we tried to reach you by phone. When is a good time to call back?`;
}

const DIGIT_PROFILE_LABELS = {
  verification: 'OTP',
  otp: 'OTP',
  ssn: 'SSN',
  dob: 'DOB',
  routing_number: 'Routing',
  account_number: 'Account #',
  phone: 'Phone',
  tax_id: 'Tax ID',
  ein: 'EIN',
  claim_number: 'Claim',
  reservation_number: 'Reservation',
  ticket_number: 'Ticket',
  case_number: 'Case',
  account: 'Account',
  zip: 'ZIP',
  extension: 'Ext',
  amount: 'Amount',
  callback_confirm: 'Callback',
  card_number: 'Card',
  cvv: 'CVV',
  card_expiry: 'Expiry',
  generic: 'Digits'
};

function buildDigitSummary(digitEvents = []) {
  if (!Array.isArray(digitEvents) || digitEvents.length === 0) {
    return { summary: '', count: 0 };
  }

  const grouped = new Map();
  for (const event of digitEvents) {
    const profile = event.profile || 'generic';
    if (!grouped.has(profile)) {
      grouped.set(profile, []);
    }
    grouped.get(profile).push(event);
  }

  const parts = [];
  let acceptedCount = 0;

  for (const [profile, events] of grouped.entries()) {
    const acceptedEvents = events.filter((e) => e.accepted);
    const chosen = acceptedEvents.length ? acceptedEvents[acceptedEvents.length - 1] : events[events.length - 1];
    const label = DIGIT_PROFILE_LABELS[profile] || profile;
    let value = chosen.digits || '';

    if (profile === 'amount' && value) {
      const cents = Number(value);
      if (!Number.isNaN(cents)) {
        value = `$${(cents / 100).toFixed(2)}`;
      }
    }
    if (profile === 'card_expiry' && value) {
      if (value.length === 4) {
        value = `${value.slice(0, 2)}/${value.slice(2)}`;
      } else if (value.length === 6) {
        value = `${value.slice(0, 2)}/${value.slice(2)}`;
      }
    }

    if (!value) {
      value = 'none';
    }

    const suffix = chosen.accepted ? '' : ' (unverified)';
    if (chosen.accepted) {
      acceptedCount += 1;
    }
    parts.push(`${label}: ${value}${suffix}`);
  }

  return {
    summary: parts.join(' • '),
    count: acceptedCount
  };
}

function shouldCloseConversation(text = '') {
  const lower = String(text || '').toLowerCase();
  if (!lower) return false;
  return !!lower.match(/\b(thanks|thank you|bye|goodbye|appreciate|that.s all|that is all|have a good|bye bye)\b/);
}

const ADMIN_HEADER_NAME = 'x-admin-token';
const SUPPORTED_PROVIDERS = ['twilio', 'aws', 'vonage'];
let currentProvider = config.platform?.provider || 'twilio';
let storedProvider = currentProvider;
const awsContactMap = new Map();
const vonageCallMap = new Map();

let awsConnectAdapter = null;
let awsTtsAdapter = null;
let vonageVoiceAdapter = null;

const builtinPersonas = [
  {
    id: 'general',
    label: 'General',
    description: 'General voice call assistant',
    purposes: [{ id: 'general', label: 'General' }],
    default_purpose: 'general',
    default_emotion: 'neutral',
    default_urgency: 'normal',
    default_technical_level: 'general'
  }
];

function requireAdminToken(req, res, next) {
  const token = config.admin?.apiToken;
  if (!token) {
    return res.status(500).json({ success: false, error: 'Admin token not configured' });
  }
  const provided = req.headers[ADMIN_HEADER_NAME];
  if (!provided || provided !== token) {
    return res.status(403).json({ success: false, error: 'Admin token required' });
  }
  return next();
}

function getProviderReadiness() {
  return {
    twilio: !!(config.twilio.accountSid && config.twilio.authToken && config.twilio.fromNumber),
    aws: !!(config.aws.connect.instanceId && config.aws.connect.contactFlowId),
    vonage: !!(config.vonage.apiKey && config.vonage.apiSecret && config.vonage.applicationId && config.vonage.privateKey)
  };
}

let warnedMachineDetection = false;
function isMachineDetectionEnabled() {
  const value = String(config.twilio?.machineDetection || '').toLowerCase();
  if (!value) return false;
  if (['disable', 'disabled', 'off', 'false', '0', 'none'].includes(value)) return false;
  return true;
}

function warnIfMachineDetectionDisabled(context = '') {
  if (warnedMachineDetection) return;
  if (currentProvider !== 'twilio') return;
  if (isMachineDetectionEnabled()) return;
  const suffix = context ? ` (${context})` : '';
  console.warn(`⚠️ Twilio AMD is not enabled${suffix}. Voicemail detection may be unreliable. Set TWILIO_MACHINE_DETECTION=Enable.`);
  warnedMachineDetection = true;
}

function getAwsConnectAdapter() {
  if (!awsConnectAdapter) {
    awsConnectAdapter = new AwsConnectAdapter(config.aws);
  }
  return awsConnectAdapter;
}

function getVonageVoiceAdapter() {
  if (!vonageVoiceAdapter) {
    vonageVoiceAdapter = new VonageVoiceAdapter(config.vonage);
  }
  return vonageVoiceAdapter;
}

function getAwsTtsAdapter() {
  if (!awsTtsAdapter) {
    awsTtsAdapter = new AwsTtsAdapter(config.aws);
  }
  return awsTtsAdapter;
}

async function endCallForProvider(callSid) {
  const callConfig = callConfigurations.get(callSid);
  const provider = callConfig?.provider || currentProvider;

  if (provider === 'twilio') {
    const accountSid = config.twilio.accountSid;
    const authToken = config.twilio.authToken;
    if (!accountSid || !authToken) {
      throw new Error('Twilio credentials not configured');
    }
    const client = twilio(accountSid, authToken);
    await client.calls(callSid).update({ status: 'completed' });
    return;
  }

  if (provider === 'aws') {
    const contactId = callConfig?.provider_metadata?.contact_id;
    if (!contactId) {
      throw new Error('AWS contact id not available');
    }
    const awsAdapter = getAwsConnectAdapter();
    await awsAdapter.stopContact({ contactId });
    return;
  }

  if (provider === 'vonage') {
    const callUuid = callConfig?.provider_metadata?.vonage_uuid || callSid;
    const vonageAdapter = getVonageVoiceAdapter();
    await vonageAdapter.hangupCall(callUuid);
    return;
  }

  throw new Error(`Unsupported provider ${provider}`);
}

function estimateSpeechDurationMs(text = '') {
  const words = String(text || '').trim().split(/\s+/).filter(Boolean).length;
  const baseMs = 1200;
  const perWordMs = 420;
  const estimated = baseMs + (words * perWordMs);
  return Math.max(1600, Math.min(12000, estimated));
}

async function speakAndEndCall(callSid, message, reason = 'completed') {
  if (!callSid || callEndLocks.has(callSid)) {
    return;
  }
  callEndLocks.set(callSid, true);
  clearSilenceTimer(callSid);
  if (digitService) {
    digitService.clearCallState(callSid);
  }

  const text = message || 'Thank you for your time. Goodbye.';
  const callConfig = callConfigurations.get(callSid);
  const provider = callConfig?.provider || currentProvider;
  const session = activeCalls.get(callSid);
  if (session) {
    session.ending = true;
  }

  webhookService.addLiveEvent(callSid, `👋 Ending call (${reason})`, { force: true });
  webhookService.setLiveCallPhase(callSid, 'ending').catch(() => {});

  try {
    await db.addTranscript({
      call_sid: callSid,
      speaker: 'ai',
      message: text,
      interaction_count: session?.interactionCount || 0,
      personality_used: 'closing'
    });
    webhookService.recordTranscriptTurn(callSid, 'agent', text);
  } catch (dbError) {
    console.error('Database error adding closing transcript:', dbError);
  }

  try {
    await db.updateCallState(callSid, 'call_ending', {
      reason,
      message: text
    });
  } catch (stateError) {
    console.error('Database error logging call ending:', stateError);
  }

  const delayMs = estimateSpeechDurationMs(text);

  if (provider === 'aws') {
      try {
        const ttsAdapter = getAwsTtsAdapter();
        const voiceId = resolveVoiceModel(callConfig);
        const { key } = await ttsAdapter.synthesizeToS3(text, voiceId ? { voiceId } : {});
        const contactId = callConfig?.provider_metadata?.contact_id;
        if (contactId) {
          const awsAdapter = getAwsConnectAdapter();
          await awsAdapter.enqueueAudioPlayback({ contactId, audioKey: key });
        }
        scheduleSpeechTicks(callSid, 'agent_speaking', estimateSpeechDurationMs(text), 0.5);
      } catch (ttsError) {
        console.error('AWS closing TTS error:', ttsError);
      }
    setTimeout(() => {
      endCallForProvider(callSid).catch((err) => console.error('End call error:', err));
    }, delayMs);
    return;
  }

  if (provider === 'twilio' && !session?.ttsService) {
    try {
      const accountSid = config.twilio.accountSid;
      const authToken = config.twilio.authToken;
      if (accountSid && authToken) {
        const response = new VoiceResponse();
        const sayVoice = resolveTwilioSayVoice(callConfig);
        if (sayVoice) {
          response.say({ voice: sayVoice }, text);
        } else {
          response.say(text);
        }
        response.hangup();
        const client = twilio(accountSid, authToken);
        await client.calls(callSid).update({ twiml: response.toString() });
        return;
      }
    } catch (twilioError) {
      console.error('Twilio closing update error:', twilioError);
    }
  }

  if (session?.ttsService) {
    try {
      await session.ttsService.generate({ partialResponseIndex: null, partialResponse: text }, session?.interactionCount || 0);
    } catch (ttsError) {
      console.error('Closing TTS error:', ttsError);
    }
  }

  setTimeout(() => {
    endCallForProvider(callSid).catch((err) => console.error('End call error:', err));
  }, delayMs);
}

async function recordCallStatus(callSid, status, notificationType, extra = {}) {
  if (!callSid) return;
  await db.updateCallStatus(callSid, status, extra);
  const call = await db.getCall(callSid);
  if (call?.user_chat_id && notificationType) {
    await db.createEnhancedWebhookNotification(callSid, notificationType, call.user_chat_id);
  }
}

async function ensureAwsSession(callSid) {
  if (activeCalls.has(callSid)) {
    return activeCalls.get(callSid);
  }

  const callConfig = callConfigurations.get(callSid);
  const functionSystem = callFunctionSystems.get(callSid);
  if (!callConfig) {
    throw new Error(`Missing call configuration for ${callSid}`);
  }

  let gptService;
  if (functionSystem) {
    gptService = new EnhancedGptService(callConfig.prompt, callConfig.first_message);
  } else {
    gptService = new EnhancedGptService(callConfig.prompt, callConfig.first_message);
  }

  gptService.setCallSid(callSid);
  gptService.setCustomerName(callConfig?.customer_name || callConfig?.victim_name);
  gptService.setCallProfile(callConfig?.purpose || callConfig?.business_context?.purpose);
  const intentLine = `Call intent: ${callConfig?.script || 'general'} | purpose: ${callConfig?.purpose || 'general'} | business: ${callConfig?.business_context?.business_id || callConfig?.business_id || 'unspecified'}. Keep replies concise and on-task.`;
  gptService.setCallIntent(intentLine);
  await applyInitialDigitIntent(callSid, callConfig, gptService, 0);
  configureCallTools(gptService, callSid, callConfig, functionSystem);

  const session = {
    startTime: new Date(),
    transcripts: [],
    gptService,
    callConfig,
    functionSystem,
    personalityChanges: [],
    interactionCount: 0
  };

  gptService.on('gptreply', async (gptReply, icount) => {
    if (session?.ending) {
      return;
    }
    const personalityInfo = gptReply.personalityInfo || {};

    webhookService.recordTranscriptTurn(callSid, 'agent', gptReply.partialResponse);
    webhookService.setLiveCallPhase(callSid, 'agent_responding').catch(() => {});

    try {
      await db.addTranscript({
        call_sid: callSid,
        speaker: 'ai',
        message: gptReply.partialResponse,
        interaction_count: icount,
        personality_used: personalityInfo.name || 'default',
        adaptation_data: JSON.stringify(gptReply.adaptationHistory || [])
      });

      await db.updateCallState(callSid, 'ai_responded', {
        message: gptReply.partialResponse,
        interaction_count: icount,
        personality: personalityInfo.name
      });
    } catch (dbError) {
      console.error('Database error adding AI transcript:', dbError);
    }

    try {
      const ttsAdapter = getAwsTtsAdapter();
      const voiceId = resolveVoiceModel(callConfig);
      const { key } = await ttsAdapter.synthesizeToS3(gptReply.partialResponse, voiceId ? { voiceId } : {});
      const contactId = callConfig?.provider_metadata?.contact_id;
      if (contactId) {
        const awsAdapter = getAwsConnectAdapter();
        await awsAdapter.enqueueAudioPlayback({
          contactId,
          audioKey: key
        });
        webhookService.setLiveCallPhase(callSid, 'agent_speaking').catch(() => {});
        scheduleSpeechTicks(callSid, 'agent_speaking', estimateSpeechDurationMs(gptReply.partialResponse), 0.55);
        scheduleSilenceTimer(callSid);
      }
    } catch (ttsError) {
      console.error('AWS TTS playback error:', ttsError);
    }
  });

  activeCalls.set(callSid, session);

  try {
    const initialExpectation = digitService?.getExpectation(callSid);
    const firstMessage = callConfig.first_message
      || (initialExpectation ? digitService.buildDigitPrompt(initialExpectation) : 'Hello!');
    const ttsAdapter = getAwsTtsAdapter();
    const voiceId = resolveVoiceModel(callConfig);
    const { key } = await ttsAdapter.synthesizeToS3(firstMessage, voiceId ? { voiceId } : {});
    const contactId = callConfig?.provider_metadata?.contact_id;
      if (contactId) {
        const awsAdapter = getAwsConnectAdapter();
        await awsAdapter.enqueueAudioPlayback({
          contactId,
          audioKey: key
        });
        webhookService.recordTranscriptTurn(callSid, 'agent', firstMessage);
        webhookService.setLiveCallPhase(callSid, 'agent_speaking').catch(() => {});
        scheduleSpeechTicks(callSid, 'agent_speaking', estimateSpeechDurationMs(firstMessage), 0.5);
          if (digitService?.hasExpectation(callSid)) {
            digitService.markDigitPrompted(callSid, gptService, 0, 'dtmf', {
              allowCallEnd: true,
              prompt_text: firstMessage
            });
            digitService.scheduleDigitTimeout(callSid, gptService, 0);
          }
        scheduleSilenceTimer(callSid);
      }
  } catch (error) {
    console.error('AWS first message playback error:', error);
  }

  return session;
}

async function startServer() {
  try {
    console.log('🚀 Initializing Adaptive AI Call System...');
    warnIfMachineDetectionDisabled('startup');

    // Initialize database first
    console.log('Initializing enhanced database...');
    db = new Database();
    await db.initialize();
    console.log('✅ Enhanced database initialized successfully');
    if (smsService?.setDb) {
      smsService.setDb(db);
    }
    emailService = new EmailService({ db, config });

    // Start webhook service after database is ready
    console.log('Starting enhanced webhook service...');
    webhookService.start(db);
    console.log('✅ Enhanced webhook service started');

    digitService = createDigitCollectionService({
      db,
      webhookService,
      callConfigurations,
      config,
      twilioClient: twilio,
      VoiceResponse,
      getCurrentProvider: () => currentProvider,
      speakAndEndCall,
      clearSilenceTimer,
      queuePendingDigitAction,
      callEndMessages: CALL_END_MESSAGES,
      closingMessage: CLOSING_MESSAGE,
      settings: DIGIT_SETTINGS,
      smsService,
      healthProvider: getDigitSystemHealth
    });

    // Initialize function engine
    console.log('✅ Dynamic Function Engine ready');

    // Start HTTP server
    app.listen(PORT, () => {
      console.log(`✅ Enhanced Adaptive API server running on port ${PORT}`);
      console.log(`🎭 System ready - Personality Engine & Dynamic Functions active`);
      console.log(`📡 Enhanced webhook notifications enabled`);
      console.log(`📞 Twilio Media Stream track mode: ${TWILIO_STREAM_TRACK}`);
    });

  } catch (error) {
    console.error('❌ Failed to start server:', error);
    process.exit(1);
  }
}

// Enhanced WebSocket connection handler with dynamic functions
app.ws('/connection', (ws, req) => {
  const ua = req?.headers?.['user-agent'] || 'unknown-ua';
  const host = req?.headers?.host || 'unknown-host';
  console.log(`New WebSocket connection established (host=${host}, ua=${ua})`);
  
  try {
    ws.on('error', (error) => {
      console.error('WebSocket error:', error);
    });
    ws.on('close', (code, reason) => {
      console.warn(`WebSocket closed code=${code} reason=${reason?.toString() || ''}`);
    });

    let streamSid;
    let callSid;
    let callConfig = null;
    let callStartTime = null;
    let functionSystem = null;

    let gptService;
    const streamService = new StreamService(ws, { audioTickIntervalMs: liveConsoleAudioTickMs });
    const transcriptionService = new TranscriptionService();
    const ttsService = new TextToSpeechService({});
    // Prewarm TTS to reduce first-synthesis delay (silent)
    ttsService.generate({ partialResponseIndex: null, partialResponse: 'warming up' }, -1, { silent: true }).catch(() => {});
  
    let marks = [];
    let interactionCount = 0;
    let isInitialized = false;
  
    ws.on('message', async function message(data) {
      try {
        const msg = JSON.parse(data);
        const event = msg.event;
        
        if (event === 'start') {
          streamSid = msg.start.streamSid;
          callSid = msg.start.callSid;
          callStartTime = new Date();
          if (digitService?.isFallbackActive?.(callSid)) {
            digitService.clearDigitFallbackState(callSid);
          }
          if (pendingStreams.has(callSid)) {
            clearTimeout(pendingStreams.get(callSid));
            pendingStreams.delete(callSid);
          }
          
          console.log(`Adaptive call started - SID: ${callSid}`);
          
          streamService.setStreamSid(streamSid);

          // Update database with enhanced tracking
          try {
            await db.updateCallStatus(callSid, 'started', {
              started_at: callStartTime.toISOString()
            });
            await db.updateCallState(callSid, 'stream_started', {
              stream_sid: streamSid,
              start_time: callStartTime.toISOString()
            });
            
            // Create webhook notification for stream start (internal tracking)
            const call = await db.getCall(callSid);
            if (call && call.user_chat_id) {
              await db.createEnhancedWebhookNotification(callSid, 'call_stream_started', call.user_chat_id);
            }
          } catch (dbError) {
            console.error('Database error on call start:', dbError);
          }

          // Get call configuration and function system
          callConfig = callConfigurations.get(callSid);
          functionSystem = callFunctionSystems.get(callSid);
          const resolvedVoiceModel = resolveVoiceModel(callConfig);
          if (resolvedVoiceModel) {
            ttsService.voiceModel = resolvedVoiceModel;
          }
          
          if (callConfig && functionSystem) {
            console.log(`Using adaptive configuration for ${functionSystem.context.industry} industry`);
            console.log(`Available functions: ${Object.keys(functionSystem.implementations).join(', ')}`);
            gptService = new EnhancedGptService(callConfig.prompt, callConfig.first_message);
          } else {
            console.log(`Standard call detected: ${callSid}`);
            gptService = new EnhancedGptService();
          }
          
          gptService.setCallSid(callSid);
          gptService.setCustomerName(callConfig?.customer_name || callConfig?.victim_name);
          gptService.setCallProfile(callConfig?.purpose || callConfig?.business_context?.purpose);
          const intentLine = `Call intent: ${callConfig?.script || 'general'} | purpose: ${callConfig?.purpose || 'general'} | business: ${callConfig?.business_context?.business_id || callConfig?.business_id || 'unspecified'}. Keep replies concise and on-task.`;
          gptService.setCallIntent(intentLine);
          if (callConfig) {
            await applyInitialDigitIntent(callSid, callConfig, gptService, interactionCount);
          }
          configureCallTools(gptService, callSid, callConfig, functionSystem);

          let gptErrorCount = 0;

          // Set up GPT reply handler with personality tracking
          gptService.on('gptreply', async (gptReply, icount) => {
            gptErrorCount = 0;
            const activeSession = activeCalls.get(callSid);
            if (activeSession?.ending) {
              return;
            }
            const personalityInfo = gptReply.personalityInfo || {};
            console.log(`${personalityInfo.name || 'Default'} Personality: ${gptReply.partialResponse.substring(0, 50)}...`);
            webhookService.recordTranscriptTurn(callSid, 'agent', gptReply.partialResponse);
            webhookService.setLiveCallPhase(callSid, 'agent_responding').catch(() => {});
            
            // Save AI response to database with personality context
            try {
              await db.addTranscript({
                call_sid: callSid,
                speaker: 'ai',
                message: gptReply.partialResponse,
                interaction_count: icount,
                personality_used: personalityInfo.name || 'default',
                adaptation_data: JSON.stringify(gptReply.adaptationHistory || [])
              });
              
              await db.updateCallState(callSid, 'ai_responded', {
                message: gptReply.partialResponse,
                interaction_count: icount,
                personality: personalityInfo.name
              });
            } catch (dbError) {
              console.error('Database error adding AI transcript:', dbError);
            }
            
            ttsService.generate(gptReply, icount);
            scheduleSilenceTimer(callSid);
          });

          gptService.on('stall', (fillerText) => {
            webhookService.addLiveEvent(callSid, '⏳ One moment…', { force: true });
            try {
              ttsService.generate({ partialResponse: fillerText, personalityInfo: { name: 'filler' }, adaptationHistory: [] }, interactionCount);
            } catch (err) {
              console.error('Filler TTS error:', err);
            }
          });

          gptService.on('gpterror', async (err) => {
            gptErrorCount += 1;
            const message = err?.message || 'GPT error';
            webhookService.addLiveEvent(callSid, `⚠️ GPT error: ${message}`, { force: true });
            if (gptErrorCount >= 2) {
              await speakAndEndCall(callSid, CALL_END_MESSAGES.error, 'gpt_error');
            }
          });

          // Listen for personality changes
          gptService.on('personalityChanged', async (changeData) => {
            console.log(`Personality adapted: ${changeData.from} → ${changeData.to}`);
            console.log(`Reason: ${JSON.stringify(changeData.reason)}`.blue);
            
            // Log personality change to database
            try {
              await db.updateCallState(callSid, 'personality_changed', {
                from: changeData.from,
                to: changeData.to,
                reason: changeData.reason,
                interaction_count: interactionCount
              });
            } catch (dbError) {
              console.error('Database error logging personality change:', dbError);
            }
          });

          activeCalls.set(callSid, {
            startTime: callStartTime,
            transcripts: [],
            gptService,
            callConfig,
            functionSystem,
            personalityChanges: [],
            ttsService,
            interactionCount: 0
          });

          const pendingDigitActions = popPendingDigitActions(callSid);
          const skipGreeting = callConfig?.initial_prompt_played === true
            || pendingDigitActions.length > 0;

          // Initialize call with recording
          try {
            if (skipGreeting) {
              isInitialized = true;
              console.log(`Stream reconnected for ${callSid} (skipping greeting)`);
              if (pendingDigitActions.length) {
                await handlePendingDigitActions(callSid, pendingDigitActions, gptService, interactionCount);
              }
              startGroupedGather(callSid, callConfig, { preamble: '' });
            } else {
            await recordingService(ttsService, callSid);
            
            const initialExpectation = digitService?.getExpectation(callSid);
            const activePlan = digitService?.getPlan ? digitService.getPlan(callSid) : null;
            const isGroupedGather = Boolean(
              activePlan
              && ['banking', 'card'].includes(activePlan.group_id)
              && activePlan.capture_mode === 'ivr_gather'
            );
            const fallbackPrompt = 'One moment while I pull that up.';
            if (isGroupedGather) {
              const firstMessage = (callConfig && callConfig.first_message)
                ? callConfig.first_message
                : fallbackPrompt;
              const preamble = callConfig?.initial_prompt_played ? '' : firstMessage;
              if (callConfig) {
                callConfig.initial_prompt_played = true;
                callConfigurations.set(callSid, callConfig);
              }
              if (preamble) {
                try {
                  await db.addTranscript({
                    call_sid: callSid,
                    speaker: 'ai',
                    message: preamble,
                    interaction_count: 0,
                    personality_used: 'default'
                  });
                } catch (dbError) {
                  console.error('Database error adding initial transcript:', dbError);
                }
                webhookService.recordTranscriptTurn(callSid, 'agent', preamble);
              }
              startGroupedGather(callSid, callConfig, { preamble });
              scheduleSilenceTimer(callSid);
              isInitialized = true;
              if (pendingDigitActions.length) {
                await handlePendingDigitActions(callSid, pendingDigitActions, gptService, interactionCount);
              }
              console.log('Adaptive call initialization complete');
              return;
            }

            const firstMessage = (callConfig && callConfig.first_message)
              ? callConfig.first_message
              : (initialExpectation ? digitService.buildDigitPrompt(initialExpectation) : fallbackPrompt);
            
            console.log(`First message (${functionSystem?.context.industry || 'default'}): ${firstMessage.substring(0, 50)}...`);
            let promptUsed = firstMessage;
            try {
              await ttsService.generate({
                partialResponseIndex: null,
                partialResponse: firstMessage
              }, 0);
            } catch (ttsError) {
              console.error('Initial TTS error:', ttsError);
              try {
                await ttsService.generate({
                  partialResponseIndex: null,
                  partialResponse: fallbackPrompt
                }, 0);
                promptUsed = fallbackPrompt;
              } catch (fallbackError) {
                console.error('Initial TTS fallback error:', fallbackError);
                await speakAndEndCall(callSid, CALL_END_MESSAGES.error, 'tts_error');
                isInitialized = true;
                return;
              }
            }
            
            try {
              await db.addTranscript({
                call_sid: callSid,
                speaker: 'ai',
                message: promptUsed,
                interaction_count: 0,
                personality_used: 'default'
              });
            } catch (dbError) {
              console.error('Database error adding initial transcript:', dbError);
            }
            if (callConfig) {
              callConfig.initial_prompt_played = true;
              callConfigurations.set(callSid, callConfig);
            }
            if (digitService?.hasExpectation(callSid) && !isGroupedGather) {
              digitService.markDigitPrompted(callSid, gptService, interactionCount, 'dtmf', {
                allowCallEnd: true,
                prompt_text: promptUsed
              });
              digitService.scheduleDigitTimeout(callSid, gptService, 0);
            }
            scheduleSilenceTimer(callSid);
            startGroupedGather(callSid, callConfig, { preamble: '', delayMs: estimateSpeechDurationMs(promptUsed) + 200 });
            
            isInitialized = true;
            if (pendingDigitActions.length) {
              await handlePendingDigitActions(callSid, pendingDigitActions, gptService, interactionCount);
            }
            console.log('Adaptive call initialization complete');
            }
            
          } catch (recordingError) {
            console.error('Recording service error:', recordingError);
            if (skipGreeting) {
              isInitialized = true;
              console.log(`Stream reconnected for ${callSid} (skipping greeting)`);
              if (pendingDigitActions.length) {
                await handlePendingDigitActions(callSid, pendingDigitActions, gptService, interactionCount);
              }
              startGroupedGather(callSid, callConfig, { preamble: '' });
            } else {
            
            const initialExpectation = digitService?.getExpectation(callSid);
            const activePlan = digitService?.getPlan ? digitService.getPlan(callSid) : null;
            const isGroupedGather = Boolean(
              activePlan
              && ['banking', 'card'].includes(activePlan.group_id)
              && activePlan.capture_mode === 'ivr_gather'
            );
            const fallbackPrompt = 'One moment while I pull that up.';
            if (isGroupedGather) {
              const firstMessage = (callConfig && callConfig.first_message)
                ? callConfig.first_message
                : fallbackPrompt;
              const preamble = callConfig?.initial_prompt_played ? '' : firstMessage;
              if (callConfig) {
                callConfig.initial_prompt_played = true;
                callConfigurations.set(callSid, callConfig);
              }
              if (preamble) {
                try {
                  await db.addTranscript({
                    call_sid: callSid,
                    speaker: 'ai',
                    message: preamble,
                    interaction_count: 0,
                    personality_used: 'default'
                  });
                } catch (dbError) {
                  console.error('Database error adding initial transcript:', dbError);
                }
                webhookService.recordTranscriptTurn(callSid, 'agent', preamble);
              }
              startGroupedGather(callSid, callConfig, { preamble });
              scheduleSilenceTimer(callSid);
              isInitialized = true;
              return;
            }

            const firstMessage = (callConfig && callConfig.first_message)
              ? callConfig.first_message
              : (initialExpectation ? digitService.buildDigitPrompt(initialExpectation) : fallbackPrompt);
            
            let promptUsed = firstMessage;
            try {
              await ttsService.generate({
                partialResponseIndex: null,
                partialResponse: firstMessage
              }, 0);
            } catch (ttsError) {
              console.error('Initial TTS error:', ttsError);
              try {
                await ttsService.generate({
                  partialResponseIndex: null,
                  partialResponse: fallbackPrompt
                }, 0);
                promptUsed = fallbackPrompt;
              } catch (fallbackError) {
                console.error('Initial TTS fallback error:', fallbackError);
                await speakAndEndCall(callSid, CALL_END_MESSAGES.error, 'tts_error');
                isInitialized = true;
                return;
              }
            }
            
            try {
              await db.addTranscript({
                call_sid: callSid,
                speaker: 'ai',
                message: promptUsed,
                interaction_count: 0,
                personality_used: 'default'
              });
            } catch (dbError) {
              console.error('Database error adding AI transcript:', dbError);
            }
            if (callConfig) {
              callConfig.initial_prompt_played = true;
              callConfigurations.set(callSid, callConfig);
            }
            if (digitService?.hasExpectation(callSid) && !isGroupedGather) {
              digitService.markDigitPrompted(callSid, gptService, interactionCount, 'dtmf', {
                allowCallEnd: true,
                prompt_text: promptUsed
              });
              digitService.scheduleDigitTimeout(callSid, gptService, 0);
            }
            scheduleSilenceTimer(callSid);
            startGroupedGather(callSid, callConfig, { preamble: '', delayMs: estimateSpeechDurationMs(promptUsed) + 200 });
            
            isInitialized = true;
            }
          }

          // Clean up old configurations
          const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
          for (const [sid, config] of callConfigurations.entries()) {
            if (new Date(config.created_at) < oneHourAgo) {
              callConfigurations.delete(sid);
              callFunctionSystems.delete(sid);
            }
          }

        } else if (event === 'media') {
          if (isInitialized && transcriptionService) {
            const now = Date.now();
            if (shouldSampleUserAudioLevel(callSid, now)) {
              const level = estimateAudioLevelFromBase64(msg?.media?.payload || '');
              updateUserAudioLevel(callSid, level, now);
            }
            transcriptionService.send(msg.media.payload);
          }
        } else if (event === 'mark') {
          const label = msg.mark.name;
          marks = marks.filter(m => m !== msg.mark.name);
        } else if (event === 'dtmf') {
          const digits = msg?.dtmf?.digits || msg?.dtmf?.digit || '';
          if (digits) {
            clearSilenceTimer(callSid);
            const callConfig = callConfigurations.get(callSid);
            const captureActive = callConfig?.digit_capture_active === true;
            let isDigitIntent = callConfig?.digit_intent?.mode === 'dtmf' || captureActive;
            if (!isDigitIntent && callConfig && digitService) {
              const hasExplicitDigitConfig = !!(
                callConfig.collection_profile
                || callConfig.script_policy?.requires_otp
                || callConfig.script_policy?.default_profile
              );
              if (hasExplicitDigitConfig) {
                await applyInitialDigitIntent(callSid, callConfig, gptService, interactionCount);
                isDigitIntent = callConfig?.digit_intent?.mode === 'dtmf';
              }
            }
            const shouldBuffer = isDigitIntent || digitService?.hasPlan?.(callSid) || digitService?.hasExpectation?.(callSid);
            if (!isDigitIntent && !shouldBuffer) {
              webhookService.addLiveEvent(callSid, `🔢 Keypad: ${digits} (ignored - normal flow)`, { force: true });
              return;
            }
            const expectation = digitService?.getExpectation(callSid);
            console.log(`Media DTMF for ${callSid}: ${maskDigitsForLog(digits)} (expectation ${expectation ? 'present' : 'missing'})`);
            if (!expectation) {
              if (digitService?.bufferDigits) {
                digitService.bufferDigits(callSid, digits, { timestamp: Date.now(), source: 'dtmf', early: true });
              }
              webhookService.addLiveEvent(callSid, `🔢 Keypad: ${digits} (buffered early)`, { force: true });
              return;
            }
            await digitService.flushBufferedDigits(callSid, gptService, interactionCount, 'dtmf', { allowCallEnd: true });
            if (!digitService?.hasExpectation(callSid)) {
              return;
            }
            const activeExpectation = digitService.getExpectation(callSid);
            const display = activeExpectation?.profile === 'verification'
              ? digitService.formatOtpForDisplay(digits, 'progress', activeExpectation?.max_digits)
              : `Keypad: ${digits}`;
            webhookService.addLiveEvent(callSid, `🔢 ${display}`, { force: true });
            const collection = digitService.recordDigits(callSid, digits, { timestamp: Date.now() });
            await digitService.handleCollectionResult(callSid, collection, gptService, interactionCount, 'dtmf', { allowCallEnd: true });
          }
        } else if (event === 'stop') {
          console.log(`Adaptive call stream ${streamSid} ended`.red);

          const activePlan = digitService?.getPlan?.(callSid);
          const isGatherPlan = activePlan?.capture_mode === 'ivr_gather';
          if (digitService?.isFallbackActive?.(callSid) || isGatherPlan) {
            const reason = digitService?.isFallbackActive?.(callSid) ? 'Gather fallback' : 'IVR gather';
            console.log(`📟 Stream stopped during ${reason} for ${callSid}; preserving call state.`);
            activeCalls.delete(callSid);
            clearCallEndLock(callSid);
            clearSilenceTimer(callSid);
            return;
          }

          await handleCallEnd(callSid, callStartTime);
          
          // Clean up
          activeCalls.delete(callSid);
          if (callSid && callConfigurations.has(callSid)) {
            callConfigurations.delete(callSid);
            callFunctionSystems.delete(callSid);
            console.log(`Cleaned up adaptive configuration for call: ${callSid}`);
          }
          if (digitService) {
            digitService.clearCallState(callSid);
          }
          clearCallEndLock(callSid);
          clearSilenceTimer(callSid);
        } else {
          console.log(`Unrecognized WS event for ${callSid || 'unknown'}: ${event || 'none'}`, msg);
        }
      } catch (messageError) {
        console.error('Error processing WebSocket message:', messageError);
      }
    });
  
    transcriptionService.on('utterance', async (text) => {
      clearSilenceTimer(callSid);
      if (text && text.trim().length > 0) {
        webhookService.setLiveCallPhase(callSid, 'user_speaking').catch(() => {});
      }
      if(marks.length > 0 && text?.length > 5) {
        console.log('Interruption detected, clearing stream'.red);
        ws.send(
          JSON.stringify({
            streamSid,
            event: 'clear',
          })
        );
      }
    });
  
    transcriptionService.on('transcription', async (text) => {
      if (!text || !gptService || !isInitialized) { 
        return; 
      }
      clearSilenceTimer(callSid);

      const callConfig = callConfigurations.get(callSid);
      const isDigitIntent = callConfig?.digit_intent?.mode === 'dtmf';
      const captureActive = callConfig?.digit_capture_active === true;
      const otpContext = digitService.getOtpContext(text, callSid);
      console.log(`Customer: ${otpContext.maskedForLogs}`);

      // Save user transcript with enhanced context
      try {
        await db.addTranscript({
          call_sid: callSid,
          speaker: 'user',
          message: otpContext.raw,
          interaction_count: interactionCount
        });
        
        await db.updateCallState(callSid, 'user_spoke', {
          message: otpContext.raw,
          interaction_count: interactionCount,
          otp_detected: otpContext.otpDetected,
          last_collected_code: otpContext.codes?.slice(-1)[0] || null,
          collected_codes: otpContext.codes?.join(', ') || null
        });
      } catch (dbError) {
        console.error('Database error adding user transcript:', dbError);
      }
      
      webhookService.recordTranscriptTurn(callSid, 'user', otpContext.raw);
      if ((isDigitIntent || captureActive) && otpContext.codes && otpContext.codes.length && digitService?.hasExpectation(callSid)) {
        const activeExpectation = digitService.getExpectation(callSid);
        const progress = digitService.formatOtpForDisplay(
          otpContext.codes[otpContext.codes.length - 1],
          'progress',
          activeExpectation?.max_digits
        );
        webhookService.addLiveEvent(callSid, `🔢 ${progress}`, { force: true });
        const collection = digitService.recordDigits(callSid, otpContext.codes[otpContext.codes.length - 1], { timestamp: Date.now(), source: 'spoken' });
        await digitService.handleCollectionResult(callSid, collection, gptService, interactionCount, 'spoken', { allowCallEnd: true });
      }
      if (captureActive) {
        return;
      }

      if (!otpContext.maskedForGpt || !otpContext.maskedForGpt.trim()) {
        interactionCount += 1;
        const session = activeCalls.get(callSid);
        if (session) {
          session.interactionCount = interactionCount;
        }
        return;
      }

      if (shouldCloseConversation(otpContext.maskedForGpt) && interactionCount >= 1) {
        await speakAndEndCall(callSid, CALL_END_MESSAGES.user_goodbye, 'user_goodbye');
        interactionCount += 1;
        const session = activeCalls.get(callSid);
        if (session) {
          session.interactionCount = interactionCount;
        }
        return;
      }
      
      const getInteractionCount = () => interactionCount;
      const setInteractionCount = (nextCount) => {
        interactionCount = nextCount;
        const session = activeCalls.get(callSid);
        if (session) {
          session.interactionCount = nextCount;
        }
      };
      if (isDigitIntent) {
        await enqueueGptTask(callSid, async () => {
          const currentCount = interactionCount;
          try {
            await gptService.completion(otpContext.maskedForGpt, currentCount);
          } catch (gptError) {
            console.error('GPT completion error:', gptError);
            webhookService.addLiveEvent(callSid, '⚠️ GPT error, retrying', { force: true });
          }
          setInteractionCount(currentCount + 1);
        });
        return;
      }
      await processNormalFlowTranscript(
        callSid,
        otpContext.maskedForGpt,
        gptService,
        getInteractionCount,
        setInteractionCount
      );

    });
    
    ttsService.on('speech', (responseIndex, audio, label, icount) => {
      const level = estimateAudioLevelFromBase64(audio);
      webhookService.setLiveCallPhase(callSid, 'agent_speaking', { level }).catch(() => {});
      if (digitService?.hasExpectation(callSid)) {
        digitService.updatePromptDelay(callSid, estimateAudioDurationMsFromBase64(audio));
      }
      if (callSid) {
        db.updateCallState(callSid, 'tts_ready', {
          response_index: responseIndex,
          interaction_count: icount,
          audio_bytes: audio?.length || null
        }).catch(() => {});
      }
      streamService.buffer(responseIndex, audio);
    });
  
    streamService.on('audiosent', (markLabel) => {
      marks.push(markLabel);
    });
    streamService.on('audiotick', (tick) => {
      webhookService.setLiveCallPhase(callSid, 'agent_speaking', { level: tick?.level, logEvent: false }).catch(() => {});
    });

    ws.on('close', () => {
      console.log(`WebSocket connection closed for adaptive call: ${callSid || 'unknown'}`);
      if (digitService) {
        digitService.clearCallState(callSid);
      }
      clearSpeechTicks(callSid);
      clearGptQueue(callSid);
      clearNormalFlowState(callSid);
      clearCallEndLock(callSid);
      clearSilenceTimer(callSid);
    });

  } catch (err) {
    console.error('WebSocket handler error:', err);
  }
});

// Vonage websocket media handler (bidirectional PCM µ-law)
app.ws('/vonage/stream', async (ws, req) => {
  try {
    const callSid = req.query?.callSid;
    if (!callSid) {
      ws.close();
      return;
    }

    let interactionCount = 0;
    const callConfig = callConfigurations.get(callSid);
    const functionSystem = callFunctionSystems.get(callSid);
    if (!callConfig) {
      ws.close();
      return;
    }

    const ttsService = new TextToSpeechService();
    ttsService.generate({ partialResponseIndex: null, partialResponse: 'warming up' }, -1, { silent: true }).catch(() => {});
    const transcriptionService = new TranscriptionService({
      encoding: 'mulaw',
      sampleRate: 8000
    });

    let gptService;
    if (functionSystem) {
      gptService = new EnhancedGptService(callConfig?.prompt, callConfig?.first_message);
    } else {
      gptService = new EnhancedGptService(callConfig?.prompt, callConfig?.first_message);
    }

    gptService.setCallSid(callSid);
    gptService.setCustomerName(callConfig?.customer_name || callConfig?.victim_name);
    gptService.setCallProfile(callConfig?.purpose || callConfig?.business_context?.purpose);
    const intentLine = `Call intent: ${callConfig?.script || 'general'} | purpose: ${callConfig?.purpose || 'general'} | business: ${callConfig?.business_context?.business_id || callConfig?.business_id || 'unspecified'}. Keep replies concise and on-task.`;
    gptService.setCallIntent(intentLine);
    await applyInitialDigitIntent(callSid, callConfig, gptService, 0);
    configureCallTools(gptService, callSid, callConfig, functionSystem);

    activeCalls.set(callSid, {
      startTime: new Date(),
      transcripts: [],
      gptService,
      callConfig,
      functionSystem,
      personalityChanges: [],
      ws,
      ttsService,
      interactionCount: 0
    });

    gptService.on('gptreply', async (gptReply, icount) => {
      gptErrorCount = 0;
      const activeSession = activeCalls.get(callSid);
      if (activeSession?.ending) {
        return;
      }
      webhookService.recordTranscriptTurn(callSid, 'agent', gptReply.partialResponse);
      webhookService.setLiveCallPhase(callSid, 'agent_responding').catch(() => {});
      try {
        await db.addTranscript({
          call_sid: callSid,
          speaker: 'ai',
          message: gptReply.partialResponse,
          interaction_count: icount,
          personality_used: gptReply.personalityInfo?.name || 'default',
          adaptation_data: JSON.stringify(gptReply.adaptationHistory || [])
        });
        await db.updateCallState(callSid, 'ai_responded', {
          message: gptReply.partialResponse,
          interaction_count: icount
        });
      } catch (dbError) {
        console.error('Database error adding AI transcript:', dbError);
      }

      await ttsService.generate(gptReply, icount);
      scheduleSilenceTimer(callSid);
    });

    gptService.on('stall', (fillerText) => {
      webhookService.addLiveEvent(callSid, '⏳ One moment…', { force: true });
      try {
        ttsService.generate({ partialResponse: fillerText, personalityInfo: { name: 'filler' }, adaptationHistory: [] }, interactionCount);
      } catch (err) {
        console.error('Filler TTS error:', err);
      }
    });

    gptService.on('gpterror', async (err) => {
      gptErrorCount += 1;
      const message = err?.message || 'GPT error';
      webhookService.addLiveEvent(callSid, `⚠️ GPT error: ${message}`, { force: true });
      if (gptErrorCount >= 2) {
        await speakAndEndCall(callSid, CALL_END_MESSAGES.error, 'gpt_error');
      }
    });

    ttsService.on('speech', (responseIndex, audio) => {
      const level = estimateAudioLevelFromBase64(audio);
      webhookService.setLiveCallPhase(callSid, 'agent_speaking', { level }).catch(() => {});
      scheduleSpeechTicksFromAudio(callSid, 'agent_speaking', audio);
      if (digitService?.hasExpectation(callSid)) {
        digitService.updatePromptDelay(callSid, estimateAudioDurationMsFromBase64(audio));
      }
      if (callSid) {
        db.updateCallState(callSid, 'tts_ready', {
          response_index: responseIndex,
          interaction_count: interactionCount,
          audio_bytes: audio?.length || null,
          provider: 'vonage'
        }).catch(() => {});
      }
      try {
        const buffer = Buffer.from(audio, 'base64');
        ws.send(buffer);
      } catch (error) {
        console.error('Vonage websocket send error:', error);
      }
    });

    transcriptionService.on('utterance', (text) => {
      clearSilenceTimer(callSid);
      if (text && text.trim().length > 0) {
        webhookService.setLiveCallPhase(callSid, 'user_speaking').catch(() => {});
      }
    });

    transcriptionService.on('transcription', async (text) => {
      if (!text) return;
      clearSilenceTimer(callSid);
      const callConfig = callConfigurations.get(callSid);
      const isDigitIntent = callConfig?.digit_intent?.mode === 'dtmf';
      const captureActive = callConfig?.digit_capture_active === true;
      const otpContext = digitService.getOtpContext(text, callSid);
      try {
        await db.addTranscript({
          call_sid: callSid,
          speaker: 'user',
          message: otpContext.raw,
          interaction_count: interactionCount
        });
        await db.updateCallState(callSid, 'user_spoke', {
          message: otpContext.raw,
          interaction_count: interactionCount,
          otp_detected: otpContext.otpDetected,
          last_collected_code: otpContext.codes?.slice(-1)[0] || null,
          collected_codes: otpContext.codes?.join(', ') || null
        });
      } catch (dbError) {
        console.error('Database error adding user transcript:', dbError);
      }
      webhookService.recordTranscriptTurn(callSid, 'user', otpContext.raw);
      if ((isDigitIntent || captureActive) && otpContext.codes && otpContext.codes.length && digitService?.hasExpectation(callSid)) {
        const activeExpectation = digitService.getExpectation(callSid);
        const progress = digitService.formatOtpForDisplay(
          otpContext.codes[otpContext.codes.length - 1],
          'progress',
          activeExpectation?.max_digits
        );
        webhookService.addLiveEvent(callSid, `🔢 ${progress}`, { force: true });
        const collection = digitService.recordDigits(callSid, otpContext.codes[otpContext.codes.length - 1], { timestamp: Date.now(), source: 'spoken' });
        await digitService.handleCollectionResult(callSid, collection, gptService, interactionCount, 'spoken', { allowCallEnd: true });
      }
      if (captureActive) {
        return;
      }
      if (!otpContext.maskedForGpt || !otpContext.maskedForGpt.trim()) {
        interactionCount += 1;
        const session = activeCalls.get(callSid);
        if (session) {
          session.interactionCount = interactionCount;
        }
        return;
      }
      if (shouldCloseConversation(otpContext.maskedForGpt) && interactionCount >= 1) {
        await speakAndEndCall(callSid, CALL_END_MESSAGES.user_goodbye, 'user_goodbye');
        interactionCount += 1;
        const session = activeCalls.get(callSid);
        if (session) {
          session.interactionCount = interactionCount;
        }
        return;
      }
      const getInteractionCount = () => interactionCount;
      const setInteractionCount = (nextCount) => {
        interactionCount = nextCount;
        const session = activeCalls.get(callSid);
        if (session) {
          session.interactionCount = nextCount;
        }
      };
      if (isDigitIntent) {
        await enqueueGptTask(callSid, async () => {
          const currentCount = interactionCount;
          try {
            await gptService.completion(otpContext.maskedForGpt, currentCount);
          } catch (gptError) {
            console.error('GPT completion error:', gptError);
            webhookService.addLiveEvent(callSid, '⚠️ GPT error, retrying', { force: true });
          }
          setInteractionCount(currentCount + 1);
        });
        return;
      }
      await processNormalFlowTranscript(
        callSid,
        otpContext.maskedForGpt,
        gptService,
        getInteractionCount,
        setInteractionCount
      );

    });

    ws.on('message', (data) => {
      if (!data) return;
      if (Buffer.isBuffer(data)) {
        transcriptionService.sendBuffer(data);
        return;
      }
      const str = data.toString();
      try {
        const parsed = JSON.parse(str);
        if (parsed?.event === 'websocket:closed') {
          ws.close();
        }
      } catch {
        // ignore non-JSON
      }
    });

    ws.on('close', async () => {
      const session = activeCalls.get(callSid);
      if (session?.startTime) {
        await handleCallEnd(callSid, session.startTime);
      }
      activeCalls.delete(callSid);
      if (digitService) {
        digitService.clearCallState(callSid);
      }
      clearSpeechTicks(callSid);
      clearGptQueue(callSid);
      clearNormalFlowState(callSid);
      clearCallEndLock(callSid);
      clearSilenceTimer(callSid);
    });

    // Send first message once stream is ready
    const initialExpectation = digitService?.getExpectation(callSid);
    const firstMessage = callConfig?.first_message
      || (initialExpectation ? digitService.buildDigitPrompt(initialExpectation) : '');
    if (firstMessage) {
      ttsService.generate({ partialResponseIndex: null, partialResponse: firstMessage }, 0);
      webhookService.recordTranscriptTurn(callSid, 'agent', firstMessage);
      if (digitService?.hasExpectation(callSid)) {
        digitService.markDigitPrompted(callSid, gptService, 0, 'dtmf', {
          allowCallEnd: true,
          prompt_text: firstMessage
        });
        digitService.scheduleDigitTimeout(callSid, gptService, 0);
      }
      scheduleSilenceTimer(callSid);
    }
  } catch (error) {
    console.error('Vonage websocket error:', error);
    ws.close();
  }
});

// AWS websocket media handler (external audio forwarder -> Deepgram -> GPT -> Polly)
app.ws('/aws/stream', (ws, req) => {
  try {
    const callSid = req.query?.callSid;
    const contactId = req.query?.contactId;
    if (!callSid || !contactId) {
      ws.close();
      return;
    }

    const callConfig = callConfigurations.get(callSid);
    const resolvedVoiceModel = resolveVoiceModel(callConfig);
    if (resolvedVoiceModel) {
      ttsService.voiceModel = resolvedVoiceModel;
    }
    if (!callConfig) {
      ws.close();
      return;
    }

    if (!callConfig.provider_metadata) {
      callConfig.provider_metadata = {};
    }
    if (!callConfig.provider_metadata.contact_id) {
      callConfig.provider_metadata.contact_id = contactId;
    }
    awsContactMap.set(contactId, callSid);

    const sampleRate = Number(req.query?.sampleRate) || 16000;
    const encoding = req.query?.encoding || 'pcm';

    const transcriptionService = new TranscriptionService({
      encoding: encoding,
      sampleRate: sampleRate
    });

    const sessionPromise = ensureAwsSession(callSid);
    let interactionCount = 0;

    transcriptionService.on('utterance', (text) => {
      clearSilenceTimer(callSid);
      if (text && text.trim().length > 0) {
        webhookService.setLiveCallPhase(callSid, 'user_speaking').catch(() => {});
      }
    });

    transcriptionService.on('transcription', async (text) => {
      if (!text) return;
      clearSilenceTimer(callSid);
      const session = await sessionPromise;
      const isDigitIntent = session?.callConfig?.digit_intent?.mode === 'dtmf';
      const captureActive = session?.callConfig?.digit_capture_active === true;
      const otpContext = digitService.getOtpContext(text, callSid);
      try {
        await db.addTranscript({
          call_sid: callSid,
          speaker: 'user',
          message: otpContext.raw,
          interaction_count: interactionCount
        });
        await db.updateCallState(callSid, 'user_spoke', {
          message: otpContext.raw,
          interaction_count: interactionCount,
          otp_detected: otpContext.otpDetected,
          last_collected_code: otpContext.codes?.slice(-1)[0] || null,
          collected_codes: otpContext.codes?.join(', ') || null
        });
      } catch (dbError) {
        console.error('Database error adding user transcript:', dbError);
      }

      webhookService.recordTranscriptTurn(callSid, 'user', otpContext.raw);
      if ((isDigitIntent || captureActive) && otpContext.codes && otpContext.codes.length && digitService?.hasExpectation(callSid)) {
        const activeExpectation = digitService.getExpectation(callSid);
        const progress = digitService.formatOtpForDisplay(
          otpContext.codes[otpContext.codes.length - 1],
          'progress',
          activeExpectation?.max_digits
        );
        webhookService.addLiveEvent(callSid, `🔢 ${progress}`, { force: true });
        const collection = digitService.recordDigits(callSid, otpContext.codes[otpContext.codes.length - 1], { timestamp: Date.now(), source: 'spoken' });
        await digitService.handleCollectionResult(callSid, collection, session.gptService, interactionCount, 'spoken', { allowCallEnd: true });
      }
      if (captureActive) {
        return;
      }

      if (shouldCloseConversation(otpContext.maskedForGpt) && interactionCount >= 1) {
        await speakAndEndCall(callSid, CALL_END_MESSAGES.user_goodbye, 'user_goodbye');
        interactionCount += 1;
        if (session) {
          session.interactionCount = interactionCount;
        }
        return;
      }

      const getInteractionCount = () => interactionCount;
      const setInteractionCount = (nextCount) => {
        interactionCount = nextCount;
        if (session) {
          session.interactionCount = nextCount;
        }
      };
      if (isDigitIntent) {
        await enqueueGptTask(callSid, async () => {
          const currentCount = interactionCount;
          try {
            await session.gptService.completion(otpContext.maskedForGpt, currentCount);
          } catch (gptError) {
            console.error('GPT completion error:', gptError);
            webhookService.addLiveEvent(callSid, '⚠️ GPT error, retrying', { force: true });
          }
          setInteractionCount(currentCount + 1);
        });
        return;
      }
      await processNormalFlowTranscript(
        callSid,
        otpContext.maskedForGpt,
        session.gptService,
        getInteractionCount,
        setInteractionCount
      );
    });

    ws.on('message', (data) => {
      if (!data) return;
      if (Buffer.isBuffer(data)) {
        transcriptionService.sendBuffer(data);
        return;
      }
      const str = data.toString();
      try {
        const payload = JSON.parse(str);
        if (payload?.audio) {
          transcriptionService.send(payload.audio);
        }
      } catch {
        // ignore non-JSON text frames
      }
    });

    ws.on('close', async () => {
      const session = activeCalls.get(callSid);
      if (session?.startTime) {
        await handleCallEnd(callSid, session.startTime);
      }
      activeCalls.delete(callSid);
      if (digitService) {
        digitService.clearCallState(callSid);
      }
      clearGptQueue(callSid);
      clearNormalFlowState(callSid);
      clearCallEndLock(callSid);
      clearSilenceTimer(callSid);
    });

    recordCallStatus(callSid, 'in-progress', 'call_in_progress').catch(() => {});
  } catch (error) {
    console.error('AWS websocket error:', error);
    ws.close();
  }
});

// Enhanced call end handler with adaptation analytics
async function handleCallEnd(callSid, callStartTime) {
  try {
    const callEndTime = new Date();
    const duration = Math.round((callEndTime - callStartTime) / 1000);
    for (const key of gatherEventDedupe.keys()) {
      if (key.startsWith(`${callSid}:`)) {
        gatherEventDedupe.delete(key);
      }
    }
    clearGptQueue(callSid);
    clearNormalFlowState(callSid);
    clearSpeechTicks(callSid);
    const terminalStatuses = new Set(['completed', 'no-answer', 'no_answer', 'busy', 'failed', 'canceled']);
    const normalizeStatus = (value) => String(value || '').toLowerCase().replace(/_/g, '-');
    const initialCallDetails = await db.getCall(callSid);
    const persistedStatus = normalizeStatus(initialCallDetails?.status || initialCallDetails?.twilio_status);
    const finalStatus = terminalStatuses.has(persistedStatus) ? persistedStatus : 'completed';
    const notificationMap = {
      completed: 'call_completed',
      'no-answer': 'call_no_answer',
      busy: 'call_busy',
      failed: 'call_failed',
      canceled: 'call_canceled'
    };
    const notificationType = notificationMap[finalStatus] || 'call_completed';
    if (digitService) {
      digitService.clearCallState(callSid);
    }
    clearCallEndLock(callSid);
    clearSilenceTimer(callSid);

    const transcripts = (await db.getCallTranscripts(callSid)) || [];
    const summary = generateCallSummary(transcripts, duration);
    const digitEvents = await db.getCallDigits(callSid).catch(() => []);
    const digitSummary = buildDigitSummary(digitEvents);
    
    // Get personality adaptation data
    const callSession = activeCalls.get(callSid);
    let adaptationAnalysis = {};
    
    if (callSession && callSession.gptService) {
      const conversationAnalysis = callSession.gptService.getConversationAnalysis();
      adaptationAnalysis = {
        personalityChanges: conversationAnalysis.personalityChanges,
        finalPersonality: conversationAnalysis.currentPersonality,
        adaptationEffectiveness: conversationAnalysis.personalityChanges / Math.max(conversationAnalysis.totalInteractions / 10, 1),
        businessContext: callSession.functionSystem?.context || {}
      };
    }
    
    await db.updateCallStatus(callSid, finalStatus, {
      ended_at: callEndTime.toISOString(),
      duration: duration,
      call_summary: summary.summary,
      ai_analysis: JSON.stringify({...summary.analysis, adaptation: adaptationAnalysis}),
      digit_summary: digitSummary.summary,
      digit_count: digitSummary.count
    });

    await db.updateCallState(callSid, 'call_ended', {
      end_time: callEndTime.toISOString(),
      duration: duration,
      total_interactions: transcripts.length,
      personality_adaptations: adaptationAnalysis.personalityChanges || 0
    });

    const callDetails = await db.getCall(callSid);
    
    // Create enhanced webhook notification for completion
    if (callDetails && callDetails.user_chat_id) {
      if (callDetails.last_otp) {
        const masked = digitService ? digitService.formatOtpForDisplay(callDetails.last_otp, 'masked') : callDetails.last_otp;
        const otpMsg = `🔐 ${masked} (call ${callSid.slice(-6)})`;
        try {
          await webhookService.sendTelegramMessage(callDetails.user_chat_id, otpMsg);
        } catch (err) {
          console.error('Error sending OTP to user:', err);
        }
      }

      if (digitEvents && digitEvents.length) {
        const lines = digitEvents
          .filter((d) => d.digits)
          .map((d) => {
            const label = DIGIT_PROFILE_LABELS[d.profile] || d.profile;
            const display = digitService ? digitService.formatDigitsGeneral(d.digits, null, 'notify') : d.digits;
            const src = d.source || 'unknown';
            return `• ${label} [${src}]: ${display}`;
          });
        // Suppressed verbose digit timeline to avoid leaking sensitive digits in notifications
      }
      await db.createEnhancedWebhookNotification(callSid, notificationType, callDetails.user_chat_id);
      
      // Schedule transcript notification with delay
      if (finalStatus === 'completed') {
        setTimeout(async () => {
          try {
            await db.createEnhancedWebhookNotification(callSid, 'call_transcript', callDetails.user_chat_id);
          } catch (transcriptError) {
            console.error('Error creating transcript notification:', transcriptError);
          }
        }, 2000);
      }
    }

    console.log(`Enhanced adaptive call ${callSid} ended (${finalStatus})`);
    console.log(`Duration: ${duration}s | Messages: ${transcripts.length} | Adaptations: ${adaptationAnalysis.personalityChanges || 0}`);
    if (adaptationAnalysis.finalPersonality) {
      console.log(`Final personality: ${adaptationAnalysis.finalPersonality}`);
    }

    // Log service health
    await db.logServiceHealth('call_system', `call_${finalStatus}`, {
      call_sid: callSid,
      duration: duration,
      interactions: transcripts.length,
      adaptations: adaptationAnalysis.personalityChanges || 0
    });

  } catch (error) {
    console.error('Error handling enhanced adaptive call end:', error);
    
    // Log error to service health
    try {
      await db.logServiceHealth('call_system', 'error', {
        operation: 'handle_call_end',
        call_sid: callSid,
        error: error.message
      });
    } catch (logError) {
      console.error('Failed to log service health error:', logError);
    }
  }
}

function generateCallSummary(transcripts, duration) {
  if (!transcripts || transcripts.length === 0) {
    return {
      summary: 'No conversation recorded',
      analysis: { total_messages: 0, user_messages: 0, ai_messages: 0 }
    };
  }

  const userMessages = transcripts.filter(t => t.speaker === 'user');
  const aiMessages = transcripts.filter(t => t.speaker === 'ai');
  
  const analysis = {
    total_messages: transcripts.length,
    user_messages: userMessages.length,
    ai_messages: aiMessages.length,
    duration_seconds: duration,
    conversation_turns: Math.max(userMessages.length, aiMessages.length)
  };

  const summary = `Enhanced adaptive call completed with ${transcripts.length} messages over ${Math.round(duration/60)} minutes. ` +
    `User spoke ${userMessages.length} times, AI responded ${aiMessages.length} times.`;

  return { summary, analysis };
}

function handleTwilioIncoming(req, res) {
  try {
    warnOnInvalidTwilioSignature(req, '/incoming');
    const host = resolveHost(req);
    if (!host) {
      return res.status(500).send('Server hostname not configured');
    }
    console.log(`Incoming call webhook (${req.method}) from ${req.body?.From || 'unknown'} to ${req.body?.To || 'unknown'} host=${host}`);
    const callSid = req.body?.CallSid;
    if (callSid) {
      const timeout = setTimeout(() => {
        if (!activeCalls.has(callSid)) {
          console.warn(`WebSocket not established for CallSid ${callSid} within 5s of /incoming. Check WSS reachability to ${host}.`);
        }
        pendingStreams.delete(callSid);
      }, 5000);
      pendingStreams.set(callSid, timeout);
    }
    const response = new VoiceResponse();
    const connect = response.connect();
    // Request both audio + DTMF events from Twilio Media Streams
    connect.stream({
      url: `wss://${host}/connection`,
      track: TWILIO_STREAM_TRACK,
      statusCallback: `https://${host}/webhook/twilio-stream`,
      statusCallbackMethod: 'POST',
      statusCallbackEvent: ['start', 'end']
    });

    res.type('text/xml');
    res.end(response.toString());
  } catch (err) {
    console.log(err);
    res.status(500).send('Error');
  }
}

// Incoming endpoint used by Twilio to connect the call to our websocket stream
app.post('/incoming', handleTwilioIncoming);
app.get('/incoming', handleTwilioIncoming);

// Telegram callback webhook (live console actions)
app.post('/webhook/telegram', async (req, res) => {
  try {
    const update = req.body;
    res.status(200).send('OK');

    if (!update) return;
    const cb = update.callback_query;
    if (!cb?.data) return;

    const parts = cb.data.split(':');
    const prefix = parts[0];
    let action = null;
    let callSid = null;
    if (prefix === 'lc') {
      action = parts[1];
      callSid = parts[2];
    } else if (prefix === 'recap' || prefix === 'retry') {
      action = parts[1];
      callSid = parts[2];
    } else {
      callSid = parts[1];
    }
    if (!prefix || !callSid || (prefix === 'lc' && !action)) {
      webhookService.answerCallbackQuery(cb.id, 'Unsupported action').catch(() => {});
      return;
    }

    if (prefix === 'retry') {
      const retryAction = action;
      try {
        const callRecord = await db.getCall(callSid).catch(() => null);
        const chatId = cb.message?.chat?.id;
        if (!callRecord) {
          webhookService.answerCallbackQuery(cb.id, 'Call not found').catch(() => {});
          return;
        }
        if (callRecord.user_chat_id && chatId && String(callRecord.user_chat_id) !== String(chatId)) {
          webhookService.answerCallbackQuery(cb.id, 'Not authorized for this call').catch(() => {});
          return;
        }

        if (retryAction === 'sms') {
          if (!callRecord?.phone_number) {
            webhookService.answerCallbackQuery(cb.id, 'No phone number on record').catch(() => {});
            return;
          }
          const callState = await db.getLatestCallState(callSid, 'call_created').catch(() => null);
          const smsBody = buildRetrySmsBody(callRecord, callState);
          try {
            await smsService.sendSMS(callRecord.phone_number, smsBody);
            webhookService.answerCallbackQuery(cb.id, 'SMS sent').catch(() => {});
            await webhookService.sendTelegramMessage(chatId, '💬 Follow-up SMS sent to the victim.');
          } catch (smsError) {
            webhookService.answerCallbackQuery(cb.id, 'Failed to send SMS').catch(() => {});
            await webhookService.sendTelegramMessage(chatId, `❌ Failed to send follow-up SMS: ${smsError.message || smsError}`);
          }
          return;
        }

        const payload = await buildRetryPayload(callSid);
        const delayMs = retryAction === '15m' ? 15 * 60 * 1000 : 0;
        const host = config.server?.hostname;
        if (!host) {
          webhookService.answerCallbackQuery(cb.id, 'Server hostname not configured').catch(() => {});
          return;
        }

        if (delayMs > 0) {
          setTimeout(() => {
            placeOutboundCall(payload, host).catch((err) => {
              console.error('Scheduled retry failed:', err);
            });
          }, delayMs);
          webhookService.answerCallbackQuery(cb.id, 'Retry scheduled').catch(() => {});
          await webhookService.sendTelegramMessage(chatId, `⏲ Retry scheduled in 15 minutes for ${formatContactLabel(payload)}.`);
          return;
        }

        const retryResult = await placeOutboundCall(payload, host);
        webhookService.answerCallbackQuery(cb.id, 'Retry started').catch(() => {});
        await webhookService.sendTelegramMessage(chatId, `🔁 Retry started for ${formatContactLabel(payload)} (call ${retryResult.callId.slice(-6)}).`);
      } catch (error) {
        webhookService.answerCallbackQuery(cb.id, 'Retry failed').catch(() => {});
        await webhookService.sendTelegramMessage(cb.message?.chat?.id, `❌ Retry failed: ${error.message || error}`);
      }
      return;
    }

    if (prefix === 'recap') {
      try {
        const callRecord = await db.getCall(callSid).catch(() => null);
        const chatId = cb.message?.chat?.id;
        if (callRecord?.user_chat_id && chatId && String(callRecord.user_chat_id) !== String(chatId)) {
          webhookService.answerCallbackQuery(cb.id, 'Not authorized for this call').catch(() => {});
          return;
        }

        const recapAction = parts[1];
        if (recapAction === 'skip') {
          webhookService.answerCallbackQuery(cb.id, 'Skipped').catch(() => {});
          return;
        }

        if (recapAction === 'sms') {
          if (!callRecord?.phone_number) {
            webhookService.answerCallbackQuery(cb.id, 'No phone number on record').catch(() => {});
            return;
          }

          const smsBody = buildRecapSmsBody(callRecord);
          try {
            await smsService.sendSMS(callRecord.phone_number, smsBody);
            webhookService.answerCallbackQuery(cb.id, 'Recap sent via SMS').catch(() => {});
            await webhookService.sendTelegramMessage(chatId, '📩 Recap sent via SMS to the victim.');
          } catch (smsError) {
            webhookService.answerCallbackQuery(cb.id, 'Failed to send SMS').catch(() => {});
            await webhookService.sendTelegramMessage(chatId, `❌ Failed to send recap SMS: ${smsError.message || smsError}`);
          }
          return;
        }
      } catch (error) {
        webhookService.answerCallbackQuery(cb.id, 'Error handling recap').catch(() => {});
      }
      return;
    }

    const callRecord = await db.getCall(callSid).catch(() => null);
    const chatId = cb.message?.chat?.id;
    if (callRecord?.user_chat_id && chatId && String(callRecord.user_chat_id) !== String(chatId)) {
      webhookService.answerCallbackQuery(cb.id, 'Not authorized for this call').catch(() => {});
      return;
    }

    if (prefix === 'tr') {
      webhookService.answerCallbackQuery(cb.id, 'Sending transcript...').catch(() => {});
      await webhookService.sendFullTranscript(callSid, chatId, cb.message?.message_id);
      return;
    }

    if (prefix === 'rca') {
      webhookService.answerCallbackQuery(cb.id, 'Fetching recording…').catch(() => {});
      try {
        await db.updateCallState(callSid, 'recording_access_requested', {
          at: new Date().toISOString()
        });
      } catch (stateError) {
        console.error('Failed to log recording access request:', stateError);
      }
      await webhookService.sendTelegramMessage(chatId, '🎧 Recording is being prepared. You will receive it here if available.');
      return;
    }

    if (action === 'rec') {
      webhookService.lockConsoleButtons(callSid, 'Recording…');
      try {
        await db.updateCallState(callSid, 'recording_requested', { at: new Date().toISOString() });
        webhookService.addLiveEvent(callSid, '⏺ Recording requested', { force: true });
        webhookService.answerCallbackQuery(cb.id, 'Recording toggled').catch(() => {});
      } catch (e) {
        webhookService.answerCallbackQuery(cb.id, `Failed: ${e.message}`.slice(0, 180)).catch(() => {});
      }
      setTimeout(() => webhookService.unlockConsoleButtons(callSid), 1200);
      return;
    }

    if (action === 'compact') {
      const isCompact = webhookService.toggleConsoleCompact(callSid);
      if (isCompact === null) {
        webhookService.answerCallbackQuery(cb.id, 'Console not active').catch(() => {});
        return;
      }
      webhookService.answerCallbackQuery(cb.id, isCompact ? 'Compact view enabled' : 'Full view enabled').catch(() => {});
      return;
    }

    if (action === 'end') {
      webhookService.lockConsoleButtons(callSid, 'Ending…');
      try {
        await endCallForProvider(callSid);
        webhookService.setLiveCallPhase(callSid, 'ended').catch(() => {});
        webhookService.answerCallbackQuery(cb.id, 'Ending call...').catch(() => {});
      } catch (e) {
        webhookService.answerCallbackQuery(cb.id, `Failed: ${e.message}`.slice(0, 180)).catch(() => {});
        webhookService.unlockConsoleButtons(callSid);
      }
      setTimeout(() => webhookService.unlockConsoleButtons(callSid), 1500);
      return;
    }

    if (action === 'xfer') {
      if (!config.twilio.transferNumber) {
        webhookService.answerCallbackQuery(cb.id, 'Transfer not configured').catch(() => {});
        return;
      }
      webhookService.lockConsoleButtons(callSid, 'Transferring…');
      try {
        const transferCall = require('./functions/transferCall');
        await transferCall({ callSid });
        webhookService.markToolInvocation(callSid, 'transferCall').catch(() => {});
        webhookService.answerCallbackQuery(cb.id, 'Transferring...').catch(() => {});
      } catch (e) {
        webhookService.answerCallbackQuery(cb.id, `Transfer failed: ${e.message}`.slice(0, 180)).catch(() => {});
        webhookService.unlockConsoleButtons(callSid);
      }
      setTimeout(() => webhookService.unlockConsoleButtons(callSid), 2000);
      return;
    }
  } catch (error) {
    try { res.status(200).send('OK'); } catch {}
    console.error('Telegram webhook error:', error);
  }
});

const handleVonageAnswer = (req, res) => {
  const callSid = req.query.callSid;
  const wsUrl = `wss://${config.server.hostname}/vonage/stream?callSid=${callSid}`;

  res.json([
    {
      action: 'connect',
      endpoint: [
        {
          type: 'websocket',
          uri: wsUrl,
          'content-type': 'audio/pcmu;rate=8000'
        }
      ]
    }
  ]);
};

const handleVonageEvent = async (req, res) => {
  try {
    const { uuid, status, duration } = req.body || {};
    const callSid = req.query.callSid || (uuid ? vonageCallMap.get(uuid) : null) || uuid;

    const statusMap = {
      started: { status: 'initiated', notification: 'call_initiated' },
      ringing: { status: 'ringing', notification: 'call_ringing' },
      answered: { status: 'answered', notification: 'call_answered' },
      completed: { status: 'completed', notification: 'call_completed' },
      busy: { status: 'busy', notification: 'call_busy' },
      failed: { status: 'failed', notification: 'call_failed' },
      timeout: { status: 'no-answer', notification: 'call_no_answer' },
      cancelled: { status: 'canceled', notification: 'call_canceled' }
    };

    const mapped = statusMap[String(status || '').toLowerCase()];
    if (callSid && mapped) {
      await recordCallStatus(callSid, mapped.status, mapped.notification, {
        duration: duration ? parseInt(duration, 10) : undefined
      });
      if (mapped.status === 'completed') {
        const session = activeCalls.get(callSid);
        if (session?.startTime) {
          await handleCallEnd(callSid, session.startTime);
        }
        activeCalls.delete(callSid);
      }
    }

    res.status(200).send('OK');
  } catch (error) {
    console.error('Vonage webhook error:', error);
    res.status(200).send('OK');
  }
};

app.get('/webhook/vonage/answer', handleVonageAnswer);
app.get('/answer', handleVonageAnswer);

app.post('/webhook/vonage/event', handleVonageEvent);
app.post('/event', handleVonageEvent);

app.post('/webhook/aws/status', async (req, res) => {
  try {
    const { contactId, status, duration, callSid } = req.body || {};
    const resolvedCallSid = callSid || (contactId ? awsContactMap.get(contactId) : null);
    if (!resolvedCallSid) {
      return res.status(200).send('OK');
    }

    const normalized = String(status || '').toLowerCase();
    const map = {
      initiated: { status: 'initiated', notification: 'call_initiated' },
      connected: { status: 'answered', notification: 'call_answered' },
      ended: { status: 'completed', notification: 'call_completed' },
      failed: { status: 'failed', notification: 'call_failed' },
      no_answer: { status: 'no-answer', notification: 'call_no_answer' },
      busy: { status: 'busy', notification: 'call_busy' }
    };
    const mapped = map[normalized];
    if (mapped) {
      await recordCallStatus(resolvedCallSid, mapped.status, mapped.notification, {
        duration: duration ? parseInt(duration, 10) : undefined
      });
      if (mapped.status === 'completed') {
        const session = activeCalls.get(resolvedCallSid);
        if (session?.startTime) {
          await handleCallEnd(resolvedCallSid, session.startTime);
        }
        activeCalls.delete(resolvedCallSid);
      }
    }

    res.status(200).send('OK');
  } catch (error) {
    console.error('AWS status webhook error:', error);
    res.status(200).send('OK');
  }
});

app.post('/aws/transcripts', async (req, res) => {
  try {
    const { callSid, transcript, isPartial } = req.body || {};
    if (!callSid || !transcript) {
      return res.status(400).json({ success: false, error: 'callSid and transcript required' });
    }
    if (isPartial) {
      return res.status(200).json({ success: true });
    }
    const session = await ensureAwsSession(callSid);
    clearSilenceTimer(callSid);
    await db.addTranscript({
      call_sid: callSid,
      speaker: 'user',
      message: transcript,
      interaction_count: session.interactionCount
    });
    await db.updateCallState(callSid, 'user_spoke', {
      message: transcript,
      interaction_count: session.interactionCount
    });
    if (shouldCloseConversation(transcript) && session.interactionCount >= 1) {
      await speakAndEndCall(callSid, CALL_END_MESSAGES.user_goodbye, 'user_goodbye');
      session.interactionCount += 1;
      return res.status(200).json({ success: true });
    }
    enqueueGptTask(callSid, async () => {
      const currentCount = session.interactionCount || 0;
      try {
        await session.gptService.completion(transcript, currentCount);
      } catch (gptError) {
        console.error('GPT completion error:', gptError);
        webhookService.addLiveEvent(callSid, '⚠️ GPT error, retrying', { force: true });
      }
      session.interactionCount = currentCount + 1;
    });
    res.status(200).json({ success: true });
  } catch (error) {
    console.error('AWS transcript webhook error:', error);
    res.status(500).json({ success: false, error: 'Failed to ingest transcript' });
  }
});

// Provider status/update endpoints (admin only)
app.get('/admin/provider', requireAdminToken, async (req, res) => {
  const readiness = getProviderReadiness();

  res.json({
    provider: currentProvider,
    stored_provider: storedProvider,
    supported_providers: SUPPORTED_PROVIDERS,
    twilio_ready: readiness.twilio,
    aws_ready: readiness.aws,
    vonage_ready: readiness.vonage
  });
});

app.post('/admin/provider', requireAdminToken, async (req, res) => {
  const { provider } = req.body || {};
  if (!provider || !SUPPORTED_PROVIDERS.includes(provider)) {
    return res.status(400).json({ success: false, error: 'Unsupported provider' });
  }
  const readiness = getProviderReadiness();
  if (!readiness[provider]) {
    return res.status(400).json({ success: false, error: `Provider ${provider} is not configured` });
  }
  const normalized = provider.toLowerCase();
  const changed = normalized !== currentProvider;
  currentProvider = normalized;
  storedProvider = normalized;
  return res.json({ success: true, provider: currentProvider, changed });
});

app.post('/admin/replay/call-status', requireAdminToken, async (req, res) => {
  try {
    const payload = req.body || {};
    const sample = payload.sample;
    const callSid = payload.call_sid || payload.CallSid;
    if (sample && !callSid) {
      return res.status(400).json({ success: false, error: 'call_sid is required when using sample' });
    }
    const resolvedPayload = sample ? (buildSampleCallStatusPayload(sample, callSid) || payload) : payload;
    const result = await processCallStatusWebhookPayload(resolvedPayload, { source: 'replay' });
    if (!result?.ok) {
      return res.status(404).json({ success: false, error: result?.error || 'call_not_found' });
    }
    return res.json({ success: true, ...result });
  } catch (error) {
    console.error('Replay call-status error:', error);
    return res.status(500).json({ success: false, error: error.message });
  }
});

// Personas list for bot selection
app.get('/api/personas', async (req, res) => {
  res.json({
    success: true,
    builtin: builtinPersonas,
    custom: []
  });
});

// Call script endpoints for bot script management
app.get('/api/call-scripts', async (req, res) => {
  try {
    const scripts = await db.getCallTemplates();
    res.json({ success: true, scripts });
  } catch (error) {
    res.status(500).json({ success: false, error: 'Failed to fetch call scripts' });
  }
});

app.get('/api/call-scripts/:id', async (req, res) => {
  try {
    const scriptId = Number(req.params.id);
    if (Number.isNaN(scriptId)) {
      return res.status(400).json({ success: false, error: 'Invalid script id' });
    }
    const script = await db.getCallTemplateById(scriptId);
    if (!script) {
      return res.status(404).json({ success: false, error: 'Script not found' });
    }
    res.json({ success: true, script });
  } catch (error) {
    res.status(500).json({ success: false, error: 'Failed to fetch call script' });
  }
});

app.post('/api/call-scripts', async (req, res) => {
  try {
    const { name, first_message } = req.body || {};
    if (!name || !first_message) {
      return res.status(400).json({ success: false, error: 'name and first_message are required' });
    }
    const id = await db.createCallTemplate(req.body);
    const script = await db.getCallTemplateById(id);
    res.status(201).json({ success: true, script });
  } catch (error) {
    res.status(500).json({ success: false, error: 'Failed to create call script' });
  }
});

app.put('/api/call-scripts/:id', async (req, res) => {
  try {
    const scriptId = Number(req.params.id);
    if (Number.isNaN(scriptId)) {
      return res.status(400).json({ success: false, error: 'Invalid script id' });
    }
    const updated = await db.updateCallTemplate(scriptId, req.body || {});
    if (!updated) {
      return res.status(404).json({ success: false, error: 'Script not found' });
    }
    const script = await db.getCallTemplateById(scriptId);
    res.json({ success: true, script });
  } catch (error) {
    res.status(500).json({ success: false, error: 'Failed to update call script' });
  }
});

app.delete('/api/call-scripts/:id', async (req, res) => {
  try {
    const scriptId = Number(req.params.id);
    if (Number.isNaN(scriptId)) {
      return res.status(400).json({ success: false, error: 'Invalid script id' });
    }
    const deleted = await db.deleteCallTemplate(scriptId);
    if (!deleted) {
      return res.status(404).json({ success: false, error: 'Script not found' });
    }
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ success: false, error: 'Failed to delete call script' });
  }
});

app.post('/api/call-scripts/:id/clone', async (req, res) => {
  try {
    const scriptId = Number(req.params.id);
    if (Number.isNaN(scriptId)) {
      return res.status(400).json({ success: false, error: 'Invalid script id' });
    }
    const existing = await db.getCallTemplateById(scriptId);
    if (!existing) {
      return res.status(404).json({ success: false, error: 'Script not found' });
    }
    const payload = {
      ...existing,
      name: req.body?.name || `${existing.name} Copy`
    };
    delete payload.id;
    const newId = await db.createCallTemplate(payload);
    const script = await db.getCallTemplateById(newId);
    res.status(201).json({ success: true, script });
  } catch (error) {
    res.status(500).json({ success: false, error: 'Failed to clone call script' });
  }
});

async function buildRetryPayload(callSid) {
  const callRecord = await db.getCall(callSid);
  if (!callRecord) {
    throw new Error('Call not found');
  }
  const callState = await db.getLatestCallState(callSid, 'call_created').catch(() => null);

  return {
    number: callRecord.phone_number,
    prompt: callRecord.prompt,
    first_message: callRecord.first_message,
    user_chat_id: callRecord.user_chat_id,
    customer_name: callState?.customer_name || callState?.victim_name || null,
    business_id: callState?.business_id || null,
    script: callState?.script || null,
    script_id: callState?.script_id || null,
    purpose: callState?.purpose || null,
    emotion: callState?.emotion || null,
    urgency: callState?.urgency || null,
    technical_level: callState?.technical_level || null,
    voice_model: callState?.voice_model || null,
    collection_profile: callState?.collection_profile || null,
    collection_expected_length: callState?.collection_expected_length || null,
    collection_timeout_s: callState?.collection_timeout_s || null,
    collection_max_retries: callState?.collection_max_retries || null,
    collection_mask_for_gpt: callState?.collection_mask_for_gpt,
    collection_speak_confirmation: callState?.collection_speak_confirmation
  };
}

async function placeOutboundCall(payload, hostOverride = null) {
  const {
    number,
    prompt,
    first_message,
    user_chat_id,
    customer_name,
    business_id,
    script,
    script_id,
    purpose,
    emotion,
    urgency,
    technical_level,
    voice_model,
    collection_profile,
    collection_expected_length,
    collection_timeout_s,
    collection_max_retries,
    collection_mask_for_gpt,
    collection_speak_confirmation
  } = payload || {};

  if (!number || !prompt || !first_message) {
    throw new Error('Missing required fields: number, prompt, and first_message are required');
  }

  if (!number.match(/^\+[1-9]\d{1,14}$/)) {
    throw new Error('Invalid phone number format. Use E.164 format (e.g., +1234567890)');
  }

  const host = hostOverride || config.server?.hostname;
  if (!host) {
    throw new Error('Server hostname not configured');
  }

  console.log('Generating adaptive function system for call...'.blue);
  const functionSystem = functionEngine.generateAdaptiveFunctionSystem(prompt, first_message);
  console.log(`Generated ${functionSystem.functions.length} functions for ${functionSystem.context.industry} industry`);

  let callId;
  let callStatus = 'queued';
  let providerMetadata = {};

  if (currentProvider === 'twilio') {
    warnIfMachineDetectionDisabled('outbound-call');
    const accountSid = config.twilio.accountSid;
    const authToken = config.twilio.authToken;
    const fromNumber = config.twilio.fromNumber;

    if (!accountSid || !authToken || !fromNumber) {
      throw new Error('Twilio credentials not configured');
    }

    const client = twilio(accountSid, authToken);
    const twimlUrl = `https://${host}/incoming`;
    const statusUrl = `https://${host}/webhook/call-status`;
    console.log(`Twilio call URLs: twiml=${twimlUrl} statusCallback=${statusUrl}`);
    const callPayload = {
      url: twimlUrl,
      to: number,
      from: fromNumber,
      statusCallback: statusUrl,
      statusCallbackEvent: ['initiated', 'ringing', 'answered', 'completed', 'busy', 'no-answer', 'canceled', 'failed'],
      statusCallbackMethod: 'POST'
    };
    if (config.twilio?.machineDetection) {
      callPayload.machineDetection = config.twilio.machineDetection;
    }
    if (Number.isFinite(config.twilio?.machineDetectionTimeout)) {
      callPayload.machineDetectionTimeout = config.twilio.machineDetectionTimeout;
    }
    const call = await client.calls.create(callPayload);
    callId = call.sid;
    callStatus = call.status || 'queued';
  } else if (currentProvider === 'aws') {
    const awsAdapter = getAwsConnectAdapter();
    callId = uuidv4();
    const response = await awsAdapter.startOutboundCall({
      destinationPhoneNumber: number,
      clientToken: callId,
      attributes: {
        CALL_SID: callId,
        FIRST_MESSAGE: first_message
      }
    });
    providerMetadata = { contact_id: response.ContactId };
    if (response.ContactId) {
      awsContactMap.set(response.ContactId, callId);
    }
    callStatus = 'queued';
  } else if (currentProvider === 'vonage') {
    const vonageAdapter = getVonageVoiceAdapter();
    callId = uuidv4();
    const answerUrl = config.vonage.voice.answerUrl ||
      `https://${host}/webhook/vonage/answer?callSid=${callId}`;
    const eventUrl = config.vonage.voice.eventUrl ||
      `https://${host}/webhook/vonage/event?callSid=${callId}`;
    const response = await vonageAdapter.createOutboundCall({
      to: number,
      callSid: callId,
      answerUrl,
      eventUrl
    });
    const vonageUuid = response?.uuid;
    providerMetadata = { vonage_uuid: vonageUuid };
    if (vonageUuid) {
      vonageCallMap.set(vonageUuid, callId);
    }
    callStatus = response?.status || 'queued';
  } else {
    throw new Error(`Unsupported provider ${currentProvider}`);
  }

  let scriptPolicy = {};
  if (script_id) {
    try {
      const tpl = await db.getCallTemplateById(Number(script_id));
      if (tpl) {
        scriptPolicy = {
          requires_otp: !!tpl.requires_otp,
          default_profile: tpl.default_profile || null,
          expected_length: tpl.expected_length || null,
          allow_terminator: !!tpl.allow_terminator,
          terminator_char: tpl.terminator_char || null
        };
      }
    } catch (err) {
      console.error('Script metadata load error:', err);
    }
  }

  const callConfig = {
    prompt: prompt,
    first_message: first_message,
    created_at: new Date().toISOString(),
    user_chat_id: user_chat_id,
    customer_name: customer_name || null,
    provider: currentProvider,
    provider_metadata: providerMetadata,
    business_context: functionSystem.context,
    function_count: functionSystem.functions.length,
    purpose: purpose || null,
    business_id: business_id || null,
    script: script || null,
    script_id: script_id || null,
    emotion: emotion || null,
    urgency: urgency || null,
    technical_level: technical_level || null,
    voice_model: voice_model || null,
    collection_profile: collection_profile || null,
    collection_expected_length: collection_expected_length || null,
    collection_timeout_s: collection_timeout_s || null,
    collection_max_retries: collection_max_retries || null,
    collection_mask_for_gpt: collection_mask_for_gpt,
    collection_speak_confirmation: collection_speak_confirmation,
    script_policy: scriptPolicy
  };

  callConfigurations.set(callId, callConfig);
  callFunctionSystems.set(callId, functionSystem);

  try {
    await db.createCall({
      call_sid: callId,
      phone_number: number,
      prompt: prompt,
      first_message: first_message,
      user_chat_id: user_chat_id,
      business_context: JSON.stringify(functionSystem.context),
      generated_functions: JSON.stringify(functionSystem.functions.map(f => f.function.name))
    });
    await db.updateCallState(callId, 'call_created', {
      customer_name: customer_name || null,
      business_id: business_id || null,
      script: script || null,
      script_id: script_id || null,
      purpose: purpose || null,
      emotion: emotion || null,
      urgency: urgency || null,
      technical_level: technical_level || null,
      voice_model: voice_model || null,
      provider: currentProvider,
      provider_metadata: providerMetadata,
      collection_profile: collection_profile || null,
      collection_expected_length: collection_expected_length || null,
      collection_timeout_s: collection_timeout_s || null,
      collection_max_retries: collection_max_retries || null,
      collection_mask_for_gpt: collection_mask_for_gpt,
      collection_speak_confirmation: collection_speak_confirmation
    });

    if (user_chat_id) {
      await db.createEnhancedWebhookNotification(callId, 'call_initiated', user_chat_id);
    }

    console.log(`Enhanced adaptive call created: ${callId} to ${number}`);
    console.log(`Business context: ${functionSystem.context.industry} - ${functionSystem.context.businessType}`);
  } catch (dbError) {
    console.error('Database error:', dbError);
  }

  return { callId, callStatus, functionSystem };
}

// Enhanced outbound call endpoint with dynamic function generation
app.post('/outbound-call', async (req, res) => {
  try {
    const resolvedCustomerName = req.body?.customer_name ?? req.body?.victim_name ?? null;
    const payload = {
      number: req.body?.number,
      prompt: req.body?.prompt,
      first_message: req.body?.first_message,
      user_chat_id: req.body?.user_chat_id,
      customer_name: resolvedCustomerName,
      business_id: req.body?.business_id,
      script: req.body?.script,
      script_id: req.body?.script_id,
      purpose: req.body?.purpose,
      emotion: req.body?.emotion,
      urgency: req.body?.urgency,
      technical_level: req.body?.technical_level,
      voice_model: req.body?.voice_model,
      collection_profile: req.body?.collection_profile,
      collection_expected_length: req.body?.collection_expected_length,
      collection_timeout_s: req.body?.collection_timeout_s,
      collection_max_retries: req.body?.collection_max_retries,
      collection_mask_for_gpt: req.body?.collection_mask_for_gpt,
      collection_speak_confirmation: req.body?.collection_speak_confirmation
    };

    const host = resolveHost(req) || config.server?.hostname;
    const result = await placeOutboundCall(payload, host);

    res.json({
      success: true,
      call_sid: result.callId,
      to: payload.number,
      status: result.callStatus,
      provider: currentProvider,
      business_context: result.functionSystem.context,
      generated_functions: result.functionSystem.functions.length,
      function_types: result.functionSystem.functions.map(f => f.function.name),
      enhanced_webhooks: true
    });
  } catch (error) {
    console.error('Error creating enhanced adaptive outbound call:', error);
    res.status(500).json({
      error: 'Failed to create outbound call',
      details: error.message
    });
  }
});

function buildSampleCallStatusPayload(sample, callSid) {
  if (!callSid) {
    return null;
  }
  const normalized = String(sample || '').toLowerCase();
  const base = {
    CallSid: callSid,
    From: '+15551230000',
    To: '+15551239999'
  };
  if (normalized === 'voicemail') {
    return {
      ...base,
      CallStatus: 'completed',
      AnsweredBy: 'machine_start',
      CallDuration: '0',
      DialCallDuration: '0'
    };
  }
  if (normalized === 'human') {
    return {
      ...base,
      CallStatus: 'completed',
      AnsweredBy: 'human',
      CallDuration: '42',
      DialCallDuration: '42'
    };
  }
  if (normalized === 'no-answer') {
    return {
      ...base,
      CallStatus: 'no-answer'
    };
  }
  return null;
}

async function processCallStatusWebhookPayload(payload = {}, options = {}) {
  const {
    CallSid,
    CallStatus,
    Duration,
    From,
    To,
    CallDuration,
    AnsweredBy,
    ErrorCode,
    ErrorMessage,
    DialCallDuration
  } = payload || {};

  if (!CallSid) {
    const err = new Error('Missing CallSid');
    err.code = 'missing_call_sid';
    throw err;
  }

  const source = options.source || 'provider';

  console.log(`Fixed Webhook: Call ${CallSid} status: ${CallStatus}`.blue);
  console.log(`Debug Info:`);
  console.log(`Duration: ${Duration || 'N/A'}`);
  console.log(`CallDuration: ${CallDuration || 'N/A'}`);
  console.log(`DialCallDuration: ${DialCallDuration || 'N/A'}`);
  console.log(`AnsweredBy: ${AnsweredBy || 'N/A'}`);

  const durationCandidates = [Duration, CallDuration, DialCallDuration]
    .map((value) => parseInt(value, 10))
    .filter((value) => Number.isFinite(value));
  const durationValue = durationCandidates.length ? Math.max(...durationCandidates) : 0;

  const call = await db.getCall(CallSid);
  if (!call) {
    console.warn(`Webhook received for unknown call: ${CallSid}`);
    return { ok: false, error: 'call_not_found', callSid: CallSid };
  }

  let notificationType = null;
  const rawStatus = String(CallStatus || '').toLowerCase();
  const answeredByValue = String(AnsweredBy || '').toLowerCase();
  const isMachineAnswered = ['machine_start', 'machine_end', 'machine', 'fax'].includes(answeredByValue);
  const voicemailDetected = isMachineAnswered;
  let actualStatus = rawStatus || 'unknown';

  if (voicemailDetected) {
    console.log(`AMD detected voicemail (${answeredByValue}) - classifying as no-answer`.yellow);
    actualStatus = 'no-answer';
    notificationType = 'call_no_answer';
  } else if (actualStatus === 'completed') {
    const priorStatus = String(call.status || '').toLowerCase();
    const hasAnswerEvidence =
      !!call.started_at ||
      ['answered', 'in-progress', 'completed'].includes(priorStatus) ||
      durationValue > 0;

    console.log(`Analyzing completed call: Duration = ${durationValue}s`);

    if ((durationValue === 0 || durationValue < 3) && !hasAnswerEvidence) {
      console.log(`Short duration detected (${durationValue}s) - treating as no-answer`.red);
      actualStatus = 'no-answer';
      notificationType = 'call_no_answer';
    } else if (voicemailDetected && durationValue < 10 && !hasAnswerEvidence) {
      console.log(`Voicemail detected with short duration - classifying as no-answer`.red);
      actualStatus = 'no-answer';
      notificationType = 'call_no_answer';
    } else {
      console.log(`Valid call duration (${durationValue}s) - confirmed answered`);
      actualStatus = 'completed';
      notificationType = 'call_completed';
    }
  } else {
    switch (actualStatus) {
      case 'queued':
      case 'initiated':
        notificationType = 'call_initiated';
        break;
      case 'ringing':
        notificationType = 'call_ringing';
        break;
      case 'in-progress':
        notificationType = 'call_in_progress';
        break;
      case 'answered':
        notificationType = 'call_answered';
        break;
      case 'busy':
        notificationType = 'call_busy';
        break;
      case 'no-answer':
        notificationType = 'call_no_answer';
        break;
      case 'voicemail':
        actualStatus = 'no-answer';
        notificationType = 'call_no_answer';
        break;
      case 'failed':
        notificationType = 'call_failed';
        break;
      case 'canceled':
        notificationType = 'call_canceled';
        break;
      default:
        console.warn(`Unknown call status: ${CallStatus}`);
        notificationType = `call_${actualStatus}`;
    }
  }

  const priorStatus = String(call.status || '').toLowerCase();
  const hasAnswerEvidence = !!call.started_at ||
    ['answered', 'in-progress', 'completed'].includes(priorStatus) ||
    durationValue > 0 ||
    !!AnsweredBy;

  if (actualStatus === 'no-answer' && hasAnswerEvidence && !voicemailDetected) {
    actualStatus = 'completed';
    notificationType = 'call_completed';
  }

  console.log(`Final determination: ${CallStatus} → ${actualStatus} → ${notificationType}`);

  const updateData = {
    duration: durationValue,
    twilio_status: CallStatus,
    answered_by: AnsweredBy,
    error_code: ErrorCode,
    error_message: ErrorMessage
  };

  if (actualStatus === 'ringing') {
    try {
      await db.updateCallState(CallSid, 'ringing', { at: new Date().toISOString() });
    } catch (stateError) {
      console.error('Failed to record ringing state:', stateError);
    }
  }

  if (actualStatus === 'no-answer' && call.created_at) {
    let ringStart = null;
    try {
      const ringState = await db.getLatestCallState(CallSid, 'ringing');
      ringStart = ringState?.at || ringState?.timestamp || null;
    } catch (stateError) {
      console.error('Failed to load ringing state:', stateError);
    }

    const now = new Date();
    const callStart = new Date(call.created_at);
    const ringStartTime = ringStart ? new Date(ringStart) : callStart;
    const ringDuration = Math.round((now - ringStartTime) / 1000);
    updateData.ring_duration = ringDuration;
    if (!updateData.duration || updateData.duration < ringDuration) {
      updateData.duration = ringDuration;
    }
    console.log(`Calculated ring duration: ${ringDuration}s`);
  }

  if (['in-progress', 'answered'].includes(actualStatus) && !call.started_at) {
    updateData.started_at = new Date().toISOString();
  } else if (!call.ended_at) {
    const isTerminal = ['completed', 'no-answer', 'failed', 'busy', 'canceled'].includes(actualStatus);
    const rawTerminal = ['completed', 'no-answer', 'failed', 'busy', 'canceled'].includes(rawStatus);
    if (isTerminal && rawTerminal) {
      updateData.ended_at = new Date().toISOString();
    }
  }

  await db.updateCallStatus(CallSid, actualStatus, updateData);

  if (call.user_chat_id && notificationType && !options.skipNotifications) {
    try {
      await db.createEnhancedWebhookNotification(CallSid, notificationType, call.user_chat_id);
      console.log(`📨 Created corrected ${notificationType} notification for call ${CallSid}`);

      if (actualStatus !== CallStatus.toLowerCase()) {
        await db.logServiceHealth('webhook_system', 'status_corrected', {
          call_sid: CallSid,
          original_status: CallStatus,
          corrected_status: actualStatus,
          duration: updateData.duration,
          reason: 'Short duration analysis',
          source
        });
      }
    } catch (notificationError) {
      console.error('Error creating enhanced webhook notification:', notificationError);
    }
  }

  console.log(`Fixed webhook processed: ${CallSid} -> ${CallStatus} (corrected to: ${actualStatus})`);
  if (updateData.duration) {
    const minutes = Math.floor(updateData.duration / 60);
    const seconds = updateData.duration % 60;
    console.log(`Call metrics: ${minutes}:${String(seconds).padStart(2, '0')} duration`);
  }

  await db.logServiceHealth('webhook_system', 'status_received', {
    call_sid: CallSid,
    original_status: CallStatus,
    final_status: actualStatus,
    duration: updateData.duration,
    answered_by: AnsweredBy,
    correction_applied: actualStatus !== CallStatus.toLowerCase(),
    source
  });

  return {
    ok: true,
    callSid: CallSid,
    rawStatus,
    actualStatus,
    notificationType,
    duration: updateData.duration,
    voicemailDetected
  };
}

// Enhanced webhook endpoint for call status updates

app.post('/webhook/call-status', async (req, res) => {
  try {
    warnOnInvalidTwilioSignature(req, '/webhook/call-status');
    await processCallStatusWebhookPayload(req.body, { source: 'provider' });
  } catch (error) {
    console.error('Error processing fixed call status webhook:', error);
    
    // Log error to service health
    try {
      await db.logServiceHealth('webhook_system', 'error', {
        operation: 'process_webhook',
        error: error.message,
        call_sid: req.body?.CallSid
      });
    } catch (logError) {
      console.error('Failed to log webhook error:', logError);
    }
  }
  res.status(200).send('OK');
});

// Twilio Media Stream status callback
app.post('/webhook/twilio-stream', (req, res) => {
  try {
    const payload = req.body || {};
    const callSid = payload.CallSid || payload.callSid || 'unknown';
    const streamSid = payload.StreamSid || payload.streamSid || 'unknown';
    const eventType = payload.EventType || payload.eventType || payload.event || 'unknown';
    console.log(`Twilio stream status: callSid=${callSid} streamSid=${streamSid} event=${eventType}`);
    if (Object.keys(payload).length > 0) {
      console.log(`Twilio stream payload: ${JSON.stringify(payload)}`);
    }
  } catch (err) {
    console.error('Twilio stream status webhook error:', err);
  }
  res.status(200).send('OK');
});


// Enhanced API endpoints with adaptation analytics

// Get call details with enhanced personality and function analytics
app.get('/api/calls/:callSid', async (req, res) => {
  try {
    const { callSid } = req.params;
    
    const call = await db.getCall(callSid);
    if (!call) {
      return res.status(404).json({ error: 'Call not found' });
    }
    let callState = null;
    try {
      callState = await db.getLatestCallState(callSid, 'call_created');
    } catch (_) {
      callState = null;
    }
    const enrichedCall = callState?.customer_name || callState?.victim_name
      ? { ...call, customer_name: callState?.customer_name || callState?.victim_name }
      : call;
    const normalizedCall = normalizeCallRecordForApi(enrichedCall);

    const transcripts = await db.getCallTranscripts(callSid);
    
    // Parse adaptation data
    let adaptationData = {};
    try {
      if (call.ai_analysis) {
        const analysis = JSON.parse(call.ai_analysis);
        adaptationData = analysis.adaptation || {};
      }
    } catch (e) {
      console.error('Error parsing adaptation data:', e);
    }

    // Get webhook notifications for this call
    const webhookNotifications = await new Promise((resolve, reject) => {
      db.db.all(
        `SELECT * FROM webhook_notifications WHERE call_sid = ? ORDER BY created_at DESC`,
        [callSid],
        (err, rows) => {
          if (err) reject(err);
          else resolve(rows || []);
        }
      );
    });
    
    res.json({
      call: normalizedCall,
      transcripts,
      transcript_count: transcripts.length,
      adaptation_analytics: adaptationData,
      business_context: call.business_context ? JSON.parse(call.business_context) : null,
      webhook_notifications: webhookNotifications,
      enhanced_features: true
    });
  } catch (error) {
    console.error('Error fetching enhanced adaptive call details:', error);
    res.status(500).json({ error: 'Failed to fetch call details' });
  }
});

app.get('/api/calls/:callSid/transcript/audio', async (req, res) => {
  try {
    const { callSid } = req.params;
    const call = await db.getCall(callSid);
    if (!call) {
      return res.status(404).json({ error: 'Call not found' });
    }
    const entry = await ensureTranscriptAudio(callSid);
    if (entry.status === 'processing') {
      return res.status(202).json({ status: 'processing', retry_after_ms: 2000 });
    }
    if (entry.status === 'error') {
      return res.status(500).json({ status: 'error', error: entry.error || 'Transcript audio failed' });
    }
    res.setHeader('Content-Type', 'audio/mpeg');
    res.setHeader('Content-Disposition', 'attachment; filename="transcript.mp3"');
    return res.status(200).send(entry.buffer);
  } catch (error) {
    console.error('Error generating transcript audio:', error);
    return res.status(500).json({ status: 'error', error: error.message || 'Transcript audio failed' });
  }
});

// Enhanced call status endpoint with real-time metrics
app.get('/api/calls/:callSid/status', async (req, res) => {
  try {
    const { callSid } = req.params;
    
    const call = await db.getCall(callSid);
    if (!call) {
      return res.status(404).json({ error: 'Call not found' });
    }

    // Get recent call states for detailed progress tracking
    const recentStates = await new Promise((resolve, reject) => {
      db.db.all(
        `SELECT state, data, timestamp FROM call_states 
         WHERE call_sid = ? 
         ORDER BY timestamp DESC 
         LIMIT 10`,
        [callSid],
        (err, rows) => {
          if (err) reject(err);
          else resolve(rows || []);
        }
      );
    });

    // Get enhanced webhook notification status
    const notificationStatus = await new Promise((resolve, reject) => {
      db.db.all(
        `SELECT notification_type, status, created_at, sent_at, delivery_time_ms, error_message 
         FROM webhook_notifications 
         WHERE call_sid = ? 
         ORDER BY created_at DESC`,
        [callSid],
        (err, rows) => {
          if (err) reject(err);
          else resolve(rows || []);
        }
      );
    });

    // Calculate enhanced call timing metrics
    let timingMetrics = {};
    if (call.created_at) {
      const now = new Date();
      const created = new Date(call.created_at);
      timingMetrics.total_elapsed = Math.round((now - created) / 1000);
      
      if (call.started_at) {
        const started = new Date(call.started_at);
        timingMetrics.time_to_answer = Math.round((started - created) / 1000);
      }
      
      if (call.ended_at) {
        const ended = new Date(call.ended_at);
        timingMetrics.call_duration = call.duration || Math.round((ended - new Date(call.started_at || call.created_at)) / 1000);
      }

      // Calculate ring duration if available
      if (call.ring_duration) {
        timingMetrics.ring_duration = call.ring_duration;
      }
    }

    res.json({
      call: {
        ...call,
        timing_metrics: timingMetrics
      },
      recent_states: recentStates,
      notification_status: notificationStatus,
      webhook_service_status: webhookService.getCallStatusStats(),
      enhanced_tracking: true
    });
    
  } catch (error) {
    console.error('Error fetching enhanced call status:', error);
    res.status(500).json({ error: 'Failed to fetch call status' });
  }
});

// Call latency diagnostics endpoint (best-effort)
app.get('/api/calls/:callSid/latency', async (req, res) => {
  try {
    const { callSid } = req.params;
    const call = await db.getCall(callSid);
    if (!call) {
      return res.status(404).json({ error: 'Call not found' });
    }

    const states = await new Promise((resolve, reject) => {
      db.db.all(
        `SELECT state, timestamp FROM call_states
         WHERE call_sid = ?
           AND state IN ('user_spoke', 'ai_responded', 'tts_ready')
         ORDER BY timestamp DESC`,
        [callSid],
        (err, rows) => {
          if (err) reject(err);
          else resolve(rows || []);
        }
      );
    });

    const latest = { user_spoke: null, ai_responded: null, tts_ready: null };
    for (const row of states) {
      if (!latest[row.state]) {
        latest[row.state] = row;
      }
    }

    const toMs = (row) => (row?.timestamp ? new Date(row.timestamp).getTime() : null);
    const userSpokeAt = toMs(latest.user_spoke);
    const aiRespondedAt = toMs(latest.ai_responded);
    const ttsReadyAt = toMs(latest.tts_ready);

    const gptMs = userSpokeAt && aiRespondedAt && aiRespondedAt >= userSpokeAt
      ? aiRespondedAt - userSpokeAt
      : null;
    const ttsMs = aiRespondedAt && ttsReadyAt && ttsReadyAt >= aiRespondedAt
      ? ttsReadyAt - aiRespondedAt
      : null;

    res.json({
      call_sid: callSid,
      latency_metrics: {
        stt_ms: null,
        gpt_ms: gptMs,
        tts_ms: ttsMs
      },
      call_duration: call.duration || 0,
      source: 'call_states',
      computed_at: new Date().toISOString()
    });
  } catch (error) {
    console.error('Error fetching call latency:', error);
    res.status(500).json({ error: 'Failed to fetch call latency' });
  }
});

// Manual notification trigger endpoint (for testing)
app.post('/api/calls/:callSid/notify', async (req, res) => {
  try {
    const { callSid } = req.params;
    const { status, user_chat_id } = req.body;
    
    if (!status || !user_chat_id) {
      return res.status(400).json({ 
        error: 'Both status and user_chat_id are required' 
      });
    }

    const call = await db.getCall(callSid);
    if (!call) {
      return res.status(404).json({ error: 'Call not found' });
    }

    // Send immediate enhanced notification
    const success = await webhookService.sendImmediateStatus(callSid, status, user_chat_id);
    
    if (success) {
      res.json({ 
        success: true, 
        message: `Enhanced manual notification sent: ${status}`,
        call_sid: callSid,
        enhanced: true
      });
    } else {
      res.status(500).json({ 
        success: false, 
        error: 'Failed to send enhanced notification' 
      });
    }
    
  } catch (error) {
    console.error('Error sending enhanced manual notification:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to send notification',
      details: error.message 
    });
  }
});

// Get enhanced adaptation analytics dashboard data
app.get('/api/analytics/adaptations', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 50;
    const calls = await db.getCallsWithTranscripts(limit);
    
    const analyticsData = {
      total_calls: calls.length,
      calls_with_adaptations: 0,
      total_adaptations: 0,
      personality_usage: {},
      industry_breakdown: {},
      adaptation_triggers: {},
      enhanced_features: true
    };

    calls.forEach(call => {
      try {
        if (call.ai_analysis) {
          const analysis = JSON.parse(call.ai_analysis);
          if (analysis.adaptation && analysis.adaptation.personalityChanges > 0) {
            analyticsData.calls_with_adaptations++;
            analyticsData.total_adaptations += analysis.adaptation.personalityChanges;
            
            // Track final personality usage
            const finalPersonality = analysis.adaptation.finalPersonality;
            if (finalPersonality) {
              analyticsData.personality_usage[finalPersonality] = 
                (analyticsData.personality_usage[finalPersonality] || 0) + 1;
            }
            
            // Track industry usage
            const industry = analysis.adaptation.businessContext?.industry;
            if (industry) {
              analyticsData.industry_breakdown[industry] = 
                (analyticsData.industry_breakdown[industry] || 0) + 1;
            }
          }
        }
      } catch (e) {
        // Skip calls with invalid analysis data
      }
    });

    analyticsData.adaptation_rate = analyticsData.total_calls > 0 ? 
      (analyticsData.calls_with_adaptations / analyticsData.total_calls * 100).toFixed(1) : 0;
    
    analyticsData.avg_adaptations_per_call = analyticsData.calls_with_adaptations > 0 ? 
      (analyticsData.total_adaptations / analyticsData.calls_with_adaptations).toFixed(1) : 0;

    res.json(analyticsData);
  } catch (error) {
    console.error('Error fetching enhanced adaptation analytics:', error);
    res.status(500).json({ error: 'Failed to fetch analytics' });
  }
});

// Enhanced notification analytics endpoint
app.get('/api/analytics/notifications', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 100;
    const hours = parseInt(req.query.hours) || 24;
    
    const notificationStats = await new Promise((resolve, reject) => {
      db.db.all(`
        SELECT 
          notification_type,
          status,
          COUNT(*) as count,
          AVG(CASE 
            WHEN sent_at IS NOT NULL AND created_at IS NOT NULL 
            THEN (julianday(sent_at) - julianday(created_at)) * 86400 
            ELSE NULL 
          END) as avg_delivery_time_seconds,
          AVG(delivery_time_ms) as avg_delivery_time_ms
        FROM webhook_notifications 
        WHERE created_at >= datetime('now', '-${hours} hours')
        GROUP BY notification_type, status
        ORDER BY notification_type, status
      `, (err, rows) => {
        if (err) reject(err);
        else resolve(rows || []);
      });
    });

    const recentNotifications = await new Promise((resolve, reject) => {
      db.db.all(`
        SELECT 
          wn.*,
          c.phone_number,
          c.status as call_status,
          c.twilio_status
        FROM webhook_notifications wn
        LEFT JOIN calls c ON wn.call_sid = c.call_sid
        WHERE wn.created_at >= datetime('now', '-${hours} hours')
        ORDER BY wn.created_at DESC
        LIMIT ${limit}
      `, (err, rows) => {
        if (err) reject(err);
        else resolve(rows || []);
      });
    });

    // Calculate enhanced summary metrics
    const totalNotifications = notificationStats.reduce((sum, stat) => sum + stat.count, 0);
    const successfulNotifications = notificationStats
      .filter(stat => stat.status === 'sent')
      .reduce((sum, stat) => sum + stat.count, 0);
    
    const successRate = totalNotifications > 0 ? 
      ((successfulNotifications / totalNotifications) * 100).toFixed(1) : 0;

    const avgDeliveryTime = notificationStats
      .filter(stat => stat.avg_delivery_time_seconds !== null)
      .reduce((sum, stat, _, arr) => {
        return sum + (stat.avg_delivery_time_seconds / arr.length);
      }, 0);

    // Get notification metrics from database
    const notificationMetrics = await db.getNotificationAnalytics(Math.ceil(hours / 24));

    res.json({
      summary: {
        total_notifications: totalNotifications,
        successful_notifications: successfulNotifications,
        success_rate_percent: parseFloat(successRate),
        average_delivery_time_seconds: avgDeliveryTime.toFixed(2),
        time_period_hours: hours,
        enhanced_tracking: true
      },
      notification_breakdown: notificationStats,
      recent_notifications: recentNotifications,
      historical_metrics: notificationMetrics,
      webhook_service_health: await webhookService.healthCheck()
    });
    
  } catch (error) {
    console.error('Error fetching enhanced notification analytics:', error);
    res.status(500).json({ 
      error: 'Failed to fetch notification analytics',
      details: error.message 
    });
  }
});

// Generate functions for a given prompt (testing endpoint)
app.post('/api/generate-functions', async (req, res) => {
  try {
    const { prompt, first_message } = req.body;
    
    if (!prompt || !first_message) {
      return res.status(400).json({ error: 'Both prompt and first_message are required' });
    }

    const functionSystem = functionEngine.generateAdaptiveFunctionSystem(prompt, first_message);
    
    res.json({
      success: true,
      business_context: functionSystem.context,
      functions: functionSystem.functions,
      function_count: functionSystem.functions.length,
      analysis: functionEngine.getBusinessAnalysis(),
      enhanced: true
    });
  } catch (error) {
    console.error('Error generating enhanced functions:', error);
    res.status(500).json({ error: 'Failed to generate functions' });
  }
});

// Version info endpoint for bot diagnostics
app.get('/api/version', (req, res) => {
  res.json({
    name: apiPackage.name || 'api',
    version: apiPackage.version || 'unknown',
    provider: currentProvider,
    timestamp: new Date().toISOString()
  });
});

// Enhanced health endpoint with comprehensive system status
app.get('/health', async (req, res) => {
  try {
    const calls = await db.getCallsWithTranscripts(1);
    const webhookHealth = await webhookService.healthCheck();
    const callStats = webhookService.getCallStatusStats();
    const notificationMetrics = await db.getNotificationAnalytics(1);
    
    // Check service health logs
    const recentHealthLogs = await new Promise((resolve, reject) => {
      db.db.all(`
        SELECT service_name, status, COUNT(*) as count
        FROM service_health_logs 
        WHERE timestamp >= datetime('now', '-1 hour')
        GROUP BY service_name, status
        ORDER BY service_name
      `, (err, rows) => {
        if (err) reject(err);
        else resolve(rows || []);
      });
    });
    
    res.json({ 
      status: 'healthy', 
      timestamp: new Date().toISOString(),
      enhanced_features: true,
      services: {
        database: {
          connected: true,
          recent_calls: calls.length
        },
        webhook_service: webhookHealth,
        call_tracking: callStats,
        notification_system: {
          total_today: notificationMetrics.total_notifications,
          success_rate: notificationMetrics.overall_success_rate + '%',
          avg_delivery_time: notificationMetrics.breakdown.length > 0 ? 
            notificationMetrics.breakdown[0].avg_delivery_time + 'ms' : 'N/A'
        }
      },
      active_calls: callConfigurations.size,
      adaptation_engine: {
        available_scripts: functionEngine ? functionEngine.getBusinessAnalysis().availableTemplates.length : 0,
        active_function_systems: callFunctionSystems.size
      },
      system_health: recentHealthLogs
    });
  } catch (error) {
    console.error('Enhanced health check error:', error);
    res.status(500).json({
      status: 'unhealthy',
      timestamp: new Date().toISOString(),
      enhanced_features: true,
      error: error.message,
      services: {
        database: {
          connected: false,
          error: error.message
        },
        webhook_service: {
          status: 'error',
          reason: 'Database connection failed'
        }
      }
    });
  }
});

// Enhanced system maintenance endpoint
app.post('/api/system/cleanup', async (req, res) => {
  try {
    const { days_to_keep = 30 } = req.body;
    
    console.log(`Starting enhanced system cleanup (keeping ${days_to_keep} days)...`);
    
    const cleanedRecords = await db.cleanupOldRecords(days_to_keep);
    
    // Log cleanup operation
    await db.logServiceHealth('system_maintenance', 'cleanup_completed', {
      records_cleaned: cleanedRecords,
      days_kept: days_to_keep
    });
    
    res.json({
      success: true,
      records_cleaned: cleanedRecords,
      days_kept: days_to_keep,
      timestamp: new Date().toISOString(),
      enhanced: true
    });
    
  } catch (error) {
    console.error('Error during enhanced system cleanup:', error);
    
    await db.logServiceHealth('system_maintenance', 'cleanup_failed', {
      error: error.message
    });
    
    res.status(500).json({
      success: false,
      error: 'System cleanup failed',
      details: error.message
    });
  }
});

// Basic calls list endpoint
app.get('/api/calls', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 10, 50); // Max 50 calls
    const offset = parseInt(req.query.offset) || 0;
    
    console.log(`Fetching calls list: limit=${limit}, offset=${offset}`);
    
    // Get calls from database using the new method
    const calls = await db.getRecentCalls(limit, offset);
    const totalCount = await db.getCallsCount();

    // Format the response with enhanced data
    const formattedCalls = calls.map(call => {
      const normalized = normalizeCallRecordForApi(call);
      return {
      ...normalized,
      transcript_count: call.transcript_count || 0,
      created_date: new Date(call.created_at).toLocaleDateString(),
      duration_formatted: call.duration ? 
        `${Math.floor(call.duration/60)}:${String(call.duration%60).padStart(2,'0')}` : 
        'N/A',
      // Parse JSON fields safely
      business_context: call.business_context ? 
        (() => { try { return JSON.parse(call.business_context); } catch { return null; } })() : 
        null,
      generated_functions: call.generated_functions ?
        (() => { try { return JSON.parse(call.generated_functions); } catch { return []; } })() :
        []
      };
    });

    res.json({
      success: true,
      calls: formattedCalls,
      pagination: {
        total: totalCount,
        limit: limit,
        offset: offset,
        has_more: offset + limit < totalCount
      },
      enhanced_features: true
    });

  } catch (error) {
    console.error('Error fetching calls list:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch calls list',
      details: error.message
    });
  }
});

// Enhanced calls list endpoint with filters
app.get('/api/calls/list', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 10, 50);
    const offset = parseInt(req.query.offset) || 0;
    const status = req.query.status; // Filter by status
    const phone = req.query.phone; // Filter by phone number
    const dateFrom = req.query.date_from; // Filter by date range
    const dateTo = req.query.date_to;

    let whereClause = '';
    let queryParams = [];
    
    // Build dynamic where clause
    const conditions = [];
    
    if (status) {
      conditions.push('c.status = ?');
      queryParams.push(status);
    }
    
    if (phone) {
      conditions.push('c.phone_number LIKE ?');
      queryParams.push(`%${phone}%`);
    }
    
    if (dateFrom) {
      conditions.push('c.created_at >= ?');
      queryParams.push(dateFrom);
    }
    
    if (dateTo) {
      conditions.push('c.created_at <= ?');
      queryParams.push(dateTo);
    }
    
    if (conditions.length > 0) {
      whereClause = 'WHERE ' + conditions.join(' AND ');
    }

    const query = `
      SELECT 
        c.*,
        COUNT(t.id) as transcript_count,
        GROUP_CONCAT(DISTINCT t.speaker) as speakers,
        MIN(t.timestamp) as conversation_start,
        MAX(t.timestamp) as conversation_end
      FROM calls c
      LEFT JOIN transcripts t ON c.call_sid = t.call_sid
      ${whereClause}
      GROUP BY c.call_sid
      ORDER BY c.created_at DESC
      LIMIT ? OFFSET ?
    `;

    queryParams.push(limit, offset);
    
    const calls = await new Promise((resolve, reject) => {
      db.db.all(query, queryParams, (err, rows) => {
        if (err) {
          console.error('Database error in enhanced calls query:', err);
          reject(err);
        } else {
          resolve(rows || []);
        }
      });
    });

    // Get filtered count
    const countQuery = `SELECT COUNT(*) as count FROM calls c ${whereClause}`;
    const totalCount = await new Promise((resolve, reject) => {
      db.db.get(countQuery, queryParams.slice(0, -2), (err, row) => {
        if (err) {
          console.error('Database error counting filtered calls:', err);
          resolve(0);
        } else {
          resolve(row?.count || 0);
        }
      });
    });

    // Enhanced formatting
    const enhancedCalls = calls.map(call => {
      const hasConversation = call.speakers && call.speakers.includes('user') && call.speakers.includes('ai');
      const conversationDuration = call.conversation_start && call.conversation_end ?
        Math.round((new Date(call.conversation_end) - new Date(call.conversation_start)) / 1000) : 0;

      return {
        call_sid: call.call_sid,
        phone_number: call.phone_number,
        status: call.status,
        twilio_status: call.twilio_status,
        created_at: call.created_at,
        started_at: call.started_at,
        ended_at: call.ended_at,
        duration: call.duration,
        transcript_count: call.transcript_count || 0,
        has_conversation: hasConversation,
        conversation_duration: conversationDuration,
        call_summary: call.call_summary,
        user_chat_id: call.user_chat_id,
        // Enhanced metadata
        business_context: call.business_context ? 
          (() => { try { return JSON.parse(call.business_context); } catch { return null; } })() : null,
        generated_functions_count: call.generated_functions ?
          (() => { try { return JSON.parse(call.generated_functions).length; } catch { return 0; } })() : 0,
        // Formatted fields
        created_date: new Date(call.created_at).toLocaleDateString(),
        created_time: new Date(call.created_at).toLocaleTimeString(),
        duration_formatted: call.duration ? 
          `${Math.floor(call.duration/60)}:${String(call.duration%60).padStart(2,'0')}` : 'N/A',
        status_icon: getStatusIcon(call.status),
        enhanced: true
      };
    });

    res.json({
      success: true,
      calls: enhancedCalls,
      filters: {
        status,
        phone,
        date_from: dateFrom,
        date_to: dateTo
      },
      pagination: {
        total: totalCount,
        limit: limit,
        offset: offset,
        has_more: offset + limit < totalCount,
        current_page: Math.floor(offset / limit) + 1,
        total_pages: Math.ceil(totalCount / limit)
      },
      enhanced_features: true
    });

  } catch (error) {
    console.error('Error in enhanced calls list:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch enhanced calls list',
      details: error.message
    });
  }
});

// Helper function for status icons
function getStatusIcon(status) {
  const icons = {
    'completed': '✅',
    'no-answer': '📶',
    'busy': '📞',
    'failed': '❌',
    'canceled': '🎫',
    'in-progress': '🔄',
    'ringing': '📲'
  };
  return icons[status] || '❓';
}

function normalizeCallRecordForApi(call) {
  if (!call || typeof call !== 'object') return call;
  const normalized = { ...call };
  return normalized;
}

// Add calls analytics endpoint
app.get('/api/calls/analytics', async (req, res) => {
  try {
    const days = parseInt(req.query.days) || 7;
    const dateFrom = new Date(Date.now() - (days * 24 * 60 * 60 * 1000)).toISOString();

    // Get comprehensive analytics
    const analytics = await new Promise((resolve, reject) => {
      const queries = {
        // Total calls in period
        totalCalls: `SELECT COUNT(*) as count FROM calls WHERE created_at >= ?`,
        
        // Calls by status
        statusBreakdown: `
          SELECT status, COUNT(*) as count 
          FROM calls 
          WHERE created_at >= ? 
          GROUP BY status 
          ORDER BY count DESC
        `,
        
        // Average call duration
        avgDuration: `
          SELECT AVG(duration) as avg_duration 
          FROM calls 
          WHERE created_at >= ? AND duration > 0
        `,
        
        // Success rate (completed calls with conversation)
        successRate: `
          SELECT 
            COUNT(CASE WHEN c.status = 'completed' AND t.transcript_count > 0 THEN 1 END) as successful,
            COUNT(*) as total
          FROM calls c
          LEFT JOIN (
            SELECT call_sid, COUNT(*) as transcript_count 
            FROM transcripts 
            WHERE speaker = 'user' 
            GROUP BY call_sid
          ) t ON c.call_sid = t.call_sid
          WHERE c.created_at >= ?
        `,
        
        // Daily call volume
        dailyVolume: `
          SELECT 
            DATE(created_at) as date,
            COUNT(*) as calls,
            COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed
          FROM calls 
          WHERE created_at >= ? 
          GROUP BY DATE(created_at) 
          ORDER BY date DESC
        `
      };

      const results = {};
      let completed = 0;
      const total = Object.keys(queries).length;

      for (const [key, query] of Object.entries(queries)) {
        db.db.all(query, [dateFrom], (err, rows) => {
          if (err) {
            console.error(`Analytics query error for ${key}:`, err);
            results[key] = null;
          } else {
            results[key] = rows;
          }
          
          completed++;
          if (completed === total) {
            resolve(results);
          }
        });
      }
    });

    // Process analytics data
    const processedAnalytics = {
      period: {
        days: days,
        from: dateFrom,
        to: new Date().toISOString()
      },
      summary: {
        total_calls: analytics.totalCalls?.[0]?.count || 0,
        average_duration: analytics.avgDuration?.[0]?.avg_duration ? 
          Math.round(analytics.avgDuration[0].avg_duration) : 0,
        success_rate: analytics.successRate?.[0] ? 
          Math.round((analytics.successRate[0].successful / analytics.successRate[0].total) * 100) : 0
      },
      status_breakdown: analytics.statusBreakdown || [],
      daily_volume: analytics.dailyVolume || [],
      enhanced_features: true
    };

    res.json(processedAnalytics);

  } catch (error) {
    console.error('Error fetching call analytics:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch analytics',
      details: error.message
    });
  }
});

// Search calls endpoint
app.get('/api/calls/search', async (req, res) => {
  try {
    const query = req.query.q;
    const limit = Math.min(parseInt(req.query.limit) || 20, 50);
    
    if (!query || query.length < 2) {
      return res.status(400).json({
        success: false,
        error: 'Search query must be at least 2 characters'
      });
    }

    // Search in calls and transcripts
    const searchResults = await new Promise((resolve, reject) => {
      const searchQuery = `
        SELECT DISTINCT
          c.*,
          COUNT(t.id) as transcript_count,
          GROUP_CONCAT(t.message, ' ') as conversation_text
        FROM calls c
        LEFT JOIN transcripts t ON c.call_sid = t.call_sid
        WHERE 
          c.phone_number LIKE ? OR
          c.call_summary LIKE ? OR
          c.prompt LIKE ? OR
          c.first_message LIKE ? OR
          t.message LIKE ?
        GROUP BY c.call_sid
        ORDER BY c.created_at DESC
        LIMIT ?
      `;
      
      const searchTerm = `%${query}%`;
      const params = [searchTerm, searchTerm, searchTerm, searchTerm, searchTerm, limit];
      
      db.db.all(searchQuery, params, (err, rows) => {
        if (err) {
          console.error('Search query error:', err);
          reject(err);
        } else {
          resolve(rows || []);
        }
      });
    });

    const formattedResults = searchResults.map(call => ({
      call_sid: call.call_sid,
      phone_number: call.phone_number,
      status: call.status,
      created_at: call.created_at,
      duration: call.duration,
      transcript_count: call.transcript_count || 0,
      call_summary: call.call_summary,
      // Highlight matching text (basic implementation)
      matching_text: call.conversation_text ?
        `${digitService ? digitService.maskOtpForExternal(call.conversation_text) : call.conversation_text}`.substring(0, 200) + '...' : null,
      created_date: new Date(call.created_at).toLocaleDateString(),
      duration_formatted: call.duration ? 
        `${Math.floor(call.duration/60)}:${String(call.duration%60).padStart(2,'0')}` : 'N/A'
    }));

    res.json({
      success: true,
      query: query,
      results: formattedResults,
      result_count: formattedResults.length,
      enhanced_search: true
    });

  } catch (error) {
    console.error('Error in call search:', error);
    res.status(500).json({
      success: false,
      error: 'Search failed',
      details: error.message
    });
  }
});

// SMS webhook endpoints
app.post('/webhook/sms', async (req, res) => {
    try {
        warnOnInvalidTwilioSignature(req, '/webhook/sms');
        const { From, Body, MessageSid, SmsStatus } = req.body;

        console.log(`SMS webhook: ${From} -> ${maskSmsBodyForLog(Body)}`);

        if (digitService?.handleIncomingSms) {
            const handled = await digitService.handleIncomingSms(From, Body);
            if (handled?.handled) {
                res.status(200).send('OK');
                return;
            }
        }

        // Handle incoming SMS with AI
        const result = await smsService.handleIncomingSMS(From, Body, MessageSid);

        // Save to database if needed
        if (db) {
            await db.saveSMSMessage({
                message_sid: MessageSid,
                from_number: From,
                body: Body,
                status: SmsStatus,
                direction: 'inbound',
                ai_response: result.ai_response,
                response_message_sid: result.message_sid
            });
        }

        res.status(200).send('OK');
    } catch (error) {
        console.error('SMS webhook error:', error);
        res.status(500).send('Error');
    }
});

app.post('/webhook/sms-status', async (req, res) => {
  try {
    warnOnInvalidTwilioSignature(req, '/webhook/sms-status');
        const { MessageSid, MessageStatus, ErrorCode, ErrorMessage } = req.body;

        console.log(`SMS status update: ${MessageSid} -> ${MessageStatus}`);

        if (db) {
            await db.updateSMSStatus(MessageSid, {
                status: MessageStatus,
                error_code: ErrorCode,
                error_message: ErrorMessage,
                updated_at: new Date()
            });
        }

        res.status(200).send('OK');
    } catch (error) {
        console.error('SMS status webhook error:', error);
        res.status(500).send('OK'); // Return OK to prevent retries
  }
});

app.get('/webhook/twilio-tts', (req, res) => {
  const key = String(req.query?.key || '').trim();
  if (!key) {
    res.status(400).send('Missing key');
    return;
  }
  const entry = twilioTtsCache.get(key);
  if (!entry || entry.expiresAt <= Date.now()) {
    twilioTtsCache.delete(key);
    res.status(404).send('Not found');
    return;
  }
  res.set('Cache-Control', `public, max-age=${Math.floor(TWILIO_TTS_CACHE_TTL_MS / 1000)}`);
  res.type(entry.contentType || 'audio/wav');
  res.send(entry.buffer);
});

// Email webhook endpoints
app.post('/webhook/email', async (req, res) => {
  try {
        if (!emailService) {
            return res.status(500).json({ success: false, error: 'Email service not initialized' });
        }
        const result = await emailService.handleProviderEvent(req.body || {});
        res.json({ success: true, ...result });
    } catch (error) {
        console.error('❌ Email webhook error:', error);
        res.status(500).json({ success: false, error: 'Email webhook processing failed', details: error.message });
    }
});

app.get('/webhook/email-unsubscribe', async (req, res) => {
    try {
        const email = String(req.query?.email || '').trim().toLowerCase();
        const messageId = String(req.query?.message_id || '').trim();
        if (!email) {
            return res.status(400).send('Missing email');
        }
        await db.setEmailSuppression(email, 'unsubscribe', 'link');
        if (messageId) {
            await db.addEmailEvent(messageId, 'complained', { reason: 'unsubscribe' });
            await db.updateEmailMessageStatus(messageId, {
                status: 'complained',
                failure_reason: 'unsubscribe',
                failed_at: new Date().toISOString()
            });
        }
        res.send('Unsubscribed');
    } catch (error) {
        console.error('❌ Email unsubscribe error:', error);
        res.status(500).send('Unsubscribe failed');
    }
});

const twilioGatherHandler = createTwilioGatherHandler({
  warnOnInvalidTwilioSignature,
  getDigitService: () => digitService,
  callConfigurations,
  config,
  VoiceResponse,
  webhookService,
  resolveHost,
  buildTwilioStreamTwiml,
  clearPendingDigitReprompts,
  callEndLocks,
  gatherEventDedupe,
  maskDigitsForLog,
  callEndMessages: CALL_END_MESSAGES,
  closingMessage: CLOSING_MESSAGE,
  queuePendingDigitAction,
  getTwilioTtsAudioUrl,
  ttsTimeoutMs: Number(config.twilio?.ttsMaxWaitMs) || 1200,
  shouldUseTwilioPlay,
  resolveTwilioSayVoice,
  isGroupedGatherPlan
});

// Twilio Gather fallback handler (DTMF)
app.post('/webhook/twilio-gather', twilioGatherHandler);

// Email API endpoints
app.post('/email/send', async (req, res) => {
    try {
        if (!emailService) {
            return res.status(500).json({ success: false, error: 'Email service not initialized' });
        }
        const idempotencyKey = req.headers['idempotency-key'] || req.headers['Idempotency-Key'];
        const result = await emailService.enqueueEmail(req.body || {}, { idempotencyKey });
        res.json({
            success: true,
            message_id: result.message_id,
            deduped: result.deduped || false,
            suppressed: result.suppressed || false
        });
    } catch (error) {
        const status = error.code === 'idempotency_conflict' ? 409 : 400;
        res.status(status).json({ success: false, error: error.message, missing: error.missing });
    }
});

app.post('/email/bulk', async (req, res) => {
    try {
        if (!emailService) {
            return res.status(500).json({ success: false, error: 'Email service not initialized' });
        }
        const idempotencyKey = req.headers['idempotency-key'] || req.headers['Idempotency-Key'];
        const result = await emailService.enqueueBulk(req.body || {}, { idempotencyKey });
        res.json({
            success: true,
            bulk_job_id: result.bulk_job_id,
            deduped: result.deduped || false
        });
    } catch (error) {
        const status = error.code === 'idempotency_conflict' ? 409 : 400;
        res.status(status).json({ success: false, error: error.message });
    }
});

app.post('/email/preview', async (req, res) => {
    try {
        if (!emailService) {
            return res.status(500).json({ success: false, error: 'Email service not initialized' });
        }
        const result = await emailService.previewScript(req.body || {});
        res.json({ success: result.ok, ...result });
    } catch (error) {
        res.status(400).json({ success: false, error: error.message });
    }
});

function normalizeEmailMessageForApi(message) {
    if (!message || typeof message !== 'object') return message;
    const normalized = { ...message };
    if ('template_id' in normalized) {
        normalized.script_id = normalized.template_id;
        delete normalized.template_id;
    }
    return normalized;
}

function normalizeEmailJobForApi(job) {
    if (!job || typeof job !== 'object') return job;
    const normalized = { ...job };
    if ('template_id' in normalized) {
        normalized.script_id = normalized.template_id;
        delete normalized.template_id;
    }
    return normalized;
}

app.get('/email/messages/:id', async (req, res) => {
    try {
        const messageId = req.params.id;
        const message = await db.getEmailMessage(messageId);
        if (!message) {
            return res.status(404).json({ success: false, error: 'Message not found' });
        }
        const events = await db.listEmailEvents(messageId);
        res.json({ success: true, message: normalizeEmailMessageForApi(message), events });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.get('/email/bulk/:jobId', async (req, res) => {
    try {
        const jobId = req.params.jobId;
        const job = await db.getEmailBulkJob(jobId);
        if (!job) {
            return res.status(404).json({ success: false, error: 'Bulk job not found' });
        }
        res.json({ success: true, job: normalizeEmailJobForApi(job) });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// Send single SMS endpoint
app.post('/api/sms/send', async (req, res) => {
    try {
        const {
            to,
            message,
            from,
            user_chat_id,
            options = {},
            idempotency_key,
            allow_quiet_hours,
            quiet_hours,
            media_url
        } = req.body;

        if (!to || !message) {
            return res.status(400).json({
                success: false,
                error: 'Phone number and message are required'
            });
        }

        // Validate phone number format
        if (!to.match(/^\+[1-9]\d{1,14}$/)) {
            return res.status(400).json({
                success: false,
                error: 'Invalid phone number format. Use E.164 format (e.g., +1234567890)'
            });
        }

        const smsOptions = { ...(options || {}) };
        if (idempotency_key && !smsOptions.idempotencyKey) {
            smsOptions.idempotencyKey = idempotency_key;
        }
        if (allow_quiet_hours === false) {
            smsOptions.allowQuietHours = false;
        }
        if (quiet_hours && !smsOptions.quietHours) {
            smsOptions.quietHours = quiet_hours;
        }
        if (media_url && !smsOptions.mediaUrl) {
            smsOptions.mediaUrl = media_url;
        }

        const result = await smsService.sendSMS(to, message, from, smsOptions);

        // Save to database
        if (db) {
            await db.saveSMSMessage({
                message_sid: result.message_sid,
                to_number: to,
                from_number: result.from,
                body: message,
                status: result.status,
                direction: 'outbound',
                user_chat_id: user_chat_id
            });

            // Create webhook notification
            if (user_chat_id) {
                await db.createEnhancedWebhookNotification(
                    result.message_sid,
                    'sms_sent',
                    user_chat_id
                );
            }
        }

        res.json({
            success: true,
            ...result
        });
    } catch (error) {
        console.error('❌ SMS send error:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to send SMS',
            details: error.message
        });
    }
});

// Send bulk SMS endpoint
app.post('/api/sms/bulk', async (req, res) => {
    try {
        const {
            recipients,
            message,
            options = {},
            user_chat_id,
            from,
            sms_options
        } = req.body;

        if (!recipients || !Array.isArray(recipients) || recipients.length === 0) {
            return res.status(400).json({
                success: false,
                error: 'Recipients array is required and must not be empty'
            });
        }

        if (!message) {
            return res.status(400).json({
                success: false,
                error: 'Message is required'
            });
        }

        if (recipients.length > 100) {
            return res.status(400).json({
                success: false,
                error: 'Maximum 100 recipients per bulk send'
            });
        }

        const bulkOptions = { ...(options || {}) };
        if (from && !bulkOptions.from) {
            bulkOptions.from = from;
        }
        if (sms_options && !bulkOptions.smsOptions) {
            bulkOptions.smsOptions = sms_options;
        }

        const result = await smsService.sendBulkSMS(recipients, message, bulkOptions);

        // Log bulk operation
        if (db) {
            await db.logBulkSMSOperation({
                total_recipients: result.total,
                successful: result.successful,
                failed: result.failed,
                message: message,
                user_chat_id: user_chat_id,
                timestamp: new Date()
            });
        }

        res.json({
            success: true,
            ...result
        });
    } catch (error) {
        console.error('❌ Bulk SMS error:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to send bulk SMS',
            details: error.message
        });
    }
});

// Schedule SMS endpoint
app.post('/api/sms/schedule', async (req, res) => {
    try {
        const { to, message, scheduled_time, options = {} } = req.body;

        if (!to || !message || !scheduled_time) {
            return res.status(400).json({
                success: false,
                error: 'Phone number, message, and scheduled_time are required'
            });
        }

        const scheduledDate = new Date(scheduled_time);
        if (scheduledDate <= new Date()) {
            return res.status(400).json({
                success: false,
                error: 'Scheduled time must be in the future'
            });
        }

        const result = await smsService.scheduleSMS(to, message, scheduled_time, options);

        res.json({
            success: true,
            ...result
        });
    } catch (error) {
        console.error('❌ SMS schedule error:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to schedule SMS',
            details: error.message
        });
    }
});

// SMS scripts endpoint
app.get('/api/sms/scripts', async (req, res) => {
    try {
        const { script_name, variables } = req.query;

        if (script_name) {
            try {
                const parsedVariables = variables ? JSON.parse(variables) : {};
                const script = smsService.getScript(script_name, parsedVariables);

                res.json({
                    success: true,
                    script_name,
                    script,
                    variables: parsedVariables
                });
            } catch (scriptError) {
                res.status(400).json({
                    success: false,
                    error: scriptError.message
                });
            }
        } else {
            // Return available scripts
            res.json({
                success: true,
                available_scripts: [
                    'welcome', 'appointment_reminder', 'verification', 'order_update',
                    'payment_reminder', 'promotional', 'customer_service', 'survey'
                ]
            });
        }
    } catch (error) {
        console.error('❌ SMS scripts error:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to get scripts'
        });
    }
});

// Get SMS messages from database for conversation view
app.get('/api/sms/messages/conversation/:phone', async (req, res) => {
    try {
        const { phone } = req.params;
        const limit = Math.min(parseInt(req.query.limit) || 50, 100);

        if (!phone) {
            return res.status(400).json({
                success: false,
                error: 'Phone number is required'
            });
        }

        const messages = await db.getSMSConversation(phone, limit);

        res.json({
            success: true,
            phone: phone,
            messages: messages,
            message_count: messages.length
        });

    } catch (error) {
        console.error('❌ Error fetching SMS conversation from database:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to fetch conversation',
            details: error.message
        });
    }
});

// Get recent SMS messages from database
app.get('/api/sms/messages/recent', async (req, res) => {
    try {
        const limit = Math.min(parseInt(req.query.limit) || 10, 50);
        const offset = parseInt(req.query.offset) || 0;

        const messages = await db.getSMSMessages(limit, offset);

        res.json({
            success: true,
            messages: messages,
            count: messages.length,
            limit: limit,
            offset: offset
        });

    } catch (error) {
        console.error('❌ Error fetching recent SMS messages:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to fetch recent messages',
            details: error.message
        });
    }
});

// Get SMS database statistics
app.get('/api/sms/database-stats', async (req, res) => {
    try {
        const hours = parseInt(req.query.hours) || 24;
        const dateFrom = new Date(Date.now() - (hours * 60 * 60 * 1000)).toISOString();

        // Get comprehensive SMS statistics from database
        const stats = await new Promise((resolve, reject) => {
            const queries = {
                // Total messages
                totalMessages: `SELECT COUNT(*) as count FROM sms_messages`,
                
                // Messages by direction
                messagesByDirection: `
                    SELECT direction, COUNT(*) as count 
                    FROM sms_messages 
                    GROUP BY direction
                `,
                
                // Messages by status
                messagesByStatus: `
                    SELECT status, COUNT(*) as count 
                    FROM sms_messages 
                    GROUP BY status
                    ORDER BY count DESC
                `,
                
                // Recent messages
                recentMessages: `
                    SELECT * FROM sms_messages 
                    WHERE created_at >= ?
                    ORDER BY created_at DESC 
                    LIMIT 5
                `,
                
                // Bulk operations
                bulkOperations: `SELECT COUNT(*) as count FROM bulk_sms_operations`,
                
                // Recent bulk operations
                recentBulkOps: `
                    SELECT * FROM bulk_sms_operations 
                    WHERE created_at >= ?
                    ORDER BY created_at DESC 
                    LIMIT 3
                `
            };

            const results = {};
            let completed = 0;
            const total = Object.keys(queries).length;

            for (const [key, query] of Object.entries(queries)) {
                const params = ['recentMessages', 'recentBulkOps'].includes(key) ? [dateFrom] : [];
                
                db.db.all(query, params, (err, rows) => {
                    if (err) {
                        console.error(`SMS stats query error for ${key}:`, err);
                        results[key] = key.includes('recent') ? [] : [{ count: 0 }];
                    } else {
                        results[key] = rows || [];
                    }
                    
                    completed++;
                    if (completed === total) {
                        resolve(results);
                    }
                });
            }
        });

        // Process the statistics
        const processedStats = {
            total_messages: stats.totalMessages[0]?.count || 0,
            sent_messages: stats.messagesByDirection.find(d => d.direction === 'outbound')?.count || 0,
            received_messages: stats.messagesByDirection.find(d => d.direction === 'inbound')?.count || 0,
            delivered_count: stats.messagesByStatus.find(s => s.status === 'delivered')?.count || 0,
            failed_count: stats.messagesByStatus.find(s => s.status === 'failed')?.count || 0,
            pending_count: stats.messagesByStatus.find(s => s.status === 'pending')?.count || 0,
            bulk_operations: stats.bulkOperations[0]?.count || 0,
            recent_messages: stats.recentMessages || [],
            recent_bulk_operations: stats.recentBulkOps || [],
            status_breakdown: stats.messagesByStatus || [],
            direction_breakdown: stats.messagesByDirection || [],
            time_period_hours: hours
        };

        // Calculate success rate
        const totalSent = processedStats.sent_messages;
        const delivered = processedStats.delivered_count;
        processedStats.success_rate = totalSent > 0 ? 
            Math.round((delivered / totalSent) * 100) : 0;

        res.json({
            success: true,
            ...processedStats
        });

    } catch (error) {
        console.error('❌ Error fetching SMS database statistics:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to fetch database statistics',
            details: error.message
        });
    }
});

// Get SMS status by message SID
app.get('/api/sms/status/:messageSid', async (req, res) => {
    try {
        const { messageSid } = req.params;

        const message = await new Promise((resolve, reject) => {
            db.db.get(
                `SELECT * FROM sms_messages WHERE message_sid = ?`,
                [messageSid],
                (err, row) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(row);
                    }
                }
            );
        });

        if (!message) {
            return res.status(404).json({
                success: false,
                error: 'Message not found'
            });
        }

        res.json({
            success: true,
            message: message
        });

    } catch (error) {
        console.error('❌ Error fetching SMS status:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to fetch message status',
            details: error.message
        });
    }
});

// Enhanced SMS scripts endpoint with better error handling
app.get('/api/sms/scripts/:scriptName?', async (req, res) => {
    try {
        const { scriptName } = req.params;
        const { variables } = req.query;

        // Built-in scripts (fallback)
        const builtInScripts = {
            welcome: 'Welcome to our service! We\'re excited to have you aboard. Reply HELP for assistance or STOP to unsubscribe.',
            appointment_reminder: 'Reminder: You have an appointment on {date} at {time}. Reply CONFIRM to confirm or RESCHEDULE to change.',
            verification: 'Your verification code is: {code}. This code will expire in 10 minutes. Do not share this code with anyone.',
            order_update: 'Order #{order_id} update: {status}. Track your order at {tracking_url}',
            payment_reminder: 'Payment reminder: Your payment of {amount} is due on {due_date}. Pay now: {payment_url}',
            promotional: '🎉 Special offer just for you! {offer_text} Use code {promo_code}. Valid until {expiry_date}. Reply STOP to opt out.',
            customer_service: 'Thanks for contacting us! We\'ve received your message and will respond within 24 hours. For urgent matters, call {phone}.',
            survey: 'How was your experience with us? Rate us 1-5 stars by replying with a number. Your feedback helps us improve!'
        };

        if (scriptName) {
            // Get specific script
            if (!builtInScripts[scriptName]) {
                return res.status(404).json({
                    success: false,
                    error: `Script '${scriptName}' not found`
                });
            }

            let script = builtInScripts[scriptName];
            let parsedVariables = {};

            // Parse and apply variables if provided
            if (variables) {
                try {
                    parsedVariables = JSON.parse(variables);
                    
                    // Replace variables in script
                    for (const [key, value] of Object.entries(parsedVariables)) {
                        script = script.replace(new RegExp(`{${key}}`, 'g'), value);
                    }
                } catch (parseError) {
                    console.error('Error parsing script variables:', parseError);
                    // Continue with script without variable substitution
                }
            }

            res.json({
                success: true,
                script_name: scriptName,
                script: script,
                original_script: builtInScripts[scriptName],
                variables: parsedVariables
            });

        } else {
            // Get list of available scripts
            res.json({
                success: true,
                available_scripts: Object.keys(builtInScripts),
                script_count: Object.keys(builtInScripts).length
            });
        }

    } catch (error) {
        console.error('❌ Error handling SMS scripts:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to process script request',
            details: error.message
        });
    }
});

// SMS webhook delivery status notifications (enhanced)
app.post('/webhook/sms-delivery', async (req, res) => {
    try {
        const { MessageSid, MessageStatus, ErrorCode, ErrorMessage, To, From } = req.body;

        console.log(`📱 SMS Delivery Status: ${MessageSid} -> ${MessageStatus}`);

        // Update message status in database
        if (db) {
            await db.updateSMSStatus(MessageSid, {
                status: MessageStatus,
                error_code: ErrorCode,
                error_message: ErrorMessage
            });

            // Get the original message to find user_chat_id for notification
            const message = await new Promise((resolve, reject) => {
                db.db.get(
                    `SELECT * FROM sms_messages WHERE message_sid = ?`,
                    [MessageSid],
                    (err, row) => {
                        if (err) reject(err);
                        else resolve(row);
                    }
                );
            });

            // Create webhook notification if user_chat_id exists
            if (message && message.user_chat_id) {
                const notificationType = MessageStatus === 'delivered' ? 'sms_delivered' :
                                       MessageStatus === 'failed' ? 'sms_failed' :
                                       `sms_${MessageStatus}`;

                await db.createEnhancedWebhookNotification(
                    MessageSid,
                    notificationType,
                    message.user_chat_id,
                    MessageStatus === 'failed' ? 'high' : 'normal'
                );

                console.log(`📨 Created ${notificationType} notification for user ${message.user_chat_id}`);
            }
        }

        res.status(200).send('OK');
    } catch (error) {
        console.error('❌ SMS delivery webhook error:', error);
        res.status(200).send('OK'); // Always return 200 to prevent retries
    }
});

// Get SMS statistics
app.get('/api/sms/stats', async (req, res) => {
  try {
    const stats = smsService.getStatistics();
    const activeConversations = smsService.getActiveConversations();
    
    res.json({
      success: true,
      statistics: stats,
      active_conversations: activeConversations.slice(0, 20), // Last 20 conversations
      sms_service_enabled: true
    });
    
  } catch (error) {
    console.error('❌ SMS stats error:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to get SMS statistics'
    });
  }
});

// Bulk SMS status endpoint
app.get('/api/sms/bulk/status', async (req, res) => {
    try {
        const limit = Math.min(parseInt(req.query.limit) || 10, 50);
        const hours = parseInt(req.query.hours) || 24;
        const dateFrom = new Date(Date.now() - (hours * 60 * 60 * 1000)).toISOString();

        const bulkOperations = await new Promise((resolve, reject) => {
            db.db.all(`
                SELECT * FROM bulk_sms_operations 
                WHERE created_at >= ?
                ORDER BY created_at DESC 
                LIMIT ?
            `, [dateFrom, limit], (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows || []);
                }
            });
        });

        // Get summary statistics
        const summary = bulkOperations.reduce((acc, op) => {
            acc.totalOperations += 1;
            acc.totalRecipients += op.total_recipients;
            acc.totalSuccessful += op.successful;
            acc.totalFailed += op.failed;
            return acc;
        }, {
            totalOperations: 0,
            totalRecipients: 0,
            totalSuccessful: 0,
            totalFailed: 0
        });

        summary.successRate = summary.totalRecipients > 0 ? 
            Math.round((summary.totalSuccessful / summary.totalRecipients) * 100) : 0;

        res.json({
            success: true,
            summary: summary,
            operations: bulkOperations,
            time_period_hours: hours
        });

    } catch (error) {
        console.error('❌ Error fetching bulk SMS status:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to fetch bulk SMS status',
            details: error.message
        });
    }
});

// SMS analytics dashboard endpoint
app.get('/api/sms/analytics', async (req, res) => {
    try {
        const days = parseInt(req.query.days) || 7;
        const dateFrom = new Date(Date.now() - (days * 24 * 60 * 60 * 1000)).toISOString();

        const analytics = await new Promise((resolve, reject) => {
            const queries = {
                // Daily message volume
                dailyVolume: `
                    SELECT 
                        DATE(created_at) as date,
                        COUNT(*) as total,
                        COUNT(CASE WHEN direction = 'outbound' THEN 1 END) as sent,
                        COUNT(CASE WHEN direction = 'inbound' THEN 1 END) as received,
                        COUNT(CASE WHEN status = 'delivered' THEN 1 END) as delivered,
                        COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed
                    FROM sms_messages 
                    WHERE created_at >= ?
                    GROUP BY DATE(created_at) 
                    ORDER BY date DESC
                `,
                
                // Hourly distribution
                hourlyDistribution: `
                    SELECT 
                        strftime('%H', created_at) as hour,
                        COUNT(*) as count
                    FROM sms_messages 
                    WHERE created_at >= ?
                    GROUP BY strftime('%H', created_at)
                    ORDER BY hour
                `,
                
                // Top phone numbers (anonymized)
                topNumbers: `
                    SELECT 
                        SUBSTR(COALESCE(to_number, from_number), 1, 6) || 'XXXX' as phone_prefix,
                        COUNT(*) as message_count
                    FROM sms_messages 
                    WHERE created_at >= ?
                    GROUP BY SUBSTR(COALESCE(to_number, from_number), 1, 6)
                    ORDER BY message_count DESC 
                    LIMIT 10
                `,
                
                // Error analysis
                errorAnalysis: `
                    SELECT 
                        error_code,
                        error_message,
                        COUNT(*) as count
                    FROM sms_messages 
                    WHERE created_at >= ? AND error_code IS NOT NULL
                    GROUP BY error_code, error_message
                    ORDER BY count DESC
                    LIMIT 10
                `
            };

            const results = {};
            let completed = 0;
            const total = Object.keys(queries).length;

            for (const [key, query] of Object.entries(queries)) {
                db.db.all(query, [dateFrom], (err, rows) => {
                    if (err) {
                        console.error(`SMS analytics query error for ${key}:`, err);
                        results[key] = [];
                    } else {
                        results[key] = rows || [];
                    }
                    
                    completed++;
                    if (completed === total) {
                        resolve(results);
                    }
                });
            }
        });

        // Calculate summary metrics
        const summary = {
            total_messages: 0,
            total_sent: 0,
            total_received: 0,
            total_delivered: 0,
            total_failed: 0,
            delivery_rate: 0,
            error_rate: 0
        };

        analytics.dailyVolume.forEach(day => {
            summary.total_messages += day.total;
            summary.total_sent += day.sent;
            summary.total_received += day.received;
            summary.total_delivered += day.delivered;
            summary.total_failed += day.failed;
        });

        if (summary.total_sent > 0) {
            summary.delivery_rate = Math.round((summary.total_delivered / summary.total_sent) * 100);
            summary.error_rate = Math.round((summary.total_failed / summary.total_sent) * 100);
        }

        res.json({
            success: true,
            period: {
                days: days,
                from: dateFrom,
                to: new Date().toISOString()
            },
            summary: summary,
            daily_volume: analytics.dailyVolume,
            hourly_distribution: analytics.hourlyDistribution,
            top_numbers: analytics.topNumbers,
            error_analysis: analytics.errorAnalysis,
            enhanced_analytics: true
        });

    } catch (error) {
        console.error('❌ Error fetching SMS analytics:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to fetch SMS analytics',
            details: error.message
        });
    }
});

// SMS search endpoint
app.get('/api/sms/search', async (req, res) => {
    try {
        const query = req.query.q;
        const limit = Math.min(parseInt(req.query.limit) || 20, 50);
        const direction = req.query.direction; // 'inbound', 'outbound', or null for all
        const status = req.query.status; // message status filter

        if (!query || query.length < 2) {
            return res.status(400).json({
                success: false,
                error: 'Search query must be at least 2 characters'
            });
        }

        let whereClause = `WHERE (body LIKE ? OR to_number LIKE ? OR from_number LIKE ?)`;
        let queryParams = [`%${query}%`, `%${query}%`, `%${query}%`];

        if (direction) {
            whereClause += ` AND direction = ?`;
            queryParams.push(direction);
        }

        if (status) {
            whereClause += ` AND status = ?`;
            queryParams.push(status);
        }

        queryParams.push(limit);

        const searchResults = await new Promise((resolve, reject) => {
            const searchQuery = `
                SELECT * FROM sms_messages 
                ${whereClause}
                ORDER BY created_at DESC
                LIMIT ?
            `;

            db.db.all(searchQuery, queryParams, (err, rows) => {
                if (err) {
                    console.error('SMS search query error:', err);
                    reject(err);
                } else {
                    resolve(rows || []);
                }
            });
        });

        // Format results for display
        const formattedResults = searchResults.map(msg => ({
            message_sid: msg.message_sid,
            phone: msg.to_number || msg.from_number,
            direction: msg.direction,
            status: msg.status,
            body: msg.body,
            created_at: msg.created_at,
            created_date: new Date(msg.created_at).toLocaleDateString(),
            created_time: new Date(msg.created_at).toLocaleTimeString(),
            // Highlight matching text (basic implementation)
            highlighted_body: msg.body.replace(
                new RegExp(query, 'gi'), 
                `**${query}**`
            ),
            error_info: msg.error_code ? {
                code: msg.error_code,
                message: msg.error_message
            } : null
        }));

        res.json({
            success: true,
            query: query,
            filters: { direction, status },
            results: formattedResults,
            result_count: formattedResults.length,
            enhanced_search: true
        });

    } catch (error) {
        console.error('❌ Error in SMS search:', error);
        res.status(500).json({
            success: false,
            error: 'Search failed',
            details: error.message
        });
    }
});

// Export SMS data endpoint
app.get('/api/sms/export', async (req, res) => {
    try {
        const format = req.query.format || 'json'; // 'json' or 'csv'
        const days = parseInt(req.query.days) || 30;
        const dateFrom = new Date(Date.now() - (days * 24 * 60 * 60 * 1000)).toISOString();

        const messages = await new Promise((resolve, reject) => {
            db.db.all(`
                SELECT 
                    message_sid,
                    to_number,
                    from_number,
                    body,
                    status,
                    direction,
                    created_at,
                    updated_at,
                    error_code,
                    error_message,
                    ai_response
                FROM sms_messages 
                WHERE created_at >= ?
                ORDER BY created_at DESC
            `, [dateFrom], (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows || []);
                }
            });
        });

        if (format === 'csv') {
            // Generate CSV
            const csvHeaders = [
                'Message SID', 'To Number', 'From Number', 'Message Body', 
                'Status', 'Direction', 'Created At', 'Updated At', 
                'Error Code', 'Error Message', 'AI Response'
            ];

            let csvContent = csvHeaders.join(',') + '\n';
            
            messages.forEach(msg => {
                const row = [
                    msg.message_sid || '',
                    msg.to_number || '',
                    msg.from_number || '',
                    `"${(msg.body || '').replace(/"/g, '""')}"`, // Escape quotes
                    msg.status || '',
                    msg.direction || '',
                    msg.created_at || '',
                    msg.updated_at || '',
                    msg.error_code || '',
                    `"${(msg.error_message || '').replace(/"/g, '""')}"`,
                    `"${(msg.ai_response || '').replace(/"/g, '""')}"`
                ];
                csvContent += row.join(',') + '\n';
            });

            res.setHeader('Content-Type', 'text/csv');
            res.setHeader('Content-Disposition', `attachment; filename="sms-export-${new Date().toISOString().split('T')[0]}.csv"`);
            res.send(csvContent);

        } else {
            // Return JSON
            res.json({
                success: true,
                export_info: {
                    total_messages: messages.length,
                    date_range: {
                        from: dateFrom,
                        to: new Date().toISOString()
                    },
                    exported_at: new Date().toISOString()
                },
                messages: messages
            });
        }

    } catch (error) {
        console.error('❌ Error exporting SMS data:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to export SMS data',
            details: error.message
        });
    }
});

// SMS system health check
app.get('/api/sms/health', async (req, res) => {
    try {
        const health = {
            timestamp: new Date().toISOString(),
            status: 'healthy',
            services: {
                database: { status: 'unknown' },
                twilio: { status: 'unknown' },
                sms_service: { status: 'unknown' }
            },
            statistics: {
                active_conversations: 0,
                scheduled_messages: 0,
                recent_messages: 0
            }
        };

        // Check database connectivity
        try {
            const dbTest = await new Promise((resolve, reject) => {
                db.db.get('SELECT COUNT(*) as count FROM sms_messages LIMIT 1', (err, row) => {
                    if (err) reject(err);
                    else resolve(row);
                });
            });
            
            health.services.database.status = 'healthy';
            health.services.database.message_count = dbTest.count;
        } catch (dbError) {
            health.services.database.status = 'unhealthy';
            health.services.database.error = dbError.message;
            health.status = 'degraded';
        }

        // Check SMS service if available
        try {
            if (smsService) {
                const stats = smsService.getStatistics();
                health.services.sms_service.status = 'healthy';
                health.statistics.active_conversations = stats.active_conversations;
                health.statistics.scheduled_messages = stats.scheduled_messages;
            } else {
                health.services.sms_service.status = 'not_initialized';
            }
        } catch (smsError) {
            health.services.sms_service.status = 'unhealthy';
            health.services.sms_service.error = smsError.message;
        }

        // Check recent activity
        try {
            const recentCount = await new Promise((resolve, reject) => {
                const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();
                db.db.get(
                    'SELECT COUNT(*) as count FROM sms_messages WHERE created_at >= ?',
                    [oneHourAgo],
                    (err, row) => {
                        if (err) reject(err);
                        else resolve(row.count || 0);
                    }
                );
            });
            
            health.statistics.recent_messages = recentCount;
        } catch (recentError) {
            console.warn('Could not get recent message count:', recentError);
        }

        // Check Twilio connectivity (basic check)
        try {
            if (process.env.TWILIO_ACCOUNT_SID && process.env.TWILIO_AUTH_TOKEN) {
                health.services.twilio.status = 'configured';
                health.services.twilio.account_sid = process.env.TWILIO_ACCOUNT_SID.substring(0, 8) + '...';
            } else {
                health.services.twilio.status = 'not_configured';
                health.status = 'degraded';
            }
        } catch (twilioError) {
            health.services.twilio.status = 'error';
            health.services.twilio.error = twilioError.message;
        }

        res.json(health);

    } catch (error) {
        console.error('❌ SMS health check error:', error);
        res.status(500).json({
            timestamp: new Date().toISOString(),
            status: 'unhealthy',
            error: 'Health check failed',
            details: error.message
        });
    }
});

// Clean up old SMS conversations (manual trigger)
app.post('/api/sms/cleanup-conversations', async (req, res) => {
    try {
        if (!smsService) {
            return res.status(500).json({
                success: false,
                error: 'SMS service not initialized'
            });
        }

        const maxAgeHours = parseInt(req.body.max_age_hours) || 24;
        const cleaned = smsService.cleanupOldConversations(maxAgeHours);

        res.json({
            success: true,
            cleaned_count: cleaned,
            max_age_hours: maxAgeHours,
            timestamp: new Date().toISOString()
        });

    } catch (error) {
        console.error('❌ Error cleaning up SMS conversations:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to cleanup conversations',
            details: error.message
        });
    }
});

// Start scheduled message processor
setInterval(() => {
    smsService.processScheduledMessages().catch(error => {
        console.error('❌ Scheduled SMS processing error:', error);
    });
}, 60000); // Check every minute

// Start email queue processor
setInterval(() => {
    if (!emailService) {
        return;
    }
    emailService.processQueue({ limit: 10 }).catch(error => {
        console.error('❌ Email queue processing error:', error);
    });
}, config.email?.queueIntervalMs || 5000);

// Cleanup old conversations every hour
setInterval(() => {
    smsService.cleanupOldConversations(24); // Keep conversations for 24 hours
}, 60 * 60 * 1000);

startServer();

// Enhanced graceful shutdown with comprehensive cleanup
process.on('SIGINT', async () => {
  console.log('\n🛑 Shutting down enhanced adaptive system gracefully...');
  
  try {
    // Log shutdown start
    await db.logServiceHealth('system', 'shutdown_initiated', {
      active_calls: callConfigurations.size,
      tracked_calls: callFunctionSystems.size
    });
    
    // Stop services
    webhookService.stop();
    callConfigurations.clear();
    callFunctionSystems.clear();
    
    // Log successful shutdown
    await db.logServiceHealth('system', 'shutdown_completed', {
      timestamp: new Date().toISOString()
    });
    
    await db.close();
    console.log('✅ Enhanced adaptive system shutdown complete');
  } catch (shutdownError) {
    console.error('❌ Error during shutdown:', shutdownError);
  }
  
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\nShutting down enhanced adaptive system gracefully...');
  
  try {
    // Log shutdown start
    await db.logServiceHealth('system', 'shutdown_initiated', {
      active_calls: callConfigurations.size,
      tracked_calls: callFunctionSystems.size,
      reason: 'SIGTERM'
    });
    
    // Stop services
    webhookService.stop();
    callConfigurations.clear();
    callFunctionSystems.clear();
    
    // Log successful shutdown
    await db.logServiceHealth('system', 'shutdown_completed', {
      timestamp: new Date().toISOString()
    });
    
    await db.close();
    console.log('Enhanced adaptive system shutdown complete');
  } catch (shutdownError) {
    console.error('Error during shutdown:', shutdownError);
  }
  
  process.exit(0);
});
