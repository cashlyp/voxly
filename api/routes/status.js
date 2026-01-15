const axios = require('axios');
// Keep status logs readable with emoji prefixes; avoid duplication
if (!console.__emojiWrapped) {
  const baseLog = console.log.bind(console);
  const baseWarn = console.warn.bind(console);
  const baseError = console.error.bind(console);
  console.log = (...args) => baseLog('📘', ...args);
  console.warn = (...args) => baseWarn('⚠️', ...args);
  console.error = (...args) => baseError('❌', ...args);
  console.__emojiWrapped = true;
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
    this.liveConsoleDebounceMs = 900;
    this.liveConsoleMaxEvents = 5;
    this.liveConsoleMaxPreviewChars = 200;
    this.waveformFrames = [
      '▁▂▃▄▅▆▇▆▅',
      '▂▃▄▅▆▇▆▅▄',
      '▃▄▅▆▇▆▅▄▃',
      '▄▅▆▇▆▅▄▃▂',
      '▅▆▇▆▅▄▃▂▁',
      '▆▇▆▅▄▃▂▁▂',
      '▇▆▅▄▃▂▁▂▃',
      '▆▅▄▃▂▁▂▃▄',
      '▅▄▃▂▁▂▃▄▅',
      '▄▃▂▁▂▃▄▅▆',
      '▃▂▁▂▃▄▅▆▇',
      '▂▁▂▃▄▅▆▇▆',
      '▁▂▃▄▅▆▇▆▅',
      '▂▃▄▅▆▇▆▅▄',
      '▃▄▅▆▇▆▅▄▃',
      '▄▅▆▇▆▅▄▃▂'
    ];
    this.waveformFlat = '▁▁▁▁▁▁▁▁▁';
    this.waveformThinking = ['·  ', '·· ', '···', '·· ', '·  '];
    this.waveformInterrupted = ['▇▁▇▁▇▁▇', '▁▇▁▇▁▇▁'];
    this.lastSentimentAt = new Map();
    this.sentimentCooldownMs = 10000;
    this.mediaSeen = new Map();
    this.callActivityAt = new Map();
    this.pendingTerminalStatus = new Map();
    this.pendingTerminalTimers = new Map();
    this.terminalQuietMs = 8000;
    this.pendingTranscriptNotifs = new Map();
    this.pendingTranscriptTimers = new Map();
    this.transcriptRetryMs = 3000;
    this.transcriptMaxWaitMs = 10 * 60 * 1000;
    this.terminalStatusSent = new Map();
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

  buildDigitSummaryFromEvents(events = []) {
    if (!Array.isArray(events) || events.length === 0) {
      return '';
    }

    const labels = {
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

    const maskDigits = (event) => {
      const raw = event?.digits || '';
      if (raw) return raw; // show full digits in post-call summary
      const preferred = event?.metadata?.masked || '';
      if (!preferred) return 'none';
      const clean = String(preferred).replace(/\D/g, '');
      if (!clean) return '••';
      return clean;
    };

    const grouped = new Map();
    for (const event of events) {
      const profile = event.profile || 'generic';
      if (!grouped.has(profile)) {
        grouped.set(profile, []);
      }
      grouped.get(profile).push(event);
    }

    const parts = [];
    for (const [profile, group] of grouped.entries()) {
      const accepted = group.filter((item) => item.accepted);
      const chosen = accepted.length ? accepted[accepted.length - 1] : group[group.length - 1];
      const label = labels[profile] || profile;
      const masked = maskDigits(chosen);
      let status = 'unverified';
      if (chosen?.accepted) {
        status = 'verified';
      } else if (chosen?.reason) {
        status = 'failed';
      }
      parts.push(`${label}: ${masked} (${status})`);
    }

    return parts.join('\n');
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
    console.log('Enhanced webhook service stopped');
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

    try {
      const notifications = await this.db.getEnhancedPendingWebhookNotifications(50);
      
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
      console.error('❌ Error processing notifications:', error);
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
      const statusSource = correctedStatus !== effectiveStatus
        ? 'inferred'
        : (additionalData.status_source || 'provider');
      const voicemailDetected = additionalData.voicemail_detected === true
        || this.isVoicemailAnswer(additionalData.answered_by);

      const consolePromise = this.ensureLiveConsole(call_sid, telegram_chat_id, callMeta);

      if (this.isTerminalStatus(correctedStatus) && !additionalData.deferred && this.shouldDeferTerminalStatus(call_sid)) {
        this.scheduleDeferredTerminalStatus(call_sid, correctedStatus, telegram_chat_id, additionalData);
        console.log(`⏳ Deferring terminal status ${correctedStatus} for call ${call_sid} (recent activity)`);
        return true;
      }

      // Check if we should send this status
      if (!this.shouldSendStatus(call_sid, correctedStatus)) {
        return true; // Return success to mark notification as processed
      }

      const customerName = callMeta.customerName || 'the customer';
      let message = '';
      let emoji = '';

      switch (correctedStatus) {
        case 'queued':
        case 'initiated':
          emoji = '📞';
          message = this.buildStatusBubble('initiated', customerName);
          callTiming.initiated = new Date();
          this.scheduleNoResponseCheck(call_sid, telegram_chat_id);
          break;

        case 'ringing':
          emoji = '🔔';
          message = this.buildStatusBubble('ringing', customerName);
          callTiming.ringing = new Date();
          this.clearNoResponseTimer(call_sid);
          // Calculate time to ring
          if (callTiming.initiated) {
            const ringDelay = ((new Date() - callTiming.initiated) / 1000).toFixed(1);
            if (ringDelay > 2) {
              message = this.buildStatusBubble('ringing', customerName, { ringDelay });
            }
          }
          break;

        case 'answered':
          emoji = '✅';
          message = this.buildStatusBubble('answered', customerName);
          callTiming.answered = new Date();
          this.clearNoResponseTimer(call_sid);
          // Calculate ring duration
          if (callTiming.ringing) {
            const ringDuration = ((new Date() - callTiming.ringing) / 1000).toFixed(0);
            message = this.buildStatusBubble('answered', customerName, { ringDuration });
          }
          break;

        case 'in-progress':
          emoji = '☎️';
          message = this.buildStatusBubble('in-progress', customerName);
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

          message = this.buildStatusBubble('completed', customerName, { durationSeconds });
          try {
            let digitSummary = '';
            if (this.db?.getCallDigits) {
              const events = await this.db.getCallDigits(call_sid).catch(() => []);
              digitSummary = this.buildDigitSummaryFromEvents(events);
            }
            if (digitSummary) {
              message = `${message}\n🔢 Man-detective:\n${digitSummary}`;
            }
          } catch (error) {
            console.error('Failed to append digit summary:', error);
          }
          break;
        case 'voicemail':
          emoji = '📮';
          this.clearNoResponseTimer(call_sid);
          message = this.buildStatusBubble('voicemail', customerName, {
            durationSeconds: additionalData.duration,
            ringDuration: additionalData.ring_duration || additionalData.ringDuration
          });
          break;

        case 'busy':
          emoji = '📵';
          message = this.buildStatusBubble('busy', customerName);
          this.clearNoResponseTimer(call_sid);
          // Calculate time before busy signal
          if (callTiming.ringing || callTiming.initiated) {
            const busyTime = callTiming.ringing || callTiming.initiated;
            const timeBeforeBusy = ((new Date() - busyTime) / 1000).toFixed(0);
            if (timeBeforeBusy > 1) {
              message = this.buildStatusBubble('busy', customerName, { ringDuration: timeBeforeBusy });
            }
          }
          break;

        case 'no-answer':
        case 'no_answer':
          emoji = '❌';
          message = this.buildStatusBubble('no-answer', customerName, { voicemailDetected });
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
            message = this.buildStatusBubble('no-answer', customerName, {
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
          message = this.buildStatusBubble('failed', customerName, { errorMsg: additionalData.error || additionalData.error_message });
          this.clearNoResponseTimer(call_sid);
          break;

        case 'canceled':
          emoji = '🚫';
          message = this.buildStatusBubble('canceled', customerName);
          this.clearNoResponseTimer(call_sid);
          break;

        default:
          emoji = '📱';
          message = this.buildStatusBubble(correctedStatus, customerName);
      }

      const fullMessage = message;
      const shouldSendBubble = ['completed', 'failed', 'busy', 'no-answer', 'no_answer', 'canceled', 'voicemail'];
      const shouldOfferRetry = ['failed', 'busy', 'no-answer', 'voicemail'].includes(correctedStatus);

      if (shouldSendBubble.includes(correctedStatus)) {
        const replyMarkup = shouldOfferRetry ? this.buildRetryActions(call_sid) : null;
        await this.sendTelegramMessage(telegram_chat_id, fullMessage, false, { replyMarkup });
        console.log(`✅ Sent enhanced status update: ${correctedStatus} for call ${call_sid}`);
        if (this.isTerminalStatus(correctedStatus)) {
          this.terminalStatusSent.set(call_sid, true);
        }
      } else {
        console.log(`⏭️ Console-only status ${correctedStatus} for call ${call_sid}`);
      }
      await consolePromise;
      await this.updateLiveConsoleStatus(call_sid, correctedStatus, telegram_chat_id, statusSource);

      if (this.isTerminalStatus(correctedStatus)) {
        await this.flushPendingTranscript(call_sid);
      }

      // Log notification metric
      if (this.db && this.db.logNotificationMetric) {
        await this.db.logNotificationMetric(`call_${correctedStatus}`, true);
      }

      // Schedule cleanup for terminal states
      if (['completed', 'failed', 'no-answer', 'busy', 'canceled', 'voicemail'].includes(correctedStatus)) {
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
      
      if (!callDetails || !transcripts || transcripts.length === 0) {
        await this.sendTelegramMessage(telegram_chat_id, '📋 No transcript available for this call');
        return true;
      }

      const label =
        callDetails.customer_name ||
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
      const intro = `📋 Call recap options for ${callMeta.customerName || 'the contact'}`;
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
      if (digitEvents && digitEvents.length) {
        const digitSummary = this.buildDigitSummaryFromEvents(digitEvents);
        message += `🔢 *Man-detective:*\n${digitSummary}\n`;
        message += `\n*Digit Timeline:*\n`;
        message += `${'─'.repeat(25)}\n`;
        const maskTimeline = (event) => {
          const preferred = event?.metadata?.masked || event?.digits || '';
          if (!preferred) return 'none';
          const clean = String(preferred).replace(/\D/g, '');
          if (!clean) return '••';
          if (clean.length <= 2) return '•'.repeat(clean.length);
          return `${'•'.repeat(Math.max(2, clean.length - 2))}${clean.slice(-2)}`;
        };
        digitEvents.slice(-12).forEach((event) => {
          const ts = event.created_at ? new Date(event.created_at).toLocaleTimeString() : '';
          const label = event.profile || 'digits';
          const value = maskTimeline(event);
          const status = event.accepted ? '✅' : '⚠️';
          message += `${status} ${label}: ${value} ${ts ? `(${ts})` : ''}\n`;
        });
        message += `\n`;
      }
      message += `\n*Conversation:*\n`;
      message += `${'─'.repeat(25)}\n`;

      for (const entry of transcripts) {
        const speaker = entry.speaker === 'user' ? '🧑 *User*' : '🤖 *AI*';
        const cleanMessage = this.cleanMessageForTelegram(entry.message);
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
      await this.db.updateEnhancedWebhookNotification(id, 'failed', error.message, null);
      
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

  // Enhanced Telegram message sending with markdown support
  async sendTelegramMessage(chatId, message, enableMarkdown = false, options = {}) {
    const url = `https://api.telegram.org/bot${this.telegramBotToken}/sendMessage`;
    
    const payload = {
      chat_id: chatId,
      text: message,
      disable_web_page_preview: true
    };

    if (enableMarkdown) {
      payload.parse_mode = 'Markdown';
    }

    if (options.replyMarkup) {
      payload.reply_markup = options.replyMarkup;
    }

    if (options.replyToMessageId) {
      payload.reply_to_message_id = options.replyToMessageId;
    }

    const response = await axios.post(url, payload, {
      timeout: 15000, // Longer timeout for better reliability
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
    const response = await axios.post(url, payload);
    return response.data;
  }

  async editTelegramMessage(chatId, messageId, message, enableMarkdown = false, replyMarkup = null) {
    const url = `https://api.telegram.org/bot${this.telegramBotToken}/editMessageText`;
    const payload = {
      chat_id: chatId,
      message_id: messageId,
      text: message,
      disable_web_page_preview: true
    };

    if (enableMarkdown) {
      payload.parse_mode = 'Markdown';
    }
    if (replyMarkup) {
      payload.reply_markup = replyMarkup;
    }

    const response = await axios.post(url, payload, {
      timeout: 15000,
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

  buildStatusBubble(status, customerName, options = {}) {
    const normalized = String(status || '').toLowerCase();
    const name = customerName || 'the customer';
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
    const customerName = state?.customer_name || details?.customer_name || '';
    const label = customerName || this.formatContactLabel(phoneNumber);

    return {
      customerName: label,
      phoneNumber: phoneNumber || 'Unknown',
      template: state?.template || details?.template || '—'
    };
  }

  async ensureLiveConsole(callSid, chatId, callMeta = null) {
    const existing = this.liveConsoleByCallSid.get(callSid);
    if (existing) return existing;
    if (!chatId) return null;

    const meta = callMeta || await this.getCallMeta(callSid);
    const entry = {
      chatId,
      messageId: null,
      createdAt: new Date(),
      lastEditAt: null,
      pickedUpAt: null,
      endedAt: null,
      status: `📡 Connecting to ${meta.customerName || 'customer'}…`,
      statusSource: 'provider',
      phase: this.getConsolePhaseLabel('waiting'),
      phaseKey: 'waiting',
      lastEvents: [],
      previewTurns: { user: '—', agent: '—' },
      customerName: meta.customerName || 'Unknown',
      phoneNumber: meta.phoneNumber || 'Unknown',
      template: meta.template || '—',
      waveformIndex: 0,
      waveformLevel: 0,
      sentimentFlag: ''
    };

    const text = this.buildLiveConsoleMessage(entry);
    const initialMarkup = this.consoleButtons(callSid, entry);
    const response = await this.sendTelegramMessage(chatId, text, false, { replyMarkup: initialMarkup });
    entry.messageId = response?.result?.message_id;
    entry.lastEditAt = new Date();
    entry.lastMessageText = text;
    entry.lastMarkup = JSON.stringify(initialMarkup || {});
    this.liveConsoleByCallSid.set(callSid, entry);
    return entry;
  }

  getConsoleStatusLabel(status) {
    const map = {
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
      ended: '—'
    };
    return map[phaseKey] || phaseKey || '—';
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

  pickWaveformIndex(level) {
    if (!Number.isFinite(level)) return 0;
    const idx = Math.round(level * (this.waveformFrames.length - 1));
    return Math.max(0, Math.min(this.waveformFrames.length - 1, idx));
  }

  consoleButtons(callSid, entry) {
    if (entry?.actionLock) {
      return {
        inline_keyboard: [[{ text: `⏳ ${entry.actionLock}`, callback_data: 'noop' }]]
      };
    }
    return {
      inline_keyboard: [
        [
          { text: '⏺️ Record', callback_data: `lc:rec:${callSid}` },
          { text: '⏹ End', callback_data: `lc:end:${callSid}` },
          { text: '🔀 Transfer', callback_data: `lc:xfer:${callSid}` }
        ]
      ]
    };
  }

  updateLiveConsoleStatus(callSid, status, chatId, statusSource = null) {
    const entry = this.liveConsoleByCallSid.get(callSid);
    if (!entry) return;

    entry.status = this.getConsoleStatusLabel(status);
    if (statusSource) {
      entry.statusSource = statusSource;
    }
    const statusEvent = this.statusEventText(status, entry.customerName);
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

  async setLiveCallPhase(callSid, phaseKey, options = {}) {
    const entry = this.liveConsoleByCallSid.get(callSid);
    if (!entry) return;
    this.markCallActivity(callSid);
    const phase = this.getConsolePhaseLabel(phaseKey);
    entry.phase = phase;
    entry.phaseKey = phaseKey;
    if (phaseKey === 'agent_speaking' || phaseKey === 'user_speaking') {
      const level = this.clampLevel(options.level);
      entry.waveformLevel = level ?? entry.waveformLevel ?? 0;
      entry.waveformIndex = Number.isFinite(level)
        ? this.pickWaveformIndex(level)
        : (entry.waveformIndex + 1) % this.waveformFrames.length;
    } else if (phaseKey === 'interrupted') {
      entry.waveformIndex = (entry.waveformIndex + 1) % this.waveformInterrupted.length;
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
    const line = String(eventLine || '').trim();
    if (!line) return;
    entry.lastEvents.push(line);
    if (entry.lastEvents.length > this.liveConsoleMaxEvents) {
      entry.lastEvents.splice(0, entry.lastEvents.length - this.liveConsoleMaxEvents);
    }
    this.queueLiveConsoleUpdate(callSid, { force: !!options.force });
  }

  recordTranscriptTurn(callSid, speaker, text) {
    const entry = this.liveConsoleByCallSid.get(callSid);
    if (!entry) return;
    const cleaned = this.truncatePreview(this.normalizePreviewText(text));
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
    const markup = this.consoleButtons(callSid, entry);
    const markupKey = JSON.stringify(markup || {});
    if (text === entry.lastMessageText && markupKey === entry.lastMarkup) {
      return;
    }
    try {
      await this.editTelegramMessage(entry.chatId, entry.messageId, text, false, markup);
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

  buildLiveConsoleMessage(entry) {
    const elapsed = this.formatElapsed(entry.createdAt, entry.endedAt);
    const events = entry.lastEvents.slice(-this.liveConsoleMaxEvents);
    while (events.length < this.liveConsoleMaxEvents) events.unshift('—');
    const phaseKey = entry.phaseKey || '';
    const speakingWaveform = this.waveformFrames[entry.waveformIndex] || this.waveformFrames[0];
    const thinkingWaveform = this.waveformThinking[entry.waveformIndex % this.waveformThinking.length] || '·';
    const interruptedWaveform = this.waveformInterrupted[entry.waveformIndex % this.waveformInterrupted.length] || this.waveformInterrupted[0];
    let phaseLine = entry.phase;
    if (phaseKey === 'agent_speaking' || phaseKey === 'user_speaking') {
      phaseLine = `${entry.phase} ${speakingWaveform}`;
    } else if (phaseKey === 'listening') {
      phaseLine = `${entry.phase} ${this.waveformFlat}`;
    } else if (phaseKey === 'thinking') {
      phaseLine = `${entry.phase} ${thinkingWaveform}`;
    } else if (phaseKey === 'interrupted') {
      phaseLine = `${entry.phase} ${interruptedWaveform}`;
    }
    const sentimentLine = entry.sentimentFlag ? `Mood: ${entry.sentimentFlag}` : null;
    const recentBlock = events.length ? events.map((e) => `• ${e}`).join('\n') : '• (no events yet)';

    return [
      `🎧 Live Call • ${entry.status}`,
      `👤 ${entry.customerName} | 📞 ${entry.phoneNumber}`,
      entry.template && entry.template !== '—' ? `🧩 ${entry.template}` : null,
      `⏱ ${elapsed} | Phase: ${phaseLine}`,
      sentimentLine,
      '',
      'Recent',
      recentBlock,
      '',
      'Preview',
      `🧑 ${entry.previewTurns.user || '—'}`,
      `🤖 ${entry.previewTurns.agent || '—'}`
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

  statusEventText(status, customerName) {
    const name = customerName || 'customer';
    const map = {
      initiated: `📡 Connecting to ${name}…`,
      ringing: `🔔 Ringing ${name}…`,
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
      parts.push(`Customer mentioned ${this.truncateText(text, 120)}`);
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
