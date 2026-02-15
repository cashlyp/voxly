'use strict';

const twilio = require('twilio');
const { createDigitCollectionService } = require('../functions/Digit');

function createServiceHarness(overrides = {}) {
  const callConfigurations = new Map();
  const callSid = 'CA_TEST_DIGIT_001';
  callConfigurations.set(callSid, {
    provider: 'twilio',
    voice_model: 'aura-asteria-en',
    call_mode: 'dtmf_capture',
    digit_capture_active: true
  });

  const callsUpdate = jest.fn().mockResolvedValue({ sid: callSid });
  const twilioClient = jest.fn(() => ({
    calls: jest.fn(() => ({
      update: callsUpdate
    }))
  }));

  const service = createDigitCollectionService({
    db: {
      addCallDigitEvent: jest.fn().mockResolvedValue(),
      updateCallState: jest.fn().mockResolvedValue(),
      addCallMetric: jest.fn().mockResolvedValue(),
      logServiceHealth: jest.fn().mockResolvedValue(),
      addTranscript: jest.fn().mockResolvedValue()
    },
    webhookService: {
      addLiveEvent: jest.fn(),
      recordTranscriptTurn: jest.fn()
    },
    callConfigurations,
    config: {
      server: { hostname: 'example.test' },
      twilio: {
        accountSid: 'AC123',
        authToken: 'auth',
        gatherFallback: true
      },
      platform: { provider: 'twilio' }
    },
    twilioClient,
    VoiceResponse: twilio.twiml.VoiceResponse,
    getCurrentProvider: () => 'twilio',
    speakAndEndCall: jest.fn().mockResolvedValue(),
    clearSilenceTimer: jest.fn(),
    queuePendingDigitAction: jest.fn(),
    getTwilioTtsAudioUrl: overrides.getTwilioTtsAudioUrl || jest.fn().mockResolvedValue('https://example.test/audio/prompt.wav'),
    setCallFlowState: jest.fn(),
    callEndMessages: {
      no_response: 'No input received.',
      failure: 'We could not verify that input.'
    }
  });

  return {
    callSid,
    service,
    callsUpdate,
    getTwilioTtsAudioUrl: overrides.getTwilioTtsAudioUrl
  };
}

describe('digit gather transport and profile-driven validation', () => {
  test('sendTwilioGather returns false when Deepgram prompt URL is unavailable', async () => {
    const ttsMock = jest.fn().mockResolvedValue(null);
    const { callSid, service, callsUpdate } = createServiceHarness({
      getTwilioTtsAudioUrl: ttsMock
    });

    service.setExpectation(callSid, {
      profile: 'otp',
      prompt: 'Enter your code now.',
      min_digits: 6,
      max_digits: 6
    });

    const expectation = service.getExpectation(callSid);
    const sent = await service.sendTwilioGather(callSid, expectation, {
      prompt: 'Enter your code now.'
    });

    expect(sent).toBe(false);
    expect(callsUpdate).not.toHaveBeenCalled();
  });

  test('sendTwilioGather emits TwiML with <Play> and no <Say> for digit prompts', async () => {
    const ttsMock = jest.fn().mockResolvedValue('https://example.test/audio/prompt.wav');
    const { callSid, service, callsUpdate } = createServiceHarness({
      getTwilioTtsAudioUrl: ttsMock
    });

    service.setExpectation(callSid, {
      profile: 'otp',
      prompt: 'Enter your code now.',
      min_digits: 6,
      max_digits: 6
    });

    const expectation = service.getExpectation(callSid);
    const sent = await service.sendTwilioGather(callSid, expectation, {
      prompt: 'Enter your code now.'
    });

    expect(sent).toBe(true);
    expect(callsUpdate).toHaveBeenCalledTimes(1);
    const payload = callsUpdate.mock.calls[0][0] || {};
    expect(payload.twiml).toContain('<Play>https://example.test/audio/prompt.wav</Play>');
    expect(payload.twiml).not.toContain('<Say');
  });

  test('card_number validation and fallback are profile-driven', () => {
    const { callSid, service } = createServiceHarness();

    service.setExpectation(callSid, {
      profile: 'card_number',
      min_digits: 16,
      max_digits: 16,
      max_retries: 1
    });

    const invalid1 = service.recordDigits(callSid, '4111111111111112', {
      source: 'gather',
      full_input: true,
      timestamp: Date.now()
    });
    expect(invalid1.accepted).toBe(false);
    expect(invalid1.reason).toBe('invalid_card_number');
    expect(Boolean(invalid1.fallback)).toBe(false);

    const invalid2 = service.recordDigits(callSid, '4111111111111113', {
      source: 'gather',
      full_input: true,
      timestamp: Date.now() + 1
    });
    expect(invalid2.accepted).toBe(false);
    expect(invalid2.reason).toBe('invalid_card_number');
    expect(Boolean(invalid2.fallback)).toBe(true);

    service.setExpectation(callSid, {
      profile: 'card_number',
      min_digits: 16,
      max_digits: 16,
      max_retries: 1
    });

    const valid = service.recordDigits(callSid, '4111111111111111', {
      source: 'gather',
      full_input: true,
      timestamp: Date.now() + 2
    });
    expect(valid.accepted).toBe(true);
    expect(valid.reason).toBeNull();
  });
});
