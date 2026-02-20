const { createDigitCollectionService } = require('../functions/Digit');

class FakeVoiceResponse {
  constructor() {
    this.actions = [];
  }

  say(message) {
    this.actions.push({ type: 'say', message });
    return this;
  }

  redirect(attrs, url) {
    this.actions.push({ type: 'redirect', url, attrs });
    return this;
  }

  hangup() {
    this.actions.push({ type: 'hangup' });
    return this;
  }

  pay(attrs = {}) {
    const entry = { type: 'pay', attrs, prompts: [] };
    this.actions.push(entry);
    return {
      prompt: ({ for: target }) => ({
        say: (message) => {
          entry.prompts.push({ for: target, message });
        },
      }),
    };
  }

  toString() {
    return JSON.stringify(this.actions);
  }
}

function buildService(overrides = {}) {
  const callConfigurations = overrides.callConfigurations || new Map();
  const db = {
    updateCallState: jest.fn().mockResolvedValue(undefined),
    getLatestCallState: jest.fn().mockResolvedValue(null),
    reserveProviderEventIdempotency: jest
      .fn()
      .mockResolvedValue({ reserved: true }),
    ...(overrides.db || {}),
  };

  const service = createDigitCollectionService({
    db,
    webhookService: {
      addLiveEvent: jest.fn(),
      ...(overrides.webhookService || {}),
    },
    callConfigurations,
    config: {
      server: { hostname: 'api.example.test' },
      twilio: { accountSid: 'AC123', authToken: 'token' },
      ...(overrides.config || {}),
    },
    twilioClient:
      overrides.twilioClient ||
      (() => ({
        calls: () => ({
          update: jest.fn().mockResolvedValue(undefined),
        }),
      })),
    VoiceResponse: FakeVoiceResponse,
    getCurrentProvider: () => 'twilio',
    speakAndEndCall: jest.fn(),
    clearSilenceTimer: jest.fn(),
    queuePendingDigitAction: jest.fn(),
    getTwilioTtsAudioUrl: jest.fn().mockResolvedValue('https://audio.test/prompt.mp3'),
    getPaymentFeatureConfig:
      overrides.getPaymentFeatureConfig ||
      (() => ({
        enabled: true,
        kill_switch: false,
        allow_twilio: true,
        require_script_opt_in: false,
        default_currency: 'USD',
        min_amount: 0,
        max_amount: 0,
        max_attempts_per_call: 3,
        retry_cooldown_ms: 0,
        webhook_idempotency_ttl_ms: 300000,
      })),
  });

  return { service, db, callConfigurations };
}

describe('Digit payment state + idempotency', () => {
  test('requestPhonePayment respects runtime kill switch', async () => {
    const callConfigurations = new Map();
    callConfigurations.set('CA100', {
      provider: 'twilio',
      payment_enabled: true,
      payment_amount: '19.99',
      payment_connector: 'ConnectorA',
      flow_state: 'normal',
    });

    const { service } = buildService({
      callConfigurations,
      getPaymentFeatureConfig: () => ({
        enabled: true,
        kill_switch: true,
        allow_twilio: true,
        require_script_opt_in: false,
        default_currency: 'USD',
        min_amount: 0,
        max_amount: 0,
        max_attempts_per_call: 3,
        retry_cooldown_ms: 0,
        webhook_idempotency_ttl_ms: 300000,
      }),
    });

    const result = await service.requestPhonePayment('CA100', {});

    expect(result).toMatchObject({
      error: 'payment_feature_disabled',
      reason: 'kill_switch',
    });
  });

  test('buildTwilioPaymentTwiml moves payment state to active', async () => {
    const callConfigurations = new Map();
    callConfigurations.set('CA200', {
      provider: 'twilio',
      payment_enabled: true,
      payment_in_progress: true,
      payment_state: 'requested',
      payment_session: {
        payment_id: 'pay_1',
        amount: '42.00',
        currency: 'USD',
        payment_connector: 'ConnectorB',
      },
    });

    const { service, db } = buildService({ callConfigurations });

    const result = await service.buildTwilioPaymentTwiml('CA200', {
      paymentId: 'pay_1',
      hostname: 'api.example.test',
    });

    expect(result.ok).toBe(true);
    expect(callConfigurations.get('CA200').payment_state).toBe('active');
    expect(db.updateCallState).toHaveBeenCalledWith(
      'CA200',
      'payment_session_started',
      expect.objectContaining({ payment_state: 'active' }),
    );
  });

  test('handleTwilioPaymentCompletion suppresses duplicate webhook side effects', async () => {
    const callConfigurations = new Map();
    callConfigurations.set('CA300', {
      provider: 'twilio',
      payment_enabled: true,
      payment_in_progress: true,
      payment_state: 'active',
      payment_session: {
        payment_id: 'pay_2',
        amount: '11.00',
        currency: 'USD',
        payment_connector: 'ConnectorC',
      },
    });

    const { service, db } = buildService({ callConfigurations });
    const payload = {
      Result: 'success',
      PaymentAmount: '11.00',
      PaymentCurrency: 'USD',
      PaymentConfirmationCode: 'ABC123',
    };

    const first = await service.handleTwilioPaymentCompletion('CA300', payload, {
      paymentId: 'pay_2',
      hostname: 'api.example.test',
    });
    const second = await service.handleTwilioPaymentCompletion('CA300', payload, {
      paymentId: 'pay_2',
      hostname: 'api.example.test',
    });

    expect(first.ok).toBe(true);
    expect(first.duplicate).toBeUndefined();
    expect(second).toMatchObject({ ok: true, duplicate: true, success: true });
    expect(
      db.updateCallState.mock.calls.filter(
        ([, stateName]) => stateName === 'payment_session_completed',
      ),
    ).toHaveLength(1);
    expect(callConfigurations.get('CA300').payment_state).toBe('completed');
    expect(callConfigurations.get('CA300').payment_in_progress).toBe(false);
  });

  test('requestPhonePayment enforces per-call attempt limits', async () => {
    const callConfigurations = new Map();
    callConfigurations.set('CA400', {
      provider: 'twilio',
      payment_enabled: true,
      payment_amount: '9.99',
      payment_connector: 'ConnectorX',
      payment_attempt_count: 2,
      flow_state: 'normal',
    });

    const { service } = buildService({
      callConfigurations,
      getPaymentFeatureConfig: () => ({
        enabled: true,
        kill_switch: false,
        allow_twilio: true,
        require_script_opt_in: false,
        default_currency: 'USD',
        min_amount: 0,
        max_amount: 0,
        max_attempts_per_call: 2,
        retry_cooldown_ms: 0,
        webhook_idempotency_ttl_ms: 300000,
      }),
    });

    const result = await service.requestPhonePayment('CA400', {});
    expect(result).toMatchObject({ error: 'payment_attempt_limit' });
  });

  test('reconcilePaymentSession closes stale active payment state', async () => {
    const callConfigurations = new Map();
    callConfigurations.set('CA500', {
      provider: 'twilio',
      payment_enabled: true,
      payment_in_progress: true,
      payment_state: 'active',
      flow_state: 'payment_active',
      payment_session: {
        payment_id: 'pay_5',
        amount: '15.00',
        currency: 'USD',
        payment_connector: 'ConnectorY',
      },
    });

    const { service, db } = buildService({ callConfigurations });
    const result = await service.reconcilePaymentSession('CA500', {
      reason: 'payment_reconcile_stale',
      source: 'test_worker',
    });

    expect(result).toMatchObject({ ok: true, reconciled: true, state: 'failed' });
    expect(callConfigurations.get('CA500').payment_state).toBe('failed');
    expect(callConfigurations.get('CA500').payment_in_progress).toBe(false);
    expect(db.updateCallState).toHaveBeenCalledWith(
      'CA500',
      'payment_session_reconciled',
      expect.objectContaining({
        payment_state: 'failed',
        reconcile_reason: 'payment_reconcile_stale',
      }),
    );
  });
});
