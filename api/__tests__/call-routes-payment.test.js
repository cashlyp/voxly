const { registerCallRoutes } = require('../controllers/callRoutes');

function createAppStub() {
  const routes = {};
  return {
    routes,
    post(path, ...handlers) {
      routes[`POST ${path}`] = handlers[handlers.length - 1];
    },
    get(path, ...handlers) {
      routes[`GET ${path}`] = handlers[handlers.length - 1];
    },
  };
}

function createResMock() {
  const res = {
    statusCode: null,
    body: null,
  };
  res.status = jest.fn((code) => {
    res.statusCode = code;
    return res;
  });
  res.json = jest.fn((body) => {
    res.body = body;
    return res;
  });
  return res;
}

describe('Outbound call payment payload wiring', () => {
  test('forwards custom prompt/first_message and payment settings', async () => {
    const placeOutboundCall = jest.fn().mockResolvedValue({
      callId: 'CA100',
      callStatus: 'queued',
      provider: 'twilio',
      functionSystem: {
        context: { industry: 'support' },
        functions: [{ function: { name: 'start_payment' } }],
      },
    });
    const sendApiError = jest.fn();
    const app = createAppStub();

    registerCallRoutes(app, {
      requireOutboundAuthorization: jest.fn((req, res, next) => next && next()),
      sendApiError,
      resolveHost: jest.fn(() => 'api.example.test'),
      config: { server: { hostname: 'api.example.test' } },
      placeOutboundCall,
      buildErrorDetails: (error) => error?.message || null,
      getCurrentProvider: () => 'twilio',
      parseBoundedInteger: (value, { defaultValue }) => defaultValue,
      normalizeCallRecordForApi: (row) => row,
      getDb: () => null,
      getDigitService: () => null,
      getCallDirection: () => 'outbound',
    });

    const handler = app.routes['POST /outbound-call'];
    const req = {
      requestId: 'req_1',
      body: {
        number: '+15555550123',
        prompt: 'Custom payment-aware prompt',
        first_message: 'Hello from custom first message',
        user_chat_id: '999',
        script: 'custom',
        script_id: 42,
        payment_enabled: true,
        payment_connector: 'Pay Connector A',
        payment_amount: '49.99',
        payment_currency: 'usd',
        payment_description: 'Invoice #42',
        payment_start_message: undefined,
        payment_success_message: undefined,
        payment_failure_message: undefined,
        payment_retry_message: undefined,
      },
      headers: {},
    };
    const res = createResMock();

    await handler(req, res);

    expect(sendApiError).not.toHaveBeenCalled();
    expect(placeOutboundCall).toHaveBeenCalledWith(
      {
        number: '+15555550123',
        prompt: 'Custom payment-aware prompt',
        first_message: 'Hello from custom first message',
        user_chat_id: '999',
        customer_name: null,
        business_id: undefined,
        script: 'custom',
        script_id: 42,
        purpose: undefined,
        emotion: undefined,
        urgency: undefined,
        technical_level: undefined,
        voice_model: undefined,
        collection_profile: undefined,
        collection_expected_length: undefined,
        collection_timeout_s: undefined,
        collection_max_retries: undefined,
        collection_mask_for_gpt: undefined,
        collection_speak_confirmation: undefined,
        payment_enabled: true,
        payment_connector: 'Pay Connector A',
        payment_amount: '49.99',
        payment_currency: 'usd',
        payment_description: 'Invoice #42',
        payment_start_message: undefined,
        payment_success_message: undefined,
        payment_failure_message: undefined,
        payment_retry_message: undefined,
      },
      'api.example.test',
    );
    expect(res.json).toHaveBeenCalledWith(
      expect.objectContaining({
        success: true,
        call_sid: 'CA100',
        status: 'queued',
        provider: 'twilio',
        warnings: [],
      }),
    );
  });

  test('returns validation error when payment settings are provided without script_id', async () => {
    const paymentError = new Error('Payment settings require a valid script_id.');
    paymentError.code = 'payment_requires_script';
    paymentError.status = 400;

    const placeOutboundCall = jest.fn().mockRejectedValue(paymentError);
    const sendApiError = jest.fn();
    const app = createAppStub();

    registerCallRoutes(app, {
      requireOutboundAuthorization: jest.fn((req, res, next) => next && next()),
      sendApiError,
      resolveHost: jest.fn(() => 'api.example.test'),
      config: { server: { hostname: 'api.example.test' } },
      placeOutboundCall,
      buildErrorDetails: (error) => error?.message || null,
      getCurrentProvider: () => 'twilio',
      parseBoundedInteger: (value, { defaultValue }) => defaultValue,
      normalizeCallRecordForApi: (row) => row,
      getDb: () => null,
      getDigitService: () => null,
      getCallDirection: () => 'outbound',
    });

    const handler = app.routes['POST /outbound-call'];
    const req = {
      requestId: 'req_2',
      body: {
        number: '+15555550123',
        prompt: 'Prompt',
        first_message: 'Hello',
        payment_enabled: true,
      },
      headers: {},
    };
    const res = createResMock();

    await handler(req, res);

    expect(sendApiError).toHaveBeenCalledWith(
      res,
      400,
      'payment_requires_script',
      'Payment settings require a valid script_id.',
      'req_2',
      expect.any(Object),
    );
  });
});
