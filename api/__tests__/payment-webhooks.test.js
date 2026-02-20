const { registerWebhookRoutes } = require('../controllers/webhookRoutes');

function createAppStub() {
  const routes = {};
  return {
    routes,
    get(path, handler) {
      routes[`GET ${path}`] = handler;
    },
    post(path, handler) {
      routes[`POST ${path}`] = handler;
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
  res.type = jest.fn(() => res);
  res.send = jest.fn((body) => {
    res.body = body;
    return res;
  });
  res.end = jest.fn((body) => {
    res.body = body;
    return res;
  });
  return res;
}

function setupHandlers({
  signatureOk = true,
  digitService = {},
} = {}) {
  const app = createAppStub();
  const requireValidTwilioSignature = jest.fn(() => signatureOk);
  const getDigitService = jest.fn(() => digitService);
  registerWebhookRoutes(app, {
    config: { server: { hostname: 'api.example.test' } },
    requireValidTwilioSignature,
    getDigitService,
    handleTwilioGatherWebhook: jest.fn(),
  });

  return {
    routes: app.routes,
    requireValidTwilioSignature,
    getDigitService,
  };
}

describe('Twilio pay webhook delegation', () => {
  test('start webhook delegates to digitService and returns TwiML', async () => {
    const buildTwilioPaymentTwiml = jest
      .fn()
      .mockResolvedValue({ ok: true, twiml: '<Response><Say>pay</Say></Response>' });
    const { routes, requireValidTwilioSignature } = setupHandlers({
      digitService: { buildTwilioPaymentTwiml },
    });
    const handler = routes['POST /webhook/twilio-pay/start'];

    const req = {
      body: { CallSid: 'CA123' },
      query: { paymentId: 'pay_1' },
      headers: {},
    };
    const res = createResMock();

    await handler(req, res);

    expect(requireValidTwilioSignature).toHaveBeenCalledWith(
      req,
      res,
      '/webhook/twilio-pay/start',
    );
    expect(buildTwilioPaymentTwiml).toHaveBeenCalledWith('CA123', {
      paymentId: 'pay_1',
      hostname: 'api.example.test',
    });
    expect(res.type).toHaveBeenCalledWith('text/xml');
    expect(res.end).toHaveBeenCalledWith('<Response><Say>pay</Say></Response>');
  });

  test('complete webhook delegates to digitService and returns TwiML', async () => {
    const handleTwilioPaymentCompletion = jest
      .fn()
      .mockResolvedValue({ ok: true, twiml: '<Response><Say>done</Say></Response>' });
    const { routes, requireValidTwilioSignature } = setupHandlers({
      digitService: { handleTwilioPaymentCompletion },
    });
    const handler = routes['POST /webhook/twilio-pay/complete'];

    const req = {
      body: { CallSid: 'CA456', Result: 'success' },
      query: { paymentId: 'pay_2' },
      headers: {},
    };
    const res = createResMock();

    await handler(req, res);

    expect(requireValidTwilioSignature).toHaveBeenCalledWith(
      req,
      res,
      '/webhook/twilio-pay/complete',
    );
    expect(handleTwilioPaymentCompletion).toHaveBeenCalledWith(
      'CA456',
      req.body,
      { paymentId: 'pay_2', hostname: 'api.example.test' },
    );
    expect(res.type).toHaveBeenCalledWith('text/xml');
    expect(res.end).toHaveBeenCalledWith('<Response><Say>done</Say></Response>');
  });

  test('status webhook delegates to digitService and returns OK', async () => {
    const handleTwilioPaymentStatus = jest.fn().mockResolvedValue({ ok: true });
    const { routes, requireValidTwilioSignature } = setupHandlers({
      digitService: { handleTwilioPaymentStatus },
    });
    const handler = routes['POST /webhook/twilio-pay/status'];

    const req = {
      body: { CallSid: 'CA789', PaymentEvent: 'payment-completed' },
      query: { paymentId: 'pay_3' },
      headers: {},
    };
    const res = createResMock();

    await handler(req, res);

    expect(requireValidTwilioSignature).toHaveBeenCalledWith(
      req,
      res,
      '/webhook/twilio-pay/status',
    );
    expect(handleTwilioPaymentStatus).toHaveBeenCalledWith('CA789', req.body, {
      paymentId: 'pay_3',
    });
    expect(res.status).toHaveBeenCalledWith(200);
    expect(res.send).toHaveBeenCalledWith('OK');
  });

  test('signature failure short-circuits start webhook', async () => {
    const buildTwilioPaymentTwiml = jest.fn();
    const { routes } = setupHandlers({
      signatureOk: false,
      digitService: { buildTwilioPaymentTwiml },
    });
    const handler = routes['POST /webhook/twilio-pay/start'];

    const req = {
      body: { CallSid: 'CA999' },
      query: { paymentId: 'pay_9' },
      headers: {},
    };
    const res = createResMock();

    await handler(req, res);

    expect(buildTwilioPaymentTwiml).not.toHaveBeenCalled();
    expect(res.send).not.toHaveBeenCalled();
    expect(res.end).not.toHaveBeenCalled();
  });
});
