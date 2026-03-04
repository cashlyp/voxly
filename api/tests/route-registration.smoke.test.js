'use strict';

const { registerCallRoutes } = require('../controllers/callRoutes');
const { registerStatusRoutes } = require('../controllers/statusRoutes');
const { registerWebhookRoutes } = require('../controllers/webhookRoutes');

function createMockApp() {
  const routes = [];
  const register = (method) => (path, ...handlers) => {
    routes.push({ method, path, handlersCount: handlers.length });
  };
  return {
    routes,
    get: register('GET'),
    post: register('POST'),
    put: register('PUT'),
    delete: register('DELETE'),
    patch: register('PATCH'),
  };
}

const noopMiddleware = (_req, _res, next) => {
  if (typeof next === 'function') next();
};

function buildCallRouteCtx() {
  return {
    requireOutboundAuthorization: noopMiddleware,
    sendApiError: jest.fn(),
    resolveHost: () => 'example.test',
    config: {},
    placeOutboundCall: jest.fn(),
    buildErrorDetails: jest.fn(),
    getCurrentProvider: () => 'twilio',
    getDb: () => null,
    isSafeId: () => true,
    normalizeCallRecordForApi: (call) => call,
    buildDigitSummary: () => ({ summary: '', count: 0 }),
    parsePagination: () => ({ limit: 10, offset: 0 }),
    normalizeCallStatus: (value) => value,
    normalizeDateFilter: () => null,
    parseBoundedInteger: () => 20,
    getDigitService: () => null,
    getTranscriptAudioUrl: jest.fn(),
    transcriptAudioTimeoutMs: 12000,
    transcriptAudioMaxChars: 2600,
  };
}

function buildStatusRouteCtx() {
  return {
    requireOutboundAuthorization: noopMiddleware,
    getDb: () => null,
    isSafeId: () => true,
    normalizeCallRecordForApi: (call) => call,
    buildDigitSummary: () => ({ summary: '', count: 0 }),
    webhookService: {
      getCallStatusStats: () => ({}),
      healthCheck: async () => ({ status: 'ok' }),
    },
    getProviderReadiness: () => ({ twilio: true, vonage: true }),
    appVersion: 'test',
    getCurrentProvider: () => 'twilio',
    getCurrentSmsProvider: () => 'twilio',
    getCurrentEmailProvider: () => 'sendgrid',
    getProviderCompatibilityReport: () => ({}),
    callConfigurations: new Map(),
    config: {},
    verifyHmacSignature: () => ({ ok: true }),
    hasAdminToken: () => true,
    refreshInboundDefaultScript: async () => {},
    getInboundHealthContext: () => ({
      inboundDefaultSummary: null,
      inboundEnvSummary: null,
    }),
    supportedProviders: ['twilio', 'vonage'],
    providerHealth: new Map(),
    isProviderDegraded: () => false,
    pruneExpiredKeypadProviderOverrides: () => {},
    keypadProviderOverrides: new Map(),
    functionEngine: null,
    callFunctionSystems: new Map(),
  };
}

function buildWebhookRouteCtx() {
  const noopHandler = (_req, res) => {
    if (res && typeof res.status === 'function') {
      res.status(200).send('OK');
    }
  };
  return {
    config: {},
    handleTwilioGatherWebhook: noopHandler,
    requireValidTwilioSignature: () => true,
    requireValidAwsWebhook: () => true,
    requireValidEmailWebhook: () => true,
    requireValidVonageWebhook: () => true,
    requireValidTelegramWebhook: () => true,
    processCallStatusWebhookPayload: async () => {},
    getDb: () => null,
    streamStatusDedupe: new Map(),
    activeStreamConnections: new Map(),
    shouldProcessProviderEvent: () => true,
    shouldProcessProviderEventAsync: async () => true,
    recordCallStatus: async () => {},
    getVonageCallPayload: () => ({}),
    getVonageDtmfDigits: () => '',
    resolveVonageCallSid: () => '',
    isOutboundVonageDirection: () => false,
    buildVonageInboundCallSid: () => '',
    refreshInboundDefaultScript: async () => {},
    hydrateCallConfigFromDb: async () => ({}),
    ensureCallSetup: async () => ({}),
    ensureCallRecord: async () => ({}),
    normalizePhoneForFlag: (value) => value,
    shouldRateLimitInbound: () => ({ limited: false }),
    rememberVonageCallMapping: () => {},
    handleExternalDtmfInput: async () => {},
    clearVonageCallMappings: () => {},
    buildVonageWebsocketUrl: () => 'wss://example.test/vonage/stream',
    getVonageWebsocketContentType: () => 'audio/l16;rate=16000',
    buildVonageEventWebhookUrl: () => 'https://example.test/event',
    resolveHost: () => 'example.test',
    buildRetrySmsBody: () => 'retry',
    buildRetryPayload: async () => ({}),
    scheduleCallJob: async () => ({}),
    formatContactLabel: () => 'contact',
    placeOutboundCall: async () => ({ callId: 'CA123' }),
    buildRecapSmsBody: () => 'recap',
    logConsoleAction: async () => {},
    buildInboundSmsBody: () => 'inbound',
    buildCallbackPayload: () => ({}),
    endCallForProvider: async () => {},
    webhookService: {
      answerCallbackQuery: async () => {},
      sendTelegramMessage: async () => {},
      setInboundGate: () => {},
      sendCallStatusUpdate: async () => {},
      togglePreviewRedaction: () => false,
      toggleConsoleActions: () => false,
      lockConsoleButtons: () => {},
      unlockConsoleButtons: () => {},
      addLiveEvent: () => {},
      setCallerFlag: () => {},
      markToolInvocation: async () => {},
      setLiveCallPhase: async () => {},
    },
    buildVonageTalkHangupNcco: () => [],
    buildVonageUnavailableNcco: () => [],
    buildTwilioStreamTwiml: () => '<Response/>',
    getCallConfigurations: () => new Map(),
    getCallFunctionSystems: () => new Map(),
    getCallDirections: () => new Map(),
    getAwsContactMap: () => new Map(),
    getActiveCalls: () => new Map(),
    handleCallEnd: async () => {},
    maskPhoneForLog: () => '***0000',
    maskSmsBodyForLog: () => '[masked]',
    smsService: {
      sendSMS: async () => ({ success: true }),
      handleIncomingSMS: async () => ({ success: true }),
      processDeliveryUpdate: async () => ({ success: true }),
    },
    smsWebhookDedupeTtlMs: 120000,
    getDigitService: () => null,
    getEmailService: () => null,
  };
}

describe('API route registration smoke', () => {
  test('registers critical call/status/webhook routes', () => {
    const app = createMockApp();
    registerCallRoutes(app, buildCallRouteCtx());
    registerStatusRoutes(app, buildStatusRouteCtx());
    registerWebhookRoutes(app, buildWebhookRouteCtx());

    const routeSet = new Set(
      app.routes.map((route) => `${route.method} ${route.path}`),
    );

    expect(routeSet.has('POST /outbound-call')).toBe(true);
    expect(routeSet.has('GET /api/calls/:callSid')).toBe(true);
    expect(routeSet.has('GET /api/calls/:callSid/status')).toBe(true);
    expect(routeSet.has('GET /health')).toBe(true);
    expect(routeSet.has('POST /webhook/call-status')).toBe(true);
    expect(routeSet.has('POST /webhook/sms')).toBe(true);
    expect(routeSet.has('POST /webhook/sms-status')).toBe(true);
    expect(routeSet.has('POST /webhook/sms-delivery')).toBe(true);
    expect(routeSet.has('GET /va')).toBe(true);
    expect(routeSet.has('POST /ve')).toBe(true);
  });
});
