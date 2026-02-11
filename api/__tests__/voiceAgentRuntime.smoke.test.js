"use strict";

process.env.NODE_ENV = process.env.NODE_ENV || "test";
process.env.TWILIO_ACCOUNT_SID =
  process.env.TWILIO_ACCOUNT_SID || "AC00000000000000000000000000000000";
process.env.TWILIO_AUTH_TOKEN =
  process.env.TWILIO_AUTH_TOKEN || "test_auth_token";
process.env.FROM_NUMBER = process.env.FROM_NUMBER || "+15005550006";
process.env.DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY || "test_key";
process.env.OPENROUTER_API_KEY =
  process.env.OPENROUTER_API_KEY || "test_openrouter_key";
process.env.TELEGRAM_BOT_TOKEN =
  process.env.TELEGRAM_BOT_TOKEN || "123456:test_bot_token";

const { __testables } = require("../app");

const {
  getVoiceAgentRuntimeConfig,
  getOrCreateVoiceAgentRuntimeState,
  resetVoiceAgentRuntimeForTests,
  resetVoiceAgentCircuitStateForTests,
  setDbForTests,
  isVoiceAgentCircuitOpen,
  recordVoiceAgentCircuitFailure,
  persistVoiceAgentCircuitStateNow,
  loadPersistedVoiceAgentCircuitState,
  resolveVoiceAgentFallbackSkipReason,
  beginVoiceAgentFallbackIfAllowed,
} = __testables;

describe("voice-agent runtime smoke", () => {
  beforeEach(() => {
    resetVoiceAgentRuntimeForTests();
    resetVoiceAgentCircuitStateForTests();
    setDbForTests(null);
  });

  afterEach(() => {
    resetVoiceAgentRuntimeForTests();
    resetVoiceAgentCircuitStateForTests();
    setDbForTests(null);
  });

  test("circuit breaker is provider-scoped", () => {
    const threshold = getVoiceAgentRuntimeConfig().circuitFailureThreshold;
    for (let i = 0; i < threshold; i += 1) {
      recordVoiceAgentCircuitFailure("unit_test_failure", {
        provider: "twilio",
        callSid: "CA_TWILIO_SCOPE",
      });
    }

    expect(isVoiceAgentCircuitOpen("twilio")).toBe(true);
    expect(isVoiceAgentCircuitOpen("vonage")).toBe(false);
  });

  test("circuit state persists and reloads across restart", async () => {
    const settings = {};
    const mockDb = {
      setSetting: jest.fn(async (key, value) => {
        settings[key] = value;
        return 1;
      }),
      getSetting: jest.fn(async (key) => settings[key] ?? null),
      addCallMetric: jest.fn(async () => 1),
      updateCallState: jest.fn(async () => 1),
    };
    setDbForTests(mockDb);

    const threshold = getVoiceAgentRuntimeConfig().circuitFailureThreshold;
    for (let i = 0; i < threshold; i += 1) {
      recordVoiceAgentCircuitFailure("unit_test_persist", {
        provider: "twilio",
        callSid: "CA_PERSIST",
      });
    }
    expect(isVoiceAgentCircuitOpen("twilio")).toBe(true);

    await persistVoiceAgentCircuitStateNow();

    resetVoiceAgentCircuitStateForTests();
    setDbForTests(mockDb);
    await loadPersistedVoiceAgentCircuitState();

    expect(isVoiceAgentCircuitOpen("twilio")).toBe(true);
    expect(isVoiceAgentCircuitOpen("vonage")).toBe(false);
    expect(mockDb.setSetting).toHaveBeenCalled();
    expect(mockDb.getSetting).toHaveBeenCalled();
  });

  test("fallback is skipped for normal close codes and terminal statuses", async () => {
    const skipByCloseCode = await resolveVoiceAgentFallbackSkipReason(
      "CA_SKIP_CLOSE",
      { closeCode: 1000 },
    );
    expect(skipByCloseCode).toBe("close_code_1000");

    const mockDb = {
      getCall: jest.fn(async () => ({ status: "completed" })),
      addCallMetric: jest.fn(async () => 1),
      updateCallState: jest.fn(async () => 1),
    };
    setDbForTests(mockDb);

    const fallbackResult = await beginVoiceAgentFallbackIfAllowed(
      "CA_SKIP_TERMINAL",
      "stream_closed_1006",
      { source: "smoke_test" },
    );

    expect(fallbackResult.started).toBe(false);
    expect(fallbackResult.skipped).toBe(true);
    expect(fallbackResult.skipReason).toBe("terminal_status_completed");

    const state = getOrCreateVoiceAgentRuntimeState("CA_SKIP_TERMINAL");
    expect(state.fallbackRequested).toBe(false);
  });
});
