jest.mock("../functions/profileRegistry", () => ({
  getProfileRuntimeContract: jest.fn(),
  normalizeProfileType: jest.fn((value) => String(value || "").trim().toLowerCase()),
  listProfileTypes: jest.fn(() => ["creator", "friendship"]),
}));

const {
  evaluateCanaryConversationQuality,
  runCallCanarySweep,
} = require("./callCanary");
const { getProfileRuntimeContract } = require("../functions/profileRegistry");

describe("call canary quality gate", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test("blocks call plan when fail-closed quality gate finds blocker", async () => {
    getProfileRuntimeContract.mockReturnValue({
      defaultFirstMessage: "Are you there? Can you hear me?",
      safeFallback: "Keep the call professional and clear.",
      responseConstraints: { maxChars: 220, maxQuestions: 1 },
    });
    const placeOutboundCall = jest.fn(async () => ({ callId: "CA123", callStatus: "queued" }));

    const result = await runCallCanarySweep({
      config: {
        callCanary: {
          enabled: true,
          dryRun: false,
          targetNumber: "+15555551234",
          profiles: ["creator"],
          providers: ["twilio"],
          maxCallsPerRun: 1,
          quality: {
            enabled: true,
            failClosed: true,
            minScore: 70,
          },
        },
      },
      placeOutboundCall,
      getProviderReadiness: () => ({ twilio: true }),
    });

    expect(result.ok).toBe(false);
    expect(result.attempted).toBe(1);
    expect(result.failed).toBe(1);
    expect(result.success).toBe(0);
    expect(result.quality.blocked).toBe(1);
    expect(result.attempts[0].status).toBe("quality_blocked");
    expect(placeOutboundCall).not.toHaveBeenCalled();
  });

  test("sanitizes noisy first message before live canary placement", async () => {
    getProfileRuntimeContract.mockReturnValue({
      defaultFirstMessage: "Hey there text me on Instagram DM now",
      safeFallback: "Stay safe and clear.",
      responseConstraints: { maxChars: 220, maxQuestions: 1 },
    });
    const placeOutboundCall = jest.fn(async () => ({ callId: "CA234", callStatus: "queued" }));

    const result = await runCallCanarySweep({
      config: {
        callCanary: {
          enabled: true,
          dryRun: false,
          targetNumber: "+15555557654",
          profiles: ["creator"],
          providers: ["twilio"],
          maxCallsPerRun: 1,
          quality: {
            enabled: true,
            failClosed: true,
            minScore: 60,
          },
        },
      },
      placeOutboundCall,
      getProviderReadiness: () => ({ twilio: true }),
    });

    expect(result.ok).toBe(true);
    expect(result.success).toBe(1);
    expect(result.quality.adjusted).toBe(1);
    expect(placeOutboundCall).toHaveBeenCalledTimes(1);
    const payload = placeOutboundCall.mock.calls[0][0];
    expect(String(payload.first_message || "")).not.toMatch(/instagram/i);
    expect(String(payload.first_message || "")).not.toMatch(/\bdm\b/i);
    expect(String(payload.first_message || "")).toMatch(/call/i);
  });

  test("returns disabled quality result when quality gate is disabled", () => {
    const result = evaluateCanaryConversationQuality({
      profile: "creator",
      firstMessage: "Hello there.",
      prompt: "Keep it clear and short.",
      runtimeContract: { responseConstraints: { maxChars: 220, maxQuestions: 1 } },
      qualityConfig: { enabled: false },
    });

    expect(result.enabled).toBe(false);
    expect(result.status).toBe("disabled");
    expect(result.blocked).toBe(false);
  });
});
