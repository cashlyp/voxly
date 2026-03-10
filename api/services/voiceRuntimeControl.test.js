const {
  evaluateVoiceAgentAutoCanaryDecision,
} = require("./voiceRuntimeControl");

describe("voice runtime control auto-canary decisions", () => {
  test("steps down canary percent when quality breach is detected", () => {
    const nowMs = Date.now();
    const decision = evaluateVoiceAgentAutoCanaryDecision({
      config: {
        enabled: true,
        failClosedOnBreach: false,
        stepDownPercent: 10,
        minSamples: 2,
        minPercent: 5,
        maxPercent: 50,
      },
      mode: "hybrid",
      manualCanaryOverride: false,
      currentCanaryPercent: 40,
      configuredCanaryPercent: 50,
      summary: {
        selected: 12,
        selectedSinceLastEval: 2,
        errorRate: 0.02,
        fallbackRate: 0.03,
        fallbackNoAudioRate: 0.01,
      },
      qualityBreach: true,
      qualityBreachReason: "call_canary_quality_blocked",
      cooldownUntilMs: 0,
      circuitOpen: false,
      nowMs,
    });

    expect(decision.action).toBe("set_canary");
    expect(decision.reason).toBe("call_canary_quality_blocked");
    expect(decision.nextCanaryPercent).toBe(30);
    expect(decision.nextCooldownUntilMs).toBeGreaterThan(nowMs);
  });

  test("keeps healthy step-up behavior when no quality breach exists", () => {
    const decision = evaluateVoiceAgentAutoCanaryDecision({
      config: {
        enabled: true,
        minSamples: 2,
        minPercent: 5,
        maxPercent: 40,
        stepUpPercent: 5,
      },
      mode: "hybrid",
      manualCanaryOverride: false,
      currentCanaryPercent: 10,
      configuredCanaryPercent: 35,
      summary: {
        selected: 10,
        selectedSinceLastEval: 3,
        errorRate: 0.01,
        fallbackRate: 0.02,
        fallbackNoAudioRate: 0.01,
      },
      qualityBreach: false,
      cooldownUntilMs: 0,
      circuitOpen: false,
      nowMs: Date.now(),
    });

    expect(decision.action).toBe("set_canary");
    expect(decision.reason).toBe("healthy_step_up");
    expect(decision.nextCanaryPercent).toBe(15);
  });
});
