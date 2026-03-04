const {
  clampCanaryPercent,
  parseVoiceRuntimeModeOverride,
  parseVoiceRuntimeCanaryOverride,
  sanitizePersistedVoiceRuntimeControls,
  buildPersistedVoiceRuntimeControlsPayload,
  shouldFallbackVoiceAgentOnDtmf,
  evaluateVoiceAgentAutoCanaryDecision,
  applyVoiceRuntimeControlMutation,
} = require("../services/voiceRuntimeControl");

describe("voice runtime control helpers", () => {
  test("DTMF fallback decision is deterministic for voice-agent mode", () => {
    expect(shouldFallbackVoiceAgentOnDtmf("voice_agent")).toBe(true);
    expect(shouldFallbackVoiceAgentOnDtmf("legacy")).toBe(false);
    expect(shouldFallbackVoiceAgentOnDtmf("hybrid")).toBe(false);
  });

  test("parses and sanitizes persisted control settings", () => {
    const nowMs = Date.now();
    const parsed = sanitizePersistedVoiceRuntimeControls(
      {
        mode_override: "hybrid",
        canary_percent_override: "21",
        canary_percent_override_source: "manual",
        forced_legacy_until_ms: nowMs + 60000,
        auto_canary_cooldown_until_ms: nowMs + 120000,
        auto_canary_last_eval_at_ms: nowMs - 1000,
        updated_at: new Date(nowMs).toISOString(),
      },
      nowMs,
    );

    expect(parsed.modeOverride).toBe("hybrid");
    expect(parsed.canaryPercentOverride).toBe(21);
    expect(parsed.canaryPercentOverrideSource).toBe("manual");
    expect(parsed.forcedLegacyUntilMs).toBeGreaterThan(nowMs);
    expect(parsed.autoCanaryCooldownUntilMs).toBeGreaterThan(nowMs);
    expect(parsed.autoCanaryLastEvalAtMs).toBeGreaterThan(0);
    expect(parsed.updatedAt).toBeTruthy();
  });

  test("builds persisted payload with normalized values", () => {
    const nowMs = Date.now();
    const payload = buildPersistedVoiceRuntimeControlsPayload(
      {
        modeOverride: parseVoiceRuntimeModeOverride("voice_agent"),
        canaryPercentOverride: parseVoiceRuntimeCanaryOverride("31"),
        canaryPercentOverrideSource: "auto_canary",
        forcedLegacyUntilMs: nowMs + 5000,
      },
      nowMs,
    );

    expect(payload.mode_override).toBe("voice_agent");
    expect(payload.canary_percent_override).toBe(31);
    expect(payload.canary_percent_override_source).toBe("auto_canary");
    expect(payload.forced_legacy_until_ms).toBeGreaterThan(nowMs);
  });

  test("auto-canary bootstraps from 0 to minimum percent in hybrid mode", () => {
    const decision = evaluateVoiceAgentAutoCanaryDecision({
      config: {
        enabled: true,
        minPercent: 5,
        maxPercent: 40,
        stepUpPercent: 5,
        stepDownPercent: 10,
        minSamples: 3,
      },
      mode: "hybrid",
      currentCanaryPercent: 0,
      configuredCanaryPercent: 30,
      summary: {
        selected: 0,
        selectedSinceLastEval: 0,
        errorRate: 0,
        fallbackRate: 0,
      },
      cooldownUntilMs: 0,
      nowMs: Date.now(),
    });

    expect(decision.action).toBe("set_canary");
    expect(decision.reason).toBe("bootstrap");
    expect(decision.nextCanaryPercent).toBe(5);
  });

  test("auto-canary reduces to zero on SLO breach when fail-closed", () => {
    const decision = evaluateVoiceAgentAutoCanaryDecision({
      config: {
        enabled: true,
        minPercent: 5,
        maxPercent: 50,
        stepUpPercent: 5,
        stepDownPercent: 10,
        minSamples: 5,
        maxErrorRate: 0.2,
        maxFallbackRate: 0.25,
        failClosedOnBreach: true,
        cooldownMs: 180000,
      },
      mode: "hybrid",
      currentCanaryPercent: 30,
      configuredCanaryPercent: 40,
      summary: {
        selected: 10,
        selectedSinceLastEval: 5,
        errorRate: 0.4,
        fallbackRate: 0.1,
      },
      cooldownUntilMs: 0,
      nowMs: Date.now(),
    });

    expect(decision.action).toBe("set_canary");
    expect(decision.reason).toBe("slo_breach");
    expect(decision.nextCanaryPercent).toBe(0);
    expect(decision.nextCooldownUntilMs).toBeGreaterThan(0);
  });

  test("auto-canary steps up when healthy and fresh samples exist", () => {
    const decision = evaluateVoiceAgentAutoCanaryDecision({
      config: {
        enabled: true,
        minPercent: 5,
        maxPercent: 35,
        stepUpPercent: 5,
        stepDownPercent: 10,
        minSamples: 5,
        maxErrorRate: 0.2,
        maxFallbackRate: 0.25,
      },
      mode: "hybrid",
      currentCanaryPercent: 10,
      configuredCanaryPercent: 30,
      summary: {
        selected: 12,
        selectedSinceLastEval: 3,
        errorRate: 0.05,
        fallbackRate: 0.05,
      },
      cooldownUntilMs: 0,
      nowMs: Date.now(),
    });

    expect(decision.action).toBe("set_canary");
    expect(decision.reason).toBe("healthy_step_up");
    expect(decision.nextCanaryPercent).toBe(15);
  });

  test("clamps canary percent into valid range", () => {
    expect(clampCanaryPercent(-12, 0)).toBe(0);
    expect(clampCanaryPercent(105, 0)).toBe(100);
    expect(clampCanaryPercent("17.9", 0)).toBe(17);
  });

  test("applies admin mutation for mode/canary/manual source", () => {
    const nowMs = Date.now();
    const result = applyVoiceRuntimeControlMutation({
      nowMs,
      circuitCooldownMs: 180000,
      state: {
        modeOverride: null,
        canaryPercentOverride: null,
        forcedLegacyUntilMs: 0,
      },
      body: {
        mode: "hybrid",
        canary_percent: 14,
      },
    });

    expect(result.ok).toBe(true);
    expect(result.state.modeOverride).toBe("hybrid");
    expect(result.state.canaryPercentOverride).toBe(14);
    expect(result.state.canaryPercentOverrideSource).toBe("manual");
    expect(result.state.runtimeOverrideUpdatedAtMs).toBe(nowMs);
    expect(result.actions).toEqual(
      expect.arrayContaining(["mode_override_updated", "canary_override_updated"]),
    );
  });

  test("applies admin mutation for force-legacy and circuit reset", () => {
    const nowMs = Date.now();
    const result = applyVoiceRuntimeControlMutation({
      nowMs,
      circuitCooldownMs: 180000,
      state: {
        modeOverride: "hybrid",
        canaryPercentOverride: 20,
        canaryPercentOverrideSource: "manual",
        forcedLegacyUntilMs: nowMs + 120000,
        autoCanaryCooldownUntilMs: nowMs + 120000,
        autoCanaryLastEvalAtMs: nowMs - 2000,
        runtimeOverrideUpdatedAtMs: nowMs - 4000,
      },
      body: {
        force_legacy_for_ms: 30000,
        reset_circuit: true,
      },
    });

    expect(result.ok).toBe(true);
    expect(result.state.forcedLegacyUntilMs).toBe(0);
    expect(result.state.autoCanaryCooldownUntilMs).toBe(0);
    expect(result.state.autoCanaryLastEvalAtMs).toBe(0);
    expect(result.resetCircuitRequested).toBe(true);
    expect(result.actions).toEqual(
      expect.arrayContaining(["force_legacy_window_updated", "circuit_reset"]),
    );
  });

  test("rejects invalid admin mutation payload", () => {
    const result = applyVoiceRuntimeControlMutation({
      nowMs: Date.now(),
      body: {
        mode: "bad_mode",
      },
      state: {},
    });
    expect(result.ok).toBe(false);
    expect(result.error).toMatch(/Invalid mode override/i);
  });
});
