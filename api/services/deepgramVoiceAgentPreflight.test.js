const {
  shouldRunDeepgramVoiceAgentPreflight,
  normalizeThinkModelsResponse,
  evaluateThinkModelCompatibility,
  executeDeepgramVoiceAgentThinkPreflight,
} = require("./deepgramVoiceAgentPreflight");

describe("deepgram voice agent preflight", () => {
  test("runs only when enabled and mode is hybrid/voice_agent", () => {
    expect(
      shouldRunDeepgramVoiceAgentPreflight({ enabled: false, mode: "voice_agent" }),
    ).toBe(false);
    expect(
      shouldRunDeepgramVoiceAgentPreflight({ enabled: true, mode: "legacy" }),
    ).toBe(false);
    expect(
      shouldRunDeepgramVoiceAgentPreflight({ enabled: true, mode: "hybrid" }),
    ).toBe(true);
  });

  test("normalizes think-model catalog from multiple payload shapes", () => {
    const payload = {
      result: {
        models: [
          { provider: { type: "openai" }, model: "gpt-4o-mini" },
          { provider: "open_ai", id: "gpt-4o" },
        ],
      },
    };
    const result = normalizeThinkModelsResponse(payload);
    expect(result).toEqual([
      { provider: "open_ai", id: "gpt-4o-mini", name: "" },
      { provider: "open_ai", id: "gpt-4o", name: "" },
    ]);
  });

  test("treats provider alias and prefixed model IDs as compatible", () => {
    const result = evaluateThinkModelCompatibility(
      [
        { provider: "open_ai", id: "open_ai/gpt-4o-mini" },
        { provider: "open_ai", id: "gpt-4.1-mini" },
      ],
      { provider: "openai", model: "gpt-4o-mini" },
    );
    expect(result.isSupported).toBe(true);
    expect(result.provider).toBe("open_ai");
  });

  test("skips execution when voice agent runtime is inactive", async () => {
    const result = await executeDeepgramVoiceAgentThinkPreflight({
      enabled: false,
      mode: "legacy",
    });
    expect(result.ok).toBe(true);
    expect(result.skipped).toBe(true);
    expect(result.reason).toBe("voice_agent_runtime_not_active");
  });

  test("succeeds with alias provider catalog", async () => {
    const fetchImpl = jest.fn(async () => ({
      ok: true,
      json: async () => ({
        models: [
          {
            provider: "openai",
            id: "open_ai/gpt-4o-mini",
            name: "OpenAI GPT-4o Mini",
          },
        ],
      }),
    }));
    const result = await executeDeepgramVoiceAgentThinkPreflight({
      fetchImpl,
      apiKey: "dg_test_key",
      enabled: true,
      mode: "voice_agent",
      thinkProvider: "open_ai",
      thinkModel: "gpt-4o-mini",
      timeoutMs: 2000,
    });
    expect(result.ok).toBe(true);
    expect(result.skipped).toBe(false);
    expect(result.provider).toBe("open_ai");
    expect(result.model).toBe("gpt-4o-mini");
    expect(result.catalogSize).toBe(1);
  });
});
