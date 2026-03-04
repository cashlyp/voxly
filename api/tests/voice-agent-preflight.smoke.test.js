"use strict";

const {
  shouldRunDeepgramVoiceAgentPreflight,
  normalizeThinkModelsResponse,
  evaluateThinkModelCompatibility,
  executeDeepgramVoiceAgentThinkPreflight,
} = require("../services/deepgramVoiceAgentPreflight");

function buildJsonResponse(payload, status = 200, statusText = "OK") {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText,
    json: async () => payload,
  };
}

describe("Deepgram Voice Agent preflight smoke", () => {
  test("runs only when voice agent runtime is enabled and active", () => {
    expect(
      shouldRunDeepgramVoiceAgentPreflight({ enabled: false, mode: "voice_agent" }),
    ).toBe(false);
    expect(
      shouldRunDeepgramVoiceAgentPreflight({ enabled: true, mode: "legacy" }),
    ).toBe(false);
    expect(
      shouldRunDeepgramVoiceAgentPreflight({ enabled: true, mode: "hybrid" }),
    ).toBe(true);
    expect(
      shouldRunDeepgramVoiceAgentPreflight({ enabled: true, mode: "voice_agent" }),
    ).toBe(true);
  });

  test("normalizes catalog entries and evaluates compatibility", () => {
    const models = normalizeThinkModelsResponse({
      models: [
        { provider: "open_ai", id: "gpt-4o-mini", name: "GPT-4o mini" },
        { provider: "open_ai", id: "gpt-4.1", name: "GPT-4.1" },
        { provider: "anthropic", id: "claude-4-5-haiku-latest" },
      ],
    });
    const evaluation = evaluateThinkModelCompatibility(models, {
      provider: "open_ai",
      model: "gpt-4o-mini",
    });
    expect(evaluation.isSupported).toBe(true);
    expect(evaluation.providerModelCount).toBe(2);
    expect(evaluation.providerModelsSample).toContain("gpt-4o-mini");
  });

  test("returns skipped result when runtime is inactive", async () => {
    const fetchImpl = jest.fn();
    const result = await executeDeepgramVoiceAgentThinkPreflight({
      fetchImpl,
      apiKey: "dg-key",
      enabled: false,
      mode: "legacy",
    });
    expect(result.skipped).toBe(true);
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  test("validates configured model against Deepgram catalog", async () => {
    const fetchImpl = jest.fn(async () =>
      buildJsonResponse({
        models: [
          { provider: "open_ai", id: "gpt-4o-mini" },
          { provider: "open_ai", id: "gpt-4.1" },
        ],
      }),
    );

    const result = await executeDeepgramVoiceAgentThinkPreflight({
      fetchImpl,
      apiKey: "dg-key",
      enabled: true,
      mode: "voice_agent",
      thinkProvider: "open_ai",
      thinkModel: "gpt-4o-mini",
    });

    expect(result.ok).toBe(true);
    expect(result.skipped).toBe(false);
    expect(result.provider).toBe("open_ai");
    expect(result.model).toBe("gpt-4o-mini");
  });

  test("throws clear error when configured model is unsupported", async () => {
    const fetchImpl = jest.fn(async () =>
      buildJsonResponse({
        models: [{ provider: "open_ai", id: "gpt-4.1" }],
      }),
    );

    await expect(
      executeDeepgramVoiceAgentThinkPreflight({
        fetchImpl,
        apiKey: "dg-key",
        enabled: true,
        mode: "voice_agent",
        thinkProvider: "open_ai",
        thinkModel: "gpt-4o-mini",
      }),
    ).rejects.toMatchObject({
      code: "voice_agent_preflight_model_unsupported",
    });
  });
});
