const {
  parseVoiceModelPool,
  selectDeepgramVoiceModel,
  buildDeepgramVoiceModelCatalog,
  fetchDeepgramTtsModels,
} = require("./deepgramVoiceModels");

describe("deepgram voice models", () => {
  test("parses and deduplicates model pools from delimited strings", () => {
    expect(
      parseVoiceModelPool(
        "aura-2-helena-en|aura-2-arcas-en, aura-2-helena-en ; aura-2-thalia-en",
      ),
    ).toEqual([
      "aura-2-helena-en",
      "aura-2-arcas-en",
      "aura-2-thalia-en",
    ]);
  });

  test("prefers explicit model when it is not a legacy alias", () => {
    const result = selectDeepgramVoiceModel({
      candidateModel: "aura-2-thalia-en",
      conversationProfile: "dating",
      profileVoiceMap: {
        dating: "aura-2-helena-en|aura-2-arcas-en",
      },
      fallbackSpeakModel: "aura-2-andromeda-en",
      callSid: "CA111",
    });

    expect(result.model).toBe("aura-2-thalia-en");
    expect(result.source).toBe("explicit");
  });

  test("uses mapped pool and deterministic selection when no explicit model is set", () => {
    const first = selectDeepgramVoiceModel({
      candidateModel: "",
      conversationProfile: "dating",
      profileVoiceMap: {
        dating: "aura-2-helena-en|aura-2-arcas-en",
      },
      fallbackSpeakModel: "aura-2-andromeda-en",
      callSid: "CA_A",
    });
    const second = selectDeepgramVoiceModel({
      candidateModel: "",
      conversationProfile: "dating",
      profileVoiceMap: {
        dating: "aura-2-helena-en|aura-2-arcas-en",
      },
      fallbackSpeakModel: "aura-2-andromeda-en",
      callSid: "CA_A",
    });

    expect(first.source).toBe("mapped_pool");
    expect(["aura-2-helena-en", "aura-2-arcas-en"]).toContain(first.model);
    expect(second.model).toBe(first.model);
  });

  test("uses female-first dating fallback when mapping is absent", () => {
    const result = selectDeepgramVoiceModel({
      candidateModel: "woman",
      conversationProfile: "dating",
      profileVoiceMap: {},
      fallbackSpeakModel: "aura-2-andromeda-en",
      callSid: "CA_DATING_1",
    });

    expect(result.source).toBe("dating_female_first");
    expect(result.model).toBe("aura-2-helena-en");
  });

  test("builds a catalog that merges curated and remote voices", () => {
    const catalog = buildDeepgramVoiceModelCatalog({
      remoteModels: [
        {
          name: "aura-2-zen-en",
          description: "Experimental voice",
        },
      ],
    });
    const ids = catalog.models.map((entry) => entry.id);

    expect(ids).toContain("aura-2-helena-en");
    expect(ids).toContain("aura-2-zen-en");
    expect(catalog.recommendedByFlow?.dating?.length).toBeGreaterThan(0);
  });

  test("fetches and normalizes remote tts models", async () => {
    const fetchImpl = jest.fn(async () => ({
      ok: true,
      json: async () => ({
        tts: [
          { name: "aura-2-helena-en" },
          { id: "aura-2-arcas-en", description: "deep male voice" },
        ],
      }),
    }));

    const result = await fetchDeepgramTtsModels({
      fetchImpl,
      apiKey: "dg_test_key",
      timeoutMs: 2000,
    });

    expect(result.ok).toBe(true);
    expect(result.models.map((entry) => entry.id)).toEqual([
      "aura-2-helena-en",
      "aura-2-arcas-en",
    ]);
  });
});
