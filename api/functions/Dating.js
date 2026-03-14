const {
  PROFILE_DEFINITIONS,
  PROFILE_ALIASES,
  PROFILE_TONE_DIAL,
  DEFAULT_PROFILE_TYPE,
  normalizeProfileType,
  getProfileDefinition,
  listProfileTypes,
  getRelationshipObjectiveTags,
  getRelationshipFlowTypes,
  buildProfilePromptBundle,
  buildRelationshipContext,
  evaluateProfileToolPolicy,
  applyProfilePolicyGates,
  validateProfilePacks,
} = require("./profileRegistry");

const DATING_OBJECTIVE_TAG = PROFILE_DEFINITIONS.dating.objectiveTag;
const DATING_FLOW_TYPE = PROFILE_DEFINITIONS.dating.flowType;
const DATING_PROMPT_MARKER = PROFILE_DEFINITIONS.dating.marker;

const CELEBRITY_OBJECTIVE_TAG = PROFILE_DEFINITIONS.celebrity.objectiveTag;
const CELEBRITY_FLOW_TYPE = PROFILE_DEFINITIONS.celebrity.flowType;
const CELEBRITY_PROMPT_MARKER = PROFILE_DEFINITIONS.celebrity.marker;

const RELATIONSHIP_PROFILE_TYPES = Object.freeze(listProfileTypes());
const RELATIONSHIP_OBJECTIVE_TAGS = Object.freeze(getRelationshipObjectiveTags());
const RELATIONSHIP_FLOW_TYPES = Object.freeze(getRelationshipFlowTypes());
const RELATIONSHIP_PROFILE_ALIASES = Object.freeze({ ...PROFILE_ALIASES });
const RELATIONSHIP_PROFILE_OBJECTIVE_MAP = Object.freeze(
  RELATIONSHIP_PROFILE_TYPES.reduce((acc, profileType) => {
    const definition = getProfileDefinition(profileType);
    if (definition?.objectiveTag) {
      acc[profileType] = definition.objectiveTag;
    }
    return acc;
  }, {}),
);
const RELATIONSHIP_PROFILE_FLOW_MAP = Object.freeze(
  RELATIONSHIP_PROFILE_TYPES.reduce((acc, profileType) => {
    const definition = getProfileDefinition(profileType);
    if (definition?.flowType) {
      acc[profileType] = definition.flowType;
    }
    return acc;
  }, {}),
);

const PROFILE_TEXT_SIGNALS = Object.freeze({
  dating: ["dating", "girlfriend", "boyfriend", "romance", "situationship"],
  celebrity: ["celebrity", "fan club", "artist", "official assistant"],
  fan: ["fandom", "fan", "community update", "exclusive drop"],
  creator: ["creator", "collab", "partnership", "ugc"],
  friendship: ["friend", "check in", "reconnect", "catch up"],
  networking: ["network", "follow-up", "intro", "referral"],
  community: ["community", "member", "moderation", "onboarding"],
  marketplace_seller: ["listing", "buyer", "marketplace", "item details"],
  real_estate_agent: ["listing", "property", "open house", "tour"],
});

function normalizeLower(value) {
  return String(value || "").trim().toLowerCase();
}

function normalizeRelationshipProfileType(value, fallback = DEFAULT_PROFILE_TYPE) {
  return normalizeProfileType(value, fallback);
}

function normalizeObjectiveTags(value) {
  if (!Array.isArray(value)) return [];
  return value
    .map((entry) => normalizeLower(entry))
    .filter(Boolean);
}

function getProfileByObjectiveTag(tag = "") {
  const normalized = normalizeLower(tag);
  if (!normalized) return "";
  if (normalized === "celebrity_fan_engagement") {
    return "celebrity";
  }
  for (const profileType of RELATIONSHIP_PROFILE_TYPES) {
    const definition = getProfileDefinition(profileType);
    if (!definition) continue;
    if (normalizeLower(definition.objectiveTag) === normalized) {
      return definition.id;
    }
  }
  if (normalized === "fan_engagement") return "fan";
  return "";
}

function hasDatingObjectiveTag(tags) {
  return normalizeObjectiveTags(tags).some((tag) => {
    const profileType = getProfileByObjectiveTag(tag);
    return profileType === "dating";
  });
}

function hasCelebrityObjectiveTag(tags) {
  return normalizeObjectiveTags(tags).some((tag) => {
    const profileType = getProfileByObjectiveTag(tag);
    return profileType === "celebrity" || profileType === "fan";
  });
}

function isDatingPurpose(value) {
  return normalizeRelationshipProfileType(value) === "dating";
}

function isCelebrityPurpose(value) {
  return normalizeRelationshipProfileType(value) === "celebrity";
}

function inferProfileFromTextWithSignals(...texts) {
  const merged = texts
    .map((entry) => normalizeLower(entry))
    .filter(Boolean)
    .join(" ");
  if (!merged) {
    return {
      profileType: "",
      matchedSignals: [],
      score: 0,
      ambiguous: false,
    };
  }

  const scored = [];
  for (const [profileType, signals] of Object.entries(PROFILE_TEXT_SIGNALS)) {
    const hits = [];
    let score = 0;
    for (const token of signals) {
      const normalizedToken = normalizeLower(token);
      if (!normalizedToken || !merged.includes(normalizedToken)) continue;
      hits.push(normalizedToken);
      // Multi-word hits are stronger intent signals than generic single words.
      score += normalizedToken.includes(" ") ? 2 : 1;
    }
    if (score > 0) {
      scored.push({ profileType, score, matchedSignals: hits });
    }
  }

  if (!scored.length) {
    return {
      profileType: "",
      matchedSignals: [],
      score: 0,
      ambiguous: false,
    };
  }

  scored.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    if (b.matchedSignals.length !== a.matchedSignals.length) {
      return b.matchedSignals.length - a.matchedSignals.length;
    }
    return a.profileType.localeCompare(b.profileType);
  });

  const best = scored[0];
  const tied = scored.filter((entry) => entry.score === best.score);
  if (tied.length > 1) {
    return {
      profileType: "",
      matchedSignals: [],
      score: best.score,
      ambiguous: true,
    };
  }

  return {
    profileType: best.profileType,
    matchedSignals: best.matchedSignals.slice(0, 6),
    score: best.score,
    ambiguous: false,
  };
}

function detectProfileFromMarkers(...texts) {
  const merged = texts
    .map((entry) => normalizeLower(entry))
    .filter(Boolean)
    .join(" ");
  if (!merged) return "";

  for (const profileType of RELATIONSHIP_PROFILE_TYPES) {
    const definition = getProfileDefinition(profileType);
    const marker = normalizeLower(definition?.marker);
    if (!marker) continue;
    if (merged.includes(marker)) {
      return definition.id;
    }
  }
  return "";
}

function detectProfileFromScriptTemplate(template = {}) {
  if (!template || typeof template !== "object") return "";

  const explicitProfile = normalizeRelationshipProfileType(
    template.profile_type || template.purpose || template.default_profile,
    "",
  );
  if (explicitProfile && explicitProfile !== DEFAULT_PROFILE_TYPE) {
    return explicitProfile;
  }

  const flowType = normalizeRelationshipProfileType(template.flow_type, "");
  if (flowType && flowType !== DEFAULT_PROFILE_TYPE) {
    return flowType;
  }

  if (Array.isArray(template.flow_types)) {
    for (const flowEntry of template.flow_types) {
      const normalizedFlow = normalizeRelationshipProfileType(flowEntry, "");
      if (normalizedFlow && normalizedFlow !== DEFAULT_PROFILE_TYPE) {
        return normalizedFlow;
      }
    }
  }

  const objectiveTags = normalizeObjectiveTags(template.objective_tags);
  for (const tag of objectiveTags) {
    const profileType = getProfileByObjectiveTag(tag);
    if (profileType) {
      return profileType;
    }
  }

  return "";
}

function isDatingScriptTemplate(template = {}) {
  return detectProfileFromScriptTemplate(template) === "dating";
}

function isCelebrityScriptTemplate(template = {}) {
  return detectProfileFromScriptTemplate(template) === "celebrity";
}

function deriveConversationProfile(input = {}) {
  return deriveConversationProfileDecision(input).profile_type;
}

function deriveConversationProfileDecision(input = {}) {
  const fallback = normalizeRelationshipProfileType(input.fallback, DEFAULT_PROFILE_TYPE);
  const explicitProfile = normalizeRelationshipProfileType(input.purpose, "");
  if (explicitProfile && explicitProfile !== DEFAULT_PROFILE_TYPE) {
    return {
      profile_type: explicitProfile,
      source: "purpose",
      confidence: "high",
      matched_signals: [],
      ambiguous: false,
    };
  }

  const templateProfile = detectProfileFromScriptTemplate(input.scriptTemplate || {});
  if (templateProfile) {
    return {
      profile_type: templateProfile,
      source: "script_template",
      confidence: "high",
      matched_signals: [],
      ambiguous: false,
    };
  }

  const markerProfile = detectProfileFromMarkers(input.prompt, input.firstMessage);
  if (markerProfile) {
    return {
      profile_type: markerProfile,
      source: "prompt_marker",
      confidence: "high",
      matched_signals: [],
      ambiguous: false,
    };
  }

  const inferred = inferProfileFromTextWithSignals(input.prompt, input.firstMessage);
  if (inferred.profileType) {
    return {
      profile_type: inferred.profileType,
      source: "text_signals",
      confidence: inferred.score >= 3 ? "high" : "medium",
      matched_signals: inferred.matchedSignals,
      ambiguous: false,
    };
  }

  return {
    profile_type: fallback,
    source: inferred.ambiguous ? "fallback_ambiguous" : "fallback_default",
    confidence: "low",
    matched_signals: [],
    ambiguous: inferred.ambiguous === true,
  };
}

function buildConversationProfilePromptBundle(profileType, options = {}) {
  const normalized = normalizeRelationshipProfileType(profileType, DEFAULT_PROFILE_TYPE);
  return buildProfilePromptBundle(normalized, options);
}

function buildDatingPromptBundle(options = {}) {
  return buildConversationProfilePromptBundle("dating", options);
}

function buildCelebrityPromptBundle(options = {}) {
  return buildConversationProfilePromptBundle("celebrity", options);
}

function createProfileContextToolkit(profileType, options = {}) {
  const normalizedProfile = normalizeRelationshipProfileType(
    profileType,
    DEFAULT_PROFILE_TYPE,
  );
  const definition = getProfileDefinition(normalizedProfile, "");
  if (!definition || normalizedProfile === DEFAULT_PROFILE_TYPE) {
    return { tools: [], implementations: {} };
  }

  const callSid = String(options.callSid || "").trim();
  const getCallConfig =
    typeof options.getCallConfig === "function" ? options.getCallConfig : () => null;
  const setCallConfig =
    typeof options.setCallConfig === "function" ? options.setCallConfig : () => {};
  const queueRuntimePersist =
    typeof options.queueRuntimePersist === "function"
      ? options.queueRuntimePersist
      : null;
  const updateCallState =
    typeof options.updateCallState === "function" ? options.updateCallState : null;
  const addLiveEvent =
    typeof options.addLiveEvent === "function" ? options.addLiveEvent : null;

  const toolBaseName = String(options.toolBaseName || normalizedProfile)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, "_");
  const legacyContextKey = String(options.contextAlias || `${toolBaseName}_context`).trim();
  const setToolName = `set_${toolBaseName}_context`;
  const getToolName = `get_${toolBaseName}_context`;
  const platformEnum = Object.keys(PROFILE_TONE_DIAL);

  const tools = [
    {
      type: "function",
      function: {
        name: setToolName,
        description: `Update live ${normalizedProfile} relationship context for this call.`,
        permission: "write",
        parameters: {
          type: "object",
          properties: {
            profile_type: {
              type: "string",
              enum: RELATIONSHIP_PROFILE_TYPES,
              description: "Relationship profile type.",
            },
            stage: {
              type: "string",
              enum: definition.stageEnum,
              description: "Current relationship stage.",
            },
            vibe: {
              type: "string",
              enum: definition.vibeEnum,
              description: "Current emotional signal/vibe.",
            },
            goal: {
              type: "string",
              enum: definition.goalEnum,
              description: "Primary next-turn objective.",
            },
            platform: {
              type: "string",
              enum: platformEnum,
              description: "Conversation platform style target.",
            },
            context_notes: {
              type: "string",
              description: "Short shared runtime note for this profile.",
            },
            note: {
              type: "string",
              description: "Alias of context_notes for compatibility.",
            },
          },
          required: [],
        },
      },
    },
    {
      type: "function",
      function: {
        name: getToolName,
        description: `Fetch current ${normalizedProfile} relationship context for this call.`,
        permission: "read",
        parameters: {
          type: "object",
          properties: {},
          required: [],
        },
      },
    },
  ];

  const implementations = {
    [setToolName]: async (args = {}) => {
      const callConfig = getCallConfig();
      if (!callConfig || typeof callConfig !== "object") {
        return { status: "error", error: "call_config_unavailable" };
      }

      const previous =
        callConfig?.relationship_profile &&
        typeof callConfig.relationship_profile === "object" &&
        callConfig.relationship_profile.profile_type === normalizedProfile
          ? callConfig.relationship_profile
          : callConfig?.[legacyContextKey] &&
              typeof callConfig[legacyContextKey] === "object"
            ? callConfig[legacyContextKey]
            : {};

      const nextContext = buildRelationshipContext(normalizedProfile, args, previous);
      callConfig.relationship_profile = nextContext;
      callConfig[legacyContextKey] = nextContext;
      callConfig.conversation_profile = normalizedProfile;
      if (!callConfig.purpose) {
        callConfig.purpose = normalizedProfile;
      }
      setCallConfig(callConfig);

      if (queueRuntimePersist && callSid) {
        queueRuntimePersist(callSid, {
          snapshot: {
            source: setToolName,
            conversation_profile: normalizedProfile,
            purpose: callConfig.purpose,
            relationship_profile: nextContext,
            [legacyContextKey]: nextContext,
          },
        });
      }

      if (updateCallState && callSid) {
        updateCallState(callSid, `${normalizedProfile}_context_updated`, {
          conversation_profile: normalizedProfile,
          purpose: callConfig.purpose,
          relationship_profile: nextContext,
          [legacyContextKey]: nextContext,
          at: new Date().toISOString(),
        }).catch(() => {});
      }

      if (addLiveEvent && callSid) {
        addLiveEvent(
          callSid,
          `Relationship profile context updated (${normalizedProfile}, stage: ${nextContext.stage})`,
          { force: false },
        );
      }

      return {
        status: "ok",
        conversation_profile: normalizedProfile,
        relationship_profile: nextContext,
        [legacyContextKey]: nextContext,
      };
    },

    [getToolName]: async () => {
      const callConfig = getCallConfig();
      const context =
        callConfig?.relationship_profile &&
        typeof callConfig.relationship_profile === "object" &&
        callConfig.relationship_profile.profile_type === normalizedProfile
          ? callConfig.relationship_profile
          : callConfig?.[legacyContextKey] &&
              typeof callConfig[legacyContextKey] === "object"
            ? callConfig[legacyContextKey]
            : buildRelationshipContext(normalizedProfile, {}, {});

      return {
        status: "ok",
        conversation_profile: normalizedProfile,
        relationship_profile: context,
        [legacyContextKey]: context,
      };
    },
  };

  return { tools, implementations };
}

function createConversationProfileToolkit(profileType, options = {}) {
  return createProfileContextToolkit(profileType, options);
}

function validateRelationshipProfilePacks(options = {}) {
  return validateProfilePacks(options);
}

function createDatingFunctionToolkit(options = {}) {
  return createProfileContextToolkit("dating", {
    ...options,
    toolBaseName: "dating",
    contextAlias: "dating_context",
  });
}

function createCelebrityFunctionToolkit(options = {}) {
  return createProfileContextToolkit("celebrity", {
    ...options,
    toolBaseName: "celebrity",
    contextAlias: "celebrity_context",
  });
}

module.exports = {
  DATING_OBJECTIVE_TAG,
  DATING_FLOW_TYPE,
  DATING_PROMPT_MARKER,
  CELEBRITY_OBJECTIVE_TAG,
  CELEBRITY_FLOW_TYPE,
  CELEBRITY_PROMPT_MARKER,
  RELATIONSHIP_PROFILE_TYPES,
  RELATIONSHIP_OBJECTIVE_TAGS,
  RELATIONSHIP_FLOW_TYPES,
  RELATIONSHIP_PROFILE_ALIASES,
  RELATIONSHIP_PROFILE_OBJECTIVE_MAP,
  RELATIONSHIP_PROFILE_FLOW_MAP,
  normalizeRelationshipProfileType,
  isDatingPurpose,
  isCelebrityPurpose,
  hasDatingObjectiveTag,
  hasCelebrityObjectiveTag,
  isDatingScriptTemplate,
  isCelebrityScriptTemplate,
  deriveConversationProfile,
  deriveConversationProfileDecision,
  buildDatingPromptBundle,
  buildCelebrityPromptBundle,
  buildConversationProfilePromptBundle,
  createDatingFunctionToolkit,
  createCelebrityFunctionToolkit,
  createConversationProfileToolkit,
  validateRelationshipProfilePacks,
  evaluateConversationProfileToolPolicy: evaluateProfileToolPolicy,
  applyConversationPolicyGates: applyProfilePolicyGates,
};
