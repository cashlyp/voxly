const fs = require("fs");
const path = require("path");

const DEFAULT_PROFILE_TYPE = "general";

const PROFILE_TONE_DIAL = Object.freeze({
  instagram: "Platform style: Instagram. Short playful lines, concise and social.",
  x: "Platform style: X. Direct, compact, and factual with minimal fluff.",
  tiktok: "Platform style: TikTok. Energetic, fast, and trend-aware but clear.",
  whatsapp: "Platform style: WhatsApp. Warm and conversational with short paragraphs.",
  imessage: "Platform style: iMessage. Clean and concise, natural personal tone.",
  sms: "Platform style: SMS. Brief, clear, and action-oriented.",
  voice: "Platform style: Voice call. Short spoken lines with natural pauses.",
  textnow: "Platform style: TextNow. Casual and concise with clear intent.",
});

const PROFILE_ALIASES = Object.freeze({
  default: DEFAULT_PROFILE_TYPE,
  romance: "dating",
  relationship: "dating",
  celebrity: "celebrity",
  celebrity_profile: "celebrity",
  celeb: "celebrity",
  influencer: "fan",
  fan_engagement: "fan",
  celebrity_fan_engagement: "celebrity",
  creator_collab: "creator",
  creator_outreach: "creator",
  friend: "friendship",
  social: "community",
  marketplace: "marketplace_seller",
  "marketplace seller": "marketplace_seller",
  seller: "marketplace_seller",
  realtor: "real_estate_agent",
  estate: "real_estate_agent",
  real_estate: "real_estate_agent",
  "real estate agent": "real_estate_agent",
});

function readProfilePack(profileId, fallbackText) {
  try {
    const filePath = path.join(__dirname, "profiles", `${profileId}.md`);
    const value = fs.readFileSync(filePath, "utf8");
    const normalized = String(value || "").trim();
    return normalized || fallbackText;
  } catch (_) {
    return fallbackText;
  }
}

const PROFILE_DEFINITIONS = Object.freeze({
  dating: {
    id: "dating",
    flowType: "dating",
    objectiveTag: "dating_engagement",
    marker: "[profile_dating_v2]",
    defaultFirstMessage: "Hi, this is your assistant. I wanted a quick check-in.",
    contextKey: "relationship_profile_context",
    stageEnum: ["talking", "situationship", "dating", "exclusive", "complicated"],
    vibeEnum: ["sweet", "flirty", "dry", "stressed", "bold", "neutral"],
    goalEnum: ["bond", "flirt", "make_plans", "soothe", "boundary", "re_engage"],
    safeFallback: "I can keep this respectful and low-pressure. Let us continue with a clear, safe next step.",
    policy: {
      antiImpersonation: true,
      antiHarassment: true,
      antiCoercion: true,
      antiMoneyPressure: true,
    },
  },
  celebrity: {
    id: "celebrity",
    flowType: "celebrity",
    objectiveTag: "celebrity_fan_engagement",
    marker: "[celebrity_profile_v1]",
    defaultFirstMessage:
      "Hi, this is the official fan engagement assistant. Thanks for being part of the community.",
    contextKey: "relationship_profile_context",
    stageEnum: ["new_fan", "engaged_fan", "community_member", "vip_supporter", "event_ready"],
    vibeEnum: ["excited", "curious", "supportive", "skeptical", "frustrated", "neutral"],
    goalEnum: ["welcome", "announce", "invite", "engage", "support", "handoff"],
    safeFallback:
      "I am the official virtual assistant for this community. I can only continue with transparent and safe guidance.",
    policy: {
      antiImpersonation: true,
      antiHarassment: true,
      antiCoercion: true,
      antiMoneyPressure: true,
    },
  },
  fan: {
    id: "fan",
    flowType: "fan",
    objectiveTag: "fan_engagement",
    marker: "[profile_fan_v2]",
    defaultFirstMessage:
      "Hi, this is the official fan engagement assistant. Thanks for being part of the community.",
    contextKey: "relationship_profile_context",
    stageEnum: ["new_fan", "engaged_fan", "community_member", "vip_supporter", "event_ready"],
    vibeEnum: ["excited", "curious", "supportive", "skeptical", "frustrated", "neutral"],
    goalEnum: ["welcome", "announce", "invite", "engage", "support", "handoff"],
    safeFallback:
      "I am the official virtual assistant for this community. I can only continue with transparent and safe guidance.",
    policy: {
      antiImpersonation: true,
      antiHarassment: true,
      antiCoercion: true,
      antiMoneyPressure: true,
    },
  },
  creator: {
    id: "creator",
    flowType: "creator",
    objectiveTag: "creator_engagement",
    marker: "[profile_creator_v1]",
    defaultFirstMessage:
      "Hi, this is a creator collaboration assistant. I have a quick partnership update.",
    contextKey: "relationship_profile_context",
    stageEnum: ["prospect", "qualified", "interested", "negotiating", "active_partner"],
    vibeEnum: ["professional", "curious", "busy", "skeptical", "positive", "neutral"],
    goalEnum: ["qualify", "pitch", "schedule", "align", "confirm", "handoff"],
    safeFallback:
      "I can continue with a clear and respectful collaboration flow, without pressure.",
    policy: {
      antiImpersonation: true,
      antiHarassment: true,
      antiCoercion: true,
      antiMoneyPressure: true,
    },
  },
  friendship: {
    id: "friendship",
    flowType: "friendship",
    objectiveTag: "friendship_engagement",
    marker: "[profile_friendship_v1]",
    defaultFirstMessage: "Hi, this is a friendly check-in assistant. I wanted to reconnect briefly.",
    contextKey: "relationship_profile_context",
    stageEnum: ["reconnect", "active_friend", "close_friend", "cooling", "support_mode"],
    vibeEnum: ["warm", "playful", "calm", "stressed", "reserved", "neutral"],
    goalEnum: ["check_in", "support", "plan", "resolve", "encourage", "close"],
    safeFallback:
      "I can continue with a respectful and supportive check-in only.",
    policy: {
      antiImpersonation: true,
      antiHarassment: true,
      antiCoercion: true,
      antiMoneyPressure: true,
    },
  },
  networking: {
    id: "networking",
    flowType: "networking",
    objectiveTag: "networking_engagement",
    marker: "[profile_networking_v1]",
    defaultFirstMessage: "Hi, this is a networking follow-up assistant. I have a quick update.",
    contextKey: "relationship_profile_context",
    stageEnum: ["intro", "followup", "qualified", "scheduled", "closed"],
    vibeEnum: ["professional", "friendly", "direct", "busy", "hesitant", "neutral"],
    goalEnum: ["introduce", "follow_up", "schedule", "qualify", "connect", "close"],
    safeFallback:
      "I can continue with professional, concise, and respectful networking guidance.",
    policy: {
      antiImpersonation: true,
      antiHarassment: true,
      antiCoercion: true,
      antiMoneyPressure: true,
    },
  },
  community: {
    id: "community",
    flowType: "community",
    objectiveTag: "community_engagement",
    marker: "[profile_community_v1]",
    defaultFirstMessage: "Hi, this is your community assistant with a quick update.",
    contextKey: "relationship_profile_context",
    stageEnum: ["onboarding", "active", "event_cycle", "support", "retention"],
    vibeEnum: ["welcoming", "energetic", "calm", "strict", "helpful", "neutral"],
    goalEnum: ["welcome", "announce", "invite", "moderate", "support", "retain"],
    safeFallback:
      "I can continue with a safe, inclusive, and policy-compliant community flow.",
    policy: {
      antiImpersonation: true,
      antiHarassment: true,
      antiCoercion: true,
      antiMoneyPressure: true,
    },
  },
  marketplace_seller: {
    id: "marketplace_seller",
    flowType: "marketplace_seller",
    objectiveTag: "marketplace_seller_engagement",
    marker: "[profile_marketplace_seller_v1]",
    defaultFirstMessage:
      "Hi, this is a marketplace assistant. I can help confirm item details and next steps.",
    contextKey: "relationship_profile_context",
    stageEnum: ["listing", "inquiry", "negotiation", "pending", "fulfilled"],
    vibeEnum: ["professional", "trustful", "price_sensitive", "urgent", "neutral"],
    goalEnum: ["qualify", "confirm", "schedule", "negotiate", "close", "handoff"],
    safeFallback:
      "I can continue with safe marketplace guidance. Use secure payment methods only.",
    policy: {
      antiImpersonation: true,
      antiHarassment: true,
      antiCoercion: true,
      antiMoneyPressure: true,
    },
  },
  real_estate_agent: {
    id: "real_estate_agent",
    flowType: "real_estate_agent",
    objectiveTag: "real_estate_agent_engagement",
    marker: "[profile_real_estate_agent_v1]",
    defaultFirstMessage:
      "Hi, this is a real-estate assistant. I can help with a quick property follow-up.",
    contextKey: "relationship_profile_context",
    stageEnum: ["lead", "qualified", "tour_scheduled", "offer_stage", "closed"],
    vibeEnum: ["professional", "curious", "hesitant", "motivated", "neutral"],
    goalEnum: ["qualify", "schedule_tour", "share_listing", "follow_up", "handoff", "close"],
    safeFallback:
      "I can continue with compliant real-estate guidance and a clear next step.",
    policy: {
      antiImpersonation: true,
      antiHarassment: true,
      antiCoercion: true,
      antiMoneyPressure: true,
    },
  },
});

function normalizeProfileType(value, fallback = DEFAULT_PROFILE_TYPE) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return fallback;
  if (PROFILE_DEFINITIONS[raw]) return raw;
  if (PROFILE_ALIASES[raw] && PROFILE_DEFINITIONS[PROFILE_ALIASES[raw]]) {
    return PROFILE_ALIASES[raw];
  }
  return fallback;
}

function getProfileDefinition(profileType, fallback = DEFAULT_PROFILE_TYPE) {
  const normalized = normalizeProfileType(profileType, fallback);
  return PROFILE_DEFINITIONS[normalized] || null;
}

function listProfileTypes() {
  return Object.keys(PROFILE_DEFINITIONS);
}

function listProfileDefinitions() {
  return listProfileTypes().map((profileType) => PROFILE_DEFINITIONS[profileType]);
}

function getRelationshipObjectiveTags() {
  return listProfileDefinitions().map((definition) => definition.objectiveTag);
}

function getRelationshipFlowTypes() {
  return listProfileDefinitions().map((definition) => definition.flowType);
}

function getProfilePack(profileType) {
  const definition = getProfileDefinition(profileType);
  if (!definition) return "";
  return readProfilePack(definition.id, `${definition.id} profile pack`);
}

function buildPlatformToneDialBlock() {
  const lines = ["Social-platform tone dial:"];
  for (const [platform, directive] of Object.entries(PROFILE_TONE_DIAL)) {
    lines.push(`- ${platform}: ${directive}`);
  }
  return lines.join("\n");
}

function buildProfilePromptBundle(profileType, options = {}) {
  const definition = getProfileDefinition(profileType);
  const basePrompt = String(options.basePrompt || "").trim();
  const firstMessage = String(options.firstMessage || "").trim();
  if (!definition) {
    return {
      prompt: basePrompt,
      firstMessage,
      applied: false,
      profileType: DEFAULT_PROFILE_TYPE,
    };
  }

  if (basePrompt.includes(definition.marker)) {
    return {
      prompt: basePrompt,
      firstMessage: firstMessage || definition.defaultFirstMessage,
      applied: false,
      profileType: definition.id,
    };
  }

  const mergedPrompt = [
    basePrompt,
    definition.marker,
    `Relationship profile type: ${definition.id}`,
    getProfilePack(definition.id),
    buildPlatformToneDialBlock(),
    "Policy gates: anti-impersonation, anti-harassment, anti-coercion, anti-money-pressure. If triggered, return a safe fallback response.",
  ]
    .filter(Boolean)
    .join("\n\n");

  return {
    prompt: mergedPrompt,
    firstMessage: firstMessage || definition.defaultFirstMessage,
    applied: true,
    profileType: definition.id,
  };
}

function normalizeEnumValue(value, allowed, fallbackValue) {
  const normalized = String(value || "").trim().toLowerCase();
  if (allowed.includes(normalized)) {
    return normalized;
  }
  return fallbackValue;
}

function sanitizeContextNotes(value, maxLength = 320) {
  const normalized = String(value || "").trim();
  if (!normalized) return "";
  return normalized.slice(0, maxLength);
}

function buildRelationshipContext(profileType, input = {}, previous = {}) {
  const definition = getProfileDefinition(profileType);
  if (!definition) {
    return null;
  }

  return {
    profile_type: definition.id,
    stage: normalizeEnumValue(input.stage, definition.stageEnum, previous.stage || definition.stageEnum[0]),
    vibe: normalizeEnumValue(input.vibe, definition.vibeEnum, previous.vibe || "neutral"),
    goal: normalizeEnumValue(input.goal, definition.goalEnum, previous.goal || definition.goalEnum[0]),
    platform: normalizeEnumValue(
      input.platform,
      Object.keys(PROFILE_TONE_DIAL),
      previous.platform || "voice",
    ),
    context_notes: sanitizeContextNotes(
      input.context_notes || input.note || previous.context_notes || previous.note || "",
    ),
    updated_at: new Date().toISOString(),
  };
}

function getProfilePolicy(profileType) {
  const definition = getProfileDefinition(profileType);
  return definition?.policy || null;
}

function applyProfilePolicyGates(rawText = "", profileType = DEFAULT_PROFILE_TYPE) {
  const text = String(rawText || "").trim();
  if (!text) {
    return { text: "", replaced: false, blocked: [] };
  }

  const definition = getProfileDefinition(profileType);
  if (!definition) {
    return { text, replaced: false, blocked: [] };
  }

  const lower = text.toLowerCase();
  const blocked = [];

  if (definition.policy?.antiImpersonation) {
    if (
      /\b(i am|this is)\s+(the\s+)?(real\s+)?(celebrity|artist|influencer|creator)\b/i.test(
        text,
      ) ||
      /\bthis is personally\b/i.test(text)
    ) {
      blocked.push("anti_impersonation");
    }
  }

  if (definition.policy?.antiHarassment) {
    const harassmentTerms = ["idiot", "stupid", "loser", "worthless", "shut up", "moron"];
    if (harassmentTerms.some((term) => lower.includes(term))) {
      blocked.push("anti_harassment");
    }
  }

  if (definition.policy?.antiCoercion) {
    const coercionTerms = ["or else", "you must", "no choice", "if you do not", "do it now"];
    if (coercionTerms.some((term) => lower.includes(term))) {
      blocked.push("anti_coercion");
    }
  }

  if (definition.policy?.antiMoneyPressure) {
    const moneyPressureTerms = [
      "send money now",
      "wire me",
      "cashapp me",
      "gift card",
      "crypto transfer",
      "pay immediately",
    ];
    if (moneyPressureTerms.some((term) => lower.includes(term))) {
      blocked.push("anti_money_pressure");
    }
  }

  if (!blocked.length) {
    return { text, replaced: false, blocked };
  }

  return {
    text: definition.safeFallback,
    replaced: true,
    blocked,
  };
}

module.exports = {
  DEFAULT_PROFILE_TYPE,
  PROFILE_DEFINITIONS,
  PROFILE_TONE_DIAL,
  PROFILE_ALIASES,
  normalizeProfileType,
  getProfileDefinition,
  listProfileTypes,
  listProfileDefinitions,
  getRelationshipObjectiveTags,
  getRelationshipFlowTypes,
  getProfilePack,
  buildProfilePromptBundle,
  buildRelationshipContext,
  getProfilePolicy,
  applyProfilePolicyGates,
};
