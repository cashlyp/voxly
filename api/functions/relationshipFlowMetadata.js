const {
  RELATIONSHIP_PROFILE_TYPES,
  RELATIONSHIP_OBJECTIVE_TAGS,
  RELATIONSHIP_FLOW_TYPES,
  RELATIONSHIP_PROFILE_ALIASES,
  RELATIONSHIP_PROFILE_OBJECTIVE_MAP,
  RELATIONSHIP_PROFILE_FLOW_MAP,
  normalizeRelationshipProfileType,
} = require("./Dating");

const CORE_CALL_OBJECTIVE_IDS = Object.freeze([
  "collect_payment",
  "verify_identity",
  "appointment_confirm",
  "service_recovery",
  "general_outreach",
]);

const CORE_CALL_FLOW_TYPES = Object.freeze([
  "payment_collection",
  "identity_verification",
  "appointment_confirmation",
  "service_recovery",
  "general_outreach",
]);

const RELATIONSHIP_FLOW_TYPE_SET = new Set(RELATIONSHIP_FLOW_TYPES);

const FLOW_OBJECTIVE_TAG_MAP = Object.freeze({
  payment_collection: "collect_payment",
  identity_verification: "verify_identity",
  appointment_confirmation: "appointment_confirm",
  service_recovery: "service_recovery",
  general_outreach: "general_outreach",
  ...RELATIONSHIP_FLOW_TYPES.reduce((acc, flowType) => {
    const normalizedProfile = normalizeRelationshipProfileType(flowType, "");
    const objectiveTag = RELATIONSHIP_PROFILE_OBJECTIVE_MAP[normalizedProfile];
    if (objectiveTag) {
      acc[flowType] = objectiveTag;
    }
    return acc;
  }, {}),
});

const FLOW_OBJECTIVE_TAGS = Object.freeze(
  Array.from(new Set(Object.values(FLOW_OBJECTIVE_TAG_MAP))),
);

const CALL_OBJECTIVE_IDS = Object.freeze([
  ...CORE_CALL_OBJECTIVE_IDS,
  ...RELATIONSHIP_OBJECTIVE_TAGS,
]);

const CALL_SCRIPT_FLOW_TYPES = Object.freeze([
  ...CORE_CALL_FLOW_TYPES,
  ...RELATIONSHIP_FLOW_TYPES,
  "general",
]);

const CALL_SCRIPT_FLOW_TYPE_ALIASES = Object.freeze(
  (() => {
    const aliases = {
      payment_collection: "payment_collection",
      "payment-collection": "payment_collection",
      payment_flow: "payment_collection",
      payment: "payment_collection",
      collect_payment: "payment_collection",
      "collect-payment": "payment_collection",
      identity_verification: "identity_verification",
      "identity-verification": "identity_verification",
      identity: "identity_verification",
      verify_identity: "identity_verification",
      "verify-identity": "identity_verification",
      otp: "identity_verification",
      digit_capture: "identity_verification",
      "digit-capture": "identity_verification",
      appointment_confirmation: "appointment_confirmation",
      "appointment-confirmation": "appointment_confirmation",
      appointment_confirm: "appointment_confirmation",
      "appointment-confirm": "appointment_confirmation",
      appointment: "appointment_confirmation",
      service_recovery: "service_recovery",
      "service-recovery": "service_recovery",
      recovery: "service_recovery",
      general_outreach: "general_outreach",
      "general-outreach": "general_outreach",
      outreach: "general_outreach",
      general: "general",
      default: "general",
    };

    RELATIONSHIP_PROFILE_TYPES.forEach((profileType) => {
      const flowType = RELATIONSHIP_PROFILE_FLOW_MAP[profileType] || profileType;
      const objectiveTag = RELATIONSHIP_PROFILE_OBJECTIVE_MAP[profileType];
      aliases[profileType] = flowType;
      aliases[flowType] = flowType;
      aliases[`${profileType}_flow`] = flowType;
      aliases[`${profileType}-flow`] = flowType;
      if (objectiveTag) {
        aliases[objectiveTag] = flowType;
      }
    });

    Object.entries(RELATIONSHIP_PROFILE_ALIASES).forEach(([alias, profileType]) => {
      const normalizedProfile = normalizeRelationshipProfileType(profileType, "");
      const flowType = RELATIONSHIP_PROFILE_FLOW_MAP[normalizedProfile] || normalizedProfile;
      if (flowType && RELATIONSHIP_FLOW_TYPE_SET.has(flowType)) {
        aliases[String(alias || "").trim().toLowerCase()] = flowType;
      }
    });

    return aliases;
  })(),
);

const CALL_OBJECTIVE_TAG_ALIASES = Object.freeze(
  (() => {
    const aliases = {};
    FLOW_OBJECTIVE_TAGS.forEach((objectiveTag) => {
      aliases[objectiveTag] = objectiveTag;
    });
    Object.entries(CALL_SCRIPT_FLOW_TYPE_ALIASES).forEach(([alias, flowType]) => {
      const objectiveTag = FLOW_OBJECTIVE_TAG_MAP[flowType];
      if (objectiveTag) {
        aliases[alias] = objectiveTag;
      }
    });
    return aliases;
  })(),
);

function normalizeCallScriptFlowType(value) {
  if (value === undefined || value === null) return null;
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return null;
  return CALL_SCRIPT_FLOW_TYPE_ALIASES[raw] || null;
}

function normalizeObjectiveTag(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return "";
  return CALL_OBJECTIVE_TAG_ALIASES[raw] || raw;
}

function parseObjectiveTags(value) {
  if (value === undefined || value === null) return [];
  if (Array.isArray(value)) {
    return value.map((entry) => normalizeObjectiveTag(entry)).filter(Boolean);
  }
  const raw = String(value || "").trim();
  if (!raw) return [];
  if (raw.startsWith("[")) {
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        return parsed.map((entry) => normalizeObjectiveTag(entry)).filter(Boolean);
      }
      return [];
    } catch (_) {
      return [];
    }
  }
  return raw
    .split(",")
    .map((entry) => normalizeObjectiveTag(entry))
    .filter(Boolean);
}

function buildObjectiveTagsForFlow(flowType = null, existingTags = []) {
  const normalizedFlow = normalizeCallScriptFlowType(flowType);
  const merged = Array.isArray(existingTags)
    ? existingTags.map((entry) => normalizeObjectiveTag(entry)).filter(Boolean)
    : [];
  const keep = merged.filter((tag) => !FLOW_OBJECTIVE_TAGS.includes(tag));
  if (!normalizedFlow || normalizedFlow === "general") {
    return keep;
  }
  const mappedTag = FLOW_OBJECTIVE_TAG_MAP[normalizedFlow];
  if (mappedTag && !keep.includes(mappedTag)) {
    keep.push(mappedTag);
  }
  return keep;
}

function isRelationshipFlowType(value) {
  const normalized = normalizeCallScriptFlowType(value) || normalizeRelationshipProfileType(value, "");
  return RELATIONSHIP_FLOW_TYPE_SET.has(normalized);
}

function getCallScriptFlowTypes(script = {}) {
  const rawFlowTypes = Array.isArray(script.flow_types)
    ? script.flow_types
    : script.flow_type
      ? [script.flow_type]
      : [];

  const normalized = [];
  rawFlowTypes.forEach((entry) => {
    const flowType = normalizeCallScriptFlowType(entry);
    if (flowType && !normalized.includes(flowType)) {
      normalized.push(flowType);
    }
  });
  if (normalized.length) {
    return normalized;
  }

  const objectiveTags = Array.isArray(script.objective_tags)
    ? script.objective_tags.map((entry) => normalizeObjectiveTag(entry))
    : [];

  const fallback = [];
  const add = (flowType) => {
    if (!fallback.includes(flowType)) {
      fallback.push(flowType);
    }
  };

  if (script.supports_payment === true || objectiveTags.includes("collect_payment")) {
    add("payment_collection");
  }

  const defaultProfile = normalizeRelationshipProfileType(script.default_profile, "");
  if (
    script.supports_digit_capture === true ||
    script.requires_otp === true ||
    (defaultProfile && !RELATIONSHIP_FLOW_TYPE_SET.has(defaultProfile)) ||
    objectiveTags.includes("verify_identity")
  ) {
    add("identity_verification");
  }

  if (objectiveTags.includes("appointment_confirm")) {
    add("appointment_confirmation");
  }
  if (objectiveTags.includes("service_recovery")) {
    add("service_recovery");
  }
  if (objectiveTags.includes("general_outreach")) {
    add("general_outreach");
  }

  RELATIONSHIP_FLOW_TYPES.forEach((flowType) => {
    const objectiveTag = FLOW_OBJECTIVE_TAG_MAP[flowType];
    if (objectiveTag && objectiveTags.includes(objectiveTag)) {
      add(flowType);
    }
  });

  if (!fallback.length) {
    add("general");
  }
  return fallback;
}

function getPrimaryFlowType(script = {}) {
  const flowTypes = getCallScriptFlowTypes(script);
  return flowTypes[0] || "general";
}

function getEffectiveObjectiveTags(script = {}) {
  const existingTags = Array.isArray(script.objective_tags)
    ? script.objective_tags.map((entry) => normalizeObjectiveTag(entry)).filter(Boolean)
    : [];
  const primaryFlow = getPrimaryFlowType(script);
  return buildObjectiveTagsForFlow(primaryFlow, existingTags);
}

module.exports = {
  CORE_CALL_OBJECTIVE_IDS,
  CORE_CALL_FLOW_TYPES,
  CALL_OBJECTIVE_IDS,
  CALL_SCRIPT_FLOW_TYPES,
  CALL_SCRIPT_FLOW_TYPE_ALIASES,
  CALL_OBJECTIVE_TAG_ALIASES,
  FLOW_OBJECTIVE_TAG_MAP,
  FLOW_OBJECTIVE_TAGS,
  normalizeCallScriptFlowType,
  normalizeObjectiveTag,
  parseObjectiveTags,
  buildObjectiveTagsForFlow,
  getCallScriptFlowTypes,
  getPrimaryFlowType,
  getEffectiveObjectiveTags,
  isRelationshipFlowType,
};

