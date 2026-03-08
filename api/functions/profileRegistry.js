const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

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

const PROFILE_POLICY_KEYS = Object.freeze([
  "antiImpersonation",
  "antiHarassment",
  "antiCoercion",
  "antiMoneyPressure",
]);

const PROFILE_RESPONSE_CONSTRAINTS = Object.freeze({
  general: Object.freeze({ maxChars: 260, maxQuestions: 1 }),
  dating: Object.freeze({ maxChars: 220, maxQuestions: 1 }),
  celebrity: Object.freeze({ maxChars: 220, maxQuestions: 1 }),
  fan: Object.freeze({ maxChars: 220, maxQuestions: 1 }),
  creator: Object.freeze({ maxChars: 220, maxQuestions: 1 }),
  friendship: Object.freeze({ maxChars: 240, maxQuestions: 1 }),
  networking: Object.freeze({ maxChars: 220, maxQuestions: 1 }),
  community: Object.freeze({ maxChars: 240, maxQuestions: 1 }),
  marketplace_seller: Object.freeze({ maxChars: 220, maxQuestions: 1 }),
  real_estate_agent: Object.freeze({ maxChars: 220, maxQuestions: 1 }),
});

const REQUIRED_PROFILE_FRONTMATTER_KEYS = Object.freeze([
  "id",
  "pack_version",
  "contract_version",
  "objective_tag",
  "flow_type",
  "default_first_message",
  "safe_fallback",
  "max_chars",
  "max_questions",
  "policy_flags",
  "allowed_tools",
  "blocked_tools",
]);

const MANDATORY_POLICY_FLAGS = Object.freeze([
  "anti_impersonation",
  "anti_harassment",
  "anti_coercion",
  "anti_money_pressure",
]);

const PROFILE_PACK_CACHE = new Map(); // profileId -> parsed pack document

function stripWrappingQuotes(value = "") {
  const text = String(value || "").trim();
  if (
    (text.startsWith('"') && text.endsWith('"')) ||
    (text.startsWith("'") && text.endsWith("'"))
  ) {
    return text.slice(1, -1).trim();
  }
  return text;
}

function normalizeFrontmatterList(value) {
  if (Array.isArray(value)) {
    return value
      .map((entry) => stripWrappingQuotes(String(entry || "")))
      .filter(Boolean);
  }
  const text = stripWrappingQuotes(value);
  if (!text) return [];
  if (text.startsWith("[") && text.endsWith("]")) {
    const inner = text.slice(1, -1).trim();
    if (!inner) return [];
    return inner
      .split(",")
      .map((entry) => stripWrappingQuotes(entry))
      .filter(Boolean);
  }
  if (text.includes(",")) {
    return text
      .split(",")
      .map((entry) => stripWrappingQuotes(entry))
      .filter(Boolean);
  }
  return [text];
}

function parseFrontmatterValue(rawValue = "") {
  const value = stripWrappingQuotes(rawValue);
  if (!value) return "";
  if (/^(true|false)$/i.test(value)) {
    return value.toLowerCase() === "true";
  }
  if (/^-?\d+$/.test(value)) {
    return Number(value);
  }
  if (value.startsWith("[") && value.endsWith("]")) {
    return normalizeFrontmatterList(value);
  }
  return value;
}

function parseProfilePackDocument(rawText = "") {
  const normalized = String(rawText || "").replace(/\r\n/g, "\n");
  const lines = normalized.split("\n");
  if (!lines.length || lines[0].trim() !== "---") {
    return {
      hasFrontmatter: false,
      frontmatter: null,
      content: normalized.trim(),
    };
  }

  let closingIndex = -1;
  for (let index = 1; index < lines.length; index += 1) {
    if (lines[index].trim() === "---") {
      closingIndex = index;
      break;
    }
  }

  if (closingIndex === -1) {
    return {
      hasFrontmatter: false,
      frontmatter: null,
      content: normalized.trim(),
    };
  }

  const frontmatter = {};
  const frontmatterLines = lines.slice(1, closingIndex);
  for (const line of frontmatterLines) {
    const trimmed = String(line || "").trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const match = trimmed.match(/^([A-Za-z0-9_]+)\s*:\s*(.*)$/);
    if (!match) continue;
    const key = String(match[1] || "").trim().toLowerCase();
    const rawValue = String(match[2] || "").trim();
    frontmatter[key] = parseFrontmatterValue(rawValue);
  }

  const content = lines.slice(closingIndex + 1).join("\n").trim();
  return {
    hasFrontmatter: true,
    frontmatter,
    content,
  };
}

function loadProfilePackDocument(profileId, fallbackText = "") {
  const profileKey = String(profileId || "").trim().toLowerCase();
  if (!profileKey) {
    return {
      profileId: "",
      filePath: "",
      hasFrontmatter: false,
      frontmatter: null,
      content: String(fallbackText || "").trim(),
    };
  }
  if (PROFILE_PACK_CACHE.has(profileKey)) {
    return PROFILE_PACK_CACHE.get(profileKey);
  }

  const filePath = path.join(__dirname, "profiles", `${profileKey}.md`);
  let rawText = "";
  try {
    rawText = fs.readFileSync(filePath, "utf8");
  } catch (_) {
    const fallbackDocument = {
      profileId: profileKey,
      filePath,
      hasFrontmatter: false,
      frontmatter: null,
      content: String(fallbackText || "").trim(),
    };
    PROFILE_PACK_CACHE.set(profileKey, fallbackDocument);
    return fallbackDocument;
  }

  const parsed = parseProfilePackDocument(rawText);
  const document = {
    profileId: profileKey,
    filePath,
    hasFrontmatter: parsed.hasFrontmatter,
    frontmatter: parsed.frontmatter,
    content: parsed.content || String(fallbackText || "").trim(),
  };
  PROFILE_PACK_CACHE.set(profileKey, document);
  return document;
}

function readProfilePack(profileId, fallbackText) {
  const doc = loadProfilePackDocument(profileId, fallbackText);
  return String(doc?.content || "").trim() || String(fallbackText || "").trim();
}

function getProfilesDirectory() {
  return path.join(__dirname, "profiles");
}

function listProfilePackFiles() {
  const dirPath = getProfilesDirectory();
  try {
    return fs
      .readdirSync(dirPath)
      .filter((entry) => entry.toLowerCase().endsWith(".md"))
      .sort();
  } catch (_) {
    return [];
  }
}

function validateProfileFrontmatterContract(frontmatter, definition, options = {}) {
  const errors = [];
  const warnings = [];
  const isRequired = options.isRequired === true;
  const normalizedFrontmatter =
    frontmatter && typeof frontmatter === "object" ? frontmatter : {};

  if (isRequired) {
    for (const key of REQUIRED_PROFILE_FRONTMATTER_KEYS) {
      if (normalizedFrontmatter[key] === undefined) {
        errors.push(`Missing required frontmatter key: ${key}`);
      }
    }
  }

  const idValue = String(normalizedFrontmatter.id || "").trim().toLowerCase();
  if (idValue && definition && idValue !== definition.id) {
    errors.push(`frontmatter.id must be "${definition.id}"`);
  }

  const objectiveTag = String(normalizedFrontmatter.objective_tag || "")
    .trim()
    .toLowerCase();
  if (objectiveTag && definition && objectiveTag !== String(definition.objectiveTag || "").toLowerCase()) {
    errors.push(`frontmatter.objective_tag must be "${definition.objectiveTag}"`);
  }

  const flowType = String(normalizedFrontmatter.flow_type || "")
    .trim()
    .toLowerCase();
  if (flowType && definition && flowType !== String(definition.flowType || "").toLowerCase()) {
    errors.push(`frontmatter.flow_type must be "${definition.flowType}"`);
  }

  const packVersion = String(normalizedFrontmatter.pack_version || "").trim();
  if (packVersion && !/^v[0-9][a-z0-9._-]*$/i.test(packVersion)) {
    errors.push("frontmatter.pack_version must match v<semver-like>");
  }
  const contractVersion = String(normalizedFrontmatter.contract_version || "").trim();
  if (contractVersion && !/^c[0-9][a-z0-9._-]*$/i.test(contractVersion)) {
    errors.push("frontmatter.contract_version must match c<semver-like>");
  }

  const defaultFirstMessage = String(
    normalizedFrontmatter.default_first_message || "",
  ).trim();
  if (isRequired && defaultFirstMessage.length < 6) {
    errors.push("frontmatter.default_first_message must be at least 6 characters");
  }

  const safeFallback = String(normalizedFrontmatter.safe_fallback || "").trim();
  if (isRequired && safeFallback.length < 24) {
    errors.push("frontmatter.safe_fallback must be at least 24 characters");
  }

  const maxChars = Number(normalizedFrontmatter.max_chars);
  if (
    Number.isFinite(maxChars) &&
    (maxChars < 80 || maxChars > 500)
  ) {
    errors.push("frontmatter.max_chars must be between 80 and 500");
  }
  const maxQuestions = Number(normalizedFrontmatter.max_questions);
  if (
    Number.isFinite(maxQuestions) &&
    (maxQuestions < 0 || maxQuestions > 3)
  ) {
    errors.push("frontmatter.max_questions must be between 0 and 3");
  }

  const policyFlags = normalizeFrontmatterList(normalizedFrontmatter.policy_flags).map(
    (entry) => String(entry || "").trim().toLowerCase(),
  );
  if (isRequired) {
    for (const key of MANDATORY_POLICY_FLAGS) {
      if (!policyFlags.includes(key)) {
        errors.push(`frontmatter.policy_flags must include ${key}`);
      }
    }
  }

  const allowedTools = normalizeFrontmatterList(normalizedFrontmatter.allowed_tools);
  const blockedTools = normalizeFrontmatterList(normalizedFrontmatter.blocked_tools);
  if (normalizedFrontmatter.allowed_tools !== undefined && !Array.isArray(allowedTools)) {
    warnings.push("frontmatter.allowed_tools should be a list.");
  }
  if (normalizedFrontmatter.blocked_tools !== undefined && !Array.isArray(blockedTools)) {
    warnings.push("frontmatter.blocked_tools should be a list.");
  }

  return {
    errors,
    warnings,
  };
}

function validateProfilePackText(fileName, rawText, options = {}) {
  const parsed = parseProfilePackDocument(rawText);
  const text = String(parsed?.content || "").trim();
  const frontmatter = parsed?.frontmatter || null;
  const errors = [];
  const warnings = [];
  const isRequired = options.isRequired === true;
  const definition =
    options?.definition && typeof options.definition === "object"
      ? options.definition
      : null;

  if (!text) {
    errors.push("Profile pack is empty.");
    return { file: fileName, ok: false, errors, warnings };
  }

  if (isRequired && parsed?.hasFrontmatter !== true) {
    errors.push("Required profile pack must include YAML frontmatter.");
  }
  if (frontmatter && typeof frontmatter === "object") {
    const contract = validateProfileFrontmatterContract(frontmatter, definition, {
      isRequired,
    });
    errors.push(...contract.errors);
    warnings.push(...contract.warnings);
  }

  const firstLine = String(text.split("\n")[0] || "").trim();
  if (!firstLine.startsWith("# ")) {
    errors.push("First line must be a markdown H1 heading.");
  }

  const lines = text.split("\n").map((line) => line.trim()).filter(Boolean);
  if (lines.length < 3) {
    warnings.push("Profile pack is very short; consider adding richer guidance.");
  }

  if (isRequired && text.length < 80) {
    warnings.push("Required profile pack is unusually short.");
  }
  if (isRequired && !/\n##\s+purpose\b/i.test(`\n${text}`)) {
    errors.push('Required profile pack must include a "## Purpose" section.');
  }
  if (
    isRequired &&
    !/\n##\s+(safety|safety rules|safety boundaries|boundaries)\b/i.test(
      `\n${text}`,
    )
  ) {
    warnings.push(
      'Required profile pack should include a dedicated safety/boundaries section heading.',
    );
  }

  const normalized = text.toLowerCase();
  const hasSafetySignal =
    normalized.includes("safe") ||
    normalized.includes("safety") ||
    normalized.includes("policy") ||
    normalized.includes("boundary") ||
    normalized.includes("avoid") ||
    normalized.includes("do not") ||
    normalized.includes("never");
  if (!hasSafetySignal) {
    warnings.push(
      "No explicit safety/policy language found. Consider adding guardrails.",
    );
  }

  return { file: fileName, ok: errors.length === 0, errors, warnings };
}

function sanitizeEnumList(value = []) {
  if (!Array.isArray(value)) return [];
  return value
    .map((entry) => String(entry || "").trim().toLowerCase())
    .filter(Boolean);
}

function validateProfileDefinitionContract(profileKey = "", definition = {}) {
  const errors = [];
  const warnings = [];
  const key = String(profileKey || "").trim().toLowerCase();
  const id = String(definition?.id || "").trim().toLowerCase();

  if (!id) errors.push("Missing required field: id");
  if (key && id && key !== id) {
    errors.push(`Definition id "${id}" must match key "${key}"`);
  }

  const requiredStringFields = [
    "flowType",
    "objectiveTag",
    "marker",
    "defaultFirstMessage",
    "contextKey",
    "safeFallback",
  ];
  for (const field of requiredStringFields) {
    const value = String(definition?.[field] || "").trim();
    if (!value) {
      errors.push(`Missing required field: ${field}`);
    }
  }

  const enumFields = ["stageEnum", "vibeEnum", "goalEnum"];
  for (const field of enumFields) {
    const values = sanitizeEnumList(definition?.[field]);
    if (!values.length) {
      errors.push(`${field} must contain at least one value`);
      continue;
    }
    if (new Set(values).size !== values.length) {
      errors.push(`${field} contains duplicate values`);
    }
  }

  if (String(definition?.safeFallback || "").trim().length < 40) {
    warnings.push("safeFallback is short; consider a clearer policy-safe fallback.");
  }
  if (!String(definition?.defaultFirstMessage || "").trim()) {
    warnings.push("defaultFirstMessage should be non-empty for predictable greeting behavior.");
  }

  const policy =
    definition?.policy && typeof definition.policy === "object"
      ? definition.policy
      : null;
  if (!policy) {
    errors.push("Missing required field: policy");
  } else {
    for (const keyName of PROFILE_POLICY_KEYS) {
      if (typeof policy[keyName] !== "boolean") {
        errors.push(`policy.${keyName} must be a boolean`);
      }
    }
  }

  return {
    profile: id || key || "unknown",
    ok: errors.length === 0,
    errors,
    warnings,
  };
}

function validateProfileDefinitions() {
  const checks = [];
  const errors = [];
  const warnings = [];
  const objectiveTagOwners = new Map();
  const flowTypeOwners = new Map();

  for (const [profileKey, definition] of Object.entries(PROFILE_DEFINITIONS)) {
    const result = validateProfileDefinitionContract(profileKey, definition);
    checks.push(result);
    errors.push(...result.errors.map((entry) => `${profileKey}: ${entry}`));
    warnings.push(...result.warnings.map((entry) => `${profileKey}: ${entry}`));

    const objectiveTag = String(definition?.objectiveTag || "").trim().toLowerCase();
    if (objectiveTag) {
      const owner = objectiveTagOwners.get(objectiveTag);
      if (owner && owner !== profileKey) {
        errors.push(
          `Duplicate objectiveTag "${objectiveTag}" shared by ${owner} and ${profileKey}`,
        );
      } else {
        objectiveTagOwners.set(objectiveTag, profileKey);
      }
    }

    const flowType = String(definition?.flowType || "").trim().toLowerCase();
    if (flowType) {
      const owner = flowTypeOwners.get(flowType);
      if (owner && owner !== profileKey) {
        errors.push(
          `Duplicate flowType "${flowType}" shared by ${owner} and ${profileKey}`,
        );
      } else {
        flowTypeOwners.set(flowType, profileKey);
      }
    }
  }

  return {
    ok: errors.length === 0,
    checked_profiles: checks.length,
    errors,
    warnings,
    checks,
  };
}

function validateProfilePacks(options = {}) {
  const strict = options.strict === true;
  const failOnWarnings = options.failOnWarnings === true;
  const includeAuxiliary = options.includeAuxiliary !== false;
  const profileDir = getProfilesDirectory();
  const files = listProfilePackFiles();
  const fileSet = new Set(files);
  const checks = [];
  const errors = [];
  const warnings = [];
  const definitionValidation = validateProfileDefinitions();

  if (!definitionValidation.ok) {
    errors.push(...definitionValidation.errors);
  }
  if (definitionValidation.warnings.length) {
    warnings.push(...definitionValidation.warnings);
  }

  for (const definition of Object.values(PROFILE_DEFINITIONS)) {
    const fileName = `${definition.id}.md`;
    const filePath = path.join(profileDir, fileName);
    if (!fileSet.has(fileName)) {
      const message = `Missing required profile pack: ${fileName}`;
      checks.push({
        file: fileName,
        ok: false,
        errors: [message],
        warnings: [],
      });
      errors.push(message);
      continue;
    }
    const content = fs.readFileSync(filePath, "utf8");
    const result = validateProfilePackText(fileName, content, {
      isRequired: true,
      definition,
    });
    checks.push(result);
    errors.push(...result.errors.map((entry) => `${fileName}: ${entry}`));
    warnings.push(...result.warnings.map((entry) => `${fileName}: ${entry}`));
  }

  if (includeAuxiliary) {
    for (const fileName of files) {
      const knownRequired = Object.values(PROFILE_DEFINITIONS).some(
        (definition) => `${definition.id}.md` === fileName,
      );
      if (knownRequired) continue;
      const filePath = path.join(profileDir, fileName);
      const content = fs.readFileSync(filePath, "utf8");
      const result = validateProfilePackText(fileName, content, { isRequired: false });
      checks.push(result);
      errors.push(...result.errors.map((entry) => `${fileName}: ${entry}`));
      warnings.push(...result.warnings.map((entry) => `${fileName}: ${entry}`));
    }
  }

  const ok = errors.length === 0 && (!strict || !failOnWarnings || warnings.length === 0);
  return {
    ok,
    strict,
    fail_on_warnings: failOnWarnings,
    checked_files: checks.length,
    required_profiles: Object.keys(PROFILE_DEFINITIONS).length,
    errors,
    warnings,
    checks,
    profile_definition_validation: definitionValidation,
  };
}

function deriveProfilePackVersion(definition = {}) {
  const explicitVersion = String(definition?.packVersion || "").trim();
  if (explicitVersion) {
    return explicitVersion.toLowerCase().startsWith("v")
      ? explicitVersion
      : `v${explicitVersion}`;
  }
  const marker = String(definition?.marker || "");
  const markerMatch = marker.match(/_v([0-9][a-z0-9._-]*)\]/i);
  if (markerMatch?.[1]) {
    return `v${markerMatch[1]}`;
  }
  return "v1";
}

function deriveProfileContractVersion(definition = {}) {
  const explicitVersion = String(definition?.contractVersion || "").trim();
  if (!explicitVersion) return "c1";
  return explicitVersion.toLowerCase().startsWith("c")
    ? explicitVersion
    : `c${explicitVersion}`;
}

function normalizeProfilePackVersion(value, fallback = "v1") {
  const normalized = String(value || "").trim();
  if (!normalized) return fallback;
  if (!/^v[0-9][a-z0-9._-]*$/i.test(normalized)) return fallback;
  return normalized;
}

function normalizeProfileContractVersion(value, fallback = "c1") {
  const normalized = String(value || "").trim();
  if (!normalized) return fallback;
  if (!/^c[0-9][a-z0-9._-]*$/i.test(normalized)) return fallback;
  return normalized;
}

function computeProfilePackChecksum(definition = {}, profilePack = "") {
  const payload = [
    String(definition?.id || ""),
    String(definition?.marker || ""),
    String(profilePack || ""),
  ].join("|");
  return crypto.createHash("sha256").update(payload).digest("hex").slice(0, 16);
}

function getProfileRuntimeContract(profileType) {
  const definition = getProfileDefinition(profileType);
  if (!definition) {
    return {
      id: DEFAULT_PROFILE_TYPE,
      profilePack: "",
      packVersion: null,
      contractVersion: null,
      defaultFirstMessage: "",
      safeFallback:
        "I can continue in a safe and respectful way with a clear next step.",
      responseConstraints: PROFILE_RESPONSE_CONSTRAINTS.general,
      policyFlags: [...MANDATORY_POLICY_FLAGS],
      allowedTools: [],
      blockedTools: [],
      frontmatter: null,
    };
  }

  const document = loadProfilePackDocument(definition.id, `${definition.id} profile pack`);
  const frontmatter =
    document?.frontmatter && typeof document.frontmatter === "object"
      ? document.frontmatter
      : null;

  const responseConstraints = getProfileResponseConstraints(definition.id, frontmatter);
  const packVersion = normalizeProfilePackVersion(
    frontmatter?.pack_version,
    deriveProfilePackVersion(definition),
  );
  const contractVersion = normalizeProfileContractVersion(
    frontmatter?.contract_version,
    deriveProfileContractVersion(definition),
  );
  const defaultFirstMessage = String(
    frontmatter?.default_first_message || definition.defaultFirstMessage || "",
  ).trim();
  const safeFallback = String(
    frontmatter?.safe_fallback || definition.safeFallback || "",
  ).trim();

  return {
    id: definition.id,
    profilePack: String(document?.content || "").trim(),
    packVersion,
    contractVersion,
    defaultFirstMessage,
    safeFallback: safeFallback || definition.safeFallback,
    responseConstraints,
    policyFlags: normalizeFrontmatterList(frontmatter?.policy_flags).map((entry) =>
      String(entry || "").trim().toLowerCase(),
    ),
    allowedTools: normalizeFrontmatterList(frontmatter?.allowed_tools).map((entry) =>
      String(entry || "").trim().toLowerCase(),
    ),
    blockedTools: normalizeFrontmatterList(frontmatter?.blocked_tools).map((entry) =>
      String(entry || "").trim().toLowerCase(),
    ),
    frontmatter,
  };
}

const PROFILE_DEFINITIONS = Object.freeze({
  dating: {
    id: "dating",
    flowType: "dating",
    objectiveTag: "dating_engagement",
    marker: "[profile_dating_v2]",
    defaultFirstMessage: "Hi babe, how are you doing?",
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
  const contract = getProfileRuntimeContract(profileType);
  return String(contract.profilePack || "").trim();
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
      profilePackVersion: null,
      profilePackChecksum: null,
      profileContractVersion: null,
      profileResponseConstraints: PROFILE_RESPONSE_CONSTRAINTS.general,
    };
  }

  const profileContract = getProfileRuntimeContract(definition.id);
  const profilePack = profileContract.profilePack;
  const profilePackVersion = profileContract.packVersion;
  const profilePackChecksum = computeProfilePackChecksum(definition, profilePack);
  const profileContractVersion = profileContract.contractVersion;
  const profileResponseConstraints = profileContract.responseConstraints;

  if (basePrompt.includes(definition.marker)) {
    return {
      prompt: basePrompt,
      firstMessage: firstMessage || profileContract.defaultFirstMessage,
      applied: false,
      profileType: definition.id,
      profilePackVersion,
      profilePackChecksum,
      profileContractVersion,
      profileResponseConstraints,
    };
  }

  const mergedPrompt = [
    basePrompt,
    definition.marker,
    `Relationship profile type: ${definition.id}`,
    profilePack,
    buildPlatformToneDialBlock(),
    `Response constraints: max ${profileResponseConstraints.maxChars} spoken characters and at most ${profileResponseConstraints.maxQuestions} direct question per turn.`,
    "Policy gates: anti-impersonation, anti-harassment, anti-coercion, anti-money-pressure. If triggered, return a safe fallback response.",
  ]
    .filter(Boolean)
    .join("\n\n");

  return {
    prompt: mergedPrompt,
    firstMessage: firstMessage || profileContract.defaultFirstMessage,
    applied: true,
    profileType: definition.id,
    profilePackVersion,
    profilePackChecksum,
    profileContractVersion,
    profileResponseConstraints,
  };
}

function normalizeEnumValue(value, allowed, fallbackValue) {
  const normalized = String(value || "").trim().toLowerCase();
  if (allowed.includes(normalized)) {
    return normalized;
  }
  return fallbackValue;
}

function getProfileResponseConstraints(profileType, frontmatter = null) {
  const normalized = normalizeProfileType(profileType, DEFAULT_PROFILE_TYPE);
  const fallback =
    PROFILE_RESPONSE_CONSTRAINTS[normalized] || PROFILE_RESPONSE_CONSTRAINTS.general;
  const metadata =
    frontmatter && typeof frontmatter === "object" ? frontmatter : {};

  const maxChars = Number(metadata.max_chars);
  const maxQuestions = Number(metadata.max_questions);
  return {
    maxChars:
      Number.isFinite(maxChars) && maxChars >= 80 && maxChars <= 500
        ? Math.floor(maxChars)
        : Number(fallback.maxChars),
    maxQuestions:
      Number.isFinite(maxQuestions) && maxQuestions >= 0 && maxQuestions <= 3
        ? Math.floor(maxQuestions)
        : Number(fallback.maxQuestions),
  };
}

function resolveStageTransition(definition = {}, requestedStage = "", previousStage = "") {
  const stages = sanitizeEnumList(definition?.stageEnum);
  if (!stages.length) {
    return {
      stage: "",
      transition: {
        requested: String(requestedStage || "").trim().toLowerCase(),
        previous: String(previousStage || "").trim().toLowerCase(),
        resolved: "",
        blocked: false,
        reason: "missing_stage_enum",
      },
    };
  }

  const requested = normalizeEnumValue(requestedStage, stages, "");
  const hasPreviousStage = String(previousStage || "").trim() !== "";
  const previous = normalizeEnumValue(previousStage, stages, stages[0]);
  if (!requested) {
    return {
      stage: previous,
      transition: {
        requested: String(requestedStage || "").trim().toLowerCase(),
        previous,
        resolved: previous,
        blocked: false,
        reason: "requested_stage_missing",
      },
    };
  }
  if (!hasPreviousStage) {
    return {
      stage: requested,
      transition: {
        requested,
        previous: null,
        resolved: requested,
        blocked: false,
        reason: "initial_stage_set",
      },
    };
  }

  const requestedIndex = stages.indexOf(requested);
  const previousIndex = stages.indexOf(previous);
  if (requestedIndex === -1 || previousIndex === -1) {
    return {
      stage: previous,
      transition: {
        requested,
        previous,
        resolved: previous,
        blocked: true,
        reason: "invalid_stage_value",
      },
    };
  }

  const delta = requestedIndex - previousIndex;
  if (Math.abs(delta) <= 1) {
    return {
      stage: requested,
      transition: {
        requested,
        previous,
        resolved: requested,
        blocked: false,
        reason: "allowed_neighbor_transition",
      },
    };
  }

  const resolvedIndex =
    delta > 0
      ? Math.min(previousIndex + 1, stages.length - 1)
      : Math.max(previousIndex - 1, 0);
  const resolvedStage = stages[resolvedIndex];
  return {
    stage: resolvedStage,
    transition: {
      requested,
      previous,
      resolved: resolvedStage,
      blocked: true,
      reason: "stage_jump_limited",
    },
  };
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

  const stageDecision = resolveStageTransition(
    definition,
    input.stage,
    previous.stage,
  );

  return {
    profile_type: definition.id,
    stage: stageDecision.stage,
    stage_requested: String(input.stage || "").trim() || null,
    stage_transition: stageDecision.transition,
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

function normalizeToolName(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, "_");
}

function toToolSet(value) {
  if (!Array.isArray(value)) return new Set();
  return new Set(value.map((entry) => normalizeToolName(entry)).filter(Boolean));
}

function pickToolPolicyConfig(context = {}) {
  const runtimeConfig =
    context?.callConfig && typeof context.callConfig === "object"
      ? context.callConfig
      : {};
  const candidates = [
    context?.toolPolicy,
    runtimeConfig?.tool_policy,
    runtimeConfig?.relationship_profile?.tool_policy,
    runtimeConfig?.script_policy?.tool_policy,
  ];
  for (const candidate of candidates) {
    if (
      candidate &&
      typeof candidate === "object" &&
      !Array.isArray(candidate)
    ) {
      return candidate;
    }
  }
  return null;
}

function evaluateProfileToolPolicy(
  profileType,
  toolRequest = {},
  context = {},
) {
  const normalizedProfile = normalizeProfileType(profileType, DEFAULT_PROFILE_TYPE);
  const definition =
    getProfileDefinition(normalizedProfile, DEFAULT_PROFILE_TYPE) ||
    getProfileDefinition(DEFAULT_PROFILE_TYPE, DEFAULT_PROFILE_TYPE);
  const toolName = normalizeToolName(
    toolRequest.toolName ||
      toolRequest.tool_name ||
      toolRequest.name ||
      toolRequest.functionName ||
      "",
  );
  const args =
    toolRequest.args && typeof toolRequest.args === "object"
      ? toolRequest.args
      : {};
  const configPolicy = pickToolPolicyConfig(context);
  const blockedTools = toToolSet(configPolicy?.blocked_tools);
  const allowedTools = toToolSet(configPolicy?.allowed_tools);

  const deny = (reason, message, extras = {}) => ({
    allowed: false,
    action: "deny",
    code: "tool_policy_blocked",
    reason,
    message,
    profile_type: definition?.id || normalizedProfile || DEFAULT_PROFILE_TYPE,
    blocked: [reason],
    metadata: {
      source: "profile_registry",
      tool: toolName || null,
      ...extras,
    },
  });

  if (!toolName) {
    return {
      allowed: true,
      action: "allow",
      code: "ok",
      reason: "missing_tool_name",
      message: null,
      profile_type: definition?.id || normalizedProfile || DEFAULT_PROFILE_TYPE,
      blocked: [],
      metadata: { source: "profile_registry" },
    };
  }

  if (allowedTools.size > 0 && !allowedTools.has(toolName)) {
    return deny(
      "tool_not_in_allow_list",
      `Tool ${toolName} is not allowed for this profile context.`,
      { allow_list_enforced: true },
    );
  }

  if (blockedTools.has(toolName)) {
    return deny(
      "tool_in_block_list",
      `Tool ${toolName} is blocked for this profile context.`,
      { block_list_enforced: true },
    );
  }

  const moneyTensionActive =
    context?.callConfig?.money_tension_active === true ||
    context?.callConfig?.relationship_profile?.money_tension_active === true;
  if (
    definition?.policy?.antiMoneyPressure &&
    toolName === "start_payment" &&
    moneyTensionActive
  ) {
    return deny(
      "money_pressure_guard",
      "Payment actions are blocked while money tension is active in this profile.",
    );
  }

  if (
    definition?.policy?.antiCoercion &&
    (args.force_now === true || args.force === true || args.require_now === true)
  ) {
    return deny(
      "coercion_guard",
      "Forced actions are blocked by profile safety policy.",
    );
  }

  return {
    allowed: true,
    action: "allow",
    code: "ok",
    reason: "allowed",
    message: null,
    profile_type: definition?.id || normalizedProfile || DEFAULT_PROFILE_TYPE,
    blocked: [],
    metadata: {
      source: "profile_registry",
      tool: toolName,
      policy_present: configPolicy != null,
    },
  };
}

function applyProfilePolicyGates(rawText = "", profileType = DEFAULT_PROFILE_TYPE) {
  const text = String(rawText || "").trim();
  if (!text) {
    return {
      text: "",
      replaced: false,
      blocked: [],
      risk_level: "none",
      action: "allow",
      findings: [],
    };
  }

  const definition = getProfileDefinition(profileType);
  if (!definition) {
    return {
      text,
      replaced: false,
      blocked: [],
      risk_level: "none",
      action: "allow",
      findings: [],
    };
  }
  const runtimeContract = getProfileRuntimeContract(definition.id);

  const lower = text.toLowerCase();
  const findings = [];
  const addFinding = (rule, signal) => {
    findings.push({ rule, signal });
  };

  if (definition.policy?.antiImpersonation) {
    const impersonationPatterns = [
      /\b(i am|i'm|this is)\s+(the\s+)?(real\s+)?(celebrity|artist|influencer|creator)\b/i,
      /\bthis is personally\b/i,
      /\bi('?m| am)\s+the\s+artist\b/i,
      /\bit('?s| is)\s+me,\s*(the\s+)?(celebrity|artist)\b/i,
      /\bofficially\s+me\b/i,
    ];
    if (impersonationPatterns.some((pattern) => pattern.test(text))) {
      addFinding("anti_impersonation", "impersonation_phrase");
    }
  }

  if (definition.policy?.antiHarassment) {
    const harassmentTerms = ["idiot", "stupid", "loser", "worthless", "shut up", "moron"];
    if (harassmentTerms.some((term) => lower.includes(term))) {
      addFinding("anti_harassment", "abusive_language");
    }
  }

  if (definition.policy?.antiCoercion) {
    const coercionTerms = [
      "or else",
      "you must",
      "no choice",
      "if you do not",
      "do it now",
      "you better",
      "last warning",
      "don't make me",
      "or there will be consequences",
    ];
    if (coercionTerms.some((term) => lower.includes(term))) {
      addFinding("anti_coercion", "coercive_phrase");
    }
  }

  if (definition.policy?.antiMoneyPressure) {
    const moneyPressurePatterns = [
      /\b(send|wire|transfer|pay)\s+(me\s+)?(money|funds)\s*(now|immediately|right now)?\b/i,
      /\b(cashapp|venmo|zelle|paypal)\s+(me|now)\b/i,
      /\b(pay|send)\s+(by\s+)?gift\s*card(s)?\b/i,
      /\b(crypto|bitcoin|btc|usdt|eth)\s+(transfer|payment|now)\b/i,
      /\bpay\s+immediately\b/i,
    ];
    if (moneyPressurePatterns.some((pattern) => pattern.test(text))) {
      addFinding("anti_money_pressure", "money_pressure_phrase");
    }
  }

  const blocked = Array.from(new Set(findings.map((entry) => entry.rule)));
  const riskWeights = {
    anti_impersonation: 3,
    anti_coercion: 3,
    anti_money_pressure: 2,
    anti_harassment: 2,
    response_length_guard: 1,
    question_density_guard: 1,
  };
  const riskScore = blocked.reduce(
    (total, rule) => total + Number(riskWeights[rule] || 1),
    0,
  );
  const riskLevel = blocked.length
    ? riskScore >= 3
      ? "high"
      : "medium"
    : "none";
  const action = blocked.length ? "fallback" : "allow";

  if (!blocked.length) {
    const constraints = runtimeContract.responseConstraints;
    let constrainedText = text;
    const constrainedFindings = [];
    const maxChars = Number(constraints?.maxChars);
    if (Number.isFinite(maxChars) && maxChars > 0 && constrainedText.length > maxChars) {
      let sliced = constrainedText.slice(0, maxChars);
      const boundary = Math.max(
        sliced.lastIndexOf("."),
        sliced.lastIndexOf("!"),
        sliced.lastIndexOf("?"),
        sliced.lastIndexOf(","),
        sliced.lastIndexOf(" "),
      );
      if (boundary >= 80) {
        sliced = sliced.slice(0, boundary + 1);
      }
      constrainedText = sliced.trim();
      constrainedFindings.push({
        rule: "response_length_guard",
        signal: "max_chars_exceeded",
      });
    }
    const maxQuestions = Number(constraints?.maxQuestions);
    if (Number.isFinite(maxQuestions) && maxQuestions >= 0) {
      let seenQuestions = 0;
      let questionTrimmed = false;
      let next = "";
      for (const char of constrainedText) {
        if (char === "?") {
          seenQuestions += 1;
          if (seenQuestions > maxQuestions) {
            next += ".";
            questionTrimmed = true;
            continue;
          }
        }
        next += char;
      }
      if (questionTrimmed) {
        constrainedText = next;
        constrainedFindings.push({
          rule: "question_density_guard",
          signal: "too_many_questions",
        });
      }
    }
    if (constrainedFindings.length > 0) {
      return {
        text: constrainedText,
        replaced: constrainedText !== text,
        blocked: constrainedFindings.map((entry) => entry.rule),
        risk_level: "low",
        action: "sanitize",
        findings: constrainedFindings,
      };
    }
    return {
      text,
      replaced: false,
      blocked,
      risk_level: riskLevel,
      action,
      findings,
    };
  }

  return {
    text: runtimeContract.safeFallback,
    replaced: true,
    blocked,
    risk_level: riskLevel,
    action,
    findings,
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
  parseProfilePackDocument,
  loadProfilePackDocument,
  getProfileRuntimeContract,
  getProfilePack,
  listProfilePackFiles,
  validateProfilePacks,
  validateProfileDefinitions,
  buildProfilePromptBundle,
  buildRelationshipContext,
  getProfileResponseConstraints,
  getProfilePolicy,
  evaluateProfileToolPolicy,
  applyProfilePolicyGates,
};
