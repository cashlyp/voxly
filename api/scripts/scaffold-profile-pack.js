#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

function parseArgs(argv = []) {
  const flags = {};
  const positionals = [];
  for (let i = 0; i < argv.length; i += 1) {
    const token = String(argv[i] || "").trim();
    if (!token) continue;
    if (!token.startsWith("--")) {
      positionals.push(token);
      continue;
    }
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || String(next).startsWith("--")) {
      flags[key] = "1";
      continue;
    }
    flags[key] = String(next).trim();
    i += 1;
  }
  return { flags, positionals };
}

function boolFlag(value) {
  if (value === undefined || value === null) return false;
  const normalized = String(value).trim().toLowerCase();
  return ["1", "true", "yes", "on"].includes(normalized);
}

function normalizeProfileId(raw = "") {
  return String(raw || "").trim().toLowerCase();
}

function isValidProfileId(profileId = "") {
  return /^[a-z][a-z0-9_]*$/.test(profileId);
}

function ensureDir(targetPath) {
  fs.mkdirSync(targetPath, { recursive: true });
}

function writeFileSafe(filePath, content, { force = false } = {}) {
  if (!force && fs.existsSync(filePath)) {
    throw new Error(`File already exists: ${filePath}`);
  }
  fs.writeFileSync(filePath, content, "utf8");
}

function buildPrimaryTemplate(profileId) {
  return `---
id: ${profileId}
pack_version: v1
contract_version: c1
objective_tag: ${profileId}_engagement
flow_type: ${profileId}
default_first_message: "Hi, this is the ${profileId.replace(/_/g, " ")} assistant."
safe_fallback: "I can continue safely and respectfully with a clear next step."
max_chars: 220
max_questions: 1
policy_flags: [anti_impersonation, anti_harassment, anti_coercion, anti_money_pressure]
allowed_tools: []
blocked_tools: []
---

# ${profileId.replace(/_/g, " ").replace(/\b\w/g, (m) => m.toUpperCase())} Flow Pack

## Purpose
Describe the operational behavior for this profile.

## Safety Boundaries
- Keep communication transparent and policy-safe.
- Avoid coercion, harassment, and money pressure.
- Do not impersonate real private individuals.

## Voice Style
- concise, natural spoken phrasing
- one clear move per turn
- professional and respectful
`;
}

function buildCompanionTemplate(profileId) {
  return `# ${profileId.replace(/_/g, " ").replace(/\b\w/g, (m) => m.toUpperCase())} Companion Style Layer

## Purpose
This file is the companion style layer for \`${profileId}.md\`.
It standardizes tone and structure, while routing and policy remain in \`${profileId}.md\`.

## Compatibility
Works with:
- \`${profileId}.md\`

If there is any conflict, \`${profileId}.md\` wins.

## Voice Style
- concise and natural spoken phrasing
- one clear move per turn
- safe, respectful, non-coercive language
`;
}

function main() {
  const { flags, positionals } = parseArgs(process.argv.slice(2));
  const profileId = normalizeProfileId(flags.id || positionals[0] || "");
  const force = boolFlag(flags.force);

  if (!profileId) {
    console.error(
      "Usage: node scripts/scaffold-profile-pack.js <profile_id> [--force]\nExample: node scripts/scaffold-profile-pack.js travel_agent",
    );
    process.exit(2);
    return;
  }
  if (!isValidProfileId(profileId)) {
    console.error(
      `Invalid profile id "${profileId}". Use lowercase letters, numbers, and underscores only, starting with a letter.`,
    );
    process.exit(2);
    return;
  }

  const profilesRoot = path.join(__dirname, "..", "functions", "profiles");
  const profileDir = path.join(profilesRoot, profileId);
  const primaryPath = path.join(profileDir, `${profileId}.md`);
  const companionPath = path.join(profileDir, "profile.md");

  ensureDir(profileDir);
  writeFileSafe(primaryPath, buildPrimaryTemplate(profileId), { force });
  writeFileSafe(companionPath, buildCompanionTemplate(profileId), { force });

  console.log(`✅ Scaffolded profile pack for "${profileId}"`);
  console.log(`- ${primaryPath}`);
  console.log(`- ${companionPath}`);
  console.log("Next steps:");
  console.log("1) Fill in profile-specific content in both files.");
  console.log("2) Add profile definition in api/functions/profileRegistry.js if needed.");
  console.log("3) Run: npm --prefix api run validate:profiles");
}

main();
