#!/usr/bin/env node

const { validateRelationshipProfilePacks } = require("../functions/Dating");
const { runProfileRuntimeSmoke } = require("../functions/profileRegistry");

function hasFlag(name) {
  return process.argv.includes(name);
}

function main() {
  const strict = hasFlag("--strict");
  const failOnWarnings = hasFlag("--fail-on-warn") || hasFlag("--fail-on-warnings");
  const includeAuxiliary = !hasFlag("--required-only");
  const skipRuntimeSmoke = hasFlag("--skip-runtime-smoke");

  const result = validateRelationshipProfilePacks({
    strict,
    failOnWarnings,
    includeAuxiliary,
  });
  const runtimeSmoke = skipRuntimeSmoke
    ? {
        ok: true,
        checked_profiles: 0,
        errors: [],
        warnings: [],
      }
    : runProfileRuntimeSmoke({});

  const totalWarnings = result.warnings.length + runtimeSmoke.warnings.length;
  const totalErrors = result.errors.length + runtimeSmoke.errors.length;
  const headline = `Profile pack validation: checked=${result.checked_files}, required=${result.required_profiles}, runtime_smoke=${runtimeSmoke.checked_profiles}, warnings=${totalWarnings}, errors=${totalErrors}`;
  if (result.ok && runtimeSmoke.ok) {
    console.log(`✅ ${headline}`);
    if (result.warnings.length) {
      result.warnings.slice(0, 20).forEach((entry) => console.log(`⚠️ ${entry}`));
    }
    if (runtimeSmoke.warnings.length) {
      runtimeSmoke.warnings.slice(0, 20).forEach((entry) => console.log(`⚠️ ${entry}`));
    }
    process.exit(0);
  }

  console.error(`❌ ${headline}`);
  result.errors.slice(0, 50).forEach((entry) => console.error(`- ${entry}`));
  runtimeSmoke.errors.slice(0, 50).forEach((entry) => console.error(`- ${entry}`));
  if (result.warnings.length) {
    console.error("Warnings:");
    result.warnings.slice(0, 50).forEach((entry) => console.error(`- ${entry}`));
  }
  if (runtimeSmoke.warnings.length) {
    console.error("Runtime warnings:");
    runtimeSmoke.warnings.slice(0, 50).forEach((entry) => console.error(`- ${entry}`));
  }
  process.exit(1);
}

main();
