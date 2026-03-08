#!/usr/bin/env node

const { validateRelationshipProfilePacks } = require("../functions/Dating");

function hasFlag(name) {
  return process.argv.includes(name);
}

function main() {
  const strict = hasFlag("--strict");
  const failOnWarnings = hasFlag("--fail-on-warn") || hasFlag("--fail-on-warnings");
  const includeAuxiliary = !hasFlag("--required-only");

  const result = validateRelationshipProfilePacks({
    strict,
    failOnWarnings,
    includeAuxiliary,
  });

  const headline = `Profile pack validation: checked=${result.checked_files}, required=${result.required_profiles}, warnings=${result.warnings.length}, errors=${result.errors.length}`;
  if (result.ok) {
    console.log(`✅ ${headline}`);
    if (result.warnings.length) {
      result.warnings.slice(0, 20).forEach((entry) => console.log(`⚠️ ${entry}`));
    }
    process.exit(0);
  }

  console.error(`❌ ${headline}`);
  result.errors.slice(0, 50).forEach((entry) => console.error(`- ${entry}`));
  if (result.warnings.length) {
    console.error("Warnings:");
    result.warnings.slice(0, 50).forEach((entry) => console.error(`- ${entry}`));
  }
  process.exit(1);
}

main();
