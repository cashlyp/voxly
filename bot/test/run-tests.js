"use strict";

const fs = require("fs");
const path = require("path");

const testDir = __dirname;
const testFiles = fs
  .readdirSync(testDir, { withFileTypes: true })
  .filter((entry) => entry.isFile() && entry.name.endsWith(".test.js"))
  .map((entry) => entry.name)
  .sort();

if (testFiles.length === 0) {
  console.error("No bot test files found in bot/test.");
  process.exit(1);
}

let failures = 0;

for (const file of testFiles) {
  const fullPath = path.join(testDir, file);
  try {
    require(fullPath);
    console.log(`PASS ${file}`);
  } catch (error) {
    failures += 1;
    console.error(`FAIL ${file}`);
    console.error(error?.stack || error?.message || String(error));
  }
}

if (failures > 0) {
  process.exit(1);
}

console.log(`Ran ${testFiles.length} bot test file(s).`);
