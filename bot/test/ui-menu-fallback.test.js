"use strict";

const assert = require("assert");
const { __testables } = require("../utils/ui");

function testDetectsEntityParseError() {
  const parseError = new Error(
    "Call to 'sendMessage' failed! (400: Bad Request: can't parse entities: Can't find end of the entity starting at byte offset 76)",
  );
  assert.strictEqual(__testables.isEntityParseError(parseError), true);
  assert.strictEqual(__testables.isEntityParseError(new Error("network timeout")), false);
}

function testConvertsMarkdownPromptToSafeHtmlText() {
  const input = "☎️ *Call Script Designer*\nFilter: [foo_bar](https://example.com)";
  const output = __testables.toHtmlSafeMenuText(input);
  assert.ok(!output.includes("*"));
  assert.ok(!output.includes("[foo_bar]("));
  assert.ok(output.includes("foo_bar (https://example.com)"));
}

testDetectsEntityParseError();
testConvertsMarkdownPromptToSafeHtmlText();
