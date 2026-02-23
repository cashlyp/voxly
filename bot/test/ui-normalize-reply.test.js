"use strict";

const assert = require("assert");
const { normalizeReply } = require("../utils/ui");

function testNormalizeReplyDoesNotAutoMarkdown() {
  const normalized = normalizeReply("hello_world");
  assert.strictEqual(normalized.options.parse_mode, undefined);
}

function testNormalizeReplyAutoHtmlWhenTagsPresent() {
  const normalized = normalizeReply("<b>Hello</b>");
  assert.strictEqual(normalized.options.parse_mode, "HTML");
}

function testNormalizeReplyAutoMarkdownForStyledSectionText() {
  const normalized = normalizeReply("*Title*\nBody");
  assert.strictEqual(normalized.options.parse_mode, "Markdown");
}

testNormalizeReplyDoesNotAutoMarkdown();
testNormalizeReplyAutoHtmlWhenTagsPresent();
testNormalizeReplyAutoMarkdownForStyledSectionText();
