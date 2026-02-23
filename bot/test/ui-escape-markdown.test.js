"use strict";

const assert = require("assert");
const { escapeMarkdown } = require("../utils/ui");

function testEscapeMarkdownEscapesSpecialCharacters() {
  const input = "@john_doe*(test)";
  const escaped = escapeMarkdown(input);
  assert.strictEqual(escaped, "@john\\_doe\\*\\(test\\)");
}

testEscapeMarkdownEscapesSpecialCharacters();
