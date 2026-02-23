"use strict";

const assert = require("assert");
const {
  hasActiveConversation,
  isSafeCallIdentifier,
  __testables,
} = require("../utils/runtimeGuards");

function testHasActiveConversation() {
  assert.strictEqual(hasActiveConversation(null), false);
  assert.strictEqual(hasActiveConversation({}), false);
  assert.strictEqual(
    hasActiveConversation({
      conversation: { active: () => [] },
    }),
    false,
  );
  assert.strictEqual(
    hasActiveConversation({
      conversation: { active: () => ["call-conversation"] },
    }),
    true,
  );
  assert.strictEqual(
    hasActiveConversation({
      conversation: { active: () => ({ "scripts-conversation": 1 }) },
    }),
    true,
  );
  assert.strictEqual(
    hasActiveConversation({
      conversation: { active: () => ({ "scripts-conversation": 0 }) },
    }),
    false,
  );
  assert.strictEqual(
    hasActiveConversation({
      session: {
        conversation: { "scripts-conversation": { cursor: "abc" } },
      },
    }),
    true,
  );
  assert.strictEqual(
    hasActiveConversation({
      session: {
        conversation: {},
      },
      conversation: { active: () => ({}) },
    }),
    false,
  );
  assert.strictEqual(
    hasActiveConversation({
      session: {
        currentOp: { id: "op-123" },
      },
      conversation: { active: () => ({}) },
    }),
    true,
  );
  assert.strictEqual(
    hasActiveConversation({
      conversation: { active: () => Promise.resolve(["scripts-conversation"]) },
    }),
    true,
  );
  assert.strictEqual(
    hasActiveConversation({
      conversation: { active: () => 1 },
    }),
    true,
  );
  assert.strictEqual(
    hasActiveConversation({
      conversation: { active: () => { throw new Error("boom"); } },
    }),
    false,
  );
}

function testConversationSessionStateProbe() {
  assert.strictEqual(__testables.hasConversationSessionState({}), false);
  assert.strictEqual(
    __testables.hasConversationSessionState({
      session: { conversation: {} },
    }),
    false,
  );
  assert.strictEqual(
    __testables.hasConversationSessionState({
      session: {
        conversation: {
          "scripts-conversation": ["state"],
        },
      },
    }),
    true,
  );
}

function testSafeCallIdentifier() {
  assert.strictEqual(isSafeCallIdentifier("CA1234567890abcdef1234567890abcd"), true);
  assert.strictEqual(isSafeCallIdentifier("9f4f66f2-acde-4307-8e6a"), true);
  assert.strictEqual(isSafeCallIdentifier(""), false);
  assert.strictEqual(isSafeCallIdentifier("../etc/passwd"), false);
  assert.strictEqual(isSafeCallIdentifier("abc"), false);
}

testHasActiveConversation();
testConversationSessionStateProbe();
testSafeCallIdentifier();
