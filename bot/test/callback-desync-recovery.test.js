"use strict";

const assert = require("assert");
const {
  parseCallbackAction,
  resolveConversationFromPrefix,
  getConversationRecoveryTarget,
} = require("../utils/conversationRecovery");

function testParseSignedConversationAction() {
  const parsed = parseCallbackAction("sms-script:12345678:select");
  assert.deepStrictEqual(parsed, {
    prefix: "sms-script",
    opId: "12345678",
    value: "select",
  });
}

function testParseActionWithoutOperationToken() {
  const parsed = parseCallbackAction("EMAIL_STATUS:msg_1");
  assert.deepStrictEqual(parsed, {
    prefix: "EMAIL_STATUS",
    opId: null,
    value: "msg_1",
  });
}

function testRecoveryTargetMapping() {
  const target = getConversationRecoveryTarget("call-script:12345678:pick");
  assert.ok(target);
  assert.strictEqual(target.conversationTarget, "call-conversation");
}

function testUnknownPrefixHasNoRecoveryTarget() {
  const target = getConversationRecoveryTarget("unknown:abcdef12:value");
  assert.strictEqual(target, null);
}

function testPrefixResolverCoverage() {
  assert.strictEqual(resolveConversationFromPrefix("sms-foo"), "sms-conversation");
  assert.strictEqual(resolveConversationFromPrefix("bulk-email-x"), "bulk-email-conversation");
  assert.strictEqual(resolveConversationFromPrefix("inbound-default"), "scripts-conversation");
  assert.strictEqual(resolveConversationFromPrefix("inbound-default-select"), "scripts-conversation");
  assert.strictEqual(resolveConversationFromPrefix("persona-choose"), "persona-conversation");
}

testParseSignedConversationAction();
testParseActionWithoutOperationToken();
testRecoveryTargetMapping();
testUnknownPrefixHasNoRecoveryTarget();
testPrefixResolverCoverage();
