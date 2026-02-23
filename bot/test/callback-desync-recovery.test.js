"use strict";

const assert = require("assert");
const {
  parseCallbackAction,
  resolveConversationFromPrefix,
  getConversationRecoveryTarget,
  getSelectionTokenFromAction,
  buildCallbackReplayQueue,
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
  assert.strictEqual(resolveConversationFromPrefix("call-script-main"), "scripts-conversation");
  assert.strictEqual(resolveConversationFromPrefix("sms-script-main"), "scripts-conversation");
  assert.strictEqual(resolveConversationFromPrefix("script-channel"), "scripts-conversation");
  assert.strictEqual(resolveConversationFromPrefix("email-template-main"), "email-templates-conversation");
  assert.strictEqual(resolveConversationFromPrefix("inbound-default"), "scripts-conversation");
  assert.strictEqual(resolveConversationFromPrefix("inbound-default-select"), "scripts-conversation");
  assert.strictEqual(resolveConversationFromPrefix("persona-choose"), "persona-conversation");
}

function testSelectionTokenExtraction() {
  assert.strictEqual(
    getSelectionTokenFromAction("sms-script-main:bd0c9bd3:3fg7:0"),
    "0",
  );
  assert.strictEqual(
    getSelectionTokenFromAction("call-script-main:1234abcd:2"),
    "2",
  );
  assert.strictEqual(getSelectionTokenFromAction("script-channel"), null);
}

function testScriptReplayQueueBuilder() {
  assert.deepStrictEqual(
    buildCallbackReplayQueue("sms-script-main:bd0c9bd3:3fg7:0"),
    ["script-channel:1", "sms-script-main:0"],
  );
  assert.deepStrictEqual(
    buildCallbackReplayQueue("call-script-main:abcd1234:2"),
    ["script-channel:0", "call-script-main:2"],
  );
  assert.deepStrictEqual(
    buildCallbackReplayQueue("email-template-main:deadbeef:4"),
    ["script-channel:2", "email-template-main:4"],
  );
  assert.deepStrictEqual(
    buildCallbackReplayQueue("inbound-default:1234abcd:9z9z:1"),
    ["script-channel:0", "call-script-main:2", "inbound-default:1"],
  );
}

testParseSignedConversationAction();
testParseActionWithoutOperationToken();
testRecoveryTargetMapping();
testUnknownPrefixHasNoRecoveryTarget();
testPrefixResolverCoverage();
testSelectionTokenExtraction();
testScriptReplayQueueBuilder();
