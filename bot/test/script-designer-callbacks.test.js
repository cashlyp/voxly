"use strict";

const assert = require("assert");
const {
  isScriptDesignerPrefix,
  parseScriptDesignerCallbackAction,
  isScriptDesignerAction,
} = require("../utils/scriptDesignerCallbacks");

function testScriptDesignerPrefixMatcher() {
  assert.strictEqual(isScriptDesignerPrefix("script-channel"), true);
  assert.strictEqual(isScriptDesignerPrefix("call-script-main"), true);
  assert.strictEqual(isScriptDesignerPrefix("sms-script-main"), true);
  assert.strictEqual(isScriptDesignerPrefix("inbound-default"), true);
  assert.strictEqual(isScriptDesignerPrefix("email-template-main"), true);
  assert.strictEqual(isScriptDesignerPrefix("persona-main"), false);
}

function testParserNormalizesLegacyAction() {
  const parsed = parseScriptDesignerCallbackAction(
    "sms-script-main:bd0c9bd3:3fg7:0",
  );
  assert.strictEqual(parsed.isScriptDesigner, true);
  assert.strictEqual(parsed.valid, true);
  assert.strictEqual(parsed.selectionToken, "0");
  assert.strictEqual(parsed.normalizedAction, "sms-script-main:0");
  assert.strictEqual(parsed.legacy, true);
}

function testParserAcceptsModernAction() {
  const parsed = parseScriptDesignerCallbackAction("call-script-main:2");
  assert.strictEqual(parsed.isScriptDesigner, true);
  assert.strictEqual(parsed.valid, true);
  assert.strictEqual(parsed.normalizedAction, "call-script-main:2");
  assert.strictEqual(parsed.legacy, false);
}

function testParserRejectsInvalidSelection() {
  const parsed = parseScriptDesignerCallbackAction("script-channel:abc");
  assert.strictEqual(parsed.isScriptDesigner, true);
  assert.strictEqual(parsed.valid, false);
  assert.strictEqual(parsed.reason, "invalid_selection_token");
}

function testParserRejectsInvalidNonce() {
  const parsed = parseScriptDesignerCallbackAction(
    "call-script-main:abcd1234:*bad*:0",
  );
  assert.strictEqual(parsed.isScriptDesigner, true);
  assert.strictEqual(parsed.valid, false);
  assert.strictEqual(parsed.reason, "invalid_nonce");
}

function testIsScriptDesignerAction() {
  assert.strictEqual(isScriptDesignerAction("call-script-main:1"), true);
  assert.strictEqual(isScriptDesignerAction("SMS_SEND"), false);
}

testScriptDesignerPrefixMatcher();
testParserNormalizesLegacyAction();
testParserAcceptsModernAction();
testParserRejectsInvalidSelection();
testParserRejectsInvalidNonce();
testIsScriptDesignerAction();

