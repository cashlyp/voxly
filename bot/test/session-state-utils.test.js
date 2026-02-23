"use strict";

const assert = require("assert");
const { isSlashCommandInput, FlowContext } = require("../utils/sessionState");

function testIsSlashCommandInput() {
  assert.strictEqual(isSlashCommandInput("/menu"), true);
  assert.strictEqual(isSlashCommandInput(" /help "), true);
  assert.strictEqual(isSlashCommandInput("/"), false);
  assert.strictEqual(isSlashCommandInput("hello"), false);
  assert.strictEqual(isSlashCommandInput(null), false);
}

function testFlowContextExpiryAndTouch() {
  const now = Date.now();
  const flow = new FlowContext("call", 1000, {
    createdAt: now - 2000,
    updatedAt: now - 2000,
  });
  assert.strictEqual(flow.expired, true);
  flow.reset("call");
  assert.strictEqual(flow.expired, false);
  flow.touch("step-1");
  assert.strictEqual(flow.step, "step-1");
}

testIsSlashCommandInput();
testFlowContextExpiryAndTouch();
