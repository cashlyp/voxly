"use strict";

const assert = require("assert");
const {
  isSlashCommandInput,
  FlowContext,
  startOperation,
} = require("../utils/sessionState");

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

function testStartOperationReusesSameCommand() {
  const ctx = { session: {} };
  const firstId = startOperation(ctx, "scripts", { source: "initial" });
  const firstToken = ctx.session.currentOp?.token;
  const originalStartedAt = ctx.session.currentOp?.startedAt;
  ctx.session.currentOp.startedAt = 1;
  const replayId = startOperation(ctx, "scripts", { phase: "replay" });
  const replayToken = ctx.session.currentOp?.token;

  assert.strictEqual(replayId, firstId);
  assert.strictEqual(replayToken, firstToken);
  assert(
    Number(ctx.session.currentOp?.startedAt) > 1,
    "Expected replayed operation to refresh startedAt",
  );
  assert(
    Number(ctx.session.currentOp?.startedAt) >= Number(originalStartedAt || 0),
    "Expected replayed operation startedAt to remain current",
  );
  assert.strictEqual(ctx.session.currentOp?.metadata?.source, "initial");
  assert.strictEqual(ctx.session.currentOp?.metadata?.phase, "replay");

  const nextId = startOperation(ctx, "sms");
  assert.notStrictEqual(nextId, firstId);
}

testIsSlashCommandInput();
testFlowContextExpiryAndTouch();
testStartOperationReusesSameCommand();
