'use strict';

const assert = require('assert');
const {
  parseCallbackAction,
  resolveConversationFromPrefix,
  getConversationRecoveryTarget,
  recoverConversationFromCallback
} = require('../utils/conversationRecovery');

async function run(name, fn) {
  try {
    await fn();
    process.stdout.write(`ok - ${name}\n`);
  } catch (error) {
    process.stderr.write(`not ok - ${name}\n`);
    throw error;
  }
}

async function testParseCallbackAction() {
  const parsedWithOp = parseCallbackAction('sms-preview:123e4567-e89b-12d3-a456-426614174000:send');
  assert.deepStrictEqual(parsedWithOp, {
    prefix: 'sms-preview',
    opId: '123e4567-e89b-12d3-a456-426614174000',
    value: 'send'
  });

  const parsedWithoutOp = parseCallbackAction('sms-preview:send');
  assert.deepStrictEqual(parsedWithoutOp, {
    prefix: 'sms-preview',
    opId: null,
    value: 'send'
  });

  assert.strictEqual(parseCallbackAction('SMS_SEND'), null);
}

async function testResolveConversationFromPrefix() {
  assert.strictEqual(resolveConversationFromPrefix('sms-preview'), 'sms-conversation');
  assert.strictEqual(resolveConversationFromPrefix('bulk-email-status'), 'bulk-email-conversation');
  assert.strictEqual(resolveConversationFromPrefix('call-config'), 'call-conversation');
  assert.strictEqual(resolveConversationFromPrefix('unknown-prefix'), null);
}

async function testGetConversationRecoveryTarget() {
  const target = getConversationRecoveryTarget('sms-preview:send');
  assert.strictEqual(target.parsed.prefix, 'sms-preview');
  assert.strictEqual(target.conversationTarget, 'sms-conversation');
  assert.strictEqual(getConversationRecoveryTarget('SMS_SEND'), null);
  assert.strictEqual(getConversationRecoveryTarget('unknown-prefix:value'), null);
}

async function testRecoverConversationFromCallback() {
  const calls = [];
  const ctx = {
    reply: async (message) => {
      calls.push({ type: 'reply', message });
    },
    conversation: {
      enter: async (conversationName) => {
        calls.push({ type: 'enter', conversationName });
      }
    }
  };

  const adapters = {
    cancelActiveFlow: async (_ctx, reason) => {
      calls.push({ type: 'cancel', reason });
    },
    resetSession: (_ctx) => {
      calls.push({ type: 'reset' });
    },
    clearMenuMessages: async (_ctx) => {
      calls.push({ type: 'clear_menu' });
    }
  };

  const recovered = await recoverConversationFromCallback(
    ctx,
    'sms-preview:send',
    'sms-conversation',
    adapters
  );

  assert.strictEqual(recovered, true);
  assert.deepStrictEqual(
    calls.map((entry) => entry.type),
    ['cancel', 'reset', 'clear_menu', 'reply', 'enter']
  );
  assert.strictEqual(calls[0].reason, 'desynced_callback:sms-preview:send');
  assert.ok(calls[3].message.includes('Reopening that flow so you can continue.'));
  assert.strictEqual(calls[4].conversationName, 'sms-conversation');

  const noRecovery = await recoverConversationFromCallback(
    null,
    'sms-preview:send',
    'sms-conversation',
    adapters
  );
  assert.strictEqual(noRecovery, false);
}

async function main() {
  await run('parse callback action', testParseCallbackAction);
  await run('resolve conversation prefix', testResolveConversationFromPrefix);
  await run('get recovery target', testGetConversationRecoveryTarget);
  await run('recover desynced conversation callback', testRecoverConversationFromCallback);
  process.stdout.write('All callback desync recovery tests passed.\n');
}

main().catch((error) => {
  process.stderr.write(`${error.stack || error}\n`);
  process.exit(1);
});
