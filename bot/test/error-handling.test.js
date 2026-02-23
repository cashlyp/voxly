"use strict";

const assert = require("assert");
const {
  OperationCancelledError,
  waitForConversationText,
} = require("../utils/sessionState");

// Test 1: Conversation timeout
async function testConversationTimeout() {
  console.log("Testing conversation timeout...");

  const mockConversation = {
    wait: async () => {
      // Simulate waiting forever
      return new Promise(() => {
        // Never resolves
      });
    },
  };

  const mockCtx = {
    reply: async () => {},
    from: { id: 123 },
    session: {},
  };

  try {
    await waitForConversationText(mockConversation, mockCtx, {
      timeoutMs: 1000, // 1 second timeout
      timeoutMessage: "Timeout test",
    });
    assert.fail("Should have thrown timeout error");
  } catch (error) {
    assert(
      error instanceof OperationCancelledError,
      "Should throw OperationCancelledError",
    );
    assert(
      error.message.includes("timeout"),
      "Error message should mention timeout",
    );
    console.log("✓ Conversation timeout test passed");
  }
}

// Test 2: Conversation with invalid message
async function testConversationInvalidMessage() {
  console.log("Testing conversation invalid message handling...");

  let retryCount = 0;
  const mockConversation = {
    wait: async () => {
      retryCount++;
      if (retryCount === 1) {
        // First call returns no text
        return {
          message: {},
          callbackQuery: { id: "cq1" },
          answerCallbackQuery: async () => {},
        };
      }
      // Second call returns valid text
      return { message: { text: "valid input" } };
    },
  };

  const mockCtx = {
    reply: async () => {
      console.log("Reply called");
    },
    from: { id: 123 },
    session: {},
  };

  const result = await waitForConversationText(mockConversation, mockCtx, {
    timeoutMs: 10000,
    invalidMessage: "Invalid",
  });

  assert.strictEqual(
    result.text,
    "valid input",
    "Should return text from second iteration",
  );
  assert.strictEqual(retryCount, 2, "Should have retried once");
  console.log("✓ Conversation invalid message test passed");
}

// Test 3: Conversation with empty message
async function testConversationEmptyMessage() {
  console.log("Testing conversation empty message handling...");

  let retryCount = 0;
  const mockConversation = {
    wait: async () => {
      retryCount++;
      if (retryCount === 1) {
        return { message: { text: "   " } }; // Empty text
      }
      return { message: { text: "non-empty" } };
    },
  };

  const mockCtx = {
    reply: async () => {},
    from: { id: 123 },
    session: {},
  };

  const result = await waitForConversationText(mockConversation, mockCtx, {
    timeoutMs: 10000,
    allowEmpty: false,
    emptyMessage: "Empty",
  });

  assert.strictEqual(result.text, "non-empty", "Should skip empty message");
  assert.strictEqual(retryCount, 2, "Should have retried once");
  console.log("✓ Conversation empty message test passed");
}

// Test 4: Input validation
async function testInputValidation() {
  console.log("Testing input validation...");

  const {
    validatePhoneNumber,
    validateCallSid,
    validateScriptId,
    validateCallbackDataSize,
    validateTextInput,
  } = require("../utils/inputValidator");

  // Phone validation
  assert(
    validatePhoneNumber("+14155552671") === true,
    "Valid phone should pass",
  );
  assert(validatePhoneNumber("4155552671") === true, "Valid phone should pass");
  assert(validatePhoneNumber("123") !== true, "Too short phone should fail");
  assert(validatePhoneNumber("abc") !== true, "Invalid phone should fail");
  console.log("✓ Phone validation passed");

  // Call SID validation
  assert(validateCallSid("call123abc") === true, "Valid call SID should pass");
  assert(validateCallSid("12345") !== true, "Too short SID should fail");
  assert(validateCallSid("call@#$%") !== true, "Invalid chars SID should fail");
  console.log("✓ Call SID validation passed");

  // Script ID validation
  assert(
    validateScriptId("script-1.0") === true,
    "Valid script ID should pass",
  );
  assert(validateScriptId("") !== true, "Empty script ID should fail");
  console.log("✓ Script ID validation passed");

  // Callback data size
  assert(
    validateCallbackDataSize("a".repeat(64)) === true,
    "Valid size should pass",
  );
  assert(
    validateCallbackDataSize("a".repeat(65)) !== true,
    "Oversized callback should fail",
  );
  console.log("✓ Callback data size validation passed");

  // Text input
  assert(validateTextInput("hello world") === true, "Valid text should pass");
  assert(
    validateTextInput("a".repeat(2001)) !== true,
    "Oversized text should fail",
  );
  console.log("✓ Text input validation passed");
}

// Test 5: Database error handling
async function testDatabaseErrorHandling() {
  console.log("Testing database error handling...");

  const { safeDbQuery } = require("../utils/dbUtils");

  const mockCtx = {
    from: { id: 123 },
    session: { currentOp: { id: "op-123" } },
  };

  // Test successful query
  const result = await safeDbQuery(mockCtx, "testOp", async () => {
    return { success: true };
  });
  assert.deepStrictEqual(
    result,
    { success: true },
    "Should return result on success",
  );
  console.log("✓ Successful query test passed");

  // Test error handling
  try {
    await safeDbQuery(mockCtx, "failOp", async () => {
      throw new Error("Test DB error");
    });
    assert.fail("Should have thrown error");
  } catch (error) {
    assert(error.message === "Test DB error", "Should propagate error");
    console.log("✓ Error handling test passed");
  }
}

// Test 6: Operation cancelled error
async function testOperationCancelledError() {
  console.log("Testing OperationCancelledError...");

  const error = new OperationCancelledError("Test cancellation");
  assert(error instanceof Error, "Should be an Error");
  assert(error.name === "OperationCancelledError", "Should have correct name");
  assert(error.message === "Test cancellation", "Should preserve message");
  console.log("✓ OperationCancelledError test passed");
}

// Run all tests
async function runTests() {
  try {
    await testConversationTimeout();
    await testConversationInvalidMessage();
    await testConversationEmptyMessage();
    await testInputValidation();
    await testDatabaseErrorHandling();
    await testOperationCancelledError();

    console.log("\n✅ All error handling tests passed!");
  } catch (error) {
    console.error("\n❌ Test failed:", error);
    process.exit(1);
  }
}

runTests();
