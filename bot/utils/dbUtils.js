"use strict";

/**
 * Database utilities for safe error handling and promise-based queries.
 * Wraps sqlite3 callbacks to provide better error handling and logging.
 */

const { getCurrentOpId } = require("./sessionState");

/**
 * Promisify a database callback-based function
 * @param {Function} dbFn - Database function that takes callback
 * @param {any[]} args - Arguments to pass to db function
 * @returns {Promise} - Resolves with result or rejects with error
 */
function promiseifyDb(dbFn, args = []) {
  return new Promise((resolve, reject) => {
    try {
      dbFn(...args, (err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    } catch (error) {
      reject(error);
    }
  });
}

/**
 * Safe database query with error context
 * @param {Object} ctx - Telegram context (optional)
 * @param {String} operation - Name of operation (e.g., 'getUser')
 * @param {Function} queryFn - Async function that performs the query
 * @returns {Promise}
 */
async function safeDbQuery(ctx, operation, queryFn) {
  const opId = ctx ? getCurrentOpId(ctx) : null;
  const userId = ctx?.from?.id || "unknown";
  const startTime = Date.now();

  try {
    const result = await queryFn();
    const duration = Date.now() - startTime;
    if (duration > 5000) {
      console.warn(
        `Slow DB query [op=${operation}] [opId=${opId}] [user=${userId}] took ${duration}ms`,
      );
    }
    return result;
  } catch (error) {
    const duration = Date.now() - startTime;
    console.error(
      `DB query failed [op=${operation}] [opId=${opId}] [user=${userId}] after ${duration}ms:`,
      error.message,
    );

    // Don't swallow database locked errors - they indicate concurrent access issues
    if (error.message && error.message.includes("database is locked")) {
      console.error(
        "Database concurrency issue detected - consider implementing connection pooling",
      );
    }

    throw error;
  }
}

/**
 * Validate database result exists and has expected structure
 * @param {any} result - Query result
 * @param {String} expectedType - 'object', 'array', or 'number'
 * @returns {boolean|any} - Returns result if valid, throws otherwise
 */
function validateDbResult(result, expectedType = "object") {
  if (expectedType === "array" && !Array.isArray(result)) {
    if (result === undefined || result === null) {
      return [];
    }
    throw new Error(`Expected array result but got ${typeof result}`);
  }

  if (expectedType === "object" && typeof result !== "object") {
    if (result === undefined || result === null) {
      return null;
    }
    throw new Error(`Expected object result but got ${typeof result}`);
  }

  if (expectedType === "number" && typeof result !== "number") {
    throw new Error(`Expected number result but got ${typeof result}`);
  }

  return result;
}

module.exports = {
  promiseifyDb,
  safeDbQuery,
  validateDbResult,
};
