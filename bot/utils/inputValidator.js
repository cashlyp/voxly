"use strict";

/**
 * Input validation utilities for safety and security.
 * Validates user inputs, callback data, and external data.
 */

const MAX_CALLBACK_DATA_LENGTH = 64;
const MAX_PHONE_LENGTH = 20;
const MIN_PHONE_LENGTH = 6;
const MAX_SCRIPT_ID_LENGTH = 80;
const MAX_CALL_SID_LENGTH = 80;
const MAX_TEXT_INPUT_LENGTH = 2000;

/**
 * Validate callback data payload size and format
 * @param {string} data - Raw callback data
 * @param {number} maxLength - Max allowed length (default 64)
 * @returns {boolean|string} - true if valid, error string if not
 */
function validateCallbackDataSize(data, maxLength = MAX_CALLBACK_DATA_LENGTH) {
  if (!data || typeof data !== "string") {
    return "Callback data missing or invalid type";
  }
  if (data.length > maxLength) {
    return `Callback data exceeds maximum length of ${maxLength}`;
  }
  return true;
}

/**
 * Validate E.164-style phone number
 * Allows: +1234567890 or 1234567890
 * @param {string} phone - Phone number to validate
 * @returns {boolean|string} - true if valid, error string if not
 */
function validatePhoneNumber(phone) {
  if (!phone || typeof phone !== "string") {
    return "Phone number missing or invalid type";
  }
  const normalized = phone.trim();
  if (
    normalized.length < MIN_PHONE_LENGTH ||
    normalized.length > MAX_PHONE_LENGTH
  ) {
    return `Phone number length must be ${MIN_PHONE_LENGTH}-${MAX_PHONE_LENGTH} characters`;
  }
  // Allow +, digits, spaces, dashes, parentheses only
  if (!/^[\d+\s\-()]+$/.test(normalized)) {
    return "Phone number contains invalid characters";
  }
  // Must have at least 6 consecutive digits
  const digitsOnly = normalized.replace(/\D/g, "");
  if (digitsOnly.length < MIN_PHONE_LENGTH) {
    return "Phone number must contain at least 6 digits";
  }
  return true;
}

/**
 * Validate call SID from API
 * @param {string} sid - Call SID
 * @returns {boolean|string}
 */
function validateCallSid(sid) {
  if (!sid || typeof sid !== "string") {
    return "Call SID missing or invalid type";
  }
  const trimmed = sid.trim();
  if (trimmed.length < 6 || trimmed.length > MAX_CALL_SID_LENGTH) {
    return `Call SID length must be 6-${MAX_CALL_SID_LENGTH} characters`;
  }
  // Alphanumeric, underscores, hyphens only
  if (!/^[A-Za-z0-9_-]+$/.test(trimmed)) {
    return "Call SID contains invalid characters";
  }
  return true;
}

/**
 * Validate script ID from API
 * @param {string} scriptId - Script ID
 * @returns {boolean|string}
 */
function validateScriptId(scriptId) {
  if (!scriptId || typeof scriptId !== "string") {
    return "Script ID missing or invalid type";
  }
  const trimmed = scriptId.trim();
  if (trimmed.length < 1 || trimmed.length > MAX_SCRIPT_ID_LENGTH) {
    return `Script ID length must be 1-${MAX_SCRIPT_ID_LENGTH} characters`;
  }
  // Alphanumeric, underscores, hyphens, dots only
  if (!/^[A-Za-z0-9_.-]+$/.test(trimmed)) {
    return "Script ID contains invalid characters";
  }
  return true;
}

/**
 * Validate user text input
 * @param {string} text - User text
 * @param {number} maxLength - Max allowed length
 * @returns {boolean|string}
 */
function validateTextInput(text, maxLength = MAX_TEXT_INPUT_LENGTH) {
  if (typeof text !== "string") {
    return "Text input invalid type";
  }
  if (text.length > maxLength) {
    return `Input exceeds maximum length of ${maxLength} characters`;
  }
  return true;
}

/**
 * Validate Telegram user ID
 * @param {number|string} userId - Telegram user ID
 * @returns {boolean|string}
 */
function validateTelegramUserId(userId) {
  if (!userId) {
    return "User ID missing";
  }
  const id = Number(userId);
  if (!Number.isInteger(id) || id <= 0) {
    return "User ID must be a positive integer";
  }
  return true;
}

/**
 * Validate Telegram username
 * @param {string} username - Telegram username
 * @returns {boolean|string}
 */
function validateTelegramUsername(username) {
  if (!username || typeof username !== "string") {
    return "Username missing or invalid type";
  }
  const trimmed = username.trim();
  // Telegram usernames are 5-32 chars, alphanumeric and underscore
  if (trimmed.length < 3 || trimmed.length > 32) {
    return "Username length must be 3-32 characters";
  }
  if (!/^[A-Za-z0-9_]+$/.test(trimmed)) {
    return "Username can only contain letters, numbers, and underscores";
  }
  return true;
}

module.exports = {
  validateCallbackDataSize,
  validatePhoneNumber,
  validateCallSid,
  validateScriptId,
  validateTextInput,
  validateTelegramUserId,
  validateTelegramUsername,
  MAX_CALLBACK_DATA_LENGTH,
  MAX_PHONE_LENGTH,
  MAX_SCRIPT_ID_LENGTH,
  MAX_CALL_SID_LENGTH,
  MAX_TEXT_INPUT_LENGTH,
};
