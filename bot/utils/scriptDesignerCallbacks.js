"use strict";

const SCRIPT_DESIGNER_PREFIX_PATTERNS = [
  /^script-channel$/,
  /^call-script-/,
  /^sms-script-/,
  /^inbound-default/,
  /^email-template-/,
];

const OP_TOKEN_PATTERN = /^[0-9a-fA-F-]{8,}$/;
const NONCE_PATTERN = /^[A-Za-z0-9_-]{1,24}$/;
const SELECTION_PATTERN = /^\d+$/;

function isScriptDesignerPrefix(prefix) {
  const candidate = String(prefix || "");
  return SCRIPT_DESIGNER_PREFIX_PATTERNS.some((pattern) =>
    pattern.test(candidate),
  );
}

function parseScriptDesignerCallbackAction(action) {
  const rawAction = String(action || "").trim();
  if (!rawAction) {
    return {
      isScriptDesigner: false,
      valid: false,
      reason: "empty_action",
      rawAction,
    };
  }

  const parts = rawAction.split(":").filter((part) => part.length > 0);
  const prefix = parts[0] || "";
  if (!isScriptDesignerPrefix(prefix)) {
    return {
      isScriptDesigner: false,
      valid: false,
      reason: "non_script_prefix",
      rawAction,
      prefix,
    };
  }

  if (parts.length < 2) {
    return {
      isScriptDesigner: true,
      valid: false,
      reason: "missing_selection",
      rawAction,
      prefix,
    };
  }

  const selectionToken = parts[parts.length - 1];
  if (!SELECTION_PATTERN.test(selectionToken)) {
    return {
      isScriptDesigner: true,
      valid: false,
      reason: "invalid_selection_token",
      rawAction,
      prefix,
      selectionToken,
    };
  }

  const middle = parts.slice(1, -1);
  if (middle.length > 2) {
    return {
      isScriptDesigner: true,
      valid: false,
      reason: "unexpected_token_count",
      rawAction,
      prefix,
      selectionToken,
    };
  }
  if (middle.length >= 1 && !OP_TOKEN_PATTERN.test(middle[0])) {
    return {
      isScriptDesigner: true,
      valid: false,
      reason: "invalid_op_token",
      rawAction,
      prefix,
      selectionToken,
      opToken: middle[0],
    };
  }
  if (middle.length === 2 && !NONCE_PATTERN.test(middle[1])) {
    return {
      isScriptDesigner: true,
      valid: false,
      reason: "invalid_nonce",
      rawAction,
      prefix,
      selectionToken,
      nonce: middle[1],
    };
  }

  const normalizedAction = `${prefix}:${selectionToken}`;
  return {
    isScriptDesigner: true,
    valid: true,
    reason: null,
    rawAction,
    prefix,
    selectionToken,
    selectionIndex: Number(selectionToken),
    normalizedAction,
    opToken:
      middle.length >= 1 ? middle[0].replace(/-/g, "").slice(0, 8) : null,
    nonce: middle.length === 2 ? middle[1] : null,
    legacy: middle.length > 0,
  };
}

function isScriptDesignerAction(action) {
  return parseScriptDesignerCallbackAction(action).isScriptDesigner;
}

module.exports = {
  SCRIPT_DESIGNER_PREFIX_PATTERNS,
  isScriptDesignerPrefix,
  parseScriptDesignerCallbackAction,
  isScriptDesignerAction,
};

