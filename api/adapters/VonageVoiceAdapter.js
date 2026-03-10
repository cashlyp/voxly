const { Vonage } = require("@vonage/server-sdk");
const { runWithTimeout } = require("../utils/asyncControl");

function isValidHttpsUrl(value) {
  if (!value || typeof value !== "string") return false;
  try {
    const parsed = new URL(value);
    return parsed.protocol === "https:";
  } catch {
    return false;
  }
}

function maskPhoneForLog(value) {
  const digits = String(value || "").replace(/\D/g, "");
  if (!digits) return null;
  if (digits.length <= 4) return "*".repeat(digits.length);
  return `${"*".repeat(Math.max(2, digits.length - 4))}${digits.slice(-4)}`;
}

function normalizePositiveInteger(value, fallback, { min = 0 } = {}) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  const normalized = Math.floor(parsed);
  if (normalized < min) return fallback;
  return normalized;
}

function extractErrorStatusCode(error) {
  const candidates = [
    error?.statusCode,
    error?.status,
    error?.response?.statusCode,
    error?.response?.status,
  ];
  for (const candidate of candidates) {
    const numeric = Number(candidate);
    if (Number.isFinite(numeric) && numeric > 0) {
      return numeric;
    }
  }
  return null;
}

function extractErrorCode(error) {
  const candidates = [
    error?.code,
    error?.errorCode,
    error?.response?.code,
    error?.response?.errorCode,
  ];
  for (const candidate of candidates) {
    const normalized = String(candidate || "").trim();
    if (normalized) return normalized;
  }
  return "";
}

function isRetriableStatus(statusCode) {
  if (!Number.isFinite(Number(statusCode))) return false;
  const normalized = Number(statusCode);
  return [408, 409, 425, 429, 500, 502, 503, 504].includes(normalized);
}

function isRetriableErrorCode(code) {
  const normalized = String(code || "").trim().toLowerCase();
  if (!normalized) return false;
  return new Set([
    "operation_timeout",
    "vonage_provider_timeout",
    "etimedout",
    "esockettimedout",
    "econnreset",
    "econnaborted",
    "econnrefused",
    "enotfound",
    "eai_again",
    "network_error",
    "fetcherror",
  ]).has(normalized);
}

function buildRetryDelayMs(baseMs, maxDelayMs, jitterMs, retryNumber) {
  const exponent = Math.max(0, Number(retryNumber) - 1);
  const withoutJitter = baseMs * (2 ** exponent);
  const jitter = jitterMs > 0 ? Math.floor(Math.random() * (jitterMs + 1)) : 0;
  return Math.max(0, Math.min(maxDelayMs, withoutJitter + jitter));
}

function sleepMs(delayMs) {
  const normalized = normalizePositiveInteger(delayMs, 0);
  if (!normalized) return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, normalized);
    if (typeof timer?.unref === "function") {
      timer.unref();
    }
  });
}

function sanitizePhoneNumber(value, label) {
  const normalized = String(value || "")
    .trim()
    .replace(/[\s()-]/g, "");
  if (!normalized || !/^\+?[0-9]{7,15}$/.test(normalized)) {
    throw new Error(`VonageVoiceAdapter requires a valid ${label} number`);
  }
  return normalized;
}

function validateInlineNcco(ncco) {
  if (!Array.isArray(ncco) || ncco.length === 0) return false;
  for (const action of ncco) {
    if (!action || typeof action !== "object") {
      throw new Error("VonageVoiceAdapter.createOutboundCall ncco actions must be objects");
    }
    const actionType = String(action.action || "").trim();
    if (!actionType) {
      throw new Error("VonageVoiceAdapter.createOutboundCall ncco actions require an action field");
    }
  }
  return true;
}

class VonageVoiceAdapter {
  constructor(config = {}, logger = console) {
    const { apiKey, apiSecret, applicationId, privateKey, voice = {} } = config;
    const injectedClient = config.client || null;

    if (!injectedClient && (!apiKey || !apiSecret || !applicationId || !privateKey)) {
      throw new Error(
        "VonageVoiceAdapter requires apiKey, apiSecret, applicationId, and privateKey",
      );
    }

    this.logger = logger;
    this.fromNumber = String(voice.fromNumber || "").trim();
    this.answerUrlOverride = voice.answerUrl;
    this.eventUrlOverride = voice.eventUrl;
    const timeoutMs = Number(voice.requestTimeoutMs || config.requestTimeoutMs);
    this.requestTimeoutMs =
      Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : 15000;
    this.retryAttempts = normalizePositiveInteger(
      voice.retryAttempts ?? config.retryAttempts,
      1,
      { min: 0 },
    );
    // Creating outbound calls is intentionally conservative to avoid duplicate call risks.
    this.createRetryAttempts = normalizePositiveInteger(
      voice.createRetryAttempts ?? config.createRetryAttempts,
      0,
      { min: 0 },
    );
    this.retryBaseMs = normalizePositiveInteger(
      voice.retryBaseMs ?? config.retryBaseMs,
      250,
      { min: 0 },
    );
    this.retryMaxDelayMs = normalizePositiveInteger(
      voice.retryMaxDelayMs ?? config.retryMaxDelayMs,
      2000,
      { min: 0 },
    );
    this.retryJitterMs = normalizePositiveInteger(
      voice.retryJitterMs ?? config.retryJitterMs,
      120,
      { min: 0 },
    );

    this.client =
      injectedClient ||
      new Vonage({
        apiKey,
        apiSecret,
        applicationId,
        privateKey,
      });
  }

  async executeVoiceOperation(operationName, runner, options = {}) {
    const maxRetries = normalizePositiveInteger(options.retryAttempts, 0, {
      min: 0,
    });
    const maxAttempts = Math.max(1, maxRetries + 1);
    const timeoutLabel = String(options.timeoutLabel || operationName);
    const timeoutCode = String(options.timeoutCode || "vonage_provider_timeout");
    const meta =
      options.meta && typeof options.meta === "object" ? options.meta : {};

    let attempt = 0;
    while (attempt < maxAttempts) {
      const attemptNumber = attempt + 1;
      try {
        return await runWithTimeout(Promise.resolve().then(() => runner()), {
          timeoutMs: this.requestTimeoutMs,
          label: timeoutLabel,
          timeoutCode,
          logger: this.logger,
          meta: {
            provider: "vonage",
            operation: operationName,
            attempt: attemptNumber,
            max_attempts: maxAttempts,
            ...meta,
          },
          warnAfterMs: Math.min(
            5000,
            Math.max(1000, Math.floor(this.requestTimeoutMs / 2)),
          ),
        });
      } catch (error) {
        const statusCode = extractErrorStatusCode(error);
        const errorCode = extractErrorCode(error);
        const retriable = isRetriableStatus(statusCode) || isRetriableErrorCode(errorCode);
        const willRetry = retriable && attemptNumber < maxAttempts;
        this.logger?.warn?.("vonage_voice_operation_failed", {
          provider: "vonage",
          operation: operationName,
          attempt: attemptNumber,
          max_attempts: maxAttempts,
          retriable,
          will_retry: willRetry,
          status_code: statusCode,
          code: errorCode || null,
          error: error?.message || String(error || "unknown_error"),
          ...meta,
        });
        if (!willRetry) {
          if (error && typeof error === "object") {
            error.provider = "vonage";
            error.operation = operationName;
            if (statusCode && !error.statusCode) {
              error.statusCode = statusCode;
            }
          }
          throw error;
        }
        const retryNumber = attemptNumber;
        const delayMs = buildRetryDelayMs(
          this.retryBaseMs,
          this.retryMaxDelayMs,
          this.retryJitterMs,
          retryNumber,
        );
        await sleepMs(delayMs);
      }
      attempt += 1;
    }
    throw new Error(`VonageVoiceAdapter ${operationName} exceeded retry budget`);
  }

  /**
   * Create an outbound call via Vonage Voice API.
   * @param {Object} options
   * @param {string} options.to E.164 destination number.
   * @param {string} options.callSid Internal call identifier.
   * @param {string} options.answerUrl Public URL returning NCCO.
   * @param {string} options.eventUrl Public URL receiving call status events.
   * @param {Array<object>} options.ncco Inline NCCO payload (preferred).
   * @returns {Promise<object>}
   */
  async createOutboundCall(options = {}) {
    const { to, callSid, answerUrl, eventUrl, ncco } = options;
    if (!to) {
      throw new Error(
        "VonageVoiceAdapter.createOutboundCall requires destination number",
      );
    }
    if (!callSid) {
      throw new Error("VonageVoiceAdapter.createOutboundCall requires callSid");
    }
    if (!this.fromNumber) {
      throw new Error(
        "VonageVoiceAdapter requires VONAGE_VOICE_FROM_NUMBER for outbound calls",
      );
    }
    const normalizedTo = sanitizePhoneNumber(to, "destination");
    const normalizedFrom = sanitizePhoneNumber(this.fromNumber, "from");

    const finalAnswerUrl = this.answerUrlOverride || answerUrl;
    const finalEventUrl = this.eventUrlOverride || eventUrl;
    const hasInlineNcco = validateInlineNcco(ncco);

    if (!hasInlineNcco && !isValidHttpsUrl(finalAnswerUrl)) {
      throw new Error(
        "VonageVoiceAdapter.createOutboundCall requires a valid HTTPS answerUrl when ncco is not provided",
      );
    }
    if (finalEventUrl && !isValidHttpsUrl(finalEventUrl)) {
      throw new Error(
        "VonageVoiceAdapter.createOutboundCall requires eventUrl to be a valid HTTPS URL",
      );
    }

    const payload = {
      to: [
        {
          type: "phone",
          number: normalizedTo,
        },
      ],
      from: {
        type: "phone",
        number: normalizedFrom,
      },
    };

    if (hasInlineNcco) {
      payload.ncco = ncco;
    } else {
      payload.answer_url = [finalAnswerUrl];
      payload.answer_method = "GET";
    }

    if (finalEventUrl) {
      payload.event_url = [finalEventUrl];
      payload.event_method = "POST";
    }

    this.logger.info?.("VonageVoiceAdapter: creating outbound call", {
      to: maskPhoneForLog(normalizedTo),
      callSid,
      from: maskPhoneForLog(normalizedFrom),
      hasInlineNcco,
      answerUrl: payload.answer_url?.[0] || null,
      eventUrl: payload.event_url?.[0] || null,
    });

    const response = await this.executeVoiceOperation(
      "create_outbound_call",
      () => this.client.voice.createOutboundCall(payload),
      {
        timeoutLabel: "vonage_create_call_timeout",
        retryAttempts: this.createRetryAttempts,
        meta: {
          callSid,
          to: maskPhoneForLog(normalizedTo),
        },
      },
    );
    if (!response?.uuid) {
      this.logger.warn?.("vonage_create_call_missing_uuid", {
        provider: "vonage",
        operation: "create_outbound_call",
        callSid,
      });
    }
    return response;
  }

  async hangupCall(callUuid) {
    if (!callUuid) {
      throw new Error("VonageVoiceAdapter.hangupCall requires call UUID");
    }
    await this.executeVoiceOperation(
      "hangup",
      () => {
        if (typeof this.client?.voice?.hangupCall === "function") {
          return this.client.voice.hangupCall(callUuid);
        }
        return this.client.voice.updateCall(callUuid, { action: "hangup" });
      },
      {
        timeoutLabel: "vonage_hangup_timeout",
        retryAttempts: this.retryAttempts,
        meta: { call_uuid: String(callUuid) },
      },
    );
  }

  async transferCallWithURL(callUuid, url) {
    if (!callUuid) {
      throw new Error("VonageVoiceAdapter.transferCallWithURL requires call UUID");
    }
    if (!isValidHttpsUrl(url)) {
      throw new Error(
        "VonageVoiceAdapter.transferCallWithURL requires a valid HTTPS URL",
      );
    }
    await this.executeVoiceOperation(
      "transfer_with_url",
      () => this.client.voice.transferCallWithURL(callUuid, url),
      {
        timeoutLabel: "vonage_transfer_timeout",
        retryAttempts: this.retryAttempts,
        meta: { call_uuid: String(callUuid) },
      },
    );
  }
}

module.exports = VonageVoiceAdapter;
