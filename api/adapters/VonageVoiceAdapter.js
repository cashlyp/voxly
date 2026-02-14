const { Vonage } = require("@vonage/server-sdk");

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

class VonageVoiceAdapter {
  constructor(config = {}, logger = console) {
    const { apiKey, apiSecret, applicationId, privateKey, voice = {} } = config;

    if (!apiKey || !apiSecret || !applicationId || !privateKey) {
      throw new Error(
        "VonageVoiceAdapter requires apiKey, apiSecret, applicationId, and privateKey",
      );
    }

    this.logger = logger;
    this.fromNumber = voice.fromNumber;
    this.answerUrlOverride = voice.answerUrl;
    this.eventUrlOverride = voice.eventUrl;
    const timeoutMs = Number(voice.requestTimeoutMs || config.requestTimeoutMs);
    this.requestTimeoutMs =
      Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : 15000;

    this.client = new Vonage({
      apiKey,
      apiSecret,
      applicationId,
      privateKey,
    });
  }

  withTimeout(promise, label = "vonage_request_timeout") {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        const timeoutError = new Error(label);
        timeoutError.code = "vonage_provider_timeout";
        reject(timeoutError);
      }, this.requestTimeoutMs);
      Promise.resolve(promise)
        .then((result) => {
          clearTimeout(timer);
          resolve(result);
        })
        .catch((error) => {
          clearTimeout(timer);
          reject(error);
        });
    });
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

    const finalAnswerUrl = this.answerUrlOverride || answerUrl;
    const finalEventUrl = this.eventUrlOverride || eventUrl;
    const hasInlineNcco = Array.isArray(ncco) && ncco.length > 0;

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
          number: to,
        },
      ],
      from: {
        type: "phone",
        number: this.fromNumber,
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
      to: maskPhoneForLog(to),
      callSid,
      from: maskPhoneForLog(this.fromNumber),
      hasInlineNcco,
      answerUrl: payload.answer_url?.[0] || null,
      eventUrl: payload.event_url?.[0] || null,
    });

    const response = await this.withTimeout(
      this.client.voice.createOutboundCall(payload),
      "vonage_create_call_timeout",
    );
    return response;
  }

  async hangupCall(callUuid) {
    if (!callUuid) {
      throw new Error("VonageVoiceAdapter.hangupCall requires call UUID");
    }
    await this.withTimeout(
      this.client.voice.updateCall(callUuid, { action: "hangup" }),
      "vonage_hangup_timeout",
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
    await this.withTimeout(
      this.client.voice.transferCallWithURL(callUuid, url),
      "vonage_transfer_timeout",
    );
  }
}

module.exports = VonageVoiceAdapter;
