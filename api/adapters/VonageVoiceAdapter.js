const { Vonage } = require('@vonage/server-sdk');

class VonageVoiceAdapter {
  constructor(config = {}, logger = console) {
    const { apiKey, apiSecret, applicationId, privateKey, voice = {} } = config;

    if (!apiKey || !apiSecret || !applicationId || !privateKey) {
      throw new Error('VonageVoiceAdapter requires apiKey, apiSecret, applicationId, and privateKey');
    }

    this.logger = logger;
    this.fromNumber = voice.fromNumber;
    this.answerUrlOverride = voice.answerUrl;
    this.eventUrlOverride = voice.eventUrl;

    this.client = new Vonage({
      apiKey,
      apiSecret,
      applicationId,
      privateKey,
    });
  }

  /**
   * Create an outbound call via Vonage Voice API.
   * @param {Object} options
   * @param {string} options.to E.164 destination number.
   * @param {string} options.callSid Internal call identifier.
   * @param {string} options.answerUrl Public URL returning NCCO.
   * @param {string} options.eventUrl Public URL receiving call status events.
   * @returns {Promise<object>}
   */
  async createOutboundCall(options = {}) {
    const { to, callSid, answerUrl, eventUrl } = options;
    if (!to) {
      throw new Error('VonageVoiceAdapter.createOutboundCall requires destination number');
    }
    if (!callSid) {
      throw new Error('VonageVoiceAdapter.createOutboundCall requires callSid');
    }

    const payload = {
      to: [
        {
          type: 'phone',
          number: to,
        },
      ],
      from: {
        type: 'phone',
        number: this.fromNumber,
      },
      answer_url: [this.answerUrlOverride || answerUrl],
      event_url: [this.eventUrlOverride || eventUrl],
    };

    this.logger.info?.('VonageVoiceAdapter: creating outbound call', {
      to,
      callSid,
      answerUrl: payload.answer_url[0],
      eventUrl: payload.event_url[0],
    });

    const response = await this.client.voice.createOutboundCall(payload);
    return response;
  }

  async hangupCall(callUuid) {
    if (!callUuid) {
      throw new Error('VonageVoiceAdapter.hangupCall requires call UUID');
    }
    await this.client.voice.updateCall(callUuid, { action: 'hangup' });
  }
}

module.exports = VonageVoiceAdapter;
