const { Vonage } = require('@vonage/server-sdk');

class VonageSmsAdapter {
  constructor(config = {}, logger = console) {
    const { apiKey, apiSecret, sms = {} } = config;

    if (!apiKey || !apiSecret) {
      throw new Error('VonageSmsAdapter requires apiKey and apiSecret');
    }

    this.logger = logger;
    this.fromNumber = sms.fromNumber;

    this.client = new Vonage({
      apiKey,
      apiSecret,
    });
  }

  async sendSms({ to, body, from, statusCallback }) {
    if (!to) {
      throw new Error('VonageSmsAdapter.sendSms requires destination number');
    }
    if (!body) {
      throw new Error('VonageSmsAdapter.sendSms requires message body');
    }

    const payload = {
      to,
      from: from || this.fromNumber,
      text: body,
    };

    if (statusCallback) {
      payload['callback'] = statusCallback;
    }

    this.logger.info?.('VonageSmsAdapter: sending SMS', {
      to,
      from: payload.from,
    });

    const response = await this.client.sms.send(payload);
    return response;
  }
}

module.exports = VonageSmsAdapter;
