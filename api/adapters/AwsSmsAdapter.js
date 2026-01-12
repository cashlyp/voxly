const { PinpointClient, SendMessagesCommand } = require('@aws-sdk/client-pinpoint');

/**
 * AwsSmsAdapter provides minimal Pinpoint SMS send capability that mirrors the
 * previous Twilio-based smsService contract. Delivery status webhooks should be
 * wired through SNS/Lambda and POSTed to the API separately.
 */
class AwsSmsAdapter {
  /**
   * @param {object} config
   * @param {string} config.region AWS region for Pinpoint
   * @param {object} config.pinpoint
   * @param {string} config.pinpoint.applicationId Pinpoint project ID
   * @param {string} [config.pinpoint.originationNumber] Default origination number
   * @param {Console} [logger]
   */
  constructor(config, logger = console) {
    if (!config?.pinpoint?.applicationId) {
      throw new Error('AwsSmsAdapter requires aws.pinpoint.applicationId');
    }

    this.config = config;
    this.logger = logger;
    this.client = new PinpointClient({
      region: config.pinpoint.region || config.region,
    });
  }

  /**
   * Dispatch a transactional SMS via Pinpoint.
   * @param {object} payload
   * @param {string} payload.to Destination phone number (E.164)
   * @param {string} payload.body Message body
   * @param {string} [payload.from] Override origination number/short code
   * @param {object} [payload.context] Additional Attributes -> MessageAttributes
   */
  async sendSms(payload) {
    const { to, body, from, context = {} } = payload || {};
    if (!to) {
      throw new Error('AwsSmsAdapter.sendSms requires destination "to" number');
    }
    if (!body) {
      throw new Error('AwsSmsAdapter.sendSms requires message "body"');
    }

    const origination = from || this.config.pinpoint.originationNumber;
    if (!origination) {
      throw new Error('AwsSmsAdapter needs an origination number (Pinpoint origination or from)');
    }

    const command = new SendMessagesCommand({
      ApplicationId: this.config.pinpoint.applicationId,
      MessageRequest: {
        Context: context,
        Addresses: {
          [to]: {
            ChannelType: 'SMS',
          },
        },
        MessageConfiguration: {
          SMSMessage: {
            Body: body,
            MessageType: 'TRANSACTIONAL',
            OriginationNumber: origination,
          },
        },
      },
    });

    const response = await this.client.send(command);
    const endpoint = response?.MessageResponse?.Result?.[to];
    this.logger.info?.('Sent Pinpoint SMS', {
      destination: to,
      status: endpoint?.DeliveryStatus,
      messageId: endpoint?.MessageId,
    });

    return response;
  }
}

module.exports = AwsSmsAdapter;
