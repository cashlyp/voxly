const { ConnectClient, StartOutboundVoiceContactCommand, StopContactCommand, UpdateContactAttributesCommand } = require('@aws-sdk/client-connect');

/**
 * AwsConnectAdapter encapsulates interaction with Amazon Connect for originating calls
 * and instructing the active contact flow to play synthesized prompts.
 *
 * The adapter does not enforce a specific contact-flow implementation. Instead it relies on
 * shared contact attributes that the flow reads to fetch Polly audio from S3 or toggle actions.
 */
class AwsConnectAdapter {
  /**
   * @param {object} config
   * @param {string} config.region AWS region, e.g. us-east-1
   * @param {object} config.connect
   * @param {string} config.connect.instanceId Amazon Connect instance id
   * @param {string} config.connect.contactFlowId Contact flow id that streams audio to KVS
   * @param {string} [config.connect.queueId] Optional queue id for outbound calls
   * @param {string} [config.connect.sourcePhoneNumber] Outbound CLI phone number
   * @param {Console} [logger] optional logger (defaults to console)
   */
  constructor(config, logger = console) {
    if (!config?.connect?.instanceId) {
      throw new Error('AwsConnectAdapter requires connect.instanceId');
    }
    if (!config?.connect?.contactFlowId) {
      throw new Error('AwsConnectAdapter requires connect.contactFlowId');
    }

    this.config = config;
    this.logger = logger;
    this.client = new ConnectClient({ region: config.region });
  }

  /**
   * Start an outbound voice contact using the configured contact flow.
   * @param {object} options
   * @param {string} options.destinationPhoneNumber E.164 formatted destination
   * @param {string} options.clientToken Idempotency token (call SID)
   * @param {object} [options.attributes] Map of Connect contact attributes for the flow
   * @returns {Promise<object>} StartOutboundVoiceContact response
   */
  async startOutboundCall(options) {
    const { destinationPhoneNumber, clientToken, attributes = {} } = options || {};

    if (!destinationPhoneNumber) {
      throw new Error('AwsConnectAdapter.startOutboundCall requires destinationPhoneNumber');
    }
    if (!clientToken) {
      throw new Error('AwsConnectAdapter.startOutboundCall requires clientToken');
    }

    const params = {
      InstanceId: this.config.connect.instanceId,
      ContactFlowId: this.config.connect.contactFlowId,
      ClientToken: clientToken,
      DestinationPhoneNumber: destinationPhoneNumber,
      Attributes: attributes,
    };

    if (this.config.connect.queueId) {
      params.QueueId = this.config.connect.queueId;
    }
    if (this.config.connect.sourcePhoneNumber) {
      params.SourcePhoneNumber = this.config.connect.sourcePhoneNumber;
    }

    this.logger.info?.('Calling Amazon Connect StartOutboundVoiceContact', {
      destinationPhoneNumber,
      clientToken,
      contactFlowId: this.config.connect.contactFlowId,
    });

    const command = new StartOutboundVoiceContactCommand(params);
    const response = await this.client.send(command);
    this.logger.info?.('Amazon Connect outbound contact started', {
      contactId: response.ContactId,
      clientToken,
    });
    return response;
  }

  /**
   * Stop an active contact. The call typically transitions to completed or cancelled state.
   * @param {object} options
   * @param {string} options.contactId Amazon Connect contact ID
   * @param {string} [options.instanceId] Override Connect instance ID
   */
  async stopContact(options) {
    const { contactId, instanceId } = options || {};
    if (!contactId) {
      throw new Error('AwsConnectAdapter.stopContact requires contactId');
    }

    const params = {
      InstanceId: instanceId || this.config.connect.instanceId,
      ContactId: contactId,
    };

    const command = new StopContactCommand(params);
    await this.client.send(command);
    this.logger.info?.('Amazon Connect contact stopped', params);
  }

  /**
   * Request the contact flow to play audio from a pre-signed S3 URL or bucket key.
   * The contact flow must inspect the attributes and fetch the referenced audio asset.
   *
   * @param {object} options
   * @param {string} options.contactId Target Connect contact
   * @param {string} options.audioKey S3 key or URL of the Polly synthesized audio
   * @param {number} [options.expirySeconds] Optional expiry in seconds for presigned URLs
   * @param {object} [options.additionalAttributes] Attributes to merge into the payload
   */
  async enqueueAudioPlayback(options) {
    const { contactId, audioKey, expirySeconds, additionalAttributes = {} } = options || {};
    if (!contactId) {
      throw new Error('AwsConnectAdapter.enqueueAudioPlayback requires contactId');
    }
    if (!audioKey) {
      throw new Error('AwsConnectAdapter.enqueueAudioPlayback requires audioKey');
    }

    const attributes = {
      NEXT_PROMPT_KEY: audioKey,
      ...(expirySeconds ? { NEXT_PROMPT_TTL: String(expirySeconds) } : {}),
      ...additionalAttributes,
    };

    const command = new UpdateContactAttributesCommand({
      InstanceId: this.config.connect.instanceId,
      ContactId: contactId,
      Attributes: attributes,
    });

    await this.client.send(command);
    this.logger.info?.('Updated contact attributes for audio playback', {
      contactId,
      audioKey,
    });
    return attributes;
  }
}

module.exports = AwsConnectAdapter;
