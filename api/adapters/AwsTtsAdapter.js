const { PollyClient, SynthesizeSpeechCommand } = require('@aws-sdk/client-polly');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { streamCollector } = require('@aws-sdk/util-stream-node');
const { v4: uuidv4 } = require('uuid');

/**
 * AwsTtsAdapter wraps Amazon Polly to provide synthesized audio suitable for
 * playback inside Amazon Connect. It optionally persists the audio to S3,
 * returning the S3 object key that Connect contact flows can load.
 */
class AwsTtsAdapter {
  /**
   * @param {object} config
   * @param {string} config.region AWS region
   * @param {object} config.polly
   * @param {string} config.polly.voiceId Polly voice name (e.g. Joanna)
   * @param {string} [config.polly.outputBucket] S3 bucket for storing prompts
   * @param {string} [config.polly.outputPrefix] Default key prefix for prompts
   * @param {object} [config.s3]
   * @param {string} [config.s3.mediaBucket] Optional bucket override
   * @param {Console} [logger] optional logger
   */
  constructor(config, logger = console) {
    if (!config?.region) {
      throw new Error('AwsTtsAdapter requires aws.region in configuration');
    }

    this.config = config;
    this.logger = logger;
    this.polly = new PollyClient({ region: config.region });
    this.s3Bucket = config.s3?.mediaBucket || config.polly?.outputBucket;
    this.s3Prefix = config.polly?.outputPrefix || 'tts/';
    this.voiceId = config.polly?.voiceId || 'Joanna';
    this.s3 = this.s3Bucket ? new S3Client({ region: config.region }) : null;
  }

  /**
   * Generate audio for the provided text. Returns a Buffer.
   * @param {string} text
   * @param {object} [options]
   * @param {string} [options.voiceId]
   * @param {string} [options.engine] neural or standard
   * @param {string} [options.outputFormat] defaults to pcm
   */
  async synthesize(text, options = {}) {
    if (!text || !text.trim()) {
      throw new Error('AwsTtsAdapter.synthesize requires non-empty text');
    }

    const params = {
      OutputFormat: options.outputFormat || 'pcm',
      Text: text,
      VoiceId: options.voiceId || this.voiceId,
      Engine: options.engine || 'neural',
      SampleRate: options.sampleRate || '16000',
    };

    const command = new SynthesizeSpeechCommand(params);
    const response = await this.polly.send(command);
    const audioArray = await streamCollector(response.AudioStream);
    this.logger.info?.('Polly synthesized speech', {
      voiceId: params.VoiceId,
      outputFormat: params.OutputFormat,
    });

    return Buffer.from(audioArray);
  }

  /**
   * Synthesize text and upload it to the configured S3 bucket, returning { bucket, key }.
   * @param {string} text
   * @param {object} metadata Additional metadata to persist with the object
   */
  async synthesizeToS3(text, metadata = {}) {
    if (!this.s3) {
      throw new Error('AwsTtsAdapter requires aws.polly.outputBucket or aws.s3.mediaBucket for synthesizeToS3');
    }

    const audioBuffer = await this.synthesize(text, metadata);
    const key = `${this.s3Prefix}${uuidv4()}.pcm`;
    const params = {
      Bucket: this.s3Bucket,
      Key: key,
      Body: audioBuffer,
      ContentType: 'audio/pcm',
      Metadata: {
        voiceId: metadata.voiceId || this.voiceId,
        ...Object.keys(metadata).reduce((acc, k) => {
          const value = metadata[k];
          if (value !== undefined && value !== null) {
            acc[k.toString().toLowerCase()] = String(value);
          }
          return acc;
        }, {}),
      },
    };

    const command = new PutObjectCommand(params);
    await this.s3.send(command);
    this.logger.info?.('Uploaded Polly audio to S3', { bucket: params.Bucket, key: params.Key });

    return { bucket: params.Bucket, key: params.Key };
  }
}

module.exports = AwsTtsAdapter;
