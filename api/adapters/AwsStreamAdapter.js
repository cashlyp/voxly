const EventEmitter = require('events');
const fetch = require('node-fetch');
const { TranscribeStreamingClient, StartStreamTranscriptionCommand } = require('@aws-sdk/client-transcribe-streaming');

/**
 * AwsStreamAdapter is designed to run inside an ECS/Fargate worker. It receives
 * PCM audio chunks (decoded from Kinesis Video Streams), forwards them to Amazon
 * Transcribe streaming, and POSTs the resulting transcripts back to the Node API.
 */
class AwsStreamAdapter extends EventEmitter {
  /**
   * @param {object} config AWS configuration
   * @param {string} config.region AWS region
  * @param {object} config.transcribe
   * @param {string} config.transcribe.languageCode Language code for transcription
   * @param {string} [config.transcribe.vocabularyFilterName] Optional vocabulary filter
   * @param {object} options
   * @param {string} options.apiBaseUrl Base URL of the Node API for transcript webhooks
   * @param {string} options.callSid Unique call identifier (mirrors Twilio SID concept)
   * @param {string} options.contactId Amazon Connect contact ID
   * @param {number} [options.sampleRate] PCM sample rate (default 16000)
   * @param {Console} [options.logger] optional logger
   */
  constructor(config, options = {}) {
    super();

    if (!config?.region) {
      throw new Error('AwsStreamAdapter requires aws.region');
    }
    if (!options.apiBaseUrl) {
      throw new Error('AwsStreamAdapter requires options.apiBaseUrl');
    }
    if (!options.callSid) {
      throw new Error('AwsStreamAdapter requires options.callSid');
    }
    if (!options.contactId) {
      throw new Error('AwsStreamAdapter requires options.contactId');
    }

    this.config = config;
    this.logger = options.logger || console;
    this.client = new TranscribeStreamingClient({ region: config.region });
    this.languageCode = config.transcribe?.languageCode || 'en-US';
    this.vocabularyFilterName = config.transcribe?.vocabularyFilterName;
    this.sampleRate = options.sampleRate || 16000;
    this.apiBaseUrl = options.apiBaseUrl.replace(/\/+$/, '');
    this.callSid = options.callSid;
    this.contactId = options.contactId;

    this.queue = [];
    this.waiters = [];
    this.closed = false;
    this.started = false;
  }

  /**
   * Push raw PCM audio (Buffer or Uint8Array). Base64 strings are also accepted.
   * The method resolves immediately; audio is streamed asynchronously.
   * @param {Buffer|string|Uint8Array} chunk
   */
  pushAudio(chunk) {
    if (this.closed) {
      throw new Error('AwsStreamAdapter is closed');
    }

    let buffer = chunk;
    if (typeof chunk === 'string') {
      buffer = Buffer.from(chunk, 'base64');
    } else if (!(chunk instanceof Buffer)) {
      buffer = Buffer.from(chunk);
    }

    this.queue.push(buffer);
    this._flush();
  }

  /**
   * Signal the transcription stream to finish.
   */
  async end() {
    this.closed = true;
    this.queue.push(null);
    this._flush();
    await this.finished;
  }

  /**
   * Begin the streaming transcription session.
   */
  async start() {
    if (this.started) {
      return this.finished;
    }
    this.started = true;

    const audioStream = this._createAsyncIterable();
    const command = new StartStreamTranscriptionCommand({
      LanguageCode: this.languageCode,
      MediaEncoding: 'pcm',
      MediaSampleRateHertz: this.sampleRate,
      VocabularyFilterName: this.vocabularyFilterName,
      AudioStream: audioStream,
    });

    this.logger.info?.('Starting Amazon Transcribe streaming session', {
      callSid: this.callSid,
      contactId: this.contactId,
      languageCode: this.languageCode,
    });

    const promise = this.client.send(command)
      .then(async (response) => {
        for await (const event of response.TranscriptResultStream) {
          for (const result of event.Transcript.Results || []) {
            const alternative = result.Alternatives?.[0];
            if (!alternative?.Transcript) {
              continue;
            }

            const transcriptText = alternative.Transcript.trim();
            if (!transcriptText) {
              continue;
            }

            const payload = {
              callSid: this.callSid,
              contactId: this.contactId,
              isPartial: result.IsPartial,
              transcript: transcriptText,
              startTime: result.StartTime,
              endTime: result.EndTime,
            };

            this.emit(result.IsPartial ? 'partial' : 'transcript', payload);
            await this._postTranscript(payload);
          }
        }
      })
      .catch((error) => {
        this.logger.error?.('Transcribe streaming error', { error: error.message });
        this.emit('error', error);
        throw error;
      })
      .finally(() => {
        this.closed = true;
        this.emit('closed');
      });

    this.finished = promise;
    return promise;
  }

  async _postTranscript(payload) {
    const url = `${this.apiBaseUrl}/aws/transcripts`;
    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-aws-contact-id': this.contactId,
        },
        body: JSON.stringify(payload),
      });
      if (!response.ok) {
        const text = await response.text();
        this.logger.error?.('Failed to POST transcript to API', {
          status: response.status,
          body: text,
        });
      }
    } catch (error) {
      this.logger.error?.('Error POSTing transcript to API', { error: error.message });
    }
  }

  _createAsyncIterable() {
    const self = this;
    return async function* audioGenerator() {
      while (true) {
        const chunk = await self._next();
        if (chunk === null) {
          return;
        }
        yield { AudioEvent: { AudioChunk: chunk } };
      }
    };
  }

  _next() {
    if (this.queue.length > 0) {
      return Promise.resolve(this.queue.shift());
    }

    return new Promise((resolve) => {
      this.waiters.push(resolve);
    });
  }

  _flush() {
    while (this.waiters.length > 0 && this.queue.length > 0) {
      const waiter = this.waiters.shift();
      waiter(this.queue.shift());
    }
  }
}

module.exports = AwsStreamAdapter;
