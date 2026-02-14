require('colors');
const { createClient, LiveTranscriptionEvents } = require('@deepgram/sdk');
const { Buffer } = require('node:buffer');
const EventEmitter = require('events');
const config = require('../config');


class TranscriptionService extends EventEmitter {
  constructor(options = {}) {
    super();
    const apiKey = String(config.deepgram?.apiKey || '').trim();
    if (!apiKey) {
      throw new Error('DEEPGRAM_API_KEY is not configured');
    }
    const deepgram = createClient(apiKey);
    const encoding = options.encoding || 'mulaw';
    const sampleRate = options.sampleRate || 8000;
    const model = options.model || config.deepgram.model;
    this.closed = false;
    this.dgConnection = deepgram.listen.live({
      encoding: encoding,
      sample_rate: String(sampleRate),
      model: model,
      punctuate: true,
      interim_results: true,
      endpointing: 200,
      utterance_end_ms: 1000
    });

    this.finalResult = '';
    this.speechFinal = false; // used to determine if we have seen speech_final=true indicating that deepgram detected a natural pause in the speakers speech. 

    this.dgConnection.on(LiveTranscriptionEvents.Open, () => {
      console.log('STT -> Deepgram connection open'.green);
    });

    this.dgConnection.on(LiveTranscriptionEvents.Transcript, (transcriptionEvent) => {
      const alternatives = transcriptionEvent.channel?.alternatives;
      let text = '';
      if (alternatives) {
        text = alternatives[0]?.transcript;
      }

      // if we receive an UtteranceEnd and speech_final has not already happened then we should consider this the end of of the human speech and emit the transcription
      if (transcriptionEvent.type === 'UtteranceEnd') {
        if (!this.speechFinal) {
          console.log(`UtteranceEnd received before speechFinal, emit the text collected so far: ${this.finalResult}`.yellow);
          this.emit('transcription', this.finalResult);
          return;
        } else {
          console.log('STT -> Speech was already final when UtteranceEnd recevied'.yellow);
          return;
        }
      }

      // console.log(text, "is_final: ", transcription?.is_final, "speech_final: ", transcription.speech_final);
      // if is_final that means that this chunk of the transcription is accurate and we need to add it to the finalResult
      if (transcriptionEvent.is_final === true && text.trim().length > 0) {
        this.finalResult += ` ${text}`;
        // if speech_final and is_final that means this text is accurate and it's a natural pause in the speakers speech. We need to send this to the assistant for processing
        if (transcriptionEvent.speech_final === true) {
          this.speechFinal = true; // this will prevent a utterance end which shows up after speechFinal from sending another response
          this.emit('transcription', this.finalResult);
          this.finalResult = '';
        } else {
          // if we receive a message without speechFinal reset speechFinal to false, this will allow any subsequent utteranceEnd messages to properly indicate the end of a message
          this.speechFinal = false;
        }
      } else {
        this.emit('utterance', text);
      }
    });

    this.dgConnection.on(LiveTranscriptionEvents.Error, (error) => {
      console.error('STT -> deepgram error');
      console.error(error);
      this.emit('error', error);
    });

    this.dgConnection.on(LiveTranscriptionEvents.Warning, (warning) => {
      console.error('STT -> deepgram warning');
      console.error(warning);
      this.emit('warning', warning);
    });

    this.dgConnection.on(LiveTranscriptionEvents.Metadata, (metadata) => {
      console.error('STT -> deepgram metadata');
      console.error(metadata);
    });

    this.dgConnection.on(LiveTranscriptionEvents.Close, () => {
      console.log('STT -> Deepgram connection closed'.yellow);
      this.emit('close');
    });
  }

  /**
   * Send the payload to Deepgram
   * @param {String|Buffer} payload Base64 audio or raw buffer
   */
  send(payload) {
    if (!this.closed && this.dgConnection.getReadyState() === 1) {
      try {
        if (Buffer.isBuffer(payload)) {
          this.dgConnection.send(payload);
        } else {
          this.dgConnection.send(Buffer.from(payload, 'base64'));
        }
      } catch (error) {
        this.emit('error', error);
      }
    }
  }

  sendBuffer(buffer) {
    if (!this.closed && this.dgConnection.getReadyState() === 1 && Buffer.isBuffer(buffer)) {
      try {
        this.dgConnection.send(buffer);
      } catch (error) {
        this.emit('error', error);
      }
    }
  }

  close() {
    try {
      if (!this.dgConnection) return;
      if (this.closed) return;
      this.closed = true;
      if (typeof this.dgConnection.finish === 'function') {
        this.dgConnection.finish();
        return;
      }
      if (typeof this.dgConnection.requestClose === 'function') {
        this.dgConnection.requestClose();
      }
    } catch (error) {
      this.emit('error', error);
    }
  }
}

module.exports = { TranscriptionService };
