require('dotenv').config();
const { Buffer } = require('node:buffer');
const EventEmitter = require('events');
const fetch = require('node-fetch');
const config = require('../config');

const TTS_CACHE_TTL_MS = 15 * 60 * 1000;
const TTS_CACHE_MAX_ITEMS = 200;
const ttsCache = new Map(); // key -> { audio, at }
const ttsInflight = new Map(); // key -> Promise

function buildTtsCacheKey(text, voiceModel) {
  const cleanText = String(text || '').trim();
  return `${voiceModel || 'default'}::${cleanText}`;
}

function pruneTtsCache() {
  if (ttsCache.size <= TTS_CACHE_MAX_ITEMS) return;
  const entries = [...ttsCache.entries()].sort((a, b) => a[1].at - b[1].at);
  const overflow = entries.length - TTS_CACHE_MAX_ITEMS;
  for (let i = 0; i < overflow; i += 1) {
    ttsCache.delete(entries[i][0]);
  }
}

class TextToSpeechService extends EventEmitter {
  constructor(options = {}) {
    super();
    this.nextExpectedIndex = 0;
    this.speechBuffer = {};
    this.voiceModel = options.voiceModel || null;
    
    // Validate required environment variables
    if (!config.deepgram.apiKey) {
      console.error('❌ DEEPGRAM_API_KEY is not set');
    }
    if (!config.deepgram.voiceModel) {
      console.warn('⚠️ VOICE_MODEL not set, using default');
    }
    
    const activeVoice = this.voiceModel || config.deepgram.voiceModel || 'default';
    console.log(`🎵 TTS Service initialized with voice model: ${activeVoice}`);
  }

  async fetchSpeechAudio(text, voiceModel) {
    const url = `https://api.deepgram.com/v1/speak?model=${voiceModel}&encoding=mulaw&sample_rate=8000&container=none`;
    console.log(`🌐 Making TTS request to: ${url}`.gray);
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Authorization': `Token ${config.deepgram.apiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ text }),
      timeout: 10000
    });

    console.log(`📡 TTS Response status: ${response.status}`.blue);
    if (response.status !== 200) {
      const errorText = await response.text();
      console.error('❌ Deepgram TTS error:');
      console.error('Status:', response.status);
      console.error('Status Text:', response.statusText);
      console.error('Error Response:', errorText);
      throw new Error(`TTS API error: ${response.status} - ${response.statusText}`);
    }

    const audioBuffer = await response.buffer();
    return Buffer.from(audioBuffer).toString('base64');
  }

  async generate(gptReply, interactionCount, options = {}) {
    const { partialResponseIndex, partialResponse } = gptReply || {};
    const silent = !!options.silent;

    if (!partialResponse) { 
      console.warn('⚠️ TTS: No partialResponse provided');
      return; 
    }

    console.log(`🎵 TTS generating for: "${partialResponse.substring(0, 50)}..."`.cyan);

    try {
      const voiceModel = options.voiceModel || this.voiceModel || config.deepgram.voiceModel || 'aura-asteria-en';
      const key = buildTtsCacheKey(partialResponse, voiceModel);
      const cached = ttsCache.get(key);
      const now = Date.now();
      if (cached && now - cached.at < TTS_CACHE_TTL_MS) {
        if (!silent) {
          this.emit('speech', partialResponseIndex, cached.audio, partialResponse, interactionCount);
        }
        return;
      }
      if (cached) {
        ttsCache.delete(key);
      }

      if (ttsInflight.has(key)) {
        const sharedAudio = await ttsInflight.get(key);
        if (sharedAudio && !silent) {
          this.emit('speech', partialResponseIndex, sharedAudio, partialResponse, interactionCount);
        }
        return;
      }

      const requestPromise = (async () => {
        try {
          const base64String = await this.fetchSpeechAudio(partialResponse, voiceModel);
          ttsCache.set(key, { audio: base64String, at: Date.now() });
          pruneTtsCache();
          return base64String;
        } catch (primaryError) {
          const fallbackVoice = config.deepgram.voiceModel || 'aura-asteria-en';
          if (fallbackVoice && fallbackVoice !== voiceModel) {
            try {
              const base64String = await this.fetchSpeechAudio(partialResponse, fallbackVoice);
              const fallbackKey = buildTtsCacheKey(partialResponse, fallbackVoice);
              ttsCache.set(fallbackKey, { audio: base64String, at: Date.now() });
              pruneTtsCache();
              return base64String;
            } catch (fallbackError) {
              throw fallbackError;
            }
          }
          throw primaryError;
        }
      })();

      ttsInflight.set(key, requestPromise);
      const base64String = await requestPromise.finally(() => {
        ttsInflight.delete(key);
      });
      if (base64String && !silent) {
        console.log(`✅ TTS audio generated, size: ${base64String.length} chars`.green);
        this.emit('speech', partialResponseIndex, base64String, partialResponse, interactionCount);
      }
    } catch (err) {
      console.error('❌ Error occurred in TextToSpeech service:', err.message);
      console.error('Error stack:', err.stack);
      
      // Emit an error event so the caller can handle it
      this.emit('error', err);
      
      // Don't throw the error to prevent crashing the call
      // Instead, try to continue without this audio
    }
  }
}

module.exports = { TextToSpeechService };
