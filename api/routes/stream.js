const EventEmitter = require('events');
const uuid = require('uuid');

class StreamService extends EventEmitter {
  constructor(websocket) {
    super();
    this.ws = websocket;
    this.expectedAudioIndex = 0;
    this.audioBuffer = {};
    this.streamSid = '';
    this.audioTickIntervalMs = 250;
    this.audioTickTimer = null;
  }

  setStreamSid (streamSid) {
    this.streamSid = streamSid;
  }

  buffer (index, audio) {
    // Escape hatch for intro message, which doesn't have an index
    if(index === null) {
      this.sendAudio(audio);
    } else if(index === this.expectedAudioIndex) {
      this.sendAudio(audio);
      this.expectedAudioIndex++;

      while(Object.prototype.hasOwnProperty.call(this.audioBuffer, this.expectedAudioIndex)) {
        const bufferedAudio = this.audioBuffer[this.expectedAudioIndex];
        this.sendAudio(bufferedAudio);
        this.expectedAudioIndex++;
      }
    } else {
      this.audioBuffer[index] = audio;
    }
  }

  sendAudio (audio) {
    this.startAudioTicks(audio);
    this.ws.send(
      JSON.stringify({
        streamSid: this.streamSid,
        event: 'media',
        media: {
          payload: audio,
        },
      })
    );
    // When the media completes you will receive a `mark` message with the label
    const markLabel = uuid.v4();
    this.ws.send(
      JSON.stringify({
        streamSid: this.streamSid,
        event: 'mark',
        mark: {
          name: markLabel
        }
      })
    );
    this.emit('audiosent', markLabel);
  }

  estimateAudioStats (base64 = '') {
    if (!base64) return { durationMs: 0, level: null };
    let buffer;
    try {
      buffer = Buffer.from(base64, 'base64');
    } catch (_) {
      return { durationMs: 0, level: null };
    }
    const length = buffer.length;
    if (!length) return { durationMs: 0, level: null };
    const durationMs = Math.round((length / 8000) * 1000);
    const step = Math.max(1, Math.floor(length / 800));
    let sum = 0;
    let count = 0;
    for (let i = 0; i < length; i += step) {
      sum += Math.abs(buffer[i] - 128);
      count += 1;
    }
    const level = count ? Math.max(0, Math.min(1, sum / (count * 128))) : null;
    return { durationMs, level };
  }

  startAudioTicks (audio) {
    if (this.audioTickTimer) {
      clearInterval(this.audioTickTimer);
      this.audioTickTimer = null;
    }
    const { durationMs, level } = this.estimateAudioStats(audio);
    this.emit('audiotick', { level, progress: 0, durationMs });
    if (!durationMs || durationMs <= this.audioTickIntervalMs) {
      return;
    }
    const start = Date.now();
    this.audioTickTimer = setInterval(() => {
      const elapsed = Date.now() - start;
      if (elapsed >= durationMs) {
        clearInterval(this.audioTickTimer);
        this.audioTickTimer = null;
        return;
      }
      this.emit('audiotick', { level, progress: elapsed / durationMs, durationMs });
    }, this.audioTickIntervalMs);
  }
}

module.exports = {StreamService};
