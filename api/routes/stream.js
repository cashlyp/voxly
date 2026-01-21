const EventEmitter = require('events');
const uuid = require('uuid');

class StreamService extends EventEmitter {
  constructor(websocket, options = {}) {
    super();
    this.ws = websocket;
    this.expectedAudioIndex = 0;
    this.audioBuffer = {};
    this.streamSid = '';
    const interval = Number(options.audioTickIntervalMs);
    this.audioTickIntervalMs = Number.isFinite(interval) && interval > 0 ? interval : 160;
    this.audioTickTimer = null;
    const maxBuffered = Number(options.maxBufferedChunks);
    this.maxBufferedChunks = Number.isFinite(maxBuffered) && maxBuffered > 0 ? maxBuffered : 64;
  }

  setStreamSid (streamSid) {
    this.streamSid = streamSid;
  }

  buffer (index, audio) {
    // Escape hatch for intro message, which doesn't have an index
    if(index === null) {
      this.sendAudio(audio);
      return;
    }

    const normalizedIndex = Number(index);
    if (!Number.isFinite(normalizedIndex)) {
      this.sendAudio(audio);
      return;
    }

    if (normalizedIndex < this.expectedAudioIndex) {
      return;
    }

    if (Object.keys(this.audioBuffer).length >= this.maxBufferedChunks) {
      this.audioBuffer = {};
      this.expectedAudioIndex = normalizedIndex;
    }

    if(normalizedIndex === this.expectedAudioIndex) {
      this.sendAudio(audio);
      this.expectedAudioIndex++;

      while(Object.prototype.hasOwnProperty.call(this.audioBuffer, this.expectedAudioIndex)) {
        const bufferedAudio = this.audioBuffer[this.expectedAudioIndex];
        this.sendAudio(bufferedAudio);
        this.expectedAudioIndex++;
      }
    } else {
      this.audioBuffer[normalizedIndex] = audio;
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
    if (!base64) return { durationMs: 0, level: null, levels: [] };
    let buffer;
    try {
      buffer = Buffer.from(base64, 'base64');
    } catch (_) {
      return { durationMs: 0, level: null, levels: [] };
    }
    const length = buffer.length;
    if (!length) return { durationMs: 0, level: null, levels: [] };
    const durationMs = Math.round((length / 8000) * 1000);
    const maxFrames = 48;
    const frames = Math.min(maxFrames, Math.max(1, Math.ceil(durationMs / this.audioTickIntervalMs)));
    const bytesPerFrame = Math.max(1, Math.floor(length / frames));
    const levels = new Array(frames).fill(0);
    let total = 0;
    let totalCount = 0;
    for (let frame = 0; frame < frames; frame += 1) {
      const start = frame * bytesPerFrame;
      const end = frame === frames - 1 ? length : Math.min(length, start + bytesPerFrame);
      const span = Math.max(1, end - start);
      const step = Math.max(1, Math.floor(span / 120));
      let sum = 0;
      let count = 0;
      for (let i = start; i < end; i += step) {
        sum += Math.abs(buffer[i] - 128);
        count += 1;
      }
      const level = count ? Math.max(0, Math.min(1, sum / (count * 128))) : 0;
      levels[frame] = level;
      total += sum;
      totalCount += count;
    }
    const level = totalCount ? Math.max(0, Math.min(1, total / (totalCount * 128))) : null;
    return { durationMs, level, levels };
  }

  startAudioTicks (audio) {
    if (this.audioTickTimer) {
      clearInterval(this.audioTickTimer);
      this.audioTickTimer = null;
    }
    const { durationMs, level, levels } = this.estimateAudioStats(audio);
    const totalFrames = levels?.length || 0;
    this.emit('audiotick', { level, progress: 0, durationMs, frameIndex: 0, frames: totalFrames });
    if (!durationMs || durationMs <= this.audioTickIntervalMs || !totalFrames) {
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
      const progress = elapsed / durationMs;
      const idx = Math.min(totalFrames - 1, Math.floor(progress * totalFrames));
      const frameLevel = Number.isFinite(levels[idx]) ? levels[idx] : level;
      this.emit('audiotick', {
        level: frameLevel,
        progress,
        durationMs,
        frameIndex: idx,
        frames: totalFrames
      });
    }, this.audioTickIntervalMs);
  }
}

module.exports = {StreamService};
