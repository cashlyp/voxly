const EventEmitter = require("events");

let WebSocketCtor = typeof WebSocket !== "undefined" ? WebSocket : null;
if (!WebSocketCtor) {
  try {
    WebSocketCtor = require("ws");
  } catch (_) {
    WebSocketCtor = null;
  }
}

const DEFAULT_ENDPOINT = "wss://agent.deepgram.com/v1/agent/converse";
const DEFAULT_KEEPALIVE_MS = 15000;

function toAudioBase64(value) {
  if (!value) return null;
  if (Buffer.isBuffer(value)) {
    return value.toString("base64");
  }
  if (typeof value === "string") {
    return value.trim() || null;
  }
  return null;
}

function looksLikeJson(text) {
  const value = String(text || "").trim();
  return value.startsWith("{") || value.startsWith("[");
}

function asRole(value) {
  const role = String(value || "").toLowerCase();
  if (["assistant", "agent", "ai", "bot"].includes(role)) return "ai";
  return "user";
}

function normalizeAgentAudioEncoding(value, fallback = "mulaw") {
  const raw = String(value || "")
    .trim()
    .toLowerCase();
  if (!raw) return fallback;
  if (["mulaw", "mu-law", "pcm_mulaw", "audio/pcmu", "pcmu"].includes(raw)) {
    return "mulaw";
  }
  if (["linear16", "l16", "pcm16", "audio/l16"].includes(raw)) {
    return "linear16";
  }
  return fallback;
}

function asPositiveSampleRate(value, fallback = 8000) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.round(parsed);
}

function attachSocketListener(socket, event, handler) {
  if (!socket || typeof handler !== "function") return;
  if (typeof socket.on === "function") {
    socket.on(event, handler);
    return;
  }
  if (typeof socket.addEventListener !== "function") return;
  socket.addEventListener(event, (payload) => {
    if (event === "message") {
      handler(payload?.data ?? payload);
      return;
    }
    if (event === "close") {
      handler(payload?.code, payload?.reason);
      return;
    }
    if (event === "error") {
      handler(payload?.error || payload);
      return;
    }
    handler(payload);
  });
}

function attachSocketOnce(socket, event, handler) {
  if (!socket || typeof handler !== "function") return;
  if (typeof socket.once === "function") {
    socket.once(event, handler);
    return;
  }
  if (typeof socket.addEventListener !== "function") return;
  const wrapped = (payload) => {
    socket.removeEventListener(event, wrapped);
    if (event === "close") {
      handler(payload?.code, payload?.reason);
      return;
    }
    handler(payload);
  };
  socket.addEventListener(event, wrapped);
}

function socketIsOpen(socket) {
  if (!socket) return false;
  const state = socket.readyState;
  if (typeof state !== "number") return false;
  if (typeof WebSocketCtor?.OPEN === "number") {
    return state === WebSocketCtor.OPEN;
  }
  return state === 1;
}

function normalizeMessageData(value) {
  if (value == null) return null;
  if (Buffer.isBuffer(value)) return value;
  if (typeof value === "string") return value;
  if (value instanceof ArrayBuffer) {
    return Buffer.from(value);
  }
  if (ArrayBuffer.isView(value)) {
    return Buffer.from(value.buffer, value.byteOffset, value.byteLength);
  }
  if (typeof value?.arrayBuffer === "function") {
    return value.arrayBuffer().then((buffer) => Buffer.from(buffer));
  }
  return value;
}

class VoiceAgentBridge extends EventEmitter {
  constructor(options = {}) {
    super();
    this.apiKey = options.apiKey;
    this.endpoint = options.endpoint || DEFAULT_ENDPOINT;
    this.keepAliveMs = Number.isFinite(Number(options.keepAliveMs))
      ? Number(options.keepAliveMs)
      : DEFAULT_KEEPALIVE_MS;
    this.listenModel = options.listenModel || "nova-3";
    this.thinkProviderType = options.thinkProviderType || "open_ai";
    this.thinkModel = options.thinkModel || "gpt-4o-mini";
    this.speakModel = options.speakModel || null;
    this.ws = null;
    this.keepAliveTimer = null;
  }

  isOpen() {
    return socketIsOpen(this.ws);
  }

  async connect(session = {}) {
    if (!WebSocketCtor) {
      throw new Error('Missing dependency "ws" required for Voice Agent bridge');
    }
    if (!this.apiKey) {
      throw new Error("DEEPGRAM_API_KEY is required for Voice Agent bridge");
    }
    if (this.isOpen()) return;

    await this.close();

    await new Promise((resolve, reject) => {
      const headers = {
        Authorization: `Token ${this.apiKey}`,
      };
      this.ws = new WebSocketCtor(this.endpoint, { headers });

      let settled = false;
      const settle = (err) => {
        if (settled) return;
        settled = true;
        if (err) reject(err);
        else resolve();
      };

      attachSocketListener(this.ws, "open", () => {
        try {
          this.sendSettings(session);
          this.startKeepAlive();
          this.emit("ready");
          settle();
        } catch (error) {
          settle(error);
        }
      });

      attachSocketListener(this.ws, "message", async (message) => {
        try {
          const normalized = await normalizeMessageData(message);
          this.handleAgentMessage(normalized);
        } catch (error) {
          this.emit("error", error);
        }
      });

      attachSocketListener(this.ws, "error", (error) => {
        this.emit("error", error);
        settle(error);
      });

      attachSocketListener(this.ws, "close", (code, reason) => {
        this.stopKeepAlive();
        this.emit("close", { code, reason: reason ? reason.toString() : "" });
      });
    });
  }

  sendSettings(session = {}) {
    const prompt =
      session.prompt ||
      "You are a concise voice assistant. Keep responses short and clear.";
    const greeting =
      session.firstMessage || "Hello. How can I help you today?";
    const listenModel = session.listenModel || this.listenModel;
    const thinkModel = session.thinkModel || this.thinkModel;
    const speakModel =
      session.voiceModel || session.speakModel || this.speakModel || "aura-2-thalia-en";
    const inputEncoding = normalizeAgentAudioEncoding(
      session.inputEncoding,
      "mulaw",
    );
    const inputSampleRate = asPositiveSampleRate(session.inputSampleRate, 8000);
    const outputEncoding = normalizeAgentAudioEncoding(
      session.outputEncoding,
      inputEncoding,
    );
    const outputSampleRate = asPositiveSampleRate(
      session.outputSampleRate,
      inputSampleRate,
    );

    const settings = {
      type: "Settings",
      audio: {
        input: {
          encoding: inputEncoding,
          sample_rate: inputSampleRate,
        },
        output: {
          encoding: outputEncoding,
          sample_rate: outputSampleRate,
          container: "none",
        },
      },
      agent: {
        language: "en",
        listen: {
          provider: {
            type: "deepgram",
            model: listenModel,
          },
        },
        think: {
          provider: {
            type: this.thinkProviderType,
            model: thinkModel,
          },
          prompt,
        },
        speak: {
          provider: {
            type: "deepgram",
            model: speakModel,
          },
        },
        greeting,
      },
    };

    if (Array.isArray(session.functions) && session.functions.length) {
      settings.agent.think.functions = session.functions;
    }
    this.sendJson(settings);
  }

  startKeepAlive() {
    this.stopKeepAlive();
    if (!this.keepAliveMs || this.keepAliveMs <= 0) return;
    this.keepAliveTimer = setInterval(() => {
      if (!this.isOpen()) return;
      this.sendJson({ type: "KeepAlive" });
    }, this.keepAliveMs);
  }

  stopKeepAlive() {
    if (!this.keepAliveTimer) return;
    clearInterval(this.keepAliveTimer);
    this.keepAliveTimer = null;
  }

  sendJson(payload = {}) {
    if (!this.isOpen()) return;
    this.ws.send(JSON.stringify(payload));
  }

  sendTwilioAudio(base64Audio) {
    if (!this.isOpen()) return;
    if (!base64Audio) return;
    const audio = Buffer.from(base64Audio, "base64");
    this.ws.send(audio);
  }

  sendFunctionResponse(callId, result) {
    const response =
      typeof result === "string" ? result : JSON.stringify(result || {});
    this.sendJson({
      type: "FunctionCallResponse",
      function_call_id: callId,
      output: response,
    });
  }

  updatePrompt(prompt) {
    if (!prompt) return;
    this.sendJson({ type: "UpdatePrompt", prompt: String(prompt) });
  }

  updateSpeak(model) {
    if (!model) return;
    this.sendJson({
      type: "UpdateSpeak",
      speak: {
        provider: {
          type: "deepgram",
          model: String(model),
        },
      },
    });
  }

  injectUserMessage(text) {
    if (!text) return;
    this.sendJson({ type: "InjectUserMessage", text: String(text) });
  }

  extractAudioFromJson(message) {
    return (
      toAudioBase64(message?.audio) ||
      toAudioBase64(message?.audio_base64) ||
      toAudioBase64(message?.output_audio) ||
      toAudioBase64(message?.data?.audio) ||
      null
    );
  }

  handleAgentMessage(payload) {
    if (payload == null) return;
    if (Buffer.isBuffer(payload)) {
      this.emit("audio", payload.toString("base64"));
      return;
    }

    const text = String(payload || "");
    if (!looksLikeJson(text)) {
      return;
    }

    let parsed;
    try {
      parsed = JSON.parse(text);
    } catch (_) {
      return;
    }

    const type = String(parsed?.type || parsed?.event || parsed?.kind || "");
    const normalizedType = type.toLowerCase();

    const jsonAudio = this.extractAudioFromJson(parsed);
    if (jsonAudio) {
      this.emit("audio", jsonAudio);
    }

    if (normalizedType.includes("error")) {
      const details =
        parsed?.description || parsed?.message || "Voice Agent error";
      this.emit("error", new Error(details));
      return;
    }

    if (normalizedType.includes("functioncallrequest")) {
      const callId =
        parsed?.function_call_id || parsed?.id || parsed?.call_id || null;
      const functionName =
        parsed?.function_name || parsed?.name || parsed?.function?.name || "";
      const argumentsRaw =
        parsed?.arguments || parsed?.args || parsed?.function?.arguments || {};
      this.emit("functionCallRequest", {
        id: callId,
        name: functionName,
        arguments: argumentsRaw,
      });
      return;
    }

    if (normalizedType.includes("conversationtext")) {
      const role = asRole(parsed?.role || parsed?.speaker);
      const textValue = parsed?.text || parsed?.content || "";
      if (textValue) {
        this.emit("conversationText", { role, text: String(textValue) });
      }
      return;
    }

    if (parsed?.role && (parsed?.text || parsed?.content)) {
      const role = asRole(parsed.role);
      const textValue = parsed.text || parsed.content;
      this.emit("conversationText", { role, text: String(textValue) });
      return;
    }

    this.emit("event", parsed);
  }

  async close() {
    this.stopKeepAlive();
    if (!this.ws) return;
    const socket = this.ws;
    this.ws = null;
    if (socket.readyState === 0 || socketIsOpen(socket)) {
      await new Promise((resolve) => {
        attachSocketOnce(socket, "close", () => resolve());
        socket.close();
      }).catch(() => {});
    }
  }
}

module.exports = {
  VoiceAgentBridge,
};
