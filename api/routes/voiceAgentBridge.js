const EventEmitter = require("events");
const { createClient, AgentEvents } = require("@deepgram/sdk");

const DEFAULT_ENDPOINT = "wss://agent.deepgram.com/v1/agent/converse";
const DEFAULT_KEEPALIVE_MS = 8000;
const WELCOME_FALLBACK_SEND_SETTINGS_MS = 1500;
const SETTINGS_APPLIED_TIMEOUT_MS = 10000;
const CLOSE_WAIT_TIMEOUT_MS = 1500;
const MAX_BUFFERED_AUDIO_FRAMES = 200;

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

function asRole(value) {
  const role = String(value || "").toLowerCase();
  if (["assistant", "agent", "ai", "bot"].includes(role)) return "ai";
  return "user";
}

function sanitizeErrorMessage(error) {
  const raw =
    error?.message ||
    error?.error?.message ||
    error?.description ||
    error?.type ||
    "Voice Agent error";
  let safe = String(raw || "Voice Agent error");

  // Redact phone-like values in logs/errors.
  safe = safe.replace(/\+?\d[\d\s().-]{6,}\d/g, (match) => {
    const digits = String(match || "").replace(/\D/g, "");
    if (digits.length < 4) return "[redacted-phone]";
    const suffix = digits.slice(-2);
    return `${"*".repeat(Math.max(4, digits.length - 2))}${suffix}`;
  });

  // Redact email-like values in logs/errors.
  safe = safe.replace(
    /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi,
    "[redacted-email]",
  );

  if (safe.length > 240) {
    safe = `${safe.slice(0, 220)}...[redacted]`;
  }
  return safe;
}

function toError(error) {
  if (error instanceof Error) {
    return new Error(sanitizeErrorMessage(error));
  }
  return new Error(sanitizeErrorMessage(error));
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

    this.connection = null;
    this.deepgram = null;
    this.keepAliveTimer = null;
    this.settingsApplied = false;
    this.pendingAudioFrames = [];
    this.dropAgentAudio = false;
  }

  isOpen() {
    return Boolean(this.connection?.isConnected?.());
  }

  validateConfig() {
    const missing = [];
    if (!String(this.apiKey || "").trim()) {
      missing.push("DEEPGRAM_API_KEY");
    }
    if (!String(this.endpoint || "").trim()) {
      missing.push("DEEPGRAM_VOICE_AGENT_ENDPOINT");
    }
    if (!missing.length) return;
    throw new Error(
      `Deepgram Voice Agent configuration error: missing required env ${missing.join(", ")}`,
    );
  }

  buildSettings(session = {}) {
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
      },
      // Keep both variants for compatibility with existing payload expectations.
      greeting,
    };

    if (Array.isArray(session.functions) && session.functions.length) {
      settings.agent.think.functions = session.functions;
    }
    return settings;
  }

  async connect(session = {}) {
    this.validateConfig();
    if (this.isOpen() && this.settingsApplied) return;

    await this.close();
    this.settingsApplied = false;
    this.dropAgentAudio = false;
    this.pendingAudioFrames = [];

    this.deepgram = createClient(this.apiKey, {
      agent: {
        websocket: {
          options: {
            url: this.endpoint,
          },
        },
      },
    });
    this.connection = this.deepgram.agent();

    await new Promise((resolve, reject) => {
      const conn = this.connection;
      if (!conn) {
        reject(new Error("Failed to initialize Deepgram Voice Agent connection"));
        return;
      }

      let settled = false;
      let settingsSent = false;
      let welcomeFallbackTimer = null;
      let settingsAckTimer = null;

      const settle = (err) => {
        if (settled) return;
        settled = true;
        if (welcomeFallbackTimer) {
          clearTimeout(welcomeFallbackTimer);
          welcomeFallbackTimer = null;
        }
        if (settingsAckTimer) {
          clearTimeout(settingsAckTimer);
          settingsAckTimer = null;
        }
        if (err) {
          this.settingsApplied = false;
          reject(toError(err));
          return;
        }
        resolve();
      };

      const sendSettingsOnce = () => {
        if (settingsSent || !conn) return;
        settingsSent = true;
        try {
          conn.configure(this.buildSettings(session));
        } catch (error) {
          settle(
            new Error(
              `Voice Agent configure failed: ${sanitizeErrorMessage(error)}`,
            ),
          );
          return;
        }
        settingsAckTimer = setTimeout(() => {
          if (this.settingsApplied) return;
          settle(
            new Error(
              "Voice Agent SettingsApplied timeout: agent did not confirm configuration",
            ),
          );
        }, SETTINGS_APPLIED_TIMEOUT_MS);
      };

      conn.on(AgentEvents.Open, () => {
        welcomeFallbackTimer = setTimeout(() => {
          sendSettingsOnce();
        }, WELCOME_FALLBACK_SEND_SETTINGS_MS);
      });

      conn.on(AgentEvents.Welcome, () => {
        sendSettingsOnce();
      });

      conn.on(AgentEvents.SettingsApplied, (event) => {
        this.settingsApplied = true;
        this.startKeepAlive();
        this.flushPendingAudio();
        this.emit("ready");
        this.emit("event", event || { type: "SettingsApplied" });
        settle();
      });

      conn.on(AgentEvents.Audio, (data) => {
        if (this.dropAgentAudio) return;
        if (!data) return;
        const buffer = Buffer.isBuffer(data) ? data : Buffer.from(data);
        if (!buffer.length) return;
        this.emit("audio", buffer.toString("base64"));
      });

      conn.on(AgentEvents.ConversationText, (message) => {
        const role = asRole(message?.role);
        const text = String(message?.content || message?.text || "").trim();
        if (!text) return;
        this.emit("conversationText", { role, text });
      });

      conn.on(AgentEvents.FunctionCallRequest, (message) => {
        const calls = Array.isArray(message?.functions)
          ? message.functions
          : [message?.function || message].filter(Boolean);
        calls.forEach((fn) => {
          const callId = fn?.id || fn?.function_call_id || fn?.call_id || null;
          const functionName = fn?.name || fn?.function_name || "";
          const argumentsRaw =
            fn?.arguments || fn?.args || fn?.function?.arguments || {};
          this.emit("functionCallRequest", {
            id: callId,
            name: functionName,
            arguments: argumentsRaw,
            clientSide: fn?.client_side !== false,
          });
        });
      });

      conn.on(AgentEvents.UserStartedSpeaking, (event) => {
        // Barge-in: stop forwarding in-progress agent audio until the current turn ends.
        this.dropAgentAudio = true;
        this.emit("event", event || { type: "UserStartedSpeaking" });
      });

      conn.on(AgentEvents.AgentAudioDone, (event) => {
        this.dropAgentAudio = false;
        this.emit("event", event || { type: "AgentAudioDone" });
      });

      conn.on(AgentEvents.AgentStartedSpeaking, (event) => {
        this.emit("event", event || { type: "AgentStartedSpeaking" });
      });

      conn.on(AgentEvents.AgentThinking, (event) => {
        this.emit("event", event || { type: "AgentThinking" });
      });

      conn.on(AgentEvents.InjectionRefused, (event) => {
        this.emit("event", event || { type: "InjectionRefused" });
      });

      conn.on(AgentEvents.PromptUpdated, (event) => {
        this.emit("event", event || { type: "PromptUpdated" });
      });

      conn.on(AgentEvents.SpeakUpdated, (event) => {
        this.emit("event", event || { type: "SpeakUpdated" });
      });

      conn.on(AgentEvents.Unhandled, (event) => {
        this.emit("event", event || { type: "Unhandled" });
      });

      conn.on(AgentEvents.Error, (error) => {
        const safeError = toError(error);
        this.emit("error", safeError);
        if (!this.settingsApplied) {
          settle(safeError);
        }
      });

      conn.on(AgentEvents.Close, (event) => {
        this.stopKeepAlive();
        this.settingsApplied = false;
        const closeInfo = {
          code: Number(event?.code || 0),
          reason: sanitizeErrorMessage(event?.reason || ""),
        };
        this.emit("close", closeInfo);
        if (!settled) {
          settle(
            new Error(
              `Voice Agent socket closed before ready (code=${closeInfo.code || "unknown"}, reason=${closeInfo.reason || "none"})`,
            ),
          );
        }
      });
    });
  }

  startKeepAlive() {
    this.stopKeepAlive();
    if (!this.keepAliveMs || this.keepAliveMs <= 0) return;
    this.keepAliveTimer = setInterval(() => {
      if (!this.isOpen() || !this.connection) return;
      try {
        this.connection.keepAlive();
      } catch (_) {
        // Keepalive is best effort. Errors are propagated via AgentEvents.Error.
      }
    }, this.keepAliveMs);
  }

  stopKeepAlive() {
    if (!this.keepAliveTimer) return;
    clearInterval(this.keepAliveTimer);
    this.keepAliveTimer = null;
  }

  bufferPendingAudio(base64Audio) {
    if (!base64Audio) return;
    this.pendingAudioFrames.push(base64Audio);
    if (this.pendingAudioFrames.length > MAX_BUFFERED_AUDIO_FRAMES) {
      this.pendingAudioFrames.splice(
        0,
        this.pendingAudioFrames.length - MAX_BUFFERED_AUDIO_FRAMES,
      );
    }
  }

  flushPendingAudio() {
    if (!this.settingsApplied || !this.isOpen()) return;
    if (!this.pendingAudioFrames.length) return;
    const frames = this.pendingAudioFrames.splice(0);
    frames.forEach((base64Audio) => {
      if (!base64Audio || !this.connection) return;
      const audio = Buffer.from(base64Audio, "base64");
      if (!audio.length) return;
      this.connection.send(audio);
    });
  }

  sendTwilioAudio(base64Audio) {
    if (!base64Audio) return;
    if (!this.settingsApplied || !this.isOpen() || !this.connection) {
      this.bufferPendingAudio(base64Audio);
      return;
    }
    if (this.pendingAudioFrames.length) {
      this.flushPendingAudio();
    }
    const audio = Buffer.from(base64Audio, "base64");
    if (!audio.length) return;
    this.connection.send(audio);
  }

  sendFunctionResponse(callId, result, functionName = null) {
    if (!this.connection || !this.isOpen()) return;
    if (!callId) return;
    const responseContent =
      typeof result === "string" ? result : JSON.stringify(result || {});
    this.connection.functionCallResponse({
      id: String(callId),
      name: String(functionName || "function_call"),
      content: responseContent,
    });
  }

  updatePrompt(prompt) {
    if (!prompt || !this.connection || !this.isOpen()) return;
    this.connection.updatePrompt(String(prompt));
  }

  updateSpeak(model) {
    if (!model || !this.connection || !this.isOpen()) return;
    this.connection.updateSpeak({
      provider: {
        type: "deepgram",
        model: String(model),
      },
    });
  }

  injectUserMessage(text) {
    if (!text || !this.connection || !this.isOpen()) return;
    // SDK helper currently provides injectAgentMessage(). We send protocol-level
    // InjectUserMessage for keypad webhook integration parity.
    this.connection.send(
      JSON.stringify({
        type: "InjectUserMessage",
        content: String(text),
      }),
    );
  }

  async close() {
    this.stopKeepAlive();
    this.settingsApplied = false;
    this.dropAgentAudio = false;
    this.pendingAudioFrames = [];

    if (!this.connection) return;
    const conn = this.connection;
    this.connection = null;

    await new Promise((resolve) => {
      let done = false;
      const finalize = () => {
        if (done) return;
        done = true;
        resolve();
      };
      const timeout = setTimeout(() => {
        finalize();
      }, CLOSE_WAIT_TIMEOUT_MS);

      try {
        if (typeof conn.once === "function") {
          conn.once(AgentEvents.Close, () => {
            clearTimeout(timeout);
            finalize();
          });
        }
      } catch (_) {
        clearTimeout(timeout);
        finalize();
      }

      try {
        conn.disconnect();
      } catch (_) {
        clearTimeout(timeout);
        finalize();
      }
    }).catch(() => {});
  }
}

module.exports = {
  VoiceAgentBridge,
};
