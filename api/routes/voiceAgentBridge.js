const EventEmitter = require("events");
const { Buffer } = require("node:buffer");
const { createClient, AgentEvents } = require("@deepgram/sdk");

function toFiniteNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function normalizeText(value, fallback = "") {
  if (value === undefined || value === null) return fallback;
  const text = String(value).trim();
  return text || fallback;
}

function sanitizeFunctionDefinition(tool) {
  const source = tool && typeof tool === "object" && tool.function
    ? tool.function
    : tool;
  const name = normalizeText(source?.name);
  if (!name) return null;
  return {
    name,
    description: normalizeText(source?.description) || undefined,
    parameters:
      source?.parameters && typeof source.parameters === "object"
        ? source.parameters
        : { type: "object", properties: {}, required: [] },
  };
}

function sanitizeThinkConfig(thinkConfig = {}, managedThinkOnly = true) {
  if (managedThinkOnly && thinkConfig?.endpoint?.url) {
    throw new Error(
      "voice_agent_managed_think_requires_no_endpoint",
    );
  }
  const functionEntries = Array.isArray(thinkConfig?.functions)
    ? thinkConfig.functions
    : [];
  if (
    managedThinkOnly &&
    functionEntries.some((entry) => entry?.endpoint?.url)
  ) {
    throw new Error(
      "voice_agent_managed_think_requires_no_function_endpoints",
    );
  }

  const providerType = normalizeText(thinkConfig?.provider?.type, "open_ai");
  const providerModel = normalizeText(
    thinkConfig?.provider?.model || thinkConfig?.model,
    "gpt-4o-mini",
  );
  const provider = {
    type: providerType,
    model: providerModel,
  };

  const temperature = Number(thinkConfig?.provider?.temperature);
  if (Number.isFinite(temperature)) {
    provider.temperature = temperature;
  }

  const normalized = { provider };
  const prompt = normalizeText(thinkConfig?.prompt || thinkConfig?.instructions);
  if (prompt) {
    normalized.prompt = prompt;
  }

  if (functionEntries.length > 0) {
    const mapped = functionEntries
      .map((entry) => sanitizeFunctionDefinition(entry))
      .filter(Boolean);
    if (mapped.length > 0) {
      normalized.functions = mapped;
    }
  }

  if (!managedThinkOnly && thinkConfig?.endpoint?.url) {
    const headers =
      thinkConfig.endpoint.headers && typeof thinkConfig.endpoint.headers === "object"
        ? thinkConfig.endpoint.headers
        : undefined;
    normalized.endpoint = {
      url: String(thinkConfig.endpoint.url),
      headers,
    };
  }

  return normalized;
}

function buildManagedVoiceAgentSettings(options = {}) {
  const inputEncoding = normalizeText(options?.audio?.input?.encoding, "mulaw");
  const inputSampleRate = toFiniteNumber(options?.audio?.input?.sample_rate, 8000);
  const outputEncoding = normalizeText(options?.audio?.output?.encoding, "mulaw");
  const outputSampleRate = toFiniteNumber(options?.audio?.output?.sample_rate, 8000);
  const outputContainer = normalizeText(options?.audio?.output?.container, "none");
  const listenModel = normalizeText(options?.agent?.listen?.provider?.model, "nova-2");
  const speakModel = normalizeText(
    options?.agent?.speak?.provider?.model,
    "aura-asteria-en",
  );
  const language = normalizeText(options?.agent?.language?.type, "en");

  const settings = {
    audio: {
      input: {
        encoding: inputEncoding,
        sample_rate: inputSampleRate,
      },
      output: {
        encoding: outputEncoding,
        sample_rate: outputSampleRate,
        container: outputContainer,
      },
    },
    agent: {
      language: {
        type: language,
      },
      listen: {
        provider: {
          type: "deepgram",
          model: listenModel,
        },
      },
      speak: {
        provider: {
          type: "deepgram",
          model: speakModel,
        },
      },
      think: sanitizeThinkConfig(
        options?.agent?.think || {},
        options?.managedThinkOnly !== false,
      ),
    },
  };

  const greeting = normalizeText(options?.greeting);
  if (greeting) {
    settings.greeting = greeting;
  }

  return settings;
}

class VoiceAgentBridge extends EventEmitter {
  constructor(options = {}) {
    super();
    this.apiKey = normalizeText(options.apiKey);
    if (!this.apiKey) {
      throw new Error("Deepgram API key is required for VoiceAgentBridge");
    }
    this.openTimeoutMs = Math.max(
      1000,
      toFiniteNumber(options.openTimeoutMs, 8000),
    );
    this.settingsTimeoutMs = Math.max(
      1000,
      toFiniteNumber(options.settingsTimeoutMs, 8000),
    );
    this.keepAliveMs = Math.max(
      5000,
      toFiniteNumber(options.keepAliveMs, 8000),
    );
    this.managedThinkOnly = options.managedThinkOnly !== false;
    this.logPrefix = normalizeText(options.logPrefix, "voice-agent");

    this.settings = buildManagedVoiceAgentSettings({
      managedThinkOnly: this.managedThinkOnly,
      audio: options.audio,
      agent: options.agent,
      greeting: options.greeting,
    });

    this.client = createClient(this.apiKey);
    this.connection = null;
    this.connected = false;
    this.settingsApplied = false;
    this.lastAudioSentAt = 0;
    this.keepAliveTimer = null;
    this.connectPromise = null;
  }

  connect() {
    if (this.connectPromise) {
      return this.connectPromise;
    }

    this.connection = this.client.agent();

    this.connectPromise = new Promise((resolve, reject) => {
      let openTimer = null;
      let settingsTimer = null;
      let settled = false;

      const finish = (error) => {
        if (settled) return;
        settled = true;
        if (openTimer) clearTimeout(openTimer);
        if (settingsTimer) clearTimeout(settingsTimer);
        if (error) {
          reject(error);
          return;
        }
        resolve();
      };

      this.connection.once(AgentEvents.Open, () => {
        this.connected = true;
        this.emit("open");
        try {
          this.connection.configure(this.settings);
        } catch (error) {
          finish(error);
          return;
        }
        settingsTimer = setTimeout(() => {
          finish(new Error("voice_agent_settings_timeout"));
        }, this.settingsTimeoutMs);
        if (settingsTimer && typeof settingsTimer.unref === "function") {
          settingsTimer.unref();
        }
      });

      this.connection.once(AgentEvents.SettingsApplied, (payload) => {
        this.settingsApplied = true;
        this.startKeepAlive();
        this.emit("settingsApplied", payload);
        finish();
      });

      this.connection.once(AgentEvents.Error, (event) => {
        if (!this.settingsApplied) {
          const message =
            event?.error?.message || event?.message || "voice_agent_connection_error";
          finish(new Error(String(message)));
        }
      });

      openTimer = setTimeout(() => {
        finish(new Error("voice_agent_open_timeout"));
      }, this.openTimeoutMs);
      if (openTimer && typeof openTimer.unref === "function") {
        openTimer.unref();
      }
    });

    this.attachRuntimeEvents();

    return this.connectPromise;
  }

  attachRuntimeEvents() {
    if (!this.connection) return;

    this.connection.on(AgentEvents.ConversationText, (payload) => {
      const role = normalizeText(payload?.role).toLowerCase();
      const content = normalizeText(payload?.content || payload?.message);
      if (!content) return;
      this.emit("conversationText", {
        role,
        content,
        raw: payload,
      });
    });

    this.connection.on(AgentEvents.UserStartedSpeaking, (payload) => {
      this.emit("userStartedSpeaking", payload);
    });

    this.connection.on(AgentEvents.AgentStartedSpeaking, (payload) => {
      this.emit("agentStartedSpeaking", payload);
    });

    this.connection.on(AgentEvents.AgentThinking, (payload) => {
      this.emit("agentThinking", payload);
    });

    this.connection.on(AgentEvents.AgentAudioDone, (payload) => {
      this.emit("agentAudioDone", payload);
    });

    this.connection.on(AgentEvents.FunctionCallRequest, (payload) => {
      this.emit("functionCallRequest", payload);
    });

    this.connection.on(AgentEvents.Audio, (audioChunk) => {
      try {
        const buffer = Buffer.isBuffer(audioChunk)
          ? audioChunk
          : Buffer.from(audioChunk);
        if (!buffer.length) return;
        this.emit("audio", {
          base64: buffer.toString("base64"),
          bytes: buffer.length,
        });
      } catch (error) {
        this.emit("error", error);
      }
    });

    this.connection.on(AgentEvents.Error, (event) => {
      const message =
        event?.error?.message || event?.message || "voice_agent_runtime_error";
      this.emit("error", new Error(String(message)));
    });

    this.connection.on(AgentEvents.Close, (payload) => {
      this.connected = false;
      this.stopKeepAlive();
      this.emit("close", payload);
    });

    this.connection.on(AgentEvents.Unhandled, (payload) => {
      this.emit("unhandled", payload);
    });
  }

  sendAudioBase64(base64Payload) {
    if (!this.connection || !this.settingsApplied) return false;
    if (!base64Payload) return false;
    try {
      const payload = Buffer.from(base64Payload, "base64");
      if (!payload.length) return false;
      this.connection.send(payload);
      this.lastAudioSentAt = Date.now();
      return true;
    } catch (error) {
      this.emit("error", error);
      return false;
    }
  }

  respondFunctionCall(response = {}) {
    if (!this.connection) return false;
    const id = normalizeText(response.id);
    const name = normalizeText(response.name);
    const content =
      typeof response.content === "string"
        ? response.content
        : JSON.stringify(response.content || {});
    if (!id || !name) {
      return false;
    }
    try {
      this.connection.functionCallResponse({ id, name, content });
      return true;
    } catch (error) {
      this.emit("error", error);
      return false;
    }
  }

  injectAgentMessage(content) {
    if (!this.connection) return false;
    const text = normalizeText(content);
    if (!text) return false;
    try {
      this.connection.injectAgentMessage(text);
      return true;
    } catch (error) {
      this.emit("error", error);
      return false;
    }
  }

  startKeepAlive() {
    this.stopKeepAlive();
    this.keepAliveTimer = setInterval(() => {
      if (!this.connection || !this.settingsApplied) return;
      const silenceMs = Date.now() - Number(this.lastAudioSentAt || 0);
      if (silenceMs < this.keepAliveMs) return;
      try {
        this.connection.keepAlive();
      } catch (error) {
        this.emit("error", error);
      }
    }, this.keepAliveMs);
    if (this.keepAliveTimer && typeof this.keepAliveTimer.unref === "function") {
      this.keepAliveTimer.unref();
    }
  }

  stopKeepAlive() {
    if (this.keepAliveTimer) {
      clearInterval(this.keepAliveTimer);
      this.keepAliveTimer = null;
    }
  }

  close() {
    this.stopKeepAlive();
    this.settingsApplied = false;
    this.connected = false;
    if (!this.connection) return;
    try {
      if (typeof this.connection.requestClose === "function") {
        this.connection.requestClose();
      } else if (typeof this.connection.finish === "function") {
        this.connection.finish();
      }
    } catch (error) {
      this.emit("error", error);
    }
  }
}

module.exports = {
  VoiceAgentBridge,
  buildManagedVoiceAgentSettings,
};
