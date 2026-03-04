jest.mock("@deepgram/sdk", () => {
  const EventEmitter = require("events");
  const AgentEvents = {
    Open: "Open",
    Close: "Close",
    Error: "Error",
    Audio: "Audio",
    Welcome: "Welcome",
    SettingsApplied: "SettingsApplied",
    ConversationText: "ConversationText",
    UserStartedSpeaking: "UserStartedSpeaking",
    AgentThinking: "AgentThinking",
    FunctionCallRequest: "FunctionCallRequest",
    AgentStartedSpeaking: "AgentStartedSpeaking",
    AgentAudioDone: "AgentAudioDone",
    Unhandled: "Unhandled",
  };

  const state = {
    lastConnection: null,
  };

  const createConnection = () => {
    const connection = new EventEmitter();
    connection.send = jest.fn();
    connection.configure = jest.fn();
    connection.functionCallResponse = jest.fn();
    connection.injectAgentMessage = jest.fn();
    connection.keepAlive = jest.fn();
    connection.requestClose = jest.fn();
    return connection;
  };

  const createClient = jest.fn(() => ({
    agent: jest.fn(() => {
      state.lastConnection = createConnection();
      return state.lastConnection;
    }),
  }));

  return {
    createClient,
    AgentEvents,
    __state: state,
  };
});

const sdk = require("@deepgram/sdk");
const {
  VoiceAgentBridge,
  buildManagedVoiceAgentSettings,
} = require("../routes/voiceAgentBridge");

function getConnection() {
  return sdk.__state.lastConnection;
}

describe("VoiceAgentBridge smoke", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    jest.useRealTimers();
  });

  test("enforces managed think mode with no custom endpoint", () => {
    expect(() =>
      buildManagedVoiceAgentSettings({
        managedThinkOnly: true,
        agent: {
          think: {
            endpoint: {
              url: "https://example.test/custom-llm",
            },
          },
        },
      }),
    ).toThrow("voice_agent_managed_think_requires_no_endpoint");
  });

  test("connects, configures settings, and resolves after SettingsApplied", async () => {
    const bridge = new VoiceAgentBridge({
      apiKey: "test-key",
      openTimeoutMs: 5000,
      settingsTimeoutMs: 5000,
      managedThinkOnly: true,
      agent: {
        think: {
          provider: {
            type: "open_ai",
            model: "gpt-4o-mini",
          },
          prompt: "You are concise.",
        },
      },
    });

    const promise = bridge.connect();
    const connection = getConnection();
    expect(connection).toBeTruthy();

    connection.emit(sdk.AgentEvents.Open);
    expect(connection.configure).toHaveBeenCalledTimes(1);

    const payload = connection.configure.mock.calls[0][0];
    expect(payload.agent.think.provider.type).toBe("open_ai");
    expect(payload.agent.think.endpoint).toBeUndefined();
    expect(payload.agent.language).toBe("en");
    expect(typeof payload.agent.language).toBe("string");

    connection.emit(sdk.AgentEvents.SettingsApplied, { ok: true });
    await expect(promise).resolves.toBeUndefined();
  });

  test("times out cleanly when open handshake never arrives", async () => {
    const bridge = new VoiceAgentBridge({
      apiKey: "test-key",
      openTimeoutMs: 25,
      settingsTimeoutMs: 100,
    });

    const promise = bridge.connect();
    await expect(promise).rejects.toThrow("voice_agent_open_timeout");
  });

  test("setup errors reject connect without emitting runtime error", async () => {
    const bridge = new VoiceAgentBridge({
      apiKey: "test-key",
      openTimeoutMs: 5000,
      settingsTimeoutMs: 5000,
    });
    const runtimeErrors = [];
    bridge.on("error", (error) => runtimeErrors.push(error));

    const promise = bridge.connect();
    const connection = getConnection();
    connection.emit(sdk.AgentEvents.Open);
    connection.emit(sdk.AgentEvents.Error, {
      type: "Error",
      code: "invalid_settings",
      description: "Invalid think provider model",
    });

    await expect(promise).rejects.toThrow("Invalid think provider model");
    expect(runtimeErrors).toHaveLength(0);
  });

  test("runtime errors include extracted provider details after settings", async () => {
    const bridge = new VoiceAgentBridge({
      apiKey: "test-key",
      openTimeoutMs: 5000,
      settingsTimeoutMs: 5000,
    });

    const promise = bridge.connect();
    const connection = getConnection();

    connection.emit(sdk.AgentEvents.Open);
    connection.emit(sdk.AgentEvents.SettingsApplied, { ok: true });
    await promise;

    const runtimeErrors = [];
    bridge.on("error", (error) => runtimeErrors.push(error));

    connection.emit(sdk.AgentEvents.Error, {
      type: "Error",
      code: "llm_provider_unavailable",
      description: "Managed think provider unavailable",
    });

    expect(runtimeErrors).toHaveLength(1);
    expect(runtimeErrors[0].message).toBe("Managed think provider unavailable");
    expect(runtimeErrors[0].code).toBe("llm_provider_unavailable");
  });

  test("accepts language as string and preserves valid settings shape", () => {
    const payload = buildManagedVoiceAgentSettings({
      agent: {
        language: "en",
        think: {
          provider: {
            type: "open_ai",
            model: "gpt-4o-mini",
          },
        },
      },
    });
    expect(payload.agent.language).toBe("en");
    expect(typeof payload.agent.language).toBe("string");
  });

  test("emits audio base64 and function call request events", async () => {
    const bridge = new VoiceAgentBridge({
      apiKey: "test-key",
      openTimeoutMs: 5000,
      settingsTimeoutMs: 5000,
    });

    const promise = bridge.connect();
    const connection = getConnection();

    connection.emit(sdk.AgentEvents.Open);
    connection.emit(sdk.AgentEvents.SettingsApplied, { ok: true });
    await promise;

    const audioEvents = [];
    const functionEvents = [];

    bridge.on("audio", (payload) => audioEvents.push(payload));
    bridge.on("functionCallRequest", (payload) => functionEvents.push(payload));

    connection.emit(sdk.AgentEvents.Audio, Buffer.from("abc"));
    connection.emit(sdk.AgentEvents.FunctionCallRequest, {
      functions: [{ id: "fn-1", name: "lookup", arguments: "{}" }],
    });

    expect(audioEvents).toHaveLength(1);
    expect(audioEvents[0].base64).toBe(Buffer.from("abc").toString("base64"));
    expect(functionEvents).toHaveLength(1);

    const sent = bridge.respondFunctionCall({
      id: "fn-1",
      name: "lookup",
      content: { ok: true },
    });
    expect(sent).toBe(true);
    expect(connection.functionCallResponse).toHaveBeenCalledWith({
      id: "fn-1",
      name: "lookup",
      content: JSON.stringify({ ok: true }),
    });
  });
});
