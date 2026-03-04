"use strict";

jest.mock("../config", () => ({
  deepgram: {
    apiKey: "test-key",
    model: "nova-2",
  },
}));

jest.mock("@deepgram/sdk", () => {
  const EventEmitter = require("events");
  const LiveTranscriptionEvents = {
    Open: "Open",
    Transcript: "Transcript",
    Error: "Error",
    Warning: "Warning",
    Metadata: "Metadata",
    Close: "Close",
  };

  const state = {
    connection: null,
  };

  const createConnection = () => {
    const connection = new EventEmitter();
    connection.send = jest.fn();
    connection.finish = jest.fn();
    connection.requestClose = jest.fn();
    connection.getReadyState = jest.fn(() => 1);
    return connection;
  };

  const createClient = jest.fn(() => ({
    listen: {
      live: jest.fn(() => {
        state.connection = createConnection();
        return state.connection;
      }),
    },
  }));

  return {
    createClient,
    LiveTranscriptionEvents,
    __state: state,
  };
});

const sdk = require("@deepgram/sdk");
const { TranscriptionService } = require("../routes/transcription");

function getConnection() {
  return sdk.__state.connection;
}

describe("TranscriptionService smoke", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test("emits close on unexpected remote close", () => {
    const service = new TranscriptionService();
    const onClose = jest.fn();
    service.on("close", onClose);

    const connection = getConnection();
    connection.emit(sdk.LiveTranscriptionEvents.Close, { code: 1006 });

    expect(onClose).toHaveBeenCalledTimes(1);
  });

  test("does not emit close when close is client-initiated", () => {
    const service = new TranscriptionService();
    const onClose = jest.fn();
    service.on("close", onClose);

    const connection = getConnection();
    service.close();
    connection.emit(sdk.LiveTranscriptionEvents.Close, { code: 1000 });

    expect(onClose).not.toHaveBeenCalled();
  });

  test("logs metadata as informational signal", () => {
    const service = new TranscriptionService();
    const logSpy = jest.spyOn(console, "log").mockImplementation(() => {});
    const errorSpy = jest.spyOn(console, "error").mockImplementation(() => {});

    const connection = getConnection();
    connection.emit(sdk.LiveTranscriptionEvents.Metadata, {
      type: "Metadata",
      request_id: "req_123",
    });

    expect(logSpy).toHaveBeenCalledWith("STT -> deepgram metadata");
    expect(errorSpy).not.toHaveBeenCalledWith("STT -> deepgram metadata");

    logSpy.mockRestore();
    errorSpy.mockRestore();
    service.close();
  });
});
