const VonageVoiceAdapter = require("./VonageVoiceAdapter");

function buildHarness(voiceOverrides = {}) {
  const client = {
    voice: {
      createOutboundCall: jest.fn(),
      updateCall: jest.fn(),
      transferCallWithURL: jest.fn(),
    },
  };
  const logger = {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
  };
  const adapter = new VonageVoiceAdapter(
    {
      client,
      voice: {
        fromNumber: "+15550001111",
        requestTimeoutMs: 2500,
        retryAttempts: 1,
        createRetryAttempts: 0,
        retryBaseMs: 1,
        retryMaxDelayMs: 1,
        retryJitterMs: 0,
        ...voiceOverrides,
      },
    },
    logger,
  );
  return { adapter, client, logger };
}

describe("VonageVoiceAdapter", () => {
  test("creates outbound call with inline NCCO payload", async () => {
    const { adapter, client } = buildHarness();
    client.voice.createOutboundCall.mockResolvedValue({
      uuid: "vonage-uuid-1",
      status: "started",
    });

    const result = await adapter.createOutboundCall({
      to: "+15551231234",
      callSid: "call-1",
      eventUrl: "https://example.com/event",
      ncco: [{ action: "talk", text: "hello" }],
    });

    expect(result.uuid).toBe("vonage-uuid-1");
    expect(client.voice.createOutboundCall).toHaveBeenCalledTimes(1);
    const payload = client.voice.createOutboundCall.mock.calls[0][0];
    expect(payload.to?.[0]?.number).toBe("+15551231234");
    expect(payload.from?.number).toBe("+15550001111");
    expect(payload.ncco).toEqual([{ action: "talk", text: "hello" }]);
    expect(payload.answer_url).toBeUndefined();
    expect(payload.event_url).toEqual(["https://example.com/event"]);
    expect(payload.event_method).toBe("POST");
  });

  test("does not retry outbound create by default", async () => {
    const { adapter, client } = buildHarness();
    const timeoutError = new Error("network timeout");
    timeoutError.code = "ECONNRESET";
    client.voice.createOutboundCall.mockRejectedValue(timeoutError);

    await expect(
      adapter.createOutboundCall({
        to: "+15551231234",
        callSid: "call-2",
        answerUrl: "https://example.com/answer",
      }),
    ).rejects.toThrow("network timeout");

    expect(client.voice.createOutboundCall).toHaveBeenCalledTimes(1);
  });

  test("retries outbound create when explicitly enabled", async () => {
    const { adapter, client } = buildHarness({ createRetryAttempts: 1 });
    const transientError = new Error("transient error");
    transientError.code = "ECONNABORTED";
    client.voice.createOutboundCall
      .mockRejectedValueOnce(transientError)
      .mockResolvedValueOnce({ uuid: "vonage-uuid-2", status: "queued" });

    const result = await adapter.createOutboundCall({
      to: "+15551231234",
      callSid: "call-3",
      answerUrl: "https://example.com/answer",
    });

    expect(result.uuid).toBe("vonage-uuid-2");
    expect(client.voice.createOutboundCall).toHaveBeenCalledTimes(2);
  });

  test("retries hangup on transient provider failure", async () => {
    const { adapter, client } = buildHarness({ retryAttempts: 1 });
    const transientError = new Error("provider unavailable");
    transientError.statusCode = 503;
    client.voice.updateCall
      .mockRejectedValueOnce(transientError)
      .mockResolvedValueOnce({});

    await expect(adapter.hangupCall("uuid-1")).resolves.toBeUndefined();
    expect(client.voice.updateCall).toHaveBeenCalledTimes(2);
    expect(client.voice.updateCall).toHaveBeenCalledWith("uuid-1", {
      action: "hangup",
    });
  });

  test("uses SDK hangupCall method when available", async () => {
    const { adapter, client } = buildHarness();
    client.voice.hangupCall = jest.fn().mockResolvedValue({});
    client.voice.updateCall.mockImplementation(() => {
      throw new Error("updateCall should not be used");
    });

    await expect(adapter.hangupCall("uuid-2")).resolves.toBeUndefined();
    expect(client.voice.hangupCall).toHaveBeenCalledTimes(1);
    expect(client.voice.hangupCall).toHaveBeenCalledWith("uuid-2");
  });
});
