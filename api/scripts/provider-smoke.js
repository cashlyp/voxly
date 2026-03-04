#!/usr/bin/env node

const twilio = require("twilio");
const { Vonage } = require("@vonage/server-sdk");
const config = require("../config");
const { __testables: smsTestables } = require("../routes/sms");
const VonageVoiceAdapter = require("../adapters/VonageVoiceAdapter");
const {
  resolvePaymentExecutionMode,
  buildCanonicalCallStatusEvent,
} = require("../adapters/providerFlowPolicy");
const { buildProviderCallbackUrls } = require("../adapters/providerPreflight");

const { TwilioSmsAdapter, VonageSmsAdapter } = smsTestables || {};

function boolFrom(value, fallback = false) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return fallback;
}

function assert(condition, message) {
  if (!condition) {
    const error = new Error(message || "Assertion failed");
    error.code = "assertion_failed";
    throw error;
  }
}

function redactError(error) {
  const message = String(error?.message || error || "unknown_error").trim();
  if (!message) return "unknown_error";
  return message.length > 220 ? `${message.slice(0, 217)}...` : message;
}

function withTimeout(promise, timeoutMs = 7000, timeoutCode = "smoke_timeout") {
  const safeMs = Number(timeoutMs);
  if (!Number.isFinite(safeMs) || safeMs <= 0) {
    return Promise.resolve(promise);
  }
  return new Promise((resolve, reject) => {
    let done = false;
    const timer = setTimeout(() => {
      if (done) return;
      done = true;
      const timeoutError = new Error(`Timed out after ${safeMs}ms`);
      timeoutError.code = timeoutCode;
      reject(timeoutError);
    }, safeMs);

    Promise.resolve(promise)
      .then((result) => {
        if (done) return;
        done = true;
        clearTimeout(timer);
        resolve(result);
      })
      .catch((error) => {
        if (done) return;
        done = true;
        clearTimeout(timer);
        reject(error);
      });
  });
}

function createRunner() {
  const checks = [];

  async function runCheck(name, fn, options = {}) {
    const started = Date.now();
    const mode = options.mode || "offline";
    try {
      const details = await fn();
      checks.push({
        name,
        mode,
        status: "pass",
        duration_ms: Date.now() - started,
        details: details || null,
      });
    } catch (error) {
      checks.push({
        name,
        mode,
        status: "fail",
        duration_ms: Date.now() - started,
        error: redactError(error),
      });
    }
  }

  function addSkip(name, reason, options = {}) {
    checks.push({
      name,
      mode: options.mode || "offline",
      status: "skip",
      duration_ms: 0,
      reason,
    });
  }

  function printReport() {
    const total = checks.length;
    const passed = checks.filter((check) => check.status === "pass").length;
    const failed = checks.filter((check) => check.status === "fail").length;
    const skipped = checks.filter((check) => check.status === "skip").length;

    console.log("Provider smoke report");
    console.log(`Summary: pass=${passed} fail=${failed} skip=${skipped} total=${total}`);
    for (const check of checks) {
      const tag =
        check.status === "pass"
          ? "[PASS]"
          : check.status === "skip"
            ? "[SKIP]"
            : "[FAIL]";
      console.log(
        `${tag} (${check.mode}) ${check.name} (${check.duration_ms}ms)${
          check.error ? ` :: ${check.error}` : check.reason ? ` :: ${check.reason}` : ""
        }`,
      );
    }

    return {
      total,
      passed,
      failed,
      skipped,
      checks,
    };
  }

  return {
    runCheck,
    addSkip,
    printReport,
  };
}

async function runOfflineChecks(runner) {
  if (!TwilioSmsAdapter || !VonageSmsAdapter) {
    throw new Error("SMS adapter test hooks are unavailable");
  }

  await runner.runCheck("sms.twilio.request_and_normalization", async () => {
    const calls = [];
    const fakeTwilioClient = {
      messages: {
        create: async (payload) => {
          calls.push(payload);
          return {
            sid: "SM_SMOKE_TWILIO",
            status: "queued",
          };
        },
      },
    };

    const adapter = new TwilioSmsAdapter({
      client: fakeTwilioClient,
      defaultFrom: "+15550001111",
    });

    const response = await adapter.sendSms(
      {
        to: "+15550002222",
        from: "+15550001111",
        body: "hello parity",
        statusCallback: "https://smoke.example/webhook/sms-status",
        mediaUrl: "https://smoke.example/image.png",
      },
      {
        withTimeout: (promise) => Promise.resolve(promise),
        providerTimeoutMs: 1000,
      },
    );

    assert(calls.length === 1, "Twilio adapter did not invoke client.messages.create");
    assert(calls[0].statusCallback, "Twilio request missing statusCallback");
    assert(calls[0].mediaUrl, "Twilio request missing mediaUrl");
    assert(response.provider === "twilio", "Twilio response provider normalization failed");
    assert(response.messageSid === "SM_SMOKE_TWILIO", "Twilio message SID normalization failed");
    return {
      request_keys: Object.keys(calls[0]).sort(),
      normalized_status: response.status,
    };
  });

  await runner.runCheck("sms.vonage.request_and_normalization", async () => {
    const calls = [];
    const fakeVonageClient = {
      sms: {
        send: async (payload) => {
          calls.push(payload);
          return {
            messages: [
              {
                "message-id": "VONAGE_SMOKE_ID",
                status: "0",
              },
            ],
          };
        },
      },
    };

    const adapter = new VonageSmsAdapter({
      client: fakeVonageClient,
      defaultFrom: "VONAGE",
      apiKey: "dummy",
      apiSecret: "dummy",
    });

    const response = await adapter.sendSms(
      {
        to: "+15550003333",
        from: "VONAGE",
        body: "hello parity",
        statusCallback: "https://smoke.example/vd",
        idempotencyKey: "idempotency-test-value",
      },
      {
        withTimeout: (promise) => Promise.resolve(promise),
        providerTimeoutMs: 1000,
      },
    );

    assert(calls.length === 1, "Vonage adapter did not invoke client.sms.send");
    assert(calls[0].callback, "Vonage request missing callback URL");
    assert(calls[0]["status-report-req"] === 1, "Vonage request missing status-report-req");
    assert(calls[0]["client-ref"], "Vonage request missing client-ref");
    assert(response.provider === "vonage", "Vonage response provider normalization failed");
    assert(response.messageSid === "VONAGE_SMOKE_ID", "Vonage message SID normalization failed");
    return {
      request_keys: Object.keys(calls[0]).sort(),
      normalized_status: response.status,
    };
  });

  await runner.runCheck("voice.vonage.adapter_interface", async () => {
    const observed = {
      outboundPayload: null,
    };
    const fakeVoiceClient = {
      voice: {
        createOutboundCall: async (payload) => {
          observed.outboundPayload = payload;
          return { uuid: "VONAGE_UUID", status: "started" };
        },
      },
    };

    const adapter = new VonageVoiceAdapter({
      apiKey: "key",
      apiSecret: "secret",
      applicationId: "app-id",
      privateKey: "private-key",
      voice: {
        fromNumber: "+15550001111",
      },
      client: fakeVoiceClient,
    }, { info: () => {} });

    const result = await adapter.createOutboundCall({
      to: "+15550004444",
      callSid: "call-smoke-1",
      answerUrl: "https://smoke.example/answer",
      eventUrl: "https://smoke.example/event",
    });

    assert(result.uuid === "VONAGE_UUID", "Vonage adapter did not return outbound call response");
    assert(
      observed.outboundPayload?.answer_url?.[0] === "https://smoke.example/answer",
      "Vonage adapter did not map answer_url",
    );
    assert(
      observed.outboundPayload?.event_url?.[0] === "https://smoke.example/event",
      "Vonage adapter did not map event_url",
    );
    return {
      payload_keys: Object.keys(observed.outboundPayload || {}).sort(),
    };
  });

  await runner.runCheck("voice.callback_url_mapping", async () => {
    const twilioCallbacks = buildProviderCallbackUrls("twilio", "call", config, {
      hostOverride: "smoke.example",
    });
    const vonageCallbacks = buildProviderCallbackUrls("vonage", "call", config, {
      hostOverride: "smoke.example",
    });

    assert(
      twilioCallbacks.urls.includes("https://smoke.example/incoming"),
      "Twilio callback mapping missing /incoming",
    );
    assert(
      twilioCallbacks.urls.includes("https://smoke.example/webhook/call-status"),
      "Twilio callback mapping missing /webhook/call-status",
    );
    assert(vonageCallbacks.urls.length >= 2, "Vonage callback mapping missing answer/event URLs");
    return {
      twilio_urls: twilioCallbacks.urls,
      vonage_urls: vonageCallbacks.urls,
    };
  });

  await runner.runCheck("voice.status_callback_parity", async () => {
    const twilioCompleted = buildCanonicalCallStatusEvent(
      "twilio",
      {
        CallSid: "CA_SMOKE_1",
        CallStatus: "completed",
      },
      { callSid: "CA_SMOKE_1" },
    );

    const vonageCompleted = buildCanonicalCallStatusEvent(
      "vonage",
      {
        status: "completed",
      },
      { callSid: "CA_SMOKE_1" },
    );

    const twilioNoAnswer = buildCanonicalCallStatusEvent(
      "twilio",
      {
        CallSid: "CA_SMOKE_2",
        CallStatus: "no-answer",
      },
      { callSid: "CA_SMOKE_2" },
    );

    const vonageNoAnswer = buildCanonicalCallStatusEvent(
      "vonage",
      {
        status: "timeout",
      },
      { callSid: "CA_SMOKE_2" },
    );

    assert(twilioCompleted.status === "completed", "Twilio canonical status mismatch");
    assert(vonageCompleted.status === "completed", "Vonage canonical completed mapping mismatch");
    assert(twilioNoAnswer.status === "no-answer", "Twilio canonical no-answer mismatch");
    assert(vonageNoAnswer.status === "no-answer", "Vonage canonical no-answer mismatch");
    return {
      canonical_completed: [twilioCompleted.status, vonageCompleted.status],
      canonical_no_answer: [twilioNoAnswer.status, vonageNoAnswer.status],
    };
  });

  await runner.runCheck("payment_mode.parity", async () => {
    const featureEnabled = {
      enabled: true,
      kill_switch: false,
      allow_twilio: true,
    };
    const featureKillSwitch = {
      enabled: true,
      kill_switch: true,
      allow_twilio: true,
    };

    const twilioNative = resolvePaymentExecutionMode({
      provider: "twilio",
      featureConfig: featureEnabled,
      hasNativeAdapter: true,
      smsFallbackEnabled: true,
      smsServiceReady: true,
    });
    const vonageFallback = resolvePaymentExecutionMode({
      provider: "vonage",
      featureConfig: featureEnabled,
      hasNativeAdapter: false,
      smsFallbackEnabled: true,
      smsServiceReady: true,
    });
    const twilioKilled = resolvePaymentExecutionMode({
      provider: "twilio",
      featureConfig: featureKillSwitch,
      hasNativeAdapter: true,
      smsFallbackEnabled: true,
      smsServiceReady: true,
    });
    const vonageNoFallback = resolvePaymentExecutionMode({
      provider: "vonage",
      featureConfig: featureEnabled,
      hasNativeAdapter: false,
      smsFallbackEnabled: false,
      smsServiceReady: true,
    });

    assert(twilioNative.mode === "native", "Twilio payment mode should be native");
    assert(vonageFallback.mode === "sms_fallback", "Vonage payment mode should be sms_fallback");
    assert(twilioKilled.mode === "disabled", "Kill switch should disable Twilio payment mode");
    assert(vonageNoFallback.mode === "disabled", "Vonage without fallback should be disabled");

    return {
      definition:
        "payment-mode is resolvePaymentExecutionMode(provider, featureConfig, adapter/fallback readiness)",
      scenarios: {
        twilio_native: twilioNative.mode,
        vonage_fallback: vonageFallback.mode,
        kill_switch: twilioKilled.mode,
        fallback_disabled: vonageNoFallback.mode,
      },
    };
  });
}

async function runLiveChecks(runner) {
  const timeoutMs = Number(process.env.LIVE_SMOKE_TIMEOUT_MS) || 10000;

  if (config.twilio?.accountSid && config.twilio?.authToken) {
    await runner.runCheck(
      "live.twilio.auth",
      async () => {
        const client = twilio(config.twilio.accountSid, config.twilio.authToken);
        const account = await withTimeout(
          client.api.v2010.accounts(config.twilio.accountSid).fetch(),
          timeoutMs,
          "live_twilio_auth_timeout",
        );
        assert(account?.sid, "Twilio account fetch did not return SID");
        return {
          account_sid: account.sid,
        };
      },
      { mode: "live" },
    );
  } else {
    runner.addSkip(
      "live.twilio.auth",
      "Twilio credentials missing; skipping live auth probe",
      { mode: "live" },
    );
  }

  if (config.vonage?.apiKey && config.vonage?.apiSecret) {
    await runner.runCheck(
      "live.vonage.auth",
      async () => {
        const client = new Vonage({
          apiKey: config.vonage.apiKey,
          apiSecret: config.vonage.apiSecret,
          applicationId: config.vonage.applicationId,
          privateKey: config.vonage.privateKey,
        });
        const balance = await withTimeout(
          client.account.getBalance(),
          timeoutMs,
          "live_vonage_auth_timeout",
        );
        const value = Number(balance?.value ?? balance?.balance);
        assert(Number.isFinite(value), "Vonage balance probe did not return numeric value");
        return {
          account_balance: value,
        };
      },
      { mode: "live" },
    );
  } else {
    runner.addSkip(
      "live.vonage.auth",
      "Vonage credentials missing; skipping live auth probe",
      { mode: "live" },
    );
  }
}

async function main() {
  const liveMode = boolFrom(process.env.LIVE_SMOKE, false);
  const runner = createRunner();

  await runOfflineChecks(runner);

  if (liveMode) {
    await runLiveChecks(runner);
  } else {
    runner.addSkip(
      "live.provider_auth",
      "LIVE_SMOKE is not enabled (set LIVE_SMOKE=1 to run live checks)",
      { mode: "live" },
    );
  }

  const report = runner.printReport();
  if (report.failed > 0) {
    process.exit(1);
    return;
  }
  process.exit(0);
}

main().catch((error) => {
  console.error(`Provider smoke runner failed: ${redactError(error)}`);
  process.exit(2);
});
