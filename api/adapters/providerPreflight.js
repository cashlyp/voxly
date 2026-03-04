const twilio = require("twilio");
const fetch = require("node-fetch");
const { Vonage } = require("@vonage/server-sdk");
const { runWithTimeout } = require("../utils/asyncControl");

const CHECK_STATUS = Object.freeze({
  PASS: "pass",
  FAIL: "fail",
  WARN: "warn",
  SKIP: "skip",
});

const SUPPORTED_PROVIDER_PREFLIGHT_CHANNELS = Object.freeze({
  call: Object.freeze(["twilio", "vonage"]),
  sms: Object.freeze(["twilio", "vonage"]),
});

const REQUIRED_ROUTE_GROUPS = Object.freeze({
  call: Object.freeze({
    twilio: Object.freeze([
      Object.freeze({
        id: "twilio_incoming",
        label: "Twilio incoming voice route",
        anyOf: Object.freeze([
          Object.freeze({ method: "POST", path: "/incoming" }),
          Object.freeze({ method: "GET", path: "/incoming" }),
        ]),
      }),
      Object.freeze({
        id: "twilio_call_status",
        label: "Twilio call status webhook route",
        anyOf: Object.freeze([
          Object.freeze({ method: "POST", path: "/webhook/call-status" }),
        ]),
      }),
      Object.freeze({
        id: "twilio_stream_status",
        label: "Twilio stream status webhook route",
        anyOf: Object.freeze([
          Object.freeze({ method: "POST", path: "/webhook/twilio-stream" }),
        ]),
      }),
    ]),
    vonage: Object.freeze([
      Object.freeze({
        id: "vonage_answer",
        label: "Vonage answer webhook route",
        anyOf: Object.freeze([
          Object.freeze({ method: "GET", path: "/va" }),
          Object.freeze({ method: "GET", path: "/answer" }),
        ]),
      }),
      Object.freeze({
        id: "vonage_event",
        label: "Vonage event webhook route",
        anyOf: Object.freeze([
          Object.freeze({ method: "POST", path: "/ve" }),
          Object.freeze({ method: "POST", path: "/event" }),
        ]),
      }),
      Object.freeze({
        id: "vonage_stream_ws",
        label: "Vonage media websocket route",
        anyOf: Object.freeze([
          Object.freeze({ method: "GET", path: "/vonage/stream" }),
        ]),
      }),
    ]),
  }),
  sms: Object.freeze({
    twilio: Object.freeze([
      Object.freeze({
        id: "twilio_sms_inbound",
        label: "Twilio inbound SMS webhook route",
        anyOf: Object.freeze([
          Object.freeze({ method: "POST", path: "/webhook/sms" }),
        ]),
      }),
      Object.freeze({
        id: "twilio_sms_status",
        label: "Twilio SMS status webhook route",
        anyOf: Object.freeze([
          Object.freeze({ method: "POST", path: "/webhook/sms-status" }),
        ]),
      }),
      Object.freeze({
        id: "twilio_sms_delivery",
        label: "Twilio SMS delivery webhook route",
        anyOf: Object.freeze([
          Object.freeze({ method: "POST", path: "/webhook/sms-delivery" }),
        ]),
      }),
    ]),
    vonage: Object.freeze([
      Object.freeze({
        id: "vonage_sms_inbound",
        label: "Vonage inbound SMS webhook route",
        anyOf: Object.freeze([
          Object.freeze({ method: "GET", path: "/vs" }),
          Object.freeze({ method: "POST", path: "/vs" }),
        ]),
      }),
      Object.freeze({
        id: "vonage_sms_delivery",
        label: "Vonage SMS delivery webhook route",
        anyOf: Object.freeze([
          Object.freeze({ method: "GET", path: "/vd" }),
          Object.freeze({ method: "POST", path: "/vd" }),
        ]),
      }),
    ]),
  }),
});

class ProviderPreflightError extends Error {
  constructor(message, options = {}) {
    super(message || "Provider preflight failed");
    this.name = "ProviderPreflightError";
    this.code = options.code || "provider_preflight_failed";
    this.provider = normalizeProvider(options.provider);
    this.channel = normalizeChannel(options.channel);
    this.mode = String(options.mode || "activation");
    this.report = options.report || null;
  }

  toJSON() {
    return {
      name: this.name,
      code: this.code,
      message: this.message,
      provider: this.provider,
      channel: this.channel,
      mode: this.mode,
      report: this.report,
    };
  }
}

function normalizeProvider(value) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

function normalizeChannel(value) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

function normalizeHost(hostValue) {
  const raw = String(hostValue || "").trim();
  if (!raw) return "";
  return raw
    .replace(/^https?:\/\//i, "")
    .replace(/^wss?:\/\//i, "")
    .replace(/\/.*$/, "")
    .replace(/\/+$/, "");
}

function isHttpsUrl(value) {
  if (!value || typeof value !== "string") return false;
  try {
    const parsed = new URL(value);
    return parsed.protocol === "https:";
  } catch {
    return false;
  }
}

function redactError(error) {
  const message = String(error?.message || error || "unknown_error").trim();
  if (!message) return "unknown_error";
  return message.length > 220 ? `${message.slice(0, 217)}...` : message;
}

function formatDurationMs(startedAtMs) {
  const elapsed = Date.now() - startedAtMs;
  return Number.isFinite(elapsed) && elapsed >= 0 ? elapsed : 0;
}

function createCheckResult(id, label, status, options = {}) {
  return {
    id,
    label,
    status,
    reason: options.reason || null,
    suggested_fix: options.suggestedFix || null,
    details: options.details || null,
    duration_ms:
      Number.isFinite(Number(options.durationMs)) && Number(options.durationMs) >= 0
        ? Math.floor(Number(options.durationMs))
        : 0,
  };
}

function createBaseReport(options = {}) {
  return {
    provider: normalizeProvider(options.provider),
    channel: normalizeChannel(options.channel),
    mode: String(options.mode || "activation"),
    generated_at: new Date().toISOString(),
    ok: false,
    checks: [],
    summary: {
      pass: 0,
      fail: 0,
      warn: 0,
      skip: 0,
      total: 0,
    },
  };
}

function finalizeReport(report) {
  const summary = {
    pass: 0,
    fail: 0,
    warn: 0,
    skip: 0,
    total: Array.isArray(report?.checks) ? report.checks.length : 0,
  };
  for (const check of report.checks || []) {
    if (check.status === CHECK_STATUS.PASS) {
      summary.pass += 1;
    } else if (check.status === CHECK_STATUS.FAIL) {
      summary.fail += 1;
    } else if (check.status === CHECK_STATUS.WARN) {
      summary.warn += 1;
    } else if (check.status === CHECK_STATUS.SKIP) {
      summary.skip += 1;
    }
  }
  report.summary = summary;
  report.ok = summary.fail === 0;
  return report;
}

function getSupportedProviders(channel) {
  return SUPPORTED_PROVIDER_PREFLIGHT_CHANNELS[normalizeChannel(channel)] || [];
}

function isProviderSupported(channel, provider) {
  return getSupportedProviders(channel).includes(normalizeProvider(provider));
}

async function runCheck(report, id, label, fn) {
  const startedAtMs = Date.now();
  try {
    const result = await fn();
    const normalizedStatus = Object.values(CHECK_STATUS).includes(result?.status)
      ? result.status
      : CHECK_STATUS.FAIL;
    report.checks.push(
      createCheckResult(id, label, normalizedStatus, {
        reason: result?.reason,
        suggestedFix: result?.suggestedFix,
        details: result?.details,
        durationMs: result?.durationMs ?? formatDurationMs(startedAtMs),
      }),
    );
  } catch (error) {
    report.checks.push(
      createCheckResult(id, label, CHECK_STATUS.FAIL, {
        reason: redactError(error),
        suggestedFix: "Inspect provider settings and retry preflight.",
        durationMs: formatDurationMs(startedAtMs),
      }),
    );
  }
}

function collectRouteMethods(layer) {
  if (!layer?.route?.methods) return [];
  return Object.keys(layer.route.methods)
    .filter((method) => layer.route.methods[method] === true)
    .map((method) => method.toUpperCase());
}

function collectRegisteredRoutes(app, routes = new Set(), layers = null) {
  const stack = layers || app?._router?.stack;
  if (!Array.isArray(stack)) {
    return routes;
  }

  for (const layer of stack) {
    if (layer?.route?.path) {
      const methods = collectRouteMethods(layer);
      for (const method of methods) {
        routes.add(`${method} ${layer.route.path}`);
      }
      continue;
    }

    if (Array.isArray(layer?.handle?.stack)) {
      collectRegisteredRoutes(app, routes, layer.handle.stack);
    }
  }
  return routes;
}

function buildTwilioCallbackUrls(channel, config, options = {}) {
  const host = normalizeHost(options.hostOverride || config?.server?.hostname);
  if (!host) {
    return {
      host: "",
      urls: [],
      reason: "SERVER is not configured",
    };
  }
  const baseUrl = `https://${host}`;
  if (normalizeChannel(channel) === "call") {
    return {
      host,
      urls: [
        `${baseUrl}/incoming`,
        `${baseUrl}/webhook/call-status`,
        `${baseUrl}/webhook/twilio-stream`,
      ],
      base_url: baseUrl,
    };
  }
  return {
    host,
    urls: [
      `${baseUrl}/webhook/sms`,
      `${baseUrl}/webhook/sms-status`,
      `${baseUrl}/webhook/sms-delivery`,
    ],
    base_url: baseUrl,
  };
}

function buildVonageCallbackUrls(channel, config, options = {}) {
  const host = normalizeHost(options.hostOverride || config?.server?.hostname);
  const baseUrl = host ? `https://${host}` : "";
  if (normalizeChannel(channel) === "call") {
    const answerUrl = String(config?.vonage?.voice?.answerUrl || "").trim();
    const eventUrl = String(config?.vonage?.voice?.eventUrl || "").trim();
    return {
      host,
      base_url: baseUrl,
      urls: [
        answerUrl || (baseUrl ? `${baseUrl}/answer` : ""),
        eventUrl || (baseUrl ? `${baseUrl}/event` : ""),
      ].filter(Boolean),
      reason: !answerUrl && !eventUrl && !baseUrl
        ? "Neither SERVER nor explicit VONAGE_ANSWER_URL/VONAGE_EVENT_URL is configured"
        : null,
    };
  }

  return {
    host,
    base_url: baseUrl,
    urls: baseUrl ? [`${baseUrl}/vs`, `${baseUrl}/vd`] : [],
    reason: baseUrl ? null : "SERVER is not configured for Vonage SMS callbacks",
  };
}

function buildProviderCallbackUrls(provider, channel, config, options = {}) {
  const normalizedProvider = normalizeProvider(provider);
  if (normalizedProvider === "twilio") {
    return buildTwilioCallbackUrls(channel, config, options);
  }
  if (normalizedProvider === "vonage") {
    return buildVonageCallbackUrls(channel, config, options);
  }
  return {
    host: "",
    base_url: "",
    urls: [],
    reason: `Unsupported provider ${normalizedProvider || provider}`,
  };
}

async function probeHttpReachability(url, timeoutMs = 4000) {
  const safeUrl = String(url || "").trim();
  if (!safeUrl) {
    return {
      ok: false,
      statusCode: null,
      reason: "missing_url",
    };
  }

  let response;
  try {
    response = await runWithTimeout(
      fetch(safeUrl, {
        method: "HEAD",
      }),
      {
        timeoutMs,
        label: "provider_preflight_http_head_probe",
        timeoutCode: "preflight_probe_timeout",
        logger: console,
        meta: {
          scope: "provider_preflight",
        },
      },
    );
  } catch (headError) {
    try {
      response = await runWithTimeout(
        fetch(safeUrl, {
          method: "GET",
        }),
        {
          timeoutMs,
          label: "provider_preflight_http_get_probe",
          timeoutCode: "preflight_probe_timeout",
          logger: console,
          meta: {
            scope: "provider_preflight",
          },
        },
      );
    } catch (getError) {
      return {
        ok: false,
        statusCode: null,
        reason: redactError(getError || headError),
      };
    }
  }

  const statusCode = Number(response?.status);
  const ok = Number.isFinite(statusCode) && statusCode >= 200 && statusCode < 500;
  return {
    ok,
    statusCode: Number.isFinite(statusCode) ? statusCode : null,
    reason: ok ? null : `Unexpected status ${statusCode}`,
  };
}

async function runTwilioCredentialCheck(channel, config, options = {}) {
  const missing = [];
  if (!config?.twilio?.accountSid) missing.push("TWILIO_ACCOUNT_SID");
  if (!config?.twilio?.authToken) missing.push("TWILIO_AUTH_TOKEN");
  if (!config?.twilio?.fromNumber) missing.push("FROM_NUMBER");
  if (missing.length > 0) {
    return {
      status: CHECK_STATUS.FAIL,
      reason: `Missing required credentials: ${missing.join(", ")}`,
      suggestedFix: "Set required Twilio env vars and redeploy.",
      details: { missing },
    };
  }

  if (options.allowNetwork !== true) {
    return {
      status: CHECK_STATUS.WARN,
      reason: "Network auth probe skipped (allowNetwork=false)",
      suggestedFix: "Run live preflight with network checks enabled before promotion.",
    };
  }

  try {
    const client = twilio(config.twilio.accountSid, config.twilio.authToken);
    const account = await runWithTimeout(
      client.api.v2010.accounts(config.twilio.accountSid).fetch(),
      {
        timeoutMs: options.timeoutMs,
        label: "provider_preflight_twilio_auth_probe",
        timeoutCode: "twilio_auth_probe_timeout",
        logger: console,
        meta: {
          provider: "twilio",
          scope: "provider_preflight",
        },
      },
    );
    if (!account?.sid) {
      return {
        status: CHECK_STATUS.FAIL,
        reason: "Twilio auth probe did not return a valid account SID",
        suggestedFix: "Verify TWILIO_ACCOUNT_SID/TWILIO_AUTH_TOKEN values.",
      };
    }
    return {
      status: CHECK_STATUS.PASS,
      details: {
        account_sid: account.sid,
        channel: normalizeChannel(channel),
      },
    };
  } catch (error) {
    return {
      status: CHECK_STATUS.FAIL,
      reason: redactError(error),
      suggestedFix:
        "Confirm Twilio credentials are valid and API access is allowed from this environment.",
    };
  }
}

async function runVonageCredentialCheck(channel, config, options = {}) {
  const missing = [];
  if (!config?.vonage?.apiKey) missing.push("VONAGE_API_KEY");
  if (!config?.vonage?.apiSecret) missing.push("VONAGE_API_SECRET");
  const normalizedChannel = normalizeChannel(channel);
  if (normalizedChannel === "call") {
    if (!config?.vonage?.applicationId) missing.push("VONAGE_APPLICATION_ID");
    if (!config?.vonage?.privateKey) missing.push("VONAGE_PRIVATE_KEY");
    if (!config?.vonage?.voice?.fromNumber) missing.push("VONAGE_VOICE_FROM_NUMBER");
  }
  if (normalizedChannel === "sms" && !config?.vonage?.sms?.fromNumber) {
    missing.push("VONAGE_SMS_FROM_NUMBER");
  }

  if (missing.length > 0) {
    return {
      status: CHECK_STATUS.FAIL,
      reason: `Missing required credentials: ${missing.join(", ")}`,
      suggestedFix: "Set required Vonage env vars and redeploy.",
      details: { missing },
    };
  }

  if (options.allowNetwork !== true) {
    return {
      status: CHECK_STATUS.WARN,
      reason: "Network auth probe skipped (allowNetwork=false)",
      suggestedFix: "Run live preflight with network checks enabled before promotion.",
    };
  }

  try {
    const client = new Vonage({
      apiKey: config.vonage.apiKey,
      apiSecret: config.vonage.apiSecret,
      applicationId: config.vonage.applicationId,
      privateKey: config.vonage.privateKey,
    });
    const balance = await runWithTimeout(
      client.account.getBalance(),
      {
        timeoutMs: options.timeoutMs,
        label: "provider_preflight_vonage_auth_probe",
        timeoutCode: "vonage_auth_probe_timeout",
        logger: console,
        meta: {
          provider: "vonage",
          scope: "provider_preflight",
        },
      },
    );
    const value = Number(balance?.value ?? balance?.balance);
    if (!Number.isFinite(value)) {
      return {
        status: CHECK_STATUS.FAIL,
        reason: "Vonage auth probe did not return account balance",
        suggestedFix: "Verify Vonage API key/secret and account health.",
      };
    }
    return {
      status: CHECK_STATUS.PASS,
      details: {
        account_balance: value,
      },
    };
  } catch (error) {
    return {
      status: CHECK_STATUS.FAIL,
      reason: redactError(error),
      suggestedFix:
        "Confirm Vonage credentials are valid and API access is allowed from this environment.",
    };
  }
}

function runWebhookAuthCheck(provider, channel, config, options = {}) {
  const normalizedProvider = normalizeProvider(provider);
  const normalizedChannel = normalizeChannel(channel);
  if (normalizedProvider === "twilio") {
    const mode = String(config?.twilio?.webhookValidation || "warn").toLowerCase();
    if (mode === "off") {
      return {
        status: CHECK_STATUS.FAIL,
        reason: "TWILIO_WEBHOOK_VALIDATION is off",
        suggestedFix: "Set TWILIO_WEBHOOK_VALIDATION to warn or strict.",
      };
    }
    if (!config?.twilio?.authToken) {
      return {
        status: CHECK_STATUS.FAIL,
        reason: "TWILIO_AUTH_TOKEN missing for signature validation",
        suggestedFix: "Provide TWILIO_AUTH_TOKEN and redeploy.",
      };
    }
    if (options?.guards?.twilio !== true) {
      return {
        status: CHECK_STATUS.FAIL,
        reason: "Twilio signature guard is not wired",
        suggestedFix:
          "Ensure Twilio webhook handlers call requireValidTwilioSignature before state mutation.",
      };
    }
    return {
      status: CHECK_STATUS.PASS,
      details: {
        validation_mode: mode,
        channel: normalizedChannel,
      },
    };
  }

  if (normalizedProvider === "vonage") {
    const mode = String(config?.vonage?.webhookValidation || "warn").toLowerCase();
    if (mode === "off") {
      return {
        status: CHECK_STATUS.FAIL,
        reason: "VONAGE_WEBHOOK_VALIDATION is off",
        suggestedFix: "Set VONAGE_WEBHOOK_VALIDATION to warn or strict.",
      };
    }
    if (mode === "strict" && !config?.vonage?.webhookSignatureSecret) {
      return {
        status: CHECK_STATUS.FAIL,
        reason: "Strict Vonage webhook validation requires VONAGE_WEBHOOK_SIGNATURE_SECRET",
        suggestedFix:
          "Set VONAGE_WEBHOOK_SIGNATURE_SECRET or lower VONAGE_WEBHOOK_VALIDATION risk mode.",
      };
    }
    if (options?.guards?.vonage !== true) {
      return {
        status: CHECK_STATUS.FAIL,
        reason: "Vonage webhook guard is not wired",
        suggestedFix:
          "Ensure Vonage webhook handlers call requireValidVonageWebhook before state mutation.",
      };
    }
    return {
      status: CHECK_STATUS.PASS,
      details: {
        validation_mode: mode,
        channel: normalizedChannel,
      },
    };
  }

  return {
    status: CHECK_STATUS.SKIP,
    reason: `Unsupported provider ${normalizedProvider || provider}`,
  };
}

async function runCallbackUrlCheck(provider, channel, config, options = {}) {
  const callbacks = buildProviderCallbackUrls(provider, channel, config, {
    hostOverride: options.hostOverride,
  });

  if (callbacks.reason && callbacks.urls.length === 0) {
    return {
      status: CHECK_STATUS.FAIL,
      reason: callbacks.reason,
      suggestedFix:
        "Set SERVER or provider-specific callback URL environment variables.",
    };
  }

  const invalidUrls = callbacks.urls.filter((url) => !isHttpsUrl(url));
  if (invalidUrls.length > 0) {
    return {
      status: CHECK_STATUS.FAIL,
      reason: `Callback URLs must be HTTPS. Invalid entries: ${invalidUrls.join(", ")}`,
      suggestedFix: "Configure HTTPS callback URLs for provider webhooks.",
      details: { invalid_urls: invalidUrls },
    };
  }

  if (options.requireReachability !== true) {
    return {
      status: CHECK_STATUS.PASS,
      reason: null,
      details: {
        callback_urls: callbacks.urls,
        reachability: "skipped",
      },
    };
  }

  const target = callbacks.base_url ? `${callbacks.base_url}/health` : callbacks.urls[0];
  const probe = await probeHttpReachability(target, options.timeoutMs);
  if (!probe.ok) {
    return {
      status: CHECK_STATUS.FAIL,
      reason: `Callback base reachability probe failed for ${target}: ${probe.reason || "unreachable"}`,
      suggestedFix:
        "Ensure SERVER points to a reachable HTTPS host and ingress routes requests to this service.",
      details: {
        target,
        status_code: probe.statusCode,
      },
    };
  }

  return {
    status: CHECK_STATUS.PASS,
    details: {
      callback_urls: callbacks.urls,
      reachability_probe: {
        target,
        status_code: probe.statusCode,
      },
    },
  };
}

function routeReferenceToString(routeRef = {}) {
  return `${String(routeRef.method || "").toUpperCase()} ${routeRef.path}`;
}

function hasRoute(routeSet, routeRef) {
  return routeSet.has(routeReferenceToString(routeRef));
}

function runRequiredRouteCheck(provider, channel, app) {
  const normalizedProvider = normalizeProvider(provider);
  const normalizedChannel = normalizeChannel(channel);
  const groups = REQUIRED_ROUTE_GROUPS?.[normalizedChannel]?.[normalizedProvider] || [];
  if (!groups.length) {
    return {
      status: CHECK_STATUS.SKIP,
      reason: `No route requirements registered for ${normalizedProvider}/${normalizedChannel}`,
    };
  }

  if (!app?._router?.stack) {
    return {
      status: CHECK_STATUS.FAIL,
      reason: "Express router stack is unavailable",
      suggestedFix: "Run preflight from a live API process after routes are registered.",
    };
  }

  const routeSet = collectRegisteredRoutes(app);
  const missing = [];

  for (const group of groups) {
    const groupSatisfied = (group.anyOf || []).some((routeRef) =>
      hasRoute(routeSet, routeRef),
    );
    if (!groupSatisfied) {
      missing.push({
        id: group.id,
        label: group.label,
        expected_routes: (group.anyOf || []).map(routeReferenceToString),
      });
    }
  }

  if (missing.length > 0) {
    return {
      status: CHECK_STATUS.FAIL,
      reason: `Missing required ${normalizedProvider.toUpperCase()} ${normalizedChannel} route registration(s)`,
      suggestedFix:
        "Ensure registerWebhookRoutes/app route declarations include required voice + SMS routes.",
      details: {
        missing,
      },
    };
  }

  return {
    status: CHECK_STATUS.PASS,
    details: {
      verified_groups: groups.map((group) => group.id),
      route_count: routeSet.size,
    },
  };
}

async function runProviderPreflight(options = {}) {
  const provider = normalizeProvider(options.provider);
  const channel = normalizeChannel(options.channel || "call");
  const mode = String(options.mode || "activation");
  const config = options.config || {};
  const report = createBaseReport({ provider, channel, mode });

  if (!isProviderSupported(channel, provider)) {
    report.checks.push(
      createCheckResult(
        "supported_provider",
        "Supported provider/channel",
        CHECK_STATUS.FAIL,
        {
          reason: `Unsupported provider/channel combination: ${provider || "unknown"}/${channel || "unknown"}`,
          suggestedFix: `Use one of: ${getSupportedProviders(channel).join(", ") || "none"}`,
        },
      ),
    );
    return finalizeReport(report);
  }

  await runCheck(
    report,
    "credentials_auth",
    "Credentials and provider auth",
    async () => {
      if (provider === "twilio") {
        return runTwilioCredentialCheck(channel, config, {
          allowNetwork: options.allowNetwork,
          timeoutMs: options.timeoutMs,
        });
      }
      if (provider === "vonage") {
        return runVonageCredentialCheck(channel, config, {
          allowNetwork: options.allowNetwork,
          timeoutMs: options.timeoutMs,
        });
      }
      return {
        status: CHECK_STATUS.SKIP,
        reason: `No credential probe implemented for ${provider}`,
      };
    },
  );

  await runCheck(
    report,
    "webhook_auth",
    "Webhook auth configuration and guard",
    async () =>
      runWebhookAuthCheck(provider, channel, config, {
        guards: options.guards || {},
      }),
  );

  await runCheck(
    report,
    "callback_urls",
    "Callback URL configuration and reachability",
    async () =>
      runCallbackUrlCheck(provider, channel, config, {
        requireReachability: options.requireReachability,
        timeoutMs: options.timeoutMs,
        hostOverride: options.hostOverride,
      }),
  );

  await runCheck(
    report,
    "required_routes",
    "Required route registration",
    async () => runRequiredRouteCheck(provider, channel, options.app),
  );

  return finalizeReport(report);
}

function assertProviderPreflight(report, options = {}) {
  if (report?.ok === true) {
    return report;
  }
  const provider = normalizeProvider(options.provider || report?.provider);
  const channel = normalizeChannel(options.channel || report?.channel);
  const mode = String(options.mode || report?.mode || "activation");
  throw new ProviderPreflightError(
    `Provider preflight failed for ${provider || "unknown"}/${channel || "unknown"}`,
    {
      code: "provider_preflight_failed",
      provider,
      channel,
      mode,
      report: report || null,
    },
  );
}

function formatPreflightReport(report) {
  if (!report) {
    return "No preflight report generated.";
  }
  const status = report.ok ? "PASS" : "FAIL";
  const lines = [
    `Provider Preflight ${status}: ${report.provider}/${report.channel} (mode=${report.mode})`,
    `Generated: ${report.generated_at}`,
  ];

  for (const check of report.checks || []) {
    const icon =
      check.status === CHECK_STATUS.PASS
        ? "[PASS]"
        : check.status === CHECK_STATUS.WARN
          ? "[WARN]"
          : check.status === CHECK_STATUS.SKIP
            ? "[SKIP]"
            : "[FAIL]";
    const reason = check.reason ? ` - ${check.reason}` : "";
    lines.push(`${icon} ${check.id}: ${check.label}${reason}`);
    if (check.suggested_fix) {
      lines.push(`  fix: ${check.suggested_fix}`);
    }
  }

  lines.push(
    `Summary: pass=${report.summary.pass} fail=${report.summary.fail} warn=${report.summary.warn} skip=${report.summary.skip}`,
  );
  return lines.join("\n");
}

module.exports = {
  CHECK_STATUS,
  REQUIRED_ROUTE_GROUPS,
  SUPPORTED_PROVIDER_PREFLIGHT_CHANNELS,
  ProviderPreflightError,
  normalizeProvider,
  normalizeChannel,
  normalizeHost,
  isProviderSupported,
  collectRegisteredRoutes,
  buildProviderCallbackUrls,
  runProviderPreflight,
  assertProviderPreflight,
  formatPreflightReport,
};
