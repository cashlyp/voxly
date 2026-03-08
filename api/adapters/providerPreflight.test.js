const {
  buildProviderCallbackUrls,
  isProviderSupported,
  runProviderPreflight,
} = require("./providerPreflight");

function buildRouteLayer(method, path) {
  return {
    route: {
      path,
      methods: {
        [String(method || "GET").toLowerCase()]: true,
      },
    },
  };
}

function buildMockApp(routes = []) {
  return {
    _router: {
      stack: routes.map((entry) => buildRouteLayer(entry.method, entry.path)),
    },
  };
}

describe("provider preflight aws parity", () => {
  test("supports AWS call provider but not AWS sms provider", () => {
    expect(isProviderSupported("call", "aws")).toBe(true);
    expect(isProviderSupported("sms", "aws")).toBe(false);
  });

  test("builds AWS callback URLs for call channel", () => {
    const result = buildProviderCallbackUrls(
      "aws",
      "call",
      {
        server: {
          hostname: "voice.example.com",
        },
      },
      {},
    );
    expect(result.urls).toEqual([
      "https://voice.example.com/aws/transcripts",
      "https://voice.example.com/aws/stream",
    ]);
  });

  test("passes AWS call preflight in offline mode when guards/routes are present", async () => {
    const report = await runProviderPreflight({
      provider: "aws",
      channel: "call",
      mode: "activation",
      allowNetwork: false,
      requireReachability: false,
      config: {
        server: {
          hostname: "voice.example.com",
        },
        apiAuth: {
          hmacSecret: "shared-hmac-secret",
        },
        aws: {
          region: "us-east-1",
          webhookValidation: "warn",
          webhookSecret: "aws-webhook-secret",
          connect: {
            instanceId: "instance-123",
            contactFlowId: "flow-456",
          },
        },
      },
      app: buildMockApp([
        { method: "POST", path: "/aws/transcripts" },
        { method: "GET", path: "/aws/stream" },
      ]),
      guards: {
        awsWebhook: true,
        awsStream: true,
      },
    });

    expect(report.ok).toBe(true);
    expect(report.summary.fail).toBe(0);
    expect(report.summary.warn).toBe(1);
    const callbackCheck = report.checks.find((check) => check.id === "callback_urls");
    expect(callbackCheck?.status).toBe("pass");
    expect(callbackCheck?.details?.callback_urls || []).toContain(
      "https://voice.example.com/aws/transcripts",
    );
  });

  test("fails strict AWS webhook auth when no secret/hmac is configured", async () => {
    const report = await runProviderPreflight({
      provider: "aws",
      channel: "call",
      mode: "activation",
      allowNetwork: false,
      requireReachability: false,
      config: {
        server: {
          hostname: "voice.example.com",
        },
        apiAuth: {
          hmacSecret: "",
        },
        aws: {
          region: "us-east-1",
          webhookValidation: "strict",
          webhookSecret: "",
          connect: {
            instanceId: "instance-123",
            contactFlowId: "flow-456",
          },
        },
      },
      app: buildMockApp([
        { method: "POST", path: "/aws/transcripts" },
        { method: "GET", path: "/aws/stream" },
      ]),
      guards: {
        awsWebhook: true,
        awsStream: true,
      },
    });

    expect(report.ok).toBe(false);
    const authCheck = report.checks.find((check) => check.id === "webhook_auth");
    expect(authCheck?.status).toBe("fail");
    expect(String(authCheck?.reason || "")).toContain("requires");
  });
});
