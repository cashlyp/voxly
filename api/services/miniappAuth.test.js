"use strict";

const {
  buildDataCheckString,
  computeInitDataHash,
  validateInitData,
  createMiniAppSessionToken,
  verifyMiniAppSessionToken,
  MiniAppAuthError,
} = require("./miniappAuth");

function buildInitDataQuery({
  botToken = "123456:TEST_TOKEN",
  authDate = 1710000000,
  queryId = "AAEAAAE",
  user = { id: 123456789, username: "admin_user", first_name: "Admin" },
}) {
  const params = new URLSearchParams();
  params.set("auth_date", String(authDate));
  params.set("query_id", queryId);
  params.set("user", JSON.stringify(user));
  const rawWithoutHash = params.toString();
  const hash = computeInitDataHash(rawWithoutHash, botToken);
  params.set("hash", hash);
  return {
    raw: params.toString(),
    hash,
    botToken,
  };
}

describe("miniappAuth", () => {
  test("buildDataCheckString sorts keys and excludes hash", () => {
    const params = new URLSearchParams();
    params.set("b", "2");
    params.set("hash", "skip");
    params.set("a", "1");
    expect(buildDataCheckString(params)).toBe("a=1\nb=2");
  });

  test("buildDataCheckString excludes signature field as well", () => {
    const params = new URLSearchParams();
    params.set("auth_date", "1710000000");
    params.set("signature", "skip-me");
    params.set("hash", "skip-too");
    params.set("query_id", "QID");
    expect(buildDataCheckString(params)).toBe("auth_date=1710000000\nquery_id=QID");
  });

  test("validateInitData accepts valid payload", () => {
    const { raw, botToken } = buildInitDataQuery({});
    const result = validateInitData(raw, botToken, {
      nowSeconds: 1710000100,
      maxAgeSeconds: 600,
    });
    expect(result.user.id).toBe(123456789);
    expect(result.queryId).toBe("AAEAAAE");
  });

  test("validateInitData rejects expired payload", () => {
    const { raw, botToken } = buildInitDataQuery({ authDate: 1710000000 });
    expect(() =>
      validateInitData(raw, botToken, {
        nowSeconds: 1710001001,
        maxAgeSeconds: 1000,
      }),
    ).toThrow(MiniAppAuthError);
  });

  test("validateInitData rejects tampered payload", () => {
    const { raw, botToken } = buildInitDataQuery({});
    const tampered = raw.replace("admin_user", "evil_user");
    expect(() => validateInitData(tampered, botToken)).toThrow(MiniAppAuthError);
  });

  test("session token round-trip works", () => {
    const claims = {
      sub: "tg:123456789",
      telegram_id: 123456789,
      role: "admin",
      caps: ["provider_manage", "sms_bulk_manage"],
    };
    const token = createMiniAppSessionToken(claims, "secret", {
      nowSeconds: 1710000000,
      ttlSeconds: 600,
    });
    const payload = verifyMiniAppSessionToken(token, "secret", {
      nowSeconds: 1710000300,
    });
    expect(payload.sub).toBe("tg:123456789");
    expect(payload.role).toBe("admin");
    expect(payload.caps).toContain("provider_manage");
  });

  test("session token expires as expected", () => {
    const token = createMiniAppSessionToken(
      { sub: "tg:1", role: "admin", caps: [] },
      "secret",
      { nowSeconds: 1710000000, ttlSeconds: 60 },
    );
    expect(() =>
      verifyMiniAppSessionToken(token, "secret", {
        nowSeconds: 1710000700,
      }),
    ).toThrow(MiniAppAuthError);
  });
});
