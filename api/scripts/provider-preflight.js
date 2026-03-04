#!/usr/bin/env node

const fetch = require("node-fetch");
const { formatPreflightReport } = require("../adapters/providerPreflight");

function parseArgs(argv = []) {
  const result = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = String(argv[i] || "");
    if (!token.startsWith("--")) continue;
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || String(next).startsWith("--")) {
      result[key] = "1";
      continue;
    }
    result[key] = String(next);
    i += 1;
  }
  return result;
}

function boolFrom(value, fallback = true) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return fallback;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const baseUrl = String(
    args["base-url"] ||
      process.env.PROVIDER_PREFLIGHT_BASE_URL ||
      process.env.BASE_URL ||
      `http://127.0.0.1:${process.env.PORT || "3000"}`,
  )
    .trim()
    .replace(/\/+$/, "");
  const adminToken = String(
    args.token || process.env.ADMIN_API_TOKEN || process.env.API_SECRET || "",
  ).trim();

  if (!adminToken) {
    console.error(
      "Missing admin token. Provide --token or set ADMIN_API_TOKEN/API_SECRET.",
    );
    process.exit(2);
    return;
  }

  const provider = String(args.provider || "").trim().toLowerCase();
  const channel = String(args.channel || "call").trim().toLowerCase();
  const network = boolFrom(args.network, true) ? "1" : "0";
  const reachability = boolFrom(args.reachability, true) ? "1" : "0";
  const timeoutMsRaw = Number(args["timeout-ms"] || args.timeout_ms);
  const timeoutMs =
    Number.isFinite(timeoutMsRaw) && timeoutMsRaw > 0
      ? Math.min(Math.floor(timeoutMsRaw), 20000)
      : 7000;

  const url = new URL(`${baseUrl}/admin/provider/preflight`);
  url.searchParams.set("channel", channel);
  if (provider) {
    url.searchParams.set("provider", provider);
  }
  url.searchParams.set("network", network);
  url.searchParams.set("reachability", reachability);
  url.searchParams.set("timeout_ms", String(timeoutMs));

  let response;
  try {
    response = await fetch(url.toString(), {
      method: "GET",
      headers: {
        "x-admin-token": adminToken,
      },
    });
  } catch (error) {
    console.error(`Preflight request failed: ${error.message || error}`);
    process.exit(2);
    return;
  }

  let payload = null;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }

  if (!payload) {
    console.error(`Preflight response was not valid JSON (status=${response.status})`);
    process.exit(2);
    return;
  }

  if (payload.report) {
    console.log(formatPreflightReport(payload.report));
  } else {
    console.log(JSON.stringify(payload, null, 2));
  }

  if (!response.ok || payload.success !== true) {
    process.exit(1);
    return;
  }
  process.exit(0);
}

main().catch((error) => {
  console.error(`Provider preflight CLI failed: ${error.message || error}`);
  process.exit(2);
});
