const colors = require("colors");

if (String(process.env.NODE_DISABLE_COLORS || "").trim() !== "1") {
  // PM2 and redirected streams are often non-TTY; force ANSI colors unless explicitly disabled.
  if (!process.env.FORCE_COLOR) {
    process.env.FORCE_COLOR = "1";
  }
  if (typeof colors.enable === "function") {
    colors.enable();
  } else {
    colors.enabled = true;
  }
}

if (!console.__apiColorWrapped) {
  const baseLog = console.log.bind(console);
  const baseInfo = console.info.bind(console);
  const baseWarn = console.warn.bind(console);
  const baseError = console.error.bind(console);
  const baseDebug = (typeof console.debug === "function"
    ? console.debug
    : console.log
  ).bind(console);

  const ts = () => new Date().toISOString().gray;
  const levelTag = (label, color) => `[${label}]`[color];

  console.log = (...args) => baseLog(ts(), levelTag("LOG", "cyan"), ...args);
  console.info = (...args) =>
    baseInfo(ts(), levelTag("INFO", "blue"), ...args);
  console.warn = (...args) =>
    baseWarn(ts(), levelTag("WARN", "yellow"), ...args);
  console.error = (...args) =>
    baseError(ts(), levelTag("ERROR", "red"), ...args);
  console.debug = (...args) =>
    baseDebug(ts(), levelTag("DEBUG", "magenta"), ...args);

  console.__apiColorWrapped = true;
}

module.exports = {
  installed: true,
};
