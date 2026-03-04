'use strict';

function maybeUnref(timer) {
  if (timer && typeof timer.unref === 'function') {
    timer.unref();
  }
}

function normalizeTimeoutMs(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return null;
  }
  return Math.floor(parsed);
}

function buildTimeoutError(label, timeoutMs, timeoutCode) {
  const error = new Error(`${label} timed out after ${timeoutMs}ms`);
  error.code = timeoutCode || 'operation_timeout';
  error.timeoutMs = timeoutMs;
  return error;
}

async function runWithTimeout(operationPromise, options = {}) {
  const timeoutMs = normalizeTimeoutMs(options.timeoutMs);
  if (!timeoutMs) {
    return Promise.resolve(operationPromise);
  }

  const label = String(options.label || 'operation');
  const timeoutCode = String(options.timeoutCode || 'operation_timeout');
  const logger = options.logger || console;
  const meta =
    options.meta && typeof options.meta === 'object' ? options.meta : {};
  const warnAfterMsRaw = normalizeTimeoutMs(options.warnAfterMs);
  const warnAfterMs =
    warnAfterMsRaw && warnAfterMsRaw < timeoutMs ? warnAfterMsRaw : null;

  let settled = false;
  let warnTimer = null;
  let timeoutTimer = null;

  return new Promise((resolve, reject) => {
    const clearTimers = () => {
      if (warnTimer) {
        clearTimeout(warnTimer);
        warnTimer = null;
      }
      if (timeoutTimer) {
        clearTimeout(timeoutTimer);
        timeoutTimer = null;
      }
    };

    if (warnAfterMs) {
      warnTimer = setTimeout(() => {
        if (settled) return;
        logger?.warn?.('long_running_operation', {
          label,
          warn_after_ms: warnAfterMs,
          timeout_ms: timeoutMs,
          ...meta,
        });
      }, warnAfterMs);
      maybeUnref(warnTimer);
    }

    timeoutTimer = setTimeout(() => {
      if (settled) return;
      settled = true;
      clearTimers();
      const timeoutError = buildTimeoutError(label, timeoutMs, timeoutCode);
      logger?.error?.('operation_timeout', {
        label,
        timeout_ms: timeoutMs,
        code: timeoutCode,
        ...meta,
      });
      reject(timeoutError);
    }, timeoutMs);
    maybeUnref(timeoutTimer);

    Promise.resolve(operationPromise)
      .then((result) => {
        if (settled) return;
        settled = true;
        clearTimers();
        resolve(result);
      })
      .catch((error) => {
        if (settled) return;
        settled = true;
        clearTimers();
        reject(error);
      });
  });
}

module.exports = {
  runWithTimeout,
};
