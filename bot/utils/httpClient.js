const axios = require('axios');

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withRetry(fn, options = {}) {
  const {
    retries = 2,
    baseDelayMs = 250,
    maxDelayMs = 2000,
    retryOn = (error) => {
      if (!error) return false;
      if (error.code && ['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'ENOTFOUND'].includes(error.code)) {
        return true;
      }
      const status = error.response?.status;
      return status >= 500 && status < 600;
    }
  } = options;

  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (error) {
      attempt += 1;
      if (attempt > retries || !retryOn(error)) {
        throw error;
      }
      const delay = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, attempt - 1));
      await sleep(delay);
    }
  }
}

async function get(ctx, url, options = {}) {
  return withRetry(() => axios.get(url, options), options.retry);
}

async function post(ctx, url, data, options = {}) {
  return withRetry(() => axios.post(url, data, options), options.retry);
}

async function put(ctx, url, data, options = {}) {
  return withRetry(() => axios.put(url, data, options), options.retry);
}

async function del(ctx, url, options = {}) {
  return withRetry(() => axios.delete(url, options), options.retry);
}

module.exports = {
  withRetry,
  get,
  post,
  put,
  del
};
