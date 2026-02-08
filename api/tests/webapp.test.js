const crypto = require('crypto');
const request = require('supertest');
const http = require('http');
const net = require('net');

jest.mock('sqlite3', () => {
  class FakeDatabase {
    constructor(_path, callback) {
      if (typeof callback === 'function') {
        setImmediate(() => callback(null));
      }
    }

    run(...args) {
      const cb = args[args.length - 1];
      if (typeof cb === 'function') cb(null);
    }

    all(...args) {
      const cb = args[args.length - 1];
      if (typeof cb === 'function') cb(null, []);
    }

    get(...args) {
      const cb = args[args.length - 1];
      if (typeof cb === 'function') cb(null, null);
    }

    prepare() {
      return {
        run: (...args) => {
          const cb = args[args.length - 1];
          if (typeof cb === 'function') cb(null);
        },
        finalize: () => {},
      };
    }

    serialize(callback) {
      if (typeof callback === 'function') callback();
    }

    exec(_sql, callback) {
      if (typeof callback === 'function') callback(null);
    }

    close(callback) {
      if (typeof callback === 'function') callback(null);
    }
  }

  return {
    verbose: () => ({ Database: FakeDatabase }),
  };
});

jest.setTimeout(10000);

let allowListen = true;
let server;
const sockets = new Set();

beforeAll((done) => {
  const server = net.createServer();
  server.unref();
  server.once('error', () => {
    allowListen = false;
    console.warn('Skipping webapp tests: listen not permitted in this environment.');
    try {
      server.close(() => done());
    } catch {
      done();
    }
  });
  server.listen(0, '127.0.0.1', () => {
    server.close(() => done());
  });
});

const originalServerListen = http.Server.prototype.listen;
http.Server.prototype.listen = function overrideListen(...args) {
  if (typeof args[0] === 'function') {
    return originalServerListen.call(this, 0, '127.0.0.1', args[0]);
  }
  if (typeof args[1] === 'function') {
    return originalServerListen.call(this, args[0], '127.0.0.1', args[1]);
  }
  if (typeof args[2] === 'function') {
    return originalServerListen.call(this, args[0], '127.0.0.1', args[2]);
  }
  if (!args[1]) {
    args[1] = '127.0.0.1';
  }
  return originalServerListen.apply(this, args);
};

process.env.NODE_ENV = 'test';
process.env.TELEGRAM_BOT_TOKEN = 'test-bot-token';
process.env.MINIAPP_JWT_SECRET = 'test-jwt-secret';
process.env.TELEGRAM_ADMIN_CHAT_IDS = '1111';
process.env.TELEGRAM_OPERATOR_CHAT_IDS = '2222';
process.env.CALL_PROVIDER = 'twilio';
process.env.TWILIO_ACCOUNT_SID = 'ACXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX';
process.env.TWILIO_AUTH_TOKEN = 'test';

const { app } = require('../app');

const originalListen = app.listen.bind(app);
app.listen = (port, hostname, backlog, callback) => {
  if (typeof hostname === 'function') {
    return originalListen(port, '127.0.0.1', hostname);
  }
  if (typeof backlog === 'function') {
    return originalListen(port, '127.0.0.1', backlog);
  }
  return originalListen(port, '127.0.0.1', backlog, callback);
};

beforeAll((done) => {
  if (!allowListen) return done();
  server = app.listen(0, '127.0.0.1', () => {
    server.on('connection', (socket) => {
      sockets.add(socket);
      socket.on('close', () => sockets.delete(socket));
    });
    done();
  });
});

afterAll((done) => {
  if (!server) return done();
  sockets.forEach((socket) => socket.destroy());
  server.close(done);
});

function requestServer() {
  return request(server);
}

function buildInitData(userId, authDateOverride) {
  const user = {
    id: userId,
    username: `user${userId}`,
    first_name: 'Test',
    last_name: 'User',
  };
  const authDate = authDateOverride ?? Math.floor(Date.now() / 1000);
  const params = new URLSearchParams();
  params.set('auth_date', String(authDate));
  params.set('user', JSON.stringify(user));

  const entries = [];
  params.forEach((value, key) => entries.push([key, value]));
  entries.sort((a, b) => a[0].localeCompare(b[0]));
  const dataCheckString = entries.map(([key, value]) => `${key}=${value}`).join('\n');
  const secret = crypto.createHmac('sha256', 'WebAppData')
    .update(process.env.TELEGRAM_BOT_TOKEN)
    .digest();
  const hash = crypto.createHmac('sha256', secret).update(dataCheckString).digest('hex');
  params.set('hash', hash);
  return params.toString();
}

function restoreEnv(envBackup) {
  for (const key of Object.keys(process.env)) {
    if (!(key in envBackup)) {
      delete process.env[key];
    }
  }
  Object.assign(process.env, envBackup);
}

function base64UrlEncode(value) {
  return Buffer.from(value)
    .toString('base64')
    .replace(/=/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');
}

function signWebappJwt(payload) {
  const header = { alg: 'HS256', typ: 'JWT' };
  const encodedHeader = base64UrlEncode(JSON.stringify(header));
  const encodedPayload = base64UrlEncode(JSON.stringify(payload));
  const data = `${encodedHeader}.${encodedPayload}`;
  const signature = crypto
    .createHmac('sha256', process.env.MINIAPP_JWT_SECRET)
    .update(data)
    .digest('base64')
    .replace(/=/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');
  return `${data}.${signature}`;
}

async function authAs(userId) {
  const initData = buildInitData(userId);
  const response = await requestServer()
    .post('/webapp/auth')
    .send({ initData });
  return response;
}

describe('Webapp auth and roles', () => {
  it('authenticates admin user and returns JWT', async () => {
    if (!allowListen) return;
    const response = await authAs(1111);
    expect(response.status).toBe(200);
    expect(response.body.ok).toBe(true);
    expect(response.body.token).toBeTruthy();
    expect(response.body.accessToken).toBeTruthy();
    expect(response.body.accessToken).toBe(response.body.token);
    expect(response.body.roles).toContain('admin');
  });

  it('authenticates operator user and returns operator role', async () => {
    if (!allowListen) return;
    const response = await authAs(2222);
    expect(response.status).toBe(200);
    expect(response.body.ok).toBe(true);
    expect(response.body.roles).toContain('operator');
    expect(response.body.role).toBe('operator');
  });

  it('rejects unauthorized user', async () => {
    if (!allowListen) return;
    const response = await authAs(3333);
    expect(response.status).toBe(403);
    expect(response.body.error).toBe('not_authorized');
  });

  it('blocks operator from admin settings', async () => {
    if (!allowListen) return;
    const authResponse = await authAs(2222);
    const token = authResponse.body.accessToken || authResponse.body.token;
    const response = await requestServer()
      .get('/webapp/settings')
      .set('Authorization', `Bearer ${token}`);
    expect(response.status).toBe(403);
  });

  it('rejects expired initData', async () => {
    if (!allowListen) return;
    const expired = Math.floor(Date.now() / 1000) - 3600;
    const initData = buildInitData(1111, expired);
    const response = await requestServer()
      .post('/webapp/auth')
      .send({ initData });
    expect(response.status).toBe(401);
    expect(response.body.error).toBe('expired_init_data');
  });

  it('rejects tokens missing required claims', async () => {
    if (!allowListen) return;
    const now = Math.floor(Date.now() / 1000);
    const basePayload = {
      sub: '1111',
      iat: now,
      roles: ['admin'],
      iss: 'voicdnut-webapp',
      aud: 'voicednut-miniapp',
      exp: now + 300,
    };
    const cases = [
      { omit: 'exp', error: 'missing_exp' },
      { omit: 'iss', error: 'missing_issuer' },
      { omit: 'aud', error: 'missing_audience' },
    ];
    for (const testCase of cases) {
      const payload = { ...basePayload };
      delete payload[testCase.omit];
      const token = signWebappJwt(payload);
      const response = await requestServer()
        .get('/webapp/me')
        .set('Authorization', `Bearer ${token}`);
      expect(response.status).toBe(401);
      expect(response.body.error).toBe(testCase.error);
    }
  });

  it('rejects query-string tokens for non-SSE endpoints', async () => {
    if (!allowListen) return;
    const authResponse = await authAs(1111);
    const token = authResponse.body.token;
    const response = await requestServer().get(`/webapp/me?token=${token}`);
    expect(response.status).toBe(401);
    expect(response.body.error).toBe('missing_token');
  });
});

describe('Webapp outbound calls', () => {
  it('blocks viewer from outbound call', async () => {
    if (!allowListen) return;
    const authResponse = await authAs(2222);
    const token = authResponse.body.token;
    const response = await requestServer()
      .post('/webapp/outbound-call')
      .set('Authorization', `Bearer ${token}`)
      .send({ number: '+1234567890', prompt: 'test', first_message: 'hi' });
    expect(response.status).toBe(403);
  });

  it('validates required outbound payload', async () => {
    if (!allowListen) return;
    const authResponse = await authAs(1111);
    const token = authResponse.body.token;
    const response = await requestServer()
      .post('/webapp/outbound-call')
      .set('Authorization', `Bearer ${token}`)
      .send({});
    expect(response.status).toBe(400);
    expect(response.body.error).toBe('missing_fields');
  });

  it('rejects invalid phone numbers', async () => {
    if (!allowListen) return;
    const authResponse = await authAs(1111);
    const token = authResponse.body.token;
    const response = await requestServer()
      .post('/webapp/outbound-call')
      .set('Authorization', `Bearer ${token}`)
      .send({ number: '555', prompt: 'test', first_message: 'hi' });
    expect(response.status).toBe(400);
    expect(response.body.error).toBe('invalid_number');
  });
});

describe('Webapp SSE', () => {
  it('requires auth for SSE', async () => {
    if (!allowListen) return;
    const response = await requestServer().get('/webapp/sse');
    expect(response.status).toBe(401);
  });

  it('opens SSE stream with token', (done) => {
    if (!allowListen) return done();
    authAs(1111).then((authResponse) => {
      const token = authResponse.body.token;
      const port = server.address().port;
      const req = http.request(
        {
          hostname: '127.0.0.1',
          port,
          path: `/webapp/sse?token=${token}`,
          method: 'GET',
        },
        (res) => {
          expect(res.statusCode).toBe(200);
          expect(res.headers['content-type']).toMatch(/text\/event-stream/);
          res.destroy();
          done();
        },
      );
      req.on('error', done);
      req.end();
    }).catch(done);
  });
});

describe('Miniapp auth and tokens', () => {
  it('rejects query-string tokens for non-stream endpoints', async () => {
    if (!allowListen) return;
    const initData = buildInitData(1111);
    const bootstrap = await requestServer()
      .post('/miniapp/bootstrap')
      .send({ initData });
    expect(bootstrap.status).toBe(200);
    const token = bootstrap.body.session_token;
    const response = await requestServer()
      .get(`/miniapp/calls/active?token=${token}`)
      .set('X-Telegram-Init-Data', initData);
    expect(response.status).toBe(401);
    expect(response.body.error).toBe('missing_session');
  });
});

describe('Origin allowlist (production)', () => {
  it('rejects requests when allowlist is missing', async () => {
    if (!allowListen) return;
    const envBackup = { ...process.env };
    try {
      jest.resetModules();
      process.env.NODE_ENV = 'production';
      process.env.API_SECRET = 'test-secret';
      process.env.MINIAPP_ALLOWED_ORIGINS = '';
      process.env.TELEGRAM_BOT_TOKEN = 'test-bot-token';
      process.env.MINIAPP_JWT_SECRET = 'test-jwt-secret';
      process.env.TELEGRAM_ADMIN_CHAT_IDS = '1111';
      process.env.TELEGRAM_OPERATOR_CHAT_IDS = '2222';
      process.env.CALL_PROVIDER = 'twilio';
      process.env.TWILIO_ACCOUNT_SID = 'ACXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX';
      process.env.TWILIO_AUTH_TOKEN = 'test';
      process.env.FROM_NUMBER = '+15555550123';
      process.env.OPENROUTER_API_KEY = 'test-openrouter';
      process.env.DEEPGRAM_API_KEY = 'test-deepgram';
      const { app: prodApp } = require('../app');
      const initData = buildInitData(1111);
      const prodServer = prodApp.listen(0, '127.0.0.1');
      await new Promise((resolve) => prodServer.once('listening', resolve));
      const response = await request(prodServer)
        .post('/webapp/auth')
        .set('Origin', 'https://example.com')
        .send({ initData });
      await new Promise((resolve) => prodServer.close(resolve));
      expect(response.status).toBe(403);
      expect(response.body.error).toBe('origin_not_allowed');
    } finally {
      restoreEnv(envBackup);
    }
  });
});
