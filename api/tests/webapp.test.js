const crypto = require('crypto');
const request = require('supertest');
const http = require('http');
const net = require('net');

jest.mock('sqlite3', () => {
  class FakeDatabase {
    constructor(_path, callback) {
      if (typeof callback === 'function') {
        callback(null);
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
process.env.TELEGRAM_VIEWER_CHAT_IDS = '2222';
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

function buildInitData(userId) {
  const user = {
    id: userId,
    username: `user${userId}`,
    first_name: 'Test',
    last_name: 'User',
  };
  const authDate = Math.floor(Date.now() / 1000);
  const params = new URLSearchParams();
  params.set('auth_date', String(authDate));
  params.set('user', JSON.stringify(user));

  const entries = [];
  params.forEach((value, key) => entries.push([key, value]));
  entries.sort((a, b) => a[0].localeCompare(b[0]));
  const dataCheckString = entries.map(([key, value]) => `${key}=${value}`).join('\n');
  const secret = crypto.createHmac('sha256', process.env.TELEGRAM_BOT_TOKEN).update('WebAppData').digest();
  const hash = crypto.createHmac('sha256', secret).update(dataCheckString).digest('hex');
  params.set('hash', hash);
  return params.toString();
}

async function authAs(userId) {
  const initData = buildInitData(userId);
  const response = await request(app)
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
    expect(response.body.roles).toContain('admin');
  });

  it('rejects unauthorized user', async () => {
    if (!allowListen) return;
    const response = await authAs(3333);
    expect(response.status).toBe(403);
    expect(response.body.error).toBe('not_authorized');
  });

  it('blocks viewer from admin settings', async () => {
    if (!allowListen) return;
    const authResponse = await authAs(2222);
    const token = authResponse.body.token;
    const response = await request(app)
      .get('/webapp/settings')
      .set('Authorization', `Bearer ${token}`);
    expect(response.status).toBe(403);
  });
});

describe('Webapp SSE', () => {
  it('requires auth for SSE', async () => {
    if (!allowListen) return;
    const response = await request(app).get('/webapp/sse');
    expect(response.status).toBe(401);
  });

  it('opens SSE stream with token', (done) => {
    if (!allowListen) return done();
    authAs(1111).then((authResponse) => {
      const token = authResponse.body.token;
      const req = request(app).get(`/webapp/sse?token=${token}`);
      req.on('response', (res) => {
        expect(res.statusCode).toBe(200);
        expect(res.headers['content-type']).toMatch(/text\/event-stream/);
        res.destroy();
        done();
      });
      req.end();
    }).catch(done);
  });
});
