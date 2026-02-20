const path = require('path');
const sqlite3 = require('sqlite3').verbose();

// Store DB in project root as data.db
const dbPath = path.resolve(__dirname, '../db/data.db');
const db = new sqlite3.Database(dbPath);

const { userId, username } = require('../config').admin;

db.serialize(() => {
  db.exec(`
    PRAGMA foreign_keys = ON;
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;
    PRAGMA busy_timeout = 5000;
  `, (err) => {
    if (err) {
      console.error('Database pragma initialization error:', err.message);
    }
  });

  db.run(`CREATE TABLE IF NOT EXISTS users (
    telegram_id INTEGER PRIMARY KEY,
    username TEXT,
    role TEXT CHECK(role IN ('ADMIN','USER')) NOT NULL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
  db.run(`INSERT OR IGNORE INTO users (telegram_id, username, role) VALUES (?, ?, 'ADMIN')`, [userId, username]);

  db.run(`CREATE TABLE IF NOT EXISTS script_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    script_id TEXT NOT NULL,
    script_type TEXT NOT NULL,
    version_number INTEGER NOT NULL,
    payload TEXT NOT NULL,
    created_by TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_script_versions_lookup ON script_versions(script_id, script_type, version_number)`);
});

function getUser(id, cb) {
  db.get(`SELECT * FROM users WHERE telegram_id = ?`, [id], (e, r) => {
    if (e) return cb(null);
    cb(r);
  });
}
function addUser(id, username, role = 'USER', cb = () => {}) {
  db.run(`INSERT OR IGNORE INTO users (telegram_id, username, role) VALUES (?, ?, ?)`, [id, username, role], cb);
}
function getUserList(cb) {
  db.all(`SELECT * FROM users ORDER BY role DESC`, [], (e, r) => {
    if (e) {
      console.error('Database error in getUserList:', e);
      return cb(e, null);
    }
    cb(null, r || []);
  });
}
function promoteUser(id, cb = () => {}) {
  db.run(`UPDATE users SET role = 'ADMIN' WHERE telegram_id = ?`, [id], cb);
}
function removeUser(id, cb = () => {}) {
  db.run(`DELETE FROM users WHERE telegram_id = ?`, [id], cb);
}
function isAdmin(id, cb) {
  db.get(`SELECT role FROM users WHERE telegram_id = ?`, [id], (e, r) => {
    if (e) return cb(false);
    cb(r?.role === 'ADMIN');
  });
}
function expireInactiveUsers(days = 30) {
  db.run(`DELETE FROM users WHERE timestamp <= datetime('now', ? || ' days')`, [`-${days}`]);
}

function getNextScriptVersion(scriptId, scriptType) {
  return new Promise((resolve, reject) => {
    const sql = `SELECT MAX(version_number) AS max_version FROM script_versions WHERE script_id = ? AND script_type = ?`;
    db.get(sql, [scriptId, scriptType], (err, row) => {
      if (err) return reject(err);
      const next = Number(row?.max_version || 0) + 1;
      resolve(next);
    });
  });
}

async function saveScriptVersion(scriptId, scriptType, payload, createdBy = null) {
  if (!scriptId || !scriptType || !payload) return null;
  const version = await getNextScriptVersion(scriptId, scriptType);
  return new Promise((resolve, reject) => {
    const stmt = db.prepare(`
      INSERT INTO script_versions (script_id, script_type, version_number, payload, created_by)
      VALUES (?, ?, ?, ?, ?)
    `);
    stmt.run([
      String(scriptId),
      String(scriptType),
      version,
      JSON.stringify(payload),
      createdBy
    ], function(err) {
      if (err) {
        reject(err);
      } else {
        resolve({ id: this.lastID, version });
      }
    });
    stmt.finalize();
  });
}

function listScriptVersions(scriptId, scriptType, limit = 10) {
  return new Promise((resolve, reject) => {
    const sql = `
      SELECT id, script_id, script_type, version_number, created_by, created_at
      FROM script_versions
      WHERE script_id = ? AND script_type = ?
      ORDER BY version_number DESC
      LIMIT ?
    `;
    db.all(sql, [String(scriptId), String(scriptType), limit], (err, rows) => {
      if (err) return reject(err);
      resolve(rows || []);
    });
  });
}

function getScriptVersion(scriptId, scriptType, versionNumber) {
  return new Promise((resolve, reject) => {
    const sql = `
      SELECT id, script_id, script_type, version_number, payload, created_by, created_at
      FROM script_versions
      WHERE script_id = ? AND script_type = ? AND version_number = ?
      LIMIT 1
    `;
    db.get(sql, [String(scriptId), String(scriptType), Number(versionNumber)], (err, row) => {
      if (err) return reject(err);
      if (!row) return resolve(null);
      let payload = null;
      try {
        payload = JSON.parse(row.payload);
      } catch (_) {}
      resolve({ ...row, payload });
    });
  });
}

function getLatestScriptVersion(scriptId, scriptType) {
  return new Promise((resolve, reject) => {
    const sql = `
      SELECT id, script_id, script_type, version_number, payload, created_by, created_at
      FROM script_versions
      WHERE script_id = ? AND script_type = ?
      ORDER BY version_number DESC
      LIMIT 1
    `;
    db.get(sql, [String(scriptId), String(scriptType)], (err, row) => {
      if (err) return reject(err);
      if (!row) return resolve(null);
      let payload = null;
      try {
        payload = JSON.parse(row.payload);
      } catch (_) {}
      resolve({ ...row, payload });
    });
  });
}

function closeDb() {
  return new Promise((resolve) => {
    db.close((err) => {
      if (err) {
        console.error('Database close error:', err.message);
      }
      resolve();
    });
  });
}

module.exports = {
  getUser, addUser, getUserList, promoteUser, removeUser,
  isAdmin, expireInactiveUsers,
  saveScriptVersion,
  listScriptVersions,
  getScriptVersion,
  getLatestScriptVersion,
  closeDb
};
