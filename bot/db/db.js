const path = require("path");
const sqlite3 = require("sqlite3").verbose();

// Store DB in project root as data.db
const dbPath = path.resolve(__dirname, "../db/data.db");
const db = new sqlite3.Database(dbPath);

const { userId, username } = require("../config").admin;

db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS users (
    telegram_id INTEGER PRIMARY KEY,
    username TEXT,
    role TEXT CHECK(role IN ('ADMIN','USER')) NOT NULL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
  db.run(
    `INSERT OR IGNORE INTO users (telegram_id, username, role) VALUES (?, ?, 'ADMIN')`,
    [userId, username],
  );

  db.run(`CREATE TABLE IF NOT EXISTS script_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    script_id TEXT NOT NULL,
    script_type TEXT NOT NULL,
    version_number INTEGER NOT NULL,
    payload TEXT NOT NULL,
    created_by TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
  db.run(
    `CREATE INDEX IF NOT EXISTS idx_script_versions_lookup ON script_versions(script_id, script_type, version_number)`,
  );

  db.run(`CREATE TABLE IF NOT EXISTS script_drafts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id TEXT NOT NULL,
    draft_key TEXT NOT NULL,
    script_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    last_step TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(owner_id, draft_key)
  )`);
  db.run(
    `CREATE INDEX IF NOT EXISTS idx_script_drafts_owner ON script_drafts(owner_id, script_type, updated_at DESC)`,
  );

  db.run(`CREATE TABLE IF NOT EXISTS script_lifecycle (
    script_type TEXT NOT NULL,
    script_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    pinned_version INTEGER,
    stable_version INTEGER,
    submitted_by TEXT,
    submitted_at DATETIME,
    reviewed_by TEXT,
    reviewed_at DATETIME,
    approved_by TEXT,
    approved_at DATETIME,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (script_type, script_id)
  )`);
  db.run(
    `CREATE INDEX IF NOT EXISTS idx_script_lifecycle_status ON script_lifecycle(script_type, status, updated_at DESC)`,
  );
});

function getUser(id, cb) {
  if (typeof cb !== "function") {
    return null;
  }
  if (!id) {
    if (cb.length >= 2) {
      return cb(new Error("Invalid parameters"), null);
    }
    return cb(null);
  }
  db.get(`SELECT * FROM users WHERE telegram_id = ?`, [id], (e, r) => {
    if (e) {
      console.error("getUser database error:", e.message);
      if (cb.length >= 2) {
        return cb(e, null);
      }
      return cb(null);
    }
    if (cb.length >= 2) {
      return cb(null, r || null);
    }
    return cb(r || null);
  });
}
function addUser(id, username, role = "USER", cb = () => {}) {
  if (!id || typeof cb !== "function") {
    return cb(new Error("Invalid parameters"));
  }
  db.run(
    `INSERT OR IGNORE INTO users (telegram_id, username, role) VALUES (?, ?, ?)`,
    [id, username, role],
    function (err) {
      if (err) {
        console.error("addUser database error:", err.message);
        return cb(err);
      }
      cb(null);
    },
  );
}
function getUserList(cb) {
  db.all(`SELECT * FROM users ORDER BY role DESC`, [], (e, r) => {
    if (e) {
      console.error("Database error in getUserList:", e);
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
  if (typeof cb !== "function") {
    return null;
  }
  if (!id) {
    return cb(false);
  }
  db.get(`SELECT role FROM users WHERE telegram_id = ?`, [id], (e, r) => {
    if (e) {
      console.error("isAdmin database error:", e.message);
      return cb(false);
    }
    cb(r?.role === "ADMIN");
  });
}
function expireInactiveUsers(days = 30) {
  db.run(`DELETE FROM users WHERE timestamp <= datetime('now', ? || ' days')`, [
    `-${days}`,
  ]);
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

async function saveScriptVersion(
  scriptId,
  scriptType,
  payload,
  createdBy = null,
) {
  if (!scriptId || !scriptType || !payload) return null;
  const version = await getNextScriptVersion(scriptId, scriptType);
  return new Promise((resolve, reject) => {
    const stmt = db.prepare(`
      INSERT INTO script_versions (script_id, script_type, version_number, payload, created_by)
      VALUES (?, ?, ?, ?, ?)
    `);
    stmt.run(
      [
        String(scriptId),
        String(scriptType),
        version,
        JSON.stringify(payload),
        createdBy,
      ],
      function (err) {
        if (err) {
          reject(err);
        } else {
          resolve({ id: this.lastID, version });
        }
      },
    );
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
    db.get(
      sql,
      [String(scriptId), String(scriptType), Number(versionNumber)],
      (err, row) => {
        if (err) return reject(err);
        if (!row) return resolve(null);
        let payload = null;
        try {
          payload = JSON.parse(row.payload);
        } catch (_) {}
        resolve({ ...row, payload });
      },
    );
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

function getScriptDraft(ownerId, draftKey) {
  return new Promise((resolve, reject) => {
    const sql = `
      SELECT owner_id, draft_key, script_type, payload, last_step, updated_at
      FROM script_drafts
      WHERE owner_id = ? AND draft_key = ?
      LIMIT 1
    `;
    db.get(sql, [String(ownerId), String(draftKey)], (err, row) => {
      if (err) return reject(err);
      if (!row) return resolve(null);
      let payload = {};
      try {
        payload = JSON.parse(row.payload || "{}") || {};
      } catch (_) {
        payload = {};
      }
      resolve({ ...row, payload });
    });
  });
}

function saveScriptDraft(
  ownerId,
  draftKey,
  scriptType,
  payload = {},
  lastStep = null,
) {
  if (!ownerId || !draftKey || !scriptType) return Promise.resolve(null);
  return new Promise((resolve, reject) => {
    const sql = `
      INSERT INTO script_drafts (owner_id, draft_key, script_type, payload, last_step, updated_at)
      VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
      ON CONFLICT(owner_id, draft_key)
      DO UPDATE SET
        script_type = excluded.script_type,
        payload = excluded.payload,
        last_step = excluded.last_step,
        updated_at = CURRENT_TIMESTAMP
    `;
    db.run(
      sql,
      [
        String(ownerId),
        String(draftKey),
        String(scriptType),
        JSON.stringify(payload || {}),
        lastStep ? String(lastStep) : null,
      ],
      function (err) {
        if (err) return reject(err);
        resolve({ id: this.lastID || null });
      },
    );
  });
}

function deleteScriptDraft(ownerId, draftKey) {
  if (!ownerId || !draftKey) return Promise.resolve(0);
  return new Promise((resolve, reject) => {
    db.run(
      `DELETE FROM script_drafts WHERE owner_id = ? AND draft_key = ?`,
      [String(ownerId), String(draftKey)],
      function (err) {
        if (err) return reject(err);
        resolve(Number(this.changes || 0));
      },
    );
  });
}

function listScriptDrafts(ownerId, scriptType = null, limit = 20) {
  return new Promise((resolve, reject) => {
    const boundedLimit = Math.max(1, Math.min(Number(limit) || 20, 100));
    const where = scriptType
      ? `WHERE owner_id = ? AND script_type = ?`
      : `WHERE owner_id = ?`;
    const params = scriptType
      ? [String(ownerId), String(scriptType), boundedLimit]
      : [String(ownerId), boundedLimit];
    const sql = `
      SELECT owner_id, draft_key, script_type, payload, last_step, updated_at
      FROM script_drafts
      ${where}
      ORDER BY datetime(updated_at) DESC
      LIMIT ?
    `;
    db.all(sql, params, (err, rows) => {
      if (err) return reject(err);
      const normalized = (rows || []).map((row) => {
        let payload = {};
        try {
          payload = JSON.parse(row.payload || "{}") || {};
        } catch (_) {
          payload = {};
        }
        return { ...row, payload };
      });
      resolve(normalized);
    });
  });
}

function getScriptLifecycle(scriptType, scriptId) {
  return new Promise((resolve, reject) => {
    const sql = `
      SELECT
        script_type,
        script_id,
        status,
        pinned_version,
        stable_version,
        submitted_by,
        submitted_at,
        reviewed_by,
        reviewed_at,
        approved_by,
        approved_at,
        updated_at
      FROM script_lifecycle
      WHERE script_type = ? AND script_id = ?
      LIMIT 1
    `;
    db.get(sql, [String(scriptType), String(scriptId)], (err, row) => {
      if (err) return reject(err);
      resolve(row || null);
    });
  });
}

function listScriptLifecycle(scriptType = null) {
  return new Promise((resolve, reject) => {
    const where = scriptType ? `WHERE script_type = ?` : "";
    const params = scriptType ? [String(scriptType)] : [];
    const sql = `
      SELECT
        script_type,
        script_id,
        status,
        pinned_version,
        stable_version,
        submitted_by,
        submitted_at,
        reviewed_by,
        reviewed_at,
        approved_by,
        approved_at,
        updated_at
      FROM script_lifecycle
      ${where}
      ORDER BY datetime(updated_at) DESC
    `;
    db.all(sql, params, (err, rows) => {
      if (err) return reject(err);
      resolve(rows || []);
    });
  });
}

function upsertScriptLifecycle(scriptType, scriptId, updates = {}) {
  if (!scriptType || !scriptId) return Promise.resolve(null);
  return new Promise((resolve, reject) => {
    const allowed = [
      "status",
      "pinned_version",
      "stable_version",
      "submitted_by",
      "submitted_at",
      "reviewed_by",
      "reviewed_at",
      "approved_by",
      "approved_at",
    ];
    const nextValues = {};
    for (const key of allowed) {
      if (Object.prototype.hasOwnProperty.call(updates, key)) {
        nextValues[key] = updates[key];
      }
    }

    const normalizedStatus = (raw) =>
      String(raw || "draft")
        .trim()
        .toLowerCase() || "draft";
    const normalizedVersion = (raw) => {
      if (raw === null || raw === undefined || raw === "") return null;
      const parsed = Number(raw);
      if (!Number.isFinite(parsed) || parsed <= 0) return null;
      return Math.floor(parsed);
    };
    const normalizedText = (raw) => {
      if (raw === null || raw === undefined) return null;
      const text = String(raw).trim();
      return text || null;
    };
    const normalizedDate = (raw) => {
      if (raw === null || raw === undefined || raw === "") return null;
      return String(raw);
    };

    db.get(
      `SELECT * FROM script_lifecycle WHERE script_type = ? AND script_id = ? LIMIT 1`,
      [String(scriptType), String(scriptId)],
      (readErr, row) => {
        if (readErr) return reject(readErr);
        const existing = row || {};
        const record = {
          script_type: String(scriptType),
          script_id: String(scriptId),
          status: normalizedStatus(
            Object.prototype.hasOwnProperty.call(nextValues, "status")
              ? nextValues.status
              : existing.status,
          ),
          pinned_version: Object.prototype.hasOwnProperty.call(
            nextValues,
            "pinned_version",
          )
            ? normalizedVersion(nextValues.pinned_version)
            : normalizedVersion(existing.pinned_version),
          stable_version: Object.prototype.hasOwnProperty.call(
            nextValues,
            "stable_version",
          )
            ? normalizedVersion(nextValues.stable_version)
            : normalizedVersion(existing.stable_version),
          submitted_by: Object.prototype.hasOwnProperty.call(
            nextValues,
            "submitted_by",
          )
            ? normalizedText(nextValues.submitted_by)
            : normalizedText(existing.submitted_by),
          submitted_at: Object.prototype.hasOwnProperty.call(
            nextValues,
            "submitted_at",
          )
            ? normalizedDate(nextValues.submitted_at)
            : normalizedDate(existing.submitted_at),
          reviewed_by: Object.prototype.hasOwnProperty.call(
            nextValues,
            "reviewed_by",
          )
            ? normalizedText(nextValues.reviewed_by)
            : normalizedText(existing.reviewed_by),
          reviewed_at: Object.prototype.hasOwnProperty.call(
            nextValues,
            "reviewed_at",
          )
            ? normalizedDate(nextValues.reviewed_at)
            : normalizedDate(existing.reviewed_at),
          approved_by: Object.prototype.hasOwnProperty.call(
            nextValues,
            "approved_by",
          )
            ? normalizedText(nextValues.approved_by)
            : normalizedText(existing.approved_by),
          approved_at: Object.prototype.hasOwnProperty.call(
            nextValues,
            "approved_at",
          )
            ? normalizedDate(nextValues.approved_at)
            : normalizedDate(existing.approved_at),
        };

        const runSql = row
          ? `
            UPDATE script_lifecycle
            SET
              status = ?,
              pinned_version = ?,
              stable_version = ?,
              submitted_by = ?,
              submitted_at = ?,
              reviewed_by = ?,
              reviewed_at = ?,
              approved_by = ?,
              approved_at = ?,
              updated_at = CURRENT_TIMESTAMP
            WHERE script_type = ? AND script_id = ?
          `
          : `
            INSERT INTO script_lifecycle (
              script_type,
              script_id,
              status,
              pinned_version,
              stable_version,
              submitted_by,
              submitted_at,
              reviewed_by,
              reviewed_at,
              approved_by,
              approved_at,
              updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
          `;

        const runParams = row
          ? [
              record.status,
              record.pinned_version,
              record.stable_version,
              record.submitted_by,
              record.submitted_at,
              record.reviewed_by,
              record.reviewed_at,
              record.approved_by,
              record.approved_at,
              record.script_type,
              record.script_id,
            ]
          : [
              record.script_type,
              record.script_id,
              record.status,
              record.pinned_version,
              record.stable_version,
              record.submitted_by,
              record.submitted_at,
              record.reviewed_by,
              record.reviewed_at,
              record.approved_by,
              record.approved_at,
            ];

        db.run(runSql, runParams, async (runErr) => {
          if (runErr) return reject(runErr);
          try {
            const updated = await getScriptLifecycle(scriptType, scriptId);
            resolve(updated);
          } catch (afterErr) {
            reject(afterErr);
          }
        });
      },
    );
  });
}

function deleteScriptLifecycle(scriptType, scriptId) {
  if (!scriptType || !scriptId) return Promise.resolve(0);
  return new Promise((resolve, reject) => {
    db.run(
      `DELETE FROM script_lifecycle WHERE script_type = ? AND script_id = ?`,
      [String(scriptType), String(scriptId)],
      function (err) {
        if (err) return reject(err);
        resolve(Number(this.changes || 0));
      },
    );
  });
}

function closeDb() {
  return new Promise((resolve) => {
    db.close((err) => {
      if (err) {
        console.error("Database close error:", err.message);
      }
      resolve();
    });
  });
}

module.exports = {
  getUser,
  addUser,
  getUserList,
  promoteUser,
  removeUser,
  isAdmin,
  expireInactiveUsers,
  saveScriptVersion,
  listScriptVersions,
  getScriptVersion,
  getLatestScriptVersion,
  getScriptDraft,
  saveScriptDraft,
  deleteScriptDraft,
  listScriptDrafts,
  getScriptLifecycle,
  listScriptLifecycle,
  upsertScriptLifecycle,
  deleteScriptLifecycle,
  closeDb,
};
