const path = require('path');
const sqlite3 = require('sqlite3').verbose();

// Store DB in project root as data.db
const dbPath = path.resolve(__dirname, '../db/data.db');
const db = new sqlite3.Database(dbPath);

const { userId, username } = require('../config').admin;

db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS users (
    telegram_id INTEGER PRIMARY KEY,
    username TEXT,
    role TEXT CHECK(role IN ('ADMIN','USER')) NOT NULL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
  db.run(`INSERT OR IGNORE INTO users (telegram_id, username, role) VALUES (?, ?, 'ADMIN')`, [userId, username]);
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

module.exports = {
  getUser, addUser, getUserList, promoteUser, removeUser,
  isAdmin, expireInactiveUsers
};