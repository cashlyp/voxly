const fs = require("fs");
const os = require("os");
const path = require("path");
const Database = require("../db/db");

describe("database startup recovery", () => {
  jest.setTimeout(20000);

  test("auto rebuild restores service when sqlite file is corrupt", async () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "voxly-db-recover-"));
    const dbPath = path.join(tempRoot, "data.db");
    const backupDir = path.join(tempRoot, "corrupt-backups");
    fs.writeFileSync(dbPath, "not-a-valid-sqlite-image");

    const db = new Database({
      dbPath,
      startupIntegrityCheck: true,
      autoRebuildOnCorrupt: true,
      corruptBackupDir: backupDir,
    });

    try {
      await db.initialize();
      expect(db.isInitialized).toBe(true);
      const health = await db.healthCheck();
      expect(health.status).toBe("healthy");

      const recovery = db.getStartupRecoveryState();
      expect(recovery.rebuilt).toBe(true);
      expect(Array.isArray(recovery.backup_files)).toBe(true);
      expect(recovery.backup_files.length).toBeGreaterThan(0);
      recovery.backup_files.forEach((file) => {
        expect(fs.existsSync(file)).toBe(true);
      });
      expect(fs.existsSync(dbPath)).toBe(true);
    } finally {
      await db.close();
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("strict startup rejects corruption when auto rebuild is disabled", async () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "voxly-db-strict-"));
    const dbPath = path.join(tempRoot, "data.db");
    fs.writeFileSync(dbPath, "definitely-not-sqlite");

    const db = new Database({
      dbPath,
      startupIntegrityCheck: true,
      autoRebuildOnCorrupt: false,
    });

    try {
      await expect(db.initialize()).rejects.toThrow();
      expect(db.isInitialized).toBe(false);
    } finally {
      await db.close();
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });
});
