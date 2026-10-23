import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
import { runMigrations } from './migrate.js';
import { PostgresBridge } from './postgres-bridge.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '../../..');

/**
 * Checks if core tables exist and executes migrations if not yet initialized.
 * @param {Database} db
 */
export function ensureDatabaseInitialized(db) {
  try {
    const row = db.isPostgres
      ? db.queryOne("SELECT COUNT(*)::int AS count FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'merchants'")
      : db.queryOne("SELECT count(*) as count FROM sqlite_master WHERE type='table' AND name='merchants'");
    if (!row || Number(row.count) === 0) {
      return runMigrations(db);
    }
    return { alreadyInitialized: true };
  } catch {
    return runMigrations(db);
  }
}

/**
 * Universal Database Client for YOUPI PG.
 * Uses native node:sqlite for high-performance, embedded zero-dependency local operation
 * with full transactional ACID guarantees.
 */
export class Database {
  /**
   * @param {{ dbPath?: string, inMemory?: boolean, autoMigrate?: boolean }} [options={}]
   */
  constructor(options = {}) {
    if (options.inMemory) {
      this.isPostgres = false;
      this.db = new DatabaseSync(':memory:');
    } else if (process.env.DATABASE_URL && !options.dbPath) {
      this.isPostgres = true;
      this.db = null;
      this.pg = new PostgresBridge(process.env.DATABASE_URL);
    } else {
      this.isPostgres = false;
      let dbPath;
      if (options.dbPath) {
        dbPath = path.isAbsolute(options.dbPath)
          ? options.dbPath
          : path.resolve(process.cwd(), options.dbPath);
      } else if (process.env.SQLITE_DB_PATH) {
        dbPath = path.isAbsolute(process.env.SQLITE_DB_PATH)
          ? process.env.SQLITE_DB_PATH
          : path.resolve(repoRoot, process.env.SQLITE_DB_PATH);
      } else {
        dbPath = path.resolve(repoRoot, 'data/youpi_pg.sqlite');
      }
      const dir = path.dirname(dbPath);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
      }
      this.db = new DatabaseSync(dbPath);
      // The file holds webhook secrets, session-token indexes, OTPs and customer PII.
      // Default file creation honours umask, which on many hosts leaves it group/world
      // readable, so restrict it explicitly.
      try { fs.chmodSync(dbPath, 0o600); } catch {}
      try { fs.chmodSync(dir, 0o700); } catch {}
    }

    if (this.isPostgres) {
      if (options.autoMigrate !== false) ensureDatabaseInitialized(this);
      return;
    }

    // Enable foreign keys, busy timeout, and WAL mode for maximum concurrency and integrity
    this.db.exec('PRAGMA foreign_keys = ON;');
    this.db.exec('PRAGMA busy_timeout = 5000;');
    try {
      this.db.exec('PRAGMA journal_mode = WAL;');
    } catch {}

    // Auto-initialize SQLite on first boot if core tables do not exist
    if (!options.inMemory && options.autoMigrate !== false) {
      ensureDatabaseInitialized(this);
    }
  }

  /**
   * Normalizes parameter markers from PostgreSQL style ($1, $2) to SQLite style (?, ?).
   * @param {string} sql
   * @returns {string}
   */
  normalizeSql(sql) {
    if (!this.isPostgres) return sql.replace(/\$\d+/g, '?');

    let normalized = String(sql)
      .replace(/CREATE EXTENSION IF NOT EXISTS\s+"uuid-ossp";?/gi, '')
      .replace(/datetime\('now',\s*'-(\d+) minutes?'\)/gi, "(CURRENT_TIMESTAMP - INTERVAL '$1 minutes')")
      .replace(/datetime\('now'\)/gi, 'CURRENT_TIMESTAMP')
      .replace(/ORDER BY\s+created_at\s+DESC,\s*rowid\s+DESC/gi, 'ORDER BY created_at DESC')
      .replace(/INSERT\s+OR\s+IGNORE/gi, 'INSERT')
      .replace(/\b(is_active|is_merchant_enabled|is_slot_busy|is_phone_verified|is_successful)\s*=\s*1\b/gi, '$1 = TRUE')
      .replace(/\b(is_active|is_merchant_enabled|is_slot_busy|is_phone_verified|is_successful)\s*=\s*0\b/gi, '$1 = FALSE')
      .replace(/\bSET\s+(is_active|is_merchant_enabled|is_slot_busy|is_phone_verified|is_successful)\s*=\s*1\b/gi, 'SET $1 = TRUE')
      .replace(/\bSET\s+(is_active|is_merchant_enabled|is_slot_busy|is_phone_verified|is_successful)\s*=\s*0\b/gi, 'SET $1 = FALSE');

    let index = 0;
    return normalized.replace(/\?/g, () => `$${++index}`);
  }

  /**
   * Executes a statement (INSERT, UPDATE, DELETE, DDL).
   * @param {string} sql
   * @param {any[]} [params=[]]
   * @returns {{ changes: number, lastInsertRowid: number | bigint }}
   */
  execute(sql, params = []) {
    if (this.isPostgres) {
      const normalized = this.normalizeSql(sql).trim();
      if (!normalized) return { changes: 0, lastInsertRowid: 0 };
      return this.pg.request({ op: 'execute', sql: normalized, params });
    }
    const normalized = this.normalizeSql(sql);
    const stmt = this.db.prepare(normalized);
    return stmt.run(...params);
  }

  run(sql, params = []) {
    return this.execute(sql, params);
  }

  /**
   * Queries multiple rows.
   * @param {string} sql
   * @param {any[]} [params=[]]
   * @returns {any[]}
   */
  query(sql, params = []) {
    if (this.isPostgres) {
      const normalized = this.normalizeSql(sql).trim();
      if (!normalized) return [];
      return this.pg.request({ op: 'query', sql: normalized, params });
    }
    const normalized = this.normalizeSql(sql);
    const stmt = this.db.prepare(normalized);
    return stmt.all(...params);
  }

  /**
   * Queries a single row.
   * @param {string} sql
   * @param {any[]} [params=[]]
   * @returns {any | null}
   */
  queryOne(sql, params = []) {
    if (this.isPostgres) return this.query(sql, params)[0] || null;
    const normalized = this.normalizeSql(sql);
    const stmt = this.db.prepare(normalized);
    const res = stmt.get(...params);
    return res !== undefined ? res : null;
  }

  /**
   * Executes a batch of DDL or SQL statements.
   * @param {string} sql
   */
  exec(sql) {
    if (this.isPostgres) {
      const normalized = this.normalizeSql(sql).trim();
      if (normalized) this.pg.request({ op: 'exec', sql: normalized });
      return;
    }
    this.db.exec(sql);
  }

  /**
   * Executes a callback within an ACID database transaction.
   * Automatically rolls back on exception.
   * @template T
   * @param {() => T} callback
   * @returns {T}
   */
  transaction(callback) {
    if (this.isPostgres) {
      this.pg.request({ op: 'begin' });
      try {
        const result = callback();
        this.pg.request({ op: 'commit' });
        return result;
      } catch (err) {
        try { this.pg.request({ op: 'rollback' }); } catch {}
        throw err;
      }
    }
    // IMMEDIATE (not DEFERRED) so the write lock is taken up front. A deferred transaction
    // starts as a read transaction and only upgrades on first write, which lets two
    // concurrent slot-acquisition callers both observe a free slot before either writes.
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      const result = callback();
      this.db.exec('COMMIT;');
      return result;
    } catch (err) {
      try {
        this.db.exec('ROLLBACK;');
      } catch {
        // Ignore rollback error if already rolled back
      }
      throw err;
    }
  }

  /**
   * Closes the database connection.
   */
  close() {
    if (this.isPostgres) this.pg.close();
    else this.db.close();
  }
}

let defaultDbInstance = null;

/**
 * Returns the singleton database instance.
 * @param {{ dbPath?: string, inMemory?: boolean }} [options={}]
 * @returns {Database}
 */
export function getDatabase(options = {}) {
  if (!defaultDbInstance) {
    defaultDbInstance = new Database(options);
    ensureDatabaseInitialized(defaultDbInstance);
  }
  return defaultDbInstance;
}
