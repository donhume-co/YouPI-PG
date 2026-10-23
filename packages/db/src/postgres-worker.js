import { parentPort, workerData } from 'node:worker_threads';
import { neon, Pool, neonConfig } from '@neondatabase/serverless';

// Neon HTTP queries avoid keeping a WebSocket/TCP connection alive in a
// serverless function. Interactive transactions still use a pooled client.
neonConfig.poolQueryViaFetch = true;
const neonSql = neon(workerData.connectionString);
const pool = new Pool({ connectionString: workerData.connectionString, connectionTimeoutMillis: 10000 });
let transactionClient = null;
let transactionOpen = false;

function encode(value) {
  return JSON.stringify(value, (_, item) => typeof item === 'bigint' ? `${item}` : item);
}

function signal(sab, payload) {
  const control = new Int32Array(sab, 0, 2);
  const bytes = new Uint8Array(sab, 8);
  const body = encode(payload);
  const encoded = new TextEncoder().encode(body);
  if (encoded.byteLength > bytes.byteLength) {
    throw new Error('Postgres response exceeded the synchronous bridge buffer.');
  }
  bytes.set(encoded);
  Atomics.store(control, 1, encoded.byteLength);
  Atomics.store(control, 0, 1);
  Atomics.notify(control, 0);
}

function splitSqlStatements(sql) {
  const statements = [];
  let start = 0;
  let quote = null;
  let dollarTag = null;
  for (let i = 0; i < sql.length; i += 1) {
    const ch = sql[i];
    const next = sql[i + 1];
    if (dollarTag) {
      if (sql.startsWith(dollarTag, i)) {
        i += dollarTag.length - 1;
        dollarTag = null;
      }
      continue;
    }
    if (quote) {
      if (ch === quote && next === quote) {
        i += 1;
      } else if (ch === quote && sql[i - 1] !== '\\') {
        quote = null;
      }
      continue;
    }
    if (ch === "'" || ch === '"') {
      quote = ch;
      continue;
    }
    if (ch === '$') {
      const match = sql.slice(i).match(/^\$[A-Za-z_][A-Za-z0-9_]*\$/);
      if (match) {
        dollarTag = match[0];
        i += dollarTag.length - 1;
        continue;
      }
    }
    if (ch === ';') {
      const statement = sql.slice(start, i).trim();
      if (statement) statements.push(statement);
      start = i + 1;
    }
  }
  const tail = sql.slice(start).trim();
  if (tail) statements.push(tail);
  return statements;
}

async function handle(message) {
  const { sab, op, sql, params = [] } = message;
  try {
    if (op === 'close') {
      if (transactionClient) {
        try { await transactionClient.release(); } catch {}
        transactionClient = null;
      }
      await pool.end();
      signal(sab, { ok: true, result: null });
      return;
    }

    if (op === 'begin') {
      transactionClient = await pool.connect();
      await transactionClient.query('BEGIN');
      transactionOpen = true;
      signal(sab, { ok: true, result: null });
      return;
    }
    if (op === 'commit') {
      await transactionClient.query('COMMIT');
      transactionClient.release();
      transactionClient = null;
      transactionOpen = false;
      signal(sab, { ok: true, result: null });
      return;
    }
    if (op === 'rollback') {
      if (transactionClient) await transactionClient.query('ROLLBACK');
      if (transactionClient) transactionClient.release();
      transactionClient = null;
      transactionOpen = false;
      signal(sab, { ok: true, result: null });
      return;
    }

    // Schema migrations and index setup are sent as a batch of SQL statements.
    // Neon HTTP prepared statements reject multiple commands in one request.
    // Execute each statement individually so serverless startup does not depend
    // on opening a websocket just to apply schema DDL.
    if (op === 'exec') {
      const statements = splitSqlStatements(String(sql));
      for (const statement of statements) {
        if (transactionOpen) {
          await transactionClient.query(statement);
        } else {
          await neonSql.query(statement, []);
        }
      }
      signal(sab, { ok: true, result: null });
      return;
    }

    const result = transactionOpen
      ? await transactionClient.query({ text: sql, values: params })
      : { rows: await neonSql.query(sql, params), rowCount: 0 };
    if (op === 'query') {
      signal(sab, { ok: true, result: result.rows });
    } else {
      signal(sab, {
        ok: true,
        result: { changes: result.rowCount || 0, lastInsertRowid: 0 }
      });
    }
  } catch (error) {
    signal(sab, {
      ok: false,
      error: {
        name: error?.name || 'PostgresError',
        message: error?.message || String(error),
        code: error?.code
      }
    });
  }
}

let queue = Promise.resolve();
parentPort.on('message', (message) => {
  queue = queue.then(() => handle(message));
});
