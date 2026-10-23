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
    // Neon HTTP prepared statements reject multiple commands in one request;
    // use the pg pool's simple-query path for this DDL-only operation instead.
    if (op === 'exec') {
      if (transactionOpen) {
        await transactionClient.query(sql);
      } else if (String(sql).split(';').filter((statement) => statement.trim()).length <= 1) {
        // Keep single-statement DDL on the HTTP path; it is faster and avoids
        // opening a websocket for every compatibility ALTER/CREATE attempt.
        await neonSql.query(sql, params);
      } else {
        // `poolQueryViaFetch` uses Neon HTTP prepared statements, which reject
        // multi-command SQL. Temporarily use the pool's websocket/simple-query
        // path for this migration-only batch, then restore the serverless-safe
        // fetch mode for normal calls.
        const previousPoolMode = neonConfig.poolQueryViaFetch;
        neonConfig.poolQueryViaFetch = false;
        try {
          await pool.query(sql);
        } finally {
          neonConfig.poolQueryViaFetch = previousPoolMode;
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
