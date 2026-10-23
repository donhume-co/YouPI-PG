import { parentPort, workerData } from 'node:worker_threads';
import { Client } from '@neondatabase/serverless';

const client = new Client(workerData.connectionString);
let connected = false;
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

async function connect() {
  if (!connected) {
    await client.connect();
    connected = true;
  }
}

async function handle(message) {
  const { sab, op, sql, params = [] } = message;
  try {
    if (op === 'close') {
      if (connected) await client.end();
      signal(sab, { ok: true, result: null });
      return;
    }

    await connect();
    if (op === 'begin') {
      await client.query('BEGIN');
      transactionOpen = true;
      signal(sab, { ok: true, result: null });
      return;
    }
    if (op === 'commit') {
      await client.query('COMMIT');
      transactionOpen = false;
      signal(sab, { ok: true, result: null });
      return;
    }
    if (op === 'rollback') {
      await client.query('ROLLBACK');
      transactionOpen = false;
      signal(sab, { ok: true, result: null });
      return;
    }

    const result = await client.query({ text: sql, values: params });
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
