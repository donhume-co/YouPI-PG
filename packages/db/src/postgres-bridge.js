import { Worker } from 'node:worker_threads';

const RESPONSE_BYTES = 4 * 1024 * 1024;

/**
 * Synchronous compatibility bridge for the existing repository API.
 * The application was intentionally written around synchronous node:sqlite calls.
 * A worker owns the async Neon connection while callers retain the same synchronous
 * query/transaction contract, so payment routes do not need a risky broad rewrite.
 */
export class PostgresBridge {
  constructor(connectionString) {
    this.worker = new Worker(new URL('./postgres-worker.js', import.meta.url), {
      type: 'module',
      workerData: { connectionString },
      execArgv: []
    });
    this.worker.on('error', (error) => {
      this.workerError = error;
    });
    this.closed = false;
  }

  request(payload) {
    if (this.closed) throw new Error('Postgres bridge is closed.');
    const sab = new SharedArrayBuffer(8 + RESPONSE_BYTES);
    const control = new Int32Array(sab, 0, 2);
    const bytes = new Uint8Array(sab, 8);
    this.worker.postMessage({ ...payload, sab });
    const waitResult = Atomics.wait(control, 0, 0, 30000);
    if (waitResult === 'timed-out') {
      throw this.workerError || new Error('Timed out waiting for Neon Postgres response.');
    }
    const length = Atomics.load(control, 1);
    const body = new TextDecoder().decode(bytes.subarray(0, length));
    const response = JSON.parse(body);
    if (!response.ok) {
      const error = new Error(response.error?.message || 'Postgres query failed.');
      error.name = response.error?.name || 'PostgresError';
      error.code = response.error?.code;
      throw error;
    }
    return response.result;
  }

  close() {
    if (this.closed) return;
    try {
      this.request({ op: 'close' });
    } finally {
      this.closed = true;
      void this.worker.terminate();
    }
  }
}
