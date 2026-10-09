import pg from 'pg';
import type { Pool, PoolClient } from 'pg';
import { errorMessage } from './errors.ts';

export function createPool(connectionString: string, max = 10): Pool {
  const pool = new pg.Pool({ connectionString, max });
  // idle clients can emit errors (e.g. server restart); unhandled, they crash the process
  pool.on('error', (err) => {
    console.error(JSON.stringify({ level: 'error', event: 'pool.idle_client_error', error: errorMessage(err) }));
  });
  return pool;
}

export async function withTransaction<T>(pool: Pool, fn: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  let broken = false;
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch {
      broken = true;
    }
    throw err;
  } finally {
    client.release(broken);
  }
}
