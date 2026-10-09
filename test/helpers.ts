import type { Pool } from 'pg';
import { requireEnv } from '../src/config.ts';
import { createPool } from '../src/db.ts';
import type { LogFields, Logger } from '../src/logger.ts';
import { migrate } from '../src/migrate.ts';

export function testDatabaseUrl(): string {
  const url = requireEnv(process.env, 'TEST_DATABASE_URL');
  const dbName = decodeURIComponent(new URL(url).pathname.slice(1));
  if (!dbName.endsWith('_test')) {
    throw new Error(`TEST_DATABASE_URL points at '${dbName}'; expected a database ending in _test`);
  }
  return url;
}

export async function setupTestDb(maxConnections = 20): Promise<Pool> {
  const pool = createPool(testDatabaseUrl(), maxConnections);
  await migrate(pool);
  return pool;
}

export async function resetJobs(pool: Pool): Promise<void> {
  await pool.query('TRUNCATE jobs RESTART IDENTITY');
}

export async function statusCounts(pool: Pool): Promise<Record<string, number>> {
  const result = await pool.query<{ status: string; n: number }>(
    'SELECT status, count(*)::int AS n FROM jobs GROUP BY status',
  );
  return Object.fromEntries(result.rows.map((r) => [r.status, r.n]));
}

export interface LogEntry {
  level: 'info' | 'error';
  event: string;
  fields: LogFields;
}

export function createMemoryLogger(): Logger & { entries: LogEntry[] } {
  const entries: LogEntry[] = [];
  return {
    entries,
    info: (event, fields = {}) => entries.push({ level: 'info', event, fields }),
    error: (event, fields = {}) => entries.push({ level: 'error', event, fields }),
  };
}

export async function waitFor(check: () => Promise<boolean>, timeoutMs: number, what: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`timed out after ${timeoutMs}ms waiting for ${what}`);
}
