import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import type { Pool } from 'pg';
import { withTransaction } from './db.ts';

export const MIGRATIONS_DIR = path.join(import.meta.dirname, '..', 'migrations');

export async function migrate(pool: Pool, dir: string = MIGRATIONS_DIR): Promise<string[]> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      name       TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);

  const files = (await readdir(dir)).filter((f) => f.endsWith('.sql')).sort();
  const done = await pool.query<{ name: string }>('SELECT name FROM schema_migrations');
  const alreadyApplied = new Set(done.rows.map((r) => r.name));

  const applied: string[] = [];
  for (const file of files) {
    if (alreadyApplied.has(file)) continue;
    const sql = await readFile(path.join(dir, file), 'utf8');
    try {
      await withTransaction(pool, async (client) => {
        await client.query(sql);
        await client.query('INSERT INTO schema_migrations (name) VALUES ($1)', [file]);
      });
    } catch (err) {
      throw new Error(`migration ${file} failed`, { cause: err });
    }
    applied.push(file);
  }
  return applied;
}
