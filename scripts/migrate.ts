import { loadConfig } from '../src/config.ts';
import { createPool } from '../src/db.ts';
import { migrate } from '../src/migrate.ts';

const pool = createPool(loadConfig().databaseUrl, 1);
try {
  const applied = await migrate(pool);
  console.log(applied.length > 0 ? `applied: ${applied.join(', ')}` : 'database already up to date');
} finally {
  await pool.end();
}
