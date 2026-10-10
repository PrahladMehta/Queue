// npm run enqueue -- <type> [key=value ...] [--count N] [--max-attempts N]
import { parseArgs } from 'node:util';
import { loadConfig } from '../src/config.ts';
import { createPool } from '../src/db.ts';
import { enqueue, type EnqueueOptions } from '../src/queue.ts';
import type { JsonValue } from '../src/types.ts';

const USAGE = 'usage: npm run enqueue -- <type> [key=value ...] [--count N] [--max-attempts N]';

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    count: { type: 'string', default: '1' },
    'max-attempts': { type: 'string' },
  },
});

const [type, ...pairs] = positionals;
if (type === undefined) {
  console.error(USAGE);
  process.exit(2);
}

const count = Number(values.count);
if (!Number.isInteger(count) || count < 1) {
  console.error(`--count must be a positive integer, got '${values.count}'`);
  process.exit(2);
}

const options: EnqueueOptions = {};
if (values['max-attempts'] !== undefined) {
  const maxAttempts = Number(values['max-attempts']);
  if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
    console.error(`--max-attempts must be a positive integer, got '${values['max-attempts']}'`);
    process.exit(2);
  }
  options.maxAttempts = maxAttempts;
}

const payload: Record<string, JsonValue> = {};
for (const pair of pairs) {
  const eq = pair.indexOf('=');
  if (eq <= 0) {
    console.error(`expected key=value, got '${pair}'\n${USAGE}`);
    process.exit(2);
  }
  const raw = pair.slice(eq + 1);
  payload[pair.slice(0, eq)] = raw !== '' && Number.isFinite(Number(raw)) ? Number(raw) : raw;
}

const pool = createPool(loadConfig().databaseUrl, 1);
try {
  for (let i = 0; i < count; i++) {
    const job = await enqueue(pool, type, payload, options);
    console.log(
      `enqueued job ${job.id} type=${job.type} payload=${JSON.stringify(job.payload)} maxAttempts=${job.maxAttempts}`,
    );
  }
} finally {
  await pool.end();
}
