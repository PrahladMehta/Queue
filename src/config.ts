export interface Config {
  databaseUrl: string;
  pollIntervalMs: number;
}

export function requireEnv(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name];
  if (value === undefined || value.trim() === '') {
    throw new Error(`Missing environment variable ${name}`);
  }
  return value;
}

function positiveIntEnv(env: NodeJS.ProcessEnv, name: string, fallback: number): number {
  const raw = env[name];
  if (raw === undefined || raw.trim() === '') return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer, got '${raw}'`);
  }
  return value;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  return {
    databaseUrl: requireEnv(env, 'DATABASE_URL'),
    pollIntervalMs: positiveIntEnv(env, 'POLL_INTERVAL_MS', 1000),
  };
}
