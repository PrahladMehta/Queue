import type { BackoffOptions } from './backoff.ts';

export interface Config {
  databaseUrl: string;
  pollIntervalMs: number;
  retry: BackoffOptions;
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
  const retry = {
    baseMs: positiveIntEnv(env, 'RETRY_BASE_MS', 1000),
    maxMs: positiveIntEnv(env, 'RETRY_MAX_MS', 60_000),
  };
  if (retry.maxMs < retry.baseMs) {
    throw new Error(`RETRY_MAX_MS (${retry.maxMs}) must be >= RETRY_BASE_MS (${retry.baseMs})`);
  }
  return {
    databaseUrl: requireEnv(env, 'DATABASE_URL'),
    pollIntervalMs: positiveIntEnv(env, 'POLL_INTERVAL_MS', 1000),
    retry,
  };
}
