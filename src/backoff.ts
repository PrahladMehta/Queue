export interface BackoffOptions {
  baseMs: number;
  maxMs: number;
}

export const DEFAULT_RETRY: BackoffOptions = { baseMs: 1000, maxMs: 60_000 };

// exponential with a cap, then "full jitter": a random delay in [0, ceiling)
export function retryDelayMs(attempt: number, opts: BackoffOptions, random: () => number = Math.random): number {
  if (!Number.isInteger(attempt) || attempt < 1) {
    throw new Error(`attempt must be a positive integer, got ${attempt}`);
  }
  const ceiling = Math.min(opts.maxMs, opts.baseMs * 2 ** (attempt - 1));
  return Math.floor(random() * ceiling);
}
