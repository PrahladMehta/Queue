import assert from 'node:assert/strict';
import { test } from 'node:test';
import { retryDelayMs } from '../src/backoff.ts';

const opts = { baseMs: 1000, maxMs: 60_000 };
const half = (): number => 0.5;

test('doubles each attempt', () => {
  assert.deepEqual(
    [1, 2, 3, 4, 5].map((attempt) => retryDelayMs(attempt, opts, half)),
    [500, 1000, 2000, 4000, 8000],
  );
});

test('caps at maxMs', () => {
  assert.equal(retryDelayMs(7, opts, half), 30_000);
  assert.equal(retryDelayMs(30, opts, half), 30_000);
});

test('huge attempt numbers stay finite', () => {
  const delay = retryDelayMs(5000, opts, half);
  assert.ok(Number.isFinite(delay));
  assert.equal(delay, 30_000);
});

test('jitter stays in [0, ceiling)', () => {
  assert.equal(retryDelayMs(3, opts, () => 0), 0);
  assert.equal(retryDelayMs(3, opts, () => 0.999999), 3999);
  for (let i = 0; i < 1000; i++) {
    const delay = retryDelayMs(3, opts);
    assert.ok(delay >= 0 && delay < 4000, `delay ${delay} out of range`);
    assert.ok(Number.isInteger(delay));
  }
});

test('rejects attempt < 1', () => {
  assert.throws(() => retryDelayMs(0, opts), /positive integer/);
  assert.throws(() => retryDelayMs(1.5, opts), /positive integer/);
});
