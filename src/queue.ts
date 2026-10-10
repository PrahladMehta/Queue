import type { QueryResult, QueryResultRow } from 'pg';
import { JOB_STATUSES, type Job, type JobStatus, type JsonValue } from './types.ts';

// Pool or a PoolClient inside a transaction
export interface Db {
  query<R extends QueryResultRow>(text: string, values?: unknown[]): Promise<QueryResult<R>>;
}

export class InvalidTransitionError extends Error {
  readonly jobId: string;
  readonly to: JobStatus;
  readonly actual: JobStatus | null;

  constructor(jobId: string, to: JobStatus, actual: JobStatus | null) {
    super(
      actual === null
        ? `cannot move job ${jobId} to '${to}': job does not exist`
        : `cannot move job ${jobId} to '${to}': it is '${actual}', must be 'running'`,
    );
    this.name = 'InvalidTransitionError';
    this.jobId = jobId;
    this.to = to;
    this.actual = actual;
  }
}

interface JobRow {
  id: string;
  type: string;
  payload: JsonValue;
  status: string;
  attempts: number;
  max_attempts: number;
  last_error: string | null;
  run_at: Date;
  created_at: Date;
  started_at: Date | null;
  finished_at: Date | null;
}

function isJobStatus(value: string): value is JobStatus {
  return (JOB_STATUSES as readonly string[]).includes(value);
}

function toJob(row: JobRow): Job {
  if (!isJobStatus(row.status)) {
    throw new Error(`job ${row.id} has unknown status '${row.status}'`);
  }
  return {
    id: row.id,
    type: row.type,
    payload: row.payload,
    status: row.status,
    attempts: row.attempts,
    maxAttempts: row.max_attempts,
    lastError: row.last_error,
    runAt: row.run_at,
    createdAt: row.created_at,
    startedAt: row.started_at,
    finishedAt: row.finished_at,
  };
}

export interface EnqueueOptions {
  maxAttempts?: number;
}

export async function enqueue(db: Db, type: string, payload: JsonValue = {}, options: EnqueueOptions = {}): Promise<Job> {
  if (type.trim() === '') throw new Error('job type must be a non-empty string');
  const { maxAttempts } = options;
  if (maxAttempts !== undefined && (!Number.isInteger(maxAttempts) || maxAttempts < 1)) {
    throw new Error(`maxAttempts must be a positive integer, got ${maxAttempts}`);
  }

  // stringify ourselves: pg sends JS arrays as Postgres arrays, not JSON
  const json = JSON.stringify(payload);
  const result = maxAttempts === undefined
    ? await db.query<JobRow>(
        'INSERT INTO jobs (type, payload) VALUES ($1, $2::jsonb) RETURNING *',
        [type, json],
      )
    : await db.query<JobRow>(
        'INSERT INTO jobs (type, payload, max_attempts) VALUES ($1, $2::jsonb, $3) RETURNING *',
        [type, json, maxAttempts],
      );
  const row = result.rows[0];
  if (row === undefined) throw new Error('enqueue: INSERT returned no row');
  return toJob(row);
}

const CLAIM_SQL = `
  UPDATE jobs
  SET    status = 'running', started_at = now(), attempts = attempts + 1
  WHERE  id = (
    SELECT id
    FROM   jobs
    WHERE  status = 'queued' AND run_at <= now()
    ORDER  BY run_at, id
    LIMIT  1
    FOR UPDATE SKIP LOCKED
  )
  RETURNING *`;

export async function claimNext(db: Db): Promise<Job | null> {
  const result = await db.query<JobRow>(CLAIM_SQL);
  const row = result.rows[0];
  return row === undefined ? null : toJob(row);
}

export function markSucceeded(db: Db, jobId: string): Promise<Job> {
  return finish(db, jobId, 'succeeded', null);
}

export function markFailed(db: Db, jobId: string, error: string): Promise<Job> {
  return finish(db, jobId, 'failed', error);
}

async function finish(db: Db, jobId: string, to: 'succeeded' | 'failed', error: string | null): Promise<Job> {
  const result = await db.query<JobRow>(
    `UPDATE jobs
     SET    status = $2, last_error = $3, finished_at = now()
     WHERE  id = $1 AND status = 'running'
     RETURNING *`,
    [jobId, to, error],
  );
  const row = result.rows[0];
  if (row !== undefined) return toJob(row);

  const current = await getJob(db, jobId);
  throw new InvalidTransitionError(jobId, to, current?.status ?? null);
}

// run_at uses the database clock, so workers with skewed clocks still agree
export async function retryLater(db: Db, jobId: string, error: string, delayMs: number): Promise<Job> {
  if (!Number.isInteger(delayMs) || delayMs < 0) {
    throw new Error(`delayMs must be a non-negative integer, got ${delayMs}`);
  }
  const result = await db.query<JobRow>(
    `UPDATE jobs
     SET    status = 'queued', last_error = $2, run_at = now() + $3::int * interval '1 millisecond'
     WHERE  id = $1 AND status = 'running' AND attempts < max_attempts
     RETURNING *`,
    [jobId, error, delayMs],
  );
  const row = result.rows[0];
  if (row !== undefined) return toJob(row);

  const current = await getJob(db, jobId);
  if (current?.status === 'running') {
    throw new Error(`job ${jobId} has no attempts left (${current.attempts}/${current.maxAttempts})`);
  }
  throw new InvalidTransitionError(jobId, 'queued', current?.status ?? null);
}

export async function getJob(db: Db, jobId: string): Promise<Job | null> {
  const result = await db.query<JobRow>('SELECT * FROM jobs WHERE id = $1', [jobId]);
  const row = result.rows[0];
  return row === undefined ? null : toJob(row);
}
