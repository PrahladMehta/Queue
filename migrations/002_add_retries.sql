ALTER TABLE jobs
  ADD COLUMN run_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  ADD COLUMN max_attempts INTEGER     NOT NULL DEFAULT 5 CHECK (max_attempts >= 1),
  ADD CONSTRAINT jobs_attempts_within_max CHECK (attempts <= max_attempts);

DROP INDEX jobs_queued_idx;
CREATE INDEX jobs_claimable_idx ON jobs (run_at, id) WHERE status = 'queued';
