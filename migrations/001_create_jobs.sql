CREATE TABLE jobs (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  type        TEXT        NOT NULL CHECK (type <> ''),
  payload     JSONB       NOT NULL DEFAULT '{}'::jsonb,
  status      TEXT        NOT NULL DEFAULT 'queued'
              CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
  attempts    INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  last_error  TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at  TIMESTAMPTZ,
  finished_at TIMESTAMPTZ
);

CREATE INDEX jobs_queued_idx ON jobs (id) WHERE status = 'queued';
