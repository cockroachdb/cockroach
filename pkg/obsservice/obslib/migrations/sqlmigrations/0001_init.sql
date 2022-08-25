-- +goose Up
CREATE TABLE sessions(
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  epoch INT NOT NULL,
  expiration TIMESTAMPTZ NOT NULL,
  UNIQUE (id, epoch)
);

-- monitoring_leases stores leases entitling Obs Service nodes to monitor
-- specific CRDB nodes.
CREATE TABLE monitoring_leases(
  -- The ID of the CRDB node or SQL pod being monitored.
  -- There can be at most one lease at a time for a given target_id.
  target_id INT NOT NULL PRIMARY KEY,
  -- The ID of the session that this lease belongs to. The lease is valid if the
  -- session is valid.
  session_id UUID NOT NULL,
  epoch INT NOT NULL,
  CONSTRAINT fk_session FOREIGN KEY (session_id, epoch)
    REFERENCES sessions(id, epoch)
    ON DELETE CASCADE
    ON UPDATE NO ACTION
);

CREATE TABLE cluster_events(
  timestamp TIMESTAMP NOT NULL,
  id BYTES NOT NULL DEFAULT uuid_v4(),
  cluster_id BYTES NOT NULL,
  instance_id INT NOT NULL,
  event_type STRING NOT NULL,
  event JSONB,
  CONSTRAINT "primary" PRIMARY KEY (timestamp, id) USING HASH WITH (bucket_count = 16)
) WITH (ttl_expire_after = '3 months');

-- +goose Down
DROP TABLE cluster_events;
DROP TABLE monitoring_leases;
