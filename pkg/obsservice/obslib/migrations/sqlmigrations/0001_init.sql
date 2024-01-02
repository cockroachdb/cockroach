-- +goose Up
CREATE TABLE cluster_events(
  timestamp     TIMESTAMPTZ NOT NULL,
  org_id        STRING NOT NULL,
  cluster_id    STRING NOT NULL,
  tenant_id     STRING NOT NULL,
  event_id      STRING NOT NULL,
  event_type    STRING NOT NULL,
  event         JSONB,
  CONSTRAINT "primary" PRIMARY KEY (timestamp, event_id) USING HASH WITH (bucket_count = 16)
) WITH (ttl_expire_after = '3 months');

-- +goose Down
DROP TABLE cluster_events;
