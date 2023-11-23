-- +goose Up
CREATE TABLE cluster_events(
  timestamp     TIMESTAMP NOT NULL,
  org_id        BYTES NOT NULL,
  cluster_id    BYTES NOT NULL,
  tenant_id     INT NOT NULL,
  event_id      BYTES NOT NULL DEFAULT uuid_v4(),
  event_type    STRING NOT NULL,
  event         JSONB,
  CONSTRAINT "primary" PRIMARY KEY (timestamp, event_id) USING HASH WITH (bucket_count = 16)
) WITH (ttl_expire_after = '3 months');

-- +goose Down
DROP TABLE cluster_events;
