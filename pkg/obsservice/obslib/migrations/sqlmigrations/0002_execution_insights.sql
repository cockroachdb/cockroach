-- +goose Up
CREATE TABLE execution_insights (
    timestamp             TIMESTAMP NOT NULL,
    id                    UUID DEFAULT gen_random_uuid() NOT NULL,
    session_id            STRING NOT NULL,
    txn_id                UUID NOT NULL,
    txn_fingerprint_id    BYTES NOT NULL,
    stmt_id               STRING NOT NULL,
    stmt_fingerprint_id   BYTES NOT NULL,
    problem               STRING NOT NULL,
    causes                STRING[] NOT NULL,
    query                 STRING NOT NULL,
    status                STRING NOT NULL,
    start_time            TIMESTAMP NOT NULL,
    end_time              TIMESTAMP NOT NULL,
    full_scan             BOOL NOT NULL,
    user_name             STRING NOT NULL,
    app_name              STRING NOT NULL,
    database_name         STRING NOT NULL,
    plan_gist             STRING NOT NULL,
    rows_read             INT8 NOT NULL,
    rows_written          INT8 NOT NULL,
    priority              STRING NOT NULL,
    retries               INT8 NOT NULL,
    last_retry_reason     STRING,
    exec_node_ids         INT8[] NOT NULL,
    contention            INTERVAL,
    contention_events     JSONB,
    index_recommendations STRING[] NOT NULL,
    implicit_txn          BOOL NOT NULL,

    CONSTRAINT "primary" PRIMARY KEY (timestamp, id) USING HASH WITH (bucket_count = 16)
);

-- +goose Down
DROP TABLE execution_insights;
