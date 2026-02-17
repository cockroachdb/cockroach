// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cockroachdb

import "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/database"

// GetTasksMigrations returns all migrations for the tasks repository.
func GetTasksMigrations() []database.Migration {
	return []database.Migration{
		{
			Version:     1,
			Description: "Initial tasks table and indexes",
			SQL: `
-- Tasks table for storing task information
CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type VARCHAR(255) NOT NULL,
    state VARCHAR(50) NOT NULL CHECK (state IN ('pending', 'running', 'done', 'failed')),
    consumer_id VARCHAR(255),
    creation_datetime TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_datetime TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_tasks_state_creation ON tasks (state, creation_datetime);
CREATE INDEX IF NOT EXISTS idx_tasks_type ON tasks (type);
CREATE INDEX IF NOT EXISTS idx_tasks_consumer_id ON tasks (consumer_id) WHERE consumer_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_update_datetime ON tasks (update_datetime);

-- Index for cleanup queries (finding stale running tasks)
CREATE INDEX IF NOT EXISTS idx_tasks_stale_cleanup ON tasks (state, update_datetime) WHERE state = 'running';
			`,
		},
		{
			Version:     2,
			Description: "Add payload and error fields to tasks table",
			SQL: `
-- Add payload field for storing task-specific options/arguments as JSON
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS payload JSONB;

-- Add error field for storing error messages when tasks fail
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS error TEXT;

-- Index for querying tasks by payload content (GIN index for JSONB)
CREATE INDEX IF NOT EXISTS idx_tasks_payload ON tasks USING GIN (payload) WHERE payload IS NOT NULL;
			`,
		},
		{
			Version:     3,
			Description: "Optimize indexes for better query performance",
			SQL: `
-- Drop redundant or suboptimal indexes
DROP INDEX IF EXISTS idx_tasks_type;  -- Redundant with new idx_tasks_state_type
DROP INDEX IF EXISTS idx_tasks_update_datetime;  -- Suboptimal for purge queries
DROP INDEX IF EXISTS idx_tasks_stale_cleanup;  -- Replaced by new idx_tasks_state_update

-- Add index for efficient statistics queries that group by state and type
-- Optimizes: SELECT state, type, count(*) FROM tasks GROUP BY state, type
CREATE INDEX IF NOT EXISTS idx_tasks_state_type ON tasks (state, type);

-- Add optimized index for purge queries (covers both general purges and stale cleanup)
-- Optimizes: DELETE FROM tasks WHERE state = $1 AND update_datetime < $2
CREATE INDEX IF NOT EXISTS idx_tasks_state_update ON tasks (state, update_datetime);
			`,
		},
		{
			Version:     4,
			Description: "Add covering indexes for task polling and purge operations",
			SQL: `
-- Covering index for task worker polling queries
-- Optimizes: SELECT * FROM tasks WHERE type = $1 AND state = $2 ORDER BY update_datetime DESC LIMIT $3
-- STORING clause includes all columns workers need, avoiding table lookups
CREATE INDEX IF NOT EXISTS idx_tasks_type_state_covering
    ON tasks(type, state)
    STORING (consumer_id, creation_datetime, update_datetime, payload, error);

-- Covering index for purge/cleanup queries
-- Optimizes: DELETE FROM tasks WHERE state = $1 AND update_datetime < $2
-- STORING clause enables index-only scans for identification before deletion
CREATE INDEX IF NOT EXISTS idx_tasks_state_update_covering
    ON tasks(state, update_datetime)
    STORING (type, consumer_id, creation_datetime, payload);

-- Index for filtering tasks by type and creation time
-- Optimizes: SELECT * FROM tasks WHERE type = $1 AND state != $2 AND creation_datetime > $3 ORDER BY creation_datetime
CREATE INDEX IF NOT EXISTS idx_tasks_type_creation
    ON tasks(type, creation_datetime)
    STORING (state, consumer_id, update_datetime, payload, error);
			`,
		},
		{
			Version:     5,
			Description: "Add reference and concurrency key fields",
			SQL: `
-- Add reference field for linking tasks to external entities (e.g. "provisionings#<uuid>").
-- NULL when the task has no foreign reference.
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS reference VARCHAR(512);

-- Add concurrency_key field for worker lock scoping.
-- NULL means no lock key (task can run concurrently with others).
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS concurrency_key VARCHAR(512);

-- Partial index for efficient lookups by reference (only rows with a reference).
CREATE INDEX IF NOT EXISTS idx_tasks_reference ON tasks (reference) WHERE reference IS NOT NULL;

-- Locking invariant: only one running task per concurrency key at a time.
-- Pending tasks may queue freely for the same key.
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_concurrency_key_running_unique
    ON tasks (concurrency_key)
    WHERE concurrency_key IS NOT NULL AND state = 'running';
				`,
		},
	}
}
