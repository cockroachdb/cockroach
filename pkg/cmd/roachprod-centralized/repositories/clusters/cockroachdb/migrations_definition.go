// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cockroachdb

import "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/database"

// GetClustersMigrations returns the database migrations for the clusters repository.
func GetClustersMigrations() []database.Migration {
	return []database.Migration{
		{
			Version:     1,
			Description: "Create clusters table",
			SQL: `
				CREATE TABLE IF NOT EXISTS clusters (
					name VARCHAR(255) PRIMARY KEY,
					data JSONB NOT NULL,
					created_at TIMESTAMPTZ DEFAULT now(),
					updated_at TIMESTAMPTZ DEFAULT now()
				);
			`,
		},
		{
			Version:     2,
			Description: "Create cluster sync state table",
			SQL: `
				CREATE TABLE IF NOT EXISTS cluster_sync_state (
					id INT PRIMARY KEY DEFAULT 1,
					in_progress BOOLEAN NOT NULL DEFAULT FALSE,
					instance_id VARCHAR(255),
					started_at TIMESTAMPTZ,
					CONSTRAINT single_sync_state CHECK (id = 1)
				);
			`,
		},
		{
			Version:     3,
			Description: "Create cluster operations queue table",
			SQL: `
				CREATE TABLE IF NOT EXISTS cluster_operations (
					id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
					operation_type VARCHAR(50) NOT NULL,
					cluster_name VARCHAR(255) NOT NULL,
					cluster_data JSONB NOT NULL,
					created_at TIMESTAMPTZ DEFAULT now(),
					INDEX idx_cluster_operations_created_at (created_at)
				);
			`,
		},
		{
			Version:     4,
			Description: "Add covering index for cluster operations replay queries",
			SQL: `
-- Covering index for sync operation replay queries
-- Optimizes: SELECT * FROM cluster_operations ORDER BY created_at ASC
-- STORING clause includes all columns needed for operation replay, avoiding table lookups
CREATE INDEX IF NOT EXISTS idx_cluster_operations_created_covering
    ON cluster_operations(created_at)
    STORING (operation_type, cluster_name, cluster_data);
			`,
		},
		{
			Version:     5,
			Description: "Add migration_locks table for distributed migration coordination",
			SQL: `
-- Migration locks table (if not already created by database utils)
CREATE TABLE IF NOT EXISTS migration_locks (
    lock_key STRING PRIMARY KEY,
    generation INT NOT NULL DEFAULT 0,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index for efficient stale lock cleanup queries
-- Optimizes: DELETE FROM migration_locks WHERE lock_key = $1 AND acquired_at < now() - interval
CREATE INDEX IF NOT EXISTS idx_migration_locks_key_acquired
    ON migration_locks(lock_key, acquired_at);
			`,
		},
	}
}
