// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cockroachdb

import "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/database"

// GetHealthMigrations returns all migrations for the health repository.
func GetHealthMigrations() []database.Migration {
	return []database.Migration{
		{
			Version:     1,
			Description: "Create instance health table for heartbeat tracking",
			SQL: `
-- Instance health tracking table
-- Health is determined by heartbeat timing, not status column
CREATE TABLE IF NOT EXISTS instance_health (
    instance_id VARCHAR(255) PRIMARY KEY,
    hostname VARCHAR(255) NOT NULL,
	mode INT NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL,
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata JSONB
);

-- Index for efficient health checks and cleanup queries
CREATE INDEX IF NOT EXISTS idx_instance_health_heartbeat ON instance_health(last_heartbeat);
			`,
		},
		{
			Version:     2,
			Description: "Add composite index for atomic health checks in cluster operations",
			SQL: `
-- Critical index for atomic health check queries used in ConditionalEnqueueOperation
-- Enables efficient: WHERE instance_id = X AND last_heartbeat > now() - interval
-- Without this index, health checks do full table scans causing p99 latency spikes
CREATE INDEX IF NOT EXISTS idx_instance_health_id_heartbeat
    ON instance_health(instance_id, last_heartbeat);
			`,
		},
		{
			Version:     3,
			Description: "Add descending index on last_heartbeat for health dashboard queries",
			SQL: `
-- Index for efficient health dashboard queries that order by last_heartbeat DESC
-- Stores commonly accessed columns to avoid table lookups
CREATE INDEX IF NOT EXISTS idx_instance_health_heartbeat_desc
    ON instance_health(last_heartbeat DESC)
    STORING (hostname, mode, started_at, metadata);
			`,
		},
	}
}
