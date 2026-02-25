// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cockroachdb

import "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/database"

// GetProvisioningsMigrations returns the database migrations for the
// provisionings repository. The environments table must already exist
// (environments migrations must run first) because provisionings.environment
// has a foreign key to environments(name).
func GetProvisioningsMigrations() []database.Migration {
	return []database.Migration{
		{
			Version:     1,
			Description: "Create provisionings table",
			SQL: `
				CREATE TABLE IF NOT EXISTS provisionings (
					id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
					name VARCHAR(255) NOT NULL,
					environment VARCHAR(63) NOT NULL REFERENCES environments(name) ON DELETE RESTRICT,
					template_type VARCHAR(255) NOT NULL,
					template_checksum VARCHAR(64) NOT NULL DEFAULT '',
					template_snapshot BYTEA,
					state VARCHAR(50) NOT NULL DEFAULT 'new',
					identifier VARCHAR(20) NOT NULL,
					variables JSONB NOT NULL DEFAULT '{}',
					outputs JSONB NOT NULL DEFAULT '{}',
					plan_output JSONB,
					error TEXT DEFAULT '',
					owner VARCHAR(255) NOT NULL,
					cluster_name VARCHAR(255) DEFAULT '',
					lifetime_seconds BIGINT DEFAULT 0,
					created_at TIMESTAMPTZ DEFAULT now(),
					updated_at TIMESTAMPTZ DEFAULT now(),
					expires_at TIMESTAMPTZ,
					last_step VARCHAR(100) DEFAULT '',
					INDEX idx_provisionings_state (state),
					INDEX idx_provisionings_environment (environment),
					INDEX idx_provisionings_owner (owner),
					INDEX idx_provisionings_expires (expires_at) WHERE expires_at IS NOT NULL,
					INDEX idx_provisionings_cluster (cluster_name) WHERE cluster_name != '',
					UNIQUE INDEX idx_provisionings_identifier (identifier)
				);
			`,
		},
	}
}
