// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cockroachdb

import "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/database"

// GetEnvironmentsMigrations returns the database migrations for the
// environments repository.
func GetEnvironmentsMigrations() []database.Migration {
	return []database.Migration{
		{
			Version:     1,
			Description: "Create environments and environment_variables tables",
			SQL: `
				CREATE TABLE IF NOT EXISTS environments (
					name VARCHAR(63) PRIMARY KEY,
					description TEXT DEFAULT '',
					owner VARCHAR(255) NOT NULL DEFAULT '',
					created_at TIMESTAMPTZ DEFAULT now(),
					updated_at TIMESTAMPTZ DEFAULT now()
				);
				CREATE TABLE IF NOT EXISTS environment_variables (
					environment_name VARCHAR(63) NOT NULL REFERENCES environments(name) ON DELETE CASCADE,
					key VARCHAR(190) NOT NULL,
					value TEXT NOT NULL DEFAULT '',
					type VARCHAR(50) NOT NULL DEFAULT 'plaintext',
					created_at TIMESTAMPTZ DEFAULT now(),
					updated_at TIMESTAMPTZ DEFAULT now(),
					PRIMARY KEY (environment_name, key)
				);
			`,
		},
	}
}
