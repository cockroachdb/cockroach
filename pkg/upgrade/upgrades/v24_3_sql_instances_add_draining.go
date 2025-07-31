// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// sqlInstancesAddDrainingMigration adds a new column `is_draining` to the
// system.sql_instances table.
func sqlInstancesAddDrainingMigration(
	ctx context.Context, clusterVersion clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	return migrateTable(ctx, clusterVersion, deps, operation{
		name:           "add-draining-column",
		schemaList:     []string{"is_draining"},
		schemaExistsFn: columnExists,
		query:          `ALTER TABLE system.sql_instances ADD COLUMN IF NOT EXISTS is_draining BOOL NULL FAMILY "primary"`,
	},
		keys.SQLInstancesTableID,
		systemschema.SQLInstancesTable())
}
