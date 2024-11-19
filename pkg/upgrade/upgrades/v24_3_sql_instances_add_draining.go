// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	ctx context.Context, cs clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	for _, op := range []operation{
		{
			name:       "add-is-draining-to-sql-instances",
			schemaList: []string{"is_draining"},
			query: `ALTER TABLE system.sql_instances ` +
				`ADD COLUMN IF NOT EXISTS is_draining BOOL NULL`,
			schemaExistsFn: hasColumn,
		},
	} {
		if err := migrateTable(
			ctx, cs, deps, op, keys.SQLInstancesTableID, systemschema.SQLInstancesTable()); err != nil {
			return err
		}
	}
	return nil
}
