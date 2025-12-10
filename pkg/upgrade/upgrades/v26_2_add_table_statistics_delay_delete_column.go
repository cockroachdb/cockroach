// Copyright 2025 The Cockroach Authors.
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

const (
	addTableStatisticsDelayDeleteColumn = `
	ALTER TABLE system.table_statistics
	ADD COLUMN IF NOT EXISTS "delayDelete" BOOL DEFAULT FALSE
	FAMILY "fam_0_tableID_statisticID_name_columnIDs_createdAt_rowCount_distinctCount_nullCount_histogram"
`
)

// tableStatisticsDelayDeleteColumnMigration adds the delayDelete column
// for the system.table_statistics table.
func tableStatisticsDelayDeleteColumnMigration(
	ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	op := operation{
		name:           "add-delay-delete-column-to-table-statistics",
		schemaList:     []string{"delayDelete"},
		query:          addTableStatisticsDelayDeleteColumn,
		schemaExistsFn: hasColumn,
	}
	if err := migrateTable(ctx, version, deps, op, keys.TableStatisticsTableID,
		systemschema.TableStatisticsTable); err != nil {
		return err
	}
	return nil
}
