// Copyright 2022 The Cockroach Authors.
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

const addPartialStatisticsPredicateAndIDCol = `
ALTER TABLE system.table_statistics
ADD COLUMN IF NOT EXISTS "partialPredicate" STRING
FAMILY "fam_0_tableID_statisticID_name_columnIDs_createdAt_rowCount_distinctCount_nullCount_histogram",
ADD COLUMN IF NOT EXISTS "fullStatisticID" INT8
FAMILY "fam_0_tableID_statisticID_name_columnIDs_createdAt_rowCount_distinctCount_nullCount_histogram"`

func alterSystemTableStatisticsAddPartialPredicateAndID(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	op := operation{
		name:           "add-table-statistics-partialPredicate-col",
		schemaList:     []string{"partialPredicate", "fullStatisticID"},
		query:          addPartialStatisticsPredicateAndIDCol,
		schemaExistsFn: hasColumn,
	}
	if err := migrateTable(ctx, cs, d, op, keys.TableStatisticsTableID, systemschema.TableStatisticsTable); err != nil {
		return err
	}
	return nil
}
