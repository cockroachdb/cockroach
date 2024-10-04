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

const addIndexesUsageCol = `
ALTER TABLE system.statement_statistics
ADD COLUMN IF NOT EXISTS "indexes_usage" JSONB
AS ((statistics->'statistics':::STRING)->'indexes':::STRING) VIRTUAL
`

const addIndexOnIndexesUsageCol = `
CREATE INVERTED INDEX indexes_usage_idx
ON system.statement_statistics (indexes_usage)
`

func createIndexOnIndexUsageOnSystemStatementStatistics(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	for _, op := range []operation{
		{
			name:           "create-indexes-usage-col-statement-statistics",
			schemaList:     []string{"indexes_usage"},
			query:          addIndexesUsageCol,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "create-indexes-usage-idx-statement-statistics",
			schemaList:     []string{"indexes_usage_idx"},
			query:          addIndexOnIndexesUsageCol,
			schemaExistsFn: hasIndex,
		},
	} {
		if err := migrateTable(ctx, cs, d, op, keys.StatementStatisticsTableID, systemschema.StatementStatisticsTable); err != nil {
			return err
		}
	}
	return nil
}
