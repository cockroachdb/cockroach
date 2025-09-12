// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const (
	updateStatementStatisticsAutostatsFraction = `
ALTER TABLE system.statement_statistics
SET (sql_stats_automatic_collection_fraction_stale_rows = 4, sql_stats_automatic_partial_collection_fraction_stale_rows = 1);
`
	updateTransactionStatisticsAutostatsFraction = `
ALTER TABLE system.transaction_statistics
SET (sql_stats_automatic_collection_fraction_stale_rows = 4, sql_stats_automatic_partial_collection_fraction_stale_rows = 1);
`
)

func systemStatsTablesAutostatsFractionMigration(
	ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// Update statement statistics table
	_, err := deps.InternalExecutor.Exec(
		ctx, "update-statement-statistics-autostats-fraction",
		nil, updateStatementStatisticsAutostatsFraction,
	)
	if err != nil {
		return err
	}

	// Update transaction statistics table
	_, err = deps.InternalExecutor.Exec(
		ctx, "update-transaction-statistics-autostats-fraction",
		nil, updateTransactionStatisticsAutostatsFraction,
	)
	if err != nil {
		return err
	}

	return nil
}
