// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestStatsTablesAutostatsFunctionMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V25_4)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					ClusterVersionOverride:         clusterversion.MinSupported.Version(),
				},
			},
		},
	}

	var (
		ctx   = context.Background()
		tc    = testcluster.StartTestCluster(t, 1, clusterArgs)
		s     = tc.Server(0)
		sqlDB = tc.ServerConn(0)
	)
	defer tc.Stopper().Stop(ctx)

	// Inject the old copy of the descriptors.
	// Note: At the time of writing, injecting the legacy table is not
	// necessary to make this test pass because the `MinSupportedVersion` is
	// 25.2 and that version doesn't apply the `AutoStatsSettings` anyway.
	// These are put here more to signal intent, but are not strictly
	// required.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.StatementStatisticsTable,
		getOldStatementStatisticsTable)
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.TransactionStatisticsTable,
		getOldTransactionStatisticsTable)

	var createStatmentStats string
	sqlDB.QueryRow("SELECT create_statement FROM [SHOW CREATE TABLE system.statement_statistics]").Scan(&createStatmentStats)
	require.NotContains(t,
		string(createStatmentStats),
		`WITH (sql_stats_automatic_collection_fraction_stale_rows = 4, sql_stats_automatic_partial_collection_fraction_stale_rows = 1);`)

	var createTransactionStats string
	sqlDB.QueryRow("SELECT create_statement FROM [SHOW CREATE TABLE system.transaction_statistics]").Scan(&createTransactionStats)
	require.NotContains(t,
		string(createTransactionStats),
		`WITH (sql_stats_automatic_collection_fraction_stale_rows = 4, sql_stats_automatic_partial_collection_fraction_stale_rows = 1);`)

	// Run the upgrade.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V25_4_SystemStatsTablesAutostatsFraction,
		nil,   /* done */
		false, /* expectError */
	)

	var createStatementAfter string
	sqlDB.QueryRow("SELECT create_statement FROM [SHOW CREATE TABLE system.statement_statistics]").Scan(&createStatementAfter)
	require.Contains(t,
		string(createStatementAfter),
		`WITH (sql_stats_automatic_collection_fraction_stale_rows = 4, sql_stats_automatic_partial_collection_fraction_stale_rows = 1);`)

	var createTransactionStatsAfter string
	sqlDB.QueryRow("SELECT create_statement FROM [SHOW CREATE TABLE system.transaction_statistics]").Scan(&createTransactionStatsAfter)
	require.Contains(t,
		string(createTransactionStatsAfter),
		`WITH (sql_stats_automatic_collection_fraction_stale_rows = 4, sql_stats_automatic_partial_collection_fraction_stale_rows = 1);`)
}

func getOldTransactionStatisticsTable() *descpb.TableDescriptor {
	// Copy the TransactionStatisticsTable definition from system.go but without AutoStatsSettings
	tableDesc := *systemschema.TransactionStatisticsTable.TableDesc()
	// Set version to 1 for injection and remove the AutoStatsSettings that were added in the v25.4 migration
	tableDesc.Version = 1
	tableDesc.AutoStatsSettings = nil
	return &tableDesc
}

func getOldStatementStatisticsTable() *descpb.TableDescriptor {
	// Copy the StatementStatisticsTable definition from system.go but without AutoStatsSettings
	tableDesc := *systemschema.StatementStatisticsTable.TableDesc()
	// Set version to 1 for injection and remove the AutoStatsSettings that were added in the v25.4 migration
	tableDesc.Version = 1
	tableDesc.AutoStatsSettings = nil
	return &tableDesc
}
