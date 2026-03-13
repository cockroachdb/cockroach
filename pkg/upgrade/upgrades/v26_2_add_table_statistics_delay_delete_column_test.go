// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestTableStatisticsDelayDeleteColumnMigration verifies that the
// V26_2_AddTableStatisticsDelayDeleteColumn migration correctly adds the
// delayDelete column, and that SHOW STATISTICS and the stats cache work
// correctly both before and after the migration. Before the migration,
// GetTableStatisticsStmt uses `false AS "delayDelete"` as a fallback; after
// the migration, it reads the real column.
func TestTableStatisticsDelayDeleteColumnMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V26_2_AddTableStatisticsDelayDeleteColumn)

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

	var (
		validationStmts = []string{
			`SELECT "delayDelete" FROM system.table_statistics LIMIT 0`,
		}
		validationSchemas = []upgrades.Schema{
			{Name: "delayDelete", ValidationFn: upgrades.HasColumn},
			{Name: "fam_0_tableID_statisticID_name_columnIDs_createdAt_rowCount_distinctCount_nullCount_histogram", ValidationFn: upgrades.HasColumnFamily},
		}
	)

	// Inject the old copy of the descriptor.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.TableStatisticsTable,
		getOldTableStatisticsDescriptor)
	validateSchemaExists := func(expectExists bool) {
		upgrades.ValidateSchemaExists(
			ctx,
			t,
			s,
			sqlDB,
			keys.TableStatisticsTableID,
			systemschema.TableStatisticsTable,
			validationStmts,
			validationSchemas,
			expectExists,
		)
	}
	// Validate that the table_statistics table has the old schema.
	validateSchemaExists(false)

	// Create a test table with canary stats enabled (sql_stats_canary_window)
	// and collect statistics before the migration. This exercises the
	// pre-migration code path where GetTableStatisticsStmt uses
	// `false AS "delayDelete"`.
	_, err := sqlDB.Exec(
		`CREATE TABLE t (k INT PRIMARY KEY, v STRING) WITH (sql_stats_canary_window = '15s')`,
	)
	require.NoError(t, err)
	_, err = sqlDB.Exec(
		`INSERT INTO t SELECT i, 'val' || i::STRING FROM generate_series(1, 100) AS g(i)`,
	)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`ANALYZE t`)
	require.NoError(t, err)

	// Verify SHOW STATISTICS works before migration.
	var statsCount int
	err = sqlDB.QueryRow(
		`SELECT count(*) FROM [SHOW STATISTICS FOR TABLE t]`,
	).Scan(&statsCount)
	require.NoError(t, err)
	require.Greater(t, statsCount, 0)

	// Insert more data and collect stats again. Even though the table has
	// canary_window set, the version gate prevents MarkDelayDelete from
	// running, so all stats should remain delay_delete=false.
	_, err = sqlDB.Exec(
		`INSERT INTO t SELECT i, 'more' || i::STRING FROM generate_series(101, 200) AS g(i)`,
	)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`ANALYZE t`)
	require.NoError(t, err)

	var delayDeleteTrueCount int
	err = sqlDB.QueryRow(
		`SELECT count(*) FROM [SHOW STATISTICS FOR TABLE t] WHERE delay_delete = true`,
	).Scan(&delayDeleteTrueCount)
	require.NoError(t, err)
	require.Equal(t, 0, delayDeleteTrueCount,
		"before migration: delay_delete should stay false even with canary_window set")

	// Verify SHOW STATISTICS USING JSON works before migration.
	var jsonStats string
	err = sqlDB.QueryRow(
		`SELECT statistics::STRING FROM [SHOW STATISTICS USING JSON FOR TABLE t]`,
	).Scan(&jsonStats)
	require.NoError(t, err)
	require.NotEmpty(t, jsonStats)

	// Run the upgrade.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V26_2_AddTableStatisticsDelayDeleteColumn,
		nil,   /* done */
		false, /* expectError */
	)
	// Validate that the table has new schema.
	validateSchemaExists(true)

	// Verify SHOW STATISTICS works after migration.
	var statsCountAfter int
	err = sqlDB.QueryRow(
		`SELECT count(*) FROM [SHOW STATISTICS FOR TABLE t]`,
	).Scan(&statsCountAfter)
	require.NoError(t, err)
	require.Greater(t, statsCountAfter, 0)

	// Now with the migration complete, the canary stats path is active.
	// Collecting new stats should mark old ones with delay_delete=true.
	_, err = sqlDB.Exec(
		`INSERT INTO t SELECT i, 'post' || i::STRING FROM generate_series(201, 300) AS g(i)`,
	)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`ANALYZE t`)
	require.NoError(t, err)

	// After migration with canary_window set, old stats should be marked
	// delay_delete=true while the freshest stats remain false.
	err = sqlDB.QueryRow(
		`SELECT count(*) FROM [SHOW STATISTICS FOR TABLE t] WHERE delay_delete = true`,
	).Scan(&delayDeleteTrueCount)
	require.NoError(t, err)
	require.Greater(t, delayDeleteTrueCount, 0,
		"after migration: expected some stats with delay_delete=true when canary_window is set")

	// The freshest stats should still have delay_delete=false.
	var latestDelayDelete bool
	err = sqlDB.QueryRow(
		`SELECT delay_delete FROM [SHOW STATISTICS FOR TABLE t] ORDER BY created DESC LIMIT 1`,
	).Scan(&latestDelayDelete)
	require.NoError(t, err)
	require.False(t, latestDelayDelete,
		"freshest stats should have delay_delete=false")

	// Verify SHOW STATISTICS USING JSON works after migration and includes
	// the delay_delete field.
	var jsonStatsAfter string
	err = sqlDB.QueryRow(
		`SELECT statistics::STRING FROM [SHOW STATISTICS USING JSON FOR TABLE t]`,
	).Scan(&jsonStatsAfter)
	require.NoError(t, err)
	require.NotEmpty(t, jsonStatsAfter)
	require.Contains(t, jsonStatsAfter, "delay_delete",
		"expected JSON stats to contain delay_delete field")
}

// getOldTableStatisticsDescriptor returns the
// system.table_statistics table descriptor that was being used
// before adding the delayDelete column to the current version.
func getOldTableStatisticsDescriptor() *descpb.TableDescriptor {
	uniqueRowIDString := "unique_rowid()"
	nowString := "now()"
	zeroIntString := "0:::INT8"
	return &descpb.TableDescriptor{
		Name:                    string(catconstants.TableStatisticsTableName),
		ID:                      keys.TableStatisticsTableID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "tableID", ID: 1, Type: types.Int},
			{Name: "statisticID", ID: 2, Type: types.Int, DefaultExpr: &uniqueRowIDString},
			{Name: "name", ID: 3, Type: types.String, Nullable: true},
			{Name: "columnIDs", ID: 4, Type: types.IntArray},
			{Name: "createdAt", ID: 5, Type: types.Timestamp, DefaultExpr: &nowString},
			{Name: "rowCount", ID: 6, Type: types.Int},
			{Name: "distinctCount", ID: 7, Type: types.Int},
			{Name: "nullCount", ID: 8, Type: types.Int},
			{Name: "histogram", ID: 9, Type: types.Bytes, Nullable: true},
			{Name: "avgSize", ID: 10, Type: types.Int, DefaultExpr: &zeroIntString},
			{Name: "partialPredicate", ID: 11, Type: types.String, Nullable: true},
			{Name: "fullStatisticID", ID: 12, Type: types.Int, Nullable: true},
			// Note: delayDelete column (ID 13) is intentionally omitted for the old descriptor
		},
		NextColumnID: 13,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name: "fam_0_tableID_statisticID_name_columnIDs_createdAt_rowCount_distinctCount_nullCount_histogram",
				ID:   0,
				ColumnNames: []string{
					"tableID",
					"statisticID",
					"name",
					"columnIDs",
					"createdAt",
					"rowCount",
					"distinctCount",
					"nullCount",
					"histogram",
					"avgSize",
					"partialPredicate",
					"fullStatisticID",
					// Note: delayDelete column is intentionally omitted for the old descriptor
				},
				ColumnIDs: []descpb.ColumnID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
			},
		},
		NextFamilyID: 1,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"tableID", "statisticID"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1, 2},
			ConstraintID:        1,
		},
		NextIndexID:      2,
		Privileges:       catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, username.NodeUserName()),
		NextMutationID:   1,
		FormatVersion:    3,
		NextConstraintID: 2,
	}
}
