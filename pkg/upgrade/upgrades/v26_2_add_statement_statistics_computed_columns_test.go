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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestStatementStatisticsComputedColumnsMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V26_2_AddStatementStatisticsComputedColumns)

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
			`SELECT exec_sample_count FROM system.statement_statistics LIMIT 0`,
			`SELECT svc_lat_sum FROM system.statement_statistics LIMIT 0`,
			`SELECT cpu_sql_nanos_sum FROM system.statement_statistics LIMIT 0`,
			`SELECT contention_time_sum FROM system.statement_statistics LIMIT 0`,
			`SELECT svc_lat_sum_sq FROM system.statement_statistics LIMIT 0`,
			`SELECT cpu_sql_nanos_sum_sq FROM system.statement_statistics LIMIT 0`,
			`SELECT contention_time_sum_sq FROM system.statement_statistics LIMIT 0`,
			`SELECT kv_cpu_time_nanos FROM system.statement_statistics LIMIT 0`,
			`SELECT kv_cpu_time_nanos_sum FROM system.statement_statistics LIMIT 0`,
			`SELECT kv_cpu_time_nanos_sum_sq FROM system.statement_statistics LIMIT 0`,
			`SELECT admission_wait_time_sum FROM system.statement_statistics LIMIT 0`,
			`SELECT admission_wait_time_sum_sq FROM system.statement_statistics LIMIT 0`,
			`SELECT rows_read_sum FROM system.statement_statistics LIMIT 0`,
			`SELECT rows_written_sum FROM system.statement_statistics LIMIT 0`,
			`SELECT bytes_read_sum FROM system.statement_statistics LIMIT 0`,
			`SELECT bytes_read_sum_sq FROM system.statement_statistics LIMIT 0`,
		}
		validationSchemas = []upgrades.Schema{
			// Use ColumnExists instead of HasColumn because computed expressions
			// may be normalized differently by the SQL parser.
			{Name: "exec_sample_count", ValidationFn: upgrades.ColumnExists},
			{Name: "svc_lat_sum", ValidationFn: upgrades.ColumnExists},
			{Name: "cpu_sql_nanos_sum", ValidationFn: upgrades.ColumnExists},
			{Name: "contention_time_sum", ValidationFn: upgrades.ColumnExists},
			{Name: "svc_lat_sum_sq", ValidationFn: upgrades.ColumnExists},
			{Name: "cpu_sql_nanos_sum_sq", ValidationFn: upgrades.ColumnExists},
			{Name: "contention_time_sum_sq", ValidationFn: upgrades.ColumnExists},
			{Name: "kv_cpu_time_nanos", ValidationFn: upgrades.ColumnExists},
			{Name: "kv_cpu_time_nanos_sum", ValidationFn: upgrades.ColumnExists},
			{Name: "kv_cpu_time_nanos_sum_sq", ValidationFn: upgrades.ColumnExists},
			{Name: "admission_wait_time_sum", ValidationFn: upgrades.ColumnExists},
			{Name: "admission_wait_time_sum_sq", ValidationFn: upgrades.ColumnExists},
			{Name: "rows_read_sum", ValidationFn: upgrades.ColumnExists},
			{Name: "rows_written_sum", ValidationFn: upgrades.ColumnExists},
			{Name: "bytes_read_sum", ValidationFn: upgrades.ColumnExists},
			{Name: "bytes_read_sum_sq", ValidationFn: upgrades.ColumnExists},
			{Name: "stmt_fp_ts_cov_counts", ValidationFn: upgrades.IndexExists},
		}
	)

	// Inject the old copy of the descriptor.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.StatementStatisticsTable,
		getOldStatementStatisticsDescriptor)
	validateSchemaExists := func(expectExists bool) {
		upgrades.ValidateSchemaExists(
			ctx,
			t,
			s,
			sqlDB,
			keys.StatementStatisticsTableID,
			systemschema.StatementStatisticsTable,
			validationStmts,
			validationSchemas,
			expectExists,
		)
	}
	// Validate that the statement_statistics table has the old schema.
	validateSchemaExists(false)
	// Run the upgrade.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V26_2_AddStatementStatisticsComputedColumns,
		nil,   /* done */
		false, /* expectError */
	)
	// Validate that the table has new schema.
	validateSchemaExists(true)
}

// getOldStatementStatisticsDescriptor returns the system.statement_statistics
// table descriptor that was being used before adding the new computed columns
// and covering index.
func getOldStatementStatisticsDescriptor() *descpb.TableDescriptor {
	defaultIndexRec := "ARRAY[]:::STRING[]"
	sqlStmtHashComputeExpr := "mod(fnv32(crdb_internal.datums_to_bytes(aggregated_ts, app_name, fingerprint_id, node_id, plan_hash, transaction_fingerprint_id)), 8:::INT8)"
	indexUsageComputeExpr := "(statistics->'statistics':::STRING)->'indexes':::STRING"
	executionCountComputeExpr := "((statistics->'statistics':::STRING)->'cnt':::STRING)::INT8"
	serviceLatencyComputeExpr := "(((statistics->'statistics':::STRING)->'svcLat':::STRING)->'mean':::STRING)::FLOAT8"
	cpuSqlNanosComputeExpr := "(((statistics->'execution_statistics':::STRING)->'cpuSQLNanos':::STRING)->'mean':::STRING)::FLOAT8"
	contentionTimeComputeExpr := "(((statistics->'execution_statistics':::STRING)->'contentionTime':::STRING)->'mean':::STRING)::FLOAT8"
	totalEstimatedExecutionTimeExpr := "((statistics->'statistics':::STRING)->>'cnt':::STRING)::FLOAT8 * (((statistics->'statistics':::STRING)->'svcLat':::STRING)->>'mean':::STRING)::FLOAT8"
	p99LatencyComputeExpr := "(((statistics->'statistics':::STRING)->'latencyInfo':::STRING)->'p99':::STRING)::FLOAT8"

	return &descpb.TableDescriptor{
		Name:                    string(catconstants.StatementStatisticsTableName),
		ID:                      keys.StatementStatisticsTableID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "aggregated_ts", ID: 1, Type: types.TimestampTZ, Nullable: false},
			{Name: "fingerprint_id", ID: 2, Type: types.Bytes, Nullable: false},
			{Name: "transaction_fingerprint_id", ID: 3, Type: types.Bytes, Nullable: false},
			{Name: "plan_hash", ID: 4, Type: types.Bytes, Nullable: false},
			{Name: "app_name", ID: 5, Type: types.String, Nullable: false},
			{Name: "node_id", ID: 6, Type: types.Int, Nullable: false},
			{Name: "agg_interval", ID: 7, Type: types.Interval, Nullable: false},
			{Name: "metadata", ID: 8, Type: types.Jsonb, Nullable: false},
			{Name: "statistics", ID: 9, Type: types.Jsonb, Nullable: false},
			{Name: "plan", ID: 10, Type: types.Jsonb, Nullable: false},
			{
				Name:        "crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_plan_hash_transaction_fingerprint_id_shard_8",
				ID:          11,
				Type:        types.Int4,
				Nullable:    false,
				ComputeExpr: &sqlStmtHashComputeExpr,
				Hidden:      true,
			},
			{Name: "index_recommendations", ID: 12, Type: types.StringArray, Nullable: false, DefaultExpr: &defaultIndexRec},
			{Name: "indexes_usage", ID: 13, Type: types.Jsonb, Nullable: true, Virtual: true, ComputeExpr: &indexUsageComputeExpr},
			{Name: "execution_count", ID: 14, Type: types.Int, Nullable: true, ComputeExpr: &executionCountComputeExpr},
			{Name: "service_latency", ID: 15, Type: types.Float, Nullable: true, ComputeExpr: &serviceLatencyComputeExpr},
			{Name: "cpu_sql_nanos", ID: 16, Type: types.Float, Nullable: true, ComputeExpr: &cpuSqlNanosComputeExpr},
			{Name: "contention_time", ID: 17, Type: types.Float, Nullable: true, ComputeExpr: &contentionTimeComputeExpr},
			{Name: "total_estimated_execution_time", ID: 18, Type: types.Float, Nullable: true, ComputeExpr: &totalEstimatedExecutionTimeExpr},
			{Name: "p99_latency", ID: 19, Type: types.Float, Nullable: true, ComputeExpr: &p99LatencyComputeExpr},
			// Note: columns 20-35 are intentionally omitted for the old descriptor
		},
		NextColumnID: 20,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name: "primary",
				ID:   0,
				ColumnNames: []string{
					"crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_plan_hash_transaction_fingerprint_id_shard_8",
					"aggregated_ts", "fingerprint_id", "transaction_fingerprint_id", "plan_hash", "app_name", "node_id",
					"agg_interval", "metadata", "statistics", "plan", "index_recommendations", "execution_count",
					"service_latency", "cpu_sql_nanos", "contention_time", "total_estimated_execution_time",
					"p99_latency",
					// Note: new columns are intentionally omitted
				},
				ColumnIDs: []descpb.ColumnID{11, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 15, 16, 17, 18, 19},
			},
		},
		NextFamilyID: 1,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:   tabledesc.LegacyPrimaryKeyIndexName,
			ID:     1,
			Unique: true,
			KeyColumnNames: []string{
				"crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_plan_hash_transaction_fingerprint_id_shard_8",
				"aggregated_ts",
				"fingerprint_id",
				"transaction_fingerprint_id",
				"plan_hash",
				"app_name",
				"node_id",
			},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_ASC,
				catenumpb.IndexColumn_ASC,
				catenumpb.IndexColumn_ASC,
				catenumpb.IndexColumn_ASC,
				catenumpb.IndexColumn_ASC,
				catenumpb.IndexColumn_ASC,
				catenumpb.IndexColumn_ASC,
			},
			KeyColumnIDs: []descpb.ColumnID{11, 1, 2, 3, 4, 5, 6},
			Sharded: catpb.ShardedDescriptor{
				IsSharded:    true,
				Name:         "crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_plan_hash_transaction_fingerprint_id_shard_8",
				ShardBuckets: 8,
				ColumnNames: []string{
					"aggregated_ts",
					"app_name",
					"fingerprint_id",
					"node_id",
					"plan_hash",
					"transaction_fingerprint_id",
				},
			},
			ConstraintID: 1,
		},
		Indexes: []descpb.IndexDescriptor{
			{
				Name:   "fingerprint_stats_idx",
				ID:     2,
				Unique: false,
				KeyColumnNames: []string{
					"fingerprint_id",
					"transaction_fingerprint_id",
				},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{
					catenumpb.IndexColumn_ASC,
					catenumpb.IndexColumn_ASC,
				},
				KeyColumnIDs:       []descpb.ColumnID{2, 3},
				KeySuffixColumnIDs: []descpb.ColumnID{11, 1, 4, 5, 6},
			},
			{
				Name:   "indexes_usage_idx",
				ID:     3,
				Unique: false,
				KeyColumnNames: []string{
					"indexes_usage",
				},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{
					catenumpb.IndexColumn_ASC,
				},
				KeyColumnIDs:       []descpb.ColumnID{13},
				KeySuffixColumnIDs: []descpb.ColumnID{11, 1, 2, 3, 4, 5, 6},
			},
			// Note: stmt_fp_ts_cov_counts index is intentionally omitted
		},
		NextIndexID:      4,
		Privileges:       catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadData, username.NodeUserName()),
		NextMutationID:   1,
		FormatVersion:    3,
		NextConstraintID: 3,
		Checks: []*descpb.TableDescriptor_CheckConstraint{{
			Expr:                  "crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_plan_hash_transaction_fingerprint_id_shard_8 IN (0:::INT8, 1:::INT8, 2:::INT8, 3:::INT8, 4:::INT8, 5:::INT8, 6:::INT8, 7:::INT8)",
			Name:                  "check_crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_plan_hash_transaction_fingerprint_id_shard_8",
			Validity:              descpb.ConstraintValidity_Validated,
			ColumnIDs:             []descpb.ColumnID{11},
			FromHashShardedColumn: true,
			ConstraintID:          2,
		}},
		AutoStatsSettings: &catpb.AutoStatsSettings{
			FractionStaleRows:        &[]float64{4.0}[0],
			PartialFractionStaleRows: &[]float64{1.0}[0],
		},
	}
}
