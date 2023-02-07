// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestAlterSystemStatementStatisticsTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 95530, "bump minBinary to 22.2. Skip 22.2 mixed-version tests for future cleanup")

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.TODODelete_V22_2AlterSystemStatementStatisticsAddIndexRecommendations - 1),
				},
			},
		},
	}

	var (
		ctx = context.Background()

		tc    = testcluster.StartTestCluster(t, 1, clusterArgs)
		s     = tc.Server(0)
		sqlDB = tc.ServerConn(0)
	)
	defer tc.Stopper().Stop(ctx)

	var (
		validationSchemas = []upgrades.Schema{
			{Name: "index_recommendations", ValidationFn: upgrades.HasColumn},
			{Name: "primary", ValidationFn: upgrades.HasColumnFamily},
		}
	)

	// Inject the old copy of the descriptor.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.StatementStatisticsTable, getDeprecatedStatementStatisticsDescriptor)
	// Validate that the table statement_statistics has the old schema.
	upgrades.ValidateSchemaExists(
		ctx,
		t,
		s,
		sqlDB,
		keys.StatementStatisticsTableID,
		systemschema.StatementStatisticsTable,
		[]string{},
		validationSchemas,
		false, /* expectExists */
	)
	// Run the upgrade.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.TODODelete_V22_2AlterSystemStatementStatisticsAddIndexRecommendations,
		nil,   /* done */
		false, /* expectError */
	)
	// Validate that the table has new schema.
	upgrades.ValidateSchemaExists(
		ctx,
		t,
		s,
		sqlDB,
		keys.StatementStatisticsTableID,
		systemschema.StatementStatisticsTable,
		[]string{},
		validationSchemas,
		true, /* expectExists */
	)
}

// getDeprecatedStatementStatisticsDescriptor returns the system.statement_statistics
// table descriptor that was being used before adding a new column in the
// current version.
func getDeprecatedStatementStatisticsDescriptor() *descpb.TableDescriptor {
	sqlStmtHashComputeExpr := `mod(fnv32(crdb_internal.datums_to_bytes(aggregated_ts, app_name, fingerprint_id, node_id, plan_hash, transaction_fingerprint_id)), 8:::INT8)`

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
		},
		NextColumnID: 12,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name: "primary",
				ID:   0,
				ColumnNames: []string{
					"crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_plan_hash_transaction_fingerprint_id_shard_8",
					"aggregated_ts", "fingerprint_id", "transaction_fingerprint_id", "plan_hash", "app_name", "node_id",
					"agg_interval", "metadata", "statistics", "plan",
				},
				ColumnIDs:       []descpb.ColumnID{11, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
				DefaultColumnID: 0,
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
			Version:      descpb.StrictIndexColumnIDGuaranteesVersion,
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
		},
		NextIndexID:    3,
		Privileges:     catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, username.NodeUserName()),
		NextMutationID: 1,
		FormatVersion:  3,
	}
}
