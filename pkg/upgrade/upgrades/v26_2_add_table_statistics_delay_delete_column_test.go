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
)

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
	// Validate that the table_statistics table has the old
	// schema.
	validateSchemaExists(false)
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
