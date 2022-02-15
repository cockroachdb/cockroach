// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/migration/migrations"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestAlterSystemTableStatisticsTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.AlterSystemTableStatisticsAddAvgSizeCol - 1),
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
		validationSchemas = []migrations.Schema{
			{Name: "avgSize", ValidationFn: migrations.HasColumn},
		}
	)

	// Inject the old copy of the descriptor.
	migrations.InjectLegacyTable(ctx, t, s, systemschema.TableStatisticsTable, getDeprecatedTableStatisticsDescriptor)
	// Validate that the table statistics table has the old schema.
	migrations.ValidateSchemaExists(
		ctx,
		t,
		s,
		sqlDB,
		keys.TableStatisticsTableID,
		systemschema.TableStatisticsTable,
		[]string{},
		validationSchemas,
		false, /* expectExists */
	)
	// Run the migration.
	migrations.Migrate(
		t,
		sqlDB,
		clusterversion.AlterSystemTableStatisticsAddAvgSizeCol,
		nil,   /* done */
		false, /* expectError */
	)
	// Validate that the table has new schema.
	migrations.ValidateSchemaExists(
		ctx,
		t,
		s,
		sqlDB,
		keys.TableStatisticsTableID,
		systemschema.TableStatisticsTable,
		[]string{},
		validationSchemas,
		true, /* expectExists */
	)
}

// getDeprecatedTableStatisticsDescriptor returns the system.table_statistics
// table descriptor that was being used before adding a new column in the
// current version.
func getDeprecatedTableStatisticsDescriptor() *descpb.TableDescriptor {
	uniqueRowIDString := "unique_rowid()"
	nowString := "now():::TIMESTAMP"

	return &descpb.TableDescriptor{
		Name:                    "table_statistics",
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
		},
		NextColumnID: 10,
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
				},
				ColumnIDs: []descpb.ColumnID{1, 2, 3, 4, 5, 6, 7, 8, 9},
			},
		},
		NextFamilyID: 1,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"tableID", "statisticID"},
			KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1, 2},
		},
		NextIndexID:    2,
		Privileges:     catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, security.NodeUserName()),
		NextMutationID: 1,
		FormatVersion:  3,
	}
}
