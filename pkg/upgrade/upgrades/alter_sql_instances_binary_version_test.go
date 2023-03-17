// Copyright 2023 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestAlterSystemSQLInstancesTableAddBinaryVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.V23_1AlterSystemSQLInstancesAddBinaryVersion - 1),
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
			{Name: "binary_version", ValidationFn: upgrades.HasColumn},
		}
	)

	// Inject the old copy of the descriptor.
	sqlInstancesTable := systemschema.SQLInstancesTable()
	upgrades.InjectLegacyTable(ctx, t, s, sqlInstancesTable, getDeprecatedSqlInstancesDescriptorWithoutBinaryVersion)
	// Validate that the table sql_instances has the old schema.
	upgrades.ValidateSchemaExists(
		ctx,
		t,
		s,
		sqlDB,
		keys.SQLInstancesTableID,
		systemschema.SQLInstancesTable(),
		[]string{},
		validationSchemas,
		false, /* expectExists */
	)
	// Run the upgrade.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V23_1AlterSystemSQLInstancesAddBinaryVersion,
		nil,   /* done */
		false, /* expectError */
	)
	// Validate that the table has new schema.
	upgrades.ValidateSchemaExists(
		ctx,
		t,
		s,
		sqlDB,
		keys.SQLInstancesTableID,
		sqlInstancesTable,
		[]string{},
		validationSchemas,
		true, /* expectExists */
	)
}

// getDeprecatedSqlInstancesDescriptorWithoutBinaryVersion returns the system.sql_instances
// table descriptor that was being used before adding a new column in the
// current version.
func getDeprecatedSqlInstancesDescriptorWithoutBinaryVersion() *descpb.TableDescriptor {
	return &descpb.TableDescriptor{
		Name:                    string(catconstants.SQLInstancesTableName),
		ID:                      keys.SQLInstancesTableID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Int, Nullable: false},
			{Name: "addr", ID: 2, Type: types.String, Nullable: true},
			{Name: "session_id", ID: 3, Type: types.Bytes, Nullable: true},
			{Name: "locality", ID: 4, Type: types.Jsonb, Nullable: true},
			{Name: "sql_addr", ID: 5, Type: types.String, Nullable: true},
			{Name: "crdb_region", ID: 6, Type: types.Bytes, Nullable: false},
		},
		NextColumnID: 7,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:            "primary",
				ID:              0,
				ColumnNames:     []string{"id", "addr", "session_id", "locality", "sql_addr", "crdb_region"},
				ColumnIDs:       []descpb.ColumnID{1, 2, 3, 4, 5, 6},
				DefaultColumnID: 0,
			},
		},

		NextFamilyID: 1,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  2,
			Unique:              true,
			KeyColumnNames:      []string{"crdb_region", "id"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{6, 1},
		},
		NextIndexID:    3,
		Privileges:     catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, username.NodeUserName()),
		NextMutationID: 1,
		FormatVersion:  3,
	}
}
