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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestSystemPrivilegesIndexMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.ByKey(clusterversion.V23_1AlterSystemPrivilegesAddIndexOnPathAndUsername-1),
		false,
	)

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.V23_1AlterSystemPrivilegesAddIndexOnPathAndUsername - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()
	tdb := sqlutils.MakeSQLRunner(db)
	s := tc.Server(0)

	// Inject the descriptor for the system.privileges table from before
	// the index on (path,user) column was added.
	upgrades.InjectLegacyTable(
		ctx,
		t,
		s,
		systemschema.SystemPrivilegeTable,
		getTableDescForSystemPrivilegesTableBeforeIndexonPathUsername,
	)

	// Run migration.
	_, err := tc.Conns[0].ExecContext(
		ctx,
		`SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1AlterSystemPrivilegesAddIndexOnPathAndUsername).String(),
	)
	require.NoError(t, err)

	expectedSchema := `CREATE TABLE public.privileges (
	username STRING NOT NULL,
	path STRING NOT NULL,
	privileges STRING[] NOT NULL,
	grant_options STRING[] NOT NULL,
	user_id OID NULL,
	CONSTRAINT "primary" PRIMARY KEY (username ASC, path ASC),
	UNIQUE INDEX privileges_path_username_idx_key (path ASC, username ASC) STORING (privileges, grant_options)
)`
	r := tdb.QueryRow(t, "SELECT create_statement FROM [SHOW CREATE TABLE system.privileges]")
	var actualSchema string
	r.Scan(&actualSchema)
	require.Equal(t, expectedSchema, actualSchema)
}

func getTableDescForSystemPrivilegesTableBeforeIndexonPathUsername() *descpb.TableDescriptor {
	return &descpb.TableDescriptor{
		Name:                    string(catconstants.SystemPrivilegeTableName),
		ID:                      descpb.InvalidID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "username", ID: 1, Type: types.String},
			{Name: "path", ID: 2, Type: types.String},
			{Name: "privileges", ID: 3, Type: types.StringArray},
			{Name: "grant_options", ID: 4, Type: types.StringArray},
			{Name: "user_id", ID: 5, Type: types.Oid, Nullable: true},
		},
		NextColumnID: 6,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:        "primary",
				ID:          0,
				ColumnNames: []string{"username", "path", "privileges", "grant_options", "user_id"},
				ColumnIDs:   []descpb.ColumnID{1, 2, 3, 4, 5},
			},
		},
		NextFamilyID: 1,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"username", "path"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1, 2},
		},
		NextIndexID:      2,
		Privileges:       catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, username.NodeUserName()),
		FormatVersion:    descpb.InterleavedFormatVersion,
		NextMutationID:   1,
		NextConstraintID: 1,
	}
}
