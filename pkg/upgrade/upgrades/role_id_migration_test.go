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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func runTestRoleIDMigration(t *testing.T, numUsers int) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.ByKey(clusterversion.RoleIDSequence-1),
		false,
	)

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.RoleIDSequence - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()
	tdb := sqlutils.MakeSQLRunner(db)
	s := tc.Server(0)

	// Delete system.role_id_seq.
	tdb.Exec(t, `INSERT INTO system.users VALUES ('node', '', false, 0)`)
	tdb.Exec(t, `GRANT node TO root`)
	tdb.Exec(t, `DROP SEQUENCE system.role_id_seq`)
	tdb.Exec(t, `REVOKE node FROM root`)

	err := tc.Servers[0].DB().Del(ctx, catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, keys.RoleIDSequenceID))
	require.NoError(t, err)
	err = tc.Servers[0].DB().Del(ctx, keys.SystemSQLCodec.SequenceKey(uint32(keys.RoleIDSequenceID)))
	require.NoError(t, err)

	// Remove entries from system.users.
	tdb.Exec(t, `DELETE FROM system.users WHERE username = 'root' OR username ='admin' OR username='node'`)

	tdb.CheckQueryResults(t, `SELECT * FROM system.users`, [][]string{})

	// Inject the old copy of the descriptor.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.UsersTable,
		getDeprecatedSystemUsersTable)

	// Rewrite entries into system.users.
	tdb.Exec(t, `INSERT INTO system.users VALUES ('root', '', false), ('admin', '', true)`)

	tdb.CheckQueryResults(t, `SELECT * FROM system.users`, [][]string{
		{"admin", "", "true"},
		{"root", "", "false"},
	})

	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.RoleIDSequence).String())
	require.NoError(t, err)

	tdb.CheckQueryResults(t, `SELECT * FROM system.role_id_seq`, [][]string{
		{"100", "0", "true"},
	})

	for i := 0; i < numUsers; i++ {
		tdb.Exec(t, fmt.Sprintf(`CREATE USER testuser%d`, i))
	}

	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.SystemUsersUserIDMigration).String())
	require.NoError(t, err)

	tdb.CheckQueryResults(t, `SELECT * FROM system.users WHERE user_id IS NULL`, [][]string{})
	tdb.Exec(t, `CREATE USER testuser_last`)
	tdb.CheckQueryResults(t, `SELECT * FROM system.users WHERE username IN ('admin', 'root', 'testuser0', 'testuser_last')`, [][]string{
		{"admin", "", "true", "2"},
		{"root", "", "false", "1"},
		{"testuser0", "NULL", "false", "101"},
		{"testuser_last", "NULL", "false", fmt.Sprint(101 + numUsers)},
	})
}

func TestRoleIDMigration1User(t *testing.T) {
	runTestRoleIDMigration(t, 1)
}

func TestRoleIDMigration10000Users(t *testing.T) {
	skip.UnderStress(t)
	runTestRoleIDMigration(t, 10000)
}

func getDeprecatedSystemUsersTable() *descpb.TableDescriptor {
	falseBoolString := "false"

	return &descpb.TableDescriptor{
		Name:                    "users",
		ID:                      keys.UsersTableID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "username", ID: 1, Type: types.String},
			{Name: "hashedPassword", ID: 2, Type: types.Bytes, Nullable: true},
			{Name: "isRole", ID: 3, Type: types.Bool, DefaultExpr: &falseBoolString},
		},
		NextColumnID: 4,
		Families: []descpb.ColumnFamilyDescriptor{
			{Name: "primary", ID: 0, ColumnNames: []string{"username"}, ColumnIDs: []descpb.ColumnID{1}},
			{Name: "fam_2_hashedPassword", ID: 2, ColumnNames: []string{"hashedPassword"}, ColumnIDs: []descpb.ColumnID{2}, DefaultColumnID: 2},
			{Name: "fam_3_isRole", ID: 3, ColumnNames: []string{"isRole"}, ColumnIDs: []descpb.ColumnID{3}, DefaultColumnID: 3},
		},
		NextFamilyID: 4,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"username"},
			KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1},
		},
		NextIndexID:      2,
		Privileges:       catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, username.NodeUserName()),
		FormatVersion:    descpb.InterleavedFormatVersion,
		NextMutationID:   1,
		NextConstraintID: 1,
	}
}
