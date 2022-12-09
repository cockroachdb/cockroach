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
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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

func TestRoleMembersIDMigrationNoUsers(t *testing.T) {
	runTestRoleMembersIDMigration(t, 0)
}

func TestRoleMembersIDMigration10Users(t *testing.T) {
	runTestRoleMembersIDMigration(t, 10)
}

func TestRoleMembersIDMigration1500Users(t *testing.T) {
	skip.UnderRace(t)
	skip.UnderStress(t)
	runTestRoleMembersIDMigration(t, 1500)
}

func runTestRoleMembersIDMigration(t *testing.T, numUsers int) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.ByKey(clusterversion.V23_1RoleMembersTableHasIDColumns-1),
		false,
	)

	tc := testcluster.StartTestCluster(t, 1 /* nodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.V23_1RoleMembersTableHasIDColumns - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()
	tdb := sqlutils.MakeSQLRunner(db)
	s := tc.Server(0)

	// Delete all rows from the system.role_members table.
	tdb.Exec(t, "DELETE FROM system.role_members WHERE true")
	tdb.CheckQueryResults(t, "SELECT * FROM system.role_members", [][]string{})

	// Inject the descriptor for the system.role_members table from before the
	// ID columns were added.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.RoleMembersTable, getTableDescForSystemRoleMembersTableBeforeIDCols)

	// Insert row that would have been added in a legacy startup migration.
	tdb.Exec(t, `INSERT INTO system.role_members ("role", "member", "isAdmin") VALUES ('admin', 'root', true)`)
	tdb.CheckQueryResults(t, `SELECT * FROM system.role_members`, [][]string{
		{"admin", "root", "true"},
	})

	// Create test users.
	expectedNumRoleMembersRows := 1
	for i := 0; i < numUsers; i++ {
		tdb.Exec(t, fmt.Sprintf("CREATE USER testuser%d", i))
		if i == 0 {
			continue
		}
		// Randomly choose an earlier testuser to grant to the current testuser.
		grantStmt := fmt.Sprintf("GRANT testuser%d to testuser%d", rand.Intn(i), i)
		if rand.Intn(2) == 1 {
			grantStmt += " WITH ADMIN OPTION"
		}
		tdb.Exec(t, grantStmt)
		expectedNumRoleMembersRows += 1
	}
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.role_members", [][]string{
		{fmt.Sprintf("%d", expectedNumRoleMembersRows)},
	})

	// Run migrations.
	_, err := tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1RoleMembersTableHasIDColumns).String())
	require.NoError(t, err)
	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1RoleMembersIDColumnsBackfilled).String())
	require.NoError(t, err)

	// Check some basic conditions on system.role_members after the migration is complete.
	tdb.CheckQueryResults(t, "SELECT * FROM system.role_members WHERE role_id IS NULL OR member_id IS NULL", [][]string{})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.role_members AS a JOIN system.users AS b on a.role = b.username AND a.role_id <> b.user_id", [][]string{{"0"}})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.role_members AS a JOIN system.users AS b on a.member = b.username AND a.member_id <> b.user_id", [][]string{{"0"}})

	tdb.CheckQueryResults(t, `SELECT * FROM system.role_members WHERE "role" = 'admin'`, [][]string{
		{"admin", "root", "true", "2", "1"},
	})

	// Verify that the final schema matches the expected one.
	expectedSchema := `CREATE TABLE public.role_members (
	"role" STRING NOT NULL,
	member STRING NOT NULL,
	"isAdmin" BOOL NOT NULL,
	role_id OID NOT NULL,
	member_id OID NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY ("role" ASC, member ASC),
	INDEX role_members_role_idx ("role" ASC),
	INDEX role_members_member_idx (member ASC),
	INDEX role_members_role_id_idx (role_id ASC),
	INDEX role_members_member_id_idx (member_id ASC),
	UNIQUE INDEX role_members_role_id_member_id_key (role_id ASC, member_id ASC),
	FAMILY "primary" ("role", member),
	FAMILY "fam_3_isAdmin" ("isAdmin"),
	FAMILY fam_4_role_id (role_id),
	FAMILY fam_5_member_id (member_id)
)`
	r := tc.Conns[0].QueryRow("SELECT create_statement FROM [SHOW CREATE TABLE system.role_members]")
	var actualSchema string
	require.NoError(t, r.Scan(&actualSchema))
	require.Equal(t, expectedSchema, actualSchema)
}

func getTableDescForSystemRoleMembersTableBeforeIDCols() *descpb.TableDescriptor {
	return &descpb.TableDescriptor{
		Name:                    "role_members",
		ID:                      keys.RoleMembersTableID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "role", ID: 1, Type: types.String},
			{Name: "member", ID: 2, Type: types.String},
			{Name: "isAdmin", ID: 3, Type: types.Bool},
		},
		NextColumnID: 4,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:        "primary",
				ID:          0,
				ColumnNames: []string{"role", "member"},
				ColumnIDs:   []descpb.ColumnID{1, 2},
			},
			{
				Name:            "fam_3_isAdmin",
				ID:              3,
				ColumnNames:     []string{"isAdmin"},
				ColumnIDs:       []descpb.ColumnID{3},
				DefaultColumnID: 3,
			},
		},
		NextFamilyID: 4,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"role", "member"},
			KeyColumnDirections: []catpb.IndexColumn_Direction{catpb.IndexColumn_ASC, catpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1, 2},
		},
		Indexes: []descpb.IndexDescriptor{
			{
				Name:                "role_members_role_idx",
				ID:                  2,
				Unique:              false,
				KeyColumnNames:      []string{"role"},
				KeyColumnDirections: []catpb.IndexColumn_Direction{catpb.IndexColumn_ASC},
				KeyColumnIDs:        []descpb.ColumnID{1},
				KeySuffixColumnIDs:  []descpb.ColumnID{2},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
			},
			{
				Name:                "role_members_member_idx",
				ID:                  3,
				Unique:              false,
				KeyColumnNames:      []string{"member"},
				KeyColumnDirections: []catpb.IndexColumn_Direction{catpb.IndexColumn_ASC},
				KeyColumnIDs:        []descpb.ColumnID{2},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
			},
		},
		NextIndexID:      4,
		Privileges:       catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, username.NodeUserName()),
		FormatVersion:    descpb.InterleavedFormatVersion,
		NextMutationID:   1,
		NextConstraintID: 1,
	}
}
