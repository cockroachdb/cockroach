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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func runTestRoleMembersMigration(t *testing.T, numUsers int) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.ByKey(clusterversion.RoleMembersTableHasIDColumns-1),
		false,
	)

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.RoleMembersTableHasIDColumns - 1),
				},
			},
		},
	})

	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()
	tdb := sqlutils.MakeSQLRunner(db)
	s := tc.Server(0)

	tdb.Exec(t, `DELETE FROM system.role_members WHERE true`)

	// Inject the old copy of the descriptor.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.UsersTable,
		getDeprecatedSystemMembersOptionsTable)

	// Always create user 0.
	tdb.Exec(t, "CREATE USER testuser0")

	if numUsers > 100 {
		id := uint32(100)
		var wg sync.WaitGroup
		wg.Add(100)
		for i := 0; i < 100; i++ {
			go func(capI int) {
				defer wg.Done()
				for j := 0; j < numUsers/100; j++ {
					val := atomic.AddUint32(&id, 1)
					tdb.Exec(t, fmt.Sprintf(`INSERT INTO system.users VALUES ('testuser%dx%d', '', false, %d)`, capI, j, val))
					if j > 0 {
						tdb.Exec(t, fmt.Sprintf(
							`INSERT INTO system.role_members VALUES ('testuser%dx%d', 'testuser%dx%d', %t)`,
							capI, j, capI, j-1, j%2 == 0),
						)
					}
				}
			}(i)
		}
		wg.Wait()
		tdb.Exec(t, fmt.Sprintf(`SELECT setval('system.role_id_seq', %d)`, id+1))
	} else {
		for i := 1; i < numUsers; i++ {
			tdb.Exec(t, fmt.Sprintf(`CREATE USER testuser%d`, i))
			tdb.Exec(t, fmt.Sprintf(`GRANT testuser%d to testuser%d`, i, i-1))
		}
	}

	var originalCount int
	countRow := tdb.QueryRow(t, `SELECT count(*) FROM system.role_members`)
	countRow.Scan(&originalCount)

	_, err := tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.RoleMembersTableHasIDColumns).String())
	require.NoError(t, err)

	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.RoleMembersIDColumnsAreBackfilled).String())
	require.NoError(t, err)

	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.SetRoleMembersUserIDColumnsNotNull).String())
	require.NoError(t, err)

	tdb.CheckQueryResults(t, `SELECT * FROM system.role_members WHERE role_id IS NULL OR member_id IS NULL`, [][]string{})

	tdb.CheckQueryResults(t, `SELECT count(*) FROM system.role_members a JOIN system.users b ON a.role = b.username AND a.role_id <> b.user_id`, [][]string{{"0"}})
	tdb.CheckQueryResults(t, `SELECT count(*) FROM system.role_members a JOIN system.users b ON a.member = b.username AND a.member_id <> b.user_id`, [][]string{{"0"}})

	tdb.CheckQueryResults(t, `SELECT count(*) FROM system.role_members`, [][]string{
		{fmt.Sprintf("%d", originalCount)},
	})

	tdb.Exec(t, `CREATE USER testuser_last`)
	tdb.Exec(t, `GRANT testuser_last TO testuser0`)

	tdb.CheckQueryResults(t, `SELECT count(*) FROM system.role_members a JOIN system.users b ON a.role = b.username AND a.role_id <> b.user_id`, [][]string{{"0"}})
	tdb.CheckQueryResults(t, `SELECT count(*) FROM system.role_members a JOIN system.users b ON a.member = b.username AND a.member_id <> b.user_id`, [][]string{{"0"}})

	tdb.CheckQueryResults(t, `SELECT count(*) FROM system.role_members`, [][]string{
		{fmt.Sprintf("%d", originalCount+1)},
	})

	// Verify that the schema is correct.
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
	UNIQUE INDEX role_members_role_member_role_id_member_id_key ("role" ASC, member ASC, role_id ASC, member_id ASC),
	FAMILY "primary" ("role", member),
	FAMILY "fam_3_isAdmin" ("isAdmin"),
	FAMILY fam_4_role_id (role_id),
	FAMILY fam_5_member_id (member_id)
)`

	row := tc.Conns[0].QueryRow(`SELECT create_statement FROM [SHOW CREATE TABLE system.role_members]`)
	var actualSchema string
	err = row.Scan(&actualSchema)
	require.NoError(t, err)

	require.Equal(t, expectedSchema, actualSchema)
}

func TestRoleMemberMigration1User(t *testing.T) {
	runTestRoleMembersMigration(t, 1)
}

func TestRoleMemberMigration100User(t *testing.T) {
	runTestRoleMembersMigration(t, 100)
}

func TestRoleMemberMigration15000User(t *testing.T) {
	skip.UnderStress(t)
	runTestRoleMembersMigration(t, 15000)
}

// systemTable is copied over from systemschema/system.go.
func systemTable(
	name catconstants.SystemTableName,
	id descpb.ID,
	columns []descpb.ColumnDescriptor,
	families []descpb.ColumnFamilyDescriptor,
	indexes ...descpb.IndexDescriptor,
) *descpb.TableDescriptor {
	tbl := descpb.TableDescriptor{
		Name:                    string(name),
		ID:                      id,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.SystemPublicSchemaID,
		Version:                 1,
		Columns:                 columns,
		Families:                families,
		PrimaryIndex:            indexes[0],
		Indexes:                 indexes[1:],
		FormatVersion:           descpb.InterleavedFormatVersion,
		NextMutationID:          1,
		NextConstraintID:        1,
	}
	for _, col := range columns {
		if tbl.NextColumnID <= col.ID {
			tbl.NextColumnID = col.ID + 1
		}
	}
	for _, fam := range families {
		if tbl.NextFamilyID <= fam.ID {
			tbl.NextFamilyID = fam.ID + 1
		}
	}
	for i, idx := range indexes {
		if tbl.NextIndexID <= idx.ID {
			tbl.NextIndexID = idx.ID + 1
		}
		// Only assigned constraint IDs to unique non-primary indexes.
		if idx.Unique && i >= 1 {
			tbl.Indexes[i-1].ConstraintID = tbl.NextConstraintID
			tbl.NextConstraintID++
		}
	}

	// When creating tables normally, unique index constraint ids are
	// assigned before the primary index.
	tbl.PrimaryIndex.ConstraintID = tbl.NextConstraintID
	tbl.NextConstraintID++
	return &tbl
}

func getDeprecatedSystemMembersOptionsTable() *descpb.TableDescriptor {
	return systemTable(
		catconstants.RoleMembersTableName,
		keys.RoleMembersTableID,
		[]descpb.ColumnDescriptor{
			{Name: "role", ID: 1, Type: types.String},
			{Name: "member", ID: 2, Type: types.String},
			{Name: "isAdmin", ID: 3, Type: types.Bool},
		},
		[]descpb.ColumnFamilyDescriptor{
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
		descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"role", "member"},
			KeyColumnDirections: []catpb.IndexColumn_Direction{catpb.IndexColumn_ASC, catpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1, 2},
		},
		descpb.IndexDescriptor{
			Name:                "role_members_role_idx",
			ID:                  2,
			Unique:              false,
			KeyColumnNames:      []string{"role"},
			KeyColumnDirections: []catpb.IndexColumn_Direction{catpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1},
			KeySuffixColumnIDs:  []descpb.ColumnID{2},
			Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
		},
		descpb.IndexDescriptor{
			Name:                "role_members_member_idx",
			ID:                  3,
			Unique:              false,
			KeyColumnNames:      []string{"member"},
			KeyColumnDirections: []catpb.IndexColumn_Direction{catpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{2},
			KeySuffixColumnIDs:  []descpb.ColumnID{1},
			Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
		},
	)
}
