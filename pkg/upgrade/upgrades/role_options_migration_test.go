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
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func runTestRoleOptionsMigration(t *testing.T, numUsers int) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.ByKey(clusterversion.V22_2RoleOptionsTableHasIDColumn-1),
		false,
	)

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.V22_2RoleOptionsTableHasIDColumn - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()
	tdb := sqlutils.MakeSQLRunner(db)
	s := tc.Server(0)

	// Inject the old copy of the descriptor.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.RoleOptionsTable, getDeprecatedSystemRoleOptionsTable)

	tdb.Exec(t, "CREATE USER testuser")
	tdb.Exec(t, `ALTER ROLE testuser CREATEROLE NOLOGIN VALID UNTIL "2021-01-01" CONTROLJOB`)

	tdb.CheckQueryResults(t, `SELECT * FROM system.role_options`, [][]string{
		{"testuser", "CONTROLJOB", "NULL"},
		{"testuser", "CREATEROLE", "NULL"},
		{"testuser", "NOLOGIN", "NULL"},
		{"testuser", "VALID UNTIL", "2021-01-01 00:00:00+00:00"},
	})

	type roleOption struct {
		option string
		val    string
	}
	roleOptions := make([]roleOption, len(roleoption.ByName))
	i := 0
	for ro := range roleoption.ByName {
		if ro == "VALID UNTIL" {
			roleOptions[i] = roleOption{
				option: ro,
				val:    `2021-01-01`,
			}
		} else if ro == "PASSWORD" {
			continue
		} else {
			roleOptions[i] = roleOption{
				option: ro,
			}
		}
		i++
	}

	if numUsers > 100 {
		var id uint32
		// We're not guaranteed that the ID of the first created
		// user is 100.
		idRow := tdb.QueryRow(t, `SELECT last_value - 1 FROM system.role_id_seq`)
		idRow.Scan(&id)
		var wg sync.WaitGroup
		wg.Add(100)
		// Parallelize user creation.
		for i := 0; i < 100; i++ {
			go func(capI int) {
				defer wg.Done()
				for j := 0; j < numUsers/100; j++ {
					// Rely on a regular CREATE USER to assign ids.
					val := atomic.AddUint32(&id, 1)
					tdb.Exec(t, fmt.Sprintf(`INSERT INTO system.users VALUES ('testuser%dx%d', '', false, %d)`, capI, j, val))
					ro := roleOptions[capI%len(roleOptions)]
					if ro.val != "" {
						tdb.Exec(t, fmt.Sprintf(`INSERT INTO system.role_options VALUES ('testuser%dx%d', '%s', '%s')`, capI, j, ro.option, ro.val))
					} else {
						tdb.Exec(t, fmt.Sprintf(`INSERT INTO system.role_options VALUES ('testuser%dx%d', '%s', '')`, capI, j, ro.option))
					}
				}
			}(i)
		}
		wg.Wait()
		tdb.Exec(t, fmt.Sprintf(`SELECT setval('system.role_id_seq', %d)`, id+1))
	} else {
		for i := 0; i < numUsers; i++ {
			ro := roleOptions[i%len(roleOptions)]
			if ro.val != "" {
				tdb.Exec(t, fmt.Sprintf("CREATE USER testuser%d %s '%s'", i, ro.option, ro.val))
			} else {
				tdb.Exec(t, fmt.Sprintf("CREATE USER testuser%d %s", i, ro.option))

			}
		}
	}

	expectedUsers := numUsers + 3 // root, admin, testuser
	tdb.CheckQueryResults(t, `SELECT count(*) FROM system.users`, [][]string{{fmt.Sprintf(`%d`, expectedUsers)}})

	tdb.Exec(t, "CREATE USER testuser_last")
	tdb.Exec(t, `ALTER ROLE testuser_last CREATEROLE`)

	var testuserID int
	var testuserLastID int
	row := tdb.QueryRow(t, `SELECT user_id FROM system.users WHERE username = 'testuser'`)
	row.Scan(&testuserID)
	row = tdb.QueryRow(t, `SELECT user_id FROM system.users WHERE username = 'testuser_last'`)
	row.Scan(&testuserLastID)

	tdb.CheckQueryResults(t, `SELECT * FROM system.users WHERE username = 'testuser' or username = 'testuser_last'`, [][]string{
		{"testuser", "NULL", "false", fmt.Sprintf("%d", testuserID)},
		{"testuser_last", "NULL", "false", fmt.Sprintf("%d", testuserLastID)},
	})

	tdb.CheckQueryResults(t, `SELECT * FROM system.role_options WHERE username = 'testuser' or username = 'testuser_last'`, [][]string{
		{"testuser", "CONTROLJOB", "NULL"},
		{"testuser", "CREATEROLE", "NULL"},
		{"testuser", "NOLOGIN", "NULL"},
		{"testuser", "VALID UNTIL", "2021-01-01 00:00:00+00:00"},
		{"testuser_last", "CREATEROLE", "NULL"},
	})

	_, err := tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V22_2RoleOptionsTableHasIDColumn).String())
	require.NoError(t, err)

	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V22_2RoleOptionsIDColumnIsBackfilled).String())
	require.NoError(t, err)

	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V22_2SetRoleOptionsUserIDColumnNotNull).String())
	require.NoError(t, err)

	tdb.CheckQueryResults(t, `SELECT * FROM system.role_options WHERE username = 'testuser' or username = 'testuser_last'`, [][]string{
		{"testuser", "CONTROLJOB", "NULL", fmt.Sprintf("%d", testuserID)},
		{"testuser", "CREATEROLE", "NULL", fmt.Sprintf("%d", testuserID)},
		{"testuser", "NOLOGIN", "NULL", fmt.Sprintf("%d", testuserID)},
		{"testuser", "VALID UNTIL", "2021-01-01 00:00:00+00:00", fmt.Sprintf("%d", testuserID)},
		{"testuser_last", "CREATEROLE", "NULL", fmt.Sprintf(`%d`, testuserLastID)},
	})

	tdb.CheckQueryResults(t, `SELECT count(*) FROM system.role_options a JOIN system.users b ON a.username = b.username AND a.user_id <> b.user_id`, [][]string{{"0"}})

	// Verify that the schema is correct.
	expectedSchema := `CREATE TABLE public.role_options (
	username STRING NOT NULL,
	option STRING NOT NULL,
	value STRING NULL,
	user_id OID NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (username ASC, option ASC),
	INDEX users_user_id_idx (user_id ASC)
)`
	r := tc.Conns[0].QueryRow(`SELECT create_statement FROM [SHOW CREATE TABLE system.role_options]`)
	var actualSchema string
	err = r.Scan(&actualSchema)
	require.NoError(t, err)

	require.Equal(t, expectedSchema, actualSchema)
}

func TestRoleOptionsMigration1User(t *testing.T) {
	runTestRoleOptionsMigration(t, 1)
}

func TestRoleOptionsMigration100User(t *testing.T) {
	runTestRoleOptionsMigration(t, 100)
}

func TestRoleOptionsMigration15000User(t *testing.T) {
	skip.UnderStress(t)
	skip.UnderRace(t)
	runTestRoleOptionsMigration(t, 15000)
}

func getDeprecatedSystemRoleOptionsTable() *descpb.TableDescriptor {
	return &descpb.TableDescriptor{
		Name:                    "role_options",
		ID:                      keys.RoleOptionsTableID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "username", ID: 1, Type: types.String},
			{Name: "option", ID: 2, Type: types.String},
			{Name: "value", ID: 3, Type: types.String, Nullable: true},
		},
		NextColumnID: 4,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:            "primary",
				ColumnNames:     []string{"username", "option", "value"},
				ColumnIDs:       []descpb.ColumnID{1, 2, 3},
				DefaultColumnID: 3,
			},
		},
		NextFamilyID: 2,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"username", "option"},
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
