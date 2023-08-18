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
	gosql "database/sql"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
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

func TestRoleMembersIDMigration1200Users(t *testing.T) {
	skip.UnderRace(t)
	skip.UnderStress(t)
	// Choosing a number larger than 1000 tests that the batching logic in
	// this upgrade works correctly.
	runTestRoleMembersIDMigration(t, 1200)
}

func runTestRoleMembersIDMigration(t *testing.T, numUsers int) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1 /* nodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLEvalContext: &eval.TestingKnobs{
					ForceProductionValues: true,
				},
				Server: &server.TestingKnobs{
					BootstrapVersionKeyOverride:    clusterversion.V22_2,
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

	// Delete all rows from the system.role_members table.
	tdb.Exec(t, "DELETE FROM system.role_members WHERE true")
	tdb.CheckQueryResults(t, "SELECT * FROM system.role_members", [][]string{})

	// Insert row that would have been added in a legacy startup migration.
	tdb.Exec(t, `INSERT INTO system.role_members ("role", "member", "isAdmin") VALUES ('admin', 'root', true)`)
	tdb.CheckQueryResults(t, `SELECT * FROM system.role_members`, [][]string{
		{"admin", "root", "true"},
	})

	// Create test users.
	upgrades.ExecForCountInTxns(ctx, t, db, numUsers, 100 /* txCount */, func(tx *gosql.Tx, i int) error {
		if _, err := tx.Exec(fmt.Sprintf("CREATE USER testuser%d", i)); err != nil {
			return err
		}
		if i == 0 {
			if _, err := tx.Exec(fmt.Sprintf("GRANT admin TO testuser%d", i)); err != nil {
				return err
			}
			return nil
		}
		// Randomly choose an earlier test user to grant to the current test user.
		grantStmt := fmt.Sprintf("GRANT testuser%d TO testuser%d", rand.Intn(i), i)
		if rand.Intn(2) == 1 {
			grantStmt += " WITH ADMIN OPTION"
		}
		if _, err := tx.Exec(grantStmt); err != nil {
			return err
		}
		return nil
	})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.role_members", [][]string{
		{fmt.Sprintf("%d", numUsers+1)},
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

	tdb.CheckQueryResults(t, `SELECT * FROM system.role_members WHERE "role" = 'admin' AND "member" = 'root'`, [][]string{
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
