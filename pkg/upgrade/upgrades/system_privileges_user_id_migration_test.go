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
	gosql "database/sql"
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestSystemPrivilegesUserIDMigrationNoUsers(t *testing.T) {
	runTestSystemPrivilegesUserIDMigration(t, 0)
}

func TestSystemPrivilegesUserIDMigration10Users(t *testing.T) {
	runTestSystemPrivilegesUserIDMigration(t, 10)
}

func TestSystemPrivilegesUserIDMigration1500Users(t *testing.T) {
	skip.UnderRace(t)
	skip.UnderStress(t)
	runTestSystemPrivilegesUserIDMigration(t, 1500)
}

func runTestSystemPrivilegesUserIDMigration(t *testing.T, numUsers int) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1 /* nodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLEvalContext: &eval.TestingKnobs{
					ForceProductionValues: true,
				},
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BootstrapVersionKeyOverride:    clusterversion.V22_2,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.V23_1SystemPrivilegesTableHasUserIDColumn - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()
	tdb := sqlutils.MakeSQLRunner(db)

	// Create test users and add rows for each user to system.privileges.
	upgrades.ExecForCountInTxns(ctx, t, db, numUsers, 100 /* txCount */, func(tx *gosql.Tx, i int) error {
		if _, err := tx.Exec(fmt.Sprintf("CREATE USER testuser%d", i)); err != nil {
			return err
		}
		if _, err := tx.Exec(fmt.Sprintf("GRANT SYSTEM MODIFYCLUSTERSETTING TO testuser%d", i)); err != nil {
			return err
		}
		return nil
	})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.privileges", [][]string{{strconv.Itoa(numUsers)}})

	// Create a row in system.privileges for the "public" role.
	tdb.Exec(t, "REVOKE SELECT ON crdb_internal.tables FROM public")
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.privileges", [][]string{{strconv.Itoa(numUsers + 1)}})

	// Run migrations.
	_, err := tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1SystemPrivilegesTableHasUserIDColumn).String())
	require.NoError(t, err)
	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1SystemPrivilegesTableUserIDColumnBackfilled).String())
	require.NoError(t, err)

	// Verify that the final schema matches the expected one.
	expectedSchema := `CREATE TABLE public.privileges (
	username STRING NOT NULL,
	path STRING NOT NULL,
	privileges STRING[] NOT NULL,
	grant_options STRING[] NOT NULL,
	user_id OID NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (username ASC, path ASC),
	UNIQUE INDEX privileges_path_user_id_key (path ASC, user_id ASC) STORING (privileges, grant_options)
)`
	r := tdb.QueryRow(t, "SELECT create_statement FROM [SHOW CREATE TABLE system.privileges]")
	var actualSchema string
	r.Scan(&actualSchema)
	require.Equal(t, expectedSchema, actualSchema)

	// Check that the backfill was successful and correct.
	tdb.CheckQueryResults(t, "SELECT * FROM system.privileges WHERE user_id IS NULL", [][]string{})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.privileges", [][]string{{strconv.Itoa(numUsers + 1)}})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.privileges AS a JOIN system.users AS b ON a.username = b.username AND a.user_id <> b.user_id", [][]string{{"0"}})
	tdb.CheckQueryResults(t,
		fmt.Sprintf("SELECT count(*) FROM system.privileges WHERE username = '%s' AND user_id <> %d",
			username.PublicRole, username.PublicRoleID),
		[][]string{{"0"}})
}
