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
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestDatabaseRoleSettingsUserIDMigrationNoUsers(t *testing.T) {
	runTestDatabaseRoleSettingsUserIDMigration(t, 0)
}

func TestDatabaseRoleSettingsUserIDMigration10Users(t *testing.T) {
	runTestDatabaseRoleSettingsUserIDMigration(t, 10)
}

func TestDatabaseRoleSettingsUserIDMigration1500Users(t *testing.T) {
	skip.UnderRace(t)
	skip.UnderStress(t)
	runTestDatabaseRoleSettingsUserIDMigration(t, 1500)
}

func runTestDatabaseRoleSettingsUserIDMigration(t *testing.T, numUsers int) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1 /* nodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BootstrapVersionKeyOverride:    clusterversion.V22_2,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.V23_1DatabaseRoleSettingsHasRoleIDColumn - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()
	tdb := sqlutils.MakeSQLRunner(db)

	// Create test users and add rows for each user to system.database_role_settings.
	upgrades.ExecForCountInTxns(ctx, t, db, numUsers, 100 /* txCount */, func(tx *gosql.Tx, i int) error {
		if _, err := tx.Exec(fmt.Sprintf("CREATE USER testuser%d", i)); err != nil {
			return err
		}
		if _, err := tx.Exec(fmt.Sprintf(`ALTER USER testuser%d SET application_name = 'roach sql'`, i)); err != nil {
			return err
		}
		return nil
	})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.database_role_settings", [][]string{{strconv.Itoa(numUsers)}})

	// Create a row in system.database_role_settings for the empty role.
	tdb.Exec(t, "ALTER ROLE ALL SET timezone = 'America/New_York'")
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.database_role_settings", [][]string{{strconv.Itoa(numUsers + 1)}})

	// Run migrations.
	_, err := tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1DatabaseRoleSettingsHasRoleIDColumn).String())
	require.NoError(t, err)
	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1DatabaseRoleSettingsRoleIDColumnBackfilled).String())
	require.NoError(t, err)

	// Verify that the final schema matches the expected one.
	expectedSchema := `CREATE TABLE public.database_role_settings (
	database_id OID NOT NULL,
	role_name STRING NOT NULL,
	settings STRING[] NOT NULL,
	role_id OID NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (database_id ASC, role_name ASC),
	UNIQUE INDEX database_role_settings_database_id_role_id_key (database_id ASC, role_id ASC) STORING (settings)
)`
	r := tdb.QueryRow(t, "SELECT create_statement FROM [SHOW CREATE TABLE system.database_role_settings]")
	var actualSchema string
	r.Scan(&actualSchema)
	require.Equal(t, expectedSchema, actualSchema)

	// Check that the backfill was successful and correct.
	tdb.CheckQueryResults(t, "SELECT * FROM system.database_role_settings WHERE role_id IS NULL", [][]string{})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.database_role_settings", [][]string{{strconv.Itoa(numUsers + 1)}})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.database_role_settings AS a JOIN system.users AS b ON a.role_name = b.username AND a.role_id <> b.user_id", [][]string{{"0"}})
	tdb.CheckQueryResults(t,
		fmt.Sprintf("SELECT count(*) FROM system.privileges WHERE username = '%s' AND user_id <> %d",
			username.EmptyRole, username.EmptyRoleID),
		[][]string{{"0"}})
}
