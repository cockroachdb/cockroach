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
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestWebSessionsUserIDMigrationNoUsers(t *testing.T) {
	runTestWebSessionsUserIDMigration(t, 0)
}

func TestWebSessionsUserIDMigration10Users(t *testing.T) {
	runTestWebSessionsUserIDMigration(t, 10)
}

func TestWebSessionsUserIDMigration1500Users(t *testing.T) {
	skip.UnderRace(t)
	skip.UnderStress(t)
	runTestWebSessionsUserIDMigration(t, 1500)
}

func runTestWebSessionsUserIDMigration(t *testing.T, numUsers int) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1 /* nodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BootstrapVersionKeyOverride:    clusterversion.V22_2,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.V23_1WebSessionsTableHasUserIDColumn - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()
	tdb := sqlutils.MakeSQLRunner(db)

	// Create test users.
	upgrades.ExecForCountInTxns(ctx, t, db, numUsers, 100 /* txCount */, func(tx *gosql.Tx, i int) error {
		if _, err := tx.Exec(fmt.Sprintf("CREATE USER testuser%d", i)); err != nil {
			return err
		}
		// Simulate the INSERT that happens in the actual authentication code.
		if _, err := tx.Exec(fmt.Sprintf(`
INSERT INTO system.web_sessions ("hashedSecret", username, "createdAt", "expiresAt", "lastUsedAt")
VALUES (
	'\xe77edd369fc3a955a129d2ced69d97717eeee49912e4369a2b687a4d8c36f798',
	'testuser%d',
	'2023-02-14 20:56:30.699447',
	'2023-02-21 20:56:30.699242',
	'2023-02-14 20:56:30.699447'
)
`, i)); err != nil {
			return err
		}
		return nil
	})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.web_sessions", [][]string{{strconv.Itoa(numUsers)}})

	// Drop a user to test that migration deletes orphaned rows.
	if numUsers > 0 {
		tdb.Exec(t, "DROP USER testuser0")
		numUsers -= 1
	}

	// Run migrations.
	_, err := tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1WebSessionsTableHasUserIDColumn).String())
	require.NoError(t, err)
	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1WebSessionsTableUserIDColumnBackfilled).String())
	require.NoError(t, err)

	// Verify that the final schema matches the expected one.
	expectedSchema := `CREATE TABLE public.web_sessions (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	"hashedSecret" BYTES NOT NULL,
	username STRING NOT NULL,
	"createdAt" TIMESTAMP NOT NULL DEFAULT now():::TIMESTAMP,
	"expiresAt" TIMESTAMP NOT NULL,
	"revokedAt" TIMESTAMP NULL,
	"lastUsedAt" TIMESTAMP NOT NULL DEFAULT now():::TIMESTAMP,
	"auditInfo" STRING NULL,
	user_id OID NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (id ASC),
	INDEX "web_sessions_expiresAt_idx" ("expiresAt" ASC),
	INDEX "web_sessions_createdAt_idx" ("createdAt" ASC),
	INDEX "web_sessions_revokedAt_idx" ("revokedAt" ASC),
	INDEX "web_sessions_lastUsedAt_idx" ("lastUsedAt" ASC),
	FAMILY "fam_0_id_hashedSecret_username_createdAt_expiresAt_revokedAt_lastUsedAt_auditInfo" (id, "hashedSecret", username, "createdAt", "expiresAt", "revokedAt", "lastUsedAt", "auditInfo", user_id)
)`
	r := tdb.QueryRow(t, "SELECT create_statement FROM [SHOW CREATE TABLE system.web_sessions]")
	var actualSchema string
	r.Scan(&actualSchema)
	require.Equal(t, expectedSchema, actualSchema)

	// Check that the backfill was successful and correct.
	tdb.CheckQueryResults(t, "SELECT * FROM system.web_sessions WHERE user_id IS NULL", [][]string{})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.web_sessions", [][]string{{strconv.Itoa(numUsers)}})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.web_sessions AS a JOIN system.users AS b ON a.username = b.username AND a.user_id <> b.user_id", [][]string{{"0"}})
}
