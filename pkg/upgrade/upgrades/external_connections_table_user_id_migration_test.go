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
	_ "github.com/cockroachdb/cockroach/pkg/cloud/userfile"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestExternalConnectionsUserIDMigrationNoUsers(t *testing.T) {
	runTestExternalConnectionsUserIDMigration(t, 0)
}

func TestExternalConnectionsUserIDMigration10Users(t *testing.T) {
	skip.UnderRace(t)
	skip.UnderStress(t)
	runTestExternalConnectionsUserIDMigration(t, 10)
}

func runTestExternalConnectionsUserIDMigration(t *testing.T, numUsers int) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1 /* nodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.V23_1ExternalConnectionsTableHasOwnerIDColumn - 1),
					BootstrapVersionKeyOverride:    clusterversion.V22_2,
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
		if _, err := tx.Exec(fmt.Sprintf("GRANT SYSTEM EXTERNALCONNECTION TO testuser%d", i)); err != nil {
			return err
		}
		return nil
	})
	upgrades.ExecForCountInTxns(ctx, t, db, numUsers, 100 /* txCount */, func(tx *gosql.Tx, i int) error {
		if _, err := tx.Exec(fmt.Sprintf("SET ROLE TO testuser%d", i)); err != nil {
			return err
		}
		externalConnName := fmt.Sprintf("connection%d", i)
		if _, err := tx.Exec(fmt.Sprintf("CREATE EXTERNAL CONNECTION %[1]s AS 'userfile:///%[1]s'", externalConnName)); err != nil {
			return err
		}
		return nil
	})
	tdb.Exec(t, "SET ROLE TO root")
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.external_connections", [][]string{{strconv.Itoa(numUsers)}})

	// Run migrations.
	_, err := tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1ExternalConnectionsTableHasOwnerIDColumn).String())
	require.NoError(t, err)
	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1ExternalConnectionsTableOwnerIDColumnBackfilled).String())
	require.NoError(t, err)

	// Verify that the final schema matches the expected one.
	expectedSchema := `CREATE TABLE public.external_connections (
	connection_name STRING NOT NULL,
	created TIMESTAMP NOT NULL DEFAULT now():::TIMESTAMP,
	updated TIMESTAMP NOT NULL DEFAULT now():::TIMESTAMP,
	connection_type STRING NOT NULL,
	connection_details BYTES NOT NULL,
	owner STRING NOT NULL,
	owner_id OID NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (connection_name ASC)
)`
	r := tdb.QueryRow(t, "SELECT create_statement FROM [SHOW CREATE TABLE system.external_connections]")
	var actualSchema string
	r.Scan(&actualSchema)
	require.Equal(t, expectedSchema, actualSchema)

	// Check that the backfill was successful and correct.
	tdb.CheckQueryResults(t, "SELECT * FROM system.external_connections WHERE owner_id IS NULL", [][]string{})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.external_connections", [][]string{{strconv.Itoa(numUsers)}})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.external_connections JOIN system.users ON owner = username AND owner_id <> user_id", [][]string{{"0"}})
}
