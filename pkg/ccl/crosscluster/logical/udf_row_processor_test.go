// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//	https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package logical

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestUDFWithRandomTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, sqlA, sqlB, cleanup := setupTwoDBUDFTestCluster(t)
	defer cleanup()

	runnerA := sqlutils.MakeSQLRunner(sqlA)
	runnerB := sqlutils.MakeSQLRunner(sqlB)

	tableName := "rand_table"
	rng, _ := randutil.NewPseudoRand()
	createStmt := randgen.RandCreateTableWithName(
		ctx,
		rng,
		tableName,
		1,
		false, /* isMultiregion */
		// We do not have full support for column families.
		randgen.SkipColumnFamilyMutation(),
		randgen.RequirePrimaryIndex(),
	)
	stmt := tree.SerializeForDisplay(createStmt)
	t.Logf(stmt)
	runnerA.Exec(t, stmt)
	runnerB.Exec(t, stmt)
	runnerB.Exec(t, applierTypes)
	runnerB.Exec(t, `
		CREATE OR REPLACE FUNCTION repl_apply(action STRING, data rand_table, existing rand_table)
		RETURNS crdb_replication_applier_decision
		AS $$
		BEGIN
		RETURN ('accept_proposed', NULL);
		END;
		$$ LANGUAGE plpgsql
		`)

	numInserts := 20
	_, err := randgen.PopulateTableWithRandData(rng,
		sqlA, tableName, numInserts, nil)
	require.NoError(t, err)

	addCol := fmt.Sprintf(`ALTER TABLE %s `+lwwColumnAdd, tableName)
	runnerA.Exec(t, addCol)
	runnerB.Exec(t, addCol)

	dbAURL, cleanup := s.PGUrl(t, serverutils.DBName("a"))
	defer cleanup()

	streamStartStmt := fmt.Sprintf("CREATE LOGICAL REPLICATION STREAM FROM TABLE %[1]s ON $1 INTO TABLE %[1]s", tableName)
	var jobBID jobspb.JobID
	runnerB.QueryRow(t, streamStartStmt, dbAURL.String()).Scan(&jobBID)

	t.Logf("waiting for replication job %d", jobBID)
	WaitUntilReplicatedTime(t, s.Clock().Now(), runnerB, jobBID)
	runnerA.Exec(t, fmt.Sprintf("DELETE FROM %s LIMIT 5", tableName))
	WaitUntilReplicatedTime(t, s.Clock().Now(), runnerB, jobBID)

	compareReplicatedTables(t, s, "a", "b", tableName, runnerA, runnerB)
}

func TestUDFApplieSpecified(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, sqlA, sqlB, cleanup := setupTwoDBUDFTestCluster(t)
	defer cleanup()

	runnerA := sqlutils.MakeSQLRunner(sqlA)
	runnerB := sqlutils.MakeSQLRunner(sqlB)

	tableName := "tallies"
	stmt := "CREATE TABLE tallies(pk INT PRIMARY KEY, v INT)"
	runnerA.Exec(t, stmt)
	runnerA.Exec(t, "INSERT INTO tallies VALUES (1, 10), (2, 22), (3, 33)")
	runnerB.Exec(t, stmt)
	runnerB.Exec(t, applierTypes)
	runnerB.Exec(t, `
		CREATE OR REPLACE FUNCTION repl_apply(action STRING, proposed tallies, existing tallies)
		RETURNS crdb_replication_applier_decision
		AS $$
		BEGIN
		IF action = 'insert' OR action = 'update' THEN
			RETURN ('upsert_specified', ((proposed).pk, (proposed).v + 1000));
		END IF;
		RETURN ('accept_proposed', NULL);
		END
		$$ LANGUAGE plpgsql
		`)

	addCol := fmt.Sprintf(`ALTER TABLE %s `+lwwColumnAdd, tableName)
	runnerA.Exec(t, addCol)
	runnerB.Exec(t, addCol)

	dbAURL, cleanup := s.PGUrl(t, serverutils.DBName("a"))
	defer cleanup()

	streamStartStmt := fmt.Sprintf("CREATE LOGICAL REPLICATION STREAM FROM TABLE %[1]s ON $1 INTO TABLE %[1]s", tableName)
	var jobBID jobspb.JobID
	runnerB.QueryRow(t, streamStartStmt, dbAURL.String()).Scan(&jobBID)

	t.Logf("waiting for replication job %d", jobBID)
	WaitUntilReplicatedTime(t, s.Clock().Now(), runnerB, jobBID)
	runnerB.CheckQueryResults(t, "SELECT * FROM tallies", [][]string{
		{"1", "1010"},
		{"2", "1022"},
		{"3", "1033"},
	})
}

func TestUDFInsertOnly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, sqlA, sqlB, cleanup := setupTwoDBUDFTestCluster(t)
	defer cleanup()

	runnerA := sqlutils.MakeSQLRunner(sqlA)
	runnerB := sqlutils.MakeSQLRunner(sqlB)
	tableName := "tallies"
	stmt := "CREATE TABLE tallies(pk INT PRIMARY KEY, v INT)"
	runnerA.Exec(t, stmt)
	runnerA.Exec(t, "INSERT INTO tallies VALUES (1, 10), (2, 22), (3, 33), (4, 44)")
	runnerB.Exec(t, stmt)
	runnerB.Exec(t, applierTypes)
	runnerB.Exec(t, `
		CREATE OR REPLACE FUNCTION repl_apply(action STRING, proposed tallies, existing tallies)
		RETURNS crdb_replication_applier_decision
		AS $$
		BEGIN
		IF action = 'insert' THEN
			RETURN ('accept_proposed', NULL);
		END IF;
		RETURN ('ignore_proposed', NULL);
		END
		$$ LANGUAGE plpgsql
		`)

	addCol := fmt.Sprintf(`ALTER TABLE %s `+lwwColumnAdd, tableName)
	runnerA.Exec(t, addCol)
	runnerB.Exec(t, addCol)

	dbAURL, cleanup := s.PGUrl(t, serverutils.DBName("a"))
	defer cleanup()

	streamStartStmt := fmt.Sprintf("CREATE LOGICAL REPLICATION STREAM FROM TABLE %[1]s ON $1 INTO TABLE %[1]s", tableName)
	var jobBID jobspb.JobID
	runnerB.QueryRow(t, streamStartStmt, dbAURL.String()).Scan(&jobBID)

	t.Logf("waiting for replication job %d", jobBID)
	WaitUntilReplicatedTime(t, s.Clock().Now(), runnerB, jobBID)
	runnerA.Exec(t, "INSERT INTO tallies VALUES (5, 55)")
	runnerA.Exec(t, "DELETE FROM tallies WHERE pk = 4")
	runnerA.Exec(t, "UPDATE tallies SET v = 333 WHERE pk = 3")
	WaitUntilReplicatedTime(t, s.Clock().Now(), runnerB, jobBID)

	runnerB.CheckQueryResults(t, "SELECT * FROM tallies", [][]string{
		{"1", "10"},
		{"2", "22"},
		{"3", "33"},
		{"4", "44"},
		{"5", "55"},
	})
}

func setupTwoDBUDFTestCluster(
	t *testing.T,
) (serverutils.ApplicationLayerInterface, *gosql.DB, *gosql.DB, func()) {
	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	s := srv.ApplicationLayer()

	_, err := sqlDB.Exec("CREATE DATABASE a")
	require.NoError(t, err)
	_, err = sqlDB.Exec("CREATE DATABASE b")
	require.NoError(t, err)

	sqlA := s.SQLConn(t, serverutils.DBName("a"))
	sqlB := s.SQLConn(t, serverutils.DBName("b"))
	for _, s := range testClusterSettings {
		_, err := sqlA.Exec(s)
		require.NoError(t, err)
	}
	defaultSQLProcessor = udfApplierProcessor
	return s, sqlA, sqlB, func() {
		srv.Stopper().Stop(ctx)
		defaultSQLProcessor = lwwProcessor
	}
}
