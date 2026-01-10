// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrtestutils"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestTxnModeSmoketest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestDoesNotWorkWithExternalProcessMode(134857),
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(conn)

	// Configure low latency replication settings
	sysRunner := sqlutils.MakeSQLRunner(srv.SystemLayer().SQLConn(t))
	ldrtestutils.ApplyLowLatencyReplicationSettings(t, sysRunner, runner)

	// Create source and destination databases
	runner.Exec(t, "CREATE DATABASE source_db")
	runner.Exec(t, "CREATE DATABASE dest_db")

	sourceDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("source_db")))
	destDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("dest_db")))

	for _, db := range [](*sqlutils.SQLRunner){sourceDB, destDB} {
		// NOTE: this only works right now because the parent's descriptor id sorts
		// before the childs and we don't run deletes in the same txn.
		db.Exec(t, "CREATE TABLE parent (id INT PRIMARY KEY)")
		db.Exec(t, "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id))")
	}

	// Get connection URL for source database
	sourceURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("source_db"))

	// Create logical replication stream with transaction mode
	var jobID jobspb.JobID
	destDB.QueryRow(t,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLES (parent, child) ON $1 INTO TABLES (parent, child) WITH MODE = 'transaction'",
		sourceURL.String(),
	).Scan(&jobID)

	// Check job status
	var status string
	destDB.QueryRow(t, "SELECT status FROM [SHOW JOB $1]", jobID).Scan(&status)
	t.Logf("Job %d status: %s", jobID, status)

	// Insert data into source after starting replication
	sourceDB.Exec(t, "INSERT INTO parent (id) VALUES (1); INSERT INTO child (id, parent_id) VALUES (1, 1), (2, 1), (3, 1)")

	// Wait for replication to catch up
	now := s.Clock().Now()

	// The only way to get time to advance is to perform some writes.
	sourceDB.Exec(t, "INSERT INTO child (id, parent_id) VALUES (4, 1)")

	ldrtestutils.WaitUntilReplicatedTime(t, now, destDB, jobID)

	// Verify data was replicated
	destDB.CheckQueryResults(t, "SELECT * FROM child ORDER BY id", [][]string{
		{"1", "1"},
		{"2", "1"},
		{"3", "1"},
		{"4", "1"},
	})
}

func TestTxnModeUniqueConstraintUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestDoesNotWorkWithExternalProcessMode(134857),
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(conn)

	// Configure low latency replication settings
	sysRunner := sqlutils.MakeSQLRunner(srv.SystemLayer().SQLConn(t))
	ldrtestutils.ApplyLowLatencyReplicationSettings(t, sysRunner, runner)

	// Create source and destination databases
	runner.Exec(t, "CREATE DATABASE source_db")
	runner.Exec(t, "CREATE DATABASE dest_db")

	sourceDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("source_db")))
	destDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("dest_db")))

	// Create table with UUID primary key and unique int column
	for _, db := range [](*sqlutils.SQLRunner){sourceDB, destDB} {
		db.Exec(t, "CREATE TABLE test_table (uuid UUID PRIMARY KEY, unique_value INT UNIQUE)")
	}

	// Get connection URL for source database
	sourceURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("source_db"))

	// Create logical replication stream with transaction mode
	var jobID jobspb.JobID
	destDB.QueryRow(t,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLES (source_db.test_table) ON $1 INTO TABLES (dest_db.test_table) WITH MODE = 'transaction'",
		sourceURL.String(),
	).Scan(&jobID)

	// Insert initial rows after starting replication
	sourceDB.Exec(t, "INSERT INTO test_table (uuid, unique_value) VALUES (gen_random_uuid(), 1337)")

	// Update the UUID (primary key) of the row with unique_value = 1337
	// This tests that lock synthesis correctly orders the delete and insert operations
	now := s.Clock().Now()
	sourceDB.Exec(t, "UPDATE test_table SET uuid = gen_random_uuid() WHERE unique_value = 1337")

	// TODO(jeffswenson): once we have periodic checkpointing wait for a time after the update.
	ldrtestutils.WaitUntilReplicatedTime(t, now, destDB, jobID)

	// Verify the update was replicated (row with unique_value = 1337 still exists with new UUID)
	destDB.CheckQueryResults(t, "SELECT unique_value FROM test_table ORDER BY unique_value", [][]string{
		{"1337"},
	})

	// Verify we can still query by unique_value
	var count int
	destDB.QueryRow(t, "SELECT count(*) FROM test_table WHERE unique_value = 1337").Scan(&count)
	if count != 1 {
		t.Fatalf("expected 1 row with unique_value = 1337, got %d", count)
	}
}
