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

	// Enable rangefeed on the system layer
	sysRunner := sqlutils.MakeSQLRunner(srv.SystemLayer().SQLConn(t))
	sysRunner.Exec(t, "SET CLUSTER SETTING kv.rangefeed.enabled = true")

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

