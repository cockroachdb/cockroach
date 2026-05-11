// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrtestutils"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestTxnModePauseOnConflict verifies that a transactional LDR job pauses when
// a replicated transaction violates a unique constraint on the destination,
// and that the conflicting row is not applied.
func TestTxnModePauseOnConflict(t *testing.T) {
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

	sysRunner := sqlutils.MakeSQLRunner(srv.SystemLayer().SQLConn(t))
	ldrtestutils.ApplyLowLatencyReplicationSettings(t, sysRunner, runner)

	runner.Exec(t, "CREATE DATABASE source_db")
	runner.Exec(t, "CREATE DATABASE dest_db")

	sourceDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("source_db")))
	destDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("dest_db")))

	for _, db := range []*sqlutils.SQLRunner{sourceDB, destDB} {
		db.Exec(t, "CREATE TABLE tab (pk INT PRIMARY KEY, val STRING NOT NULL)")
		db.Exec(t, "CREATE UNIQUE INDEX ON tab(val)")
	}

	destDB.Exec(t, "INSERT INTO tab VALUES (100, 'collide')")

	sourceURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("source_db"))

	var jobID jobspb.JobID
	destDB.QueryRow(t,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH MODE = 'transactional'",
		sourceURL.String(),
	).Scan(&jobID)

	sourceDB.Exec(t, "INSERT INTO tab VALUES (1, 'collide')")

	jobutils.WaitForJobToPause(t, destDB, jobID)

	var runningStatus string
	destDB.QueryRow(t, "SELECT running_status FROM [SHOW JOBS] WHERE job_id = $1", jobID).Scan(&runningStatus)
	require.Contains(t, runningStatus, "replication error")
	require.Contains(t, runningStatus, "duplicate key value violates unique constraint")

	// The conflicting source row at pk=1 must not have been applied.
	destDB.CheckQueryResults(t, "SELECT pk, val FROM tab ORDER BY pk", [][]string{
		{"100", "collide"},
	})

	// Check that the replicated time equals the MVCC timestamp of the
	// conflicting source insert minus one logical tick.
	var conflictMVCCDec apd.Decimal
	sourceDB.QueryRow(t, "SELECT crdb_internal_mvcc_timestamp FROM tab WHERE pk = 1").Scan(&conflictMVCCDec)
	conflictMVCC, err := hlc.DecimalToHLC(&conflictMVCCDec)
	require.NoError(t, err)
	progress := jobutils.GetJobProgress(t, destDB, jobID)
	replicatedTime := progress.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication.ReplicatedTime
	require.Equal(t, conflictMVCC.Prev(), replicatedTime)
}

// TestTxnModeResumeAfterFixingConflict verifies that when a transactional LDR
// job pauses on a unique constraint conflict, the user can remove the
// conflicting row on the destination, resume the job, and the
// previously failed transaction is retried and applied successfully.
func TestTxnModeResumeAfterFixingConflict(t *testing.T) {
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

	sysRunner := sqlutils.MakeSQLRunner(srv.SystemLayer().SQLConn(t))
	ldrtestutils.ApplyLowLatencyReplicationSettings(t, sysRunner, runner)

	runner.Exec(t, "CREATE DATABASE source_db")
	runner.Exec(t, "CREATE DATABASE dest_db")

	sourceDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("source_db")))
	destDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("dest_db")))

	for _, db := range []*sqlutils.SQLRunner{sourceDB, destDB} {
		db.Exec(t, "CREATE TABLE tab (pk INT PRIMARY KEY, val STRING NOT NULL)")
		db.Exec(t, "CREATE UNIQUE INDEX ON tab(val)")
	}

	destDB.Exec(t, "INSERT INTO tab VALUES (100, 'collide')")

	sourceURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("source_db"))

	var jobID jobspb.JobID
	destDB.QueryRow(t,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH MODE = 'transactional'",
		sourceURL.String(),
	).Scan(&jobID)

	sourceDB.Exec(t, "INSERT INTO tab VALUES (1, 'collide')")

	jobutils.WaitForJobToPause(t, destDB, jobID)

	// Remove the conflicting row on the destination so the transaction
	// can be retried successfully.
	destDB.Exec(t, "DELETE FROM tab WHERE pk = 100")

	destDB.Exec(t, "RESUME JOB $1", jobID)
	jobutils.WaitForJobToRun(t, destDB, jobID)

	now := srv.Clock().Now()
	ldrtestutils.WaitUntilReplicatedTime(t, now, destDB, jobID)

	destDB.CheckQueryResults(t,
		"SELECT pk, val FROM tab ORDER BY pk",
		[][]string{{"1", "collide"}},
	)
}

// TestTxnModeResumePausesAgainOnUnresolvedConflict verifies that resuming a
// paused transactional LDR job without fixing the conflict causes the job to
// pause again at the same replicated time.
func TestTxnModeResumePausesAgainOnUnresolvedConflict(t *testing.T) {
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

	sysRunner := sqlutils.MakeSQLRunner(srv.SystemLayer().SQLConn(t))
	ldrtestutils.ApplyLowLatencyReplicationSettings(t, sysRunner, runner)

	runner.Exec(t, "CREATE DATABASE source_db")
	runner.Exec(t, "CREATE DATABASE dest_db")

	sourceDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("source_db")))
	destDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("dest_db")))

	for _, db := range []*sqlutils.SQLRunner{sourceDB, destDB} {
		db.Exec(t, "CREATE TABLE tab (pk INT PRIMARY KEY, val STRING NOT NULL)")
		db.Exec(t, "CREATE UNIQUE INDEX ON tab(val)")
	}

	destDB.Exec(t, "INSERT INTO tab VALUES (100, 'collide')")

	sourceURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("source_db"))

	var jobID jobspb.JobID
	destDB.QueryRow(t,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH MODE = 'transactional'",
		sourceURL.String(),
	).Scan(&jobID)

	sourceDB.Exec(t, "INSERT INTO tab VALUES (1, 'collide')")

	jobutils.WaitForJobToPause(t, destDB, jobID)

	progressFirst := jobutils.GetJobProgress(t, destDB, jobID)
	replicatedFirst := progressFirst.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication.ReplicatedTime

	destDB.Exec(t, "RESUME JOB $1", jobID)
	jobutils.WaitForJobToPause(t, destDB, jobID)

	progressSecond := jobutils.GetJobProgress(t, destDB, jobID)
	replicatedSecond := progressSecond.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication.ReplicatedTime

	require.Equal(t, replicatedFirst, replicatedSecond)
}

// TestTxnModePauseOnEarliestConflict verifies that when multiple replicated
// transactions conflict at different timestamps, the job converges on the
// first conflict (by timestamp) and drains every prior transaction before
// pausing.
func TestTxnModePauseOnEarliestConflict(t *testing.T) {
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

	sysRunner := sqlutils.MakeSQLRunner(srv.SystemLayer().SQLConn(t))
	ldrtestutils.ApplyLowLatencyReplicationSettings(t, sysRunner, runner)

	runner.Exec(t, "CREATE DATABASE source_db")
	runner.Exec(t, "CREATE DATABASE dest_db")

	sourceDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("source_db")))
	destDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("dest_db")))

	for _, db := range []*sqlutils.SQLRunner{sourceDB, destDB} {
		db.Exec(t, "CREATE TABLE tab (pk INT PRIMARY KEY, val STRING NOT NULL, extra STRING NOT NULL)")
		db.Exec(t, "CREATE UNIQUE INDEX tab_first_idx ON tab(val)")
		db.Exec(t, "CREATE UNIQUE INDEX tab_second_idx ON tab(extra)")
	}

	destDB.Exec(t, "INSERT INTO tab VALUES (100, 'first-collide', 'pre-1'), (101, 'pre-2', 'second-collide')")

	sourceURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("source_db"))

	var jobID jobspb.JobID
	destDB.QueryRow(t,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH MODE = 'transactional'",
		sourceURL.String(),
	).Scan(&jobID)

	sourceDB.Exec(t, "INSERT INTO tab VALUES (1, 'ok-1', 'distinct-1')")
	sourceDB.Exec(t, "INSERT INTO tab VALUES (2, 'first-collide', 'distinct-2')")
	sourceDB.Exec(t, "INSERT INTO tab VALUES (3, 'ok-2', 'distinct-3')")
	sourceDB.Exec(t, "INSERT INTO tab VALUES (4, 'distinct-4', 'second-collide')")

	jobutils.WaitForJobToPause(t, destDB, jobID)

	var runningStatus string
	destDB.QueryRow(t, "SELECT running_status FROM [SHOW JOBS] WHERE job_id = $1", jobID).Scan(&runningStatus)
	require.Contains(t, runningStatus, "replication error")
	require.Contains(t, runningStatus, "duplicate key value violates unique constraint")
	require.Contains(t, runningStatus, "tab_first_idx")
	require.NotContains(t, runningStatus, "tab_second_idx")

	destDB.CheckQueryResults(t, "SELECT pk FROM tab WHERE pk IN (1, 2, 4) ORDER BY pk", [][]string{
		{"1"},
	})

	// Check that the replicated time equals the MVCC timestamp of the earliest
	// conflicting source insert (pk=2) minus one logical tick.
	var conflictMVCCDec apd.Decimal
	sourceDB.QueryRow(t, "SELECT crdb_internal_mvcc_timestamp FROM tab WHERE pk = 2").Scan(&conflictMVCCDec)
	conflictMVCC, err := hlc.DecimalToHLC(&conflictMVCCDec)
	require.NoError(t, err)
	progress := jobutils.GetJobProgress(t, destDB, jobID)
	replicatedTime := progress.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication.ReplicatedTime
	require.Equal(t, conflictMVCC.Prev(), replicatedTime)
}
