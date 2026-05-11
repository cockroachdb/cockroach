// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrtestutils"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestAlterLDRSkipAdvancesReplicatedTime(t *testing.T) {
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

	progressBefore := jobutils.GetJobProgress(t, destDB, jobID)
	replicatedBefore := progressBefore.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication.ReplicatedTime

	destDB.Exec(t, fmt.Sprintf("ALTER LOGICAL REPLICATION STREAM %d SKIP", jobID))

	progressAfter := jobutils.GetJobProgress(t, destDB, jobID)
	replicatedAfter := progressAfter.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication.ReplicatedTime
	require.Equal(t, replicatedBefore.Next(), replicatedAfter)

	destDB.Exec(t, "RESUME JOB $1", jobID)
	jobutils.WaitForJobToRun(t, destDB, jobID)

	sourceDB.Exec(t, "INSERT INTO tab VALUES (2, 'after-skip')")
	now := srv.Clock().Now()
	ldrtestutils.WaitUntilReplicatedTime(t, now, destDB, jobID)

	destDB.CheckQueryResults(t,
		"SELECT pk, val FROM tab ORDER BY pk",
		[][]string{{"2", "after-skip"}, {"100", "collide"}},
	)
}

func TestAlterLDRSkipRejectsInvalidState(t *testing.T) {
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
	}

	sourceURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("source_db"))

	t.Run("skip on running job", func(t *testing.T) {
		var jobID jobspb.JobID
		destDB.QueryRow(t,
			"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH MODE = 'transactional'",
			sourceURL.String(),
		).Scan(&jobID)
		jobutils.WaitForJobToRun(t, destDB, jobID)

		_, err := destDB.DB.ExecContext(ctx,
			fmt.Sprintf("ALTER LOGICAL REPLICATION STREAM %d SKIP", jobID))
		require.Error(t, err)
		require.Contains(t, err.Error(), "job is not paused")

		destDB.Exec(t, "CANCEL JOB $1", jobID)
		jobutils.WaitForJobToHaveStatus(t, destDB, jobID, jobs.StateCanceled)
	})

	t.Run("skip on non-transactional job", func(t *testing.T) {
		var jobID jobspb.JobID
		destDB.QueryRow(t,
			"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH MODE = 'immediate'",
			sourceURL.String(),
		).Scan(&jobID)
		jobutils.WaitForJobToRun(t, destDB, jobID)
		destDB.Exec(t, "PAUSE JOB $1", jobID)
		jobutils.WaitForJobToPause(t, destDB, jobID)

		_, err := destDB.DB.ExecContext(ctx,
			fmt.Sprintf("ALTER LOGICAL REPLICATION STREAM %d SKIP", jobID))
		require.Error(t, err)
		require.Contains(t, err.Error(), "SKIP is only supported for transactional mode")

		destDB.Exec(t, "CANCEL JOB $1", jobID)
		jobutils.WaitForJobToHaveStatus(t, destDB, jobID, jobs.StateCanceled)
	})
}
