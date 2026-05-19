// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode_test

import (
	"context"
	"fmt"
	"testing"

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

	cluster, sourceDB, destDB := setupTxnModeTest(t, 1 /* numNodes */)
	defer cluster.Stopper().Stop(ctx)
	jobID := setupConflictingLDR(t, cluster, sourceDB, destDB)
	jobutils.WaitForJobToPause(t, destDB, jobID)

	replicatedBefore, err := ldrtestutils.GetReplicatedTime(t, destDB, jobID)
	require.NoError(t, err)

	destDB.Exec(t, fmt.Sprintf("ALTER LOGICAL REPLICATION STREAM %d SKIP", jobID))

	replicatedAfter, err := ldrtestutils.GetReplicatedTime(t, destDB, jobID)
	require.NoError(t, err)
	require.Equal(t, replicatedBefore.Next(), replicatedAfter)

	destDB.Exec(t, "RESUME JOB $1", jobID)
	jobutils.WaitForJobToRun(t, destDB, jobID)

	sourceDB.Exec(t, "INSERT INTO tab VALUES (2, 'after-skip')")
	ldrtestutils.WaitUntilReplicatedTime(t, cluster.Server(0).Clock().Now(), destDB, jobID)

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

	cluster, sourceDB, destDB := setupTxnModeTest(t, 1 /* numNodes */)
	defer cluster.Stopper().Stop(ctx)

	for _, db := range []*sqlutils.SQLRunner{sourceDB, destDB} {
		db.Exec(t, "CREATE TABLE tab (pk INT PRIMARY KEY, val STRING NOT NULL)")
	}

	s := cluster.Server(0).ApplicationLayer()
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
		require.Contains(t, err.Error(), "is not paused")

		destDB.Exec(t, "CANCEL JOB $1", jobID)
		jobutils.WaitForJobToHaveStatus(t, destDB, jobID, jobs.StateCanceled)
	})

	t.Run("skip on non-LDR job", func(t *testing.T) {
		var nonLDRJobID jobspb.JobID
		destDB.QueryRow(t,
			"SELECT job_id FROM crdb_internal.jobs WHERE job_type != 'LOGICAL REPLICATION' LIMIT 1",
		).Scan(&nonLDRJobID)
		destDB.Exec(t, "PAUSE JOB $1", nonLDRJobID)
		jobutils.WaitForJobToPause(t, destDB, nonLDRJobID)

		_, err := destDB.DB.ExecContext(ctx,
			fmt.Sprintf("ALTER LOGICAL REPLICATION STREAM %d SKIP", nonLDRJobID))
		require.Error(t, err)
		require.Contains(t, err.Error(), "is not a logical replication job")

		destDB.Exec(t, "RESUME JOB $1", nonLDRJobID)
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
