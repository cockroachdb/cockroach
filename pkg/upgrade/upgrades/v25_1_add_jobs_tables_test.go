// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestBackfillJobsTablesAndColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V25_1)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					ClusterVersionOverride:         clusterversion.MinSupported.Version(),
				},
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, clusterArgs)
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

	reg := tc.Server(0).JobRegistry().(*jobs.Registry)
	resume := make(map[jobspb.JobID]chan error)
	for i := 0; i < 10; i++ {
		resume[jobspb.JobID(i+1000)] = make(chan error)
	}

	defer func() {
		for _, ch := range resume {
			close(ch)
		}
	}()

	defer jobs.TestingRegisterConstructor(jobspb.TypeImport, func(j *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return jobstest.FakeResumer{
			OnResume: func(ctx context.Context) error {
				return <-resume[j.ID()]
			},
		}
	}, jobs.DisablesTenantCostControl)()

	mkJob := func(id jobspb.JobID, desc string) {
		var sj *jobs.StartableJob
		require.NoError(t, tc.Servers[0].InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return reg.CreateStartableJobWithTxn(ctx, &sj, id, txn,
				jobs.Record{
					Details:     jobspb.ImportDetails{},
					Progress:    jobspb.ImportProgress{},
					Description: desc,
					Username:    username.RootUserName(),
				})
		}))
		require.NoError(t, sj.Start(ctx))
	}

	getJob := func(id jobspb.JobID) *jobs.Job {
		j, err := reg.LoadJob(ctx, id)
		require.NoError(t, err)
		return j
	}

	mkJob(1000, "zero")
	resume[1000] <- nil

	mkJob(1001, "one")
	resume[1001] <- errors.New("one failed")

	// Make two jobs while still on 24.3; update their progress and status.
	mkJob(1002, "two")
	mkJob(1003, "three")
	require.NoError(t, getJob(1002).NoTxn().FractionProgressed(ctx, jobs.FractionUpdater(.5)))
	require.NoError(t, getJob(1003).NoTxn().UpdateStatusMessage(ctx, "up that hill"))

	// Do this again on the verison that includes status and progress tables.
	sqlDB.Exec(t, `SET CLUSTER SETTING version = $1`, clusterversion.V25_1_AddJobsTables.Version().String())
	mkJob(1004, "four")
	mkJob(1005, "five")

	require.NoError(t, getJob(1004).NoTxn().FractionProgressed(ctx, jobs.FractionUpdater(.5)))
	require.NoError(t, getJob(1005).NoTxn().UpdateStatusMessage(ctx, "up that hill"))

	// And again on the version that has the new columns.
	sqlDB.Exec(t, `SET CLUSTER SETTING version = $1`, clusterversion.V25_1_JobsWritesFence.Version().String())

	mkJob(1006, "six")
	mkJob(1007, "seven")
	mkJob(1008, "eight")
	require.NoError(t, getJob(1006).NoTxn().FractionProgressed(ctx, jobs.FractionUpdater(.5)))
	require.NoError(t, getJob(1006).NoTxn().UpdateStatusMessage(ctx, "up that hill"))
	require.NoError(t, getJob(1007).NoTxn().FractionProgressed(ctx, jobs.FractionUpdater(.5)))
	require.NoError(t, getJob(1007).NoTxn().UpdateStatusMessage(ctx, "up that hill"))
	resume[1006] <- nil
	resume[1008] <- errors.New("eight failed")

	jobutils.WaitForJobToSucceed(t, sqlDB, 1000)
	jobutils.WaitForJobToFail(t, sqlDB, 1001)
	jobutils.WaitForJobToSucceed(t, sqlDB, 1006)
	jobutils.WaitForJobToFail(t, sqlDB, 1008)

	// Let's look at the jobs table before the backfill.
	sqlDB.CheckQueryResults(t,
		`SELECT id, description, owner, finished IS NOT NULL, error_msg FROM system.jobs WHERE id >= 1000 AND id <= 1008 ORDER BY id `,
		[][]string{
			{"1000", "NULL", "NULL", "false", "NULL"},
			{"1001", "NULL", "NULL", "false", "NULL"},
			{"1002", "NULL", "NULL", "false", "NULL"},
			{"1003", "NULL", "NULL", "false", "NULL"},
			{"1004", "NULL", "NULL", "false", "NULL"},
			{"1005", "NULL", "NULL", "false", "NULL"},
			{"1006", "six", "root", "true", "NULL"},
			{"1007", "seven", "root", "false", "NULL"},
			{"1008", "eight", "root", "true", "eight failed"},
		})

	sqlDB.CheckQueryResults(t,
		`SELECT job_id, fraction FROM system.job_progress WHERE job_id >= 1000 AND job_id <= 1008 ORDER BY job_id `,
		[][]string{
			{"1004", "0.5"},
			{"1005", "0"},
			{"1006", "1"},
			{"1007", "0.5"},
			{"1008", "0"},
		})

	sqlDB.CheckQueryResults(t,
		`SELECT job_id, status FROM system.job_status WHERE job_id >= 1000 AND job_id <= 1008 ORDER BY job_id `,
		[][]string{
			{"1005", "up that hill"},
			{"1006", "up that hill"},
			{"1007", "up that hill"},
		})

	// Now let's do the backfill (lower the page size to make it paginate).
	upgrades.TestingSetJobsBackfillPageSize(3)
	sqlDB.Exec(t, `SET CLUSTER SETTING version = $1`, clusterversion.V25_1_JobsBackfill.Version().String())

	// Let's look at the jobs table after the backfill.
	sqlDB.CheckQueryResults(t,
		`SELECT id, description, owner, finished IS NOT NULL, error_msg FROM system.jobs WHERE id >= 1000 AND id <= 1008 ORDER BY id `,
		[][]string{
			{"1000", "zero", "root", "true", "NULL"},
			{"1001", "one", "root", "true", "one failed"},
			{"1002", "two", "root", "false", "NULL"},
			{"1003", "three", "root", "false", "NULL"},
			{"1004", "four", "root", "false", "NULL"},
			{"1005", "five", "root", "false", "NULL"},
			{"1006", "six", "root", "true", "NULL"},
			{"1007", "seven", "root", "false", "NULL"},
			{"1008", "eight", "root", "true", "eight failed"},
		})

	sqlDB.CheckQueryResults(t,
		`SELECT job_id, fraction FROM system.job_progress WHERE job_id >= 1000 AND job_id <= 1008 ORDER BY job_id `,
		[][]string{
			{"1000", "1"},
			{"1001", "0"},
			{"1002", "0.5"},
			{"1003", "0"},
			{"1004", "0.5"},
			{"1005", "0"},
			{"1006", "1"},
			{"1007", "0.5"},
			{"1008", "0"},
		})

	sqlDB.CheckQueryResults(t,
		`SELECT job_id, status FROM system.job_status WHERE job_id >= 1000 AND job_id <= 1008 ORDER BY job_id `,
		[][]string{
			{"1003", "up that hill"},
			{"1005", "up that hill"},
			{"1006", "up that hill"},
			{"1007", "up that hill"},
		})

}
