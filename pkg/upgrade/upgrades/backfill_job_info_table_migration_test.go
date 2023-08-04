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
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestBackfillJobsInfoTable tests that the `system.job_info` table is
// backfilled with the progress and payload of each job in the `system.jobs`
// table.
func TestBackfillJobsInfoTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		// Disable all automatic jobs creation and adoption.
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.V22_2),
				},
			},
		},
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, clusterArgs)
	r := tc.Server(0).JobRegistry().(*jobs.Registry)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	defer tc.Stopper().Stop(ctx)

	createJob := func(id jobspb.JobID, details jobspb.Details, progress jobspb.ProgressDetails) {
		defaultRecord := jobs.Record{
			Details:  details,
			Progress: progress,
			Username: username.TestUserName(),
		}

		var job *jobs.StartableJob
		db := tc.Server(0).InternalDB().(*sql.InternalDB)
		err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return r.CreateStartableJobWithTxn(ctx, &job, id, txn, defaultRecord)
		})
		require.NoError(t, err)
	}

	// Create a few different types of jobs.
	createJob(1, jobspb.BackupDetails{}, jobspb.BackupProgress{})
	createJob(2, jobspb.RestoreDetails{}, jobspb.RestoreProgress{})
	createJob(3, jobspb.ChangefeedDetails{}, jobspb.ChangefeedProgress{})

	upgrades.Upgrade(t, tc.ServerConn(0), clusterversion.V23_1CreateSystemJobInfoTable, nil, false)

	// Create two more jobs that we should see written to both system.jobs and
	// system.job_info.
	createJob(4, jobspb.ImportDetails{}, jobspb.ImportProgress{})
	createJob(5, jobspb.SchemaChangeDetails{}, jobspb.SchemaChangeProgress{})

	// Validate that we see 2 rows (payload and progress) in the system.job_info
	// table for each row written in the system.jobs table since the last upgrade.
	sqlDB.CheckQueryResults(t, `
SELECT count(*) FROM system.jobs AS j, system.job_info AS i
WHERE j.id = i.job_id AND (j.payload = i.value OR j.progress = i.value) AND (j.id >= 1 AND j.id <= 5)
`,
		[][]string{{"4"}})

	upgrades.Upgrade(t, tc.ServerConn(0), clusterversion.V23_1JobInfoTableIsBackfilled, nil, false)

	// At this point the entire system.jobs table should be backfilled into the
	// system.job_info table.
	// We expect to see 14 rows because of:
	// - 4 rows from before
	// - 10 rows (payload + progress) for the 5 jobs in system.jobs
	sqlDB.CheckQueryResults(t, `
SELECT count(*) FROM system.jobs AS j, system.job_info AS i
WHERE j.id = i.job_id AND (j.payload = i.value OR j.progress = i.value) AND (j.id >= 1 AND j.id <= 5)
`,
		[][]string{{"14"}})
}

var _ jobs.Resumer = &fakeJob{}

type fakeJob struct {
	job      *jobs.Job
	ch1, ch2 chan<- string
}

func (r *fakeJob) Resume(ctx context.Context, _ interface{}) error {
	ch := r.ch1
	if r.job.Progress().Details.(*jobspb.Progress_Import).Import.ResumePos[0] == 2 {
		ch = r.ch2
	}
	select {
	case ch <- fmt.Sprintf("%s %v",
		r.job.Details().(jobspb.ImportDetails).BackupPath,
		r.job.Progress().Details.(*jobspb.Progress_Import).Import.ResumePos):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *fakeJob) OnFailOrCancel(ctx context.Context, execCtx interface{}, _ error) error {
	return nil
}

func TestIncompleteBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{Knobs: base.TestingKnobs{Server: &server.TestingKnobs{
			DisableAutomaticVersionUpgrade: make(chan struct{}),
			BootstrapVersionKeyOverride:    clusterversion.V22_2,
			BinaryVersionOverride:          clusterversion.ByKey(clusterversion.V22_2),
		}}},
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, clusterArgs)
	defer tc.Stopper().Stop(ctx)

	r := tc.Server(0).JobRegistry().(*jobs.Registry)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	ch1 := make(chan string, 1)
	ch2 := make(chan string, 1)

	jobs.RegisterConstructor(
		jobspb.TypeImport,
		func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer { return &fakeJob{job: job, ch1: ch1, ch2: ch2} },
		jobs.UsesTenantCostControl,
	)

	var adoptedJob, runningJob *jobs.StartableJob
	runningID, adoptedID := jobspb.JobID(5550001), jobspb.JobID(5550002)
	require.NoError(t, tc.Server(0).InternalDB().(isql.DB).Txn(ctx, func(
		ctx context.Context, txn isql.Txn,
	) (err error) {

		if err := r.CreateStartableJobWithTxn(ctx, &adoptedJob, adoptedID, txn, jobs.Record{
			Username: username.RootUserName(),
			Details:  jobspb.ImportDetails{BackupPath: "adopted"},
			Progress: jobspb.ImportProgress{ResumePos: []int64{1}},
		}); err != nil {
			return err
		}

		if err := r.CreateStartableJobWithTxn(ctx, &runningJob, runningID, txn, jobs.Record{
			Username: username.RootUserName(),
			Details:  jobspb.ImportDetails{BackupPath: "running"},
			Progress: jobspb.ImportProgress{ResumePos: []int64{2}},
		}); err != nil {
			return err
		}
		return nil
	}))

	upgrades.TestingSkipInfoBackfill = true
	defer func() {
		upgrades.TestingSkipInfoBackfill = false
	}()

	sqlDB.Exec(t, "SET CLUSTER SETTING version = $1", clusterversion.ByKey(clusterversion.V23_1).String())
	r.TestingForgetJob(adoptedID)
	r.NotifyToResume(ctx, adoptedID)

	require.NoError(t, runningJob.Start(ctx))
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	require.NoError(t, runningJob.AwaitCompletion(ctx))

	select {
	case res := <-ch1:
		require.Equal(t, "adopted [1]", res)
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for job to run")
	}

	select {
	case res := <-ch2:
		require.Equal(t, "running [2]", res)
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for job to run")
	}

}
