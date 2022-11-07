// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type fakeResumer struct {
	job     *jobs.Job
	started chan struct{}
}

func (f *fakeResumer) Resume(ctx context.Context, execCtx interface{}) error {
	close(f.started)
	<-ctx.Done()
	return nil
}

func (f *fakeResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}, jobErr error) error {
	return nil
}

var _ jobs.Resumer = (*fakeResumer)(nil)

// Verifies that static jobs are created on server starup.
func TestCreatesStaticJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const staticJobID jobspb.JobID = 99
	resumer := &fakeResumer{started: make(chan struct{})}

	restore := jobs.TestingRegisterConstructor(
		jobspb.TypeImport,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			resumer.job = job
			return resumer
		}, jobs.UsesTenantCostControl)
	defer restore()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			UpgradeManager: &upgrade.TestingKnobs{
				RegistryOverride: func(cv clusterversion.ClusterVersion) (upgrade.Upgrade, bool) {
					if !cv.Version.Equal(clusterversion.TestingClusterVersion) {
						return nil, false
					}
					return upgrades.TestingGetUpgradeForStaticJob(
						staticJobID, clusterversion.TestingBinaryMinSupportedVersion,
						jobspb.ImportDetails{}, jobspb.ImportProgress{},
					), true
				},
			},
		},
	})
	defer s.Stopper().Stop(context.Background())

	<-resumer.started
	require.EqualValues(t, staticJobID, resumer.job.ID())

	// By default, static jobs are not cancellable.
	_, err := db.ExecContext(context.Background(), "CANCEL JOB $1", staticJobID)
	require.Regexp(t, fmt.Sprintf("job %d: not cancelable", staticJobID), err)

	// Even though by default the job is non-cancellable,
	// we should still be able to cancel it (by writing appropriate migration
	// to deprecate that job).
	// We simply check to make sure dropping static, non-cancellable job works.
	defer upgrades.TestingRetireStaticJob(staticJobID)()
	require.NoError(t, upgrades.TestingDeprecateStaticJob(
		context.Background(), staticJobID, s.JobRegistry().(*jobs.Registry)),
	)
	sqlDB := sqlutils.MakeSQLRunner(db)
	testutils.SucceedsSoon(t, func() error {
		var jobStatus string
		query := `SELECT status FROM [SHOW JOB $1]`
		sqlDB.QueryRow(t, query, staticJobID).Scan(&jobStatus)
		if jobs.StatusCanceled != jobs.Status(jobStatus) {
			return errors.Newf("Expected status:%s but found status:%s", jobs.StatusCanceled, jobStatus)
		}
		return nil
	})
}

// TestShowAllStaticJobs checks the sanity of configured static jobs.
// Executing SHOW JOB on those static jobs will hopefully shake out
// most of the misconfigurations.
func TestShowAllStaticJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer s.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, "SHOW JOBS")
	sqlDB.Exec(t, "SHOW AUTOMATIC JOBS")
	for id := range upgrades.TestingGetStaticJobIDs() {
		sqlDB.Exec(t, "SHOW JOB $1", id)
	}
}
