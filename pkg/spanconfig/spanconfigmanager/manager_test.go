// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigmanager_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigmanager"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// TestManagerConcurrentJobCreation ensures that only one of two concurrent
// attempts to create the auto span config reconciliation job are successful. We
// also ensure that the created job is what we expect.
// Sketch:
// - The first goroutine checks and ensures that the auto span config
// reconciliation job does not exists. Blocks after checking.
// - The second goroutine checks and ensures the auto span config reconciliation
// job does not exists and creates it. Unblocks the first goroutine.
// - The first goroutine tries to create the job but gets restarted. It
// subsequently notices that the job does indeed exist so ends up not creating
// one.
func TestManagerConcurrentJobCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true, // disable the automatic job creation
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	unblocker := make(chan struct{})
	isBlocked := make(chan struct{})
	count := 0

	ts := tc.Server(0)
	manager := spanconfigmanager.New(
		ts.DB(),
		ts.JobRegistry().(*jobs.Registry),
		ts.InternalExecutor().(*sql.InternalExecutor),
		ts.Stopper(),
		ts.ClusterSettings(),
		ts.SpanConfigReconciler().(spanconfig.Reconciler),
		&spanconfig.TestingKnobs{
			ManagerCreatedJobInterceptor: func(jobI interface{}) {
				job := jobI.(*jobs.Job)
				require.True(t, job.Payload().Noncancelable)
				require.Equal(t, job.Payload().Description, "reconciling span configurations")
			},
			ManagerAfterCheckedReconciliationJobExistsInterceptor: func(exists bool) {
				// First entrant blocks, subsequent entrants do not.
				count++
				curCount := count
				if curCount == 1 {
					close(isBlocked)
					<-unblocker
				}

				// We expect to check if the reconciliation job exists a total of 3
				// times. The first two times both entrants will find that the job
				// doesn't exist. Only one of the entrants will be able to create the
				// job and the other is expected to retry. When it retries it expects
				// to find that the job does indeed exist.
				if curCount <= 2 {
					require.False(t, exists)
				} else if curCount == 3 {
					require.True(t, exists)
				} else {
					t.Fatal("expected to check to reconciliation job exists only 3 times")
				}
			},
		},
	)

	var g errgroup.Group
	g.Go(func() error {
		started, err := manager.TestingCreateAndStartJobIfNoneExists(ctx)
		if err != nil {
			return err
		}
		if started {
			return errors.New("expected no job to start, but it did")
		}
		return nil
	})
	g.Go(func() error {
		// Only try to start the job if the first goroutine has reached the testing
		// knob and is blocked.
		<-isBlocked
		started, err := manager.TestingCreateAndStartJobIfNoneExists(ctx)
		if err != nil {
			return err
		}
		if !started {
			return errors.New("expected job to start, but it did not")
		}
		close(unblocker)
		return nil
	})

	require.NoError(t, g.Wait())
}

// TestManagerRestartsJobIfFailed ensures that the auto span config
// reconciliation is created if one has previously failed. We don't expect this
// job to fail, but in the event it happens, we want a new one to start.
func TestManagerStartsJobIfFailed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true, // disable the automatic job creation
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	ts := tc.Server(0)
	manager := spanconfigmanager.New(
		ts.DB(),
		ts.JobRegistry().(*jobs.Registry),
		ts.InternalExecutor().(*sql.InternalExecutor),
		ts.Stopper(),
		ts.ClusterSettings(),
		ts.SpanConfigReconciler().(spanconfig.Reconciler),
		&spanconfig.TestingKnobs{
			ManagerAfterCheckedReconciliationJobExistsInterceptor: func(exists bool) {
				require.False(t, exists)
			},
		},
	)

	payload, err := protoutil.Marshal(&jobspb.Payload{
		UsernameProto: security.RootUserName().EncodeProto(),
		Details:       jobspb.WrapPayloadDetails(jobspb.AutoSpanConfigReconciliationDetails{}),
	})
	require.NoError(t, err)

	_, err = tc.ServerConn(0).Exec(
		`INSERT INTO system.jobs (status, payload) VALUES ($1, $2)`,
		jobs.StatusFailed,
		payload,
	)
	require.NoError(t, err)

	started, err := manager.TestingCreateAndStartJobIfNoneExists(ctx)
	require.NoError(t, err)
	require.True(t, started)
}

func TestManagerCheckJobConditions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true, // disable the automatic job creation
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	ts := tc.Server(0)
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, `SET CLUSTER SETTING spanconfig.reconciliation_job.enabled = false;`)

	var interceptCount int32
	checkInterceptCountGreaterThan := func(min int32) int32 {
		var currentCount int32
		testutils.SucceedsSoon(t, func() error {
			if currentCount = atomic.LoadInt32(&interceptCount); !(currentCount > min) {
				return errors.Errorf("expected intercept count(=%d) > min(=%d)",
					currentCount, min)
			}
			return nil
		})
		return currentCount
	}
	manager := spanconfigmanager.New(
		ts.DB(),
		ts.JobRegistry().(*jobs.Registry),
		ts.InternalExecutor().(*sql.InternalExecutor),
		ts.Stopper(),
		ts.ClusterSettings(),
		ts.SpanConfigReconciler().(spanconfig.Reconciler),
		&spanconfig.TestingKnobs{
			ManagerDisableJobCreation: true,
			ManagerCheckJobInterceptor: func() {
				atomic.AddInt32(&interceptCount, 1)
			},
		},
	)
	var currentCount int32
	require.NoError(t, manager.Start(ctx))
	currentCount = checkInterceptCountGreaterThan(currentCount) // wait for an initial check

	tdb.Exec(t, `SET CLUSTER SETTING spanconfig.reconciliation_job.enabled = true;`)
	currentCount = checkInterceptCountGreaterThan(currentCount) // the job enablement setting triggers a check

	tdb.Exec(t, `SET CLUSTER SETTING spanconfig.reconciliation_job.check_interval = '25m'`)
	_ = checkInterceptCountGreaterThan(currentCount) // the job check interval setting triggers a check
}

// TestReconciliationJobIsIdle ensures that the reconciliation job, when
// resumed, is marked as idle.
func TestReconciliationJobIsIdle(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var jobID jobspb.JobID
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerCreatedJobInterceptor: func(jobI interface{}) {
						jobID = jobI.(*jobs.Job).ID()
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	jobRegistry := tc.Server(0).JobRegistry().(*jobs.Registry)
	testutils.SucceedsSoon(t, func() error {
		if jobID == jobspb.JobID(0) {
			return errors.New("waiting for reconciliation job to be started")
		}
		if !jobRegistry.TestingIsJobIdle(jobID) {
			return errors.New("expected reconciliation job to be idle")
		}
		return nil
	})
}
