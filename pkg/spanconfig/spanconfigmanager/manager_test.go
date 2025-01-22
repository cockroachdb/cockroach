// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigmanager_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigmanager"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SpanConfig: &spanconfig.TestingKnobs{
				ManagerDisableJobCreation: true, // disable the automatic job creation
			},
		},
	})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()

	unblocker := make(chan struct{})
	isBlocked := make(chan struct{})
	count := 0

	manager := spanconfigmanager.New(
		ts.InternalDB().(isql.DB),
		ts.JobRegistry().(*jobs.Registry),
		ts.AppStopper(),
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SpanConfig: &spanconfig.TestingKnobs{
				ManagerDisableJobCreation: true, // disable the automatic job creation
			},
		},
	})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()

	manager := spanconfigmanager.New(
		ts.InternalDB().(isql.DB),
		ts.JobRegistry().(*jobs.Registry),
		ts.AppStopper(),
		ts.ClusterSettings(),
		ts.SpanConfigReconciler().(spanconfig.Reconciler),
		&spanconfig.TestingKnobs{
			ManagerAfterCheckedReconciliationJobExistsInterceptor: func(exists bool) {
				require.False(t, exists)
			},
		},
	)

	payload, err := protoutil.Marshal(&jobspb.Payload{
		UsernameProto: username.RootUserName().EncodeProto(),
		Details:       jobspb.WrapPayloadDetails(jobspb.AutoSpanConfigReconciliationDetails{}),
	})
	require.NoError(t, err)

	id := ts.JobRegistry().(*jobs.Registry).MakeJobID()
	_, err = db.Exec(
		`INSERT INTO system.jobs (id, status) VALUES ($1, $2)`,
		id,
		jobs.StateFailed,
	)
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO system.job_info (job_id, info_key, value) VALUES ($1, $2, $3)`,
		id,
		jobs.GetLegacyPayloadKey(),
		payload,
	)
	require.NoError(t, err)

	started, err := manager.TestingCreateAndStartJobIfNoneExists(ctx)
	require.NoError(t, err)
	require.True(t, started)
}

func TestManagerCheckJobConditions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SpanConfig: &spanconfig.TestingKnobs{
				ManagerDisableJobCreation: true, // disable the automatic job creation
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	ts := srv.ApplicationLayer()
	tdb := sqlutils.MakeSQLRunner(db)
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
		ts.InternalDB().(isql.DB),
		ts.JobRegistry().(*jobs.Registry),
		ts.AppStopper(),
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
	defer log.Scope(t).Close(t)

	var jobID jobspb.JobID
	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(109457),

		Knobs: base.TestingKnobs{
			SpanConfig: &spanconfig.TestingKnobs{
				ManagerCreatedJobInterceptor: func(jobI interface{}) {
					jobID = jobI.(*jobs.Job).ID()
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()

	jobRegistry := ts.JobRegistry().(*jobs.Registry)
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

// TestReconciliationJobErrorAndRecovery tests that injecting an error into the
// reconciliation job fails the entire job (if bypassing the internal retry); if
// re-checked by the manager (and assuming we're no longer injecting an error),
// the job runs successfully. We also verify that when the job as a whole is
// bounced, we always run the full reconciliation process.
func TestReconciliationJobErrorAndRecovery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	mu := struct {
		syncutil.Mutex
		err         error
		lastStartTS hlc.Timestamp
	}{}

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SpanConfig: &spanconfig.TestingKnobs{
				ReconcilerInitialInterceptor: func(startTS hlc.Timestamp) {
					mu.Lock()
					defer mu.Unlock()
					mu.lastStartTS = startTS
				},
				ManagerDisableJobCreation:                      true, // disable the automatic job creation
				JobDisableInternalRetry:                        true,
				SQLWatcherCheckpointNoopsEveryDurationOverride: 100 * time.Millisecond,
				JobOnCheckpointInterceptor: func(_ hlc.Timestamp) error {
					mu.Lock()
					defer mu.Unlock()

					return mu.err
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()

	tdb := sqlutils.MakeSQLRunner(db)

	var jobID jobspb.JobID
	manager := spanconfigmanager.New(
		ts.InternalDB().(isql.DB),
		ts.JobRegistry().(*jobs.Registry),
		ts.AppStopper(),
		ts.ClusterSettings(),
		ts.SpanConfigReconciler().(spanconfig.Reconciler),
		&spanconfig.TestingKnobs{
			ManagerCreatedJobInterceptor: func(jobI interface{}) {
				jobID = jobI.(*jobs.Job).ID()
			},
		},
	)

	started, err := manager.TestingCreateAndStartJobIfNoneExists(ctx)
	require.NoError(t, err)
	require.True(t, started)

	testutils.SucceedsSoon(t, func() error {
		if jobID == jobspb.JobID(0) {
			return errors.New("waiting for reconciliation job to be started")
		}
		return nil
	})

	waitForJobCheckpoint(t, tdb)

	mu.Lock()
	mu.err = errors.New("injected")
	mu.Unlock()

	waitForJobState(t, tdb, jobID, jobs.StateFailed)

	mu.Lock()
	mu.err = nil
	mu.Unlock()

	started, err = manager.TestingCreateAndStartJobIfNoneExists(ctx)
	require.NoError(t, err)
	require.True(t, started)

	waitForJobState(t, tdb, jobID, jobs.StateRunning)

	mu.Lock()
	require.True(t, mu.lastStartTS.IsEmpty(), "expected reconciler to start with empty checkpoint")
	mu.Unlock()
}

// TestReconciliationUsesRightCheckpoint verifies that the reconciliation
// internal retry uses the right checkpoint during internal retries.
func TestReconciliationUsesRightCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	errCh := make(chan error)
	mu := struct {
		syncutil.Mutex
		lastStartTS hlc.Timestamp
	}{}

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SpanConfig: &spanconfig.TestingKnobs{
				ReconcilerInitialInterceptor: func(startTS hlc.Timestamp) {
					mu.Lock()
					defer mu.Unlock()
					mu.lastStartTS = startTS
				},
				ManagerDisableJobCreation:                      true, // disable the automatic job creation
				SQLWatcherCheckpointNoopsEveryDurationOverride: 10 * time.Millisecond,
				JobOnCheckpointInterceptor: func(_ hlc.Timestamp) error {
					select {
					case err := <-errCh:
						return err
					default:
						return nil
					}
				},
				JobOverrideRetryOptions: &retry.Options{ // for a faster test
					InitialBackoff: 10 * time.Millisecond,
					MaxBackoff:     10 * time.Millisecond,
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()

	tdb := sqlutils.MakeSQLRunner(db)
	tdb.Exec(t, `SET CLUSTER SETTING spanconfig.reconciliation_job.checkpoint_interval = '10ms'`)
	for _, l := range []serverutils.ApplicationLayerInterface{ts, srv.SystemLayer()} {
		closedts.TargetDuration.Override(ctx, &l.ClusterSettings().SV, 100*time.Millisecond)
	}

	manager := spanconfigmanager.New(
		ts.InternalDB().(isql.DB),
		ts.JobRegistry().(*jobs.Registry),
		ts.AppStopper(),
		ts.ClusterSettings(),
		ts.SpanConfigReconciler().(spanconfig.Reconciler),
		nil,
	)

	started, err := manager.TestingCreateAndStartJobIfNoneExists(ctx)
	require.NoError(t, err)
	require.True(t, started)

	waitForJobCheckpoint(t, tdb)

	mu.Lock()
	require.True(t, mu.lastStartTS.IsEmpty())
	mu.Unlock()

	// Force an internal retry.
	errCh <- errors.New("injected err")

	testutils.SucceedsSoon(t, func() error {
		mu.Lock()
		defer mu.Unlock()
		if mu.lastStartTS.IsEmpty() {
			return errors.New("expected reconciler to start with non-empty checkpoint")
		}
		return nil
	})

	// Force another internal retry, but one that's supposed to force a full
	// reconciliation pass.
	errCh <- spanconfig.NewMismatchedDescriptorTypesError(catalog.Database, catalog.Schema)

	testutils.SucceedsSoon(t, func() error {
		mu.Lock()
		defer mu.Unlock()
		if !mu.lastStartTS.IsEmpty() {
			return errors.New("expected reconciler to start with empty checkpoint")
		}
		return nil
	})
}

func waitForJobState(t *testing.T, tdb *sqlutils.SQLRunner, jobID jobspb.JobID, status jobs.State) {
	testutils.SucceedsSoon(t, func() error {
		var jobStatus string
		tdb.QueryRow(t, `SELECT status FROM system.jobs WHERE id = $1`, jobID).Scan(&jobStatus)

		if jobs.State(jobStatus) != status {
			return errors.Newf("expected jobID %d to have status %, got %s", jobID, status, jobStatus)
		}
		return nil
	})
}

func waitForJobCheckpoint(t *testing.T, tdb *sqlutils.SQLRunner) {
	testutils.SucceedsSoon(t, func() error {
		var progressBytes []byte
		tdb.QueryRow(t, `
SELECT progress FROM crdb_internal.system_jobs
  WHERE id = (SELECT job_id FROM [SHOW AUTOMATIC JOBS] WHERE job_type = 'AUTO SPAN CONFIG RECONCILIATION')
`).Scan(&progressBytes)

		var progress jobspb.Progress
		if err := protoutil.Unmarshal(progressBytes, &progress); err != nil {
			return err
		}
		sp, ok := progress.GetDetails().(*jobspb.Progress_AutoSpanConfigReconciliation)
		require.Truef(t, ok, "unexpected job progress type")
		if sp.AutoSpanConfigReconciliation.Checkpoint.IsEmpty() {
			return errors.New("waiting for span config reconciliation...")
		}

		return nil
	})
}
