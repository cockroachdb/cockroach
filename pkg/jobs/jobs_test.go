// Copyright 2017 The Cockroach Authors.
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
	gosql "database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/google/go-cmp/cmp"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// expectation defines the information necessary to determine the validity of
// a job in the system.jobs table.
type expectation struct {
	DB                *gosql.DB
	Record            jobs.Record
	Type              jobspb.Type
	Before            time.Time
	FractionCompleted float32
	Error             string
}

func (expected *expectation) verify(id *int64, expectedStatus jobs.Status) error {
	var statusString string
	var created time.Time
	var payloadBytes []byte
	var progressBytes []byte
	if err := expected.DB.QueryRow(
		`SELECT status, created, payload, progress FROM system.jobs WHERE id = $1`, id,
	).Scan(
		&statusString, &created, &payloadBytes, &progressBytes,
	); err != nil {
		return err
	}

	var payload jobspb.Payload
	if err := protoutil.Unmarshal(payloadBytes, &payload); err != nil {
		return err
	}
	var progress jobspb.Progress
	if err := protoutil.Unmarshal(progressBytes, &progress); err != nil {
		return err
	}

	// Verify the upstream-provided fields.
	details := payload.UnwrapDetails()
	progressDetail := progress.UnwrapDetails()

	if e, a := expected.Record, (jobs.Record{
		Description:   payload.Description,
		Details:       details,
		DescriptorIDs: payload.DescriptorIDs,
		Username:      payload.Username,
		Progress:      progressDetail,
	}); !reflect.DeepEqual(e, a) {
		diff := strings.Join(pretty.Diff(e, a), "\n")
		return errors.Errorf("Records do not match:\n%s", diff)
	}

	// Verify internally-managed fields.
	status := jobs.Status(statusString)
	if e, a := expectedStatus, status; e != a {
		return errors.Errorf("expected status %v, got %v", e, a)
	}
	if e, a := expected.Type, payload.Type(); e != a {
		return errors.Errorf("expected type %v, got type %v", e, a)
	}
	if e, a := expected.FractionCompleted, progress.GetFractionCompleted(); e != a {
		return errors.Errorf("expected fraction completed %f, got %f", e, a)
	}

	started := timeutil.FromUnixMicros(payload.StartedMicros)
	if started.Equal(timeutil.UnixEpoch) && status == jobs.StatusSucceeded {
		return errors.Errorf("started time is empty but job claims to be successful")
	}
	if status == jobs.StatusRunning || status == jobs.StatusPauseRequested {
		return nil
	}

	if e, a := expected.Error, payload.Error; e != a {
		return errors.Errorf("expected error %q, got %q", e, a)
	}
	return nil
}

func TestJobsTableProgressFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	var table, schema string
	sqlutils.MakeSQLRunner(db).QueryRow(t, `SHOW CREATE system.jobs`).Scan(&table, &schema)
	if !strings.Contains(schema, `FAMILY progress (progress)`) {
		t.Fatalf("expected progress family, got %q", schema)
	}
}

type counters struct {
	ResumeExit int
	// These sometimes retry so just use bool.
	ResumeStart, OnFailOrCancelStart, OnFailOrCancelExit, Success bool
}

type registryTestSuite struct {
	ctx         context.Context
	oldInterval time.Duration
	s           serverutils.TestServerInterface
	outerDB     *gosql.DB
	sqlDB       *sqlutils.SQLRunner
	registry    *jobs.Registry
	done        chan struct{}
	mockJob     jobs.Record
	job         *jobs.Job
	mu          struct {
		syncutil.Mutex
		a counters
		e counters
	}
	resumeCh            chan error
	progressCh          chan struct{}
	failOrCancelCh      chan error
	resumeCheckCh       chan struct{}
	failOrCancelCheckCh chan struct{}
	onPauseRequest      jobs.OnPauseRequestFunc
	// Instead of a ch for success, use a variable because it can retry since it
	// is in a transaction.
	successErr error
}

func noopPauseRequestFunc(
	ctx context.Context, planHookState interface{}, txn *kv.Txn, progress *jobspb.Progress,
) error {
	return nil
}

func (rts *registryTestSuite) setUp(t *testing.T) {
	rts.oldInterval = jobs.DefaultAdoptInterval
	jobs.DefaultAdoptInterval = time.Millisecond
	rts.ctx = context.Background()
	rts.s, rts.outerDB, _ = serverutils.StartServer(t, base.TestServerArgs{})
	rts.sqlDB = sqlutils.MakeSQLRunner(rts.outerDB)
	rts.registry = rts.s.JobRegistry().(*jobs.Registry)
	rts.done = make(chan struct{})
	rts.mockJob = jobs.Record{Details: jobspb.ImportDetails{}, Progress: jobspb.ImportProgress{}}

	rts.resumeCh = make(chan error)
	rts.progressCh = make(chan struct{})
	rts.failOrCancelCh = make(chan error)
	rts.resumeCheckCh = make(chan struct{})
	rts.failOrCancelCheckCh = make(chan struct{})
	rts.onPauseRequest = noopPauseRequestFunc

	jobs.RegisterConstructor(jobspb.TypeImport, func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return jobs.FakeResumer{
			OnResume: func(ctx context.Context, _ chan<- tree.Datums) error {
				t.Log("Starting resume")
				rts.mu.Lock()
				rts.mu.a.ResumeStart = true
				rts.mu.Unlock()
				defer func() {
					rts.mu.Lock()
					rts.mu.a.ResumeExit++
					rts.mu.Unlock()
					t.Log("Exiting resume")
				}()
				for {
					<-rts.resumeCheckCh
					select {
					case <-ctx.Done():
						rts.mu.Lock()
						rts.mu.a.ResumeExit--
						rts.mu.Unlock()
						return ctx.Err()
					case err := <-rts.resumeCh:
						return err
					case <-rts.progressCh:
						err := job.FractionProgressed(rts.ctx, jobs.FractionUpdater(0))
						if err != nil {
							return err
						}
					}
				}
			},
			FailOrCancel: func(ctx context.Context) error {
				t.Log("Starting FailOrCancel")
				rts.mu.Lock()
				rts.mu.a.OnFailOrCancelStart = true
				rts.mu.Unlock()
				<-rts.failOrCancelCheckCh
				select {
				case <-ctx.Done():
					rts.mu.Lock()
					rts.mu.a.OnFailOrCancelExit = false
					rts.mu.Unlock()
					return ctx.Err()
				case err := <-rts.failOrCancelCh:
					rts.mu.Lock()
					rts.mu.a.OnFailOrCancelExit = true
					rts.mu.Unlock()
					t.Log("Exiting OnFailOrCancel")
					return err
				}
			},

			Success: func() error {
				t.Log("Starting success")
				rts.mu.Lock()
				defer func() {
					rts.mu.Unlock()
					t.Log("Exiting success")
				}()
				rts.mu.a.Success = true
				return rts.successErr
			},
			PauseRequest: func(ctx context.Context, execCfg interface{}, txn *kv.Txn, progress *jobspb.Progress) error {
				return rts.onPauseRequest(ctx, execCfg, txn, progress)
			},
		}
	})
}

func (rts *registryTestSuite) tearDown() {
	close(rts.resumeCh)
	close(rts.progressCh)
	close(rts.resumeCheckCh)
	close(rts.done)
	rts.s.Stopper().Stop(rts.ctx)
	jobs.DefaultAdoptInterval = rts.oldInterval
	jobs.ResetConstructors()()
}

func (rts *registryTestSuite) check(t *testing.T, expectedStatus jobs.Status) {
	t.Helper()
	opts := retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		MaxBackoff:     time.Second,
		Multiplier:     2,
	}
	if err := retry.WithMaxAttempts(rts.ctx, opts, 10, func() error {
		rts.mu.Lock()
		defer rts.mu.Unlock()
		if diff := cmp.Diff(rts.mu.e, rts.mu.a); diff != "" {
			return errors.Errorf("unexpected diff: %s", diff)
		}
		if expectedStatus == "" {
			return nil
		}
		st, err := rts.job.CurrentStatus(rts.ctx)
		if err != nil {
			return err
		}
		if expectedStatus != st {
			return errors.Errorf("expected job status: %s but got: %s", expectedStatus, st)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryLifecycle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("normal success", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()

		j, _, err := rts.registry.CreateAndStartJob(rts.ctx, nil, rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)

		rts.resumeCh <- nil
		rts.mu.e.ResumeExit++
		rts.mu.e.Success = true
		rts.check(t, jobs.StatusSucceeded)
	})

	t.Run("create separately success", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()

		j, err := rts.registry.CreateJobWithTxn(rts.ctx, rts.mockJob, nil)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.check(t, jobs.StatusRunning)

		rts.resumeCheckCh <- struct{}{}
		rts.resumeCh <- nil
		rts.mu.e.ResumeExit++
		rts.mu.e.Success = true
		rts.check(t, jobs.StatusSucceeded)
	})

	t.Run("pause running", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()

		j, _, err := rts.registry.CreateAndStartJob(rts.ctx, nil, rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)

		rts.sqlDB.Exec(t, "PAUSE JOB $1", *j.ID())
		rts.check(t, jobs.StatusPaused)

		rts.sqlDB.Exec(t, "PAUSE JOB $1", *j.ID())
		rts.check(t, jobs.StatusPaused)

		rts.sqlDB.Exec(t, "RESUME JOB $1", *j.ID())
		rts.check(t, jobs.StatusRunning)

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)
		rts.resumeCh <- nil
		rts.mu.e.ResumeExit++

		rts.mu.e.Success = true
		rts.check(t, jobs.StatusSucceeded)
	})

	t.Run("pause reverting", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()

		j, _, err := rts.registry.CreateAndStartJob(rts.ctx, nil, rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)

		// Make Resume fail.
		rts.resumeCh <- errors.New("resume failed")
		rts.mu.e.ResumeExit++
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StatusReverting)

		rts.sqlDB.Exec(t, "PAUSE JOB $1", *j.ID())
		rts.check(t, jobs.StatusPaused)

		rts.sqlDB.Exec(t, "PAUSE JOB $1", *j.ID())
		rts.check(t, jobs.StatusPaused)

		rts.sqlDB.Exec(t, "RESUME JOB $1", *j.ID())
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StatusReverting)
		close(rts.failOrCancelCheckCh)

		rts.failOrCancelCh <- nil
		close(rts.failOrCancelCh)
		rts.mu.e.OnFailOrCancelExit = true
		rts.check(t, jobs.StatusFailed)
	})

	t.Run("cancel running", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()
		j, _, err := rts.registry.CreateAndStartJob(rts.ctx, nil, rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)

		rts.sqlDB.Exec(t, "CANCEL JOB $1", *j.ID())
		rts.mu.e.OnFailOrCancelStart = true
		rts.check(t, jobs.StatusReverting)

		rts.failOrCancelCheckCh <- struct{}{}
		close(rts.failOrCancelCheckCh)
		rts.failOrCancelCh <- nil
		close(rts.failOrCancelCh)
		rts.mu.e.OnFailOrCancelExit = true

		rts.check(t, jobs.StatusCanceled)
	})

	t.Run("cancel reverting", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()
		j, _, err := rts.registry.CreateAndStartJob(rts.ctx, nil, rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)

		rts.sqlDB.Exec(t, "CANCEL JOB $1", *j.ID())
		rts.mu.e.OnFailOrCancelStart = true
		rts.check(t, jobs.StatusReverting)

		rts.sqlDB.ExpectErr(t, "status reverting cannot be requested to be canceled", "CANCEL JOB $1", *j.ID())
		rts.check(t, jobs.StatusReverting)

		close(rts.failOrCancelCheckCh)
		close(rts.failOrCancelCh)
	})

	t.Run("cancel pause running", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()

		j, _, err := rts.registry.CreateAndStartJob(rts.ctx, nil, rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)

		rts.sqlDB.Exec(t, "PAUSE JOB $1", *j.ID())
		rts.check(t, jobs.StatusPaused)

		rts.sqlDB.Exec(t, "CANCEL JOB $1", *j.ID())
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		close(rts.failOrCancelCheckCh)
		rts.check(t, jobs.StatusReverting)

		rts.failOrCancelCh <- nil
		rts.mu.e.OnFailOrCancelExit = true
		close(rts.failOrCancelCh)
		rts.check(t, jobs.StatusCanceled)
	})

	t.Run("cancel pause reverting", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()

		j, _, err := rts.registry.CreateAndStartJob(rts.ctx, nil, rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)

		// Make Resume fail.
		rts.resumeCh <- errors.New("resume failed")
		rts.mu.e.ResumeExit++
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StatusReverting)

		rts.sqlDB.Exec(t, "PAUSE JOB $1", *j.ID())
		rts.check(t, jobs.StatusPaused)

		rts.sqlDB.ExpectErr(t, "paused and has non-nil FinalResumeError resume", "CANCEL JOB $1", *j.ID())
		rts.check(t, jobs.StatusPaused)

		rts.sqlDB.Exec(t, "RESUME JOB $1", *j.ID())
		rts.failOrCancelCheckCh <- struct{}{}
		close(rts.failOrCancelCheckCh)
		rts.check(t, jobs.StatusReverting)

		rts.failOrCancelCh <- nil
		close(rts.failOrCancelCh)
		rts.mu.e.OnFailOrCancelExit = true
		rts.check(t, jobs.StatusFailed)
	})

	// Verify that pause and cancel in a rollback do nothing.
	t.Run("rollback", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()
		job, _, err := rts.registry.CreateAndStartJob(rts.ctx, nil, rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = job

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)

		// Rollback a CANCEL.
		{
			txn, err := rts.outerDB.Begin()
			if err != nil {
				t.Fatal(err)
			}
			// OnFailOrCancel is *not* called in the same txn as the job is marked
			// cancel-requested and it will only be called when the job is adopted
			// again.
			if _, err := txn.Exec("CANCEL JOB $1", *job.ID()); err != nil {
				t.Fatal(err)
			}
			if err := txn.Rollback(); err != nil {
				t.Fatal(err)
			}
			rts.progressCh <- struct{}{}
			rts.resumeCheckCh <- struct{}{}
			rts.check(t, jobs.StatusRunning)
		}
		// Rollback a PAUSE.
		{
			txn, err := rts.outerDB.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := txn.Exec("PAUSE JOB $1", *job.ID()); err != nil {
				t.Fatal(err)
			}
			if err := txn.Rollback(); err != nil {
				t.Fatal(err)
			}
			rts.progressCh <- struct{}{}
			rts.resumeCheckCh <- struct{}{}
			rts.check(t, jobs.StatusRunning)
		}
		// Now pause it for reals.
		{
			txn, err := rts.outerDB.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := txn.Exec("PAUSE JOB $1", *job.ID()); err != nil {
				t.Fatal(err)
			}
			// Not committed yet, so state shouldn't have changed.
			// Don't check status in txn.
			rts.check(t, "")
			if err := txn.Commit(); err != nil {
				t.Fatal(err)
			}
			rts.check(t, jobs.StatusPaused)
		}
		// Rollback a RESUME.
		{
			txn, err := rts.outerDB.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := txn.Exec("RESUME JOB $1", *job.ID()); err != nil {
				t.Fatal(err)
			}
			if err := txn.Rollback(); err != nil {
				t.Fatal(err)
			}
			rts.check(t, jobs.StatusPaused)
		}
		// Commit a RESUME.
		{
			txn, err := rts.outerDB.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := txn.Exec("RESUME JOB $1", *job.ID()); err != nil {
				t.Fatal(err)
			}
			// Not committed yet, so state shouldn't have changed.
			// Don't check status in txn.
			rts.check(t, "")
			if err := txn.Commit(); err != nil {
				t.Fatal(err)
			}
		}
		rts.mu.e.ResumeStart = true
		rts.check(t, jobs.StatusRunning)
		rts.resumeCheckCh <- struct{}{}
		rts.resumeCh <- nil
		rts.mu.e.ResumeExit++
		rts.mu.e.Success = true
		rts.check(t, jobs.StatusSucceeded)
	})

	t.Run("failed running", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()

		j, _, err := rts.registry.CreateAndStartJob(rts.ctx, nil, rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.check(t, jobs.StatusRunning)

		rts.resumeCheckCh <- struct{}{}
		rts.resumeCh <- errors.New("resume failed")
		rts.mu.e.ResumeExit++
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		close(rts.failOrCancelCheckCh)
		rts.check(t, jobs.StatusReverting)

		rts.failOrCancelCh <- nil
		rts.mu.e.OnFailOrCancelExit = true
		close(rts.failOrCancelCh)
		rts.check(t, jobs.StatusFailed)
	})

	// Attempt to mark success, but fail, but fail that also.
	t.Run("fail marking success and fail OnFailOrCancel", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()

		// Make marking success fail.
		rts.successErr = errors.New("injected failure at marking as succeeded")
		j, _, err := rts.registry.CreateAndStartJob(rts.ctx, nil, rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)

		rts.resumeCh <- nil
		rts.mu.e.ResumeExit++
		rts.mu.e.Success = true
		rts.mu.e.OnFailOrCancelStart = true

		// The job is now in state reverting and will never resume again because
		// OnFailOrCancel also fails.
		rts.check(t, jobs.StatusReverting)
		rts.failOrCancelCheckCh <- struct{}{}
		rts.mu.e.OnFailOrCancelExit = true
		close(rts.failOrCancelCheckCh)
		rts.failOrCancelCh <- errors.New("injected failure while blocked in reverting")
		rts.check(t, jobs.StatusFailed)
	})

	// Fail the job, but also fail to mark it failed.
	t.Run("fail marking failed", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()

		// Make marking success fail.
		rts.successErr = errors.New("resume failed")
		j, _, err := rts.registry.CreateAndStartJob(rts.ctx, nil, rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)

		rts.resumeCh <- errors.New("resume failed")
		rts.mu.e.ResumeExit++
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		close(rts.failOrCancelCheckCh)
		// The job is now in state reverting and will never resume again.
		rts.check(t, jobs.StatusReverting)

		// But let it fail.
		rts.mu.e.OnFailOrCancelExit = true
		rts.failOrCancelCh <- errors.New("resume failed")
		rts.check(t, jobs.StatusFailed)
	})

	t.Run("OnPauseRequest", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()
		madeUpSpans := []roachpb.Span{
			{Key: roachpb.Key("foo")},
		}
		rts.onPauseRequest = func(ctx context.Context, planHookState interface{}, txn *kv.Txn, progress *jobspb.Progress) error {
			progress.GetImport().SpanProgress = madeUpSpans
			return nil
		}

		job, _, err := rts.registry.CreateAndStartJob(rts.ctx, nil, rts.mockJob)
		require.NoError(t, err)
		rts.job = job

		rts.resumeCheckCh <- struct{}{}
		rts.mu.e.ResumeStart = true
		rts.check(t, jobs.StatusRunning)

		// Request that the job is paused.
		pauseErrCh := make(chan error)
		go func() {
			_, err := rts.outerDB.Exec("PAUSE JOB $1", *job.ID())
			pauseErrCh <- err
		}()

		// Ensure that the pause went off without a problem.
		require.NoError(t, <-pauseErrCh)
		rts.check(t, jobs.StatusPaused)
		{
			// Make sure the side-effects of our pause function occurred.
			j, err := rts.registry.LoadJob(rts.ctx, *job.ID())
			require.NoError(t, err)
			progress := j.Progress()
			require.Equal(t, madeUpSpans, progress.GetImport().SpanProgress)
		}
	})
	t.Run("OnPauseRequest failure does not pause", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()

		rts.onPauseRequest = func(ctx context.Context, planHookState interface{}, txn *kv.Txn, progress *jobspb.Progress) error {
			return errors.New("boom")
		}

		job, _, err := rts.registry.CreateAndStartJob(rts.ctx, nil, rts.mockJob)
		require.NoError(t, err)
		rts.job = job

		// Allow the job to start.
		rts.resumeCheckCh <- struct{}{}
		rts.mu.e.ResumeStart = true
		rts.check(t, jobs.StatusRunning)

		// Request that the job is paused and ensure that the pause hit the error
		// and failed to pause.
		_, err = rts.outerDB.Exec("PAUSE JOB $1", *job.ID())
		require.Regexp(t, "boom", err)

		// Allow the job to complete.
		rts.resumeCh <- nil
		rts.mu.e.Success = true
		rts.mu.e.ResumeExit++
		rts.check(t, jobs.StatusSucceeded)
	})
}

func TestJobLifecycle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer jobs.ResetConstructors()()

	ctx := context.Background()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	registry := s.JobRegistry().(*jobs.Registry)

	createJob := func(record jobs.Record) (*jobs.Job, expectation) {
		beforeTime := timeutil.Now()
		job := registry.NewJob(record)
		if err := job.Created(ctx); err != nil {
			t.Fatal(err)
		}
		payload := job.Payload()
		return job, expectation{
			DB:     sqlDB,
			Record: record,
			Type:   payload.Type(),
			Before: beforeTime,
		}
	}

	defaultRecord := jobs.Record{
		// Job does not accept an empty Details field, so arbitrarily provide
		// ImportDetails.
		Details:  jobspb.ImportDetails{},
		Progress: jobspb.ImportProgress{},
	}

	createDefaultJob := func() (*jobs.Job, expectation) {
		return createJob(defaultRecord)
	}

	done := make(chan struct{})
	defer close(done)

	jobs.RegisterConstructor(jobspb.TypeImport, func(_ *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return jobs.FakeResumer{
			OnResume: func(ctx context.Context, _ chan<- tree.Datums) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-done:
					return nil
				}
			},
		}
	})

	startLeasedJob := func(t *testing.T, record jobs.Record) (*jobs.Job, expectation) {
		beforeTime := timeutil.Now()
		job, _, err := registry.CreateAndStartJob(ctx, nil, record)
		if err != nil {
			t.Fatal(err)
		}
		payload := job.Payload()
		return job, expectation{
			DB:     sqlDB,
			Record: record,
			Type:   payload.Type(),
			Before: beforeTime,
		}
	}

	t.Run("valid job lifecycles succeed", func(t *testing.T) {
		// Woody is a successful job.
		woodyJob, woodyExp := createJob(jobs.Record{
			Description:   "There's a snake in my boot!",
			Username:      "Woody Pride",
			DescriptorIDs: []sqlbase.ID{1, 2, 3},
			Details:       jobspb.RestoreDetails{},
			Progress:      jobspb.RestoreProgress{},
		})

		if err := woodyExp.verify(woodyJob.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}

		if err := woodyJob.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := woodyExp.verify(woodyJob.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}

		// This fraction completed progression tests that calling Progressed with a
		// fractionCompleted that is less than the last-recorded fractionCompleted
		// is observed.
		progresses := []struct {
			actual   float32
			expected float32
		}{
			{0.0, 0.0}, {0.5, 0.5}, {0.5, 0.5}, {0.4, 0.4}, {0.8, 0.8}, {1.0, 1.0},
		}
		for _, f := range progresses {
			if err := woodyJob.FractionProgressed(ctx, jobs.FractionUpdater(f.actual)); err != nil {
				t.Fatal(err)
			}
			woodyExp.FractionCompleted = f.expected
			if err := woodyExp.verify(woodyJob.ID(), jobs.StatusRunning); err != nil {
				t.Fatal(err)
			}
		}

		// Test Progressed callbacks.
		if err := woodyJob.FractionProgressed(ctx, func(_ context.Context, details jobspb.ProgressDetails) float32 {
			details.(*jobspb.Progress_Restore).Restore.HighWater = roachpb.Key("mariana")
			return 1.0
		}); err != nil {
			t.Fatal(err)
		}
		woodyExp.Record.Progress = jobspb.RestoreProgress{HighWater: roachpb.Key("mariana")}
		if err := woodyExp.verify(woodyJob.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}

		if err := woodyJob.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := woodyExp.verify(woodyJob.ID(), jobs.StatusSucceeded); err != nil {
			t.Fatal(err)
		}

		// Buzz fails after it starts running.
		buzzRecord := jobs.Record{
			Description:   "To infinity and beyond!",
			Username:      "Buzz Lightyear",
			DescriptorIDs: []sqlbase.ID{3, 2, 1},
			Details:       jobspb.BackupDetails{},
			Progress:      jobspb.BackupProgress{},
		}
		buzzExp := expectation{
			DB:     sqlDB,
			Record: buzzRecord,
			Type:   jobspb.TypeBackup,
			Before: timeutil.Now(),
			Error:  "Buzz Lightyear can't fly",
		}
		buzzJob := registry.NewJob(buzzRecord)

		if err := buzzJob.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := buzzExp.verify(buzzJob.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}

		if err := buzzJob.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := buzzExp.verify(buzzJob.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}

		if err := buzzJob.FractionProgressed(ctx, jobs.FractionUpdater(.42)); err != nil {
			t.Fatal(err)
		}
		buzzExp.FractionCompleted = .42
		if err := buzzExp.verify(buzzJob.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}

		if err := buzzJob.Failed(ctx, errors.New("Buzz Lightyear can't fly")); err != nil {
			t.Fatal(err)
		}
		if err := buzzExp.verify(buzzJob.ID(), jobs.StatusFailed); err != nil {
			t.Fatal(err)
		}

		// Ensure that logging Buzz didn't corrupt Woody.
		if err := woodyExp.verify(woodyJob.ID(), jobs.StatusSucceeded); err != nil {
			t.Fatal(err)
		}

		// Sid fails before it starts running.
		sidJob, sidExp := createJob(jobs.Record{
			Description:   "The toys! The toys are alive!",
			Username:      "Sid Phillips",
			DescriptorIDs: []sqlbase.ID{6, 6, 6},
			Details:       jobspb.RestoreDetails{},
			Progress:      jobspb.RestoreProgress{},
		})

		if err := sidExp.verify(sidJob.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}

		if err := sidJob.Failed(ctx, errors.New("Sid is a total failure")); err != nil {
			t.Fatal(err)
		}
		sidExp.Error = "Sid is a total failure"
		if err := sidExp.verify(sidJob.ID(), jobs.StatusFailed); err != nil {
			t.Fatal(err)
		}

		// Ensure that logging Sid didn't corrupt Woody or Buzz.
		if err := woodyExp.verify(woodyJob.ID(), jobs.StatusSucceeded); err != nil {
			t.Fatal(err)
		}
		if err := buzzExp.verify(buzzJob.ID(), jobs.StatusFailed); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("FinishedWith", func(t *testing.T) {
		t.Run("nil error marks job as successful", func(t *testing.T) {
			job, exp := createDefaultJob()
			exp.FractionCompleted = 1.0
			if err := job.Started(ctx); err != nil {
				t.Fatal(err)
			}
			if err := job.Succeeded(ctx); err != nil {
				t.Fatal(err)
			}
			if err := exp.verify(job.ID(), jobs.StatusSucceeded); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("non-nil error marks job as failed", func(t *testing.T) {
			job, exp := createDefaultJob()
			boom := errors.New("boom")
			exp.Error = boom.Error()
			if err := job.Failed(ctx, boom); err != nil {
				t.Fatal(err)
			}
			if err := exp.verify(job.ID(), jobs.StatusFailed); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("internal errors are not swallowed if marking job as successful", func(t *testing.T) {
			job, _ := createDefaultJob()
			if _, err := sqlDB.Exec(
				`UPDATE system.jobs SET payload = 'garbage' WHERE id = $1`, *job.ID(),
			); err != nil {
				t.Fatal(err)
			}
			if err := job.Succeeded(ctx); !testutils.IsError(err, "wrong wireType") {
				t.Fatalf("unexpected: %v", err)
			}
		})

		t.Run("internal errors are not swallowed if marking job as failed", func(t *testing.T) {
			job, _ := createDefaultJob()
			if _, err := sqlDB.Exec(
				`UPDATE system.jobs SET payload = 'garbage' WHERE id = $1`, *job.ID(),
			); err != nil {
				t.Fatal(err)
			}
			if err := job.Failed(ctx, errors.New("boom")); !testutils.IsError(err, "wrong wireType") {
				t.Fatalf("unexpected: %v", err)
			}
		})
	})

	t.Run("cancelable jobs can be paused until finished", func(t *testing.T) {
		job, exp := startLeasedJob(t, defaultRecord)

		if err := registry.PauseRequested(ctx, nil, *job.ID()); err != nil {
			t.Fatal(err)
		}
		if err := job.Paused(ctx); err != nil {
			t.Fatal(err)
		}
		if err := exp.verify(job.ID(), jobs.StatusPaused); err != nil {
			t.Fatal(err)
		}
		if err := registry.Unpause(ctx, nil, *job.ID()); err != nil {
			t.Fatal(err)
		}
		// Resume the job again to ensure that the resumption is idempotent.
		if err := registry.Unpause(ctx, nil, *job.ID()); err != nil {
			t.Fatal(err)
		}
		if err := exp.verify(job.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}

		// PauseRequested fails after job is successful.
		if err := job.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := registry.PauseRequested(ctx, nil, *job.ID()); !testutils.IsError(err, "cannot be requested to be paused") {
			t.Fatalf("expected 'cannot pause succeeded job', but got '%s'", err)
		}
	})

	t.Run("cancelable jobs can be canceled until finished", func(t *testing.T) {
		{
			job, exp := startLeasedJob(t, defaultRecord)
			if err := registry.CancelRequested(ctx, nil, *job.ID()); err != nil {
				t.Fatal(err)
			}
			if err := exp.verify(job.ID(), jobs.StatusCancelRequested); err != nil {
				t.Fatal(err)
			}
		}

		{
			job, exp := startLeasedJob(t, defaultRecord)
			if err := job.Started(ctx); err != nil {
				t.Fatal(err)
			}
			if err := registry.CancelRequested(ctx, nil, *job.ID()); err != nil {
				t.Fatal(err)
			}
			if err := exp.verify(job.ID(), jobs.StatusCancelRequested); err != nil {
				t.Fatal(err)
			}
		}

		{
			job, exp := startLeasedJob(t, defaultRecord)
			if err := registry.PauseRequested(ctx, nil, *job.ID()); err != nil {
				t.Fatal(err)
			}
			if err := job.Paused(ctx); err != nil {
				t.Fatal(err)
			}
			if err := registry.CancelRequested(ctx, nil, *job.ID()); err != nil {
				t.Fatal(err)
			}
			if err := exp.verify(job.ID(), jobs.StatusCancelRequested); err != nil {
				t.Fatal(err)
			}
		}

		{
			job, _ := startLeasedJob(t, defaultRecord)
			if err := job.Succeeded(ctx); err != nil {
				t.Fatal(err)
			}
			expectedErr := "job with status succeeded cannot be requested to be canceled"
			if err := registry.CancelRequested(ctx, nil, *job.ID()); !testutils.IsError(err, expectedErr) {
				t.Fatalf("expected '%s', but got '%s'", expectedErr, err)
			}
		}
	})

	t.Run("unpaused jobs cannot be resumed", func(t *testing.T) {
		{
			job, _ := startLeasedJob(t, defaultRecord)
			if err := registry.CancelRequested(ctx, nil, *job.ID()); err != nil {
				t.Fatal(err)
			}
			if err := registry.Unpause(ctx, nil, *job.ID()); !testutils.IsError(err, "cannot be resumed") {
				t.Errorf("got unexpected status '%v'", err)
			}
		}

		{
			job, _ := startLeasedJob(t, defaultRecord)
			if err := job.Succeeded(ctx); err != nil {
				t.Fatal(err)
			}
			expectedErr := fmt.Sprintf("job with status %s cannot be resumed", jobs.StatusSucceeded)
			if err := registry.Unpause(ctx, nil, *job.ID()); !testutils.IsError(err, expectedErr) {
				t.Errorf("expected '%s', but got '%v'", expectedErr, err)
			}
		}
	})

	t.Run("bad job details fail", func(t *testing.T) {
		defer func() {
			if r, ok := recover().(string); !ok || !strings.Contains(r, "unknown details type int") {
				t.Fatalf("expected 'unknown details type int', but got: %v", r)
			}
		}()

		job := registry.NewJob(jobs.Record{
			Details: 42,
		})
		_ = job.Created(ctx)
	})

	t.Run("update before create fails", func(t *testing.T) {
		job := registry.NewJob(jobs.Record{
			Details:  jobspb.RestoreDetails{},
			Progress: jobspb.RestoreProgress{},
		})
		if err := job.Started(ctx); !testutils.IsError(err, "job not created") {
			t.Fatalf("expected 'job not created' error, but got %v", err)
		}
	})

	t.Run("same state transition twice succeeds silently", func(t *testing.T) {
		job, _ := createDefaultJob()
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("high-water progress works", func(t *testing.T) {
		job, _ := createDefaultJob()
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		highWaters := []hlc.Timestamp{
			{WallTime: 1, Logical: 1},
			{WallTime: 2, Logical: 0},
		}
		for _, ts := range highWaters {
			if err := job.HighWaterProgressed(
				ctx, func(context.Context, *kv.Txn, jobspb.ProgressDetails) (hlc.Timestamp, error) { return ts, nil },
			); err != nil {
				t.Fatal(err)
			}
			p := job.Progress()
			if actual := *p.GetHighWater(); actual != ts {
				t.Fatalf(`got %s expected %s`, actual, ts)
			}
		}
	})

	t.Run("out of bounds progress fails", func(t *testing.T) {
		job, _ := createDefaultJob()
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.FractionProgressed(ctx, jobs.FractionUpdater(-0.1)); !testutils.IsError(err, "outside allowable range") {
			t.Fatalf("expected 'outside allowable range' error, but got %v", err)
		}
		if err := job.FractionProgressed(ctx, jobs.FractionUpdater(1.1)); !testutils.IsError(err, "outside allowable range") {
			t.Fatalf("expected 'outside allowable range' error, but got %v", err)
		}
		if err := job.HighWaterProgressed(
			ctx, func(context.Context, *kv.Txn, jobspb.ProgressDetails) (hlc.Timestamp, error) {
				return hlc.Timestamp{WallTime: -1}, nil
			},
		); !testutils.IsError(err, "outside allowable range") {
			t.Fatalf("expected 'outside allowable range' error, but got %v", err)
		}
	})

	t.Run("error propagates", func(t *testing.T) {
		job, _ := createDefaultJob()
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.HighWaterProgressed(
			ctx, func(context.Context, *kv.Txn, jobspb.ProgressDetails) (hlc.Timestamp, error) {
				return hlc.Timestamp{WallTime: 2}, errors.Errorf("boom")
			},
		); !testutils.IsError(err, "boom") {
			t.Fatalf("expected 'boom' error, but got %v", err)
		}
	})

	t.Run("progress on finished job fails", func(t *testing.T) {
		job, _ := createDefaultJob()
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.FractionProgressed(ctx, jobs.FractionUpdater(0.5)); !testutils.IsError(
			err, `cannot update progress on succeeded job \(id \d+\)`,
		) {
			t.Fatalf("expected 'cannot update progress' error, but got %v", err)
		}
	})

	t.Run("progress on paused job fails", func(t *testing.T) {
		job, _ := startLeasedJob(t, defaultRecord)
		if err := registry.PauseRequested(ctx, nil, *job.ID()); err != nil {
			t.Fatal(err)
		}
		if err := job.FractionProgressed(ctx, jobs.FractionUpdater(0.5)); !testutils.IsError(
			err, `cannot update progress on pause-requested job`,
		) {
			t.Fatalf("expected progress error, but got %v", err)
		}
	})

	t.Run("progress on canceled job fails", func(t *testing.T) {
		job, _ := startLeasedJob(t, defaultRecord)
		if err := registry.CancelRequested(ctx, nil, *job.ID()); err != nil {
			t.Fatal(err)
		}
		if err := job.FractionProgressed(ctx, jobs.FractionUpdater(0.5)); !testutils.IsError(
			err, `cannot update progress on cancel-requested job \(id \d+\)`,
		) {
			t.Fatalf("expected progress error, but got %v", err)
		}
	})

	t.Run("succeeded forces fraction completed to 1.0", func(t *testing.T) {
		job, exp := createDefaultJob()
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.FractionProgressed(ctx, jobs.FractionUpdater(0.2)); err != nil {
			t.Fatal(err)
		}
		if err := job.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		exp.FractionCompleted = 1.0
		if err := exp.verify(job.ID(), jobs.StatusSucceeded); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("set details works", func(t *testing.T) {
		job, exp := createJob(jobs.Record{
			Details:  jobspb.RestoreDetails{},
			Progress: jobspb.RestoreProgress{},
		})
		if err := exp.verify(job.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}
		newDetails := jobspb.RestoreDetails{URIs: []string{"new"}}
		exp.Record.Details = newDetails
		if err := job.SetDetails(ctx, newDetails); err != nil {
			t.Fatal(err)
		}
		if err := exp.verify(job.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("set progress works", func(t *testing.T) {
		job, exp := createJob(jobs.Record{
			Details:  jobspb.RestoreDetails{},
			Progress: jobspb.RestoreProgress{},
		})
		if err := exp.verify(job.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}
		newDetails := jobspb.RestoreProgress{HighWater: []byte{42}}
		exp.Record.Progress = newDetails
		if err := job.SetProgress(ctx, newDetails); err != nil {
			t.Fatal(err)
		}
		if err := exp.verify(job.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}
	})

}

// TestShowJobs manually inserts a row into system.jobs and checks that the
// encoded protobuf payload is properly decoded and visible in
// crdb_internal.jobs.
func TestShowJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, rawSQLDB, _ := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(rawSQLDB)
	defer s.Stopper().Stop(context.Background())

	// row represents a row returned from crdb_internal.jobs, but
	// *not* a row in system.jobs.
	type row struct {
		id                int64
		typ               string
		status            string
		description       string
		username          string
		err               string
		created           time.Time
		started           time.Time
		finished          time.Time
		modified          time.Time
		fractionCompleted float32
		highWater         hlc.Timestamp
		coordinatorID     roachpb.NodeID
		details           jobspb.Details
	}

	for _, in := range []row{
		{
			id:          42,
			typ:         "SCHEMA CHANGE",
			status:      "superfailed",
			description: "failjob",
			username:    "failure",
			err:         "boom",
			// lib/pq returns time.Time objects with goofy locations, which breaks
			// reflect.DeepEqual without this time.FixedZone song and dance.
			// See: https://github.com/lib/pq/issues/329
			created:           timeutil.Unix(1, 0).In(time.FixedZone("", 0)),
			started:           timeutil.Unix(2, 0).In(time.FixedZone("", 0)),
			finished:          timeutil.Unix(3, 0).In(time.FixedZone("", 0)),
			modified:          timeutil.Unix(4, 0).In(time.FixedZone("", 0)),
			fractionCompleted: 0.42,
			coordinatorID:     7,
			details:           jobspb.SchemaChangeDetails{},
		},
		{
			id:          43,
			typ:         "CHANGEFEED",
			status:      "running",
			description: "persistent feed",
			username:    "persistent",
			err:         "",
			// lib/pq returns time.Time objects with goofy locations, which breaks
			// reflect.DeepEqual without this time.FixedZone song and dance.
			// See: https://github.com/lib/pq/issues/329
			created:  timeutil.Unix(1, 0).In(time.FixedZone("", 0)),
			started:  timeutil.Unix(2, 0).In(time.FixedZone("", 0)),
			finished: timeutil.Unix(3, 0).In(time.FixedZone("", 0)),
			modified: timeutil.Unix(4, 0).In(time.FixedZone("", 0)),
			highWater: hlc.Timestamp{
				WallTime: 1533143242000000,
				Logical:  4,
			},
			coordinatorID: 7,
			details:       jobspb.ChangefeedDetails{},
		},
	} {
		t.Run("", func(t *testing.T) {
			// system.jobs is part proper SQL columns, part protobuf, so we can't use the
			// row struct directly.
			inPayload, err := protoutil.Marshal(&jobspb.Payload{
				Description:    in.description,
				StartedMicros:  in.started.UnixNano() / time.Microsecond.Nanoseconds(),
				FinishedMicros: in.finished.UnixNano() / time.Microsecond.Nanoseconds(),
				Username:       in.username,
				Lease: &jobspb.Lease{
					NodeID: 7,
				},
				Error:   in.err,
				Details: jobspb.WrapPayloadDetails(in.details),
			})
			if err != nil {
				t.Fatal(err)
			}

			progress := &jobspb.Progress{
				ModifiedMicros: in.modified.UnixNano() / time.Microsecond.Nanoseconds(),
			}
			if in.highWater != (hlc.Timestamp{}) {
				progress.Progress = &jobspb.Progress_HighWater{
					HighWater: &in.highWater,
				}
			} else {
				progress.Progress = &jobspb.Progress_FractionCompleted{
					FractionCompleted: in.fractionCompleted,
				}
			}
			inProgress, err := protoutil.Marshal(progress)
			if err != nil {
				t.Fatal(err)
			}
			sqlDB.Exec(t,
				`INSERT INTO system.jobs (id, status, created, payload, progress) VALUES ($1, $2, $3, $4, $5)`,
				in.id, in.status, in.created, inPayload, inProgress,
			)

			var out row
			var maybeFractionCompleted *float32
			var decimalHighWater *apd.Decimal
			sqlDB.QueryRow(t, `
      SELECT job_id, job_type, status, created, description, started, finished, modified,
             fraction_completed, high_water_timestamp, user_name, ifnull(error, ''), coordinator_id
        FROM crdb_internal.jobs WHERE job_id = $1`, in.id).Scan(
				&out.id, &out.typ, &out.status, &out.created, &out.description, &out.started,
				&out.finished, &out.modified, &maybeFractionCompleted, &decimalHighWater, &out.username,
				&out.err, &out.coordinatorID,
			)

			if decimalHighWater != nil {
				var err error
				out.highWater, err = tree.DecimalToHLC(decimalHighWater)
				if err != nil {
					t.Fatal(err)
				}
			}

			if maybeFractionCompleted != nil {
				out.fractionCompleted = *maybeFractionCompleted
			}

			// details field is not explicitly checked for equality; its value is
			// confirmed via the job_type field, which is dependent on the details
			// field.
			out.details = in.details

			if !reflect.DeepEqual(in, out) {
				diff := strings.Join(pretty.Diff(in, out), "\n")
				t.Fatalf("in job did not match out job:\n%s", diff)
			}
		})
	}
}

func TestShowAutomaticJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, rawSQLDB, _ := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(rawSQLDB)
	defer s.Stopper().Stop(context.Background())

	// row represents a row returned from crdb_internal.jobs, but
	// *not* a row in system.jobs.
	type row struct {
		id      int64
		typ     string
		status  string
		details jobspb.Details
	}

	rows := []row{
		{
			id:      1,
			typ:     "CREATE STATS",
			status:  "running",
			details: jobspb.CreateStatsDetails{Name: "my_stats"},
		},
		{
			id:      2,
			typ:     "AUTO CREATE STATS",
			status:  "running",
			details: jobspb.CreateStatsDetails{Name: "__auto__"},
		},
	}

	for _, in := range rows {
		// system.jobs is part proper SQL columns, part protobuf, so we can't use the
		// row struct directly.
		inPayload, err := protoutil.Marshal(&jobspb.Payload{
			Details: jobspb.WrapPayloadDetails(in.details),
		})
		if err != nil {
			t.Fatal(err)
		}

		sqlDB.Exec(t,
			`INSERT INTO system.jobs (id, status, payload) VALUES ($1, $2, $3)`,
			in.id, in.status, inPayload,
		)
	}

	var out row

	sqlDB.QueryRow(t, `SELECT job_id, job_type FROM [SHOW JOB 1]`).Scan(&out.id, &out.typ)
	if out.id != 1 || out.typ != "CREATE STATS" {
		t.Fatalf("Expected id:%d and type:%s but found id:%d and type:%s",
			1, "CREATE STATS", out.id, out.typ)
	}

	sqlDB.QueryRow(t, `SELECT job_id, job_type FROM [SHOW JOBS SELECT 1]`).Scan(&out.id, &out.typ)
	if out.id != 1 || out.typ != "CREATE STATS" {
		t.Fatalf("Expected id:%d and type:%s but found id:%d and type:%s",
			1, "CREATE STATS", out.id, out.typ)
	}

	sqlDB.QueryRow(t, `SELECT job_id, job_type FROM [SHOW JOBS (SELECT 1)]`).Scan(&out.id, &out.typ)
	if out.id != 1 || out.typ != "CREATE STATS" {
		t.Fatalf("Expected id:%d and type:%s but found id:%d and type:%s",
			1, "CREATE STATS", out.id, out.typ)
	}
	sqlDB.QueryRow(t, `SELECT job_id, job_type FROM [SHOW JOB 2]`).Scan(&out.id, &out.typ)
	if out.id != 2 || out.typ != "AUTO CREATE STATS" {
		t.Fatalf("Expected id:%d and type:%s but found id:%d and type:%s",
			2, "AUTO CREATE STATS", out.id, out.typ)
	}

	sqlDB.QueryRow(t, `SELECT job_id, job_type FROM [SHOW JOBS SELECT 2]`).Scan(&out.id, &out.typ)
	if out.id != 2 || out.typ != "AUTO CREATE STATS" {
		t.Fatalf("Expected id:%d and type:%s but found id:%d and type:%s",
			2, "AUTO CREATE STATS", out.id, out.typ)
	}

	sqlDB.QueryRow(t, `SELECT job_id, job_type FROM [SHOW JOBS]`).Scan(&out.id, &out.typ)
	if out.id != 1 || out.typ != "CREATE STATS" {
		t.Fatalf("Expected id:%d and type:%s but found id:%d and type:%s",
			1, "CREATE STATS", out.id, out.typ)
	}

	sqlDB.QueryRow(t, `SELECT job_id, job_type FROM [SHOW AUTOMATIC JOBS]`).Scan(&out.id, &out.typ)
	if out.id != 2 || out.typ != "AUTO CREATE STATS" {
		t.Fatalf("Expected id:%d and type:%s but found id:%d and type:%s",
			2, "AUTO CREATE STATS", out.id, out.typ)
	}
}

func TestShowJobsWithError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	// Create at least 6 rows, ensuring 3 rows are corrupted.
	// Ensure there is at least one row in system.jobs.
	if _, err := sqlDB.Exec(`
     CREATE TABLE foo(x INT); ALTER TABLE foo ADD COLUMN y INT;
	`); err != nil {
		t.Fatal(err)
	}
	// Get the id of the ADD COLUMN job to use later.
	var jobID int64
	if err := sqlDB.QueryRow(`SELECT id FROM system.jobs ORDER BY id DESC LIMIT 1`).Scan(&jobID); err != nil {
		t.Fatal(err)
	}

	// Now insert more rows based on the valid row, some of which are corrupted.
	if _, err := sqlDB.Exec(`
     -- Create a corrupted payload field from the most recent row.
     INSERT INTO system.jobs(id, status, payload, progress) SELECT id+1, status, '\xaaaa'::BYTES, progress FROM system.jobs WHERE id = $1;
	`, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`
     -- Create a corrupted progress field.
     INSERT INTO system.jobs(id, status, payload, progress) SELECT id+2, status, payload, '\xaaaa'::BYTES FROM system.jobs WHERE id = $1; 
	`, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`
     -- Corrupt both fields.
     INSERT INTO system.jobs(id, status, payload, progress) SELECT id+3, status, '\xaaaa'::BYTES, '\xaaaa'::BYTES FROM system.jobs WHERE id = $1;
	`, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`
     -- Test what happens with a NULL progress field (which is a valid value).
     INSERT INTO system.jobs(id, status, payload, progress) SELECT id+4, status, payload, NULL::BYTES FROM system.jobs WHERE id = $1;
	`, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`
     -- Test what happens with a NULL progress field (which is a valid value).
     INSERT INTO system.jobs(id, status, payload, progress) SELECT id+5, status, '\xaaaa'::BYTES, NULL::BYTES FROM system.jobs WHERE id = $1;
	`, jobID); err != nil {
		t.Fatal(err)
	}

	// Extract the last 6 rows from the query.
	rows, err := sqlDB.Query(`
  WITH a AS (SELECT job_id, description, fraction_completed, error FROM [SHOW JOBS] ORDER BY job_id DESC LIMIT 6)
  SELECT ifnull(description, 'NULL'), ifnull(fraction_completed, -1)::string, ifnull(error,'NULL') FROM a ORDER BY job_id ASC`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var desc, frac, errStr string

	// Valid row.
	rowNum := 0
	if !rows.Next() {
		t.Fatalf("%d too few rows", rowNum)
	}
	if err := rows.Scan(&desc, &frac, &errStr); err != nil {
		t.Fatalf("%d: %v", rowNum, err)
	}
	t.Logf("row %d: %q %q %v", rowNum, desc, errStr, frac)
	if desc == "NULL" || errStr != "" || frac[0] == '-' {
		t.Fatalf("%d: invalid row", rowNum)
	}
	rowNum++

	// Corrupted payload but valid progress.
	if !rows.Next() {
		t.Fatalf("%d: too few rows", rowNum)
	}
	if err := rows.Scan(&desc, &frac, &errStr); err != nil {
		t.Fatalf("%d: %v", rowNum, err)
	}
	t.Logf("row %d: %q %q %v", rowNum, desc, errStr, frac)
	if desc != "NULL" || !strings.HasPrefix(errStr, "error decoding payload") || frac[0] == '-' {
		t.Fatalf("%d: invalid row", rowNum)
	}
	rowNum++

	// Corrupted progress but valid payload.
	if !rows.Next() {
		t.Fatalf("%d: too few rows", rowNum)
	}
	if err := rows.Scan(&desc, &frac, &errStr); err != nil {
		t.Fatalf("%d: %v", rowNum, err)
	}
	t.Logf("row %d: %q %q %v", rowNum, desc, errStr, frac)
	if desc == "NULL" || !strings.HasPrefix(errStr, "error decoding progress") || frac[0] != '-' {
		t.Fatalf("%d: invalid row", rowNum)
	}
	rowNum++

	// Both payload and progress corrupted.
	if !rows.Next() {
		t.Fatalf("%d: too few rows", rowNum)
	}
	if err := rows.Scan(&desc, &frac, &errStr); err != nil {
		t.Fatalf("%d: %v", rowNum, err)
	}
	t.Logf("row: %q %q %v", desc, errStr, frac)
	if desc != "NULL" ||
		!strings.Contains(errStr, "error decoding payload") ||
		!strings.Contains(errStr, "error decoding progress") ||
		frac[0] != '-' {
		t.Fatalf("%d: invalid row", rowNum)
	}
	rowNum++

	// Valid payload and missing progress.
	if !rows.Next() {
		t.Fatalf("%d too few rows", rowNum)
	}
	if err := rows.Scan(&desc, &frac, &errStr); err != nil {
		t.Fatalf("%d: %v", rowNum, err)
	}
	t.Logf("row %d: %q %q %v", rowNum, desc, errStr, frac)
	if desc == "NULL" || errStr != "" || frac[0] != '-' {
		t.Fatalf("%d: invalid row", rowNum)
	}
	rowNum++

	// Invalid payload and missing progress.
	if !rows.Next() {
		t.Fatalf("%d too few rows", rowNum)
	}
	if err := rows.Scan(&desc, &frac, &errStr); err != nil {
		t.Fatalf("%d: %v", rowNum, err)
	}
	t.Logf("row %d: %q %q %v", rowNum, desc, errStr, frac)
	if desc != "NULL" ||
		!strings.Contains(errStr, "error decoding payload") ||
		strings.Contains(errStr, "error decoding progress") ||
		frac[0] != '-' {
		t.Fatalf("%d: invalid row", rowNum)
	}
	rowNum++
}

func TestShowJobWhenComplete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Canceling a job relies on adopt daemon to move the job to state
	// reverting.
	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 10 * time.Millisecond
	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	registry := s.JobRegistry().(*jobs.Registry)
	mockJob := jobs.Record{Details: jobspb.ImportDetails{}, Progress: jobspb.ImportProgress{}}
	done := make(chan struct{})
	defer close(done)
	jobs.RegisterConstructor(
		jobspb.TypeImport, func(_ *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return jobs.FakeResumer{
				OnResume: func(ctx context.Context, _ chan<- tree.Datums) error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-done:
						return nil
					}
				},
			}
		})

	type row struct {
		id     int64
		status string
	}
	var out row

	t.Run("show job", func(t *testing.T) {
		// Start a job and cancel it so it is in state finished and then query it with
		// SHOW JOB WHEN COMPLETE.
		job, _, err := registry.CreateAndStartJob(ctx, nil, mockJob)
		if err != nil {
			t.Fatal(err)
		}
		group := ctxgroup.WithContext(ctx)
		group.GoCtx(func(ctx context.Context) error {
			if err := db.QueryRowContext(
				ctx,
				`SELECT job_id, status
				 FROM [SHOW JOB WHEN COMPLETE $1]`,
				*job.ID()).Scan(&out.id, &out.status); err != nil {
				return err
			}
			if out.status != "canceled" {
				return errors.Errorf(
					"Expected status 'canceled' but got '%s'", out.status)
			}
			if *job.ID() != out.id {
				return errors.Errorf(
					"Expected job id %d but got %d", *job.ID(), out.id)
			}
			return nil
		})
		// Give a chance for the above group to schedule in order to test that
		// SHOW JOBS WHEN COMPLETE does block until the job is canceled.
		time.Sleep(2 * time.Millisecond)
		if _, err = db.ExecContext(ctx, "CANCEL JOB $1", *job.ID()); err == nil {
			err = group.Wait()
		}
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("show jobs", func(t *testing.T) {
		// Start two jobs and cancel the first one to make sure the
		// query still blocks until the second job is also canceled.
		var jobs [2]*jobs.Job
		for i := range jobs {
			job, _, err := registry.CreateAndStartJob(ctx, nil, mockJob)
			if err != nil {
				t.Fatal(err)
			}
			jobs[i] = job
		}
		if _, err := db.ExecContext(ctx, "CANCEL JOB $1", *jobs[0].ID()); err != nil {
			t.Fatal(err)
		}
		group := ctxgroup.WithContext(ctx)
		group.GoCtx(func(ctx context.Context) error {
			rows, err := db.QueryContext(ctx,
				`SELECT job_id, status
				 FROM [SHOW JOBS WHEN COMPLETE (SELECT $1 UNION SELECT $2)]`,
				*jobs[0].ID(), *jobs[1].ID())
			if err != nil {
				return err
			}
			var cnt int
			for rows.Next() {
				if err := rows.Scan(&out.id, &out.status); err != nil {
					return err
				}
				cnt += 1
				switch out.id {
				case *jobs[0].ID():
				case *jobs[1].ID():
					// SHOW JOBS WHEN COMPLETE finishes only after all jobs are
					// canceled.
					if out.status != "canceled" {
						return errors.Errorf(
							"Expected status 'canceled' but got '%s'",
							out.status)
					}
				default:
					return errors.Errorf(
						"Expected either id:%d or id:%d but got: %d",
						*jobs[0].ID(), *jobs[1].ID(), out.id)
				}
			}
			if cnt != 2 {
				return errors.Errorf("Expected 2 results but found %d", cnt)
			}
			return nil
		})
		// Give a chance for the above group to schedule in order to test that
		// SHOW JOBS WHEN COMPLETE does block until the job is canceled.
		time.Sleep(2 * time.Millisecond)
		var err error
		if _, err = db.ExecContext(ctx, "CANCEL JOB $1", *jobs[1].ID()); err == nil {
			err = group.Wait()
		}
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestJobInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer jobs.ResetConstructors()()

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 5 * time.Second

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Accessed atomically.
	var hasRun int32
	var job *jobs.Job

	defer sql.ClearPlanHooks()
	// Piggy back on BACKUP to be able to create a succeeding test job.
	sql.AddPlanHook(
		func(_ context.Context, stmt tree.Statement, phs sql.PlanHookState,
		) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
			st, ok := stmt.(*tree.Backup)
			if !ok {
				return nil, nil, nil, false, nil
			}
			fn := func(_ context.Context, _ []sql.PlanNode, _ chan<- tree.Datums) error {
				var err error
				job, err = phs.ExtendedEvalContext().QueueJob(
					jobs.Record{
						Description: st.String(),
						Details:     jobspb.BackupDetails{},
						Progress:    jobspb.BackupProgress{},
					},
				)
				return err
			}
			return fn, nil, nil, false, nil
		},
	)
	jobs.RegisterConstructor(jobspb.TypeBackup, func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return jobs.FakeResumer{
			OnResume: func(ctx context.Context, _ chan<- tree.Datums) error {
				t.Logf("Resuming job: %+v", job.Payload())
				atomic.AddInt32(&hasRun, 1)
				return nil
			},
			FailOrCancel: func(ctx context.Context) error {
				atomic.AddInt32(&hasRun, 1)
				return nil
			},
		}
	})
	// Piggy back on RESTORE to be able to create a failing test job.
	sql.AddPlanHook(
		func(_ context.Context, stmt tree.Statement, phs sql.PlanHookState,
		) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
			_, ok := stmt.(*tree.Restore)
			if !ok {
				return nil, nil, nil, false, nil
			}
			fn := func(_ context.Context, _ []sql.PlanNode, _ chan<- tree.Datums) error {
				var err error
				job, err = phs.ExtendedEvalContext().QueueJob(
					jobs.Record{
						Description: "RESTORE",
						Details:     jobspb.RestoreDetails{},
						Progress:    jobspb.RestoreProgress{},
					},
				)
				return err
			}
			return fn, nil, nil, false, nil
		},
	)
	jobs.RegisterConstructor(jobspb.TypeRestore, func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return jobs.FakeResumer{
			OnResume: func(_ context.Context, _ chan<- tree.Datums) error {
				return errors.New("RESTORE failed")
			},
		}
	})

	t.Run("rollback txn", func(t *testing.T) {
		start := timeutil.Now()

		txn, err := sqlDB.Begin()
		require.NoError(t, err)
		_, err = txn.Exec("BACKUP tobeaborted TO doesnotmattter")
		require.NoError(t, err)

		// If we rollback then the job should not run
		require.NoError(t, txn.Rollback())
		registry := s.JobRegistry().(*jobs.Registry)
		_, err = registry.LoadJob(ctx, *job.ID())
		require.Error(t, err, "the job should not exist after the txn is rolled back")
		require.True(t, jobs.HasJobNotFoundError(err))

		sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
		// Just in case the job was scheduled let's wait for it to finish
		// to avoid a race.
		sqlRunner.Exec(t, "SHOW JOB WHEN COMPLETE $1", *job.ID())
		require.Equal(t, int32(0), atomic.LoadInt32(&hasRun),
			"job has run in transaction before txn commit")

		require.True(t, timeutil.Since(start) < jobs.DefaultAdoptInterval, "job should have been adopted immediately")
	})

	t.Run("normal success", func(t *testing.T) {
		start := timeutil.Now()

		// Now let's actually commit the transaction and check that the job ran.
		txn, err := sqlDB.Begin()
		require.NoError(t, err)
		_, err = txn.Exec("BACKUP tocommit TO foo")
		require.NoError(t, err)
		// Committing will block and wait for all jobs to run.
		require.NoError(t, txn.Commit())
		registry := s.JobRegistry().(*jobs.Registry)
		j, err := registry.LoadJob(ctx, *job.ID())
		require.NoError(t, err, "queued job not found")
		require.NotEqual(t, int32(0), atomic.LoadInt32(&hasRun),
			"job scheduled in transaction did not run")
		require.Equal(t, int32(1), atomic.LoadInt32(&hasRun),
			"more than one job ran")
		require.Equal(t, "", j.Payload().Error)

		require.True(t, timeutil.Since(start) < jobs.DefaultAdoptInterval, "job should have been adopted immediately")
	})

	t.Run("one of the queued jobs fails", func(t *testing.T) {
		start := timeutil.Now()
		txn, err := sqlDB.Begin()
		require.NoError(t, err)

		// Add a succeeding job.
		_, err = txn.Exec("BACKUP doesnotmatter TO doesnotmattter")
		require.NoError(t, err)
		// We hooked up a failing test job to RESTORE.
		_, err = txn.Exec("RESTORE TABLE tbl FROM somewhere")
		require.NoError(t, err)

		// Now let's actually commit the transaction and check that there is a
		// failure.
		require.Error(t, txn.Commit())
		require.True(t, timeutil.Since(start) < jobs.DefaultAdoptInterval, "job should have been adopted immediately")
	})
}

// TestStartableJobErrors tests that the StartableJob returns the expected
// errors when used incorrectly and performs the appropriate cleanup in
// CleanupOnRollback.
func TestStartableJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer jobs.ResetConstructors()()

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	jr := s.JobRegistry().(*jobs.Registry)
	var resumeFunc atomic.Value
	resumeFunc.Store(func(ctx context.Context, _ chan<- tree.Datums) error {
		return nil
	})
	setResumeFunc := func(f func(ctx context.Context, _ chan<- tree.Datums) error) (cleanup func()) {
		prev := resumeFunc.Load()
		resumeFunc.Store(f)
		return func() { resumeFunc.Store(prev) }
	}
	jobs.RegisterConstructor(jobspb.TypeRestore, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return jobs.FakeResumer{
			OnResume: func(ctx context.Context, resultsCh chan<- tree.Datums) error {
				return resumeFunc.Load().(func(ctx context.Context, _ chan<- tree.Datums) error)(ctx, resultsCh)
			},
		}
	})
	rec := jobs.Record{
		Description:   "There's a snake in my boot!",
		Username:      "Woody Pride",
		DescriptorIDs: []sqlbase.ID{1, 2, 3},
		Details:       jobspb.RestoreDetails{},
		Progress:      jobspb.RestoreProgress{},
	}
	createStartableJob := func(t *testing.T) (sj *jobs.StartableJob) {
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			sj, err = jr.CreateStartableJobWithTxn(ctx, rec, txn, nil)
			return err
		}))
		return sj
	}
	t.Run("Start called more than once", func(t *testing.T) {
		sj := createStartableJob(t)
		errCh, err := sj.Start(ctx)
		require.NoError(t, err)
		_, err = sj.Start(ctx)
		require.Regexp(t, `StartableJob \d+ cannot be started more than once`, err)
		require.NoError(t, <-errCh)
	})
	t.Run("Start called with active txn", func(t *testing.T) {
		txn := db.NewTxn(ctx, "test")
		defer func() {
			require.NoError(t, txn.Rollback(ctx))
		}()
		sj, err := jr.CreateStartableJobWithTxn(ctx, rec, txn, nil)
		require.NoError(t, err)
		_, err = sj.Start(ctx)
		require.Regexp(t, `cannot resume .* job which is not committed`, err)
	})
	t.Run("Start called with aborted txn", func(t *testing.T) {
		txn := db.NewTxn(ctx, "test")
		sj, err := jr.CreateStartableJobWithTxn(ctx, rec, txn, nil)
		require.NoError(t, err)
		require.NoError(t, txn.Rollback(ctx))
		_, err = sj.Start(ctx)
		require.Regexp(t, `cannot resume .* job which is not committed`, err)
	})
	t.Run("CleanupOnRollback called with active txn", func(t *testing.T) {
		txn := db.NewTxn(ctx, "test")
		defer func() {
			require.NoError(t, txn.Rollback(ctx))
		}()
		sj, err := jr.CreateStartableJobWithTxn(ctx, rec, txn, nil)
		require.NoError(t, err)
		err = sj.CleanupOnRollback(ctx)
		require.Regexp(t, `cannot call CleanupOnRollback for a StartableJob with a non-finalized transaction`, err)
	})
	t.Run("CleanupOnRollback called with committed txn", func(t *testing.T) {
		sj := createStartableJob(t)
		err := sj.CleanupOnRollback(ctx)
		require.Regexp(t, `cannot call CleanupOnRollback for a StartableJob created by a committed transaction`, err)
	})
	t.Run("CleanupOnRollback positive case", func(t *testing.T) {
		txn := db.NewTxn(ctx, "test")
		sj, err := jr.CreateStartableJobWithTxn(ctx, rec, txn, nil)
		require.NoError(t, err)
		require.NoError(t, txn.Rollback(ctx))
		require.NoError(t, sj.CleanupOnRollback(ctx))
		for _, id := range jr.CurrentlyRunningJobs() {
			require.NotEqual(t, id, *sj.ID())
		}
	})
	t.Run("Cancel", func(t *testing.T) {
		txn := db.NewTxn(ctx, "test")
		sj, err := jr.CreateStartableJobWithTxn(ctx, rec, txn, nil)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(ctx))
		require.NoError(t, sj.Cancel(ctx))
		status, err := sj.CurrentStatus(ctx)
		require.NoError(t, err)
		require.Equal(t, jobs.StatusCancelRequested, status)
		for _, id := range jr.CurrentlyRunningJobs() {
			require.NotEqual(t, id, *sj.ID())
		}
		_, err = sj.Start(ctx)
		require.Regexp(t, "job with status cancel-requested cannot be marked started", err)
	})
	setUpRunTest := func(t *testing.T) (
		sj *jobs.StartableJob,
		resultCh <-chan tree.Datums,
		blockResume func() (waitForBlocked func() (resultsCh chan<- tree.Datums, unblockWithError func(error))),
		cleanup func(),
	) {
		type blockResp struct {
			errCh     chan error
			resultsCh chan<- tree.Datums
		}
		blockCh := make(chan chan blockResp, 1)
		blockResume = func() (waitForBlocked func() (resultsCh chan<- tree.Datums, unblockWithError func(error))) {
			blockRequest := make(chan blockResp, 1)
			blockCh <- blockRequest // from test to resumer
			return func() (resultsCh chan<- tree.Datums, unblockWithError func(error)) {
				blocked := <-blockRequest // from resumer to test
				return blocked.resultsCh, func(err error) {
					blocked.errCh <- err // from test to resumer
				}
			}
		}
		cleanup = setResumeFunc(func(ctx context.Context, resultsCh chan<- tree.Datums) error {
			select {
			case blockRequest := <-blockCh:
				unblock := make(chan error)
				blockRequest <- blockResp{
					errCh:     unblock,
					resultsCh: resultsCh,
				}
				if err := <-unblock; err != nil {
					return err
				}
			default:
			}
			return nil
		})
		clientResults := make(chan tree.Datums)
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			sj, err = jr.CreateStartableJobWithTxn(ctx, rec, txn, clientResults)
			return err
		}))
		return sj, clientResults, blockResume, cleanup
	}
	t.Run("Run - error during resume", func(t *testing.T) {
		sj, _, blockResume, cleanup := setUpRunTest(t)
		defer cleanup()
		waitForBlocked := blockResume()
		runErr := make(chan error)
		go func() { runErr <- sj.Run(ctx) }()
		_, unblock := waitForBlocked()
		unblock(errors.New("boom"))
		require.Regexp(t, "boom", <-runErr)
	})
	t.Run("Run - client canceled", func(t *testing.T) {
		sj, _, blockResume, cleanup := setUpRunTest(t)
		defer cleanup()
		ctxToCancel, cancel := context.WithCancel(ctx)
		runErr := make(chan error)
		waitForBlocked := blockResume()
		go func() { runErr <- sj.Run(ctxToCancel) }()
		resultsCh, unblock := waitForBlocked()
		cancel()
		require.Regexp(t, context.Canceled, <-runErr)
		resultsCh <- tree.Datums{}
		unblock(nil)
		testutils.SucceedsSoon(t, func() error {
			loaded, err := jr.LoadJob(ctx, *sj.ID())
			require.NoError(t, err)
			st, err := loaded.CurrentStatus(ctx)
			require.NoError(t, err)
			if st != jobs.StatusSucceeded {
				return errors.Errorf("expected %s, got %s", jobs.StatusSucceeded, st)
			}
			return nil
		})
	})
	t.Run("Run - results chan closed", func(t *testing.T) {
		sj, clientResultsChan, blockResume, cleanup := setUpRunTest(t)
		defer cleanup()
		runErr := make(chan error)
		waitForBlocked := blockResume()
		go func() { runErr <- sj.Run(ctx) }()
		resultsCh, unblock := waitForBlocked()
		go func() {
			_, ok := <-clientResultsChan
			assert.True(t, ok)
			_, ok = <-clientResultsChan
			assert.False(t, ok)
			unblock(nil)
		}()
		resultsCh <- tree.Datums{}
		close(resultsCh)
		require.NoError(t, <-runErr)
	})
}

// TestUnmigratedSchemaChangeJobs tests that schema change jobs created in 19.2
// that have not undergone a migration cannot be adopted, canceled, or paused.
func TestUnmigratedSchemaChangeJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer jobs.ResetConstructors()()
	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 10 * time.Millisecond

	ctx := context.Background()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	registry := s.JobRegistry().(*jobs.Registry)

	// The default FormatVersion value in SchemaChangeDetails corresponds to a
	// pre-20.1 job.
	rec := jobs.Record{
		DescriptorIDs: []sqlbase.ID{1},
		Details:       jobspb.SchemaChangeDetails{},
		Progress:      jobspb.SchemaChangeProgress{},
	}

	t.Run("job is not adopted", func(t *testing.T) {
		resuming := make(chan struct{})
		jobs.RegisterConstructor(jobspb.TypeSchemaChange, func(_ *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return jobs.FakeResumer{
				OnResume: func(ctx context.Context, _ chan<- tree.Datums) error {
					resuming <- struct{}{}
					return nil
				},
			}
		})
		select {
		case <-resuming:
			t.Fatal("job was resumed")
		case <-time.After(time.Second):
			// With an adopt interval of 10 ms, within 1 s we can be reasonably sure
			// that the job was not adopted.
		}
	})

	t.Run("pause not supported", func(t *testing.T) {
		job, err := registry.CreateJobWithTxn(ctx, rec, nil /* txn */)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sqlDB.Exec("PAUSE JOB $1", *job.ID()); !testutils.IsError(err, "cannot be paused in this version") {
			t.Fatal(err)
		}
	})

	t.Run("cancel not supported", func(t *testing.T) {
		job, err := registry.CreateJobWithTxn(ctx, rec, nil /* txn */)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sqlDB.Exec("CANCEL JOB $1", *job.ID()); !testutils.IsError(err, "cannot be canceled in this version") {
			t.Fatal(err)
		}
	})
}
