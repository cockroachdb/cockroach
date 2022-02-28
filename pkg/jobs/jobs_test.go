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
	"archive/zip"
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/gogo/protobuf/types"
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

func (expected *expectation) verify(id jobspb.JobID, expectedStatus jobs.Status) error {
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
		Username:      payload.UsernameProto.Decode(),
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
	defer log.Scope(t).Close(t)

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
	ctx            context.Context
	s              serverutils.TestServerInterface
	tempDirCleanup func()
	outerDB        *gosql.DB
	sqlDB          *sqlutils.SQLRunner
	registry       *jobs.Registry
	done           chan struct{}
	mockJob        jobs.Record
	job            *jobs.StartableJob
	mu             struct {
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

	// beforeUpdate is invoked in the BeforeUpdate testing knob if non-nil.
	beforeUpdate func(orig, updated jobs.JobMetadata) error

	// afterJobStateMachine is invoked in the AfterJobStateMachine testing knob if
	// non-nil.
	afterJobStateMachine func()

	// Instead of a ch for success, use a variable because it can retry since it
	// is in a transaction.
	successErr error

	// controls whether job resumers will ask for a real tracing span.
	traceRealSpan bool
}

func noopPauseRequestFunc(
	ctx context.Context, planHookState interface{}, txn *kv.Txn, progress *jobspb.Progress,
) error {
	return nil
}

var _ jobs.TraceableJob = (*jobs.FakeResumer)(nil)

func (rts *registryTestSuite) setUp(t *testing.T) {
	rts.ctx = context.Background()

	var args base.TestServerArgs
	{
		knobs := jobs.NewTestingKnobsWithShortIntervals()
		knobs.BeforeUpdate = func(orig, updated jobs.JobMetadata) error {
			if rts.beforeUpdate != nil {
				return rts.beforeUpdate(orig, updated)
			}
			return nil
		}
		knobs.AfterJobStateMachine = func() {
			if rts.afterJobStateMachine != nil {
				rts.afterJobStateMachine()
			}
		}
		args.Knobs.JobsTestingKnobs = knobs
		args.Knobs.SpanConfig = &spanconfig.TestingKnobs{
			ManagerDisableJobCreation: true,
		}

		if rts.traceRealSpan {
			baseDir, dirCleanupFn := testutils.TempDir(t)
			rts.tempDirCleanup = dirCleanupFn
			traceDir := filepath.Join(baseDir, "trace_dir")
			args.TraceDir = traceDir
		}
	}

	rts.s, rts.outerDB, _ = serverutils.StartServer(t, args)
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
			TraceRealSpan: rts.traceRealSpan,
			OnResume: func(ctx context.Context) error {
				t.Log("Starting resume")
				if rts.traceRealSpan {
					// Add a dummy recording so we actually see something in the trace.
					span := tracing.SpanFromContext(ctx)
					span.RecordStructured(&types.StringValue{Value: "boom"})
				}
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
						err := job.FractionProgressed(rts.ctx, nil /* txn */, jobs.FractionUpdater(0))
						if err != nil {
							return err
						}
					}
				}
			},
			FailOrCancel: func(ctx context.Context) error {
				t.Log("Starting FailOrCancel")
				if rts.traceRealSpan {
					// Add a dummy recording so we actually see something in the trace.
					span := tracing.SpanFromContext(ctx)
					span.RecordStructured(&types.StringValue{Value: "boom"})
				}
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
					t.Log("Exiting FailOrCancel")
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
	jobs.ResetConstructors()()
	if rts.tempDirCleanup != nil {
		rts.tempDirCleanup()
	}
}

func (rts *registryTestSuite) check(t *testing.T, expectedStatus jobs.Status) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		rts.mu.Lock()
		defer rts.mu.Unlock()
		if diff := cmp.Diff(rts.mu.e, rts.mu.a); diff != "" {
			return errors.Errorf("unexpected diff: %s", diff)
		}
		if expectedStatus == "" {
			return nil
		}
		st, err := rts.job.TestingCurrentStatus(rts.ctx, nil /* txn */)
		if err != nil {
			return err
		}
		if expectedStatus != st {
			return errors.Errorf("expected job status: %s but got: %s", expectedStatus, st)
		}
		return nil
	})
}

func TestRegistryLifecycle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("normal success", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()

		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.s.DB(), rts.mockJob)
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
		t.Log("Done")
	})

	t.Run("create separately success", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()

		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.s.DB(), rts.mockJob)
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

		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.s.DB(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)

		rts.sqlDB.Exec(t, "PAUSE JOB $1", j.ID())
		rts.check(t, jobs.StatusPaused)

		rts.sqlDB.Exec(t, "PAUSE JOB $1", j.ID())
		rts.check(t, jobs.StatusPaused)

		rts.sqlDB.Exec(t, "RESUME JOB $1", j.ID())
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

		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.s.DB(), rts.mockJob)
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

		rts.sqlDB.Exec(t, "PAUSE JOB $1", j.ID())
		rts.check(t, jobs.StatusPaused)

		rts.sqlDB.Exec(t, "PAUSE JOB $1", j.ID())
		rts.check(t, jobs.StatusPaused)

		rts.sqlDB.Exec(t, "RESUME JOB $1", j.ID())
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
		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.s.DB(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)

		rts.sqlDB.Exec(t, "CANCEL JOB $1", j.ID())
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
		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.s.DB(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)

		rts.sqlDB.Exec(t, "CANCEL JOB $1", j.ID())
		rts.mu.e.OnFailOrCancelStart = true
		rts.check(t, jobs.StatusReverting)

		rts.sqlDB.ExpectErr(t, "status reverting cannot be requested to be canceled", "CANCEL JOB $1", j.ID())
		rts.check(t, jobs.StatusReverting)

		close(rts.failOrCancelCheckCh)
		close(rts.failOrCancelCh)
	})

	t.Run("cancel pause running", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()

		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.s.DB(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)

		rts.sqlDB.Exec(t, "PAUSE JOB $1", j.ID())
		rts.check(t, jobs.StatusPaused)

		rts.sqlDB.Exec(t, "CANCEL JOB $1", j.ID())
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

		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.s.DB(), rts.mockJob)
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

		rts.sqlDB.Exec(t, "PAUSE JOB $1", j.ID())
		rts.check(t, jobs.StatusPaused)

		rts.sqlDB.ExpectErr(t, "paused and has non-nil FinalResumeError .* resume failed", "CANCEL JOB $1", j.ID())
		rts.check(t, jobs.StatusPaused)

		rts.sqlDB.Exec(t, "RESUME JOB $1", j.ID())
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
		job, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.s.DB(), rts.mockJob)
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
			if _, err := txn.Exec("CANCEL JOB $1", job.ID()); err != nil {
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
			if _, err := txn.Exec("PAUSE JOB $1", job.ID()); err != nil {
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
			if _, err := txn.Exec("PAUSE JOB $1", job.ID()); err != nil {
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
			if _, err := txn.Exec("RESUME JOB $1", job.ID()); err != nil {
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
			if _, err := txn.Exec("RESUME JOB $1", job.ID()); err != nil {
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

		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.s.DB(), rts.mockJob)
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
		var triedToMarkSucceeded atomic.Value
		triedToMarkSucceeded.Store(false)
		rts := registryTestSuite{beforeUpdate: func(orig, updated jobs.JobMetadata) error {
			// Fail marking succeeded.
			if updated.Status == jobs.StatusSucceeded {
				triedToMarkSucceeded.Store(true)
				return errors.New("injected failure at marking as succeeded")
			}
			return nil
		}}
		rts.setUp(t)
		defer rts.tearDown()

		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.s.DB(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)
		// Let the resumer complete without error.
		rts.resumeCh <- nil
		rts.mu.e.ResumeExit++
		rts.mu.e.Success = true

		// The job is retried as we failed to mark the job successful.
		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)
		// Fail the resumer to transition to reverting state.
		rts.resumeCh <- errors.New("injected error in resume")
		rts.mu.e.ResumeExit++

		// The job is now in state reverting and will never resume again because
		// OnFailOrCancel also fails.
		//
		// First retry.
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		require.True(t, triedToMarkSucceeded.Load().(bool))
		rts.check(t, jobs.StatusReverting)
		rts.failOrCancelCh <- errors.New("injected failure while blocked in reverting")
		rts.mu.e.OnFailOrCancelExit = true

		// The job will be retried as all reverting jobs are retried.
		//
		// Second retry.
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StatusReverting)
		rts.failOrCancelCh <- errors.New("injected failure while blocked in reverting")
		rts.mu.e.OnFailOrCancelExit = true

		// The job will stay in reverting state. Let it fail to exit the test.
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StatusReverting)
		close(rts.failOrCancelCh)
		rts.mu.e.OnFailOrCancelExit = true

		rts.check(t, jobs.StatusFailed)
	})
	// Succeed the job but inject an error actually marking the jobs successful.
	// This could happen due to a transient network error or something like that.
	// It would not make sense to revert a job in this scenario.
	t.Run("fail marking success", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()

		// Inject an error in the update to move the job to "succeeded" one time.
		var failed atomic.Value
		failed.Store(false)
		rts.beforeUpdate = func(orig, updated jobs.JobMetadata) error {
			if updated.Status == jobs.StatusSucceeded && !failed.Load().(bool) {
				failed.Store(true)
				return errors.New("boom")
			}
			return nil
		}

		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.s.DB(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		// Make sure the job hits the error when it attempts to succeed.
		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)
		rts.resumeCh <- nil
		testutils.SucceedsSoon(t, func() error {
			if !failed.Load().(bool) {
				return errors.New("not yet failed")
			}
			return nil
		})
		rts.mu.e.ResumeExit++

		// Make sure the job retries and then succeeds.
		rts.resumeCheckCh <- struct{}{}
		rts.resumeCh <- nil
		rts.mu.e.ResumeExit++
		rts.mu.e.Success = true
		rts.check(t, jobs.StatusSucceeded)
	})

	// Fail the job, but also fail to mark it failed.
	t.Run("fail marking failed", func(t *testing.T) {
		var triedToMarkFailed atomic.Value
		triedToMarkFailed.Store(false)
		rts := registryTestSuite{beforeUpdate: func(orig, updated jobs.JobMetadata) error {
			if triedToMarkFailed.Load().(bool) == true {
				return nil
			}
			if updated.Status == jobs.StatusFailed {
				triedToMarkFailed.Store(true)
				return errors.New("injected error while marking as failed")
			}
			return nil
		}}
		rts.setUp(t)
		defer rts.tearDown()

		// Make marking success fail.
		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.s.DB(), rts.mockJob)
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
		rts.check(t, jobs.StatusReverting)
		// The job is now in state reverting and will never resume again.
		// Let revert complete without error so that the job is attempted to mark as failed.
		rts.failOrCancelCh <- nil
		rts.mu.e.OnFailOrCancelExit = true

		// We failed to mark the jobs as failed, resulting in the job to be retried.
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StatusReverting)
		require.True(t, triedToMarkFailed.Load().(bool))
		// Let the job complete to exit the test.
		close(rts.failOrCancelCh)
		rts.mu.e.OnFailOrCancelExit = true
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

		job, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.s.DB(), rts.mockJob)
		require.NoError(t, err)
		rts.job = job

		rts.resumeCheckCh <- struct{}{}
		rts.mu.e.ResumeStart = true
		rts.check(t, jobs.StatusRunning)

		// Request that the job is paused.
		pauseErrCh := make(chan error)
		go func() {
			_, err := rts.outerDB.Exec("PAUSE JOB $1", job.ID())
			pauseErrCh <- err
		}()

		// Ensure that the pause went off without a problem.
		require.NoError(t, <-pauseErrCh)
		rts.check(t, jobs.StatusPaused)
		{
			// Make sure the side-effects of our pause function occurred.
			j, err := rts.registry.LoadJob(rts.ctx, job.ID())
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

		job, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.s.DB(), rts.mockJob)
		require.NoError(t, err)
		rts.job = job

		// Allow the job to start.
		rts.resumeCheckCh <- struct{}{}
		rts.mu.e.ResumeStart = true
		rts.check(t, jobs.StatusRunning)

		// Request that the job is paused and ensure that the pause hit the error
		// and failed to pause.
		_, err = rts.outerDB.Exec("PAUSE JOB $1", job.ID())
		require.Regexp(t, "boom", err)

		// Allow the job to complete.
		rts.resumeCh <- nil
		rts.mu.e.Success = true
		rts.mu.e.ResumeExit++
		rts.check(t, jobs.StatusSucceeded)
	})
	t.Run("fail setting trace ID", func(t *testing.T) {
		// The trace ID is set on the job above the state machine loop.
		// This tests a regression where we fail to set trace ID and then
		// don't clear the in-memory state that we were running this job.
		// That prevents the job from being re-run.

		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()
		rts.traceRealSpan = true

		// Inject an error in the update to record the trace ID.
		var failed atomic.Value
		failed.Store(false)
		rts.beforeUpdate = func(orig, updated jobs.JobMetadata) error {
			if !failed.Load().(bool) &&
				orig.Progress.TraceID == 0 &&
				updated.Progress != nil &&
				updated.Progress.TraceID != 0 {
				failed.Store(true)
				return errors.New("boom")
			}
			return nil
		}

		j, err := jobs.TestingCreateAndStartJob(context.Background(), rts.registry, rts.s.DB(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		testutils.SucceedsSoon(t, func() error {
			if !failed.Load().(bool) {
				return errors.New("not yet failed")
			}
			return nil
		})

		// Make sure the job retries and then succeeds.
		rts.resumeCheckCh <- struct{}{}
		rts.resumeCh <- nil
		rts.mu.e.ResumeStart = true
		rts.mu.e.ResumeExit++
		rts.mu.e.Success = true
		rts.check(t, jobs.StatusSucceeded)
	})

	t.Run("dump traces on pause-unpause-success", func(t *testing.T) {
		completeCh := make(chan struct{})
		rts := registryTestSuite{traceRealSpan: true, afterJobStateMachine: func() {
			completeCh <- struct{}{}
		}}
		rts.setUp(t)
		defer rts.tearDown()

		pauseUnpauseJob := func(expectedNumFiles int) {
			j, err := jobs.TestingCreateAndStartJob(context.Background(), rts.registry, rts.s.DB(), rts.mockJob)
			if err != nil {
				t.Fatal(err)
			}
			rts.job = j

			rts.mu.e.ResumeStart = true
			rts.resumeCheckCh <- struct{}{}
			rts.check(t, jobs.StatusRunning)

			rts.sqlDB.Exec(t, "PAUSE JOB $1", j.ID())
			rts.check(t, jobs.StatusPaused)

			<-completeCh
			checkTraceFiles(t, rts.registry, expectedNumFiles)

			rts.sqlDB.Exec(t, "RESUME JOB $1", j.ID())

			rts.mu.e.ResumeStart = true
			rts.resumeCheckCh <- struct{}{}
			rts.check(t, jobs.StatusRunning)
			rts.resumeCh <- nil
			rts.mu.e.ResumeExit++

			rts.mu.e.Success = true
			rts.check(t, jobs.StatusSucceeded)

			<-completeCh
			checkTraceFiles(t, rts.registry, expectedNumFiles)
		}
		rts.sqlDB.Exec(t, `SET CLUSTER SETTING jobs.trace.force_dump_mode='never'`)
		pauseUnpauseJob(0)

		rts.sqlDB.Exec(t, `SET CLUSTER SETTING jobs.trace.force_dump_mode='onFail'`)
		pauseUnpauseJob(0)

		rts.sqlDB.Exec(t, `SET CLUSTER SETTING jobs.trace.force_dump_mode='onStop'`)
		pauseUnpauseJob(1)
	})

	t.Run("dump traces on fail", func(t *testing.T) {
		completeCh := make(chan struct{})
		rts := registryTestSuite{traceRealSpan: true, afterJobStateMachine: func() {
			completeCh <- struct{}{}
		}}
		rts.setUp(t)
		defer rts.tearDown()

		runJobAndFail := func(expectedNumFiles int) {
			j, err := jobs.TestingCreateAndStartJob(context.Background(), rts.registry, rts.s.DB(), rts.mockJob)
			if err != nil {
				t.Fatal(err)
			}
			rts.job = j

			rts.mu.e.ResumeStart = true
			rts.resumeCheckCh <- struct{}{}
			rts.check(t, jobs.StatusRunning)

			rts.resumeCh <- errors.New("boom")
			rts.mu.e.ResumeExit++
			rts.mu.e.OnFailOrCancelStart = true
			rts.failOrCancelCheckCh <- struct{}{}
			rts.check(t, jobs.StatusReverting)

			rts.failOrCancelCh <- nil
			rts.mu.e.OnFailOrCancelExit = true
			rts.check(t, jobs.StatusFailed)

			<-completeCh
			checkTraceFiles(t, rts.registry, expectedNumFiles)
		}

		rts.sqlDB.Exec(t, `SET CLUSTER SETTING jobs.trace.force_dump_mode='never'`)
		runJobAndFail(0)

		rts.sqlDB.Exec(t, `SET CLUSTER SETTING jobs.trace.force_dump_mode='onFail'`)
		runJobAndFail(1)

		rts.sqlDB.Exec(t, `SET CLUSTER SETTING jobs.trace.force_dump_mode='onStop'`)
		runJobAndFail(1)
	})

	t.Run("dump traces on cancel", func(t *testing.T) {
		completeCh := make(chan struct{})
		rts := registryTestSuite{traceRealSpan: true, afterJobStateMachine: func() {
			completeCh <- struct{}{}
		}}
		rts.setUp(t)
		defer rts.tearDown()
		rts.sqlDB.Exec(t, `SET CLUSTER SETTING jobs.trace.force_dump_mode='onStop'`)
		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.s.DB(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)

		rts.sqlDB.Exec(t, "CANCEL JOB $1", j.ID())

		<-completeCh
		checkTraceFiles(t, rts.registry, 1)

		rts.mu.e.OnFailOrCancelStart = true
		rts.check(t, jobs.StatusReverting)

		rts.failOrCancelCheckCh <- struct{}{}
		close(rts.failOrCancelCheckCh)
		rts.failOrCancelCh <- nil
		close(rts.failOrCancelCh)
		rts.mu.e.OnFailOrCancelExit = true

		rts.check(t, jobs.StatusCanceled)

		<-completeCh
	})
}

func checkTraceFiles(t *testing.T, registry *jobs.Registry, expectedNumFiles int) {
	t.Helper()
	// Check the configured inflight trace dir for dumped zip files.
	expList := []string{"node1-trace.txt", "node1-jaeger.json"}
	traceDumpDir := jobs.TestingGetTraceDumpDir(registry)
	files := make([]string, 0)
	require.NoError(t, filepath.Walk(traceDumpDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		files = append(files, path)
		return nil
	}))

	require.Equal(t, expectedNumFiles, len(files))
	for _, file := range files {
		checkBundle(t, file, expList)
	}

	// Cleanup files for next iteration of the test.
	for _, file := range files {
		require.NoError(t, os.Remove(file))
	}
}

func TestJobLifecycle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.ResetConstructors()()

	ctx := context.Background()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	registry := s.JobRegistry().(*jobs.Registry)

	createJob := func(record jobs.Record) (*jobs.Job, expectation) {
		beforeTime := timeutil.Now()
		job, err := registry.CreateAdoptableJobWithTxn(ctx, record, registry.MakeJobID(), nil /* txn */)
		require.NoError(t, err)
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
			OnResume: func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-done:
					return nil
				}
			},
		}
	})

	startLeasedJob := func(t *testing.T, record jobs.Record) (*jobs.StartableJob, expectation) {
		beforeTime := timeutil.Now()
		job, err := jobs.TestingCreateAndStartJob(ctx, registry, s.DB(), record)
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
		woodyPride, _ := security.MakeSQLUsernameFromUserInput("Woody Pride", security.UsernameValidation)
		woodyJob, woodyExp := createJob(jobs.Record{
			Description:   "There's a snake in my boot!",
			Username:      woodyPride,
			DescriptorIDs: []descpb.ID{1, 2, 3},
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
			if err := woodyJob.FractionProgressed(ctx, nil /* txn */, jobs.FractionUpdater(f.actual)); err != nil {
				t.Fatal(err)
			}
			woodyExp.FractionCompleted = f.expected
			if err := woodyExp.verify(woodyJob.ID(), jobs.StatusRunning); err != nil {
				t.Fatal(err)
			}
		}

		// Test Progressed callbacks.
		if err := woodyJob.FractionProgressed(ctx, nil /* txn */, func(_ context.Context, details jobspb.ProgressDetails) float32 {
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
		buzzL, _ := security.MakeSQLUsernameFromUserInput("Buzz Lightyear", security.UsernameValidation)
		buzzRecord := jobs.Record{
			Description:   "To infinity and beyond!",
			Username:      buzzL,
			DescriptorIDs: []descpb.ID{3, 2, 1},
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
		buzzJob, err := registry.CreateAdoptableJobWithTxn(ctx, buzzRecord, registry.MakeJobID(), nil /* txn */)
		require.NoError(t, err)
		if err := buzzExp.verify(buzzJob.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}

		if err := buzzJob.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := buzzExp.verify(buzzJob.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}

		if err := buzzJob.FractionProgressed(ctx, nil /* txn */, jobs.FractionUpdater(.42)); err != nil {
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
		sidP, _ := security.MakeSQLUsernameFromUserInput("Sid Phillips", security.UsernameValidation)
		sidJob, sidExp := createJob(jobs.Record{
			Description:   "The toys! The toys are alive!",
			Username:      sidP,
			DescriptorIDs: []descpb.ID{6, 6, 6},
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
				`UPDATE system.jobs SET payload = 'garbage' WHERE id = $1`, job.ID(),
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
				`UPDATE system.jobs SET payload = 'garbage' WHERE id = $1`, job.ID(),
			); err != nil {
				t.Fatal(err)
			}
			if err := job.Failed(ctx, errors.New("boom")); !testutils.IsError(err, "wrong wireType") {
				t.Fatalf("unexpected: %v", err)
			}
		})
		t.Run("huge errors are truncated if marking job as failed", func(t *testing.T) {
			hugeErr := strings.Repeat("a", 2048)
			truncatedHugeErr := "boom: " + strings.Repeat("a", 1018) + " -- TRUNCATED"
			err := errors.Errorf("boom: %s", hugeErr)
			job, exp := createDefaultJob()
			exp.Error = truncatedHugeErr
			if err := job.Failed(ctx, err); err != nil {
				t.Fatal(err)
			}
			if err := exp.verify(job.ID(), jobs.StatusFailed); err != nil {
				t.Fatal(err)
			}
		})

	})

	t.Run("cancelable jobs can be paused until finished", func(t *testing.T) {
		job, exp := startLeasedJob(t, defaultRecord)

		if err := registry.PauseRequested(ctx, nil, job.ID(), ""); err != nil {
			t.Fatal(err)
		}
		if err := job.Paused(ctx); err != nil {
			t.Fatal(err)
		}
		if err := exp.verify(job.ID(), jobs.StatusPaused); err != nil {
			t.Fatal(err)
		}
		if err := registry.Unpause(ctx, nil, job.ID()); err != nil {
			t.Fatal(err)
		}
		// Resume the job again to ensure that the resumption is idempotent.
		if err := registry.Unpause(ctx, nil, job.ID()); err != nil {
			t.Fatal(err)
		}
		if err := exp.verify(job.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}

		// PauseRequested fails after job is successful.
		if err := job.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := registry.PauseRequested(ctx, nil, job.ID(), ""); !testutils.IsError(err, "cannot be requested to be paused") {
			t.Fatalf("expected 'cannot pause succeeded job', but got '%s'", err)
		}
	})

	t.Run("cancelable jobs can be canceled until finished", func(t *testing.T) {
		{
			job, exp := startLeasedJob(t, defaultRecord)
			if err := registry.CancelRequested(ctx, nil, job.ID()); err != nil {
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
			if err := registry.CancelRequested(ctx, nil, job.ID()); err != nil {
				t.Fatal(err)
			}
			if err := exp.verify(job.ID(), jobs.StatusCancelRequested); err != nil {
				t.Fatal(err)
			}
		}

		{
			job, exp := startLeasedJob(t, defaultRecord)
			if err := registry.PauseRequested(ctx, nil, job.ID(), ""); err != nil {
				t.Fatal(err)
			}
			if err := job.Paused(ctx); err != nil {
				t.Fatal(err)
			}
			if err := registry.CancelRequested(ctx, nil, job.ID()); err != nil {
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
			if err := registry.CancelRequested(ctx, nil, job.ID()); !testutils.IsError(err, expectedErr) {
				t.Fatalf("expected '%s', but got '%s'", expectedErr, err)
			}
		}
	})

	t.Run("unpaused jobs cannot be resumed", func(t *testing.T) {
		{
			job, _ := startLeasedJob(t, defaultRecord)
			if err := registry.CancelRequested(ctx, nil, job.ID()); err != nil {
				t.Fatal(err)
			}
			if err := registry.Unpause(ctx, nil, job.ID()); !testutils.IsError(err, "cannot be resumed") {
				t.Errorf("got unexpected status '%v'", err)
			}
		}

		{
			job, _ := startLeasedJob(t, defaultRecord)
			if err := job.Succeeded(ctx); err != nil {
				t.Fatal(err)
			}
			expectedErr := fmt.Sprintf("job with status %s cannot be resumed", jobs.StatusSucceeded)
			if err := registry.Unpause(ctx, nil, job.ID()); !testutils.IsError(err, expectedErr) {
				t.Errorf("expected '%s', but got '%v'", expectedErr, err)
			}
		}
	})

	t.Run("bad job details fail", func(t *testing.T) {
		defer func() {
			if r, ok := recover().(error); !ok || !strings.Contains(r.Error(), "unknown details type int") {
				t.Fatalf("expected 'unknown details type int', but got: %v", r)
			}
		}()
		// Ignore the returned error because this code is expecting the call to
		// panic.
		_, _ = registry.CreateAdoptableJobWithTxn(ctx, jobs.Record{
			Details: 42,
		}, registry.MakeJobID(), nil /* txn */)
	})

	t.Run("update before create fails", func(t *testing.T) {
		// Attempt to create the job but abort the transaction.
		var job *jobs.Job
		require.Regexp(t, "boom", s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			job, _ = registry.CreateAdoptableJobWithTxn(ctx, jobs.Record{
				Details:  jobspb.RestoreDetails{},
				Progress: jobspb.RestoreProgress{},
			}, registry.MakeJobID(), txn)
			return errors.New("boom")
		}))
		if err := job.Started(ctx); !testutils.IsError(err, "not found in system.jobs table") {
			t.Fatalf("unexpected error %v", err)
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
			require.NoError(t, job.Update(ctx, nil, func(_ *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
				return jobs.UpdateHighwaterProgressed(ts, md, ju)
			}))
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
		if err := job.FractionProgressed(ctx, nil /* txn */, jobs.FractionUpdater(-0.1)); !testutils.IsError(err, "outside allowable range") {
			t.Fatalf("expected 'outside allowable range' error, but got %v", err)
		}
		if err := job.FractionProgressed(ctx, nil /* txn */, jobs.FractionUpdater(1.1)); !testutils.IsError(err, "outside allowable range") {
			t.Fatalf("expected 'outside allowable range' error, but got %v", err)
		}
		if err := job.Update(ctx, nil, func(_ *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			return jobs.UpdateHighwaterProgressed(hlc.Timestamp{WallTime: -1}, md, ju)
		}); !testutils.IsError(err, "outside allowable range") {
			t.Fatalf("expected 'outside allowable range' error, but got %v", err)
		}
	})

	t.Run("error propagates", func(t *testing.T) {
		job, _ := createDefaultJob()
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Update(ctx, nil, func(_ *kv.Txn, _ jobs.JobMetadata, ju *jobs.JobUpdater) error {
			return errors.Errorf("boom")
		}); !testutils.IsError(err, "boom") {
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
		if err := job.FractionProgressed(ctx, nil /* txn */, jobs.FractionUpdater(0.5)); !testutils.IsError(
			err, `cannot update progress on succeeded job \(id \d+\)`,
		) {
			t.Fatalf("expected 'cannot update progress' error, but got %v", err)
		}
	})

	t.Run("progress on paused job fails", func(t *testing.T) {
		job, _ := startLeasedJob(t, defaultRecord)
		if err := registry.PauseRequested(ctx, nil, job.ID(), ""); err != nil {
			t.Fatal(err)
		}
		if err := job.FractionProgressed(ctx, nil /* txn */, jobs.FractionUpdater(0.5)); !testutils.IsError(
			err, `cannot update progress on pause-requested job`,
		) {
			t.Fatalf("expected progress error, but got %v", err)
		}
	})

	t.Run("progress on canceled job fails", func(t *testing.T) {
		job, _ := startLeasedJob(t, defaultRecord)
		if err := registry.CancelRequested(ctx, nil, job.ID()); err != nil {
			t.Fatal(err)
		}
		if err := job.FractionProgressed(ctx, nil /* txn */, jobs.FractionUpdater(0.5)); !testutils.IsError(
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
		if err := job.FractionProgressed(ctx, nil /* txn */, jobs.FractionUpdater(0.2)); err != nil {
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

	updateClaimStmt := `UPDATE system.jobs SET claim_session_id = $1 WHERE id = $2`
	updateStatusStmt := `UPDATE system.jobs SET status = $1 WHERE id = $2`

	t.Run("set details works", func(t *testing.T) {
		job, exp := startLeasedJob(t, defaultRecord)
		require.NoError(t, exp.verify(job.ID(), jobs.StatusRunning))
		newDetails := jobspb.ImportDetails{URIs: []string{"new"}}
		exp.Record.Details = newDetails
		require.NoError(t, job.SetDetails(ctx, nil /* txn */, newDetails))
		require.NoError(t, exp.verify(job.ID(), jobs.StatusRunning))
		require.NoError(t, job.SetDetails(ctx, nil /* txn */, newDetails))

		// Now change job's session id and check that updates are rejected.
		_, err := exp.DB.Exec(updateClaimStmt, "!@#!@$!$@#", job.ID())
		require.NoError(t, err)
		require.Error(t, job.SetDetails(ctx, nil /* txn */, newDetails))
		require.NoError(t, exp.verify(job.ID(), jobs.StatusRunning))
	})

	t.Run("set details fails", func(t *testing.T) {
		job, exp := startLeasedJob(t, defaultRecord)
		require.NoError(t, exp.verify(job.ID(), jobs.StatusRunning))
		_, err := exp.DB.Exec(updateStatusStmt, jobs.StatusCancelRequested, job.ID())
		require.NoError(t, err)
		require.Error(t, job.SetDetails(ctx, nil /* txn */, jobspb.ImportDetails{URIs: []string{"new"}}))
		require.NoError(t, exp.verify(job.ID(), jobs.StatusCancelRequested))
	})

	t.Run("set progress works", func(t *testing.T) {
		job, exp := startLeasedJob(t, defaultRecord)
		require.NoError(t, exp.verify(job.ID(), jobs.StatusRunning))
		newProgress := jobspb.ImportProgress{ResumePos: []int64{42}}
		exp.Record.Progress = newProgress
		require.NoError(t, job.SetProgress(ctx, nil /* txn */, newProgress))
		require.NoError(t, exp.verify(job.ID(), jobs.StatusRunning))

		// Now change job's session id and check that updates are rejected.
		_, err := exp.DB.Exec(updateClaimStmt, "!@#!@$!$@#", job.ID())
		require.NoError(t, err)
		require.Error(t, job.SetDetails(ctx, nil /* txn */, newProgress))
		require.NoError(t, exp.verify(job.ID(), jobs.StatusRunning))
	})

	t.Run("set progress fails", func(t *testing.T) {
		job, exp := startLeasedJob(t, defaultRecord)
		require.NoError(t, exp.verify(job.ID(), jobs.StatusRunning))
		_, err := exp.DB.Exec(updateStatusStmt, jobs.StatusPauseRequested, job.ID())
		require.NoError(t, err)
		require.Error(t, job.SetProgress(ctx, nil /* txn */, jobspb.ImportProgress{ResumePos: []int64{42}}))
		require.NoError(t, exp.verify(job.ID(), jobs.StatusPauseRequested))
	})

	t.Run("job with created by fields", func(t *testing.T) {
		createdByType := "internal_test"

		resumerJob := make(chan *jobs.Job, 1)
		jobs.RegisterConstructor(
			jobspb.TypeBackup, func(j *jobs.Job, _ *cluster.Settings) jobs.Resumer {
				return jobs.FakeResumer{
					OnResume: func(ctx context.Context) error {
						resumerJob <- j
						return nil
					},
				}
			})

		jobID := registry.MakeJobID()
		record := jobs.Record{
			Details:   jobspb.BackupDetails{},
			Progress:  jobspb.BackupProgress{},
			CreatedBy: &jobs.CreatedByInfo{Name: createdByType, ID: 123},
		}
		job, err := registry.CreateAdoptableJobWithTxn(ctx, record, jobID, nil /* txn */)
		require.NoError(t, err)

		loadedJob, err := registry.LoadJob(ctx, jobID)
		require.NoError(t, err)
		require.NotNil(t, loadedJob.CreatedBy())
		require.Equal(t, job.CreatedBy(), loadedJob.CreatedBy())
		registry.TestingNudgeAdoptionQueue()
		resumedJob := <-resumerJob
		require.NotNil(t, resumedJob.CreatedBy())
		require.Equal(t, job.CreatedBy(), resumedJob.CreatedBy())

	})
}

// TestShowJobs manually inserts a row into system.jobs and checks that the
// encoded protobuf payload is properly decoded and visible in
// crdb_internal.jobs.
func TestShowJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	s, rawSQLDB, _ := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(rawSQLDB)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	session, err := s.SQLLivenessProvider().(sqlliveness.Provider).Session(ctx)
	require.NoError(t, err)

	// row represents a row returned from crdb_internal.jobs, but
	// *not* a row in system.jobs.
	type row struct {
		id                jobspb.JobID
		typ               string
		status            string
		description       string
		username          security.SQLUsername
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

	const instanceID = 7
	for _, in := range []row{
		{
			id:          42,
			typ:         "SCHEMA CHANGE",
			status:      "superfailed",
			description: "failjob",
			username:    security.MakeSQLUsernameFromPreNormalizedString("failure"),
			err:         "boom",
			// lib/pq returns time.Time objects with goofy locations, which breaks
			// reflect.DeepEqual without this time.FixedZone song and dance.
			// See: https://github.com/lib/pq/issues/329
			created:           timeutil.Unix(1, 0).In(time.FixedZone("", 0)),
			started:           timeutil.Unix(2, 0).In(time.FixedZone("", 0)),
			finished:          timeutil.Unix(3, 0).In(time.FixedZone("", 0)),
			modified:          timeutil.Unix(4, 0).In(time.FixedZone("", 0)),
			fractionCompleted: 0.42,
			coordinatorID:     instanceID,
			details:           jobspb.SchemaChangeDetails{},
		},
		{
			id:          43,
			typ:         "CHANGEFEED",
			status:      "running",
			description: "persistent feed",
			username:    security.MakeSQLUsernameFromPreNormalizedString("persistent"),
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
			coordinatorID: instanceID,
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
				UsernameProto:  in.username.EncodeProto(),
				Error:          in.err,
				Details:        jobspb.WrapPayloadDetails(in.details),
			})
			if err != nil {
				t.Fatal(err)
			}

			progress := &jobspb.Progress{
				ModifiedMicros: in.modified.UnixNano() / time.Microsecond.Nanoseconds(),
			}
			if !in.highWater.IsEmpty() {
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
				`INSERT INTO system.jobs (id, status, created, payload, progress, claim_session_id, claim_instance_id) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				in.id, in.status, in.created, inPayload, inProgress, session.ID().UnsafeBytes(), instanceID,
			)

			var out row
			var maybeFractionCompleted *float32
			var decimalHighWater *apd.Decimal
			var resultUsername string
			sqlDB.QueryRow(t, `
      SELECT job_id, job_type, status, created, description, started, finished, modified,
             fraction_completed, high_water_timestamp, user_name, ifnull(error, ''), coordinator_id
        FROM crdb_internal.jobs WHERE job_id = $1`, in.id).Scan(
				&out.id, &out.typ, &out.status, &out.created, &out.description, &out.started,
				&out.finished, &out.modified, &maybeFractionCompleted, &decimalHighWater, &resultUsername,
				&out.err, &out.coordinatorID,
			)
			out.username = security.MakeSQLUsernameFromPreNormalizedString(resultUsername)

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
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	s, rawSQLDB, _ := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(rawSQLDB)
	defer s.Stopper().Stop(context.Background())

	// row represents a row returned from crdb_internal.jobs, but
	// *not* a row in system.jobs.
	type row struct {
		id      jobspb.JobID
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
			UsernameProto: security.RootUserName().EncodeProto(),
			Details:       jobspb.WrapPayloadDetails(in.details),
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
	defer log.Scope(t).Close(t)

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
	var jobID jobspb.JobID
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
	defer log.Scope(t).Close(t)
	// Canceling a job relies on adopt daemon to move the job to state reverting.
	args := base.TestServerArgs{Knobs: base.TestingKnobs{
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}}

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, args)
	defer s.Stopper().Stop(ctx)
	registry := s.JobRegistry().(*jobs.Registry)
	mockJob := jobs.Record{
		Username: security.RootUserName(),
		Details:  jobspb.ImportDetails{},
		Progress: jobspb.ImportProgress{},
	}
	done := make(chan struct{})
	defer close(done)
	jobs.RegisterConstructor(
		jobspb.TypeImport, func(_ *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return jobs.FakeResumer{
				OnResume: func(ctx context.Context) error {
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
		id     jobspb.JobID
		status string
	}
	var out row

	t.Run("show job", func(t *testing.T) {
		// Start a job and cancel it so it is in state finished and then query it with
		// SHOW JOB WHEN COMPLETE.
		job, err := jobs.TestingCreateAndStartJob(ctx, registry, s.DB(), mockJob)
		if err != nil {
			t.Fatal(err)
		}
		group := ctxgroup.WithContext(ctx)
		group.GoCtx(func(ctx context.Context) error {
			if err := db.QueryRowContext(
				ctx,
				`SELECT job_id, status
				 FROM [SHOW JOB WHEN COMPLETE $1]`,
				job.ID()).Scan(&out.id, &out.status); err != nil {
				return err
			}
			if out.status != "canceled" {
				return errors.Errorf(
					"Expected status 'canceled' but got '%s'", out.status)
			}
			if job.ID() != out.id {
				return errors.Errorf(
					"Expected job id %d but got %d", job.ID(), out.id)
			}
			return nil
		})
		// Give a chance for the above group to schedule in order to test that
		// SHOW JOBS WHEN COMPLETE does block until the job is canceled.
		time.Sleep(2 * time.Millisecond)
		if _, err = db.ExecContext(ctx, "CANCEL JOB $1", job.ID()); err == nil {
			err = group.Wait()
		}
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("show jobs", func(t *testing.T) {
		// Start two jobs and cancel the first one to make sure the
		// query still blocks until the second job is also canceled.
		var jobsToStart [2]*jobs.StartableJob
		for i := range jobsToStart {
			job, err := jobs.TestingCreateAndStartJob(ctx, registry, s.DB(), mockJob)
			if err != nil {
				t.Fatal(err)
			}
			jobsToStart[i] = job
		}
		if _, err := db.ExecContext(ctx, "CANCEL JOB $1", jobsToStart[0].ID()); err != nil {
			t.Fatal(err)
		}
		group := ctxgroup.WithContext(ctx)
		group.GoCtx(func(ctx context.Context) error {
			rows, err := db.QueryContext(ctx,
				`SELECT job_id, status
				 FROM [SHOW JOBS WHEN COMPLETE (SELECT $1 UNION SELECT $2)]`,
				jobsToStart[0].ID(), jobsToStart[1].ID())
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
				case jobsToStart[0].ID():
				case jobsToStart[1].ID():
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
						jobsToStart[0].ID(), jobsToStart[1].ID(), out.id)
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
		if _, err = db.ExecContext(ctx, "CANCEL JOB $1", jobsToStart[1].ID()); err == nil {
			err = group.Wait()
		}
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestJobInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.ResetConstructors()()

	// Set the adoption interval to be very long to test the adoption channel.
	args := base.TestServerArgs{Knobs: base.TestingKnobs{
		JobsTestingKnobs: jobs.NewTestingKnobsWithIntervals(time.Hour, time.Hour, time.Hour, time.Hour)},
	}
	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, args)
	defer s.Stopper().Stop(ctx)

	// Accessed atomically.
	var hasRun int32
	var job *jobs.Job

	defer sql.ClearPlanHooks()
	// Piggy back on BACKUP to be able to create a succeeding test job.
	sql.AddPlanHook(
		"test",
		func(_ context.Context, stmt tree.Statement, execCtx sql.PlanHookState,
		) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
			st, ok := stmt.(*tree.Backup)
			if !ok {
				return nil, nil, nil, false, nil
			}
			fn := func(ctx context.Context, _ []sql.PlanNode, _ chan<- tree.Datums) error {
				var err error
				job, err = execCtx.ExtendedEvalContext().QueueJob(
					ctx,
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
			OnResume: func(ctx context.Context) error {
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
		"test",
		func(_ context.Context, stmt tree.Statement, execCtx sql.PlanHookState,
		) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
			_, ok := stmt.(*tree.Restore)
			if !ok {
				return nil, nil, nil, false, nil
			}
			fn := func(ctx context.Context, _ []sql.PlanNode, _ chan<- tree.Datums) error {
				var err error
				job, err = execCtx.ExtendedEvalContext().QueueJob(
					ctx,
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
			OnResume: func(_ context.Context) error {
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
		_, err = registry.LoadJob(ctx, job.ID())
		require.Error(t, err, "the job should not exist after the txn is rolled back")
		require.True(t, jobs.HasJobNotFoundError(err))

		sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
		// Just in case the job was scheduled let's wait for it to finish
		// to avoid a race.
		sqlRunner.Exec(t, "SHOW JOB WHEN COMPLETE $1", job.ID())
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
		j, err := registry.LoadJob(ctx, job.ID())
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

func TestStartableJobMixedVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.ResetConstructors()()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.TestingBinaryMinSupportedVersion,
		false, /* initializeVersion */
	)
	s, sqlDB, db := serverutils.StartServer(t, base.TestServerArgs{
		Settings: st,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				BinaryVersionOverride:          clusterversion.TestingBinaryMinSupportedVersion,
				DisableAutomaticVersionUpgrade: make(chan struct{}),
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	jr := s.JobRegistry().(*jobs.Registry)
	_, err := sqlDB.Exec("SELECT now()")
	require.NoError(t, err)

	jobs.RegisterConstructor(jobspb.TypeImport, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return jobs.FakeResumer{}
	})
	var j *jobs.StartableJob
	jobID := jr.MakeJobID()
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		err = jr.CreateStartableJobWithTxn(ctx, &j, jobID, txn, jobs.Record{
			Details:  jobspb.ImportDetails{},
			Progress: jobspb.ImportProgress{},
		})
		return err
	}))
	_, err = sqlDB.Exec("SET CLUSTER SETTING version = crdb_internal.node_executable_version()")
	require.NoError(t, err)
	require.NoError(t, j.Start(ctx))
	require.NoError(t, j.AwaitCompletion(ctx))
}

// TestStartableJobErrors tests that the StartableJob returns the expected
// errors when used incorrectly and performs the appropriate cleanup in
// CleanupOnRollback.
func TestStartableJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.ResetConstructors()()

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	jr := s.JobRegistry().(*jobs.Registry)
	var resumeFunc atomic.Value
	resumeFunc.Store(func(ctx context.Context) error {
		return nil
	})
	setResumeFunc := func(f func(ctx context.Context) error) (cleanup func()) {
		prev := resumeFunc.Load()
		resumeFunc.Store(f)
		return func() { resumeFunc.Store(prev) }
	}
	jobs.RegisterConstructor(jobspb.TypeRestore, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return jobs.FakeResumer{
			OnResume: func(ctx context.Context) error {
				return resumeFunc.Load().(func(ctx context.Context) error)(ctx)
			},
		}
	})
	woodyP, _ := security.MakeSQLUsernameFromUserInput("Woody Pride", security.UsernameValidation)
	rec := jobs.Record{
		Description:   "There's a snake in my boot!",
		Username:      woodyP,
		DescriptorIDs: []descpb.ID{1, 2, 3},
		Details:       jobspb.RestoreDetails{},
		Progress:      jobspb.RestoreProgress{},
	}
	createStartableJob := func(t *testing.T) (sj *jobs.StartableJob) {
		jobID := jr.MakeJobID()
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			return jr.CreateStartableJobWithTxn(ctx, &sj, jobID, txn, rec)
		}))
		return sj
	}
	t.Run("Start called more than once", func(t *testing.T) {
		sj := createStartableJob(t)
		err := sj.Start(ctx)
		require.NoError(t, err)
		err = sj.Start(ctx)
		require.Regexp(t, `StartableJob \d+ cannot be started more than once`, err)
		require.NoError(t, sj.AwaitCompletion(ctx))
	})
	t.Run("Start called with active txn", func(t *testing.T) {
		txn := db.NewTxn(ctx, "test")
		defer func() {
			require.NoError(t, txn.Rollback(ctx))
		}()
		var sj *jobs.StartableJob
		err := jr.CreateStartableJobWithTxn(ctx, &sj, jr.MakeJobID(), txn, rec)
		require.NoError(t, err)
		err = sj.Start(ctx)
		require.Regexp(t, `cannot resume .* job which is not committed`, err)
	})
	t.Run("Start called with aborted txn", func(t *testing.T) {
		txn := db.NewTxn(ctx, "test")
		var sj *jobs.StartableJob
		err := jr.CreateStartableJobWithTxn(ctx, &sj, jr.MakeJobID(), txn, rec)
		require.NoError(t, err)
		require.NoError(t, txn.Rollback(ctx))
		err = sj.Start(ctx)
		require.Regexp(t, `cannot resume .* job which is not committed`, err)
	})
	t.Run("CleanupOnRollback called with active txn", func(t *testing.T) {
		txn := db.NewTxn(ctx, "test")
		defer func() {
			require.NoError(t, txn.Rollback(ctx))
		}()
		var sj *jobs.StartableJob
		err := jr.CreateStartableJobWithTxn(ctx, &sj, jr.MakeJobID(), txn, rec)
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
		var sj *jobs.StartableJob
		err := jr.CreateStartableJobWithTxn(ctx, &sj, jr.MakeJobID(), txn, rec)
		require.NoError(t, err)
		require.NoError(t, txn.Rollback(ctx))
		require.NoError(t, sj.CleanupOnRollback(ctx))
		for _, id := range jr.CurrentlyRunningJobs() {
			require.NotEqual(t, id, sj.ID())
		}
	})
	t.Run("Cancel", func(t *testing.T) {
		txn := db.NewTxn(ctx, "test")
		var sj *jobs.StartableJob
		err := jr.CreateStartableJobWithTxn(ctx, &sj, jr.MakeJobID(), txn, rec)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(ctx))
		require.NoError(t, sj.Cancel(ctx))
		status, err := sj.TestingCurrentStatus(ctx, nil /* txn */)
		require.NoError(t, err)
		require.Equal(t, jobs.StatusCancelRequested, status)
		// Start should fail since we have already called cancel on the job.
		err = sj.Start(ctx)
		require.Regexp(t, "cannot be started more than once", err)
	})
	setUpRunTest := func(t *testing.T) (
		sj *jobs.StartableJob,
		resultCh <-chan tree.Datums,
		blockResume func() (waitForBlocked func() (unblockWithError func(error))),
		cleanup func(),
	) {
		type blockResp struct {
			errCh chan error
		}
		blockCh := make(chan chan blockResp, 1)
		blockResume = func() (waitForBlocked func() (unblockWithError func(error))) {
			blockRequest := make(chan blockResp, 1)
			blockCh <- blockRequest // from test to resumer
			return func() (unblockWithError func(error)) {
				blocked := <-blockRequest // from resumer to test
				return func(err error) {
					blocked.errCh <- err // from test to resumer
				}
			}
		}
		cleanup = setResumeFunc(func(ctx context.Context) error {
			select {
			case blockRequest := <-blockCh:
				unblock := make(chan error)
				blockRequest <- blockResp{
					errCh: unblock,
				}
				if err := <-unblock; err != nil {
					return err
				}
			default:
			}
			return nil
		})
		clientResults := make(chan tree.Datums)
		jobID := jr.MakeJobID()
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			return jr.CreateStartableJobWithTxn(ctx, &sj, jobID, txn, rec)
		}))
		return sj, clientResults, blockResume, cleanup
	}
	t.Run("Run - error during resume", func(t *testing.T) {
		sj, _, blockResume, cleanup := setUpRunTest(t)
		defer cleanup()
		waitForBlocked := blockResume()
		runErr := make(chan error)
		require.NoError(t, sj.Start(ctx))
		go func() { runErr <- sj.AwaitCompletion(ctx) }()
		unblock := waitForBlocked()
		unblock(errors.New("boom"))
		require.Regexp(t, "boom", <-runErr)
	})
	t.Run("Run - client canceled", func(t *testing.T) {
		sj, _, blockResume, cleanup := setUpRunTest(t)
		defer cleanup()
		ctxToCancel, cancel := context.WithCancel(ctx)
		runErr := make(chan error)
		waitForBlocked := blockResume()
		require.NoError(t, sj.Start(ctx))
		go func() { runErr <- sj.AwaitCompletion(ctxToCancel) }()
		unblock := waitForBlocked()
		cancel()
		require.Regexp(t, context.Canceled, <-runErr)
		unblock(nil)
		testutils.SucceedsSoon(t, func() error {
			loaded, err := jr.LoadJob(ctx, sj.ID())
			require.NoError(t, err)
			st, err := loaded.TestingCurrentStatus(ctx, nil /* txn */)
			require.NoError(t, err)
			if st != jobs.StatusSucceeded {
				return errors.Errorf("expected %s, got %s", jobs.StatusSucceeded, st)
			}
			return nil
		})
	})
}

// TestStartableJobTxnRetry tests that in the presence of transaction retries,
// StartableJobs created in the transaction are correctly registered exactly
// once.
func TestStartableJobTxnRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.ResetConstructors()()

	ctx := context.Background()

	const txnName = "create job"
	haveInjectedRetry := false
	params := base.TestServerArgs{}
	params.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: func(ctx context.Context, r roachpb.BatchRequest) *roachpb.Error {
			if r.Txn == nil || r.Txn.Name != txnName {
				return nil
			}
			if _, ok := r.GetArg(roachpb.EndTxn); ok {
				if !haveInjectedRetry {
					haveInjectedRetry = true
					// Force a retry error the first time.
					return roachpb.NewError(roachpb.NewTransactionRetryError(roachpb.RETRY_REASON_UNKNOWN, "injected error"))
				}
			}
			return nil
		},
	}
	s, _, db := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	jr := s.JobRegistry().(*jobs.Registry)
	jobs.RegisterConstructor(jobspb.TypeRestore, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return jobs.FakeResumer{}
	})
	rec := jobs.Record{
		Details:  jobspb.RestoreDetails{},
		Progress: jobspb.RestoreProgress{},
	}

	jobID := jr.MakeJobID()
	var sj *jobs.StartableJob
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txn.SetDebugName(txnName)
		return jr.CreateStartableJobWithTxn(ctx, &sj, jobID, txn, rec)
	}))
	require.True(t, haveInjectedRetry)
	require.NoError(t, sj.Start(ctx))
}

// TestUnmigratedSchemaChangeJobs tests that schema change jobs created in 19.2
// that have not undergone a migration cannot be adopted, canceled, or paused.
func TestUnmigratedSchemaChangeJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.ResetConstructors()()

	ctx := context.Background()
	args := base.TestServerArgs{Knobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()}}
	s, sqlDB, _ := serverutils.StartServer(t, args)
	defer s.Stopper().Stop(ctx)

	registry := s.JobRegistry().(*jobs.Registry)

	// The default FormatVersion value in SchemaChangeDetails corresponds to a
	// pre-20.1 job.
	rec := jobs.Record{
		DescriptorIDs: []descpb.ID{1},
		Details:       jobspb.SchemaChangeDetails{},
		Progress:      jobspb.SchemaChangeProgress{},
	}

	t.Run("job is not adopted", func(t *testing.T) {
		defer jobs.ResetConstructors()()
		resuming := make(chan struct{})
		jobs.RegisterConstructor(jobspb.TypeSchemaChange, func(_ *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return jobs.FakeResumer{
				OnResume: func(ctx context.Context) error {
					resuming <- struct{}{}
					return nil
				},
			}
		})
		select {
		case <-resuming:
			t.Fatal("job was resumed")
		case <-time.After(100 * time.Millisecond):
			// With an adopt interval of 10 ms, within 100ms we can be reasonably sure
			// that the job was not adopted. At the very least, the test would be
			// flaky.
		}
	})

	t.Run("pause not supported", func(t *testing.T) {
		job, err := registry.CreateJobWithTxn(ctx, rec, registry.MakeJobID(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sqlDB.Exec("PAUSE JOB $1", job.ID()); !testutils.IsError(err, "cannot be paused in this version") {
			t.Fatal(err)
		}
	})

	t.Run("cancel not supported", func(t *testing.T) {
		job, err := registry.CreateJobWithTxn(ctx, rec, registry.MakeJobID(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sqlDB.Exec("CANCEL JOB $1", job.ID()); !testutils.IsError(err, "cannot be canceled in this version") {
			t.Fatal(err)
		}
	})
}

func TestRegistryTestingNudgeAdoptionQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.ResetConstructors()()

	ctx := context.Background()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	registry := s.JobRegistry().(*jobs.Registry)

	// The default FormatVersion value in SchemaChangeDetails corresponds to a
	// pre-20.1 job.
	rec := jobs.Record{
		DescriptorIDs: []descpb.ID{1},
		Details:       jobspb.BackupDetails{},
		Progress:      jobspb.BackupProgress{},
	}

	defer jobs.ResetConstructors()()
	resuming := make(chan struct{})
	jobs.RegisterConstructor(jobspb.TypeBackup, func(_ *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return jobs.FakeResumer{
			OnResume: func(ctx context.Context) error {
				resuming <- struct{}{}
				return nil
			},
		}
	})
	before := timeutil.Now()
	jobID := registry.MakeJobID()
	_, err := registry.CreateAdoptableJobWithTxn(ctx, rec, jobID, nil /* txn */)
	require.NoError(t, err)
	registry.TestingNudgeAdoptionQueue()
	// We want the job to be resumed very rapidly. We set this long timeout of 2s
	// to deal with extremely slow stressrace. The adoption interval is still
	// much larger than this so this should be a sufficient test.
	const aLongTime = 5 * time.Second
	select {
	case <-resuming:
	case <-time.After(aLongTime):
		t.Fatal("job was not adopted")
	}
	loaded, err := registry.LoadJob(ctx, jobID)
	require.NoError(t, err)
	started := timeutil.Unix(0, loaded.Payload().StartedMicros*1000)
	require.True(t, started.After(before),
		"started: %v, before:	%v", started, before)
}

func TestStatusSafeFormatter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	redacted := string(redact.Sprint(jobs.StatusCanceled).Redact())
	expected := string(jobs.StatusCanceled)
	require.Equal(t, expected, redacted)
}

func TestMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	resuming := make(chan chan error, 1)
	waitForErr := func(ctx context.Context) error {
		errCh := make(chan error)
		select {
		case resuming <- errCh:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case err := <-errCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	int64EqSoon := func(t *testing.T, f func() int64, exp int64) {
		t.Helper()
		testutils.SucceedsSoon(t, func() error {
			if vv := f(); vv != exp {
				return errors.Errorf("expected %d, got %d", exp, vv)
			}
			return nil
		})
	}
	res := jobs.FakeResumer{
		OnResume: func(ctx context.Context) error {
			return waitForErr(ctx)
		},
		FailOrCancel: func(ctx context.Context) error {
			return waitForErr(ctx)
		},
	}
	jobs.RegisterConstructor(jobspb.TypeBackup, func(_ *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return res
	})
	jobs.RegisterConstructor(jobspb.TypeImport, func(_ *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return res
	})
	setup := func(t *testing.T) (
		s serverutils.TestServerInterface, db *gosql.DB, r *jobs.Registry, cleanup func(),
	) {
		jobConstructorCleanup := jobs.ResetConstructors()
		args := base.TestServerArgs{Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()},
		}
		s, db, _ = serverutils.StartServer(t, args)
		r = s.JobRegistry().(*jobs.Registry)
		return s, db, r, func() {
			jobConstructorCleanup()
			s.Stopper().Stop(ctx)
		}
	}

	t.Run("success", func(t *testing.T) {
		_, _, registry, cleanup := setup(t)
		defer cleanup()
		rec := jobs.Record{
			DescriptorIDs: []descpb.ID{1},
			Details:       jobspb.BackupDetails{},
			Progress:      jobspb.BackupProgress{},
		}
		_, err := registry.CreateAdoptableJobWithTxn(ctx, rec, registry.MakeJobID(), nil /* txn */)
		require.NoError(t, err)
		errCh := <-resuming
		backupMetrics := registry.MetricsStruct().JobMetrics[jobspb.TypeBackup]
		require.Equal(t, int64(1), backupMetrics.CurrentlyRunning.Value())
		errCh <- nil
		int64EqSoon(t, backupMetrics.ResumeCompleted.Count, 1)
	})
	t.Run("restart, pause, resume, then success", func(t *testing.T) {
		_, db, registry, cleanup := setup(t)
		defer cleanup()
		rec := jobs.Record{
			DescriptorIDs: []descpb.ID{1},
			Details:       jobspb.ImportDetails{},
			Progress:      jobspb.ImportProgress{},
		}
		importMetrics := registry.MetricsStruct().JobMetrics[jobspb.TypeImport]

		jobID := registry.MakeJobID()
		_, err := registry.CreateAdoptableJobWithTxn(ctx, rec, jobID, nil /* txn */)
		require.NoError(t, err)
		{
			// Fail the Resume with a retriable error.
			errCh := <-resuming
			require.Equal(t, int64(1), importMetrics.CurrentlyRunning.Value())
			errCh <- jobs.MarkAsRetryJobError(errors.New("boom"))
			int64EqSoon(t, importMetrics.ResumeRetryError.Count, 1)
			// It will be retried.
			int64EqSoon(t, importMetrics.CurrentlyRunning.Value, 1)
		}
		{
			// We'll pause the job this time around and make sure it stops running.
			<-resuming
			require.Equal(t, int64(1), importMetrics.CurrentlyRunning.Value())
			require.NoError(t, registry.PauseRequested(ctx, nil, jobID, "for testing"))
			int64EqSoon(t, importMetrics.ResumeRetryError.Count, 2)
			require.Equal(t, int64(0), importMetrics.ResumeFailed.Count())
			require.Equal(t, int64(0), importMetrics.ResumeCompleted.Count())
			require.Equal(t, int64(0), importMetrics.CurrentlyRunning.Value())
		}
		{
			// Wait for the job to be marked paused.
			tdb := sqlutils.MakeSQLRunner(db)
			q := fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %d", jobID)
			tdb.CheckQueryResultsRetry(t, q, [][]string{{"paused"}})

			var payloadBytes []byte
			var payload jobspb.Payload
			var status string
			tdb.QueryRow(t, fmt.Sprintf("SELECT status, payload FROM system.jobs where id = %d", jobID)).Scan(
				&status, &payloadBytes)
			require.Equal(t, "paused", status)
			require.NoError(t, protoutil.Unmarshal(payloadBytes, &payload))
			require.Equal(t, "for testing", payload.PauseReason)
		}
		{
			// Now resume the job and let it succeed.
			require.NoError(t, registry.Unpause(ctx, nil, jobID))
			errCh := <-resuming
			require.Equal(t, int64(1), importMetrics.CurrentlyRunning.Value())
			errCh <- nil
			int64EqSoon(t, importMetrics.ResumeCompleted.Count, 1)
		}
	})
	t.Run("failure then restarts in revert", func(t *testing.T) {
		_, _, registry, cleanup := setup(t)
		defer cleanup()
		rec := jobs.Record{
			DescriptorIDs: []descpb.ID{1},
			Details:       jobspb.ImportDetails{},
			Progress:      jobspb.ImportProgress{},
		}
		importMetrics := registry.MetricsStruct().JobMetrics[jobspb.TypeImport]

		_, err := registry.CreateAdoptableJobWithTxn(ctx, rec, registry.MakeJobID(), nil /* txn */)
		require.NoError(t, err)
		{
			// Fail the Resume with a permanent error.
			errCh := <-resuming
			require.Equal(t, int64(1), importMetrics.CurrentlyRunning.Value())
			errCh <- errors.Errorf("boom")
			int64EqSoon(t, importMetrics.ResumeFailed.Count, 1)
			require.Equal(t, int64(0), importMetrics.ResumeCompleted.Count())
			require.Equal(t, int64(0), importMetrics.ResumeRetryError.Count())
		}
		{
			// We'll inject retriable errors in OnFailOrCancel.
			errCh := <-resuming
			require.Equal(t, int64(1), importMetrics.CurrentlyRunning.Value())
			errCh <- jobs.MarkAsRetryJobError(errors.New("boom"))
			int64EqSoon(t, importMetrics.FailOrCancelRetryError.Count, 1)
		}
		{
			errCh := <-resuming
			require.Equal(t, int64(1), importMetrics.CurrentlyRunning.Value())
			errCh <- nil
			int64EqSoon(t, importMetrics.FailOrCancelCompleted.Count, 1)
		}
	})
	t.Run("fail, pause, resume, then success on failure", func(t *testing.T) {
		_, db, registry, cleanup := setup(t)
		defer cleanup()
		rec := jobs.Record{
			DescriptorIDs: []descpb.ID{1},
			Details:       jobspb.ImportDetails{},
			Progress:      jobspb.ImportProgress{},
		}
		importMetrics := registry.MetricsStruct().JobMetrics[jobspb.TypeImport]

		jobID := registry.MakeJobID()
		_, err := registry.CreateAdoptableJobWithTxn(ctx, rec, jobID, nil /* txn */)
		require.NoError(t, err)
		{
			// Fail the Resume with a retriable error.
			errCh := <-resuming
			require.Equal(t, int64(1), importMetrics.CurrentlyRunning.Value())
			errCh <- errors.New("boom")
			int64EqSoon(t, importMetrics.ResumeFailed.Count, 1)
			// It will be retried.
			int64EqSoon(t, importMetrics.CurrentlyRunning.Value, 1)
		}
		{
			// We'll pause the job this time around and make sure it stops running.
			<-resuming
			require.Equal(t, int64(1), importMetrics.CurrentlyRunning.Value())
			require.NoError(t, registry.PauseRequested(ctx, nil, jobID, ""))
			int64EqSoon(t, importMetrics.FailOrCancelRetryError.Count, 1)
			require.Equal(t, int64(1), importMetrics.ResumeFailed.Count())
			require.Equal(t, int64(0), importMetrics.ResumeCompleted.Count())
			require.Equal(t, int64(0), importMetrics.CurrentlyRunning.Value())
		}
		{
			// Wait for the job to be marked paused.
			tdb := sqlutils.MakeSQLRunner(db)
			q := fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %d", jobID)
			tdb.CheckQueryResultsRetry(t, q, [][]string{{"paused"}})
		}
		{
			// Now resume the job and let it succeed.
			require.NoError(t, registry.Unpause(ctx, nil, jobID))
			errCh := <-resuming
			require.Equal(t, int64(1), importMetrics.CurrentlyRunning.Value())
			errCh <- nil
			int64EqSoon(t, importMetrics.FailOrCancelCompleted.Count, 1)
			int64EqSoon(t, importMetrics.FailOrCancelFailed.Count, 0)
		}
	})
}

// TestLoseLeaseDuringExecution tests that it is safe to call update during
// job execution when the job has lost its lease and that that update operation
// will fail.
//
// This is a regression test for #58049.
func TestLoseLeaseDuringExecution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.ResetConstructors()()

	// Disable the loops from messing with the job execution.
	knobs := base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithIntervals(time.Hour, time.Hour, time.Hour, time.Hour)}

	ctx := context.Background()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{Knobs: knobs})
	defer s.Stopper().Stop(ctx)
	registry := s.JobRegistry().(*jobs.Registry)

	// The default FormatVersion value in SchemaChangeDetails corresponds to a
	// pre-20.1 job.
	rec := jobs.Record{
		DescriptorIDs: []descpb.ID{1},
		Details:       jobspb.BackupDetails{},
		Progress:      jobspb.BackupProgress{},
	}

	defer jobs.ResetConstructors()()
	resumed := make(chan error, 1)
	jobs.RegisterConstructor(jobspb.TypeBackup, func(j *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return jobs.FakeResumer{
			OnResume: func(ctx context.Context) error {
				defer close(resumed)
				_, err := s.InternalExecutor().(sqlutil.InternalExecutor).Exec(
					ctx, "set-claim-null", nil, /* txn */
					`UPDATE system.jobs SET claim_session_id = NULL WHERE id = $1`,
					j.ID())
				assert.NoError(t, err)
				err = j.Update(ctx, nil /* txn */, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
					return nil
				})
				resumed <- err
				return err
			},
		}
	})

	_, err := registry.CreateJobWithTxn(ctx, rec, registry.MakeJobID(), nil)
	require.NoError(t, err)
	registry.TestingNudgeAdoptionQueue()
	require.Regexp(t, `expected session "\w+" but found NULL`, <-resumed)
}

func checkBundle(t *testing.T, zipFile string, expectedFiles []string) {
	t.Helper()
	r, err := zip.OpenReader(zipFile)
	require.NoError(t, err)

	// Make sure the bundle contains the expected list of files.
	filesInZip := make([]string, 0)
	for _, f := range r.File {
		if f.UncompressedSize64 == 0 {
			t.Fatalf("file %s is empty", f.Name)
		}
		filesInZip = append(filesInZip, f.Name)
	}
	sort.Strings(filesInZip)
	sort.Strings(expectedFiles)
	require.Equal(t, expectedFiles, filesInZip)
}

// TestPauseReason tests pausing a job with a user specified reason.
func TestPauseReason(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	registry := s.JobRegistry().(*jobs.Registry)
	defer s.Stopper().Stop(ctx)

	done := make(chan struct{})
	defer close(done)

	jobs.RegisterConstructor(jobspb.TypeImport, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return jobs.FakeResumer{
			OnResume: func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-done:
					return nil
				}
			},
		}
	})

	rec := jobs.Record{
		DescriptorIDs: []descpb.ID{1},
		Details:       jobspb.ImportDetails{},
		Progress:      jobspb.ImportProgress{},
	}
	tdb := sqlutils.MakeSQLRunner(db)

	jobID := registry.MakeJobID()
	_, err := registry.CreateAdoptableJobWithTxn(ctx, rec, jobID, nil /* txn */)
	require.NoError(t, err)

	// First wait until the job is running
	q := fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %d", jobID)
	tdb.CheckQueryResultsRetry(t, q, [][]string{{"running"}})

	getStatusAndPayload := func(t *testing.T, id jobspb.JobID) (string, jobspb.Payload) {
		var payloadBytes []byte
		var payload jobspb.Payload
		var status string
		tdb.QueryRow(t, "SELECT status, payload FROM system.jobs where id = $1", jobID).Scan(
			&status, &payloadBytes)
		require.NoError(t, protoutil.Unmarshal(payloadBytes, &payload))

		return status, payload
	}

	checkStatusAndPauseReason := func(t *testing.T, id jobspb.JobID, expStatus, expPauseReason string) {
		status, payload := getStatusAndPayload(t, id)
		require.Equal(t, expStatus, status, "status")
		require.Equal(t, expPauseReason, payload.PauseReason, "pause reason")
	}

	{
		// Next, pause the job with a reason. Wait for pause and make sure the pause reason is set.
		require.NoError(t, registry.PauseRequested(ctx, nil, jobID, "for testing"))
		tdb.CheckQueryResultsRetry(t, q, [][]string{{"paused"}})
		checkStatusAndPauseReason(t, jobID, "paused", "for testing")
	}

	{
		// Now resume the job. Verify that the job is running now, but the pause reason is still there.
		require.NoError(t, registry.Unpause(ctx, nil, jobID))
		tdb.CheckQueryResultsRetry(t, q, [][]string{{"running"}})

		checkStatusAndPauseReason(t, jobID, "running", "for testing")
	}
	{
		// Pause the job again with a different reason. Verify that the job is paused with the reason.
		require.NoError(t, registry.PauseRequested(ctx, nil, jobID, "second time"))
		tdb.CheckQueryResultsRetry(t, q, [][]string{{"paused"}})
		checkStatusAndPauseReason(t, jobID, "paused", "second time")
	}
}

// TestJobsRetry tests that (1) non-cancelable jobs retry if they fail with an
// error marked as permanent, (2) reverting job always retry instead of failing.
func TestJobsRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("retry non-cancelable running", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()
		// Make mockJob non-cancelable, ensuring that non-cancelable jobs are retried in running state.
		rts.mockJob.SetNonCancelable(rts.ctx, func(ctx context.Context, nonCancelable bool) bool {
			return true
		})
		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.s.DB(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		// First job run in running state.
		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)
		// Make Resume fail.
		rts.resumeCh <- errors.New("non-permanent error")
		rts.mu.e.ResumeExit++

		// Job should be retried in running state.
		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)
		rts.resumeCh <- jobs.MarkAsPermanentJobError(errors.New("permanent error"))
		rts.mu.e.ResumeExit++

		// Job should now revert.
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StatusReverting)
		rts.failOrCancelCh <- nil
		rts.mu.e.OnFailOrCancelExit = true

		close(rts.failOrCancelCh)
		close(rts.failOrCancelCheckCh)
		rts.check(t, jobs.StatusFailed)
	})

	t.Run("retry reverting", func(t *testing.T) {
		// - Create a job.
		// - Fail the job in resume to cause the job to revert.
		// - Fail the job in revert state using a non-retryable error.
		// - Make sure that the jobs is retried and is again in the revert state.
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()
		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.s.DB(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)

		// Make Resume fail.
		rts.resumeCh <- errors.New("failing resume to revert")
		rts.mu.e.ResumeExit++

		// Job is now reverting.
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StatusReverting)

		// Fail the job in reverting state without a retryable error.
		rts.failOrCancelCh <- errors.New("failing with a non-retryable error")
		rts.mu.e.OnFailOrCancelExit = true

		// Job should be retried.
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StatusReverting)
		rts.failOrCancelCh <- nil
		rts.mu.e.OnFailOrCancelExit = true

		close(rts.failOrCancelCh)
		close(rts.failOrCancelCheckCh)
		rts.check(t, jobs.StatusFailed)
	})

	t.Run("retry non-cancelable reverting", func(t *testing.T) {
		// - Create a non-cancelable job.
		// - Fail the job in resume with a permanent error to cause the job to revert.
		// - Fail the job in revert state using a permanent error to ensure that the
		//   retries with a permanent error as well.
		// - Make sure that the jobs is retried and is again in the revert state.
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()
		// Make mockJob non-cancelable, ensuring that non-cancelable jobs are retried in reverting state.
		rts.mockJob.SetNonCancelable(rts.ctx, func(ctx context.Context, nonCancelable bool) bool {
			return true
		})
		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.s.DB(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)

		// Make Resume fail with a permanent error.
		rts.resumeCh <- jobs.MarkAsPermanentJobError(errors.New("permanent error"))
		rts.mu.e.ResumeExit++

		// Job is now reverting.
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StatusReverting)

		// Fail the job in reverting state with a permanent error a retryable error.
		rts.failOrCancelCh <- jobs.MarkAsPermanentJobError(errors.New("permanent error"))
		rts.mu.e.OnFailOrCancelExit = true

		// Job should be retried.
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StatusReverting)
		rts.failOrCancelCh <- nil
		rts.mu.e.OnFailOrCancelExit = true

		close(rts.failOrCancelCh)
		close(rts.failOrCancelCheckCh)
		rts.check(t, jobs.StatusFailed)
	})
}

func TestPausepoints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, db := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	registry := s.JobRegistry().(*jobs.Registry)
	defer s.Stopper().Stop(ctx)

	jobs.RegisterConstructor(jobspb.TypeImport, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return jobs.FakeResumer{
			OnResume: func(ctx context.Context) error {
				if err := registry.CheckPausepoint("test_pause_foo"); err != nil {
					return err
				}
				return nil
			},
		}
	})

	rec := jobs.Record{
		DescriptorIDs: []descpb.ID{1},
		Details:       jobspb.ImportDetails{},
		Progress:      jobspb.ImportProgress{},
	}

	for _, tc := range []struct {
		name     string
		points   string
		expected jobs.Status
	}{
		{"none", "", jobs.StatusSucceeded},
		{"pausepoint-only", "test_pause_foo", jobs.StatusPaused},
		{"other-var-only", "test_pause_bar", jobs.StatusSucceeded},
		{"pausepoint-and-other", "test_pause_bar,test_pause_foo,test_pause_baz", jobs.StatusPaused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sqlDB.Exec("SET CLUSTER SETTING jobs.debug.pausepoints = $1", tc.points)
			require.NoError(t, err)

			jobID := registry.MakeJobID()
			var sj *jobs.StartableJob
			require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
				return registry.CreateStartableJobWithTxn(ctx, &sj, jobID, txn, rec)
			}))
			require.NoError(t, sj.Start(ctx))
			if tc.expected == jobs.StatusSucceeded {
				require.NoError(t, sj.AwaitCompletion(ctx))
			} else {
				require.Error(t, sj.AwaitCompletion(ctx))
			}
			status, err := sj.TestingCurrentStatus(ctx, nil)
			// Map pause-requested to paused to avoid races.
			if status == jobs.StatusPauseRequested {
				status = jobs.StatusPaused
			}
			require.Equal(t, tc.expected, status)
			require.NoError(t, err)
		})
	}
}
