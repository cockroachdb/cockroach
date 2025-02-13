// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs_test

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apd "github.com/cockroachdb/apd/v3"
	_ "github.com/cockroachdb/cockroach/pkg/backup" // we use backup jobs as placeholders.
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/gogo/protobuf/types"
	"github.com/google/go-cmp/cmp"
	"github.com/kr/pretty"
	io_prometheus_client "github.com/prometheus/client_model/go"
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

func (expected *expectation) verify(id jobspb.JobID, expectedState jobs.State) error {
	var stateString string
	var created time.Time
	var payloadBytes []byte
	var progressBytes []byte
	if err := expected.DB.QueryRow(
		`SELECT status, created, payload, progress FROM crdb_internal.system_jobs WHERE id = $1`, id,
	).Scan(
		&stateString, &created, &payloadBytes, &progressBytes,
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
	state := jobs.State(stateString)
	if e, a := expectedState, state; e != a {
		return errors.Errorf("expected state %v, got %v", e, a)
	}
	if e, a := expected.Type, payload.Type(); e != a {
		return errors.Errorf("expected type %v, got type %v", e, a)
	}
	if e, a := expected.FractionCompleted, progress.GetFractionCompleted(); e != a {
		return errors.Errorf("expected fraction completed %f, got %f", e, a)
	}

	started := timeutil.FromUnixMicros(payload.StartedMicros)
	if started.Equal(timeutil.UnixEpoch) && state == jobs.StateSucceeded {
		return errors.Errorf("started time is empty but job claims to be successful")
	}
	if state == jobs.StateRunning || state == jobs.StatePauseRequested {
		return nil
	}

	if e, a := expected.Error, payload.Error; e != a {
		return errors.Errorf("expected error %q, got %q", e, a)
	}
	return nil
}

type counters struct {
	ResumeExit int
	// These sometimes retry so just use bool.
	ResumeStart, OnFailOrCancelStart, OnFailOrCancelExit, Success bool
}

type registryTestSuite struct {
	ctx                context.Context
	s                  serverutils.TestServerInterface
	tempDirCleanup     func()
	outerDB            *gosql.DB
	sqlDB              *sqlutils.SQLRunner
	registry           *jobs.Registry
	done               chan struct{}
	mockJob            jobs.Record
	job                *jobs.StartableJob
	statusChangeLogSpy *logtestutils.StructuredLogSpy[eventpb.StatusChange]
	logSpyCleanup      func()
	mu                 struct {
		syncutil.Mutex
		a counters
		e counters
	}
	resumeCh            chan error
	progressCh          chan struct{}
	failOrCancelCh      chan error
	resumeCheckCh       chan struct{}
	failOrCancelCheckCh chan struct{}

	// beforeUpdate is invoked in the BeforeUpdate testing knob if non-nil.
	beforeUpdate func(orig, updated jobs.JobMetadata) error

	// afterJobStateMachine is invoked in the AfterJobStateMachine testing knob if
	// non-nil.
	afterJobStateMachine func(jobspb.JobID)

	// Instead of a ch for success, use a variable because it can retry since it
	// is in a transaction.
	successErr error

	// controls whether job resumers will ask for a real tracing span.
	traceRealSpan bool
}

var _ jobs.TraceableJob = (*jobstest.FakeResumer)(nil)

func (rts *registryTestSuite) setUp(t *testing.T) func() {
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
		knobs.AfterJobStateMachine = func(id jobspb.JobID) {
			if rts.afterJobStateMachine != nil {
				rts.afterJobStateMachine(id)
			}
		}
		args.Knobs.JobsTestingKnobs = knobs
		args.Knobs.SpanConfig = &spanconfig.TestingKnobs{
			ManagerDisableJobCreation: true,
		}
		args.Knobs.UpgradeManager = &upgradebase.TestingKnobs{
			DontUseJobs:                           true,
			SkipJobMetricsPollingJobBootstrap:     true,
			SkipUpdateSQLActivityJobBootstrap:     true,
			SkipMVCCStatisticsJobBootstrap:        true,
			SkipUpdateTableMetadataCacheBootstrap: true,
			SkipSqlActivityFlushJobBootstrap:      true,
		}
		args.Knobs.KeyVisualizer = &keyvisualizer.TestingKnobs{SkipJobBootstrap: true}

		if rts.traceRealSpan {
			baseDir, dirCleanupFn := testutils.TempDir(t)
			rts.tempDirCleanup = dirCleanupFn
			traceDir := filepath.Join(baseDir, "trace_dir")
			args.TraceDir = traceDir
		}
	}

	spy := logtestutils.NewStructuredLogSpy(
		t,
		[]logpb.Channel{logpb.Channel_OPS},
		[]string{"status_change"},
		func(entry logpb.Entry) (eventpb.StatusChange, error) {
			var structuredPayload eventpb.StatusChange
			err := json.Unmarshal([]byte(entry.Message[entry.StructuredStart:entry.StructuredEnd]), &structuredPayload)
			if err != nil {
				return structuredPayload, err
			}
			return structuredPayload, nil
		},
	)

	rts.statusChangeLogSpy = spy
	rts.logSpyCleanup = log.InterceptWith(rts.ctx, spy)
	rts.s, rts.outerDB, _ = serverutils.StartServer(t, args)
	rts.sqlDB = sqlutils.MakeSQLRunner(rts.outerDB)
	rts.registry = rts.s.JobRegistry().(*jobs.Registry)
	rts.done = make(chan struct{})
	rts.mockJob = jobs.Record{Details: jobspb.ImportDetails{}, Progress: jobspb.ImportProgress{}, Username: username.TestUserName()}

	rts.resumeCh = make(chan error)
	rts.progressCh = make(chan struct{})
	rts.failOrCancelCh = make(chan error)
	rts.resumeCheckCh = make(chan struct{})
	rts.failOrCancelCheckCh = make(chan struct{})
	rts.sqlDB.Exec(t, "CREATE USER testuser")

	return jobs.TestingRegisterConstructor(jobspb.TypeImport, func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return jobstest.FakeResumer{
			TraceRealSpan: rts.traceRealSpan,
			OnResume: func(ctx context.Context) error {
				t.Log("Starting resume")
				l, ok := pprof.Label(ctx, "job")
				assert.True(t, ok)
				assert.Contains(t, l, fmt.Sprintf("%d", job.ID()))
				payload := job.Payload()
				jobType := payload.Type().String()
				assert.Contains(t, l, jobType)
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
						err := job.NoTxn().FractionProgressed(rts.ctx, jobs.FractionUpdater(0))
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
		}
	}, jobs.UsesTenantCostControl)
}

func (rts *registryTestSuite) tearDown() {
	close(rts.resumeCh)
	close(rts.progressCh)
	close(rts.resumeCheckCh)
	close(rts.done)
	rts.logSpyCleanup()
	rts.s.Stopper().Stop(rts.ctx)
	jobs.ResetConstructors()()
	if rts.tempDirCleanup != nil {
		rts.tempDirCleanup()
	}
}

func (rts *registryTestSuite) check(t *testing.T, expectedState jobs.State) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		rts.mu.Lock()
		defer rts.mu.Unlock()
		if diff := cmp.Diff(rts.mu.e, rts.mu.a); diff != "" {
			return errors.Errorf("unexpected diff: %s", diff)
		}
		if expectedState == "" {
			return nil
		}
		st, err := rts.job.TestingCurrentState(rts.ctx)
		if err != nil {
			return err
		}
		if expectedState != st {
			return errors.Errorf("expected job state: %s but got: %s", expectedState, st)
		}
		return nil
	})
}

func (rts *registryTestSuite) checkStateChangeLog(
	t *testing.T, expectedNewState jobs.State, expectedPrevState jobs.State, expectedError string,
) {
	testutils.SucceedsSoon(t, func() error {
		logs := rts.statusChangeLogSpy.GetUnreadLogs(logpb.Channel_OPS)
		for i, jobEventsLog := range logs {
			if jobEventsLog.JobID == int64(rts.job.ID()) &&
				jobEventsLog.PreviousStatus == string(expectedPrevState) &&
				jobEventsLog.NewStatus == string(expectedNewState) &&
				strings.Contains(jobEventsLog.Error, expectedError) {
				rts.statusChangeLogSpy.SetLastNLogsAsUnread(logpb.Channel_OPS, len(logs)-i+1)
				return nil
			}
		}
		return errors.Errorf("expected log for state change from %s to %s", expectedPrevState, expectedNewState)
	})
}

func (rts *registryTestSuite) idb() isql.DB {
	return rts.s.InternalDB().(isql.DB)
}

func TestRegistryLifecycle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("normal success", func(t *testing.T) {
		rts := registryTestSuite{}
		defer rts.setUp(t)()
		defer rts.tearDown()

		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.idb(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StateRunning)
		rts.checkStateChangeLog(t, jobs.StateRunning, jobs.StateRunning, "")
		rts.resumeCh <- nil
		rts.mu.e.ResumeExit++
		rts.mu.e.Success = true
		rts.check(t, jobs.StateSucceeded)
		rts.checkStateChangeLog(t, jobs.StateSucceeded, jobs.StateRunning, "")
		t.Log("Done")
	})

	t.Run("create separately success", func(t *testing.T) {
		rts := registryTestSuite{}
		defer rts.setUp(t)()
		defer rts.tearDown()

		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.idb(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.check(t, jobs.StateRunning)
		rts.checkStateChangeLog(t, jobs.StateRunning, jobs.StateRunning, "")

		rts.resumeCheckCh <- struct{}{}
		rts.resumeCh <- nil
		rts.mu.e.ResumeExit++
		rts.mu.e.Success = true
		rts.check(t, jobs.StateSucceeded)
		rts.checkStateChangeLog(t, jobs.StateSucceeded, jobs.StateRunning, "")
	})

	t.Run("pause running", func(t *testing.T) {
		rts := registryTestSuite{}
		defer rts.setUp(t)()
		defer rts.tearDown()

		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.idb(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StateRunning)
		rts.checkStateChangeLog(t, jobs.StateRunning, jobs.StateRunning, "")

		rts.sqlDB.Exec(t, "PAUSE JOB $1", j.ID())
		rts.check(t, jobs.StatePaused)
		rts.checkStateChangeLog(t, jobs.StatePauseRequested, jobs.StateRunning, "")
		rts.checkStateChangeLog(t, jobs.StatePaused, jobs.StatePauseRequested, "")

		rts.sqlDB.Exec(t, "PAUSE JOB $1", j.ID())
		rts.check(t, jobs.StatePaused)

		rts.sqlDB.Exec(t, "RESUME JOB $1", j.ID())
		rts.check(t, jobs.StateRunning)
		rts.checkStateChangeLog(t, jobs.StateRunning, jobs.StatePaused, "")

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StateRunning)
		rts.resumeCh <- nil
		rts.mu.e.ResumeExit++

		rts.mu.e.Success = true
		rts.check(t, jobs.StateSucceeded)
		rts.checkStateChangeLog(t, jobs.StateSucceeded, jobs.StateRunning, "")
	})

	t.Run("pause reverting", func(t *testing.T) {
		rts := registryTestSuite{}
		defer rts.setUp(t)()
		defer rts.tearDown()

		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.idb(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StateRunning)
		rts.checkStateChangeLog(t, jobs.StateRunning, jobs.StateRunning, "")

		// Make Resume fail.
		rts.resumeCh <- errors.New("resume failed")
		rts.mu.e.ResumeExit++
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StateReverting)
		rts.checkStateChangeLog(t, jobs.StateReverting, jobs.StateRunning, "")

		rts.sqlDB.Exec(t, "PAUSE JOB $1", j.ID())
		rts.check(t, jobs.StatePaused)
		rts.checkStateChangeLog(t, jobs.StatePauseRequested, jobs.StateReverting, "")
		rts.checkStateChangeLog(t, jobs.StatePaused, jobs.StatePauseRequested, "")

		rts.sqlDB.Exec(t, "PAUSE JOB $1", j.ID())
		rts.check(t, jobs.StatePaused)

		rts.sqlDB.Exec(t, "RESUME JOB $1", j.ID())
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StateReverting)
		rts.checkStateChangeLog(t, jobs.StateReverting, jobs.StatePaused, "")
		close(rts.failOrCancelCheckCh)

		rts.failOrCancelCh <- nil
		close(rts.failOrCancelCh)
		rts.mu.e.OnFailOrCancelExit = true
		rts.check(t, jobs.StateFailed)
		rts.checkStateChangeLog(t, jobs.StateFailed, jobs.StateReverting, "")
	})

	t.Run("cancel running", func(t *testing.T) {
		rts := registryTestSuite{}
		defer rts.setUp(t)()
		defer rts.tearDown()
		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.idb(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StateRunning)
		rts.checkStateChangeLog(t, jobs.StateRunning, jobs.StateRunning, "")

		rts.sqlDB.Exec(t, "CANCEL JOB $1", j.ID())
		rts.mu.e.OnFailOrCancelStart = true
		rts.check(t, jobs.StateReverting)
		rts.checkStateChangeLog(t, jobs.StateCancelRequested, jobs.StateRunning, "")
		rts.checkStateChangeLog(t, jobs.StateReverting, jobs.StateCancelRequested, "")

		rts.failOrCancelCheckCh <- struct{}{}
		close(rts.failOrCancelCheckCh)
		rts.failOrCancelCh <- nil
		close(rts.failOrCancelCh)
		rts.mu.e.OnFailOrCancelExit = true

		rts.check(t, jobs.StateCanceled)
		rts.checkStateChangeLog(t, jobs.StateCanceled, jobs.StateReverting, "")
	})

	t.Run("cancel reverting", func(t *testing.T) {
		rts := registryTestSuite{}
		defer rts.setUp(t)()
		defer rts.tearDown()
		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.idb(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StateRunning)
		rts.checkStateChangeLog(t, jobs.StateRunning, jobs.StateRunning, "")

		rts.sqlDB.Exec(t, "CANCEL JOB $1", j.ID())
		rts.mu.e.OnFailOrCancelStart = true
		rts.check(t, jobs.StateReverting)

		rts.sqlDB.ExpectErr(t, "state reverting cannot be requested to be canceled", "CANCEL JOB $1", j.ID())
		rts.check(t, jobs.StateReverting)
		rts.checkStateChangeLog(t, jobs.StateCancelRequested, jobs.StateRunning, "")
		rts.checkStateChangeLog(t, jobs.StateReverting, jobs.StateCancelRequested, "")

		close(rts.failOrCancelCheckCh)
		close(rts.failOrCancelCh)
	})

	t.Run("cancel pause running", func(t *testing.T) {
		rts := registryTestSuite{}
		defer rts.setUp(t)()
		defer rts.tearDown()

		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.idb(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StateRunning)
		rts.checkStateChangeLog(t, jobs.StateRunning, jobs.StateRunning, "")

		rts.sqlDB.Exec(t, "PAUSE JOB $1", j.ID())
		rts.check(t, jobs.StatePaused)
		rts.checkStateChangeLog(t, jobs.StatePauseRequested, jobs.StateRunning, "")
		rts.checkStateChangeLog(t, jobs.StatePaused, jobs.StatePauseRequested, "")

		rts.sqlDB.Exec(t, "CANCEL JOB $1", j.ID())
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		close(rts.failOrCancelCheckCh)
		rts.check(t, jobs.StateReverting)
		rts.checkStateChangeLog(t, jobs.StateCancelRequested, jobs.StatePaused, "")
		rts.checkStateChangeLog(t, jobs.StateReverting, jobs.StateCancelRequested, "")

		rts.failOrCancelCh <- nil
		rts.mu.e.OnFailOrCancelExit = true
		close(rts.failOrCancelCh)
		rts.check(t, jobs.StateCanceled)
		rts.checkStateChangeLog(t, jobs.StateCanceled, jobs.StateReverting, "")
	})

	t.Run("cancel pause reverting", func(t *testing.T) {
		rts := registryTestSuite{}
		defer rts.setUp(t)()
		defer rts.tearDown()

		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.idb(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StateRunning)
		rts.checkStateChangeLog(t, jobs.StateRunning, jobs.StateRunning, "")

		// Make Resume fail.
		rts.resumeCh <- errors.New("resume failed")
		rts.mu.e.ResumeExit++
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StateReverting)
		rts.checkStateChangeLog(t, jobs.StateReverting, jobs.StateRunning, "")

		rts.sqlDB.Exec(t, "PAUSE JOB $1", j.ID())
		rts.check(t, jobs.StatePaused)
		rts.checkStateChangeLog(t, jobs.StatePauseRequested, jobs.StateReverting, "")

		rts.sqlDB.ExpectErr(t, "paused and has non-nil FinalResumeError .* resume failed", "CANCEL JOB $1", j.ID())
		rts.check(t, jobs.StatePaused)

		rts.sqlDB.Exec(t, "RESUME JOB $1", j.ID())
		rts.failOrCancelCheckCh <- struct{}{}
		close(rts.failOrCancelCheckCh)
		rts.check(t, jobs.StateReverting)
		rts.checkStateChangeLog(t, jobs.StateReverting, jobs.StatePaused, "")

		rts.failOrCancelCh <- nil
		close(rts.failOrCancelCh)
		rts.mu.e.OnFailOrCancelExit = true
		rts.check(t, jobs.StateFailed)
		rts.checkStateChangeLog(t, jobs.StateFailed, jobs.StateReverting, "")
	})

	// Verify that pause and cancel in a rollback do nothing.
	t.Run("rollback", func(t *testing.T) {
		rts := registryTestSuite{}
		defer rts.setUp(t)()
		defer rts.tearDown()
		job, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.idb(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = job

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StateRunning)
		rts.sqlDB.MaxTxnRetries = 10
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
			rts.check(t, jobs.StateRunning)
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
			rts.check(t, jobs.StateRunning)
		}
		// Now pause it for reals.
		{
			rts.sqlDB.RunWithRetriableTxn(t, func(txn *gosql.Tx) error {
				if _, err := txn.Exec("PAUSE JOB $1", job.ID()); err != nil {
					return err
				}
				// Not committed yet, so state shouldn't have changed.
				// Don't check status in txn.
				rts.check(t, "")
				return nil
			})
			rts.check(t, jobs.StatePaused)
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
			rts.check(t, jobs.StatePaused)
		}
		// Commit a RESUME.
		{
			rts.sqlDB.RunWithRetriableTxn(t, func(txn *gosql.Tx) error {
				if _, err := txn.Exec("RESUME JOB $1", job.ID()); err != nil {
					return err
				}
				// Not committed yet, so state shouldn't have changed.
				// Don't check status in txn.
				rts.check(t, "")
				return nil
			})
		}
		rts.mu.e.ResumeStart = true
		rts.check(t, jobs.StateRunning)
		rts.resumeCheckCh <- struct{}{}
		rts.resumeCh <- nil
		rts.mu.e.ResumeExit++
		rts.mu.e.Success = true
		rts.check(t, jobs.StateSucceeded)
	})

	t.Run("failed running", func(t *testing.T) {
		rts := registryTestSuite{}
		defer rts.setUp(t)()
		defer rts.tearDown()

		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.idb(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.check(t, jobs.StateRunning)
		rts.checkStateChangeLog(t, jobs.StateRunning, jobs.StateRunning, "")

		rts.resumeCheckCh <- struct{}{}
		rts.resumeCh <- errors.New("resume failed")
		rts.mu.e.ResumeExit++
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		close(rts.failOrCancelCheckCh)
		rts.check(t, jobs.StateReverting)
		rts.checkStateChangeLog(t, jobs.StateReverting, jobs.StateRunning, "")

		rts.failOrCancelCh <- nil
		rts.mu.e.OnFailOrCancelExit = true
		close(rts.failOrCancelCh)
		rts.check(t, jobs.StateFailed)
		rts.checkStateChangeLog(t, jobs.StateFailed, jobs.StateReverting, "")
	})

	// Attempt to mark success, but fail, but fail that also.
	t.Run("fail marking success and fail OnFailOrCancel", func(t *testing.T) {
		var triedToMarkSucceeded atomic.Bool
		var injectFailures atomic.Bool
		rts := registryTestSuite{beforeUpdate: func(orig, updated jobs.JobMetadata) error {
			// Fail marking succeeded.
			if updated.State == jobs.StateSucceeded && injectFailures.Load() {
				triedToMarkSucceeded.Store(true)
				return errors.New("injected failure at marking as succeeded")
			}
			return nil
		}}
		defer rts.setUp(t)()
		defer rts.tearDown()

		injectFailures.Store(true)
		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.idb(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StateRunning)
		rts.checkStateChangeLog(t, jobs.StateRunning, jobs.StateRunning, "")
		// Let the resumer complete without error.
		rts.resumeCh <- nil
		rts.mu.e.ResumeExit++
		rts.mu.e.Success = true

		// The job is retried as we failed to mark the job successful.
		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StateRunning)
		// Fail the resumer to transition to reverting state.
		rts.resumeCh <- errors.New("injected error in resume")
		rts.mu.e.ResumeExit++

		// The job is now in state reverting and will never resume again because
		// OnFailOrCancel also fails.
		//
		// First retry.
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		require.True(t, triedToMarkSucceeded.Load())
		rts.check(t, jobs.StateReverting)
		rts.failOrCancelCh <- errors.New("injected failure while blocked in reverting")
		rts.mu.e.OnFailOrCancelExit = true

		// The job will be retried as all reverting jobs are retried.
		//
		// Second retry.
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StateReverting)
		rts.failOrCancelCh <- errors.New("injected failure while blocked in reverting")
		rts.mu.e.OnFailOrCancelExit = true
		rts.checkStateChangeLog(t, jobs.StateReverting, jobs.StateRunning, "injected error in resume")

		// The job will stay in reverting state. Let it fail to exit the test.
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StateReverting)
		close(rts.failOrCancelCh)
		rts.mu.e.OnFailOrCancelExit = true

		rts.check(t, jobs.StateFailed)
		rts.checkStateChangeLog(t, jobs.StateFailed, jobs.StateReverting, "injected error in resume")
	})
	// Succeed the job but inject an error actually marking the jobs successful.
	// This could happen due to a transient network error or something like that.
	// It would not make sense to revert a job in this scenario.
	t.Run("fail marking success", func(t *testing.T) {
		rts := registryTestSuite{}
		defer rts.setUp(t)()
		defer rts.tearDown()

		// Inject an error in the update to move the job to "succeeded" one time.
		var failed atomic.Value
		failed.Store(false)
		rts.beforeUpdate = func(orig, updated jobs.JobMetadata) error {
			if updated.State == jobs.StateSucceeded && !failed.Load().(bool) {
				failed.Store(true)
				return errors.New("boom")
			}
			return nil
		}

		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.idb(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		// Make sure the job hits the error when it attempts to succeed.
		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StateRunning)
		rts.checkStateChangeLog(t, jobs.StateRunning, jobs.StateRunning, "")
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
		rts.check(t, jobs.StateSucceeded)
		rts.checkStateChangeLog(t, jobs.StateSucceeded, jobs.StateRunning, "")
	})

	// Fail the job, but also fail to mark it failed.
	t.Run("fail marking failed", func(t *testing.T) {
		var triedToMarkFailed atomic.Value
		triedToMarkFailed.Store(false)
		rts := registryTestSuite{beforeUpdate: func(orig, updated jobs.JobMetadata) error {
			if triedToMarkFailed.Load().(bool) == true {
				return nil
			}
			if updated.State == jobs.StateFailed {
				triedToMarkFailed.Store(true)
				return errors.New("injected error while marking as failed")
			}
			return nil
		}}
		defer rts.setUp(t)()
		defer rts.tearDown()

		// Make marking success fail.
		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.idb(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StateRunning)
		rts.checkStateChangeLog(t, jobs.StateRunning, jobs.StateRunning, "")

		rts.resumeCh <- errors.New("resume failed")
		rts.mu.e.ResumeExit++

		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StateReverting)
		rts.checkStateChangeLog(t, jobs.StateReverting, jobs.StateRunning, "resume failed")
		// The job is now in state reverting and will never resume again.
		// Let revert complete without error so that the job is attempted to mark as failed.
		rts.failOrCancelCh <- nil
		rts.mu.e.OnFailOrCancelExit = true

		// We failed to mark the jobs as failed, resulting in the job to be retried.
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StateReverting)
		require.True(t, triedToMarkFailed.Load().(bool))
		// Let the job complete to exit the test.
		close(rts.failOrCancelCh)
		rts.mu.e.OnFailOrCancelExit = true
		rts.check(t, jobs.StateFailed)
		rts.checkStateChangeLog(t, jobs.StateFailed, jobs.StateReverting, "resume failed")
	})
	t.Run("dump traces on pause-unpause-success", func(t *testing.T) {
		ctx := context.Background()
		completeCh := make(chan struct{})
		var expectedJobID atomic.Int64
		rts := registryTestSuite{traceRealSpan: true, afterJobStateMachine: func(id jobspb.JobID) {
			if int64(id) != expectedJobID.Load() {
				return
			}
			completeCh <- struct{}{}
		}}
		defer rts.setUp(t)()
		defer rts.tearDown()
		pauseUnpauseJob := func(expectedNumFiles int) {
			j, err := jobs.TestingCreateAndStartJob(ctx, rts.registry, rts.idb(), rts.mockJob)
			require.NoError(t, err)
			rts.job = j
			expectedJobID.Store(int64(j.ID()))

			rts.mu.e.ResumeStart = true
			rts.resumeCheckCh <- struct{}{}
			rts.check(t, jobs.StateRunning)

			rts.sqlDB.Exec(t, "PAUSE JOB $1", j.ID())
			rts.check(t, jobs.StatePaused)

			<-completeCh
			checkTraceFiles(ctx, t, expectedNumFiles, j.ID(), rts.s)

			rts.sqlDB.Exec(t, "RESUME JOB $1", j.ID())

			rts.mu.e.ResumeStart = true
			rts.resumeCheckCh <- struct{}{}
			rts.check(t, jobs.StateRunning)
			rts.resumeCh <- nil
			rts.mu.e.ResumeExit++

			rts.mu.e.Success = true
			rts.check(t, jobs.StateSucceeded)

			<-completeCh
			checkTraceFiles(ctx, t, expectedNumFiles+2, j.ID(), rts.s)
		}
		pauseUnpauseJob(2)
	})

	t.Run("dump traces on fail", func(t *testing.T) {
		ctx := context.Background()
		completeCh := make(chan struct{})
		var expectedJobID atomic.Int64
		rts := registryTestSuite{traceRealSpan: true, afterJobStateMachine: func(id jobspb.JobID) {
			if int64(id) != expectedJobID.Load() {
				return
			}
			completeCh <- struct{}{}
		}}
		defer rts.setUp(t)()
		defer rts.tearDown()

		runJobAndFail := func(expectedNumFiles int) {
			j, err := jobs.TestingCreateAndStartJob(ctx, rts.registry, rts.idb(), rts.mockJob)
			require.NoError(t, err)
			rts.job = j
			expectedJobID.Store(int64(j.ID()))

			rts.mu.e.ResumeStart = true
			rts.resumeCheckCh <- struct{}{}
			rts.check(t, jobs.StateRunning)

			rts.resumeCh <- errors.New("boom")
			rts.mu.e.ResumeExit++
			rts.mu.e.OnFailOrCancelStart = true
			rts.failOrCancelCheckCh <- struct{}{}
			rts.check(t, jobs.StateReverting)

			rts.failOrCancelCh <- nil
			rts.mu.e.OnFailOrCancelExit = true
			rts.check(t, jobs.StateFailed)

			<-completeCh
			checkTraceFiles(ctx, t, expectedNumFiles, j.ID(), rts.s)
		}

		runJobAndFail(2)
	})

	t.Run("dump traces on cancel", func(t *testing.T) {
		completeCh := make(chan struct{})
		var expectedJobID atomic.Int64
		rts := registryTestSuite{traceRealSpan: true, afterJobStateMachine: func(id jobspb.JobID) {
			if int64(id) != expectedJobID.Load() {
				return
			}
			completeCh <- struct{}{}
		}}
		defer rts.setUp(t)()
		defer rts.tearDown()

		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.idb(), rts.mockJob)
		require.NoError(t, err)
		rts.job = j
		expectedJobID.Store(int64(j.ID()))
		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StateRunning)

		rts.sqlDB.Exec(t, "CANCEL JOB $1", j.ID())

		<-completeCh
		checkTraceFiles(rts.ctx, t, 2, j.ID(), rts.s)

		rts.mu.e.OnFailOrCancelStart = true
		rts.check(t, jobs.StateReverting)

		rts.failOrCancelCheckCh <- struct{}{}
		close(rts.failOrCancelCheckCh)
		rts.failOrCancelCh <- nil
		close(rts.failOrCancelCh)
		rts.mu.e.OnFailOrCancelExit = true

		rts.check(t, jobs.StateCanceled)

		<-completeCh
	})
	t.Run("job with created by fields", func(t *testing.T) {
		createdByType := "internal_test"
		rts := registryTestSuite{}
		defer rts.setUp(t)()
		defer rts.tearDown()

		resumerJob := make(chan *jobs.Job, 1)
		cleanup := jobs.TestingRegisterConstructor(
			jobspb.TypeBackup, func(j *jobs.Job, _ *cluster.Settings) jobs.Resumer {
				return jobstest.FakeResumer{
					OnResume: func(ctx context.Context) error {
						resumerJob <- j
						return nil
					},
				}
			}, jobs.UsesTenantCostControl)
		defer cleanup()
		jobID := rts.registry.MakeJobID()
		record := jobs.Record{
			Details:   jobspb.BackupDetails{},
			Progress:  jobspb.BackupProgress{},
			CreatedBy: &jobs.CreatedByInfo{Name: createdByType, ID: 123},
			Username:  username.TestUserName(),
		}
		var job *jobs.Job
		require.NoError(t, rts.s.InternalDB().(isql.DB).Txn(rts.ctx, func(ctx context.Context, txn isql.Txn) error {
			txn.SessionData().Location = time.FixedZone("UTC+5", 5*60*60)
			j, err := rts.registry.CreateAdoptableJobWithTxn(rts.ctx, record, jobID, txn)
			job = j
			return err
		}))

		rts.sqlDB.CheckQueryResults(t, "SELECT created <= now() FROM system.jobs WHERE id = "+jobID.String(), [][]string{{"true"}})

		loadedJob, err := rts.registry.LoadJob(rts.ctx, jobID)
		require.NoError(t, err)
		require.NotNil(t, loadedJob.CreatedBy())
		require.Equal(t, job.CreatedBy(), loadedJob.CreatedBy())
		rts.registry.TestingNudgeAdoptionQueue()
		resumedJob := <-resumerJob
		require.NotNil(t, resumedJob.CreatedBy())
		require.Equal(t, job.CreatedBy(), resumedJob.CreatedBy())

	})
}

func checkTraceFiles(
	ctx context.Context,
	t *testing.T,
	expectedNumFiles int,
	jobID jobspb.JobID,
	s serverutils.TestServerInterface,
) {
	t.Helper()

	recordings := make([][]byte, 0)
	execCfg := s.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)
	edFiles, err := jobs.ListExecutionDetailFiles(ctx, execCfg.InternalDB, jobID)
	require.NoError(t, err)
	require.Len(t, edFiles, expectedNumFiles)

	require.NoError(t, execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		for _, f := range edFiles {
			data, err := jobs.ReadExecutionDetailFile(ctx, f, txn, jobID)
			if err != nil {
				return err
			}
			// Trace files are dumped in `binpb` and `binpb.txt` format. The former
			// should be unmarshal-able.
			if strings.HasSuffix(f, "binpb") {
				td := jobspb.TraceData{}
				require.NoError(t, protoutil.Unmarshal(data, &td))
				require.NotEmpty(t, td.CollectedSpans)
			}
			recordings = append(recordings, data)
		}
		return nil
	}))
	if len(recordings) != expectedNumFiles {
		t.Fatalf("expected %d entries but found %d", expectedNumFiles, len(recordings))
	}
}

// TestJobLifecycle tests the invariants about the job lifecycle
// querires. It does not depend on the registries job management tasks
// and assumes that it maintains the lease on the job through all
// state transitions.
func TestJobLifecycle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.ResetConstructors()()

	ctx := context.Background()

	var params base.TestServerArgs
	params.Knobs.JobsTestingKnobs = &jobs.TestingKnobs{DisableRegistryLifecycleManagent: true}
	srv, sqlDB, _ := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	registry := s.JobRegistry().(*jobs.Registry)

	createJob := func(record jobs.Record) (*jobs.Job, expectation) {
		beforeTime := timeutil.Now()
		job, err := registry.CreateJobWithTxn(ctx, record, registry.MakeJobID(), nil /* txn */)
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
		Username: username.TestUserName(),
	}

	createDefaultJob := func() (*jobs.Job, expectation) {
		return createJob(defaultRecord)
	}

	t.Run("valid job lifecycles succeed", func(t *testing.T) {
		// Woody is a successful job.
		woodyPride, _ := username.MakeSQLUsernameFromUserInput("Woody Pride", username.PurposeValidation)
		woodyJob, woodyExp := createJob(jobs.Record{
			Description:   "There's a snake in my boot!",
			Username:      woodyPride,
			DescriptorIDs: []descpb.ID{1, 2, 3},
			Details:       jobspb.RestoreDetails{},
			Progress:      jobspb.RestoreProgress{},
		})

		if err := woodyExp.verify(woodyJob.ID(), jobs.StateRunning); err != nil {
			t.Fatal(err)
		}

		if err := woodyJob.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := woodyExp.verify(woodyJob.ID(), jobs.StateRunning); err != nil {
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
			if err := woodyJob.NoTxn().FractionProgressed(ctx, jobs.FractionUpdater(f.actual)); err != nil {
				t.Fatal(err)
			}
			woodyExp.FractionCompleted = f.expected
			if err := woodyExp.verify(woodyJob.ID(), jobs.StateRunning); err != nil {
				t.Fatal(err)
			}
		}

		// Test Progressed callbacks.
		if err := woodyJob.NoTxn().FractionProgressed(ctx, func(_ context.Context, details jobspb.ProgressDetails) float32 {
			details.(*jobspb.Progress_Restore).Restore.HighWater = roachpb.Key("mariana")
			return 1.0
		}); err != nil {
			t.Fatal(err)
		}
		woodyExp.Record.Progress = jobspb.RestoreProgress{HighWater: roachpb.Key("mariana")}
		if err := woodyExp.verify(woodyJob.ID(), jobs.StateRunning); err != nil {
			t.Fatal(err)
		}

		if err := woodyJob.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := woodyExp.verify(woodyJob.ID(), jobs.StateSucceeded); err != nil {
			t.Fatal(err)
		}

		// Buzz fails after it starts running.
		buzzL, _ := username.MakeSQLUsernameFromUserInput("Buzz Lightyear", username.PurposeValidation)
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
		if err := buzzExp.verify(buzzJob.ID(), jobs.StateRunning); err != nil {
			t.Fatal(err)
		}

		if err := buzzJob.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := buzzExp.verify(buzzJob.ID(), jobs.StateRunning); err != nil {
			t.Fatal(err)
		}

		if err := buzzJob.NoTxn().FractionProgressed(ctx, jobs.FractionUpdater(.42)); err != nil {
			t.Fatal(err)
		}
		buzzExp.FractionCompleted = .42
		if err := buzzExp.verify(buzzJob.ID(), jobs.StateRunning); err != nil {
			t.Fatal(err)
		}

		if err := buzzJob.Failed(ctx, errors.New("Buzz Lightyear can't fly")); err != nil {
			t.Fatal(err)
		}
		if err := buzzExp.verify(buzzJob.ID(), jobs.StateFailed); err != nil {
			t.Fatal(err)
		}

		// Ensure that logging Buzz didn't corrupt Woody.
		if err := woodyExp.verify(woodyJob.ID(), jobs.StateSucceeded); err != nil {
			t.Fatal(err)
		}

		// Sid fails before it starts running.
		sidP, _ := username.MakeSQLUsernameFromUserInput("Sid Phillips", username.PurposeValidation)
		sidJob, sidExp := createJob(jobs.Record{
			Description:   "The toys! The toys are alive!",
			Username:      sidP,
			DescriptorIDs: []descpb.ID{6, 6, 6},
			Details:       jobspb.RestoreDetails{},
			Progress:      jobspb.RestoreProgress{},
		})

		if err := sidExp.verify(sidJob.ID(), jobs.StateRunning); err != nil {
			t.Fatal(err)
		}

		if err := sidJob.Failed(ctx, errors.New("Sid is a total failure")); err != nil {
			t.Fatal(err)
		}
		sidExp.Error = "Sid is a total failure"
		if err := sidExp.verify(sidJob.ID(), jobs.StateFailed); err != nil {
			t.Fatal(err)
		}

		// Ensure that logging Sid didn't corrupt Woody or Buzz.
		if err := woodyExp.verify(woodyJob.ID(), jobs.StateSucceeded); err != nil {
			t.Fatal(err)
		}
		if err := buzzExp.verify(buzzJob.ID(), jobs.StateFailed); err != nil {
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
			if err := exp.verify(job.ID(), jobs.StateSucceeded); err != nil {
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
			if err := exp.verify(job.ID(), jobs.StateFailed); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("internal errors are not swallowed if marking job as successful", func(t *testing.T) {
			job, _ := createDefaultJob()
			if _, err := sqlDB.Exec(
				`UPDATE system.job_info SET value = 'garbage' WHERE job_id = $1 AND info_key = $2`, job.ID(),
				jobs.GetLegacyPayloadKey(),
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
				`UPDATE system.job_info SET value = 'garbage' WHERE job_id = $1 AND info_key = $2`, job.ID(),
				jobs.GetLegacyPayloadKey(),
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
			if err := exp.verify(job.ID(), jobs.StateFailed); err != nil {
				t.Fatal(err)
			}
		})

	})

	t.Run("cancelable jobs can be paused until finished", func(t *testing.T) {
		job, exp := createDefaultJob()

		if err := registry.PauseRequested(ctx, nil, job.ID(), ""); err != nil {
			t.Fatal(err)
		}
		if err := job.Paused(ctx); err != nil {
			t.Fatal(err)
		}
		if err := exp.verify(job.ID(), jobs.StatePaused); err != nil {
			t.Fatal(err)
		}
		if err := registry.Unpause(ctx, nil, job.ID()); err != nil {
			t.Fatal(err)
		}
		// Resume the job again to ensure that the resumption is idempotent.
		if err := registry.Unpause(ctx, nil, job.ID()); err != nil {
			t.Fatal(err)
		}
		if err := exp.verify(job.ID(), jobs.StateRunning); err != nil {
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
			job, exp := createDefaultJob()
			if err := job.NoTxn().CancelRequested(ctx); err != nil {
				t.Fatal(err)
			}
			if err := exp.verify(job.ID(), jobs.StateCancelRequested); err != nil {
				t.Fatal(err)
			}
		}

		{
			job, exp := createDefaultJob()
			if err := job.Started(ctx); err != nil {
				t.Fatal(err)
			}
			if err := registry.CancelRequested(ctx, nil, job.ID()); err != nil {
				t.Fatal(err)
			}
			if err := exp.verify(job.ID(), jobs.StateCancelRequested); err != nil {
				t.Fatal(err)
			}
		}

		{
			job, exp := createDefaultJob()
			if err := registry.PauseRequested(ctx, nil, job.ID(), ""); err != nil {
				t.Fatal(err)
			}
			if err := job.Paused(ctx); err != nil {
				t.Fatal(err)
			}
			if err := registry.CancelRequested(ctx, nil, job.ID()); err != nil {
				t.Fatal(err)
			}
			if err := exp.verify(job.ID(), jobs.StateCancelRequested); err != nil {
				t.Fatal(err)
			}
		}

		{
			job, _ := createDefaultJob()
			if err := job.Succeeded(ctx); err != nil {
				t.Fatal(err)
			}
			expectedErr := "job with state succeeded cannot be requested to be canceled"
			if err := registry.CancelRequested(ctx, nil, job.ID()); !testutils.IsError(err, expectedErr) {
				t.Fatalf("expected '%s', but got '%s'", expectedErr, err)
			}
		}
	})

	t.Run("Unpaused jobs cannot be resumed", func(t *testing.T) {
		{
			job, _ := createDefaultJob()
			if err := registry.CancelRequested(ctx, nil, job.ID()); err != nil {
				t.Fatal(err)
			}
			if err := registry.Unpause(ctx, nil, job.ID()); !testutils.IsError(err, "cannot be resumed") {
				t.Errorf("got unexpected state '%v'", err)
			}
		}

		{
			job, _ := createDefaultJob()
			if err := job.Succeeded(ctx); err != nil {
				t.Fatal(err)
			}
			expectedErr := fmt.Sprintf("job with state %s cannot be resumed", jobs.StateSucceeded)
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
			Details:  42,
			Username: username.TestUserName(),
		}, registry.MakeJobID(), nil /* txn */)
	})

	t.Run("update before create fails", func(t *testing.T) {
		// Attempt to create the job but abort the transaction.
		var job *jobs.Job
		require.Regexp(t, "boom", s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			job, _ = registry.CreateAdoptableJobWithTxn(ctx, jobs.Record{
				Details:  jobspb.RestoreDetails{},
				Progress: jobspb.RestoreProgress{},
				Username: username.TestUserName(),
			}, registry.MakeJobID(), txn)
			return errors.New("boom")
		}))
		if err := job.Started(ctx); !testutils.IsError(err, "does not exist") {
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

	t.Run("out of bounds progress fails", func(t *testing.T) {
		job, _ := createDefaultJob()
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.NoTxn().FractionProgressed(ctx, jobs.FractionUpdater(-0.1)); !testutils.IsError(err, "outside allowable range") {
			t.Fatalf("expected 'outside allowable range' error, but got %v", err)
		}
		if err := job.NoTxn().FractionProgressed(ctx, jobs.FractionUpdater(1.1)); !testutils.IsError(err, "outside allowable range") {
			t.Fatalf("expected 'outside allowable range' error, but got %v", err)
		}
	})

	t.Run("error propagates", func(t *testing.T) {
		job, _ := createDefaultJob()
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.NoTxn().Update(ctx, func(_ isql.Txn, _ jobs.JobMetadata, ju *jobs.JobUpdater) error {
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
		if err := job.NoTxn().FractionProgressed(ctx, jobs.FractionUpdater(0.5)); !testutils.IsError(
			err, `cannot update progress on succeeded job \(id \d+\)`,
		) {
			t.Fatalf("expected 'cannot update progress' error, but got %v", err)
		}
	})

	t.Run("progress on paused job fails", func(t *testing.T) {
		job, _ := createDefaultJob()
		if err := registry.PauseRequested(ctx, nil, job.ID(), ""); err != nil {
			t.Fatal(err)
		}
		if err := job.NoTxn().FractionProgressed(ctx, jobs.FractionUpdater(0.5)); !testutils.IsError(
			err, `cannot update progress on pause-requested job`,
		) {
			t.Fatalf("expected progress error, but got %v", err)
		}
	})

	t.Run("progress on canceled job fails", func(t *testing.T) {
		job, _ := createDefaultJob()
		if err := registry.CancelRequested(ctx, nil, job.ID()); err != nil {
			t.Fatal(err)
		}
		if err := job.NoTxn().FractionProgressed(ctx, jobs.FractionUpdater(0.5)); !testutils.IsError(
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
		if err := job.NoTxn().FractionProgressed(ctx, jobs.FractionUpdater(0.2)); err != nil {
			t.Fatal(err)
		}
		if err := job.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		exp.FractionCompleted = 1.0
		if err := exp.verify(job.ID(), jobs.StateSucceeded); err != nil {
			t.Fatal(err)
		}
	})

	updateClaimStmt := `UPDATE system.jobs SET claim_session_id = $1 WHERE id = $2`
	updateStateStmt := `UPDATE system.jobs SET status = $1 WHERE id = $2`

	t.Run("set details works", func(t *testing.T) {
		job, exp := createDefaultJob()
		require.NoError(t, exp.verify(job.ID(), jobs.StateRunning))
		newDetails := jobspb.ImportDetails{URIs: []string{"new"}}
		exp.Record.Details = newDetails
		require.NoError(t, job.NoTxn().SetDetails(ctx, newDetails))
		require.NoError(t, exp.verify(job.ID(), jobs.StateRunning))
		require.NoError(t, job.NoTxn().SetDetails(ctx, newDetails))

		// Now change job's session id and check that updates are rejected.
		_, err := exp.DB.Exec(updateClaimStmt, "!@#!@$!$@#", job.ID())
		require.NoError(t, err)
		require.Error(t, job.NoTxn().SetDetails(ctx, newDetails))
		require.NoError(t, exp.verify(job.ID(), jobs.StateRunning))
	})

	t.Run("set details fails", func(t *testing.T) {
		job, exp := createDefaultJob()
		require.NoError(t, exp.verify(job.ID(), jobs.StateRunning))
		_, err := exp.DB.Exec(updateStateStmt, jobs.StateCancelRequested, job.ID())
		require.NoError(t, err)
		require.Error(t, job.NoTxn().SetDetails(ctx, jobspb.ImportDetails{URIs: []string{"new"}}))
		require.NoError(t, exp.verify(job.ID(), jobs.StateCancelRequested))
	})

	t.Run("set progress works", func(t *testing.T) {
		job, exp := createDefaultJob()
		require.NoError(t, exp.verify(job.ID(), jobs.StateRunning))
		newProgress := jobspb.ImportProgress{ResumePos: []int64{42}}
		exp.Record.Progress = newProgress
		require.NoError(t, job.NoTxn().SetProgress(ctx, newProgress))
		require.NoError(t, exp.verify(job.ID(), jobs.StateRunning))

		// Now change job's session id and check that updates are rejected.
		_, err := exp.DB.Exec(updateClaimStmt, "!@#!@$!$@#", job.ID())
		require.NoError(t, err)
		require.Error(t, job.NoTxn().SetDetails(ctx, newProgress))
		require.NoError(t, exp.verify(job.ID(), jobs.StateRunning))
	})

	t.Run("set progress fails", func(t *testing.T) {
		job, exp := createDefaultJob()
		require.NoError(t, exp.verify(job.ID(), jobs.StateRunning))
		_, err := exp.DB.Exec(updateStateStmt, jobs.StatePauseRequested, job.ID())
		require.NoError(t, err)
		require.Error(t, job.NoTxn().SetProgress(ctx, jobspb.ImportProgress{ResumePos: []int64{42}}))
		require.NoError(t, exp.verify(job.ID(), jobs.StatePauseRequested))
	})
}

// TestShowJobs manually inserts a row into system.jobs and checks that the
// encoded protobuf payload is properly decoded and visible in
// crdb_internal.jobs.
func TestShowJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var params base.TestServerArgs
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
		state             string
		description       string
		username          username.SQLUsername
		err               string
		created           time.Time
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
			id:                42,
			typ:               "SCHEMA CHANGE",
			state:             "superfailed",
			description:       "failjob",
			username:          username.MakeSQLUsernameFromPreNormalizedString("failure"),
			err:               "boom",
			created:           timeutil.Unix(1, 0),
			finished:          timeutil.Unix(3, 0),
			modified:          timeutil.Unix(4, 0),
			fractionCompleted: 0.42,
			coordinatorID:     instanceID,
			details:           jobspb.SchemaChangeDetails{},
		},
		{
			id:          43,
			typ:         "CHANGEFEED",
			state:       "running",
			description: "persistent feed",
			username:    username.MakeSQLUsernameFromPreNormalizedString("persistent"),
			err:         "",
			created:     timeutil.Unix(1, 0),
			finished:    timeutil.Unix(3, 0),
			modified:    timeutil.Unix(4, 0),
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
				`INSERT INTO system.jobs (id, status, job_type, owner, description, created, finished, error_msg, claim_session_id, claim_instance_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
				in.id, in.state, in.typ, in.username.Normalized(), in.description, in.created, in.finished, in.err, session.ID().UnsafeBytes(), instanceID,
			)
			sqlDB.Exec(t, `INSERT INTO system.job_progress (job_id, written, resolved, fraction) VALUES ($1, $2, $3, $4)`, in.id, in.modified, in.highWater.AsOfSystemTime(), in.fractionCompleted)

			// TODO(dt): delete these once sql.jobs.legacy_vtable.enabled is gone.
			sqlDB.Exec(t, `INSERT INTO system.job_info (job_id, info_key, value) VALUES ($1, $2, $3)`, in.id, jobs.GetLegacyPayloadKey(), inPayload)
			sqlDB.Exec(t, `INSERT INTO system.job_info (job_id, info_key, value) VALUES ($1, $2, $3)`, in.id, jobs.GetLegacyProgressKey(), inProgress)

			var out row
			var maybeFractionCompleted *float32
			var decimalHighWater *apd.Decimal
			var resultUsername string
			sqlDB.QueryRow(t, `
      SELECT job_id, job_type, status, created, description, finished, modified,
             fraction_completed, high_water_timestamp, user_name, ifnull(error, ''), coordinator_id
        FROM crdb_internal.jobs WHERE job_id = $1`, in.id).Scan(
				&out.id, &out.typ, &out.state, &out.created, &out.description,
				&out.finished, &out.modified, &maybeFractionCompleted, &decimalHighWater, &resultUsername,
				&out.err, &out.coordinatorID,
			)
			out.username = username.MakeSQLUsernameFromPreNormalizedString(resultUsername)

			if decimalHighWater != nil {
				var err error
				out.highWater, err = hlc.DecimalToHLC(decimalHighWater)
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
			// All the timestamps won't compare with simple == due locality, so we
			// check that they are the same instant with time.Equal ourselves.
			if out.created.Equal(in.created) {
				out.created = in.created
			}
			if out.finished.Equal(in.finished) {
				out.finished = in.finished
			}
			if out.modified.Equal(in.modified) {
				out.modified = in.modified
			}

			// Locations don't compare well so nuke them.
			for _, ts := range []*time.Time{&in.created, &in.finished, &in.modified} {
				*ts = ts.UTC()
			}
			for _, ts := range []*time.Time{&out.created, &out.finished, &out.modified} {
				*ts = ts.UTC()
			}

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

	var params base.TestServerArgs
	params.Knobs.KeyVisualizer = &keyvisualizer.TestingKnobs{
		SkipJobBootstrap: true,
	}
	params.Knobs.JobsTestingKnobs = &jobs.TestingKnobs{
		DisableAdoptions: true,
	}
	params.Knobs.UpgradeManager = &upgradebase.TestingKnobs{DontUseJobs: true}
	s, rawSQLDB, _ := serverutils.StartServer(t, params)
	ctx := context.Background()
	r := s.ApplicationLayer().JobRegistry().(*jobs.Registry)
	sqlDB := sqlutils.MakeSQLRunner(rawSQLDB)
	defer s.Stopper().Stop(context.Background())

	// row represents a row returned from crdb_internal.jobs, but
	// *not* a row in system.jobs.
	type row struct {
		id      jobspb.JobID
		typ     string
		state   string
		details jobspb.Details
	}

	rows := []row{
		{
			id:      1,
			typ:     "CREATE STATS",
			state:   "running",
			details: jobspb.CreateStatsDetails{Name: "my_stats"},
		},
		{
			id:      2,
			typ:     "AUTO CREATE STATS",
			state:   "running",
			details: jobspb.CreateStatsDetails{Name: "__auto__"},
		},
	}

	for _, in := range rows {
		// system.jobs is part proper SQL columns, part protobuf, so we can't use the
		// row struct directly.
		rawPayload := &jobspb.Payload{
			UsernameProto: username.RootUserName().EncodeProto(),
			Details:       jobspb.WrapPayloadDetails(in.details),
		}
		// progress is set to a placeholder value.
		progress := &jobspb.Progress{
			Details: jobspb.WrapProgressDetails(jobspb.ImportProgress{}),
		}

		rec := jobs.Record{
			JobID:    in.id,
			Username: username.TestUserName(),
			Details:  rawPayload.UnwrapDetails(),
			Progress: progress.UnwrapDetails(),
		}
		_, err := r.CreateJobWithTxn(ctx, rec, in.id, nil /* txn */)
		require.NoError(t, err)
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

	sqlDB.QueryRow(t, `SELECT job_id, job_type FROM [SHOW JOBS] ORDER BY job_id LIMIT 1`).Scan(&out.id, &out.typ)
	if out.id != 1 || out.typ != "CREATE STATS" {
		t.Fatalf("Expected id:%d and type:%s but found id:%d and type:%s",
			1, "CREATE STATS", out.id, out.typ)
	}

	sqlDB.QueryRow(t, `SELECT job_id, job_type FROM [SHOW AUTOMATIC JOBS] ORDER BY job_id LIMIT 1`).Scan(&out.id, &out.typ)
	if out.id != 2 || out.typ != "AUTO CREATE STATS" {
		t.Fatalf("Expected id:%d and type:%s but found id:%d and type:%s",
			2, "AUTO CREATE STATS", out.id, out.typ)
	}
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
		Username: username.TestUserName(),
		Details:  jobspb.ImportDetails{},
		Progress: jobspb.ImportProgress{},
	}
	_, err := db.Exec("CREATE USER testuser")
	require.NoError(t, err)
	done := make(chan struct{})
	defer close(done)
	cleanup := jobs.TestingRegisterConstructor(
		jobspb.TypeImport, func(_ *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return jobstest.FakeResumer{
				OnResume: func(ctx context.Context) error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-done:
						return nil
					}
				},
			}
		}, jobs.UsesTenantCostControl)
	defer cleanup()

	type row struct {
		id    jobspb.JobID
		state string
	}
	var out row
	insqlDB := s.InternalDB().(isql.DB)
	t.Run("show job", func(t *testing.T) {
		// Start a job and cancel it so it is in state finished and then query it with
		// SHOW JOB WHEN COMPLETE.
		job, err := jobs.TestingCreateAndStartJob(ctx, registry, insqlDB, mockJob)
		if err != nil {
			t.Fatal(err)
		}
		group := ctxgroup.WithContext(ctx)
		group.GoCtx(func(ctx context.Context) error {
			if err := db.QueryRowContext(
				ctx,
				`SELECT job_id, status
				 FROM [SHOW JOB WHEN COMPLETE $1]`,
				job.ID()).Scan(&out.id, &out.state); err != nil {
				return err
			}
			if out.state != "canceled" {
				return errors.Errorf(
					"Expected state 'canceled' but got '%s'", out.state)
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
			job, err := jobs.TestingCreateAndStartJob(ctx, registry, insqlDB, mockJob)
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
				if err := rows.Scan(&out.id, &out.state); err != nil {
					return err
				}
				cnt += 1
				switch out.id {
				case jobsToStart[0].ID():
				case jobsToStart[1].ID():
					// SHOW JOBS WHEN COMPLETE finishes only after all jobs are
					// canceled.
					if out.state != "canceled" {
						return errors.Errorf(
							"Expected state 'canceled' but got '%s'",
							out.state)
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
	var jobID jobspb.JobID

	defer sql.ClearPlanHooks()
	// Piggy back on BACKUP to be able to create a succeeding test job.
	sql.ClearPlanHooks()
	sql.AddPlanHook(
		"test",
		func(_ context.Context, stmt tree.Statement, execCtx sql.PlanHookState,
		) (sql.PlanHookRowFn, colinfo.ResultColumns, bool, error) {
			st, ok := stmt.(*tree.Backup)
			if !ok {
				return nil, nil, false, nil
			}
			fn := func(ctx context.Context, _ chan<- tree.Datums) error {
				var err error
				jobID = execCtx.ExtendedEvalContext().QueueJob(&jobs.Record{
					Description: st.String(),
					Details:     jobspb.BackupDetails{},
					Progress:    jobspb.BackupProgress{},
					Username:    username.TestUserName(),
				})
				return err
			}
			return fn, nil, false, nil
		},
		func(_ context.Context, stmt tree.Statement, execCtx sql.PlanHookState,
		) (matched bool, _ colinfo.ResultColumns, _ error) {
			_, matched = stmt.(*tree.Backup)
			return matched, nil, nil
		},
	)
	cleanup := jobs.TestingRegisterConstructor(jobspb.TypeBackup, func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return jobstest.FakeResumer{
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
	}, jobs.UsesTenantCostControl)
	defer cleanup()

	// Piggy back on RESTORE to be able to create a failing test job.
	sql.AddPlanHook(
		"test",
		func(_ context.Context, stmt tree.Statement, execCtx sql.PlanHookState,
		) (sql.PlanHookRowFn, colinfo.ResultColumns, bool, error) {
			_, ok := stmt.(*tree.Restore)
			if !ok {
				return nil, nil, false, nil
			}
			fn := func(ctx context.Context, _ chan<- tree.Datums) error {
				var err error
				jobID = execCtx.ExtendedEvalContext().QueueJob(&jobs.Record{
					Description: "RESTORE",
					Details:     jobspb.RestoreDetails{},
					Progress:    jobspb.RestoreProgress{},
					Username:    username.TestUserName(),
				})
				return err
			}
			return fn, nil, false, nil
		},
		func(_ context.Context, stmt tree.Statement, execCtx sql.PlanHookState,
		) (matched bool, _ colinfo.ResultColumns, _ error) {
			_, matched = stmt.(*tree.Restore)
			return matched, nil, nil
		},
	)
	defer jobs.TestingRegisterConstructor(jobspb.TypeRestore, func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return jobstest.FakeResumer{
			OnResume: func(_ context.Context) error {
				return errors.New("RESTORE failed")
			},
		}
	}, jobs.UsesTenantCostControl)()

	t.Run("rollback txn", func(t *testing.T) {
		start := timeutil.Now()

		txn, err := sqlDB.Begin()
		require.NoError(t, err)
		_, err = txn.Exec("BACKUP tobeaborted INTO doesnotmattter")
		require.NoError(t, err)

		// If we rollback then the job should not run
		require.NoError(t, txn.Rollback())
		registry := s.JobRegistry().(*jobs.Registry)
		_, err = registry.LoadJob(ctx, jobID)
		require.Error(t, err, "the job should not exist after the txn is rolled back")
		require.True(t, jobs.HasJobNotFoundError(err))

		sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
		// Just in case the job was scheduled let's wait for it to finish
		// to avoid a race.
		sqlRunner.Exec(t, "SHOW JOB WHEN COMPLETE $1", jobID)
		require.Equal(t, int32(0), atomic.LoadInt32(&hasRun),
			"job has run in transaction before txn commit")
		require.True(t, timeutil.Since(start) < jobs.DefaultAdoptInterval, "job should have been adopted immediately")
	})

	t.Run("normal success", func(t *testing.T) {
		start := timeutil.Now()

		// Now let's actually commit the transaction and check that the job ran.
		txn, err := sqlDB.Begin()
		require.NoError(t, err)
		_, err = txn.Exec("BACKUP tocommit INTO foo")
		require.NoError(t, err)
		// Committing will block and wait for all jobs to run.
		require.NoError(t, txn.Commit())
		registry := s.JobRegistry().(*jobs.Registry)
		j, err := registry.LoadJob(ctx, jobID)
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
		_, err = txn.Exec("BACKUP doesnotmatter INTO doesnotmattter")
		require.NoError(t, err)
		// We hooked up a failing test job to RESTORE.
		_, err = txn.Exec("RESTORE TABLE tbl FROM LATEST IN somewhere")
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
		clusterversion.Latest.Version(),
		clusterversion.MinSupported.Version(),
		false, /* initializeVersion */
	)
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: st,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				ClusterVersionOverride:         clusterversion.MinSupported.Version(),
				DisableAutomaticVersionUpgrade: make(chan struct{}),
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	jr := s.JobRegistry().(*jobs.Registry)
	_, err := sqlDB.Exec("SELECT now()")
	require.NoError(t, err)

	cleanup := jobs.TestingRegisterConstructor(jobspb.TypeImport, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return jobstest.FakeResumer{}
	}, jobs.UsesTenantCostControl)
	defer cleanup()
	var j *jobs.StartableJob
	jobID := jr.MakeJobID()
	insqlDB := s.InternalDB().(isql.DB)
	require.NoError(t, insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
		err = jr.CreateStartableJobWithTxn(ctx, &j, jobID, txn, jobs.Record{
			Details:  jobspb.ImportDetails{},
			Progress: jobspb.ImportProgress{},
			Username: username.RootUserName(),
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
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
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
	cleanup := jobs.TestingRegisterConstructor(jobspb.TypeRestore, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return jobstest.FakeResumer{
			OnResume: func(ctx context.Context) error {
				return resumeFunc.Load().(func(ctx context.Context) error)(ctx)
			},
		}
	}, jobs.UsesTenantCostControl)
	defer cleanup()
	woodyP, _ := username.MakeSQLUsernameFromUserInput("Woody Pride", username.PurposeValidation)
	rec := jobs.Record{
		Description:   "There's a snake in my boot!",
		Username:      woodyP,
		DescriptorIDs: []descpb.ID{1, 2, 3},
		Details:       jobspb.RestoreDetails{},
		Progress:      jobspb.RestoreProgress{},
	}
	insqlDB := s.InternalDB().(isql.DB)
	createStartableJob := func(t *testing.T) (sj *jobs.StartableJob) {
		jobID := jr.MakeJobID()
		require.NoError(t, insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
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
		require.Regexp(t, "boom", insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			var sj *jobs.StartableJob
			err := jr.CreateStartableJobWithTxn(ctx, &sj, jr.MakeJobID(), txn, rec)
			require.NoError(t, err)
			err = sj.Start(ctx)
			require.Regexp(t, `cannot resume .* job which is not committed`, err)
			return errors.New("boom")
		}))
	})
	t.Run("Start called with aborted txn", func(t *testing.T) {
		var sj *jobs.StartableJob
		_ = insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			err := jr.CreateStartableJobWithTxn(ctx, &sj, jr.MakeJobID(), txn, rec)
			if err != nil {
				return err
			}
			return txn.KV().Rollback(ctx)
		})
		err := sj.Start(ctx)
		require.Regexp(t, `cannot resume .* job which is not committed`, err)
	})
	t.Run("CleanupOnRollback called with active txn", func(t *testing.T) {
		require.Regexp(t, "boom", insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			var sj *jobs.StartableJob
			err := jr.CreateStartableJobWithTxn(ctx, &sj, jr.MakeJobID(), txn, rec)
			require.NoError(t, err)
			err = sj.CleanupOnRollback(ctx)
			require.Regexp(t, `cannot call CleanupOnRollback for a StartableJob with a non-finalized transaction`, err)
			return errors.New("boom")
		}))
	})
	t.Run("CleanupOnRollback called with committed txn", func(t *testing.T) {
		sj := createStartableJob(t)
		err := sj.CleanupOnRollback(ctx)
		require.Regexp(t, `cannot call CleanupOnRollback for a StartableJob created by a committed transaction`, err)
	})
	t.Run("CleanupOnRollback positive case", func(t *testing.T) {
		var sj *jobs.StartableJob
		require.Regexp(t, "boom", insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			err := jr.CreateStartableJobWithTxn(ctx, &sj, jr.MakeJobID(), txn, rec)
			require.NoError(t, err)
			return errors.New("boom")
		}))
		require.NoError(t, sj.CleanupOnRollback(ctx))
		for _, id := range jr.CurrentlyRunningJobs() {
			require.NotEqual(t, id, sj.ID())
		}
	})
	t.Run("Cancel", func(t *testing.T) {
		sj := createStartableJob(t)
		require.NoError(t, sj.Cancel(ctx))
		state, err := sj.TestingCurrentState(ctx)
		require.NoError(t, err)
		require.Equal(t, jobs.StateCancelRequested, state)
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
		require.NoError(t, insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
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
			st, err := loaded.TestingCurrentState(ctx)
			require.NoError(t, err)
			if st != jobs.StateSucceeded {
				return errors.Errorf("expected %s, got %s", jobs.StateSucceeded, st)
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
	params := base.TestServerArgs{}
	requestFilter, verifyFunc := testutils.TestingRequestFilterRetryTxnWithPrefix(t, txnName, 1)
	params.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: requestFilter,
	}
	s := serverutils.StartServerOnly(t, params)
	defer s.Stopper().Stop(ctx)
	jr := s.JobRegistry().(*jobs.Registry)
	cleanup := jobs.TestingRegisterConstructor(jobspb.TypeRestore, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return jobstest.FakeResumer{}
	}, jobs.UsesTenantCostControl)
	rec := jobs.Record{
		Details:  jobspb.RestoreDetails{},
		Progress: jobspb.RestoreProgress{},
		Username: username.TestUserName(),
	}
	cleanup()

	db := s.InternalDB().(isql.DB)
	jobID := jr.MakeJobID()
	var sj *jobs.StartableJob
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		txn.KV().SetDebugName(txnName)
		return jr.CreateStartableJobWithTxn(ctx, &sj, jobID, txn, rec)
	}))
	verifyFunc()
	require.NoError(t, sj.Start(ctx))
}

func TestRegistryTestingNudgeAdoptionQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.ResetConstructors()()

	ctx := context.Background()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	registry := s.JobRegistry().(*jobs.Registry)

	// The default FormatVersion value in SchemaChangeDetails corresponds to a
	// pre-20.1 job.
	rec := jobs.Record{
		DescriptorIDs: []descpb.ID{1},
		Details:       jobspb.BackupDetails{},
		Progress:      jobspb.BackupProgress{},
		Username:      username.TestUserName(),
	}

	defer jobs.ResetConstructors()()
	resuming := make(chan struct{})
	cleanup := jobs.TestingRegisterConstructor(jobspb.TypeBackup, func(_ *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return jobstest.FakeResumer{
			OnResume: func(ctx context.Context) error {
				resuming <- struct{}{}
				return nil
			},
		}
	}, jobs.UsesTenantCostControl)
	defer cleanup()

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

func TestStateSafeFormatter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	redacted := string(redact.Sprint(jobs.StateCanceled).Redact())
	expected := string(jobs.StateCanceled)
	require.Equal(t, expected, redacted)
}

type fakeMetrics struct {
	N *metric.Counter
}

func (fm fakeMetrics) MetricStruct() {}

func makeFakeMetrics() fakeMetrics {
	return fakeMetrics{
		N: metric.NewCounter(metric.Metadata{
			Name:        "fake.count",
			Help:        "utterly fake metric",
			Measurement: "N",
			Unit:        metric.Unit_COUNT,
			MetricType:  io_prometheus_client.MetricType_COUNTER,
		}),
	}
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

	fakeBackupMetrics := makeFakeMetrics()
	defer jobs.TestingRegisterConstructor(jobspb.TypeBackup,
		func(j *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return jobstest.FakeResumer{
				OnResume: func(ctx context.Context) error {
					defer fakeBackupMetrics.N.Inc(1)
					return waitForErr(ctx)
				},
				FailOrCancel: func(ctx context.Context) error {
					return waitForErr(ctx)
				},
			}
		},
		jobs.UsesTenantCostControl, jobs.WithJobMetrics(fakeBackupMetrics),
	)()

	defer jobs.TestingRegisterConstructor(jobspb.TypeImport, func(_ *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return jobstest.FakeResumer{
			OnResume: func(ctx context.Context) error {
				return waitForErr(ctx)
			},
			FailOrCancel: func(ctx context.Context) error {
				return waitForErr(ctx)
			},
		}
	}, jobs.UsesTenantCostControl)()

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
			Username:      username.TestUserName(),
		}
		_, err := registry.CreateAdoptableJobWithTxn(ctx, rec, registry.MakeJobID(), nil /* txn */)
		require.NoError(t, err)
		errCh := <-resuming
		backupMetrics := registry.MetricsStruct().JobMetrics[jobspb.TypeBackup]
		require.Equal(t, int64(1), backupMetrics.CurrentlyRunning.Value())
		errCh <- nil
		int64EqSoon(t, backupMetrics.ResumeCompleted.Count, 1)
		int64EqSoon(t, registry.MetricsStruct().JobSpecificMetrics[jobspb.TypeBackup].(fakeMetrics).N.Count, 1)

	})
	t.Run("restart, pause, resume, then success", func(t *testing.T) {
		_, db, registry, cleanup := setup(t)
		defer cleanup()
		rec := jobs.Record{
			DescriptorIDs: []descpb.ID{1},
			Details:       jobspb.ImportDetails{},
			Progress:      jobspb.ImportProgress{},
			Username:      username.TestUserName(),
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
			var state string
			tdb.QueryRow(t, fmt.Sprintf("SELECT status, payload FROM (%s)",
				jobutils.InternalSystemJobsBaseQuery), jobID).Scan(
				&state, &payloadBytes)
			require.Equal(t, "paused", state)
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
			Username:      username.TestUserName(),
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
			Username:      username.TestUserName(),
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

	s := serverutils.StartServerOnly(t, base.TestServerArgs{Knobs: knobs})
	defer s.Stopper().Stop(ctx)
	registry := s.JobRegistry().(*jobs.Registry)

	// The default FormatVersion value in SchemaChangeDetails corresponds to a
	// pre-20.1 job.
	rec := jobs.Record{
		DescriptorIDs: []descpb.ID{1},
		Details:       jobspb.BackupDetails{},
		Progress:      jobspb.BackupProgress{},
		Username:      username.TestUserName(),
	}

	defer jobs.ResetConstructors()()
	resumed := make(chan error, 1)
	defer jobs.TestingRegisterConstructor(jobspb.TypeBackup, func(j *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return jobstest.FakeResumer{
			OnResume: func(ctx context.Context) error {
				defer close(resumed)
				_, err := s.InternalExecutor().(isql.Executor).Exec(
					ctx, "set-claim-null", nil, /* txn */
					`UPDATE system.jobs SET claim_session_id = NULL WHERE id = $1`,
					j.ID())
				assert.NoError(t, err)
				err = j.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
					return nil
				})
				resumed <- err
				return err
			},
		}
	}, jobs.UsesTenantCostControl)()

	_, err := registry.CreateJobWithTxn(ctx, rec, registry.MakeJobID(), nil)
	require.NoError(t, err)
	registry.TestingNudgeAdoptionQueue()
	require.Regexp(t, `expected session "\w+" but found NULL`, <-resumed)
}

// TestJobsRetry tests that (1) non-cancelable jobs retry if they fail with an
// error marked as permanent, (2) reverting job always retry instead of failing.
func TestJobsRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("retry non-cancelable running", func(t *testing.T) {
		rts := registryTestSuite{}
		defer rts.setUp(t)()
		defer rts.tearDown()
		// Make mockJob non-cancelable, ensuring that non-cancelable jobs are retried in running state.
		rts.mockJob.SetNonCancelable(rts.ctx, func(ctx context.Context, nonCancelable bool) bool {
			return true
		})
		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.idb(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		// First job run in running state.
		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StateRunning)
		// Make Resume fail.
		rts.resumeCh <- errors.New("non-permanent error")
		rts.mu.e.ResumeExit++

		// Job should be retried in running state.
		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StateRunning)
		rts.resumeCh <- jobs.MarkAsPermanentJobError(errors.New("permanent error"))
		rts.mu.e.ResumeExit++

		// Job should now revert.
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StateReverting)
		rts.failOrCancelCh <- nil
		rts.mu.e.OnFailOrCancelExit = true

		close(rts.failOrCancelCh)
		close(rts.failOrCancelCheckCh)
		rts.check(t, jobs.StateFailed)
	})

	t.Run("retry reverting", func(t *testing.T) {
		// - Create a job.
		// - Fail the job in resume to cause the job to revert.
		// - Fail the job in revert state using a non-retryable error.
		// - Make sure that the jobs is retried and is again in the revert state.
		rts := registryTestSuite{}
		defer rts.setUp(t)()
		defer rts.tearDown()
		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.idb(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StateRunning)

		// Make Resume fail.
		rts.resumeCh <- errors.New("failing resume to revert")
		rts.mu.e.ResumeExit++

		// Job is now reverting.
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StateReverting)

		// Fail the job in reverting state without a retryable error.
		rts.failOrCancelCh <- errors.New("failing with a non-retryable error")
		rts.mu.e.OnFailOrCancelExit = true

		// Job should be retried.
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StateReverting)
		rts.failOrCancelCh <- nil
		rts.mu.e.OnFailOrCancelExit = true

		close(rts.failOrCancelCh)
		close(rts.failOrCancelCheckCh)
		rts.check(t, jobs.StateFailed)
	})

	t.Run("retry non-cancelable reverting", func(t *testing.T) {
		// - Create a non-cancelable job.
		// - Fail the job in resume with a permanent error to cause the job to revert.
		// - Fail the job in revert state using a permanent error to ensure that the
		//   retries with a permanent error as well.
		// - Make sure that the jobs is retried and is again in the revert state.
		rts := registryTestSuite{}
		defer rts.setUp(t)()
		defer rts.tearDown()
		// Make mockJob non-cancelable, ensuring that non-cancelable jobs are retried in reverting state.
		rts.mockJob.SetNonCancelable(rts.ctx, func(ctx context.Context, nonCancelable bool) bool {
			return true
		})
		j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.idb(), rts.mockJob)
		if err != nil {
			t.Fatal(err)
		}
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StateRunning)

		// Make Resume fail with a permanent error.
		rts.resumeCh <- jobs.MarkAsPermanentJobError(errors.New("permanent error"))
		rts.mu.e.ResumeExit++

		// Job is now reverting.
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StateReverting)

		// Fail the job in reverting state with a permanent error a retryable error.
		rts.failOrCancelCh <- jobs.MarkAsPermanentJobError(errors.New("permanent error"))
		rts.mu.e.OnFailOrCancelExit = true

		// Job should be retried.
		rts.mu.e.OnFailOrCancelStart = true
		rts.failOrCancelCheckCh <- struct{}{}
		rts.check(t, jobs.StateReverting)
		rts.failOrCancelCh <- nil
		rts.mu.e.OnFailOrCancelExit = true

		close(rts.failOrCancelCh)
		close(rts.failOrCancelCheckCh)
		rts.check(t, jobs.StateFailed)
	})
}

func TestPausepoints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	registry := s.JobRegistry().(*jobs.Registry)
	defer s.Stopper().Stop(ctx)
	idb := s.InternalDB().(isql.DB)
	defer jobs.TestingRegisterConstructor(jobspb.TypeImport, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return jobstest.FakeResumer{
			OnResume: func(ctx context.Context) error {
				if err := registry.CheckPausepoint("test_pause_foo"); err != nil {
					return err
				}
				return nil
			},
		}
	}, jobs.UsesTenantCostControl)()

	rec := jobs.Record{
		DescriptorIDs: []descpb.ID{1},
		Details:       jobspb.ImportDetails{},
		Progress:      jobspb.ImportProgress{},
		Username:      username.TestUserName(),
	}

	for _, tc := range []struct {
		name     string
		points   string
		expected jobs.State
	}{
		{"none", "", jobs.StateSucceeded},
		{"pausepoint-only", "test_pause_foo", jobs.StatePaused},
		{"other-var-only", "test_pause_bar", jobs.StateSucceeded},
		{"pausepoint-and-other", "test_pause_bar,test_pause_foo,test_pause_baz", jobs.StatePaused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sqlDB.Exec("SET CLUSTER SETTING jobs.debug.pausepoints = $1", tc.points)
			require.NoError(t, err)

			jobID := registry.MakeJobID()
			var sj *jobs.StartableJob
			require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
				return registry.CreateStartableJobWithTxn(ctx, &sj, jobID, txn, rec)
			}))
			require.NoError(t, sj.Start(ctx))
			if tc.expected == jobs.StateSucceeded {
				require.NoError(t, sj.AwaitCompletion(ctx))
			} else {
				require.Error(t, sj.AwaitCompletion(ctx))
			}
			state, err := sj.TestingCurrentState(ctx)
			// Map pause-requested to paused to avoid races.
			if state == jobs.StatePauseRequested {
				state = jobs.StatePaused
			}
			require.Equal(t, tc.expected, state)
			require.NoError(t, err)
		})
	}
}

func TestJobTypeMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderShort(t)

	ctx := context.Background()
	// Make sure we set polling interval before we start the server. Otherwise, we
	// might pick up the default value (30 second), which would make this test
	// slow.
	args := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
		Settings: cluster.MakeTestingClusterSettings(),
	}
	jobs.PollJobsMetricsInterval.Override(ctx, &args.Settings.SV, 10*time.Millisecond)
	s, sqlDB, _ := serverutils.StartServer(t, args)
	defer s.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(sqlDB)
	runner.Exec(t, "CREATE USER testuser")
	reg := s.JobRegistry().(*jobs.Registry)

	waitForPausedCount := func(typ jobspb.Type, numPaused int64) {
		testutils.SucceedsSoon(t, func() error {
			currentlyPaused := reg.MetricsStruct().JobMetrics[typ].CurrentlyPaused.Value()
			if currentlyPaused != numPaused {
				return fmt.Errorf(
					"expected (%+v) paused jobs of type (%+v), found (%+v)",
					numPaused,
					typ,
					currentlyPaused,
				)
			}
			return nil
		})
	}

	typeToRecord := map[jobspb.Type]jobs.Record{
		jobspb.TypeChangefeed: {
			Details:  jobspb.ChangefeedDetails{},
			Progress: jobspb.ChangefeedProgress{},
			Username: username.TestUserName(),
		},
		jobspb.TypeImport: {
			Details:  jobspb.ImportDetails{},
			Progress: jobspb.ImportProgress{},
			Username: username.TestUserName(),
		},
		jobspb.TypeSchemaChange: {
			Details:  jobspb.SchemaChangeDetails{},
			Progress: jobspb.SchemaChangeProgress{},
			Username: username.TestUserName(),
		},
	}

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	writePTSRecord := func(jobID jobspb.JobID) (uuid.UUID, error) {
		id := uuid.MakeV4()
		record := jobsprotectedts.MakeRecord(
			id, int64(jobID), s.Clock().Now(), nil,
			jobsprotectedts.Jobs, ptpb.MakeClusterTarget(),
		)
		return id,
			execCfg.InternalDB.Txn(context.Background(), func(ctx context.Context, txn isql.Txn) error {
				return execCfg.ProtectedTimestampProvider.WithTxn(txn).Protect(context.Background(), record)
			})
	}
	relesePTSRecord := func(id uuid.UUID) error {
		return execCfg.InternalDB.Txn(context.Background(), func(ctx context.Context, txn isql.Txn) error {
			return execCfg.ProtectedTimestampProvider.WithTxn(txn).Release(context.Background(), id)
		})
	}

	checkPTSCounts := func(typ jobspb.Type, count int64) {
		testutils.SucceedsSoon(t, func() error {
			m := reg.MetricsStruct().JobMetrics[typ]
			if m.NumJobsWithPTS.Value() == count && (count == 0 || m.ProtectedAge.Value() > 0) {
				return nil
			}
			return errors.Newf("still waiting for PTS count to reach %d: c=%d age=%d",
				count, m.NumJobsWithPTS.Value(), m.ProtectedAge.Value())
		})
	}

	for typ := range typeToRecord {
		defer jobs.TestingRegisterConstructor(typ, func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return jobstest.FakeResumer{
				OnResume: func(ctx context.Context) error {
					<-ctx.Done()
					return ctx.Err()
				},
			}
		}, jobs.UsesTenantCostControl)()
	}

	makeJob := func(ctx context.Context,
		typ jobspb.Type,
	) *jobs.StartableJob {
		j, err := jobs.TestingCreateAndStartJob(ctx, reg, s.InternalDB().(isql.DB), typeToRecord[typ])
		if err != nil {
			t.Fatal(err)
		}
		return j
	}

	cfJob := makeJob(context.Background(), jobspb.TypeChangefeed)
	cfJob2 := makeJob(context.Background(), jobspb.TypeChangefeed)
	importJob := makeJob(context.Background(), jobspb.TypeImport)
	scJob := makeJob(context.Background(), jobspb.TypeSchemaChange)

	// Write few PTS records
	cfJobPTSID, err := writePTSRecord(cfJob.ID())
	require.NoError(t, err)
	_, err = writePTSRecord(cfJob.ID())
	require.NoError(t, err)
	importJobPTSID, err := writePTSRecord(importJob.ID())
	require.NoError(t, err)

	checkPTSCounts(jobspb.TypeChangefeed, 2)
	checkPTSCounts(jobspb.TypeImport, 1)

	// Pause all job types.
	runner.Exec(t, "PAUSE JOB $1", cfJob.ID())
	waitForPausedCount(jobspb.TypeChangefeed, 1)
	runner.Exec(t, "PAUSE JOB $1", cfJob2.ID())
	waitForPausedCount(jobspb.TypeChangefeed, 2)
	runner.Exec(t, "PAUSE JOB $1", importJob.ID())
	waitForPausedCount(jobspb.TypeImport, 1)
	runner.Exec(t, "PAUSE JOB $1", scJob.ID())
	waitForPausedCount(jobspb.TypeSchemaChange, 1)

	// Release some of the pts records.
	require.NoError(t, relesePTSRecord(cfJobPTSID))
	require.NoError(t, relesePTSRecord(importJobPTSID))
	checkPTSCounts(jobspb.TypeChangefeed, 1)
	checkPTSCounts(jobspb.TypeImport, 0)

	// Resume / cancel jobs.
	runner.Exec(t, "RESUME JOB $1", cfJob.ID())
	waitForPausedCount(jobspb.TypeChangefeed, 1)
	runner.Exec(t, "CANCEL JOB $1", cfJob2.ID())
	waitForPausedCount(jobspb.TypeChangefeed, 0)
	runner.Exec(t, "RESUME JOB $1", importJob.ID())
	waitForPausedCount(jobspb.TypeImport, 0)
	runner.Exec(t, "CANCEL JOB $1", scJob.ID())
	waitForPausedCount(jobspb.TypeSchemaChange, 0)

	runner.Exec(t, "CANCEL JOB $1", cfJob.ID())
	runner.Exec(t, "CANCEL JOB $1", importJob.ID())
}

func TestLoadJobProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	r := s.ApplicationLayer().JobRegistry().(*jobs.Registry)
	defer s.Stopper().Stop(context.Background())

	progress := &jobspb.Progress{
		Details: jobspb.WrapProgressDetails(jobspb.ImportProgress{ReadProgress: []float32{7.1}}),
	}
	rec := jobs.Record{
		JobID:    7,
		Username: username.TestUserName(),
		Details:  jobspb.ImportDetails{},
		Progress: progress.UnwrapDetails(),
	}
	_, err := r.CreateJobWithTxn(ctx, rec, 7, nil)
	require.NoError(t, err)

	p, err := jobs.LoadJobProgress(ctx, s.InternalDB().(isql.DB), 7)
	require.NoError(t, err)
	require.Equal(t, []float32{7.1}, p.GetDetails().(*jobspb.Progress_Import).Import.ReadProgress)
}
