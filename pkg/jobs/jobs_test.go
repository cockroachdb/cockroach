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
	"testing"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
	"github.com/kr/pretty"
	"github.com/pkg/errors"
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

	if status == jobs.StatusPending {
		return nil
	}
	started := timeutil.FromUnixMicros(payload.StartedMicros)
	if started.Equal(timeutil.UnixEpoch) && status == jobs.StatusSucceeded {
		return errors.Errorf("started time is empty but job claims to be successful")
	}
	if status == jobs.StatusRunning || status == jobs.StatusPaused {
		return nil
	}

	if e, a := expected.Error, payload.Error; e != a {
		return errors.Errorf("expected error %q, got %q", e, a)
	}
	return nil
}

func TestJobsTableProgressFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	var table, schema string
	sqlutils.MakeSQLRunner(db).QueryRow(t, `SHOW CREATE system.jobs`).Scan(&table, &schema)
	if !strings.Contains(schema, `FAMILY progress (progress)`) {
		t.Fatalf("expected progress family, got %q", schema)
	}
}

func TestRegistryLifecycle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer jobs.ResetConstructors()()

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond

	ctx := context.TODO()

	s, outerDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(outerDB)

	registry := s.JobRegistry().(*jobs.Registry)

	done := make(chan struct{})
	defer close(done)

	type Counters struct {
		resume, resumeExit, terminal int
		// These sometimes retry, so just use bools.
		fail, success bool
	}

	var lock syncutil.Mutex
	var e, a Counters

	mockJob := jobs.Record{Details: jobspb.ImportDetails{}, Progress: jobspb.ImportProgress{}}

	check := func(t *testing.T) {
		t.Helper()
		if err := retry.ForDuration(time.Second*5, func() error {
			lock.Lock()
			defer lock.Unlock()
			if e != a {
				return errors.Errorf("expected %v, got %v", e, a)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	clear := func() {
		lock.Lock()
		a = Counters{}
		e = Counters{}
		lock.Unlock()
	}

	resumeCh := make(chan error)
	progressCh := make(chan struct{})
	// resumeCheckCh is used to wait for the resume check loop to start. This is
	// useful to prevent race conditions where progressCh checking jobs.Progressed
	// can race with a PAUSE or CANCEL transaction.
	resumeCheckCh := make(chan struct{})
	termCh := make(chan struct{})

	// Instead of a ch for success and fail, use a variable because they can
	// retry since they are in a transaction.
	var successErr, failErr error

	jobs.RegisterConstructor(jobspb.TypeImport, func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return jobs.FakeResumer{
			OnResume: func() error {
				lock.Lock()
				a.resume++
				lock.Unlock()
				defer func() {
					lock.Lock()
					a.resumeExit++
					lock.Unlock()
				}()
				for {
					<-resumeCheckCh
					select {
					case err := <-resumeCh:
						return err
					case <-progressCh:
						err := job.FractionProgressed(ctx, jobs.FractionUpdater(0))
						if err != nil {
							return err
						}
						// continue
					}
				}
			},

			Fail: func() error {
				lock.Lock()
				defer lock.Unlock()
				a.fail = true
				return failErr
			},

			Success: func() error {
				lock.Lock()
				defer lock.Unlock()
				a.success = true
				return successErr
			},

			Terminal: func() {
				lock.Lock()
				a.terminal++
				lock.Unlock()
				termCh <- struct{}{}
			},
		}
	})

	var jobErr = errors.New("error")

	t.Run("normal success", func(t *testing.T) {
		clear()
		_, _, err := registry.StartJob(ctx, nil, mockJob)
		if err != nil {
			t.Fatal(err)
		}
		e.resume++
		check(t)
		resumeCheckCh <- struct{}{}
		resumeCh <- nil
		e.resumeExit++
		e.success = true
		e.terminal++
		<-termCh
		check(t)
	})

	t.Run("pause", func(t *testing.T) {
		clear()
		job, _, err := registry.StartJob(ctx, nil, mockJob)
		if err != nil {
			t.Fatal(err)
		}
		e.resume++
		check(t)
		sqlDB.Exec(t, "PAUSE JOB $1", *job.ID())
		resumeCheckCh <- struct{}{}
		progressCh <- struct{}{}
		e.resumeExit++
		check(t)
		sqlDB.Exec(t, "PAUSE JOB $1", *job.ID())
		check(t)
		sqlDB.Exec(t, "RESUME JOB $1", *job.ID())
		resumeCheckCh <- struct{}{}
		resumeCh <- nil
		e.resume++
		e.resumeExit++
		e.success = true
		e.terminal++
		<-termCh
		check(t)
	})

	t.Run("cancel", func(t *testing.T) {
		clear()
		job, _, err := registry.StartJob(ctx, nil, mockJob)
		if err != nil {
			t.Fatal(err)
		}
		e.resume++
		check(t)
		sqlDB.Exec(t, "CANCEL JOB $1", *job.ID())
		// Test for a canceled error message.
		if err := job.CheckStatus(ctx); !testutils.IsError(err, "cannot update progress on canceled job") {
			t.Fatalf("unexpected %v", err)
		}
		resumeCheckCh <- struct{}{}
		progressCh <- struct{}{}
		e.resumeExit++
		e.fail = true
		e.terminal++
		<-termCh
		check(t)
	})

	// Verify that pause and cancel in a rollback do nothing.
	t.Run("rollback", func(t *testing.T) {
		clear()
		job, _, err := registry.StartJob(ctx, nil, mockJob)
		if err != nil {
			t.Fatal(err)
		}
		e.resume++
		resumeCheckCh <- struct{}{}
		check(t)
		// Rollback a CANCEL.
		{
			txn, err := outerDB.Begin()
			if err != nil {
				t.Fatal(err)
			}
			// OnFailOrCancel is called before the txn fails, so this should be set.
			e.fail = true
			if _, err := txn.Exec("CANCEL JOB $1", *job.ID()); err != nil {
				t.Fatal(err)
			}
			if err := txn.Rollback(); err != nil {
				t.Fatal(err)
			}
			progressCh <- struct{}{}
			resumeCheckCh <- struct{}{}
			check(t)
		}
		// Rollback a PAUSE.
		{
			txn, err := outerDB.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := txn.Exec("PAUSE JOB $1", *job.ID()); err != nil {
				t.Fatal(err)
			}
			if err := txn.Rollback(); err != nil {
				t.Fatal(err)
			}
			progressCh <- struct{}{}
			resumeCheckCh <- struct{}{}
			check(t)
		}
		// Now pause it for reals.
		{
			txn, err := outerDB.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := txn.Exec("PAUSE JOB $1", *job.ID()); err != nil {
				t.Fatal(err)
			}
			// Not committed yet, so state shouldn't have changed.
			check(t)
			if err := txn.Commit(); err != nil {
				t.Fatal(err)
			}
			// Test for a paused error message.
			if err := job.CheckStatus(ctx); !testutils.IsError(err, "cannot update progress on paused job") {
				t.Fatalf("unexpected %v", err)
			}
		}
		progressCh <- struct{}{}
		e.resumeExit++
		check(t)
		// Rollback a RESUME.
		{
			txn, err := outerDB.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := txn.Exec("RESUME JOB $1", *job.ID()); err != nil {
				t.Fatal(err)
			}
			if err := txn.Rollback(); err != nil {
				t.Fatal(err)
			}
			check(t)
		}
		// Commit a RESUME.
		{
			txn, err := outerDB.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := txn.Exec("RESUME JOB $1", *job.ID()); err != nil {
				t.Fatal(err)
			}
			// Not committed yet, so state shouldn't have changed.
			check(t)
			if err := txn.Commit(); err != nil {
				t.Fatal(err)
			}
		}
		e.resume++
		check(t)
		resumeCheckCh <- struct{}{}
		resumeCh <- nil
		e.resumeExit++
		e.success = true
		e.terminal++
		<-termCh
		check(t)
	})

	t.Run("failed running", func(t *testing.T) {
		clear()
		_, _, err := registry.StartJob(ctx, nil, mockJob)
		if err != nil {
			t.Fatal(err)
		}
		e.resume++
		check(t)
		resumeCheckCh <- struct{}{}
		resumeCh <- jobErr
		e.resumeExit++
		e.fail = true
		e.terminal++
		<-termCh
		check(t)
	})

	// Attempt to mark success, but fail.
	t.Run("fail marking success", func(t *testing.T) {
		clear()
		successErr = jobErr
		defer func() { successErr = nil }()
		_, _, err := registry.StartJob(ctx, nil, mockJob)
		if err != nil {
			t.Fatal(err)
		}
		e.resume++
		check(t)
		resumeCheckCh <- struct{}{}
		resumeCh <- nil
		e.resumeExit++
		e.success = true
		e.fail = true
		e.terminal++
		<-termCh
		check(t)
	})

	// Fail the job, so expected it to attempt to mark failed, but fail that
	// also. Thus it should not trigger OnTerminal.
	t.Run("fail marking success and failed", func(t *testing.T) {
		clear()
		successErr = jobErr
		failErr = jobErr
		defer func() { failErr = nil }()
		_, _, err := registry.StartJob(ctx, nil, mockJob)
		if err != nil {
			t.Fatal(err)
		}
		e.resume++
		check(t)
		resumeCheckCh <- struct{}{}
		resumeCh <- nil
		e.resumeExit++
		e.success = true
		e.fail = true
		// It should restart.
		e.resume++
		check(t)
		// But let it succeed.
		successErr = nil
		resumeCheckCh <- struct{}{}
		resumeCh <- nil
		e.resumeExit++
		e.terminal++
		<-termCh
		check(t)
	})

	// Fail the job, but also fail to mark it failed. No OnTerminal.
	t.Run("fail marking failed", func(t *testing.T) {
		clear()
		failErr = jobErr
		_, _, err := registry.StartJob(ctx, nil, mockJob)
		if err != nil {
			t.Fatal(err)
		}
		e.resume++
		check(t)
		resumeCheckCh <- struct{}{}
		resumeCh <- jobErr
		e.resumeExit++
		e.fail = true
		// It should restart.
		e.resume++
		check(t)
		// But let it fail.
		failErr = nil
		resumeCheckCh <- struct{}{}
		resumeCh <- jobErr
		e.resumeExit++
		e.terminal++
		<-termCh
		check(t)
	})

	t.Run("fail 2.0 jobs with no progress", func(t *testing.T) {
		clear()
		job, _, err := registry.StartJob(ctx, nil, mockJob)
		if err != nil {
			t.Fatal(err)
		}
		e.resume++
		check(t)
		sqlDB.Exec(t, "PAUSE JOB $1", *job.ID())
		resumeCheckCh <- struct{}{}
		progressCh <- struct{}{}
		e.resumeExit++
		check(t)
		sqlDB.Exec(t, `UPDATE system.jobs SET progress = NULL, status = $2 WHERE id = $1`, *job.ID(), jobs.StatusRunning)
		testutils.SucceedsSoon(t, func() error {
			var status jobs.Status
			var err string
			sqlDB.QueryRow(t, `SELECT error, status FROM [SHOW JOBS] WHERE job_id = $1`, *job.ID()).Scan(&err, &status)
			if status != jobs.StatusFailed {
				return errors.Errorf("unexpected status: %s", status)
			}
			if err != "job predates cluster upgrade and must be re-run" {
				return errors.Errorf("unexpected error: %s", err)
			}
			return nil
		})
		check(t)
	})
}

func TestJobLifecycle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer jobs.ResetConstructors()()

	ctx := context.TODO()

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
			OnResume: func() error {
				<-done
				return nil
			},
		}
	})

	startLeasedJob := func(t *testing.T, record jobs.Record) (*jobs.Job, expectation) {
		beforeTime := timeutil.Now()
		job, _, err := registry.StartJob(ctx, nil, record)
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

		if err := woodyJob.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := woodyExp.verify(woodyJob.ID(), jobs.StatusPending); err != nil {
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

		if err := woodyJob.Succeeded(ctx, jobs.NoopFn); err != nil {
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
		if err := buzzExp.verify(buzzJob.ID(), jobs.StatusPending); err != nil {
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

		if err := buzzJob.Failed(ctx, errors.New("Buzz Lightyear can't fly"), jobs.NoopFn); err != nil {
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

		if err := sidJob.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := sidExp.verify(sidJob.ID(), jobs.StatusPending); err != nil {
			t.Fatal(err)
		}

		if err := sidJob.Failed(ctx, errors.New("Sid is a total failure"), jobs.NoopFn); err != nil {
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
			if err := job.Succeeded(ctx, jobs.NoopFn); err != nil {
				t.Fatal(err)
			}
			if err := exp.verify(job.ID(), jobs.StatusSucceeded); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("non-nil error marks job as failed", func(t *testing.T) {
			job, exp := createDefaultJob()
			exp.Error = "boom"
			if err := job.Failed(ctx, errors.New(exp.Error), jobs.NoopFn); err != nil {
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
			if err := job.Succeeded(ctx, jobs.NoopFn); !testutils.IsError(err, "wrong wireType") {
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
			if err := job.Failed(ctx, errors.New("boom"), jobs.NoopFn); !testutils.IsError(err, "wrong wireType") {
				t.Fatalf("unexpected: %v", err)
			}
		})
	})

	t.Run("cancelable jobs can be paused until finished", func(t *testing.T) {
		job, exp := startLeasedJob(t, defaultRecord)

		if err := registry.Pause(ctx, nil, *job.ID()); err != nil {
			t.Fatal(err)
		}
		if err := registry.Pause(ctx, nil, *job.ID()); err != nil {
			t.Fatal(err)
		}
		if err := exp.verify(job.ID(), jobs.StatusPaused); err != nil {
			t.Fatal(err)
		}
		if err := registry.Resume(ctx, nil, *job.ID()); err != nil {
			t.Fatal(err)
		}
		if err := registry.Resume(ctx, nil, *job.ID()); err != nil {
			t.Fatal(err)
		}
		if err := exp.verify(job.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}

		// Pause fails after job is successful.
		if err := job.Succeeded(ctx, jobs.NoopFn); err != nil {
			t.Fatal(err)
		}
		if err := registry.Pause(ctx, nil, *job.ID()); !testutils.IsError(err, "cannot pause succeeded job") {
			t.Fatalf("expected 'cannot pause succeeded job', but got '%s'", err)
		}
	})

	t.Run("cancelable jobs can be canceled until finished", func(t *testing.T) {
		{
			job, exp := startLeasedJob(t, defaultRecord)
			if err := registry.Cancel(ctx, nil, *job.ID()); err != nil {
				t.Fatal(err)
			}
			if err := exp.verify(job.ID(), jobs.StatusCanceled); err != nil {
				t.Fatal(err)
			}
		}

		{
			job, exp := startLeasedJob(t, defaultRecord)
			if err := job.Started(ctx); err != nil {
				t.Fatal(err)
			}
			if err := registry.Cancel(ctx, nil, *job.ID()); err != nil {
				t.Fatal(err)
			}
			if err := exp.verify(job.ID(), jobs.StatusCanceled); err != nil {
				t.Fatal(err)
			}
		}

		{
			job, exp := startLeasedJob(t, defaultRecord)
			if err := registry.Pause(ctx, nil, *job.ID()); err != nil {
				t.Fatal(err)
			}
			if err := registry.Cancel(ctx, nil, *job.ID()); err != nil {
				t.Fatal(err)
			}
			if err := exp.verify(job.ID(), jobs.StatusCanceled); err != nil {
				t.Fatal(err)
			}
		}

		{
			job, _ := startLeasedJob(t, defaultRecord)
			if err := job.Succeeded(ctx, jobs.NoopFn); err != nil {
				t.Fatal(err)
			}
			expectedErr := "job with status succeeded cannot be canceled"
			if err := registry.Cancel(ctx, nil, *job.ID()); !testutils.IsError(err, expectedErr) {
				t.Fatalf("expected '%s', but got '%s'", expectedErr, err)
			}
		}
	})

	t.Run("unpaused jobs cannot be resumed", func(t *testing.T) {
		checkResumeFails := func(job *jobs.Job, status jobs.Status) {
			expectedErr := fmt.Sprintf("job with status %s cannot be resumed", status)
			if err := registry.Resume(ctx, nil, *job.ID()); !testutils.IsError(err, expectedErr) {
				t.Errorf("expected '%s', but got '%v'", expectedErr, err)
			}
		}

		{
			job, _ := createDefaultJob()
			checkResumeFails(job, jobs.StatusPending)
		}

		{
			job, _ := startLeasedJob(t, defaultRecord)
			if err := registry.Cancel(ctx, nil, *job.ID()); err != nil {
				t.Fatal(err)
			}
			checkResumeFails(job, jobs.StatusCanceled)
		}

		{
			job, _ := startLeasedJob(t, defaultRecord)
			if err := job.Succeeded(ctx, jobs.NoopFn); err != nil {
				t.Fatal(err)
			}
			checkResumeFails(job, jobs.StatusSucceeded)
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
		if err := job.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Succeeded(ctx, jobs.NoopFn); err != nil {
			t.Fatal(err)
		}
		if err := job.Succeeded(ctx, jobs.NoopFn); err != nil {
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
				ctx, func(context.Context, jobspb.ProgressDetails) hlc.Timestamp { return ts },
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
			ctx, func(context.Context, jobspb.ProgressDetails) hlc.Timestamp {
				return hlc.Timestamp{WallTime: -1}
			},
		); !testutils.IsError(err, "outside allowable range") {
			t.Fatalf("expected 'outside allowable range' error, but got %v", err)
		}
	})

	t.Run("progress on non-started job fails", func(t *testing.T) {
		job, _ := createDefaultJob()
		if err := job.FractionProgressed(ctx, jobs.FractionUpdater(0.5)); !testutils.IsError(
			err, `cannot update progress on pending job \(id \d+\)`,
		) {
			t.Fatalf("expected 'cannot update progress' error, but got %v", err)
		}
	})

	t.Run("progress on finished job fails", func(t *testing.T) {
		job, _ := createDefaultJob()
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Succeeded(ctx, jobs.NoopFn); err != nil {
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
		if err := registry.Pause(ctx, nil, *job.ID()); err != nil {
			t.Fatal(err)
		}
		if err := job.FractionProgressed(ctx, jobs.FractionUpdater(0.5)); !testutils.IsError(
			err, `cannot update progress on paused job \(id \d+\)`,
		) {
			t.Fatalf("expected progress error, but got %v", err)
		}
	})

	t.Run("progress on canceled job fails", func(t *testing.T) {
		job, _ := startLeasedJob(t, defaultRecord)
		if err := registry.Cancel(ctx, nil, *job.ID()); err != nil {
			t.Fatal(err)
		}
		if err := job.FractionProgressed(ctx, jobs.FractionUpdater(0.5)); !testutils.IsError(
			err, `cannot update progress on canceled job \(id \d+\)`,
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
		if err := job.Succeeded(ctx, jobs.NoopFn); err != nil {
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
		if err := exp.verify(job.ID(), jobs.StatusPending); err != nil {
			t.Fatal(err)
		}
		newDetails := jobspb.RestoreDetails{URIs: []string{"new"}}
		exp.Record.Details = newDetails
		if err := job.SetDetails(ctx, newDetails); err != nil {
			t.Fatal(err)
		}
		if err := exp.verify(job.ID(), jobs.StatusPending); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("set progress works", func(t *testing.T) {
		job, exp := createJob(jobs.Record{
			Details:  jobspb.RestoreDetails{},
			Progress: jobspb.RestoreProgress{},
		})
		if err := exp.verify(job.ID(), jobs.StatusPending); err != nil {
			t.Fatal(err)
		}
		newDetails := jobspb.RestoreProgress{HighWater: []byte{42}}
		exp.Record.Progress = newDetails
		if err := job.SetProgress(ctx, newDetails); err != nil {
			t.Fatal(err)
		}
		if err := exp.verify(job.ID(), jobs.StatusPending); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("cannot pause or resume schema changes", func(t *testing.T) {
		job, _ := createJob(jobs.Record{
			Details:  jobspb.SchemaChangeDetails{},
			Progress: jobspb.SchemaChangeProgress{},
		})
		if err := registry.Pause(ctx, nil, *job.ID()); !testutils.IsError(err, "is not controllable") {
			t.Fatalf("unexpected %v", err)
		}
		if err := registry.Resume(ctx, nil, *job.ID()); !testutils.IsError(err, "is not controllable") {
			t.Fatalf("unexpected %v", err)
		}
		if err := registry.Cancel(ctx, nil, *job.ID()); err != nil {
			t.Fatalf("unexpected %v", err)
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
	defer s.Stopper().Stop(context.TODO())

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
	defer s.Stopper().Stop(context.TODO())

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
	defer s.Stopper().Stop(context.TODO())

	// Create at least 4 row, ensuring the last 3 rows are corrupted.
	if _, err := sqlDB.Exec(`
     -- Ensure there is at least one row in system.jobs.
     CREATE TABLE foo(x INT); ALTER TABLE foo ADD COLUMN y INT;
     -- Create a corrupted payload field from the first row.
     INSERT INTO system.jobs(id, status, payload, progress) SELECT id+1, status, '\xaaaa'::BYTES, progress FROM system.jobs ORDER BY id LIMIT 1;
     -- Create a corrupted progress field.
     INSERT INTO system.jobs(id, status, payload, progress) SELECT id+2, status, payload, '\xaaaa'::BYTES FROM system.jobs ORDER BY id LIMIT 1;
     -- Corrupt both fields.
     INSERT INTO system.jobs(id, status, payload, progress) SELECT id+3, status, '\xaaaa'::BYTES, '\xaaaa'::BYTES FROM system.jobs ORDER BY id LIMIT 1;
     -- Test what happens with a NULL progress field (which is a valid value).
     INSERT INTO system.jobs(id, status, payload, progress) SELECT id+4, status, payload, NULL::BYTES FROM system.jobs ORDER BY id LIMIT 1;
     INSERT INTO system.jobs(id, status, payload, progress) SELECT id+5, status, '\xaaaa'::BYTES, NULL::BYTES FROM system.jobs ORDER BY id LIMIT 1;
	`); err != nil {
		t.Fatal(err)
	}

	// Extract the last 4 rows from the query.
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
	ctx := context.TODO()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	registry := s.JobRegistry().(*jobs.Registry)
	mockJob := jobs.Record{Details: jobspb.ImportDetails{}, Progress: jobspb.ImportProgress{}}
	done := make(chan struct{})
	defer close(done)
	jobs.RegisterConstructor(
		jobspb.TypeImport, func(_ *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return jobs.FakeResumer{
				OnResume: func() error {
					<-done
					return nil
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
		job, _, err := registry.StartJob(ctx, nil, mockJob)
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
					"Expected job id %d but got %d", job.ID(), out.id)
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
			job, _, err := registry.StartJob(ctx, nil, mockJob)
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
