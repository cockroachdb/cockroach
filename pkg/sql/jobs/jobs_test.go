// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package jobs_test

import (
	gosql "database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/kr/pretty"
)

// expectation defines the information necessary to determine the validity of
// a job in the system.jobs table.
type expectation struct {
	DB                *gosql.DB
	Record            jobs.Record
	Type              jobs.Type
	Before            time.Time
	FractionCompleted float32
	Error             string
}

func (expected *expectation) verify(id *int64, expectedStatus jobs.Status) error {
	var statusString string
	var created time.Time
	var payloadBytes []byte
	if err := expected.DB.QueryRow(
		`SELECT status, created, payload FROM system.jobs WHERE id = $1`, id,
	).Scan(
		&statusString, &created, &payloadBytes,
	); err != nil {
		return err
	}

	var payload jobs.Payload
	if err := payload.Unmarshal(payloadBytes); err != nil {
		return err
	}

	// Verify the upstream-provided fields.
	details, err := payload.UnwrapDetails()
	if err != nil {
		return err
	}
	if e, a := expected.Record, (jobs.Record{
		Description:   payload.Description,
		Details:       details,
		DescriptorIDs: payload.DescriptorIDs,
		Username:      payload.Username,
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
	if e, a := expected.FractionCompleted, payload.FractionCompleted; e != a {
		return errors.Errorf("expected fraction completed %f, got %f", e, a)
	}

	// Check internally-managed timestamps for sanity.
	started := timeutil.FromUnixMicros(payload.StartedMicros)
	modified := timeutil.FromUnixMicros(payload.ModifiedMicros)
	finished := timeutil.FromUnixMicros(payload.FinishedMicros)

	verifyModifiedAgainst := func(name string, ts time.Time) error {
		if modified.Before(ts) {
			return errors.Errorf("modified time %v before %s time %v", modified, name, ts)
		}
		if now := timeutil.Now().Round(time.Microsecond); modified.After(now) {
			return errors.Errorf("modified time %v after current time %v", modified, now)
		}
		return nil
	}

	if expected.Before.After(created) {
		return errors.Errorf(
			"created time %v is before expected created time %v",
			created, expected.Before,
		)
	}
	if status == jobs.StatusPending {
		return verifyModifiedAgainst("created", created)
	}

	if started.Equal(timeutil.UnixEpoch) && status == jobs.StatusSucceeded {
		return errors.Errorf("started time is empty but job claims to be successful")
	}
	if !started.Equal(timeutil.UnixEpoch) && created.After(started) {
		return errors.Errorf("created time %v is after started time %v", created, started)
	}
	if status == jobs.StatusRunning {
		return verifyModifiedAgainst("started", started)
	}

	if started.After(finished) {
		return errors.Errorf("started time %v is after finished time %v", started, finished)
	}
	if e, a := expected.Error, payload.Error; e != a {
		return errors.Errorf("expected error %v, got %v", e, a)
	}
	return verifyModifiedAgainst("finished", finished)
}

func TestJobLifecycle(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	registry := s.JobRegistry().(*jobs.Registry)

	createJob := func(typ jobs.Type, cancelFn func(), record jobs.Record) (*jobs.Job, expectation) {
		beforeTime := timeutil.Now()
		job := registry.NewJob(record)
		if err := job.Created(ctx, cancelFn); err != nil {
			t.Fatal(err)
		}
		return job, expectation{
			DB:     sqlDB,
			Record: record,
			Type:   typ,
			Before: beforeTime,
		}
	}

	createDefaultJob := func(cancelFn func()) (*jobs.Job, expectation) {
		return createJob(jobs.TypeBackup, cancelFn, jobs.Record{
			// Job does not accept an empty Details field, so arbitrarily provide
			// BackupDetails.
			Details: jobs.BackupDetails{},
		})
	}

	t.Run("valid job lifecycles succeed", func(t *testing.T) {
		// Woody is a successful job.
		woodyJob, woodyExp := createJob(jobs.TypeRestore, jobs.WithoutCancel, jobs.Record{
			Description:   "There's a snake in my boot!",
			Username:      "Woody Pride",
			DescriptorIDs: []sqlbase.ID{1, 2, 3},
			Details:       jobs.RestoreDetails{},
		})

		if err := woodyJob.Created(ctx, jobs.WithoutCancel); err != nil {
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
		// is silently ignored.
		progresses := []struct {
			actual   float32
			expected float32
		}{
			{0.0, 0.0}, {0.5, 0.5}, {0.5, 0.5}, {0.4, 0.5}, {0.8, 0.8}, {1.0, 1.0},
		}
		for _, f := range progresses {
			if err := woodyJob.Progressed(ctx, f.actual, jobs.Noop); err != nil {
				t.Fatal(err)
			}
			woodyExp.FractionCompleted = f.expected
			if err := woodyExp.verify(woodyJob.ID(), jobs.StatusRunning); err != nil {
				t.Fatal(err)
			}
		}

		// Test Progressed callbacks.
		if err := woodyJob.Progressed(ctx, 1.0, func(_ context.Context, details interface{}) {
			details.(*jobs.Payload_Restore).Restore.LowWaterMark = roachpb.Key("mariana")
		}); err != nil {
			t.Fatal(err)
		}
		woodyExp.Record.Details = jobs.RestoreDetails{LowWaterMark: roachpb.Key("mariana")}
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
		}
		buzzExp := expectation{
			DB:     sqlDB,
			Record: buzzRecord,
			Type:   jobs.TypeBackup,
			Before: timeutil.Now(),
			Error:  "Buzz Lightyear can't fly",
		}
		buzzJob := registry.NewJob(buzzRecord)

		// Test modifying the job details before calling `Created`.
		buzzJob.Record.Details = jobs.BackupDetails{}
		buzzExp.Record.Details = jobs.BackupDetails{}
		if err := buzzJob.Created(ctx, jobs.WithoutCancel); err != nil {
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

		if err := buzzJob.Progressed(ctx, .42, jobs.Noop); err != nil {
			t.Fatal(err)
		}
		buzzExp.FractionCompleted = .42
		if err := buzzExp.verify(buzzJob.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}

		buzzJob.Failed(ctx, errors.New("Buzz Lightyear can't fly"))
		if err := buzzExp.verify(buzzJob.ID(), jobs.StatusFailed); err != nil {
			t.Fatal(err)
		}

		// Ensure that logging Buzz didn't corrupt Woody.
		if err := woodyExp.verify(woodyJob.ID(), jobs.StatusSucceeded); err != nil {
			t.Fatal(err)
		}

		// Sid fails before it starts running.
		sidJob, sidExp := createJob(jobs.TypeRestore, jobs.WithoutCancel, jobs.Record{
			Description:   "The toys! The toys are alive!",
			Username:      "Sid Phillips",
			DescriptorIDs: []sqlbase.ID{6, 6, 6},
			Details:       jobs.RestoreDetails{},
		})

		if err := sidJob.Created(ctx, jobs.WithoutCancel); err != nil {
			t.Fatal(err)
		}
		if err := sidExp.verify(sidJob.ID(), jobs.StatusPending); err != nil {
			t.Fatal(err)
		}

		sidJob.Failed(ctx, errors.New("Sid is a total failure"))
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
			job, exp := createDefaultJob(jobs.WithoutCancel)
			exp.FractionCompleted = 1.0
			if err := job.Started(ctx); err != nil {
				t.Fatal(err)
			}
			if err := job.FinishedWith(ctx, nil); err != nil {
				t.Fatal(err)
			}
			if err := exp.verify(job.ID(), jobs.StatusSucceeded); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("non-nil error marks job as failed", func(t *testing.T) {
			job, exp := createDefaultJob(jobs.WithoutCancel)
			exp.Error = "boom"
			if err := job.FinishedWith(ctx, errors.New(exp.Error)); err != nil {
				t.Fatal(err)
			}
			if err := exp.verify(job.ID(), jobs.StatusFailed); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("internal errors are swallowed if marking job as successful", func(t *testing.T) {
			job, _ := createDefaultJob(jobs.WithoutCancel)
			if _, err := sqlDB.Exec(
				`UPDATE system.jobs SET payload = 'garbage' WHERE id = $1`, *job.ID(),
			); err != nil {
				t.Fatal(err)
			}
			if err := job.FinishedWith(ctx, nil); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("internal errors are swallowed if marking job as failed", func(t *testing.T) {
			job, _ := createDefaultJob(jobs.WithoutCancel)
			if _, err := sqlDB.Exec(
				`UPDATE system.jobs SET payload = 'garbage' WHERE id = $1`, *job.ID(),
			); err != nil {
				t.Fatal(err)
			}
			if err := job.FinishedWith(ctx, errors.New("boom")); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("progress on paused job errors yields user-friendly message", func(t *testing.T) {
			job, _ := createDefaultJob(func() {})
			if err := job.Paused(ctx); err != nil {
				t.Fatal(err)
			}
			err := job.Progressed(ctx, 0.42, jobs.Noop)
			if err := job.FinishedWith(ctx, err); !testutils.IsError(err, "job paused") {
				t.Fatalf("expected 'job paused' error, but got %v", err)
			}
		})

		t.Run("progress on canceled job errors yields user-friendly message", func(t *testing.T) {
			job, _ := createDefaultJob(func() {})
			if err := job.Canceled(ctx); err != nil {
				t.Fatal(err)
			}
			err := job.Progressed(ctx, 0.42, jobs.Noop)
			if err := job.FinishedWith(ctx, err); !testutils.IsError(err, "job canceled") {
				t.Fatalf("expected 'job paused' error, but got %v", err)
			}
		})
	})

	t.Run("uncancelable jobs cannot be paused or canceled", func(t *testing.T) {
		{
			job, _ := createDefaultJob(jobs.WithoutCancel)
			if err := job.Paused(ctx); !testutils.IsError(
				err, `job created by node without PAUSE support`,
			) {
				t.Fatalf("expected 'job created by node without PAUSE support' error, but got %s", err)
			}
			if err := job.Canceled(ctx); !testutils.IsError(
				err, `job created by node without CANCEL support`,
			) {
				t.Fatalf("expected 'job created by node without CANCEL support' error, but got %s", err)
			}
		}

		testCases := []struct {
			typ     jobs.Type
			details jobs.Details
			name    string
		}{
			{jobs.TypeSchemaChange, jobs.SchemaChangeDetails{}, "schema change"},
			{jobs.TypeImport, jobs.ImportDetails{}, "import"},
		}
		for _, tc := range testCases {
			job, _ := createJob(tc.typ, jobs.WithoutCancel, jobs.Record{
				Details: tc.details,
			})
			pauseErr := fmt.Sprintf("%s jobs do not support PAUSE", tc.name)
			if err := job.Paused(ctx); !testutils.IsError(err, pauseErr) {
				t.Fatalf("expected '%s' error, but got %s", pauseErr, err)
			}
			cancelErr := fmt.Sprintf("%s jobs do not support CANCEL", tc.name)
			if err := job.Canceled(ctx); !testutils.IsError(err, cancelErr) {
				t.Fatalf("expected '%s' error, but got %s", cancelErr, err)
			}
		}
	})

	t.Run("cancelable jobs can be paused until finished", func(t *testing.T) {
		job, exp := createDefaultJob(func() {})

		// Pause and resume succeeds while job is pending. Additionally verify that
		// the status is updated as expected.
		if err := job.Paused(ctx); err != nil {
			t.Fatal(err)
		}
		if err := exp.verify(job.ID(), jobs.StatusPaused); err != nil {
			t.Fatal(err)
		}
		if err := job.Resumed(ctx); err != nil {
			t.Fatal(err)
		}
		if err := exp.verify(job.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}

		// Pause and resume succeeds while job is running. Additionally verify that
		// pausing a paused job and resuming a resumed job silently succeeds.
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Paused(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Paused(ctx); err != nil {
			t.Fatal(err)
		}
		if err := exp.verify(job.ID(), jobs.StatusPaused); err != nil {
			t.Fatal(err)
		}
		if err := job.Resumed(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Resumed(ctx); err != nil {
			t.Fatal(err)
		}
		if err := exp.verify(job.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}

		// Pause fails after job is successful.
		if err := job.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Paused(ctx); !testutils.IsError(err, "cannot pause succeeded job") {
			t.Fatalf("expected 'cannot pause succeeded job', but got '%s'", err)
		}
	})

	t.Run("cancelable jobs can be canceled until finished", func(t *testing.T) {
		{
			job, exp := createDefaultJob(func() {})
			if err := job.Canceled(ctx); err != nil {
				t.Fatal(err)
			}
			if err := exp.verify(job.ID(), jobs.StatusCanceled); err != nil {
				t.Fatal(err)
			}
		}

		{
			job, exp := createDefaultJob(func() {})
			if err := job.Started(ctx); err != nil {
				t.Fatal(err)
			}
			if err := job.Canceled(ctx); err != nil {
				t.Fatal(err)
			}
			if err := exp.verify(job.ID(), jobs.StatusCanceled); err != nil {
				t.Fatal(err)
			}
		}

		{
			job, exp := createDefaultJob(func() {})
			if err := job.Paused(ctx); err != nil {
				t.Fatal(err)
			}
			if err := job.Canceled(ctx); err != nil {
				t.Fatal(err)
			}
			if err := exp.verify(job.ID(), jobs.StatusCanceled); err != nil {
				t.Fatal(err)
			}
		}

		{
			job, _ := createDefaultJob(func() {})
			if err := job.Succeeded(ctx); err != nil {
				t.Fatal(err)
			}
			expectedErr := "job with status succeeded cannot be canceled"
			if err := job.Canceled(ctx); !testutils.IsError(err, expectedErr) {
				t.Fatalf("expected '%s', but got '%s'", expectedErr, err)
			}
		}
	})

	t.Run("unpaused jobs cannot be resumed", func(t *testing.T) {
		checkResumeFails := func(job *jobs.Job, status jobs.Status) {
			expectedErr := fmt.Sprintf("job with status %s cannot be resumed", status)
			if err := job.Resumed(ctx); !testutils.IsError(err, expectedErr) {
				t.Errorf("expected '%s', but got '%v'", expectedErr, err)
			}
		}

		{
			job, _ := createDefaultJob(func() {})
			checkResumeFails(job, jobs.StatusPending)
		}

		{
			job, _ := createDefaultJob(func() {})
			if err := job.Canceled(ctx); err != nil {
				t.Fatal(err)
			}
			checkResumeFails(job, jobs.StatusCanceled)
		}

		{
			job, _ := createDefaultJob(func() {})
			if err := job.Succeeded(ctx); err != nil {
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
		_ = job.Created(ctx, jobs.WithoutCancel)
	})

	t.Run("update before create fails", func(t *testing.T) {
		job := registry.NewJob(jobs.Record{})
		if err := job.Started(ctx); !testutils.IsError(err, "job not created") {
			t.Fatalf("expected 'job not created' error, but got %v", err)
		}
	})

	t.Run("same state transition twice succeeds silently", func(t *testing.T) {
		job, _ := createDefaultJob(jobs.WithoutCancel)
		if err := job.Created(ctx, jobs.WithoutCancel); err != nil {
			t.Fatal(err)
		}
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
		job, _ := createDefaultJob(jobs.WithoutCancel)
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Progressed(ctx, -0.1, jobs.Noop); !testutils.IsError(err, "outside allowable range") {
			t.Fatalf("expected 'outside allowable range' error, but got %v", err)
		}
		if err := job.Progressed(ctx, 1.1, jobs.Noop); !testutils.IsError(err, "outside allowable range") {
			t.Fatalf("expected 'outside allowable range' error, but got %v", err)
		}
	})

	t.Run("progress on non-started job fails", func(t *testing.T) {
		job, _ := createDefaultJob(jobs.WithoutCancel)
		if err := job.Progressed(ctx, 0.5, jobs.Noop); !testutils.IsError(
			err, `cannot update progress on pending job \(id \d+\)`,
		) {
			t.Fatalf("expected 'cannot update progress' error, but got %v", err)
		}
	})

	t.Run("progress on finished job fails", func(t *testing.T) {
		job, _ := createDefaultJob(jobs.WithoutCancel)
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Progressed(ctx, 0.5, jobs.Noop); !testutils.IsError(
			err, `cannot update progress on succeeded job \(id \d+\)`,
		) {
			t.Fatalf("expected 'cannot update progress' error, but got %v", err)
		}
	})

	t.Run("progress on paused job fails", func(t *testing.T) {
		job, _ := createDefaultJob(func() {})
		if err := job.Paused(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Progressed(ctx, 0.5, jobs.Noop); !testutils.IsError(
			err, `cannot update progress on paused job \(id \d+\)`,
		) {
			t.Fatalf("expected progress error, but got %v", err)
		}
	})

	t.Run("progress on canceled job fails", func(t *testing.T) {
		job, _ := createDefaultJob(func() {})
		if err := job.Canceled(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Progressed(ctx, 0.5, jobs.Noop); !testutils.IsError(
			err, `cannot update progress on canceled job \(id \d+\)`,
		) {
			t.Fatalf("expected progress error, but got %v", err)
		}
	})

	t.Run("succeeded forces fraction completed to 1.0", func(t *testing.T) {
		job, exp := createDefaultJob(jobs.WithoutCancel)
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Progressed(ctx, 0.2, jobs.Noop); err != nil {
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
		job, exp := createJob(jobs.TypeRestore, jobs.WithoutCancel, jobs.Record{
			Details: jobs.RestoreDetails{},
		})
		if err := exp.verify(job.ID(), jobs.StatusPending); err != nil {
			t.Fatal(err)
		}
		newDetails := jobs.RestoreDetails{LowWaterMark: []byte{42}}
		exp.Record.Details = newDetails
		if err := job.SetDetails(ctx, newDetails); err != nil {
			t.Fatal(err)
		}
		if err := exp.verify(job.ID(), jobs.StatusPending); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRunAndWaitForTerminalState(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Intentionally share the server between subtests, so job records
	// accumulate over time.
	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	tests := []struct {
		name   string
		status jobs.Status
		err    string
		execFn func(context.Context) error
	}{
		{
			"non-job execFn",
			"", "no jobs found",
			func(_ context.Context) error { return nil },
		},
		{
			"pre-job error",
			"", "exec failed before job was created.*pre-job error",
			func(_ context.Context) error { return errors.New("pre-job error") },
		},
		{
			"job succeeded",
			jobs.StatusSucceeded, "",
			func(_ context.Context) error {
				registry := s.JobRegistry().(*jobs.Registry)
				job := registry.NewJob(jobs.Record{Details: jobs.BackupDetails{}})
				if err := job.Created(ctx, jobs.WithoutCancel); err != nil {
					return err
				}
				return job.Succeeded(ctx)
			},
		},
		{
			"job failed",
			jobs.StatusFailed, "in-job error",
			func(_ context.Context) error {
				registry := s.JobRegistry().(*jobs.Registry)
				job := registry.NewJob(jobs.Record{Details: jobs.BackupDetails{}})
				if err := job.Created(ctx, jobs.WithoutCancel); err != nil {
					return err
				}
				err := errors.New("in-job error")
				job.Failed(ctx, err)
				return err
			},
		},
		{
			"job lease transfer then succeeded",
			jobs.StatusSucceeded, "",
			func(ctx context.Context) error {
				var cancel func()
				ctx, cancel = context.WithCancel(ctx)

				registry := s.JobRegistry().(*jobs.Registry)
				job := registry.NewJob(jobs.Record{Details: jobs.BackupDetails{}})
				if err := job.Created(ctx, cancel); err != nil {
					return err
				}
				if err := job.Succeeded(ctx); err != nil {
					return err
				}
				return errors.New("lease transferred")
			},
		},
		{
			"job lease transfer then failed",
			jobs.StatusFailed, "in-job error",
			func(ctx context.Context) error {
				var cancel func()
				ctx, cancel = context.WithCancel(ctx)

				registry := s.JobRegistry().(*jobs.Registry)
				job := registry.NewJob(jobs.Record{Details: jobs.BackupDetails{}})
				if err := job.Created(ctx, cancel); err != nil {
					return err
				}
				job.Failed(ctx, errors.New("in-job error"))
				return errors.New("lease transferred")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, status, err := jobs.RunAndWaitForTerminalState(ctx, sqlDB, test.execFn)
			if !testutils.IsError(err, test.err) {
				t.Fatalf("got %v expected %v", err, test.err)
			}
			if status != test.status {
				t.Fatalf("got [%s] expected [%s]", status, test.status)
			}
		})
	}
}
