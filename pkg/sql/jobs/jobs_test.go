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
//
// Author: Nikhil Benesch (nikhil.benesch@gmail.com)

package jobs_test

import (
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
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/kr/pretty"
)

// expectation defines the information necessary to determine the validity of
// a job in the system.jobs table.
type expectation struct {
	Record            jobs.Record
	Type              string
	Before            time.Time
	FractionCompleted float32
	Error             string
}

// verifyJobRecord verifies that the expectation matches the job record
// stored in the system.jobs table.
func verifyJobRecord(
	db *sqlutils.SQLRunner,
	job *jobs.Job,
	expectedStatus jobs.Status,
	expected expectation,
) error {
	var statusString string
	var created time.Time
	var payloadBytes []byte
	db.QueryRow(
		`SELECT status, created, payload FROM system.jobs WHERE id = $1`, job.ID(),
	).Scan(&statusString, &created, &payloadBytes)

	var payload jobs.Payload
	payload.Unmarshal(payloadBytes)

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
	if e, a := expected.Type, payload.Typ(); e != a {
		return errors.Errorf("expected type %v, got type %v", e, a)
	}
	if e, a := expected.FractionCompleted, payload.FractionCompleted; e != a {
		return errors.Errorf("expected fraction completed %f, got %f", e, a)
	}

	// Check internally-managed timestamps for sanity.
	started := time.Unix(0, payload.StartedMicros*time.Microsecond.Nanoseconds())
	modified := time.Unix(0, payload.ModifiedMicros*time.Microsecond.Nanoseconds())
	finished := time.Unix(0, payload.FinishedMicros*time.Microsecond.Nanoseconds())

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

	if started == time.Unix(0, 0) && status == jobs.StatusSucceeded {
		return errors.Errorf("started time is empty but job claims to be successful")
	}
	if started != time.Unix(0, 0) && created.After(started) {
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

	s, rawSQLDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	registry := s.JobRegistry().(*jobs.Registry)

	t.Run("valid job lifecycles succeed", func(t *testing.T) {
		db := sqlutils.MakeSQLRunner(t, rawSQLDB)

		// Woody is a successful job.
		woodyRecord := jobs.Record{
			Description:   "There's a snake in my boot!",
			Username:      "Woody Pride",
			DescriptorIDs: []sqlbase.ID{1, 2, 3},
			Details:       jobs.RestoreDetails{},
		}
		woodyExpectation := expectation{
			Record: woodyRecord,
			Type:   jobs.TypeRestore,
			Before: timeutil.Now(),
		}
		woodyJob := registry.NewJob(woodyRecord)

		if err := woodyJob.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, woodyJob, jobs.StatusPending, woodyExpectation); err != nil {
			t.Fatal(err)
		}

		if err := woodyJob.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, woodyJob, jobs.StatusRunning, woodyExpectation); err != nil {
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
			woodyExpectation.FractionCompleted = f.expected
			if err := verifyJobRecord(db, woodyJob, jobs.StatusRunning, woodyExpectation); err != nil {
				t.Fatal(err)
			}
		}

		// Test Progressed callbacks.
		if err := woodyJob.Progressed(ctx, 1.0, func(_ context.Context, details interface{}) {
			details.(*jobs.Payload_Restore).Restore.LowWaterMark = roachpb.Key("mariana")
		}); err != nil {
			t.Fatal(err)
		}
		woodyExpectation.Record.Details = jobs.RestoreDetails{LowWaterMark: roachpb.Key("mariana")}
		if err := verifyJobRecord(db, woodyJob, jobs.StatusRunning, woodyExpectation); err != nil {
			t.Fatal(err)
		}

		if err := woodyJob.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, woodyJob, jobs.StatusSucceeded, woodyExpectation); err != nil {
			t.Fatal(err)
		}

		// Buzz fails after it starts running.
		buzzRecord := jobs.Record{
			Description:   "To infinity and beyond!",
			Username:      "Buzz Lightyear",
			DescriptorIDs: []sqlbase.ID{3, 2, 1},
		}
		buzzExpectation := expectation{
			Record: buzzRecord,
			Type:   jobs.TypeBackup,
			Before: timeutil.Now(),
			Error:  "Buzz Lightyear can't fly",
		}
		buzzJob := registry.NewJob(buzzRecord)

		// Test modifying the job details before calling `Created`.
		buzzJob.Record.Details = jobs.BackupDetails{}
		buzzExpectation.Record.Details = jobs.BackupDetails{}
		if err := buzzJob.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, buzzJob, jobs.StatusPending, buzzExpectation); err != nil {
			t.Fatal(err)
		}

		if err := buzzJob.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, buzzJob, jobs.StatusRunning, buzzExpectation); err != nil {
			t.Fatal(err)
		}

		if err := buzzJob.Progressed(ctx, .42, jobs.Noop); err != nil {
			t.Fatal(err)
		}
		buzzExpectation.FractionCompleted = .42
		if err := verifyJobRecord(db, buzzJob, jobs.StatusRunning, buzzExpectation); err != nil {
			t.Fatal(err)
		}

		buzzJob.Failed(ctx, errors.New("Buzz Lightyear can't fly"))
		if err := verifyJobRecord(db, buzzJob, jobs.StatusFailed, buzzExpectation); err != nil {
			t.Fatal(err)
		}

		// Ensure that logging Buzz didn't corrupt Woody.
		if err := verifyJobRecord(db, woodyJob, jobs.StatusSucceeded, woodyExpectation); err != nil {
			t.Fatal(err)
		}

		// Sid fails before it starts running.
		sidRecord := jobs.Record{
			Description:   "The toys! The toys are alive!",
			Username:      "Sid Phillips",
			DescriptorIDs: []sqlbase.ID{6, 6, 6},
			Details:       jobs.RestoreDetails{},
		}
		sidExpectation := expectation{
			Record: sidRecord,
			Type:   jobs.TypeRestore,
			Before: timeutil.Now(),
			Error:  "Sid is a total failure",
		}
		sidJob := registry.NewJob(sidRecord)

		if err := sidJob.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, sidJob, jobs.StatusPending, sidExpectation); err != nil {
			t.Fatal(err)
		}

		sidJob.Failed(ctx, errors.New("Sid is a total failure"))
		if err := verifyJobRecord(db, sidJob, jobs.StatusFailed, sidExpectation); err != nil {
			t.Fatal(err)
		}

		// Ensure that logging Sid didn't corrupt Woody or Buzz.
		if err := verifyJobRecord(db, woodyJob, jobs.StatusSucceeded, woodyExpectation); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, buzzJob, jobs.StatusFailed, buzzExpectation); err != nil {
			t.Fatal(err)
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
		job := registry.NewJob(jobs.Record{})
		if err := job.Started(ctx); !testutils.IsError(err, "job not created") {
			t.Fatalf("expected 'job not created' error, but got %v", err)
		}
	})

	t.Run("same state transition twice succeeds silently", func(t *testing.T) {
		job := registry.NewJob(jobs.Record{
			Details: jobs.BackupDetails{},
		})
		if err := job.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Created(ctx); err != nil {
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
		job := registry.NewJob(jobs.Record{
			Details: jobs.BackupDetails{},
		})
		if err := job.Created(ctx); err != nil {
			t.Fatal(err)
		}
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
		job := registry.NewJob(jobs.Record{
			Details: jobs.BackupDetails{},
		})
		if err := job.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Progressed(ctx, 0.5, jobs.Noop); !testutils.IsError(err, `job \d+ not started`) {
			t.Fatalf("expected 'job not started' error, but got %v", err)
		}
	})

	t.Run("progress on finished job fails", func(t *testing.T) {
		job := registry.NewJob(jobs.Record{
			Details: jobs.BackupDetails{},
		})
		if err := job.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Progressed(ctx, 0.5, jobs.Noop); !testutils.IsError(err, `job \d+ already finished`) {
			t.Fatalf("expected 'job already finished' error, but got %v", err)
		}
	})

	t.Run("succeeded forces fraction completed to 1.0", func(t *testing.T) {
		db := sqlutils.MakeSQLRunner(t, rawSQLDB)
		record := jobs.Record{Details: jobs.BackupDetails{}}
		expect := expectation{
			Record:            record,
			Type:              jobs.TypeBackup,
			Before:            timeutil.Now(),
			FractionCompleted: 1.0,
		}
		job := registry.NewJob(record)
		if err := job.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Progressed(ctx, 0.2, jobs.Noop); err != nil {
			t.Fatal(err)
		}
		if err := job.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, job, jobs.StatusSucceeded, expect); err != nil {
			t.Fatal(err)
		}
	})
}
