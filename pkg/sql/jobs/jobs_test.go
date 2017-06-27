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

	"fmt"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/kr/pretty"
	"github.com/lib/pq"
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
	kvDB *client.DB,
	ex sqlutil.InternalExecutor,
	job *jobs.Job,
	expectedStatus jobs.Status,
	expected expectation,
) error {
	testSource := func(source string) error {
		var actualJob jobs.Record
		var typ string
		var rawDescriptorIDs pq.Int64Array
		var statusString string
		var created pq.NullTime
		var started pq.NullTime
		var finished pq.NullTime
		var modified pq.NullTime
		var fractionCompleted float32
		var errMessage string
		// We have to query for the nth job created rather than filtering by ID,
		// because job-generating SQL queries (e.g. BACKUP) do not currently return
		// the job ID.
		db.QueryRow(fmt.Sprintf(
			`SELECT type, description, username, descriptor_ids, status,
			   created, started, finished, modified, fraction_completed, error
			FROM %s WHERE created >= $1 ORDER BY created LIMIT 1`, source),
			expected.Before,
		).Scan(
			&typ, &actualJob.Description, &actualJob.Username, &rawDescriptorIDs, &statusString,
			&created, &started, &finished, &modified, &fractionCompleted, &errMessage,
		)

		// Verify a newly-instantiated Record's properties.
		fetched, err := jobs.GetJob(context.TODO(), kvDB, ex, *job.ID())
		if err != nil {
			return err
		}
		if e, a := job.Payload(), fetched.Payload(); !reflect.DeepEqual(e, a) {
			diff := strings.Join(pretty.Diff(e, a), "\n")
			return errors.Errorf("Record Payloads do not match:\n%s", diff)
		}

		// Verify the upstream-provided fields.
		actualJob.Details = fetched.Record.Details
		for _, id := range rawDescriptorIDs {
			actualJob.DescriptorIDs = append(actualJob.DescriptorIDs, sqlbase.ID(id))
		}
		if e, a := expected.Record, actualJob; !reflect.DeepEqual(e, a) {
			diff := strings.Join(pretty.Diff(e, a), "\n")
			return errors.Errorf("JobRecords do not match:\n%s", diff)
		}

		// Verify Record-managed fields.
		status := jobs.Status(statusString)
		if e, a := expectedStatus, status; e != a {
			return errors.Errorf("expected status %v, got %v", e, a)
		}
		if e, a := expected.Type, typ; e != a {
			return errors.Errorf("expected type %v, got type %v", e, a)
		}
		if e, a := expected.FractionCompleted, fractionCompleted; e != a {
			return errors.Errorf("expected fraction completed %f, got %f", e, a)
		}

		// Check Record-managed timestamps for sanity.
		verifyModifiedAgainst := func(name string, ts time.Time) error {
			if modified.Time.Before(ts) {
				return errors.Errorf("modified time %v before %s time %v", modified, name, ts)
			}
			if now := timeutil.Now().Round(time.Microsecond); modified.Time.After(now) {
				return errors.Errorf("modified time %v after current time %v", modified, now)
			}
			return nil
		}

		if expected.Before.After(created.Time) {
			return errors.Errorf(
				"created time %v is before expected created time %v",
				created, expected.Before,
			)
		}
		if status == jobs.StatusPending {
			return verifyModifiedAgainst("created", created.Time)
		}

		if !started.Valid && status == jobs.StatusSucceeded {
			return errors.Errorf("started time is NULL but job claims to be successful")
		}
		if started.Valid && created.Time.After(started.Time) {
			return errors.Errorf("created time %v is after started time %v", created, started)
		}
		if status == jobs.StatusRunning {
			return verifyModifiedAgainst("started", started.Time)
		}

		if started.Time.After(finished.Time) {
			return errors.Errorf("started time %v is after finished time %v", started, finished)
		}
		if e, a := expected.Error, errMessage; e != a {
			return errors.Errorf("expected error %v, got %v", e, a)
		}
		return verifyModifiedAgainst("finished", finished.Time)
	}
	if err := testSource(`crdb_internal.jobs`); err != nil {
		return err
	}
	return testSource(`[SHOW JOBS]`)
}

func TestJobLogger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.TODO()
	s, rawSQLDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	executor := sql.InternalExecutor{LeaseManager: s.LeaseManager().(*sql.LeaseManager)}
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
		woodyJob := jobs.NewJob(kvDB, executor, woodyRecord)

		if err := woodyJob.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, kvDB, executor, woodyJob, jobs.StatusPending, woodyExpectation); err != nil {
			t.Fatal(err)
		}

		if err := woodyJob.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, kvDB, executor, woodyJob, jobs.StatusRunning, woodyExpectation); err != nil {
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
			if err := verifyJobRecord(db, kvDB, executor, woodyJob, jobs.StatusRunning, woodyExpectation); err != nil {
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
		if err := verifyJobRecord(db, kvDB, executor, woodyJob, jobs.StatusRunning, woodyExpectation); err != nil {
			t.Fatal(err)
		}

		if err := woodyJob.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, kvDB, executor, woodyJob, jobs.StatusSucceeded, woodyExpectation); err != nil {
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
		buzzJob := jobs.NewJob(kvDB, executor, buzzRecord)

		// Test modifying the job details before calling `Created`.
		buzzJob.Record.Details = jobs.BackupDetails{}
		buzzExpectation.Record.Details = jobs.BackupDetails{}
		if err := buzzJob.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, kvDB, executor, buzzJob, jobs.StatusPending, buzzExpectation); err != nil {
			t.Fatal(err)
		}

		if err := buzzJob.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, kvDB, executor, buzzJob, jobs.StatusRunning, buzzExpectation); err != nil {
			t.Fatal(err)
		}

		if err := buzzJob.Progressed(ctx, .42, jobs.Noop); err != nil {
			t.Fatal(err)
		}
		buzzExpectation.FractionCompleted = .42
		if err := verifyJobRecord(db, kvDB, executor, buzzJob, jobs.StatusRunning, buzzExpectation); err != nil {
			t.Fatal(err)
		}

		buzzJob.Failed(ctx, errors.New("Buzz Lightyear can't fly"))
		if err := verifyJobRecord(db, kvDB, executor, buzzJob, jobs.StatusFailed, buzzExpectation); err != nil {
			t.Fatal(err)
		}

		// Ensure that logging Buzz didn't corrupt Woody.
		if err := verifyJobRecord(db, kvDB, executor, woodyJob, jobs.StatusSucceeded, woodyExpectation); err != nil {
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
		sidJob := jobs.NewJob(kvDB, executor, sidRecord)

		if err := sidJob.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, kvDB, executor, sidJob, jobs.StatusPending, sidExpectation); err != nil {
			t.Fatal(err)
		}

		sidJob.Failed(ctx, errors.New("Sid is a total failure"))
		if err := verifyJobRecord(db, kvDB, executor, sidJob, jobs.StatusFailed, sidExpectation); err != nil {
			t.Fatal(err)
		}

		// Ensure that logging Sid didn't corrupt Woody or Buzz.
		if err := verifyJobRecord(db, kvDB, executor, woodyJob, jobs.StatusSucceeded, woodyExpectation); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, kvDB, executor, buzzJob, jobs.StatusFailed, buzzExpectation); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("bad job details fail", func(t *testing.T) {
		job := jobs.NewJob(kvDB, executor, jobs.Record{
			Details: 42,
		})
		if err := job.Created(ctx); !testutils.IsError(err, "unsupported job details type int") {
			t.Fatalf("expected 'unsupported job details type int', but got %v", err)
		}
	})

	t.Run("update before create fails", func(t *testing.T) {
		job := jobs.NewJob(kvDB, executor, jobs.Record{})
		if err := job.Started(ctx); !testutils.IsError(err, "job not created") {
			t.Fatalf("expected 'job not created' error, but got %v", err)
		}
	})

	t.Run("same state transition twice succeeds silently", func(t *testing.T) {
		job := jobs.NewJob(kvDB, executor, jobs.Record{
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
		job := jobs.NewJob(kvDB, executor, jobs.Record{
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
		job := jobs.NewJob(kvDB, executor, jobs.Record{
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
		job := jobs.NewJob(kvDB, executor, jobs.Record{
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
		job := jobs.NewJob(kvDB, executor, record)
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
		if err := verifyJobRecord(db, kvDB, executor, job, jobs.StatusSucceeded, expect); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("set details works", func(t *testing.T) {
		db := sqlutils.MakeSQLRunner(t, rawSQLDB)
		record := jobs.Record{Details: jobs.SchemaChangeDetails{}}
		expectation := expectation{
			Record:            record,
			Type:              jobs.TypeSchemaChange,
			Before:            timeutil.Now(),
			FractionCompleted: 1.0,
		}
		newDetails := jobs.SchemaChangeDetails{ReadAsOf: 1000}
		expectation.Record.Details = newDetails
		job := jobs.NewJob(kvDB, executor, record)
		if err := job.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.SetDetails(ctx, newDetails); err != nil {
			t.Fatal(err)
		}
		if err := job.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, kvDB, executor, job, jobs.StatusSucceeded, expectation); err != nil {
			t.Fatal(err)
		}
	})
}
