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

// jobExpectation defines the information necessary to determine the validity of
// a job in the system.jobs table.
type jobExpectation struct {
	Job               jobs.JobRecord
	Type              string
	Before            time.Time
	FractionCompleted float32
	Error             string
}

// verifyJobRecord verifies that the jobExpectation matches the job record
// stored in the system.jobs table.
func verifyJobRecord(
	db *sqlutils.SQLRunner,
	kvDB *client.DB,
	ex sqlutil.InternalExecutor,
	jl *jobs.JobLogger,
	expectedStatus jobs.JobStatus,
	expected jobExpectation,
) error {
	testSource := func(source string) error {
		var actualJob jobs.JobRecord
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

		// Verify a newly-instantiated JobLogger's properties.
		fetched, err := jobs.GetJobLogger(context.TODO(), kvDB, ex, *jl.JobID())
		if err != nil {
			return err
		}
		if e, a := jl.Payload(), fetched.Payload(); !reflect.DeepEqual(e, a) {
			diff := strings.Join(pretty.Diff(e, a), "\n")
			return errors.Errorf("Job Payloads do not match:\n%s", diff)
		}

		// Verify the upstream-provided fields.
		actualJob.Details = fetched.Job.Details
		for _, id := range rawDescriptorIDs {
			actualJob.DescriptorIDs = append(actualJob.DescriptorIDs, sqlbase.ID(id))
		}
		if e, a := expected.Job, actualJob; !reflect.DeepEqual(e, a) {
			diff := strings.Join(pretty.Diff(e, a), "\n")
			return errors.Errorf("JobRecords do not match:\n%s", diff)
		}

		// Verify JobLogger-managed fields.
		status := jobs.JobStatus(statusString)
		if e, a := expectedStatus, status; e != a {
			return errors.Errorf("expected status %v, got %v", e, a)
		}
		if e, a := expected.Type, typ; e != a {
			return errors.Errorf("expected type %v, got type %v", e, a)
		}
		if e, a := expected.FractionCompleted, fractionCompleted; e != a {
			return errors.Errorf("expected fraction completed %f, got %f", e, a)
		}

		// Check JobLogger-managed timestamps for sanity.
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
		if status == jobs.JobStatusPending {
			return verifyModifiedAgainst("created", created.Time)
		}

		if !started.Valid && status == jobs.JobStatusSucceeded {
			return errors.Errorf("started time is NULL but job claims to be successful")
		}
		if started.Valid && created.Time.After(started.Time) {
			return errors.Errorf("created time %v is after started time %v", created, started)
		}
		if status == jobs.JobStatusRunning {
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
		woodyJob := jobs.JobRecord{
			Description:   "There's a snake in my boot!",
			Username:      "Woody Pride",
			DescriptorIDs: []sqlbase.ID{1, 2, 3},
			Details:       jobs.RestoreJobDetails{},
		}
		woodyExpectation := jobExpectation{
			Job:    woodyJob,
			Type:   jobs.JobTypeRestore,
			Before: timeutil.Now(),
		}
		woodyLogger := jobs.NewJobLogger(kvDB, executor, woodyJob)

		if err := woodyLogger.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, kvDB, executor, woodyLogger, jobs.JobStatusPending, woodyExpectation); err != nil {
			t.Fatal(err)
		}

		if err := woodyLogger.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, kvDB, executor, woodyLogger, jobs.JobStatusRunning, woodyExpectation); err != nil {
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
			if err := woodyLogger.Progressed(ctx, f.actual, jobs.Noop); err != nil {
				t.Fatal(err)
			}
			woodyExpectation.FractionCompleted = f.expected
			if err := verifyJobRecord(db, kvDB, executor, woodyLogger, jobs.JobStatusRunning, woodyExpectation); err != nil {
				t.Fatal(err)
			}
		}

		// Test Progressed callbacks.
		if err := woodyLogger.Progressed(ctx, 1.0, func(_ context.Context, details interface{}) {
			details.(*jobs.JobPayload_Restore).Restore.LowWaterMark = roachpb.Key("mariana")
		}); err != nil {
			t.Fatal(err)
		}
		woodyExpectation.Job.Details = jobs.RestoreJobDetails{LowWaterMark: roachpb.Key("mariana")}
		if err := verifyJobRecord(db, kvDB, executor, woodyLogger, jobs.JobStatusRunning, woodyExpectation); err != nil {
			t.Fatal(err)
		}

		if err := woodyLogger.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, kvDB, executor, woodyLogger, jobs.JobStatusSucceeded, woodyExpectation); err != nil {
			t.Fatal(err)
		}

		// Buzz fails after it starts running.
		buzzJob := jobs.JobRecord{
			Description:   "To infinity and beyond!",
			Username:      "Buzz Lightyear",
			DescriptorIDs: []sqlbase.ID{3, 2, 1},
		}
		buzzExpectation := jobExpectation{
			Job:    buzzJob,
			Type:   jobs.JobTypeBackup,
			Before: timeutil.Now(),
			Error:  "Buzz Lightyear can't fly",
		}
		buzzLogger := jobs.NewJobLogger(kvDB, executor, buzzJob)

		// Test modifying the job details before calling `Created`.
		buzzLogger.Job.Details = jobs.BackupJobDetails{}
		buzzExpectation.Job.Details = jobs.BackupJobDetails{}
		if err := buzzLogger.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, kvDB, executor, buzzLogger, jobs.JobStatusPending, buzzExpectation); err != nil {
			t.Fatal(err)
		}

		if err := buzzLogger.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, kvDB, executor, buzzLogger, jobs.JobStatusRunning, buzzExpectation); err != nil {
			t.Fatal(err)
		}

		if err := buzzLogger.Progressed(ctx, .42, jobs.Noop); err != nil {
			t.Fatal(err)
		}
		buzzExpectation.FractionCompleted = .42
		if err := verifyJobRecord(db, kvDB, executor, buzzLogger, jobs.JobStatusRunning, buzzExpectation); err != nil {
			t.Fatal(err)
		}

		buzzLogger.Failed(ctx, errors.New("Buzz Lightyear can't fly"))
		if err := verifyJobRecord(db, kvDB, executor, buzzLogger, jobs.JobStatusFailed, buzzExpectation); err != nil {
			t.Fatal(err)
		}

		// Ensure that logging Buzz didn't corrupt Woody.
		if err := verifyJobRecord(db, kvDB, executor, woodyLogger, jobs.JobStatusSucceeded, woodyExpectation); err != nil {
			t.Fatal(err)
		}

		// Sid fails before it starts running.
		sidJob := jobs.JobRecord{
			Description:   "The toys! The toys are alive!",
			Username:      "Sid Phillips",
			DescriptorIDs: []sqlbase.ID{6, 6, 6},
			Details:       jobs.RestoreJobDetails{},
		}
		sidExpectation := jobExpectation{
			Job:    sidJob,
			Type:   jobs.JobTypeRestore,
			Before: timeutil.Now(),
			Error:  "Sid is a total failure",
		}
		sidLogger := jobs.NewJobLogger(kvDB, executor, sidJob)

		if err := sidLogger.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, kvDB, executor, sidLogger, jobs.JobStatusPending, sidExpectation); err != nil {
			t.Fatal(err)
		}

		sidLogger.Failed(ctx, errors.New("Sid is a total failure"))
		if err := verifyJobRecord(db, kvDB, executor, sidLogger, jobs.JobStatusFailed, sidExpectation); err != nil {
			t.Fatal(err)
		}

		// Ensure that logging Sid didn't corrupt Woody or Buzz.
		if err := verifyJobRecord(db, kvDB, executor, woodyLogger, jobs.JobStatusSucceeded, woodyExpectation); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, kvDB, executor, buzzLogger, jobs.JobStatusFailed, buzzExpectation); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("bad job details fail", func(t *testing.T) {
		logger := jobs.NewJobLogger(kvDB, executor, jobs.JobRecord{
			Details: 42,
		})
		if err := logger.Created(ctx); !testutils.IsError(err, "unsupported job details type int") {
			t.Fatalf("expected 'unsupported job details type int', but got %v", err)
		}
	})

	t.Run("update before create fails", func(t *testing.T) {
		logger := jobs.NewJobLogger(kvDB, executor, jobs.JobRecord{})
		if err := logger.Started(ctx); !testutils.IsError(err, "job not created") {
			t.Fatalf("expected 'job not created' error, but got %v", err)
		}
	})

	t.Run("same state transition twice succeeds silently", func(t *testing.T) {
		logger := jobs.NewJobLogger(kvDB, executor, jobs.JobRecord{
			Details: jobs.BackupJobDetails{},
		})
		if err := logger.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("out of bounds progress fails", func(t *testing.T) {
		logger := jobs.NewJobLogger(kvDB, executor, jobs.JobRecord{
			Details: jobs.BackupJobDetails{},
		})
		if err := logger.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Progressed(ctx, -0.1, jobs.Noop); !testutils.IsError(err, "outside allowable range") {
			t.Fatalf("expected 'outside allowable range' error, but got %v", err)
		}
		if err := logger.Progressed(ctx, 1.1, jobs.Noop); !testutils.IsError(err, "outside allowable range") {
			t.Fatalf("expected 'outside allowable range' error, but got %v", err)
		}
	})

	t.Run("progress on non-started job fails", func(t *testing.T) {
		logger := jobs.NewJobLogger(kvDB, executor, jobs.JobRecord{
			Details: jobs.BackupJobDetails{},
		})
		if err := logger.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Progressed(ctx, 0.5, jobs.Noop); !testutils.IsError(err, `job \d+ not started`) {
			t.Fatalf("expected 'job not started' error, but got %v", err)
		}
	})

	t.Run("progress on finished job fails", func(t *testing.T) {
		logger := jobs.NewJobLogger(kvDB, executor, jobs.JobRecord{
			Details: jobs.BackupJobDetails{},
		})
		if err := logger.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Progressed(ctx, 0.5, jobs.Noop); !testutils.IsError(err, `job \d+ already finished`) {
			t.Fatalf("expected 'job already finished' error, but got %v", err)
		}
	})

	t.Run("succeeded forces fraction completed to 1.0", func(t *testing.T) {
		db := sqlutils.MakeSQLRunner(t, rawSQLDB)
		job := jobs.JobRecord{Details: jobs.BackupJobDetails{}}
		expectation := jobExpectation{
			Job:               job,
			Type:              jobs.JobTypeBackup,
			Before:            timeutil.Now(),
			FractionCompleted: 1.0,
		}
		logger := jobs.NewJobLogger(kvDB, executor, job)
		if err := logger.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Progressed(ctx, 0.2, jobs.Noop); err != nil {
			t.Fatal(err)
		}
		if err := logger.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, kvDB, executor, logger, jobs.JobStatusSucceeded, expectation); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("set details works", func(t *testing.T) {
		db := sqlutils.MakeSQLRunner(t, rawSQLDB)
		job := jobs.JobRecord{Details: jobs.SchemaChangeJobDetails{}}
		expectation := jobExpectation{
			Job:               job,
			Type:              jobs.JobTypeSchemaChange,
			Before:            timeutil.Now(),
			FractionCompleted: 1.0,
		}
		newDetails := jobs.SchemaChangeJobDetails{ReadAsOf: 1000}
		expectation.Job.Details = newDetails
		logger := jobs.NewJobLogger(kvDB, executor, job)
		if err := logger.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.SetDetails(ctx, newDetails); err != nil {
			t.Fatal(err)
		}
		if err := logger.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, kvDB, executor, logger, jobs.JobStatusSucceeded, expectation); err != nil {
			t.Fatal(err)
		}
	})
}
