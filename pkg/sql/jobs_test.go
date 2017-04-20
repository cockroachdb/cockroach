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

package sql_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	Job               sql.JobRecord
	Type              string
	Before            time.Time
	FractionCompleted float32
	Error             string
}

// verifyJobRecord verifies that the jobExpectation matches the job record
// stored in the system.jobs table.
func verifyJobRecord(
	db *sqlutils.SQLRunner, expectedStatus sql.JobStatus, expected jobExpectation,
) error {
	var actualJob sql.JobRecord
	var typ string
	var rawDescriptorIDs pq.Int64Array
	var statusString string
	var created pq.NullTime
	var started pq.NullTime
	var finished pq.NullTime
	var modified pq.NullTime
	var fractionCompleted float32
	var err string
	// We have to query for the nth job created rather than filtering by ID,
	// because job-generating SQL queries (e.g. BACKUP) do not currently return
	// the job ID.
	db.QueryRow(`
		SELECT type, description, username, descriptor_ids, status,
			   created, started, finished, modified, fraction_completed, error
		FROM crdb_internal.jobs WHERE created >= $1 ORDER BY created LIMIT 1`,
		expected.Before,
	).Scan(
		&typ, &actualJob.Description, &actualJob.Username, &rawDescriptorIDs, &statusString,
		&created, &started, &finished, &modified, &fractionCompleted, &err,
	)

	// Verify the upstream-provided fields.
	expected.Job.Details = nil
	for _, id := range rawDescriptorIDs {
		actualJob.DescriptorIDs = append(actualJob.DescriptorIDs, sqlbase.ID(id))
	}
	if e, a := expected.Job, actualJob; !reflect.DeepEqual(e, a) {
		diff := strings.Join(pretty.Diff(e, a), "\n")
		return errors.Errorf("JobRecords do not match:\n%s", diff)
	}

	// Verify JobLogger-managed fields.
	status := sql.JobStatus(statusString)
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
	if status == sql.JobStatusPending {
		return verifyModifiedAgainst("created", created.Time)
	}

	if !started.Valid && status == sql.JobStatusSucceeded {
		return errors.Errorf("started time is NULL but job claims to be successful")
	}
	if started.Valid && created.Time.After(started.Time) {
		return errors.Errorf("created time %v is after started time %v", created, started)
	}
	if status == sql.JobStatusRunning {
		return verifyModifiedAgainst("started", started.Time)
	}

	if started.Time.After(finished.Time) {
		return errors.Errorf("started time %v is after finished time %v", started, finished)
	}
	if e, a := expected.Error, err; e != a {
		return errors.Errorf("expected error %v, got %v", e, a)
	}
	return verifyModifiedAgainst("finished", finished.Time)
}

func TestJobLogger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.TODO()
	params, _ := createTestServerParams()
	s, rawSQLDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	t.Run("valid job lifecycles succeed", func(t *testing.T) {
		db := sqlutils.MakeSQLRunner(t, rawSQLDB)

		// Woody is a successful job.
		woodyJob := sql.JobRecord{
			Description:   "There's a snake in my boot!",
			Username:      "Woody Pride",
			DescriptorIDs: []sqlbase.ID{1, 2, 3},
			Details:       sql.BackupJobDetails{},
		}
		woodyExpectation := jobExpectation{
			Job:    woodyJob,
			Type:   sql.JobTypeBackup,
			Before: timeutil.Now(),
		}
		woodyLogger := sql.NewJobLogger(kvDB, s.LeaseManager().(*sql.LeaseManager), woodyJob)

		if err := woodyLogger.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, sql.JobStatusPending, woodyExpectation); err != nil {
			t.Fatal(err)
		}

		if err := woodyLogger.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, sql.JobStatusRunning, woodyExpectation); err != nil {
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
			if err := woodyLogger.Progressed(ctx, f.actual); err != nil {
				t.Fatal(err)
			}
			woodyExpectation.FractionCompleted = f.expected
			if err := verifyJobRecord(db, sql.JobStatusRunning, woodyExpectation); err != nil {
				t.Fatal(err)
			}
		}

		if err := woodyLogger.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, sql.JobStatusSucceeded, woodyExpectation); err != nil {
			t.Fatal(err)
		}

		// Buzz fails after it starts running.
		buzzJob := sql.JobRecord{
			Description:   "To infinity and beyond!",
			Username:      "Buzz Lightyear",
			DescriptorIDs: []sqlbase.ID{3, 2, 1},
		}
		buzzExpectation := jobExpectation{
			Job:    buzzJob,
			Type:   sql.JobTypeRestore,
			Before: timeutil.Now(),
			Error:  "Buzz Lightyear can't fly",
		}
		buzzLogger := sql.NewJobLogger(kvDB, s.LeaseManager().(*sql.LeaseManager), buzzJob)

		// Test modifying the job details before calling `Created`.
		buzzLogger.Job.Details = sql.RestoreJobDetails{}
		if err := buzzLogger.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, sql.JobStatusPending, buzzExpectation); err != nil {
			t.Fatal(err)
		}

		if err := buzzLogger.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, sql.JobStatusRunning, buzzExpectation); err != nil {
			t.Fatal(err)
		}

		if err := buzzLogger.Progressed(ctx, .42); err != nil {
			t.Fatal(err)
		}
		buzzExpectation.FractionCompleted = .42
		if err := verifyJobRecord(db, sql.JobStatusRunning, buzzExpectation); err != nil {
			t.Fatal(err)
		}

		buzzLogger.Failed(ctx, errors.New("Buzz Lightyear can't fly"))
		if err := verifyJobRecord(db, sql.JobStatusFailed, buzzExpectation); err != nil {
			t.Fatal(err)
		}

		// Ensure that logging Buzz didn't corrupt Woody.
		if err := verifyJobRecord(db, sql.JobStatusSucceeded, woodyExpectation); err != nil {
			t.Fatal(err)
		}

		// Sid fails before it starts running.
		sidJob := sql.JobRecord{
			Description:   "The toys! The toys are alive!",
			Username:      "Sid Phillips",
			DescriptorIDs: []sqlbase.ID{6, 6, 6},
			Details:       sql.RestoreJobDetails{},
		}
		sidExpectation := jobExpectation{
			Job:    sidJob,
			Type:   sql.JobTypeRestore,
			Before: timeutil.Now(),
			Error:  "Sid is a total failure",
		}
		sidLogger := sql.NewJobLogger(kvDB, s.LeaseManager().(*sql.LeaseManager), sidJob)

		if err := sidLogger.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, sql.JobStatusPending, sidExpectation); err != nil {
			t.Fatal(err)
		}

		sidLogger.Failed(ctx, errors.New("Sid is a total failure"))
		if err := verifyJobRecord(db, sql.JobStatusFailed, sidExpectation); err != nil {
			t.Fatal(err)
		}

		// Ensure that logging Sid didn't corrupt Woody or Buzz.
		if err := verifyJobRecord(db, sql.JobStatusSucceeded, woodyExpectation); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, sql.JobStatusFailed, buzzExpectation); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("bad job details fail", func(t *testing.T) {
		logger := sql.NewJobLogger(kvDB, s.LeaseManager().(*sql.LeaseManager), sql.JobRecord{
			Details: 42,
		})
		if err := logger.Created(ctx); !testutils.IsError(err, "unsupported job details type int") {
			t.Fatalf("expected 'unsupported job details type int', but got %v", err)
		}
	})

	t.Run("update before create fails", func(t *testing.T) {
		logger := sql.NewJobLogger(kvDB, s.LeaseManager().(*sql.LeaseManager), sql.JobRecord{})
		if err := logger.Started(ctx); !testutils.IsError(err, "job not created") {
			t.Fatalf("expected 'job not created' error, but got %v", err)
		}
	})

	t.Run("same state transition twice fails", func(t *testing.T) {
		logger := sql.NewJobLogger(kvDB, s.LeaseManager().(*sql.LeaseManager), sql.JobRecord{
			Details: sql.BackupJobDetails{},
		})
		if err := logger.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Created(ctx); !testutils.IsError(err, `job \d+ already created`) {
			t.Fatalf("expected 'job already created' error, but got %v", err)
		}
		if err := logger.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Started(ctx); !testutils.IsError(err, `job \d+ already started`) {
			t.Fatalf("expected 'job already started' error, but got %v", err)
		}
		if err := logger.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Succeeded(ctx); !testutils.IsError(err, `job \d+ already finished`) {
			t.Fatalf("expected 'job already finished' error, but got %v", err)
		}
	})

	t.Run("out of bounds progress fails", func(t *testing.T) {
		logger := sql.NewJobLogger(kvDB, s.LeaseManager().(*sql.LeaseManager), sql.JobRecord{
			Details: sql.BackupJobDetails{},
		})
		if err := logger.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Progressed(ctx, -0.1); !testutils.IsError(err, "outside allowable range") {
			t.Fatalf("expected 'outside allowable range' error, but got %v", err)
		}
		if err := logger.Progressed(ctx, 1.1); !testutils.IsError(err, "outside allowable range") {
			t.Fatalf("expected 'outside allowable range' error, but got %v", err)
		}
	})

	t.Run("progress on non-started job fails", func(t *testing.T) {
		logger := sql.NewJobLogger(kvDB, s.LeaseManager().(*sql.LeaseManager), sql.JobRecord{
			Details: sql.BackupJobDetails{},
		})
		if err := logger.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Progressed(ctx, 0.5); !testutils.IsError(err, `job \d+ not started`) {
			t.Fatalf("expected 'job not started' error, but got %v", err)
		}
	})

	t.Run("progress on finished job fails", func(t *testing.T) {
		logger := sql.NewJobLogger(kvDB, s.LeaseManager().(*sql.LeaseManager), sql.JobRecord{
			Details: sql.BackupJobDetails{},
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
		if err := logger.Progressed(ctx, 0.5); !testutils.IsError(err, `job \d+ already finished`) {
			t.Fatalf("expected 'job already finished' error, but got %v", err)
		}
	})

	t.Run("succeeded forces fraction completed to 1.0", func(t *testing.T) {
		db := sqlutils.MakeSQLRunner(t, rawSQLDB)
		job := sql.JobRecord{Details: sql.BackupJobDetails{}}
		expectation := jobExpectation{
			Job:               job,
			Type:              sql.JobTypeBackup,
			Before:            timeutil.Now(),
			FractionCompleted: 1.0,
		}
		logger := sql.NewJobLogger(kvDB, s.LeaseManager().(*sql.LeaseManager), job)
		if err := logger.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Progressed(ctx, 0.2); err != nil {
			t.Fatal(err)
		}
		if err := logger.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := verifyJobRecord(db, sql.JobStatusSucceeded, expectation); err != nil {
			t.Fatal(err)
		}
	})
}
