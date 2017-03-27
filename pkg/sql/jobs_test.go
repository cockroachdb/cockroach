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
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

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
	Offset int
	Job    sql.JobRecord
	Type   string
	Before time.Time
	Error  string
}

// verifyJobRecord verifies that the jobExpectation matches the job record
// stored in the system.jobs table.
func verifyJobRecord(
	t *testing.T, db *sqlutils.SQLRunner, expectedStatus sql.JobStatus, expected jobExpectation,
) {
	var actualJob sql.JobRecord
	var typ string
	var rawDescriptorIDs pq.Int64Array
	var statusString string
	var created pq.NullTime
	var started pq.NullTime
	var finished pq.NullTime
	var modified pq.NullTime
	var err string
	// We have to query for the nth job created rather than filtering by ID,
	// because job-generating SQL queries (e.g. BACKUP) do not currently return
	// the job ID.
	db.QueryRow(`
		SELECT type, description, username, descriptor_ids, status,
			   created, started, finished, modified, error
		FROM crdb_internal.jobs ORDER BY created LIMIT 1 OFFSET $1`,
		expected.Offset,
	).Scan(
		&typ, &actualJob.Description, &actualJob.Username, &rawDescriptorIDs, &statusString,
		&created, &started, &finished, &modified, &err,
	)

	// Verify the upstream-provided fields.
	expected.Job.Details = nil
	for _, id := range rawDescriptorIDs {
		actualJob.DescriptorIDs = append(actualJob.DescriptorIDs, sqlbase.ID(id))
	}
	if e, a := expected.Job, actualJob; !reflect.DeepEqual(e, a) {
		diff := strings.Join(pretty.Diff(e, a), "\n")
		t.Errorf("job %d: JobRecords do not match:\n%s", expected.Offset, diff)
	}

	// Verify JobLogger-managed text fields.
	status := sql.JobStatus(statusString)
	if e, a := expectedStatus, status; e != a {
		t.Errorf("job %d: expected status %v, got %v", expected.Offset, e, a)
		return
	}
	if e, a := expected.Type, typ; e != a {
		t.Errorf("job %d: expected type %v, got type %v", expected.Offset, e, a)
	}

	// Check JobLogger-managed timestamps for sanity.
	verifyModifiedAgainst := func(name string, ts time.Time) {
		if modified.Time.Before(ts) {
			t.Errorf("job %d: modified time %v before %s time %v", expected.Offset, modified, name, ts)
		}
		if now := timeutil.Now().Round(time.Microsecond); modified.Time.After(now) {
			t.Errorf("job %d: modified time %v after current time %v", expected.Offset, modified, now)
		}
	}

	if expected.Before.After(created.Time) {
		t.Errorf(
			"job %d: created time %v is before expected created time %v",
			expected.Offset, created, expected.Before,
		)
	}
	if status == sql.JobStatusPending {
		verifyModifiedAgainst("created", created.Time)
		return
	}

	if !started.Valid && status == sql.JobStatusSucceeded {
		t.Errorf("job %d: started time is NULL but job claims to be successful", expected.Offset)
	}
	if started.Valid && created.Time.After(started.Time) {
		t.Errorf("job %d: created time %v is after started time %v", expected.Offset, created, started)
	}
	if status == sql.JobStatusRunning {
		verifyModifiedAgainst("started", started.Time)
		return
	}

	if started.Time.After(finished.Time) {
		t.Errorf("job %d: started time %v is after finished time %v", expected.Offset, started, finished)
	}
	verifyModifiedAgainst("finished", finished.Time)
	if e, a := expected.Error, err; e != a {
		t.Errorf("job %d: expected error %v, got %v", expected.Offset, e, a)
	}
}

func TestJobLogger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.TODO()
	params, _ := createTestServerParams()
	s, rawSQLDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

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
		verifyJobRecord(t, db, sql.JobStatusPending, woodyExpectation)
		if err := woodyLogger.Started(ctx); err != nil {
			t.Fatal(err)
		}
		verifyJobRecord(t, db, sql.JobStatusRunning, woodyExpectation)
		if err := woodyLogger.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		verifyJobRecord(t, db, sql.JobStatusSucceeded, woodyExpectation)

		// Buzz fails after it starts running.
		buzzJob := sql.JobRecord{
			Description:   "To infinity and beyond!",
			Username:      "Buzz Lightyear",
			DescriptorIDs: []sqlbase.ID{3, 2, 1},
		}
		buzzExpectation := jobExpectation{
			Offset: 1,
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
		verifyJobRecord(t, db, sql.JobStatusPending, buzzExpectation)
		if err := buzzLogger.Started(ctx); err != nil {
			t.Fatal(err)
		}
		verifyJobRecord(t, db, sql.JobStatusRunning, buzzExpectation)
		buzzLogger.Failed(ctx, errors.New("Buzz Lightyear can't fly"))
		verifyJobRecord(t, db, sql.JobStatusFailed, buzzExpectation)
		// Ensure that logging Buzz didn't corrupt Woody.
		verifyJobRecord(t, db, sql.JobStatusSucceeded, woodyExpectation)

		// Sid fails before it starts running.
		sidJob := sql.JobRecord{
			Description:   "The toys! The toys are alive!",
			Username:      "Sid Phillips",
			DescriptorIDs: []sqlbase.ID{6, 6, 6},
			Details:       sql.RestoreJobDetails{},
		}
		sidExpectation := jobExpectation{
			Offset: 2,
			Job:    sidJob,
			Type:   sql.JobTypeRestore,
			Before: timeutil.Now(),
			Error:  "Sid is a total failure",
		}
		sidLogger := sql.NewJobLogger(kvDB, s.LeaseManager().(*sql.LeaseManager), sidJob)
		if err := sidLogger.Created(ctx); err != nil {
			t.Fatal(err)
		}
		verifyJobRecord(t, db, sql.JobStatusPending, sidExpectation)
		sidLogger.Failed(ctx, errors.New("Sid is a total failure"))
		verifyJobRecord(t, db, sql.JobStatusFailed, sidExpectation)
		// Ensure that logging Sid didn't corrupt Woody or Buzz.
		verifyJobRecord(t, db, sql.JobStatusSucceeded, woodyExpectation)
		verifyJobRecord(t, db, sql.JobStatusFailed, buzzExpectation)
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

	t.Run("same status twice fails", func(t *testing.T) {
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
}
