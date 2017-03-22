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
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

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
		woodyExpectation := sql.JobExpectation{
			Job:    woodyJob,
			Type:   sql.JobTypeBackup,
			Before: timeutil.Now(),
		}
		woodyLogger := sql.NewJobLogger(kvDB, s.LeaseManager().(*sql.LeaseManager), woodyJob)
		if err := woodyLogger.Created(ctx); err != nil {
			t.Fatal(err)
		}
		sql.VerifyJobRecord(t, db, sql.JobStatusPending, woodyExpectation)
		if err := woodyLogger.Started(ctx); err != nil {
			t.Fatal(err)
		}
		sql.VerifyJobRecord(t, db, sql.JobStatusRunning, woodyExpectation)
		if err := woodyLogger.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		sql.VerifyJobRecord(t, db, sql.JobStatusSucceeded, woodyExpectation)

		// Buzz fails after it starts running.
		buzzJob := sql.JobRecord{
			Description:   "To infinity and beyond!",
			Username:      "Buzz Lightyear",
			DescriptorIDs: []sqlbase.ID{3, 2, 1},
		}
		buzzExpectation := sql.JobExpectation{
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
		sql.VerifyJobRecord(t, db, sql.JobStatusPending, buzzExpectation)
		if err := buzzLogger.Started(ctx); err != nil {
			t.Fatal(err)
		}
		sql.VerifyJobRecord(t, db, sql.JobStatusRunning, buzzExpectation)
		if err := buzzLogger.Failed(ctx, errors.New("Buzz Lightyear can't fly")); err != nil {
			t.Fatal(err)
		}
		sql.VerifyJobRecord(t, db, sql.JobStatusFailed, buzzExpectation)
		// Ensure that logging Buzz didn't corrupt Woody.
		sql.VerifyJobRecord(t, db, sql.JobStatusSucceeded, woodyExpectation)

		// Sid fails before it starts running.
		sidJob := sql.JobRecord{
			Description:   "The toys! The toys are alive!",
			Username:      "Sid Phillips",
			DescriptorIDs: []sqlbase.ID{6, 6, 6},
			Details:       sql.RestoreJobDetails{},
		}
		sidExpectation := sql.JobExpectation{
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
		sql.VerifyJobRecord(t, db, sql.JobStatusPending, sidExpectation)
		if err := sidLogger.Failed(ctx, errors.New("Sid is a total failure")); err != nil {
			t.Fatal(err)
		}
		sql.VerifyJobRecord(t, db, sql.JobStatusFailed, sidExpectation)
		// Ensure that logging Sid didn't corrupt Woody or Buzz.
		sql.VerifyJobRecord(t, db, sql.JobStatusSucceeded, woodyExpectation)
		sql.VerifyJobRecord(t, db, sql.JobStatusFailed, buzzExpectation)
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
		if err := logger.Succeeded(ctx); !testutils.IsError(err, "job not created") {
			t.Fatalf("expected 'job not created' error, but got %v", err)
		}
		if err := logger.Failed(ctx, errors.New("test fail")); !testutils.IsError(err, "job not created") {
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
		if err := logger.Failed(ctx, errors.New("test fail")); !testutils.IsError(err, `job \d+ already finished`) {
			t.Fatalf("expected 'job already finished' error, but got %v", err)
		}
	})

	t.Run("Failed with nil error fails", func(t *testing.T) {
		logger := sql.NewJobLogger(kvDB, s.LeaseManager().(*sql.LeaseManager), sql.JobRecord{
			Details: sql.BackupJobDetails{},
		})
		if err := logger.Created(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := logger.Failed(ctx, nil); !testutils.IsError(err, "Failed called with nil error") {
			t.Fatalf("expected 'Failed called with nil error' error, but got %v", err)
		}
	})
}
