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

package sql

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/gogo/protobuf/proto"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// JobLogger manages logging the progress of long-running system processes, like
// backups and restores, to to the system.jobs table.
//
// The Job field can be directly modified before Created is called. Updates to
// the Job field after the job has been created will not be written to the
// database, however, even when calling e.g. Started or Succeeded.
type JobLogger struct {
	db    *client.DB
	ex    InternalExecutor
	jobID *int64
	Job   JobRecord
}

// JobRecord stores the job fields that are not automatically managed by
// JobLogger.
type JobRecord struct {
	Description   string
	Username      string
	DescriptorIDs []sqlbase.ID
	Details       interface{}
}

// JobStatus represents the status of a job in the system.jobs table.
type JobStatus string

const (
	// JobStatusPending is for jobs that have been created but on which work has
	// not yet started.
	JobStatusPending JobStatus = "pending"
	// JobStatusRunning is for jobs that are currently in progress.
	JobStatusRunning JobStatus = "running"
	// JobStatusFailed is for jobs that failed.
	JobStatusFailed JobStatus = "failed"
	// JobStatusSucceeded is for jobs that have successfully completed.
	JobStatusSucceeded JobStatus = "succeeded"
)

// NewJobLogger creates a new JobLogger.
func NewJobLogger(db *client.DB, leaseMgr *LeaseManager, job JobRecord) JobLogger {
	return JobLogger{
		db:  db,
		ex:  InternalExecutor{LeaseManager: leaseMgr},
		Job: job,
	}
}

// Created records the creation of a new job in the system.jobs table and
// remembers the assigned ID of the job in the JobLogger. The job information is
// read from the Job field at the time Created is called.
func (jl *JobLogger) Created(ctx context.Context) error {
	payload := &JobPayload{
		Description:   jl.Job.Description,
		Username:      jl.Job.Username,
		DescriptorIDs: jl.Job.DescriptorIDs,
	}
	switch d := jl.Job.Details.(type) {
	case BackupJobDetails:
		payload.Details = &JobPayload_Backup{Backup: &d}
	case RestoreJobDetails:
		payload.Details = &JobPayload_Restore{Restore: &d}
	default:
		return errors.Errorf("JobLogger: unsupported job details type %T", d)
	}
	return jl.insertJobRecord(ctx, payload)
}

// Started marks the tracked job as started.
func (jl *JobLogger) Started(ctx context.Context) error {
	return jl.updateJobRecord(ctx, JobStatusRunning, func(payload *JobPayload) error {
		if payload.Started != nil {
			return errors.Errorf("JobLogger: job %d already started", jl.jobID)
		}
		payload.Started = jobTimestamp(timeutil.Now())
		return nil
	})
}

// Failed marks the tracked job as having failed with the given error.
func (jl *JobLogger) Failed(ctx context.Context, err error) error {
	if err == nil {
		return errors.New("JobLogger: Failed called with nil error")
	}
	return jl.updateJobRecord(ctx, JobStatusFailed, func(payload *JobPayload) error {
		if payload.Finished != nil {
			return errors.Errorf("JobLogger: job %d already finished", jl.jobID)
		}
		payload.Error = err.Error()
		payload.Finished = jobTimestamp(timeutil.Now())
		return nil
	})
}

// Succeeded marks the tracked job as having succeeded.
func (jl *JobLogger) Succeeded(ctx context.Context) error {
	return jl.updateJobRecord(ctx, JobStatusSucceeded, func(payload *JobPayload) error {
		if payload.Finished != nil {
			return errors.Errorf("JobLogger: job %d already finished", jl.jobID)
		}
		payload.Finished = jobTimestamp(timeutil.Now())
		return nil
	})
}

func (jl *JobLogger) insertJobRecord(ctx context.Context, payload *JobPayload) error {
	if jl.jobID != nil {
		return errors.Errorf("JobLogger cannot create job: job %d already created", jl.jobID)
	}

	var row parser.Datums
	if err := jl.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		payload.Modified = protoutil.Time(txn.Proto().OrigTimestamp.GoTime().Round(time.Microsecond))
		payloadBytes, err := protoutil.Marshal(payload)
		if err != nil {
			return err
		}

		const stmt = "INSERT INTO system.jobs (status, payload) VALUES ($1, $2) RETURNING id"
		row, err = jl.ex.QueryRowInTransaction(ctx, "job-insert", txn, stmt, JobStatusPending, payloadBytes)
		return err
	}); err != nil {
		return err
	}
	jl.jobID = (*int64)(row[0].(*parser.DInt))

	return nil
}

func (jl *JobLogger) updateJobRecord(
	ctx context.Context, newStatus JobStatus, updateFn func(payload *JobPayload) error,
) error {
	if jl.jobID == nil {
		return errors.New("JobLogger cannot update job: job not created")
	}

	const selectStmt = "SELECT payload FROM system.jobs WHERE id = $1"
	const updateStmt = "UPDATE system.jobs SET status = $1, payload = $2 WHERE id = $3"

	return jl.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		row, err := jl.ex.QueryRowInTransaction(ctx, "log-job", txn, selectStmt, *jl.jobID)
		if err != nil {
			return err
		}

		payload, err := unmarshalJobPayload(row[0])
		if err != nil {
			return err
		}
		if err := updateFn(payload); err != nil {
			return err
		}
		payload.Modified = jobTimestamp(timeutil.Now())
		payloadBytes, err := protoutil.Marshal(payload)
		if err != nil {
			return err
		}

		n, err := jl.ex.ExecuteStatementInTransaction(
			ctx, "job-update", txn, updateStmt, newStatus, payloadBytes, *jl.jobID)
		if err != nil {
			return err
		}
		if n != 1 {
			return errors.Errorf("JobLogger: expected exactly one row affected, but %d rows affected by job update", n)
		}

		return nil
	})
}

// Job types are named for the SQL query that creates them.
const (
	JobTypeBackup  string = "BACKUP"
	JobTypeRestore string = "RESTORE"
)

func (jp *JobPayload) typ() string {
	switch jp.Details.(type) {
	case *JobPayload_Backup:
		return JobTypeBackup
	case *JobPayload_Restore:
		return JobTypeRestore
	default:
		panic("JobPayload.typ called on a payload with an unknown details type")
	}
}

func unmarshalJobPayload(datum parser.Datum) (*JobPayload, error) {
	payload := &JobPayload{}
	if err := proto.Unmarshal([]byte(*datum.(*parser.DBytes)), payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// TIMESTAMP columns only have microsecond resolution, but protobuf columns have
// nanosecond resolution. Since the modified field lives in the protobuf but the
// created field is a TIMESTAMP column, this can yield weird anomalies where
// modified < created unless we round all timestamps to the nearest microsecond.
func jobTimestamp(ts time.Time) *time.Time {
	return protoutil.Time(ts.Round(time.Microsecond))
}

// JobExpectation defines the information necessary to determine the validity of
// a job in the system.jobs table. Exposed for testing only.
type JobExpectation struct {
	Offset int
	Job    JobRecord
	Type   string
	Before time.Time
	Error  string
}

// VerifyJobRecord verifies that the JobExpectation matches the job record
// stored in the system.jobs table. Exposed for testing only.
func VerifyJobRecord(
	t *testing.T, db *sqlutils.SQLRunner, expectedStatus JobStatus, expected JobExpectation,
) {
	var typ string
	var description string
	var username string
	var descriptorArray pq.Int64Array
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
		&typ, &description, &username, &descriptorArray, &statusString,
		&created, &started, &finished, &modified, &err,
	)
	status := JobStatus(statusString)

	verifyModifiedAgainst := func(name string, time time.Time) {
		if modified.Time.Before(time) {
			t.Errorf("job %d: modified time %v before %s time %v", expected.Offset, modified, name, time)
		}
		if now := timeutil.Now(); modified.Time.After(now) {
			t.Errorf("job %d: modified time %v after current time %v", expected.Offset, modified, now)
		}
	}

	if e, a := expectedStatus, status; e != a {
		t.Errorf("job %d: expected status %v, got %v", expected.Offset, e, a)
		return
	}
	if e, a := expected.Type, typ; e != a {
		t.Errorf("job %d: expected type %v, got type %v", expected.Offset, e, a)
	}
	if e, a := expected.Job.Description, description; e != a {
		t.Errorf("job %d: expected description %v, got %v", expected.Offset, e, a)
	}
	if e, a := expected.Job.Username, username; e != a {
		t.Errorf("job %d: expected user %v, got %v", expected.Offset, e, a)
	}

	descriptors := make([]int, len(descriptorArray))
	for _, id := range descriptorArray {
		descriptors = append(descriptors, int(id))
	}
	expectedDescriptors := make([]int, len(expected.Job.DescriptorIDs))
	for _, id := range expected.Job.DescriptorIDs {
		expectedDescriptors = append(expectedDescriptors, int(id))
	}
	sort.Ints(expectedDescriptors)
	sort.Ints(descriptors)
	if e, a := expectedDescriptors, descriptors; !reflect.DeepEqual(e, a) {
		t.Errorf("job %d: expected descriptors %v, got %v", expected.Offset, e, a)
	}

	if expected.Before.After(created.Time) {
		t.Errorf(
			"job %d: created time %v is before expected created time %v",
			expected.Offset, created, expected.Before,
		)
	}
	if status == JobStatusPending {
		verifyModifiedAgainst("created", created.Time)
		return
	}

	if !started.Valid && status == JobStatusSucceeded {
		t.Errorf("job %d: started time is NULL but job claims to be successful", expected.Offset)
	}
	if started.Valid && created.Time.After(started.Time) {
		t.Errorf("job %d: created time %v is after started time %v", expected.Offset, created, started)
	}
	if status == JobStatusRunning {
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
