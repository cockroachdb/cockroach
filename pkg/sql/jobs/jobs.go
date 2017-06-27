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

package jobs

import (
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

// JobLogger manages logging the progress of long-running system processes, like
// backups and restores, to the system.jobs table.
//
// The Job field can be directly modified before Created is called. Updates to
// the Job field after the job has been created will not be written to the
// database, however, even when calling e.g. Started or Succeeded.
type JobLogger struct {
	db    *client.DB
	ex    sqlutil.InternalExecutor
	jobID *int64
	Job   JobRecord
	txn   *client.Txn

	mu struct {
		syncutil.Mutex
		payload JobPayload
	}
}

// JobDetails is a marker interface for job details proto structs.
type JobDetails interface{}

// JobRecord stores the job fields that are not automatically managed by
// JobLogger.
type JobRecord struct {
	Description   string
	Username      string
	DescriptorIDs sqlbase.IDs
	Details       JobDetails
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
func NewJobLogger(db *client.DB, ex sqlutil.InternalExecutor, job JobRecord) *JobLogger {
	return &JobLogger{
		db:  db,
		ex:  ex,
		Job: job,
	}
}

// GetJobLogger creates a new JobLogger initialized from a previously created
// job id.
func GetJobLogger(
	ctx context.Context, db *client.DB, ex sqlutil.InternalExecutor, jobID int64,
) (*JobLogger, error) {
	jl := &JobLogger{
		db:    db,
		ex:    ex,
		jobID: &jobID,
	}
	if err := jl.runInTxn(ctx, func(ctx context.Context, txn *client.Txn) error {
		payload, err := jl.retrieveJobPayload(ctx, txn)
		if err != nil {
			return err
		}
		jl.Job.Description = payload.Description
		jl.Job.Username = payload.Username
		jl.Job.DescriptorIDs = payload.DescriptorIDs
		switch d := payload.Details.(type) {
		case *JobPayload_Backup:
			jl.Job.Details = *d.Backup
		case *JobPayload_Restore:
			jl.Job.Details = *d.Restore
		case *JobPayload_SchemaChange:
			jl.Job.Details = *d.SchemaChange
		default:
			return errors.Errorf("JobLogger: unsupported job details type %T", d)
		}
		// Don't need to lock because we're the only one who has a handle on this
		// JobLogger so far.
		jl.mu.payload = *payload
		return nil
	}); err != nil {
		return nil, err
	}
	return jl, nil
}

func (jl *JobLogger) runInTxn(
	ctx context.Context, retryable func(context.Context, *client.Txn) error,
) error {
	if jl.txn != nil {
		defer func() { jl.txn = nil }()
		return jl.txn.Exec(ctx, client.TxnExecOptions{AutoRetry: true, AssignTimestampImmediately: true},
			func(ctx context.Context, txn *client.Txn, _ *client.TxnExecOptions) error {
				return retryable(ctx, txn)
			})
	}
	return jl.db.Txn(ctx, retryable)
}

// WithTxn sets the transaction that this JobLogger will use for its next
// operation. If the transaction is nil, the JobLogger will create a one-off
// transaction instead. If you use WithTxn, this JobLogger will no longer be
// threadsafe.
func (jl *JobLogger) WithTxn(txn *client.Txn) *JobLogger {
	jl.txn = txn
	return jl
}

// JobID returns the ID of the job that this JobLogger is currently tracking.
// This will be nil if Created has not yet been called.
func (jl *JobLogger) JobID() *int64 {
	return jl.jobID
}

// setDetails sets the Details field on a JobPayload, making sure that the
// input details is of a supported type.
func setDetails(payload *JobPayload, details interface{}) error {
	switch d := details.(type) {
	case BackupJobDetails:
		payload.Details = &JobPayload_Backup{Backup: &d}
	case RestoreJobDetails:
		payload.Details = &JobPayload_Restore{Restore: &d}
	case SchemaChangeJobDetails:
		payload.Details = &JobPayload_SchemaChange{SchemaChange: &d}
	default:
		return errors.Errorf("JobLogger: unsupported job details type %T", d)
	}
	return nil
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
	if err := setDetails(payload, jl.Job.Details); err != nil {
		return err
	}
	return jl.insertJobRecord(ctx, payload)
}

// Started marks the tracked job as started.
func (jl *JobLogger) Started(ctx context.Context) error {
	return jl.updateJobRecord(ctx, JobStatusRunning, func(payload *JobPayload) (bool, error) {
		if payload.StartedMicros != 0 {
			// Already started - do nothing.
			return false, nil
		}
		payload.StartedMicros = jobTimestamp(timeutil.Now())
		return true, nil
	})
}

// ProgressedFn is a callback that allows arbitrary modifications to a job's
// details when updating its progress.
type ProgressedFn func(ctx context.Context, details interface{})

// Noop is a nil ProgressedFn.
var Noop ProgressedFn

// Progressed updates the progress of the tracked job to fractionCompleted. A
// fractionCompleted that is less than the currently-recorded fractionCompleted
// will be silently ignored. If progressedFn is non-nil, it will be invoked with
// a pointer to the job's details to allow for modifications to the details
// before the job is saved. If no such modifications are required, pass Noop
// instead of nil for readability.
func (jl *JobLogger) Progressed(
	ctx context.Context, fractionCompleted float32, progressedFn ProgressedFn,
) error {
	if fractionCompleted < 0.0 || fractionCompleted > 1.0 {
		return errors.Errorf(
			"JobLogger: fractionCompleted %f is outside allowable range [0.0, 1.0] (job %d)",
			fractionCompleted, jl.jobID,
		)
	}
	return jl.updateJobRecord(ctx, JobStatusRunning, func(payload *JobPayload) (bool, error) {
		if payload.StartedMicros == 0 {
			return false, errors.Errorf("JobLogger: job %d not started", jl.jobID)
		}
		if payload.FinishedMicros != 0 {
			return false, errors.Errorf("JobLogger: job %d already finished", jl.jobID)
		}
		if fractionCompleted > payload.FractionCompleted {
			payload.FractionCompleted = fractionCompleted
		}
		if progressedFn != nil {
			progressedFn(ctx, payload.Details)
		}
		return true, nil
	})
}

// Failed marks the tracked job as having failed with the given error. Any
// errors encountered while updating the jobs table are logged but not returned,
// under the assumption that the the caller is already handling a more important
// error and doesn't care about this one.
func (jl *JobLogger) Failed(ctx context.Context, err error) {
	// To simplify cleanup routines, it is not an error to call Failed on a job
	// that was never Created.
	if jl.jobID == nil {
		return
	}
	internalErr := jl.updateJobRecord(ctx, JobStatusFailed, func(payload *JobPayload) (bool, error) {
		if payload.FinishedMicros != 0 {
			// Already finished - do nothing.
			return false, nil
		}
		payload.Error = err.Error()
		payload.FinishedMicros = jobTimestamp(timeutil.Now())
		return true, nil
	})
	if internalErr != nil {
		log.Errorf(ctx, "JobLogger: ignoring error %v while logging failure for job %d: %+v",
			err, jl.jobID, internalErr)
	}
}

// Succeeded marks the tracked job as having succeeded and sets its fraction
// completed to 1.0.
func (jl *JobLogger) Succeeded(ctx context.Context) error {
	return jl.updateJobRecord(ctx, JobStatusSucceeded, func(payload *JobPayload) (bool, error) {
		if payload.FinishedMicros != 0 {
			// Already finished - do nothing.
			return false, nil
		}
		payload.FinishedMicros = jobTimestamp(timeutil.Now())
		payload.FractionCompleted = 1.0
		return true, nil
	})
}

// SetDetails sets the details field of the tracked job.
func (jl *JobLogger) SetDetails(ctx context.Context, details interface{}) error {
	return jl.updateJobRecord(ctx, JobStatusSucceeded, func(payload *JobPayload) (bool, error) {
		if err := setDetails(payload, details); err != nil {
			return false, err
		}
		return true, nil
	})
}

// Payload returns the most recently sent JobPayload for this JobLogger. Will
// return an empty JobPayload until Created() is called on a new JobLogger.
func (jl *JobLogger) Payload() JobPayload {
	jl.mu.Lock()
	defer jl.mu.Unlock()
	return jl.mu.payload
}

func (jl *JobLogger) insertJobRecord(ctx context.Context, payload *JobPayload) error {
	if jl.jobID != nil {
		// Already created - do nothing.
		return nil
	}

	var row parser.Datums
	if err := jl.runInTxn(ctx, func(ctx context.Context, txn *client.Txn) error {
		payload.ModifiedMicros = jobTimestamp(txn.Proto().OrigTimestamp.GoTime())
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
	jl.mu.payload = *payload
	jl.jobID = (*int64)(row[0].(*parser.DInt))

	return nil
}

func (jl *JobLogger) retrieveJobPayload(ctx context.Context, txn *client.Txn) (*JobPayload, error) {
	const selectStmt = "SELECT payload FROM system.jobs WHERE id = $1"
	row, err := jl.ex.QueryRowInTransaction(ctx, "log-job", txn, selectStmt, *jl.jobID)
	if err != nil {
		return nil, err
	}

	return UnmarshalJobPayload(row[0])
}

func (jl *JobLogger) updateJobRecord(
	ctx context.Context, newStatus JobStatus, updateFn func(*JobPayload) (doUpdate bool, err error),
) error {
	if jl.jobID == nil {
		return errors.New("JobLogger cannot update job: job not created")
	}

	var payload *JobPayload
	if err := jl.runInTxn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		payload, err = jl.retrieveJobPayload(ctx, txn)
		if err != nil {
			return err
		}
		doUpdate, err := updateFn(payload)
		if err != nil {
			return err
		}
		if !doUpdate {
			return nil
		}
		payload.ModifiedMicros = jobTimestamp(timeutil.Now())
		payloadBytes, err := protoutil.Marshal(payload)
		if err != nil {
			return err
		}

		const updateStmt = "UPDATE system.jobs SET status = $1, payload = $2 WHERE id = $3"
		n, err := jl.ex.ExecuteStatementInTransaction(
			ctx, "job-update", txn, updateStmt, newStatus, payloadBytes, *jl.jobID)
		if err != nil {
			return err
		}
		if n != 1 {
			return errors.Errorf("JobLogger: expected exactly one row affected, but %d rows affected by job update", n)
		}
		return nil
	}); err != nil {
		return err
	}
	if payload != nil {
		jl.mu.Lock()
		jl.mu.payload = *payload
		jl.mu.Unlock()
	}
	return nil
}

// Job types are named for the SQL query that creates them.
const (
	JobTypeBackup       string = "BACKUP"
	JobTypeRestore      string = "RESTORE"
	JobTypeSchemaChange string = "SCHEMA CHANGE"
)

// Typ returns the payload's job type.
func (jp *JobPayload) Typ() string {
	switch jp.Details.(type) {
	case *JobPayload_Backup:
		return JobTypeBackup
	case *JobPayload_Restore:
		return JobTypeRestore
	case *JobPayload_SchemaChange:
		return JobTypeSchemaChange
	default:
		panic("JobPayload.Typ called on a payload with an unknown details type")
	}
}

// UnmarshalJobPayload unmarshals and returns the JobPayload encoded in the
// input datum, which should be a DBytes.
func UnmarshalJobPayload(datum parser.Datum) (*JobPayload, error) {
	payload := &JobPayload{}
	bytes, ok := datum.(*parser.DBytes)
	if !ok {
		return nil, errors.Errorf(
			"JobLogger: failed to unmarshal job payload as DBytes (was %T)", bytes)
	}
	if err := proto.Unmarshal([]byte(*bytes), payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// TIMESTAMP columns round to the nearest microsecond, so we replicate that
// behavior for our protobuf fields. Naive truncation can lead to anomalies
// where jobs are started before they're created.
func jobTimestamp(ts time.Time) int64 {
	return ts.Round(time.Microsecond).UnixNano() / time.Microsecond.Nanoseconds()
}

var _ JobDetails = BackupJobDetails{}
var _ JobDetails = RestoreJobDetails{}
var _ JobDetails = SchemaChangeJobDetails{}
