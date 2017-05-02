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
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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
	ex    InternalExecutor
	jobID *int64
	Job   JobRecord
}

// JobRecord stores the job fields that are not automatically managed by
// JobLogger.
type JobRecord struct {
	Description   string
	Username      string
	DescriptorIDs sqlbase.IDs
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

// JobID returns the ID of the job that this JobLogger is currently tracking.
// This will be nil if Created has not yet been called.
func (jl *JobLogger) JobID() *int64 {
	return jl.jobID
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
	return jl.updateJobRecord(ctx, JobStatusRunning, func(payload *JobPayload) (bool, error) {
		if payload.StartedMicros != 0 {
			return false, errors.Errorf("JobLogger: job %d already started", jl.jobID)
		}
		payload.StartedMicros = jobTimestamp(timeutil.Now())
		return true, nil
	})
}

// Progressed updates the progress of the tracked job to fractionCompleted. A
// fractionCompleted that is less than the currently-recorded fractionCompleted
// will be silently ignored.
func (jl *JobLogger) Progressed(ctx context.Context, fractionCompleted float32) error {
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
		if fractionCompleted <= payload.FractionCompleted {
			return false, nil
		}
		payload.FractionCompleted = fractionCompleted
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
			return false, errors.Errorf("JobLogger: job %d already finished", jl.jobID)
		}
		payload.Error = err.Error()
		payload.FinishedMicros = jobTimestamp(timeutil.Now())
		return true, nil
	})
	if internalErr != nil {
		log.Errorf(ctx, "JobLogger: ignoring error while logging failure for job %d: %+v",
			jl.jobID, internalErr)
	}
}

// Succeeded marks the tracked job as having succeeded and sets its fraction
// completed to 1.0.
func (jl *JobLogger) Succeeded(ctx context.Context) error {
	return jl.updateJobRecord(ctx, JobStatusSucceeded, func(payload *JobPayload) (bool, error) {
		if payload.FinishedMicros != 0 {
			return false, errors.Errorf("JobLogger: job %d already finished", jl.jobID)
		}
		payload.FinishedMicros = jobTimestamp(timeutil.Now())
		payload.FractionCompleted = 1.0
		return true, nil
	})
}

func (jl *JobLogger) insertJobRecord(ctx context.Context, payload *JobPayload) error {
	if jl.jobID != nil {
		return errors.Errorf("JobLogger cannot create job: job %d already created", jl.jobID)
	}

	var row parser.Datums
	if err := jl.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
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
	jl.jobID = (*int64)(row[0].(*parser.DInt))

	return nil
}

func (jl *JobLogger) updateJobRecord(
	ctx context.Context, newStatus JobStatus, updateFn func(*JobPayload) (doUpdate bool, err error),
) error {
	if jl.jobID == nil {
		return errors.New("JobLogger cannot update job: job not created")
	}

	return jl.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		const selectStmt = "SELECT payload FROM system.jobs WHERE id = $1"
		row, err := jl.ex.QueryRowInTransaction(ctx, "log-job", txn, selectStmt, *jl.jobID)
		if err != nil {
			return err
		}

		payload, err := unmarshalJobPayload(row[0])
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
