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
// permissions and limitations under the License.⁄
//
// Author: Nikhil Benesch (benesch@cockroachlabs.com)

package sql

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

// JobStatus represents the status of a job in the system.jobs table.
type JobStatus string

const (
	// JobStatusPending is for jobs that have been created but on which work has
	// not yet started.
	JobStatusPending JobStatus = "pending"
	// JobStatusRunning is for in-progress jobs.
	JobStatusRunning JobStatus = "running"
	// JobStatusFailed is for jobs that have failed.
	JobStatusFailed JobStatus = "failed"
	// JobStatusSucceeded is for jobs that have succeeded.
	JobStatusSucceeded JobStatus = "succeeded"
)

// JobLogger allows long-running system processes, like backups and restores, to
// log their progress to the system.jobs table.
type JobLogger struct {
	db    *client.DB
	ex    InternalExecutor
	JobID *int64
}

// NewJobLogger creates a new JobLogger.
func NewJobLogger(db *client.DB, leaseMgr *LeaseManager) JobLogger {
	return JobLogger{
		db: db,
		ex: InternalExecutor{LeaseManager: leaseMgr},
	}
}

func (jl *JobLogger) insertJobRecord(ctx context.Context, payload *JobPayload) error {
	if jl.JobID != nil {
		return errors.Errorf("cannot log new job; this JobLogger is already tracking job %d", jl.JobID)
	}

	payload.Modified = timeutil.Now().UnixNano()
	payloadBytes, err := protoutil.Marshal(payload)
	if err != nil {
		return err
	}

	var row parser.Datums
	err = jl.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		const stmt = "INSERT INTO system.jobs (status, payload) VALUES ($1, $2) RETURNING id"
		row, err = jl.ex.QueryRowInTransaction(ctx, "job-insert", txn, stmt, JobStatusPending, payloadBytes)
		return err
	})
	if err != nil {
		return err
	}
	jl.JobID = (*int64)(row[0].(*parser.DInt))

	return nil
}

func (jl *JobLogger) updateJobRecord(
	ctx context.Context, newStatus JobStatus, updateFn func(payload *JobPayload),
) error {
	if jl.JobID == nil {
		return errors.New("cannot log update to job; this JobLogger is not tracking a job")
	}

	const selectStmt = "SELECT payload FROM system.jobs WHERE id = $1"
	const updateStmt = "UPDATE system.jobs SET status = $1, payload = $2 WHERE id = $3"

	return jl.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		row, err := jl.ex.QueryRowInTransaction(ctx, "log-job", txn, selectStmt, *jl.JobID)
		if err != nil {
			return err
		}

		payload := &JobPayload{}
		if err := proto.Unmarshal([]byte(*row[0].(*parser.DBytes)), payload); err != nil {
			return err
		}
		updateFn(payload)
		payload.Modified = timeutil.Now().UnixNano()
		payloadBytes, err := protoutil.Marshal(payload)
		if err != nil {
			return err
		}

		n, err := jl.ex.ExecuteStatementInTransaction(
			ctx, "job-update", txn, updateStmt, newStatus, payloadBytes, *jl.JobID)
		if err != nil {
			return err
		}
		if n != 1 {
			return errors.Errorf("%d rows affected by job update; expected exactly one row affected", n)
		}

		return nil
	})
}

// Created logs the creation of a new job to the system.jobs table and remembers
// the ID of the new job in the JobLogger.
func (jl *JobLogger) Created(
	ctx context.Context,
	description string,
	creator string,
	descriptorIDs []sqlbase.ID,
	details interface{},
) error {
	payload := &JobPayload{
		Description:   description,
		Creator:       creator,
		DescriptorIDs: descriptorIDs,
	}
	switch d := details.(type) {
	case BackupJobPayload:
		payload.Details = &JobPayload_BackupDetails{BackupDetails: &d}
	case RestoreJobPayload:
		payload.Details = &JobPayload_RestoreDetails{RestoreDetails: &d}
	case SchemaChangeJobPayload:
		payload.Details = &JobPayload_SchemaChangeDetails{SchemaChangeDetails: &d}
	default:
		return errors.Errorf("unsupported job type %T", d)
	}
	return jl.insertJobRecord(ctx, payload)
}

// Started marks the tracked job as started.
func (jl *JobLogger) Started(ctx context.Context, lease JobLease) error {
	return jl.updateJobRecord(ctx, JobStatusRunning, func(payload *JobPayload) {
		payload.Lease = lease
		payload.Started = timeutil.Now().UnixNano()
	})
}

// Failed marks the tracked job as having failed with the given error.
func (jl *JobLogger) Failed(ctx context.Context, err error) error {
	return jl.updateJobRecord(ctx, JobStatusFailed, func(payload *JobPayload) {
		payload.Error = err.Error()
		payload.Finished = timeutil.Now().UnixNano()
	})
}

// Succeeded marks the tracked job as having succeeded.
func (jl *JobLogger) Succeeded(ctx context.Context) error {
	return jl.updateJobRecord(ctx, JobStatusSucceeded, func(payload *JobPayload) {
		payload.Finished = timeutil.Now().UnixNano()
	})
}
