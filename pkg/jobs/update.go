// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// UpdateFn is the callback passed to Job.Update. It is called from the context
// of a transaction and is passed the current metadata for the job. The callback
// can modify metadata using the JobUpdater and the changes will be persisted
// within the same transaction.
//
// The function is free to modify contents of JobMetadata in place (but the
// changes will be ignored unless JobUpdater is used).
type UpdateFn func(txn *kv.Txn, md JobMetadata, ju *JobUpdater) error

// JobMetadata groups the job metadata values passed to UpdateFn.
type JobMetadata struct {
	ID       int64
	Status   Status
	Payload  *jobspb.Payload
	Progress *jobspb.Progress
}

// CheckRunningOrReverting returns an InvalidStatusError if md.Status is not
// StatusRunning or StatusReverting.
func (md *JobMetadata) CheckRunningOrReverting() error {
	if md.Status != StatusRunning && md.Status != StatusReverting {
		return &InvalidStatusError{md.ID, md.Status, "update progress on", md.Payload.Error}
	}
	return nil
}

// JobUpdater accumulates changes to job metadata that are to be persisted.
type JobUpdater struct {
	md JobMetadata
}

// UpdateStatus sets a new status (to be persisted).
func (ju *JobUpdater) UpdateStatus(status Status) {
	ju.md.Status = status
}

// UpdatePayload sets a new Payload (to be persisted).
//
// WARNING: the payload can be large (resulting in a large KV for each version);
// it shouldn't be updated frequently.
func (ju *JobUpdater) UpdatePayload(payload *jobspb.Payload) {
	ju.md.Payload = payload
}

// UpdateProgress sets a new Progress (to be persisted).
func (ju *JobUpdater) UpdateProgress(progress *jobspb.Progress) {
	ju.md.Progress = progress
}

func (ju *JobUpdater) hasUpdates() bool {
	return ju.md != JobMetadata{}
}

// Update is used to read the metadata for a job and potentially update it.
//
// The updateFn is called in the context of a transaction and is passed the
// current metadata for the job. It can choose to update parts of the metadata
// using the JobUpdater, causing them to be updated within the same transaction.
//
// Sample usage:
//
//   err := j.Update(ctx, func(_ *client.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
//     if md.Status != StatusRunning {
//       return errors.New("job no longer running")
//     }
//     md.UpdateStatus(StatusPaused)
//     // <modify md.Payload>
//     md.UpdatePayload(md.Payload)
//   }
//
// Note that there are various convenience wrappers (like FractionProgressed)
// defined in jobs.go.
func (j *Job) Update(ctx context.Context, updateFn UpdateFn) error {
	if j.id == nil {
		return errors.New("Job: cannot update: job not created")
	}

	var payload *jobspb.Payload
	var progress *jobspb.Progress
	if err := j.runInTxn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		const selectStmt = "SELECT status, payload, progress FROM system.jobs WHERE id = $1"
		row, err := j.registry.ex.QueryRowEx(
			ctx, "log-job", txn, sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			selectStmt, *j.id)
		if err != nil {
			return err
		}
		if row == nil {
			return errors.Errorf("no such job %d found", *j.id)
		}

		statusString, ok := row[0].(*tree.DString)
		if !ok {
			return errors.Errorf("Job: expected string status on job %d, but got %T", *j.id, statusString)
		}
		status := Status(*statusString)
		if payload, err = UnmarshalPayload(row[1]); err != nil {
			return err
		}
		if progress, err = UnmarshalProgress(row[2]); err != nil {
			return err
		}

		md := JobMetadata{
			ID:       *j.id,
			Status:   status,
			Payload:  payload,
			Progress: progress,
		}
		var ju JobUpdater
		if err := updateFn(txn, md, &ju); err != nil {
			return err
		}

		if !ju.hasUpdates() {
			return nil
		}

		// Build a statement of the following form, depending on which properties
		// need updating:
		//
		//   UPDATE system.jobs
		//   SET
		//     [status = $2,]
		//     [payload = $y,]
		//     [progress = $z]
		//   WHERE
		//     id = $1

		var setters []string
		params := []interface{}{*j.id} // $1 is always the job ID.
		addSetter := func(column string, value interface{}) {
			params = append(params, value)
			setters = append(setters, fmt.Sprintf("%s = $%d", column, len(params)))
		}

		if ju.md.Status != "" {
			addSetter("status", ju.md.Status)
		}

		if ju.md.Payload != nil {
			payload = ju.md.Payload
			payloadBytes, err := protoutil.Marshal(payload)
			if err != nil {
				return err
			}
			addSetter("payload", payloadBytes)
		}

		if ju.md.Progress != nil {
			progress = ju.md.Progress
			progress.ModifiedMicros = timeutil.ToUnixMicros(txn.ReadTimestamp().GoTime())
			progressBytes, err := protoutil.Marshal(progress)
			if err != nil {
				return err
			}
			addSetter("progress", progressBytes)
		}

		updateStmt := fmt.Sprintf(
			"UPDATE system.jobs SET %s WHERE id = $1",
			strings.Join(setters, ", "),
		)
		n, err := j.registry.ex.Exec(ctx, "job-update", txn, updateStmt, params...)
		if err != nil {
			return err
		}
		if n != 1 {
			return errors.Errorf(
				"Job: expected exactly one row affected, but %d rows affected by job update", n,
			)
		}
		return nil
	}); err != nil {
		return err
	}
	if payload != nil {
		j.mu.Lock()
		j.mu.payload = *payload
		j.mu.Unlock()
	}
	if progress != nil {
		j.mu.Lock()
		j.mu.progress = *progress
		j.mu.Unlock()
	}
	return nil
}
