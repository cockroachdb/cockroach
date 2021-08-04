// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package persistedsqlstats

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// CreateCompactionJob creates a system.jobs record if there is no other
// SQL Stats compaction job running. This is invoked by the scheduled job
// Executor.
func CreateCompactionJob(
	ctx context.Context,
	createdByInfo *jobs.CreatedByInfo,
	txn *kv.Txn,
	ie sqlutil.InternalExecutor,
	jobRegistry *jobs.Registry,
) (jobspb.JobID, error) {
	if err := CheckExistingCompactionJob(ctx, nil /* job */, ie, txn); err != nil {
		return jobspb.InvalidJobID, err
	}

	record := jobs.Record{
		Description: "SQL Stats compaction",
		Username:    security.NodeUserName(),
		Details:     jobspb.SQLStatsCompactionDetails{},
		Progress:    jobspb.SQLStatsCompactionProgress{},
		CreatedBy:   createdByInfo,
	}

	jobID := jobRegistry.MakeJobID()
	if _, err := jobRegistry.CreateAdoptableJobWithTxn(ctx, record, jobID, txn); err != nil {
		return jobspb.InvalidJobID, err
	}
	return jobID, nil
}

// CheckExistingCompactionJob checks for existing SQL Stats Compaction job
// that are either PAUSED, CANCELED, or RUNNING. If so, it returns a
// ErrConcurrentSQLStatsCompaction.
func CheckExistingCompactionJob(
	ctx context.Context, job *jobs.Job, ie sqlutil.InternalExecutor, txn *kv.Txn,
) error {
	jobID := jobspb.InvalidJobID
	if job != nil {
		jobID = job.ID()
	}
	exists, err := jobs.RunningJobExists(ctx, jobID, ie, txn, func(payload *jobspb.Payload) bool {
		return payload.Type() == jobspb.TypeSQLStatsCompaction
	})

	if err == nil && exists {
		err = ErrConcurrentSQLStatsCompaction
	}
	return err
}
