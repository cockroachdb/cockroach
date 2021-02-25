// Copyright 2020 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
)

// InlineExecutorName is the name associated with scheduled job executor which
// runs jobs "inline" -- that is, it doesn't spawn external system.job to do its work.
const InlineExecutorName = "inline"

// inlineScheduledJobExecutor implements ScheduledJobExecutor interface.
// This executor runs SQL statement "inline" -- that is, it executes statement
// directly under transaction.
type inlineScheduledJobExecutor struct{}

var _ ScheduledJobExecutor = &inlineScheduledJobExecutor{}

const retryFailedJobAfter = time.Minute

// ExecuteJob implements ScheduledJobExecutor interface.
func (e *inlineScheduledJobExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	_ scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
	txn *kv.Txn,
) error {
	sqlArgs := &jobspb.SqlStatementExecutionArg{}

	if err := types.UnmarshalAny(schedule.ExecutionArgs().Args, sqlArgs); err != nil {
		return errors.Wrapf(err, "expected SqlStatementExecutionArg")
	}

	// TODO(yevgeniy): this is too simplistic.  It would be nice
	// to capture execution traces, or some similar debug information and save that.
	// Also, performing this under the same transaction as the scan loop is not ideal
	// since a single failure would result in rollback for all of the changes.
	_, err := cfg.InternalExecutor.ExecEx(ctx, "inline-exec", txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		sqlArgs.Statement,
	)

	if err != nil {
		return err
	}

	// TODO(yevgeniy): Log execution result
	return nil
}

// NotifyJobTermination implements ScheduledJobExecutor interface.
func (e *inlineScheduledJobExecutor) NotifyJobTermination(
	ctx context.Context,
	jobID jobspb.JobID,
	jobStatus Status,
	_ jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
	ex sqlutil.InternalExecutor,
	txn *kv.Txn,
) error {
	// For now, only interested in failed status.
	if jobStatus == StatusFailed {
		DefaultHandleFailedRun(schedule, "job %d failed", jobID)
	}
	return nil
}

// Metrics implements ScheduledJobExecutor interface
func (e *inlineScheduledJobExecutor) Metrics() metric.Struct {
	return nil
}

func init() {
	RegisterScheduledJobExecutorFactory(
		InlineExecutorName,
		func() (ScheduledJobExecutor, error) {
			return &inlineScheduledJobExecutor{}, nil
		})
}
