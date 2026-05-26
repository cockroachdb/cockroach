// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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
	txn isql.Txn,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
) error {
	sqlArgs := &jobspb.SqlStatementExecutionArg{}

	if err := types.UnmarshalAny(schedule.ExecutionArgs().Args, sqlArgs); err != nil {
		return errors.Wrapf(err, "expected SqlStatementExecutionArg")
	}

	// TODO(yevgeniy): this is too simplistic.  It would be nice
	// to capture execution traces, or some similar debug information and save that.
	// Also, performing this under the same transaction as the scan loop is not ideal
	// since a single failure would result in rollback for all of the changes.
	_, err := txn.ExecEx(ctx, "inline-exec", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
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
	txn isql.Txn,
	jobID jobspb.JobID,
	jobState State,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
) error {
	// For now, only interested in failed state.
	if jobState == StateFailed {
		DefaultHandleFailedRun(schedule, "job %d failed", jobID)
	}
	return nil
}

// Metrics implements ScheduledJobExecutor interface
func (e *inlineScheduledJobExecutor) Metrics() metric.Struct {
	return nil
}

func (e *inlineScheduledJobExecutor) GetCreateScheduleStatement(
	ctx context.Context, txn isql.Txn, env scheduledjobs.JobSchedulerEnv, sj *ScheduledJob,
) (string, error) {
	return "", errors.AssertionFailedf("unimplemented method: 'GetCreateScheduleStatement'")
}

func init() {
	RegisterScheduledJobExecutorFactory(
		InlineExecutorName,
		func() (ScheduledJobExecutor, error) {
			return &inlineScheduledJobExecutor{}, nil
		})
}
