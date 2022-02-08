// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scheduledloggingjobs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

// CreateLoggingScheduleJob creates a scheduled job.
func CreateLoggingScheduleJob(
	ctx context.Context,
	ie sqlutil.InternalExecutor,
	txn *kv.Txn,
	label string,
	scheduleInterval string,
	executorType tree.ScheduledJobExecutorType,
	scheduleDetails jobspb.ScheduleDetails,
	executionArgs protoutil.Message,
) (*jobs.ScheduledJob, error) {
	// Check for duplicate based on job label.
	scheduleExists, err := checkExistingLoggingSchedule(ctx, ie, txn, label)
	if err != nil {
		return nil, err
	}
	if scheduleExists {
		return nil, errors.Newf("creating multiple scheduled jobs with the label '%s' is not allowed", label)
	}

	loggingScheduleJob := jobs.NewScheduledJob(scheduledjobs.ProdJobSchedulerEnv)
	if err := loggingScheduleJob.SetSchedule(scheduleInterval); err != nil {
		return nil, err
	}

	loggingScheduleJob.SetScheduleDetails(scheduleDetails)

	args, err := pbtypes.MarshalAny(executionArgs)
	if err != nil {
		return nil, err
	}
	loggingScheduleJob.SetExecutionDetails(
		executorType.InternalName(),
		jobspb.ExecutionArguments{Args: args},
	)

	loggingScheduleJob.SetScheduleLabel(label)
	loggingScheduleJob.SetOwner(security.NodeUserName())

	loggingScheduleJob.SetScheduleStatus(string(jobs.StatusPending))
	if err := loggingScheduleJob.Create(ctx, ie, txn); err != nil {
		return nil, err
	}

	return loggingScheduleJob, nil
}

func checkExistingLoggingSchedule(
	ctx context.Context, ie sqlutil.InternalExecutor, txn *kv.Txn, label string,
) (exists bool, _ error) {
	query := "SELECT count(*) FROM system.scheduled_jobs WHERE schedule_name = $1"

	row, err := ie.QueryRowEx(ctx, "check-existing-logging-schedule", txn,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		query, label,
	)

	if err != nil {
		return false /* exists */, err
	}

	if row == nil {
		return false /* exists */, errors.AssertionFailedf("unexpected empty result when querying system.scheduled_job")
	}

	if len(row) != 1 {
		return false /* exists */, errors.AssertionFailedf("unexpectedly received %d columns", len(row))
	}

	// Defensively check the count.
	return tree.MustBeDInt(row[0]) > 0, nil /* err */
}
