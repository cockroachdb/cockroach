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
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

const compactionScheduleName = "sql-stats-compaction"

const (
	// ScheduledStatusWaiting is the sql stats compaction scheduled job status
	// when it is initially created in the system.scheduled_jobs table.
	ScheduledStatusWaiting = "waiting to be ran by scheduled job scheduler"

	// ScheduledStatusCreated is the sql stats compaction scheduled job status
	// when it has been executed by the scheduled job scheduler and created the
	// job record for sql stat compaction job.
	ScheduledStatusCreated = "sql stats compaction job created"

	// ScheduledStatusExecuting is the sql stats compaction scheduled job status
	// when the sql stats compaction job starts executing.
	ScheduledStatusExecuting = "executing sql stats  compaction"

	// ScheduledStatusCompleted is the sql stats compaction scheduled job status
	// when the sql stats compaction job completed.
	ScheduledStatusCompleted = "sql stats compaction completed"
)

// ErrDuplicatedSchedules indicates that there is already a schedule for sql
// stats compaction job existing in the system.scheduled_jobs table.
var ErrDuplicatedSchedules = errors.New("creating multiple sql stats compaction is disallowed")

// CreateSQLStatsCompactionScheduleIfNotYetExist registers SQL Stats compaction job with the
// scheduled job subsystem so the compaction job can be run periodically. This
// is done during the cluster startup migration.
func CreateSQLStatsCompactionScheduleIfNotYetExist(
	ctx context.Context, ie sqlutil.InternalExecutor, txn *kv.Txn,
) error {
	scheduleExists, err := checkExistingCompactionSchedule(ctx, ie, txn)
	if err != nil {
		return err
	}

	if scheduleExists {
		return ErrDuplicatedSchedules
	}

	compactionJob := jobs.NewScheduledJob(scheduledjobs.ProdJobSchedulerEnv)

	// TODO(azhng): currently we don't have ability to change recurrence
	//  expression for the scheduled job. We want to create a cluster setting
	//  and register a SetOnChange() callback onto that cluster setting so
	//  the scheduled job recurrence can be changed via SQL Shell.
	if err := compactionJob.SetSchedule("@hourly"); err != nil {
		return err
	}

	compactionJob.SetScheduleDetails(jobspb.ScheduleDetails{
		Wait:    jobspb.ScheduleDetails_SKIP,
		OnError: jobspb.ScheduleDetails_RETRY_SCHED,
	})

	compactionJob.SetScheduleLabel(compactionScheduleName)
	compactionJob.SetNextRun(timeutil.Now().Add(time.Hour))
	compactionJob.SetOwner(security.NodeUserName())

	any, err := pbtypes.MarshalAny(&ScheduledSQLStatsCompactorExecutionArgs{})
	if err != nil {
		return err
	}
	compactionJob.SetExecutionDetails(
		tree.ScheduledSQLStatsCompactionExecutor.InternalName(),
		jobspb.ExecutionArguments{Args: any},
	)

	compactionJob.SetScheduleStatus(ScheduledStatusWaiting)
	return compactionJob.Create(ctx, ie, txn)
}

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

func checkExistingCompactionSchedule(
	ctx context.Context, ie sqlutil.InternalExecutor, txn *kv.Txn,
) (exists bool, err error) {
	query := `
SELECT count(*) FROM system.scheduled_jobs WHERE schedule_name = $1
`

	row, err := ie.QueryRowEx(ctx, "check-existing-sql-stats-schedule", txn,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		query, compactionScheduleName,
	)

	if err != nil {
		return exists, err
	}

	if row == nil {
		return exists, errors.AssertionFailedf("unexpected empty result when querying system.scheduled_job")
	}

	if len(row) != 1 {
		return exists, errors.AssertionFailedf("unexpectedly received %d columns", len(row))
	}

	return tree.MustBeDInt(row[0]) == 1, nil /* err */
}
