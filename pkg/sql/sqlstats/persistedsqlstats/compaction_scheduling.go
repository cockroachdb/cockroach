// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package persistedsqlstats

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

const compactionScheduleName = "sql-stats-compaction"

// ErrDuplicatedSchedules indicates that there is already a schedule for sql
// stats compaction job existing in the system.scheduled_jobs table.
var ErrDuplicatedSchedules = errors.New("creating multiple sql stats compaction is disallowed")

// CreateSQLStatsCompactionScheduleIfNotYetExist registers SQL Stats compaction job with the
// scheduled job subsystem so the compaction job can be run periodically. This
// is done during the cluster startup upgrade.
func CreateSQLStatsCompactionScheduleIfNotYetExist(
	ctx context.Context, txn isql.Txn, st *cluster.Settings, clusterID uuid.UUID,
) (*jobs.ScheduledJob, error) {
	scheduleExists, err := checkExistingCompactionSchedule(ctx, txn)
	if err != nil {
		return nil, err
	}

	if scheduleExists {
		return nil, ErrDuplicatedSchedules
	}

	compactionSchedule := jobs.NewScheduledJob(scheduledjobs.ProdJobSchedulerEnv)

	schedule := scheduledjobs.MaybeRewriteCronExpr(
		clusterID, SQLStatsCleanupRecurrence.Get(&st.SV),
	)
	if err := compactionSchedule.SetScheduleAndNextRun(schedule); err != nil {
		return nil, err
	}

	compactionSchedule.SetScheduleDetails(jobspb.ScheduleDetails{
		Wait:                   jobspb.ScheduleDetails_SKIP,
		OnError:                jobspb.ScheduleDetails_RETRY_SCHED,
		ClusterID:              clusterID,
		CreationClusterVersion: st.Version.ActiveVersion(ctx),
	})

	compactionSchedule.SetScheduleLabel(compactionScheduleName)
	compactionSchedule.SetOwner(username.NodeUserName())

	args, err := pbtypes.MarshalAny(&ScheduledSQLStatsCompactorExecutionArgs{})
	if err != nil {
		return nil, err
	}
	compactionSchedule.SetExecutionDetails(
		tree.ScheduledSQLStatsCompactionExecutor.InternalName(),
		jobspb.ExecutionArguments{Args: args},
	)

	compactionSchedule.SetScheduleStatus(string(jobs.StatePending))
	if err := jobs.ScheduledJobTxn(txn).Create(ctx, compactionSchedule); err != nil {
		return nil, err
	}

	return compactionSchedule, nil
}

// CreateCompactionJob creates a system.jobs record.
// We do not need to worry about checking if the job already exist;
// at most 1 job semantics are enforced by scheduled jobs system.
func CreateCompactionJob(
	ctx context.Context, createdByInfo *jobs.CreatedByInfo, txn isql.Txn, jobRegistry *jobs.Registry,
) (jobspb.JobID, error) {
	record := jobs.Record{
		Description: "automatic SQL Stats compaction",
		Username:    username.NodeUserName(),
		Details:     jobspb.AutoSQLStatsCompactionDetails{},
		Progress:    jobspb.AutoSQLStatsCompactionProgress{},
		CreatedBy:   createdByInfo,
	}

	jobID := jobRegistry.MakeJobID()
	if _, err := jobRegistry.CreateAdoptableJobWithTxn(ctx, record, jobID, txn); err != nil {
		return jobspb.InvalidJobID, err
	}
	return jobID, nil
}

func checkExistingCompactionSchedule(ctx context.Context, txn isql.Txn) (exists bool, _ error) {
	query := "SELECT count(*) FROM system.scheduled_jobs WHERE schedule_name = $1"

	row, err := txn.QueryRowEx(ctx, "check-existing-sql-stats-schedule", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		query, compactionScheduleName,
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
