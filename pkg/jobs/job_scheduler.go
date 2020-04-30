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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// jobSchedulerEnv is an environment for running scheduled jobs.
// This environment facilitates dependency injection mechanism for tests.
type jobSchedulerEnv interface {
	// ScheduledJobsTableName returns the name of the scheduled_jobs table.
	ScheduledJobsTableName() string
	// SystemJobsTableName returns the name of the system jobs table.
	SystemJobsTableName() string
	// Now returns current time.
	Now() time.Time
	// NowExpr returns expression representing current time when
	// used in the database queries.
	NowExpr() string
}

// production jobSchedulerEnv implementation.
type prodJobSchedulerEnvImpl struct{}

var prodJobSchedulerEnv jobSchedulerEnv = &prodJobSchedulerEnvImpl{}

const createdByName = "schedule"

func (e *prodJobSchedulerEnvImpl) ScheduledJobsTableName() string {
	return "system.scheduled_jobs"
}

func (e *prodJobSchedulerEnvImpl) SystemJobsTableName() string {
	return "system.jobs"
}

func (e *prodJobSchedulerEnvImpl) Now() time.Time {
	return timeutil.Now()
}

func (e *prodJobSchedulerEnvImpl) NowExpr() string {
	return "current_timestamp()"
}

// jobScheduler is responsible for finding and starting scheduled
// jobs that need to be executed.
type jobScheduler struct {
	env jobSchedulerEnv
	db  *kv.DB
	ex  sqlutil.InternalExecutor
}

func newJobScheduler(env jobSchedulerEnv, db *kv.DB, ex sqlutil.InternalExecutor) *jobScheduler {
	if env == nil {
		env = prodJobSchedulerEnv
	}
	return &jobScheduler{
		env: env,
		db:  db,
		ex:  ex,
	}
}

// getFindJobsStatement returns SQL statement used for finding
// scheduled jobs that should be started.
func (s *jobScheduler) getFindJobsStatement() string {
	return fmt.Sprintf(
		`
SELECT
  (SELECT count(*) 
   FROM %s J
   WHERE 
      J.created_by_type = '%s' AND J.created_by_id = S.schedule_id AND 
      J.status NOT IN ('failed', 'succeeded', 'cancelled')
  ) AS num_running, S.*
FROM %s S
WHERE next_run < %s
`, s.env.SystemJobsTableName(), createdByName, s.env.ScheduledJobsTableName(), s.env.NowExpr())
}

// unmarshalScheduledJOb is a helper to desiralize a row returned by
// getFindJobsStatement() into a ScheduledJob
func (s *jobScheduler) unmarshalScheduledJob(
	row []tree.Datum, cols []sqlbase.ResultColumn,
) (*ScheduledJob, int64, error) {
	j := NewScheduledJob(s.env)
	if err := j.InitFromDatums(row[1:], cols[1:]); err != nil {
		return nil, 0, err
	}

	if n, ok := row[0].(*tree.DInt); ok {
		return j, int64(*n), nil
	}

	return nil, 0, errors.Newf("expected int found %T instead", row[0])
}

const recheckRunningAfter = 1 * time.Minute

func (s *jobScheduler) processSchedule(
	ctx context.Context, schedule *ScheduledJob, numRunning int64, txn *kv.Txn,
) error {
	if numRunning > 0 {
		switch schedule.ScheduleDetails().Wait {
		case jobspb.ScheduleDetails_WAIT:
			// TODO(yevgeniy): We might need to persist more state.
			// In particular, it'd be nice to add more time when repeatedly rescheduling
			// a job.  It would also be nice not to log each event.
			schedule.SetNextRun(s.env.Now().Add(recheckRunningAfter))
			schedule.AddScheduleChangeReason("reschedule: %d running", numRunning)
			return schedule.Update(ctx, s.ex, txn)
		case jobspb.ScheduleDetails_SKIP:
			if err := schedule.ScheduleNextRun(); err != nil {
				return err
			}
			schedule.AddScheduleChangeReason("rescheduled: %d running", numRunning)
			return schedule.Update(ctx, s.ex, txn)
		}
	}

	// Schedule the next job run.
	// We do this step early, before the actual execution, to grab a lock on
	// the scheduled_jobs table.
	if err := schedule.ScheduleNextRun(); err != nil {
		return err
	}

	if err := schedule.Update(ctx, s.ex, txn); err != nil {
		return err
	}

	executor, err := NewScheduledJobExecutor(schedule.ExecutorType(), s.ex)
	if err != nil {
		return err
	}

	// Grab job executor and execute the job.
	if err := executor.ExecuteJob(ctx, schedule, txn); err != nil {
		return err
	}

	// Persist any mutations to the underlying schedule.
	return schedule.Update(ctx, s.ex, txn)
}

func (s *jobScheduler) findJobs(ctx context.Context, txn *kv.Txn) error {
	rows, cols, err := s.ex.QueryWithCols(ctx, "find-scheduled-jobs", nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		s.getFindJobsStatement(),
	)

	if err != nil {
		return err
	}

	for _, row := range rows {
		// TODO(yevgeniy): Stopping entire loop because of one bad schedule is probably
		// not a great idea.  Improve handling of parsing/job execution errors.
		schedule, numRunning, err := s.unmarshalScheduledJob(row, cols)

		if err != nil {
			return err
		}

		if err := s.processSchedule(ctx, schedule, numRunning, txn); err != nil {
			return err
		}
	}
	return nil
}

// TODO(yevgeniy): Implement daemon to periodically call findJobs
