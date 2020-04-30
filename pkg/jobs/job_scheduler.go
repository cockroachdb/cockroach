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
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/fmtsafe/testdata/src/github.com/cockroachdb/errors"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// jobSchedulerEnv is an environment for running scheduled jobs.
// This environment facilitates dependency injection mechanism for tests.
type jobSchedulerEnv interface {
	// ScheduledJobsTableName returns the name of the crontab table.
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
type prodJobSchedulerEnv struct{}

var prodCronEnv jobSchedulerEnv = &prodJobSchedulerEnv{}

func (e *prodJobSchedulerEnv) ScheduledJobsTableName() string {
	return "system.crontab"
}

func (e *prodJobSchedulerEnv) SystemJobsTableName() string {
	return "system.jobs"
}

func (e *prodJobSchedulerEnv) Now() time.Time {
	return timeutil.Now()
}

func (e *prodJobSchedulerEnv) NowExpr() string {
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
		env = prodCronEnv
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
  C.sched_id, C.job_name, C.schedule_expr, C.job_args, C.schedule_details, C.schedule_changes, C.owner,
  (SELECT count(*) 
   FROM %s J
   WHERE J.sched_id = C.sched_id AND J.status NOT IN ('failed', 'succeeded', 'cancelled')
  ) AS num_running
FROM %s C
WHERE next_run < %s
`, s.env.SystemJobsTableName(), s.env.ScheduledJobsTableName(), s.env.NowExpr())
}

// unmarshalScheduledJOb is a helper to desiralize a row returned by
// getFindJobsStatement() into a scheduledJob
func (s *jobScheduler) unmarshalScheduledJob(
	row []tree.Datum, cols []sqlbase.ResultColumn,
) (*scheduledJob, int64, error) {
	j := &scheduledJob{env: s.env}
	if err := j.InitFromDatums(row[:7], cols); err != nil {

		return nil, 0, err
	}

	if n, ok := row[7].(*tree.DInt); ok {
		return j, int64(*n), nil
	}
	return nil, 0, errors.Newf("expected int found %T instead", row[7])
}

const recheckRunningAfter = 1 * time.Minute

func (s *jobScheduler) processJob(
	ctx context.Context, j *scheduledJob, numRunning int64, txn *kv.Txn,
) error {
	if numRunning > 0 {
		switch j.ScheduleDetails().Wait {
		case jobspb.ScheduleDetails_WAIT:
			// TODO(yevgeniy): We might need to persist more state.
			// In particular, it'd be nice to add more time when repeatedly rescheduling
			// a job.  It would also be nice not to log each event.
			j.SetNextRun(s.env.Now().Add(recheckRunningAfter))
			j.AddScheduleChangeReason("reschedule: %d running", numRunning)
			return j.Update(ctx, s.ex, txn)
		case jobspb.ScheduleDetails_SKIP:
			if err := j.ScheduleNextRun(); err != nil {
				return err
			}
			j.AddScheduleChangeReason("rescheduled: %d running", numRunning)
			return j.Update(ctx, s.ex, txn)
		}
	}

	// Execute SQL statement.
	// TODO(yevgeniy): Support executor types.
	//
	// TODO(yevgeniy): this is too simplistic.  It would be nice
	// to capture execution traces, or some similar debug information and save that.
	// Also, performing this under the same transaction as the scan loop is not ideal
	// since a single failure would result in rollback for all of the changes.
	// n, err := s.ex.ExecEx(ctx, "sched-exec", txn,
	// 	sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
	// 	j.jobArgs.Sql,
	// )
	//
	// if err != nil {
	// 	return err
	// }

	// Schedule the next job run.
	if err := j.ScheduleNextRun(); err != nil {
		return err
	}
	// j.AddScheduleChangeReason("executed: rows effected %d", n)
	return j.Update(ctx, s.ex, txn)
}

func (s *jobScheduler) findJobs(ctx context.Context) {
	err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		rows, cols, err := s.ex.QueryWithCols(ctx, "sched-find", nil,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			s.getFindJobsStatement(),
		)

		if err != nil {
			return err
		}

		for _, row := range rows {
			j, numRunning, err := s.unmarshalScheduledJob(row, cols)

			if err != nil {
				log.Errorf(ctx, "Error parsing scheduled jobs record: %v", err)
				continue
			}

			if err := s.processJob(ctx, j, numRunning, txn); err != nil {
				log.Errorf(ctx, "Error processing scheduled job: schedule_id=%d %v", j.ScheduleID)
			}
		}
		return nil
	})

	if err != nil {
		log.Errorf(ctx, "Error finding scheduled jobs to run: %v", err)
	}
}

// TODO(yevgeniy): Hook daemon to the server; add tests.
func (s *jobScheduler) Start(ctx context.Context, stopper *stop.Stopper) {
	stopper.RunWorker(ctx, func(ctx context.Context) {
		for {
			select {
			case <-stopper.ShouldStop():
				return
			case <-time.After(time.Second):
				s.findJobs(ctx)
			}
		}
	})
}
