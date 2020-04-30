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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// cronEnv is an environment for running scheduled jobs.
// This environment facilitates dependency injection mechanism for tests.
type cronEnv interface {
	// CrontabTableName returns the name of the crontab table.
	CrontabTableName() string
	// SystemJobsTableName returns the name of the system jobs table.
	SystemJobsTableName() string
	// Now returns current time.
	Now() time.Time
	// NowExpr returns expression representing current time when
	// used in the database queries.
	NowExpr() string
}

// production cronEnv implementation.
type prodCronEnvImpl struct{}

var prodCronEnv cronEnv = &prodCronEnvImpl{}

func (e *prodCronEnvImpl) CrontabTableName() string {
	return "system.crontab"
}

func (e *prodCronEnvImpl) SystemJobsTableName() string {
	return "system.jobs"
}

func (e *prodCronEnvImpl) Now() time.Time {
	return timeutil.Now()
}

func (e *prodCronEnvImpl) NowExpr() string {
	return "current_timestamp()"
}

// cronDaemon is responsible for finding and starting scheduled
// jobs that need to be executed.
type cronDaemon struct {
	env cronEnv
	db  *kv.DB
	ex  sqlutil.InternalExecutor
}

func newCronDaemon(env cronEnv, db *kv.DB, ex sqlutil.InternalExecutor) *cronDaemon {
	if env == nil {
		env = prodCronEnv
	}
	return &cronDaemon{
		env: env,
		db:  db,
		ex:  ex,
	}
}

// getFindJobsStatement returns SQL statement used for finding
// scheduled jobs that should be started.
func (c *cronDaemon) getFindJobsStatement() string {
	return fmt.Sprintf(
		`
SELECT 
  C.sched_id, C.job_name, C.schedule_expr, C.job_details, C.exec_spec, C.change_info, C.owner,
  (SELECT count(*) 
   FROM %s J
   WHERE J.sched_id = C.sched_id AND J.status NOT IN ('failed', 'succeeded', 'cancelled')
  ) AS num_running
FROM %s C
WHERE next_run < %s
`, c.env.SystemJobsTableName(), c.env.CrontabTableName(), c.env.NowExpr())
}

// unmarshalScheduledJOb is a helper to desiralize a row returned by
// getFindJobsStatement() into a scheduledJob
func (c *cronDaemon) unmarshalScheduledJob(row []tree.Datum) (*scheduledJob, int64, error) {
	j := &scheduledJob{env: c.env}
	if err := j.FromDatums(row[:7],
		&j.schedID, &j.jobName, &j.schedExpr,
		&j.jobDetails, &j.execSpec, &j.changeInfo, &j.owner); err != nil {
		return nil, 0, err
	}

	var numRunning intSerializer
	if err := numRunning.FromDatum(row[7]); err != nil {
		return nil, 0, err
	}
	return j, numRunning.Value(), nil
}

const recheckRunningAfter = 1 * time.Minute

func (c *cronDaemon) processJob(
	ctx context.Context, j *scheduledJob, numRunning int64, txn *kv.Txn,
) error {
	if numRunning > 0 {
		switch j.execSpec.Value().Wait {
		case jobspb.ScheduleDetails_WAIT:
			// TODO(yevgeniy): We might need to persist more state.
			// In particular, it'd be nice to add more time when repeatedly rescheduling
			// a job.  It would also be nice not to log each event.
			j.nextRun.Set(c.env.Now().Add(recheckRunningAfter))
			j.AddChangeReason("reschedule: %d running", numRunning)
			return j.Update(ctx, c.ex, txn)
		case jobspb.ScheduleDetails_SKIP:
			if err := j.ScheduleNextRun(); err != nil {
				return err
			}
			j.AddChangeReason("rescheduled: %d running", numRunning)
			return j.Update(ctx, c.ex, txn)
		}
	}

	// Execute SQL statement.
	// TODO(yevgeniy): Support executor types.
	//
	// TODO(yevgeniy): this is too simplistic.  It would be nice
	// to capture execution traces, or some similar debug information and save that.
	// Also, performing this under the same transaction as the scan loop is not ideal
	// since a single failure would result in rollback for all of the changes.
	n, err := c.ex.ExecEx(ctx, "exec-cron-jobs", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		j.jobDetails.Sql,
	)

	if err != nil {
		return err
	}

	// Schedule the next job run.
	if err := j.ScheduleNextRun(); err != nil {
		return err
	}
	j.AddChangeReason("executed: rows effected %d", n)
	return j.Update(ctx, c.ex, txn)
}

func (c *cronDaemon) findJobs(ctx context.Context) {
	err := c.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		rows, err := c.ex.QueryEx(ctx, "find-cron-jobs", nil,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			c.getFindJobsStatement(),
		)

		if err != nil {
			return err
		}

		for _, row := range rows {
			j, numRunning, err := c.unmarshalScheduledJob(row)

			if err != nil {
				log.Errorf(ctx, "Error parsing scheduled jobs record: %v", err)
				continue
			}

			if err := c.processJob(ctx, j, numRunning, txn); err != nil {
				log.Errorf(ctx, "Error processing scheduled job: sched_id=%d %v", j.schedID, err)
			}
		}
		return nil
	})

	if err != nil {
		log.Errorf(ctx, "Error finding scheduled jobs to run: %v", err)
	}
}

// TODO(yevgeniy): Hook daemon to the server; add tests.
func (c *cronDaemon) Start(ctx context.Context, stopper *stop.Stopper) {
	stopper.RunWorker(ctx, func(ctx context.Context) {
		for {
			select {
			case <-stopper.ShouldStop():
				return
			case <-time.After(time.Second):
				c.findJobs(ctx)
			}
		}
	})
}
