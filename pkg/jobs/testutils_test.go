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
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// testJobSchedulerEnv is a test implementation of cron environment.
// allows injection fake time, as well as use of non-system tables.
type testJobSchedulerEnv struct {
	scheduledJobsTableName string
	jobsTableName          string
	now                    time.Time
}

func (t *testJobSchedulerEnv) ScheduledJobsTableName() string {
	return t.scheduledJobsTableName
}

func (t *testJobSchedulerEnv) SystemJobsTableName() string {
	return t.jobsTableName
}

func (t *testJobSchedulerEnv) Now() time.Time {
	return t.now
}

func (t *testJobSchedulerEnv) NowExpr() string {
	return fmt.Sprintf("TIMESTAMPTZ '%s'",
		t.now.Format("2020-01-02 03:04:05.999999999"))
}

// Returns schema for the scheduled jobs table.
func (t *testJobSchedulerEnv) getScheduledJobsTableSchema() string {
	return strings.Replace(sqlbase.ScheduledJobsTableSchema, "system.scheduled_jobs", t.scheduledJobsTableName, 1)
}

// Return s schema for the jobs table.
func (t *testJobSchedulerEnv) getJobsTableSchema() string {
	if t.jobsTableName == "system.jobs" {
		return sqlbase.JobsTableSchema
	}

	// TODO(yevgeniy): Remove this override once system.jobs schema changed to include
	// schedule id.
	return fmt.Sprintf(`
CREATE TABLE %s (
	id                INT8      DEFAULT unique_rowid() PRIMARY KEY,
	sched_id          INT8,
	status            STRING    NOT NULL,
	created           TIMESTAMP NOT NULL DEFAULT now(),
	payload           BYTES     NOT NULL,
	progress          BYTES,
	INDEX (status, created),
	FAMILY (id, status, created, payload),
	FAMILY progress (progress)
);`, t.jobsTableName)
}

type testHelper struct {
	env   *testJobSchedulerEnv
	kvDB  *kv.DB
	sqlDB *sqlutils.SQLRunner
	ex    sqlutil.InternalExecutor
}

func newTestCronEnv() *testJobSchedulerEnv {
	return &testJobSchedulerEnv{
		scheduledJobsTableName: "defaultdb.scheduled_jobs",
		jobsTableName:          "defaultdb.system_jobs",
		now:                    timeutil.Now(),
	}
}

// newTestHelper creates and initializes appropriate state for a test,
// returning testHelper as well as a cleanup function.
func newTestHelper(t *testing.T) (*testHelper, func()) {
	s, db, kvdb := serverutils.StartServer(t, base.TestServerArgs{})
	sqlDB := sqlutils.MakeSQLRunner(db)

	// Setup test scheduled jobs table.
	env := newTestCronEnv()

	sqlDB.Exec(t, env.getScheduledJobsTableSchema())
	sqlDB.Exec(t, env.getJobsTableSchema())

	return &testHelper{
			env:   env,
			kvDB:  kvdb,
			sqlDB: sqlDB,
			ex:    s.InternalExecutor().(sqlutil.InternalExecutor),
		}, func() {
			if env.scheduledJobsTableName == "defaultdb.scheduled_jobs" {
				sqlDB.Exec(t, "DROP TABLE "+env.scheduledJobsTableName)
			}
			if env.jobsTableName != "defaultdb.system_jobs" {
				sqlDB.Exec(t, "DROP TABLE "+env.jobsTableName)
			}
			s.Stopper().Stop(context.Background())
		}
}

// newScheduledJob is a helper to create job with helper environment.
func (h *testHelper) newScheduledJob(jobName, sql string) *scheduledJob {
	j := newScheduledJob(h.env)
	j.SetScheduleName(jobName)
	j.SetExecutionDetails("test-executor", jobspb.ExecutionArguments{})
	// j.executionArgs.Mutable().Sql = sql
	return j
}

// loads (almost) all columns for the specified scheduled job.
func (h *testHelper) loadJob(t *testing.T, id int64) *scheduledJob {
	j := &scheduledJob{env: h.env}

	require.NoError(t,
		h.kvDB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) (err error) {
			rows, cols, err := h.ex.QueryWithCols(ctx, "sched-load", txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				fmt.Sprintf(
					"SELECT * FROM %s WHERE schedule_id = %d",
					h.env.ScheduledJobsTableName(), id),
			)
			if err != nil {
				return err
			}

			if len(rows) != 1 {
				return errors.Newf("expected 1 row, got %d", len(rows))
			}
			return j.InitFromDatums(rows[0], cols)
		}),
	)
	return j
}
