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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

// testJobSchedulerEnv is a test implementation of cron environment.
// allows injection fake time, as well as use of non-system tables.
type testJobSchedulerEnv struct {
	scheduledJobsTableName string
	jobsTableName          string
	mu                     struct {
		syncutil.Mutex
		now time.Time
	}
}

func (e *testJobSchedulerEnv) ScheduledJobsTableName() string {
	return e.scheduledJobsTableName
}

func (e *testJobSchedulerEnv) SystemJobsTableName() string {
	return e.jobsTableName
}

func (e *testJobSchedulerEnv) Now() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.mu.now
}

func (e *testJobSchedulerEnv) AdvanceTime(d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.now = e.mu.now.Add(d)
}

func (e *testJobSchedulerEnv) SetTime(t time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.now = t
}

const timestampTZLayout = "2006-01-02 15:04:05.000000"

func (e *testJobSchedulerEnv) NowExpr() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return fmt.Sprintf("TIMESTAMPTZ '%s'", e.mu.now.Format(timestampTZLayout))
}

// Returns schema for the scheduled jobs table.
func (e *testJobSchedulerEnv) getScheduledJobsTableSchema() string {
	return strings.Replace(sqlbase.ScheduledJobsTableSchema,
		"system.scheduled_jobs", e.scheduledJobsTableName, 1)
}

// Returns schema for the jobs table.
func (e *testJobSchedulerEnv) getJobsTableSchema() string {
	if e.jobsTableName == "system.jobs" {
		return sqlbase.JobsTableSchema
	}
	return strings.Replace(sqlbase.JobsTableSchema, "system.jobs", e.jobsTableName, 1)
}

type testHelper struct {
	env   *testJobSchedulerEnv
	kvDB  *kv.DB
	sqlDB *sqlutils.SQLRunner
	ex    sqlutil.InternalExecutor
}

func newTestCronEnv() *testJobSchedulerEnv {
	env := &testJobSchedulerEnv{
		scheduledJobsTableName: "defaultdb.scheduled_jobs",
		jobsTableName:          "defaultdb.system_jobs",
	}
	env.mu.now = timeutil.Now()
	return env
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

// NewScheduledJob is a helper to create job with helper environment.
func (h *testHelper) newScheduledJob(t *testing.T, scheduleName, sql string) *ScheduledJob {
	j := NewScheduledJob(h.env)
	j.SetScheduleName(scheduleName)
	any, err := types.MarshalAny(&jobspb.SqlStatementExecutionArg{Statement: sql})
	require.NoError(t, err)
	j.SetExecutionDetails(InlineExecutorName, jobspb.ExecutionArguments{Args: any})
	return j
}

func (h *testHelper) newScheduledJobForExecutor(
	scheduleName, executorName string, executorArgs *types.Any,
) *ScheduledJob {
	j := NewScheduledJob(h.env)
	j.SetScheduleName(scheduleName)
	j.SetExecutionDetails(executorName, jobspb.ExecutionArguments{Args: executorArgs})
	return j
}

// loads (almost) all columns for the specified scheduled job.
func (h *testHelper) loadJob(t *testing.T, id int64) *ScheduledJob {
	j := NewScheduledJob(h.env)
	rows, cols, err := h.ex.QueryWithCols(context.Background(), "sched-load", nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		fmt.Sprintf(
			"SELECT * FROM %s WHERE schedule_id = %d",
			h.env.ScheduledJobsTableName(), id),
	)
	require.NoError(t, err)

	require.Equal(t, 1, len(rows))
	require.NoError(t, j.InitFromDatums(rows[0], cols))
	return j
}

func registerScopedScheduledJobExecutor(name string, ex ScheduledJobExecutor) func() {
	RegisterScheduledJobExecutorFactory(
		name,
		func(_ sqlutil.InternalExecutor) (ScheduledJobExecutor, error) {
			return ex, nil
		})
	return func() {
		delete(registeredExecutorFactories, name)
	}
}
