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
	"github.com/gogo/protobuf/types"
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

const timestampTZLayout = "2006-01-02 15:04:05.000000"
const timestampTZWithTZLayout = "2006-01-02 15:04:05.000000 -0700 MST"

func (t *testJobSchedulerEnv) NowExpr() string {
	return fmt.Sprintf("TIMESTAMPTZ '%s'", t.now.Format(timestampTZLayout))
}

// Returns schema for the scheduled jobs table.
func (t *testJobSchedulerEnv) getScheduledJobsTableSchema() string {
	return strings.Replace(sqlbase.ScheduledJobsTableSchema,
		"system.scheduled_jobs", t.scheduledJobsTableName, 1)
}

// Returns schema for the jobs table.
func (t *testJobSchedulerEnv) getJobsTableSchema() string {
	if t.jobsTableName == "system.jobs" {
		return sqlbase.JobsTableSchema
	}
	return strings.Replace(sqlbase.JobsTableSchema, "system.jobs", t.jobsTableName, 1)
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

// NewScheduledJob is a helper to create job with helper environment.
func (h *testHelper) newScheduledJob(t *testing.T, jobName, sql string) *ScheduledJob {
	j := NewScheduledJob(h.env)
	j.SetScheduleName(jobName)
	any, err := types.MarshalAny(&jobspb.SqlStatementExecutionArg{Statement: sql})
	require.NoError(t, err)
	j.SetExecutionDetails(InlineExecutorName, jobspb.ExecutionArguments{Args: any})
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
