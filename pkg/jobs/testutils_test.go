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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

type testHelper struct {
	env     *jobstest.JobSchedulerTestEnv
	cfg     *scheduledjobs.JobExecutionConfig
	sqlDB   *sqlutils.SQLRunner
	stopper *stop.Stopper
}

// newTestHelper creates and initializes appropriate state for a test,
// returning testHelper as well as a cleanup function.
func newTestHelper(t *testing.T) (*testHelper, func()) {
	s, db, kvdb := serverutils.StartServer(t, base.TestServerArgs{})
	cfg := &scheduledjobs.JobExecutionConfig{
		Settings:         s.ClusterSettings(),
		InternalExecutor: s.InternalExecutor().(sqlutil.InternalExecutor),
		DB:               kvdb,
		PlanHookMaker:    nil,
	}
	sqlDB := sqlutils.MakeSQLRunner(db)

	// Setup test scheduled jobs table.
	env := jobstest.NewJobSchedulerTestEnv(jobstest.UseTestTables, timeutil.Now())

	sqlDB.Exec(t, jobstest.GetScheduledJobsTableSchema(env))
	sqlDB.Exec(t, jobstest.GetJobsTableSchema(env))

	restoreRegistry := settings.TestingSaveRegistry()
	return &testHelper{
			env:     env,
			cfg:     cfg,
			sqlDB:   sqlDB,
			stopper: s.Stopper(),
		}, func() {
			sqlDB.Exec(t, "DROP TABLE "+env.SystemJobsTableName())
			sqlDB.Exec(t, "DROP TABLE "+env.ScheduledJobsTableName())
			s.Stopper().Stop(context.Background())
			restoreRegistry()
		}
}

// newScheduledJob is a helper to create scheduled job with helper environment.
func (h *testHelper) newScheduledJob(t *testing.T, scheduleName, sql string) *ScheduledJob {
	j := NewScheduledJob(h.env)
	j.SetScheduleName(scheduleName)
	any, err := types.MarshalAny(&jobspb.SqlStatementExecutionArg{Statement: sql})
	require.NoError(t, err)
	j.SetExecutionDetails(InlineExecutorName, jobspb.ExecutionArguments{Args: any})
	return j
}

// newScheduledJobForExecutor is a helper to create scheduled job for the specified
// executor and its args.
func (h *testHelper) newScheduledJobForExecutor(
	scheduleName, executorName string, executorArgs *types.Any,
) *ScheduledJob {
	j := NewScheduledJob(h.env)
	j.SetScheduleName(scheduleName)
	j.SetExecutionDetails(executorName, jobspb.ExecutionArguments{Args: executorArgs})
	return j
}

// loadSchedule loads  all columns for the specified scheduled job.
func (h *testHelper) loadSchedule(t *testing.T, id int64) *ScheduledJob {
	j := NewScheduledJob(h.env)
	rows, cols, err := h.cfg.InternalExecutor.QueryWithCols(
		context.Background(), "sched-load", nil,
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

// startJobSchedulerDaemon starts running job scheduler daemon.
func (h *testHelper) startJobSchedulerDaemon() {
	StartJobSchedulerDaemon(context.Background(), h.stopper, h.cfg, h.env)
}

// registerScopedScheduledJobExecutor registers executor under the name,
// and returns a function which, when invoked, de-registers this executor.
func registerScopedScheduledJobExecutor(name string, ex ScheduledJobExecutor) func() {
	RegisterScheduledJobExecutorFactory(
		name,
		func() (ScheduledJobExecutor, error) {
			return ex, nil
		})
	return func() {
		delete(registeredExecutorFactories, name)
	}
}
