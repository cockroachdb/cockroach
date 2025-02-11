// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

type execSchedulesFn func(ctx context.Context, maxSchedules int64) error
type testHelper struct {
	env           *jobstest.JobSchedulerTestEnv
	server        serverutils.TestServerInterface
	execSchedules execSchedulesFn
	cfg           *scheduledjobs.JobExecutionConfig
	sqlDB         *sqlutils.SQLRunner
}

// newTestHelper creates and initializes appropriate state for a test,
// returning testHelper as well as a cleanup function.
// This test helper does not use system tables for jobs and scheduled jobs.
// It creates separate tables for the test, that are then dropped when cleanup
// function executes.  Because of this, the execution of job scheduler daemon
// is disabled by this test helper.
// If you want to run daemon, invoke it directly.
//
// The testHelper will accelerate the adoption and cancellation loops inside of
// the registry.
func newTestHelper(t *testing.T) (*testHelper, func()) {
	return newTestHelperForTables(t, jobstest.UseTestTables, nil)
}

func newTestHelperWithServerArgs(
	t *testing.T, argsFn func(args *base.TestServerArgs),
) (*testHelper, func()) {
	return newTestHelperForTables(t, jobstest.UseTestTables, argsFn)
}

func newTestHelperForTables(
	t *testing.T, envTableType jobstest.EnvTablesType, argsFn func(args *base.TestServerArgs),
) (*testHelper, func()) {
	var execSchedules execSchedulesFn

	// Setup test scheduled jobs table.
	env := jobstest.NewJobSchedulerTestEnv(envTableType, timeutil.Now())
	knobs := &TestingKnobs{
		JobSchedulerEnv: env,
		TakeOverJobsScheduling: func(daemon func(ctx context.Context, maxSchedules int64) error) {
			execSchedules = daemon
		},
	}

	args := base.TestServerArgs{
		Knobs: base.TestingKnobs{JobsTestingKnobs: knobs},
	}
	if argsFn != nil {
		argsFn(&args)
	}

	s, db, _ := serverutils.StartServer(t, args)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, "CREATE USER testuser")

	if envTableType == jobstest.UseTestTables {
		sqlDB.Exec(t, jobstest.GetScheduledJobsTableSchema(env))
		sqlDB.Exec(t, jobstest.GetJobsTableSchema(env))
	}

	return &testHelper{
			env:    env,
			server: s,
			cfg: &scheduledjobs.JobExecutionConfig{
				Settings:     s.ClusterSettings(),
				DB:           s.InternalDB().(isql.DB),
				TestingKnobs: knobs,
			},
			sqlDB:         sqlDB,
			execSchedules: execSchedules,
		}, func() {
			if envTableType == jobstest.UseTestTables {
				sqlDB.Exec(t, "DROP TABLE "+env.SystemJobsTableName())
				sqlDB.Exec(t, "DROP TABLE "+env.ScheduledJobsTableName())
			}
			s.Stopper().Stop(context.Background())
		}
}

// newScheduledJob is a helper to create scheduled job with helper environment.
func (h *testHelper) newScheduledJob(t *testing.T, scheduleLabel, sql string) *ScheduledJob {
	j := NewScheduledJob(h.env)
	j.SetScheduleLabel(scheduleLabel)
	j.SetOwner(username.TestUserName())
	any, err := types.MarshalAny(&jobspb.SqlStatementExecutionArg{Statement: sql})
	require.NoError(t, err)
	j.SetScheduleDetails(jobstest.AddDummyScheduleDetails(jobspb.ScheduleDetails{}))
	j.SetExecutionDetails(InlineExecutorName, jobspb.ExecutionArguments{Args: any})
	return j
}

// newScheduledJobForExecutor is a helper to create scheduled job for the specified
// executor and its args.
func (h *testHelper) newScheduledJobForExecutor(
	scheduleLabel, executorName string, executorArgs *types.Any,
) *ScheduledJob {
	j := NewScheduledJob(h.env)
	j.SetScheduleLabel(scheduleLabel)
	j.SetOwner(username.TestUserName())
	j.SetExecutionDetails(executorName, jobspb.ExecutionArguments{Args: executorArgs})
	j.SetScheduleDetails(jobstest.AddDummyScheduleDetails(jobspb.ScheduleDetails{}))
	return j
}

// loadSchedule loads  all columns for the specified scheduled job.
func (h *testHelper) loadSchedule(t *testing.T, id jobspb.ScheduleID) *ScheduledJob {
	j := NewScheduledJob(h.env)
	row, cols, err := h.cfg.DB.Executor().QueryRowExWithCols(
		context.Background(), "sched-load", nil,
		sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(
			"SELECT * FROM %s WHERE schedule_id = %d",
			h.env.ScheduledJobsTableName(), id),
	)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.NoError(t, j.InitFromDatums(row, cols))
	return j
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
		executorRegistry.Lock()
		defer executorRegistry.Unlock()
		delete(executorRegistry.factories, name)
		delete(executorRegistry.executors, name)
	}
}

// addFakeJob adds a fake job associated with the specified scheduleID.
// Returns the id of the newly created job.
func addFakeJob(
	t *testing.T, h *testHelper, scheduleID jobspb.ScheduleID, state State, txn isql.Txn,
) jobspb.JobID {
	datums, err := txn.QueryRowEx(context.Background(), "fake-job", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(`
INSERT INTO %s (created_by_type, created_by_id, status)
VALUES ($1, $2, $3)
RETURNING id`,
			h.env.SystemJobsTableName(),
		),
		CreatedByScheduledJobs, scheduleID, state,
	)
	require.NoError(t, err)
	require.NotNil(t, datums)
	return jobspb.JobID(tree.MustBeDInt(datums[0]))
}
