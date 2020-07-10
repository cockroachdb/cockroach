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

type testHelper struct {
	env   *jobstest.JobSchedulerTestEnv
	kvDB  *kv.DB
	sqlDB *sqlutils.SQLRunner
	ex    sqlutil.InternalExecutor
}

// newTestHelper creates and initializes appropriate state for a test,
// returning testHelper as well as a cleanup function.
func newTestHelper(t *testing.T) (*testHelper, func()) {
	s, db, kvdb := serverutils.StartServer(t, base.TestServerArgs{})
	sqlDB := sqlutils.MakeSQLRunner(db)

	// Setup test scheduled jobs table.
	env := jobstest.NewJobSchedulerTestEnv(jobstest.UseTestTables, timeutil.Now())

	sqlDB.Exec(t, jobstest.GetScheduledJobsTableSchema(env))
	sqlDB.Exec(t, jobstest.GetJobsTableSchema(env))

	return &testHelper{
			env:   env,
			kvDB:  kvdb,
			sqlDB: sqlDB,
			ex:    s.InternalExecutor().(sqlutil.InternalExecutor),
		}, func() {
			sqlDB.Exec(t, "DROP TABLE "+env.ScheduledJobsTableName())
			sqlDB.Exec(t, "DROP TABLE "+env.SystemJobsTableName())
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
