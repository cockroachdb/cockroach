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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// testCronEnv is a test implementation of cron environment.
// allows injection fake time, as well as use of non-system tables.
type testCronEnv struct {
	crontabTableName string
	jobsTableName    string
	now              time.Time
}

func (t *testCronEnv) CrontabTableName() string {
	return t.crontabTableName
}

func (t *testCronEnv) SystemJobsTableName() string {
	return t.jobsTableName
}

func (t *testCronEnv) Now() time.Time {
	return t.now
}

func (t *testCronEnv) NowExpr() string {
	return fmt.Sprintf("TIMESTAMPTZ '%s'",
		t.now.Format("2020-01-02 03:04:05.999999999"))
}

// Returns schema for the crontab table.
func (t *testCronEnv) getCrontabTableSchema() string {
	return strings.Replace(sqlbase.CronTableSchema, "system.crontab", t.crontabTableName, 1)
}

// Return s schema for the jobs table.
func (t *testCronEnv) getJobsTableSchema() string {
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
	env   *testCronEnv
	kvDB  *kv.DB
	sqlDB *sqlutils.SQLRunner
	ex    sqlutil.InternalExecutor
}

func newTestCronEnv() *testCronEnv {
	return &testCronEnv{
		crontabTableName: "defaultdb.crontab",
		jobsTableName:    "defaultdb.system_jobs",
		now:              timeutil.Now(),
	}
}

// newTestHelper creates and initializes appropriate state for a test,
// returning testHelper as well as a cleanup function.
func newTestHelper(t *testing.T) (*testHelper, func()) {
	s, db, kvdb := serverutils.StartServer(t, base.TestServerArgs{})
	sqlDB := sqlutils.MakeSQLRunner(db)

	// Setup test crontab table.
	env := newTestCronEnv()

	sqlDB.Exec(t, env.getCrontabTableSchema())
	sqlDB.Exec(t, env.getJobsTableSchema())

	return &testHelper{
			env:   env,
			kvDB:  kvdb,
			sqlDB: sqlDB,
			ex:    s.InternalExecutor().(sqlutil.InternalExecutor),
		}, func() {
			if env.crontabTableName == "defaultdb.crontab" {
				sqlDB.Exec(t, "DROP TABLE "+env.crontabTableName)
			}
			if env.jobsTableName != "defaultdb.system_jobs" {
				sqlDB.Exec(t, "DROP TABLE "+env.jobsTableName)
			}
			s.Stopper().Stop(context.Background())
		}
}

// newScheduledJob is a helper to create job with helper environment.
func (h *testHelper) newScheduledJob(jobName, sql string) *scheduledJob {
	j := &scheduledJob{env: h.env}
	j.jobName.Set(jobName)
	j.jobDetails.Mutable().Sql = sql
	return j
}

// loads (almost) all columns for the specified scheduled job.
func (h *testHelper) loadJob(t *testing.T, id int64) *scheduledJob {
	j := &scheduledJob{env: h.env}

	require.NoError(t,
		h.kvDB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) (err error) {
			row, err := h.ex.QueryRowEx(ctx, "cron-load", txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				fmt.Sprintf(
					"SELECT sched_id, job_name, owner, schedule_expr, next_run, job_details,"+
						"exec_spec, change_info FROM %s WHERE sched_id = %d",
					h.env.CrontabTableName(), id),
			)
			if err != nil {
				return err
			}

			return j.FromDatums(row, &j.schedID, &j.jobName, &j.owner, &j.schedExpr,
				&j.nextRun, &j.jobDetails, &j.execSpec, &j.changeInfo)
		}),
	)
	return j
}
