// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/gogo/protobuf/types"
	"github.com/gorhill/cronexpr"
	"github.com/stretchr/testify/require"
)

const allSchedules = 0

// testHelper starts a server, and arranges for job scheduling daemon to
// use jobstest.JobSchedulerTestEnv.
// This helper also arranges for the manual override of scheduling logic
// via executeSchedules callback.
type execSchedulesFn = func(ctx context.Context, maxSchedules int64, txn *kv.Txn) error
type testHelper struct {
	server           serverutils.TestServerInterface
	env              *jobstest.JobSchedulerTestEnv
	cfg              *scheduledjobs.JobExecutionConfig
	sqlDB            *sqlutils.SQLRunner
	executeSchedules execSchedulesFn
}

// newTestHelper creates and initializes appropriate state for a test,
// returning testHelper as well as a cleanup function.
func newTestHelper(t *testing.T) (*testHelper, func()) {
	dir, dirCleanupFn := testutils.TempDir(t)

	th := &testHelper{
		env: jobstest.NewJobSchedulerTestEnv(jobstest.UseSystemTables, timeutil.Now()),
	}

	knobs := &jobs.TestingKnobs{
		JobSchedulerEnv: th.env,
		TakeOverJobsScheduling: func(fn execSchedulesFn) {
			th.executeSchedules = fn
		},

		CaptureJobExecutionConfig: func(config *scheduledjobs.JobExecutionConfig) {
			th.cfg = config
		},
	}

	args := base.TestServerArgs{
		ExternalIODir: dir,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: knobs,
		},
	}
	s, db, _ := serverutils.StartServer(t, args)
	require.NotNil(t, th.cfg)
	th.sqlDB = sqlutils.MakeSQLRunner(db)
	th.server = s

	return th, func() {
		dirCleanupFn()
		s.Stopper().Stop(context.Background())
	}
}

func (h *testHelper) clearSchedules(t *testing.T) {
	t.Helper()
	h.sqlDB.Exec(t, "DELETE FROM system.scheduled_jobs WHERE true")
}

func (h *testHelper) waitForSuccessfulScheduledJob(t *testing.T, scheduleID int64) {
	// Force newly created job to be adopted and verify it succeeds.
	h.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()

	query := "SELECT id FROM " + h.env.SystemJobsTableName() +
		" WHERE status=$1 AND created_by_type=$2 AND created_by_id=$3"

	testutils.SucceedsSoon(t, func() error {
		var unused int64
		return h.sqlDB.DB.QueryRowContext(context.Background(),
			query, jobs.StatusSucceeded, jobs.CreatedByScheduledJobs, scheduleID).Scan(&unused)
	})
}

// createBackupSchedule executes specified "CREATE SCHEDULE FOR BACKUP" query, with
// the provided arguments.  Returns the list of created schedules
func (h *testHelper) createBackupSchedule(
	query string, args ...interface{},
) ([]*jobs.ScheduledJob, error) {
	// Execute statement and get the list of schedule IDs created by the query.
	ctx := context.Background()
	rows, err := h.sqlDB.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	var unusedDescr string
	var unusedTS *time.Time
	var id int64
	var scheduleIDs string
	for rows.Next() {
		if err := rows.Scan(&id, &unusedTS, &unusedDescr); err != nil {
			return nil, err
		}
		scheduleIDs += fmt.Sprintf("%d,", id)
	}

	// Query system.scheduled_job table and load those schedules.
	datums, cols, err := h.cfg.InternalExecutor.QueryWithCols(
		context.Background(), "sched-load", nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"SELECT * FROM system.scheduled_jobs WHERE schedule_id IN ($1)",
		scheduleIDs[:len(scheduleIDs)-1],
	)
	if err != nil {
		return nil, err
	}

	var schedules []*jobs.ScheduledJob
	for _, row := range datums {
		s := jobs.NewScheduledJob(h.env)
		if err := s.InitFromDatums(row, cols); err != nil {
			return nil, err
		}
		schedules = append(schedules, s)
	}
	return schedules, nil
}

// This test examines serialized representation of backup schedule arguments
// when the scheduled backup statement executes.  This test does not concern
// itself with the actual scheduling and the execution of those backups.
func TestSerializesScheduledBackupExecutionArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	th, cleanup := newTestHelper(t)
	defer cleanup()

	testCases := []struct {
		name         string
		query        string
		queryArgs    []interface{}
		scheduleName string
		nextRun      time.Time
		backupStmt   string
		errMsg       string
	}{
		{
			name:         "full-cluster",
			query:        "CREATE SCHEDULE FOR BACKUP TO 'somewhere' RECURRING NEVER",
			backupStmt:   "BACKUP TO 'somewhere' WITH detached",
			scheduleName: "FULL BACKUP OF CLUSTER",
		},
		{
			name:         "full-cluster-with-name-arg",
			query:        `CREATE SCHEDULE $1 FOR BACKUP TO 'somewhere' WITH revision_history, detached RECURRING '@hourly'`,
			queryArgs:    []interface{}{"my_backup_name"},
			scheduleName: "my_backup_name",
			nextRun:      cronexpr.MustParse("@hourly").Next(th.env.Now()).Round(time.Microsecond),
			backupStmt:   "BACKUP TO 'somewhere' WITH revision_history, detached",
		},
		{
			name: "multiple-tables-with-encryption",
			query: `
CREATE SCHEDULE FOR BACKUP TABLE db.*, other_db.foo TO 'somewhere'
WITH encryption_passphrase='secret' RECURRING '@daily'`,
			scheduleName: "FULL BACKUP OF TABLE db.*, other_db.foo",
			nextRun:      cronexpr.MustParse("@daily").Next(th.env.Now()).Round(time.Microsecond),
			backupStmt:   `BACKUP TABLE db.*, other_db.foo TO 'somewhere' WITH encryption_passphrase='secret', detached`,
		},
		{
			name: "multiple-db-multi-destination",
			query: `
CREATE SCHEDULE FOR BACKUP DATABASE db1, db2 TO ('somewhere', 'anywhere')
WITH revision_history RECURRING '1 2 3 4 5'
WITH EXPERIMENTAL SCHEDULE OPTIONS first_run=$1
`,
			queryArgs:    []interface{}{th.env.Now().Add(time.Minute)},
			scheduleName: "FULL BACKUP OF DATABASE db1, db2",
			backupStmt:   `BACKUP DATABASE db1, db2 TO ('somewhere', 'anywhere') WITH revision_history, detached`,
			nextRun:      th.env.Now().Add(time.Minute).Round(time.Microsecond),
		},
		{
			name:   "missing-destination-placeholder",
			query:  `CREATE SCHEDULE FOR BACKUP TABLE t TO $1 RECURRING NEVER`,
			errMsg: "failed to evaluate backup destination paths",
		},
		{
			name:   "missing-encryption-placeholder",
			query:  `CREATE SCHEDULE FOR BACKUP TABLE t TO 'foo' WITH encryption_passphrase=$1 RECURRING NEVER`,
			errMsg: "failed to evaluate backup encryption_passphrase",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer th.clearSchedules(t)

			schedules, err := th.createBackupSchedule(tc.query, tc.queryArgs...)
			if len(tc.errMsg) > 0 {
				require.True(t, testutils.IsError(err, tc.errMsg))
				return
			}

			require.Equal(t, 1, len(schedules))
			schedule := schedules[0]

			require.Equal(t, scheduledBackupExecutorName, schedule.ExecutorType())
			require.EqualValues(t, tc.nextRun, schedule.NextRun())
			var arg ScheduledBackupExecutionArgs
			require.NoError(t, types.UnmarshalAny(schedule.ExecutionArgs().Args, &arg))
			require.Equal(t, ScheduledBackupExecutionArgs_FULL, arg.BackupType)
			require.Equal(t, tc.backupStmt, arg.BackupStatement)
		})
	}
}

func TestScheduleBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, `
CREATE DATABASE db;
USE db;

CREATE TABLE t1(a int);
INSERT INTO t1 values (1), (10), (100);

CREATE TABLE t2(b int);
INSERT INTO t2 VALUES (3), (2), (1);

CREATE TABLE t3(c int);
INSERT INTO t3 VALUES (5), (5), (7);

CREATE DATABASE other_db;
USE other_db;

CREATE TABLE t1(a int);
INSERT INTO t1 values (-1), (10), (-100);
`)

	// We'll be manipulating schedule time via th.env, but we can't fool actual backup
	// when it comes to AsOf time.  So, override AsOf backup clause to be the current time.
	th.cfg.TestingKnobs.(*jobs.TestingKnobs).OverrideAsOfClause = func(clause *tree.AsOfClause) {
		expr, err := tree.MakeDTimestampTZ(th.cfg.DB.Clock().PhysicalTime(), time.Microsecond)
		require.NoError(t, err)
		clause.Expr = expr
	}

	type dbTables struct {
		db     string
		tables []string
	}
	expectBackupTables := func(dbTbls ...dbTables) [][]string {
		sort.Slice(dbTbls, func(i, j int) bool { return dbTbls[i].db < dbTbls[j].db })
		var res [][]string
		for _, dbt := range dbTbls {
			sort.Strings(dbt.tables)
			for _, tbl := range dbt.tables {
				res = append(res, []string{dbt.db, tbl})
			}
		}
		return res
	}

	testCases := []struct {
		name         string
		schedule     string
		verifyTables [][]string
	}{
		{
			name:     "cluster-backup",
			schedule: "CREATE SCHEDULE FOR BACKUP TO $1 RECURRING '@hourly'",
			verifyTables: expectBackupTables(
				dbTables{"db", []string{"t1", "t2", "t3"}},
				dbTables{"other_db", []string{"t1"}},
				dbTables{"system", fullClusterSystemTables},
			),
		},
		{
			name:         "tables-backup-with-history",
			schedule:     "CREATE SCHEDULE FOR BACKUP db.t2, db.t3 TO $1 WITH revision_history RECURRING '@hourly'",
			verifyTables: expectBackupTables(dbTables{"db", []string{"t2", "t3"}}),
		},
		{
			name:     "table-backup-in-different-dbs",
			schedule: "CREATE SCHEDULE FOR BACKUP db.t1, other_db.t1, db.t3 TO $1 RECURRING '@hourly'",
			verifyTables: expectBackupTables(
				dbTables{"db", []string{"t1", "t3"}},
				dbTables{"other_db", []string{"t1"}},
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			destination := "nodelocal://0/backup/" + tc.name
			schedules, err := th.createBackupSchedule(tc.schedule, destination)
			require.NoError(t, err)
			require.EqualValues(t, 1, len(schedules))

			// Force the schedule to execute.
			th.env.SetTime(schedules[0].NextRun().Add(time.Second))
			require.NoError(t,
				th.cfg.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
					return th.executeSchedules(ctx, allSchedules, txn)
				}))

			// Wait for the backup to complete.
			th.waitForSuccessfulScheduledJob(t, schedules[0].ScheduleID())

			// Verify backup.
			backedUp := th.sqlDB.QueryStr(t,
				`SELECT database_name, table_name FROM [SHOW BACKUP $1] ORDER BY database_name, table_name`,
				destination)
			require.Equal(t, tc.verifyTables, backedUp)
		})
	}
}
