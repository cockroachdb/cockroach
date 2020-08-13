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
	gosql "database/sql"
	"fmt"
	"io/ioutil"
	"net/url"
	"path"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
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
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

const allSchedules = 0

// testHelper starts a server, and arranges for job scheduling daemon to
// use jobstest.JobSchedulerTestEnv.
// This helper also arranges for the manual override of scheduling logic
// via executeSchedules callback.
type execSchedulesFn = func(ctx context.Context, maxSchedules int64, txn *kv.Txn) error
type testHelper struct {
	iodir            string
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
		env:   jobstest.NewJobSchedulerTestEnv(jobstest.UseSystemTables, timeutil.Now()),
		iodir: dir,
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
	t *testing.T, query string, args ...interface{},
) ([]*jobs.ScheduledJob, error) {
	// Execute statement and get the list of schedule IDs created by the query.
	ctx := context.Background()
	rows, err := h.sqlDB.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	var unusedStr string
	var unusedTS *time.Time
	var schedules []*jobs.ScheduledJob
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id, &unusedStr, &unusedTS, &unusedStr, &unusedStr))
		// Query system.scheduled_job table and load those schedules.
		datums, cols, err := h.cfg.InternalExecutor.QueryWithCols(
			context.Background(), "sched-load", nil,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"SELECT * FROM system.scheduled_jobs WHERE schedule_id = $1",
			id,
		)
		require.NoError(t, err)
		require.Equal(t, 1, len(datums))

		s := jobs.NewScheduledJob(h.env)
		require.NoError(t, s.InitFromDatums(datums[0], cols))
		schedules = append(schedules, s)
	}

	return schedules, nil
}

func getScheduledBackupStatement(t *testing.T, arg *jobspb.ExecutionArguments) string {
	var backup ScheduledBackupExecutionArgs
	require.NoError(t, pbtypes.UnmarshalAny(arg.Args, &backup))
	return backup.BackupStatement
}

type userType bool

const freeUser userType = false
const enterpriseUser userType = true

func (t userType) String() string {
	if t == freeUser {
		return "free user"
	}
	return "enterprise user"
}

// This test examines serialized representation of backup schedule arguments
// when the scheduled backup statement executes.  This test does not concern
// itself with the actual scheduling and the execution of those backups.
func TestSerializesScheduledBackupExecutionArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	th, cleanup := newTestHelper(t)
	defer cleanup()

	type expectedSchedule struct {
		nameRe     string
		backupStmt string
		period     time.Duration
		runsNow    bool
	}

	testCases := []struct {
		name              string
		query             string
		queryArgs         []interface{}
		user              userType
		expectedSchedules []expectedSchedule
		errMsg            string
	}{
		{
			name:  "full-cluster",
			query: "CREATE SCHEDULE FOR BACKUP INTO 'somewhere' RECURRING '@hourly'",
			user:  freeUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:     "BACKUP .+",
					backupStmt: "BACKUP INTO 'somewhere' WITH detached",
					period:     time.Hour,
				},
			},
		},
		{
			name:  "full-cluster-with-name",
			query: "CREATE SCHEDULE 'my-backup' FOR BACKUP INTO 'somewhere' RECURRING '@hourly'",
			user:  freeUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:     "my-backup",
					backupStmt: "BACKUP INTO 'somewhere' WITH detached",
					period:     time.Hour,
				},
			},
		},
		{
			name:  "full-cluster-always",
			query: "CREATE SCHEDULE FOR BACKUP INTO 'somewhere' RECURRING '@hourly' FULL BACKUP ALWAYS",
			user:  freeUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:     "BACKUP .+",
					backupStmt: "BACKUP INTO 'somewhere' WITH detached",
					period:     time.Hour,
				},
			},
		},
		{
			name:  "full-cluster",
			query: "CREATE SCHEDULE FOR BACKUP INTO 'somewhere' RECURRING '@hourly'",
			user:  enterpriseUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:     "BACKUP .*: INCREMENTAL",
					backupStmt: "BACKUP INTO LATEST IN 'somewhere' WITH detached",
					period:     time.Hour,
				},
				{
					nameRe:     "BACKUP .+",
					backupStmt: "BACKUP INTO 'somewhere' WITH detached",
					period:     24 * time.Hour,
					runsNow:    true,
				},
			},
		},
		{
			name:  "full-cluster-with-name",
			query: "CREATE SCHEDULE 'my-backup' FOR BACKUP INTO 'somewhere' RECURRING '@hourly'",
			user:  enterpriseUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:     "my-backup: INCREMENTAL",
					backupStmt: "BACKUP INTO LATEST IN 'somewhere' WITH detached",
					period:     time.Hour,
				},
				{
					nameRe:     "my-backup",
					backupStmt: "BACKUP INTO 'somewhere' WITH detached",
					period:     24 * time.Hour,
					runsNow:    true,
				},
			},
		},
		{
			name:  "full-cluster-always",
			query: "CREATE SCHEDULE FOR BACKUP INTO 'somewhere' RECURRING '@hourly' FULL BACKUP ALWAYS",
			user:  enterpriseUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:     "BACKUP .+",
					backupStmt: "BACKUP INTO 'somewhere' WITH detached",
					period:     time.Hour,
				},
			},
		},
		{
			name:   "enterprise-license-required-for-incremental",
			query:  "CREATE SCHEDULE FOR BACKUP INTO 'somewhere' RECURRING '@hourly' FULL BACKUP '@weekly'",
			user:   freeUser,
			errMsg: "use of BACKUP INTO LATEST requires an enterprise license",
		},
		{
			name:   "enterprise-license-required-for-revision-history",
			query:  "CREATE SCHEDULE FOR BACKUP INTO 'somewhere' WITH revision_history RECURRING '@hourly'",
			user:   freeUser,
			errMsg: "use of revision_history requires an enterprise license",
		},
		{
			name:   "enterprise-license-required-for-encryption",
			query:  "CREATE SCHEDULE FOR BACKUP INTO 'somewhere'  WITH encryption_passphrase='secret' RECURRING '@hourly'",
			user:   freeUser,
			errMsg: "use of encryption requires an enterprise license",
		},
		{
			name:      "full-cluster-with-name-arg",
			query:     `CREATE SCHEDULE $1 FOR BACKUP INTO 'somewhere' WITH revision_history, detached RECURRING '@hourly'`,
			queryArgs: []interface{}{"my_backup_name"},
			user:      enterpriseUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:     "my_backup_name: INCREMENTAL",
					backupStmt: "BACKUP INTO LATEST IN 'somewhere' WITH revision_history, detached",
					period:     time.Hour,
				},
				{
					nameRe:     "my_backup_name",
					backupStmt: "BACKUP INTO 'somewhere' WITH revision_history, detached",
					period:     24 * time.Hour,
					runsNow:    true,
				},
			},
		},
		{
			name: "multiple-tables-with-encryption",
			user: enterpriseUser,
			query: `
		CREATE SCHEDULE FOR BACKUP TABLE db.*, other_db.foo INTO 'somewhere'
		WITH encryption_passphrase='secret' RECURRING '@weekly'`,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:     "BACKUP .*",
					backupStmt: "BACKUP TABLE db.*, other_db.foo INTO 'somewhere' WITH encryption_passphrase='secret', detached",
					period:     7 * 24 * time.Hour,
				},
			},
		},
		{
			name: "partitioned-backup",
			user: enterpriseUser,
			query: `
		CREATE SCHEDULE FOR BACKUP DATABASE db1, db2 INTO ('somewhere', 'anywhere')
		WITH revision_history 
    RECURRING '1 2 * * *'
    FULL BACKUP ALWAYS
		WITH EXPERIMENTAL SCHEDULE OPTIONS first_run=$1
		`,
			queryArgs: []interface{}{th.env.Now().Add(time.Minute)},
			expectedSchedules: []expectedSchedule{
				{
					nameRe:     "BACKUP .+",
					backupStmt: "BACKUP DATABASE db1, db2 INTO ('somewhere', 'anywhere') WITH revision_history, detached",
					period:     24 * time.Hour,
				},
			},
		},
		{
			name:   "missing-destination-placeholder",
			query:  `CREATE SCHEDULE FOR BACKUP TABLE t INTO $1 RECURRING '@hourly'`,
			errMsg: "failed to evaluate backup destination paths",
		},
		{
			name:   "missing-encryption-placeholder",
			user:   enterpriseUser,
			query:  `CREATE SCHEDULE FOR BACKUP INTO 'foo' WITH encryption_passphrase=$1 RECURRING '@hourly'`,
			errMsg: "failed to evaluate backup encryption_passphrase",
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s-%s", tc.name, tc.user), func(t *testing.T) {
			defer th.clearSchedules(t)

			if tc.user == freeUser {
				defer utilccl.TestingDisableEnterprise()()
			} else {
				defer utilccl.TestingEnableEnterprise()()
			}

			schedules, err := th.createBackupSchedule(t, tc.query, tc.queryArgs...)
			if len(tc.errMsg) > 0 {
				require.True(t, testutils.IsError(err, tc.errMsg),
					"expected error to match %q, found %q instead", tc.errMsg, err.Error())
				return
			}

			require.NoError(t, err)
			require.Equal(t, len(tc.expectedSchedules), len(schedules))

			// Build a map of expected backup statement to expected schedule.
			expectedByName := make(map[string]expectedSchedule)
			for _, s := range tc.expectedSchedules {
				expectedByName[s.backupStmt] = s
			}

			for _, s := range schedules {
				stmt := getScheduledBackupStatement(t, s.ExecutionArgs())
				expectedSchedule, ok := expectedByName[stmt]
				require.True(t, ok, "could not find matching name for %q", stmt)
				require.Regexp(t, regexp.MustCompile(expectedSchedule.nameRe), s.ScheduleName())

				frequency, err := s.Frequency()
				require.NoError(t, err)
				require.EqualValues(t, expectedSchedule.period, frequency, expectedSchedule)

				if expectedSchedule.runsNow {
					require.EqualValues(t, th.env.Now().Round(time.Microsecond), s.ScheduledRunTime())
				}
			}
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
			schedule: "CREATE SCHEDULE FOR BACKUP INTO $1 RECURRING '@hourly' FULL BACKUP ALWAYS",
			verifyTables: expectBackupTables(
				dbTables{"db", []string{"t1", "t2", "t3"}},
				dbTables{"other_db", []string{"t1"}},
				dbTables{"system", fullClusterSystemTables},
			),
		},
		{
			name:         "tables-backup-with-history",
			schedule:     "CREATE SCHEDULE FOR BACKUP db.t2, db.t3 INTO $1 WITH revision_history RECURRING '@hourly' FULL BACKUP ALWAYS",
			verifyTables: expectBackupTables(dbTables{"db", []string{"t2", "t3"}}),
		},
		{
			name:     "table-backup-in-different-dbs",
			schedule: "CREATE SCHEDULE FOR BACKUP db.t1, other_db.t1, db.t3 INTO $1 RECURRING '@hourly' FULL BACKUP ALWAYS",
			verifyTables: expectBackupTables(
				dbTables{"db", []string{"t1", "t3"}},
				dbTables{"other_db", []string{"t1"}},
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			destination := "nodelocal://0/backup/" + tc.name
			schedules, err := th.createBackupSchedule(t, tc.schedule, destination)
			require.NoError(t, err)
			require.EqualValues(t, 1, len(schedules))
			defer th.sqlDB.Exec(t, "DROP SCHEDULE $1", schedules[0].ScheduleID())

			// Force the schedule to execute.
			th.env.SetTime(schedules[0].NextRun().Add(time.Second))
			require.NoError(t,
				th.cfg.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
					return th.executeSchedules(ctx, allSchedules, txn)
				}))

			// Wait for the backup complete.
			th.waitForSuccessfulScheduledJob(t, schedules[0].ScheduleID())

			// Verify backup.
			latest, err := ioutil.ReadFile(path.Join(th.iodir, "backup", tc.name, latestFileName))
			require.NoError(t, err)
			backedUp := th.sqlDB.QueryStr(t,
				`SELECT database_name, table_name FROM [SHOW BACKUP $1] ORDER BY database_name, table_name`,
				fmt.Sprintf("%s/%s", destination, string(latest)))
			require.Equal(t, tc.verifyTables, backedUp)
		})
	}
}

func TestCreateBackupScheduleRequiresAdminRole(t *testing.T) {
	defer leaktest.AfterTest(t)()
	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, `CREATE USER testuser`)
	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, th.server.ServingSQLAddr(),
		"TestCreateSchedule-testuser", url.User("testuser"),
	)
	defer cleanupFunc()
	testuser, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, testuser.Close())
	}()

	_, err = testuser.Exec("CREATE SCHEDULE FOR BACKUP INTO 'somewhere' RECURRING '@daily'")
	require.Error(t, err)
}
