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
	"strconv"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
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
	iodir            string
	server           serverutils.TestServerInterface
	env              *jobstest.JobSchedulerTestEnv
	cfg              *scheduledjobs.JobExecutionConfig
	sqlDB            *sqlutils.SQLRunner
	executeSchedules func() error
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
			th.executeSchedules = func() error {
				defer th.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
				return th.cfg.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
					return fn(ctx, allSchedules, txn)
				})
			}
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
	query := "SELECT id FROM " + h.env.SystemJobsTableName() +
		" WHERE status=$1 AND created_by_type=$2 AND created_by_id=$3"

	testutils.SucceedsSoon(t, func() error {
		// Force newly created job to be adopted and verify it succeeds.
		h.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
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
		require.NoError(t, rows.Scan(&id, &unusedStr, &unusedStr, &unusedTS, &unusedStr, &unusedStr))
		// Query system.scheduled_job table and load those schedules.
		datums, cols, err := h.cfg.InternalExecutor.QueryRowExWithCols(
			context.Background(), "sched-load", nil,
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			"SELECT * FROM system.scheduled_jobs WHERE schedule_id = $1",
			id,
		)
		require.NoError(t, err)
		require.NotNil(t, datums)

		s := jobs.NewScheduledJob(h.env)
		require.NoError(t, s.InitFromDatums(datums, cols))
		schedules = append(schedules, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
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
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	type expectedSchedule struct {
		nameRe     string
		backupStmt string
		period     time.Duration
		runsNow    bool
		shownStmt  string
		paused     bool
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
			query: "CREATE SCHEDULE FOR BACKUP INTO 'nodelocal://0/backup?AWS_SECRET_ACCESS_KEY=neverappears' RECURRING '@hourly'",
			user:  freeUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:     "BACKUP .+",
					backupStmt: "BACKUP INTO 'nodelocal://0/backup?AWS_SECRET_ACCESS_KEY=neverappears' WITH detached",
					shownStmt:  "BACKUP INTO 'nodelocal://0/backup?AWS_SECRET_ACCESS_KEY=redacted' WITH detached",
					period:     time.Hour,
				},
			},
		},
		{
			name:  "full-cluster-with-name",
			query: "CREATE SCHEDULE 'my-backup' FOR BACKUP INTO 'nodelocal://0/backup' RECURRING '@hourly'",
			user:  freeUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:     "my-backup",
					backupStmt: "BACKUP INTO 'nodelocal://0/backup' WITH detached",
					period:     time.Hour,
				},
			},
		},
		{
			name:  "full-cluster-always",
			query: "CREATE SCHEDULE FOR BACKUP INTO 'nodelocal://0/backup' RECURRING '@hourly' FULL BACKUP ALWAYS",
			user:  freeUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:     "BACKUP .+",
					backupStmt: "BACKUP INTO 'nodelocal://0/backup' WITH detached",
					period:     time.Hour,
				},
			},
		},
		{
			name:  "full-cluster",
			query: "CREATE SCHEDULE FOR BACKUP INTO 'nodelocal://0/backup' RECURRING '@hourly'",
			user:  enterpriseUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:     "BACKUP .*",
					backupStmt: "BACKUP INTO LATEST IN 'nodelocal://0/backup' WITH detached",
					period:     time.Hour,
					paused:     true,
				},
				{
					nameRe:     "BACKUP .+",
					backupStmt: "BACKUP INTO 'nodelocal://0/backup' WITH detached",
					period:     24 * time.Hour,
					runsNow:    true,
				},
			},
		},
		{
			name:  "full-cluster-with-name",
			query: "CREATE SCHEDULE 'my-backup' FOR BACKUP INTO 'nodelocal://0/backup' RECURRING '@hourly'",
			user:  enterpriseUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:     "my-backup",
					backupStmt: "BACKUP INTO LATEST IN 'nodelocal://0/backup' WITH detached",
					period:     time.Hour,
					paused:     true,
				},
				{
					nameRe:     "my-backup",
					backupStmt: "BACKUP INTO 'nodelocal://0/backup' WITH detached",
					period:     24 * time.Hour,
					runsNow:    true,
				},
			},
		},
		{
			name:  "full-cluster-with-interleaved-table",
			query: "CREATE SCHEDULE FOR BACKUP INTO 'nodelocal://0/backup?AWS_SECRET_ACCESS_KEY=neverappears' WITH INCLUDE_DEPRECATED_INTERLEAVES RECURRING '@hourly'",
			user:  freeUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:     "BACKUP .+",
					backupStmt: "BACKUP INTO 'nodelocal://0/backup?AWS_SECRET_ACCESS_KEY=neverappears' WITH detached, include_deprecated_interleaves",
					shownStmt:  "BACKUP INTO 'nodelocal://0/backup?AWS_SECRET_ACCESS_KEY=redacted' WITH detached, include_deprecated_interleaves",
					period:     time.Hour,
				},
			},
		},
		{
			name:  "full-cluster-always",
			query: "CREATE SCHEDULE FOR BACKUP INTO 'nodelocal://0/backup' RECURRING '@hourly' FULL BACKUP ALWAYS",
			user:  enterpriseUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:     "BACKUP .+",
					backupStmt: "BACKUP INTO 'nodelocal://0/backup' WITH detached",
					period:     time.Hour,
				},
			},
		},
		{
			name:   "enterprise-license-required-for-incremental",
			query:  "CREATE SCHEDULE FOR BACKUP INTO 'nodelocal://0/backup' RECURRING '@hourly' FULL BACKUP '@weekly'",
			user:   freeUser,
			errMsg: "use of BACKUP INTO LATEST requires an enterprise license",
		},
		{
			name:   "enterprise-license-required-for-revision-history",
			query:  "CREATE SCHEDULE FOR BACKUP INTO 'nodelocal://0/backup' WITH revision_history RECURRING '@hourly'",
			user:   freeUser,
			errMsg: "use of BACKUP with revision_history requires an enterprise license",
		},
		{
			name:   "enterprise-license-required-for-encryption",
			query:  "CREATE SCHEDULE FOR BACKUP INTO 'nodelocal://0/backup'  WITH encryption_passphrase = 'secret' RECURRING '@hourly'",
			user:   freeUser,
			errMsg: "use of BACKUP with encryption requires an enterprise license",
		},
		{
			name:      "full-cluster-with-name-arg",
			query:     `CREATE SCHEDULE $1 FOR BACKUP INTO 'nodelocal://0/backup' WITH revision_history, detached RECURRING '@hourly'`,
			queryArgs: []interface{}{"my_backup_name"},
			user:      enterpriseUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:     "my_backup_name",
					backupStmt: "BACKUP INTO LATEST IN 'nodelocal://0/backup' WITH revision_history, detached",
					period:     time.Hour,
					paused:     true,
				},
				{
					nameRe:     "my_backup_name",
					backupStmt: "BACKUP INTO 'nodelocal://0/backup' WITH revision_history, detached",
					period:     24 * time.Hour,
					runsNow:    true,
				},
			},
		},
		{
			name: "multiple-tables-with-encryption",
			user: enterpriseUser,
			query: `
		CREATE SCHEDULE FOR BACKUP TABLE system.jobs, system.scheduled_jobs INTO 'nodelocal://0/backup'
		WITH encryption_passphrase = 'secret' RECURRING '@weekly'`,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:     "BACKUP .*",
					backupStmt: "BACKUP TABLE system.jobs, system.scheduled_jobs INTO 'nodelocal://0/backup' WITH encryption_passphrase = 'secret', detached",
					shownStmt:  "BACKUP TABLE system.jobs, system.scheduled_jobs INTO 'nodelocal://0/backup' WITH encryption_passphrase = '*****', detached",
					period:     7 * 24 * time.Hour,
				},
			},
		},
		{
			name: "partitioned-backup",
			user: enterpriseUser,
			query: `
		CREATE SCHEDULE FOR BACKUP DATABASE system
    INTO ('nodelocal://0/backup?COCKROACH_LOCALITY=x%3Dy', 'nodelocal://0/backup2?COCKROACH_LOCALITY=default')
		WITH revision_history
    RECURRING '1 2 * * *'
    FULL BACKUP ALWAYS
		WITH SCHEDULE OPTIONS first_run=$1
		`,
			queryArgs: []interface{}{th.env.Now().Add(time.Minute)},
			expectedSchedules: []expectedSchedule{
				{
					nameRe: "BACKUP .+",
					backupStmt: "BACKUP DATABASE system INTO " +
						"('nodelocal://0/backup?COCKROACH_LOCALITY=x%3Dy', 'nodelocal://0/backup2?COCKROACH_LOCALITY=default') " +
						"WITH revision_history, detached",
					period: 24 * time.Hour,
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

			shown := th.sqlDB.QueryStr(t, `SELECT id, command->'backup_statement' FROM [SHOW SCHEDULES]`)
			require.Equal(t, len(tc.expectedSchedules), len(shown))
			shownByID := map[int64]string{}
			for _, i := range shown {
				id, err := strconv.ParseInt(i[0], 10, 64)
				require.NoError(t, err)
				shownByID[id] = i[1]
			}

			// Build a map of expected backup statement to expected schedule.
			expectedByName := make(map[string]expectedSchedule)
			for _, s := range tc.expectedSchedules {
				expectedByName[s.backupStmt] = s
			}

			for _, s := range schedules {
				stmt := getScheduledBackupStatement(t, s.ExecutionArgs())
				expectedSchedule, ok := expectedByName[stmt]
				require.True(t, ok, "could not find matching name for %q", stmt)
				require.Regexp(t, regexp.MustCompile(expectedSchedule.nameRe), s.ScheduleLabel())

				expectedShown := fmt.Sprintf("%q", expectedSchedule.backupStmt)
				if expectedSchedule.shownStmt != "" {
					expectedShown = fmt.Sprintf("%q", expectedSchedule.shownStmt)
				}
				require.Equal(t, expectedShown, shownByID[s.ScheduleID()])

				frequency, err := s.Frequency()
				require.NoError(t, err)
				require.EqualValues(t, expectedSchedule.period, frequency, expectedSchedule)

				require.Equal(t, expectedSchedule.paused, s.IsPaused())
				if expectedSchedule.runsNow {
					require.EqualValues(t, th.env.Now().Round(time.Microsecond), s.ScheduledRunTime())
				}
			}
		})
	}
}

func TestScheduleBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

	expectedSystemTables := make([]string, 0)
	for systemTableName := range GetSystemTablesToIncludeInClusterBackup() {
		expectedSystemTables = append(expectedSystemTables, systemTableName)
	}

	testCases := []struct {
		name         string
		schedule     string
		verifyTables [][]string
	}{
		{
			name:     "cluster-backup",
			schedule: "CREATE SCHEDULE FOR BACKUP INTO $1 RECURRING '@hourly'",
			verifyTables: expectBackupTables(
				dbTables{"db", []string{"t1", "t2", "t3"}},
				dbTables{"other_db", []string{"t1"}},
				dbTables{"system", expectedSystemTables},
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
			require.LessOrEqual(t, 1, len(schedules))

			// Either 1 or two schedules will be created.
			// One of them (incremental) must be paused.
			var full, inc *jobs.ScheduledJob
			if len(schedules) == 1 {
				full = schedules[0]
			} else {
				require.Equal(t, 2, len(schedules))
				full, inc = schedules[0], schedules[1]
				if full.IsPaused() {
					full, inc = inc, full // Swap: inc should be paused.
				}
				require.True(t, inc.IsPaused())
				require.False(t, full.IsPaused())

				// The full should list incremental as a schedule to unpause.
				args := &ScheduledBackupExecutionArgs{}
				require.NoError(t, pbtypes.UnmarshalAny(full.ExecutionArgs().Args, args))
				require.EqualValues(t, inc.ScheduleID(), args.UnpauseOnSuccess)
			}

			defer func() {
				th.sqlDB.Exec(t, "DROP SCHEDULE $1", full.ScheduleID())
				if inc != nil {
					th.sqlDB.Exec(t, "DROP SCHEDULE $1", inc.ScheduleID())
				}
			}()

			// Force the schedule to execute.
			th.env.SetTime(full.NextRun().Add(time.Second))
			require.NoError(t, th.executeSchedules())

			// Wait for the backup complete.
			th.waitForSuccessfulScheduledJob(t, full.ScheduleID())

			if inc != nil {
				// Once the full backup completes, the incremental one should no longer be paused.
				loadedInc, err := jobs.LoadScheduledJob(
					context.Background(), th.env, inc.ScheduleID(), th.cfg.InternalExecutor, nil)
				require.NoError(t, err)
				require.False(t, loadedInc.IsPaused())
			}

			// Verify backup.
			latest, err := ioutil.ReadFile(path.Join(th.iodir, "backup", tc.name, latestFileName))
			require.NoError(t, err)
			backedUp := th.sqlDB.QueryStr(t,
				`SELECT database_name, object_name FROM [SHOW BACKUP $1] WHERE object_type='table' ORDER BY database_name, object_name`,
				fmt.Sprintf("%s/%s", destination, string(latest)))
			require.Equal(t, tc.verifyTables, backedUp)
		})
	}
}

func TestCreateBackupScheduleRequiresAdminRole(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

func TestCreateBackupScheduleCollectionOverwrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	const collectionLocation = "nodelocal://1/collection"

	th.sqlDB.Exec(t, `BACKUP INTO $1`, collectionLocation)

	// Expect that trying to normally create a scheduled backup to this location
	// fails.
	th.sqlDB.ExpectErr(t, "backups already created in",
		"CREATE SCHEDULE FOR BACKUP INTO 'nodelocal://1/collection' RECURRING '@daily';")

	// Expect that we can override this option with the ignore_existing_backups
	// flag.
	th.sqlDB.Exec(t, "CREATE SCHEDULE FOR BACKUP INTO 'nodelocal://1/collection' "+
		"RECURRING '@daily' WITH SCHEDULE OPTIONS ignore_existing_backups;")
}

func TestCreateBackupScheduleInExplicitTxnRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	res := th.sqlDB.Query(t, "SELECT id FROM [SHOW SCHEDULES];")
	require.False(t, res.Next())
	require.NoError(t, res.Err())

	th.sqlDB.Exec(t, "BEGIN;")
	th.sqlDB.Exec(t, "CREATE SCHEDULE FOR BACKUP INTO 'nodelocal://1/collection' RECURRING '@daily';")
	th.sqlDB.Exec(t, "ROLLBACK;")

	res = th.sqlDB.Query(t, "SELECT id FROM [SHOW SCHEDULES];")
	require.False(t, res.Next())
	require.NoError(t, res.Err())
}

// Normally, we issue backups with AOST set to be the scheduled nextRun.
// But if the schedule time is way in the past, the backup will fail.
// This test verifies that scheduled backups will start working
// (eventually), even after the cluster has been down for a long period.
func TestScheduleBackupRecoversFromClusterDown(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, `
CREATE DATABASE db;
USE db;
CREATE TABLE t(a int);
INSERT INTO t values (1), (10), (100);
`)

	loadSchedule := func(t *testing.T, id int64) *jobs.ScheduledJob {
		loaded, err := jobs.LoadScheduledJob(
			context.Background(), th.env, id, th.cfg.InternalExecutor, nil)
		require.NoError(t, err)
		return loaded
	}

	advanceNextRun := func(t *testing.T, id int64, delta time.Duration) {
		// Adjust next run by the specified delta (which maybe negative).
		s := loadSchedule(t, id)
		s.SetNextRun(th.env.Now().Add(delta))
		require.NoError(t, s.Update(context.Background(), th.cfg.InternalExecutor, nil))
	}

	// We'll be manipulating schedule time via th.env, but we can't fool actual backup
	// when it comes to AsOf time.  So, override AsOf backup clause to be the current time.
	useRealTimeAOST := func() func() {
		knobs := th.cfg.TestingKnobs.(*jobs.TestingKnobs)
		knobs.OverrideAsOfClause = func(clause *tree.AsOfClause) {
			expr, err := tree.MakeDTimestampTZ(th.cfg.DB.Clock().PhysicalTime(), time.Microsecond)
			require.NoError(t, err)
			clause.Expr = expr
		}
		return func() {
			knobs.OverrideAsOfClause = nil
		}
	}

	// Create backup schedules for this test.
	// Returns schedule IDs for full and incremental schedules, plus a cleanup function.
	createSchedules := func(t *testing.T, name string) (int64, int64, func()) {
		schedules, err := th.createBackupSchedule(t,
			"CREATE SCHEDULE FOR BACKUP INTO $1 RECURRING '*/5 * * * *'",
			"nodelocal://0/backup/"+name)
		require.NoError(t, err)

		// We expect full & incremental schedule to be created.
		require.Equal(t, 2, len(schedules))

		// Order schedules so that the full schedule is the first one
		fullID, incID := schedules[0].ScheduleID(), schedules[1].ScheduleID()
		if schedules[0].IsPaused() {
			fullID, incID = incID, fullID
		}

		// For the initial backup, we need to ensure that AOST is the current time.
		defer useRealTimeAOST()()

		// Force full backup to execute (this unpauses incremental).
		advanceNextRun(t, fullID, -1*time.Minute)
		require.NoError(t, th.executeSchedules())
		th.waitForSuccessfulScheduledJob(t, fullID)

		// Do the same for the incremental.
		advanceNextRun(t, incID, -1*time.Minute)
		require.NoError(t, th.executeSchedules())
		th.waitForSuccessfulScheduledJob(t, incID)

		return fullID,
			incID,
			func() {
				th.sqlDB.Exec(t, "DROP SCHEDULE $1", schedules[0].ScheduleID())
				th.sqlDB.Exec(t, "DROP SCHEDULE $1", schedules[1].ScheduleID())
			}
	}

	markOldAndSetSchedulesPolicy := func(
		t *testing.T,
		fullID, incID int64,
		onError jobspb.ScheduleDetails_ErrorHandlingBehavior,
	) {
		for _, id := range []int64{fullID, incID} {
			// Pretend we were down for a year.
			s := loadSchedule(t, id)
			s.SetNextRun(s.NextRun().Add(-365 * 24 * time.Hour))
			// Set onError policy to the specified value.
			s.SetScheduleDetails(jobspb.ScheduleDetails{
				OnError: onError,
			})
			require.NoError(t, s.Update(context.Background(), th.cfg.InternalExecutor, nil))
		}
	}

	t.Run("pause", func(t *testing.T) {
		fullID, incID, cleanup := createSchedules(t, "pause")
		defer cleanup()

		markOldAndSetSchedulesPolicy(t, fullID, incID, jobspb.ScheduleDetails_PAUSE_SCHED)

		require.NoError(t, th.executeSchedules())

		// AOST way in the past causes backup planning to fail.  We don't need
		// to wait for any jobs, and the schedules should now be paused.
		for _, id := range []int64{fullID, incID} {
			require.True(t, loadSchedule(t, id).IsPaused())
		}
	})

	metrics := func() *jobs.ExecutorMetrics {
		ex, _, err := jobs.GetScheduledJobExecutor(tree.ScheduledBackupExecutor.InternalName())
		require.NoError(t, err)
		require.NotNil(t, ex.Metrics())
		return ex.Metrics().(*backupMetrics).ExecutorMetrics
	}()

	t.Run("retry", func(t *testing.T) {
		fullID, incID, cleanup := createSchedules(t, "retry")
		defer cleanup()

		markOldAndSetSchedulesPolicy(t, fullID, incID, jobspb.ScheduleDetails_RETRY_SOON)

		require.NoError(t, th.executeSchedules())

		// AOST way in the past causes backup planning to fail.  We don't need
		// to wait for any jobs, and the schedule nextRun should be advanced
		// a bit in the future.
		for _, id := range []int64{fullID, incID} {
			require.True(t, loadSchedule(t, id).NextRun().Sub(th.env.Now()) > 0)
		}

		// We expect that, eventually, both backups would succeed.
		defer useRealTimeAOST()()
		th.env.AdvanceTime(time.Hour)
		initialSucceeded := metrics.NumSucceeded.Count()

		require.NoError(t, th.executeSchedules())

		testutils.SucceedsSoon(t, func() error {
			delta := metrics.NumSucceeded.Count() - initialSucceeded
			if delta == 2 {
				return nil
			}
			return errors.Newf("expected 2 backup to succeed, got %d", delta)
		})
	})

	t.Run("reschedule", func(t *testing.T) {
		fullID, incID, cleanup := createSchedules(t, "reschedule")
		defer cleanup()

		markOldAndSetSchedulesPolicy(t, fullID, incID, jobspb.ScheduleDetails_RETRY_SCHED)

		require.NoError(t, th.executeSchedules())

		// AOST way in the past causes backup planning to fail.  We don't need
		// to wait for any jobs, and the schedule nextRun should be advanced
		// to the next scheduled recurrence.
		for _, id := range []int64{fullID, incID} {
			s := loadSchedule(t, id)
			require.EqualValues(t,
				cronexpr.MustParse(s.ScheduleExpr()).Next(th.env.Now()).Round(time.Microsecond),
				s.NextRun())
		}

		// We expect that, eventually, both backups would succeed.
		defer useRealTimeAOST()()
		th.env.AdvanceTime(25 * time.Hour) // Go to next day to guarantee daily triggers.
		initialSucceeded := metrics.NumSucceeded.Count()

		require.NoError(t, th.executeSchedules())

		testutils.SucceedsSoon(t, func() error {
			delta := metrics.NumSucceeded.Count() - initialSucceeded
			if delta == 2 {
				return nil
			}
			return errors.Newf("expected 2 backup to succeed, got %d", delta)
		})
	})
}
