// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs/schedulebase"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
)

const allSchedules = 0

// testHelper starts a server, and arranges for job scheduling daemon to
// use jobstest.JobSchedulerTestEnv.
// This helper also arranges for the manual override of scheduling logic
// via executeSchedules callback.
type execSchedulesFn = func(ctx context.Context, maxSchedules int64) error
type testHelper struct {
	iodir            string
	server           serverutils.ApplicationLayerInterface
	env              *jobstest.JobSchedulerTestEnv
	cfg              *scheduledjobs.JobExecutionConfig
	sqlDB            *sqlutils.SQLRunner
	executeSchedules func() error
}

func (th *testHelper) protectedTimestamps() protectedts.Manager {
	return th.server.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
}

// newTestHelper creates and initializes appropriate state for a test,
// returning testHelper as well as a cleanup function.
func newTestHelper(t *testing.T) (*testHelper, func()) {
	dir, dirCleanupFn := testutils.TempDir(t)

	th := &testHelper{
		env: jobstest.NewJobSchedulerTestEnv(
			jobstest.UseSystemTables, timeutil.Now(), tree.ScheduledBackupExecutor),
		iodir: dir,
	}

	knobs := &jobs.TestingKnobs{
		JobSchedulerEnv: th.env,
		TakeOverJobsScheduling: func(fn execSchedulesFn) {
			th.executeSchedules = func() error {
				defer th.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
				return fn(context.Background(), allSchedules)
			}
		},
		CaptureJobExecutionConfig: func(config *scheduledjobs.JobExecutionConfig) {
			th.cfg = config
		},
		IntervalOverrides: jobs.NewTestingKnobsWithShortIntervals().IntervalOverrides,
	}

	args := base.TestServerArgs{
		Locality:      roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "of-france"}}},
		Settings:      cluster.MakeClusterSettings(),
		ExternalIODir: dir,
		// Some scheduled backup tests fail when run within a tenant. More
		// investigation is required. Tracked with #76378.
		DefaultTestTenant: base.TODOTestTenantDisabled,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: knobs,
		},
	}
	jobs.PollJobsMetricsInterval.Override(context.Background(), &args.Settings.SV, 250*time.Millisecond)
	s, db, _ := serverutils.StartServer(t, args)
	require.NotNil(t, th.cfg)
	th.sqlDB = sqlutils.MakeSQLRunner(db)
	th.server = s.ApplicationLayer()
	th.sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`) // speeds up test

	return th, func() {
		dirCleanupFn()
		s.Stopper().Stop(context.Background())
	}
}

func (h *testHelper) setOverrideAsOfClauseKnob(t *testing.T) {
	// We'll be manipulating schedule time via th.env, but we can't fool actual
	// backup when it comes to AsOf time.  So, override AsOf backup clause to be
	// the current time.
	h.cfg.TestingKnobs.(*jobs.TestingKnobs).OverrideAsOfClause = func(clause *tree.AsOfClause, _ time.Time) {
		expr, err := tree.MakeDTimestampTZ(h.cfg.DB.KV().Clock().PhysicalTime(), time.Microsecond)
		require.NoError(t, err)
		clause.Expr = expr
	}
}

func (h *testHelper) loadSchedule(t *testing.T, scheduleID jobspb.ScheduleID) *jobs.ScheduledJob {
	t.Helper()

	loaded, err := jobs.ScheduledJobDB(h.internalDB()).
		Load(context.Background(), h.env, scheduleID)
	require.NoError(t, err)
	return loaded
}

func (h *testHelper) clearSchedules(t *testing.T) {
	t.Helper()
	h.sqlDB.Exec(t, "DELETE FROM system.scheduled_jobs WHERE true")
}

func (h *testHelper) waitForSuccessfulScheduledJob(t *testing.T, scheduleID jobspb.ScheduleID) {
	query := "SELECT id FROM " + h.env.SystemJobsTableName() +
		" WHERE status=$1 AND created_by_type=$2 AND created_by_id=$3"

	testutils.SucceedsSoon(t, func() error {
		// Force newly created job to be adopted and verify it succeeds.
		h.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
		var unused int64
		return h.sqlDB.DB.QueryRowContext(context.Background(),
			query, jobs.StateSucceeded, jobs.CreatedByScheduledJobs, scheduleID).Scan(&unused)
	})
}

func (h *testHelper) waitForSuccessfulScheduledJobCount(
	t *testing.T, scheduleID jobspb.ScheduleID, expectedCount int,
) {
	query := "SELECT count(*) FROM " + h.env.SystemJobsTableName() +
		" WHERE status=$1 AND created_by_type=$2 AND created_by_id=$3"

	testutils.SucceedsSoon(t, func() error {
		// Force newly created job to be adopted and verify it succeeds.
		h.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
		var count int
		err := h.sqlDB.DB.QueryRowContext(context.Background(),
			query, jobs.StateSucceeded, jobs.CreatedByScheduledJobs, scheduleID).Scan(&count)
		require.NoError(t, err)
		if count != expectedCount {
			return errors.Newf("expected %d jobs; found %d", expectedCount, count)
		}
		return nil
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
		datums, cols, err := h.cfg.DB.Executor().QueryRowExWithCols(
			context.Background(), "sched-load", nil,
			sessiondata.NodeUserSessionDataOverride,
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

func (th *testHelper) internalDB() descs.DB {
	return th.server.InternalDB().(descs.DB)
}

func getScheduledBackupStatement(t *testing.T, arg *jobspb.ExecutionArguments) string {
	var backup backuppb.ScheduledBackupExecutionArgs
	require.NoError(t, pbtypes.UnmarshalAny(arg.Args, &backup))
	return backup.BackupStatement
}

func getScheduledBackupChainProtectedTimestamp(t *testing.T, arg *jobspb.ExecutionArguments) bool {
	var backup backuppb.ScheduledBackupExecutionArgs
	require.NoError(t, pbtypes.UnmarshalAny(arg.Args, &backup))
	return backup.ChainProtectedTimestampRecords
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

func TestScheduledTableBackupNameQualification(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, `
CREATE DATABASE mydb;
USE mydb;

CREATE TABLE t1(a int);
INSERT INTO t1 values (1), (10), (100);

CREATE TABLE t2(b int);
INSERT INTO t2 VALUES (3), (2), (1);

CREATE TABLE t3(c int);
INSERT INTO t3 VALUES (5), (5), (7);

CREATE TABLE "my.tbl"(d int);

CREATE SCHEMA myschema;
CREATE TABLE myschema.mytbl(a int);

CREATE DATABASE other_db;
CREATE TABLE other_db.t1(a int);
`)

	testCases := []struct {
		name               string
		query              string
		expectedBackupStmt string
	}{
		{
			name:               "fully-qualified-table-name",
			query:              "CREATE SCHEDULE FOR BACKUP mydb.public.t1 INTO $1 RECURRING '@hourly'",
			expectedBackupStmt: "BACKUP TABLE mydb.public.t1 INTO %s'%s' WITH OPTIONS (detached)",
		},
		{
			name:               "schema-qualified-table-name",
			query:              "CREATE SCHEDULE FOR BACKUP public.t1 INTO $1 RECURRING '@hourly'",
			expectedBackupStmt: "BACKUP TABLE mydb.public.t1 INTO %s'%s' WITH OPTIONS (detached)",
		},
		{
			name:               "uds-qualified-table-name",
			query:              "CREATE SCHEDULE FOR BACKUP myschema.mytbl INTO $1 RECURRING '@hourly'",
			expectedBackupStmt: "BACKUP TABLE mydb.myschema.mytbl INTO %s'%s' WITH OPTIONS (detached)",
		},
		{
			name:               "db-qualified-table-name",
			query:              "CREATE SCHEDULE FOR BACKUP mydb.t1 INTO $1 RECURRING '@hourly'",
			expectedBackupStmt: "BACKUP TABLE mydb.public.t1 INTO %s'%s' WITH OPTIONS (detached)",
		},
		{
			name:               "unqualified-table-name",
			query:              "CREATE SCHEDULE FOR BACKUP t1 INTO $1 RECURRING '@hourly'",
			expectedBackupStmt: "BACKUP TABLE mydb.public.t1 INTO %s'%s' WITH OPTIONS (detached)",
		},
		{
			name:               "unqualified-table-name-with-symbols",
			query:              `CREATE SCHEDULE FOR BACKUP "my.tbl" INTO $1 RECURRING '@hourly'`,
			expectedBackupStmt: `BACKUP TABLE mydb.public."my.tbl" INTO %s'%s' WITH OPTIONS (detached)`,
		},
		{
			name:               "table-names-from-different-db",
			query:              "CREATE SCHEDULE FOR BACKUP t1, other_db.t1 INTO $1 RECURRING '@hourly'",
			expectedBackupStmt: "BACKUP TABLE mydb.public.t1, other_db.public.t1 INTO %s'%s' WITH OPTIONS (detached)",
		},
		{
			name:               "unqualified-all-tables-selectors",
			query:              "CREATE SCHEDULE FOR BACKUP * INTO $1 RECURRING '@hourly'",
			expectedBackupStmt: "BACKUP TABLE mydb.public.* INTO %s'%s' WITH OPTIONS (detached)",
		},
		{
			name:               "all-tables-selectors-with-user-defined-schema",
			query:              "CREATE SCHEDULE FOR BACKUP myschema.* INTO $1 RECURRING '@hourly'",
			expectedBackupStmt: "BACKUP TABLE mydb.myschema.* INTO %s'%s' WITH OPTIONS (detached)",
		},
		{
			name:               "partially-qualified-all-tables-selectors-with-different-db",
			query:              "CREATE SCHEDULE FOR BACKUP other_db.* INTO $1 RECURRING '@hourly'",
			expectedBackupStmt: "BACKUP TABLE other_db.public.* INTO %s'%s' WITH OPTIONS (detached)",
		},
		{
			name:               "fully-qualified-all-tables-selectors-with-multiple-dbs",
			query:              "CREATE SCHEDULE FOR BACKUP *, other_db.* INTO $1 RECURRING '@hourly'",
			expectedBackupStmt: "BACKUP TABLE mydb.public.*, other_db.public.* INTO %s'%s' WITH OPTIONS (detached)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer th.clearSchedules(t)
			defer utilccl.TestingDisableEnterprise()()

			destination := "nodelocal://1/backup/" + tc.name
			schedules, err := th.createBackupSchedule(t, tc.query, destination)
			require.NoError(t, err)

			for i, s := range schedules {
				var latest string
				if i == 0 {
					latest = "LATEST IN "
				}
				stmt := getScheduledBackupStatement(t, s.ExecutionArgs())
				require.Equal(t, fmt.Sprintf(tc.expectedBackupStmt, latest, destination), stmt, "schedule %d", i)
			}
		})
	}
}

// This test examines serialized representation of backup schedule arguments
// when the scheduled backup statement executes.  This test does not concern
// itself with the actual scheduling and the execution of those backups.
func TestSerializesScheduledBackupExecutionArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, `SET CLUSTER SETTING schedules.backup.gc_protection.enabled = true`)

	type expectedSchedule struct {
		nameRe                        string
		backupStmt                    string
		period                        time.Duration
		runsNow                       bool
		shownStmt                     string
		paused                        bool
		chainProtectedTimestampRecord bool
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
			query: "CREATE SCHEDULE FOR BACKUP INTO 'nodelocal://1/backup?AWS_SECRET_ACCESS_KEY=neverappears' RECURRING '@hourly'",
			user:  freeUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:                        "BACKUP .*",
					backupStmt:                    "BACKUP INTO LATEST IN 'nodelocal://1/backup?AWS_SECRET_ACCESS_KEY=neverappears' WITH OPTIONS (detached)",
					shownStmt:                     "BACKUP INTO LATEST IN 'nodelocal://1/backup?AWS_SECRET_ACCESS_KEY=redacted' WITH OPTIONS (detached)",
					period:                        time.Hour,
					paused:                        true,
					chainProtectedTimestampRecord: true,
				},
				{
					nameRe:                        "BACKUP .+",
					backupStmt:                    "BACKUP INTO 'nodelocal://1/backup?AWS_SECRET_ACCESS_KEY=neverappears' WITH OPTIONS (detached)",
					shownStmt:                     "BACKUP INTO 'nodelocal://1/backup?AWS_SECRET_ACCESS_KEY=redacted' WITH OPTIONS (detached)",
					period:                        24 * time.Hour,
					runsNow:                       true,
					chainProtectedTimestampRecord: true,
				},
			},
		},
		{
			name:  "full-cluster-with-name",
			query: "CREATE SCHEDULE 'my-backup' FOR BACKUP INTO 'nodelocal://1/backup' RECURRING '@hourly'",
			user:  freeUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:                        "my-backup",
					backupStmt:                    "BACKUP INTO LATEST IN 'nodelocal://1/backup' WITH OPTIONS (detached)",
					period:                        time.Hour,
					paused:                        true,
					chainProtectedTimestampRecord: true,
				},
				{
					nameRe:                        "my-backup",
					backupStmt:                    "BACKUP INTO 'nodelocal://1/backup' WITH OPTIONS (detached)",
					period:                        24 * time.Hour,
					runsNow:                       true,
					chainProtectedTimestampRecord: true,
				},
			},
		},
		{
			name:  "full-cluster-always",
			query: "CREATE SCHEDULE FOR BACKUP INTO 'nodelocal://1/backup' RECURRING '@hourly' FULL BACKUP ALWAYS",
			user:  freeUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:     "BACKUP .+",
					backupStmt: "BACKUP INTO 'nodelocal://1/backup' WITH OPTIONS (detached)",
					period:     time.Hour,
				},
			},
		},
		{
			name:  "full-cluster",
			query: "CREATE SCHEDULE FOR BACKUP INTO 'nodelocal://1/backup' RECURRING '@hourly'",
			user:  enterpriseUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:                        "BACKUP .*",
					backupStmt:                    "BACKUP INTO LATEST IN 'nodelocal://1/backup' WITH OPTIONS (detached)",
					period:                        time.Hour,
					paused:                        true,
					chainProtectedTimestampRecord: true,
				},
				{
					nameRe:                        "BACKUP .+",
					backupStmt:                    "BACKUP INTO 'nodelocal://1/backup' WITH OPTIONS (detached)",
					period:                        24 * time.Hour,
					runsNow:                       true,
					chainProtectedTimestampRecord: true,
				},
			},
		},
		{
			name:  "full-cluster-with-name",
			query: "CREATE SCHEDULE 'my-backup' FOR BACKUP INTO 'nodelocal://1/backup' RECURRING '@hourly'",
			user:  enterpriseUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:                        "my-backup",
					backupStmt:                    "BACKUP INTO LATEST IN 'nodelocal://1/backup' WITH OPTIONS (detached)",
					period:                        time.Hour,
					paused:                        true,
					chainProtectedTimestampRecord: true,
				},
				{
					nameRe:                        "my-backup",
					backupStmt:                    "BACKUP INTO 'nodelocal://1/backup' WITH OPTIONS (detached)",
					period:                        24 * time.Hour,
					runsNow:                       true,
					chainProtectedTimestampRecord: true,
				},
			},
		},
		{
			name:  "full-cluster-always",
			query: "CREATE SCHEDULE FOR BACKUP INTO 'nodelocal://1/backup' WITH revision_history RECURRING '@hourly' FULL BACKUP ALWAYS",
			user:  enterpriseUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:     "BACKUP .+",
					backupStmt: "BACKUP INTO 'nodelocal://1/backup' WITH OPTIONS (revision_history = true, detached)",
					period:     time.Hour,
				},
			},
		},
		{
			name:  "full-cluster-remote-incremental-location",
			query: "CREATE SCHEDULE FOR BACKUP INTO 'nodelocal://1/backup' WITH incremental_location = 'nodelocal://1/incremental' RECURRING '@hourly'",
			user:  enterpriseUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:                        "BACKUP .*",
					backupStmt:                    "BACKUP INTO LATEST IN 'nodelocal://1/backup' WITH OPTIONS (detached, incremental_location = 'nodelocal://1/incremental')",
					period:                        time.Hour,
					paused:                        true,
					chainProtectedTimestampRecord: true,
				},
				{
					nameRe:                        "BACKUP .+",
					backupStmt:                    "BACKUP INTO 'nodelocal://1/backup' WITH OPTIONS (detached)",
					period:                        24 * time.Hour,
					runsNow:                       true,
					chainProtectedTimestampRecord: true,
				},
			},
		},
		{
			name: "multiple-tables-with-revision-history",
			user: enterpriseUser,
			query: `
		CREATE SCHEDULE FOR BACKUP TABLE system.jobs, system.scheduled_jobs INTO 'nodelocal://1/backup'
		WITH revision_history RECURRING '@hourly'`,
			expectedSchedules: []expectedSchedule{
				{
					nameRe: "BACKUP .*",
					backupStmt: "BACKUP TABLE system.public.jobs, " +
						"system.public.scheduled_jobs INTO LATEST IN 'nodelocal://1/backup' WITH" +
						" OPTIONS (revision_history = true, detached)",
					period:                        time.Hour,
					paused:                        true,
					chainProtectedTimestampRecord: true,
				},
				{
					nameRe: "BACKUP .+",
					backupStmt: "BACKUP TABLE system.public.jobs, " +
						"system.public.scheduled_jobs INTO 'nodelocal://1/backup' WITH OPTIONS (revision_history = true, detached)",
					period:                        24 * time.Hour,
					runsNow:                       true,
					chainProtectedTimestampRecord: true,
				},
			},
		},
		{
			name: "database-with-revision-history",
			user: enterpriseUser,
			query: `
		CREATE SCHEDULE FOR BACKUP DATABASE system INTO 'nodelocal://1/backup'
		WITH revision_history RECURRING '@hourly'`,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:                        "BACKUP .*",
					backupStmt:                    "BACKUP DATABASE system INTO LATEST IN 'nodelocal://1/backup' WITH OPTIONS (revision_history = true, detached)",
					period:                        time.Hour,
					paused:                        true,
					chainProtectedTimestampRecord: true,
				},
				{
					nameRe:                        "BACKUP .+",
					backupStmt:                    "BACKUP DATABASE system INTO 'nodelocal://1/backup' WITH OPTIONS (revision_history = true, detached)",
					period:                        24 * time.Hour,
					runsNow:                       true,
					chainProtectedTimestampRecord: true,
				},
			},
		},
		{
			name: "wildcard-with-revision-history",
			user: enterpriseUser,
			query: `
		CREATE SCHEDULE FOR BACKUP TABLE system.* INTO 'nodelocal://1/backup'
		WITH revision_history RECURRING '@hourly'`,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:                        "BACKUP .*",
					backupStmt:                    "BACKUP TABLE system.public.* INTO LATEST IN 'nodelocal://1/backup' WITH OPTIONS (revision_history = true, detached)",
					period:                        time.Hour,
					paused:                        true,
					chainProtectedTimestampRecord: true,
				},
				{
					nameRe:                        "BACKUP .+",
					backupStmt:                    "BACKUP TABLE system.public.* INTO 'nodelocal://1/backup' WITH OPTIONS (revision_history = true, detached)",
					period:                        24 * time.Hour,
					runsNow:                       true,
					chainProtectedTimestampRecord: true,
				},
			},
		},
		{
			name:      "full-cluster-with-name-arg",
			query:     `CREATE SCHEDULE $1 FOR BACKUP INTO 'nodelocal://1/backup' WITH revision_history, detached RECURRING '@hourly'`,
			queryArgs: []interface{}{"my_backup_name"},
			user:      enterpriseUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:                        "my_backup_name",
					backupStmt:                    "BACKUP INTO LATEST IN 'nodelocal://1/backup' WITH OPTIONS (revision_history = true, detached)",
					period:                        time.Hour,
					paused:                        true,
					chainProtectedTimestampRecord: true,
				},
				{
					nameRe:                        "my_backup_name",
					backupStmt:                    "BACKUP INTO 'nodelocal://1/backup' WITH OPTIONS (revision_history = true, detached)",
					period:                        24 * time.Hour,
					runsNow:                       true,
					chainProtectedTimestampRecord: true,
				},
			},
		},
		{
			name:      "full-cluster-with-revision-history-arg",
			query:     `CREATE SCHEDULE my_backup_name FOR BACKUP INTO 'nodelocal://1/backup' WITH revision_history = $1 RECURRING '@hourly'`,
			queryArgs: []interface{}{true},
			user:      enterpriseUser,
			expectedSchedules: []expectedSchedule{
				{
					nameRe:                        "my_backup_name",
					backupStmt:                    "BACKUP INTO LATEST IN 'nodelocal://1/backup' WITH OPTIONS (revision_history = true, detached)",
					period:                        time.Hour,
					paused:                        true,
					chainProtectedTimestampRecord: true,
				},
				{
					nameRe:                        "my_backup_name",
					backupStmt:                    "BACKUP INTO 'nodelocal://1/backup' WITH OPTIONS (revision_history = true, detached)",
					period:                        24 * time.Hour,
					runsNow:                       true,
					chainProtectedTimestampRecord: true,
				},
			},
		},
		{
			name: "multiple-tables-with-encryption",
			user: enterpriseUser,
			query: `
		CREATE SCHEDULE FOR BACKUP TABLE system.jobs, system.scheduled_jobs INTO 'nodelocal://1/backup'
		WITH revision_history, encryption_passphrase = 'secret' RECURRING '@weekly'`,
			expectedSchedules: []expectedSchedule{
				{
					nameRe: "BACKUP .*",
					backupStmt: "BACKUP TABLE system.public.jobs, " +
						"system.public.scheduled_jobs INTO 'nodelocal://1/backup' WITH" +
						" OPTIONS (revision_history = true, encryption_passphrase = 'secret', detached)",
					shownStmt: "BACKUP TABLE system.public.jobs, " +
						"system.public.scheduled_jobs INTO 'nodelocal://1/backup' WITH" +
						" OPTIONS (revision_history = true, encryption_passphrase = '*****', detached)",
					period: 7 * 24 * time.Hour,
				},
			},
		},
		{
			name: "partitioned-backup",
			user: enterpriseUser,
			query: `
		CREATE SCHEDULE FOR BACKUP DATABASE system
    INTO ('nodelocal://1/backup?COCKROACH_LOCALITY=x%3Dy', 'nodelocal://1/backup2?COCKROACH_LOCALITY=default')
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
						"('nodelocal://1/backup?COCKROACH_LOCALITY=x%3Dy', 'nodelocal://1/backup2?COCKROACH_LOCALITY=default') " +
						"WITH OPTIONS (revision_history = true, detached)",
					period: 24 * time.Hour,
				},
			},
		},
		{
			name: "exec-loc",
			user: enterpriseUser,
			query: `
		CREATE SCHEDULE FOR BACKUP DATABASE system
    INTO 'nodelocal://1/backup'
		WITH revision_history, execution locality = 'region=of-france'
    RECURRING '1 2 * * *'
    FULL BACKUP ALWAYS
		WITH SCHEDULE OPTIONS first_run=$1
		`,
			queryArgs: []interface{}{th.env.Now().Add(time.Minute)},
			expectedSchedules: []expectedSchedule{
				{
					nameRe: "BACKUP .+",
					backupStmt: "BACKUP DATABASE system INTO 'nodelocal://1/backup' " +
						"WITH OPTIONS (revision_history = true, detached, execution locality = 'region=of-france')",
					period: 24 * time.Hour,
				},
			},
		},
		{
			name:   "missing-destination-placeholder",
			query:  `CREATE SCHEDULE FOR BACKUP TABLE system.public.jobs INTO $1 RECURRING '@hourly'`,
			errMsg: "failed to evaluate backup destination paths",
		},
		{
			name:   "missing-encryption-placeholder",
			user:   enterpriseUser,
			query:  `CREATE SCHEDULE FOR BACKUP INTO 'foo' WITH encryption_passphrase=$1 RECURRING '@hourly'`,
			errMsg: "failed to evaluate backup encryption_passphrase",
		},
	}

	for i, tc := range testCases {
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

			shown := th.sqlDB.QueryStr(t, `SELECT id, command->'backup_statement' FROM [SHOW SCHEDULES] WHERE command->>'backup_statement' LIKE 'BACKUP%'`)
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
				require.True(t, ok, "in test case %d, could not find matching name for %q", i, stmt)
				require.Regexp(t, regexp.MustCompile(expectedSchedule.nameRe), s.ScheduleLabel())

				expectedShown := fmt.Sprintf("%q", expectedSchedule.backupStmt)
				if expectedSchedule.shownStmt != "" {
					expectedShown = fmt.Sprintf("%q", expectedSchedule.shownStmt)
				}
				require.Equal(t, expectedShown, shownByID[int64(s.ScheduleID())])

				frequency, err := s.Frequency()
				require.NoError(t, err)
				require.EqualValues(t, expectedSchedule.period, frequency, expectedSchedule)

				require.Equal(t, expectedSchedule.paused, s.IsPaused())
				if expectedSchedule.runsNow {
					require.EqualValues(t, th.env.Now().Round(time.Microsecond), s.ScheduledRunTime())
				}
				require.Equal(t, expectedSchedule.chainProtectedTimestampRecord,
					getScheduledBackupChainProtectedTimestamp(t, s.ExecutionArgs()))
			}
		})
	}
}

// TestIncrementalScheduleBackupOnPreviousRunning tests that incremental
// schedules always set `on_previous_running` to `wait` regardless of what the
// full schedule is configured with.
// For an explanation please refer to `create_scheduled_backup.go` where we
// configure the option for the incremental schedule.
func TestIncrementalScheduleBackupOnPreviousRunning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, `
CREATE DATABASE db;
USE db;

CREATE TABLE t1(a int);
INSERT INTO t1 values (1), (10), (100);
`)
	th.setOverrideAsOfClauseKnob(t)

	checkScheduleDetailsWaitOption := func(schedules []*jobs.ScheduledJob,
		expectedFullOption, expectedIncOption jobspb.ScheduleDetails_WaitBehavior) {
		require.Len(t, schedules, 2)
		full, inc := schedules[0], schedules[1]
		if full.IsPaused() {
			full, inc = inc, full // Swap: inc should be paused.
		}
		require.Equal(t, expectedFullOption, full.ScheduleDetails().Wait)
		require.Equal(t, expectedIncOption, inc.ScheduleDetails().Wait)
	}

	t.Run("on_previous_running=start", func(t *testing.T) {
		schedule := `CREATE SCHEDULE FOR BACKUP INTO $1 RECURRING '@hourly' WITH SCHEDULE OPTIONS on_previous_running = 'start'`
		destination := "nodelocal://1/backup/"
		schedules, err := th.createBackupSchedule(t, schedule, destination)
		require.NoError(t, err)
		require.Len(t, schedules, 2)
		checkScheduleDetailsWaitOption(schedules, jobspb.ScheduleDetails_NO_WAIT, jobspb.ScheduleDetails_WAIT)
	})

	t.Run("on_previous_running=skip", func(t *testing.T) {
		schedule := `CREATE SCHEDULE FOR BACKUP INTO $1 RECURRING '@hourly' WITH SCHEDULE OPTIONS on_previous_running = 'skip'`
		destination := "nodelocal://1/backup/"
		schedules, err := th.createBackupSchedule(t, schedule, destination)
		require.NoError(t, err)
		require.Len(t, schedules, 2)
		checkScheduleDetailsWaitOption(schedules, jobspb.ScheduleDetails_SKIP, jobspb.ScheduleDetails_WAIT)
	})

	t.Run("on_previous_running=wait", func(t *testing.T) {
		schedule := `CREATE SCHEDULE FOR BACKUP INTO $1 RECURRING '@hourly' WITH SCHEDULE OPTIONS on_previous_running = 'wait'`
		destination := "nodelocal://1/backup/"
		schedules, err := th.createBackupSchedule(t, schedule, destination)
		require.NoError(t, err)
		require.Len(t, schedules, 2)
		checkScheduleDetailsWaitOption(schedules, jobspb.ScheduleDetails_WAIT, jobspb.ScheduleDetails_WAIT)
	})
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

	th.setOverrideAsOfClauseKnob(t)

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
		for _, enabled := range []bool{true, false} {
			testName := fmt.Sprintf("%s_chaining=%t", tc.name, enabled)
			t.Run(testName, func(t *testing.T) {
				th.sqlDB.Exec(t, fmt.Sprintf(`SET CLUSTER SETTING schedules.backup.gc_protection.enabled = %t`, enabled))
				destination := "nodelocal://1/backup/" + testName
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
					args := &backuppb.ScheduledBackupExecutionArgs{}
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
					loadedInc, err := jobs.ScheduledJobDB(th.internalDB()).
						Load(context.Background(), th.env, inc.ScheduleID())
					require.NoError(t, err)

					require.False(t, loadedInc.IsPaused())
				}

				// Verify backup.
				backedUp := th.sqlDB.QueryStr(t,
					`SELECT database_name, object_name FROM [SHOW BACKUP FROM LATEST IN $1] WHERE object_type='table' ORDER BY database_name, object_name`,
					destination,
				)
				require.Equal(t, tc.verifyTables, backedUp)
			})
		}
	}
}

func TestCreateBackupScheduleRequiresAdminRole(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, `CREATE USER testuser`)
	testuser := th.server.SQLConn(t, serverutils.User("testuser"))

	_, err := testuser.Exec("CREATE SCHEDULE FOR BACKUP INTO 'somewhere' RECURRING '@daily'")
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

// TestCreateBackupScheduleIfNotExists: checks if adding IF NOT EXISTS will
// create the schedule only if the schedule label doesn't already exist.
func TestCreateBackupScheduleIfNotExists(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	const collectionLocation = "nodelocal://1/collection"
	const scheduleLabel = "foo"
	const createQuery = "CREATE SCHEDULE IF NOT EXISTS '%s' FOR BACKUP INTO '%s' RECURRING '@daily' FULL BACKUP ALWAYS;"

	th.sqlDB.Exec(t, fmt.Sprintf(createQuery, scheduleLabel, collectionLocation))

	// no op expected
	th.sqlDB.Exec(t, fmt.Sprintf(createQuery, scheduleLabel, collectionLocation))

	const selectQuery = "SELECT label FROM [SHOW SCHEDULES] WHERE command->>'backup_statement' LIKE 'BACKUP%';"

	rows, err := th.cfg.DB.Executor().QueryBufferedEx(
		context.Background(), "check-sched", nil,
		sessiondata.NodeUserSessionDataOverride,
		selectQuery)

	require.NoError(t, err)
	require.Equal(t, 1, len(rows))

	// the 'bar' schedule should get scheduled
	const newScheduleLabel = "bar"

	th.sqlDB.Exec(t, fmt.Sprintf(createQuery, newScheduleLabel, collectionLocation))

	rows, err = th.cfg.DB.Executor().QueryBufferedEx(
		context.Background(), "check-sched2", nil,
		sessiondata.NodeUserSessionDataOverride,
		selectQuery)

	require.NoError(t, err)
	require.Equal(t, 2, len(rows))
}

func TestCreateBackupScheduleInExplicitTxnRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	res := th.sqlDB.Query(t, "SELECT id FROM [SHOW SCHEDULES] WHERE label LIKE 'BACKUP%';")
	require.False(t, res.Next())
	require.NoError(t, res.Err())

	th.sqlDB.Exec(t, "BEGIN;")
	th.sqlDB.Exec(t, "CREATE SCHEDULE FOR BACKUP INTO 'nodelocal://1/collection' RECURRING '@daily';")
	th.sqlDB.Exec(t, "ROLLBACK;")

	res = th.sqlDB.Query(t, "SELECT id FROM [SHOW SCHEDULES] WHERE label LIKE 'BACKUP%';")
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

	advanceNextRun := func(t *testing.T, id jobspb.ScheduleID, delta time.Duration) {
		// Adjust next run by the specified delta (which maybe negative).
		s := th.loadSchedule(t, id)
		s.SetNextRun(th.env.Now().Add(delta))
		schedules := jobs.ScheduledJobDB(th.internalDB())
		require.NoError(t, schedules.Update(context.Background(), s))
	}

	// We'll be manipulating schedule time via th.env, but we can't fool actual backup
	// when it comes to AsOf time.  So, override AsOf backup clause to be the current time.
	useRealTimeAOST := func() func() {
		th.setOverrideAsOfClauseKnob(t)
		knobs := th.cfg.TestingKnobs.(*jobs.TestingKnobs)
		return func() {
			knobs.OverrideAsOfClause = nil
		}
	}

	// Create backup schedules for this test.
	// Returns schedule IDs for full and incremental schedules, plus a cleanup function.
	createSchedules := func(t *testing.T, name string) (jobspb.ScheduleID, jobspb.ScheduleID, func()) {
		schedules, err := th.createBackupSchedule(t,
			"CREATE SCHEDULE FOR BACKUP INTO $1 RECURRING '*/5 * * * *'",
			"nodelocal://1/backup/"+name)
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
		fullID, incID jobspb.ScheduleID,
		onError jobspb.ScheduleDetails_ErrorHandlingBehavior,
	) {
		for _, id := range []jobspb.ScheduleID{fullID, incID} {
			// Pretend we were down for a year.
			s := th.loadSchedule(t, id)
			s.SetNextRun(s.NextRun().Add(-365 * 24 * time.Hour))
			// Set onError policy to the specified value.
			details := s.ScheduleDetails()
			details.OnError = onError
			s.SetScheduleDetails(*details)
			schedules := jobs.ScheduledJobDB(th.internalDB())
			require.NoError(t, schedules.Update(context.Background(), s))
		}
	}

	t.Run("pause", func(t *testing.T) {
		fullID, incID, cleanup := createSchedules(t, "pause")
		defer cleanup()

		markOldAndSetSchedulesPolicy(t, fullID, incID, jobspb.ScheduleDetails_PAUSE_SCHED)

		require.NoError(t, th.executeSchedules())

		// AOST way in the past causes backup planning to fail.  We don't need
		// to wait for any jobs, and the schedules should now be paused.
		for _, id := range []jobspb.ScheduleID{fullID, incID} {
			require.True(t, th.loadSchedule(t, id).IsPaused())
		}
	})

	metrics := func() *backupMetrics {
		ex, err := jobs.GetScheduledJobExecutor(tree.ScheduledBackupExecutor.InternalName())
		require.NoError(t, err)
		require.NotNil(t, ex.Metrics())
		return ex.Metrics().(*backupMetrics)
	}()

	t.Run("retry", func(t *testing.T) {
		fullID, incID, cleanup := createSchedules(t, "retry")
		defer cleanup()

		markOldAndSetSchedulesPolicy(t, fullID, incID, jobspb.ScheduleDetails_RETRY_SOON)

		require.NoError(t, th.executeSchedules())

		// AOST way in the past causes backup planning to fail.  We don't need
		// to wait for any jobs, and the schedule nextRun should be advanced
		// a bit in the future.
		for _, id := range []jobspb.ScheduleID{fullID, incID} {
			require.True(t, th.loadSchedule(t, id).NextRun().Sub(th.env.Now()) > 0)
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
		for _, id := range []jobspb.ScheduleID{fullID, incID} {
			s := th.loadSchedule(t, id)
			e, err := cron.ParseStandard(s.ScheduleExpr())
			require.NoError(t, err)
			require.EqualValues(t,
				e.Next(th.env.Now()).Round(time.Microsecond),
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
		testutils.SucceedsSoon(t, func() error {
			if metrics.NumWithPTS.Value() > 0 {
				return nil
			}
			return errors.New("still waiting for pts count > 0")
		})
	})
}

func extractBackupNode(sj *jobs.ScheduledJob) (*tree.Backup, error) {
	args := &backuppb.ScheduledBackupExecutionArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return nil, errors.Wrap(err, "un-marshaling args")
	}

	node, err := parser.ParseOne(args.BackupStatement)
	if err != nil {
		return nil, errors.Wrap(err, "parsing backup statement")
	}

	if backupStmt, ok := node.AST.(*tree.Backup); ok {
		return backupStmt, nil
	}

	return nil, errors.Newf("unexpect node type %T", node)
}

func constructExpectedScheduledBackupNode(
	t *testing.T, sj *jobs.ScheduledJob, fullBackupAlways bool, fullRecurrence, recurrence string,
) *tree.ScheduledBackup {
	t.Helper()
	args := &backuppb.ScheduledBackupExecutionArgs{}
	err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args)
	require.NoError(t, err)

	backupNode, err := extractBackupNode(sj)
	require.NoError(t, err)
	firstRun, err := tree.MakeDTimestampTZ(sj.ScheduledRunTime(), time.Microsecond)
	require.NoError(t, err)
	wait, err := schedulebase.ParseOnPreviousRunningOption(sj.ScheduleDetails().Wait)
	require.NoError(t, err)
	onError, err := schedulebase.ParseOnErrorOption(sj.ScheduleDetails().OnError)
	require.NoError(t, err)
	scheduleOptions := tree.KVOptions{
		tree.KVOption{
			Key:   optFirstRun,
			Value: firstRun,
		},
		tree.KVOption{
			Key:   optOnExecFailure,
			Value: tree.NewDString(onError),
		},
		tree.KVOption{
			Key:   optOnPreviousRunning,
			Value: tree.NewDString(wait),
		},
	}
	sb := &tree.ScheduledBackup{
		ScheduleLabelSpec: tree.LabelSpec{
			IfNotExists: false,
			Label:       tree.NewDString(sj.ScheduleLabel())},
		Recurrence: tree.NewDString(recurrence),
		FullBackup: &tree.FullBackupClause{
			AlwaysFull: fullBackupAlways,
		},
		Targets:         backupNode.Targets,
		To:              backupNode.To,
		BackupOptions:   backupNode.Options,
		ScheduleOptions: scheduleOptions,
	}
	if !fullBackupAlways {
		sb.FullBackup.Recurrence = tree.NewDString(fullRecurrence)
	}
	return sb
}

func TestShowCreateScheduleStatement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	testCases := []struct {
		name             string
		query            string
		fullBackupAlways bool
		fullRecurrence   string
		recurrence       string
	}{
		{
			name:           "full-incremental-schedule",
			query:          `CREATE SCHEDULE foo FOR BACKUP INTO '%s' RECURRING '@hourly'`,
			fullRecurrence: "@daily",
			recurrence:     "@hourly",
		},
		{
			name:             "full-schedule",
			query:            `CREATE SCHEDULE FOR BACKUP INTO '%s' RECURRING '@hourly' FULL BACKUP ALWAYS`,
			fullBackupAlways: true,
			fullRecurrence:   "@hourly",
			recurrence:       "@hourly",
		},
		{
			name:           "full-incremental-schedule-with-option",
			query:          `CREATE SCHEDULE FOR BACKUP INTO '%s' RECURRING '@hourly' FULL BACKUP '@daily' WITH SCHEDULE OPTIONS on_execution_failure = 'pause', ignore_existing_backups`,
			fullRecurrence: "@daily",
			recurrence:     "@hourly",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer utilccl.TestingEnableEnterprise()()
			defer th.clearSchedules(t)

			destination := "nodelocal://1/" + tc.name
			createScheduleQuery := fmt.Sprintf(tc.query, destination)
			schedules, err := th.createBackupSchedule(t, createScheduleQuery)
			require.NoError(t, err)

			// Find the full schedule, it will be the one with a scheduled next run.
			var fullSchedule *jobs.ScheduledJob
			for _, schedule := range schedules {
				if !schedule.ScheduledRunTime().IsZero() {
					fullSchedule = schedule
					break
				}
			}
			expectedScheduleNode := constructExpectedScheduledBackupNode(t, fullSchedule,
				tc.fullBackupAlways, tc.fullRecurrence, tc.recurrence)

			t.Run("show-create-all-schedules", func(t *testing.T) {
				rows := th.sqlDB.QueryStr(t, "SELECT * FROM [ SHOW CREATE ALL SCHEDULES ] WHERE create_statement LIKE '%FOR BACKUP%'")
				cols, err := th.sqlDB.Query(t, "SHOW CREATE ALL SCHEDULES").Columns()
				require.NoError(t, err)
				// The number of rows returned should be equal to the number of schedules created
				require.Equal(t, len(schedules), len(rows))
				require.Equal(t, cols, []string{"schedule_id", "create_statement"})

				for _, row := range rows {
					// Ensure that each row has schedule_id, create_stmt.
					require.Len(t, row, 2)
					showCreateScheduleStmt := row[1]
					require.Equal(t, expectedScheduleNode.String(), showCreateScheduleStmt)
				}
			})

			t.Run("show-create-schedule-by-id", func(t *testing.T) {
				for _, sj := range schedules {
					rows := th.sqlDB.QueryStr(t, fmt.Sprintf("SHOW CREATE SCHEDULE %d", sj.ScheduleID()))
					require.Equal(t, 1, len(rows))
					cols, err := th.sqlDB.Query(t, fmt.Sprintf("SHOW CREATE SCHEDULE %d", sj.ScheduleID())).Columns()
					require.NoError(t, err)
					require.Equal(t, cols, []string{"schedule_id", "create_statement"})
					require.Equal(t, expectedScheduleNode.String(), rows[0][1])
				}
			})
		})
	}
}

// TestCreateScheduledBackupTelemetry tests CREATE SCHEDULE FOR BACKUP correctly
// publishes telemetry events about the schedule creation.
func TestCreateScheduledBackupTelemetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()
	var asOfInterval int64

	// We'll be manipulating schedule time via th.env, but we can't fool actual backup
	// when it comes to AsOf time.  So, override AsOf backup clause to be the current time.
	useRealTimeAOST := func() func() {
		knobs := th.cfg.TestingKnobs.(*jobs.TestingKnobs)
		knobs.OverrideAsOfClause = func(clause *tree.AsOfClause, stmtTimestamp time.Time) {
			expr, err := tree.MakeDTimestampTZ(th.cfg.DB.KV().Clock().PhysicalTime(), time.Microsecond)
			asOfInterval = expr.Time.UnixNano() - stmtTimestamp.UnixNano()
			require.NoError(t, err)
			clause.Expr = expr
		}
		return func() {
			knobs.OverrideAsOfClause = nil
		}
	}
	defer useRealTimeAOST()()

	// Create a schedule and verify that the correct telemetry event was logged.
	query := `CREATE SCHEDULE FOR BACKUP INTO $1 RECURRING '@hourly' FULL BACKUP '@daily'
WITH SCHEDULE OPTIONS on_execution_failure = 'pause', ignore_existing_backups, first_run=$2`
	loc := "userfile:///logging"

	beforeBackup := th.env.Now()
	firstRun := th.env.Now().Add(time.Minute).Round(time.Microsecond)

	th.sqlDB.Exec(t, `SET application_name = 'backup_test'`)
	schedules, err := th.createBackupSchedule(t, query, loc, firstRun)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, 2, len(schedules))

	expectedCreateSchedule := eventpb.RecoveryEvent{
		CommonEventDetails: logpb.CommonEventDetails{
			EventType: "recovery_event",
		},
		RecoveryType:            createdScheduleEventType,
		TargetScope:             clusterScope.String(),
		TargetCount:             1,
		DestinationSubdirType:   standardSubdirType,
		DestinationStorageTypes: []string{"userfile"},
		DestinationAuthTypes:    []string{"specified"},
		AsOfInterval:            asOfInterval,
		Options:                 []string{telemetryOptionDetached},
		RecurringCron:           "@hourly",
		FullBackupCron:          "@daily",
		OnExecutionFailure:      "PAUSE_SCHED",
		OnPreviousRunning:       "WAIT",
		IgnoreExistingBackup:    true,
		CustomFirstRunTime:      firstRun.UnixNano(),
		ApplicationName:         "backup_test",
	}

	requireRecoveryEvent(t, beforeBackup.UnixNano(), createdScheduleEventType, expectedCreateSchedule)

	// Also verify that BACKUP telemetry is logged when the BACKUP job in the
	// schedule is executed.
	th.env.AdvanceTime(2 * time.Minute)
	require.NoError(t, th.executeSchedules())

	expectedScheduledBackup := eventpb.RecoveryEvent{
		CommonEventDetails: logpb.CommonEventDetails{
			EventType: "recovery_event",
		},
		ApplicationName:         "$ internal-exec-backup",
		RecoveryType:            scheduledBackupEventType,
		TargetScope:             clusterScope.String(),
		TargetCount:             1,
		DestinationSubdirType:   standardSubdirType,
		DestinationStorageTypes: []string{"userfile"},
		DestinationAuthTypes:    []string{"specified"},
		AsOfInterval:            asOfInterval,
		Options:                 []string{telemetryOptionDetached},
	}
	requireRecoveryEvent(t, beforeBackup.UnixNano(), scheduledBackupEventType, expectedScheduledBackup)
}

// TestPauseScheduledBackupOnNewClusterID ensures that a schedule backup pauses
// if it is running on a cluster with a different ID than is stored in its
// details.
func TestPauseScheduledBackupOnNewClusterID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()
	th.setOverrideAsOfClauseKnob(t)

	schedules, err := th.createBackupSchedule(t,
		"CREATE SCHEDULE FOR BACKUP INTO 'nodelocal://1/backup' RECURRING '@hourly' FULL BACKUP ALWAYS")
	require.NoError(t, err)

	full := schedules[0]

	// Force the schedule to execute.
	th.env.SetTime(full.NextRun().Add(time.Second))
	require.NoError(t, th.executeSchedules())
	th.waitForSuccessfulScheduledJob(t, full.ScheduleID())

	scheduleStorage := jobs.ScheduledJobDB(th.internalDB())

	hostClusterID := full.ScheduleDetails().ClusterID
	require.NotZero(t, hostClusterID)

	updateClusterIDAndExecute := func(clusterID uuid.UUID, scheduleID jobspb.ScheduleID) {
		schedule := th.loadSchedule(t, scheduleID)
		details := schedule.ScheduleDetails()
		details.ClusterID = clusterID
		schedule.SetScheduleDetails(*details)
		th.env.SetTime(schedule.NextRun().Add(time.Second))
		require.NoError(t, scheduleStorage.Update(context.Background(), schedule))
		require.NoError(t, th.executeSchedules())
	}

	t.Run("pause schedule due to different cluster id", func(t *testing.T) {
		updateClusterIDAndExecute(jobstest.DummyClusterID, full.ScheduleID())

		// Expect the schedule to pause because of the different cluster ID
		testutils.SucceedsSoon(t, func() error {
			expectPausedSchedule := th.loadSchedule(t, full.ScheduleID())
			if !expectPausedSchedule.IsPaused() {
				return errors.New("schedule has not paused yet")
			}
			// The cluster ID should have been reset.
			require.Equal(t, hostClusterID, expectPausedSchedule.ScheduleDetails().ClusterID)
			return nil
		})

		// Resume the schedule
		th.sqlDB.Exec(t, "RESUME SCHEDULE $1", full.ScheduleID())
		resumedSchedule := th.loadSchedule(t, full.ScheduleID())
		require.False(t, resumedSchedule.IsPaused())
		th.env.SetTime(resumedSchedule.NextRun().Add(time.Second))
		require.NoError(t, th.executeSchedules())
		th.waitForSuccessfulScheduledJobCount(t, full.ScheduleID(), 2)
	})
	t.Run("empty cluster id does not affect schedule", func(t *testing.T) {
		updateClusterIDAndExecute(uuid.UUID{}, full.ScheduleID())
		th.waitForSuccessfulScheduledJobCount(t, full.ScheduleID(), 3)
	})
}
