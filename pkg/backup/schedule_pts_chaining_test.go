// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Create backup schedules for this test.
// Returns schedule IDs for full and incremental schedules, plus a cleanup function.
func (th *testHelper) createSchedules(
	t *testing.T, backupStmt string, opts ...string,
) (jobspb.ScheduleID, jobspb.ScheduleID, func()) {
	backupOpts := ""
	if len(opts) > 0 {
		backupOpts = fmt.Sprintf(" WITH %s", strings.Join(opts, ", "))
	}
	backupQuery := fmt.Sprintf("%s%s", backupStmt, backupOpts)
	schedules, err := th.createBackupSchedule(t,
		fmt.Sprintf("CREATE SCHEDULE FOR %s RECURRING '*/2 * * * *'", backupQuery))
	require.NoError(t, err)

	// We expect full & incremental schedule to be created.
	require.Equal(t, 2, len(schedules))

	// Order schedules so that the full schedule is the first one
	fullID, incID := schedules[0].ScheduleID(), schedules[1].ScheduleID()
	if schedules[0].IsPaused() {
		fullID, incID = incID, fullID
	}

	return fullID,
		incID,
		func() {
			th.sqlDB.Exec(t, "DROP SCHEDULE $1", schedules[0].ScheduleID())
			th.sqlDB.Exec(t, "DROP SCHEDULE $1", schedules[1].ScheduleID())
		}
}

func checkPTSRecord(
	ctx context.Context,
	t *testing.T,
	th *testHelper,
	id uuid.UUID,
	schedule *jobs.ScheduledJob,
	timestamp hlc.Timestamp,
) {
	var ptsRecord *ptpb.Record
	var err error
	require.NoError(t, th.internalDB().Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		ptsRecord, err = th.protectedTimestamps().WithTxn(txn).
			GetRecord(context.Background(), id)
		require.NoError(t, err)
		return nil
	}))
	encodedScheduleID := []byte(strconv.FormatInt(int64(schedule.ScheduleID()), 10))
	require.Equal(t, encodedScheduleID, ptsRecord.Meta)
	require.Equal(t, jobsprotectedts.GetMetaType(jobsprotectedts.Schedules), ptsRecord.MetaType)
	require.Equal(t, timestamp, ptsRecord.Timestamp)
}

// TestScheduleChainingLifecycle tests that full and incremental schedules are
// updating protected timestamp records as expected.
func TestScheduleChainingLifecycle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	roundedCurrentTime := th.cfg.DB.KV().Clock().PhysicalTime().Round(time.Minute * 5)
	if roundedCurrentTime.Hour() == 0 && roundedCurrentTime.Minute() == 0 {
		// The backup schedule in this test uses the following crontab recurrence:
		// '*/2 * * * *'. In english, this means "run a full backup now, and then
		// run a full backup every day at midnight, and an incremental every 2
		// minutes. This test relies on an incremental backup running after the
		// first full backup. But, what happens if the first full backup gets
		// scheduled to run within 5 minutes of midnight? A second full backup may
		// get scheduled before the expected incremental backup, breaking the
		// invariant the test expects. For this reason, skip the test if it's
		// running close to midnight #spooky.
		skip.WithIssue(t, 91640, "test flakes when the machine clock is too close to midnight")
	}
	th.sqlDB.Exec(t, `
CREATE DATABASE db;
USE db;
CREATE TABLE t(a int);
INSERT INTO t values (1), (10), (100);
`)

	backupAsOfTimes := make([]time.Time, 0)
	th.cfg.TestingKnobs.(*jobs.TestingKnobs).OverrideAsOfClause = func(clause *tree.AsOfClause, _ time.Time) {
		backupAsOfTime := th.cfg.DB.KV().Clock().PhysicalTime()
		expr, err := tree.MakeDTimestampTZ(backupAsOfTime, time.Microsecond)
		require.NoError(t, err)
		clause.Expr = expr
		backupAsOfTimes = append(backupAsOfTimes, backupAsOfTime)
	}

	runSchedule := func(t *testing.T, schedule *jobs.ScheduledJob) {
		th.env.SetTime(schedule.NextRun().Add(time.Second))
		require.NoError(t, th.executeSchedules())
		th.waitForSuccessfulScheduledJob(t, schedule.ScheduleID())
	}

	clearSuccessfulJobEntryForSchedule := func(t *testing.T, schedule *jobs.ScheduledJob) {
		query := "DELETE FROM " + th.env.SystemJobsTableName() +
			" WHERE status=$1 AND created_by_type=$2 AND created_by_id=$3"
		_, err := th.sqlDB.DB.ExecContext(context.Background(), query, jobs.StateSucceeded,
			jobs.CreatedByScheduledJobs, schedule.ScheduleID())
		require.NoError(t, err)
	}

	ctx := context.Background()

	for _, tc := range []struct {
		name       string
		backupStmt string
	}{
		{
			name:       "cluster",
			backupStmt: `BACKUP INTO 'nodelocal://1/%s'`,
		},
		{
			name:       "database",
			backupStmt: `BACKUP DATABASE db INTO 'nodelocal://1/%s'`,
		},
		{
			name:       "table",
			backupStmt: `BACKUP TABLE t INTO 'nodelocal://1/%s'`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fullID, incID, cleanupSchedules := th.createSchedules(t, fmt.Sprintf(tc.backupStmt, tc.name),
				"revision_history")
			defer cleanupSchedules()
			defer func() { backupAsOfTimes = backupAsOfTimes[:0] }()

			schedules := jobs.ScheduledJobDB(th.internalDB())
			fullSchedule := th.loadSchedule(t, fullID)
			_, fullArgs, err := getScheduledBackupExecutionArgsFromSchedule(
				ctx, th.env, schedules, fullID,
			)
			require.NoError(t, err)

			// Force full backup to execute (this unpauses incremental).
			runSchedule(t, fullSchedule)

			// Check that there is no PTS record on the full schedule.
			incSchedule := th.loadSchedule(t, incID)
			_, incArgs, err := getScheduledBackupExecutionArgsFromSchedule(
				ctx, th.env, schedules, incID,
			)
			require.NoError(t, err)
			require.Nil(t, fullArgs.ProtectedTimestampRecord)

			// Check that there is a PTS record on the incremental schedule.
			ptsOnIncID := incArgs.ProtectedTimestampRecord
			require.NotNil(t, ptsOnIncID)
			checkPTSRecord(ctx, t, th, *ptsOnIncID, incSchedule,
				hlc.Timestamp{WallTime: backupAsOfTimes[0].Round(time.Microsecond).UnixNano()})

			// Force inc backup to execute.
			runSchedule(t, incSchedule)

			// Check that the pts record was updated to the inc backups' EndTime.
			checkPTSRecord(ctx, t, th, *ptsOnIncID, incSchedule,
				hlc.Timestamp{WallTime: backupAsOfTimes[1].Round(time.Microsecond).UnixNano()})

			// Pause the incSchedule so that it doesn't run when we forward the env time
			// to re-run the full schedule.
			incSchedule = th.loadSchedule(t, incSchedule.ScheduleID())
			incSchedule.Pause()
			require.NoError(t, schedules.Update(ctx, incSchedule))

			clearSuccessfulJobEntryForSchedule(t, fullSchedule)

			// Force another full backup to execute.
			fullSchedule = th.loadSchedule(t, fullSchedule.ScheduleID())
			runSchedule(t, fullSchedule)

			// Check that the pts record on the inc schedule has been overwritten with a new
			// record written by the full backup.
			incSchedule = th.loadSchedule(t, incSchedule.ScheduleID())
			_, incArgs, err = getScheduledBackupExecutionArgsFromSchedule(
				ctx, th.env, schedules, incID,
			)
			require.NoError(t, err)
			require.NotEqual(t, *ptsOnIncID, *incArgs.ProtectedTimestampRecord)

			// Check that the old pts record has been released.
			require.NoError(t, th.cfg.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				pts := th.protectedTimestamps().WithTxn(txn)
				_, err := pts.GetRecord(ctx, *ptsOnIncID)
				require.True(t, errors.Is(err, protectedts.ErrNotExists))
				return nil
			}))
			checkPTSRecord(ctx, t, th, *incArgs.ProtectedTimestampRecord, incSchedule,
				hlc.Timestamp{WallTime: backupAsOfTimes[2].Round(time.Microsecond).UnixNano()})
		})
	}
}

func TestScheduleChainingEdgeCases(t *testing.T) {
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

	backupAsOfTimes := make([]time.Time, 0)
	th.cfg.TestingKnobs.(*jobs.TestingKnobs).OverrideAsOfClause = func(clause *tree.AsOfClause, _ time.Time) {
		backupAsOfTime := th.cfg.DB.KV().Clock().PhysicalTime()
		expr, err := tree.MakeDTimestampTZ(backupAsOfTime, time.Microsecond)
		require.NoError(t, err)
		clause.Expr = expr
		backupAsOfTimes = append(backupAsOfTimes, backupAsOfTime)
	}

	runSchedule := func(t *testing.T, schedule *jobs.ScheduledJob) {
		th.env.SetTime(schedule.NextRun().Add(time.Second))
		require.NoError(t, th.executeSchedules())
		th.waitForSuccessfulScheduledJob(t, schedule.ScheduleID())
	}

	clusterBackupStmt := `BACKUP INTO 'nodelocal://1/%s'`

	ctx := context.Background()

	t.Run("inc schedule is dropped", func(t *testing.T) {
		fullID, incID, cleanupSchedules := th.createSchedules(t, fmt.Sprintf(clusterBackupStmt, "foo"))
		defer cleanupSchedules()
		defer func() { backupAsOfTimes = backupAsOfTimes[:0] }()

		th.sqlDB.Exec(t, `DROP SCHEDULE $1`, incID)

		fullSchedule := th.loadSchedule(t, fullID)
		// Force full backup to execute (this unpauses incremental, which should be a no-op).
		runSchedule(t, fullSchedule)

		var numRows int
		th.sqlDB.QueryRow(t, `SELECT count(*) FROM system.protected_ts_records`).Scan(&numRows)
		require.Zero(t, numRows)
	})

	t.Run("full schedule is dropped after first run", func(t *testing.T) {
		fullID, incID, cleanupSchedules := th.createSchedules(t, fmt.Sprintf(clusterBackupStmt, "bar"))
		defer cleanupSchedules()
		defer func() { backupAsOfTimes = backupAsOfTimes[:0] }()

		schedules := jobs.ScheduledJobDB(th.internalDB())

		fullSchedule := th.loadSchedule(t, fullID)
		_, fullArgs, err := getScheduledBackupExecutionArgsFromSchedule(
			ctx, th.env, schedules, fullID,
		)
		require.NoError(t, err)
		// Force full backup to execute (this unpauses incremental).
		runSchedule(t, fullSchedule)

		// Check that there is no PTS record on the full schedule.
		require.Nil(t, fullArgs.ProtectedTimestampRecord)

		// Check that there is a PTS record on the incremental schedule.
		incSchedule := th.loadSchedule(t, incID)
		_, incArgs, err := getScheduledBackupExecutionArgsFromSchedule(
			ctx, th.env, schedules, incID,
		)
		require.NoError(t, err)
		ptsOnIncID := incArgs.ProtectedTimestampRecord
		require.NotNil(t, ptsOnIncID)
		checkPTSRecord(ctx, t, th, *ptsOnIncID, incSchedule,
			hlc.Timestamp{WallTime: backupAsOfTimes[0].Round(time.Microsecond).UnixNano()})

		th.sqlDB.Exec(t, `DROP SCHEDULE $1`, fullID)

		_, err = schedules.Load(ctx, th.env, incID)
		require.Error(t, err)

		// Check that the incremental schedule's PTS is dropped
		require.NoError(t, th.cfg.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			pts := th.protectedTimestamps().WithTxn(txn)
			_, err := pts.GetRecord(ctx, *ptsOnIncID)
			require.True(t, errors.Is(err, protectedts.ErrNotExists))
			return nil
		}))
	})
}

// TestDropScheduleReleasePTSRecord tests that dropping a schedule will release
// the protected timestamp assocaited with that schedule.
func TestDropScheduleReleasePTSRecord(t *testing.T) {
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

	backupAsOfTimes := make([]time.Time, 0)
	th.cfg.TestingKnobs.(*jobs.TestingKnobs).OverrideAsOfClause = func(clause *tree.AsOfClause, _ time.Time) {
		backupAsOfTime := th.cfg.DB.KV().Clock().PhysicalTime()
		expr, err := tree.MakeDTimestampTZ(backupAsOfTime, time.Microsecond)
		require.NoError(t, err)
		clause.Expr = expr
		backupAsOfTimes = append(backupAsOfTimes, backupAsOfTime)
	}

	runSchedule := func(t *testing.T, schedule *jobs.ScheduledJob) {
		th.env.SetTime(schedule.NextRun().Add(time.Second))
		require.NoError(t, th.executeSchedules())
		th.waitForSuccessfulScheduledJob(t, schedule.ScheduleID())
	}

	ctx := context.Background()

	clusterBackupStmt := `BACKUP INTO 'nodelocal://1/%s'`
	fullID, incID, cleanupSchedules := th.createSchedules(t, fmt.Sprintf(clusterBackupStmt, "foo"),
		"revision_history")
	defer cleanupSchedules()
	defer func() { backupAsOfTimes = backupAsOfTimes[:0] }()

	fullSchedule := th.loadSchedule(t, fullID)
	// Force full backup to execute (this unpauses incremental).
	runSchedule(t, fullSchedule)

	// Check that the incremental schedule has a protected timestamp record
	// written on it by the full schedule.
	schedules := jobs.ScheduledJobDB(th.internalDB())
	incSchedule := th.loadSchedule(t, incID)
	_, incArgs, err := getScheduledBackupExecutionArgsFromSchedule(
		ctx, th.env, schedules, incID,
	)
	require.NoError(t, err)
	ptsOnIncID := incArgs.ProtectedTimestampRecord
	require.NotNil(t, ptsOnIncID)
	checkPTSRecord(ctx, t, th, *ptsOnIncID, incSchedule,
		hlc.Timestamp{WallTime: backupAsOfTimes[0].Round(time.Microsecond).UnixNano()})

	th.sqlDB.Exec(t, `DROP SCHEDULE $1`, incID)

	// Ensure that the pts record on the incremental schedule has been released
	// by the DROP.
	var numRows int
	th.sqlDB.QueryRow(t, `SELECT count(*) FROM system.protected_ts_records`).Scan(&numRows)
	require.Zero(t, numRows)

	// Also ensure that the full schedule doesn't have DependentID set anymore.
	_, fullArgs, err := getScheduledBackupExecutionArgsFromSchedule(
		ctx, th.env, schedules, fullID,
	)
	require.NoError(t, err)
	require.Zero(t, fullArgs.DependentScheduleID)
}

// TestScheduleChainingWithDatabaseExpansion checks that chaining also applies
// to new schema objects created in between backups.
func TestScheduleChainingWithDatabaseExpansion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	roundedCurrentTime := th.cfg.DB.KV().Clock().PhysicalTime().Round(time.Minute * 5)
	if roundedCurrentTime.Hour() == 0 && roundedCurrentTime.Minute() == 0 {
		// The backup schedule in this test uses the following crontab recurrence:
		// '*/2 * * * *'. In english, this means "run a full backup now, and then
		// run a full backup every day at midnight, and an incremental every 2
		// minutes. This test relies on an incremental backup running after the
		// first full backup and pulling up the PTS record. But, what happens if the
		// first full backup gets scheduled to run within 2 minutes of midnight? A
		// second full backup may get scheduled before the expected incremental
		// backup, breaking the invariant the test expects. For this reason, skip
		// the test if it's running close to midnight which is rare.
		skip.IgnoreLint(t, "test flakes when the machine clock is too close to midnight")
	}

	th.sqlDB.Exec(t, `
CREATE DATABASE db;
USE db;
CREATE TABLE t(a INT, b INT);
INSERT INTO t select x, y from generate_series(1, 100) as g(x), generate_series(1, 100) as g2(y);
`)

	backupAsOfTimes := make([]time.Time, 0)
	th.cfg.TestingKnobs.(*jobs.TestingKnobs).OverrideAsOfClause = func(clause *tree.AsOfClause, _ time.Time) {
		backupAsOfTime := th.cfg.DB.KV().Clock().PhysicalTime()
		expr, err := tree.MakeDTimestampTZ(backupAsOfTime, time.Microsecond)
		require.NoError(t, err)
		clause.Expr = expr
		backupAsOfTimes = append(backupAsOfTimes, backupAsOfTime)
	}

	runSchedule := func(t *testing.T, schedule *jobs.ScheduledJob) {
		th.env.SetTime(schedule.NextRun().Add(time.Second))
		require.NoError(t, th.executeSchedules())
		th.waitForSuccessfulScheduledJob(t, schedule.ScheduleID())
	}

	checkProtectionPolicy := func(expectedProtectionPolicy hlc.Timestamp, databaseName, tableName string) {
		testutils.SucceedsSoon(t, func() error {
			trace := runGCWithTrace(t, th.sqlDB,
				false /* skipShouldQueue */, databaseName, tableName)

			// Check the trace for the applicable protection policy.
			processedPattern := fmt.Sprintf("(?s)has a protection policy protecting: %s.*shouldQueue=false",
				expectedProtectionPolicy.String())
			processedRegexp := regexp.MustCompile(processedPattern)
			if !processedRegexp.MatchString(trace) {
				return errors.Newf("%q does not match %q", trace, processedRegexp)
			}
			return nil
		})
	}

	clearSuccessfulJobForSchedule := func(t *testing.T, schedule *jobs.ScheduledJob) {
		query := "DELETE FROM " + th.env.SystemJobsTableName() +
			" WHERE status=$1 AND created_by_type=$2 AND created_by_id=$3"
		_, err := th.sqlDB.DB.ExecContext(context.Background(), query, jobs.StateSucceeded,
			jobs.CreatedByScheduledJobs, schedule.ScheduleID())
		require.NoError(t, err)
	}

	clusterBackupStmt := `BACKUP DATABASE db INTO 'nodelocal://1/%s'`
	fullID, incID, cleanupSchedules := th.createSchedules(t, fmt.Sprintf(clusterBackupStmt, "foo"),
		"revision_history")
	defer cleanupSchedules()

	fullSchedule := th.loadSchedule(t, fullID)

	// Force full backup to execute (this unpauses incremental). After the
	// completion of the full backup we should have a pts record on database db.
	runSchedule(t, fullSchedule)

	// Manually enqueuing a range for mvccGC should see the protection policy.
	fullBackupAsOf := hlc.Timestamp{WallTime: backupAsOfTimes[0].Round(time.Microsecond).UnixNano()}
	checkProtectionPolicy(fullBackupAsOf, "db", "t")

	// Insert some more data.
	th.sqlDB.Exec(t, `
INSERT INTO t select x, y from generate_series(1, 100) as g(x), generate_series(1, 100) as g2(y);
`)

	// Now, let's run the incremental schedule.
	incSchedule := th.loadSchedule(t, incID)
	runSchedule(t, incSchedule)
	clearSuccessfulJobForSchedule(t, incSchedule)

	// The protection policy should have been pulled up.
	incBackupAsOf := hlc.Timestamp{WallTime: backupAsOfTimes[1].Round(time.Microsecond).UnixNano()}
	checkProtectionPolicy(incBackupAsOf, "db", "t")

	// Let's create another table in the database. This table should also be
	// covered by the protection policy that the incremental schedule is holding
	// on to.
	th.sqlDB.Exec(t, `CREATE TABLE s (a INT, b INT)`)
	th.sqlDB.Exec(t, `
INSERT INTO s select x, y from generate_series(1, 100) as g(x), generate_series(1, 100) as g2(y);
`)

	// The protection policy should exist on the newly created table.
	checkProtectionPolicy(incBackupAsOf, "db", "s")

	// We should be able to run an incremental backup.
	incSchedule = th.loadSchedule(t, incID)
	runSchedule(t, incSchedule)
}
