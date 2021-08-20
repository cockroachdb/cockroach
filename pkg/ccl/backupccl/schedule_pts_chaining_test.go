// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Create backup schedules for this test.
// Returns schedule IDs for full and incremental schedules, plus a cleanup function.
func (th *testHelper) createSchedules(t *testing.T, name string) (int64, int64, func()) {
	schedules, err := th.createBackupSchedule(t,
		"CREATE SCHEDULE FOR BACKUP INTO $1 WITH revision_history RECURRING '*/5 * * * *'",
		"nodelocal://0/backup/"+name)
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
	require.NoError(t, th.server.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		ptsRecord, err = th.server.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider.
			GetRecord(context.Background(), txn, id)
		require.NoError(t, err)
		return nil
	}))
	encodedScheduleID := []byte(strconv.FormatInt(schedule.ScheduleID(), 10))
	require.Equal(t, encodedScheduleID, ptsRecord.Meta)
	require.Equal(t, jobsprotectedts.GetMetaType(jobsprotectedts.Schedules), ptsRecord.MetaType)
	require.Equal(t, timestamp, ptsRecord.Timestamp)
}

func TestScheduleBackupChainsProtectedTimestampRecords(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, `SET CLUSTER SETTING schedules.backup.gc_protection.enabled = true`)
	th.sqlDB.Exec(t, `
CREATE DATABASE db;
USE db;
CREATE TABLE t(a int);
INSERT INTO t values (1), (10), (100);
`)

	backupAsOfTimes := make([]time.Time, 0)
	th.cfg.TestingKnobs.(*jobs.TestingKnobs).OverrideAsOfClause = func(clause *tree.AsOfClause) {
		backupAsOfTime := th.cfg.DB.Clock().PhysicalTime()
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
		_, err := th.sqlDB.DB.ExecContext(context.Background(), query, jobs.StatusSucceeded,
			jobs.CreatedByScheduledJobs, schedule.ScheduleID())
		require.NoError(t, err)
	}

	ctx := context.Background()

	fullID, incID, cleanupSchedules := th.createSchedules(t, "foo")
	defer cleanupSchedules()

	fullSchedule := th.loadSchedule(t, fullID)
	_, fullArgs, err := getScheduledBackupExecutionArgsFromSchedule(ctx, th.env, nil,
		th.server.InternalExecutor().(*sql.InternalExecutor), fullID)
	require.NoError(t, err)

	// Force full backup to execute (this unpauses incremental).
	runSchedule(t, fullSchedule)

	// Check that there is no PTS record on the full schedule.
	incSchedule := th.loadSchedule(t, incID)
	_, incArgs, err := getScheduledBackupExecutionArgsFromSchedule(ctx, th.env, nil,
		th.server.InternalExecutor().(*sql.InternalExecutor), incID)
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
	require.NoError(t, incSchedule.Update(context.Background(), th.cfg.InternalExecutor, nil))

	clearSuccessfulJobEntryForSchedule(t, fullSchedule)

	// Force another full backup to execute.
	fullSchedule = th.loadSchedule(t, fullSchedule.ScheduleID())
	runSchedule(t, fullSchedule)

	// Check that the pts record on the inc schedule has been overwritten with a new
	// record written by the full backup.
	incSchedule = th.loadSchedule(t, incSchedule.ScheduleID())
	_, incArgs, err = getScheduledBackupExecutionArgsFromSchedule(ctx, th.env, nil,
		th.server.InternalExecutor().(*sql.InternalExecutor), incID)
	require.NoError(t, err)
	require.NotEqual(t, *ptsOnIncID, *incArgs.ProtectedTimestampRecord)

	// Check that the old pts record has been released.
	require.NoError(t, th.cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := th.server.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider.GetRecord(
			ctx, txn, *ptsOnIncID)
		require.True(t, errors.Is(err, protectedts.ErrNotExists))
		return nil
	}))
	checkPTSRecord(ctx, t, th, *incArgs.ProtectedTimestampRecord, incSchedule,
		hlc.Timestamp{WallTime: backupAsOfTimes[2].Round(time.Microsecond).UnixNano()})
}

func TestPTSChainingEdgeCases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, `SET CLUSTER SETTING schedules.backup.gc_protection.enabled = true`)
	th.sqlDB.Exec(t, `
CREATE DATABASE db;
USE db;
CREATE TABLE t(a int);
INSERT INTO t values (1), (10), (100);
`)

	backupAsOfTimes := make([]time.Time, 0)
	th.cfg.TestingKnobs.(*jobs.TestingKnobs).OverrideAsOfClause = func(clause *tree.AsOfClause) {
		backupAsOfTime := th.cfg.DB.Clock().PhysicalTime()
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

	clearSuccessfulJobForSchedule := func(t *testing.T, schedule *jobs.ScheduledJob) {
		query := "DELETE FROM " + th.env.SystemJobsTableName() +
			" WHERE status=$1 AND created_by_type=$2 AND created_by_id=$3"
		_, err := th.sqlDB.DB.ExecContext(context.Background(), query, jobs.StatusSucceeded,
			jobs.CreatedByScheduledJobs, schedule.ScheduleID())
		require.NoError(t, err)
	}

	ctx := context.Background()

	t.Run("inc schedule is dropped", func(t *testing.T) {
		fullID, incID, cleanupSchedules := th.createSchedules(t, "foo")
		defer cleanupSchedules()
		defer func() { backupAsOfTimes = backupAsOfTimes[:0] }()

		th.sqlDB.Exec(t, `DROP SCHEDULE $1`, incID)

		fullSchedule := th.loadSchedule(t, fullID)
		// Force full backup to execute (this unpauses incremental, which should be a no-op).
		runSchedule(t, fullSchedule)

		var numRows int
		th.sqlDB.QueryRow(t, `SELECT num_records FROM system.protected_ts_meta`).Scan(&numRows)
		require.Zero(t, numRows)
	})

	t.Run("full schedule is dropped after first run", func(t *testing.T) {
		fullID, incID, cleanupSchedules := th.createSchedules(t, "bar")
		defer cleanupSchedules()
		defer func() { backupAsOfTimes = backupAsOfTimes[:0] }()

		fullSchedule := th.loadSchedule(t, fullID)
		_, fullArgs, err := getScheduledBackupExecutionArgsFromSchedule(ctx, th.env, nil,
			th.server.InternalExecutor().(*sql.InternalExecutor), fullID)
		require.NoError(t, err)
		// Force full backup to execute (this unpauses incremental).
		runSchedule(t, fullSchedule)

		// Check that there is no PTS record on the full schedule.
		require.Nil(t, fullArgs.ProtectedTimestampRecord)

		// Check that there is a PTS record on the incremental schedule.
		incSchedule := th.loadSchedule(t, incID)
		_, incArgs, err := getScheduledBackupExecutionArgsFromSchedule(ctx, th.env, nil,
			th.server.InternalExecutor().(*sql.InternalExecutor), incID)
		require.NoError(t, err)
		ptsOnIncID := incArgs.ProtectedTimestampRecord
		require.NotNil(t, ptsOnIncID)
		checkPTSRecord(ctx, t, th, *ptsOnIncID, incSchedule,
			hlc.Timestamp{WallTime: backupAsOfTimes[0].Round(time.Microsecond).UnixNano()})

		th.sqlDB.Exec(t, `DROP SCHEDULE $1`, fullID)

		// Run the inc schedule.
		runSchedule(t, incSchedule)
		checkPTSRecord(ctx, t, th, *ptsOnIncID, incSchedule,
			hlc.Timestamp{WallTime: backupAsOfTimes[1].Round(time.Microsecond).UnixNano()})
		clearSuccessfulJobForSchedule(t, incSchedule)

		// Run it again.
		incSchedule = th.loadSchedule(t, incID)
		runSchedule(t, incSchedule)
		checkPTSRecord(ctx, t, th, *ptsOnIncID, incSchedule,
			hlc.Timestamp{WallTime: backupAsOfTimes[2].Round(time.Microsecond).UnixNano()})
	})
}

func TestDropScheduleReleasePTSRecord(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, `SET CLUSTER SETTING schedules.backup.gc_protection.enabled = true`)
	th.sqlDB.Exec(t, `
CREATE DATABASE db;
USE db;
CREATE TABLE t(a int);
INSERT INTO t values (1), (10), (100);
`)

	backupAsOfTimes := make([]time.Time, 0)
	th.cfg.TestingKnobs.(*jobs.TestingKnobs).OverrideAsOfClause = func(clause *tree.AsOfClause) {
		backupAsOfTime := th.cfg.DB.Clock().PhysicalTime()
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

	fullID, incID, cleanupSchedules := th.createSchedules(t, "foo")
	defer cleanupSchedules()
	defer func() { backupAsOfTimes = backupAsOfTimes[:0] }()

	fullSchedule := th.loadSchedule(t, fullID)
	// Force full backup to execute (this unpauses incremental, which should be a no-op).
	runSchedule(t, fullSchedule)

	incSchedule := th.loadSchedule(t, incID)
	_, incArgs, err := getScheduledBackupExecutionArgsFromSchedule(ctx, th.env, nil,
		th.server.InternalExecutor().(*sql.InternalExecutor), incID)
	require.NoError(t, err)
	ptsOnIncID := incArgs.ProtectedTimestampRecord
	require.NotNil(t, ptsOnIncID)
	checkPTSRecord(ctx, t, th, *ptsOnIncID, incSchedule,
		hlc.Timestamp{WallTime: backupAsOfTimes[0].Round(time.Microsecond).UnixNano()})

	th.sqlDB.Exec(t, `DROP SCHEDULE $1`, incID)

	// Ensure that the pts record on the incremental schedule has been released
	// by the DROP.
	var numRows int
	th.sqlDB.QueryRow(t, `SELECT num_records FROM system.protected_ts_meta`).Scan(&numRows)
	require.Zero(t, numRows)

	// Also ensure that the full schedule doesn't have DependentID set anymore.
	_, fullArgs, err := getScheduledBackupExecutionArgsFromSchedule(ctx, th.env, nil,
		th.server.InternalExecutor().(*sql.InternalExecutor), fullID)
	require.NoError(t, err)
	require.Zero(t, fullArgs.DependentScheduleID)
}
