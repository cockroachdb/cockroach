// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/fatih/structs"
	"github.com/stretchr/testify/require"
)

func TestBackupCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tempDir, tempDirCleanup := testutils.TempDir(t)
	defer tempDirCleanup()
	st := cluster.MakeTestingClusterSettings()
	backupinfo.WriteMetadataWithExternalSSTsEnabled.Override(ctx, &st.SV, true)
	_, db, cleanupDB := backupRestoreTestSetupEmpty(
		t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Settings: st,
			},
		},
	)
	defer cleanupDB()

	// Expects start/end to be nanosecond epoch.
	startCompaction := func(bucket int, subdir string, start, end int64) jobspb.JobID {
		compactionBuiltin := `SELECT crdb_internal.backup_compaction(
ARRAY['nodelocal://1/backup/%d'], '%s', ''::BYTES, %d::DECIMAL, %d::DECIMAL
)`
		row := db.QueryRow(t, fmt.Sprintf(compactionBuiltin, bucket, subdir, start, end))
		var jobID jobspb.JobID
		row.Scan(&jobID)
		return jobID
	}
	// Note: Each subtest should create their backups in their own subdirectory to
	// avoid false negatives from subtests relying on backups from other subtests.
	const fullBackupAostCmd = "BACKUP INTO 'nodelocal://1/backup/%d' AS OF SYSTEM TIME '%d'"
	const incBackupCmd = "BACKUP INTO LATEST IN 'nodelocal://1/backup/%d'"
	const incBackupAostCmd = "BACKUP INTO LATEST IN 'nodelocal://1/backup/%d' AS OF SYSTEM TIME '%d'"

	t.Run("basic operations insert, update, and delete", func(t *testing.T) {
		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE IF EXISTS foo")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		start := getTime(t)
		db.Exec(t, fmt.Sprintf(fullBackupAostCmd, 1, start))
		var backupPath string
		db.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup/1'").Scan(&backupPath)

		// Run twice to test compaction on top of compaction.
		for i := range 2 {
			db.Exec(t, "INSERT INTO foo VALUES (2, 2), (3, 3)")
			db.Exec(t, fmt.Sprintf(incBackupCmd, 1))
			db.Exec(t, "UPDATE foo SET b = b + 1 WHERE a = 2")
			db.Exec(t, fmt.Sprintf(incBackupCmd, 1))
			db.Exec(t, "DELETE FROM foo WHERE a = 3")
			end := getTime(t)
			db.Exec(
				t,
				fmt.Sprintf(incBackupAostCmd, 1, end),
			)
			jobutils.WaitForJobToSucceed(t, db, startCompaction(1, backupPath, start, end))
			validateCompactedBackupForTables(
				t, db, []string{"foo"}, "'nodelocal://1/backup/1'", start, end, 2+i,
			)
			start = end
		}

		// Ensure that additional backups were created.
		var numBackups int
		db.QueryRow(
			t,
			"SELECT count(DISTINCT (start_time, end_time)) FROM "+
				"[SHOW BACKUP FROM $1 IN 'nodelocal://1/backup/1']",
			backupPath,
		).Scan(&numBackups)
		require.Equal(t, 9, numBackups)
	})

	t.Run("create and drop tables", func(t *testing.T) {
		defer func() {
			db.Exec(t, "DROP TABLE IF EXISTS foo, bar, baz")
		}()
		db.Exec(t, "CREATE TABLE foo (a INT PRIMARY KEY, b INT)")
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		start := getTime(t)
		db.Exec(t, fmt.Sprintf(fullBackupAostCmd, 2, start))

		db.Exec(t, "CREATE TABLE bar (a INT, b INT)")
		db.Exec(t, "INSERT INTO bar VALUES (1, 1)")
		db.Exec(t, fmt.Sprintf(incBackupCmd, 2))

		db.Exec(t, "INSERT INTO bar VALUES (2, 2)")
		db.Exec(t, fmt.Sprintf(incBackupCmd, 2))

		db.Exec(t, "CREATE TABLE baz (a INT, b INT)")
		db.Exec(t, "INSERT INTO baz VALUES (3, 3)")
		end := getTime(t)
		db.Exec(
			t,
			fmt.Sprintf(incBackupAostCmd, 2, end),
		)
		var backupPath string
		db.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup/2'").Scan(&backupPath)
		jobutils.WaitForJobToSucceed(t, db, startCompaction(2, backupPath, start, end))
		validateCompactedBackupForTables(
			t, db,
			[]string{"foo", "bar", "baz"},
			"'nodelocal://1/backup/2'",
			start, end, 2,
		)

		db.Exec(t, "DROP TABLE bar")
		end = getTime(t)
		db.Exec(
			t,
			fmt.Sprintf(incBackupAostCmd, 2, end),
		)
		jobutils.WaitForJobToSucceed(t, db, startCompaction(2, backupPath, start, end))

		db.Exec(t, "DROP TABLE foo, baz")
		db.Exec(t, "RESTORE FROM LATEST IN 'nodelocal://1/backup/2'")
		rows := db.QueryStr(t, "SELECT * FROM [SHOW TABLES] WHERE table_name = 'bar'")
		require.Empty(t, rows)
	})

	t.Run("create indexes", func(t *testing.T) {
		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1), (2, 2), (3, 3)")
		start := getTime(t)
		db.Exec(t, fmt.Sprintf(fullBackupAostCmd, 3, start))

		db.Exec(t, "CREATE INDEX bar ON foo (a)")
		db.Exec(t, "CREATE INDEX baz ON foo (a)")
		db.Exec(t, fmt.Sprintf(incBackupCmd, 3))

		db.Exec(t, "CREATE INDEX qux ON foo (b)")
		db.Exec(t, "DROP INDEX foo@bar")
		end := getTime(t)
		db.Exec(
			t,
			fmt.Sprintf(incBackupAostCmd, 3, end),
		)
		var backupPath string
		db.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup/3'").Scan(&backupPath)
		jobutils.WaitForJobToSucceed(t, db, startCompaction(3, backupPath, start, end))

		var numIndexes, restoredNumIndexes int
		db.QueryRow(t, "SELECT count(*) FROM [SHOW INDEXES FROM foo]").Scan(&numIndexes)
		db.Exec(t, "DROP TABLE foo")
		db.Exec(t, "RESTORE TABLE foo FROM LATEST IN 'nodelocal://1/backup/3'")
		db.QueryRow(t, "SELECT count(*) FROM [SHOW INDEXES FROM foo]").Scan(&restoredNumIndexes)
		require.Equal(t, numIndexes, restoredNumIndexes)
	})

	t.Run("compact middle of backup chain", func(t *testing.T) {
		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		db.Exec(t, "BACKUP INTO 'nodelocal://1/backup/4'")

		db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
		start := getTime(t)
		db.Exec(
			t,
			fmt.Sprintf(incBackupAostCmd, 4, start),
		)

		db.Exec(t, "INSERT INTO foo VALUES (3, 3)")
		db.Exec(t, fmt.Sprintf(incBackupCmd, 4))

		db.Exec(t, "INSERT INTO foo VALUES (4, 4)")
		db.Exec(t, fmt.Sprintf(incBackupCmd, 4))

		db.Exec(t, "INSERT INTO foo VALUES (5, 5)")
		end := getTime(t)
		db.Exec(
			t,
			fmt.Sprintf(incBackupAostCmd, 4, end),
		)

		db.Exec(t, "INSERT INTO foo VALUES (6, 6)")
		db.Exec(t, fmt.Sprintf(incBackupCmd, 4))

		var backupPath string
		db.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup/4'").Scan(&backupPath)
		jobutils.WaitForJobToSucceed(t, db, startCompaction(4, backupPath, start, end))
		validateCompactedBackupForTables(
			t, db, []string{"foo"}, "'nodelocal://1/backup/4'", start, end, 4,
		)
	})

	t.Run("table-level backups", func(t *testing.T) {
		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		start := getTime(t)
		db.Exec(t, fmt.Sprintf(
			fullBackupAostCmd, 5, start,
		))

		db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
		db.Exec(t, fmt.Sprintf(incBackupCmd, 5))
		db.Exec(t, "UPDATE foo SET b = b + 1 WHERE a = 2")
		db.Exec(t, "DELETE FROM foo WHERE a = 1")
		end := getTime(t)
		db.Exec(
			t,
			fmt.Sprintf(
				incBackupAostCmd, 5, end,
			),
		)

		var backupPath string
		db.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup/5'").Scan(&backupPath)
		jobutils.WaitForJobToSucceed(t, db, startCompaction(5, backupPath, start, end))
		validateCompactedBackupForTables(
			t, db, []string{"foo"}, "'nodelocal://1/backup/5'", start, end, 2,
		)
	})

	t.Run("encrypted backups", func(t *testing.T) {
		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
			db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		opts := "encryption_passphrase = 'correct-horse-battery-staple'"
		start := getTime(t)
		db.Exec(t, fmt.Sprintf(
			fullBackupAostCmd+" WITH %s", 6, start, opts,
		))
		db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
		db.Exec(
			t,
			fmt.Sprintf(incBackupCmd+" WITH %s", 6, opts),
		)
		db.Exec(t, "UPDATE foo SET b = b + 1 WHERE a = 2")
		end := getTime(t)
		db.Exec(
			t,
			fmt.Sprintf(incBackupAostCmd+" WITH %s", 6, end, opts),
		)
		var backupPath string
		db.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup/6'").Scan(&backupPath)
		var jobID jobspb.JobID
		pause := rand.Intn(2) == 0
		if pause {
			db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'backup_compaction.after.details_has_checkpoint'")
		}
		db.QueryRow(
			t,
			fmt.Sprintf(
				`SELECT crdb_internal.backup_compaction(
ARRAY['nodelocal://1/backup/6'],
'%s',
crdb_internal.json_to_pb(
'cockroach.sql.jobs.jobspb.BackupEncryptionOptions',
'{"mode": 0, "raw_passphrase": "correct-horse-battery-staple"}'
), '%d', '%d')`,
				backupPath, start, end,
			),
		).Scan(&jobID)
		if pause {
			jobutils.WaitForJobToPause(t, db, jobID)
			db.Exec(t, "RESUME JOB $1", jobID)
		}
		jobutils.WaitForJobToSucceed(t, db, jobID)
		validateCompactedBackupForTablesWithOpts(
			t, db, []string{"foo"}, "'nodelocal://1/backup/6'", start, end, 2, opts,
		)
	})

	t.Run("pause resume and cancel", func(t *testing.T) {
		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
			db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		start := getTime(t)
		db.Exec(t, fmt.Sprintf(fullBackupAostCmd, 7, start))
		db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
		db.Exec(t, fmt.Sprintf(incBackupCmd, 7))
		db.Exec(t, "INSERT INTO foo VALUES (3, 3)")
		end := getTime(t)
		db.Exec(t, fmt.Sprintf(incBackupAostCmd, 7, end))
		db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'backup_compaction.after.details_has_checkpoint'")

		var backupPath string
		db.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup/7'").Scan(&backupPath)
		jobID := startCompaction(7, backupPath, start, end)
		jobutils.WaitForJobToPause(t, db, jobID)
		db.Exec(t, "RESUME JOB $1", jobID)
		jobutils.WaitForJobToSucceed(t, db, jobID)
		validateCompactedBackupForTables(
			t, db, []string{"foo"}, "'nodelocal://1/backup/7'", start, end, 2,
		)

		db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
		db.Exec(t, "INSERT INTO foo VALUES (4, 4)")
		end = getTime(t)
		db.Exec(t, fmt.Sprintf(incBackupAostCmd, 7, end))
		db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'backup_compaction.after.details_has_checkpoint'")
		jobID = startCompaction(7, backupPath, start, end)
		jobutils.WaitForJobToPause(t, db, jobID)
		db.Exec(t, "CANCEL JOB $1", jobID)
		jobutils.WaitForJobToCancel(t, db, jobID)
	})

	t.Run("builtin using backup statement", func(t *testing.T) {
		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		start := getTime(t)
		opts := "encryption_passphrase = 'correct-horse-battery-staple'"
		db.Exec(t, fmt.Sprintf(fullBackupAostCmd+" WITH %s", 9, start, opts))
		db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
		db.Exec(t, fmt.Sprintf(incBackupCmd+" WITH %s", 9, opts))
		db.Exec(t, "INSERT INTO foo VALUES (3, 3)")
		end := getTime(t)
		db.Exec(t, fmt.Sprintf(incBackupAostCmd+" WITH %s", 9, end, opts))
		var backupPath string
		db.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup/9'").Scan(&backupPath)
		var jobID jobspb.JobID
		db.QueryRow(t, fmt.Sprintf(
			`SELECT crdb_internal.backup_compaction(
        'BACKUP INTO LATEST IN ''nodelocal://1/backup/9'' WITH encryption_passphrase = ''correct-horse-battery-staple''',
        '%s', %d::DECIMAL, %d::DECIMAL
      )`, backupPath, start, end,
		)).Scan(&jobID)
		jobutils.WaitForJobToSucceed(t, db, jobID)
		validateCompactedBackupForTablesWithOpts(
			t, db, []string{"foo"}, "'nodelocal://1/backup/9'", start, end, 2, opts,
		)
	})

	t.Run("compaction of chain ending in a compacted backup", func(t *testing.T) {
		// This test is to ensure that the second compaction job does not
		// clobber the first compaction in the bucket. To do so, we take the
		// following chain:
		// F -> I1 -> I2 -> I3
		// We compact I2 and I3 to get C1 and then compact I1 and C1 to get C2.
		// Both C1 and C2 will have the same end time, but C2 should not clobber C1.
		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		start := getTime(t)
		db.Exec(t, fmt.Sprintf(fullBackupAostCmd, 11, start))

		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		mid := getTime(t)
		db.Exec(t, fmt.Sprintf(incBackupAostCmd, 11, mid))

		db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
		db.Exec(t, fmt.Sprintf(incBackupCmd, 11))

		db.Exec(t, "INSERT INTO foo VALUES (3, 3)")
		end := getTime(t)
		db.Exec(t, fmt.Sprintf(incBackupAostCmd, 11, end))

		var backupPath string
		db.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup/11'").Scan(&backupPath)
		c1JobID := startCompaction(11, backupPath, mid, end)
		jobutils.WaitForJobToSucceed(t, db, c1JobID)

		c2JobID := startCompaction(11, backupPath, start, end)
		jobutils.WaitForJobToSucceed(t, db, c2JobID)
		ensureBackupExists(t, db, "'nodelocal://1/backup/11'", mid, end, "")
		ensureBackupExists(t, db, "'nodelocal://1/backup/11'", start, end, "")
	})
	// TODO (kev-cao): Once range keys are supported by the compaction
	// iterator, add tests for dropped tables/indexes.
}

func TestScheduledBackupCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var blockCompaction chan struct{}
	var testKnobs []func(*base.TestingKnobs)
	if rand.Intn(2) == 0 {
		// Artificially force a scheduled full backup to also execute before the
		// compaction job resolves its destination, which will cause the LATEST
		// file to be updated. This is to ensure that the compaction job correctly
		// identifies the full backup it belongs to and does not attempt to chain
		// off of the second full backup.
		blockCompaction = make(chan struct{})
		defer close(blockCompaction)
		testKnobs = append(testKnobs, func(testKnobs *base.TestingKnobs) {
			testKnobs.BackupRestore = &sql.BackupRestoreTestingKnobs{
				RunBeforeResolvingCompactionDest: func() error {
					<-blockCompaction
					return nil
				},
			}
		})
	}
	th, cleanup := newTestHelper(t, testKnobs...)
	defer cleanup()

	th.setOverrideAsOfClauseKnob(t)
	// Time is set to a time such that no full backup will unexpectedly run as we
	// artificially time travel. This ensures deterministic behavior that is not
	// impacted by when the test runs.
	th.env.SetTime(time.Date(2025, 3, 27, 1, 0, 0, 0, time.UTC))

	th.sqlDB.Exec(t, "SET CLUSTER SETTING backup.compaction.threshold = 4")
	th.sqlDB.Exec(t, "SET CLUSTER SETTING backup.compaction.window_size = 3")
	schedules, err := th.createBackupSchedule(
		t, "CREATE SCHEDULE FOR BACKUP INTO $1 RECURRING '@hourly'", "nodelocal://1/backup",
	)
	require.NoError(t, err)
	require.Equal(t, 2, len(schedules))

	full, inc := schedules[0], schedules[1]
	if full.IsPaused() {
		full, inc = inc, full
	}

	th.env.SetTime(full.NextRun().Add(time.Second))
	require.NoError(t, th.executeSchedules())
	th.waitForSuccessfulScheduledJob(t, full.ScheduleID())
	var backupPath string
	th.sqlDB.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup'").Scan(&backupPath)

	for range 3 {
		inc, err = jobs.ScheduledJobDB(th.internalDB()).
			Load(context.Background(), th.env, inc.ScheduleID())
		require.NoError(t, err)

		th.env.SetTime(inc.NextRun().Add(time.Second))
		require.NoError(t, th.executeSchedules())
		th.waitForSuccessfulScheduledJob(t, inc.ScheduleID())
	}

	if blockCompaction != nil {
		t.Log("executing second full backup before compaction job resolves destination")
		// Instead of fast forwarding to the full backup's next run, which can result in additional
		// incrementeals being triggered by `executeSchedules`, we update the full's next run to the
		// current time.
		th.sqlDB.QueryStr(t, fmt.Sprintf("ALTER BACKUP SCHEDULE %d EXECUTE FULL IMMEDIATELY", full.ScheduleID()))
		full, err = jobs.ScheduledJobDB(th.internalDB()).
			Load(context.Background(), th.env, full.ScheduleID())
		require.NoError(t, err)
		th.env.SetTime(full.NextRun().Add(time.Second))
		require.NoError(t, th.executeSchedules())
		th.waitForSuccessfulScheduledJob(t, full.ScheduleID())
		blockCompaction <- struct{}{}
	}

	var jobID jobspb.JobID
	// The scheduler is notified of the backup job completion and then the
	// compaction job is created in a separate transaction. As such, we need to
	// poll for the compaction job to be created.
	testutils.SucceedsSoon(t, func() error {
		return th.sqlDB.DB.QueryRowContext(
			ctx,
			`SELECT job_id FROM [SHOW JOBS] WHERE description ILIKE 'COMPACT%' AND job_type = 'BACKUP'`,
		).Scan(&jobID)
	})

	testutils.SucceedsSoon(t, func() error {
		th.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
		var unused int64
		return th.sqlDB.DB.QueryRowContext(
			ctx,
			"SELECT job_id FROM [SHOW JOBS] WHERE job_id = $1 AND status = $2",
			jobID, jobs.StateSucceeded,
		).Scan(&unused)
	})

	var numBackups int
	th.sqlDB.QueryRow(
		t,
		fmt.Sprintf("SELECT count(DISTINCT (start_time, end_time)) FROM "+
			"[SHOW BACKUP FROM '%s' IN 'nodelocal://1/backup']", backupPath),
	).Scan(&numBackups)
	require.Equal(t, 5, numBackups)
}

func TestBackupCompactionUnsupportedOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	backupCompactionThreshold.Override(ctx, &st.SV, 3)
	execCfg := sql.ExecutorConfig{
		Settings: st,
	}
	testcases := []struct {
		name    string
		details jobspb.BackupDetails
		error   string
	}{
		{
			"execution locality not supported",
			jobspb.BackupDetails{
				ScheduleID: 1,
				ExecutionLocality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "region", Value: "us-east-2"},
					},
				},
			},
			"execution locality not supported for compaction",
		},
		{
			"revision history not supported",
			jobspb.BackupDetails{
				ScheduleID:      1,
				RevisionHistory: true,
			},
			"revision history not supported for compaction",
		},
		{
			"scheduled backups only",
			jobspb.BackupDetails{
				ScheduleID: 0,
			},
			"only scheduled backups can be compacted",
		},
		{
			"incremental locations not supported",
			jobspb.BackupDetails{
				ScheduleID: 1,
				Destination: jobspb.BackupDetails_Destination{
					IncrementalStorage: []string{"nodelocal://1/backup/incs"},
				},
			},
			"custom incremental storage location not supported for compaction",
		},
		{
			"tenant specific backups not supported",
			jobspb.BackupDetails{
				ScheduleID:        1,
				SpecificTenantIds: []roachpb.TenantID{roachpb.MustMakeTenantID(12)},
			},
			"backups of tenants not supported for compaction",
		},
		{
			"backups including tenants not supported",
			jobspb.BackupDetails{
				ScheduleID:                 1,
				IncludeAllSecondaryTenants: true,
			},
			"backups of tenants not supported for compaction",
		},
		{
			"locality aware backups not supported",
			jobspb.BackupDetails{
				ScheduleID: 1,
				URIsByLocalityKV: map[string]string{
					"region=us-east-2": "nodelocal://1/backup",
				},
			},
			"locality aware backups not supported for compaction",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			tc.details.StartTime = hlc.Timestamp{WallTime: 100} // Nonzero start time
			jobID, err := maybeStartCompactionJob(
				ctx, &execCfg, username.RootUserName(), tc.details,
			)
			require.Zero(t, jobID)
			require.ErrorContains(t, err, tc.error)
		})
	}
}

func TestCompactionCheckpointing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The backup needs to be large enough that checkpoints will be created, so
	// we pick a large number of accounts.
	const numAccounts = 1000
	var manifestNumFiles atomic.Int32
	manifestNumFiles.Store(-1) // -1 means we haven't seen the manifests yet.
	_, db, _, cleanup := backupRestoreTestSetupWithParams(
		t, singleNode, numAccounts, InitManualReplication, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					BackupRestore: &sql.BackupRestoreTestingKnobs{
						AfterLoadingCompactionManifestOnResume: func(m *backuppb.BackupManifest) {
							manifestNumFiles.Store(int32(len(m.Files)))
						},
					},
				},
			},
		},
	)
	defer cleanup()
	writeQueries := func() {
		db.Exec(t, "UPDATE data.bank SET balance = balance + 1")
	}
	db.Exec(t, "SET CLUSTER SETTING bulkio.backup.checkpoint_interval = '10ms'")
	start := getTime(t)
	db.Exec(t, fmt.Sprintf("BACKUP INTO 'nodelocal://1/backup' AS OF SYSTEM TIME %d", start))
	writeQueries()
	db.Exec(t, "BACKUP INTO LATEST IN 'nodelocal://1/backup'")
	writeQueries()
	end := getTime(t)
	db.Exec(t, fmt.Sprintf("BACKUP INTO LATEST IN 'nodelocal://1/backup' AS OF SYSTEM TIME %d", end))

	var backupPath string
	db.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup'").Scan(&backupPath)

	db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'backup_compaction.after.write_checkpoint'")
	var jobID jobspb.JobID
	db.QueryRow(
		t,
		"SELECT crdb_internal.backup_compaction(ARRAY['nodelocal://1/backup'], $1, ''::BYTES, $2, $3)",
		backupPath, start, end,
	).Scan(&jobID)
	// Ensure that the very first manifest when the job is initially started has
	// no files.
	testutils.SucceedsSoon(t, func() error {
		if manifestNumFiles.Load() < 0 {
			return fmt.Errorf("waiting for manifest to be loaded")
		}
		return nil
	})
	require.Equal(t, 0, int(manifestNumFiles.Load()), "expected no files in manifest")

	// Wait for the job to hit the pausepoint.
	jobutils.WaitForJobToPause(t, db, jobID)
	// Don't bother pausing on other checkpoints.
	db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
	db.Exec(t, "RESUME JOB $1", jobID)
	jobutils.WaitForJobToRun(t, db, jobID)

	// Now that the job has been paused and resumed after previously hitting a
	// checkpoint, the initial manifest at the start of the job should have
	// some files from before the pause.
	testutils.SucceedsSoon(t, func() error {
		if manifestNumFiles.Load() <= 0 {
			return fmt.Errorf("waiting for manifest to be loaded")
		}
		return nil
	})
	require.Greater(t, int(manifestNumFiles.Load()), 0, "expected non-zero number of files in manifest")

	jobutils.WaitForJobToSucceed(t, db, jobID)
	validateCompactedBackupForTables(t, db, []string{"bank"}, "'nodelocal://1/backup'", start, end, 2)
}

func TestToggleCompactionForRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc, db, _, cleanup := backupRestoreTestSetup(
		t, singleNode, 1 /* numAccounts */, InitManualReplication,
	)
	defer cleanup()
	start := getTime(t)
	db.Exec(t, fmt.Sprintf("BACKUP INTO 'nodelocal://1/backup' AS OF SYSTEM TIME '%d'", start))
	db.Exec(t, "BACKUP INTO LATEST IN 'nodelocal://1/backup'")
	end := getTime(t)
	db.Exec(t, fmt.Sprintf("BACKUP INTO LATEST IN 'nodelocal://1/backup' AS OF SYSTEM TIME '%d'", end))
	var backupPath string
	db.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup'").Scan(&backupPath)
	var compactionID jobspb.JobID
	db.QueryRow(
		t,
		fmt.Sprintf(
			`SELECT crdb_internal.backup_compaction(
				'BACKUP INTO LATEST IN ''nodelocal://1/backup''',
				'%s', %d::DECIMAL, %d::DECIMAL
			)`,
			backupPath, start, end,
		),
	).Scan(&compactionID)
	waitForSuccessfulJob(t, tc, compactionID)

	var compRestoreID, classicRestoreID jobspb.JobID
	var unused any
	db.QueryRow(
		t, "RESTORE DATABASE data FROM LATEST IN 'nodelocal://1/backup' WITH new_db_name = 'data1'",
	).Scan(&compRestoreID, &unused, &unused, &unused)
	db.Exec(t, "SET CLUSTER SETTING restore.compacted_backups.enabled = false")
	db.QueryRow(
		t, "RESTORE DATABASE data FROM LATEST IN 'nodelocal://1/backup' WITH new_db_name = 'data2'",
	).Scan(&classicRestoreID, &unused, &unused, &unused)

	require.Equal(t, 2, getNumBackupsInRestore(t, db, compRestoreID))
	require.Equal(t, 3, getNumBackupsInRestore(t, db, classicRestoreID))
}

func TestCheckCompactionManifestFields(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// NOTE: Whenever a new field is added to the backup manifest, it will need to be
	// added to the corresponding lists below.
	inherited := []string{
		"EndTime",
		"Spans",
		"CompleteDbs",
		"NodeID",
		"ClusterID",
		"ClusterVersion",
		"FormatVersion",
		"BuildInfo",
		"DescriptorCoverage",
		"StatisticsFilenames",
		"ElidedPrefix",
	}
	overridden := []string{
		"ID",
		"StartTime",
		"IntroducedSpans",
		"IsCompacted",
	}
	emptied := []string{
		"Dir",
		"DescriptorChanges",
		"Files",
		"EntryCounts",
	}
	// Ignored fields are fields that we do not check because either:
	// 1. They are no longer used
	// 2. There is a case-specific reason for not checking them
	// 3. Compaction does not support these fields and there are safeguards
	// against compaction running against such backups. The test is simpler if we
	// ignore these fields.
	ignored := []string{
		"TenantsDeprecated",
		"DeprecatedStatistics",
		// HasExternalManifestSSTs is a unique case because it will be set to true
		// for all manifests, except when creating a new manifest, it is always set
		// to false until the manifest has been split up and written.
		"HasExternalManifestSSTs",
		// Descriptors is ignored because an actual backup manifest loaded from
		// storage will have this field emptied due to using slim manifests whereas
		// createCompactedManifest will have filled this out.
		"Descriptors",
		"Tenants",
		"LocalityKVs",
		"PartitionDescriptorFilenames",
		"MVCCFilter",
		"RevisionStartTime",
	}

	statisticsFilenames := make(map[descpb.ID]string)
	statisticsFilenames[1] = "foo"
	lastBackup := backuppb.BackupManifest{
		StartTime:         hlc.Timestamp{WallTime: 2},
		EndTime:           hlc.Timestamp{WallTime: 3},
		RevisionStartTime: hlc.Timestamp{WallTime: 2},
		Spans: []roachpb.Span{
			{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
			{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
			{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
		},
		IntroducedSpans: []roachpb.Span{
			{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
		},
		DescriptorChanges: []backuppb.BackupManifest_DescriptorRevision{
			{},
		},
		Files: []backuppb.BackupManifest_File{{}},
		Descriptors: []descpb.Descriptor{
			{
				Union: &descpb.Descriptor_Table{
					Table: &descpb.TableDescriptor{
						ID:   descpb.ID(1),
						Name: "table",
					},
				},
			},
		},
		CompleteDbs: []descpb.ID{1},
		EntryCounts: roachpb.RowCount{
			Rows: 1,
		},
		Dir: cloudpb.ExternalStorage{
			Provider: cloudpb.ExternalStorageProvider_external,
		},
		FormatVersion: 1,
		ClusterID:     uuid.MakeV4(),
		NodeID:        1,
		BuildInfo: build.Info{
			Tag: "v1.0.0",
		},
		ClusterVersion:      roachpb.Version{Major: 1},
		ID:                  uuid.MakeV4(),
		StatisticsFilenames: statisticsFilenames,
		DescriptorCoverage:  tree.AllDescriptors,
		ElidedPrefix:        execinfrapb.ElidePrefix_TenantAndTable,
		IsCompacted:         false,
	}
	lastBackupStruct := structs.New(lastBackup)

	// Test that all fields in a backup manifest have been explicitly specified in
	// this test. This ensures that if the backup manifest fields are ever
	// updated, this test forces the developer to ensure the manifest creation for
	// compacted backups is correct.
	specifiedFields := make([]string, 0, len(ignored)+len(inherited)+len(overridden)+len(emptied))
	specifiedFields = append(specifiedFields, ignored...)
	specifiedFields = append(specifiedFields, inherited...)
	specifiedFields = append(specifiedFields, overridden...)
	specifiedFields = append(specifiedFields, emptied...)
	for _, field := range lastBackupStruct.Names() {
		if !slices.Contains(specifiedFields, field) {
			t.Fatalf("field %s not specified in test", field)
		}
	}

	// Ensuring that fields that are supposed to be inherited/emptied contain
	// non-zero values in the original backup manifest. This avoids false
	// positives such as the test reporting a field was properly emptied but only
	// because the original backup already had it empty.
	checkNotEmpty := make([]string, 0, len(inherited)+len(emptied))
	checkNotEmpty = append(checkNotEmpty, inherited...)
	checkNotEmpty = append(checkNotEmpty, emptied...)
	for _, field := range checkNotEmpty {
		require.Falsef(
			t, lastBackupStruct.Field(field).IsZero(),
			"field %s should not be empty to avoid false positives", field,
		)
	}

	compactChain := compactionChain{
		backupChain: []backuppb.BackupManifest{
			// Create some additional backups such that IntroducedSpans for the
			// compacted backup should be different from the last backup.
			{
				EndTime: hlc.Timestamp{WallTime: 1},
				Spans: []roachpb.Span{
					{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
				},
			},
			{
				StartTime: hlc.Timestamp{WallTime: 1},
				EndTime:   hlc.Timestamp{WallTime: 2},
				Spans: []roachpb.Span{
					{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
				},
				IntroducedSpans: []roachpb.Span{
					{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
				},
			},
			lastBackup,
		},
		start:    hlc.Timestamp{WallTime: 1},
		end:      hlc.Timestamp{WallTime: 3},
		startIdx: 1,
		endIdx:   3,
	}

	compactManifest, err := compactChain.createCompactionManifest(
		context.Background(), jobspb.BackupDetails{},
	)
	require.NoError(t, err)
	compactManifestStruct := structs.New(compactManifest)

	for _, field := range inherited {
		require.Equalf(
			t, lastBackupStruct.Field(field).Value(), compactManifestStruct.Field(field).Value(),
			"field %s should be inherited", field,
		)
	}
	for _, field := range overridden {
		require.NotEqualf(
			t, lastBackupStruct.Field(field).Value(), compactManifestStruct.Field(field).Value(),
			"field %s should be overridden", field,
		)
	}
	for _, field := range emptied {
		require.Truef(t, compactManifestStruct.Field(field).IsZero(), "field %s should be empty", field)
	}
}

// validateCompactedBackupForTables is a wrapper around
// validateCompactedBackupForTablesWithOpts that passes in no options.
func validateCompactedBackupForTables(
	t *testing.T,
	db *sqlutils.SQLRunner,
	tables []string,
	collectionURIs string,
	start, end int64,
	numBackups int,
) {
	t.Helper()
	validateCompactedBackupForTablesWithOpts(t, db, tables, collectionURIs, start, end, numBackups, "")
}

// validateCompactedBackupForTablesWithOpts validates that a compacted backup
// with the specified start and end time (in nanoseconds) exists and restores
// from that backup. It checks that the tables specified have the same contents
// before and after the restore. It also checks that the restore process used
// the number of backups specified in numBackups.
func validateCompactedBackupForTablesWithOpts(
	t *testing.T,
	db *sqlutils.SQLRunner,
	tables []string,
	collectionURIs string,
	start, end int64,
	numBackups int,
	opts string,
) {
	t.Helper()
	ensureBackupExists(t, db, collectionURIs, start, end, opts)
	rows := make(map[string][][]string)
	for _, table := range tables {
		rows[table] = db.QueryStr(t, "SELECT * FROM "+table)
	}
	tablesList := strings.Join(tables, ", ")
	db.Exec(t, "DROP TABLE "+tablesList)
	restoreQuery := fmt.Sprintf("RESTORE TABLE %s FROM LATEST IN (%s)", tablesList, collectionURIs)
	if opts != "" {
		restoreQuery += " WITH " + opts
	}
	row := db.QueryRow(t, restoreQuery)
	var restoreJobID jobspb.JobID
	var discard *any
	row.Scan(&restoreJobID, &discard, &discard, &discard)
	for table, originalRows := range rows {
		restoredRows := db.QueryStr(t, "SELECT * FROM "+table)
		require.Equal(t, originalRows, restoredRows, "table %s", table)
	}

	require.Equal(t, numBackups, getNumBackupsInRestore(t, db, restoreJobID))
}

// getNumBackupsInRestore returns the number of backups used in the restore
func getNumBackupsInRestore(t *testing.T, db *sqlutils.SQLRunner, jobID jobspb.JobID) int {
	t.Helper()
	var detailsStr string
	db.QueryRow(t, `SELECT crdb_internal.pb_to_json(
		'cockroach.sql.jobs.jobspb.Payload',
		value
	)::JSONB->>'restore'
	FROM system.job_info
	WHERE job_id = $1
	AND info_key = 'legacy_payload';
	`, jobID).Scan(&detailsStr)
	var details jobspb.RestoreDetails
	require.NoError(t, json.Unmarshal([]byte(detailsStr), &details))
	return len(details.URIs)
}

// ensureBackupExists ensures that a backup exists that spans the given start and end times.
func ensureBackupExists(
	t *testing.T, db *sqlutils.SQLRunner, collectionURIs string, start, end int64, opts string,
) {
	t.Helper()
	showBackupQ := fmt.Sprintf(`SHOW BACKUP FROM LATEST IN (%s)`, collectionURIs)
	if opts != "" {
		showBackupQ += " WITH " + opts
	}
	// Convert times to millisecond epoch. We compare millisecond epoch instead of
	// nanosecond epoch because the backup time is stored in milliseconds, but timeutil.Now()
	// will return a nanosecond-precise epoch.
	times := db.Query(t,
		fmt.Sprintf(`SELECT DISTINCT
		COALESCE(start_time::DECIMAL * 1e6, 0),
		COALESCE(end_time::DECIMAL * 1e6, 0)
		FROM [%s]`, showBackupQ),
	)
	defer times.Close()
	found := false
	for times.Next() {
		var startTime, endTime int64
		require.NoError(t, times.Scan(&startTime, &endTime))
		if startTime == start/1e3 && endTime == end/1e3 {
			found = true
			break
		}
	}
	require.True(t, found, "missing backup with start time %d and end time %d", start, end)
}

// getTime returns the current time in nanoseconds since epoch.
func getTime(t *testing.T) int64 {
	t.Helper()
	time, err := strconv.ParseFloat(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}.AsOfSystemTime(), 64)
	require.NoError(t, err)
	return int64(time)
}
