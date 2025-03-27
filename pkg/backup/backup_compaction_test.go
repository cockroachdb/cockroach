// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	tc, db, cleanupDB := backupRestoreTestSetupEmpty(
		t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Settings: st,
			},
		},
	)
	defer cleanupDB()

	// Expects start/end to be nanosecond epoch.
	startCompaction := func(bucket int, start, end int64) jobspb.JobID {
		compactionBuiltin := `SELECT crdb_internal.backup_compaction(
ARRAY['nodelocal://1/backup/%d'], 'LATEST', ''::BYTES, %d::DECIMAL, %d::DECIMAL
)`
		row := db.QueryRow(t, fmt.Sprintf(compactionBuiltin, bucket, start, end))
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
			db.Exec(t, "DROP TABLE foo")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		start := getTime(t)
		db.Exec(t, fmt.Sprintf(fullBackupAostCmd, 1, start))
		var backupPath string
		db.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup/1'").Scan(&backupPath)

		// Run twice to test compaction on top of compaction.
		var firstCompactionEndTime int64
		for range 2 {
			db.Exec(t, "INSERT INTO foo VALUES (2, 2), (3, 3)")
			db.Exec(t, fmt.Sprintf(incBackupCmd, 1))
			db.Exec(t, "UPDATE foo SET b = b + 1 WHERE a = 2")
			mid := getTime(t)
			db.Exec(t, fmt.Sprintf(incBackupAostCmd, 1, mid))
			db.Exec(t, "DELETE FROM foo WHERE a = 3")
			end := getTime(t)
			db.Exec(
				t,
				fmt.Sprintf(incBackupAostCmd, 1, end),
			)
			waitForSuccessfulJob(t, tc, startCompaction(1, start, end))
			// Check we can restore to an intermediate time spanned by a compacted backup
			db.Exec(t, fmt.Sprintf("RESTORE DATABASE defaultdb FROM LATEST IN 'nodelocal://1/backup/1' AS OF SYSTEM TIME '%d' with new_db_name=defaultdb2", mid))
			db.Exec(t, "DROP DATABASE defaultdb2 CASCADE")

			validateCompactedBackupForTables(t, db, []string{"foo"}, "'nodelocal://1/backup/1'", start, end)
			if firstCompactionEndTime == 0 {
				firstCompactionEndTime = end
			}
		}

		// Check we can restore from an intermediate time that points to a compacted backup that is also spanned by a compacted backup.
		db.Exec(t, fmt.Sprintf("RESTORE DATABASE defaultdb FROM LATEST IN 'nodelocal://1/backup/1' AS OF SYSTEM TIME '%d' with new_db_name=defaultdb2", firstCompactionEndTime))
		db.Exec(t, "DROP DATABASE defaultdb2 CASCADE")

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
		waitForSuccessfulJob(t, tc, startCompaction(2, start, end))
		validateCompactedBackupForTables(
			t, db,
			[]string{"foo", "bar", "baz"},
			"'nodelocal://1/backup/2'",
			start, end,
		)

		db.Exec(t, "DROP TABLE bar")
		end = getTime(t)
		db.Exec(
			t,
			fmt.Sprintf(incBackupAostCmd, 2, end),
		)
		waitForSuccessfulJob(t, tc, startCompaction(2, start, end))

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
		waitForSuccessfulJob(t, tc, startCompaction(3, start, end))

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

		waitForSuccessfulJob(t, tc, startCompaction(4, start, end))
		validateCompactedBackupForTables(t, db, []string{"foo"}, "'nodelocal://1/backup/4'", start, end)
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

		waitForSuccessfulJob(t, tc, startCompaction(5, start, end))
		validateCompactedBackupForTables(t, db, []string{"foo"}, "'nodelocal://1/backup/5'", start, end)
	})

	t.Run("encrypted backups", func(t *testing.T) {
		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
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
		var jobID jobspb.JobID
		db.QueryRow(
			t,
			fmt.Sprintf(
				`SELECT crdb_internal.backup_compaction(
ARRAY['nodelocal://1/backup/6'], 
'LATEST',
crdb_internal.json_to_pb(
'cockroach.sql.jobs.jobspb.BackupEncryptionOptions',
'{"mode": 0, "raw_passphrase": "correct-horse-battery-staple"}'
), '%d', '%d')`,
				start, end,
			),
		).Scan(&jobID)
		waitForSuccessfulJob(t, tc, jobID)
		validateCompactedBackupForTablesWithOpts(
			t, db, []string{"foo"}, "'nodelocal://1/backup/6'", start, end, opts,
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
		db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'backup.after.details_has_checkpoint'")

		jobID := startCompaction(7, start, end)
		jobutils.WaitForJobToPause(t, db, jobID)
		db.Exec(t, "RESUME JOB $1", jobID)
		waitForSuccessfulJob(t, tc, jobID)
		validateCompactedBackupForTables(t, db, []string{"foo"}, "'nodelocal://1/backup/7'", start, end)

		db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
		db.Exec(t, "INSERT INTO foo VALUES (4, 4)")
		end = getTime(t)
		db.Exec(t, fmt.Sprintf(incBackupAostCmd, 7, end))
		db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'backup.after.details_has_checkpoint'")
		jobID = startCompaction(7, start, end)
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
		var jobID jobspb.JobID
		db.QueryRow(t, fmt.Sprintf(
			`SELECT crdb_internal.backup_compaction(
        'BACKUP INTO LATEST IN ''nodelocal://1/backup/9'' WITH encryption_passphrase = ''correct-horse-battery-staple''', 
        %d::DECIMAL, %d::DECIMAL
      )`, start, end,
		)).Scan(&jobID)
		waitForSuccessfulJob(t, tc, jobID)
		validateCompactedBackupForTablesWithOpts(
			t, db, []string{"foo"}, "'nodelocal://1/backup/9'", start, end, opts,
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

		c1JobID := startCompaction(11, mid, end)
		waitForSuccessfulJob(t, tc, c1JobID)

		c2JobID := startCompaction(11, start, end)
		waitForSuccessfulJob(t, tc, c2JobID)
		ensureBackupExists(t, db, "'nodelocal://1/backup/11'", mid, end, "")
		ensureBackupExists(t, db, "'nodelocal://1/backup/11'", start, end, "")
	})
	// TODO (kev-cao): Once range keys are supported by the compaction
	// iterator, add tests for dropped tables/indexes.
}

func TestBackupCompactionLocalityAware(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderDuress(t, "node startup is slow")

	tempDir, tempDirCleanup := testutils.TempDir(t)
	defer tempDirCleanup()
	tc, db, cleanupDB := backupRestoreTestSetupEmpty(
		t, multiNode, tempDir, InitManualReplication,
		base.TestClusterArgs{
			ServerArgsPerNode: map[int]base.TestServerArgs{
				0: {
					Locality: roachpb.Locality{Tiers: []roachpb.Tier{
						{Key: "region", Value: "west"},
						{Key: "az", Value: "az1"},
						{Key: "dc", Value: "dc1"},
					}},
				},
				1: {
					Locality: roachpb.Locality{Tiers: []roachpb.Tier{
						{Key: "region", Value: "east"},
						{Key: "az", Value: "az1"},
						{Key: "dc", Value: "dc2"},
					}},
				},
				2: {
					Locality: roachpb.Locality{Tiers: []roachpb.Tier{
						{Key: "region", Value: "east"},
						{Key: "az", Value: "az2"},
						{Key: "dc", Value: "dc3"},
					}},
				},
			},
		},
	)
	defer cleanupDB()
	collectionURIs := strings.Join([]string{
		fmt.Sprintf(
			"'nodelocal://1/backup?COCKROACH_LOCALITY=%s'",
			url.QueryEscape("default"),
		),
		fmt.Sprintf(
			"'nodelocal://2/backup?COCKROACH_LOCALITY=%s'",
			url.QueryEscape("dc=dc2"),
		),
		fmt.Sprintf(
			"'nodelocal://3/backup?COCKROACH_LOCALITY=%s'",
			url.QueryEscape("region=west"),
		),
	}, ", ")
	db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
	db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
	start := getTime(t)
	db.Exec(
		t,
		fmt.Sprintf("BACKUP INTO (%s) AS OF SYSTEM TIME '%d'", collectionURIs, start),
	)

	db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
	db.Exec(
		t,
		fmt.Sprintf("BACKUP INTO LATEST IN (%s)", collectionURIs),
	)

	db.Exec(t, "INSERT INTO foo VALUES (3, 3)")
	end := getTime(t)
	db.Exec(
		t,
		fmt.Sprintf("BACKUP INTO LATEST IN (%s) AS OF SYSTEM TIME '%d'", collectionURIs, end),
	)
	compactionBuiltin := "SELECT crdb_internal.backup_compaction(ARRAY[%s], 'LATEST', '', %d::DECIMAL, %d::DECIMAL)"
	row := db.QueryRow(t, fmt.Sprintf(compactionBuiltin, collectionURIs, start, end))
	var jobID jobspb.JobID
	row.Scan(&jobID)
	waitForSuccessfulJob(t, tc, jobID)
	validateCompactedBackupForTables(t, db, []string{"foo"}, collectionURIs, start, end)
}

func TestScheduledBackupCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 143543, "flaky test")

	ctx := context.Background()
	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.setOverrideAsOfClauseKnob(t)

	th.sqlDB.Exec(t, "SET CLUSTER SETTING backup.compaction.threshold = 3")
	th.sqlDB.Exec(t, "SET CLUSTER SETTING backup.compaction.window_size = 2")
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

	inc, err = jobs.ScheduledJobDB(th.internalDB()).
		Load(context.Background(), th.env, inc.ScheduleID())
	require.NoError(t, err)

	th.env.SetTime(inc.NextRun().Add(time.Second))
	require.NoError(t, th.executeSchedules())
	th.waitForSuccessfulScheduledJob(t, inc.ScheduleID())

	inc, err = jobs.ScheduledJobDB(th.internalDB()).
		Load(context.Background(), th.env, inc.ScheduleID())
	require.NoError(t, err)

	th.env.SetTime(inc.NextRun().Add(time.Second))
	require.NoError(t, th.executeSchedules())
	th.waitForSuccessfulScheduledJob(t, inc.ScheduleID())

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
		"SELECT count(DISTINCT (start_time, end_time)) FROM "+
			"[SHOW BACKUP FROM LATEST IN 'nodelocal://1/backup']",
	).Scan(&numBackups)
	require.Equal(t, 4, numBackups)
}

// Start and end are unix epoch in nanoseconds.
func validateCompactedBackupForTables(
	t *testing.T, db *sqlutils.SQLRunner, tables []string, collectionURIs string, start, end int64,
) {
	t.Helper()
	validateCompactedBackupForTablesWithOpts(t, db, tables, collectionURIs, start, end, "")
}

// Start and end are unix epoch in nanoseconds.
func validateCompactedBackupForTablesWithOpts(
	t *testing.T,
	db *sqlutils.SQLRunner,
	tables []string,
	collectionURIs string,
	start, end int64,
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
	db.Exec(t, restoreQuery)
	for table, originalRows := range rows {
		restoredRows := db.QueryStr(t, "SELECT * FROM "+table)
		require.Equal(t, originalRows, restoredRows, "table %s", table)
	}
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
