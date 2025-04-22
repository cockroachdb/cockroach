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
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
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

	startCompaction := func(
		db *sqlutils.SQLRunner, qBuilder *backupQueryBuilder, start, end hlc.Timestamp,
	) jobspb.JobID {
		t.Helper()
		row := db.QueryRow(t, qBuilder.compactBackup(db, start, end))
		var jobID jobspb.JobID
		row.Scan(&jobID)
		return jobID
	}
	// Note: Each subtest should create their backups in their own subdirectory to
	// avoid false negatives from subtests relying on backups from other subtests.
	t.Run("basic operations insert, update, and delete", func(t *testing.T) {
		qBuilder := newBackupQueryBuilder(
			t, []string{"nodelocal://1/backup/1"}, fullCluster, noOpts,
		)

		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE IF EXISTS foo")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		start := getTime()
		db.Exec(t, qBuilder.fullBackup(start))

		// Run twice to test compaction on top of compaction.
		for i := range 2 {
			db.Exec(t, "INSERT INTO foo VALUES (2, 2), (3, 3)")
			db.Exec(t, qBuilder.incBackup(noAOST))
			db.Exec(t, "UPDATE foo SET b = b + 1 WHERE a = 2")
			db.Exec(t, qBuilder.incBackup(noAOST))
			db.Exec(t, "DELETE FROM foo WHERE a = 3")
			end := getTime()
			db.Exec(t, qBuilder.incBackup(end))
			waitForSuccessfulJob(t, tc, startCompaction(db, qBuilder, start, end))
			validateCompactedBackupForTables(t, db, qBuilder, []string{"foo"}, start, end, 2+i)
			start = end
		}

		// Ensure that additional backups were created.
		var numBackups int
		db.QueryRow(
			t, fmt.Sprintf("SELECT count(DISTINCT (start_time, end_time)) FROM [%s]", qBuilder.showBackup()),
		).Scan(&numBackups)
		require.Equal(t, 9, numBackups)
	})

	t.Run("create and drop tables", func(t *testing.T) {
		qBuilder := newBackupQueryBuilder(
			t, []string{"nodelocal://1/backup/2"}, fullCluster, noOpts,
		)

		defer func() {
			db.Exec(t, "DROP TABLE IF EXISTS foo, bar, baz")
		}()
		db.Exec(t, "CREATE TABLE foo (a INT PRIMARY KEY, b INT)")
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		start := getTime()
		db.Exec(t, qBuilder.fullBackup(start))

		db.Exec(t, "CREATE TABLE bar (a INT, b INT)")
		db.Exec(t, "INSERT INTO bar VALUES (1, 1)")
		db.Exec(t, qBuilder.incBackup(noAOST))

		db.Exec(t, "INSERT INTO bar VALUES (2, 2)")
		db.Exec(t, qBuilder.incBackup(noAOST))

		db.Exec(t, "CREATE TABLE baz (a INT, b INT)")
		db.Exec(t, "INSERT INTO baz VALUES (3, 3)")
		end := getTime()
		db.Exec(t, qBuilder.incBackup(end))

		var backupPath string
		db.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup/2'").Scan(&backupPath)
		waitForSuccessfulJob(t, tc, startCompaction(db, qBuilder, start, end))
		validateCompactedBackupForTables(t, db, qBuilder, []string{"foo", "bar", "baz"}, start, end, 2)

		db.Exec(t, "DROP TABLE bar")
		end = getTime()
		db.Exec(t, qBuilder.incBackup(end))
		waitForSuccessfulJob(t, tc, startCompaction(db, qBuilder, start, end))

		db.Exec(t, "DROP TABLE foo, baz")
		db.Exec(t, qBuilder.restore(fullCluster, noAOST))
		rows := db.QueryStr(t, "SELECT * FROM [SHOW TABLES] WHERE table_name = 'bar'")
		require.Empty(t, rows)
	})

	t.Run("create indexes", func(t *testing.T) {
		qBuilder := newBackupQueryBuilder(
			t, []string{"nodelocal://1/backup/3"}, fullCluster, noOpts,
		)

		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1), (2, 2), (3, 3)")
		start := getTime()
		db.Exec(t, qBuilder.fullBackup(start))

		db.Exec(t, "CREATE INDEX bar ON foo (a)")
		db.Exec(t, "CREATE INDEX baz ON foo (a)")
		db.Exec(t, qBuilder.incBackup(noAOST))

		db.Exec(t, "CREATE INDEX qux ON foo (b)")
		db.Exec(t, "DROP INDEX foo@bar")
		end := getTime()
		db.Exec(t, qBuilder.incBackup(end))

		waitForSuccessfulJob(t, tc, startCompaction(db, qBuilder, start, end))

		var numIndexes, restoredNumIndexes int
		db.QueryRow(t, "SELECT count(*) FROM [SHOW INDEXES FROM foo]").Scan(&numIndexes)
		db.Exec(t, "DROP TABLE foo")
		db.Exec(t, qBuilder.restore("TABLE foo", noAOST))
		db.QueryRow(t, "SELECT count(*) FROM [SHOW INDEXES FROM foo]").Scan(&restoredNumIndexes)
		require.Equal(t, numIndexes, restoredNumIndexes)
	})

	t.Run("compact middle of backup chain", func(t *testing.T) {
		qBuilder := newBackupQueryBuilder(
			t, []string{"nodelocal://1/backup/4"}, fullCluster, noOpts,
		)

		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		db.Exec(t, qBuilder.fullBackup(noAOST))

		db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
		start := getTime()
		db.Exec(t, qBuilder.incBackup(start))

		db.Exec(t, "INSERT INTO foo VALUES (3, 3)")
		db.Exec(t, qBuilder.incBackup(noAOST))

		db.Exec(t, "INSERT INTO foo VALUES (4, 4)")
		db.Exec(t, qBuilder.incBackup(noAOST))

		db.Exec(t, "INSERT INTO foo VALUES (5, 5)")
		end := getTime()
		db.Exec(t, qBuilder.incBackup(end))

		db.Exec(t, "INSERT INTO foo VALUES (6, 6)")
		db.Exec(t, qBuilder.incBackup(noAOST))

		waitForSuccessfulJob(t, tc, startCompaction(db, qBuilder, start, end))
		validateCompactedBackupForTables(t, db, qBuilder, []string{"foo"}, start, end, 4)
	})

	t.Run("table-level backups", func(t *testing.T) {
		qBuilder := newBackupQueryBuilder(
			t, []string{"nodelocal://1/backup/5"}, fullCluster, noOpts,
		)

		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		start := getTime()
		db.Exec(t, qBuilder.fullBackup(start))

		db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
		db.Exec(t, qBuilder.incBackup(noAOST))
		db.Exec(t, "UPDATE foo SET b = b + 1 WHERE a = 2")
		db.Exec(t, "DELETE FROM foo WHERE a = 1")
		end := getTime()
		db.Exec(t, qBuilder.incBackup(end))

		waitForSuccessfulJob(t, tc, startCompaction(db, qBuilder, start, end))
		validateCompactedBackupForTables(t, db, qBuilder, []string{"foo"}, start, end, 2)
	})

	t.Run("encrypted backups", func(t *testing.T) {
		qBuilder := newBackupQueryBuilder(
			t, []string{"nodelocal://1/backup/6"}, fullCluster, &tree.BackupOptions{
				EncryptionPassphrase: tree.NewDString("correct-horse-battery-staple"),
			},
		)

		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
			db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		start := getTime()
		db.Exec(t, qBuilder.fullBackup(start))

		db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
		db.Exec(t, qBuilder.incBackup(noAOST))

		db.Exec(t, "UPDATE foo SET b = b + 1 WHERE a = 2")
		end := getTime()
		db.Exec(t, qBuilder.incBackup(end))

		var jobID jobspb.JobID
		pause := rand.Intn(2) == 0
		if pause {
			db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'backup_compaction.after.details_has_checkpoint'")
		}
		db.QueryRow(t, qBuilder.compactBackup(db, start, end)).Scan(&jobID)
		if pause {
			jobutils.WaitForJobToPause(t, db, jobID)
			db.Exec(t, "RESUME JOB $1", jobID)
		}
		waitForSuccessfulJob(t, tc, jobID)
		validateCompactedBackupForTables(t, db, qBuilder, []string{"foo"}, start, end, 2)
	})

	t.Run("pause resume and cancel", func(t *testing.T) {
		qBuilder := newBackupQueryBuilder(
			t, []string{"nodelocal://1/backup/7"}, fullCluster, noOpts,
		)

		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
			db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		start := getTime()
		db.Exec(t, qBuilder.fullBackup(start))
		db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
		db.Exec(t, qBuilder.incBackup(noAOST))
		db.Exec(t, "INSERT INTO foo VALUES (3, 3)")
		end := getTime()
		db.Exec(t, qBuilder.incBackup(end))
		db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'backup_compaction.after.details_has_checkpoint'")

		jobID := startCompaction(db, qBuilder, start, end)
		jobutils.WaitForJobToPause(t, db, jobID)
		db.Exec(t, "RESUME JOB $1", jobID)
		waitForSuccessfulJob(t, tc, jobID)
		validateCompactedBackupForTables(t, db, qBuilder, []string{"foo"}, start, end, 2)

		db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
		db.Exec(t, "INSERT INTO foo VALUES (4, 4)")
		end = getTime()
		db.Exec(t, qBuilder.incBackup(end))
		db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'backup_compaction.after.details_has_checkpoint'")
		jobID = startCompaction(db, qBuilder, start, end)
		jobutils.WaitForJobToPause(t, db, jobID)
		db.Exec(t, "CANCEL JOB $1", jobID)
		jobutils.WaitForJobToCancel(t, db, jobID)
	})

	t.Run("compaction of chain ending in a compacted backup", func(t *testing.T) {
		// This test is to ensure that the second compaction job does not
		// clobber the first compaction in the bucket. To do so, we take the
		// following chain:
		// F -> I1 -> I2 -> I3
		// We compact I2 and I3 to get C1 and then compact I1 and C1 to get C2.
		// Both C1 and C2 will have the same end time, but C2 should not clobber C1.
		qBuilder := newBackupQueryBuilder(
			t, []string{"nodelocal://1/backup/8"}, fullCluster, noOpts,
		)
		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		start := getTime()
		db.Exec(t, qBuilder.fullBackup(start))

		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		mid := getTime()
		db.Exec(t, qBuilder.incBackup(mid))

		db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
		db.Exec(t, qBuilder.incBackup(noAOST))

		db.Exec(t, "INSERT INTO foo VALUES (3, 3)")
		end := getTime()
		db.Exec(t, qBuilder.incBackup(end))

		c1JobID := startCompaction(db, qBuilder, mid, end)
		waitForSuccessfulJob(t, tc, c1JobID)

		c2JobID := startCompaction(db, qBuilder, start, end)
		waitForSuccessfulJob(t, tc, c2JobID)
		ensureBackupExists(t, db, qBuilder, mid, end)
		ensureBackupExists(t, db, qBuilder, start, end)
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
	var backupPath string
	th.sqlDB.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup'").Scan(&backupPath)

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
	require.Equal(t, 4, numBackups)
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
	tc, db, _, cleanup := backupRestoreTestSetupWithParams(
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

	qBuilder := newBackupQueryBuilder(
		t, []string{"nodelocal://1/backup"}, fullCluster, noOpts,
	)

	writeQueries := func() {
		db.Exec(t, "UPDATE data.bank SET balance = balance + 1")
	}
	db.Exec(t, "SET CLUSTER SETTING bulkio.backup.checkpoint_interval = '10ms'")
	start := getTime()
	db.Exec(t, qBuilder.fullBackup(start))
	writeQueries()
	db.Exec(t, qBuilder.incBackup(noAOST))
	writeQueries()
	end := getTime()
	db.Exec(t, qBuilder.incBackup(end))

	var backupPath string
	db.QueryRow(t, qBuilder.showBackups()).Scan(&backupPath)

	db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'backup_compaction.after.write_checkpoint'")
	var jobID jobspb.JobID
	db.QueryRow(t, qBuilder.compactBackup(db, start, end)).Scan(&jobID)

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

	waitForSuccessfulJob(t, tc, jobID)
	validateCompactedBackupForTables(t, db, qBuilder, []string{"bank"}, start, end, 2)
}

// validateCompactedBackupForTables validates that a compacted backup
// with the specified start and end time (in nanoseconds) exists and restores
// from that backup. It checks that the tables specified have the same contents
// before and after the restore. It also checks that the restore process used
// the number of backups specified in numBackups.
func validateCompactedBackupForTables(
	t *testing.T,
	db *sqlutils.SQLRunner,
	qBuilder *backupQueryBuilder,
	tables []string,
	start, end hlc.Timestamp,
	numBackups int,
) {
	t.Helper()
	ensureBackupExists(t, db, qBuilder, start, end)
	rows := make(map[string][][]string)
	for _, table := range tables {
		rows[table] = db.QueryStr(t, "SELECT * FROM "+table)
	}
	tablesList := strings.Join(tables, ", ")
	db.Exec(t, "DROP TABLE "+tablesList)
	row := db.QueryRow(t, qBuilder.restore("TABLE "+tablesList, noAOST))
	var restoreJobID jobspb.JobID
	var discard *any
	row.Scan(&restoreJobID, &discard, &discard, &discard)
	for table, originalRows := range rows {
		restoredRows := db.QueryStr(t, "SELECT * FROM "+table)
		require.Equal(t, originalRows, restoredRows, "table %s", table)
	}
	// Check that the number of backups used in the restore is correct.
	var detailsStr string
	db.QueryRow(t, `SELECT crdb_internal.pb_to_json(
		'cockroach.sql.jobs.jobspb.Payload',
		value
	)::JSONB->>'restore' 
	FROM system.job_info 
	WHERE job_id = $1
	AND info_key = 'legacy_payload';
	`, restoreJobID).Scan(&detailsStr)
	var details jobspb.RestoreDetails
	require.NoError(t, json.Unmarshal([]byte(detailsStr), &details))
	require.Equal(t, numBackups, len(details.URIs))
}

// ensureBackupExists ensures that a backup exists that spans the given start and end times.
func ensureBackupExists(
	t *testing.T, db *sqlutils.SQLRunner, qBuilder *backupQueryBuilder, start, end hlc.Timestamp,
) {
	t.Helper()
	startTime, err := strconv.ParseFloat(start.AsOfSystemTime(), 64)
	require.NoError(t, err)
	endTime, err := strconv.ParseFloat(end.AsOfSystemTime(), 64)
	require.NoError(t, err)
	startNano, endNano := int64(startTime), int64(endTime)

	// Convert times to millisecond epoch. We compare millisecond epoch instead of
	// nanosecond epoch because the backup time is stored in milliseconds, but timeutil.Now()
	// will return a nanosecond-precise epoch.
	times := db.Query(t,
		fmt.Sprintf(`SELECT DISTINCT 
		COALESCE(start_time::DECIMAL * 1e6, 0), 
		COALESCE(end_time::DECIMAL * 1e6, 0) 
		FROM [%s]`, qBuilder.showBackup()),
	)
	defer times.Close()
	found := false
	for times.Next() {
		var startTime, endTime int64
		require.NoError(t, times.Scan(&startTime, &endTime))
		if startTime == startNano/1e3 && endTime == endNano/1e3 {
			found = true
			break
		}
	}
	require.True(t, found, "missing backup with start time %d and end time %d", startNano, endNano)
}

func getTime() hlc.Timestamp {
	return hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
}

// Some named variables to improve test readability.
const fullCluster = ""

var noOpts *tree.BackupOptions = nil
var noAOST = hlc.Timestamp{}

type backupQueryBuilder struct {
	t *testing.T
	// Comma separated list of targets in backup statement format
	// (e.g. "TABLE foo, bar")
	targets string
	// Comma separated list of quoted collection URIs (locality-aware)
	collectionURI string
	opts          *tree.BackupOptions
}

func newBackupQueryBuilder(
	t *testing.T, collectionURI []string, targets string, opts *tree.BackupOptions,
) *backupQueryBuilder {
	collectionURIStr := strings.Join(util.Map(collectionURI, func(uri string) string {
		return fmt.Sprintf("'%s'", uri)
	}), ", ")
	return &backupQueryBuilder{
		t:             t,
		targets:       targets,
		collectionURI: collectionURIStr,
		opts:          opts,
	}
}

// fullBackup creates a `BACKUP INTO` statement
func (b *backupQueryBuilder) fullBackup(aost hlc.Timestamp) string {
	b.t.Helper()
	return fmt.Sprintf(
		"BACKUP %s INTO (%s) %s %s",
		b.targets, b.collectionURI, b.aostExpr(aost), b.optsExpr(),
	)
}

// incBackup creates a `BACKUP INTO LATEST` statement against the latest full
// backup in the collection.
func (b *backupQueryBuilder) incBackup(aost hlc.Timestamp) string {
	b.t.Helper()
	return fmt.Sprintf(
		"BACKUP %s INTO LATEST IN (%s) %s %s",
		b.targets, b.collectionURI, b.aostExpr(aost), b.optsExpr(),
	)
}

// compactBackup creates a `crdb_internal.backup_compaction` statement against
// the latest full backup in the collection.
func (b *backupQueryBuilder) compactBackup(
	db *sqlutils.SQLRunner, start hlc.Timestamp, end hlc.Timestamp,
) string {
	b.t.Helper()
	startTime, err := strconv.ParseFloat(start.AsOfSystemTime(), 64)
	require.NoError(b.t, err)
	endTime, err := strconv.ParseFloat(end.AsOfSystemTime(), 64)
	require.NoError(b.t, err)

	var fullPath string
	db.QueryRow(b.t, b.showBackups()).Scan(&fullPath)
	backupStmt := strings.ReplaceAll(b.fullBackup(noAOST), "'", "''")

	return fmt.Sprintf(
		`SELECT crdb_internal.backup_compaction(
			'%s', '%s', %d::DECIMAL, %d::DECIMAL
		)`,
		backupStmt, fullPath, int64(startTime), int64(endTime),
	)
}

// showBackups creates a `SHOW BACKUPS` statement.
func (b *backupQueryBuilder) showBackups() string {
	return fmt.Sprintf("SHOW BACKUPS IN (%s)", b.collectionURI)
}

// restore creates a `RESTORE` statement that restores from the latest backup
// chain in the collection.
func (b *backupQueryBuilder) restore(targets string, aost hlc.Timestamp) string {
	optsExpr := ""
	if b.opts != nil {
		fmtCtx := tree.NewFmtCtx(tree.FmtShowPasswords)
		restoreOpts := tree.RestoreOptions{}
		restoreOpts.EncryptionPassphrase = b.opts.EncryptionPassphrase
		restoreOpts.DecryptionKMSURI = b.opts.EncryptionKMSURI
		restoreOpts.Format(fmtCtx)
		optsExpr = "WITH " + fmtCtx.String()
	}

	return fmt.Sprintf(
		"RESTORE %s FROM LATEST IN (%s) %s %s",
		targets, b.collectionURI, b.aostExpr(aost), optsExpr,
	)
}

// showBackup creates a `SHOW BACKUP` statement for the latest backup in the
// collection.
func (b *backupQueryBuilder) showBackup() string {
	optsExpr := ""
	if b.opts != nil {
		fmtCtx := tree.NewFmtCtx(tree.FmtShowPasswords)
		showOpts := tree.ShowBackupOptions{}
		showOpts.EncryptionPassphrase = b.opts.EncryptionPassphrase
		showOpts.DecryptionKMSURI = b.opts.EncryptionKMSURI
		showOpts.Format(fmtCtx)
		optsExpr = fmt.Sprintf("WITH %s", fmtCtx.String())
	}
	return fmt.Sprintf(
		`SHOW BACKUP FROM LATEST IN (%s) %s`, b.collectionURI, optsExpr,
	)
}

func (b *backupQueryBuilder) optsExpr() string {
	if b.opts == nil {
		return ""
	}
	fmtCtx := tree.NewFmtCtx(tree.FmtShowPasswords)
	b.opts.Format(fmtCtx)
	return fmt.Sprintf("WITH %s", fmtCtx.String())
}

func (b *backupQueryBuilder) aostExpr(aost hlc.Timestamp) string {
	b.t.Helper()
	if aost.IsEmpty() {
		return ""
	}
	time, err := strconv.ParseFloat(aost.AsOfSystemTime(), 64)
	require.NoError(b.t, err)
	return fmt.Sprintf("AS OF SYSTEM TIME '%d'", int64(time))
}
