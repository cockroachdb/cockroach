// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func makeTick(id int) revlogpb.Manifest {
	return revlogpb.Manifest{
		TickStart: hlc.Timestamp{WallTime: int64(id) * 1e10},
		TickEnd:   hlc.Timestamp{WallTime: int64(id+1) * 1e10},
	}
}

func makeTicks(n int) []revlogpb.Manifest {
	ticks := make([]revlogpb.Manifest, n)
	for i := range ticks {
		ticks[i] = makeTick(i)
	}
	return ticks
}

// newTestStorage returns a cloud.ExternalStorage backed by a temporary
// directory.
func newTestStorage(t *testing.T) cloud.ExternalStorage {
	t.Helper()
	return nodelocal.TestingMakeNodelocalStorage(
		t.TempDir(), cluster.MakeTestingClusterSettings(), cloudpb.ExternalStorage{},
	)
}

func testTickEnd(sec int) hlc.Timestamp {
	return hlc.Timestamp{
		WallTime: time.Date(2026, 4, 20, 15, 30, sec, 0, time.UTC).UnixNano(),
	}
}

// writeTestTick writes events to a tick data file, seals the tick, and
// returns the manifest.
func writeTestTick(
	t *testing.T,
	ctx context.Context,
	es cloud.ExternalStorage,
	te hlc.Timestamp,
	fileID int64,
	events []revlog.Event,
) revlogpb.Manifest {
	t.Helper()
	w, err := revlog.NewTickWriter(ctx, es, te, fileID, 0 /* flushOrder */)
	require.NoError(t, err)
	for _, ev := range events {
		require.NoError(t, w.Add(ev.Key, ev.Timestamp, ev.Value.RawBytes, ev.PrevValue.RawBytes))
	}
	f, _, err := w.Close()
	require.NoError(t, err)
	m := revlogpb.Manifest{
		TickStart: te.AddDuration(-10 * time.Second),
		TickEnd:   te,
		Files:     []revlogpb.File{f},
	}
	require.NoError(t, revlog.WriteTickManifest(ctx, es, m))
	return m
}

func testEvent(k string, walltime int64, logical int32, v string) revlog.Event {
	var val roachpb.Value
	if v != "" {
		val.RawBytes = []byte(v)
	}
	return revlog.Event{
		Key:       roachpb.Key(k),
		Timestamp: hlc.Timestamp{WallTime: walltime, Logical: logical},
		Value:     val,
	}
}

// collectMergedEntries collects all entries from mergeTickEvents into
// a slice, failing the test on any iteration error.
func collectMergedEntries(
	t *testing.T,
	ctx context.Context,
	es cloud.ExternalStorage,
	spec execinfrapb.RevlogLocalMergeSpec,
) []mergedEntry {
	t.Helper()
	var result []mergedEntry
	for entry, err := range mergeTickEvents(ctx, es, spec) {
		require.NoError(t, err)
		result = append(result, entry)
	}
	return result
}

func TestMergeTickEvents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	es := newTestStorage(t)
	defer es.Close()

	// Tick 1 (ending at :10): keys a, b, c with timestamps 1000-2000.
	te1 := testTickEnd(10)
	m1 := writeTestTick(t, ctx, es, te1, 1, []revlog.Event{
		testEvent("a", 1000, 0, "a_v1"),
		testEvent("a", 2000, 0, "a_v2"),
		testEvent("b", 1500, 0, "b_v1"),
		testEvent("c", 1000, 0, "c_v1"),
	})

	// Tick 2 (ending at :20): keys a, b, d with timestamps 3000-4000.
	te2 := testTickEnd(20)
	m2 := writeTestTick(t, ctx, es, te2, 2, []revlog.Event{
		testEvent("a", 3000, 0, "a_v3"),
		testEvent("b", 4000, 0, "b_v2"),
		testEvent("d", 3500, 0, "d_v1"),
	})

	t.Run("dedup keeps latest per key", func(t *testing.T) {
		spec := execinfrapb.RevlogLocalMergeSpec{
			Ticks: []revlogpb.Manifest{m1, m2},
		}
		merged := collectMergedEntries(t, ctx, es, spec)

		// Expected: one entry per key, with the latest timestamp.
		// a@3000, b@4000, c@1000, d@3500
		require.Len(t, merged, 4)

		byKey := make(map[string]mergedEntry)
		for _, e := range merged {
			byKey[string(e.Key.Key)] = e
		}
		require.Equal(t, int64(3000), byKey["a"].Key.Timestamp.WallTime)
		require.Equal(t, []byte("a_v3"), byKey["a"].Value)

		require.Equal(t, int64(4000), byKey["b"].Key.Timestamp.WallTime)
		require.Equal(t, []byte("b_v2"), byKey["b"].Value)

		require.Equal(t, int64(1000), byKey["c"].Key.Timestamp.WallTime)
		require.Equal(t, []byte("c_v1"), byKey["c"].Value)

		require.Equal(t, int64(3500), byKey["d"].Key.Timestamp.WallTime)
		require.Equal(t, []byte("d_v1"), byKey["d"].Value)
	})

	t.Run("AOST filters future events", func(t *testing.T) {
		spec := execinfrapb.RevlogLocalMergeSpec{
			Ticks: []revlogpb.Manifest{m1, m2},
			// AOST at 2500: filters out a@3000, b@4000, d@3500.
			RestoreTimestamp: hlc.Timestamp{WallTime: 2500},
		}
		merged := collectMergedEntries(t, ctx, es, spec)

		// a@2000, b@1500, c@1000 — d is entirely past AOST.
		require.Len(t, merged, 3)
		byKey := make(map[string]mergedEntry)
		for _, e := range merged {
			byKey[string(e.Key.Key)] = e
		}
		require.Equal(t, int64(2000), byKey["a"].Key.Timestamp.WallTime)
		require.Equal(t, []byte("a_v2"), byKey["a"].Value)

		require.Equal(t, int64(1500), byKey["b"].Key.Timestamp.WallTime)
		require.Equal(t, []byte("b_v1"), byKey["b"].Value)

		require.Equal(t, int64(1000), byKey["c"].Key.Timestamp.WallTime)
		require.Equal(t, []byte("c_v1"), byKey["c"].Value)
	})

	t.Run("tombstones preserved", func(t *testing.T) {
		te3 := testTickEnd(30)
		m3 := writeTestTick(t, ctx, es, te3, 3, []revlog.Event{
			// Delete key "a" at ts 5000 (empty value = tombstone).
			testEvent("a", 5000, 0, ""),
		})
		spec := execinfrapb.RevlogLocalMergeSpec{
			Ticks: []revlogpb.Manifest{m1, m2, m3},
		}
		merged := collectMergedEntries(t, ctx, es, spec)

		byKey := make(map[string]mergedEntry)
		for _, e := range merged {
			byKey[string(e.Key.Key)] = e
		}
		// a should be a tombstone at ts 5000.
		require.Equal(t, int64(5000), byKey["a"].Key.Timestamp.WallTime)
		require.Empty(t, byKey["a"].Value, "tombstone should have empty value")
	})

	t.Run("empty ticks", func(t *testing.T) {
		spec := execinfrapb.RevlogLocalMergeSpec{}
		merged := collectMergedEntries(t, ctx, es, spec)
		require.Empty(t, merged)
	})

	t.Run("output sorted by key", func(t *testing.T) {
		spec := execinfrapb.RevlogLocalMergeSpec{
			Ticks: []revlogpb.Manifest{m1, m2},
		}
		merged := collectMergedEntries(t, ctx, es, spec)
		// Events emerge from the heap in key-ascending order.
		for i := 1; i < len(merged); i++ {
			require.True(t,
				merged[i-1].Key.Key.Compare(merged[i].Key.Key) < 0,
				"keys must be strictly ascending: %s >= %s",
				merged[i-1].Key.Key, merged[i].Key.Key,
			)
		}
	})

	t.Run("interleaved timestamps across ticks", func(t *testing.T) {
		// Key "x" has revisions in both ticks with interleaved
		// timestamps. The heap must merge them correctly and dedup
		// must pick the latest.
		te4 := testTickEnd(40)
		m4 := writeTestTick(t, ctx, es, te4, 4, []revlog.Event{
			testEvent("x", 100, 0, "x_early"),
			testEvent("x", 300, 0, "x_mid"),
		})
		te5 := testTickEnd(50)
		m5 := writeTestTick(t, ctx, es, te5, 5, []revlog.Event{
			testEvent("x", 200, 0, "x_between"),
			testEvent("x", 400, 0, "x_latest"),
		})
		spec := execinfrapb.RevlogLocalMergeSpec{
			Ticks: []revlogpb.Manifest{m4, m5},
		}
		merged := collectMergedEntries(t, ctx, es, spec)
		require.Len(t, merged, 1)
		require.Equal(t, int64(400), merged[0].Key.Timestamp.WallTime)
		require.Equal(t, []byte("x_latest"), merged[0].Value)
	})

	t.Run("AOST boundary is inclusive", func(t *testing.T) {
		// An event exactly at the AOST should be kept.
		spec := execinfrapb.RevlogLocalMergeSpec{
			Ticks:            []revlogpb.Manifest{m1},
			RestoreTimestamp: hlc.Timestamp{WallTime: 2000},
		}
		merged := collectMergedEntries(t, ctx, es, spec)
		byKey := make(map[string]mergedEntry)
		for _, e := range merged {
			byKey[string(e.Key.Key)] = e
		}
		// a@2000 is exactly at the AOST — should be kept.
		require.Equal(t, int64(2000), byKey["a"].Key.Timestamp.WallTime)
	})
}

func TestAssignTicksToNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("even distribution", func(t *testing.T) {
		ticks := makeTicks(12)
		assignments := assignTicksToNodes(ticks, 3)
		require.Len(t, assignments, 3)
		for i, a := range assignments {
			require.Len(t, a, 4, "node %d", i)
		}
	})

	t.Run("uneven distribution", func(t *testing.T) {
		ticks := makeTicks(10)
		assignments := assignTicksToNodes(ticks, 3)
		require.Len(t, assignments, 3)
		// 10 ticks across 3 nodes: one gets 4, two get 3.
		counts := []int{len(assignments[0]), len(assignments[1]), len(assignments[2])}
		total := 0
		for _, c := range counts {
			total += c
			require.True(t, c == 3 || c == 4, "expected 3 or 4, got %d", c)
		}
		require.Equal(t, 10, total)
	})

	t.Run("single node", func(t *testing.T) {
		ticks := makeTicks(5)
		assignments := assignTicksToNodes(ticks, 1)
		require.Len(t, assignments, 1)
		require.Len(t, assignments[0], 5)
	})

	t.Run("empty ticks", func(t *testing.T) {
		assignments := assignTicksToNodes(nil, 3)
		require.Len(t, assignments, 3)
		for i, a := range assignments {
			require.Empty(t, a, "node %d", i)
		}
	})

	t.Run("more nodes than ticks", func(t *testing.T) {
		ticks := makeTicks(2)
		assignments := assignTicksToNodes(ticks, 5)
		require.Len(t, assignments, 5)
		nonEmpty := 0
		for _, a := range assignments {
			nonEmpty += len(a)
		}
		require.Equal(t, 2, nonEmpty)
	})

	t.Run("no loss or duplication", func(t *testing.T) {
		ticks := makeTicks(20)
		// Copy the original tick ends to check completeness after shuffle.
		expected := make(map[int64]struct{}, len(ticks))
		for _, tick := range ticks {
			expected[tick.TickEnd.WallTime] = struct{}{}
		}

		assignments := assignTicksToNodes(ticks, 4)
		got := make(map[int64]struct{})
		for _, nodeAssign := range assignments {
			for _, tick := range nodeAssign {
				_, dup := got[tick.TickEnd.WallTime]
				require.False(t, dup, "duplicate tick %v", tick.TickEnd)
				got[tick.TickEnd.WallTime] = struct{}{}
			}
		}
		require.Equal(t, expected, got)
	})
}

// TestMergeTickEventsFromBackup exercises the merge algorithm against
// real revision log data produced by BACKUP ... WITH REVISION STREAM.
//
//  1. Start a single-node cluster and create a table.
//  2. Run INSERT, UPDATE, and DELETE to generate MVCC history.
//  3. BACKUP INTO ... WITH REVISION STREAM to create a backup with a
//     sibling revision log job.
//  4. Wait for the revlog job to produce at least one closed tick.
//  5. Read all ticks from the revlog and run mergeTickEvents.
//  6. Validate: output is non-empty, keys are sorted, each key
//     appears exactly once, and values are properly encoded.
func TestMergeTickEventsFromBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// System tenant is required because kv.rangefeed.enabled is
	// operator-only, and the revlog producer needs rangefeeds.
	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	}
	_, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(
		t, singleNode, 0 /* numAccounts */, InitManualReplication, params,
	)
	defer cleanup()

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	sqlDB.Exec(t, `CREATE TABLE data.kv (k INT PRIMARY KEY, v STRING)`)

	// Run backup with revision stream to start the sibling revlog
	// job. The revlog producer watches via rangefeed from this
	// point forward.
	const destSubdir = "revlog-merge-test"
	dest := "nodelocal://1/" + destSubdir
	sqlDB.Exec(t, `BACKUP INTO $1 WITH REVISION STREAM`, dest)

	collectionDir := filepath.Join(dir, destSubdir)
	siblingID := readSiblingJobID(t, collectionDir)
	jobutils.WaitForJobToRun(t, sqlDB, siblingID)

	// Mutations happen after the revlog job starts so the rangefeed
	// captures them.
	sqlDB.Exec(t, `INSERT INTO data.kv VALUES (1, 'a'), (2, 'b'), (3, 'c')`)
	sqlDB.Exec(t, `UPDATE data.kv SET v = 'a2' WHERE k = 1`)
	sqlDB.Exec(t, `DELETE FROM data.kv WHERE k = 2`)

	// Wait for the revlog job to resolve past the mutations by
	// polling its high_water_timestamp via SHOW JOBS.
	require.NoError(t, testutils.SucceedsWithinError(func() error {
		var resolvedTS *string
		sqlDB.QueryRow(t,
			`SELECT resolved_timestamp::STRING `+
				`FROM [SHOW JOBS WITH RESOLVED TIMESTAMP] WHERE job_id = $1`,
			siblingID,
		).Scan(&resolvedTS)
		if resolvedTS == nil {
			return errors.New("revlog job has no resolved timestamp yet")
		}
		return nil
	}, 90*time.Second))

	// Open the collection storage and discover ticks.
	es := nodelocal.TestingMakeNodelocalStorage(
		collectionDir,
		cluster.MakeTestingClusterSettings(),
		cloudpb.ExternalStorage{},
	)
	defer es.Close()

	lr := revlog.NewLogReader(es)
	var manifests []revlogpb.Manifest
	for tick, tickErr := range lr.Ticks(ctx, hlc.Timestamp{WallTime: 1}, hlc.MaxTimestamp) {
		require.NoError(t, tickErr)
		manifests = append(manifests, tick.Manifest)
	}
	require.NotEmpty(t, manifests, "expected at least one tick manifest")
	t.Logf("discovered %d tick(s)", len(manifests))

	// Run the merge algorithm.
	spec := execinfrapb.RevlogLocalMergeSpec{
		Ticks: manifests,
	}
	merged := collectMergedEntries(t, ctx, es, spec)
	require.NotEmpty(t, merged, "expected non-empty merge output")

	// Validate: keys must be in ascending order and unique.
	seen := make(map[string]bool)
	for i, e := range merged {
		keyStr := string(e.Key.Key)
		require.False(t, seen[keyStr],
			"duplicate key %s in merge output", e.Key.Key)
		seen[keyStr] = true

		if i > 0 {
			require.True(t,
				merged[i-1].Key.Key.Compare(e.Key.Key) < 0,
				"keys not sorted: %s >= %s",
				merged[i-1].Key.Key, e.Key.Key,
			)
		}

		// Non-tombstone values must be valid roachpb.Value
		// encoding (4-byte checksum + 1-byte tag + data).
		if len(e.Value) > 0 {
			require.GreaterOrEqual(t, len(e.Value), 5,
				"value for key %s too short to be a roachpb.Value", e.Key.Key)
		}
	}

	t.Logf("merge produced %d deduplicated entries", len(merged))

	// Cancel the sibling job so cleanup doesn't hang.
	sqlDB.Exec(t, `CANCEL JOB $1`, siblingID)
	jobutils.WaitForJobToCancel(t, sqlDB, siblingID)
}

// waitForRevlogPastTimestamp polls SHOW JOBS WITH RESOLVED TIMESTAMP
// until the revlog job's resolved timestamp is at or past the given
// HLC timestamp (expressed as a decimal string from
// cluster_logical_timestamp()).
func waitForRevlogPastTimestamp(
	t *testing.T, sqlDB *sqlutils.SQLRunner, siblingID jobspb.JobID, target string,
) {
	t.Helper()
	require.NoError(t, testutils.SucceedsWithinError(func() error {
		var pastTarget *bool
		sqlDB.QueryRow(t,
			`SELECT resolved_timestamp >= $2::DECIMAL `+
				`FROM [SHOW JOBS WITH RESOLVED TIMESTAMP] WHERE job_id = $1`,
			siblingID, target,
		).Scan(&pastTarget)
		if pastTarget == nil || !*pastTarget {
			return errors.Newf(
				"revlog has not resolved past target %s", target,
			)
		}
		return nil
	}, 90*time.Second))
}

// clusterTimestamp returns the current cluster_logical_timestamp()
// as a decimal string suitable for AS OF SYSTEM TIME.
func clusterTimestamp(t *testing.T, sqlDB *sqlutils.SQLRunner) string {
	t.Helper()
	var ts string
	sqlDB.QueryRow(t,
		`SELECT cluster_logical_timestamp()::STRING`,
	).Scan(&ts)
	return ts
}

// TestRevlogAOSTExceedsResolved verifies that restoring to an AOST
// beyond the revision log's last resolved tick fails with an
// appropriate error during planning.
func TestRevlogAOSTExceedsResolved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	}
	_, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(
		t, singleNode, 0 /* numAccounts */, InitManualReplication, params,
	)
	defer cleanup()

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	sqlDB.Exec(t, `CREATE DATABASE aost_exceed`)
	sqlDB.Exec(t, `CREATE TABLE aost_exceed.tbl (k INT PRIMARY KEY)`)

	const destSubdir = "aost-exceed"
	dest := "nodelocal://1/" + destSubdir
	sqlDB.Exec(t, `BACKUP DATABASE aost_exceed INTO $1 WITH REVISION STREAM`, dest)

	collectionDir := filepath.Join(dir, destSubdir)
	siblingID := readSiblingJobID(t, collectionDir)
	jobutils.WaitForJobToRun(t, sqlDB, siblingID)

	// Insert some data and wait for at least one resolved tick.
	sqlDB.Exec(t, `INSERT INTO aost_exceed.tbl VALUES (1)`)
	afterInsert := clusterTimestamp(t, sqlDB)
	waitForRevlogPastTimestamp(t, sqlDB, siblingID, afterInsert)

	// Cancel the revlog job so it stops resolving.
	sqlDB.Exec(t, `CANCEL JOB $1`, siblingID)
	jobutils.WaitForJobToCancel(t, sqlDB, siblingID)

	// Take a cluster timestamp well after the revlog stopped
	// resolving. Since the revlog job is canceled, this
	// timestamp will be beyond the revlog's last resolved tick.
	beyondRevlog := clusterTimestamp(t, sqlDB)

	// Attempt to restore past the revlog's resolved point.
	sqlDB.Exec(t, `DROP DATABASE aost_exceed CASCADE`)
	sqlDB.ExpectErr(t,
		"revision log has not resolved",
		fmt.Sprintf(
			`RESTORE DATABASE aost_exceed FROM LATEST IN '%s' AS OF SYSTEM TIME '%s'`,
			dest, beyondRevlog,
		),
	)
}

// TestRevlogDescriptorResolution tests descriptor resolution during
// restore planning when a revision log is present, ensuring that
// schema changes during the revlog window are correctly merged.
func TestRevlogDescriptorResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	}
	_, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(
		t, singleNode, 0 /* numAccounts */, InitManualReplication, params,
	)
	defer cleanup()

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)

	t.Run("RESTORE DATABASE with table created during revlog", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE DATABASE db_create`)
		sqlDB.Exec(t, `CREATE TABLE db_create.original (k INT PRIMARY KEY, v STRING)`)
		sqlDB.Exec(t, `INSERT INTO db_create.original VALUES (1, 'one')`)

		const destSubdir = "db-create"
		dest := "nodelocal://1/" + destSubdir
		sqlDB.Exec(t, `BACKUP DATABASE db_create INTO $1 WITH REVISION STREAM`, dest)

		collectionDir := filepath.Join(dir, destSubdir)
		siblingID := readSiblingJobID(t, collectionDir)
		jobutils.WaitForJobToRun(t, sqlDB, siblingID)

		// Create a new table after the backup. The revlog's
		// descriptor rangefeed will capture this schema change.
		sqlDB.Exec(t, `CREATE TABLE db_create.new_tbl (k INT PRIMARY KEY, v STRING)`)
		sqlDB.Exec(t, `INSERT INTO db_create.new_tbl VALUES (10, 'ten')`)

		afterCreate := clusterTimestamp(t, sqlDB)
		waitForRevlogPastTimestamp(t, sqlDB, siblingID, afterCreate)

		sqlDB.Exec(t, `CANCEL JOB $1`, siblingID)
		jobutils.WaitForJobToCancel(t, sqlDB, siblingID)

		// Drop the database, then restore to the post-create AOST.
		sqlDB.Exec(t, `DROP DATABASE db_create CASCADE`)
		sqlDB.Exec(t, fmt.Sprintf(
			`RESTORE DATABASE db_create FROM LATEST IN '%s' AS OF SYSTEM TIME '%s'`,
			dest, afterCreate,
		))

		// Both the original and the new table should exist.
		// The original table's data comes from the backup.
		var count int
		sqlDB.QueryRow(t, `SELECT count(*) FROM db_create.original`).Scan(&count)
		require.Equal(t, 1, count, "original table should have 1 row")
		// The new table's descriptor was resolved from the revlog
		// schema changes and created during restore. Its data
		// would come from the revlog data ticks, which is handled
		// by the backup job's revision log stream (not tested
		// here).
		sqlDB.Exec(t, `SELECT * FROM db_create.new_tbl`)

		sqlDB.Exec(t, `DROP DATABASE db_create CASCADE`)
	})

	t.Run("RESTORE TABLE for table created during revlog", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE DATABASE tbl_create`)
		sqlDB.Exec(t, `CREATE TABLE tbl_create.original (k INT PRIMARY KEY)`)

		const destSubdir = "tbl-create"
		dest := "nodelocal://1/" + destSubdir
		sqlDB.Exec(t, `BACKUP DATABASE tbl_create INTO $1 WITH REVISION STREAM`, dest)

		collectionDir := filepath.Join(dir, destSubdir)
		siblingID := readSiblingJobID(t, collectionDir)
		jobutils.WaitForJobToRun(t, sqlDB, siblingID)

		sqlDB.Exec(t, `CREATE TABLE tbl_create.new_tbl (k INT PRIMARY KEY, v STRING)`)
		sqlDB.Exec(t, `INSERT INTO tbl_create.new_tbl VALUES (42, 'hello')`)

		afterCreate := clusterTimestamp(t, sqlDB)
		waitForRevlogPastTimestamp(t, sqlDB, siblingID, afterCreate)

		sqlDB.Exec(t, `CANCEL JOB $1`, siblingID)
		jobutils.WaitForJobToCancel(t, sqlDB, siblingID)

		// Drop the table, then restore just that table from the
		// revlog-augmented descriptor set.
		sqlDB.Exec(t, `DROP TABLE tbl_create.new_tbl`)
		sqlDB.Exec(t, fmt.Sprintf(
			`RESTORE TABLE tbl_create.new_tbl FROM LATEST IN '%s' AS OF SYSTEM TIME '%s'`,
			dest, afterCreate,
		))

		// Verify the table was created by the descriptor
		// resolution. Data comes from revlog ticks.
		sqlDB.Exec(t, `SELECT * FROM tbl_create.new_tbl`)

		sqlDB.Exec(t, `DROP DATABASE tbl_create CASCADE`)
	})

	t.Run("RESTORE TABLE after drop fails", func(t *testing.T) {
		// TODO(kev-cao): The revlog stream currently fails when a
		// table is dropped, so the resolved timestamp never
		// advances past the drop. Skip until the revlog writer
		// handles drops correctly.
		skip.IgnoreLint(t, "blocked on revlog stream drop handling")

		sqlDB.Exec(t, `CREATE DATABASE tbl_drop`)
		sqlDB.Exec(t, `CREATE TABLE tbl_drop.ephemeral (k INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO tbl_drop.ephemeral VALUES (1)`)
		// An anchor table keeps the revlog scope alive after
		// ephemeral is dropped, so the revlog job continues to
		// resolve through the drop timestamp.
		sqlDB.Exec(t, `CREATE TABLE tbl_drop.anchor (k INT PRIMARY KEY)`)

		const destSubdir = "tbl-drop"
		dest := "nodelocal://1/" + destSubdir
		sqlDB.Exec(t, `BACKUP DATABASE tbl_drop INTO $1 WITH REVISION STREAM`, dest)

		collectionDir := filepath.Join(dir, destSubdir)
		siblingID := readSiblingJobID(t, collectionDir)
		jobutils.WaitForJobToRun(t, sqlDB, siblingID)

		// Drop the table while the revlog is watching.
		beforeDrop := clusterTimestamp(t, sqlDB)
		sqlDB.Exec(t, `DROP TABLE tbl_drop.ephemeral`)
		afterDrop := clusterTimestamp(t, sqlDB)
		waitForRevlogPastTimestamp(t, sqlDB, siblingID, afterDrop)

		sqlDB.Exec(t, `CANCEL JOB $1`, siblingID)
		jobutils.WaitForJobToCancel(t, sqlDB, siblingID)

		// Restoring the dropped table to a time AFTER the drop
		// should fail because the table no longer exists.
		sqlDB.ExpectErr(t,
			"no tables or databases matched",
			fmt.Sprintf(
				`RESTORE TABLE tbl_drop.ephemeral FROM LATEST IN '%s' AS OF SYSTEM TIME '%s'`,
				dest, afterDrop,
			),
		)

		// Restoring the same table to a time BEFORE the drop
		// should succeed.
		sqlDB.Exec(t, fmt.Sprintf(
			`RESTORE TABLE tbl_drop.ephemeral FROM LATEST IN '%s' AS OF SYSTEM TIME '%s'`,
			dest, beforeDrop,
		))
		var count int
		sqlDB.QueryRow(t, `SELECT count(*) FROM tbl_drop.ephemeral`).Scan(&count)
		require.Equal(t, 1, count)

		sqlDB.Exec(t, `DROP DATABASE tbl_drop CASCADE`)
	})
}

// TestRestoreFromRevlog runs a full restore through the revlog path
// and verifies that the restored data in KV reflects the mutations
// captured by the revision log. The database has two tables but the
// restore targets only one, exercising the rekey skip logic for
// unscoped revlog events.
func TestRestoreFromRevlog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	}
	_, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(
		t, singleNode, 0 /* numAccounts */, InitManualReplication, params,
	)
	defer cleanup()

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	sqlDB.Exec(t, `CREATE DATABASE src`)
	sqlDB.Exec(t, `CREATE TABLE src.foo (k INT PRIMARY KEY, v STRING)`)
	sqlDB.Exec(t, `INSERT INTO src.foo VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')`)
	// A second table in the same database ensures the revlog
	// contains events for tables outside the restore scope.
	sqlDB.Exec(t, `CREATE TABLE src.bar (k INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO src.bar VALUES (1), (2)`)

	// BACKUP with revision stream to start the sibling revlog job.
	const destSubdir = "revlog-restore-data"
	dest := "nodelocal://1/" + destSubdir
	sqlDB.Exec(t, `BACKUP DATABASE src INTO $1 WITH REVISION STREAM`, dest)

	collectionDir := filepath.Join(dir, destSubdir)
	siblingID := readSiblingJobID(t, collectionDir)
	jobutils.WaitForJobToRun(t, sqlDB, siblingID)

	// Mutations after the backup — captured by the revlog
	// rangefeed. Mutate both tables to ensure the revlog has
	// events for tables that won't have rekeys.
	sqlDB.Exec(t, `INSERT INTO src.foo VALUES (100, 'new')`)
	sqlDB.Exec(t, `UPDATE src.foo SET v = 'updated' WHERE k = 1`)
	sqlDB.Exec(t, `DELETE FROM src.foo WHERE k = 2`)
	sqlDB.Exec(t, `INSERT INTO src.bar VALUES (3)`)

	// Record the AOST after mutations.
	aost := clusterTimestamp(t, sqlDB)

	// Wait for the revlog job to resolve past the AOST.
	waitForRevlogPastTimestamp(t, sqlDB, siblingID, aost)

	// Cancel the revlog job before restoring.
	sqlDB.Exec(t, `CANCEL JOB $1`, siblingID)
	jobutils.WaitForJobToCancel(t, sqlDB, siblingID)

	// Restore only src.foo — the revlog data contains events for
	// both foo and bar, but only foo has rekeys.
	sqlDB.Exec(t, `CREATE DATABASE restored`)
	sqlDB.Exec(t, fmt.Sprintf(
		`RESTORE TABLE src.foo FROM LATEST IN '%s' AS OF SYSTEM TIME '%s' WITH into_db = 'restored'`,
		dest, aost,
	))

	// Verify the restored data matches the expected state at the
	// AOST: k=1 updated, k=2 deleted, k=3 unchanged, k=100 inserted.
	rows := sqlDB.QueryStr(t,
		`SELECT k, v FROM restored.foo ORDER BY k`,
	)
	expected := [][]string{
		{"1", "updated"},
		{"3", "gamma"},
		{"100", "new"},
	}
	require.Equal(t, expected, rows)

	// Verify that intermediate SSTs were cleaned up after restore.
	// The local merge writes to nodelocal://{id}/job/{jobID}/map/,
	// which maps to {dir}/job/{jobID}/ on disk.
	jobDir := filepath.Join(dir, "job")
	entries, err := os.ReadDir(jobDir)
	if err == nil {
		for _, e := range entries {
			subdir := filepath.Join(jobDir, e.Name())
			files, _ := filepath.Glob(
				filepath.Join(subdir, "**", "*.sst"),
			)
			require.Empty(t, files,
				"expected no leftover SSTs in %s", subdir,
			)
		}
	}
	// If jobDir doesn't exist at all, cleanup succeeded.
}
