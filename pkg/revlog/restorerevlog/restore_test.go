// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package restorerevlog

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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

// collectMergedEntries collects all entries from MergeTickEvents into
// a slice, failing the test on any iteration error.
func collectMergedEntries(
	t *testing.T,
	ctx context.Context,
	es cloud.ExternalStorage,
	spec execinfrapb.RevlogLocalMergeSpec,
) []MergedEntry {
	t.Helper()
	var result []MergedEntry
	for entry, err := range MergeTickEvents(ctx, es, spec) {
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

		byKey := make(map[string]MergedEntry)
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
		byKey := make(map[string]MergedEntry)
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

		byKey := make(map[string]MergedEntry)
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
		byKey := make(map[string]MergedEntry)
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
		assignments := AssignTicksToNodes(ticks, 3)
		require.Len(t, assignments, 3)
		for i, a := range assignments {
			require.Len(t, a, 4, "node %d", i)
		}
	})

	t.Run("uneven distribution", func(t *testing.T) {
		ticks := makeTicks(10)
		assignments := AssignTicksToNodes(ticks, 3)
		require.Len(t, assignments, 3)
		// 10 ticks across 3 nodes: one gets 4, two get 3.
		counts := []int{
			len(assignments[0]),
			len(assignments[1]),
			len(assignments[2]),
		}
		total := 0
		for _, c := range counts {
			total += c
			require.True(t, c == 3 || c == 4,
				"expected 3 or 4, got %d", c)
		}
		require.Equal(t, 10, total)
	})

	t.Run("single node", func(t *testing.T) {
		ticks := makeTicks(5)
		assignments := AssignTicksToNodes(ticks, 1)
		require.Len(t, assignments, 1)
		require.Len(t, assignments[0], 5)
	})

	t.Run("empty ticks", func(t *testing.T) {
		assignments := AssignTicksToNodes(nil, 3)
		require.Len(t, assignments, 3)
		for i, a := range assignments {
			require.Empty(t, a, "node %d", i)
		}
	})

	t.Run("more nodes than ticks", func(t *testing.T) {
		ticks := makeTicks(2)
		assignments := AssignTicksToNodes(ticks, 5)
		require.Len(t, assignments, 5)
		nonEmpty := 0
		for _, a := range assignments {
			nonEmpty += len(a)
		}
		require.Equal(t, 2, nonEmpty)
	})

	t.Run("no loss or duplication", func(t *testing.T) {
		ticks := makeTicks(20)
		expected := make(map[int64]struct{}, len(ticks))
		for _, tick := range ticks {
			expected[tick.TickEnd.WallTime] = struct{}{}
		}

		assignments := AssignTicksToNodes(ticks, 4)
		got := make(map[int64]struct{})
		for _, nodeAssign := range assignments {
			for _, tick := range nodeAssign {
				_, dup := got[tick.TickEnd.WallTime]
				require.False(t, dup,
					"duplicate tick %v", tick.TickEnd)
				got[tick.TickEnd.WallTime] = struct{}{}
			}
		}
		require.Equal(t, expected, got)
	})
}
