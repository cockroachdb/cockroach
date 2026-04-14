// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag/wagpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// testEngines holds the separated log and state engines used by truncator
// tests.
type testEngines struct {
	Engines
	seq wag.Seq
}

// makeTestEngines creates separated log and state engines. The caller must
// defer Close() to ensure engines are closed before leak checks.
func makeTestEngines() testEngines {
	return testEngines{
		Engines: MakeSeparatedEnginesForTesting(
			storage.NewDefaultInMemForTesting(),
			storage.NewDefaultInMemForTesting(),
		),
	}
}

// writeWAGNode writes a WAG node containing the given event to the log engine,
// auto-incrementing the WAG sequence number.
func (e *testEngines) writeWAGNode(t *testing.T, event wagpb.Event) {
	t.Helper()
	index := e.seq.Next()
	require.NoError(t, wag.Write(e.LogEngine(), index, wagpb.Node{Events: []wagpb.Event{event}}))
}

// writeRaftLogEntry writes a dummy raft log entry to the log engine.
func (e *testEngines) writeRaftLogEntry(
	t *testing.T, rangeID roachpb.RangeID, index kvpb.RaftIndex,
) {
	t.Helper()
	require.NoError(t, e.LogEngine().PutUnversioned(
		keys.RaftLogKeyFromPrefix(keys.RaftLogPrefix(rangeID), index), []byte("entry")))
}

// raftLogIndices returns the indices of all raft log entries for the given range
// in the log engine.
func (e *testEngines) raftLogIndices(t *testing.T, rangeID roachpb.RangeID) []kvpb.RaftIndex {
	t.Helper()
	sl := MakeStateLoader(rangeID)
	prefix := sl.RaftLogPrefix()
	var indices []kvpb.RaftIndex
	iter, err := e.LogEngine().NewMVCCIterator(
		context.Background(), storage.MVCCKeyIterKind, storage.IterOptions{
			LowerBound:   prefix,
			UpperBound:   prefix.PrefixEnd(),
			ReadCategory: fs.ReplicationReadCategory,
		})
	require.NoError(t, err)
	defer iter.Close()
	iter.SeekGE(storage.MakeMVCCMetadataKey(prefix))
	for ; ; iter.Next() {
		ok, err := iter.Valid()
		require.NoError(t, err)
		if !ok {
			break
		}
		suffix := iter.UnsafeKey().Key[len(prefix):]
		index, err := keys.DecodeRaftLogKeyFromSuffix(suffix)
		require.NoError(t, err)
		indices = append(indices, index)
	}
	return indices
}

// listWAGNodes returns the indices of all WAG nodes in the log engine.
func (e *testEngines) listWAGNodes(t *testing.T) []uint64 {
	t.Helper()
	var indices []uint64
	var iter wag.Iterator
	for index := range iter.Iter(context.Background(), e.LogEngine()) {
		indices = append(indices, index)
	}
	require.NoError(t, iter.Error())
	return indices
}

func TestTruncateApplied(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	r1 := roachpb.FullReplicaID{RangeID: 1, ReplicaID: 1}
	r2 := roachpb.FullReplicaID{RangeID: 1, ReplicaID: 2}
	sl := MakeStateLoader(1 /* rangeID */)

	for _, tc := range []struct {
		setup          func(t *testing.T, e *testEngines)
		wantWAGIndices []uint64
	}{
		{
			setup: func(t *testing.T, e *testEngines) {
				require.NoError(t, sl.SetRaftReplicaID(ctx, e.StateEngine(), r1.ReplicaID))
				require.NoError(t, sl.SetRangeAppliedState(ctx, e.StateEngine(),
					&kvserverpb.RangeAppliedState{RaftAppliedIndex: 10}))
			},
			// There are no WAG nodes, so the truncation is a no-op.
			wantWAGIndices: nil,
		},
		{
			setup: func(t *testing.T, e *testEngines) {
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r2, 0), Type: wagpb.EventCreate})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r2, 15), Type: wagpb.EventInit})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r2, 20), Type: wagpb.EventApply})
				require.NoError(t, sl.SetRaftReplicaID(ctx, e.StateEngine(), r1.ReplicaID))
				require.NoError(t, sl.SetRangeAppliedState(ctx, e.StateEngine(),
					&kvserverpb.RangeAppliedState{RaftAppliedIndex: 9}))
			},
			// No WAG node has been applied yet because the current
			// replicaID is < the event replicaID.
			wantWAGIndices: []uint64{1, 2, 3},
		},
		{
			setup: func(t *testing.T, e *testEngines) {
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 0), Type: wagpb.EventCreate})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 15), Type: wagpb.EventInit})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 20), Type: wagpb.EventApply})
				require.NoError(t, sl.SetRaftReplicaID(ctx, e.StateEngine(), r1.ReplicaID))
				require.NoError(t, sl.SetRangeAppliedState(ctx, e.StateEngine(),
					&kvserverpb.RangeAppliedState{RaftAppliedIndex: 15}))
			},
			// Raft applied index is 15, only the first two WAG nodes should be
			// truncated.
			wantWAGIndices: []uint64{3},
		},
		{
			setup: func(t *testing.T, e *testEngines) {
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 0), Type: wagpb.EventCreate})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 15), Type: wagpb.EventInit})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 20), Type: wagpb.EventApply})
				require.NoError(t, sl.SetRaftReplicaID(ctx, e.StateEngine(), r1.ReplicaID))
				require.NoError(t, sl.SetRangeAppliedState(ctx, e.StateEngine(),
					&kvserverpb.RangeAppliedState{RaftAppliedIndex: 20}))
			},
			// Raft applied index is 20, all WAG nodes should be truncated.
			wantWAGIndices: nil,
		},
		{
			setup: func(t *testing.T, e *testEngines) {
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 0), Type: wagpb.EventCreate})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 20), Type: wagpb.EventDestroy})
				require.NoError(t, sl.SetRangeTombstone(ctx, e.StateEngine(),
					kvserverpb.RangeTombstone{NextReplicaID: r1.ReplicaID + 1}))
			},
			// A range tombstone confirms destruction, WAG nodes to that replica
			// should be truncated.
			wantWAGIndices: nil,
		},
		{
			setup: func(t *testing.T, e *testEngines) {
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 0), Type: wagpb.EventCreate})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 20), Type: wagpb.EventSubsume})
				require.NoError(t, sl.SetRangeTombstone(ctx, e.StateEngine(),
					kvserverpb.RangeTombstone{NextReplicaID: r1.ReplicaID + 1}))
			},
			// A range tombstone confirms subsumption, WAG nodes to that replica
			// should be truncated.
			wantWAGIndices: nil,
		},
		{
			setup: func(t *testing.T, e *testEngines) {
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 0), Type: wagpb.EventCreate})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 20), Type: wagpb.EventDestroy})
				require.NoError(t, sl.SetRaftReplicaID(ctx, e.StateEngine(), r2.ReplicaID))
				require.NoError(t, sl.SetRangeAppliedState(ctx, e.StateEngine(),
					&kvserverpb.RangeAppliedState{RaftAppliedIndex: 20}))
			},
			// A newer replica supersedes the old one, WAG nodes for the old
			// replica should be truncated.
			wantWAGIndices: nil,
		},
		{
			setup: func(t *testing.T, e *testEngines) {
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 0), Type: wagpb.EventCreate})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 20), Type: wagpb.EventSubsume})
				require.NoError(t, sl.SetRaftReplicaID(ctx, e.StateEngine(), r2.ReplicaID))
				require.NoError(t, sl.SetRangeAppliedState(ctx, e.StateEngine(),
					&kvserverpb.RangeAppliedState{RaftAppliedIndex: 20}))
			},
			// A newer replica supersedes the subsumed one, WAG nodes for the old
			// replica should be truncated.
			wantWAGIndices: nil,
		},
		{
			setup: func(t *testing.T, e *testEngines) {
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 0), Type: wagpb.EventCreate})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 20), Type: wagpb.EventDestroy})
				require.NoError(t, sl.SetRaftReplicaID(ctx, e.StateEngine(), r1.ReplicaID))
				// EventDestroy won't be deleted if the replicaID still matches the
				// WAG node's replica ID because it means that the event hasn't applied
				// yet. In this case, we won't even read the applied index.
				require.NoError(t, sl.SetRangeAppliedState(ctx, e.StateEngine(),
					&kvserverpb.RangeAppliedState{RaftAppliedIndex: 20}))
			},
			wantWAGIndices: []uint64{2},
		},
		{
			setup: func(t *testing.T, e *testEngines) {
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 0), Type: wagpb.EventCreate})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 20), Type: wagpb.EventSubsume})
				require.NoError(t, sl.SetRaftReplicaID(ctx, e.StateEngine(), r1.ReplicaID))
				// EventSubsume won't be deleted if the replicaID still matches the
				// WAG node's replica ID because it means that the event hasn't applied
				// yet. In this case, we won't even read the applied index.
				require.NoError(t, sl.SetRangeAppliedState(ctx, e.StateEngine(),
					&kvserverpb.RangeAppliedState{RaftAppliedIndex: 20}))
			},
			wantWAGIndices: []uint64{2},
		},
	} {
		t.Run("", func(t *testing.T) {
			e := makeTestEngines()
			defer e.Close()
			truncator := NewWAGTruncator(st, e.Engines, &e.seq, 0, WAGTruncatorTestingKnobs{})
			tc.setup(t, &e)
			require.NoError(t, e.stateEngine.Flush())
			_, err := truncator.truncateAppliedNodes(ctx, 0, /* startIndex */
				math.MaxUint64 /* lastIndex */, true /* ignoreGaps */)
			require.NoError(t, err)
			require.Equal(t, tc.wantWAGIndices, e.listWAGNodes(t))
		})
	}
}

// TestTruncateAndClearRaftState verifies that
// truncateAppliedWAGNodeAndClearRaftState only clears raft log entries and
// sideloaded files up to the destroyed/subsumed replica's last index. Entries
// and files beyond that index may belong to a newer replica and must be
// preserved.
func TestTruncateAndClearRaftState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	r1 := roachpb.FullReplicaID{RangeID: 1, ReplicaID: 1}
	r2 := roachpb.FullReplicaID{RangeID: 1, ReplicaID: 2}
	sl := MakeStateLoader(1 /* rangeID */)

	for _, eventType := range []wagpb.EventType{wagpb.EventDestroy, wagpb.EventSubsume} {
		t.Run(eventType.String(), func(t *testing.T) {
			e := makeTestEngines()
			defer e.Close()
			truncator := NewWAGTruncator(st, e.Engines, &e.seq, 0, WAGTruncatorTestingKnobs{})

			// Write WAG nodes: init then destroy/subsume at index 20.
			e.writeWAGNode(t, wagpb.Event{
				Addr: wagpb.MakeAddr(r1, 10), Type: wagpb.EventInit,
			})
			e.writeWAGNode(t, wagpb.Event{
				Addr: wagpb.MakeAddr(r1, 20), Type: eventType,
			})

			// Create a WAG node for a newer replica for the same range.
			e.writeWAGNode(t, wagpb.Event{
				Addr: wagpb.MakeAddr(r2, 0), Type: wagpb.EventCreate,
			})

			// Tombstone confirms destruction/subsumption.
			require.NoError(t, sl.SetRangeTombstone(ctx, e.StateEngine(),
				kvserverpb.RangeTombstone{NextReplicaID: 2}))

			// Write raft log entries 10-25. Entries <= 20 belong to the old replica.
			// Entries >= 21 belong to the new replica using the same RangeID.
			for idx := 10; idx <= 25; idx++ {
				e.writeRaftLogEntry(t, 1 /* rangeID */, kvpb.RaftIndex(idx))
			}

			// Create sideloaded files using the state engine's VFS, matching
			// production where sideloaded entries are on the state engine.
			baseDir := e.StateEngine().GetAuxiliaryDir()
			env := e.StateEngine().Env()
			limiter := rate.NewLimiter(rate.Inf, math.MaxInt64)
			ss := logstore.NewDiskSideloadStorage(st, 1, baseDir, limiter, env)
			for _, idx := range []kvpb.RaftIndex{10, 15, 20, 21, 25} {
				require.NoError(t, ss.Put(ctx, idx, 1 /* term */, []byte("sst-data")))
			}
			require.NoError(t, e.stateEngine.Flush())
			_, err := truncator.truncateAppliedNodes(ctx, 0, /* startIndex */
				math.MaxUint64 /* lastIndex */, true /* ignoreGaps */)
			require.NoError(t, err)
			// Raft entries <= 20 belong to the old replica and must be deleted. The
			// rest shouldn't be deleted by the WAG truncator.
			require.Equal(t,
				[]kvpb.RaftIndex{21, 22, 23, 24, 25},
				e.raftLogIndices(t, 1 /* rangeID */),
			)
			// Sideloaded files at entries 10, 15, and 20 belong to the old replica
			// and must be deleted. The rest shouldn't be deleted by the WAG
			// truncator.
			for _, idx := range []kvpb.RaftIndex{10, 15, 20} {
				_, err := ss.Get(ctx, idx, 1)
				require.Errorf(t, err, "sideloaded file at index %d should have been deleted", idx)
			}
			for _, idx := range []kvpb.RaftIndex{21, 25} {
				data, err := ss.Get(ctx, idx, 1)
				require.NoErrorf(t, err, "sideloaded file at index %d should be preserved", idx)
				require.Equal(t, []byte("sst-data"), data)
			}
			// Only the WAG node for the newer replica should be left.
			require.Equal(t, []uint64{3}, e.listWAGNodes(t))
		})
	}
}

// TestTruncateGapHandling verifies that truncateAppliedWAGNodeAndClearRaftState
// handles gaps in WAG node indices correctly.
//
// The test sets up three WAG nodes with gaps between them:
// [Index: 2] -> [Index: 4] -> [Index: 6]
func TestTruncateGapHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	r1 := roachpb.FullReplicaID{RangeID: 1, ReplicaID: 1}
	sl := MakeStateLoader(1 /* rangeID */)

	for _, calls := range [][]struct {
		index                  uint64
		lastIndex              uint64
		ignoreGaps             bool
		wantTruncated          bool
		wantLastTruncatedIndex uint64
		wantWAGIndices         []uint64
	}{
		{
			// Using index=0, lastIndex=math.MaxUint64 removes
			// the first WAG node regardless of its index.
			{index: 0, lastIndex: math.MaxUint64, ignoreGaps: true,
				wantTruncated: true, wantLastTruncatedIndex: 2, wantWAGIndices: []uint64{4, 6}},
			{index: 0, lastIndex: math.MaxUint64, ignoreGaps: true,
				wantTruncated: true, wantLastTruncatedIndex: 4, wantWAGIndices: []uint64{6}},
			{index: 0, lastIndex: math.MaxUint64, ignoreGaps: true,
				wantTruncated: true, wantLastTruncatedIndex: 6, wantWAGIndices: nil},
			// Attempting to truncate when there are no WAG nodes should not cause
			// an error.
			{index: 0, lastIndex: math.MaxUint64, ignoreGaps: true,
				wantTruncated: false, wantLastTruncatedIndex: 0, wantWAGIndices: nil},
		},
		{
			// Exact same case as above, but with tighter bounds to verify that index
			// and lastIndex are inclusive.
			{index: 0, lastIndex: 6, ignoreGaps: true,
				wantTruncated: true, wantLastTruncatedIndex: 2, wantWAGIndices: []uint64{4, 6}},
			{index: 2, lastIndex: 6, ignoreGaps: true,
				wantTruncated: true, wantLastTruncatedIndex: 4, wantWAGIndices: []uint64{6}},
			{index: 4, lastIndex: 6, ignoreGaps: true,
				wantTruncated: true, wantLastTruncatedIndex: 6, wantWAGIndices: nil},
		},
		{
			// When ignoreGaps=false, we can only truncate the WAG node with the exact
			// index.
			{index: 0, lastIndex: math.MaxUint64, ignoreGaps: false,
				wantTruncated: false, wantLastTruncatedIndex: 0, wantWAGIndices: []uint64{2, 4, 6}},
			{index: 2, lastIndex: math.MaxUint64, ignoreGaps: false,
				wantTruncated: true, wantLastTruncatedIndex: 2, wantWAGIndices: []uint64{4, 6}},
			{index: 2, lastIndex: math.MaxUint64, ignoreGaps: false,
				wantTruncated: false, wantLastTruncatedIndex: 0, wantWAGIndices: []uint64{4, 6}},
			{index: 4, lastIndex: 4, ignoreGaps: false,
				wantTruncated: true, wantLastTruncatedIndex: 4, wantWAGIndices: []uint64{6}},
			{index: 5, lastIndex: math.MaxUint64, ignoreGaps: false,
				wantTruncated: false, wantLastTruncatedIndex: 0, wantWAGIndices: []uint64{6}},
			{index: 6, lastIndex: math.MaxUint64, ignoreGaps: false,
				wantTruncated: true, wantLastTruncatedIndex: 6, wantWAGIndices: nil},
			// Attempting to truncate when there are no WAG nodes should not cause an
			// error.
			{index: 6, lastIndex: math.MaxUint64, ignoreGaps: false,
				wantTruncated: false, wantLastTruncatedIndex: 0, wantWAGIndices: nil},
		},
	} {
		t.Run("", func(t *testing.T) {
			e := makeTestEngines()
			defer e.Close()
			truncator := NewWAGTruncator(st, e.Engines, &e.seq, 0, WAGTruncatorTestingKnobs{})

			// Write WAG nodes at indices 2, 4, 6.
			e.seq.Next()
			e.writeWAGNode(t, wagpb.Event{
				Addr: wagpb.MakeAddr(r1, 0), Type: wagpb.EventCreate,
			})
			e.seq.Next()
			e.writeWAGNode(t, wagpb.Event{
				Addr: wagpb.MakeAddr(r1, 15), Type: wagpb.EventInit,
			})
			e.seq.Next()
			e.writeWAGNode(t, wagpb.Event{
				Addr: wagpb.MakeAddr(r1, 20), Type: wagpb.EventApply,
			})

			// Set applied state so all WAG nodes are considered applied.
			require.NoError(t, sl.SetRaftReplicaID(ctx, e.StateEngine(), r1.ReplicaID))
			require.NoError(t, sl.SetRangeAppliedState(ctx, e.StateEngine(),
				&kvserverpb.RangeAppliedState{RaftAppliedIndex: 20}))
			require.NoError(t, e.stateEngine.Flush())

			for _, c := range calls {
				stateReader := e.StateEngine().NewReader(storage.GuaranteedDurability)
				b := e.LogEngine().NewWriteBatch()
				truncated, lastTruncatedIndex, err := truncator.truncateAppliedWAGNodeAndClearRaftState(
					ctx, Raft{RO: e.LogEngine(), WO: b}, stateReader, c.index, c.lastIndex, c.ignoreGaps,
				)
				require.NoError(t, err)
				require.Equal(t, c.wantTruncated, truncated)
				require.Equal(t, c.wantLastTruncatedIndex, lastTruncatedIndex)
				if truncated {
					require.NoError(t, b.Commit(false /* sync */))
				}
				b.Close()
				stateReader.Close()
				require.Equal(t, c.wantWAGIndices, e.listWAGNodes(t))
			}
		})
	}
}

// TestTruncateAppliedNodes exercises truncateAppliedNodes across all
// combinations of startIndex, lastIndex, and ignoreGaps. The test sets up WAG
// nodes at indices [2, 4, 5, 6]. Node 6 isn't ready for truncation yet.
func TestTruncateAppliedNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	r1 := roachpb.FullReplicaID{RangeID: 1, ReplicaID: 1}
	sl := MakeStateLoader(r1.RangeID)

	for _, tc := range []struct {
		startIndex        uint64
		lastIndex         uint64
		ignoreGaps        bool
		wantLastTruncated uint64
		wantRemaining     []uint64
	}{
		// --- ignoreGaps=true: skips over gaps in WAG indices ---
		{
			startIndex: 0, lastIndex: 1, ignoreGaps: true,
			wantLastTruncated: 0, wantRemaining: []uint64{2, 4, 5, 6},
		},
		{
			startIndex: 0, lastIndex: 2, ignoreGaps: true,
			wantLastTruncated: 2, wantRemaining: []uint64{4, 5, 6},
		},
		{
			startIndex: 0, lastIndex: 6, ignoreGaps: true,
			wantLastTruncated: 5, wantRemaining: []uint64{6},
		},
		{
			startIndex: 0, lastIndex: math.MaxUint64, ignoreGaps: true,
			wantLastTruncated: 5, wantRemaining: []uint64{6},
		},
		{
			startIndex: 3, lastIndex: math.MaxUint64, ignoreGaps: true,
			wantLastTruncated: 5, wantRemaining: []uint64{2, 6},
		},
		{
			startIndex: 7, lastIndex: math.MaxUint64, ignoreGaps: true,
			wantLastTruncated: 0, wantRemaining: []uint64{2, 4, 5, 6},
		},
		// --- ignoreGaps=false: stops at first gap in WAG indices ---
		{
			startIndex: 0, lastIndex: math.MaxUint64, ignoreGaps: false,
			wantLastTruncated: 0, wantRemaining: []uint64{2, 4, 5, 6},
		},
		{
			startIndex: 2, lastIndex: 2, ignoreGaps: false,
			wantLastTruncated: 2, wantRemaining: []uint64{4, 5, 6},
		},
		{
			startIndex: 2, lastIndex: math.MaxUint64, ignoreGaps: false,
			wantLastTruncated: 2, wantRemaining: []uint64{4, 5, 6},
		},
		{
			startIndex: 3, lastIndex: math.MaxUint64, ignoreGaps: false,
			wantLastTruncated: 0, wantRemaining: []uint64{2, 4, 5, 6},
		},
		{
			startIndex: 4, lastIndex: 4, ignoreGaps: false,
			wantLastTruncated: 4, wantRemaining: []uint64{2, 5, 6},
		},
		{
			startIndex: 4, lastIndex: math.MaxUint64, ignoreGaps: false,
			wantLastTruncated: 5, wantRemaining: []uint64{2, 6},
		},
		{
			startIndex: 6, lastIndex: math.MaxUint64, ignoreGaps: false,
			wantLastTruncated: 0, wantRemaining: []uint64{2, 4, 5, 6},
		},
		{
			startIndex: 7, lastIndex: math.MaxUint64, ignoreGaps: false,
			wantLastTruncated: 0, wantRemaining: []uint64{2, 4, 5, 6},
		},
	} {
		t.Run("", func(t *testing.T) {
			e := makeTestEngines()
			defer e.Close()
			truncator := NewWAGTruncator(st, e.Engines, &e.seq, 0, WAGTruncatorTestingKnobs{})

			// Write WAG nodes at indices 2, 4, 5.
			e.seq.Next()
			e.writeWAGNode(t, wagpb.Event{
				Addr: wagpb.MakeAddr(r1, 0), Type: wagpb.EventCreate,
			})
			e.seq.Next()
			e.writeWAGNode(t, wagpb.Event{
				Addr: wagpb.MakeAddr(r1, 15), Type: wagpb.EventInit,
			})
			e.writeWAGNode(t, wagpb.Event{
				Addr: wagpb.MakeAddr(r1, 20), Type: wagpb.EventApply,
			})
			e.writeWAGNode(t, wagpb.Event{
				Addr: wagpb.MakeAddr(r1, 25), Type: wagpb.EventApply,
			})

			require.NoError(t, sl.SetRaftReplicaID(ctx, e.StateEngine(), r1.ReplicaID))
			require.NoError(t, sl.SetRangeAppliedState(ctx, e.StateEngine(),
				&kvserverpb.RangeAppliedState{RaftAppliedIndex: 20}))
			require.NoError(t, e.stateEngine.Flush())

			lastTruncated, err := truncator.truncateAppliedNodes(ctx, tc.startIndex, tc.lastIndex, tc.ignoreGaps)
			require.NoError(t, err)
			require.Equal(t, tc.wantLastTruncated, lastTruncated)
			require.Equal(t, tc.wantRemaining, e.listWAGNodes(t))
		})
	}
}

// TestWAGTruncatorBackground verifies that the WAGTruncator background
// goroutine only truncates WAG nodes when both conditions are met: (1) the
// state engine has flushed, and (2) there are WAG nodes that are eligible for
// truncation.
func TestWAGTruncatorBackground(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	e := makeTestEngines()
	defer e.Close()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	r1 := roachpb.FullReplicaID{RangeID: 1, ReplicaID: 1}
	sl := MakeStateLoader(r1.RangeID)

	// Initialize replica state so events can be considered applied.
	require.NoError(t, sl.SetRaftReplicaID(ctx, e.StateEngine(), r1.ReplicaID))
	require.NoError(t, sl.SetRangeAppliedState(ctx, e.StateEngine(),
		&kvserverpb.RangeAppliedState{RaftAppliedIndex: 100}))

	// Write two WAG nodes whose events are applied with a gap in between them.
	// This is to simulate some WAG nodes at engine startup.
	e.writeWAGNode(t, wagpb.Event{
		Addr: wagpb.MakeAddr(r1, 10), Type: wagpb.EventInit,
	})
	e.seq.Next()
	e.writeWAGNode(t, wagpb.Event{
		Addr: wagpb.MakeAddr(r1, 20), Type: wagpb.EventApply,
	})

	// Start the periodic WAG truncation background task with a knob that
	// signals when truncation completes, so the test can synchronize
	// deterministically instead of polling.
	truncationDone := make(chan struct{}, 1)
	truncator := NewWAGTruncator(st, e.Engines, &e.seq,
		e.seq.Load(), /* lastIndexAfterStartup */
		WAGTruncatorTestingKnobs{
			AfterTruncationCallback: func() {
				truncationDone <- struct{}{}
			},
		})
	require.NoError(t, truncator.Start(ctx, stopper))
	// Create a WAG node after startup.
	e.writeWAGNode(t, wagpb.Event{
		Addr: wagpb.MakeAddr(r1, 30), Type: wagpb.EventApply,
	})
	require.Equal(t, []uint64{1, 3, 4}, e.listWAGNodes(t))

	flushAndWaitForTruncation := func() {
		require.NoError(t, e.StateEngine().Flush())
		truncator.DurabilityAdvancedCallback()
		<-truncationDone
	}
	// We expect all WAG nodes to be truncated when the state engine is flushed.
	flushAndWaitForTruncation()
	require.Equal(t, ([]uint64)(nil), e.listWAGNodes(t))
	require.Equal(t, uint64(4), truncator.lastTruncatedWAGIndex.Load())

	// Write two WAG nodes whose events are applied (index <= 100).
	e.writeWAGNode(t, wagpb.Event{
		Addr: wagpb.MakeAddr(r1, 40), Type: wagpb.EventApply,
	})
	e.writeWAGNode(t, wagpb.Event{
		Addr: wagpb.MakeAddr(r1, 50), Type: wagpb.EventApply,
	})
	require.Equal(t, []uint64{5, 6}, e.listWAGNodes(t))

	// Now flush the state engine and signal again. Both nodes should be
	// truncated since their events are applied.
	flushAndWaitForTruncation()
	require.Equal(t, ([]uint64)(nil), e.listWAGNodes(t))
	require.Equal(t, uint64(6), truncator.lastTruncatedWAGIndex.Load())

	// Write another WAG node but it is NOT applied yet.
	e.writeWAGNode(t, wagpb.Event{
		Addr: wagpb.MakeAddr(r1, 200), Type: wagpb.EventApply,
	})
	flushAndWaitForTruncation()
	// Node 7 should remain because its event isn't applied yet.
	require.Equal(t, []uint64{7}, e.listWAGNodes(t))
	require.Equal(t, uint64(6), truncator.lastTruncatedWAGIndex.Load())

	// Advance the applied index past 200 and flush. Now node 7 should be
	// truncated.
	require.NoError(t, sl.SetRangeAppliedState(ctx, e.StateEngine(),
		&kvserverpb.RangeAppliedState{RaftAppliedIndex: 200}))
	flushAndWaitForTruncation()
	require.Equal(t, ([]uint64)(nil), e.listWAGNodes(t))
	require.Equal(t, uint64(7), truncator.lastTruncatedWAGIndex.Load())
}
