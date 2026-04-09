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
			truncator := MakeWAGTruncator(st, e.StateEngine(), e.LogEngine(),
				rate.NewLimiter(rate.Inf, math.MaxInt64))
			tc.setup(t, &e)
			require.NoError(t, e.stateEngine.Flush())
			require.NoError(t, truncator.TruncateAll(ctx))
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

			truncator := MakeWAGTruncator(st, e.StateEngine(), e.LogEngine(),
				rate.NewLimiter(rate.Inf, math.MaxInt64))

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
			require.NoError(t, truncator.TruncateAll(ctx))
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
// handles gaps in WAG node indices correctly based on expectedIndex. When
// expectedIndex is 0, the first node is deleted regardless of its index. When
// non-zero, only the node at that exact index is deleted.
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

	type call struct {
		index             uint64
		expectedTruncated bool
		wantWAGIndices    []uint64
	}

	for _, tc := range []struct {
		calls []call
	}{
		{
			// index=0 removes the first WAG node regardless of its index.
			calls: []call{
				{index: 0, expectedTruncated: true, wantWAGIndices: []uint64{4, 6}},
				{index: 0, expectedTruncated: true, wantWAGIndices: []uint64{6}},
				{index: 0, expectedTruncated: true, wantWAGIndices: nil},
			},
		},
		{
			// A non-existent index is a no-op.
			calls: []call{
				{index: 1, expectedTruncated: false, wantWAGIndices: []uint64{2, 4, 6}},
				{index: 3, expectedTruncated: false, wantWAGIndices: []uint64{2, 4, 6}},
				{index: 5, expectedTruncated: false, wantWAGIndices: []uint64{2, 4, 6}},
				{index: 7, expectedTruncated: false, wantWAGIndices: []uint64{2, 4, 6}},
			},
		},
		{
			// In theory, we can remove a WAG node at an index that is not the first.
			calls: []call{
				{index: 4, expectedTruncated: true, wantWAGIndices: []uint64{2, 6}},
				{index: 6, expectedTruncated: true, wantWAGIndices: []uint64{2}},
				{index: 2, expectedTruncated: true, wantWAGIndices: nil},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			e := makeTestEngines()
			defer e.Close()
			truncator := MakeWAGTruncator(
				st, e.StateEngine(), e.LogEngine(),
				rate.NewLimiter(rate.Inf, math.MaxInt64),
			)

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

			for _, c := range tc.calls {
				stateReader := e.StateEngine().NewReader(storage.GuaranteedDurability)
				b := e.LogEngine().NewWriteBatch()
				truncated, err := truncator.truncateAppliedWAGNodeAndClearRaftState(
					ctx, Raft{RO: e.LogEngine(), WO: b}, stateReader, c.index,
				)
				require.NoError(t, err)
				require.Equal(t, c.expectedTruncated, truncated)
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
