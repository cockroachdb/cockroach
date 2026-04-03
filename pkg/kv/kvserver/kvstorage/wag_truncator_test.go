// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag/wagpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
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

// truncateWAGNodes repeatedly truncates applied WAG nodes until none remain
// applied.
func (e *testEngines) truncateWAGNodes(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	for {
		b := e.LogEngine().NewWriteBatch()
		truncated, err := truncateAppliedNode(
			ctx, Raft{RO: e.LogEngine(), WO: b}, e.StateEngine(),
		)
		if err == nil && truncated {
			err = b.Commit(false /* sync */)
		}
		b.Close()
		require.NoError(t, err)
		if !truncated {
			return
		}
	}
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
			// replica id is < the event replica id.
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
				// EventDestroy won't be deleted if the replica id still matches the
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
				// EventSubsume won't be deleted if the replica id still matches the
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
			tc.setup(t, &e)
			e.truncateWAGNodes(t)
			require.Equal(t, tc.wantWAGIndices, e.listWAGNodes(t))
		})
	}
}
