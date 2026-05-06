// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// makeTestEntry creates a raftpb.Entry with an encoded RaftCommand for testing.
func makeTestEntry(
	t *testing.T, index kvpb.RaftIndex, term kvpb.RaftTerm, cmd kvserverpb.RaftCommand,
) raftpb.Entry {
	t.Helper()
	cmdID := kvserverbase.CmdIDKey(fmt.Sprintf("%08d", index))
	data, err := raftlog.EncodeCommand(
		context.Background(), &cmd, cmdID, raftlog.EncodeOptions{},
	)
	require.NoError(t, err)
	return raftpb.Entry{
		Index: uint64(index),
		Term:  uint64(term),
		Type:  raftpb.EntryNormal,
		Data:  data,
	}
}

// makeWriteBatch creates a WriteBatch containing a single unversioned
// key-value pair.
func makeWriteBatch(t *testing.T, eng storage.Engine, key, val string) *kvserverpb.WriteBatch {
	t.Helper()
	b := eng.NewBatch()
	defer b.Close()
	require.NoError(t, b.PutUnversioned(roachpb.Key(key), []byte(val)))
	return &kvserverpb.WriteBatch{Data: b.Repr()}
}

// TestAppBatchApplyEntry demonstrates the intended usage of appBatch for
// standalone log application: create an appBatch with a pebble batch, apply
// multiple entries, write the applied state, and commit atomically.
func TestAppBatchApplyEntry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	eng := kvstorage.MakeSeparatedEnginesForTesting(
		storage.NewDefaultInMemForTesting(), storage.NewDefaultInMemForTesting(),
	)
	defer eng.Close()
	var seq wag.Seq
	bf := kvstorage.MakeBatchFactory(&eng, &seq)

	lease := roachpb.Lease{Sequence: 1}
	ms := enginepb.MVCCStats{}
	rangeID := roachpb.RangeID(1)

	stateEng := eng.StateEngine()
	ent1 := makeTestEntry(t, 10, 1, kvserverpb.RaftCommand{
		ProposerLeaseSequence: lease.Sequence,
		MaxLeaseIndex:         10,
		WriteBatch:            makeWriteBatch(t, stateEng, "key1", "val1"),
	})
	ent2 := makeTestEntry(t, 11, 1, kvserverpb.RaftCommand{
		ProposerLeaseSequence: lease.Sequence,
		MaxLeaseIndex:         11,
		WriteBatch:            makeWriteBatch(t, stateEng, "key2", "val2"),
	})

	// Create an appBatch with a pebble batch and initial state.
	ab := appBatch{
		state: kvserverpb.ReplicaState{
			RaftAppliedIndex: 9,
			Lease:            &lease,
			GCThreshold:      &hlc.Timestamp{},
			Desc:             &roachpb.RangeDescriptor{RangeID: rangeID},
			Stats:            &ms,
		},
		batch: bf.NewBatch(),
		sl:    kvstorage.MakeStateLoader(rangeID),
	}
	defer ab.batch.Close()

	// Apply multiple entries into the same batch.
	var cmd replicatedCmd
	require.NoError(t, cmd.Decode(&ent1))
	require.NoError(t, ab.applyEntry(ctx, &cmd))
	require.NoError(t, cmd.Decode(&ent2))
	require.NoError(t, ab.applyEntry(ctx, &cmd))

	// Write the applied state and commit atomically.
	require.NoError(t, ab.addAppliedStateToBatch(ctx))
	require.NoError(t, ab.batch.Commit(false /* sync */))

	// Verify in-memory state was updated.
	require.Equal(t, kvpb.RaftIndex(11), ab.state.RaftAppliedIndex)
	require.Equal(t, kvpb.LeaseAppliedIndex(11), ab.state.LeaseAppliedIndex)

	// Verify both write batches were applied to the state engine.
	for _, kv := range []struct{ k, v string }{{"key1", "val1"}, {"key2", "val2"}} {
		kvs, err := storage.Scan(
			ctx, stateEng, roachpb.Key(kv.k), roachpb.Key(kv.k).Next(), 1,
		)
		require.NoError(t, err)
		require.Len(t, kvs, 1)
		require.Equal(t, []byte(kv.v), kvs[0].Value)
	}
}

// TestLoadRangeDescriptor verifies that loadRangeDescriptor finds a descriptor
// by range ID when scanning all descriptors on disk.
func TestLoadRangeDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	// Write two descriptors.
	for _, tc := range []struct {
		rangeID  roachpb.RangeID
		startKey string
		endKey   string
	}{
		{rangeID: 1, startKey: "a", endKey: "m"},
		{rangeID: 2, startKey: "m", endKey: "z"},
	} {
		desc := roachpb.RangeDescriptor{
			RangeID:  tc.rangeID,
			StartKey: keys.MustAddr(roachpb.Key(tc.startKey)),
			EndKey:   keys.MustAddr(roachpb.Key(tc.endKey)),
			InternalReplicas: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, ReplicaID: 1},
			},
		}
		descKey := keys.RangeDescriptorKey(desc.StartKey)
		require.NoError(t, storage.MVCCPutProto(
			ctx, eng, descKey, hlc.Timestamp{WallTime: 1}, &desc, storage.MVCCWriteOptions{},
		))
	}

	// Find each by range ID.
	desc, err := loadRangeDescriptor(ctx, eng, 1)
	require.NoError(t, err)
	require.Equal(t, roachpb.RangeID(1), desc.RangeID)
	require.Equal(t, keys.MustAddr(roachpb.Key("a")), desc.StartKey)

	desc, err = loadRangeDescriptor(ctx, eng, 2)
	require.NoError(t, err)
	require.Equal(t, roachpb.RangeID(2), desc.RangeID)

	// Missing range ID returns an error.
	_, err = loadRangeDescriptor(ctx, eng, 99)
	require.ErrorContains(t, err, "descriptor not found for r99")
}
