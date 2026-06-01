// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
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
	b := eng.NewWriteBatch()
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
	ent1 := makeTestEntry(t, 11, 1, kvserverpb.RaftCommand{
		ProposerLeaseSequence: lease.Sequence,
		MaxLeaseIndex:         11,
		WriteBatch:            makeWriteBatch(t, stateEng, "key1", "val1"),
	})
	ent2 := makeTestEntry(t, 12, 1, kvserverpb.RaftCommand{
		ProposerLeaseSequence: lease.Sequence,
		MaxLeaseIndex:         12,
		WriteBatch:            makeWriteBatch(t, stateEng, "key2", "val2"),
	})

	// Create an appBatch with a pebble batch and initial state.
	ab := appBatch{
		state: kvserverpb.ReplicaState{
			RaftAppliedIndex: 10,
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
	require.NoError(t, ab.applyEntry(ctx, &cmd, postAddEnv{}))
	require.NoError(t, cmd.Decode(&ent2))
	require.NoError(t, ab.applyEntry(ctx, &cmd, postAddEnv{}))

	// Write the applied state and commit atomically.
	require.NoError(t, ab.addAppliedStateToBatch(ctx))
	require.NoError(t, ab.batch.Commit(false /* sync */))

	// Verify in-memory state was updated.
	require.Equal(t, kvpb.RaftIndex(12), ab.state.RaftAppliedIndex)
	require.Equal(t, kvpb.LeaseAppliedIndex(12), ab.state.LeaseAppliedIndex)

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

// TestReplayBatchAddSSTable verifies that an AddSSTable command stored in the
// raft log as a thin entry (sideloaded payload on disk) is correctly re-inlined
// and applied to the state engine by the standalone replay path. It drives
// logstore.VisitInlined directly — the same primitive replayRaftLogBatch uses.
func TestReplayBatchAddSSTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	st := cluster.MakeTestingClusterSettings()
	eng := kvstorage.MakeSeparatedEnginesForTesting(
		storage.NewDefaultInMemForTesting(), storage.NewDefaultInMemForTesting(),
	)
	defer eng.Close()
	var seq wag.Seq
	bf := kvstorage.MakeBatchFactory(&eng, &seq)
	stateEng := eng.StateEngine()
	logEng := eng.LogEngine()
	bulkLimiter := rate.NewLimiter(rate.Inf, math.MaxInt64)

	rangeID := roachpb.RangeID(1)
	sideloaded := logstore.NewDiskSideloadStorage(
		st, rangeID, stateEng.GetAuxiliaryDir(), bulkLimiter, stateEng.Env(),
	)

	// Build an SST containing multiple keys.
	type kv struct{ k, v string }
	kvs := []kv{{"key1", "val1"}, {"key2", "val2"}, {"key3", "val3"}}
	ts := hlc.Timestamp{WallTime: 1}
	sstFile := &storage.MemObject{}
	sstW := storage.MakeIngestionSSTWriter(ctx, st, sstFile)
	defer sstW.Close()
	ingested := make([]storage.MVCCKeyValue, len(kvs))
	for i, kv := range kvs {
		val := roachpb.MakeValueFromBytes([]byte(kv.v))
		val.InitChecksum([]byte(kv.k))
		ingested[i] = storage.MVCCKeyValue{
			Key:   storage.MVCCKey{Key: []byte(kv.k), Timestamp: ts},
			Value: val.RawBytes,
		}
		require.NoError(t, sstW.Put(ingested[i].Key, ingested[i].Value))
	}
	require.NoError(t, sstW.Finish())
	sstData := sstFile.Data()

	// Wrap the SST in an AddSSTable command, strip the payload into sideloaded
	// storage (mirroring logstore.StoreEntries on the append path), and write
	// the thin entry to the raft log.
	idx, term := kvpb.RaftIndex(11), kvpb.RaftTerm(1)
	lease := roachpb.Lease{Sequence: 1}
	fatEnt := makeTestEntry(t, idx, term, kvserverpb.RaftCommand{
		ProposerLeaseSequence: lease.Sequence,
		MaxLeaseIndex:         11,
		ReplicatedEvalResult: kvserverpb.ReplicatedEvalResult{
			AddSSTable: &kvserverpb.ReplicatedEvalResult_AddSSTable{
				Data:  sstData,
				CRC32: util.CRC32(sstData),
			},
		},
	})
	thinEnts, _, err := logstore.MaybeSideloadEntries(
		ctx, []raftpb.Entry{fatEnt}, sideloaded,
	)
	require.NoError(t, err)
	require.Len(t, thinEnts, 1)
	require.NoError(t, storage.MVCCPutProto(
		ctx, logEng, keys.RaftLogKey(rangeID, idx), hlc.Timestamp{},
		&thinEnts[0], storage.MVCCWriteOptions{},
	))

	// Minimal replayBatch — just enough state for assertAndCheckCommand to pass.
	rb := &replayBatch{
		ab: appBatch{
			state: kvserverpb.ReplicaState{
				RaftAppliedIndex: 10,
				Lease:            &lease,
				GCThreshold:      &hlc.Timestamp{},
				Desc:             &roachpb.RangeDescriptor{RangeID: rangeID},
				Stats:            &enginepb.MVCCStats{},
			},
			batch: bf.NewBatch(),
			sl:    kvstorage.MakeStateLoader(rangeID),
		},
		env: postAddEnv{
			st:          st,
			eng:         stateEng,
			sideloaded:  sideloaded,
			bulkLimiter: bulkLimiter,
		},
	}
	defer rb.Close()

	// Drive iteration the same way replayRaftLogBatch does: VisitInlined reads
	// the thin entry from the raft log, inlines its sideloaded payload via
	// rb.Sideloaded(), and yields a fat entry to ApplyEntry.
	require.NoError(t, logstore.VisitInlined(
		ctx, logEng, rangeID, rb.Sideloaded(), nil, /* eCache */
		idx, idx+1, func(ent raftpb.Entry) error {
			_, err := rb.ApplyEntry(ctx, ent)
			return err
		},
	))
	require.NoError(t, rb.Commit(ctx))

	// The SST was ingested into the state engine: all keys are visible.
	for _, want := range ingested {
		got, err := storage.Scan(ctx, stateEng, want.Key.Key, want.Key.Key.Next(), 1)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, want.Value, got[0].Value)
	}
}
