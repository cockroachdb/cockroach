// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/print"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestDestroyReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	storage.DisableMetamorphicSimpleValueEncoding(t) // for deterministic output

	printBatch := func(name string, b storage.WriteBatch) string {
		str, err := print.DecodeWriteBatch(b.Repr())
		require.NoError(t, err)
		return fmt.Sprintf(">> %s:\n%s", name, str)
	}
	mutate := func(name string, eng storage.Engine, write func(storage.Writer)) string {
		b := eng.NewWriteBatch()
		defer b.Close()
		write(b)
		str := printBatch(name, b)
		require.NoError(t, b.Commit(false))
		return str
	}
	mutateSep := func(name string, eng Engines, write func(ReadWriter)) string {
		b := makeTestBatch(eng)
		defer b.close()
		write(b.readWriter())
		var str string
		if eng.Separated() {
			str = printBatch(name+"/state", b.batch) + printBatch(name+"/raft", b.raftBatch)
		} else {
			str = printBatch(name, b.batch)
		}
		require.NoError(t, b.commit())
		return str
	}

	r := replicaInfo{
		id:      roachpb.FullReplicaID{RangeID: 123, ReplicaID: 3},
		hs:      raftpb.HardState{Term: 5, Commit: 14},
		ts:      kvserverpb.RaftTruncatedState{Index: 10, Term: 5},
		keys:    roachpb.RSpan{Key: []byte("a"), EndKey: []byte("z")},
		last:    15,
		applied: 12,
	}

	runTest := func(t *testing.T, e Engines) {
		ctx := context.Background()
		out := mutate("raft", e.LogEngine(), func(w storage.Writer) {
			r.createRaftState(ctx, t, w)
		}) + mutate("state", e.StateEngine(), func(w storage.Writer) {
			r.createStateMachine(ctx, t, w)
		}) + mutateSep("destroy", e, func(rw ReadWriter) {
			require.NoError(t, DestroyReplica(
				ctx, rw,
				DestroyReplicaInfo{FullReplicaID: r.id, Keys: r.keys}, r.id.ReplicaID+1,
			))
		})

		out = strings.ReplaceAll(out, "\n\n", "\n")
		name := strings.ReplaceAll(t.Name(), "/", "-") + ".txt"
		echotest.Require(t, out, filepath.Join(datapathutils.TestDataPath(t), name))
	}

	t.Run("one-eng", func(t *testing.T) {
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()
		runTest(t, MakeEngines(eng))
	})
	t.Run("sep-eng", func(t *testing.T) {
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()
		runTest(t, MakeSeparatedEnginesForTesting(eng, eng))
	})

}

// replicaInfo contains the basic info about the replica, used for generating
// its storage counterpart.
//
// TODO(pav-kv): make it reusable for other tests.
type replicaInfo struct {
	id      roachpb.FullReplicaID
	hs      raftpb.HardState
	ts      kvserverpb.RaftTruncatedState
	keys    roachpb.RSpan
	last    kvpb.RaftIndex
	applied kvpb.RaftIndex
}

func (r *replicaInfo) createRaftState(ctx context.Context, t *testing.T, w storage.Writer) {
	sl := logstore.NewStateLoader(r.id.RangeID)
	require.NoError(t, sl.SetHardState(ctx, w, r.hs))
	require.NoError(t, sl.SetRaftTruncatedState(ctx, w, &r.ts))
	for i := r.ts.Index + 1; i <= r.last; i++ {
		require.NoError(t, storage.MVCCBlindPutProto(
			ctx, w,
			sl.RaftLogKey(i), hlc.Timestamp{}, /* timestamp */
			&raftpb.Entry{Index: uint64(i), Term: 5},
			storage.MVCCWriteOptions{},
		))
	}
}

func (r *replicaInfo) createStateMachine(ctx context.Context, t *testing.T, w storage.Writer) {
	sl := MakeStateLoader(r.id.RangeID)
	require.NoError(t, sl.SetRangeTombstone(ctx, w, kvserverpb.RangeTombstone{
		NextReplicaID: r.id.ReplicaID,
	}))
	require.NoError(t, sl.SetRaftReplicaID(ctx, w, r.id.ReplicaID))
	// TODO(pav-kv): figure out whether LastReplicaGCTimestamp should be in the
	// log or state engine.
	require.NoError(t, storage.MVCCBlindPutProto(
		ctx, w,
		keys.RangeLastReplicaGCTimestampKey(r.id.RangeID),
		hlc.Timestamp{}, /* timestamp */
		&hlc.Timestamp{WallTime: 12345678},
		storage.MVCCWriteOptions{},
	))
	createRangeData(t, w, r.keys)
}

func createRangeData(t *testing.T, w storage.Writer, span roachpb.RSpan) {
	ts := hlc.Timestamp{WallTime: 1}
	for _, k := range []roachpb.Key{
		keys.RangeDescriptorKey(span.Key),   // system
		span.Key.AsRawKey(),                 // user
		roachpb.Key(span.EndKey).Prevish(2), // user
	} {
		// Put something under the system or user key.
		require.NoError(t, w.PutMVCC(
			storage.MVCCKey{Key: k, Timestamp: ts}, storage.MVCCValue{},
		))
		// Put something under the corresponding lock key.
		ek, _ := storage.LockTableKey{
			Key: k, Strength: lock.Intent, TxnUUID: uuid.UUID{},
		}.ToEngineKey(nil)
		require.NoError(t, w.PutEngineKey(ek, nil))
	}
}

type testBatch struct {
	batch     storage.Batch
	raftBatch storage.Batch
}

func makeTestBatch(eng Engines) testBatch {
	if !eng.Separated() {
		return testBatch{batch: eng.Engine().NewBatch()}
	}
	return testBatch{
		batch:     eng.StateEngine().NewBatch(),
		raftBatch: eng.LogEngine().NewBatch(),
	}
}

func (b *testBatch) readWriter() ReadWriter {
	if b.raftBatch == nil {
		return TODOReadWriter(b.batch)
	}
	return ReadWriter{
		State: State{RO: b.batch, WO: b.batch},
		Raft:  Raft{RO: b.raftBatch, WO: b.raftBatch},
	}
}

func (b *testBatch) commit() error {
	if b.raftBatch != nil {
		if err := b.raftBatch.Commit(false /* sync */); err != nil {
			return err
		}
	}
	return b.batch.Commit(false /* sync */)
}

func (b *testBatch) close() {
	b.batch.Close()
	if b.raftBatch != nil {
		b.raftBatch.Close()
	}
}
