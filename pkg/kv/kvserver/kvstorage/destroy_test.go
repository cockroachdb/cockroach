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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestDestroyReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	storage.DisableMetamorphicSimpleValueEncoding(t) // for deterministic output
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	var sb redact.StringBuilder
	ctx := context.Background()
	mutate := func(name string, write func(storage.ReadWriter)) {
		b := eng.NewBatch()
		defer b.Close()
		write(b)
		str, err := print.DecodeWriteBatch(b.Repr())
		require.NoError(t, err)
		_, err = sb.WriteString(fmt.Sprintf(">> %s:\n%s", name, str))
		require.NoError(t, err)
		require.NoError(t, b.Commit(false))
	}

	r := replicaInfo{
		id:      roachpb.FullReplicaID{RangeID: 123, ReplicaID: 3},
		hs:      raftpb.HardState{Term: 5, Commit: 14},
		ts:      kvserverpb.RaftTruncatedState{Index: 10, Term: 5},
		keys:    roachpb.RSpan{Key: []byte("a"), EndKey: []byte("z")},
		last:    15,
		applied: 12,
	}
	mutate("raft", func(rw storage.ReadWriter) {
		r.createRaftState(ctx, t, rw)
	})
	mutate("state", func(rw storage.ReadWriter) {
		r.createStateMachine(ctx, t, rw)
	})
	mutate("destroy", func(rw storage.ReadWriter) {
		require.NoError(t, DestroyReplica(ctx, r.id, rw, rw, r.id.ReplicaID+1, ClearRangeDataOptions{
			ClearUnreplicatedByRangeID: true,
			ClearReplicatedByRangeID:   true,
			ClearReplicatedBySpan:      r.keys,
		}))
	})

	str := strings.ReplaceAll(sb.String(), "\n\n", "\n")
	echotest.Require(t, str, filepath.Join(datapathutils.TestDataPath(t), t.Name()+".txt"))
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

func (r *replicaInfo) createStateMachine(ctx context.Context, t *testing.T, rw storage.ReadWriter) {
	sl := stateloader.Make(r.id.RangeID)
	require.NoError(t, sl.SetRangeTombstone(ctx, rw, kvserverpb.RangeTombstone{
		NextReplicaID: r.id.ReplicaID,
	}))
	require.NoError(t, sl.SetRaftReplicaID(ctx, rw, r.id.ReplicaID))
	// TODO(pav-kv): figure out whether LastReplicaGCTimestamp should be in the
	// log or state engine.
	require.NoError(t, storage.MVCCBlindPutProto(
		ctx, rw,
		keys.RangeLastReplicaGCTimestampKey(r.id.RangeID),
		hlc.Timestamp{}, /* timestamp */
		&hlc.Timestamp{WallTime: 12345678},
		storage.MVCCWriteOptions{},
	))
	createRangeData(t, rw, r.keys)
}

func createRangeData(t *testing.T, rw storage.ReadWriter, span roachpb.RSpan) {
	ts := hlc.Timestamp{WallTime: 1}
	for _, k := range []roachpb.Key{
		keys.RangeDescriptorKey(span.Key),   // system
		span.Key.AsRawKey(),                 // user
		roachpb.Key(span.EndKey).Prevish(2), // user
	} {
		// Put something under the system or user key.
		require.NoError(t, rw.PutMVCC(
			storage.MVCCKey{Key: k, Timestamp: ts}, storage.MVCCValue{},
		))
		// Put something under the corresponding lock key.
		ek, _ := storage.LockTableKey{
			Key: k, Strength: lock.Intent, TxnUUID: uuid.UUID{},
		}.ToEngineKey(nil)
		require.NoError(t, rw.PutEngineKey(ek, nil))
	}
}
