// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/print"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

// TestPrepareSnapApply tests the snapshot application code, which prepares a
// set of SSTs to be ingested together with the snapshot SSTs. It this test, the
// snapshot [a,k) subsumes replicas [a,b) and [b,z). Note that the key span
// covered by the subsumed replicas is wider than the snapshot's.
func TestPrepareSnapApply(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	storage.DisableMetamorphicSimpleValueEncoding(t) // for deterministic output
	testutils.RunTrueAndFalse(t, "sep-eng", testPrepareSnapApply)
}

// testPrepareSnapApply tests the snapshot application code, which prepares a
// set of SSTs to be ingested together with the snapshot SSTs. It this test, the
// snapshot [a,k) subsumes replicas [a,b) and [b,z). Note that the key span
// covered by the subsumed replicas is wider than the snapshot's.
//
// If separateEngines is true, the raft engine write is separated from the rest.
func testPrepareSnapApply(t *testing.T, separateEngines bool) {
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	var sb redact.StringBuilder

	printBatch := func(b storage.WriteBatch) string {
		str, err := print.DecodeWriteBatch(b.Repr())
		require.NoError(t, err)
		return str
	}
	writeSST := func(ctx context.Context, write func(context.Context, storage.Writer) error) error {
		// Use WriteBatch so that we print the writes in exactly the order in which
		// they are made. The real code creates an SST writer.
		b := eng.NewWriteBatch()
		defer b.Close()
		if err := write(ctx, b); err != nil {
			return err
		}
		str := printBatch(b)
		if str == "" {
			return nil
		}
		_, err := sb.WriteString(fmt.Sprintf(">> sst:\n%s", str))
		require.NoError(t, err)
		return nil
	}

	desc := func(id roachpb.RangeID, start, end string) *roachpb.RangeDescriptor {
		return &roachpb.RangeDescriptor{
			RangeID:  id,
			StartKey: roachpb.RKey(start),
			EndKey:   roachpb.RKey(end),
		}
	}

	const replicaID = 4
	id := roachpb.FullReplicaID{RangeID: 123, ReplicaID: replicaID}
	descA := desc(101, "a", "b")
	descB := desc(102, "b", "z")
	createRangeData(t, eng, *descA)
	createRangeData(t, eng, *descB)

	sl := kvstorage.MakeStateLoader(id.RangeID)
	ctx := context.Background()
	require.NoError(t, sl.SetRaftReplicaID(ctx, eng, id.ReplicaID))
	for _, rID := range []roachpb.RangeID{101, 102} {
		require.NoError(t, kvstorage.MakeStateLoader(rID).SetRaftReplicaID(ctx, eng, replicaID))
	}

	rangeDesc := desc(id.RangeID, "a", "k")
	swb := snapWriteBuilder{
		id: id,
		sl: sl,
		wr: snapWriter{
			stateRO:  eng,
			raftRO:   eng,
			writeSST: writeSST,
			cleared:  rditer.MakeReplicatedKeySpans(rangeDesc),
		},

		truncState: kvserverpb.RaftTruncatedState{Index: 100, Term: 20},
		hardState:  raftpb.HardState{Term: 20, Commit: 100},
		desc:       rangeDesc,
		origDesc:   rangeDesc,
		subsume: []kvstorage.DestroyReplicaInfo{
			{FullReplicaID: roachpb.FullReplicaID{RangeID: descA.RangeID, ReplicaID: replicaID}, Keys: descA.RSpan()},
			{FullReplicaID: roachpb.FullReplicaID{RangeID: descB.RangeID, ReplicaID: replicaID}, Keys: descB.RSpan()},
		},
	}
	if separateEngines {
		swb.wr.separateEngines(eng)
		defer swb.wr.close()
	}

	require.NoError(t, swb.prepareSnapApply(ctx))

	for _, span := range swb.wr.cleared {
		sb.Printf(">> cleared: %v\n", span)
	}
	sb.Printf(">> excise: %v\n", swb.desc.KeySpan().AsRawSpanWithNoLocals())
	if separateEngines {
		sb.Printf(">> raft:\n%s", printBatch(swb.wr.raftWO))
	}

	echotest.Require(t, sb.String(), filepath.Join("testdata", t.Name()))
}

func createRangeData(t *testing.T, eng storage.Engine, desc roachpb.RangeDescriptor) {
	ts := hlc.Timestamp{WallTime: 1}
	for _, k := range []roachpb.Key{
		keys.RangeDescriptorKey(desc.StartKey), // system
		desc.StartKey.AsRawKey(),               // user
		roachpb.Key(desc.EndKey).Prevish(2),    // user
	} {
		// Put something under the system or user key.
		require.NoError(t, eng.PutMVCC(
			storage.MVCCKey{Key: k, Timestamp: ts}, storage.MVCCValue{},
		))
		// Put something under the corresponding lock key.
		ek, _ := storage.LockTableKey{
			Key: k, Strength: lock.Intent, TxnUUID: uuid.UUID{},
		}.ToEngineKey(nil)
		require.NoError(t, eng.PutEngineKey(ek, nil))
	}

	// Add some raft state: HardState, TruncatedState and log entries.
	const truncIndex, numEntries = 10, 3
	ctx := context.Background()

	sl := logstore.NewStateLoader(desc.RangeID)
	require.NoError(t, sl.SetRaftTruncatedState(
		ctx, eng, &kvserverpb.RaftTruncatedState{Index: truncIndex, Term: 5},
	))
	require.NoError(t, sl.SetHardState(
		ctx, eng, raftpb.HardState{Term: 6, Commit: truncIndex + numEntries},
	))
	for i := truncIndex + 1; i <= truncIndex+numEntries; i++ {
		require.NoError(t, storage.MVCCBlindPutProto(
			ctx, eng,
			sl.RaftLogKey(kvpb.RaftIndex(i)), hlc.Timestamp{},
			&raftpb.Entry{Index: uint64(i), Term: 6},
			storage.MVCCWriteOptions{},
		))
	}
}
