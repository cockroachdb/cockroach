// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

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

func TestVerifySnap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Parses spans like "a-b".
	desc := func(s string) roachpb.RangeDescriptor {
		if s == "" {
			return roachpb.RangeDescriptor{}
		}
		require.True(t, len(s) == 3 && s[1] == '-')
		return roachpb.RangeDescriptor{StartKey: roachpb.RKey(s[:1]), EndKey: roachpb.RKey(s[2:])}
	}
	// Parses lists of spans like "a-b c-d".
	sub := func(s string) []kvstorage.DestroyReplicaInfo {
		if s == "" {
			return nil
		}
		var res []kvstorage.DestroyReplicaInfo
		for _, part := range strings.Split(s, " ") {
			d := desc(part)
			res = append(res, kvstorage.DestroyReplicaInfo{Keys: d.RSpan()})
		}
		return res
	}

	for _, tc := range []struct {
		snap  string
		prev  string
		sub   string
		want  roachpb.RKey
		notOk bool
	}{
		{snap: "b-z", prev: "a-z", notOk: true},    // StartKey changed
		{snap: "a-z", sub: "c-d a-b", notOk: true}, // subsumed is unordered
		{snap: "a-z", sub: "a-c b-d", notOk: true}, // overlapping subsumed
		// Subsumed replicas not strictly to the right from StartKey.
		{snap: "c-d", sub: "a-b", notOk: true},
		{snap: "c-d", sub: "a-c", notOk: true},
		{snap: "c-d", sub: "a-d", notOk: true},
		{snap: "c-d", sub: "c-d", notOk: true},
		// Subsumed replicas beyond the EndKey.
		{snap: "a-b", sub: "b-c", notOk: true},
		{snap: "a-b", sub: "c-d", notOk: true},
		// Initializing a replica. Cases of subsumed replicas wider and narrower.
		{snap: "a-f", want: nil},
		{snap: "a-f", sub: "b-e", want: nil},
		{snap: "a-f", sub: "b-f", want: nil},
		{snap: "a-f", sub: "b-c d-f", want: nil},
		{snap: "a-f", sub: "b-z", want: roachpb.RKey("z")},
		{snap: "a-f", sub: "b-e e-z", want: roachpb.RKey("z")},
		// Snapshot into an initialized replica. Cases where replica+subsumed is
		// wider or narrower than the snapshot.
		{snap: "a-k", prev: "a-c", want: nil},
		{snap: "a-k", prev: "a-k", want: nil},
		{snap: "a-k", prev: "a-c", sub: "c-d", want: nil},
		{snap: "a-k", prev: "a-c", sub: "c-k", want: nil},
		{snap: "a-k", prev: "a-c", sub: "c-d e-k", want: nil},
		{snap: "a-k", prev: "a-z", want: roachpb.RKey("z")},
		{snap: "a-k", prev: "a-c", sub: "c-z", want: roachpb.RKey("z")},
		{snap: "a-k", prev: "a-c", sub: "c-d e-z", want: roachpb.RKey("z")},
	} {
		t.Run("", func(t *testing.T) {
			prev, snap, subsume := desc(tc.prev), desc(tc.snap), sub(tc.sub)
			clearEnd, err := verifySnap(&prev, &snap, subsume)
			require.Equal(t, tc.notOk, err != nil)
			require.Equal(t, tc.want, clearEnd)
		})
	}
}

// TestPrepareSnapApply tests the snapshot application code, which prepares a
// set of SSTs to be ingested together with the snapshot SSTs. It this test, a
// replica [a,c) receives a snapshot [a,k) which makes it wider and also
// subsumes replica [c,e) and [f,z) that it overlaps.
//
// Note that the key span covered by the pre-snapshot replica and the subsumed
// replicas is wider than the snapshot's.
//
// If separateEngines is true, the raft engine write is separated from the rest.
// TODO(sep-raft-log): test the "apply as batch" variation as well.
func testPrepareSnapApply(t *testing.T, separateEngines bool) {
	const replicaID = 4
	id := roachpb.FullReplicaID{RangeID: 123, ReplicaID: replicaID}

	desc := func(id roachpb.RangeID, start, end string) *roachpb.RangeDescriptor {
		return &roachpb.RangeDescriptor{
			RangeID:  id,
			StartKey: roachpb.RKey(start),
			EndKey:   roachpb.RKey(end),
		}
	}
	// snapshot: [a---------------)k
	// replica:  [a---)c
	//  subsume:      [c---)e [f---------)z
	snapDesc := desc(id.RangeID, "a", "k")
	origDesc := desc(id.RangeID, "a", "c")
	descA := desc(101, "c", "e")
	descB := desc(102, "f", "z")

	var eng kvstorage.Engines
	if separateEngines {
		e := storage.NewDefaultInMemForTesting()
		eng = kvstorage.MakeSeparatedEnginesForTesting(e, e)
	} else {
		eng = kvstorage.MakeEngines(storage.NewDefaultInMemForTesting())
	}
	defer eng.Close()

	createRangeData(t, eng.StateEngine(), *descA)
	createRaftState(t, eng.LogEngine(), descA.RangeID)
	createRangeData(t, eng.StateEngine(), *descB)
	createRaftState(t, eng.LogEngine(), descB.RangeID)

	sl := kvstorage.MakeStateLoader(id.RangeID)
	ctx := context.Background()
	require.NoError(t, sl.SetRaftReplicaID(ctx, eng.StateEngine(), id.ReplicaID))
	for _, rID := range []roachpb.RangeID{descA.RangeID, descB.RangeID} {
		require.NoError(t, kvstorage.MakeStateLoader(rID).SetRaftReplicaID(
			ctx, eng.StateEngine(), replicaID))
	}

	printBatch := func(b storage.WriteBatch) string {
		str, err := print.DecodeWriteBatch(b.Repr())
		require.NoError(t, err)
		return str
	}
	var sb redact.StringBuilder
	writeSST := func(ctx context.Context, write func(context.Context, storage.Writer) error) error {
		// Use WriteBatch so that we print the writes in exactly the order in which
		// they are made. The real code creates an SST writer.
		var b storage.WriteBatch
		if eng.Separated() {
			b = eng.StateEngine().NewWriteBatch()
		} else {
			b = eng.Engine().NewWriteBatch()
		}
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

	sw := snapWriter{
		eng:      eng,
		writeSST: writeSST,
	}
	require.NoError(t, sw.prepareSnapApply(ctx, snapWrite{
		sl:         sl.StateLoader,
		truncState: kvserverpb.RaftTruncatedState{Index: 100, Term: 20},
		hardState:  raftpb.HardState{Term: 20, Commit: 100},
		desc:       snapDesc,
		origDesc:   origDesc,
		subsume: []kvstorage.DestroyReplicaInfo{
			{FullReplicaID: roachpb.FullReplicaID{RangeID: descA.RangeID, ReplicaID: replicaID}, Keys: descA.RSpan()},
			{FullReplicaID: roachpb.FullReplicaID{RangeID: descB.RangeID, ReplicaID: replicaID}, Keys: descB.RSpan()},
		},
	}))
	if eng.Separated() {
		sb.Printf(">> raft:\n%s", printBatch(sw.raftWO))
	}

	// The snapshot construction code is spread across MultiSSTWriter and
	// snapWriteBuilder. We only test the latter here, but for information also
	// print the replicated spans that MultiSSTWriter generates SSTs for.
	//
	// TODO(pav-kv): check a few invariants, such as that all SSTs don't overlap,
	// including with the replicated spans generated here.
	for _, span := range rditer.MakeReplicatedKeySpans(snapDesc) {
		sb.Printf(">> repl: %v\n", span)
	}
	sb.Printf(">> excise: %v\n", snapDesc.KeySpan().AsRawSpanWithNoLocals())

	echotest.Require(t, sb.String(), filepath.Join("testdata", t.Name()))
}

// createRangeData writes some state machine state for the given range.
func createRangeData(t *testing.T, stateWO kvstorage.StateWO, desc roachpb.RangeDescriptor) {
	ts := hlc.Timestamp{WallTime: 1}
	for _, k := range []roachpb.Key{
		keys.RangeDescriptorKey(desc.StartKey), // system
		desc.StartKey.AsRawKey(),               // user
		roachpb.Key(desc.EndKey).Prevish(2),    // user
	} {
		// Put something under the system or user key.
		require.NoError(t, stateWO.PutMVCC(
			storage.MVCCKey{Key: k, Timestamp: ts}, storage.MVCCValue{},
		))
		// Put something under the corresponding lock key.
		ek, _ := storage.LockTableKey{
			Key: k, Strength: lock.Intent, TxnUUID: uuid.UUID{},
		}.ToEngineKey(nil)
		require.NoError(t, stateWO.PutEngineKey(ek, nil))
	}
}

// createRaftState writes some raft state for the given range: HardState,
// RaftTruncatedState, and log entries.
func createRaftState(t *testing.T, raftWO kvstorage.RaftWO, rangeID roachpb.RangeID) {
	const truncIndex, numEntries = 10, 3
	ctx := context.Background()

	sl := logstore.NewStateLoader(rangeID)
	require.NoError(t, sl.SetRaftTruncatedState(
		ctx, raftWO, &kvserverpb.RaftTruncatedState{Index: truncIndex, Term: 5},
	))
	require.NoError(t, sl.SetHardState(
		ctx, raftWO, raftpb.HardState{Term: 6, Commit: truncIndex + numEntries},
	))
	for i := truncIndex + 1; i <= truncIndex+numEntries; i++ {
		require.NoError(t, storage.MVCCBlindPutProto(
			ctx, raftWO,
			sl.RaftLogKey(kvpb.RaftIndex(i)), hlc.Timestamp{},
			&raftpb.Entry{Index: uint64(i), Term: 6},
			storage.MVCCWriteOptions{},
		))
	}
}

func TestPrepareSnapApply(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	storage.DisableMetamorphicSimpleValueEncoding(t) // for deterministic output
	testutils.RunTrueAndFalse(t, "sep-eng", testPrepareSnapApply)
}
