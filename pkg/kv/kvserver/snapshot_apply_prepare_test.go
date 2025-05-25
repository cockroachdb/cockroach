// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
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

	var sb redact.StringBuilder
	writeSST := func(_ context.Context, data []byte) error {
		// TODO(pav-kv): range deletions are printed separately from point keys, and
		// this out-of-order output looks confusing. Improve it.
		return storageutils.ReportSSTEntries(&sb, "sst", data)
	}

	desc := func(id roachpb.RangeID, start, end string) *roachpb.RangeDescriptor {
		return &roachpb.RangeDescriptor{
			RangeID:  id,
			StartKey: roachpb.RKey(start),
			EndKey:   roachpb.RKey(end),
		}
	}

	id := storage.FullReplicaID{RangeID: 123, ReplicaID: 4}
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	descA := desc(101, "a", "b")
	descB := desc(102, "b", "z")
	createRangeData(t, eng, *descA)
	createRangeData(t, eng, *descB)

	sl := stateloader.Make(id.RangeID)
	ctx := context.Background()
	require.NoError(t, sl.SetRaftReplicaID(ctx, eng, id.ReplicaID))
	for _, rID := range []roachpb.RangeID{101, 102} {
		require.NoError(t, stateloader.Make(rID).SetRaftReplicaID(ctx, eng, id.ReplicaID))
	}

	in := prepareSnapApplyInput{
		id:       id,
		st:       cluster.MakeTestingClusterSettings(),
		todoEng:  eng,
		sl:       sl,
		writeSST: writeSST,

		truncState:    kvserverpb.RaftTruncatedState{Index: 100, Term: 20},
		hardState:     raftpb.HardState{Term: 20, Commit: 100},
		desc:          desc(id.RangeID, "a", "k"),
		subsumedDescs: []*roachpb.RangeDescriptor{descA, descB},
	}

	clearedUnreplicatedSpan, clearedSubsumedSpans, err := prepareSnapApply(ctx, in)
	require.NoError(t, err)

	sb.Printf(">> unrepl: %v\n", clearedUnreplicatedSpan)
	for _, span := range rditer.MakeReplicatedKeySpans(in.desc) {
		sb.Printf(">> repl: %v\n", span)
	}
	for _, span := range clearedSubsumedSpans {
		sb.Printf(">> subsumed: %v\n", span)
	}
	sb.Printf(">> excise: %v\n", in.desc.KeySpan().AsRawSpanWithNoLocals())

	echotest.Require(t, sb.String(), filepath.Join("testdata", t.Name()+".txt"))
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
}
