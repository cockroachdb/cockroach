package kvserver

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestPrepareSnapApply(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var sb redact.StringBuilder
	writeSST := func(_ context.Context, data []byte) error {
		return storageutils.ReportSSTEntries(&sb, "sst", data)
	}

	desc := func(id roachpb.RangeID, start, end string) *roachpb.RangeDescriptor {
		return &roachpb.RangeDescriptor{
			RangeID:  id,
			StartKey: roachpb.RKey(start),
			EndKey:   roachpb.RKey(end),
		}
	}

	const rangeID, replicaID = roachpb.RangeID(123), roachpb.ReplicaID(4)
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	sl := stateloader.Make(rangeID)
	ctx := context.Background()
	require.NoError(t, sl.SetRaftReplicaID(ctx, eng, replicaID))
	for _, rID := range []roachpb.RangeID{101, 102} {
		require.NoError(t, stateloader.Make(rID).SetRaftReplicaID(ctx, eng, replicaID))
	}

	in := prepareSnapApplyInput{
		id:       storage.FullReplicaID{RangeID: rangeID, ReplicaID: 4},
		st:       cluster.MakeTestingClusterSettings(),
		todoEng:  eng,
		sl:       sl,
		writeSST: writeSST,

		truncState: kvserverpb.RaftTruncatedState{Index: 100, Term: 20},
		hardState:  raftpb.HardState{Term: 20, Commit: 100},
		desc:       desc(rangeID, "a", "k"),
		subsumedDescs: []*roachpb.RangeDescriptor{
			desc(101, "a", "b"),
			desc(102, "b", "z"),
		},
	}

	clearedUnreplicatedSpan, clearedSubsumedSpans, err := prepareSnapApply(ctx, in)
	require.NoError(t, err)

	sb.Printf(">> excise: %v\n", in.desc.KeySpan().AsRawSpanWithNoLocals())
	for _, span := range rditer.MakeReplicatedKeySpans(in.desc) {
		sb.Printf(">> repl: %v\n", span)
	}
	sb.Printf(">> unrepl: %v\n", clearedUnreplicatedSpan)
	for _, span := range clearedSubsumedSpans {
		sb.Printf(">> subsumed: %v\n", span)
	}

	echotest.Require(t, sb.String(), filepath.Join("testdata", t.Name()+".txt"))
}
