package batcheval_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestDeleteRangeTombstoneSetsGCHint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	serv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue: true,
				DisableSplitQueue: true,
			},
		},
	})
	s := serv.(*server.TestServer)
	defer s.Stopper().Stop(ctx)

	store, err := s.Stores().GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	key := roachpb.Key("b")
	content := []byte("test")

	pArgs := &roachpb.PutRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Value: roachpb.MakeValueFromBytes(content),
	}
	if _, pErr := kv.SendWrapped(ctx, s.DistSender(), pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	r, err := s.LookupRange(key)
	require.NoError(t, err, "failed to lookup range")

	gcHintOpt := roachpb.GCRangeHint{
		EstimatedRangeRemovalTime: hlc.Timestamp{WallTime: 2e9},
	}
	drArgs := &roachpb.DeleteRangeRequest{
		GcRangeHint:       &gcHintOpt,
		UseRangeTombstone: true,
		RequestHeader: roachpb.RequestHeader{
			Key:    r.StartKey.AsRawKey(),
			EndKey: r.EndKey.AsRawKey(),
		},
	}
	if _, pErr := kv.SendWrapped(ctx, s.DistSender(), drArgs); pErr != nil {
		t.Fatal(pErr)
	}

	repl := store.LookupReplica(roachpb.RKey(key))
	gcHint := repl.GetGCHint()

	require.EqualValues(t, gcHintOpt, gcHint, "gc hint was not set correctly")
}

