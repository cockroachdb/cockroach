package kvserver_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"

	"github.com/cockroachdb/cockroach/pkg/util/log"

	"github.com/cockroachdb/cockroach/pkg/server"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestRecoverRangeWithNoReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	}
	tc := testcluster.StartTestCluster(t, 2, args)
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	desc, err := tc.AddReplicas(k, tc.Target(1))
	require.NoError(t, err)
	require.NoError(t, tc.TransferRangeLease(desc, tc.Target(1)))

	svr := tc.Server(0)
	require.NoError(t, svr.DB().Put(ctx, k, "bar"))

	tc.StopServer(1)

	// Sanity check that requests to the ScratchRange time out.
	cCtx, cancel := context.WithTimeout(ctx, 10000*time.Millisecond)
	defer cancel()
	require.Error(t, svr.DB().Put(cCtx, k, "baz"))
	require.Equal(t, context.DeadlineExceeded, cCtx.Err())

	// TODO: Figure out: is this necessary?
	//var exp []byte
	//if err := svr.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
	//	kvs, err := sql.ScanMetaKVs(ctx, txn, roachpb.Span{
	//		Key:    roachpb.KeyMin,
	//		EndKey: roachpb.KeyMax,
	//	})
	//	if err != nil {
	//		return err
	//	}
	//
	//	for i := range kvs {
	//		if err := kvs[i].Value.GetProto(&desc); err != nil {
	//			return err
	//		}
	//		if desc.RangeID == desc.RangeID {
	//			exp = kvs[i].Value.TagAndDataBytes()
	//			return nil
	//		}
	//	}
	//	return errors.Newf("r%d not found", desc.RangeID)
	//}); err != nil {
	//	// TODO: update this
	//	t.Fatal(err)
	//}

	var store *kvserver.Store
	if err := svr.GetStores().(*kvserver.Stores).VisitStores(func(inner *kvserver.Store) error {
		if store == nil {
			store = inner
		}
		return nil
	}); err != nil {
		// TODO: update this
		t.Fatal(err)
	}
	storeID, nodeID := store.Ident.StoreID, store.Ident.NodeID

	// TODO: Figure out: is this necessary?
	//if err := store.DB().CPut(
	//	ctx, keys.RangeMetaKey(desc.EndKey).AsRawKey(), &desc, exp,
	//); err != nil {
	//	t.Fatal(err)
	//}

	_, err = svr.Node().(*server.Node).UnsafeHealRange(
		ctx,
		&roachpb.UnsafeHealRangeRequest{Desc: desc, NodeID: int32(nodeID), StoreID: int32(storeID)},
	)
	require.NoError(t, err)
	log.Info(ctx, "snapshot applied")

	require.Error(t, svr.DB().Put(cCtx, k, "baz"))
}
