package kvserver_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestRecoverRangeWithNoReplicas tests the UnsafeHealRange method to
// recover a range whose replicas are all lost. It starts a cluster with
// two nodes, n1 and n2 with a range isolated to n2. Then, it stops n2
// and checks the range is unavailable. Next, it updates the meta2 range
// descriptor entry and uses UnsafeHealRange to resuscitate the range.
// Finally, it checks that the range is available again.
//func TestRecoverRangeWithNoReplicas(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//	ctx := context.Background()
//
//	args := base.TestClusterArgs{
//		ReplicationMode: base.ReplicationManual,
//	}
//	tc := testcluster.StartTestCluster(t, 2, args)
//	defer tc.Stopper().Stop(ctx)
//
//	// Set up a scratch range isolated to n2.
//	k := tc.ScratchRange(t)
//	desc, err := tc.AddVoters(k, tc.Target(1))
//	require.NoError(t, err)
//	require.NoError(t, tc.TransferRangeLease(desc, tc.Target(1)))
//	desc, err = tc.RemoveVoters(k, tc.Target(0))
//	require.NoError(t, err)
//	require.Len(t, desc.Replicas().All(), 1)
//
//	srv := tc.Server(0)
//	require.NoError(t, srv.DB().Put(ctx, k, "bar"))
//
//	metaKey := keys.RangeMetaKey(desc.EndKey).AsRawKey()
//	// Read the meta2 which we are about to update below while n2
//	// is still available. This avoids an intent from the previous
//	// replication change to linger; such an intent would be anchored
//	// on the scratch range, which is unavailable when n2 goes down.
//	_, err = srv.DB().Get(ctx, metaKey)
//	require.NoError(t, err)
//	tc.StopServer(1)
//
//	// Sanity check that requests to the ScratchRange fail.
//	func() {
//		cCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
//		defer cancel()
//		err := srv.DB().Put(cCtx, k, "baz")
//		// NB: we don't assert on the exact error since our RPC layer
//		// tries to return a better error than DeadlineExceeded (at
//		// the time of writing, we get a connection failure to n2),
//		// and we don't wait for the context to even time out.
//		// We're probably checking in the RPC layer whether we've
//		// retried with an up-to-date desc and fail fast if so.
//		require.Error(t, err)
//	}()
//
//	// Get the store on the designated survivor n1.
//	var store *kvserver.Store
//	require.NoError(t, srv.GetStores().(*kvserver.Stores).VisitStores(func(inner *kvserver.Store) error {
//		store = inner
//		return nil
//	}))
//	if store == nil {
//		t.Fatal("no store found on n1")
//	}
//
//	storeID, nodeID := store.Ident.StoreID, store.Ident.NodeID
//
//	// Update range descriptor and update meta ranges for the descriptor.
//	// Remove dead replica.
//	deadReplicas := append([]roachpb.ReplicaDescriptor(nil), desc.Replicas().All()...)
//	for _, rd := range deadReplicas {
//		desc.RemoveReplica(rd.NodeID, rd.StoreID)
//	}
//	// Add new replica.
//	desc.AddReplica(nodeID, storeID, roachpb.VOTER_FULL)
//	// We should increment the generation so that the various
//	// caches will recognize this descriptor as newer. This
//	// hasn't seemed to be necessary in this test, but it
//	// can't hurt either.
//	desc.IncrementGeneration()
//
//	t.Logf("starting recovery using desc %+v", desc)
//	// Update the meta2 entry. Note that we're intentionally
//	// eschewing updateRangeAddressing since the copy of the
//	// descriptor that resides on the range itself has lost quorum.
//	require.NoError(t, srv.DB().Put(ctx, metaKey, &desc))
//
//	// Call UnsafeHealRange to send a snapshot to the designated survivor.
//	t.Logf("sending snapshot")
//	err = srv.Node().(*server.Node).unsafeHealRange(
//		ctx,
//		desc, nodeID, storeID,
//	)
//	require.NoError(t, err)
//	require.NoError(t, srv.DB().Put(ctx, k, "baz"))
//}

func TestResetQuorum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	}
	tc := testcluster.StartTestCluster(t, 2, args)
	defer tc.Stopper().Stop(ctx)

	// Set up a scratch range isolated to n2.
	k := tc.ScratchRange(t)
	desc, err := tc.AddVoters(k, tc.Target(1))
	require.NoError(t, err)
	require.NoError(t, tc.TransferRangeLease(desc, tc.Target(1)))
	desc, err = tc.RemoveVoters(k, tc.Target(0))
	require.NoError(t, err)
	require.Len(t, desc.Replicas().All(), 1)

	srv := tc.Server(0)
	require.NoError(t, srv.DB().Put(ctx, k, "bar"))

	metaKey := keys.RangeMetaKey(desc.EndKey).AsRawKey()
	// Read the meta2 which we are about to update below while n2
	// is still available. This avoids an intent from the previous
	// replication change to linger; such an intent would be anchored
	// on the scratch range, which is unavailable when n2 goes down.
	_, err = srv.DB().Get(ctx, metaKey)
	require.NoError(t, err)
	tc.StopServer(1)

	// Sanity check that requests to the ScratchRange fail.
	func() {
		cCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		err := srv.DB().Put(cCtx, k, "baz")
		// NB: we don't assert on the exact error since our RPC layer
		// tries to return a better error than DeadlineExceeded (at
		// the time of writing, we get a connection failure to n2),
		// and we don't wait for the context to even time out.
		// We're probably checking in the RPC layer whether we've
		// retried with an up-to-date desc and fail fast if so.
		require.Error(t, err)
	}()

	// Get the store on the designated survivor n1.
	var store *kvserver.Store
	require.NoError(t, srv.GetStores().(*kvserver.Stores).VisitStores(func(inner *kvserver.Store) error {
		store = inner
		return nil
	}))
	if store == nil {
		t.Fatal("no store found on n1")
	}

	// Call ResetQuorum to reset quorum on the unhealthy range.
	t.Logf("resetting quorum on node id: %v, store id: %v", store.NodeID(), store.StoreID())
	_, err = srv.Node().(*server.Node).ResetQuorum(
		ctx,
		&roachpb.ResetQuorumRequest{
			RangeID: int32(desc.RangeID),
			StoreID: int32(store.StoreID()),
		},
	)
	require.NoError(t, err)

	t.Logf("resetting quorum complete")

	require.NoError(t, srv.DB().Put(ctx, k, "baz"))
}
