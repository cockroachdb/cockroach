package kvserver_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/util/log"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestRecoverRangeWithNoReplicas starts a cluster with two nodes, n1 and n2
// with a range isolated to n2. Then, it stops n2 and checks the range is
// unavailable. Finally, it uses UnsafeHealRange to resuscitate the range and
// checks that it is available again.
func TestRecoverRangeWithNoReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	}
	tc := testcluster.StartTestCluster(t, 2, args)
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	desc, err := tc.AddVoters(k, tc.Target(1))
	require.NoError(t, err)
	require.NoError(t, tc.TransferRangeLease(desc, tc.Target(1)))

	srv := tc.Server(0)
	require.NoError(t, srv.DB().Put(ctx, k, "bar"))

	tc.StopServer(1)

	// Sanity check that requests to the ScratchRange time out.
	cCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	require.Error(t, srv.DB().Put(cCtx, k, "baz"))
	require.Equal(t, context.DeadlineExceeded, cCtx.Err())

	// Get the store on the designated survivor (nodeID 1).
	var store *kvserver.Store
	require.NoError(t, srv.GetStores().(*kvserver.Stores).VisitStores(func(inner *kvserver.Store) error {
		store = inner
		return nil
	}))
	if store == nil {
		log.Fatal(ctx, "could not find store on node 1.")
	}

	storeID, nodeID := store.Ident.StoreID, store.Ident.NodeID

	// Update range descriptor and update meta ranges for the descriptor.
	// Remove dead replica.
	deadReplicas := append([]roachpb.ReplicaDescriptor(nil), desc.Replicas().All()...)
	for _, rd := range deadReplicas {
		log.Info(ctx, fmt.Sprintf("removed %v %v", rd.NodeID, rd.StoreID))
		desc.RemoveReplica(rd.NodeID, rd.StoreID)
	}
	// Add new replica.
	desc.AddReplica(nodeID, storeID, roachpb.VOTER_FULL)
	log.Info(ctx, fmt.Sprintf("added %v %v", nodeID, storeID))
	// Write to meta1 and meta2.
	var b kv.Batch
	require.NoError(t, kvserver.UpdateRangeAddressing(&b, &desc))
	require.NoError(t, srv.DB().NewTxn(ctx, "update range descriptor").Run(ctx, &b))

	// Call UnsafeHealRange to apply a new snapshot to the designated survivor.
	_, err = srv.Node().(*server.Node).UnsafeHealRange(
		ctx,
		&roachpb.UnsafeHealRangeRequest{Desc: desc, NodeID: int32(nodeID), StoreID: int32(storeID)},
	)
	require.NoError(t, err)

	log.Info(ctx, "unsafe heal range request sent")

	require.NoError(t, srv.DB().Put(ctx, k, "baz"))
}
