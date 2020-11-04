package kvserver_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestResetQuorum tests the ResetQuorum method to restore quorum for a
// range whose replicas are all lost. It starts a cluster with two nodes,
// n1 and n2, with a range isolated to n2. Then, it stops n2 and checks
// that the range is unavailable. Finally, it uses ResetQuorum to make the
// range accessible again and checks that the range is available again.
func TestResetQuorum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	livenessDuration := 3000 * time.Millisecond

	clusterArgs := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				NodeLiveness: kvserver.NodeLivenessTestingKnobs{
					LivenessDuration: livenessDuration, // set duration so n2 liveness expires shortly after stopping
					RenewalDuration:  1500 * time.Millisecond,
				},
			},
		},
	}
	tc := testcluster.StartTestCluster(t, 2, clusterArgs)
	defer tc.Stopper().Stop(ctx)

	n1, n2 := 0, 1

	// Set up a scratch range isolated to n2.
	k := tc.ScratchRange(t)
	desc, err := tc.AddVoters(k, tc.Target(n2))
	require.NoError(t, err)
	require.NoError(t, tc.TransferRangeLease(desc, tc.Target(n2)))
	desc, err = tc.RemoveVoters(k, tc.Target(n1))
	require.NoError(t, err)
	require.Len(t, desc.Replicas().All(), 1)

	srv := tc.Server(n1)

	require.NoError(t, srv.DB().Put(ctx, k, "bar"))

	metaKey := keys.RangeMetaKey(desc.EndKey).AsRawKey()
	// Read the meta2 which ResetQuorum will be updating while n2
	// is still available. This avoids an intent from the previous
	// replication change to linger; such an intent would be anchored
	// on the scratch range, which is unavailable when n2 goes down.
	_, err = srv.DB().Get(ctx, metaKey)
	require.NoError(t, err)
	tc.StopServer(n2)
	// Wait for n2 liveness to expire.
	time.Sleep(livenessDuration)

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
		},
	)
	require.NoError(t, err)

	t.Logf("resetting quorum complete")

	require.NoError(t, srv.DB().Put(ctx, k, "baz"))
}
