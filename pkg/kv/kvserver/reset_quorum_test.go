// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestResetQuorum tests the ResetQuorum method in various scenarios. We do this
// by starting a five-node cluster with a scratch range replicated to all nodes
// but n1, with n5 as a non-voting replica. We then shut down nodes as
// necessary, to simulate partial or total data loss, check to see if the range
// is unavailable, and finally attempt to use ResetQuorum against it.
//
// We cover resetting quorum for:
// 1. A range with no remaining replicas.
// 2. A range with the only replica on the node being addressed. We
// expect the existing replica to be nuked away and to end up with an
// empty slate.
// 3. A range with replicas on nodes other than the one being addressed.
// We expect existing replicas to be nuked away and to end up with an
// empty slate, except for the non-voting replica.
// 4. A range with a leaseholder replica on a node other than the one being
// addressed. We expect it to be nuked, except for the non-voting replica.
// 5. A range that has not lost quorum (error expected).
// 6. A meta range (error expected).
func TestResetQuorum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStress(t, "too many nodes")
	skip.UnderRace(t, "takes >1m under race")
	skip.UnderShort(t)

	ctx := context.Background()
	livenessDuration := 3000 * time.Millisecond

	// This function sets up a test cluster with 5 nodes with a scratch
	// range on n2, n3, and n4 with n5 as a non-voting replica.
	setup := func(
		t *testing.T,
	) (tc *testcluster.TestCluster, k roachpb.Key, id roachpb.RangeID) {
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
		tc = testcluster.StartTestCluster(t, 5, clusterArgs)

		n1, n2, n3, n4, n5 := 0, 1, 2, 3, 4

		// Set up a scratch range isolated to n2, n3, and n4, with a non-voter on n5.
		k = tc.ScratchRange(t)
		_, err := tc.AddVoters(k, tc.Target(n2))
		require.NoError(t, err)
		_, err = tc.AddVoters(k, tc.Target(n3))
		require.NoError(t, err)
		desc, err := tc.AddVoters(k, tc.Target(n4))
		require.NoError(t, err)
		_, err = tc.AddNonVoters(k, tc.Target(n5))
		require.NoError(t, err)
		require.NoError(t, tc.TransferRangeLease(desc, tc.Target(n2)))
		desc, err = tc.RemoveVoters(k, tc.Target(n1))
		require.NoError(t, err)
		require.Len(t, desc.Replicas().Descriptors(), 4)

		srv := tc.Server(n1)

		require.NoError(t, srv.DB().Put(ctx, k, "original"))

		metaKey := keys.RangeMetaKey(desc.EndKey).AsRawKey()
		// Read the meta2 which ResetQuorum updates while the range
		// is still available. This avoids an intent from the previous
		// replication change to linger; such an intent would be anchored
		// on the scratch range, which is unavailable when quorum is lost.
		_, err = srv.DB().Get(ctx, metaKey)
		require.NoError(t, err)

		return tc, k, desc.RangeID
	}

	// This function verifies a range is unavailable by checking
	// that requests to a key in the range fail.
	checkUnavailable := func(srv serverutils.TestServerInterface, k roachpb.Key) {
		cCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		err := srv.DB().Put(cCtx, k, "unavailable")
		// NB: we don't assert on the exact error since our RPC layer
		// tries to return a better error than DeadlineExceeded (at
		// the time of writing, we get a connection failure to n2),
		// and we don't wait for the context to even time out.
		// We're probably checking in the RPC layer whether we've
		// retried with an up-to-date desc and fail fast if so.
		require.Error(t, err)
	}

	// This function verifies quorum for rangeID was successfully
	// reset by getting the range descriptor from meta2 and
	// verifying that the given node is the only replica.
	verifyResult := func(
		t *testing.T, srv serverutils.TestServerInterface, k roachpb.Key, rangeID roachpb.RangeID,
	) {
		val, err := srv.DB().Get(ctx, k)
		require.NoError(t, err)
		if val.ValueBytes() != nil {
			t.Fatalf("key not empty, expected to be empty after resetting quorum")
		}

		require.NoError(t, srv.DB().Put(ctx, k, "reset"))

		var updatedDesc roachpb.RangeDescriptor
		require.NoError(t, srv.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			kvs, err := kvclient.ScanMetaKVs(ctx, txn, roachpb.Span{
				Key:    roachpb.KeyMin,
				EndKey: roachpb.KeyMax,
			})
			if err != nil {
				return err
			}

			for i := range kvs {
				if err := kvs[i].Value.GetProto(&updatedDesc); err != nil {
					return err
				}
				if updatedDesc.RangeID == rangeID {
					return nil
				}
			}
			return errors.Errorf("range id %v not found after resetting quorum", rangeID)
		}))
		if len(updatedDesc.Replicas().Descriptors()) != 1 {
			t.Fatalf("found %v replicas found after resetting quorum, expected 1", len(updatedDesc.Replicas().Descriptors()))
		}
		if updatedDesc.Replicas().Descriptors()[0].NodeID != srv.NodeID() {
			t.Fatalf("replica found after resetting quorum is on node id %v, expected node id %v", updatedDesc.Replicas().Descriptors()[0].NodeID, srv.NodeID())
		}
	}

	checkReplicaGC := func(t *testing.T, srv serverutils.TestServerInterface, rangeID roachpb.RangeID) {
		store, err := srv.GetStores().(*kvserver.Stores).GetStore(srv.GetFirstStoreID())
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return store.GetReplicaIfExists(rangeID) == nil
		}, 10*time.Second, 200*time.Millisecond, "old replica not GCed on node %v", srv.NodeID())
	}

	checkConsistency := func(t *testing.T, srvs ...serverutils.TestServerInterface) {
		for _, srv := range srvs {
			store, err := srv.GetStores().(*kvserver.Stores).GetStore(srv.GetFirstStoreID())
			require.NoError(t, err)
			err = store.ForceConsistencyQueueProcess()
			require.NoError(t, err, "consistency check failed on node %v", srv.NodeID())
		}
	}

	t.Run("with-full-quorum-loss", func(t *testing.T) {
		tc, k, id := setup(t)
		defer tc.Stopper().Stop(ctx)
		n1, n2, n3, n4, n5 := 0, 1, 2, 3, 4
		srv := tc.Server(n1)
		tc.StopServer(n2)
		tc.StopServer(n3)
		tc.StopServer(n4)
		tc.StopServer(n5)

		// Wait for n2, n3, n4, and n5 liveness to expire.
		time.Sleep(livenessDuration)
		checkUnavailable(srv, k)

		// Call ResetQuorum to reset quorum on the unhealthy range.
		_, err := srv.Node().(*server.Node).ResetQuorum(ctx, &roachpb.ResetQuorumRequest{
			RangeID: int32(id),
		})
		require.NoError(t, err)
		verifyResult(t, srv, k, id)

		// Run consistency check on survivor.
		checkConsistency(t, srv)
	})

	t.Run("with-replica-on-target", func(t *testing.T) {
		tc, k, id := setup(t)
		defer tc.Stopper().Stop(ctx)
		n2, n3, n4, n5 := 1, 2, 3, 4
		srv := tc.Server(n4)
		tc.StopServer(n2)
		tc.StopServer(n3)
		tc.StopServer(n5)

		// Wait for n2, n3, and n4 liveness to expire.
		time.Sleep(livenessDuration)
		checkUnavailable(srv, k)

		// Call ResetQuorum to reset quorum on the unhealthy range.
		_, err := srv.Node().(*server.Node).ResetQuorum(ctx, &roachpb.ResetQuorumRequest{
			RangeID: int32(id),
		})
		require.NoError(t, err)
		verifyResult(t, srv, k, id)

		// Run consistency check on survivor.
		checkConsistency(t, srv)
	})

	t.Run("with-replicas-elsewhere", func(t *testing.T) {
		tc, k, id := setup(t)
		defer tc.Stopper().Stop(ctx)
		n1, n2, n3, n4, n5 := 0, 1, 2, 3, 4
		srv := tc.Server(n1)
		tc.StopServer(n2)
		tc.StopServer(n3)

		// Wait for n2 and n3 liveness to expire.
		time.Sleep(livenessDuration)
		checkUnavailable(srv, k)

		// Call ResetQuorum to reset quorum on the unhealthy range.
		_, err := srv.Node().(*server.Node).ResetQuorum(ctx, &roachpb.ResetQuorumRequest{
			RangeID: int32(id),
		})
		require.NoError(t, err)
		verifyResult(t, srv, k, id)

		// Check that voting replica on n4 was GCed, and check consistency
		// both of reset (n1), GCed (n4), and possibly stale (n5) replicas.
		checkReplicaGC(t, tc.Server(n4), id)
		checkConsistency(t, srv, tc.Server(n4), tc.Server(n5))

		// Non-voting replica (n5) may or may not have been GCed, so we don't
		// assert anything about it. But in any case, requests via that node
		// should go to the reset leaseholder and return the reset value.
		kv, err := tc.Server(n5).DB().Get(ctx, k)
		require.NoError(t, err)
		require.Equal(t, []byte("reset"), kv.ValueBytes())
	})

	t.Run("with-leaseholder-elsewhere", func(t *testing.T) {
		tc, k, id := setup(t)
		defer tc.Stopper().Stop(ctx)
		n1, n2, n3, n4, n5 := 0, 1, 2, 3, 4
		srv := tc.Server(n1)
		tc.StopServer(n3)
		tc.StopServer(n4)

		// Wait for n3 and n4 liveness to expire.
		time.Sleep(livenessDuration)
		checkUnavailable(srv, k)

		// Call ResetQuorum to reset quorum on the unhealthy range.
		_, err := srv.Node().(*server.Node).ResetQuorum(ctx, &roachpb.ResetQuorumRequest{
			RangeID: int32(id),
		})
		require.NoError(t, err)
		verifyResult(t, srv, k, id)

		// Check that the old leaseholder replica on n2 is GCed, and check
		// consistency both of reset (n1), GCed (n2), and possibly stale (n5)
		// replicas.
		checkReplicaGC(t, tc.Server(n2), id)
		checkConsistency(t, srv, tc.Server(n2), tc.Server(n5))

		// Non-voting replica (n5) may or may not have been GCed, so we don't
		// assert anything about it. But in any case, requests via that node
		// should go to the reset leaseholder and return the reset value.
		kv, err := tc.Server(n5).DB().Get(ctx, k)
		require.NoError(t, err)
		require.Equal(t, []byte("reset"), kv.ValueBytes())
	})

	t.Run("without-quorum-loss", func(t *testing.T) {
		tc, _, id := setup(t)
		defer tc.Stopper().Stop(ctx)
		srv := tc.Server(0)

		// Call ResetQuorum to attempt to reset quorum on a healthy range.
		_, err := srv.Node().(*server.Node).ResetQuorum(ctx, &roachpb.ResetQuorumRequest{
			RangeID: int32(id),
		})
		testutils.IsError(err, "targeted range to recover has not lost quorum.")
	})

	t.Run("with-meta-range", func(t *testing.T) {
		tc, _, _ := setup(t)
		defer tc.Stopper().Stop(ctx)
		srv := tc.Server(0)

		// Call ResetQuorum to attempt to reset quorum on a meta range.
		_, err := srv.Node().(*server.Node).ResetQuorum(ctx, &roachpb.ResetQuorumRequest{
			RangeID: int32(keys.MetaRangesID),
		})
		testutils.IsError(err, "targeted range to recover is a meta1 or meta2 range.")
	})
}
