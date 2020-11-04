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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestResetQuorum tests the ResetQuorum method to restore quorum for a
// range whose replicas are all lost. It starts a cluster with two nodes,
// n1 and n2, with a range isolated to n2. Then, it stops n2 and checks
// that the range is unavailable. Finally, it uses ResetQuorum to make the
// range accessible again and checks that the range is available again.
//
// TODO(thesamhuang): Add additional testing to cover two cases:
// 1. if there's an existing replica and we send the recovery snapshot to
// that replica, we still get the quorum reset and end up with an empty
// range.
// 2. if there's an existing replica and we send the recovery snapshot to
// another node that doesn't have that replica, we also still get the
// reset, and replicaGC removes the original survivor when triggered.
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

	// Get range descriptor from meta2 and verify it was updated correctly.
	var updatedDesc roachpb.RangeDescriptor
	require.NoError(t, store.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		kvs, err := sql.ScanMetaKVs(ctx, txn, roachpb.Span{
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
			if updatedDesc.RangeID == desc.RangeID {
				return nil
			}
		}
		return errors.Errorf("range id %v not found after resetting quorum", desc.RangeID)
	}))
	if len(updatedDesc.Replicas().All()) != 1 {
		t.Fatalf("found %v replicas found after resetting quorum, expected 1", len(updatedDesc.Replicas().All()))
	}
	if updatedDesc.Replicas().All()[0].NodeID != srv.NodeID() {
		t.Fatalf("replica found after resetting quorum is on node id %v, expected node id %v", updatedDesc.Replicas().All()[0].NodeID, srv.NodeID())
	}

}
