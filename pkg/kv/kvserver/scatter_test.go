// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func adminScatterArgs(key roachpb.Key, randomizeLeases bool) *kvpb.AdminScatterRequest {
	return &kvpb.AdminScatterRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    key,
			EndKey: key.Next(),
		},
		RandomizeLeases: randomizeLeases,
	}
}

// TestAdminScatterWithDrainingNodes tests that when `AdminScatter` is called
// with the `RandomizeLeases` option, it does not transfer leases to draining
// nodes.
func TestAdminScatterWithDrainingNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// We set up a scratch range on 2 nodes, and then drain the second node.
	const drainingServerIdx = 1
	const drainingNodeID = drainingServerIdx + 1
	tc := testcluster.StartTestCluster(
		t, 2, base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		},
	)
	defer tc.Stopper().Stop(ctx)
	scratchKey := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, scratchKey, tc.Target(drainingServerIdx))

	client := tc.GetAdminClient(t, drainingServerIdx)
	drain(ctx, t, client, drainingNodeID)

	nonDrainingStore := tc.GetFirstStoreFromServer(t, 0)
	drainingStore := tc.GetFirstStoreFromServer(t, drainingServerIdx)

	// Wait until the non-draining node is aware of the draining node.
	testutils.SucceedsSoon(t, tc.Servers[drainingServerIdx].HeartbeatNodeLiveness)
	testutils.SucceedsSoon(t, func() error {
		isDraining, err := nonDrainingStore.GetStoreConfig().StorePool.IsDraining(drainingStore.StoreID())
		if err != nil {
			return err
		}
		if !isDraining {
			return errors.Newf("expected s%d to be in draining state", drainingStore.StoreID())
		}
		return nil
	})

	// Now, we repeatedly call `AdminScatter` with the `RandomizeLeases` option on
	// this scratch range, and ensure that the lease is never transferred to the
	// draining node.
	const numIterations = 50
	for i := 0; i < numIterations; i++ {
		_, pErr := kv.SendWrapped(
			ctx, nonDrainingStore.TestSender(), adminScatterArgs(scratchKey, true /* randomizeLeases */),
		)
		require.Nil(t, pErr)
		leaseHolder, err := tc.FindRangeLeaseHolder(tc.LookupRangeOrFatal(t, scratchKey), nil /* hint */)
		require.NoError(t, err)
		require.NotEqual(t, drainingStore.StoreID(), leaseHolder.StoreID)
	}
}
