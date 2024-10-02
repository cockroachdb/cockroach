// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage_api_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestRangesResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer kvserver.EnableLeaseHistoryForTesting(100)()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(110019),
	})
	defer srv.Stopper().Stop(context.Background())
	ts := srv.ApplicationLayer()

	// TODO(#110019): grant a special capability to the secondary tenant
	// before the endpoint can be accessed.

	t.Run("test ranges response", func(t *testing.T) {
		// Perform a scan to ensure that all the raft groups are initialized.
		if _, err := srv.SystemLayer().DB().Scan(context.Background(), keys.LocalMax, roachpb.KeyMax, 0); err != nil {
			t.Fatal(err)
		}

		var response serverpb.RangesResponse
		if err := srvtestutils.GetStatusJSONProto(ts, "ranges/local", &response); err != nil {
			t.Fatal(err)
		}
		if len(response.Ranges) == 0 {
			t.Errorf("didn't get any ranges")
		}
		for _, ri := range response.Ranges {
			// Do some simple validation based on the fact that this is a
			// single-node cluster.
			if ri.RaftState.State != "StateLeader" && ri.RaftState.State != server.RaftStateDormant {
				t.Errorf("expected to be Raft leader or dormant, but was '%s'", ri.RaftState.State)
			}
			expReplica := roachpb.ReplicaDescriptor{
				NodeID:    1,
				StoreID:   1,
				ReplicaID: 1,
			}
			if len(ri.State.Desc.InternalReplicas) != 1 || ri.State.Desc.InternalReplicas[0] != expReplica {
				t.Errorf("unexpected replica list %+v", ri.State.Desc.InternalReplicas)
			}
			if ri.State.Lease == nil || ri.State.Lease.Empty() {
				t.Error("expected a nontrivial Lease")
			}
			if ri.State.LastIndex == 0 {
				t.Error("expected positive LastIndex")
			}
			if len(ri.LeaseHistory) == 0 {
				t.Error("expected at least one lease history entry")
			}
		}
	})

	t.Run("test ranges pagination", func(t *testing.T) {
		ctx := context.Background()
		rpcStopper := stop.NewStopper()
		defer rpcStopper.Stop(ctx)

		client := ts.GetStatusClient(t)
		resp1, err := client.Ranges(ctx, &serverpb.RangesRequest{
			Limit: 1,
		})
		require.NoError(t, err)
		require.Len(t, resp1.Ranges, 1)
		require.Equal(t, int(resp1.Next), 1)

		resp2, err := client.Ranges(ctx, &serverpb.RangesRequest{
			Limit:  1,
			Offset: resp1.Next,
		})
		require.NoError(t, err)
		require.Len(t, resp2.Ranges, 1)
		require.Equal(t, int(resp2.Next), 2)

		// Verify pagination functions based on ascending RangeID order.
		require.True(t, resp1.Ranges[0].State.Desc.RangeID < resp2.Ranges[0].State.Desc.RangeID)
	})
}

// TestTenantRangesSecurity checks that the storage layer properly zeroes out
// if it's missing a tenant ID in the TenantRanges back-end RPC.
func TestTenantRangesSecurity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)
	ts := srv.SystemLayer()

	t.Run("returns error when TenantID not set in ctx", func(t *testing.T) {
		rpcStopper := stop.NewStopper()
		defer rpcStopper.Stop(ctx)

		client := ts.GetStatusClient(t)
		_, err := client.TenantRanges(ctx, &serverpb.TenantRangesRequest{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "no tenant ID found in context")
	})
}

func TestRangeResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer kvserver.EnableLeaseHistoryForTesting(100)()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(110018),
	})
	defer srv.Stopper().Stop(context.Background())
	ts := srv.ApplicationLayer()

	// Perform a scan to ensure that all the raft groups are initialized.
	if _, err := srv.SystemLayer().DB().Scan(context.Background(), keys.LocalMax, roachpb.KeyMax, 0); err != nil {
		t.Fatal(err)
	}

	// TODO(#110018): grant a special capability to the secondary tenant
	// before the endpoint can be accessed.

	var response serverpb.RangeResponse
	if err := srvtestutils.GetStatusJSONProto(ts, "range/1", &response); err != nil {
		t.Fatal(err)
	}

	// This is a single node cluster, so only expect a single response.
	if e, a := 1, len(response.ResponsesByNodeID); e != a {
		t.Errorf("got the wrong number of responses, expected %d, actual %d", e, a)
	}

	node1Response := response.ResponsesByNodeID[response.NodeID]

	// The response should come back as valid.
	if !node1Response.Response {
		t.Errorf("node1's response returned as false, expected true")
	}

	// The response should include just the one range.
	if e, a := 1, len(node1Response.Infos); e != a {
		t.Fatalf("got the wrong number of ranges in the response, expected %d, actual %d", e, a)
	}

	info := node1Response.Infos[0]
	expReplica := roachpb.ReplicaDescriptor{
		NodeID:    1,
		StoreID:   1,
		ReplicaID: 1,
	}

	// Check some other values.
	if len(info.State.Desc.InternalReplicas) != 1 || info.State.Desc.InternalReplicas[0] != expReplica {
		t.Errorf("unexpected replica list %+v", info.State.Desc.InternalReplicas)
	}

	if info.State.Lease == nil || info.State.Lease.Empty() {
		t.Error("expected a nontrivial Lease")
	}

	if info.State.LastIndex == 0 {
		t.Error("expected positive LastIndex")
	}

	if len(info.LeaseHistory) == 0 {
		t.Error("expected at least one lease history entry")
	}
}
