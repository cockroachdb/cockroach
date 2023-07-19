// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage_api_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	ts := s.(*server.TestServer)

	t.Run("test ranges response", func(t *testing.T) {
		// Perform a scan to ensure that all the raft groups are initialized.
		if _, err := ts.DB().Scan(context.Background(), keys.LocalMax, roachpb.KeyMax, 0); err != nil {
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

		conn, err := ts.RPCContext().GRPCDialNode(ts.ServingRPCAddr(), ts.NodeID(), rpc.DefaultClass).Connect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		client := serverpb.NewStatusClient(conn)
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

func TestTenantRangesResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	ts := s.(*server.TestServer)

	t.Run("returns error when TenantID not set in ctx", func(t *testing.T) {
		rpcStopper := stop.NewStopper()
		defer rpcStopper.Stop(ctx)

		conn, err := ts.RPCContext().GRPCDialNode(ts.ServingRPCAddr(), ts.NodeID(), rpc.DefaultClass).Connect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		client := serverpb.NewStatusClient(conn)
		_, err = client.TenantRanges(ctx, &serverpb.TenantRangesRequest{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "no tenant ID found in context")
	})
}

func TestRangeResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer kvserver.EnableLeaseHistoryForTesting(100)()
	ts, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer ts.Stopper().Stop(context.Background())

	// Perform a scan to ensure that all the raft groups are initialized.
	if _, err := ts.DB().Scan(context.Background(), keys.LocalMax, roachpb.KeyMax, 0); err != nil {
		t.Fatal(err)
	}

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
		t.Errorf("got the wrong number of ranges in the response, expected %d, actual %d", e, a)
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
