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
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// TestStatusJson verifies that status endpoints return expected Json results.
// The content type of the responses is always httputil.JSONContentType.
func TestStatusJson(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.(*server.TestServer)

	nodeID := ts.Gossip().NodeID.Get()
	addr, err := ts.Gossip().GetNodeIDAddress(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	sqlAddr, err := ts.Gossip().GetNodeIDSQLAddress(nodeID)
	if err != nil {
		t.Fatal(err)
	}

	var nodes serverpb.NodesResponse
	testutils.SucceedsSoon(t, func() error {
		if err := srvtestutils.GetStatusJSONProto(s, "nodes", &nodes); err != nil {
			t.Fatal(err)
		}

		if len(nodes.Nodes) == 0 {
			return errors.Errorf("expected non-empty node list, got: %v", nodes)
		}
		return nil
	})

	for _, path := range []string{
		apiconstants.StatusPrefix + "details/local",
		apiconstants.StatusPrefix + "details/" + strconv.FormatUint(uint64(nodeID), 10),
	} {
		var details serverpb.DetailsResponse
		if err := serverutils.GetJSONProto(s, path, &details); err != nil {
			t.Fatal(err)
		}
		if a, e := details.NodeID, nodeID; a != e {
			t.Errorf("expected: %d, got: %d", e, a)
		}
		if a, e := details.Address, *addr; a != e {
			t.Errorf("expected: %v, got: %v", e, a)
		}
		if a, e := details.SQLAddress, *sqlAddr; a != e {
			t.Errorf("expected: %v, got: %v", e, a)
		}
		if a, e := details.BuildInfo, build.GetInfo(); a != e {
			t.Errorf("expected: %v, got: %v", e, a)
		}
	}
}

// TestNodeStatusResponse verifies that node status returns the expected
// results.
func TestNodeStatusResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	srv, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.(*server.TestServer)
	node := s.Node().(*server.Node)

	wrapper := serverpb.NodesResponse{}

	// Check that the node statuses cannot be accessed via a non-admin account.
	if err := srvtestutils.GetStatusJSONProtoWithAdminOption(s, "nodes", &wrapper, false /* isAdmin */); !testutils.IsError(err, "status: 403") {
		t.Fatalf("expected privilege error, got %v", err)
	}

	// Now fetch all the node statuses as admin.
	if err := srvtestutils.GetStatusJSONProto(s, "nodes", &wrapper); err != nil {
		t.Fatal(err)
	}
	nodeStatuses := wrapper.Nodes

	if len(nodeStatuses) != 1 {
		t.Errorf("too many node statuses returned - expected:1 actual:%d", len(nodeStatuses))
	}
	if !node.Descriptor.Equal(&nodeStatuses[0].Desc) {
		t.Errorf("node status descriptors are not equal\nexpected:%+v\nactual:%+v\n", node.Descriptor, nodeStatuses[0].Desc)
	}

	// Now fetch each one individually. Loop through the nodeStatuses to use the
	// ids only.
	for _, oldNodeStatus := range nodeStatuses {
		nodeStatus := statuspb.NodeStatus{}
		nodeURL := "nodes/" + oldNodeStatus.Desc.NodeID.String()
		// Check that the node statuses cannot be accessed via a non-admin account.
		if err := srvtestutils.GetStatusJSONProtoWithAdminOption(s, nodeURL, &nodeStatus, false /* isAdmin */); !testutils.IsError(err, "status: 403") {
			t.Fatalf("expected privilege error, got %v", err)
		}

		// Now access that node's status.
		if err := srvtestutils.GetStatusJSONProto(s, nodeURL, &nodeStatus); err != nil {
			t.Fatal(err)
		}
		if !node.Descriptor.Equal(&nodeStatus.Desc) {
			t.Errorf("node status descriptors are not equal\nexpected:%+v\nactual:%+v\n", node.Descriptor, nodeStatus.Desc)
		}
	}
}

// TestMetricsRecording verifies that Node statistics are periodically recorded
// as time series data.
func TestMetricsRecording(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Verify that metrics for the current timestamp are recorded. This should
	// be true very quickly even though DefaultMetricsSampleInterval is large,
	// because the server writes an entry eagerly on startup.
	testutils.SucceedsSoon(t, func() error {
		now := s.Clock().PhysicalNow()

		var data roachpb.InternalTimeSeriesData
		for _, keyName := range []string{
			"cr.store.livebytes.1",
			"cr.node.sys.go.allocbytes.1",
		} {
			key := ts.MakeDataKey(keyName, "", ts.Resolution10s, now)
			if err := kvDB.GetProto(ctx, key, &data); err != nil {
				return err
			}
		}
		return nil
	})
}

// TestMetricsEndpoint retrieves the metrics endpoint, which is currently only
// used for development purposes. The metrics within the response are verified
// in other tests.
func TestMetricsEndpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	srv, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())

	s := srv.(*server.TestServer)

	if _, err := srvtestutils.GetText(s, s.AdminURL().WithPath(apiconstants.StatusPrefix+"metrics/"+s.Gossip().NodeID.String()).String()); err != nil {
		t.Fatal(err)
	}
}

func TestNodesGRPCResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.(*server.TestServer)

	rootConfig := testutils.NewTestBaseContext(username.RootUserName())
	rpcContext := srvtestutils.NewRPCTestContext(context.Background(), ts, rootConfig)
	var request serverpb.NodesRequest

	url := ts.ServingRPCAddr()
	nodeID := ts.NodeID()
	conn, err := rpcContext.GRPCDialNode(url, nodeID, rpc.DefaultClass).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := serverpb.NewStatusClient(conn)

	response, err := client.Nodes(context.Background(), &request)
	if err != nil {
		t.Fatal(err)
	}

	if a, e := len(response.Nodes), 1; a != e {
		t.Errorf("expected %d node(s), found %d", e, a)
	}
}
