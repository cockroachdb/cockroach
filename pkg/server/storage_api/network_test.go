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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestNetworkConnectivity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	numNodes := 3
	testCluster := serverutils.StartNewTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)
	ts := testCluster.Server(0)

	var resp serverpb.NetworkConnectivityResponse
	// Should wait because endpoint relies on Gossip.
	testutils.SucceedsSoon(t, func() error {
		if err := srvtestutils.GetStatusJSONProto(ts, "connectivity", &resp); err != nil {
			return err
		}
		if len(resp.ErrorsByNodeID) > 0 {
			return errors.Errorf("expected no errors but got: %d", len(resp.ErrorsByNodeID))
		}
		if len(resp.Connections) < numNodes {
			return errors.Errorf("expected results from %d nodes but got: %d", numNodes, len(resp.ErrorsByNodeID))
		}
		return nil
	})
	// Test when one node is stopped.
	stoppedNodeID := testCluster.Server(1).NodeID()
	testCluster.Server(1).Stopper().Stop(ctx)

	testutils.SucceedsSoon(t, func() error {
		if err := srvtestutils.GetStatusJSONProto(ts, "connectivity", &resp); err != nil {
			return err
		}
		require.Equal(t, len(resp.Connections), numNodes-1)
		fmt.Printf("got status: %s", resp.Connections[ts.NodeID()].Peers[stoppedNodeID].Status.String())
		if resp.Connections[ts.NodeID()].Peers[stoppedNodeID].Status != serverpb.NetworkConnectivityResponse_ERROR {
			return errors.New("waiting for connection state to be changed.")
		}
		if latency := resp.Connections[ts.NodeID()].Peers[stoppedNodeID].Latency; latency > 0 {
			return errors.Errorf("expected latency to be 0 but got %s", latency.String())
		}
		return nil
	})
}
