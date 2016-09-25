// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer@cockroachlabs.com)

package storage_test

import (
	"testing"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/netutil"
)

func verifyLiveness(t *testing.T, mtc *multiTestContext) {
	util.SucceedsSoon(t, func() error {
		for _, nl := range mtc.nodeLivenesses {
			for _, g := range mtc.gossips {
				live, err := nl.IsLive(g.GetNodeID())
				if !live {
					return errors.Errorf("node %d not live", g.GetNodeID())
				} else if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func TestNodeLiveness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, mtc)

	// Advance clock past the liveness threshold to verify IsLive becomes false.
	mtc.manualClock.Increment(storage.LivenessThreshold.Nanoseconds() + 1)
	for idx, nl := range mtc.nodeLivenesses {
		nodeID := mtc.gossips[idx].GetNodeID()
		live, err := nl.IsLive(nodeID)
		if live {
			t.Errorf("expected node %d to be considered not-live after advancing node clock", nodeID)
		} else if err != nil {
			t.Error(err)
		}
	}
	// Trigger a manual heartbeat and verify liveness is reestablished.
	for _, nl := range mtc.nodeLivenesses {
		nl.ManualHeartbeat()
	}
	verifyLiveness(t, mtc)
}

// TestNodeLivenessIncrement verifies that incrementing the epoch of a
// node requires the node to be considered not-live and that on
// increment, no other nodes believe the epoch-incremented node to be
// live (that is, its received timestamp is not advanced.
func TestNodeLivenessIncrement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 2)
	defer mtc.Stop()
	verifyLiveness(t, mtc)

	// First try to increment the epoch of a known-live node.
	deadNodeID := mtc.gossips[1].GetNodeID()
	if err := mtc.nodeLivenesses[0].IncrementEpoch(context.Background(), deadNodeID); err == nil {
		t.Fatalf("expected error incrementing a live node")
	}

	// Advance clock past liveness threshold & increment epoch.
	oldLiveness, err := mtc.nodeLivenesses[0].GetLiveness(deadNodeID)
	if err != nil {
		t.Fatal(err)
	}
	mtc.manualClock.Increment(storage.LivenessThreshold.Nanoseconds() + 1)
	if err := mtc.nodeLivenesses[0].IncrementEpoch(context.Background(), deadNodeID); err != nil {
		t.Fatalf("unexpected error incrementing a live node: %s", err)
	}

	// Verify that the epoch has been advanced
	util.SucceedsSoon(t, func() error {
		newLiveness, err := mtc.nodeLivenesses[0].GetLiveness(deadNodeID)
		if err != nil {
			return err
		}
		if newLiveness.Epoch != oldLiveness.Epoch+1 {
			return errors.Errorf("expected epoch to increment")
		}
		if newLiveness.LastHeartbeat != oldLiveness.LastHeartbeat {
			return errors.Errorf("expected last heartbeat to remain unchanged")
		}
		if live, err := mtc.nodeLivenesses[0].IsLive(deadNodeID); live || err != nil {
			return errors.Errorf("expected dead node to remain dead after epoch increment: %s", err)
		}
		return nil
	})
}

// TestNodeLivenessRestart verifies that if nodes are shutdown and
// restarted, the node liveness records are re-gossiped immediately.
func TestNodeLivenessRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 1)
	defer mtc.Stop()

	// After verifying node is in liveness table, stop store.
	verifyLiveness(t, mtc)
	mtc.stopStore(0)

	// Create a new gossip instance and connect it.
	grpcServer := rpc.NewServer(mtc.rpcContext)
	ln, err := netutil.ListenAndServeGRPC(mtc.transportStopper, grpcServer, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := mtc.getNodeIDAddress(mtc.gossips[0].GetNodeID())
	if err != nil {
		t.Fatal(err)
	}
	r, err := resolver.NewResolverFromAddress(addr)
	if err != nil {
		t.Fatal(err)
	}
	g := gossip.New(context.TODO(), mtc.rpcContext, grpcServer,
		[]resolver.Resolver{r}, mtc.transportStopper, metric.NewRegistry())
	g.SetNodeID(roachpb.NodeID(2))
	g.Start(ln.Addr())

	// Clear the liveness record in store 1's gossip to make sure we're
	// seeing the liveness record properly gossiped at store startup.
	key := gossip.MakeNodeLivenessKey(1)
	if err := mtc.gossips[0].AddInfoProto(key, &storage.Liveness{}, 0); err != nil {
		t.Fatal(err)
	}

	// Restart store and verify gossip contains liveness record for node ID 1.
	mtc.restartStore(0)
	util.SucceedsSoon(t, func() error {
		var liveness storage.Liveness
		if err := g.GetInfoProto(key, &liveness); err != nil {
			return err
		}
		if liveness.Epoch == 0 {
			return errors.Errorf("expected non-empty liveness record")
		}
		return nil
	})
}
