// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package replicaoracle

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TestRandomOracle defeats TestUnused for RandomChoice.
func TestRandomOracle(t *testing.T) {
	_ = NewOracle(RandomChoice, Config{})
}

func TestClosest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	g, _ := makeGossip(t, stopper)
	nd, _ := g.GetNodeDescriptor(1)
	o := NewOracle(ClosestChoice, Config{
		NodeDescs: g,
		NodeDesc:  *nd,
	})
	o.(*closestOracle).latencyFunc = func(s string) (time.Duration, bool) {
		if strings.HasSuffix(s, "2") {
			return time.Nanosecond, true
		}
		return time.Millisecond, true
	}
	info, err := o.ChoosePreferredReplica(
		ctx,
		nil, /* txn */
		&roachpb.RangeDescriptor{
			InternalReplicas: []roachpb.ReplicaDescriptor{
				{NodeID: 4, StoreID: 4},
				{NodeID: 2, StoreID: 2},
				{NodeID: 3, StoreID: 3},
			},
		},
		nil, /* leaseHolder */
		roachpb.LAG_BY_CLUSTER_SETTING,
		QueryState{},
	)
	if err != nil {
		t.Fatalf("Failed to choose closest replica: %v", err)
	}
	if info.NodeID != 2 {
		t.Fatalf("Failed to choose node 2, got %v", info.NodeID)
	}
}

func makeGossip(t *testing.T, stopper *stop.Stopper) (*gossip.Gossip, *hlc.Clock) {
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	server := rpc.NewServer(rpcContext)

	const nodeID = 1
	g := gossip.NewTest(nodeID, rpcContext, server, stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())
	if err := g.SetNodeDescriptor(newNodeDesc(nodeID)); err != nil {
		t.Fatal(err)
	}
	if err := g.AddInfo(gossip.KeySentinel, nil, time.Hour); err != nil {
		t.Fatal(err)
	}
	for i := roachpb.NodeID(2); i <= 3; i++ {
		err := g.AddInfoProto(gossip.MakeNodeIDKey(i), newNodeDesc(i), gossip.NodeDescriptorTTL)
		if err != nil {
			t.Fatal(err)
		}
	}
	return g, clock
}

func newNodeDesc(nodeID roachpb.NodeID) *roachpb.NodeDescriptor {
	return &roachpb.NodeDescriptor{
		NodeID:  nodeID,
		Address: util.MakeUnresolvedAddr("tcp", fmt.Sprintf("invalid.invalid:%d", nodeID)),
	}
}
