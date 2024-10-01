// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replicaoracle

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

// TestRandomOracle defeats TestUnused for RandomChoice.
func TestRandomOracle(t *testing.T) {
	_ = NewOracle(RandomChoice, Config{})
}

func TestClosest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.RunTrueAndFalse(t, "valid-latency-func", func(t *testing.T, validLatencyFunc bool) {
		ctx := context.Background()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		g, _ := makeGossip(t, stopper, []int{2, 3})
		nd2, err := g.GetNodeDescriptor(2)
		require.NoError(t, err)
		o := NewOracle(ClosestChoice, Config{
			NodeDescs:  g,
			NodeID:     1,
			Locality:   nd2.Locality, // pretend node 2 is closest.
			Settings:   cluster.MakeTestingClusterSettings(),
			HealthFunc: func(_ roachpb.NodeID) bool { return true },
			LatencyFunc: func(id roachpb.NodeID) (time.Duration, bool) {
				if id == 2 {
					return time.Nanosecond, validLatencyFunc
				}
				return time.Millisecond, validLatencyFunc
			},
		})
		internalReplicas := []roachpb.ReplicaDescriptor{
			{NodeID: 4, StoreID: 4},
			{NodeID: 2, StoreID: 2},
			{NodeID: 3, StoreID: 3},
		}
		rand.Shuffle(len(internalReplicas), func(i, j int) {
			internalReplicas[i], internalReplicas[j] = internalReplicas[j], internalReplicas[i]
		})
		info, ignoreMisplannedRanges, err := o.ChoosePreferredReplica(
			ctx,
			nil, /* txn */
			&roachpb.RangeDescriptor{
				InternalReplicas: internalReplicas,
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
		if ignoreMisplannedRanges {
			t.Fatalf("Expected ignoreMisplannedRanges to be false since nil leaseholder was provided")
		}
	})
}

func makeGossip(t *testing.T, stopper *stop.Stopper, nodeIDs []int) (*gossip.Gossip, *hlc.Clock) {
	clock := hlc.NewClockForTesting(nil)

	const nodeID = 1
	g := gossip.NewTest(nodeID, stopper, metric.NewRegistry())
	if err := g.SetNodeDescriptor(newNodeDesc(nodeID)); err != nil {
		t.Fatal(err)
	}
	if err := g.AddInfo(gossip.KeySentinel, nil, time.Hour); err != nil {
		t.Fatal(err)
	}
	for _, id := range nodeIDs {
		i := roachpb.NodeID(id)
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
		Locality: roachpb.Locality{
			Tiers: []roachpb.Tier{
				{
					Key:   "region",
					Value: fmt.Sprintf("region_%d", nodeID),
				},
			},
		},
	}
}

func TestPreferFollower(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	g, _ := makeGossip(t, stopper, []int{2, 3, 4, 5, 6})
	o := NewOracle(PreferFollowerChoice, Config{
		NodeDescs: g,
	})
	internalReplicas := []roachpb.ReplicaDescriptor{
		{ReplicaID: 2, NodeID: 2, StoreID: 2, Type: roachpb.VOTER_FULL},
		{ReplicaID: 3, NodeID: 3, StoreID: 3, Type: roachpb.VOTER_FULL},
		{ReplicaID: 4, NodeID: 4, StoreID: 4, Type: roachpb.VOTER_FULL},
		{ReplicaID: 5, NodeID: 5, StoreID: 5, Type: roachpb.NON_VOTER},
		{ReplicaID: 6, NodeID: 6, StoreID: 6, Type: roachpb.NON_VOTER},
	}
	rand.Shuffle(len(internalReplicas), func(i, j int) {
		internalReplicas[i], internalReplicas[j] = internalReplicas[j], internalReplicas[i]
	})
	info, _, err := o.ChoosePreferredReplica(
		ctx,
		nil, /* txn */
		&roachpb.RangeDescriptor{
			InternalReplicas: internalReplicas,
		},
		nil, /* leaseHolder */
		roachpb.LAG_BY_CLUSTER_SETTING,
		QueryState{},
	)
	if err != nil {
		t.Fatalf("Failed to choose follower replica: %v", err)
	}

	fullVoters := make(map[roachpb.NodeID]bool)
	for _, r := range internalReplicas {
		if r.Type == roachpb.VOTER_FULL {
			fullVoters[r.NodeID] = true
		}
	}

	if fullVoters[info.NodeID] {
		t.Fatalf("Chose a VOTER_FULL replica: %d", info.NodeID)
	}
}
