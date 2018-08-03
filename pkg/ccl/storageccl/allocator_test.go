// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

func TestAllocatorLeasePreferences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, a, _ := createTestAllocator(true /* deterministic */)
	defer stopper.Stop(context.Background())

	// 4 stores with distinct localities, store attributes, and node attributes
	// where the lease count for each store is equal to 100x the store ID.
	var stores []*roachpb.StoreDescriptor
	for i := 1; i <= 4; i++ {
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID: roachpb.StoreID(i),
			Attrs:   roachpb.Attributes{Attrs: []string{fmt.Sprintf("s%d", i)}},
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(i),
				Attrs:  roachpb.Attributes{Attrs: []string{fmt.Sprintf("n%d", i)}},
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "dc", Value: strconv.Itoa(i)},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{LeaseCount: int32(100 * i)},
		})
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	preferDC1 := []config.LeasePreference{
		{Constraints: []config.Constraint{{Key: "dc", Value: "1", Type: config.Constraint_REQUIRED}}},
	}
	preferDC4Then3Then2 := []config.LeasePreference{
		{Constraints: []config.Constraint{{Key: "dc", Value: "4", Type: config.Constraint_REQUIRED}}},
		{Constraints: []config.Constraint{{Key: "dc", Value: "3", Type: config.Constraint_REQUIRED}}},
		{Constraints: []config.Constraint{{Key: "dc", Value: "2", Type: config.Constraint_REQUIRED}}},
	}
	preferN2ThenS3 := []config.LeasePreference{
		{Constraints: []config.Constraint{{Value: "n2", Type: config.Constraint_REQUIRED}}},
		{Constraints: []config.Constraint{{Value: "s3", Type: config.Constraint_REQUIRED}}},
	}
	preferNotS1ThenNotN2 := []config.LeasePreference{
		{Constraints: []config.Constraint{{Value: "s1", Type: config.Constraint_PROHIBITED}}},
		{Constraints: []config.Constraint{{Value: "n2", Type: config.Constraint_PROHIBITED}}},
	}
	preferNotS1AndNotN2 := []config.LeasePreference{
		{
			Constraints: []config.Constraint{
				{Value: "s1", Type: config.Constraint_PROHIBITED},
				{Value: "n2", Type: config.Constraint_PROHIBITED},
			},
		},
	}
	preferMatchesNothing := []config.LeasePreference{
		{Constraints: []config.Constraint{{Key: "dc", Value: "5", Type: config.Constraint_REQUIRED}}},
		{Constraints: []config.Constraint{{Value: "n6", Type: config.Constraint_REQUIRED}}},
	}

	testCases := []struct {
		leaseholder        roachpb.StoreID
		existing           []roachpb.ReplicaDescriptor
		preferences        []config.LeasePreference
		expectedCheckTrue  roachpb.StoreID /* checkTransferLeaseSource = true */
		expectedCheckFalse roachpb.StoreID /* checkTransferLeaseSource = false */
	}{
		{1, nil, preferDC1, 0, 0},
		{1, replicas(1, 2, 3, 4), preferDC1, 0, 2},
		{1, replicas(2, 3, 4), preferDC1, 0, 2},
		{2, replicas(1, 2, 3, 4), preferDC1, 1, 1},
		{2, replicas(2, 3, 4), preferDC1, 0, 3},
		{4, replicas(2, 3, 4), preferDC1, 2, 2},
		{1, nil, preferDC4Then3Then2, 0, 0},
		{1, replicas(1, 2, 3, 4), preferDC4Then3Then2, 4, 4},
		{1, replicas(1, 2, 3), preferDC4Then3Then2, 3, 3},
		{1, replicas(1, 2), preferDC4Then3Then2, 2, 2},
		{3, replicas(1, 2, 3, 4), preferDC4Then3Then2, 4, 4},
		{3, replicas(1, 2, 3), preferDC4Then3Then2, 0, 2},
		{3, replicas(1, 3), preferDC4Then3Then2, 0, 1},
		{4, replicas(1, 2, 3, 4), preferDC4Then3Then2, 0, 3},
		{4, replicas(1, 2, 4), preferDC4Then3Then2, 0, 2},
		{4, replicas(1, 4), preferDC4Then3Then2, 0, 1},
		{1, replicas(1, 2, 3, 4), preferN2ThenS3, 2, 2},
		{1, replicas(1, 3, 4), preferN2ThenS3, 3, 3},
		{1, replicas(1, 4), preferN2ThenS3, 0, 4},
		{2, replicas(1, 2, 3, 4), preferN2ThenS3, 0, 3},
		{2, replicas(1, 2, 4), preferN2ThenS3, 0, 1},
		{3, replicas(1, 2, 3, 4), preferN2ThenS3, 2, 2},
		{3, replicas(1, 3, 4), preferN2ThenS3, 0, 1},
		{4, replicas(1, 4), preferN2ThenS3, 1, 1},
		{1, replicas(1, 2, 3, 4), preferNotS1ThenNotN2, 2, 2},
		{1, replicas(1, 3, 4), preferNotS1ThenNotN2, 3, 3},
		{1, replicas(1, 2), preferNotS1ThenNotN2, 2, 2},
		{1, replicas(1), preferNotS1ThenNotN2, 0, 0},
		{2, replicas(1, 2, 3, 4), preferNotS1ThenNotN2, 0, 3},
		{2, replicas(2, 3, 4), preferNotS1ThenNotN2, 0, 3},
		{2, replicas(1, 2, 3), preferNotS1ThenNotN2, 0, 3},
		{2, replicas(1, 2, 4), preferNotS1ThenNotN2, 0, 4},
		{4, replicas(1, 2, 3, 4), preferNotS1ThenNotN2, 2, 2},
		{4, replicas(1, 4), preferNotS1ThenNotN2, 0, 1},
		{1, replicas(1, 2, 3, 4), preferNotS1AndNotN2, 3, 3},
		{1, replicas(1, 2), preferNotS1AndNotN2, 0, 2},
		{2, replicas(1, 2, 3, 4), preferNotS1AndNotN2, 3, 3},
		{2, replicas(2, 3, 4), preferNotS1AndNotN2, 3, 3},
		{2, replicas(1, 2, 3), preferNotS1AndNotN2, 3, 3},
		{2, replicas(1, 2, 4), preferNotS1AndNotN2, 4, 4},
		{3, replicas(1, 3), preferNotS1AndNotN2, 0, 1},
		{4, replicas(1, 4), preferNotS1AndNotN2, 0, 1},
		{1, replicas(1, 2, 3, 4), preferMatchesNothing, 0, 2},
		{2, replicas(1, 2, 3, 4), preferMatchesNothing, 0, 1},
		{3, replicas(1, 3, 4), preferMatchesNothing, 1, 1},
		{4, replicas(1, 3, 4), preferMatchesNothing, 1, 1},
		{4, replicas(2, 3, 4), preferMatchesNothing, 2, 2},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			zone := config.ZoneConfig{LeasePreferences: c.preferences}
			result := a.ShouldTransferLease(
				context.Background(),
				zone,
				c.existing,
				c.leaseholder,
				0,
				nil, /* replicaStats */
			)
			expectTransfer := c.expectedCheckTrue != 0
			if expectTransfer != result {
				t.Errorf("expected %v, but found %v", expectTransfer, result)
			}
			target := a.TransferLeaseTarget(
				context.Background(),
				zone,
				c.existing,
				c.leaseholder,
				0,
				nil,   /* replicaStats */
				true,  /* checkTransferLeaseSource */
				true,  /* checkCandidateFullness */
				false, /* alwaysAllowDecisionWithoutStats */
			)
			if c.expectedCheckTrue != target.StoreID {
				t.Errorf("expected s%d for check=true, but found %v", c.expectedCheckTrue, target)
			}
			target = a.TransferLeaseTarget(
				context.Background(),
				zone,
				c.existing,
				c.leaseholder,
				0,
				nil,   /* replicaStats */
				false, /* checkTransferLeaseSource */
				true,  /* checkCandidateFullness */
				false, /* alwaysAllowDecisionWithoutStats */
			)
			if c.expectedCheckFalse != target.StoreID {
				t.Errorf("expected s%d for check=false, but found %v", c.expectedCheckFalse, target)
			}
		})
	}
}

func TestAllocatorLeasePreferencesMultipleStoresPerLocality(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ true)
	defer stopper.Stop(context.Background())

	// 6 stores, 2 in each of 3 distinct localities.
	var stores []*roachpb.StoreDescriptor
	for i := 1; i <= 6; i++ {
		var region, zone string
		if i <= 2 {
			region = "us-east1"
			zone = "us-east1-a"
		} else if i <= 4 {
			region = "us-east1"
			zone = "us-east1-b"
		} else {
			region = "us-west1"
			zone = "us-west1-a"
		}
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID: roachpb.StoreID(i),
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(i),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "region", Value: region},
						{Key: "zone", Value: zone},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{LeaseCount: int32(100 * i)},
		})
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	preferEast := []config.LeasePreference{
		{Constraints: []config.Constraint{{Key: "region", Value: "us-east1", Type: config.Constraint_REQUIRED}}},
	}
	preferNotEast := []config.LeasePreference{
		{Constraints: []config.Constraint{{Key: "region", Value: "us-east1", Type: config.Constraint_PROHIBITED}}},
	}

	testCases := []struct {
		leaseholder        roachpb.StoreID
		existing           []roachpb.ReplicaDescriptor
		preferences        []config.LeasePreference
		expectedCheckTrue  roachpb.StoreID /* checkTransferLeaseSource = true */
		expectedCheckFalse roachpb.StoreID /* checkTransferLeaseSource = false */
	}{
		{1, replicas(1, 3, 5), preferEast, 0, 3},
		{1, replicas(1, 2, 3), preferEast, 0, 2},
		{3, replicas(1, 3, 5), preferEast, 0, 1},
		{5, replicas(1, 4, 5), preferEast, 1, 1},
		{5, replicas(3, 4, 5), preferEast, 3, 3},
		{1, replicas(1, 5, 6), preferEast, 0, 5},
		{1, replicas(1, 3, 5), preferNotEast, 5, 5},
		{1, replicas(1, 5, 6), preferNotEast, 5, 5},
		{3, replicas(1, 3, 5), preferNotEast, 5, 5},
		{5, replicas(1, 5, 6), preferNotEast, 0, 6},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			zone := config.ZoneConfig{LeasePreferences: c.preferences}
			target := a.TransferLeaseTarget(
				context.Background(),
				zone,
				c.existing,
				c.leaseholder,
				0,
				nil,   /* replicaStats */
				true,  /* checkTransferLeaseSource */
				true,  /* checkCandidateFullness */
				false, /* alwaysAllowDecisionWithoutStats */
			)
			if c.expectedCheckTrue != target.StoreID {
				t.Errorf("expected s%d for check=true, but found %v", c.expectedCheckTrue, target)
			}
			target = a.TransferLeaseTarget(
				context.Background(),
				zone,
				c.existing,
				c.leaseholder,
				0,
				nil,   /* replicaStats */
				false, /* checkTransferLeaseSource */
				true,  /* checkCandidateFullness */
				false, /* alwaysAllowDecisionWithoutStats */
			)
			if c.expectedCheckFalse != target.StoreID {
				t.Errorf("expected s%d for check=false, but found %v", c.expectedCheckFalse, target)
			}
		})
	}
}

// All utilities from this point onward have been duplicated from _test.go files
// in the storage package. If there's a good way to pull in such dependencies
// without having to expose them to non-test code, it would be nice to do so.

// createTestAllocator creates a stopper, gossip, store pool and allocator for
// use in tests. Stopper must be stopped by the caller.
func createTestAllocator(
	deterministic bool,
) (*stop.Stopper, *gossip.Gossip, *storage.StorePool, storage.Allocator, *hlc.ManualClock) {
	stopper, g, manual, storePool, _ := createTestStorePool(
		storage.TestTimeUntilStoreDeadOff, deterministic, storage.NodeLivenessStatus_LIVE)
	a := storage.MakeAllocator(storePool, func(string) (time.Duration, bool) {
		return 0, true
	})
	return stopper, g, storePool, a, manual
}

// createTestStorePool creates a stopper, gossip and storePool for use in
// tests. Stopper must be stopped by the caller.
func createTestStorePool(
	timeUntilStoreDeadValue time.Duration,
	deterministic bool,
	defaultNodeStatus storage.NodeLivenessStatus,
) (*stop.Stopper, *gossip.Gossip, *hlc.ManualClock, *storage.StorePool, *mockNodeLiveness) {
	stopper := stop.NewStopper()
	mc := hlc.NewManualClock(123)
	clock := hlc.NewClock(mc.UnixNano, time.Nanosecond)
	st := cluster.MakeTestingClusterSettings()
	rpcContext := rpc.NewContext(
		log.AmbientContext{Tracer: st.Tracer}, &base.Config{Insecure: true}, clock, stopper,
		&st.Version)
	server := rpc.NewServer(rpcContext) // never started
	g := gossip.NewTest(1, rpcContext, server, stopper, metric.NewRegistry())
	mnl := newMockNodeLiveness(defaultNodeStatus)

	storage.TimeUntilStoreDead.Override(&st.SV, timeUntilStoreDeadValue)
	storePool := storage.NewStorePool(
		log.AmbientContext{Tracer: st.Tracer},
		st,
		g,
		clock,
		mnl.nodeLivenessFunc,
		deterministic,
	)
	return stopper, g, mc, storePool, mnl
}

type mockNodeLiveness struct {
	syncutil.Mutex
	defaultNodeStatus storage.NodeLivenessStatus
	nodes             map[roachpb.NodeID]storage.NodeLivenessStatus
}

func newMockNodeLiveness(defaultNodeStatus storage.NodeLivenessStatus) *mockNodeLiveness {
	return &mockNodeLiveness{
		defaultNodeStatus: defaultNodeStatus,
		nodes:             map[roachpb.NodeID]storage.NodeLivenessStatus{},
	}
}

func (m *mockNodeLiveness) nodeLivenessFunc(
	nodeID roachpb.NodeID, now time.Time, threshold time.Duration,
) storage.NodeLivenessStatus {
	m.Lock()
	defer m.Unlock()
	if status, ok := m.nodes[nodeID]; ok {
		return status
	}
	return m.defaultNodeStatus
}

func replicas(storeIDs ...roachpb.StoreID) []roachpb.ReplicaDescriptor {
	res := make([]roachpb.ReplicaDescriptor, len(storeIDs))
	for i, storeID := range storeIDs {
		res[i].NodeID = roachpb.NodeID(storeID)
		res[i].StoreID = storeID
	}
	return res
}
