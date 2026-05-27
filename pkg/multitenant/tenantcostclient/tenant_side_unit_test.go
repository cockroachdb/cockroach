// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostclient

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// This package contains unit tests that need access to un-exported functions.

func TestCalculateBackgroundCPUSecs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name          string
		globalCPURate float64
		deltaTime     time.Duration
		localCPUSecs  float64
		expected      float64
	}{
		{
			name:          "global consumption is zero",
			globalCPURate: 0,
			deltaTime:     time.Second,
			localCPUSecs:  2,
			expected:      0.2,
		},
		{
			name:          "local is small fraction of global consumption",
			globalCPURate: 12,
			deltaTime:     time.Second,
			localCPUSecs:  2,
			expected:      0.1,
		},
		{
			name:          "local consumption is greater than amortization limit",
			globalCPURate: 8,
			deltaTime:     time.Second,
			localCPUSecs:  8,
			expected:      0.6,
		},
		{
			name:          "local consumption is greater than amortization limit but less than global",
			globalCPURate: 12,
			deltaTime:     time.Second,
			localCPUSecs:  8,
			expected:      0.4,
		},
		{
			name:          "time delta is > 1, less than amortization limit",
			globalCPURate: 2,
			deltaTime:     2 * time.Second,
			localCPUSecs:  8,
			expected:      0.8,
		},
		{
			name:          "time delta is > 1, equal to amortization limit",
			globalCPURate: 6,
			deltaTime:     2 * time.Second,
			localCPUSecs:  12,
			expected:      1.2,
		},
	}

	cpuModel := tenantcostmodel.EstimatedCPUModel{
		BackgroundCPU: struct {
			Amount       tenantcostmodel.EstimatedCPU
			Amortization float64
		}{
			Amount:       0.6,
			Amortization: 6,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := calculateBackgroundCPUSecs(
				&cpuModel, tc.globalCPURate, tc.deltaTime, tc.localCPUSecs)
			require.InEpsilon(t, tc.expected, actual, 0.00000001)
		})
	}
}

func TestComputeNetworkCost(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	st := cluster.MakeTestingClusterSettings()

	// Set regional cost multiplier table.
	//                     | us-east1 | eu-central1 | asia-southeast1
	//     -----------------------------------------------------------
	//        us-east1     |    0     |      1      |       1.5
	//       eu-central1   |    2     |      0      |       2.5
	//     asia-southeast1 |    3     |     3.5     |        0
	costTable := `{"regionPairs": [
		{"fromRegion": "us-east1", "toRegion": "eu-central1", "cost": 1},
		{"fromRegion": "us-east1", "toRegion": "asia-southeast1", "cost": 1.5},
		{"fromRegion": "eu-central1", "toRegion": "us-east1", "cost": 2},
		{"fromRegion": "eu-central1", "toRegion": "asia-southeast1", "cost": 2.5},
		{"fromRegion": "asia-southeast1", "toRegion": "us-east1", "cost": 3},
		{"fromRegion": "asia-southeast1", "toRegion": "eu-central1", "cost": 3.5}
	]}`
	require.NoError(t, tenantcostmodel.CrossRegionNetworkCostSetting.Validate(nil, costTable))
	tenantcostmodel.CrossRegionNetworkCostSetting.Override(ctx, &st.SV, costTable)

	newRangeDescriptor := func(numReplicas int) *roachpb.RangeDescriptor {
		desc := &roachpb.RangeDescriptor{
			InternalReplicas: make([]roachpb.ReplicaDescriptor, numReplicas),
		}
		// ReplicaIDs are always NodeIDs + 1 for this test.
		for i := 1; i <= numReplicas; i++ {
			desc.InternalReplicas[i-1].NodeID = roachpb.NodeID(i)
			desc.InternalReplicas[i-1].ReplicaID = roachpb.ReplicaID(i + 1)
		}
		return desc
	}

	makeLocality := func(region string) roachpb.Locality {
		return roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "az", Value: fmt.Sprintf("az%d", rand.Intn(10))},
				{Key: "region", Value: region},
				{Key: "dc", Value: fmt.Sprintf("dc%d", rand.Intn(10))},
			},
		}
	}

	makeNodeDescriptor := func(nodeID int, region string) roachpb.NodeDescriptor {
		return roachpb.NodeDescriptor{
			NodeID:   roachpb.NodeID(nodeID),
			Address:  util.UnresolvedAddr{},
			Locality: makeLocality(region),
		}
	}

	for _, tc := range []struct {
		name          string
		locality      roachpb.Locality
		nodes         []roachpb.NodeDescriptor
		targetRange   *roachpb.RangeDescriptor
		targetReplica *roachpb.ReplicaDescriptor
		expectedRead  tenantcostmodel.NetworkCost
		expectedWrite tenantcostmodel.NetworkCost
	}{
		{
			name:          "no locality in current node",
			targetRange:   newRangeDescriptor(1),
			expectedRead:  0,
			expectedWrite: 0,
		},
		{
			name: "node descriptors have no locality",
			nodes: []roachpb.NodeDescriptor{
				{NodeID: 1, Address: util.UnresolvedAddr{}},
				{NodeID: 2, Address: util.UnresolvedAddr{}},
				{NodeID: 3, Address: util.UnresolvedAddr{}},
			},
			locality: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "eu-central1"},
				{Key: "az", Value: "az2"},
				{Key: "dc", Value: "dc3"},
			}},
			targetRange: newRangeDescriptor(2),
			// Points to descriptor with NodeID 2.
			targetReplica: &roachpb.ReplicaDescriptor{NodeID: 2, ReplicaID: 3},
			expectedRead:  0,
			expectedWrite: 0,
		},
		{
			name: "node descriptors not in gossip",
			nodes: []roachpb.NodeDescriptor{
				makeNodeDescriptor(1, "us-east1"),        // 2.0
				makeNodeDescriptor(2, "eu-central1"),     // 0
				makeNodeDescriptor(3, "asia-southeast1"), // 2.5
			},
			locality: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "eu-central1"},
				{Key: "az", Value: "az2"},
				{Key: "dc", Value: "dc3"},
			}},
			targetRange:   newRangeDescriptor(6),
			targetReplica: &roachpb.ReplicaDescriptor{NodeID: 6, ReplicaID: 7},
			expectedRead:  0,
			expectedWrite: 2.0 + 2.5,
		},
		{
			name: "all node descriptors in gossip",
			nodes: []roachpb.NodeDescriptor{
				makeNodeDescriptor(1, "us-east1"), // 3.0
			},
			locality: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "asia-southeast1"},
			}},
			targetRange: newRangeDescriptor(1),
			// Points to descriptor with NodeID 1.
			targetReplica: &roachpb.ReplicaDescriptor{NodeID: 1, ReplicaID: 2},
			expectedRead:  1.5,
			expectedWrite: 3.0,
		},
		{
			name: "local operations on global table",
			nodes: []roachpb.NodeDescriptor{
				makeNodeDescriptor(1, "us-east1"),        // 0 * 3
				makeNodeDescriptor(2, "eu-central1"),     // 1.0
				makeNodeDescriptor(3, "asia-southeast1"), // 1.5
			},
			locality: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-east1"},
			}},
			targetRange: func() *roachpb.RangeDescriptor {
				rd := newRangeDescriptor(5)
				// Remap 4 and 5 to us-east1.
				rd.InternalReplicas[3].NodeID = 1
				rd.InternalReplicas[4].NodeID = 1
				return rd
			}(),
			// Points to descriptor with NodeID 1.
			targetReplica: &roachpb.ReplicaDescriptor{ReplicaID: 2},
			expectedRead:  0,
			expectedWrite: 1.0 + 1.5,
		},
		{
			name: "remote operations on global table",
			nodes: []roachpb.NodeDescriptor{
				makeNodeDescriptor(1, "us-east1"),        // 3.0
				makeNodeDescriptor(2, "eu-central1"),     // 3.5
				makeNodeDescriptor(3, "asia-southeast1"), // 0
			},
			locality: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "asia-southeast1"},
			}},
			targetRange: func() *roachpb.RangeDescriptor {
				rd := newRangeDescriptor(5)
				// Remap 4 and 5 to us-east1.
				rd.InternalReplicas[3].NodeID = 1
				rd.InternalReplicas[4].NodeID = 1
				return rd
			}(),
			targetReplica: &roachpb.ReplicaDescriptor{NodeID: 1, ReplicaID: 2},
			expectedRead:  1.5,
			expectedWrite: 3.0*3 + 3.5,
		},
	} {
		for _, isWrite := range []bool{true, false} {
			t.Run(fmt.Sprintf("isWrite=%t/%s", isWrite, tc.name), func(t *testing.T) {
				nodeDescs := &TestNodeDescStore{tc.nodes}
				controller, err := newTenantSideCostController(
					st, roachpb.MustMakeTenantID(10), nil /* provider */, nodeDescs,
					tc.locality, timeutil.DefaultTimeSource{}, nil /* testInstr */)
				require.NoError(t, err)
				res := controller.computeNetworkCost(ctx, tc.targetRange, tc.targetReplica, isWrite)
				if isWrite {
					require.InDelta(t, float64(tc.expectedWrite), float64(res), 0.01)
				} else {
					require.InDelta(t, float64(tc.expectedRead), float64(res), 0.01)
				}
			})
		}
	}
}

func TestUpdateEstimatedWriteReplicationBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	newRangeDescriptor := func(numReplicas int) *roachpb.RangeDescriptor {
		desc := &roachpb.RangeDescriptor{
			InternalReplicas: make([]roachpb.ReplicaDescriptor, numReplicas),
		}
		// ReplicaIDs are always NodeIDs + 1 for this test.
		for i := 1; i <= numReplicas; i++ {
			desc.InternalReplicas[i-1].NodeID = roachpb.NodeID(i)
			desc.InternalReplicas[i-1].ReplicaID = roachpb.ReplicaID(i + 1)
		}
		return desc
	}

	makeLocality := func(region string) roachpb.Locality {
		return roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: region}}}
	}

	makeNodeDescriptor := func(nodeID int, region string) roachpb.NodeDescriptor {
		return roachpb.NodeDescriptor{
			NodeID:   roachpb.NodeID(nodeID),
			Address:  util.UnresolvedAddr{},
			Locality: makeLocality(region),
		}
	}

	for _, tc := range []struct {
		name          string
		locality      roachpb.Locality
		nodes         []roachpb.NodeDescriptor
		targetRange   *roachpb.RangeDescriptor
		targetReplica *roachpb.ReplicaDescriptor
		expected      int64
	}{
		{
			name: "missing node descriptor for leaseholder",
			nodes: []roachpb.NodeDescriptor{
				makeNodeDescriptor(2, "eu-central1"),
				makeNodeDescriptor(3, "asia-southeast1"),
			},
			locality: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "asia-southeast1"},
			}},
			targetRange: func() *roachpb.RangeDescriptor {
				return newRangeDescriptor(5)
			}(),
			targetReplica: &roachpb.ReplicaDescriptor{NodeID: 1, ReplicaID: 2},
			expected:      0,
		},
		{
			name: "missing some node descriptors",
			nodes: []roachpb.NodeDescriptor{
				makeNodeDescriptor(1, "us-east1"),
				makeNodeDescriptor(3, "eu-central1"),
				makeNodeDescriptor(5, "asia-southeast1"),
				makeNodeDescriptor(7, "us-central1"),
			},
			locality: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "asia-southeast1"},
			}},
			targetRange: func() *roachpb.RangeDescriptor {
				return newRangeDescriptor(8)
			}(),
			targetReplica: &roachpb.ReplicaDescriptor{NodeID: 3, ReplicaID: 4},
			expected:      300,
		},
		{
			name: "write for range with 5 replicas",
			nodes: []roachpb.NodeDescriptor{
				makeNodeDescriptor(1, "us-east1"),
				makeNodeDescriptor(2, "eu-central1"),
				makeNodeDescriptor(3, "asia-southeast1"),
			},
			locality: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "asia-southeast1"},
			}},
			targetRange: func() *roachpb.RangeDescriptor {
				rd := newRangeDescriptor(5)
				rd.InternalReplicas[3].NodeID = 1
				rd.InternalReplicas[3].ReplicaID = 2
				rd.InternalReplicas[4].NodeID = 2
				rd.InternalReplicas[4].ReplicaID = 3
				return rd
			}(),
			targetReplica: &roachpb.ReplicaDescriptor{NodeID: 1, ReplicaID: 2},
			expected:      300,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodeDescs := &TestNodeDescStore{tc.nodes}
			controller, err := newTenantSideCostController(
				cluster.MakeTestingClusterSettings(), roachpb.MustMakeTenantID(10),
				nil /* provider */, nodeDescs, tc.locality, timeutil.DefaultTimeSource{},
				nil /* testInstr */)
			require.NoError(t, err)
			before := controller.metrics.EstimatedReplicationBytes.Count()
			controller.UpdateEstimatedWriteReplicationBytes(ctx, tc.targetRange, tc.targetReplica, 100)
			after := controller.metrics.EstimatedReplicationBytes.Count()
			require.Equal(t, tc.expected, after-before)
		})
	}
}

type TestNodeDescStore struct {
	NodeDescs []roachpb.NodeDescriptor
}

// Implements the kvclient.NodeDescStore interface.
func (st *TestNodeDescStore) GetNodeDescriptor(
	nodeID roachpb.NodeID,
) (*roachpb.NodeDescriptor, error) {
	for i := range st.NodeDescs {
		if st.NodeDescs[i].NodeID == nodeID {
			return &st.NodeDescs[i], nil
		}
	}
	return nil, errors.Newf("could not find nodeID %d", nodeID)
}

// Implements the kvclient.NodeDescStore interface.
func (st *TestNodeDescStore) GetNodeDescriptorCount() int {
	return len(st.NodeDescs)
}

// Implements the kvclient.NodeDescStore interface.
func (*TestNodeDescStore) GetStoreDescriptor(roachpb.StoreID) (*roachpb.StoreDescriptor, error) {
	return nil, errors.New("Not implemented")
}
