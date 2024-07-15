// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostclient

import (
	"cmp"
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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
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

func TestDistSenderComputeNetworkCost(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	rddb := MockRangeDescriptorDB(func(key roachpb.RKey, reverse bool) (
		[]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error,
	) {
		// This test should not be using this at all, but DistSender insists on
		// having a non-nil one.
		return nil, nil, errors.New("range desc db unexpectedly used")
	})
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

	ruModel := tenantcostmodel.RequestUnitModelFromSettings(&st.SV)

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

	makeReplicaInfo := func(replicaID int, region string) ReplicaInfo {
		return ReplicaInfo{
			ReplicaDescriptor: roachpb.ReplicaDescriptor{
				ReplicaID: roachpb.ReplicaID(replicaID),
			},
			Locality: makeLocality(region),
		}
	}

	for _, tc := range []struct {
		name          string
		cfg           *DistSenderConfig
		desc          *roachpb.RangeDescriptor
		replicas      ReplicaSlice
		curReplica    *roachpb.ReplicaDescriptor
		expectedRead  tenantcostmodel.NetworkCost
		expectedWrite tenantcostmodel.NetworkCost
	}{
		{
			name:          "no kv interceptor",
			cfg:           &DistSenderConfig{},
			desc:          newRangeDescriptor(5),
			expectedRead:  0,
			expectedWrite: 0,
		},
		{
			name: "no cost config",
			cfg: &DistSenderConfig{
				KVInterceptor: &mockTenantSideCostController{},
			},
			desc:          newRangeDescriptor(2),
			expectedRead:  0,
			expectedWrite: 0,
		},
		{
			name: "no locality in current node",
			cfg: &DistSenderConfig{
				KVInterceptor: &mockTenantSideCostController{ruModel: &ruModel},
			},
			desc:          newRangeDescriptor(1),
			expectedRead:  0,
			expectedWrite: 0,
		},
		{
			name: "replicas=nil/replicas no locality",
			cfg: &DistSenderConfig{
				KVInterceptor: &mockTenantSideCostController{ruModel: &ruModel},
				NodeDescs: &mockNodeStore{
					nodes: []roachpb.NodeDescriptor{
						{NodeID: 1, Address: util.UnresolvedAddr{}},
						{NodeID: 2, Address: util.UnresolvedAddr{}},
						{NodeID: 3, Address: util.UnresolvedAddr{}},
					},
				},
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "eu-central1"},
					{Key: "az", Value: "az2"},
					{Key: "dc", Value: "dc3"},
				}},
			},
			desc: newRangeDescriptor(2),
			// Points to descriptor with NodeID 2.
			curReplica:    &roachpb.ReplicaDescriptor{NodeID: 2, ReplicaID: 3},
			expectedRead:  0,
			expectedWrite: 0,
		},
		{
			name: "replicas!=nil/replicas no locality",
			cfg: &DistSenderConfig{
				KVInterceptor: &mockTenantSideCostController{ruModel: &ruModel},
				NodeDescs: &mockNodeStore{
					nodes: []roachpb.NodeDescriptor{
						{NodeID: 1, Address: util.UnresolvedAddr{}},
						{NodeID: 2, Address: util.UnresolvedAddr{}},
						{NodeID: 3, Address: util.UnresolvedAddr{}},
					},
				},
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "eu-central1"},
				}},
			},
			desc: newRangeDescriptor(10),
			replicas: []ReplicaInfo{
				makeReplicaInfo(1, "foo"),
				makeReplicaInfo(2, "bar"),
				makeReplicaInfo(3, ""), // Missing region.
			},
			curReplica:    &roachpb.ReplicaDescriptor{ReplicaID: 3},
			expectedRead:  0,
			expectedWrite: 0,
		},
		{
			name: "some node descriptors not in gossip",
			cfg: &DistSenderConfig{
				KVInterceptor: &mockTenantSideCostController{ruModel: &ruModel},
				NodeDescs: &mockNodeStore{
					nodes: []roachpb.NodeDescriptor{
						makeNodeDescriptor(1, "us-east1"),        // 2.0
						makeNodeDescriptor(2, "eu-central1"),     // 0
						makeNodeDescriptor(3, "asia-southeast1"), // 2.5
					},
				},
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "eu-central1"},
					{Key: "az", Value: "az2"},
					{Key: "dc", Value: "dc3"},
				}},
			},
			desc:          newRangeDescriptor(6),
			curReplica:    &roachpb.ReplicaDescriptor{NodeID: 6, ReplicaID: 7},
			expectedRead:  0,
			expectedWrite: 2.0 + 2.5,
		},
		{
			name: "all node descriptors in gossip",
			cfg: &DistSenderConfig{
				KVInterceptor: &mockTenantSideCostController{ruModel: &ruModel},
				NodeDescs: &mockNodeStore{
					nodes: []roachpb.NodeDescriptor{
						makeNodeDescriptor(1, "us-east1"), // 3.0
					},
				},
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "asia-southeast1"},
				}},
			},
			desc: newRangeDescriptor(1),
			// Points to descriptor with NodeID 1.
			curReplica:    &roachpb.ReplicaDescriptor{NodeID: 1, ReplicaID: 2},
			expectedRead:  1.5,
			expectedWrite: 3.0,
		},
		{
			name: "local operations on global table",
			cfg: &DistSenderConfig{
				KVInterceptor: &mockTenantSideCostController{ruModel: &ruModel},
				NodeDescs: &mockNodeStore{
					nodes: []roachpb.NodeDescriptor{
						makeNodeDescriptor(1, "us-east1"),        // 0 * 3
						makeNodeDescriptor(2, "eu-central1"),     // 1.0
						makeNodeDescriptor(3, "asia-southeast1"), // 1.5
					},
				},
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "us-east1"},
				}},
			},
			desc: func() *roachpb.RangeDescriptor {
				rd := newRangeDescriptor(5)
				// Remap 4 and 5 to us-east1.
				rd.InternalReplicas[3].NodeID = 1
				rd.InternalReplicas[4].NodeID = 1
				return rd
			}(),
			// Points to descriptor with NodeID 1.
			curReplica:    &roachpb.ReplicaDescriptor{ReplicaID: 2},
			expectedRead:  0,
			expectedWrite: 1.0 + 1.5,
		},
		{
			name: "remote operations on global table",
			cfg: &DistSenderConfig{
				KVInterceptor: &mockTenantSideCostController{ruModel: &ruModel},
				NodeDescs: &mockNodeStore{
					nodes: []roachpb.NodeDescriptor{
						makeNodeDescriptor(1, "us-east1"),        // 3.0
						makeNodeDescriptor(2, "eu-central1"),     // 3.5
						makeNodeDescriptor(3, "asia-southeast1"), // 0
					},
				},
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "asia-southeast1"},
				}},
			},
			desc: func() *roachpb.RangeDescriptor {
				rd := newRangeDescriptor(5)
				// Remap 4 and 5 to us-east1.
				rd.InternalReplicas[3].NodeID = 1
				rd.InternalReplicas[4].NodeID = 1
				return rd
			}(),
			curReplica:    &roachpb.ReplicaDescriptor{NodeID: 1, ReplicaID: 2},
			expectedRead:  1.5,
			expectedWrite: 3.0*3 + 3.5,
		},
	} {
		for _, isWrite := range []bool{true, false} {
			t.Run(fmt.Sprintf("isWrite=%t/%s", isWrite, tc.name), func(t *testing.T) {
				tc.cfg.AmbientCtx = log.MakeTestingAmbientContext(tracing.NewTracer())
				tc.cfg.Stopper = stopper
				tc.cfg.RangeDescriptorDB = rddb
				tc.cfg.Settings = st
				tc.cfg.TransportFactory = func(SendOptions, ReplicaSlice) Transport {
					assert.Fail(t, "test should not try and use the transport factory")
					return nil
				}
				ds := NewDistSender(*tc.cfg)

				res := ds.computeNetworkCost(ctx, tc.desc, tc.curReplica, isWrite)
				if isWrite {
					require.InDelta(t, float64(tc.expectedWrite), float64(res), 0.01)
				} else {
					require.InDelta(t, float64(tc.expectedRead), float64(res), 0.01)
				}
			})
		}
	}
}

func TestDistSenderComputeWriteReplicationNetworkPaths(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	rddb := MockRangeDescriptorDB(func(key roachpb.RKey, reverse bool) (
		[]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error,
	) {
		// This test should not be using this at all, but DistSender insists on
		// having a non-nil one.
		return nil, nil, errors.New("range desc db unexpectedly used")
	})

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

	makeNetworkPath := func(
		fromID int, fromRegion string, toID int, toRegion string,
	) tenantcostmodel.LocalityNetworkPath {
		return tenantcostmodel.LocalityNetworkPath{
			FromNodeID:   roachpb.NodeID(fromID),
			FromLocality: makeLocality(fromRegion),
			ToNodeID:     roachpb.NodeID(toID),
			ToLocality:   makeLocality(toRegion),
		}
	}

	for _, tc := range []struct {
		name          string
		cfg           *DistSenderConfig
		desc          *roachpb.RangeDescriptor
		curReplica    *roachpb.ReplicaDescriptor
		expectedPaths []tenantcostmodel.LocalityNetworkPath
	}{
		{
			name: "missing node descriptor for leaseholder",
			cfg: &DistSenderConfig{
				NodeDescs: &mockNodeStore{
					nodes: []roachpb.NodeDescriptor{
						makeNodeDescriptor(2, "eu-central1"),
						makeNodeDescriptor(3, "asia-southeast1"),
					},
				},
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "asia-southeast1"},
				}},
			},
			desc: func() *roachpb.RangeDescriptor {
				return newRangeDescriptor(5)
			}(),
			curReplica:    &roachpb.ReplicaDescriptor{NodeID: 1, ReplicaID: 2},
			expectedPaths: []tenantcostmodel.LocalityNetworkPath{},
		},
		{
			name: "missing some node descriptors",
			cfg: &DistSenderConfig{
				NodeDescs: &mockNodeStore{
					nodes: []roachpb.NodeDescriptor{
						makeNodeDescriptor(1, "us-east1"),
						makeNodeDescriptor(3, "eu-central1"),
						makeNodeDescriptor(5, "asia-southeast1"),
						makeNodeDescriptor(7, "us-central1"),
					},
				},
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "asia-southeast1"},
				}},
			},
			desc: func() *roachpb.RangeDescriptor {
				return newRangeDescriptor(8)
			}(),
			curReplica: &roachpb.ReplicaDescriptor{NodeID: 3, ReplicaID: 4},
			expectedPaths: []tenantcostmodel.LocalityNetworkPath{
				makeNetworkPath(3, "eu-central1", 1, "us-east1"),
				makeNetworkPath(3, "eu-central1", 5, "asia-southeast1"),
				makeNetworkPath(3, "eu-central1", 7, "us-central1"),
			},
		},
		{
			name: "write for range with 5 replicas",
			cfg: &DistSenderConfig{
				NodeDescs: &mockNodeStore{
					nodes: []roachpb.NodeDescriptor{
						makeNodeDescriptor(1, "us-east1"),
						makeNodeDescriptor(2, "eu-central1"),
						makeNodeDescriptor(3, "asia-southeast1"),
					},
				},
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "asia-southeast1"},
				}},
			},
			desc: func() *roachpb.RangeDescriptor {
				rd := newRangeDescriptor(5)
				rd.InternalReplicas[3].NodeID = 1
				rd.InternalReplicas[3].ReplicaID = 2
				rd.InternalReplicas[4].NodeID = 2
				rd.InternalReplicas[4].ReplicaID = 3
				return rd
			}(),
			curReplica: &roachpb.ReplicaDescriptor{NodeID: 1, ReplicaID: 2},
			expectedPaths: []tenantcostmodel.LocalityNetworkPath{
				makeNetworkPath(1, "us-east1", 2, "eu-central1"),
				makeNetworkPath(1, "us-east1", 2, "eu-central1"),
				makeNetworkPath(1, "us-east1", 3, "asia-southeast1"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.cfg.AmbientCtx = log.MakeTestingAmbientContext(tracing.NewTracer())
			tc.cfg.Stopper = stopper
			tc.cfg.RangeDescriptorDB = rddb
			tc.cfg.Settings = cluster.MakeTestingClusterSettings()
			tc.cfg.TransportFactory = func(SendOptions, ReplicaSlice) Transport {
				assert.Fail(t, "test should not try and use the transport factory")
				return nil
			}
			ds := NewDistSender(*tc.cfg)
			paths := ds.computeWriteReplicationNetworkPaths(ctx, tc.desc, tc.curReplica)
			slices.SortFunc(paths, func(a, b tenantcostmodel.LocalityNetworkPath) int {
				if a.FromNodeID == b.FromNodeID {
					return cmp.Compare(a.ToNodeID, b.ToNodeID)
				}
				return cmp.Compare(a.FromNodeID, b.FromNodeID)
			})
			require.Equal(t, tc.expectedPaths, paths)
		})
	}
}
