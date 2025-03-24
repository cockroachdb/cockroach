// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregion

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestLatencyTracker verifies the basic functionality of the LatencyTracker,
// including initialization, latency tracking across different localities, and
// disabling/enabling behavior.
func TestLatencyTracker(t *testing.T) {
	nodeID1, nodeID2, nodeID3 := roachpb.NodeID(1), roachpb.NodeID(2), roachpb.NodeID(3)

	// Define test localities.
	sameZoneLocality := roachpb.Locality{
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east"},
			{Key: "zone", Value: "us-east-1a"},
		},
	}
	sameRegionDiffZoneLocality := roachpb.Locality{
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east"},
			{Key: "zone", Value: "us-east-1b"},
		},
	}
	crossRegionLocality := roachpb.Locality{
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-west"},
			{Key: "zone", Value: "us-west-1a"},
		},
	}

	// Create a latency map for testing.
	latencyMap := map[roachpb.NodeID]time.Duration{
		nodeID1: 10 * time.Millisecond,  // same zone
		nodeID2: 20 * time.Millisecond,  // same region, different zone
		nodeID3: 100 * time.Millisecond, // cross region
	}

	// Create node descriptor map.
	nodeDescs := map[roachpb.NodeID]*roachpb.NodeDescriptor{
		nodeID1: {
			NodeID:   nodeID1,
			Locality: sameZoneLocality,
		},
		nodeID2: {
			NodeID:   nodeID2,
			Locality: sameRegionDiffZoneLocality,
		},
		nodeID3: {
			NodeID:   nodeID3,
			Locality: crossRegionLocality,
		},
	}

	getLatency := func(nodeID roachpb.NodeID) (time.Duration, bool) {
		lat, ok := latencyMap[nodeID]
		return lat, ok
	}

	getNodeDesc := func(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error) {
		if _, ok := nodeDescs[nodeID]; !ok {
			return nil, errors.Newf("node descriptor not found for node ID %v", nodeID)
		}
		return nodeDescs[nodeID], nil
	}

	// Test initial state of the latency tracker.
	t.Run("initial state", func(t *testing.T) {
		lt := NewLatencyTracker(sameZoneLocality, getLatency, getNodeDesc)
		require.False(t, lt.Enabled())

		// All locality types should return default latency when disabled.
		for i := roachpb.LocalityComparisonType_UNDEFINED; i < maxLocalityComparisonType; i++ {
			require.Equal(t, closedts.DefaultMaxNetworkRTT, lt.GetLatencyByLocalityProximity(i))
		}
	})

	// Test refreshing latencies and verifying they match expected values.
	t.Run("refresh and verify latencies", func(t *testing.T) {
		lt := NewLatencyTracker(sameZoneLocality, getLatency, getNodeDesc)
		lt.RefreshLatency([]roachpb.NodeID{nodeID1, nodeID2, nodeID3})
		require.True(t, lt.Enabled())

		// Verify latencies for each locality comparison type.
		tests := []struct {
			lct      roachpb.LocalityComparisonType
			expected time.Duration
		}{
			{roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE, 10 * time.Millisecond},
			{roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE, 20 * time.Millisecond},
			{roachpb.LocalityComparisonType_CROSS_REGION, 100 * time.Millisecond},
			{roachpb.LocalityComparisonType_UNDEFINED, closedts.DefaultMaxNetworkRTT},
		}

		for _, tc := range tests {
			actual := lt.GetLatencyByLocalityProximity(tc.lct)
			require.Equal(t, tc.expected, actual, "locality type: %v", tc.lct)
		}
	})

	t.Run("disable tracker", func(t *testing.T) {
		lt := NewLatencyTracker(sameZoneLocality, getLatency, getNodeDesc)
		lt.RefreshLatency([]roachpb.NodeID{nodeID1, nodeID2, nodeID3})
		require.True(t, lt.Enabled())

		lt.Disable()
		require.False(t, lt.Enabled())

		// All locality types should return default latency when disabled.
		for i := roachpb.LocalityComparisonType_UNDEFINED; i < maxLocalityComparisonType; i++ {
			require.Equal(t, closedts.DefaultMaxNetworkRTT, lt.GetLatencyByLocalityProximity(i))
		}
	})

	t.Run("multiple nodes same locality", func(t *testing.T) {
		// Create new node with same zone locality but higher latency.
		nodeID4 := roachpb.NodeID(4)
		latencyMap[nodeID4] = 15 * time.Millisecond
		nodeDescs[nodeID4] = &roachpb.NodeDescriptor{
			NodeID:   nodeID4,
			Locality: sameZoneLocality,
		}

		lt := NewLatencyTracker(sameZoneLocality, getLatency, getNodeDesc)
		lt.RefreshLatency([]roachpb.NodeID{nodeID1, nodeID4})

		// Should use the maximum latency among nodes with same locality.
		require.Equal(t, 15*time.Millisecond,
			lt.GetLatencyByLocalityProximity(roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE))
	})

	t.Run("missing node descriptor", func(t *testing.T) {
		lt := NewLatencyTracker(sameZoneLocality, getLatency, getNodeDesc)
		// Include a non-existent node ID.
		lt.RefreshLatency([]roachpb.NodeID{nodeID1, 999})

		// Should still work for existing nodes.
		require.Equal(t, 10*time.Millisecond,
			lt.GetLatencyByLocalityProximity(roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE))
	})
}

func TestLatencyTrackerMaxLatencySelection(t *testing.T) {
	// Define base localities.
	baseLocality := roachpb.Locality{
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east"},
			{Key: "zone", Value: "us-east-1a"},
		},
	}

	// Helper to create a node descriptor.
	makeNodeDesc := func(nodeID roachpb.NodeID, locality roachpb.Locality) *roachpb.NodeDescriptor {
		return &roachpb.NodeDescriptor{
			NodeID:   nodeID,
			Locality: locality,
		}
	}

	tests := []struct {
		name  string
		nodes map[roachpb.NodeID]struct {
			locality roachpb.Locality
			latency  time.Duration
		}
		expectedLatencies map[roachpb.LocalityComparisonType]time.Duration
	}{
		{
			name: "multiple same zone nodes with varying latencies",
			nodes: map[roachpb.NodeID]struct {
				locality roachpb.Locality
				latency  time.Duration
			}{
				1: {
					locality: roachpb.Locality{
						Tiers: []roachpb.Tier{
							{Key: "region", Value: "us-east"},
							{Key: "zone", Value: "us-east-1a"},
						},
					},
					latency: 10 * time.Millisecond,
				},
				2: {
					locality: roachpb.Locality{
						Tiers: []roachpb.Tier{
							{Key: "region", Value: "us-east"},
							{Key: "zone", Value: "us-east-1a"},
						},
					},
					latency: 25 * time.Millisecond,
				},
				3: {
					locality: roachpb.Locality{
						Tiers: []roachpb.Tier{
							{Key: "region", Value: "us-east"},
							{Key: "zone", Value: "us-east-1a"},
						},
					},
					latency: 15 * time.Millisecond,
				},
			},
			expectedLatencies: map[roachpb.LocalityComparisonType]time.Duration{
				roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE: 25 * time.Millisecond,
			},
		},
		{
			name: "mixed locality types with multiple nodes each",
			nodes: map[roachpb.NodeID]struct {
				locality roachpb.Locality
				latency  time.Duration
			}{
				// Same zone nodes.
				1: {
					locality: roachpb.Locality{
						Tiers: []roachpb.Tier{
							{Key: "region", Value: "us-east"},
							{Key: "zone", Value: "us-east-1a"},
						},
					},
					latency: 10 * time.Millisecond,
				},
				2: {
					locality: roachpb.Locality{
						Tiers: []roachpb.Tier{
							{Key: "region", Value: "us-east"},
							{Key: "zone", Value: "us-east-1a"},
						},
					},
					latency: 15 * time.Millisecond,
				},
				// Cross zone, same region nodes.
				3: {
					locality: roachpb.Locality{
						Tiers: []roachpb.Tier{
							{Key: "region", Value: "us-east"},
							{Key: "zone", Value: "us-east-1b"},
						},
					},
					latency: 30 * time.Millisecond,
				},
				4: {
					locality: roachpb.Locality{
						Tiers: []roachpb.Tier{
							{Key: "region", Value: "us-east"},
							{Key: "zone", Value: "us-east-1c"},
						},
					},
					latency: 40 * time.Millisecond,
				},
				// Cross region nodes.
				5: {
					locality: roachpb.Locality{
						Tiers: []roachpb.Tier{
							{Key: "region", Value: "us-west"},
							{Key: "zone", Value: "us-west-1a"},
						},
					},
					latency: 90 * time.Millisecond,
				},
				6: {
					locality: roachpb.Locality{
						Tiers: []roachpb.Tier{
							{Key: "region", Value: "eu-west"},
							{Key: "zone", Value: "eu-west-1a"},
						},
					},
					latency: 100 * time.Millisecond,
				},
			},
			expectedLatencies: map[roachpb.LocalityComparisonType]time.Duration{
				roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE:  15 * time.Millisecond,
				roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE: 40 * time.Millisecond,
				roachpb.LocalityComparisonType_CROSS_REGION:           100 * time.Millisecond,
			},
		},
		{
			name: "zero latencies mixed with non-zero",
			nodes: map[roachpb.NodeID]struct {
				locality roachpb.Locality
				latency  time.Duration
			}{
				1: {
					locality: roachpb.Locality{
						Tiers: []roachpb.Tier{
							{Key: "region", Value: "us-east"},
							{Key: "zone", Value: "us-east-1a"},
						},
					},
					latency: 0,
				},
				2: {
					locality: roachpb.Locality{
						Tiers: []roachpb.Tier{
							{Key: "region", Value: "us-east"},
							{Key: "zone", Value: "us-east-1a"},
						},
					},
					latency: 20 * time.Millisecond,
				},
				3: {
					locality: roachpb.Locality{
						Tiers: []roachpb.Tier{
							{Key: "region", Value: "us-east"},
							{Key: "zone", Value: "us-east-1a"},
						},
					},
					latency: 15 * time.Millisecond,
				},
			},
			expectedLatencies: map[roachpb.LocalityComparisonType]time.Duration{
				roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE: 20 * time.Millisecond,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create maps for the LatencyTracker constructor.
			nodeDescs := make(map[roachpb.NodeID]*roachpb.NodeDescriptor)
			latencyMap := make(map[roachpb.NodeID]time.Duration)
			var nodeIDs []roachpb.NodeID

			for nodeID, info := range tc.nodes {
				nodeDescs[nodeID] = makeNodeDesc(nodeID, info.locality)
				latencyMap[nodeID] = info.latency
				nodeIDs = append(nodeIDs, nodeID)
			}

			getLatency := func(nodeID roachpb.NodeID) (time.Duration, bool) {
				lat, ok := latencyMap[nodeID]
				return lat, ok
			}

			getNodeDesc := func(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error) {
				return nodeDescs[nodeID], nil
			}

			lt := NewLatencyTracker(baseLocality, getLatency, getNodeDesc)
			lt.RefreshLatency(nodeIDs)
			require.Equal(t, closedts.DefaultMaxNetworkRTT,
				lt.GetLatencyByLocalityProximity(roachpb.LocalityComparisonType_UNDEFINED))
			for lct, expectedLatency := range tc.expectedLatencies {
				actual := lt.GetLatencyByLocalityProximity(lct)
				require.Equal(t, expectedLatency, actual,
					"locality comparison type %v: expected %v, got %v",
					lct, expectedLatency, actual)
			}
		})
	}
}
