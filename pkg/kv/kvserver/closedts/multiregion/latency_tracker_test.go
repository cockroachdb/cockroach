package multiregion

import (
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

// TestLatencyRefresherBasic verifies the basic functionality of the LatencyTracker.
func TestLatencyRefresherBasic(t *testing.T) {
	baseLocality := roachpb.Locality{
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east"},
			{Key: "zone", Value: "us-east-1a"},
		},
	}

	nodeDescs := map[roachpb.NodeID]*roachpb.NodeDescriptor{
		1: { // Same zone
			NodeID: 1,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "us-east"},
					{Key: "zone", Value: "us-east-1a"},
				},
			},
		},
		2: { // Cross zone
			NodeID: 2,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "us-east"},
					{Key: "zone", Value: "us-east-1b"},
				},
			},
		},
		3: { // Cross region
			NodeID: 3,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "us-west"},
					{Key: "zone", Value: "us-west-1a"},
				},
			},
		},
		4: { // Invalid/undefined locality
			NodeID: 4,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{},
			},
		},
	}

	testCases := []struct {
		name         string
		nodeIDs      []roachpb.NodeID
		latencyMap   map[roachpb.NodeID]time.Duration
		expectedRTTs map[roachpb.LocalityComparisonType]time.Duration
	}{
		{
			name:    "basic test with all locality types",
			nodeIDs: []roachpb.NodeID{1, 2, 3, 4},
			latencyMap: map[roachpb.NodeID]time.Duration{
				1: 1 * time.Millisecond,  // same zone
				2: 5 * time.Millisecond,  // cross zone
				3: 25 * time.Millisecond, // cross region
			},
			expectedRTTs: map[roachpb.LocalityComparisonType]time.Duration{
				roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE:  1 * time.Millisecond,
				roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE: 5 * time.Millisecond,
				roachpb.LocalityComparisonType_CROSS_REGION:           25 * time.Millisecond,
				roachpb.LocalityComparisonType_UNDEFINED:              closedts.DefaultMaxNetworkRTT,
			},
		},
		{
			name:    "missing latencies keep default",
			nodeIDs: []roachpb.NodeID{1, 2, 3},
			latencyMap: map[roachpb.NodeID]time.Duration{
				1: 1 * time.Millisecond,
				// Missing latency for node 2
				3: 25 * time.Millisecond,
			},
			expectedRTTs: map[roachpb.LocalityComparisonType]time.Duration{
				roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE:  1 * time.Millisecond,
				roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE: closedts.DefaultMaxNetworkRTT,
				roachpb.LocalityComparisonType_CROSS_REGION:           25 * time.Millisecond,
				roachpb.LocalityComparisonType_UNDEFINED:              closedts.DefaultMaxNetworkRTT,
			},
		},
		{
			name:    "multiple nodes of same locality type",
			nodeIDs: []roachpb.NodeID{1, 2, 3},
			latencyMap: map[roachpb.NodeID]time.Duration{
				1: 1 * time.Millisecond,
				2: 10 * time.Millisecond,
				3: 20 * time.Millisecond,
			},
			expectedRTTs: map[roachpb.LocalityComparisonType]time.Duration{
				roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE:  1 * time.Millisecond,
				roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE: 10 * time.Millisecond,
				roachpb.LocalityComparisonType_CROSS_REGION:           20 * time.Millisecond,
				roachpb.LocalityComparisonType_UNDEFINED:              closedts.DefaultMaxNetworkRTT,
			},
		},
		{
			name:       "empty node list maintains defaults",
			nodeIDs:    []roachpb.NodeID{},
			latencyMap: map[roachpb.NodeID]time.Duration{},
			expectedRTTs: map[roachpb.LocalityComparisonType]time.Duration{
				roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE:  closedts.DefaultMaxNetworkRTT,
				roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE: closedts.DefaultMaxNetworkRTT,
				roachpb.LocalityComparisonType_CROSS_REGION:           closedts.DefaultMaxNetworkRTT,
				roachpb.LocalityComparisonType_UNDEFINED:              closedts.DefaultMaxNetworkRTT,
			},
		},
		{
			name:    "invalid node descriptors",
			nodeIDs: []roachpb.NodeID{99}, // Non-existent node
			latencyMap: map[roachpb.NodeID]time.Duration{
				99: 50 * time.Millisecond,
			},
			expectedRTTs: map[roachpb.LocalityComparisonType]time.Duration{
				roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE:  closedts.DefaultMaxNetworkRTT,
				roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE: closedts.DefaultMaxNetworkRTT,
				roachpb.LocalityComparisonType_CROSS_REGION:           closedts.DefaultMaxNetworkRTT,
				roachpb.LocalityComparisonType_UNDEFINED:              closedts.DefaultMaxNetworkRTT,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			getNodeDesc := func(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error) {
				if desc, ok := nodeDescs[nodeID]; ok {
					return desc, nil
				}
				return nil, errors.New("node not found")
			}

			getLatency := func(nodeID roachpb.NodeID) (time.Duration, bool) {
				if latency, ok := tc.latencyMap[nodeID]; ok {
					return latency, true
				}
				return 0, false
			}

			lr := NewLatencyTracker(baseLocality, getLatency, getNodeDesc)
			lr.RefreshLatency(tc.nodeIDs)

			// Verify RTTs for all locality comparison types
			for lct := roachpb.LocalityComparisonType(0); lct < roachpb.LocalityComparisonType_MAX_LOCALITY_COMPARISON_TYPE; lct++ {
				expected := tc.expectedRTTs[lct]
				actual := lr.GetLatencyByLocalityProximity(lct)
				require.Equal(t, expected, actual,
					"incorrect RTT for locality comparison type %v", lct)
			}
		})
	}
}

// TestLatencyRefresherUpdateSingleLocality verifies the behavior of updating latency
// for individual locality comparison types. It tests:
// - Updates to valid locality types
// - Updates to undefined locality types
// - Updates to max locality types
func TestLatencyRefresherUpdateSingleLocality(t *testing.T) {
	baseLocality := roachpb.Locality{
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east"},
		},
	}

	lr := NewLatencyTracker(
		baseLocality,
		func(roachpb.NodeID) (time.Duration, bool) { return 0, false },
		func(roachpb.NodeID) (*roachpb.NodeDescriptor, error) { return nil, nil },
	)

	testCases := []struct {
		name      string
		lct       roachpb.LocalityComparisonType
		latency   time.Duration
		expectSet bool
	}{
		{
			name:      "update valid locality type",
			lct:       roachpb.LocalityComparisonType_CROSS_REGION,
			latency:   100 * time.Millisecond,
			expectSet: true,
		},
		{
			name:      "update undefined locality type",
			lct:       roachpb.LocalityComparisonType_UNDEFINED,
			latency:   100 * time.Millisecond,
			expectSet: false,
		},
		{
			name:      "update max locality type",
			lct:       roachpb.LocalityComparisonType_MAX_LOCALITY_COMPARISON_TYPE,
			latency:   100 * time.Millisecond,
			expectSet: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			originalValue := lr.GetLatencyByLocalityProximity(tc.lct)
			lr.updateLatencyForLocalityProximity(tc.lct, tc.latency)
			newValue := lr.GetLatencyByLocalityProximity(tc.lct)

			if tc.expectSet {
				require.Equal(t, tc.latency, newValue)
			} else {
				require.Equal(t, originalValue, newValue)
			}
		})
	}
}

// TestLatencyRefresherMaxLatencySelection verifies that the LatencyTracker correctly
// selects the maximum latency among multiple instances of the same locality type.
func TestLatencyRefresherMaxLatencySelection(t *testing.T) {
	baseLocality := roachpb.Locality{
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east"},
			{Key: "zone", Value: "us-east-1a"},
		},
	}

	nodeDescs := map[roachpb.NodeID]*roachpb.NodeDescriptor{
		1: { // Same zone instance 1
			NodeID: 1,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "us-east"},
					{Key: "zone", Value: "us-east-1a"},
				},
			},
		},
		2: { // Same zone instance 2
			NodeID: 2,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "us-east"},
					{Key: "zone", Value: "us-east-1a"},
				},
			},
		},
		3: { // Cross zone instance 1
			NodeID: 3,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "us-east"},
					{Key: "zone", Value: "us-east-1b"},
				},
			},
		},
		4: { // Cross zone instance 2
			NodeID: 4,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "us-east"},
					{Key: "zone", Value: "us-east-1b"},
				},
			},
		},
		5: { // Cross region instance 1
			NodeID: 5,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "us-west"},
					{Key: "zone", Value: "us-west-1a"},
				},
			},
		},
		6: { // Cross region instance 2
			NodeID: 6,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "eu-west"},
					{Key: "zone", Value: "eu-west-1a"},
				},
			},
		},
	}

	testCases := []struct {
		name         string
		nodeIDs      []roachpb.NodeID
		latencyMap   map[roachpb.NodeID]time.Duration
		expectedRTTs map[roachpb.LocalityComparisonType]time.Duration
	}{
		{
			name:    "max latency among multiple instances per locality",
			nodeIDs: []roachpb.NodeID{1, 2, 3, 4, 5, 6},
			latencyMap: map[roachpb.NodeID]time.Duration{
				1: 1 * time.Millisecond,  // same zone - lower
				2: 5 * time.Millisecond,  // same zone - higher
				3: 10 * time.Millisecond, // cross zone - lower
				4: 15 * time.Millisecond, // cross zone - higher
				5: 50 * time.Millisecond, // cross region - lower
				6: 80 * time.Millisecond, // cross region - higher
			},
			expectedRTTs: map[roachpb.LocalityComparisonType]time.Duration{
				roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE:  5 * time.Millisecond,  // max of same zone
				roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE: 15 * time.Millisecond, // max of cross zone
				roachpb.LocalityComparisonType_CROSS_REGION:           80 * time.Millisecond, // max of cross region
				roachpb.LocalityComparisonType_UNDEFINED:              closedts.DefaultMaxNetworkRTT,
			},
		},
		{
			name:    "mixed valid and invalid latencies within same locality",
			nodeIDs: []roachpb.NodeID{1, 2, 3, 4, 5, 6},
			latencyMap: map[roachpb.NodeID]time.Duration{
				1: 5 * time.Millisecond,  // same zone - valid
				2: 0,                     // same zone - invalid
				3: 15 * time.Millisecond, // cross zone - valid
				4: 0,                     // cross zone - invalid
				5: 80 * time.Millisecond, // cross region - valid
				6: 0,                     // cross region - invalid
			},
			expectedRTTs: map[roachpb.LocalityComparisonType]time.Duration{
				roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE:  5 * time.Millisecond,
				roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE: 15 * time.Millisecond,
				roachpb.LocalityComparisonType_CROSS_REGION:           80 * time.Millisecond,
				roachpb.LocalityComparisonType_UNDEFINED:              closedts.DefaultMaxNetworkRTT,
			},
		},
		{
			name:    "some localities with all invalid latencies",
			nodeIDs: []roachpb.NodeID{1, 2, 3, 4, 5, 6},
			latencyMap: map[roachpb.NodeID]time.Duration{
				1: 5 * time.Millisecond,  // same zone - valid
				2: 3 * time.Millisecond,  // same zone - valid
				3: 0,                     // cross zone - invalid
				4: 0,                     // cross zone - invalid
				5: 80 * time.Millisecond, // cross region - valid
				6: 60 * time.Millisecond, // cross region - valid
			},
			expectedRTTs: map[roachpb.LocalityComparisonType]time.Duration{
				roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE:  5 * time.Millisecond,          // max of valid same zone
				roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE: closedts.DefaultMaxNetworkRTT, // all invalid
				roachpb.LocalityComparisonType_CROSS_REGION:           80 * time.Millisecond,         // max of valid cross region
				roachpb.LocalityComparisonType_UNDEFINED:              closedts.DefaultMaxNetworkRTT,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			getNodeDesc := func(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error) {
				if desc, ok := nodeDescs[nodeID]; ok {
					return desc, nil
				}
				return nil, errors.New("node not found")
			}
			getLatency := func(nodeID roachpb.NodeID) (time.Duration, bool) {
				if latency, ok := tc.latencyMap[nodeID]; ok {
					return latency, latency != 0 // Return false for zero latencies
				}
				return 0, false
			}

			lr := NewLatencyTracker(baseLocality, getLatency, getNodeDesc)
			lr.RefreshLatency(tc.nodeIDs)

			// Verify RTTs for all locality comparison types
			for lct := roachpb.LocalityComparisonType(0); lct < roachpb.LocalityComparisonType_MAX_LOCALITY_COMPARISON_TYPE; lct++ {
				expected := tc.expectedRTTs[lct]
				actual := lr.GetLatencyByLocalityProximity(lct)
				require.Equal(t, expected, actual,
					"incorrect RTT for locality comparison type %v", lct)
			}
		})
	}
}
