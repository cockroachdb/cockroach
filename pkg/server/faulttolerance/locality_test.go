// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package faulttolerance

import (
	"context"
	"iter"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

// testReplica is a mock implementation of the replicaDescriber interface.
type testReplica struct {
	desc *roachpb.RangeDescriptor
}

// Desc implements the replicaDescriber interface.
func (r *testReplica) Desc() *roachpb.RangeDescriptor {
	return r.desc
}

// createReplicaDescriptor creates a ReplicaDescriptor with the given node ID.
func createReplicaDescriptor(nodeID roachpb.NodeID) roachpb.ReplicaDescriptor {
	return roachpb.ReplicaDescriptor{
		NodeID: nodeID,
		Type:   roachpb.VOTER_FULL,
	}
}

// createTestReplica creates a testReplica with the given replica descriptors.
func createTestReplica(replicas ...roachpb.ReplicaDescriptor) *testReplica {
	desc := &roachpb.RangeDescriptor{
		InternalReplicas: replicas,
	}
	return &testReplica{desc: desc}
}

// TestMakeFailureDomains tests the makeFailureDomains function, which is the core
// of the ComputeFaultTolerance function.
func TestMakeFailureDomains(t *testing.T) {
	tests := []struct {
		name            string
		nodeLocalities  map[roachpb.NodeID]roachpb.Locality
		expectedDomains []string
	}{
		{
			name: "single region",
			nodeLocalities: map[roachpb.NodeID]roachpb.Locality{
				1: {Tiers: []roachpb.Tier{{Key: "region", Value: "us-east"}}},
				2: {Tiers: []roachpb.Tier{{Key: "region", Value: "us-east"}}},
				3: {Tiers: []roachpb.Tier{{Key: "region", Value: "us-east"}}},
			},
			expectedDomains: []string{
				"region=us-east",
			},
		},
		{
			name: "multiple regions",
			nodeLocalities: map[roachpb.NodeID]roachpb.Locality{
				1: {Tiers: []roachpb.Tier{{Key: "region", Value: "us-east"}}},
				2: {Tiers: []roachpb.Tier{{Key: "region", Value: "us-west"}}},
				3: {Tiers: []roachpb.Tier{{Key: "region", Value: "eu-west"}}},
			},
			expectedDomains: []string{
				"region=us-east",
				"region=us-west",
				"region=eu-west",
			},
		},
		{
			name: "hierarchical localities",
			nodeLocalities: map[roachpb.NodeID]roachpb.Locality{
				1: {Tiers: []roachpb.Tier{
					{Key: "region", Value: "us-east"},
					{Key: "zone", Value: "us-east-1"},
				}},
				2: {Tiers: []roachpb.Tier{
					{Key: "region", Value: "us-east"},
					{Key: "zone", Value: "us-east-2"},
				}},
				3: {Tiers: []roachpb.Tier{
					{Key: "region", Value: "us-west"},
					{Key: "zone", Value: "us-west-1"},
				}},
			},
			expectedDomains: []string{
				"region=us-east",
				"region=us-west",
				"region=us-east,zone=us-east-1",
				"region=us-east,zone=us-east-2",
				"region=us-west,zone=us-west-1",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Call makeFailureDomains with the test case's nodeLocalities
			domains := makeFailureDomains(tc.nodeLocalities)

			// Check that all expected domains are present
			for _, expectedDomain := range tc.expectedDomains {
				_, ok := domains[expectedDomain]
				require.True(t, ok, "Expected domain %q not found", expectedDomain)
			}

			// Check that there are no unexpected domains
			require.Equal(t, len(tc.expectedDomains), len(domains), "Unexpected number of domains")
		})
	}
}

func TestComputeFaultTolerance(t *testing.T) {
	tests := []struct {
		name           string
		nodeLocalities map[roachpb.NodeID]roachpb.Locality
		livenessMap    livenesspb.NodeVitalityMap
		replicas       []*testReplica
		expected       map[string]int
	}{
		{
			name: "all nodes live, single region",
			nodeLocalities: map[roachpb.NodeID]roachpb.Locality{
				1: {Tiers: []roachpb.Tier{{Key: "region", Value: "us-east"}}},
				2: {Tiers: []roachpb.Tier{{Key: "region", Value: "us-east"}}},
				3: {Tiers: []roachpb.Tier{{Key: "region", Value: "us-east"}}},
			},
			livenessMap: livenesspb.NodeVitalityMap{
				1: livenesspb.FakeNodeVitality(true),
				2: livenesspb.FakeNodeVitality(true),
				3: livenesspb.FakeNodeVitality(true),
			},
			replicas: []*testReplica{
				createTestReplica(
					createReplicaDescriptor(1),
					createReplicaDescriptor(2),
					createReplicaDescriptor(3),
				),
			},
			expected: map[string]int{
				"region=us-east": -2, // All nodes in same region, so failure of region means no quorum
			},
		},
		{
			name: "all nodes live, multiple regions",
			nodeLocalities: map[roachpb.NodeID]roachpb.Locality{
				1: {Tiers: []roachpb.Tier{{Key: "region", Value: "us-east"}}},
				2: {Tiers: []roachpb.Tier{{Key: "region", Value: "us-west"}}},
				3: {Tiers: []roachpb.Tier{{Key: "region", Value: "eu-west"}}},
			},
			livenessMap: livenesspb.NodeVitalityMap{
				1: livenesspb.FakeNodeVitality(true),
				2: livenesspb.FakeNodeVitality(true),
				3: livenesspb.FakeNodeVitality(true),
			},
			replicas: []*testReplica{
				createTestReplica(
					createReplicaDescriptor(1),
					createReplicaDescriptor(2),
					createReplicaDescriptor(3),
				),
			},
			expected: map[string]int{
				"region=us-east": 0, // Can lose us-east and still have quorum
				"region=us-west": 0, // Can lose us-west and still have quorum
				"region=eu-west": 0, // Can lose eu-west and still have quorum
			},
		},
		{
			name: "some nodes dead, multiple regions",
			nodeLocalities: map[roachpb.NodeID]roachpb.Locality{
				1: {Tiers: []roachpb.Tier{{Key: "region", Value: "us-east"}}},
				2: {Tiers: []roachpb.Tier{{Key: "region", Value: "us-west"}}},
				3: {Tiers: []roachpb.Tier{{Key: "region", Value: "eu-west"}}},
				4: {Tiers: []roachpb.Tier{{Key: "region", Value: "eu-west"}}},
			},
			livenessMap: livenesspb.NodeVitalityMap{
				1: livenesspb.FakeNodeVitality(true),
				2: livenesspb.FakeNodeVitality(true),
				3: livenesspb.FakeNodeVitality(false), // Node 3 is dead
				4: livenesspb.FakeNodeVitality(true),
			},
			replicas: []*testReplica{
				createTestReplica(
					createReplicaDescriptor(1),
					createReplicaDescriptor(2),
					createReplicaDescriptor(3),
				),
				createTestReplica(
					createReplicaDescriptor(1),
					createReplicaDescriptor(2),
					createReplicaDescriptor(4),
				),
			},
			expected: map[string]int{
				"region=us-east": -1, // With node 3 down, can't lose the replica in us-east
				"region=us-west": -1, // With node 3 down, can't lose the replica in us-west
				"region=eu-west": 0,  // We can tolerate node 3 and 4 down, since no range needs both nodes
			},
		},
		{
			name: "hierarchical localities",
			nodeLocalities: map[roachpb.NodeID]roachpb.Locality{
				1: {Tiers: []roachpb.Tier{
					{Key: "region", Value: "us-east"},
					{Key: "zone", Value: "us-east-1"},
				}},
				2: {Tiers: []roachpb.Tier{
					{Key: "region", Value: "us-east"},
					{Key: "zone", Value: "us-east-2"},
				}},
				3: {Tiers: []roachpb.Tier{
					{Key: "region", Value: "us-west"},
					{Key: "zone", Value: "us-west-1"},
				}},
			},
			livenessMap: livenesspb.NodeVitalityMap{
				1: livenesspb.FakeNodeVitality(true),
				2: livenesspb.FakeNodeVitality(true),
				3: livenesspb.FakeNodeVitality(true),
			},
			replicas: []*testReplica{
				createTestReplica(
					createReplicaDescriptor(1),
					createReplicaDescriptor(2),
					createReplicaDescriptor(3),
				),
			},
			expected: map[string]int{
				"region=us-east":                -1, // Losing us-east means losing 2 nodes, which breaks quorum
				"region=us-west":                0,  // Can lose us-west and still have quorum
				"region=us-east,zone=us-east-1": 0,  // Can lose us-east-1 and still have quorum
				"region=us-east,zone=us-east-2": 0,  // Can lose us-east-2 and still have quorum
				"region=us-west,zone=us-west-1": 0,  // Can lose us-west-1 and still have quorum
			},
		},
		{
			name: "multiple replicas with different margins",
			nodeLocalities: map[roachpb.NodeID]roachpb.Locality{
				1: {Tiers: []roachpb.Tier{{Key: "region", Value: "us-east"}}},
				2: {Tiers: []roachpb.Tier{{Key: "region", Value: "us-west"}}},
				3: {Tiers: []roachpb.Tier{{Key: "region", Value: "eu-west"}}},
				4: {Tiers: []roachpb.Tier{{Key: "region", Value: "ap-east"}}},
			},
			livenessMap: livenesspb.NodeVitalityMap{
				1: livenesspb.FakeNodeVitality(true),
				2: livenesspb.FakeNodeVitality(true),
				3: livenesspb.FakeNodeVitality(true),
				4: livenesspb.FakeNodeVitality(false),
			},
			replicas: []*testReplica{
				// First replica with nodes 1, 2, 3
				createTestReplica(
					createReplicaDescriptor(1),
					createReplicaDescriptor(2),
					createReplicaDescriptor(3),
				),
				// Second replica with nodes 2, 3, 4
				createTestReplica(
					createReplicaDescriptor(2),
					createReplicaDescriptor(3),
					createReplicaDescriptor(4),
				),
			},
			expected: map[string]int{
				"region=us-east": 0,  // Can lose us-east in both replicas
				"region=us-west": -1, // Losing us-west would break quorum in second replica
				"region=eu-west": -1, // Losing eu-west would break quorum in second replica
				"region=ap-east": 0,  // Can lose ap-east in both replicas
			},
		},
		{
			name: "region plus node fault tolerance",
			nodeLocalities: map[roachpb.NodeID]roachpb.Locality{
				1: {Tiers: []roachpb.Tier{{Key: "region", Value: "us-east"}}},
				2: {Tiers: []roachpb.Tier{{Key: "region", Value: "us-east"}}},
				3: {Tiers: []roachpb.Tier{{Key: "region", Value: "us-east"}}},
				4: {Tiers: []roachpb.Tier{{Key: "region", Value: "us-west"}}},
				5: {Tiers: []roachpb.Tier{{Key: "region", Value: "us-west"}}},
				6: {Tiers: []roachpb.Tier{{Key: "region", Value: "us-west"}}},
				7: {Tiers: []roachpb.Tier{{Key: "region", Value: "eu-west"}}},
				8: {Tiers: []roachpb.Tier{{Key: "region", Value: "eu-west"}}},
				9: {Tiers: []roachpb.Tier{{Key: "region", Value: "eu-west"}}},
			},
			livenessMap: livenesspb.TestCreateNodeVitality(1, 2, 3, 4, 5, 6, 7, 8, 9).ScanNodeVitalityFromCache(),
			replicas: []*testReplica{
				createTestReplica(
					createReplicaDescriptor(1),
					createReplicaDescriptor(2),
					createReplicaDescriptor(3),
					createReplicaDescriptor(4),
					createReplicaDescriptor(5),
					createReplicaDescriptor(6),
					createReplicaDescriptor(7),
					createReplicaDescriptor(8),
					createReplicaDescriptor(9),
				),
			},
			expected: map[string]int{
				"region=us-east": 1, // Can lose each region, and one other node at RF=9
				"region=us-west": 1,
				"region=eu-west": 1,
			},
		},
	}

	// Create a function that satisfies the iter.Seq[replicaDescriber] interface
	// by using the testReplica objects directly.
	mockReplicaIterator := func(replicas []*testReplica) iter.Seq[replicaDescriber] {
		return func(yield func(replicaDescriber) bool) {
			for _, r := range replicas {
				if !yield(r) {
					break
				}
			}
		}
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			// Create a sequence of replicas for testing
			replicaSeq := mockReplicaIterator(tc.replicas)

			// Call computeFaultToleranceImpl directly
			result, err := computeFaultToleranceImpl(ctx, tc.livenessMap, tc.nodeLocalities, replicaSeq)
			require.NoError(t, err)

			// Verify the results
			require.Equal(t, tc.expected, result)
		})
	}
}
