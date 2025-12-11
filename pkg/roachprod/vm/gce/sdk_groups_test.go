// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestInstanceTemplateNamePrefix(t *testing.T) {
	testCases := []struct {
		name        string
		clusterName string
		expected    string
	}{
		{
			name:        "simple cluster name",
			clusterName: "test-cluster",
			expected:    "test-cluster-template",
		},
		{
			name:        "cluster with numbers",
			clusterName: "cluster123",
			expected:    "cluster123-template",
		},
		{
			name:        "cluster with hyphens",
			clusterName: "my-test-cluster",
			expected:    "my-test-cluster-template",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := instanceTemplateNamePrefix(tc.clusterName)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestInstanceTemplateName(t *testing.T) {
	testCases := []struct {
		name        string
		clusterName string
		zone        string
		expected    string
	}{
		{
			name:        "us-east1-b zone",
			clusterName: "test-cluster",
			zone:        "us-east1-b",
			expected:    "test-cluster-template-us-east1-b",
		},
		{
			name:        "europe-west2-a zone",
			clusterName: "prod",
			zone:        "europe-west2-a",
			expected:    "prod-template-europe-west2-a",
		},
		{
			name:        "cluster with hyphens",
			clusterName: "my-cluster-name",
			zone:        "asia-southeast1-c",
			expected:    "my-cluster-name-template-asia-southeast1-c",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := instanceTemplateName(tc.clusterName, tc.zone)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestGetZoneFromTemplate(t *testing.T) {
	testCases := []struct {
		name         string
		template     *computepb.InstanceTemplate
		expectedZone string
	}{
		{
			name: "template with us-east1-b zone",
			template: &computepb.InstanceTemplate{
				Name: proto.String("test-cluster-template-us-east1-b"),
				Properties: &computepb.InstanceProperties{
					Labels: map[string]string{
						vm.TagCluster: "test-cluster",
					},
				},
			},
			expectedZone: "us-east1-b",
		},
		{
			name: "template with europe-west2-a zone",
			template: &computepb.InstanceTemplate{
				Name: proto.String("prod-template-europe-west2-a"),
				Properties: &computepb.InstanceProperties{
					Labels: map[string]string{
						vm.TagCluster: "prod",
					},
				},
			},
			expectedZone: "europe-west2-a",
		},
		{
			name: "template with cluster name with hyphens",
			template: &computepb.InstanceTemplate{
				Name: proto.String("my-test-cluster-template-asia-southeast1-c"),
				Properties: &computepb.InstanceProperties{
					Labels: map[string]string{
						vm.TagCluster: "my-test-cluster",
					},
				},
			},
			expectedZone: "asia-southeast1-c",
		},
	}

	p := &Provider{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := p.getZoneFromTemplate(tc.template)
			assert.Equal(t, tc.expectedZone, result)
		})
	}
}

func TestComputeGrowDistribution(t *testing.T) {
	testCases := []struct {
		name              string
		groups            []*computepb.InstanceGroupManager
		newNodeCount      int
		expectedAddCounts []int
	}{
		{
			name: "distribute evenly across 2 equal-sized groups",
			groups: []*computepb.InstanceGroupManager{
				{TargetSize: proto.Int32(5)},
				{TargetSize: proto.Int32(5)},
			},
			newNodeCount:      4,
			expectedAddCounts: []int{2, 2},
		},
		{
			name: "distribute to smallest group first",
			groups: []*computepb.InstanceGroupManager{
				{TargetSize: proto.Int32(3)}, // Smallest
				{TargetSize: proto.Int32(5)},
				{TargetSize: proto.Int32(5)},
			},
			newNodeCount:      6,
			expectedAddCounts: []int{4, 1, 1}, // Algorithm fills smallest until equal, then round-robins
		},
		{
			name: "single group",
			groups: []*computepb.InstanceGroupManager{
				{TargetSize: proto.Int32(10)},
			},
			newNodeCount:      5,
			expectedAddCounts: []int{5},
		},
		{
			name: "three groups, unequal sizes",
			groups: []*computepb.InstanceGroupManager{
				{TargetSize: proto.Int32(2)},
				{TargetSize: proto.Int32(4)},
				{TargetSize: proto.Int32(6)},
			},
			newNodeCount:      9,
			expectedAddCounts: []int{5, 3, 1}, // Fills smallest groups first
		},
		{
			name: "add zero nodes",
			groups: []*computepb.InstanceGroupManager{
				{TargetSize: proto.Int32(5)},
				{TargetSize: proto.Int32(5)},
			},
			newNodeCount:      0,
			expectedAddCounts: []int{0, 0},
		},
	}

	p := &Provider{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := p.computeGrowDistribution(tc.groups, tc.newNodeCount)
			require.Equal(t, len(tc.expectedAddCounts), len(result))
			assert.Equal(t, tc.expectedAddCounts, result)

			// Verify total adds equals newNodeCount
			totalAdds := 0
			for _, count := range result {
				totalAdds += count
			}
			assert.Equal(t, tc.newNodeCount, totalAdds)
		})
	}

	// Property-based testing (matching the original TestComputeGrowDistribution)
	// This tests with randomized inputs to ensure the distribution algorithm
	// maintains its invariants across a wide range of scenarios
	t.Run("property-based testing", func(t *testing.T) {
		rng, _ := randutil.NewTestRand()
		c := quick.Config{
			MaxCount: 128,
			Rand:     rng,
			Values: func(values []reflect.Value, r *rand.Rand) {
				// Generate random instance group sizes
				count := r.Intn(10) + 1 // 1-10 groups
				groups := make([]*computepb.InstanceGroupManager, count)
				for i := 0; i < count; i++ {
					groups[i] = &computepb.InstanceGroupManager{
						TargetSize: proto.Int32(int32(r.Intn(32))),
					}
				}
				values[0] = reflect.ValueOf(groups)
			},
		}

		testDistribution := func(groups []*computepb.InstanceGroupManager) bool {
			// Generate a random number of new nodes to add
			newNodeCount := rng.Intn(24) + 1

			// Compute the total number of nodes before the distribution and
			// the maximum distance between the number of nodes in the groups
			totalNodesBefore := 0
			curMax, curMin := 0.0, math.MaxFloat64
			for _, g := range groups {
				size := int(g.GetTargetSize())
				totalNodesBefore += size
				curMax = math.Max(curMax, float64(size))
				curMin = math.Min(curMin, float64(size))
			}
			maxDistanceBefore := curMax - curMin

			// Sort the groups by size (smallest first) as required by the algorithm
			sort.Slice(groups, func(i, j int) bool {
				return groups[i].GetTargetSize() < groups[j].GetTargetSize()
			})

			// Compute the new distribution
			p := &Provider{}
			newTargetSize := p.computeGrowDistribution(groups, newNodeCount)

			// Apply the distribution to the group sizes
			for idx := range newTargetSize {
				groups[idx].TargetSize = proto.Int32(groups[idx].GetTargetSize() + int32(newTargetSize[idx]))
			}

			// Compute the total number of nodes after the distribution and the maximum
			// distance between the number of nodes in the groups
			totalNodesAfter := 0
			curMax, curMin = 0.0, math.MaxFloat64
			for _, g := range groups {
				size := int(g.GetTargetSize())
				totalNodesAfter += size
				curMax = math.Max(curMax, float64(size))
				curMin = math.Min(curMin, float64(size))
			}
			maxDistanceAfter := curMax - curMin

			// The total number of nodes should be the sum of the new node count and the
			// total number of nodes before the distribution
			if totalNodesAfter != totalNodesBefore+newNodeCount {
				return false
			}

			// The maximum distance between the number of nodes in the groups should not
			// increase by more than 1, otherwise the new distribution was not fair
			if maxDistanceAfter > maxDistanceBefore+1.0 {
				return false
			}

			return true
		}

		if err := quick.Check(testDistribution, &c); err != nil {
			t.Error(err)
		}
	})
}
