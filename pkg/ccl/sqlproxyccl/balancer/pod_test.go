// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package balancer

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestSelectTenantPods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("no pods", func(t *testing.T) {
		require.Nil(t, selectTenantPod(0, nil, map[string]int{}))
	})

	t.Run("nil counts map", func(t *testing.T) {
		require.Nil(t, selectTenantPod(0, nil, nil))
	})

	t.Run("one pod", func(t *testing.T) {
		pod := selectTenantPod(
			0, []*tenant.Pod{{TenantID: 3, Addr: "1"}}, map[string]int{},
		)
		require.Equal(t, "1", pod.Addr)
	})

	t.Run("many pods", func(t *testing.T) {
		for _, update := range []bool{true, false} {
			t.Run(fmt.Sprintf("update=%v", update), func(t *testing.T) {
				counts := map[string]int{
					"1": 0,
					"2": 5,
					"3": 9,
				}

				pods := []*tenant.Pod{
					{TenantID: 10, Addr: "1"},
					{TenantID: 10, Addr: "2"},
					{TenantID: 10, Addr: "3"},
				}

				distribution := map[string]int{}
				rng := rand.New(rand.NewSource(0))

				for i := 0; i < 10000; i++ {
					pod := selectTenantPod(rng.Float32(), pods, counts)
					if update {
						// Simulate the case where the cache gets updated after.
						counts[pod.Addr] += 1
					}
					distribution[pod.Addr]++
				}

				if update {
					// Distribution should eventually converge to ~3333 each.
					require.Equal(t, map[string]int{
						"1": 3334,
						"2": 3272,
						"3": 3394,
					}, distribution)
				} else {
					// Assert that the distribution is a roughly based on the
					// following weights:
					// - w1: 45/59 (7627)
					// - w2: 9/59 (1525)
					// - w3: 5/59 (847)
					require.Equal(t, map[string]int{
						"1": 7507,
						"2": 1594,
						"3": 899,
					}, distribution)
				}
			})
		}
	})
}

func TestConstructPodWeights(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		name     string
		counts   []int
		expected []float32
	}{
		{
			name:     "single pod with zero weight",
			counts:   []int{0},
			expected: []float32{1},
		},
		{
			name:     "single pod",
			counts:   []int{100},
			expected: []float32{1},
		},
		{
			name:     "equal zero weights",
			counts:   []int{0, 0, 0},
			expected: []float32{0.333, 0.333, 0.333},
		},
		{
			name:     "equal weights",
			counts:   []int{1000, 1000},
			expected: []float32{0.5, 0.5},
		},
		{
			name:     "imbalance",
			counts:   []int{1, 4, 8},
			expected: []float32{0.727, 0.182, 0.091},
		},
		{
			name:     "new pod added",
			counts:   []int{10, 0},
			expected: []float32{0.091, 0.909},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.counts) != len(tc.expected) {
				t.Fatalf("counts != expected")
			}

			podCount := len(tc.counts)
			var pods []*tenant.Pod
			counts := make(map[string]int)
			expectedWeights := make(map[string]float32)
			for i := 0; i < podCount; i++ {
				addr := fmt.Sprintf("%d", i)
				counts[addr] = tc.counts[i]
				expectedWeights[addr] = tc.expected[i]
				pods = append(pods, &tenant.Pod{TenantID: 42, Addr: addr})
			}

			weights := constructPodWeights(pods, counts)
			require.InDeltaMapValues(t, expectedWeights, weights, 0.05)
		})
	}
}
