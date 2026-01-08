// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeCPUCores_ToRateCapacityNanos(t *testing.T) {
	testCases := []struct {
		name     string
		cores    NodeCPUCores
		expected NodeCPURateCapacities
	}{
		{
			name:     "empty",
			cores:    NodeCPUCores{},
			expected: NodeCPURateCapacities{},
		},
		{
			name:     "single_core",
			cores:    NodeCPUCores{1.0},
			expected: NodeCPURateCapacities{1e9},
		},
		{
			name:     "multiple_cores",
			cores:    NodeCPUCores{1.0, 2.0, 3.0},
			expected: NodeCPURateCapacities{1e9, 2e9, 3e9},
		},
		{
			name:     "fractional_cores",
			cores:    NodeCPUCores{1.5, 2.75, 3.25},
			expected: NodeCPURateCapacities{1500e6, 2750e6, 3250e6},
		},
		{
			name:     "round_down_to_nearest_nanosecond",
			cores:    NodeCPUCores{1.4999999998, 2.1111111111999},
			expected: NodeCPURateCapacities{1499999999, 2111111111},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.cores.ToRateCapacityNanos())
		})
	}
}
