// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
)

func TestNodeCPURateCapacities_String(t *testing.T) {
	testCases := []struct {
		name     string
		capacity NodeCPURateCapacities
	}{
		{
			name:     "empty",
			capacity: NodeCPURateCapacities{},
		},
		{
			name:     "single_capacity",
			capacity: NodeCPURateCapacities{uint64(time.Second.Nanoseconds())},
		},
		{
			name: "multiple_capacities",
			capacity: NodeCPURateCapacities{
				uint64(2 * time.Second.Nanoseconds()),
				uint64(4 * time.Second.Nanoseconds()),
				uint64(8 * time.Second.Nanoseconds()),
			},
		},
		{
			name: "fractional_seconds",
			capacity: NodeCPURateCapacities{
				uint64(500 * time.Millisecond.Nanoseconds()),
				uint64(1500 * time.Millisecond.Nanoseconds()),
			},
		},
		{
			name: "mixed_exact_and_fractional",
			capacity: NodeCPURateCapacities{
				uint64(time.Second.Nanoseconds()),            // 1.00
				uint64(500 * time.Millisecond.Nanoseconds()), // 0.50
				uint64(2 * time.Second.Nanoseconds()),        // 2.00
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			echotest.Require(t, tc.capacity.String(), filepath.Join("testdata", "NodeCPURateCapacities_String", tc.name+".txt"))
		})
	}
}
