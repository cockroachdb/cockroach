// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComputeMaxFractionPendingIncDec(t *testing.T) {
	tests := []struct {
		name           string
		reported       LoadVector
		adjusted       LoadVector
		expectedMaxInc float64
		expectedMaxDec float64
	}{
		{
			name:           "both_zero",
			reported:       LoadVector{},
			adjusted:       LoadVector{},
			expectedMaxInc: 0,
			expectedMaxDec: 0,
		},
		{
			name:           "reported_zero_adjusted_nonzero",
			reported:       LoadVector{},
			adjusted:       LoadVector{CPURate: 100, WriteBandwidth: 200, ByteSize: 300},
			expectedMaxInc: 1000,
			expectedMaxDec: 1000,
		},
		{
			name:           "reported_nonzero_adjusted_zero",
			reported:       LoadVector{CPURate: 100, WriteBandwidth: 200, ByteSize: 300},
			adjusted:       LoadVector{CPURate: 0, WriteBandwidth: 0, ByteSize: 0},
			expectedMaxInc: 0,
			expectedMaxDec: 1.0, // abs((0-100)/100) = 1.0, same for all dimensions
		},
		{
			name:           "simple_increase",
			reported:       LoadVector{CPURate: 100},
			adjusted:       LoadVector{CPURate: 150},
			expectedMaxInc: 0.5,
			expectedMaxDec: 0,
		},
		{
			name:           "simple_decrease",
			reported:       LoadVector{CPURate: 100},
			adjusted:       LoadVector{CPURate: 50},
			expectedMaxInc: 0,
			expectedMaxDec: 0.5,
		},
		{
			name:           "multiple_dimensions_increase",
			reported:       LoadVector{CPURate: 100, WriteBandwidth: 200, ByteSize: 300},
			adjusted:       LoadVector{CPURate: 150, WriteBandwidth: 250, ByteSize: 400},
			expectedMaxInc: 0.5, // max(0.5, 0.25, 0.333...)
			expectedMaxDec: 0,
		},
		{
			name:           "multiple_dimensions_decrease",
			reported:       LoadVector{CPURate: 100, WriteBandwidth: 200, ByteSize: 300},
			adjusted:       LoadVector{CPURate: 50, WriteBandwidth: 100, ByteSize: 150},
			expectedMaxInc: 0,
			expectedMaxDec: 0.5, // max(0.5, 0.5, 0.5)
		},
		{
			name:           "mixed_increase_and_decrease",
			reported:       LoadVector{CPURate: 100, WriteBandwidth: 200, ByteSize: 300},
			adjusted:       LoadVector{CPURate: 150, WriteBandwidth: 100, ByteSize: 300},
			expectedMaxInc: 0.5, // from CPURate
			expectedMaxDec: 0.5, // from WriteBandwidth
		},
		{
			name:           "negative_adjusted_load",
			reported:       LoadVector{CPURate: 100},
			adjusted:       LoadVector{CPURate: -50},
			expectedMaxInc: 0,
			expectedMaxDec: 1.5, // abs(-50 - 100) / 100 = 1.5
		},
		{
			name:           "fractional_change",
			reported:       LoadVector{CPURate: 1000},
			adjusted:       LoadVector{CPURate: 1100},
			expectedMaxInc: 0.1,
			expectedMaxDec: 0,
		},
		{
			name:           "max_across_dimensions",
			reported:       LoadVector{CPURate: 100, WriteBandwidth: 200, ByteSize: 300},
			adjusted:       LoadVector{CPURate: 120, WriteBandwidth: 300, ByteSize: 450},
			expectedMaxInc: 0.5, // max(0.2, 0.5, 0.5)
			expectedMaxDec: 0,
		},
		{
			name:           "one_dimension_zero_others_change",
			reported:       LoadVector{CPURate: 0, WriteBandwidth: 100, ByteSize: 200},
			adjusted:       LoadVector{CPURate: 50, WriteBandwidth: 150, ByteSize: 100},
			expectedMaxInc: 1000, // from CPURate (reported=0)
			expectedMaxDec: 1000, // from CPURate (reported=0)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			maxInc, maxDec := computeMaxFractionPendingIncDec(tt.reported, tt.adjusted)
			require.InDelta(t, tt.expectedMaxInc, maxInc, 1e-9, "max fraction increase")
			require.InDelta(t, tt.expectedMaxDec, maxDec, 1e-9, "max fraction decrease")
		})
	}
}
