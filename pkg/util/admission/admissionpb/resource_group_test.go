// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admissionpb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalize(t *testing.T) {
	tests := []struct {
		name              string
		weights           []int64
		expectedBurstFrac []float64
	}{
		{
			name:              "empty",
			weights:           nil,
			expectedBurstFrac: nil,
		},
		{
			name:              "single group below floor",
			weights:           []int64{50},
			expectedBurstFrac: []float64{1.0},
		},
		{
			name:              "sum below 100 still normalized",
			weights:           []int64{20, 20},
			expectedBurstFrac: []float64{0.5, 0.5},
		},
		{
			name:              "sum equals 100",
			weights:           []int64{80, 20},
			expectedBurstFrac: []float64{0.8, 0.2},
		},
		{
			name:              "sum above 100",
			weights:           []int64{100, 100, 50},
			expectedBurstFrac: []float64{100.0 / 250.0, 100.0 / 250.0, 50.0 / 250.0},
		},
		{
			// Non-positive CPUWeight is rejected at the SQL boundary. The
			// helper still must not panic on divide-by-zero if it ever sees
			// such input: a single zero-weight group yields BurstFrac=0,
			// not a NaN.
			name:              "non-positive weight tolerated",
			weights:           []int64{0},
			expectedBurstFrac: []float64{0},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfgs := make([]ResourceGroupConfig, len(tc.weights))
			for i, w := range tc.weights {
				cfgs[i] = ResourceGroupConfig{CPUWeight: w}
			}
			got := Normalize(cfgs)
			require.Len(t, got, len(tc.expectedBurstFrac))
			for i, want := range tc.expectedBurstFrac {
				require.InDelta(t, want, got[i].BurstFrac, 1e-9,
					"index %d, weight %d", i, tc.weights[i])
			}
		})
	}
}
