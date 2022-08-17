// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/stretchr/testify/require"
)

func TestAllocationBenchPenaltyCalculation(t *testing.T) {
	varArray := func(vals ...float64) []float64 {
		return vals
	}

	makeStats := func(valsArrays ...[]float64) map[string]clusterstats.StatSummary {
		ret := map[string]clusterstats.StatSummary{}
		for i, vals := range valsArrays {
			ret[fmt.Sprintf("%d", i)] = clusterstats.StatSummary{Value: vals}
		}
		return ret
	}

	makePenalties := func(penalties ...float64) map[string]float64 {
		ret := map[string]float64{}
		for i, penalty := range penalties {
			ret[fmt.Sprintf("%d", i)] = penalty
		}
		return ret
	}

	testCases := []struct {
		desc       string
		stats      map[string]clusterstats.StatSummary
		thresholds map[string]float64
		expected   float64
	}{
		{
			desc:       "no penaty: no stats",
			stats:      makeStats(varArray()),
			thresholds: makePenalties(7),
			expected:   0,
		},
		{
			desc:       "no penalty: threshold=value",
			stats:      makeStats(varArray(7, 7, 7, 7, 7, 7, 7)),
			thresholds: makePenalties(7),
			expected:   0,
		},
		{
			desc:       "no penalty: inf threshold",
			stats:      makeStats(varArray(1000, 1, 9999, 7777, 2323)),
			thresholds: makePenalties(10e10),
			expected:   0,
		},
		{
			desc:       "max penalty: 0 threshold",
			stats:      makeStats(varArray(5, 4, 3, 2, 1)),
			thresholds: makePenalties(0),
			expected:   100,
		},
		{
			desc:       "half penalty",
			stats:      makeStats(varArray(8, 7, 6, 5, 4, 3)),
			thresholds: makePenalties(5),
			expected:   50,
		},
		{
			desc:       "2/3 penalty",
			stats:      makeStats(varArray(8, 7, 6, 5, 4, 3)),
			thresholds: makePenalties(4),
			expected:   66.67,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.expected, combinePenalties(tc.stats, tc.thresholds))
		})
	}
}
