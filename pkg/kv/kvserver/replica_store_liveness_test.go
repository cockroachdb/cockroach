// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestRaftFortificationEnabledForRangeID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Ensure fracEnabled=0.0 never returns true.
	t.Run("fracEnabled=0.0", func(t *testing.T) {
		for i := range 10_000 {
			require.False(t, raftFortificationEnabledForRangeID(0.0, roachpb.RangeID(i)))
		}
	})

	// Ensure fracEnabled=1.0 always returns true.
	t.Run("fracEnabled=1.0", func(t *testing.T) {
		for i := range 10_000 {
			require.True(t, raftFortificationEnabledForRangeID(1.0, roachpb.RangeID(i)))
		}
	})

	// Test some specific cases with partially enabled fortification.
	testCases := []struct {
		fracEnabled float64
		rangeID     roachpb.RangeID
		exp         bool
	}{
		// 25% enabled.
		{0.25, 1, false},
		{0.25, 2, true},
		{0.25, 3, false},
		{0.25, 4, true},
		{0.25, 5, false},
		{0.25, 6, false},
		{0.25, 7, false},
		{0.25, 8, false},
		// 50% enabled.
		{0.50, 1, false},
		{0.50, 2, true},
		{0.50, 3, false},
		{0.50, 4, true},
		{0.50, 5, false},
		{0.50, 6, false},
		{0.50, 7, true},
		{0.50, 8, false},
		// 75% enabled.
		{0.75, 1, false},
		{0.75, 2, true},
		{0.75, 3, true},
		{0.75, 4, true},
		{0.75, 5, true},
		{0.75, 6, false},
		{0.75, 7, true},
		{0.75, 8, false},
	}
	for _, tc := range testCases {
		name := fmt.Sprintf("fracEnabled=%f/rangeID=%d", tc.fracEnabled, tc.rangeID)
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.exp, raftFortificationEnabledForRangeID(tc.fracEnabled, tc.rangeID))
		})
	}

	// Test with different fracEnabled precisions.
	for _, fracEnabled := range []float64{0.1, 0.01, 0.001} {
		t.Run(fmt.Sprintf("fracEnabled=%f", fracEnabled), func(t *testing.T) {
			enabled := 0
			const total = 100_000
			for i := range total {
				if raftFortificationEnabledForRangeID(fracEnabled, roachpb.RangeID(i)) {
					enabled++
				}
			}
			exp := int(fracEnabled * float64(total))
			require.InEpsilonf(t, exp, enabled, 0.1, "expected %d, got %d", exp, enabled)
		})
	}

	// Test panic on invalid fracEnabled.
	t.Run("fracEnabled=invalid", func(t *testing.T) {
		require.Panics(t, func() {
			raftFortificationEnabledForRangeID(-0.1, 1)
		})
		require.Panics(t, func() {
			raftFortificationEnabledForRangeID(1.1, 1)
		})
	})
}

func BenchmarkRaftFortificationEnabledForRangeID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = raftFortificationEnabledForRangeID(0.5, roachpb.RangeID(i))
	}
}
