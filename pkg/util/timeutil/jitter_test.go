// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestJitter(t *testing.T) {
	tests := []struct {
		name             string
		interval         time.Duration
		percentage       float64
		randFloat        func() float64
		expectedDuration time.Duration
	}{
		{
			name:             "no jitter",
			interval:         time.Second,
			percentage:       0,
			randFloat:        func() float64 { return 0.5 },
			expectedDuration: time.Second,
		},
		{
			name:             "max jitter with rand=0",
			interval:         time.Second,
			percentage:       1.0,
			randFloat:        func() float64 { return 0 },
			expectedDuration: time.Duration(0),
		},
		{
			name:             "max jitter with rand=1",
			interval:         time.Second,
			percentage:       1.0,
			randFloat:        func() float64 { return 1.0 },
			expectedDuration: time.Second * 2,
		},
		{
			name:             "50% jitter with rand=0",
			interval:         time.Second,
			percentage:       0.5,
			randFloat:        func() float64 { return 0 },
			expectedDuration: time.Second / 2,
		},
		{
			name:             "50% jitter with rand=1",
			interval:         time.Second,
			percentage:       0.5,
			randFloat:        func() float64 { return 1.0 },
			expectedDuration: time.Second * 3 / 2,
		},
		{
			name:             "50% jitter with rand=0.5",
			interval:         time.Second,
			percentage:       0.5,
			randFloat:        func() float64 { return 0.5 },
			expectedDuration: time.Second,
		},
		{
			name:             "small interval",
			interval:         time.Nanosecond,
			percentage:       0.5,
			randFloat:        func() float64 { return 0.5 },
			expectedDuration: time.Nanosecond,
		},
		{
			name:             "large interval",
			interval:         time.Hour,
			percentage:       0.25,
			randFloat:        func() float64 { return 0.75 },
			expectedDuration: time.Duration(1.125 * float64(time.Hour)),
		},
		{
			name:             "negative interval with positive jitter",
			interval:         -time.Second,
			percentage:       0.5,
			randFloat:        func() float64 { return 0.5 },
			expectedDuration: -time.Second,
		},
		{
			name:             "custom jitter percentage",
			interval:         time.Minute,
			percentage:       0.3,
			randFloat:        func() float64 { return 0.75 },
			expectedDuration: time.Minute * 115 / 100, // 1 - 0.3 + 2*0.3*0.75 = 0.7 + 0.45 = 1.15
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := timeutil.JitterWithRand(tc.interval, tc.percentage, tc.randFloat)
			require.Equal(t, tc.expectedDuration, result)
		})
	}
}

func TestJitterNegativePercentagePanics(t *testing.T) {
	require.Panics(t, func() {
		timeutil.JitterWithRand(time.Second, -0.1, func() float64 { return 0.5 })
	})
}

func TestJitterPercentageGreaterThanOnePanics(t *testing.T) {
	require.Panics(t, func() {
		timeutil.JitterWithRand(time.Second, 1.1, func() float64 { return 0.5 })
	})
}

func TestJitterNoRand(t *testing.T) {
	// Basic test for the Jitter function
	// We can't test the exact output since it uses random values,
	// but we can check that it doesn't panic and returns a duration
	result := timeutil.Jitter(time.Second, 0.5)
	require.GreaterOrEqual(t, result, time.Second/2)
	require.LessOrEqual(t, result, time.Second*3/2)
}
