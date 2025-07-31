// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package telemetry_test

import (
	"math"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

// TestGetCounterDoesNotRace ensures that concurrent calls to GetCounter for
// the same feature always returns the same pointer. Even when this race was
// possible this test would fail on every run but would fail rapidly under
// stressrace.
func TestGetCounterDoesNotRace(t *testing.T) {
	const N = 100
	var wg sync.WaitGroup
	wg.Add(N)
	counters := make([]telemetry.Counter, N)
	for i := 0; i < N; i++ {
		go func(i int) {
			counters[i] = telemetry.GetCounter("test.foo")
			wg.Done()
		}(i)
	}
	wg.Wait()
	counterSet := make(map[telemetry.Counter]struct{})
	for _, c := range counters {
		counterSet[c] = struct{}{}
	}
	require.Len(t, counterSet, 1)
}

// TestBucket checks integer quantization.
func TestBucket(t *testing.T) {
	testData := []struct {
		input    int64
		expected int64
	}{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 3},
		{4, 4},
		{5, 5},
		{6, 6},
		{7, 7},
		{8, 8},
		{9, 9},
		{10, 10},
		{11, 10},
		{20, 10},
		{99, 10},
		{100, 100},
		{101, 100},
		{200, 100},
		{999, 100},
		{1000, 1000},
		{math.MaxInt64, 1000000000000000000},
		{-1, -1},
		{-2, -2},
		{-3, -3},
		{-4, -4},
		{-5, -5},
		{-6, -6},
		{-7, -7},
		{-8, -8},
		{-9, -9},
		{-10, -10},
		{-11, -10},
		{-20, -10},
		{-100, -100},
		{-200, -100},
		{math.MinInt64, -1000000000000000000},
	}

	for _, tc := range testData {
		if actual, expected := telemetry.Bucket10(tc.input), tc.expected; actual != expected {
			t.Errorf("%d: expected %d, got %d", tc.input, expected, actual)
		}
	}
}

// TestCounterWithMetric verifies that only the telemetry is reset to zero when,
// for example, a report is created.
func TestCounterWithMetric(t *testing.T) {
	cm := telemetry.NewCounterWithMetric(metric.Metadata{Name: "test-metric"})
	cm.Inc()

	// Using GetFeatureCounts to read the telemetry value.
	m1 := telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ReadOnly)
	require.Equal(t, int32(1), m1["test-metric"])
	require.Equal(t, int64(1), cm.Count())

	// Reset the telemetry.
	telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ResetCounts)

	// Verify only the telemetry is back to 0.
	m2 := telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ReadOnly)
	require.Equal(t, int32(0), m2["test-metric"])
	require.Equal(t, int64(1), cm.Count())
}
