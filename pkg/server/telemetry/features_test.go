// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package telemetry_test

import (
	"math"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
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
