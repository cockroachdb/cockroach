// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package replicastats

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func floatsEqual(x, y float64) bool {
	diff := math.Abs(y - x)
	return diff < 0.00000001
}

func floatMapsEqual(expected, actual map[string]float64) bool {
	if len(expected) != len(actual) {
		return false
	}
	for k, v1 := range expected {
		v2, ok := actual[k]
		if !ok {
			return false
		}
		if !floatsEqual(v1, v2) {
			return false
		}
	}
	return true
}

func TestReplicaStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	clock := hlc.NewClock(manual, time.Nanosecond /* maxOffset */)

	gceLocalities := map[roachpb.NodeID]string{
		1: "region=us-east1,zone=us-east1-a",
		2: "region=us-east1,zone=us-east1-b",
		3: "region=us-west1,zone=us-west1-a",
		4: "",
	}
	mismatchedLocalities := map[roachpb.NodeID]string{
		1: "region=us-east1,zone=a",
		2: "region=us-east1,zone=b",
		3: "region=us-west1,zone=a",
		4: "zone=us-central1-a",
	}
	missingLocalities := map[roachpb.NodeID]string{}

	testCases := []struct {
		localities map[roachpb.NodeID]string
		reqs       []roachpb.NodeID
		expected   PerLocalityCounts
	}{
		{
			localities: gceLocalities,
			reqs:       []roachpb.NodeID{},
			expected:   PerLocalityCounts{},
		},
		{
			localities: gceLocalities,
			reqs:       []roachpb.NodeID{1, 1, 1},
			expected: PerLocalityCounts{
				"region=us-east1,zone=us-east1-a": 3,
			},
		},
		{
			localities: gceLocalities,
			reqs:       []roachpb.NodeID{1, 2, 3},
			expected: PerLocalityCounts{
				"region=us-east1,zone=us-east1-a": 1,
				"region=us-east1,zone=us-east1-b": 1,
				"region=us-west1,zone=us-west1-a": 1,
			},
		},
		{
			localities: gceLocalities,
			reqs:       []roachpb.NodeID{4, 5, 6},
			expected: PerLocalityCounts{
				"": 3,
			},
		},
		{
			localities: gceLocalities,
			reqs:       []roachpb.NodeID{1, 4, 2, 5, 3, 6},
			expected: PerLocalityCounts{
				"region=us-east1,zone=us-east1-a": 1,
				"region=us-east1,zone=us-east1-b": 1,
				"region=us-west1,zone=us-west1-a": 1,
				"":                                3,
			},
		},
		{
			localities: mismatchedLocalities,
			reqs:       []roachpb.NodeID{},
			expected:   PerLocalityCounts{},
		},
		{
			localities: mismatchedLocalities,
			reqs:       []roachpb.NodeID{1, 1, 1},
			expected: PerLocalityCounts{
				"region=us-east1,zone=a": 3,
			},
		},
		{
			localities: mismatchedLocalities,
			reqs:       []roachpb.NodeID{1, 2, 3, 4},
			expected: PerLocalityCounts{
				"region=us-east1,zone=a": 1,
				"region=us-east1,zone=b": 1,
				"region=us-west1,zone=a": 1,
				"zone=us-central1-a":     1,
			},
		},
		{
			localities: mismatchedLocalities,
			reqs:       []roachpb.NodeID{4, 5, 6},
			expected: PerLocalityCounts{
				"zone=us-central1-a": 1,
				"":                   2,
			},
		},
		{
			localities: mismatchedLocalities,
			reqs:       []roachpb.NodeID{1, 4, 2, 5, 3, 6},
			expected: PerLocalityCounts{
				"region=us-east1,zone=a": 1,
				"region=us-east1,zone=b": 1,
				"region=us-west1,zone=a": 1,
				"zone=us-central1-a":     1,
				"":                       2,
			},
		},
		{
			localities: missingLocalities,
			reqs:       []roachpb.NodeID{},
			expected:   PerLocalityCounts{},
		},
		{
			localities: missingLocalities,
			reqs:       []roachpb.NodeID{1, 1, 1},
			expected: PerLocalityCounts{
				"": 3,
			},
		},
		{
			localities: missingLocalities,
			reqs:       []roachpb.NodeID{1, 2, 3, 4, 5, 6},
			expected: PerLocalityCounts{
				"": 6,
			},
		},
	}
	for i, tc := range testCases {
		rs := NewReplicaStats(clock, func(nodeID roachpb.NodeID) string {
			return tc.localities[nodeID]
		})
		for _, req := range tc.reqs {
			rs.RecordCount(1, req)
		}
		manual.Advance(time.Second)
		if actual, _ := rs.PerLocalityDecayingRate(); !floatMapsEqual(tc.expected, actual) {
			t.Errorf("%d: incorrect per-locality QPS averages: %s", i, pretty.Diff(tc.expected, actual))
		}
		var expectedAvgQPS float64
		for _, v := range tc.expected {
			expectedAvgQPS += v
		}
		if actual, _ := rs.AverageRatePerSecond(); actual != expectedAvgQPS {
			t.Errorf("%d: avgQPS() got %f, want %f", i, actual, expectedAvgQPS)
		}
		// Verify that QPS numbers get cut in half after another second.
		manual.Advance(time.Second)
		for k, v := range tc.expected {
			tc.expected[k] = v / 2
		}
		if actual, _ := rs.PerLocalityDecayingRate(); !floatMapsEqual(tc.expected, actual) {
			t.Errorf("%d: incorrect per-locality QPS averages: %s", i, pretty.Diff(tc.expected, actual))
		}
		expectedAvgQPS /= 2
		if actual, _ := rs.AverageRatePerSecond(); actual != expectedAvgQPS {
			t.Errorf("%d: avgQPS() got %f, want %f", i, actual, expectedAvgQPS)
		}
		rs.ResetRequestCounts()
		if actual, _ := rs.SumLocked(); actual != 0 {
			t.Errorf("%d: unexpected non-empty QPS averages after resetting: %+v", i, actual)
		}
	}
}

func TestReplicaStatsDecay(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	clock := hlc.NewClock(manual, time.Nanosecond /* maxOffset */)

	awsLocalities := map[roachpb.NodeID]string{
		1: "region=us-east-1,zone=us-east-1a",
		2: "region=us-east-1,zone=us-east-1b",
		3: "region=us-west-1,zone=us-west-1a",
	}

	rs := NewReplicaStats(clock, func(nodeID roachpb.NodeID) string {
		return awsLocalities[nodeID]
	})

	{
		counts, dur := rs.PerLocalityDecayingRate()
		if len(counts) != 0 {
			t.Errorf("expected empty request counts, got %+v", counts)
		}
		if dur != 0 {
			t.Errorf("expected duration = 0, got %v", dur)
		}
		manual.Advance(1)
		if _, dur := rs.PerLocalityDecayingRate(); dur != 1 {
			t.Errorf("expected duration = 1, got %v", dur)
		}
		rs.ResetRequestCounts()
	}

	{
		for _, req := range []roachpb.NodeID{1, 1, 2, 2, 3} {
			rs.RecordCount(1, req)
		}
		counts := PerLocalityCounts{
			awsLocalities[1]: 2,
			awsLocalities[2]: 2,
			awsLocalities[3]: 1,
		}
		actual, dur := rs.PerLocalityDecayingRate()
		if dur != 0 {
			t.Errorf("expected duration = 0, got %v", dur)
		}
		if !reflect.DeepEqual(counts, actual) {
			t.Errorf("incorrect per-locality request counts: %s", pretty.Diff(counts, actual))
		}

		var totalDuration time.Duration
		for i := 0; i < len(rs.Mu.records)-1; i++ {
			manual.Advance(replStatsRotateInterval)
			totalDuration = time.Duration(float64(replStatsRotateInterval+totalDuration) * decayFactor)
			expected := make(PerLocalityCounts)
			for k, v := range counts {
				counts[k] = v * decayFactor
				expected[k] = counts[k] / totalDuration.Seconds()
			}
			actual, dur = rs.PerLocalityDecayingRate()
			if expectedDur := replStatsRotateInterval * time.Duration(i+1); dur != expectedDur {
				t.Errorf("expected duration = %v, got %v", expectedDur, dur)
			}
			// We can't just use DeepEqual to compare these due to the float
			// multiplication inaccuracies.
			if !floatMapsEqual(expected, actual) {
				t.Errorf("%d: incorrect per-locality request counts: %s", i, pretty.Diff(expected, actual))
			}
		}

		manual.Advance(replStatsRotateInterval)
		expected := make(PerLocalityCounts)
		if actual, _ := rs.PerLocalityDecayingRate(); !reflect.DeepEqual(expected, actual) {
			t.Errorf("incorrect per-locality request counts: %s", pretty.Diff(expected, actual))
		}
		rs.ResetRequestCounts()
	}

	{
		for _, req := range []roachpb.NodeID{1, 1, 2, 2, 3} {
			rs.RecordCount(1, req)
		}
		manual.Advance(replStatsRotateInterval)
		for _, req := range []roachpb.NodeID{2, 2, 3, 3, 3} {
			rs.RecordCount(1, req)
		}
		durationDivisor := time.Duration(float64(replStatsRotateInterval) * decayFactor).Seconds()
		expected := PerLocalityCounts{
			// We expect the first loop's requests to be decreased by decayFactor,
			// but not the second loop's.
			awsLocalities[1]: 2 * decayFactor / durationDivisor,
			awsLocalities[2]: (2*decayFactor + 2) / durationDivisor,
			awsLocalities[3]: (1*decayFactor + 3) / durationDivisor,
		}
		if actual, _ := rs.PerLocalityDecayingRate(); !reflect.DeepEqual(expected, actual) {
			t.Errorf("incorrect per-locality request counts: %s", pretty.Diff(expected, actual))
		}
	}
}

// TestReplicaStatsDecaySmoothing verifies that there is a smooth decrease
// in request counts over time rather than a massive drop when the count
// windows get rotated.
func TestReplicaStatsDecaySmoothing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	clock := hlc.NewClock(manual, time.Nanosecond /* maxOffset */)
	awsLocalities := map[roachpb.NodeID]string{
		1: "region=us-east-1,zone=us-east-1a",
		2: "region=us-east-1,zone=us-east-1b",
		3: "region=us-west-1,zone=us-west-1a",
	}
	rs := NewReplicaStats(clock, func(nodeID roachpb.NodeID) string {
		return awsLocalities[nodeID]
	})
	rs.RecordCount(1, 1)
	rs.RecordCount(1, 1)
	rs.RecordCount(1, 2)
	rs.RecordCount(1, 2)
	rs.RecordCount(1, 3)
	expected := PerLocalityCounts{
		awsLocalities[1]: 2,
		awsLocalities[2]: 2,
		awsLocalities[3]: 1,
	}
	if actual, _ := rs.PerLocalityDecayingRate(); !reflect.DeepEqual(expected, actual) {
		t.Errorf("incorrect per-locality request counts: %s", pretty.Diff(expected, actual))
	}

	increment := replStatsRotateInterval / 2
	manual.Advance(increment)
	actual1, dur := rs.PerLocalityDecayingRate()
	if dur != increment {
		t.Errorf("expected duration = %v; got %v", increment, dur)
	}
	for k := range expected {
		expected[k] /= increment.Seconds()
	}
	if !floatMapsEqual(expected, actual1) {
		t.Errorf("incorrect per-locality request counts: %s", pretty.Diff(expected, actual1))
	}

	// Verify that all values decrease as time advances if no requests come in.
	manual.Advance(1)
	actual2, _ := rs.PerLocalityDecayingRate()
	if len(actual1) != len(actual2) {
		t.Fatalf("unexpected different results sizes (expected %d, got %d)", len(actual1), len(actual2))
	}
	for k := range actual1 {
		if actual2[k] >= actual1[k] {
			t.Errorf("expected newer count %f to be smaller than older count %f", actual2[k], actual2[k])
		}
	}

	// Ditto for passing a window boundary.
	manual.Advance(increment)
	actual3, _ := rs.PerLocalityDecayingRate()
	if len(actual2) != len(actual3) {
		t.Fatalf("unexpected different results sizes (expected %d, got %d)", len(actual2), len(actual3))
	}
	for k := range actual2 {
		if actual3[k] >= actual2[k] {
			t.Errorf("expected newer count %f to be smaller than older count %f", actual3[k], actual3[k])
		}
	}
}

func genTestingReplicaStats(windowedMultipliers []int, n, offset int) *ReplicaStats {
	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	clock := hlc.NewClock(manual, time.Nanosecond /* maxOffset */)
	awsLocalities := map[roachpb.NodeID]string{
		1: "region=us-east-1,zone=us-east-1a",
		2: "region=us-east-1,zone=us-east-1b",
		3: "region=us-west-1,zone=us-west-1a",
	}
	rs := NewReplicaStats(clock, func(nodeID roachpb.NodeID) string {
		return awsLocalities[nodeID]
	})

	for i := 0; i < offset; i++ {
		manual.Advance(replStatsRotateInterval)
	}

	// Here we generate recorded counts against the three localities. For
	// simplicity, each locality has 1/5, 2/5 and 3/5 of the aggregate count
	// recorded against it respectively.
	for _, multiplier := range windowedMultipliers {
		for i := 1; i <= n; i++ {
			rs.RecordCount(float64(i*1*multiplier), 1)
			rs.RecordCount(float64(i*2*multiplier), 2)
			rs.RecordCount(float64(i*3*multiplier), 3)
		}
		// rotate the window
		manual.Advance(replStatsRotateInterval)
	}
	return rs
}

// TestReplicaStatsSplit asserts that splitting replica stats distributes the
// recorded counts evenly among the split stats. It assigns the same maximum
// and minimum for both halves of the split.
func TestReplicaStatsSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	windowedMultipliersInitial := []int{10, 20, 30, 40, 50, 60}
	windowedMultipliersSplit := []int{5, 10, 15, 20, 25, 30}
	nilMultipliers := []int{}

	testCases := []struct {
		expectedSplit  []int
		windowsInitial []int
		rotation       int
	}{
		{
			expectedSplit:  windowedMultipliersSplit,
			windowsInitial: windowedMultipliersInitial,
			rotation:       0,
		},
		{
			expectedSplit:  windowedMultipliersSplit,
			windowsInitial: windowedMultipliersInitial,
			rotation:       3,
		},
		{
			expectedSplit:  windowedMultipliersSplit,
			windowsInitial: windowedMultipliersInitial,
			rotation:       50,
		},
		{
			expectedSplit:  windowedMultipliersSplit[:0],
			windowsInitial: windowedMultipliersInitial[:0],
			rotation:       0,
		},
		{
			expectedSplit:  windowedMultipliersSplit[:2],
			windowsInitial: windowedMultipliersInitial[:2],
			rotation:       0,
		},
	}

	for _, tc := range testCases {
		initial := genTestingReplicaStats(tc.windowsInitial, 10, tc.rotation)
		expected := genTestingReplicaStats(tc.expectedSplit, 10, tc.rotation)
		otherHalf := genTestingReplicaStats(nilMultipliers, 0, 0)
		n := len(initial.Mu.records)

		// Adjust the max/min/count, since the generated replica stats will have
		// max/min that is too high and missing half the counts.
		for i := range initial.Mu.records {
			idx := (initial.Mu.idx + n - i) % n

			// We don't want to adjust the aggregates when the initial window
			// is uninitialzed.
			if initial.Mu.records[idx] == nil {
				continue
			}

			expected.Mu.records[idx].count = initial.Mu.records[idx].count / 2
			expected.Mu.records[idx].max = initial.Mu.records[idx].max
			expected.Mu.records[idx].min = initial.Mu.records[idx].min
		}

		initial.SplitRequestCounts(otherHalf)

		for i := range expected.Mu.records {
			idxExpected, leftIdx, rightIdx := (expected.Mu.idx+n-i)%n, (initial.Mu.idx+n-i)%n, (otherHalf.Mu.idx+n-i)%n
			assert.Equal(t, expected.Mu.records[idxExpected], initial.Mu.records[leftIdx])
			assert.Equal(t, expected.Mu.records[idxExpected], otherHalf.Mu.records[rightIdx])
		}
	}
}

// TestReplicaStatsMerge asserts that merging two replica stats will retain the
// aggregate properties of the more suitable record for each window. It also
// asserts that rotated time windows are properly accounted for i.e.
// [1,2,3,4,5,6], [4,5,6,1,2,3], where 6 is the most recent window, are
// correctly merged together in each.
func TestReplicaStatsMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	windowedMultipliers1 := []int{1, 2, 3, 4, 5, 6}
	windowedMultipliers10 := []int{10, 20, 30, 40, 50, 60}
	expectedMultipliers := []int{11, 22, 33, 44, 55, 66}

	testCases := []struct {
		windowsA   []int
		windowsB   []int
		windowsExp []int
		rotateA    int
		rotateB    int
	}{
		{
			windowsA:   windowedMultipliers1,
			windowsB:   windowedMultipliers10,
			windowsExp: expectedMultipliers,
			rotateA:    0,
			rotateB:    0,
		},
		{
			windowsA:   windowedMultipliers1,
			windowsB:   windowedMultipliers10,
			windowsExp: expectedMultipliers,
			rotateA:    3,
			rotateB:    1,
		},
		{
			windowsA:   windowedMultipliers1,
			windowsB:   windowedMultipliers10,
			windowsExp: expectedMultipliers,
			rotateA:    50,
			rotateB:    109,
		},
		// Ensure that with uninitialzed entries in replica stats, the result
		// adjusts correctly. Here we expect the sliding windows to be
		// merged from the most recent window (tail) backwards.
		{
			windowsA:   []int{},
			windowsB:   []int{},
			windowsExp: []int{},
			rotateA:    0,
			rotateB:    0,
		},
		{
			windowsA:   []int{1, 1},
			windowsB:   []int{1, 1, 1, 1, 1, 1},
			windowsExp: []int{1, 1, 1, 1, 2, 2},
			rotateA:    0,
			rotateB:    0,
		},
		{
			windowsA:   []int{1, 1, 1, 1, 1, 1},
			windowsB:   []int{1, 1},
			windowsExp: []int{1, 1, 1, 1, 2, 2},
			rotateA:    0,
			rotateB:    0,
		},
	}

	for _, tc := range testCases {
		rsA := genTestingReplicaStats(tc.windowsA, 10, tc.rotateA)
		rsB := genTestingReplicaStats(tc.windowsB, 10, tc.rotateB)
		expectedRs := genTestingReplicaStats(tc.windowsExp, 10, tc.rotateA)
		n := len(expectedRs.Mu.records)

		// Adjust the max/min/count, since the generated replica stats will have
		// max/min that is too high and missing half the counts.
		for i := range expectedRs.Mu.records {

			idxExpected, idxA, idxB := (expectedRs.Mu.idx+n-i)%n, (rsA.Mu.idx+n-i)%n, (rsB.Mu.idx+n-i)%n
			if expectedRs.Mu.records[idxExpected] == nil {
				continue
			}

			expectedRs.Mu.records[idxExpected].count = 0
			expectedRs.Mu.records[idxExpected].max = -math.MaxFloat64
			expectedRs.Mu.records[idxExpected].min = math.MaxFloat64

			if rsA.Mu.records[idxA] != nil {
				expectedRs.Mu.records[idxExpected].count = rsA.Mu.records[idxA].count
				expectedRs.Mu.records[idxExpected].max = rsA.Mu.records[idxA].max
				expectedRs.Mu.records[idxExpected].min = rsA.Mu.records[idxA].min
			}
			if rsB.Mu.records[idxB] != nil {
				expectedRs.Mu.records[idxExpected].count += rsB.Mu.records[idxB].count
				expectedRs.Mu.records[idxExpected].max = math.Max(expectedRs.Mu.records[idxExpected].max, rsB.Mu.records[idxB].max)
				expectedRs.Mu.records[idxExpected].min = math.Min(expectedRs.Mu.records[idxExpected].min, rsB.Mu.records[idxB].min)
			}
		}
		rsA.MergeRequestCounts(rsB)

		for i := range expectedRs.Mu.records {
			idxExpected, idxA := (expectedRs.Mu.idx+n-i)%n, (rsA.Mu.idx+n-i)%n
			assert.Equal(t, expectedRs.Mu.records[idxExpected], rsA.Mu.records[idxA], "expected idx: %d, merged idx %d", idxExpected, idxA)
		}
	}
}

// TestReplicaStatsRecordAggregate asserts that the aggregate stats collected
// per window are accurate.
func TestReplicaStatsRecordAggregate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	awsLocalities := map[roachpb.NodeID]string{
		1: "region=us-east-1,zone=us-east-1a",
		2: "region=us-east-1,zone=us-east-1b",
		3: "region=us-west-1,zone=us-west-1a",
	}

	n := 100
	rs := genTestingReplicaStats([]int{1, 0, 0, 0, 0, 0}, n, 0)

	expectedSum := float64(n*(n+1)) / 2.0

	for i := 1; i <= n; i++ {
		rs.RecordCount(float64(i*1), 1)
		rs.RecordCount(float64(i*2), 2)
		rs.RecordCount(float64(i*3), 3)
	}

	expectedLocalityCounts := PerLocalityCounts{
		awsLocalities[1]: 1 * expectedSum,
		awsLocalities[2]: 2 * expectedSum,
		awsLocalities[3]: 3 * expectedSum,
	}
	expectedStatsRecord := &replicaStatsRecord{
		localityCounts: expectedLocalityCounts,
		sum:            expectedSum * 6,
		max:            float64(n * 3),
		min:            1,
		count:          int64(n * 3),
	}

	require.Equal(t, expectedStatsRecord, rs.Mu.records[rs.Mu.idx])
}
