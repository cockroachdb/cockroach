// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replicastats

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

func testingStartTime() time.Time {
	return time.Date(2022, 12, 16, 11, 0, 0, 0, time.UTC)
}

func TestReplicaStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	now := testingStartTime()

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
		rs := NewReplicaStats(now, func(nodeID roachpb.NodeID) string {
			return tc.localities[nodeID]
		})
		for _, req := range tc.reqs {
			rs.RecordCount(now, 1, req)
		}
		now = now.Add(time.Second)
		if actual := rs.SnapshotRatedSummary(now); !floatMapsEqual(tc.expected, actual.LocalityCounts) {
			t.Errorf("%d: incorrect per-locality QPS averages: %s", i, pretty.Diff(tc.expected, actual))
		}
		var expectedAvgQPS float64
		for _, v := range tc.expected {
			expectedAvgQPS += v
		}
		if actual, _ := rs.AverageRatePerSecond(now); actual != expectedAvgQPS {
			t.Errorf("%d: avgQPS() got %f, want %f", i, actual, expectedAvgQPS)
		}
		// Verify that QPS numbers get cut in half after another second.
		now = now.Add(time.Second)
		for k, v := range tc.expected {
			tc.expected[k] = v / 2
		}
		if actual := rs.SnapshotRatedSummary(now); !floatMapsEqual(tc.expected, actual.LocalityCounts) {
			t.Errorf("%d: incorrect per-locality QPS averages: %s", i, pretty.Diff(tc.expected, actual.LocalityCounts))
		}
		expectedAvgQPS /= 2
		if actual, _ := rs.AverageRatePerSecond(now); actual != expectedAvgQPS {
			t.Errorf("%d: avgQPS() got %f, want %f", i, actual, expectedAvgQPS)
		}
		rs.ResetRequestCounts(now)
		if actual, _ := rs.Sum(); actual != 0 {
			t.Errorf("%d: unexpected non-empty QPS averages after resetting: %+v", i, actual)
		}
	}
}

func TestReplicaStatsDecay(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	now := testingStartTime()

	awsLocalities := map[roachpb.NodeID]string{
		1: "region=us-east-1,zone=us-east-1a",
		2: "region=us-east-1,zone=us-east-1b",
		3: "region=us-west-1,zone=us-west-1a",
	}

	rs := NewReplicaStats(now, func(nodeID roachpb.NodeID) string {
		return awsLocalities[nodeID]
	})

	{
		actual := rs.SnapshotRatedSummary(now)
		if len(actual.LocalityCounts) != 0 {
			t.Errorf("expected empty request counts, got %+v", actual.LocalityCounts)
		}
		if actual.Duration != 0 {
			t.Errorf("expected duration = 0, got %v", actual.Duration)
		}
		now = now.Add(1)
		if dur := rs.SnapshotRatedSummary(now).Duration; dur != 1 {
			t.Errorf("expected duration = 1, got %v", dur)
		}
		rs.ResetRequestCounts(now)
	}

	{
		for _, req := range []roachpb.NodeID{1, 1, 2, 2, 3} {
			rs.RecordCount(now, 1, req)
		}
		counts := PerLocalityCounts{
			awsLocalities[1]: 2,
			awsLocalities[2]: 2,
			awsLocalities[3]: 1,
		}
		actual := rs.SnapshotRatedSummary(now)
		if actual.Duration != 0 {
			t.Errorf("expected duration = 0, got %v", actual.Duration)
		}
		if !reflect.DeepEqual(counts, actual.LocalityCounts) {
			t.Errorf("incorrect per-locality request counts: %s", pretty.Diff(counts, actual.LocalityCounts))
		}

		var totalDuration time.Duration
		for i := 0; i < len(rs.records)-1; i++ {
			now = now.Add(replStatsRotateInterval)
			totalDuration = time.Duration(float64(replStatsRotateInterval+totalDuration) * decayFactor)
			expected := make(PerLocalityCounts)
			for k, v := range counts {
				counts[k] = v * decayFactor
				expected[k] = counts[k] / totalDuration.Seconds()
			}
			actual = rs.SnapshotRatedSummary(now)
			if expectedDur := replStatsRotateInterval * time.Duration(i+1); actual.Duration != expectedDur {
				t.Errorf("expected duration = %v, got %v", expectedDur, actual.Duration)
			}
			// We can't just use DeepEqual to compare these due to the float
			// multiplication inaccuracies.
			if !floatMapsEqual(expected, actual.LocalityCounts) {
				t.Errorf("%d: incorrect per-locality request counts: %s", i, pretty.Diff(expected, actual.LocalityCounts))
			}
		}

		// All of the recorded values should have been rotated out, zeroing out
		// all the windows. Assert that no entries in the locality count map
		// have any value other than zero. The keys are not cleared as they are
		// likely to appear again.
		now = now.Add(replStatsRotateInterval)
		actualCounts := rs.SnapshotRatedSummary(now).LocalityCounts
		for _, v := range actualCounts {
			require.Zero(t, v)
		}
		rs.ResetRequestCounts(now)
	}

	{
		for _, req := range []roachpb.NodeID{1, 1, 2, 2, 3} {
			rs.RecordCount(now, 1, req)
		}
		now = now.Add(replStatsRotateInterval)
		for _, req := range []roachpb.NodeID{2, 2, 3, 3, 3} {
			rs.RecordCount(now, 1, req)
		}
		durationDivisor := time.Duration(float64(replStatsRotateInterval) * decayFactor).Seconds()
		expected := PerLocalityCounts{
			// We expect the first loop's requests to be decreased by decayFactor,
			// but not the second loop's.
			awsLocalities[1]: 2 * decayFactor / durationDivisor,
			awsLocalities[2]: (2*decayFactor + 2) / durationDivisor,
			awsLocalities[3]: (1*decayFactor + 3) / durationDivisor,
		}
		if actual := rs.SnapshotRatedSummary(now); !reflect.DeepEqual(expected, actual.LocalityCounts) {
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

	now := testingStartTime()

	awsLocalities := map[roachpb.NodeID]string{
		1: "region=us-east-1,zone=us-east-1a",
		2: "region=us-east-1,zone=us-east-1b",
		3: "region=us-west-1,zone=us-west-1a",
	}
	rs := NewReplicaStats(now, func(nodeID roachpb.NodeID) string {
		return awsLocalities[nodeID]
	})
	rs.RecordCount(now, 1, 1)
	rs.RecordCount(now, 1, 1)
	rs.RecordCount(now, 1, 2)
	rs.RecordCount(now, 1, 2)
	rs.RecordCount(now, 1, 3)
	expected := PerLocalityCounts{
		awsLocalities[1]: 2,
		awsLocalities[2]: 2,
		awsLocalities[3]: 1,
	}
	if actual := rs.SnapshotRatedSummary(now); !reflect.DeepEqual(expected, actual.LocalityCounts) {
		t.Errorf("incorrect per-locality request counts: %s", pretty.Diff(expected, actual))
	}

	increment := replStatsRotateInterval / 2
	now = now.Add(increment)
	actual1 := rs.SnapshotRatedSummary(now)
	if actual1.Duration != increment {
		t.Errorf("expected duration = %v; got %v", increment, actual1.Duration)
	}
	for k := range expected {
		expected[k] /= increment.Seconds()
	}
	if !floatMapsEqual(expected, actual1.LocalityCounts) {
		t.Errorf("incorrect per-locality request counts: %s", pretty.Diff(expected, actual1.LocalityCounts))
	}

	// Verify that all values decrease as time advances if no requests come in.
	now = now.Add(time.Second)
	actual2 := rs.SnapshotRatedSummary(now)
	if len(actual1.LocalityCounts) != len(actual2.LocalityCounts) {
		t.Fatalf("unexpected different results sizes (expected %d, got %d)", len(actual1.LocalityCounts), len(actual2.LocalityCounts))
	}
	for k := range actual1.LocalityCounts {
		if actual2.LocalityCounts[k] >= actual1.LocalityCounts[k] {
			t.Errorf("expected newer count %f to be smaller than older count %f", actual2.LocalityCounts[k], actual2.LocalityCounts[k])
		}
	}

	// Ditto for passing a window boundary.
	now = now.Add(increment)
	actual3 := rs.SnapshotRatedSummary(now)
	if len(actual2.LocalityCounts) != len(actual3.LocalityCounts) {
		t.Fatalf("unexpected different results sizes (expected %d, got %d)", len(actual2.LocalityCounts), len(actual3.LocalityCounts))
	}
	for k := range actual2.LocalityCounts {
		if actual3.LocalityCounts[k] >= actual2.LocalityCounts[k] {
			t.Errorf("expected newer count %f to be smaller than older count %f", actual3.LocalityCounts[k], actual3.LocalityCounts[k])
		}
	}
}

func genTestingReplicaStatsWithAWSLocalities(
	windowedMultipliers []int, n, offset int,
) *ReplicaStats {
	return genTestingReplicaStats(windowedMultipliers, n, offset, func(nodeID roachpb.NodeID) string {
		awsLocalities := map[roachpb.NodeID]string{
			1: "region=us-east-1,zone=us-east-1a",
			2: "region=us-east-1,zone=us-east-1b",
			3: "region=us-west-1,zone=us-west-1a",
		}
		return awsLocalities[nodeID]
	})
}

func genTestingReplicaStats(
	windowedMultipliers []int, n, offset int, localityOracle LocalityOracle,
) *ReplicaStats {
	now := testingStartTime()
	rs := NewReplicaStats(now, localityOracle)

	now = now.Add(replStatsRotateInterval * time.Duration(offset))

	// Here we generate recorded counts against the three localities. For
	// simplicity, each locality has 1/5, 2/5 and 3/5 of the aggregate count
	// recorded against it respectively.
	for _, multiplier := range windowedMultipliers {
		for i := 1; i <= n; i++ {
			rs.RecordCount(now, float64(i*1*multiplier), 1)
			rs.RecordCount(now, float64(i*2*multiplier), 2)
			rs.RecordCount(now, float64(i*3*multiplier), 3)
		}
		// rotate the window
		now = now.Add(replStatsRotateInterval)
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
		initial := genTestingReplicaStatsWithAWSLocalities(tc.windowsInitial, 10, tc.rotation)
		expected := genTestingReplicaStatsWithAWSLocalities(tc.expectedSplit, 10, tc.rotation)
		otherHalf := genTestingReplicaStatsWithAWSLocalities(nilMultipliers, 0, 0)
		n := len(initial.records)

		initial.SplitRequestCounts(otherHalf)

		for i := range expected.records {
			idxExpected, leftIdx, rightIdx := (expected.idx+n-i)%n, (initial.idx+n-i)%n, (otherHalf.idx+n-i)%n
			assert.Equal(t, expected.records[idxExpected], initial.records[leftIdx])
			assert.Equal(t, expected.records[idxExpected], otherHalf.records[rightIdx])
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
		rsA := genTestingReplicaStatsWithAWSLocalities(tc.windowsA, 10, tc.rotateA)
		rsB := genTestingReplicaStatsWithAWSLocalities(tc.windowsB, 10, tc.rotateB)
		expectedRs := genTestingReplicaStatsWithAWSLocalities(tc.windowsExp, 10, tc.rotateA)
		n := len(expectedRs.records)

		rsA.MergeRequestCounts(rsB)

		for i := range expectedRs.records {
			idxExpected, idxA := (expectedRs.idx+n-i)%n, (rsA.idx+n-i)%n
			assert.Equal(t, expectedRs.records[idxExpected], rsA.records[idxA], "expected idx: %d, merged idx %d", idxExpected, idxA)
		}
	}
}

// TestReplicaStatsLocalityCountsSplitMergeReset asserts that after a merge or
// split, where there is a mismatch in locality tracking that the locality
// tracking on each resulting replica stats is correct.
func TestReplicaStatsLocalityCountsSplitMergeReset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	initTestStats := func() (*ReplicaStats, *ReplicaStats) {
		noLocalityStats := genTestingReplicaStats([]int{1, 1, 1, 1, 1, 1}, 100, 0, nil /* localityOracle */)
		localityStats := genTestingReplicaStatsWithAWSLocalities([]int{1, 1, 1, 1, 1, 1}, 100, 0)
		return noLocalityStats, localityStats
	}

	assertEmptyLocalityTracking := func(t *testing.T, rs *ReplicaStats) {
		require.Nil(t, rs.getNodeLocality)
		for i := range rs.records {
			require.Nil(t, rs.records[i].localityCounts)
		}
		require.Equal(t, PerLocalityCounts{}, rs.PerLocalityDecayingRate(testingStartTime()))
	}

	assertNonEmptyLocalityTracking := func(t *testing.T, rs *ReplicaStats) {
		require.NotNil(t, rs.getNodeLocality)
		for i := range rs.records {
			require.NotNil(t, rs.records[i].localityCounts)
		}
		require.NotEqual(t, PerLocalityCounts{}, rs.PerLocalityDecayingRate(testingStartTime()))
	}

	t.Run("no-locality.merge(locality)", func(t *testing.T) {
		// Merge the locality stats into the no-locality stats.
		noLocalityStats, localityStats := initTestStats()
		noLocalityStats.MergeRequestCounts(localityStats)
		assertEmptyLocalityTracking(t, noLocalityStats)
		// We reset the locality replica stats after merging it into the
		// no-locality, the locality stats should have only empty records.
		require.Equal(t, PerLocalityCounts{}, localityStats.PerLocalityDecayingRate(testingStartTime()))

	})
	t.Run("locality.merge(no-locality)", func(t *testing.T) {
		// Merge the no-locality stats into the locality stats.
		noLocalityStats, localityStats := initTestStats()
		localityStats.MergeRequestCounts(noLocalityStats)
		assertEmptyLocalityTracking(t, noLocalityStats)
		assertNonEmptyLocalityTracking(t, localityStats)
	})
	t.Run("locality.split(no-locality)", func(t *testing.T) {
		// Split the locality stats request counts between itself and the
		// no-locality stats.
		noLocalityStats, localityStats := initTestStats()
		localityStats.SplitRequestCounts(noLocalityStats)
		assertEmptyLocalityTracking(t, noLocalityStats)
		assertNonEmptyLocalityTracking(t, localityStats)
	})
	t.Run("no-locality.split(locality)", func(t *testing.T) {
		// Split the no-locality stats request counts between itself and the
		// locality stats.
		noLocalityStats, localityStats := initTestStats()
		noLocalityStats.SplitRequestCounts(localityStats)
		assertEmptyLocalityTracking(t, noLocalityStats)
		assertNonEmptyLocalityTracking(t, localityStats)
	})
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
	rs := genTestingReplicaStatsWithAWSLocalities([]int{1, 0, 0, 0, 0, 0}, n, 0)

	expectedSum := float64(n*(n+1)) / 2.0

	for i := 1; i <= n; i++ {
		rs.RecordCount(rs.lastRotate, float64(i*1), 1)
		rs.RecordCount(rs.lastRotate, float64(i*2), 2)
		rs.RecordCount(rs.lastRotate, float64(i*3), 3)
	}

	expectedLocalityCounts := PerLocalityCounts{
		awsLocalities[1]: 1 * expectedSum,
		awsLocalities[2]: 2 * expectedSum,
		awsLocalities[3]: 3 * expectedSum,
	}
	expectedStatsRecord := replicaStatsRecord{
		localityCounts: &expectedLocalityCounts,
		sum:            expectedSum * 6,
		active:         true,
	}

	require.Equal(t, expectedStatsRecord, rs.records[rs.idx])
}
