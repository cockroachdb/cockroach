// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type stubTime struct {
	syncutil.RWMutex
	t time.Time
}

func (s *stubTime) setTime(t time.Time) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.t = t
}

func (s *stubTime) TimeNow() time.Time {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.t
}

// TestUpdateRollingQueryCounts tests if query counts are updated correctly.
// Updates that occur within 1s should be bucketed into a single count.
// Updates that occur greater than 1s apart should have separate query count
// entries.
func TestUpdateRollingQueryCounts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stubTime := stubTime{}
	stubTime.setTime(timeutil.Now())

	type numUpdatesPerDelay struct {
		numUpdates int
		timeDelay  time.Duration
	}

	testData := []struct {
		intervalLength      int
		updatesPerDelay     []numUpdatesPerDelay
		expectedQueryCounts []int64
	}{
		// Test case for single update.
		{
			intervalLength:      1,
			updatesPerDelay:     []numUpdatesPerDelay{{1, 0}},
			expectedQueryCounts: []int64{1},
		},
		// Test case for multiple updates in same timestamp.
		{
			intervalLength:      1,
			updatesPerDelay:     []numUpdatesPerDelay{{3, 0}},
			expectedQueryCounts: []int64{3},
		},
		// Test case for multiple updates, with multiple timestamps.
		{
			intervalLength:      3,
			updatesPerDelay:     []numUpdatesPerDelay{{2, 0}, {5, 1}, {3, 3}},
			expectedQueryCounts: []int64{2, 5, 3},
		},
	}

	for _, tc := range testData {
		freshMetrics := NewTelemetryLoggingMetrics(defaultSmoothingAlpha, int64(tc.intervalLength))
		freshMetrics.Knobs = &TelemetryLoggingTestingKnobs{
			getTimeNow: stubTime.TimeNow,
		}

		for i := 0; i < len(tc.updatesPerDelay); i++ {
			secondsDelay := tc.updatesPerDelay[i].timeDelay
			numUpdates := tc.updatesPerDelay[i].numUpdates
			stubTime.setTime(stubTime.TimeNow().Add(time.Second * secondsDelay))
			for j := 0; j < numUpdates; j++ {
				freshMetrics.updateRollingQueryCounts()
			}
		}

		circIdx := freshMetrics.mu.rollingQueryCounts.nextIndex(freshMetrics.mu.rollingQueryCounts.endPointer())
		for i := 0; i < len(freshMetrics.mu.rollingQueryCounts.queryCounts); i++ {
			queryCount := freshMetrics.mu.rollingQueryCounts.getQueryCount(circIdx)
			require.Equal(t, tc.expectedQueryCounts[i], queryCount.count)
			circIdx = freshMetrics.mu.rollingQueryCounts.nextIndex(circIdx)
		}
	}
}

// TestExpSmoothQPS tests if the correct smoothed QPS value is computed, given
// a list of query counts with timestamps.
func TestExpSmoothQPS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defaultIntervalLength := 6

	startTime := timeutil.Now()

	testData := []struct {
		intervalLength    int
		testQueryCounts   []queryCountAndTime
		expectedSmoothQPS int64
	}{
		// Test case for when number of recorded query counts is less
		// than the designated interval length.
		{
			defaultIntervalLength,
			[]queryCountAndTime{
				{startTime, 1},
			},
			0,
		},
		// Test case for interval length of 1.
		{
			1,
			[]queryCountAndTime{
				{startTime, 1},
			},
			1,
		},
		// Test case for erratic increases in query count and timestamp.
		// Also testing truncation of final smoothed value due to int64 type casting.
		{
			defaultIntervalLength,
			[]queryCountAndTime{
				{startTime, 1},
				{startTime.Add(time.Second * 2), 8},
				{startTime.Add(time.Second * 5), 15},
				{startTime.Add(time.Second * 9), 28},
				{startTime.Add(time.Second * 11), 54},
				{startTime.Add(time.Second * 12), 103},
			},
			// Result is 87.02 truncated to 87
			87,
		},

		// Test case for fluctuation in query count.
		// Also testing truncation of final smoothed value due to int64 type casting.
		{
			defaultIntervalLength,
			[]queryCountAndTime{
				{startTime, 1},
				{startTime.Add(time.Second * 3), 21},
				{startTime.Add(time.Second * 10), 4},
				{startTime.Add(time.Second * 12), 34},
				{startTime.Add(time.Second * 15), 12},
				{startTime.Add(time.Second * 16), 40},
			},
			// Average QPS is 16.4, truncated to 16 when used as the initially smoothed value.
			// Result is 33.2 truncated to 33
			33,
		},
		// Test case for truncation of individual QPS values (i.e. between each query count)
		// due to int64 type casting. Consequently impacts the initial smoothed value (average QPS)
		// and final smoothed value.
		{
			defaultIntervalLength,
			[]queryCountAndTime{
				{startTime, 1},
				{startTime.Add(time.Second * 4), 2},
				{startTime.Add(time.Second * 7), 11},
				{startTime.Add(time.Second * 10), 4},
				{startTime.Add(time.Second * 13), 5},
				{startTime.Add(time.Second * 15), 11},
			},
			// Average QPS is 2.
			// Result is 4.2 truncated to 4.
			4,
		},
	}

	for _, tc := range testData {
		freshMetrics := NewTelemetryLoggingMetrics(defaultSmoothingAlpha, int64(tc.intervalLength))
		for _, tcQc := range tc.testQueryCounts {
			freshMetrics.mu.rollingQueryCounts.insert(tcQc)
		}
		require.Equal(t, tc.expectedSmoothQPS, freshMetrics.expSmoothQPS())
	}
}

// TestCalcAvgQPS tests if the average QPS between two query counts is computed correctly.
func TestCalcAvgQPS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	startTime := timeutil.Now()
	startTimePlusOneSecond := startTime.Add(time.Second)
	startTimePlusTenSeconds := startTime.Add(time.Second * 10)

	type queryCountsAndTimes struct {
		curr queryCountAndTime
		prev queryCountAndTime
	}

	testData := []struct {
		queryCountAndTimes queryCountsAndTimes
		expectedQPS        int64
	}{
		{
			queryCountsAndTimes{
				curr: queryCountAndTime{
					startTimePlusTenSeconds,
					20,
				},
				prev: queryCountAndTime{
					startTime,
					10,
				},
			},
			2,
		},
		{
			queryCountsAndTimes{
				curr: queryCountAndTime{
					startTimePlusOneSecond,
					20,
				},
				prev: queryCountAndTime{
					startTime,
					10,
				},
			},
			20,
		},
		{
			queryCountsAndTimes{
				curr: queryCountAndTime{
					startTimePlusOneSecond,
					5,
				},
				prev: queryCountAndTime{
					startTime,
					10,
				},
			},
			5,
		},
		{
			queryCountsAndTimes{
				curr: queryCountAndTime{
					startTimePlusTenSeconds,
					6,
				},
				prev: queryCountAndTime{
					startTime,
					1,
				},
			},
			0,
		},
		{
			queryCountsAndTimes{
				curr: queryCountAndTime{
					startTimePlusTenSeconds,
					6,
				},
				prev: queryCountAndTime{},
			},
			0,
		},
		{
			queryCountsAndTimes{
				curr: queryCountAndTime{},
				prev: queryCountAndTime{},
			},
			0,
		},
	}

	for _, test := range testData {
		curr := test.queryCountAndTimes.curr
		prev := test.queryCountAndTimes.prev
		resultQPS := calcAvgQPS(&curr, &prev)
		require.Equal(t, resultQPS, test.expectedQPS)
	}
}
