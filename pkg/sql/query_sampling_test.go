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

func TestUpdateRollingQueryCounts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defaultAlpha := 0.8
	defaultIntervalLength := 10
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
			intervalLength:      defaultIntervalLength,
			updatesPerDelay:     []numUpdatesPerDelay{{1, 0}},
			expectedQueryCounts: []int64{1},
		},
		// Test case for multiple updates in same timestamp.
		{
			intervalLength:      defaultIntervalLength,
			updatesPerDelay:     []numUpdatesPerDelay{{3, 0}},
			expectedQueryCounts: []int64{3},
		},
		// Test case for multiple updates, with multiple timestamps.
		{
			intervalLength:      defaultIntervalLength,
			updatesPerDelay:     []numUpdatesPerDelay{{2, 0}, {5, 1}, {3, 3}},
			expectedQueryCounts: []int64{2, 5, 3},
		},
	}

	for _, tc := range testData {
		freshMetrics := NewTelemetryLoggingMetrics(defaultAlpha, int64(tc.intervalLength))
		freshMetrics.Knobs = &TelemetryLoggingTestingKnobs{
			GetTimeNow: stubTime.TimeNow,
		}

		for i := 0; i < len(tc.updatesPerDelay); i++ {
			secondsDelay := tc.updatesPerDelay[i].timeDelay
			numUpdates := tc.updatesPerDelay[i].numUpdates
			stubTime.setTime(stubTime.TimeNow().Add(time.Second * secondsDelay))
			for j := 0; j < numUpdates; j++ {
				freshMetrics.UpdateRollingQueryCounts()
			}
		}

		for idx, queryCount := range freshMetrics.RollingQueryCounts.QueryCounts {
			require.Equal(t, tc.expectedQueryCounts[idx], queryCount.Count())
		}
	}
}

func TestExpSmoothQPS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defaultAlpha := 0.8
	defaultIntervalLength := 6

	startTime := timeutil.Now()

	testData := []struct {
		intervalLength    int
		testQueryCounts   []QueryCountAndTime
		expectedSmoothQPS int64
	}{
		// Test case for when number of recorded query counts is less
		// than the designated interval length.
		{
			defaultIntervalLength,
			[]QueryCountAndTime{
				NewQueryCountAndTime(startTime.Unix(), 1),
			},
			0,
		},
		// Test case for interval length of 1.
		{
			1,
			[]QueryCountAndTime{
				NewQueryCountAndTime(startTime.Unix(), 1),
			},
			1,
		},
		// Test case for erratic increases in query count and timestamp.
		// Also testing truncation of final smoothed value due to int64 type casting.
		{
			defaultIntervalLength,
			[]QueryCountAndTime{
				NewQueryCountAndTime(startTime.Unix(), 1),
				NewQueryCountAndTime(startTime.Add(time.Second*2).Unix(), 8),
				NewQueryCountAndTime(startTime.Add(time.Second*5).Unix(), 15),
				NewQueryCountAndTime(startTime.Add(time.Second*9).Unix(), 28),
				NewQueryCountAndTime(startTime.Add(time.Second*11).Unix(), 54),
				NewQueryCountAndTime(startTime.Add(time.Second*12).Unix(), 103),
			},
			// Result is 87.02 truncated to 87
			87,
		},

		// Test case for fluctuation in query count.
		// Also testing truncation of final smoothed value due to int64 type casting.
		{
			defaultIntervalLength,
			[]QueryCountAndTime{
				NewQueryCountAndTime(startTime.Unix(), 1),
				NewQueryCountAndTime(startTime.Add(time.Second*3).Unix(), 21),
				NewQueryCountAndTime(startTime.Add(time.Second*10).Unix(), 4),
				NewQueryCountAndTime(startTime.Add(time.Second*12).Unix(), 34),
				NewQueryCountAndTime(startTime.Add(time.Second*15).Unix(), 12),
				NewQueryCountAndTime(startTime.Add(time.Second*16).Unix(), 40),
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
			[]QueryCountAndTime{
				NewQueryCountAndTime(startTime.Unix(), 1),
				NewQueryCountAndTime(startTime.Add(time.Second*4).Unix(), 2),
				NewQueryCountAndTime(startTime.Add(time.Second*7).Unix(), 11),
				NewQueryCountAndTime(startTime.Add(time.Second*10).Unix(), 4),
				NewQueryCountAndTime(startTime.Add(time.Second*13).Unix(), 5),
				NewQueryCountAndTime(startTime.Add(time.Second*15).Unix(), 11),
			},
			// Average QPS is 2.
			// Result is 4.2 truncated to 4.
			4,
		},
	}

	for _, tc := range testData {
		freshMetrics := NewTelemetryLoggingMetrics(defaultAlpha, int64(tc.intervalLength))
		for _, tcQc := range tc.testQueryCounts {
			freshMetrics.RollingQueryCounts.Insert(tcQc)
		}
		require.Equal(t, tc.expectedSmoothQPS, freshMetrics.ExpSmoothQPS())
	}
}

func TestCalcAvgQPS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	startTime := timeutil.Now()
	startTimePlusOneSecond := startTime.Add(time.Second)
	startTimePlusTenSeconds := startTime.Add(time.Second * 10)

	type QueryCountsAndTimes struct {
		curr QueryCountAndTime
		prev QueryCountAndTime
	}

	testData := []struct {
		queryCountAndTimes QueryCountsAndTimes
		expectedQPS        int64
	}{
		{
			QueryCountsAndTimes{
				curr: QueryCountAndTime{
					startTimePlusTenSeconds.Unix(),
					20,
				},
				prev: QueryCountAndTime{
					startTime.Unix(),
					10,
				},
			},
			2,
		},
		{
			QueryCountsAndTimes{
				curr: QueryCountAndTime{
					startTimePlusOneSecond.Unix(),
					20,
				},
				prev: QueryCountAndTime{
					startTime.Unix(),
					10,
				},
			},
			20,
		},
		{
			QueryCountsAndTimes{
				curr: QueryCountAndTime{
					startTimePlusOneSecond.Unix(),
					5,
				},
				prev: QueryCountAndTime{
					startTime.Unix(),
					10,
				},
			},
			5,
		},
		{
			QueryCountsAndTimes{
				curr: QueryCountAndTime{
					startTimePlusTenSeconds.Unix(),
					6,
				},
				prev: QueryCountAndTime{
					startTime.Unix(),
					1,
				},
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
