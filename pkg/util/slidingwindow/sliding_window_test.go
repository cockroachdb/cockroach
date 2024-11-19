// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package slidingwindow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestSlidingWindow asserts that the sliding windows correctly rotate old
// windows; report the correct aggregate query and duration.
func TestSlidingWindow(t *testing.T) {
	startTime := time.Date(1, 2, 3, 4, 5, 6, 7, time.UTC)
	interval := time.Second * 1

	max := func(a, b float64) float64 {
		if a > b {
			return a
		}
		return b
	}
	sum := func(a, b float64) float64 {
		return a + b
	}

	testCases := []struct {
		desc             string
		recordedTicks    [][]float64
		binOp            func(a, b float64) float64
		expectedWindows  []float64
		expectedAgg      float64
		expectedDuration time.Duration
	}{
		{
			desc:             "every window, no overlap max",
			recordedTicks:    [][]float64{{1}, {2}, {3}, {4}},
			binOp:            max,
			expectedWindows:  []float64{4, 3, 2, 1},
			expectedAgg:      4,
			expectedDuration: interval * 4,
		},
		{
			desc:             "every window, no overlap sum",
			recordedTicks:    [][]float64{{1}, {2}, {3}, {4}},
			binOp:            sum,
			expectedWindows:  []float64{4, 3, 2, 1},
			expectedAgg:      10,
			expectedDuration: interval * 4,
		},
		{
			desc:             "partial windows, overlap max",
			recordedTicks:    [][]float64{{}, {1}, {}, {2}, {}, {3}, {}, {4}},
			binOp:            max,
			expectedWindows:  []float64{4, 0, 3, 0},
			expectedAgg:      4,
			expectedDuration: interval * 4,
		},
		{
			desc:             "partial windows, overlap sum",
			recordedTicks:    [][]float64{{}, {1}, {}, {2}, {}, {3}, {}, {4}},
			binOp:            sum,
			expectedWindows:  []float64{4, 0, 3, 0},
			expectedAgg:      7,
			expectedDuration: interval * 4,
		},
		{
			desc:             "empty windows",
			recordedTicks:    [][]float64{{}, {}, {}, {}, {}, {}, {}, {}},
			binOp:            sum,
			expectedWindows:  []float64{0, 0, 0, 0},
			expectedAgg:      0,
			expectedDuration: interval * 4,
		},
		{
			desc:          "2 windows, max",
			recordedTicks: [][]float64{{100, 200, 300}, {200, 400, 600}},
			binOp:         max,
			// We increment the query time by a second below, so we expect the
			// duration to be rounded up to 3.
			expectedWindows:  []float64{600, 300, 0},
			expectedAgg:      600,
			expectedDuration: interval * 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			swag := NewSwag(startTime, interval, 4, tc.binOp)

			curTime := startTime
			for _, records := range tc.recordedTicks {
				curTime = curTime.Add(swag.rotateInterval)
				for _, record := range records {
					swag.Record(curTime, record)
				}
			}

			// Add another interval to the query time, just shy of the rotate
			// interval - to round up the duration returned from query and
			// avoid nasty decimals.
			curTime = curTime.Add(swag.rotateInterval - time.Microsecond)
			agg, duration := swag.Query(curTime)

			require.EqualValues(t, tc.expectedWindows, swag.Windows(curTime))
			require.Equal(t, tc.expectedAgg, agg)
			require.Equal(t, tc.expectedDuration, duration.Round(time.Second))
		})
	}

}
