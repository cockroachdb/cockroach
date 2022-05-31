// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package asim

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestScannerReplicaPacer asserts that the replica scanner pacer keeps the
// correct pacing, given the desired loop interval time and max/min intervals
// set.
func TestScannerReplicaPacer(t *testing.T) {
	start := time.Date(2022, 03, 21, 11, 0, 0, 0, time.UTC)

	makeTick := func(tick int64) time.Time {
		return start.Add(time.Duration(tick) * time.Second)
	}

	testCases := []struct {
		desc               string
		loopInterval       time.Duration
		minInterval        time.Duration
		maxInterval        time.Duration
		replCount          int
		ticks              []int64
		expectedReplCounts []int
	}{
		{
			desc:               "one repl per tick",
			loopInterval:       5 * time.Second,
			minInterval:        time.Millisecond,
			maxInterval:        time.Minute,
			replCount:          5,
			ticks:              []int64{0, 1, 2, 3, 4, 5},
			expectedReplCounts: []int{0, 1, 1, 1, 1, 1},
		},
		{
			desc:               "three repl per tick",
			loopInterval:       5 * time.Second,
			minInterval:        time.Millisecond,
			maxInterval:        time.Minute,
			replCount:          15,
			ticks:              []int64{0, 1, 2, 3, 4, 5},
			expectedReplCounts: []int{0, 3, 3, 3, 3, 3},
		},
		{
			desc:               "one repl per tick, 2 loops",
			loopInterval:       5 * time.Second,
			minInterval:        time.Millisecond,
			maxInterval:        time.Minute,
			replCount:          5,
			ticks:              []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			expectedReplCounts: []int{0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		},
		{
			desc:               "maximum interval default",
			loopInterval:       10 * time.Second,
			minInterval:        time.Millisecond,
			maxInterval:        time.Second,
			replCount:          5,
			ticks:              []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			expectedReplCounts: []int{0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		},
		{
			desc:               "minimum interval default",
			loopInterval:       5 * time.Second,
			minInterval:        time.Second,
			maxInterval:        time.Minute,
			replCount:          10,
			ticks:              []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			expectedReplCounts: []int{0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		},
	}

	for _, tc := range testCases {
		nextReplsFn := func() []*Replica {
			repls := make([]*Replica, tc.replCount)
			for i := range repls {
				repls[i] = &Replica{}
			}
			return repls
		}
		t.Run(tc.desc, func(t *testing.T) {
			pacer := NewScannerReplicaPacer(nextReplsFn, tc.loopInterval, tc.minInterval, tc.maxInterval)
			results := make([]int, 0, 1)
			for _, tick := range tc.ticks {
				replsThisTick := 0
				for {
					if repl := pacer.Next(makeTick(tick)); repl == nil {
						break
					}

					replsThisTick++
				}
				results = append(results, replsThisTick)
			}
			require.Equal(t, tc.expectedReplCounts, results)
		})

	}
}
