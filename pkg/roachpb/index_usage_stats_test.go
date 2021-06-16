// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestAddIndexUsageStats(t *testing.T) {
	testCases := []struct {
		data     IndexUsageStatistics
		expected IndexUsageStatistics
	}{
		{
			data: IndexUsageStatistics{
				FullScanCount:     1,
				NonFullScanCount:  1,
				LookupJoinCount:   1,
				ZigzagJoinCount:   1,
				InvertedJoinCount: 1,
				LastScan:          timeutil.Unix(10, 1),
				TotalScanRows:     1,
				UpdateCounts:      1,
				TotalUpdatedRows:  1,
				LastUpdate:        timeutil.Unix(10, 2),
				LastJoin:          timeutil.Unix(10, 3),
			},
			expected: IndexUsageStatistics{
				FullScanCount:     1,
				NonFullScanCount:  1,
				LookupJoinCount:   1,
				ZigzagJoinCount:   1,
				InvertedJoinCount: 1,
				LastScan:          timeutil.Unix(10, 1),
				TotalScanRows:     1,
				UpdateCounts:      1,
				TotalUpdatedRows:  1,
				LastUpdate:        timeutil.Unix(10, 2),
				LastJoin:          timeutil.Unix(10, 3),
			},
		},
		{
			data: IndexUsageStatistics{
				FullScanCount:    9,
				NonFullScanCount: 2,
				TotalScanRows:    500,
				LastScan:         timeutil.Unix(20, 1),
			},
			expected: IndexUsageStatistics{
				FullScanCount:     10,
				NonFullScanCount:  3,
				LookupJoinCount:   1,
				ZigzagJoinCount:   1,
				InvertedJoinCount: 1,
				LastScan:          timeutil.Unix(20, 1),
				TotalScanRows:     501,
				UpdateCounts:      1,
				TotalUpdatedRows:  1,
				LastUpdate:        timeutil.Unix(10, 2),
				LastJoin:          timeutil.Unix(10, 3),
			},
		},
		{
			data: IndexUsageStatistics{
				LookupJoinCount:   2,
				ZigzagJoinCount:   4,
				InvertedJoinCount: 30,
				UpdateCounts:      10,
				TotalUpdatedRows:  200,
				LastUpdate:        timeutil.Unix(30, 1),
				LastJoin:          timeutil.Unix(30, 2),
			},
			expected: IndexUsageStatistics{
				FullScanCount:     10,
				NonFullScanCount:  3,
				LookupJoinCount:   3,
				ZigzagJoinCount:   5,
				InvertedJoinCount: 31,
				LastScan:          timeutil.Unix(20, 1),
				TotalScanRows:     501,
				UpdateCounts:      11,
				TotalUpdatedRows:  201,
				LastUpdate:        timeutil.Unix(30, 1),
				LastJoin:          timeutil.Unix(30, 2),
			},
		},
	}

	state := IndexUsageStatistics{}

	for i := range testCases {
		state.Add(&testCases[i].data)
		require.Equal(t, testCases[i].expected, state)
	}

	// Ensure that we have tested all fields.
	numFields := reflect.ValueOf(state).NumField()
	for i := 0; i < numFields; i++ {
		val := reflect.ValueOf(state).Field(i)
		require.False(t, val.IsZero(), "expected all fields to be tested, but %s is not", val.String())
	}
}
