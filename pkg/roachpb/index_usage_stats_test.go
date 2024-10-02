// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
				TotalReadCount:   1,
				TotalWriteCount:  1,
				TotalRowsWritten: 1,
				TotalRowsRead:    1,
				LastRead:         timeutil.Unix(10, 1),
				LastWrite:        timeutil.Unix(10, 2),
			},
			expected: IndexUsageStatistics{
				TotalReadCount:   1,
				TotalWriteCount:  1,
				TotalRowsWritten: 1,
				TotalRowsRead:    1,
				LastRead:         timeutil.Unix(10, 1),
				LastWrite:        timeutil.Unix(10, 2),
			},
		},
		{
			data: IndexUsageStatistics{
				TotalReadCount: 2,
				TotalRowsRead:  9,
				LastRead:       timeutil.Unix(20, 1),
			},
			expected: IndexUsageStatistics{
				TotalReadCount:   3,
				TotalWriteCount:  1,
				TotalRowsWritten: 1,
				TotalRowsRead:    10,
				LastRead:         timeutil.Unix(20, 1),
				LastWrite:        timeutil.Unix(10, 2),
			},
		},
		{
			data: IndexUsageStatistics{
				TotalWriteCount:  4,
				TotalRowsWritten: 30,
				LastWrite:        timeutil.Unix(30, 1),
			},
			expected: IndexUsageStatistics{
				TotalReadCount:   3,
				TotalWriteCount:  5,
				TotalRowsWritten: 31,
				TotalRowsRead:    10,
				LastRead:         timeutil.Unix(20, 1),
				LastWrite:        timeutil.Unix(30, 1),
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
