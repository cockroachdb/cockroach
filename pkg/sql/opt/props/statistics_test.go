// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package props

import (
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
)

func TestColumnStatisticsSort(t *testing.T) {
	type testCase struct {
		input    ColumnStatistics
		expected string
	}
	testCases := []testCase{
		{
			input: ColumnStatistics{
				{Cols: opt.MakeColSet(3)},
				{Cols: opt.MakeColSet(1)},
				{Cols: opt.MakeColSet(5)},
			},
			expected: "(1) (3) (5)",
		},
		{
			input: ColumnStatistics{
				{Cols: opt.MakeColSet(1, 3)},
				{Cols: opt.MakeColSet(1, 5)},
			},
			expected: "(1,3) (1,5)",
		},
		{
			input: ColumnStatistics{
				{Cols: opt.MakeColSet(3)},
				{Cols: opt.MakeColSet(1, 7)},
				{Cols: opt.MakeColSet(5)},
				{Cols: opt.MakeColSet(1, 3)},
				{Cols: opt.MakeColSet(1, 4, 6)},
				{Cols: opt.MakeColSet(1, 4, 7)},
			},
			expected: "(3) (5) (1,3) (1,7) (1,4,6) (1,4,7)",
		},
	}

	for _, tc := range testCases {
		sort.Sort(tc.input)
		var cols []string
		for i := 0; i < len(tc.input); i++ {
			cols = append(cols, tc.input[i].Cols.String())
		}
		result := strings.Join(cols, " ")
		if result != tc.expected {
			t.Errorf("expected %q, got %q", tc.expected, result)
		}
	}
}
