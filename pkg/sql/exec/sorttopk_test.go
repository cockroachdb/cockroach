// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestTopKSorter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tcs := []struct {
		name     string
		tuples   tuples
		expected tuples
		ordCols  []distsqlpb.Ordering_Column
		typ      []types.T
		k        uint16
	}{
		{
			name:     "k < input length",
			tuples:   tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			expected: tuples{{1}, {2}, {3}},
			typ:      []types.T{types.Int64},
			ordCols:  []distsqlpb.Ordering_Column{{ColIdx: 0}},
			k:        3,
		},
		{
			name:     "k > input length",
			tuples:   tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			expected: tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			typ:      []types.T{types.Int64},
			ordCols:  []distsqlpb.Ordering_Column{{ColIdx: 0}},
			k:        10,
		},
		{
			name:     "nulls",
			tuples:   tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {nil}},
			expected: tuples{{nil}, {1}, {2}},
			typ:      []types.T{types.Int64},
			ordCols:  []distsqlpb.Ordering_Column{{ColIdx: 0}},
			k:        3,
		},
		{
			name:     "descending",
			tuples:   tuples{{0, 1}, {0, 2}, {0, 3}, {0, 4}, {0, 5}, {1, 5}},
			expected: tuples{{0, 5}, {1, 5}, {0, 4}},
			typ:      []types.T{types.Int64, types.Int64},
			ordCols: []distsqlpb.Ordering_Column{
				{ColIdx: 1, Direction: distsqlpb.Ordering_Column_DESC},
				{ColIdx: 0, Direction: distsqlpb.Ordering_Column_ASC},
			},
			k: 3,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			runTests(t, []tuples{tc.tuples}, func(t *testing.T, input []Operator) {
				sort := NewTopKSorter(input[0], tc.typ, tc.ordCols, tc.k)
				cols := make([]int, len(tc.typ))
				for i := range cols {
					cols[i] = i
				}
				out := newOpTestOutput(sort, cols, tc.expected)
				if err := out.Verify(); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}
