// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

var topKSortTestCases []sortTestCase

func init() {
	topKSortTestCases = []sortTestCase{
		{
			description: "k < input length",
			tuples:      tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			expected:    tuples{{1}, {2}, {3}},
			typs:        []*types.T{types.Int},
			ordCols:     []execinfrapb.Ordering_Column{{ColIdx: 0}},
			k:           3,
		},
		{
			description: "k > input length",
			tuples:      tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			expected:    tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			typs:        []*types.T{types.Int},
			ordCols:     []execinfrapb.Ordering_Column{{ColIdx: 0}},
			k:           10,
		},
		{
			description: "nulls",
			tuples:      tuples{{1}, {2}, {nil}, {3}, {4}, {5}, {6}, {7}, {nil}},
			expected:    tuples{{nil}, {nil}, {1}},
			typs:        []*types.T{types.Int},
			ordCols:     []execinfrapb.Ordering_Column{{ColIdx: 0}},
			k:           3,
		},
		{
			description: "descending",
			tuples:      tuples{{0, 1}, {0, 2}, {0, 3}, {0, 4}, {0, 5}, {1, 5}},
			expected:    tuples{{0, 5}, {1, 5}, {0, 4}},
			typs:        []*types.T{types.Int, types.Int},
			ordCols: []execinfrapb.Ordering_Column{
				{ColIdx: 1, Direction: execinfrapb.Ordering_Column_DESC},
				{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC},
			},
			k: 3,
		},
	}
}

func TestTopKSorter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range topKSortTestCases {
		t.Run(tc.description, func(t *testing.T) {
			runTests(t, []tuples{tc.tuples}, tc.expected, orderedVerifier, func(input []colexecbase.Operator) (colexecbase.Operator, error) {
				return NewTopKSorter(testAllocator, input[0], tc.typs, tc.ordCols, tc.k), nil
			})
		})
	}
}
