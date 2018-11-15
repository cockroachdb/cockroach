// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package exec

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestSort(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tcs := []struct {
		tuples   tuples
		expected tuples
		typ      []types.T
	}{
		{
			tuples:   tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			expected: tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			typ:      []types.T{types.Int64},
		},
		{
			tuples:   tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			expected: tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			typ:      []types.T{types.Int64},
		},
		{
			tuples:   tuples{{1}, {3}, {2}, {4}, {5}, {6}, {7}},
			expected: tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			typ:      []types.T{types.Int64},
		},
		{
			tuples:   tuples{{1}, {5}, {3}, {7}, {2}, {6}, {4}},
			expected: tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			typ:      []types.T{types.Int64},
		},
		{
			tuples:   tuples{{1}, {5}, {3}, {3}, {2}, {6}, {4}},
			expected: tuples{{1}, {2}, {3}, {3}, {4}, {5}, {6}},
			typ:      []types.T{types.Int64},
		},
		{
			tuples:   tuples{{false}, {true}},
			expected: tuples{{false}, {true}},
			typ:      []types.T{types.Bool},
		},
		{
			tuples:   tuples{{true}, {false}},
			expected: tuples{{false}, {true}},
			typ:      []types.T{types.Bool},
		},
		{
			tuples:   tuples{{3.2}, {2.0}, {2.4}},
			expected: tuples{{2.0}, {2.4}, {3.2}},
			typ:      []types.T{types.Float64},
		},
	}
	for _, tc := range tcs {
		runTests(t, []tuples{tc.tuples}, []types.T{}, func(t *testing.T, input []Operator) {
			sortSpec := sortSpec{
				sortCol:    0,
				direction:  ascending,
				inputTypes: tc.typ,
			}

			sort := NewSortOp(input[0], sortSpec)
			out := newOpTestOutput(sort, []int{0}, tc.expected)

			if err := out.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
