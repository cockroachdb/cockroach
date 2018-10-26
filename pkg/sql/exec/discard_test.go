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
)

func TestDiscard(t *testing.T) {
	tcs := []struct {
		tuples     tuples
		expected   tuples
		colsToKeep []uint32
	}{
		{
			colsToKeep: []uint32{0, 2},
			tuples: tuples{
				{1, 2, 3},
				{1, 2, 3},
			},
			expected: tuples{
				{1, 3},
				{1, 3},
			},
		},
		{
			colsToKeep: []uint32{0, 1},
			tuples: tuples{
				{1, 2, 3},
				{1, 2, 3},
			},
			expected: tuples{
				{1, 2},
				{1, 2},
			},
		},
		{
			colsToKeep: []uint32{1, 2},
			tuples: tuples{
				{1, 2, 3},
				{1, 2, 3},
			},
			expected: tuples{
				{2, 3},
				{2, 3},
			},
		},
	}
	for _, tc := range tcs {
		runTests(t, tc.tuples, []types.T{}, func(t *testing.T, input Operator) {
			count := NewDiscardOp(input, tc.colsToKeep)
			out := newOpTestOutput(count, []int{0, 1}, tc.expected)

			if err := out.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
