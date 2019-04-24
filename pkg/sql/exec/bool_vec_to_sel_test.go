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

import "testing"

func TestBoolVecToSelOp(t *testing.T) {
	tcs := []struct {
		boolCol  uint32
		tuples   tuples
		expected tuples
	}{
		{
			boolCol:  0,
			tuples:   tuples{{true}, {false}, {true}},
			expected: tuples{{true}, {true}},
		},
	}
	for _, tc := range tcs {
		runTests(t, []tuples{tc.tuples}, func(t *testing.T, input []Operator) {
			op := NewBoolVecToSelOp(input[0], 0)
			out := newOpTestOutput(op, []int{0}, tc.expected)
			if err := out.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
