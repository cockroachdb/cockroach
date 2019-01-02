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

package props_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/util"
)

func TestColStatsMap(t *testing.T) {
	testcases := []struct {
		cols     []int
		remove   bool
		clear    bool
		expected string
	}{
		{cols: []int{1}, expected: "(1)"},
		{cols: []int{1}, expected: "(1)"},
		{cols: []int{2}, expected: "(1)+(2)"},
		{cols: []int{1, 2}, expected: "(1)+(2)+(1,2)"},
		{cols: []int{1, 2}, expected: "(1)+(2)+(1,2)"},
		{cols: []int{2}, expected: "(1)+(2)+(1,2)"},
		{cols: []int{1}, remove: true, expected: "(2)"},

		// Add after removing.
		{cols: []int{2, 3}, expected: "(2)+(2,3)"},
		{cols: []int{2, 3, 4}, expected: "(2)+(2,3)+(2-4)"},
		{cols: []int{3}, expected: "(2)+(2,3)+(2-4)+(3)"},
		{cols: []int{3, 4}, expected: "(2)+(2,3)+(2-4)+(3)+(3,4)"},
		{cols: []int{5, 7}, expected: "(2)+(2,3)+(2-4)+(3)+(3,4)+(5,7)"},
		{cols: []int{5}, expected: "(2)+(2,3)+(2-4)+(3)+(3,4)+(5,7)+(5)"},
		{cols: []int{3, 4}, remove: true, expected: "(2)+(5,7)+(5)"},

		// Add after clearing.
		{cols: []int{}, clear: true, expected: ""},
		{cols: []int{5}, expected: "(5)"},
		{cols: []int{1}, expected: "(5)+(1)"},
		{cols: []int{1, 5}, expected: "(5)+(1)+(1,5)"},
		{cols: []int{5, 6}, expected: "(5)+(1)+(1,5)+(5,6)"},
		{cols: []int{2}, expected: "(5)+(1)+(1,5)+(5,6)+(2)"},
		{cols: []int{1, 2}, expected: "(5)+(1)+(1,5)+(5,6)+(2)+(1,2)"},

		// Remove node, where remaining nodes still require prefix tree index.
		{cols: []int{6}, remove: true, expected: "(5)+(1)+(1,5)+(2)+(1,2)"},
		{cols: []int{3, 4}, expected: "(5)+(1)+(1,5)+(2)+(1,2)+(3,4)"},
	}

	var stats props.ColStatsMap
	for _, tc := range testcases {
		cols := util.MakeFastIntSet(tc.cols...)
		if !tc.remove {
			if tc.clear {
				stats.Clear()
			} else {
				stats.Add(cols)
			}
		} else {
			stats.RemoveIntersecting(cols)
		}

		var b strings.Builder
		for i := 0; i < stats.Count(); i++ {
			get := stats.Get(i)
			if i != 0 {
				b.WriteRune('+')
			}
			fmt.Fprint(&b, get.Cols)

			lookup, ok := stats.Lookup(get.Cols)
			if !ok {
				t.Errorf("could not find cols in map: %s", get.Cols)
			}
			if get != lookup {
				t.Errorf("lookup did not return expected colstat: %+v vs. %+v", get, lookup)
			}
		}

		if b.String() != tc.expected {
			t.Errorf("expected: %s, actual: %s", tc.expected, b.String())
		}
	}
}
