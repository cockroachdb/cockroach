// Copyright 2016 The Cockroach Authors.
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

package sql

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

type desiredCase struct {
	line            int
	desired         sqlbase.ColumnOrdering
	expected        int
	expectedReverse int
}

type computeOrderCase struct {
	existing orderingInfo
	cases    []desiredCase
}

const asc = encoding.Ascending
const desc = encoding.Descending

// Sample usage:
//   makeColumnGroups(1, 2, asc, 5, desc, 4, 6, asc) -> (1/2)+,5-,(4/6)+
func makeColumnGroups(args ...interface{}) []orderingColumnGroup {
	var res []orderingColumnGroup
	var curGroup util.FastIntSet
	for _, a := range args {
		switch v := a.(type) {
		case int:
			curGroup.Add(uint32(v))
		case encoding.Direction:
			res = append(res, orderingColumnGroup{
				cols: curGroup,
				dir:  v,
			})
			curGroup = util.FastIntSet{}
		default:
			panic(fmt.Sprintf("unknown type %T", a))
		}
	}
	if !curGroup.Empty() {
		panic("last arg must be a direction")
	}
	return res
}

// Sample usage:
//   makeColumnOrdering(1, asc, 2, desc, 3, asc) -> 1+,2-,3+
func makeColumnOrdering(args ...interface{}) sqlbase.ColumnOrdering {
	var res sqlbase.ColumnOrdering
	for i := 0; i < len(args); i += 2 {
		res = append(res, sqlbase.ColumnOrderInfo{
			ColIdx:    args[i].(int),
			Direction: args[i+1].(encoding.Direction),
		})
	}
	return res
}

func defTestCase(expected, expectedReverse int, desired sqlbase.ColumnOrdering) desiredCase {
	// The line number is used to identify testcases in error messages.
	_, line, _ := caller.Lookup(1)
	return desiredCase{
		line:            line,
		desired:         desired,
		expected:        expected,
		expectedReverse: expectedReverse,
	}
}

func TestComputeOrderingMatch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testSets := []computeOrderCase{
		{
			// No existing ordering.
			existing: orderingInfo{
				ordering: nil,
				isKey:    false,
			},
			cases: []desiredCase{
				defTestCase(0, 0, makeColumnOrdering(1, desc, 5, asc)),
			},
		},
		{
			// Ordering with no constant columns.
			existing: orderingInfo{
				ordering: makeColumnGroups(1, desc, 2, asc),
				isKey:    false,
			},
			cases: []desiredCase{
				defTestCase(1, 0, makeColumnOrdering(1, desc, 5, asc)),
				defTestCase(0, 1, makeColumnOrdering(1, asc, 5, asc, 2, asc)),
			},
		},
		{
			// Ordering with column groups and no constant columns.
			existing: orderingInfo{
				ordering: makeColumnGroups(1, 2, desc, 3, 4, asc),
				isKey:    false,
			},
			cases: []desiredCase{
				defTestCase(2, 0, makeColumnOrdering(1, desc, 3, asc)),
				defTestCase(2, 0, makeColumnOrdering(1, desc, 4, asc)),
				defTestCase(2, 0, makeColumnOrdering(2, desc, 3, asc)),
				defTestCase(2, 0, makeColumnOrdering(2, desc, 4, asc)),
				defTestCase(4, 0, makeColumnOrdering(1, desc, 3, asc, 1, desc, 3, asc)),
				defTestCase(4, 0, makeColumnOrdering(1, desc, 3, asc, 2, desc, 4, asc)),
				defTestCase(1, 0, makeColumnOrdering(1, desc, 5, asc)),
				defTestCase(0, 3, makeColumnOrdering(1, asc, 4, desc, 2, asc)),
				defTestCase(0, 1, makeColumnOrdering(2, asc, 5, asc, 2, asc)),
			},
		},
		{
			// Ordering with no constant columns but with isKey.
			existing: orderingInfo{
				ordering: makeColumnGroups(1, desc, 2, asc),
				isKey:    true,
			},
			cases: []desiredCase{
				defTestCase(1, 0, makeColumnOrdering(1, desc, 5, asc)),
				defTestCase(3, 0, makeColumnOrdering(1, desc, 2, asc, 5, asc)),
				defTestCase(4, 0, makeColumnOrdering(1, desc, 2, asc, 5, asc, 6, desc)),
				defTestCase(0, 1, makeColumnOrdering(1, asc, 5, asc, 2, asc)),
				defTestCase(0, 3, makeColumnOrdering(1, asc, 2, desc, 5, asc)),
				defTestCase(0, 4, makeColumnOrdering(1, asc, 2, desc, 5, asc, 6, asc)),
			},
		},
		{
			// Ordering with column groups, no constant columns but with isKey.
			existing: orderingInfo{
				ordering: makeColumnGroups(1, 2, 3, desc, 4, asc),
				isKey:    true,
			},
			cases: []desiredCase{
				defTestCase(1, 0, makeColumnOrdering(1, desc, 5, asc)),
				defTestCase(1, 0, makeColumnOrdering(2, desc, 5, asc)),
				defTestCase(1, 0, makeColumnOrdering(3, desc, 5, asc)),
				defTestCase(3, 0, makeColumnOrdering(1, desc, 4, asc, 5, asc)),
				defTestCase(3, 0, makeColumnOrdering(2, desc, 4, asc, 5, asc)),
				defTestCase(3, 0, makeColumnOrdering(3, desc, 4, asc, 5, asc)),
				defTestCase(4, 0, makeColumnOrdering(1, desc, 4, asc, 5, asc, 6, desc)),
				defTestCase(4, 0, makeColumnOrdering(2, desc, 4, asc, 5, asc, 6, desc)),
				defTestCase(4, 0, makeColumnOrdering(3, desc, 4, asc, 5, asc, 6, desc)),
				defTestCase(0, 1, makeColumnOrdering(1, asc, 5, asc, 4, asc)),
				defTestCase(0, 1, makeColumnOrdering(2, asc, 5, asc, 4, asc)),
				defTestCase(0, 1, makeColumnOrdering(3, asc, 5, asc, 4, asc)),
				defTestCase(0, 3, makeColumnOrdering(1, asc, 4, desc, 5, asc)),
				defTestCase(0, 3, makeColumnOrdering(2, asc, 4, desc, 5, asc)),
				defTestCase(0, 3, makeColumnOrdering(3, asc, 4, desc, 5, asc)),
				defTestCase(0, 4, makeColumnOrdering(1, asc, 4, desc, 5, asc, 6, asc)),
				defTestCase(0, 4, makeColumnOrdering(2, asc, 4, desc, 5, asc, 6, asc)),
				defTestCase(0, 4, makeColumnOrdering(3, asc, 4, desc, 5, asc, 6, asc)),
			},
		},
		{
			// Ordering with only constant columns.
			existing: orderingInfo{
				constantCols: util.MakeFastIntSet(1, 2),
				ordering:     nil,
				isKey:        false,
			},
			cases: []desiredCase{
				defTestCase(1, 1, makeColumnOrdering(2, desc, 5, asc, 1, asc)),
				defTestCase(0, 0, makeColumnOrdering(5, asc, 2, asc)),
			},
		},
		{
			// Ordering with constant columns.
			existing: orderingInfo{
				constantCols: util.MakeFastIntSet(0, 5, 6),
				ordering:     makeColumnGroups(1, desc, 2, asc),
				isKey:        false,
			},
			cases: []desiredCase{
				defTestCase(2, 0, makeColumnOrdering(1, desc, 5, asc)),
				defTestCase(2, 1, makeColumnOrdering(5, asc, 1, desc)),
				defTestCase(2, 2, makeColumnOrdering(0, desc, 5, asc)),
				defTestCase(1, 0, makeColumnOrdering(1, desc, 2, desc)),
				defTestCase(5, 2, makeColumnOrdering(0, asc, 6, desc, 1, desc, 5, desc, 2, asc)),
				defTestCase(2, 2, makeColumnOrdering(0, asc, 6, desc, 2, asc, 5, desc, 1, desc)),
			},
		},
		{
			// Ordering with group columns and constant columns.
			existing: orderingInfo{
				constantCols: util.MakeFastIntSet(0, 8, 9),
				ordering:     makeColumnGroups(1, 2, 3, desc, 4, 5, asc),
				isKey:        false,
			},
			cases: []desiredCase{
				defTestCase(2, 0, makeColumnOrdering(1, desc, 8, asc)),
				defTestCase(2, 0, makeColumnOrdering(2, desc, 8, asc)),
				defTestCase(2, 0, makeColumnOrdering(3, desc, 8, asc)),
				defTestCase(2, 1, makeColumnOrdering(8, asc, 2, desc)),
				defTestCase(2, 2, makeColumnOrdering(0, desc, 8, asc)),
				defTestCase(1, 0, makeColumnOrdering(2, desc, 5, desc)),
				defTestCase(5, 2, makeColumnOrdering(0, asc, 9, desc, 2, desc, 8, desc, 5, asc)),
				defTestCase(2, 2, makeColumnOrdering(0, asc, 9, desc, 4, asc, 8, desc, 3, desc)),
			},
		},
		{
			// Ordering with constant columns and isKey.
			existing: orderingInfo{
				constantCols: util.MakeFastIntSet(0, 5, 6),
				ordering:     makeColumnGroups(1, desc, 2, asc),
				isKey:        true,
			},
			cases: []desiredCase{
				defTestCase(2, 0, makeColumnOrdering(1, desc, 5, asc)),
				defTestCase(2, 1, makeColumnOrdering(5, asc, 1, desc)),
				defTestCase(4, 0, makeColumnOrdering(1, desc, 5, asc, 2, asc, 7, desc)),
				defTestCase(4, 1, makeColumnOrdering(5, asc, 1, desc, 2, asc, 7, desc)),
				defTestCase(2, 2, makeColumnOrdering(0, desc, 5, asc)),
				defTestCase(2, 1, makeColumnOrdering(5, asc, 1, desc, 2, desc)),
				defTestCase(1, 0, makeColumnOrdering(1, desc, 2, desc)),
				defTestCase(6, 2, makeColumnOrdering(0, asc, 6, desc, 1, desc, 5, desc, 2, asc, 9, asc)),
				defTestCase(2, 2, makeColumnOrdering(0, asc, 6, desc, 2, asc, 5, desc, 1, desc)),
			},
		},
		{
			// Ordering with column groups, constant columns and isKey.
			existing: orderingInfo{
				constantCols: util.MakeFastIntSet(0, 5, 6),
				ordering:     makeColumnGroups(1, 8, desc, 2, 9, asc),
				isKey:        true,
			},
			cases: []desiredCase{
				defTestCase(2, 0, makeColumnOrdering(8, desc, 5, asc)),
				defTestCase(2, 0, makeColumnOrdering(1, desc, 5, asc)),
				defTestCase(2, 1, makeColumnOrdering(5, asc, 1, desc)),
				defTestCase(4, 0, makeColumnOrdering(1, desc, 5, asc, 9, asc, 7, desc)),
				defTestCase(4, 1, makeColumnOrdering(5, asc, 8, desc, 2, asc, 7, desc)),
				defTestCase(2, 2, makeColumnOrdering(0, desc, 5, asc)),
				defTestCase(2, 1, makeColumnOrdering(5, asc, 8, desc, 2, desc)),
				defTestCase(1, 0, makeColumnOrdering(8, desc, 2, desc)),
				defTestCase(6, 2, makeColumnOrdering(0, asc, 6, desc, 8, desc, 5, desc, 2, asc, 9, asc)),
				defTestCase(2, 2, makeColumnOrdering(0, asc, 6, desc, 9, asc, 5, desc, 1, desc)),
			},
		},
	}

	for _, ts := range testSets {
		for _, tc := range ts.cases {
			res := ts.existing.computeMatch(tc.desired)
			resRev := ts.existing.reverse().computeMatch(tc.desired)
			if res != tc.expected || resRev != tc.expectedReverse {
				t.Errorf("Test defined on line %d failed: expected:%d/%d got:%d/%d",
					tc.line, tc.expected, tc.expectedReverse, res, resRev)
			}
		}
	}
}

func TestTrimOrderingGuarantee(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rng, _ := randutil.NewPseudoRand()

	// This test verifies the guarantee of trim: before and after are equal
	// in:
	//   before := ord.computeMatch(desired)
	//   ord.trim(desired)
	//   after := ord.computeMatch(desired)

	// genColumn generates a column index that does not appear already in the
	// ordering (as a constant column or inside a column group).
	genColumn := func(o orderingInfo) int {
	Loop:
		for tries := 0; ; tries++ {
			// Increase the range if we can't find a new valid id.
			x := rng.Intn(10 + tries/10)
			if o.constantCols.Contains(uint32(x)) {
				continue Loop
			}
			for _, group := range o.ordering {
				if group.cols.Contains(uint32(x)) {
					continue Loop
				}
			}
			return x
		}
	}
	genDir := func() encoding.Direction {
		if rng.Intn(2) == 0 {
			return encoding.Descending
		}
		return encoding.Ascending
	}

	for _, numConstCols := range []int{0, 1, 2, 4} {
		for _, numGroups := range []int{0, 1, 2, 4, 5} {
			for _, maxGroupSize := range []int{1, 2, 4} {
				for _, isKey := range []bool{false, true} {
					for tries := 0; tries < 20; tries++ {
						o := orderingInfo{isKey: isKey}
						for i := 0; i < numConstCols; i++ {
							o.addConstantColumn(genColumn(o))
						}
						for i := 0; i < numGroups; i++ {
							size := 1 + rng.Intn(maxGroupSize)
							var group util.FastIntSet
							for j := 0; j < size; j++ {
								group.Add(uint32(genColumn(o)))
							}
							o.addColumnGroup(group, genDir())
						}
						for _, desiredLen := range []int{0, 1, 2, 4, 5} {
							for desiredTries := 0; desiredTries < 10; desiredTries++ {
								desired := make(sqlbase.ColumnOrdering, desiredLen)
								perm := rng.Perm(10)
								for i := range desired {
									desired[i].ColIdx = perm[i]
									desired[i].Direction = genDir()
								}
								oCopy := o.copy()
								before := oCopy.computeMatch(desired)
								oCopy.trim(desired)
								after := oCopy.computeMatch(desired)
								if before != after {
									t.Errorf(
										"before: %d  after: %d  ordering: %s  desired: %v trimmed: %s",
										before, after, o.AsString(nil), desired, oCopy.AsString(nil),
									)
								}
							}
						}
					}
				}
			}
		}
	}
}

func TestTrimOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		ord      orderingInfo
		desired  sqlbase.ColumnOrdering
		expected orderingInfo
	}{
		{
			name: "basic-prefix-1",
			ord: orderingInfo{
				ordering: makeColumnGroups(1, asc, 2, desc),
				isKey:    true,
			},
			desired: makeColumnOrdering(1, asc),
			expected: orderingInfo{
				ordering: makeColumnGroups(1, asc),
				isKey:    false,
			},
		},
		{
			name: "basic-prefix-1-with-groups",
			ord: orderingInfo{
				ordering: makeColumnGroups(1, 5, asc, 2, desc),
				isKey:    true,
			},
			desired: makeColumnOrdering(1, asc, 5, asc),
			expected: orderingInfo{
				ordering: makeColumnGroups(1, 5, asc),
				isKey:    false,
			},
		},
		{
			name: "direction-mismatch",
			ord: orderingInfo{
				ordering: makeColumnGroups(1, asc, 2, desc),
				isKey:    true,
			},
			desired: makeColumnOrdering(1, desc),
			expected: orderingInfo{
				ordering: makeColumnGroups(),
				isKey:    false,
			},
		},
		{
			name: "const-columns-1",
			ord: orderingInfo{
				constantCols: util.MakeFastIntSet(0, 5, 6),
				ordering:     makeColumnGroups(1, desc, 2, desc),
				isKey:        true,
			},
			desired: makeColumnOrdering(5, asc, 1, desc),
			expected: orderingInfo{
				constantCols: util.MakeFastIntSet(0, 5, 6),
				ordering:     makeColumnGroups(1, desc),
				isKey:        false,
			},
		},
		{
			name: "const-columns-2",
			ord: orderingInfo{
				constantCols: util.MakeFastIntSet(0, 5, 6),
				ordering:     makeColumnGroups(1, desc, 2, desc, 3, asc),
				isKey:        true,
			},
			desired: makeColumnOrdering(5, asc, 1, desc, 0, desc, 2, desc),
			expected: orderingInfo{
				constantCols: util.MakeFastIntSet(0, 5, 6),
				ordering:     makeColumnGroups(1, desc, 2, desc),
				isKey:        false,
			},
		},
		{
			name: "const-columns-with-groups",
			ord: orderingInfo{
				constantCols: util.MakeFastIntSet(0, 5, 6),
				ordering:     makeColumnGroups(1, 7, desc, 2, 9, desc),
				isKey:        true,
			},
			desired: makeColumnOrdering(5, asc, 1, desc, 6, desc, 7, asc),
			expected: orderingInfo{
				constantCols: util.MakeFastIntSet(0, 5, 6),
				ordering:     makeColumnGroups(1, 7, desc),
				isKey:        false,
			},
		},
		{
			name: "no-match",
			ord: orderingInfo{
				constantCols: util.MakeFastIntSet(0, 5, 6),
				ordering:     makeColumnGroups(1, desc, 2, desc, 3, asc),
				isKey:        true,
			},
			desired: makeColumnOrdering(5, asc, 4, asc),
			expected: orderingInfo{
				constantCols: util.MakeFastIntSet(0, 5, 6),
				ordering:     makeColumnGroups(),
				isKey:        false,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.ord.trim(tc.desired)
			if !tc.ord.constantCols.Equals(tc.expected.constantCols) ||
				tc.ord.isKey != tc.expected.isKey ||
				len(tc.ord.ordering) != len(tc.expected.ordering) {
				t.Errorf("expected %v, got %v", tc.expected, tc.ord)
			}
			for i, o := range tc.ord.ordering {
				if !o.cols.Equals(tc.expected.ordering[i].cols) ||
					o.dir != tc.expected.ordering[i].dir {
					t.Errorf("expected %v, got %v", tc.expected, tc.ord)
					break
				}
			}
		})
	}
}

func TestComputeMergeJoinOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name       string
		a, b       orderingInfo
		colA, colB []int
		expected   sqlbase.ColumnOrdering
	}{
		{
			name: "basic",
			a: orderingInfo{
				ordering: makeColumnGroups(1, asc, 2, desc, 3, asc),
			},
			b: orderingInfo{
				ordering: makeColumnGroups(3, asc, 4, desc),
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc, 1, desc),
		},
		{
			name: "groups",
			a: orderingInfo{
				ordering: makeColumnGroups(1, asc, 2, 5, desc, 3, asc),
			},
			b: orderingInfo{
				ordering: makeColumnGroups(2, 3, asc, 4, desc),
			},
			colA:     []int{1, 5},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc, 1, desc),
		},
		{
			name: "const-a",
			a: orderingInfo{
				constantCols: util.MakeFastIntSet(1, 2),
			},
			b: orderingInfo{
				ordering: makeColumnGroups(3, asc, 4, desc),
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc, 1, desc),
		},
		{
			name: "const-b",
			a: orderingInfo{
				ordering: makeColumnGroups(1, asc, 2, desc, 3, asc),
			},
			b: orderingInfo{
				constantCols: util.MakeFastIntSet(3, 4),
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc, 1, desc),
		},
		{
			name: "const-with-groups",
			a: orderingInfo{
				ordering: makeColumnGroups(1, 4, 5, asc, 2, desc, 3, asc),
			},
			b: orderingInfo{
				constantCols: util.MakeFastIntSet(3, 4),
			},
			colA:     []int{5, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc, 1, desc),
		},
		{
			name: "const-both",
			a: orderingInfo{
				ordering:     makeColumnGroups(2, desc),
				constantCols: util.MakeFastIntSet(1),
			},
			b: orderingInfo{
				ordering:     makeColumnGroups(3, asc),
				constantCols: util.MakeFastIntSet(4),
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(1, desc, 0, asc),
		},
		{
			name: "key-a",
			a: orderingInfo{
				ordering: makeColumnGroups(1, asc),
				isKey:    true,
			},
			b: orderingInfo{
				ordering: makeColumnGroups(3, asc, 4, desc),
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc, 1, desc),
		},
		{
			name: "key-b",
			a: orderingInfo{
				ordering: makeColumnGroups(1, asc, 2, desc),
			},
			b: orderingInfo{
				ordering: makeColumnGroups(3, asc),
				isKey:    true,
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc, 1, desc),
		},
		{
			name: "partial-ordering-1",
			a: orderingInfo{
				ordering: makeColumnGroups(1, asc, 3, asc, 2, desc),
			},
			b: orderingInfo{
				ordering: makeColumnGroups(3, asc, 4, desc),
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc),
		},
		{
			name: "partial-ordering-2",
			a: orderingInfo{
				ordering: makeColumnGroups(1, asc, 2, desc, 3, asc),
			},
			b: orderingInfo{
				ordering: makeColumnGroups(3, asc, 5, desc, 4, desc),
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc),
		},
		{
			name: "partial-ordering-with-groups",
			a: orderingInfo{
				ordering: makeColumnGroups(1, 8, asc, 3, 9, asc, 2, desc),
			},
			b: orderingInfo{
				ordering: makeColumnGroups(3, 5, asc, 4, desc),
			},
			colA:     []int{8, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc),
		},
		{
			name: "grouped-cols",
			a: orderingInfo{
				ordering: makeColumnGroups(0, 2, asc, 1, 3, asc),
			},
			b: orderingInfo{
				ordering: makeColumnGroups(0, asc, 1, asc, 2, asc, 3, asc),
			},
			colA:     []int{0, 1, 2, 3},
			colB:     []int{0, 1, 2, 3},
			expected: makeColumnOrdering(0, asc, 1, asc, 2, asc, 3, asc),
		},
		{
			name: "grouped-cols-opposite-order",
			a: orderingInfo{
				ordering: makeColumnGroups(0, 2, asc, 1, 3, asc),
			},
			b: orderingInfo{
				ordering: makeColumnGroups(0, asc, 1, asc, 2, desc, 3, desc),
			},
			colA:     []int{0, 1, 2, 3},
			colB:     []int{0, 1, 2, 3},
			expected: makeColumnOrdering(0, asc, 1, asc, 2, desc, 3, desc),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := computeMergeJoinOrdering(tc.a, tc.b, tc.colA, tc.colB)
			if !reflect.DeepEqual(tc.expected, result) {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
		})
	}
}
