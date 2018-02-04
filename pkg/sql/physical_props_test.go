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
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

const asc = encoding.Ascending
const desc = encoding.Descending

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

type equivGroups [][]int
type constCols []int
type notNullCols []int
type weakKeys [][]int

// makePhysProps creates a physicalProps. The parameters supported are:
//  - ColumnOrdering: ordering
//  - equivGroups: equivalency groups
//  - constCols: constant columns
//  - notNullCols: not-null columns (in addition to constant columns)
//  - weakKeys: weak key sets
//
// The parameters can be passed in any order. Examples:
//   makePhysProps(
//     equivGroups{{0,1},{2,3}},
//     constCols{4},
//     makeColumnOrdering(1, asc, 2, desc),
//   )
func makePhysProps(args ...interface{}) physicalProps {
	var props physicalProps

	// First, process equivalency groups.
	for _, a := range args {
		switch a := a.(type) {
		case equivGroups:
			// Equivalency groups.
			for _, group := range a {
				for i := 1; i < len(group); i++ {
					props.eqGroups.Union(group[0], group[i])
				}
			}
		}
	}

	// Process all other parameters.
	for _, a := range args {
		switch a := a.(type) {
		case sqlbase.ColumnOrdering:
			props.ordering = a
			for i := range props.ordering {
				props.ordering[i].ColIdx = props.eqGroups.Find(props.ordering[i].ColIdx)
			}
		case constCols:
			// Constant columns.
			for _, i := range a {
				group := props.eqGroups.Find(i)
				props.constantCols.Add(group)
				props.notNullCols.Add(group)
			}
		case notNullCols:
			for _, i := range a {
				props.notNullCols.Add(props.eqGroups.Find(i))
			}
		case weakKeys:
			props.weakKeys = make([]util.FastIntSet, len(a))
			for i := range a {
				for _, j := range a[i] {
					props.weakKeys[i].Add(props.eqGroups.Find(j))
				}
			}
		case equivGroups:
			// Already handled
		default:
			panic(fmt.Sprintf("unknown parameter type %T", a))
		}
	}
	props.check()
	return props
}

func propsEqual(a, b physicalProps) bool {
	a.check()
	b.check()
	if !a.eqGroups.Equals(b.eqGroups) ||
		!a.constantCols.Equals(b.constantCols) ||
		!a.notNullCols.Equals(b.notNullCols) ||
		len(a.weakKeys) != len(b.weakKeys) ||
		len(a.ordering) != len(b.ordering) {
		return false
	}
	// Verify the key sets match.
	for i := range a.weakKeys {
		if !a.weakKeys[i].Equals(b.weakKeys[i]) {
			return false
		}
	}

	// Verify the ordering is the same.
	for i, o := range a.ordering {
		if o != b.ordering[i] {
			return false
		}
	}
	return true
}

func TestComputeOrderingMatch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type desiredCase struct {
		line            int
		desired         sqlbase.ColumnOrdering
		expected        int
		expectedReverse int
	}

	defTestCase := func(expected, expectedReverse int, desired sqlbase.ColumnOrdering) desiredCase {
		// The line number is used to identify testcases in error messages.
		_, line, _ := caller.Lookup(1)
		return desiredCase{
			line:            line,
			desired:         desired,
			expected:        expected,
			expectedReverse: expectedReverse,
		}
	}

	type computeOrderCase struct {
		existing physicalProps
		cases    []desiredCase
	}

	testSets := []computeOrderCase{
		{
			// No existing ordering.
			existing: makePhysProps(),
			cases: []desiredCase{
				defTestCase(0, 0, makeColumnOrdering(1, desc, 5, asc)),
			},
		},
		{
			// Ordering with no constant columns.
			existing: makePhysProps(makeColumnOrdering(1, desc, 2, asc)),
			cases: []desiredCase{
				defTestCase(1, 0, makeColumnOrdering(1, desc, 5, asc)),
				defTestCase(0, 1, makeColumnOrdering(1, asc, 5, asc, 2, asc)),
			},
		},
		{
			// Ordering with column groups and no constant columns.
			existing: makePhysProps(
				equivGroups{{1, 2}, {3, 4}},
				makeColumnOrdering(1, desc, 3, asc),
			),
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
			// Ordering with no constant columns but with key.
			existing: makePhysProps(
				makeColumnOrdering(1, desc, 2, asc),
				weakKeys{{1, 2}},
				notNullCols{1, 2},
			),
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
			// Ordering with column groups, no constant columns but with key.
			existing: makePhysProps(
				equivGroups{{1, 2, 3}},
				makeColumnOrdering(1, desc, 4, asc),
				weakKeys{{1, 4}},
				notNullCols{1, 4},
			),
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
			existing: makePhysProps(
				constCols{1, 2},
			),
			cases: []desiredCase{
				defTestCase(1, 1, makeColumnOrdering(2, desc, 5, asc, 1, asc)),
				defTestCase(0, 0, makeColumnOrdering(5, asc, 2, asc)),
			},
		},
		{
			// Ordering with constant columns.
			existing: makePhysProps(
				constCols{0, 5, 6},
				makeColumnOrdering(1, desc, 2, asc),
			),
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
			existing: makePhysProps(
				equivGroups{{1, 2, 3}, {4, 5}},
				constCols{0, 8, 9},
				makeColumnOrdering(1, desc, 4, asc),
			),
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
			// Ordering with constant columns and (useless) weak key.
			existing: makePhysProps(
				constCols{0, 5, 6},
				makeColumnOrdering(1, desc, 2, asc),
				weakKeys{{1, 2}},
			),
			cases: []desiredCase{
				defTestCase(2, 0, makeColumnOrdering(1, desc, 5, asc)),
				defTestCase(2, 1, makeColumnOrdering(5, asc, 1, desc)),
				defTestCase(3, 0, makeColumnOrdering(1, desc, 5, asc, 2, asc, 7, desc)),
				defTestCase(3, 1, makeColumnOrdering(5, asc, 1, desc, 2, asc, 7, desc)),
				defTestCase(2, 2, makeColumnOrdering(0, desc, 5, asc)),
				defTestCase(2, 1, makeColumnOrdering(5, asc, 1, desc, 2, desc)),
				defTestCase(1, 0, makeColumnOrdering(1, desc, 2, desc)),
				defTestCase(5, 2, makeColumnOrdering(0, asc, 6, desc, 1, desc, 5, desc, 2, asc, 9, asc)),
				defTestCase(2, 2, makeColumnOrdering(0, asc, 6, desc, 2, asc, 5, desc, 1, desc)),
			},
		},
		{
			// Ordering with constant columns and key.
			existing: makePhysProps(
				constCols{0, 5, 6},
				makeColumnOrdering(1, desc, 2, asc),
				weakKeys{{1, 2}},
				notNullCols{1, 2},
			),
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
			// Ordering with column groups, constant columns and key.
			existing: makePhysProps(
				equivGroups{{1, 8}, {2, 9}},
				constCols{0, 5, 6},
				makeColumnOrdering(1, desc, 2, asc),
				weakKeys{{1, 2}},
				notNullCols{1, 2},
			),
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

	for tsIdx := range testSets {
		ts := testSets[tsIdx]
		for tcIdx := range ts.cases {
			tc := ts.cases[tcIdx]
			t.Run(fmt.Sprintf("line%d", tc.line), func(t *testing.T) {
				t.Parallel()
				res := ts.existing.computeMatch(tc.desired)
				resRev := ts.existing.reverse().computeMatch(tc.desired)
				if res != tc.expected || resRev != tc.expectedReverse {
					t.Errorf("expected:%d/%d got:%d/%d", tc.expected, tc.expectedReverse, res, resRev)
				}
			})
		}
	}
}

func TestTrimOrderingGuarantee(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This test verifies the guarantee of trim: before and after are equal
	// in:
	//   before := ord.computeMatch(desired)
	//   ord.public.trim(desired)
	//   after := ord.computeMatch(desired)

	genDir := func(rng *rand.Rand) encoding.Direction {
		if rng.Intn(2) == 0 {
			return encoding.Descending
		}
		return encoding.Ascending
	}

	for _, numConstCols := range []int{0, 1, 2, 4} {
		for _, numEquiv := range []int{0, 1, 2, 4, 5} {
			for _, numOrderCols := range []int{0, 1, 2, 4, 7} {
				for _, isKey := range []bool{false, true} {
					name := fmt.Sprintf("%d,%d,%d,%t", numConstCols, numEquiv, numOrderCols, isKey)
					t.Run(name, func(t *testing.T) {
						t.Parallel()
						rng, _ := randutil.NewPseudoRand()
						for tries := 0; tries < 20; tries++ {
							if numOrderCols == 0 && isKey {
								continue
							}
							o := physicalProps{}

							for i := 0; i < numEquiv; i++ {
								o.addEquivalency(rng.Intn(10), rng.Intn(10))
							}
							var used util.FastIntSet
							for i := 0; i < numConstCols; i++ {
								j := rng.Intn(10)
								for o.eqGroups.Find(j) != j {
									j = rng.Intn(10)
								}
								o.addConstantColumn(j)
								used.Add(j)
							}

							var keySet util.FastIntSet
							for i := 0; i < numOrderCols; i++ {
								for tries := 0; ; tries++ {
									// Increase the range if we can't find a new valid id.
									x := rng.Intn(10 + tries/10)
									if o.eqGroups.Find(x) == x && !used.Contains(x) {
										used.Add(x)
										keySet.Add(x)
										o.addOrderColumn(x, genDir(rng))
										break
									}
								}
							}
							if isKey {
								o.addWeakKey(keySet)
							}
							o.check()
							for _, desiredLen := range []int{0, 1, 2, 4, 5} {
								for desiredTries := 0; desiredTries < 10; desiredTries++ {
									desired := make(sqlbase.ColumnOrdering, desiredLen)
									perm := rng.Perm(10)
									for i := range desired {
										desired[i].ColIdx = perm[i]
										desired[i].Direction = genDir(rng)
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
					})
				}
			}
		}
	}
}

func TestTrimOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		props    physicalProps
		desired  sqlbase.ColumnOrdering
		expected physicalProps
	}{
		{
			name: "basic-prefix-1",
			props: makePhysProps(
				makeColumnOrdering(1, asc, 2, desc),
				weakKeys{{1, 2}},
			),
			desired: makeColumnOrdering(1, asc),
			expected: makePhysProps(
				makeColumnOrdering(1, asc),
				weakKeys{{1, 2}},
			),
		},
		{
			name: "basic-prefix-1-with-groups",
			props: makePhysProps(
				equivGroups{{1, 5}},
				makeColumnOrdering(1, asc, 2, desc),
				weakKeys{{1, 2}},
			),
			desired: makeColumnOrdering(1, asc, 5, asc),
			expected: makePhysProps(
				equivGroups{{1, 5}},
				makeColumnOrdering(1, asc),
				weakKeys{{1, 2}},
			),
		},
		{
			name: "direction-mismatch",
			props: makePhysProps(
				makeColumnOrdering(1, asc, 2, desc),
				weakKeys{{1, 2}},
			),
			desired: makeColumnOrdering(1, desc),
			expected: makePhysProps(
				weakKeys{{1, 2}},
			),
		},
		{
			name: "const-columns-1",
			props: makePhysProps(
				constCols{0, 5, 6},
				makeColumnOrdering(1, desc, 2, desc),
				weakKeys{{1, 2}},
			),
			desired: makeColumnOrdering(5, asc, 1, desc),
			expected: makePhysProps(
				constCols{0, 5, 6},
				makeColumnOrdering(1, desc),
				weakKeys{{1, 2}},
			),
		},

		{
			name: "const-columns-2",
			props: makePhysProps(
				constCols{0, 5, 6},
				makeColumnOrdering(1, desc, 2, desc, 3, asc),
				weakKeys{{1, 2, 3}},
			),
			desired: makeColumnOrdering(5, asc, 1, desc, 0, desc, 2, desc),
			expected: makePhysProps(
				constCols{0, 5, 6},
				makeColumnOrdering(1, desc, 2, desc),
				weakKeys{{1, 2, 3}},
			),
		},

		{
			name: "const-columns-with-groups",
			props: makePhysProps(
				equivGroups{{1, 7}, {2, 9}},
				constCols{0, 5, 6},
				makeColumnOrdering(1, desc, 2, desc),
				weakKeys{{1, 2}},
			),
			desired: makeColumnOrdering(5, asc, 1, desc, 6, desc, 7, asc),
			expected: makePhysProps(
				equivGroups{{1, 7}, {2, 9}},
				constCols{0, 5, 6},
				makeColumnOrdering(1, desc),
				weakKeys{{1, 2}},
			),
		},
		{
			name: "no-match",
			props: makePhysProps(
				constCols{0, 5, 6},
				makeColumnOrdering(1, desc, 2, desc, 3, asc),
				weakKeys{{1, 2, 3}},
			),
			desired: makeColumnOrdering(5, asc, 4, asc),
			expected: makePhysProps(
				constCols{0, 5, 6},
				weakKeys{{1, 2, 3}},
			),
		},
	}

	for i := range testCases {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.props.trim(tc.desired)
			if !propsEqual(tc.props, tc.expected) {
				t.Errorf("expected %s, got %s", tc.expected.AsString(nil), tc.props.AsString(nil))
			}
		})
	}
}

func TestComputeMergeJoinOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name       string
		a, b       physicalProps
		colA, colB []int
		expected   sqlbase.ColumnOrdering
	}{
		{
			name: "basic",
			a: makePhysProps(
				makeColumnOrdering(1, asc, 2, desc, 3, asc),
			),
			b: makePhysProps(
				makeColumnOrdering(3, asc, 4, desc),
			),
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc, 1, desc),
		},

		{
			name: "groups",
			a: makePhysProps(
				equivGroups{{2, 5}},
				makeColumnOrdering(1, asc, 2, desc, 3, asc),
			),
			b: makePhysProps(
				equivGroups{{2, 3}},
				makeColumnOrdering(2, asc, 4, desc),
			),
			colA:     []int{1, 5},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc, 1, desc),
		},

		{
			name: "const-a",
			a: makePhysProps(
				constCols{1, 2},
			),
			b: makePhysProps(
				makeColumnOrdering(3, asc, 4, desc),
			),
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc, 1, desc),
		},

		{
			name: "const-b",
			a: makePhysProps(
				makeColumnOrdering(1, asc, 2, desc, 3, asc),
			),
			b: makePhysProps(
				constCols{3, 4},
			),
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc, 1, desc),
		},

		{
			name: "const-with-groups",
			a: makePhysProps(
				equivGroups{{1, 4, 5}},
				makeColumnOrdering(1, asc, 2, desc, 3, asc),
			),
			b: makePhysProps(
				constCols{3, 4},
			),
			colA:     []int{5, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc, 1, desc),
		},
		{
			name: "const-both",
			a: makePhysProps(
				constCols{1},
				makeColumnOrdering(2, desc),
			),
			b: makePhysProps(
				constCols{4},
				makeColumnOrdering(3, asc),
			),
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(1, desc, 0, asc),
		},
		{
			name: "key-a",
			a: makePhysProps(
				makeColumnOrdering(1, asc),
				weakKeys{{1}},
				notNullCols{1},
			),
			b: makePhysProps(
				makeColumnOrdering(3, asc, 4, desc),
			),
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc, 1, desc),
		},

		{
			name: "key-b",
			a: makePhysProps(
				makeColumnOrdering(1, asc, 2, desc),
			),
			b: makePhysProps(
				makeColumnOrdering(3, asc),
				weakKeys{{3}},
				notNullCols{3},
			),
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc, 1, desc),
		},

		{
			name: "partial-ordering-1",
			a: makePhysProps(
				makeColumnOrdering(1, asc, 3, asc, 2, desc),
			),
			b: makePhysProps(
				makeColumnOrdering(3, asc, 4, desc),
			),
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc),
		},
		{
			name: "partial-ordering-2",
			a: makePhysProps(
				makeColumnOrdering(1, asc, 2, desc, 3, asc),
			),
			b: makePhysProps(
				makeColumnOrdering(3, asc, 5, desc, 4, desc),
			),
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc),
		},
		{
			name: "partial-ordering-with-groups",
			a: makePhysProps(
				equivGroups{{1, 8}, {3, 9}},
				makeColumnOrdering(1, asc, 3, asc, 2, desc),
			),
			b: makePhysProps(
				equivGroups{{3, 5}},
				makeColumnOrdering(3, asc, 4, desc),
			),
			colA:     []int{8, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc),
		},
		{
			name: "grouped-cols",
			a: makePhysProps(
				equivGroups{{0, 2}, {1, 3}},
				makeColumnOrdering(2, asc, 3, asc),
			),
			b: makePhysProps(
				makeColumnOrdering(0, asc, 1, asc, 2, asc, 3, asc),
			),
			colA:     []int{0, 1, 2, 3},
			colB:     []int{0, 1, 2, 3},
			expected: makeColumnOrdering(0, asc, 1, asc, 2, asc, 3, asc),
		},
		{
			name: "grouped-cols-opposite-order",
			a: makePhysProps(
				equivGroups{{0, 2}, {1, 3}},
				makeColumnOrdering(2, asc, 1, asc),
			),
			b: makePhysProps(
				makeColumnOrdering(0, asc, 1, asc, 2, desc, 3, desc),
			),
			colA:     []int{0, 1, 2, 3},
			colB:     []int{0, 1, 2, 3},
			expected: makeColumnOrdering(0, asc, 1, asc, 2, desc, 3, desc),
		},
	}
	for i := range testCases {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := computeMergeJoinOrdering(tc.a, tc.b, tc.colA, tc.colB)
			if !reflect.DeepEqual(tc.expected, result) {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
		})
	}
}

func TestProjectOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ord := makePhysProps(
		equivGroups{{0, 1, 2}, {3, 4}, {5, 6, 7}},
		constCols{3, 8, 9},
		weakKeys{{0, 5}, {5, 10}, {0, 10, 11}},
		makeColumnOrdering(0, asc, 10, asc, 5, desc),
	)
	testCases := []struct {
		columns  []int
		expected physicalProps
	}{
		{
			columns: []int{0},
			expected: makePhysProps(
				makeColumnOrdering(0, asc),
			),
		},
		{
			columns: []int{0, -1, 0, 2},
			expected: makePhysProps(
				equivGroups{{0, 2, 3}},
				makeColumnOrdering(0, asc),
			),
		},
		{
			columns: []int{2, 0},
			expected: makePhysProps(
				equivGroups{{0, 1}},
				makeColumnOrdering(1, asc),
			),
		},
		{
			columns: []int{4, 7, 1},
			expected: makePhysProps(
				constCols{0},
				weakKeys{{1, 2}},
				makeColumnOrdering(2, asc),
			),
		},
		{
			columns: []int{0, 3, 5, 10, 8},
			expected: makePhysProps(
				constCols{1, 4},
				weakKeys{{0, 2}, {2, 3}},
				makeColumnOrdering(0, asc, 3, asc, 2, desc),
			),
		},
		{
			columns: []int{0, 1, 2, 3, 4, 5, 6, 10, 8},
			expected: makePhysProps(
				equivGroups{{0, 1, 2}, {3, 4}, {5, 6}},
				constCols{3, 8},
				weakKeys{{0, 5}, {5, 7}},
				makeColumnOrdering(0, asc, 7, asc, 5, desc),
			),
		},
	}
	for tIdx := range testCases {
		tc := testCases[tIdx]
		t.Run(fmt.Sprintf("%d", tIdx), func(t *testing.T) {
			t.Parallel()
			res := ord.project(tc.columns)
			if !propsEqual(res, tc.expected) {
				t.Errorf("expected %s, got %s", tc.expected.AsString(nil), res.AsString(nil))
			}
		})
	}
}
