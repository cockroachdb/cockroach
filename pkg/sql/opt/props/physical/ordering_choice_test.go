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

package physical_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/util"
)

func TestOrderingChoice_FromOrdering(t *testing.T) {
	var oc physical.OrderingChoice
	oc.FromOrdering(opt.Ordering{1, -2, 3})
	if exp, actual := "+1,-2,+3", oc.String(); exp != actual {
		t.Errorf("expected %s, got %s", exp, actual)
	}

	oc.FromOrderingWithOptCols(opt.Ordering{1, -2, 3, 4, -5}, util.MakeFastIntSet(1, 3, 5))
	if exp, actual := "-2,+4 opt(1,3,5)", oc.String(); exp != actual {
		t.Errorf("expected %s, got %s", exp, actual)
	}
}

func TestOrderingChoice_ToOrdering(t *testing.T) {
	testcases := []struct {
		s string
		o opt.Ordering
	}{
		{s: " ", o: opt.Ordering{}},
		{s: "+1", o: opt.Ordering{1}},
		{s: "-1,+(2|3) opt(4,5)", o: opt.Ordering{-1, 2}},
		{s: "+(1|2),-(3|4),+5", o: opt.Ordering{1, -3, 5}},
	}

	for _, tc := range testcases {
		choice := physical.ParseOrderingChoice(tc.s)
		ordering := choice.ToOrdering()
		if len(ordering) != len(tc.o) {
			t.Errorf("%s: expected %s, actual: %s", tc.s, tc.o, ordering)
		} else {
			for i := range ordering {
				if ordering[i] != tc.o[i] {
					t.Errorf("%s: expected %s, actual: %s", tc.s, tc.o, ordering)
				}
			}
		}
	}
}

func TestOrderingChoice_ColSet(t *testing.T) {
	testcases := []struct {
		s  string
		cs opt.ColSet
	}{
		{s: "", cs: util.MakeFastIntSet()},
		{s: "+1", cs: util.MakeFastIntSet(1)},
		{s: "-1,+(2|3) opt(4,5)", cs: util.MakeFastIntSet(1, 2, 3)},
		{s: "+(1|2),-(3|4),+5", cs: util.MakeFastIntSet(1, 2, 3, 4, 5)},
	}

	for _, tc := range testcases {
		choice := physical.ParseOrderingChoice(tc.s)
		colSet := choice.ColSet()
		if !colSet.Equals(tc.cs) {
			t.Errorf("%s: expected %s, actual: %s", tc.s, tc.cs, colSet)
		}
	}
}

func TestOrderingChoice_Implies(t *testing.T) {
	testcases := []struct {
		left     string
		right    string
		expected bool
	}{
		{left: "", right: "", expected: true},
		{left: "+1", right: "", expected: true},
		{left: "+1", right: "+1", expected: true},
		{left: "+1,-2", right: "+1", expected: true},
		{left: "+1,-2", right: "+1,-2", expected: true},
		{left: "+1", right: "+1 opt(2)", expected: true},
		{left: "-2,+1", right: "+1 opt(2)", expected: true},
		{left: "+1", right: "+(1|2)", expected: true},
		{left: "+(1|2)", right: "+(1|2|3)", expected: true},
		{left: "+(1|2),-4", right: "+(1|2|3),-(4|5)", expected: true},
		{left: "+(1|2) opt(4)", right: "+(1|2|3) opt(4)", expected: true},

		{left: "", right: "+1", expected: false},
		{left: "+1", right: "-1", expected: false},
		{left: "+1", right: "-1,-2", expected: false},
		{left: "+1 opt(2)", right: "+1", expected: false},
		{left: "+1 opt(2)", right: "+1 opt(3)", expected: false},
		{left: "+(1|2)", right: "-(1|2)", expected: false},
		{left: "+(1|2)", right: "+(3|4)", expected: false},
		{left: "+(1|2)", right: "+(2|3)", expected: false},
		{left: "+(1|2|3)", right: "+(1|2)", expected: false},
		{left: "+(1|2)", right: "+1 opt(2)", expected: false},
		{left: "+(1|2),-(3|4)", right: "+(1|2),-(3|4),+5", expected: false},
		{left: "+1", right: "+3 opt(1,2)", expected: false},
		{left: "+3 opt(1,2)", right: "+1", expected: false},
	}

	for _, tc := range testcases {
		left := physical.ParseOrderingChoice(tc.left)
		right := physical.ParseOrderingChoice(tc.right)
		if left.Implies(&right) != tc.expected {
			if tc.expected {
				t.Errorf("expected %s to imply %s", tc.left, tc.right)
			} else {
				t.Errorf("expected %s to not imply %s", tc.left, tc.right)
			}
		}
	}
}

// TestOrderingChoice_Intersection tests Intersects and Intersection.
func TestOrderingChoice_Intersection(t *testing.T) {
	testcases := []struct {
		left           string
		right          string
		expected       string
		nonCommutative bool
	}{
		{left: "", right: "", expected: ""},
		{left: "+1", right: "", expected: "+1"},
		{left: "+1 opt(2)", right: "", expected: "+1 opt(2)"},
		{left: "+1", right: "+1", expected: "+1"},
		{left: "+1,-2", right: "+1", expected: "+1,-2"},
		{left: "+1,-2", right: "+1,-2", expected: "+1,-2"},
		{left: "+1", right: "+1 opt(2)", expected: "+1"},
		{left: "+1", right: "+2 opt(1)", expected: "+1,+2"},
		{left: "-2,+1", right: "+1 opt(2)", expected: "-2,+1"},
		{left: "+1", right: "+(1|2)", expected: "+1"},
		{left: "+(1|2)", right: "+(1|2|3)", expected: "+(1|2)"},
		{left: "+(1|2),-4", right: "+(1|2|3),-(4|5)", expected: "+(1|2),-4"},
		{left: "+(1|2) opt(4)", right: "+(1|2|3) opt(4)", expected: "+(1|2) opt(4)"},

		{left: "+1 opt(2,3,4)", right: "+1 opt(4,5)", expected: "+1 opt(4)"},
		{left: "+1 opt(2,3,4)", right: "+1 opt(4,5)", expected: "+1 opt(4)"},
		{left: "+1,+4,+5", right: "+4,+5 opt(1)", expected: "+1,+4,+5"},
		{left: "+(1|2),+(3|4)", right: "+(2|3),+(4|5)", expected: "+2,+4"},
		{left: "+(1|2|3),+(4|5)", right: "+(2|3),+(4|5|6)", expected: "+(2|3),+(4|5)"},

		{left: "+1", right: "+2", expected: "NO"},
		{left: "+1", right: "+2 opt(2)", expected: "NO"},
		{left: "+1", right: "-1 opt(2)", expected: "NO"},
		{left: "+(1|2),+(3|4)", right: "+(2|5),+(6|7)", expected: "NO"},

		// Non-commutative cases.
		{
			left:           "+1 opt(2,5)",
			right:          "+2 opt(1,5)",
			expected:       "+1,+2 opt(5)",
			nonCommutative: true,
		},
		{
			left:           "+2 opt(1,5)",
			right:          "+1 opt(2,5)",
			expected:       "+2,+1 opt(5)",
			nonCommutative: true,
		},
		{
			left:           "+(1|2),+(3|4) opt(6)",
			right:          "+(2|3),+(5|6) opt(4)",
			expected:       "+2,+4,+(5|6)",
			nonCommutative: true,
		},
		{
			left:           "+(2|3),+(5|6) opt(4)",
			right:          "+(1|2),+(3|4) opt(6)",
			expected:       "+2,+6,+(3|4)",
			nonCommutative: true,
		},
		{
			left:           "+(1|2|3),-(4|5|6) opt(7)",
			right:          "-7 opt(2,3,5,6)",
			expected:       "+(2|3),-(5|6),-7",
			nonCommutative: true,
		},
		{
			left:           "-7 opt(2,3,5,6)",
			right:          "+(1|2|3),-(4|5|6) opt(7)",
			expected:       "-7,+(1|2|3),-(4|5|6)",
			nonCommutative: true,
		},
	}

	getRes := func(left, right physical.OrderingChoice) string {
		if !left.Intersects(&right) {
			return "NO"
		}
		return left.Intersection(&right).String()
	}

	for _, tc := range testcases {
		left := physical.ParseOrderingChoice(tc.left)
		right := physical.ParseOrderingChoice(tc.right)

		res := getRes(left, right)
		if res != tc.expected {
			t.Errorf(
				"intersection between '%s' and '%s': expected '%s', got '%s'",
				left, right, tc.expected, res,
			)
		}
		if !tc.nonCommutative {
			if res2 := getRes(right, left); res2 != res {
				t.Errorf(
					"intersection not commutative: left='%s' right='%s': '%s' vs '%s'",
					left, right, res, res2,
				)
			}
		}
	}
}

func TestOrderingChoice_SubsetOfCols(t *testing.T) {
	testcases := []struct {
		s        string
		cs       opt.ColSet
		expected bool
	}{
		{s: "", cs: util.MakeFastIntSet(), expected: true},
		{s: "", cs: util.MakeFastIntSet(1), expected: true},
		{s: "+1", cs: util.MakeFastIntSet(1), expected: true},
		{s: "-1", cs: util.MakeFastIntSet(1, 2), expected: true},
		{s: "+1 opt(2)", cs: util.MakeFastIntSet(1), expected: false},
		{s: "+1 opt(2)", cs: util.MakeFastIntSet(1, 2), expected: true},
		{s: "+(1|2)", cs: util.MakeFastIntSet(1, 2, 3), expected: true},
		{s: "+(1|2)", cs: util.MakeFastIntSet(2), expected: false},
		{s: "+1,-(2|3),-4 opt(4,5)", cs: util.MakeFastIntSet(1, 3, 4), expected: false},
		{s: "+1,-(2|3),-4 opt(4,5)", cs: util.MakeFastIntSet(1, 2, 3, 4), expected: false},
		{s: "+1,-(2|3),-4 opt(4,5)", cs: util.MakeFastIntSet(1, 2, 3, 4, 5), expected: true},
	}

	for _, tc := range testcases {
		choice := physical.ParseOrderingChoice(tc.s)
		if choice.SubsetOfCols(tc.cs) != tc.expected {
			if tc.expected {
				t.Errorf("%s: expected cols to be subset of %s", tc.s, tc.cs)
			} else {
				t.Errorf("%s: expected cols to not be subset of %s", tc.s, tc.cs)
			}
		}
	}
}

func TestOrderingChoice_CanProjectCols(t *testing.T) {
	testcases := []struct {
		s        string
		cs       opt.ColSet
		expected bool
	}{
		{s: "", cs: util.MakeFastIntSet(), expected: true},
		{s: "", cs: util.MakeFastIntSet(1), expected: true},
		{s: "+1", cs: util.MakeFastIntSet(1), expected: true},
		{s: "-1", cs: util.MakeFastIntSet(1, 2), expected: true},
		{s: "+1 opt(2)", cs: util.MakeFastIntSet(1), expected: true},
		{s: "+(1|2)", cs: util.MakeFastIntSet(1), expected: true},
		{s: "+(1|2)", cs: util.MakeFastIntSet(2), expected: true},
		{s: "+1,-(2|3),-4 opt(4,5)", cs: util.MakeFastIntSet(1, 3, 4), expected: true},

		{s: "+1", cs: util.MakeFastIntSet(), expected: false},
		{s: "+1,+2", cs: util.MakeFastIntSet(1), expected: false},
		{s: "+(1|2)", cs: util.MakeFastIntSet(3), expected: false},
	}

	for _, tc := range testcases {
		choice := physical.ParseOrderingChoice(tc.s)
		if choice.CanProjectCols(tc.cs) != tc.expected {
			if tc.expected {
				t.Errorf("%s: expected CanProject(%s)", tc.s, tc.cs)
			} else {
				t.Errorf("%s: expected !CanProject(%s)", tc.s, tc.cs)
			}
		}
	}
}

func TestOrderingChoice_MatchesAt(t *testing.T) {
	s1 := "+1"
	s2 := "+1,-2 opt(3,4)"
	s3 := "+(1|2)"
	s4 := "+(1|2),-3,+(4|5) opt(6,7)"

	testcases := []struct {
		s        string
		idx      int
		col      opt.ColumnID
		desc     bool
		expected bool
	}{
		{s: s1, idx: 0, col: 1, desc: false, expected: true},
		{s: s1, idx: 0, col: 1, desc: true, expected: false},
		{s: s1, idx: 0, col: 2, desc: false, expected: false},

		{s: s2, idx: 0, col: 1, desc: false, expected: true},
		{s: s2, idx: 1, col: 2, desc: true, expected: true},
		{s: s2, idx: 1, col: 2, desc: false, expected: false},
		{s: s2, idx: 1, col: 3, desc: false, expected: true},
		{s: s2, idx: 0, col: 4, desc: false, expected: true},

		{s: s3, idx: 0, col: 1, desc: false, expected: true},
		{s: s3, idx: 0, col: 2, desc: false, expected: true},
		{s: s3, idx: 0, col: 2, desc: true, expected: false},
		{s: s3, idx: 0, col: 3, desc: false, expected: false},

		{s: s4, idx: 0, col: 6, desc: false, expected: true},
		{s: s4, idx: 1, col: 3, desc: true, expected: true},
		{s: s4, idx: 2, col: 5, desc: false, expected: true},
		{s: s4, idx: 2, col: 7, desc: true, expected: true},
	}

	for _, tc := range testcases {
		ordering := physical.ParseOrderingChoice(tc.s)
		ordCol := opt.MakeOrderingColumn(tc.col, tc.desc)
		if ordering.MatchesAt(tc.idx, ordCol) != tc.expected {
			if tc.expected {
				t.Errorf("expected %s to match at index %d: %s", ordCol, tc.idx, tc.s)
			} else {
				t.Errorf("expected %s not to match at index %d: %s", ordCol, tc.idx, tc.s)
			}
		}
	}
}

func TestOrderingChoice_Copy(t *testing.T) {
	ordering := physical.ParseOrderingChoice("+1,-(2|3) opt(4,5)")
	copied := ordering.Copy()
	col := physical.OrderingColumnChoice{Group: util.MakeFastIntSet(6, 7), Descending: true}
	copied.Columns = append(copied.Columns, col)

	// ()-->(8)
	// (3)==(9)
	// (9)==(3)
	var fd props.FuncDepSet
	fd.AddConstants(util.MakeFastIntSet(8))
	fd.AddEquivalency(3, 9)
	copied.Simplify(&fd)

	if ordering.String() != "+1,-(2|3) opt(4,5)" {
		t.Errorf("original was modified: %s", ordering.String())
	}

	if copied.String() != "+1,-(2|3|9),-(6|7) opt(4,5,8)" {
		t.Errorf("copy is not correct: %s", copied.String())
	}
}

func TestOrderingChoice_Simplify(t *testing.T) {
	// ()-->(4,5)
	// (1)==(1,3)
	// (2)==(1)
	// (3)==(1)
	var fd1 props.FuncDepSet
	fd1.AddConstants(util.MakeFastIntSet(4, 5))
	fd1.AddEquivalency(1, 2)
	fd1.AddEquivalency(1, 3)

	// (1)-->(1,2,3,4,5)
	// (2)-->(4)
	// (4)-->(5)
	// (2)==(3)
	// (3)==(2)
	var fd2 props.FuncDepSet
	fd2.AddStrictKey(util.MakeFastIntSet(1), util.MakeFastIntSet(1, 2, 3, 4, 5))
	fd2.AddSynthesizedCol(util.MakeFastIntSet(2), 4)
	fd2.AddSynthesizedCol(util.MakeFastIntSet(4), 5)
	fd2.AddEquivalency(2, 3)

	testcases := []struct {
		fdset    *props.FuncDepSet
		s        string
		expected string
	}{
		{fdset: &props.FuncDepSet{}, s: "", expected: ""},

		// Constants and equivalencies.
		{fdset: &fd1, s: "", expected: ""},
		{fdset: &fd1, s: "+1,+4", expected: "+(1|2|3) opt(4,5)"},
		{fdset: &fd1, s: "+2,+4", expected: "+(1|2|3) opt(4,5)"},
		{fdset: &fd1, s: "+1,+2 opt(4)", expected: "+(1|2|3) opt(4,5)"},
		{fdset: &fd1, s: "+1,+2 opt(4)", expected: "+(1|2|3) opt(4,5)"},
		{fdset: &fd1, s: "+(4|5)", expected: ""},
		{fdset: &fd1, s: "+(4|5) opt(3)", expected: ""},
		{fdset: &fd1, s: "+(4|5|6)", expected: "+6 opt(4,5)"},
		{fdset: &fd1, s: "+(4|6)", expected: "+6 opt(4,5)"},

		// Columns functionally determine one another.
		{fdset: &fd2, s: "", expected: ""},
		{fdset: &fd2, s: "+1,+2,+4", expected: "+1"},
		{fdset: &fd2, s: "+2,+4,+5", expected: "+(2|3)"},
		{fdset: &fd2, s: "+3,+5", expected: "+(2|3)"},
		{fdset: &fd2, s: "-(2|3),+1,+5", expected: "-(2|3),+1"},
		{fdset: &fd2, s: "-(2|4),+5,+1", expected: "-(2|3|4),+1"},
	}

	for _, tc := range testcases {
		ordering := physical.ParseOrderingChoice(tc.s)

		if ordering.String() != tc.expected && !ordering.CanSimplify(tc.fdset) {
			t.Errorf("%s: expected CanSimplify to be true", tc.s)
		}

		ordering.Simplify(tc.fdset)
		if ordering.String() != tc.expected {
			t.Errorf("%s: expected %s, actual %s", tc.s, tc.expected, ordering.String())
		}

		if ordering.CanSimplify(tc.fdset) {
			t.Errorf("%s: expected CanSimplify to be false", ordering.String())
		}
	}
}

func TestOrderingChoice_Truncate(t *testing.T) {
	testcases := []struct {
		s        string
		n        int
		expected string
	}{
		{s: "", n: 0, expected: ""},
		{s: "", n: 1, expected: ""},
		{s: "+1,+(2|3),-4 opt(5,6)", n: 0, expected: ""},
		{s: "+1,+(2|3),-4 opt(5,6)", n: 1, expected: "+1 opt(5,6)"},
		{s: "+1,+(2|3),-4 opt(5,6)", n: 2, expected: "+1,+(2|3) opt(5,6)"},
		{s: "+1,+(2|3),-4 opt(5,6)", n: 3, expected: "+1,+(2|3),-4 opt(5,6)"},
		{s: "+1,+(2|3),-4 opt(5,6)", n: 4, expected: "+1,+(2|3),-4 opt(5,6)"},
	}

	for _, tc := range testcases {
		choice := physical.ParseOrderingChoice(tc.s)
		choice.Truncate(tc.n)
		if choice.String() != tc.expected {
			t.Errorf("%s: n=%d, expected: %s, actual: %s", tc.s, tc.n, tc.expected, choice.String())
		}
	}
}

func TestOrderingChoice_ProjectCols(t *testing.T) {
	testcases := []struct {
		s        string
		cols     []int
		expected string
	}{
		{s: "", cols: []int{}, expected: ""},
		{s: "+1,+(2|3),-4 opt(5,6)", cols: []int{1, 2, 3, 4, 5, 6}, expected: "+1,+(2|3),-4 opt(5,6)"},
		{s: "+1,+(2|3),-4 opt(5,6)", cols: []int{1, 2, 4, 5, 6}, expected: "+1,+2,-4 opt(5,6)"},
		{s: "+1,+(2|3),-4 opt(5,6)", cols: []int{1, 3, 4, 5, 6}, expected: "+1,+3,-4 opt(5,6)"},
		{s: "+1,+(2|3),-4 opt(5,6)", cols: []int{1, 2, 4, 5}, expected: "+1,+2,-4 opt(5)"},
		{s: "+1,+(2|3),-4 opt(5,6)", cols: []int{1, 2, 4}, expected: "+1,+2,-4"},
	}

	for _, tc := range testcases {
		choice := physical.ParseOrderingChoice(tc.s)
		choice.ProjectCols(util.MakeFastIntSet(tc.cols...))
		if choice.String() != tc.expected {
			t.Errorf("%s: cols=%v, expected: %s, actual: %s", tc.s, tc.cols, tc.expected, choice.String())
		}
	}
}

func TestOrderingChoice_Equals(t *testing.T) {
	testcases := []struct {
		left     string
		right    string
		expected bool
	}{
		{left: "", right: "", expected: true},
		{left: "+1", right: "+1", expected: true},
		{left: "+1,+2", right: "+1,+2", expected: true},
		{left: "+(1|2)", right: "+(2|1)", expected: true},
		{left: "+(1|2),+3", right: "+(2|1),+3", expected: true},
		{left: "+(1|2),-(3|4) opt(5,6)", right: "+(2|1),-(4|3) opt(6,5)", expected: true},

		{left: "+1", right: "", expected: false},
		{left: "+1", right: "-1", expected: false},
		{left: "+1", right: "+2", expected: false},
		{left: "+1,+2", right: "+2,+1", expected: false},
		{left: "+1 opt(2)", right: "+1 opt(2,3)", expected: false},
	}

	for _, tc := range testcases {
		left := physical.ParseOrderingChoice(tc.left)
		right := physical.ParseOrderingChoice(tc.right)
		if left.Equals(&right) != tc.expected {
			if tc.expected {
				t.Errorf("expected %s to equal %s", tc.left, tc.right)
			} else {
				t.Errorf("expected %s to not equal %s", tc.left, tc.right)
			}
		}
	}
}
