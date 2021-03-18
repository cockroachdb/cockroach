package xform

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

func Test_remapOrderingSetColumns(t *testing.T) {
	var os1 opt.OrderingSet
	os1.Add(opt.Ordering{1})

	testCases := []struct {
		os        string
		outCols   opt.ColSet
		equivCols [][2]opt.ColumnID
		expected  string
	}{
		{
			os:       "(+1)",
			outCols:  c(1),
			expected: "(+1)",
		},
		{
			os:       "(+1)",
			outCols:  c(2),
			expected: "(+1)",
		},
		{
			os:        "(+1)",
			outCols:   c(2),
			equivCols: [][2]opt.ColumnID{{1, 2}},
			expected:  "(+2)",
		},
		{
			os:        "(+1,+2) (+1,-2)",
			outCols:   c(3, 4),
			equivCols: [][2]opt.ColumnID{{1, 3}, {2, 4}},
			expected:  "(+3,+4) (+3,-4)",
		},
		{
			os:        "(+1,-2) (+2,-1)",
			outCols:   c(3, 4),
			equivCols: [][2]opt.ColumnID{{1, 3}, {2, 4}},
			expected:  "(+3,-4) (+4,-3)",
		},
		{
			os:        "(+1,-2) (+1,-3)",
			outCols:   c(3, 4),
			equivCols: [][2]opt.ColumnID{{1, 3}, {2, 4}},
			expected:  "(+3,-4) (+1,-3)",
		},
		{
			os:        "(+1,+2) (+1,-2)",
			outCols:   c(1, 4),
			equivCols: [][2]opt.ColumnID{{1, 3}, {2, 4}},
			expected:  "(+1,+4) (+1,-4)",
		},
		{
			os:        "(+1,+2) (+1,-2)",
			outCols:   c(3, 4),
			equivCols: [][2]opt.ColumnID{{2, 4}},
			expected:  "(+1,+4) (+1,-4)",
		},
		{
			os:        "(+1,+2,+3) (-3,-2,-1)",
			outCols:   c(1, 3, 4),
			equivCols: [][2]opt.ColumnID{{2, 4}},
			expected:  "(+1,+4,+3) (-3,-4,-1)",
		},
	}

	for _, tc := range testCases {
		fds := &props.FuncDepSet{}
		for _, cols := range tc.equivCols {
			fds.AddEquivalency(cols[0], cols[1])
		}
		os := parseOrderingSet(tc.os)
		beforeRemap := os.String()
		cpy := os.Copy()
		remapOrderingSetColumns(cpy, fds, tc.outCols)
		if actual := cpy.String(); actual != tc.expected {
			t.Errorf("expected %s; got %s", tc.expected, actual)
		}
		if beforeRemap != os.String() {
			t.Errorf("expected original ordering set %s to be unchanged; but was changed to %s", beforeRemap, os.String())
		}
	}
}

func parseOrderingSet(s string) (os opt.OrderingSet) {
	for _, ordStr := range strings.Split(s, " ") {
		ordStr = strings.Trim(ordStr, "()")
		ord := physical.ParseOrdering(ordStr)
		os.Add(ord)
	}
	return os
}

func c(cols ...opt.ColumnID) opt.ColSet {
	return opt.MakeColSet(cols...)
}
