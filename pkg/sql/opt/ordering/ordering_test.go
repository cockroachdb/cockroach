// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ordering

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/stretchr/testify/require"
)

func TestFinalizeProvided(t *testing.T) {
	c := func(cols ...opt.ColumnID) opt.ColSet {
		return opt.MakeColSet(cols...)
	}
	testCases := []struct {
		outCols  opt.ColSet
		required string
		provided string
		expected string
	}{
		{ // case 1
			outCols:  c(1, 2, 3),
			required: "+1,+2,+3",
			provided: "+1,+2,+3",
			expected: "+1,+2,+3",
		},
		{ // case 2
			outCols:  c(1, 3),
			required: "+(1|2),+3",
			provided: "+1,+2,+3",
			expected: "+1,+3",
		},
		{ // case 3
			outCols:  c(1, 3),
			required: "+(1|2),+3",
			provided: "+1,-2,+3",
			expected: "+1,+3",
		},
		{ // case 4
			outCols:  c(2, 4),
			required: "-(1|2),+(3|4)",
			provided: "-1,+2,+3",
			expected: "-2,+4",
		},
		{ // case 5
			outCols:  c(1, 2, 3, 4, 5),
			required: "+4,-5 opt(1,2)",
			provided: "+4,-1,-5,+2",
			expected: "+4,-5",
		},
		{ // case 6
			outCols:  c(1, 2, 3),
			required: "+1 opt(2)",
			provided: "+1,+2,+3",
			expected: "+1",
		},
		{ // case 7
			outCols:  c(1, 2, 3),
			required: "+1,+3 opt(2)",
			provided: "+1,+2,+3",
			expected: "+1,+3",
		},
		{ // case 8
			outCols:  c(2, 4, 5),
			required: "+4,-5 opt(1,2,3)",
			provided: "-2,+4,-5,+7",
			expected: "+4,-5",
		},
		{ // case 9
			outCols:  c(2, 5, 3),
			required: "+(1|2),-(3|4) opt(5)",
			provided: "+2,-5,-3,+4",
			expected: "+2,-3",
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("case%d", i+1), func(t *testing.T) {
			required := props.ParseOrderingChoice(tc.required)
			provided := props.ParseOrdering(tc.provided)
			require.Equal(t, tc.expected, finalizeProvided(provided, &required, tc.outCols).String())
		})
	}
}

// testFDs returns FDs that can be used for testing:
//   - emptyFD
//   - equivFD: (1)==(2), (2)==(1), (3)==(4), (4)==(3)
//   - constFD: ()-->(1,2)
func testFDs() (emptyFD, equivFD, constFD props.FuncDepSet) {
	equivFD.AddEquivalency(1, 2)
	equivFD.AddEquivalency(3, 4)

	constFD.AddConstants(opt.MakeColSet(1, 2))

	return emptyFD, equivFD, constFD
}
