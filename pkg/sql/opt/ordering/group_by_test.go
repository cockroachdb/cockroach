// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func TestDistinctOnProvided(t *testing.T) {
	evalCtx := tree.NewTestingEvalContext(nil /* st */)
	var f norm.Factory
	f.Init(evalCtx, testcat.New())
	md := f.Metadata()
	for i := 1; i <= 5; i++ {
		md.AddColumn(fmt.Sprintf("c%d", i), types.Int)
	}
	c := func(cols ...opt.ColumnID) opt.ColSet {
		return opt.MakeColSet(cols...)
	}

	fd1eq5 := props.FuncDepSet{}
	fd1eq5.AddEquivalency(1, 5)

	// DistinctOn might not project all input columns, so we have three sets of
	// columns, corresponding to this SQL:
	//   SELECT <outCols> FROM
	//     SELECT DISTINCT ON(<groupingCols>) <inputCols>
	testCases := []struct {
		inCols       opt.ColSet
		inFDs        props.FuncDepSet
		outCols      opt.ColSet
		groupingCols opt.ColSet
		required     string
		internal     string
		input        string
		expected     string
	}{
		{ // case 1: Internal ordering is stronger; the provided ordering needs
			//         trimming.
			inCols:       c(1, 2, 3, 4, 5),
			inFDs:        props.FuncDepSet{},
			outCols:      c(1, 2, 3, 4, 5),
			groupingCols: c(1, 2),
			required:     "+1",
			internal:     "+1,+5",
			input:        "+1,+5",
			expected:     "+1",
		},
		{ // case 2: Projecting all input columns; ok to pass through provided.
			inCols:       c(1, 2, 3, 4, 5),
			inFDs:        fd1eq5,
			outCols:      c(1, 2, 3, 4, 5),
			groupingCols: c(1, 2),
			required:     "+(1|5)",
			internal:     "",
			input:        "+5",
			expected:     "+5",
		},
		{ // case 3: Not projecting all input columns; the provided ordering
			//         needs remapping.
			inCols:       c(1, 2, 3, 4, 5),
			inFDs:        fd1eq5,
			outCols:      c(1, 2, 3),
			groupingCols: c(1, 2),
			required:     "+(1|5)",
			internal:     "",
			input:        "+5",
			expected:     "+1",
		},
		{ // case 4: The provided ordering needs both trimming and remapping.
			inCols:       c(1, 2, 3, 4, 5),
			inFDs:        fd1eq5,
			outCols:      c(1, 2, 3),
			groupingCols: c(1, 2),
			required:     "+(1|5)",
			internal:     "+5,+4",
			input:        "+5,+4",
			expected:     "+1",
		},
	}
	for tcIdx, tc := range testCases {
		t.Run(fmt.Sprintf("case%d", tcIdx+1), func(t *testing.T) {
			input := &testexpr.Instance{
				Rel: &props.Relational{
					OutputCols: tc.outCols,
					FuncDeps:   fd1eq5,
				},
				Provided: &physical.Provided{
					Ordering: props.ParseOrdering(tc.input),
				},
			}
			p := memo.GroupingPrivate{
				GroupingCols: tc.groupingCols,
				Ordering:     props.ParseOrderingChoice(tc.internal),
			}
			var aggs memo.AggregationsExpr
			tc.outCols.Difference(tc.groupingCols).ForEach(func(col opt.ColumnID) {
				aggs = append(aggs, f.ConstructAggregationsItem(
					f.ConstructFirstAgg(f.ConstructVariable(col)),
					col,
				))
			})
			distinctOn := f.Memo().MemoizeDistinctOn(input, aggs, &p)
			req := props.ParseOrderingChoice(tc.required)
			res := distinctOnBuildProvided(distinctOn, &req).String()
			if res != tc.expected {
				t.Errorf("expected '%s', got '%s'", tc.expected, res)
			}
		})
	}
}
