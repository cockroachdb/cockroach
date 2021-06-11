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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func TestInvertedJoinProvided(t *testing.T) {
	tc := testcat.New()
	evalCtx := tree.NewTestingEvalContext(nil /* st */)
	var f norm.Factory
	f.Init(evalCtx, tc)
	md := f.Metadata()

	// Add the table with the inverted index.
	if _, err := tc.ExecuteDDL(
		"CREATE TABLE t1 (c1 INT, c2 INT, c3 GEOMETRY, PRIMARY KEY(c1, c2), INVERTED INDEX (c3))",
	); err != nil {
		t.Fatal(err)
	}
	tn := tree.NewUnqualifiedTableName("t1")
	tab := md.AddTable(tc.Table(tn), tn)

	if c1 := tab.ColumnID(0); c1 != 1 {
		t.Fatalf("unexpected ID for column c1: %d\n", c1)
	}

	// Add the table that will be used as the input to the inverted join.
	// It starts at c6 since column IDs 4 and 5 were used for the system column
	// and inverted key column in the previous table.
	if _, err := tc.ExecuteDDL(
		"CREATE TABLE t2 (c6 INT, c7 GEOMETRY)",
	); err != nil {
		t.Fatal(err)
	}
	tn = tree.NewUnqualifiedTableName("t2")
	inputTab := md.AddTable(tc.Table(tn), tn)

	if c7 := inputTab.ColumnID(0); c7 != 7 {
		t.Fatalf("unexpected ID for column c7: %d\n", c7)
	}

	c := func(cols ...opt.ColumnID) opt.ColSet {
		return opt.MakeColSet(cols...)
	}

	testCases := []struct {
		outCols  opt.ColSet
		required string
		input    string
		provided string
	}{
		// In these tests, the input (left side of the join) has columns 7,8 and the
		// index (right side) has columns 1,2,5 (where 5 is the inverted key column
		// representing 3) and the join has condition st_intersects(c8, c3) AND
		// c2 = c7.
		//
		{ // case 1: the inverted join adds columns 1,2 from the table and retains the
			// input columns.
			outCols:  c(1, 2, 7, 8),
			required: "+7,+8",
			input:    "+7,+8",
			provided: "+7,+8",
		},
		{ // case 2: same output columns as case 1. The provided ordering
			// on 7 is equivalent to an ordering on 2.
			outCols:  c(1, 2, 7, 8),
			required: "+2",
			input:    "+7",
			provided: "+7",
		},
		{ // case 3: the inverted join does not produce input column 7; we must
			// remap the input ordering to refer to output column 2 instead.
			outCols:  c(1, 2),
			required: "-2",
			input:    "-7",
			provided: "-2",
		},
	}

	for tcIdx, tc := range testCases {
		t.Run(fmt.Sprintf("case%d", tcIdx+1), func(t *testing.T) {
			input := &testexpr.Instance{
				Rel: &props.Relational{},
				Provided: &physical.Provided{
					Ordering: props.ParseOrdering(tc.input),
				},
			}
			args := memo.ScalarListExpr{
				f.ConstructVariable(opt.ColumnID(8)), f.ConstructVariable(opt.ColumnID(3)),
			}
			name := "st_intersects"
			funcProps, overload, ok := memo.FindFunction(&args, name)
			if !ok {
				panic(errors.AssertionFailedf("could not find overload for %s", name))
			}
			invertedExpr := f.ConstructFunction(args, &memo.FunctionPrivate{
				Name:       name,
				Typ:        types.Bool,
				Properties: funcProps,
				Overload:   overload,
			})

			invertedJoin := f.Memo().MemoizeInvertedJoin(
				input,
				memo.FiltersExpr{
					f.ConstructFiltersItem(f.ConstructEq(
						f.ConstructVariable(opt.ColumnID(2)), f.ConstructVariable(opt.ColumnID(7)),
					)),
				},
				&memo.InvertedJoinPrivate{
					JoinType:     opt.InnerJoinOp,
					InvertedExpr: invertedExpr,
					Table:        tab,
					Index:        1,
					Cols:         tc.outCols,
				},
			)
			req := props.ParseOrderingChoice(tc.required)
			res := invertedJoinBuildProvided(invertedJoin, &req).String()
			if res != tc.provided {
				t.Errorf("expected '%s', got '%s'", tc.provided, res)
			}
		})
	}
}
