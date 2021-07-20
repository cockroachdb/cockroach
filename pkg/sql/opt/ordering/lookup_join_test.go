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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestLookupJoinProvided(t *testing.T) {
	tc := testcat.New()
	if _, err := tc.ExecuteDDL(
		"CREATE TABLE t (c1 INT, c2 INT, c3 INT, c4 INT, PRIMARY KEY(c1, c2))",
	); err != nil {
		t.Fatal(err)
	}
	evalCtx := tree.NewTestingEvalContext(nil /* st */)
	var f norm.Factory
	f.Init(evalCtx, tc)
	md := f.Metadata()
	tn := tree.NewUnqualifiedTableName("t")
	tab := md.AddTable(tc.Table(tn), tn)

	if c1 := tab.ColumnID(0); c1 != 1 {
		t.Fatalf("unexpected ID for column c1: %d\n", c1)
	}

	c := func(cols ...opt.ColumnID) opt.ColSet {
		return opt.MakeColSet(cols...)
	}

	testCases := []struct {
		keyCols  opt.ColList
		outCols  opt.ColSet
		required string
		input    string
		provided string
	}{
		// In these tests, the input (left side of the join) has columns 5,6 and the
		// table (right side) has columns 1,2,3,4 and the join has condition
		// (c5, c6) = (c1, c2).
		//
		{ // case 1: the lookup join adds columns 3,4 from the table and retains the
			// input columns.
			keyCols:  opt.ColList{5, 6},
			outCols:  c(3, 4, 5, 6),
			required: "+5,+6",
			input:    "+5,+6",
			provided: "+5,+6",
		},
		{ // case 2: the lookup join produces all columns. The provided ordering
			// on 5,6 is equivalent to an ordering on 1,2.
			keyCols:  opt.ColList{5, 6},
			outCols:  c(1, 2, 3, 4, 5, 6),
			required: "-1,+2",
			input:    "-5,+6",
			provided: "-5,+6",
		},
		{ // case 3: the lookup join does not produce input columns 5,6; we must
			// remap the input ordering to refer to output columns 1,2 instead.
			keyCols:  opt.ColList{5, 6},
			outCols:  c(1, 2, 3, 4),
			required: "+1,-2",
			input:    "+5,-6",
			provided: "+1,-2",
		},
		{ // case 4: a hybrid of the two cases above (we need to remap column 6).
			keyCols:  opt.ColList{5, 6},
			outCols:  c(1, 2, 3, 4, 5),
			required: "-1,-2",
			input:    "-5,-6",
			provided: "-5,-2",
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
			lookupJoin := f.Memo().MemoizeLookupJoin(
				input,
				nil, /* FiltersExpr */
				&memo.LookupJoinPrivate{
					JoinType: opt.InnerJoinOp,
					Table:    tab,
					Index:    cat.PrimaryIndex,
					KeyCols:  tc.keyCols,
					Cols:     tc.outCols,
				},
			)
			req := props.ParseOrderingChoice(tc.required)
			res := lookupJoinBuildProvided(lookupJoin, &req).String()
			if res != tc.provided {
				t.Errorf("expected '%s', got '%s'", tc.provided, res)
			}
		})
	}
}
