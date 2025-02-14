// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ordering

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func TestLookupJoinProvided(t *testing.T) {
	tc := testcat.New()
	if _, err := tc.ExecuteDDL(`
		CREATE TABLE t (
			c1 INT, c2 INT, c3 INT, c4 INT,
			PRIMARY KEY(c1, c2), 
			INDEX desc_idx(c1, c2 DESC) STORING(c3, c4)
		)
	`); err != nil {
		t.Fatal(err)
	}
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.NewTestingEvalContext(st)
	var f norm.Factory
	f.Init(context.Background(), evalCtx, tc)
	md := f.Metadata()
	tn := tree.NewUnqualifiedTableName("t")
	tab := md.AddTable(tc.Table(tn), tn)
	for i := 0; i < 4; i++ {
		md.AddColumn(fmt.Sprintf("input_col%d", i), types.Int)
	}

	if c1 := tab.ColumnID(0); c1 != 1 {
		t.Fatalf("unexpected ID for column c1: %d\n", c1)
	}

	const descendingIndex = 1
	idxName := md.Table(tab).Index(descendingIndex).Name()
	if idxName != "desc_idx" {
		t.Fatalf("unexpected index: %s, expected desc_idx", idxName)
	}

	c := func(cols ...opt.ColumnID) opt.ColSet {
		return opt.MakeColSet(cols...)
	}

	testCases := []struct {
		index    cat.IndexOrdinal
		keyCols  opt.ColList
		inputKey opt.ColSet
		outCols  opt.ColSet
		required string
		input    string
		provided string
	}{
		// In these tests, the input (left side of the join) has columns 5,6 and the
		// table (right side) has columns 1,2,3,4.
		//
		{ // case 1: the lookup join adds columns 3,4 from the table and retains the
			// input columns. Joining on (c1, c2) = (c5, c6).
			keyCols:  opt.ColList{5, 6},
			inputKey: c(5, 6),
			outCols:  c(3, 4, 5, 6, 7, 8),
			required: "+5,+6",
			input:    "+5,+6",
			provided: "+5,+6",
		},
		{ // case 2: the lookup join produces all columns. The provided ordering
			// on 5,6 is equivalent to an ordering on 1,2. Joining on (c1, c2) =
			// (c5, c6).
			keyCols:  opt.ColList{5, 6},
			inputKey: c(5, 6),
			outCols:  c(1, 2, 3, 4, 5, 6, 7, 8),
			required: "-(1|5),+(2|6)",
			input:    "-5,+6",
			provided: "-5,+6",
		},
		{ // case 3: the lookup join does not produce input columns 5,6; we must
			// remap the input ordering to refer to output columns 1,2 instead.
			// Joining on (c1, c2) = (c5, c6).
			keyCols:  opt.ColList{5, 6},
			inputKey: c(5, 6),
			outCols:  c(1, 2, 3, 4),
			required: "+(1|5),-(2|6)",
			input:    "+5,-6",
			provided: "+1,-2",
		},
		{ // case 4: a hybrid of the two cases above (we need to remap column 6).
			// Joining on (c1, c2) = (c5, c6).
			keyCols:  opt.ColList{5, 6},
			inputKey: c(5, 6),
			outCols:  c(1, 2, 3, 4, 5),
			required: "-(1|5),-(2|6)",
			input:    "-5,-6",
			provided: "-5,-2",
		},
		{ // case 5: the lookup join adds column c2 as an ordering column. Joining
			// on c1 = c5.
			keyCols:  opt.ColList{5},
			inputKey: c(5, 6),
			outCols:  c(2, 3, 4, 5, 6),
			required: "+(1|5),+6,+2",
			input:    "+5,+6",
			provided: "+5,+6,+2",
		},
		{ // case 6: the lookup join outputs all columns and adds column c2 as an
			// ordering column. Joining on c1 = c6.
			keyCols:  opt.ColList{6},
			inputKey: c(6),
			outCols:  c(1, 2, 3, 4, 5, 6),
			required: "-5,+(1|6),+2",
			input:    "-5,+6",
			provided: "-5,+6,+2",
		},
		{ // case 7: the lookup join does not produce input columns 5,6; we must
			// remap the input ordering to refer to output column 1 instead.
			keyCols:  opt.ColList{5},
			inputKey: c(5),
			outCols:  c(1, 2, 3, 4),
			required: "+(1|5),+2",
			input:    "+5",
			provided: "+1,+2",
		},
		{ // case 8: the lookup join preserves the input ordering and maintains the
			// ordering of the descending index on lookups. Joining on c1 = c5.
			index:    descendingIndex,
			keyCols:  opt.ColList{5},
			inputKey: c(5, 6),
			outCols:  c(2, 3, 4, 5, 6),
			required: "+(1|5),+6,-2",
			input:    "+5,+6",
			provided: "+5,+6,-2",
		},
	}

	for tcIdx, tc := range testCases {
		t.Run(fmt.Sprintf("case%d", tcIdx+1), func(t *testing.T) {
			inputFDs := props.FuncDepSet{}
			inputFDs.AddStrictKey(tc.inputKey, c(5, 6, 7, 8))
			input := &testexpr.Instance{
				Rel: &props.Relational{
					OutputCols: c(5, 6, 7, 8),
					FuncDeps:   inputFDs,
				},
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
					Index:    tc.index,
					KeyCols:  tc.keyCols,
					Cols:     tc.outCols,
				},
			)
			req := props.ParseOrderingChoice(tc.required)
			res := lookupJoinBuildProvided(f.Memo(), lookupJoin, &req).String()
			if res != tc.provided {
				t.Errorf("expected '%s', got '%s'", tc.provided, res)
			}
		})
	}
}

func TestLookupJoinCanProvide(t *testing.T) {
	tc := testcat.New()
	if _, err := tc.ExecuteDDL(`
		CREATE TABLE t (
			c1 INT, c2 INT, c3 INT, c4 INT, 
			PRIMARY KEY(c1, c2), 
			INDEX sec_idx(c3, c4, c1, c2),
			INDEX desc_idx(c1, c2 DESC) STORING(c3, c4)
		)
	`); err != nil {
		t.Fatal(err)
	}
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.NewTestingEvalContext(st)
	var f norm.Factory
	f.Init(context.Background(), evalCtx, tc)
	md := f.Metadata()
	tn := tree.NewUnqualifiedTableName("t")
	tab := md.AddTable(tc.Table(tn), tn)

	if c1 := tab.ColumnID(0); c1 != 1 {
		t.Fatalf("unexpected ID for column c1: %d\n", c1)
	}

	const secondaryIndex, descendingIndex = 1, 2
	idxName := md.Table(tab).Index(secondaryIndex).Name()
	if idxName != "sec_idx" {
		t.Fatalf("unexpected index: %s, expected sec_idx", idxName)
	}
	idxName = md.Table(tab).Index(descendingIndex).Name()
	if idxName != "desc_idx" {
		t.Fatalf("unexpected index: %s, expected desc_idx", idxName)
	}

	c := opt.MakeColSet

	errorString := func(canProvide bool) string {
		if canProvide {
			return "expected to be able to provide %s"
		}
		return "expected not to be able to provide %s"
	}

	testCases := []struct {
		idx        cat.IndexOrdinal
		keyCols    opt.ColList
		outCols    opt.ColSet
		inputKey   opt.ColSet
		required   string
		canProvide bool
	}{
		// In these tests, the input (left side of the join) has columns 5,6 and the
		// table (right side) has columns 1,2,3,4.
		//
		{ // Case 1: the ordering can project input columns.
			keyCols:    opt.ColList{5},
			outCols:    c(1, 5, 6),
			inputKey:   c(5, 6),
			required:   "+(1|5),+6",
			canProvide: true,
		},
		{ // Case 2: the ordering can project input columns.
			keyCols:    opt.ColList{5, 6},
			outCols:    c(1, 2, 5, 6),
			inputKey:   c(5, 6),
			required:   "+(1|5),+(2|6)",
			canProvide: true,
		},
		{ // Case 3: the ordering cannot project only input columns, but the lookup
			// can maintain the ordering on both input and lookup columns.
			keyCols:    opt.ColList{5},
			outCols:    c(1, 2, 5, 6),
			inputKey:   c(5, 6),
			required:   "+(1|5),+6,+2",
			canProvide: true,
		},
		{ // Case 4: the ordering cannot project only input columns, but the lookup
			// can maintain the ordering on both input and lookup columns.
			idx:        secondaryIndex,
			keyCols:    opt.ColList{5},
			outCols:    c(1, 2, 3, 4, 5, 6),
			inputKey:   c(5),
			required:   "+(3|5),+4",
			canProvide: true,
		},
		{ // Case 5: the ordering cannot project only input columns, but the lookup
			// can maintain the ordering on both input and lookup columns.
			idx:        secondaryIndex,
			keyCols:    opt.ColList{5},
			outCols:    c(1, 2, 5, 6),
			inputKey:   c(5, 6),
			required:   "+(3|5),+6,+4,+2 opt(1)",
			canProvide: true,
		},
		{ // Case 6: the ordering cannot be satisfied because the input and lookup
			// ordering columns are interleaved.
			keyCols:    opt.ColList{5},
			outCols:    c(1, 2, 5, 6),
			inputKey:   c(5, 6),
			required:   "+(1|5),+2,+6",
			canProvide: false,
		},
		{ // Case 7: the ordering cannot be satisfied because the input ordering
			// columns do not form a key over the input.
			keyCols:    opt.ColList{5},
			outCols:    c(1, 2, 5, 6),
			inputKey:   c(6),
			required:   "+(1|5),+2",
			canProvide: false,
		},
		{ // Case 8: the ordering cannot be satisfied because the lookup ordering
			// involves columns that are not part of the index.
			keyCols:    opt.ColList{5, 6},
			outCols:    c(1, 3, 5, 6),
			inputKey:   c(5, 6),
			required:   "+(1|5),+6,+3",
			canProvide: false,
		},
		{ // Case 9: the ordering cannot be satisfied because the lookup ordering
			// columns are not in index order.
			idx:        secondaryIndex,
			keyCols:    opt.ColList{5},
			outCols:    c(1, 2, 3, 4, 5, 6),
			inputKey:   c(5),
			required:   "+(3|5),+1,+4",
			canProvide: false,
		},
		{ // Case 10: the ordering cannot be satisfied because one of the lookup
			// ordering columns is sorted in the wrong direction.
			keyCols:    opt.ColList{5},
			outCols:    c(1, 2, 5, 6),
			inputKey:   c(5, 6),
			required:   "+(1|5),+6,-2",
			canProvide: false,
		},
		{ // Case 11: an ordering with a descending column can be satisfied..
			idx:        descendingIndex,
			keyCols:    opt.ColList{5},
			outCols:    c(1, 2, 5, 6),
			inputKey:   c(5, 6),
			required:   "+(1|5),+6,-2",
			canProvide: true,
		},
		{ // Case 12: the ordering cannot be satisfied because the required ordering
			// is missing index column c1.
			idx:        secondaryIndex,
			keyCols:    opt.ColList{5},
			outCols:    c(1, 2, 5, 6),
			inputKey:   c(5, 6),
			required:   "+(3|5),+6,+4,+2",
			canProvide: false,
		},
	}

	for tcIdx, tc := range testCases {
		t.Run(fmt.Sprintf("case%d", tcIdx+1), func(t *testing.T) {
			fds := props.FuncDepSet{}
			fds.AddStrictKey(tc.inputKey, c(5, 6))
			input := &testexpr.Instance{
				Rel: &props.Relational{
					OutputCols: c(5, 6),
					FuncDeps:   fds,
				},
			}
			lookupJoin := f.Memo().MemoizeLookupJoin(
				input,
				nil, /* FiltersExpr */
				&memo.LookupJoinPrivate{
					JoinType: opt.InnerJoinOp,
					Table:    tab,
					Index:    tc.idx,
					KeyCols:  tc.keyCols,
					Cols:     tc.outCols,
				},
			)
			req := props.ParseOrderingChoice(tc.required)
			canProvide := lookupJoinCanProvideOrdering(f.Memo(), lookupJoin, &req)
			if canProvide != tc.canProvide {
				t.Errorf(errorString(tc.canProvide), req)
			}
		})
	}
}

func TestTrySatisfyRequired(t *testing.T) {
	testCases := []struct {
		required string
		provided string
		prefix   string
		toExtend string
	}{
		{ // Case 1: required ordering is prefix of provided.
			required: "+1,+2,+3",
			provided: "+1,+2,+3,+4",
			prefix:   "+1,+2,+3",
			toExtend: "",
		},
		{ // Case 2: required ordering is empty.
			required: "",
			provided: "+1,-2",
			prefix:   "",
			toExtend: "",
		},
		{ // Case 3: provided ordering includes optional columns.
			required: "+1,+2,+3 opt(4,5)",
			provided: "+1,-4,+2,+5,+3",
			prefix:   "+1,-4,+2,+5,+3",
			toExtend: "",
		},
		{ // Case 4: required ordering includes equivalent columns.
			required: "+(1|4),-(2|5),+3",
			provided: "+1,-2,+3",
			prefix:   "+1,-2,+3",
			toExtend: "",
		},
		{ // Case 5: provided ordering is prefix of required.
			required: "+1,+2,+3",
			provided: "+1,+2",
			prefix:   "+1,+2",
			toExtend: "+3",
		},
		{ // Case 6: provided ordering has non-optional columns between required
			// columns.
			required: "+1,+2,+3",
			provided: "+1,+2,+4,+3",
			prefix:   "+1,+2",
			toExtend: "+3",
		},
		{ // Case 7: provided ordering column is in the wrong direction.
			required: "+1,+2,+3",
			provided: "+1,-2,+3",
			prefix:   "+1",
			toExtend: "+2,+3",
		},
		{ // Case 8: provided ordering is empty and required is non-empty.
			required: "+1",
			provided: "",
			prefix:   "",
			toExtend: "+1",
		},
	}

	expect := func(exp, got string) {
		t.Helper()
		if got != exp {
			t.Errorf("expected %s; got %s", exp, got)
		}
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("case%d", i+1), func(t *testing.T) {
			required := props.ParseOrderingChoice(tc.required)
			provided := props.ParseOrdering(tc.provided)
			prefix, toExtend := trySatisfyRequired(&required, provided)
			prefixString, toExtendString := "", ""
			if prefix != nil {
				prefixString = prefix.String()
			}
			if toExtend != nil {
				toExtendString = toExtend.String()
			}
			expect(tc.prefix, prefixString)
			expect(tc.toExtend, toExtendString)
		})
	}
}
