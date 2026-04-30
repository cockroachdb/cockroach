// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memo

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func makePlaceholder() opt.ScalarExpr {
	return &PlaceholderExpr{Value: &tree.Placeholder{Idx: 0}, Typ: types.Int}
}

func makeVariable(col opt.ColumnID) opt.ScalarExpr {
	return &VariableExpr{Col: col, Typ: types.Int}
}

func makeTuple(elems ...opt.ScalarExpr) opt.ScalarExpr {
	return &TupleExpr{Elems: elems, Typ: types.MakeTuple([]*types.T{types.Int})}
}

func makeAndExpr() opt.ScalarExpr {
	return &AndExpr{Left: makeConstInt(1), Right: makeConstInt(2)}
}

func makeConstInt(val int) opt.ScalarExpr {
	return &ConstExpr{Value: tree.NewDInt(tree.DInt(val)), Typ: types.Int}
}

func TestIsConstantsAndPlaceholders(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name string
		list ScalarListExpr
		want bool
	}{
		{"empty", ScalarListExpr{}, true},
		{"constants only", ScalarListExpr{makeConstInt(1), makeConstInt(2)}, true},
		{"placeholders only", ScalarListExpr{makePlaceholder(), makePlaceholder()}, true},
		{"mixed constants and placeholders", ScalarListExpr{makeConstInt(1), makePlaceholder()}, true},
		{"tuple of constants", ScalarListExpr{makeTuple(makeConstInt(1), makeConstInt(2))}, true},
		{"tuple of placeholders", ScalarListExpr{makeTuple(makePlaceholder())}, true},
		{"tuple of mixed", ScalarListExpr{makeTuple(makeConstInt(1), makePlaceholder())}, true},
		{
			"nested tuple",
			ScalarListExpr{makeTuple(makeTuple(makeConstInt(1)))},
			false,
		},
		{"variable rejected", ScalarListExpr{makeVariable(1)}, false},
		{"non-const expr rejected", ScalarListExpr{makeAndExpr()}, false},
		{
			"constant and variable rejected",
			ScalarListExpr{makeConstInt(1), makeVariable(1)},
			false,
		},
		{
			"tuple containing variable rejected",
			ScalarListExpr{makeTuple(makeVariable(1))},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.list.IsConstantsAndPlaceholders(); got != tt.want {
				t.Errorf("IsConstantsAndPlaceholders() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsConstantsAndPlaceholdersAndVariables(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name string
		list ScalarListExpr
		want bool
	}{
		{"empty", ScalarListExpr{}, true},
		{"constants only", ScalarListExpr{makeConstInt(1), makeConstInt(2)}, true},
		{"placeholders only", ScalarListExpr{makePlaceholder(), makePlaceholder()}, true},
		{"variables only", ScalarListExpr{makeVariable(1), makeVariable(2)}, true},
		{
			"mixed constants, placeholders, and variables",
			ScalarListExpr{makeConstInt(1), makePlaceholder(), makeVariable(1)},
			true,
		},
		{"tuple of constants", ScalarListExpr{makeTuple(makeConstInt(1))}, true},
		{"tuple of variables", ScalarListExpr{makeTuple(makeVariable(1))}, true},
		{
			"tuple of mixed",
			ScalarListExpr{makeTuple(makeConstInt(1), makePlaceholder(), makeVariable(1))},
			true,
		},
		{
			"nested tuple",
			ScalarListExpr{makeTuple(makeTuple(makeConstInt(1)))},
			false,
		},
		{"non-const expr rejected", ScalarListExpr{makeAndExpr()}, false},
		{
			"tuple containing non-const expr rejected",
			ScalarListExpr{makeTuple(makeAndExpr())},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.list.IsConstantsAndPlaceholdersAndVariables(); got != tt.want {
				t.Errorf("IsConstantsAndPlaceholdersAndVariables() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsSortedUniqueList(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	evalCtx := eval.MakeTestingEvalContext(nil)

	tests := []struct {
		name string
		list ScalarListExpr
		want bool
	}{
		{"empty", ScalarListExpr{}, true},
		{"single", ScalarListExpr{makeConstInt(5)}, true},
		{"sorted unique", ScalarListExpr{makeConstInt(1), makeConstInt(2), makeConstInt(3)}, true},
		{"unsorted", ScalarListExpr{makeConstInt(3), makeConstInt(1), makeConstInt(2)}, false},
		{"duplicates", ScalarListExpr{makeConstInt(1), makeConstInt(1), makeConstInt(2)}, false},
		{"reverse", ScalarListExpr{makeConstInt(3), makeConstInt(2), makeConstInt(1)}, false},
		{"duplicate at end", ScalarListExpr{makeConstInt(1), makeConstInt(2), makeConstInt(2)}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsSortedUniqueList(ctx, &evalCtx, tt.list); got != tt.want {
				t.Errorf("IsSortedUniqueList() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConstructSortedUniqueList(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	evalCtx := eval.MakeTestingEvalContext(nil)

	tests := []struct {
		name     string
		list     ScalarListExpr
		wantVals []int
	}{
		{"already sorted unique", ScalarListExpr{makeConstInt(1), makeConstInt(2), makeConstInt(3)}, []int{1, 2, 3}},
		{"unsorted", ScalarListExpr{makeConstInt(3), makeConstInt(1), makeConstInt(2)}, []int{1, 2, 3}},
		{"with duplicates", ScalarListExpr{makeConstInt(2), makeConstInt(1), makeConstInt(2), makeConstInt(3), makeConstInt(1)}, []int{1, 2, 3}},
		{"all duplicates", ScalarListExpr{makeConstInt(5), makeConstInt(5), makeConstInt(5)}, []int{5}},
		{"single element", ScalarListExpr{makeConstInt(7)}, []int{7}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			origLen := len(tt.list)

			result, typ := ConstructSortedUniqueList(ctx, &evalCtx, tt.list)

			if len(result) != len(tt.wantVals) {
				t.Fatalf("got %d elements, want %d", len(result), len(tt.wantVals))
			}
			for i, want := range tt.wantVals {
				got := int(*ExtractConstDatum(result[i]).(*tree.DInt))
				if got != want {
					t.Errorf("result[%d] = %d, want %d", i, got, want)
				}
			}

			// Verify returned type is a tuple with correct length.
			if typ.Family() != types.TupleFamily {
				t.Fatalf("expected TupleFamily, got %s", typ.Family())
			}
			if len(typ.TupleContents()) != len(tt.wantVals) {
				t.Errorf("tuple has %d contents, want %d", len(typ.TupleContents()), len(tt.wantVals))
			}

			// Verify the original list was not mutated.
			if len(tt.list) != origLen {
				t.Errorf("original list length changed from %d to %d", origLen, len(tt.list))
			}
		})
	}
}
