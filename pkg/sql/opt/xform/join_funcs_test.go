// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestCustomFuncs_makeRangeFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fb := makeFilterBuilder(t)
	f := fb.f
	col := fb.tbl.ColumnID(0)
	variable := f.ConstructVariable(col)
	intLow := tree.NewDInt(0)
	intHigh := tree.NewDInt(1)
	lowVal := f.ConstructConstVal(intLow, types.Int)
	highVal := f.ConstructConstVal(intHigh, types.Int)

	tests := []struct {
		name          string
		filter        memo.FiltersItem
		start         constraint.Key
		startBoundary constraint.SpanBoundary
		end           constraint.Key
		endBoundary   constraint.SpanBoundary
	}{
		{"lt", fb.makeFilter(f.ConstructLt(variable, highVal)),
			constraint.EmptyKey, constraint.IncludeBoundary,
			constraint.MakeKey(intHigh), constraint.ExcludeBoundary,
		},
		{"le", fb.makeFilter(f.ConstructLe(variable, highVal)),
			constraint.EmptyKey, constraint.IncludeBoundary,
			constraint.MakeKey(intHigh), constraint.IncludeBoundary,
		},
		{"gt", fb.makeFilter(f.ConstructGt(variable, lowVal)),
			constraint.MakeKey(intLow), constraint.ExcludeBoundary,
			constraint.EmptyKey, constraint.IncludeBoundary,
		},
		{"ge", fb.makeFilter(f.ConstructGe(variable, lowVal)),
			constraint.MakeKey(intLow), constraint.IncludeBoundary,
			constraint.EmptyKey, constraint.IncludeBoundary,
		},
		{"ge&lt", fb.makeFilter(f.ConstructAnd(
			f.ConstructGe(variable, lowVal), f.ConstructLt(variable, highVal))),
			constraint.MakeKey(intLow), constraint.IncludeBoundary,
			constraint.MakeKey(intHigh), constraint.ExcludeBoundary,
		},
		{"ge&le", fb.makeFilter(f.ConstructAnd(
			f.ConstructGe(variable, lowVal), f.ConstructLe(variable, highVal))),
			constraint.MakeKey(intLow), constraint.IncludeBoundary,
			constraint.MakeKey(intHigh), constraint.IncludeBoundary,
		},
		{"gt&lt", fb.makeFilter(f.ConstructAnd(
			f.ConstructGt(variable, lowVal), f.ConstructLt(variable, highVal))),
			constraint.MakeKey(intLow), constraint.ExcludeBoundary,
			constraint.MakeKey(intHigh), constraint.ExcludeBoundary,
		},
		{"gt&le", fb.makeFilter(f.ConstructAnd(
			f.ConstructGt(variable, lowVal), f.ConstructLe(variable, highVal))),
			constraint.MakeKey(intLow), constraint.ExcludeBoundary,
			constraint.MakeKey(intHigh), constraint.IncludeBoundary,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fb.o.explorer.funcs
			var sp constraint.Span
			sp.Init(tt.start, tt.startBoundary, tt.end, tt.endBoundary)
			if got := c.makeRangeFilterFromSpan(col, &sp); !reflect.DeepEqual(got, tt.filter) {
				t.Errorf("makeRangeFilter() = %v, want %v", got, tt.filter)
			}
		})
	}
}

type testFilterBuilder struct {
	t   *testing.T
	ctx *tree.EvalContext
	o   *Optimizer
	f   *norm.Factory
	tbl opt.TableID
}

func makeFilterBuilder(t *testing.T) testFilterBuilder {
	var o Optimizer
	ctx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	o.Init(&ctx, nil)
	f := o.Factory()
	cat := testcat.New()
	if _, err := cat.ExecuteDDL("CREATE TABLE a (x INT PRIMARY KEY, y INT)"); err != nil {
		t.Fatal(err)
	}
	tn := tree.NewTableNameWithSchema("t", tree.PublicSchemaName, "a")
	tbl := f.Metadata().AddTable(cat.Table(tn), tn)
	return testFilterBuilder{
		t:   t,
		ctx: &ctx,
		o:   &o,
		f:   f,
		tbl: tbl,
	}
}

func (fb *testFilterBuilder) makeFilter(expr opt.ScalarExpr) memo.FiltersItem {
	filtersItem := memo.FiltersItem{Condition: expr}
	filtersItem.PopulateProps(fb.f.Memo())
	return filtersItem
}

func TestCustomFuncs_isCanonicalFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fb := makeFilterBuilder(t)
	f := fb.f
	col := fb.tbl.ColumnID(0)
	vals := memo.ScalarListExpr{f.ConstructConstVal(tree.NewDInt(0), types.Int), f.ConstructConstVal(tree.NewDInt(0), types.Int)}
	tuple := f.ConstructTuple(vals, types.MakeTuple([]*types.T{types.Int, types.Int}))

	type args struct {
	}
	tests := []struct {
		name   string
		filter memo.FiltersItem
		want   bool
	}{
		// test that True, False, Null values are hit as const
		{name: "eq-int",
			filter: f.ConstructFiltersItem(
				f.ConstructEq(
					f.ConstructConstVal(tree.NewDInt(10), types.Int),
					f.ConstructVariable(col),
				)),
			want: true,
		},
		{name: "neq-int",
			filter: f.ConstructFiltersItem(
				f.ConstructNe(
					f.ConstructVariable(col),
					f.ConstructConstVal(tree.NewDInt(10), types.Int),
				)),
			want: false,
		},
		{name: "eq-null",
			filter: f.ConstructFiltersItem(
				f.ConstructEq(
					f.ConstructVariable(col),
					f.ConstructConstVal(tree.DNull, types.Any),
				)),
			want: true,
		},
		{name: "eq-true",
			filter: f.ConstructFiltersItem(
				f.ConstructEq(
					f.ConstructVariable(col),
					f.ConstructConstVal(tree.DBoolTrue, types.Bool),
				)),
			want: true,
		},
		{name: "neq-false",
			filter: f.ConstructFiltersItem(
				f.ConstructNot(
					f.ConstructVariable(col))),
			want: false,
		},
		{name: "in-tuple",
			filter: f.ConstructFiltersItem(
				f.ConstructIn(
					f.ConstructVariable(col),
					tuple,
				)),
			want: true,
		},
		{name: "and-eq-lt",
			filter: f.ConstructFiltersItem(
				f.ConstructAnd(
					f.ConstructEq(
						f.ConstructVariable(col),
						f.ConstructConstVal(tree.NewDInt(10), types.Int)),
					f.ConstructLt(
						f.ConstructVariable(col),
						f.ConstructConstVal(tree.NewDInt(10), types.Int)),
				)),
			want: true,
		},
		{name: "or-eq-lt",
			filter: f.ConstructFiltersItem(
				f.ConstructOr(
					f.ConstructEq(
						f.ConstructVariable(col),
						f.ConstructConstVal(tree.NewDInt(10), types.Int)),
					f.ConstructLt(
						f.ConstructVariable(col),
						f.ConstructConstVal(tree.NewDInt(10), types.Int)),
				)),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fb.o.explorer.funcs
			if got := c.isCanonicalLookupJoinFilter(tt.filter); got != tt.want {
				t.Errorf("isCanonicalLookupJoinFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}
