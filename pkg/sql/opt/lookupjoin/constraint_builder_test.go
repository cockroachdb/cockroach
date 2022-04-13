// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lookupjoin_test

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/lookupjoin"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestMakeRangeFilterFromSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fb := makeFilterBuilder(t)
	col := fb.tbl.ColumnID(0)
	intLow := tree.NewDInt(0)
	intHigh := tree.NewDInt(1)
	nullKey := constraint.MakeKey(tree.DNull)

	tests := []struct {
		name          string
		filter        string
		start         constraint.Key
		startBoundary constraint.SpanBoundary
		end           constraint.Key
		endBoundary   constraint.SpanBoundary
	}{
		{"lt", "@1 < 1",
			constraint.EmptyKey, constraint.IncludeBoundary,
			constraint.MakeKey(intHigh), constraint.ExcludeBoundary,
		},
		{"le", "@1 <= 1",
			constraint.EmptyKey, constraint.IncludeBoundary,
			constraint.MakeKey(intHigh), constraint.IncludeBoundary,
		},
		{"gt", "@1 > 0",
			constraint.MakeKey(intLow), constraint.ExcludeBoundary,
			constraint.EmptyKey, constraint.IncludeBoundary,
		},
		{"ge", "@1 >= 0",
			constraint.MakeKey(intLow), constraint.IncludeBoundary,
			constraint.EmptyKey, constraint.IncludeBoundary,
		},
		{"lt-null", "@1 < 1",
			nullKey, constraint.ExcludeBoundary,
			constraint.MakeKey(intHigh), constraint.ExcludeBoundary,
		},
		{"le-null", "@1 <= 1",
			nullKey, constraint.ExcludeBoundary,
			constraint.MakeKey(intHigh), constraint.IncludeBoundary,
		},
		{"gt-null", "@1 > 0",
			constraint.MakeKey(intLow), constraint.ExcludeBoundary,
			nullKey, constraint.IncludeBoundary,
		},
		{"ge-null", "@1 >= 0",
			constraint.MakeKey(intLow), constraint.IncludeBoundary,
			nullKey, constraint.IncludeBoundary,
		},
		{"ge&lt", "@1 >= 0 AND @1 < 1",
			constraint.MakeKey(intLow), constraint.IncludeBoundary,
			constraint.MakeKey(intHigh), constraint.ExcludeBoundary,
		},
		{"ge&le", "@1 >= 0 AND @1 <= 1",
			constraint.MakeKey(intLow), constraint.IncludeBoundary,
			constraint.MakeKey(intHigh), constraint.IncludeBoundary,
		},
		{"gt&lt", "@1 > 0 AND @1 < 1",
			constraint.MakeKey(intLow), constraint.ExcludeBoundary,
			constraint.MakeKey(intHigh), constraint.ExcludeBoundary,
		},
		{"gt&le", "@1 > 0 AND @1 <= 1",
			constraint.MakeKey(intLow), constraint.ExcludeBoundary,
			constraint.MakeKey(intHigh), constraint.IncludeBoundary,
		},
	}
	var b lookupjoin.ConstraintBuilder
	b.Init(fb.f, nil, nil, 0, opt.ColSet{}, opt.ColSet{}, nil, nil)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sp constraint.Span
			sp.Init(tt.start, tt.startBoundary, tt.end, tt.endBoundary)
			want := fb.buildFilter(tt.filter)
			if got := lookupjoin.TestingMakeRangeFilterFromSpan(&b, col, &sp); !reflect.DeepEqual(got, want) {
				t.Errorf("makeRangeFilter() = %v, want %v", got, want)
			}
		})
	}
}

func TestIsCanonicalFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fb := makeFilterBuilder(t)

	tests := []struct {
		name   string
		filter string
		want   bool
	}{
		// Test that True, False, Null values are hit as const.
		{name: "eq-int",
			filter: "i = 10",
			want:   true,
		},
		{name: "neq-int",
			filter: "i != 10",
			want:   false,
		},
		{name: "eq-null",
			filter: "i = NULL",
			want:   true,
		},
		{name: "eq-true",
			filter: "b = TRUE",
			want:   true,
		},
		{name: "in-tuple",
			filter: "i IN (1,2)",
			want:   true,
		},
		{name: "and-eq-lt",
			filter: "i = 10 AND i < 10",
			want:   false,
		},
		{name: "or-eq-lt",
			filter: "i = 10 OR i < 10",
			want:   false,
		},
		{name: "and-in-lt",
			filter: "i IN (10, 20, 30) AND i > 10",
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := fb.buildFilter(tt.filter)
			if got := lookupjoin.TestingIsCanonicalLookupJoinFilter(filter); got != tt.want {
				t.Errorf("isCanonicalFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}

type testFilterBuilder struct {
	t       *testing.T
	semaCtx *tree.SemaContext
	evalCtx *tree.EvalContext
	f       *norm.Factory
	tbl     opt.TableID
}

func makeFilterBuilder(t *testing.T) testFilterBuilder {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var f norm.Factory
	f.Init(&evalCtx, nil)
	cat := testcat.New()
	if _, err := cat.ExecuteDDL("CREATE TABLE a (i INT PRIMARY KEY, b BOOL)"); err != nil {
		t.Fatal(err)
	}
	tn := tree.NewTableNameWithSchema("t", tree.PublicSchemaName, "a")
	tbl := f.Metadata().AddTable(cat.Table(tn), tn)
	return testFilterBuilder{
		t:       t,
		semaCtx: &tree.SemaContext{},
		evalCtx: &evalCtx,
		// o:       &o,
		f:   &f,
		tbl: tbl,
	}
}

func (fb *testFilterBuilder) buildFilter(str string) memo.FiltersItem {
	return testutils.BuildFilters(fb.t, fb.f, fb.semaCtx, fb.evalCtx, str)[0]
}
