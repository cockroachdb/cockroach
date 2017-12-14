// Copyright 2017 The Cockroach Authors.
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

// This file defines structures and basic functionality that is useful when
// building distsql plans. It does not contain the actual physical planning
// code.

package distsqlplan

import (
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestProjectionAndRendering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We don't care about actual types, so we use ColumnType.Locale to store an
	// arbitrary string.
	strToType := func(s string) sqlbase.ColumnType {
		return sqlbase.ColumnType{Locale: &s}
	}

	// For each test case we set up processors with a certain post-process spec,
	// run a function that adds a projection or a rendering, and verify the output
	// post-process spec (as well as ResultTypes, Ordering).
	testCases := []struct {
		// post-process spec of the last stage in the plan.
		post distsqlrun.PostProcessSpec
		// Comma-separated list of result "types".
		resultTypes string
		// ordering in a string like "0,1,-2" (negative values = descending). Can't
		// express descending on column 0, deal with it.
		ordering string

		// function that applies a projection or rendering.
		action func(p *PhysicalPlan)

		// expected post-process spec of the last stage in the resulting plan.
		expPost distsqlrun.PostProcessSpec
		// expected result types, same format and strings as resultTypes.
		expResultTypes string
		// expected ordeering, same format as ordering.
		expOrdering string
	}{
		{
			// Simple projection.
			post:        distsqlrun.PostProcessSpec{},
			resultTypes: "A,B,C,D",

			action: func(p *PhysicalPlan) {
				p.AddProjection([]uint32{1, 3, 2})
			},

			expPost: distsqlrun.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{1, 3, 2},
			},
			expResultTypes: "B,D,C",
		},

		{
			// Projection with ordering.
			post:        distsqlrun.PostProcessSpec{},
			resultTypes: "A,B,C,D",
			ordering:    "2",

			action: func(p *PhysicalPlan) {
				p.AddProjection([]uint32{2})
			},

			expPost: distsqlrun.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{2},
			},
			expResultTypes: "C",
			expOrdering:    "0",
		},

		{
			// Projection with ordering that refers to non-projected column.
			post:        distsqlrun.PostProcessSpec{},
			resultTypes: "A,B,C,D",
			ordering:    "2,-1,3",

			action: func(p *PhysicalPlan) {
				p.AddProjection([]uint32{2, 3})
			},

			expPost: distsqlrun.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{2, 3, 1},
			},
			expResultTypes: "C,D,B",
			expOrdering:    "0,-2,1",
		},

		{
			// Projection after projection.
			post: distsqlrun.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{5, 6, 7, 8},
			},
			resultTypes: "A,B,C,D",
			ordering:    "3",

			action: func(p *PhysicalPlan) {
				p.AddProjection([]uint32{3, 1})
			},

			expPost: distsqlrun.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{8, 6},
			},
			expResultTypes: "D,B",
			expOrdering:    "0",
		},

		{
			// Projection after projection; ordering refers to non-projected column.
			post: distsqlrun.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{5, 6, 7, 8},
			},
			resultTypes: "A,B,C,D",
			ordering:    "0,3",

			action: func(p *PhysicalPlan) {
				p.AddProjection([]uint32{3, 1})
			},

			expPost: distsqlrun.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{8, 6, 5},
			},
			expResultTypes: "D,B,A",
			expOrdering:    "2,0",
		},

		{
			// Projection after rendering.
			post: distsqlrun.PostProcessSpec{
				RenderExprs: []distsqlrun.Expression{{Expr: "@5"}, {Expr: "@1 + @2"}, {Expr: "@6"}},
			},
			resultTypes: "A,B,C",
			ordering:    "2",

			action: func(p *PhysicalPlan) {
				p.AddProjection([]uint32{2, 0})
			},

			expPost: distsqlrun.PostProcessSpec{
				RenderExprs: []distsqlrun.Expression{{Expr: "@6"}, {Expr: "@5"}},
			},
			expResultTypes: "C,A",
			expOrdering:    "0",
		},

		{
			// Projection after rendering; ordering refers to non-projected column.
			post: distsqlrun.PostProcessSpec{
				RenderExprs: []distsqlrun.Expression{{Expr: "@5"}, {Expr: "@1 + @2"}, {Expr: "@6"}},
			},
			resultTypes: "A,B,C",
			ordering:    "2,-1",

			action: func(p *PhysicalPlan) {
				p.AddProjection([]uint32{2})
			},

			expPost: distsqlrun.PostProcessSpec{
				RenderExprs: []distsqlrun.Expression{{Expr: "@6"}, {Expr: "@1 + @2"}},
			},
			expResultTypes: "C,B",
			expOrdering:    "0,-1",
		},

		{
			// Identity rendering.
			post:        distsqlrun.PostProcessSpec{},
			resultTypes: "A,B,C,D",

			action: func(p *PhysicalPlan) {
				p.AddRendering(
					[]tree.TypedExpr{
						&tree.IndexedVar{Idx: 10},
						&tree.IndexedVar{Idx: 11},
						&tree.IndexedVar{Idx: 12},
						&tree.IndexedVar{Idx: 13},
					},
					nil,
					[]int{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3},
					[]sqlbase.ColumnType{strToType("A"), strToType("B"), strToType("C"), strToType("D")},
				)
			},

			expPost:        distsqlrun.PostProcessSpec{},
			expResultTypes: "A,B,C,D",
		},

		{
			// Rendering that becomes projection.
			post:        distsqlrun.PostProcessSpec{},
			resultTypes: "A,B,C,D",

			action: func(p *PhysicalPlan) {
				p.AddRendering(
					[]tree.TypedExpr{
						&tree.IndexedVar{Idx: 11},
						&tree.IndexedVar{Idx: 13},
						&tree.IndexedVar{Idx: 12},
					},
					nil,
					[]int{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3},
					[]sqlbase.ColumnType{strToType("B"), strToType("D"), strToType("C")},
				)
			},

			expPost: distsqlrun.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{1, 3, 2},
			},
			expResultTypes: "B,D,C",
		},

		{
			// Rendering with ordering that refers to non-projected column.
			post:        distsqlrun.PostProcessSpec{},
			resultTypes: "A,B,C,D",
			ordering:    "3",

			action: func(p *PhysicalPlan) {
				p.AddRendering(
					[]tree.TypedExpr{
						&tree.BinaryExpr{
							Operator: tree.Plus,
							Left:     &tree.IndexedVar{Idx: 1},
							Right:    &tree.IndexedVar{Idx: 2},
						},
					},
					nil,
					[]int{0, 1, 2},
					[]sqlbase.ColumnType{strToType("X")},
				)
			},

			expPost: distsqlrun.PostProcessSpec{
				RenderExprs: []distsqlrun.Expression{{Expr: "@2 + @3"}, {Expr: "@4"}},
			},
			expResultTypes: "X,D",
			expOrdering:    "1",
		},
		{
			// Rendering with ordering that refers to non-projected column after
			// projection.
			post: distsqlrun.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{5, 6, 7, 8},
			},
			resultTypes: "A,B,C,D",
			ordering:    "0,-3",

			action: func(p *PhysicalPlan) {
				p.AddRendering(
					[]tree.TypedExpr{
						&tree.BinaryExpr{
							Operator: tree.Plus,
							Left:     &tree.IndexedVar{Idx: 11},
							Right:    &tree.IndexedVar{Idx: 12},
						},
						&tree.IndexedVar{Idx: 10},
					},
					nil,
					[]int{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2},
					[]sqlbase.ColumnType{strToType("X"), strToType("A")},
				)
			},

			expPost: distsqlrun.PostProcessSpec{
				RenderExprs: []distsqlrun.Expression{{Expr: "@7 + @8"}, {Expr: "@6"}, {Expr: "@9"}},
			},
			expResultTypes: "X,A,D",
			expOrdering:    "1,-2",
		},
	}

	for testIdx, tc := range testCases {
		p := PhysicalPlan{
			Processors: []Processor{
				{Spec: distsqlrun.ProcessorSpec{Post: tc.post}},
				{Spec: distsqlrun.ProcessorSpec{Post: tc.post}},
			},
			ResultRouters: []ProcessorIdx{0, 1},
		}

		if tc.ordering != "" {
			for _, s := range strings.Split(tc.ordering, ",") {
				var o distsqlrun.Ordering_Column
				col, _ := strconv.Atoi(s)
				if col >= 0 {
					o.ColIdx = uint32(col)
					o.Direction = distsqlrun.Ordering_Column_ASC
				} else {
					o.ColIdx = uint32(-col)
					o.Direction = distsqlrun.Ordering_Column_DESC
				}
				p.MergeOrdering.Columns = append(p.MergeOrdering.Columns, o)
			}
		}

		for _, s := range strings.Split(tc.resultTypes, ",") {
			p.ResultTypes = append(p.ResultTypes, strToType(s))
		}

		tc.action(&p)

		if post := p.GetLastStagePost(); !reflect.DeepEqual(post, tc.expPost) {
			t.Errorf("%d: incorrect post:\n%s\nexpected:\n%s", testIdx, &post, &tc.expPost)
		}
		var resTypes []string
		for _, t := range p.ResultTypes {
			resTypes = append(resTypes, *t.Locale)
		}
		if r := strings.Join(resTypes, ","); r != tc.expResultTypes {
			t.Errorf("%d: incorrect result types: %s expected %s", testIdx, r, tc.expResultTypes)
		}

		var ord []string
		for _, c := range p.MergeOrdering.Columns {
			i := int(c.ColIdx)
			if c.Direction == distsqlrun.Ordering_Column_DESC {
				i = -i
			}
			ord = append(ord, strconv.Itoa(i))
		}
		if o := strings.Join(ord, ","); o != tc.expOrdering {
			t.Errorf("%d: incorrect ordering: '%s' expected '%s'", testIdx, o, tc.expOrdering)
		}
	}
}

func TestMergeResultTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	empty := []sqlbase.ColumnType{}
	null := []sqlbase.ColumnType{{SemanticType: sqlbase.ColumnType_NULL}}
	typeInt := []sqlbase.ColumnType{{SemanticType: sqlbase.ColumnType_INT}}

	testData := []struct {
		name     string
		left     []sqlbase.ColumnType
		right    []sqlbase.ColumnType
		expected *[]sqlbase.ColumnType
		err      bool
	}{
		{"both empty", empty, empty, &empty, false},
		{"left empty", empty, typeInt, nil, true},
		{"right empty", typeInt, empty, nil, true},
		{"both null", null, null, &null, false},
		{"left null", null, typeInt, &typeInt, false},
		{"right null", typeInt, null, &typeInt, false},
		{"both int", typeInt, typeInt, &typeInt, false},
	}
	for _, td := range testData {
		t.Run(td.name, func(t *testing.T) {
			result, err := MergeResultTypes(td.left, td.right)
			if td.err {
				if err == nil {
					t.Fatalf("expected error, got %+v", result)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if !reflect.DeepEqual(*td.expected, result) {
				t.Fatalf("expected %+v, got %+v", *td.expected, result)
			}
		})
	}
}
