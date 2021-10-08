// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package physicalplan

import (
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestProjectionAndRendering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We don't care about actual types, so we use ColumnType.Locale to store an
	// arbitrary string.
	strToType := func(s string) *types.T {
		return types.MakeCollatedString(types.String, s)
	}

	// For each test case we set up processors with a certain post-process spec,
	// run a function that adds a projection or a rendering, and verify the output
	// post-process spec (as well as ResultTypes).
	testCases := []struct {
		// post-process spec of the last stage in the plan.
		post execinfrapb.PostProcessSpec
		// Comma-separated list of result "types".
		resultTypes string

		// function that applies a projection or rendering.
		action func(p *PhysicalPlan)

		// expected post-process spec of the last stage in the resulting plan.
		expPost execinfrapb.PostProcessSpec
		// expected result types, same format and strings as resultTypes.
		expResultTypes string
	}{
		// Simple projections.
		{
			post:        execinfrapb.PostProcessSpec{},
			resultTypes: "A,B,C,D",

			action: func(p *PhysicalPlan) {
				p.AddProjection([]uint32{1, 3, 2}, execinfrapb.Ordering{})
			},

			expPost: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{1, 3, 2},
			},
			expResultTypes: "B,D,C",
		},
		{
			post:        execinfrapb.PostProcessSpec{},
			resultTypes: "A,B,C,D",

			action: func(p *PhysicalPlan) {
				p.AddProjection([]uint32{2}, execinfrapb.Ordering{})
			},

			expPost: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{2},
			},
			expResultTypes: "C",
		},

		// Projection after projection.
		{
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{5, 6, 7, 8},
			},
			resultTypes: "A,B,C,D",

			action: func(p *PhysicalPlan) {
				p.AddProjection([]uint32{3, 1}, execinfrapb.Ordering{})
			},

			expPost: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{8, 6},
			},
			expResultTypes: "D,B",
		},

		// Projection after rendering.
		{
			post: execinfrapb.PostProcessSpec{
				RenderExprs: []execinfrapb.Expression{{Expr: "@5"}, {Expr: "@1 + @2"}, {Expr: "@6"}},
			},
			resultTypes: "A,B,C",

			action: func(p *PhysicalPlan) {
				p.AddProjection([]uint32{2, 0}, execinfrapb.Ordering{})
			},

			expPost: execinfrapb.PostProcessSpec{
				RenderExprs: []execinfrapb.Expression{{Expr: "@6"}, {Expr: "@5"}},
			},
			expResultTypes: "C,A",
		},

		// Identity rendering.
		{
			post:        execinfrapb.PostProcessSpec{},
			resultTypes: "A,B,C,D",

			action: func(p *PhysicalPlan) {
				if err := p.AddRendering(
					[]tree.TypedExpr{
						&tree.IndexedVar{Idx: 10},
						&tree.IndexedVar{Idx: 11},
						&tree.IndexedVar{Idx: 12},
						&tree.IndexedVar{Idx: 13},
					},
					fakeExprContext{},
					[]int{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3},
					[]*types.T{strToType("A"), strToType("B"), strToType("C"), strToType("D")},
					execinfrapb.Ordering{},
				); err != nil {
					t.Fatal(err)
				}
			},

			expPost:        execinfrapb.PostProcessSpec{},
			expResultTypes: "A,B,C,D",
		},

		// Rendering that becomes projection.
		{
			post:        execinfrapb.PostProcessSpec{},
			resultTypes: "A,B,C,D",

			action: func(p *PhysicalPlan) {
				if err := p.AddRendering(
					[]tree.TypedExpr{
						&tree.IndexedVar{Idx: 11},
						&tree.IndexedVar{Idx: 13},
						&tree.IndexedVar{Idx: 12},
					},
					fakeExprContext{},
					[]int{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3},
					[]*types.T{strToType("B"), strToType("D"), strToType("C")},
					execinfrapb.Ordering{},
				); err != nil {
					t.Fatal(err)
				}

			},

			expPost: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{1, 3, 2},
			},
			expResultTypes: "B,D,C",
		},
	}

	for testIdx, tc := range testCases {
		p := PhysicalPlan{
			PhysicalInfrastructure: &PhysicalInfrastructure{
				Processors: []Processor{
					{Spec: execinfrapb.ProcessorSpec{Post: tc.post}},
					{Spec: execinfrapb.ProcessorSpec{Post: tc.post}},
				},
			},
			ResultRouters: []ProcessorIdx{0, 1},
			Distribution:  LocalPlan,
		}

		var resultTypes []*types.T
		for _, s := range strings.Split(tc.resultTypes, ",") {
			resultTypes = append(resultTypes, strToType(s))
		}
		for i := range p.Processors {
			p.Processors[i].Spec.ResultTypes = resultTypes
		}

		tc.action(&p)

		post := p.GetLastStagePost()
		// The actual planning always sets unserialized LocalExpr field on the
		// expressions, however, we don't do that for the expected results. In
		// order to be able to use the deep comparison below we manually unset
		// that unserialized field.
		for i := range post.RenderExprs {
			post.RenderExprs[i].LocalExpr = nil
		}
		if !reflect.DeepEqual(post, tc.expPost) {
			t.Errorf("%d: incorrect post:\n%s\nexpected:\n%s", testIdx, &post, &tc.expPost)
		}
		var resTypes []string
		for _, t := range p.GetResultTypes() {
			resTypes = append(resTypes, t.Locale())
		}
		if r := strings.Join(resTypes, ","); r != tc.expResultTypes {
			t.Errorf("%d: incorrect result types: %s expected %s", testIdx, r, tc.expResultTypes)
		}
	}
}
