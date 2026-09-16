// Copyright 2026 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func TestSetOpOrdering144925(t *testing.T) {
	intersectOps := []opt.Operator{opt.IntersectOp, opt.IntersectAllOp}
	filterOps := []opt.Operator{opt.IntersectOp, opt.IntersectAllOp, opt.ExceptOp, opt.ExceptAllOp}
	allOps := []opt.Operator{
		opt.IntersectOp, opt.IntersectAllOp, opt.ExceptOp, opt.ExceptAllOp,
		opt.UnionOp, opt.UnionAllOp,
	}
	testCases := []struct {
		name      string
		ops       []opt.Operator
		required  string
		internal  string
		leftFD    string
		rightFD   string
		streaming string
		children  [2]string
	}{
		{
			name: "left-equivalence", ops: filterOps,
			required: "+(7|8)", leftFD: "equivalence",
			streaming: "+7,+8,+9", children: [2]string{"+(1|3),+2", "+6,+4,+5"},
		},
		{
			name: "right-equivalence", ops: intersectOps,
			required: "+(7|8)", rightFD: "equivalence",
			streaming: "+7,+8,+9", children: [2]string{"+3,+1,+2", "+(4|6),+5"},
		},
		{
			name: "left-constant", ops: filterOps,
			required: "+8 opt(7)", leftFD: "constant",
			streaming: "+8,+7,+9", children: [2]string{"+1,+2 opt(3)", "+4,+6,+5"},
		},
		{
			name: "right-constant", ops: intersectOps,
			required: "+8 opt(7)", rightFD: "constant",
			streaming: "+8,+7,+9", children: [2]string{"+1,+3,+2", "+4,+5 opt(6)"},
		},
		{
			name: "internal-equivalence", ops: filterOps,
			internal: "+(7|8),+9", leftFD: "equivalence",
			streaming: "+7,+9,+8", children: [2]string{"+(1|3),+2", "+6,+5,+4"},
		},
		{
			name: "internal-constant", ops: filterOps,
			internal: "+8,+7,+9", leftFD: "constant",
			streaming: "+8,+7,+9", children: [2]string{"+1,+2 opt(3)", "+4,+6,+5"},
		},
		{
			name: "required-and-internal", ops: filterOps,
			required: "+(7|8)", internal: "+(7|8),-9", leftFD: "equivalence",
			streaming: "+7,-9,+8", children: [2]string{"+(1|3),-2", "+6,-5,+4"},
		},
		{
			name: "hash", ops: allOps,
			streaming: "", children: [2]string{"", ""},
		},
		{
			name: "descending-prefix", ops: allOps[:len(allOps)-1],
			required:  "-8",
			streaming: "-8,+7,+9", children: [2]string{"-1,+3,+2", "-4,+6,+5"},
		},
		{
			name: "union-shared-equivalence", ops: []opt.Operator{opt.UnionOp},
			required: "+(7|8)", leftFD: "equivalence", rightFD: "equivalence",
			streaming: "+7,+8,+9", children: [2]string{"+(1|3),+2", "+(4|6),+5"},
		},
		{
			name: "union-all-prefix", ops: []opt.Operator{opt.UnionAllOp},
			required:  "-8",
			streaming: "-8", children: [2]string{"-1", "-4"},
		},
		{
			name: "union-all-shared-equivalence", ops: []opt.Operator{opt.UnionAllOp},
			required: "+(7|8)", leftFD: "equivalence", rightFD: "equivalence",
			streaming: "+7", children: [2]string{"+(1|3)", "+(4|6)"},
		},
	}
	for _, tc := range testCases {
		for _, op := range tc.ops {
			t.Run(fmt.Sprintf("%s/%s", tc.name, op), func(t *testing.T) {
				st := cluster.MakeTestingClusterSettings()
				evalCtx := eval.NewTestingEvalContext(st)
				var f norm.Factory
				f.Init(context.Background(), evalCtx, testcat.New())
				for i := 1; i <= 9; i++ {
					f.Metadata().AddColumn(fmt.Sprintf("c%d", i), types.Int)
				}

				// Output 7,8,9 maps to left 3,1,2 and right 6,4,5. Neither
				// column IDs nor the lists' positions determine the merge order.
				private := memo.SetPrivate{
					OutCols: opt.ColList{8, 7, 9}, LeftCols: opt.ColList{1, 3, 2},
					RightCols: opt.ColList{4, 6, 5}, Ordering: props.ParseOrderingChoice(tc.internal),
				}
				makeInput := func(cols opt.ColList, fd string) *testexpr.Instance {
					input := &testexpr.Instance{Rel: &props.Relational{OutputCols: cols.ToSet()}}
					switch fd {
					case "equivalence":
						input.Rel.FuncDeps.AddEquivalency(cols[0], cols[1])
					case "constant":
						input.Rel.FuncDeps.AddConstants(opt.MakeColSet(cols[1]))
					}
					return input
				}
				left := makeInput(private.LeftCols, tc.leftFD)
				right := makeInput(private.RightCols, tc.rightFD)
				constructors := map[opt.Operator]func(memo.RelExpr, memo.RelExpr, *memo.SetPrivate) memo.RelExpr{
					opt.IntersectOp:    f.Memo().MemoizeIntersect,
					opt.IntersectAllOp: f.Memo().MemoizeIntersectAll,
					opt.ExceptOp:       f.Memo().MemoizeExcept,
					opt.ExceptAllOp:    f.Memo().MemoizeExceptAll,
					opt.UnionOp:        f.Memo().MemoizeUnion,
					opt.UnionAllOp:     f.Memo().MemoizeUnionAll,
				}
				expr := constructors[op](left, right, &private)
				required := props.ParseOrderingChoice(tc.required)
				requiredBefore := required.String()
				privateBefore := fmt.Sprintf("%+v", expr.Private())

				streaming := StreamingSetOpOrdering(expr, &required)
				if got := streaming.String(); got != tc.streaming {
					t.Errorf("streaming ordering: expected %q, got %q", tc.streaming, got)
				}
				for childIdx, cols := range []opt.ColList{private.LeftCols, private.RightCols} {
					childReq := setOpBuildChildReqOrdering(expr, &required, childIdx)
					if got := childReq.String(); got != tc.children[childIdx] {
						t.Errorf("child %d: expected %q, got %q", childIdx, tc.children[childIdx], got)
					}

					// Each child's requirement must guarantee the executor's same
					// concrete merge order. Only that child's FDs can justify
					// omitting a column or choosing an equivalent column instead.
					var mergeReq props.OrderingChoice
					mergeReq.FromOrdering(streaming)
					mergeReq = mergeReq.RemapColumns(private.OutCols, cols)
					mergeReq.Simplify(&expr.Child(childIdx).(memo.RelExpr).Relational().FuncDeps)
					if !childReq.Implies(&mergeReq) {
						t.Errorf("child %d: %s does not guarantee merge ordering %s", childIdx, childReq, mergeReq)
					}
				}
				if required.String() != requiredBefore || fmt.Sprintf("%+v", expr.Private()) != privateBefore {
					t.Fatal("building a set ordering mutated the required ordering or set private")
				}
			})
		}
	}
}
