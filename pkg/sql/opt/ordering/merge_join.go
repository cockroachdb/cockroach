// Copyright 2018 The Cockroach Authors.
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

package ordering

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

func mergeJoinCanProvideOrdering(expr memo.RelExpr, required *physical.OrderingChoice) bool {
	m := expr.(*memo.MergeJoinExpr)
	// TODO(radu): in principle, we could pass through an ordering that covers
	// more than the equality columns. For example, if we have a merge join
	// with left ordering a+,b+ and right ordering x+,y+ we could guarantee
	// a+,b+,c+ if we pass that requirement through to the left side. However,
	// this requires a specific contract on the execution side on which side's
	// ordering is preserved when multiple rows match on the equality columns.
	switch m.JoinType {
	case opt.InnerJoinOp:
		return m.LeftOrdering.Implies(required) || m.RightOrdering.Implies(required)

	case opt.LeftJoinOp, opt.SemiJoinOp, opt.AntiJoinOp:
		return m.LeftOrdering.Implies(required)

	case opt.RightJoinOp:
		return m.RightOrdering.Implies(required)

	default:
		return false
	}
}

func mergeJoinBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
	switch childIdx {
	case 0:
		return parent.(*memo.MergeJoinExpr).LeftOrdering
	case 1:
		return parent.(*memo.MergeJoinExpr).RightOrdering
	default:
		return physical.OrderingChoice{}
	}
}

func mergeJoinBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	m := expr.(*memo.MergeJoinExpr)
	// This code parallels the one in mergeJoinCanProvideOrdering: the required
	// ordering has to be provided by one of the inputs (as allowed by the join
	// type).
	var provided opt.Ordering
	switch m.JoinType {
	case opt.InnerJoinOp:
		if m.LeftOrdering.Implies(required) {
			provided = m.Left.ProvidedPhysical().Ordering
		} else {
			provided = m.Right.ProvidedPhysical().Ordering
		}

	case opt.LeftJoinOp, opt.SemiJoinOp, opt.AntiJoinOp:
		provided = m.Left.ProvidedPhysical().Ordering

	case opt.RightJoinOp:
		provided = m.Right.ProvidedPhysical().Ordering
	}
	// The input's ordering satisfies both <required> and the ordering required by
	// the merge join itself; it may need to be trimmed.
	return trimProvided(provided, required, &expr.Relational().FuncDeps)
}
