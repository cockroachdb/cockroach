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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

func mergeJoinCanProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
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
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	switch childIdx {
	case 0:
		return parent.(*memo.MergeJoinExpr).LeftOrdering
	case 1:
		return parent.(*memo.MergeJoinExpr).RightOrdering
	default:
		return props.OrderingChoice{}
	}
}

func mergeJoinBuildProvided(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
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
