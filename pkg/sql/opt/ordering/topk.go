// Copyright 2021 The Cockroach Authors.
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

func topKCanProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	// TopK orders its own input, so the ordering it can provide is its own.
	topK := expr.(*memo.TopKExpr)
	return required.Intersects(&topK.Ordering)
}

func topKBuildProvided(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	// TopK orders its own input, so the ordering it provides is its own.
	return trimProvided(expr.(*memo.TopKExpr).Ordering.ToOrdering(), required, &expr.Relational().FuncDeps)
}

func topKBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	// If Top K has an input ordering to impose on its child for partial order
	// optimizations, then require the child to have that ordering.
	topK := parent.(*memo.TopKExpr)
	return topK.PartialOrdering
}

// TopKColOrdering returns an ordering on sort columns that is guaranteed on the
// input of a Top K operator.
func TopKColOrdering(topk *memo.TopKExpr, required *props.OrderingChoice) opt.Ordering {
	ordering := make(opt.Ordering, len(required.Columns))
	for i, rc := range required.Columns {
		cols := rc.Group.Intersection(topk.Ordering.ColSet())
		colID, ok := cols.Next(0)
		if !ok {
			// This refers to a column that is not in the top K ordering.
			// The rest of the ordering is not useful.
			return ordering[:i]
		}
		ordering[i] = opt.MakeOrderingColumn(colID, rc.Descending)
	}
	return ordering
}
