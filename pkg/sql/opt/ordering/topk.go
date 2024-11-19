// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ordering

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

func topKCanProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	// TopK orders its own input, so the ordering it can provide is its own.
	topK := expr.(*memo.TopKExpr)
	return topK.Ordering.Implies(required)
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
