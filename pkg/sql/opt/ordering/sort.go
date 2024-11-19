// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ordering

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

func sortBuildProvided(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	provided := required.ToOrdering()
	// The required ordering might not have been simplified (if normalization
	// rules are off) so we may need to trim.
	return trimProvided(provided, required, &expr.Relational().FuncDeps)
}

func sortBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	return parent.(*memo.SortExpr).InputOrdering
}
