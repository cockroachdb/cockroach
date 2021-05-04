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
