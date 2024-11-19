// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ordering

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

func distributeCanProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	// Distribute operator can always pass through ordering to its input.
	return true
}

func distributeBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	// We can pass through any required ordering to the input.
	return *required
}

func distributeBuildProvided(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	d := expr.(*memo.DistributeExpr)
	return d.Input.ProvidedPhysical().Ordering
}
