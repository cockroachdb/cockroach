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

func withCanProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	// With operator can always pass through ordering to its main input.
	return true
}

func withBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	switch childIdx {
	case 0:
		return parent.(*memo.WithExpr).BindingOrdering

	case 1:
		// We can pass through any required ordering to the main query.
		return *required
	}
	return props.OrderingChoice{}
}

func withBuildProvided(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	w := expr.(*memo.WithExpr)
	return w.Main.ProvidedPhysical().Ordering
}
