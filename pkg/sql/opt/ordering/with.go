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
