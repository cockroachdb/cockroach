// Copyright 2022 The Cockroach Authors.
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
