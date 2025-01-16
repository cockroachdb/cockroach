// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pheromone

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

func VisibleToPheromone(expr memo.RelExpr) bool {
	switch expr.(type) {
	case *memo.NormCycleTestRelExpr:
		return false
	case *memo.MemoCycleTestRelExpr:
		return false
	case *memo.ProjectExpr:
		return false
	case *memo.BarrierExpr:
		return false
	case *memo.DistributeExpr:
		return false
	case *memo.ExplainExpr:
		return false
	default:
		return true
	}
}

func BuildChildRequired(
	parent memo.RelExpr, required *physical.Pheromone, childIdx int,
) *physical.Pheromone {
	if required.Any() {
		return nil
	}

	if !VisibleToPheromone(parent) {
		return required
	}

	return required.Child(childIdx)
}
