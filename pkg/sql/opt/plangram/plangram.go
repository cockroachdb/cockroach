// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plangram

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

// VisibleToPlanGram returns false if expr is invisible to PlanGram
// matching. Invisible expressions (e.g. Distribute, Barrier, Explain, etc) are
// ignored during PlanGram matching.
func VisibleToPlanGram(expr memo.RelExpr) bool {
	switch expr.(type) {
	// Invisible expressions must be unary so that the required PlanGram term can
	// be passed down to the child group.
	case *memo.NormCycleTestRelExpr, *memo.MemoCycleTestRelExpr, *memo.BarrierExpr,
		*memo.DistributeExpr, *memo.ExplainExpr:
		return false
	default:
		return true
	}
}

// BuildChildRequired returns the PlanGram term for the nth child of
// the parent expression.
func BuildChildRequired(
	parent memo.RelExpr, required physical.PlanGram, childIdx int,
) physical.PlanGram {
	if !VisibleToPlanGram(parent) {
		// For expressions not visible to PlanGrams, the current term is simply
		// passed down.
		return required
	}
	if !required.Matches(parent) {
		// Once we hit a mismatch, NonePlanGram is passed downward to reduce the
		// number of optimization calls for lower groups.
		return physical.NonePlanGram
	}
	return required.Child(childIdx)
}

// CanProvide returns true if the expr can be costed using the current PlanGram
// term. Except for expressions invisible to PlanGrams, the coster can only cost
// expressions using PlanGram terms that don't have alternates (i.e. PlanGram
// expressions).
func CanProvide(expr memo.RelExpr, required physical.PlanGram) bool {
	if !VisibleToPlanGram(expr) {
		return true
	}
	return !required.HasAlternates()
}
