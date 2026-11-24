// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plangram

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

// BuildChildRequired returns the PlanGram term for the nth child of
// the parent expression.
func BuildChildRequired(
	parent memo.RelExpr, required physical.PlanGram, childIdx int, md *opt.Metadata,
) physical.PlanGram {
	if !physical.VisibleToPlanGram(parent.Op()) {
		// For expressions not visible to PlanGrams, the current term is simply
		// passed down.
		return required
	}
	if !required.Matches(parent, md) {
		// Once we hit a mismatch, NonePlanGram is passed downward to reduce the
		// number of optimization calls for lower groups.
		return physical.NonePlanGram
	}
	return required.Child(childIdx)
}

// CanProvide is counter-intuitive for PlanGrams: it does not return whether the
// current optimizer expression matches the current PlanGram term, as one would
// expect, but instead returns whether the expression *can be costed at all*
// using the term. Then costing will check whether the expression matches the
// term.
//
// The reason for this nuance is that the coster can only compare expressions
// against PlanGram terms that don't have alternates (i.e. terms that are
// concrete expressions). PlanGram terms that are productions with multiple
// rules must be expanded first into each of their alternate terms by
// enforceProps.
func CanProvide(_ memo.RelExpr, required physical.PlanGram) bool {
	return !required.HasAlternates()
}
