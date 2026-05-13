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
	parent memo.RelExpr, required physical.PlanGram, childIdx int, mem *memo.Memo,
) physical.PlanGram {
	if !VisibleToPlanGram(parent) {
		return required
	}
	var md *opt.Metadata
	if mem != nil {
		md = mem.Metadata()
	}
	if !required.Matches(parent, md) {
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

func init() {
	physical.MatchFieldsFunc = matchFields
}

// matchFields checks whether the fields of a PlanGram expression match the
// given optimizer expression. It extracts table names from the expression's
// private and compares them against the field values.
func matchFields(fields []physical.PlanGramExprField, e opt.Expr, md *opt.Metadata) bool {
	if md == nil {
		return false
	}
	for _, f := range fields {
		switch f.Key {
		case "Table":
			tableID, ok := tableIDFromExpr(e)
			if !ok {
				return false
			}
			if string(md.Table(tableID).Name()) != f.Val {
				return false
			}
		case "LeftTable":
			zj, ok := e.(*memo.ZigzagJoinExpr)
			if !ok {
				return false
			}
			if string(md.Table(zj.LeftTable).Name()) != f.Val {
				return false
			}
		case "RightTable":
			zj, ok := e.(*memo.ZigzagJoinExpr)
			if !ok {
				return false
			}
			if string(md.Table(zj.RightTable).Name()) != f.Val {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// tableIDFromExpr extracts the TableID from expressions that have a Table
// field in their private.
func tableIDFromExpr(e opt.Expr) (opt.TableID, bool) {
	switch t := e.(type) {
	case *memo.ScanExpr:
		return t.Table, true
	case *memo.IndexJoinExpr:
		return t.Table, true
	case *memo.LookupJoinExpr:
		return t.Table, true
	case *memo.InvertedJoinExpr:
		return t.Table, true
	case *memo.VectorSearchExpr:
		return t.Table, true
	case *memo.VectorMutationSearchExpr:
		return t.Table, true
	default:
		return 0, false
	}
}
