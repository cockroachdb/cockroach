// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

// CanInlineWith returns whether or not it's valid to inline binding in expr.
// This is the case when materialize is explicitly set to false, or when:
// 1. binding has no volatile expressions (because once it's inlined, there's no
//    guarantee it will be executed fully), and
// 2. binding is referenced at most once in expr.
func (c *CustomFuncs) CanInlineWith(binding, expr memo.RelExpr, private *memo.WithPrivate) bool {
	// If materialization is set, ignore the checks below.
	if private.Mtr.Set {
		return !private.Mtr.Materialize
	}
	if binding.Relational().VolatilitySet.HasVolatile() {
		return false
	}
	return c.WithUses(expr)[private.ID].Count <= 1
}

// InlineWith replaces all references to the With expression in input (via
// WithScans) with its definition.
func (c *CustomFuncs) InlineWith(binding, input memo.RelExpr, priv *memo.WithPrivate) memo.RelExpr {
	var replace ReplaceFunc
	replace = func(nd opt.Expr) opt.Expr {
		switch t := nd.(type) {
		case *memo.WithScanExpr:
			if t.With == priv.ID {
				// TODO(justin): it might be worth carefully walking the tree and
				// renaming variables as we do this replacement so that this projection
				// is unnecessary (assuming there's at most one reference to the
				// WithScan, which might be false if we heuristically inline multiple
				// times in the future).
				projections := make(memo.ProjectionsExpr, len(t.InCols))
				for i := range t.InCols {
					projections[i] = c.f.ConstructProjectionsItem(
						c.f.ConstructVariable(t.InCols[i]),
						t.OutCols[i],
					)
				}
				return c.f.ConstructProject(binding, projections, opt.ColSet{})
			}
			// TODO(justin): should apply joins block inlining because they can lead
			// to expressions being executed multiple times?
		}
		return c.f.Replace(nd, replace)
	}

	return replace(input).(memo.RelExpr)
}

// WithUses returns the WithUsesMap for the given expression.
func (c *CustomFuncs) WithUses(r opt.Expr) props.WithUsesMap {
	switch e := r.(type) {
	case memo.RelExpr:
		relProps := e.Relational()

		// Lazily calculate and store the WithUses value.
		if !relProps.IsAvailable(props.WithUses) {
			relProps.Shared.Rule.WithUses = c.deriveWithUses(r)
			relProps.SetAvailable(props.WithUses)
		}
		return relProps.Shared.Rule.WithUses

	case memo.ScalarPropsExpr:
		scalarProps := e.ScalarProps()

		// Lazily calculate and store the WithUses value.
		if !scalarProps.IsAvailable(props.WithUses) {
			scalarProps.Shared.Rule.WithUses = c.deriveWithUses(r)
			scalarProps.SetAvailable(props.WithUses)
		}
		return scalarProps.Shared.Rule.WithUses

	default:
		return c.deriveWithUses(r)
	}
}

// deriveWithUses collects information about WithScans in the expression.
func (c *CustomFuncs) deriveWithUses(r opt.Expr) props.WithUsesMap {
	// We don't allow the information to escape the scope of the WITH itself, so
	// we exclude that ID from the results.
	var excludedID opt.WithID

	switch e := r.(type) {
	case *memo.WithScanExpr:
		info := props.WithUseInfo{
			Count:    1,
			UsedCols: e.InCols.ToSet(),
		}
		return props.WithUsesMap{e.With: info}

	case *memo.WithExpr:
		excludedID = e.ID

	default:
		if opt.IsMutationOp(e) {
			// Note: this can still be 0.
			excludedID = e.Private().(*memo.MutationPrivate).WithID
		}
	}

	var result props.WithUsesMap
	for i, n := 0, r.ChildCount(); i < n; i++ {
		childUses := c.WithUses(r.Child(i))
		for id, info := range childUses {
			if id == excludedID {
				continue
			}
			if result == nil {
				result = make(props.WithUsesMap, len(childUses))
			}
			existing := result[id]
			existing.Count += info.Count
			existing.UsedCols.UnionWith(info.UsedCols)
			result[id] = existing
		}
	}
	return result
}
