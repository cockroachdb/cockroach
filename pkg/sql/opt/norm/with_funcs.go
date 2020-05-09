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
// This is the case when:
// 1. binding has no side-effects (because once it's inlined, there's no
//    guarantee it will be executed fully), and
// 2. binding is referenced at most once in expr.
func (c *CustomFuncs) CanInlineWith(binding, expr memo.RelExpr, private *memo.WithPrivate) bool {
	if binding.Relational().CanHaveSideEffects {
		return false
	}
	return c.WithUses(expr)[private.ID] <= 1
}

// InlineWith replaces all references to the With expression in input (via
// WithScans) with its definition.
func (c *CustomFuncs) InlineWith(binding, input memo.RelExpr, priv *memo.WithPrivate) memo.RelExpr {
	var replace ReplaceFunc
	replace = func(nd opt.Expr) opt.Expr {
		switch t := nd.(type) {
		case *memo.WithScanExpr:
			if t.ID == priv.ID {
				// TODO(justin): it might be worth carefully walking the tree and
				// renaming variables as we do this replacement so that this projection
				// is unnecessary (assuming there's at most one reference to the
				// WithScan, which might be false if we heuristically inline multiple
				// times in the future).
				projections := make(memo.ProjectionsExpr, len(t.InCols))
				for i := range t.InCols {
					projections[i] = memo.ProjectionsItem{
						Element:    c.f.ConstructVariable(t.InCols[i]),
						ColPrivate: memo.ColPrivate{Col: t.OutCols[i]},
					}
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

// WithUses returns a map mapping WithIDs to the number of times a given With
// expression is referenced in the given expression.
func (c *CustomFuncs) WithUses(r opt.Expr) map[opt.WithID]int {
	switch e := r.(type) {
	case memo.RelExpr:
		relProps := e.Relational()
		if !relProps.IsAvailable(props.WithUses) {
			relProps.SetAvailable(props.WithUses)
			relProps.Shared.Rule.WithUses = c.deriveWithUses(r)
		}
		return relProps.Shared.Rule.WithUses

	case memo.ScalarPropsExpr:
		scalarProps := e.ScalarProps(c.mem)

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

// deriveWithUses computes the number of times each WithScan is referenced. It's
// used to decide if we can inline a With or not.
func (c *CustomFuncs) deriveWithUses(r opt.Expr) map[opt.WithID]int {
	var result map[opt.WithID]int
	switch e := r.(type) {
	case *memo.WithScanExpr:
		result = map[opt.WithID]int{e.ID: 1}
	default:
		for i, n := 0, r.ChildCount(); i < n; i++ {
			for id, useCount := range c.WithUses(r.Child(i)) {
				if result == nil {
					result = make(map[opt.WithID]int)
				}
				result[id] = result[id] + useCount
			}
		}
	}

	return result
}
