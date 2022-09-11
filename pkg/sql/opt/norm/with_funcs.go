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
	"github.com/cockroachdb/errors"
)

// CanInlineWith returns whether or not it's valid to inline binding in expr.
// This is the case when materialize is explicitly set to false, or when:
//  1. binding has no volatile expressions (because once it's inlined, there's no
//     guarantee it will be executed fully), and
//  2. binding is referenced at most once in expr.
func (c *CustomFuncs) CanInlineWith(binding, expr memo.RelExpr, private *memo.WithPrivate) bool {
	// If materialization is set, ignore the checks below.
	if private.Mtr.Set {
		return !private.Mtr.Materialize
	}
	if binding.Relational().VolatilitySet.HasVolatile() {
		return false
	}
	return memo.WithUses(expr)[private.ID].Count <= 1
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

// CanInlineWithScan returns whether or not it's valid and heuristically cheaper
// to inline a WithScanExpr with its bound expression from the memo. Currently
// this only allows inlining leak-proof constant VALUES clauses of the form
// `column IN (VALUES(...))` or `column NOT IN(VALUES(...))`, but could likely
// be extended to handle other expressions in the future.
func (c *CustomFuncs) CanInlineWithScan(private *memo.WithScanPrivate, scalar opt.ScalarExpr) bool {
	if !private.CanInlineInPlace {
		return false
	}
	// If we don't have `column IN(...)` or `column NOT IN(...)` or
	// (col1, col2 ... coln) IN/NOT IN (...), it is not cheaper to inline because
	// we wouldn't be avoiding one or more joins.
	if tupleExpr, ok := scalar.(*memo.TupleExpr); ok {
		for _, scalarExpr := range tupleExpr.Elems {
			if scalarExpr.Op() != opt.VariableOp {
				return false
			}
		}
	} else if scalar.Op() != opt.VariableOp {
		return false
	}
	expr := c.mem.Metadata().WithBinding(private.With)
	var valuesExpr *memo.ValuesExpr
	var ok bool
	if valuesExpr, ok = expr.(*memo.ValuesExpr); !ok {
		return false
	}
	if !valuesExpr.IsConstantsAndPlaceholders() {
		return false
	}
	return true
}

// InlineWithScan replaces a WithScanExpr with its bound expression, mapped to
// new output ColumnIDs.
func (c *CustomFuncs) InlineWithScan(private *memo.WithScanPrivate) memo.RelExpr {
	expr := c.mem.Metadata().WithBinding(private.With)
	var valuesExpr *memo.ValuesExpr
	var ok bool
	valuesExpr.Op()
	if valuesExpr, ok = expr.(*memo.ValuesExpr); !ok {
		// Didn't find the expected VALUES.
		panic(errors.AssertionFailedf("attempt to inline a WithScan which is not a VALUES clause; operator: %s",
			expr.Op().String()))
	}
	projections := make(memo.ProjectionsExpr, len(private.InCols))
	for i := range private.InCols {
		projections[i] = c.f.ConstructProjectionsItem(
			c.f.ConstructVariable(private.InCols[i]),
			private.OutCols[i],
		)
	}
	// Shallow copy the values in case this WITH binding is inlined more than
	// once.
	newRows := make(memo.ScalarListExpr, len(valuesExpr.Rows))
	copy(newRows, valuesExpr.Rows)
	newCols := make(opt.ColList, len(valuesExpr.ValuesPrivate.Cols))
	copy(newCols, valuesExpr.ValuesPrivate.Cols)
	newValuesExpr := c.f.ConstructValues(newRows, &memo.ValuesPrivate{
		Cols: newCols,
		ID:   c.f.Metadata().NextUniqueID(),
	})
	return c.f.ConstructProject(newValuesExpr, projections, opt.ColSet{})
}
