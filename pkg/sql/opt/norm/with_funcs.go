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

// ApplyLimitToRecursiveCTEScan re-optimizes the recursive branch of a recursive
// CTE with a limit applied to the binding. This is possible when both the
// initial and recursive branches have a limit.
func (c *CustomFuncs) ApplyLimitToRecursiveCTEScan(
	binding, initial, recursive memo.RelExpr, private *memo.RecursiveCTEPrivate,
) memo.RelExpr {
	// The cardinality of each iteration is at least the minimum of the min
	// cardinality guaranteed by both branches, and at most the maximum of the max
	// cardinality guaranteed by both branches. Additionally, the working table
	// will always have at least one row.
	newCard := initial.Relational().Cardinality.Union(recursive.Relational().Cardinality)
	newCard = newCard.AtLeast(props.OneCardinality)
	newOutCols := binding.Relational().OutputCols.Copy()
	// Use the initial branch's row count estimate for WithScan estimate.
	newRowCount := initial.Relational().Statistics().RowCount
	newBinding := c.f.ConstructFakeRel(&memo.FakeRelPrivate{
		Props: MakeBindingPropsForRecursiveCTE(newCard, newOutCols, newRowCount),
	})

	// Re-optimize the recursive branch with the new properties.
	withID := private.WithID
	newWithID := c.f.Memo().NextWithID()
	c.f.Metadata().AddWithBinding(newWithID, newBinding)

	var replace ReplaceFunc
	replace = func(e opt.Expr) opt.Expr {
		if withScan, ok := e.(*memo.WithScanExpr); ok && withScan.With == withID {
			// Reconstruct the with scan using the new binding.
			return c.f.ConstructWithScan(c.duplicateWithScanPrivate(&withScan.WithScanPrivate, newWithID))
		}
		return c.f.Replace(e, replace)
	}
	newRecursive := replace(recursive).(memo.RelExpr)
	newPrivate := c.duplicateRecursiveCTEPrivate(private, newWithID)
	return c.f.ConstructRecursiveCTE(newBinding, initial, newRecursive, newPrivate)
}

// MakeBindingPropsForRecursiveCTE makes a Relational struct that applies to all
// iterations of a recursive CTE. The caller must verify that the supplied
// cardinality applies to all iterations.
func MakeBindingPropsForRecursiveCTE(
	card props.Cardinality, outCols opt.ColSet, rowCount float64,
) *props.Relational {
	bindingProps := &props.Relational{}
	bindingProps.OutputCols = outCols
	bindingProps.Cardinality = card.AtLeast(props.OneCardinality)
	bindingProps.Statistics().RowCount = rowCount
	// Row count must be greater than 0 or the stats code will throw an error.
	// Set it to 1 to match the cardinality.
	if bindingProps.Statistics().RowCount < 1 {
		bindingProps.Statistics().RowCount = 1
	}
	// We can infer a zero-column key in the case when there are zero or one rows.
	if bindingProps.Cardinality.IsZeroOrOne() {
		bindingProps.FuncDeps.AddStrictKey(opt.ColSet{}, outCols)
	}
	return bindingProps
}

func (c *CustomFuncs) duplicateWithScanPrivate(
	private *memo.WithScanPrivate, newID opt.WithID,
) *memo.WithScanPrivate {
	newPrivate := &memo.WithScanPrivate{
		With:    newID,
		Name:    private.Name,
		InCols:  make(opt.ColList, len(private.InCols)),
		OutCols: make(opt.ColList, len(private.OutCols)),
		ID:      c.f.Metadata().NextUniqueID(),
	}
	copy(newPrivate.InCols, private.InCols)
	copy(newPrivate.OutCols, private.OutCols)
	return newPrivate
}

func (c *CustomFuncs) duplicateRecursiveCTEPrivate(
	private *memo.RecursiveCTEPrivate, newID opt.WithID,
) *memo.RecursiveCTEPrivate {
	newPrivate := &memo.RecursiveCTEPrivate{
		Name:          private.Name,
		WithID:        newID,
		InitialCols:   make(opt.ColList, len(private.InitialCols)),
		RecursiveCols: make(opt.ColList, len(private.RecursiveCols)),
		OutCols:       make(opt.ColList, len(private.OutCols)),
		Deduplicate:   private.Deduplicate,
	}
	copy(newPrivate.InitialCols, private.InitialCols)
	copy(newPrivate.RecursiveCols, private.RecursiveCols)
	copy(newPrivate.OutCols, private.OutCols)
	return newPrivate
}
