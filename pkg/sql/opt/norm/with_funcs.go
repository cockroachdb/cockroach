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

// ApplyLimitToRecursiveCTEScan re-optimizes the recursive branch of a recursive
// CTE with a limit applied to the binding. This is possible when both the
// initial and recursive branches have a limit.
func (c *CustomFuncs) ApplyLimitToRecursiveCTEScan(
	binding, initial, recursive memo.RelExpr, private *memo.RecursiveCTEPrivate,
) memo.RelExpr {
	// The cardinality of each iteration is at least the minimum of the min
	// cardinality guaranteed by both branches, and at most the maximum of the max
	// cardinality guaranteed by both branches.
	newCard := initial.Relational().Cardinality.Union(recursive.Relational().Cardinality)
	if private.Deduplicate {
		// Rows may be filtered by the de-duplication process, so we can only
		// guarantee that each WithScan will return at least one row.
		newCard = newCard.AsLowAs(1)
	}

	// Use the initial branch's row count estimate for WithScan estimate.
	newRowCount := initial.Relational().Statistics().RowCount
	newBinding := c.f.ConstructFakeRel(&memo.FakeRelPrivate{
		Props: MakeBindingPropsForRecursiveCTE(newCard, binding.Relational().OutputCols, newRowCount),
	})

	// Re-optimize the recursive branch with the new properties.
	withID := private.WithID
	newWithID := c.f.Memo().NextWithID()
	c.f.Metadata().AddWithBinding(newWithID, newBinding)

	var replace ReplaceFunc
	replace = func(e opt.Expr) opt.Expr {
		if withScan, ok := e.(*memo.WithScanExpr); ok && withScan.With == withID {
			// Reconstruct the with scan using the new binding.
			newPrivate := withScan.WithScanPrivate
			newPrivate.With = newWithID
			newPrivate.ID = c.f.Metadata().NextUniqueID()
			return c.f.ConstructWithScan(&newPrivate)
		}
		return c.f.Replace(e, replace)
	}
	newRecursive := replace(recursive).(memo.RelExpr)
	newPrivate := *private
	newPrivate.WithID = newWithID
	return c.f.ConstructRecursiveCTE(newBinding, initial, newRecursive, &newPrivate)
}

// MakeBindingPropsForRecursiveCTE makes a Relational struct that applies to all
// iterations of a recursive CTE. The caller must verify that the supplied
// cardinality applies to all iterations.
func MakeBindingPropsForRecursiveCTE(
	card props.Cardinality, outCols opt.ColSet, rowCount float64,
) *props.Relational {
	// The working table will always have at least one row, because any iteration
	// that returns zero rows will end the loop.
	card = card.AtLeast(props.OneCardinality)
	bindingProps := &props.Relational{}
	bindingProps.OutputCols = outCols
	bindingProps.Cardinality = card
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

// CanAddRecursiveLimit traverses the given expression tree and checks whether a
// limit that applies to all WithScanExprs with the given ID also applies to the
// given expression. This is the case when the expression does not duplicate
// input rows or add new ones. For example, a limit on the input of a DistinctOn
// always applies to the DistinctOn, but the same is not true for a
// ScalarGroupBy because it returns one row when the input returns zero rows.
// This check is used to infer cardinality for recursive CTE expressions.
//
// dedupCols describes the set of columns which are de-duplicated by an ancestor
// of the given expression. This is useful for handling cases where a join
// duplicates input rows (thus invalidating a limit on input rows) and a later
// operator de-duplicates (making the limit valid once more).
func (c *CustomFuncs) CanAddRecursiveLimit(
	expr memo.RelExpr, withID opt.WithID, dedupCols opt.ColSet,
) bool {
	switch t := expr.(type) {
	case *memo.WithScanExpr:
		return t.With == withID
	case *memo.ProjectExpr, *memo.SelectExpr, *memo.LimitExpr,
		*memo.OrdinalityExpr, *memo.WindowExpr:
		// The operators never add rows to the input, although they can remove them.
		// Therefore, a limit that applies to the input also applies to the output
		// of these operators. In the case of a LimitExpr, we assume the limit is
		// larger than the candidate limit since if this wasn't the case, the limit
		// should have been propagated to the expression with which
		// CanAddRecursiveLimit was originally called.
		return c.CanAddRecursiveLimit(t.Child(0).(memo.RelExpr), withID, dedupCols)
	case *memo.DistinctOnExpr, *memo.GroupByExpr:
		// DistinctOn and GroupBy expressions de-duplicate the grouping columns.
		private := t.Private().(*memo.GroupingPrivate)
		dedupCols = private.GroupingCols
		return c.CanAddRecursiveLimit(t.Child(0).(memo.RelExpr), withID, dedupCols)
	case *memo.InnerJoinExpr:
		left, right := t.Child(0).(memo.RelExpr), t.Child(1).(memo.RelExpr)
		if c.JoinDoesNotDuplicateLeftRows(t) ||
			(!dedupCols.Empty() && dedupCols.SubsetOf(left.Relational().OutputCols)) {
			// Either this join will not de-duplicate left rows, or any duplication
			// will be reversed by an ancestor DistinctOn or GroupBy. It is sufficient
			// to check whether the de-duplicated columns is a subset of the left
			// output columns because grouping on a strict subset of columns always
			// implies grouping on the containing set. In other words, grouping on a
			// subset is equivalent to first grouping on the containing set, then
			// grouping on the subset.
			if c.CanAddRecursiveLimit(left, withID, dedupCols) {
				return true
			}
		}
		if c.JoinDoesNotDuplicateRightRows(t) ||
			(!dedupCols.Empty() && dedupCols.SubsetOf(right.Relational().OutputCols)) {
			// This join will not duplicate right rows, so a limit applying to the
			// right input applies to the output of the join.
			if c.CanAddRecursiveLimit(right, withID, dedupCols) {
				return true
			}
		}
	case *memo.LeftJoinExpr:
		// We can't propagate a limit through the right side of a LeftJoin because
		// the join will add unmatched left rows, meaning the cardinality will
		// always be at least that of the left input even if the right input has a
		// limit. FullJoins can't be considered at all here for the same reasons.
		left := t.Child(0).(memo.RelExpr)
		if c.JoinDoesNotDuplicateLeftRows(t) ||
			(!dedupCols.Empty() && dedupCols.SubsetOf(left.Relational().OutputCols)) {
			// This join will not duplicate left rows, so a limit applying to the left
			// input applies to the output of the join.
			if c.CanAddRecursiveLimit(left, withID, dedupCols) {
				return true
			}
		}
	case *memo.SemiJoinExpr, *memo.AntiJoinExpr:
		// SemiJoins and AntiJoins never duplicate left rows (and don't output right
		// rows).
		return c.CanAddRecursiveLimit(t.Child(0).(memo.RelExpr), withID, dedupCols)
	case *memo.WithExpr:
		return c.CanAddRecursiveLimit(t.Main, withID, dedupCols)
	}
	return false
}

// GetRecursiveWithID returns the WithID associated with the recursive CTE
// corresponding to the given private.
func (c *CustomFuncs) GetRecursiveWithID(private *memo.RecursiveCTEPrivate) opt.WithID {
	return private.WithID
}
