// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
)

func GenericFiltersForIndex(
	index cat.Index, tm *opt.TableMeta, filters memo.FiltersExpr, optionalFilters memo.FiltersExpr,
) memo.FiltersExpr {
	var genericFilters memo.FiltersExpr
	placeholderCount := 0
	nonOptionalFilterCount := 0

	appendToGenericFilters := func(f memo.FiltersItem) {
		if genericFilters == nil {
			genericFilters = make(memo.FiltersExpr, 0, len(filters)+len(optionalFilters))
		}
		genericFilters = append(genericFilters, f)
	}

	// Find filters that are likely to constrain a prefix of the index columns.
EACH_COLUMN:
	for i, n := 0, index.KeyColumnCount(); i < n; i++ {
		ord := index.Column(i).Ordinal()
		col := tm.MetaID.ColumnID(ord)
		found := false

		// Search the filters first.
		for i := range filters {
			if ok, placeholder := exprConstrainsCol(filters[i].Condition, col); ok {
				nonOptionalFilterCount++
				if placeholder {
					placeholderCount++
				}
				appendToGenericFilters(filters[i])
				found = true
			}
		}

		if found {
			continue EACH_COLUMN
		}

		// Search the optional filters if the filters did not constrain the
		// column.
		for i := range optionalFilters {
			if ok, _ := exprConstrainsCol(filters[i].Condition, col); ok {
				appendToGenericFilters(filters[i])
				continue EACH_COLUMN
			}
		}

		// The column could not be constrained.
		break
	}

	// Return the collected filters if they contain non-optional filters and
	// placeholders.
	if len(genericFilters) > 0 && nonOptionalFilterCount > 0 && placeholderCount > 0 {
		return genericFilters
	}

	return nil
}

func exprConstrainsCol(e opt.ScalarExpr, col opt.ColumnID) (ok bool, placeholder bool) {
	switch t := e.(type) {
	case *memo.EqExpr, *memo.InExpr, *memo.LtExpr, *memo.LeExpr, *memo.GtExpr, *memo.GeExpr, *memo.LikeExpr:
		return equalityConstrainsCol(t, col)
	case *memo.AndExpr:
		if ok, placeholder = exprConstrainsCol(t.Left, col); ok {
			return ok, placeholder
		}
		return exprConstrainsCol(t.Right, col)
	case *memo.OrExpr:
		ok, leftPlaceholder := exprConstrainsCol(t.Left, col)
		if !ok {
			return false, false
		}
		ok, rightPlaceholder := exprConstrainsCol(t.Right, col)
		return ok, leftPlaceholder || rightPlaceholder
	default:
		return false, false
	}
}

func equalityConstrainsCol(e opt.ScalarExpr, col opt.ColumnID) (ok bool, placeholder bool) {
	v, ok := e.Child(0).(*memo.VariableExpr)
	if !ok || v.Col != col {
		return false, false
	}
	// TODO(mgartner): This probably needs to handle constant tuples and arrays.
	// And also tuples and arrays with a mix of constants and placeholders.
	if opt.IsConstValueOp(e.Child(1)) {
		return true, false
	}
	if _, ok := e.Child(1).(*memo.PlaceholderExpr); ok {
		return true, true
	}
	if tupleWithPlaceholderOrConst(e.Child(1)) {
		return true, true
	}
	return false, false
}

func tupleWithPlaceholderOrConst(e opt.Expr) bool {
	if t, ok := e.(*memo.TupleExpr); ok {
		for i := range t.Elems {
			if _, ok := t.Elems[i].(*memo.PlaceholderExpr); ok {
				return true
			}
			if _, ok := t.Elems[i].(*memo.ConstExpr); ok {
				return true
			}
		}
	}
	return false
}

// func extractDatum(e opt.Expr) tree.Datum {
// 	if opt.IsConstValueOp(e) {
// 		return memo.ExtractConstDatum(e)
// 	}
// 	if p, ok := e.(*memo.PlaceholderExpr); ok {
// 		return p.Value.(*tree.Placeholder)
// 	}
// 	panic(errors.AssertionFailedf("unexpected expression on RHS of '=': %T", e))
// }

// func BuildConstraintFromGenericFilters(
// 	index cat.Index,
// 	tm *opt.TableMeta,
// 	genFilters memo.FiltersExpr,
// 	tabID opt.TableID,
// 	tabMeta *opt.TableMeta,
// 	evalCtx *eval.Context,
// 	ctx context.Context,
// ) (_ *constraint.Constraint, err error) {
// 	var constraint constraint.Constraint
//
// EACH_COLUMN:
// 	for i, n := 0, index.KeyColumnCount(); i < n; i++ {
// 		ord := index.Column(i).Ordinal()
// 		col := tm.MetaID.ColumnID(ord)
// 		for i := range genFilters {
// 			if eq, ok := genFilters[i].Condition.(*memo.EqExpr); ok {
// 				v, ok := eq.Left.(*memo.VariableExpr)
// 				if !ok {
// 					continue
// 				}
// 				p, ok := eq.Right.(*memo.PlaceholderExpr)
// 				if !ok {
// 					continue
// 				}
// 				val, err := eval.Expr(ctx, evalCtx, p)
// 				if err != nil {
// 					return nil, err
// 				}
// 				eqSpan()
//
// 			}
// 			constraint.AddConstraint(tabID, tabMeta.MetaID.ColumnID(0), filters[i].Condition)
// 		}
// 		break EACH_COLUMN
// 	}
// 	constraint.
//
// 	return &constraint
// }

// eqSpan returns a span that constrains a column to a single value (which
// can be DNull).
// func eqSpan(offset int, value tree.Datum, out *constraint.Constraint) {
// 	var span constraint.Span
// 	key := constraint.MakeKey(value)
// 	span.Init(key, constraint.IncludeBoundary, key, constraint.IncludeBoundary)
// 	out.InitSingleSpan(&c.keyCtx[offset], &span)
// }
