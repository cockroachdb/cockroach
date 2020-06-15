// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package partialidx

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
)

// FiltersImplyPredicate attempts to prove that a partial index predicate is
// implied by the given filters. If implication is proven, the function returns
// the remaining filters that don't match the predicate expression and true. If
// implication cannot be proven, nil and false are returned. Note that this
// "proof" of implication is not mathematically formal or rigorous. For the sake
// of efficiency and reduced complexity this proof is a best-effort attempt and
// false-negatives are possible.
//
// The remaining filters are identical to the input filters except that any
// parts that exactly match the predicate expression are removed. When the
// remaining filters are applied on top of a scan of a partial index with the
// given predicate, the resulting expression is equivalent to the original
// expression. This reduces the complexity of the filters and allows any columns
// that are referenced only in the filters to be pruned from the query plan.
//
// If implication is proven, the input filters cannot always be reduced. As an
// example, the filter "a > 10" implies the predicate "a > 5", but the filter
// must remain in order to further filter out values between 6 and 10 that would
// be returned from a partial index scan.
//
// The logic for testing implication of ScalarExprs with conjunctions,
// disjunctions, and atoms (anything that is not an AND or OR) is the same as
// Postgres's predtest library.
//
// The logic is as follows, where "=>" means "implies":
//
//   atom A => atom B iff:          A contains B
//   atom A => AND-expr B iff:      A => each of B's children
//   atom A => OR-expr B iff:       A => any of B's children
//
//   AND-expr A => atom B iff:      any of A's children => B
//   AND-expr A => AND-expr B iff:  A => each of B's children
//   AND-expr A => OR-expr B iff:   A => any of B's children OR
//                                    any of A's children => B
//
//   OR-expr A => atom B iff:       each of A's children => B
//   OR-expr A => AND-expr B iff:   A => each of B's children
//   OR-expr A => OR-expr B iff:    each of A's children => any of B's children
//
func FiltersImplyPredicate(
	filters memo.FiltersExpr, pred opt.ScalarExpr, f *norm.Factory,
) (remainingFilters memo.FiltersExpr, ok bool) {

	// First, check for exact matches at the root FiltersExpr. This check is not
	// necessary for correctness because the recursive approach below handles
	// all cases. However, this is a faster path for common cases where
	// expressions in filters are exact matches to the entire predicate.
	for i := range filters {
		c := filters[i].Condition

		// If the FiltersItem's condition is an exact match to the predicate,
		// remove the FilterItem from the remaining filters and return true.
		if c == pred {
			return filters.RemoveFiltersItem(&filters[i]), true
		}

		// If the FiltersItem's condition is a RangeExpr, unbox it and check for
		// an exact match. RangeExprs are only created in the
		// ConsolidateSelectFilters normalization rule and only exist as direct
		// children of a FiltersItem. The predicate will not contain a
		// RangeExpr, but the predicate may be an exact match to a RangeExpr's
		// child.
		if r, ok := c.(*memo.RangeExpr); ok {
			if r.And == pred {
				return filters.RemoveFiltersItem(&filters[i]), true
			}
		}
	}

	// If no exact match was found, recursively check the sub-expressions of the
	// filters and predicate. Use exactMatches to keep track of expressions in
	// filters that exactly matches expressions in pred, so that the can be
	// removed from the remaining filters.
	exactMatches := make(map[opt.Expr]struct{})
	if scalarExprImpliesPredicate(&filters, pred, exactMatches) {
		remainingFilters = simplifyFiltersExpr(filters, exactMatches, f)
		return remainingFilters, true
	}

	return nil, false
}

// exprImpliesPredicate returns true if the expression e implies the ScalarExpr
// pred. If e or any of its encountered sub-expressions are exact matches to
// expressions within pred, they are added to the exactMatches set.
//
// Note that exprImpliesPredicate short-circuits when e is proven to imply pred,
// and is not guaranteed to traverse either expression tree entirely. Therefore,
// there may be expressions in both trees that match exactly but are not added
// to exactMatches.
func scalarExprImpliesPredicate(
	e opt.ScalarExpr, pred opt.ScalarExpr, exactMatches map[opt.Expr]struct{},
) bool {
	// If the expressions are an exact match, then e implies pred.
	if e == pred {
		exactMatches[e] = struct{}{}
		return true
	}

	switch t := e.(type) {
	case *memo.FiltersExpr:
		return filtersExprImpliesPredicate(t, pred, exactMatches)

	case *memo.RangeExpr:
		and := t.And.(*memo.AndExpr)
		return andExprImpliesPredicate(and, pred, exactMatches)

	case *memo.AndExpr:
		return andExprImpliesPredicate(t, pred, exactMatches)

	case *memo.OrExpr:
		// TODO(mgartner): Handle OR filters.
		return false

	default:
		return atomImpliesPredicate(e, pred, exactMatches)
	}
}

// filtersExprImpliesPredicate returns true if the FiltersExpr e implies the
// ScalarExpr pred.
func filtersExprImpliesPredicate(
	e *memo.FiltersExpr, pred opt.ScalarExpr, exactMatches map[opt.Expr]struct{},
) bool {
	switch pt := pred.(type) {
	case *memo.AndExpr:
		// AND-expr A => AND-expr B iff A => each of B's children.
		leftPredImplied := filtersExprImpliesPredicate(e, pt.Left, exactMatches)
		if leftPredImplied {
			return filtersExprImpliesPredicate(e, pt.Right, exactMatches)
		}
		return false

	case *memo.OrExpr:
		// AND-expr A => OR-expr B iff A => any of B's children OR
		// any of A's children => B.
		// TODO(mgartner): Handle OR predicates.
		return false

	default:
		// AND-pred A => atom B iff any of A's children => B.
		for i := range *e {
			if scalarExprImpliesPredicate((*e)[i].Condition, pred, exactMatches) {
				return true
			}
		}
		return false
	}
}

// andExprImpliesPredicate returns true if the AndExpr e implies the ScalarExpr
// pred. This function transforms e into an equivalent FiltersExpr that is
// passed to filtersExprImpliesPredicate to prevent duplicating logic for both
// types of conjunctions.
func andExprImpliesPredicate(
	e *memo.AndExpr, pred opt.ScalarExpr, exactMatches map[opt.Expr]struct{},
) bool {
	f := make(memo.FiltersExpr, 2)
	f[0] = memo.FiltersItem{Condition: e.Left}
	f[1] = memo.FiltersItem{Condition: e.Right}
	return filtersExprImpliesPredicate(&f, pred, exactMatches)
}

// atomImpliedPredicate returns true if the atom expression e implies the
// ScalarExpr pred. The atom e cannot be an AndExpr, OrExpr, RangeExpr, or
// FiltersExpr.
func atomImpliesPredicate(
	e opt.ScalarExpr, pred opt.ScalarExpr, exactMatches map[opt.Expr]struct{},
) bool {
	switch pt := pred.(type) {
	case *memo.AndExpr:
		// atom A => AND-expr B iff A => each of B's children.
		leftPredImplied := atomImpliesPredicate(e, pt.Left, exactMatches)
		rightPredImplied := atomImpliesPredicate(e, pt.Right, exactMatches)
		return leftPredImplied && rightPredImplied

	case *memo.OrExpr:
		// atom A => OR-expr B iff A => any of B's children.
		// TODO(mgartner): Handle OR predicates.
		return false

	default:
		// atom A => atom B iff A contains B.
		// TODO(mgartner): Handle inexact atom matches.
		return false
	}
}

// simplifyFiltersExpr returns a new FiltersExpr with any expressions in e that
// exist in exactMatches removed.
//
// If a FiltersItem at the root exists in exactMatches, the entire FiltersItem
// is omitted from the returned FiltersItem. If not, the FiltersItem is
// recursively searched. See simplifyScalarExpr for more details.
func simplifyFiltersExpr(
	e memo.FiltersExpr, exactMatches map[opt.Expr]struct{}, f *norm.Factory,
) memo.FiltersExpr {
	filters := make(memo.FiltersExpr, 0, len(e))

	for i := range e {
		// If an entire FiltersItem exists in exactMatches, don't add it to the
		// output filters.
		if _, ok := exactMatches[e[i].Condition]; ok {
			continue
		}

		// Otherwise, attempt to recursively simplify the FilterItem's Condition
		// and append the result to the filters.
		s := simplifyScalarExpr(e[i].Condition, exactMatches, f)

		// If the scalar expression was reduced to True, don't add it to the
		// filters.
		if s != memo.TrueSingleton {
			filters = append(filters, f.ConstructFiltersItem(s))
		}
	}

	return filters
}

// simplifyScalarExpr returns a new ScalarExpr with any expressions in e that
// exist in exactMatches removed.
func simplifyScalarExpr(
	e opt.ScalarExpr, exactMatches map[opt.Expr]struct{}, f *norm.Factory,
) opt.ScalarExpr {

	switch t := e.(type) {
	case *memo.RangeExpr:
		and := simplifyScalarExpr(t.And, exactMatches, f)
		return f.ConstructRange(and)

	case *memo.AndExpr:
		_, leftIsExactMatch := exactMatches[t.Left]
		_, rightIsExactMatch := exactMatches[t.Right]
		if leftIsExactMatch && rightIsExactMatch {
			return memo.TrueSingleton
		}
		if leftIsExactMatch {
			return simplifyScalarExpr(t.Right, exactMatches, f)
		}
		if rightIsExactMatch {
			return simplifyScalarExpr(t.Left, exactMatches, f)
		}
		left := simplifyScalarExpr(t.Left, exactMatches, f)
		right := simplifyScalarExpr(t.Right, exactMatches, f)
		return f.ConstructAnd(left, right)

	default:
		return e
	}
}
