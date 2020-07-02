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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// Implicator is used to 1) prove that query filters imply a partial index
// predicate expression and 2) reduce the original filters into a simplified set
// of filters that are equivalent to the original when applied on top of a
// partial index scan. The FiltersImplyPredicate function handles both of these
// tasks.
//
// I. Proving Implication
//
// Filters "imply" a predicate expression if truthful evaluation of the filters
// guarantees truthful evaluation of the predicate. As a simple example, the
// expression "a > 10" implies "a > 0" because all values that satisfy "a > 10"
// also satisfy "a > 0". Note that implication is not symmetrical; "a > 0" does
// not imply "a > 10".
//
// We use the same logic as Postgres's predtest library to prove implication.
// Note that this "proof" is not mathematically formal or rigorous. For the sake
// of efficiency and reduced complexity this proof is a best-effort attempt and
// false-negatives are possible.
//
// The logic is as follows, where "=>" means "implies" and an "atom" is any
// expression that is not a logical conjunction or disjunction.
//
//   atom A => atom B iff:          B contains A
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
// II. Remaining Filters
//
// The remaining filters that are returned upon a proof of implication are
// identical to the input filters except that unnecessary expressions are
// removed. When the remaining filters are applied on top of a scan of a partial
// index with the given predicate, the resulting expression is equivalent to the
// original expression.
//
// Removing unnecessary filter expressions reduces the complexity of the filters
// and allows any columns that are referenced only in the filters to be pruned
// from the query plan.
//
// We can safely remove an expression from the filters if all of the following
// are true:
//
//   1. The expression exactly matches an expression in the predicate. This
//   prevents returning empty remaining filters for the implication below. The
//   original filters must be applied on top of a partial index scan with the
//   a > 0 predicate to filter out rows where a is between 0 and 10.
//
//     a > 10
//     =>
//     a > 0
//
//   2. The expression does not reside within a disjunction in the predicate.
//   This prevents the function from returning empty remaining filters for the
//   implication below. The original filters must be applied on top of a partial
//   index scan with the predicate to filter out rows where a > 0 but
//   b != 'foo'.
//
//     b = 'foo'
//     =>
//     a > 0 OR b = 'foo'
//
//   3. The expression does not reside within a disjunction in the filters. This
//   prevents the function from incorrectly reducing the filters for the
//   implication below. The original filters must be applied in this case to
//   filter out rows where a is false and c is true, but b is false.
//
//     a OR (b AND c)
//     =>
//     a OR c
//
// An unfortunate side-effect of these three rules is that they prevent reducing
// the remaining filters in some cases in which it is theoretically possible to
// simplify the filters. For example, consider the implication below.
//
//   a OR b
//   =>
//   b OR a
//
// In this case, the remaining filters could be empty, but they are not, because
// of the asymmetry of the expressions. Individually a and b are exact matches
// in both the filters and the predicate, but rule #2 and rule #3 prevent this
// function from traversing the OR expressions and removing a and b from the
// remaining filters. It would be difficult to support this case without
// breaking the other cases prevented by each of the three rules.
//
// A set of opt.Expr keeps track of exact matches encountered while exploring
// the filters and predicate expressions. If implication is proven, the filters
// expression is traversed and the expressions in the opt.Expr set are removed.
// While proving implication this set is not passed to recursive calls when a
// disjunction is encountered in the predicate (rule #2), and disjunctions in
// the filters are never traversed when searching for exact matches to remove
// (rule #3).
type Implicator struct {
	f       *norm.Factory
	md      *opt.Metadata
	evalCtx *tree.EvalContext

	// constraintCache stores constraints built from atoms. Caching the
	// constraints prevents building constraints for the same atom multiple
	// times.
	constraintCache map[opt.ScalarExpr]constraintResult
}

// constraintResult contains the result of building the constraint for an atom.
// It includes both the constraint and the tight boolean.
type constraintResult struct {
	c     *constraint.Set
	tight bool
}

// Init initializes an Implicator with the given factory, metadata, and eval
// context.
func (im *Implicator) Init(f *norm.Factory, md *opt.Metadata, evalCtx *tree.EvalContext) {
	im.f = f
	im.md = md
	im.evalCtx = evalCtx
	im.constraintCache = make(map[opt.ScalarExpr]constraintResult)
}

// FiltersImplyPredicate attempts to prove that a partial index predicate is
// implied by the given filters. If implication is proven, the function returns
// the remaining filters (which when applied on top of a partial index scan,
// make a plan equivalent to the original) and true. If implication cannot be
// proven, nil and false are returned. See Implicator for more details on how
// implication is proven and how the remaining filters are determined.
func (im *Implicator) FiltersImplyPredicate(
	filters memo.FiltersExpr, pred opt.ScalarExpr,
) (remainingFilters memo.FiltersExpr, ok bool) {
	// Empty filters are equivalent to True, which only implies True.
	if len(filters) == 0 && pred == memo.TrueSingleton {
		return filters, true
	}

	// Next, check for exact matches at the root FiltersExpr. This check is not
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

	// Populate the constraint cache with any constraints already generated for
	// FiltersItems that are atoms.
	for _, f := range filters {
		op := f.Condition.Op()
		if f.ScalarProps().Constraints != nil && op != opt.AndOp && op != opt.OrOp && op != opt.RangeOp {
			im.cacheConstraint(f.Condition, f.ScalarProps().Constraints, f.ScalarProps().TightConstraints)
		}
	}

	// If no exact match was found, recursively check the sub-expressions of the
	// filters and predicate. Use exactMatches to keep track of expressions in
	// filters that exactly matches expressions in pred, so that the can be
	// removed from the remaining filters.
	exactMatches := make(map[opt.Expr]struct{})
	if im.scalarExprImpliesPredicate(&filters, pred, exactMatches) {
		remainingFilters = im.simplifyFiltersExpr(filters, exactMatches)
		return remainingFilters, true
	}

	return nil, false
}

// scalarExprImpliesPredicate returns true if the expression e implies the
// ScalarExpr pred. If e or any of its encountered sub-expressions are exact
// matches to expressions within pred, they are added to the exactMatches set
// (except any expressions under an OrExpr).
//
// Note that scalarExprImpliesPredicate short-circuits when e is proven to imply
// pred, and is not guaranteed to traverse either expression tree entirely.
// Therefore, there may be expressions in both trees that match exactly but are
// not added to exactMatches.
//
// Also note that exactMatches is optional, and nil can be passed when it is not
// necessary to keep track of exactly matching expressions.
func (im *Implicator) scalarExprImpliesPredicate(
	e opt.ScalarExpr, pred opt.ScalarExpr, exactMatches map[opt.Expr]struct{},
) bool {

	// If the expressions are an exact match, then e implies pred.
	if e == pred {
		if exactMatches != nil {
			exactMatches[e] = struct{}{}
		}
		return true
	}

	switch t := e.(type) {
	case *memo.FiltersExpr:
		return im.filtersExprImpliesPredicate(t, pred, exactMatches)

	case *memo.RangeExpr:
		and := t.And.(*memo.AndExpr)
		return im.andExprImpliesPredicate(and, pred, exactMatches)

	case *memo.AndExpr:
		return im.andExprImpliesPredicate(t, pred, exactMatches)

	case *memo.OrExpr:
		return im.orExprImpliesPredicate(t, pred)

	default:
		return im.atomImpliesPredicate(e, pred, exactMatches)
	}
}

// filtersExprImpliesPredicate returns true if the FiltersExpr e implies the
// ScalarExpr pred.
func (im *Implicator) filtersExprImpliesPredicate(
	e *memo.FiltersExpr, pred opt.ScalarExpr, exactMatches map[opt.Expr]struct{},
) bool {
	switch pt := pred.(type) {
	case *memo.AndExpr:
		// AND-expr A => AND-expr B iff A => each of B's children.
		return im.filtersExprImpliesPredicate(e, pt.Left, exactMatches) &&
			im.filtersExprImpliesPredicate(e, pt.Right, exactMatches)

	case *memo.OrExpr:
		// The logic for proving that an AND implies an OR is:
		//
		//   AND-expr A => OR-expr B iff A => any of B's children OR
		//   any of A's children => B
		//
		// Here we handle the first part, checking if A implies any of B's
		// children. The second part is logically equivalent AND => atom
		// implication, and is handled after the switch statement.
		//
		// Do not pass exactMatches to the recursive call with pt.Left or
		// pt.Right, because matching expressions below a disjunction in a
		// predicate cannot be removed from the remaining filters. See
		// FiltersImplyPredicate (rule #2) for more details.
		if im.filtersExprImpliesPredicate(e, pt.Left, nil /* exactMatches */) {
			return true
		}
		if im.filtersExprImpliesPredicate(e, pt.Right, nil /* exactMatches */) {
			return true
		}
	}

	// If the above cases have not proven or disproven implication, there are
	// two more cases to consider, both handled by the same code path:
	//
	//   AND-expr A => OR-expr B if any of A's children => B
	//   AND-pred A => atom B iff any of A's children => B
	for i := range *e {
		if im.scalarExprImpliesPredicate((*e)[i].Condition, pred, exactMatches) {
			return true
		}
	}

	return false
}

// andExprImpliesPredicate returns true if the AndExpr e implies the ScalarExpr
// pred. This function transforms e into an equivalent FiltersExpr that is
// passed to filtersExprImpliesPredicate to prevent duplicating logic for both
// types of conjunctions.
func (im *Implicator) andExprImpliesPredicate(
	e *memo.AndExpr, pred opt.ScalarExpr, exactMatches map[opt.Expr]struct{},
) bool {
	f := make(memo.FiltersExpr, 2)
	f[0] = memo.FiltersItem{Condition: e.Left}
	f[1] = memo.FiltersItem{Condition: e.Right}
	return im.filtersExprImpliesPredicate(&f, pred, exactMatches)
}

// orExprImpliesPredicate returns true if the FiltersExpr e implies the
// ScalarExpr pred.
//
// Note that in all recursive calls within this function, we do not pass
// exactMatches. See FiltersImplyPredicate (rule #3) for more details.
func (im *Implicator) orExprImpliesPredicate(e *memo.OrExpr, pred opt.ScalarExpr) bool {
	switch pt := pred.(type) {
	case *memo.AndExpr:
		// OR-expr A => AND-expr B iff A => each of B's children.
		return im.orExprImpliesPredicate(e, pt.Left) &&
			im.orExprImpliesPredicate(e, pt.Right)

	case *memo.OrExpr:
		// OR-expr A => OR-expr B iff each of A's children => any of B's
		// children.
		//
		// We must flatten all adjacent ORs in order to handle cases such as:
		//   (a OR b) => ((a OR b) OR c)
		eFlat := flattenOrExpr(e)
		predFlat := flattenOrExpr(pt)
		for i := range eFlat {
			eChildImpliesAnyPredChild := false
			for j := range predFlat {
				if im.scalarExprImpliesPredicate(eFlat[i], predFlat[j], nil /* exactMatches */) {
					eChildImpliesAnyPredChild = true
					break
				}
			}
			if !eChildImpliesAnyPredChild {
				return false
			}
		}
		return true

	default:
		// OR-expr A => atom B iff each of A's children => B.
		return im.scalarExprImpliesPredicate(e.Left, pred, nil /* exactMatches */) &&
			im.scalarExprImpliesPredicate(e.Right, pred, nil /* exactMatches */)
	}
}

// atomImpliesPredicate returns true if the atom expression e implies the
// ScalarExpr pred. The atom e cannot be an AndExpr, OrExpr, RangeExpr, or
// FiltersExpr.
func (im *Implicator) atomImpliesPredicate(
	e opt.ScalarExpr, pred opt.ScalarExpr, exactMatches map[opt.Expr]struct{},
) bool {
	switch pt := pred.(type) {
	case *memo.AndExpr:
		// atom A => AND-expr B iff A => each of B's children.
		leftPredImplied := im.atomImpliesPredicate(e, pt.Left, exactMatches)
		rightPredImplied := im.atomImpliesPredicate(e, pt.Right, exactMatches)
		return leftPredImplied && rightPredImplied

	case *memo.OrExpr:
		// atom A => OR-expr B iff A => any of B's children.
		if im.atomImpliesPredicate(e, pt.Left, exactMatches) {
			return true
		}
		return im.atomImpliesPredicate(e, pt.Right, exactMatches)

	default:
		// atom A => atom B iff B contains A.
		return im.atomContainsAtom(pred, e)
	}
}

// atomContainsAtom returns true if atom expression a contains atom expression
// b, meaning that all values for variables in which b evaluates to true, a also
// evaluates to true.
//
// Constraints are used to prove containment because they make it easy to assess
// if one expression contains another, handling many types of expressions
// including comparison operators, IN operators, and tuples.
func (im *Implicator) atomContainsAtom(a, b opt.ScalarExpr) bool {
	// Build constraint sets for a and b, unless they have been cached.
	aSet, aTight, ok := im.fetchConstraint(a)
	if !ok {
		aSet, aTight = memo.BuildConstraints(a, im.md, im.evalCtx)
		im.cacheConstraint(a, aSet, aTight)
	}
	bSet, bTight, ok := im.fetchConstraint(b)
	if !ok {
		bSet, bTight = memo.BuildConstraints(b, im.md, im.evalCtx)
		im.cacheConstraint(b, bSet, bTight)
	}

	// If either set has more than one constraint, then constraints cannot be
	// used to prove containment. This happens when an expression has more than
	// one variable. For example:
	//
	//   @1 > @2
	//
	// Produces the constraint set:
	//
	//   /1: (/NULL - ]; /2: (/NULL - ]
	//
	// TODO(mgartner): Prove implication in cases like (a > b) => (a >= b),
	// without using constraints.
	if aSet.Length() > 1 || bSet.Length() > 1 {
		return false
	}

	// Containment cannot be proven if either constraint is not tight, because
	// the constraint does not fully represent the expression.
	if !aTight || !bTight {
		return false
	}

	ac := aSet.Constraint(0)
	bc := bSet.Constraint(0)

	// If the columns in ac are not a prefix of the columns in bc, then ac
	// cannot contain bc.
	if !ac.Columns.IsPrefixOf(&bc.Columns) {
		return false
	}

	return ac.Contains(im.evalCtx, bc)
}

// cacheConstraint caches a constraint set and a tight boolean for the given
// scalar expression.
func (im *Implicator) cacheConstraint(e opt.ScalarExpr, c *constraint.Set, tight bool) {
	if _, ok := im.constraintCache[e]; !ok {
		im.constraintCache[e] = constraintResult{
			c:     c,
			tight: tight,
		}
	}
}

// fetchConstraint returns a constraint set, tight boolean, and true if the
// cache contains an entry for the given scalar expression. It returns
// ok = false if the scalar expression does not exist in the cache.
func (im *Implicator) fetchConstraint(e opt.ScalarExpr) (_ *constraint.Set, tight bool, ok bool) {
	if res, ok := im.constraintCache[e]; ok {
		return res.c, res.tight, true
	}
	return nil, false, false
}

// simplifyFiltersExpr returns a new FiltersExpr with any expressions in e that
// exist in exactMatches removed.
//
// If a FiltersItem at the root exists in exactMatches, the entire FiltersItem
// is omitted from the returned FiltersItem. If not, the FiltersItem is
// recursively searched. See simplifyScalarExpr for more details.
func (im *Implicator) simplifyFiltersExpr(
	e memo.FiltersExpr, exactMatches map[opt.Expr]struct{},
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
		s := im.simplifyScalarExpr(e[i].Condition, exactMatches)

		// If the scalar expression was reduced to True, don't add it to the
		// filters.
		if s != memo.TrueSingleton {
			filters = append(filters, im.f.ConstructFiltersItem(s))
		}
	}

	return filters
}

// simplifyScalarExpr simplifies combinations of RangeExprs and AndExprs by
// "removing" any expressions present in exactMatches. Note that expressions
// cannot simply be removed because RangeExprs and AndExprs are not lists of
// expressions. Instead, expressions in exactMatches are replaced with other
// expressions that are logically equivalent to removal of the expression.
//
// Also note that we do not attempt to traverse OrExprs. See
// FiltersImplyPredicate (rule #3) for more details.
func (im *Implicator) simplifyScalarExpr(
	e opt.ScalarExpr, exactMatches map[opt.Expr]struct{},
) opt.ScalarExpr {

	switch t := e.(type) {
	case *memo.RangeExpr:
		and := im.simplifyScalarExpr(t.And, exactMatches)
		return im.f.ConstructRange(and)

	case *memo.AndExpr:
		_, leftIsExactMatch := exactMatches[t.Left]
		_, rightIsExactMatch := exactMatches[t.Right]
		if leftIsExactMatch && rightIsExactMatch {
			return memo.TrueSingleton
		}
		if leftIsExactMatch {
			return im.simplifyScalarExpr(t.Right, exactMatches)
		}
		if rightIsExactMatch {
			return im.simplifyScalarExpr(t.Left, exactMatches)
		}
		left := im.simplifyScalarExpr(t.Left, exactMatches)
		right := im.simplifyScalarExpr(t.Right, exactMatches)
		return im.f.ConstructAnd(left, right)

	default:
		return e
	}
}

// flattenOrExpr returns a list of ScalarExprs that are all adjacent via
// disjunctions to the input OrExpr.
//
// For example, the input:
//
//   a OR (b AND c) OR (d OR e)
//
// Results in:
//
//  [a, (b AND c), d, e]
//
func flattenOrExpr(or *memo.OrExpr) []opt.ScalarExpr {
	ors := make([]opt.ScalarExpr, 0, 2)

	var collect func(e opt.ScalarExpr)
	collect = func(e opt.ScalarExpr) {
		if and, ok := e.(*memo.OrExpr); ok {
			collect(and.Left)
			collect(and.Right)
		} else {
			ors = append(ors, e)
		}
	}
	collect(or)

	return ors
}
