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
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
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
// false negatives are possible (see the false-negative test file for known
// false negatives).
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
// An exprSet keeps track of exact matches encountered while exploring the
// filters and predicate expressions. If implication is proven, the filters
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
	constraintCache map[opt.ScalarExpr]constraintCacheItem
}

// constraintCacheItem contains the result of building the constraint for an atom.
// It includes both the constraint and the tight boolean.
type constraintCacheItem struct {
	c     *constraint.Set
	tight bool
}

// Init initializes an Implicator with the given factory, metadata, and eval
// context. It also resets the constraint cache.
func (im *Implicator) Init(f *norm.Factory, md *opt.Metadata, evalCtx *tree.EvalContext) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*im = Implicator{
		f:       f,
		md:      md,
		evalCtx: evalCtx,
	}
}

// ClearCache empties the Implicator's constraint cache.
func (im *Implicator) ClearCache() {
	im.constraintCache = nil
}

// FiltersImplyPredicate attempts to prove that a partial index predicate is
// implied by the given filters. If implication is proven, the function returns
// the remaining filters (which when applied on top of a partial index scan,
// make a plan equivalent to the original) and true. If implication cannot be
// proven, nil and false are returned. See Implicator for more details on how
// implication is proven and how the remaining filters are determined.
func (im *Implicator) FiltersImplyPredicate(
	filters memo.FiltersExpr, pred memo.FiltersExpr,
) (remainingFilters memo.FiltersExpr, ok bool) {
	// True is only implied by true.
	if pred.IsTrue() {
		return filters, filters.IsTrue()
	}

	// Check for exact matches for all FiltersItems in pred. This check is not
	// necessary for correctness because the recursive approach below handles
	// all cases. However, this is a faster path for common cases where
	// expressions in filters are exact matches to the entire predicate.
	if remFilters, ok := im.filtersImplyPredicateFastPath(filters, pred); ok {
		return remFilters, true
	}

	// Populate the constraint cache with any constraints already generated for
	// FiltersItems that are atoms.
	im.warmCache(filters)
	im.warmCache(pred)

	// If no exact match was found, recursively check the sub-expressions of the
	// filters and predicate. Use exactMatches to keep track of expressions in
	// filters that exactly match expressions in pred, so that they can be
	// removed from the remaining filters.
	exactMatches := make(exprSet)
	if im.scalarExprImpliesPredicate(&filters, &pred, exactMatches) {
		remainingFilters = im.simplifyFiltersExpr(filters, exactMatches)
		return remainingFilters, true
	}

	return nil, false
}

// filtersImplyPredicateFastPath returns remaining filters and true if every
// FiltersItem condition in pred exists in filters. This is a faster path for
// proving implication in common cases where expressions in filters are exact
// matches to expressions in the predicate.
//
// If this function returns false it is NOT proven that the filters do not imply
// pred. Instead, it indicates that the slower recursive walk of both expression
// trees is required to prove or disprove implication.
func (im *Implicator) filtersImplyPredicateFastPath(
	filters memo.FiltersExpr, pred memo.FiltersExpr,
) (remainingFilters memo.FiltersExpr, ok bool) {
	var filtersToRemove util.FastIntSet

	// For every FiltersItem in pred, search for a matching FiltersItem in
	// filters.
	for i := range pred {
		predCondition := pred[i].Condition
		exactMatchFound := false
		for j := range filters {
			filterCondition := filters[j].Condition

			// If there is a match, track the index of the filter so that it can
			// be removed from the remaining filters and move on to the next
			// predicate FiltersItem.
			if predCondition == filterCondition {
				exactMatchFound = true
				filtersToRemove.Add(j)
				break
			}
		}

		// If an exact match to the predicate filter was not found in filters,
		// then implication cannot be proven.
		if !exactMatchFound {
			return nil, false
		}
	}

	// Return an empty FiltersExpr if all filters are to be removed.
	if len(filters) == filtersToRemove.Len() {
		return memo.FiltersExpr{}, true
	}

	// Build the remaining filters from FiltersItems in filters which did not
	// have matches in the predicate.
	remainingFilters = make(memo.FiltersExpr, 0, len(filters)-filtersToRemove.Len())
	for i := range filters {
		if !filtersToRemove.Contains(i) {
			remainingFilters = append(remainingFilters, filters[i])
		}
	}
	return remainingFilters, true
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
	e opt.ScalarExpr, pred opt.ScalarExpr, exactMatches exprSet,
) bool {

	// If the expressions are an exact match, then e implies pred.
	if e == pred {
		exactMatches.add(e)
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

	case *memo.InExpr:
		return im.inExprImpliesPredicate(t, pred, exactMatches)

	default:
		return im.atomImpliesPredicate(e, pred, exactMatches)
	}
}

// filtersExprImpliesPredicate returns true if the FiltersExpr e implies the
// ScalarExpr pred.
func (im *Implicator) filtersExprImpliesPredicate(
	e *memo.FiltersExpr, pred opt.ScalarExpr, exactMatches exprSet,
) bool {
	switch pt := pred.(type) {
	case *memo.FiltersExpr:
		// AND-expr A => AND-expr B iff A => each of B's children.
		for i := range *pt {
			if !im.filtersExprImpliesPredicate(e, (*pt)[i].Condition, exactMatches) {
				return false
			}
		}
		return true

	case *memo.RangeExpr:
		// AND-expr A => AND-expr B iff A => each of B's children.
		and := pt.And.(*memo.AndExpr)
		return im.filtersExprImpliesPredicate(e, and.Left, exactMatches) &&
			im.filtersExprImpliesPredicate(e, and.Right, exactMatches)

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
		// children. The second part is logically equivalent to AND => atom
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
	e *memo.AndExpr, pred opt.ScalarExpr, exactMatches exprSet,
) bool {
	f := make(memo.FiltersExpr, 2)
	f[0] = memo.FiltersItem{Condition: e.Left}
	f[1] = memo.FiltersItem{Condition: e.Right}
	return im.filtersExprImpliesPredicate(&f, pred, exactMatches)
}

// orExprImpliesPredicate returns true if the OrExpr e implies the ScalarExpr
// pred.
//
// Note that in all recursive calls within this function, we do not pass
// exactMatches. See FiltersImplyPredicate (rule #3) for more details.
func (im *Implicator) orExprImpliesPredicate(e *memo.OrExpr, pred opt.ScalarExpr) bool {
	switch pt := pred.(type) {
	case *memo.FiltersExpr:
		// OR-expr A => AND-expr B iff A => each of B's children.
		for i := range *pt {
			if !im.orExprImpliesPredicate(e, (*pt)[i].Condition) {
				return false
			}
		}
		return true

	case *memo.RangeExpr:
		// OR-expr A => AND-expr B iff A => each of B's children.
		and := pt.And.(*memo.AndExpr)
		return im.orExprImpliesPredicate(e, and.Left) &&
			im.orExprImpliesPredicate(e, and.Right)

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

// inExprImpliesPredicate returns true if the InExpr e implies the ScalarExpr
// pred. It is similar to atomImpliesPredicate, except that it handles a special
// case where e is an IN expression and pred is an OR expression, such as:
//
//   a IN (1, 2)
//   =>
//   a = 1 OR a = 2
//
// Bespoke logic for IN filters and OR predicates in this form is required
// because the OrExpr must be considered as a single atomic unit in order to
// prove that the InExpr implies it. If the OrExpr was not treated as an atom,
// then (a = 1) and (a = 2) are considered individually. Neither expression
// individually contains (a IN (1, 2)), so implication could not be proven.
//
// If pred is not an OrExpr, it falls-back to treating pred as a non-atom and
// calls atomImpliesPredicate.
func (im *Implicator) inExprImpliesPredicate(
	e *memo.InExpr, pred opt.ScalarExpr, exactMatches exprSet,
) bool {
	// If pred is an OrExpr, treat it as an atom.
	if pt, ok := pred.(*memo.OrExpr); ok {
		return im.atomImpliesAtom(e, pt, exactMatches)
	}

	// If pred is not an OrExpr, then fallback to standard atom-filter
	// implication.
	return im.atomImpliesPredicate(e, pred, exactMatches)
}

// atomImpliesPredicate returns true if the atom expression e implies the
// ScalarExpr pred. The atom e cannot be an AndExpr, OrExpr, RangeExpr, or
// FiltersExpr.
func (im *Implicator) atomImpliesPredicate(
	e opt.ScalarExpr, pred opt.ScalarExpr, exactMatches exprSet,
) bool {
	switch pt := pred.(type) {
	case *memo.FiltersExpr:
		// atom A => AND-expr B iff A => each of B's children.
		for i := range *pt {
			if !im.atomImpliesPredicate(e, (*pt)[i].Condition, exactMatches) {
				return false
			}
		}
		return true

	case *memo.RangeExpr:
		// atom A => AND-expr B iff A => each of B's children.
		and := pt.And.(*memo.AndExpr)
		return im.atomImpliesPredicate(e, and.Left, exactMatches) &&
			im.atomImpliesPredicate(e, and.Right, exactMatches)

	case *memo.AndExpr:
		// atom A => AND-expr B iff A => each of B's children.
		return im.atomImpliesPredicate(e, pt.Left, exactMatches) &&
			im.atomImpliesPredicate(e, pt.Right, exactMatches)

	case *memo.OrExpr:
		// atom A => OR-expr B iff A => any of B's children.
		if im.atomImpliesPredicate(e, pt.Left, exactMatches) {
			return true
		}
		return im.atomImpliesPredicate(e, pt.Right, exactMatches)

	default:
		// atom A => atom B iff B contains A.
		return im.atomImpliesAtom(e, pred, exactMatches)
	}
}

// atomImpliesAtom returns true if the predicate atom expression, pred, contains
// atom expression e, meaning that all values for variables in which e evaluates
// to true, pred also evaluates to true.
//
// Constraints are used to prove containment because they make it easy to assess
// if one expression contains another, handling many types of expressions
// including comparison operators, IN operators, and tuples.
func (im *Implicator) atomImpliesAtom(
	e opt.ScalarExpr, pred opt.ScalarExpr, exactMatches exprSet,
) bool {
	// Check for containment of comparison expressions with two variables, like
	// a = b.
	if res, ok := im.twoVarComparisonImpliesTwoVarComparison(e, pred, exactMatches); ok {
		return res
	}

	// Build constraint sets for e and pred, unless they have been cached.
	eSet, eTight, ok := im.fetchConstraint(e)
	if !ok {
		eSet, eTight = memo.BuildConstraints(e, im.md, im.evalCtx)
		im.cacheConstraint(e, eSet, eTight)
	}
	predSet, predTight, ok := im.fetchConstraint(pred)
	if !ok {
		predSet, predTight = memo.BuildConstraints(pred, im.md, im.evalCtx)
		im.cacheConstraint(pred, predSet, predTight)
	}

	// If e is a contradiction, it represents an empty set of rows. The empty
	// set is contained by all sets, so a contradiction implies all predicates.
	if eSet == constraint.Contradiction {
		return true
	}

	// If pred is a contradiction, it represents an empty set of rows. The only
	// set contained by the empty set is itself (handled in the conditional
	// above). No other filters imply a contradiction.
	if predSet == constraint.Contradiction {
		return false
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
	if eSet.Length() > 1 || predSet.Length() > 1 {
		return false
	}

	// Containment cannot be proven if either constraint is not tight, because
	// the constraint does not fully represent the expression.
	if !eTight || !predTight {
		return false
	}

	eConstraint := eSet.Constraint(0)
	predConstraint := predSet.Constraint(0)

	// If the columns in predConstraint are not a prefix of the columns in
	// eConstraint, then predConstraint cannot contain eConstraint.
	if !predConstraint.Columns.IsPrefixOf(&eConstraint.Columns) {
		return false
	}

	// If predConstraint contains eConstraint, then eConstraint implies
	// predConstraint.
	if predConstraint.Contains(im.evalCtx, eConstraint) {
		// If the constraints contain each other, then they are semantically
		// equivalent and the filter atom can be removed from the remaining filters.
		// For example:
		//
		//   (a::INT > 17)
		//   =>
		//   (a::INT >= 18)
		//
		// (a > 17) is not the same expression as (a >= 18) syntactically, but
		// they are semantically equivalent because there are no integers
		// between 17 and 18. Therefore, there is no need to apply (a > 17) as a
		// filter after the partial index scan.
		exactMatches.addIf(e, func() bool {
			return eConstraint.Columns.IsPrefixOf(&predConstraint.Columns) &&
				eConstraint.Contains(im.evalCtx, predConstraint)
		})
		return true
	}

	return false
}

// twoVarComparisonImpliesTwoVarComparison returns true if pred contains e,
// where both expressions are comparisons (=, <, >, <=, >=, !=) of two
// variables. If either expressions is not a comparison of two variables, this
// function returns ok=false.
//
// For example, it can be prove that (a > b) implies (a >= b) because all
// values of a and b that satisfy the first expression also satisfy the second
// expression.
func (im *Implicator) twoVarComparisonImpliesTwoVarComparison(
	e opt.ScalarExpr, pred opt.ScalarExpr, exactMatches exprSet,
) (containment bool, ok bool) {
	if !isTwoVarComparison(e) || !isTwoVarComparison(pred) {
		return false, false
	}

	commutedOp := func(op opt.Operator) opt.Operator {
		switch op {
		case opt.EqOp:
			return opt.EqOp
		case opt.NeOp:
			return opt.NeOp
		case opt.LtOp:
			return opt.GtOp
		case opt.GtOp:
			return opt.LtOp
		case opt.LeOp:
			return opt.GeOp
		case opt.GeOp:
			return opt.LeOp
		default:
			panic(errors.AssertionFailedf("%s has no commuted operator", op))
		}
	}

	predLeftCol := pred.Child(0).(*memo.VariableExpr).Col
	predRightCol := pred.Child(1).(*memo.VariableExpr).Col
	impliesPred := func(a opt.ColumnID, b opt.ColumnID, op opt.Operator) bool {
		// If the columns are not the same, then pred is not implied.
		if a != predLeftCol || b != predRightCol {
			return false
		}

		// If the columns are the same and the ops are the same, then pred is
		// implied.
		if op == pred.Op() {
			return true
		}

		switch op {
		case opt.EqOp:
			// a = b implies a <= b and a >= b
			return pred.Op() == opt.LeOp || pred.Op() == opt.GeOp
		case opt.LtOp:
			// a < b implies a <= b and a != b
			return pred.Op() == opt.LeOp || pred.Op() == opt.NeOp
		case opt.GtOp:
			// a > b implies a >= b and a != b
			return pred.Op() == opt.GeOp || pred.Op() == opt.NeOp
		default:
			return false
		}
	}

	eLeftCol := e.Child(0).(*memo.VariableExpr).Col
	eRightCol := e.Child(1).(*memo.VariableExpr).Col
	if impliesPred(eLeftCol, eRightCol, e.Op()) || impliesPred(eRightCol, eLeftCol, commutedOp(e.Op())) {
		// If both operators are equal, or e's commuted operator is equal to
		// pred's operator, then e is an exact match to pred and it should be
		// removed from the remaining filters. For example, (a > b) and
		// (b < a) both individually imply (a > b) with no remaining filters.
		exactMatches.addIf(e, func() bool {
			return e.Op() == pred.Op() || commutedOp(e.Op()) == pred.Op()
		})
		return true, true
	}

	return false, true
}

// initConstraintCache initializes the constraintCache field if it has not yet
// been initialized.
func (im *Implicator) initConstraintCache() {
	if im.constraintCache == nil {
		im.constraintCache = make(map[opt.ScalarExpr]constraintCacheItem)
	}
}

// cacheConstraint caches a constraint set and a tight boolean for the given
// scalar expression.
func (im *Implicator) cacheConstraint(e opt.ScalarExpr, c *constraint.Set, tight bool) {
	im.initConstraintCache()
	if _, ok := im.constraintCache[e]; !ok {
		im.constraintCache[e] = constraintCacheItem{
			c:     c,
			tight: tight,
		}
	}
}

// fetchConstraint returns a constraint set, tight boolean, and true if the
// cache contains an entry for the given scalar expression. It returns
// ok = false if the scalar expression does not exist in the cache.
func (im *Implicator) fetchConstraint(e opt.ScalarExpr) (_ *constraint.Set, tight bool, ok bool) {
	im.initConstraintCache()
	if res, ok := im.constraintCache[e]; ok {
		return res.c, res.tight, true
	}
	return nil, false, false
}

// warmCache adds top-level atom constraints of filters to the cache. This
// prevents rebuilding constraints that have already have been built, reducing
// the overhead of proving implication.
func (im *Implicator) warmCache(filters memo.FiltersExpr) {
	for _, f := range filters {
		op := f.Condition.Op()
		if f.ScalarProps().Constraints != nil && op != opt.AndOp && op != opt.OrOp && op != opt.RangeOp {
			im.cacheConstraint(f.Condition, f.ScalarProps().Constraints, f.ScalarProps().TightConstraints)
		}
	}
}

// simplifyFiltersExpr returns a new FiltersExpr with any expressions in e that
// exist in exactMatches removed.
//
// If a FiltersItem at the root exists in exactMatches, the entire FiltersItem
// is omitted from the returned FiltersItem. If not, the FiltersItem is
// recursively searched. See simplifyScalarExpr for more details.
func (im *Implicator) simplifyFiltersExpr(
	e memo.FiltersExpr, exactMatches exprSet,
) memo.FiltersExpr {
	// If exactMatches is empty, then e cannot be simplified.
	if exactMatches.empty() {
		return e
	}

	filters := make(memo.FiltersExpr, 0, len(e))
	for i := range e {
		// If an entire FiltersItem exists in exactMatches, don't add it to the
		// output filters.
		if exactMatches.contains(e[i].Condition) {
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
func (im *Implicator) simplifyScalarExpr(e opt.ScalarExpr, exactMatches exprSet) opt.ScalarExpr {

	switch t := e.(type) {
	case *memo.RangeExpr:
		and := im.simplifyScalarExpr(t.And, exactMatches)
		return im.f.ConstructRange(and)

	case *memo.AndExpr:
		leftIsExactMatch := exactMatches.contains(t.Left)
		rightIsExactMatch := exactMatches.contains(t.Right)
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

// isTwoVarComparison returns true if the expression is a comparison
// expression (=, <, >, <=, >=, !=) and both side of the comparison are
// variables.
func isTwoVarComparison(e opt.ScalarExpr) bool {
	op := e.Op()
	return (op == opt.EqOp || op == opt.LtOp || op == opt.GtOp || op == opt.LeOp || op == opt.GeOp || op == opt.NeOp) &&
		e.Child(0).Op() == opt.VariableOp &&
		e.Child(1).Op() == opt.VariableOp
}

// exprSet represents a set of opt.Expr. It prevents nil pointer exceptions when
// tracking exact expression matches during implication. The nil value is passed
// in several places to avoid adding child expressions to the exact matches set,
// such as when traversing an OrExpr.
type exprSet map[opt.Expr]struct{}

// add adds an expression to the set if the set is non-nil.
func (s exprSet) add(e opt.Expr) {
	if s != nil {
		s[e] = struct{}{}
	}
}

// addIf adds an expression to the set if the set is non-nil and the given
// function returns true. addIf short-circuits and does not call the function if
// the set is nil.
func (s exprSet) addIf(e opt.Expr, fn func() bool) {
	if s != nil && fn() {
		s[e] = struct{}{}
	}
}

// empty returns true if the set is nil or empty.
func (s exprSet) empty() bool {
	return len(s) == 0
}

// contains returns true if the set is non-nil and the given expression exists
// in the set.
func (s exprSet) contains(e opt.Expr) bool {
	if s != nil {
		_, ok := s[e]
		return ok
	}
	return false
}
