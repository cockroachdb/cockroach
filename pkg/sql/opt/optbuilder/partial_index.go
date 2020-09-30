// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// buildPartialIndexPredicate builds a memo.FiltersExpr from the given
// tree.Expr. The expression must be of type Bool and it must be immutable.
//
// Note: This function should only be used to build partial index predicate
// expressions that have only a table's columns in scope and that are not part
// of the relational expression tree. For example, this is used to populate the
// TableMeta.PartialIndexPredicates cache and for determining arbiter indexes in
// UPSERT and INSERT ON CONFLICT mutations. But it is not used for building
// synthesized mutation columns that determine whether or not to PUT or DEL a
// partial index entry for a row; these synthesized columns are projected as
// part of the opt expression tree and they reference columns beyond a table's
// base scope.
func (b *Builder) buildPartialIndexPredicate(tableScope *scope, expr tree.Expr) memo.FiltersExpr {
	texpr := tableScope.resolveAndRequireType(expr, types.Bool)

	var scalar opt.ScalarExpr
	b.factory.FoldingControl().TemporarilyDisallowStableFolds(func() {
		scalar = b.buildScalar(texpr, tableScope, nil, nil, nil)
	})

	// Wrap the scalar in a FiltersItem.
	filter := b.factory.ConstructFiltersItem(scalar)

	// Expressions with non-immutable operators are not supported as partial
	// index predicates.
	if filter.ScalarProps().VolatilitySet.HasStable() || filter.ScalarProps().VolatilitySet.HasVolatile() {
		panic(errors.AssertionFailedf("partial index predicate is not immutable"))
	}

	// Wrap the expression in a FiltersExpr and normalize it by constructing a
	// Select expression.
	filters := memo.FiltersExpr{filter}
	selExpr := b.factory.ConstructSelect(tableScope.expr, filters)

	// If the normalized relational expression is a Select, return the filters.
	if sel, ok := selExpr.(*memo.SelectExpr); ok {
		return sel.Filters
	}

	// Otherwise, the filters may be either true or false. Check the cardinality
	// to determine which one.
	if selExpr.Relational().Cardinality.IsZero() {
		return memo.FiltersExpr{b.factory.ConstructFiltersItem(memo.FalseSingleton)}
	}

	return memo.TrueFilter
}

// buildArbiterPredicate performs specific normalization functions to
// normalize an arbiter predicate. We cannot perform full normalization,
// because different filters should be treated separately. For example,
// x < 0 AND x > 0 is not a contradiction when used as an arbiter predicate.
func (b *Builder) buildArbiterPredicate(tableScope *scope, expr tree.Expr) memo.FiltersExpr {
	texpr := tableScope.resolveAndRequireType(expr, types.Bool)
	scalar := b.buildScalar(texpr, tableScope, nil, nil, nil)

	// Wrap the expression in a FiltersExpr.
	pred := memo.FiltersExpr{b.factory.ConstructFiltersItem(scalar)}

	// Run SimplifyFilters so that adjacent top-level AND expressions are
	// flattened into individual FiltersItems, like they would be during
	// normalization of a SELECT query. See the SimplifySelectFilters
	// normalization rule.
	//
	// NOTE: We currently do not recursively simplify the filters like
	// SimplifySelectFilters rule does. This could cause a false-negative when
	// partialidx.Implicator tries to prove that a filter implies a partial
	// index predicate. This trade-off avoids complexity until we find a
	// real-world example that motivates the recursive normalization.
	if !b.factory.CustomFuncs().IsFilterFalse(pred) {
		pred = b.factory.CustomFuncs().SimplifyFilters(pred)
	}

	// Run ConsolidateFilters so that adjacent top-level FiltersItems that
	// constrain a single variable are combined into a RangeExpr. See the
	// ConsolidateSelectFilters normalization rule.
	if b.factory.CustomFuncs().CanConsolidateFilters(pred) {
		pred = b.factory.CustomFuncs().ConsolidateFilters(pred)
	}

	// Run InlineConstVar so that constant variables are inlined. See the
	// InlineConstVar normalization rule.
	if b.factory.CustomFuncs().CanInlineConstVar(pred) {
		pred = b.factory.CustomFuncs().InlineConstVar(pred)
	}

	return pred
}
