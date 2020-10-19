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
// Returns an error if any non-immutable operators are found.
//
// Note: This function should only be used to build partial index or arbiter
// predicate expressions that have only a table's columns in scope and that are
// not part of the relational expression tree. For example, this is used to
// populate the TableMeta.PartialIndexPredicates cache and for determining
// arbiter indexes in UPSERT and INSERT ON CONFLICT mutations. But it is not
// used for building synthesized mutation columns that determine whether or not
// to PUT or DEL a partial index entry for a row; these synthesized columns are
// projected as part of the opt expression tree and they reference columns
// beyond a table's base scope.
func (b *Builder) buildPartialIndexPredicate(
	tableScope *scope, expr tree.Expr, context string,
) (memo.FiltersExpr, error) {
	texpr := resolvePartialIndexPredicate(tableScope, expr)

	var scalar opt.ScalarExpr
	b.factory.FoldingControl().TemporarilyDisallowStableFolds(func() {
		scalar = b.buildScalar(texpr, tableScope, nil, nil, nil)
	})

	// Wrap the scalar in a FiltersItem.
	filter := b.factory.ConstructFiltersItem(scalar)

	// Expressions with non-immutable operators are not supported as partial
	// index or arbiter predicates.
	if filter.ScalarProps().VolatilitySet.HasStable() || filter.ScalarProps().VolatilitySet.HasVolatile() {
		return nil, tree.NewContextDependentOpsNotAllowedError(context)
	}

	// Wrap the expression in a FiltersExpr and normalize it by constructing a
	// Select expression.
	filters := memo.FiltersExpr{filter}
	selExpr := b.factory.ConstructSelect(tableScope.expr, filters)

	// If the normalized relational expression is a Select, return the filters.
	if sel, ok := selExpr.(*memo.SelectExpr); ok {
		return sel.Filters, nil
	}

	// Otherwise, the filters may be either true or false. Check the cardinality
	// to determine which one.
	if selExpr.Relational().Cardinality.IsZero() {
		return memo.FiltersExpr{b.factory.ConstructFiltersItem(memo.FalseSingleton)}, nil
	}

	return memo.TrueFilter, nil
}

// resolvePartialIndexPredicate attempts to resolve the type of expr as a
// boolean and return a tree.TypedExpr if successful. It asserts that no errors
// occur during resolution because the predicate should always be valid within
// this context. If an error occurs, it is likely due to a bug in the optimizer.
func resolvePartialIndexPredicate(tableScope *scope, expr tree.Expr) tree.TypedExpr {
	defer func() {
		if r := recover(); r != nil {
			panic(errors.AssertionFailedf("unexpected error during partial index predicate type resolution: %v", r))
		}
	}()
	return tableScope.resolveAndRequireType(expr, types.Bool)
}
