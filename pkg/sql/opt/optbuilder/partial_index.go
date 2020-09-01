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
// Simple normalization is applied to the expression in order to facilitate
// implication logic. See Factory.NormalizePartialIndexPredicate for more
// details.
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

	// Wrap the predicate filter expression in a FiltersExpr and normalize it.
	filters := memo.FiltersExpr{filter}
	return b.factory.NormalizePartialIndexPredicate(filters)
}
