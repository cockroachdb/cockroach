// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// constructDistinct wraps inScope.group in a DistinctOn operator corresponding
// to a SELECT DISTINCT statement.
func (b *Builder) constructDistinct(inScope *scope) memo.RelExpr {
	// We are doing a distinct along all the projected columns.
	var private memo.GroupingPrivate
	for i := range inScope.cols {
		if inScope.cols[i].visibility == visible {
			private.GroupingCols.Add(inScope.cols[i].id)
		}
	}

	// Check that the ordering only refers to projected columns.
	// This will cause an error for queries like:
	//   SELECT DISTINCT a FROM t ORDER BY b
	// Note: this behavior is consistent with PostgreSQL.
	for _, col := range inScope.ordering {
		if !private.GroupingCols.Contains(col.ID()) {
			panic(pgerror.Newf(
				pgcode.InvalidColumnReference,
				"for SELECT DISTINCT, ORDER BY expressions must appear in select list",
			))
		}
	}

	// We don't set def.Ordering. Because the ordering can only refer to projected
	// columns, it does not affect the results; it doesn't need to be required of
	// the DistinctOn input.
	input := inScope.expr.(memo.RelExpr)
	return b.factory.ConstructDistinctOn(input, memo.EmptyAggregationsExpr, &private)
}

// buildDistinctOn builds a set of memo groups that represent a DISTINCT ON
// expression. If nullsAreDistinct is true, then construct the UpsertDistinctOn
// operator rather than the DistinctOn operator (see the UpsertDistinctOn
// operator comment for details on the differences). The errorOnDup parameter
// controls whether multiple rows in the same distinct group trigger an error.
// If empty, no error is triggered. This can only take on a value in the
// EnsureDistinctOn and EnsureUpsertDistinctOn cases.
func (b *Builder) buildDistinctOn(
	distinctOnCols opt.ColSet, inScope *scope, nullsAreDistinct bool, errorOnDup string,
) (outScope *scope) {
	// When there is a DISTINCT ON clause, the ORDER BY clause is restricted to either:
	//  1. Contain a subset of columns from the ON list, or
	//  2. Start with a permutation of all columns from the ON list.
	//
	// In case 1, the ORDER BY simply specifies an output ordering as usual.
	// Example:
	//   SELECT DISTINCT ON (a, b) c, d FROM t ORDER BY a, b
	//
	// In case 2, the ORDER BY columns serve two separate semantic purposes:
	//  - the prefix that contains the ON columns specifies an output ordering;
	//  - the rest of the columns affect the actual results of the query: for each
	//    set of values, the chosen row is the first according to that ordering.
	// Example:
	//   SELECT DISTINCT ON (a) b, c FROM t ORDER BY a, e
	//   This means: for each value of a, choose the (b, c) from the row with the
	//   smallest e value, and order these results by a.
	//
	// Note: this behavior is consistent with PostgreSQL.

	// Check that the DISTINCT ON expressions match the initial ORDER BY
	// expressions.
	var seen opt.ColSet
	for _, col := range inScope.ordering {
		if !distinctOnCols.Contains(col.ID()) {
			panic(pgerror.Newf(
				pgcode.InvalidColumnReference,
				"SELECT DISTINCT ON expressions must match initial ORDER BY expressions",
			))
		}
		seen.Add(col.ID())
		if seen.Equals(distinctOnCols) {
			// All DISTINCT ON columns showed up; other columns are allowed in the
			// rest of the ORDER BY (case 2 above).
			break
		}
	}

	private := memo.GroupingPrivate{GroupingCols: distinctOnCols.Copy(),
		NullsAreDistinct: nullsAreDistinct, ErrorOnDup: errorOnDup}

	// The ordering is used for intra-group ordering. Ordering with respect to the
	// DISTINCT ON columns doesn't affect intra-group ordering, so we add these
	// columns as optional.
	private.Ordering.FromOrderingWithOptCols(inScope.ordering, distinctOnCols)

	// Set up a new scope for the output of DISTINCT ON. This scope differs from
	// the input scope in that it doesn't have "extra" ORDER BY columns, e.g.
	// column e in case 2 example:
	//   SELECT DISTINCT ON (a) b, c FROM t ORDER BY a, e
	//
	//             +-------------------------+
	//             |   inScope  |  outScope  |
	// +-----------+------------+------------+
	// |   cols    |    b, c    |    b, c    |
	// | extraCols |    a, e    |     a      |
	// | ordering  |   a+, e+   |     a+     |
	// +-----------+------------+------------+
	outScope = inScope.replace()
	outScope.cols = make([]scopeColumn, 0, len(inScope.cols))
	// Add the output columns.
	for i := range inScope.cols {
		outScope.cols = append(outScope.cols, inScope.cols[i])
	}

	// Add any extra ON columns.
	outScope.extraCols = make([]scopeColumn, 0, len(inScope.extraCols))
	for i := range inScope.extraCols {
		if distinctOnCols.Contains(inScope.extraCols[i].id) {
			outScope.extraCols = append(outScope.extraCols, inScope.extraCols[i])
		}
	}

	// Retain the prefix of the ordering that refers to the ON columns.
	outScope.ordering = inScope.ordering
	for i, col := range inScope.ordering {
		if !distinctOnCols.Contains(col.ID()) {
			outScope.ordering = outScope.ordering[:i]
			break
		}
	}

	aggs := make(memo.AggregationsExpr, 0, len(inScope.cols))

	// Build FirstAgg for all visible columns except the DistinctOnCols
	// (and eliminate duplicates).
	excluded := distinctOnCols.Copy()
	for i := range outScope.cols {
		if id := outScope.cols[i].id; !excluded.Contains(id) {
			excluded.Add(id)
			aggs = append(aggs, b.factory.ConstructAggregationsItem(
				b.factory.ConstructFirstAgg(b.factory.ConstructVariable(id)),
				id,
			))
		}
	}

	input := inScope.expr.(memo.RelExpr)
	if nullsAreDistinct {
		if errorOnDup == "" {
			outScope.expr = b.factory.ConstructUpsertDistinctOn(input, aggs, &private)
		} else {
			outScope.expr = b.factory.ConstructEnsureUpsertDistinctOn(input, aggs, &private)
		}
	} else {
		if errorOnDup == "" {
			outScope.expr = b.factory.ConstructDistinctOn(input, aggs, &private)
		} else {
			outScope.expr = b.factory.ConstructEnsureDistinctOn(input, aggs, &private)
		}
	}
	return outScope
}

// analyzeDistinctOnArgs analyzes the DISTINCT ON columns and adds the
// resulting typed expressions to distinctOnScope.
func (b *Builder) analyzeDistinctOnArgs(
	distinctOn tree.DistinctOn, inScope, projectionsScope *scope,
) (distinctOnScope *scope) {
	if len(distinctOn) == 0 {
		return nil
	}

	distinctOnScope = inScope.push()
	distinctOnScope.cols = make([]scopeColumn, 0, len(distinctOn))

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
	b.semaCtx.Properties.Require(exprKindDistinctOn.String(), tree.RejectGenerators)
	inScope.context = exprKindDistinctOn

	for i := range distinctOn {
		b.analyzeExtraArgument(distinctOn[i], inScope, projectionsScope, distinctOnScope)
	}
	return distinctOnScope
}

// buildDistinctOnArgs builds the DISTINCT ON columns, adding to
// projectionsScope.extraCols as necessary.
// The set of DISTINCT ON columns is stored in projectionsScope.distinctOnCols.
func (b *Builder) buildDistinctOnArgs(inScope, projectionsScope, distinctOnScope *scope) {
	if distinctOnScope == nil {
		return
	}

	for i := range distinctOnScope.cols {
		b.addOrderByOrDistinctOnColumn(
			inScope, projectionsScope, distinctOnScope, &distinctOnScope.cols[i],
		)
	}
	projectionsScope.addExtraColumns(distinctOnScope.cols)
	projectionsScope.distinctOnCols = distinctOnScope.colSet()
}
