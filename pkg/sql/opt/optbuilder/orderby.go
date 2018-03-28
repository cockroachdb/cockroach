// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// buildOrderBy builds an Ordering physical property from the ORDER BY clause.
// ORDER BY is not a relational expression, but instead a required physical
// property on the output.
//
// Since the ordering property can only refer to output columns, we may need
// to add a projection for the ordering columns. For example, consider the
// following query:
//     SELECT a FROM t ORDER BY c
// The `c` column must be retained in the projection (and the presentation
// property then omits it).
//
// buildOrderBy returns a list of memo groups that combines projected columns
// from the SELECT list (the projections parameter) and any ORDER BY columns
// that are not already present in the SELECT list. buildOrderBy adds any new
// ORDER BY columns to the projectionsScope and sets the ordering and
// presentation properties on the projectionsScope. These properties later
// become part of the required physical properties returned by Build.
func (b *Builder) buildOrderBy(
	orderBy tree.OrderBy,
	in memo.GroupID,
	projections []memo.GroupID,
	inScope, projectionsScope *scope,
) []memo.GroupID {
	if orderBy == nil {
		return projections
	}

	orderByScope := inScope.push()
	orderByScope.physicalProps.Ordering = make(memo.Ordering, 0, len(orderBy))
	orderByProjections := make([]memo.GroupID, 0, len(orderBy))

	// TODO(rytaft): rewrite ORDER BY if it uses ORDER BY INDEX tbl@idx or
	// ORDER BY PRIMARY KEY syntax.

	for i := range orderBy {
		orderByProjections = b.buildOrdering(
			orderBy[i], orderByProjections, inScope, projectionsScope, orderByScope,
		)
	}

	return b.buildOrderByProject(projections, orderByProjections, projectionsScope, orderByScope)
}

// buildOrdering sets up the projection(s) of a single ORDER BY argument.
// Typically this is a single column, with the exception of *.
//
// The projections are appended to the projections slice (and the resulting
// slice is returned). Corresponding columns are added to the orderByScope.
func (b *Builder) buildOrdering(
	order *tree.Order, projections []memo.GroupID, inScope, projectionsScope, orderByScope *scope,
) []memo.GroupID {
	// Unwrap parenthesized expressions like "((a))" to "a".
	expr := tree.StripParens(order.Expr)

	// The logical data source for ORDER BY is the list of column
	// expressions for a SELECT, as specified in the input SQL text
	// (or an entire UNION or VALUES clause).  Alas, SQL has some
	// historical baggage from SQL92 and there are some special cases:
	//
	// SQL92 rules:
	//
	// 1) if the expression is the aliased (AS) name of an
	//    expression in a SELECT clause, then use that
	//    expression as sort key.
	//    e.g. SELECT a AS b, b AS c ORDER BY b
	//    this sorts on the first column.
	//
	// 2) column ordinals. If a simple integer literal is used,
	//    optionally enclosed within parentheses but *not subject to
	//    any arithmetic*, then this refers to one of the columns of
	//    the data source. Then use the SELECT expression at that
	//    ordinal position as sort key.
	//
	// SQL99 rules:
	//
	// 3) otherwise, if the expression is already in the SELECT list,
	//    then use that expression as sort key.
	//    e.g. SELECT b AS c ORDER BY b
	//    this sorts on the first column.
	//    (this is an optimization)
	//
	// 4) if the sort key is not dependent on the data source (no
	//    IndexedVar) then simply do not sort. (this is an optimization)
	//
	// 5) otherwise, add a new projection with the ORDER BY expression
	//    and use that as sort key.
	//    e.g. SELECT a FROM t ORDER by b
	//    e.g. SELECT a, b FROM t ORDER by a+b

	// First, deal with projection aliases.
	idx := colIdxByProjectionAlias(expr, "ORDER BY", projectionsScope)

	// If the expression does not refer to an alias, deal with
	// column ordinals.
	if idx == -1 {
		idx = colIndex(len(projectionsScope.cols), expr, "ORDER BY")
	}

	var exprs []tree.TypedExpr
	if idx != -1 {
		exprs = []tree.TypedExpr{&projectionsScope.cols[idx]}
	} else {
		exprs = b.expandStarAndResolveType(expr, inScope)

		// ORDER BY (a, b) -> ORDER BY a, b
		exprs = flattenTuples(exprs)
	}

	// Build each of the ORDER BY columns. As a side effect, this will append new
	// columns to the end of orderByScope.cols.
	start := len(orderByScope.cols)
	for _, e := range exprs {
		// Ensure we can order on the given column.
		ensureColumnOrderable(e)
		scalar := b.buildScalarProjection(e, "", inScope, orderByScope)

		// Use an existing projection if possible.
		// TODO(rytaft): If we store the memo.GroupID in the column, we can avoid
		// rebuilding the expression.
		if col := projectionsScope.findExistingCol(e); col != nil {
			// buildScalarProjection added a new column to the end of
			// orderByScope.cols. Replace it with the already existing column.
			orderByScope.cols[len(orderByScope.cols)-1] = *col
		}

		projections = append(projections, scalar)
	}

	// Add the new columns to the ordering.
	for i := start; i < len(orderByScope.cols); i++ {
		col := opt.MakeOrderingColumn(
			orderByScope.cols[i].id,
			order.Direction == tree.Descending,
		)
		orderByScope.physicalProps.Ordering = append(orderByScope.physicalProps.Ordering, col)
	}

	return projections
}

// buildOrderByProject returns a list of memo groups that combines projections
// and any items from orderByProjections that are not already present in
// projections. buildOrderByProject adds any new ORDER BY columns to the
// projectionsScope and sets the ordering and presentation properties on the
// projectionsScope. These properties later become part of the required
// physical properties returned by Build.
func (b *Builder) buildOrderByProject(
	projections, orderByProjections []memo.GroupID, projectionsScope, orderByScope *scope,
) []memo.GroupID {
	for i := range orderByScope.cols {
		col := &orderByScope.cols[i]

		// Only append order by columns that aren't already present.
		if findColByIndex(projectionsScope.cols, col.id) == nil {
			projectionsScope.cols = append(projectionsScope.cols, *col)
			projectionsScope.cols[len(projectionsScope.cols)-1].hidden = true
			projections = append(projections, orderByProjections[i])
		}
	}

	projectionsScope.physicalProps.Ordering = orderByScope.physicalProps.Ordering
	projectionsScope.physicalProps.Presentation = makePresentation(projectionsScope.cols)
	return projections
}

func ensureColumnOrderable(e tree.TypedExpr) {
	if _, ok := e.ResolvedType().(types.TArray); ok || e.ResolvedType() == types.JSON {
		panic(builderError{pgerror.NewErrorf(pgerror.CodeFeatureNotSupportedError,
			"can't order by column type %s", e.ResolvedType())})
	}
}
