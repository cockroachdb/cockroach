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
// buildOrderBy builds a set of memo groups for any ORDER BY columns that are
// not already present in the SELECT list (as represented by the initial set
// of columns in projectionsScope). buildOrderBy adds these new ORDER BY
// columns to the projectionsScope and sets the ordering and presentation
// properties on the projectionsScope. These properties later become part of
// the required physical properties returned by Build.
func (b *Builder) buildOrderBy(orderBy tree.OrderBy, inScope, projectionsScope *scope) {
	if orderBy == nil {
		return
	}

	orderByScope := inScope.push()
	orderByScope.physicalProps.Ordering = make(memo.Ordering, 0, len(orderBy))

	// TODO(rytaft): rewrite ORDER BY if it uses ORDER BY INDEX tbl@idx or
	// ORDER BY PRIMARY KEY syntax.

	for i := range orderBy {
		b.buildOrdering(orderBy[i], inScope, projectionsScope, orderByScope)
	}

	b.buildOrderByProject(projectionsScope, orderByScope)
}

// buildOrdering sets up the projection(s) of a single ORDER BY argument.
// Typically this is a single column, with the exception of *.
//
// The projection columns are added to the orderByScope.
func (b *Builder) buildOrdering(order *tree.Order, inScope, projectionsScope, orderByScope *scope) {
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

		// Use an existing projection if possible. Otherwise, build a new
		// projection.
		if col := projectionsScope.findExistingCol(e); col != nil {
			orderByScope.cols = append(orderByScope.cols, *col)
		} else {
			b.buildScalarProjection(e, "", inScope, orderByScope)
		}
	}

	// Add the new columns to the ordering.
	for i := start; i < len(orderByScope.cols); i++ {
		col := opt.MakeOrderingColumn(
			orderByScope.cols[i].id,
			order.Direction == tree.Descending,
		)
		orderByScope.physicalProps.Ordering = append(orderByScope.physicalProps.Ordering, col)
	}
}

// buildOrderByProject adds any columns from orderByScope to projectionsScope
// that are not already present in projectionsScope. buildOrderByProject also
// sets the ordering and presentation properties on the projectionsScope.
// These properties later become part of the required physical properties
// returned by Build.
func (b *Builder) buildOrderByProject(projectionsScope, orderByScope *scope) {
	for i := range orderByScope.cols {
		col := &orderByScope.cols[i]

		// Only append order by columns that aren't already present.
		if findColByIndex(projectionsScope.cols, col.id) == nil {
			projectionsScope.cols = append(projectionsScope.cols, *col)
			projectionsScope.cols[len(projectionsScope.cols)-1].hidden = true
		}
	}

	projectionsScope.physicalProps.Ordering = orderByScope.physicalProps.Ordering
	projectionsScope.physicalProps.Presentation = makePresentation(projectionsScope.cols)
}

func ensureColumnOrderable(e tree.TypedExpr) {
	if _, ok := e.ResolvedType().(types.TArray); ok || e.ResolvedType() == types.JSON {
		panic(builderError{pgerror.NewErrorf(pgerror.CodeFeatureNotSupportedError,
			"can't order by column type %s", e.ResolvedType())})
	}
}
