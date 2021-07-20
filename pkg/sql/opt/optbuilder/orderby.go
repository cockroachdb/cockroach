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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// analyzeOrderBy analyzes an Ordering physical property from the ORDER BY
// clause and adds the resulting typed expressions to orderByScope.
func (b *Builder) analyzeOrderBy(
	orderBy tree.OrderBy, inScope, projectionsScope *scope, rejectFlags tree.SemaRejectFlags,
) (orderByScope *scope) {
	if orderBy == nil {
		return nil
	}

	orderByScope = inScope.push()
	orderByScope.cols = make([]scopeColumn, 0, len(orderBy))

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
	b.semaCtx.Properties.Require(exprKindOrderBy.String(), rejectFlags)
	inScope.context = exprKindOrderBy

	for i := range orderBy {
		b.analyzeOrderByArg(orderBy[i], inScope, projectionsScope, orderByScope)
	}
	return orderByScope
}

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
// columns to the projectionsScope and sets the ordering property on the
// projectionsScope. This property later becomes part of the required physical
// properties returned by Build.
func (b *Builder) buildOrderBy(inScope, projectionsScope, orderByScope *scope) {
	if orderByScope == nil {
		return
	}

	orderByScope.ordering = make([]opt.OrderingColumn, 0, len(orderByScope.cols))

	for i := range orderByScope.cols {
		b.buildOrderByArg(inScope, projectionsScope, orderByScope, &orderByScope.cols[i])
	}

	projectionsScope.setOrdering(orderByScope.cols, orderByScope.ordering)
}

// findIndexByName returns an index in the table with the given name. If the
// name is empty the primary index is returned.
func (b *Builder) findIndexByName(table cat.Table, name tree.UnrestrictedName) (cat.Index, error) {
	if name == "" {
		return table.Index(0), nil
	}

	for i, n := 0, table.IndexCount(); i < n; i++ {
		idx := table.Index(i)
		if tree.Name(name) == idx.Name() {
			return idx, nil
		}
	}

	return nil, pgerror.Newf(pgcode.UndefinedObject,
		`index %q not found`, name)
}

// addOrderByOrDistinctOnColumn builds extraCol.expr as a column in extraColsScope; if it is
// already projected in projectionsScope then that projection is re-used.
func (b *Builder) addOrderByOrDistinctOnColumn(
	inScope, projectionsScope, extraColsScope *scope, extraCol *scopeColumn,
) {
	// Use an existing projection if possible (even if it has side-effects; see
	// the SQL99 rules described in analyzeExtraArgument). Otherwise, build a new
	// projection.
	if col := projectionsScope.findExistingCol(
		extraCol.getExpr(),
		true, /* allowSideEffects */
	); col != nil {
		extraCol.id = col.id
	} else {
		b.buildScalar(extraCol.getExpr(), inScope, extraColsScope, extraCol, nil)
	}
}

// analyzeOrderByIndex appends to the orderByScope a column for each indexed
// column in the specified index, including the implicit primary key columns.
func (b *Builder) analyzeOrderByIndex(
	order *tree.Order, inScope, projectionsScope, orderByScope *scope,
) {
	tab, tn := b.resolveTable(&order.Table, privilege.SELECT)
	index, err := b.findIndexByName(tab, order.Index)
	if err != nil {
		panic(err)
	}

	// We fully qualify the table name in case another table expression was
	// aliased to the same name as an existing table.
	tn.ExplicitCatalog = true
	tn.ExplicitSchema = true

	// Append each key column from the index (including the implicit primary key
	// columns) to the ordering scope.
	for i, n := 0, index.KeyColumnCount(); i < n; i++ {
		// Columns which are indexable are always orderable.
		col := index.Column(i)
		desc := col.Descending

		// DESC inverts the order of the index.
		if order.Direction == tree.Descending {
			desc = !desc
		}

		colItem := tree.NewColumnItem(&tn, col.ColName())
		expr := inScope.resolveType(colItem, types.Any)
		outCol := orderByScope.addColumn(scopeColName(""), expr)
		outCol.descending = desc
	}
}

// analyzeOrderByArg analyzes a single ORDER BY argument. Typically this is a
// single column, with the exception of qualified star "table.*". The resulting
// typed expression(s) are added to orderByScope.
func (b *Builder) analyzeOrderByArg(
	order *tree.Order, inScope, projectionsScope, orderByScope *scope,
) {
	if order.OrderType == tree.OrderByIndex {
		b.analyzeOrderByIndex(order, inScope, projectionsScope, orderByScope)
		return
	}

	// Analyze the ORDER BY column(s).
	start := len(orderByScope.cols)
	b.analyzeExtraArgument(order.Expr, inScope, projectionsScope, orderByScope)
	for i := start; i < len(orderByScope.cols); i++ {
		col := &orderByScope.cols[i]
		col.descending = order.Direction == tree.Descending
	}
}

// buildOrderByArg sets up the projection of a single ORDER BY argument.
// The projection column is built in the orderByScope and used to build
// an ordering on the same scope.
func (b *Builder) buildOrderByArg(
	inScope, projectionsScope, orderByScope *scope, orderByCol *scopeColumn,
) {
	// Build the ORDER BY column.
	b.addOrderByOrDistinctOnColumn(inScope, projectionsScope, orderByScope, orderByCol)

	// Add the new column to the ordering.
	orderByScope.ordering = append(orderByScope.ordering,
		opt.MakeOrderingColumn(orderByCol.id, orderByCol.descending),
	)
}

// analyzeExtraArgument analyzes a single ORDER BY or DISTINCT ON argument.
// Typically this is a single column, with the exception of qualified star
// (table.*). The resulting typed expression(s) are added to extraColsScope.
func (b *Builder) analyzeExtraArgument(
	expr tree.Expr, inScope, projectionsScope, extraColsScope *scope,
) {
	// Unwrap parenthesized expressions like "((a))" to "a".
	expr = tree.StripParens(expr)

	// The logical data source for ORDER BY or DISTINCT ON is the list of column
	// expressions for a SELECT, as specified in the input SQL text (or an entire
	// UNION or VALUES clause).  Alas, SQL has some historical baggage from SQL92
	// and there are some special cases:
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
	idx := colIdxByProjectionAlias(expr, inScope.context.String(), projectionsScope)

	// If the expression does not refer to an alias, deal with
	// column ordinals.
	if idx == -1 {
		idx = colIndex(len(projectionsScope.cols), expr, inScope.context.String())
	}

	var exprs tree.TypedExprs
	if idx != -1 {
		exprs = []tree.TypedExpr{projectionsScope.cols[idx].getExpr()}
	} else {
		exprs = b.expandStarAndResolveType(expr, inScope)

		// ORDER BY (a, b) -> ORDER BY a, b
		exprs = flattenTuples(exprs)
	}

	for _, e := range exprs {
		// Ensure we can order on the given column(s).
		ensureColumnOrderable(e)
		extraColsScope.addColumn(scopeColName(""), e)
	}
}

func ensureColumnOrderable(e tree.TypedExpr) {
	typ := e.ResolvedType()
	if typ.Family() == types.JsonFamily ||
		(typ.Family() == types.ArrayFamily && typ.ArrayContents().Family() == types.JsonFamily) {
		panic(unimplementedWithIssueDetailf(35706, "", "can't order by column type jsonb"))
	}
}
