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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// analyzeOrderBy analyzes an Ordering physical property from the ORDER BY
// clause. The untyped orderBy is returned as a new tree.OrderBy with one or
// more typed expressions.
func (b *Builder) analyzeOrderBy(
	orderBy tree.OrderBy, labels []string, selExprs tree.TypedExprs, inScope *scope,
) tree.OrderBy {
	if orderBy == nil {
		return nil
	}

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
	b.semaCtx.Properties.Require("ORDER BY", tree.RejectGenerators)

	orderByOut := make(tree.OrderBy, 0, len(orderBy))
	for i := range orderBy {
		ob := b.analyzeOrderByArg(orderBy[i], labels, selExprs, inScope)
		orderByOut = append(orderByOut, ob...)
	}

	return orderByOut
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
func (b *Builder) buildOrderBy(orderBy tree.OrderBy, inScope, projectionsScope *scope) {
	if orderBy == nil {
		return
	}

	orderByScope := inScope.push()
	orderByScope.ordering = make([]opt.OrderingColumn, 0, len(orderBy))

	for i := range orderBy {
		b.buildOrderByArg(orderBy[i], inScope, projectionsScope, orderByScope)
	}

	projectionsScope.setOrdering(orderByScope.cols, orderByScope.ordering)
}

// findIndexByName returns an index in the table with the given name. If the
// name is empty the primary index is returned.
func (b *Builder) findIndexByName(table opt.Table, name tree.UnrestrictedName) (opt.Index, error) {
	if name == "" {
		return table.Index(0), nil
	}

	for i, n := 0, table.IndexCount(); i < n; i++ {
		idx := table.Index(i)
		if string(name) == idx.IdxName() {
			return idx, nil
		}
	}

	return nil, fmt.Errorf(`index %q not found`, name)
}

// addExtraColumn adds expr as a column to extraColsScope; if it is
// already projected in projectionsScope then that projection is re-used.
func (b *Builder) addExtraColumn(
	expr tree.TypedExpr, inScope, projectionsScope, extraColsScope *scope,
) {
	// Use an existing projection if possible. Otherwise, build a new
	// projection.
	if col := projectionsScope.findExistingCol(expr); col != nil {
		extraColsScope.cols = append(extraColsScope.cols, *col)
	} else {
		b.buildScalarProjection(expr, "" /* label */, inScope, extraColsScope)
	}
}

// buildOrderByIndex appends to the ordering a column for each indexed column
// in the specified index, including the implicit primary key columns.
func (b *Builder) buildOrderByIndex(
	order *tree.Order, inScope, projectionsScope, orderByScope *scope,
) {
	tn, err := order.Table.Normalize()
	if err != nil {
		panic(builderError{err})
	}

	tab, ok := b.resolveDataSource(tn, privilege.SELECT).(opt.Table)
	if !ok {
		panic(builderError{sqlbase.NewWrongObjectTypeError(tn, "table")})
	}

	index, err := b.findIndexByName(tab, order.Index)
	if err != nil {
		panic(builderError{err})
	}

	start := len(orderByScope.cols)
	// Append each key column from the index (including the implicit primary key
	// columns) to the ordering scope.
	for i, n := 0, index.KeyColumnCount(); i < n; i++ {
		// Columns which are indexable are always orderable.
		col := index.Column(i)
		if err != nil {
			panic(err)
		}

		colItem := tree.NewColumnItem(tab.Name(), col.Column.ColName())
		expr := inScope.resolveType(colItem, types.Any, "ORDER BY")
		b.addExtraColumn(expr, inScope, projectionsScope, orderByScope)
	}

	// Add the new columns to the ordering.
	for i := start; i < len(orderByScope.cols); i++ {
		desc := index.Column(i - start).Descending

		// DESC inverts the order of the index.
		if order.Direction == tree.Descending {
			desc = !desc
		}

		orderByScope.ordering = append(orderByScope.ordering,
			opt.MakeOrderingColumn(orderByScope.cols[i].id, desc),
		)
	}
}

// analyzeOrderByArg analyzes a single ORDER BY argument. Typically this is a
// single column, with the exception of qualified star "table.*". The argument
// is returned as a new tree.OrderBy with one or more typed expressions.
func (b *Builder) analyzeOrderByArg(
	order *tree.Order, labels []string, selExprs tree.TypedExprs, inScope *scope,
) tree.OrderBy {
	if order.OrderType == tree.OrderByIndex {
		// No aggregates possible. This case will be handled later by
		// buildOrderByArg.
		return tree.OrderBy{order}
	}

	// Analyze the ORDER BY column(s).
	exprs := b.analyzeExtraArgument(order.Expr, labels, selExprs, inScope, "ORDER BY")
	orderBy := make(tree.OrderBy, len(exprs))
	for i, e := range exprs {
		orderBy[i] = &tree.Order{
			Expr:      e,
			Direction: order.Direction,
		}
	}
	return orderBy
}

// buildOrderByArg sets up the projection of a single ORDER BY argument.
// The projection column is added to the orderByScope and used to build
// an ordering on the same scope.
func (b *Builder) buildOrderByArg(
	order *tree.Order, inScope, projectionsScope, orderByScope *scope,
) {
	if order.OrderType == tree.OrderByIndex {
		b.buildOrderByIndex(order, inScope, projectionsScope, orderByScope)
		return
	}

	// Build the ORDER BY column. As a side effect, this will append a new
	// column to the end of orderByScope.cols.
	b.addExtraColumn(order.Expr.(tree.TypedExpr), inScope, projectionsScope, orderByScope)

	// Add the new column to the ordering.
	idx := len(orderByScope.cols) - 1
	orderByScope.ordering = append(orderByScope.ordering,
		opt.MakeOrderingColumn(orderByScope.cols[idx].id, order.Direction == tree.Descending),
	)
}

// analyzeExtraArgument analyzes a single ORDER BY or DISTINCT ON argument.
// Typically this is a single column, with the exception of qualified star
// (table.*). The argument is returned as a slice of typed expressions.
func (b *Builder) analyzeExtraArgument(
	expr tree.Expr, labels []string, selExprs tree.TypedExprs, inScope *scope, context string,
) (exprs tree.TypedExprs) {
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
	idx := colIdxByProjectionAlias(expr, context, labels, selExprs)

	// If the expression does not refer to an alias, deal with
	// column ordinals.
	if idx == -1 {
		idx = colIndex(len(selExprs), expr, context)
	}

	if idx != -1 {
		exprs = []tree.TypedExpr{selExprs[idx]}
	} else {
		exprs = b.expandStarAndResolveType(expr, inScope, context)

		// ORDER BY (a, b) -> ORDER BY a, b
		exprs = flattenTuples(exprs)
	}

	// Ensure we can order on the given column(s).
	for _, e := range exprs {
		ensureColumnOrderable(e)
	}

	return exprs
}

func ensureColumnOrderable(e tree.TypedExpr) {
	if _, ok := e.ResolvedType().(types.TArray); ok || e.ResolvedType() == types.JSON {
		panic(unimplementedf("can't order by column type %s", e.ResolvedType()))
	}
}
