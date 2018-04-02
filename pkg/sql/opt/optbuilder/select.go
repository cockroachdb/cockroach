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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// buildTable builds a set of memo groups that represent the given table
// expression. For example, if the tree.TableExpr consists of a single table,
// the resulting set of memo groups will consist of a single group with a
// scanOp operator. Joins will result in the construction of several groups,
// including two for the left and right table scans, at least one for the join
// condition, and one for the join itself.
// TODO(rytaft): Add support for function and join table expressions.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildTable(texpr tree.TableExpr, inScope *scope) (outScope *scope) {
	// NB: The case statements are sorted lexicographically.
	switch source := texpr.(type) {
	case *tree.AliasedTableExpr:
		outScope = b.buildTable(source.Expr, inScope)

		// Overwrite output properties with any alias information.
		b.renameSource(source.As, outScope)
		return outScope

	case *tree.JoinTableExpr:
		return b.buildJoin(source, inScope)

	case *tree.NormalizableTableName:
		tn, err := source.Normalize()
		if err != nil {
			panic(builderError{err})
		}
		tab, err := b.catalog.FindTable(b.ctx, tn)
		if err != nil {
			panic(builderError{err})
		}

		return b.buildScan(tab, tn, inScope)

	case *tree.ParenTableExpr:
		return b.buildTable(source.Expr, inScope)

	case *tree.Subquery:
		outScope = b.buildStmt(source.Select, inScope)
		outScope.removeHiddenCols()
		return outScope

	default:
		panic(errorf("not yet implemented: table expr: %T", texpr))
	}
}

// renameSource applies an AS clause to the columns in scope.
func (b *Builder) renameSource(as tree.AliasClause, scope *scope) {
	var tableAlias tree.Name
	colAlias := as.Cols

	if as.Alias != "" {
		// TODO(rytaft): Handle anonymous tables such as set-generating
		// functions with just one column.

		// If an alias was specified, use that.
		tableAlias = as.Alias
		for i := range scope.cols {
			scope.cols[i].table.TableName = tableAlias
		}
	}

	if len(colAlias) > 0 {
		// The column aliases can only refer to explicit columns.
		for colIdx, aliasIdx := 0, 0; aliasIdx < len(colAlias); colIdx++ {
			if colIdx >= len(scope.cols) {
				panic(errorf(
					"source %q has %d columns available but %d columns specified",
					tableAlias, aliasIdx, len(colAlias)))
			}
			if scope.cols[colIdx].hidden {
				continue
			}
			scope.cols[colIdx].name = colAlias[aliasIdx]
			aliasIdx++
		}
	}
}

// buildScan builds a memo group for a scanOp expression on the given table
// with the given table name.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildScan(tab opt.Table, tn *tree.TableName, inScope *scope) (outScope *scope) {
	tabID := b.factory.Metadata().AddTable(tab)
	scanOpDef := memo.ScanOpDef{Table: tabID}

	outScope = inScope.push()
	for i := 0; i < tab.ColumnCount(); i++ {
		col := tab.Column(i)
		colID := b.factory.Metadata().TableColumn(tabID, i)
		name := tree.Name(col.ColName())
		colProps := scopeColumn{
			id:       colID,
			origName: name,
			name:     name,
			table:    *tn,
			typ:      col.DatumType(),
			hidden:   col.IsHidden(),
		}

		scanOpDef.Cols.Add(int(colID))
		b.colMap = append(b.colMap, colProps)
		outScope.cols = append(outScope.cols, colProps)
	}

	outScope.group = b.factory.ConstructScan(b.factory.InternScanOpDef(&scanOpDef))
	return outScope
}

// buildSelect builds a set of memo groups that represent the given select
// statement.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildSelect(stmt *tree.Select, inScope *scope) (outScope *scope) {
	wrapped := stmt.Select
	orderBy := stmt.OrderBy
	limit := stmt.Limit

	for s, ok := wrapped.(*tree.ParenSelect); ok; s, ok = wrapped.(*tree.ParenSelect) {
		stmt = s.Select
		wrapped = stmt.Select
		if stmt.OrderBy != nil {
			if orderBy != nil {
				panic(errorf("multiple ORDER BY clauses not allowed"))
			}
			orderBy = stmt.OrderBy
		}
		if stmt.Limit != nil {
			if limit != nil {
				panic(errorf("multiple LIMIT clauses not allowed"))
			}
			limit = stmt.Limit
		}
	}

	// NB: The case statements are sorted lexicographically.
	switch t := stmt.Select.(type) {
	case *tree.SelectClause:
		outScope = b.buildSelectClause(t, orderBy, inScope)

	case *tree.UnionClause:
		outScope = b.buildUnion(t, inScope)

	case *tree.ValuesClause:
		outScope = b.buildValuesClause(t, inScope)

	default:
		panic(errorf("not yet implemented: select statement: %T", stmt.Select))
	}

	if outScope.physicalProps.Ordering == nil && orderBy != nil {
		projectionsScope := outScope.replace()
		projectionsScope.cols = make([]scopeColumn, 0, len(outScope.cols))
		for i := range outScope.cols {
			b.buildScalarProjection(&outScope.cols[i], "", outScope, projectionsScope)
		}
		b.buildOrderBy(orderBy, outScope, projectionsScope)
		b.constructProject(outScope, projectionsScope)
		outScope = projectionsScope
	}

	if limit != nil {
		b.buildLimit(limit, inScope, outScope)
	}

	// TODO(rytaft): Support FILTER expression.
	return outScope
}

// buildSelectClause builds a set of memo groups that represent the given
// select clause. We pass the entire select statement rather than just the
// select clause in order to handle ORDER BY scoping rules. ORDER BY can sort
// results using columns from the FROM/GROUP BY clause and/or from the
// projection list.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildSelectClause(
	sel *tree.SelectClause, orderBy tree.OrderBy, inScope *scope,
) (outScope *scope) {
	fromScope := b.buildFrom(sel.From, sel.Where, inScope)
	outScope = fromScope

	var projectionsScope *scope
	if b.needsAggregation(sel) {
		outScope, projectionsScope = b.buildAggregation(sel, orderBy, fromScope)
	} else {
		projectionsScope = fromScope.replace()
		b.buildProjectionList(sel.Exprs, fromScope, projectionsScope)
		b.buildOrderBy(orderBy, outScope, projectionsScope)
	}

	// Construct the projection.
	b.constructProject(outScope, projectionsScope)
	outScope = projectionsScope

	// Wrap with distinct operator if it exists.
	outScope = b.buildDistinct(sel.Distinct, outScope)
	return outScope
}

// buildFrom builds a set of memo groups that represent the given FROM statement
// and WHERE clause.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildFrom(from *tree.From, where *tree.Where, inScope *scope) (outScope *scope) {
	for _, table := range from.Tables {
		tableScope := b.buildTable(table, inScope)

		if outScope == nil {
			outScope = tableScope
			continue
		}

		outScope.appendColumns(tableScope)
		outScope.group = b.factory.ConstructInnerJoin(
			outScope.group, tableScope.group, b.factory.ConstructTrue(),
		)
	}

	if outScope == nil {
		// TODO(peter): This should be a table with 1 row and 0 columns to match
		// current cockroach behavior.
		rows := []memo.GroupID{b.factory.ConstructTuple(b.factory.InternList(nil))}
		outScope = inScope.push()
		outScope.group = b.factory.ConstructValues(
			b.factory.InternList(rows),
			b.factory.InternColList(opt.ColList{}),
		)
	}

	if where != nil {
		// All "from" columns are visible to the filter expression.
		texpr := outScope.resolveType(where.Expr, types.Bool)

		// Make sure there are no aggregation/window functions in the filter
		// (after subqueries have been expanded).
		b.assertNoAggregationOrWindowing(texpr, "WHERE")

		filter := b.buildScalar(texpr, outScope)
		outScope.group = b.factory.ConstructSelect(outScope.group, filter)
	}

	return outScope
}
