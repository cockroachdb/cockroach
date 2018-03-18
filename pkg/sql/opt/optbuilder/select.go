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
func (b *Builder) buildTable(
	texpr tree.TableExpr, inScope *scope,
) (out memo.GroupID, outScope *scope) {
	// NB: The case statements are sorted lexicographically.
	switch source := texpr.(type) {
	case *tree.AliasedTableExpr:
		out, outScope = b.buildTable(source.Expr, inScope)

		// Overwrite output properties with any alias information.
		b.renameSource(source.As, outScope)
		return out, outScope

	case *tree.JoinTableExpr:
		return b.buildJoin(source, inScope)

	case *tree.NormalizableTableName:
		tn, err := source.Normalize()
		if err != nil {
			panic(builderError{err})
		}
		tbl, err := b.catalog.FindTable(b.ctx, tn)
		if err != nil {
			panic(builderError{err})
		}

		return b.buildScan(tbl, tn, inScope)

	case *tree.ParenTableExpr:
		return b.buildTable(source.Expr, inScope)

	case *tree.Subquery:
		return b.buildStmt(source.Select, inScope)

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
func (b *Builder) buildScan(
	tbl opt.Table, tn *tree.TableName, inScope *scope,
) (out memo.GroupID, outScope *scope) {
	tblIndex := b.factory.Metadata().AddTable(tbl)
	scanOpDef := memo.ScanOpDef{Table: tblIndex}

	outScope = inScope.push()
	for i := 0; i < tbl.ColumnCount(); i++ {
		col := tbl.Column(i)
		colIndex := b.factory.Metadata().TableColumn(tblIndex, i)
		name := tree.Name(col.ColName())
		colProps := columnProps{
			index:    colIndex,
			origName: name,
			name:     name,
			table:    *tn,
			typ:      col.DatumType(),
			hidden:   col.IsHidden(),
		}

		scanOpDef.Cols.Add(int(colIndex))
		b.colMap = append(b.colMap, colProps)
		outScope.cols = append(outScope.cols, colProps)
	}

	return b.factory.ConstructScan(b.factory.InternPrivate(&scanOpDef)), outScope
}

// buildSelect builds a set of memo groups that represent the given select
// statement.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildSelect(
	stmt *tree.Select, inScope *scope,
) (out memo.GroupID, outScope *scope) {
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
			limit = stmt.Limit
		}
	}

	// NB: The case statements are sorted lexicographically.
	switch t := stmt.Select.(type) {
	case *tree.SelectClause:
		out, outScope = b.buildSelectClause(stmt, inScope)

	case *tree.UnionClause:
		out, outScope = b.buildUnion(t, inScope)

	case *tree.ValuesClause:
		out, outScope = b.buildValuesClause(t, inScope)

	default:
		panic(errorf("not yet implemented: select statement: %T", stmt.Select))
	}

	if outScope.ordering == nil && orderBy != nil {
		projectionsScope := outScope.push()
		projectionsScope.cols = make([]columnProps, 0, len(outScope.cols))
		projections := make([]memo.GroupID, 0, len(outScope.cols))
		for i := range outScope.cols {
			p := b.buildScalarProjection(&outScope.cols[i], "", outScope, projectionsScope)
			projections = append(projections, p)
		}
		out, outScope = b.buildOrderBy(orderBy, out, projections, outScope, projectionsScope)
	}

	if limit != nil {
		out, outScope = b.buildLimit(limit, inScope, out, outScope)
	}

	// TODO(rytaft): Support FILTER expression.
	return out, outScope
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
	stmt *tree.Select, inScope *scope,
) (out memo.GroupID, outScope *scope) {
	sel := stmt.Select.(*tree.SelectClause)

	var fromScope *scope
	out, fromScope = b.buildFrom(sel.From, sel.Where, inScope)
	outScope = fromScope

	var projections []memo.GroupID
	var projectionsScope *scope
	if b.needsAggregation(sel) {
		out, outScope, projections, projectionsScope = b.buildAggregation(sel, out, fromScope)
	} else {
		projectionsScope = fromScope.push()
		projections = b.buildProjectionList(sel.Exprs, fromScope, projectionsScope)
	}

	if stmt.OrderBy != nil {
		// Wrap with distinct operator if it exists.
		out, outScope = b.buildDistinct(out, sel.Distinct, projectionsScope.cols, outScope)

		// OrderBy can reference columns from either the from/grouping clause or
		// the projections clause.
		out, outScope = b.buildOrderBy(stmt.OrderBy, out, projections, outScope, projectionsScope)
		return out, outScope
	}

	// Don't add an unnecessary "pass through" project expression.
	if !projectionsScope.hasSameColumns(outScope) {
		p := b.constructList(opt.ProjectionsOp, projections, projectionsScope.cols)
		out = b.factory.ConstructProject(out, p)
		outScope = projectionsScope
	}

	// Wrap with distinct operator if it exists.
	out, outScope = b.buildDistinct(out, sel.Distinct, outScope.cols, outScope)
	return out, outScope
}

// buildFrom builds a set of memo groups that represent the given FROM statement
// and WHERE clause.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildFrom(
	from *tree.From, where *tree.Where, inScope *scope,
) (out memo.GroupID, outScope *scope) {
	var left, right memo.GroupID

	for _, table := range from.Tables {
		var rightScope *scope
		right, rightScope = b.buildTable(table, inScope)

		if left == 0 {
			left = right
			outScope = rightScope
			continue
		}

		outScope.appendColumns(rightScope)

		left = b.factory.ConstructInnerJoin(left, right, b.factory.ConstructTrue())
	}

	if left == 0 {
		// TODO(peter): This should be a table with 1 row and 0 columns to match
		// current cockroach behavior.
		rows := []memo.GroupID{b.factory.ConstructTuple(b.factory.InternList(nil))}
		out = b.factory.ConstructValues(
			b.factory.InternList(rows),
			b.factory.InternPrivate(&opt.ColList{}),
		)
		outScope = inScope
	} else {
		out = left
	}

	if where != nil {
		// All "from" columns are visible to the filter expression.
		texpr := outScope.resolveType(where.Expr, types.Bool)
		filter := b.buildScalar(texpr, outScope)
		out = b.factory.ConstructSelect(out, filter)
	}

	return out, outScope
}
