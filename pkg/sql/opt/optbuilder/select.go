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

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// buildDataSource builds a set of memo groups that represent the given table
// expression. For example, if the tree.TableExpr consists of a single table,
// the resulting set of memo groups will consist of a single group with a
// scanOp operator. Joins will result in the construction of several groups,
// including two for the left and right table scans, at least one for the join
// condition, and one for the join itself.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildDataSource(
	texpr tree.TableExpr, indexFlags *tree.IndexFlags, inScope *scope,
) (outScope *scope) {
	// NB: The case statements are sorted lexicographically.
	switch source := texpr.(type) {
	case *tree.AliasedTableExpr:
		if source.IndexFlags != nil {
			indexFlags = source.IndexFlags
		}

		outScope = b.buildDataSource(source.Expr, indexFlags, inScope)

		if source.Ordinality {
			outScope = b.buildWithOrdinality("ordinality", outScope)
		}

		// Overwrite output properties with any alias information.
		b.renameSource(source.As, outScope)

		return outScope

	case *tree.JoinTableExpr:
		return b.buildJoin(source, inScope)

	case *tree.TableName:
		tn := source

		// CTEs take precedence over other data sources.
		if cte := inScope.resolveCTE(tn); cte != nil {
			if cte.used {
				panic(builderError{fmt.Errorf("unsupported multiple use of CTE clause %q", tn)})
			}
			cte.used = true

			outScope = inScope.push()

			// TODO(justin): once we support mutations here, we will want to include a
			// spool operation.
			outScope.expr = cte.expr
			outScope.cols = cte.cols
			return outScope
		}

		ds := b.resolveDataSource(tn, privilege.SELECT)
		switch t := ds.(type) {
		case opt.Table:
			return b.buildScan(t, tn, nil /* ordinals */, indexFlags, inScope)
		case opt.View:
			return b.buildView(t, inScope)
		default:
			panic(unimplementedf("sequences are not supported"))
		}

	case *tree.ParenTableExpr:
		return b.buildDataSource(source.Expr, indexFlags, inScope)

	case *tree.RowsFromExpr:
		return b.buildZip(source.Items, inScope)

	case *tree.Subquery:
		outScope = b.buildStmt(source.Select, inScope)

		// Treat the subquery result as an anonymous data source (i.e. column names
		// are not qualified). Remove hidden columns, as they are not accessible
		// outside the subquery.
		outScope.setTableAlias("")
		outScope.removeHiddenCols()

		return outScope

	case *tree.StatementSource:
		outScope = b.buildStmt(source.Statement, inScope)
		if len(outScope.cols) == 0 {
			panic(builderError{pgerror.NewErrorf(pgerror.CodeFeatureNotSupportedError,
				"statement source \"%v\" does not return any columns", source.Statement)})
		}
		return outScope

	case *tree.TableRef:
		ds := b.resolveDataSourceRef(source, privilege.SELECT)
		switch t := ds.(type) {
		case opt.Table:
			outScope = b.buildScanFromTableRef(t, source, indexFlags, inScope)
		default:
			panic(unimplementedf("view and sequence numeric refs are not supported"))
		}
		b.renameSource(source.As, outScope)
		return outScope

	default:
		panic(builderError{fmt.Errorf("unknown table expr: %T", texpr)})
	}
}

// buildView parses the view query text and builds it as a Select expression.
func (b *Builder) buildView(view opt.View, inScope *scope) (outScope *scope) {
	// Cache the AST so that multiple references won't need to reparse.
	if b.views == nil {
		b.views = make(map[opt.View]*tree.Select)
	}

	// Check whether view has already been parsed, and if not, parse now.
	sel, ok := b.views[view]
	if !ok {
		stmt, err := parser.ParseOne(view.Query())
		if err != nil {
			wrapped := errors.Wrapf(err, "failed to parse underlying query from view %q", view.Name())
			panic(builderError{wrapped})
		}

		sel, ok = stmt.(*tree.Select)
		if !ok {
			panic("expected SELECT statement")
		}

		b.views[view] = sel
	}

	// When building the view, we don't want to check for the SELECT privilege
	// on the underlying tables, just on the view itself. Checking on the
	// underlying tables as well would defeat the purpose of having separate
	// SELECT privileges on the view, which is intended to allow for exposing
	// some subset of a restricted table's data to less privileged users.
	if !b.skipSelectPrivilegeChecks {
		b.skipSelectPrivilegeChecks = true
		defer func() { b.skipSelectPrivilegeChecks = false }()
	}

	outScope = b.buildSelect(sel, nil /* desiredTypes */, &scope{builder: b})

	// Update data source name to be the name of the view. And if view columns
	// are specified, then update names of output columns.
	hasCols := view.ColumnNameCount() > 0
	for i := range outScope.cols {
		outScope.cols[i].table = *view.Name()
		if hasCols {
			outScope.cols[i].name = view.ColumnName(i)
		}
	}

	return outScope
}

// renameSource applies an AS clause to the columns in scope.
func (b *Builder) renameSource(as tree.AliasClause, scope *scope) {
	var tableAlias tree.TableName
	colAlias := as.Cols

	if as.Alias != "" {
		// Special case for Postgres compatibility: if a data source does not
		// currently have a name, and it is a set-generating function or a scalar
		// function with just one column, and the AS clause doesn't specify column
		// names, then use the specified table name both as the column name and
		// table name.
		noColNameSpecified := len(colAlias) == 0
		if scope.isAnonymousTable() && noColNameSpecified {
			// SRFs and scalar functions used as a data source are always wrapped in
			// a ProjectSet operation.
			if ps, ok := scope.expr.(*memo.ProjectSetExpr); ok && ps.Relational().OutputCols.Len() == 1 {
				colAlias = tree.NameList{as.Alias}
			}
		}

		// If an alias was specified, use that to qualify the column names.
		tableAlias = tree.MakeUnqualifiedTableName(as.Alias)
		scope.setTableAlias(as.Alias)
	}

	if len(colAlias) > 0 {
		// The column aliases can only refer to explicit columns.
		for colIdx, aliasIdx := 0, 0; aliasIdx < len(colAlias); colIdx++ {
			if colIdx >= len(scope.cols) {
				srcName := tree.ErrString(&tableAlias)
				panic(builderError{pgerror.NewErrorf(
					pgerror.CodeInvalidColumnReferenceError,
					"source %q has %d columns available but %d columns specified",
					srcName, aliasIdx, len(colAlias),
				)})
			}
			if scope.cols[colIdx].hidden {
				continue
			}
			scope.cols[colIdx].name = colAlias[aliasIdx]
			aliasIdx++
		}
	}
}

// buildScanFromTableRef adds support for numeric references in queries.
// For example:
// SELECT * FROM [53 as t]; (table reference)
// SELECT * FROM [53(1) as t]; (+columnar reference)
// SELECT * FROM [53(1) as t]@1; (+index reference)
// Note, the query SELECT * FROM [53() as t] is unsupported. Column lists must
// be non-empty
func (b *Builder) buildScanFromTableRef(
	tab opt.Table, ref *tree.TableRef, indexFlags *tree.IndexFlags, inScope *scope,
) (outScope *scope) {
	if ref.Columns != nil && len(ref.Columns) == 0 {
		panic(builderError{pgerror.NewErrorf(pgerror.CodeSyntaxError,
			"an explicit list of column IDs must include at least one column")})
	}

	// See tree.TableRef: "Note that a nil [Columns] array means 'unspecified'
	// (all columns). whereas an array of length 0 means 'zero columns'.
	// Lists of zero columns are not supported and will throw an error."
	// The error for lists of zero columns is thrown in the caller function
	// for buildScanFromTableRef.
	var ordinals []int
	if ref.Columns != nil {
		ordinals = make([]int, len(ref.Columns))
		for i, c := range ref.Columns {
			ord, err := tab.LookupColumnOrdinal(uint32(c))
			if err != nil {
				panic(builderError{err})
			}
			ordinals[i] = ord
		}
	}
	return b.buildScan(tab, tab.Name(), ordinals, indexFlags, inScope)
}

// buildScan builds a memo group for a ScanOp or VirtualScanOp expression on the
// given table with the given table name. If the ordinals slice is not nil, then
// only columns with ordinals in that list are projected by the scan. Otherwise,
// all columns from the table are projected.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildScan(
	tab opt.Table, tn *tree.TableName, ordinals []int, indexFlags *tree.IndexFlags, inScope *scope,
) (outScope *scope) {
	md := b.factory.Metadata()
	tabID := md.AddTable(tab)

	colCount := len(ordinals)
	if colCount == 0 {
		colCount = tab.ColumnCount()
	}

	var tabColIDs opt.ColSet
	outScope = inScope.push()
	outScope.cols = make([]scopeColumn, colCount)
	for i := 0; i < colCount; i++ {
		ord := i
		if ordinals != nil {
			ord = ordinals[i]
		}

		col := tab.Column(ord)
		colID := tabID.ColumnID(ord)
		tabColIDs.Add(int(colID))
		name := col.ColName()
		outScope.cols[i] = scopeColumn{
			id:     colID,
			name:   name,
			table:  *tn,
			typ:    col.DatumType(),
			hidden: col.IsHidden(),
		}
	}

	if tab.IsVirtualTable() {
		if indexFlags != nil {
			panic(builderError{errors.Errorf("index flags not allowed with virtual tables")})
		}
		private := memo.VirtualScanPrivate{Table: tabID, Cols: tabColIDs}
		outScope.expr = b.factory.ConstructVirtualScan(&private)
	} else {
		private := memo.ScanPrivate{Table: tabID, Cols: tabColIDs}

		if indexFlags != nil {
			private.Flags.NoIndexJoin = indexFlags.NoIndexJoin
			if indexFlags.Index != "" || indexFlags.IndexID != 0 {
				idx := -1
				for i := 0; i < tab.IndexCount(); i++ {
					if tab.Index(i).IdxName() == string(indexFlags.Index) ||
						tab.Index(i).InternalID() == uint64(indexFlags.IndexID) {
						idx = i
						break
					}
				}
				if idx == -1 {
					var err error
					if indexFlags.Index != "" {
						err = errors.Errorf("index %q not found", tree.ErrString(&indexFlags.Index))
					} else {
						err = errors.Errorf("index [%d] not found", indexFlags.IndexID)
					}
					panic(builderError{err})
				}
				private.Flags.ForceIndex = true
				private.Flags.Index = idx
			}
		}

		outScope.expr = b.factory.ConstructScan(&private)
	}
	return outScope
}

// buildWithOrdinality builds a group which appends an increasing integer column to
// the output. colName optionally denotes the name this column is given, or can
// be blank for none.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildWithOrdinality(colName string, inScope *scope) (outScope *scope) {
	col := b.synthesizeColumn(inScope, colName, types.Int, nil, nil /* scalar */)

	// See https://www.cockroachlabs.com/docs/stable/query-order.html#order-preservation
	// for the semantics around WITH ORDINALITY and ordering.

	input := inScope.expr.(memo.RelExpr)
	inScope.expr = b.factory.ConstructRowNumber(input, &memo.RowNumberPrivate{
		Ordering: inScope.makeOrderingChoice(),
		ColID:    col.id,
	})

	return inScope
}

func (b *Builder) buildCTE(ctes []*tree.CTE, inScope *scope) (outScope *scope) {
	outScope = inScope.push()

	outScope.ctes = make(map[string]*cteSource)
	for i := range ctes {
		cteScope := b.buildStmt(ctes[i].Stmt, outScope)
		cols := cteScope.cols
		name := ctes[i].Name.Alias

		if _, ok := outScope.ctes[name.String()]; ok {
			panic(builderError{
				fmt.Errorf("WITH query name %s specified more than once", ctes[i].Name.Alias),
			})
		}

		// Names for the output columns can optionally be specified.
		if ctes[i].Name.Cols != nil {
			if len(cteScope.cols) != len(ctes[i].Name.Cols) {
				panic(builderError{
					fmt.Errorf(
						"source %q has %d columns available but %d columns specified",
						name, len(cteScope.cols), len(ctes[i].Name.Cols),
					),
				})
			}

			cols = make([]scopeColumn, len(cteScope.cols))
			tableName := tree.MakeUnqualifiedTableName(ctes[i].Name.Alias)
			copy(cols, cteScope.cols)
			for j := range cols {
				cols[j].name = ctes[i].Name.Cols[j]
				cols[j].table = tableName
			}
		}

		if len(cols) == 0 {
			panic(builderError{pgerror.NewErrorf(pgerror.CodeFeatureNotSupportedError,
				"WITH clause %q does not have a RETURNING clause", tree.ErrString(&name))})
		}

		outScope.ctes[ctes[i].Name.Alias.String()] = &cteSource{
			name: ctes[i].Name,
			cols: cols,
			expr: cteScope.expr,
		}
	}

	return outScope
}

// checkCTEUsage ensures that a CTE that contains a mutation (like INSERT) is
// used at least once by the query. Otherwise, it might not be executed.
func (b *Builder) checkCTEUsage(inScope *scope) {
	for alias, source := range inScope.ctes {
		if !source.used && source.expr.Relational().CanMutate {
			panic(builderError{pgerror.UnimplementedWithIssueErrorf(24307,
				"common table expression %q with side effects was not used in query", alias)})
		}
	}
}

// buildSelect builds a set of memo groups that represent the given select
// statement.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildSelect(
	stmt *tree.Select, desiredTypes []types.T, inScope *scope,
) (outScope *scope) {
	wrapped := stmt.Select
	orderBy := stmt.OrderBy
	limit := stmt.Limit
	with := stmt.With

	for s, ok := wrapped.(*tree.ParenSelect); ok; s, ok = wrapped.(*tree.ParenSelect) {
		stmt = s.Select
		if stmt.With != nil {
			if with != nil {
				// (WITH ... (WITH ...))
				// Currently we are unable to nest the scopes inside ParenSelect so we
				// must refuse the syntax so that the query does not get invalid results.
				panic(builderError{pgerror.UnimplementedWithIssueError(24303,
					"multiple WITH clauses in parentheses")})
			}
			with = s.Select.With
		}
		wrapped = stmt.Select
		if stmt.OrderBy != nil {
			if orderBy != nil {
				panic(builderError{pgerror.NewErrorf(
					pgerror.CodeSyntaxError, "multiple ORDER BY clauses not allowed",
				)})
			}
			orderBy = stmt.OrderBy
		}
		if stmt.Limit != nil {
			if limit != nil {
				panic(builderError{pgerror.NewErrorf(
					pgerror.CodeSyntaxError, "multiple LIMIT clauses not allowed",
				)})
			}
			limit = stmt.Limit
		}
	}

	if with != nil {
		inScope = b.buildCTE(with.CTEList, inScope)
		defer b.checkCTEUsage(inScope)
	}

	// NB: The case statements are sorted lexicographically.
	switch t := stmt.Select.(type) {
	case *tree.SelectClause:
		outScope = b.buildSelectClause(t, orderBy, desiredTypes, inScope)

	case *tree.UnionClause:
		outScope = b.buildUnion(t, desiredTypes, inScope)

	case *tree.ValuesClause:
		outScope = b.buildValuesClause(t, desiredTypes, inScope)

	default:
		panic(fmt.Errorf("unknown select statement: %T", stmt.Select))
	}

	if outScope.ordering.Empty() && orderBy != nil {
		projectionsScope := outScope.replace()
		projectionsScope.cols = make([]scopeColumn, 0, len(outScope.cols))
		for i := range outScope.cols {
			expr := &outScope.cols[i]
			col := b.addColumn(projectionsScope, "" /* label */, expr)
			b.buildScalar(expr, outScope, projectionsScope, col, nil)
		}
		orderByScope := b.analyzeOrderBy(orderBy, outScope, projectionsScope)
		b.buildOrderBy(outScope, projectionsScope, orderByScope)
		b.constructProjectForScope(outScope, projectionsScope)
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
	sel *tree.SelectClause, orderBy tree.OrderBy, desiredTypes []types.T, inScope *scope,
) (outScope *scope) {
	fromScope := b.buildFrom(sel.From, sel.Where, inScope)
	projectionsScope := fromScope.replace()

	// This is where the magic happens. When this call reaches an aggregate
	// function that refers to variables in fromScope or an ancestor scope,
	// buildAggregateFunction is called which adds columns to the appropriate
	// aggInScope and aggOutScope.
	b.analyzeProjectionList(sel.Exprs, desiredTypes, fromScope, projectionsScope)

	// Any aggregates in the HAVING, ORDER BY and DISTINCT ON clauses (if they
	// exist) will be added here.
	havingExpr := b.analyzeHaving(sel.Having, fromScope)
	orderByScope := b.analyzeOrderBy(orderBy, fromScope, projectionsScope)
	distinctOnScope := b.analyzeDistinctOnArgs(sel.DistinctOn, fromScope, projectionsScope)

	if b.needsAggregation(sel, fromScope) {
		outScope = b.buildAggregation(
			sel, havingExpr, fromScope, projectionsScope, orderByScope, distinctOnScope,
		)
	} else {
		b.buildProjectionList(fromScope, projectionsScope)
		b.buildOrderBy(fromScope, projectionsScope, orderByScope)
		b.buildDistinctOnArgs(fromScope, projectionsScope, distinctOnScope)
		if len(fromScope.srfs) > 0 {
			fromScope.expr = b.constructProjectSet(fromScope.expr, fromScope.srfs)
		}
		outScope = fromScope
	}

	// Construct the projection.
	b.constructProjectForScope(outScope, projectionsScope)
	outScope = projectionsScope

	if sel.Distinct {
		if projectionsScope.distinctOnCols.Empty() {
			outScope.expr = b.constructDistinct(outScope)
		} else {
			outScope = b.buildDistinctOn(projectionsScope.distinctOnCols, outScope)
		}
	}
	return outScope
}

// buildFrom builds a set of memo groups that represent the given FROM statement
// and WHERE clause.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildFrom(from *tree.From, where *tree.Where, inScope *scope) (outScope *scope) {
	// The root AS OF clause is recognized and handled by the executor. The only
	// thing that must be done at this point is to ensure that if any timestamps
	// are specified, the root SELECT was an AS OF SYSTEM TIME and that the time
	// specified matches the one found at the root.
	if from.AsOf.Expr != nil {
		b.validateAsOf(from.AsOf)
	}

	if len(from.Tables) > 0 {
		outScope = b.buildFromTables(from.Tables, inScope)
	} else {
		outScope = inScope.push()
		outScope.expr = b.factory.ConstructValues(memo.ScalarListWithEmptyTuple, opt.ColList{})
	}

	if where != nil {
		// We need to save and restore the previous value of the field in
		// semaCtx in case we are recursively called within a subquery
		// context.
		defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
		b.semaCtx.Properties.Require("WHERE", tree.RejectSpecial)
		outScope.context = "WHERE"

		// All "from" columns are visible to the filter expression.
		texpr := outScope.resolveAndRequireType(where.Expr, types.Bool)

		filter := b.buildScalar(texpr, outScope, nil, nil, nil)

		// Wrap the filter in a FiltersOp.
		outScope.expr = b.factory.ConstructSelect(
			outScope.expr.(memo.RelExpr),
			memo.FiltersExpr{{Condition: filter}},
		)
	}

	return outScope
}

// buildFromTables recursively builds a series of InnerJoin expressions that
// join together the given FROM tables. The tables are joined in the reverse
// order that they appear in the list, with the innermost join involving the
// tables at the end of the list. For example:
//
//   SELECT * FROM a,b,c
//
// is joined like:
//
//   SELECT * FROM a JOIN (b JOIN c ON true) ON true
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildFromTables(tables tree.TableExprs, inScope *scope) (outScope *scope) {
	outScope = b.buildDataSource(tables[0], nil /* indexFlags */, inScope)

	// Recursively build table join.
	tables = tables[1:]
	if len(tables) == 0 {
		return outScope
	}
	tableScope := b.buildFromTables(tables, inScope)

	// Check that the same table name is not used multiple times.
	b.validateJoinTableNames(outScope, tableScope)

	outScope.appendColumnsFromScope(tableScope)

	left := outScope.expr.(memo.RelExpr)
	right := tableScope.expr.(memo.RelExpr)
	outScope.expr = b.factory.ConstructInnerJoin(left, right, memo.TrueFilter)
	return outScope
}

// validateAsOf ensures that any AS OF SYSTEM TIME timestamp is consistent with
// that of the root statement.
func (b *Builder) validateAsOf(asOf tree.AsOfClause) {
	ts, err := tree.EvalAsOfTimestamp(asOf, hlc.MaxTimestamp, b.semaCtx, b.evalCtx)
	if err != nil {
		panic(builderError{err})
	}

	if b.semaCtx.AsOfTimestamp == nil {
		panic(builderError{errors.Errorf("AS OF SYSTEM TIME must be provided on a top-level statement")})
	}

	if *b.semaCtx.AsOfTimestamp != ts {
		panic(builderError{errors.Errorf("cannot specify AS OF SYSTEM TIME with different timestamps")})
	}
}
