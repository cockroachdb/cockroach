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
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

const (
	excludeMutations = false
	includeMutations = true
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
			telemetry.Inc(sqltelemetry.IndexHintUseCounter)
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
			outScope = inScope.push()

			inCols := make(opt.ColList, len(cte.cols))
			outCols := make(opt.ColList, len(cte.cols))
			outScope.cols = nil
			i := 0
			for _, col := range cte.cols {
				id := col.id
				c := b.factory.Metadata().ColumnMeta(id)
				newCol := b.synthesizeColumn(outScope, string(col.name), c.Type, nil, nil)
				newCol.table = *tn
				inCols[i] = id
				outCols[i] = newCol.id
				i++
			}

			outScope.expr = b.factory.ConstructWithScan(&memo.WithScanPrivate{
				ID:           cte.id,
				Name:         string(cte.name.Alias),
				InCols:       inCols,
				OutCols:      outCols,
				BindingProps: cte.expr.Relational(),
			})
			return outScope
		}

		ds, resName := b.resolveDataSource(tn, privilege.SELECT)
		switch t := ds.(type) {
		case cat.Table:
			tabMeta := b.addTable(t, &resName)
			return b.buildScan(tabMeta, nil /* ordinals */, indexFlags, excludeMutations, inScope)
		case cat.View:
			return b.buildView(t, &resName, inScope)
		case cat.Sequence:
			return b.buildSequenceSelect(t, &resName, inScope)
		default:
			panic(errors.AssertionFailedf("unknown DataSource type %T", ds))
		}

	case *tree.ParenTableExpr:
		return b.buildDataSource(source.Expr, indexFlags, inScope)

	case *tree.RowsFromExpr:
		return b.buildZip(source.Items, inScope)

	case *tree.Subquery:
		outScope = b.buildStmt(source.Select, nil /* desiredTypes */, inScope)

		// Treat the subquery result as an anonymous data source (i.e. column names
		// are not qualified). Remove hidden columns, as they are not accessible
		// outside the subquery.
		outScope.setTableAlias("")
		outScope.removeHiddenCols()

		return outScope

	case *tree.StatementSource:
		outScope = b.buildStmt(source.Statement, nil /* desiredTypes */, inScope)
		if len(outScope.cols) == 0 {
			panic(pgerror.Newf(pgcode.UndefinedColumn,
				"statement source \"%v\" does not return any columns", source.Statement))
		}
		return outScope

	case *tree.TableRef:
		ds := b.resolveDataSourceRef(source, privilege.SELECT)
		switch t := ds.(type) {
		case cat.Table:
			outScope = b.buildScanFromTableRef(t, source, indexFlags, inScope)
		case cat.View:
			if source.Columns != nil {
				panic(pgerror.Newf(pgcode.FeatureNotSupported,
					"cannot specify an explicit column list when accessing a view by reference"))
			}
			tn := tree.MakeUnqualifiedTableName(t.Name())

			outScope = b.buildView(t, &tn, inScope)
		case cat.Sequence:
			tn := tree.MakeUnqualifiedTableName(t.Name())
			// Any explicitly listed columns are ignored.
			outScope = b.buildSequenceSelect(t, &tn, inScope)
		default:
			panic(errors.AssertionFailedf("unsupported catalog object"))
		}
		b.renameSource(source.As, outScope)
		return outScope

	default:
		panic(errors.AssertionFailedf("unknown table expr: %T", texpr))
	}
}

// buildView parses the view query text and builds it as a Select expression.
func (b *Builder) buildView(
	view cat.View, viewName *tree.TableName, inScope *scope,
) (outScope *scope) {
	// Cache the AST so that multiple references won't need to reparse.
	if b.views == nil {
		b.views = make(map[cat.View]*tree.Select)
	}

	// Check whether view has already been parsed, and if not, parse now.
	sel, ok := b.views[view]
	if !ok {
		stmt, err := parser.ParseOne(view.Query())
		if err != nil {
			wrapped := pgerror.Wrapf(err, pgcode.Syntax,
				"failed to parse underlying query from view %q", view.Name())
			panic(wrapped)
		}

		sel, ok = stmt.AST.(*tree.Select)
		if !ok {
			panic(errors.AssertionFailedf("expected SELECT statement"))
		}

		b.views[view] = sel

		// Keep track of referenced views for EXPLAIN (opt, env).
		b.factory.Metadata().AddView(view)
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
		outScope.cols[i].table = *viewName
		if hasCols {
			outScope.cols[i].name = view.ColumnName(i)
		}
	}

	return outScope
}

// renameSource applies an AS clause to the columns in scope.
func (b *Builder) renameSource(as tree.AliasClause, scope *scope) {
	if as.Alias != "" {
		colAlias := as.Cols

		// Special case for Postgres compatibility: if a data source does not
		// currently have a name, and it is a set-generating function or a scalar
		// function with just one column, and the AS clause doesn't specify column
		// names, then use the specified table name both as the column name and
		// table name.
		noColNameSpecified := len(colAlias) == 0
		if scope.isAnonymousTable() && noColNameSpecified && scope.singleSRFColumn {
			colAlias = tree.NameList{as.Alias}
		}

		// If an alias was specified, use that to qualify the column names.
		tableAlias := tree.MakeUnqualifiedTableName(as.Alias)
		scope.setTableAlias(as.Alias)

		// If input expression is a ScanExpr, then override metadata aliases for
		// pretty-printing.
		scan, isScan := scope.expr.(*memo.ScanExpr)
		if isScan {
			tabMeta := b.factory.Metadata().TableMeta(scan.ScanPrivate.Table)
			tabMeta.Alias = tree.MakeUnqualifiedTableName(as.Alias)
		}

		if len(colAlias) > 0 {
			// The column aliases can only refer to explicit columns.
			for colIdx, aliasIdx := 0, 0; aliasIdx < len(colAlias); colIdx++ {
				if colIdx >= len(scope.cols) {
					srcName := tree.ErrString(&tableAlias)
					panic(pgerror.Newf(
						pgcode.InvalidColumnReference,
						"source %q has %d columns available but %d columns specified",
						srcName, aliasIdx, len(colAlias),
					))
				}
				col := &scope.cols[colIdx]
				if col.hidden {
					continue
				}
				col.name = colAlias[aliasIdx]
				if isScan {
					// Override column metadata alias.
					colMeta := b.factory.Metadata().ColumnMeta(col.id)
					colMeta.Alias = string(colAlias[aliasIdx])
				}
				aliasIdx++
			}
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
	tab cat.Table, ref *tree.TableRef, indexFlags *tree.IndexFlags, inScope *scope,
) (outScope *scope) {
	if ref.Columns != nil && len(ref.Columns) == 0 {
		panic(pgerror.Newf(pgcode.Syntax,
			"an explicit list of column IDs must include at least one column"))
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
			ord := 0
			cnt := tab.ColumnCount()
			for ord < cnt {
				if tab.Column(ord).ColID() == cat.StableID(c) {
					break
				}
				ord++
			}
			if ord >= cnt {
				panic(pgerror.Newf(pgcode.UndefinedColumn,
					"column [%d] does not exist", c))
			}
			ordinals[i] = ord
		}
	}

	tn := tree.MakeUnqualifiedTableName(tab.Name())
	tabMeta := b.addTable(tab, &tn)
	return b.buildScan(tabMeta, ordinals, indexFlags, excludeMutations, inScope)
}

// addTable adds a table to the metadata and returns the TableMeta. The table
// name is passed separately in order to preserve knowledge of whether the
// catalog and schema names were explicitly specified.
func (b *Builder) addTable(tab cat.Table, alias *tree.TableName) *opt.TableMeta {
	md := b.factory.Metadata()
	tabID := md.AddTable(tab, alias)
	return md.TableMeta(tabID)
}

// buildScan builds a memo group for a ScanOp or VirtualScanOp expression on the
// given table.
//
// If the ordinals slice is not nil, then only columns with ordinals in that
// list are projected by the scan. Otherwise, all columns from the table are
// projected.
//
// See Builder.buildStmt for a description of the remaining input and return
// values.
func (b *Builder) buildScan(
	tabMeta *opt.TableMeta,
	ordinals []int,
	indexFlags *tree.IndexFlags,
	scanMutationCols bool,
	inScope *scope,
) (outScope *scope) {
	tab := tabMeta.Table
	tabID := tabMeta.MetaID

	if indexFlags != nil && indexFlags.IgnoreForeignKeys {
		tabMeta.IgnoreForeignKeys = true
	}

	colCount := len(ordinals)
	if colCount == 0 {
		// If scanning mutation columns, then include writable and deletable
		// columns in the output, in addition to public columns.
		if scanMutationCols {
			colCount = tab.DeletableColumnCount()
		} else {
			colCount = tab.ColumnCount()
		}
	}

	var tabColIDs opt.ColSet
	outScope = inScope.push()
	outScope.cols = make([]scopeColumn, 0, colCount)
	for i := 0; i < colCount; i++ {
		ord := i
		if ordinals != nil {
			ord = ordinals[i]
		}

		col := tab.Column(ord)
		colID := tabID.ColumnID(ord)
		tabColIDs.Add(colID)
		name := col.ColName()
		isMutation := cat.IsMutationColumn(tab, ord)
		outScope.cols = append(outScope.cols, scopeColumn{
			id:       colID,
			name:     name,
			table:    tabMeta.Alias,
			typ:      col.DatumType(),
			hidden:   col.IsHidden() || isMutation,
			mutation: isMutation,
		})
	}

	if tab.IsVirtualTable() {
		if indexFlags != nil {
			panic(pgerror.Newf(pgcode.Syntax,
				"index flags not allowed with virtual tables"))
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
					if tab.Index(i).Name() == tree.Name(indexFlags.Index) ||
						tab.Index(i).ID() == cat.StableID(indexFlags.IndexID) {
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
					panic(err)
				}
				private.Flags.ForceIndex = true
				private.Flags.Index = idx
				private.Flags.Direction = indexFlags.Direction
			}
		}
		outScope.expr = b.factory.ConstructScan(&private)
		b.addCheckConstraintsToScan(outScope, tabMeta)
	}
	return outScope
}

// addCheckConstraintsToScan finds all the check constraints that apply to the
// table and adds them to the table metadata. To do this, the scalar expression
// of the check constraints are built here.
func (b *Builder) addCheckConstraintsToScan(scope *scope, tabMeta *opt.TableMeta) {
	tab := tabMeta.Table
	// Find all the check constraints that apply to the table and add them
	// to the table meta data. To do this, we must build them into scalar
	// expressions.
	for i, n := 0, tab.CheckCount(); i < n; i++ {
		checkConstraint := tab.Check(i)

		// Only add validated check constraints to the table's metadata.
		if !checkConstraint.Validated {
			continue
		}
		expr, err := parser.ParseExpr(string(checkConstraint.Constraint))
		if err != nil {
			panic(err)
		}

		texpr := scope.resolveAndRequireType(expr, types.Bool)
		tabMeta.AddConstraint(b.buildScalar(texpr, scope, nil, nil, nil))
	}
}

func (b *Builder) buildSequenceSelect(
	seq cat.Sequence, seqName *tree.TableName, inScope *scope,
) (outScope *scope) {
	md := b.factory.Metadata()
	outScope = inScope.push()

	cols := make(opt.ColList, len(sqlbase.SequenceSelectColumns))

	for i, c := range sqlbase.SequenceSelectColumns {
		cols[i] = md.AddColumn(c.Name, c.Typ)
	}

	outScope.cols = make([]scopeColumn, 3)
	for i, c := range cols {
		col := md.ColumnMeta(c)
		outScope.cols[i] = scopeColumn{
			id:    c,
			name:  tree.Name(col.Alias),
			table: *seqName,
			typ:   col.Type,
		}
	}

	private := memo.SequenceSelectPrivate{
		Sequence: md.AddSequence(seq),
		Cols:     cols,
	}
	outScope.expr = b.factory.ConstructSequenceSelect(&private)
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
	inScope.expr = b.factory.ConstructOrdinality(input, &memo.OrdinalityPrivate{
		Ordering: inScope.makeOrderingChoice(),
		ColID:    col.id,
	})

	return inScope
}

func (b *Builder) buildCTE(
	ctes []*tree.CTE, inScope *scope,
) (outScope *scope, addedCTEs []cteSource) {
	outScope = inScope.push()

	start := len(b.ctes)

	outScope.ctes = make(map[string]*cteSource)
	for i := range ctes {
		cteScope := b.buildStmt(ctes[i].Stmt, nil /* desiredTypes */, outScope)
		cols := cteScope.cols
		name := ctes[i].Name.Alias

		// TODO(justin): lift this restriction when possible. WITH should be hoistable.
		if b.subquery != nil && !b.subquery.outerCols.Empty() {
			panic(pgerror.Newf(pgcode.FeatureNotSupported, "CTEs may not be correlated"))
		}

		if _, ok := outScope.ctes[name.String()]; ok {
			panic(pgerror.Newf(
				pgcode.DuplicateAlias,
				"WITH query name %s specified more than once", ctes[i].Name.Alias),
			)
		}

		// Names for the output columns can optionally be specified.
		if ctes[i].Name.Cols != nil {
			if len(cteScope.cols) != len(ctes[i].Name.Cols) {
				panic(pgerror.Newf(
					pgcode.InvalidColumnReference,
					"source %q has %d columns available but %d columns specified",
					name, len(cteScope.cols), len(ctes[i].Name.Cols),
				))
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
			panic(pgerror.Newf(pgcode.FeatureNotSupported,
				"WITH clause %q does not have a RETURNING clause", tree.ErrString(&name)))
		}

		projectionsScope := cteScope.replace()
		projectionsScope.appendColumnsFromScope(cteScope)
		b.constructProjectForScope(cteScope, projectionsScope)

		cteScope = projectionsScope

		id := b.factory.Memo().NextWithID()

		// No good way to show non-select expressions, like INSERT, here.
		var stmt tree.SelectStatement
		if sel, ok := ctes[i].Stmt.(*tree.Select); ok {
			stmt = sel.Select
		}

		b.ctes = append(b.ctes, cteSource{
			name:         ctes[i].Name,
			cols:         cols,
			originalExpr: stmt,
			expr:         cteScope.expr,
			id:           id,
		})
		cte := &b.ctes[len(b.ctes)-1]
		outScope.ctes[ctes[i].Name.Alias.String()] = cte
	}

	telemetry.Inc(sqltelemetry.CteUseCounter)

	return outScope, b.ctes[start:]
}

// wrapWithCTEs adds With expressions on top of an expression.
func (b *Builder) wrapWithCTEs(expr memo.RelExpr, ctes []cteSource) memo.RelExpr {
	// Since later CTEs can refer to earlier ones, we want to add these in
	// reverse order.
	for i := len(ctes) - 1; i >= 0; i-- {
		expr = b.factory.ConstructWith(
			ctes[i].expr,
			expr,
			&memo.WithPrivate{
				ID:           ctes[i].id,
				Name:         string(ctes[i].name.Alias),
				OriginalExpr: &tree.Subquery{Select: ctes[i].originalExpr},
			},
		)
	}
	return expr
}

// buildSelectStmt builds a set of memo groups that represent the given select
// statement.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildSelectStmt(
	stmt tree.SelectStatement, desiredTypes []*types.T, inScope *scope,
) (outScope *scope) {
	// NB: The case statements are sorted lexicographically.
	switch stmt := stmt.(type) {
	case *tree.ParenSelect:
		return b.buildSelect(stmt.Select, desiredTypes, inScope)

	case *tree.SelectClause:
		return b.buildSelectClause(stmt, nil /* orderBy */, desiredTypes, inScope)

	case *tree.UnionClause:
		return b.buildUnion(stmt, desiredTypes, inScope)

	case *tree.ValuesClause:
		return b.buildValuesClause(stmt, desiredTypes, inScope)

	default:
		panic(errors.AssertionFailedf("unknown select statement type: %T", stmt))
	}
}

// buildSelect builds a set of memo groups that represent the given select
// expression.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildSelect(
	stmt *tree.Select, desiredTypes []*types.T, inScope *scope,
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
				panic(unimplemented.NewWithIssue(24303, "multiple WITH clauses in parentheses"))
			}
			with = s.Select.With
		}
		wrapped = stmt.Select
		if stmt.OrderBy != nil {
			if orderBy != nil {
				panic(pgerror.Newf(
					pgcode.Syntax, "multiple ORDER BY clauses not allowed",
				))
			}
			orderBy = stmt.OrderBy
		}
		if stmt.Limit != nil {
			if limit != nil {
				panic(pgerror.Newf(
					pgcode.Syntax, "multiple LIMIT clauses not allowed",
				))
			}
			limit = stmt.Limit
		}
	}

	var ctes []cteSource
	if with != nil {
		inScope, ctes = b.buildCTE(with.CTEList, inScope)
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
		panic(pgerror.Newf(pgcode.FeatureNotSupported,
			"unknown select statement: %T", stmt.Select))
	}

	if outScope.ordering.Empty() && orderBy != nil {
		projectionsScope := outScope.replace()
		projectionsScope.cols = make([]scopeColumn, 0, len(outScope.cols))
		for i := range outScope.cols {
			expr := &outScope.cols[i]
			col := b.addColumn(projectionsScope, "" /* alias */, expr)
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

	outScope.expr = b.wrapWithCTEs(outScope.expr, ctes)

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
	sel *tree.SelectClause, orderBy tree.OrderBy, desiredTypes []*types.T, inScope *scope,
) (outScope *scope) {
	fromScope := b.buildFrom(sel.From, inScope)
	b.processWindowDefs(sel, fromScope)
	b.buildWhere(sel.Where, fromScope)

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

	var groupingCols []scopeColumn
	var having opt.ScalarExpr
	needsAgg := b.needsAggregation(sel, fromScope)
	if needsAgg {
		// Grouping columns must be built before building the projection list so
		// we can check that any column references that appear in the SELECT list
		// outside of aggregate functions are present in the grouping list.
		groupingCols = b.buildGroupingColumns(sel, fromScope)
		having = b.buildHaving(havingExpr, fromScope)
	}

	b.buildProjectionList(fromScope, projectionsScope)
	b.buildOrderBy(fromScope, projectionsScope, orderByScope)
	b.buildDistinctOnArgs(fromScope, projectionsScope, distinctOnScope)
	b.buildProjectSet(fromScope)

	if needsAgg {
		// We must wait to build the aggregation until after the above block since
		// any SRFs found in the SELECT list will change the FROM scope (they
		// create an implicit lateral join).
		outScope = b.buildAggregation(groupingCols, having, fromScope)
	} else {
		outScope = fromScope
	}

	b.buildWindow(outScope, fromScope)

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

// buildFrom builds a set of memo groups that represent the given FROM clause.
//
// See Builder.buildStmt for a description of the remaining input and return
// values.
func (b *Builder) buildFrom(from tree.From, inScope *scope) (outScope *scope) {
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
		outScope.expr = b.factory.ConstructValues(memo.ScalarListWithEmptyTuple, &memo.ValuesPrivate{
			Cols: opt.ColList{},
			ID:   b.factory.Metadata().NextValuesID(),
		})
	}

	return outScope
}

// processWindowDefs validates that any window defs have unique names and adds
// them to the given scope.
func (b *Builder) processWindowDefs(sel *tree.SelectClause, fromScope *scope) {
	// Just do an O(n^2) loop since the number of window defs is likely small.
	for i := range sel.Window {
		for j := i + 1; j < len(sel.Window); j++ {
			if sel.Window[i].Name == sel.Window[j].Name {
				panic(pgerror.Newf(
					pgcode.Windowing,
					"window %q is already defined",
					sel.Window[i].Name,
				))
			}
		}
	}

	// Pass down the set of window definitions so that they can be referenced
	// elsewhere in the SELECT.
	fromScope.windowDefs = sel.Window
}

// buildWhere builds a set of memo groups that represent the given WHERE clause.
//
// See Builder.buildStmt for a description of the remaining input and return
// values.
func (b *Builder) buildWhere(where *tree.Where, inScope *scope) {
	if where == nil {
		return
	}

	filter := b.resolveAndBuildScalar(where.Expr, types.Bool, "WHERE", tree.RejectSpecial, inScope)

	// Wrap the filter in a FiltersOp.
	inScope.expr = b.factory.ConstructSelect(
		inScope.expr.(memo.RelExpr),
		memo.FiltersExpr{{Condition: filter}},
	)
}

// buildFromTables builds a series of InnerJoin expressions that together
// represent the given FROM tables.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildFromTables(tables tree.TableExprs, inScope *scope) (outScope *scope) {
	// If there are any lateral data sources, we need to build the join tree
	// left-deep instead of right-deep.
	for i := range tables {
		if b.exprIsLateral(tables[i]) {
			return b.buildFromWithLateral(tables, inScope)
		}
	}
	return b.buildFromTablesRightDeep(tables, inScope)
}

// buildFromTablesRightDeep recursively builds a series of InnerJoin
// expressions that join together the given FROM tables. The tables are joined
// in the reverse order that they appear in the list, with the innermost join
// involving the tables at the end of the list. For example:
//
//   SELECT * FROM a,b,c
//
// is joined like:
//
//   SELECT * FROM a JOIN (b JOIN c ON true) ON true
//
// This ordering is guaranteed for queries not involving lateral joins for the
// time being, to ensure we don't break any queries which have been
// hand-optimized.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildFromTablesRightDeep(
	tables tree.TableExprs, inScope *scope,
) (outScope *scope) {
	outScope = b.buildDataSource(tables[0], nil /* indexFlags */, inScope)

	// Recursively build table join.
	tables = tables[1:]
	if len(tables) == 0 {
		return outScope
	}
	tableScope := b.buildFromTablesRightDeep(tables, inScope)

	// Check that the same table name is not used multiple times.
	b.validateJoinTableNames(outScope, tableScope)

	outScope.appendColumnsFromScope(tableScope)

	left := outScope.expr.(memo.RelExpr)
	right := tableScope.expr.(memo.RelExpr)
	outScope.expr = b.factory.ConstructInnerJoin(left, right, memo.TrueFilter, memo.EmptyJoinPrivate)
	return outScope
}

// exprIsLateral returns whether the table expression should have access to the
// scope of the tables to the left of it.
func (b *Builder) exprIsLateral(t tree.TableExpr) bool {
	ate, ok := t.(*tree.AliasedTableExpr)
	if !ok {
		return false
	}
	// Expressions which explicitly use the LATERAL keyword are lateral.
	if ate.Lateral {
		return true
	}
	// SRFs are always lateral.
	_, ok = ate.Expr.(*tree.RowsFromExpr)
	return ok
}

// buildFromWithLateral builds a FROM clause in the case where it contains a
// LATERAL table.  This differs from buildFromTablesRightDeep because the
// semantics of LATERAL require that the join tree is built left-deep (from
// left-to-right) rather than right-deep (from right-to-left) which we do
// typically for perf backwards-compatibility.
//
//   SELECT * FROM a, b, c
//
//   buildFromTablesRightDeep: a JOIN (b JOIN c)
//   buildFromWithLateral:     (a JOIN b) JOIN c
func (b *Builder) buildFromWithLateral(tables tree.TableExprs, inScope *scope) (outScope *scope) {
	outScope = b.buildDataSource(tables[0], nil /* indexFlags */, inScope)
	for i := 1; i < len(tables); i++ {
		scope := inScope
		// Lateral expressions need to be able to refer to the expressions that
		// have been built already.
		if b.exprIsLateral(tables[i]) {
			scope = outScope
		}
		tableScope := b.buildDataSource(tables[i], nil /* indexFlags */, scope)

		// Check that the same table name is not used multiple times.
		b.validateJoinTableNames(outScope, tableScope)

		outScope.appendColumnsFromScope(tableScope)

		left := outScope.expr.(memo.RelExpr)
		right := tableScope.expr.(memo.RelExpr)
		outScope.expr = b.factory.ConstructInnerJoinApply(left, right, memo.TrueFilter, memo.EmptyJoinPrivate)
	}

	return outScope
}

// validateAsOf ensures that any AS OF SYSTEM TIME timestamp is consistent with
// that of the root statement.
func (b *Builder) validateAsOf(asOf tree.AsOfClause) {
	ts, err := tree.EvalAsOfTimestamp(asOf, b.semaCtx, b.evalCtx)
	if err != nil {
		panic(err)
	}

	if b.semaCtx.AsOfTimestamp == nil {
		panic(pgerror.Newf(pgcode.Syntax,
			"AS OF SYSTEM TIME must be provided on a top-level statement"))
	}

	if *b.semaCtx.AsOfTimestamp != ts {
		panic(unimplementedWithIssueDetailf(35712, "",
			"cannot specify AS OF SYSTEM TIME with different timestamps"))
	}
}
