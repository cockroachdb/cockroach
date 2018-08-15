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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/pkg/errors"
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

	case *tree.NormalizableTableName:
		tn, err := source.Normalize()
		if err != nil {
			panic(builderError{err})
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
		panic(unimplementedf("not yet implemented: table expr: %T", texpr))
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

	outScope = b.buildSelect(sel, &scope{builder: b})

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
			// a Zip operation.
			ev := memo.MakeNormExprView(b.factory.Memo(), scope.group)
			if ev.Operator() == opt.ZipOp && ev.Logical().Relational.OutputCols.Len() == 1 {
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

	tabName := tree.AsStringWithFlags(tn, b.FmtFlags)
	tabID := md.AddTableWithName(tab, tabName)

	colCount := len(ordinals)
	if colCount == 0 {
		colCount = tab.ColumnCount()
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
		tabColIDs.Add(int(colID))
		name := tree.Name(col.ColName())
		outScope.cols = append(outScope.cols, scopeColumn{
			id:       colID,
			origName: name,
			name:     name,
			table:    *tn,
			typ:      col.DatumType(),
			hidden:   col.IsHidden(),
		})
	}

	if tab.IsVirtualTable() {
		if indexFlags != nil {
			panic(builderError{errors.Errorf("index flags not allowed with virtual tables")})
		}
		def := memo.VirtualScanOpDef{Table: tabID, Cols: tabColIDs}
		outScope.group = b.factory.ConstructVirtualScan(b.factory.InternVirtualScanOpDef(&def))
	} else {
		def := memo.ScanOpDef{Table: tabID, Cols: tabColIDs}

		if indexFlags != nil {
			def.Flags.NoIndexJoin = indexFlags.NoIndexJoin
			if indexFlags.Index != "" {
				idx := -1
				for i := 0; i < tab.IndexCount(); i++ {
					if tab.Index(i).IdxName() == string(indexFlags.Index) {
						idx = i
						break
					}
				}
				if idx == -1 {
					panic(builderError{errors.Errorf("index %q not found", tree.ErrString(&indexFlags.Index))})
				}
				def.Flags.ForceIndex = true
				def.Flags.Index = idx
			} else if indexFlags.IndexID != 0 {
				// TODO(radu): support IndexIDs.
				panic(unimplementedf("index IDs not supported in index flags"))
			}
		}

		outScope.group = b.factory.ConstructScan(b.factory.InternScanOpDef(&def))
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
	col := b.synthesizeColumn(inScope, colName, types.Int, nil, 0)

	// See https://www.cockroachlabs.com/docs/stable/query-order.html#order-preservation
	// for the semantics around WITH ORDINALITY and ordering.

	inScope.group = b.factory.ConstructRowNumber(
		inScope.group,
		b.factory.InternRowNumberDef(&memo.RowNumberDef{
			Ordering: inScope.makeOrderingChoice(),
			ColID:    col.id,
		}),
	)

	return inScope
}

// buildSelect builds a set of memo groups that represent the given select
// statement.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildSelect(stmt *tree.Select, inScope *scope) (outScope *scope) {
	if stmt.With != nil {
		panic(unimplementedf("with clause not supported"))
	}

	wrapped := stmt.Select
	orderBy := stmt.OrderBy
	limit := stmt.Limit

	for s, ok := wrapped.(*tree.ParenSelect); ok; s, ok = wrapped.(*tree.ParenSelect) {
		stmt = s.Select
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

	// NB: The case statements are sorted lexicographically.
	switch t := stmt.Select.(type) {
	case *tree.SelectClause:
		outScope = b.buildSelectClause(t, orderBy, inScope)

	case *tree.UnionClause:
		outScope = b.buildUnion(t, inScope)

	case *tree.ValuesClause:
		outScope = b.buildValuesClause(t, inScope)

	default:
		panic(fmt.Errorf("unknown select statement: %T", stmt.Select))
	}

	if outScope.ordering.Empty() && orderBy != nil {
		projectionsScope := outScope.replace()
		projectionsScope.cols = make([]scopeColumn, 0, len(outScope.cols))
		for i := range outScope.cols {
			b.buildScalarProjection(&outScope.cols[i], "", outScope, projectionsScope)
		}
		b.buildOrderBy(orderBy, outScope, projectionsScope)
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
	sel *tree.SelectClause, orderBy tree.OrderBy, inScope *scope,
) (outScope *scope) {
	fromScope := b.buildFrom(sel.From, sel.Where, inScope)

	var projectionsScope *scope
	if b.needsAggregation(sel, orderBy) {
		outScope, projectionsScope = b.buildAggregation(sel, orderBy, fromScope)
	} else {
		projectionsScope = fromScope.replace()
		b.buildProjectionList(sel.Exprs, fromScope, projectionsScope)
		b.buildOrderBy(orderBy, fromScope, projectionsScope)
		b.buildDistinctOnArgs(sel.DistinctOn, fromScope, projectionsScope)
		outScope = fromScope
	}

	if len(fromScope.srfs) > 0 {
		outScope.group = b.constructProjectSet(outScope.group, fromScope.srfs)
	}

	// Construct the projection.
	b.constructProjectForScope(outScope, projectionsScope)
	outScope = projectionsScope

	if sel.Distinct {
		if projectionsScope.distinctOnCols.Empty() {
			outScope.group = b.constructDistinct(outScope)
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

	var joinTables map[string]struct{}
	colsAdded := 0

	for _, table := range from.Tables {
		tableScope := b.buildDataSource(table, nil /* indexFlags */, inScope)

		if outScope == nil {
			outScope = tableScope
			continue
		}

		// Build a map of the table names in the join.
		if joinTables == nil {
			joinTables = make(map[string]struct{})
		}
		for _, col := range outScope.cols[colsAdded:] {
			joinTables[col.table.FQString()] = exists
		}
		colsAdded = len(outScope.cols)

		// Check that the same table name is not used multiple times.
		b.validateJoinTableNames(joinTables, tableScope)

		outScope.appendColumns(tableScope)
		outScope.group = b.factory.ConstructInnerJoin(
			outScope.group, tableScope.group, b.factory.ConstructTrue(),
		)
	}

	if outScope == nil {
		rows := []memo.GroupID{b.factory.ConstructTuple(
			b.factory.InternList(nil), b.factory.InternType(memo.EmptyTupleType),
		)}
		outScope = inScope.push()
		outScope.group = b.factory.ConstructValues(
			b.factory.InternList(rows),
			b.factory.InternColList(opt.ColList{}),
		)
	}

	if where != nil {
		// We need to save and restore the previous value of the field in
		// semaCtx in case we are recursively called within a subquery
		// context.
		defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
		b.semaCtx.Properties.Require("WHERE", tree.RejectSpecial)

		// All "from" columns are visible to the filter expression.
		texpr := outScope.resolveAndRequireType(where.Expr, types.Bool, "WHERE")

		filter := b.buildScalar(texpr, outScope)
		// Wrap the filter in a FiltersOp.
		filter = b.factory.ConstructFilters(b.factory.InternList([]memo.GroupID{filter}))
		outScope.group = b.factory.ConstructSelect(outScope.group, filter)
	}

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
