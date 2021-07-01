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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
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
	texpr tree.TableExpr, indexFlags *tree.IndexFlags, locking lockingSpec, inScope *scope,
) (outScope *scope) {
	defer func(prevAtRoot bool) {
		inScope.atRoot = prevAtRoot
	}(inScope.atRoot)
	inScope.atRoot = false
	// NB: The case statements are sorted lexicographically.
	switch source := texpr.(type) {
	case *tree.AliasedTableExpr:
		if source.IndexFlags != nil {
			telemetry.Inc(sqltelemetry.IndexHintUseCounter)
			telemetry.Inc(sqltelemetry.IndexHintSelectUseCounter)
			indexFlags = source.IndexFlags
		}
		if source.As.Alias != "" {
			locking = locking.filter(source.As.Alias)
		}

		outScope = b.buildDataSource(source.Expr, indexFlags, locking, inScope)

		if source.Ordinality {
			outScope = b.buildWithOrdinality(outScope)
		}

		// Overwrite output properties with any alias information.
		b.renameSource(source.As, outScope)

		return outScope

	case *tree.JoinTableExpr:
		return b.buildJoin(source, locking, inScope)

	case *tree.TableName:
		tn := source

		// CTEs take precedence over other data sources.
		if cte := inScope.resolveCTE(tn); cte != nil {
			locking.ignoreLockingForCTE()
			outScope = inScope.push()
			inCols := make(opt.ColList, len(cte.cols))
			outCols := make(opt.ColList, len(cte.cols))
			outScope.cols = nil
			for i, col := range cte.cols {
				id := col.ID
				c := b.factory.Metadata().ColumnMeta(id)
				newCol := b.synthesizeColumn(outScope, scopeColName(tree.Name(col.Alias)), c.Type, nil, nil)
				newCol.table = *tn
				inCols[i] = id
				outCols[i] = newCol.id
			}

			outScope.expr = b.factory.ConstructWithScan(&memo.WithScanPrivate{
				With:    cte.id,
				Name:    string(cte.name.Alias),
				InCols:  inCols,
				OutCols: outCols,
				ID:      b.factory.Metadata().NextUniqueID(),
			})

			return outScope
		}

		ds, depName, resName := b.resolveDataSource(tn, privilege.SELECT)

		locking = locking.filter(tn.ObjectName)
		if locking.isSet() {
			// SELECT ... FOR [KEY] UPDATE/SHARE also requires UPDATE privileges.
			b.checkPrivilege(depName, ds, privilege.UPDATE)
		}

		switch t := ds.(type) {
		case cat.Table:
			tabMeta := b.addTable(t, &resName)
			return b.buildScan(
				tabMeta,
				tableOrdinals(t, columnKinds{
					includeMutations:       false,
					includeSystem:          true,
					includeVirtualInverted: false,
					includeVirtualComputed: true,
				}),
				indexFlags, locking, inScope,
			)

		case cat.Sequence:
			return b.buildSequenceSelect(t, &resName, inScope)

		case cat.View:
			return b.buildView(t, &resName, locking, inScope)

		default:
			panic(errors.AssertionFailedf("unknown DataSource type %T", ds))
		}

	case *tree.ParenTableExpr:
		return b.buildDataSource(source.Expr, indexFlags, locking, inScope)

	case *tree.RowsFromExpr:
		return b.buildZip(source.Items, inScope)

	case *tree.Subquery:
		// Remove any target relations from the current scope's locking spec, as
		// those only apply to relations in this statement. Interestingly, this
		// would not be necessary if we required all subqueries to have aliases
		// like Postgres does.
		locking = locking.withoutTargets()

		outScope = b.buildSelectStmt(source.Select, locking, nil /* desiredTypes */, inScope)

		// Treat the subquery result as an anonymous data source (i.e. column names
		// are not qualified). Remove hidden columns, as they are not accessible
		// outside the subquery.
		outScope.setTableAlias("")
		outScope.removeHiddenCols()

		return outScope

	case *tree.StatementSource:
		// This is the special '[ ... ]' syntax. We treat this as syntactic sugar
		// for a top-level CTE, so it cannot refer to anything in the input scope.
		// See #41078.
		emptyScope := b.allocScope()
		innerScope := b.buildStmt(source.Statement, nil /* desiredTypes */, emptyScope)
		if len(innerScope.cols) == 0 {
			panic(pgerror.Newf(pgcode.UndefinedColumn,
				"statement source \"%v\" does not return any columns", source.Statement))
		}

		id := b.factory.Memo().NextWithID()
		b.factory.Metadata().AddWithBinding(id, innerScope.expr)
		cte := &cteSource{
			name:         tree.AliasClause{},
			cols:         innerScope.makePresentationWithHiddenCols(),
			originalExpr: source.Statement,
			expr:         innerScope.expr,
			id:           id,
		}
		b.addCTE(cte)

		inCols := make(opt.ColList, len(cte.cols))
		outCols := make(opt.ColList, len(cte.cols))
		for i, col := range cte.cols {
			id := col.ID
			c := b.factory.Metadata().ColumnMeta(id)
			inCols[i] = id
			outCols[i] = b.factory.Metadata().AddColumn(col.Alias, c.Type)
		}

		locking.ignoreLockingForCTE()
		outScope = inScope.push()
		// Similar to appendColumnsFromScope, but with re-numbering the column IDs.
		for i, col := range innerScope.cols {
			col.scalar = nil
			col.id = outCols[i]
			outScope.cols = append(outScope.cols, col)
		}

		outScope.expr = b.factory.ConstructWithScan(&memo.WithScanPrivate{
			With:    cte.id,
			Name:    string(cte.name.Alias),
			InCols:  inCols,
			OutCols: outCols,
			ID:      b.factory.Metadata().NextUniqueID(),
		})

		return outScope

	case *tree.TableRef:
		ds, depName := b.resolveDataSourceRef(source, privilege.SELECT)

		locking = locking.filter(source.As.Alias)
		if locking.isSet() {
			// SELECT ... FOR [KEY] UPDATE/SHARE also requires UPDATE privileges.
			b.checkPrivilege(depName, ds, privilege.UPDATE)
		}

		switch t := ds.(type) {
		case cat.Table:
			outScope = b.buildScanFromTableRef(t, source, indexFlags, locking, inScope)
		case cat.View:
			if source.Columns != nil {
				panic(pgerror.Newf(pgcode.FeatureNotSupported,
					"cannot specify an explicit column list when accessing a view by reference"))
			}
			tn := tree.MakeUnqualifiedTableName(t.Name())

			outScope = b.buildView(t, &tn, locking, inScope)
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
	view cat.View, viewName *tree.TableName, locking lockingSpec, inScope *scope,
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
	trackDeps := b.trackViewDeps
	if trackDeps {
		// We are only interested in the direct dependency on this view descriptor.
		// Any further dependency by the view's query should not be tracked.
		b.trackViewDeps = false
		defer func() { b.trackViewDeps = true }()
	}

	// We don't want the view to be able to refer to any outer scopes in the
	// query. This shouldn't happen if the view is valid but there may be
	// cornercases (e.g. renaming tables referenced by the view). To be safe, we
	// build the view with an empty scope. But after that, we reattach the scope
	// to the existing scope chain because we want the rest of the query to be
	// able to refer to the higher scopes (see #46180).
	emptyScope := b.allocScope()
	outScope = b.buildSelect(sel, locking, nil /* desiredTypes */, emptyScope)
	emptyScope.parent = inScope

	// Update data source name to be the name of the view. And if view columns
	// are specified, then update names of output columns.
	hasCols := view.ColumnNameCount() > 0
	for i := range outScope.cols {
		outScope.cols[i].table = *viewName
		if hasCols {
			outScope.cols[i].name = scopeColName(view.ColumnName(i))
		}
	}

	if trackDeps && !view.IsSystemView() {
		dep := opt.ViewDep{DataSource: view}
		for i := range outScope.cols {
			dep.ColumnOrdinals.Add(i)
		}
		b.viewDeps = append(b.viewDeps, dep)
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
				if col.visibility != visible {
					continue
				}
				col.name = scopeColName(colAlias[aliasIdx])
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
	tab cat.Table,
	ref *tree.TableRef,
	indexFlags *tree.IndexFlags,
	locking lockingSpec,
	inScope *scope,
) (outScope *scope) {
	var ordinals []int
	if ref.Columns != nil {
		// See tree.TableRef: "Note that a nil [Columns] array means 'unspecified'
		// (all columns). whereas an array of length 0 means 'zero columns'.
		// Lists of zero columns are not supported and will throw an error."
		if len(ref.Columns) == 0 {
			panic(pgerror.Newf(pgcode.Syntax,
				"an explicit list of column IDs must include at least one column"))
		}
		ordinals = resolveNumericColumnRefs(tab, ref.Columns)
	} else {
		ordinals = tableOrdinals(tab, columnKinds{
			includeMutations:       false,
			includeSystem:          true,
			includeVirtualInverted: false,
			includeVirtualComputed: true,
		})
	}

	tn := tree.MakeUnqualifiedTableName(tab.Name())
	tabMeta := b.addTable(tab, &tn)
	return b.buildScan(tabMeta, ordinals, indexFlags, locking, inScope)
}

// addTable adds a table to the metadata and returns the TableMeta. The table
// name is passed separately in order to preserve knowledge of whether the
// catalog and schema names were explicitly specified.
func (b *Builder) addTable(tab cat.Table, alias *tree.TableName) *opt.TableMeta {
	md := b.factory.Metadata()
	tabID := md.AddTable(tab, alias)
	return md.TableMeta(tabID)
}

// buildScan builds a memo group for a ScanOp expression on the given table. If
// the ordinals list contains any VirtualComputed columns, a ProjectOp is built
// on top.
//
// The resulting scope and expression output the given table ordinals. If an
// ordinal is for a VirtualComputed column, the ordinals it depends on must also
// be in the list (in practice, this coincides with all "ordinary" table columns
// being in the list).
//
// If scanMutationCols is true, then include columns being added or dropped from
// the table. These are currently required by the execution engine as "fetch
// columns", when performing mutation DML statements (INSERT, UPDATE, UPSERT,
// DELETE).
//
// NOTE: Callers must take care that mutation columns (columns that are being
//       added or dropped from the table) are only used when performing mutation
//       DML statements (INSERT, UPDATE, UPSERT, DELETE). They cannot be used in
//       any other way because they may not have been initialized yet by the
//       backfiller!
//
// See Builder.buildStmt for a description of the remaining input and return
// values.
func (b *Builder) buildScan(
	tabMeta *opt.TableMeta,
	ordinals []int,
	indexFlags *tree.IndexFlags,
	locking lockingSpec,
	inScope *scope,
) (outScope *scope) {
	if ordinals == nil {
		panic(errors.AssertionFailedf("no ordinals"))
	}
	tab := tabMeta.Table
	tabID := tabMeta.MetaID

	if indexFlags != nil {
		if indexFlags.IgnoreForeignKeys {
			tabMeta.IgnoreForeignKeys = true
		}
		if indexFlags.IgnoreUniqueWithoutIndexKeys {
			tabMeta.IgnoreUniqueWithoutIndexKeys = true
		}
	}

	outScope = inScope.push()

	// We collect VirtualComputed columns separately; these cannot be scanned,
	// they can only be projected afterward.
	var scanColIDs, virtualColIDs opt.ColSet
	outScope.cols = make([]scopeColumn, len(ordinals))
	for i, ord := range ordinals {
		col := tab.Column(ord)
		colID := tabID.ColumnID(ord)
		name := col.ColName()
		if col.IsVirtualComputed() {
			virtualColIDs.Add(colID)
		} else {
			scanColIDs.Add(colID)
		}
		kind := col.Kind()
		outScope.cols[i] = scopeColumn{
			id:           colID,
			name:         scopeColName(name),
			table:        tabMeta.Alias,
			typ:          col.DatumType(),
			visibility:   columnVisibility(col.Visibility()),
			kind:         kind,
			mutation:     kind == cat.WriteOnly || kind == cat.DeleteOnly,
			tableOrdinal: ord,
		}
	}

	if tab.IsVirtualTable() {
		if indexFlags != nil {
			panic(pgerror.Newf(pgcode.Syntax,
				"index flags not allowed with virtual tables"))
		}
		if locking.isSet() {
			panic(pgerror.Newf(pgcode.Syntax,
				"%s not allowed with virtual tables", locking.get().Strength))
		}
		private := memo.ScanPrivate{Table: tabID, Cols: scanColIDs}
		outScope.expr = b.factory.ConstructScan(&private)

		// Note: virtual tables should not be collected as view dependencies.
		return outScope
	}

	private := memo.ScanPrivate{Table: tabID, Cols: scanColIDs}
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
					err = pgerror.Newf(pgcode.UndefinedObject,
						"index %q not found", tree.ErrString(&indexFlags.Index))
				} else {
					err = pgerror.Newf(pgcode.UndefinedObject,
						"index [%d] not found", indexFlags.IndexID)
				}
				panic(err)
			}
			private.Flags.ForceIndex = true
			private.Flags.Index = idx
			private.Flags.Direction = indexFlags.Direction
		}
	}
	if locking.isSet() {
		private.Locking = locking.get()
	}

	b.addCheckConstraintsForTable(tabMeta)
	b.addComputedColsForTable(tabMeta)

	outScope.expr = b.factory.ConstructScan(&private)

	// Add the partial indexes after constructing the scan so we can use the
	// logical properties of the scan to fully normalize the index predicates.
	// We don't need to add deletable partial index predicates in the context of
	// a scan.
	b.addPartialIndexPredicatesForTable(tabMeta, outScope.expr)

	if !virtualColIDs.Empty() {
		// Project the expressions for the virtual columns (and pass through all
		// scanned columns).
		// TODO(radu): we don't currently support virtual columns depending on other
		// virtual columns.
		proj := make(memo.ProjectionsExpr, 0, virtualColIDs.Len())
		virtualColIDs.ForEach(func(col opt.ColumnID) {
			item := b.factory.ConstructProjectionsItem(tabMeta.ComputedCols[col], col)
			if !item.ScalarProps().OuterCols.SubsetOf(scanColIDs) {
				panic(errors.AssertionFailedf("scanned virtual column depends on non-scanned column"))
			}
			proj = append(proj, item)
		})
		outScope.expr = b.factory.ConstructProject(outScope.expr, proj, scanColIDs)
	}

	if b.trackViewDeps {
		dep := opt.ViewDep{DataSource: tab}
		dep.ColumnIDToOrd = make(map[opt.ColumnID]int)
		// We will track the ColumnID to Ord mapping so Ords can be added
		// when a column is referenced.
		for i, col := range outScope.cols {
			dep.ColumnIDToOrd[col.id] = ordinals[i]
		}
		if private.Flags.ForceIndex {
			dep.SpecificIndex = true
			dep.Index = private.Flags.Index
		}
		b.viewDeps = append(b.viewDeps, dep)
	}
	return outScope
}

// addCheckConstraintsForTable extracts filters from the check constraints that
// apply to the table and adds them to the table metadata (see
// TableMeta.Constraints). To do this, the scalar expressions of the check
// constraints are built here.
//
// These expressions are used as "known truths" about table data; as such they
// can only contain immutable operators.
func (b *Builder) addCheckConstraintsForTable(tabMeta *opt.TableMeta) {
	// Columns of a user defined type have a constraint to ensure
	// enum values for that column belong to the UDT. We do not want to
	// track view deps here, or else a view depending on a table with a
	// column that is a UDT will result in a type dependency being added
	// between the view and the UDT, even if the view does not use that column.
	if b.trackViewDeps {
		b.trackViewDeps = false
		defer func() {
			b.trackViewDeps = true
		}()
	}
	tab := tabMeta.Table

	// Check if we have any validated check constraints. Only validated
	// constraints are known to hold on existing table data.
	numChecks := tab.CheckCount()
	chkIdx := 0
	for ; chkIdx < numChecks; chkIdx++ {
		if tab.Check(chkIdx).Validated {
			break
		}
	}
	if chkIdx == numChecks {
		return
	}

	// Create a scope that can be used for building the scalar expressions.
	tableScope := b.allocScope()
	tableScope.appendOrdinaryColumnsFromTable(tabMeta, &tabMeta.Alias)

	// Find the non-nullable table columns. Mutation columns can be NULL during
	// backfill, so they should be excluded.
	var notNullCols opt.ColSet
	for i, n := 0, tab.ColumnCount(); i < n; i++ {
		if col := tab.Column(i); !col.IsNullable() && !col.IsMutation() {
			notNullCols.Add(tabMeta.MetaID.ColumnID(i))
		}
	}

	var filters memo.FiltersExpr
	// Skip to the first validated constraint we found above.
	for ; chkIdx < numChecks; chkIdx++ {
		checkConstraint := tab.Check(chkIdx)

		// Only add validated check constraints to the table's metadata.
		if !checkConstraint.Validated {
			continue
		}
		expr, err := parser.ParseExpr(checkConstraint.Constraint)
		if err != nil {
			panic(err)
		}

		texpr := tableScope.resolveAndRequireType(expr, types.Bool)
		var condition opt.ScalarExpr
		b.factory.FoldingControl().TemporarilyDisallowStableFolds(func() {
			condition = b.buildScalar(texpr, tableScope, nil, nil, nil)
		})
		// Check constraints that are guaranteed to not evaluate to NULL
		// are the only ones converted into filters. This is because a NULL
		// constraint is interpreted as passing, whereas a NULL filter is not.
		if memo.ExprIsNeverNull(condition, notNullCols) {
			// Check if the expression contains non-immutable operators.
			var sharedProps props.Shared
			memo.BuildSharedProps(condition, &sharedProps)
			if !sharedProps.VolatilitySet.HasStable() && !sharedProps.VolatilitySet.HasVolatile() {
				filters = append(filters, b.factory.ConstructFiltersItem(condition))
			}
		}
	}
	if len(filters) > 0 {
		tabMeta.SetConstraints(&filters)
	}
}

// addComputedColsForTable finds all computed columns in the given table and
// caches them in the table metadata as scalar expressions. These expressions
// are used as "known truths" about table data. Any columns for which the
// expression contains non-immutable operators are omitted.
func (b *Builder) addComputedColsForTable(tabMeta *opt.TableMeta) {
	// We do not want to track view deps here, otherwise a view depending
	// on a table with a computed column of a UDT will result in a
	// type dependency being added between the view and the UDT,
	// even if the view does not use that column.
	if b.trackViewDeps {
		b.trackViewDeps = false
		defer func() {
			b.trackViewDeps = true
		}()
	}
	var tableScope *scope
	tab := tabMeta.Table
	for i, n := 0, tab.ColumnCount(); i < n; i++ {
		tabCol := tab.Column(i)
		if !tabCol.IsComputed() {
			continue
		}
		if tabCol.IsMutation() {
			// Mutation columns can be NULL during backfill, so they won't equal the
			// computed column expression value (in general).
			continue
		}
		expr, err := parser.ParseExpr(tabCol.ComputedExprStr())
		if err != nil {
			panic(err)
		}

		if tableScope == nil {
			tableScope = b.allocScope()
			tableScope.appendOrdinaryColumnsFromTable(tabMeta, &tabMeta.Alias)
		}

		if texpr := tableScope.resolveAndRequireType(expr, tabCol.DatumType()); texpr != nil {
			colID := tabMeta.MetaID.ColumnID(i)
			var scalar opt.ScalarExpr
			b.factory.FoldingControl().TemporarilyDisallowStableFolds(func() {
				scalar = b.buildScalar(texpr, tableScope, nil, nil, nil)
			})
			// Check if the expression contains non-immutable operators.
			var sharedProps props.Shared
			memo.BuildSharedProps(scalar, &sharedProps)
			if !sharedProps.VolatilitySet.HasStable() && !sharedProps.VolatilitySet.HasVolatile() {
				tabMeta.AddComputedCol(colID, scalar)
			}
		}
	}
}

func (b *Builder) buildSequenceSelect(
	seq cat.Sequence, seqName *tree.TableName, inScope *scope,
) (outScope *scope) {
	md := b.factory.Metadata()
	outScope = inScope.push()

	cols := make(opt.ColList, len(colinfo.SequenceSelectColumns))

	for i, c := range colinfo.SequenceSelectColumns {
		cols[i] = md.AddColumn(c.Name, c.Typ)
	}

	outScope.cols = make([]scopeColumn, 3)
	for i, c := range cols {
		col := md.ColumnMeta(c)
		outScope.cols[i] = scopeColumn{
			id:    c,
			name:  scopeColName(tree.Name(col.Alias)),
			table: *seqName,
			typ:   col.Type,
		}
	}

	private := memo.SequenceSelectPrivate{
		Sequence: md.AddSequence(seq),
		Cols:     cols,
	}
	outScope.expr = b.factory.ConstructSequenceSelect(&private)

	if b.trackViewDeps {
		b.viewDeps = append(b.viewDeps, opt.ViewDep{DataSource: seq})
	}
	return outScope
}

// buildWithOrdinality builds a group which appends an increasing integer column
// to the output.
//
// See Builder.buildStmt for a description of the remaining input and return
// values.
func (b *Builder) buildWithOrdinality(inScope *scope) (outScope *scope) {
	col := b.synthesizeColumn(inScope, scopeColName("ordinality"), types.Int, nil, nil /* scalar */)

	// See https://www.cockroachlabs.com/docs/stable/query-order.html#order-preservation
	// for the semantics around WITH ORDINALITY and ordering.

	input := inScope.expr.(memo.RelExpr)
	inScope.expr = b.factory.ConstructOrdinality(input, &memo.OrdinalityPrivate{
		Ordering: inScope.makeOrderingChoice(),
		ColID:    col.id,
	})

	return inScope
}

// buildSelectStmt builds a set of memo groups that represent the given select
// statement.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildSelectStmt(
	stmt tree.SelectStatement, locking lockingSpec, desiredTypes []*types.T, inScope *scope,
) (outScope *scope) {
	// NB: The case statements are sorted lexicographically.
	switch stmt := stmt.(type) {
	case *tree.ParenSelect:
		return b.buildSelect(stmt.Select, locking, desiredTypes, inScope)

	case *tree.SelectClause:
		return b.buildSelectClause(stmt, nil /* orderBy */, locking, desiredTypes, inScope)

	case *tree.UnionClause:
		return b.buildUnionClause(stmt, desiredTypes, inScope)

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
	stmt *tree.Select, locking lockingSpec, desiredTypes []*types.T, inScope *scope,
) (outScope *scope) {
	wrapped := stmt.Select
	with := stmt.With
	orderBy := stmt.OrderBy
	limit := stmt.Limit
	locking.apply(stmt.Locking)

	for s, ok := wrapped.(*tree.ParenSelect); ok; s, ok = wrapped.(*tree.ParenSelect) {
		stmt = s.Select
		wrapped = stmt.Select
		if stmt.With != nil {
			if with != nil {
				// (WITH ... (WITH ...))
				// Currently we are unable to nest the scopes inside ParenSelect so we
				// must refuse the syntax so that the query does not get invalid results.
				panic(unimplemented.NewWithIssue(
					24303, "multiple WITH clauses in parentheses",
				))
			}
			with = s.Select.With
		}
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
		if stmt.Locking != nil {
			locking.apply(stmt.Locking)
		}
	}

	return b.processWiths(with, inScope, func(inScope *scope) *scope {
		return b.buildSelectStmtWithoutParens(
			wrapped, orderBy, limit, locking, desiredTypes, inScope,
		)
	})
}

// buildSelectStmtWithoutParens builds a set of memo groups that represent
// the given select statement components. The wrapped select statement can
// be any variant except ParenSelect, which should be unwrapped by callers.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildSelectStmtWithoutParens(
	wrapped tree.SelectStatement,
	orderBy tree.OrderBy,
	limit *tree.Limit,
	locking lockingSpec,
	desiredTypes []*types.T,
	inScope *scope,
) (outScope *scope) {
	// NB: The case statements are sorted lexicographically.
	switch t := wrapped.(type) {
	case *tree.ParenSelect:
		panic(errors.AssertionFailedf(
			"%T in buildSelectStmtWithoutParens", wrapped))

	case *tree.SelectClause:
		outScope = b.buildSelectClause(t, orderBy, locking, desiredTypes, inScope)

	case *tree.UnionClause:
		b.rejectIfLocking(locking, "UNION/INTERSECT/EXCEPT")
		outScope = b.buildUnionClause(t, desiredTypes, inScope)

	case *tree.ValuesClause:
		b.rejectIfLocking(locking, "VALUES")
		outScope = b.buildValuesClause(t, desiredTypes, inScope)

	default:
		panic(pgerror.Newf(pgcode.FeatureNotSupported,
			"unknown select statement: %T", wrapped))
	}

	if outScope.ordering.Empty() && orderBy != nil {
		projectionsScope := outScope.replace()
		projectionsScope.cols = make([]scopeColumn, 0, len(outScope.cols))
		for i := range outScope.cols {
			expr := &outScope.cols[i]
			col := projectionsScope.addColumn(scopeColName(""), expr)
			b.buildScalar(expr, outScope, projectionsScope, col, nil)
		}
		orderByScope := b.analyzeOrderBy(orderBy, outScope, projectionsScope, tree.RejectGenerators|tree.RejectAggregates|tree.RejectWindowApplications)
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
	sel *tree.SelectClause,
	orderBy tree.OrderBy,
	locking lockingSpec,
	desiredTypes []*types.T,
	inScope *scope,
) (outScope *scope) {
	fromScope := b.buildFrom(sel.From, locking, inScope)

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
	orderByScope := b.analyzeOrderBy(orderBy, fromScope, projectionsScope, tree.RejectGenerators)
	distinctOnScope := b.analyzeDistinctOnArgs(sel.DistinctOn, fromScope, projectionsScope)

	var having opt.ScalarExpr
	needsAgg := b.needsAggregation(sel, fromScope)
	if needsAgg {
		// Grouping columns must be built before building the projection list so
		// we can check that any column references that appear in the SELECT list
		// outside of aggregate functions are present in the grouping list.
		b.buildGroupingColumns(sel, projectionsScope, fromScope)
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
		outScope = b.buildAggregation(having, fromScope)
	} else {
		outScope = fromScope
	}

	b.buildWindow(outScope, fromScope)
	b.validateLockingInFrom(sel, locking, fromScope)

	// Construct the projection.
	b.constructProjectForScope(outScope, projectionsScope)
	outScope = projectionsScope

	if sel.Distinct {
		if projectionsScope.distinctOnCols.Empty() {
			outScope.expr = b.constructDistinct(outScope)
		} else {
			outScope = b.buildDistinctOn(
				projectionsScope.distinctOnCols,
				outScope,
				false, /* nullsAreDistinct */
				"",    /* errorOnDup */
			)
		}
	}
	return outScope
}

// buildFrom builds a set of memo groups that represent the given FROM clause.
//
// See Builder.buildStmt for a description of the remaining input and return
// values.
func (b *Builder) buildFrom(from tree.From, locking lockingSpec, inScope *scope) (outScope *scope) {
	// The root AS OF clause is recognized and handled by the executor. The only
	// thing that must be done at this point is to ensure that if any timestamps
	// are specified, the root SELECT was an AS OF SYSTEM TIME and that the time
	// specified matches the one found at the root.
	if from.AsOf.Expr != nil {
		b.validateAsOf(from.AsOf)
	}

	if len(from.Tables) > 0 {
		outScope = b.buildFromTables(from.Tables, locking, inScope)
	} else {
		outScope = inScope.push()
		outScope.expr = b.factory.ConstructValues(memo.ScalarListWithEmptyTuple, &memo.ValuesPrivate{
			Cols: opt.ColList{},
			ID:   b.factory.Metadata().NextUniqueID(),
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

	filter := b.resolveAndBuildScalar(
		where.Expr,
		types.Bool,
		exprKindWhere,
		tree.RejectGenerators|tree.RejectWindowApplications,
		inScope,
	)

	// Wrap the filter in a FiltersOp.
	inScope.expr = b.factory.ConstructSelect(
		inScope.expr.(memo.RelExpr),
		memo.FiltersExpr{b.factory.ConstructFiltersItem(filter)},
	)
}

// buildFromTables builds a series of InnerJoin expressions that together
// represent the given FROM tables.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildFromTables(
	tables tree.TableExprs, locking lockingSpec, inScope *scope,
) (outScope *scope) {
	// If there are any lateral data sources, we need to build the join tree
	// left-deep instead of right-deep.
	for i := range tables {
		if b.exprIsLateral(tables[i]) {
			telemetry.Inc(sqltelemetry.LateralJoinUseCounter)
			return b.buildFromWithLateral(tables, locking, inScope)
		}
	}
	return b.buildFromTablesRightDeep(tables, locking, inScope)
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
	tables tree.TableExprs, locking lockingSpec, inScope *scope,
) (outScope *scope) {
	outScope = b.buildDataSource(tables[0], nil /* indexFlags */, locking, inScope)

	// Recursively build table join.
	tables = tables[1:]
	if len(tables) == 0 {
		return outScope
	}
	tableScope := b.buildFromTablesRightDeep(tables, locking, inScope)

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
func (b *Builder) buildFromWithLateral(
	tables tree.TableExprs, locking lockingSpec, inScope *scope,
) (outScope *scope) {
	outScope = b.buildDataSource(tables[0], nil /* indexFlags */, locking, inScope)
	for i := 1; i < len(tables); i++ {
		scope := inScope
		// Lateral expressions need to be able to refer to the expressions that
		// have been built already.
		if b.exprIsLateral(tables[i]) {
			scope = outScope
			scope.context = exprKindLateralJoin
		}
		tableScope := b.buildDataSource(tables[i], nil /* indexFlags */, locking, scope)

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
	ts, err := tree.EvalAsOfTimestamp(b.ctx, asOf, b.semaCtx, b.evalCtx)
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

// validateLockingInFrom checks for operations that are not supported with FOR
// [KEY] UPDATE/SHARE. If a locking clause was specified with the select and an
// incompatible operation is in use, a locking error is raised.
func (b *Builder) validateLockingInFrom(
	sel *tree.SelectClause, locking lockingSpec, fromScope *scope,
) {
	if !locking.isSet() {
		// No FOR [KEY] UPDATE/SHARE locking modes in scope.
		return
	}

	switch {
	case sel.Distinct:
		b.raiseLockingContextError(locking, "DISTINCT clause")

	case sel.GroupBy != nil:
		b.raiseLockingContextError(locking, "GROUP BY clause")

	case sel.Having != nil:
		b.raiseLockingContextError(locking, "HAVING clause")

	case fromScope.groupby != nil && fromScope.groupby.hasAggregates():
		b.raiseLockingContextError(locking, "aggregate functions")

	case len(fromScope.windows) != 0:
		b.raiseLockingContextError(locking, "window functions")

	case len(fromScope.srfs) != 0:
		b.raiseLockingContextError(locking, "set-returning functions in the target list")
	}

	for _, li := range locking {
		// Validate locking strength.
		switch li.Strength {
		case tree.ForNone:
			// AST nodes should not be created with this locking strength.
			panic(errors.AssertionFailedf("locking item without strength"))
		case tree.ForUpdate, tree.ForNoKeyUpdate, tree.ForShare, tree.ForKeyShare:
			// CockroachDB treats all of the FOR LOCKED modes as no-ops. Since all
			// transactions are serializable in CockroachDB, clients can't observe
			// whether or not FOR UPDATE (or any of the other weaker modes) actually
			// created a lock. This behavior may improve as the transaction model gains
			// more capabilities.
		default:
			panic(errors.AssertionFailedf("unknown locking strength: %s", li.Strength))
		}

		// Validating locking wait policy.
		switch li.WaitPolicy {
		case tree.LockWaitBlock:
			// Default. Block on conflicting locks.
		case tree.LockWaitSkip:
			panic(unimplementedWithIssueDetailf(40476, "",
				"SKIP LOCKED lock wait policy is not supported"))
		case tree.LockWaitError:
			// Raise an error on conflicting locks.
		default:
			panic(errors.AssertionFailedf("unknown locking wait policy: %s", li.WaitPolicy))
		}

		// Validate locking targets by checking that all targets are well-formed
		// and all point to real relations present in the FROM clause.
		for _, target := range li.Targets {
			// Insist on unqualified alias names here. We could probably do
			// something smarter, but it's better to just mirror Postgres
			// exactly. See transformLockingClause in Postgres' source.
			if target.CatalogName != "" || target.SchemaName != "" {
				panic(pgerror.Newf(pgcode.Syntax,
					"%s must specify unqualified relation names", li.Strength))
			}

			// Search for the target in fromScope. If a target is missing from
			// the scope then raise an error. This will end up looping over all
			// columns in scope for each of the locking targets. We could use a
			// more efficient data structure (e.g. a hash map of relation names)
			// to improve the time complexity here, but we expect the number of
			// columns to be small enough that doing so is likely not worth it.
			found := false
			for _, col := range fromScope.cols {
				if target.ObjectName == col.table.ObjectName {
					found = true
					break
				}
			}
			if !found {
				panic(pgerror.Newf(
					pgcode.UndefinedTable,
					"relation %q in %s clause not found in FROM clause",
					target.ObjectName, li.Strength,
				))
			}
		}
	}
}

// rejectIfLocking raises a locking error if a locking clause was specified.
func (b *Builder) rejectIfLocking(locking lockingSpec, context string) {
	if !locking.isSet() {
		// No FOR [KEY] UPDATE/SHARE locking modes in scope.
		return
	}
	b.raiseLockingContextError(locking, context)
}

// raiseLockingContextError raises an error indicating that a row-level locking
// clause is not permitted in the specified context. locking.isSet() must be true.
func (b *Builder) raiseLockingContextError(locking lockingSpec, context string) {
	panic(pgerror.Newf(pgcode.FeatureNotSupported,
		"%s is not allowed with %s", locking.get().Strength, context))
}
