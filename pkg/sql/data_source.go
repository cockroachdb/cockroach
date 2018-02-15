// Copyright 2016 The Cockroach Authors.
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

package sql

import (
	"bytes"
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// For more detailed documentation on DataSourceInfos, see
// sqlbase/data_source.go.

// planDataSource contains the data source information for data
// produced by a planNode.
type planDataSource struct {
	// info which describe the columns.
	info *sqlbase.DataSourceInfo

	// plan which can be used to retrieve the data.
	plan planNode
}

// getSources combines zero or more FROM sources into cross-joins.
func (p *planner) getSources(
	ctx context.Context, sources []tree.TableExpr, scanVisibility scanVisibility,
) (planDataSource, error) {
	switch len(sources) {
	case 0:
		plan := &unaryNode{}
		return planDataSource{
			info: sqlbase.NewSourceInfoForSingleTable(sqlbase.AnonymousTable, planColumns(plan)),
			plan: plan,
		}, nil

	case 1:
		return p.getDataSource(ctx, sources[0], nil, scanVisibility)

	default:
		left, err := p.getDataSource(ctx, sources[0], nil, scanVisibility)
		if err != nil {
			return planDataSource{}, err
		}
		right, err := p.getSources(ctx, sources[1:], scanVisibility)
		if err != nil {
			return planDataSource{}, err
		}
		return p.makeJoin(ctx, "CROSS JOIN", left, right, nil)
	}
}

// getVirtualDataSource attempts to find a virtual table with the
// given name.
func (p *planner) getVirtualDataSource(
	ctx context.Context, tn *tree.TableName,
) (planDataSource, bool, error) {
	virtual, err := p.getVirtualTabler().getVirtualTableEntry(tn)
	if err != nil {
		return planDataSource{}, false, err
	}
	if virtual.desc != nil {
		columns, constructor := virtual.getPlanInfo(ctx)

		// The virtual table constructor takes the target database
		// as "prefix" argument. This is either the prefix in the
		// virtual table name given in the SQL query explicitly,
		// or, if no prefix is given,
		// the current database if one is set,
		// or the empty prefix if the user is root (to show everything),
		// or "system" otherwise (to only show virtual tables to non-root users).
		//
		// It is particularly important to not use the empty prefix for
		// non-root users, because client libraries that mistakenly do not
		// set a current database tend to be badly, badly behaved if they
		// see tables from multiple databases (as in, "drop tables from
		// the wrong db" mis-behaved). Lack of data in the vtable in the
		// case where there is no current database is thus safer.
		//
		// Meanwhile the root user probably would be inconvenienced by
		// this.
		catalog := string(tn.CatalogName)
		if !tn.ExplicitCatalog {
			catalog = p.SessionData().Database
			if catalog == "" && p.RequireSuperUser(ctx, "access virtual tables across all databases") != nil {
				catalog = sqlbase.SystemDB.Name
			}
		}

		// Define the name of the source visible in EXPLAIN(NOEXPAND).
		sourceName := tree.MakeTableNameWithCatalog(
			tree.Name(catalog), tn.SchemaName, tree.Name(virtual.desc.Name))

		// The resulting node.
		return planDataSource{
			info: sqlbase.NewSourceInfoForSingleTable(sourceName, columns),
			plan: &delayedNode{
				name:    sourceName.String(),
				columns: columns,
				constructor: func(ctx context.Context, p *planner) (planNode, error) {
					return constructor(ctx, p, catalog)
				},
			},
		}, true, nil
	}
	return planDataSource{}, false, nil
}

// getDataSourceAsOneColumn builds a planDataSource from a data source
// clause and ensures that it returns one column. If the plan would
// return zero or more than one column, the columns are grouped into
// a tuple. This is needed for SRF substitution (e.g. `SELECT
// pg_get_keywords()`).
func (p *planner) getDataSourceAsOneColumn(
	ctx context.Context, src *tree.FuncExpr,
) (planDataSource, error) {
	ds, err := p.getDataSource(ctx, src, nil, publicColumns)
	if err != nil {
		return ds, err
	}
	if len(ds.info.SourceColumns) == 1 {
		return ds, nil
	}

	// Zero or more than one column: make a tuple.

	// We use the name of the function to determine the name of the
	// rendered column.
	fd, err := src.Func.Resolve(p.SessionData().SearchPath)
	if err != nil {
		return planDataSource{}, err
	}
	newPlan, err := p.makeTupleRender(ctx, ds, fd.Name)
	if err != nil {
		return planDataSource{}, err
	}

	tn := tree.MakeUnqualifiedTableName(tree.Name(fd.Name))
	return planDataSource{
		info: sqlbase.NewSourceInfoForSingleTable(tn, planColumns(newPlan)),
		plan: newPlan,
	}, nil
}

// getDataSource builds a planDataSource from a single data source clause
// (TableExpr) in a SelectClause.
func (p *planner) getDataSource(
	ctx context.Context, src tree.TableExpr, hints *tree.IndexHints, scanVisibility scanVisibility,
) (planDataSource, error) {
	switch t := src.(type) {
	case *tree.NormalizableTableName:
		// If there's a CTE with this name, it takes priority over the normal flow.
		ds, foundCTE, err := p.getCTEDataSource(t)
		if err != nil {
			return planDataSource{}, err
		}
		if foundCTE {
			return ds, nil
		}
		// Usual case: a table.
		tn, err := p.QualifyWithDatabase(ctx, t)
		if err != nil {
			return planDataSource{}, err
		}

		// Check if this is a virtual table.
		ds, foundVirtual, err := p.getVirtualDataSource(ctx, tn)
		if err != nil {
			return planDataSource{}, err
		}
		if foundVirtual {
			return ds, nil
		}
		return p.getTableScanOrSequenceOrViewPlan(ctx, tn, hints, scanVisibility)

	case *tree.FuncExpr:
		return p.getGeneratorPlan(ctx, t)

	case *tree.Subquery:
		return p.getSubqueryPlan(ctx, sqlbase.AnonymousTable, t.Select, nil)

	case *tree.JoinTableExpr:
		// Joins: two sources.
		left, err := p.getDataSource(ctx, t.Left, nil, scanVisibility)
		if err != nil {
			return left, err
		}
		right, err := p.getDataSource(ctx, t.Right, nil, scanVisibility)
		if err != nil {
			return right, err
		}
		return p.makeJoin(ctx, t.Join, left, right, t.Cond)

	case *tree.StatementSource:
		plan, err := p.newPlan(ctx, t.Statement, nil /* desiredTypes */)
		if err != nil {
			return planDataSource{}, err
		}
		cols := planColumns(plan)
		if len(cols) == 0 {
			return planDataSource{}, pgerror.NewErrorf(pgerror.CodeFeatureNotSupportedError,
				"statement source \"%v\" does not return any columns", t.Statement)
		}
		return planDataSource{
			info: sqlbase.NewSourceInfoForSingleTable(sqlbase.AnonymousTable, cols),
			plan: plan,
		}, nil

	case *tree.ParenTableExpr:
		return p.getDataSource(ctx, t.Expr, hints, scanVisibility)

	case *tree.TableRef:
		return p.getTableScanByRef(ctx, t, hints, scanVisibility)

	case *tree.AliasedTableExpr:
		// Alias clause: source AS alias(cols...)

		if t.Hints != nil {
			hints = t.Hints
		}

		src, err := p.getDataSource(ctx, t.Expr, hints, scanVisibility)
		if err != nil {
			return src, err
		}

		if t.Ordinality {
			// The WITH ORDINALITY clause numbers the rows coming out of the
			// data source. See the comments next to the definition of
			// `ordinalityNode` in particular how this restricts
			// optimizations.
			src = p.wrapOrdinality(src)
		}

		return renameSource(src, t.As, false)

	default:
		return planDataSource{}, errors.Errorf("unsupported FROM type %T", src)
	}
}

func (p *planner) QualifyWithDatabase(
	ctx context.Context, t *tree.NormalizableTableName,
) (*tree.TableName, error) {
	tn, err := t.Normalize()
	if err != nil {
		return nil, err
	}
	if tn.SchemaName == "" {
		if err := p.searchAndQualifyDatabase(ctx, tn); err != nil {
			return nil, err
		}
	}
	return tn, nil
}

func (p *planner) getTableDescByID(
	ctx context.Context, tableID sqlbase.ID,
) (*sqlbase.TableDescriptor, error) {
	descFunc := p.Tables().getTableVersionByID
	if p.avoidCachedDescriptors {
		descFunc = sqlbase.GetTableDescFromID
	}
	return descFunc(ctx, p.txn, tableID)
}

func (p *planner) getTableScanByRef(
	ctx context.Context, tref *tree.TableRef, hints *tree.IndexHints, scanVisibility scanVisibility,
) (planDataSource, error) {
	desc, err := p.getTableDescByID(ctx, sqlbase.ID(tref.TableID))
	if err != nil {
		return planDataSource{}, errors.Errorf("%s: %v", tree.ErrString(tref), err)
	}

	// Ideally, we'd like to populate DatabaseName here, however that
	// would require a reverse-lookup from DB ID to database name, and
	// we do not provide an API to do this without a KV lookup. The
	// cost of a KV lookup to populate a field only used in (uncommon)
	// error messages is unwarranted.
	// So instead, we hide the schema part so as to prevent
	// pretty-printing a potentially confusing empty database name in
	// error messages (we want `foo` not `"".foo`).
	tn := tree.MakeUnqualifiedTableName(tree.Name(desc.Name))

	src, err := p.getPlanForDesc(ctx, desc, &tn, hints, scanVisibility, tref.Columns)
	if err != nil {
		return src, err
	}

	return renameSource(src, tref.As, true)
}

// renameSource applies an AS clause to a data source.
func renameSource(
	src planDataSource, as tree.AliasClause, includeHidden bool,
) (planDataSource, error) {
	var tableAlias tree.TableName
	colAlias := as.Cols

	if as.Alias != "" {
		// Special case for Postgres compatibility: if a data source does
		// not currently have a name, and it is a set-generating function
		// with just one column, and the AS clause doesn't specify column
		// names, then use the specified table name both as the
		// column name and table name.
		isAnonymousTable := (len(src.info.SourceAliases) == 0 ||
			(len(src.info.SourceAliases) == 1 && src.info.SourceAliases[0].Name == sqlbase.AnonymousTable))
		noColNameSpecified := len(colAlias) == 0
		if vg, ok := src.plan.(*valueGenerator); ok && isAnonymousTable && noColNameSpecified {
			if tType, ok := vg.expr.ResolvedType().(types.TTable); ok && len(tType.Cols) == 1 {
				colAlias = tree.NameList{as.Alias}
			}
		}

		// If an alias was specified, use that.
		tableAlias.TableName = as.Alias
		src.info.SourceAliases = sqlbase.SourceAliases{{
			Name:      tableAlias,
			ColumnSet: sqlbase.FillColumnRange(0, len(src.info.SourceColumns)-1),
		}}
	}

	if len(colAlias) > 0 {
		// Make a copy of the slice since we are about to modify the contents.
		src.info.SourceColumns = append(sqlbase.ResultColumns(nil), src.info.SourceColumns...)

		// The column aliases can only refer to explicit columns.
		for colIdx, aliasIdx := 0, 0; aliasIdx < len(colAlias); colIdx++ {
			if colIdx >= len(src.info.SourceColumns) {
				var srcName string
				if tableAlias.SchemaName != "" {
					srcName = tree.ErrString(&tableAlias)
				} else {
					srcName = tree.ErrString(&tableAlias.TableName)
				}

				return planDataSource{}, errors.Errorf(
					"source %q has %d columns available but %d columns specified",
					srcName, aliasIdx, len(colAlias))
			}
			if !includeHidden && src.info.SourceColumns[colIdx].Hidden {
				continue
			}
			src.info.SourceColumns[colIdx].Name = string(colAlias[aliasIdx])
			aliasIdx++
		}
	}
	return src, nil
}

// getTableScanOrSequenceOrViewPlan builds a planDataSource from a single data source
// clause (either a table or a view) in a SelectClause, expanding views out
// into subqueries.
func (p *planner) getTableScanOrSequenceOrViewPlan(
	ctx context.Context, tn *tree.TableName, hints *tree.IndexHints, scanVisibility scanVisibility,
) (planDataSource, error) {
	if tn.ExplicitCatalog {
		// Prefixes are currently only supported for virtual tables.
		return planDataSource{}, tree.NewInvalidNameErrorf(
			"invalid table name: %q", tree.ErrString(tn))
	}

	desc, err := p.getTableDesc(ctx, tn)
	if err != nil {
		return planDataSource{}, err
	}

	return p.getPlanForDesc(ctx, desc, tn, hints, scanVisibility, nil)
}

func (p *planner) getTableDesc(
	ctx context.Context, tn *tree.TableName,
) (*sqlbase.TableDescriptor, error) {
	if p.avoidCachedDescriptors {
		return MustGetTableOrViewDesc(
			ctx, p.txn, p.getVirtualTabler(), tn, false /*allowAdding*/)
	}
	return p.Tables().getTableVersion(ctx, p.txn, p.getVirtualTabler(), tn)
}

// bumpTableVersion loads the table descriptor for 'table', calls UpVersion and persists it.
func (p *planner) bumpTableVersion(ctx context.Context, tn *tree.TableName) error {
	tableDesc, err := MustGetTableOrViewDesc(
		ctx, p.txn, p.getVirtualTabler(), tn, false /*allowAdding*/)
	if err != nil {
		return err
	}

	return p.saveNonmutationAndNotify(ctx, tableDesc)
}

func (p *planner) getPlanForDesc(
	ctx context.Context,
	desc *sqlbase.TableDescriptor,
	tn *tree.TableName,
	hints *tree.IndexHints,
	scanVisibility scanVisibility,
	wantedColumns []tree.ColumnID,
) (planDataSource, error) {
	if desc.IsView() {
		if wantedColumns != nil {
			return planDataSource{},
				errors.Errorf("cannot specify an explicit column list when accessing a view by reference")
		}
		return p.getViewPlan(ctx, tn, desc)
	} else if desc.IsSequence() {
		return p.getSequenceSource(ctx, *tn, desc)
	} else if !desc.IsTable() {
		return planDataSource{}, errors.Errorf(
			"unexpected table descriptor of type %s for %q", desc.TypeName(), tree.ErrString(tn))
	}

	// This name designates a real table.
	scan := p.Scan()
	if err := scan.initTable(ctx, p, desc, hints, scanVisibility, wantedColumns); err != nil {
		return planDataSource{}, err
	}

	ds := planDataSource{
		info: sqlbase.NewSourceInfoForSingleTable(*tn, planColumns(scan)),
		plan: scan,
	}
	ds.info.NumBackfillColumns = scan.numBackfillColumns
	return ds, nil
}

// getViewPlan builds a planDataSource for the view specified by the
// table name and descriptor, expanding out its subquery plan.
func (p *planner) getViewPlan(
	ctx context.Context, tn *tree.TableName, desc *sqlbase.TableDescriptor,
) (planDataSource, error) {
	stmt, err := parser.ParseOne(desc.ViewQuery)
	if err != nil {
		return planDataSource{}, errors.Wrapf(err, "failed to parse underlying query from view %q", tn)
	}
	sel, ok := stmt.(*tree.Select)
	if !ok {
		return planDataSource{},
			errors.Errorf("failed to parse underlying query from view %q as a select", tn)
	}

	// When constructing the subquery plan, we don't want to check for the SELECT
	// privilege on the underlying tables, just on the view itself. Checking on
	// the underlying tables as well would defeat the purpose of having separate
	// SELECT privileges on the view, which is intended to allow for exposing
	// some subset of a restricted table's data to less privileged users.
	if !p.skipSelectPrivilegeChecks {
		if err := p.CheckPrivilege(ctx, desc, privilege.SELECT); err != nil {
			return planDataSource{}, err
		}
		p.skipSelectPrivilegeChecks = true
		defer func() { p.skipSelectPrivilegeChecks = false }()
	}

	// Register the dependency to the planner, if requested.
	if p.curPlan.deps != nil {
		usedColumns := make([]sqlbase.ColumnID, len(desc.Columns))
		for i := range desc.Columns {
			usedColumns[i] = desc.Columns[i].ID
		}
		deps := p.curPlan.deps[desc.ID]
		deps.desc = desc
		deps.deps = append(deps.deps, sqlbase.TableDescriptor_Reference{ColumnIDs: usedColumns})
		p.curPlan.deps[desc.ID] = deps

		// We are only interested in the dependency to this view descriptor. Any
		// further dependency by the view's query should not be tracked in this planner.
		defer func(prev planDependencies) { p.curPlan.deps = prev }(p.curPlan.deps)
		p.curPlan.deps = nil
	}

	plan, err := p.newPlan(ctx, sel, nil)
	if err != nil {
		return planDataSource{}, err
	}
	return planDataSource{
		info: sqlbase.NewSourceInfoForSingleTable(*tn, sqlbase.ResultColumnsFromColDescs(desc.Columns)),
		plan: plan,
	}, nil
}

// getSubqueryPlan builds a planDataSource for a select statement, including
// for simple VALUES statements.
func (p *planner) getSubqueryPlan(
	ctx context.Context, tn tree.TableName, sel tree.SelectStatement, cols sqlbase.ResultColumns,
) (planDataSource, error) {
	plan, err := p.newPlan(ctx, sel, nil)
	if err != nil {
		return planDataSource{}, err
	}
	if len(cols) == 0 {
		cols = planColumns(plan)
	}
	return planDataSource{
		info: sqlbase.NewSourceInfoForSingleTable(tn, cols),
		plan: plan,
	}, nil
}

func (p *planner) getGeneratorPlan(ctx context.Context, t *tree.FuncExpr) (planDataSource, error) {
	plan, err := p.makeGenerator(ctx, t)
	if err != nil {
		return planDataSource{}, err
	}
	return planDataSource{
		info: sqlbase.NewSourceInfoForSingleTable(sqlbase.AnonymousTable, planColumns(plan)),
		plan: plan,
	}, nil
}

func (p *planner) getSequenceSource(
	ctx context.Context, tn tree.TableName, desc *sqlbase.TableDescriptor,
) (planDataSource, error) {
	if err := p.CheckPrivilege(ctx, desc, privilege.SELECT); err != nil {
		return planDataSource{}, err
	}

	node, err := p.SequenceSelectNode(desc)
	if err != nil {
		return planDataSource{}, err
	}
	return planDataSource{
		plan: node,
		info: sqlbase.NewSourceInfoForSingleTable(tn, sequenceSelectColumns),
	}, nil
}

// expandStar returns the array of column metadata and name
// expressions that correspond to the expansion of a star.
func expandStar(
	src *sqlbase.DataSourceInfo, v tree.VarName, ivarHelper tree.IndexedVarHelper,
) (columns sqlbase.ResultColumns, exprs []tree.TypedExpr, err error) {
	if len(src.SourceColumns) == 0 {
		return nil, nil, pgerror.NewErrorf(pgerror.CodeInvalidNameError,
			"cannot use %q without a FROM clause", tree.ErrString(v))
	}

	colSel := func(idx int) {
		col := src.SourceColumns[idx]
		if !col.Hidden {
			ivar := ivarHelper.IndexedVar(idx)
			columns = append(columns, sqlbase.ResultColumn{Name: col.Name, Typ: ivar.ResolvedType()})
			exprs = append(exprs, ivar)
		}
	}

	tableName := sqlbase.AnonymousTable
	if a, ok := v.(*tree.AllColumnsSelector); ok {
		tableName = a.TableName
	}
	if tableName.Table() == "" {
		for i := 0; i < len(src.SourceColumns); i++ {
			colSel(i)
		}
	} else {
		qualifiedTn, err := checkDatabaseName(src, tableName)
		if err != nil {
			return nil, nil, err
		}

		colSet, ok := src.SourceAliases.ColumnSet(qualifiedTn)
		if !ok {
			return nil, nil, sqlbase.NewUndefinedRelationError(&tableName)
		}
		for i, ok := colSet.Next(0); ok; i, ok = colSet.Next(i + 1) {
			colSel(i)
		}
	}

	return columns, exprs, nil
}

func newUnknownSourceError(tn *tree.TableName) error {
	return pgerror.NewErrorf(pgerror.CodeUndefinedTableError,
		"source name %q not found in FROM clause", tree.ErrString(tn))
}

func newAmbiguousSourceError(t *tree.Name, dbContext *tree.Name) error {
	if *dbContext == "" {
		return pgerror.NewErrorf(pgerror.CodeAmbiguousAliasError,
			"ambiguous source name: %q", tree.ErrString(t))
	}
	return pgerror.NewErrorf(pgerror.CodeAmbiguousAliasError,
		"ambiguous source name: %q (within database %q)",
		tree.ErrString(t), tree.ErrString(dbContext))
}

// checkDatabaseNameMulti checks whether the given TableName is unambiguous
// for the set of sources and if it is, qualifies the missing database name.
func checkDatabaseNameMulti(
	sources sqlbase.MultiSourceInfo, tn tree.TableName,
) (tree.TableName, error) {
	if tn.SchemaName == "" {
		// No database name yet. Try to find one.
		found := false
		for _, src := range sources {
			for _, alias := range src.SourceAliases {
				if alias.Name.TableName == tn.TableName {
					if found {
						return tree.TableName{}, newAmbiguousSourceError(&tn.TableName, &tree.NoName)
					}
					tn.SchemaName = alias.Name.SchemaName
					found = true
				}
			}
		}
		if !found {
			return tree.TableName{}, newUnknownSourceError(&tn)
		}
		return tn, nil
	}

	// Database given. Check that the name is unambiguous.
	found := false
	for _, src := range sources {
		if _, ok := src.SourceAliases.SrcIdx(tn); ok {
			if found {
				return tree.TableName{}, newAmbiguousSourceError(&tn.TableName, &tn.SchemaName)
			}
			found = true
		}
	}
	if !found {
		return tree.TableName{}, newUnknownSourceError(&tn)
	}
	return tn, nil
}

// checkDatabaseName checks whether the given TableName is unambiguous
// within this source and if it is, qualifies the missing database name.
func checkDatabaseName(src *sqlbase.DataSourceInfo, tn tree.TableName) (tree.TableName, error) {
	if tn.SchemaName == "" {
		// No database name yet. Try to find one.
		found := false
		for _, alias := range src.SourceAliases {
			if alias.Name.TableName == tn.TableName {
				if found {
					return tree.TableName{}, newAmbiguousSourceError(&tn.TableName, &tree.NoName)
				}
				found = true
				tn.SchemaName = alias.Name.SchemaName
			}
		}
		if !found {
			return tree.TableName{}, newUnknownSourceError(&tn)
		}
		return tn, nil
	}

	// Database given.
	if _, found := src.SourceAliases.SrcIdx(tn); !found {
		return tree.TableName{}, newUnknownSourceError(&tn)
	}
	return tn, nil
}

// invalidSrcIdx is the srcIdx value returned by findColumn() when there is no match.
const invalidSrcIdx = -1

// invalidColIdx is the colIdx value returned by findColumn() when there is no match.
const invalidColIdx = -1

func findColHelper(
	sources sqlbase.MultiSourceInfo,
	src *sqlbase.DataSourceInfo,
	c *tree.ColumnItem,
	colName string,
	iSrc, srcIdx, colIdx, idx int,
) (int, int, error) {
	col := src.SourceColumns[idx]
	if col.Name == colName {
		// Do not return a match if:
		// 1. The column is being backfilled and therefore should not be
		// used to resolve a column expression, and,
		// 2. The column expression being resolved is not from a selector
		// column expression from an UPDATE/DELETE.
		if backfillThreshold := len(src.SourceColumns) - src.NumBackfillColumns; idx >= backfillThreshold && !c.ForUpdateOrDelete {
			return invalidSrcIdx, invalidColIdx,
				pgerror.NewErrorf(pgerror.CodeInvalidColumnReferenceError,
					"column %q is being backfilled", tree.ErrString(c))
		}
		if colIdx != invalidColIdx {
			colString := tree.ErrString(c)
			var msgBuf bytes.Buffer
			sep := ""
			fmtCandidate := func(alias *sqlbase.SourceAlias) {
				name := tree.ErrString(&alias.Name.TableName)
				if len(name) == 0 {
					name = "<anonymous>"
				}
				fmt.Fprintf(&msgBuf, "%s%s.%s", sep, name, colString)
			}
			for i := range src.SourceAliases {
				fmtCandidate(&src.SourceAliases[i])
				sep = ", "
			}
			if iSrc != srcIdx {
				for i := range sources[srcIdx].SourceAliases {
					fmtCandidate(&sources[srcIdx].SourceAliases[i])
					sep = ", "
				}
			}
			return invalidSrcIdx,
				invalidColIdx,
				pgerror.NewErrorf(pgerror.CodeAmbiguousColumnError,
					"column reference %q is ambiguous (candidates: %s)", colString, msgBuf.String(),
				)
		}
		srcIdx = iSrc
		colIdx = idx
	}
	return srcIdx, colIdx, nil
}

// findColumn looks up the column specified by a ColumnItem. The
// function returns the index of the source in the MultiSourceInfo
// array and the column index for the column array of that
// source. Returns invalid indices and an error if the source is not
// found or the name is ambiguous.
func findColumn(
	sources sqlbase.MultiSourceInfo, c *tree.ColumnItem,
) (srcIdx int, colIdx int, err error) {
	colName := string(c.ColumnName)
	var tableName tree.TableName
	if c.TableName.Table() != "" {
		tn, err := checkDatabaseNameMulti(sources, c.TableName)
		if err != nil {
			return invalidSrcIdx, invalidColIdx, err
		}
		tableName = tn

		// Propagate the discovered database name back to the original VarName.
		// (to clarify the output of e.g. EXPLAIN)
		c.TableName.SchemaName = tableName.SchemaName
	}

	colIdx = invalidColIdx
	for iSrc, src := range sources {
		colSet, ok := src.SourceAliases.ColumnSet(tableName)
		if !ok {
			// The data source "src" has no column for table tableName.
			// Try again with the next one.
			continue
		}
		for idx, ok := colSet.Next(0); ok; idx, ok = colSet.Next(idx + 1) {
			srcIdx, colIdx, err = findColHelper(sources, src, c, colName, iSrc, srcIdx, colIdx, idx)
			if err != nil {
				return srcIdx, colIdx, err
			}
		}
	}

	if colIdx == invalidColIdx && tableName.Table() == "" {
		// Try harder: unqualified column names can look at all
		// columns, not just columns of the anonymous table.
		for iSrc, src := range sources {
			for idx := 0; idx < len(src.SourceColumns); idx++ {
				srcIdx, colIdx, err = findColHelper(sources, src, c, colName, iSrc, srcIdx, colIdx, idx)
				if err != nil {
					return srcIdx, colIdx, err
				}
			}
		}
	}

	if colIdx == invalidColIdx {
		return invalidSrcIdx, invalidColIdx,
			pgerror.NewErrorf(pgerror.CodeUndefinedColumnError,
				"column name %q not found", tree.ErrString(c))
	}

	return srcIdx, colIdx, nil
}
