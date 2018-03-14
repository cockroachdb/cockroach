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
	"context"

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
		// A cross join is an inner join without a condition.
		return p.makeJoin(ctx, sqlbase.InnerJoin, left, right, nil)
	}
}

// getVirtualDataSource attempts to find a virtual table with the
// given name.
func (p *planner) getVirtualDataSource(
	ctx context.Context, tn *tree.TableName,
) (planDataSource, error) {
	virtual, err := p.getVirtualTabler().getVirtualTableEntry(tn)
	if err != nil {
		return planDataSource{}, err
	}

	columns, constructor := virtual.getPlanInfo(ctx)

	// Define the name of the source visible in EXPLAIN(NOEXPAND).
	sourceName := tree.MakeTableNameWithSchema(
		tn.CatalogName, tn.SchemaName, tn.TableName)

	// The resulting node.
	return planDataSource{
		info: sqlbase.NewSourceInfoForSingleTable(sourceName, columns),
		plan: &delayedNode{
			name:    sourceName.String(),
			columns: columns,
			constructor: func(ctx context.Context, p *planner) (planNode, error) {
				return constructor(ctx, p, tn.Catalog())
			},
		},
	}, nil
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
		tn, err := t.Normalize()
		if err != nil {
			return planDataSource{}, err
		}

		// If there's a CTE with this name, it takes priority over the normal flow.
		ds, foundCTE, err := p.getCTEDataSource(tn)
		if foundCTE || err != nil {
			return ds, err
		}

		desc, err := ResolveExistingObject(ctx, p, tn, true /*required*/, anyDescType)
		if err != nil {
			return planDataSource{}, err
		}
		if desc.IsVirtualTable() {
			return p.getVirtualDataSource(ctx, tn)
		}

		colCfg := scanColumnsConfig{visibility: scanVisibility}
		return p.getPlanForDesc(ctx, desc, tn, hints, colCfg)

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
		return p.makeJoin(ctx, sqlbase.JoinTypeFromAstString(t.Join), left, right, t.Cond)

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

// QualifyWithDatabase asserts that the table with the given name
// exists, and expands its table name with the details about its path.
func (p *planner) QualifyWithDatabase(
	ctx context.Context, t *tree.NormalizableTableName,
) (*tree.TableName, error) {
	tn, err := t.Normalize()
	if err != nil {
		return nil, err
	}
	if _, err := ResolveExistingObject(ctx, p, tn, true /*required*/, anyDescType); err != nil {
		return nil, err
	}
	return tn, nil
}

func (p *planner) getTableDescByID(
	ctx context.Context, tableID sqlbase.ID,
) (*sqlbase.TableDescriptor, error) {
	// TODO(knz): replace this by an API on SchemaAccessor/SchemaResolver.
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
		return planDataSource{}, errors.Wrapf(err, "%s", tree.ErrString(tref))
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

	colCfg := scanColumnsConfig{
		wantedColumns:       tref.Columns,
		addUnwantedAsHidden: true,
		visibility:          scanVisibility,
	}
	src, err := p.getPlanForDesc(ctx, desc, &tn, hints, colCfg)
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
				srcName := tree.ErrString(&tableAlias)
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

func (p *planner) getPlanForDesc(
	ctx context.Context,
	desc *sqlbase.TableDescriptor,
	tn *tree.TableName,
	hints *tree.IndexHints,
	colCfg scanColumnsConfig,
) (planDataSource, error) {
	if desc.IsView() {
		if colCfg.wantedColumns != nil {
			return planDataSource{},
				errors.Errorf("cannot specify an explicit column list when accessing a view by reference")
		}
		return p.getViewPlan(ctx, tn, desc)
	}
	if desc.IsSequence() {
		return p.getSequenceSource(ctx, *tn, desc)
	}
	if !desc.IsTable() {
		return planDataSource{}, errors.Errorf(
			"unexpected table descriptor of type %s for %q", desc.TypeName(), tree.ErrString(tn))
	}

	// This name designates a real table.
	scan := p.Scan()
	if err := scan.initTable(ctx, p, desc, hints, colCfg); err != nil {
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
	ctx context.Context,
	src sqlbase.MultiSourceInfo,
	v tree.VarName,
	ivarHelper tree.IndexedVarHelper,
) (columns sqlbase.ResultColumns, exprs []tree.TypedExpr, err error) {
	if len(src) == 0 || len(src[0].SourceColumns) == 0 {
		return nil, nil, pgerror.NewErrorf(pgerror.CodeInvalidNameError,
			"cannot use %q without a FROM clause", tree.ErrString(v))
	}

	colSel := func(src *sqlbase.DataSourceInfo, idx int) {
		col := src.SourceColumns[idx]
		if !col.Hidden {
			ivar := ivarHelper.IndexedVar(idx + src.ColOffset)
			columns = append(columns, sqlbase.ResultColumn{Name: col.Name, Typ: ivar.ResolvedType()})
			exprs = append(exprs, ivar)
		}
	}

	switch sel := v.(type) {
	case tree.UnqualifiedStar:
		// Simple case: a straight '*'. Take all columns.
		for _, ds := range src {
			for i := 0; i < len(ds.SourceColumns); i++ {
				colSel(ds, i)
			}
		}
	case *tree.AllColumnsSelector:
		tn, err := tree.NormalizeTableName(&sel.TableName)
		if err != nil {
			return nil, nil, err
		}
		resolver := sqlbase.ColumnResolver{Sources: src}
		numRes, _, _, err := resolver.FindSourceMatchingName(ctx, tn)
		if err != nil {
			return nil, nil, err
		}
		if numRes == tree.NoResults {
			return nil, nil, pgerror.NewErrorf(pgerror.CodeUndefinedColumnError,
				"no data source named %q", tree.ErrString(&tn))
		}
		ds := src[resolver.ResolverState.SrcIdx]
		colSet := ds.SourceAliases[resolver.ResolverState.ColSetIdx].ColumnSet
		for i, ok := colSet.Next(0); ok; i, ok = colSet.Next(i + 1) {
			colSel(ds, i)
		}
	}

	return columns, exprs, nil
}

// getAliasedTableName returns the underlying table name for a TableExpr that
// could be either an alias or a normal table name. It also returns the original
// table name, which will be equal to the alias name if the input is an alias,
// or identical to the table name if the input is a normal table name.
//
// This is not meant to perform name resolution, but rather simply to
// extract the name indicated after FROM in
// DELETE/INSERT/UPDATE/UPSERT.
func (p *planner) getAliasedTableName(n tree.TableExpr) (*tree.TableName, *tree.TableName, error) {
	var alias *tree.TableName
	if ate, ok := n.(*tree.AliasedTableExpr); ok {
		n = ate.Expr
		// It's okay to ignore the As columns here, as they're not permitted in
		// DML aliases where this function is used.
		alias = tree.NewUnqualifiedTableName(ate.As.Alias)
	}
	table, ok := n.(*tree.NormalizableTableName)
	if !ok {
		return nil, nil, errors.Errorf("TODO(pmattis): unsupported FROM: %s", n)
	}
	tn, err := table.Normalize()
	if err != nil {
		return nil, nil, err
	}
	if alias == nil {
		alias = tn
	}
	return tn, alias, nil
}
