// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
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

	columns, constructor := virtual.getPlanInfo()

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

// getDataSource builds a planDataSource from a single data source clause
// (TableExpr) in a SelectClause.
func (p *planner) getDataSource(
	ctx context.Context,
	src tree.TableExpr,
	indexFlags *tree.IndexFlags,
	scanVisibility scanVisibility,
) (planDataSource, error) {
	switch t := src.(type) {
	case *tree.TableName:
		tn := t

		// If there's a CTE with this name, it takes priority over the normal flow.
		ds, foundCTE, err := p.getCTEDataSource(tn)
		if foundCTE || err != nil {
			return ds, err
		}

		desc, err := ResolveExistingObject(ctx, p, tn, true /*required*/, ResolveAnyDescType)
		if err != nil {
			return planDataSource{}, err
		}
		if desc.IsVirtualTable() && !desc.IsView() {
			return p.getVirtualDataSource(ctx, tn)
		}

		colCfg := scanColumnsConfig{visibility: scanVisibility}
		return p.getPlanForDesc(ctx, desc, tn, indexFlags, colCfg)

	case *tree.RowsFromExpr:
		return p.getPlanForRowsFrom(ctx, t.Items...)

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
		return p.makeJoin(ctx, sqlbase.JoinTypeFromAstString(t.JoinType), left, right, t.Cond)

	case *tree.StatementSource:
		plan, err := p.newPlan(ctx, t.Statement, nil /* desiredTypes */)
		if err != nil {
			return planDataSource{}, err
		}
		cols := planColumns(plan)
		if len(cols) == 0 {
			return planDataSource{}, pgerror.Newf(pgerror.CodeUndefinedColumnError,
				"statement source \"%v\" does not return any columns", t.Statement)
		}
		return planDataSource{
			info: sqlbase.NewSourceInfoForSingleTable(sqlbase.AnonymousTable, cols),
			plan: plan,
		}, nil

	case *tree.ParenTableExpr:
		return p.getDataSource(ctx, t.Expr, indexFlags, scanVisibility)

	case *tree.TableRef:
		return p.getTableScanByRef(ctx, t, indexFlags, scanVisibility)

	case *tree.AliasedTableExpr:
		// Alias clause: source AS alias(cols...)
		if t.Lateral {
			return planDataSource{}, pgerror.Newf(pgerror.CodeFeatureNotSupportedError, "LATERAL is not supported")
		}

		if t.IndexFlags != nil {
			indexFlags = t.IndexFlags
		}

		src, err := p.getDataSource(ctx, t.Expr, indexFlags, scanVisibility)
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

func (p *planner) getTableScanByRef(
	ctx context.Context,
	tref *tree.TableRef,
	indexFlags *tree.IndexFlags,
	scanVisibility scanVisibility,
) (planDataSource, error) {
	flags := ObjectLookupFlags{CommonLookupFlags: CommonLookupFlags{
		avoidCached: p.avoidCachedDescriptors,
	}}
	desc, err := p.Tables().getTableVersionByID(ctx, p.txn, sqlbase.ID(tref.TableID), flags)
	if err != nil {
		return planDataSource{}, pgerror.Wrapf(err, pgerror.CodeSyntaxError,
			"%s", tree.ErrString(tref))
	}

	if tref.Columns != nil && len(tref.Columns) == 0 {
		return planDataSource{}, pgerror.Newf(pgerror.CodeSyntaxError,
			"an explicit list of column IDs must include at least one column")
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

	// Add mutation columns to list of wanted columns if they were requested.
	wantedColumns := tref.Columns
	if scanVisibility == publicAndNonPublicColumns {
		wantedColumns = wantedColumns[:len(wantedColumns):len(wantedColumns)]
		for _, c := range desc.MutationColumns() {
			wantedColumns = append(wantedColumns, tree.ColumnID(c.ID))
		}
	}

	colCfg := scanColumnsConfig{
		wantedColumns:       wantedColumns,
		addUnwantedAsHidden: true,
		visibility:          scanVisibility,
	}
	src, err := p.getPlanForDesc(ctx, desc, &tn, indexFlags, colCfg)
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

		// A SRF uses projectSetNode.
		if vg, ok := src.plan.(*projectSetNode); ok &&
			isAnonymousTable && noColNameSpecified && len(vg.funcs) == 1 {
			// And we only pluck the name if the projection is done over the
			// unary table, and there is just one column in the result.
			if _, ok := vg.source.(*unaryNode); ok && vg.numColsPerGen[0] == 1 {
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
				return planDataSource{}, pgerror.Newf(
					pgerror.CodeInvalidColumnReferenceError,
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
	desc *sqlbase.ImmutableTableDescriptor,
	tn *tree.TableName,
	indexFlags *tree.IndexFlags,
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
	if err := scan.initTable(ctx, p, desc, indexFlags, colCfg); err != nil {
		return planDataSource{}, err
	}
	scan.parallelScansEnabled = sqlbase.ParallelScans.Get(&p.extendedEvalCtx.Settings.SV)

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
	ctx context.Context, tn *tree.TableName, desc *sqlbase.ImmutableTableDescriptor,
) (planDataSource, error) {
	stmt, err := parser.ParseOne(desc.ViewQuery)
	if err != nil {
		return planDataSource{}, pgerror.Wrapf(err, pgerror.CodeSyntaxError,
			"failed to parse underlying query from view %q", tn)
	}
	sel, ok := stmt.AST.(*tree.Select)
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

// getPlanForRowsFrom builds the plan for a ROWS FROM(...) expression.
func (p *planner) getPlanForRowsFrom(
	ctx context.Context, exprs ...tree.Expr,
) (planDataSource, error) {
	srcPlan := &unaryNode{}
	srcInfo := sqlbase.NewSourceInfoForSingleTable(sqlbase.AnonymousTable, nil)
	return p.ProjectSet(ctx, srcPlan, srcInfo, "ROWS FROM", nil, exprs...)
}

func (p *planner) getSequenceSource(
	ctx context.Context, tn tree.TableName, desc *sqlbase.ImmutableTableDescriptor,
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
		// DML aliases where this function is used. The grammar does not allow
		// them, so the parser would have reported an error if they were present.
		if ate.As.Alias != "" {
			alias = tree.NewUnqualifiedTableName(ate.As.Alias)
		}
	}
	tn, ok := n.(*tree.TableName)
	if !ok {
		return nil, nil, unimplemented.New(
			"complex table expression in UPDATE/DELETE",
			"cannot use a complex table name with DELETE/UPDATE",
		)
	}
	if alias == nil {
		alias = tn
	}
	return tn, alias, nil
}
