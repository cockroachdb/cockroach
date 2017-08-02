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
	"fmt"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// To understand dataSourceInfo below it is crucial to understand the
// meaning of a "data source" and its relationship to names/IndexedVars.
//
// A data source is an object that can deliver rows of column data,
// where each row is implemented in CockroachDB as an array of values.
// The defining property of a data source is that the columns in its
// result row arrays are always 0-indexed.
//
// From the language perspective, data sources are defined indirectly by:
// - the FROM clause in a SELECT statement;
// - JOIN clauses within the FROM clause;
// - the clause that follows INSERT INTO colName(Cols...);
// - the clause that follows UPSERT ....;
// - the invisible data source defined by the original table row during
//   UPSERT, if it exists.
//
// Most expressions (parser.Expr trees) in CockroachDB refer to a
// single data source. A notable exception is UPSERT, where expressions
// can refer to two sources: one for the values being inserted, one for
// the original row data in the table for the conflicting (already
// existing) rows.
//
// Meanwhile, IndexedVars in CockroachDB provide the interface between
// symbolic names in expressions (e.g. "f.x", called VarNames,
// or names) and data sources. During evaluation, an IndexedVar must
// resolve to a column value. For a given name there are thus two
// subsequent questions that must be answered:
//
// - which data source is the name referring to? (when there is more than 1 source)
// - which 0-indexed column in that data source is the name referring to?
//
// The IndexedVar must distinguish data sources because the same column index
// may refer to different columns in different data sources. For
// example in an UPSERT statement the IndexedVar for "excluded.x" could refer
// to column 0 in the (already existing) table row, whereas "src.x" could
// refer to column 0 in the valueNode that provides values to insert.
//
// Within this context, the infrastructure for data sources and IndexedVars
// is implemented as follows:
//
// - dataSourceInfo provides column metadata for exactly one data source;
// - multiSourceInfo is an array of one or more dataSourceInfo
// - the index in IndexedVars points to one of the columns in the
//   logical concatenation of all items in the multiSourceInfo;
// - IndexedVarResolver (select_name_resolution.go) is tasked with
//   linking back IndexedVars with their data source and column index.
//
// This being said, there is a misunderstanding one should be careful
// to avoid: *there is no direct relationship between data sources and
// table names* in SQL. In other words:
//
// - the same table name can be present in two or more data sources; for example
//   with:
//        INSERT INTO excluded VALUES (42) ON CONFLICT (x) DO UPDATE ...
//   the name "excluded" can refer either to the data source for VALUES(42)
//   or the implicit data source corresponding to the rows in the original table
//   that conflict with the new values.
//
//   When this happens, a name of the form "excluded.x" must be
//   resolved by considering all the data sources; if there is more
//   than one data source providing the table name "excluded" (as in
//   this case), the query is rejected with an ambiguity error.
//
// - a single data source may provide values for multiple table names; for
//   example with:
//         SELECT * FROM (f CROSS JOIN g) WHERE f.x = g.x
//   there is a single data source corresponding to the results of the
//   CROSS JOIN, providing a single 0-indexed array of values on each
//   result row.
//
//   (multiple table names for a single data source happen in JOINed sources
//   and JOINed sources only. Note that a FROM clause with a comma-separated
//   list of sources is a CROSS JOIN in disguise.)
//
//   When this happens, names of the form "f.x" in either WHERE,
//   SELECT renders, or other expressions which can refer to the data
//   source do not refer to the "internal" data sources of the JOIN;
//   they always refer to the final result rows of the JOIN source as
//   a whole.
//
//   This implies that a single dataSourceInfo that provides metadata
//   for a complex JOIN clause must "know" which table name is
//   associated with each column in its result set.
//

type dataSourceInfo struct {
	// sourceColumns match the plan.Columns() 1-to-1. However the column
	// names might be different if the statement renames them using AS.
	sourceColumns sqlbase.ResultColumns

	// sourceAliases indicates to which table alias column ranges
	// belong.
	// These often correspond to the original table names for each
	// column but might be different if the statement renames
	// them using AS.
	sourceAliases sourceAliases
}

// planDataSource contains the data source information for data
// produced by a planNode.
type planDataSource struct {
	// info which describe the columns.
	info *dataSourceInfo

	// plan which can be used to retrieve the data.
	plan planNode
}

// columnRange identifies a non-empty set of columns in a
// selection. This is used by dataSourceInfo.sourceAliases to map
// table names to column ranges.
type columnRange []int

// sourceAlias associates a table name (alias) to a set of columns in the result
// row of a data source.
type sourceAlias struct {
	name        parser.TableName
	columnRange columnRange
}

func (src *dataSourceInfo) String() string {
	var buf bytes.Buffer
	for i := range src.sourceColumns {
		if i > 0 {
			buf.WriteByte('\t')
		}
		fmt.Fprintf(&buf, "%d", i)
	}
	buf.WriteString("\toutput column positions\n")
	for i, c := range src.sourceColumns {
		if i > 0 {
			buf.WriteByte('\t')
		}
		if c.Hidden {
			buf.WriteByte('*')
		}
		buf.WriteString(c.Name)
	}
	buf.WriteString("\toutput column names\n")
	for _, a := range src.sourceAliases {
		for i := range src.sourceColumns {
			if i > 0 {
				buf.WriteByte('\t')
			}
			for _, j := range a.columnRange {
				if i == j {
					buf.WriteString("x")
					break
				}
			}
		}
		if a.name == anonymousTable {
			buf.WriteString("\t<anonymous table>")
		} else {
			fmt.Fprintf(&buf, "\t'%s'", a.name.String())
		}
		fmt.Fprintf(&buf, " - %q\n", a.columnRange)
	}
	return buf.String()
}

type sourceAliases []sourceAlias

// srcIdx looks up a source by qualified name and returns the index of the
// source (and whether we found one).
func (s sourceAliases) srcIdx(name parser.TableName) (srcIdx int, found bool) {
	for i := range s {
		if s[i].name.DatabaseName == name.DatabaseName && s[i].name.TableName == name.TableName {
			return i, true
		}
	}
	return -1, false
}

// columnRange looks up a source by name and returns the column range (and
// whether we found the name).
func (s sourceAliases) columnRange(name parser.TableName) (colRange columnRange, found bool) {
	idx, ok := s.srcIdx(name)
	if !ok {
		return nil, false
	}
	return s[idx].columnRange, true
}

// anonymousTable is the empty table name, used when a data source
// has no own name, e.g. VALUES, subqueries or the empty source.
var anonymousTable = parser.TableName{}

// fillColumnRange creates a single range that refers to all the
// columns between firstIdx and lastIdx, inclusive.
func fillColumnRange(firstIdx, lastIdx int) columnRange {
	res := make(columnRange, lastIdx-firstIdx+1)
	for i := range res {
		res[i] = i + firstIdx
	}
	return res
}

// newSourceInfoForSingleTable creates a simple dataSourceInfo
// which maps the same tableAlias to all columns.
func newSourceInfoForSingleTable(
	tn parser.TableName, columns sqlbase.ResultColumns,
) *dataSourceInfo {
	return &dataSourceInfo{
		sourceColumns: columns,
		sourceAliases: sourceAliases{{name: tn, columnRange: fillColumnRange(0, len(columns)-1)}},
	}
}

// getSources combines zero or more FROM sources into cross-joins.
func (p *planner) getSources(
	ctx context.Context, sources []parser.TableExpr, scanVisibility scanVisibility,
) (planDataSource, error) {
	switch len(sources) {
	case 0:
		plan := &emptyNode{results: true}
		return planDataSource{
			info: newSourceInfoForSingleTable(anonymousTable, planColumns(plan)),
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
	ctx context.Context, tn *parser.TableName,
) (planDataSource, bool, error) {
	virtual, err := p.session.virtualSchemas.getVirtualTableEntry(tn)
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
		// or "system" otherwise (to only show virt tables to non-root users).
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
		prefix := string(tn.PrefixName)
		if !tn.PrefixOriginallySpecified {
			prefix = p.session.Database
			if prefix == "" && p.session.User != security.RootUser {
				prefix = sqlbase.SystemDB.Name
			}
		}

		// Define the name of the source visible in EXPLAIN(NOEXPAND).
		sourceName := parser.TableName{
			PrefixName:   parser.Name(prefix),
			TableName:    parser.Name(virtual.desc.Name),
			DatabaseName: tn.DatabaseName,
		}

		// The resulting node.
		return planDataSource{
			info: newSourceInfoForSingleTable(sourceName, columns),
			plan: &delayedNode{
				name:    sourceName.String(),
				columns: columns,
				constructor: func(ctx context.Context, p *planner) (planNode, error) {
					return constructor(ctx, p, prefix)
				},
			},
		}, true, nil
	}
	return planDataSource{}, false, nil
}

// getDataSource builds a planDataSource from a single data source clause
// (TableExpr) in a SelectClause.
func (p *planner) getDataSource(
	ctx context.Context,
	src parser.TableExpr,
	hints *parser.IndexHints,
	scanVisibility scanVisibility,
) (planDataSource, error) {
	switch t := src.(type) {
	case *parser.NormalizableTableName:
		// Usual case: a table.
		tn, err := p.QualifyWithDatabase(ctx, t)
		if err != nil {
			return planDataSource{}, err
		}

		// Is this perhaps a name for a virtual table?
		ds, foundVirtual, err := p.getVirtualDataSource(ctx, tn)
		if err != nil {
			return planDataSource{}, err
		}
		if foundVirtual {
			return ds, nil
		}
		return p.getTableScanOrViewPlan(ctx, tn, hints, scanVisibility)

	case *parser.FuncExpr:
		return p.getGeneratorPlan(ctx, t)

	case *parser.Subquery:
		return p.getSubqueryPlan(ctx, anonymousTable, t.Select, nil)

	case *parser.JoinTableExpr:
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

	case *parser.StatementSource:
		plan, err := p.newPlan(ctx, t.Statement, nil)
		if err != nil {
			return planDataSource{}, err
		}
		return planDataSource{
			info: newSourceInfoForSingleTable(anonymousTable, planColumns(plan)),
			plan: plan,
		}, nil

	case *parser.ParenTableExpr:
		return p.getDataSource(ctx, t.Expr, hints, scanVisibility)

	case *parser.TableRef:
		return p.getTableScanByRef(ctx, t, hints, scanVisibility)

	case *parser.AliasedTableExpr:
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
	ctx context.Context, t *parser.NormalizableTableName,
) (*parser.TableName, error) {
	tn, err := t.Normalize()
	if err != nil {
		return nil, err
	}
	if tn.DatabaseName == "" {
		if err := p.searchAndQualifyDatabase(ctx, tn); err != nil {
			return nil, err
		}
	}
	return tn, nil
}

func (p *planner) getTableDescByID(
	ctx context.Context, tableID sqlbase.ID,
) (*sqlbase.TableDescriptor, error) {
	descFunc := p.session.tables.getTableVersionByID
	if p.avoidCachedDescriptors {
		descFunc = sqlbase.GetTableDescFromID
	}
	return descFunc(ctx, p.txn, tableID)
}

func (p *planner) getTableScanByRef(
	ctx context.Context,
	tref *parser.TableRef,
	hints *parser.IndexHints,
	scanVisibility scanVisibility,
) (planDataSource, error) {
	desc, err := p.getTableDescByID(ctx, sqlbase.ID(tref.TableID))
	if err != nil {
		return planDataSource{}, errors.Errorf("%s: %v", parser.ErrString(tref), err)
	}

	tn := parser.TableName{
		TableName: parser.Name(desc.Name),
		// Ideally, we'd like to populate DatabaseName here, however that
		// would require a reverse-lookup from DB ID to database name, and
		// we do not provide an API to do this without a KV lookup. The
		// cost of a KV lookup to populate a field only used in (uncommon)
		// error messages is unwarranted.
		// So instead, we mark the "database name as originally omitted"
		// so as to prevent pretty-printing a potentially confusing empty
		// database name in error messages (we want `foo` not `"".foo`).
		DBNameOriginallyOmitted: true,
	}

	src, err := p.getPlanForDesc(ctx, desc, &tn, hints, scanVisibility, tref.Columns)
	if err != nil {
		return src, err
	}

	return renameSource(src, tref.As, true)
}

// renameSource applies an AS clause to a data source.
func renameSource(
	src planDataSource, as parser.AliasClause, includeHidden bool,
) (planDataSource, error) {
	var tableAlias parser.TableName
	if as.Alias != "" {
		// If an alias was specified, use that.
		tableAlias.TableName = as.Alias
		src.info.sourceAliases = sourceAliases{{
			name:        tableAlias,
			columnRange: fillColumnRange(0, len(src.info.sourceColumns)-1),
		}}
	}
	colAlias := as.Cols

	if len(colAlias) > 0 {
		// Make a copy of the slice since we are about to modify the contents.
		src.info.sourceColumns = append(sqlbase.ResultColumns(nil), src.info.sourceColumns...)

		// The column aliases can only refer to explicit columns.
		for colIdx, aliasIdx := 0, 0; aliasIdx < len(colAlias); colIdx++ {
			if colIdx >= len(src.info.sourceColumns) {
				var srcName string
				if tableAlias.DatabaseName != "" {
					srcName = parser.ErrString(&tableAlias)
				} else {
					srcName = parser.ErrString(tableAlias.TableName)
				}

				return planDataSource{}, errors.Errorf(
					"source %q has %d columns available but %d columns specified",
					srcName, aliasIdx, len(colAlias))
			}
			if !includeHidden && src.info.sourceColumns[colIdx].Hidden {
				continue
			}
			src.info.sourceColumns[colIdx].Name = string(colAlias[aliasIdx])
			aliasIdx++
		}
	}
	return src, nil
}

// getTableScanOrViewPlan builds a planDataSource from a single data source
// clause (either a table or a view) in a SelectClause, expanding views out
// into subqueries.
func (p *planner) getTableScanOrViewPlan(
	ctx context.Context,
	tn *parser.TableName,
	hints *parser.IndexHints,
	scanVisibility scanVisibility,
) (planDataSource, error) {
	if tn.PrefixOriginallySpecified {
		// Prefixes are currently only supported for virtual tables.
		return planDataSource{}, parser.NewInvalidNameErrorf(
			"invalid table name: %q", parser.ErrString(tn))
	}

	desc, err := p.getTableDesc(ctx, tn)
	if err != nil {
		return planDataSource{}, err
	}

	return p.getPlanForDesc(ctx, desc, tn, hints, scanVisibility, nil)
}

func (p *planner) getTableDesc(
	ctx context.Context, tn *parser.TableName,
) (*sqlbase.TableDescriptor, error) {
	if p.avoidCachedDescriptors {
		// AS OF SYSTEM TIME queries need to fetch the table descriptor at the
		// specified time, and never lease anything. The proto transaction already
		// has its timestamps set correctly so getTableOrViewDesc will fetch with
		// the correct timestamp.
		return MustGetTableOrViewDesc(
			ctx, p.txn, p.getVirtualTabler(), tn, false /*allowAdding*/)
	}
	return p.session.tables.getTableVersion(ctx, p.txn, p.getVirtualTabler(), tn)
}

func (p *planner) getPlanForDesc(
	ctx context.Context,
	desc *sqlbase.TableDescriptor,
	tn *parser.TableName,
	hints *parser.IndexHints,
	scanVisibility scanVisibility,
	wantedColumns []parser.ColumnID,
) (planDataSource, error) {
	if desc.IsView() {
		if wantedColumns != nil {
			return planDataSource{},
				errors.Errorf("cannot specify an explicit column list when accessing a view by reference")
		}
		return p.getViewPlan(ctx, tn, desc)
	} else if !desc.IsTable() {
		return planDataSource{}, errors.Errorf(
			"unexpected table descriptor of type %s for %q", desc.TypeName(), parser.ErrString(tn))
	}

	// This name designates a real table.
	scan := p.Scan()
	if err := scan.initTable(p, desc, hints, scanVisibility, wantedColumns); err != nil {
		return planDataSource{}, err
	}

	return planDataSource{
		info: newSourceInfoForSingleTable(*tn, planColumns(scan)),
		plan: scan,
	}, nil
}

// getViewPlan builds a planDataSource for the view specified by the
// table name and descriptor, expanding out its subquery plan.
func (p *planner) getViewPlan(
	ctx context.Context, tn *parser.TableName, desc *sqlbase.TableDescriptor,
) (planDataSource, error) {
	stmt, err := parser.ParseOne(desc.ViewQuery)
	if err != nil {
		return planDataSource{}, errors.Wrapf(err, "failed to parse underlying query from view %q", tn)
	}
	sel, ok := stmt.(*parser.Select)
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
		if err := p.CheckPrivilege(desc, privilege.SELECT); err != nil {
			return planDataSource{}, err
		}
		p.skipSelectPrivilegeChecks = true
		defer func() { p.skipSelectPrivilegeChecks = false }()
	}

	// Register the dependency to the planner, if requested.
	if p.planDeps != nil {
		usedColumns := make([]sqlbase.ColumnID, len(desc.Columns))
		for i := range desc.Columns {
			usedColumns[i] = desc.Columns[i].ID
		}
		deps := p.planDeps[desc.ID]
		deps.desc = desc
		deps.deps = append(deps.deps, sqlbase.TableDescriptor_Reference{ColumnIDs: usedColumns})
		p.planDeps[desc.ID] = deps

		// We are only interested in the dependency to this view descriptor. Any
		// further dependency by the view's query should not be tracked in this planner.
		defer func(prev planDependencies) { p.planDeps = prev }(p.planDeps)
		p.planDeps = nil
	}

	// TODO(a-robinson): Support ORDER BY and LIMIT in views. Is it as simple as
	// just passing the entire select here or will inserting an ORDER BY in the
	// middle of a query plan break things?
	return p.getSubqueryPlan(ctx, *tn, sel.Select, sqlbase.ResultColumnsFromColDescs(desc.Columns))
}

// getSubqueryPlan builds a planDataSource for a select statement, including
// for simple VALUES statements.
func (p *planner) getSubqueryPlan(
	ctx context.Context, tn parser.TableName, sel parser.SelectStatement, cols sqlbase.ResultColumns,
) (planDataSource, error) {
	plan, err := p.newPlan(ctx, sel, nil)
	if err != nil {
		return planDataSource{}, err
	}
	if len(cols) == 0 {
		cols = planColumns(plan)
	}
	return planDataSource{
		info: newSourceInfoForSingleTable(tn, cols),
		plan: plan,
	}, nil
}

func (p *planner) getGeneratorPlan(
	ctx context.Context, t *parser.FuncExpr,
) (planDataSource, error) {
	plan, err := p.makeGenerator(ctx, t)
	if err != nil {
		return planDataSource{}, err
	}
	return planDataSource{
		info: newSourceInfoForSingleTable(anonymousTable, planColumns(plan)),
		plan: plan,
	}, nil
}

// expandStar returns the array of column metadata and name
// expressions that correspond to the expansion of a star.
func (src *dataSourceInfo) expandStar(
	v parser.VarName, ivarHelper parser.IndexedVarHelper,
) (columns sqlbase.ResultColumns, exprs []parser.TypedExpr, err error) {
	if len(src.sourceColumns) == 0 {
		return nil, nil, pgerror.NewErrorf(pgerror.CodeInvalidNameError,
			"cannot use %q without a FROM clause", parser.ErrString(v))
	}

	colSel := func(idx int) {
		col := src.sourceColumns[idx]
		if !col.Hidden {
			ivar := ivarHelper.IndexedVar(idx)
			columns = append(columns, sqlbase.ResultColumn{Name: col.Name, Typ: ivar.ResolvedType()})
			exprs = append(exprs, ivar)
		}
	}

	tableName := parser.TableName{DBNameOriginallyOmitted: true}
	if a, ok := v.(*parser.AllColumnsSelector); ok {
		tableName = a.TableName
	}
	if tableName.Table() == "" {
		for i := 0; i < len(src.sourceColumns); i++ {
			colSel(i)
		}
	} else {
		qualifiedTn, err := src.checkDatabaseName(tableName)
		if err != nil {
			return nil, nil, err
		}

		colRange, ok := src.sourceAliases.columnRange(qualifiedTn)
		if !ok {
			return nil, nil, sqlbase.NewUndefinedRelationError(&tableName)
		}
		for _, i := range colRange {
			colSel(i)
		}
	}

	return columns, exprs, nil
}

type multiSourceInfo []*dataSourceInfo

func newUnknownSourceError(tn *parser.TableName) error {
	return pgerror.NewErrorf(pgerror.CodeUndefinedTableError,
		"source name %q not found in FROM clause", parser.ErrString(tn))
}

func newAmbiguousSourceError(t parser.Name, dbContext parser.Name) error {
	if dbContext == "" {
		return pgerror.NewErrorf(pgerror.CodeAmbiguousAliasError,
			"ambiguous source name: %q", parser.ErrString(t))

	}
	return pgerror.NewErrorf(pgerror.CodeAmbiguousAliasError,
		"ambiguous source name: %q (within database %q)",
		parser.ErrString(t), parser.ErrString(dbContext))
}

// checkDatabaseName checks whether the given TableName is unambiguous
// for the set of sources and if it is, qualifies the missing database name.
func (sources multiSourceInfo) checkDatabaseName(tn parser.TableName) (parser.TableName, error) {
	if tn.DatabaseName == "" {
		// No database name yet. Try to find one.
		found := false
		for _, src := range sources {
			for _, alias := range src.sourceAliases {
				if alias.name.TableName == tn.TableName {
					if found {
						return parser.TableName{}, newAmbiguousSourceError(tn.TableName, "")
					}
					tn.DatabaseName = alias.name.DatabaseName
					found = true
				}
			}
		}
		if !found {
			return parser.TableName{}, newUnknownSourceError(&tn)
		}
		return tn, nil
	}

	// Database given. Check that the name is unambiguous.
	found := false
	for _, src := range sources {
		if _, ok := src.sourceAliases.srcIdx(tn); ok {
			if found {
				return parser.TableName{}, newAmbiguousSourceError(tn.TableName, tn.DatabaseName)
			}
			found = true
		}
	}
	if !found {
		return parser.TableName{}, newUnknownSourceError(&tn)
	}
	return tn, nil
}

// checkDatabaseName checks whether the given TableName is unambiguous
// within this source and if it is, qualifies the missing database name.
func (src *dataSourceInfo) checkDatabaseName(tn parser.TableName) (parser.TableName, error) {
	if tn.DatabaseName == "" {
		// No database name yet. Try to find one.
		found := false
		for _, alias := range src.sourceAliases {
			if alias.name.TableName == tn.TableName {
				if found {
					return parser.TableName{}, newAmbiguousSourceError(tn.TableName, "")
				}
				found = true
				tn.DatabaseName = alias.name.DatabaseName
			}
		}
		if !found {
			return parser.TableName{}, newUnknownSourceError(&tn)
		}
		return tn, nil
	}

	// Database given.
	if _, found := src.sourceAliases.srcIdx(tn); !found {
		return parser.TableName{}, newUnknownSourceError(&tn)
	}
	return tn, nil
}

// findColumn looks up the column specified by a ColumnItem. The
// function returns the index of the source in the multiSourceInfo
// array and the column index for the column array of that
// source. Returns invalid indices and an error if the source is not
// found or the name is ambiguous.
func (sources multiSourceInfo) findColumn(
	c *parser.ColumnItem,
) (srcIdx int, colIdx int, err error) {
	if len(c.Selector) > 0 {
		return invalidSrcIdx, invalidColIdx, pgerror.UnimplementedWithIssueErrorf(8318, "compound types not supported yet: %q", c)
	}

	colName := string(c.ColumnName)
	var tableName parser.TableName
	if c.TableName.Table() != "" {
		tn, err := sources.checkDatabaseName(c.TableName)
		if err != nil {
			return invalidSrcIdx, invalidColIdx, err
		}
		tableName = tn

		// Propagate the discovered database name back to the original VarName.
		// (to clarify the output of e.g. EXPLAIN)
		c.TableName.DatabaseName = tableName.DatabaseName
	}

	findColHelper := func(src *dataSourceInfo, iSrc, srcIdx, colIdx int, idx int) (int, int, error) {
		col := src.sourceColumns[idx]
		if col.Name == colName {
			if colIdx != invalidColIdx {
				return invalidSrcIdx, invalidColIdx, pgerror.NewErrorf(pgerror.CodeAmbiguousColumnError,
					"column reference %q is ambiguous", parser.ErrString(c))
			}
			srcIdx = iSrc
			colIdx = idx
		}
		return srcIdx, colIdx, nil
	}

	colIdx = invalidColIdx
	for iSrc, src := range sources {
		colRange, ok := src.sourceAliases.columnRange(tableName)
		if !ok {
			// The data source "src" has no column for table tableName.
			// Try again with the net one.
			continue
		}
		for _, idx := range colRange {
			srcIdx, colIdx, err = findColHelper(src, iSrc, srcIdx, colIdx, idx)
			if err != nil {
				return srcIdx, colIdx, err
			}
		}
	}

	if colIdx == invalidColIdx && tableName.Table() == "" {
		// Try harder: unqualified column names can look at all
		// columns, not just columns of the anonymous table.
		for iSrc, src := range sources {
			for idx := 0; idx < len(src.sourceColumns); idx++ {
				srcIdx, colIdx, err = findColHelper(src, iSrc, srcIdx, colIdx, idx)
				if err != nil {
					return srcIdx, colIdx, err
				}
			}
		}
	}

	if colIdx == invalidColIdx {
		return invalidSrcIdx, invalidColIdx,
			pgerror.NewErrorf(pgerror.CodeUndefinedColumnError,
				"column name %q not found", parser.ErrString(c))
	}

	return srcIdx, colIdx, nil
}

// findTableAlias returns the first table alias providing the column
// index given as argument. The index must be valid.
func (src *dataSourceInfo) findTableAlias(colIdx int) (parser.TableName, bool) {
	for _, alias := range src.sourceAliases {
		for _, idx := range alias.columnRange {
			if colIdx == idx {
				return alias.name, true
			}
		}
	}
	return anonymousTable, false
}

func (src *dataSourceInfo) FormatVar(buf *bytes.Buffer, f parser.FmtFlags, colIdx int) {
	if f.ShowTableAliases {
		tableAlias, found := src.findTableAlias(colIdx)
		if found {
			if tableAlias.TableName != "" {
				if tableAlias.DatabaseName != "" {
					parser.FormatNode(buf, f, tableAlias.DatabaseName)
					buf.WriteByte('.')
				}
				parser.FormatNode(buf, f, tableAlias.TableName)
				buf.WriteByte('.')
			}
		} else {
			buf.WriteString("_.")
		}
	}
	parser.Name(src.sourceColumns[colIdx].Name).Format(buf, f)
}
