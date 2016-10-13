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
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package sql

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/pkg/errors"
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
// - multiSourceInfo is an array of one ore more dataSourceInfo
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
	sourceColumns ResultColumns

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

// sourceAliases associates a table name (alias) to a set of columns
// in the result row of a data source.
type sourceAliases map[parser.TableName]columnRange

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
func newSourceInfoForSingleTable(tn parser.TableName, columns ResultColumns) *dataSourceInfo {
	norm := sqlbase.NormalizeTableName(tn)
	return &dataSourceInfo{
		sourceColumns: columns,
		sourceAliases: sourceAliases{norm: fillColumnRange(0, len(columns)-1)},
	}
}

// getSources combines zero or more FROM sources into cross-joins.
func (p *planner) getSources(
	sources []parser.TableExpr, scanVisibility scanVisibility,
) (planDataSource, error) {
	switch len(sources) {
	case 0:
		plan := &emptyNode{results: true}
		return planDataSource{
			info: newSourceInfoForSingleTable(parser.TableName{}, plan.Columns()),
			plan: plan,
		}, nil

	case 1:
		return p.getDataSource(sources[0], nil, scanVisibility)

	default:
		left, err := p.getDataSource(sources[0], nil, scanVisibility)
		if err != nil {
			return planDataSource{}, err
		}
		right, err := p.getSources(sources[1:], scanVisibility)
		if err != nil {
			return planDataSource{}, err
		}
		return p.makeJoin("CROSS JOIN", left, right, nil)
	}
}

// getVirtualDataSource attempts to find a virtual table with the
// given name.
func (p *planner) getVirtualDataSource(tn *parser.TableName) (planDataSource, bool, error) {
	virtual, err := p.virtualSchemas().getVirtualTableEntry(tn)
	if err != nil {
		return planDataSource{}, false, err
	}
	if virtual.desc != nil {
		columns, constructor := virtual.getPlanInfo()
		sourceName := parser.TableName{
			TableName:    parser.Name(virtual.desc.Name),
			DatabaseName: tn.DatabaseName,
		}
		return planDataSource{
			info: newSourceInfoForSingleTable(sourceName, columns),
			plan: &delayedValuesNode{
				p:           p,
				name:        sourceName.String(),
				columns:     columns,
				constructor: constructor,
			},
		}, true, nil
	}
	return planDataSource{}, false, nil
}

// getDataSource builds a planDataSource from a single data source clause
// (TableExpr) in a SelectClause.
func (p *planner) getDataSource(
	src parser.TableExpr, hints *parser.IndexHints, scanVisibility scanVisibility,
) (planDataSource, error) {
	switch t := src.(type) {
	case *parser.NormalizableTableName:
		// Usual case: a table.
		tn, err := t.Normalize()
		if err != nil {
			return planDataSource{}, err
		}
		if tn.DatabaseName == "" {
			database, err := p.databaseFromSearchPath(tn)
			if err != nil {
				return planDataSource{}, err
			}
			if err := tn.QualifyWithDatabase(database); err != nil {
				return planDataSource{}, err
			}
		}

		// Is this perhaps a name for a virtual table?
		ds, foundVirtual, err := p.getVirtualDataSource(tn)
		if err != nil {
			return planDataSource{}, err
		}
		if foundVirtual {
			return ds, nil
		}
		return p.getTableScanOrViewPlan(tn, hints, scanVisibility)

	case *parser.Subquery:
		return p.getSubqueryPlan(t.Select, nil)

	case *parser.JoinTableExpr:
		// Joins: two sources.
		left, err := p.getDataSource(t.Left, nil, scanVisibility)
		if err != nil {
			return left, err
		}
		right, err := p.getDataSource(t.Right, nil, scanVisibility)
		if err != nil {
			return right, err
		}
		return p.makeJoin(t.Join, left, right, t.Cond)

	case *parser.ParenTableExpr:
		return p.getDataSource(t.Expr, hints, scanVisibility)

	case *parser.AliasedTableExpr:
		// Alias clause: source AS alias(cols...)
		src, err := p.getDataSource(t.Expr, t.Hints, scanVisibility)
		if err != nil {
			return src, err
		}

		var tableAlias parser.TableName
		if t.As.Alias != "" {
			// If an alias was specified, use that.
			tableAlias.TableName = parser.Name(sqlbase.NormalizeName(t.As.Alias))
			src.info.sourceAliases = sourceAliases{
				tableAlias: fillColumnRange(0, len(src.info.sourceColumns)-1),
			}
		}
		colAlias := t.As.Cols

		if len(colAlias) > 0 {
			// Make a copy of the slice since we are about to modify the contents.
			src.info.sourceColumns = append(ResultColumns(nil), src.info.sourceColumns...)

			// The column aliases can only refer to explicit columns.
			for colIdx, aliasIdx := 0, 0; aliasIdx < len(colAlias); colIdx++ {
				if colIdx >= len(src.info.sourceColumns) {
					var srcName string
					if tableAlias.DatabaseName != "" {
						srcName = tableAlias.String()
					} else {
						srcName = tableAlias.TableName.String()
					}

					return planDataSource{}, errors.Errorf(
						"source %q has %d columns available but %d columns specified",
						srcName, aliasIdx, len(colAlias))
				}
				if src.info.sourceColumns[colIdx].hidden {
					continue
				}
				src.info.sourceColumns[colIdx].Name = string(colAlias[aliasIdx])
				aliasIdx++
			}
		}
		return src, nil

	default:
		return planDataSource{}, errors.Errorf("unsupported FROM type %T", src)
	}
}

// getTableScanOrViewPlan builds a planDataSource from a single data source
// clause (either a table or a view) in a SelectClause, expanding views out
// into subqueries.
func (p *planner) getTableScanOrViewPlan(
	tn *parser.TableName, hints *parser.IndexHints, scanVisibility scanVisibility,
) (planDataSource, error) {
	descFunc := p.getTableLease
	if p.asOf {
		// AS OF SYSTEM TIME queries need to fetch the table descriptor at the
		// specified time, and never lease anything. The proto transaction already
		// has its timestamps set correctly so mustGetTableDesc will fetch with the
		// correct timestamp.
		descFunc = p.mustGetTableDesc
	}
	desc, err := descFunc(tn)
	if err != nil {
		return planDataSource{}, err
	}

	if desc.IsView() {
		return p.getViewPlan(tn, desc)
	} else if !desc.IsTable() {
		return planDataSource{},
			errors.Errorf("unexpected table descriptor of type %s for %q", desc.TypeName(), tn)
	}

	// This name designates a real table.
	scan := p.Scan()
	if err := scan.initTable(p, desc, hints, scanVisibility); err != nil {
		return planDataSource{}, err
	}

	return planDataSource{
		info: newSourceInfoForSingleTable(*tn, scan.Columns()),
		plan: scan,
	}, nil
}

// getViewPlan builds a planDataSource for the view specified by the
// table name and descriptor, expanding out its subquery plan.
func (p *planner) getViewPlan(
	tn *parser.TableName, desc *sqlbase.TableDescriptor,
) (planDataSource, error) {
	// Parse the query as Traditional syntax because we know the query was
	// saved in the descriptor by printing it with parser.Format.
	stmt, err := parser.ParseOneTraditional(desc.ViewQuery)
	if err != nil {
		return planDataSource{}, errors.Wrapf(err, "failed to parse underlying query from view %q", tn)
	}
	sel, ok := stmt.(*parser.Select)
	if !ok {
		return planDataSource{},
			errors.Errorf("failed to parse underlying query from view %q as a select", tn)
	}
	// TODO(a-robinson): Support ORDER BY and LIMIT in views. Is it as simple as
	// just passing the entire select here or will inserting an ORDER BY in the
	// middle of a query plan break things?
	return p.getSubqueryPlan(sel.Select, makeResultColumns(desc.Columns))
}

// getSubqueryPlan builds a planDataSource for a select statement, including
// for simple VALUES statements.
func (p *planner) getSubqueryPlan(
	sel parser.SelectStatement, cols ResultColumns,
) (planDataSource, error) {
	plan, err := p.newPlan(sel, nil, false)
	if err != nil {
		return planDataSource{}, err
	}
	if len(cols) == 0 {
		cols = plan.Columns()
	}
	return planDataSource{
		info: newSourceInfoForSingleTable(parser.TableName{}, cols),
		plan: plan,
	}, nil
}

// expandStar returns the array of column metadata and name
// expressions that correspond to the expansion of a star.
func (src *dataSourceInfo) expandStar(
	v parser.VarName, ivarHelper parser.IndexedVarHelper,
) (columns ResultColumns, exprs []parser.TypedExpr, err error) {
	if len(src.sourceColumns) == 0 {
		return nil, nil, fmt.Errorf("cannot use %q without a FROM clause", v)
	}

	colSel := func(idx int) {
		col := src.sourceColumns[idx]
		if !col.hidden {
			ivar := ivarHelper.IndexedVar(idx)
			columns = append(columns, ResultColumn{Name: col.Name, Typ: ivar.ReturnType()})
			exprs = append(exprs, ivar)
		}
	}

	tableName := parser.TableName{}
	if a, ok := v.(*parser.AllColumnsSelector); ok {
		tableName = a.TableName
	}
	if tableName.Table() == "" {
		for i := 0; i < len(src.sourceColumns); i++ {
			colSel(i)
		}
	} else {
		norm := sqlbase.NormalizeTableName(tableName)

		qualifiedTn, err := src.checkDatabaseName(norm)
		if err != nil {
			return nil, nil, err
		}

		colRange, ok := src.sourceAliases[qualifiedTn]
		if !ok {
			return nil, nil, fmt.Errorf("table %q not found", tableName.String())
		}
		for _, i := range colRange {
			colSel(i)
		}
	}

	return columns, exprs, nil
}

// findUnaliasedColumn looks up the column specified by a VarName, not
// taking column renames into account (but table renames will be taken
// into account). That is, given a table "blah" with single column "y",
// findUnaliasedColumn("y") returns a valid index even in the context
// of:
//     SELECT * FROM blah as foo(x)
// If the VarName specifies a table name, only columns that have that
// name as their source alias are considered. If the VarName does not
// specify a table name, all columns in the data source are
// considered.  If no column is found, invalidColIdx is returned with
// no error.
func (p *planDataSource) findUnaliasedColumn(c *parser.ColumnItem) (colIdx int, err error) {
	colName := sqlbase.NormalizeName(c.ColumnName)
	tableName := sqlbase.NormalizeTableName(c.TableName)

	if tableName.Table() != "" {
		tn, err := p.info.checkDatabaseName(tableName)
		if err != nil {
			return invalidColIdx, nil
		}
		tableName = tn
	}

	colIdx = invalidColIdx
	planColumns := p.plan.Columns()

	selCol := func(colIdx int, idx int) (int, error) {
		col := planColumns[idx]
		if sqlbase.ReNormalizeName(col.Name) == colName {
			if colIdx != invalidColIdx {
				return invalidColIdx, fmt.Errorf("column reference %q is ambiguous", c)
			}
			colIdx = idx
		}
		return colIdx, nil
	}

	if tableName.Table() == "" {
		for idx := 0; idx < len(p.info.sourceColumns); idx++ {
			colIdx, err = selCol(colIdx, idx)
			if err != nil {
				return colIdx, err
			}
		}
	} else {
		colRange, ok := p.info.sourceAliases[tableName]
		if !ok {
			// A table name is specified, but there is no column with this
			// table name.
			return invalidColIdx, nil
		}
		for _, idx := range colRange {
			colIdx, err = selCol(colIdx, idx)
			if err != nil {
				return colIdx, err
			}
		}
	}

	return colIdx, nil
}

type multiSourceInfo []*dataSourceInfo

// checkDatabaseName checks whether the given TableName is unambiguous
// for the set of sources and if it is, qualifies the missing database name.
func (sources multiSourceInfo) checkDatabaseName(tn parser.TableName) (parser.TableName, error) {
	found := false
	if tn.DatabaseName == "" {
		// No database name yet. Try to find one.
		for _, src := range sources {
			for name := range src.sourceAliases {
				if name.TableName == tn.TableName {
					if found {
						return parser.TableName{}, fmt.Errorf("ambiguous source name: %q", tn.TableName)
					}
					tn.DatabaseName = name.DatabaseName
					found = true
				}
			}
		}
		if !found {
			return parser.TableName{}, fmt.Errorf("source name %q not found in FROM clause", tn.TableName)
		}
		return tn, nil
	}

	// Database given. Check that the name is unambiguous.
	for _, src := range sources {
		if _, ok := src.sourceAliases[tn]; ok {
			if found {
				return parser.TableName{}, fmt.Errorf("ambiguous source name: %q (within database %q)",
					tn.TableName, tn.DatabaseName)
			}
			found = true
		}
	}
	if !found {
		return parser.TableName{}, fmt.Errorf("table %q not selected in FROM clause", &tn)
	}
	return tn, nil
}

// checkDatabaseName checks whether the given TableName is unambiguous
// within this source and if it is, qualifies the missing database name.
func (src *dataSourceInfo) checkDatabaseName(tn parser.TableName) (parser.TableName, error) {
	found := false
	if tn.DatabaseName == "" {
		// No database name yet. Try to find one.
		for name := range src.sourceAliases {
			if name.TableName == tn.TableName {
				if found {
					return parser.TableName{}, fmt.Errorf("ambiguous source name: %q", tn.TableName)
				}
				tn.DatabaseName = name.DatabaseName
				found = true
			}
		}
		if !found {
			return parser.TableName{}, fmt.Errorf("source name %q not found in FROM clause", tn.TableName)
		}
		return tn, nil
	}

	// Database given. Check that the name is unambiguous.
	if _, ok := src.sourceAliases[tn]; ok {
		if found {
			return parser.TableName{}, fmt.Errorf("ambiguous source name: %q (within database %q)",
				tn.TableName, tn.DatabaseName)
		}
		found = true
	}

	if !found {
		return parser.TableName{}, fmt.Errorf("table %q not selected in FROM clause", &tn)
	}
	return tn, nil
}

// findColumn looks up the column specified by a ColumnItem. The
// function returns the index of the source in the multiSourceInfo
// array and the column index for the column array of that
// source. Returns invalid indices and an error if the source is not
// found or the name is ambiguous.
func (sources multiSourceInfo) findColumn(c *parser.ColumnItem) (srcIdx int, colIdx int, err error) {
	if len(c.Selector) > 0 {
		return invalidSrcIdx, invalidColIdx, util.UnimplementedWithIssueErrorf(8318, "compound types not supported yet: %q", c)
	}

	colName := sqlbase.NormalizeName(c.ColumnName)
	var tableName parser.TableName
	if c.TableName.Table() != "" {
		tableName = sqlbase.NormalizeTableName(c.TableName)

		tn, err := sources.checkDatabaseName(tableName)
		if err != nil {
			return invalidSrcIdx, invalidColIdx, err
		}
		tableName = tn

		// Propagate the discovered database name back to the original VarName.
		// (to clarify the output of e.g. EXPLAIN)
		c.TableName.DatabaseName = tableName.DatabaseName
	}

	colIdx = invalidColIdx
	for iSrc, src := range sources {
		findColHelper := func(src *dataSourceInfo, iSrc, srcIdx, colIdx int, idx int) (int, int, error) {
			col := src.sourceColumns[idx]
			if sqlbase.ReNormalizeName(col.Name) == colName {
				if colIdx != invalidColIdx {
					return invalidSrcIdx, invalidColIdx, fmt.Errorf("column reference %q is ambiguous", c)
				}
				srcIdx = iSrc
				colIdx = idx
			}
			return srcIdx, colIdx, nil
		}

		if tableName.Table() == "" {
			for idx := 0; idx < len(src.sourceColumns); idx++ {
				srcIdx, colIdx, err = findColHelper(src, iSrc, srcIdx, colIdx, idx)
				if err != nil {
					return srcIdx, colIdx, err
				}
			}
		} else {
			colRange, ok := src.sourceAliases[tableName]
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
	}

	if colIdx == invalidColIdx {
		return invalidSrcIdx, invalidColIdx, fmt.Errorf("column name %q not found", c)
	}

	return srcIdx, colIdx, nil
}

// concatDataSourceInfos creates a new dataSourceInfo that represents
// the side-by-side concatenation of the two data sources described by
// its arguments.  If it detects that a table alias appears on both
// sides, an ambiguity is reported.
func concatDataSourceInfos(left *dataSourceInfo, right *dataSourceInfo) (*dataSourceInfo, error) {
	aliases := make(sourceAliases)
	nColsLeft := len(left.sourceColumns)
	for alias, colRange := range right.sourceAliases {
		newRange := make(columnRange, len(colRange))
		for i, idx := range colRange {
			newRange[i] = idx + nColsLeft
		}
		aliases[alias] = newRange
	}
	for k, v := range left.sourceAliases {
		aliases[k] = v
	}

	columns := make(ResultColumns, 0, len(left.sourceColumns)+len(right.sourceColumns))
	columns = append(columns, left.sourceColumns...)
	columns = append(columns, right.sourceColumns...)

	return &dataSourceInfo{sourceColumns: columns, sourceAliases: aliases}, nil
}

// findTableAlias returns the first table alias providing the column
// index given as argument. The index must be valid.
func (src *dataSourceInfo) findTableAlias(colIdx int) parser.TableName {
	for alias, colRange := range src.sourceAliases {
		for _, idx := range colRange {
			if colIdx == idx {
				return alias
			}
		}
	}
	panic(fmt.Sprintf("no alias for position %d in %q", colIdx, src.sourceAliases))
}

func (src *dataSourceInfo) FormatVar(buf *bytes.Buffer, f parser.FmtFlags, colIdx int) {
	if f == parser.FmtQualify {
		tableAlias := src.findTableAlias(colIdx)
		if tableAlias.TableName != "" {
			if tableAlias.DatabaseName != "" {
				parser.FormatNode(buf, f, tableAlias.DatabaseName)
				buf.WriteByte('.')
			}
			parser.FormatNode(buf, f, tableAlias.TableName)
			buf.WriteByte('.')
		}
	}
	buf.WriteString(src.sourceColumns[colIdx].Name)
}
