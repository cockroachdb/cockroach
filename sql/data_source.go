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
	"fmt"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util"
)

// To understand dataSource below it is crucial to understand the
// meaning of a "data source" and its relationship to qnames/qvalues.
//
// A data source is an object that can deliver rows of column data,
// implemented in CockroachDB as an array of values.
// The defining property in a data source is that the columns in its
// result row arrays are always numbered from 0.
//
// From the language perspective, data sources are defined indirectly by:
// - the FROM clause in a SELECT statement
// - JOIN clauses within the FROM clause
// - the clause that follows INSERT INTO colName(Cols...)
// - the clause that follows UPSERT ....
// - the invisible data source defined by the original table row during
//   UPSERT, if it exists.
//
// Most expression (parser.Expr trees) in CockroachDB refer to a
// single data source. A notable exception is UPSERT, where expressions
// can refer to two sources one for the values being inserted, one for
// the original row data in the table for conflicting (already
// existing) rows.
//
// Meanwhile, qvalues in CockroachDB provide the interface between
// symbolic names in expressions (e.g. "f.x", called QualifiedNames,
// or qnames) and data sources. During evaluation, a qvalue must
// resolve to a column value. For a given qname there are thus two
// subsequent questions that must be answered:
//
// - which data source is the qname referring to? (when there is more than 1 source)
// - which 0-indexed column in that data source is the qname referring to?
//
// The qvalue must distinguish data sources because the same column index
// may refer to different columns in different data sources. For
// example in an UPSERT statement the qvalue for "excluded.x" could refer
// to column 0 in the (already existing) table row, whereas "src.x" could
// refer to column 0 in the valueNode that provides values to insert.
//
// Within this context, the infrastructure for data sources and qvalues
// is implemented as follows:
//
// - dataSourceInfo provides column metadata for exactly one data source
// - the columnRef in qvalues contains a link (pointer) to the
//   dataSourceInfo for its data source
// - qvalResolver (select_qvalue.go) is tasked with linking back qvalues with
//   their data source and column index.
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
//   When this happens, a qname of the form "excluded.x" must be resolved
//   by considering all the data sources; if there is more than one data
//   source providing the table name "excluded" (as in this case), an ambiguity
//   is reported.
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
//   When this happens, qnames of the form "f.x" in either WHERE,
//   select renders or other expression which can refer to the data
//   source do not refer to the "internal" data sources of the JOIN;
//   they always refer to the final result rows of the JOIN source as
//   a whole.
//
//   This implies that a single dataSourceInfo that provides metadata
//   for a complex JOIN clause must "know" which table name is
//   associated with each column in its result set.
//

type dataSourceInfo struct {
	// resultColumns which match the plan.Columns() 1-to-1. However the
	// column names might be different if the statement renames them
	// using AS.
	sourceColumns []ResultColumn

	// source table names for each column.
	// These often correspond to the original table names for each
	// columns but might be different if the statement renames
	// them using AS.
	sourceAliases sourceAliases
}

// planDataSource contains the data source information for data
// produced by a planNode.
type planDataSource struct {
	// info which describe the columns.
	info dataSourceInfo

	// plan which can be used to retrieve the data.
	plan planNode
}

type columnRange struct {
	firstIdx int
	lastIdx  int
}
type sourceAliases map[string]columnRange

// makeSourceInfoForSingleTable creates a simple dataSourceInfo
// which maps the same tableAlias to all columns.
func makeSourceInfoForSingleTable(tableAlias string, columns []ResultColumn) dataSourceInfo {
	norm := sqlbase.NormalizeName(tableAlias)
	return dataSourceInfo{
		sourceColumns: columns,
		sourceAliases: sourceAliases{norm: columnRange{firstIdx: 0, lastIdx: len(columns) - 1}},
	}
}

// getSources combines zero or more FROM sources into cross-joins.
func (p *planner) getSources(
	sources []parser.TableExpr, scanVisibility scanVisibility,
) (planDataSource, error) {
	n := len(sources)
	switch n {
	case 0:
		plan := &emptyNode{results: true}
		return planDataSource{
			info: makeSourceInfoForSingleTable("", plan.Columns()),
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

// getDataSource builds a planDataSource from a single data source clause
// (TableExpr) in a SelectClause.
func (p *planner) getDataSource(
	src parser.TableExpr,
	hints *parser.IndexHints,
	scanVisibility scanVisibility,
) (planDataSource, error) {
	switch t := src.(type) {
	case *parser.QualifiedName:
		// Usual case: a table.
		scan := p.Scan()
		tableName, err := scan.initTable(p, t, hints, scanVisibility)
		if err != nil {
			return planDataSource{}, err
		}

		return planDataSource{
			info: makeSourceInfoForSingleTable(tableName, scan.Columns()),
			plan: scan,
		}, nil

	case *parser.Subquery:
		// We have a subquery (this includes a simple "VALUES").
		plan, err := p.newPlan(t.Select, nil, false)
		if err != nil {
			return planDataSource{}, err
		}
		return planDataSource{
			info: makeSourceInfoForSingleTable("", plan.Columns()),
			plan: plan,
		}, nil

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
		src, err := p.getDataSource(t.Expr, t.Hints, scanVisibility)
		if err != nil {
			return src, err
		}

		var tableAlias string
		if t.As.Alias != "" {
			// If an alias was specified, use that.
			tableAlias = sqlbase.NormalizeName(string(t.As.Alias))
			src.info.sourceAliases = sourceAliases{
				tableAlias: columnRange{
					firstIdx: 0,
					lastIdx:  len(src.info.sourceColumns) - 1,
				},
			}
		}
		colAlias := t.As.Cols

		if len(colAlias) > 0 {
			// Make a copy of the slice since we are about to modify the contents.
			src.info.sourceColumns = append([]ResultColumn(nil), src.info.sourceColumns...)

			// The column aliases can only refer to explicit columns.
			for colIdx, aliasIdx := 0, 0; aliasIdx < len(colAlias); colIdx++ {
				if colIdx >= len(src.info.sourceColumns) {
					return planDataSource{}, util.Errorf(
						"table \"%s\" has %d columns available but %d columns specified",
						tableAlias, aliasIdx, len(colAlias))
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
		panic(fmt.Sprintf("unexpected TableExpr type: %T", t))
	}
}

// expandStar returns the array of column metadata and qname
// expressions that correspond to the expansion of a qname star.
func (src *dataSourceInfo) expandStar(
	qname *parser.QualifiedName, qvals qvalMap,
) (columns []ResultColumn, exprs []parser.TypedExpr, err error) {
	if len(src.sourceColumns) == 0 {
		return nil, nil, fmt.Errorf("\"%s\" with no tables specified is not valid", qname)
	}

	var colRange columnRange
	tableName := qname.Table()
	if tableName == "" {
		colRange = columnRange{firstIdx: 0, lastIdx: len(src.sourceColumns) - 1}
	} else {
		norm := sqlbase.NormalizeName(tableName)
		newRange, ok := src.sourceAliases[norm]
		if !ok {
			return nil, nil, fmt.Errorf("table \"%s\" not found", tableName)
		}
		colRange = newRange
	}

	for idx := colRange.firstIdx; idx <= colRange.lastIdx; idx++ {
		col := src.sourceColumns[idx]
		if col.hidden {
			continue
		}
		qval := qvals.getQVal(columnRef{src, idx})
		columns = append(columns, ResultColumn{Name: col.Name, Typ: qval.datum})
		exprs = append(exprs, qval)
	}

	if len(exprs) == 0 {
		return nil, nil, fmt.Errorf("table \"%s\" contains no selectable columns", tableName)
	}

	return columns, exprs, nil
}

// findUnaliasedColumn looks up the column specified by a qname, not
// taking column renames into account (but table renames will be taken
// into account).
func (p *planDataSource) findUnaliasedColumn(
	qname *parser.QualifiedName,
) (colIdx int, err error) {
	if err := qname.NormalizeColumnName(); err != nil {
		return invalidColIdx, err
	}

	colName := sqlbase.NormalizeName(qname.Column())
	tableName := sqlbase.NormalizeName(qname.Table())

	var colRange columnRange
	if tableName == "" {
		colRange = columnRange{firstIdx: 0, lastIdx: len(p.info.sourceColumns) - 1}
	} else {
		newRange, ok := p.info.sourceAliases[tableName]
		if !ok {
			return invalidColIdx, nil
		}
		colRange = newRange
	}

	colIdx = invalidColIdx
	planColumns := p.plan.Columns()
	for idx := colRange.firstIdx; idx <= colRange.lastIdx; idx++ {
		col := planColumns[idx]
		if sqlbase.NormalizeName(col.Name) == colName {
			if colIdx != invalidColIdx {
				return invalidColIdx, fmt.Errorf("column reference \"%s\" is ambiguous", qname)
			}
			colIdx = idx
		}
	}

	return colIdx, nil
}

type multiSourceInfo []*dataSourceInfo

// findColumn looks up the column specified by a qname. The qname
// will be normalized.
func (sources multiSourceInfo) findColumn(
	qname *parser.QualifiedName,
) (info *dataSourceInfo, colIdx int, err error) {
	if err := qname.NormalizeColumnName(); err != nil {
		return nil, invalidColIdx, err
	}

	// We can't resolve stars to a single column.
	if qname.IsStar() {
		panic("star qnames should really not be reaching this point!")
	}

	colName := sqlbase.NormalizeName(qname.Column())
	tableName := sqlbase.NormalizeName(qname.Table())

	colIdx = invalidColIdx
	for _, src := range sources {
		var colRange columnRange
		if tableName == "" {
			colRange = columnRange{firstIdx: 0, lastIdx: len(src.sourceColumns) - 1}
		} else {
			newRange, ok := src.sourceAliases[tableName]
			if !ok {
				// the data source "src" has no column for table tableName.
				continue
			}
			colRange = newRange
		}

		for idx := colRange.firstIdx; idx <= colRange.lastIdx; idx++ {
			col := src.sourceColumns[idx]
			if sqlbase.NormalizeName(col.Name) == colName {
				if colIdx != invalidColIdx {
					return nil, invalidColIdx, fmt.Errorf("column reference \"%s\" is ambiguous", qname)
				}
				info = src
				colIdx = idx
			}
		}
	}

	if colIdx == invalidColIdx {
		return nil, invalidColIdx, fmt.Errorf("qualified name \"%s\" not found", qname)
	}

	return info, colIdx, nil
}

// concatDataSourceInfos concatenates the column metadata
// for two data sources. If a table name appears on both sides,
// an ambiguity is reported. The members of the left dataSourceInfo
// are reused.
func concatDataSourceInfos(left dataSourceInfo, right dataSourceInfo) (dataSourceInfo, error) {
	nColsLeft := len(left.sourceColumns)
	for alias, colRange := range right.sourceAliases {
		if _, ok := left.sourceAliases[alias]; ok {
			return dataSourceInfo{}, fmt.Errorf("table name \"%s\" specified more than once", alias)
		}
		left.sourceAliases[alias] = columnRange{
			firstIdx: colRange.firstIdx + nColsLeft,
			lastIdx:  colRange.lastIdx + nColsLeft,
		}
	}
	left.sourceColumns = append(left.sourceColumns, right.sourceColumns...)
	return left, nil
}

// findTableAlias returns the table alias providing the column
// index given as argument. The index must be valid.
func (src *dataSourceInfo) findTableAlias(colIdx int) string {
	for alias, colRange := range src.sourceAliases {
		if colIdx >= colRange.firstIdx && colIdx <= colRange.lastIdx {
			return alias
		}
	}
	panic(fmt.Sprintf("no alias for position %d in %q", colIdx, src.sourceAliases))
}
