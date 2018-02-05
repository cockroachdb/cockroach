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

package sqlbase

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// To understand DataSourceInfo below it is crucial to understand the
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
// Most expressions (tree.Expr trees) in CockroachDB refer to a
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
// - DataSourceInfo provides column metadata for exactly one data source;
// - MultiSourceInfo is an array of one or more DataSourceInfo
// - the index in IndexedVars points to one of the columns in the
//   logical concatenation of all items in the MultiSourceInfo;
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
//   This implies that a single DataSourceInfo that provides metadata
//   for a complex JOIN clause must "know" which table name is
//   associated with each column in its result set.
//

// DataSourceInfo provides column metadata for exactly one data source.
type DataSourceInfo struct {
	// SourceColumns match the plan.Columns() 1-to-1. However the column
	// names might be different if the statement renames them using AS.
	SourceColumns ResultColumns

	// SourceAliases indicates to which table alias column ranges
	// belong.
	// These often correspond to the original table names for each
	// column but might be different if the statement renames
	// them using AS.
	SourceAliases SourceAliases

	// ColOffset is the offset of the first column in this DataSourceInfo in the
	// MultiSourceInfo array it is part of.
	// The value is populated and used during name resolution, and shouldn't get
	// touched by anything but the nameResolutionVisitor without care.
	ColOffset int

	// The number of backfill source columns. The backfill columns are
	// always the last columns from SourceColumns.
	NumBackfillColumns int
}

// SourceAlias associates a table name (alias) to a set of columns in the result
// row of a data source.
type SourceAlias struct {
	Name tree.TableName
	// ColumnSet identifies a non-empty set of columns in a
	// selection. This is used by DataSourceInfo.SourceAliases to map
	// table names to column ranges.
	ColumnSet util.FastIntSet
}

// InvalidSrcIdx is the srcIdx value returned by findColumn() when there is no match.
const InvalidSrcIdx = -1

// InvalidColIdx is the colIdx value returned by findColumn() when there is no match.
const InvalidColIdx = -1

func (src *DataSourceInfo) String() string {
	var buf bytes.Buffer
	for i := range src.SourceColumns {
		if i > 0 {
			buf.WriteByte('\t')
		}
		fmt.Fprintf(&buf, "%d", i)
	}
	buf.WriteString("\toutput column positions\n")
	for i, c := range src.SourceColumns {
		if i > 0 {
			buf.WriteByte('\t')
		}
		if c.Hidden {
			buf.WriteByte('*')
		}
		buf.WriteString(c.Name)
	}
	buf.WriteString("\toutput column names\n")
	for _, a := range src.SourceAliases {
		for i := range src.SourceColumns {
			if i > 0 {
				buf.WriteByte('\t')
			}
			if a.ColumnSet.Contains(i) {
				buf.WriteString("x")
			}
		}
		if a.Name == AnonymousTable {
			buf.WriteString("\t<anonymous table>")
		} else {
			fmt.Fprintf(&buf, "\t'%s'", a.Name.String())
		}
		fmt.Fprintf(&buf, " - %s\n", a.ColumnSet)
	}
	return buf.String()
}

// SourceAliases is an array of one or more SourceAlias.
type SourceAliases []SourceAlias

// SrcIdx looks up a source by qualified name and returns the index of the
// source (and whether we found one).
func (s SourceAliases) SrcIdx(name tree.TableName) (srcIdx int, found bool) {
	for i := range s {
		if s[i].Name.SchemaName == name.SchemaName && s[i].Name.TableName == name.TableName {
			return i, true
		}
	}
	return -1, false
}

// ColumnSet looks up a source by name and returns the column set (and
// whether we found the name).
func (s SourceAliases) ColumnSet(name tree.TableName) (_ util.FastIntSet, found bool) {
	idx, ok := s.SrcIdx(name)
	if !ok {
		return util.FastIntSet{}, false
	}
	return s[idx].ColumnSet, true
}

// AnonymousTable is the empty table name, used when a data source
// has no own name, e.g. VALUES, subqueries or the empty source.
var AnonymousTable = tree.TableName{}

// FillColumnRange creates a single range that refers to all the
// columns between firstIdx and lastIdx, inclusive.
func FillColumnRange(firstIdx, lastIdx int) util.FastIntSet {
	var res util.FastIntSet
	for i := firstIdx; i <= lastIdx; i++ {
		res.Add(i)
	}
	return res
}

// NewSourceInfoForSingleTable creates a simple DataSourceInfo
// which maps the same tableAlias to all columns.
func NewSourceInfoForSingleTable(tn tree.TableName, columns ResultColumns) *DataSourceInfo {
	return &DataSourceInfo{
		SourceColumns: columns,
		SourceAliases: SourceAliases{{Name: tn, ColumnSet: FillColumnRange(0, len(columns)-1)}},
	}
}

// ExpandStar returns the array of column metadata and name
// expressions that correspond to the expansion of a star.
func (src *DataSourceInfo) ExpandStar(
	v tree.VarName, ivarHelper tree.IndexedVarHelper,
) (columns ResultColumns, exprs []tree.TypedExpr, err error) {
	if len(src.SourceColumns) == 0 {
		return nil, nil, pgerror.NewErrorf(pgerror.CodeInvalidNameError,
			"cannot use %q without a FROM clause", tree.ErrString(v))
	}

	colSel := func(idx int) {
		col := src.SourceColumns[idx]
		if !col.Hidden {
			ivar := ivarHelper.IndexedVar(idx)
			columns = append(columns, ResultColumn{Name: col.Name, Typ: ivar.ResolvedType()})
			exprs = append(exprs, ivar)
		}
	}

	tableName := AnonymousTable
	if a, ok := v.(*tree.AllColumnsSelector); ok {
		tableName = a.TableName
	}
	if tableName.Table() == "" {
		for i := 0; i < len(src.SourceColumns); i++ {
			colSel(i)
		}
	} else {
		qualifiedTn, err := src.checkDatabaseName(tableName)
		if err != nil {
			return nil, nil, err
		}

		colSet, ok := src.SourceAliases.ColumnSet(qualifiedTn)
		if !ok {
			return nil, nil, NewUndefinedRelationError(&tableName)
		}
		for i, ok := colSet.Next(0); ok; i, ok = colSet.Next(i + 1) {
			colSel(i)
		}
	}

	return columns, exprs, nil
}

// MultiSourceInfo is an array of one or more DataSourceInfo.
type MultiSourceInfo []*DataSourceInfo

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

// checkDatabaseName checks whether the given TableName is unambiguous
// for the set of sources and if it is, qualifies the missing database name.
func (sources MultiSourceInfo) checkDatabaseName(tn tree.TableName) (tree.TableName, error) {
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
func (src *DataSourceInfo) checkDatabaseName(tn tree.TableName) (tree.TableName, error) {
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

func findColHelper(
	sources MultiSourceInfo,
	src *DataSourceInfo,
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
			return InvalidSrcIdx, InvalidColIdx,
				pgerror.NewErrorf(pgerror.CodeInvalidColumnReferenceError,
					"column %q is being backfilled", tree.ErrString(c))
		}
		if colIdx != InvalidColIdx {
			colString := tree.ErrString(c)
			var msgBuf bytes.Buffer
			sep := ""
			fmtCandidate := func(alias *SourceAlias) {
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
			return InvalidSrcIdx, InvalidColIdx, pgerror.NewErrorf(pgerror.CodeAmbiguousColumnError,
				"column reference %q is ambiguous (candidates: %s)", colString, msgBuf.String())
		}
		srcIdx = iSrc
		colIdx = idx
	}
	return srcIdx, colIdx, nil
}

// FindColumn looks up the column specified by a ColumnItem. The
// function returns the index of the source in the MultiSourceInfo
// array and the column index for the column array of that
// source. Returns invalid indices and an error if the source is not
// found or the name is ambiguous.
func (sources MultiSourceInfo) FindColumn(c *tree.ColumnItem) (srcIdx int, colIdx int, err error) {
	colName := string(c.ColumnName)
	var tableName tree.TableName
	if c.TableName.Table() != "" {
		tn, err := sources.checkDatabaseName(c.TableName)
		if err != nil {
			return InvalidSrcIdx, InvalidColIdx, err
		}
		tableName = tn

		// Propagate the discovered database name back to the original VarName.
		// (to clarify the output of e.g. EXPLAIN)
		c.TableName.SchemaName = tableName.SchemaName
	}

	colIdx = InvalidColIdx
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

	if colIdx == InvalidColIdx && tableName.Table() == "" {
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

	if colIdx == InvalidColIdx {
		return InvalidSrcIdx, InvalidColIdx,
			pgerror.NewErrorf(pgerror.CodeUndefinedColumnError,
				"column name %q not found", tree.ErrString(c))
	}

	return srcIdx, colIdx, nil
}

// findTableAlias returns the first table alias providing the column
// index given as argument. The index must be valid.
func (src *DataSourceInfo) findTableAlias(colIdx int) (tree.TableName, bool) {
	for _, alias := range src.SourceAliases {
		if alias.ColumnSet.Contains(colIdx) {
			return alias.Name, true
		}
	}
	return AnonymousTable, false
}

type varFormatter struct {
	TableName  tree.TableName
	ColumnName tree.Name
}

// Format implements the NodeFormatter interface.
func (c *varFormatter) Format(ctx *tree.FmtCtx) {
	if ctx.HasFlags(tree.FmtShowTableAliases) && c.TableName.TableName != "" {
		if c.TableName.SchemaName != "" {
			ctx.FormatNode(&c.TableName.SchemaName)
			ctx.WriteByte('.')
		}

		ctx.FormatNode(&c.TableName.TableName)
		ctx.WriteByte('.')
	}
	ctx.FormatNode(&c.ColumnName)
}

// NodeFormatter returns a tree.NodeFormatter that, when formatted,
// represents the object at the input column index.
func (src *DataSourceInfo) NodeFormatter(colIdx int) tree.NodeFormatter {
	var ret varFormatter
	ret.ColumnName = tree.Name(src.SourceColumns[colIdx].Name)
	if tableAlias, found := src.findTableAlias(colIdx); found {
		ret.TableName = tableAlias
	}
	return &ret
}
