// Copyright 2015 The Cockroach Authors.
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
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/tracing"
)

// A scanNode handles scanning over the key/value pairs for a table and
// reconstructing them into rows.
type scanNode struct {
	p     *planner
	desc  sqlbase.TableDescriptor
	index *sqlbase.IndexDescriptor

	// Set if an index was explicitly specified.
	specifiedIndex *sqlbase.IndexDescriptor
	// Set if the NO_INDEX_JOIN hint was given.
	noIndexJoin bool

	// There is a 1-1 correspondence between desc.Columns and resultColumns.
	resultColumns []ResultColumn
	// Contains values for the current row. There is a 1-1 correspondence
	// between resultColumns and values in row.
	row parser.DTuple
	// For each column in resultColumns, indicates if the value is needed (used
	// as an optimization when the upper layer doesn't need all values).
	valNeededForCol []bool

	// Map used to get the index for columns in desc.Columns.
	colIdxMap map[sqlbase.ColumnID]int

	spans            []sqlbase.Span
	isSecondaryIndex bool
	reverse          bool
	ordering         orderingInfo
	err              error

	explain   explainMode
	rowIndex  int // the index of the current row
	debugVals debugValues

	// filter that can be evaluated using only this table/index; it contains
	// parser.IndexedVar leaves generated using filterVars.
	filter     parser.TypedExpr
	filterVars parser.IndexedVarHelper

	scanInitialized bool
	fetcher         sqlbase.RowFetcher

	limitHint int64
}

func (p *planner) Scan() *scanNode {
	return &scanNode{p: p}
}

func (n *scanNode) Columns() []ResultColumn {
	return n.resultColumns
}

func (n *scanNode) Ordering() orderingInfo {
	return n.ordering
}

func (n *scanNode) Values() parser.DTuple {
	return n.row
}

func (n *scanNode) MarkDebug(mode explainMode) {
	if mode != explainDebug {
		panic(fmt.Sprintf("unknown debug mode %d", mode))
	}
	n.explain = mode
}

func (n *scanNode) DebugValues() debugValues {
	if n.explain != explainDebug {
		panic(fmt.Sprintf("node not in debug mode (mode %d)", n.explain))
	}
	return n.debugVals
}

func (n *scanNode) SetLimitHint(numRows int64, soft bool) {
	n.limitHint = numRows
	if soft || n.filter != nil {
		// Read a multiple of the limit if the limit is "soft".
		n.limitHint *= 2
	}
}

func (n *scanNode) expandPlan() error {
	return nil
}

func (n *scanNode) Start() error {
	if err := n.fetcher.Init(&n.desc, n.colIdxMap, n.index, n.reverse,
		n.isSecondaryIndex, n.valNeededForCol); err != nil {
		return err
	}

	return nil
}

// initScan sets up the rowFetcher and starts a scan. On error, sets n.err and
// returns false.
func (n *scanNode) initScan() (success bool) {

	if len(n.spans) == 0 {
		// If no spans were specified retrieve all of the keys that start with our
		// index key prefix. This isn't needed for the fetcher, but it is for
		// other external users of n.spans.
		start := roachpb.Key(sqlbase.MakeIndexKeyPrefix(n.desc.ID, n.index.ID))
		n.spans = append(n.spans, sqlbase.Span{Start: start, End: start.PrefixEnd()})
	}

	n.err = n.fetcher.StartScan(n.p.txn, n.spans, n.limitHint)
	if n.err != nil {
		return false
	}
	n.scanInitialized = true
	return true
}

// debugNext is a helper function used by Next() when in explainDebug mode.
func (n *scanNode) debugNext() bool {
	// In debug mode, we output a set of debug values for each key.
	n.debugVals.rowIdx = n.rowIndex
	n.debugVals.key, n.debugVals.value, n.row, n.err = n.fetcher.NextKeyDebug()
	if n.err != nil || n.debugVals.key == "" {
		return false
	}

	if n.row != nil {
		passesFilter, err := sqlbase.RunFilter(n.filter, n.p.evalCtx)
		if err != nil {
			n.err = err
			return false
		}
		if passesFilter {
			n.debugVals.output = debugValueRow
		} else {
			n.debugVals.output = debugValueFiltered
		}
		n.rowIndex++
	} else {
		n.debugVals.output = debugValuePartial
	}
	return true
}

func (n *scanNode) Next() bool {
	tracing.AnnotateTrace()

	if n.err != nil {
		return false
	}

	if !n.scanInitialized && !n.initScan() {
		// Hit error during initScan
		return false
	}

	if n.explain == explainDebug {
		return n.debugNext()
	}

	// We fetch one row at a time until we find one that passes the filter.
	for {
		n.row, n.err = n.fetcher.NextRow()
		if n.err != nil || n.row == nil {
			return false
		}
		passesFilter, err := sqlbase.RunFilter(n.filter, n.p.evalCtx)
		if err != nil {
			n.err = err
			return false
		}
		if passesFilter {
			return true
		}
	}
}

func (n *scanNode) Err() error {
	return n.err
}

func (n *scanNode) ExplainPlan(_ bool) (name, description string, children []planNode) {
	if n.reverse {
		name = "revscan"
	} else {
		name = "scan"
	}
	description = fmt.Sprintf("%s@%s %s", n.desc.Name, n.index.Name, sqlbase.PrettySpans(n.spans, 2))
	return name, description, nil
}

func (n *scanNode) ExplainTypes(regTypes func(string, string)) {
	if n.filter != nil {
		regTypes("filter", parser.AsStringWithFlags(n.filter, parser.FmtShowTypes))
	}
}

// Initializes a scanNode with a tableName. Returns the table or index name that can be used for
// fully-qualified columns if an alias is not specified.
func (n *scanNode) initTable(
	p *planner, tableName *parser.QualifiedName, indexHints *parser.IndexHints,
) (string, error) {
	var err error
	n.desc, err = p.getTableLease(tableName)
	if err != nil {
		n.err = err
		return "", n.err
	}

	if err := p.checkPrivilege(&n.desc, privilege.SELECT); err != nil {
		return "", err
	}

	alias := n.desc.Name

	if indexHints != nil && indexHints.Index != "" {
		indexName := sqlbase.NormalizeName(string(indexHints.Index))
		if indexName == sqlbase.NormalizeName(n.desc.PrimaryIndex.Name) {
			n.specifiedIndex = &n.desc.PrimaryIndex
		} else {
			for i := range n.desc.Indexes {
				if indexName == sqlbase.NormalizeName(n.desc.Indexes[i].Name) {
					n.specifiedIndex = &n.desc.Indexes[i]
					break
				}
			}
			if n.specifiedIndex == nil {
				n.err = fmt.Errorf("index \"%s\" not found", indexName)
				return "", n.err
			}
		}
	}
	n.noIndexJoin = (indexHints != nil && indexHints.NoIndexJoin)
	n.initDescDefaults()
	return alias, nil
}

// setNeededColumns sets the flags indicating which columns are needed by the upper layer.
func (n *scanNode) setNeededColumns(needed []bool) {
	if len(needed) != len(n.valNeededForCol) {
		panic(fmt.Sprintf("invalid setNeededColumns argument (len %d instead of %d): %v",
			len(needed), len(n.valNeededForCol), needed))
	}
	copy(n.valNeededForCol, needed)
}

// Initializes the column structures.
func (n *scanNode) initDescDefaults() {
	n.index = &n.desc.PrimaryIndex
	cols := n.desc.Columns
	n.resultColumns = makeResultColumns(cols)
	n.colIdxMap = make(map[sqlbase.ColumnID]int, len(cols))
	for i, c := range cols {
		n.colIdxMap[c.ID] = i
	}
	n.valNeededForCol = make([]bool, len(cols))
	for i := range cols {
		n.valNeededForCol[i] = true
	}
	n.row = make([]parser.Datum, len(cols))
	n.filterVars = parser.MakeIndexedVarHelper(n, len(cols))
}

// initOrdering initializes the ordering info using the selected index. This
// must be called after index selection is performed.
func (n *scanNode) initOrdering(exactPrefix int) {
	if n.index == nil {
		return
	}
	n.ordering = n.computeOrdering(n.index, exactPrefix, n.reverse)
}

// computeOrdering calculates ordering information for table columns assuming that:
//    - we scan a given index (potentially in reverse order), and
//    - the first `exactPrefix` columns of the index each have an exact (single value) match
//      (see orderingInfo).
func (n *scanNode) computeOrdering(
	index *sqlbase.IndexDescriptor, exactPrefix int, reverse bool,
) orderingInfo {
	var ordering orderingInfo

	columnIDs, dirs := index.FullColumnIDs()

	for i, colID := range columnIDs {
		idx, ok := n.colIdxMap[colID]
		if !ok {
			panic(fmt.Sprintf("index refers to unknown column id %d", colID))
		}
		if i < exactPrefix {
			ordering.addExactMatchColumn(idx)
		} else {
			dir := dirs[i]
			if reverse {
				dir = dir.Reverse()
			}
			ordering.addColumn(idx, dir)
		}
	}
	// We included any implicit columns, so the results are unique.
	ordering.unique = true
	return ordering
}

// scanNode implements parser.IndexedVarContainer.
var _ parser.IndexedVarContainer = &scanNode{}

func (n *scanNode) IndexedVarEval(idx int, ctx parser.EvalContext) (parser.Datum, error) {
	return n.row[idx].Eval(ctx)
}

func (n *scanNode) IndexedVarReturnType(idx int) parser.Datum {
	return n.resultColumns[idx].Typ.ReturnType()
}

func (n *scanNode) IndexedVarString(idx int) string {
	return string(n.resultColumns[idx].Name)
}
