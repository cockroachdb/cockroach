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
	"bytes"
	"fmt"
	"math"

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

	// The table columns, possibly including ones currently in schema changes.
	cols []sqlbase.ColumnDescriptor
	// There is a 1-1 correspondence between cols and resultColumns.
	resultColumns []ResultColumn
	// Contains values for the current row. There is a 1-1 correspondence
	// between resultColumns and values in row.
	row parser.DTuple
	// For each column in resultColumns, indicates if the value is needed (used
	// as an optimization when the upper layer doesn't need all values).
	valNeededForCol []bool

	// Map used to get the index for columns in cols.
	colIdxMap map[sqlbase.ColumnID]int

	spans            []sqlbase.Span
	isSecondaryIndex bool
	reverse          bool
	ordering         orderingInfo

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
	limitSoft bool
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
	// Either a limitNode or EXPLAIN is pushing a limit down onto this
	// node. The special value math.MaxInt64 means "no limit".
	if numRows != math.MaxInt64 {
		n.limitHint = numRows
		// If we have a filter, some of the rows we retrieve may not pass the
		// filter so the limit becomes "soft".
		n.limitSoft = soft || n.filter != nil
	}
}

func (n *scanNode) expandPlan() error {
	return n.p.expandSubqueryPlans(n.filter)
}

func (n *scanNode) Start() error {
	err := n.fetcher.Init(&n.desc, n.colIdxMap, n.index, n.reverse, n.isSecondaryIndex, n.cols,
		n.valNeededForCol)
	if err != nil {
		return err
	}

	return n.p.startSubqueryPlans(n.filter)
}

// initScan sets up the rowFetcher and starts a scan.
func (n *scanNode) initScan() error {
	if len(n.spans) == 0 {
		// If no spans were specified retrieve all of the keys that start with our
		// index key prefix. This isn't needed for the fetcher, but it is for
		// other external users of n.spans.
		start := roachpb.Key(sqlbase.MakeIndexKeyPrefix(&n.desc, n.index.ID))
		n.spans = append(n.spans, sqlbase.Span{Start: start, End: start.PrefixEnd()})
	}

	limitHint := n.limitHint
	if limitHint != 0 && n.limitSoft {
		// Read a multiple of the limit if the limit is "soft".
		limitHint *= 2
	}

	if err := n.fetcher.StartScan(n.p.txn, n.spans, limitHint); err != nil {
		return err
	}
	n.scanInitialized = true
	return nil
}

// debugNext is a helper function used by Next() when in explainDebug mode.
func (n *scanNode) debugNext() (bool, error) {
	// In debug mode, we output a set of debug values for each key.
	n.debugVals.rowIdx = n.rowIndex
	var err error
	n.debugVals.key, n.debugVals.value, n.row, err = n.fetcher.NextKeyDebug()
	if err != nil || n.debugVals.key == "" {
		return false, err
	}

	if n.row != nil {
		passesFilter, err := sqlbase.RunFilter(n.filter, &n.p.evalCtx)
		if err != nil {
			return false, err
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
	return true, nil
}

func (n *scanNode) Next() (bool, error) {
	tracing.AnnotateTrace()
	if !n.scanInitialized {
		if err := n.initScan(); err != nil {
			return false, err
		}
	}

	if n.explain == explainDebug {
		return n.debugNext()
	}

	// We fetch one row at a time until we find one that passes the filter.
	for {
		var err error
		n.row, err = n.fetcher.NextRow()
		if err != nil || n.row == nil {
			return false, err
		}
		passesFilter, err := sqlbase.RunFilter(n.filter, &n.p.evalCtx)
		if err != nil {
			return false, err
		}
		if passesFilter {
			return true, nil
		}
	}
}

func (n *scanNode) ExplainPlan(_ bool) (name, description string, children []planNode) {
	if n.reverse {
		name = "revscan"
	} else {
		name = "scan"
	}
	var desc bytes.Buffer
	fmt.Fprintf(&desc, "%s@%s", n.desc.Name, n.index.Name)
	spans := sqlbase.PrettySpans(n.spans, 2)
	if spans != "" {
		fmt.Fprintf(&desc, " %s", spans)
	}
	if n.limitHint > 0 && !n.limitSoft {
		if n.limitHint == 1 {
			desc.WriteString(" (max 1 row)")
		} else {
			fmt.Fprintf(&desc, " (max %d rows)", n.limitHint)
		}
	}

	subplans := n.p.collectSubqueryPlans(n.filter, nil)

	return name, desc.String(), subplans
}

func (n *scanNode) ExplainTypes(regTypes func(string, string)) {
	if n.filter != nil {
		regTypes("filter", parser.AsStringWithFlags(n.filter, parser.FmtShowTypes))
	}
}

// Initializes a scanNode with a tableName. Returns the table or index name that can be used for
// fully-qualified columns if an alias is not specified.
func (n *scanNode) initTable(
	p *planner,
	tableName *parser.QualifiedName,
	indexHints *parser.IndexHints,
	scanVisibility scanVisibility,
) (string, error) {
	descFunc := p.getTableLease
	if p.asOf {
		// AS OF SYSTEM TIME queries need to fetch the table descriptor at the
		// specified time, and never lease anything. The proto transaction already
		// has its timestamps set correctly so mustGetTableDesc will fetch with the
		// correct timestamp.
		descFunc = p.mustGetTableDesc
	}
	desc, err := descFunc(tableName)
	if err != nil {
		return "", err
	}
	n.desc = *desc

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
				return "", fmt.Errorf("index \"%s\" not found", indexName)
			}
		}
	}
	n.noIndexJoin = (indexHints != nil && indexHints.NoIndexJoin)
	n.initDescDefaults(scanVisibility)
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
func (n *scanNode) initDescDefaults(scanVisibility scanVisibility) {
	n.index = &n.desc.PrimaryIndex
	n.cols = make([]sqlbase.ColumnDescriptor, 0, len(n.desc.Columns)+len(n.desc.Mutations))
	switch scanVisibility {
	case publicColumns:
		n.cols = append(n.cols, n.desc.Columns...)
	case publicAndNonPublicColumns:
		n.cols = append(n.cols, n.desc.Columns...)
		for _, mutation := range n.desc.Mutations {
			if c := mutation.GetColumn(); c != nil {
				n.cols = append(n.cols, *c)
			}
		}
	}
	n.resultColumns = makeResultColumns(n.cols)
	n.colIdxMap = make(map[sqlbase.ColumnID]int, len(n.cols))
	for i, c := range n.cols {
		n.colIdxMap[c.ID] = i
	}
	n.valNeededForCol = make([]bool, len(n.cols))
	for i := range n.cols {
		n.valNeededForCol[i] = true
	}
	n.row = make([]parser.Datum, len(n.cols))
	n.filterVars = parser.MakeIndexedVarHelper(n, len(n.cols))
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

func (n *scanNode) IndexedVarEval(idx int, ctx *parser.EvalContext) (parser.Datum, error) {
	return n.row[idx].Eval(ctx)
}

func (n *scanNode) IndexedVarReturnType(idx int) parser.Datum {
	return n.resultColumns[idx].Typ.ReturnType()
}

func (n *scanNode) IndexedVarString(idx int) string {
	return n.resultColumns[idx].Name
}

// scanVisibility represents which table columns should be included in a scan.
type scanVisibility int

const (
	publicColumns             scanVisibility = 0
	publicAndNonPublicColumns scanVisibility = 1
)
