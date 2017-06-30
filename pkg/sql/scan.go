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

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
	resultColumns sqlbase.ResultColumns
	// Contains values for the current row. There is a 1-1 correspondence
	// between resultColumns and values in row.
	row parser.Datums
	// For each column in resultColumns, indicates if the value is
	// needed (used as an optimization when the upper layer doesn't need
	// all values).
	// TODO(radu/knz): currently the optimization always loads the
	// entire row from KV and only skips unnecessary decodes to
	// Datum. Investigate whether performance is to be gained (e.g. for
	// tables with wide rows) by reading only certain columns from KV
	// using point lookups instead of a single range lookup for the
	// entire row.
	valNeededForCol []bool

	// Map used to get the index for columns in cols.
	colIdxMap map[sqlbase.ColumnID]int

	spans            []roachpb.Span
	isSecondaryIndex bool
	reverse          bool
	ordering         orderingInfo

	explain   explainMode
	rowIndex  int64 // the index of the current row
	debugVals debugValues

	// filter that can be evaluated using only this table/index; it contains
	// parser.IndexedVar leaves generated using filterVars.
	filter     parser.TypedExpr
	filterVars parser.IndexedVarHelper

	scanInitialized bool
	fetcher         sqlbase.RowFetcher

	// if non-zero, hardLimit indicates that the scanNode only needs to provide
	// this many rows (after applying any filter). It is a "hard" guarantee that
	// Next will only be called this many times.
	hardLimit int64
	// if non-zero, softLimit is an estimation that only this many rows (after
	// applying any filter) might be needed. It is a (potentially optimistic)
	// "hint". If hardLimit is set (non-zero), softLimit must be unset (zero).
	softLimit int64

	disableBatchLimits bool

	scanVisibility scanVisibility
	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	noCopy util.NoCopy
}

func (p *planner) Scan() *scanNode {
	return &scanNode{p: p}
}

func (n *scanNode) Columns() sqlbase.ResultColumns {
	return n.resultColumns
}

func (n *scanNode) Ordering() orderingInfo {
	return n.ordering
}

func (n *scanNode) Values() parser.Datums {
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

// disableBatchLimit disables the kvfetcher batch limits. Used for index-join,
// where we scan batches of unordered spans.
func (n *scanNode) disableBatchLimit() {
	n.disableBatchLimits = true
	n.hardLimit = 0
	n.softLimit = 0
}

func (n *scanNode) Start(context.Context) error {
	return n.fetcher.Init(&n.desc, n.colIdxMap, n.index, n.reverse, n.isSecondaryIndex, n.cols,
		n.valNeededForCol, false /* returnRangeInfo */)
}

func (n *scanNode) Close(context.Context) {}

// initScan sets up the rowFetcher and starts a scan.
func (n *scanNode) initScan(ctx context.Context) error {
	limitHint := n.limitHint()
	if err := n.fetcher.StartScan(ctx, n.p.txn, n.spans, !n.disableBatchLimits, limitHint); err != nil {
		return err
	}
	n.scanInitialized = true
	return nil
}

func (n *scanNode) limitHint() int64 {
	var limitHint int64
	if n.hardLimit != 0 {
		limitHint = n.hardLimit
		if !isFilterTrue(n.filter) {
			// The limit is hard, but it applies after the filter; read a multiple of
			// the limit to avoid needing a second batch. The multiple should be an
			// estimate for the selectivity of the filter, but we have no way of
			// calculating that right now.
			limitHint *= 2
		}
	} else {
		// Like above, read a multiple of the limit when the limit is "soft".
		limitHint = n.softLimit * 2
	}
	return limitHint
}

// debugNext is a helper function used by Next() when in explainDebug mode.
func (n *scanNode) debugNext(ctx context.Context) (bool, error) {
	if n.hardLimit > 0 && n.rowIndex >= n.hardLimit {
		return false, nil
	}

	// In debug mode, we output a set of debug values for each key.
	n.debugVals.rowIdx = int(n.rowIndex)
	var err error
	var encRow sqlbase.EncDatumRow
	n.debugVals.key, n.debugVals.value, encRow, err = n.fetcher.NextKeyDebug(ctx)
	if err != nil || n.debugVals.key == "" {
		return false, err
	}
	if encRow == nil {
		n.row = nil
		n.debugVals.output = debugValuePartial
		return true, nil
	}
	datums := make(parser.Datums, len(encRow))
	var da sqlbase.DatumAlloc

	if err := sqlbase.EncDatumRowToDatums(datums, encRow, &da); err != nil {
		return false, errors.Errorf("Could not decode row: %v", err)
	}
	n.row = datums
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
	return true, nil
}

func (n *scanNode) Next(ctx context.Context) (bool, error) {
	tracing.AnnotateTrace()
	if !n.scanInitialized {
		if err := n.initScan(ctx); err != nil {
			return false, err
		}
	}

	if n.explain == explainDebug {
		return n.debugNext(ctx)
	}

	// We fetch one row at a time until we find one that passes the filter.
	for n.hardLimit == 0 || n.rowIndex < n.hardLimit {
		var err error
		n.row, err = n.fetcher.NextRowDecoded(ctx)
		if err != nil || n.row == nil {
			return false, err
		}
		passesFilter, err := sqlbase.RunFilter(n.filter, &n.p.evalCtx)
		if err != nil {
			return false, err
		}
		if passesFilter {
			n.rowIndex++
			return true, nil
		}
	}
	return false, nil
}

// Initializes a scanNode with a table descriptor.
func (n *scanNode) initTable(
	p *planner,
	desc *sqlbase.TableDescriptor,
	indexHints *parser.IndexHints,
	scanVisibility scanVisibility,
	wantedColumns []parser.ColumnID,
) error {
	n.desc = *desc

	if !p.skipSelectPrivilegeChecks {
		if err := p.CheckPrivilege(&n.desc, privilege.SELECT); err != nil {
			return err
		}
	}

	if indexHints != nil {
		if err := n.lookupSpecifiedIndex(indexHints); err != nil {
			return err
		}
	}
	n.noIndexJoin = (indexHints != nil && indexHints.NoIndexJoin)
	return n.initDescDefaults(scanVisibility, wantedColumns)
}

func (n *scanNode) lookupSpecifiedIndex(indexHints *parser.IndexHints) error {
	if indexHints.Index != "" {
		// Search index by name.
		indexName := indexHints.Index.Normalize()
		if indexName == parser.ReNormalizeName(n.desc.PrimaryIndex.Name) {
			n.specifiedIndex = &n.desc.PrimaryIndex
		} else {
			for i := range n.desc.Indexes {
				if indexName == parser.ReNormalizeName(n.desc.Indexes[i].Name) {
					n.specifiedIndex = &n.desc.Indexes[i]
					break
				}
			}
		}
		if n.specifiedIndex == nil {
			return errors.Errorf("index \"%s\" not found", indexName)
		}
	} else if indexHints.IndexID != 0 {
		// Search index by ID.
		if n.desc.PrimaryIndex.ID == sqlbase.IndexID(indexHints.IndexID) {
			n.specifiedIndex = &n.desc.PrimaryIndex
		} else {
			for i := range n.desc.Indexes {
				if n.desc.Indexes[i].ID == sqlbase.IndexID(indexHints.IndexID) {
					n.specifiedIndex = &n.desc.Indexes[i]
					break
				}
			}
		}
		if n.specifiedIndex == nil {
			return errors.Errorf("index %d not found", indexHints.IndexID)
		}
	}
	return nil
}

// Either pick all columns or just those selected.
func filterColumns(
	dst []sqlbase.ColumnDescriptor, wantedColumns []parser.ColumnID, src []sqlbase.ColumnDescriptor,
) ([]sqlbase.ColumnDescriptor, error) {
	if wantedColumns == nil {
		dst = append(dst, src...)
	} else {
		for _, wc := range wantedColumns {
			found := false
			for _, c := range src {
				if c.ID == sqlbase.ColumnID(wc) {
					dst = append(dst, c)
					found = true
					break
				}
			}
			if !found {
				return nil, errors.Errorf("column %d does not exist", wc)
			}
		}
		dst = appendUnselectedColumns(dst, wantedColumns, src)
	}
	return dst, nil
}

// appendUnselectedColumns adds into dst all the column descriptors
// from src that are not already listed in wantedColumns. The added
// columns, if any, are marked "hidden".
func appendUnselectedColumns(
	dst []sqlbase.ColumnDescriptor, wantedColumns []parser.ColumnID, src []sqlbase.ColumnDescriptor,
) []sqlbase.ColumnDescriptor {
	for _, c := range src {
		found := false
		for _, wc := range wantedColumns {
			if sqlbase.ColumnID(wc) == c.ID {
				found = true
				break
			}
		}
		if !found {
			col := c
			col.Hidden = true
			dst = append(dst, col)
		}
	}
	return dst
}

// Initializes the column structures.
// An error may be returned only if wantedColumns is set.
func (n *scanNode) initDescDefaults(
	scanVisibility scanVisibility, wantedColumns []parser.ColumnID,
) error {
	n.scanVisibility = scanVisibility
	n.index = &n.desc.PrimaryIndex
	n.cols = make([]sqlbase.ColumnDescriptor, 0, len(n.desc.Columns)+len(n.desc.Mutations))
	var err error
	n.cols, err = filterColumns(n.cols, wantedColumns, n.desc.Columns)
	if err != nil {
		return err
	}
	switch scanVisibility {
	case publicColumns:
		// Mutations are invisible.
	case publicAndNonPublicColumns:
		for _, mutation := range n.desc.Mutations {
			if c := mutation.GetColumn(); c != nil {
				col := *c
				// Even if the column is non-nullable it can be null in the
				// middle of a schema change.
				col.Nullable = true
				n.cols = append(n.cols, col)
			}
		}
	}
	n.resultColumns = sqlbase.ResultColumnsFromColDescs(n.cols)
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
	return nil
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

func (n *scanNode) Spans(ctx context.Context) (reads, writes roachpb.Spans, err error) {
	return n.spans, nil, nil
}

// scanNode implements parser.IndexedVarContainer.
var _ parser.IndexedVarContainer = &scanNode{}

func (n *scanNode) IndexedVarEval(idx int, ctx *parser.EvalContext) (parser.Datum, error) {
	return n.row[idx].Eval(ctx)
}

func (n *scanNode) IndexedVarResolvedType(idx int) parser.Type {
	return n.resultColumns[idx].Typ
}

func (n *scanNode) IndexedVarFormat(buf *bytes.Buffer, f parser.FmtFlags, idx int) {
	parser.Name(n.resultColumns[idx].Name).Format(buf, f)
}

// scanVisibility represents which table columns should be included in a scan.
type scanVisibility int

const (
	publicColumns             scanVisibility = 0
	publicAndNonPublicColumns scanVisibility = 1
)
