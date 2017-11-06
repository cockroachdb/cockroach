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

package sql

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

var scanNodePool = sync.Pool{
	New: func() interface{} {
		return &scanNode{}
	},
}

// A scanNode handles scanning over the key/value pairs for a table and
// reconstructing them into rows.
type scanNode struct {
	desc  *sqlbase.TableDescriptor
	index *sqlbase.IndexDescriptor

	// Set if an index was explicitly specified.
	specifiedIndex *sqlbase.IndexDescriptor
	// Set if the NO_INDEX_JOIN hint was given.
	noIndexJoin bool

	// The table columns, possibly including ones currently in schema changes.
	cols []sqlbase.ColumnDescriptor
	// There is a 1-1 correspondence between cols and resultColumns.
	resultColumns sqlbase.ResultColumns

	// For each column in resultColumns, indicates if the value is
	// needed (used as an optimization when the upper layer doesn't need
	// all values).
	// TODO(radu/knz): currently the optimization always loads the
	// entire row from KV and only skips unnecessary decodes to
	// Datum. Investigate whether performance is to be gained (e.g. for
	// tables with wide rows) by reading only certain columns from KV
	// using point lookups instead of a single range lookup for the
	// entire row.
	valNeededForCol util.FastIntSet

	// Map used to get the index for columns in cols.
	colIdxMap map[sqlbase.ColumnID]int

	spans   []roachpb.Span
	reverse bool
	props   physicalProps

	// Indicates if this scanNode will do a physical data check. This is
	// only true when running SCRUB commands.
	isCheck bool

	// filter that can be evaluated using only this table/index; it contains
	// tree.IndexedVar leaves generated using filterVars.
	filter     tree.TypedExpr
	filterVars tree.IndexedVarHelper

	// origFilter is the original filtering expression, which might have gotten
	// simplified during index selection. For example "k > 0" is converted to a
	// span and the filter is nil. But we still want to deduce not-null columns
	// from the original filter.
	origFilter tree.TypedExpr

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

	run scanRun

	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	//
	//lint:ignore U1000 this marker prevents by-value copies.
	noCopy util.NoCopy
}

// scanVisibility represents which table columns should be included in a scan.
type scanVisibility int

const (
	publicColumns             scanVisibility = 0
	publicAndNonPublicColumns scanVisibility = 1
)

func (p *planner) Scan() *scanNode {
	n := scanNodePool.Get().(*scanNode)
	return n
}

// scanNode implements tree.IndexedVarContainer.
var _ tree.IndexedVarContainer = &scanNode{}

func (n *scanNode) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return n.run.row[idx].Eval(ctx)
}

func (n *scanNode) IndexedVarResolvedType(idx int) types.T {
	return n.resultColumns[idx].Typ
}

func (n *scanNode) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return tree.Name(n.resultColumns[idx].Name)
}

// scanRun contains the run-time state of scanNode during local execution.
type scanRun struct {
	// Contains values for the current row. There is a 1-1 correspondence
	// between resultColumns and values in row.
	row tree.Datums

	// the index of the current row.
	rowIndex int64

	scanInitialized  bool
	isSecondaryIndex bool

	fetcher sqlbase.MultiRowFetcher
}

func (n *scanNode) Start(params runParams) error {
	tableArgs := sqlbase.MultiRowFetcherTableArgs{
		Desc:             n.desc,
		Index:            n.index,
		ColIdxMap:        n.colIdxMap,
		IsSecondaryIndex: n.run.isSecondaryIndex,
		Cols:             n.cols,
		ValNeededForCol:  n.valNeededForCol.Copy(),
	}
	return n.run.fetcher.Init(n.reverse, false, /* returnRangeInfo */
		false /* isCheck */, &params.p.alloc, tableArgs)
}

func (n *scanNode) Close(context.Context) {
	*n = scanNode{}
	scanNodePool.Put(n)
}

func (n *scanNode) Next(params runParams) (bool, error) {
	tracing.AnnotateTrace()
	if !n.run.scanInitialized {
		if err := n.initScan(params); err != nil {
			return false, err
		}
	}

	// We fetch one row at a time until we find one that passes the filter.
	for n.hardLimit == 0 || n.run.rowIndex < n.hardLimit {
		var err error
		n.run.row, _, _, err = n.run.fetcher.NextRowDecoded(params.ctx)
		if err != nil || n.run.row == nil {
			return false, err
		}
		params.evalCtx.IVarHelper = &n.filterVars
		passesFilter, err := sqlbase.RunFilter(n.filter, params.evalCtx)
		if err != nil {
			return false, err
		}
		if passesFilter {
			n.run.rowIndex++
			return true, nil
		}
	}
	return false, nil
}

func (n *scanNode) Values() tree.Datums {
	return n.run.row
}

// disableBatchLimit disables the kvfetcher batch limits. Used for index-join,
// where we scan batches of unordered spans.
func (n *scanNode) disableBatchLimit() {
	n.disableBatchLimits = true
	n.hardLimit = 0
	n.softLimit = 0
}

// initScan sets up the rowFetcher and starts a scan.
func (n *scanNode) initScan(params runParams) error {
	limitHint := n.limitHint()
	if err := n.run.fetcher.StartScan(
		params.ctx,
		params.p.txn,
		n.spans,
		!n.disableBatchLimits,
		limitHint,
		params.p.session.Tracing.KVTracingEnabled(),
	); err != nil {
		return err
	}
	n.run.scanInitialized = true
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

// Initializes a scanNode with a table descriptor.
// wantedColumns is optional.
func (n *scanNode) initTable(
	p *planner,
	desc *sqlbase.TableDescriptor,
	indexHints *tree.IndexHints,
	scanVisibility scanVisibility,
	wantedColumns []tree.ColumnID,
) error {
	n.desc = desc

	if !p.skipSelectPrivilegeChecks {
		if err := p.CheckPrivilege(n.desc, privilege.SELECT); err != nil {
			return err
		}
	}

	if indexHints != nil {
		if err := n.lookupSpecifiedIndex(indexHints); err != nil {
			return err
		}
	}

	n.noIndexJoin = (indexHints != nil && indexHints.NoIndexJoin)
	return n.initDescDefaults(p.planDeps, scanVisibility, wantedColumns)
}

func (n *scanNode) lookupSpecifiedIndex(indexHints *tree.IndexHints) error {
	if indexHints.Index != "" {
		// Search index by name.
		indexName := string(indexHints.Index)
		if indexName == n.desc.PrimaryIndex.Name {
			n.specifiedIndex = &n.desc.PrimaryIndex
		} else {
			for i := range n.desc.Indexes {
				if indexName == n.desc.Indexes[i].Name {
					n.specifiedIndex = &n.desc.Indexes[i]
					break
				}
			}
		}
		if n.specifiedIndex == nil {
			return errors.Errorf("index %q not found", tree.ErrString(indexHints.Index))
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
			return errors.Errorf("index [%d] not found", indexHints.IndexID)
		}
	}
	return nil
}

// Either pick all columns or just those selected.
func filterColumns(
	dst []sqlbase.ColumnDescriptor, wantedColumns []tree.ColumnID, src []sqlbase.ColumnDescriptor,
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
				return nil, errors.Errorf("column [%d] does not exist", wc)
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
	dst []sqlbase.ColumnDescriptor, wantedColumns []tree.ColumnID, src []sqlbase.ColumnDescriptor,
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
// wantedColumns is optional.
// An error may be returned only if wantedColumns is set.
func (n *scanNode) initDescDefaults(
	planDeps planDependencies, scanVisibility scanVisibility, wantedColumns []tree.ColumnID,
) error {
	n.scanVisibility = scanVisibility
	n.index = &n.desc.PrimaryIndex
	n.cols = make([]sqlbase.ColumnDescriptor, 0, len(n.desc.Columns)+len(n.desc.Mutations))
	var err error
	n.cols, err = filterColumns(n.cols, wantedColumns, n.desc.Columns)
	if err != nil {
		return err
	}

	// Register the dependency to the planner, if requested.
	if planDeps != nil {
		indexID := sqlbase.IndexID(0)
		if n.specifiedIndex != nil {
			indexID = n.specifiedIndex.ID
		}
		var usedColumns []sqlbase.ColumnID
		if wantedColumns == nil {
			usedColumns = make([]sqlbase.ColumnID, len(n.desc.Columns))
			for i := range n.desc.Columns {
				usedColumns[i] = n.desc.Columns[i].ID
			}
		} else {
			usedColumns = make([]sqlbase.ColumnID, len(wantedColumns))
			for i, c := range wantedColumns {
				usedColumns[i] = sqlbase.ColumnID(c)
			}
		}
		deps := planDeps[n.desc.ID]
		deps.desc = n.desc
		deps.deps = append(deps.deps, sqlbase.TableDescriptor_Reference{
			IndexID:   indexID,
			ColumnIDs: usedColumns,
		})
		planDeps[n.desc.ID] = deps
	}

	// Set up the rest of the scanNode.
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
	n.valNeededForCol = util.FastIntSet{}
	n.valNeededForCol.AddRange(0, len(n.cols)-1)
	n.run.row = make([]tree.Datum, len(n.cols))
	n.filterVars = tree.MakeIndexedVarHelper(n, len(n.cols))
	return nil
}

// initOrdering initializes the ordering info using the selected index. This
// must be called after index selection is performed.
func (n *scanNode) initOrdering(exactPrefix int, evalCtx *tree.EvalContext) {
	if n.index == nil {
		return
	}
	n.props = n.computePhysicalProps(n.index, exactPrefix, n.reverse, evalCtx)
}

// computePhysicalProps calculates ordering information for table columns
// assuming that:
//   - we scan a given index (potentially in reverse order), and
//   - the first `exactPrefix` columns of the index each have a constant value
//     (see physicalProps).
func (n *scanNode) computePhysicalProps(
	index *sqlbase.IndexDescriptor, exactPrefix int, reverse bool, evalCtx *tree.EvalContext,
) physicalProps {
	var pp physicalProps

	columnIDs, dirs := index.FullColumnIDs()

	var keySet util.FastIntSet
	for i, colID := range columnIDs {
		idx, ok := n.colIdxMap[colID]
		if !ok {
			panic(fmt.Sprintf("index refers to unknown column id %d", colID))
		}
		if i < exactPrefix {
			pp.addConstantColumn(idx)
		} else {
			dir := dirs[i]
			if reverse {
				dir = dir.Reverse()
			}
			pp.addOrderColumn(idx, dir)
		}
		if !n.cols[idx].Nullable {
			pp.addNotNullColumn(idx)
		}
		keySet.Add(idx)
	}

	// We included any implicit columns, so the columns form a (possibly weak)
	// key.
	pp.addWeakKey(keySet)
	pp.applyExpr(evalCtx, n.origFilter)
	return pp
}
