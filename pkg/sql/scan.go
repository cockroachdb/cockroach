// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

var scanNodePool = sync.Pool{
	New: func() interface{} {
		return &scanNode{}
	},
}

// A scanNode handles scanning over the key/value pairs for a table and
// reconstructing them into rows.
type scanNode struct {
	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	_ util.NoCopy

	desc  *sqlbase.ImmutableTableDescriptor
	index *sqlbase.IndexDescriptor

	// Set if an index was explicitly specified.
	specifiedIndex *sqlbase.IndexDescriptor
	// Set if the NO_INDEX_JOIN hint was given.
	noIndexJoin bool

	colCfg scanColumnsConfig
	// The table columns, possibly including ones currently in schema changes.
	// TODO(radu/knz): currently we always load the entire row from KV and only
	// skip unnecessary decodes to Datum. Investigate whether performance is to
	// be gained (e.g. for tables with wide rows) by reading only certain
	// columns from KV using point lookups instead of a single range lookup for
	// the entire row.
	cols []*sqlbase.ColumnDescriptor
	// There is a 1-1 correspondence between cols and resultColumns.
	resultColumns sqlbase.ResultColumns

	// Map used to get the index for columns in cols.
	colIdxMap map[sqlbase.ColumnID]int

	spans   []roachpb.Span
	reverse bool

	reqOrdering ReqOrdering

	// filter that can be evaluated using only this table/index; it contains
	// tree.IndexedVar leaves generated using filterVars.
	filter     tree.TypedExpr
	filterVars tree.IndexedVarHelper

	// if non-zero, hardLimit indicates that the scanNode only needs to provide
	// this many rows (after applying any filter). It is a "hard" guarantee that
	// Next will only be called this many times.
	hardLimit int64
	// if non-zero, softLimit is an estimation that only this many rows (after
	// applying any filter) might be needed. It is a (potentially optimistic)
	// "hint". If hardLimit is set (non-zero), softLimit must be unset (zero).
	softLimit int64

	disableBatchLimits bool

	// Should be set to true if sqlbase.ParallelScans is true.
	parallelScansEnabled bool

	// Is this a full scan of an index?
	isFull bool

	// Indicates if this scanNode will do a physical data check. This is
	// only true when running SCRUB commands.
	isCheck bool

	// maxResults, if greater than 0, is the maximum number of results that a
	// scan is guaranteed to return.
	maxResults uint64

	// estimatedRowCount is the estimated number of rows that this scanNode will
	// output. When there are no statistics to make the estimation, it will be
	// set to zero.
	estimatedRowCount uint64

	// lockingStrength and lockingWaitPolicy represent the row-level locking
	// mode of the Scan.
	lockingStrength   sqlbase.ScanLockingStrength
	lockingWaitPolicy sqlbase.ScanLockingWaitPolicy
}

// scanColumnsConfig controls the "schema" of a scan node.
type scanColumnsConfig struct {
	// wantedColumns contains all the columns are part of the scan node schema,
	// in this order (with the caveat that the addUnwantedAsHidden flag below
	// can add more columns). Non public columns can only be added if allowed
	// by the visibility flag below.
	wantedColumns []tree.ColumnID

	// When set, the columns that are not in the wantedColumns list are added to
	// the list of columns as hidden columns.
	addUnwantedAsHidden bool

	// If visibility is set to execinfra.ScanVisibilityPublicAndNotPublic, then
	// mutation columns can be added to the list of columns.
	visibility execinfrapb.ScanVisibility
}

func (cfg scanColumnsConfig) assertValidReqOrdering(reqOrdering exec.OutputOrdering) error {
	for i := range reqOrdering {
		if reqOrdering[i].ColIdx >= len(cfg.wantedColumns) {
			return errors.Errorf("invalid reqOrdering: %v", reqOrdering)
		}
	}
	return nil
}

func (p *planner) Scan() *scanNode {
	n := scanNodePool.Get().(*scanNode)
	return n
}

// scanNode implements tree.IndexedVarContainer.
var _ tree.IndexedVarContainer = &scanNode{}

func (n *scanNode) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	panic("scanNode can't be run in local mode")
}

func (n *scanNode) IndexedVarResolvedType(idx int) *types.T {
	return n.resultColumns[idx].Typ
}

func (n *scanNode) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return (*tree.Name)(&n.resultColumns[idx].Name)
}

func (n *scanNode) startExec(params runParams) error {
	panic("scanNode can't be run in local mode")
}

func (n *scanNode) Close(context.Context) {
	*n = scanNode{}
	scanNodePool.Put(n)
}

func (n *scanNode) Next(params runParams) (bool, error) {
	panic("scanNode can't be run in local mode")
}

func (n *scanNode) Values() tree.Datums {
	panic("scanNode can't be run in local mode")
}

// disableBatchLimit disables the kvfetcher batch limits. Used for index-join,
// where we scan batches of unordered spans.
func (n *scanNode) disableBatchLimit() {
	n.disableBatchLimits = true
	n.hardLimit = 0
	n.softLimit = 0
}

// canParallelize returns true if this scanNode can be parallelized at the
// distSender level safely.
func (n *scanNode) canParallelize() bool {
	// We choose only to parallelize if we are certain that no more than
	// ParallelScanResultThreshold results will be returned, to prevent potential
	// memory blowup.
	// We can't parallelize if we have a non-zero limit hint, since DistSender
	// is limited to running limited batches serially.
	return n.maxResults != 0 &&
		n.maxResults < execinfra.ParallelScanResultThreshold &&
		n.limitHint() == 0 &&
		n.parallelScansEnabled
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
		// TODO(yuzefovich): shouldn't soft limit already account for the
		// selectivity of any filter and whatnot?
		limitHint = n.softLimit * 2
	}
	return limitHint
}

// Initializes a scanNode with a table descriptor.
func (n *scanNode) initTable(
	ctx context.Context,
	p *planner,
	desc *sqlbase.ImmutableTableDescriptor,
	indexFlags *tree.IndexFlags,
	colCfg scanColumnsConfig,
) error {
	n.desc = desc

	if !p.skipSelectPrivilegeChecks {
		if err := p.CheckPrivilege(ctx, n.desc, privilege.SELECT); err != nil {
			return err
		}
	}

	if indexFlags != nil {
		if err := n.lookupSpecifiedIndex(indexFlags); err != nil {
			return err
		}
	}

	n.noIndexJoin = (indexFlags != nil && indexFlags.NoIndexJoin)
	return n.initDescDefaults(colCfg)
}

func (n *scanNode) lookupSpecifiedIndex(indexFlags *tree.IndexFlags) error {
	if indexFlags.Index != "" {
		// Search index by name.
		indexName := string(indexFlags.Index)
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
			return errors.Errorf("index %q not found", tree.ErrString(&indexFlags.Index))
		}
	} else if indexFlags.IndexID != 0 {
		// Search index by ID.
		if n.desc.PrimaryIndex.ID == sqlbase.IndexID(indexFlags.IndexID) {
			n.specifiedIndex = &n.desc.PrimaryIndex
		} else {
			for i := range n.desc.Indexes {
				if n.desc.Indexes[i].ID == sqlbase.IndexID(indexFlags.IndexID) {
					n.specifiedIndex = &n.desc.Indexes[i]
					break
				}
			}
		}
		if n.specifiedIndex == nil {
			return errors.Errorf("index [%d] not found", indexFlags.IndexID)
		}
	}
	return nil
}

// initColsForScan initializes cols according to desc and colCfg.
func initColsForScan(
	desc *sqlbase.ImmutableTableDescriptor, colCfg scanColumnsConfig,
) (cols []*sqlbase.ColumnDescriptor, err error) {
	if colCfg.wantedColumns == nil {
		return nil, errors.AssertionFailedf("unexpectedly wantedColumns is nil")
	}

	cols = make([]*sqlbase.ColumnDescriptor, 0, len(desc.ReadableColumns))
	for _, wc := range colCfg.wantedColumns {
		var c *sqlbase.ColumnDescriptor
		var err error
		if id := sqlbase.ColumnID(wc); colCfg.visibility == execinfra.ScanVisibilityPublic {
			c, err = desc.FindActiveColumnByID(id)
		} else {
			c, _, err = desc.FindReadableColumnByID(id)
		}
		if err != nil {
			return cols, err
		}

		cols = append(cols, c)
	}

	if colCfg.addUnwantedAsHidden {
		for i := range desc.Columns {
			c := &desc.Columns[i]
			found := false
			for _, wc := range colCfg.wantedColumns {
				if sqlbase.ColumnID(wc) == c.ID {
					found = true
					break
				}
			}
			if !found {
				// NB: we could amortize this allocation using a second slice,
				// but addUnwantedAsHidden is only used by scrub, so doing so
				// doesn't seem worth it.
				col := *c
				col.Hidden = true
				cols = append(cols, &col)
			}
		}
	}

	return cols, nil
}

// Initializes the column structures.
func (n *scanNode) initDescDefaults(colCfg scanColumnsConfig) error {
	n.colCfg = colCfg
	n.index = &n.desc.PrimaryIndex

	var err error
	n.cols, err = initColsForScan(n.desc, n.colCfg)
	if err != nil {
		return err
	}

	// Set up the rest of the scanNode.
	n.resultColumns = sqlbase.ResultColumnsFromColDescPtrs(n.desc.GetID(), n.cols)
	n.colIdxMap = make(map[sqlbase.ColumnID]int, len(n.cols))
	for i, c := range n.cols {
		n.colIdxMap[c.ID] = i
	}
	n.filterVars = tree.MakeIndexedVarHelper(n, len(n.cols))
	return nil
}
