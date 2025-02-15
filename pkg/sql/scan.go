// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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

	zeroInputPlanNode

	desc  catalog.TableDescriptor
	index catalog.Index

	colCfg scanColumnsConfig
	// The table columns, possibly including ones currently in schema changes.
	// TODO(radu/knz): currently we always load the entire row from KV and only
	// skip unnecessary decodes to Datum. Investigate whether performance is to
	// be gained (e.g. for tables with wide rows) by reading only certain
	// columns from KV using point lookups instead of a single range lookup for
	// the entire row.
	cols []catalog.Column
	// There is a 1-1 correspondence between cols and resultColumns.
	resultColumns colinfo.ResultColumns

	spans   []roachpb.Span
	reverse bool

	reqOrdering ReqOrdering

	// if non-zero, hardLimit indicates that the scanNode only needs to provide
	// this many rows.
	hardLimit int64
	// if non-zero, softLimit is an estimation that only this many rows might be
	// needed. It is a (potentially optimistic) "hint". If hardLimit is set
	// (non-zero), softLimit must be unset (zero).
	softLimit int64

	disableBatchLimits bool

	// See exec.Factory.ConstructScan.
	parallelize bool

	// Is this a full scan of an index?
	isFull bool

	// estimatedRowCount is the estimated number of rows that this scanNode will
	// output. When there are no statistics to make the estimation, it will be
	// set to zero.
	estimatedRowCount uint64

	// lockingStrength, lockingWaitPolicy, and lockingDurability represent the
	// row-level locking mode of the Scan.
	lockingStrength   descpb.ScanLockingStrength
	lockingWaitPolicy descpb.ScanLockingWaitPolicy
	lockingDurability descpb.ScanLockingDurability

	// containsSystemColumns holds whether or not this scan is expected to
	// produce any system columns.
	containsSystemColumns bool

	// localityOptimized is true if this scan is part of a locality optimized
	// search strategy, which uses a limited UNION ALL operator to try to find a
	// row on nodes in the gateway's region before fanning out to remote nodes. In
	// order for this optimization to work, the DistSQL planner must create a
	// local plan.
	localityOptimized bool
}

// scanColumnsConfig controls the "schema" of a scan node.
type scanColumnsConfig struct {
	// wantedColumns contains all the columns are part of the scan node schema,
	// in this order. Must not be nil (even if empty).
	wantedColumns []tree.ColumnID

	// invertedColumnID/invertedColumnType are used to map the column ID of the
	// inverted column (if it exists) to the column type actually stored in the
	// index. For example, the inverted column of an inverted index has type
	// bytes, even though the column descriptor matches the source column
	// (Geometry, Geography, JSON or Array).
	invertedColumnID   tree.ColumnID
	invertedColumnType *types.T
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

// scanNode implements eval.IndexedVarContainer.
var _ tree.IndexedVarContainer = &scanNode{}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (n *scanNode) IndexedVarResolvedType(idx int) *types.T {
	return n.resultColumns[idx].Typ
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

// Initializes a scanNode with a table descriptor.
func (n *scanNode) initTable(
	ctx context.Context, p *planner, desc catalog.TableDescriptor, colCfg scanColumnsConfig,
) error {
	n.desc = desc

	// Check if any system columns are requested, as they need special handling.
	n.containsSystemColumns = scanContainsSystemColumns(&colCfg)

	return n.initDescDefaults(colCfg)
}

// initColsForScan initializes cols according to desc and colCfg.
func initColsForScan(
	desc catalog.TableDescriptor, colCfg scanColumnsConfig,
) (cols []catalog.Column, err error) {
	if colCfg.wantedColumns == nil {
		return nil, errors.AssertionFailedf("wantedColumns is nil")
	}

	cols = make([]catalog.Column, len(colCfg.wantedColumns))
	for i, colID := range colCfg.wantedColumns {
		col, err := catalog.MustFindColumnByID(desc, colID)
		if err != nil {
			return cols, err
		}

		// If this is an inverted column, create a new descriptor with the
		// correct type.
		if colCfg.invertedColumnID == colID && !colCfg.invertedColumnType.Identical(col.GetType()) {
			col = col.DeepCopy()
			col.ColumnDesc().Type = colCfg.invertedColumnType
		}
		cols[i] = col
	}

	return cols, nil
}

// Initializes the column structures.
func (n *scanNode) initDescDefaults(colCfg scanColumnsConfig) error {
	n.colCfg = colCfg
	n.index = n.desc.GetPrimaryIndex()

	var err error
	n.cols, err = initColsForScan(n.desc, n.colCfg)
	if err != nil {
		return err
	}

	// Set up the rest of the scanNode.
	n.resultColumns = colinfo.ResultColumnsFromColumns(n.desc.GetID(), n.cols)
	return nil
}

// initDescSpecificCol initializes the column structures with the
// index that the provided column is the prefix for.
func (n *scanNode) initDescSpecificCol(colCfg scanColumnsConfig, prefixCol catalog.Column) error {
	n.colCfg = colCfg
	indexes := n.desc.ActiveIndexes()
	prefixColID := prefixCol.GetID()

	// Currently, we pick the first index that we find, even if there exist multiple in the
	// table where prefixCol is the key column.
	foundIndex := false
	for _, idx := range indexes {
		if idx.GetType().AllowsPrefixColumns() || idx.IsPartial() {
			continue
		}
		columns := n.desc.IndexKeyColumns(idx)
		if len(columns) > 0 {
			if columns[0].GetID() == prefixColID {
				n.index = idx
				foundIndex = true
				break
			}
		}
	}
	if !foundIndex {
		return pgerror.Newf(pgcode.InvalidColumnReference,
			"table %s does not contain a non-partial forward index with %s as a prefix column",
			n.desc.GetName(),
			prefixCol.GetName())
	}
	var err error
	n.cols, err = initColsForScan(n.desc, n.colCfg)
	if err != nil {
		return err
	}
	// Set up the rest of the scanNode.
	n.resultColumns = colinfo.ResultColumnsFromColumns(n.desc.GetID(), n.cols)
	return nil
}
