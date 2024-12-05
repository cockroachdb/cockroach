// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/redact"
)

// rowContainerHelper is a wrapper around a disk-backed row container that
// should be used by planNodes (or similar components) whenever they need to
// buffer data. init or initWithDedup must be called before the first use.
type rowContainerHelper struct {
	memMonitor          *mon.BytesMonitor
	unlimitedMemMonitor *mon.BytesMonitor
	diskMonitor         *mon.BytesMonitor
	rows                *rowcontainer.DiskBackedRowContainer
	scratch             rowenc.EncDatumRow
}

func (c *rowContainerHelper) Init(
	ctx context.Context, typs []*types.T, evalContext *extendedEvalContext, opName redact.SafeString,
) {
	c.initMonitors(ctx, evalContext, opName)
	distSQLCfg := &evalContext.DistSQLPlanner.distSQLSrv.ServerConfig
	c.rows = &rowcontainer.DiskBackedRowContainer{}
	c.rows.Init(
		colinfo.NoOrdering, typs, &evalContext.Context, distSQLCfg.TempStorage,
		c.memMonitor, c.unlimitedMemMonitor, c.diskMonitor,
	)
	c.scratch = make(rowenc.EncDatumRow, len(typs))
}

// InitWithDedup is a variant of init that is used if row deduplication
// functionality is needed (see addRowWithDedup).
func (c *rowContainerHelper) InitWithDedup(
	ctx context.Context, typs []*types.T, evalContext *extendedEvalContext, opName redact.SafeString,
) {
	c.initMonitors(ctx, evalContext, opName)
	distSQLCfg := &evalContext.DistSQLPlanner.distSQLSrv.ServerConfig
	c.rows = &rowcontainer.DiskBackedRowContainer{}
	// The DiskBackedRowContainer can be configured to deduplicate along the
	// columns in the ordering (these columns form the "key" if the container has
	// to spill to disk).
	ordering := make(colinfo.ColumnOrdering, len(typs))
	for i := range ordering {
		ordering[i].ColIdx = i
		ordering[i].Direction = encoding.Ascending
	}
	c.rows.Init(
		ordering, typs, &evalContext.Context, distSQLCfg.TempStorage,
		c.memMonitor, c.unlimitedMemMonitor, c.diskMonitor,
	)
	c.rows.DoDeDuplicate()
	c.scratch = make(rowenc.EncDatumRow, len(typs))
}

// InitWithParentMon is a variant of Init that allows the parent memory monitor
// to be specified. This is useful when the container should not be owned by the
// current transaction (e.g. a SQL cursor that lives on the session).
func (c *rowContainerHelper) InitWithParentMon(
	ctx context.Context,
	typs []*types.T,
	parent *mon.BytesMonitor,
	evalContext *extendedEvalContext,
	opName redact.SafeString,
) {
	distSQLCfg := &evalContext.DistSQLPlanner.distSQLSrv.ServerConfig
	// TODO(yuzefovich): currently the memory usage of c.memMonitor and
	// c.unlimitedMemMonitor don't count against sql.mem.distsql.current metric.
	// Fix it.
	c.memMonitor = execinfra.NewLimitedMonitorNoFlowCtx(
		ctx, parent, distSQLCfg, evalContext.SessionData(),
		opName+"-limited",
	)
	c.unlimitedMemMonitor = execinfra.NewMonitor(
		ctx, parent, opName+"-unlimited",
	)
	c.diskMonitor = execinfra.NewMonitor(
		ctx, distSQLCfg.ParentDiskMonitor, opName+"-disk",
	)
	c.rows = &rowcontainer.DiskBackedRowContainer{}
	c.rows.Init(
		colinfo.NoOrdering, typs, &evalContext.Context, distSQLCfg.TempStorage,
		c.memMonitor, c.unlimitedMemMonitor, c.diskMonitor,
	)
	c.scratch = make(rowenc.EncDatumRow, len(typs))
}

func (c *rowContainerHelper) initMonitors(
	ctx context.Context, evalContext *extendedEvalContext, opName redact.SafeString,
) {
	distSQLCfg := &evalContext.DistSQLPlanner.distSQLSrv.ServerConfig
	// TODO(yuzefovich): currently the memory usage of c.memMonitor and
	// c.unlimitedMemMonitor don't count against sql.mem.distsql.current metric.
	// Fix it.
	c.memMonitor = execinfra.NewLimitedMonitorNoFlowCtx(
		ctx, evalContext.Planner.Mon(), distSQLCfg, evalContext.SessionData(),
		opName+"-limited",
	)
	c.unlimitedMemMonitor = execinfra.NewMonitor(
		ctx, evalContext.Planner.Mon(), opName+"-unlimited",
	)
	c.diskMonitor = execinfra.NewMonitor(
		ctx, distSQLCfg.ParentDiskMonitor, opName+"-disk",
	)
}

// AddRow adds the given row to the container.
func (c *rowContainerHelper) AddRow(ctx context.Context, row tree.Datums) error {
	for i := range row {
		c.scratch[i].Datum = row[i]
	}
	return c.rows.AddRow(ctx, c.scratch)
}

// AddRowWithDedup adds the given row if not already present in the container.
// To use this method, InitWithDedup must be used first.
func (c *rowContainerHelper) AddRowWithDedup(
	ctx context.Context, row tree.Datums,
) (added bool, _ error) {
	for i := range row {
		c.scratch[i].Datum = row[i]
	}
	lenBefore := c.rows.Len()
	if _, err := c.rows.AddRowWithDeDup(ctx, c.scratch); err != nil {
		return false, err
	}
	return c.rows.Len() > lenBefore, nil
}

// Len returns the number of rows buffered so far.
func (c *rowContainerHelper) Len() int {
	return c.rows.Len()
}

// Clear prepares the helper for reuse (it resets the underlying container which
// will delete all buffered data; also, the container will be using the
// in-memory variant even if it spilled on the previous usage).
func (c *rowContainerHelper) Clear(ctx context.Context) error {
	return c.rows.UnsafeReset(ctx)
}

// Close must be called once the helper is no longer needed to clean up any
// resources.
func (c *rowContainerHelper) Close(ctx context.Context) {
	if c.rows != nil {
		c.rows.Close(ctx)
		c.memMonitor.Stop(ctx)
		c.unlimitedMemMonitor.Stop(ctx)
		c.diskMonitor.Stop(ctx)
		c.rows = nil
	}
}

// rowContainerIterator is a wrapper around rowcontainer.RowIterator that takes
// care of advancing the underlying iterator and converting the rows to
// tree.Datums.
type rowContainerIterator struct {
	iter rowcontainer.RowIterator
}

// newRowContainerIterator returns a new rowContainerIterator that must be
// closed once no longer needed.
func newRowContainerIterator(ctx context.Context, c rowContainerHelper) *rowContainerIterator {
	i := &rowContainerIterator{iter: c.rows.NewIterator(ctx)}
	i.iter.Rewind()
	return i
}

// Next returns the next row of the iterator or an error if encountered. It
// returns nil, nil when the iterator has been exhausted.
func (i *rowContainerIterator) Next() (tree.Datums, error) {
	defer i.iter.Next()
	if valid, err := i.iter.Valid(); err != nil {
		return nil, err
	} else if !valid {
		// All rows have been exhausted.
		return nil, nil
	}
	return i.iter.Row()
}

func (i *rowContainerIterator) Close() {
	i.iter.Close()
}
