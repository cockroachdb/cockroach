// Copyright 2021 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// rowContainerHelper is a wrapper around a disk-backed row container that
// should be used by planNodes (or similar components) whenever they need to
// buffer data. init must be called before the first use.
type rowContainerHelper struct {
	rows        *rowcontainer.DiskBackedRowContainer
	scratch     rowenc.EncDatumRow
	memMonitor  *mon.BytesMonitor
	diskMonitor *mon.BytesMonitor
}

func (c *rowContainerHelper) init(
	typs []*types.T, evalContext *extendedEvalContext, opName string,
) {
	distSQLCfg := &evalContext.DistSQLPlanner.distSQLSrv.ServerConfig
	c.memMonitor = execinfra.NewLimitedMonitorNoFlowCtx(
		evalContext.Context, evalContext.Mon, distSQLCfg, evalContext.SessionData,
		fmt.Sprintf("%s-limited", opName),
	)
	c.diskMonitor = execinfra.NewMonitor(evalContext.Context, distSQLCfg.ParentDiskMonitor, fmt.Sprintf("%s-disk", opName))
	c.rows = &rowcontainer.DiskBackedRowContainer{}
	c.rows.Init(
		colinfo.NoOrdering, typs, &evalContext.EvalContext,
		distSQLCfg.TempStorage, c.memMonitor, c.diskMonitor,
	)
	c.scratch = make(rowenc.EncDatumRow, len(typs))
}

// addRow adds a given row to the helper and returns any error it encounters.
func (c *rowContainerHelper) addRow(ctx context.Context, row tree.Datums) error {
	for i := range row {
		c.scratch[i].Datum = row[i]
	}
	return c.rows.AddRow(ctx, c.scratch)
}

// len returns the number of rows buffered so far.
func (c *rowContainerHelper) len() int {
	return c.rows.Len()
}

// clear prepares the helper for reuse (it resets the underlying container which
// will delete all buffered data; also, the container will be using the
// in-memory variant even if it spilled on the previous usage).
func (c *rowContainerHelper) clear(ctx context.Context) error {
	return c.rows.UnsafeReset(ctx)
}

// close must be called once the helper is no longer needed to clean up any
// resources.
func (c *rowContainerHelper) close(ctx context.Context) {
	if c.rows != nil {
		c.rows.Close(ctx)
		c.memMonitor.Stop(ctx)
		c.diskMonitor.Stop(ctx)
		c.rows = nil
	}
}

// rowContainerIterator is a wrapper around rowcontainer.RowIterator that takes
// care of advancing the underlying iterator and converting the rows to
// tree.Datums.
type rowContainerIterator struct {
	iter rowcontainer.RowIterator

	typs   []*types.T
	datums tree.Datums
	da     rowenc.DatumAlloc
}

// newRowContainerIterator returns a new rowContainerIterator that must be
// closed once no longer needed.
func newRowContainerIterator(
	ctx context.Context, c rowContainerHelper, typs []*types.T,
) *rowContainerIterator {
	i := &rowContainerIterator{
		iter:   c.rows.NewIterator(ctx),
		typs:   typs,
		datums: make(tree.Datums, len(typs)),
	}
	i.iter.Rewind()
	return i
}

// next returns the next row of the iterator or an error if encountered. It
// returns nil, nil when the iterator has been exhausted.
func (i *rowContainerIterator) next() (tree.Datums, error) {
	defer i.iter.Next()
	if valid, err := i.iter.Valid(); err != nil {
		return nil, err
	} else if !valid {
		// All rows have been exhausted.
		return nil, nil
	}
	row, err := i.iter.Row()
	if err != nil {
		return nil, err
	}
	if err = rowenc.EncDatumRowToDatums(i.typs, i.datums, row, &i.da); err != nil {
		return nil, err
	}
	return i.datums, nil
}

func (i *rowContainerIterator) close() {
	i.iter.Close()
}
