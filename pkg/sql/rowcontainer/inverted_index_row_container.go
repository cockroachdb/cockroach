// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowcontainer

import (
	"context"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/diskmap"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

type RowIndex uint64

// InvertedIndexRowContainer is a container that dedups the added rows and
// assigns an index to each deduped row. NULLs are considered equal.
type InvertedIndexRowContainer interface {
	Len() int
	AddRowAndDedup(context.Context, sqlbase.EncDatumRow) (RowIndex, error)
	GetRow(context.Context, RowIndex) (tree.IndexedRow, error)
	UnsafeReset(context.Context) error
	Close(context.Context)
}

// MemInvertedIndexRowContainer is an in-memory implementation of
// InvertedIndexRowContainer.
type MemInvertedIndexRowContainer struct {
	container *MemRowContainer
	encoder   columnEncoder
	eqCols    columns
	rowsMap   map[string]RowIndex
	// The memory account for rowsMap. The datums themselves are all in the
	// MemRowContainer.
	rowsMapAcc mon.BoundAccount
}

var _ InvertedIndexRowContainer = (*MemInvertedIndexRowContainer)(nil)

func NewMemInvertedIndexRowContainer(
	types []types.T, evalCtx *tree.EvalContext, memoryMonitor *mon.BytesMonitor,
) *MemInvertedIndexRowContainer {
	mrc := &MemRowContainer{}
	mrc.InitWithMon(nil, types, evalCtx, memoryMonitor, 0)
	imrc := &MemInvertedIndexRowContainer{
		container:  mrc,
		rowsMap:    make(map[string]RowIndex),
		rowsMapAcc: mrc.evalCtx.Mon.MakeBoundAccount(),
	}
	for i := range types {
		imrc.eqCols = append(imrc.eqCols, uint32(i))
	}
	imrc.encoder.init(types, imrc.eqCols, true /* nulls are equal */)
	return imrc
}

func (c *MemInvertedIndexRowContainer) Len() int {
	return len(c.rowsMap)
}

const sizeOfRowIndex = int64(unsafe.Sizeof(RowIndex(0)))

func (c *MemInvertedIndexRowContainer) AddRowAndDedup(
	ctx context.Context, row sqlbase.EncDatumRow,
) (RowIndex, error) {
	encoded, err := c.encoder.encodeEqualityCols(ctx, row, c.eqCols)
	if err != nil {
		return 0, err
	}
	strEncoded := string(encoded)
	if rowIdx, ok := c.rowsMap[strEncoded]; ok {
		return rowIdx, nil
	}
	usage := sizeOfRowIndex + int64(len(strEncoded))
	if err := c.rowsMapAcc.Grow(ctx, usage); err != nil {
		return 0, err
	}
	rowIdx := RowIndex(len(c.rowsMap))
	c.rowsMap[strEncoded] = rowIdx
	if err := c.container.AddRow(ctx, row); err != nil {
		return 0, err
	}
	return rowIdx, nil
}

func (c *MemInvertedIndexRowContainer) GetRow(
	ctx context.Context, idx RowIndex,
) (tree.IndexedRow, error) {
	return c.container.GetRow(ctx, int(idx))
}

func (c *MemInvertedIndexRowContainer) UnsafeReset(ctx context.Context) error {
	if err := c.container.UnsafeReset(ctx); err != nil {
		return err
	}
	c.rowsMap = make(map[string]RowIndex)
	return nil
}

func (c *MemInvertedIndexRowContainer) Close(ctx context.Context) {
	c.container.Close(ctx)
	c.rowsMapAcc.Close(ctx)
}

type DiskBackedInvertedIndexRowContainer struct {
	// TODO
}

var _ InvertedIndexRowContainer = (*DiskBackedInvertedIndexRowContainer)(nil)

func NewDiskBackedInvertedIndexRowContainer(
	types []types.T,
	evalCtx *tree.EvalContext,
	engine diskmap.Factory,
	memoryMonitor *mon.BytesMonitor,
	diskMonitor *mon.BytesMonitor,
) *DiskBackedInvertedIndexRowContainer {
	return nil
}
func (c *DiskBackedInvertedIndexRowContainer) Len() int {
	return 0
}
func (c *DiskBackedInvertedIndexRowContainer) AddRowAndDedup(
	context.Context, sqlbase.EncDatumRow,
) (RowIndex, error) {
	return 0, nil
}
func (c *DiskBackedInvertedIndexRowContainer) GetRow(
	ctx context.Context, idx RowIndex,
) (tree.IndexedRow, error) {
	return nil, nil
}
func (c *DiskBackedInvertedIndexRowContainer) UnsafeReset(context.Context) error {
	return nil
}
func (c *DiskBackedInvertedIndexRowContainer) Spilled() bool {
	return false
}
func (c *DiskBackedInvertedIndexRowContainer) Close(context.Context) {
}
