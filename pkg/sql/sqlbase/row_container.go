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
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package sqlbase

import (
	"fmt"
	"unsafe"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

const (
	// targetChunkSize is the target number of Datums in a RowContainer chunk.
	targetChunkSize = 64
	// SizeOfDatum is the memory size of a Datum reference.
	SizeOfDatum = int64(unsafe.Sizeof(parser.Datum(nil)))
	// SizeOfDatums is the memory size of a Datum slice.
	SizeOfDatums = int64(unsafe.Sizeof(parser.Datums(nil)))
)

// RowContainer is a container for rows of Datums which tracks the
// approximate amount of memory allocated for row data.
// Rows must be added using AddRow(); once the work is done
// the Close() method must be called to release the allocated memory.
//
// TODO(knz): this does not currently track the amount of memory used
// for the outer array of Datums references.
type RowContainer struct {
	numCols int

	// rowsPerChunk is the number of rows in a chunk; we pack multiple rows in a
	// single []Datum to reduce the overhead of the slice if we have few columns.
	rowsPerChunk int
	// preallocChunks is the number of chunks we allocate upfront (on the first
	// AddRow call).
	preallocChunks int
	chunks         [][]parser.Datum
	numRows        int

	// chunkMemSize is the memory used by a chunk.
	chunkMemSize int64
	// fixedColsSize is the sum of widths of fixed-width columns in a
	// single row.
	fixedColsSize int64
	// varSizedColumns indicates for which columns the datum size
	// is variable.
	varSizedColumns []int

	// deletedRows is the number of rows that have been deleted from the front
	// of the container. When this number reaches rowsPerChunk we delete that chunk
	// and reset this back to zero.
	deletedRows int

	// memAcc tracks the current memory consumption of this
	// RowContainer.
	memAcc mon.BoundAccount
}

// ColTypeInfo is a type that allows multiple representations of column type
// information (to avoid conversions and allocations).
type ColTypeInfo struct {
	// Only one of these fields can be set.
	resCols  ResultColumns
	colTypes []ColumnType
}

// ColTypeInfoFromResCols creates a ColTypeInfo from ResultColumns.
func ColTypeInfoFromResCols(resCols ResultColumns) ColTypeInfo {
	return ColTypeInfo{resCols: resCols}
}

// ColTypeInfoFromColTypes creates a ColTypeInfo from []ColumnType.
func ColTypeInfoFromColTypes(colTypes []ColumnType) ColTypeInfo {
	return ColTypeInfo{colTypes: colTypes}
}

// NumColumns returns the number of columns in the type.
func (ti ColTypeInfo) NumColumns() int {
	if ti.resCols != nil {
		return len(ti.resCols)
	}
	return len(ti.colTypes)
}

// Type returns the datum type of the i-th column.
func (ti ColTypeInfo) Type(idx int) parser.Type {
	if ti.resCols != nil {
		return ti.resCols[idx].Typ
	}
	return ti.colTypes[idx].ToDatumType()
}

// NewRowContainer allocates a new row container.
//
// The acc argument indicates where to register memory allocations by
// this row container. Should probably be created by
// Session.makeBoundAccount() or Session.TxnState.makeBoundAccount().
//
// The rowCapacity argument indicates how many rows are to be
// expected; it is used to pre-allocate the outer array of row
// references, in the fashion of Go's capacity argument to the make()
// function.
//
// Note that we could, but do not (yet), report the size of the row
// container itself to the monitor in this constructor. This is
// because the various planNodes are not (yet) equipped to call
// Close() upon encountering errors in their constructor (all nodes
// initializing a RowContainer there) and SetLimitHint() (for sortNode
// which initializes a RowContainer there). This would be rather
// error-prone to implement consistently and hellishly difficult to
// test properly.  The trade-off is that very large table schemas or
// column selections could cause unchecked and potentially dangerous
// memory growth.
func NewRowContainer(acc mon.BoundAccount, ti ColTypeInfo, rowCapacity int) *RowContainer {
	c := MakeRowContainer(acc, ti, rowCapacity)
	return &c
}

// MakeRowContainer is the non-pointer version of NewRowContainer, suitable to
// avoid unnecessary indirections when RowContainer is already part of an on-heap
// structure.
func MakeRowContainer(acc mon.BoundAccount, ti ColTypeInfo, rowCapacity int) RowContainer {
	nCols := ti.NumColumns()

	c := RowContainer{
		numCols:        nCols,
		memAcc:         acc,
		preallocChunks: 1,
	}

	if nCols != 0 {
		c.rowsPerChunk = (targetChunkSize + nCols - 1) / nCols
		if rowCapacity > 0 {
			c.preallocChunks = (rowCapacity + c.rowsPerChunk - 1) / c.rowsPerChunk
		}
	}

	for i := 0; i < nCols; i++ {
		sz, variable := ti.Type(i).Size()
		if variable {
			if c.varSizedColumns == nil {
				// Only allocate varSizedColumns if necessary.
				c.varSizedColumns = make([]int, 0, nCols)
			}
			c.varSizedColumns = append(c.varSizedColumns, i)
		} else {
			c.fixedColsSize += int64(sz)
		}
	}

	// Precalculate the memory used for a chunk, specifically by the Datums in the
	// chunk and the slice pointing at the chunk.
	c.chunkMemSize = SizeOfDatum * int64(c.rowsPerChunk*c.numCols)
	c.chunkMemSize += SizeOfDatums

	return c
}

// Clear resets the container and releases the associated memory. This allows
// the RowContainer to be reused.
func (c *RowContainer) Clear(ctx context.Context) {
	c.numRows = 0
	c.deletedRows = 0
	c.chunks = nil
	c.memAcc.Clear(ctx)
}

// Close releases the memory associated with the RowContainer.
func (c *RowContainer) Close(ctx context.Context) {
	c.chunks = nil
	c.varSizedColumns = nil
	c.memAcc.Close(ctx)
}

func (c *RowContainer) allocChunks(ctx context.Context, numChunks int) error {
	datumsPerChunk := c.rowsPerChunk * c.numCols

	if err := c.memAcc.Grow(ctx, c.chunkMemSize*int64(numChunks)); err != nil {
		return err
	}

	if c.chunks == nil {
		c.chunks = make([][]parser.Datum, 0, numChunks)
	}

	datums := make([]parser.Datum, numChunks*datumsPerChunk)
	for i, pos := 0, 0; i < numChunks; i++ {
		c.chunks = append(c.chunks, datums[pos:pos+datumsPerChunk])
		pos += datumsPerChunk
	}
	return nil
}

// rowSize computes the size of a single row.
func (c *RowContainer) rowSize(row parser.Datums) int64 {
	rsz := c.fixedColsSize
	for _, i := range c.varSizedColumns {
		rsz += int64(row[i].Size())
	}
	return rsz
}

// getChunkAndPos returns the chunk index and the position inside the chunk for
// a given row index.
func (c *RowContainer) getChunkAndPos(rowIdx int) (chunk int, pos int) {
	// This is a potential hot path; use int32 for faster division.
	row := int32(rowIdx + c.deletedRows)
	div := int32(c.rowsPerChunk)
	return int(row / div), int(row % div * int32(c.numCols))
}

// AddRow attempts to insert a new row in the RowContainer. The row slice is not
// used directly: the Datum values inside the Datums are copied to internal storage.
// Returns an error if the allocation was denied by the MemoryMonitor.
func (c *RowContainer) AddRow(ctx context.Context, row parser.Datums) (parser.Datums, error) {
	if len(row) != c.numCols {
		panic(fmt.Sprintf("invalid row length %d, expected %d", len(row), c.numCols))
	}
	if c.numCols == 0 {
		c.numRows++
		return nil, nil
	}
	if err := c.memAcc.Grow(ctx, c.rowSize(row)); err != nil {
		return nil, err
	}
	chunk, pos := c.getChunkAndPos(c.numRows)
	if chunk == len(c.chunks) {
		numChunks := c.preallocChunks
		if len(c.chunks) > 0 {
			// Grow the number of chunks by a fraction.
			numChunks = 1 + len(c.chunks)/8
		}
		if err := c.allocChunks(ctx, numChunks); err != nil {
			return nil, err
		}
	}
	copy(c.chunks[chunk][pos:pos+c.numCols], row)
	c.numRows++
	return c.chunks[chunk][pos : pos+c.numCols : pos+c.numCols], nil
}

// Len reports the number of rows currently held in this RowContainer.
func (c *RowContainer) Len() int {
	return c.numRows
}

// At accesses a row at a specific index.
func (c *RowContainer) At(i int) parser.Datums {
	if i < 0 || i >= c.numRows {
		panic(fmt.Sprintf("row index %d out of range", i))
	}
	if c.numCols == 0 {
		// We don't want to return nil, as in some contexts nil is used as a special
		// value to indicate that there are no more rows. Note that this doesn't
		// actually allocate anything.
		return make(parser.Datums, 0)
	}
	chunk, pos := c.getChunkAndPos(i)
	return c.chunks[chunk][pos : pos+c.numCols : pos+c.numCols]
}

// Swap exchanges two rows. Used for sorting.
func (c *RowContainer) Swap(i, j int) {
	r1 := c.At(i)
	r2 := c.At(j)
	for idx := 0; idx < c.numCols; idx++ {
		r1[idx], r2[idx] = r2[idx], r1[idx]
	}
}

// PopFirst discards the first row in the RowContainer.
func (c *RowContainer) PopFirst() {
	if c.numRows == 0 {
		panic("no rows added to container, nothing to pop")
	}
	c.numRows--
	if c.numCols != 0 {
		c.deletedRows++
		if c.deletedRows == c.rowsPerChunk {
			c.deletedRows = 0
			c.chunks = c.chunks[1:]
		}
	}
}

// Replace substitutes one row for another. This does query the
// MemoryMonitor to determine whether the new row fits the
// allowance.
func (c *RowContainer) Replace(ctx context.Context, i int, newRow parser.Datums) error {
	newSz := c.rowSize(newRow)
	row := c.At(i)
	oldSz := c.rowSize(row)
	if newSz != oldSz {
		if err := c.memAcc.ResizeItem(ctx, oldSz, newSz); err != nil {
			return err
		}
	}
	copy(row, newRow)
	return nil
}
