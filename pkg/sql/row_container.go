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

package sql

import (
	"fmt"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

// targetChunkSize is the target number of Datums in a RowContainer chunk.
const targetChunkSize = 64

// RowContainer is a container for rows of DTuples which tracks the
// approximate amount of memory allocated for row data.
// Rows must be added using AddRow(); once the work is done
// the Close() method must be called to release the allocated memory.
//
// TODO(knz): this does not currently track the amount of memory used
// for the outer array of DTuple references.
type RowContainer struct {
	numCols int

	// rowsPerChunk is the number of rows in a chunk; we pack multiple rows in a
	// single []Datum to reduce the overhead of the slice if we have few columns.
	rowsPerChunk int
	chunks       [][]parser.Datum
	numRows      int

	// fixedColsSize is the sum of widths of fixed-width columns in a
	// single row.
	fixedColsSize int64
	// varSizedColumns indicates for which columns the datum size
	// is variable.
	varSizedColumns []int

	// memAcc tracks the current memory consumption of this
	// RowContainer.
	memAcc mon.BoundAccount
}

func (c *RowContainer) allocChunks(numChunks int) {
	datumsPerChunk := c.rowsPerChunk * c.numCols

	datums := make([]parser.Datum, numChunks*datumsPerChunk)
	for i, pos := 0, 0; i < numChunks; i++ {
		c.chunks = append(c.chunks, datums[pos:pos+datumsPerChunk])
		pos += datumsPerChunk
	}
}

// NewRowContainer allocates a new row container.
//
// The acc argument indicates where to register memory allocations by
// this row container. Should probably be created by
// Session.makeSessionBoundAccount() or Session.makeTxnBoundAccount().
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
func NewRowContainer(acc mon.BoundAccount, h ResultColumns, rowCapacity int) *RowContainer {
	nCols := len(h)

	c := &RowContainer{
		numCols: nCols,
		memAcc:  acc,
	}

	if nCols != 0 {
		c.rowsPerChunk = (targetChunkSize + nCols - 1) / nCols
		preallocChunks := (rowCapacity + c.rowsPerChunk - 1) / c.rowsPerChunk
		if preallocChunks < 1 {
			preallocChunks = 1
		}
		c.chunks = make([][]parser.Datum, 0, preallocChunks)
		c.allocChunks(preallocChunks)
	}

	for i := 0; i < nCols; i++ {
		sz, variable := h[i].Typ.Size()
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
	c.fixedColsSize += int64(unsafe.Sizeof(parser.Datum(nil)) * uintptr(nCols))

	return c
}

// Close releases the memory associated with the RowContainer.
func (c *RowContainer) Close() {
	c.chunks = nil
	c.varSizedColumns = nil
	c.memAcc.Close()
}

// rowSize computes the size of a single row.
func (c *RowContainer) rowSize(row parser.DTuple) int64 {
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
	row := int32(rowIdx)
	div := int32(c.rowsPerChunk)
	return int(row / div), int(row % div * int32(c.numCols))

}

// AddRow attempts to insert a new row in the RowContainer. The row slice is not
// used directly: the Datums inside the DTuple are copied to internal storage.
// Returns an error if the allocation was denied by the MemoryMonitor.
func (c *RowContainer) AddRow(row parser.DTuple) error {
	if len(row) != c.numCols {
		panic(fmt.Sprintf("invalid row length %d, expected %d", len(row), c.numCols))
	}
	if err := c.memAcc.Grow(c.rowSize(row)); err != nil {
		return err
	}
	if c.numCols != 0 {
		chunk, pos := c.getChunkAndPos(c.numRows)
		if chunk == len(c.chunks) {
			// Grow the number of chunks by a fraction.
			c.allocChunks(1 + len(c.chunks)/8)
		}
		copy(c.chunks[chunk][pos:pos+c.numCols], row)
	}
	c.numRows++
	return nil
}

// Len reports the number of rows currently held in this RowContainer.
func (c *RowContainer) Len() int {
	return c.numRows
}

// NumCols reports the number of columns held in this RowContainer.
func (c *RowContainer) NumCols() int {
	return c.numCols
}

// At accesses a row at a specific index.
func (c *RowContainer) At(i int) parser.DTuple {
	if i < 0 || i >= c.numRows {
		panic(fmt.Sprintf("row index %d out of range", i))
	}
	if c.numCols == 0 {
		return nil
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

// Replace substitutes one row for another. This does query the
// MemoryMonitor to determine whether the new row fits the
// allowance.
func (c *RowContainer) Replace(i int, newRow parser.DTuple) error {
	newSz := c.rowSize(newRow)
	row := c.At(i)
	oldSz := c.rowSize(row)
	if newSz != oldSz {
		if err := c.memAcc.ResizeItem(oldSz, newSz); err != nil {
			return err
		}
	}
	copy(row, newRow)
	return nil
}
