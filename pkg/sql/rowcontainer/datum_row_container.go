// Copyright 2016 The Cockroach Authors.
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
	"math/bits"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// RowContainer is a container for rows of Datums which tracks the
// approximate amount of memory allocated for row data.
// Rows must be added using AddRow(); once the work is done
// the Close() method must be called to release the allocated memory.
//
// TODO(knz): this does not currently track the amount of memory used
// for the outer array of Datums references.
type RowContainer struct {
	// We should not copy this structure around; each copy would have a
	// different memAcc (among other things like aliasing chunks).
	_ util.NoCopy

	numCols int

	// rowsPerChunk is the number of rows in a chunk; we pack multiple rows in a
	// single []Datum to reduce the overhead of the slice if we have few
	// columns. Must be a power of 2 as determination of the chunk given a row
	// index is performed using shifting.
	rowsPerChunk      int
	rowsPerChunkShift uint
	chunks            [][]tree.Datum
	firstChunk        [1][]tree.Datum // avoids allocation
	numRows           int

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

// NewRowContainer allocates a new row container.
//
// The acc argument indicates where to register memory allocations by
// this row container. Should probably be created by
// Session.makeBoundAccount() or Session.TxnState.makeBoundAccount().
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
func NewRowContainer(acc mon.BoundAccount, ti colinfo.ColTypeInfo) *RowContainer {
	return NewRowContainerWithCapacity(acc, ti, 0)
}

// NewRowContainerWithCapacity is like NewRowContainer, but it accepts a
// rowCapacity argument.
//
// If provided, rowCapacity indicates how many rows are to be expected.
// The value is used to configure the size of chunks that are allocated
// within the container such that if no more than the specific number of
// rows is added to the container, only a single chunk will be allocated
// and wasted space will be kept to a minimum.
func NewRowContainerWithCapacity(
	acc mon.BoundAccount, ti colinfo.ColTypeInfo, rowCapacity int,
) *RowContainer {
	c := &RowContainer{}
	c.Init(acc, ti, rowCapacity)
	return c
}

var rowsPerChunkShift = uint(util.ConstantWithMetamorphicTestValue(
	"row-container-rows-per-chunk-shift",
	6, /* defaultValue */
	1, /* metamorphicValue */
))

// Init can be used instead of NewRowContainer if we have a RowContainer that is
// already part of an on-heap structure.
func (c *RowContainer) Init(acc mon.BoundAccount, ti colinfo.ColTypeInfo, rowCapacity int) {
	nCols := ti.NumColumns()

	c.numCols = nCols
	c.memAcc = acc

	if rowCapacity != 0 {
		// If there is a row capacity provided, we use a single chunk with
		// sufficient capacity. The following is equivalent to:
		//
		//  c.rowsPerChunkShift = ceil(log2(rowCapacity))
		//
		c.rowsPerChunkShift = 64 - uint(bits.LeadingZeros64(uint64(rowCapacity-1)))
	} else if nCols != 0 {
		// If the rows have columns, we use 64 rows per chunk.
		c.rowsPerChunkShift = rowsPerChunkShift
	} else {
		// If there are no columns, every row gets mapped to the first chunk,
		// which ends up being a zero-length slice because each row contains no
		// columns.
		c.rowsPerChunkShift = 32
	}
	c.rowsPerChunk = 1 << c.rowsPerChunkShift

	for i := 0; i < nCols; i++ {
		sz, variable := tree.DatumTypeSize(ti.Type(i))
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

	if nCols > 0 {
		// Precalculate the memory used for a chunk, specifically by the Datums
		// in the chunk and the slice pointing at the chunk.
		// Note that when there are no columns, we simply track the number of
		// rows added in c.numRows and don't allocate any memory.
		c.chunkMemSize = tree.SizeOfDatum * int64(c.rowsPerChunk*c.numCols)
		c.chunkMemSize += tree.SizeOfDatums
	}
}

// Clear resets the container and releases the associated memory. This allows
// the RowContainer to be reused.
func (c *RowContainer) Clear(ctx context.Context) {
	c.chunks = nil
	c.numRows = 0
	c.deletedRows = 0
	c.memAcc.Clear(ctx)
}

// UnsafeReset resets the container without releasing the associated memory. This
// allows the RowContainer to be reused, but keeps the previously-allocated
// buffers around for reuse. This is desirable if this RowContainer will be used
// and reset many times in the course of a computation before eventually being
// discarded. It's unsafe because it immediately renders all previously
// allocated rows unsafe - they might be overwritten without notice. This is
// only safe to use if it's guaranteed that all previous rows retrieved by At
// have been copied or otherwise not retained.
func (c *RowContainer) UnsafeReset(ctx context.Context) error {
	c.numRows = 0
	c.deletedRows = 0
	return c.memAcc.ResizeTo(ctx, int64(len(c.chunks))*c.chunkMemSize)
}

// Close releases the memory associated with the RowContainer.
func (c *RowContainer) Close(ctx context.Context) {
	if c == nil {
		// Allow Close on an uninitialized container.
		return
	}
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
		if numChunks == 1 {
			c.chunks = c.firstChunk[:0:1]
		} else {
			c.chunks = make([][]tree.Datum, 0, numChunks)
		}
	}

	datums := make([]tree.Datum, numChunks*datumsPerChunk)
	for i, pos := 0, 0; i < numChunks; i++ {
		c.chunks = append(c.chunks, datums[pos:pos+datumsPerChunk])
		pos += datumsPerChunk
	}
	return nil
}

// rowSize computes the size of a single row.
func (c *RowContainer) rowSize(row tree.Datums) int64 {
	rsz := c.fixedColsSize
	for _, i := range c.varSizedColumns {
		rsz += int64(row[i].Size())
	}
	return rsz
}

// getChunkAndPos returns the chunk index and the position inside the chunk for
// a given row index.
func (c *RowContainer) getChunkAndPos(rowIdx int) (chunk int, pos int) {
	// This is a hot path; use shifting to avoid division.
	row := rowIdx + c.deletedRows
	chunk = row >> c.rowsPerChunkShift
	return chunk, (row - (chunk << c.rowsPerChunkShift)) * (c.numCols)
}

// AddRow attempts to insert a new row in the RowContainer. The row slice is not
// used directly: the Datum values inside the Datums are copied to internal storage.
// Returns an error if the allocation was denied by the MemoryMonitor.
func (c *RowContainer) AddRow(ctx context.Context, row tree.Datums) (tree.Datums, error) {
	if len(row) != c.numCols {
		panic(errors.AssertionFailedf("invalid row length %d, expected %d", len(row), c.numCols))
	}
	if c.numCols == 0 {
		if c.chunks == nil {
			c.chunks = [][]tree.Datum{{}}
		}
		c.numRows++
		return nil, nil
	}
	// Note that it is important that we perform the memory accounting before
	// actually adding the row.
	if err := c.memAcc.Grow(ctx, c.rowSize(row)); err != nil {
		return nil, err
	}
	chunk, pos := c.getChunkAndPos(c.numRows)
	if chunk == len(c.chunks) {
		// Grow the number of chunks by a fraction.
		numChunks := 1 + len(c.chunks)/8
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

// NumCols reports the number of columns for each row in the container.
func (c *RowContainer) NumCols() int {
	return c.numCols
}

// At accesses a row at a specific index. Note that it does *not* copy the row:
// callers must copy the row if they wish to mutate it.
func (c *RowContainer) At(i int) tree.Datums {
	// This is a hot-path: do not add additional checks here.
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
func (c *RowContainer) PopFirst(ctx context.Context) {
	if c.numRows == 0 {
		panic("no rows added to container, nothing to pop")
	}
	c.numRows--
	if c.numCols != 0 {
		c.deletedRows++
		if c.deletedRows == c.rowsPerChunk {
			// We release the memory for rows in chunks. This includes the
			// chunk slice (allocated by allocChunks) and the Datums.
			size := c.chunkMemSize
			for i, pos := 0, 0; i < c.rowsPerChunk; i, pos = i+1, pos+c.numCols {
				size += c.rowSize(c.chunks[0][pos : pos+c.numCols])
			}
			// Reset the pointer so the slice can be garbage collected.
			c.chunks[0] = nil
			c.deletedRows = 0
			c.chunks = c.chunks[1:]
			c.memAcc.Shrink(ctx, size)
		}
	}
}

// Replace substitutes one row for another. This does query the
// MemoryMonitor to determine whether the new row fits the
// allowance.
func (c *RowContainer) Replace(ctx context.Context, i int, newRow tree.Datums) error {
	newSz := c.rowSize(newRow)
	row := c.At(i)
	oldSz := c.rowSize(row)
	if newSz != oldSz {
		if err := c.memAcc.Resize(ctx, oldSz, newSz); err != nil {
			return err
		}
	}
	copy(row, newRow)
	return nil
}
