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
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

// RowContainer is a container for rows of DTuples which tracks the
// approximate amount of memory allocated for row data.
// Rows must be added using AddRow(); once the work is done
// the Close() method must be called to release the allocated memory.
//
// TODO(knz): this does not currently track the amount of memory used
// for the outer array of DTuple references.
type RowContainer struct {
	p    *planner
	rows []parser.DTuple

	// fixedColsSize is the sum of widths of fixed-width columns in a
	// single row.
	fixedColsSize int64
	// varSizedColumns indicates for which columns the datum size
	// is variable.
	varSizedColumns []int

	// memAcc tracks the current memory consumption of this
	// RowContainer.
	memAcc WrappableMemoryAccount
}

// NewRowContainer allocates a new row container.
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
func (p *planner) NewRowContainer(h ResultColumns, rowCapacity int) *RowContainer {
	nCols := len(h)

	res := &RowContainer{
		p:               p,
		rows:            make([]parser.DTuple, 0, rowCapacity),
		varSizedColumns: make([]int, 0, nCols),
		memAcc:          p.session.OpenAccount(),
	}

	for i := 0; i < nCols; i++ {
		sz, variable := h[i].Typ.Size()
		if variable {
			res.varSizedColumns = append(res.varSizedColumns, i)
		} else {
			res.fixedColsSize += int64(sz)
		}
	}
	res.fixedColsSize += int64(unsafe.Sizeof(parser.Datum(nil)) * uintptr(nCols))

	return res
}

// Close releases the memory associated with the RowContainer.
func (c *RowContainer) Close() {
	c.rows = nil
	c.varSizedColumns = nil
	c.memAcc.W(c.p.session).Close()
}

// rowSize computes the size of a single row.
func (c *RowContainer) rowSize(row parser.DTuple) int64 {
	rsz := c.fixedColsSize
	for _, i := range c.varSizedColumns {
		rsz += int64(row[i].Size())
	}
	return rsz
}

// AddRow attempts to insert a new row in the RowContainer.
// Returns an error if the allocation was denied by the MemoryUsageMonitor.
func (c *RowContainer) AddRow(row parser.DTuple) error {
	if err := c.memAcc.W(c.p.session).Grow(c.rowSize(row)); err != nil {
		return err
	}
	c.rows = append(c.rows, row)
	return nil
}

// Len reports the number of rows currently held in this RowContainer.
func (c *RowContainer) Len() int {
	return len(c.rows)
}

// At accesses a row at a specific index.
func (c *RowContainer) At(i int) parser.DTuple {
	return c.rows[i]
}

// Swap exchanges two rows. Used for sorting.
func (c *RowContainer) Swap(i, j int) {
	c.rows[i], c.rows[j] = c.rows[j], c.rows[i]
}

// PseudoPop retrieves a pointer to the last row, and decreases the
// visible size of the RowContainer.  This is used for heap sorting in
// sql.sortNode.  A pointer is returned to avoid an allocation when
// passing the DTuple as an interface{} to heap.Push().
// Note that the pointer is only valid until the next call to AddRow.
// We use this for heap sorting in sort.go.
func (c *RowContainer) PseudoPop() *parser.DTuple {
	idx := len(c.rows) - 1
	x := &(c.rows)[idx]
	c.rows = c.rows[:idx]
	return x
}

// ResetLen cancels the effects of PseudoPop(), that is, it restores
// the visible size of the RowContainer to its actual size.
func (c *RowContainer) ResetLen(l int) {
	c.rows = c.rows[:l]
}

// Replace substitutes one row for another. This does query the
// MemoryUsageMonitor to determine whether the new row fits the
// allowance.
func (c *RowContainer) Replace(i int, newRow parser.DTuple) error {
	newSz := c.rowSize(newRow)
	oldSz := int64(0)
	if c.rows[i] != nil {
		oldSz = c.rowSize(c.rows[i])
	}
	if newSz != oldSz {
		if err := c.memAcc.W(c.p.session).ResizeItem(oldSz, newSz); err != nil {
			return err
		}
	}
	c.rows[i] = newRow
	return nil
}
