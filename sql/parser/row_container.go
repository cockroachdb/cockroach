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

package parser

import "unsafe"

// MemoryUsageMonitor is the interface via which row data allocations
// are checked for allowable usage.
type MemoryUsageMonitor interface {
	ReserveMemory(int64) error
	ReleaseMemory(int64)
}

// ColumnHeader serves as interface for the definition of columns to
// initialize a RowContainer. This is really sql.ResultColumns,
// defined here via an interface to avoid a circular dependency.
type ColumnHeader interface {
	NumColumns() int
	ColumnType(col int) Datum
}

// RowContainer is a managed container for rows of DTuples.
type RowContainer struct {
	mon  MemoryUsageMonitor
	rows []DTuple

	// fixedRowSize is the sum of widths of fixed-width columns.
	fixedRowSize int64
	// varSizedColumns indicates for which columns the datum size
	// is variable
	varSizedColumns []int

	// allocated is the current number of bytes allocated in this
	// RowContainer.
	allocated int64
}

// NewRowContainer allocates a new row container.
func NewRowContainer(mon MemoryUsageMonitor, h ColumnHeader, capacity int) *RowContainer {
	nCols := h.NumColumns()

	res := &RowContainer{
		mon:             mon,
		rows:            make([]DTuple, 0, capacity),
		varSizedColumns: make([]int, 0, nCols),
	}

	for i := 0; i < nCols; i++ {
		typ := h.ColumnType(i)
		sz, variable := typ.Size()
		if variable {
			res.varSizedColumns = append(res.varSizedColumns, i)
		} else {
			res.fixedRowSize += int64(sz)
		}
	}
	res.fixedRowSize += int64(unsafe.Sizeof(make(DTuple, 0, nCols)))

	return res
}

// Close releases the memory associated with the RowContainer.
func (c *RowContainer) Close() {
	if c == nil {
		return
	}
	c.rows = nil
	c.mon.ReleaseMemory(c.allocated)
	c.varSizedColumns = nil
	c.allocated = 0
}

// rowSize computes the size of a single row.
func (c *RowContainer) rowSize(row DTuple) int64 {
	rsz := c.fixedRowSize
	for _, i := range c.varSizedColumns {
		sz, _ := row[i].Size()
		rsz += int64(sz)
	}
	return rsz
}

// AddRow attempts to insert a new row in the RowContainer.
// Returns an error if the allocation was denied by the MemoryUsageMonitor.
func (c *RowContainer) AddRow(row DTuple) error {
	rowSize := c.rowSize(row)
	if err := c.mon.ReserveMemory(rowSize); err != nil {
		return err
	}
	c.allocated += rowSize
	c.rows = append(c.rows, row)
	return nil
}

// Len reports the number of rows currently held in this RowContainer.
func (c *RowContainer) Len() int {
	return len(c.rows)
}

// At accesses a row at a specific index.
func (c *RowContainer) At(i int) DTuple {
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
func (c *RowContainer) PseudoPop() *DTuple {
	idx := len(c.rows) - 1
	x := &(c.rows)[idx]
	c.rows = c.rows[:idx]
	return x
}

// ResetLen cancels the effects of PseudoPop(), that is, it restores
// the visible size of the RowContainer to its actual size.
func (c *RowContainer) ResetLen(len int) {
	c.rows = c.rows[:len]
}

// Replace substitute one row for another. This does enquire to the
// MemoryUsageMonitor whether the new row fits the allowance.
func (c *RowContainer) Replace(i int, newRow DTuple) error {
	newSz := c.rowSize(newRow)
	oldSz := int64(0)
	if c.rows[i] != nil {
		oldSz = c.rowSize(c.rows[i])
	}
	if newSz != oldSz {
		c.mon.ReleaseMemory(oldSz)
		if err := c.mon.ReserveMemory(newSz); err != nil {
			return err
		}
		c.allocated += newSz - oldSz
	}
	c.rows[i] = newRow
	return nil
}
