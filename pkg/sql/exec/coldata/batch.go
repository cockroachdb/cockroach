// Copyright 2018 The Cockroach Authors.
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

package coldata

import (
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// Batch is the type that columnar operators receive and produce. It
// represents a set of column vectors (partial data columns) as well as
// metadata about a batch, like the selection vector (which rows in the column
// batch are selected).
type Batch interface {
	// Length returns the number of values in the columns in the batch.
	Length() uint16
	// SetLength sets the number of values in the columns in the batch.
	SetLength(uint16)
	// Width returns the number of columns in the batch.
	Width() int
	// ColVec returns the ith Vec in this batch.
	ColVec(i int) Vec
	// ColVecs returns all of the underlying Vecs in this batch.
	ColVecs() []Vec
	// Selection, if not nil, returns the selection vector on this batch: a
	// densely-packed list of the indices in each column that have not been
	// filtered out by a previous step.
	Selection() []uint16
	// SetSelection sets whether this batch is using its selection vector or not.
	SetSelection(bool)
	// AppendCol appends a Vec with the specified type to this batch.
	AppendCol(types.T)
	// Reset modifies the caller in-place to have the given length and columns
	// with the given types. If it's possible, Reset will reuse the existing
	// columns and allocations, invalidating existing references to the Batch or
	// its Vecs. However, Reset does _not_ zero out the column data.
	Reset(types []types.T, length int)
}

var _ Batch = &MemBatch{}

// BatchSize is the maximum number of tuples that fit in a column batch.
// TODO(jordan): tune
const BatchSize = 1024

// NewMemBatch allocates a new in-memory Batch.
// TODO(jordan): pool these allocations.
func NewMemBatch(types []types.T) Batch {
	return NewMemBatchWithSize(types, BatchSize)
}

// NewMemBatchWithSize allocates a new in-memory Batch with the given column
// size. Use for operators that have a precisely-sized output batch.
func NewMemBatchWithSize(types []types.T, size int) Batch {
	if max := math.MaxUint16; size > max {
		panic(fmt.Sprintf(`batches cannot have length larger than %d; requested %d`, max, size))
	}
	b := &MemBatch{}
	b.b = make([]Vec, len(types))

	for i, t := range types {
		b.b[i] = NewMemColumn(t, size)
	}
	b.sel = make([]uint16, size)

	return b
}

// MemBatch is an in-memory implementation of Batch.
type MemBatch struct {
	// length of batch or sel in tuples
	n uint16
	// slice of columns in this batch.
	b      []Vec
	useSel bool
	// if useSel is true, a selection vector from upstream. a selection vector is
	// a list of selected column indexes in this memBatch's columns.
	sel []uint16
}

// Length implements the Batch interface.
func (m *MemBatch) Length() uint16 {
	return m.n
}

// Width implements the Batch interface.
func (m *MemBatch) Width() int {
	return len(m.b)
}

// ColVec implements the Batch interface.
func (m *MemBatch) ColVec(i int) Vec {
	return m.b[i]
}

// ColVecs implements the Batch interface.
func (m *MemBatch) ColVecs() []Vec {
	return m.b
}

// Selection implements the Batch interface.
func (m *MemBatch) Selection() []uint16 {
	if !m.useSel {
		return nil
	}
	return m.sel
}

// SetSelection implements the Batch interface.
func (m *MemBatch) SetSelection(b bool) {
	m.useSel = b
}

// SetLength implements the Batch interface.
func (m *MemBatch) SetLength(n uint16) {
	m.n = n
}

// AppendCol implements the Batch interface.
func (m *MemBatch) AppendCol(t types.T) {
	m.b = append(m.b, NewMemColumn(t, BatchSize))
}

// Reset implements the Batch interface.
func (m *MemBatch) Reset(types []types.T, length int) {
	// TODO(dan): Reset could be more aggressive about finding columns to reuse.
	if m == nil || int(m.Length()) != length || m.Width() != len(types) {
		*m = *NewMemBatchWithSize(types, length).(*MemBatch)
		m.SetLength(uint16(length))
		return
	}
	for i, col := range m.ColVecs() {
		if col.Type() != types[i] {
			*m = *NewMemBatchWithSize(types, length).(*MemBatch)
			m.SetLength(uint16(length))
			return
		}
	}
	m.SetLength(uint16(length))
	for _, col := range m.ColVecs() {
		col.Nulls().UnsetNulls()
	}
}
