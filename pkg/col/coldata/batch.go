// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldata

import (
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
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
	// AppendCol appends the given Vec to this batch.
	AppendCol(Vec)
	// Reset modifies the caller in-place to have the given length and columns
	// with the given coltypes. If it's possible, Reset will reuse the existing
	// columns and allocations, invalidating existing references to the Batch or
	// its Vecs. However, Reset does _not_ zero out the column data.
	//
	// NOTE: Reset can allocate a new Batch, so when calling from the vectorized
	// engine consider either allocating a new Batch explicitly via
	// colexec.Allocator or calling ResetInternalBatch.
	Reset(types []coltypes.T, length int)
	// ResetInternalBatch resets a batch and its underlying Vecs for reuse. It's
	// important for callers to call ResetInternalBatch if they own internal
	// batches that they reuse as not doing this could result in correctness
	// or memory blowup issues.
	ResetInternalBatch()
}

var _ Batch = &MemBatch{}

const maxBatchSize = 1024

var batchSize = uint16(1024)

// BatchSize is the maximum number of tuples that fit in a column batch.
// TODO(jordan): tune
func BatchSize() uint16 {
	return batchSize
}

// NewMemBatch allocates a new in-memory Batch. A coltypes.Unknown type
// will create a placeholder Vec that may not be accessed.
// TODO(jordan): pool these allocations.
func NewMemBatch(types []coltypes.T) Batch {
	return NewMemBatchWithSize(types, int(BatchSize()))
}

// NewMemBatchWithSize allocates a new in-memory Batch with the given column
// size. Use for operators that have a precisely-sized output batch.
func NewMemBatchWithSize(types []coltypes.T, size int) Batch {
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

// ZeroBatch is a schema-less Batch of length 0.
var ZeroBatch = NewMemBatchWithSize(nil /* types */, 0 /* size */)

func init() {
	ZeroBatch.SetLength(0)
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
	for _, v := range m.b {
		if v.Type() == coltypes.Bytes {
			v.Bytes().UpdateOffsetsToBeNonDecreasing(uint64(n))
		}
	}
}

// AppendCol implements the Batch interface.
func (m *MemBatch) AppendCol(col Vec) {
	m.b = append(m.b, col)
}

// Reset implements the Batch interface.
func (m *MemBatch) Reset(types []coltypes.T, length int) {
	// The columns are always sized the same as the selection vector, so use it as
	// a shortcut for the capacity (like a go slice, the batch's `Length` could be
	// shorter than the capacity). We could be more defensive and type switch
	// every column to verify its capacity, but that doesn't seem necessary yet.
	hasColCapacity := len(m.sel) >= length
	if m == nil || !hasColCapacity || m.Width() < len(types) {
		*m = *NewMemBatchWithSize(types, length).(*MemBatch)
		m.SetLength(uint16(length))
		return
	}
	for i := range types {
		if m.ColVec(i).Type() != types[i] {
			*m = *NewMemBatchWithSize(types, length).(*MemBatch)
			m.SetLength(uint16(length))
			return
		}
	}
	// Yay! We can reuse m. NB It's not specified in the Reset contract, but
	// probably a good idea to keep all modifications below this line.
	m.SetLength(uint16(length))
	m.SetSelection(false)
	m.sel = m.sel[:length]
	m.b = m.b[:len(types)]
	for _, col := range m.ColVecs() {
		col.Nulls().UnsetNulls()
		if col.Type() == coltypes.Bytes {
			col.Bytes().Reset()
		}
	}
}

// ResetInternalBatch implements the Batch interface.
func (m *MemBatch) ResetInternalBatch() {
	m.SetSelection(false)
	for _, v := range m.b {
		if v.Type() != coltypes.Unhandled {
			v.Nulls().UnsetNulls()
		}
		if v.Type() == coltypes.Bytes {
			v.Bytes().Reset()
		}
	}
}
