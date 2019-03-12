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

import "github.com/cockroachdb/cockroach/pkg/sql/exec/types"

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
	// Vec returns the ith Vec in this batch.
	ColVec(i int) Vec
	// ColVecs returns all of the underlying ColVecs in this batch.
	ColVecs() []Vec
	// Selection, if not nil, returns the selection vector on this batch: a
	// densely-packed list of the indices in each column that have not been
	// filtered out by a previous step.
	Selection() []uint16
	// SetSelection sets whether this batch is using its selection vector or not.
	SetSelection(bool)
	// AppendCol appends a Vec with the specified type to this batch.
	AppendCol(types.T)
}

var _ Batch = &memBatch{}

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
	b := &memBatch{}
	b.b = make([]Vec, len(types))

	for i, t := range types {
		b.b[i] = NewMemColumn(t, size)
	}
	b.sel = make([]uint16, size)

	return b
}

type memBatch struct {
	// length of batch or sel in tuples
	n uint16
	// slice of columns in this batch.
	b      []Vec
	useSel bool
	// if useSel is true, a selection vector from upstream. a selection vector is
	// a list of selected column indexes in this memBatch's columns.
	sel []uint16
}

func (m *memBatch) Length() uint16 {
	return m.n
}

func (m *memBatch) Width() int {
	return len(m.b)
}

func (m *memBatch) ColVec(i int) Vec {
	return m.b[i]
}

func (m *memBatch) ColVecs() []Vec {
	return m.b
}

func (m *memBatch) Selection() []uint16 {
	if !m.useSel {
		return nil
	}
	return m.sel
}

func (m *memBatch) SetSelection(b bool) {
	m.useSel = b
}

func (m *memBatch) SetLength(n uint16) {
	m.n = n
}

func (m *memBatch) AppendCol(t types.T) {
	m.b = append(m.b, NewMemColumn(t, BatchSize))
}
