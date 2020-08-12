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
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// Batch is the type that columnar operators receive and produce. It
// represents a set of column vectors (partial data columns) as well as
// metadata about a batch, like the selection vector (which rows in the column
// batch are selected).
type Batch interface {
	// Length returns the number of values in the columns in the batch.
	Length() int
	// SetLength sets the number of values in the columns in the batch.
	SetLength(int)
	// Width returns the number of columns in the batch.
	Width() int
	// ColVec returns the ith Vec in this batch.
	ColVec(i int) Vec
	// ColVecs returns all of the underlying Vecs in this batch.
	ColVecs() []Vec
	// Selection, if not nil, returns the selection vector on this batch: a
	// densely-packed list of the indices in each column that have not been
	// filtered out by a previous step.
	Selection() []int
	// SetSelection sets whether this batch is using its selection vector or not.
	SetSelection(bool)
	// AppendCol appends the given Vec to this batch.
	AppendCol(Vec)
	// ReplaceCol replaces the current Vec at the provided index with the
	// provided Vec. The original and the replacement vectors *must* be of the
	// same type.
	ReplaceCol(Vec, int)
	// Reset modifies the caller in-place to have the given length and columns
	// with the given types. If it's possible, Reset will reuse the existing
	// columns and allocations, invalidating existing references to the Batch or
	// its Vecs. However, Reset does _not_ zero out the column data.
	//
	// NOTE: Reset can allocate a new Batch, so when calling from the vectorized
	// engine consider either allocating a new Batch explicitly via
	// colexec.Allocator or calling ResetInternalBatch.
	Reset(typs []*types.T, length int, factory ColumnFactory)
	// ResetInternalBatch resets a batch and its underlying Vecs for reuse. It's
	// important for callers to call ResetInternalBatch if they own internal
	// batches that they reuse as not doing this could result in correctness
	// or memory blowup issues.
	ResetInternalBatch()
	// String returns a pretty representation of this batch.
	String() string
}

var _ Batch = &MemBatch{}

// TODO(jordan): tune.
const defaultBatchSize = 1024

var batchSize int64 = defaultBatchSize

// BatchSize is the maximum number of tuples that fit in a column batch.
func BatchSize() int {
	return int(atomic.LoadInt64(&batchSize))
}

// MaxBatchSize is the maximum acceptable size of batches.
const MaxBatchSize = 4096

// SetBatchSizeForTests modifies batchSize variable. It should only be used in
// tests. batch sizes greater than MaxBatchSize will return an error.
func SetBatchSizeForTests(newBatchSize int) error {
	if newBatchSize > MaxBatchSize {
		return errors.Errorf("batch size %d greater than maximum allowed batch size %d", newBatchSize, MaxBatchSize)
	}
	atomic.SwapInt64(&batchSize, int64(newBatchSize))
	return nil
}

// ResetBatchSizeForTests resets the batchSize variable to the default batch
// size. It should only be used in tests.
func ResetBatchSizeForTests() {
	atomic.SwapInt64(&batchSize, defaultBatchSize)
}

// NewMemBatch allocates a new in-memory Batch. An unsupported type will create
// a placeholder Vec that may not be accessed.
// TODO(jordan): pool these allocations.
func NewMemBatch(typs []*types.T, factory ColumnFactory) Batch {
	return NewMemBatchWithSize(typs, BatchSize(), factory)
}

// NewMemBatchWithSize allocates a new in-memory Batch with the given column
// size. Use for operators that have a precisely-sized output batch.
func NewMemBatchWithSize(typs []*types.T, size int, factory ColumnFactory) Batch {
	b := NewMemBatchNoCols(typs, size).(*MemBatch)
	for i, t := range typs {
		b.b[i] = NewMemColumn(t, size, factory)
		if b.b[i].CanonicalTypeFamily() == types.BytesFamily {
			b.bytesVecIdxs = append(b.bytesVecIdxs, i)
		}
	}
	return b
}

// NewMemBatchNoCols creates a "skeleton" of new in-memory Batch. It allocates
// memory for the selection vector but does *not* allocate any memory for the
// column vectors - those will have to be added separately.
func NewMemBatchNoCols(typs []*types.T, size int) Batch {
	if max := math.MaxUint16; size > max {
		panic(fmt.Sprintf(`batches cannot have length larger than %d; requested %d`, max, size))
	}
	b := &MemBatch{}
	b.b = make([]Vec, len(typs))
	b.sel = make([]int, size)
	return b
}

// ZeroBatch is a schema-less Batch of length 0.
var ZeroBatch = &zeroBatch{
	MemBatch: NewMemBatchWithSize(
		nil /* types */, 0 /* size */, StandardColumnFactory,
	).(*MemBatch),
}

// zeroBatch is a wrapper around MemBatch that prohibits modifications of the
// batch.
type zeroBatch struct {
	*MemBatch
}

var _ Batch = &zeroBatch{}

func (b *zeroBatch) Length() int {
	return 0
}

func (b *zeroBatch) SetLength(int) {
	panic("length should not be changed on zero batch")
}

func (b *zeroBatch) SetSelection(bool) {
	panic("selection should not be changed on zero batch")
}

func (b *zeroBatch) AppendCol(Vec) {
	panic("no columns should be appended to zero batch")
}

func (b *zeroBatch) ReplaceCol(Vec, int) {
	panic("no columns should be replaced in zero batch")
}

func (b *zeroBatch) Reset([]*types.T, int, ColumnFactory) {
	panic("zero batch should not be reset")
}

// MemBatch is an in-memory implementation of Batch.
type MemBatch struct {
	// length of batch or sel in tuples
	n int
	// slice of columns in this batch.
	b []Vec
	// bytesVecIdxs stores the indices of all vectors of Bytes type in b. Bytes
	// vectors require special handling, so rather than iterating over all
	// vectors and checking whether they are of Bytes type we store this slice
	// separately.
	bytesVecIdxs []int
	useSel       bool
	// if useSel is true, a selection vector from upstream. a selection vector is
	// a list of selected column indexes in this memBatch's columns.
	sel []int
}

// Length implements the Batch interface.
func (m *MemBatch) Length() int {
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
func (m *MemBatch) Selection() []int {
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
func (m *MemBatch) SetLength(n int) {
	m.n = n
	if n > 0 {
		for _, bytesVecIdx := range m.bytesVecIdxs {
			m.b[bytesVecIdx].Bytes().UpdateOffsetsToBeNonDecreasing(n)
		}
	}
}

// AppendCol implements the Batch interface.
func (m *MemBatch) AppendCol(col Vec) {
	if col.CanonicalTypeFamily() == types.BytesFamily {
		m.bytesVecIdxs = append(m.bytesVecIdxs, len(m.b))
	}
	m.b = append(m.b, col)
}

// ReplaceCol implements the Batch interface.
func (m *MemBatch) ReplaceCol(col Vec, colIdx int) {
	if m.b[colIdx] != nil && !m.b[colIdx].Type().Identical(col.Type()) {
		panic(fmt.Sprintf("unexpected replacement: original vector is %s "+
			"whereas the replacement is %s", m.b[colIdx].Type(), col.Type()))
	}
	m.b[colIdx] = col
}

// Reset implements the Batch interface.
func (m *MemBatch) Reset(typs []*types.T, length int, factory ColumnFactory) {
	// The columns are always sized the same as the selection vector, so use it as
	// a shortcut for the capacity (like a go slice, the batch's `Length` could be
	// shorter than the capacity). We could be more defensive and type switch
	// every column to verify its capacity, but that doesn't seem necessary yet.
	cannotReuse := m == nil || len(m.sel) < length || m.Width() < len(typs)
	for i := 0; i < len(typs) && !cannotReuse; i++ {
		// TODO(yuzefovich): change this when DatumVec is introduced.
		// TODO(yuzefovich): requiring that types are "identical" might be an
		// overkill - the vectors could have the same physical representation
		// but non-identical types. Think through this more.
		if !m.ColVec(i).Type().Identical(typs[i]) {
			cannotReuse = true
		}
	}
	if cannotReuse {
		*m = *NewMemBatchWithSize(typs, length, factory).(*MemBatch)
		m.SetLength(length)
		return
	}
	// Yay! We can reuse m. NB It's not specified in the Reset contract, but
	// probably a good idea to keep all modifications below this line.
	m.SetLength(length)
	m.sel = m.sel[:length]
	m.b = m.b[:len(typs)]
	for i, idx := range m.bytesVecIdxs {
		if idx >= len(typs) {
			m.bytesVecIdxs = m.bytesVecIdxs[:i]
			break
		}
	}
	m.ResetInternalBatch()
}

// ResetInternalBatch implements the Batch interface.
func (m *MemBatch) ResetInternalBatch() {
	m.SetSelection(false)
	for _, v := range m.b {
		if v.CanonicalTypeFamily() != types.UnknownFamily {
			v.Nulls().UnsetNulls()
		}
	}
	for _, bytesVecIdx := range m.bytesVecIdxs {
		m.b[bytesVecIdx].Bytes().Reset()
	}
}

// String returns a pretty representation of this batch.
func (m *MemBatch) String() string {
	if m.Length() == 0 {
		return "[zero-length batch]"
	}
	var builder strings.Builder
	strs := make([]string, len(m.ColVecs()))
	for i := 0; i < m.Length(); i++ {
		builder.WriteString("\n[")
		for colIdx, v := range m.ColVecs() {
			strs[colIdx] = fmt.Sprintf("%v", GetValueAt(v, i))
		}
		builder.WriteString(strings.Join(strs, ", "))
		builder.WriteString("]")
	}
	builder.WriteString("\n")
	return builder.String()
}
