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

	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// Batch is the type that columnar operators receive and produce. It
// represents a set of column vectors (partial data columns) as well as
// metadata about a batch, like the selection vector (which rows in the column
// batch are selected).
type Batch interface {
	// Length returns the number of values in the columns in the batch.
	Length() int
	// SetLength sets the number of values in the columns in the batch. Note
	// that if the selection vector will be set or updated on the batch, it must
	// be set **before** setting the length.
	SetLength(int)
	// Capacity returns the maximum number of values that can be stored in the
	// columns in the batch. Note that it could be a lower bound meaning some
	// of the Vecs could actually have larger underlying capacity (for example,
	// if they have been appended to).
	Capacity() int
	// Width returns the number of columns in the batch.
	Width() int
	// ColVec returns the ith Vec in this batch.
	ColVec(i int) Vec
	// ColVecs returns all of the underlying Vecs in this batch.
	ColVecs() []Vec
	// Selection, if not nil, returns the selection vector on this batch: a
	// densely-packed list of the *increasing* indices in each column that have
	// not been filtered out by a previous step.
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
	// or memory blowup issues. It unsets the selection and sets the length to
	// 0. Notably, it deeply resets the datum-backed vectors and returns the
	// number of bytes released as a result of the reset.
	ResetInternalBatch() int64
	// String returns a pretty representation of this batch.
	String() string
}

var _ Batch = &MemBatch{}

// defaultBatchSize is the size of batches that is used in the non-test setting.
// Initially, 1024 was picked based on MonetDB/X100 paper and was later
// confirmed to be very good using tpchvec/bench benchmark on TPC-H queries
// (the best number according to that benchmark was 1280, but it was negligibly
// better, so we decided to keep 1024 as it is a power of 2).
var defaultBatchSize = int64(util.ConstantWithMetamorphicTestRange(
	"coldata-batch-size",
	1024, /* defaultValue */
	// min is set to 3 to match colexec's minBatchSize setting.
	3, /* min */
	MaxBatchSize,
))

var batchSize = defaultBatchSize

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

// NewMemBatch allocates a new in-memory Batch.
// TODO(jordan): pool these allocations.
func NewMemBatch(typs []*types.T, factory ColumnFactory) Batch {
	return NewMemBatchWithCapacity(typs, BatchSize(), factory)
}

// NewMemBatchWithCapacity allocates a new in-memory Batch with the given
// column size. Use for operators that have a precisely-sized output batch.
func NewMemBatchWithCapacity(typs []*types.T, capacity int, factory ColumnFactory) Batch {
	b := NewMemBatchNoCols(typs, capacity).(*MemBatch)
	cols := make([]memColumn, len(typs))
	for i, t := range typs {
		col := &cols[i]
		col.init(t, capacity, factory)
		b.b[i] = col
		if col.IsBytesLike() {
			b.bytesVecIdxs.Add(i)
		} else if col.CanonicalTypeFamily() == typeconv.DatumVecCanonicalTypeFamily {
			b.datumVecIdxs.Add(i)
		}
	}
	return b
}

// NewMemBatchNoCols creates a "skeleton" of new in-memory Batch. It allocates
// memory for the selection vector but does *not* allocate any memory for the
// column vectors - those will have to be added separately.
func NewMemBatchNoCols(typs []*types.T, capacity int) Batch {
	if max := math.MaxUint16; capacity > max {
		panic(fmt.Sprintf(`batches cannot have capacity larger than %d; requested %d`, max, capacity))
	}
	b := &MemBatch{}
	b.capacity = capacity
	b.b = make([]Vec, len(typs))
	b.sel = make([]int, capacity)
	return b
}

// ZeroBatch is a schema-less Batch of length 0.
var ZeroBatch = &zeroBatch{
	MemBatch: NewMemBatchWithCapacity(
		nil /* typs */, 0 /* capacity */, StandardColumnFactory,
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

func (b *zeroBatch) Capacity() int {
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
	// length is the length of batch or sel in tuples.
	length int
	// capacity is the maximum number of tuples that can be stored in this
	// MemBatch.
	capacity int
	// b is the slice of columns in this batch.
	b []Vec
	// bytesVecIdxs stores the indices of all vectors of Bytes type in b. Bytes
	// vectors require special handling, so rather than iterating over all
	// vectors and checking whether they are of Bytes type we store this slice
	// separately.
	bytesVecIdxs util.FastIntSet
	// datumVecIdxs stores the indices of all datum-backed vectors in b.
	datumVecIdxs util.FastIntSet
	useSel       bool
	// sel is - if useSel is true - a selection vector from upstream. A
	// selection vector is a list of selected tuple indices in this memBatch's
	// columns (tuples for which indices are not in sel are considered to be
	// "not present").
	sel []int
}

// Length implements the Batch interface.
func (m *MemBatch) Length() int {
	return m.length
}

// Capacity implements the Batch interface.
func (m *MemBatch) Capacity() int {
	return m.capacity
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
func (m *MemBatch) SetLength(length int) {
	m.length = length
	if length > 0 {
		// In order to maintain the invariant of Bytes vectors we need to update
		// offsets up to the element with the largest index that can be accessed
		// by the batch.
		maxIdx := length - 1
		if m.useSel {
			// Note that here we rely on the fact that selection vectors are
			// increasing sequences.
			maxIdx = m.sel[length-1]
		}
		for i, ok := m.bytesVecIdxs.Next(0); ok; i, ok = m.bytesVecIdxs.Next(i + 1) {
			UpdateOffsetsToBeNonDecreasing(m.b[i], maxIdx+1)
		}
	}
}

// AppendCol implements the Batch interface.
func (m *MemBatch) AppendCol(col Vec) {
	if col.IsBytesLike() {
		m.bytesVecIdxs.Add(len(m.b))
	} else if col.CanonicalTypeFamily() == typeconv.DatumVecCanonicalTypeFamily {
		m.datumVecIdxs.Add(len(m.b))
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
	cannotReuse := m == nil || m.Capacity() < length || m.Width() < len(typs)
	for i := 0; i < len(typs) && !cannotReuse; i++ {
		// TODO(yuzefovich): change this when DatumVec is introduced.
		// TODO(yuzefovich): requiring that types are "identical" might be an
		// overkill - the vectors could have the same physical representation
		// but non-identical types. Think through this more.
		if !m.ColVec(i).Type().Identical(typs[i]) {
			cannotReuse = true
			break
		}
	}
	if cannotReuse {
		*m = *NewMemBatchWithCapacity(typs, length, factory).(*MemBatch)
		m.SetLength(length)
		return
	}
	// Yay! We can reuse m. NB It's not specified in the Reset contract, but
	// probably a good idea to keep all modifications below this line.
	//
	// Note that we're intentionally not calling m.SetLength() here because
	// that would update offsets in the bytes vectors which is not necessary
	// since those will get reset in ResetInternalBatch anyway.
	m.b = m.b[:len(typs)]
	m.sel = m.sel[:length]
	for i, ok := m.bytesVecIdxs.Next(0); ok; i, ok = m.bytesVecIdxs.Next(i + 1) {
		if i >= len(typs) {
			m.bytesVecIdxs.Remove(i)
		}
	}
	for i, ok := m.datumVecIdxs.Next(0); ok; i, ok = m.datumVecIdxs.Next(i + 1) {
		if i >= len(typs) {
			m.datumVecIdxs.Remove(i)
		}
	}
	m.ResetInternalBatch()
	m.SetLength(length)
}

// ResetInternalBatch implements the Batch interface.
func (m *MemBatch) ResetInternalBatch() int64 {
	m.SetLength(0 /* length */)
	m.SetSelection(false)
	for _, v := range m.b {
		if v.CanonicalTypeFamily() != types.UnknownFamily {
			v.Nulls().UnsetNulls()
		}
	}
	for i, ok := m.bytesVecIdxs.Next(0); ok; i, ok = m.bytesVecIdxs.Next(i + 1) {
		Reset(m.b[i])
	}
	var released int64
	for i, ok := m.datumVecIdxs.Next(0); ok; i, ok = m.datumVecIdxs.Next(i + 1) {
		released += m.b[i].Datum().Reset()
	}
	return released
}

// String returns a pretty representation of this batch.
func (m *MemBatch) String() string {
	if m.Length() == 0 {
		return "[zero-length batch]"
	}
	if VecsToStringWithRowPrefix == nil {
		panic("need to inject the implementation from sql/colconv package")
	}
	return strings.Join(VecsToStringWithRowPrefix(m.ColVecs(), m.Length(), m.Selection(), "" /* prefix */), "\n")
}

// VecsToStringWithRowPrefix returns a pretty representation of the vectors.
// This method will convert all vectors to datums in order to print everything
// in the same manner as the tree.Datum representation does. Each row is printed
// in a separate string.
//
// The implementation lives in colconv package and is injected during the
// initialization.
var VecsToStringWithRowPrefix func(vecs []Vec, length int, sel []int, prefix string) []string
