// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package coldata

import (
	"fmt"
	"math"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
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
	ColVec(i int) *Vec
	// ColVecs returns all of the underlying Vecs in this batch.
	ColVecs() []*Vec
	// Selection - if not nil - returns the selection vector on this batch: a
	// densely-packed list of the *increasing* indices in each column that have
	// not been filtered out by a previous step.
	// TODO(yuzefovich): consider ensuring that the length of the returned slice
	// equals the length of the batch.
	// TODO(yuzefovich): consider using []int16 to reduce memory footprint.
	Selection() []int
	// SetSelection sets whether this batch is using its selection vector or not.
	SetSelection(bool)
	// AppendCol appends the given Vec to this batch.
	AppendCol(*Vec)
	// ReplaceCol replaces the current Vec at the provided index with the
	// provided Vec. The original and the replacement vectors *must* be of the
	// same type.
	ReplaceCol(*Vec, int)
	// Reset modifies the caller in-place to have the given length and columns
	// with the given types. If it's possible, Reset will reuse the existing
	// columns and allocations, invalidating existing references to the Batch or
	// its Vecs. However, Reset does _not_ zero out the column data.
	//
	// NOTE: Reset can allocate a new Batch, so when calling from the vectorized
	// engine consider either allocating a new Batch explicitly via
	// colmem.Allocator or calling ResetInternalBatch.
	Reset(typs []*types.T, length int, factory ColumnFactory)
	// ResetInternalBatch resets a batch and its underlying Vecs for reuse. It's
	// important for callers to call ResetInternalBatch if they own internal
	// batches that they reuse as not doing this could result in correctness
	// or memory blowup issues. It unsets the selection and sets the length to
	// 0.
	ResetInternalBatch()
	// String returns a pretty representation of this batch.
	String() string
}

var _ Batch = &MemBatch{}

// DefaultColdataBatchSize is the default value of coldata-batch-size.
const DefaultColdataBatchSize = 1024

// defaultBatchSize is the size of batches that is used in the non-test setting.
// Initially, 1024 was picked based on MonetDB/X100 paper and was later
// confirmed to be very good using tpchvec/bench benchmark on TPC-H queries
// (the best number according to that benchmark was 1280, but it was negligibly
// better, so we decided to keep 1024 as it is a power of 2).
var defaultBatchSize = int64(metamorphic.ConstantWithTestRange(
	"coldata-batch-size",
	DefaultColdataBatchSize, /* defaultValue */
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

const (
	boolAllocIdx int = iota
	bytesAllocIdx
	int16AllocIdx
	int32AllocIdx
	int64AllocIdx
	float64AllocIdx
	decimalAllocIdx
	timeAllocIdx
	intervalAllocIdx
	jsonAllocIdx
	datumAllocIdx
	numCanonicalTypes
)

//gcassert:inline
func toAllocIdx(canonicalTypeFamily types.Family, width int32) int {
	switch canonicalTypeFamily {
	case types.BoolFamily:
		return boolAllocIdx
	case types.BytesFamily:
		return bytesAllocIdx
	case types.IntFamily:
		switch width {
		case 16:
			return int16AllocIdx
		case 32:
			return int32AllocIdx
		case 0, 64:
			return int64AllocIdx
		default:
			return -1
		}
	case types.FloatFamily:
		return float64AllocIdx
	case types.DecimalFamily:
		return decimalAllocIdx
	case types.TimestampTZFamily:
		return timeAllocIdx
	case types.IntervalFamily:
		return intervalAllocIdx
	case types.JsonFamily:
		return jsonAllocIdx
	case typeconv.DatumVecCanonicalTypeFamily:
		return datumAllocIdx
	default:
		// Return negative number which will cause index out of bounds error if
		// a new natively-supported type is added and this switch is not
		// updated.
		return -1
	}
}

var (
	unexpectedAllocIdxErr = errors.New("unexpected allocIdx")
	datumBackedType       = types.TSQuery
)

func init() {
	if typeconv.TypeFamilyToCanonicalTypeFamily(datumBackedType.Family()) != typeconv.DatumVecCanonicalTypeFamily {
		panic("update datumBackedType to be actually datum-backed")
	}
}

//gcassert:inline
func toCanonicalType(allocIdx int) *types.T {
	switch allocIdx {
	case boolAllocIdx:
		return types.Bool
	case bytesAllocIdx:
		return types.Bytes
	case int16AllocIdx:
		return types.Int2
	case int32AllocIdx:
		return types.Int4
	case int64AllocIdx:
		return types.Int
	case float64AllocIdx:
		return types.Float
	case decimalAllocIdx:
		return types.Decimal
	case timeAllocIdx:
		return types.TimestampTZ
	case intervalAllocIdx:
		return types.Interval
	case jsonAllocIdx:
		return types.Json
	case datumAllocIdx:
		return datumBackedType
	default:
		panic(unexpectedAllocIdxErr)
	}
}

// NewMemBatchWithCapacity allocates a new in-memory Batch with the given
// column size. Use for operators that have a precisely-sized output batch.
func NewMemBatchWithCapacity(typs []*types.T, capacity int, factory ColumnFactory) Batch {
	b := NewMemBatchNoCols(typs, capacity).(*MemBatch)
	vecs := make([]Vec, len(typs))
	// numForCanonicalType will track how many times a particular canonical type
	// family appears in typs.
	//
	//gcassert:noescape
	var numForCanonicalType [numCanonicalTypes]int
	for i, t := range typs {
		vecs[i].t = t
		vecs[i].canonicalTypeFamily = typeconv.TypeFamilyToCanonicalTypeFamily(t.Family())
		b.b[i] = &vecs[i]
		allocIdx := toAllocIdx(vecs[i].canonicalTypeFamily, vecs[i].t.Width())
		numForCanonicalType[allocIdx]++
	}
	var maxSame int
	for _, numColumns := range numForCanonicalType {
		maxSame = max(maxSame, numColumns)
	}
	// First, initialize columns that can be batch-allocated together for the
	// same canonical type.
	if maxSame > 1 {
		scratchColumns := make([]Column, maxSame)
		for allocIdx, numColumns := range numForCanonicalType {
			if numColumns > 1 {
				scratch := scratchColumns[:numColumns]
				t := toCanonicalType(allocIdx)
				factory.MakeColumns(scratch, t, capacity)
				for i := range vecs {
					if toAllocIdx(vecs[i].canonicalTypeFamily, vecs[i].t.Width()) == allocIdx {
						vecs[i].col = scratch[0]
						scratch = scratch[1:]
					}
				}
			}
		}
		if numForCanonicalType[datumAllocIdx] > 1 {
			// We need to set the correct type on each datum-backed vector.
			for i := range vecs {
				if vecs[i].canonicalTypeFamily == typeconv.DatumVecCanonicalTypeFamily {
					vecs[i].Datum().SetType(vecs[i].t)
				}
			}
		}
	}
	// Initialize all remaining columns while also initializing all Nulls
	// bitmaps.
	nullsCap := nullsStorageCap(capacity)
	nullsAlloc := make([]byte, len(typs)*nullsCap)
	for i := range vecs {
		if vecs[i].col == nil {
			vecs[i].col = factory.MakeColumn(vecs[i].t, capacity)
		}
		vecs[i].nulls = newNulls(nullsAlloc[:nullsCap:nullsCap])
		nullsAlloc = nullsAlloc[nullsCap:]
	}
	return b
}

var batchCapacityTooLargeErr = errors.New("batches cannot have capacity larger than max uint16")

// NewMemBatchNoCols creates a "skeleton" of new in-memory Batch. It allocates
// memory for the selection vector but does *not* allocate any memory for the
// column vectors - those will have to be added separately.
func NewMemBatchNoCols(typs []*types.T, capacity int) Batch {
	if capacity > math.MaxUint16 {
		panic(batchCapacityTooLargeErr)
	}
	b := &MemBatch{}
	b.capacity = capacity
	b.b = make([]*Vec, len(typs))
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

func (b *zeroBatch) AppendCol(*Vec) {
	panic("no columns should be appended to zero batch")
}

func (b *zeroBatch) ReplaceCol(*Vec, int) {
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
	b      []*Vec
	useSel bool
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
func (m *MemBatch) ColVec(i int) *Vec {
	return m.b[i]
}

// ColVecs implements the Batch interface.
func (m *MemBatch) ColVecs() []*Vec {
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
}

// AppendCol implements the Batch interface.
func (m *MemBatch) AppendCol(col *Vec) {
	m.b = append(m.b, col)
}

// ReplaceCol implements the Batch interface.
func (m *MemBatch) ReplaceCol(col *Vec, colIdx int) {
	if m.b[colIdx] != nil && m.b[colIdx].t != nil && !m.b[colIdx].Type().Identical(col.Type()) {
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
		if v := m.ColVec(i); !v.Type().Identical(typs[i]) {
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
	m.ResetInternalBatch()
	m.SetLength(length)
}

// ResetInternalBatch implements the Batch interface.
func (m *MemBatch) ResetInternalBatch() {
	m.SetLength(0 /* length */)
	m.SetSelection(false)
	for _, v := range m.b {
		if v.CanonicalTypeFamily() != types.UnknownFamily {
			v.Nulls().UnsetNulls()
			ResetIfBytesLike(v)
		}
	}
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
var VecsToStringWithRowPrefix func(vecs []*Vec, length int, sel []int, prefix string) []string

// GetBatchMemSize returns the total memory footprint of the batch.
//
// The implementation lives in the sql/colmem package since it depends on
// sem/tree, and we don't want to make coldata depend on that.
var GetBatchMemSize func(Batch) int64
