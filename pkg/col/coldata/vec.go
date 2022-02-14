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

	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// Column is an interface that represents a raw array of a Go native type.
type Column interface {
	// Len returns the number of elements in the Column.
	Len() int
}

// SliceArgs represents the arguments passed in to Vec.Append and Nulls.set.
type SliceArgs struct {
	// Src is the data being appended.
	Src Vec
	// Sel is an optional slice specifying indices to append to the destination
	// slice. Note that Src{Start,End}Idx apply to Sel.
	Sel []int
	// DestIdx is the first index that Append will append to.
	DestIdx int
	// SrcStartIdx is the index of the first element in Src that Append will
	// append.
	SrcStartIdx int
	// SrcEndIdx is the exclusive end index of Src. i.e. the element in the index
	// before SrcEndIdx is the last element appended to the destination slice,
	// similar to Src[SrcStartIdx:SrcEndIdx].
	SrcEndIdx int
}

// Vec is an interface that represents a column vector that's accessible by
// Go native types.
type Vec interface {
	// Type returns the type of data stored in this Vec. Consider whether
	// CanonicalTypeFamily() should be used instead.
	Type() *types.T
	// CanonicalTypeFamily returns the canonical type family of data stored in
	// this Vec.
	CanonicalTypeFamily() types.Family
	// IsBytesLike returns true if this data is stored with a flat bytes
	// representation.
	IsBytesLike() bool

	// Bool returns a bool list.
	Bool() Bools
	// Int16 returns an int16 slice.
	Int16() Int16s
	// Int32 returns an int32 slice.
	Int32() Int32s
	// Int64 returns an int64 slice.
	Int64() Int64s
	// Float64 returns a float64 slice.
	Float64() Float64s
	// Bytes returns a flat Bytes representation.
	Bytes() *Bytes
	// Decimal returns an apd.Decimal slice.
	Decimal() Decimals
	// Timestamp returns a time.Time slice.
	Timestamp() Times
	// Interval returns a duration.Duration slice.
	Interval() Durations
	// JSON returns a vector of JSONs.
	JSON() *JSONs
	// Datum returns a vector of Datums.
	Datum() DatumVec

	// Col returns the raw, typeless backing storage for this Vec.
	Col() Column

	// SetCol sets the member column (in the case of mutable columns).
	SetCol(Column)

	// TemplateType returns an []interface{} and is used for operator templates.
	// Do not call this from normal code - it'll always panic.
	TemplateType() []interface{}

	// Append uses SliceArgs to append elements of a source Vec into this Vec.
	// It is logically equivalent to:
	// destVec = append(destVec[:args.DestIdx], args.Src[args.SrcStartIdx:args.SrcEndIdx])
	// An optional Sel slice can also be provided to apply a filter on the source
	// Vec.
	// Refer to the SliceArgs comment for specifics and TestAppend for examples.
	//
	// Note: Append()'ing from a Vector into itself is only supported when
	// args.SrcStartIdx == args.SrcEndIdx.
	Append(SliceArgs)

	// Copy uses SliceArgs to copy elements of a source Vec into this Vec. It is
	// logically equivalent to:
	// copy(destVec[args.DestIdx:], args.Src[args.SrcStartIdx:args.SrcEndIdx])
	// An optional Sel slice can also be provided to apply a filter on the source
	// Vec.
	// Refer to the SliceArgs comment for specifics and TestCopy for examples.
	Copy(SliceArgs)

	// CopyWithReorderedSource copies a value at position order[sel[i]] in src
	// into the receiver at position sel[i]. len(sel) elements are copied.
	// Resulting values of elements not mentioned in sel are undefined after
	// this function.
	CopyWithReorderedSource(src Vec, sel, order []int)

	// Window returns a "window" into the Vec. A "window" is similar to Golang's
	// slice of the current Vec from [start, end), but the returned object is NOT
	// allowed to be modified (the modification might result in an undefined
	// behavior).
	Window(start int, end int) Vec

	// MaybeHasNulls returns true if the column possibly has any null values, and
	// returns false if the column definitely has no null values.
	MaybeHasNulls() bool

	// Nulls returns the nulls vector for the column.
	Nulls() *Nulls

	// SetNulls sets the nulls vector for this column.
	SetNulls(Nulls)

	// Length returns the length of the slice that is underlying this Vec.
	Length() int

	// Capacity returns the capacity of the Golang's slice that is underlying
	// this Vec. Note that if there is no "slice" (like in case of flat bytes),
	// then "capacity" of such object is equal to the number of elements.
	Capacity() int
}

var _ Vec = &memColumn{}

// memColumn is a simple pass-through implementation of Vec that just casts
// a generic interface{} to the proper type when requested.
type memColumn struct {
	t                   *types.T
	canonicalTypeFamily types.Family
	col                 Column
	nulls               Nulls
}

// ColumnFactory is an interface that can construct columns for Batches.
type ColumnFactory interface {
	MakeColumn(t *types.T, length int) Column
}

type defaultColumnFactory struct{}

// StandardColumnFactory is a factory that produces columns of types that are
// explicitly supported by the vectorized engine (i.e. not datum-backed).
var StandardColumnFactory ColumnFactory = &defaultColumnFactory{}

func (cf *defaultColumnFactory) MakeColumn(t *types.T, length int) Column {
	switch canonicalTypeFamily := typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()); canonicalTypeFamily {
	case types.BoolFamily:
		return make(Bools, length)
	case types.BytesFamily:
		if t.Family() == types.UuidFamily {
			return NewBytesWithAvgLength(length, uuid.Size)
		}
		return NewBytes(length)
	case types.IntFamily:
		switch t.Width() {
		case 16:
			return make(Int16s, length)
		case 32:
			return make(Int32s, length)
		case 0, 64:
			return make(Int64s, length)
		default:
			panic(fmt.Sprintf("unexpected integer width: %d", t.Width()))
		}
	case types.FloatFamily:
		return make(Float64s, length)
	case types.DecimalFamily:
		return make(Decimals, length)
	case types.TimestampTZFamily:
		return make(Times, length)
	case types.IntervalFamily:
		return make(Durations, length)
	case types.JsonFamily:
		return NewJSONs(length)
	default:
		panic(fmt.Sprintf("StandardColumnFactory doesn't support %s", t))
	}
}

// NewMemColumn returns a new memColumn, initialized with a length using the
// given column factory.
func NewMemColumn(t *types.T, length int, factory ColumnFactory) Vec {
	var m memColumn
	m.init(t, length, factory)
	return &m
}

// init initializes the receiver with a length using the given column factory.
func (m *memColumn) init(t *types.T, length int, factory ColumnFactory) {
	*m = memColumn{
		t:                   t,
		canonicalTypeFamily: typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()),
		col:                 factory.MakeColumn(t, length),
		nulls:               NewNulls(length),
	}
}

func (m *memColumn) Type() *types.T {
	return m.t
}

func (m *memColumn) CanonicalTypeFamily() types.Family {
	return m.canonicalTypeFamily
}

func (m *memColumn) IsBytesLike() bool {
	switch m.canonicalTypeFamily {
	case types.BytesFamily, types.JsonFamily:
		return true
	}
	return false
}

func (m *memColumn) SetCol(col Column) {
	m.col = col
}

func (m *memColumn) Bool() Bools {
	return m.col.(Bools)
}

func (m *memColumn) Int16() Int16s {
	return m.col.(Int16s)
}

func (m *memColumn) Int32() Int32s {
	return m.col.(Int32s)
}

func (m *memColumn) Int64() Int64s {
	return m.col.(Int64s)
}

func (m *memColumn) Float64() Float64s {
	return m.col.(Float64s)
}

func (m *memColumn) Bytes() *Bytes {
	return m.col.(*Bytes)
}

func (m *memColumn) Decimal() Decimals {
	return m.col.(Decimals)
}

func (m *memColumn) Timestamp() Times {
	return m.col.(Times)
}

func (m *memColumn) Interval() Durations {
	return m.col.(Durations)
}

func (m *memColumn) JSON() *JSONs {
	return m.col.(*JSONs)
}

func (m *memColumn) Datum() DatumVec {
	return m.col.(DatumVec)
}

func (m *memColumn) Col() Column {
	return m.col
}

func (m *memColumn) TemplateType() []interface{} {
	panic("don't call this from non template code")
}

func (m *memColumn) MaybeHasNulls() bool {
	return m.nulls.maybeHasNulls
}

func (m *memColumn) Nulls() *Nulls {
	return &m.nulls
}

func (m *memColumn) SetNulls(n Nulls) {
	m.nulls = n
}

func (m *memColumn) Length() int {
	return m.col.Len()
}

func (m *memColumn) Capacity() int {
	switch m.CanonicalTypeFamily() {
	case types.BoolFamily:
		return cap(m.col.(Bools))
	case types.BytesFamily:
		return m.Bytes().Len()
	case types.IntFamily:
		switch m.t.Width() {
		case 16:
			return cap(m.col.(Int16s))
		case 32:
			return cap(m.col.(Int32s))
		case 0, 64:
			return cap(m.col.(Int64s))
		default:
			panic(fmt.Sprintf("unexpected int width: %d", m.t.Width()))
		}
	case types.FloatFamily:
		return cap(m.col.(Float64s))
	case types.DecimalFamily:
		return cap(m.col.(Decimals))
	case types.TimestampTZFamily:
		return cap(m.col.(Times))
	case types.IntervalFamily:
		return cap(m.col.(Durations))
	case types.JsonFamily:
		return m.JSON().Len()
	case typeconv.DatumVecCanonicalTypeFamily:
		return m.col.(DatumVec).Cap()
	default:
		panic(fmt.Sprintf("unhandled type %s", m.t))
	}
}
