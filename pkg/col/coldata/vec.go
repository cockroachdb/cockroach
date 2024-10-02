// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package coldata exposes utilities for handling columnarized data.
package coldata

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// Column is an interface that represents a raw array of a Go native type.
type Column interface {
	// Len returns the number of elements in the Column.
	Len() int
}

// SliceArgs represents the arguments passed in to Vec.Append and Nulls.set.
type SliceArgs struct {
	// Src is the data being appended.
	Src *Vec
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

// Vec is a column vector that's accessible by Go native types.
// TODO(yuzefovich): consider storing / passing vectors by value rather than
// pointer.
type Vec struct {
	t                   *types.T
	canonicalTypeFamily types.Family
	col                 Column
	nulls               Nulls
}

// ColumnFactory is an interface that can construct columns for Batches.
type ColumnFactory interface {
	MakeColumn(t *types.T, length int) Column
	// MakeColumns batch-allocates columns of the given type and the given
	// length. Note that datum-backed vectors will be incomplete - the caller
	// must set the correct type on each one.
	MakeColumns(columns []Column, t *types.T, length int)
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

func (cf *defaultColumnFactory) MakeColumns(columns []Column, t *types.T, length int) {
	allocLength := len(columns) * length
	switch canonicalTypeFamily := typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()); canonicalTypeFamily {
	case types.BoolFamily:
		alloc := make(Bools, allocLength)
		for i := range columns {
			columns[i] = alloc[:length:length]
			alloc = alloc[length:]
		}
	case types.BytesFamily:
		alloc := make([]element, allocLength)
		wrapperAlloc := make([]Bytes, len(columns))
		for i := range columns {
			wrapperAlloc[i].elements = alloc[:length:length]
			columns[i] = &wrapperAlloc[i]
			alloc = alloc[length:]
		}
	case types.IntFamily:
		switch t.Width() {
		case 16:
			alloc := make(Int16s, allocLength)
			for i := range columns {
				columns[i] = alloc[:length:length]
				alloc = alloc[length:]
			}
		case 32:
			alloc := make(Int32s, allocLength)
			for i := range columns {
				columns[i] = alloc[:length:length]
				alloc = alloc[length:]
			}
		case 0, 64:
			alloc := make(Int64s, allocLength)
			for i := range columns {
				columns[i] = alloc[:length:length]
				alloc = alloc[length:]
			}
		default:
			panic(fmt.Sprintf("unexpected integer width: %d", t.Width()))
		}
	case types.FloatFamily:
		alloc := make(Float64s, allocLength)
		for i := range columns {
			columns[i] = alloc[:length:length]
			alloc = alloc[length:]
		}
	case types.DecimalFamily:
		alloc := make(Decimals, allocLength)
		for i := range columns {
			columns[i] = alloc[:length:length]
			alloc = alloc[length:]
		}
	case types.TimestampTZFamily:
		alloc := make(Times, allocLength)
		for i := range columns {
			columns[i] = alloc[:length:length]
			alloc = alloc[length:]
		}
	case types.IntervalFamily:
		alloc := make(Durations, allocLength)
		for i := range columns {
			columns[i] = alloc[:length:length]
			alloc = alloc[length:]
		}
	case types.JsonFamily:
		alloc := make([]element, allocLength)
		wrapperAlloc := make([]JSONs, len(columns))
		for i := range columns {
			wrapperAlloc[i].elements = alloc[:length:length]
			columns[i] = &wrapperAlloc[i]
			alloc = alloc[length:]
		}
	default:
		panic(fmt.Sprintf("StandardColumnFactory doesn't support %s", t))
	}
}

// NewVec returns a new Vec, initialized with a length using the given column
// factory.
func NewVec(t *types.T, length int, factory ColumnFactory) *Vec {
	return &Vec{
		t:                   t,
		canonicalTypeFamily: typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()),
		col:                 factory.MakeColumn(t, length),
		nulls:               NewNulls(length),
	}
}

// Type returns the type of data stored in this Vec. Consider whether
// CanonicalTypeFamily() should be used instead.
func (v *Vec) Type() *types.T {
	return v.t
}

// CanonicalTypeFamily returns the canonical type family of data stored in this
// Vec.
func (v *Vec) CanonicalTypeFamily() types.Family {
	return v.canonicalTypeFamily
}

// SetCol sets the member column (in the case of mutable columns).
func (v *Vec) SetCol(col Column) {
	v.col = col
}

// Bool returns a bool list.
func (v *Vec) Bool() Bools {
	return v.col.(Bools)
}

// Int16 returns an int16 slice.
func (v *Vec) Int16() Int16s {
	return v.col.(Int16s)
}

// Int32 returns an int32 slice.
func (v *Vec) Int32() Int32s {
	return v.col.(Int32s)
}

// Int64 returns an int64 slice.
func (v *Vec) Int64() Int64s {
	return v.col.(Int64s)
}

// Float64 returns a float64 slice.
func (v *Vec) Float64() Float64s {
	return v.col.(Float64s)
}

// Bytes returns a flat Bytes representation.
func (v *Vec) Bytes() *Bytes {
	return v.col.(*Bytes)
}

// Decimal returns an apd.Decimal slice.
func (v *Vec) Decimal() Decimals {
	return v.col.(Decimals)
}

// Timestamp returns a time.Time slice.
func (v *Vec) Timestamp() Times {
	return v.col.(Times)
}

// Interval returns a duration.Duration slice.
func (v *Vec) Interval() Durations {
	return v.col.(Durations)
}

// JSON returns a vector of JSONs.
func (v *Vec) JSON() *JSONs {
	return v.col.(*JSONs)
}

// Datum returns a vector of Datums.
func (v *Vec) Datum() DatumVec {
	return v.col.(DatumVec)
}

// Col returns the raw, typeless backing storage for this Vec.
func (v *Vec) Col() Column {
	return v.col
}

// TemplateType returns an []interface{} and is used for operator templates.
// Do not call this from normal code - it'll always panic.
func (v *Vec) TemplateType() []interface{} {
	panic("don't call this from non template code")
}

// MaybeHasNulls returns true if the column possibly has any null values, and
// returns false if the column definitely has no null values.
func (v *Vec) MaybeHasNulls() bool {
	return v.nulls.maybeHasNulls
}

// Nulls returns the nulls vector for the column.
func (v *Vec) Nulls() *Nulls {
	return &v.nulls
}

// SetNulls sets the nulls vector for this column.
func (v *Vec) SetNulls(n Nulls) {
	v.nulls = n
}

// Length returns the length of the slice that is underlying this Vec.
func (v *Vec) Length() int {
	return v.col.Len()
}

// Capacity returns the capacity of the Golang's slice that is underlying
// this Vec. Note that if there is no "slice" (like in case of flat bytes),
// then "capacity" of such object is equal to the number of elements.
func (v *Vec) Capacity() int {
	switch v.CanonicalTypeFamily() {
	case types.BoolFamily:
		return cap(v.col.(Bools))
	case types.BytesFamily:
		return v.Bytes().Len()
	case types.IntFamily:
		switch v.t.Width() {
		case 16:
			return cap(v.col.(Int16s))
		case 32:
			return cap(v.col.(Int32s))
		case 0, 64:
			return cap(v.col.(Int64s))
		default:
			panic(fmt.Sprintf("unexpected int width: %d", v.t.Width()))
		}
	case types.FloatFamily:
		return cap(v.col.(Float64s))
	case types.DecimalFamily:
		return cap(v.col.(Decimals))
	case types.TimestampTZFamily:
		return cap(v.col.(Times))
	case types.IntervalFamily:
		return cap(v.col.(Durations))
	case types.JsonFamily:
		return v.JSON().Len()
	case typeconv.DatumVecCanonicalTypeFamily:
		return v.col.(DatumVec).Cap()
	default:
		panic(fmt.Sprintf("unhandled type %s", v.t))
	}
}
