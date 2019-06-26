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

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// column is an interface that represents a raw array of a Go native type.
type column interface{}

// AppendArgs represents the arguments passed in to Vec.Append.
type AppendArgs struct {
	// ColType is the type of both the destination and source slices.
	ColType types.T
	// Src is the data being appended.
	Src Vec
	// Sel is an optional slice specifying indices to append to the destination
	// slice. Note that Src{Start,End}Idx apply to Sel.
	Sel []uint16
	// DestIdx is the first index that Append will append to.
	DestIdx uint64
	// SrcStartIdx is the index of the first element in Src that Append will
	// append.
	SrcStartIdx uint16
	// SrcEndIdx is the exclusive end index of Src. i.e. the element in the index
	// before SrcEndIdx is the last element appended to the destination slice,
	// similar to Src[SrcStartIdx:SrcEndIdx].
	SrcEndIdx uint16
}

// CopyArgs represents the arguments passed in to Vec.Copy.
type CopyArgs struct {
	// ColType is the type of both the destination and source slices.
	ColType types.T
	// Src is the data being copied.
	Src Vec
	// Sel is an optional slice specifying indices to copy to the destination
	// slice. Note that Src{Start,End}Idx apply to Sel.
	Sel []uint16
	// Sel64 overrides Sel. Used when the amount of data being copied exceeds the
	// representation capabilities of a []uint16.
	Sel64 []uint64
	// DestIdx is the first index that Copy will copy to.
	DestIdx uint64
	// SrcStartIdx is the index of the first element in Src that Copy will copy.
	SrcStartIdx uint64
	// SrcEndIdx is the exclusive end index of Src. i.e. the element in the index
	// before SrcEndIdx is the last element copied into the destination slice,
	// similar to Src[SrcStartIdx:SrcEndIdx].
	SrcEndIdx uint64

	// Nils exists to support the hashJoiner's use case of Copy before the
	// migration to a single Copy method. It overrides the use of a selection
	// vector only in the Sel64 case and simply sets the destination slice's value
	// to NULL if the index is true, otherwise defaulting to using the selection
	// vector.
	// TODO(asubiotto): Get rid of this.
	// DEPRECATED: DO NOT USE, it should not be Copy's responsibility to care
	// about this.
	Nils []bool
}

// Vec is an interface that represents a column vector that's accessible by
// Go native types.
type Vec interface {
	// Type returns the type of data stored in this Vec.
	Type() types.T

	// TODO(jordan): is a bitmap or slice of bools better?
	// Bool returns a bool list.
	Bool() []bool
	// Int8 returns an int8 slice.
	Int8() []int8
	// Int16 returns an int16 slice.
	Int16() []int16
	// Int32 returns an int32 slice.
	Int32() []int32
	// Int64 returns an int64 slice.
	Int64() []int64
	// Float32 returns a float32 slice.
	Float32() []float32
	// Float64 returns a float64 slice.
	Float64() []float64
	// Bytes returns a []byte slice.
	Bytes() [][]byte
	// TODO(jordan): should this be [][]byte?
	// Decimal returns an apd.Decimal slice.
	Decimal() []apd.Decimal

	// Col returns the raw, typeless backing storage for this Vec.
	Col() interface{}

	// SetCol sets the member column (in the case of mutable columns).
	SetCol(interface{})

	// TemplateType returns an []interface{} and is used for operator templates.
	// Do not call this from normal code - it'll always panic.
	_TemplateType() []interface{}

	// Append uses AppendArgs to append elements of a source Vec into this Vec.
	// It is logically equivalent to:
	// destVec = append(destVec[:args.DestIdx], args.Src[args.SrcStartIdx:args.SrcEndIdx])
	// An optional Sel slice can also be provided to apply a filter on the source
	// Vec.
	// Refer to the AppendArgs comment for specifics and TestAppend for examples.
	Append(AppendArgs)

	// Copy uses CopyArgs to copy elements of a source Vec into this Vec. It is
	// logically equivalent to:
	// copy(destVec[args.DestIdx:], args.Src[args.SrcStartIdx:args.SrcEndIdx])
	// An optional Sel slice can also be provided to apply a filter on the source
	// Vec.
	// Refer to the CopyArgs comment for specifics and TestCopy for examples.
	Copy(CopyArgs)

	// Slice returns a new Vec representing a slice of the current Vec from
	// [start, end).
	Slice(colType types.T, start uint64, end uint64) Vec

	// PrettyValueAt returns a "pretty"value for the idx'th value in this Vec.
	// It uses the reflect package and is not suitable for calling in hot paths.
	PrettyValueAt(idx uint16, colType types.T) string

	// HasNulls returns true if the column has any null values.
	HasNulls() bool

	// Nulls returns the nulls vector for the column.
	Nulls() *Nulls

	// SetNulls sets the nulls vector for this column.
	SetNulls(*Nulls)
}

var _ Vec = &memColumn{}

// memColumn is a simple pass-through implementation of Vec that just casts
// a generic interface{} to the proper type when requested.
type memColumn struct {
	t     types.T
	col   column
	nulls Nulls
}

// NewMemColumn returns a new memColumn, initialized with a length.
func NewMemColumn(t types.T, n int) Vec {
	nulls := NewNulls(n)

	switch t {
	case types.Bool:
		return &memColumn{t: t, col: make([]bool, n), nulls: nulls}
	case types.Bytes:
		return &memColumn{t: t, col: make([][]byte, n), nulls: nulls}
	case types.Int8:
		return &memColumn{t: t, col: make([]int8, n), nulls: nulls}
	case types.Int16:
		return &memColumn{t: t, col: make([]int16, n), nulls: nulls}
	case types.Int32:
		return &memColumn{t: t, col: make([]int32, n), nulls: nulls}
	case types.Int64:
		return &memColumn{t: t, col: make([]int64, n), nulls: nulls}
	case types.Float32:
		return &memColumn{t: t, col: make([]float32, n), nulls: nulls}
	case types.Float64:
		return &memColumn{t: t, col: make([]float64, n), nulls: nulls}
	case types.Decimal:
		return &memColumn{t: t, col: make([]apd.Decimal, n), nulls: nulls}
	default:
		panic(fmt.Sprintf("unhandled type %s", t))
	}
}

func (m *memColumn) Type() types.T {
	return m.t
}

func (m *memColumn) SetCol(col interface{}) {
	m.col = col
}

func (m *memColumn) Bool() []bool {
	return m.col.([]bool)
}

func (m *memColumn) Int8() []int8 {
	return m.col.([]int8)
}

func (m *memColumn) Int16() []int16 {
	return m.col.([]int16)
}

func (m *memColumn) Int32() []int32 {
	return m.col.([]int32)
}

func (m *memColumn) Int64() []int64 {
	return m.col.([]int64)
}

func (m *memColumn) Float32() []float32 {
	return m.col.([]float32)
}

func (m *memColumn) Float64() []float64 {
	return m.col.([]float64)
}

func (m *memColumn) Bytes() [][]byte {
	return m.col.([][]byte)
}

func (m *memColumn) Decimal() []apd.Decimal {
	return m.col.([]apd.Decimal)
}

func (m *memColumn) Col() interface{} {
	return m.col
}

func (m *memColumn) _TemplateType() []interface{} {
	panic("don't call this from non template code")
}

func (m *memColumn) HasNulls() bool {
	return m.nulls.hasNulls
}

func (m *memColumn) Nulls() *Nulls {
	return &m.nulls
}

func (m *memColumn) SetNulls(n *Nulls) {
	m.nulls = *n
}
