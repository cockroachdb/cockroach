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

package exec

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// ColVec is an interface that represents a column vector that's accessible by
// Go native types.
type ColVec interface {
	Nulls

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
	// Float64 returns an float64 slice.
	Float64() []float64
	// Bytes returns a []byte slice.
	Bytes() [][]byte

	// Col returns the raw, typeless backing storage for this ColVec.
	Col() interface{}
}

// Nulls represents a list of potentially nullable values.
type Nulls interface {
	// HasNulls returns true if the column has any null values.
	HasNulls() bool

	// At returns true if the ith value of the column is null.
	NullAt(i uint16) bool

	// SetNull sets the ith value of the column to null.
	SetNull(i uint16)

	// Rank returns the index of the ith non-null value in the column.
	Rank(i uint16) uint16
}

var _ ColVec = memColumn{}

// memColumn is a simple pass-through implementation of ColVec that just casts
// a generic interface{} to the proper type when requested.
type memColumn struct {
	col interface{}
}

// newMemColumn returns a new memColumn, initialized with a type and length.
func newMemColumn(t types.T, n int) memColumn {
	switch t {
	case types.Bool:
		return memColumn{col: make([]bool, n)}
	case types.Bytes:
		return memColumn{col: make([][]byte, n)}
	case types.Int16:
		return memColumn{col: make([]int16, n)}
	case types.Int32:
		return memColumn{col: make([]int32, n)}
	case types.Int64:
		return memColumn{col: make([]int64, n)}
	case types.Float32:
		return memColumn{col: make([]float32, n)}
	case types.Float64:
		return memColumn{col: make([]float64, n)}
	default:
		panic(fmt.Sprintf("unhandled type %d", t))
	}
}

func (m memColumn) HasNulls() bool {
	return false
}

func (m memColumn) NullAt(i uint16) bool {
	return false
}

func (m memColumn) SetNull(i uint16) {}

func (m memColumn) Rank(i uint16) uint16 {
	return i
}

func (m memColumn) Bool() []bool {
	return m.col.([]bool)
}

func (m memColumn) Int8() []int8 {
	return m.col.([]int8)
}

func (m memColumn) Int16() []int16 {
	return m.col.([]int16)
}

func (m memColumn) Int32() []int32 {
	return m.col.([]int32)
}

func (m memColumn) Int64() []int64 {
	return m.col.([]int64)
}

func (m memColumn) Float32() []float32 {
	return m.col.([]float32)
}

func (m memColumn) Float64() []float64 {
	return m.col.([]float64)
}

func (m memColumn) Bytes() [][]byte {
	return m.col.([][]byte)
}

func (m memColumn) Col() interface{} {
	return m.col
}
