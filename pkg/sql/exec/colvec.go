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

// ColVec is an interface that represents a column vector that's accessible by
// Go native types.
type ColVec interface {
	Nulls

	// TODO(jordan): is a bitmap or slice of bools better?
	// Bool returns a bool list.
	Bool() Bools
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
	// Bytes returns a Bytes object, allowing retrieval of multiple byte slices.
	Bytes() Bytes
}

// Nulls represents a list of potentially nullable values.
type Nulls interface {
	// HasNulls returns true if the column has any null values.
	HasNulls() bool

	// At returns true if the ith value of the column is null.
	NullAt(i uint16) bool

	// Rank returns the index of the ith non-null value in the column.
	Rank(i int) int
}

// Bools is an interface that represents a list of bools.
type Bools interface {
	// At returns the ith bool in the list.
	At(i uint16) bool
}

// Bytes is an interface that represents a list of byte slices.
type Bytes interface {
	// At returns the ith byte slice in the list.
	At(i uint16) []byte
}

var _ ColVec = memColumn{}

// memColumn is a simple pass-through implementation of ColVec that just casts
// a generic interface{} to the proper type when requested.
type memColumn struct {
	col interface{}
}

func (m memColumn) HasNulls() bool {
	return false
}

func (m memColumn) NullAt(i uint16) bool {
	return false
}

func (m memColumn) Rank(i int) int {
	return i
}

func (m memColumn) Bool() Bools {
	return m.col.(memBools)
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

func (m memColumn) Bytes() Bytes {
	return m.col.(memBytes)
}

var _ Bools = memBools{}

type memBools struct {
	col []bool
}

func (m memBools) At(i uint16) bool {
	return m.col[i]
}

var _ Bytes = memBytes{}

type memBytes struct {
	col [][]byte
}

func (m memBytes) At(i uint16) []byte {
	return m.col[i]
}
