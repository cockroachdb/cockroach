// Copyright 2020 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
)

// Bools is a slice of bool.
type Bools []bool

// Int16s is a slice of int16.
type Int16s []int16

// Int32s is a slice of int32.
type Int32s []int32

// Int64s is a slice of int64.
type Int64s []int64

// Float64s is a slice of float64.
type Float64s []float64

// Decimals is a slice of apd.Decimal.
type Decimals []apd.Decimal

// Times is a slice of time.Time.
type Times []time.Time

// Durations is a slice of duration.Duration.
type Durations []duration.Duration

// Get returns the element at index idx of the vector. The element cannot be
// used anymore once the vector is modified.
//gcassert:inline
func (c Bools) Get(idx int) bool { return c[idx] }

// Get returns the element at index idx of the vector. The element cannot be
// used anymore once the vector is modified.
//gcassert:inline
func (c Int16s) Get(idx int) int16 { return c[idx] }

// Get returns the element at index idx of the vector. The element cannot be
// used anymore once the vector is modified.
//gcassert:inline
func (c Int32s) Get(idx int) int32 { return c[idx] }

// Get returns the element at index idx of the vector. The element cannot be
// used anymore once the vector is modified.
//gcassert:inline
func (c Int64s) Get(idx int) int64 { return c[idx] }

// Get returns the element at index idx of the vector. The element cannot be
// used anymore once the vector is modified.
//gcassert:inline
func (c Float64s) Get(idx int) float64 { return c[idx] }

// Get returns the element at index idx of the vector. The element cannot be
// used anymore once the vector is modified.
//gcassert:inline
func (c Decimals) Get(idx int) apd.Decimal { return c[idx] }

// Get returns the element at index idx of the vector. The element cannot be
// used anymore once the vector is modified.
//gcassert:inline
func (c Times) Get(idx int) time.Time { return c[idx] }

// Get returns the element at index idx of the vector. The element cannot be
// used anymore once the vector is modified.
//gcassert:inline
func (c Durations) Get(idx int) duration.Duration { return c[idx] }

// Set sets the element at index idx of the vector to val.
//gcassert:inline
func (c Bools) Set(idx int, val bool) { c[idx] = val }

// Set sets the element at index idx of the vector to val.
//gcassert:inline
func (c Int16s) Set(idx int, val int16) { c[idx] = val }

// Set sets the element at index idx of the vector to val.
//gcassert:inline
func (c Int32s) Set(idx int, val int32) { c[idx] = val }

// Set sets the element at index idx of the vector to val.
//gcassert:inline
func (c Int64s) Set(idx int, val int64) { c[idx] = val }

// Set sets the element at index idx of the vector to val.
//gcassert:inline
func (c Float64s) Set(idx int, val float64) { c[idx] = val }

// Set sets the element at index idx of the vector to val.
//
// Note that this method is usually inlined, but it isn't in case of the merge
// joiner generated code (probably because of the size of the functions), so we
// don't assert the inlining with the GCAssert linter.
// TODO(yuzefovich): consider whether Get and Set on Decimals should operate on
// pointers to apd.Decimal.
func (c Decimals) Set(idx int, val apd.Decimal) { c[idx].Set(&val) }

// Set sets the element at index idx of the vector to val.
//gcassert:inline
func (c Times) Set(idx int, val time.Time) { c[idx] = val }

// Set sets the element at index idx of the vector to val.
//gcassert:inline
func (c Durations) Set(idx int, val duration.Duration) { c[idx] = val }

// Len returns the length of the vector.
//gcassert:inline
func (c Bools) Len() int { return len(c) }

// Len returns the length of the vector.
//gcassert:inline
func (c Int16s) Len() int { return len(c) }

// Len returns the length of the vector.
//gcassert:inline
func (c Int32s) Len() int { return len(c) }

// Len returns the length of the vector.
//gcassert:inline
func (c Int64s) Len() int { return len(c) }

// Len returns the length of the vector.
//gcassert:inline
func (c Float64s) Len() int { return len(c) }

// Len returns the length of the vector.
//gcassert:inline
func (c Decimals) Len() int { return len(c) }

// Len returns the length of the vector.
//gcassert:inline
func (c Times) Len() int { return len(c) }

// Len returns the length of the vector.
//gcassert:inline
func (c Durations) Len() int { return len(c) }

// CopySlice copies src[srcStartIdx:srcEndIdx] into c starting at position
// destIdx.
//gcassert:inline
func (c Bools) CopySlice(src Bools, destIdx, srcStartIdx, srcEndIdx int) {
	copy(c[destIdx:], src[srcStartIdx:srcEndIdx])
}

// CopySlice copies src[srcStartIdx:srcEndIdx] into c starting at position
// destIdx.
//gcassert:inline
func (c Int16s) CopySlice(src Int16s, destIdx, srcStartIdx, srcEndIdx int) {
	copy(c[destIdx:], src[srcStartIdx:srcEndIdx])
}

// CopySlice copies src[srcStartIdx:srcEndIdx] into c starting at position
// destIdx.
//gcassert:inline
func (c Int32s) CopySlice(src Int32s, destIdx, srcStartIdx, srcEndIdx int) {
	copy(c[destIdx:], src[srcStartIdx:srcEndIdx])
}

// CopySlice copies src[srcStartIdx:srcEndIdx] into c starting at position
// destIdx.
//gcassert:inline
func (c Int64s) CopySlice(src Int64s, destIdx, srcStartIdx, srcEndIdx int) {
	copy(c[destIdx:], src[srcStartIdx:srcEndIdx])
}

// CopySlice copies src[srcStartIdx:srcEndIdx] into c starting at position
// destIdx.
//gcassert:inline
func (c Float64s) CopySlice(src Float64s, destIdx, srcStartIdx, srcEndIdx int) {
	copy(c[destIdx:], src[srcStartIdx:srcEndIdx])
}

// CopySlice copies src[srcStartIdx:srcEndIdx] into c starting at position
// destIdx.
//
// Note that this method is usually inlined, but it isn't in case of the
// memColumn.Copy generated code (probably because of the size of that
// function), so we don't assert the inlining with the GCAssert linter.
func (c Decimals) CopySlice(src Decimals, destIdx, srcStartIdx, srcEndIdx int) {
	srcSlice := src[srcStartIdx:srcEndIdx]
	for i := range srcSlice {
		c[destIdx+i].Set(&srcSlice[i])
	}
}

// CopySlice copies src[srcStartIdx:srcEndIdx] into c starting at position
// destIdx.
//gcassert:inline
func (c Times) CopySlice(src Times, destIdx, srcStartIdx, srcEndIdx int) {
	copy(c[destIdx:], src[srcStartIdx:srcEndIdx])
}

// CopySlice copies src[srcStartIdx:srcEndIdx] into c starting at position
// destIdx.
//gcassert:inline
func (c Durations) CopySlice(src Durations, destIdx, srcStartIdx, srcEndIdx int) {
	copy(c[destIdx:], src[srcStartIdx:srcEndIdx])
}

// Window returns the window into the vector.
//gcassert:inline
func (c Bools) Window(start, end int) Bools { return c[start:end] }

// Window returns the window into the vector.
//gcassert:inline
func (c Int16s) Window(start, end int) Int16s { return c[start:end] }

// Window returns the window into the vector.
//gcassert:inline
func (c Int32s) Window(start, end int) Int32s { return c[start:end] }

// Window returns the window into the vector.
//gcassert:inline
func (c Int64s) Window(start, end int) Int64s { return c[start:end] }

// Window returns the window into the vector.
//gcassert:inline
func (c Float64s) Window(start, end int) Float64s { return c[start:end] }

// Window returns the window into the vector.
//gcassert:inline
func (c Decimals) Window(start, end int) Decimals { return c[start:end] }

// Window returns the window into the vector.
//gcassert:inline
func (c Times) Window(start, end int) Times { return c[start:end] }

// Window returns the window into the vector.
//gcassert:inline
func (c Durations) Window(start, end int) Durations { return c[start:end] }
