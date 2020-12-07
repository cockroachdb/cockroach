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
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
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

// Decimals is a flat representation of apd.Decimal objects.
type Decimals struct {
	Bytes
}

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

func (c *Decimals) getDecimalAndCoeffbytes(idx int) (*apd.Decimal, []byte) {
	slice := c.Bytes.Get(idx)
	if len(slice) == 0 {
		// If there's a null in the slice, it'll have no data.
		return nil, nil
	}
	return encoding.UnsafeCastDecimal(slice), slice[decimalSize:]
}

// Get returns the element at index idx of the vector. The element cannot be
// used anymore once the vector is modified.
//gcassert:inline
func (c *Decimals) Get(idx int) apd.Decimal {
	slice := c.Bytes.Get(idx)
	if len(slice) == 0 {
		// If there's a null in the slice, it'll have no data.
		return apd.Decimal{}
	}
	ret := encoding.UnsafeCastDecimal(slice)
	return *ret
}

// Get returns the element at index idx of the vector. The element cannot be
// used anymore once the vector is modified.
//gcassert:inline
func (c Times) Get(idx int) time.Time { return c[idx] }

// Get returns the element at index idx of the vector. The element cannot be
// used anymore once the vector is modified.
//gcassert:inline
func (c Durations) Get(idx int) duration.Duration { return c[idx] }

// Len returns the length of the vector.
func (c Bools) Len() int { return len(c) }

// Len returns the length of the vector.
func (c Int16s) Len() int { return len(c) }

// Len returns the length of the vector.
func (c Int32s) Len() int { return len(c) }

// Len returns the length of the vector.
func (c Int64s) Len() int { return len(c) }

func (c Int64s) Set(idx int, agg int64) { c[idx] = agg }

// Len returns the length of the vector.
func (c Float64s) Len() int { return len(c) }

func (c Float64s) Set(idx int, agg float64) { c[idx] = agg }

// Len returns the length of the vector.
func (c Times) Len() int { return len(c) }

// Len returns the length of the vector.
func (c Durations) Len() int { return len(c) }

func (c Durations) Set(idx int, agg duration.Duration) { c[idx] = agg }
