// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package props

import (
	"fmt"
	"math"
)

// ZeroCardinality indicates that no rows will be returned by expression.
var ZeroCardinality = Cardinality{}

// OneCardinality indicates that exactly one row will be returned by expression.
var OneCardinality = Cardinality{Min: 1, Max: 1}

// AnyCardinality indicates that any number of rows can be returned by an
// expression.
var AnyCardinality = Cardinality{Min: 0, Max: math.MaxUint32}

// Cardinality is the number of rows that can be returned by a relational
// expression. Both Min and Max are inclusive bounds. If Max = math.MaxUint32,
// that indicates there is no limit to the number of returned rows. Max should
// always be >= Min, or method results are undefined. Cardinality is determined
// from the relational properties, and are "hard" bounds that are never
// incorrect. This constrasts with row cardinality derived by the statistics
// code, which are only estimates and may be incorrect.
type Cardinality struct {
	Min uint32
	Max uint32
}

// IsZero returns true if the expression never returns any rows.
func (c Cardinality) IsZero() bool {
	return c.Min == 0 && c.Max == 0
}

// IsOne returns true if the expression always returns one row.
func (c Cardinality) IsOne() bool {
	return c.Min == 1 && c.Max == 1
}

// IsZeroOrOne is true if the expression never returns more than one row.
func (c Cardinality) IsZeroOrOne() bool {
	return c.Max <= 1
}

// CanBeZero is true if the expression can return zero rows.
func (c Cardinality) CanBeZero() bool {
	return c.Min == 0
}

// IsUnbounded returns true if the expression has unbounded maximum cardinality.
func (c Cardinality) IsUnbounded() bool {
	return c.Max == AnyCardinality.Max
}

// AsLowAs ratchets the min bound downwards in order to ensure that it allows
// values that are >= the min value.
func (c Cardinality) AsLowAs(min uint32) Cardinality {
	return Cardinality{
		Min: minVal(c.Min, min),
		Max: c.Max,
	}
}

// Limit ratchets the bounds downwards so that they're no bigger than the given
// max value.
func (c Cardinality) Limit(max uint32) Cardinality {
	return Cardinality{
		Min: minVal(c.Min, max),
		Max: minVal(c.Max, max),
	}
}

// AtLeast ratchets the bounds upwards so that they're at least as large as the
// bounds in the given cardinality.
func (c Cardinality) AtLeast(other Cardinality) Cardinality {
	return Cardinality{
		Min: maxVal(c.Min, other.Min),
		Max: maxVal(c.Max, other.Max),
	}
}

// Add sums the min and max bounds to get a combined count of rows.
func (c Cardinality) Add(other Cardinality) Cardinality {
	// Make sure to detect overflow.
	min := c.Min + other.Min
	if min < c.Min {
		min = math.MaxUint32
	}
	max := c.Max + other.Max
	if max < c.Max {
		max = math.MaxUint32
	}
	return Cardinality{Min: min, Max: max}
}

// Product multiples the min and max bounds to get the combined product of rows.
func (c Cardinality) Product(other Cardinality) Cardinality {
	// Prevent overflow by using 64-bit multiplication.
	min := uint64(c.Min) * uint64(other.Min)
	if min > math.MaxUint32 {
		min = math.MaxUint32
	}
	max := uint64(c.Max) * uint64(other.Max)
	if max > math.MaxUint32 {
		max = math.MaxUint32
	}
	return Cardinality{Min: uint32(min), Max: uint32(max)}
}

// Skip subtracts the given number of rows from the min and max bounds to
// account for skipped rows.
func (c Cardinality) Skip(rows uint32) Cardinality {
	min := c.Min - rows
	if min > c.Min {
		min = 0
	}
	if c.Max == math.MaxUint32 {
		// No upper bound, so treat it as if it's infinity.
		return Cardinality{Min: min, Max: c.Max}
	}
	max := c.Max - rows
	if max > c.Max {
		max = 0
	}
	return Cardinality{Min: min, Max: max}
}

func (c Cardinality) String() string {
	if c.Max == math.MaxUint32 {
		return fmt.Sprintf("[%d - ]", c.Min)
	}
	return fmt.Sprintf("[%d - %d]", c.Min, c.Max)
}

func minVal(a, b uint32) uint32 {
	if a <= b {
		return a
	}
	return b
}

func maxVal(a, b uint32) uint32 {
	if a >= b {
		return a
	}
	return b
}
