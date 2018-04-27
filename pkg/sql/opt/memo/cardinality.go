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

package memo

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

// AsLowAs ratchets the min bound downwards in order to ensure that it allows
// values that are >= the min value.
func (c Cardinality) AsLowAs(min uint32) Cardinality {
	if min < c.Min {
		return Cardinality{Min: min, Max: c.Max}
	}
	return c
}

// AtLeast ratchets the bounds upwards so that they're at least as big as the
// given min value.
func (c Cardinality) AtLeast(min uint32) Cardinality {
	if c.Min > min {
		min = c.Min
	}
	max := min
	if c.Max > max {
		max = c.Max
	}
	return Cardinality{Min: min, Max: max}
}

// AtMost ratchets the bounds downwards so that they're no bigger than the given
// max value.
func (c Cardinality) AtMost(max uint32) Cardinality {
	min := max
	if c.Min < min {
		min = c.Min
	}
	if c.Max < max {
		max = c.Max
	}
	return Cardinality{Min: min, Max: max}
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
