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
	"math"
)

// Cost is the best-effort approximation of the actual cost of executing a
// particular operator tree.
// TODO: Need more details about what one "unit" of cost means.
type Cost float64

// MaxCost is the maximum possible estimated cost. It's used to suppress memo
// group members during testing, by setting their cost so high that any other
// member will have a lower cost.
var MaxCost = Cost(math.Inf(+1))

// Less returns true if this cost is lower than the given cost.
func (c Cost) Less(other Cost) bool {
	// Two plans with the same cost can have slightly different floating point
	// results (e.g. same subcosts being added up in a different order). So we
	// treat plans with very similar cost as equal.
	//
	// We use "units of least precision" for similarity: this is the number of
	// representable floating point numbers in-between the two values. This is
	// better than a fixed epsilon because the allowed error is proportional to
	// the magnitude of the numbers. Because the mantissa is in the low bits, we
	// can just use the bit representations as integers.
	const ulpTolerance = 1000
	return math.Float64bits(float64(c))+ulpTolerance <= math.Float64bits(float64(other))
}

// Sub subtracts the other cost from this cost and returns the result.
func (c Cost) Sub(other Cost) Cost {
	return c - other
}
