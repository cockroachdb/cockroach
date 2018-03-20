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
// particular expression tree.
// TODO: Need more details about what one "unit" of cost means.
type Cost float64

// MaxCost is the maximum possible estimated cost. It's used as the
// starting value of an uninitialized best expression, so that any other
// expression will have a lower cost.
var MaxCost = Cost(math.Inf(+1))

// Less returns true if this cost is lower than the given cost.
func (c Cost) Less(other Cost) bool {
	return c < other
}

// Sub subtracts the other cost from this cost and returns the result.
func (c Cost) Sub(other Cost) Cost {
	return c - other
}
