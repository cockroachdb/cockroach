// Copyright 2016 The Cockroach Authors.
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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package sqlbase

import "github.com/cockroachdb/cockroach/pkg/util/encoding"

// ColumnOrderInfo describes a column (as an index) and a desired order
// direction.
type ColumnOrderInfo struct {
	ColIdx    int
	Direction encoding.Direction
}

// ColumnOrdering is used to describe a desired column ordering. For example,
//     []ColumnOrderInfo{ {3, true}, {1, false} }
// represents an ordering first by column 3 (descending), then by column 1 (ascending).
type ColumnOrdering []ColumnOrderInfo

// IsPrefixOf returns true if the receiver ordering matches a prefix of the
// given ordering. In this case, rows with an order conforming to b
// automatically conform to a.
func (a ColumnOrdering) IsPrefixOf(b ColumnOrdering) bool {
	if len(a) > len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
