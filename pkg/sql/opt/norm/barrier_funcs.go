// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package norm

import "github.com/cockroachdb/cockroach/pkg/sql/opt/memo"

// BarriersEqual returns true if the two BarrierPrivate structs are the same.
func (c *CustomFuncs) BarriersEqual(left, right *memo.BarrierPrivate) bool {
	if left == nil || right == nil {
		return left == right // true if both nil, false otherwise
	}
	return *left == *right
}
