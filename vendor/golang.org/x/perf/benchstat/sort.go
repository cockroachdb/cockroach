// Copyright 2018 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package benchstat

import (
	"math"
	"sort"
)

// An Order defines a sort order for a table.
// It reports whether t.Rows[i] should appear before t.Rows[j].
type Order func(t *Table, i, j int) bool

// ByName sorts tables by the Benchmark name column
func ByName(t *Table, i, j int) bool {
	return t.Rows[i].Benchmark < t.Rows[j].Benchmark
}

// ByDelta sorts tables by the Delta column,
// reversing the order when larger is better (for "speed" results).
func ByDelta(t *Table, i, j int) bool {
	return math.Abs(t.Rows[i].PctDelta)*float64(t.Rows[i].Change) <
		math.Abs(t.Rows[j].PctDelta)*float64(t.Rows[j].Change)
}

// Reverse returns the reverse of the given order.
func Reverse(order Order) Order {
	return func(t *Table, i, j int) bool { return order(t, j, i) }
}

// Sort sorts a Table t (in place) by the given order.
func Sort(t *Table, order Order) {
	sort.SliceStable(t.Rows, func(i, j int) bool { return order(t, i, j) })
}
