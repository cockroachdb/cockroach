// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package heaputil provides helper functions for working with heaps.
package heaputil

import "github.com/cockroachdb/cockroach/pkg/util/container/heap"

// Valid returns whether a heap is a valid heap based on the min-heap
// invariant defined by heap.Interface.
func Valid[T any](h heap.Interface[T]) bool {
	n := h.Len()
	for i := 0; i < n; i++ {
		left, right := 2*i+1, 2*i+2
		if left < n && h.Less(left, i) {
			return false
		}
		if right < n && h.Less(right, i) {
			return false
		}
	}
	return true
}
