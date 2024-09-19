// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

// MCV contains an index into a slice of samples, and the count of samples equal
// to the sample at idx.
type MCV struct {
	idx   int
	count int
}

// An MCVHeap is used to track the most common values in a slice of samples.
type MCVHeap []MCV

func (h MCVHeap) Len() int { return len(h) }
func (h MCVHeap) Less(i, j int) bool {
	return h[i].count < h[j].count
}
func (h MCVHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *MCVHeap) Push(x MCV) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x)
}

func (h *MCVHeap) Pop() MCV {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
