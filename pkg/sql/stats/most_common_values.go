// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stats

import "github.com/cockroachdb/cockroach/pkg/sql/opt/cat"

// An histogramMCVHeap is used to keep the most common values in a histogram ordered
// in decreasing order of frequency.
type histogramMCVHeap histogram

func (h histogramMCVHeap) Len() int { return len(h.mcvs) }
func (h histogramMCVHeap) Less(i, j int) bool {
	return h.buckets[h.mcvs[i].BucketIdx].NumEq > h.buckets[h.mcvs[j].BucketIdx].NumEq
}
func (h histogramMCVHeap) Swap(i, j int) { h.mcvs[i], h.mcvs[j] = h.mcvs[j], h.mcvs[i] }

func (h histogramMCVHeap) Push(x cat.MostCommonValue) {
	h.mcvs = append(h.mcvs, x)
}

func (h histogramMCVHeap) Pop() cat.MostCommonValue {
	old := h.mcvs
	n := len(old)
	x := old[n-1]
	h.mcvs = old[0 : n-1]
	return x
}

// MCV contains an index into a slice of samples, and the count of values equal
// to the sample at idx.
type MCV struct {
	idx   int
	count int
}

// An MCVHeap is used to keep the most common values in a slice of samples
// ordered in decreasing order of frequency.
type MCVHeap []MCV

func (h MCVHeap) Len() int { return len(h) }
func (h MCVHeap) Less(i, j int) bool {
	return h[i].count > h[j].count
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
