// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package util

import (
	"container/heap"
	"math"
	"math/rand"

	"github.com/golang/glog"
)

// A WeightedValue is used to represent the items sampled by
// WeightedReservoirSample.
type WeightedValue struct {
	Value interface{}
	key   float64
}

// Less implements the Ordered interface.
func (wv WeightedValue) Less(zv WeightedValue) bool {
	return wv.key < zv.key
}

// A WeightedValueHeap implements a heap structure on a slice of weighted
// values for use in in-memory weighted reservoir sampling.
type WeightedValueHeap []WeightedValue

// Len, Less and Swap implement sort.Interface.
func (h WeightedValueHeap) Len() int           { return len(h) }
func (h WeightedValueHeap) Less(i, j int) bool { return h[i].Less(h[j]) }
func (h WeightedValueHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

// Push appends an element to the slice.
func (h *WeightedValueHeap) Push(x interface{}) { *h = append(*h, x.(WeightedValue)) }

// Pop removes the last element of the slice.
func (h *WeightedValueHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// A WeightedReservoirSample implements the weighted reservoir sampling
// algorithm as proposed by Efraimidis-Spirakis (2005).
type WeightedReservoirSample struct {
	size      int
	Heap      heap.Interface
	threshold float64 // keeps track of the minimal key.
}

// NewWeightedReservoirSample creates a new reservoir sample on the given
// heap. If minHeap is nil, an in-memory heap is created and used.
func NewWeightedReservoirSample(size int, minHeap heap.Interface) *WeightedReservoirSample {
	if size < 1 {
		panic("attempt to create a reservoir sample with invalid size")
	}
	if minHeap == nil {
		h := make(WeightedValueHeap, 0, size)
		minHeap = &h
	}
	rs := &WeightedReservoirSample{
		size:      size,
		Heap:      minHeap,
		threshold: 0,
	}
	return rs
}

// Consider offers a new value to the underlying reservoir using weight one.
// Using the same weight for all values considered throughout the lifetime of
// a sample is equivalent to a non-weighted reservoir sampling algorithm.
func (rs *WeightedReservoirSample) Consider(value interface{}) {
	rs.ConsiderWeighted(value, 1)
}

// ConsiderWeighted lets the sample inspect a new value with a positive given
// weight. A weight of one corresponds to the unweighted reservoir sampling
// algorithm. A nonpositive weight will lead to the item being rejected without
// having been observed. To avoid numerical instabilities, it is advisable to
// stay away from zero and infinity, or more generally from regions in which
// computing x**1/weight may be ill-behaved.
func (rs *WeightedReservoirSample) ConsiderWeighted(value interface{}, weight float64) {
	if weight <= 0 {
		glog.Warningf("reservoir sample received non-positive weight %f", weight)
		return
	}
	h := rs.Heap
	wv := WeightedValue{
		Value: value,
		key:   rs.makeKey(weight),
	}
	if h.Len() < rs.size {
		heap.Push(h, wv)
		if rs.threshold == 0 || wv.key < rs.threshold {
			rs.threshold = wv.key
		}
		return
	}

	if wv.key > rs.threshold {
		// Remove the element with threshold key.
		heap.Pop(h)
		// Add in the new element (which has a higher key).
		heap.Push(h, wv)
		// Update the threshold to reflect the new threshold.
		twv := heap.Pop(h).(WeightedValue)
		rs.threshold = twv.key
		heap.Push(h, twv)
	}
}

// makeKey is used internally by ConsiderWeighted for probabilistic sampling.
func (rs *WeightedReservoirSample) makeKey(weight float64) float64 {
	return math.Pow(rand.Float64(), 1.0/weight)
}
