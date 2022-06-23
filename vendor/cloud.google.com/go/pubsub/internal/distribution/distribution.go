// Copyright 2017 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package distribution

import (
	"log"
	"math"
	"sort"
	"sync"
	"sync/atomic"
)

// D is a distribution. Methods of D can be called concurrently by multiple
// goroutines.
type D struct {
	buckets []uint64
	// sumsReuse is the scratch space that is reused
	// to store sums during invocations of Percentile.
	// After an invocation of New(n):
	//    len(buckets) == len(sumsReuse) == n
	sumsReuse []uint64
	mu        sync.Mutex
}

// New creates a new distribution capable of holding values from 0 to n-1.
func New(n int) *D {
	return &D{
		buckets:   make([]uint64, n),
		sumsReuse: make([]uint64, n),
	}
}

// Record records value v to the distribution.
// To help with distributions with long tails, if v is larger than the maximum value,
// Record records the maximum value instead.
// If v is negative, Record panics.
func (d *D) Record(v int) {
	if v < 0 {
		log.Panicf("Record: value out of range: %d", v)
	} else if v >= len(d.buckets) {
		v = len(d.buckets) - 1
	}
	atomic.AddUint64(&d.buckets[v], 1)
}

// Percentile computes the p-th percentile of the distribution where
// p is between 0 and 1. This method may be called by multiple goroutines.
func (d *D) Percentile(p float64) int {
	// NOTE: This implementation uses the nearest-rank method.
	// https://en.wikipedia.org/wiki/Percentile#The_nearest-rank_method

	if p < 0 || p > 1 {
		log.Panicf("Percentile: percentile out of range: %f", p)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	var sum uint64
	for i := range d.sumsReuse {
		sum += atomic.LoadUint64(&d.buckets[i])
		d.sumsReuse[i] = sum
	}

	target := uint64(math.Ceil(float64(sum) * p))
	return sort.Search(len(d.sumsReuse), func(i int) bool { return d.sumsReuse[i] >= target })
}
