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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package gossip

import (
	"fmt"
	"math"
)

// filter is a counting bloom filter, used to approximate the number
// of differences between InfoStores from different nodes, with
// minimal network overhead.
type filter struct {
	// k is the number of hashes.
	k uint32
	// n is the number of insertions.
	n uint32
	// r is the number of putative removals.
	r uint32
	// b is the number of bits in each slot.
	b uint32
	// m is the number of slots in the filter.
	m uint32
	// maxCount is the maximum count for a slot.
	maxCount uint32
	// data contains the slot data.
	data []byte
	// hasher provides independent hashes.
	hasher *hasher
}

// probFalsePositive computes the probability of a false positive.
func probFalsePositive(N uint32, K uint32, M uint32) float64 {
	pSet := 1.0 - math.Pow(float64(M-1)/float64(M), float64(N*K))
	return math.Pow(pSet, float64(K))
}

// computeOptimalValues computes minimum number of slots such that
// the maximum false positive probability (maxFP) is guaranteed.
// Returns the number of slots (M) as well as optimal number of hashes (K).
//
// Math from: http://en.wikipedia.org/wiki/Bloom_filter
func computeOptimalValues(N uint32, maxFP float64) (uint32, uint32) {
	logN2 := math.Log(2)
	M := uint32(math.Ceil(-float64(N) * math.Log(maxFP) / (logN2 * logN2)))
	K1 := uint32(math.Ceil((float64(M) / float64(N)) * logN2))
	K2 := uint32(math.Floor((float64(M) / float64(N)) * logN2))
	if probFalsePositive(N, K1, M) < probFalsePositive(N, K2, M) {
		return M, K1
	}
	return M, K2
}

// newFilter allocates and returns a new filter with expected number of
// insertions N, Number of bits per slot B, and expected value of a false
// positive < maxFP.
func newFilter(N uint32, B uint32, maxFP float64) (*filter, error) {
	// TODO(spencer): we probably would be well-served using a 3-bit
	// filter, so we should relax the following constraint and get a
	// little bit fancier with the bit arithmetic to handle cross-byte
	// slot values.
	if B != 1 && B != 2 && B != 4 && B != 8 {
		return nil, fmt.Errorf("number of bits (%d) must be a divisor of 8", B)
	}
	if maxFP <= 0 || maxFP >= 1 {
		return nil, fmt.Errorf("max false positives must be 0 <= maxFP < 1: %f", maxFP)
	}
	M, K := computeOptimalValues(N, maxFP)
	maxCount := uint32((1 << B) - 1)
	numBytes := (M*B + 7) / 8
	bytes := make([]byte, numBytes, numBytes)
	return &filter{
		k:        K,
		b:        B,
		m:        M,
		maxCount: maxCount,
		data:     bytes,
		hasher:   newHasher(),
	}, nil
}

// incrementSlot increments slot value by the specified amount, bounding at
// maximum slot value.
func (f *filter) incrementSlot(slot uint32, incr int32) {
	val := int32(f.getSlot(slot)) + incr
	if val > int32(f.maxCount) {
		val = int32(f.maxCount)
	} else if val < 0 {
		val = 0
	}
	bitIndex := slot * f.b
	byteIndex := bitIndex / 8
	byteOffset := bitIndex % 8
	f.data[byteIndex] = byte(uint32(f.data[byteIndex]) & ^(f.maxCount << byteOffset))
	f.data[byteIndex] = byte(uint32(f.data[byteIndex]) | uint32(val)<<byteOffset)
}

// getSlot returns the slot value.
func (f *filter) getSlot(slot uint32) uint32 {
	bitIndex := slot * f.b
	byteIndex := bitIndex / 8
	byteOffset := bitIndex % 8
	return (uint32(f.data[byteIndex]) & (f.maxCount << byteOffset)) >> byteOffset
}

// addKey adds the key to the filter.
func (f *filter) addKey(key string) {
	f.hasher.hashKey(key)
	for i := uint32(0); i < f.k; i++ {
		slot := f.hasher.getHash(i) % f.m
		f.incrementSlot(slot, 1)
	}
	f.n++
}

// hasKey checks whether key has been added to the filter. The chance this
// method returns an incorrect value is given by probFalsePositive().
func (f *filter) hasKey(key string) bool {
	f.hasher.hashKey(key)
	for i := uint32(0); i < f.k; i++ {
		slot := f.hasher.getHash(i) % f.m
		if f.getSlot(slot) == 0 {
			return false
		}
	}
	return true
}

// removeKey removes a key by first verifying it's likely been seen and then
// decrementing each of the slots it hashes to. Returns true if the key was
// "removed"; false otherwise.
func (f *filter) removeKey(key string) bool {
	if f.hasKey(key) {
		f.hasher.hashKey(key)
		for i := uint32(0); i < f.k; i++ {
			slot := f.hasher.getHash(i) % f.m
			f.incrementSlot(slot, -1)
		}
		f.r++
		return true
	}
	return false
}

// probFalsePositive returns the probability the filter returns a false
// positive.
func (f *filter) probFalsePositive() float64 {
	if f.r != 0 {
		return probFalsePositive(f.approximateInsertions(), f.k, f.m)
	}
	return probFalsePositive(f.n, f.k, f.m)
}

// approximateInsertions determines the approximate number of items
// inserted into the Filter after removals.
func (f *filter) approximateInsertions() uint32 {
	count := uint32(0)
	for i := uint32(0); i < f.m; i++ {
		count += f.getSlot(i)
	}
	return count / f.k
}
