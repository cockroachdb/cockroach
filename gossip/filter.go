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
	K        uint32  // Number of hashes
	N        uint32  // Number of insertions
	R        uint32  // Number of putative removals
	B        uint32  // Number of bits in each slot
	M        uint32  // Number of slots in filter
	MaxCount uint32  // Maximum count for a slot
	Data     []byte  // Slot data
	hasher   *hasher // Provides independent hashes
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
// positive < maxFP. Number of bits must be 0 < B <= 8.
func newFilter(N uint32, B uint32, maxFP float64) (*filter, error) {
	if N == 0 {
		return nil, fmt.Errorf("number of insertions (N) must be > 0")
	}
	if B == 0 || B > 8 {
		return nil, fmt.Errorf("number of bits (%d) must be 0 < B <= 8", B)
	}
	if maxFP <= 0 || maxFP >= 1 {
		return nil, fmt.Errorf("max false positives must be 0 <= maxFP < 1: %f", maxFP)
	}
	M, K := computeOptimalValues(N, maxFP)
	maxCount := uint32((1 << B) - 1)
	numBytes := (M*B + 7) / 8
	bytes := make([]byte, numBytes, numBytes)
	return &filter{
		K:        K,
		B:        B,
		M:        M,
		MaxCount: maxCount,
		Data:     bytes,
		hasher:   newHasher(),
	}, nil
}

// visitSlotBytes visits each byte (either one or two) that make up
// the bits in the specified slot. "fn" is invoked on each byte in
// turn, with values supplied for byteIndex, byteOffset, bitMask,
// and valBitOffset.
func (f *filter) visitSlotBytes(slot uint32, fn func(uint32, uint32, uint32, uint32)) {
	bitIndex := slot * f.B
	byteIndex := bitIndex / 8
	byteOffset := bitIndex % 8

	// Things are tricky here because we deal with crossing byte boundaries.
	lastBit := byteOffset + f.B
	valBitOffset := uint32(0)
	for bit := byteOffset; bit < lastBit; {
		b := lastBit - bit
		if b > 8-byteOffset {
			b = 8 - byteOffset
		}
		bitMask := uint32((1 << b) - 1)

		fn(byteIndex, byteOffset, bitMask, valBitOffset) // call supplied method

		bit += b
		valBitOffset += b
		byteIndex++
		byteOffset = (byteOffset + b) % 8
	}
}

// incrementSlot increments slot value by the specified amount, bounding at
// maximum slot value.
func (f *filter) incrementSlot(slot uint32, incr int32) {
	val := int32(f.getSlot(slot)) + incr
	if val > int32(f.MaxCount) {
		val = int32(f.MaxCount)
	} else if val < 0 {
		val = 0
	}
	f.visitSlotBytes(slot, func(byteIndex uint32, byteOffset uint32, bitMask uint32, valBitOffset uint32) {
		f.Data[byteIndex] = byte(uint32(f.Data[byteIndex]) & ^(bitMask << byteOffset))
		f.Data[byteIndex] = byte(uint32(f.Data[byteIndex]) | (uint32(val>>valBitOffset)&bitMask)<<byteOffset)
	})
}

// getSlot returns the slot value.
func (f *filter) getSlot(slot uint32) uint32 {
	val := uint32(0)
	f.visitSlotBytes(slot, func(byteIndex uint32, byteOffset uint32, bitMask uint32, valBitOffset uint32) {
		val |= ((uint32(f.Data[byteIndex]) & (bitMask << byteOffset)) >> byteOffset) << valBitOffset
	})
	return val
}

// addKey adds the key to the filter.
func (f *filter) addKey(key string) {
	f.hasher.hashKey(key)
	for i := uint32(0); i < f.K; i++ {
		slot := f.hasher.getHash(i) % f.M
		f.incrementSlot(slot, 1)
	}
	f.N++
}

// hasKey checks whether key has been added to the filter. The chance this
// method returns an incorrect value is given by ProbFalsePositive().
func (f *filter) hasKey(key string) bool {
	f.hasher.hashKey(key)
	for i := uint32(0); i < f.K; i++ {
		slot := f.hasher.getHash(i) % f.M
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
		for i := uint32(0); i < f.K; i++ {
			slot := f.hasher.getHash(i) % f.M
			f.incrementSlot(slot, -1)
		}
		f.R++
		return true
	}
	return false
}

// probFalsePositive returns the probability the filter returns a false
// positive.
func (f *filter) probFalsePositive() float64 {
	if f.R != 0 {
		return probFalsePositive(f.approximateInsertions(), f.K, f.M)
	}
	return probFalsePositive(f.N, f.K, f.M)
}

// approximateInsertions determines the approximate number of items
// inserted into the filter after removals.
func (f *filter) approximateInsertions() uint32 {
	count := uint32(0)
	for i := uint32(0); i < f.M; i++ {
		count += f.getSlot(i)
	}
	return count / f.K
}
