// Copyright 2018 The Cockroach Authors.
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

package util

import "golang.org/x/tools/container/intsets"

// These constants determine the "small" representation: we pack <numVals>
// values of <numBits> bits into <numWords> 64-bit words. Each value is 0 if the
// corresponding key is not set, otherwise it is the value+1.
//
// It's desirable for efficiency that numBits, numValsPerWord are powers of two.
//
// The current settings support a map from keys in [0, 31] to values in [0, 14].
// Note that one value is reserved to indicate an unmapped element.
const (
	numWords       = 2
	numBits        = 4
	numValsPerWord = 64 / numBits              // 16
	numVals        = numWords * numValsPerWord // 32
	mask           = (1 << numBits) - 1
	maxValue       = mask - 1
)

// FastIntMap is a replacement for map[int]int which is more efficient
// when the values are small. It can be passed by value.
type FastIntMap struct {
	small [numWords]uint64
	large map[int]int
}

// returns -1 if the value is unmapped
func (m *FastIntMap) getSmallVal(idx uint32) int32 {
	word := idx / numValsPerWord
	pos := (idx % numValsPerWord) * numBits
	return int32((m.small[word]>>pos)&mask) - 1
}

func (m *FastIntMap) setSmallVal(idx uint32, val int32) {
	word := idx / numValsPerWord
	pos := (idx % numValsPerWord) * numBits
	m.small[word] = m.small[word] & ^(mask<<pos) | uint64(val+1)<<pos
}

func (m *FastIntMap) toLarge() map[int]int {
	res := make(map[int]int, numVals)
	for i := 0; i < numVals; i++ {
		val := m.getSmallVal(uint32(i))
		if val != -1 {
			res[i] = int(val)
		}
	}
	return res
}

func (m *FastIntMap) Set(key, val int) {
	if m.large == nil {
		if key >= 0 && key < numVals && val >= 0 && val <= maxValue {
			m.setSmallVal(uint32(key), int32(val))
			return
		}
		m.large = m.toLarge()
		m.small = [numWords]uint64{}
	}
	m.large[key] = val
}

func (m *FastIntMap) Unset(key int) {
	if m.large == nil {
		if key < 0 || key >= numVals {
			return
		}
		m.setSmallVal(uint32(key), -1)
	}
	delete(m.large, key)
}

func (m *FastIntMap) Get(key int) (value int, ok bool) {
	if m.large == nil {
		if key < 0 || key >= numVals {
			return -1, false
		}
		val := m.getSmallVal(uint32(key))
		return int(val), (val != -1)
	}
	value, ok = m.large[key]
	return value, ok
}

// MaxKey returns the maximum key that is in the map, or the
// minimum integer value if the map is empty.
func (m *FastIntMap) MaxKey() int {
	if m.large == nil {
		// TODO(radu): we could skip words that are 0
		// and use bits.LeadingZeros64.
		for i := numVals - 1; i >= 0; i-- {
			if m.getSmallVal(uint32(i)) != -1 {
				return i
			}
		}
		return intsets.MinInt
	}
	max := intsets.MinInt
	for k := range m.large {
		if max < k {
			max = k
		}
	}
	return max
}

// ForEach calls the given function for each key/value pair in the map (in
// arbitrary order).
func (m *FastIntMap) ForEach(fn func(key, val int)) {
	if m.large == nil {
		for i := 0; i < numVals; i++ {
			if val := m.getSmallVal(uint32(i)); val != -1 {
				fn(i, int(val))
			}
		}
	} else {
		for k, v := range m.large {
			fn(k, v)
		}
	}
}
