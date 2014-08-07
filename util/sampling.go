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
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package util

import (
	"math"
	"math/rand"

	"github.com/cockroachdb/cockroach/util/log"
)

// A Sample is a subset of elements picked from a large set or stream.
// Elements are made available to the sample by invoking Offer(),
// either with single or multiple inputs. The sampling algorithm
// decides internally which elements are incorporated into the
// sample and whether any existing elements are removed.
// The current size of the sample is returned by Len(); Seen()
// indicates how many elements have been offered thus far.
// Reset() resets the sample to an initial state. A copy of the
// sample can be obtained by Slice().
type Sample interface {
	Len() int
	Seen() int64
	Offer(...interface{})
	Slice() []interface{}
	Reset()
}

// A SampleStorage is an abstract representation of a storage that
// a Sample uses.
// Size() returns the internal capacity of the sample, Seen() the
// number of values observed so far (whether or not they were used
// in the sample). Any implementation is asked to make sure that
// the values returned by Seen() can never decrease. In effect, this
// means that Seen() will not increase once Seen() == math.MaxInt64.
// Discard() has to be called for an item which is observed, but not
// saved to the SampleStorage using Put().
// Slice() will return a slice containing the sampled values.
type SampleStorage interface {
	Size() int
	Seen() int64
	Discard()
	Get(int) interface{}
	Put(int, interface{})
	Slice() []interface{}
	Reset()
}

// InMemSampleStorage implements SampleStorage. It contains an internal
// slice that it maps all operations to.
type InMemSampleStorage struct {
	size int
	seen int64
	sl   []interface{}
}

// NewInMemSampleStorage creates a InMemSampleStorage of the given size.
func NewInMemSampleStorage(size int) SampleStorage {
	if size < 1 {
		log.Fatalf("attempt to create reservoir with illegal size %d", size)
	}
	imss := &InMemSampleStorage{size: size}
	imss.Reset()
	return imss
}

// Size returns the amount of elements this storage can hold.
func (s *InMemSampleStorage) Size() int { return s.size }

// Seen returns the amount of observed elements.
func (s *InMemSampleStorage) Seen() int64 { return s.seen }

// Reset resets the storage.
func (s *InMemSampleStorage) Reset() {
	s.seen = 0
	if s.sl == nil {
		s.sl = make([]interface{}, 0, s.size)
	} else {
		s.sl = s.sl[:0]
	}
}

// Slice returns a copy of the internal slice.
func (s *InMemSampleStorage) Slice() []interface{} {
	return append([]interface{}(nil), s.sl...)
}

// Get returns the value stored at index i. If no value is found,
// a nil interface is returned.
func (s *InMemSampleStorage) Get(i int) interface{} {
	if i >= 0 && i < len(s.sl) {
		return s.Get(i)
	}
	return nil
}

// Put stores the given value in the internal slice at the
// specified index.
func (s *InMemSampleStorage) Put(i int, v interface{}) {
	s.see()
	if i < s.size {
		if i >= len(s.sl) {
			s.sl = append(s.sl, v)
		} else {
			s.sl[i] = v
		}
	}
}

// Discard notifies the InMemSampleStorage of an object having
// been taken from the stream, but not stored. Effectively it
// increases Seen() by one.
func (s *InMemSampleStorage) Discard() {
	s.see()
}

// see is internally used to increase Seen() while dealing with
// overflows appropriately.
func (s *InMemSampleStorage) see() {
	if s.seen < math.MaxInt64 {
		s.seen++
	}
}

// A ReservoirSample implements the reservoir sampling algorithm.
// It fills up an internal slice and, once the slice is full,
// swaps in new elements randomly. Each element offered to the
// reservoir has equal chances of being contained in the final
// sample.
// ReservoirSample implements the Sample interface. It is not
// safe for concurrent access.
type ReservoirSample struct {
	reservoir SampleStorage
}

// NewReservoirSample creates a new reservoir sample with the
// given size, returning it as a Sample.
func NewReservoirSample(reservoir SampleStorage) Sample {
	rs := &ReservoirSample{
		reservoir: reservoir,
	}
	return rs
}

// Len returns the length of the slice that would be returned by Slice().
func (rs *ReservoirSample) Len() int {
	size, seen := rs.reservoir.Size(), rs.reservoir.Seen()
	if seen < int64(size) {
		return int(seen)
	}
	return size
}

// Reset resets the reservoir to its initial state.
func (rs *ReservoirSample) Reset() {
	rs.reservoir.Reset()
}

// Seen returns the number of individual elements that have been passed
// through Offer() so far since either the creation or last reset of the
// reservoir.
func (rs *ReservoirSample) Seen() int64 {
	return rs.reservoir.Seen()
}

// Slice returns a slice containing the sample obtained from all values
// passed to Offer(). The slice is a copy and may be used as desired without
// impairing the correctness of further sampling. The individual elements of
// the slice, referenced as interfaces, will not have been copied.
func (rs *ReservoirSample) Slice() []interface{} {
	return rs.reservoir.Slice()
}

// Offer takes one or multiple values and uses them as inputs for the sample
// being formed.
func (rs *ReservoirSample) Offer(values ...interface{}) {
	// Some potential for future batch operations here, but
	// for now let's just loop.
	for _, v := range values {
		rs.offerOne(v)
	}
}

// offerOne implements the reservoir sampling algorithm. Element #n is put
// into the array with probability Size()/#n, unless there is still room
// in the internal slice.
func (rs *ReservoirSample) offerOne(value interface{}) {
	if curLen, size := rs.Len(), rs.reservoir.Size(); curLen < size {
		// The reservoir isn't full yet, so just put the value.
		rs.reservoir.Put(curLen, value)
	} else {
		seen := rs.reservoir.Seen()
		if seen < math.MaxInt64 {
			seen++
		}
		// Pick a random slot from [0, seen). Note that the
		// SampleStorage will stop increasing Seen() on pending
		// overflow, so that in this case we continue sampling
		// with final probability size/math.MaxInt64.
		slot := rand.Int63n(seen)
		// If this slot is in the reservoir, replace its contents.
		if slot < int64(size) {
			rs.reservoir.Put(int(slot), value)
		} else {
			// If we do not put this item into the sample, let the
			// sample know so it can update Seen() accordingly.
			rs.reservoir.Discard()
		}
	}
}
