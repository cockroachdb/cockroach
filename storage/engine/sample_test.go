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

package engine

import (
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/util"
)

func runWithAllStorages(f func(storage util.SampleStorage, size int, t *testing.T), engine Engine, size int, t *testing.T) {
	ess := NewSampleStorage(engine, Key("dummyPrefix"), size)
	f(ess, size, t)
	mss := util.NewInMemSampleStorage(size)
	f(mss, size, t)
}

func average(sl []interface{}) float64 {
	res := float64(0)
	for _, v := range sl {
		res += float64(v.(int))
	}
	return res / float64(len(sl))
}

func TestReservoirSampling(t *testing.T) {
	reservoirSize := 100
	runWithAllEngines(func(engine Engine, t *testing.T) {
		runWithAllStorages(func(reservoir util.SampleStorage, reservoirSize int, t *testing.T) {
			rs := util.NewReservoirSample(reservoir)
			sl1 := []interface{}{1000, 2000, 3000, 4000, 5000}
			rs.Offer(sl1...)
			if !reflect.DeepEqual(rs.Slice(), sl1) || rs.Len() != len(sl1) {
				t.Errorf("wanted %v, got %v", sl1, rs.Slice())
			}
			// Fill up the reservoir, stopping just before it's overflowing.
			for i := len(sl1) + 1; i <= reservoirSize; i++ {
				rs.Offer(1000 * i)
				sl1 = append(sl1, interface{}(1000*i))
			}
			if rs.Len() != len(sl1) {
				t.Errorf("unexpected slice length: %d (wanted %d)", rs.Len(), len(sl1))
			}

			if rsl := rs.Slice(); !reflect.DeepEqual(rsl, sl1) {
				for ind, v := range sl1 {
					if v != rsl[ind] {
						t.Fatalf("%v != %v at %d", v, rsl[ind], ind)
					}
				}
				t.Errorf("wanted %v, got %v (truncated to last 10 items)", sl1[len(sl1)-10:], rsl[len(rsl)-10:])
			}
			rs.Offer(interface{}(123))
			if rs.Len() != len(sl1) || rs.Seen() != int64(len(sl1)+1) {
				t.Errorf("reservoir length is off, expected %d but got %d", len(sl1), rs.Len())
			}

			// Reset the reservoir.
			rs.Reset()
			offerCount := int64(5000)
			// Feed a bunch of ones first, only then a bunch of minus ones.
			for i := int64(0); i < offerCount; i++ {
				rs.Offer(1)
			}
			for i := int64(0); i < offerCount; i++ {
				rs.Offer(-1)
			}
			// This is mostly a sanity check, making sure that the mean
			// average (which is zero) is no more than roughly one standard
			// deviation off the actual result.
			maxAvg := 2. / math.Sqrt(float64(reservoirSize))
			if avg := average(rs.Slice()); rs.Seen() != 2*offerCount ||
				math.Abs(avg) > maxAvg {
				t.Errorf("saw %d items, suspicious average: |%f| > %f", rs.Seen(), avg, maxAvg)
			}
		}, engine, reservoirSize, t)
	}, t)
}

func TestSampleStorage(t *testing.T) {
	runWithAllEngines(func(engine Engine, t *testing.T) {
		testFunc := func() {
			testCases := []struct {
				index int
				value interface{}
			}{
				{3, interface{}("٣")},
				{1, interface{}("bir")},
				{5, interface{}("beş")},
				{4, interface{}("четыре")},
				{0, interface{}("٠")},
				{2, interface{}("zwei")},
			}

			testSize := len(testCases)
			es := NewSampleStorage(engine, Key("sampleprefix"), testSize)
			if size := es.Size(); size != testSize {
				t.Fatalf("size counting is incorrect, wanted %d but got %d", testSize, size)
			}
			// This should not influence Seen() or be stored anywhere
			// since it is out of bounds.
			es.Put(-1, struct{}{})
			es.Put(testSize, "apple")

			for i, c := range testCases {
				es.Put(c.index, c.value)
				// Discard i times, summing up to a running total of
				// (i+1)*(i+2)/2 seen values in each iteration.
				for j := 0; j < i; j++ {
					es.Discard()
				}
				if int(es.Seen()) != ((i+1)*(i+2))/2 {
					t.Fatalf("Seen() is incorrect at i=%d: %d", i, es.Seen())
				}
				val := es.Get(c.index)
				if !reflect.DeepEqual(val, c.value) {
					t.Fatalf("value changed: now %v (vs. %v)", val, c.value)
				}
				if sl := es.Slice(); i+1 != len(sl) {
					t.Fatalf("wanted %d elements, got: %v", i+1, sl)
				}
			}

			sl := es.Slice()
			// Check that the resulting slice contains the information and is
			// in the right order.
			for _, c := range testCases {
				if val := sl[c.index]; val != c.value {
					t.Fatalf("value changed: now %v (vs. %v)", val, c.value)
				}
			}

			newVal := interface{}("overwritten")
			es.Put(0, newVal)
			if sl2 := es.Slice(); !reflect.DeepEqual(sl2[0], newVal) {
				t.Fatalf("overwriting failed: got %v instead of %v", sl2[0], newVal)
			}

			es2 := NewSampleStorage(engine, Key("sampleprefix"), testSize)
			if es2.Size() != es.Size() ||
				!reflect.DeepEqual(es.Slice(), es2.Slice()) ||
				es.Seen() != es2.Seen() {
				t.Errorf("second instance with same prefix lost data")
			}

			// es.Reset()
		}
		testFunc()
		// TODO: Uncomment above and below once es.Reset() is implemented; see comment there.
		// testFunc();
	}, t)
}
