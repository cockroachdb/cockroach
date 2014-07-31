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
	"testing"
)

func average(sl []interface{}) float64 {
	res := float64(0)
	for _, v := range sl {
		res += float64(v.(int))
	}
	return res / float64(len(sl))
}

func TestReservoirSample(t *testing.T) {
	// This one is cheap so let us run it a bunch of times.
	for r := 0; r < 100; r++ {
		reservoirSize := 500
		reservoir := NewInMemSampleStorage(reservoirSize)
		rs := NewReservoirSample(reservoir)
		offerCount := int64(10000)
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
		maxAvg := 3. / math.Sqrt(float64(reservoirSize))
		if avg := average(rs.Slice()); rs.Seen() != 2*offerCount ||
			math.Abs(avg) > maxAvg {
			t.Errorf("saw %d items, suspicious average: |%f| > %f", rs.Seen(), avg, maxAvg)
		}
	}
}
