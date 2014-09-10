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
	"math"
	"testing"
)

func average(sl WeightedValueHeap) float64 {
	if len(sl) == 0 {
		panic("average of empty slice requested")
	}
	res := float64(0)
	for _, v := range sl {
		res += v.Value.(float64)
	}
	return res / float64(len(sl))
}

// TestWeightedReservoirSample repeatedly constructs size one reservoir samples
// from a very small set of weighted data and checks that the distribution of
// sampled values behaves as expected.
func TestWeightedReservoirSample(t *testing.T) {
	// First test whether the sample correctly ignores invalid weights.
	rs := NewWeightedReservoirSample(1, nil)
	h := rs.Heap.(*WeightedValueHeap)
	rs.ConsiderWeighted(2000.0, 0.0)
	rs.ConsiderWeighted(1234.5, -3.141259)
	if len(*h) != 0 {
		t.Fatalf("values with invalid weights were not ignored")
	}

	weightedValues := []struct {
		v string
		w float64
	}{
		{"a", 0.1},
		{"b", 0.4},
		{"c", 0.8},
		{"d", 5.0},
		{"e", 7.0},
	}

	totalWeight := float64(0)
	for _, wv := range weightedValues {
		totalWeight += wv.w
	}
	expected := make(map[string]float64)
	counts := make(map[string]int)
	for _, wv := range weightedValues {
		expected[wv.v] = wv.w / totalWeight
		counts[wv.v] = 0
	}
	numTrials := 100000
	for i := 0; i < numTrials; i++ {
		rs := NewWeightedReservoirSample(1, nil)
		h := rs.Heap.(*WeightedValueHeap)
		for _, wv := range weightedValues {
			rs.ConsiderWeighted(wv.v, wv.w)
		}
		outcome := (*h)[0].Value.(string)
		counts[outcome]++
	}

	for _, wv := range weightedValues {
		actual := float64(counts[wv.v]) / float64(numTrials)
		diff := math.Abs(expected[wv.v] - actual)
		if math.IsNaN(diff) || diff > 0.005 {
			t.Errorf("value %s: got %f, want %f (%f off)", wv.v, actual, expected[wv.v], diff)
		}
	}
}

// TestUniformReservoirSample compares the expected average of a uniformly weighted large sample.
func TestUniformReservoirSample(t *testing.T) {
	for r := 0; r < 20; r++ {
		reservoirSize := 500
		rs := NewWeightedReservoirSample(reservoirSize, nil)
		h := rs.Heap.(*WeightedValueHeap)
		offerCount := 10000
		for i := 0; i < offerCount; i++ {
			rs.Consider(2.0)
		}
		for i := 0; i < offerCount; i++ {
			rs.Consider(1.0)
		}
		// This is mostly a sanity check, making sure that the average is no
		// more than roughly one standard deviation off the actual result.
		maxOff := 3. / math.Sqrt(float64(reservoirSize))
		mean := 1.5
		if avg := average(*h); math.IsNaN(avg) || math.Abs(avg-mean) > maxOff {
			t.Errorf("suspicious average: |%f-%f| > %f", avg, mean, maxOff)
		}
	}
}
