// Copyright 2016 The Cockroach Authors.
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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package util

import (
	"math/rand"
	"sort"
	"testing"
)

// testMoveTopKToFront runs MoveTopKToFront for the given k and verifies the
// result.
func testMoveTopKToFront(t *testing.T, k int, data ...int) {
	// Make a copy of the values.
	vals = append([]int(nil), data...)
	MoveTopKToFront(sort.IntSlice(vals), k)
	for l := 0; l < k; l++ {
		for r := k; r < len(data); r++ {
			if vals[l] > vals[r] {
				t.Errorf("Error for k=%d: vals[%d]=%d is greater than vals[%d]=%d",
					k, l, vals[l], r, vals[r])
			}
		}
	}
}

// testMoveTopKToFront runs MoveTopKToFront for all possible Ks and verifies the
// result.
func testMoveTopKToFrontAllK(t *testing.T, data ...int) {
	for k := 0; k <= len(data); k++ {
		testMoveTopKToFront(t, k, data...)
	}
}

func TestMoveTopKToFront(t *testing.T) {
	// Test all possible arrays of length up to 4 with values between 0 and 4.
	for a := 0; a <= 4; a++ {
		testMoveTopKToFrontAllK(t, a)
		for b := 0; b <= 4; b++ {
			testMoveTopKToFrontAllK(t, a, b)
			for c := 0; c <= 4; c++ {
				testMoveTopKToFrontAllK(t, a, b, c)
				for d := 0; d <= 4; d++ {
					testMoveTopKToFrontAllK(t, a, b, c, d)
				}
			}
		}
	}

	// Misc tests.
	tests := [][]int{
		{0, 1, 2, 3, 4, 5, 6, 7},
		{7, 6, 5, 4, 3, 2, 1, 0},
		{7, 0, 6, 1, 5, 2, 4, 3},
		{0, 9, 2, 1, 5, 1, 7, 8, 8, 7},
		{0, 2, 9, 11, 6, 1, 7, 1, 5, 1, 4, 3, 0, 11},
	}
	for _, data := range tests {
		testMoveTopKToFrontAllK(t, data...)
	}

	// Random tests: data of size N with random values in [0, M).
	Ns := []int{10, 100}
	Ms := []int{2, 10, 100, 1000}
	for _, n := range Ns {
		for _, m := range Ms {
			data := make([]int, n)
			for _, i := range data {
				data[i] = rand.Intn(m)
			}
			testMoveTopKToFrontAllK(t, data...)
		}
	}
}

func benchmarkMoveTopKToFront(b *testing.B, data []int) {
	tmp := make([]int, len(data))

	ks := []int{len(data) / 100, len(data) / 10, len(data) / 2}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		copy(tmp, data)
		b.StartTimer()
		MoveTopKToFront(sort.IntSlice(tmp), ks[i%len(ks)])
	}
}

func BenchmarkMoveTopKToFrontRand(b *testing.B) {
	data := make([]int, 100000)
	for _, i := range data {
		data[i] = rand.Intn(len(data))
	}
	benchmarkMoveTopKToFront(b, data)
}

func BenchmarkMoveTopKToFrontSorted(b *testing.B) {
	data := make([]int, 100000)
	for _, i := range data {
		data[i] = i
	}
	benchmarkMoveTopKToFront(b, data)
}

func BenchmarkMoveTopKToFrontRevSorted(b *testing.B) {
	data := make([]int, 100000)
	for _, i := range data {
		data[i] = -i
	}
	benchmarkMoveTopKToFront(b, data)
}
