// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package num32

import (
	"math"
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

var NaN32 = float32(math.NaN())
var Inf32 = float32(math.Inf(1))

func TestDistances(t *testing.T) {
	// Test L1, L2, Cosine distance.
	testCases := []struct {
		v1     []float32
		v2     []float32
		l1     float32
		l2s    float32
		panics bool
	}{
		{v1: []float32{}, v2: []float32{}, l1: 0, l2s: 0},
		{v1: []float32{1, 2, 3}, v2: []float32{4, 5, 6}, l1: 9, l2s: 27},
		{v1: []float32{-1, -2, -3}, v2: []float32{-4, -5, -6}, l1: 9, l2s: 27},
		{v1: []float32{1, 2, 3}, v2: []float32{1, 2, 3}, l1: 0, l2s: 0},
		{v1: []float32{1, 2, 3}, v2: []float32{1, 2, 4}, l1: 1, l2s: 1},
		{v1: []float32{NaN32}, v2: []float32{1}, l1: NaN32, l2s: NaN32},
		{v1: []float32{Inf32}, v2: []float32{1}, l1: Inf32, l2s: Inf32},
		{v1: []float32{1, 2}, v2: []float32{3, 4, 5}, panics: true},
	}

	for _, tc := range testCases {
		if !tc.panics {
			l1 := L1Distance(tc.v1, tc.v2)
			l2s := L2SquaredDistance(tc.v1, tc.v2)
			require.InDelta(t, tc.l1, l1, 0.000001)
			require.InDelta(t, tc.l2s, l2s, 0.000001)
		} else {
			require.Panics(t, func() { L1Distance(tc.v1, tc.v2) })
			require.Panics(t, func() { L2SquaredDistance(tc.v1, tc.v2) })
		}
	}
}

func TestInnerProduct(t *testing.T) {
	testCases := []struct {
		v1     []float32
		v2     []float32
		ip     float32
		panics bool
	}{
		{v1: []float32{}, v2: []float32{}, ip: 0},
		{v1: []float32{1, 2, 3}, v2: []float32{4, 5, 6}, ip: 32},
		{v1: []float32{-1, -2, -3}, v2: []float32{-4, -5, -6}, ip: 32},
		{v1: []float32{0, 0, 0}, v2: []float32{0, 0, 0}, ip: 0},
		{v1: []float32{1, 2, 3}, v2: []float32{1, 2, 3}, ip: 14},
		{v1: []float32{1, 2, 3}, v2: []float32{1, 2, 4}, ip: 17},
		{v1: []float32{NaN32}, v2: []float32{1}, ip: NaN32},
		{v1: []float32{Inf32}, v2: []float32{1}, ip: Inf32},
		{v1: []float32{1, 2}, v2: []float32{3, 4, 5}, panics: true},
	}

	for _, tc := range testCases {
		if !tc.panics {
			ip := InnerProduct(tc.v1, tc.v2)
			require.InDelta(t, tc.ip, ip, 0.000001)
		} else {
			require.Panics(t, func() { InnerProduct(tc.v1, tc.v2) })
		}
	}
}

func TestZero(t *testing.T) {
	// Empty slice.
	Zero([]float32{})

	// Larger slice.
	input := []float32{1, 2, 3, 4}
	Zero(input)
	require.Equal(t, []float32{0, 0, 0, 0}, input)
}

// TestBlasWrappers contains tests for thin wrappers around BLAS functions.
func TestBlasWrappers(t *testing.T) {
	dst := []float32{1, 2, 3}
	s := []float32{4, 5, 6}
	Add(dst, s)
	require.Equal(t, []float32{5, 7, 9}, dst)
	require.Equal(t, []float32{4, 5, 6}, s)

	dst = []float32{1, 2, 3}
	Sub(dst, s)
	require.Equal(t, []float32{-3, -3, -3}, dst)
	require.Equal(t, []float32{4, 5, 6}, s)

	dst = []float32{1, 2, 3}
	Scale(1.5, dst)
	require.Equal(t, []float32{1.5, 3, 4.5}, dst)
}

func TestMul(t *testing.T) {
	testCases := []struct {
		v1     []float32
		v2     []float32
		res    []float32
		panics bool
	}{
		{v1: []float32{}, v2: []float32{}, res: []float32{}},
		{v1: []float32{1, 2}, v2: []float32{3, 4}, res: []float32{3, 8}},
		{v1: []float32{1, 2, 3}, v2: []float32{4, 5, 6}, res: []float32{4, 10, 18}},
		{v1: []float32{1, 2}, v2: []float32{3, 4, 5}, panics: true},
	}

	for _, tc := range testCases {
		if !tc.panics {
			original := slices.Clone(tc.v2)
			Mul(tc.v1, tc.v2)
			require.Equal(t, tc.res, tc.v1)
			require.Equal(t, original, tc.v2)
		} else {
			require.Panics(t, func() { Mul(tc.v1, tc.v2) })
		}
	}
}

func TestRound(t *testing.T) {
	// Empty slice.
	Round([]float64{}, 2)

	// Larger slice.
	input := []float64{1, 2.1, 3.22, 4.245, 4.255, 0.2345678}
	Round(input, 2)
	require.Equal(t, []float64{1, 2.1, 3.22, 4.25, 4.26, 0.23}, input)
}

func BenchmarkInnerProduct(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	v1 := makeRandomVector(rng, 512)
	v2 := makeRandomVector(rng, 512)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		InnerProduct(v1, v2)
	}
}

func BenchmarkMul(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	v1 := makeRandomVector(rng, 512)
	v2 := makeRandomVector(rng, 512)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Mul(v1, v2)
	}
}

func BenchmarkAdd(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	v1 := makeRandomVector(rng, 512)
	v2 := makeRandomVector(rng, 512)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Add(v1, v2)
		Sub(v1, v2)
	}
}

func BenchmarkScale(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	v1 := makeRandomVector(rng, 512)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Scale(3.5, v1)
		Scale(1/3.5, v1)
	}
}

func makeRandomVector(rng *rand.Rand, dims int) []float32 {
	v := make([]float32, dims)
	for i := range v {
		v[i] = float32(rng.NormFloat64())
	}
	return v
}
