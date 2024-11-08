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
	// Test L1, L2 and L2 squared distances.
	testCases := []struct {
		v1     []float32
		v2     []float32
		l1     float32
		l2     float32
		l2s    float32
		panics bool
	}{
		{v1: []float32{}, v2: []float32{}, l1: 0, l2: 0, l2s: 0},
		{v1: []float32{1, 2, 3}, v2: []float32{4, 5, 6}, l1: 9, l2: 5.196152, l2s: 27},
		{v1: []float32{-1, -2, -3}, v2: []float32{-4, -5, -6}, l1: 9, l2: 5.196152, l2s: 27},
		{v1: []float32{1, 2, 3}, v2: []float32{1, 2, 3}, l1: 0, l2: 0, l2s: 0},
		{v1: []float32{1, 2, 3}, v2: []float32{1, 2, 4}, l1: 1, l2: 1, l2s: 1},
		{v1: []float32{NaN32}, v2: []float32{1}, l1: NaN32, l2: NaN32, l2s: NaN32},
		{v1: []float32{Inf32}, v2: []float32{1}, l1: Inf32, l2: Inf32, l2s: Inf32},
		{v1: []float32{1, 2}, v2: []float32{3, 4, 5}, panics: true},
	}

	for _, tc := range testCases {
		if !tc.panics {
			l1 := L1Distance(tc.v1, tc.v2)
			l2 := L2Distance(tc.v1, tc.v2)
			l2s := L2SquaredDistance(tc.v1, tc.v2)
			require.InDelta(t, tc.l1, l1, 0.000001)
			require.InDelta(t, tc.l2, l2, 0.000001)
			require.InDelta(t, tc.l2s, l2s, 0.000001)
		} else {
			require.Panics(t, func() { L1Distance(tc.v1, tc.v2) })
			require.Panics(t, func() { L2Distance(tc.v1, tc.v2) })
			require.Panics(t, func() { L2SquaredDistance(tc.v1, tc.v2) })
		}
	}
}

func TestDot(t *testing.T) {
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
			ip := Dot(tc.v1, tc.v2)
			require.InDelta(t, tc.ip, ip, 0.000001)
		} else {
			require.Panics(t, func() { Dot(tc.v1, tc.v2) })
		}
	}
}

func TestNorm(t *testing.T) {
	testCases := []struct {
		v    []float32
		norm float32
	}{
		{v: []float32{}, norm: 0},
		{v: []float32{1, 2, 3}, norm: 3.7416574},
		{v: []float32{0, 0, 0}, norm: 0},
		{v: []float32{-1, -2, -3}, norm: 3.7416574},
	}

	for _, tc := range testCases {
		norm := Norm(tc.v)
		require.InDelta(t, tc.norm, norm, 0.000001)
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

func TestMinMax(t *testing.T) {
	testCases := []struct {
		v      []float32
		min    int
		max    int
		norm   float32
		panics bool
	}{
		{v: []float32{10, 2, 8, -10}, min: 3, max: 0},
		{v: []float32{-10, 2, 8, 10}, min: 0, max: 3},
		{v: []float32{2, -10, 10, 8}, min: 1, max: 2},
		{v: []float32{NaN32, NaN32, 2, -10, NaN32, 10, 8, NaN32}, min: 3, max: 5},
		{v: []float32{NaN32}, min: 0, max: 0},
		{v: []float32{NaN32, NaN32}, min: 1, max: 1},

		{v: []float32{}, panics: true},
	}

	for _, tc := range testCases {
		if !tc.panics {
			ind := MinIdx(tc.v)
			require.Equal(t, tc.min, ind)
			res := Min(tc.v)
			testEqualOrNaN(t, tc.v[tc.min], res)

			ind = MaxIdx(tc.v)
			require.Equal(t, tc.max, ind)
			res = Max(tc.v)
			testEqualOrNaN(t, tc.v[tc.max], res)
		} else {
			require.Panics(t, func() { MinIdx(tc.v) })
			require.Panics(t, func() { Min(tc.v) })
			require.Panics(t, func() { MaxIdx(tc.v) })
			require.Panics(t, func() { Max(tc.v) })
		}
	}
}

func TestSum(t *testing.T) {
	testCases := []struct {
		v   []float32
		sum float32
	}{
		{v: []float32{}, sum: 0},
		{v: []float32{1}, sum: 1},
		{v: []float32{1, 2, 3}, sum: 6},
		{v: []float32{-1, -2, -3}, sum: -6},
		{v: []float32{Inf32, Inf32}, sum: Inf32},
		{v: []float32{Inf32, -Inf32}, sum: NaN32},
		{v: []float32{0, NaN32, 1}, sum: NaN32},
	}

	for _, tc := range testCases {
		testEqualOrNaN(t, tc.sum, Sum(tc.v))
	}
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

// Test add, sub, mul, scale operations.
func TestArithmetic(t *testing.T) {
	testCases := []struct {
		v1     []float32
		v2     []float32
		add    []float32
		sub    []float32
		mul    []float32
		panics bool
	}{
		{
			v1:  []float32{},
			v2:  []float32{},
			add: []float32{},
			sub: []float32{},
			mul: []float32{},
		},
		{
			v1:  []float32{1, 2, 3},
			v2:  []float32{4, 5, 6},
			add: []float32{5, 7, 9},
			sub: []float32{-3, -3, -3},
			mul: []float32{4, 10, 18},
		},
		{
			v1:  []float32{-1, -2, -3},
			v2:  []float32{-4, -5, -6},
			add: []float32{-5, -7, -9},
			sub: []float32{3, 3, 3},
			mul: []float32{4, 10, 18},
		},
		{
			v1:  []float32{0, 0, 0},
			v2:  []float32{0, 0, 0},
			add: []float32{0, 0, 0},
			sub: []float32{0, 0, 0},
			mul: []float32{0, 0, 0},
		},
		{
			v1:  []float32{1, 2, 3},
			v2:  []float32{1, 2, 3},
			add: []float32{2, 4, 6},
			sub: []float32{0, 0, 0},
			mul: []float32{1, 4, 9},
		},
		{
			v1:  []float32{1, 2, 3},
			v2:  []float32{1, 2, 4},
			add: []float32{2, 4, 7},
			sub: []float32{0, 0, -1},
			mul: []float32{1, 4, 12},
		},

		{v1: []float32{1, 2}, v2: []float32{3, 4, 5}, panics: true},
	}

	validate := func(
		fn1 func(dst, s []float32),
		fn2 func(dst, s, t []float32) []float32,
		v1 []float32,
		v2 []float32,
		expected []float32,
	) {
		v1Clone := slices.Clone(v1)
		v2Clone := slices.Clone(v2)
		fn1(v1Clone, v2Clone)
		require.Equal(t, expected, v1Clone)
		require.Equal(t, v2, v2Clone)

		v1Clone = slices.Clone(v1)
		v2Clone = slices.Clone(v2)
		actual := fn2(make([]float32, len(v1)), v1Clone, v2Clone)
		require.Equal(t, expected, actual)
		require.Equal(t, v1, v1Clone)
		require.Equal(t, v2, v2Clone)
	}

	for _, tc := range testCases {
		if !tc.panics {
			validate(Add, AddTo, tc.v1, tc.v2, tc.add)
			validate(Sub, SubTo, tc.v1, tc.v2, tc.sub)
			validate(Mul, MulTo, tc.v1, tc.v2, tc.mul)
		} else {
			require.Panics(t, func() { Mul(tc.v1, tc.v2) })
		}
	}
}

func TestScale(t *testing.T) {
	testCases := []struct {
		v     []float32
		c     float32
		scale []float32
	}{
		{v: []float32{}, c: 10, scale: []float32{}},
		{v: []float32{1, 2, 3}, c: 1.5, scale: []float32{1.5, 3, 4.5}},
		{v: []float32{0, 0, 0}, c: 10, scale: []float32{0, 0, 0}},
		{v: []float32{-1, -2, -3}, c: 2, scale: []float32{-2, -4, -6}},
		{v: []float32{-Inf32, Inf32}, c: 2, scale: []float32{-Inf32, Inf32}},
	}

	for _, tc := range testCases {
		v := slices.Clone(tc.v)
		Scale(tc.c, v)
		require.Equal(t, tc.scale, v)

		v = slices.Clone(tc.v)
		dst := ScaleTo(make([]float32, len(v)), tc.c, v)
		require.Equal(t, tc.scale, dst)
		require.Equal(t, tc.v, v)
	}
}

func TestRound(t *testing.T) {
	// Empty slice.
	Round([]float32{}, 2)

	// Larger slice.
	input := []float32{1, 2.1, 3.22, 4.245, -4.245, 0.2345678, Inf32}
	Round(input, 2)
	require.Equal(t, []float32{1, 2.1, 3.22, 4.24, -4.24, 0.23, Inf32}, input)
}

func BenchmarkDot(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	v1 := makeRandomVector(rng, 512)
	v2 := makeRandomVector(rng, 512)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Dot(v1, v2)
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

func BenchmarkNorm(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	v1 := makeRandomVector(rng, 512)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Norm(v1)
	}
}

func makeRandomVector(rng *rand.Rand, dims int) []float32 {
	v := make([]float32, dims)
	for i := range v {
		v[i] = float32(rng.NormFloat64())
	}
	return v
}

func testEqualOrNaN(t *testing.T, v1, v2 float32) {
	require.True(t, v1 == v2 || (IsNaN(v1) && IsNaN(v2)), "%v != %v", v1, v2)
}
