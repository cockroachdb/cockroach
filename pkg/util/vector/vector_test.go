// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vector

import (
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var NaN32 = float32(math.NaN())
var Inf32 = float32(math.Inf(1))

func TestParseVector(t *testing.T) {
	testCases := []struct {
		input    string
		expected T
		hasError bool
	}{
		{input: "[1,2,3]", expected: T{1, 2, 3}, hasError: false},
		{input: "[1.0, 2.0, 3.0]", expected: T{1.0, 2.0, 3.0}, hasError: false},
		{input: "[1.0, 2.0, 3.0", expected: T{}, hasError: true},
		{input: "1.0, 2.0, 3.0]", expected: T{}, hasError: true},
		{input: "[1.0, 2.0, [3.0]]", expected: T{}, hasError: true},
		{input: "1.0, 2.0, 3.0]", expected: T{}, hasError: true},
		{input: "1.0, , 3.0]", expected: T{}, hasError: true},
		{input: "", expected: T{}, hasError: true},
		{input: "[]", expected: T{}, hasError: true},
		{input: "1.0, 2.0, 3.0", expected: T{}, hasError: true},
	}

	for _, tc := range testCases {
		result, err := ParseVector(tc.input)

		if tc.hasError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, result)
			// Test roundtripping through String().
			s := result.String()
			result, err = ParseVector(s)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		}
	}

	// Test the maxdims error case.
	var sb strings.Builder
	sb.WriteString("[")
	for i := 0; i < MaxDim; i++ {
		sb.WriteString("1,")
	}
	sb.WriteString("1]")
	_, err := ParseVector(sb.String())
	assert.Errorf(t, err, "vector cannot have more than %d dimensions", MaxDim)
}

func TestRoundtripRandomPGVector(t *testing.T) {
	rng, _ := randutil.NewTestRand()
	extra := randutil.RandBytes(rng, 10)
	for i := 0; i < 1000; i++ {
		v := Random(rng, 1000 /* maxDim */)
		encoded, err := Encode(nil, v)
		assert.NoError(t, err)
		encoded = append(encoded, extra...)
		remaining, roundtripped, err := Decode(encoded)
		assert.NoError(t, err)
		assert.Equal(t, v.String(), roundtripped.String())
		assert.Equal(t, extra, remaining)
		reEncoded, err := Encode(nil, roundtripped)
		assert.NoError(t, err)
		assert.Equal(t, encoded, append(reEncoded, extra...))
	}
}

func TestCosDistance(t *testing.T) {
	// Test L1, L2, Cosine distance.
	testCases := []struct {
		v1  []float32
		v2  []float32
		cos float64
		err bool
	}{
		{v1: []float32{}, v2: []float32{}, cos: math.NaN(), err: false},
		{v1: []float32{1, 2, 3}, v2: []float32{4, 5, 6}, cos: 0.02536815, err: false},
		{v1: []float32{-1, -2, -3}, v2: []float32{-4, -5, -6}, cos: 0.02536815, err: false},
		{v1: []float32{1, 2, 3}, v2: []float32{1, 2, 3}, cos: 0, err: false},
		{v1: []float32{1, 2, 3}, v2: []float32{1, 2, 4}, cos: 0.008539, err: false},
		{v1: []float32{NaN32}, v2: []float32{1}, cos: math.NaN(), err: false},
		{v1: []float32{Inf32}, v2: []float32{1}, cos: math.NaN(), err: false},
		// Different vector sizes errors.
		{v1: T{1, 2, 3}, v2: T{4, 5}, err: true},
	}

	for _, tc := range testCases {
		cos, cosErr := CosDistance(tc.v1, tc.v2)
		if tc.err {
			assert.Error(t, cosErr)
		} else {
			assert.NoError(t, cosErr)
			assert.InDelta(t, tc.cos, cos, 0.000001)
		}
	}
}

func TestNorm(t *testing.T) {
	testCases := []struct {
		v    T
		norm float64
	}{
		{v: T{}, norm: 0},
		{v: T{1, 2, 3}, norm: 3.7416573867739413},
		{v: T{0, 0, 0}, norm: 0},
		{v: T{-1, -2, -3}, norm: 3.7416573867739413},
	}

	for _, tc := range testCases {
		norm := Norm(tc.v)
		assert.InDelta(t, tc.norm, norm, 0.000001)
	}
}

// While the real work of these functions is done by the num32 package, test
// that the wrapper functions are working.
func TestNum32Functions(t *testing.T) {
	_, err := L1Distance(T{1, 2}, T{3, 4, 5})
	require.Error(t, err)
	res, err := L1Distance(T{1, 2, 3}, T{4, 5, 6})
	require.NoError(t, err)
	require.Equal(t, float64(9), res)

	_, err = L2Distance(T{1, 2}, T{3, 4, 5})
	require.Error(t, err)
	res, err = L2Distance(T{1, 2, 3}, T{4, 5, 6})
	require.NoError(t, err)
	require.InDelta(t, float64(5.196152422), res, 0.000001)

	_, err = InnerProduct(T{1, 2}, T{3, 4, 5})
	require.Error(t, err)
	res, err = InnerProduct(T{1, 2, 3}, T{4, 5, 6})
	require.NoError(t, err)
	require.Equal(t, float64(32), res)

	_, err = NegInnerProduct(T{1, 2}, T{3, 4, 5})
	require.Error(t, err)
	res, err = NegInnerProduct(T{1, 2, 3}, T{4, 5, 6})
	require.NoError(t, err)
	require.Equal(t, float64(-32), res)
}

func TestPointwiseOps(t *testing.T) {
	testCases := []struct {
		v1    T
		v2    T
		add   T
		minus T
		mult  T
		err   bool
	}{
		{v1: T{1, 2, 3}, v2: T{4, 5, 6}, add: T{5, 7, 9}, minus: T{-3, -3, -3}, mult: T{4, 10, 18}, err: false},
		{v1: T{-1, -2, -3}, v2: T{-4, -5, -6}, add: T{-5, -7, -9}, minus: T{3, 3, 3}, mult: T{4, 10, 18}, err: false},
		{v1: T{0, 0, 0}, v2: T{0, 0, 0}, add: T{0, 0, 0}, minus: T{0, 0, 0}, mult: T{0, 0, 0}, err: false},
		{v1: T{1, 2, 3}, v2: T{1, 2, 3}, add: T{2, 4, 6}, minus: T{0, 0, 0}, mult: T{1, 4, 9}, err: false},
		{v1: T{1, 2, 3}, v2: T{1, 2, 4}, add: T{2, 4, 7}, minus: T{0, 0, -1}, mult: T{1, 4, 12}, err: false},
		// Different vector sizes errors.
		{v1: T{1, 2, 3}, v2: T{4, 5}, err: true},
	}

	for _, tc := range testCases {
		add, addErr := Add(tc.v1, tc.v2)
		minus, minusErr := Minus(tc.v1, tc.v2)
		mult, multErr := Mult(tc.v1, tc.v2)

		if tc.err {
			assert.Error(t, addErr)
			assert.Error(t, minusErr)
			assert.Error(t, multErr)
		} else {
			assert.NoError(t, addErr)
			assert.NoError(t, minusErr)
			assert.NoError(t, multErr)
			assert.Equal(t, tc.add, add)
			assert.Equal(t, tc.minus, minus)
			assert.Equal(t, tc.mult, mult)
		}
	}
}
