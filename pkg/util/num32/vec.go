// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package num32

import (
	"github.com/cockroachdb/errors"
	"gonum.org/v1/gonum/blas/blas32"
	"gonum.org/v1/gonum/floats/scalar"
)

// L1Distance returns the L1 norm of s - t, which is the Manhattan distance
// between the two vectors. It panics if the argument lengths do not match.
func L1Distance(s []float32, t []float32) float32 {
	checkDims(s, t)
	var distance float32
	for i := range s {
		diff := s[i] - t[i]
		distance += Abs(diff)
	}
	return distance
}

// L2Distance returns the L2 norm of s - t, which is the Euclidean distance
// between the two vectors. It panics if the argument lengths do not match.
func L2Distance(s, t []float32) float32 {
	return Sqrt(L2SquaredDistance(s, t))
}

// L2SquaredDistance returns the squared L2 norm of s - t, which is the squared
// Euclidean distance between the two vectors. Comparing squared distance is
// equivalent to comparing distance, but the squared distance avoids an
// expensive square-root operation. It panics if the argument lengths do not
// match.
func L2SquaredDistance(s, t []float32) float32 {
	checkDims(s, t)
	var distance float32
	for i := range s {
		diff := s[i] - t[i]
		distance += diff * diff
	}
	return distance
}

// Dot returns the dot product of t1 and t2, also called the inner product. It
// panics if the argument lengths do not match.
func Dot(s []float32, t []float32) float32 {
	checkDims(s, t)
	var distance float32
	for i := range s {
		distance += s[i] * t[i]
	}
	return distance
}

// Norm returns the L2 norm of t.
func Norm(t []float32) float32 {
	var norm float32
	for i := range t {
		norm += t[i] * t[i]
	}
	return Sqrt(norm)
}

// Max returns the maximum value in the input slice. If the slice is empty, Max
// will panic.
func Max(s []float32) float32 {
	return s[MaxIdx(s)]
}

// MaxIdx returns the index of the maximum value in the input slice. If several
// entries have the maximum value, the first such index is returned. It panics
// if s is zero length.
func MaxIdx(s []float32) int {
	if len(s) == 0 {
		panic(errors.AssertionFailedf("MaxIdx input cannot be zero"))
	}

	// Set max index to first position that's not NaN.
	var ind int
	var max float32
	for i, v := range s {
		max = v
		ind = i
		if !IsNaN(v) {
			break
		}
	}

	// Now look for larger value. Further NaN values will be ignored.
	for i := ind + 1; i < len(s); i++ {
		if s[i] > max {
			max = s[i]
			ind = i
		}
	}
	return ind
}

// Min returns the minimum value in the input slice. It panics if s is zero
// length.
func Min(s []float32) float32 {
	return s[MinIdx(s)]
}

// MinIdx returns the index of the minimum value in the input slice. If several
// entries have the minimum value, the first such index is returned.
// It panics if s is zero length.
func MinIdx(s []float32) int {
	if len(s) == 0 {
		panic(errors.AssertionFailedf("MinIdx input cannot be zero"))
	}

	// Set min index to first position that's not NaN.
	var ind int
	var min float32
	for i, v := range s {
		min = v
		ind = i
		if !IsNaN(v) {
			break
		}
	}

	// Now look for smaller value. Further NaN values will be ignored.
	for i := ind + 1; i < len(s); i++ {
		if s[i] < min {
			min = s[i]
			ind = i
		}
	}
	return ind
}

// Sum returns the sum of the elements of the given slice.
func Sum(x []float32) float32 {
	var sum float32
	for _, v := range x {
		sum += v
	}
	return sum
}

// Zero sets every element of the given slice to zero.
//
//gcassert:inline
func Zero(dst []float32) {
	clear(dst)
}

// Round sets every element of the "dst" slice to its rounded value, using
// gonum's scalar.Round function.
//
// NB: Upcasting from float32 to float64 to perform the rounding can cause
// precision issues that affect the result.
func Round(dst []float32, prec int) {
	for i := range dst {
		val2 := float64(dst[i])
		val := scalar.Round(val2, prec)
		dst[i] = float32(val)
	}
}

// Add adds, element-wise, the elements of s and dst, and stores the result in
// dst. It panics if the argument lengths do not match.
func Add(dst []float32, s []float32) {
	// blas32.Axpy is significantly faster than Go code on amd64.
	blas32.Axpy(
		1,
		blas32.Vector{N: len(s), Data: s, Inc: 1},
		blas32.Vector{N: len(dst), Data: dst, Inc: 1})
}

// AddTo adds, element-wise, the elements of s and t and stores the result in
// dst. It panics if the argument lengths do not match.
func AddTo(dst []float32, s []float32, t []float32) []float32 {
	copy(dst, s)
	Add(dst, t)
	return dst
}

// Sub subtracts, element-wise, the elements of s from dst. It panics if the
// argument lengths do not match.
func Sub(dst []float32, s []float32) {
	// blas32.Axpy is significantly faster than Go code on amd64.
	blas32.Axpy(
		-1,
		blas32.Vector{N: len(s), Data: s, Inc: 1},
		blas32.Vector{N: len(dst), Data: dst, Inc: 1})
}

// SubTo subtracts, element-wise, the elements of t from s and stores the result
// in dst. It panics if the argument lengths do not match.
func SubTo(dst []float32, s []float32, t []float32) []float32 {
	copy(dst, s)
	Sub(dst, t)
	return dst
}

// Mul performs element-wise multiplication between dst and s and stores the
// value in dst. It panics if the argument lengths do not match.
func Mul(dst []float32, s []float32) {
	// TODO(andyk): Consider implementing this in assembly.
	checkDims(dst, s)
	for i, val := range s {
		dst[i] *= val
	}
}

// MulTo performs element-wise multiplication between s and t and stores the
// value in dst. It panics if the argument lengths do not match.
func MulTo(dst []float32, s []float32, t []float32) []float32 {
	// TODO(andyk): Consider implementing this in assembly.
	checkDims(dst, s)
	for i, val := range s {
		dst[i] = val * t[i]
	}
	return dst
}

// Scale multiplies every element in dst by the scalar c.
func Scale(c float32, dst []float32) {
	// blas32.Scale is significantly faster than Go code on amd64.
	blas32.Scal(c, blas32.Vector{N: len(dst), Data: dst, Inc: 1})
}

// ScaleTo multiplies the elements in s by c and stores the result in dst. It
// panics if the slice argument lengths do not match.
func ScaleTo(dst []float32, c float32, s []float32) []float32 {
	copy(dst, s)
	Scale(c, dst)
	return dst
}

func checkDims(v []float32, v2 []float32) {
	if len(v) != len(v2) {
		panic(errors.AssertionFailedf("different vector dimensions %d and %d", len(v), len(v2)))
	}
}
