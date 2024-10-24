// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package num32

import (
	"math"

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
		distance += float32(math.Abs(float64(diff)))
	}
	return distance
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

// InnerProduct returns the inner product of t1 and t2, also called the dot
// product. It panics if the argument lengths do not match.
func InnerProduct(s []float32, t []float32) float32 {
	var distance float32
	for i := range s {
		distance += s[i] * t[i]
	}
	return distance
}

// Zero sets every element of the given slice to zero.
//
//gcassert:inline
func Zero(dst []float32) {
	for i := range dst {
		dst[i] = 0.0
	}
}

// Round sets very element of the "dst" slice to its half away from zero rounded
// value, with the given precision.
//
// Special cases are:
//
//	Round(±0) = +0
//	Round(±Inf) = ±Inf
//	Round(NaN) = NaN
func Round(dst []float64, prec int) {
	for i := 0; i < len(dst); i++ {
		dst[i] = scalar.Round(dst[i], prec)
	}
}

// Add adds, element-wise, the elements of s and dst, and stores the result in
// dst. It panics if the argument lengths do not match.
//
//gcassert:inline
func Add(dst []float32, s []float32) {
	// blas32.Axpy is significantly faster than Go code on amd64.
	blas32.Axpy(
		1,
		blas32.Vector{N: len(s), Data: s, Inc: 1},
		blas32.Vector{N: len(dst), Data: dst, Inc: 1})
}

// Sub subtracts, element-wise, the elements of s from dst. It panics if the
// argument lengths do not match.
//
//gcassert:inline
func Sub(dst []float32, s []float32) {
	// blas32.Axpy is significantly faster than Go code on amd64.
	blas32.Axpy(
		-1,
		blas32.Vector{N: len(s), Data: s, Inc: 1},
		blas32.Vector{N: len(dst), Data: dst, Inc: 1})
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

// Scale multiplies every element in dst by the scalar c.
//
//gcassert:inline
func Scale(c float32, dst []float32) {
	// blas32.Scale is significantly faster than Go code on amd64.
	blas32.Scal(c, blas32.Vector{N: len(dst), Data: dst, Inc: 1})
}

func checkDims(v []float32, v2 []float32) {
	if len(v) != len(v2) {
		panic(errors.AssertionFailedf("different vector dimensions %d and %d", len(v), len(v2)))
	}
}
