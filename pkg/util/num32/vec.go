// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package num32

import (
	"math"

	"github.com/cockroachdb/errors"
)

// L1Distance returns the L1 norm of s - t, which is the Manhattan distance
// between the two vectors.
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
// expensive square-root operation.
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
// product.
func InnerProduct(s []float32, t []float32) float32 {
	checkDims(s, t)
	var distance float32
	for i := range s {
		distance += s[i] * t[i]
	}
	return distance
}

func checkDims(v []float32, v2 []float32) {
	if len(v) != len(v2) {
		panic(errors.AssertionFailedf("different vector dimensions %d and %d", len(v), len(v2)))
	}
}
