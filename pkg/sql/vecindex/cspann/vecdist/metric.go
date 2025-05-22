// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecdist

import (
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// Metric specifies which distance function a quantizer should use.
type Metric int

const (
	// L2Squared specifies squared Euclidean distance between two vectors, defined
	// as the square of the L2 norm of their difference:
	//  ||vec1 - vec2||²
	//
	// This is equivalent to the dot product of the difference with itself:
	//  (vec1 - vec2) · (vec1 - vec2)
	L2Squared Metric = iota

	// InnerProduct specifies inner product distance between two vectors, defined
	// as the negative value of the dot product between them:
	//  -(vec1 · vec2)
	//
	// Negating the distance ensures that the smaller the distance, the more
	// similar are the vectors, aligning this metric with L2Squared and Cosine.
	InnerProduct

	// Cosine specifies the cosine distance between two vectors, defined as one
	// minus their cosine similarity:
	//  1 - cos(vec1, vec2)
	//
	// Cosine similarity is computed as the dot product of the vectors divided
	// by the product of their norms (magnitudes):
	//  cos(vec1, vec2) = (vec1 · vec2) / (||vec1|| * ||vec2||)
	//
	// However, when using Cosine distance, we always pre-normalize vectors, so
	// that they are always unit vectors. This means the product of their norms
	// is one. Therefore, cosine distance becomes simply:
	//  1 - (vec1 · vec2)
	Cosine
)

// Measure returns the distance between two vectors, according to the specified
// distance metric. For example, for the vectors [1,2] and [4,3]:
//
//	L2Squared   : (4-1)^2 + (3-2)^2 = 10
//	InnerProduct: -(1*4 + 2*3) = -10
//
// For the cosine case, the vectors are expected to already be normalized:
//
//	Cosine      : 1 - (1/sqrt(5) * 4/5 + 2/sqrt(5) * 3/5) = 0.1056
func Measure(metric Metric, vec1, vec2 vector.T) float32 {
	switch metric {
	case L2Squared:
		return num32.L2SquaredDistance(vec1, vec2)

	case InnerProduct:
		// Calculate the distance by negating the inner product, so that the more
		// similar the vectors, the lower the distance. Note that the inner product
		// "distance" can be negative.
		return -num32.Dot(vec1, vec2)

	case Cosine:
		// Both vectors are assumed to be normalized, so cosine similarity is equal
		// to the inner product, and cosine distance is 1 - cosine similarity.
		return 1 - num32.Dot(vec1, vec2)
	}

	panic(errors.AssertionFailedf("unknown distance function %d", metric))
}
