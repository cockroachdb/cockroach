// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecpb

import (
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// MeasureDistance returns the distance between two vectors, according to the
// specified distance metric. For example, for the vectors [1,2] and [4,3]:
//
//	L2Squared   : (4-1)^2 + (3-2)^2 = 10
//	InnerProduct: -(1*4 + 2*3) = -10
//
// For the cosine case, the vectors are expected to already be normalized:
//
//	Cosine      : 1 - (1/sqrt(5) * 4/5 + 2/sqrt(5) * 3/5) = 0.1056
//
// NOTE: If the vectors are not normalized, then Cosine will return undefined
// results.
func MeasureDistance(metric DistanceMetric, vec1, vec2 vector.T) float32 {
	switch metric {
	case L2SquaredDistance:
		return num32.L2SquaredDistance(vec1, vec2)

	case InnerProductDistance:
		// Calculate the distance by negating the inner product, so that the more
		// similar the vectors, the lower the distance. Note that the inner product
		// "distance" can be negative.
		return -num32.Dot(vec1, vec2)

	case CosineDistance:
		// Both vectors are assumed to be normalized, so cosine similarity is equal
		// to the inner product, and cosine distance is 1 - cosine similarity.
		return 1 - num32.Dot(vec1, vec2)
	}

	panic(errors.AssertionFailedf("unknown distance function %d", metric))
}
