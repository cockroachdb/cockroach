// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/utils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

// queryComparer manages a query vector, applying randomization and normalization
// as needed, and efficiently calculating exact distances to data vectors.
// Randomization distributes skew more evenly across dimensions, enabling the
// index to work consistently across diverse data sets. Normalization is applied
// when using Cosine distance, which is magnitude-agnostic.
type queryComparer struct {
	// distanceMetric specifies the vector similarity function: L2Squared,
	// InnerProduct, or Cosine.
	distanceMetric vecpb.DistanceMetric
	// original is the original query vector passed to the top-level Index method.
	original vector.T
	// randomized is the query vector after random orthogonal transformation and
	// normalization (for Cosine distance).
	randomized vector.T
}

// InitOriginal sets the original query vector and prepares the comparer for
// use.
func (c *queryComparer) InitOriginal(
	distanceMetric vecpb.DistanceMetric, original vector.T, rot *RandomOrthoTransformer,
) {
	c.distanceMetric = distanceMetric
	c.original = original

	// Randomize the original query vector.
	c.randomized = utils.EnsureSliceLen(c.randomized, len(original))
	c.randomized = rot.RandomizeVector(original, c.randomized)

	// If using cosine distance, also normalize the query vector.
	if c.distanceMetric == vecpb.CosineDistance {
		num32.Normalize(c.randomized)
	}
}

// InitRandomized sets the transformed query vector in cases where the original
// query vector is not available, such as when the vector is an interior
// partition centroid. It is expected to already be randomized and normalized.
func (c *queryComparer) InitTransformed(
	distanceMetric vecpb.DistanceMetric, randomized vector.T, rot *RandomOrthoTransformer,
) {
	c.distanceMetric = distanceMetric
	c.original = nil
	c.randomized = randomized
}

// Randomized returns the query vector after it has been randomized and
// normalized as needed.
func (c *queryComparer) Randomized() vector.T {
	return c.randomized
}

// ComputeExactDistances calculates exact distances between the query vector and
// the given search candidates using the configured distance metric. The method
// modifies the candidates slice in-place, setting QueryDistance to the computed
// distance and ErrorBound to 0 (since these are exact calculations). The level
// parameter affects distance computation for certain metrics: InnerProduct
// normalizes vectors only in interior (non-leaf) levels, Cosine applies
// normalization unconditionally to all levels, and L2Squared never applies
// normalization.
//
// NOTE: The Vector field must be populated in each candidate before calling
// this method.
func (c *queryComparer) ComputeExactDistances(level Level, candidates []SearchResult) {
	normalize := false
	queryVector := c.randomized
	queryNorm := float32(1)
	if level == LeafLevel {
		// Leaf vectors have not been randomized, so compare with the original
		// vector rather than the randomized vector.
		queryVector = c.original

		// If using Cosine distance, then ensure that data vectors are normalized.
		// Also, normalize the original query vector.
		if c.distanceMetric == vecpb.CosineDistance {
			normalize = true
			queryNorm = num32.Norm(queryVector)
		}
	} else {
		// Interior centroids are already randomized, so compare with the randomized
		// (and normalized) query vector. If using Cosine or InnerProduct distance,
		// then the centroids need to be normalized.
		// NOTE: For InnerProduct, only the data vectors are normalized; the query
		// vector is not normalized (queryNorm = 1). For Cosine, the randomized
		// query vector has already been normalized.
		switch c.distanceMetric {
		case vecpb.CosineDistance, vecpb.InnerProductDistance:
			normalize = true
		}
	}

	for i := range candidates {
		candidate := &candidates[i]
		if normalize {
			// Compute inner product distance and perform needed normalization.
			candidate.QueryDistance = vecpb.MeasureDistance(vecpb.InnerProductDistance, candidate.Vector, queryVector)
			product := queryNorm * num32.Norm(candidate.Vector)
			if product != 0 {
				candidate.QueryDistance /= product
			}
			if c.distanceMetric == vecpb.CosineDistance {
				// Cosine distance for normalized vectors is 1 - (query ⋅ data).
				// We've computed the negative inner product, so just add one.
				candidate.QueryDistance++
			}
		} else {
			candidate.QueryDistance = vecpb.MeasureDistance(c.distanceMetric, candidate.Vector, queryVector)
		}
		candidate.ErrorBound = 0
	}
}
