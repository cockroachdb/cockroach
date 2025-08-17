// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/utils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

// queryVector manages a query vector, applying randomization and normalization
// as needed, and efficiently calculating exact distances to data vectors.
// Randomization distributes skew more evenly across dimensions, enabling the
// index to work consistently across diverse data sets. Normalization is applied
// when using Cosine distance, which is magnitude-agnostic.
type queryVector struct {
	// original is the original query vector passed to the top-level Index method.
	original vector.T
	// transformed is the query vector after random orthogonal transformation and
	// normalization (for Cosine distance).
	transformed vector.T
	// memOwned is true if the memory used to store the transformed vector was
	// allocated by this class. If true, the memory can be reused.
	memOwned bool
}

// Clear initializes the vector to the empty state, for possible reuse.
func (c *queryVector) Clear() {
	c.original = nil
	if !c.memOwned {
		c.transformed = nil
	} else {
		// Reuse transformed memory.
		if buildutil.CrdbTestBuild {
			// Write non-zero values to cleared memory.
			for i := range c.transformed {
				c.transformed[i] = 0xFF
			}
		}
		c.transformed = c.transformed[:0]
	}
}

// InitOriginal stores the original query vector as well as its randomized and
// possibly normalized form.
func (c *queryVector) InitOriginal(
	distanceMetric vecpb.DistanceMetric,
	original vector.T,
	rot *RandomOrthoTransformer,
) {
	c.original = original
	c.allocTransformed(original)

	// Randomize the original query vector.
	c.transformed = rot.RandomizeVector(original, c.transformed)

	// If using cosine distance, also normalize the query vector.
	if distanceMetric == vecpb.CosineDistance {
		num32.Normalize(c.transformed)
	}
}

// InitTransformed stores an already transformed query vector. This is used for
// internal vectors that were previously transformed by the index.
//
// NOTE: The original vector is set to nil. It should never be needed.
func (c *queryVector) InitTransformed(transformed vector.T) {
	c.original = nil
	c.transformed = transformed
	c.memOwned = false
}

// InitCentroid stores a transformed centroid vector. Centroids have already
// been randomized, but they need to be converted into spherical centroids (i.e.
// normalized) when using the InnerProduct or Cosine distance metric.
//
// NOTE: The original vector is set to nil. It should never be needed.
func (c *queryVector) InitCentroid(distanceMetric vecpb.DistanceMetric, centroid vector.T) {
	c.original = nil
	c.allocTransformed(centroid)
	copy(c.transformed, centroid)

	// Compute spherical centroid for Cosine and InnerProduct.
	switch distanceMetric {
	case vecpb.CosineDistance, vecpb.InnerProductDistance:
		num32.Normalize(c.transformed)
	}
}

// Transformed returns the query vector after it has been randomized and
// normalized as needed.
func (c *queryVector) Transformed() vector.T {
	return c.transformed
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
func (c *queryVector) ComputeExactDistances(
	distanceMetric vecpb.DistanceMetric, level Level, candidates []SearchResult,
) {
	normalize := false
	queryVector := c.transformed
	queryNorm := float32(1)
	if level == LeafLevel {
		// Leaf vectors have not been randomized, so compare with the original
		// vector rather than the randomized vector.
		queryVector = c.original

		// If using Cosine distance, then ensure that data vectors are normalized.
		// Also, normalize the original query vector.
		if distanceMetric == vecpb.CosineDistance {
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
		switch distanceMetric {
		case vecpb.CosineDistance, vecpb.InnerProductDistance:
			normalize = true
		}
	}

	for i := range candidates {
		candidate := &candidates[i]
		if normalize {
			// Compute inner product distance and perform needed normalization.
			candidate.QueryDistance = vecpb.MeasureDistance(
				vecpb.InnerProductDistance, candidate.Vector, queryVector)
			product := queryNorm * num32.Norm(candidate.Vector)
			if product != 0 {
				candidate.QueryDistance /= product
			}
			if distanceMetric == vecpb.CosineDistance {
				// Cosine distance for normalized vectors is 1 - (query ⋅ data).
				// We've computed the negative inner product, so just add one.
				candidate.QueryDistance++
			}
		} else {
			candidate.QueryDistance = vecpb.MeasureDistance(distanceMetric, candidate.Vector, queryVector)
		}
		candidate.ErrorBound = 0
	}
}

func (c *queryVector) allocTransformed(transformed vector.T) {
	if !c.memOwned {
		c.transformed = nil
	}
	c.transformed = utils.EnsureSliceLen(c.transformed, len(transformed))
	c.memOwned = true
}
