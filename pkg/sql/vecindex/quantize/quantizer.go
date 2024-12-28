// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quantize

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

// Quantizer compresses a set of full-size vectors to an equally-sized set of
// representative quantized vectors. Each quantized vector is a fraction of the
// size of the original full-size vector that it represents. While quantization
// loses information about the original vector, the quantized form can still be
// used to estimate the distance between the original vector and a user-provided
// query vector.
//
// Original, full-size vectors should first be randomized via a call to
// RandomizeVector and then quantized via a call to Quantize or QuantizeInSet.
//
// Quantizer implementations must be thread-safe. There should typically be only
// one Quantizer instance in the process for each index.
type Quantizer interface {
	// GetOriginalDims specifies the number of dimensions of the original
	// full-size vectors that will be quantized.
	GetOriginalDims() int

	// GetRandomDims specifies the number of dimensions of the randomized
	// vectors produced by RandomizeVector. This may be different from what
	// GetOriginalDims returns.
	GetRandomDims() int

	// RandomizeVector optionally performs a random orthogonal transformation
	// (ROT) on the input vector and writes it to the output vector. The caller
	// is responsible for allocating the output vector with length equal to
	// GetRandomDims(). If invert is true, then a previous ROT is reversed in
	// order to recover the original vector. The caller is responsible for
	// allocating the output vector with length equal to GetOriginalDims().
	//
	// Randomizing vectors distributes skew more evenly across dimensions and
	// across vectors in a set. Distance and angle between any two vectors
	// remains unchanged, as long as the same ROT is applied to both. Note that
	// the randomized vector may have a different number of dimensions than the
	// original vector.
	//
	// NOTE: This step may be a no-op for some quantization algorithms, which
	// may simply copy the original slice to the randomized slice, unchanged.
	RandomizeVector(ctx context.Context, input vector.T, output vector.T, invert bool)

	// Quantize quantizes a set of input vectors and returns their compressed
	// form as a quantized vector set. Input vectors should already have been
	// randomized.
	//
	// NOTE: The caller must ensure that a Workspace is attached to the context.
	Quantize(ctx context.Context, vectors *vector.Set) QuantizedVectorSet

	// QuantizeInSet quantizes a set of input vectors and adds their compressed
	// form to an existing quantized vector set. Input vectors should already
	// have been randomized.
	//
	// NOTE: The caller must ensure that a Workspace is attached to the context.
	QuantizeInSet(ctx context.Context, quantizedSet QuantizedVectorSet, vectors *vector.Set)

	// NewQuantizedVectorSet returns a new empty vector set preallocated to the
	// number of vectors specified.
	NewQuantizedVectorSet(capacity int, centroid vector.T) QuantizedVectorSet

	// EstimateSquaredDistances returns the estimated squared distances of the
	// query vector from each data vector represented in the given quantized
	// vector set, as well as the error bounds for those distances.
	//
	// The caller is responsible for allocating the "squaredDistances" and
	// "errorBounds" slices with length equal to the number of quantized vectors
	// in "quantizedSet". EstimateSquaredDistances will update the slices with
	// distances and distance error bounds.
	//
	// NOTE: The caller must ensure that a Workspace is attached to the context.
	EstimateSquaredDistances(
		ctx context.Context,
		quantizedSet QuantizedVectorSet,
		queryVector vector.T,
		squaredDistances []float32,
		errorBounds []float32,
	)
}

// QuantizedVectorSet is the compressed form of an original set of full-size
// vectors. It also stores a full-size centroid vector for the set, as well as
// the exact distances of the original full-size vectors from that centroid.
type QuantizedVectorSet interface {
	// GetCount returns the number of quantized vectors in the set.
	GetCount() int

	// GetCentroid returns the full-size centroid vector for the set. The
	// centroid is the average of the randomized full-size vectors, across all
	// dimensions.
	// NOTE: This centroid is calculated once, when the set is first created. It
	// is not updated when quantized vectors are added to or removed from the set.
	// Since it is immutable, this method is thread-safe.
	GetCentroid() vector.T

	// GetCentroidDistances returns the exact distances of each full-size vector
	// from the centroid.
	GetCentroidDistances() []float32

	// ReplaceWithLast removes the quantized vector at the given offset from the
	// set, replacing it with the last quantized vector in the set. The modified
	// set has one less element and the last quantized vector's position changes.
	ReplaceWithLast(offset int)

	// Clone makes a deep copy of this quantized vector set. Changes to either
	// the original or clone will not affect the other.
	Clone() QuantizedVectorSet

	// Clear removes all the elements of the vector set so that it may be reused. The
	// new centroid is copied over the existing centroid.
	Clear(centroid vector.T)
}
