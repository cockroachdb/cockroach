// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quantize

import (
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

// Quantizer compresses a set of full-size vectors to an equally-sized set of
// representative quantized vectors. Each quantized vector is a fraction of the
// size of the original full-size vector that it represents. While quantization
// loses information about the original vector, the quantized form can still be
// used to estimate the distance between the original vector and a user-provided
// query vector.
//
// Quantizer implementations must be thread-safe. There should typically be only
// one Quantizer instance in the process for each index.
type Quantizer interface {
	// GetDims specifies the number of dimensions of the vectors that will be
	// quantized.
	GetDims() int

	// Quantize quantizes a set of input vectors and returns their compressed
	// form as a quantized vector set. The set's centroid is calculated from the
	// input vectors.
	Quantize(w *workspace.T, vectors vector.Set) QuantizedVectorSet

	// QuantizeInSet quantizes a set of input vectors and adds their compressed
	// form to an existing quantized vector set.
	//
	// NOTE: The set's centroid is not recalculated to reflect the newly added
	//       vectors.
	QuantizeInSet(w *workspace.T, quantizedSet QuantizedVectorSet, vectors vector.Set)

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
	EstimateSquaredDistances(
		w *workspace.T,
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
	// centroid is the average of the vectors across all dimensions.
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

	// Clear removes all the elements of the vector set so that it may be reused.
	// The new centroid is copied over the existing centroid.
	Clear(centroid vector.T)
}
