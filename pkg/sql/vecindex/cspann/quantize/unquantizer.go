// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quantize

import (
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/vecdist"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

// UnQuantizer trivially implements the Quantizer interface, storing the
// original full-size vectors in unmodified form.
//
// All methods in UnQuantizer are thread-safe.
type UnQuantizer struct {
	// dims is the dimensionality of vectors expected by the UnQuantizer.
	dims int
	// distanceMetric determines which distance function to use.
	distanceMetric vecdist.Metric
}

var _ Quantizer = (*UnQuantizer)(nil)

// NewUnQuantizer returns a new instance of the UnQuantizer that stores vectors
// with the given number of dimensions and distance metric.
func NewUnQuantizer(dims int, distanceMetric vecdist.Metric) Quantizer {
	return &UnQuantizer{dims: dims, distanceMetric: distanceMetric}
}

// GetDims implements the Quantizer interface.
func (q *UnQuantizer) GetDims() int {
	return q.dims
}

// GetDistanceMetric implements the Quantizer interface.
func (q *UnQuantizer) GetDistanceMetric() vecdist.Metric {
	return q.distanceMetric
}

// Quantize implements the Quantizer interface.
func (q *UnQuantizer) Quantize(w *workspace.T, vectors vector.Set) QuantizedVectorSet {
	unquantizedSet := &UnQuantizedVectorSet{
		Centroid: make(vector.T, q.dims),
		Vectors:  vector.MakeSet(q.dims),
	}
	if vectors.Count != 0 {
		vectors.Centroid(unquantizedSet.Centroid)
		unquantizedSet.AddSet(vectors)
	}
	return unquantizedSet
}

// QuantizeInSet implements the Quantizer interface.
func (q *UnQuantizer) QuantizeInSet(
	w *workspace.T, quantizedSet QuantizedVectorSet, vectors vector.Set,
) {
	unquantizedSet := quantizedSet.(*UnQuantizedVectorSet)
	unquantizedSet.AddSet(vectors)
}

// NewQuantizedVectorSet implements the Quantizer interface
func (q *UnQuantizer) NewQuantizedVectorSet(capacity int, centroid vector.T) QuantizedVectorSet {
	dataBuffer := make([]float32, 0, capacity*q.GetDims())
	unquantizedSet := &UnQuantizedVectorSet{
		Centroid: centroid,
		Vectors:  vector.MakeSetFromRawData(dataBuffer, q.GetDims()),
	}
	return unquantizedSet
}

// EstimateDistances implements the Quantizer interface.
func (q *UnQuantizer) EstimateDistances(
	w *workspace.T,
	quantizedSet QuantizedVectorSet,
	queryVector vector.T,
	distances []float32,
	errorBounds []float32,
) {
	unquantizedSet := quantizedSet.(*UnQuantizedVectorSet)

	for i := 0; i < unquantizedSet.Vectors.Count; i++ {
		dataVector := unquantizedSet.Vectors.At(i)
		switch q.distanceMetric {
		case vecdist.L2Squared:
			distances[i] = num32.L2SquaredDistance(queryVector, dataVector)

		case vecdist.InnerProduct:
			// Negate inner product to get distance (i.e. the lower the distance,
			// the more similar the vectors).
			distances[i] = -num32.Dot(queryVector, dataVector)

		case vecdist.Cosine:
			// Compute cosine distance as 1 - cosine similarity. The caller is
			// expected to have normalized the query and data vectors, so cosine
			// similarity is just the inner product.
			distances[i] = 1 - num32.Dot(queryVector, dataVector)
		}
	}

	// Distances are exact, so error bounds are always zero.
	num32.Zero(errorBounds)
}
