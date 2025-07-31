// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quantize

import (
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

// UnQuantizer trivially implements the Quantizer interface, storing the
// original full-size vectors in unmodified form.
//
// All methods in UnQuantizer are thread-safe.
type UnQuantizer struct {
	dims int
}

var _ Quantizer = (*UnQuantizer)(nil)

// NewUnQuantizer returns a new instance of the UnQuantizer that stores vectors
// with the given number of dimensions.
func NewUnQuantizer(dims int) Quantizer {
	return &UnQuantizer{dims: dims}
}

// GetDims implements the Quantizer interface.
func (q *UnQuantizer) GetDims() int {
	return q.dims
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

// EstimateSquaredDistances implements the Quantizer interface.
func (q *UnQuantizer) EstimateSquaredDistances(
	w *workspace.T,
	quantizedSet QuantizedVectorSet,
	queryVector vector.T,
	squaredDistances []float32,
	errorBounds []float32,
) {
	// Distances are exact, so error bounds are always zero.
	unquantizedSet := quantizedSet.(*UnQuantizedVectorSet)
	unquantizedSet.ComputeSquaredDistances(queryVector, squaredDistances)
	num32.Zero(errorBounds)
}
