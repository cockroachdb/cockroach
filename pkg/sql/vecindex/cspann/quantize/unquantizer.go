// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quantize

import (
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/vecdist"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
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
	if buildutil.CrdbTestBuild && q.distanceMetric == vecdist.Cosine {
		validateUnitVectors(vectors)
	}

	unquantizedSet := &UnQuantizedVectorSet{
		Vectors: vector.MakeSet(q.dims),
	}
	unquantizedSet.AddSet(vectors)
	return unquantizedSet
}

// QuantizeInSet implements the Quantizer interface.
func (q *UnQuantizer) QuantizeInSet(
	w *workspace.T, quantizedSet QuantizedVectorSet, vectors vector.Set,
) {
	if buildutil.CrdbTestBuild && q.distanceMetric == vecdist.Cosine {
		validateUnitVectors(vectors)
	}

	unquantizedSet := quantizedSet.(*UnQuantizedVectorSet)
	unquantizedSet.AddSet(vectors)
}

// NewQuantizedVectorSet implements the Quantizer interface
func (q *UnQuantizer) NewQuantizedVectorSet(capacity int, centroid vector.T) QuantizedVectorSet {
	if buildutil.CrdbTestBuild && q.distanceMetric == vecdist.Cosine {
		validateUnitVector(centroid)
	}

	dataBuffer := make([]float32, 0, capacity*q.GetDims())
	unquantizedSet := &UnQuantizedVectorSet{
		Vectors: vector.MakeSetFromRawData(dataBuffer, q.GetDims()),
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
	if buildutil.CrdbTestBuild && q.distanceMetric == vecdist.Cosine {
		validateUnitVector(queryVector)
	}

	unquantizedSet := quantizedSet.(*UnQuantizedVectorSet)

	for i := range unquantizedSet.Vectors.Count {
		dataVector := unquantizedSet.Vectors.At(i)
		distances[i] = vecdist.Measure(q.distanceMetric, queryVector, dataVector)
	}

	// Distances are exact, so error bounds are always zero.
	num32.Zero(errorBounds)
}
