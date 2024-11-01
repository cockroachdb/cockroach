// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quantize

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// unQuantizer trivially implements the Quantizer interface, storing the
// original full-size vectors in unmodified form.
//
// All methods in unQuantizer are thread-safe.
type unQuantizer struct {
	dims int
}

var _ Quantizer = (*unQuantizer)(nil)

// NewUnQuantizer returns a new instance of the UnQuantizer that stores vectors
// with the given number of dimensions.
func NewUnQuantizer(dims int) Quantizer {
	return &unQuantizer{dims: dims}
}

// GetOriginalDims implements the Quantizer interface.
func (q *unQuantizer) GetOriginalDims() int {
	return q.dims
}

// GetRandomDims implements the Quantizer interface.
func (q *unQuantizer) GetRandomDims() int {
	return q.dims
}

// RandomizeVector implements the Quantizer interface.
func (q *unQuantizer) RandomizeVector(
	ctx context.Context, original vector.T, randomized vector.T, invert bool,
) {
	if len(original) != q.dims {
		panic(errors.AssertionFailedf(
			"original dimensions %d do not match quantizer dims %d", len(original), q.dims))
	}
	if len(randomized) != q.dims {
		panic(errors.AssertionFailedf(
			"randomized dimensions %d do not match quantizer dims %d", len(original), q.dims))
	}
	copy(randomized, original)
}

// Quantize implements the Quantizer interface.
func (q *unQuantizer) Quantize(ctx context.Context, vectors *vector.Set) QuantizedVectorSet {
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
func (q *unQuantizer) QuantizeInSet(
	ctx context.Context, quantizedSet QuantizedVectorSet, vectors *vector.Set,
) {
	unquantizedSet := quantizedSet.(*UnQuantizedVectorSet)
	unquantizedSet.AddSet(vectors)
}

// EstimateSquaredDistances implements the Quantizer interface.
func (q *unQuantizer) EstimateSquaredDistances(
	ctx context.Context,
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
