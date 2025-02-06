// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quantize

import (
	"slices"

	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

// GetCount implements the QuantizedVectorSet interface.
func (vs *UnQuantizedVectorSet) GetCount() int {
	return vs.Vectors.Count
}

// GetCentroid implements the QuantizedVectorSet interface.
func (vs *UnQuantizedVectorSet) GetCentroid() vector.T {
	return vs.Centroid
}

// GetCentroidDistances implements the QuantizedVectorSet interface.
func (vs *UnQuantizedVectorSet) GetCentroidDistances() []float32 {
	return vs.CentroidDistances
}

// Clone implements the QuantizedVectorSet interface.
func (vs *UnQuantizedVectorSet) Clone() QuantizedVectorSet {
	return &UnQuantizedVectorSet{
		Centroid:          slices.Clone(vs.Centroid),
		CentroidDistances: slices.Clone(vs.CentroidDistances),
		Vectors:           vs.Vectors.Clone(),
	}
}

// Clear implements the QuantizedVectorSet interface.
func (vs *UnQuantizedVectorSet) Clear(centroid vector.T) {
	copy(vs.Centroid, centroid)
	vs.CentroidDistances = vs.CentroidDistances[:0]
	vs.Vectors.Clear()
}

// ComputeSquaredDistances computes the exact squared distances between the
// given query vector and the vectors in the set, and writes the distances to
// the "squaredDistances" slice.
//
// The caller is responsible for allocating the "squaredDistances" slice with
// length equal to the number of vectors in this set.
func (vs *UnQuantizedVectorSet) ComputeSquaredDistances(
	queryVector vector.T, squaredDistances []float32,
) {
	for i := 0; i < vs.Vectors.Count; i++ {
		squaredDistances[i] = num32.L2SquaredDistance(queryVector, vs.Vectors.At(i))
	}
}

// AddSet adds the given set of vectors to this set.
func (vs *UnQuantizedVectorSet) AddSet(vectors vector.Set) {
	vs.Vectors.AddSet(vectors)

	// Compute the distance of each new vector to the set's centroid.
	prevCount := len(vs.CentroidDistances)
	vs.CentroidDistances = slices.Grow(vs.CentroidDistances, vectors.Count)
	vs.CentroidDistances = vs.CentroidDistances[:prevCount+vectors.Count]
	for i := 0; i < vectors.Count; i++ {
		vs.CentroidDistances[prevCount+i] = num32.L2Distance(vectors.At(i), vs.Centroid)
	}
}

// ReplaceWithLast implements the QuantizedVectorSet interface.
func (vs *UnQuantizedVectorSet) ReplaceWithLast(offset int) {
	lastOffset := vs.Vectors.Count - 1
	vs.Vectors.ReplaceWithLast(offset)
	vs.CentroidDistances[offset] = vs.CentroidDistances[lastOffset]
	vs.CentroidDistances = vs.CentroidDistances[:lastOffset]
}
