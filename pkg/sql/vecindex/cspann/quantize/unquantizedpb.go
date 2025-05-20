// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quantize

import "github.com/cockroachdb/cockroach/pkg/util/vector"

// GetCount implements the QuantizedVectorSet interface.
func (vs *UnQuantizedVectorSet) GetCount() int {
	return vs.Vectors.Count
}

// Clone implements the QuantizedVectorSet interface.
func (vs *UnQuantizedVectorSet) Clone() QuantizedVectorSet {
	return &UnQuantizedVectorSet{
		Vectors: vs.Vectors.Clone(),
	}
}

// Clear implements the QuantizedVectorSet interface.
func (vs *UnQuantizedVectorSet) Clear(centroid vector.T) {
	vs.Vectors.Clear()
}

// AddSet adds the given set of vectors to this set.
func (vs *UnQuantizedVectorSet) AddSet(vectors vector.Set) {
	vs.Vectors.AddSet(vectors)
}

// ReplaceWithLast implements the QuantizedVectorSet interface.
func (vs *UnQuantizedVectorSet) ReplaceWithLast(offset int) {
	vs.Vectors.ReplaceWithLast(offset)
}
