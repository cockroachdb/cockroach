// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

// splitData contains information about a smaller partition that is splitting
// from an over-sized partition.
type splitData struct {
	// Partition contains a subset of the quantized vectors and child keys from
	// the splitting partition.
	Partition *Partition
	// Vectors is the subset of full-size vectors from the splitting partition.
	// The vectors are in randomized format.
	Vectors vector.Set
	// OldCentroidDistances are the exact distances from each vector to the
	// centroid of the splitting partition.
	OldCentroidDistances []float32
}

// Init initializes the split information by creating a new partition from the
// given subset of vectors from the splitting partition.
func (s *splitData) Init(
	w *workspace.T,
	quantizer quantize.Quantizer,
	vectors vector.Set,
	oldCentroidDistances []float32,
	childKeys []ChildKey,
	valueBytes []ValueBytes,
	level Level,
) {
	s.Vectors = vectors
	s.OldCentroidDistances = oldCentroidDistances
	quantizedSet := quantizer.Quantize(w, s.Vectors)
	s.Partition = NewPartition(quantizer, quantizedSet, childKeys, valueBytes, level)
}

// ReplaceWithLast removes the vector at the given offset in the set, replacing
// it with the last vector in the set. The modified set has one less element and
// the last vector's position changes.
func (s *splitData) ReplaceWithLast(offset int) {
	s.Vectors.ReplaceWithLast(offset)
	s.OldCentroidDistances[offset] = s.OldCentroidDistances[len(s.OldCentroidDistances)-1]
	s.OldCentroidDistances = s.OldCentroidDistances[:len(s.OldCentroidDistances)-1]
	s.Partition.ReplaceWithLast(offset)
}
