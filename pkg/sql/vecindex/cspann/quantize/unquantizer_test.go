// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quantize

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
)

// Basic tests.
func TestUnQuantizerSimple(t *testing.T) {
	var workspace workspace.T
	quantizer := NewUnQuantizer(2, vecpb.L2SquaredDistance)
	require.Equal(t, 2, quantizer.GetDims())

	// Quantize empty set.
	vectors := vector.MakeSet(2)
	quantizedSet := quantizer.Quantize(&workspace, vectors)
	require.Equal(t, 0, quantizedSet.GetCount())

	// Add 3 vectors and verify centroid and centroid distances.
	vectors = vector.MakeSetFromRawData([]float32{5, 2, 1, 2, 6, 5}, 2)
	quantizedSet = quantizer.Quantize(&workspace, vectors)

	// Add 2 more vectors to existing set.
	vectors = vector.MakeSetFromRawData([]float32{4, 3, 6, 5}, 2)
	quantizer.QuantizeInSet(&workspace, quantizedSet, vectors)
	require.Equal(t, 5, quantizedSet.GetCount())

	// Ensure distances and error bounds are correct.
	distances := make([]float32, quantizedSet.GetCount())
	errorBounds := make([]float32, quantizedSet.GetCount())
	quantizer.EstimateDistances(
		&workspace, quantizedSet, vector.T{1, 1}, distances, errorBounds)
	require.Equal(t, []float32{17, 1, 41, 13, 41}, distances)
	require.Equal(t, []float32{0, 0, 0, 0, 0}, errorBounds)

	// Query vector is centroid.
	quantizer.EstimateDistances(
		&workspace, quantizedSet, vector.T{0, 0}, distances, errorBounds)
	require.Equal(t, []float32{29, 5, 61, 25, 61}, distances, 2)
	require.Equal(t, []float32{0, 0, 0, 0, 0}, errorBounds, 2)

	// Remove quantized vectors.
	quantizedSet.ReplaceWithLast(1)
	quantizedSet.ReplaceWithLast(3)
	quantizedSet.ReplaceWithLast(1)
	require.Equal(t, 2, quantizedSet.GetCount())
	distances = distances[:2]
	errorBounds = errorBounds[:2]
	quantizer.EstimateDistances(
		&workspace, quantizedSet, vector.T{1, 1}, distances, errorBounds)
	require.Equal(t, []float32{17, 41}, distances)
	require.Equal(t, []float32{0, 0}, errorBounds)

	// Remove remaining quantized vectors.
	quantizedSet.ReplaceWithLast(0)
	quantizedSet.ReplaceWithLast(0)
	require.Equal(t, 0, quantizedSet.GetCount())
	distances = distances[:0]
	errorBounds = errorBounds[:0]
	quantizer.EstimateDistances(
		&workspace, quantizedSet, vector.T{1, 1}, distances, errorBounds)

	// Empty quantized set.
	vectors = vector.MakeSet(2)
	quantizedSet = quantizer.Quantize(&workspace, vectors)

	// Add single vector to quantized set.
	vectors = vector.T{4, 4}.AsSet()
	quantizer.QuantizeInSet(&workspace, quantizedSet, vectors)
	require.Equal(t, 1, quantizedSet.GetCount())
	distances = distances[:1]
	errorBounds = errorBounds[:1]
	quantizer.EstimateDistances(
		&workspace, quantizedSet, vector.T{1, 1}, distances, errorBounds)
	require.Equal(t, []float32{18}, distances, 2)
	require.Equal(t, []float32{0}, errorBounds, 2)

	// Test InnerProduct distance metric.
	quantizer = NewUnQuantizer(2, vecpb.InnerProductDistance)
	vectors = vector.MakeSetFromRawData([]float32{5, 2, 1, 2, 6, 5}, 2)
	quantizedSet = quantizer.Quantize(&workspace, vectors)

	distances = distances[:3]
	errorBounds = errorBounds[:3]
	quantizer.EstimateDistances(
		&workspace, quantizedSet, vector.T{3, 2}, distances, errorBounds)
	require.Equal(t, []float32{-19, -7, -28}, distances)
	require.Equal(t, []float32{0, 0, 0}, errorBounds)

	// Test Cosine distance metric.
	quantizer = NewUnQuantizer(2, vecpb.CosineDistance)
	vectors = vector.MakeSetFromRawData([]float32{-1, 0, 0, 1, 0.70710678, 0.70710678}, 2)
	quantizedSet = quantizer.Quantize(&workspace, vectors)

	distances = distances[:3]
	errorBounds = errorBounds[:3]
	quantizer.EstimateDistances(
		&workspace, quantizedSet, vector.T{1, 0}, distances, errorBounds)
	require.Equal(t, []float32{2, 1, 0.29289323}, distances)
	require.Equal(t, []float32{0, 0, 0}, errorBounds)
}
