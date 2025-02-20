// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/floats/scalar"
)

func TestPartition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	childKey10 := ChildKey{PartitionKey: 10}
	childKey20 := ChildKey{PartitionKey: 20}
	childKey30 := ChildKey{PartitionKey: 30}
	childKey40 := ChildKey{PartitionKey: 40}
	childKey50 := ChildKey{PartitionKey: 50}

	valueBytes10 := ValueBytes{1, 2}
	valueBytes20 := ValueBytes{3, 4}
	valueBytes30 := ValueBytes{5, 6}
	valueBytes40 := ValueBytes{7, 8}
	valueBytes50 := ValueBytes{9, 10}
	valueBytes20b := ValueBytes{11, 12}

	// Create new partition and add 4 vectors.
	var workspace workspace.T
	quantizer := quantize.NewUnQuantizer(2)
	vectors := vector.MakeSetFromRawData([]float32{1, 2, 5, 2, 6, 6}, 2)
	quantizedSet := quantizer.Quantize(&workspace, vectors)
	childKeys := []ChildKey{childKey10, childKey20, childKey30}
	valueBytes := []ValueBytes{valueBytes10, valueBytes20, valueBytes30, valueBytes40}
	partition := NewPartition(quantizer, quantizedSet, childKeys, valueBytes, Level(1))
	require.True(t, partition.Add(&workspace, vector.T{4, 3}, childKey40, valueBytes40))

	// Add vector and expect its value to be updated.
	require.False(t, partition.Add(&workspace, vector.T{10, 10}, childKey20, valueBytes20b))

	require.Equal(t, 4, partition.Count())
	require.Equal(t, []ChildKey{childKey10, childKey40, childKey30, childKey20}, partition.ChildKeys())
	require.Equal(t, []ValueBytes{valueBytes10, valueBytes40, valueBytes30, valueBytes20b}, partition.ValueBytes())
	require.Equal(t, []float32{4, 3.33}, testutils.RoundFloats(partition.Centroid(), 2))
	checkPartitionMetadata(t, partition.Metadata(), Level(1), vector.T{4, 3.33}, 4)

	// Ensure that cloning does not disturb anything.
	cloned := partition.Clone()
	cloned.Add(&workspace, vector.T{0, -1}, childKey50, valueBytes50)

	// Search method.
	searchSet := SearchSet{MaxResults: 3}
	level, count := partition.Search(&workspace, RootKey, vector.T{1, 1}, &searchSet)
	require.Equal(t, Level(1), level)
	require.Equal(t, 4, count)
	result1 := SearchResult{
		QuerySquaredDistance: 1, ErrorBound: 0, CentroidDistance: 3.2830, ParentPartitionKey: 1, ChildKey: childKey10, ValueBytes: valueBytes10}
	result2 := SearchResult{
		QuerySquaredDistance: 13, ErrorBound: 0, CentroidDistance: 0.3333, ParentPartitionKey: 1, ChildKey: childKey40, ValueBytes: valueBytes40}
	result3 := SearchResult{
		QuerySquaredDistance: 50, ErrorBound: 0, CentroidDistance: 3.3333, ParentPartitionKey: 1, ChildKey: childKey30, ValueBytes: valueBytes30}
	results := roundResults(searchSet.PopResults(), 4)
	require.Equal(t, SearchResults{result1, result2, result3}, results)

	// Find method.
	require.Equal(t, 2, partition.Find(childKey30))
	require.Equal(t, 1, partition.Find(childKey40))
	require.Equal(t, 3, partition.Find(childKey20))
	require.Equal(t, -1, partition.Find(ChildKey{KeyBytes: []byte{1, 2}}))

	// Remove vectors.
	require.True(t, partition.ReplaceWithLastByKey(childKey20))
	require.Equal(t, []ChildKey{childKey10, childKey40, childKey30}, partition.ChildKeys())
	require.False(t, partition.ReplaceWithLastByKey(childKey20))
	require.True(t, partition.ReplaceWithLastByKey(childKey30))
	require.Equal(t, []ChildKey{childKey10, childKey40}, partition.ChildKeys())
	require.True(t, partition.ReplaceWithLastByKey(childKey10))
	require.Equal(t, []ChildKey{childKey40}, partition.ChildKeys())
	require.True(t, partition.ReplaceWithLastByKey(childKey40))
	require.Equal(t, []ChildKey{}, partition.ChildKeys())

	// Check that clone is unaffected.
	require.Equal(t, 5, cloned.Count())
	require.Equal(t, Level(1), cloned.Level())
	require.Equal(t, []ChildKey{childKey10, childKey40, childKey30, childKey20, childKey50}, cloned.ChildKeys())
	require.Equal(t, []ValueBytes{valueBytes10, valueBytes40, valueBytes30, valueBytes20b, valueBytes50}, cloned.ValueBytes())
	squaredDistances := []float32{0, 0, 0, 0, 0}
	errorBounds := []float32{0, 0, 0, 0, 0}
	cloned.Quantizer().EstimateSquaredDistances(
		&workspace, cloned.QuantizedSet(), vector.T{3, 4}, squaredDistances, errorBounds)
	require.Equal(t, []float32{8, 2, 13, 85, 34}, squaredDistances)
	checkPartitionMetadata(t, cloned.Metadata(), Level(1), vector.T{4, 3.33}, 5)
}

func roundResults(results SearchResults, prec int) SearchResults {
	for i := range results {
		result := &results[i]
		result.QuerySquaredDistance = float32(scalar.Round(float64(result.QuerySquaredDistance), prec))
		result.ErrorBound = float32(scalar.Round(float64(result.ErrorBound), prec))
		result.CentroidDistance = float32(scalar.Round(float64(result.CentroidDistance), prec))
		result.Vector = testutils.RoundFloats(result.Vector, prec)
	}
	return results
}

func checkPartitionMetadata(
	t *testing.T, metadata PartitionMetadata, level Level, centroid vector.T, count int,
) {
	require.Equal(t, level, metadata.Level)
	require.Equal(t, []float32(centroid), testutils.RoundFloats(metadata.Centroid, 2))
	require.Equal(t, count, metadata.Count)
}
