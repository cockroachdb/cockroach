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
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/floats/scalar"
)

func TestPartition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	vec10 := vector.T{1, 2}
	vec20 := vector.T{5, 2}
	vec30 := vector.T{6, 6}
	vec40 := vector.T{4, 3}
	vec50 := vector.T{8, 10}
	vec20b := vector.T{-1, 2}

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

	var workspace workspace.T
	quantizer := quantize.NewUnQuantizer(2, vecpb.L2SquaredDistance)

	// newTestPartition creates a partition with 2 vectors.
	newTestPartition := func() *Partition {
		vectors := vector.MakeSet(2)
		vectors.Add(vec10)
		vectors.Add(vec20)
		vectors.Add(vec30)
		quantizedSet := quantizer.Quantize(&workspace, vectors)
		childKeys := []ChildKey{childKey10, childKey20, childKey30}
		valueBytes := []ValueBytes{valueBytes10, valueBytes20, valueBytes30}
		metadata := MakeReadyPartitionMetadata(1, vectors.Centroid(make(vector.T, vectors.Dims)))
		return NewPartition(metadata, quantizer, quantizedSet, childKeys, valueBytes)
	}

	validateVectors := func(partition *Partition, expected []vector.T) {
		vectors := partition.quantizedSet.(*quantize.UnQuantizedVectorSet).Vectors
		require.Equal(t, len(expected), vectors.Count)
		for i := range vectors.Count {
			require.Equal(t, expected[i], vectors.At(i))
		}
	}

	t.Run("test Init", func(t *testing.T) {
		// Validate that Init sets same values.
		partition := newTestPartition()
		var partition2 Partition
		partition2.Init(
			*partition.Metadata(),
			partition.Quantizer(),
			partition.QuantizedSet(),
			partition.ChildKeys(),
			partition.ValueBytes(),
		)
		require.Equal(t, *partition, partition2)
	})

	t.Run("test Clone", func(t *testing.T) {
		partition := newTestPartition()
		cloned := partition.Clone()
		require.Equal(t, partition, cloned)

		// Update clone and make sure it is not reflected in the partition.
		cloned = partition.Clone()
		cloned.childKeys[0] = childKey40
		require.NotEqual(t, partition, cloned)

		cloned = partition.Clone()
		cloned.valueBytes[0] = valueBytes40
		require.NotEqual(t, partition, cloned)

		cloned = partition.Clone()
		cloned.quantizedSet.(*quantize.UnQuantizedVectorSet).Vectors.At(0)[0] = 99
		require.NotEqual(t, partition, cloned)
	})

	t.Run("test Add", func(t *testing.T) {
		partition := newTestPartition()
		require.True(t, partition.Add(&workspace, vec40, childKey40, valueBytes40, true /* overwrite */))
		require.Equal(t, 4, partition.Count())
		require.Equal(t, []ChildKey{childKey10, childKey20, childKey30, childKey40}, partition.ChildKeys())
		require.Equal(t, []ValueBytes{valueBytes10, valueBytes20, valueBytes30, valueBytes40}, partition.ValueBytes())
		require.Equal(t, []float32{4, 3.33}, testutils.RoundFloats(partition.Centroid(), 2))
		validateVectors(partition, []vector.T{vec10, vec20, vec30, vec40})
		checkPartitionMetadata(t, partition.Metadata(), Level(1), vector.T{4, 3.33})

		// Add vector with duplicate key and overwrite=false. Expect no-op.
		partition = newTestPartition()
		require.False(t, partition.Add(&workspace, vec20b, childKey20, valueBytes20b, false /* overwrite */))
		require.Equal(t, 3, partition.Count())
		require.Equal(t, []ValueBytes{valueBytes10, valueBytes20, valueBytes30}, partition.ValueBytes())

		// Add vector with duplicate key and overwrite=true. Expect value to be
		// updated.
		partition = newTestPartition()
		require.False(t, partition.Add(&workspace, vec20b, childKey20, valueBytes20b, true /* overwrite */))
		require.Equal(t, 3, partition.Count())
		require.Equal(t, []ChildKey{childKey10, childKey30, childKey20}, partition.ChildKeys())
		require.Equal(t, []ValueBytes{valueBytes10, valueBytes30, valueBytes20b}, partition.ValueBytes())
		validateVectors(partition, []vector.T{vec10, vec30, vec20b})
	})

	t.Run("test AddSet", func(t *testing.T) {
		// Create empty partition.
		metadata := PartitionMetadata{Level: 1, Centroid: vector.T{4, 3}}
		partition := CreateEmptyPartition(quantizer, metadata)

		// Add empty set.
		vectors := vector.MakeSet(2)
		require.False(t, partition.AddSet(
			&workspace, vectors, []ChildKey{}, []ValueBytes{}, true /* overwrite */))

		// Add set of vectors.
		vectors.Add(vec10)
		vectors.Add(vec20)
		childKeys := []ChildKey{childKey10, childKey20}
		valueBytes := []ValueBytes{valueBytes10, valueBytes20}
		require.True(t, partition.AddSet(
			&workspace, vectors, childKeys, valueBytes, true /* overwrite */))
		require.Equal(t, 2, partition.Count())
		require.Equal(t, []ChildKey{childKey10, childKey20}, partition.ChildKeys())
		require.Equal(t, []ValueBytes{valueBytes10, valueBytes20}, partition.ValueBytes())
		validateVectors(partition, []vector.T{vec10, vec20})

		// Add set containing duplicate, with overwrite=false.
		vectors = vector.MakeSet(2)
		vectors.Add(vec20b)
		vectors.Add(vec30)
		childKeys = []ChildKey{childKey20, childKey30}
		valueBytes = []ValueBytes{valueBytes20b, valueBytes30}
		require.True(t, partition.AddSet(
			&workspace, vectors, childKeys, valueBytes, false /* overwrite */))
		require.Equal(t, 3, partition.Count())
		require.Equal(t, []ChildKey{childKey10, childKey20, childKey30}, partition.ChildKeys())
		require.Equal(t, []ValueBytes{valueBytes10, valueBytes20, valueBytes30}, partition.ValueBytes())
		validateVectors(partition, []vector.T{vec10, vec20, vec30})

		// Add set containing duplicate, with overwrite=true.
		vectors = vector.MakeSet(2)
		vectors.Add(vec40)
		vectors.Add(vec20b)
		childKeys = []ChildKey{childKey40, childKey20}
		valueBytes = []ValueBytes{valueBytes40, valueBytes20b}
		require.True(t, partition.AddSet(
			&workspace, vectors, childKeys, valueBytes, true /* overwrite */))
		require.Equal(t, 4, partition.Count())
		require.Equal(t, []ChildKey{childKey10, childKey40, childKey30, childKey20}, partition.ChildKeys())
		require.Equal(t, []ValueBytes{valueBytes10, valueBytes40, valueBytes30, valueBytes20b}, partition.ValueBytes())
		validateVectors(partition, []vector.T{vec10, vec40, vec30, vec20b})

		// Add set containing only duplicates.
		vectors = vector.MakeSet(2)
		vectors.Add(vec10)
		vectors.Add(vec30)
		childKeys = []ChildKey{childKey10, childKey30}
		valueBytes = []ValueBytes{valueBytes10, valueBytes30}
		require.False(t, partition.AddSet(
			&workspace, vectors, childKeys, valueBytes, true /* overwrite */))
		require.Equal(t, 4, partition.Count())
	})

	t.Run("test Search", func(t *testing.T) {
		// Search empty partition.
		metadata := PartitionMetadata{Level: LeafLevel, Centroid: vector.T{4, 3}}
		partition := CreateEmptyPartition(quantizer, metadata)
		require.Equal(t, Level(1), partition.Level())

		searchSet := SearchSet{MaxResults: 1}
		count := partition.Search(&workspace, RootKey, vector.T{1, 1}, &searchSet)
		require.Equal(t, 0, count)
		results := roundResults(searchSet.PopResults(), 4)
		require.Equal(t, SearchResults(nil), results)

		// Search partition with 5 vectors.
		partition = newTestPartition()
		vectors := vector.MakeSet(2)
		vectors.Add(vec40)
		vectors.Add(vec50)
		childKeys := []ChildKey{childKey40, childKey50}
		valueBytes := []ValueBytes{valueBytes40, valueBytes50}
		partition.AddSet(&workspace, vectors, childKeys, valueBytes, false /* overwrite */)

		searchSet = SearchSet{MaxResults: 3}
		count = partition.Search(&workspace, RootKey, vector.T{1, 1}, &searchSet)
		require.Equal(t, 5, count)
		result1 := SearchResult{
			QueryDistance: 1, ErrorBound: 0, ParentPartitionKey: 1, ChildKey: childKey10, ValueBytes: valueBytes10}
		result2 := SearchResult{
			QueryDistance: 13, ErrorBound: 0, ParentPartitionKey: 1, ChildKey: childKey40, ValueBytes: valueBytes40}
		result3 := SearchResult{
			QueryDistance: 17, ErrorBound: 0, ParentPartitionKey: 1, ChildKey: childKey20, ValueBytes: valueBytes20}
		results = roundResults(searchSet.PopResults(), 4)
		require.Equal(t, SearchResults{result1, result2, result3}, results)
	})

	t.Run("test ReplaceWithLast", func(t *testing.T) {
		partition := newTestPartition()
		partition.ReplaceWithLast(0)
		require.Equal(t, 2, partition.Count())
		require.Equal(t, []ChildKey{childKey30, childKey20}, partition.ChildKeys())
		require.Equal(t, []ValueBytes{valueBytes30, valueBytes20}, partition.ValueBytes())
		validateVectors(partition, []vector.T{vec30, vec20})

		// Remove remaining vectors.
		partition.ReplaceWithLast(1)
		partition.ReplaceWithLast(0)
		require.Equal(t, 0, partition.Count())
		require.Equal(t, []ChildKey{}, partition.ChildKeys())
		require.Equal(t, []ValueBytes{}, partition.ValueBytes())
		validateVectors(partition, []vector.T{})
	})

	t.Run("test ReplaceWithLastByKey and Find", func(t *testing.T) {
		partition := newTestPartition()
		require.Equal(t, 0, partition.Find(childKey10))
		require.Equal(t, 2, partition.Find(childKey30))
		require.True(t, partition.ReplaceWithLastByKey(childKey10))
		require.Equal(t, 2, partition.Count())
		require.Equal(t, []ChildKey{childKey30, childKey20}, partition.ChildKeys())
		require.Equal(t, []ValueBytes{valueBytes30, valueBytes20}, partition.ValueBytes())
		validateVectors(partition, []vector.T{vec30, vec20})

		// Test key not found.
		require.Equal(t, -1, partition.Find(childKey40))
		require.False(t, partition.ReplaceWithLastByKey(childKey40))
	})

	t.Run("test Clear", func(t *testing.T) {
		partition := newTestPartition()
		require.Equal(t, 3, partition.Clear())
		require.Equal(t, 0, partition.Count())
		require.Equal(t, []ChildKey{}, partition.ChildKeys())
		require.Equal(t, []ValueBytes{}, partition.ValueBytes())
		require.Equal(t, []float32{4, 3.33}, testutils.RoundFloats(partition.Centroid(), 2))

		// Clear empty partition.
		require.Equal(t, 0, partition.Clear())
		require.Equal(t, 0, partition.Count())
		require.Equal(t, []ChildKey{}, partition.ChildKeys())
		require.Equal(t, []ValueBytes{}, partition.ValueBytes())
		require.Equal(t, []float32{4, 3.33}, testutils.RoundFloats(partition.Centroid(), 2))
	})
}

func roundResults(results SearchResults, prec int) SearchResults {
	for i := range results {
		result := &results[i]
		result.QueryDistance = float32(scalar.Round(float64(result.QueryDistance), prec))
		result.ErrorBound = float32(scalar.Round(float64(result.ErrorBound), prec))
		result.Vector = testutils.RoundFloats(result.Vector, prec)
	}
	return results
}
