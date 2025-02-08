// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
)

func commonStoreTests(
	ctx context.Context,
	t *testing.T,
	store Store,
	quantizer quantize.Quantizer,
	testPKs []PrimaryKey,
	testVectors []vector.T,
) {
	childKey2 := ChildKey{PartitionKey: 2}
	primaryKey100 := ChildKey{PrimaryKey: PrimaryKey{1, 00}}
	primaryKey200 := ChildKey{PrimaryKey: PrimaryKey{2, 00}}
	primaryKey300 := ChildKey{PrimaryKey: PrimaryKey{3, 00}}
	primaryKey400 := ChildKey{PrimaryKey: PrimaryKey{4, 00}}
	primaryKey500 := ChildKey{PrimaryKey: PrimaryKey{5, 00}}
	primaryKey600 := ChildKey{PrimaryKey: PrimaryKey{6, 00}}

	t.Run("get full vectors", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		// Include primary keys that cannot be found.
		results := []VectorWithKey{
			{Key: ChildKey{PrimaryKey: testPKs[0]}},
			{Key: ChildKey{PrimaryKey: PrimaryKey{0}}},
			{Key: ChildKey{PrimaryKey: testPKs[1]}},
			{Key: ChildKey{PrimaryKey: PrimaryKey{0}}},
			{Key: ChildKey{PrimaryKey: testPKs[0]}},
		}
		err := txn.GetFullVectors(ctx, results)
		require.NoError(t, err)
		require.Equal(t, testVectors[0], results[0].Vector)
		require.Nil(t, results[1].Vector)
		require.Equal(t, testVectors[1], results[2].Vector)
		require.Nil(t, results[3].Vector)
		require.Equal(t, testVectors[0], results[4].Vector)

		// Grab another set of vectors to ensure that saved state is properly reset.
		results = []VectorWithKey{
			{Key: ChildKey{PrimaryKey: PrimaryKey{0}}},
			{Key: ChildKey{PrimaryKey: testPKs[0]}},
			{Key: ChildKey{PrimaryKey: PrimaryKey{0}}},
			{Key: ChildKey{PrimaryKey: testPKs[1]}},
			{Key: ChildKey{PrimaryKey: PrimaryKey{0}}},
			{Key: ChildKey{PrimaryKey: testPKs[1]}},
		}
		err = txn.GetFullVectors(ctx, results)
		require.NoError(t, err)
		require.Nil(t, results[0].Vector)
		require.Equal(t, testVectors[0], results[1].Vector)
		require.Nil(t, results[2].Vector)
		require.Equal(t, testVectors[1], results[3].Vector)
		require.Nil(t, results[4].Vector)
		require.Equal(t, testVectors[1], results[5].Vector)
	})

	t.Run("search empty root partition", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		searchSet := SearchSet{MaxResults: 2}
		partitionCounts := []int{0}
		level, err := txn.SearchPartitions(
			ctx, []PartitionKey{RootKey}, vector.T{1, 1}, &searchSet, partitionCounts)
		require.NoError(t, err)
		require.Equal(t, LeafLevel, level)
		require.Nil(t, searchSet.PopResults())
		require.Equal(t, 0, partitionCounts[0])

		// Get partition metadata.
		metadata, err := txn.GetPartitionMetadata(ctx, RootKey, false /* forUpdate */)
		require.NoError(t, err)
		checkPartitionMetadata(t, metadata, Level(1), vector.T{0, 0}, 0)
	})

	t.Run("add to root partition", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		// Get partition metadata with forUpdate = true before updates.
		metadata, err := txn.GetPartitionMetadata(ctx, RootKey, true /* forUpdate */)
		require.NoError(t, err)
		checkPartitionMetadata(t, metadata, Level(1), vector.T{0, 0}, 0)

		// Add to root partition.
		metadata, err = txn.AddToPartition(ctx, RootKey, vector.T{1, 2}, primaryKey100)
		require.NoError(t, err)
		checkPartitionMetadata(t, metadata, LeafLevel, vector.T{0, 0}, 1)
		metadata, err = txn.AddToPartition(ctx, RootKey, vector.T{7, 4}, primaryKey200)
		require.NoError(t, err)
		checkPartitionMetadata(t, metadata, LeafLevel, vector.T{0, 0}, 2)
		metadata, err = txn.AddToPartition(ctx, RootKey, vector.T{4, 3}, primaryKey300)
		require.NoError(t, err)
		checkPartitionMetadata(t, metadata, LeafLevel, vector.T{0, 0}, 3)

		// Add duplicate and expect value to be overwritten
		metadata, err = txn.AddToPartition(ctx, RootKey, vector.T{5, 5}, primaryKey300)
		require.NoError(t, err)
		checkPartitionMetadata(t, metadata, LeafLevel, vector.T{0, 0}, 3)

		// Search root partition.
		searchSet := SearchSet{MaxResults: 2}
		partitionCounts := []int{0}
		level, err := txn.SearchPartitions(
			ctx, []PartitionKey{RootKey}, vector.T{1, 1}, &searchSet, partitionCounts)
		require.NoError(t, err)
		require.Equal(t, Level(1), level)
		result1 := SearchResult{QuerySquaredDistance: 1, ErrorBound: 0, CentroidDistance: 2.2361, ParentPartitionKey: 1, ChildKey: primaryKey100}
		result2 := SearchResult{QuerySquaredDistance: 32, ErrorBound: 0, CentroidDistance: 7.0711, ParentPartitionKey: 1, ChildKey: primaryKey300}
		results := searchSet.PopResults()
		roundResults(results, 4)
		require.Equal(t, SearchResults{result1, result2}, results)
		require.Equal(t, 3, partitionCounts[0])

		// Ensure partition metadata is updated.
		metadata, err = txn.GetPartitionMetadata(ctx, RootKey, true /* forUpdate */)
		require.NoError(t, err)
		checkPartitionMetadata(t, metadata, Level(1), vector.T{0, 0}, 3)
	})

	var root *Partition
	t.Run("get root partition", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		// Get root partition.
		var err error
		root, err = txn.GetPartition(ctx, RootKey)
		require.NoError(t, err)
		require.Equal(t, Level(1), root.Level())
		require.Equal(t, []ChildKey{primaryKey100, primaryKey200, primaryKey300}, root.ChildKeys())
		require.Equal(t, vector.T{0, 0}, root.Centroid())

		// Get partition centroid + full vectors.
		results := []VectorWithKey{
			{Key: ChildKey{PartitionKey: RootKey}},
			{Key: ChildKey{PrimaryKey: testPKs[0]}},
			{Key: ChildKey{PrimaryKey: PrimaryKey{0}}},
		}
		err = txn.GetFullVectors(ctx, results)
		require.NoError(t, err)
		require.Equal(t, vector.T{0, 0}, results[0].Vector)
		require.Equal(t, testVectors[0], results[1].Vector)
		require.Nil(t, results[2].Vector)

		// Get partition metadata.
		metadata, err := txn.GetPartitionMetadata(ctx, RootKey, false /* forUpdate */)
		require.NoError(t, err)
		checkPartitionMetadata(t, metadata, Level(1), vector.T{0, 0}, 3)
	})

	t.Run("replace root partition", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		// Replace root partition.
		_, err := txn.GetPartition(ctx, RootKey)
		require.NoError(t, err)
		vectors := vector.T{4, 3}.AsSet()
		quantizedSet := quantizer.Quantize(ctx, vectors)
		newRoot := NewPartition(quantizer, quantizedSet, []ChildKey{childKey2}, Level(2))
		require.NoError(t, txn.SetRootPartition(ctx, newRoot))
		newRoot, err = txn.GetPartition(ctx, RootKey)
		require.NoError(t, err)
		require.Equal(t, Level(2), newRoot.Level())
		require.Equal(t, []ChildKey{childKey2}, newRoot.ChildKeys())

		searchSet := SearchSet{MaxResults: 2}
		partitionCounts := []int{0}
		level, err := txn.SearchPartitions(
			ctx, []PartitionKey{RootKey}, vector.T{2, 2}, &searchSet, partitionCounts)
		require.NoError(t, err)
		require.Equal(t, Level(2), level)
		result3 := SearchResult{QuerySquaredDistance: 5, ErrorBound: 0, CentroidDistance: 0, ParentPartitionKey: 1, ChildKey: childKey2}
		require.Equal(t, SearchResults{result3}, searchSet.PopResults())
		require.Equal(t, 1, partitionCounts[0])

		// Get partition metadata.
		metadata, err := txn.GetPartitionMetadata(ctx, RootKey, false /* forUpdate */)
		require.NoError(t, err)
		checkPartitionMetadata(t, metadata, Level(2), vector.T{4, 3}, 1)
	})

	var partitionKey1 PartitionKey
	t.Run("insert another partition and update it", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		_, err := txn.GetPartition(ctx, RootKey)
		require.NoError(t, err)
		partitionKey1, err = txn.InsertPartition(ctx, root)
		require.NoError(t, err)
		metadata, err := txn.RemoveFromPartition(ctx, partitionKey1, primaryKey200)
		require.NoError(t, err)
		checkPartitionMetadata(t, metadata, LeafLevel, vector.T{0, 0}, 2)

		// Try to remove the same key again.
		metadata, err = txn.RemoveFromPartition(ctx, partitionKey1, primaryKey200)
		require.NoError(t, err)
		checkPartitionMetadata(t, metadata, LeafLevel, vector.T{0, 0}, 2)

		// Add an alternate element and add duplicate, expecting value to be overwritten.
		metadata, err = txn.AddToPartition(ctx, partitionKey1, vector.T{-1, 0}, primaryKey400)
		require.NoError(t, err)
		checkPartitionMetadata(t, metadata, LeafLevel, vector.T{0, 0}, 3)
		metadata, err = txn.AddToPartition(ctx, partitionKey1, vector.T{1, 1}, primaryKey400)
		require.NoError(t, err)
		checkPartitionMetadata(t, metadata, LeafLevel, vector.T{0, 0}, 3)

		searchSet := SearchSet{MaxResults: 2}
		partitionCounts := []int{0}
		level, err := txn.SearchPartitions(
			ctx, []PartitionKey{partitionKey1}, vector.T{1, 1}, &searchSet, partitionCounts)
		require.NoError(t, err)
		require.Equal(t, Level(1), level)
		result4 := SearchResult{QuerySquaredDistance: 0, ErrorBound: 0, CentroidDistance: 1.4142, ParentPartitionKey: partitionKey1, ChildKey: primaryKey400}
		result5 := SearchResult{QuerySquaredDistance: 1, ErrorBound: 0, CentroidDistance: 2.2361, ParentPartitionKey: partitionKey1, ChildKey: primaryKey100}
		require.Equal(t, SearchResults{result4, result5}, roundResults(searchSet.PopResults(), 4))
		require.Equal(t, 3, partitionCounts[0])
	})

	t.Run("search multiple partitions at leaf level", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		_, err := txn.GetPartition(ctx, RootKey)
		require.NoError(t, err)

		vectors := vector.MakeSet(2)
		vectors.Add(vector.T{4, -1})
		vectors.Add(vector.T{2, 8})
		quantizedSet := quantizer.Quantize(ctx, vectors)
		partition := NewPartition(quantizer, quantizedSet, []ChildKey{primaryKey500, primaryKey600}, LeafLevel)
		partitionKey2, err := txn.InsertPartition(ctx, partition)
		require.NoError(t, err)

		searchSet := SearchSet{MaxResults: 2}
		partitionCounts := []int{0, 0}
		level, err := txn.SearchPartitions(
			ctx, []PartitionKey{partitionKey1, partitionKey2}, vector.T{3, 1}, &searchSet, partitionCounts)
		require.NoError(t, err)
		require.Equal(t, Level(1), level)
		result4 := SearchResult{QuerySquaredDistance: 4, ErrorBound: 0, CentroidDistance: 1.41, ParentPartitionKey: partitionKey1, ChildKey: primaryKey400}
		result5 := SearchResult{QuerySquaredDistance: 5, ErrorBound: 0, CentroidDistance: 2.24, ParentPartitionKey: partitionKey1, ChildKey: primaryKey100}
		require.Equal(t, SearchResults{result4, result5}, roundResults(searchSet.PopResults(), 2))
		require.Equal(t, []int{3, 2}, partitionCounts)
	})
}
