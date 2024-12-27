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
	ctx context.Context, t *testing.T, store Store, quantizer quantize.Quantizer,
) {
	childKey2 := ChildKey{PartitionKey: 2}
	primaryKey100 := ChildKey{PrimaryKey: PrimaryKey{1, 00}}
	primaryKey200 := ChildKey{PrimaryKey: PrimaryKey{2, 00}}
	primaryKey300 := ChildKey{PrimaryKey: PrimaryKey{3, 00}}
	primaryKey400 := ChildKey{PrimaryKey: PrimaryKey{4, 00}}
	primaryKey500 := ChildKey{PrimaryKey: PrimaryKey{5, 00}}
	primaryKey600 := ChildKey{PrimaryKey: PrimaryKey{6, 00}}

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
	})

	t.Run("add to root partition", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		// Add to root partition.
		count, err := txn.AddToPartition(ctx, RootKey, vector.T{1, 2}, primaryKey100)
		require.NoError(t, err)
		require.Equal(t, 1, count)
		count, err = txn.AddToPartition(ctx, RootKey, vector.T{7, 4}, primaryKey200)
		require.NoError(t, err)
		require.Equal(t, 2, count)
		count, err = txn.AddToPartition(ctx, RootKey, vector.T{4, 3}, primaryKey300)
		require.NoError(t, err)
		require.Equal(t, 3, count)

		// Add duplicate and expect value to be overwritten
		count, err = txn.AddToPartition(ctx, RootKey, vector.T{5, 5}, primaryKey300)
		require.NoError(t, err)
		require.Equal(t, 3, count)

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

		// TODO (mw5h): Implement GetFullVectors for PersistentStore.
		// Get partition centroid + full vectors.
		if _, ok := txn.(*inMemoryTxn); ok {
			results := []VectorWithKey{
				{Key: ChildKey{PartitionKey: RootKey}},
				{Key: ChildKey{PrimaryKey: PrimaryKey{11}}},
				{Key: ChildKey{PrimaryKey: PrimaryKey{0}}},
			}
			err = txn.GetFullVectors(ctx, results)
			require.NoError(t, err)
			require.Equal(t, vector.T{0, 0}, results[0].Vector)
			require.Equal(t, vector.T{100, 200}, results[1].Vector)
			require.Nil(t, results[2].Vector)
		}
	})

	t.Run("replace root partition", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		// Replace root partition.
		_, err := txn.GetPartition(ctx, RootKey)
		require.NoError(t, err)
		vectors := vector.T{4, 3}.AsSet()
		quantizedSet := quantizer.Quantize(ctx, &vectors)
		newRoot := NewPartition(quantizer, quantizedSet, []ChildKey{childKey2}, 2)
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
	})

	var partitionKey1 PartitionKey
	t.Run("insert another partition and update it", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		_, err := txn.GetPartition(ctx, RootKey)
		require.NoError(t, err)
		partitionKey1, err = txn.InsertPartition(ctx, root)
		require.NoError(t, err)
		count, err := txn.RemoveFromPartition(ctx, partitionKey1, primaryKey200)
		require.NoError(t, err)
		require.Equal(t, 2, count)

		// Try to remove the same key again.
		count, err = txn.RemoveFromPartition(ctx, partitionKey1, primaryKey200)
		require.NoError(t, err)
		require.Equal(t, 2, count)

		// Add an alternate element and add duplicate, expecting value to be overwritten.
		count, err = txn.AddToPartition(ctx, partitionKey1, vector.T{-1, 0}, primaryKey400)
		require.NoError(t, err)
		require.Equal(t, 3, count)
		count, err = txn.AddToPartition(ctx, partitionKey1, vector.T{1, 1}, primaryKey400)
		require.NoError(t, err)
		require.Equal(t, 3, count)

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
		quantizedSet := quantizer.Quantize(ctx, &vectors)
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
