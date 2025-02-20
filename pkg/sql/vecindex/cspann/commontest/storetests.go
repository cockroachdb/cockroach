// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package commontest

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/veclib"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/floats/scalar"
)

// StoreTests run a set of generic tests that should work the same against any
// vector store.
func StoreTests(
	ctx context.Context,
	t *testing.T,
	store cspann.Store,
	quantizer quantize.Quantizer,
	testPKs []cspann.KeyBytes,
	testVectors []vector.T,
) {
	var workspace veclib.Workspace
	childKey2 := cspann.ChildKey{PartitionKey: 2}
	valueBytes2 := cspann.ValueBytes{0}
	primaryKey100 := cspann.ChildKey{KeyBytes: cspann.KeyBytes{1, 00}}
	primaryKey200 := cspann.ChildKey{KeyBytes: cspann.KeyBytes{2, 00}}
	primaryKey300 := cspann.ChildKey{KeyBytes: cspann.KeyBytes{3, 00}}
	primaryKey400 := cspann.ChildKey{KeyBytes: cspann.KeyBytes{4, 00}}
	primaryKey500 := cspann.ChildKey{KeyBytes: cspann.KeyBytes{5, 00}}
	primaryKey600 := cspann.ChildKey{KeyBytes: cspann.KeyBytes{6, 00}}
	valueBytes100 := cspann.ValueBytes{1, 2}
	valueBytes200 := cspann.ValueBytes{3, 4}
	valueBytes300 := cspann.ValueBytes{5, 6}
	valueBytes400 := cspann.ValueBytes{7, 8}
	valueBytes500 := cspann.ValueBytes{9, 10}
	valueBytes600 := cspann.ValueBytes{11, 12}

	t.Run("get full vectors", func(t *testing.T) {
		txn := BeginTransaction(ctx, t, &workspace, store)
		defer CommitTransaction(ctx, t, store, txn)

		// Include primary keys that cannot be found.
		results := []cspann.VectorWithKey{
			{Key: cspann.ChildKey{KeyBytes: testPKs[0]}},
			{Key: cspann.ChildKey{KeyBytes: cspann.KeyBytes{0}}},
			{Key: cspann.ChildKey{KeyBytes: testPKs[1]}},
			{Key: cspann.ChildKey{KeyBytes: cspann.KeyBytes{0}}},
			{Key: cspann.ChildKey{KeyBytes: testPKs[0]}},
		}
		err := txn.GetFullVectors(ctx, results)
		require.NoError(t, err)
		require.Equal(t, testVectors[0], results[0].Vector)
		require.Nil(t, results[1].Vector)
		require.Equal(t, testVectors[1], results[2].Vector)
		require.Nil(t, results[3].Vector)
		require.Equal(t, testVectors[0], results[4].Vector)

		// Grab another set of vectors to ensure that saved state is properly reset.
		results = []cspann.VectorWithKey{
			{Key: cspann.ChildKey{KeyBytes: cspann.KeyBytes{0}}},
			{Key: cspann.ChildKey{KeyBytes: testPKs[0]}},
			{Key: cspann.ChildKey{KeyBytes: cspann.KeyBytes{0}}},
			{Key: cspann.ChildKey{KeyBytes: testPKs[1]}},
			{Key: cspann.ChildKey{KeyBytes: cspann.KeyBytes{0}}},
			{Key: cspann.ChildKey{KeyBytes: testPKs[1]}},
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
		txn := BeginTransaction(ctx, t, &workspace, store)
		defer CommitTransaction(ctx, t, store, txn)

		searchSet := cspann.SearchSet{MaxResults: 2}
		partitionCounts := []int{0}
		level, err := txn.SearchPartitions(
			ctx, []cspann.PartitionKey{cspann.RootKey}, vector.T{1, 1}, &searchSet, partitionCounts)
		require.NoError(t, err)
		require.Equal(t, cspann.LeafLevel, level)
		require.Nil(t, searchSet.PopResults())
		require.Equal(t, 0, partitionCounts[0])

		// Get partition metadata.
		metadata, err := txn.GetPartitionMetadata(ctx, cspann.RootKey, false /* forUpdate */)
		require.NoError(t, err)
		CheckPartitionMetadata(t, metadata, cspann.Level(1), vector.T{0, 0}, 0)
	})

	t.Run("add to root partition", func(t *testing.T) {
		txn := BeginTransaction(ctx, t, &workspace, store)
		defer CommitTransaction(ctx, t, store, txn)

		// Get partition metadata with forUpdate = true before updates.
		metadata, err := txn.GetPartitionMetadata(ctx, cspann.RootKey, true /* forUpdate */)
		require.NoError(t, err)
		CheckPartitionMetadata(t, metadata, cspann.Level(1), vector.T{0, 0}, 0)

		// Add to root partition.
		metadata, err = txn.AddToPartition(ctx, cspann.RootKey, vector.T{1, 2}, primaryKey100, valueBytes100)
		require.NoError(t, err)
		CheckPartitionMetadata(t, metadata, cspann.LeafLevel, vector.T{0, 0}, 1)
		metadata, err = txn.AddToPartition(ctx, cspann.RootKey, vector.T{7, 4}, primaryKey200, valueBytes200)
		require.NoError(t, err)
		CheckPartitionMetadata(t, metadata, cspann.LeafLevel, vector.T{0, 0}, 2)
		metadata, err = txn.AddToPartition(ctx, cspann.RootKey, vector.T{4, 3}, primaryKey300, valueBytes300)
		require.NoError(t, err)
		CheckPartitionMetadata(t, metadata, cspann.LeafLevel, vector.T{0, 0}, 3)

		// Add duplicate and expect value to be overwritten
		metadata, err = txn.AddToPartition(ctx, cspann.RootKey, vector.T{5, 5}, primaryKey300, valueBytes300)
		require.NoError(t, err)
		CheckPartitionMetadata(t, metadata, cspann.LeafLevel, vector.T{0, 0}, 3)

		// Search root partition.
		searchSet := cspann.SearchSet{MaxResults: 2}
		partitionCounts := []int{0}
		level, err := txn.SearchPartitions(
			ctx, []cspann.PartitionKey{cspann.RootKey}, vector.T{1, 1}, &searchSet, partitionCounts)
		require.NoError(t, err)
		require.Equal(t, cspann.Level(1), level)
		result1 := cspann.SearchResult{
			QuerySquaredDistance: 1, ErrorBound: 0, CentroidDistance: 2.2361,
			ParentPartitionKey: 1, ChildKey: primaryKey100, ValueBytes: valueBytes100}
		result2 := cspann.SearchResult{
			QuerySquaredDistance: 32, ErrorBound: 0, CentroidDistance: 7.0711,
			ParentPartitionKey: 1, ChildKey: primaryKey300, ValueBytes: valueBytes300}
		results := searchSet.PopResults()
		RoundResults(results, 4)
		require.Equal(t, cspann.SearchResults{result1, result2}, results)
		require.Equal(t, 3, partitionCounts[0])

		// Ensure partition metadata is updated.
		metadata, err = txn.GetPartitionMetadata(ctx, cspann.RootKey, true /* forUpdate */)
		require.NoError(t, err)
		CheckPartitionMetadata(t, metadata, cspann.Level(1), vector.T{0, 0}, 3)
	})

	var root *cspann.Partition
	t.Run("get root partition", func(t *testing.T) {
		txn := BeginTransaction(ctx, t, &workspace, store)
		defer CommitTransaction(ctx, t, store, txn)

		// Get root partition.
		var err error
		root, err = txn.GetPartition(ctx, cspann.RootKey)
		require.NoError(t, err)
		require.Equal(t, cspann.Level(1), root.Level())
		require.Equal(t, []cspann.ChildKey{primaryKey100, primaryKey200, primaryKey300}, root.ChildKeys())
		require.Equal(t, []cspann.ValueBytes{valueBytes100, valueBytes200, valueBytes300}, root.ValueBytes())
		require.Equal(t, vector.T{0, 0}, root.Centroid())

		// Get partition centroid + full vectors.
		results := []cspann.VectorWithKey{
			{Key: cspann.ChildKey{PartitionKey: cspann.RootKey}},
			{Key: cspann.ChildKey{KeyBytes: testPKs[0]}},
			{Key: cspann.ChildKey{KeyBytes: cspann.KeyBytes{0}}},
		}
		err = txn.GetFullVectors(ctx, results)
		require.NoError(t, err)
		require.Equal(t, vector.T{0, 0}, results[0].Vector)
		require.Equal(t, testVectors[0], results[1].Vector)
		require.Nil(t, results[2].Vector)

		// Get partition metadata.
		metadata, err := txn.GetPartitionMetadata(ctx, cspann.RootKey, false /* forUpdate */)
		require.NoError(t, err)
		CheckPartitionMetadata(t, metadata, cspann.Level(1), vector.T{0, 0}, 3)
	})

	t.Run("replace root partition", func(t *testing.T) {
		txn := BeginTransaction(ctx, t, &workspace, store)
		defer CommitTransaction(ctx, t, store, txn)

		// Replace root partition.
		_, err := txn.GetPartition(ctx, cspann.RootKey)
		require.NoError(t, err)
		vectors := vector.T{4, 3}.AsSet()
		quantizedSet := quantizer.Quantize(&workspace, vectors)
		newRoot := cspann.NewPartition(quantizer, quantizedSet,
			[]cspann.ChildKey{childKey2}, []cspann.ValueBytes{valueBytes2}, cspann.Level(2))
		require.NoError(t, txn.SetRootPartition(ctx, newRoot))
		newRoot, err = txn.GetPartition(ctx, cspann.RootKey)
		require.NoError(t, err)
		require.Equal(t, cspann.Level(2), newRoot.Level())
		require.Equal(t, []cspann.ChildKey{childKey2}, newRoot.ChildKeys())
		require.Equal(t, []cspann.ValueBytes{valueBytes2}, newRoot.ValueBytes())

		searchSet := cspann.SearchSet{MaxResults: 2}
		partitionCounts := []int{0}
		level, err := txn.SearchPartitions(
			ctx, []cspann.PartitionKey{cspann.RootKey}, vector.T{2, 2}, &searchSet, partitionCounts)
		require.NoError(t, err)
		require.Equal(t, cspann.Level(2), level)
		result3 := cspann.SearchResult{
			QuerySquaredDistance: 5, ErrorBound: 0, CentroidDistance: 0, ParentPartitionKey: 1, ChildKey: childKey2, ValueBytes: valueBytes2}
		require.Equal(t, cspann.SearchResults{result3}, searchSet.PopResults())
		require.Equal(t, 1, partitionCounts[0])

		// Get partition metadata.
		metadata, err := txn.GetPartitionMetadata(ctx, cspann.RootKey, false /* forUpdate */)
		require.NoError(t, err)
		CheckPartitionMetadata(t, metadata, cspann.Level(2), vector.T{4, 3}, 1)
	})

	var partitionKey1 cspann.PartitionKey
	t.Run("insert another partition and update it", func(t *testing.T) {
		txn := BeginTransaction(ctx, t, &workspace, store)
		defer CommitTransaction(ctx, t, store, txn)

		_, err := txn.GetPartition(ctx, cspann.RootKey)
		require.NoError(t, err)
		partitionKey1, err = txn.InsertPartition(ctx, root)
		require.NoError(t, err)
		metadata, err := txn.RemoveFromPartition(ctx, partitionKey1, primaryKey200)
		require.NoError(t, err)
		CheckPartitionMetadata(t, metadata, cspann.LeafLevel, vector.T{0, 0}, 2)

		// Try to remove the same key again.
		metadata, err = txn.RemoveFromPartition(ctx, partitionKey1, primaryKey200)
		require.NoError(t, err)
		CheckPartitionMetadata(t, metadata, cspann.LeafLevel, vector.T{0, 0}, 2)

		// Add an alternate element and add duplicate, expecting value to be overwritten.
		metadata, err = txn.AddToPartition(
			ctx, partitionKey1, vector.T{-1, 0}, primaryKey400, valueBytes400)
		require.NoError(t, err)
		CheckPartitionMetadata(t, metadata, cspann.LeafLevel, vector.T{0, 0}, 3)
		metadata, err = txn.AddToPartition(
			ctx, partitionKey1, vector.T{1, 1}, primaryKey400, valueBytes400)
		require.NoError(t, err)
		CheckPartitionMetadata(t, metadata, cspann.LeafLevel, vector.T{0, 0}, 3)

		searchSet := cspann.SearchSet{MaxResults: 2}
		partitionCounts := []int{0}
		level, err := txn.SearchPartitions(
			ctx, []cspann.PartitionKey{partitionKey1}, vector.T{1, 1}, &searchSet, partitionCounts)
		require.NoError(t, err)
		require.Equal(t, cspann.Level(1), level)
		result4 := cspann.SearchResult{
			QuerySquaredDistance: 0, ErrorBound: 0, CentroidDistance: 1.4142,
			ParentPartitionKey: partitionKey1, ChildKey: primaryKey400, ValueBytes: valueBytes400}
		result5 := cspann.SearchResult{
			QuerySquaredDistance: 1, ErrorBound: 0, CentroidDistance: 2.2361,
			ParentPartitionKey: partitionKey1, ChildKey: primaryKey100, ValueBytes: valueBytes100}
		require.Equal(t, cspann.SearchResults{result4, result5}, RoundResults(searchSet.PopResults(), 4))
		require.Equal(t, 3, partitionCounts[0])
	})

	t.Run("search multiple partitions at leaf level", func(t *testing.T) {
		txn := BeginTransaction(ctx, t, &workspace, store)
		defer CommitTransaction(ctx, t, store, txn)

		_, err := txn.GetPartition(ctx, cspann.RootKey)
		require.NoError(t, err)

		vectors := vector.MakeSet(2)
		vectors.Add(vector.T{4, -1})
		vectors.Add(vector.T{2, 8})
		quantizedSet := quantizer.Quantize(&workspace, vectors)
		partition := cspann.NewPartition(
			quantizer, quantizedSet, []cspann.ChildKey{primaryKey500, primaryKey600},
			[]cspann.ValueBytes{valueBytes500, valueBytes600}, cspann.LeafLevel)
		partitionKey2, err := txn.InsertPartition(ctx, partition)
		require.NoError(t, err)

		searchSet := cspann.SearchSet{MaxResults: 2}
		partitionCounts := []int{0, 0}
		level, err := txn.SearchPartitions(
			ctx, []cspann.PartitionKey{partitionKey1, partitionKey2}, vector.T{3, 1},
			&searchSet, partitionCounts)
		require.NoError(t, err)
		require.Equal(t, cspann.Level(1), level)
		result4 := cspann.SearchResult{
			QuerySquaredDistance: 4, ErrorBound: 0, CentroidDistance: 1.41,
			ParentPartitionKey: partitionKey1, ChildKey: primaryKey400, ValueBytes: valueBytes400}
		result5 := cspann.SearchResult{
			QuerySquaredDistance: 5, ErrorBound: 0, CentroidDistance: 2.24,
			ParentPartitionKey: partitionKey1, ChildKey: primaryKey100, ValueBytes: valueBytes100}
		require.Equal(t, cspann.SearchResults{result4, result5}, RoundResults(searchSet.PopResults(), 2))
		require.Equal(t, []int{3, 2}, partitionCounts)
	})
}

// CheckPartitionMetadata tests the correctness of the given metadata's fields.
func CheckPartitionMetadata(
	t *testing.T, metadata cspann.PartitionMetadata, level cspann.Level, centroid vector.T, count int,
) {
	require.Equal(t, level, metadata.Level)
	require.Equal(t, []float32(centroid), testutils.RoundFloats(metadata.Centroid, 2))
	require.Equal(t, count, metadata.Count)
}

// BeginTransaction starts a new transaction for the given store and returns it.
func BeginTransaction(
	ctx context.Context, t *testing.T, w *veclib.Workspace, store cspann.Store,
) cspann.Txn {
	txn, err := store.Begin(ctx, w)
	require.NoError(t, err)
	return txn
}

// CommitTransaction commits a transaction that was started by BeginTransaction.
func CommitTransaction(ctx context.Context, t *testing.T, store cspann.Store, txn cspann.Txn) {
	err := store.Commit(ctx, txn)
	require.NoError(t, err)
}

// AbortTransaction aborts a transaction that was started by BeginTransaction.
func AbortTransaction(ctx context.Context, t *testing.T, store cspann.Store, txn cspann.Txn) {
	err := store.Abort(ctx, txn)
	require.NoError(t, err)
}

// RoundResults rounds all float fields in the given set of results, using the
// requested precision.
func RoundResults(results cspann.SearchResults, prec int) cspann.SearchResults {
	for i := range results {
		result := &results[i]
		result.QuerySquaredDistance = float32(scalar.Round(float64(result.QuerySquaredDistance), prec))
		result.ErrorBound = float32(scalar.Round(float64(result.ErrorBound), prec))
		result.CentroidDistance = float32(scalar.Round(float64(result.CentroidDistance), prec))
		result.Vector = testutils.RoundFloats(result.Vector, prec)
	}
	return results
}
