// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"context"
	"runtime"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/internal"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
)

func TestInMemoryStore(t *testing.T) {
	ctx := internal.WithWorkspace(context.Background(), &internal.Workspace{})

	childKey2 := ChildKey{PartitionKey: 2}
	childKey10 := ChildKey{PartitionKey: 10}
	childKey20 := ChildKey{PartitionKey: 20}
	childKey30 := ChildKey{PartitionKey: 30}
	childKey40 := ChildKey{PartitionKey: 40}
	childKey50 := ChildKey{PartitionKey: 50}
	childKey60 := ChildKey{PartitionKey: 60}

	store := NewInMemoryStore(2, 42)
	quantizer := quantize.NewUnQuantizer(2)

	t.Run("insert and get full vectors", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		vec1 := vector.T{100, 200}
		store.InsertVector(txn, []byte{11}, vec1)
		vec2 := vector.T{300, 400}
		store.InsertVector(txn, []byte{12}, vec2)

		// Include primary keys that are cannot be found.
		results := []VectorWithKey{
			{Key: ChildKey{PrimaryKey: PrimaryKey{11}}},
			{Key: ChildKey{PrimaryKey: PrimaryKey{0}}},
			{Key: ChildKey{PrimaryKey: PrimaryKey{12}}},
			{Key: ChildKey{PrimaryKey: PrimaryKey{0}}},
		}
		err := store.GetFullVectors(ctx, txn, results)
		require.NoError(t, err)
		require.Equal(t, vec1, results[0].Vector)
		require.Nil(t, results[1].Vector)
		require.Equal(t, vec2, results[2].Vector)
		require.Nil(t, results[3].Vector)
	})

	t.Run("insert empty root partition into the store", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		vectors := vector.MakeSet(2)
		quantizedSet := quantizer.Quantize(ctx, &vectors)
		root := NewPartition(quantizer, quantizedSet, []ChildKey{}, LeafLevel)
		require.NoError(t, store.SetRootPartition(ctx, txn, root))

		searchSet := SearchSet{MaxResults: 2}
		partitionCounts := []int{0}
		level, err := store.SearchPartitions(
			ctx, txn, []PartitionKey{RootKey}, vector.T{1, 1}, &searchSet, partitionCounts)
		require.NoError(t, err)
		require.Equal(t, LeafLevel, level)
		require.Nil(t, searchSet.PopResults())
		require.Equal(t, 0, partitionCounts[0])
	})

	t.Run("add to root partition", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		// Add to root partition.
		count, err := store.AddToPartition(ctx, txn, RootKey, vector.T{1, 2}, childKey10)
		require.NoError(t, err)
		require.Equal(t, 1, count)
		count, err = store.AddToPartition(ctx, txn, RootKey, vector.T{7, 4}, childKey20)
		require.NoError(t, err)
		require.Equal(t, 2, count)
		count, err = store.AddToPartition(ctx, txn, RootKey, vector.T{4, 3}, childKey30)
		require.NoError(t, err)
		require.Equal(t, 3, count)

		// Try to add duplicate.
		count, err = store.AddToPartition(ctx, txn, RootKey, vector.T{5, 5}, childKey30)
		require.NoError(t, err)
		require.Equal(t, -1, count)

		// Search root partition.
		searchSet := SearchSet{MaxResults: 2}
		partitionCounts := []int{0}
		level, err := store.SearchPartitions(
			ctx, txn, []PartitionKey{RootKey}, vector.T{1, 1}, &searchSet, partitionCounts)
		require.NoError(t, err)
		require.Equal(t, Level(1), level)
		result1 := SearchResult{QuerySquaredDistance: 1, ErrorBound: 0, CentroidDistance: 2.2361, ParentPartitionKey: 1, ChildKey: childKey10}
		result2 := SearchResult{QuerySquaredDistance: 13, ErrorBound: 0, CentroidDistance: 5, ParentPartitionKey: 1, ChildKey: childKey30}
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
		root, err = store.GetPartition(ctx, txn, RootKey)
		require.NoError(t, err)
		require.Equal(t, Level(1), root.Level())
		require.Equal(t, []ChildKey{childKey10, childKey20, childKey30}, root.ChildKeys())
		require.Equal(t, vector.T{0, 0}, root.Centroid())

		// Get partition centroid + full vectors.
		results := []VectorWithKey{
			{Key: ChildKey{PartitionKey: RootKey}},
			{Key: ChildKey{PrimaryKey: PrimaryKey{11}}},
			{Key: ChildKey{PrimaryKey: PrimaryKey{0}}},
		}
		err = store.GetFullVectors(ctx, txn, results)
		require.NoError(t, err)
		require.Equal(t, vector.T{0, 0}, results[0].Vector)
		require.Equal(t, vector.T{100, 200}, results[1].Vector)
		require.Nil(t, results[2].Vector)
	})

	t.Run("replace root partition", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		// Replace root partition.
		vectors := vector.T{4, 3}.AsSet()
		quantizedSet := quantizer.Quantize(ctx, &vectors)
		newRoot := NewPartition(quantizer, quantizedSet, []ChildKey{childKey2}, 2)
		require.NoError(t, store.SetRootPartition(ctx, txn, newRoot))
		newRoot, err := store.GetPartition(ctx, txn, RootKey)
		require.NoError(t, err)
		require.Equal(t, Level(2), newRoot.Level())
		require.Equal(t, []ChildKey{childKey2}, newRoot.ChildKeys())

		searchSet := SearchSet{MaxResults: 2}
		partitionCounts := []int{0}
		level, err := store.SearchPartitions(
			ctx, txn, []PartitionKey{RootKey}, vector.T{2, 2}, &searchSet, partitionCounts)
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

		var err error
		partitionKey1, err = store.InsertPartition(ctx, txn, root)
		require.NoError(t, err)
		require.Equal(t, PartitionKey(2), partitionKey1)
		count, err := store.RemoveFromPartition(ctx, txn, partitionKey1, childKey20)
		require.NoError(t, err)
		require.Equal(t, 2, count)
		count, err = store.RemoveFromPartition(ctx, txn, partitionKey1, childKey20)
		require.NoError(t, err)
		require.Equal(t, -1, count)

		// Add an alternate element and try to add duplicate.
		count, err = store.AddToPartition(ctx, txn, partitionKey1, vector.T{-1, 0}, childKey40)
		require.NoError(t, err)
		require.Equal(t, 3, count)
		count, err = store.AddToPartition(ctx, txn, partitionKey1, vector.T{1, 1}, childKey40)
		require.NoError(t, err)
		require.Equal(t, -1, count)

		searchSet := SearchSet{MaxResults: 2}
		partitionCounts := []int{0}
		level, err := store.SearchPartitions(
			ctx, txn, []PartitionKey{partitionKey1}, vector.T{1, 1}, &searchSet, partitionCounts)
		require.NoError(t, err)
		require.Equal(t, Level(1), level)
		result4 := SearchResult{QuerySquaredDistance: 1, ErrorBound: 0, CentroidDistance: 2.23606797749979, ParentPartitionKey: 2, ChildKey: childKey10}
		result5 := SearchResult{QuerySquaredDistance: 5, ErrorBound: 0, CentroidDistance: 1, ParentPartitionKey: 2, ChildKey: childKey40}
		require.Equal(t, SearchResults{result4, result5}, searchSet.PopResults())
		require.Equal(t, 3, partitionCounts[0])
	})

	t.Run("search multiple partitions at leaf level", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		vectors := vector.MakeSet(2)
		vectors.Add(vector.T{4, -1})
		vectors.Add(vector.T{2, 8})
		quantizedSet := quantizer.Quantize(ctx, &vectors)
		partition := NewPartition(quantizer, quantizedSet, []ChildKey{childKey50, childKey60}, LeafLevel)
		partitionKey2, err := store.InsertPartition(ctx, txn, partition)
		require.NoError(t, err)
		require.Equal(t, PartitionKey(3), partitionKey2)

		searchSet := SearchSet{MaxResults: 2}
		partitionCounts := []int{0, 0}
		level, err := store.SearchPartitions(
			ctx, txn, []PartitionKey{partitionKey1, partitionKey2}, vector.T{3, 1}, &searchSet, partitionCounts)
		require.NoError(t, err)
		require.Equal(t, Level(1), level)
		result4 := SearchResult{QuerySquaredDistance: 5, ErrorBound: 0, CentroidDistance: 5, ParentPartitionKey: 2, ChildKey: childKey30}
		result5 := SearchResult{QuerySquaredDistance: 5, ErrorBound: 0, CentroidDistance: 4.61, ParentPartitionKey: 3, ChildKey: childKey50}
		require.Equal(t, SearchResults{result4, result5}, roundResults(searchSet.PopResults(), 2))
		require.Equal(t, []int{3, 2}, partitionCounts)
	})

	t.Run("delete full vector", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		store.DeleteVector(txn, []byte{10})
		refs := []VectorWithKey{{Key: ChildKey{PrimaryKey: PrimaryKey{10}}}}
		err := store.GetFullVectors(ctx, txn, refs)
		require.NoError(t, err)
		require.Nil(t, refs[0].Vector)
	})

	t.Run("abort transaction", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer abortTransaction(ctx, t, store, txn)

		// Perform some read-only operations.
		_, err := store.GetPartition(ctx, txn, RootKey)
		require.NoError(t, err)

		err = store.GetFullVectors(ctx, txn, []VectorWithKey{
			{Key: ChildKey{PrimaryKey: PrimaryKey{11}}},
		})
		require.NoError(t, err)
	})
}

func TestInMemoryStoreConcurrency(t *testing.T) {
	ctx := internal.WithWorkspace(context.Background(), &internal.Workspace{})

	childKey10 := ChildKey{PartitionKey: 10}

	// Insert root partition into new store.
	store := NewInMemoryStore(2, 42)
	quantizer := quantize.NewUnQuantizer(2)

	var wait sync.WaitGroup
	wait.Add(1)
	func() {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		// Acquire partition lock.
		vectors := vector.MakeSet(2)
		quantizedSet := quantizer.Quantize(ctx, &vectors)
		root := NewPartition(quantizer, quantizedSet, []ChildKey{}, LeafLevel)
		require.NoError(t, store.SetRootPartition(ctx, txn, root))

		// Search root partition on background goroutine.
		go func() {
			ctx2 := internal.WithWorkspace(context.Background(), &internal.Workspace{})

			// BeginTransaction should block until the outer transaction is complete.
			txn2 := beginTransaction(ctx, t, store)
			defer commitTransaction(ctx, t, store, txn2)

			searchSet := SearchSet{MaxResults: 2}
			partitionCounts := []int{0}
			_, err := store.SearchPartitions(
				ctx2, txn2, []PartitionKey{RootKey}, vector.T{0, 0}, &searchSet, partitionCounts)
			require.NoError(t, err)
			result1 := SearchResult{QuerySquaredDistance: 25, ErrorBound: 0, CentroidDistance: 5, ParentPartitionKey: RootKey, ChildKey: childKey10}
			require.Equal(t, SearchResults{result1}, searchSet.PopResults())
			require.Equal(t, 1, partitionCounts[0])

			wait.Done()
		}()

		// Add vector to root partition after yielding to the background goroutine.
		// The add should always happen before the background search.
		runtime.Gosched()
		_, err := store.AddToPartition(ctx, txn, RootKey, vector.T{3, 4}, childKey10)
		require.NoError(t, err)
	}()

	wait.Wait()
}

func TestInMemoryStoreUpdateStats(t *testing.T) {
	ctx := context.Background()

	// Insert root partition into new store.
	store := NewInMemoryStore(2, 42)
	quantizer := quantize.NewUnQuantizer(2)

	txn := beginTransaction(ctx, t, store)
	defer commitTransaction(ctx, t, store, txn)

	vectors := vector.MakeSet(2)
	quantizedSet := quantizer.Quantize(ctx, &vectors)
	root := NewPartition(quantizer, quantizedSet, []ChildKey{}, LeafLevel)
	require.NoError(t, store.SetRootPartition(ctx, txn, root))

	// Update stats.
	stats := IndexStats{
		VectorsPerPartition: 5,
		CVStats:             []CVStats{{Mean: 1.5, Variance: 0.5}, {Mean: 1, Variance: 0.25}},
	}
	err := store.MergeStats(ctx, &stats, false /* skipMerge */)
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.NumPartitions)
	require.Equal(t, float64(5), stats.VectorsPerPartition)
	require.Equal(t, []CVStats{}, stats.CVStats)

	// Upsert new root partition with higher level and check stats.
	root.level = 3
	require.NoError(t, store.SetRootPartition(ctx, txn, root))
	stats.VectorsPerPartition = 10
	stats.CVStats = []CVStats{{Mean: 2.5, Variance: 0.5}, {Mean: 1, Variance: 0.25}}
	err = store.MergeStats(ctx, &stats, false /* skipMerge */)
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.NumPartitions)
	require.Equal(t, float64(5.5), stats.VectorsPerPartition)
	require.Equal(t, []CVStats{{Mean: 2.5, Variance: 0}, {Mean: 1, Variance: 0}}, stats.CVStats)

	// Upsert new root partition with lower level and check stats.
	root.level = 2
	require.NoError(t, store.SetRootPartition(ctx, txn, root))
	stats.VectorsPerPartition = 20
	stats.CVStats = []CVStats{{Mean: 2.5, Variance: 0.5}, {Mean: 1, Variance: 0.25}}
	err = store.MergeStats(ctx, &stats, false /* skipMerge */)
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.NumPartitions)
	require.Equal(t, float64(6.95), stats.VectorsPerPartition)
	require.Equal(t, []CVStats{{Mean: 2.5, Variance: 0.05}}, stats.CVStats)

	// skipMerge = true.
	stats.VectorsPerPartition = 100
	stats.CVStats = []CVStats{{Mean: 10, Variance: 2}}
	err = store.MergeStats(ctx, &stats, true /* skipMerge */)
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.NumPartitions)
	require.Equal(t, float64(6.95), stats.VectorsPerPartition)
	require.Equal(t, []CVStats{{Mean: 2.5, Variance: 0.05}}, stats.CVStats)
}

func TestInMemoryStoreMarshalling(t *testing.T) {
	raBitQuantizer := quantize.NewRaBitQuantizer(2, 42)
	unquantizer := quantize.NewUnQuantizer(2)
	store := InMemoryStore{
		dims: 2,
		seed: 42,
	}
	store.mu.index = map[PartitionKey]*Partition{
		10: {
			quantizer: unquantizer,
			quantizedSet: &quantize.UnQuantizedVectorSet{
				Centroid:          []float32{4, 3},
				CentroidDistances: []float32{1, 2, 3},
				Vectors: vector.Set{
					Dims:  2,
					Count: 3,
					Data:  []float32{1, 2, 3, 4, 5, 6},
				},
			},
			childKeys: []ChildKey{{PartitionKey: 10}, {PartitionKey: 20}},
			level:     1,
		},
		20: {
			quantizer: raBitQuantizer,
			quantizedSet: &quantize.UnQuantizedVectorSet{
				Centroid:          []float32{4, 3},
				CentroidDistances: []float32{1, 2, 3, 4},
				Vectors: vector.Set{
					Dims:  2,
					Count: 3,
					Data:  []float32{1, 2, 3, 4, 5, 6, 7, 8},
				},
			},
			childKeys: []ChildKey{{PartitionKey: 10}, {PartitionKey: 20}, {PartitionKey: 30}},
			level:     2,
		},
	}
	store.mu.nextKey = 100
	store.mu.vectors = map[string]vector.T{
		string([]byte{1, 2}): {10, 11},
		string([]byte{3, 4}): {12, 13},
	}
	store.mu.stats = IndexStats{
		NumPartitions:       2,
		VectorsPerPartition: 100,
		CVStats:             []CVStats{{Mean: 0.5, Variance: 0.25}},
	}

	// Round-trip the data.
	data, err := store.MarshalBinary()
	require.NoError(t, err)

	var store2 InMemoryStore
	err = store2.UnmarshalBinary(data)
	require.NoError(t, err)

	require.Len(t, store2.mu.index, 2)
	require.Equal(t, Level(1), store2.mu.index[10].level)
	require.Equal(t, 3, store2.mu.index[10].quantizedSet.GetCount())
	require.Equal(t, 2, store2.mu.index[20].quantizer.GetOriginalDims())
	require.Len(t, store2.mu.index[20].childKeys, 3)
	require.Equal(t, PartitionKey(100), store2.mu.nextKey)
	require.Len(t, store2.mu.vectors, 2)
	require.Equal(t, vector.T{12, 13}, store2.mu.vectors[string([]byte{3, 4})])
	require.Equal(t, float64(100), store2.mu.stats.VectorsPerPartition)
	require.Equal(t, int64(42), store2.seed)
}

func beginTransaction(ctx context.Context, t *testing.T, store Store) Txn {
	txn, err := store.BeginTransaction(ctx)
	require.NoError(t, err)
	return txn
}

func commitTransaction(ctx context.Context, t *testing.T, store Store, txn Txn) {
	err := store.CommitTransaction(ctx, txn)
	require.NoError(t, err)
}

func abortTransaction(ctx context.Context, t *testing.T, store Store, txn Txn) {
	err := store.AbortTransaction(ctx, txn)
	require.NoError(t, err)
}
