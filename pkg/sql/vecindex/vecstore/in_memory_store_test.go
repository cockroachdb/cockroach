// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"context"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/internal"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/floats/scalar"
)

func TestInMemoryStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

		vectors := store.GetAllVectors()
		slices.SortFunc(vectors, func(a, b VectorWithKey) int {
			return a.Key.Compare(b.Key)
		})
		require.Equal(t, []VectorWithKey{
			{Key: ChildKey{PrimaryKey: PrimaryKey{11}}, Vector: vector.T{100, 200}},
			{Key: ChildKey{PrimaryKey: PrimaryKey{12}}, Vector: vector.T{300, 400}},
		}, vectors)
	})

	t.Run("search empty root partition", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

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

		// Add duplicate and expect value to be overwritten
		count, err = store.AddToPartition(ctx, txn, RootKey, vector.T{5, 5}, childKey30)
		require.NoError(t, err)
		require.Equal(t, 3, count)

		// Search root partition.
		searchSet := SearchSet{MaxResults: 2}
		partitionCounts := []int{0}
		level, err := store.SearchPartitions(
			ctx, txn, []PartitionKey{RootKey}, vector.T{1, 1}, &searchSet, partitionCounts)
		require.NoError(t, err)
		require.Equal(t, Level(1), level)
		result1 := SearchResult{QuerySquaredDistance: 1, ErrorBound: 0, CentroidDistance: 2.2361, ParentPartitionKey: 1, ChildKey: childKey10}
		result2 := SearchResult{QuerySquaredDistance: 32, ErrorBound: 0, CentroidDistance: 7.0711, ParentPartitionKey: 1, ChildKey: childKey30}
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
		_, err := store.GetPartition(ctx, txn, RootKey)
		require.NoError(t, err)
		vectors := vector.T{4, 3}.AsSet()
		quantizedSet := quantizer.Quantize(ctx, &vectors)
		newRoot := NewPartition(quantizer, quantizedSet, []ChildKey{childKey2}, 2)
		require.NoError(t, store.SetRootPartition(ctx, txn, newRoot))
		newRoot, err = store.GetPartition(ctx, txn, RootKey)
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

		_, err := store.GetPartition(ctx, txn, RootKey)
		require.NoError(t, err)
		partitionKey1, err = store.InsertPartition(ctx, txn, root)
		require.NoError(t, err)
		require.Equal(t, PartitionKey(2), partitionKey1)
		count, err := store.RemoveFromPartition(ctx, txn, partitionKey1, childKey20)
		require.NoError(t, err)
		require.Equal(t, 2, count)

		// Try to remove the same key again.
		count, err = store.RemoveFromPartition(ctx, txn, partitionKey1, childKey20)
		require.NoError(t, err)
		require.Equal(t, 2, count)

		// Add an alternate element and add duplicate, expecting value to be overwritten.
		count, err = store.AddToPartition(ctx, txn, partitionKey1, vector.T{-1, 0}, childKey40)
		require.NoError(t, err)
		require.Equal(t, 3, count)
		count, err = store.AddToPartition(ctx, txn, partitionKey1, vector.T{1, 1}, childKey40)
		require.NoError(t, err)
		require.Equal(t, 3, count)

		searchSet := SearchSet{MaxResults: 2}
		partitionCounts := []int{0}
		level, err := store.SearchPartitions(
			ctx, txn, []PartitionKey{partitionKey1}, vector.T{1, 1}, &searchSet, partitionCounts)
		require.NoError(t, err)
		require.Equal(t, Level(1), level)
		result4 := SearchResult{QuerySquaredDistance: 0, ErrorBound: 0, CentroidDistance: 1.4142, ParentPartitionKey: 2, ChildKey: childKey40}
		result5 := SearchResult{QuerySquaredDistance: 1, ErrorBound: 0, CentroidDistance: 2.2361, ParentPartitionKey: 2, ChildKey: childKey10}
		require.Equal(t, SearchResults{result4, result5}, roundResults(searchSet.PopResults(), 4))
		require.Equal(t, 3, partitionCounts[0])
	})

	t.Run("search multiple partitions at leaf level", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		_, err := store.GetPartition(ctx, txn, RootKey)
		require.NoError(t, err)

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
		result4 := SearchResult{QuerySquaredDistance: 4, ErrorBound: 0, CentroidDistance: 1.41, ParentPartitionKey: 2, ChildKey: childKey40}
		result5 := SearchResult{QuerySquaredDistance: 5, ErrorBound: 0, CentroidDistance: 2.24, ParentPartitionKey: 2, ChildKey: childKey10}
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
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := internal.WithWorkspace(context.Background(), &internal.Workspace{})

	childKey10 := ChildKey{PartitionKey: 10}

	// Insert root partition into new store.
	store := NewInMemoryStore(2, 42)

	var wait sync.WaitGroup
	wait.Add(1)
	func() {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		// Acquire partition lock.
		_, err := store.GetPartition(ctx, txn, RootKey)
		require.NoError(t, err)

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
		_, err = store.AddToPartition(ctx, txn, RootKey, vector.T{3, 4}, childKey10)
		require.NoError(t, err)
	}()

	wait.Wait()
}

func TestInMemoryStoreUpdateStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	store := NewInMemoryStore(2, 42)
	quantizer := quantize.NewUnQuantizer(2)

	txn := beginTransaction(ctx, t, store)
	defer commitTransaction(ctx, t, store, txn)

	childKey10 := ChildKey{PartitionKey: 10}
	childKey20 := ChildKey{PartitionKey: 20}
	childKey30 := ChildKey{PartitionKey: 30}
	childKey40 := ChildKey{PartitionKey: 40}

	_, err := store.AddToPartition(ctx, txn, RootKey, vector.T{1, 2}, childKey10)
	require.NoError(t, err)
	_, err = store.AddToPartition(ctx, txn, RootKey, vector.T{3, 4}, childKey20)
	require.NoError(t, err)

	// Update stats.
	stats := IndexStats{CVStats: []CVStats{{Mean: 1.5, Variance: 0.5}, {Mean: 1, Variance: 0.25}}}
	err = store.MergeStats(ctx, &stats, false /* skipMerge */)
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.NumPartitions)
	require.Equal(t, float64(1.05), stats.VectorsPerPartition)
	require.Equal(t, []CVStats{}, stats.CVStats)

	// Upsert new root partition with higher level and check stats.
	root, err := store.GetPartition(ctx, txn, RootKey)
	require.NoError(t, err)
	root.level = 3
	require.NoError(t, store.SetRootPartition(ctx, txn, root))
	stats.CVStats = []CVStats{{Mean: 2.5, Variance: 0.5}, {Mean: 1, Variance: 0.25}}
	err = store.MergeStats(ctx, &stats, false /* skipMerge */)
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.NumPartitions)
	require.Equal(t, float64(1.0975), stats.VectorsPerPartition)
	require.Equal(t, []CVStats{{Mean: 2.5, Variance: 0}, {Mean: 1, Variance: 0}}, roundCVStats(stats.CVStats))

	// Insert new partition with lower level and check stats.
	vectors := vector.MakeSetFromRawData([]float32{5, 6}, 2)
	quantizedSet := quantizer.Quantize(ctx, &vectors)
	partition := NewPartition(quantizer, quantizedSet, []ChildKey{childKey30}, 2)
	partitionKey, err := store.InsertPartition(ctx, txn, partition)
	require.NoError(t, err)

	stats.CVStats = []CVStats{{Mean: 8, Variance: 2}, {Mean: 6, Variance: 1}}
	err = store.MergeStats(ctx, &stats, false /* skipMerge */)
	require.NoError(t, err)
	require.Equal(t, int64(2), stats.NumPartitions)
	require.Equal(t, float64(1.0926), scalar.Round(stats.VectorsPerPartition, 4))
	require.Equal(t, []CVStats{{Mean: 2.775, Variance: 0.1}, {Mean: 1.25, Variance: 0.05}}, roundCVStats(stats.CVStats))

	// Add vector to partition and check stats.
	_, err = store.AddToPartition(ctx, txn, partitionKey, vector.T{7, 8}, childKey40)
	require.NoError(t, err)

	stats.CVStats = []CVStats{{Mean: 3, Variance: 1}, {Mean: 1.5, Variance: 0.5}}
	err = store.MergeStats(ctx, &stats, false /* skipMerge */)
	require.NoError(t, err)
	require.Equal(t, int64(2), stats.NumPartitions)
	require.Equal(t, float64(1.1380), scalar.Round(stats.VectorsPerPartition, 4))
	require.Equal(t, []CVStats{{Mean: 2.7863, Variance: 0.145}, {Mean: 1.2625, Variance: 0.0725}}, roundCVStats(stats.CVStats))

	// Remove vector from partition and check stats.
	_, err = store.RemoveFromPartition(ctx, txn, partitionKey, childKey30)
	require.NoError(t, err)

	stats.CVStats = []CVStats{{Mean: 5, Variance: 2}, {Mean: 3, Variance: 1.5}}
	err = store.MergeStats(ctx, &stats, false /* skipMerge */)
	require.NoError(t, err)
	require.Equal(t, int64(2), stats.NumPartitions)
	require.Equal(t, float64(1.1311), scalar.Round(stats.VectorsPerPartition, 4))
	require.Equal(t, []CVStats{{Mean: 2.8969, Variance: 0.2378}, {Mean: 1.3494, Variance: 0.1439}}, roundCVStats(stats.CVStats))

	// skipMerge = true.
	stats.CVStats = []CVStats{{Mean: 10, Variance: 2}}
	err = store.MergeStats(ctx, &stats, true /* skipMerge */)
	require.NoError(t, err)
	require.Equal(t, int64(2), stats.NumPartitions)
	require.Equal(t, float64(1.1311), scalar.Round(stats.VectorsPerPartition, 4))
	require.Equal(t, []CVStats{{Mean: 2.8969, Variance: 0.2378}, {Mean: 1.3494, Variance: 0.1439}}, roundCVStats(stats.CVStats))
}

func TestInMemoryStoreMarshalling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	raBitQuantizer := quantize.NewRaBitQuantizer(2, 42)
	unquantizer := quantize.NewUnQuantizer(2)
	store := InMemoryStore{
		dims: 2,
		seed: 42,
	}
	store.mu.partitions = make(map[PartitionKey]*inMemoryPartition)

	inMemPartition := &inMemoryPartition{}
	inMemPartition.lock.partition = &Partition{
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
	}
	store.mu.partitions[10] = inMemPartition

	inMemPartition = &inMemoryPartition{}
	inMemPartition.lock.partition = &Partition{
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
	}
	store.mu.partitions[20] = inMemPartition

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

	store2, err := LoadInMemoryStore(data)
	require.NoError(t, err)

	require.Len(t, store2.mu.partitions, 2)
	require.Equal(t, PartitionKey(10), store2.mu.partitions[10].key)
	require.Equal(t, uint64(1), store2.mu.partitions[10].lock.created)
	require.Equal(t, Level(1), store2.mu.partitions[10].lock.partition.level)
	require.Equal(t, 3, store2.mu.partitions[10].lock.partition.quantizedSet.GetCount())
	require.Equal(t, 2, store2.mu.partitions[20].lock.partition.quantizer.GetOriginalDims())
	require.Len(t, store2.mu.partitions[20].lock.partition.childKeys, 3)
	require.Equal(t, PartitionKey(100), store2.mu.nextKey)
	require.Len(t, store2.mu.vectors, 2)
	require.Equal(t, vector.T{12, 13}, store2.mu.vectors[string([]byte{3, 4})])
	require.Equal(t, float64(100), store2.mu.stats.VectorsPerPartition)
	require.Equal(t, int64(42), store2.seed)
}

func TestInMemoryLock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("lock reentrancy", func(t *testing.T) {
		var l inMemoryLock
		l.Acquire(1)
		require.True(t, l.IsAcquiredBy(1))
		l.AcquireShared(1)
		l.AcquireShared(1)
		l.Acquire(1)
		require.True(t, l.IsAcquiredBy(1))
		l.Release()
		l.ReleaseShared()
		l.ReleaseShared()
		l.Release()
		require.False(t, l.IsAcquiredBy(1))
	})

	t.Run("shared lock does not wait for shared lock", func(t *testing.T) {
		var l inMemoryLock
		l.AcquireShared(1)
		l.AcquireShared(2)
		require.False(t, l.IsAcquiredBy(1))
		require.False(t, l.IsAcquiredBy(2))
		l.ReleaseShared()
		l.ReleaseShared()
	})

	t.Run("exclusive lock waits for exclusive lock", func(t *testing.T) {
		var l inMemoryLock
		l.Acquire(1)

		var acquired atomic.Bool
		go func() {
			l.Acquire(2)
			acquired.Store(true)
			l.Release()
		}()

		runtime.Gosched()
		require.False(t, acquired.Load())

		l.Release()
	})

	t.Run("exclusive lock waits for shared lock", func(t *testing.T) {
		var l inMemoryLock
		l.AcquireShared(1)

		var acquired atomic.Bool
		go func() {
			l.Acquire(2)
			acquired.Store(true)
			l.Release()
		}()

		runtime.Gosched()
		require.False(t, acquired.Load())
		l.ReleaseShared()
	})

	t.Run("shared lock waits for exclusive lock", func(t *testing.T) {
		var l inMemoryLock
		l.Acquire(1)

		var acquired atomic.Bool
		go func() {
			l.AcquireShared(2)
			acquired.Store(true)
			l.ReleaseShared()
		}()

		runtime.Gosched()
		require.False(t, acquired.Load())

		l.Release()
	})
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

func roundCVStats(cvstats []CVStats) []CVStats {
	for i := range cvstats {
		cvstats[i].Mean = scalar.Round(cvstats[i].Mean, 4)
		cvstats[i].Variance = scalar.Round(cvstats[i].Variance, 4)
	}
	return cvstats
}
