// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memstore

import (
	"context"
	"encoding/binary"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/commontest"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gonum.org/v1/gonum/floats/scalar"
)

// testStore implements the commontest.TestStore interface.
type testStore struct {
	*Store
	inserted uint64
}

func (ts *testStore) AllowMultipleTrees() bool {
	return true
}

func (ts *testStore) MakeTreeKey(t *testing.T, treeID int) cspann.TreeKey {
	return ToTreeKey(TreeID(treeID))
}

func (ts *testStore) InsertVector(t *testing.T, treeID int, vec vector.T) cspann.KeyBytes {
	ts.inserted++
	key := make(cspann.KeyBytes, 8)
	binary.BigEndian.PutUint64(key, ts.inserted)
	ts.Store.InsertVector(key, vec)
	return key
}

func TestMemStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	makeStore := func(quantizer quantize.Quantizer) commontest.TestStore {
		return &testStore{Store: New(2, 42)}
	}

	suite.Run(t, commontest.NewStoreTestSuite(ctx, makeStore))

	store := New(2, 42)
	treeKey := ToTreeKey(TreeID(0))
	testPKs := []cspann.KeyBytes{{11}, {12}}
	testVectors := []vector.T{{100, 200}, {300, 400}}

	t.Run("insert and get all vectors", func(t *testing.T) {
		txn := commontest.BeginTransaction(ctx, t, store)
		defer commontest.CommitTransaction(ctx, t, store, txn)

		store.InsertVector(testPKs[0], testVectors[0])
		store.InsertVector(testPKs[1], testVectors[1])

		vectors := store.GetAllVectors()
		slices.SortFunc(vectors, func(a, b cspann.VectorWithKey) int {
			return a.Key.Compare(b.Key)
		})
		require.Equal(t, []cspann.VectorWithKey{
			{Key: cspann.ChildKey{KeyBytes: testPKs[0]}, Vector: testVectors[0]},
			{Key: cspann.ChildKey{KeyBytes: testPKs[1]}, Vector: testVectors[1]},
		}, vectors)
	})

	t.Run("delete full vector", func(t *testing.T) {
		txn := commontest.BeginTransaction(ctx, t, store)
		defer commontest.CommitTransaction(ctx, t, store, txn)

		store.DeleteVector([]byte{10})
		refs := []cspann.VectorWithKey{{Key: cspann.ChildKey{KeyBytes: cspann.KeyBytes{10}}}}
		err := txn.GetFullVectors(ctx, treeKey, refs)
		require.NoError(t, err)
		require.Nil(t, refs[0].Vector)
	})

	t.Run("abort transaction", func(t *testing.T) {
		txn := commontest.BeginTransaction(ctx, t, store)
		defer commontest.AbortTransaction(ctx, t, store, txn)

		// Perform some read-only operations.
		_, err := txn.GetPartition(ctx, treeKey, cspann.RootKey)
		require.NoError(t, err)

		err = txn.GetFullVectors(ctx, treeKey, []cspann.VectorWithKey{
			{Key: cspann.ChildKey{KeyBytes: testPKs[0]}},
		})
		require.NoError(t, err)
	})
}

func TestInMemoryStoreConcurrency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	childKey1 := cspann.ChildKey{PartitionKey: 10}
	childKey2 := cspann.ChildKey{PartitionKey: 20}
	valueBytes1 := cspann.ValueBytes{1, 2}
	valueBytes2 := cspann.ValueBytes{3, 4}

	// Insert root partition into new store.
	store := New(2, 42)
	treeKey := ToTreeKey(TreeID(0))

	var wait sync.WaitGroup
	wait.Add(1)
	func() {
		txn := commontest.BeginTransaction(ctx, t, store)
		defer commontest.CommitTransaction(ctx, t, store, txn)

		// Ensure the root partition has been created.
		_, err := txn.AddToPartition(
			ctx, treeKey, cspann.RootKey, vector.T{10, 10}, childKey1, valueBytes1)
		require.NoError(t, err)

		// Acquire partition lock.
		_, err = txn.GetPartition(ctx, treeKey, cspann.RootKey)
		require.NoError(t, err)

		// Search root partition on background goroutine.
		go func() {
			// Begin transaction should block until the outer transaction is
			// complete.
			txn2 := commontest.BeginTransaction(ctx, t, store)
			defer commontest.CommitTransaction(ctx, t, store, txn2)

			searchSet := cspann.SearchSet{MaxResults: 1}
			partitionCounts := []int{0}
			_, err := txn2.SearchPartitions(ctx, treeKey,
				[]cspann.PartitionKey{cspann.RootKey}, vector.T{0, 0}, &searchSet, partitionCounts)
			require.NoError(t, err)
			result1 := cspann.SearchResult{
				QuerySquaredDistance: 25, ErrorBound: 0, CentroidDistance: 5,
				ParentPartitionKey: cspann.RootKey, ChildKey: childKey2, ValueBytes: valueBytes2}
			require.Equal(t, cspann.SearchResults{result1}, searchSet.PopResults())
			require.Equal(t, 2, partitionCounts[0])

			wait.Done()
		}()

		// Add vector to root partition after yielding to the background goroutine.
		// The add should always happen before the background search.
		runtime.Gosched()
		_, err = txn.AddToPartition(
			ctx, treeKey, cspann.RootKey, vector.T{3, 4}, childKey2, valueBytes2)
		require.NoError(t, err)
	}()

	wait.Wait()
}

func TestInMemoryStoreUpdateStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var workspace workspace.T
	store := New(2, 42)
	quantizer := quantize.NewUnQuantizer(2)
	treeKey := ToTreeKey(TreeID(0))

	txn := commontest.BeginTransaction(ctx, t, store)
	defer commontest.CommitTransaction(ctx, t, store, txn)

	childKey10 := cspann.ChildKey{PartitionKey: 10}
	childKey20 := cspann.ChildKey{PartitionKey: 20}
	childKey30 := cspann.ChildKey{PartitionKey: 30}
	childKey40 := cspann.ChildKey{PartitionKey: 40}

	valueBytes10 := cspann.ValueBytes{1, 2}
	valueBytes20 := cspann.ValueBytes{3, 4}
	valueBytes30 := cspann.ValueBytes{5, 6}
	valueBytes40 := cspann.ValueBytes{7, 8}

	_, err := txn.AddToPartition(ctx, treeKey, cspann.RootKey, vector.T{1, 2}, childKey10, valueBytes10)
	require.NoError(t, err)
	_, err = txn.AddToPartition(ctx, treeKey, cspann.RootKey, vector.T{3, 4}, childKey20, valueBytes20)
	require.NoError(t, err)

	// Update stats.
	stats := cspann.IndexStats{
		CVStats: []cspann.CVStats{{Mean: 1.5, Variance: 0.5}, {Mean: 1, Variance: 0.25}}}
	err = store.MergeStats(ctx, &stats, false /* skipMerge */)
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.NumPartitions)
	require.Equal(t, float64(1.05), stats.VectorsPerPartition)
	require.Equal(t, []cspann.CVStats{}, stats.CVStats)

	// Upsert new root partition with higher level and check stats.
	oldRoot, err := txn.GetPartition(ctx, treeKey, cspann.RootKey)
	require.NoError(t, err)
	newRoot := cspann.NewPartition(oldRoot.Quantizer(), oldRoot.QuantizedSet(),
		oldRoot.ChildKeys(), oldRoot.ValueBytes(), cspann.Level(3))
	require.NoError(t, txn.SetRootPartition(ctx, treeKey, newRoot))
	stats.CVStats = []cspann.CVStats{{Mean: 2.5, Variance: 0.5}, {Mean: 1, Variance: 0.25}}
	err = store.MergeStats(ctx, &stats, false /* skipMerge */)
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.NumPartitions)
	require.Equal(t, float64(1.0975), stats.VectorsPerPartition)
	require.Equal(t, []cspann.CVStats{{Mean: 2.5, Variance: 0}, {Mean: 1, Variance: 0}}, roundCVStats(stats.CVStats))

	// Insert new partition with lower level and check stats.
	vectors := vector.MakeSetFromRawData([]float32{5, 6}, 2)
	quantizedSet := quantizer.Quantize(&workspace, vectors)
	partition := cspann.NewPartition(quantizer, quantizedSet,
		[]cspann.ChildKey{childKey30}, []cspann.ValueBytes{valueBytes30}, 2)
	partitionKey, err := txn.InsertPartition(ctx, treeKey, partition)
	require.NoError(t, err)

	stats.CVStats = []cspann.CVStats{{Mean: 8, Variance: 2}, {Mean: 6, Variance: 1}}
	err = store.MergeStats(ctx, &stats, false /* skipMerge */)
	require.NoError(t, err)
	require.Equal(t, int64(2), stats.NumPartitions)
	require.Equal(t, float64(1.0926), scalar.Round(stats.VectorsPerPartition, 4))
	require.Equal(t, []cspann.CVStats{
		{Mean: 2.775, Variance: 0.1}, {Mean: 1.25, Variance: 0.05}}, roundCVStats(stats.CVStats))

	// Add vector to partition and check stats.
	_, err = txn.AddToPartition(ctx, treeKey, partitionKey, vector.T{7, 8}, childKey40, valueBytes40)
	require.NoError(t, err)

	stats.CVStats = []cspann.CVStats{{Mean: 3, Variance: 1}, {Mean: 1.5, Variance: 0.5}}
	err = store.MergeStats(ctx, &stats, false /* skipMerge */)
	require.NoError(t, err)
	require.Equal(t, int64(2), stats.NumPartitions)
	require.Equal(t, float64(1.1380), scalar.Round(stats.VectorsPerPartition, 4))
	require.Equal(t, []cspann.CVStats{
		{Mean: 2.7863, Variance: 0.145}, {Mean: 1.2625, Variance: 0.0725}}, roundCVStats(stats.CVStats))

	// Remove vector from partition and check stats.
	_, err = txn.RemoveFromPartition(ctx, treeKey, partitionKey, childKey30)
	require.NoError(t, err)

	stats.CVStats = []cspann.CVStats{{Mean: 5, Variance: 2}, {Mean: 3, Variance: 1.5}}
	err = store.MergeStats(ctx, &stats, false /* skipMerge */)
	require.NoError(t, err)
	require.Equal(t, int64(2), stats.NumPartitions)
	require.Equal(t, float64(1.1311), scalar.Round(stats.VectorsPerPartition, 4))
	require.Equal(t, []cspann.CVStats{
		{Mean: 2.8969, Variance: 0.2378}, {Mean: 1.3494, Variance: 0.1439}}, roundCVStats(stats.CVStats))

	// skipMerge = true.
	stats.CVStats = []cspann.CVStats{{Mean: 10, Variance: 2}}
	err = store.MergeStats(ctx, &stats, true /* skipMerge */)
	require.NoError(t, err)
	require.Equal(t, int64(2), stats.NumPartitions)
	require.Equal(t, float64(1.1311), scalar.Round(stats.VectorsPerPartition, 4))
	require.Equal(t, []cspann.CVStats{
		{Mean: 2.8969, Variance: 0.2378}, {Mean: 1.3494, Variance: 0.1439}}, roundCVStats(stats.CVStats))
}

func TestInMemoryStoreMarshalling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	raBitQuantizer := quantize.NewRaBitQuantizer(2, 42)
	unquantizer := quantize.NewUnQuantizer(2)
	store := New(2, 42)
	store.mu.partitions = make(map[qualifiedPartitionKey]*memPartition)

	memPart := &memPartition{}
	memPart.lock.partition = cspann.NewPartition(
		unquantizer,
		&quantize.UnQuantizedVectorSet{
			Centroid:          []float32{4, 3},
			CentroidDistances: []float32{1, 2, 3},
			Vectors: vector.Set{
				Dims:  2,
				Count: 3,
				Data:  []float32{1, 2, 3, 4, 5, 6},
			},
		},
		[]cspann.ChildKey{{PartitionKey: 10}, {PartitionKey: 20}},
		[]cspann.ValueBytes{{1, 2}, {3, 4}},
		cspann.Level(1))
	qkey10 := makeQualifiedPartitionKey(ToTreeKey(1), 10)
	store.mu.partitions[qkey10] = memPart

	memPart = &memPartition{}
	memPart.lock.partition = cspann.NewPartition(
		raBitQuantizer,
		&quantize.UnQuantizedVectorSet{
			Centroid:          []float32{4, 3},
			CentroidDistances: []float32{1, 2, 3, 4},
			Vectors: vector.Set{
				Dims:  2,
				Count: 3,
				Data:  []float32{1, 2, 3, 4, 5, 6, 7, 8},
			},
		},
		[]cspann.ChildKey{{PartitionKey: 10}, {PartitionKey: 20}, {PartitionKey: 30}},
		[]cspann.ValueBytes{{1, 2}, {3, 4}, {5, 6}},
		cspann.Level(2))
	qkey20 := makeQualifiedPartitionKey(ToTreeKey(1), 20)
	store.mu.partitions[qkey20] = memPart

	store.mu.nextKey = 100
	store.mu.vectors = map[string]vector.T{
		string([]byte{1, 2}): {10, 11},
		string([]byte{3, 4}): {12, 13},
	}
	store.mu.stats = cspann.IndexStats{
		NumPartitions:       2,
		VectorsPerPartition: 100,
		CVStats:             []cspann.CVStats{{Mean: 0.5, Variance: 0.25}},
	}

	// Round-trip the data.
	data, err := store.MarshalBinary()
	require.NoError(t, err)

	store2, err := Load(data)
	require.NoError(t, err)

	require.Len(t, store2.mu.partitions, 2)
	require.Equal(t, qkey10, store2.mu.partitions[qkey10].key)
	require.Equal(t, uint64(1), store2.mu.partitions[qkey10].lock.created)
	require.Equal(t, cspann.Level(1), store2.mu.partitions[qkey10].lock.partition.Level())
	require.Equal(t, 3, store2.mu.partitions[qkey10].lock.partition.QuantizedSet().GetCount())
	require.Equal(t, 2, store2.mu.partitions[qkey20].lock.partition.Quantizer().GetDims())
	require.Len(t, store2.mu.partitions[qkey20].lock.partition.ChildKeys(), 3)
	require.Len(t, store2.mu.partitions[qkey20].lock.partition.ValueBytes(), 3)
	require.Equal(t, cspann.PartitionKey(100), store2.mu.nextKey)
	require.Len(t, store2.mu.vectors, 2)
	require.Equal(t, vector.T{12, 13}, store2.mu.vectors[string([]byte{3, 4})])
	require.Equal(t, float64(100), store2.mu.stats.VectorsPerPartition)
	require.Equal(t, int64(42), store2.seed)
}

func TestInMemoryLock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("lock reentrancy", func(t *testing.T) {
		var l memLock
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
		var l memLock
		l.AcquireShared(1)
		l.AcquireShared(2)
		require.False(t, l.IsAcquiredBy(1))
		require.False(t, l.IsAcquiredBy(2))
		l.ReleaseShared()
		l.ReleaseShared()
	})

	t.Run("exclusive lock waits for exclusive lock", func(t *testing.T) {
		var l memLock
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
		var l memLock
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
		var l memLock
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

func roundCVStats(cvstats []cspann.CVStats) []cspann.CVStats {
	for i := range cvstats {
		cvstats[i].Mean = scalar.Round(cvstats[i].Mean, 4)
		cvstats[i].Variance = scalar.Round(cvstats[i].Variance, 4)
	}
	return cvstats
}
