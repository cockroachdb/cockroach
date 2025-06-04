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
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
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

func (ts *testStore) SupportsTry() bool {
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

func (ts *testStore) Close(t *testing.T) {
}

func TestMemStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	makeStore := func(quantizer quantize.Quantizer) commontest.TestStore {
		return &testStore{Store: New(quantizer, 42)}
	}

	suite.Run(t, commontest.NewStoreTestSuite(ctx, makeStore))

	quantizer := quantize.NewRaBitQuantizer(2, 42, vecpb.L2SquaredDistance)
	store := New(quantizer, 42)
	treeKey := ToTreeKey(TreeID(0))
	testPKs := []cspann.KeyBytes{{11}, {12}}
	testVectors := []vector.T{{100, 200}, {300, 400}}

	t.Run("insert and get all vectors", func(t *testing.T) {
		commontest.RunTransaction(ctx, t, store, func(txn cspann.Txn) {
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
	})

	t.Run("delete full vector", func(t *testing.T) {
		commontest.RunTransaction(ctx, t, store, func(txn cspann.Txn) {
			store.DeleteVector([]byte{10})
			refs := []cspann.VectorWithKey{{Key: cspann.ChildKey{KeyBytes: cspann.KeyBytes{10}}}}
			err := txn.GetFullVectors(ctx, treeKey, refs)
			require.NoError(t, err)
			require.Nil(t, refs[0].Vector)
		})
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

	quantizer := quantize.NewRaBitQuantizer(2, 42, vecpb.L2SquaredDistance)
	store := New(quantizer, 42)
	treeKey := ToTreeKey(TreeID(0))

	// Construct an empty root partition.
	metadata := store.makeEmptyRootMetadata()
	require.NoError(t, store.TryCreateEmptyPartition(ctx, treeKey, cspann.RootKey, metadata))

	var wait sync.WaitGroup
	wait.Add(1)
	commontest.RunTransaction(ctx, t, store, func(txn cspann.Txn) {
		// Ensure the root partition has been created.
		err := txn.AddToPartition(ctx, treeKey, cspann.RootKey, cspann.LeafLevel,
			vector.T{10, 10}, childKey1, valueBytes1)
		require.NoError(t, err)

		// Acquire partition lock.
		_, err = txn.GetPartitionMetadata(ctx, treeKey, cspann.RootKey, true /* forUpdate */)
		require.NoError(t, err)

		// Search root partition on background goroutine. The transaction should
		// not begin until the outer transaction is complete.
		go func() {
			defer wait.Done()
			commontest.RunTransaction(ctx, t, store, func(txn2 cspann.Txn) {
				searchSet := cspann.SearchSet{MaxResults: 1}
				toSearch := []cspann.PartitionToSearch{{Key: cspann.RootKey}}
				err := txn2.SearchPartitions(ctx, treeKey, toSearch, vector.T{0, 0}, &searchSet)
				require.NoError(t, err)
				result1 := cspann.SearchResult{
					QueryDistance: 25, ErrorBound: 0,
					ParentPartitionKey: cspann.RootKey, ChildKey: childKey2, ValueBytes: valueBytes2}
				require.Equal(t, cspann.SearchResults{result1}, searchSet.PopResults())
				require.Equal(t, 2, toSearch[0].Count)
			})
		}()

		// Add vector to root partition after yielding to the background goroutine.
		// The add should always happen before the background search.
		runtime.Gosched()
		err = txn.AddToPartition(
			ctx, treeKey, cspann.RootKey, cspann.LeafLevel, vector.T{3, 4}, childKey2, valueBytes2)
		require.NoError(t, err)
	})

	wait.Wait()
	if t.Failed() {
		t.FailNow()
	}
}

func TestInMemoryStoreUpdateStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	quantizer := quantize.NewUnQuantizer(2, vecpb.L2SquaredDistance)
	store := New(quantizer, 42)
	treeKey := ToTreeKey(TreeID(0))

	// Construct an empty root partition.
	metadata := store.makeEmptyRootMetadata()
	require.NoError(t, store.TryCreateEmptyPartition(ctx, treeKey, cspann.RootKey, metadata))

	commontest.RunTransaction(ctx, t, store, func(txn cspann.Txn) {
		childKey10 := cspann.ChildKey{PartitionKey: 10}
		childKey20 := cspann.ChildKey{PartitionKey: 20}

		valueBytes10 := cspann.ValueBytes{1, 2}
		valueBytes20 := cspann.ValueBytes{3, 4}

		err := txn.AddToPartition(
			ctx, treeKey, cspann.RootKey, cspann.LeafLevel, vector.T{1, 2}, childKey10, valueBytes10)
		require.NoError(t, err)
		err = txn.AddToPartition(
			ctx, treeKey, cspann.RootKey, cspann.LeafLevel, vector.T{3, 4}, childKey20, valueBytes20)
		require.NoError(t, err)

		// Update stats.
		stats := cspann.IndexStats{
			CVStats: []cspann.CVStats{{Mean: 1.5, Variance: 0.5}, {Mean: 1, Variance: 0.25}}}
		err = store.MergeStats(ctx, &stats, false /* skipMerge */)
		require.NoError(t, err)
		require.Equal(t, int64(1), stats.NumPartitions)
		require.Equal(t, []cspann.CVStats{}, stats.CVStats)

		// Increase root partition level and check stats.
		metadata, err := store.TryGetPartitionMetadata(ctx, treeKey, cspann.RootKey)
		require.NoError(t, err)

		expected := metadata
		metadata.Level = 3
		err = store.TryUpdatePartitionMetadata(ctx, treeKey, cspann.RootKey, metadata, expected)
		require.NoError(t, err)

		stats.CVStats = []cspann.CVStats{{Mean: 2.5, Variance: 0.5}, {Mean: 1, Variance: 0.25}}
		err = store.MergeStats(ctx, &stats, false /* skipMerge */)
		require.NoError(t, err)
		require.Equal(t, int64(1), stats.NumPartitions)
		expectedStats := []cspann.CVStats{{Mean: 2.5, Variance: 0}, {Mean: 1, Variance: 0}}
		require.Equal(t, expectedStats, roundCVStats(stats.CVStats))

		// Decrease root partition level, create a new partition, and check stats.
		expected = metadata
		metadata.Level = 2
		err = store.TryUpdatePartitionMetadata(ctx, treeKey, cspann.RootKey, metadata, expected)
		require.NoError(t, err)

		partitionKey := store.MakePartitionKey()
		nonRootMetadata := cspann.MakeReadyPartitionMetadata(3, vector.T{1, 2})
		err = store.TryCreateEmptyPartition(ctx, treeKey, partitionKey, nonRootMetadata)
		require.NoError(t, err)

		stats.CVStats = []cspann.CVStats{{Mean: 8, Variance: 2}, {Mean: 6, Variance: 1}}
		err = store.MergeStats(ctx, &stats, false /* skipMerge */)
		require.NoError(t, err)
		require.Equal(t, int64(2), stats.NumPartitions)
		require.Equal(t, []cspann.CVStats{{Mean: 2.775, Variance: 0.1}}, roundCVStats(stats.CVStats))

		// skipMerge = true.
		stats.CVStats = []cspann.CVStats{{Mean: 10, Variance: 2}}
		err = store.MergeStats(ctx, &stats, true /* skipMerge */)
		require.NoError(t, err)
		require.Equal(t, int64(2), stats.NumPartitions)
		require.Equal(t, []cspann.CVStats{{Mean: 2.775, Variance: 0.1}}, roundCVStats(stats.CVStats))
	})
}

func TestInMemoryStoreMarshalling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	raBitQuantizer := quantize.NewRaBitQuantizer(2, 42, vecpb.CosineDistance)
	unquantizer := quantize.NewUnQuantizer(2, vecpb.L2SquaredDistance)
	store := New(raBitQuantizer, 42)
	store.mu.partitions = make(map[qualifiedPartitionKey]*memPartition)
	centroid := []float32{4, 3}

	memPart := &memPartition{}
	memPart.lock.partition = cspann.NewPartition(
		cspann.MakeReadyPartitionMetadata(1, centroid),
		unquantizer,
		&quantize.UnQuantizedVectorSet{
			Vectors: vector.Set{
				Dims:  2,
				Count: 3,
				Data:  []float32{1, 2, 3, 4, 5, 6},
			},
		},
		[]cspann.ChildKey{{PartitionKey: 10}, {PartitionKey: 20}},
		[]cspann.ValueBytes{{1, 2}, {3, 4}})
	qkey10 := makeQualifiedPartitionKey(ToTreeKey(1), 10)
	store.mu.partitions[qkey10] = memPart

	memPart = &memPartition{}
	memPart.lock.partition = cspann.NewPartition(
		cspann.MakeReadyPartitionMetadata(2, centroid),
		raBitQuantizer,
		&quantize.UnQuantizedVectorSet{
			Vectors: vector.Set{
				Dims:  2,
				Count: 3,
				Data:  []float32{1, 2, 3, 4, 5, 6, 7, 8},
			},
		},
		[]cspann.ChildKey{{PartitionKey: 10}, {PartitionKey: 20}, {PartitionKey: 30}},
		[]cspann.ValueBytes{{1, 2}, {3, 4}, {5, 6}})
	qkey20 := makeQualifiedPartitionKey(ToTreeKey(1), 20)
	store.mu.partitions[qkey20] = memPart

	store.mu.nextKey = 100
	store.mu.vectors = map[string]vector.T{
		string([]byte{1, 2}): {10, 11},
		string([]byte{3, 4}): {12, 13},
	}
	store.mu.stats = cspann.IndexStats{
		NumPartitions: 2,
		CVStats:       []cspann.CVStats{{Mean: 0.5, Variance: 0.25}},
	}

	// Round-trip the data.
	data, err := store.MarshalBinary()
	require.NoError(t, err)

	store2, err := Load(data)
	require.NoError(t, err)

	require.NotNil(t, store2.rootQuantizer)
	require.Equal(t, vecpb.CosineDistance, store2.rootQuantizer.GetDistanceMetric())
	require.NotNil(t, store2.quantizer)
	require.Equal(t, vecpb.CosineDistance, store2.quantizer.GetDistanceMetric())
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

	t.Run("uniqueOwner does not support reentrancy", func(t *testing.T) {
		var l memLock
		l.Acquire(uniqueOwner)
		require.False(t, l.IsAcquiredBy(uniqueOwner))

		var acquired atomic.Bool
		go func() {
			l.Acquire(uniqueOwner)
			acquired.Store(true)
			l.Release()
		}()
		go func() {
			l.AcquireShared(uniqueOwner)
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
