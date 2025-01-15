// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/internal"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
)

func TestPersistentStore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := internal.WithWorkspace(context.Background(), &internal.Workspace{})
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(42)})
	defer s.Stopper().Stop(ctx)

	childKey2 := ChildKey{PartitionKey: 2}
	childKey10 := ChildKey{PartitionKey: 10}
	childKey20 := ChildKey{PartitionKey: 20}
	childKey30 := ChildKey{PartitionKey: 30}
	primaryKey200 := ChildKey{PrimaryKey: PrimaryKey{2, 00}}
	primaryKey300 := ChildKey{PrimaryKey: PrimaryKey{3, 00}}
	primaryKey400 := ChildKey{PrimaryKey: PrimaryKey{4, 00}}

	ten5Codec := keys.MakeSQLCodec(roachpb.MustMakeTenantID(5))
	prefix := rowenc.MakeIndexKeyPrefix(ten5Codec, 500, 42)
	quantizer := quantize.NewUnQuantizer(2)
	store := NewPersistentStore(kvDB, quantizer, prefix)

	// TODO(mw5h): Figure out where to create the empty root partition.
	t.Run("create empty root partition", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		emptyRoot := NewPartition(quantizer, quantizer.Quantize(ctx, &vector.Set{}), []ChildKey{}, LeafLevel)
		require.NoError(t, txn.SetRootPartition(ctx, emptyRoot))
	})

	commonStoreTests(ctx, t, store, quantizer)

	t.Run("insert a root partition into the store and read it back", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		vectors := vector.T{4, 3}.AsSet()
		quantizedSet := quantizer.Quantize(ctx, &vectors)
		root := NewPartition(quantizer, quantizedSet, []ChildKey{childKey2}, Level(2))
		require.NoError(t, txn.SetRootPartition(ctx, root))
		readRoot, err := txn.GetPartition(ctx, RootKey)
		require.NoError(t, err)
		testingAssertPartitionsEqual(t, root, readRoot)

		vectors = vector.T{4, 3}.AsSet()
		vectors.Add(vector.T{2, 1})
		quantizedSet = quantizer.Quantize(ctx, &vectors)
		root = NewPartition(quantizer, quantizedSet, []ChildKey{childKey10, childKey20}, Level(2))
		require.NoError(t, txn.SetRootPartition(ctx, root))
		readRoot, err = txn.GetPartition(ctx, RootKey)
		require.NoError(t, err)
		testingAssertPartitionsEqual(t, root, readRoot)

		vectors = vector.T{4, 3}.AsSet()
		vectors.Add(vector.T{2, 1})
		vectors.Add(vector.T{5, 6})
		quantizedSet = quantizer.Quantize(ctx, &vectors)
		root = NewPartition(quantizer, quantizedSet, []ChildKey{primaryKey200, primaryKey300, primaryKey400}, LeafLevel)
		require.NoError(t, txn.SetRootPartition(ctx, root))
		readRoot, err = txn.GetPartition(ctx, RootKey)
		require.NoError(t, err)
		testingAssertPartitionsEqual(t, root, readRoot)
	})

	t.Run("insert a partition and then delete it", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		vectors := vector.T{4, 3}.AsSet()
		quantizedSet := quantizer.Quantize(ctx, &vectors)
		testPartition := NewPartition(quantizer, quantizedSet, []ChildKey{childKey2}, Level(2))
		partitionKey, err := txn.InsertPartition(ctx, testPartition)
		require.NoError(t, err)
		newPartition, err := txn.GetPartition(ctx, partitionKey)
		require.NoError(t, err)
		testingAssertPartitionsEqual(t, testPartition, newPartition)

		err = txn.DeletePartition(ctx, partitionKey)
		require.NoError(t, err)
		_, err = txn.GetPartition(ctx, partitionKey)
		require.Error(t, err)
	})

	t.Run("add to root partition", func(t *testing.T) {
		txn := beginTransaction(ctx, t, store)
		defer commitTransaction(ctx, t, store, txn)

		emptySet := vector.MakeSet(2)
		root := NewPartition(quantizer, quantizer.Quantize(ctx, &emptySet), []ChildKey{}, Level(2))
		err := txn.SetRootPartition(ctx, root)
		require.NoError(t, err)

		// Add to root partition.
		count, err := txn.AddToPartition(ctx, RootKey, vector.T{1, 2}, childKey10)
		require.NoError(t, err)
		require.Equal(t, 1, count)
		count, err = txn.AddToPartition(ctx, RootKey, vector.T{7, 4}, childKey20)
		require.NoError(t, err)
		require.Equal(t, 2, count)
		count, err = txn.AddToPartition(ctx, RootKey, vector.T{4, 3}, childKey30)
		require.NoError(t, err)
		require.Equal(t, 3, count)

		// Add duplicate and expect value to be overwritten
		count, err = txn.AddToPartition(ctx, RootKey, vector.T{5, 5}, childKey30)
		require.NoError(t, err)
		require.Equal(t, 3, count)

		// Search root partition.
		searchSet := SearchSet{MaxResults: 2}
		partitionCounts := []int{0}
		level, err := txn.SearchPartitions(
			ctx, []PartitionKey{RootKey}, vector.T{1, 1}, &searchSet, partitionCounts)
		require.NoError(t, err)
		require.Equal(t, Level(2), level)
		result1 := SearchResult{QuerySquaredDistance: 1, ErrorBound: 0, CentroidDistance: 2.2361, ParentPartitionKey: 1, ChildKey: childKey10}
		result2 := SearchResult{QuerySquaredDistance: 32, ErrorBound: 0, CentroidDistance: 7.0711, ParentPartitionKey: 1, ChildKey: childKey30}
		results := searchSet.PopResults()
		roundResults(results, 4)
		require.Equal(t, SearchResults{result1, result2}, results)
		require.Equal(t, 3, partitionCounts[0])
	})
}
