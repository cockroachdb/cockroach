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
	primaryKey200 := ChildKey{PrimaryKey: PrimaryKey{2, 00}}
	primaryKey300 := ChildKey{PrimaryKey: PrimaryKey{3, 00}}
	primaryKey400 := ChildKey{PrimaryKey: PrimaryKey{4, 00}}

	ten5Codec := keys.MakeSQLCodec(roachpb.MustMakeTenantID(5))
	prefix := rowenc.MakeIndexKeyPrefix(ten5Codec, 500, 42)
	quantizer := quantize.NewUnQuantizer(2)
	store := NewPersistentStore(kvDB, quantizer, prefix)

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
}
