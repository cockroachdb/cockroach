// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvtenantccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/stretchr/testify/require"
)

// TestScanRangeDescriptors is an integration test to ensure that tenants can
// scan range descriptors iff they correspond to tenant owned ranges.
func TestScanRangeDescriptors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DisableDefaultTestTenant: true, // we're going to manually add tenants
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					DisableMergeQueue: true,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	ten2ID := roachpb.MustMakeTenantID(2)
	tenant2, err := tc.Server(0).StartTenant(ctx, base.TestTenantArgs{
		TenantID: ten2ID,
	})
	require.NoError(t, err)

	//Split some ranges within tenant2 that we'll scan over.
	ten2Codec := keys.MakeSQLCodec(ten2ID)
	ten2Split1 := append(ten2Codec.TenantPrefix(), 'a')
	ten2Split2 := append(ten2Codec.TenantPrefix(), 'b')
	{
		tc.SplitRangeOrFatal(t, ten2Split1)
		tc.SplitRangeOrFatal(t, ten2Split2)
		tc.SplitRangeOrFatal(t, ten2Codec.TenantPrefix().PrefixEnd()) // Last range
	}

	iteratorFactory := tenant2.RangeDescIteratorFactory().(rangedesc.IteratorFactory)
	iter, err := iteratorFactory.NewIterator(ctx, roachpb.Span{
		Key:    ten2Codec.TenantPrefix(),
		EndKey: ten2Codec.TenantPrefix().PrefixEnd(),
	})
	require.NoError(t, err)

	var rangeDescs []roachpb.RangeDescriptor
	for iter.Valid() {
		rangeDescs = append(rangeDescs, iter.CurRangeDescriptor())
		iter.Next()
	}

	require.Len(t, rangeDescs, 3)
	require.Equal(
		t,
		keys.MustAddr(ten2Codec.TenantPrefix()),
		rangeDescs[0].StartKey,
	)
	require.Equal(
		t,
		keys.MustAddr(ten2Split1),
		rangeDescs[1].StartKey,
	)
	require.Equal(
		t,
		keys.MustAddr(ten2Split2),
		rangeDescs[2].StartKey,
	)

	// Ensure the system tenant has access to all range descriptors, including
	// those belonging to secondary tenants.
	iteratorFactory = tc.Server(0).RangeDescIteratorFactory().(rangedesc.IteratorFactory)
	iter, err = iteratorFactory.NewIterator(ctx, keys.EverythingSpan)
	require.NoError(t, err)

	rangeDescs = rangeDescs[:0] // empty out.
	for iter.Valid() {
		rangeDescs = append(rangeDescs, iter.CurRangeDescriptor())
		iter.Next()
	}
	require.NoError(t, err)

	var numRanges int
	if err := tc.Server(0).GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
		numRanges = s.ReplicaCount()
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	require.Len(t, rangeDescs, numRanges)
	// Last range we created above.
	require.Equal(
		t,
		keys.MustAddr(ten2Codec.TenantPrefix().PrefixEnd()),
		rangeDescs[numRanges-1].StartKey,
	)
}
