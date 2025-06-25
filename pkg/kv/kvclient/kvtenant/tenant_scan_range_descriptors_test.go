// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvtenant_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/stretchr/testify/require"
)

func setup(
	t *testing.T, ctx context.Context,
) (*testcluster.TestCluster, serverutils.ApplicationLayerInterface, rangedesc.IteratorFactory) {
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		},
	})

	ten3ID := roachpb.MustMakeTenantID(3)
	tenant3, err := tc.Server(0).TenantController().StartTenant(ctx, base.TestTenantArgs{
		TenantID: ten3ID,
	})
	require.NoError(t, err)
	return tc, tenant3, tenant3.RangeDescIteratorFactory().(rangedesc.IteratorFactory)
}

// TestScanRangeDescriptors is an integration test to ensure that tenants can
// scan range descriptors iff they correspond to tenant owned ranges.
func TestScanRangeDescriptors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, tenant3, iteratorFactory := setup(t, ctx)
	defer tc.Stopper().Stop(ctx)

	// Split some ranges within tenant2 that we'll scan over.
	ten3Codec := tenant3.Codec()
	ten3Split1 := append(ten3Codec.TenantPrefix(), 'a')
	ten3Split2 := append(ten3Codec.TenantPrefix(), 'b')
	{
		tc.SplitRangeOrFatal(t, ten3Split1)
		tc.SplitRangeOrFatal(t, ten3Split2)
		tc.SplitRangeOrFatal(t, ten3Codec.TenantEndKey()) // Last range
	}

	iter, err := iteratorFactory.NewIterator(ctx, ten3Codec.TenantSpan())
	require.NoError(t, err)

	var rangeDescs []roachpb.RangeDescriptor
	for iter.Valid() {
		rangeDescs = append(rangeDescs, iter.CurRangeDescriptor())
		iter.Next()
	}

	require.Len(t, rangeDescs, 3)
	require.Equal(
		t,
		keys.MustAddr(ten3Codec.TenantPrefix()),
		rangeDescs[0].StartKey,
	)
	require.Equal(
		t,
		keys.MustAddr(ten3Split1),
		rangeDescs[1].StartKey,
	)
	require.Equal(
		t,
		keys.MustAddr(ten3Split2),
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
		keys.MustAddr(ten3Codec.TenantEndKey()),
		rangeDescs[numRanges-1].StartKey,
	)
}

func TestScanRangeDescriptorsOutsideTenantKeyspace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, _, iteratorFactory := setup(t, ctx)
	defer tc.Stopper().Stop(ctx)

	_, err := iteratorFactory.NewIterator(ctx, keys.EverythingSpan)
	require.ErrorContains(t, err, "requested key span /M{in-ax} not fully contained in tenant keyspace /Tenant/{3-4}")
}
