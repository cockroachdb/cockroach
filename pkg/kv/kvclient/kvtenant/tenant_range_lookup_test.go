// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvtenant_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestRangeLookupPrefetchFiltering is an integration test to ensure that
// range results are filtered for the client.
func TestRangeLookupPrefetchFiltering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
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

	// Split some ranges within tenant2 that we'll want to see in prefetch.
	ten2Codec := keys.MakeSQLCodec(ten2ID)
	ten2Split1 := append(ten2Codec.TenantPrefix(), 'a')
	ten2Split2 := append(ten2Codec.TenantPrefix(), 'b')
	{
		tc.SplitRangeOrFatal(t, ten2Split1)
		tc.SplitRangeOrFatal(t, ten2Split2)
	}

	// Split some ranges for the tenant which comes after tenant2.
	{
		ten3Codec := keys.MakeSQLCodec(roachpb.MustMakeTenantID(3))
		tc.SplitRangeOrFatal(t, ten3Codec.TenantPrefix())
		tc.SplitRangeOrFatal(t, append(ten3Codec.TenantPrefix(), 'b'))
		tc.SplitRangeOrFatal(t, append(ten3Codec.TenantPrefix(), 'c'))
	}

	// Do the fetch and make sure we prefetch all the ranges we should see,
	// and none of the ranges we should not.
	db := tenant2.DistSenderI().(*kvcoord.DistSender).RangeDescriptorCache().DB()
	prefixRKey := keys.MustAddr(ten2Codec.TenantPrefix())
	res, prefetch, err := db.RangeLookup(
		ctx, prefixRKey,
		rangecache.ReadFromLeaseholder, false, /* useReverseScan */
	)
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, prefixRKey, res[0].StartKey)
	require.Len(t, prefetch, 2)
	require.Equal(t, keys.MustAddr(ten2Split1), prefetch[0].StartKey)
	require.Equal(t, keys.MustAddr(ten2Split2), prefetch[1].StartKey)
}
