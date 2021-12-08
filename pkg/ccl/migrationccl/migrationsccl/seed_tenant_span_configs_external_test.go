// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrationsccl_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestSeedTenantSpanConfigs tests that the migration installs relevant seed
// span configs for existing secondary tenants.
func TestSeedTenantSpanConfigs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			EnableSpanConfigs: true, // we use spanconfig.KVAccessor to check if its contents are as we'd expect
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.SeedTenantSpanConfigs - 1,
					),
				},
			},
		},
	})

	defer tc.Stopper().Stop(ctx)
	ts := tc.Server(0)
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	tenantID := roachpb.MakeTenantID(10)
	_, err := ts.StartTenant(ctx, base.TestTenantArgs{TenantID: tenantID})
	require.NoError(t, err)

	scKVAccessor := ts.SpanConfigKVAccessor().(spanconfig.KVAccessor)
	tenantPrefix := keys.MakeTenantPrefix(tenantID)
	tenantSpan := roachpb.Span{Key: tenantPrefix, EndKey: tenantPrefix.PrefixEnd()}
	tenantSeedSpan := roachpb.Span{Key: tenantPrefix, EndKey: tenantPrefix.Next()}

	{
		entries, err := scKVAccessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{
			tenantSpan,
		})
		require.NoError(t, err)
		require.Empty(t, entries)
	}

	tdb.Exec(t,
		"SET CLUSTER SETTING version = $1",
		clusterversion.ByKey(clusterversion.SeedTenantSpanConfigs).String(),
	)

	{
		entries, err := scKVAccessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{
			tenantSpan,
		})
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, entries[0].Span, tenantSeedSpan)
	}
}

// TestSeedSpanConfigsWrittenWhenActive tests that seed span configs are written
// to for fresh tenants if the cluster version introduce it is already active.
func TestSeedSpanConfigsWrittenWhenActive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			EnableSpanConfigs: true, // we use spanconfig.KVAccessor to check if its contents are as we'd expect
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.SeedTenantSpanConfigs,
					),
				},
			},
		},
	})

	defer tc.Stopper().Stop(ctx)
	ts := tc.Server(0)

	tenantID := roachpb.MakeTenantID(10)
	_, err := ts.StartTenant(ctx, base.TestTenantArgs{TenantID: tenantID})
	require.NoError(t, err)

	scKVAccessor := ts.SpanConfigKVAccessor().(spanconfig.KVAccessor)
	tenantPrefix := keys.MakeTenantPrefix(tenantID)
	tenantSpan := roachpb.Span{Key: tenantPrefix, EndKey: tenantPrefix.PrefixEnd()}
	tenantSeedSpan := roachpb.Span{Key: tenantPrefix, EndKey: tenantPrefix.Next()}

	{
		entries, err := scKVAccessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{
			tenantSpan,
		})
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, entries[0].Span, tenantSeedSpan)
	}
}
