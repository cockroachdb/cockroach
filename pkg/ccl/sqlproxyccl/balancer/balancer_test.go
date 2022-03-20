// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package balancer_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/balancer"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant/testutils"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestBalancer currently tests that it calls the directory with the right
// arguments. Once we add more logic to the balancer, we'll update this
// accordingly. We don't handle the error case here since the tests in
// proxy_handler_test.go should handle that.
func TestBalancer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tenantID := roachpb.MakeTenantID(10)
	const clusterName = "foo-bar"

	testDir := &testutils.TestDirectory{
		EnsureTenantAddrFn: func(
			fnCtx context.Context,
			fnTenantID roachpb.TenantID,
			fnClusterName string,
		) (string, error) {
			require.Equal(t, ctx, fnCtx)
			require.Equal(t, tenantID, fnTenantID)
			require.Equal(t, clusterName, fnClusterName)
			return "127.0.0.10:42", nil
		},
		LookupTenantAddrsFn: func(
			fnCtx context.Context,
			fnTenantID roachpb.TenantID,
		) ([]string, error) {
			require.Equal(t, ctx, fnCtx)
			require.Equal(t, tenantID, fnTenantID)
			return []string{"127.0.0.10:42"}, nil
		},
		ReportFailureFn: func(
			fnCtx context.Context,
			fnTenantID roachpb.TenantID,
			fnAddr string,
		) error {
			require.Equal(t, ctx, fnCtx)
			require.Equal(t, tenantID, fnTenantID)
			require.Equal(t, "127.0.0.10:42", fnAddr)
			return nil
		},
	}
	b := balancer.NewBalancer(testDir)

	addr, err := b.ChoosePodAddr(ctx, tenantID, clusterName)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.10:42", addr)

	var addrs []string
	addrs, err = b.ListPodAddrs(ctx, tenantID)
	require.NoError(t, err)
	require.Equal(t, []string{"127.0.0.10:42"}, addrs)

	err = b.ReportFailure(ctx, tenantID, "127.0.0.10:42")
	require.NoError(t, err)

	ensureCount, lookupCount, reportCount := testDir.Counts()
	require.Equal(t, 1, ensureCount)
	require.Equal(t, 1, lookupCount)
	require.Equal(t, 1, reportCount)
}
