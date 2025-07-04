// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestMultiMetricRebalancerBasic tests that the multi-metric store rebalancer
// doesn't cause a panic when rebalancing, i.e. a smoke test.
func TestMultiMetricRebalancerBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	t.Run("1_node", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
		defer tc.Stopper().Stop(context.Background())
		require.NoError(t, tc.WaitForFullReplication())
		tc.GetFirstStoreFromServer(t, 0).TestingMMStoreRebalance(ctx)
	})

	t.Run("5_node", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, 5, base.TestClusterArgs{})
		defer tc.Stopper().Stop(context.Background())
		require.NoError(t, tc.WaitForFullReplication())
		tc.GetFirstStoreFromServer(t, 0).TestingMMStoreRebalance(ctx)
	})
}
