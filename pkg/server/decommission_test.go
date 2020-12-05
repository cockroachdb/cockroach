// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// This test should really be in server_test.go but can't because it uses the
// server package, which is also imported by testutils/testcluster, causing
// an import cycle.
func TestDecommissionNodeStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // saves time
	})
	defer tc.Stopper().Stop(ctx)
	decomNodeID := tc.Server(2).NodeID()

	// Make sure node status entries have been created.
	for _, srv := range tc.Servers {
		entry, err := srv.DB().Get(ctx, keys.NodeStatusKey(srv.NodeID()))
		require.NoError(t, err)
		require.NotNil(t, entry.Value, "node status entry not found for node %d", srv.NodeID())
	}

	require.NoError(t, tc.Servers[0].Decommission(
		ctx, livenesspb.MembershipStatus_DECOMMISSIONING, []roachpb.NodeID{decomNodeID}))
	require.NoError(t, tc.Servers[0].Decommission(
		ctx, livenesspb.MembershipStatus_DECOMMISSIONED, []roachpb.NodeID{decomNodeID}))

	// The node status entry should now have been cleaned up.
	entry, err := tc.Server(0).DB().Get(ctx, keys.NodeStatusKey(decomNodeID))
	require.NoError(t, err)
	require.Nil(t, entry.Value, "found stale node status entry for node %d", decomNodeID)
}
