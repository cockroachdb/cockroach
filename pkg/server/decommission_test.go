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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// This test should really be in server_test.go but can't because it uses the
// server package, which is also imported by testutils/testcluster, causing
// an import cycles.
func TestDecommissionNodeStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // saves time
	})
	defer tc.Stopper().Stop(ctx)
	decomNodeID := tc.Server(2).NodeID()

	// Wait for node status entries to have been created.
	testutils.SucceedsSoon(t, func() error {
		for _, srv := range tc.Servers {
			entry, err := srv.DB().Get(ctx, keys.NodeStatusKey(srv.NodeID()))
			if err != nil {
				return err
			} else if entry.Value == nil {
				return errors.Errorf("expected to find node status entry for %d", srv.NodeID())
			}
		}
		return nil
	})

	require.NoError(t, tc.Servers[0].Decommission(
		ctx, livenesspb.MembershipStatus_DECOMMISSIONING, []roachpb.NodeID{decomNodeID}))
	require.NoError(t, tc.Servers[0].Decommission(
		ctx, livenesspb.MembershipStatus_DECOMMISSIONED, []roachpb.NodeID{decomNodeID}))

	// The node status entries should shortly be cleaned up.
	testutils.SucceedsSoon(t, func() error {
		entry, err := tc.Server(0).DB().Get(ctx, keys.NodeStatusKey(decomNodeID))
		if err != nil {
			return err
		} else if entry.Value != nil {
			return errors.Errorf("found stale node status entry for %d", decomNodeID)
		}
		return nil
	})
}
