// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestAtomicMembershipChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This is a simple smoke test to spot obvious issues with atomic replica changes.
	// These aren't implemented at the time of writing. The compound change below is
	// internally unwound and executed step by step (see executeAdminBatch()).
	ctx := context.Background()

	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	}
	tc := testcluster.StartTestCluster(t, 4, args)
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	expDesc, err := tc.AddReplicas(k, tc.Target(1), tc.Target(2))
	require.NoError(t, err)

	// Range is now on s1,s2,s3. "Atomically" add it to s4 while removing from s3.
	// This isn't really atomic yet.

	chgs := []roachpb.ReplicationChange{
		{ChangeType: roachpb.ADD_REPLICA, Target: tc.Target(3)},
		{ChangeType: roachpb.REMOVE_REPLICA, Target: tc.Target(2)},
	}
	// TODO(tbg): when 19.2 is out, remove this "feature gate" here and in
	// AdminChangeReplicas.
	ctx = context.WithValue(ctx, "testing", "testing")
	desc, err := tc.Servers[0].DB().AdminChangeReplicas(ctx, k, expDesc, chgs)
	require.NoError(t, err)
	var stores []roachpb.StoreID
	for _, rDesc := range desc.Replicas().All() {
		stores = append(stores, rDesc.StoreID)
	}
	exp := []roachpb.StoreID{1, 2, 4}
	// TODO(tbg): test more details and scenarios (learners etc).
	require.Equal(t, exp, stores)
}
