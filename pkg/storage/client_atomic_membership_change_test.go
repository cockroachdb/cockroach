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
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/quorum"
	"go.etcd.io/etcd/raft/tracker"
)

// TestAtomicMembershipChange is a simple smoke test for atomic membership
// changes.
func TestAtomicMembershipChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	}
	tc := testcluster.StartTestCluster(t, 6, args)
	defer tc.Stopper().Stop(ctx)

	// Create a range and put it on n1, n2, n3. Intentionally do this one at a
	// time so we're not using atomic replication changes yet.
	k := tc.ScratchRange(t)
	expDesc, err := tc.AddReplicas(k, tc.Target(1))
	require.NoError(t, err)
	expDesc, err = tc.AddReplicas(k, tc.Target(2))
	require.NoError(t, err)

	// Range is now on s1,s2,s3. Atomically add s4 and remove s3.

	chgs := []roachpb.ReplicationChange{
		{ChangeType: roachpb.ADD_REPLICA, Target: tc.Target(3)},
		{ChangeType: roachpb.ADD_REPLICA, Target: tc.Target(5)},
		{ChangeType: roachpb.REMOVE_REPLICA, Target: tc.Target(2)},
		{ChangeType: roachpb.ADD_REPLICA, Target: tc.Target(4)},
	}

	repl, err := tc.Servers[0].Stores().GetReplicaForRangeID(expDesc.RangeID)
	require.NoError(t, err)
	desc, err := repl.ChangeReplicas(ctx, &expDesc, storage.SnapshotRequest_REBALANCE, storagepb.ReasonRebalance, "testing", chgs)
	require.NoError(t, err)

	// TODO(tbg): switch to this branch once AdminChangeReplicas generically uses
	// atomic replication changes. At the tiem of writing, it executes each change
	// individually.
	if false {
		desc, err := tc.Servers[0].DB().AdminChangeReplicas(
			// TODO(tbg): when 19.2 is out, remove this "feature gate" here and in
			// AdminChangeReplicas.
			context.WithValue(ctx, "testing", "testing"),
			k, expDesc, chgs,
		)
		var _ = desc // pacify linters
		require.NoError(t, err)
	}

	var stores []roachpb.StoreID
	for _, rDesc := range desc.Replicas().All() {
		if rDesc.GetType() == roachpb.ReplicaType_LEARNER {
			t.Fatalf("found a learner: %+v", desc)
		}
		stores = append(stores, rDesc.StoreID)
	}
	sort.Slice(stores, func(i, j int) bool { return stores[i] < stores[j] })
	exp := []roachpb.StoreID{1, 2, 4, 5, 6}
	require.Equal(t, exp, stores)

	descJointKey := keys.RangeDescriptorJointKey(desc.StartKey)
	testutils.SucceedsSoon(t, func() error {
		for _, idx := range []int{0, 1, 3, 4, 5} {
			// Verify that all replicas leave the joint config automatically (raft does
			// this).
			repl, err := tc.Servers[idx].Stores().GetReplicaForRangeID(expDesc.RangeID)
			require.NoError(t, err)
			act := repl.RaftStatus().Config
			exp := tracker.Config{
				Voters: quorum.JointConfig{
					{1: {}, 2: {}, 4: {}, 5: {}, 6: {}},
					{},
				},
			}
			if sl := pretty.Diff(exp, act); len(sl) > 0 {
				return errors.Errorf("exp != act:\n%s", strings.Join(sl, "\n"))
			}
			// Verify that on a replica which has the correct config, we also see that the
			// joint config is no longer persisted. This is deleted *just* before the conf
			// change is handed to Raft, so the test fails instantly at this point if there
			// is still a joint range descriptor.
			var desc roachpb.RangeDescriptor
			ok, err := engine.MVCCGetProto(ctx, repl.Engine(), descJointKey, hlc.Timestamp{}, &desc, engine.MVCCGetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatalf("still have joint descriptor %+v", desc)
			}
		}
		return nil
	})
}
