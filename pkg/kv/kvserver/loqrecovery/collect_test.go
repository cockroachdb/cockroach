// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package loqrecovery

import (
	"context"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestAcceptableDescriptorChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, td := range []struct {
		name         string
		seq          []descriptorChangeType
		expectResult bool
	}{
		{name: "no pending events", seq: []descriptorChangeType{}, expectResult: true},
		{
			name:         "descriptor commit in progress",
			seq:          []descriptorChangeType{descriptorLockClear},
			expectResult: true,
		},
		{
			name: "descriptor committed change",
			seq: []descriptorChangeType{
				descriptorLockUpdate,
				descriptorUpdate,
				descriptorLockClear,
			},
			expectResult: false,
		},
		{
			name:         "descriptor uncommitted change",
			seq:          []descriptorChangeType{descriptorLockUpdate, descriptorUpdate},
			expectResult: false,
		},
		{name: "raft update", seq: []descriptorChangeType{raftChange}, expectResult: false},
		{
			name:         "raft after commit in progress",
			seq:          []descriptorChangeType{descriptorLockClear, raftChange},
			expectResult: false,
		},
	} {
		t.Run(td.name, func(t *testing.T) {
			require.Equal(t, td.expectResult, isUpdateAllowed(td.seq))
		})
	}
}

// TestFindUpdateDescriptor verifies that we can detect changes to range descriptor in the raft log.
// To do this we split the range which updates descriptor prior to spawning RHS.
func TestFindUpdateDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	const testNode = 0

	checkRaftLog(t, ctx, testNode,
		func(ctx context.Context, tc *testcluster.TestCluster) (roachpb.RangeID, roachpb.RKey) {
			scratchKey, err := tc.Server(0).ScratchRange()
			require.NoError(t, err, "failed to get scratch range")
			srk, err := keys.Addr(scratchKey)
			require.NoError(t, err, "failed to resolve scratch key")

			rd, err := tc.LookupRange(scratchKey)
			require.NoError(t, err, "failed to get descriptor for scratch range")

			splitKey := testutils.MakeKey(scratchKey, []byte("z"))
			_, _, err = tc.SplitRange(splitKey)
			require.NoError(t, err, "failed to split scratch range")

			require.NoError(t,
				tc.Server(testNode).DB().Put(ctx, testutils.MakeKey(scratchKey, []byte("|first")),
					"some data"),
				"failed to put test value in LHS")

			return rd.RangeID, srk
		},
		func(t *testing.T, ctx context.Context, rangeID roachpb.RangeID, startKey roachpb.RKey,
			engine storage.Engine,
		) {
			seq, err := getDescriptorChangesFromRaftLog(rangeID, roachpb.Key(startKey), 0,
				math.MaxInt64, engine)
			require.NoError(t, err, "failed to read raft log data")
			require.Equal(t,
				[]descriptorChangeType{descriptorLockUpdate, descriptorUpdate, descriptorLockClear}, seq,
				"descriptor change sequence")
		})
}

// TestFindUpdateRaft verifies that we can detect raft change commands in the raft log. To do this
// we change number of replicas and then assert if raftChange updates are found in event sequence.
func TestFindUpdateRaft(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	const testNode = 0

	checkRaftLog(t, ctx, testNode,
		func(ctx context.Context, tc *testcluster.TestCluster) (roachpb.RangeID, roachpb.RKey) {
			scratchKey, err := tc.Server(0).ScratchRange()
			require.NoError(t, err, "failed to get scratch range")
			srk, err := keys.Addr(scratchKey)
			require.NoError(t, err, "failed to resolve scratch key")

			rd, err := tc.LookupRange(scratchKey)
			require.NoError(t, err, "failed to get descriptor for scratch range")

			tc.RemoveVotersOrFatal(t, scratchKey, tc.Targets(1, 2)...)

			require.NoError(t,
				tc.Server(testNode).DB().Put(ctx, testutils.MakeKey(scratchKey, []byte("|first")),
					"some data"),
				"failed to put test value in range")

			return rd.RangeID, srk
		},
		func(t *testing.T, ctx context.Context, rangeID roachpb.RangeID, startKey roachpb.RKey,
			engine storage.Engine,
		) {
			seq, err := getDescriptorChangesFromRaftLog(rangeID, roachpb.Key(startKey), 0,
				math.MaxInt64, engine)
			require.NoError(t, err, "failed to read raft log data")
			require.Contains(t, seq, raftChange, "no raft changes detected despite voter removals")
		})
}

func checkRaftLog(
	t *testing.T,
	ctx context.Context,
	nodeToMonitor int,
	action func(ctx context.Context, tc *testcluster.TestCluster) (roachpb.RangeID, roachpb.RKey),
	checker func(*testing.T, context.Context, roachpb.RangeID, roachpb.RKey, storage.Engine),
) {
	t.Helper()
	type captureRequest struct {
		rangeID  roachpb.RangeID
		startKey roachpb.RKey
		store    storage.Engine
	}

	makeSnapshot := make(chan captureRequest)
	checkerFinished := make(chan bool)

	raftFilter := func(args kvserverbase.ApplyFilterArgs) (int, *roachpb.Error) {
		t.Helper()
		select {
		case req := <-makeSnapshot:
			defer func() { checkerFinished <- true }()
			checker(t, ctx, req.rangeID, req.startKey, req.store)
		default:
		}
		return 0, nil
	}

	tc := testcluster.NewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{{InMemory: true}},
			RaftConfig: base.RaftConfig{
				// High enough interval to be longer than test but not overflow duration.
				RaftTickInterval:           math.MaxInt32,
				RaftElectionTimeoutTicks:   1000000,
				RaftLogTruncationThreshold: math.MaxInt64,
			},
			Insecure: true,
		},
		ReplicationMode: base.ReplicationAuto,
		ServerArgsPerNode: map[int]base.TestServerArgs{
			nodeToMonitor: {
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						TestingApplyFilter: raftFilter,
					},
				},
				StoreSpecs: []base.StoreSpec{{InMemory: true}},
				RaftConfig: base.RaftConfig{
					RaftTickInterval:           math.MaxInt32,
					RaftElectionTimeoutTicks:   1000000,
					RaftLogTruncationThreshold: math.MaxInt64,
				},
				Insecure: true,
			},
		},
	})

	tc.Start(t)
	defer tc.Stopper().Stop(ctx)

	require.NoError(t, tc.WaitForFullReplication(), "failed to up-replicate")

	rid, skey := action(ctx, tc)

	eng := tc.GetFirstStoreFromServer(t, nodeToMonitor).Engine()
	makeSnapshot <- captureRequest{rid, skey, eng}
	// After the test action is complete raft might be completely caught up with
	// its messages, so we will write a value into the range to ensure filter
	// fires up at least once after we requested capture.
	require.NoError(t,
		tc.Server(0).DB().Put(ctx, testutils.MakeKey(skey, []byte("second")), "some data"),
		"failed to put test value")
	ok := <-checkerFinished
	require.True(t, ok, "captured raft log for range")
}
