// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftutil"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func predIncoming(rDesc roachpb.ReplicaDescriptor) bool {
	return rDesc.Type == roachpb.VOTER_INCOMING
}
func predOutgoing(rDesc roachpb.ReplicaDescriptor) bool {
	return rDesc.Type == roachpb.VOTER_OUTGOING
}

func predDemotingToLearner(rDesc roachpb.ReplicaDescriptor) bool {
	return rDesc.Type == roachpb.VOTER_DEMOTING_LEARNER
}

type replicationTestKnobs struct {
	storeKnobs                       kvserver.StoreTestingKnobs
	replicaAddStopAfterLearnerAtomic int64
	replicaAddStopAfterJointConfig   int64
	replicationAlwaysUseJointConfig  int64
}

func (rtl *replicationTestKnobs) withStopAfterLearnerAtomic(f func()) {
	prev := atomic.SwapInt64(&rtl.replicaAddStopAfterLearnerAtomic, 1)
	defer atomic.StoreInt64(&rtl.replicaAddStopAfterLearnerAtomic, prev)
	f()
}

func (rtl *replicationTestKnobs) withStopAfterJointConfig(f func()) {
	au := atomic.SwapInt64(&rtl.replicationAlwaysUseJointConfig, 1)
	sa := atomic.SwapInt64(&rtl.replicaAddStopAfterJointConfig, 1)
	defer atomic.StoreInt64(&rtl.replicationAlwaysUseJointConfig, au)
	defer atomic.StoreInt64(&rtl.replicaAddStopAfterJointConfig, sa)
	f()
}

func makeReplicationTestKnobs() (base.TestingKnobs, *replicationTestKnobs) {
	var k replicationTestKnobs
	k.storeKnobs.VoterAddStopAfterLearnerSnapshot = func(_ []roachpb.ReplicationTarget) bool {
		return atomic.LoadInt64(&k.replicaAddStopAfterLearnerAtomic) > 0
	}
	k.storeKnobs.VoterAddStopAfterJointConfig = func() bool {
		return atomic.LoadInt64(&k.replicaAddStopAfterJointConfig) > 0
	}
	k.storeKnobs.ReplicationAlwaysUseJointConfig = func() bool {
		return atomic.LoadInt64(&k.replicationAlwaysUseJointConfig) > 0
	}
	return base.TestingKnobs{Store: &k.storeKnobs}, &k
}

func getFirstStoreReplica(
	t *testing.T, s serverutils.TestServerInterface, key roachpb.Key,
) (*kvserver.Store, *kvserver.Replica) {
	t.Helper()
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	var repl *kvserver.Replica
	testutils.SucceedsSoon(t, func() error {
		repl = store.LookupReplica(roachpb.RKey(key))
		if repl == nil {
			return errors.New(`could not find replica`)
		}
		return nil
	})
	return store, repl
}

// Some of the metrics used in these tests live on the queue objects and are
// registered with of storage.StoreMetrics instead of living on it. Example:
// queue.replicate.removelearnerreplica.
//
// TODO(dan): Move things like ReplicateQueueMetrics to be a field on
// storage.StoreMetrics and just keep a reference in newReplicateQueue. Ditto
// for other queues that do this.
func getFirstStoreMetric(t *testing.T, s serverutils.TestServerInterface, name string) int64 {
	t.Helper()
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	var c int64
	var found bool
	store.Registry().Each(func(n string, v interface{}) {
		if name == n {
			switch t := v.(type) {
			case *metric.Counter:
				c = t.Count()
				found = true
			case *metric.Gauge:
				c = t.Value()
				found = true
			}
		}
	})
	if !found {
		panic(fmt.Sprintf("couldn't find metric %s", name))
	}
	return c
}

func TestAddReplicaViaLearner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// The happy case! \o/

	blockUntilSnapshotCh := make(chan struct{})
	var receivedSnap int64
	blockSnapshotsCh := make(chan struct{})
	knobs, ltk := makeReplicationTestKnobs()
	ltk.storeKnobs.ReceiveSnapshot = func(h *kvserverpb.SnapshotRequest_Header) error {
		if atomic.CompareAndSwapInt64(&receivedSnap, 0, 1) {
			close(blockUntilSnapshotCh)
		} else {
			// Do nothing. We aren't interested in subsequent snapshots.
			return nil
		}
		select {
		case <-blockSnapshotsCh:
		case <-time.After(10 * time.Second):
			return errors.New(`test timed out`)
		}
		return nil
	}
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)
	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	scratchStartKey := tc.ScratchRange(t)

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		_, err := tc.AddVoters(scratchStartKey, tc.Target(1))
		return err
	})

	// Wait until the snapshot starts, which happens after the learner has been
	// added.
	<-blockUntilSnapshotCh
	desc := tc.LookupRangeOrFatal(t, scratchStartKey)
	require.Len(t, desc.Replicas().VoterDescriptors(), 1)
	require.Len(t, desc.Replicas().LearnerDescriptors(), 1)

	var voters, nonVoters string
	db.QueryRow(t,
		`SELECT array_to_string(replicas, ','), array_to_string(learner_replicas, ',') FROM crdb_internal.ranges_no_leases WHERE range_id = $1`,
		desc.RangeID,
	).Scan(&voters, &nonVoters)
	require.Equal(t, `1`, voters)
	require.Equal(t, `2`, nonVoters)

	// Unblock the snapshot and let the learner get promoted to a voter.
	close(blockSnapshotsCh)
	require.NoError(t, g.Wait())

	desc = tc.LookupRangeOrFatal(t, scratchStartKey)
	require.Len(t, desc.Replicas().VoterDescriptors(), 2)
	require.Len(t, desc.Replicas().LearnerDescriptors(), 0)
	require.Equal(t, int64(1), getFirstStoreMetric(t, tc.Server(1), `range.snapshots.applied-initial`))
}

func TestAddRemoveNonVotingReplicasBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	scratchStartKey := tc.ScratchRange(t)
	g, _ := errgroup.WithContext(ctx)
	g.Go(func() error {
		_, err := tc.AddNonVoters(scratchStartKey, tc.Target(1))
		return err
	})
	require.NoError(t, g.Wait())

	desc := tc.LookupRangeOrFatal(t, scratchStartKey)
	require.Len(t, desc.Replicas().NonVoterDescriptors(), 1)

	_, err := tc.RemoveNonVoters(scratchStartKey, tc.Target(1))
	require.NoError(t, err)
	desc = tc.LookupRangeOrFatal(t, scratchStartKey)
	require.NoError(t, tc.WaitForFullReplication())
	require.Len(t, desc.Replicas().NonVoterDescriptors(), 0)
}

// TestAddReplicaWithReceiverThrottling tests that outgoing snapshots on the
// delegated sender will throttle if incoming snapshots on the recipients are
// blocked.
func TestAddReplicaWithReceiverThrottling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// blockIncomingSnapshots will block receiving snapshots.
	blockIncomingSnapshots := make(chan struct{})
	waitForRebalanceToBlockCh := make(chan struct{})
	activateBlocking := int64(1)
	var count int64
	knobs, ltk := makeReplicationTestKnobs()
	ltk.storeKnobs.ReceiveSnapshot = func(h *kvserverpb.SnapshotRequest_Header) error {
		if atomic.LoadInt64(&activateBlocking) > 0 {
			// Signal waitForRebalanceToBlockCh to indicate the testing knob was hit.
			close(waitForRebalanceToBlockCh)
			blockIncomingSnapshots <- struct{}{}
		}
		return nil
	}
	ltk.storeKnobs.ThrottleEmptySnapshots = true
	ltk.storeKnobs.BeforeSendSnapshotThrottle = func() {
		atomic.AddInt64(&count, 1)
	}
	ltk.storeKnobs.AfterSendSnapshotThrottle = func() {
		atomic.AddInt64(&count, -1)
	}
	ctx := context.Background()
	tc := testcluster.StartTestCluster(
		t, 3, base.TestClusterArgs{
			ServerArgs:      base.TestServerArgs{Knobs: knobs, SnapshotSendLimit: 1},
			ReplicationMode: base.ReplicationManual,
		},
	)

	defer tc.Stopper().Stop(ctx)
	scratch := tc.ScratchRange(t)
	replicationChange := make(chan error, 2)
	g := ctxgroup.WithContext(ctx)

	// Add a non-voter to the range and expect it to block on blockIncomingSnapshots.
	g.GoCtx(
		func(ctx context.Context) error {
			desc, err := tc.LookupRange(scratch)
			if err != nil {
				return err
			}
			_, err = tc.Servers[0].DB().AdminChangeReplicas(ctx, scratch, desc,
				roachpb.MakeReplicationChanges(roachpb.ADD_NON_VOTER, tc.Target(2)),
			)
			replicationChange <- err
			return err
		},
	)

	select {
	case <-waitForRebalanceToBlockCh:
		// First replication change has hit the testing knob, continue with adding
		// second voter.
	case <-replicationChange:
		t.Fatal("did not expect the replication change to complete")
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for rebalance to block")
	}

	g.GoCtx(
		func(ctx context.Context) error {
			desc, err := tc.LookupRange(scratch)
			if err != nil {
				return err
			}
			_, err = tc.Servers[0].DB().AdminChangeReplicas(
				ctx, scratch, desc, roachpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(1)),
			)
			replicationChange <- err
			return err
		},
	)

	require.Eventually(
		t, func() bool {
			// Check that there is 1 snapshot waiting on the snapshot send semaphore,
			// as the other snapshot should currently be throttled in the semaphore.
			return atomic.LoadInt64(&count) == int64(1)
		}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond,
	)
	// Expect that the replication change is blocked on the channel, and the
	// snapshot is still throttled on the send snapshot semaphore.
	select {
	case <-time.After(1 * time.Second):
		require.Equalf(t, atomic.LoadInt64(&count), int64(1), "expected snapshot to still be blocked.")
	case <-replicationChange:
		t.Fatal("did not expect the replication change to complete")
	}

	// Disable the testing knob for blocking recipient snapshots to finish test.
	atomic.StoreInt64(&activateBlocking, 0)
	<-blockIncomingSnapshots

	// Wait for the goroutines to finish.
	require.NoError(t, g.Wait())
}

func TestLearnerRaftConfState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	verifyLearnerInRaftOnNodes := func(
		key roachpb.Key, id roachpb.ReplicaID, servers []*server.TestServer,
	) {
		t.Helper()
		var repls []*kvserver.Replica
		for _, s := range servers {
			_, repl := getFirstStoreReplica(t, s, key)
			repls = append(repls, repl)
		}
		testutils.SucceedsSoon(t, func() error {
			for _, repl := range repls {
				status := repl.RaftStatus()
				if status == nil {
					return errors.Errorf(`%s is still waking up`, repl)
				}
				if _, ok := status.Config.Learners[uint64(id)]; !ok {
					return errors.Errorf(`%s thinks %d is not a learner`, repl, id)
				}
			}
			return nil
		})
	}

	// Run the TestCluster with a known datadir so we can shut it down and start a
	// new one on top of the existing data as part of the test.
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	knobs, ltk := makeReplicationTestKnobs()
	ctx := context.Background()
	const numNodes = 2
	serverArgsPerNode := make(map[int]base.TestServerArgs)
	for i := 0; i < numNodes; i++ {
		path := filepath.Join(dir, "testserver", strconv.Itoa(i))
		serverArgsPerNode[i] = base.TestServerArgs{
			Knobs:      knobs,
			StoreSpecs: []base.StoreSpec{{InMemory: false, Path: path}},
		}
	}
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ServerArgsPerNode: serverArgsPerNode,
		ReplicationMode:   base.ReplicationManual,
	})
	defer func() {
		// We modify the value of `tc` below to start up a second cluster, so in
		// contrast to other tests, run this `defer Stop` in an anonymous func.
		tc.Stopper().Stop(ctx)
	}()

	// Add a learner replica, send a snapshot so that it's materialized as a
	// Replica on the Store, but don't promote it to a voter.
	scratchStartKey := tc.ScratchRange(t)
	var desc roachpb.RangeDescriptor
	ltk.withStopAfterLearnerAtomic(func() {
		desc = tc.AddVotersOrFatal(t, scratchStartKey, tc.Target(1))
	})
	require.Len(t, desc.Replicas().LearnerDescriptors(), 1)
	learnerReplicaID := desc.Replicas().LearnerDescriptors()[0].ReplicaID

	// Verify that raft on every node thinks it's a learner. This checks that we
	// use ConfChangeAddLearnerNode in the ConfChange and also checks that we
	// correctly generate the ConfState for the snapshot.
	verifyLearnerInRaftOnNodes(scratchStartKey, learnerReplicaID, tc.Servers)

	// Shut down the cluster and restart it, then verify again that raft on every
	// node thinks our learner is a learner. This checks that we generate the
	// initial ConfState correctly.
	tc.Stopper().Stop(ctx)
	tc = testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ServerArgsPerNode: serverArgsPerNode,
		ReplicationMode:   base.ReplicationManual,
	})
	{
		// Ping the raft group to wake it up.
		_, err := tc.Server(0).DB().Get(ctx, scratchStartKey)
		require.NoError(t, err)
	}
	verifyLearnerInRaftOnNodes(scratchStartKey, learnerReplicaID, tc.Servers)
}

func TestLearnerSnapshotFailsRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderShort(t) // Takes 90s.
	skip.UnderRace(t)

	runTest := func(t *testing.T, replicaType roachpb.ReplicaType) {
		var rejectSnapshots int64
		knobs, ltk := makeReplicationTestKnobs()
		ltk.storeKnobs.ReceiveSnapshot = func(h *kvserverpb.SnapshotRequest_Header) error {
			if atomic.LoadInt64(&rejectSnapshots) > 0 {
				return errors.New(`nope`)
			}
			return nil
		}
		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
			ServerArgs:      base.TestServerArgs{Knobs: knobs},
			ReplicationMode: base.ReplicationManual,
		})
		defer tc.Stopper().Stop(ctx)

		scratchStartKey := tc.ScratchRange(t)
		atomic.StoreInt64(&rejectSnapshots, 1)
		var err error
		switch replicaType {
		case roachpb.LEARNER:
			_, err = tc.AddVoters(scratchStartKey, tc.Target(1))
		case roachpb.NON_VOTER:
			_, err = tc.AddNonVoters(scratchStartKey, tc.Target(1))
		default:
			log.Fatalf(ctx, "unexpected replicaType: %s", replicaType)
		}

		if !testutils.IsError(err, `remote couldn't accept INITIAL snapshot`) {
			t.Fatalf(`expected "remote couldn't accept INITIAL snapshot" error got: %+v`, err)
		}
		// Make sure we cleaned up after ourselves (by removing the learner/non-voter).
		desc := tc.LookupRangeOrFatal(t, scratchStartKey)
		require.Empty(t, desc.Replicas().LearnerDescriptors())
	}

	t.Run("learner", func(t *testing.T) {
		runTest(t, roachpb.LEARNER)
	})

	t.Run("non-voter", func(t *testing.T) {
		runTest(t, roachpb.NON_VOTER)
	})
}

// In addition to testing Raft snapshots to non-voters, this test also verifies
// that recorded metrics for Raft snapshot bytes sent are also accurate.
func testRaftSnapshotsToNonVoters(t *testing.T, drainReceivingNode bool) {
	skip.UnderShort(t, "this test sleeps for a few seconds")

	var skipInitialSnapshot int64
	knobs, ltk := makeReplicationTestKnobs()
	ctx := context.Background()

	// Set it up such that the newly added non-voter will not receive its INITIAL
	// snapshot.
	ltk.storeKnobs.ReplicaSkipInitialSnapshot = func() bool {
		return atomic.LoadInt64(&skipInitialSnapshot) == 1
	}
	// Synchronize with the removal of the "best effort" lock on log truncation.
	// See (*Replica).lockLearnerSnapshot for details.
	nonVoterSnapLockRemoved := make(chan struct{}, 1)
	ltk.storeKnobs.NonVoterAfterInitialization = func() {
		nonVoterSnapLockRemoved <- struct{}{}
	}
	// Disable the raft snapshot queue, we will manually queue a replica into it
	// below.
	ltk.storeKnobs.DisableRaftSnapshotQueue = true

	// Synchronize on the moment before the snapshot gets sent so we can measure
	// the state at that time & gather metrics.
	blockUntilSnapshotSendCh := make(chan struct{})
	blockSnapshotSendCh := make(chan struct{})
	ltk.storeKnobs.SendSnapshot = func() {
		close(blockUntilSnapshotSendCh)
		select {
		case <-blockSnapshotSendCh:
		case <-time.After(10 * time.Second):
			return
		}
	}

	tc := testcluster.StartTestCluster(
		t, 2, base.TestClusterArgs{
			ServerArgs:      base.TestServerArgs{Knobs: knobs},
			ReplicationMode: base.ReplicationManual,
		},
	)
	defer tc.Stopper().Stop(ctx)
	scratchStartKey := tc.ScratchRange(t)
	g, ctx := errgroup.WithContext(ctx)

	// Record the snapshot metrics before anything has been sent / received.
	senderTotalBefore, senderMetricsMapBefore := getSnapshotBytesMetrics(t, tc, 0 /* serverIdx */)
	receiverTotalBefore, receiverMetricsMapBefore := getSnapshotBytesMetrics(t, tc, 1 /* serverIdx */)

	// Add a new voting replica, but don't initialize it. Note that
	// `tc.AddNonVoters` will not return until the newly added non-voter is
	// initialized, which we will do below via the snapshot queue.
	g.Go(func() error {
		atomic.StoreInt64(&skipInitialSnapshot, 1)
		_, err := tc.AddNonVoters(scratchStartKey, tc.Target(1))
		return err
	})

	// Wait until we remove the lock that prevents the raft snapshot queue from
	// sending this replica a snapshot.
	select {
	case <-nonVoterSnapLockRemoved:
	case <-time.After(testutils.DefaultSucceedsSoonDuration):
		t.Fatal("took too long")
	}

	scratchDesc := tc.LookupRangeOrFatal(t, scratchStartKey)
	leaseholderStore := tc.GetFirstStoreFromServer(t, 0)
	require.NotNil(t, leaseholderStore)
	leaseholderRepl, err := leaseholderStore.GetReplica(scratchDesc.RangeID)
	require.NoError(t, err)
	require.NotNil(t, leaseholderRepl)

	if drainReceivingNode {
		// Draining nodes shouldn't reject raft snapshots, so this should have no
		// effect on the outcome of this test.
		const drainingServerIdx = 1
		const drainingNodeID = drainingServerIdx + 1
		client, err := tc.GetAdminClient(ctx, t, drainingServerIdx)
		require.NoError(t, err)
		drain(ctx, t, client, drainingNodeID)
	}

	testutils.SucceedsSoon(t, func() error {
		// Manually enqueue the leaseholder replica into its store's raft snapshot
		// queue. We expect it to pick up on the fact that the non-voter on its range
		// needs a snapshot.
		recording, pErr, err := leaseholderStore.Enqueue(
			ctx,
			"raftsnapshot",
			leaseholderRepl,
			false, /* skipShouldQueue */
			false, /* async */
		)
		if pErr != nil {
			return pErr
		}
		if err != nil {
			return err
		}
		matched, err := regexp.MatchString("streamed VIA_SNAPSHOT_QUEUE snapshot.*to.*NON_VOTER", recording.String())
		if err != nil {
			return err
		}
		if !matched {
			return errors.Errorf("the raft snapshot queue did not send a snapshot to the non-voter")
		}
		return nil
	})

	// Wait until the snapshot is about to be sent before calculating what the
	// snapshot size should be. This allows our snapshot measurement to account
	// for any state changes that happen between calling AddNonVoters and the
	// snapshot being sent.
	<-blockUntilSnapshotSendCh
	store, repl := getFirstStoreReplica(t, tc.Server(0), scratchStartKey)
	snapshotLength, err := getExpectedSnapshotSizeBytes(ctx, store, repl, kvserverpb.SnapshotRequest_VIA_SNAPSHOT_QUEUE, tc.Server(1).GetFirstStoreID())
	require.NoError(t, err)

	close(blockSnapshotSendCh)
	require.NoError(t, g.Wait())

	// Record the snapshot metrics for the sender after the raft snapshot was sent.
	senderTotalAfter, senderMetricsMapAfter := getSnapshotBytesMetrics(t, tc, 0)

	// Asserts that the raft snapshot (aka recovery snapshot) bytes sent have been
	// recorded and that it was not double counted in a different metric.
	senderTotalDelta, senderMapDelta := getSnapshotMetricsDiff(senderTotalBefore, senderMetricsMapBefore, senderTotalAfter, senderMetricsMapAfter)

	senderTotalExpected := snapshotBytesMetrics{sentBytes: snapshotLength, rcvdBytes: 0}
	senderMapExpected := map[kvserverpb.SnapshotRequest_Priority]snapshotBytesMetrics{
		kvserverpb.SnapshotRequest_REBALANCE: {sentBytes: 0, rcvdBytes: 0},
		kvserverpb.SnapshotRequest_RECOVERY:  {sentBytes: snapshotLength, rcvdBytes: 0},
		kvserverpb.SnapshotRequest_UNKNOWN:   {sentBytes: 0, rcvdBytes: 0},
	}
	require.Equal(t, senderTotalExpected, senderTotalDelta)
	require.Equal(t, senderMapExpected, senderMapDelta)

	// Record the snapshot metrics for the receiver after the raft snapshot was
	// received.
	receiverTotalAfter, receiverMetricsMapAfter := getSnapshotBytesMetrics(t, tc, 1)

	// Asserts that the raft snapshot (aka recovery snapshot) bytes received have
	// been recorded and that it was not double counted in a different metric.
	receiverTotalDelta, receiverMapDelta := getSnapshotMetricsDiff(receiverTotalBefore, receiverMetricsMapBefore, receiverTotalAfter, receiverMetricsMapAfter)

	receiverTotalExpected := snapshotBytesMetrics{sentBytes: 0, rcvdBytes: snapshotLength}
	receiverMapExpected := map[kvserverpb.SnapshotRequest_Priority]snapshotBytesMetrics{
		kvserverpb.SnapshotRequest_REBALANCE: {sentBytes: 0, rcvdBytes: 0},
		kvserverpb.SnapshotRequest_RECOVERY:  {sentBytes: 0, rcvdBytes: snapshotLength},
		kvserverpb.SnapshotRequest_UNKNOWN:   {sentBytes: 0, rcvdBytes: 0},
	}
	require.Equal(t, receiverTotalExpected, receiverTotalDelta)
	require.Equal(t, receiverMapExpected, receiverMapDelta)
}

func drain(ctx context.Context, t *testing.T, client serverpb.AdminClient, drainingNodeID int) {
	stream, err := client.Drain(ctx, &serverpb.DrainRequest{
		NodeId:  strconv.Itoa(drainingNodeID),
		DoDrain: true,
	})
	require.NoError(t, err)

	// Wait until the draining node acknowledges that it's draining.
	_, err = stream.Recv()
	require.NoError(t, err)
}

// TestSnapshotsToDrainingNodes tests that rebalancing snapshots to draining
// receivers are rejected, but Raft snapshots aren't.
func TestSnapshotsToDrainingNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("rebalancing snapshots", func(t *testing.T) {
		ctx := context.Background()

		// We set up a 2 node test cluster with the second node marked draining.
		const drainingServerIdx = 1
		const drainingNodeID = drainingServerIdx + 1
		tc := testcluster.StartTestCluster(
			t, 2, base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
			},
		)
		defer tc.Stopper().Stop(ctx)
		client, err := tc.GetAdminClient(ctx, t, drainingServerIdx)
		require.NoError(t, err)
		drain(ctx, t, client, drainingNodeID)

		// Now, we try to add a replica to it, we expect that to fail.
		scratchKey := tc.ScratchRange(t)
		_, err = tc.AddVoters(scratchKey, makeReplicationTargets(drainingNodeID)...)
		require.Regexp(t, "store is draining", err)
	})

	t.Run("raft snapshots", func(t *testing.T) {
		testRaftSnapshotsToNonVoters(t, true /* drainReceivingNode */)
	})
}

// TestNonVoterCatchesUpViaRaftSnapshotQueue ensures that a non-voting replica
// in need of a snapshot will receive one via the raft snapshot queue. This is
// also meant to test that a non-voting replica that is initialized via an
// `INITIAL` snapshot during its addition is not ignored by the raft snapshot
// queue for future snapshots.
func TestNonVoterCatchesUpViaRaftSnapshotQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testRaftSnapshotsToNonVoters(t, false /* drainReceivingNode */)
}

func TestSplitWithLearnerOrJointConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	knobs, ltk := makeReplicationTestKnobs()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	// Add a learner replica, send a snapshot so that it's materialized as a
	// Replica on the Store, but don't promote it to a voter.
	scratchStartKey := tc.ScratchRange(t)
	ltk.withStopAfterLearnerAtomic(func() {
		_ = tc.AddVotersOrFatal(t, scratchStartKey, tc.Target(1))
	})

	// Splitting a learner is allowed. This orphans the two learners, but the
	// replication queue will eventually clean this up.
	left, right, err := tc.SplitRange(scratchStartKey.Next())
	require.NoError(t, err)
	require.Len(t, left.Replicas().LearnerDescriptors(), 1)
	require.Len(t, right.Replicas().LearnerDescriptors(), 1)

	// Remove the learner on the RHS.
	right = tc.RemoveVotersOrFatal(t, right.StartKey.AsRawKey(), tc.Target(1))

	// Put an incoming voter on the RHS and split again. This works because the
	// split auto-transitions us out of the joint conf before doing work.
	atomic.StoreInt64(&ltk.replicationAlwaysUseJointConfig, 1)
	atomic.StoreInt64(&ltk.replicaAddStopAfterJointConfig, 1)
	// Use SucceedsSoon to deal with the case where the RHS has not yet been
	// removed or the split has not yet been processed.
	testutils.SucceedsSoon(t, func() error {
		desc, err := tc.AddVoters(right.StartKey.AsRawKey(), tc.Target(1))
		if err == nil {
			right = desc
		} else {
			require.True(t, kvserver.IsRetriableReplicationChangeError(err), err)
		}
		return err
	})
	require.Len(t, right.Replicas().FilterToDescriptors(predIncoming), 1)
	left, right, err = tc.SplitRange(right.StartKey.AsRawKey().Next())
	require.NoError(t, err)
	require.False(t, left.Replicas().InAtomicReplicationChange(), left)
	require.False(t, right.Replicas().InAtomicReplicationChange(), right)
}

// TestSplitRetriesOnFailedExitOfJointConfig ensures that an AdminSplit will
// retry if it sees retryable errors returned while attempting to exit a joint
// configuration.
func TestSplitRetriesOnFailedExitOfJointConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name  string
		errFn func(*roachpb.TransferLeaseRequest) error
	}{
		{
			name: "targetMayNeedSnapshot",
			errFn: func(req *roachpb.TransferLeaseRequest) error {
				repl := req.Lease.Replica
				status := raftutil.ReplicaStateProbe
				return kvserver.NewLeaseTransferRejectedBecauseTargetMayNeedSnapshotError(repl, status)
			},
		},
		{
			name: "replicaNotFound",
			errFn: func(_ *roachpb.TransferLeaseRequest) error {
				return roachpb.ErrReplicaNotFound
			},
		},
		{
			name: "replicaCannotHoldLease",
			errFn: func(_ *roachpb.TransferLeaseRequest) error {
				return roachpb.ErrReplicaCannotHoldLease
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var rangeIDAtomic int64
			var rejectedCount int
			const maxRejects = 3
			reqFilter := func(ctx context.Context, ba roachpb.BatchRequest) *roachpb.Error {
				rangeID := roachpb.RangeID(atomic.LoadInt64(&rangeIDAtomic))
				if ba.RangeID == rangeID && ba.IsSingleTransferLeaseRequest() && rejectedCount < maxRejects {
					rejectedCount++
					req := ba.Requests[0].GetTransferLease()
					err := tc.errFn(req)
					return roachpb.NewError(err)
				}
				return nil
			}

			ctx := context.Background()
			knobs, ltk := makeReplicationTestKnobs()
			knobs.Store.(*kvserver.StoreTestingKnobs).TestingRequestFilter = reqFilter
			tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
				ServerArgs:      base.TestServerArgs{Knobs: knobs},
				ReplicationMode: base.ReplicationManual,
			})
			defer tc.Stopper().Stop(ctx)

			scratchStartKey := tc.ScratchRange(t)
			scratchDesc := tc.LookupRangeOrFatal(t, scratchStartKey)
			atomic.StoreInt64(&rangeIDAtomic, int64(scratchDesc.RangeID))

			// Rebalance the range from one store to the other. This will enter a joint
			// configuration and then stop because of the testing knobs.
			atomic.StoreInt64(&ltk.replicationAlwaysUseJointConfig, 1)
			atomic.StoreInt64(&ltk.replicaAddStopAfterJointConfig, 1)
			tc.RebalanceVoterOrFatal(ctx, t, scratchStartKey, tc.Target(0), tc.Target(1))

			// Perform a split of the range. This will auto-transitions us out of the
			// joint conf before doing work. However, because of the filter we installed
			// above, this will first run into a series of retryable errors when
			// attempting to perform a lease transfer. The split should retry until the
			// joint configuration completes.
			left, right := tc.SplitRangeOrFatal(t, scratchStartKey.Next())
			require.False(t, left.Replicas().InAtomicReplicationChange(), left)
			require.False(t, right.Replicas().InAtomicReplicationChange(), right)

			require.Equal(t, maxRejects, rejectedCount)
		})
	}
}

func TestReplicateQueueSeesLearnerOrJointConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// NB also see TestAllocatorRemoveLearner for a lower-level test.

	ctx := context.Background()
	knobs, ltk := makeReplicationTestKnobs()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	// Add a learner replica, send a snapshot so that it's materialized as a
	// Replica on the Store, but don't promote it to a voter.
	scratchStartKey := tc.ScratchRange(t)
	ltk.withStopAfterLearnerAtomic(func() {
		_ = tc.AddVotersOrFatal(t, scratchStartKey, tc.Target(1))
	})

	// Run the replicate queue.
	store, repl := getFirstStoreReplica(t, tc.Server(0), scratchStartKey)
	{
		require.Equal(t, int64(0), getFirstStoreMetric(t, tc.Server(0), `queue.replicate.removelearnerreplica`))
		store.SetReplicateQueueActive(true)
		trace, processErr, err := store.Enqueue(
			ctx, "replicate", repl, true /* skipShouldQueue */, false, /* async */
		)
		require.NoError(t, err)
		require.NoError(t, processErr)
		action := "next replica action: remove learner"
		require.NoError(t, testutils.MatchInOrder(trace.String(), []string{action}...))
		require.Equal(t, int64(1), getFirstStoreMetric(t, tc.Server(0), `queue.replicate.removelearnerreplica`))

		testutils.SucceedsSoon(t, func() error {
			desc := tc.LookupRangeOrFatal(t, scratchStartKey)
			if len(desc.Replicas().LearnerDescriptors()) != 0 {
				return errors.Newf("Mismatch in num learners %v, desc: %v", desc.Replicas().LearnerDescriptors(), desc)
			}
			if len(desc.Replicas().VoterDescriptors()) != 3 {
				return errors.Newf("Mismatch in num voters %v, desc: %v", desc.Replicas().VoterDescriptors(), desc)
			}
			return nil
		})
		// It has done everything it needs to do now, disable before the next test section.
		store.SetReplicateQueueActive(false)
	}

	// Create a VOTER_OUTGOING, i.e. a joint configuration.
	ltk.withStopAfterJointConfig(func() {
		desc := tc.RemoveVotersOrFatal(t, scratchStartKey, tc.Target(2))
		require.True(t, desc.Replicas().InAtomicReplicationChange(), desc)
		store.SetReplicateQueueActive(true)
		trace, processErr, err := store.Enqueue(
			ctx, "replicate", repl, true /* skipShouldQueue */, false, /* async */
		)
		require.NoError(t, err)
		require.NoError(t, processErr)
		action := "next replica action: finalize conf change"
		require.NoError(t, testutils.MatchInOrder(trace.String(), []string{action}...))

		testutils.SucceedsSoon(t, func() error {
			desc = tc.LookupRangeOrFatal(t, scratchStartKey)
			if len(desc.Replicas().VoterDescriptors()) != 3 {
				return errors.Newf("Mismatch in num voters %v, desc: %v", desc.Replicas().VoterDescriptors(), desc)
			}
			return nil
		})
		store.SetReplicateQueueActive(false)
	})
}

func TestReplicaGCQueueSeesLearnerOrJointConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	knobs, ltk := makeReplicationTestKnobs()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	// Add a learner replica, send a snapshot so that it's materialized as a
	// Replica on the Store, but don't promote it to a voter.
	scratchStartKey := tc.ScratchRange(t)
	ltk.withStopAfterLearnerAtomic(func() {
		_ = tc.AddVotersOrFatal(t, scratchStartKey, tc.Target(1))
	})

	// Run the replicaGC queue.
	checkNoGC := func() roachpb.RangeDescriptor {
		store, repl := getFirstStoreReplica(t, tc.Server(1), scratchStartKey)
		trace, processErr, err := store.Enqueue(
			ctx, "replicaGC", repl, true /* skipShouldQueue */, false, /* async */
		)
		require.NoError(t, err)
		require.NoError(t, processErr)
		const msg = `not gc'able, replica is still in range descriptor: (n2,s2):`
		require.Contains(t, trace.String(), msg)
		return tc.LookupRangeOrFatal(t, scratchStartKey)
	}
	desc := checkNoGC()
	// Make sure it didn't collect the learner.
	require.NotEmpty(t, desc.Replicas().LearnerDescriptors())

	// Now get the range into a joint config.
	tc.RemoveVotersOrFatal(t, scratchStartKey, tc.Target(1)) // remove learner

	ltk.withStopAfterJointConfig(func() {
		desc = tc.AddVotersOrFatal(t, scratchStartKey, tc.Target(1))
		require.Len(t, desc.Replicas().FilterToDescriptors(predIncoming), 1, desc)
	})

	postDesc := checkNoGC()
	require.Equal(t, desc, postDesc)
}

func TestRaftSnapshotQueueSeesLearner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	blockSnapshotsCh := make(chan struct{})
	knobs, ltk := makeReplicationTestKnobs()
	ltk.storeKnobs.DisableRaftSnapshotQueue = true
	ltk.storeKnobs.ReceiveSnapshot = func(h *kvserverpb.SnapshotRequest_Header) error {
		select {
		case <-blockSnapshotsCh:
		case <-time.After(10 * time.Second):
			return errors.New(`test timed out`)
		}
		return nil
	}
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	// Create a learner replica.
	scratchStartKey := tc.ScratchRange(t)
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		_, err := tc.AddVoters(scratchStartKey, tc.Target(1))
		return err
	})

	// Note the value of the metrics before.
	generatedBefore := getFirstStoreMetric(t, tc.Server(0), `range.snapshots.generated`)
	raftAppliedBefore := getFirstStoreMetric(t, tc.Server(0), `range.snapshots.applied-voter`)

	// Run the raftsnapshot queue. SucceedsSoon because it may take a bit for
	// raft to figure out that the replica needs a snapshot.
	store, repl := getFirstStoreReplica(t, tc.Server(0), scratchStartKey)
	testutils.SucceedsSoon(t, func() error {
		trace, processErr, err := store.Enqueue(
			ctx, "raftsnapshot", repl, true /* skipShouldQueue */, false, /* async */
		)
		if err != nil {
			return err
		}
		if processErr != nil {
			return processErr
		}
		const msg = `skipping snapshot; replica is likely a LEARNER in the process of being added: (n2,s2):2LEARNER`
		formattedTrace := trace.String()
		if !strings.Contains(formattedTrace, msg) {
			return errors.Errorf(`expected "%s" in trace got:\n%s`, msg, formattedTrace)
		}
		return nil
	})

	// Make sure it didn't send any VIA_SNAPSHOT_QUEUE snapshots.
	require.Equal(t, generatedBefore, getFirstStoreMetric(t, tc.Server(0), `range.snapshots.generated`))
	require.Equal(t, raftAppliedBefore, getFirstStoreMetric(t, tc.Server(0), `range.snapshots.applied-voter`))

	close(blockSnapshotsCh)
	require.NoError(t, g.Wait())
}

// This test verifies the result of a race between the replicate queue running
// while an AdminChangeReplicas is adding a replica.
func TestLearnerAdminChangeReplicasRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	blockUntilSnapshotCh := make(chan struct{}, 2)
	blockSnapshotsCh := make(chan struct{})
	knobs, ltk := makeReplicationTestKnobs()
	ltk.storeKnobs.ReceiveSnapshot = func(h *kvserverpb.SnapshotRequest_Header) error {
		blockUntilSnapshotCh <- struct{}{}
		<-blockSnapshotsCh
		return nil
	}
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	// Add the learner.
	scratchStartKey := tc.ScratchRange(t)
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		// NB: we don't use tc.AddVoters because that will auto-retry
		// and the test expects to see the error that results on the
		// first attempt.
		desc, err := tc.LookupRange(scratchStartKey)
		if err != nil {
			return err
		}
		_, err = tc.Servers[0].DB().AdminChangeReplicas(
			ctx, scratchStartKey, desc, roachpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(1)),
		)
		return err
	})

	// Wait until the snapshot starts, which happens after the learner has been
	// added.
	<-blockUntilSnapshotCh

	// Removes the learner out from under the coordinator running on behalf of
	// AddVoters. This simulates the replicate queue running concurrently. The
	// first thing the replicate queue would do is remove any learners it sees.
	_, err := tc.RemoveVoters(scratchStartKey, tc.Target(1))
	require.NoError(t, err)
	desc := tc.LookupRangeOrFatal(t, scratchStartKey)
	require.Len(t, desc.Replicas().VoterDescriptors(), 1)
	require.Len(t, desc.Replicas().LearnerDescriptors(), 0)

	// Unblock the snapshot, and surprise AddVoters. It should retry and error
	// that the descriptor has changed since the AdminChangeReplicas command
	// started. Alternatively it may fail in sending the snapshot because of a
	// "raft group deleted" error if the newly added learner attempts to send
	// a raft message to another node after it has been removed and then destroys
	// itself in response to a ReplicaTooOldError.
	close(blockSnapshotsCh)
	const msgRE = `descriptor changed|raft group deleted`
	if err := g.Wait(); !testutils.IsError(err, msgRE) {
		t.Fatalf(`expected %q error got: %+v`, msgRE, err)
	}
	desc = tc.LookupRangeOrFatal(t, scratchStartKey)
	require.Len(t, desc.Replicas().VoterDescriptors(), 1)
	require.Len(t, desc.Replicas().LearnerDescriptors(), 0)
}

// This test verifies the result of a race between the replicate queue running
// for the same range from two different nodes. This can happen around
// leadership changes.
func TestLearnerReplicateQueueRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var skipReceiveSnapshotKnobAtomic int64 = 1
	blockUntilSnapshotCh := make(chan struct{}, 2)
	blockSnapshotsCh := make(chan struct{})
	knobs, ltk := makeReplicationTestKnobs()
	// We must disable eager replica removal to make this test reliable.
	// If we don't then it's possible that the removed replica on store 2 will
	// notice it's removed before the snapshot is sent by the replicate queue.
	// In this case we'll get a snapshot error from the replicate queue which
	// will retry the up-replication with a new descriptor and succeed.
	ltk.storeKnobs.DisableEagerReplicaRemoval = true
	ltk.storeKnobs.ReceiveSnapshot = func(h *kvserverpb.SnapshotRequest_Header) error {
		if atomic.LoadInt64(&skipReceiveSnapshotKnobAtomic) > 0 {
			return nil
		}
		blockUntilSnapshotCh <- struct{}{}
		<-blockSnapshotsCh
		return nil
	}
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs, SnapshotSendLimit: 1},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	scratchStartKey := tc.ScratchRange(t)
	store, repl := getFirstStoreReplica(t, tc.Server(0), scratchStartKey)

	// Start with 2 replicas so the replicate queue can go from 2->3, otherwise it
	// will refuse to upreplicate to a fragile quorum of 1->2.
	tc.AddVotersOrFatal(t, scratchStartKey, tc.Target(1))
	atomic.StoreInt64(&skipReceiveSnapshotKnobAtomic, 0)

	// Run the replicate queue, this will add a learner to node 3 and start
	// sending it a snapshot. This will eventually fail and we assert some things
	// about the trace to prove it failed in the way we want.
	queue1ErrCh := make(chan error, 1)
	go func() {
		queue1ErrCh <- func() error {
			trace, processErr, err := store.Enqueue(
				ctx, "replicate", repl, true /* skipShouldQueue */, false, /* async */
			)
			if err != nil {
				return err
			}
			if !strings.Contains(processErr.Error(), `descriptor changed`) {
				// NB: errors.Wrapf(nil, ...) returns nil.
				// nolint:errwrap
				return errors.Errorf(`expected "descriptor changed" error got: %+v`, processErr)
			}
			formattedTrace := trace.String()
			expectedMessages := []string{
				`could not promote .*?n3,s3.*? to voter, rolling back:.*?change replicas of r\d+ failed: descriptor changed`,
				`learner to roll back not found`,
			}
			return testutils.MatchInOrder(formattedTrace, expectedMessages...)
		}()
	}()

	// Wait until the snapshot starts, which happens after the learner has been
	// added.
	<-blockUntilSnapshotCh

	// Remove the learner on node 3 out from under the replicate queue. This
	// simulates a second replicate queue running concurrently. The first thing
	// this second replicate queue would do is remove any learners it sees,
	// leaving the 2 voters.
	desc, err := tc.RemoveVoters(scratchStartKey, tc.Target(2))
	require.NoError(t, err)
	require.Len(t, desc.Replicas().VoterDescriptors(), 2)
	require.Len(t, desc.Replicas().LearnerDescriptors(), 0)

	// Unblock the snapshot, and surprise the replicate queue. It should retry,
	// get a descriptor changed error, and realize it should stop.
	close(blockSnapshotsCh)
	require.NoError(t, <-queue1ErrCh)
	desc = tc.LookupRangeOrFatal(t, scratchStartKey)
	require.Len(t, desc.Replicas().VoterDescriptors(), 2)
	require.Len(t, desc.Replicas().LearnerDescriptors(), 0)
}

func TestLearnerNoAcceptLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	knobs, ltk := makeReplicationTestKnobs()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	// Add a learner replica, send a snapshot so that it's materialized as a
	// Replica on the Store, but don't promote it to a voter.
	scratchStartKey := tc.ScratchRange(t)
	ltk.withStopAfterLearnerAtomic(func() {
		_ = tc.AddVotersOrFatal(t, scratchStartKey, tc.Target(1))
	})

	desc := tc.LookupRangeOrFatal(t, scratchStartKey)
	err := tc.TransferRangeLease(desc, tc.Target(1))
	exp := `replica cannot hold lease`
	if !testutils.IsError(err, exp) {
		t.Fatalf(`expected %q error got: %+v`, exp, err)
	}
}

// TestJointConfigLease verifies that incoming voters can have the
// lease transferred to them, and outgoing voters cannot.
func TestJointConfigLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	knobs, ltk := makeReplicationTestKnobs()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	atomic.StoreInt64(&ltk.replicaAddStopAfterJointConfig, 1)
	atomic.StoreInt64(&ltk.replicationAlwaysUseJointConfig, 1)
	desc := tc.AddVotersOrFatal(t, k, tc.Target(1))
	require.True(t, desc.Replicas().InAtomicReplicationChange(), desc)

	err := tc.TransferRangeLease(desc, tc.Target(1))
	require.NoError(t, err)

	// NB: we don't have to transition out of the previous joint config first
	// because this is done automatically by ChangeReplicas before it does what
	// it's asked to do.
	desc = tc.RemoveVotersOrFatal(t, k, tc.Target(0))
	err = tc.TransferRangeLease(desc, tc.Target(0))
	exp := `replica cannot hold lease`
	require.True(t, testutils.IsError(err, exp), err)
}

func TestLearnerAndVoterOutgoingFollowerRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Limiting how long transactions can run does not work well with race
	// unless we're extremely lenient, which drives up the test duration.
	skip.UnderRace(t)

	ctx := context.Background()
	knobs, ltk := makeReplicationTestKnobs()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)
	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tr := tc.Server(0).TracerI().(*tracing.Tracer)
	db.Exec(t, fmt.Sprintf(`SET CLUSTER SETTING kv.closed_timestamp.target_duration = '%s'`,
		testingTargetDuration))
	db.Exec(t, fmt.Sprintf(`SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '%s'`, testingSideTransportInterval))
	db.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.follower_reads_enabled = true`)

	scratchStartKey := tc.ScratchRange(t)
	var scratchDesc roachpb.RangeDescriptor
	ltk.withStopAfterLearnerAtomic(func() {
		scratchDesc = tc.AddVotersOrFatal(t, scratchStartKey, tc.Target(1))
	})

	check := func() {
		ts := tc.Server(0).Clock().Now()
		txn := roachpb.MakeTransaction("txn", nil, 0, ts, 0, int32(tc.Server(0).SQLInstanceID()))
		req := roachpb.BatchRequest{Header: roachpb.Header{
			RangeID:   scratchDesc.RangeID,
			Timestamp: ts,
			Txn:       &txn,
		}}
		req.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{
			Key: scratchDesc.StartKey.AsRawKey(), EndKey: scratchDesc.EndKey.AsRawKey(),
		}})

		_, repl := getFirstStoreReplica(t, tc.Server(1), scratchStartKey)
		testutils.SucceedsSoon(t, func() error {
			// Trace the Send call so we can verify that it hit the exact `learner
			// replicas cannot serve follower reads` branch that we're trying to test.
			sendCtx, getRecAndFinish := tracing.ContextWithRecordingSpan(ctx, tr, "manual read request")
			defer getRecAndFinish()
			_, pErr := repl.Send(sendCtx, req)
			err := pErr.GoError()
			if !testutils.IsError(err, `not lease holder`) {
				// NB: errors.Wrapf(nil, ...) returns nil.
				// nolint:errwrap
				return errors.Errorf(`expected "not lease holder" error got: %+v`, err)
			}
			const msg = `cannot serve follower reads`
			formattedTrace := getRecAndFinish().String()
			if !strings.Contains(formattedTrace, msg) {
				return errors.Errorf("expected a trace with `%s` got:\n%s", msg, formattedTrace)
			}
			return nil
		})
	}

	// Can't serve follower read from the LEARNER.
	check()

	atomic.StoreInt64(&ltk.replicaAddStopAfterJointConfig, 1)
	atomic.StoreInt64(&ltk.replicationAlwaysUseJointConfig, 1)

	scratchDesc = tc.RemoveVotersOrFatal(t, scratchStartKey, tc.Target(1))
	// Removing a learner doesn't get you into a joint state (no voters changed).
	require.False(t, scratchDesc.Replicas().InAtomicReplicationChange(), scratchDesc)
	scratchDesc = tc.AddVotersOrFatal(t, scratchStartKey, tc.Target(1))

	// Re-add the voter and remain in joint config.
	require.True(t, scratchDesc.Replicas().InAtomicReplicationChange(), scratchDesc)
	require.Len(t, scratchDesc.Replicas().FilterToDescriptors(predIncoming), 1)

	// Remove the voter and remain in joint config.
	scratchDesc = tc.RemoveVotersOrFatal(t, scratchStartKey, tc.Target(1))
	require.True(t, scratchDesc.Replicas().InAtomicReplicationChange(), scratchDesc)
	require.Len(t, scratchDesc.Replicas().FilterToDescriptors(predDemotingToLearner), 1)

	// Can't serve follower read from the VOTER_OUTGOING.
	check()
}

func TestLearnerOrJointConfigAdminRelocateRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	knobs, ltk := makeReplicationTestKnobs()
	tc := testcluster.StartTestCluster(t, 4, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	scratchStartKey := tc.ScratchRange(t)
	ltk.withStopAfterLearnerAtomic(func() {
		_ = tc.AddVotersOrFatal(t, scratchStartKey, tc.Target(1))
		_ = tc.AddVotersOrFatal(t, scratchStartKey, tc.Target(2))
	})

	check := func(voterTargets []roachpb.ReplicationTarget) {
		require.NoError(t, tc.Server(0).DB().AdminRelocateRange(
			ctx,
			scratchStartKey,
			voterTargets,
			[]roachpb.ReplicationTarget{},
			true, /* transferLeaseToFirstVoter */
		))
		desc := tc.LookupRangeOrFatal(t, scratchStartKey)
		voters := desc.Replicas().VoterDescriptors()
		require.Len(t, voters, len(voterTargets))
		sort.Slice(voters, func(i, j int) bool { return voters[i].NodeID < voters[j].NodeID })
		for i := range voters {
			require.Equal(t, voterTargets[i].NodeID, voters[i].NodeID, `%v`, voters)
			require.Equal(t, voterTargets[i].StoreID, voters[i].StoreID, `%v`, voters)
		}
		require.Empty(t, desc.Replicas().LearnerDescriptors())
		require.Empty(t, desc.Replicas().FilterToDescriptors(predIncoming))
		require.Empty(t, desc.Replicas().FilterToDescriptors(predOutgoing))
	}

	// Test AdminRelocateRange's treatment of learners by having one that it has
	// to remove and one that should stay and become a voter.
	//
	// Before: 1 (voter), 2 (learner), 3 (learner)
	// After: 1 (voter), 2 (voter), 4 (voter)
	check([]roachpb.ReplicationTarget{tc.Target(0), tc.Target(1), tc.Target(3)})

	// AdminRelocateRange should leave joint configs before doing its thing.
	//
	// Before: 1 (voter), 2 (voter), 4 (demoting)
	// After: 1 (voter), 2 (voter), 3 (voter)
	atomic.StoreInt64(&ltk.replicaAddStopAfterJointConfig, 1)
	atomic.StoreInt64(&ltk.replicationAlwaysUseJointConfig, 1)
	desc := tc.RemoveVotersOrFatal(t, scratchStartKey, tc.Target(3))
	require.True(t, desc.Replicas().InAtomicReplicationChange(), desc)
	require.Len(t, desc.Replicas().FilterToDescriptors(predDemotingToLearner), 1)
	atomic.StoreInt64(&ltk.replicaAddStopAfterJointConfig, 0)
	check([]roachpb.ReplicationTarget{tc.Target(0), tc.Target(1), tc.Target(2)})
}

// TestDemotedLearnerRemovalHandlesRace tests that a `ChangeReplicas` request at
// the last step of removing a demoted learner replica does not fail if it finds
// that the learner has been already removed from the range.
func TestDemotedLearnerRemovalHandlesRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var activateTestingKnob int64
	waitForRebalanceToBlockCh := make(chan struct{})
	blockDemotedLearnerRemovalCh := make(chan struct{})
	tc := testcluster.StartTestCluster(
		t, 3, base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						BeforeRemovingDemotedLearner: func() {
							if atomic.LoadInt64(&activateTestingKnob) == 1 {
								// Signal to the test that the rebalance is now trying to remove
								// the demoted learner.
								close(waitForRebalanceToBlockCh)
								// Wait for the test to manually remove the demoted learner
								// before letting the rebalance continue.
								blockDemotedLearnerRemovalCh <- struct{}{}
							}
						},
					},
				},
			},
		},
	)
	defer tc.Stopper().Stop(ctx)

	scratchKey := tc.ScratchRange(t)
	// Add a new voting replica on node 2, then rebalance it to node 3. This will
	// block on blockDemotedLearnerRemovalCh.
	tc.AddVotersOrFatal(t, scratchKey, makeReplicationTargets(2)...)
	atomic.StoreInt64(&activateTestingKnob, 1)
	rebalanceCh := make(chan error)
	var finishAndGetRecording func() tracingpb.Recording
	err := tc.Stopper().RunAsyncTask(ctx, "test", func(ctx context.Context) {
		ctx, finishAndGetRecording = tracing.ContextWithRecordingSpan(
			ctx, tc.Servers[0].Tracer(), "rebalance",
		)
		_, err := tc.RebalanceVoter(
			ctx,
			scratchKey,
			roachpb.ReplicationTarget{StoreID: 2, NodeID: 2}, /* src */
			roachpb.ReplicationTarget{StoreID: 3, NodeID: 3}, /* dest */
		)
		rebalanceCh <- err
	})
	require.NoError(t, err)
	defer func() {
		// Unblock the rebalance and expect it to complete.
		<-blockDemotedLearnerRemovalCh
		require.NoError(t, <-rebalanceCh)
		trace := finishAndGetRecording()
		// Check that we actually detected that the learner was removed, and
		// no-oped.
		require.Contains(t, trace.String(), "skipping learner removal because it was already removed")
	}()

	select {
	case <-waitForRebalanceToBlockCh:
		// Continue.
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for rebalance to block")
	}

	// Manually remove the learner replica from the range, and expect that to not
	// affect the previous rebalance anymore.
	_, leaseRepl := getFirstStoreReplica(t, tc.Servers[0], scratchKey)
	require.NotNil(t, leaseRepl)
	beforeDesc := tc.LookupRangeOrFatal(t, scratchKey)
	_, err = leaseRepl.TestingRemoveLearner(
		ctx, &beforeDesc, roachpb.ReplicationTarget{StoreID: 2, NodeID: 2},
	)
	require.NoError(t, err)
}

func TestLearnerAndJointConfigAdminMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	knobs, ltk := makeReplicationTestKnobs()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	scratchStartKey := tc.ScratchRange(t)
	splitKey1 := scratchStartKey.Next()
	splitKey2 := splitKey1.Next()
	_, _ = tc.SplitRangeOrFatal(t, splitKey1)
	_, _ = tc.SplitRangeOrFatal(t, splitKey2)

	// Three ranges (in that order):
	// desc1: will have a learner (later joint voter)
	// desc2 (unnamed): is always left vanilla
	// desc3: like desc1
	//
	// This allows testing merges that have a learner on the RHS (on desc2) and
	// the LHS (on desc1).
	var desc1, desc3 roachpb.RangeDescriptor
	ltk.withStopAfterLearnerAtomic(func() {
		desc1 = tc.AddVotersOrFatal(t, scratchStartKey, tc.Target(1))
		desc3 = tc.AddVotersOrFatal(t, splitKey2, tc.Target(1))
	})

	checkFails := func() {
		err := tc.Server(0).DB().AdminMerge(ctx, scratchStartKey)
		if exp := `cannot merge ranges.*joint state`; !testutils.IsError(err, exp) {
			t.Fatalf(`expected "%s" error got: %+v`, exp, err)
		}
		err = tc.Server(0).DB().AdminMerge(ctx, splitKey1)
		if exp := `cannot merge ranges.*joint state`; !testutils.IsError(err, exp) {
			t.Fatalf(`expected "%s" error got: %+v`, exp, err)
		}
	}

	// LEARNER on the lhs or rhs should fail.
	// desc{1,2,3} = (VOTER_FULL, LEARNER) (VOTER_FULL) (VOTER_FULL, LEARNER)
	checkFails()

	// Turn the learners on desc1 and desc3 into VOTER_INCOMINGs.
	atomic.StoreInt64(&ltk.replicaAddStopAfterJointConfig, 1)
	atomic.StoreInt64(&ltk.replicationAlwaysUseJointConfig, 1)
	desc1 = tc.RemoveVotersOrFatal(t, desc1.StartKey.AsRawKey(), tc.Target(1))
	desc1 = tc.AddVotersOrFatal(t, desc1.StartKey.AsRawKey(), tc.Target(1))
	require.Len(t, desc1.Replicas().FilterToDescriptors(predIncoming), 1)
	desc3 = tc.RemoveVotersOrFatal(t, desc3.StartKey.AsRawKey(), tc.Target(1))
	desc3 = tc.AddVotersOrFatal(t, desc3.StartKey.AsRawKey(), tc.Target(1))
	require.Len(t, desc1.Replicas().FilterToDescriptors(predIncoming), 1)

	// VOTER_INCOMING on the lhs or rhs should fail.
	// desc{1,2,3} = (VOTER_FULL, VOTER_INCOMING) (VOTER_FULL) (VOTER_FULL, VOTER_INCOMING)
	checkFails()

	// Turn the incoming voters on desc1 and desc3 into VOTER_DEMOTING_LEARNERs.
	// desc{1,2,3} = (VOTER_FULL, VOTER_DEMOTING_LEARNER) (VOTER_FULL) (VOTER_FULL, VOTER_DEMOTING_LEARNER)
	desc1 = tc.RemoveVotersOrFatal(t, desc1.StartKey.AsRawKey(), tc.Target(1))
	require.Len(t, desc1.Replicas().FilterToDescriptors(predDemotingToLearner), 1)
	desc3 = tc.RemoveVotersOrFatal(t, desc3.StartKey.AsRawKey(), tc.Target(1))
	require.Len(t, desc3.Replicas().FilterToDescriptors(predDemotingToLearner), 1)

	// VOTER_DEMOTING_LEARNER on the lhs or rhs should fail.
	checkFails()

	// Add a VOTER_INCOMING to desc2 to make sure it actually excludes this type
	// of replicas from merges (rather than really just checking whether the
	// replica sets are equal).
	// desc{1,2,3} = (VOTER_FULL, VOTER_DEMOTING_LEARNER) (VOTER_FULL, VOTER_INCOMING) (VOTER_FULL, VOTER_DEMOTING_LEARNER)
	desc2 := tc.AddVotersOrFatal(t, splitKey1, tc.Target(1))
	require.Len(t, desc2.Replicas().FilterToDescriptors(predIncoming), 1)

	checkFails()

	// Ditto VOTER_DEMOTING_LEARNER.
	// desc{1,2,3} = (VOTER_FULL, VOTER_DEMOTING_LEARNER) (VOTER_FULL, VOTER_DEMOTING_LEARNER) (VOTER_FULL, VOTER_DEMOTING_LEARNER)
	desc2 = tc.RemoveVotersOrFatal(t, desc2.StartKey.AsRawKey(), tc.Target(1))
	require.Len(t, desc2.Replicas().FilterToDescriptors(predDemotingToLearner), 1)

	checkFails()
}

// TestMergeQueueDoesNotInterruptReplicationChange verifies that the merge queue
// will correctly ignore a range that has an in-flight snapshot to a learner
// replica.
func TestMergeQueueDoesNotInterruptReplicationChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var activateSnapshotTestingKnob int64
	blockSnapshot := make(chan struct{})
	tc := testcluster.StartTestCluster(
		t, 2, base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						// Disable load-based splitting, so that the absence of sufficient
						// QPS measurements do not prevent ranges from merging.
						DisableLoadBasedSplitting: true,
						ReceiveSnapshot: func(_ *kvserverpb.SnapshotRequest_Header) error {
							if atomic.LoadInt64(&activateSnapshotTestingKnob) == 1 {
								<-blockSnapshot
							}
							return nil
						},
					},
				},
			},
		},
	)
	defer tc.Stopper().Stop(ctx)

	scratchKey := tc.ScratchRange(t)
	splitKey := scratchKey.Next()

	// Split and then unsplit the range to clear the sticky bit, otherwise the
	// mergeQueue will ignore the range.
	tc.SplitRangeOrFatal(t, splitKey)
	require.NoError(t, tc.Server(0).DB().AdminUnsplit(ctx, splitKey))

	atomic.StoreInt64(&activateSnapshotTestingKnob, 1)
	replicationChange := make(chan error)
	err := tc.Stopper().RunAsyncTask(ctx, "test", func(ctx context.Context) {
		_, err := tc.AddVoters(scratchKey, makeReplicationTargets(2)...)
		replicationChange <- err
	})
	require.NoError(t, err)

	select {
	case <-time.After(100 * time.Millisecond):
	// Continue.
	case <-replicationChange:
		t.Fatal("did not expect the replication change to complete")
	}
	defer func() {
		// Unblock the replication change and ensure that it succeeds.
		close(blockSnapshot)
		require.NoError(t, <-replicationChange)
	}()

	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	// TestCluster currently overrides this when used with ReplicationManual.
	db.Exec(t, `SET CLUSTER SETTING kv.range_merge.queue_enabled = true`)

	testutils.SucceedsSoon(t, func() error {
		// While this replication change is stalled, we'll trigger a merge and
		// ensure that the merge correctly notices that there is a snapshot in
		// flight and ignores the range.
		store, repl := getFirstStoreReplica(t, tc.Server(0), scratchKey)
		_, processErr, enqueueErr := store.Enqueue(
			ctx, "merge", repl, true /* skipShouldQueue */, false, /* async */
		)
		require.NoError(t, enqueueErr)
		require.True(t, kvserver.IsReplicationChangeInProgressError(processErr))
		return nil
	})
}

func TestMergeQueueSeesLearnerOrJointConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	knobs, ltk := makeReplicationTestKnobs()
	// Disable load-based splitting, so that the absence of sufficient QPS
	// measurements do not prevent ranges from merging.
	knobs.Store.(*kvserver.StoreTestingKnobs).DisableLoadBasedSplitting = true
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)
	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	// TestCluster currently overrides this when used with ReplicationManual.
	db.Exec(t, `SET CLUSTER SETTING kv.range_merge.queue_enabled = true`)

	scratchStartKey := tc.ScratchRange(t)
	origDesc := tc.LookupRangeOrFatal(t, scratchStartKey)

	splitKey := scratchStartKey.Next()

	splitAndUnsplit := func() roachpb.RangeDescriptor {
		desc, _ := tc.SplitRangeOrFatal(t, splitKey)
		// Unsplit the range to clear the sticky bit.
		require.NoError(t, tc.Server(0).DB().AdminUnsplit(ctx, splitKey))
		return desc
	}

	// Run the merge queue while there's a learner on the LHS.
	{
		splitAndUnsplit()

		ltk.withStopAfterLearnerAtomic(func() {
			_ = tc.AddVotersOrFatal(t, scratchStartKey, tc.Target(1))
		})

		store, repl := getFirstStoreReplica(t, tc.Server(0), scratchStartKey)
		trace, processErr, err := store.Enqueue(
			ctx, "merge", repl, true /* skipShouldQueue */, false, /* async */
		)
		require.NoError(t, err)
		require.NoError(t, processErr)
		formattedTrace := trace.String()
		expectedMessages := []string{
			`removing learner replicas \[n2,s2\]`,
			`merging to produce range: /Table/Max-/Max`,
		}
		if err := testutils.MatchInOrder(formattedTrace, expectedMessages...); err != nil {
			t.Fatal(err)
		}

		// Sanity check that the desc has the same bounds it did originally.
		desc := tc.LookupRangeOrFatal(t, scratchStartKey)
		require.Equal(t, origDesc.StartKey, desc.StartKey)
		require.Equal(t, origDesc.EndKey, desc.EndKey)
		// The merge removed the learner.
		require.Len(t, desc.Replicas().VoterDescriptors(), 1)
		require.Empty(t, desc.Replicas().LearnerDescriptors())
	}

	// Create the RHS again and repeat the same game, except this time the LHS
	// gets a VOTER_INCOMING for s2, and then the merge queue runs into it. It
	// will transition the LHS out of the joint config and then do the merge.
	{
		desc := splitAndUnsplit()

		ltk.withStopAfterJointConfig(func() {
			desc = tc.AddVotersOrFatal(t, scratchStartKey, tc.Target(1))
		})
		require.Len(t, desc.Replicas().FilterToDescriptors(predIncoming), 1, desc)

		checkTransitioningOut := func() {
			t.Helper()
			store, repl := getFirstStoreReplica(t, tc.Server(0), scratchStartKey)
			trace, processErr, err := store.Enqueue(
				ctx, "merge", repl, true /* skipShouldQueue */, false, /* async */
			)
			require.NoError(t, err)
			require.NoError(t, processErr)
			formattedTrace := trace.String()
			expectedMessages := []string{
				`transitioning out of joint configuration`,
				`merging to produce range: /Table/Max-/Max`,
			}
			if err := testutils.MatchInOrder(formattedTrace, expectedMessages...); err != nil {
				t.Fatal(err)
			}
		}

		checkTransitioningOut()
		desc = tc.LookupRangeOrFatal(t, scratchStartKey)
		require.Len(t, desc.Replicas().VoterDescriptors(), 2)
		require.False(t, desc.Replicas().InAtomicReplicationChange(), desc)

		// Repeat the game, except now we start with two replicas and we're
		// giving the RHS a VOTER_DEMOTING_LEARNER.
		desc = splitAndUnsplit()
		ltk.withStopAfterJointConfig(func() {
			descRight := tc.RemoveVotersOrFatal(t, desc.EndKey.AsRawKey(), tc.Target(1))
			require.Len(t, descRight.Replicas().FilterToDescriptors(predDemotingToLearner), 1, desc)
		})

		// This should transition out (i.e. remove the voter on s2 for the RHS)
		// and then do its thing, which means in the end we have two voters again.
		checkTransitioningOut()
		desc = tc.LookupRangeOrFatal(t, scratchStartKey)
		require.Len(t, desc.Replicas().VoterDescriptors(), 2)
		require.False(t, desc.Replicas().InAtomicReplicationChange(), desc)
	}
}

type snapshotBytesMetrics struct {
	sentBytes int64
	rcvdBytes int64
}

// getSnapshotBytesMetrics returns metrics on the number of snapshot bytes sent
// and received by a server. tc and serverIdx specify the index of the target
// server on the TestCluster TC. The function returns the total number of
// snapshot bytes sent/received, as well as a map with granular metrics on the
// number of snapshot bytes sent and received for each type of snapshot. The
// return value is of the form (totalBytes, granularMetrics), where totalBytes
// is a `snapshotBytesMetrics` struct containing the total bytes sent/received,
// and granularMetrics is the map mentioned above.
func getSnapshotBytesMetrics(
	t *testing.T, tc *testcluster.TestCluster, serverIdx int,
) (snapshotBytesMetrics, map[kvserverpb.SnapshotRequest_Priority]snapshotBytesMetrics) {
	granularMetrics := make(map[kvserverpb.SnapshotRequest_Priority]snapshotBytesMetrics)

	granularMetrics[kvserverpb.SnapshotRequest_UNKNOWN] = snapshotBytesMetrics{
		sentBytes: getFirstStoreMetric(t, tc.Server(serverIdx), "range.snapshots.unknown.sent-bytes"),
		rcvdBytes: getFirstStoreMetric(t, tc.Server(serverIdx), "range.snapshots.unknown.rcvd-bytes"),
	}
	granularMetrics[kvserverpb.SnapshotRequest_RECOVERY] = snapshotBytesMetrics{
		sentBytes: getFirstStoreMetric(t, tc.Server(serverIdx), "range.snapshots.recovery.sent-bytes"),
		rcvdBytes: getFirstStoreMetric(t, tc.Server(serverIdx), "range.snapshots.recovery.rcvd-bytes"),
	}
	granularMetrics[kvserverpb.SnapshotRequest_REBALANCE] = snapshotBytesMetrics{
		sentBytes: getFirstStoreMetric(t, tc.Server(serverIdx), "range.snapshots.rebalancing.sent-bytes"),
		rcvdBytes: getFirstStoreMetric(t, tc.Server(serverIdx), "range.snapshots.rebalancing.rcvd-bytes"),
	}

	totalBytes := snapshotBytesMetrics{
		sentBytes: getFirstStoreMetric(t, tc.Server(serverIdx), "range.snapshots.sent-bytes"),
		rcvdBytes: getFirstStoreMetric(t, tc.Server(serverIdx), "range.snapshots.rcvd-bytes"),
	}

	return totalBytes, granularMetrics
}

// getSnapshotMetricsDiff returns the delta between snapshot byte metrics
// recorded at different times. Metrics can be recorded using the
// getSnapshotBytesMetrics helper function, and the delta is returned in the
// form (totalBytes, granularMetrics). totalBytes is a
// snapshotBytesMetrics struct containing the difference in total bytes
// sent/received, and granularMetrics is the map of snapshotBytesMetrics structs
// containing deltas for each type of snapshot.
func getSnapshotMetricsDiff(
	beforeTotal snapshotBytesMetrics,
	beforeMap map[kvserverpb.SnapshotRequest_Priority]snapshotBytesMetrics,
	afterTotal snapshotBytesMetrics,
	afterMap map[kvserverpb.SnapshotRequest_Priority]snapshotBytesMetrics,
) (snapshotBytesMetrics, map[kvserverpb.SnapshotRequest_Priority]snapshotBytesMetrics) {
	diffTotal := snapshotBytesMetrics{
		sentBytes: afterTotal.sentBytes - beforeTotal.sentBytes,
		rcvdBytes: afterTotal.rcvdBytes - beforeTotal.rcvdBytes,
	}
	diffMap := map[kvserverpb.SnapshotRequest_Priority]snapshotBytesMetrics{
		kvserverpb.SnapshotRequest_REBALANCE: {
			sentBytes: afterMap[kvserverpb.SnapshotRequest_REBALANCE].sentBytes - beforeMap[kvserverpb.SnapshotRequest_REBALANCE].sentBytes,
			rcvdBytes: afterMap[kvserverpb.SnapshotRequest_REBALANCE].rcvdBytes - beforeMap[kvserverpb.SnapshotRequest_REBALANCE].rcvdBytes,
		},
		kvserverpb.SnapshotRequest_RECOVERY: {
			sentBytes: afterMap[kvserverpb.SnapshotRequest_RECOVERY].sentBytes - beforeMap[kvserverpb.SnapshotRequest_RECOVERY].sentBytes,
			rcvdBytes: afterMap[kvserverpb.SnapshotRequest_RECOVERY].rcvdBytes - beforeMap[kvserverpb.SnapshotRequest_RECOVERY].rcvdBytes,
		},
		kvserverpb.SnapshotRequest_UNKNOWN: {
			sentBytes: afterMap[kvserverpb.SnapshotRequest_UNKNOWN].sentBytes - beforeMap[kvserverpb.SnapshotRequest_UNKNOWN].sentBytes,
			rcvdBytes: afterMap[kvserverpb.SnapshotRequest_UNKNOWN].rcvdBytes - beforeMap[kvserverpb.SnapshotRequest_UNKNOWN].rcvdBytes,
		},
	}

	return diffTotal, diffMap
}

// This function returns the number of bytes sent for a snapshot. It follows the
// sending logic of kvBatchSnapshotStrategy.Send() but has one key difference.
//
// NB: This calculation assumes the snapshot size is less than
// `kv.snapshot_sender.batch_size` and will fit in a single storage.Batch.
func getExpectedSnapshotSizeBytes(
	ctx context.Context,
	originStore *kvserver.Store,
	originRepl *kvserver.Replica,
	snapType kvserverpb.SnapshotRequest_Type,
	recipientStoreID roachpb.StoreID,
) (int64, error) {
	snap, err := originRepl.GetSnapshot(ctx, snapType, recipientStoreID)
	if err != nil {
		return 0, err
	}
	defer snap.Close()

	b := originStore.Engine().NewUnindexedBatch(true)
	defer b.Close()

	err = rditer.IterateReplicaKeySpans(snap.State.Desc, snap.EngineSnap, true, /* replicatedOnly */
		func(iter storage.EngineIterator, _ roachpb.Span, keyType storage.IterKeyType) error {
			var err error
			for ok := true; ok && err == nil; ok, err = iter.NextEngineKey() {
				switch keyType {
				case storage.IterKeyTypePointsOnly:
					unsafeKey, err := iter.UnsafeEngineKey()
					if err != nil {
						return err
					}
					if err := b.PutEngineKey(unsafeKey, iter.UnsafeValue()); err != nil {
						return err
					}

				case storage.IterKeyTypeRangesOnly:
					bounds, err := iter.EngineRangeBounds()
					if err != nil {
						return err
					}
					for _, rkv := range iter.EngineRangeKeys() {
						err := b.PutEngineRangeKey(bounds.Key, bounds.EndKey, rkv.Version, rkv.Value)
						if err != nil {
							return err
						}
					}

				default:
					return errors.Errorf("unexpected key type %v", keyType)
				}
			}
			return err
		})
	return int64(b.Len()), err
}

// Tests the accuracy of the 'range.snapshots.rebalancing.rcvd-bytes' and
// 'range.snapshots.rebalancing.sent-bytes' metrics. This test adds a new
// replica to a cluster, and during the process, a learner snapshot is sent to
// the new replica.
func TestRebalancingSnapshotMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	knobs, ltk := makeReplicationTestKnobs()
	ltk.storeKnobs.DisableRaftSnapshotQueue = true

	// Synchronize on the moment before the snapshot gets sent so we can measure
	// the state at that time.
	blockUntilSnapshotSendCh := make(chan struct{})
	blockSnapshotSendCh := make(chan struct{})
	ltk.storeKnobs.SendSnapshot = func() {
		close(blockUntilSnapshotSendCh)
		select {
		case <-blockSnapshotSendCh:
		case <-time.After(10 * time.Second):
			return
		}
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	scratchStartKey := tc.ScratchRange(t)

	// Record the snapshot metrics before anything has been sent / received.
	senderTotalBefore, senderMetricsMapBefore := getSnapshotBytesMetrics(t, tc, 0 /* serverIdx */)
	receiverTotalBefore, receiverMetricsMapBefore := getSnapshotBytesMetrics(t, tc, 1 /* serverIdx */)

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		_, err := tc.AddVoters(scratchStartKey, tc.Target(1))
		return err
	})

	// Wait until the snapshot is about to be sent before calculating what the
	// snapshot size should be. This allows our snapshot measurement to account
	// for any state changes that happen between calling AddVoters and the
	// snapshot being sent.
	<-blockUntilSnapshotSendCh
	store, repl := getFirstStoreReplica(t, tc.Server(0), scratchStartKey)
	snapshotLength, err := getExpectedSnapshotSizeBytes(ctx, store, repl, kvserverpb.SnapshotRequest_INITIAL, tc.Server(1).GetFirstStoreID())
	require.NoError(t, err)

	close(blockSnapshotSendCh)
	require.NoError(t, g.Wait())

	// Record the snapshot metrics for the sender after a voter has been added. A
	// learner snapshot should have been sent from the sender to the receiver.
	senderTotalAfter, senderMetricsMapAfter := getSnapshotBytesMetrics(t, tc, 0)

	// Asserts that the learner snapshot (aka rebalancing snapshot) bytes sent
	// have been recorded and that it was not double counted in a different
	// metric.
	senderTotalDelta, senderMapDelta := getSnapshotMetricsDiff(senderTotalBefore, senderMetricsMapBefore, senderTotalAfter, senderMetricsMapAfter)

	senderTotalExpected := snapshotBytesMetrics{sentBytes: snapshotLength, rcvdBytes: 0}
	senderMapExpected := map[kvserverpb.SnapshotRequest_Priority]snapshotBytesMetrics{
		kvserverpb.SnapshotRequest_REBALANCE: {sentBytes: snapshotLength, rcvdBytes: 0},
		kvserverpb.SnapshotRequest_RECOVERY:  {sentBytes: 0, rcvdBytes: 0},
		kvserverpb.SnapshotRequest_UNKNOWN:   {sentBytes: 0, rcvdBytes: 0},
	}
	require.Equal(t, senderTotalExpected, senderTotalDelta)
	require.Equal(t, senderMapExpected, senderMapDelta)

	// Record the snapshot metrics for the receiver after a voter has been added.
	receiverTotalAfter, receiverMetricsMapAfter := getSnapshotBytesMetrics(t, tc, 1)

	// Asserts that the learner snapshot (aka rebalancing snapshot) bytes received
	// have been recorded and that it was not double counted in a different
	// metric.
	receiverTotalDelta, receiverMapDelta := getSnapshotMetricsDiff(receiverTotalBefore, receiverMetricsMapBefore, receiverTotalAfter, receiverMetricsMapAfter)

	receiverTotalExpected := snapshotBytesMetrics{sentBytes: 0, rcvdBytes: snapshotLength}
	receiverMapExpected := map[kvserverpb.SnapshotRequest_Priority]snapshotBytesMetrics{
		kvserverpb.SnapshotRequest_REBALANCE: {sentBytes: 0, rcvdBytes: snapshotLength},
		kvserverpb.SnapshotRequest_RECOVERY:  {sentBytes: 0, rcvdBytes: 0},
		kvserverpb.SnapshotRequest_UNKNOWN:   {sentBytes: 0, rcvdBytes: 0},
	}
	require.Equal(t, receiverTotalExpected, receiverTotalDelta)
	require.Equal(t, receiverMapExpected, receiverMapDelta)
}
