// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/leases"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftutil"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
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
	count, err := store.Metrics().GetStoreMetric(name)
	if err != nil {
		panic(err)
	}
	return count
}

func TestAddReplicaViaLearner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// The happy case! \o/

	blockUntilSnapshotCh := make(chan struct{})
	var receivedSnap int64
	blockSnapshotsCh := make(chan struct{})
	knobs, ltk := makeReplicationTestKnobs()
	ltk.storeKnobs.ReceiveSnapshot = func(_ context.Context, h *kvserverpb.SnapshotRequest_Header) error {
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
	ltk.storeKnobs.ReceiveSnapshot = func(_ context.Context, h *kvserverpb.SnapshotRequest_Header) error {
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
	ltk.storeKnobs.AfterSnapshotThrottle = func() {
		atomic.AddInt64(&count, -1)
	}
	ctx := context.Background()
	tc := testcluster.StartTestCluster(
		t, 3, base.TestClusterArgs{
			ServerArgs:      base.TestServerArgs{Knobs: knobs},
			ReplicationMode: base.ReplicationManual,
		},
	)

	defer tc.Stopper().Stop(ctx)

	// Disable delegating snapshots to different senders, which would otherwise
	// fail this test as snapshots could queue on different stores.
	settings := tc.Servers[0].ClusterSettings()
	sv := &settings.SV
	kvserver.NumDelegateLimit.Override(ctx, sv, 0)
	// Set snapshot send concurrency to 1.
	kvserver.SnapshotSendLimit.Override(ctx, sv, 1)
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
				kvpb.MakeReplicationChanges(roachpb.ADD_NON_VOTER, tc.Target(2)),
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
				ctx, scratch, desc, kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(1)),
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

type expectedMetric struct {
	DelegateSnapshotSuccesses int64
	DelegateSnapshotFailures  int64
	RangeSnapshotRecvFailed   int64
	RangeSnapshotRecvUnusable int64
}

func verifySnapshotMetrics(
	t *testing.T, tc *testcluster.TestCluster, expected map[int]expectedMetric,
) {
	for id, metrics := range expected {
		server := tc.Server(id)
		store, _ := server.GetStores().(*kvserver.Stores).GetStore(server.GetFirstStoreID())
		weakEqualf(t, metrics.DelegateSnapshotSuccesses, store.Metrics().DelegateSnapshotSuccesses.Count(), "metric successes, %d", id)
		weakEqualf(t, metrics.DelegateSnapshotFailures, store.Metrics().DelegateSnapshotFailures.Count(), "metric failures, %d", id)
		weakEqualf(t, metrics.RangeSnapshotRecvFailed, store.Metrics().RangeSnapshotRecvFailed.Count(), "metric recv failed, %d", id)
		weakEqualf(t, metrics.RangeSnapshotRecvUnusable, store.Metrics().RangeSnapshotRecvUnusable.Count(), "metric recv unusable, %d", id)
	}
}

const issuesFixed = false

// TODO(baptist): This should be require.Equalf, but this is not consistent
// enough. There are two reasons this is inconsistent.
// 1) Raft snapshots still sometimes sneak in. (#96841)
// 2) The delegate hasn't seen the updated descriptor in time.
func weakEqualf(t *testing.T, successes int64, count int64, s string, id int) {
	if issuesFixed {
		require.Equalf(t, successes, count, s, id)
	} else {
		if successes != count {
			log.Warningf(context.Background(), "Not equal, expected %d, got %d for %s %d", successes, count, s, id)
		}
	}
}

// TestDelegateSnapshot verifies that the correct delegate is chosen when
// sending snapshots to stores.
func TestDelegateSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// Record snapshots as they are sent on this channel for later analysis.
	requestChannel := make(chan *kvserverpb.DelegateSendSnapshotRequest, 2)
	knobs, ltk := makeReplicationTestKnobs()
	ltk.storeKnobs.SendSnapshot = func(request *kvserverpb.DelegateSendSnapshotRequest) {
		// TODO(abaptist): Remove this condition once 96841 is fixed. This
		// accounts spurious raft snapshots that are sent. Also disable the raft
		// snapshot queue using ltk.storeKnobs.DisableRaftSnapshotQueue = true.
		if request.SenderQueueName != kvserverpb.SnapshotRequest_RAFT_SNAPSHOT_QUEUE {
			requestChannel <- request
		}
	}

	localityA := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "a"}}}
	localityB := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "b"}}}
	localityServerArgs := map[int]base.TestServerArgs{
		0: {Knobs: knobs, Locality: localityA},
		1: {Knobs: knobs, Locality: localityA},
		2: {Knobs: knobs, Locality: localityB},
		3: {Knobs: knobs, Locality: localityB},
	}

	tc := testcluster.StartTestCluster(
		t, 4, base.TestClusterArgs{
			ServerArgsPerNode: localityServerArgs,
			ReplicationMode:   base.ReplicationManual,
		},
	)
	verifySnapshotMetrics(t, tc, map[int]expectedMetric{
		0: {0, 0, 0, 0},
		1: {0, 0, 0, 0},
		2: {0, 0, 0, 0},
		3: {0, 0, 0, 0},
	})

	scratchKey := tc.ScratchRange(t)
	defer tc.Stopper().Stop(ctx)
	// Node 3 (loc B) can only get the data from node 1 as it is the only replica.
	{
		_ = tc.AddVotersOrFatal(t, scratchKey, tc.Targets(2)...)
		request := <-requestChannel
		require.Equalf(t, request.DelegatedSender.StoreID, roachpb.StoreID(1), "Wrong sender for request %+v", request)
	}

	verifySnapshotMetrics(t, tc, map[int]expectedMetric{
		0: {0, 0, 0, 0},
		1: {0, 0, 0, 0},
		2: {0, 0, 0, 0},
		3: {0, 0, 0, 0},
	})

	// Node 4 (loc B) should get the delegated snapshot from node 3 which is the
	// same locality.
	{
		leaderDesc := tc.AddVotersOrFatal(t, scratchKey, tc.Targets(3)...)
		// Wait until we are sure the delegate store has received the descriptor. It
		// can't delegate until it receives the latest generation descriptor.
		testutils.SucceedsSoon(t, func() error {
			var desc roachpb.RangeDescriptor
			rKey := keys.MustAddr(scratchKey)
			require.NoError(t, tc.Servers[2].DB().GetProto(ctx, keys.RangeDescriptorKey(rKey), &desc))
			if desc.Generation != leaderDesc.Generation {
				return errors.Newf("Generation mismatch %d != %d", desc.Generation, leaderDesc.Generation)
			}
			return nil
		})

		request := <-requestChannel
		require.Equalf(t, request.DelegatedSender.StoreID, roachpb.StoreID(3), "Wrong type of request %+v", request)
		// TODO(abaptist): Remove this loop. Sometimes the delegated request fails
		// due to Raft updating the generation before we can delegate. Even with the
		// loop above to get the delegate on the correct generation, this is racy if
		// there is a raft snapshot. We fall back to not using our snapshot if the
		// generation fails, but this means that a second request is sent from the
		// leaseholder.
		for len(requestChannel) > 0 {
			<-requestChannel
		}

		verifySnapshotMetrics(t, tc, map[int]expectedMetric{
			0: {1, 0, 0, 0},
			1: {0, 0, 0, 0},
			2: {0, 0, 0, 0},
			3: {0, 0, 0, 0},
		})
	}

	// Node 2 (loc A) should get the snapshot from node 1 as they have the same locality.
	{
		_ = tc.AddVotersOrFatal(t, scratchKey, tc.Targets(1)...)
		request := <-requestChannel
		require.Equalf(t, request.DelegatedSender.StoreID, roachpb.StoreID(1), "Wrong type of request %+v", request)
	}
	verifySnapshotMetrics(t, tc, map[int]expectedMetric{
		0: {1, 0, 0, 0},
		1: {0, 0, 0, 0},
		2: {0, 0, 0, 0},
		3: {0, 0, 0, 0},
	})
}

// TestDelegateSnapshotFails is a test that ensure we fail fast when the
// sender or receiver store crashes during delegated snapshot sending.
func TestDelegateSnapshotFails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var senders struct {
		mu   syncutil.Mutex
		desc []roachpb.ReplicaDescriptor
	}

	setupFn := func(t *testing.T,
		receiveFunc func(context.Context, *kvserverpb.SnapshotRequest_Header) error,
		sendFunc func(*kvserverpb.DelegateSendSnapshotRequest),
		processRaft func(roachpb.StoreID) bool,
	) (
		*testcluster.TestCluster,
		roachpb.Key,
	) {
		senders.desc = nil
		knobs, ltk := makeReplicationTestKnobs()
		ltk.storeKnobs.ThrottleEmptySnapshots = true

		ltk.storeKnobs.SelectDelegateSnapshotSender =
			func(descriptor *roachpb.RangeDescriptor) []roachpb.ReplicaDescriptor {
				senders.mu.Lock()
				defer senders.mu.Unlock()
				return senders.desc
			}
		ltk.storeKnobs.ReceiveSnapshot = receiveFunc
		ltk.storeKnobs.SendSnapshot = sendFunc
		ltk.storeKnobs.DisableProcessRaft = processRaft

		tc := testcluster.StartTestCluster(
			t, 4, base.TestClusterArgs{
				ServerArgs:      base.TestServerArgs{Knobs: knobs},
				ReplicationMode: base.ReplicationManual,
			},
		)

		scratchKey := tc.ScratchRange(t)
		return tc, scratchKey
	}

	// Add a learner replica that will need a snapshot, kill the server
	// the learner is on. Assert that the failure is detected and change replicas
	// fails fast.
	t.Run("receiver", func(t *testing.T) {
		tc, scratchKey := setupFn(t, nil, nil, nil)
		defer tc.Stopper().Stop(ctx)

		desc, err := tc.LookupRange(scratchKey)
		require.NoError(t, err, "Unable to lookup the range")

		_, err = setupPartitionedRange(tc, desc.RangeID, 0, 0, true, unreliableRaftHandlerFuncs{})
		require.NoError(t, err)

		_, err = tc.Servers[0].DB().AdminChangeReplicas(
			ctx, scratchKey, desc, kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(1)),
		)

		require.True(t, testutils.IsError(err, "partitioned"), `expected partitioned error got: %+v`, err)
		verifySnapshotMetrics(t, tc, map[int]expectedMetric{
			0: {0, 0, 0, 0},
			1: {0, 0, 0, 0},
			2: {0, 0, 0, 0},
			3: {0, 0, 0, 0},
		})
	})

	// Add a follower replica to act as the snapshot sender, and kill the server
	// the sender is on. Assert that the failure is detected and change replicas
	// fails fast.
	t.Run("sender_no_fallback", func(t *testing.T) {
		tc, scratchKey := setupFn(t, nil, nil, nil)
		defer tc.Stopper().Stop(ctx)

		// Add a replica that will be the delegated sender, and another so we have
		// quorum with this node down.
		desc := tc.AddVotersOrFatal(t, scratchKey, tc.Targets(2, 3)...)

		// Always use node 3 (index 2) as the only delegate.
		senders.mu.Lock()
		senders.desc = append(senders.desc, roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3})
		senders.mu.Unlock()

		// Now stop accepting traffic to node 3 (index 2).
		_, err := setupPartitionedRange(tc, desc.RangeID, 0, 2, true, unreliableRaftHandlerFuncs{})
		require.NoError(t, err)

		_, err = tc.Servers[0].DB().AdminChangeReplicas(
			ctx, scratchKey, desc, kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(1)),
		)
		// The delegate can not send this request since it does not have the latest
		// generation descriptor.
		require.ErrorContains(t, err, "generation has changed")
		verifySnapshotMetrics(t, tc, map[int]expectedMetric{
			0: {0, 1, 0, 0},
			1: {0, 0, 0, 0},
			2: {0, 0, 0, 0},
			3: {0, 0, 0, 0},
		})
	})

	// Identical setup as the previous test, but allow a fallback to the leaseholder.
	t.Run("sender_with_fallback", func(t *testing.T) {
		tc, scratchKey := setupFn(t, nil, nil, nil)
		defer tc.Stopper().Stop(ctx)

		// Add a replica that will be the delegated sender, and another so we have
		// quorum with this node down
		desc := tc.AddVotersOrFatal(t, scratchKey, tc.Targets(2, 3)...)

		// First try to use node 3 (index 2) as the delegate, but fall back to the leaseholder on failure.
		senders.mu.Lock()
		senders.desc = append(senders.desc, roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3})
		senders.desc = append(senders.desc, roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 1})
		senders.mu.Unlock()

		// Now stop accepting traffic to node 3 (index 2).
		_, err := setupPartitionedRange(tc, desc.RangeID, 0, 2, true, unreliableRaftHandlerFuncs{})
		require.NoError(t, err)

		_, err = tc.Servers[0].DB().AdminChangeReplicas(
			ctx, scratchKey, desc, kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(1)),
		)
		require.NoError(t, err)
		verifySnapshotMetrics(t, tc, map[int]expectedMetric{
			0: {0, 1, 0, 0},
			1: {0, 0, 0, 0},
			2: {0, 0, 0, 0},
			3: {0, 0, 0, 0},
		})
	})
	t.Run("receiver_rejects", func(t *testing.T) {
		var block atomic.Int32
		tc, scratchKey := setupFn(
			t,
			func(_ context.Context, h *kvserverpb.SnapshotRequest_Header) error {
				// TODO(abaptist): Remove this check once #96841 is fixed.
				if h.SenderQueueName == kvserverpb.SnapshotRequest_RAFT_SNAPSHOT_QUEUE {
					return nil
				}
				if val := block.Load(); val > 0 {
					block.Add(-1)
					return errors.Newf("BAM: receive error %d", val)
				}
				return nil
			},
			nil,
			nil,
		)
		defer tc.Stopper().Stop(ctx)

		// Add a replica that will be the delegated sender, and another so we have
		// quorum with this node down
		desc := tc.AddVotersOrFatal(t, scratchKey, tc.Targets(2, 3)...)

		// First try to use node 3 (index 2) as the delegate, but fall back to the leaseholder on failure.
		senders.mu.Lock()
		senders.desc = append(senders.desc, roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3})
		senders.desc = append(senders.desc, roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 1})
		senders.mu.Unlock()

		block.Store(2)
		_, err := tc.Servers[0].DB().AdminChangeReplicas(
			ctx, scratchKey, desc, kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(1)),
		)
		require.ErrorContains(t, err, "BAM: receive error")

		// There will be two attempts to send this, both fail.
		verifySnapshotMetrics(t, tc, map[int]expectedMetric{
			0: {0, 1, 0, 0},
			1: {0, 0, 2, 0},
			2: {0, 0, 0, 0},
			3: {0, 0, 0, 0},
		})
	})
	// Test that the delegate that doesn't have the snapshot that we fall back to
	// the leaseholder and correctly increment the stats.
	t.Run("delegate_missing_range", func(t *testing.T) {
		tc, scratchKey := setupFn(t, nil, nil, nil)
		defer tc.Stopper().Stop(ctx)
		desc := tc.AddVotersOrFatal(t, scratchKey, tc.Targets(2)...)

		// First try to use node 4 (index 3) as the delegate, but fall back to the leaseholder on failure.
		senders.mu.Lock()
		senders.desc = append(senders.desc, roachpb.ReplicaDescriptor{NodeID: 4, StoreID: 4})
		senders.desc = append(senders.desc, roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 1})
		senders.mu.Unlock()

		_, err := tc.Servers[0].DB().AdminChangeReplicas(
			ctx, scratchKey, desc, kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(1)),
		)
		require.NoError(t, err)

		verifySnapshotMetrics(t, tc, map[int]expectedMetric{
			0: {0, 1, 0, 0},
			1: {0, 0, 0, 0},
			2: {0, 0, 0, 0},
			3: {0, 0, 0, 0},
		})
	})

	// NB: This test indicates a potential problem with delegated snapshots as
	// they currently work. The generation changes on a change replica request,
	// however if the delegation request races ahead of the Raft level update,
	// then the delegate will not be used. In testing we don't see this often,
	// however it is something to watch out for especially on overloaded servers.
	t.Run("delegate_raft_slow", func(t *testing.T) {
		var block atomic.Bool
		tc, scratchKey := setupFn(t, nil, nil,
			func(id roachpb.StoreID) bool {
				return id == 4 && block.Load()
			})
		defer tc.Stopper().Stop(ctx)
		desc := tc.AddVotersOrFatal(t, scratchKey, tc.Targets(2, 3)...)

		// Choose the store which we are about to block.
		senders.mu.Lock()
		senders.desc = append(senders.desc, roachpb.ReplicaDescriptor{NodeID: 4, StoreID: 4})
		senders.mu.Unlock()

		// Don't allow store 4 to see the new descriptor through Raft.
		block.Store(true)
		_, err := tc.Servers[0].DB().AdminChangeReplicas(
			ctx, scratchKey, desc, kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(1)),
		)
		require.ErrorContains(t, err, "generation has changed")

		verifySnapshotMetrics(t, tc, map[int]expectedMetric{
			0: {0, 1, 0, 0},
			1: {0, 0, 0, 0},
			2: {0, 0, 0, 0},
			3: {0, 0, 0, 0},
		})
	})

	// This test ensures that the leader doesn't truncate while the delegate
	// snapshot is in flight. Right after the leader sends the delegate request,
	// but before the snapshot has been created, truncate the log. This test is
	// not "as good" as it could be since the new node is in the probe state so
	// the log truncation constraints aren't really necessary since we don't
	// truncate anything in that state. This test will be better if it could be
	// tested on Raft snapshots.
	t.Run("truncate_during_send", func(t *testing.T) {
		var blockRaft atomic.Bool
		var truncateLog func()

		tc, scratchKey := setupFn(t, nil,
			func(*kvserverpb.DelegateSendSnapshotRequest) {
				if blockRaft.Load() {
					truncateLog()
				}
			}, nil)
		defer tc.Stopper().Stop(ctx)
		// This will truncate the log on the first store.
		truncateLog = func() {
			server := tc.Servers[0]
			store, _ := server.GetStores().(*kvserver.Stores).GetStore(server.GetFirstStoreID())
			store.MustForceRaftLogScanAndProcess()
		}

		desc := tc.AddVotersOrFatal(t, scratchKey, tc.Targets(2, 3)...)

		// Chose a delegate to block.
		senders.mu.Lock()
		senders.desc = append(senders.desc, roachpb.ReplicaDescriptor{NodeID: 4, StoreID: 4})
		senders.desc = append(senders.desc, roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 1})
		senders.mu.Unlock()
		// First try to use node 3 (index 2) as the delegate, but fall back to the leaseholder on failure.

		// Don't allow the new store to see Raft updates.
		blockRaft.Store(true)
		_, err := tc.Servers[0].DB().AdminChangeReplicas(
			ctx, scratchKey, desc, kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(1)),
		)
		require.NoError(t, err)

		verifySnapshotMetrics(t, tc, map[int]expectedMetric{
			0: {1, 0, 0, 0},
			1: {0, 0, 0, 0},
			2: {0, 0, 0, 0},
			3: {0, 0, 0, 0},
		})
	})
}

func TestLearnerRaftConfState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	verifyLearnerInRaftOnNodes := func(
		key roachpb.Key, id roachpb.ReplicaID, servers []serverutils.TestServerInterface,
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
				if _, ok := status.Config.Learners[raftpb.PeerID(id)]; !ok {
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
		var rejectSnapshotErr atomic.Value // error
		knobs, ltk := makeReplicationTestKnobs()
		ltk.storeKnobs.ReceiveSnapshot = func(_ context.Context, h *kvserverpb.SnapshotRequest_Header) error {
			if err := rejectSnapshotErr.Load().(error); err != nil {
				return err
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
		rejectSnapshotErr.Store(errors.New("boom"))
		var err error
		switch replicaType {
		case roachpb.LEARNER:
			_, err = tc.AddVoters(scratchStartKey, tc.Target(1))
		case roachpb.NON_VOTER:
			_, err = tc.AddNonVoters(scratchStartKey, tc.Target(1))
		default:
			log.Fatalf(ctx, "unexpected replicaType: %s", replicaType)
		}

		if !testutils.IsError(err, `remote couldn't accept snapshot`) {
			t.Fatalf(`expected "remote couldn't accept snapshot" error got: %+v`, err)
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

	tc := testcluster.StartTestCluster(
		t, 2, base.TestClusterArgs{
			ServerArgs:      base.TestServerArgs{Knobs: knobs},
			ReplicationMode: base.ReplicationManual,
		},
	)
	defer tc.Stopper().Stop(ctx)
	scratchStartKey := tc.ScratchRange(t)
	g, ctx := errgroup.WithContext(ctx)

	metrics := []string{".rebalancing", ".recovery", ""}
	// Record the snapshot metrics before anything has been sent / received.
	senderMetricsMapBefore := getSnapshotBytesMetrics(t, tc, 0 /* serverIdx */, metrics)
	receiverMetricsMapBefore := getSnapshotBytesMetrics(t, tc, 1 /* serverIdx */, metrics)

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
		client := tc.GetAdminClient(t, drainingServerIdx)
		drain(ctx, t, client, drainingNodeID)
	}

	testutils.SucceedsSoon(t, func() error {
		// Manually enqueue the leaseholder replica into its store's raft snapshot
		// queue. We expect it to pick up on the fact that the non-voter on its range
		// needs a snapshot.
		ctx, rec := tracing.ContextWithRecordingSpan(ctx, leaseholderStore.GetStoreConfig().Tracer(), "trace-enqueue")
		pErr, err := leaseholderStore.Enqueue(
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
		matched, err := regexp.MatchString("streamed snapshot.*to.*NON_VOTER", rec().String())
		if err != nil {
			return err
		}
		if !matched {
			return errors.Errorf("the raft snapshot queue did not send a snapshot to the non-voter")
		}
		return nil
	})

	// AddNonVoter will return after the snapshot is sent. Wait for it to do so
	// before checking asserting on snapshot sent/received metrics.
	require.NoError(t, g.Wait())
	// Record metrics.
	senderMetricsMapAfter := getSnapshotBytesMetrics(t, tc, 0, metrics)
	receiverMetricsMapAfter := getSnapshotBytesMetrics(t, tc, 1, metrics)

	// Assert that the raft snapshot (aka recovery snapshot) bytes sent have been
	// recorded and that they were not double counted in the rebalancing metric.
	senderMapDelta := getSnapshotMetricsDiff(senderMetricsMapBefore, senderMetricsMapAfter)
	require.Greater(t, senderMapDelta[".recovery"].sentBytes, int64(0))
	require.Equal(t, int64(0), senderMapDelta[".rebalancing"].sentBytes)
	require.Equal(t, senderMapDelta[""], senderMapDelta[".recovery"])

	// Assert that the raft snapshot (aka recovery snapshot) bytes received have
	// been recorded and that they were not double counted in the rebalancing
	// metric.
	receiverMapDelta := getSnapshotMetricsDiff(receiverMetricsMapBefore, receiverMetricsMapAfter)
	require.Greater(t, receiverMapDelta[".recovery"].rcvdBytes, int64(0))
	require.Equal(t, int64(0), receiverMapDelta[".rebalancing"].rcvdBytes)
	require.Equal(t, receiverMapDelta[""], receiverMapDelta[".recovery"])
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
		client := tc.GetAdminClient(t, drainingServerIdx)
		drain(ctx, t, client, drainingNodeID)

		// Now, we try to add a replica to it, we expect that to fail.
		scratchKey := tc.ScratchRange(t)
		_, err := tc.AddVoters(scratchKey, makeReplicationTargets(drainingNodeID)...)
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
		errFn func(*kvpb.TransferLeaseRequest) error
	}{
		{
			name: "targetMayNeedSnapshot",
			errFn: func(req *kvpb.TransferLeaseRequest) error {
				repl := req.Lease.Replica
				status := raftutil.ReplicaStateProbe
				return leases.NewLeaseTransferRejectedBecauseTargetMayNeedSnapshotError(repl, status)
			},
		},
		{
			name: "replicaNotFound",
			errFn: func(_ *kvpb.TransferLeaseRequest) error {
				return roachpb.ErrReplicaNotFound
			},
		},
		{
			name: "replicaCannotHoldLease",
			errFn: func(_ *kvpb.TransferLeaseRequest) error {
				return roachpb.ErrReplicaCannotHoldLease
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var rangeIDAtomic int64
			var rejectedCount int
			const maxRejects = 3
			reqFilter := func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
				rangeID := roachpb.RangeID(atomic.LoadInt64(&rangeIDAtomic))
				if ba.RangeID == rangeID && ba.IsSingleTransferLeaseRequest() && rejectedCount < maxRejects {
					rejectedCount++
					req := ba.Requests[0].GetTransferLease()
					err := tc.errFn(req)
					return kvpb.NewError(err)
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
		store.TestingSetReplicateQueueActive(true)
		traceCtx, finish := tracing.ContextWithRecordingSpan(ctx, store.GetStoreConfig().Tracer(), "trace-enqueue")
		processErr, err := store.Enqueue(
			traceCtx, "replicate", repl, true /* skipShouldQueue */, false, /* async */
		)
		require.NoError(t, err)
		require.NoError(t, processErr)
		action := "next replica action: remove learner"
		require.NoError(t, testutils.MatchInOrder(finish().String(), []string{action}...))
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
		store.TestingSetReplicateQueueActive(false)
	}

	// Create a VOTER_OUTGOING, i.e. a joint configuration.
	ltk.withStopAfterJointConfig(func() {
		desc := tc.RemoveVotersOrFatal(t, scratchStartKey, tc.Target(2))
		require.True(t, desc.Replicas().InAtomicReplicationChange(), desc)
		store.TestingSetReplicateQueueActive(true)
		traceCtx, finish := tracing.ContextWithRecordingSpan(ctx, store.GetStoreConfig().Tracer(), "trace-enqueue")
		processErr, err := store.Enqueue(
			traceCtx, "replicate", repl, true /* skipShouldQueue */, false, /* async */
		)
		require.NoError(t, err)
		require.NoError(t, processErr)
		action := "next replica action: finalize conf change"
		require.NoError(t, testutils.MatchInOrder(finish().String(), []string{action}...))

		testutils.SucceedsSoon(t, func() error {
			desc = tc.LookupRangeOrFatal(t, scratchStartKey)
			if len(desc.Replicas().VoterDescriptors()) != 3 {
				return errors.Newf("Mismatch in num voters %v, desc: %v", desc.Replicas().VoterDescriptors(), desc)
			}
			return nil
		})
		store.TestingSetReplicateQueueActive(false)
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
		traceCtx, rec := tracing.ContextWithRecordingSpan(ctx, store.GetStoreConfig().Tracer(), "trace-enqueue")
		processErr, err := store.Enqueue(
			traceCtx, "replicaGC", repl, true /* skipShouldQueue */, false, /* async */
		)
		require.NoError(t, err)
		require.NoError(t, processErr)
		const msg = `not gc'able, replica is still in range descriptor: (n2,s2):`
		require.Contains(t, rec().String(), msg)
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
	ltk.storeKnobs.ReceiveSnapshot = func(_ context.Context, h *kvserverpb.SnapshotRequest_Header) error {
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
		traceCtx, rec := tracing.ContextWithRecordingSpan(ctx, store.GetStoreConfig().Tracer(), "trace-enqueue")
		processErr, err := store.Enqueue(
			traceCtx, "raftsnapshot", repl, true /* skipShouldQueue */, false, /* async */
		)
		if err != nil {
			return err
		}
		if processErr != nil {
			return processErr
		}
		const msg = `skipping snapshot; replica is likely a LEARNER in the process of being added: (n2,s2):2LEARNER`
		formattedTrace := rec().String()
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
	ltk.storeKnobs.ReceiveSnapshot = func(_ context.Context, h *kvserverpb.SnapshotRequest_Header) error {
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
			ctx, scratchStartKey, desc, kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(1)),
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

	ctx := context.Background()
	var tc *testcluster.TestCluster

	var skipReceiveSnapshotKnobAtomic int64 = 1
	knobs, ltk := makeReplicationTestKnobs()
	// We must disable eager replica removal to make this test reliable.
	// If we don't then it's possible that the removed replica on store 2 will
	// notice it's removed before the snapshot is sent by the replicate queue.
	// In this case we'll get a snapshot error from the replicate queue which
	// will retry the up-replication with a new descriptor and succeed.
	ltk.storeKnobs.DisableEagerReplicaRemoval = true
	ltk.storeKnobs.VoterAddStopAfterLearnerSnapshot = func(targets []roachpb.ReplicationTarget) bool {
		// We need to be careful not to interfere with up-replication to node 2
		// during test setup and only invoke "concurrent queue" behavior for
		// specific target which is on node 3.
		if targets[0].NodeID != 3 {
			return false
		}
		// Remove the learner on node 3 out from under the replicate queue. This
		// simulates a second replicate queue running concurrently. The first thing
		// this second replicate queue would do is remove any learners it sees,
		// leaving the 2 voters.
		startKey := tc.ScratchRange(t)
		desc, err := tc.RemoveVoters(startKey, tc.Target(2))
		// NB: don't fatal on this goroutine, as we can't recover cleanly.
		assert.NoError(t, err)
		assert.Len(t, desc.Replicas().VoterDescriptors(), 2)
		assert.Len(t, desc.Replicas().LearnerDescriptors(), 0)
		return false
	}
	tc = testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)
	settings := tc.Servers[0].ClusterSettings()
	sv := &settings.SV
	// Set snapshot send concurrency to 1.
	kvserver.SnapshotSendLimit.Override(ctx, sv, 1)
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
			traceCtx, rec := tracing.ContextWithRecordingSpan(ctx, store.GetStoreConfig().Tracer(), "trace-enqueue")
			processErr, err := store.Enqueue(
				traceCtx, "replicate", repl, true /* skipShouldQueue */, false, /* async */
			)
			if err != nil {
				return err
			}
			if processErr == nil || !strings.Contains(processErr.Error(), `descriptor changed`) {
				return errors.Wrap(processErr, `expected "descriptor changed" error got: %+v`)
			}
			formattedTrace := rec().String()
			expectedMessages := []string{
				`could not promote .*?n3,s3.*? to voter, rolling back:.*?change replicas of r\d+ failed: descriptor changed`,
				`learner to roll back not found`,
			}
			return testutils.MatchInOrder(formattedTrace, expectedMessages...)
		}()
	}()

	require.NoError(t, <-queue1ErrCh)
	desc := tc.LookupRangeOrFatal(t, scratchStartKey)
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
	db.Exec(t, fmt.Sprintf(`SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '%s'`, testingRangeFeedInterval))
	db.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.follower_reads.enabled = true`)

	scratchStartKey := tc.ScratchRange(t)
	var scratchDesc roachpb.RangeDescriptor
	ltk.withStopAfterLearnerAtomic(func() {
		scratchDesc = tc.AddVotersOrFatal(t, scratchStartKey, tc.Target(1))
	})

	check := func() {
		ts := tc.Server(0).Clock().Now()
		txn := roachpb.MakeTransaction("txn", nil, 0, 0, ts, 0, int32(tc.Server(0).SQLInstanceID()), 0, false /* omitInRangefeeds */)
		req := &kvpb.BatchRequest{Header: kvpb.Header{
			RangeID:   scratchDesc.RangeID,
			Timestamp: ts,
			Txn:       &txn,
		}}
		req.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{
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

	tc.WaitForNodeLiveness(t)

	scratchStartKey := tc.ScratchRange(t)
	ltk.withStopAfterLearnerAtomic(func() {
		_ = tc.AddVotersOrFatal(t, scratchStartKey, tc.Target(1))
		_ = tc.AddVotersOrFatal(t, scratchStartKey, tc.Target(2))
	})

	{
		// Ensure that the test starts with the expected number of learners:
		// (n1,s1):1, (n2,s2):2 LEARNER, (n3,s3):3 LEARNER
		desc := tc.LookupRangeOrFatal(t, scratchStartKey)
		require.Len(t, desc.Replicas().LearnerDescriptors(), 2)
	}

	// Assert that initially there are no snapshots in flight to learners. This
	// is an important precondition before testing AdminRelocateRange below, as
	// AdminRelocateRange won't handle learners which have inflight snapshots and
	// this test will flake.
	testutils.SucceedsSoon(t, func() error {
		desc := tc.LookupRangeOrFatal(t, scratchStartKey)
		repl, err := tc.GetFirstStoreFromServer(t, 0).GetReplica(desc.RangeID)
		require.NoError(t, err)
		if repl.HasOutstandingLearnerSnapshotInFlightForTesting() {
			return errors.Errorf("outstanding learner snapshot in flight %s", desc)
		}
		return nil
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
	var snapshotStarted int64
	blockSnapshot := make(chan struct{})
	snapshotInProgress := make(chan struct{})
	tc := testcluster.StartTestCluster(
		t, 2, base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						// Disable load-based splitting, so that the absence of sufficient
						// QPS measurements do not prevent ranges from merging.
						DisableLoadBasedSplitting: true,
						ReceiveSnapshot: func(_ context.Context, _ *kvserverpb.SnapshotRequest_Header) error {
							if atomic.LoadInt64(&activateSnapshotTestingKnob) == 1 {
								// While the snapshot RPC should only happen once given
								// that the cluster is running under manual replication,
								// retries or other mechanisms can cause this to be called
								// multiple times, so let's ensure we only close the channel
								// snapshotInProgress once by using the snapshotStarted flag.
								if atomic.CompareAndSwapInt64(&snapshotStarted, 0, 1) {
									close(snapshotInProgress)
								}
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
	case <-snapshotInProgress:
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
	db.Exec(t, `SET CLUSTER SETTING kv.range_merge.queue.enabled = true`)

	// While this replication change is stalled, we'll trigger a merge and
	// ensure that the merge correctly notices that there is a snapshot in
	// flight and ignores the range.
	store, repl := getFirstStoreReplica(t, tc.Server(0), scratchKey)
	processErr, enqueueErr := store.Enqueue(
		ctx, "merge", repl, true /* skipShouldQueue */, false, /* async */
	)
	require.NoError(t, enqueueErr)
	require.Truef(t, kvserver.IsReplicationChangeInProgressError(processErr),
		"expected replication change in progress error, got %+v", processErr)
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
	db.Exec(t, `SET CLUSTER SETTING kv.range_merge.queue.enabled = true`)

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
		traceCtx, rec := tracing.ContextWithRecordingSpan(ctx, store.GetStoreConfig().Tracer(), "trace-enqueue")
		processErr, err := store.Enqueue(
			traceCtx, "merge", repl, true /* skipShouldQueue */, false, /* async */
		)
		require.NoError(t, err)
		require.NoError(t, processErr)
		formattedTrace := rec().String()
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
			traceCtx, rec := tracing.ContextWithRecordingSpan(ctx, store.GetStoreConfig().Tracer(), "trace-enqueue")
			processErr, err := store.Enqueue(
				traceCtx, "merge", repl, true /* skipShouldQueue */, false, /* async */
			)
			require.NoError(t, err)
			require.NoError(t, processErr)
			formattedTrace := rec().String()
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

// getSnapshotBytesMetrics retrieves the count of each snapshot metric specified
// in the metricsName associated with the target serverIdx server and returns
// the result as a map. The keys in the map correspond to the strings in input
// metricsName. The corresponding value is a `snapshotBytesMetrics` struct
// containing the total bytes sent/received of the metric.
func getSnapshotBytesMetrics(
	t *testing.T, tc *testcluster.TestCluster, serverIdx int, metricsName []string,
) map[string]snapshotBytesMetrics {
	metrics := make(map[string]snapshotBytesMetrics)

	findSnapshotBytesMetrics := func(metricName string) snapshotBytesMetrics {
		sentMetricStr := fmt.Sprintf("range.snapshots%v.sent-bytes", metricName)
		rcvdMetricStr := fmt.Sprintf("range.snapshots%v.rcvd-bytes", metricName)
		return snapshotBytesMetrics{
			sentBytes: getFirstStoreMetric(t, tc.Server(serverIdx), sentMetricStr),
			rcvdBytes: getFirstStoreMetric(t, tc.Server(serverIdx), rcvdMetricStr),
		}
	}

	for _, v := range metricsName {
		metrics[v] = findSnapshotBytesMetrics(v)
	}
	return metrics
}

// getSnapshotMetricsDiff returns the difference between the values of
// corresponding snapshot metrics in two maps. Assumption: beforeMap and
// afterMap contain the same set of keys.
func getSnapshotMetricsDiff(
	beforeMap map[string]snapshotBytesMetrics, afterMap map[string]snapshotBytesMetrics,
) map[string]snapshotBytesMetrics {
	diffMap := make(map[string]snapshotBytesMetrics)
	for metricName, beforeValue := range beforeMap {
		diffMap[metricName] = snapshotBytesMetrics{
			afterMap[metricName].sentBytes - beforeValue.sentBytes,
			afterMap[metricName].rcvdBytes - beforeValue.rcvdBytes,
		}
	}
	return diffMap
}

// This function returns the number of bytes sent for a snapshot. It follows the
// sending logic of kvBatchSnapshotStrategy.Send() but has one key difference.
//
// NB: This calculation assumes the snapshot size is less than
// `kv.snapshot_sender.batch_size` and will fit in a single storage.Batch.
func getExpectedSnapshotSizeBytes(
	ctx context.Context, originStore *kvserver.Store, originRepl *kvserver.Replica,
) (int64, error) {
	snap, err := originRepl.GetSnapshot(ctx, uuid.MakeV4())
	if err != nil {
		return 0, err
	}
	defer snap.Close()

	b := originStore.TODOEngine().NewWriteBatch()
	defer b.Close()

	err = rditer.IterateReplicaKeySpans(
		ctx, snap.State.Desc, snap.EngineSnap, true /* replicatedOnly */, rditer.ReplicatedSpansAll,
		func(iter storage.EngineIterator, _ roachpb.Span) error {
			var err error
			for ok := true; ok && err == nil; ok, err = iter.NextEngineKey() {
				hasPoint, hasRange := iter.HasPointAndRange()

				if hasPoint {
					unsafeKey, err := iter.UnsafeEngineKey()
					if err != nil {
						return err
					}
					v, err := iter.UnsafeValue()
					if err != nil {
						return err
					}
					if err := b.PutEngineKey(unsafeKey, v); err != nil {
						return err
					}
				}
				if hasRange && iter.RangeKeyChanged() {
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
				}
			}
			return err
		})
	return int64(b.Len()), err
}

// This test verifies the accuracy of snapshot metrics -
// `range.snapshots.[rebalancing|cross-region|cross-zone].rcvd-bytes` and
// `range.snapshots.[rebalancing|cross-region|cross-zone].sent-bytes`. It
// involves adding two new replicas on different nodes within the cluster,
// resulting in two learner snapshots sent across. The test then compares the
// metrics prior to and after sending the snapshot to verify the accuracy.
func TestRebalancingAndCrossRegionZoneSnapshotMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	knobs, ltk := makeReplicationTestKnobs()
	ltk.storeKnobs.DisableRaftSnapshotQueue = true

	blockUntilSnapshotSendCh := make(chan struct{})
	blockSnapshotSendCh := make(chan struct{})
	ltk.storeKnobs.SendSnapshot = func(request *kvserverpb.DelegateSendSnapshotRequest) {
		// This testing knob allows accurate calculation of expected snapshot bytes
		// by unblocking the current goroutine when `HandleDelegatedSnapshot` is
		// about to send the snapshot. In addition, it also blocks the new
		// goroutine, which was created to send the snapshot, until the calculation
		// is complete (more info below).
		close(blockUntilSnapshotSendCh)
		select {
		case <-blockSnapshotSendCh:
		case <-time.After(10 * time.Second):
			return
		}
	}

	// The initial setup ensures the correct configuration for three nodes (with
	// different localities), single-range.
	const numNodes = 3
	serverArgs := make(map[int]base.TestServerArgs)

	// The servers localities are configured so that the first snapshot sent from
	// server0 to server1 is cross-region. The second snapshot sent from server0
	// to server2 is cross-zone within same region.
	serverLocality := [numNodes]roachpb.Locality{
		{Tiers: []roachpb.Tier{{Key: "region", Value: "us-east"}, {Key: "az", Value: "us-east-1"}}},
		{Tiers: []roachpb.Tier{{Key: "region", Value: "us-west"}, {Key: "az", Value: "us-west-1"}}},
		{Tiers: []roachpb.Tier{{Key: "region", Value: "us-east"}, {Key: "az", Value: "us-east-2"}}},
	}
	for i := 0; i < numNodes; i++ {
		serverArgs[i] = base.TestServerArgs{
			Locality: serverLocality[i],
			Knobs:    knobs,
		}
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(
		t, numNodes, base.TestClusterArgs{
			ServerArgsPerNode: serverArgs,
			ReplicationMode:   base.ReplicationManual,
		},
	)

	defer tc.Stopper().Stop(ctx)

	scratchStartKey := tc.ScratchRange(t)
	desc := tc.LookupRangeOrFatal(t, scratchStartKey)
	// Wait for the expiration lease to upgrade to an epoch or leader lease.
	// Otherwise, the lease upgrade may race with the snapshot calculation below
	// and result in a different size snapshot than expected. For epoch leases
	// this is actually not necessary because the first lease that's proposed is
	// an epoch lease. For leader leases, however, the range starts off with no
	// leader so the first lease that's proposed is an expiration lease, which
	// gets upgraded to a leader lease once a leader is elected.
	tc.MaybeWaitForLeaseUpgrade(ctx, t, desc)
	// sendSnapshotFromServer is a testing helper that sends a learner snapshot
	// from server[0] to server[serverIndex] and returns the expected size (in
	// bytes) of the snapshot sent.
	sendSnapshotToServer := func(serverIndex int, changeReplicaFn func(roachpb.Key, ...roachpb.ReplicationTarget) (roachpb.RangeDescriptor, error)) int64 {
		blockUntilSnapshotSendCh = make(chan struct{})
		blockSnapshotSendCh = make(chan struct{})
		g := ctxgroup.WithContext(ctx)
		g.GoCtx(func(ctx context.Context) error {
			// A new replica at servers[serverIndex] is now added to the cluster,
			// resulting in a learner snapshot to be sent from servers[0] to
			// servers[serverIndex]. This function is executed in a new goroutine to
			// help us capture the expected snapshot bytes count accurately.
			desc := tc.LookupRangeOrFatal(t, scratchStartKey)
			desc, err := changeReplicaFn(scratchStartKey, tc.Target(serverIndex))
			scratchStartKey = desc.StartKey.AsRawKey()
			return err
		})

		// The current goroutine is blocked until the new goroutine, which has just
		// been added, is about to send the snapshot (see the testing knob above).
		// This allows us to calculate the snapshot bytes count accurately,
		// accounting for any state changes that happen between calling
		// changeReplicaFn and the snapshot being sent.
		<-blockUntilSnapshotSendCh
		store, repl := getFirstStoreReplica(t, tc.Server(0), scratchStartKey)
		snapshotLength, err := getExpectedSnapshotSizeBytes(ctx, store, repl)
		require.NoError(t, err)

		close(blockSnapshotSendCh)
		// Wait the new goroutine (sending the snapshot) to complete before
		// measuring the after-sending-snapshot metrics.
		require.NoError(t, g.Wait())
		return snapshotLength
	}

	metrics := []string{".rebalancing", ".recovery", ".cross-region", ".cross-zone", ""}
	// Record the snapshot metrics before anything has been sent / received.
	senderBefore := getSnapshotBytesMetrics(t, tc, 0 /* serverIdx */, metrics)
	firstReceiverBefore := getSnapshotBytesMetrics(t, tc, 1 /* serverIdx */, metrics)
	secReceiverBefore := getSnapshotBytesMetrics(t, tc, 2 /* serverIdx */, metrics)

	// The first replica is added as a non-voter to help avoid failure in stress
	// testing. A possible explanation in the failure is - if the first replica
	// was added as a voter, it can be stuck in a state to receive the snapshot.
	// This can cause failure to reach quorum during the second snapshot transfer.
	firstSnapshotLength := sendSnapshotToServer(1, tc.AddNonVoters)
	secSnapshotLength := sendSnapshotToServer(2, tc.AddVoters)
	totalSnapshotLength := firstSnapshotLength + secSnapshotLength

	// A learner snapshot should have been sent from the sender(server[0]) to the
	// server[1] and server[2].
	t.Run("sender", func(t *testing.T) {
		// Compare the snapshot metrics for the sender after sending two snapshots to
		// server[1] and server[2].
		senderAfter := getSnapshotBytesMetrics(t, tc, 0 /* serverIdx */, metrics)
		senderDelta := getSnapshotMetricsDiff(senderBefore, senderAfter)
		senderExpected := map[string]snapshotBytesMetrics{
			".rebalancing": {sentBytes: totalSnapshotLength, rcvdBytes: 0},
			".recovery":    {sentBytes: 0, rcvdBytes: 0},
			// The first snapshot was sent from server0 to server1, so it is
			// cross-region.
			".cross-region": {sentBytes: firstSnapshotLength, rcvdBytes: 0},
			// The second snapshot was sent from server0 to server2, so it is
			// cross-zone within same region.
			".cross-zone": {sentBytes: secSnapshotLength, rcvdBytes: 0},
			"":            {sentBytes: totalSnapshotLength, rcvdBytes: 0},
		}
		require.Equal(t, senderExpected, senderDelta)
	})

	t.Run("first receiver", func(t *testing.T) {
		// Compare the snapshot metrics for server[1] after receiving the first
		// snapshot.
		firstReceiverMetricsAfter := getSnapshotBytesMetrics(t, tc, 1 /* serverIdx */, metrics)
		firstReceiverDelta := getSnapshotMetricsDiff(firstReceiverBefore, firstReceiverMetricsAfter)
		firstReceiverExpected := map[string]snapshotBytesMetrics{
			".rebalancing": {sentBytes: 0, rcvdBytes: firstSnapshotLength},
			".recovery":    {sentBytes: 0, rcvdBytes: 0},
			// The first snapshot was sent from server0 to server1, so it is
			// cross-region.
			".cross-region": {sentBytes: 0, rcvdBytes: firstSnapshotLength},
			".cross-zone":   {sentBytes: 0, rcvdBytes: 0},
			"":              {sentBytes: 0, rcvdBytes: firstSnapshotLength},
		}
		require.Equal(t, firstReceiverExpected, firstReceiverDelta)
	})

	t.Run("second receiver", func(t *testing.T) {
		// Compare the snapshot metrics for server[2] after receiving the second
		// snapshot.
		secReceiverAfter := getSnapshotBytesMetrics(t, tc, 2 /* serverIdx */, metrics)
		secReceiverDelta := getSnapshotMetricsDiff(secReceiverBefore, secReceiverAfter)
		secReceiverExpected := map[string]snapshotBytesMetrics{
			".rebalancing": {sentBytes: 0, rcvdBytes: secSnapshotLength},
			".recovery":    {sentBytes: 0, rcvdBytes: 0},
			// The second snapshot was sent from server0 to server2, so it is
			// cross-zone within same region.
			".cross-region": {sentBytes: 0, rcvdBytes: 0},
			".cross-zone":   {sentBytes: 0, rcvdBytes: secSnapshotLength},
			"":              {sentBytes: 0, rcvdBytes: secSnapshotLength},
		}
		require.Equal(t, secReceiverExpected, secReceiverDelta)
	})

}

// TestAddVotersWithoutRaftQueue verifies that in normal operations Raft
// snapshots are not required. This test creates a range with a single voter,
// then adds two additional voters. Most of the time this succeeds, however it
// fails (today) occasionally due to the addition of the first voter being
// "incomplete" and therefore the second voter is not able to be added because
// there is no quorum.
//
// Specifically the following sequence of events happens when the leader adds
// the first voter:
//  1. AdminChangeReplicasRequest is processed on n1.
//     a) Adds a n2 as a LEARNER to raft.
//     b) Sends an initial snapshot to n2.
//     c) n2 receives and applies the snapshot.
//     d) n2 responds that it successfully applied the snapshot.
//     e) n1 receives the response and updates state to Follower.
//  2. Before step c above, n1 sends a MsgApp to n2
//     a) MsgApp - entries up-to and including the conf change.
//     b) The MsgApp is received and REJECTED because the term is wrong.
//     c) After 1e above, n1 receives the rejection.
//     d) n1 updates n2 from StateReplicate to StateProbe and then StateSnapshot.
//
// From n2's perspective, it receives the MsgApp prior to the initial snapshot.
// This results in it responding with a rejected MsgApp. Later it receives the
// snapshot and correctly applies it. However, when n1 sees the rejected MsgApp,
// it moves n2 status to StateProbe and stops sending Raft updates to it as it
// plans to fix it with a Raft Snapshot. As the raft snapshot queue is disabled
// this never happens and the state is stuck as a non-Learner in StateProbe. At
// this point, the Raft group is wedged since it only has 1/2 nodes available
// for Raft consensus.
func TestAddVotersWithoutRaftQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	// Disable the raft snapshot queue to make sure we don't require a raft snapshot.
	tc := testcluster.StartTestCluster(
		t, 3, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{DisableRaftSnapshotQueue: true}},
			},
			ReplicationMode: base.ReplicationManual,
		},
	)
	defer tc.Stopper().Stop(ctx)

	key := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, key, tc.Target(1))
	tc.AddVotersOrFatal(t, key, tc.Target(2))
}
