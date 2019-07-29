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
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/tracker"
)

// TODO(dan): Test learners and quota pool.
// TODO(dan): Grep the codebase for "preemptive" and audit.
// TODO(dan): Write a test like TestLearnerAdminChangeReplicasRace for the
//   replicate queue leadership transfer race.

type learnerTestKnobs struct {
	storeKnobs                       storage.StoreTestingKnobs
	replicaAddStopAfterLearnerAtomic int64
}

func makeLearnerTestKnobs() (base.TestingKnobs, *learnerTestKnobs) {
	var k learnerTestKnobs
	k.storeKnobs.ReplicaAddStopAfterLearnerSnapshot = func() bool {
		return atomic.LoadInt64(&k.replicaAddStopAfterLearnerAtomic) > 0
	}
	return base.TestingKnobs{Store: &k.storeKnobs}, &k
}

func getFirstStoreReplica(
	t *testing.T, s serverutils.TestServerInterface, key roachpb.Key,
) (*storage.Store, *storage.Replica) {
	t.Helper()
	store, err := s.GetStores().(*storage.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	var repl *storage.Replica
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
	store, err := s.GetStores().(*storage.Stores).GetStore(s.GetFirstStoreID())
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
	// The happy case! \o/

	blockUntilSnapshotCh := make(chan struct{})
	blockSnapshotsCh := make(chan struct{})
	knobs, ltk := makeLearnerTestKnobs()
	ltk.storeKnobs.ReceiveSnapshot = func(h *storage.SnapshotRequest_Header) error {
		close(blockUntilSnapshotCh)
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
	db.Exec(t, `SET CLUSTER SETTING kv.learner_replicas.enabled = true`)

	scratchStartKey := tc.ScratchRange(t)

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		_, err := tc.AddReplicas(scratchStartKey, tc.Target(1))
		return err
	})

	// Wait until the snapshot starts, which happens after the learner has been
	// added.
	<-blockUntilSnapshotCh
	desc := tc.LookupRangeOrFatal(t, scratchStartKey)
	require.Len(t, desc.Replicas().Voters(), 1)
	require.Len(t, desc.Replicas().Learners(), 1)

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
	require.Len(t, desc.Replicas().Voters(), 2)
	require.Len(t, desc.Replicas().Learners(), 0)
	require.Equal(t, int64(1), getFirstStoreMetric(t, tc.Server(1), `range.snapshots.learner-applied`))
}

func TestLearnerRaftConfState(t *testing.T) {
	defer leaktest.AfterTest(t)()

	verifyLearnerInRaftOnNodes := func(
		key roachpb.Key, id roachpb.ReplicaID, servers []*server.TestServer,
	) {
		t.Helper()
		var repls []*storage.Replica
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

	knobs, ltk := makeLearnerTestKnobs()
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
	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	db.Exec(t, `SET CLUSTER SETTING kv.learner_replicas.enabled = true`)

	// Add a learner replica, send a snapshot so that it's materialized as a
	// Replica on the Store, but don't promote it to a voter.
	scratchStartKey := tc.ScratchRange(t)
	atomic.StoreInt64(&ltk.replicaAddStopAfterLearnerAtomic, 1)
	desc := tc.AddReplicasOrFatal(t, scratchStartKey, tc.Target(1))
	atomic.StoreInt64(&ltk.replicaAddStopAfterLearnerAtomic, 0)
	require.Len(t, desc.Replicas().Learners(), 1)
	learnerReplicaID := desc.Replicas().Learners()[0].ReplicaID

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

	var rejectSnapshots int64
	knobs, ltk := makeLearnerTestKnobs()
	ltk.storeKnobs.ReceiveSnapshot = func(h *storage.SnapshotRequest_Header) error {
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
	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	db.Exec(t, `SET CLUSTER SETTING kv.learner_replicas.enabled = true`)

	scratchStartKey := tc.ScratchRange(t)
	atomic.StoreInt64(&rejectSnapshots, 1)
	_, err := tc.AddReplicas(scratchStartKey, tc.Target(1))
	// TODO(dan): It'd be nice if we could cancel the `AddReplicas` context before
	// returning the error from the `ReceiveSnapshot` knob to test the codepath
	// that uses a new context for the rollback, but plumbing that context is
	// annoying.
	if !testutils.IsError(err, `remote couldn't accept LEARNER snapshot`) {
		t.Fatalf(`expected "remote couldn't accept LEARNER snapshot" error got: %+v`, err)
	}

	// Make sure we cleaned up after ourselves (by removing the learner).
	desc := tc.LookupRangeOrFatal(t, scratchStartKey)
	require.Empty(t, desc.Replicas().Learners())
}

func TestSplitWithLearner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	knobs, ltk := makeLearnerTestKnobs()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)
	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	db.Exec(t, `SET CLUSTER SETTING kv.learner_replicas.enabled = true`)

	// Add a learner replica, send a snapshot so that it's materialized as a
	// Replica on the Store, but don't promote it to a voter.
	scratchStartKey := tc.ScratchRange(t)
	atomic.StoreInt64(&ltk.replicaAddStopAfterLearnerAtomic, 1)
	_ = tc.AddReplicasOrFatal(t, scratchStartKey, tc.Target(1))
	atomic.StoreInt64(&ltk.replicaAddStopAfterLearnerAtomic, 0)

	// Splitting a learner is allowed. This orphans the two learners, but the
	// replication queue will eventually clean this up.
	left, right, err := tc.SplitRange(scratchStartKey.Next())
	require.NoError(t, err)
	require.Len(t, left.Replicas().Learners(), 1)
	require.Len(t, right.Replicas().Learners(), 1)
}

func TestReplicateQueueSeesLearner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// NB also see TestAllocatorRemoveLearner for a lower-level test.

	ctx := context.Background()
	knobs, ltk := makeLearnerTestKnobs()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)
	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	db.Exec(t, `SET CLUSTER SETTING kv.learner_replicas.enabled = true`)

	// Add a learner replica, send a snapshot so that it's materialized as a
	// Replica on the Store, but don't promote it to a voter.
	scratchStartKey := tc.ScratchRange(t)
	atomic.StoreInt64(&ltk.replicaAddStopAfterLearnerAtomic, 1)
	_ = tc.AddReplicasOrFatal(t, scratchStartKey, tc.Target(1))
	atomic.StoreInt64(&ltk.replicaAddStopAfterLearnerAtomic, 0)

	// Run the replicate queue.
	store, repl := getFirstStoreReplica(t, tc.Server(0), scratchStartKey)
	require.Equal(t, int64(0), getFirstStoreMetric(t, tc.Server(0), `queue.replicate.removelearnerreplica`))
	_, errMsg, err := store.ManuallyEnqueue(ctx, "replicate", repl, true /* skipShouldQueue */)
	require.NoError(t, err)
	require.Equal(t, ``, errMsg)
	require.Equal(t, int64(1), getFirstStoreMetric(t, tc.Server(0), `queue.replicate.removelearnerreplica`))

	// Make sure it deleted the learner.
	desc := tc.LookupRangeOrFatal(t, scratchStartKey)
	require.Empty(t, desc.Replicas().Learners())

	// Bonus points: the replicate queue keeps processing until there is nothing
	// to do, so it should have upreplicated the range to 3.
	require.Len(t, desc.Replicas().Voters(), 3)
}

func TestGCQueueSeesLearner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	knobs, ltk := makeLearnerTestKnobs()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)
	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	db.Exec(t, `SET CLUSTER SETTING kv.learner_replicas.enabled = true`)

	// Add a learner replica, send a snapshot so that it's materialized as a
	// Replica on the Store, but don't promote it to a voter.
	scratchStartKey := tc.ScratchRange(t)
	atomic.StoreInt64(&ltk.replicaAddStopAfterLearnerAtomic, 1)
	_ = tc.AddReplicasOrFatal(t, scratchStartKey, tc.Target(1))
	atomic.StoreInt64(&ltk.replicaAddStopAfterLearnerAtomic, 0)

	// Run the replicaGC queue.
	store, repl := getFirstStoreReplica(t, tc.Server(1), scratchStartKey)
	trace, errMsg, err := store.ManuallyEnqueue(ctx, "replicaGC", repl, true /* skipShouldQueue */)
	require.NoError(t, err)
	require.Equal(t, ``, errMsg)
	const msg = `not gc'able, replica is still in range descriptor: (n2,s2):2LEARNER`
	require.Contains(t, tracing.FormatRecordedSpans(trace), msg)

	// Make sure it didn't collect the learner.
	desc := tc.LookupRangeOrFatal(t, scratchStartKey)
	require.NotEmpty(t, desc.Replicas().Learners())
}

func TestRaftSnapshotQueueSeesLearner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	blockSnapshotsCh := make(chan struct{})
	knobs, ltk := makeLearnerTestKnobs()
	ltk.storeKnobs.ReceiveSnapshot = func(h *storage.SnapshotRequest_Header) error {
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
	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	db.Exec(t, `SET CLUSTER SETTING kv.learner_replicas.enabled = true`)

	// Create a learner replica.
	scratchStartKey := tc.ScratchRange(t)
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		_, err := tc.AddReplicas(scratchStartKey, tc.Target(1))
		return err
	})

	// Wait until raft knows that the learner needs a snapshot.
	store, repl := getFirstStoreReplica(t, tc.Server(0), scratchStartKey)
	testutils.SucceedsSoon(t, func() error {
		for _, p := range repl.RaftStatus().Progress {
			if p.State == tracker.StateSnapshot {
				return nil
			}
		}
		return errors.New(`expected some replica to need a snapshot`)
	})

	// Note the value of the metrics before.
	generatedBefore := getFirstStoreMetric(t, tc.Server(0), `range.snapshots.generated`)
	raftAppliedBefore := getFirstStoreMetric(t, tc.Server(0), `range.snapshots.normal-applied`)

	// Run the raftsnapshot queue.
	trace, errMsg, err := store.ManuallyEnqueue(ctx, "raftsnapshot", repl, true /* skipShouldQueue */)
	require.NoError(t, err)
	require.Equal(t, ``, errMsg)
	const msg = `not sending snapshot type RAFT to learner: (n2,s2):2LEARNER`
	require.Contains(t, tracing.FormatRecordedSpans(trace), msg)

	// Make sure it didn't send any RAFT snapshots.
	require.Equal(t, generatedBefore, getFirstStoreMetric(t, tc.Server(0), `range.snapshots.generated`))
	require.Equal(t, raftAppliedBefore, getFirstStoreMetric(t, tc.Server(0), `range.snapshots.normal-applied`))

	close(blockSnapshotsCh)
	require.NoError(t, g.Wait())
}

// This test verifies the result of a race between the replicate queue running
// while an AdminChangeReplicas is adding a replica.
func TestLearnerAdminChangeReplicasRace(t *testing.T) {
	defer leaktest.AfterTest(t)()

	blockUntilSnapshotCh := make(chan struct{}, 2)
	blockSnapshotsCh := make(chan struct{})
	knobs, ltk := makeLearnerTestKnobs()
	ltk.storeKnobs.ReceiveSnapshot = func(h *storage.SnapshotRequest_Header) error {
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
	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	db.Exec(t, `SET CLUSTER SETTING kv.learner_replicas.enabled = true`)

	// Add the learner.
	scratchStartKey := tc.ScratchRange(t)
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		_, err := tc.AddReplicas(scratchStartKey, tc.Target(1))
		return err
	})

	// Wait until the snapshot starts, which happens after the learner has been
	// added.
	<-blockUntilSnapshotCh

	// Removes the learner out from under the coordinator running on behalf of
	// AddReplicas.
	_, err := tc.RemoveReplicas(scratchStartKey, tc.Target(1))
	require.NoError(t, err)
	desc := tc.LookupRangeOrFatal(t, scratchStartKey)
	require.Len(t, desc.Replicas().Voters(), 1)
	require.Len(t, desc.Replicas().Learners(), 0)

	// Unblock the snapshot, and surprise AddReplicas. It should retry and error
	// that the descriptor has changed since the AdminChangeReplicas command
	// started.
	close(blockSnapshotsCh)
	if err := g.Wait(); !testutils.IsError(err, `descriptor changed`) {
		t.Fatalf(`expected "descriptor changed" error got: %+v`, err)
	}
	desc = tc.LookupRangeOrFatal(t, scratchStartKey)
	require.Len(t, desc.Replicas().Voters(), 1)
	require.Len(t, desc.Replicas().Learners(), 0)
}

func TestLearnerNoAcceptLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	knobs, ltk := makeLearnerTestKnobs()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)
	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	db.Exec(t, `SET CLUSTER SETTING kv.learner_replicas.enabled = true`)

	// Add a learner replica, send a snapshot so that it's materialized as a
	// Replica on the Store, but don't promote it to a voter.
	scratchStartKey := tc.ScratchRange(t)
	atomic.StoreInt64(&ltk.replicaAddStopAfterLearnerAtomic, 1)
	_ = tc.AddReplicasOrFatal(t, scratchStartKey, tc.Target(1))
	atomic.StoreInt64(&ltk.replicaAddStopAfterLearnerAtomic, 0)

	desc := tc.LookupRangeOrFatal(t, scratchStartKey)
	err := tc.TransferRangeLease(desc, tc.Target(1))
	if !testutils.IsError(err, `cannot transfer lease to replica of type LEARNER`) {
		t.Fatalf(`expected "cannot transfer lease to replica of type LEARNER" error got: %+v`, err)
	}
}

func TestLearnerFollowerRead(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if util.RaceEnabled {
		// Limiting how long transactions can run does not work well with race
		// unless we're extremely lenient, which drives up the test duration.
		t.Skip("skipping under race")
	}

	ctx := context.Background()
	knobs, ltk := makeLearnerTestKnobs()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)
	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	db.Exec(t, `SET CLUSTER SETTING kv.learner_replicas.enabled = true`)
	db.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = $1`, testingTargetDuration)
	db.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.close_fraction = $1`, closeFraction)
	db.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.follower_reads_enabled = true`)

	scratchStartKey := tc.ScratchRange(t)
	atomic.StoreInt64(&ltk.replicaAddStopAfterLearnerAtomic, 1)
	scratchDesc := tc.AddReplicasOrFatal(t, scratchStartKey, tc.Target(1))
	atomic.StoreInt64(&ltk.replicaAddStopAfterLearnerAtomic, 0)

	req := roachpb.BatchRequest{Header: roachpb.Header{
		RangeID:   scratchDesc.RangeID,
		Timestamp: tc.Server(0).Clock().Now(),
	}}
	req.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{
		Key: scratchDesc.StartKey.AsRawKey(), EndKey: scratchDesc.EndKey.AsRawKey(),
	}})

	_, repl := getFirstStoreReplica(t, tc.Server(1), scratchStartKey)
	testutils.SucceedsSoon(t, func() error {
		// Trace the Send call so we can verify that it hit the exact `learner
		// replicas cannot serve follower reads` branch that we're trying to test.
		sendCtx, collect, cancel := tracing.ContextWithRecordingSpan(ctx, "manual read request")
		defer cancel()
		_, pErr := repl.Send(sendCtx, req)
		err := pErr.GoError()
		if !testutils.IsError(err, `not lease holder`) {
			return errors.Errorf(`expected "not lease holder" error got: %+v`, err)
		}
		const msg = `learner replicas cannot serve follower reads`
		formattedTrace := tracing.FormatRecordedSpans(collect())
		if !strings.Contains(formattedTrace, msg) {
			return errors.Errorf("expected a trace with `%s` got:\n%s", msg, formattedTrace)
		}
		return nil
	})
}

func TestLearnerAdminRelocateRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	knobs, ltk := makeLearnerTestKnobs()
	tc := testcluster.StartTestCluster(t, 4, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)
	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	db.Exec(t, `SET CLUSTER SETTING kv.learner_replicas.enabled = true`)

	scratchStartKey := tc.ScratchRange(t)
	atomic.StoreInt64(&ltk.replicaAddStopAfterLearnerAtomic, 1)
	_ = tc.AddReplicasOrFatal(t, scratchStartKey, tc.Target(1))
	_ = tc.AddReplicasOrFatal(t, scratchStartKey, tc.Target(2))
	atomic.StoreInt64(&ltk.replicaAddStopAfterLearnerAtomic, 0)

	// Test AdminRelocateRange's treatment of learners by having one that it has
	// to remove and one that should stay and become a voter.
	//
	// Before: 1 (voter), 2 (learner), 3 (learner)
	// After: 1 (voter), 2 (voter), 4 (voter)
	targets := []roachpb.ReplicationTarget{tc.Target(0), tc.Target(1), tc.Target(3)}
	require.NoError(t, tc.Server(0).DB().AdminRelocateRange(ctx, scratchStartKey, targets))
	desc := tc.LookupRangeOrFatal(t, scratchStartKey)
	voters := desc.Replicas().Voters()
	require.Len(t, voters, len(targets))
	sort.Slice(voters, func(i, j int) bool { return voters[i].NodeID < voters[j].NodeID })
	for i := range voters {
		require.Equal(t, targets[i].NodeID, voters[i].NodeID, `%v`, voters)
		require.Equal(t, targets[i].StoreID, voters[i].StoreID, `%v`, voters)
	}
	require.Empty(t, desc.Replicas().Learners())
}

func TestLearnerAdminMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	knobs, ltk := makeLearnerTestKnobs()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)
	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	db.Exec(t, `SET CLUSTER SETTING kv.learner_replicas.enabled = true`)

	scratchStartKey := tc.ScratchRange(t)
	splitKey1 := scratchStartKey.Next()
	splitKey2 := splitKey1.Next()
	_, _ = tc.SplitRangeOrFatal(t, splitKey1)
	_, _ = tc.SplitRangeOrFatal(t, splitKey2)

	atomic.StoreInt64(&ltk.replicaAddStopAfterLearnerAtomic, 1)
	_ = tc.AddReplicasOrFatal(t, scratchStartKey, tc.Target(1))
	_ = tc.AddReplicasOrFatal(t, splitKey2, tc.Target(1))
	atomic.StoreInt64(&ltk.replicaAddStopAfterLearnerAtomic, 0)

	// Learner on the lhs should fail.
	err := tc.Server(0).DB().AdminMerge(ctx, scratchStartKey)
	if !testutils.IsError(err, `cannot merge range with non-voter replicas on lhs`) {
		t.Fatalf(`expected "cannot merge range with non-voter replicas on lhs" error got: %+v`, err)
	}
	// Learner on the rhs should fail.
	err = tc.Server(0).DB().AdminMerge(ctx, splitKey1)
	if !testutils.IsError(err, `cannot merge range with non-voter replicas on rhs`) {
		t.Fatalf(`expected "cannot merge range with non-voter replicas on rhs" error got: %+v`, err)
	}
}

func TestMergeQueueSeesLearner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	knobs, ltk := makeLearnerTestKnobs()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)
	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	db.Exec(t, `SET CLUSTER SETTING kv.learner_replicas.enabled = true`)
	// TestCluster currently overrides this when used with ReplicationManual.
	db.Exec(t, `SET CLUSTER SETTING kv.range_merge.queue_enabled = true`)

	scratchStartKey := tc.ScratchRange(t)
	origDesc := tc.LookupRangeOrFatal(t, scratchStartKey)

	splitKey := scratchStartKey.Next()
	_, _ = tc.SplitRangeOrFatal(t, splitKey)

	atomic.StoreInt64(&ltk.replicaAddStopAfterLearnerAtomic, 1)
	_ = tc.AddReplicasOrFatal(t, scratchStartKey, tc.Target(1))
	atomic.StoreInt64(&ltk.replicaAddStopAfterLearnerAtomic, 0)

	// Unsplit the range to clear the sticky bit.
	require.NoError(t, tc.Server(0).DB().AdminUnsplit(ctx, splitKey))

	// Run the merge queue.
	store, repl := getFirstStoreReplica(t, tc.Server(0), scratchStartKey)
	trace, errMsg, err := store.ManuallyEnqueue(ctx, "merge", repl, true /* skipShouldQueue */)
	require.NoError(t, err)
	require.Equal(t, ``, errMsg)
	formattedTrace := tracing.FormatRecordedSpans(trace)
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
	require.Len(t, desc.Replicas().Voters(), 1)
	require.Empty(t, desc.Replicas().Learners())
}
