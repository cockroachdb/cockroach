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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	"go.etcd.io/etcd/raft"
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

func getFirstStoreMetric(t *testing.T, s serverutils.TestServerInterface, name string) int64 {
	t.Helper()
	store, err := s.GetStores().(*storage.Stores).GetStore(s.GetFirstStoreID())
	if err != nil {
		t.Fatal(err)
	}

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

	// Verify that raft thinks it's a learner, too.
	_, repl := getFirstStoreReplica(t, tc.Server(0), scratchStartKey)
	testutils.SucceedsSoon(t, func() error {
		for _, p := range repl.RaftStatus().Progress {
			if p.IsLearner {
				return nil
			}
		}
		return errors.New(`expected some replica to be a learner`)
	})

	var voters, nonVoters string
	db.QueryRow(t,
		`SELECT array_to_string(replicas, ','), array_to_string(non_quorum_replicas, ',') FROM crdb_internal.ranges_no_leases WHERE range_id = $1`,
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

func TestMergeWithLearner(t *testing.T) {
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
	_, _, err := tc.SplitRange(scratchStartKey.Next())
	require.NoError(t, err)

	atomic.StoreInt64(&ltk.replicaAddStopAfterLearnerAtomic, 1)
	_ = tc.AddReplicasOrFatal(t, scratchStartKey, tc.Target(1))
	atomic.StoreInt64(&ltk.replicaAddStopAfterLearnerAtomic, 0)

	_, err = tc.Server(0).MergeRanges(scratchStartKey)
	if !testutils.IsError(err, `ranges not collocated`) {
		t.Fatalf(`expected "ranges not collocated" error got: %+v`, err)
	}
	// WIP what happens now though?
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
			if p.State == raft.ProgressStateSnapshot {
				return nil
			}
		}
		return errors.New(`expected some replica to need a snapshot`)
	})

	// Check the metrics are what we expect before.
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
	// that the descriptor has changed since the AddReplicas command started.
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
	t.Skip(`WIP`)

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

	val := roachpb.MakeValueFromString("foo")
	require.NoError(t, tc.Server(0).DB().Put(ctx, scratchStartKey, &val))

	req := roachpb.BatchRequest{Header: roachpb.Header{
		RangeID:   scratchDesc.RangeID,
		Timestamp: tc.Server(0).Clock().Now(),
	}}
	req.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{
		Key: scratchDesc.StartKey.AsRawKey(), EndKey: scratchDesc.EndKey.AsRawKey(),
	}})

	_, repl := getFirstStoreReplica(t, tc.Server(1), scratchStartKey)
	testutils.SucceedsSoon(t, func() error {
		ctx, collect, cancel := tracing.ContextWithRecordingSpan(ctx, "manual read request")
		defer cancel()
		_, pErr := repl.Send(ctx, req)
		err := pErr.GoError()
		if !testutils.IsError(err, `not lease holder`) {
			return errors.Errorf(`expected "not lease holder" error got: %+v`, err)
		}
		const msg = `WIP not getting closed timestamps for some reason`
		formattedTrace := tracing.FormatRecordedSpans(collect())
		if !strings.Contains(formattedTrace, msg) {
			return errors.Errorf("expected a trace with `%s` got:\n%s", msg, formattedTrace)
		}
		return nil
	})
}
