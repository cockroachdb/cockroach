// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestReplicaProbeRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	}
	var seen struct {
		syncutil.Mutex
		m           map[roachpb.StoreID]int
		injectedErr *kvpb.Error
	}
	seen.m = map[roachpb.StoreID]int{}
	filter := func(args kvserverbase.ApplyFilterArgs) (int, *kvpb.Error) {
		if !args.IsProbe {
			return 0, args.ForcedError
		}
		if args.ForcedError == nil {
			t.Error("probe has no forcedError") // avoid Fatal on goroutine
			return 0, args.ForcedError
		}
		seen.Lock()
		defer seen.Unlock()
		seen.m[args.StoreID]++
		if seen.injectedErr != nil {
			return 0, seen.injectedErr
		}
		return 0, args.ForcedError
	}
	// NB: At one point we tried to verify that the probe the tests sends below
	// is seen in command application on *all* replicas.
	//
	// This was flaky[^1] due to snapshot application (g1 and g2 are the
	// intertwined operations):
	//
	// - g1    | `tc.AddXOrFatal` begins
	// - g1    | it calls into ChangeReplicas
	// - g1    | Replica added to descriptor
	//      g2 | leader sends first MsgApp
	// -    g2 | follower gets MsgApp, rejects it, leader enqueues in Raft snapshot queue
	// - g1    | block raft snapshots to follower
	// - g1    | send INITIAL snapshot
	// - g1    | unblock raft snapshots to follower
	// - g1    | AddVoterOrWhatever returns to the test
	// - g1    | test sends probe
	// -    g2 | raft log queue processes the queued Replica (followers's
	//           MsgAppResp following INITIAL snapshot not having reached leader yet)[^1]
	// -    g2 | it sends another snap which includes the probe
	// -    g2 | follower applies the probe via the snap.
	// - g1    | times out waiting for the interceptor to see the probe on follower.
	//
	// We attempted to disable the raft snapshot queue. This caused deadlocks: if raft
	// (erroneously or not) requests a snapshot, it will not replicate to the follower
	// until it has been caught up. But the quorum size here is two, so this would cause
	// a hanging test. We tried to work around this by marking snapshots as failed when
	// the raft snapshot queue is disabled, but this then threw off other tests[^2] that
	// wanted to observe this state.
	//
	// So we just check that the probe applies on at least one Replica, which
	// always has to be true.
	//
	// [^1]: https://github.com/cockroachdb/cockroach/pull/92380
	// [^2]: TestMigrateWithInflightSnapshot, TestSnapshotsToDrainingNodes, TestRaftSnapshotQueueSeesLearner

	args.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		// We set an ApplyFilter even though the probe should never
		// show up there (since it always catches a forced error),
		// precisely to ensure that it doesn't.
		TestingApplyCalledTwiceFilter: filter,
		// This is the main workhorse that counts probes and injects
		// errors.
		TestingApplyForcedErrFilter: filter,
	}
	tc := testcluster.StartTestCluster(t, 3 /* nodes */, args)
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	desc := tc.LookupRangeOrFatal(t, k)
	// Establish configuration [LEASEHOLDER FOLLOWER NONVOTER].
	tc.AddVotersOrFatal(t, k, tc.Target(1))
	tc.AddNonVotersOrFatal(t, k, tc.Target(2))

	probeReq := &kvpb.ProbeRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: k,
		},
	}
	// Sanity check that ProbeRequest is fit for sending through the entire KV
	// stack, with both routing policies.
	for _, srv := range tc.Servers {
		db := srv.DB()
		for _, policy := range []kvpb.RoutingPolicy{
			kvpb.RoutingPolicy_LEASEHOLDER,
			kvpb.RoutingPolicy_NEAREST,
		} {
			testutils.SucceedsSoon(t, func() error {
				var b kv.Batch
				b.AddRawRequest(probeReq)
				b.Header.RoutingPolicy = policy
				err := db.Run(ctx, &b)
				if errors.HasType(err, (*kvpb.AmbiguousResultError)(nil)) {
					// Rare but it can happen that we're proposing on a replica
					// that is just about to get a snapshot. In that case we'll
					// get:
					//
					// result is ambiguous: unable to determine whether command was applied via snapshot
					return errors.Wrapf(err, "retrying")
				}
				require.NoError(t, err)
				return nil
			})
		}
	}
	// Check expected number of probes seen on each Replica in the apply loop.
	// Each Replica must see every probe, since the probe is replicated and we
	// don't expect truncations here.
	// There could be more due to reproposals. If there are fewer, some probes
	// returned success but didn't actually go through raft (for example because
	// they short-circuited due to being no-op, which of course we're careful
	// to avoid).
	testutils.SucceedsSoon(t, func() error {
		seen.Lock()
		defer seen.Unlock()
		if exp, act := len(seen.m), len(tc.Servers); exp != act {
			return errors.Errorf("waiting for stores to apply command: %d/%d", act, exp)
		}
		// We'll usually see 2 * len(tc.Servers) probes since we sent two probes, but see
		// the comment about errant snapshots above. We just want this test to be reliable
		// so expect at least one probe in command application.
		n := 1
		for storeID, count := range seen.m {
			if count < n {
				return errors.Errorf("saw only %d probes on s%d", count, storeID)
			}
		}
		return nil
	})

	// We can also probe directly at each Replica. This is the intended use case
	// for Replica-level circuit breakers (#33007).
	for _, srv := range tc.Servers {
		repl, _, err := srv.GetStores().(*kvserver.Stores).GetReplicaForRangeID(ctx, desc.RangeID)
		require.NoError(t, err)
		ba := &kvpb.BatchRequest{}
		ba.Add(probeReq)
		ba.Timestamp = srv.Clock().Now()
		_, pErr := repl.Send(ctx, ba)
		require.NoError(t, pErr.GoError())
	}

	// If the probe applies with any nonstandard forced error, we get the error
	// back. Not sure what other error might occur in practice, but checking this
	// anyway gives us extra confidence in the implementation mechanics of this
	// request.
	injErr := kvpb.NewErrorf("bang")
	seen.Lock()
	seen.injectedErr = injErr
	seen.Unlock()
	for _, srv := range tc.Servers {
		repl, _, err := srv.GetStores().(*kvserver.Stores).GetReplicaForRangeID(ctx, desc.RangeID)
		require.NoError(t, err)
		ba := &kvpb.BatchRequest{}
		ba.Timestamp = srv.Clock().Now()
		ba.Add(probeReq)
		_, pErr := repl.Send(ctx, ba)
		require.True(t, errors.Is(pErr.GoError(), injErr.GoError()), "%+v", pErr.GoError())
	}
}
