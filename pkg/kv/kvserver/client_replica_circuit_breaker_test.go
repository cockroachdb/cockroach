// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// In all scenarios below, we are starting out with our range on n1 and n2,
// and all other ranges (in particular the liveness range) on n1.
//
// TODO(tbg): via tracing, test that when the breaker is tripped, requests fail
// fast right upon entering the replica.

const (
	n1 = 0
	n2 = 1

	pauseHeartbeats = true
	keepHeartbeats  = false
)

// This is a sanity check in which the breaker plays no role.
func TestReplicaCircuitBreaker_NotTripped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := setupCircuitBreakerTest(t, roachpb.LeaseNone)
	defer tc.Stopper().Stop(context.Background())

	// Circuit breaker doesn't get in the way of anything unless
	// something trips it.
	require.NoError(t, tc.Write(n1))
	tc.RequireIsNotLeaseholderError(t, tc.Write(n2))
	require.NoError(t, tc.Read(n1))
	tc.RequireIsNotLeaseholderError(t, tc.Read(n2))
}

// In this test, n1 holds the lease and we disable the probe and trip the
// breaker. While the breaker is tripped, requests fail-fast with either a
// breaker or lease error. When the probe is re-enabled, everything heals.
//
// This test also verifies the circuit breaker metrics. This is only done
// in this one test since little would be gained by adding it across the
// board.
func TestReplicaCircuitBreaker_LeaseholderTripped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := setupCircuitBreakerTest(t, roachpb.LeaseNone)
	defer tc.Stopper().Stop(context.Background())
	k := tc.ScratchRange(t)

	// Get lease on n1.
	require.NoError(t, tc.Write(n1))
	// Disable the probe so that when the breaker trips, it stays tripped.
	tc.SetProbeEnabled(n1, false)
	tc.TripBreaker(n1)

	s1 := tc.GetFirstStoreFromServer(t, n1)
	s2 := tc.GetFirstStoreFromServer(t, n2)
	// Both current and all-time trip events increment on n1, not on n2.
	require.EqualValues(t, 1, s1.Metrics().ReplicaCircuitBreakerCurTripped.Value())
	require.EqualValues(t, 1, s1.Metrics().ReplicaCircuitBreakerCumTripped.Count())
	require.Zero(t, s2.Metrics().ReplicaCircuitBreakerCurTripped.Value())
	require.Zero(t, s2.Metrics().ReplicaCircuitBreakerCumTripped.Count())

	// n1 can still serve reads despite the breaker having tripped, as there is a
	// valid lease and no poisoned latches prevent the read.
	require.NoError(t, tc.Read(n1))
	require.NoError(t, tc.FollowerRead(n1))
	tc.RequireIsBreakerOpen(t, tc.Write(n1))

	// When we go through the KV client stack, we still get the breaker error
	// back.
	tc.RequireIsBreakerOpen(t, tc.WriteDS(n1))
	tc.RequireIsBreakerOpen(t, tc.WriteDS(n2))

	// Can't transfer the lease away while breaker is tripped. (This would be
	// a bad idea, since n1 would stop serving strong reads, thus making the
	// outage worse).
	tc.RequireIsBreakerOpen(t,
		tc.TransferRangeLease(tc.LookupRangeOrFatal(t, k), tc.Target(n2)),
	)

	// n2 does not have the lease so all it does is redirect to the leaseholder
	// n1, but it can serve follower reads.
	tc.RequireIsNotLeaseholderError(t, tc.Read(n2))
	require.NoError(t, tc.FollowerRead(n2))
	tc.RequireIsNotLeaseholderError(t, tc.Write(n2))

	// Enable the probe. Even a read should trigger the probe
	// and within due time the breaker should heal.
	tc.SetProbeEnabled(n1, true)
	require.NoError(t, tc.Read(n1)) // this always worked
	// Writes heal soon.
	tc.UntripsSoon(t, tc.Write, n1)

	// Currently tripped drops back to zero, all-time remains at one.
	require.EqualValues(t, 0, s1.Metrics().ReplicaCircuitBreakerCurTripped.Value())
	require.EqualValues(t, 1, s1.Metrics().ReplicaCircuitBreakerCumTripped.Count())
	// s2 wasn't affected by any breaker events.
	require.Zero(t, s2.Metrics().ReplicaCircuitBreakerCurTripped.Value())
	require.Zero(t, s2.Metrics().ReplicaCircuitBreakerCumTripped.Count())
}

// In this scenario we have n1 holding the lease and we permanently trip the
// breaker on follower n2. Before the breaker is tripped, we see
// NotLeaseholderError. When it's tripped, those are supplanted by the breaker
// errors. Once we allow the breaker to probe, the breaker untrips. In
// particular, this tests that the probe can succeed even when run on a follower
// (which would not be true if it required the local Replica to execute an
// operation that requires the lease).
func TestReplicaCircuitBreaker_FollowerTripped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := setupCircuitBreakerTest(t, roachpb.LeaseNone)
	defer tc.Stopper().Stop(context.Background())

	// Get lease on n1.
	require.NoError(t, tc.Write(n1))
	// Disable the probe on n2 so that when the breaker trips, it stays tripped.
	tc.SetProbeEnabled(n2, false)
	tc.TripBreaker(n2)

	// We didn't trip the leaseholder n1, so it is unaffected.
	require.NoError(t, tc.Read(n1))
	require.NoError(t, tc.Write(n1))
	// Even if we go through DistSender, we reliably reach the leaseholder.
	// TODO(tbg): I think this relies on the leaseholder being cached. If
	// DistSender tried to contact the follower and got the breaker error, at
	// time of writing it would propagate it.
	require.NoError(t, tc.WriteDS(n1))

	tc.RequireIsNotLeaseholderError(t, tc.Read(n2))
	tc.RequireIsNotLeaseholderError(t, tc.Write(n2))
	require.NoError(t, tc.FollowerRead(n2))

	// Enable the probe again. n2 should untrip soon.
	tc.SetProbeEnabled(n2, true)
	tc.RequireIsNotLeaseholderError(t, tc.Read(n2))
	tc.RequireIsNotLeaseholderError(t, tc.Write(n2))
	testutils.SucceedsSoon(t, func() error {
		// NB: this is slightly contrived - the mere act of accessing Err() is what
		// triggers the probe! Regular requests on the replica wouldn'd do that,
		// since we're intentionally preferring a NotLeaseholderError over a breaker
		// error (and thus aren't ever accessing the breaker when we can't serve the
		// request).
		return tc.repls[n2].Breaker().Signal().Err()
	})
}

// In this scenario, the breaker is tripped and the probe is disabled and
// additionally, the liveness records for both nodes have expired. Soon after
// the probe is re-enabled, the breaker heals. In particular, the probe isn't
// doing anything that requires the lease (or whatever it does that requires
// the lease is sufficiently special cased; at time of writing it's the former
// but as the probe learns deeper checks, the plan is ultimately the latter).
func TestReplicaCircuitBreaker_LeaselessTripped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := setupCircuitBreakerTest(t, roachpb.LeaseNone)
	defer tc.Stopper().Stop(context.Background())

	// Put the lease on n1 but then trip the breaker with the probe
	// disabled.
	require.NoError(t, tc.Write(n1))
	tc.SetProbeEnabled(n1, false)
	tc.TripBreaker(n1)
	// We don't know the lease type here, so make sure the lease is expired for
	// both the cases of epoch or leader leases.
	resumeHeartbeats := tc.ExpireAllLeasesAndN1LivenessRecord(t, pauseHeartbeats)
	tc.DisableAllStoreLivenessHeartbeats.Store(true)
	tc.ManualClock.Increment(tc.Servers[0].RaftConfig().RangeLeaseDuration.Nanoseconds())

	// On n1, run into the circuit breaker when requesting lease. We have to
	// resume heartbeats for this to not time out, as requesting the new lease
	// entails doing liveness checks which can't succeed if nobody is
	// heartbeating, and we'd get stuck in liveness before reaching the circuit
	// breaker. (In other words, replica circuit breaking doesn't fail-fast
	// requests reliably if liveness is unavailable; this is tracked in #74616).
	// We don't attempt to acquire a lease on n2 since it would try and succeed
	// (except the test harness categorically prevents n2 from getting a lease,
	// injecting an error).
	resumeHeartbeats()
	tc.DisableAllStoreLivenessHeartbeats.Store(false)
	testutils.SucceedsSoon(t, func() error {
		err := tc.Read(n1)
		if errors.HasType(err, (*kvpb.NotLeaseHolderError)(nil)) {
			// Retriable errors can occur when manipulating the liveness record in
			// preparation for requesting a lease, such as:
			//
			// [NotLeaseHolderError] failed to manipulate liveness record: heartbeat
			// failed on epoch increment; r45: replica (n1,s1):1 not lease holder;
			// current lease is repl=(n1,s1):1 seq=1 start=0,0 epo=1 pro=[...]
			return err
		}
		tc.RequireIsBreakerOpen(t, err)
		tc.RequireIsBreakerOpen(t, tc.Write(n1))
		return nil
	})

	// Can still perform follower reads on both nodes, as this does not rely on
	// the lease and does not consult the breaker.
	require.NoError(t, tc.FollowerRead(n1))
	require.NoError(t, tc.FollowerRead(n2))

	// Let the breaker heal and things should go back to normal. This is not a
	// trivial thing to hold, as the probe needs to go through for this, and if
	// we're not careful, the probe itself is held up by the breaker as well, or
	// the probe will try to acquire a lease (which we're currently careful to
	// avoid).
	tc.SetProbeEnabled(n1, true)
	tc.UntripsSoon(t, tc.Read, n1)
	tc.UntripsSoon(t, tc.Write, n1)
	tc.RequireIsNotLeaseholderError(t, tc.Read(n2))
	tc.RequireIsNotLeaseholderError(t, tc.Write(n2))
}

// In this test, the range is on n1 and n2 and we take down the follower n2,
// thus losing quorum (but not the lease or leaseholder). After the
// SlowReplicationThreshold (which is reduced suitably to keep the test
// snappy) has passed, the breaker on n1's Replica trips. When n2 comes back,
// the probe on n1 succeeds and requests to n1 can acquire a lease and
// succeed.
func TestReplicaCircuitBreaker_Leaseholder_QuorumLoss(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := setupCircuitBreakerTest(t, roachpb.LeaseNone)
	defer tc.Stopper().Stop(context.Background())

	// Get lease on n1.
	require.NoError(t, tc.Write(n1))
	tc.StopServer(n2) // lose quorum

	// We didn't lose the liveness range (which is only on n1).
	tc.HeartbeatNodeLiveness(t, n1)

	// Read still works, as we have valid lease and no poisoned latch
	// underneath.
	require.NoError(t, tc.Read(n1))
	tc.SetSlowThreshold(10 * time.Millisecond)
	{
		err := tc.Write(n1)
		var ae *kvpb.AmbiguousResultError
		require.True(t, errors.As(err, &ae), "%+v", err)
		t.Log(err)
		tc.RequireIsBreakerOpen(t, err)
	}
	// We still have a valid lease, but now the above write is holding a poisoned
	// latch (this is true despite the write itself having returned already).
	// However, can still serve follower reads because those don't check latches
	// (nor do they need the lease, though there is a valid one in this case).
	{
		tc.RequireIsBreakerOpen(t, tc.Read(n1))
		require.NoError(t, tc.FollowerRead(n1))
	}

	// Bring n2 back and service should be restored.
	tc.SetSlowThreshold(0) // reset
	require.NoError(t, tc.RestartServer(n2))
	tc.UntripsSoon(t, tc.Write, n1) // poisoned latch goes away
	require.NoError(t, tc.Read(n1))
}

// In this test, the range is on n1 and n2 and we place the lease on n2 and
// shut down n2 and expire the lease. n1 will be a non-leaseholder without
// quorum, and requests to it should trip the circuit breaker. This is an
// interesting test case internally because here, the request that trips the
// breaker is the slow lease request, and not the test's actual write. Since
// leases have lots of special casing internally, this is easy to get wrong.
func TestReplicaCircuitBreaker_Follower_QuorumLoss(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := setupCircuitBreakerTest(t, roachpb.LeaseNone)
	defer tc.Stopper().Stop(context.Background())

	// Get lease to n2 so that we can lose it without taking down the system ranges.
	desc := tc.LookupRangeOrFatal(t, tc.ScratchRange(t))
	tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(n2))
	resumeHeartbeats := tc.ExpireAllLeasesAndN1LivenessRecord(t, keepHeartbeats)
	tc.StopServer(n2) // lose quorum and leaseholder
	resumeHeartbeats()

	// We didn't lose the liveness range (which is only on n1).
	tc.HeartbeatNodeLiveness(t, n1)

	tc.SetSlowThreshold(10 * time.Millisecond)
	tc.RequireIsBreakerOpen(t, tc.Write(n1))
	tc.RequireIsBreakerOpen(t, tc.Read(n1))

	// Bring n2 back and service should be restored.
	tc.SetSlowThreshold(0) // reset
	require.NoError(t, tc.RestartServer(n2))
	tc.UntripsSoon(t, tc.Write, n1)
	require.NoError(t, tc.Read(n1))
}

// This test is skipped but documents that the current circuit breakers cannot
// prevent hung requests when the *liveness* range is down.
//
// The liveness range is usually 5x-replicated and so is less likely to lose
// quorum, for resilience against asymmetric partitions it would be nice to
// also trip the local breaker if liveness updated cannot be performed. We
// can't rely on receiving an error from the liveness range back, as we may not
// be able to reach any of its Replicas (and in fact all of its Replicas may
// have been lost, in extreme cases), so we would need to guard all
// interactions with the liveness range in a timeout, which is unsatisfying.
//
// A somewhat related problem needs to be solved for general loss of all
// Replicas of a Range. In that case the request will never reach a
// per-Replica circuit breaker and it will thus fail slow. Instead, we would
// need DistSender to detect this scenario (for example, by cross-checking
// liveness against the available targets, but this gets complicated again
// due to our having bootstrapped liveness on top of the KV stack).
//
// Solving the general problem, however, wouldn't obviate the need for
// special-casing of lease-related liveness interactions, since we also want
// to protect against the case in which the liveness range is "there" but
// simply will not make progress for whatever reason.
//
// An argument can be made that in such a case it is likely that the cluster
// is unavailable in its entirety.
func TestReplicaCircuitBreaker_Liveness_QuorumLoss(t *testing.T) {
	defer leaktest.AfterTest(t)()

	skip.IgnoreLint(t, "See: https://github.com/cockroachdb/cockroach/issues/74616")

	defer log.Scope(t).Close(t)
	tc := setupCircuitBreakerTest(t, roachpb.LeaseNone)
	defer tc.Stopper().Stop(context.Background())

	// Up-replicate liveness range and move lease to n2.
	tc.AddVotersOrFatal(t, keys.NodeLivenessPrefix, tc.Target(n2))
	tc.TransferRangeLeaseOrFatal(t, tc.LookupRangeOrFatal(t, keys.NodeLivenessPrefix), tc.Target(n2))
	// Sanity check that things still work.
	require.NoError(t, tc.Write(n1))
	tc.RequireIsNotLeaseholderError(t, tc.Write(n2))
	// Remove the second replica for our main range.
	tc.RemoveVotersOrFatal(t, tc.ScratchRange(t), tc.Target(n2))

	// Now stop n2. This will lose the liveness range only since the other
	// ranges are on n1 only.
	tc.StopServer(n2)

	// Expire all leases. We also pause all heartbeats but that doesn't really
	// matter since the liveness range is unavailable anyway.
	resume := tc.ExpireAllLeasesAndN1LivenessRecord(t, pauseHeartbeats)
	defer resume()

	// Since there isn't a lease, and the liveness range is down, the circuit
	// breaker should kick into gear.
	tc.SetSlowThreshold(10 * time.Millisecond)

	// This is what fails, as the lease acquisition hangs on the liveness range
	// but nothing will ever report a problem to the breaker.
	tc.RequireIsBreakerOpen(t, tc.Read(n1))
	tc.RequireIsBreakerOpen(t, tc.Write(n1))

	tc.SetSlowThreshold(0) // reset
	require.NoError(t, tc.RestartServer(n2))

	tc.UntripsSoon(t, tc.Read, n1)
	require.NoError(t, tc.Write(n1))
}

// In this test, a txn anchored on the range losing quorum also has an intent on
// a healthy range. Quorum is lost before committing, poisoning latches for the
// txn info. When resolving the intent on the healthy range, it will hit a
// poisoned latch. This should result in a ReplicaUnavailableError from the
// original range that lost quorum, not from the range with the intent.
func TestReplicaCircuitBreaker_ResolveIntent_QuorumLoss(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := setupCircuitBreakerTest(t, roachpb.LeaseNone)
	defer tc.Stopper().Stop(ctx)

	// Get lease on n1.
	require.NoError(t, tc.Write(n1))

	// Split off a healthy range, which will inherit a lease on n1. Remove the
	// replica on n2, so that it remains healthy when we take down n2.
	failKey := tc.ScratchRange(t)
	okKey := failKey.Next()
	failDesc, _ := tc.SplitRangeOrFatal(t, okKey)
	okDesc := tc.RemoveVotersOrFatal(t, okKey, tc.Target(n2))
	t.Logf("failDesc=%s", failDesc)
	t.Logf("okDesc=%s", okDesc)

	// Start a transaction, anchoring it on the faulty range.
	db := tc.Server(n1).DB()
	txn := db.NewTxn(ctx, "test")
	require.NoError(t, txn.Put(ctx, failKey, "fail"))
	require.NoError(t, txn.Put(ctx, okKey, "ok"))

	// Lose quorum.
	tc.StopServer(n2)
	tc.HeartbeatNodeLiveness(t, n1)

	// Attempt to commit. It should fail, but will poison latches on
	// the faulty range.
	tc.SetSlowThreshold(time.Second)
	err := txn.Commit(ctx)
	tc.RequireIsBreakerOpen(t, err)

	// Read the key from the healthy range. It should fail due to a poisoned latch
	// on the txn's anchored range. This error should appear to come from the
	// failed range, not from the healthy range.
	_, err = db.Get(ctx, okKey)
	tc.RequireIsBreakerOpen(t, err)
	ruErr := &kvpb.ReplicaUnavailableError{}
	require.True(t, errors.As(err, &ruErr))
	require.Equal(t, failDesc.RangeID, ruErr.Desc.RangeID)
}

type dummyStream struct {
	name string
	ctx  context.Context
	recv chan *kvpb.RangeFeedEvent
	done chan *kvpb.Error
}

func newDummyStream(ctx context.Context, name string) *dummyStream {
	return &dummyStream{
		ctx:  ctx,
		name: name,
		recv: make(chan *kvpb.RangeFeedEvent),
		done: make(chan *kvpb.Error, 1),
	}
}

func (s *dummyStream) SendUnbufferedIsThreadSafe() {}

func (s *dummyStream) SendUnbuffered(ev *kvpb.RangeFeedEvent) error {
	if ev.Val == nil && ev.Error == nil {
		return nil
	}

	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.recv <- ev:
		return nil
	}
}

// SendError implements the Stream interface. It mocks the disconnect behavior
// by sending the error to the done channel.
func (s *dummyStream) SendError(err *kvpb.Error) {
	s.done <- err
}

func waitReplicaRangeFeed(
	ctx context.Context, r *kvserver.Replica, req *kvpb.RangeFeedRequest, stream *dummyStream,
) error {
	sendErrToStream := func(err *kvpb.Error) error {
		var event kvpb.RangeFeedEvent
		event.SetValue(&kvpb.RangeFeedError{
			Error: *err,
		})
		return stream.SendUnbuffered(&event)
	}

	_, err := r.RangeFeed(stream.ctx, req, stream, nil /* pacer */, nil /* perConsumerCatchupLimiter */)
	if err != nil {
		return sendErrToStream(kvpb.NewError(err))
	}

	select {
	case err := <-stream.done:
		return sendErrToStream(err)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// This test verifies that RangeFeed bypasses the circuit breaker. When the
// breaker is tripped, existing RangeFeeds remain in place and new ones can be
// started. When the breaker untrips, these feeds can make progress. The test
// goes as far as actually losing quorum to verify this end-to-end.
func TestReplicaCircuitBreaker_RangeFeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := setupCircuitBreakerTest(t, roachpb.LeaseNone)
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	require.NoError(t, tc.Write(n1))
	desc := tc.LookupRangeOrFatal(t, tc.ScratchRange(t))
	args := &kvpb.RangeFeedRequest{
		Span: roachpb.Span{Key: desc.StartKey.AsRawKey(), EndKey: desc.EndKey.AsRawKey()},
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream1 := newDummyStream(ctx, "rangefeed1")
	require.NoError(t, tc.Stopper().RunAsyncTask(ctx, "stream1", func(ctx context.Context) {
		err := waitReplicaRangeFeed(ctx, tc.repls[0].Replica, args, stream1)
		if ctx.Err() != nil {
			return // main goroutine stopping
		}
		assert.NoError(t, err) // avoid Fatal on goroutine
	}))

	readOneVal := func(ctx context.Context, stream *dummyStream, timeout time.Duration) error {
		for {
			select {
			case <-time.After(timeout):
				return errors.Errorf("%s: read timed out after %.2fs", stream.name, timeout.Seconds())
			case <-ctx.Done():
				return ctx.Err()
			case ev := <-stream.recv:
				if ev.Error != nil {
					return ev.Error.Error.GoError()
				}
				if ev.Val != nil {
					t.Logf("%s: %s", stream.name, ev)
					return nil
				}
			}
		}
	}

	testutils.SucceedsSoon(t, func() error {
		require.NoError(t, tc.Write(n1))
		return readOneVal(ctx, stream1, time.Millisecond)
	})

	// NB: keep heartbeats because we're not trying to lose the liveness range.
	undo := tc.ExpireAllLeasesAndN1LivenessRecord(t, keepHeartbeats)
	undo()
	tc.SetSlowThreshold(10 * time.Millisecond)
	tc.StopServer(n2)
	// NB: this is essentially always a breaker error but we can also get
	// NotLeaseholderError(heartbeat failed on epoch increment), so we retry
	// until we see the breaker error.
	testutils.SucceedsSoon(t, func() error {
		err := tc.Write(n1)
		if err == nil {
			return errors.New("no error returned")
		}
		if !tc.IsBreakerOpen(err) {
			return err
		}
		return nil
	})

	// Start another stream during the "outage" to make sure it isn't rejected by
	// the breaker.
	stream2 := newDummyStream(ctx, "rangefeed2")
	require.NoError(t, tc.Stopper().RunAsyncTask(ctx, "stream2", func(ctx context.Context) {
		err := waitReplicaRangeFeed(ctx, tc.repls[0].Replica, args, stream2)
		if ctx.Err() != nil {
			return // main goroutine stopping
		}
		assert.NoError(t, err) // avoid Fatal on goroutine
	}))

	tc.SetSlowThreshold(0)
	require.NoError(t, tc.RestartServer(n2))

	tc.UntripsSoon(t, tc.Write, n1)
	require.NoError(t, readOneVal(ctx, stream1, testutils.DefaultSucceedsSoonDuration))
	// For the stream that started mid-way through the outage, we expect it to
	// return a circuit breaker error, but in theory it could also never have
	// tried to acquire a lease, in which case it might return a value as well.
	if err := readOneVal(ctx, stream2, testutils.DefaultSucceedsSoonDuration); err != nil {
		if !tc.IsBreakerOpen(err) && !errors.HasType(err, (*kvpb.RangeFeedRetryError)(nil)) {
			t.Fatalf("%+v", err)
		}
	}
}

func TestReplicaCircuitBreaker_ExemptRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testutils.RunValues(t, "lease-type", roachpb.EpochAndLeaderLeaseType(),
		func(t *testing.T, leaseType roachpb.LeaseType) {
			tc := setupCircuitBreakerTest(t, leaseType)
			defer tc.Stopper().Stop(context.Background())

			// Put the lease on n1 but then trip the breaker with the probe
			// disabled, i.e. it will stay tripped.
			require.NoError(t, tc.Write(n1))
			tc.SetProbeEnabled(n1, false)
			tc.TripBreaker(n1)

			exemptRequests := []func() kvpb.Request{
				func() kvpb.Request { return &kvpb.ExportRequest{} },
				func() kvpb.Request {
					sstFile := &storage.MemObject{}
					sst := storage.MakeIngestionSSTWriter(context.Background(), cluster.MakeTestingClusterSettings(), sstFile)
					defer sst.Close()
					require.NoError(t, sst.LogData([]byte("hello")))
					require.NoError(t, sst.Finish())

					addReq := &kvpb.AddSSTableRequest{
						Data:           sstFile.Data(),
						IngestAsWrites: true,
					}
					return addReq
				},
				func() kvpb.Request {
					return &kvpb.RevertRangeRequest{TargetTime: tc.Servers[0].Clock().Now()}
				},
				func() kvpb.Request {
					return &kvpb.GCRequest{}
				},
				func() kvpb.Request {
					return &kvpb.ClearRangeRequest{}
				},
				func() kvpb.Request {
					return &kvpb.ProbeRequest{}
				},
			}

			for _, reqFn := range exemptRequests {
				req := reqFn()
				tc.Run(t, fmt.Sprintf("with-existing-lease/%s", req.Method()), func(t *testing.T) {
					require.NoError(t, tc.Send(n1, req))
				})
			}
			for _, reqFn := range exemptRequests {
				req := reqFn()
				tc.Run(t, fmt.Sprintf("with-acquire-lease/%s", req.Method()), func(t *testing.T) {
					resumeHeartbeats := tc.ExpireAllLeasesAndN1LivenessRecord(t, pauseHeartbeats)
					resumeHeartbeats() // intentionally resume right now so that lease can be acquired
					// NB: when looking into the traces here, we sometimes see - as expected -
					// that when the request tries to acquire a lease, the breaker is still
					// tripped. That's why there is a retry loop here.
					testutils.SucceedsSoon(t, func() error {
						err := tc.Send(n1, req)
						if errors.HasType(err, (*kvpb.NotLeaseHolderError)(nil)) {
							return err
						}
						require.NoError(t, err)
						return nil
					})
				})
			}

			var resumeNodeLivenessHeartbeats func()
			if leaseType == roachpb.LeaseEpoch {
				resumeNodeLivenessHeartbeats = tc.ExpireAllLeasesAndN1LivenessRecord(t, pauseHeartbeats)
			} else if leaseType == roachpb.LeaseLeader {
				tc.DisableAllStoreLivenessHeartbeats.Store(true)
				tc.ManualClock.Increment(tc.Servers[0].RaftConfig().RangeLeaseDuration.Nanoseconds())
			}

			for _, reqFn := range exemptRequests {
				req := reqFn()
				tc.Run(t, fmt.Sprintf("with-unavailable-lease/%s", req.Method()), func(t *testing.T) {
					if m := req.Method(); m == kvpb.Probe {
						// Probe does not require the lease, and is the most-tested of the bunch
						// already. We don't have to test it again here, which would require undue
						// amounts of special-casing below.
						skip.IgnoreLintf(t, "subtest does not apply to %s", m)
					}

					// For epoch leases usually logs [NotLeaseHolderError] lease
					// acquisition canceled because context canceled. While for leader
					// leases, there should be no error since we propose a leader lease
					// regardless of whether the LeadSupportUntil is in the future or not.
					// Therefore, we set a longer timeout for leader leases to avoid it
					// flaking in race/deadlock builds.
					var ctx context.Context
					var cancel context.CancelFunc
					if leaseType == roachpb.LeaseLeader {
						ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
					} else {
						ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
					}

					defer cancel()
					const maxWait = 5 * time.Second
					tBegin := timeutil.Now()
					err := tc.SendCtx(ctx, n1, req)

					t.Log(err)
					switch leaseType {
					case roachpb.LeaseEpoch:
						require.Error(t, err)
						require.Error(t, ctx.Err())
					case roachpb.LeaseLeader:
						require.NoError(t, err)
						require.NoError(t, ctx.Err())
					default:
						t.Fatalf("unexpected lease type: %v", leaseType)
					}

					// Make sure we didn't run into the "long" timeout inside of SendCtx but
					// actually terminated as a result of our ctx cancelling.
					require.Less(t, timeutil.Since(tBegin), maxWait)
				})
			}

			// Restore the breaker via the probe, and wait for any pending (re)proposals
			// from previous tests to be flushed.
			if leaseType == roachpb.LeaseEpoch {
				resumeNodeLivenessHeartbeats()
			} else if leaseType == roachpb.LeaseLeader {
				tc.DisableAllStoreLivenessHeartbeats.Store(false)
			}

			tc.SetProbeEnabled(n1, true)
			tc.UntripsSoon(t, tc.Write, n1)
			tc.WaitForProposals(t, n1)

			// Lose quorum (liveness stays intact).
			tc.SetSlowThreshold(10 * time.Millisecond)
			tc.StopServer(n2)
			// Let the breaker trip. This leaves a poisoned latch behind that at least some of
			// the requests will interact with.
			tc.RequireIsBreakerOpen(t, tc.Write(n1))
			tc.RequireIsBreakerOpen(t, tc.Read(n1))

			for _, reqFn := range exemptRequests {
				req := reqFn()
				tc.Run(t, fmt.Sprintf("with-poisoned-latch/%s", req.Method()), func(t *testing.T) {
					if m := req.Method(); m == kvpb.GC {
						// GC without GCKeys acquires no latches and is a pure read. If we want
						// to put a key in there, we need to pick the right timestamp (since you
						// can't GC a live key); it's all rather annoying and not worth it. In
						// the long run, we also completely want to avoid acquiring latches for
						// this request (since it should only mutate keyspace that has since
						// fallen under the GCThreshold), so avoid cooking up anything special
						// here.
						skip.IgnoreLintf(t, "subtest does not apply to %s", m)
					}
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Millisecond)
					defer cancel()
					const maxWait = 5 * time.Second
					tBegin := timeutil.Now()
					err := tc.SendCtx(ctx, n1, req)
					t.Log(err)
					require.Error(t, err)
					require.Error(t, ctx.Err())
					// Make sure we didn't run into the "long" timeout inside of SendCtx but
					// actually terminated as a result of our ctx cancelling.
					require.Less(t, timeutil.Since(tBegin), maxWait)
				})
			}
		})
}

// This tests that if the DistSender encounters individual replicas with
// ReplicaUnavailableError (i.e. tripped replica circuit breakers), then it will
// go on to try other replicas when possible (e.g. when there is a quorum
// elsewhere), but not get stuck in infinite retry loops (e.g. when replicas
// return NLHEs that point to a leaseholder or Raft leader which is unavailable,
// or when all replicas are unavailable).
//
// It does not use setupCircuitBreakerTest, which assumes only 2 nodes.
func TestReplicaCircuitBreaker_Partial_Retry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Timing-sensitive.
	skip.UnderRace(t)
	skip.UnderDeadlock(t)

	testutils.RunValues(t, "lease-type", roachpb.ExpirationAndLeaderLeaseType(),
		func(t *testing.T, leaseType roachpb.LeaseType) {
			// Use a context timeout, to prevent test hangs on failures.
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			// Use expiration-based or leader-based leases, such that the lease will
			// expire when we partition off Raft traffic on n3.
			st := cluster.MakeTestingClusterSettings()
			kvserver.OverrideDefaultLeaseType(ctx, &st.SV, leaseType)

			// Use a manual clock, so we can expire leases at will.
			manualClock := hlc.NewHybridManualClock()

			// Set up a 3-node cluster.
			tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						Server: &server.TestingKnobs{
							WallClock: manualClock,
						},
						// This test is requiring clients to go to the leaseholder first
						// to get URE errors in the case of a partial partition. If this
						// is not set, the test fails because it is counting the number
						// of URE errors it encounters.
						KVClient: &kvcoord.ClientTestingKnobs{RouteToLeaseholderFirst: true},
					},
					Settings: st,
					RaftConfig: base.RaftConfig{
						// Speed up the test.
						RaftTickInterval:           200 * time.Millisecond,
						RaftElectionTimeoutTicks:   5,
						RaftHeartbeatIntervalTicks: 1,
					},
				},
			})
			defer tc.Stopper().Stop(ctx)

			n1 := tc.Server(0)
			n2 := tc.Server(1)
			n3 := tc.Server(2)
			db1 := n1.ApplicationLayer().DB()
			db2 := n2.ApplicationLayer().DB()
			db3 := n3.ApplicationLayer().DB()
			dbs := []*kv.DB{db1, db2, db3}

			// Specify the key and value to use.
			prefix := append(n1.ApplicationLayer().Codec().TenantPrefix(), keys.ScratchRangeMin...)
			key := append(prefix.Clone(), []byte("/foo")...)
			value := []byte("bar")

			// Split off a range and upreplicate it.
			_, _, err := n1.StorageLayer().SplitRange(prefix)
			require.NoError(t, err)
			desc := tc.AddVotersOrFatal(t, prefix, tc.Targets(1, 2)...)
			t.Logf("split off range %s", desc)

			repl1 := tc.GetFirstStoreFromServer(t, 0).LookupReplica(roachpb.RKey(prefix))
			repl2 := tc.GetFirstStoreFromServer(t, 1).LookupReplica(roachpb.RKey(prefix))
			repl3 := tc.GetFirstStoreFromServer(t, 2).LookupReplica(roachpb.RKey(prefix))
			repls := []*kvserver.Replica{repl1, repl2, repl3}

			// Set up test helpers.
			requireRUEs := func(t *testing.T, dbs []*kv.DB) {
				t.Helper()
				for _, db := range dbs {
					backoffMetric := (db.NonTransactionalSender().(*kv.CrossRangeTxnWrapperSender)).Wrapped().(*kvcoord.DistSender).Metrics().InLeaseTransferBackoffs
					initialBackoff := backoffMetric.Count()
					err := db.Put(ctx, key, value)
					// Verify that we did not perform any backoff while executing this request.
					require.EqualValues(t, 0, backoffMetric.Count()-initialBackoff)
					require.Error(t, err)
					require.True(t, errors.HasType(err, (*kvpb.ReplicaUnavailableError)(nil)),
						"expected ReplicaUnavailableError, got %v", err)
				}
				t.Logf("writes failed with ReplicaUnavailableError")
			}

			requireNoRUEs := func(t *testing.T, dbs []*kv.DB) {
				t.Helper()
				for _, db := range dbs {
					require.NoError(t, db.Put(ctx, key, value))
				}
				t.Logf("writes succeeded")
			}

			// Move the leaseholder to n3, and wait for it to become the Raft leader
			// too.
			tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(2))
			t.Logf("transferred range lease to n3")

			require.Eventually(t, func() bool {
				for _, repl := range repls {
					if repl.RaftStatus().Lead != 3 {
						return false
					}
				}
				return true
			}, 5*time.Second, 100*time.Millisecond)
			t.Logf("transferred raft leadership to n3")

			requireNoRUEs(t, dbs)

			// Partition Raft traffic on n3 away from n1 and n2, and eagerly trip its
			// breaker. Note that we don't partition RPC traffic, such that client
			// requests and node liveness heartbeats still succeed.
			partitioned := &atomic.Bool{}
			partitioned.Store(true)
			dropRaftMessagesFrom(t, n1, desc, []roachpb.ReplicaID{3}, partitioned)
			dropRaftMessagesFrom(t, n2, desc, []roachpb.ReplicaID{3}, partitioned)
			dropRaftMessagesFrom(t, n3, desc, []roachpb.ReplicaID{1, 2}, partitioned)
			t.Logf("partitioned n3 raft traffic from n1 and n2")

			repl3.TripBreaker()
			t.Logf("tripped n3 circuit breaker")

			// While n3 is the leaseholder, all gateways should return RUE.
			requireRUEs(t, dbs)

			// Expire the lease, but not Raft leadership. All gateways should still
			// return RUE, since followers return NLHE pointing to the Raft leader,
			// and it will return RUE.
			lease, _ := repl3.GetLease()
			manualClock.Increment(tc.Servers[0].RaftConfig().RangeLeaseDuration.Nanoseconds())
			t.Logf("expired n%d lease", lease.Replica.ReplicaID)

			requireRUEs(t, dbs)

			// Wait for the leadership to move. Writes should now succeed -- they will
			// initially go to n3, the previous leaseholder, but it will return NLHE.
			// The DistSender will retry the other replicas, which eventually acquire
			// a new lease and serve the write.
			var leader raftpb.PeerID
			require.Eventually(t, func() bool {
				for _, repl := range repls {
					l := repl.RaftStatus().Lead
					if l == 3 {
						return false
					}
					if repl.ReplicaID() != 3 && l == 0 {
						// The old leader (3) steps down because of check quorum. A new
						// leader should be elected amongst 1 and 2, and both of them should
						// know who that is before we proceed with the test.
						return false
					} else if repl.ReplicaID() != 3 {
						leader = l
					}
				}
				return true
			}, 10*time.Second, 100*time.Millisecond)
			t.Logf("raft leadership moved to n%d", leader)

			requireNoRUEs(t, dbs)

			// Also partition n1 and n2 away from each other, and trip their breakers.
			// All nodes are now completely partitioned away from each other.
			dropRaftMessagesFrom(t, n1, desc, []roachpb.ReplicaID{2, 3}, partitioned)
			dropRaftMessagesFrom(t, n2, desc, []roachpb.ReplicaID{1, 3}, partitioned)

			repl1.TripBreaker()
			repl2.TripBreaker()
			t.Logf("partitioned all nodes and tripped their breakers")

			// n1 or n2 still has the lease. Writes should return a
			// ReplicaUnavailableError.
			requireRUEs(t, dbs)

			// Expire the lease, but not raft leadership. Writes should still error
			// because the leader's circuit breaker is tripped.
			lease, _ = repl1.GetLease()
			manualClock.Increment(tc.Servers[0].RaftConfig().RangeLeaseDuration.Nanoseconds())
			t.Logf("expired n%d lease", lease.Replica.ReplicaID)

			requireRUEs(t, dbs)

			// Wait for raft leadership to expire. Writes should error after the
			// DistSender attempts all 3 replicas and they all fail.
			require.Eventually(t, func() bool {
				for _, repl := range repls {
					if repl.RaftStatus().RaftState == raftpb.StateLeader {
						return false
					}
				}
				return true
			}, 10*time.Second, 100*time.Millisecond)
			t.Logf("no raft leader")

			requireRUEs(t, dbs)

			// Recover the partition. Writes should soon recover.
			partitioned.Store(false)
			t.Logf("partitioned healed")

			require.Eventually(t, func() bool {
				for _, db := range dbs {
					if err := db.Put(ctx, key, value); err != nil {
						return false
					}
				}
				return true
			}, 10*time.Second, 100*time.Millisecond)
			t.Logf("writes succeeded")

			require.NoError(t, ctx.Err())
		})
}

// Test infrastructure below.

func makeBreakerToggleable(b *circuit.Breaker) (setProbeEnabled func(bool)) {
	opts := b.Opts()
	origProbe := opts.AsyncProbe
	var disableProbe int32
	opts.AsyncProbe = func(report func(error), done func()) {
		if atomic.LoadInt32(&disableProbe) == 1 {
			done()
			return
		}
		origProbe(report, done)
	}
	b.Reconfigure(opts)
	return func(to bool) {
		var n int32
		if !to {
			n = 1
		}
		atomic.StoreInt32(&disableProbe, n)
	}
}

type circuitBreakerTest struct {
	t decoT
	*testcluster.TestCluster
	slowThresh                        *atomic.Value // time.Duration
	ManualClock                       *hlc.HybridManualClock
	DisableAllStoreLivenessHeartbeats *atomic.Bool
	repls                             []replWithKnob // 0 -> repl on Servers[0], etc
	seq                               int
}

func setupCircuitBreakerTest(t *testing.T, leaseType roachpb.LeaseType) *circuitBreakerTest {
	skip.UnderRace(t)
	manualClock := hlc.NewHybridManualClock()
	var rangeID int64 // atomic
	var disableAllStoreLivenessHeartbeats atomic.Bool
	slowThresh := &atomic.Value{} // supports .SetSlowThreshold(x)
	slowThresh.Store(time.Duration(0))
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	if leaseType == roachpb.LeaseNone {
		// If no lease type was explicitly provided, let metamorphism choose between
		// epoch-based leases and leader leases.
		// TODO(erikgrinaker): We may not need this for all circuit breaker tests.
		kvserver.ExpirationLeasesOnly.Override(ctx, &st.SV, false) // override metamorphism
	} else {
		kvserver.OverrideDefaultLeaseType(ctx, &st.SV, leaseType)
	}

	storeKnobs := &kvserver.StoreTestingKnobs{
		SlowReplicationThresholdOverride: func(ba *kvpb.BatchRequest) time.Duration {
			t.Helper()
			if rid := roachpb.RangeID(atomic.LoadInt64(&rangeID)); rid == 0 || ba == nil || ba.RangeID != rid {
				return 0
			}
			dur := slowThresh.Load().(time.Duration)
			if dur > 0 {
				t.Logf("%s: using slow replication threshold %s", ba.Summary(), dur)
			}
			return dur // 0 = default
		},
		// The test will often check individual replicas and the lease will always be on
		// n1. However, we don't control raft leadership placement and without this knob,
		// n1 may refuse to acquire the lease, which we don't want.
		AllowLeaseRequestProposalsWhenNotLeader: true,
		// The TestingApplyCalledTwiceFilter prevents n2 from requesting a lease (or from the lease
		// being transferred to n2). The test seems to pass pretty reliably without this
		// but it can't hurt.
		TestingApplyCalledTwiceFilter: func(args kvserverbase.ApplyFilterArgs) (int, *kvpb.Error) {
			if !args.IsLeaseRequest {
				return 0, nil
			}
			lease := args.State.Lease
			if lease == nil {
				return 0, nil
			}
			if lease.Replica.NodeID != 2 {
				return 0, nil
			}
			pErr := kvpb.NewErrorf("test prevents lease acquisition by n2")
			return 0, pErr
		},
		RangeLeaseAcquireTimeoutOverride: testutils.DefaultSucceedsSoonDuration,
		StoreLivenessKnobs: &storeliveness.TestingKnobs{
			SupportManagerKnobs: storeliveness.SupportManagerKnobs{
				DisableAllHeartbeats: &disableAllStoreLivenessHeartbeats,
			},
		},
	}
	// In some tests we'll restart servers, which means that we will be waiting
	// for raft elections. Speed this up by campaigning aggressively. This also
	// speeds up the test by calling refreshProposalsLocked more frequently, which
	// is where the logic to trip the breaker sits. Together, this cuts most tests
	// involving a restart from >4s to ~300ms.
	var raftCfg base.RaftConfig
	raftCfg.SetDefaults()
	raftCfg.RaftHeartbeatIntervalTicks = 1
	raftCfg.RaftElectionTimeoutTicks = 2
	reg := fs.NewStickyRegistry()
	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings:   st,
			RaftConfig: raftCfg,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					WallClock:         manualClock,
					StickyVFSRegistry: reg,
				},
				Store: storeKnobs,
			},
		},
	}
	tc := testcluster.StartTestCluster(t, 2, args)
	_, err := tc.ServerConn(0).Exec(`SET CLUSTER SETTING kv.replica_circuit_breaker.slow_replication_threshold = '45s'`)
	require.NoError(t, err)

	k := tc.ScratchRange(t)
	atomic.StoreInt64(&rangeID, int64(tc.LookupRangeOrFatal(t, k).RangeID))

	tc.AddVotersOrFatal(t, k, tc.Target(n2))
	require.NoError(t, tc.WaitForVoters(k, tc.Target(n2)))

	var repls []replWithKnob
	for i := range tc.Servers {
		repl := tc.GetFirstStoreFromServer(t, i).LookupReplica(keys.MustAddr(k))
		enableProbe := makeBreakerToggleable(repl.Breaker())
		repls = append(repls, replWithKnob{repl, enableProbe})
	}
	return &circuitBreakerTest{
		t:                                 decoT{t},
		TestCluster:                       tc,
		ManualClock:                       manualClock,
		repls:                             repls,
		slowThresh:                        slowThresh,
		DisableAllStoreLivenessHeartbeats: &disableAllStoreLivenessHeartbeats,
	}
}

// Run is a wrapper around t.Run that allows the test harness to print traces
// using the subtest's *testing.T.
func (cbt *circuitBreakerTest) Run(t *testing.T, name string, f func(t *testing.T)) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		t.Helper()
		outerT := cbt.t
		cbt.t = decoT{t}
		defer func() {
			cbt.t = outerT
		}()
		f(t)
	})
}

func (cbt *circuitBreakerTest) SetProbeEnabled(idx int, to bool) {
	cbt.repls[idx].setProbeEnabled(to)
}

func (cbt *circuitBreakerTest) TripBreaker(idx int) {
	repl := cbt.repls[idx].Replica
	repl.TripBreaker()
}

func (cbt *circuitBreakerTest) UntripsSoon(t *testing.T, method func(idx int) error, idx int) {
	t.Helper()
	testutils.SucceedsWithin(t, func() error {
		t.Helper()
		err := method(idx)
		// All errors coming out should be annotated as coming from
		// the circuit breaker. In rare cases, we can also see a
		// NotLeaseholderError such as this one:
		// [NotLeaseHolderError] failed to manipulate liveness record: heartbeat
		// failed on epoch increment; r45: replica (n1,s1):1 not lease holder;
		// current lease is repl=(n1,s1):1 seq=1 start=0,0 epo=1 pro=[...]
		if err != nil &&
			!errors.Is(err, circuit.ErrBreakerOpen) &&
			!errors.HasType(err, (*kvpb.NotLeaseHolderError)(nil)) {

			t.Fatalf("saw unexpected error %+v", err)
		}
		return err
	}, 2*testutils.SucceedsSoonDuration())
}

func (cbt *circuitBreakerTest) WaitForProposals(t *testing.T, idx int) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		t.Helper()

		repl := cbt.repls[idx].Replica
		if n := repl.NumPendingProposals(); n > 0 {
			return errors.Errorf("%d pending proposals", n)
		}
		return nil
	})
}

func (cbt *circuitBreakerTest) ExpireAllLeasesAndN1LivenessRecord(
	t *testing.T, pauseHeartbeats bool,
) (undo func()) {
	t.Helper()
	var fs []func()
	for idx, srv := range cbt.Servers {
		lv := srv.NodeLiveness().(*liveness.NodeLiveness)

		if pauseHeartbeats {
			undo := lv.PauseAllHeartbeatsForTest()
			fs = append(fs, undo)
		}

		self, ok := lv.Self()
		require.True(t, ok)

		ts := hlc.Timestamp{WallTime: self.Expiration.WallTime}
		require.NoError(t, srv.GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
			s.VisitReplicas(func(replica *kvserver.Replica) (wantMore bool) {
				lease, next := replica.GetLease()
				if lease.Expiration != nil {
					ts.Forward(*lease.Expiration)
				}
				if next.Expiration != nil {
					ts.Forward(*next.Expiration)
				}
				return true // more
			})
			return nil
		}))

		cbt.ManualClock.Forward(ts.WallTime)
		if idx == n1 {
			// Invalidate n1's liveness record, to make sure that ranges on n1 need
			// to acquire a new lease (vs waiting for a heartbeat to the liveness
			// record resuscitating the old one).
			//
			// Needing to do this is the reason for special-casing this entire method
			// around n1; if we stop heartbeats for both nodes, they can't increment
			// each others liveness records: if a node's liveness is paused, it doesn't
			// allow incrementing records neither. (This is silly).
			lv2 := cbt.Server(n2).NodeLiveness().(*liveness.NodeLiveness)
			testutils.SucceedsSoon(t, func() error {
				self, ok := lv.Self()
				require.True(t, ok)
				if self.IsLive(cbt.Server(n2).Clock().Now()) {
					// Someone else must have incremented epoch.
					return nil
				}
				return lv2.IncrementEpoch(context.Background(), self)
			})
		}
	}

	return func() {
		for _, f := range fs {
			f()
		}
	}
}

func (cbt *circuitBreakerTest) Send(idx int, req kvpb.Request) error {
	cbt.t.Helper()
	return cbt.SendCtx(context.Background(), idx, req)
}

func (cbt *circuitBreakerTest) SendCtx(ctx context.Context, idx int, req kvpb.Request) error {
	return cbt.SendCtxTS(ctx, idx, req, cbt.repls[idx].Clock().Now())
}

func (cbt *circuitBreakerTest) SendCtxTS(
	ctx context.Context, idx int, req kvpb.Request, ts hlc.Timestamp,
) error {
	cbt.t.Helper()
	ctx, finishAndGet := tracing.ContextWithRecordingSpan(ctx, cbt.repls[idx].Tracer, "SendCtx("+req.Method().String()+")")
	defer time.AfterFunc(10*time.Second, func() {
		rec := tracing.SpanFromContext(ctx).GetConfiguredRecording()
		cbt.t.Logf("slow request: %s", rec)
	}).Stop()
	defer func() {
		cbt.t.Helper()
		rec := finishAndGet()
		cbt.t.Logf("%s", rec)
	}()
	ba := &kvpb.BatchRequest{}
	repl := cbt.repls[idx]
	ba.RangeID = repl.Desc().RangeID
	ba.Timestamp = ts
	ba.Add(req)
	if h := req.Header(); len(h.Key) == 0 {
		h.Key = repl.Desc().StartKey.AsRawKey()
		if kvpb.IsRange(req) {
			h.EndKey = repl.Desc().EndKey.AsRawKey()
		}
		req.SetHeader(h)
	}
	parCtx := ctx
	ctx, cancel := context.WithTimeout(ctx, testutils.DefaultSucceedsSoonDuration)
	defer cancel()
	// Tag the breaker with the request. Once Send returns, we'll check that it's
	// no longer tracked by the breaker. This gives good coverage that we're not
	// going to leak memory.
	ctx = context.WithValue(ctx, req, struct{}{})

	_, pErr := repl.Send(ctx, ba)
	// If our context got canceled, return an opaque error regardless of presence or
	// absence of actual error. This makes sure we don't accidentally pass tests as
	// a result of our context cancellation.
	if err := ctx.Err(); err != nil && parCtx.Err() == nil {
		pErr = kvpb.NewErrorf("timed out waiting for batch response: %v", pErr)
	}

	return pErr.GoError()
}

func (cbt *circuitBreakerTest) WriteDS(idx int) error {
	cbt.t.Helper()
	put := kvpb.NewPut(cbt.repls[idx].Desc().StartKey.AsRawKey(), roachpb.MakeValueFromString("hello"))
	return cbt.sendViaDistSender(cbt.Servers[idx].DistSenderI().(kv.Sender), put)
}

func (cbt *circuitBreakerTest) sendViaDistSender(ds kv.Sender, req kvpb.Request) error {
	cbt.t.Helper()
	ba := &kvpb.BatchRequest{}
	ba.Add(req)
	ctx, cancel := context.WithTimeout(context.Background(), testutils.DefaultSucceedsSoonDuration)
	defer cancel()
	_, pErr := ds.Send(ctx, ba)
	// If our context got canceled, return an opaque error regardless of presence or
	// absence of actual error. This makes sure we don't accidentally pass tests as
	// a result of our context cancellation.
	if err := ctx.Err(); err != nil {
		pErr = kvpb.NewErrorf("timed out waiting for batch response: %v", pErr)
	}
	return pErr.GoError()
}

func (*circuitBreakerTest) IsBreakerOpen(err error) bool {
	// NB: we will see AmbiguousResultError here when proposals are inflight while
	// the breaker trips. These are wrapping errors, so the assertions below will
	// look through them.
	if !errors.Is(err, circuit.ErrBreakerOpen) {
		return false
	}
	return errors.HasType(err, (*kvpb.ReplicaUnavailableError)(nil))
}

func (cbt *circuitBreakerTest) RequireIsBreakerOpen(t *testing.T, err error) {
	t.Helper()
	t.Log(err)
	// NB: we will see AmbiguousResultError here when proposals are inflight while
	// the breaker trips. These are wrapping errors, so the assertions below will
	// look through them.
	require.True(t, cbt.IsBreakerOpen(err), "%+v", err)
}

func (*circuitBreakerTest) RequireIsNotLeaseholderError(t *testing.T, err error) {
	t.Helper()
	t.Log(err)
	ok := errors.HasType(err, (*kvpb.NotLeaseHolderError)(nil))
	require.True(t, ok, "%+v", err)
}

// SetSlowThreshold sets the SlowReplicationThreshold for requests sent through the
// test harness (i.e. via Write) to the provided duration. The zero value restores
// the default.
func (cbt *circuitBreakerTest) SetSlowThreshold(dur time.Duration) {
	cbt.slowThresh.Store(dur)
}

func (cbt *circuitBreakerTest) Write(idx int) error {
	cbt.t.Helper()
	repl := cbt.repls[idx]
	cbt.seq++
	put := kvpb.NewPut(
		repl.Desc().StartKey.AsRawKey(), roachpb.MakeValueFromString(fmt.Sprintf("hello-%d", cbt.seq)),
	)
	return cbt.Send(idx, put)
}

func (cbt *circuitBreakerTest) Read(idx int) error {
	cbt.t.Helper()
	repl := cbt.repls[idx]
	get := kvpb.NewGet(repl.Desc().StartKey.AsRawKey())
	return cbt.Send(idx, get)
}

func (cbt *circuitBreakerTest) FollowerRead(idx int) error {
	cbt.t.Helper()
	repl := cbt.repls[idx]
	get := kvpb.NewGet(repl.Desc().StartKey.AsRawKey())
	ctx := context.Background()
	ts := repl.GetCurrentClosedTimestamp(ctx)
	return cbt.SendCtxTS(ctx, idx, get, ts)
}

func (cbt *circuitBreakerTest) HeartbeatNodeLiveness(t *testing.T, idx int) {
	// Retry loop is needed because heartbeat may race with internal heartbeat
	// loop.
	testutils.SucceedsSoon(t, func() error {
		return cbt.Server(idx).HeartbeatNodeLiveness()
	})
}

type replWithKnob struct {
	*kvserver.Replica
	setProbeEnabled func(bool)
}

type logT interface {
	Helper()
	Logf(string, ...interface{})
}

type decoT struct {
	logT
}

func (t *decoT) Logf(format string, args ...interface{}) {
	// It can be difficult to spot the actual failure among all of the
	// traces, so this is a convenient place to annotate the logging
	// (or disable it one-off).
	t.logT.Logf("info:\n"+format, args...)
}
