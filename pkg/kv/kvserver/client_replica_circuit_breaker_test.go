// Copyright 2021 The Cockroach Authors.
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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
	tc := setupCircuitBreakerTest(t)
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
	tc := setupCircuitBreakerTest(t)
	defer tc.Stopper().Stop(context.Background())
	k := tc.ScratchRange(t)

	// Get lease on n1.
	require.NoError(t, tc.Write(n1))
	// Disable the probe so that when the breaker trips, it stays tripped.
	tc.SetProbeEnabled(n1, false)
	tc.Report(n1, errors.New("injected breaker error"))

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
	tc := setupCircuitBreakerTest(t)
	defer tc.Stopper().Stop(context.Background())

	// Get lease on n1.
	require.NoError(t, tc.Write(n1))
	// Disable the probe on n2 so that when the breaker trips, it stays tripped.
	tc.SetProbeEnabled(n2, false)
	tc.Report(n2, errors.New("injected breaker error"))

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
	tc := setupCircuitBreakerTest(t)
	defer tc.Stopper().Stop(context.Background())

	// Put the lease on n1 but then trip the breaker with the probe
	// disabled.
	require.NoError(t, tc.Write(n1))
	tc.SetProbeEnabled(n1, false)
	tc.Report(n1, errors.New("injected breaker error"))
	resumeHeartbeats := tc.ExpireAllLeasesAndN1LivenessRecord(t, pauseHeartbeats)

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
	testutils.SucceedsSoon(t, func() error {
		err := tc.Read(n1)
		if errors.HasType(err, (*roachpb.NotLeaseHolderError)(nil)) {
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
	tc := setupCircuitBreakerTest(t)
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
		var ae *roachpb.AmbiguousResultError
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
	skip.WithIssue(t, 76781, "flaky test")
	defer log.Scope(t).Close(t)
	tc := setupCircuitBreakerTest(t)
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
	tc := setupCircuitBreakerTest(t)
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

type dummyStream struct {
	name string
	t    interface {
		Helper()
		Logf(string, ...interface{})
	}
	ctx  context.Context
	recv chan *roachpb.RangeFeedEvent
}

func (s *dummyStream) Context() context.Context {
	return s.ctx
}

func (s *dummyStream) Send(ev *roachpb.RangeFeedEvent) error {
	if ev.Val == nil && ev.Error == nil {
		s.t.Logf("%s: ignoring event: %v", s.name, ev)
		return nil
	}
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.recv <- ev:
		return nil
	}
}

// This test verifies that RangeFeed bypasses the circuit breaker. When the
// breaker is tripped, existing RangeFeeds remain in place and new ones can be
// started. When the breaker untrips, these feeds can make progress. The test
// goes as far as actually losing quorum to verify this end-to-end.
func TestReplicaCircuitBreaker_RangeFeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.WithIssue(t, 76856, "flaky test")
	defer log.Scope(t).Close(t)
	tc := setupCircuitBreakerTest(t)
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	require.NoError(t, tc.Write(n1))
	desc := tc.LookupRangeOrFatal(t, tc.ScratchRange(t))
	args := &roachpb.RangeFeedRequest{
		Span: roachpb.Span{Key: desc.StartKey.AsRawKey(), EndKey: desc.EndKey.AsRawKey()},
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream1 := &dummyStream{t: t, ctx: ctx, name: "rangefeed1", recv: make(chan *roachpb.RangeFeedEvent)}
	require.NoError(t, tc.Stopper().RunAsyncTask(ctx, "stream1", func(ctx context.Context) {
		err := tc.repls[0].RangeFeed(args, stream1).GoError()
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
	tc.RequireIsBreakerOpen(t, tc.Write(n1))

	// Start another stream during the "outage" to make sure it isn't rejected by
	// the breaker.
	stream2 := &dummyStream{t: t, ctx: ctx, name: "rangefeed2", recv: make(chan *roachpb.RangeFeedEvent)}
	require.NoError(t, tc.Stopper().RunAsyncTask(ctx, "stream2", func(ctx context.Context) {
		err := tc.repls[0].RangeFeed(args, stream2).GoError()
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
		tc.RequireIsBreakerOpen(t, err)
	}
}

func TestReplicaCircuitBreaker_ExemptRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := setupCircuitBreakerTest(t)
	defer tc.Stopper().Stop(context.Background())

	// Put the lease on n1 but then trip the breaker with the probe
	// disabled, i.e. it will stay tripped.
	require.NoError(t, tc.Write(n1))
	tc.SetProbeEnabled(n1, false)
	tc.Report(n1, errors.New("injected breaker error"))

	exemptRequests := []func() roachpb.Request{
		func() roachpb.Request { return &roachpb.ExportRequest{ReturnSST: true} },
		func() roachpb.Request {
			sstFile := &storage.MemFile{}
			sst := storage.MakeIngestionSSTWriter(context.Background(), cluster.MakeTestingClusterSettings(), sstFile)
			defer sst.Close()
			require.NoError(t, sst.LogData([]byte("hello")))
			require.NoError(t, sst.Finish())

			addReq := &roachpb.AddSSTableRequest{
				Data:           sstFile.Data(),
				IngestAsWrites: true,
			}
			return addReq
		},
		func() roachpb.Request {
			return &roachpb.RevertRangeRequest{TargetTime: tc.Servers[0].Clock().Now()}
		},
		func() roachpb.Request {
			return &roachpb.GCRequest{}
		},
		func() roachpb.Request {
			return &roachpb.ClearRangeRequest{}
		},
		func() roachpb.Request {
			return &roachpb.ProbeRequest{}
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
				if errors.HasType(err, (*roachpb.NotLeaseHolderError)(nil)) {
					return err
				}
				require.NoError(t, err)
				return nil
			})
		})
	}

	resumeHeartbeats := tc.ExpireAllLeasesAndN1LivenessRecord(t, pauseHeartbeats)

	for _, reqFn := range exemptRequests {
		req := reqFn()
		tc.Run(t, fmt.Sprintf("with-unavailable-lease/%s", req.Method()), func(t *testing.T) {
			if m := req.Method(); m == roachpb.Probe {
				// Probe does not require the lease, and is the most-tested of the bunch
				// already. We don't have to test it again here, which would require undue
				// amounts of special-casing below.
				skip.IgnoreLintf(t, "subtest does not apply to %s", m)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Millisecond)
			defer cancel()
			const maxWait = 5 * time.Second
			tBegin := timeutil.Now()
			err := tc.SendCtx(ctx, n1, req)
			t.Log(err) // usually: [NotLeaseHolderError] lease acquisition canceled because context canceled
			require.Error(t, err)
			require.Error(t, ctx.Err())
			// Make sure we didn't run into the "long" timeout inside of SendCtx but
			// actually terminated as a result of our ctx cancelling.
			require.Less(t, timeutil.Since(tBegin), maxWait)
		})
	}

	// Restore the breaker via the probe.
	resumeHeartbeats()
	tc.SetProbeEnabled(n1, true)
	tc.UntripsSoon(t, tc.Write, n1)

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
			if m := req.Method(); m == roachpb.GC {
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
	slowThresh  *atomic.Value // time.Duration
	ManualClock *hlc.HybridManualClock
	repls       []replWithKnob // 0 -> repl on Servers[0], etc

	seq int
}

func setupCircuitBreakerTest(t *testing.T) *circuitBreakerTest {
	skip.UnderStressRace(t)
	manualClock := hlc.NewHybridManualClock()
	var rangeID int64             // atomic
	slowThresh := &atomic.Value{} // supports .SetSlowThreshold(x)
	slowThresh.Store(time.Duration(0))
	storeKnobs := &kvserver.StoreTestingKnobs{
		SlowReplicationThresholdOverride: func(ba *roachpb.BatchRequest) time.Duration {
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
		// The TestingApplyFilter prevents n2 from requesting a lease (or from the lease
		// being transferred to n2). The test seems to pass pretty reliably without this
		// but it can't hurt.
		TestingApplyFilter: func(args kvserverbase.ApplyFilterArgs) (int, *roachpb.Error) {
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
			pErr := roachpb.NewErrorf("test prevents lease acquisition by n2")
			return 0, pErr
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
	reg := server.NewStickyInMemEnginesRegistry()
	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			RaftConfig: raftCfg,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					ClockSource:          manualClock.UnixNano,
					StickyEngineRegistry: reg,
				},
				Store: storeKnobs,
			},
		},
	}
	tc := testcluster.StartTestCluster(t, 2, args)
	tc.Stopper().AddCloser(stop.CloserFn(reg.CloseAllStickyInMemEngines))

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
		t:           decoT{t},
		TestCluster: tc,
		ManualClock: manualClock,
		repls:       repls,
		slowThresh:  slowThresh,
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

func (cbt *circuitBreakerTest) Report(idx int, err error) {
	cbt.repls[idx].Replica.Breaker().Report(err)
}

func (cbt *circuitBreakerTest) UntripsSoon(t *testing.T, method func(idx int) error, idx int) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
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
			!errors.HasType(err, (*roachpb.NotLeaseHolderError)(nil)) {

			t.Fatalf("saw unexpected error %+v", err)
		}
		return err
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

		cbt.ManualClock.Forward(self.Expiration.WallTime)
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
				if self.IsLive(cbt.Server(n2).Clock().Now().GoTime()) {
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

func (cbt *circuitBreakerTest) Send(idx int, req roachpb.Request) error {
	cbt.t.Helper()
	return cbt.SendCtx(context.Background(), idx, req)
}

func (cbt *circuitBreakerTest) SendCtx(ctx context.Context, idx int, req roachpb.Request) error {
	return cbt.SendCtxTS(ctx, idx, req, cbt.repls[idx].Clock().Now())
}

func (cbt *circuitBreakerTest) SendCtxTS(
	ctx context.Context, idx int, req roachpb.Request, ts hlc.Timestamp,
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
	var ba roachpb.BatchRequest
	repl := cbt.repls[idx]
	ba.RangeID = repl.Desc().RangeID
	ba.Timestamp = ts
	ba.Add(req)
	if h := req.Header(); len(h.Key) == 0 {
		h.Key = repl.Desc().StartKey.AsRawKey()
		if roachpb.IsRange(req) {
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
		pErr = roachpb.NewErrorf("timed out waiting for batch response: %v", pErr)
	}

	return pErr.GoError()
}

func (cbt *circuitBreakerTest) WriteDS(idx int) error {
	cbt.t.Helper()
	put := roachpb.NewPut(cbt.repls[idx].Desc().StartKey.AsRawKey(), roachpb.MakeValueFromString("hello"))
	return cbt.sendViaDistSender(cbt.Servers[idx].DistSender(), put)
}

func (cbt *circuitBreakerTest) sendViaDistSender(
	ds *kvcoord.DistSender, req roachpb.Request,
) error {
	cbt.t.Helper()
	var ba roachpb.BatchRequest
	ba.Add(req)
	ctx, cancel := context.WithTimeout(context.Background(), testutils.DefaultSucceedsSoonDuration)
	defer cancel()
	_, pErr := ds.Send(ctx, ba)
	// If our context got canceled, return an opaque error regardless of presence or
	// absence of actual error. This makes sure we don't accidentally pass tests as
	// a result of our context cancellation.
	if err := ctx.Err(); err != nil {
		pErr = roachpb.NewErrorf("timed out waiting for batch response: %v", pErr)
	}
	return pErr.GoError()
}

func (*circuitBreakerTest) RequireIsBreakerOpen(t *testing.T, err error) {
	t.Helper()
	t.Log(err)
	// We also accept an ambiguous result wrapping a breaker error; this occurs
	// when the breaker trips while a write is already inflight.
	if aErr := (*roachpb.AmbiguousResultError)(nil); errors.As(err, &aErr) && aErr.WrappedErr != nil {
		err = aErr.WrappedErr.GoError()
	}
	require.True(t, errors.Is(err, circuit.ErrBreakerOpen), "%+v", err)
}

func (*circuitBreakerTest) RequireIsNotLeaseholderError(t *testing.T, err error) {
	t.Helper()
	t.Log(err)
	ok := errors.HasType(err, (*roachpb.NotLeaseHolderError)(nil))
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
	put := roachpb.NewPut(
		repl.Desc().StartKey.AsRawKey(), roachpb.MakeValueFromString(fmt.Sprintf("hello-%d", cbt.seq)),
	)
	return cbt.Send(idx, put)
}

func (cbt *circuitBreakerTest) Read(idx int) error {
	cbt.t.Helper()
	repl := cbt.repls[idx]
	get := roachpb.NewGet(repl.Desc().StartKey.AsRawKey(), false /* forUpdate */)
	return cbt.Send(idx, get)
}

func (cbt *circuitBreakerTest) FollowerRead(idx int) error {
	cbt.t.Helper()
	repl := cbt.repls[idx]
	get := roachpb.NewGet(repl.Desc().StartKey.AsRawKey(), false /* forUpdate */)
	ctx := context.Background()
	ts := repl.GetClosedTimestamp(ctx)
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
