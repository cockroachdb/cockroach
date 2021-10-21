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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestReplicaCircuitBreaker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := setupCircuitBreakerTest(t)
	defer tc.Stopper().Stop(ctx)

	// In all scenarios below, we are starting out with our range on n1 and n2,
	// and all other ranges (in particular the liveness range) on n1.
	const (
		n1 = 0
		n2 = 1

		pauseHeartbeats = true
		keepHeartbeats  = true
	)

	// This is a sanity check in which the breaker plays no role.
	runCircuitBreakerTest(t, "breaker-ok", func(t *testing.T, ctx context.Context, tc *circuitBreakerTest) {
		// Circuit breaker doesn't get in the way of anything unless
		// something trips it.
		require.NoError(t, tc.Write(n1))
		tc.RequireIsNotLeaseholderError(t, tc.Write(n2))
		require.NoError(t, tc.Read(n1))
		tc.RequireIsNotLeaseholderError(t, tc.Read(n2))
	})

	// In this test, n1 holds the lease and we disable the probe and trip the
	// breaker. While the breaker is tripped, requests fail-fast with either a
	// breaker or lease error. When the probe is re-enabled, everything heals.
	runCircuitBreakerTest(t, "leaseholder-tripped", func(t *testing.T, ctx context.Context, tc *circuitBreakerTest) {
		// Get lease on n1.
		require.NoError(t, tc.Write(n1))
		// Disable the probe so that when the breaker trips, it stays tripped.
		tc.SetProbeEnabled(n1, false)
		tc.Report(n1, errors.New("boom"))

		// n1 could theoretically still serve reads (there is a valid lease
		// and none of the latches are taken), but since it is hard to determine
		// that upfront we currently fail all reads as well.
		tc.RequireIsBreakerOpen(t, tc.Read(n1))
		tc.RequireIsBreakerOpen(t, tc.Write(n1))

		// When we go through the KV client stack, we still get the breaker error
		// back.
		tc.RequireIsBreakerOpen(t, tc.WriteDS(n1))
		tc.RequireIsBreakerOpen(t, tc.WriteDS(n2))

		// n2 does not have the lease so all it does is redirect to the leaseholder
		// n1.
		tc.RequireIsNotLeaseholderError(t, tc.Read(n2))
		tc.RequireIsNotLeaseholderError(t, tc.Write(n2))

		// Enable the probe. Even a read should trigger the probe
		// and within due time the breaker should heal.
		tc.SetProbeEnabled(n1, true)
		tc.UntripsSoon(t, tc.Read, n1)
		// Same behavior on writes.
		tc.Report(n1, errors.New("boom again"))
		tc.UntripsSoon(t, tc.Write, n1)
	})

	// In this scenario we have n1 holding the lease and we permanently trip the
	// breaker on follower n2. Before the breaker is tripped, we see
	// NotLeaseholderError. When it's tripped, those are supplanted by the breaker
	// errors. Once we allow the breaker to probe, the breaker untrips. In
	// particular, this tests that the probe can succeed even when run on a
	// follower (which would not be true if it required the local Replica to
	// execute an operation that requires the lease).
	runCircuitBreakerTest(t, "follower-tripped", func(t *testing.T, ctx context.Context, tc *circuitBreakerTest) {
		// Get lease on n1.
		require.NoError(t, tc.Write(n1))
		// Disable the probe on n2 so that when the breaker trips, it stays tripped.
		tc.SetProbeEnabled(n2, false)
		tc.Report(n2, errors.New("boom"))

		// We didn't trip the leaseholder n1, so it is unaffected.
		require.NoError(t, tc.Read(n1))
		require.NoError(t, tc.Write(n1))
		// Even if we go through DistSender, we reliably reach the leaseholder.
		// TODO(tbg): I think this relies on the leaseholder being cached. If
		// DistSender tried to contact the follower and got the breaker error, at
		// time of writing it would propagate it.
		require.NoError(t, tc.WriteDS(n1))

		tc.RequireIsBreakerOpen(t, tc.Read(n2))
		tc.RequireIsBreakerOpen(t, tc.Write(n2))

		// Enable the probe. Even a read should trigger the probe
		// and within due time the breaker should heal, giving us
		// NotLeaseholderErrors again.
		//
		// TODO(tbg): this test would be more meaningful with follower reads. They
		// should succeed when the breaker is open and fail if the breaker is
		// tripped. However knowing that the circuit breaker check sits at the top
		// of Replica.sendWithRangeID, it's clear that it won't make a difference.
		tc.SetProbeEnabled(n2, true)
		testutils.SucceedsSoon(t, func() error {
			if err := tc.Read(n2); !errors.HasType(err, (*roachpb.NotLeaseHolderError)(nil)) {
				return err
			}
			return nil
		})
		// Same behavior on writes.
		tc.Report(n2, errors.New("boom again"))
		testutils.SucceedsSoon(t, func() error {
			if err := tc.Write(n2); !errors.HasType(err, (*roachpb.NotLeaseHolderError)(nil)) {
				return err
			}
			return nil
		})
	})

	// In this scenario, the breaker is tripped and the probe is disabled and
	// additionally, the liveness records for both nodes have expired. Soon after
	// the probe is re-enabled, the breaker heals. In particular, the probe isn't
	// doing anything that requires the lease (or whatever it does that requires
	// the lease is sufficiently special cased; at time of writing it's the former
	// but as the probe learns deeper checks, the plan is ultimately the latter).
	runCircuitBreakerTest(t, "leaseless-tripped", func(t *testing.T, ctx context.Context, tc *circuitBreakerTest) {
		// Put the lease on n1 but then trip the breaker with the probe
		// disabled.
		require.NoError(t, tc.Write(n1))
		tc.SetProbeEnabled(n1, false)
		tc.Report(n1, errors.New("boom"))
		resumeHeartbeats := tc.ExpireAllLeases(t, pauseHeartbeats)

		// n2 (not n1) will return a NotLeaseholderError. This may be surprising -
		// why isn't it trying and succeeding to acquire a lease - but it does
		// not do that because it sees that the new leaseholder (n2) is not live
		// itself. We'll revisit this after re-enabling liveness later in the test.
		{
			err := tc.Read(n2)
			// At time of writing: not incrementing epoch on n1 because next
			// leaseholder (n2) not live.
			t.Log(err)
			tc.RequireIsNotLeaseholderError(t, err)
			// Same behavior for write on n2.
			tc.RequireIsNotLeaseholderError(t, tc.Write(n2))
		}
		// On n1, run into the circuit breaker when requesting lease.
		{
			tc.RequireIsBreakerOpen(t, tc.Read(n1))
			tc.RequireIsBreakerOpen(t, tc.Write(n1))
		}

		// Let the breaker heal and things should go back to normal. This is not a
		// trivial thing to hold, as the probe needs to go through for this, and if
		// we're not careful, the probe itself is held up by the breaker as well, or
		// the probe will try to acquire a lease (which we're currently careful to
		// avoid).
		resumeHeartbeats()
		tc.SetProbeEnabled(n1, true)
		tc.UntripsSoon(t, tc.Read, n1)
		tc.UntripsSoon(t, tc.Write, n1)
		tc.RequireIsNotLeaseholderError(t, tc.Read(n2))
		tc.RequireIsNotLeaseholderError(t, tc.Write(n2))
	})

	// In this test, the range is on n1 and n2 and we take down the follower n2,
	// thus losing quorum (but not the lease or leaseholder). After the
	// SlowReplicationThreshold (which is reduced suitably to keep the test
	// snappy) has passed, the breaker on n1's Replica trips. When n2 comes back,
	// the probe on n1 succeeds and requests to n1 can acquire a lease and
	// succeed.
	runCircuitBreakerTest(t, "leaseholder-quorum-loss", func(t *testing.T, ctx context.Context, tc *circuitBreakerTest) {
		// Get lease on n1.
		require.NoError(t, tc.Write(n1))
		tc.StopServer(n2) // lose quorum

		// We didn't lose the liveness range (which is only on n1).
		require.NoError(t, tc.Server(n1).HeartbeatNodeLiveness())
		tc.SetSlowThreshold(10 * time.Millisecond)
		{
			err := tc.Write(n1)
			var ae *roachpb.AmbiguousResultError
			require.True(t, errors.As(err, &ae), "%+v", err)
			t.Log(err)
		}
		tc.RequireIsBreakerOpen(t, tc.Read(n1))

		// Bring n2 back and service should be restored.
		tc.SetSlowThreshold(0) // reset
		require.NoError(t, tc.RestartServer(n2))
		tc.UntripsSoon(t, tc.Read, n1)
		require.NoError(t, tc.Write(n1))
	})

	// In this test, the range is on n1 and n2 and we place the lease on n2 and
	// shut down n2 and expire the lease. n1 will be a non-leaseholder without
	// quorum, and requests to it should trip the circuit breaker. This is an
	// interesting test case internally because here, the request that trips the
	// breaker is the slow lease request, and not the tests's actual write. Since
	// leases have lots of special casing internally, this is easy to get wrong.
	runCircuitBreakerTest(t, "follower-quorum-loss", func(t *testing.T, ctx context.Context, tc *circuitBreakerTest) {
		// Get lease to n2 so that we can lose it without taking down the system ranges.
		desc := tc.LookupRangeOrFatal(t, tc.ScratchRange(t))
		tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(n2))
		resumeHeartbeats := tc.ExpireAllLeases(t, keepHeartbeats)
		tc.StopServer(n2) // lose quorum and leaseholder
		resumeHeartbeats()

		// We didn't lose the liveness range (which is only on n1).
		require.NoError(t, tc.Server(n1).HeartbeatNodeLiveness())
		tc.SetSlowThreshold(10 * time.Millisecond)
		tc.RequireIsBreakerOpen(t, tc.Write(n1))
		tc.RequireIsBreakerOpen(t, tc.Read(n1))

		// Bring n2 back and service should be restored.
		tc.SetSlowThreshold(0) // reset
		require.NoError(t, tc.RestartServer(n2))
		tc.UntripsSoon(t, tc.Read, n1)
		require.NoError(t, tc.Write(n1))
	})

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
	runCircuitBreakerTest(t, "liveness-unavailable", func(t *testing.T, ctx context.Context, tc *circuitBreakerTest) {
		skip.IgnoreLint(t, "See: https://github.com/cockroachdb/cockroach/issues/74616")
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
		resume := tc.ExpireAllLeases(t, pauseHeartbeats)
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

type replWithKnob struct {
	*kvserver.Replica
	setProbeEnabled func(bool)
}

type circuitBreakerTest struct {
	*testcluster.TestCluster
	slowThresh  *atomic.Value // time.Duration
	ManualClock *hlc.HybridManualClock
	repls       []replWithKnob // 0 -> repl on Servers[0], etc
}

func runCircuitBreakerTest(
	t *testing.T, name string, f func(*testing.T, context.Context, *circuitBreakerTest),
) {
	t.Run(name, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 4*testutils.DefaultSucceedsSoonDuration)
		defer cancel()
		tc := setupCircuitBreakerTest(t)
		defer tc.Stopper().Stop(ctx)
		f(t, ctx, tc)
	})
}

func setupCircuitBreakerTest(t *testing.T) *circuitBreakerTest {
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

	tc.AddVotersOrFatal(t, k, tc.Target(1))

	var repls []replWithKnob
	for i := range tc.Servers {
		repl := tc.GetFirstStoreFromServer(t, i).LookupReplica(keys.MustAddr(k))
		enableProbe := makeBreakerToggleable(repl.Breaker())
		repls = append(repls, replWithKnob{repl, enableProbe})
	}
	return &circuitBreakerTest{
		TestCluster: tc,
		ManualClock: manualClock,
		repls:       repls,
		slowThresh:  slowThresh,
	}
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
		// the circuit breaker.
		if err != nil && !errors.Is(err, circuit.ErrBreakerOpen) {
			t.Fatalf("saw unexpected error %+v", err)
		}
		return err
	})
}

func (cbt *circuitBreakerTest) ExpireAllLeases(t *testing.T, pauseHeartbeats bool) (undo func()) {
	var maxWT int64
	var fs []func()
	for _, srv := range cbt.Servers {
		lv := srv.NodeLiveness().(*liveness.NodeLiveness)
		if pauseHeartbeats {
			undo := lv.PauseAllHeartbeatsForTest()
			fs = append(fs, undo)
		}
		self, ok := lv.Self()
		require.True(t, ok)
		if maxWT < self.Expiration.WallTime {
			maxWT = self.Expiration.WallTime
		}
	}
	cbt.ManualClock.Forward(maxWT + 1)
	return func() {
		for _, f := range fs {
			f()
		}
	}
}

func (*circuitBreakerTest) sendViaRepl(repl *kvserver.Replica, req roachpb.Request) error {
	var ba roachpb.BatchRequest
	ba.RangeID = repl.Desc().RangeID
	ba.Timestamp = repl.Clock().Now()
	ba.Add(req)
	ctx, cancel := context.WithTimeout(context.Background(), testutils.DefaultSucceedsSoonDuration)
	defer cancel()
	_, pErr := repl.Send(ctx, ba)
	// If our context got canceled, return an opaque error regardless of presence or
	// absence of actual error. This makes sure we don't accidentally pass tests as
	// a result of our context cancellation.
	if err := ctx.Err(); err != nil {
		pErr = roachpb.NewErrorf("timed out waiting for batch response: %v", pErr)
	}
	return pErr.GoError()
}

func (*circuitBreakerTest) sendViaDistSender(ds *kvcoord.DistSender, req roachpb.Request) error {
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
	require.True(t, errors.Is(err, circuit.ErrBreakerOpen), "%+v", err)
}

func (*circuitBreakerTest) RequireIsNotLeaseholderError(t *testing.T, err error) {
	ok := errors.HasType(err, (*roachpb.NotLeaseHolderError)(nil))
	require.True(t, ok, "%+v", err)
}

func (cbt *circuitBreakerTest) Write(idx int) error {
	return cbt.writeViaRepl(cbt.repls[idx].Replica)
}

func (cbt *circuitBreakerTest) WriteDS(idx int) error {
	put := roachpb.NewPut(cbt.repls[idx].Desc().StartKey.AsRawKey(), roachpb.MakeValueFromString("hello"))
	return cbt.sendViaDistSender(cbt.Servers[idx].DistSender(), put)
}

// SetSlowThreshold sets the SlowReplicationThreshold for requests sent through the
// test harness (i.e. via Write) to the provided duration. The zero value restores
// the default.
func (cbt *circuitBreakerTest) SetSlowThreshold(dur time.Duration) {
	cbt.slowThresh.Store(dur)
}

func (cbt *circuitBreakerTest) Read(idx int) error {
	return cbt.readViaRepl(cbt.repls[idx].Replica)
}

func (cbt *circuitBreakerTest) writeViaRepl(repl *kvserver.Replica) error {
	put := roachpb.NewPut(repl.Desc().StartKey.AsRawKey(), roachpb.MakeValueFromString("hello"))
	return cbt.sendViaRepl(repl, put)
}

func (cbt *circuitBreakerTest) readViaRepl(repl *kvserver.Replica) error {
	get := roachpb.NewGet(repl.Desc().StartKey.AsRawKey(), false /* forUpdate */)
	return cbt.sendViaRepl(repl, get)
}
