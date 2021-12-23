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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestReplicaCircuitBreaker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := setupCircuitBreakerTest(t)
	defer tc.Stopper().Stop(ctx)

	const (
		n1 = 0
		n2 = 1
	)

	runCircuitBreakerTest(t, "breaker-ok", func(t *testing.T, ctx context.Context, tc *circuitBreakerTest) {
		// Circuit breaker doesn't get in the way of anything unless
		// something trips it.
		require.NoError(t, tc.Write(n1))
		tc.RequireIsNotLeaseholderError(t, tc.Write(n2))
		require.NoError(t, tc.Read(n1))
		tc.RequireIsNotLeaseholderError(t, tc.Read(n2))
	})

	runCircuitBreakerTest(t, "leaseholder-tripped", func(t *testing.T, ctx context.Context, tc *circuitBreakerTest) {
		// Get lease on n1.
		require.NoError(t, tc.Write(n1))
		// Disable the probe so that when the breaker trips, it stays tripped.
		tc.SetProbeEnabled(n1, false)
		tc.Report(n1, errors.New("boom"))

		// n1, despite the tripped probe, can still serve reads as long as they
		// are valid under the lease. But writes fail fast.
		require.NoError(t, tc.Read(n1))
		tc.RequireIsBreakerOpen(t, tc.Write(n1))

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

	runCircuitBreakerTest(t, "leaseless-tripped", func(t *testing.T, ctx context.Context, tc *circuitBreakerTest) {
		// Put the lease on n1 but then trip the breaker with the probe
		// disabled.
		require.NoError(t, tc.Write(n1))
		tc.SetProbeEnabled(n1, false)
		tc.Report(n1, errors.New("boom"))
		resumeHeartbeats := tc.PauseHeartbeatsAndExpireAllLeases(t)

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
		// we're not careful, the probe itself is held up by the breaker as well.
		// Since the probe leads to a lease acquisition and the lease acquisition is
		// fairly disjoint from the request that triggered it, there is custom code
		// to make this work.
		resumeHeartbeats()
		tc.SetProbeEnabled(n1, true)
		tc.UntripsSoon(t, tc.Read, n1)
		tc.UntripsSoon(t, tc.Write, n1)
		tc.RequireIsNotLeaseholderError(t, tc.Read(n2))
		tc.RequireIsNotLeaseholderError(t, tc.Write(n2))
	})

	runCircuitBreakerTest(t, "leaseholder-unavailable", func(t *testing.T, ctx context.Context, tc *circuitBreakerTest) {
		// Get lease on n1.
		require.NoError(t, tc.Write(n1))
		tc.SetSlowThreshold(time.Second)
		tc.StopServer(n2) // lose quorum
		tc.RequireIsBreakerOpen(t, tc.Write(n1))
		tc.RequireIsBreakerOpen(t, tc.Read(n1)) // not true
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
	var rangeID int64 // atomic
	slowThresh := &atomic.Value{}
	slowThresh.Store(time.Duration(0))
	storeKnobs := &kvserver.StoreTestingKnobs{
		SlowReplicationThresholdOverride: func(ba *roachpb.BatchRequest) time.Duration {
			if rid := roachpb.RangeID(atomic.LoadInt64(&rangeID)); rid == 0 || ba.RangeID != rid {
				return 0
			}
			return slowThresh.Load().(time.Duration)
		},
	}
	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					ClockSource: manualClock.UnixNano,
				},
				Store: storeKnobs,
			},
		},
	}
	tc := testcluster.StartTestCluster(t, 2, args)

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
	testutils.SucceedsSoon(t, func() error {
		err := method(idx)
		// All errors coming out should be annotated as coming from
		// the circuit breaker.
		if err != nil && !errors.Is(err, circuit.ErrBreakerOpen) {
			t.Errorf("saw unexpected error %+v", err)
		}
		return err
	})
}

func (cbt *circuitBreakerTest) PauseHeartbeatsAndExpireAllLeases(t *testing.T) (undo func()) {
	var maxWT int64
	var fs []func()
	for _, srv := range cbt.Servers {
		lv := srv.NodeLiveness().(*liveness.NodeLiveness)
		undo := lv.PauseAllHeartbeatsForTest()
		fs = append(fs, undo)
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

func (*circuitBreakerTest) sendBatchRequest(repl *kvserver.Replica, req roachpb.Request) error {
	var ba roachpb.BatchRequest
	ba.RangeID = repl.Desc().RangeID
	ba.Timestamp = repl.Clock().Now()
	ba.Add(req)
	ctx, cancel := context.WithTimeout(context.Background(), testutils.DefaultSucceedsSoonDuration)
	defer cancel()
	_, pErr := repl.Send(ctx, ba)
	if err := ctx.Err(); err != nil {
		return errors.Wrap(pErr.GoError(), "timed out waiting for batch response")
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

func (cbt *circuitBreakerTest) SetSlowThreshold(dur time.Duration) {
	cbt.slowThresh.Store(dur)
}

func (cbt *circuitBreakerTest) Read(idx int) error {
	return cbt.readViaRepl(cbt.repls[idx].Replica)
}

func (cbt *circuitBreakerTest) writeViaRepl(repl *kvserver.Replica) error {
	put := roachpb.NewPut(repl.Desc().StartKey.AsRawKey(), roachpb.MakeValueFromString("hello"))
	return cbt.sendBatchRequest(repl, put)
}

func (cbt *circuitBreakerTest) readViaRepl(repl *kvserver.Replica) error {
	get := roachpb.NewGet(repl.Desc().StartKey.AsRawKey(), false /* forUpdate */)
	return cbt.sendBatchRequest(repl, get)
}
