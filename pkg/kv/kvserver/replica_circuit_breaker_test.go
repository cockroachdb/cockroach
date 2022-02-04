// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
)

func TestReplicaUnavailableError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var repls roachpb.ReplicaSet
	repls.AddReplica(roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 10, ReplicaID: 100})
	repls.AddReplica(roachpb.ReplicaDescriptor{NodeID: 2, StoreID: 20, ReplicaID: 200})
	desc := roachpb.NewRangeDescriptor(10, roachpb.RKey("a"), roachpb.RKey("z"), repls)
	var ba roachpb.BatchRequest
	ba.Add(&roachpb.RequestLeaseRequest{})
	lm := liveness.IsLiveMap{
		1: liveness.IsLiveMapEntry{IsLive: true},
	}
	rs := raft.Status{}
	err := replicaUnavailableError(desc, desc.Replicas().AsProto()[0], lm, &rs)
	echotest.Require(t, string(redact.Sprint(err)), testutils.TestDataPath(t, "replica_unavailable_error.txt"))
}

type circuitBreakerReplicaMock struct {
	clock *hlc.Clock
}

func (c *circuitBreakerReplicaMock) Clock() *hlc.Clock {
	return c.clock
}

func (c *circuitBreakerReplicaMock) Desc() *roachpb.RangeDescriptor {
	return &roachpb.RangeDescriptor{}
}

func (c *circuitBreakerReplicaMock) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	return ba.CreateReply(), nil
}

func (c *circuitBreakerReplicaMock) slowReplicationThreshold(
	ba *roachpb.BatchRequest,
) (time.Duration, bool) {
	return 0, false
}

func (c *circuitBreakerReplicaMock) replicaUnavailableError() error {
	return errors.New("unavailable")
}

// This test verifies that when the breaker trips and untrips again,
// there is no scenario under which the request's cancel leaks.
func TestReplicaCircuitBreaker_NoCancelRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	br, stopper := setupCircuitBreakerTest(t, "mutexmap-1")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer stopper.Stop(ctx)

	g := ctxgroup.WithContext(ctx)
	const count = 100
	for i := 0; i < count; i++ {
		i := i // for goroutine
		g.GoCtx(func(ctx context.Context) error {
			ctx, cancel := context.WithCancel(ctx)
			tok, sig, err := br.Register(ctx, cancel)
			if err != nil {
				_ = err // ignoring intentionally
				return nil
			}
			if i == count/2 {
				br.TripAsync() // probe will succeed
			}
			runtime.Gosched()
			time.Sleep(time.Duration(rand.Intn(int(time.Millisecond))))
			var pErr *roachpb.Error
			if i%2 == 0 {
				pErr = roachpb.NewErrorf("boom")
			}
			_ = br.UnregisterAndAdjustError(tok, sig, pErr)
			return nil
		})
	}
	require.NoError(t, g.Wait())
	var n int
	br.cancels.Visit(func(ctx context.Context, _ func()) (remove bool) {
		n++
		return false // keep
	})
	require.Zero(t, n, "found tracked requests")
}

func TestReplicaCircuitBreaker_Register(t *testing.T) {
	defer leaktest.AfterTest(t)()
	br, stopper := setupCircuitBreakerTest(t, "mutexmap-1")
	defer stopper.Stop(context.Background())
	ctx := withCircuitBreakerProbeMarker(context.Background())
	tok, sig, err := br.Register(ctx, func() {})
	require.NoError(t, err)
	defer br.UnregisterAndAdjustError(tok, sig, nil /* pErr */)
	require.Zero(t, sig.C())
	var n int
	br.cancels.Visit(func(ctx context.Context, f func()) (remove bool) {
		n++
		return false // keep
	})
	require.Zero(t, n, "probe context got added to CancelStorage")
}

func setupCircuitBreakerTest(t testing.TB, cs string) (*replicaCircuitBreaker, *stop.Stopper) {
	st := cluster.MakeTestingClusterSettings()
	// Enable circuit breakers.
	replicaCircuitBreakerSlowReplicationThreshold.Override(context.Background(), &st.SV, time.Hour)
	r := &circuitBreakerReplicaMock{clock: hlc.NewClock(hlc.UnixNano, 500*time.Millisecond)}
	var numShards int
	{
		_, err := fmt.Sscanf(cs, "mutexmap-%d", &numShards)
		require.NoError(t, err)
	}
	s := &MapCancelStorage{NumShards: numShards}
	onTrip := func() {}
	onReset := func() {}
	stopper := stop.NewStopper()
	br := newReplicaCircuitBreaker(st, stopper, log.AmbientContext{}, r, s, onTrip, onReset)
	return br, stopper
}

func BenchmarkReplicaCircuitBreaker_Register(b *testing.B) {
	defer leaktest.AfterTest(b)()

	for _, enabled := range []bool{false, true} {
		b.Run("enabled="+strconv.FormatBool(enabled), func(b *testing.B) {
			dss := []string{
				"mutexmap-1", "mutexmap-2", "mutexmap-4", "mutexmap-8", "mutexmap-12", "mutexmap-16",
				"mutexmap-20", "mutexmap-24", "mutexmap-32", "mutexmap-64",
			}
			if !enabled {
				dss = dss[:1]
			}
			for _, ds := range dss {
				b.Run(ds, func(b *testing.B) {
					b.ReportAllocs()
					br, stopper := setupCircuitBreakerTest(b, ds)
					defer stopper.Stop(context.Background())

					var dur time.Duration
					if enabled {
						dur = time.Hour
					}
					replicaCircuitBreakerSlowReplicationThreshold.Override(context.Background(), &br.st.SV, dur)
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							ctx, cancel := context.WithCancel(context.Background())
							tok, sig, err := br.Register(ctx, cancel)
							if err != nil {
								b.Error(err)
							}
							if pErr := br.UnregisterAndAdjustError(tok, sig, nil); pErr != nil {
								b.Error(pErr)
							}
							cancel()
						}
					})
				})
			}
		})
	}
}
