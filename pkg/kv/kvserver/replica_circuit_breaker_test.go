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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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
	//TODO implement me
	panic("implement me")
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

func setupCircuitBreakerBench(b *testing.B) (*replicaCircuitBreaker, *stop.Stopper) {
	st := cluster.MakeTestingClusterSettings()
	// Enable circuit breakers.
	replicaCircuitBreakerSlowReplicationThreshold.Override(context.Background(), &st.SV, time.Hour)
	stopper := stop.NewStopper()
	r := &circuitBreakerReplicaMock{clock: hlc.NewClock(hlc.UnixNano, 500*time.Millisecond)}
	onTrip := func() {}
	onReset := func() {}
	br := newReplicaCircuitBreaker(st, stopper, log.AmbientContext{}, r, onTrip, onReset)
	return br, stopper
}

func BenchmarkReplicaCircuitBreaker_Register(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	b.Run("mk-cancel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				ctx, cancel := context.WithCancel(ctx)
				_ = ctx
				cancel()
			}
		})
	})

	b.Run("enabled", func(b *testing.B) {
		br, stopper := setupCircuitBreakerBench(b)
		defer stopper.Stop(ctx)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				ctx, cancel := context.WithCancel(ctx)
				tok, sig, err := br.Register(ctx, cancel)
				if err != nil {
					b.Error(err)
				}
				if pErr := br.Unregister(tok, sig, nil); pErr != nil {
					b.Error(pErr)
				}
				cancel()
			}
		})
	})

	b.Run("disabled", func(b *testing.B) {
		br, stopper := setupCircuitBreakerBench(b)
		defer stopper.Stop(ctx)
		b.RunParallel(func(pb *testing.PB) {
			replicaCircuitBreakerSlowReplicationThreshold.Override(ctx, &br.st.SV, 0) // disable
			for pb.Next() {
				ctx, cancel := context.WithCancel(ctx)
				tok, sig, err := br.Register(ctx, cancel)
				if err != nil {
					b.Error(err)
				}
				if pErr := br.Unregister(tok, sig, nil); pErr != nil {
					b.Error(pErr)
				}
				cancel()
			}
		})
	})
}
