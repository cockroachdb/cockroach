// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDistSenderReplicaStall tests that the DistSender will detect a stalled
// replica and redirect read requests when the lease moves elsewhere.
func TestDistSenderReplicaStall(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "clientTimeout", func(t *testing.T, clientTimeout bool) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// The lease won't move unless we use expiration-based leases. We also
		// speed up the test by reducing various intervals and timeouts.
		st := cluster.MakeTestingClusterSettings()
		kvserver.ExpirationLeasesOnly.Override(ctx, &st.SV, true)
		kvcoord.CircuitBreakerEnabled.Override(ctx, &st.SV, true)
		kvcoord.CircuitBreakerCancellation.Override(ctx, &st.SV, true)
		kvcoord.CircuitBreakerProbeThreshold.Override(ctx, &st.SV, time.Second)
		kvcoord.CircuitBreakerProbeInterval.Override(ctx, &st.SV, time.Second)
		kvcoord.CircuitBreakerProbeTimeout.Override(ctx, &st.SV, time.Second)

		tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Settings: st,
				RaftConfig: base.RaftConfig{
					RaftTickInterval:           200 * time.Millisecond,
					RaftHeartbeatIntervalTicks: 1,
					RaftElectionTimeoutTicks:   3,
					RangeLeaseDuration:         time.Second,
				},
			},
		})
		defer tc.Stopper().Stop(ctx)

		db := tc.Server(0).ApplicationLayer().DB()

		// Create a range and upreplicate it.
		key := tc.ScratchRange(t)
		desc := tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)
		t.Logf("created %s", desc)

		// Move the lease to n3, and make sure everyone has applied it by
		// replicating a write.
		tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(2))

		_, err := db.Inc(ctx, key, 1)
		require.NoError(t, err)
		tc.WaitForValues(t, key, []int64{1, 1, 1})
		t.Logf("moved lease to n3")

		// Deadlock n3.
		repl3, err := tc.GetFirstStoreFromServer(t, 2).GetReplica(desc.RangeID)
		require.NoError(t, err)
		mu := repl3.GetMutexForTesting()
		mu.Lock()
		defer mu.Unlock()
		t.Logf("deadlocked n3")

		// Perform a read from n1, which will stall. Eventually, the lease will be
		// picked up by a different node. The DistSender should detect this and retry
		// the read there.
		//
		// We test both with and without client-side timeouts and retries.
		if clientTimeout {
			for {
				require.NoError(t, ctx.Err())

				t.Logf("sending Get request")
				ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				kv, err := db.Get(ctx, key)
				cancel()
				t.Logf("Get returned %s err=%v", kv, err)
				if err == nil {
					break
				}
			}
		} else {
			t.Logf("sending Get request")
			kv, err := db.Get(ctx, key)
			require.NoError(t, err)
			t.Logf("Get returned %s", kv)
		}
	})
}

// BenchmarkDistSenderCircuitBreakersForReplica benchmarks the cost of
// constructing new circuit breakers. In particular, the memory cost of each.
func BenchmarkDistSenderCircuitBreakersForReplica(b *testing.B) {
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	ambientCtx := log.MakeTestingAmbientCtxWithNewTracer()
	st := cluster.MakeTestingClusterSettings()
	kvcoord.CircuitBreakerEnabled.Override(ctx, &st.SV, true)

	cbs := kvcoord.NewDistSenderCircuitBreakers(
		ambientCtx, stopper, st, nil, kvcoord.MakeDistSenderMetrics())

	replDesc := &roachpb.ReplicaDescriptor{
		NodeID:    1,
		StoreID:   1,
		ReplicaID: 1,
		Type:      roachpb.VOTER_FULL,
	}
	rangeDesc := &roachpb.RangeDescriptor{
		RangeID:          0, // populated in loop below
		StartKey:         roachpb.RKey(keys.MinKey),
		EndKey:           roachpb.RKey(keys.MaxKey),
		InternalReplicas: []roachpb.ReplicaDescriptor{*replDesc},
		NextReplicaID:    2,
		Generation:       1,
	}

	var cb *kvcoord.ReplicaCircuitBreaker
	for i := 0; i < b.N; i++ {
		rangeDesc.RangeID++
		cb = cbs.ForReplica(rangeDesc, replDesc)
	}

	_ = cb
}

// BenchmarkDistSenderCircuitBreakersTrack benchmarks the cost of tracking a
// request with the circuit breaker in various scenarios. This is on the hot
// path of every request.
func BenchmarkDistSenderCircuitBreakersTrack(b *testing.B) {
	b.Run("disabled", func(b *testing.B) {
		benchmarkCircuitBreakersTrack(
			b, false /* enable */, false /* cancel */, false /* single */, "" /* errType */, 1 /* conc */)
	})

	for _, cancel := range []bool{false, true} {
		for _, alone := range []bool{false, true} {
			for _, errType := range []string{"nil", "send", "response", "ctx"} {
				for _, conc := range []int{1, 16, 64, 1024} {
					// Tests with concurrency already run multiple requests, so skip these cases.
					if !alone && conc > 1 {
						continue
					}
					b.Run(fmt.Sprintf("cancel=%t/alone=%t/err=%s/conc=%d", cancel, alone, errType, conc),
						func(b *testing.B) {
							enable := true
							benchmarkCircuitBreakersTrack(b, enable, cancel, alone, errType, conc)
						})
				}
			}
		}
	}
}

func benchmarkCircuitBreakersTrack(
	b *testing.B, enable, cancel, alone bool, errType string, conc int,
) {
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	ambientCtx := log.MakeTestingAmbientCtxWithNewTracer()
	st := cluster.MakeTestingClusterSettings()
	kvcoord.CircuitBreakerEnabled.Override(ctx, &st.SV, enable)
	kvcoord.CircuitBreakerCancellation.Override(ctx, &st.SV, cancel)

	cbs := kvcoord.NewDistSenderCircuitBreakers(
		ambientCtx, stopper, st, nil, kvcoord.MakeDistSenderMetrics())

	replDesc := &roachpb.ReplicaDescriptor{
		NodeID:    1,
		StoreID:   1,
		ReplicaID: 1,
		Type:      roachpb.VOTER_FULL,
	}
	rangeDesc := &roachpb.RangeDescriptor{
		RangeID:          1,
		StartKey:         roachpb.RKey(keys.MinKey),
		EndKey:           roachpb.RKey(keys.MaxKey),
		InternalReplicas: []roachpb.ReplicaDescriptor{*replDesc},
		NextReplicaID:    2,
		Generation:       1,
	}

	ba := &kvpb.BatchRequest{}
	ba.Add(&kvpb.GetRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: roachpb.Key("foo"),
		},
	})

	br := &kvpb.BatchResponse{}
	br.Add(&kvpb.GetResponse{})

	// If this shouldn't be the only request, add another concurrent request to
	// the tracking.
	if !alone {
		_, _, err := cbs.ForReplica(rangeDesc, replDesc).Track(ctx, ba, false /* withCommit */, 1)
		require.NoError(b, err)
	}

	// Set up the error responses.
	sendCtx := ctx
	var sendErr error
	switch errType {
	case "", "nil":
	case "send":
		sendErr = errors.New("send failed")
		br = nil
	case "response":
		*br = kvpb.BatchResponse{
			BatchResponse_Header: kvpb.BatchResponse_Header{
				Error: kvpb.NewError(errors.New("request failed")),
			},
		}
	case "ctx":
		var cancel func()
		sendCtx, cancel = context.WithCancel(ctx)
		cancel()
		sendErr = sendCtx.Err()
		br = nil
	default:
		b.Fatalf("invalid errType %q", errType)
	}

	// Setting nowNanos to 1 basically means that we'll never launch a probe.
	nowNanos := int64(1)

	var wg sync.WaitGroup
	wg.Add(conc)

	b.ResetTimer()

	// Spawn workers.
	for n := 0; n < conc; n++ {
		go func() {
			defer wg.Done()

			// Adjust b.N for concurrency.
			for i := 0; i < b.N/conc; i++ {
				cb := cbs.ForReplica(rangeDesc, replDesc)
				_, cbToken, err := cb.Track(sendCtx, ba, false /* withCommit */, nowNanos)
				if err != nil {
					assert.NoError(b, err)
					return
				}
				_ = cbToken.Done(br, sendErr, nowNanos) // ignore cancellation error
			}
		}()
	}

	wg.Wait()
}
