// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDistSenderReplicaStall tests that the DistSender will detect a stalled
// replica and redirect read requests when the lease moves elsewhere.
func TestDistSenderReplicaStall(t *testing.T) {
	defer leaktest.AfterTest(t)()
	scope := log.Scope(t)
	defer scope.Close(t)

	// See https://github.com/cockroachdb/cockroach/issues/140957.
	// If this test fails again, we should investigate the issue further, as
	// it could indicate a failure of the DistSender to circuit break.
	{
		skip.UnderDuress(t)
		defer testutils.StartExecTrace(t, scope.GetDirectory()).Finish(t)
	}

	testutils.RunTrueAndFalse(t, "clientTimeout", func(t *testing.T, clientTimeout bool) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// The lease won't move unless we use expiration-based leases. We also
		// speed up the test by reducing various intervals and timeouts.
		st := cluster.MakeTestingClusterSettings()
		kvserver.OverrideDefaultLeaseType(ctx, &st.SV, roachpb.LeaseExpiration)
		kvcoord.CircuitBreakersMode.Override(
			ctx, &st.SV, kvcoord.DistSenderCircuitBreakersAllRanges,
		)
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
		t.Log("moved lease to n3")

		// Deadlock n3.
		repl3, err := tc.GetFirstStoreFromServer(t, 2).GetReplica(desc.RangeID)
		require.NoError(t, err)
		mu := repl3.GetMutexForTesting()
		mu.Lock()
		defer mu.Unlock()
		t.Log("deadlocked n3")

		// Perform a read from n1, which will stall. Eventually, the lease will be
		// picked up by a different node. The DistSender should detect this and retry
		// the read there.
		//
		// We test both with and without client-side timeouts and retries.
		if clientTimeout {
			for {
				require.NoError(t, ctx.Err())

				t.Log("sending Get request")
				ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				kv, err := db.Get(ctx, key)
				cancel()
				t.Logf("Get returned %s err=%v", kv, err)
				if err == nil {
					break
				}
			}
		} else {
			t.Log("sending Get request")
			kv, err := db.Get(ctx, key)
			require.NoError(t, err)
			t.Logf("Get returned %s", kv)
		}
	})
}

// TestDistSenderCircuitBreakerModes validates the behavior of DistSender after
// a range stall caused by a locked mutex. It tests two different ranges
// (liveness and regular) and all three different circuit breaker modes.
func TestDistSenderCircuitBreakerModes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "scratchRange", func(t *testing.T, scratchRange bool) {
		testutils.RunValues(
			t,
			"mode",
			[]kvcoord.DistSenderCircuitBreakersMode{
				kvcoord.DistSenderCircuitBreakersNoRanges,
				kvcoord.DistSenderCircuitBreakersLivenessRangeOnly,
				kvcoord.DistSenderCircuitBreakersAllRanges,
			},
			func(t *testing.T, mode kvcoord.DistSenderCircuitBreakersMode) {
				ctx := context.Background()

				// The lease won't move unless we use expiration-based leases. This is
				// because a deadlocked range with expiration-based leases would
				// eventually cause the lease to expire. However, with epoch-based or
				// leader leases, the lease wouldn't expire due to a deadlocked range
				// since lease extensions are happening at a different layer.
				// We also speed up the test by reducing various intervals and timeouts.
				st := cluster.MakeTestingClusterSettings()
				kvserver.OverrideDefaultLeaseType(ctx, &st.SV, roachpb.LeaseExpiration)
				kvcoord.CircuitBreakersMode.Override(ctx, &st.SV, mode)
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

				key := tc.ScratchRange(t)
				desc := tc.AddVotersOrFatal(t, keys.NodeLivenessPrefix, tc.Targets(1, 2)...)
				tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(2))
				// Ensure the n1 DistSender cache is up-to-date to force the
				// first scan after a partition to attempt scanning from the
				// deadlocked store.
				_, err := tc.Server(0).NodeLiveness().(*liveness.NodeLiveness).ScanNodeVitalityFromKV(ctx)
				require.NoError(t, err)
				// Upreplicate the liveness range and put on n3.
				if scratchRange {
					// We only use the scratch range for the non-liveness mode.
					desc = tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)
					t.Logf("created %s", desc)
					// Move the lease to n3, and make sure everyone has applied it by
					// replicating a write.
					tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(2))

					_, err := db.Inc(ctx, key, 1)
					require.NoError(t, err)
					// Ensure the n1 DistSender cache is up-to-date.
					tc.WaitForValues(t, key, []int64{1, 1, 1})
					t.Log("moved lease to n3")
				}

				// Deadlock either liveness or the scratch range.
				repl, err := tc.GetFirstStoreFromServer(t, 2).GetReplica(desc.RangeID)
				require.NoError(t, err)
				mu := repl.GetMutexForTesting()
				mu.Lock()
				defer mu.Unlock()
				t.Logf("deadlocked range on n3 - %v", desc)

				if scratchRange {
					// Perform a read from n1, which will stall. Eventually, the lease will be
					// picked up by a different node. The DistSender should detect this and retry
					// the read there.
					t.Log("sending Get request to blocked range")
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					_, err := db.Get(ctx, key)
					cancel()
					if mode == kvcoord.DistSenderCircuitBreakersAllRanges {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
					}
				} else {
					// Scan the liveness range to ensure the circuit breaker is working.
					scanError := false
					// Check that we get the correct value directly from KV as
					// well as from gossip. The leases may end up on either n1
					// or n2. The SucceedsSoon looping is necessary for the
					// IsLive check below. After the liveness lease has moved,
					// we need to wait until all the other nodes have seen the
					// new leaseholder and updated their liveness records
					// against it.
					testutils.SucceedsSoon(t, func() error {
						// We allow a long timeout here to prevent failures in race or
						// stress builds. Unfortunately this means the NoRanges circuit
						// breaker test takes longer to complete.
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()
						nl, err := tc.Server(0).NodeLiveness().(*liveness.NodeLiveness).ScanNodeVitalityFromKV(ctx)
						if err != nil {
							t.Logf("attempt to scan node vitality resulted in %v", err)
							scanError = true
							return nil
						}
						// We wait until the new leaseholder has seen the node heartbeat
						// to validate we don't block any internal threads either.
						if !nl[1].IsLive(livenesspb.DistSender) {
							return errors.Errorf("expected n1 to be live")
						}
						return nil
					})

					// In NoRanges mode, we always expect to see a timeout or other error.
					if mode == kvcoord.DistSenderCircuitBreakersNoRanges {
						require.True(t, scanError)
					} else {
						// If the circuit breaker on the liveness range is enabled,
						// we don't expect to ever see timeout or other errors as it
						// should retry in DistSender and not get stuck.
						require.False(t, scanError)
						// Also verify liveness updates propagate by gossip. This
						// validates that a new leader is established and broadcasts the
						// liveness information.
						testutils.SucceedsSoon(t, func() error {
							t.Log("read liveness from gossip on n2")
							node2Cache := tc.Server(1).NodeLiveness().(*liveness.NodeLiveness).GetNodeVitalityFromCache(1)
							if !node2Cache.IsLive(livenesspb.DistSender) {
								return errors.Errorf("expected n1 to be live")
							}
							return nil
						})
					}
				}
			})
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
	kvcoord.CircuitBreakersMode.Override(
		ctx, &st.SV, kvcoord.DistSenderCircuitBreakersAllRanges,
	)

	cbs := kvcoord.NewDistSenderCircuitBreakers(
		ambientCtx, stopper, st, nil, kvcoord.MakeDistSenderMetrics(roachpb.Locality{}))

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
	b *testing.B, enable bool, cancel bool, alone bool, errType string, conc int,
) {
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	ambientCtx := log.MakeTestingAmbientCtxWithNewTracer()
	st := cluster.MakeTestingClusterSettings()
	if enable {
		kvcoord.CircuitBreakersMode.Override(ctx, &st.SV, kvcoord.DistSenderCircuitBreakersAllRanges)
	} else {
		kvcoord.CircuitBreakersMode.Override(ctx, &st.SV, kvcoord.DistSenderCircuitBreakersNoRanges)
	}
	kvcoord.CircuitBreakerCancellation.Override(ctx, &st.SV, cancel)

	cbs := kvcoord.NewDistSenderCircuitBreakers(
		ambientCtx, stopper, st, nil, kvcoord.MakeDistSenderMetrics(roachpb.Locality{}))

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
	now := crtime.Mono(1)

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
				_, cbToken, err := cb.Track(sendCtx, ba, false /* withCommit */, now)
				if err != nil {
					assert.NoError(b, err)
					return
				}
				_ = cbToken.Done(br, sendErr, now) // ignore cancellation error
			}
		}()
	}

	wg.Wait()
}
