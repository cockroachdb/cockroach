// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestStartupFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.IgnoreLint(t, "this test should be used to reproduce failures from random tests")

	// Ranges that would trigger startup failure if retry is disabled/removed:
	// /System/NodeLivenessMax (keys.BootstrapVersionKey)
	// /Table/11 (keys.SystemSQLCodec.TablePrefix(11))
	// can be used for testing purposes.

	// Set faultyKey to suspected range that is causing startup failure, comment
	// out skip statement above and run the test.
	faultyKey := keys.BootstrapVersionKey
	runCircuitBreakerTestForKey(t,
		func(spans []roachpb.Span) (good, bad []roachpb.Key) {
			for _, span := range spans {
				if span.ContainsKey(faultyKey) {
					bad = append(bad, span.Key)
				} else {
					good = append(good, span.Key)
				}
			}
			return good, bad
		})
}

// TestStartupFailureRandomRange picks one random range and runs the test on it.
// If this test failed, then suspicious range start key deduced from error could
// be used in TestStartupFailure to investigate code path not covered by startup
// retries.
func TestStartupFailureRandomRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderStressRace(t, "5 nodes with replication is too slow for stress race")

	rng, seed := randutil.NewTestRand()
	t.Log("TestStartupFailureRandomRange using seed", seed)
	runCircuitBreakerTestForKey(t,
		func(spans []roachpb.Span) (good, bad []roachpb.Key) {
			badRange := rng.Intn(len(spans))
			t.Log("triggering loss of quorum on range", spans[badRange].Key.String())
			bad = append(bad, spans[badRange].Key)
			good = make([]roachpb.Key, len(spans)-1)
			i := 0
			for ; i < badRange; i++ {
				good[i] = spans[i].Key
			}
			i++
			for ; i < len(spans); i++ {
				good[i-1] = spans[i].Key
			}
			return good, bad
		})
}

// runCircuitBreakerTestForKey causes ranges identified by passed selector
// function to lose quorum on node stop. It then triggers circuit breaker by
// probing those ranges. After all ranges are probed, it restarts last killed
// node to restore quorum and checks that startup succeeds.
// Node lifecycle:
// 1, 2, 3 - live
// 4, 5    - stopped
// 6       - stopped and restarted
func runCircuitBreakerTestForKey(
	t *testing.T, faultyRangeSelector func([]roachpb.Span) (good, bad []roachpb.Key),
) {
	const (
		nodes = 6
	)

	ctx := context.Background()

	lReg := testutils.NewListenerRegistry()
	defer lReg.Close()
	reg := server.NewStickyInMemEnginesRegistry()
	defer reg.CloseAllStickyInMemEngines()
	args := base.TestClusterArgs{
		ServerArgsPerNode: make(map[int]base.TestServerArgs),
		ReusableListeners: true,
	}
	var enableFaults atomic.Bool
	for i := 0; i < nodes; i++ {
		a := base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyEngineRegistry: reg,
				},
				SpanConfig: &spanconfig.TestingKnobs{
					ConfigureScratchRange: true,
				},
			},
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:               true,
					StickyInMemoryEngineID: strconv.FormatInt(int64(i), 10),
				},
			},
			Listener: lReg.GetOrFail(t, i),
		}
		args.ServerArgsPerNode[i] = a
	}
	tc := testcluster.NewTestCluster(t, nodes, args)
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)

	c := tc.ServerConn(0)

	dumpRanges := func() {
		fmt.Println("Dumping ranges")
		r, err := c.QueryContext(ctx,
			"select range_id, start_pretty, replicas from crdb_internal.ranges_no_leases")
		require.NoError(t, err, "failed to query ranges")
		for r.Next() {
			var rangeID int
			var startKey string
			var replicas []int32
			require.NoError(t, r.Scan(&rangeID, &startKey, pq.Array(&replicas)),
				"failed to scan range info")
			fmt.Printf("Range %d, %s, replicas %d\n", rangeID, startKey, replicas)
		}
		_ = r.Close()
	}
	// Wait for replication factor to propagate maybe.
	t.Log("waiting for replication factor to propagate")
	<-time.After(10 * time.Second)

	require.NoError(t, tc.WaitForFullReplication(), "up-replication failed")

	// TODO(oleg): remove me after figuring out how to wait for zone config
	// propagation.
	dumpRanges()

	tc.ToggleReplicateQueues(false)
	_, err := c.ExecContext(ctx, "set cluster setting kv.allocator.load_based_rebalancing='off'")
	require.NoError(t, err, "failed to disable load rebalancer")

	// replicaTargets holds template for replica placements for available and
	// unavailable range configurations.
	type replicaTargets struct {
		safe, unsafe []roachpb.StoreID
	}

	t3repl := replicaTargets{
		safe:   []roachpb.StoreID{1, 2, 3},
		unsafe: []roachpb.StoreID{1, 4, 6},
	}
	t5repl := replicaTargets{
		safe:   []roachpb.StoreID{1, 2, 3, 4, 5},
		unsafe: []roachpb.StoreID{1, 2, 4, 5, 6},
	}
	db := tc.Server(0).DB()
	prepRange := func(rk roachpb.Key, fail bool) roachpb.RKey {
		d := tc.LookupRangeOrFatal(t, rk)
		replicaTemplate := t3repl
		if len(d.InternalReplicas) > 3 {
			replicaTemplate = t5repl
		}
		targets := replicaTemplate.safe
		if fail {
			targets = replicaTemplate.unsafe
		}
		var voters []roachpb.ReplicationTarget
		for _, storeID := range targets {
			voters = append(voters, roachpb.ReplicationTarget{
				NodeID:  roachpb.NodeID(storeID),
				StoreID: storeID,
			})
		}
		for {
			t.Log("relocating range", d.RangeID, voters)
			err := db.AdminRelocateRange(ctx, rk, voters, nil, true)
			if err == nil {
				break
			}
		}
		return d.StartKey
	}

	var rangeSpans []roachpb.Span
	r, err := c.QueryContext(ctx, "select range_id, start_key, end_key from crdb_internal.ranges_no_leases order by start_key")
	require.NoError(t, err, "failed to query ranges")
	for r.Next() {
		var rangeID int
		var key roachpb.Key
		var endKey roachpb.Key
		require.NoError(t, r.Scan(&rangeID, &key, &endKey), "failed to scan range data from query")
		rangeSpans = append(rangeSpans, roachpb.Span{
			Key:    key,
			EndKey: endKey,
		})
	}
	good, bad := faultyRangeSelector(rangeSpans)
	for _, startKey := range good {
		prepRange(startKey, false)
	}
	var ranges []string
	for _, startKey := range bad {
		prepRange(startKey, true)
		ranges = append(ranges, startKey.String())
	}
	rangesList := fmt.Sprintf("[%s]", strings.Join(ranges, ", "))

	// Remove nodes permanently to only leave quorum on planned ranges.
	tc.StopServer(3)
	tc.StopServer(4)

	// Stop node with replicas that would leave ranges without quorum.
	tc.StopServer(5)

	// Probe compromised ranges to trigger circuit breakers on them. If we don't
	// do this, then restart queries will wait for quorum to be reestablished with
	// restarting node without failing.
	var wg sync.WaitGroup
	wg.Add(len(bad))
	for _, startKey := range bad {
		go func(key roachpb.Key) {
			defer wg.Done()
			_ = db.Put(context.Background(), keys.RangeProbeKey(roachpb.RKey(key)), "")
		}(startKey)
	}
	wg.Wait()

	// Restart node and check that it succeeds in reestablishing range quorum
	// necessary for startup actions.
	lReg.ReopenOrFail(t, 5)
	err = tc.RestartServer(5)
	require.NoError(t, err, "restarting server with range(s) %s tripping circuit breaker", rangesList)

	// Disable faults to make it easier for cluster to stop.
	enableFaults.Store(false)
}

func TestStartupInjectedFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const (
		nodes     = 3
		faultyIdx = 2
		failProb  = 0.1
	)
	ctx := context.Background()

	rng, seed := randutil.NewLockedTestRand()
	t.Log("TestStartupInjectedFailure random seed", seed)

	lReg := testutils.NewListenerRegistry()
	defer lReg.Close()
	reg := server.NewStickyInMemEnginesRegistry()
	defer reg.CloseAllStickyInMemEngines()

	args := base.TestClusterArgs{
		ServerArgsPerNode: make(map[int]base.TestServerArgs),
		ReusableListeners: true,
	}
	rangeWhitelist := map[roachpb.RangeID]bool{
		//48: false,
	}
	_ = rangeWhitelist
	var enableFaults atomic.Bool
	for i := 0; i < nodes; i++ {
		a := base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyEngineRegistry: reg,
				},
				SpanConfig: &spanconfig.TestingKnobs{
					ConfigureScratchRange: true,
				},
			},
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:               true,
					StickyInMemoryEngineID: strconv.FormatInt(int64(i), 10),
				},
			},
			Listener: lReg.GetOrFail(t, i),
		}
		if i != faultyIdx {
			anyID := i + 1
			a.Knobs.Store = &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(ctx context.Context, br *kvpb.BatchRequest,
				) *kvpb.Error {
					if enableFaults.Load() && br.GatewayNodeID == faultyIdx+1 && !rangeWhitelist[br.RangeID] {
						if rng.Float32() < failProb {
							t.Log("injecting fault into range ", br.RangeID)
							return kvpb.NewError(kvpb.NewReplicaUnavailableError(errors.New("injected error"),
								&roachpb.RangeDescriptor{RangeID: br.RangeID}, roachpb.ReplicaDescriptor{
									NodeID:  roachpb.NodeID(anyID),
									StoreID: roachpb.StoreID(anyID),
								}))
							//return kvpb.NewError(kvpb.NewAmbiguousResultErrorf("something happens"))
							//return roachpb.NewError(errors.Newf("simulated failure when accessing r%d", br.RangeID))
						}
					}
					return nil
				},
			}
		}
		args.ServerArgsPerNode[i] = a
	}
	tc := testcluster.NewTestCluster(t, nodes, args)
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)

	require.NoError(t, tc.WaitForFullReplication(), "up-replication failed")
	tc.StopServer(faultyIdx)

	lReg.ReopenOrFail(t, faultyIdx)
	enableFaults.Store(true)
	err := tc.RestartServer(faultyIdx)
	require.NoError(t, err, "restarting server")
	enableFaults.Store(false)
}
