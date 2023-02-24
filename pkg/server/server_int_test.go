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
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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

	const (
		nodes = 5
	)
	ctx := context.Background()
	rng, seed := randutil.NewTestRand()
	_ = rng
	t.Log("test seed", seed)

	lReg := testutils.NewListenerRegistry()
	reg := server.NewStickyInMemEnginesRegistry()
	args := base.TestClusterArgs{
		ServerArgsPerNode: make(map[int]base.TestServerArgs),
		ReusableListeners: true,
	}
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
	defer func() {
		fmt.Println("Closing listener sockets")
		lReg.Close()
	}()
	// This doesn't always work very well with testing start/stop issues as it could panic.
	defer func() {
		fmt.Println("Closing in mem engines")
		reg.CloseAllStickyInMemEngines()
	}()
	defer func() {
		fmt.Println("Stopping stopper")
		tc.Stopper().Stop(ctx)
	}()
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

	fmt.Println("Waiting upreplication")
	require.NoError(t, tc.WaitForFullReplication(), "up-replication failed")

	dumpRanges()

	_, err := c.ExecContext(ctx, "set cluster setting kv.allocator.load_based_rebalancing='off'")
	require.NoError(t, err, "failed to disable load rebalancer")

	// Move everything to safe location (nothing should be on node 4[index 3])
	//	r, err := c.QueryContext(ctx, "select range_id, replicas from crdb_internal.ranges_no_leases where array_position(replicas, $1) > 0", 4)

	// Move dangerous range to rejoining node.

	// Make system ranges fragile.
	fmt.Printf("Stopping nodes 4, 5\n")
	tc.StopServer(3)
	tc.StopServer(4)

	// Compromise system range.
	fmt.Printf("Stopping node 3\n")
	tc.StopServer(2)

	// Let circuit breakers fire.
	fmt.Printf("Waiting for circuit breakes to fire\n")
	<-time.After(80 * time.Second)

	fmt.Printf("Attempting restart\n")
	lReg.ReopenOrFail(t, 2)
	err = tc.RestartServer(2)
	require.NoError(t, err, "restarting server")

	fmt.Printf("Trying to query liveness of restarted node\n")
	db := tc.Server(0).DB()
	var liveness livenesspb.Liveness
	require.NoError(t, db.GetProto(ctx, keys.NodeLivenessKey(roachpb.NodeID(3)), &liveness),
		"failed to get liveness status")
	fmt.Printf("Node status: %s\n", liveness)
}

func TestStartupInjectedFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const (
		nodes     = 3
		faultyIdx = 2
		failProb  = 0
	)
	ctx := context.Background()

	rng, seed := randutil.NewTestRand()
	_ = rng
	t.Log("test seed", seed)

	lReg := testutils.NewListenerRegistry()
	reg := server.NewStickyInMemEnginesRegistry()
	args := base.TestClusterArgs{
		ServerArgsPerNode: make(map[int]base.TestServerArgs),
		ReusableListeners: true,
	}
	rangeWhitelist := map[roachpb.RangeID]bool{
		48: false,
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
			a.Knobs.Store = &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(ctx context.Context, br *kvpb.BatchRequest,
				) *kvpb.Error {
					if enableFaults.Load() && br.GatewayNodeID == faultyIdx+1 && !rangeWhitelist[br.RangeID] {
						if rng.Float32() < failProb {
							fmt.Printf("Injecting fault into r%d\n", br.RangeID)
							for _, r := range br.Requests {
								fmt.Printf(" %s\n", r.String())
							}
							return kvpb.NewError(kvpb.NewReplicaUnavailableError(errors.New("injected error"),
								&roachpb.RangeDescriptor{}, roachpb.ReplicaDescriptor{}))
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
	defer func() {
		fmt.Println("Closing listener sockets")
		lReg.Close()
	}()
	// This doesn't always work very well with testing start/stop issues as it could panic.
	defer func() {
		fmt.Println("Closing in mem engines")
		reg.CloseAllStickyInMemEngines()
	}()
	defer func() {
		fmt.Println("Stopping stopper")
		tc.Stopper().Stop(ctx)
	}()
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

	fmt.Println("Waiting upreplication")
	require.NoError(t, tc.WaitForFullReplication(), "up-replication failed")

	dumpRanges()

	_, err := c.ExecContext(ctx, "set cluster setting kv.allocator.load_based_rebalancing='off'")
	require.NoError(t, err, "failed to disable load rebalancer")

	// Move everything to safe location (nothing should be on node 4[index 3])
	//	r, err := c.QueryContext(ctx, "select range_id, replicas from crdb_internal.ranges_no_leases where array_position(replicas, $1) > 0", 4)

	// Compromise system range.
	fmt.Printf("Stopping node 3\n")
	tc.StopServer(2)

	fmt.Printf("Attempting restart\n")
	lReg.ReopenOrFail(t, 2)
	enableFaults.Store(true)
	err = tc.RestartServer(2)
	require.NoError(t, err, "restarting server")
	enableFaults.Store(false)

	fmt.Printf("Trying to query liveness of restarted node\n")
	db := tc.Server(0).DB()
	var liveness livenesspb.Liveness
	require.NoError(t, db.GetProto(ctx, keys.NodeLivenessKey(roachpb.NodeID(3)), &liveness),
		"failed to get liveness status")
	fmt.Printf("Node status: %s\n", liveness)
}
