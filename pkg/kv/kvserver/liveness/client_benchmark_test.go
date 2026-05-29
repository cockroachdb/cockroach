// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package liveness_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils/regionlatency"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

// benchRunCounter is used to generate unique keyspace prefixes across
// benchmark runs. Each invocation of the benchmark function gets a unique
// prefix so that Create calls don't collide with keys from prior runs.
var benchRunCounter atomic.Int64

// benchNodeState tracks the latest liveness record for a simulated node,
// protected by a mutex to allow concurrent heartbeat writers.
type benchNodeState struct {
	mu    syncutil.Mutex
	rec   liveness.Record
	store liveness.Storage
}

// BenchmarkLivenessContention measures heartbeat write latency under concurrent
// full-range scan load on a multi-node cluster with injected inter-node
// latency. It uses a synthetic keyspace via NewTestKVStorage so that N
// simulated nodes can write heartbeats against the cluster without requiring N
// actual nodes.
//
// Each simulated node's heartbeat writes and scans are distributed across the
// cluster's server DB handles, so that most operations cross the network (with
// injected latency).
//
// The benchmark reports ns/op for a single heartbeat write. The "scans/sec"
// custom metric shows the achieved scan throughput during the benchmark.
func BenchmarkLivenessContention(b *testing.B) {
	skip.UnderShort(b)
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	ctx := context.Background()

	const numServers = 3

	// Set up latency injection knobs for each server.
	latencyEnabled := &atomic.Bool{}
	addrMaps := make([]regionlatency.AddrMap, numServers)
	perServerArgs := make(map[int]base.TestServerArgs, numServers)
	for i := 0; i < numServers; i++ {
		addrMaps[i] = regionlatency.MakeAddrMap()
		perServerArgs[i] = base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					ContextTestingKnobs: rpc.ContextTestingKnobs{
						InjectedLatencyOracle:  addrMaps[i],
						InjectedLatencyEnabled: latencyEnabled.Load,
					},
				},
			},
		}
	}

	tc := testcluster.StartTestCluster(b, numServers, base.TestClusterArgs{
		ServerArgsPerNode: perServerArgs,
	})
	defer tc.Stopper().Stop(ctx)

	// Collect server addresses and DB handles.
	serverAddrs := make([]string, numServers)
	dbs := make([]*kv.DB, numServers)
	for i := 0; i < numServers; i++ {
		serverAddrs[i] = tc.Server(i).AdvRPCAddr()
		dbs[i] = tc.Server(i).DB()
	}

	// setLatency updates the injected one-way latency between all server
	// pairs and enables (or disables for zero) latency injection.
	setLatency := func(oneWay time.Duration) {
		for i := 0; i < numServers; i++ {
			for j := 0; j < numServers; j++ {
				if i != j {
					addrMaps[i].SetLatency(serverAddrs[j], oneWay)
				}
			}
		}
		latencyEnabled.Store(oneWay > 0)
	}

	for _, oneWayLatency := range []time.Duration{
		0,
		2 * time.Millisecond,  // cross-AZ
		10 * time.Millisecond, // cross-region (nearby)
	} {
		setLatency(oneWayLatency)
		for _, numNodes := range []int{10, 100} {
			for _, numScanners := range []int{0, 1, 5} {
				name := fmt.Sprintf("latency=%s/nodes=%d/scanners=%d",
					oneWayLatency, numNodes, numScanners)
				b.Run(name, func(b *testing.B) {
					benchmarkLivenessContention(ctx, b, dbs, numNodes, numScanners)
				})
			}
		}
	}
}

func benchmarkLivenessContention(
	ctx context.Context, b *testing.B, dbs []*kv.DB, numNodes int, numScanners int,
) {
	numServers := len(dbs)

	// Each sub-benchmark invocation gets its own keyspace to avoid
	// interference between the framework's calibration runs and count repeats.
	run := benchRunCounter.Add(1)
	prefix := roachpb.Key(fmt.Sprintf("/test-liveness/%d/%d/%d/",
		numNodes, numScanners, run))

	// Use the first server's DB to create all records. The keyspace will
	// land on a range whose leaseholder is on some node; operations from
	// other nodes' DBs will cross the network.
	setupStore := liveness.NewTestKVStorage(dbs[0], prefix)
	for i := 1; i <= numNodes; i++ {
		require.NoError(b, setupStore.Create(ctx, roachpb.NodeID(i)))
	}

	// Read back all records to get the raw bytes needed for CPut.
	records, err := setupStore.Scan(ctx)
	require.NoError(b, err)
	require.Len(b, records, numNodes)

	// Assign each simulated node a store backed by a different server's DB
	// (round-robin), so heartbeat writes are distributed across servers.
	nodes := make([]benchNodeState, numNodes+1) // 1-indexed
	for _, rec := range records {
		serverIdx := int(rec.NodeID) % numServers
		nodes[rec.NodeID] = benchNodeState{
			rec:   rec,
			store: liveness.NewTestKVStorage(dbs[serverIdx], prefix),
		}
	}

	// Prime each node with epoch=1, simulating its first heartbeat.
	for i := 1; i <= numNodes; i++ {
		nodeID := roachpb.NodeID(i)
		old := nodes[i].rec
		nl := old.Liveness
		nl.Epoch = 1
		nl.Expiration = hlc.LegacyTimestamp{
			WallTime: time.Now().Add(time.Minute).UnixNano(),
		}
		update := liveness.MakeTestLivenessUpdate(nl, old)
		written, err := nodes[i].store.Update(ctx, update, func(actual liveness.Record) error {
			return fmt.Errorf("unexpected condition failure for n%d", nodeID)
		})
		require.NoError(b, err)
		nodes[i].rec = written
	}

	// Create scanner stores distributed across servers.
	scanStores := make([]liveness.Storage, numScanners)
	for i := 0; i < numScanners; i++ {
		serverIdx := i % numServers
		scanStores[i] = liveness.NewTestKVStorage(dbs[serverIdx], prefix)
	}

	// Start background scanners that continuously scan the full keyspace.
	var scanCount atomic.Int64
	scanCtx, scanCancel := context.WithCancel(ctx)
	var scanWg sync.WaitGroup
	for i := 0; i < numScanners; i++ {
		scanWg.Add(1)
		store := scanStores[i]
		go func() {
			defer scanWg.Done()
			for scanCtx.Err() == nil {
				_, _ = store.Scan(scanCtx)
				scanCount.Add(1)
			}
		}()
	}

	// Benchmark: b.N counts individual heartbeat writes.
	// Each iteration picks a node round-robin and writes a heartbeat.
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		n := (i % numNodes) + 1
		doHeartbeat(ctx, &nodes[n])
	}
	elapsed := time.Since(start)
	b.StopTimer()

	scanCancel()
	scanWg.Wait()

	scans := scanCount.Load()
	if numScanners > 0 && elapsed > 0 {
		b.ReportMetric(float64(scans)/elapsed.Seconds(), "scans/sec")
	}
}

// doHeartbeat simulates a single heartbeat write for a node, updating the
// expiration. On contention it retries once with the fresh record.
func doHeartbeat(ctx context.Context, ns *benchNodeState) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	old := ns.rec
	nl := old.Liveness
	nl.Expiration = hlc.LegacyTimestamp{
		WallTime: time.Now().Add(time.Minute).UnixNano(),
	}
	update := liveness.MakeTestLivenessUpdate(nl, old)
	written, err := ns.store.Update(ctx, update, func(actual liveness.Record) error {
		ns.rec = actual
		return fmt.Errorf("contention on n%d", nl.NodeID)
	})
	if err != nil {
		// Retry once with the fresh record from the condition failure.
		old = ns.rec
		nl = old.Liveness
		nl.Expiration = hlc.LegacyTimestamp{
			WallTime: time.Now().Add(time.Minute).UnixNano(),
		}
		update = liveness.MakeTestLivenessUpdate(nl, old)
		written, err = ns.store.Update(ctx, update, func(actual liveness.Record) error {
			ns.rec = actual
			return fmt.Errorf("repeated contention on n%d", nl.NodeID)
		})
		if err != nil {
			return
		}
	}
	ns.rec = written
}
