// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/obs/ash/enrichment"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

// enrichTestEnv sets up a Sampler with sampling and enrichment
// enabled, registers a per-tenant enrichment cache, and returns
// cleanup helpers. The local node ID is fixed at 1.
type enrichTestEnv struct {
	sampler  *Sampler
	stopper  *stop.Stopper
	cache    *enrichment.Cache
	tenantID roachpb.TenantID
}

func newEnrichTestEnv(t *testing.T) *enrichTestEnv {
	t.Helper()
	ctx := context.Background()

	s, stopper := newTestSampler()
	enabled.Store(true)
	enrichment.Enabled.Override(ctx, &s.st.SV, true)
	enrichment.CacheLimit.Override(ctx, &s.st.SV, 1024)

	metrics := enrichment.NewMetrics()
	cache := enrichment.NewCache(s.st, &metrics)
	cache.Start(ctx, stopper)

	tenantID := roachpb.MustMakeTenantID(7)
	enrichment.Register(tenantID, cache)

	t.Cleanup(func() {
		enrichment.Unregister(tenantID)
		stopper.Stop(ctx)
		enabled.Store(false)
		s.resolver.Store(nil)
		s.enrichment.resolver.Store(nil)
	})

	return &enrichTestEnv{sampler: s, stopper: stopper, cache: cache, tenantID: tenantID}
}

// makeEnrichmentID returns a clusterunique.ID whose embedded
// instance ID is gatewayID. Useful for steering samples toward
// specific gateway nodes in tests.
func makeEnrichmentID(gatewayID int32, seed int64) clusterunique.ID {
	return clusterunique.GenerateID(
		hlc.Timestamp{WallTime: seed + 1}, base.SQLInstanceID(gatewayID))
}

// TestSamplerEnrichmentLocalHit verifies that a sample whose
// EnrichmentID hits the locally registered tenant cache is
// published with the cached attributes — no RPC is needed and no
// retry queue entry is created.
func TestSamplerEnrichmentLocalHit(t *testing.T) {
	env := newEnrichTestEnv(t)

	// Seed the local enrichment cache (gateway is this node).
	id := makeEnrichmentID(1, 100)
	env.cache.PutExecution(id, enrichment.Attributes{
		AppName:  "myapp",
		Database: "mydb",
		User:     "alice",
	})
	env.cache.DrainWriteBuffer()
	require.Eventually(t, func() bool {
		_, ok := env.cache.GetExecution(id)
		return ok
	}, time.Second, 10*time.Millisecond)

	cleanup := SetWorkState(env.tenantID, WorkloadInfo{
		WorkloadID:   42,
		EnrichmentID: id,
	}, WorkCPU, "Optimize")
	defer cleanup()

	env.sampler.takeSample(context.Background())

	samples := env.sampler.GetSamples(nil)
	require.Len(t, samples, 1)
	require.Equal(t, "myapp", samples[0].AppName)
	require.Equal(t, "mydb", samples[0].Database)
	require.Equal(t, "alice", samples[0].User)
	require.Empty(t, env.sampler.enrichment.retryQueue, "no retry expected on local hit")
}

// TestSamplerEnrichmentRemoteHit verifies that a sample whose
// gateway is a remote node triggers the EnrichmentResolverFn,
// applies the returned attributes, and publishes the sample.
func TestSamplerEnrichmentRemoteHit(t *testing.T) {
	env := newEnrichTestEnv(t)

	const remoteNodeID int32 = 10
	id := makeEnrichmentID(remoteNodeID, 200)

	var resolverCalls atomic.Int32
	env.sampler.SetEnrichmentResolver(func(
		ctx context.Context, nodeID roachpb.NodeID, ids []clusterunique.ID,
	) (map[clusterunique.ID]enrichment.Attributes, error) {
		resolverCalls.Add(1)
		require.Equal(t, roachpb.NodeID(remoteNodeID), nodeID)
		require.Contains(t, ids, id)
		return map[clusterunique.ID]enrichment.Attributes{
			id: {AppName: "remote-app", User: "bob"},
		}, nil
	})

	cleanup := SetWorkState(env.tenantID, WorkloadInfo{
		WorkloadID:   42,
		EnrichmentID: id,
	}, WorkCPU, "RemoteResolve")
	defer cleanup()

	env.sampler.takeSample(context.Background())

	require.Equal(t, int32(1), resolverCalls.Load())
	samples := env.sampler.GetSamples(nil)
	require.Len(t, samples, 1)
	require.Equal(t, "remote-app", samples[0].AppName)
	require.Equal(t, "bob", samples[0].User)
	require.Empty(t, env.sampler.enrichment.retryQueue)
}

// TestSamplerEnrichmentRPCFailureRequeues verifies that an RPC
// error causes the sample to be placed on the retry queue and not
// published this tick.
func TestSamplerEnrichmentRPCFailureRequeues(t *testing.T) {
	env := newEnrichTestEnv(t)

	id := makeEnrichmentID(10, 300)

	env.sampler.SetEnrichmentResolver(func(
		ctx context.Context, nodeID roachpb.NodeID, ids []clusterunique.ID,
	) (map[clusterunique.ID]enrichment.Attributes, error) {
		return nil, errors.New("simulated rpc failure")
	})

	cleanup := SetWorkState(env.tenantID, WorkloadInfo{
		WorkloadID:   42,
		EnrichmentID: id,
	}, WorkCPU, "RPCFail")
	defer cleanup()

	env.sampler.takeSample(context.Background())

	require.Empty(t, env.sampler.GetSamples(nil), "no samples published when RPC fails")
	require.Len(t, env.sampler.enrichment.retryQueue, 1)
}

// TestSamplerEnrichmentRetrySucceedsNextTick verifies that a
// sample that failed RPC on tick T is retried on tick T+1 and
// published when the RPC succeeds. The per-node backoff is reset
// for this test so the second tick can issue a fresh RPC.
func TestSamplerEnrichmentRetrySucceedsNextTick(t *testing.T) {
	env := newEnrichTestEnv(t)

	id := makeEnrichmentID(10, 400)

	var tickCount atomic.Int32
	env.sampler.SetEnrichmentResolver(func(
		ctx context.Context, nodeID roachpb.NodeID, ids []clusterunique.ID,
	) (map[clusterunique.ID]enrichment.Attributes, error) {
		if tickCount.Add(1) == 1 {
			return nil, errors.New("first tick fails")
		}
		return map[clusterunique.ID]enrichment.Attributes{
			id: {AppName: "second-tick-app"},
		}, nil
	})

	cleanup := SetWorkState(env.tenantID, WorkloadInfo{
		WorkloadID:   42,
		EnrichmentID: id,
	}, WorkCPU, "RetrySucceed")
	defer cleanup()

	// Tick 1: RPC fails, sample is requeued.
	env.sampler.takeSample(context.Background())
	require.Empty(t, env.sampler.GetSamples(nil))
	require.Len(t, env.sampler.enrichment.retryQueue, 1)

	// Reset per-node backoff so the next tick will attempt the RPC
	// again immediately rather than honoring the exponential delay.
	env.sampler.enrichment.backoff = map[roachpb.NodeID]*nodeBackoff{}

	// Remove the WorkState so the retry queue is the only source on tick 2.
	cleanup()

	// Tick 2: RPC succeeds, retry-queued sample is published.
	env.sampler.takeSample(context.Background())
	samples := env.sampler.GetSamples(nil)
	require.Len(t, samples, 1)
	require.Equal(t, "second-tick-app", samples[0].AppName)
	require.Empty(t, env.sampler.enrichment.retryQueue)
}

// TestSamplerEnrichmentDropsAgedRetries verifies that retry queue
// entries older than EnrichmentRetryMaxAge are dropped at drain
// time, with the dropped-samples metric incremented and no sample
// published.
func TestSamplerEnrichmentDropsAgedRetries(t *testing.T) {
	env := newEnrichTestEnv(t)
	ctx := context.Background()

	// Tighten the drop window so the test doesn't need to wait
	// real-world seconds.
	EnrichmentRetryMaxAge.Override(ctx, &env.sampler.st.SV, 10*time.Millisecond)

	// Hand-seed a stale entry on the retry queue.
	old := time.Now().Add(-time.Second)
	env.sampler.enrichment.retryQueue = []retryEntry{{
		sample: ASHSample{
			SampleTime:   old,
			EnrichmentID: makeEnrichmentID(10, 500),
		},
		firstCapture: old,
	}}

	beforeDropped := env.sampler.metrics.EnrichmentSamplesDropped.Count()

	env.sampler.takeSample(ctx)

	require.Empty(t, env.sampler.enrichment.retryQueue, "aged retry should be dropped")
	require.Empty(t, env.sampler.GetSamples(nil))
	require.Equal(t,
		beforeDropped+1,
		env.sampler.metrics.EnrichmentSamplesDropped.Count(),
	)
}

// TestSamplerEnrichmentParallelPerNode verifies that two samples
// destined for different remote nodes both result in resolver
// invocations (one per node) and both samples are published when
// both RPCs succeed. Concurrency of the per-node RPCs is asserted
// via a barrier: each call blocks until both calls have arrived,
// so a strictly sequential resolver would deadlock and the test
// would time out.
func TestSamplerEnrichmentParallelPerNode(t *testing.T) {
	env := newEnrichTestEnv(t)

	idA := makeEnrichmentID(10, 600)
	idB := makeEnrichmentID(11, 601)

	// Register work states from separate goroutines because the
	// rangeWorkStates iterator returns the top-of-stack state for
	// each goroutine ID — both SetWorkState calls from a single
	// goroutine would collapse to one visible state.
	var registered sync.WaitGroup
	var done = make(chan struct{})
	registered.Add(2)
	for _, spec := range []struct {
		id   clusterunique.ID
		name string
	}{{idA, "ParallelA"}, {idB, "ParallelB"}} {
		spec := spec
		go func() {
			cleanup := SetWorkState(env.tenantID, WorkloadInfo{
				WorkloadID: 1, EnrichmentID: spec.id,
			}, WorkCPU, spec.name)
			defer cleanup()
			registered.Done()
			<-done
		}()
	}
	registered.Wait()
	defer close(done)

	var startedWG sync.WaitGroup
	startedWG.Add(2)
	env.sampler.SetEnrichmentResolver(func(
		ctx context.Context, nodeID roachpb.NodeID, ids []clusterunique.ID,
	) (map[clusterunique.ID]enrichment.Attributes, error) {
		startedWG.Done()
		startedWG.Wait() // deadlocks if calls aren't concurrent
		var appName string
		var id clusterunique.ID
		switch nodeID {
		case 10:
			appName, id = "app-A", idA
		case 11:
			appName, id = "app-B", idB
		}
		return map[clusterunique.ID]enrichment.Attributes{
			id: {AppName: appName},
		}, nil
	})

	env.sampler.takeSample(context.Background())

	samples := env.sampler.GetSamples(nil)
	require.Len(t, samples, 2)
	apps := map[string]bool{}
	for _, s := range samples {
		apps[s.AppName] = true
	}
	require.True(t, apps["app-A"], "expected app-A in samples")
	require.True(t, apps["app-B"], "expected app-B in samples")
}

// TestSamplerEnrichmentNodeBackoff verifies that two consecutive
// failures against the same gateway node trigger backoff on the
// third tick: no RPC is issued, and the samples destined for that
// node move directly to the retry queue.
func TestSamplerEnrichmentNodeBackoff(t *testing.T) {
	env := newEnrichTestEnv(t)
	ctx := context.Background()

	// Force the backoff cap to be very large so even one failure
	// sets a long nextAttempt; we want to observe the skip on the
	// very next tick.
	EnrichmentNodeBackoffBase.Override(ctx, &env.sampler.st.SV, time.Hour)

	id := makeEnrichmentID(10, 700)

	var calls atomic.Int32
	env.sampler.SetEnrichmentResolver(func(
		ctx context.Context, nodeID roachpb.NodeID, ids []clusterunique.ID,
	) (map[clusterunique.ID]enrichment.Attributes, error) {
		calls.Add(1)
		return nil, errors.New("always fails")
	})

	cleanup := SetWorkState(env.tenantID, WorkloadInfo{
		WorkloadID: 42, EnrichmentID: id,
	}, WorkCPU, "Backoff")
	defer cleanup()

	// Tick 1: RPC issued and fails. Backoff installed for n10.
	env.sampler.takeSample(ctx)
	require.Equal(t, int32(1), calls.Load())
	require.Len(t, env.sampler.enrichment.retryQueue, 1)

	beforeSkipped := env.sampler.metrics.EnrichmentRPCSkipped.Count()

	// Tick 2: gateway is in backoff window — sample passes straight
	// through to the retry queue without an RPC.
	env.sampler.takeSample(ctx)
	require.Equal(t, int32(1), calls.Load(), "no new RPC during backoff")
	require.GreaterOrEqual(t,
		env.sampler.metrics.EnrichmentRPCSkipped.Count(),
		beforeSkipped+1,
	)
}

// TestSamplerEnrichmentSkippedPreFinalization verifies that the
// sampler does not issue GetASHEnrichmentData RPCs until the
// cluster version is active. Samples destined for remote
// enrichment land on the retry queue (no RPC, no metric
// increment) so a mixed-version cluster doesn't generate
// "unknown method" failures against pre-26.3 nodes.
func TestSamplerEnrichmentSkippedPreFinalization(t *testing.T) {
	ctx := context.Background()

	// Build a settings instance pinned to the version just before
	// V26_3_ASHEnrichment. MakeTestingClusterSettings can't be
	// downgraded after creation, so use the WithVersions form to
	// initialize directly at the pre-gate version.
	preGate := clusterversion.PreviousRelease.Version()
	st := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.Latest.Version(), preGate, false /* initializeVersion */)
	require.NoError(t,
		clusterversion.Initialize(ctx, preGate, &st.SV))

	stopper := stop.NewStopper()
	t.Cleanup(func() { stopper.Stop(ctx) })
	s := newSampler(1, st, stopper)
	enabled.Store(true)
	t.Cleanup(func() { enabled.Store(false) })
	enrichment.Enabled.Override(ctx, &st.SV, true)
	enrichment.CacheLimit.Override(ctx, &st.SV, 1024)

	tenantID := roachpb.MustMakeTenantID(7)
	metrics := enrichment.NewMetrics()
	cache := enrichment.NewCache(st, &metrics)
	cache.Start(ctx, stopper)
	enrichment.Register(tenantID, cache)
	t.Cleanup(func() { enrichment.Unregister(tenantID) })

	var calls atomic.Int32
	s.SetEnrichmentResolver(func(
		ctx context.Context, nodeID roachpb.NodeID, ids []clusterunique.ID,
	) (map[clusterunique.ID]enrichment.Attributes, error) {
		calls.Add(1)
		return nil, nil
	})

	cleanup := SetWorkState(tenantID, WorkloadInfo{
		WorkloadID:   42,
		EnrichmentID: makeEnrichmentID(10, 800),
	}, WorkCPU, "PreFinal")
	defer cleanup()

	s.takeSample(ctx)

	require.Equal(t, int32(0), calls.Load(), "no RPC before version finalization")
	require.Len(t, s.enrichment.retryQueue, 1)
	require.Empty(t, s.GetSamples(nil))
}

// TestSamplerEnrichmentZeroIDPassesThrough verifies that samples
// whose WorkState has no EnrichmentID are published as-is without
// going through enrichment.
func TestSamplerEnrichmentZeroIDPassesThrough(t *testing.T) {
	env := newEnrichTestEnv(t)

	cleanup := SetWorkState(env.tenantID, WorkloadInfo{
		WorkloadID: 42,
		// EnrichmentID unset (zero value).
	}, WorkCPU, "NoEnrichment")
	defer cleanup()

	env.sampler.takeSample(context.Background())

	samples := env.sampler.GetSamples(nil)
	require.Len(t, samples, 1)
	require.Empty(t, samples[0].User)
	require.Empty(t, env.sampler.enrichment.retryQueue)
}
