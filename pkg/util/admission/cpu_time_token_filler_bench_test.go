// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// BenchmarkAllocateTokens benchmarks the allocateTokens method which is called
// every 1ms in the hot path.
func BenchmarkAllocateTokens(b *testing.B) {
	granter, allocator := setupBenchAllocator()
	ctx := context.Background()
	allocator.resetInterval(ctx)

	b.ResetTimer()
	i := 0
	for b.Loop() {
		// Simulate varying remaining ticks as would happen in practice.
		remainingTicks := int64(1000 - (i % 1000))
		if remainingTicks == 0 {
			remainingTicks = 1
		}
		allocator.allocateTokens(remainingTicks)

		// Reset interval periodically to avoid token accumulation issues.
		if i%1000 == 999 {
			// Drain tokens to simulate real usage.
			granter.mu.Lock()
			granter.mu.tokensUsed = 5_000_000_000
			granter.mu.Unlock()
			allocator.resetInterval(ctx)
		}

		i++
	}
}

// BenchmarkResetInterval benchmarks the resetInterval method which is called
// every second. This includes the model's fit computation.
func BenchmarkResetInterval(b *testing.B) {
	granter, allocator := setupBenchAllocator()
	ctx := context.Background()
	allocator.resetInterval(ctx)

	b.ResetTimer()
	for b.Loop() {
		// Simulate token usage.
		granter.mu.Lock()
		granter.mu.tokensUsed = 5_000_000_000
		granter.mu.Unlock()
		allocator.resetInterval(ctx)
	}
}

// BenchmarkFiller benchmarks the filler's tick method end-to-end with real
// allocator, model, and granter.
func BenchmarkFiller(b *testing.B) {
	granter, allocator, testTime := setupBenchFiller()
	ctx := context.Background()

	filler := &cpuTimeTokenFiller{
		allocator:  allocator,
		timeSource: testTime,
		closeCh:    make(chan struct{}),
	}

	// Initialize filler state (normally done by start()).
	allocator.resetInterval(ctx)
	filler.intervalStart = testTime.Now()
	filler.lastRemainingTicks = int64(time.Second / timePerTick)

	b.ResetTimer()
	i := 0
	for b.Loop() {
		// Advance time by 1ms per tick.
		testTime.Advance(timePerTick)
		tickTime := testTime.Now()

		// Every 1000 ticks (1 second), simulate token consumption.
		if i%1000 == 999 {
			granter.mu.Lock()
			granter.mu.tokensUsed = 5_000_000_000
			granter.mu.Unlock()
		}

		filler.tick(ctx, tickTime)
		i++
	}
}

func setupBenchFiller() (*cpuTimeTokenGranter, *cpuTimeTokenAllocator, *timeutil.ManualTime) {
	granter := &cpuTimeTokenGranter{}
	tier0Granter := &cpuTimeTokenChildGranter{
		tier:   testTier0,
		parent: granter,
	}
	tier1Granter := &cpuTimeTokenChildGranter{
		tier:   testTier1,
		parent: granter,
	}
	granter.requester[testTier0] = &testRequester{
		additionalID: "tier0",
		granter:      tier0Granter,
	}
	granter.requester[testTier1] = &testRequester{
		additionalID: "tier1",
		granter:      tier1Granter,
	}

	testTime := timeutil.NewManualTime(time.Now())
	cpuProvider := &benchCPUMetricsProvider{
		capacity:           8.0,
		totalCPUTimeMillis: 0,
		deltaCPUTimeMillis: 5000,
	}

	model := &cpuTimeTokenLinearModel{
		timeSource:         testTime,
		granter:            granter,
		cpuMetricsProvider: cpuProvider,
	}

	allocator := &cpuTimeTokenAllocator{
		granter:  granter,
		settings: cluster.MakeClusterSettings(),
		model:    model,
	}

	return granter, allocator, testTime
}

func setupBenchAllocator() (*cpuTimeTokenGranter, *cpuTimeTokenAllocator) {
	granter := &cpuTimeTokenGranter{}
	tier0Granter := &cpuTimeTokenChildGranter{
		tier:   testTier0,
		parent: granter,
	}
	tier1Granter := &cpuTimeTokenChildGranter{
		tier:   testTier1,
		parent: granter,
	}
	granter.requester[testTier0] = &testRequester{
		additionalID: "tier0",
		granter:      tier0Granter,
	}
	granter.requester[testTier1] = &testRequester{
		additionalID: "tier1",
		granter:      tier1Granter,
	}

	testTime := timeutil.NewManualTime(time.Now())
	cpuProvider := &benchCPUMetricsProvider{
		capacity:           8.0,
		totalCPUTimeMillis: 0,
		deltaCPUTimeMillis: 5000,
	}

	model := &cpuTimeTokenLinearModel{
		timeSource:         testTime,
		granter:            granter,
		cpuMetricsProvider: cpuProvider,
	}

	allocator := &cpuTimeTokenAllocator{
		granter:  granter,
		settings: cluster.MakeClusterSettings(),
		model:    model,
	}

	return granter, allocator
}

// BenchmarkGetKVWorkQueue benchmarks the GetKVWorkQueue method which is called
// on every KV request in AdmitKVWork.
func BenchmarkGetKVWorkQueue(b *testing.B) {
	_, _, coords := setupBenchCoordinators()

	b.ResetTimer()
	for b.Loop() {
		_ = coords.GetKVWorkQueue(false /* isSystemTenant */)
	}
}

func setupBenchCoordinators() (*cpuTimeTokenGranter, *cpuTimeTokenAllocator, *CPUGrantCoordinators) {
	granter, allocator, testTime := setupBenchFiller()

	// Create a minimal slotsCoord with a KVWork queue.
	slotsGranter := &cpuTimeTokenGranter{}
	slotsChildGranter := &cpuTimeTokenChildGranter{
		tier:   testTier0,
		parent: slotsGranter,
	}
	slotsGranter.requester[testTier0] = &testRequester{
		additionalID: "slots",
		granter:      slotsChildGranter,
	}

	slotsCoord := &GrantCoordinator{}
	opts := makeWorkQueueOptions(KVWork)
	slotsCoord.queues[KVWork] = makeWorkQueue(
		log.MakeTestingAmbientCtxWithNewTracer(),
		KVWork,
		slotsChildGranter,
		cluster.MakeClusterSettings(),
		makeWorkQueueMetrics("kv", nil, admissionpb.NormalPri, admissionpb.LockingNormalPri),
		opts,
	)

	cpuTimeCoord := &cpuTimeTokenGrantCoordinator{
		filler: &cpuTimeTokenFiller{
			allocator:  allocator,
			timeSource: testTime,
			closeCh:    make(chan struct{}),
		},
	}

	coords := &CPUGrantCoordinators{
		st:           cluster.MakeClusterSettings(),
		slotsCoord:   slotsCoord,
		cpuTimeCoord: cpuTimeCoord,
	}

	return granter, allocator, coords
}

// benchCPUMetricsProvider is a simple CPU metrics provider for benchmarking.
type benchCPUMetricsProvider struct {
	capacity           float64
	totalCPUTimeMillis int64
	deltaCPUTimeMillis int64
}

func (p *benchCPUMetricsProvider) GetCPUUsage() (totalCPUTimeMillis int64, err error) {
	p.totalCPUTimeMillis += p.deltaCPUTimeMillis
	return p.totalCPUTimeMillis, nil
}

func (p *benchCPUMetricsProvider) GetCPUCapacity() (cpuCapacity float64) {
	return p.capacity
}
