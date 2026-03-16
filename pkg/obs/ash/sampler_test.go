// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/obs/workloadid"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// newTestSampler creates a Sampler suitable for unit tests.
func newTestSampler() (*Sampler, *stop.Stopper) {
	stopper := stop.NewStopper()
	st := cluster.MakeTestingClusterSettings()
	s := newSampler(1, st, stopper)
	return s, stopper
}

func TestSamplerTakeSample(t *testing.T) {
	s, stopper := newTestSampler()
	defer stopper.Stop(context.Background())

	enabled.Store(true)
	defer enabled.Store(false)

	tenantID := roachpb.MustMakeTenantID(5)
	cleanup := SetWorkState(tenantID, WorkloadInfo{WorkloadID: 42}, WorkCPU, "Optimize")
	defer cleanup()

	s.takeSample(context.Background())

	samples := s.GetSamples(nil)
	require.Len(t, samples, 1)

	sample := samples[0]
	require.Equal(t, roachpb.NodeID(1), sample.NodeID)
	require.Equal(t, tenantID, sample.TenantID)
	require.Equal(t, WorkCPU, sample.WorkEventType)
	require.Equal(t, "Optimize", sample.WorkEvent)
	require.NotZero(t, sample.GoroutineID)
	require.NotZero(t, sample.SampleTime)

	// WorkloadID should be the hex-encoded uint64.
	// Note(alyshan): This may change if we decide to move encoding
	// out of the sampler and perform it at read time instead.
	require.Equal(t, encodeStmtFingerprintIDToString(42), sample.WorkloadID)
}

func TestSamplerWorkloadIDCache(t *testing.T) {
	setup := func(t *testing.T) *Sampler {
		t.Helper()
		s, stopper := newTestSampler()
		t.Cleanup(func() { stopper.Stop(context.Background()) })
		enabled.Store(true)
		t.Cleanup(func() { enabled.Store(false) })
		return s
	}

	t.Run("cache hit", func(t *testing.T) {
		s := setup(t)
		tenantID := roachpb.MustMakeTenantID(3)

		// Take two samples with the same workload ID.
		cleanup := SetWorkState(tenantID, WorkloadInfo{WorkloadID: 99}, WorkIO, "BatchEval")
		s.takeSample(context.Background())
		cleanup()

		cleanup = SetWorkState(tenantID, WorkloadInfo{WorkloadID: 99}, WorkIO, "BatchEval")
		s.takeSample(context.Background())
		cleanup()

		samples := s.GetSamples(nil)
		require.Len(t, samples, 2)
		// Both samples should have the same encoded workload ID.
		require.Equal(t, samples[0].WorkloadID, samples[1].WorkloadID)
		require.Equal(t, encodeStmtFingerprintIDToString(99), samples[0].WorkloadID)

		// Verify the cache contains the entry (keyed by id + type).
		_, ok := s.workloadIDCache.Get(workloadCacheKey{id: 99})
		require.True(t, ok, "workload ID should be cached")
	})

	t.Run("different types do not collide", func(t *testing.T) {
		s := setup(t)
		tenantID := roachpb.MustMakeTenantID(3)
		const id uint64 = 42

		// Sample with the same numeric ID but as a statement fingerprint.
		cleanup := SetWorkState(tenantID, WorkloadInfo{
			WorkloadID:   id,
			WorkloadType: workloadid.WorkloadTypeStatement,
		}, WorkIO, "BatchEval")
		s.takeSample(context.Background())
		cleanup()

		// Sample with the same numeric ID but as a job.
		cleanup = SetWorkState(tenantID, WorkloadInfo{
			WorkloadID:   id,
			WorkloadType: workloadid.WorkloadTypeJob,
		}, WorkIO, "BatchEval")
		s.takeSample(context.Background())
		cleanup()

		samples := s.GetSamples(nil)
		require.Len(t, samples, 2)

		// The encoded workload IDs must differ: hex vs decimal.
		require.NotEqual(t, samples[0].WorkloadID, samples[1].WorkloadID,
			"same numeric ID with different types should produce different encodings")

		// Verify both cache entries exist independently.
		_, ok := s.workloadIDCache.Get(workloadCacheKey{
			id: id, typ: workloadid.WorkloadTypeStatement,
		})
		require.True(t, ok, "statement cache entry should exist")
		_, ok = s.workloadIDCache.Get(workloadCacheKey{
			id: id, typ: workloadid.WorkloadTypeJob,
		})
		require.True(t, ok, "job cache entry should exist")
	})
}

func TestEncodeWorkloadID(t *testing.T) {
	for _, tc := range []struct {
		name     string
		id       uint64
		typ      workloadid.WorkloadType
		expected string
	}{
		{
			name:     "statement fingerprint",
			id:       42,
			typ:      workloadid.WorkloadTypeStatement,
			expected: encodeStmtFingerprintIDToString(42),
		},
		{
			name:     "unknown type uses hex encoding",
			id:       42,
			typ:      workloadid.WorkloadTypeUnknown,
			expected: encodeStmtFingerprintIDToString(42),
		},
		{
			name:     "job ID uses decimal",
			id:       12345,
			typ:      workloadid.WorkloadTypeJob,
			expected: "12345",
		},
		{
			name:     "job ID zero",
			id:       0,
			typ:      workloadid.WorkloadTypeJob,
			expected: "0",
		},
		{
			name:     "system task LDR",
			id:       uint64(workloadid.WORKLOAD_ID_LDR),
			typ:      workloadid.WorkloadTypeSystem,
			expected: "LDR",
		},
		{
			name:     "system task unknown ID",
			id:       0,
			typ:      workloadid.WorkloadTypeSystem,
			expected: "UNKNOWN",
		},
		{
			name:     "zero ID with unknown type",
			id:       0,
			typ:      workloadid.WorkloadTypeUnknown,
			expected: encodeStmtFingerprintIDToString(0),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := encodeWorkloadID(tc.id, tc.typ)
			require.Equal(t, tc.expected, result,
				fmt.Sprintf("encodeWorkloadID(%d, %s)", tc.id, tc.typ))
		})
	}
}

func TestSamplerMultipleGoroutines(t *testing.T) {
	s, stopper := newTestSampler()
	defer stopper.Stop(context.Background())

	enabled.Store(true)
	defer enabled.Store(false)

	tenantID := roachpb.MustMakeTenantID(2)
	const numGoroutines = 5

	// Each goroutine registers its work state, then waits for a signal
	// to clean up. This ensures clearWorkState runs on the same
	// goroutine that called SetWorkState.
	ready := make(chan struct{}, numGoroutines)
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := range numGoroutines {
		go func(idx int) {
			defer wg.Done()
			cleanup := SetWorkState(
				tenantID,
				WorkloadInfo{WorkloadID: uint64(idx + 1)},
				WorkNetwork,
				"DistSenderRemote",
			)
			ready <- struct{}{}
			<-release
			cleanup()
		}(i)
	}

	// Wait for all goroutines to have registered their state.
	for range numGoroutines {
		<-ready
	}

	s.takeSample(context.Background())

	// Signal all goroutines to clean up.
	close(release)
	wg.Wait()

	samples := s.GetSamples(nil)
	require.Len(t, samples, numGoroutines)

	// Collect all goroutine IDs to verify they are distinct.
	gids := make(map[int64]struct{}, numGoroutines)
	for _, sample := range samples {
		require.Equal(t, tenantID, sample.TenantID)
		require.Equal(t, WorkNetwork, sample.WorkEventType)
		require.Equal(t, "DistSenderRemote", sample.WorkEvent)
		gids[sample.GoroutineID] = struct{}{}
	}
	require.Len(t, gids, numGoroutines, "each goroutine should have a unique ID")
}

func TestSamplerStartAndBackgroundSampling(t *testing.T) {
	s, stopper := newTestSampler()
	defer stopper.Stop(context.Background())

	ctx := context.Background()
	Enabled.Override(ctx, &s.st.SV, true)
	SampleInterval.Override(ctx, &s.st.SV, 10*time.Millisecond)

	// start() must be called before SetWorkState so that the enabled
	// atomic bool is synced from the Enabled setting.
	require.NoError(t, s.start(ctx))

	tenantID := roachpb.MustMakeTenantID(7)
	cleanup := SetWorkState(tenantID, WorkloadInfo{WorkloadID: 100}, WorkCPU, "BackgroundTest")
	defer cleanup()

	testutils.SucceedsSoon(t, func() error {
		samples := s.GetSamples(nil)
		if len(samples) == 0 {
			return errors.New("no samples collected yet")
		}
		return nil
	})

	samples := s.GetSamples(nil)
	require.NotEmpty(t, samples)
	sample := samples[0]
	require.Equal(t, roachpb.NodeID(1), sample.NodeID)
	require.Equal(t, tenantID, sample.TenantID)
	require.Equal(t, WorkCPU, sample.WorkEventType)
	require.Equal(t, "BackgroundTest", sample.WorkEvent)
}

func TestSamplerDoubleStartFails(t *testing.T) {
	s, stopper := newTestSampler()
	defer stopper.Stop(context.Background())

	ctx := context.Background()
	require.NoError(t, s.start(ctx))

	err := s.start(ctx)
	require.Error(t, err)
	require.True(t, errors.HasAssertionFailure(err))
}

func TestSamplerRuntimeSettingChanges(t *testing.T) {
	t.Run("enabled toggle", func(t *testing.T) {
		s, stopper := newTestSampler()
		defer stopper.Stop(context.Background())

		ctx := context.Background()
		Enabled.Override(ctx, &s.st.SV, true)
		SampleInterval.Override(ctx, &s.st.SV, 10*time.Millisecond)

		// start() must be called before SetWorkState so that the enabled
		// atomic bool is synced from the Enabled setting.
		require.NoError(t, s.start(ctx))

		tenantID := roachpb.MustMakeTenantID(4)
		cleanup := SetWorkState(tenantID, WorkloadInfo{WorkloadID: 50}, WorkIO, "ToggleTest")
		defer cleanup()

		testutils.SucceedsSoon(t, func() error {
			if s.buffer.Len() == 0 {
				return errors.New("no samples yet")
			}
			return nil
		})

		// Disable sampling.
		Enabled.Override(ctx, &s.st.SV, false)
		// Wait for any in-flight sample to complete.
		time.Sleep(50 * time.Millisecond)
		countAfterDisable := s.buffer.Len()
		// Verify no growth over several more intervals.
		time.Sleep(50 * time.Millisecond)
		require.Equal(t, countAfterDisable, s.buffer.Len())
	})

	t.Run("buffer size change", func(t *testing.T) {
		s, stopper := newTestSampler()
		defer stopper.Stop(context.Background())

		ctx := context.Background()
		require.NoError(t, s.start(ctx))

		// Add samples directly to the buffer.
		for range 20 {
			s.buffer.Add(ASHSample{NodeID: 1})
		}
		require.Equal(t, 20, s.buffer.Len())

		// Shrink the buffer via setting change.
		BufferSize.Override(ctx, &s.st.SV, 5)
		require.Equal(t, 5, s.buffer.Len())
	})

	t.Run("sample interval change", func(t *testing.T) {
		s, stopper := newTestSampler()
		defer stopper.Stop(context.Background())

		ctx := context.Background()
		require.NoError(t, s.start(ctx))

		newInterval := 500 * time.Millisecond
		SampleInterval.Override(ctx, &s.st.SV, newInterval)
		require.Equal(t, int64(newInterval), s.interval.Load())
	})
}

func TestSamplerMetrics(t *testing.T) {
	s, stopper := newTestSampler()
	defer stopper.Stop(context.Background())

	enabled.Store(true)
	defer enabled.Store(false)

	tenantID := roachpb.MustMakeTenantID(2)
	const numGoroutines = 5

	// Each goroutine registers its work state, then waits for a signal
	// to clean up.
	ready := make(chan struct{}, numGoroutines)
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := range numGoroutines {
		go func(idx int) {
			defer wg.Done()
			cleanup := SetWorkState(
				tenantID,
				WorkloadInfo{WorkloadID: uint64(idx + 1)},
				WorkNetwork,
				"MetricsTest",
			)
			ready <- struct{}{}
			<-release
			cleanup()
		}(i)
	}

	// Wait for all goroutines to have registered their state.
	for range numGoroutines {
		<-ready
	}

	// Verify the active work states gauge reflects the registered goroutines.
	require.Equal(t, int64(numGoroutines), s.metrics.ActiveWorkStates.Value())

	// Take a sample and verify latency and counter metrics.
	s.takeSample(context.Background())
	count, _ := s.metrics.TakeSampleLatency.CumulativeSnapshot().Total()
	require.Equal(t, int64(1), count, "expected one latency sample after one takeSample call")
	require.Equal(
		t, int64(numGoroutines), s.metrics.SamplesCollected.Count(),
		"expected samples collected to equal number of active goroutines",
	)

	// Signal all goroutines to clean up and verify the gauge drops to zero.
	close(release)
	wg.Wait()
	require.Equal(t, int64(0), s.metrics.ActiveWorkStates.Value())

	// Test nested (stacked) work states: a single goroutine calling
	// SetWorkState twice should only count as 1 active work state.
	cleanup1 := SetWorkState(tenantID, WorkloadInfo{WorkloadID: 1}, WorkCPU, "Outer")
	require.Equal(t, int64(1), s.metrics.ActiveWorkStates.Value())
	cleanup2 := SetWorkState(tenantID, WorkloadInfo{WorkloadID: 2}, WorkIO, "Inner")
	require.Equal(t, int64(1), s.metrics.ActiveWorkStates.Value(),
		"nested SetWorkState should not increase the active count")
	cleanup2()
	require.Equal(t, int64(1), s.metrics.ActiveWorkStates.Value(),
		"popping to outer state should keep the active count at 1")
	cleanup1()
	require.Equal(t, int64(0), s.metrics.ActiveWorkStates.Value(),
		"clearing the last state should drop the active count to 0")
}

func TestInitGlobalSampler(t *testing.T) {
	// Save and restore globalSampler. initSamplerOnce is consumed
	// by this test; no other test in the package calls InitGlobalSampler.
	savedSampler := globalSampler.Load()
	defer globalSampler.Store(savedSampler)
	globalSampler.Store(nil)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	st := cluster.MakeTestingClusterSettings()
	ctx := context.Background()

	initialized, err := InitGlobalSampler(ctx, 42, st, stopper)
	require.NoError(t, err)
	require.True(t, initialized)
	require.NotNil(t, globalSampler.Load())
	metrics := GlobalSamplerMetrics()
	require.NotNil(t, metrics, "first call should produce non-nil metrics")

	// Second call is a no-op (sync.Once); GlobalSamplerMetrics still
	// returns the same non-nil metrics.
	initialized, err = InitGlobalSampler(ctx, 99, st, stopper)
	require.NoError(t, err)
	require.False(t, initialized)
	metrics2 := GlobalSamplerMetrics()
	require.NotNil(t, metrics2, "metrics should remain available after second init")
	require.True(t, metrics == metrics2, "metrics pointer should be identical")
	s := globalSampler.Load()
	require.Equal(t, roachpb.NodeID(42), s.nodeID)

	// Enable and take a sample via the global sampler.
	Enabled.Override(ctx, &st.SV, true)
	tenantID := roachpb.MustMakeTenantID(10)
	cleanup := SetWorkState(tenantID, WorkloadInfo{WorkloadID: 200}, WorkCPU, "GlobalTest")
	defer cleanup()
	s.takeSample(context.Background())

	// Package-level GetSamples should return the sample.
	samples := GetSamples()
	require.Len(t, samples, 1)
	require.Equal(t, roachpb.NodeID(42), samples[0].NodeID)
	require.Equal(t, tenantID, samples[0].TenantID)
}

func TestSamplerAppName(t *testing.T) {
	// setup creates a Sampler with sampling enabled, cleaned up via
	// t.Cleanup. All subtests share this common initialization.
	setup := func(t *testing.T) *Sampler {
		t.Helper()
		s, stopper := newTestSampler()
		t.Cleanup(func() { stopper.Stop(context.Background()) })
		enabled.Store(true)
		t.Cleanup(func() { enabled.Store(false) })
		t.Cleanup(func() { s.resolver.Store(nil) })
		return s
	}

	tenantID := roachpb.MustMakeTenantID(5)

	t.Run("local cache hit", func(t *testing.T) {
		s := setup(t)
		appName := "myapp"
		appID := GetOrStoreAppNameID(appName)

		info := WorkloadInfo{WorkloadID: 42, AppNameID: appID}
		cleanup := SetWorkState(tenantID, info, WorkCPU, "Optimize")
		defer cleanup()

		s.takeSample(context.Background())

		samples := s.GetSamples(nil)
		require.Len(t, samples, 1)
		require.Equal(t, appName, samples[0].AppName)
	})

	t.Run("local cache miss", func(t *testing.T) {
		s := setup(t)
		// Use an app name ID without storing it in the cache.
		info := WorkloadInfo{WorkloadID: 42, AppNameID: 12345}
		cleanup := SetWorkState(tenantID, info, WorkCPU, "Optimize")
		defer cleanup()

		s.takeSample(context.Background())

		samples := s.GetSamples(nil)
		require.Len(t, samples, 1)
		require.Equal(t, "", samples[0].AppName)
	})

	t.Run("remote resolution", func(t *testing.T) {
		s := setup(t)
		const unknownAppID uint64 = 99999
		const remoteNodeID roachpb.NodeID = 10
		info := WorkloadInfo{
			WorkloadID:    42,
			AppNameID:     unknownAppID,
			GatewayNodeID: remoteNodeID,
		}
		cleanup := SetWorkState(tenantID, info, WorkCPU, "RemoteResolve")
		defer cleanup()

		s.SetAppNameResolver(func(
			ctx context.Context, nodeID roachpb.NodeID, ids []uint64,
		) (map[uint64]string, error) {
			require.Equal(t, remoteNodeID, nodeID)
			require.Contains(t, ids, unknownAppID)
			return map[uint64]string{unknownAppID: "remote-app"}, nil
		})

		s.takeSample(context.Background())

		samples := s.GetSamples(nil)
		require.Len(t, samples, 1)
		require.Equal(t, "remote-app", samples[0].AppName)
	})

	t.Run("skips local node", func(t *testing.T) {
		s := setup(t)
		// Gateway node ID matches the sampler's own node ID (1).
		info := WorkloadInfo{
			WorkloadID:    42,
			AppNameID:     55555,
			GatewayNodeID: 1,
		}
		cleanup := SetWorkState(tenantID, info, WorkCPU, "LocalSkip")
		defer cleanup()

		var called atomic.Bool
		s.SetAppNameResolver(func(
			ctx context.Context, nodeID roachpb.NodeID, ids []uint64,
		) (map[uint64]string, error) {
			called.Store(true)
			return nil, nil
		})

		s.takeSample(context.Background())

		require.False(t, called.Load())
		samples := s.GetSamples(nil)
		require.Len(t, samples, 1)
		require.Equal(t, "", samples[0].AppName)
	})

	t.Run("resolver error", func(t *testing.T) {
		s := setup(t)
		info := WorkloadInfo{
			WorkloadID:    42,
			AppNameID:     66666,
			GatewayNodeID: 20,
		}
		cleanup := SetWorkState(tenantID, info, WorkCPU, "ErrorCase")
		defer cleanup()

		s.SetAppNameResolver(func(
			ctx context.Context, nodeID roachpb.NodeID, ids []uint64,
		) (map[uint64]string, error) {
			return nil, errors.New("connection refused")
		})

		s.takeSample(context.Background())

		samples := s.GetSamples(nil)
		require.Len(t, samples, 1)
		require.Equal(t, "", samples[0].AppName)
	})

	t.Run("nil resolver", func(t *testing.T) {
		s := setup(t)
		// No resolver set — cache miss should leave app name empty.
		info := WorkloadInfo{
			WorkloadID:    42,
			AppNameID:     44444,
			GatewayNodeID: 30,
		}
		cleanup := SetWorkState(tenantID, info, WorkCPU, "NilResolver")
		defer cleanup()

		s.takeSample(context.Background())

		samples := s.GetSamples(nil)
		require.Len(t, samples, 1)
		require.Equal(t, "", samples[0].AppName)
	})

	t.Run("deduplicates resolver calls", func(t *testing.T) {
		s := setup(t)
		const remoteNodeID roachpb.NodeID = 10

		// Two work states with different unknown app name IDs but the
		// same gateway node ID.
		ready := make(chan struct{}, 2)
		release := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		for _, appID := range []uint64{77777, 88888} {
			go func(id uint64) {
				defer wg.Done()
				info := WorkloadInfo{
					WorkloadID:    1,
					AppNameID:     id,
					GatewayNodeID: remoteNodeID,
				}
				cl := SetWorkState(tenantID, info, WorkCPU, "Dedup")
				ready <- struct{}{}
				<-release
				cl()
			}(appID)
		}
		<-ready
		<-ready

		var callCount atomic.Int32
		s.SetAppNameResolver(func(
			ctx context.Context, nodeID roachpb.NodeID, ids []uint64,
		) (map[uint64]string, error) {
			callCount.Add(1)
			return map[uint64]string{
				77777: "app-a",
				88888: "app-b",
			}, nil
		})

		s.takeSample(context.Background())
		close(release)
		wg.Wait()

		// The resolver should have been called exactly once for the
		// single remote gateway node.
		require.Equal(t, int32(1), callCount.Load())

		samples := s.GetSamples(nil)
		require.Len(t, samples, 2)
		appNames := map[string]struct{}{}
		for _, sample := range samples {
			appNames[sample.AppName] = struct{}{}
		}
		require.Contains(t, appNames, "app-a")
		require.Contains(t, appNames, "app-b")
	})
}

func TestSamplerLogSummary(t *testing.T) {
	defer log.ScopeWithoutShowLogs(t).Close(t)

	// newSummaryTestSampler returns a sampler with lastLogTime far
	// enough in the past that the summary fires on the next call,
	// and the log interval set short.
	newSummaryTestSampler := func() (*Sampler, *stop.Stopper) {
		s, stopper := newTestSampler()
		LogInterval.Override(
			context.Background(), &s.st.SV, time.Second,
		)
		s.lastLogTime = timeutil.Now().Add(-time.Hour)
		return s, stopper
	}

	t.Run("emits summary for buffered samples", func(t *testing.T) {
		s, stopper := newSummaryTestSampler()
		defer stopper.Stop(context.Background())

		enabled.Store(true)
		defer enabled.Store(false)

		ctx := context.Background()
		spy := logtestutils.NewLogSpy(
			t,
			logtestutils.MatchesF("ash_workload_summary"),
		)
		defer log.InterceptWith(ctx, spy)()

		// Register a work state and take several samples.
		tenantID := roachpb.MustMakeTenantID(1)
		cleanup := SetWorkState(tenantID, WorkloadInfo{WorkloadID: 42}, WorkCPU, "Optimize")
		defer cleanup()

		for range 5 {
			s.takeSample(ctx)
		}

		// Push lastLogTime back so the next maybeLogSummary fires.
		s.lastLogTime = timeutil.Now().Add(-time.Hour)
		s.maybeLogSummary(ctx)

		// Verify structured events were emitted.
		entries := spy.ReadAll()
		require.NotEmpty(t, entries,
			"expected at least one ASHWorkloadSummary event")
		lastMsg := entries[len(entries)-1].Message
		require.Contains(t, lastMsg, "CPU")
		require.Contains(t, lastMsg, "Optimize")
	})

	t.Run("top-N ordering", func(t *testing.T) {
		s, stopper := newSummaryTestSampler()
		defer stopper.Stop(context.Background())

		ctx := context.Background()
		spy := logtestutils.NewStructuredLogSpy(
			t,
			[]logpb.Channel{logpb.Channel_OPS},
			[]string{"ash_workload_summary"},
			logtestutils.AsLogEntry,
		)
		defer log.InterceptWith(ctx, spy)()

		// Add samples directly to the buffer with different counts
		// to test ordering. IO gets 200, CPU gets 100, LOCK gets 50.
		now := timeutil.Now()
		for range 200 {
			s.buffer.Add(ASHSample{
				SampleTime:    now,
				WorkEventType: WorkIO,
				WorkEvent:     "BatchEval",
				WorkloadID:    "ccc",
			})
		}
		for range 100 {
			s.buffer.Add(ASHSample{
				SampleTime:    now,
				WorkEventType: WorkCPU,
				WorkEvent:     "Optimize",
				WorkloadID:    "aaa",
			})
		}
		for range 50 {
			s.buffer.Add(ASHSample{
				SampleTime:    now,
				WorkEventType: WorkLock,
				WorkEvent:     "LockWait",
				WorkloadID:    "bbb",
			})
		}

		s.maybeLogSummary(ctx)

		entries := spy.GetLogs(logpb.Channel_OPS)
		require.Len(t, entries, 3)

		// Events are emitted in descending order: IO, CPU, LOCK.
		require.Contains(t, entries[0].Message, "IO")
		require.Contains(t, entries[1].Message, "CPU")
		require.Contains(t, entries[2].Message, "LOCK")
	})

	t.Run("skips empty window", func(t *testing.T) {
		s, stopper := newSummaryTestSampler()
		defer stopper.Stop(context.Background())

		ctx := context.Background()
		spy := logtestutils.NewLogSpy(
			t,
			logtestutils.MatchesF("ash_workload_summary"),
		)
		defer log.InterceptWith(ctx, spy)()

		s.maybeLogSummary(ctx)

		require.Empty(t, spy.ReadAll(),
			"no summary should be logged when there are zero samples")
		// lastLogTime should still be updated so we don't accumulate.
		require.True(t, timeutil.Since(s.lastLogTime) < time.Second)
	})
}
