// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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
	cleanup := SetWorkState(tenantID, 42, WorkCPU, "Optimize")
	defer cleanup()

	s.takeSample()

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
	s, stopper := newTestSampler()
	defer stopper.Stop(context.Background())

	enabled.Store(true)
	defer enabled.Store(false)

	tenantID := roachpb.MustMakeTenantID(3)

	// Take two samples with the same workload ID.
	cleanup := SetWorkState(tenantID, 99, WorkIO, "BatchEval")
	s.takeSample()
	cleanup()

	cleanup = SetWorkState(tenantID, 99, WorkIO, "BatchEval")
	s.takeSample()
	cleanup()

	samples := s.GetSamples(nil)
	require.Len(t, samples, 2)
	// Both samples should have the same encoded workload ID.
	require.Equal(t, samples[0].WorkloadID, samples[1].WorkloadID)
	require.Equal(t, encodeStmtFingerprintIDToString(99), samples[0].WorkloadID)

	// Verify the cache contains the entry.
	_, ok := s.workloadIDCache.Get(uint64(99))
	require.True(t, ok, "workload ID should be cached")
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
				uint64(idx+1),
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

	s.takeSample()

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
	cleanup := SetWorkState(tenantID, 100, WorkCPU, "BackgroundTest")
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
		cleanup := SetWorkState(tenantID, 50, WorkIO, "ToggleTest")
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

	require.NoError(t, InitGlobalSampler(ctx, 42, st, stopper))
	require.NotNil(t, globalSampler.Load())

	// Second call is a no-op (sync.Once).
	require.NoError(t, InitGlobalSampler(ctx, 99, st, stopper))
	s := globalSampler.Load()
	require.Equal(t, roachpb.NodeID(42), s.nodeID)

	// Enable and take a sample via the global sampler.
	Enabled.Override(ctx, &st.SV, true)
	tenantID := roachpb.MustMakeTenantID(10)
	cleanup := SetWorkState(tenantID, 200, WorkCPU, "GlobalTest")
	defer cleanup()
	s.takeSample()

	// Package-level GetSamples should return the sample.
	samples := GetSamples()
	require.Len(t, samples, 1)
	require.Equal(t, roachpb.NodeID(42), samples[0].NodeID)
	require.Equal(t, tenantID, samples[0].TenantID)
}
