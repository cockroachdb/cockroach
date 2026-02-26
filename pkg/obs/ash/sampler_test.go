// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

// newTestSampler creates a Sampler suitable for unit tests.
func newTestSampler() (*Sampler, *stop.Stopper) {
	stopper := stop.NewStopper()
	st := cluster.MakeTestingClusterSettings()
	s := NewSampler(1, st, stopper)
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

	samples := s.GetSamples()
	require.Len(t, samples, 1)

	sample := samples[0]
	require.Equal(t, roachpb.NodeID(1), sample.NodeID)
	require.Equal(t, tenantID, sample.TenantID)
	require.Equal(t, WorkCPU, sample.WorkEventType)
	require.Equal(t, "Optimize", sample.WorkEvent)
	require.NotZero(t, sample.GoroutineID)
	require.NotZero(t, sample.SampleTime)

	// WorkloadID should be the hex-encoded uint64.
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

	samples := s.GetSamples()
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

	samples := s.GetSamples()
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
