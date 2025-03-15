// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregion

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type mockReplica struct {
	mu struct {
		latencyInfo map[roachpb.NodeID]time.Duration
	}
	muLock    syncutil.Mutex
	refreshed atomic.Int32
}

// RefreshLatency updates the latency info and increments the refresh counter.
func (m *mockReplica) RefreshLatency(latencyInfo map[roachpb.NodeID]time.Duration) {
	m.muLock.Lock()
	defer m.muLock.Unlock()
	m.mu.latencyInfo = latencyInfo
	m.refreshed.Add(1)
}

// getLatencyInfo returns the current latency info.
func (m *mockReplica) getLatencyInfo() map[roachpb.NodeID]time.Duration {
	m.muLock.Lock()
	defer m.muLock.Unlock()
	return m.mu.latencyInfo
}

// getRefreshCount returns the number of times RefreshLatency was called.
func (m *mockReplica) getRefreshCount() int32 {
	return m.refreshed.Load()
}

// TestPolicyRefresherBasic verifies basic functionality of the policy
// refresher. It ensures that the refresher is initially disabled and correctly
// updates latency info and counters on manual refresh.
func TestPolicyRefresherBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	st := cluster.MakeTestingClusterSettings()

	replica1 := &mockReplica{}
	replica2 := &mockReplica{}
	replicas := []Replica{replica1, replica2}

	latencyInfo := map[roachpb.NodeID]time.Duration{
		1: 10 * time.Millisecond,
		2: 20 * time.Millisecond,
	}

	pr := NewPolicyRefresher(
		stopper,
		st,
		func() []Replica { return replicas },
		func() map[roachpb.NodeID]time.Duration { return latencyInfo },
	)

	// Initially disabled.
	require.False(t, pr.IsEnabled())

	// Test manual refresh.
	pr.RefreshPolicies(replicas)
	require.True(t, pr.IsEnabled())
	require.Equal(t, int32(1), replica1.getRefreshCount())
	require.Equal(t, int32(1), replica2.getRefreshCount())
	require.Equal(t, latencyInfo, replica1.getLatencyInfo())
	require.Equal(t, latencyInfo, replica2.getLatencyInfo())

	// Test disable.
	pr.disableAutoTune()
	require.False(t, pr.IsEnabled())
}

// TestPolicyRefresherAutoTune verifies that policy refresher auto-tuning
// activates on the configured interval.
func TestPolicyRefresherAutoTune(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	st := cluster.MakeTestingClusterSettings()

	replica := &mockReplica{}
	replicas := []Replica{replica}

	latencyInfo := map[roachpb.NodeID]time.Duration{
		1: 10 * time.Millisecond,
	}

	pr := NewPolicyRefresher(
		stopper,
		st,
		func() []Replica { return replicas },
		func() map[roachpb.NodeID]time.Duration { return latencyInfo },
	)

	// Enable auto-tuning with a short interval.
	closedts.LeadForGlobalReadsAutoTuneInterval.Override(ctx, &st.SV, 10*time.Millisecond)
	pr.Run(ctx)

	// Wait for at least one auto-tune cycle.
	testutils.SucceedsSoon(t, func() error {
		if replica.getRefreshCount() == 0 {
			return errors.New("waiting for auto-tune refresh")
		}
		return nil
	})

	// Verify auto-tuning occurred.
	require.True(t, pr.IsEnabled())
	require.Greater(t, replica.getRefreshCount(), int32(0))
	require.Equal(t, latencyInfo, replica.getLatencyInfo())

	// Test that changing the interval triggers a refresh.
	closedts.LeadForGlobalReadsAutoTuneInterval.Override(ctx, &st.SV, 0*time.Millisecond)
	testutils.SucceedsSoon(t, func() error {
		if pr.IsEnabled() {
			return errors.New("waiting for auto-tune to be disabled")
		}
		return nil
	})

	oldCount := replica.getRefreshCount()

	// Wait to ensure no auto-tuning occurs.
	time.Sleep(50 * time.Millisecond)

	// Verify auto-tuning did not occur.
	require.False(t, pr.IsEnabled())
	require.Equal(t, oldCount, replica.getRefreshCount())

	closedts.LeadForGlobalReadsAutoTuneInterval.Override(ctx, &st.SV, 20*time.Millisecond)
	testutils.SucceedsSoon(t, func() error {
		if replica.getRefreshCount() <= oldCount {
			return errors.New("waiting for refresh after interval change")
		}
		return nil
	})
	require.True(t, pr.IsEnabled())
	require.Greater(t, replica.getRefreshCount(), oldCount)
}

// TestPolicyRefresherDisabledForOldVersion verifies that auto-tuning remains
// disabled and no refresh operations occur when running with a cluster version
// < 25.2.
func TestPolicyRefresherDisabledForOldVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	prevVersion := roachpb.Version{Major: 25, Minor: 1}
	st := cluster.MakeTestingClusterSettingsWithVersions(prevVersion, prevVersion, true)

	replica := &mockReplica{}
	replicas := []Replica{replica}

	pr := NewPolicyRefresher(
		stopper,
		st,
		func() []Replica { return replicas },
		func() map[roachpb.NodeID]time.Duration { return nil },
	)

	// Set auto-tune interval but use old version.
	closedts.LeadForGlobalReadsAutoTuneInterval.Override(ctx, &st.SV, 10*time.Millisecond)
	pr.Run(ctx)

	// Wait to ensure no auto-tuning occurs.
	time.Sleep(50 * time.Millisecond)

	// Verify auto-tuning did not occur.
	require.False(t, pr.IsEnabled())
	require.Equal(t, int32(0), replica.getRefreshCount())
}
