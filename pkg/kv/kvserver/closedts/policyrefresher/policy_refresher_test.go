// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package policyrefresher

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type mockReplica struct {
	muLock syncutil.Mutex
	nodeID roachpb.NodeID
	policy time.Duration
}

func newMockReplica(nodeID roachpb.NodeID) *mockReplica {
	return &mockReplica{
		nodeID: nodeID,
	}
}

func (m *mockReplica) RefreshPolicy(policyInfo map[roachpb.NodeID]time.Duration) {
	m.muLock.Lock()
	defer m.muLock.Unlock()
	m.policy = policyInfo[m.nodeID]
}

func (m *mockReplica) GetLatencyInfo() time.Duration {
	m.muLock.Lock()
	defer m.muLock.Unlock()
	return m.policy
}

func (m *mockReplica) BlockReplica() (unblock func()) {
	m.muLock.Lock()
	var once sync.Once
	return func() {
		once.Do(m.muLock.Unlock) //nolint:deferunlockcheck
	}
}

func TestPolicyRefresher(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	testCases := []struct {
		name            string
		replicas        []*mockReplica
		latencies       map[roachpb.NodeID]time.Duration
		autoTuneEnabled bool
		version25_2     bool
		expectRefresh   bool
	}{
		{
			name:            "basic refresh with auto-tune enabled",
			replicas:        []*mockReplica{newMockReplica(1), newMockReplica(2)},
			latencies:       map[roachpb.NodeID]time.Duration{3: 100 * time.Millisecond},
			autoTuneEnabled: true,
			version25_2:     true,
			expectRefresh:   true,
		},
		{
			name:            "auto-tune disabled",
			replicas:        []*mockReplica{newMockReplica(1)},
			latencies:       map[roachpb.NodeID]time.Duration{2: 50 * time.Millisecond},
			autoTuneEnabled: false,
			version25_2:     true,
			expectRefresh:   false,
		},
		{
			name:            "version < 25.2",
			replicas:        []*mockReplica{newMockReplica(1)},
			latencies:       map[roachpb.NodeID]time.Duration{2: 50 * time.Millisecond},
			autoTuneEnabled: true,
			version25_2:     false,
			expectRefresh:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set up cluster version.
			var st *cluster.Settings
			if tc.version25_2 {
				st = cluster.MakeTestingClusterSettings()
			} else {
				prevVersion := roachpb.Version{Major: 25, Minor: 1}
				st = cluster.MakeTestingClusterSettingsWithVersions(prevVersion, prevVersion, true)
			}

			var currentReplicas []Replica
			for _, r := range tc.replicas {
				currentReplicas = append(currentReplicas, Replica(r))
			}

			getLeaseholders := func() []Replica { return currentReplicas }
			getLatencies := func() map[roachpb.NodeID]time.Duration { return tc.latencies }

			pr := NewPolicyRefresher(stopper, st, getLeaseholders, getLatencies)
			require.NotNil(t, pr)

			// Trigger refresh.
			pr.refreshPolicies(currentReplicas)

			// Verify results.
			for _, replica := range tc.replicas {
				got := replica.GetLatencyInfo()
				if tc.expectRefresh {
					expected := tc.latencies[replica.nodeID]
					require.Equal(t, expected, got,
						"expected replica %d to have latency %v", replica.nodeID, expected)
				} else {
					require.Zero(t, got,
						"expected replica %d to have no latency", replica.nodeID)
				}
			}
		})
	}
}

func TestLatencyRefreshInterval(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	st := cluster.MakeTestingClusterSettings()

	r1 := newMockReplica(1)
	getLeaseholders := func() []Replica { return []Replica{r1} }

	m := map[roachpb.NodeID]time.Duration{
		1: 100 * time.Millisecond,
	}
	getLatencies := func() map[roachpb.NodeID]time.Duration {
		return m
	}

	testCases := []struct {
		name          string
		interval      time.Duration
		expectRefresh bool
	}{
		{
			name:          "disabled refresh (zero interval)",
			interval:      0,
			expectRefresh: false,
		},
		{
			name:          "enabled refresh (positive interval)",
			interval:      10 * time.Millisecond,
			expectRefresh: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set the refresh interval
			closedts.RangeClosedTimestampPolicyLatencyRefreshInterval.Override(
				ctx, &st.SV, tc.interval)

			pr := NewPolicyRefresher(stopper, st, getLeaseholders, getLatencies)
			require.NotNil(t, pr)

			// Start the refresher
			pr.Run(ctx)

			if tc.expectRefresh {
				testutils.SucceedsSoon(t, func() error {
					pr.mu.RLock()
					defer pr.mu.RUnlock()
					if reflect.DeepEqual(pr.mu.latencyCache, m) {
						return nil
					}
					return errors.New("latency cache does not match expected value")

				})
			} else {
				require.Empty(t, pr.mu.latencyCache)
			}
		})
	}
}

func TestPolicyRefresherEnqueueAndRun(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	st := cluster.MakeTestingClusterSettings()

	r1 := newMockReplica(1)
	r2 := newMockReplica(2)
	r3 := newMockReplica(3)
	getLeaseholders := func() []Replica { return []Replica{r1, r2} }
	latencies := map[roachpb.NodeID]time.Duration{
		1: 100 * time.Millisecond, 2: 200 * time.Millisecond, 3: 300 * time.Millisecond,
	}
	getLatencies := func() map[roachpb.NodeID]time.Duration { return latencies }

	// Enable auto-tune.
	closedts.LeadForGlobalReadsAutoTuneEnabled.Override(ctx, &st.SV, true)
	closedts.RangeClosedTimestampPolicyRefreshInterval.Override(ctx, &st.SV, 10*time.Millisecond)
	closedts.RangeClosedTimestampPolicyLatencyRefreshInterval.Override(ctx, &st.SV, 5*time.Millisecond)

	pr := NewPolicyRefresher(stopper, st, getLeaseholders, getLatencies)
	require.NotNil(t, pr)

	// Start the refresher.
	pr.Run(ctx)

	// Enqueue replicas and see if things are fine even if there are conflicts.
	pr.EnqueueReplicaForRefresh(r1)

	testutils.SucceedsSoon(t, func() error {
		if len(pr.getCurrentLatencies()) != 3 {
			return errors.New("expected latencies to be non-nil")
		}
		if got := r1.GetLatencyInfo(); got != latencies[r1.nodeID] {
			return errors.Newf("expected replica to have latency %v, got %v", latencies[r1.nodeID], got)
		}
		if got := r2.GetLatencyInfo(); got != latencies[r2.nodeID] {
			return errors.Newf("expected replica to have latency %v, got %v", latencies[r2.nodeID], got)
		}
		return nil
	})

	// Enqueue another replica which is not a leaseholder and check if it gets
	// the correct latency.
	pr.EnqueueReplicaForRefresh(r3)
	testutils.SucceedsSoon(t, func() error {
		if got := r3.GetLatencyInfo(); got != latencies[r3.nodeID] {
			return errors.Newf("expected replica to have latency %v, got %v", latencies[r3.nodeID], got)
		}
		return nil
	})
}

func TestPolicyRefresherEnqueueOnBlockingReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	st := cluster.MakeTestingClusterSettings()

	r1 := newMockReplica(1)
	getLeaseholders := func() []Replica { return []Replica{} }
	latencies := map[roachpb.NodeID]time.Duration{
		1: 100 * time.Millisecond,
	}
	getLatencies := func() map[roachpb.NodeID]time.Duration {
		return latencies
	}

	pr := NewPolicyRefresher(stopper, st, getLeaseholders, getLatencies)
	require.NotNil(t, pr)

	// Enable auto-tune.
	closedts.LeadForGlobalReadsAutoTuneEnabled.Override(ctx, &st.SV, true)
	closedts.RangeClosedTimestampPolicyRefreshInterval.Override(ctx, &st.SV, 10*time.Millisecond)
	closedts.RangeClosedTimestampPolicyLatencyRefreshInterval.Override(ctx, &st.SV, 10*time.Millisecond)

	// Start the refresher.
	pr.Run(ctx)
	pr.updateLatencyCache()

	// Block the replica refresh.
	unblock := r1.BlockReplica()

	// Although replica is blocked on RefreshPolicy, EnqueueReplicaForRefresh
	// should not block.
	pr.EnqueueReplicaForRefresh(r1)
	unblock()

	// Policy on r1 should be refreshed after getting unblocked.
	testutils.SucceedsSoon(t, func() error {
		if got := r1.GetLatencyInfo(); got != latencies[r1.nodeID] {
			return errors.Newf("expected replica to have latency %v, got %v",
				latencies[r1.nodeID], got)
		}
		return nil
	})
}
