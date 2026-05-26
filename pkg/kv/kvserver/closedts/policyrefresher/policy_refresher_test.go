// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package policyrefresher

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func newNoopPolicyRefresher(stopper *stop.Stopper, settings *cluster.Settings) *PolicyRefresher {
	return NewPolicyRefresher(stopper, settings,
		func() []Replica { return nil },
		func() map[roachpb.NodeID]time.Duration {
			return nil
		},
		nil,
	)
}

type mockSpanConfig struct {
	isGlobalRead bool
}

type mockReplica struct {
	// Note that all fields below are protected by mu.
	mu             syncutil.Mutex
	conf           mockSpanConfig
	policy         ctpb.RangeClosedTimestampPolicy
	furthestNodeID roachpb.NodeID
}

func (m *mockReplica) RefreshPolicy(latencies map[roachpb.NodeID]time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.conf.isGlobalRead {
		m.policy = ctpb.LAG_BY_CLUSTER_SETTING
		return
	}
	latency, ok := latencies[m.furthestNodeID]
	if !ok {
		m.policy = ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO
		return
	}
	m.policy = closedts.FindBucketBasedOnNetworkRTT(latency)
}

func (m *mockReplica) GetPolicy() ctpb.RangeClosedTimestampPolicy {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.policy
}

func (m *mockReplica) SetFurthestNodeID(nodeID roachpb.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.furthestNodeID = nodeID
}

func (m *mockReplica) BlockReplica() (unblock func()) {
	m.mu.Lock()
	var once sync.Once
	return func() {
		once.Do(m.mu.Unlock) //nolint:deferunlockcheck
	}
}

func (m *mockReplica) SetSpanConfig(isGlobalRead bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.conf.isGlobalRead = isGlobalRead
}

// TestPolicyRefresher tests that the policy refresher correctly calls
// RefreshPolicy for replicas.
func TestPolicyRefresher(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	pr := newNoopPolicyRefresher(stopper, cluster.MakeTestingClusterSettings())
	r := &mockReplica{}
	require.Equal(t, r.GetPolicy(), ctpb.LAG_BY_CLUSTER_SETTING)
	r.SetSpanConfig(true)
	pr.refreshPolicies([]Replica{r})
	require.Equal(t, r.GetPolicy(), ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO)
}

// TestPolicyRefresherOnEnqueue tests that the policy refresher correctly
// enqueues and refreshes replicas.
func TestPolicyRefresherOnEnqueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	st := cluster.MakeTestingClusterSettings()

	r := &mockReplica{}

	pr := newNoopPolicyRefresher(stopper, st)
	require.NotNil(t, pr)

	// Start the refresher.
	pr.Run(ctx)

	r.SetSpanConfig(true)
	pr.EnqueueReplicaForRefresh(r)

	testutils.SucceedsSoon(t, func() error {
		if r.GetPolicy() != ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO {
			return errors.Newf("expected replica to have policy %v, got %v",
				ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO, r.GetPolicy())
		}
		return nil
	})
}

// TestPolicyRefreshOnRefreshIntervalUpdate tests that the policy refresher is
// reactive to policy refresh interval change.
func TestPolicyRefreshOnRefreshIntervalUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	st := cluster.MakeTestingClusterSettings()

	r := &mockReplica{}
	getLeaseholders := func() []Replica { return []Replica{r} }
	getLatencies := func() map[roachpb.NodeID]time.Duration { return nil }

	pr := NewPolicyRefresher(stopper, st, getLeaseholders, getLatencies, nil)
	require.NotNil(t, pr)

	// Start the refresher.
	pr.Run(ctx)

	// Set the refresh interval to be really high at the start to ensure that the
	// no replicas are refreshed.
	closedts.RangeClosedTimestampPolicyRefreshInterval.Override(
		ctx, &st.SV, 1*time.Hour)

	r.SetSpanConfig(true)
	require.Equal(t, r.GetPolicy(), ctpb.LAG_BY_CLUSTER_SETTING)

	// Set the refresh interval to short enough ensure that the no replicas are
	// refreshed.
	closedts.RangeClosedTimestampPolicyRefreshInterval.Override(ctx, &st.SV, 10*time.Millisecond)

	testutils.SucceedsSoon(t, func() error {
		if r.GetPolicy() != ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO {
			return errors.Newf("expected replica to have policy %v, got %v",
				ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO, r.GetPolicy())
		}
		return nil
	})
}

// TestPolicyRefresherEnqueueOnBlockingReplica tests that the policy refresher
// correctly enqueues without blocking when a replica.RefreshPolicy is blocked.
func TestPolicyRefresherEnqueueOnBlockingReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	st := cluster.MakeTestingClusterSettings()

	r := &mockReplica{}
	pr := newNoopPolicyRefresher(stopper, st)
	require.NotNil(t, pr)

	// Start the refresher.
	pr.Run(ctx)

	// Set the span config to lead for global reads.
	r.SetSpanConfig(true)

	// Block the replica refresh.
	unblock := r.BlockReplica()

	// Although the pr.Run goroutine is blocked, EnqueueReplicaForRefresh should
	// not block.
	pr.EnqueueReplicaForRefresh(r)
	unblock()

	// Policy on r1 should be refreshed after getting unblocked.
	testutils.SucceedsSoon(t, func() error {
		if r.GetPolicy() != ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO {
			return errors.Newf("expected replica to have policy %v, got %v", ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO, r.GetPolicy())
		}
		return nil
	})
}

// TestPolicyRefresherOnLatencyIntervalUpdate tests that the policy refresher
// stays reactive to updates to the latency interval.
func TestPolicyRefresherOnLatencyIntervalUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	st := cluster.MakeTestingClusterSettings()

	r := &mockReplica{}
	getLeaseholders := func() []Replica { return []Replica{r} }

	var called atomic.Bool
	getLatencies := func() map[roachpb.NodeID]time.Duration {
		called.Store(true)
		return map[roachpb.NodeID]time.Duration{}
	}

	// Set the refresh interval to be really high at the start to ensure that the
	// latency cache is not updated.
	closedts.RangeClosedTimestampPolicyLatencyRefreshInterval.Override(
		ctx, &st.SV, 1*time.Hour)

	pr := NewPolicyRefresher(stopper, st, getLeaseholders, getLatencies, nil)
	require.NotNil(t, pr)
	pr.Run(ctx)

	time.Sleep(10 * time.Millisecond)
	require.Equal(t, false, called.Load())

	// Set the refresh interval to short enough to ensure that the latency cache
	// is updated.
	closedts.RangeClosedTimestampPolicyLatencyRefreshInterval.Override(
		ctx, &st.SV, 10*time.Millisecond)

	testutils.SucceedsSoon(t, func() error {
		if called.Load() {
			return nil
		}
		return errors.New("expected latency update")
	})
}

// TestPolicyRefresherWithLatencies tests that the policy refresher correctly
// updates policies based on latency information from different nodes.
func TestPolicyRefresherWithLatencies(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	st := cluster.MakeTestingClusterSettings()
	closedts.LeadForGlobalReadsAutoTuneEnabled.Override(ctx, &st.SV, true)

	// Create a policy refresher with test latencies.
	latencies := map[roachpb.NodeID]time.Duration{
		1: 10 * time.Millisecond,
		2: 50 * time.Millisecond,
		3: 90 * time.Millisecond,
	}

	r := &mockReplica{}
	// Configure replica to not use global reads span config initially.
	r.SetSpanConfig(false)

	getLeaseholders := func() []Replica { return []Replica{r} }
	getLatencies := func() map[roachpb.NodeID]time.Duration { return latencies }

	pr := NewPolicyRefresher(stopper, st, getLeaseholders, getLatencies, nil)
	require.NotNil(t, pr)
	require.Nil(t, pr.getCurrentLatencies())

	// Initially, the policy should be LAG_BY_CLUSTER_SETTING.
	require.Equal(t, r.GetPolicy(), ctpb.LAG_BY_CLUSTER_SETTING)

	// Enable global reads and set furthest node to 1. We should get
	// LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO policy before the cache update.
	r.SetSpanConfig(true)
	r.SetFurthestNodeID(1)
	pr.refreshPolicies([]Replica{r})
	require.Equal(t, r.GetPolicy(), ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO)

	// Update latency cache and verify we now get <20ms policy for node 1.
	pr.updateLatencyCache()
	require.NotNil(t, pr.getCurrentLatencies())
	pr.refreshPolicies([]Replica{r})
	require.Equal(t, r.GetPolicy(), ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_20MS)

	// Set furthest node to 2 and verify we get <60ms policy.
	r.SetFurthestNodeID(2)
	pr.refreshPolicies([]Replica{r})
	require.Equal(t, r.GetPolicy(), ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_60MS)

	// Set furthest node to 3 and verify we get <100ms policy.
	r.SetFurthestNodeID(3)
	pr.refreshPolicies([]Replica{r})
	require.Equal(t, r.GetPolicy(), ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_100MS)

	// Update node 3's latency to 300ms and verify we get >=300ms policy.
	latencies[3] = 300 * time.Millisecond
	pr.updateLatencyCache()
	pr.refreshPolicies([]Replica{r})
	require.Equal(t, r.GetPolicy(), ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_EQUAL_OR_GREATER_THAN_300MS)
}
