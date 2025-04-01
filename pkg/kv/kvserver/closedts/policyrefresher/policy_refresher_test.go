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

func newNoopPolicyRefresher(
	stopper *stop.Stopper,
	settings *cluster.Settings,
) *PolicyRefresher {
	return NewPolicyRefresher(stopper, settings,
		func() []Replica { return nil },
		func() map[roachpb.NodeID]time.Duration {
			return nil
		})
}

type mockReplica struct {
	muLock syncutil.Mutex
	policy ctpb.RangeClosedTimestampPolicy
}

func (m *mockReplica) RefreshPolicy(_ map[roachpb.NodeID]time.Duration) {
	m.muLock.Lock()
	defer m.muLock.Unlock()
}

func (m *mockReplica) GetPolicy() ctpb.RangeClosedTimestampPolicy {
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

func (m *mockReplica) SetSpanConfig(isGlobalRead bool) {
	m.muLock.Lock()
	defer m.muLock.Unlock()
	if isGlobalRead {
		m.policy = ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO
	} else {
		m.policy = ctpb.LAG_BY_CLUSTER_SETTING
	}
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
// enqueues and refreshes replicas. It also tests that the policy refresher is
// reactive to policy refresh interval change.
func TestPolicyRefresherOnEnqueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	st := cluster.MakeTestingClusterSettings()

	r1 := &mockReplica{}
	r2 := &mockReplica{}
	r3 := &mockReplica{}

	getLeaseholders := func() []Replica { return []Replica{r1, r2} }
	getLatencies := func() map[roachpb.NodeID]time.Duration { return nil }

	pr := NewPolicyRefresher(stopper, st, getLeaseholders, getLatencies)
	require.NotNil(t, pr)

	// Enable auto-tune.
	closedts.RangeClosedTimestampPolicyRefreshInterval.Override(ctx, &st.SV, 10*time.Millisecond)

	// Start the refresher.
	pr.Run(ctx)

	r1.SetSpanConfig(true)
	r3.SetSpanConfig(true)

	// Enqueue replicas which are also leaseholders and check if there is anything unexpected.
	pr.EnqueueReplicaForRefresh(r1)
	pr.EnqueueReplicaForRefresh(r3)

	testutils.SucceedsSoon(t, func() error {
		if r1.GetPolicy() != ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO {
			return errors.Newf("expected replica to have policy %v, got %v", ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO, r1.GetPolicy())
		}
		if r2.GetPolicy() != ctpb.LAG_BY_CLUSTER_SETTING {
			return errors.Newf("expected replica to have policy %v, got %v", ctpb.LAG_BY_CLUSTER_SETTING, r2.GetPolicy())
		}
		if r3.GetPolicy() != ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO {
			return errors.Newf("expected replica to have policy %v, got %v", ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO, r3.GetPolicy())
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

	// Enable auto-tune.
	closedts.LeadForGlobalReadsAutoTuneEnabled.Override(ctx, &st.SV, true)
	closedts.RangeClosedTimestampPolicyRefreshInterval.Override(ctx, &st.SV, 10*time.Millisecond)

	// Start the refresher.
	pr.Run(ctx)

	// Set the span config to lead for global reads.
	r.SetSpanConfig(true)

	// Block the replica refresh.
	unblock := r.BlockReplica()

	// Although replica is blocked on RefreshPolicy, EnqueueReplicaForRefresh
	// should not block.
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

	pr := NewPolicyRefresher(stopper, st, getLeaseholders, getLatencies)
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
