// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sidetransport

import (
	"context"
	"fmt"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/policyrefresher"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"storj.io/drpc"
)

// mockReplica is a mock implementation of the Replica interface.
type mockReplica struct {
	storeID roachpb.StoreID
	rangeID roachpb.RangeID
	mu      struct {
		syncutil.Mutex
		desc roachpb.RangeDescriptor
	}

	canBump        bool
	cantBumpReason CantCloseReason
	lai            kvpb.LeaseAppliedIndex
	policy         atomic.Int32 // stores ctpb.RangeClosedTimestampPolicy
}

var _ Replica = &mockReplica{}

func (m *mockReplica) StoreID() roachpb.StoreID    { return m.storeID }
func (m *mockReplica) GetRangeID() roachpb.RangeID { return m.rangeID }

func (m *mockReplica) RefreshPolicy(latencies map[roachpb.NodeID]time.Duration) {
	policy := func() ctpb.RangeClosedTimestampPolicy {
		m.mu.Lock()
		defer m.mu.Unlock()
		desc := m.mu.desc
		if latencies == nil {
			return ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO
		}
		maxLatency := time.Duration(-1)
		for _, peer := range desc.InternalReplicas {
			peerLatency, ok := latencies[peer.NodeID]
			if !ok {
				continue
			}
			// Calculate latency bucket by dividing latency by interval size and adding
			// base policy.
			maxLatency = max(maxLatency, peerLatency)
		}
		// FindBucketBasedOnNetworkRTT returns
		// LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO if maxLatency is negative.
		return closedts.FindBucketBasedOnNetworkRTT(maxLatency)
	}
	if m.policy.Load() == int32(ctpb.LAG_BY_CLUSTER_SETTING) {
		return
	}
	m.policy.Store(int32(policy()))
}

func (m *mockReplica) BumpSideTransportClosed(
	_ context.Context, _ hlc.ClockTimestamp, _ map[ctpb.RangeClosedTimestampPolicy]hlc.Timestamp,
) BumpSideTransportClosedResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	reason := ReasonUnknown
	if !m.canBump {
		reason = m.cantBumpReason
	}
	return BumpSideTransportClosedResult{
		OK:         m.canBump,
		FailReason: reason,
		Desc:       &m.mu.desc,
		LAI:        m.lai,
		Policy:     ctpb.RangeClosedTimestampPolicy(m.policy.Load()),
	}
}

func (m *mockReplica) removeReplica(nid roachpb.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	replicas := m.mu.desc.Replicas()
	for _, rd := range replicas.Descriptors() {
		if rd.NodeID == nid {
			replicas.RemoveReplica(rd.NodeID, rd.StoreID)
			m.mu.desc.SetReplicas(replicas)
			return
		}
	}
	panic(fmt.Sprintf("replica not found for n%d", nid))
}

// mockConnFactory is a mock implementation of the connFactory interface.
type mockConnFactory struct{}

func (f *mockConnFactory) new(_ *Sender, nodeID roachpb.NodeID) conn {
	return &mockConn{nodeID: nodeID}
}

// mockConn is a mock implementation of the conn interface.
type mockConn struct {
	nodeID  roachpb.NodeID
	running bool
	closed  bool
}

func (c *mockConn) run(context.Context, *stop.Stopper) { c.running = true }
func (c *mockConn) close()                             { c.closed = true }
func (c *mockConn) getState() connState                { return connState{} }

func newMockSenderWithSt(connFactory connFactory, st *cluster.Settings) (*Sender, *stop.Stopper) {
	stopper := stop.NewStopper()
	clock := hlc.NewClockForTesting(nil)
	s := newSenderWithConnFactory(stopper, st, clock, connFactory)
	s.nodeID = 1 // usually set in (*Sender).Run
	return s, stopper
}

func newMockSender(connFactory connFactory) (*Sender, *stop.Stopper) {
	return newMockSenderWithSt(connFactory, cluster.MakeTestingClusterSettings())
}

func newMockReplica(
	id roachpb.RangeID, policy ctpb.RangeClosedTimestampPolicy, nodes ...roachpb.NodeID,
) *mockReplica {
	var desc roachpb.RangeDescriptor
	desc.RangeID = id
	for _, nodeID := range nodes {
		desc.AddReplica(nodeID, roachpb.StoreID(nodeID), roachpb.VOTER_FULL)
	}
	r := &mockReplica{
		storeID: 1,
		rangeID: id,
		canBump: true,
		lai:     5,
	}
	r.policy.Store(int32(policy))
	r.mu.desc = desc
	return r
}

func newMockReplicaEx(id roachpb.RangeID, replicas ...roachpb.ReplicationTarget) *mockReplica {
	var desc roachpb.RangeDescriptor
	desc.RangeID = id
	for _, r := range replicas {
		desc.AddReplica(r.NodeID, r.StoreID, roachpb.VOTER_FULL)
	}
	r := &mockReplica{
		storeID: 1,
		rangeID: id,
		canBump: true,
		lai:     5,
	}
	r.policy.Store(int32(ctpb.LAG_BY_CLUSTER_SETTING))
	r.mu.desc = desc
	return r
}

func expGroupUpdates(s *Sender, now hlc.ClockTimestamp) []ctpb.Update_GroupUpdate {
	targetForPolicy := func(pol ctpb.RangeClosedTimestampPolicy) hlc.Timestamp {
		return closedts.TargetForPolicy(
			now,
			s.clock.MaxOffset(),
			closedts.TargetDuration.Get(&s.st.SV),
			closedts.LeadForGlobalReadsOverride.Get(&s.st.SV),
			closedts.SideTransportCloseInterval.Get(&s.st.SV),
			closedts.SideTransportPacingRefreshInterval.Get(&s.st.SV),
			pol,
		)
	}
	return []ctpb.Update_GroupUpdate{
		{Policy: ctpb.LAG_BY_CLUSTER_SETTING, ClosedTimestamp: targetForPolicy(ctpb.LAG_BY_CLUSTER_SETTING)},
		{Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO)},
		{Policy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_20MS, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_20MS)},
		{Policy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_40MS, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_40MS)},
		{Policy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_60MS, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_60MS)},
		{Policy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_80MS, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_80MS)},
		{Policy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_100MS, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_100MS)},
		{Policy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_120MS, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_120MS)},
		{Policy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_140MS, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_140MS)},
		{Policy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_160MS, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_160MS)},
		{Policy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_180MS, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_180MS)},
		{Policy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_200MS, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_200MS)},
		{Policy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_220MS, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_220MS)},
		{Policy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_240MS, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_240MS)},
		{Policy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_260MS, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_260MS)},
		{Policy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_280MS, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_280MS)},
		{Policy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_300MS, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_300MS)},
		{Policy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_EQUAL_OR_GREATER_THAN_300MS, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_EQUAL_OR_GREATER_THAN_300MS)},
	}
}

func TestSenderBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	connFactory := &mockConnFactory{}
	s, stopper := newMockSender(connFactory)
	defer stopper.Stop(ctx)

	// No leaseholders.
	now := s.publish(ctx)
	require.Len(t, s.trackedMu.tracked, 0)
	require.Len(t, s.leaseholdersMu.leaseholders, 0)
	require.Len(t, s.connsMu.conns, 0)

	require.Equal(t, ctpb.SeqNum(1), s.trackedMu.lastSeqNum)
	up, ok := s.buf.GetBySeq(ctx, 1)
	require.True(t, ok)
	require.Equal(t, roachpb.NodeID(1), up.NodeID)
	require.Equal(t, ctpb.SeqNum(1), up.SeqNum)
	require.True(t, up.Snapshot)
	require.Equal(t, expGroupUpdates(s, now), up.ClosedTimestamps)
	require.Nil(t, up.Removed)
	require.Nil(t, up.AddedOrUpdated)

	// Add a leaseholder that can close.
	r1 := newMockReplica(15, ctpb.LAG_BY_CLUSTER_SETTING, 1, 2, 3)
	s.RegisterLeaseholder(ctx, r1, 1)
	now = s.publish(ctx)
	require.Len(t, s.trackedMu.tracked, 1)
	require.Equal(t, map[roachpb.RangeID]trackedRange{
		15: {lai: 5, policy: ctpb.LAG_BY_CLUSTER_SETTING},
	}, s.trackedMu.tracked)
	require.Len(t, s.leaseholdersMu.leaseholders, 1)
	require.Len(t, s.connsMu.conns, 2)

	require.Equal(t, ctpb.SeqNum(2), s.trackedMu.lastSeqNum)
	up, ok = s.buf.GetBySeq(ctx, 2)
	require.True(t, ok)
	require.Equal(t, roachpb.NodeID(1), up.NodeID)
	require.Equal(t, ctpb.SeqNum(2), up.SeqNum)
	require.Equal(t, false, up.Snapshot)
	require.Equal(t, expGroupUpdates(s, now), up.ClosedTimestamps)
	require.Nil(t, up.Removed)
	require.Equal(t, []ctpb.Update_RangeUpdate{
		{RangeID: 15, LAI: 5, Policy: ctpb.LAG_BY_CLUSTER_SETTING},
	}, up.AddedOrUpdated)

	c2, ok := s.connsMu.conns[2]
	require.True(t, ok)
	require.Equal(t, &mockConn{nodeID: 2, running: true, closed: false}, c2.(*mockConn))
	c3, ok := s.connsMu.conns[3]
	require.True(t, ok)
	require.Equal(t, &mockConn{nodeID: 3, running: true, closed: false}, c3.(*mockConn))

	// The leaseholder can not close the next timestamp.
	r1.canBump = false
	r1.cantBumpReason = ProposalsInFlight
	now = s.publish(ctx)
	require.Len(t, s.trackedMu.tracked, 0)
	require.Len(t, s.leaseholdersMu.leaseholders, 1)
	require.Len(t, s.connsMu.conns, 2)
	require.Equal(t, 1, s.trackedMu.closingFailures[r1.cantBumpReason])

	require.Equal(t, ctpb.SeqNum(3), s.trackedMu.lastSeqNum)
	up, ok = s.buf.GetBySeq(ctx, 3)
	require.True(t, ok)
	require.Equal(t, roachpb.NodeID(1), up.NodeID)
	require.Equal(t, ctpb.SeqNum(3), up.SeqNum)
	require.Equal(t, false, up.Snapshot)
	require.Equal(t, expGroupUpdates(s, now), up.ClosedTimestamps)
	require.Equal(t, []roachpb.RangeID{15}, up.Removed)
	require.Nil(t, up.AddedOrUpdated)

	// The leaseholder loses its lease.
	s.UnregisterLeaseholder(ctx, 1, 15)
	now = s.publish(ctx)
	require.Len(t, s.trackedMu.tracked, 0)
	require.Len(t, s.leaseholdersMu.leaseholders, 0)
	require.Len(t, s.connsMu.conns, 0)

	require.Equal(t, ctpb.SeqNum(4), s.trackedMu.lastSeqNum)
	up, ok = s.buf.GetBySeq(ctx, 4)
	require.True(t, ok)
	require.Equal(t, roachpb.NodeID(1), up.NodeID)
	require.Equal(t, ctpb.SeqNum(4), up.SeqNum)
	require.Equal(t, false, up.Snapshot)
	require.Equal(t, expGroupUpdates(s, now), up.ClosedTimestamps)
	require.Nil(t, up.Removed)
	require.Nil(t, up.AddedOrUpdated)

	require.True(t, c2.(*mockConn).closed)
	require.True(t, c3.(*mockConn).closed)
}

func TestSenderConnectionChanges(t *testing.T) {
	// TODO: Two ranges.
	// Add follower for range 1: 2, 3.
	// - check conns to 2 and 3.
	// Add follower for range 2: 3, 4.
	// - check conns to 2, 3, 4.
	// Remove followers for range 2, 3.
	// - check conns to 3, 4.
	// Remove followers for range 3.
	// - check conns to 4.
}

func TestSenderColocateReplicasOnSameNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	connFactory := &mockConnFactory{}
	s, stopper := newMockSender(connFactory)
	defer stopper.Stop(ctx)

	rt := func(node, store int) roachpb.ReplicationTarget {
		return roachpb.ReplicationTarget{
			NodeID:  roachpb.NodeID(node),
			StoreID: roachpb.StoreID(store),
		}
	}

	// Add a leaseholder that can close.
	r1 := newMockReplicaEx(15, rt(1, 1), rt(1, 2), rt(2, 3))
	s.RegisterLeaseholder(ctx, r1, 1)
	now := s.publish(ctx)
	require.Len(t, s.trackedMu.tracked, 1)
	require.Equal(t, map[roachpb.RangeID]trackedRange{
		15: {lai: 5, policy: ctpb.LAG_BY_CLUSTER_SETTING},
	}, s.trackedMu.tracked)
	require.Len(t, s.leaseholdersMu.leaseholders, 1)
	// Ensure that we have two connections, one for remote node and one for local.
	// This is required for colocated replica in store 2.
	require.Len(t, s.connsMu.conns, 2)

	// Sanity check that buffer contains our leaseholder.
	require.Equal(t, ctpb.SeqNum(1), s.trackedMu.lastSeqNum)
	up, ok := s.buf.GetBySeq(ctx, 1)
	require.True(t, ok)
	require.Equal(t, roachpb.NodeID(1), up.NodeID)
	require.Equal(t, ctpb.SeqNum(1), up.SeqNum)
	require.Equal(t, true, up.Snapshot)
	require.Equal(t, expGroupUpdates(s, now), up.ClosedTimestamps)
	require.Nil(t, up.Removed)
	require.Equal(t, []ctpb.Update_RangeUpdate{
		{RangeID: 15, LAI: 5, Policy: ctpb.LAG_BY_CLUSTER_SETTING},
	}, up.AddedOrUpdated)
}

// TestSenderPolicyCountForDifferentVersions verifies that the sender tracks the
// correct number of policies based on the cluster version.
func TestSenderPolicyCountForDifferentVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testutils.RunTrueAndFalse(t, "cluster version", func(t *testing.T, useOldVersion bool) {
		var st *cluster.Settings
		var expectedPolicyCount int
		if useOldVersion {
			prevVersion := roachpb.Version{Major: 25, Minor: 1}
			st = cluster.MakeTestingClusterSettingsWithVersions(prevVersion, prevVersion, true)
			expectedPolicyCount = int(roachpb.MAX_CLOSED_TIMESTAMP_POLICY)
		} else {
			st = cluster.MakeTestingClusterSettings()
			expectedPolicyCount = int(ctpb.MAX_CLOSED_TIMESTAMP_POLICY)
		}
		connFactory := &mockConnFactory{}
		s, stopper := newMockSenderWithSt(connFactory, st)
		defer stopper.Stop(ctx)

		s.publish(ctx)
		require.Len(t, s.trackedMu.tracked, 0)
		require.Len(t, s.trackedMu.lastClosed, expectedPolicyCount)
	})
}

// TestSenderWithLatencyTracker verifies that the sender correctly updates
// closed timestamp policies based on network latency between nodes.
func TestSenderWithLatencyTracker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	connFactory := &mockConnFactory{}

	st := cluster.MakeTestingClusterSettings()
	closedts.RangeClosedTimestampPolicyRefreshInterval.Override(ctx, &st.SV, 5*time.Millisecond)
	closedts.RangeClosedTimestampPolicyLatencyRefreshInterval.Override(ctx, &st.SV, 5*time.Millisecond)
	closedts.LeadForGlobalReadsAutoTuneEnabled.Override(ctx, &st.SV, true)

	var furthestNodeLatency atomic.Int64
	furthestNodeLatency.Store(int64(100 * time.Millisecond))
	extendCrossRegionLatency := func() {
		furthestNodeLatency.Store(int64(200 * time.Millisecond))
	}

	// Mock latency function that simulates network latency between nodes.
	getLatencyFn := func() map[roachpb.NodeID]time.Duration {
		return map[roachpb.NodeID]time.Duration{
			1: 1 * time.Millisecond,
			2: 50 * time.Millisecond,
			3: time.Duration(furthestNodeLatency.Load()),
		}
	}

	s, stopper := newMockSender(connFactory)
	policyRefresher := policyrefresher.NewPolicyRefresher(stopper, st, s.GetLeaseholders, getLatencyFn, nil)
	defer stopper.Stop(ctx)
	policyRefresher.Run(ctx)

	// Verify initial state with no leaseholders.
	now := s.publish(ctx)
	require.Len(t, s.trackedMu.tracked, 0)
	require.Len(t, s.trackedMu.lastClosed, int(ctpb.MAX_CLOSED_TIMESTAMP_POLICY))
	require.Len(t, s.leaseholdersMu.leaseholders, 0)
	require.Len(t, s.connsMu.conns, 0)

	require.Equal(t, ctpb.SeqNum(1), s.trackedMu.lastSeqNum)
	up, ok := s.buf.GetBySeq(ctx, 1)
	require.True(t, ok)
	require.Equal(t, roachpb.NodeID(1), up.NodeID)
	require.Equal(t, ctpb.SeqNum(1), up.SeqNum)
	require.True(t, up.Snapshot)
	require.Equal(t, expGroupUpdates(s, now), up.ClosedTimestamps)
	require.Nil(t, up.Removed)
	require.Nil(t, up.AddedOrUpdated)

	// Add a leaseholder with replicas in different regions.
	r := newMockReplica(15, ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO, 1, 2, 3)

	// Verify policy updates when adding a leaseholder with far-away replicas.
	s.RegisterLeaseholder(ctx, r, 1)
	testutils.SucceedsSoon(t, func() error {
		if ctpb.RangeClosedTimestampPolicy(r.policy.Load()) != ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_120MS {
			return errors.New("policy not updated")
		}
		return nil
	})
	now = s.publish(ctx)
	require.Len(t, s.trackedMu.tracked, 1)
	require.Len(t, s.trackedMu.lastClosed, int(ctpb.MAX_CLOSED_TIMESTAMP_POLICY))
	require.Equal(t, map[roachpb.RangeID]trackedRange{
		15: {lai: 5, policy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_120MS},
	}, s.trackedMu.tracked)
	require.Len(t, s.leaseholdersMu.leaseholders, 1)
	require.Len(t, s.connsMu.conns, 2)

	require.Equal(t, ctpb.SeqNum(2), s.trackedMu.lastSeqNum)
	up, ok = s.buf.GetBySeq(ctx, 2)
	require.True(t, ok)
	require.Equal(t, roachpb.NodeID(1), up.NodeID)
	require.Equal(t, ctpb.SeqNum(2), up.SeqNum)
	require.Equal(t, false, up.Snapshot)
	require.Equal(t, expGroupUpdates(s, now), up.ClosedTimestamps)
	require.Nil(t, up.Removed)
	require.Equal(t, []ctpb.Update_RangeUpdate{
		{RangeID: 15, LAI: 5, Policy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_120MS},
	}, up.AddedOrUpdated)

	// Verify policy updates when far-away replica latency increases.
	extendCrossRegionLatency()
	testutils.SucceedsSoon(t, func() error {
		if ctpb.RangeClosedTimestampPolicy(r.policy.Load()) != ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_220MS {
			return errors.New("policy not updated")
		}
		return nil
	})
	now = s.publish(ctx)
	require.Equal(t, ctpb.SeqNum(3), s.trackedMu.lastSeqNum)
	up, ok = s.buf.GetBySeq(ctx, 3)
	require.True(t, ok)
	require.Equal(t, expGroupUpdates(s, now), up.ClosedTimestamps)
	require.Equal(t, map[roachpb.RangeID]trackedRange{
		15: {lai: 5, policy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_220MS},
	}, s.trackedMu.tracked)

	// Verify policy updates when removing the high-latency replica.
	r.removeReplica(3)
	testutils.SucceedsSoon(t, func() error {
		if ctpb.RangeClosedTimestampPolicy(r.policy.Load()) != ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_60MS {
			return errors.New("policy not updated")
		}
		return nil
	})
	now = s.publish(ctx)
	require.Equal(t, ctpb.SeqNum(4), s.trackedMu.lastSeqNum)
	up, ok = s.buf.GetBySeq(ctx, 4)
	require.True(t, ok)
	require.Equal(t, expGroupUpdates(s, now), up.ClosedTimestamps)
	require.Equal(t, map[roachpb.RangeID]trackedRange{
		15: {lai: 5, policy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_60MS},
	}, s.trackedMu.tracked)

	// Verify cleanup after unregistering the leaseholder.
	s.UnregisterLeaseholder(ctx, 1, 15)
	s.publish(ctx)
	require.Len(t, s.trackedMu.tracked, 0)
	require.Len(t, s.leaseholdersMu.leaseholders, 0)
	require.Len(t, s.connsMu.conns, 0)
}

func TestSenderSameRangeDifferentStores(t *testing.T) {
	// TODO: Two replicas, different stores, same replica.
}

// TODO(andrei): add test for updatesBuf.

// mockReceiver is a SideTransportServer.
type mockReceiver struct {
	stop     chan struct{}
	called   atomic.Bool
	calledCh chan struct{}
}

var _ ctpb.SideTransportServer = &mockReceiver{}

// PushUpdates is the streaming RPC handler.
func (s *mockReceiver) PushUpdates(stream ctpb.SideTransport_PushUpdatesServer) error {
	if s.called.CompareAndSwap(false, true) {
		close(s.calledCh)
	}
	// Block the RPC until close() is called.
	<-s.stop
	return nil
}

func newMockReceiver() *mockReceiver {
	return &mockReceiver{
		stop:     make(chan struct{}),
		calledCh: make(chan struct{}),
	}
}

// sideTransportGRPCServer wraps a Receiver (a real one of a mock) in a gRPC
// server listening on a network interface.
type sideTransportGRPCServer struct {
	lis      net.Listener
	srv      *grpc.Server
	receiver ctpb.SideTransportServer
}

func (s *sideTransportGRPCServer) Close() {
	s.srv.Stop()
	_ /* err */ = s.lis.Close()
}

func (s *sideTransportGRPCServer) addr() net.Addr {
	return s.lis.Addr()
}

func newMockSideTransportGRPCServer(
	ctx context.Context, stopper *stop.Stopper,
) (*sideTransportGRPCServer, error) {
	receiver := newMockReceiver()
	if err := stopper.RunAsyncTask(ctx, "stopper-watcher", func(ctx context.Context) {
		// We can't use a Closer since the receiver will be blocking inside of a task.
		<-stopper.ShouldQuiesce()
		receiver.Close()
	}); err != nil {
		return nil, err
	}
	server, err := newMockSideTransportGRPCServerWithOpts(ctx, stopper, receiver)
	if err != nil {
		return nil, err
	}
	return server, nil
}

func newMockSideTransportGRPCServerWithOpts(
	ctx context.Context, stopper *stop.Stopper, receiver ctpb.SideTransportServer,
) (*sideTransportGRPCServer, error) {
	lis, err := net.Listen("tcp", "localhost:")
	if err != nil {
		return nil, err
	}

	clock := hlc.NewClockForTesting(nil)
	grpcServer, err := rpc.NewServer(ctx, rpc.NewInsecureTestingContext(ctx, clock, stopper))
	if err != nil {
		return nil, err
	}
	ctpb.RegisterSideTransportServer(grpcServer, receiver)
	go func() {
		_ /* err */ = grpcServer.Serve(lis)
	}()
	server := &sideTransportGRPCServer{
		lis:      lis,
		srv:      grpcServer,
		receiver: receiver,
	}
	stopper.AddCloser(server)
	return server, nil
}

func (s *sideTransportGRPCServer) mockReceiver() *mockReceiver {
	return s.receiver.(*mockReceiver)
}

func (s *mockReceiver) Close() {
	close(s.stop)
}

type mockDialer struct {
	mu struct {
		syncutil.Mutex
		addrs map[roachpb.NodeID]string
		conns []*grpc.ClientConn
	}
}

var _ rpcbase.NodeDialer = &mockDialer{}

type nodeAddr struct {
	nid  roachpb.NodeID
	addr string
}

func newMockDialer(addrs ...nodeAddr) *mockDialer {
	d := &mockDialer{}
	d.mu.addrs = make(map[roachpb.NodeID]string)
	for _, addr := range addrs {
		d.mu.addrs[addr.nid] = addr.addr
	}
	return d
}

func newMockSideTransportClientFactory(nd rpcbase.NodeDialer) sideTransportClientFactory {
	return func(ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass) (ctpb.RPCSideTransportClient, error) {
		return ctpb.DialSideTransportClient(nd, ctx, nodeID, class, false) // TODO(server): enable DRPC
	}
}

func (m *mockDialer) addOrUpdateNode(nid roachpb.NodeID, addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.addrs[nid] = addr
}

func (m *mockDialer) Dial(
	ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (_ *grpc.ClientConn, _ error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	addr, ok := m.mu.addrs[nodeID]
	if !ok {
		return nil, errors.Errorf("node not configured in mockDialer: n%d", nodeID)
	}
	//lint:ignore SA1019 grpc.WithInsecure is deprecated
	c, err := grpc.Dial(addr, grpc.WithInsecure())
	if err == nil {
		m.mu.conns = append(m.mu.conns, c)
	}
	return c, err
}

func (m *mockDialer) DRPCDial(
	ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (_ drpc.Conn, _ error) {
	return nil, errors.New("DRPCDial unimplemented")
}

func (m *mockDialer) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.mu.conns {
		_ /* err */ = c.Close() // nolint:grpcconnclose
	}
}

// Test that the stopper quiescence interrupts a stream.Send.
func TestRPCConnUnblocksOnStopper(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	srv, err := newMockSideTransportGRPCServer(ctx, stopper)
	require.NoError(t, err)
	dialer := newMockDialer(nodeAddr{
		nid:  2,
		addr: srv.addr().String(),
	})
	defer dialer.Close()

	ch := make(chan struct{})
	s, stopper := newMockSender(newRPCConnFactory(newMockSideTransportClientFactory(dialer),
		connTestingKnobs{beforeSend: func(_ roachpb.NodeID, msg *ctpb.Update) {
			// Try to send an update to ch, if anyone is still listening.
			ch <- struct{}{}
		}}))
	defer stopper.Stop(ctx)

	// Add leaseholders that can close, in order to establish a connection to n2.
	// We add many of them, and we'll increment their LAIs periodically such that
	// all messages need to explicitly mention all of them, in order to get large
	// messages. This speeds up the test, since the large messages make the sender
	// block quicker.
	const numReplicas = 10000
	replicas := make([]*mockReplica, numReplicas)
	for i := 0; i < numReplicas; i++ {
		replicas[i] = newMockReplica(roachpb.RangeID(i+1), ctpb.LAG_BY_CLUSTER_SETTING, 1, 2)
		s.RegisterLeaseholder(ctx, replicas[i], 1 /* leaseSeq */)
	}

	incrementLAIs := func() {
		for _, r := range replicas {
			r.lai++
		}
	}

	s.publish(ctx)
	require.Len(t, s.connsMu.conns, 1)
	// Wait until at least one update has been delivered. This means the rpcConn
	// task has been started.
	<-srv.mockReceiver().calledCh

	// Now get the rpcConn to keep sending messages by calling s.publish()
	// repeatedly. We'll detect when the rpcConn is blocked (because the Receiver
	// is not reading any of the messages).
	senderBlocked := make(chan struct{})
	go func() {
		// Publish enough messages to fill up the network buffers and cause the
		// stream.Send() to block.
		for {
			select {
			case <-ch:
				// As soon as the conn send a message, publish another update to cause
				// the conn to send another message.
				incrementLAIs()
				s.publish(ctx)
			case <-time.After(100 * time.Millisecond):
				// The conn hasn't sent anything in a while. It must be blocked on Send.
				close(senderBlocked)
				return
			}
		}
	}()

	// Wait for the sender to appear blocked.
	<-senderBlocked

	// Stop the stopper. If this doesn't timeout, then the rpcConn's task must
	// have been unblocked.
	stopper.Stop(ctx)
}

// Test a Sender and Receiver talking gRPC to each other.
func TestSenderReceiverIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// We're going to create Receivers, corresponding to 3 nodes. Node 1 will also
	// be the Sender, so we won't expect a connection to it (the Sender doesn't
	// connect to itself).
	const numNodes = 3
	receivers := make([]*Receiver, numNodes)
	dialer := newMockDialer(nodeAddr{})
	defer dialer.Close()
	incomingStreamOnN2FromN1Terminated := make(chan error)
	for i := 0; i < numNodes; i++ {
		receiverStop := stop.NewStopper()
		defer func(i int) {
			receiverStop.Stop(ctx)
		}(i)
		nid := &base.NodeIDContainer{}
		nid.Set(ctx, roachpb.NodeID(i+1))
		stores := &mockStores{}
		knobs := receiverTestingKnobs{
			roachpb.NodeID(1): {
				onFirstMsg: make(chan struct{}),
				onMsg:      make(chan *ctpb.Update),
			},
		}
		incomingFromN1Knobs := knobs[1]
		switch nid.Get() {
		case 1:
			// n1 doesn't expect any streams, since the only active sender will be on
			// n1 and it's not supposed to connect to the local receiver.
			incomingFromN1Knobs.onRecvErr = func(_ roachpb.NodeID, _ error) {
				t.Errorf("unexpected receive error on node n%s", nid)
			}
		case 2:
			// n2 gets a special handler.
			incomingFromN1Knobs.onRecvErr = func(_ roachpb.NodeID, err error) {
				incomingStreamOnN2FromN1Terminated <- err
			}
		}
		knobs[1] = incomingFromN1Knobs
		receivers[i] = NewReceiver(nid, receiverStop, stores, knobs)
		srv, err := newMockSideTransportGRPCServerWithOpts(ctx, receiverStop, receivers[i])
		dialer.addOrUpdateNode(nid.Get(), srv.addr().String())
		require.NoError(t, err)
	}

	s, senderStopper := newMockSender(newRPCConnFactory(newMockSideTransportClientFactory(dialer), connTestingKnobs{}))
	defer senderStopper.Stop(ctx)
	s.Run(ctx, roachpb.NodeID(1))

	// Add a replica with replicas on n2 and n3.
	r1 := newMockReplica(15, ctpb.LAG_BY_CLUSTER_SETTING, 1, 2, 3)
	s.RegisterLeaseholder(ctx, r1, 1 /* leaseSeq */)
	// Check that connections to n2,3 are established.
	<-receivers[1].testingKnobs[1].onFirstMsg
	<-receivers[2].testingKnobs[1].onFirstMsg
	// Remove one of the replicas and check that the connection to the respective
	// Receiver drops (since there's no other ranges with replicas on n2).
	r1.removeReplica(roachpb.NodeID(2))
	<-incomingStreamOnN2FromN1Terminated
	// Check that the other Receiver is still receiving updates.
	<-receivers[2].testingKnobs[1].onMsg
}

type failingDialer struct {
	dialCount int32
}

var _ rpcbase.NodeDialer = &failingDialer{}

func (f *failingDialer) Dial(
	ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (_ *grpc.ClientConn, err error) {
	atomic.AddInt32(&f.dialCount, 1)
	return nil, errors.New("failingDialer")
}

func (f *failingDialer) DRPCDial(
	ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (_ drpc.Conn, err error) {
	return nil, errors.New("DRPCDial unimplemented")
}

func (f *failingDialer) callCount() int32 {
	return atomic.LoadInt32(&f.dialCount)
}

// TestRPCConnStopOnClose verifies that connections that are closed would stop
// their work loops eagerly even when nodes they are talking to are unreachable.
func TestRPCConnStopOnClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	sleepTime := time.Millisecond

	dialer := &failingDialer{}
	factory := newRPCConnFactory(newMockSideTransportClientFactory(dialer),
		connTestingKnobs{sleepOnErrOverride: sleepTime})

	s, stopper := newMockSender(factory)
	defer stopper.Stop(ctx)

	// While sender is strictly not needed to dial a connection as dialer
	// always fails dial attempts, it is needed to check if DRPC is enabled
	// or disabled.
	connection := factory.new(s, roachpb.NodeID(1))
	connection.run(ctx, stopper)

	// Wait for first dial attempt for sanity reasons.
	testutils.SucceedsSoon(t, func() error {
		if dialer.callCount() == 0 {
			return errors.New("connection didn't dial yet")
		}
		return nil
	})
	connection.close()
	// Ensure that dialing stops once connection is stopped.
	testutils.SucceedsSoon(t, func() error {
		if stopper.NumTasks() > 0 {
			return errors.New("connection worker didn't stop yet")
		}
		return nil
	})
}

// TestAllUpdatesBufAreSignalled creates a bunch of goroutines that wait on
// updatesBuf, and then publishes multiple messages to the updatesBuf. It
// verifies that all goroutines are signalled for all messages, and no goroutine
// misses any message.
func TestAllUpdatesBufAreSignalled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	connFactory := &mockConnFactory{}
	st := cluster.MakeTestingClusterSettings()
	s, stopper := newMockSenderWithSt(connFactory, st)
	defer stopper.Stop(ctx)

	const numGoroutines = 100
	const numMsgs = 100
	var counter atomic.Int64

	// Start 100 goroutines that will wait on sequence numbers 1-100.
	wg := sync.WaitGroup{}
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			for seqNum := ctpb.SeqNum(1); seqNum <= numMsgs; seqNum++ {
				_, ok := s.buf.GetBySeq(ctx, seqNum)
				if ok {
					counter.Add(1)
				}
			}
			wg.Done()
		}()
	}

	// Publish 10 messages.
	for i := 0; i < numMsgs; i++ {
		s.publish(ctx)
	}

	// Verify that eventually all goroutines received all messages.
	wg.Wait()
	require.Equal(t, int64(numGoroutines*numMsgs), counter.Load())
}

// TestPaceUpdateSignalling verifies that the task pacer properly spaces out
// signal calls after an update on the updatesBuf.
func TestPaceUpdateSignalling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Flaky under duress.
	skip.UnderDuress(t)

	// Create a mock sender to get access to the buffer.
	ctx := context.Background()
	connFactory := &mockConnFactory{}
	st := cluster.MakeTestingClusterSettings()
	closedts.SideTransportPacingRefreshInterval.Override(ctx, &st.SV, 250*time.Millisecond)
	s, stopper := newMockSenderWithSt(connFactory, st)
	defer stopper.Stop(ctx)

	// numWaiters controls how many goroutines will wait on updatesBuf.
	numWaiters := 1000

	// testPacing tests that when multiple goroutines are waiting on
	// s.buf.GetBySeq, they receive their items spaced out by at least
	// expectedMinSpread. If expectedMinSpread is 0, then pacing is disabled,
	// and we expect all goroutines to receive their items within a few ms of
	// each other.
	//
	// seqNum is the sequence number to wait for.
	testPacing := func(t *testing.T, seqNum ctpb.SeqNum, assertionFunc func(timeSpread time.Duration)) {
		// Track the times when goroutines receive items from the buffer.
		var receiveTimes []crtime.Mono
		var mu syncutil.Mutex

		// Create numWaiters goroutines that wait on s.buf.GetBySeq and record
		// receive times.
		g := ctxgroup.WithContext(ctx)
		for range numWaiters {
			g.Go(func() error {
				// Wait for the specified sequence number.
				_, ok := s.buf.GetBySeq(ctx, seqNum)
				require.True(t, ok)

				mu.Lock()
				defer mu.Unlock()
				receiveTimes = append(receiveTimes, crtime.NowMono())
				return nil
			})
		}

		// Wait until all goroutines are waiting on the buffer.
		testutils.SucceedsSoon(t, func() error {
			if s.buf.TestingGetTotalNumWaiters() < numWaiters {
				return errors.New("not all goroutines are waiting yet")
			}
			return nil
		})

		// Publish an item to the buffer, which should trigger the paced signalling.
		s.publish(ctx)

		// Wait for all goroutines to finish.
		require.NoError(t, g.Wait())

		// Verify that all goroutines received the message.
		require.Len(t, receiveTimes, numWaiters)

		// Verify that the time spread matches expectations.
		minTime := slices.Min(receiveTimes)
		maxTime := slices.Max(receiveTimes)
		timeSpread := maxTime.Sub(minTime)
		assertionFunc(timeSpread)
	}

	// Test with 250ms pacing interval - expect at least 125ms spread just to be
	// conservative. In practice, it should be closer to 250ms.
	t.Run("pacing_interval=250ms", func(t *testing.T) {
		closedts.SideTransportPacingRefreshInterval.Override(ctx, &st.SV, 250*time.Millisecond)
		testPacing(t, 1 /* seqNum */, func(timeSpread time.Duration) {
			require.GreaterOrEqual(t, timeSpread, 125*time.Millisecond)
		})
	})

	// Change to 100ms pacing interval - expect at least 50ms spread.
	t.Run("pacing_interval=100ms", func(t *testing.T) {
		closedts.SideTransportPacingRefreshInterval.Override(ctx, &st.SV, 100*time.Millisecond)
		testPacing(t, 2 /* seqNum */, func(timeSpread time.Duration) {
			require.GreaterOrEqual(t, timeSpread, 50*time.Millisecond)
		})
	})

	// Change to 0ms (disabled) pacing interval - expect all goroutines to be
	// woken within a few milliseconds of each other.
	t.Run("pacing_interval=0ms", func(t *testing.T) {
		closedts.SideTransportPacingRefreshInterval.Override(ctx, &st.SV, 0)
		testPacing(t, 3 /* seqNum */, func(timeSpread time.Duration) {
			// With pacing disabled, all 1000 goroutines should be woken nearly
			// simultaneously. The typical spread is ~1ms, but CI machines under
			// load need more headroom for goroutine scheduling.
			require.LessOrEqual(t, timeSpread, 40*time.Millisecond)
		})
	})
}
