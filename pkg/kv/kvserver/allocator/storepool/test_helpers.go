// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storepool

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// MockNodeLiveness is a testing construct to mock what node liveness status a
// store pool observes for a given node.
type MockNodeLiveness struct {
	syncutil.Mutex
	defaultNodeStatus livenesspb.NodeLivenessStatus
	nodes             map[roachpb.NodeID]livenesspb.NodeLivenessStatus
}

// NewMockNodeLiveness constructs a MockNodeLiveness, for testing purposes.
func NewMockNodeLiveness(defaultNodeStatus livenesspb.NodeLivenessStatus) *MockNodeLiveness {
	return &MockNodeLiveness{
		defaultNodeStatus: defaultNodeStatus,
		nodes:             map[roachpb.NodeID]livenesspb.NodeLivenessStatus{},
	}
}

// SetNodeStatus sets the node liveness status for the given node ID.
func (m *MockNodeLiveness) SetNodeStatus(
	nodeID roachpb.NodeID, status livenesspb.NodeLivenessStatus,
) {
	m.Lock()
	defer m.Unlock()
	m.nodes[nodeID] = status
}

// NodeLivenessFunc is the method that can be injected as part of store pool
// construction to mock out node liveness, in tests.
func (m *MockNodeLiveness) NodeLivenessFunc(nodeID roachpb.NodeID) livenesspb.NodeLivenessStatus {
	m.Lock()
	defer m.Unlock()
	if status, ok := m.nodes[nodeID]; ok {
		return status
	}
	return m.defaultNodeStatus
}

// CreateTestStorePool creates a stopper, gossip and storePool for use in
// tests. Stopper must be stopped by the caller.
func CreateTestStorePool(
	ctx context.Context,
	st *cluster.Settings,
	timeUntilNodeDeadValue time.Duration,
	deterministic bool,
	nodeCount NodeCountFunc,
	defaultNodeStatus livenesspb.NodeLivenessStatus,
) (*stop.Stopper, *gossip.Gossip, *timeutil.ManualTime, *StorePool, *MockNodeLiveness) {
	stopper := stop.NewStopper()
	// Pick a random date that is "realistic" and far enough away from 0.
	mc := timeutil.NewManualTime(time.Date(2020, 0, 0, 0, 0, 0, 0, time.UTC))
	clock := hlc.NewClockForTesting(mc)
	ambientCtx := log.MakeTestingAmbientContext(stopper.Tracer())
	g := gossip.NewTest(1, stopper, metric.NewRegistry())
	mnl := NewMockNodeLiveness(defaultNodeStatus)

	liveness.TimeUntilNodeDead.Override(ctx, &st.SV, timeUntilNodeDeadValue)
	storePool := NewStorePool(
		ambientCtx,
		st,
		g,
		clock,
		nodeCount,
		mnl.NodeLivenessFunc,
		deterministic,
	)
	return stopper, g, mc, storePool, mnl
}
