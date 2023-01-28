// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storepool

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
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
func (m *MockNodeLiveness) NodeLivenessFunc(
	nodeID roachpb.NodeID, now time.Time, threshold time.Duration,
) livenesspb.NodeLivenessStatus {
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
	timeUntilStoreDeadValue time.Duration,
	deterministic bool,
	nodeCount NodeCountFunc,
	defaultNodeStatus livenesspb.NodeLivenessStatus,
) (*stop.Stopper, *gossip.Gossip, *timeutil.ManualTime, *StorePool, *MockNodeLiveness) {
	stopper := stop.NewStopper()
	mc := timeutil.NewManualTime(timeutil.Unix(0, 123))
	clock := hlc.NewClockForTesting(mc)
	ambientCtx := log.MakeTestingAmbientContext(stopper.Tracer())
	g := gossip.NewTest(1, stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())
	mnl := NewMockNodeLiveness(defaultNodeStatus)

	TimeUntilStoreDead.Override(ctx, &st.SV, timeUntilStoreDeadValue)
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
