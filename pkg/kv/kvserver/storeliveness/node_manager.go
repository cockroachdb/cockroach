// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// NodeContainer is a container for all StoreLiveness state on a single node. It
// encapsulates all dependencies required to create per-store SupportManagers
// and keeps track of them once created.
type NodeContainer struct {
	Options         Options
	Transport       *Transport
	Knobs           *TestingKnobs
	HeartbeatTicker *timeutil.BroadcastTicker
	stopper         *stop.Stopper
	nodeID          *base.NodeIDContainer

	mu struct {
		syncutil.Mutex
		supportManagers map[roachpb.StoreID]Fabric
	}
}

var _ SupportStatus = (*NodeContainer)(nil)

// NewNodeContainer creates a new NodeContainer.
func NewNodeContainer(
	stopper *stop.Stopper,
	nodeID *base.NodeIDContainer,
	options Options,
	transport *Transport,
	knobs *TestingKnobs,
) *NodeContainer {
	ticker := timeutil.NewBroadcastTicker(options.HeartbeatInterval)
	stopper.AddCloser(stop.CloserFn(ticker.Stop))
	nc := &NodeContainer{
		Options:         options,
		Transport:       transport,
		Knobs:           knobs,
		HeartbeatTicker: ticker,
		stopper:         stopper,
		nodeID:          nodeID,
	}
	nc.mu.supportManagers = make(map[roachpb.StoreID]Fabric)
	return nc
}

// SupportManagerKnobs returns the SupportManagerKnobs from the TestingKnobs..
func (n *NodeContainer) SupportManagerKnobs() *SupportManagerKnobs {
	if n.Knobs != nil {
		return &n.Knobs.SupportManagerKnobs
	}
	return nil
}

// NewSupportManager constructs and returns a new SupportManager for the
// provided store.
func (n *NodeContainer) NewSupportManager(
	storeID roachpb.StoreID,
	engine storage.Engine,
	settings *cluster.Settings,
	clock *hlc.Clock,
) *SupportManager {
	storeIdent := slpb.StoreIdent{NodeID: n.nodeID.Get(), StoreID: storeID}
	sm := NewSupportManager(
		storeIdent, engine, n.Options, settings, n.stopper, clock,
		n.HeartbeatTicker, n.Transport, n.SupportManagerKnobs(),
	)
	n.mu.Lock()
	defer n.mu.Unlock()
	n.mu.supportManagers[storeID] = sm
	return sm
}

// IsSupporting implements the SupportStatus interface.
func (n *NodeContainer) IsSupporting(id slpb.StoreIdent) (bool, hlc.Timestamp) {
	n.mu.Lock()
	defer n.mu.Unlock()

	supporting := true
	var maxWithdrawnTS hlc.Timestamp
	for _, sm := range n.mu.supportManagers {
		isSupporting, withdrawnTS := sm.IsSupporting(id)
		supporting = supporting && isSupporting
		maxWithdrawnTS.Forward(withdrawnTS)
	}
	return supporting, maxWithdrawnTS
}
