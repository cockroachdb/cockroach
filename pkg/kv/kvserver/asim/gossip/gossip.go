// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gossip

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Gossip collects and updates the storepools of the cluster upon capacity
// changes and the gossip interval defined in the simulation settings.
type Gossip interface {
	// Tick checks for completed gossip updates and triggers new gossip
	// updates if needed.
	Tick(context.Context, time.Time, state.State)
}

// gossip is an implementation of the Gossip interface. It manages the
// dissemenation of gossip information to stores in the cluster.
type gossip struct {
	storeGossip map[state.StoreID]*storeGossiper
	settings    *config.SimulationSettings
	exchange    *fixedDelayExchange
}

var _ Gossip = &gossip{}

// storeGossiper is the store-local gossip state that tracks information
// required to decide when to gossip and pending outbound gossip infos that
// have been triggered by the underlying kvserver.StoreGossip component.
type storeGossiper struct {
	local              *kvserver.StoreGossip
	lastIntervalGossip time.Time
	descriptorGetter   func(cached bool) roachpb.StoreDescriptor
	pendingOutbound    *roachpb.StoreDescriptor
	addingStore        bool
}

func newStoreGossiper(
	descriptorGetter func(cached bool) roachpb.StoreDescriptor, clock timeutil.TimeSource,
) *storeGossiper {
	sg := &storeGossiper{
		lastIntervalGossip: time.Time{},
		descriptorGetter:   descriptorGetter,
	}

	desc := sg.descriptorGetter(false /* cached */)
	knobs := kvserver.StoreGossipTestingKnobs{AsyncDisabled: true}
	sg.local = kvserver.NewStoreGossip(sg, sg, knobs, &cluster.MakeTestingClusterSettings().SV, clock)
	sg.local.Ident = roachpb.StoreIdent{StoreID: desc.StoreID, NodeID: desc.Node.NodeID}

	return sg
}

var _ kvserver.InfoGossiper = &storeGossiper{}

var _ kvserver.StoreDescriptorProvider = &storeGossiper{}

// AddInfoProto adds or updates an info object in gossip. Returns an error
// if info couldn't be added.
func (sg *storeGossiper) AddInfoProto(key string, msg protoutil.Message, ttl time.Duration) error {
	desc := msg.(*roachpb.StoreDescriptor)
	// NB: We overwrite any pending outbound gossip infos. This behavior is
	// valid as at each tick we will check this field before sending it off to
	// the asim internal gossip exchange; any overwritten gossip infos would
	// also have been overwritten in the same tick at the receiver i.e. last
	// writer in the tick wins.
	sg.pendingOutbound = desc
	return nil
}

// Descriptor returns a StoreDescriptor including current store
// capacity information.
func (sg *storeGossiper) Descriptor(
	ctx context.Context, cached bool,
) (*roachpb.StoreDescriptor, error) {
	desc := sg.descriptorGetter(cached)
	if cached {
		capacity := sg.local.CachedCapacity()
		if capacity != (roachpb.StoreCapacity{}) {
			desc.Capacity = capacity
		}
	}
	return &desc, nil
}

// NewGossip returns an implementation of the Gossip interface.
func NewGossip(s state.State, settings *config.SimulationSettings) *gossip {
	g := &gossip{
		settings:    settings,
		storeGossip: map[state.StoreID]*storeGossiper{},
		exchange: &fixedDelayExchange{
			pending:  []exchangeInfo{},
			settings: settings,
		},
	}
	for _, store := range s.Stores() {
		g.addStoreToGossip(s, store.StoreID())
	}
	s.RegisterCapacityChangeListener(g)
	s.RegisterCapacityListener(g)
	s.RegisterConfigChangeListener(g)
	return g
}

func (g *gossip) addStoreToGossip(s state.State, storeID state.StoreID) {
	// Add the store gossip in an "adding" state initially, this is to avoid
	// recursive calls to get the store descriptor.
	g.storeGossip[storeID] = &storeGossiper{addingStore: true}
	g.storeGossip[storeID] = newStoreGossiper(func(cached bool) roachpb.StoreDescriptor {
		return s.StoreDescriptors(cached, storeID)[0]
	}, s.Clock())
}

// Tick checks for completed gossip updates and triggers new gossip
// updates if needed.
func (g *gossip) Tick(ctx context.Context, tick time.Time, s state.State) {
	stores := s.Stores()
	for _, store := range stores {
		var sg *storeGossiper
		var ok bool
		// If the store gossip for this store doesn't yet exist, create it and
		// add it to the map of store gossips.
		if sg, ok = g.storeGossip[store.StoreID()]; !ok {
			g.addStoreToGossip(s, store.StoreID())
		}

		// If the interval between the last time this store was gossiped for
		// interval and this tick is not less than the gossip interval, then we
		// shoud gossip.
		// NB: In the real code this is controlled by a gossip
		// ticker on the node that activates every 10 seconds.
		if !tick.Before(sg.lastIntervalGossip.Add(g.settings.StateExchangeInterval)) {
			sg.lastIntervalGossip = tick
			_ = sg.local.GossipStore(ctx, false /* useCached */)
		}

		// Put the pending gossip infos into the exchange.
		if sg.pendingOutbound != nil {
			desc := *sg.pendingOutbound
			g.exchange.put(tick, desc)
			// Clear the pending gossip infos for this store.
			sg.pendingOutbound = nil
		}
	}
	// Update with any recently complete gossip infos.
	g.maybeUpdateState(tick, s)
}

// CapacityChangeNotify notifies that a capacity change event has occurred
// for the store with ID StoreID.
func (g *gossip) CapacityChangeNotify(cce kvserver.CapacityChangeEvent, storeID state.StoreID) {
	if sg, ok := g.storeGossip[storeID]; ok {
		if !sg.addingStore {
			sg.local.MaybeGossipOnCapacityChange(context.Background(), cce)
		}
	} else {
		panic(
			fmt.Sprintf("capacity change event but no found store in store gossip with ID %d",
				storeID,
			))
	}
}

// NewCapacityNotify notifies that a new capacity event has occurred for
// the store with ID StoreID.
func (g *gossip) NewCapacityNotify(capacity roachpb.StoreCapacity, storeID state.StoreID) {
	if sg, ok := g.storeGossip[storeID]; ok {
		if !sg.addingStore {
			sg.local.UpdateCachedCapacity(capacity)
		}
	} else {
		panic(
			fmt.Sprintf("new capacity event but no found store in store gossip with ID %d",
				storeID,
			))
	}
}

// StoreAddNotify notifies that a new store has been added with ID storeID.
func (g *gossip) StoreAddNotify(storeID state.StoreID, s state.State) {
	g.addStoreToGossip(s, storeID)
}

func (g *gossip) maybeUpdateState(tick time.Time, s state.State) {
	// NB: The updates function gives back all store descriptors which have
	// completed exchange. We apply the update to every stores state uniformly,
	// i.e. fixed delay.
	updates := g.exchange.updates(tick)
	if len(updates) == 0 {
		return
	}

	updateMap := map[roachpb.StoreID]*storepool.StoreDetail{}
	for _, update := range updates {
		updateMap[update.Desc.StoreID] = update
	}
	for _, store := range s.Stores() {
		s.UpdateStorePool(store.StoreID(), updateMap)
	}
}
