// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gossip

import (
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

const (
	deltaFraction  = 0.01
	deltaMinLeases = 1
	deltaMaxRanges = 3
)

type gossipTriggerStats struct {
	rangeCount, leaseCount int64
}

// Gossip collects and updates the storepools of the cluster upon capacity
// changes and the gossip interval defined in the simulation settings.
type Gossip interface {
	// Tick checks for completed gossip updates and triggers new gossip
	// updates if needed.
	Tick(time.Time, state.State)
	// Gossip gossips an latest store descriptors for the stores with the IDs
	// given.
	Gossip(time.Time, state.State, ...state.StoreID)
}

type storeGossip struct {
	prevMap            map[state.StoreID]gossipTriggerStats
	lastIntervalGossip map[state.StoreID]time.Time
	settings           *config.SimulationSettings
	exchange           *fixedDelayExchange
}

// NewStoreGossip returns an implementation of the Gossip interface.
func NewStoreGossip(settings *config.SimulationSettings) Gossip {
	return newStoreGossip(settings)
}

func newStoreGossip(settings *config.SimulationSettings) *storeGossip {
	return &storeGossip{
		prevMap:            map[state.StoreID]gossipTriggerStats{},
		lastIntervalGossip: map[state.StoreID]time.Time{},
		settings:           settings,
		exchange: &fixedDelayExchange{
			pending:  []exchangeInfo{},
			settings: settings,
		},
	}
}

// Tick checks for completed gossip updates and triggers new gossip
// updates if needed.
func (g *storeGossip) Tick(tick time.Time, s state.State) {
	stores := s.Stores()
	toGossip := []state.StoreID{}
	for _, store := range stores {
		// If the interval between the last time this store was gossiped for
		// interval and this tick is not less than the gossip interval, then we
		// shoud gossip.
		if g.shouldGossipOnInterval(tick, store.StoreID()) {
			toGossip = append(toGossip, store.StoreID())
			g.lastIntervalGossip[store.StoreID()] = tick
			continue
		} else if g.shouldGossipOnCapacityChange(s, store.StoreID()) {
			toGossip = append(toGossip, store.StoreID())
		}
	}

	g.Gossip(tick, s, toGossip...)

	// Update with any recently complete gossip infos.
	g.maybeUpdateState(tick, s)
}

// Gossip gossips an latest store descriptors for the stores with the IDs
// given.
func (g *storeGossip) Gossip(tick time.Time, s state.State, stores ...state.StoreID) {
	for _, storeID := range stores {
		// Update the last capacity trigger stats to reflect the information we
		// gossiped.
		g.prevMap[storeID] = g.currentGossipTriggerStats(s, storeID)
	}

	// Put the updates into the exchanger.
	descs := s.StoreDescriptors(stores...)
	g.exchange.put(tick, descs...)
}

func (g *storeGossip) maybeUpdateState(tick time.Time, s state.State) {
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

func (g *storeGossip) shouldGossipOnInterval(tick time.Time, storeID state.StoreID) bool {
	lastIntervalGossip, ok := g.lastIntervalGossip[storeID]
	return !ok || !tick.Before(lastIntervalGossip.Add(g.settings.StateExchangeInterval))
}

func (g *storeGossip) shouldGossipOnCapacityChange(s state.State, storeID state.StoreID) bool {
	prev, cur := g.prevMap[storeID], g.currentGossipTriggerStats(s, storeID)
	rangeDelta := math.Abs(float64(prev.rangeCount - cur.rangeCount))
	leaseDelta := math.Abs(float64(prev.leaseCount - cur.leaseCount))

	rangeThreshold := math.Ceil(math.Min(float64(prev.rangeCount)*deltaFraction, deltaMaxRanges))
	leaseThreshold := math.Ceil(math.Min(float64(prev.leaseCount)*deltaFraction, deltaMinLeases))

	return rangeDelta > rangeThreshold || leaseDelta > leaseThreshold
}

func (g *storeGossip) currentGossipTriggerStats(
	state state.State, storeID state.StoreID,
) gossipTriggerStats {
	usage := state.ClusterUsageInfo().StoreUsage[storeID]
	return gossipTriggerStats{rangeCount: usage.Replicas, leaseCount: usage.Leases}
}
