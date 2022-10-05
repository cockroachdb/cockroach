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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

func TestGossip(t *testing.T) {
	settings := config.DefaultSimulationSettings()

	tick := settings.Start
	s := state.NewTestStateEvenDistribution(3, 100, 3, 1000)
	details := map[state.StoreID]*map[roachpb.StoreID]*storepool.StoreDetail{}

	for _, store := range s.Stores() {
		aloc := s.MakeAllocator(store.StoreID())
		storeDetails := aloc.StorePool.DetailsMu.StoreDetails
		details[store.StoreID()] = &storeDetails
	}

	assertStorePool := func(f func(prev, cur *map[roachpb.StoreID]*storepool.StoreDetail)) {
		var prev *map[roachpb.StoreID]*storepool.StoreDetail
		for _, cur := range details {
			if prev != nil {
				f(prev, cur)
			}
			prev = cur
		}
	}

	assertEmptyFn := func(prev, cur *map[roachpb.StoreID]*storepool.StoreDetail) {
		require.Len(t, *prev, 0)
		require.Len(t, *cur, 0)
	}

	assertSameFn := func(prev, cur *map[roachpb.StoreID]*storepool.StoreDetail) {
		require.Equal(t, *prev, *cur)
		require.Equal(t, 3, len(*cur), "cur %+v", *cur)
	}

	gossip := newStoreGossip(settings)
	// The initial storepool state should be empty for each store.
	assertStorePool(assertEmptyFn)

	gossip.Tick(tick, s)
	// The storepool state is still empty, after ticking, since the
	// delay is 1 second.
	assertStorePool(assertEmptyFn)
	// The last interval gossip time for each store should be the current tick.
	for _, lastGossip := range gossip.lastIntervalGossip {
		require.Equal(t, tick, lastGossip)
	}

	// The exchange component should contain three store descriptors, one for
	// each store.
	require.Len(t, gossip.exchange.pending, 3)

	// Add the delay interval and then assert that the storepools for each
	// store are populated.
	tick = tick.Add(settings.StateExchangeDelay)
	gossip.Tick(tick, s)

	// The exchange component should now be empty, clearing the previous
	// gossiped descriptors.
	require.Len(t, gossip.exchange.pending, 0)
	assertStorePool(assertSameFn)

	// Update the usage info leases for s1 and s2, so that it exceeds the delta
	// required to trigger a gossip update. We do this by transferring every
	// lease to s2.
	prevLeaseCount := s.StoreDescriptors(1)[0].Capacity.LeaseCount
	for _, rng := range s.Ranges() {
		s.TransferLease(rng.RangeID(), 2)
	}
	gossip.Tick(tick, s)
	// There should be just store 1 and 2 pending gossip updates in the exchanger.
	require.Len(t, gossip.exchange.pending, 2)

	// Increment the tick and check that the updated lease count information
	// reached each storepool.
	tick = tick.Add(settings.StateExchangeDelay)
	gossip.Tick(tick, s)

	require.Len(t, gossip.exchange.pending, 0)
	// NB: If all the storepools are identical, we only need to check one
	// stores to ensure it matches expectation.
	assertStorePool(assertSameFn)
	require.Equal(t, int32(0), (*details[1])[1].Desc.Capacity.LeaseCount)
	require.Equal(t, prevLeaseCount, (*details[1])[2].Desc.Capacity.LeaseCount)
}
