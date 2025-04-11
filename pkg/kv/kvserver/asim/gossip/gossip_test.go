// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gossip

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

func TestGossip(t *testing.T) {
	settings := config.DefaultSimulationSettings()

	tick := settings.StartTime
	s := state.NewStateEvenDistribution(3, 100, 3, 1000, settings)
	// Transfer all the leases to store 1 initially.
	for _, rng := range s.Ranges() {
		s.TransferLease(rng.RangeID(), 1)
	}
	details := map[state.StoreID]*syncutil.Map[roachpb.StoreID, storepool.StoreDetailMu]{}

	for _, store := range s.Stores() {
		// Cast the storepool to a concrete storepool type in order to mutate
		// it directly for testing.
		sp := s.StorePool(store.StoreID()).(*storepool.StorePool)
		details[store.StoreID()] = &sp.Details.StoreDetails
	}

	assertStorePool := func(f func(prev, cur *syncutil.Map[roachpb.StoreID, storepool.StoreDetailMu])) {
		var prev *syncutil.Map[roachpb.StoreID, storepool.StoreDetailMu]
		for _, cur := range details {
			if prev != nil {
				f(prev, cur)
			}
			prev = cur
		}
	}

	assertEmptyFn := func(prev, cur *syncutil.Map[roachpb.StoreID, storepool.StoreDetailMu]) {
		prevCount, curCount := 0, 0
		prev.Range(func(key roachpb.StoreID, value *storepool.StoreDetailMu) bool {
			prevCount++
			return true
		})
		cur.Range(func(key roachpb.StoreID, value *storepool.StoreDetailMu) bool {
			curCount++
			return true
		})
		require.Equal(t, 0, prevCount)
		require.Equal(t, 0, curCount)
	}

	assertSameFn := func(prev, cur *syncutil.Map[roachpb.StoreID, storepool.StoreDetailMu]) {
		var prevResults []struct {
			StoreID roachpb.StoreID
			Detail  *storepool.StoreDetailMu
		}
		var curResults []struct {
			StoreID roachpb.StoreID
			Detail  *storepool.StoreDetailMu
		}

		prev.Range(func(key roachpb.StoreID, value *storepool.StoreDetailMu) bool {
			prevResults = append(prevResults, struct {
				StoreID roachpb.StoreID
				Detail  *storepool.StoreDetailMu
			}{
				StoreID: key,
				Detail:  value,
			})
			return true
		})

		cur.Range(func(key roachpb.StoreID, value *storepool.StoreDetailMu) bool {
			curResults = append(curResults, struct {
				StoreID roachpb.StoreID
				Detail  *storepool.StoreDetailMu
			}{
				StoreID: key,
				Detail:  value,
			})
			return true
		})

		// Sort the results by StoreID to ensure that the order is the same.
		sort.Slice(prevResults, func(i, j int) bool {
			return prevResults[i].StoreID < prevResults[j].StoreID
		})
		sort.Slice(curResults, func(i, j int) bool {
			return curResults[i].StoreID < curResults[j].StoreID
		})

		require.Equal(t, prevResults, curResults)
	}

	gossip := NewGossip(s, settings)
	ctx := context.Background()

	// The initial storepool state should be empty for each store.
	assertStorePool(assertEmptyFn)

	gossip.Tick(ctx, tick, s)
	// The storepool state is still empty, after ticking, since the
	// delay is 1 second.
	assertStorePool(assertEmptyFn)
	// The last interval gossip time for each store should be the current tick.
	for _, sg := range gossip.storeGossip {
		require.Equal(t, tick, sg.lastIntervalGossip)
	}

	// The exchange component should contain three store descriptors, one for
	// each store.
	require.Len(t, gossip.exchange.pending, 3)

	// Add the delay interval and then assert that the storepools for each
	// store are populated.
	tick = tick.Add(settings.StateExchangeDelay)
	gossip.Tick(ctx, tick, s)

	// The exchange component should now be empty, clearing the previous
	// gossiped descriptors.
	require.Len(t, gossip.exchange.pending, 0)
	assertStorePool(assertSameFn)

	// Tick state by a large duration to ensure the below capacity changes don't
	// run into the max gossip frequency limit.
	storeTick := tick

	// Update the usage info leases for s1 and s2, so that it exceeds the delta
	// required to trigger a gossip update. We do this by transferring every
	// lease to s2.
	for _, rng := range s.Ranges() {
		s.TransferLease(rng.RangeID(), 2)
		storeTick = storeTick.Add(3 * time.Second)
		s.TickClock(storeTick)
	}
	gossip.Tick(ctx, tick, s)
	// There should be just store 1 and 2 pending gossip updates in the exchanger.
	require.Len(t, gossip.exchange.pending, 2)
	// Increment the tick and check that the updated lease count information
	// reached each storepool.
	tick = tick.Add(settings.StateExchangeDelay)
	gossip.Tick(ctx, tick, s)
	require.Len(t, gossip.exchange.pending, 0)

	// NB: If all the storepools are identical, we only need to check one
	// stores to ensure it matches expectation.
	assertStorePool(assertSameFn)
	// Assert that the lease counts are as expected after transferring all of
	// the leases to s2.
	val, ok := (*details[1]).Load(1)
	require.True(t, ok)
	require.Equal(t, int32(0), val.Desc.Capacity.LeaseCount)
	// Depending on the capacity delta threshold, s2 may not have gossiped
	// exactly when it reached 100 leases, as it earlier gossiped at 90+ leases,
	// so 100 may be < lastGossip * capacityDeltaThreshold, not triggering
	// gossip. Assert that the lease count gossiped is at least 90.
	val, ok = (*details[1]).Load(2)
	require.True(t, ok)
	require.Greater(t, val.Desc.Capacity.LeaseCount, int32(90))

	val, ok = (*details[1]).Load(3)
	require.True(t, ok)
	require.Equal(t, int32(0), val.Desc.Capacity.LeaseCount)
}
