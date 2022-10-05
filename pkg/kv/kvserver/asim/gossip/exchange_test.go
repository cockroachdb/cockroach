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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

func TestFixedDelayExchange(t *testing.T) {
	makeStoresFn := func(stores []int32) []roachpb.StoreDescriptor {
		descriptors := make([]roachpb.StoreDescriptor, len(stores))
		for i := range stores {
			descriptors[i] = roachpb.StoreDescriptor{StoreID: roachpb.StoreID(stores[i])}

		}
		return descriptors
	}

	settings := config.DefaultSimulationSettings()
	tick := settings.Start
	exchange := fixedDelayExchange{pending: []exchangeInfo{}, settings: settings}

	// There should be no updates initially.
	require.Len(t, exchange.updates(tick), 0)

	// Put an update at the current tick.
	exchange.put(tick, makeStoresFn([]int32{1, 2, 3})...)
	require.Len(t, exchange.pending, 3)

	// There should be no updates until after the tick + state exchange delay.
	halfTick := tick.Add(settings.StateExchangeDelay / 2)
	require.Len(t, exchange.updates(halfTick), 0)

	// Update the tick to be >= tick + delay, there should be three updates.
	tick = tick.Add(settings.StateExchangeDelay)
	require.Len(t, exchange.updates(tick), 3)
	require.Len(t, exchange.pending, 0)
}
