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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

// TestTickUpperBound asserts that finding the smallest end time a tick is
// bounded by, given the current end time and the interval behaves correctly.
func TestTickUpperBound(t *testing.T) {
	start := state.TestingStartTime()

	// The cases that can occur are a combination of the remainder of (tick -
	// end) / interval, and whether tick < end. The possible cases are outlined
	// in the descriptions below.
	testCases := []struct {
		desc     string
		tick     time.Time
		end      time.Time
		interval time.Duration
		expected time.Time
	}{
		{
			desc:     "tick = end and (tick - end) % interval = 0",
			tick:     start.Add(time.Second * 10),
			end:      start.Add(time.Second * 10),
			interval: time.Second * 10,
			expected: start.Add(time.Second * 20),
		},
		{
			desc:     "tick > end and (tick - end) % interval > 0",
			tick:     start.Add(time.Second * 15),
			end:      start,
			interval: time.Second * 10,
			expected: start.Add(time.Second * 20),
		},
		{
			desc:     "tick > end and (tick - end) % interval = 0",
			tick:     start.Add(time.Second * 10),
			end:      start,
			interval: time.Second * 10,
			expected: start.Add(time.Second * 20),
		},
		{
			desc:     "tick < end and (tick - end) % interval = 0",
			tick:     start.Add(time.Second * 5),
			end:      start.Add(time.Second * 25),
			interval: time.Second * 10,
			expected: start.Add(time.Second * 15),
		},
		{
			desc:     "tick < end and (tick - end) % interval < 0",
			tick:     start.Add(time.Second * 0),
			end:      start.Add(time.Second * 15),
			interval: time.Second * 10,
			expected: start.Add(time.Second * 5),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.expected, tickUpperBound(tc.tick, tc.end, tc.interval))
		})
	}
}

// TestFixedDelayExchange asserts that:
// Put inserts store descriptors at tick t. These store descriptors will become
// visible to other stores at the same time.
func TestFixedDelayExchange(t *testing.T) {
	start := state.TestingStartTime()
	interval := time.Second * 10
	delay := time.Second * 2

	makeStoreDescriptors := func(stores []int32) []roachpb.StoreDescriptor {
		descriptors := make([]roachpb.StoreDescriptor, len(stores))
		for i := range stores {
			descriptors[i] = roachpb.StoreDescriptor{StoreID: roachpb.StoreID(stores[i])}

		}
		return descriptors
	}

	testCases := []struct {
		desc     string
		stores   []int
		putOrder []int64
		puts     map[int64][]int32
		gets     []int64
		expected map[int64]map[int32]int64
	}{
		{
			desc:     "all stores put",
			putOrder: []int64{1, 15},
			puts: map[int64][]int32{
				1:  {1, 2, 3},
				15: {1, 2, 3},
			},
			expected: map[int64]map[int32]int64{
				11: {},
				12: {1: 1, 2: 1, 3: 1},
				21: {1: 1, 2: 1, 3: 1},
				22: {1: 15, 2: 15, 3: 15},
			},
		},
		{
			desc:     "some stores put",
			putOrder: []int64{1, 15},
			puts: map[int64][]int32{
				1:  {1, 3},
				15: {1, 2},
			},
			expected: map[int64]map[int32]int64{
				11: {},
				12: {1: 1, 3: 1},
				21: {1: 1, 3: 1},
				22: {1: 15, 2: 15, 3: 1},
			},
		},
		{
			desc:     "no stores put",
			puts:     map[int64][]int32{},
			putOrder: []int64{},
			expected: map[int64]map[int32]int64{
				11: {},
				12: {},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			stateExchange := NewFixedDelayExhange(start, interval, delay)
			// Put the store updates for each tick.
			for _, tick := range tc.putOrder {
				stateExchange.Put(state.OffsetTick(start, tick), makeStoreDescriptors(tc.puts[tick])...)
			}
			// Collect the results for the stores, here we expect each store to
			// view a symmetrical map of state.
			results := make(map[int64]map[int32]int64)
			for tick, storeMap := range tc.expected {
				results[tick] = make(map[int32]int64)
				for store := range storeMap {
					storeDetailMap := stateExchange.Get(state.OffsetTick(start, tick), roachpb.StoreID(store))
					for store, storeDetail := range storeDetailMap {
						results[tick][int32(store)] = state.ReverseOffsetTick(start, storeDetail.LastAvailable)
					}
				}
			}
			require.Equal(t, tc.expected, results)

		})
	}
}
