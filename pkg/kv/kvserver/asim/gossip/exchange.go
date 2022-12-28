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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// Exchange controls the dissemination of a store's state, to every other
// store in a simulation. The contract requires that:
// (1) Single value per tick: Multiple puts at tick t, with desc d should
//
//	provide an identical d to all other puts for d.
//
// (2) Calls to Put are monotonic w.r.t tick value passed in.
type Exchange interface {
	// Put inserts store state(s) at tick t. This state will be visible to
	// other stores at potentially different times t' >= t.
	Put(tick time.Time, descs ...roachpb.StoreDescriptor)

	// Get retrieves a store's view of the state exchange at tick t.
	// The view returned may not be the same for each store at t.
	Get(tick time.Time, StoreID roachpb.StoreID) map[roachpb.StoreID]*storepool.StoreDetail
}

// FixedDelayExchange implements state exchange. It groups puts at an interval,
// selecting the last put for each descriptor within an interval. Updates
// propagate at a fixed number of intervals, such that a get at time t, will
// retrieve the largest tick t' less than t - delay.
//
// TODO(kvoli,lidorcarmel): Implement gossip exchange that provides a
// configuration similar to pkg/gossip.
type FixedDelayExchange struct {
	end         time.Time
	interval    time.Duration
	delay       time.Duration
	intervalMap map[int64]*map[roachpb.StoreID]*storepool.StoreDetail
}

// NewFixedDelayExhange returns a state delay exchange with fixed delay.
func NewFixedDelayExhange(start time.Time, interval, delay time.Duration) *FixedDelayExchange {
	return &FixedDelayExchange{
		end:         start.Add(interval),
		interval:    interval,
		delay:       delay,
		intervalMap: make(map[int64]*map[roachpb.StoreID]*storepool.StoreDetail),
	}
}

// Put inserts store state(s) at tick t. This state will be visible to
// all other stores at a fixed delay.
func (u *FixedDelayExchange) Put(tick time.Time, descs ...roachpb.StoreDescriptor) {
	prevInterval, ok := u.intervalMap[u.end.Unix()]
	if !ok {
		tmpInterval := make(map[roachpb.StoreID]*storepool.StoreDetail)
		prevInterval = &tmpInterval
	}

	u.maybeUpdateInterval(tick)
	curInterval, ok := u.intervalMap[u.end.Unix()]
	if !ok {
		// Copy the previous store map, so that older exchanged configurations
		// appear in the latest tick, when queried. This could be updated to a
		// linked list or btree if copy performance affects runtime
		// significantly.
		prevIntervalCopy := make(map[roachpb.StoreID]*storepool.StoreDetail)
		for _, storeDetail := range *prevInterval {
			storeDescCopy := *storeDetail.Desc
			storeDetailCopy := *storeDetail
			storeDetailCopy.Desc = &storeDescCopy
			prevIntervalCopy[storeDetail.Desc.StoreID] = &storeDetailCopy
		}
		u.intervalMap[u.end.Unix()] = &prevIntervalCopy
		curInterval = u.intervalMap[u.end.Unix()]
	}

	for _, d := range descs {
		desc := d
		nextStoreDetail := storepool.StoreDetail{}
		nextStoreDetail.Desc = &desc
		nextStoreDetail.LastAvailable = tick
		nextStoreDetail.LastUpdatedTime = tick
		nextStoreDetail.Desc.Node = desc.Node

		(*curInterval)[desc.StoreID] = &nextStoreDetail
	}
}

// Get retrieves a store's view of the state exchange at tick t. In this
// simple implementation of state exchange, all stores have a symmetrical
// view of the others state.
func (u *FixedDelayExchange) Get(
	tick time.Time, storeID roachpb.StoreID,
) map[roachpb.StoreID]*storepool.StoreDetail {
	delayedEnd := tickUpperBound(tick.Add(-u.delay-u.interval), u.end, u.interval)
	if state, ok := u.intervalMap[delayedEnd.Unix()]; ok {
		return *state
	}
	return make(map[roachpb.StoreID]*storepool.StoreDetail)
}

// maybeUpdateInterval compares the tick given to the current interval's end
// time. When tick < intervalEnd, do nothing. When tick >= intervalEnd, update
// intervalEnd to be the smallest time that contains tick, by adding interval
// repeatedly to it.
func (u *FixedDelayExchange) maybeUpdateInterval(tick time.Time) {
	if tick.Add(u.interval).Before(u.end) {
		panic(fmt.Sprintf(
			"Only monotonic calls to fixed delay exchange are allowed for puts. "+
				"Lowest acceptable tick is greater than given (%d > %d)",
			u.end.Add(-u.interval).UTC().Second(), tick.UTC().Second()),
		)
	}
	// Tick is in the current period, no need to update the end interval.
	if tick.Before(u.end) {
		return
	}
	u.end = tickUpperBound(tick, u.end, u.interval)
}

// tickUpperBound returns the smallest interval end time, given the current end
// time and interval duration, that is greater than tick.
func tickUpperBound(tick, end time.Time, interval time.Duration) time.Time {
	delta := tick.Sub(end)
	intervalDelta := delta / interval
	if delta%interval == 0 || !tick.Before(end) {
		intervalDelta++
	}
	return end.Add(intervalDelta * interval)
}
