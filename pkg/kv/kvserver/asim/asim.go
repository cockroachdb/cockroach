// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package asim

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// Simulator simulates an entire cluster, and runs the allocator of each store
// in that cluster.
type Simulator struct {
	curr     time.Time
	end      time.Time
	interval time.Duration

	// The simulator can run multiple workload Generators in parallel.
	generators []workload.Generator

	state    state.State
	changer  state.Changer
	pacers   map[state.StoreID]ReplicaPacer
	exchange state.Exchange
}

// NewSimulator constructs a valid Simulator.
func NewSimulator(
	start, end time.Time,
	interval time.Duration,
	wgs []workload.Generator,
	initialState state.State,
	exchange state.Exchange,
	changer state.Changer,
) *Simulator {
	pacers := make(map[state.StoreID]ReplicaPacer)
	for storeID := range initialState.Stores() {
		pacers[storeID] = NewScannerReplicaPacer(
			initialState.NextReplicasFn(storeID),
			defaultLoopInterval,
			defaultMinInterInterval,
			defaultMaxIterInterval,
		)
	}

	return &Simulator{
		curr:       start,
		end:        end,
		interval:   interval,
		generators: wgs,
		state:      initialState,
		changer:    changer,
		pacers:     pacers,
		exchange:   exchange,
	}
}

// GetNextTickTime returns a simulated tick time, or an indication that the
// simulation is done.
func (s *Simulator) GetNextTickTime() (done bool, tick time.Time) {
	s.curr = s.curr.Add(s.interval)
	if s.curr.After(s.end) {
		return true, time.Time{}
	}
	return false, s.curr
}

// RunSim runs a simulation until GetNextTickTime() is done. A simulation is
// executed by "ticks" - we run a full tick and then move to next one. In each
// tick we first apply the state changes such as adding or removing Nodes, then
// we apply the load changes such as updating the QPS for replicas, and last, we
// run the actual allocator code. The input for the allocator is the state we
// updated, and the operations recommended by the allocator (rebalances,
// adding/removing replicas, etc.) are applied on a new state. This means that
// the allocators view a stale state without the recent updates form other
// allocators. Note that we are currently ignoring gossip delays, meaning all
// allocators view the exact same state in each tick.
//
// TODO: simulation run settings should be loaded from a config such as a yaml
// file or a "datadriven" style file.
func (s *Simulator) RunSim(ctx context.Context) {
	for {
		done, tick := s.GetNextTickTime()
		if done {
			break
		}
		// Update the state with generated load.
		s.tickWorkload(ctx, tick)

		// Update pending state changes.
		s.changer.Tick(tick, s.state)

		// Update each allocators view of the stores in the cluster.
		s.tickStateExchange(tick)

		// Update the store clocks with the current tick time.
		s.tickStoreClocks(tick)

		// Done with config and load updates, the state is ready for the
		// allocators. There no allocators at the moment, so instead just loop
		// back.
	}
}

func (s *Simulator) tickWorkload(ctx context.Context, tick time.Time) {
	for _, generator := range s.generators {
		for {
			done, event := generator.GetNext(tick)
			if done {
				break
			}
			s.state.ApplyLoad(event)
		}
	}
}

// tickStateExchange puts the current tick store descriptors into the state
// exchange. It then updates the exchanged descriptors for each store's store
// pool.
func (s *Simulator) tickStateExchange(tick time.Time) {
	storeDescriptors := s.state.StoreDescriptors()
	s.exchange.Put(tick, storeDescriptors...)
	for storeID := range s.state.Stores() {
		s.state.UpdateStorePool(storeID, s.exchange.Get(tick, roachpb.StoreID(storeID)))
	}
}

func (s *Simulator) tickStoreClocks(tick time.Time) {
	s.state.TickClock(tick)
}
