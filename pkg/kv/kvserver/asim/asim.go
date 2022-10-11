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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/op"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/queue"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/storerebalancer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
)

// Simulator simulates an entire cluster, and runs the allocator of each store
// in that cluster.
type Simulator struct {
	curr time.Time
	end  time.Time
	// interval is the step between ticks for active simulaton components, such
	// as the queues, store rebalancer and state changers. It should be set
	// lower than the bgInterval, as updated occur more frequently.
	interval time.Duration
	// bgInterval is the step between ticks for background simulation
	// components, such as the metrics, state exchange and workload generators.
	// It should be set higher than the interval, as it is generally more
	// costly and requires less frequent updates.
	bgInterval time.Duration
	bgLastTick time.Time

	// The simulator can run multiple workload Generators in parallel.
	generators []workload.Generator

	pacers map[state.StoreID]queue.ReplicaPacer

	// Store replicate queues.
	rqs map[state.StoreID]queue.RangeQueue
	// Store split queues.
	sqs map[state.StoreID]queue.RangeQueue
	// Store rebalancers.
	srs map[state.StoreID]storerebalancer.StoreRebalancer
	// Store operation controllers.
	controllers map[state.StoreID]op.Controller

	state   state.State
	changer state.Changer
	gossip  gossip.Gossip

	shuffler func(n int, swap func(i, j int))

	metrics *metrics.Tracker
}

// NewSimulator constructs a valid Simulator.
func NewSimulator(
	wgs []workload.Generator,
	initialState state.State,
	settings *config.SimulationSettings,
	metrics *metrics.Tracker,
) *Simulator {
	pacers := make(map[state.StoreID]queue.ReplicaPacer)
	rqs := make(map[state.StoreID]queue.RangeQueue)
	sqs := make(map[state.StoreID]queue.RangeQueue)
	srs := make(map[state.StoreID]storerebalancer.StoreRebalancer)
	controllers := make(map[state.StoreID]op.Controller)
	changer := state.NewReplicaChanger()
	gossip := gossip.NewStoreGossip(
		settings,
	)
	for i, store := range initialState.Stores() {
		storeID := store.StoreID()
		allocator := initialState.MakeAllocator(storeID)
		// TODO(kvoli): Instead of passing in individual settings to construct
		// the each ticking component, pass a pointer to the simulation
		// settings struct. That way, the settings may be adjusted dynamically
		// during a simulation.
		rqs[storeID] = queue.NewReplicateQueue(
			storeID,
			changer,
			allocator,
			settings.Start,
			settings,
		)
		sqs[storeID] = queue.NewSplitQueue(
			storeID,
			changer,
			settings.RangeSplitDelayFn(),
			settings.RangeSizeSplitThreshold,
			settings.Start,
			settings,
		)
		pacers[storeID] = queue.NewScannerReplicaPacer(
			initialState.NextReplicasFn(storeID),
			settings.PacerLoopInterval,
			settings.PacerMinIterInterval,
			settings.PacerMaxIterIterval,
			settings.Seed,
		)
		controllers[storeID] = op.NewController(
			changer,
			allocator,
			settings,
			storeID,
		)
		srs[storeID] = storerebalancer.NewStoreRebalancer(
			// Jitter the start times to avoid thrashing.
			settings.Start.Add(time.Duration(3*i)*time.Second),
			storeID,
			controllers[storeID],
			allocator,
			settings,
			storerebalancer.GetStateRaftStatusFn(initialState),
		)
	}
	gossip.Tick(state.TestingStartTime().Add(-settings.StateExchangeDelay), initialState)

	return &Simulator{
		curr:        settings.Start,
		end:         settings.Start.Add(settings.Duration),
		interval:    settings.Interval,
		bgInterval:  settings.BackgroundInterval,
		generators:  wgs,
		state:       initialState,
		changer:     changer,
		rqs:         rqs,
		sqs:         sqs,
		controllers: controllers,
		srs:         srs,
		pacers:      pacers,
		gossip:      gossip,
		metrics:     metrics,
		shuffler:    state.NewShuffler(settings.Seed),
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
// we apply the load changes such as updating the QPS for replicas, and last,
// we run the actual allocator code. The input for the allocator is the state
// we updated, and the operations recommended by the allocator (rebalances,
// adding/removing replicas, etc.) are applied on a new state. This means that
// the allocators view a stale state without the recent updates form other
// allocators. Note that we are currently ignoring asymmetric gossip delays,
// meaning all allocators view the exact same state in each tick.
//
// TODO: simulation run settings should be loaded from a config such as a yaml
// file or a "datadriven" style file.
func (s *Simulator) RunSim(ctx context.Context) {
	for {
		done, tick := s.GetNextTickTime()
		if done {
			break
		}

		// Update the store clocks with the current tick time.
		s.tickStoreClocks(tick)

		// Update the state with generated load.
		s.tickWorkload(ctx, tick)

		// Update pending state changes.
		s.tickStateChanges(ctx, tick)

		// Update each allocators view of the stores in the cluster.
		s.tickGossip(tick)

		// Done with config and load updates, the state is ready for the
		// allocators.
		stateForAlloc := s.state

		// Simulate the replicate queue logic.
		s.tickQueues(ctx, tick, stateForAlloc)

		// Simulate the store rebalancer logic.
		s.tickStoreRebalancers(ctx, tick, stateForAlloc)

		// Print tick metrics.
		s.tickMetrics(ctx, tick)

		// If we ticked the background tickable components, update the last
		// tick time.
		// TODO(kvoli): Variable component tick rates requires a proper
		// abstraction.
		if !s.bgLastTick.Add(s.bgInterval).After(tick) {
			s.bgLastTick = tick
		}
	}
}

// tickWorkload gets the next workload events and applies them to state.
func (s *Simulator) tickWorkload(ctx context.Context, tick time.Time) {
	s.shuffler(len(s.generators), func(i, j int) { s.generators[i], s.generators[j] = s.generators[j], s.generators[i] })
	for _, generator := range s.generators {
		event := generator.Tick(tick)
		s.state.ApplyLoad(event)
	}
}

// tickStateChanges ticks atomic pending changes, in the changer. Then, for
// each store ticks the pending operations such as relocate range and lease
// transfers.
func (s *Simulator) tickStateChanges(ctx context.Context, tick time.Time) {
	s.changer.Tick(tick, s.state)
	stores := s.state.Stores()
	s.shuffler(len(stores), func(i, j int) { stores[i], stores[j] = stores[j], stores[i] })
	for _, store := range stores {
		s.controllers[store.StoreID()].Tick(ctx, tick, s.state)
	}
}

// tickGossip puts the current tick store descriptors into the state
// exchange. It then updates the exchanged descriptors for each store's store
// pool.
func (s *Simulator) tickGossip(tick time.Time) {
	s.gossip.Tick(tick, s.state)
}

func (s *Simulator) tickStoreClocks(tick time.Time) {
	s.state.TickClock(tick)
}

// tickQueues iterates over the next replicas for each store to
// consider. It then enqueues each of these and ticks the replicate queue for
// processing.
func (s *Simulator) tickQueues(ctx context.Context, tick time.Time, state state.State) {
	stores := s.state.Stores()
	s.shuffler(len(stores), func(i, j int) { stores[i], stores[j] = stores[j], stores[i] })
	for _, store := range stores {
		storeID := store.StoreID()

		// Tick the split queue.
		s.sqs[storeID].Tick(ctx, tick, state)
		// Tick the replicate queue.
		s.rqs[storeID].Tick(ctx, tick, state)

		// Tick changes that may have been enqueued with a lower completion
		// than the current tick, from the queues.
		s.changer.Tick(tick, state)

		// Try adding suggested load splits that are pending for this store.
		for _, rangeID := range state.LoadSplitterFor(storeID).ClearSplitKeys() {
			if r, ok := state.LeaseHolderReplica(rangeID); ok {
				s.sqs[storeID].MaybeAdd(ctx, r, state)
			}
		}

		for {
			r := s.pacers[storeID].Next(tick)
			if r == nil {
				// No replicas to consider at this tick.
				break
			}

			// NB: Only the leaseholder replica for the range is
			// considered in the allocator.
			if !r.HoldsLease() {
				continue
			}

			// Try adding the replica to the split queue.
			s.sqs[storeID].MaybeAdd(ctx, r, state)
			// Try adding the replica to the replicate queue.
			s.rqs[storeID].MaybeAdd(ctx, r, state)
		}
	}
}

// tickStoreRebalancers iterates over the store rebalancers in the cluster and
// ticks their control loop.
func (s *Simulator) tickStoreRebalancers(ctx context.Context, tick time.Time, state state.State) {
	stores := s.state.Stores()
	s.shuffler(len(stores), func(i, j int) { stores[i], stores[j] = stores[j], stores[i] })
	for _, store := range stores {
		s.srs[store.StoreID()].Tick(ctx, tick, state)
	}
}

// tickMetrics updates the metrics up to the given tick.
func (s *Simulator) tickMetrics(ctx context.Context, tick time.Time) {
	if !s.bgLastTick.Add(s.bgInterval).After(tick) {
		s.metrics.Tick(ctx, tick, s.state)
	}
}
