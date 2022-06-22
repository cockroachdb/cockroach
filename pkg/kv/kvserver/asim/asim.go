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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Simulator simulates an entire cluster, and runs the allocator of each store
// in that cluster.
type Simulator struct {
	curr     time.Time
	end      time.Time
	interval time.Duration

	// The simulator can run multiple workload Generators in parallel.
	generators []workload.Generator

	pacers map[state.StoreID]ReplicaPacer

	// Store replicate queues.
	rqs map[state.StoreID]RangeQueue
	// Store split queues.
	sqs map[state.StoreID]RangeQueue

	state    state.State
	changer  state.Changer
	exchange state.Exchange

	metrics *MetricsTracker
}

// NewSimulator constructs a valid Simulator.
func NewSimulator(
	start, end time.Time,
	interval time.Duration,
	wgs []workload.Generator,
	initialState state.State,
	exchange state.Exchange,
	changer state.Changer,
	settings *config.SimulationSettings,
	metrics *MetricsTracker,
) *Simulator {
	pacers := make(map[state.StoreID]ReplicaPacer)
	rqs := make(map[state.StoreID]RangeQueue)
	sqs := make(map[state.StoreID]RangeQueue)
	for storeID := range initialState.Stores() {
		rqs[storeID] = NewReplicateQueue(
			storeID,
			changer,
			settings.ReplicaChangeDelayFn(),
			initialState.MakeAllocator(storeID),
			start,
		)
		sqs[storeID] = NewSplitQueue(
			storeID,
			changer,
			settings.RangeSplitDelayFn(),
			settings.RangeSizeSplitThreshold,
			start,
		)
		pacers[storeID] = NewScannerReplicaPacer(
			initialState.NextReplicasFn(storeID),
			settings.PacerLoopInterval,
			settings.PacerMinIterInterval,
			settings.PacerMaxIterIterval,
		)
	}

	return &Simulator{
		curr:       start,
		end:        end,
		interval:   interval,
		generators: wgs,
		state:      initialState,
		changer:    changer,
		rqs:        rqs,
		sqs:        sqs,
		pacers:     pacers,
		exchange:   exchange,
		metrics:    metrics,
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
		// Update the state with generated load.
		s.tickWorkload(ctx, tick)

		// Update pending state changes.
		s.changer.Tick(tick, s.state)

		// Update each allocators view of the stores in the cluster.
		s.tickStateExchange(tick)

		// Update the store clocks with the current tick time.
		s.tickStoreClocks(tick)

		// Done with config and load updates, the state is ready for the
		// allocators.
		stateForAlloc := s.state

		// Simulate the replicate queue logic.
		s.tickQueues(ctx, tick, stateForAlloc)

		// Print tick metrics.
		s.tickMetrics(ctx, tick)
	}
}

// tickWorkload gets the next workload events and applies them to state.
func (s *Simulator) tickWorkload(ctx context.Context, tick time.Time) {
	for _, generator := range s.generators {
		event := generator.Tick(tick)
		s.state.ApplyLoad(event)
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

// tickQueues iterates over the next replicas for each store to
// consider. It then enqueues each of these and ticks the replicate queue for
// processing.
func (s *Simulator) tickQueues(ctx context.Context, tick time.Time, state state.State) {
	for storeID := range state.Stores() {

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

// tickMetrics prints the metrics up to the given tick.
func (s *Simulator) tickMetrics(ctx context.Context, tick time.Time) {
	if err := s.metrics.Tick(tick, s.state); err != nil {
		log.Errorf(ctx, "error writing to csv: %v", err)
	}
}
