// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package asim

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/history"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/op"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/queue"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/scheduled"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/storerebalancer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Simulator simulates an entire cluster, and runs the allocator of each store
// in that cluster.
type Simulator struct {
	log.AmbientContext
	curr time.Time
	end  time.Time
	// interval is the step between ticks for active simulaton components, such
	// as the queues, store rebalancer and state changers. It should be set
	// lower than the bgInterval, as updated occur more frequently.
	interval time.Duration

	// The simulator can run multiple workload Generators in parallel.
	generators    []workload.Generator
	eventExecutor scheduled.EventExecutor

	pacers map[state.StoreID]queue.ReplicaPacer

	// Store replicate queues.
	rqs map[state.StoreID]queue.RangeQueue
	// Store lease queues.
	lqs map[state.StoreID]queue.RangeQueue
	// Store split queues.
	sqs map[state.StoreID]queue.RangeQueue
	// Store rebalancers.
	srs map[state.StoreID]storerebalancer.StoreRebalancer
	// Store operation controllers.
	controllers map[state.StoreID]op.Controller

	state    state.State
	changer  state.Changer
	gossip   gossip.Gossip
	shuffler func(n int, swap func(i, j int))

	settings *config.SimulationSettings

	metrics *metrics.Tracker
	history history.History
}

func (s *Simulator) Curr() time.Time {
	return s.curr
}

func (s *Simulator) State() state.State {
	return s.state
}

func (s *Simulator) EventExecutor() scheduled.EventExecutor {
	return s.eventExecutor
}

// NewSimulator constructs a valid Simulator.
func NewSimulator(
	duration time.Duration,
	wgs []workload.Generator,
	initialState state.State,
	settings *config.SimulationSettings,
	m *metrics.Tracker,
	eventExecutor scheduled.EventExecutor,
) *Simulator {
	pacers := make(map[state.StoreID]queue.ReplicaPacer)
	rqs := make(map[state.StoreID]queue.RangeQueue)
	lqs := make(map[state.StoreID]queue.RangeQueue)
	sqs := make(map[state.StoreID]queue.RangeQueue)
	srs := make(map[state.StoreID]storerebalancer.StoreRebalancer)
	changer := state.NewReplicaChanger()
	controllers := make(map[state.StoreID]op.Controller)

	s := &Simulator{
		AmbientContext: log.MakeTestingAmbientCtxWithNewTracer(),
		curr:           settings.StartTime,
		end:            settings.StartTime.Add(duration),
		interval:       settings.TickInterval,
		generators:     wgs,
		state:          initialState,
		changer:        changer,
		rqs:            rqs,
		lqs:            lqs,
		sqs:            sqs,
		controllers:    controllers,
		srs:            srs,
		pacers:         pacers,
		gossip:         gossip.NewGossip(initialState, settings),
		metrics:        m,
		shuffler:       state.NewShuffler(settings.Seed),
		// TODO(kvoli): Keeping the state around is a bit hacky, find a better
		// method of reporting the ranges.
		history:       history.History{Recorded: [][]metrics.StoreMetrics{}, S: initialState},
		eventExecutor: eventExecutor,
		settings:      settings,
	}

	for _, store := range initialState.Stores() {
		storeID := store.StoreID()
		s.addStore(storeID, settings.StartTime)
	}
	s.state.RegisterConfigChangeListener(s)

	m.Register(&s.history)
	s.AddLogTag("asim", nil)
	return s
}

// StoreAddNotify notifies that a new store has been added with ID storeID.
func (s *Simulator) StoreAddNotify(storeID state.StoreID, _ state.State) {
	s.addStore(storeID, s.curr)
}

func (s *Simulator) addStore(storeID state.StoreID, tick time.Time) {
	allocator := s.state.MakeAllocator(storeID)
	storePool := s.state.StorePool(storeID)
	s.rqs[storeID] = queue.NewReplicateQueue(
		storeID,
		s.changer,
		s.settings,
		allocator,
		storePool,
		tick,
	)
	s.lqs[storeID] = queue.NewLeaseQueue(
		storeID,
		s.changer,
		s.settings,
		allocator,
		storePool,
		tick,
	)
	s.sqs[storeID] = queue.NewSplitQueue(
		storeID,
		s.changer,
		s.settings,
		tick,
	)
	s.pacers[storeID] = queue.NewScannerReplicaPacer(
		s.state.NextReplicasFn(storeID),
		s.settings,
	)
	s.controllers[storeID] = op.NewController(
		s.changer,
		allocator,
		storePool,
		s.settings,
		storeID,
	)
	s.srs[storeID] = storerebalancer.NewStoreRebalancer(
		tick,
		storeID,
		s.controllers[storeID],
		allocator,
		storePool,
		s.settings,
		storerebalancer.GetStateRaftStatusFn(s.state),
	)
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

// History returns the current recorded history of a simulation run. Calling
// this on a Simulator that has not begun will return an empty history.
func (s *Simulator) History() history.History {
	return s.history
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

		s.AddLogTag("tick", tick.Format(time.StampMilli))
		ctx = s.AmbientContext.AnnotateCtx(ctx)

		// Update the store clocks with the current tick time.
		s.tickStoreClocks(tick)

		// Tick any events.
		s.tickEvents(ctx, tick)

		// Update the state with generated load.
		s.tickWorkload(ctx, tick)

		// Update pending state changes.
		s.tickStateChanges(ctx, tick)

		// Update each allocators view of the stores in the cluster.
		s.tickGossip(ctx, tick)

		// Done with config and load updates, the state is ready for the
		// allocators.
		stateForAlloc := s.state

		// Simulate the replicate queue logic.
		s.tickQueues(ctx, tick, stateForAlloc)

		// Simulate the store rebalancer logic.
		s.tickStoreRebalancers(ctx, tick, stateForAlloc)

		// Print tick metrics.
		s.tickMetrics(ctx, tick)
	}
}

// tickWorkload gets the next workload events and applies them to state.
func (s *Simulator) tickWorkload(ctx context.Context, tick time.Time) {
	s.shuffler(
		len(s.generators),
		func(i, j int) { s.generators[i], s.generators[j] = s.generators[j], s.generators[i] },
	)
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
func (s *Simulator) tickGossip(ctx context.Context, tick time.Time) {
	s.gossip.Tick(ctx, tick, s.state)
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
		// Tick the lease queue.
		s.lqs[storeID].Tick(ctx, tick, state)

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
			// Try adding the replica to the lease queue.
			s.lqs[storeID].MaybeAdd(ctx, r, state)
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

// tickMetrics prints the metrics up to the given tick.
func (s *Simulator) tickMetrics(ctx context.Context, tick time.Time) {
	s.metrics.Tick(ctx, tick, s.state)
}

// tickEvents ticks the registered simulation events.
func (s *Simulator) tickEvents(ctx context.Context, tick time.Time) {
	s.eventExecutor.TickEvents(ctx, tick, s.state, s.history)
}
