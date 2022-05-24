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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/google/btree"
)

// Range spans keys greater or equal to MinKey and smaller than the MinKey of
// the next range.
type Range struct {
	MinKey      string
	Leaseholder *Replica
	Desc        *roachpb.RangeDescriptor
}

// Less is part of the btree.Item interface.
func (r *Range) Less(than btree.Item) bool {
	return r.MinKey < than.(*Range).MinKey
}

// RangeMap (unlike a regular map) can return the Range responsible for a key.
type RangeMap struct {
	ranges      *btree.BTree
	lastRangeID int
}

// nextRangeID returns the next available ID to assign to a new range, updating
// it's counter. Calls to this method are monotonic.
func (rm *RangeMap) nextRangeID() int {
	rm.lastRangeID++
	return rm.lastRangeID
}

// NewRangeMap returns a valid empty RangeMap.
func NewRangeMap() *RangeMap {
	return &RangeMap{ranges: btree.New(8)}
}

// AddRange adds a range to the RangeMap. The inserted range has it's end key
// updated with it's successor min key if exists. Likewise, updating the
// predecessor range's end key with it's own min key, if a predecessor exists.
// This operation is the same as splitting a range at min key [start, end) into
// [start,minKey) [minKey, end).
func (rm *RangeMap) AddRange(minKey string) *Range {
	r := &Range{MinKey: minKey, Desc: &roachpb.RangeDescriptor{RangeID: roachpb.RangeID(rm.nextRangeID())}}

	endKey := roachpb.RKeyMax
	// Find the sucessor range in the range map, to determine the endkey.
	rm.ranges.AscendGreaterOrEqual(r, func(i btree.Item) bool {
		// The min key already exists in the range map, we cannot return a new
		// range. Instead crash here as this is a bug.
		if !r.Less(i) {
			panic(fmt.Sprintf("Range with minKey: %s already exists within the range map, unable to add new range", r.MinKey))
		}

		successorRange, _ := i.(*Range)
		endKey = roachpb.RKey(successorRange.MinKey)

		return false
	})

	// Find the predecessor range, to update it's endkey to the new range's min
	// key.
	rm.ranges.DescendLessOrEqual(r, func(i btree.Item) bool {
		// The case where the min key already exists cannot occur here, as a
		// panic will have fired in the above iteration.
		predecessorRange, _ := i.(*Range)
		predecessorRange.Desc.EndKey = roachpb.RKey(r.MinKey)
		return false
	})

	r.Desc.EndKey = endKey
	r.Desc.StartKey = roachpb.RKey(r.MinKey)
	rm.ranges.ReplaceOrInsert(r)
	return r
}

// GetRange returns the range that should own the key.
// TODO: guarantee that we have a minimum key, otherwise we might return nil.
func (rm *RangeMap) GetRange(key string) *Range {
	keyToFind := &Range{MinKey: key}
	var rng *Range

	// If keyToFind equals to MinKey of the range, we found the right range, if
	// the range is Less than keyToFind then this is the right range also.
	rm.ranges.DescendLessOrEqual(keyToFind, func(i btree.Item) bool {
		rng = i.(*Range)
		return false
	})
	return rng
}

// Replica represents a replica of a range.
type Replica struct {
	spanConf    *roachpb.SpanConfig
	rangeDesc   *roachpb.RangeDescriptor
	replDesc    *roachpb.ReplicaDescriptor
	ReplicaLoad ReplicaLoad
	leaseHolder bool
}

// Store simulates a store within a node.
type Store struct {
	Replicas  map[int]*Replica
	allocator allocatorimpl.Allocator
	pacer     ReplicaPacer
}

// Node represents a node within the cluster.
// TODO: add regions.
type Node struct {
	nodeDesc *roachpb.NodeDescriptor
	Stores   []*Store
}

// NewNode constructs a valid node.
func NewNode() *Node {
	return &Node{Stores: make([]*Store, 0, 1)}
}

// State offers two ways to access replicas, one is by looking up the node,
// then store, and then all replicas on that store - we use this, for example,
// when running the allocator code for all replicas in a store. And another by
// locating the range in the RangeMap - this is used when applying the load over
// the key space.
type State struct {
	lastNodeID int
	Nodes      map[int]*Node
	Cluster    *ClusterInfo

	// This is the entire key space.
	Ranges *RangeMap
}

// NewState constructs a valid empty state.
func NewState() *State {
	return &State{Nodes: make(map[int]*Node)}
}

// AddNode adds a node with a single store to the cluster.
func (s *State) AddNode() (nodeID int) {
	s.lastNodeID++
	nodeID = s.lastNodeID
	n := NewNode()
	n.nodeDesc = &roachpb.NodeDescriptor{NodeID: roachpb.NodeID(nodeID)}
	s.Nodes[nodeID] = n
	s.AddStore(nodeID)
	return nodeID
}

// AddStore adds a store to an existing node.
// TODO(lidorcarmel,kvoli): Add storeID parameter to support multi-store
// configurations.
func (s *State) AddStore(node int) {
	allocator := allocatorimpl.MakeAllocator(
		nil,
		func(string) (time.Duration, bool) {
			return 0, true
		},
		nil,
	)
	store := &Store{
		allocator: allocator,
		Replicas:  make(map[int]*Replica),
	}
	nextReplsFn := func() []*Replica {
		repls := make([]*Replica, 0, 1)
		for _, repl := range store.Replicas {
			repls = append(repls, repl)
		}
		return repls
	}

	store.pacer = NewScannerReplicaPacer(
		nextReplsFn,
		defaultLoopInterval,
		defaultMinInterInterval,
		defaultMaxIterInterval,
	)

	s.Nodes[node].Stores = append(s.Nodes[node].Stores, store)
}

// AddReplica adds a new replica for a range to the first store on the node.
// TODO: support multiple stores per node.
func (s *State) AddReplica(r *Range, node int) int {
	// The node doesn't exist, we are unable to add a replica to a store on
	// that node.
	if _, ok := s.Nodes[node]; !ok {
		panic(fmt.Sprintf("No node: %d exists, unable to add a replica on range %s", node, r.Desc.RangeID.String()))
	}

	nodeImpl := s.Nodes[node]
	// Initially we assume that there is one store per node.
	desc := r.Desc.AddReplica(roachpb.NodeID(node), roachpb.StoreID(node), roachpb.VOTER_FULL)

	repl := &Replica{
		spanConf:    &roachpb.SpanConfig{},
		rangeDesc:   r.Desc,
		replDesc:    &desc,
		ReplicaLoad: &ReplicaLoadCounter{},
	}

	store := nodeImpl.Stores[0]
	// NB: The store map of replicas is the key value pair range id -> replica.
	//     Adding more than one replica for a range, per store should fail.
	if existing, ok := store.Replicas[int(r.Desc.RangeID)]; ok {
		panic(fmt.Sprintf(
			"replica for range %s already exists on node %d with"+
				"replicaID %s, unable to add a replica",
			r.Desc.RangeID,
			node, existing.replDesc.ReplicaID,
		))
	}

	store.Replicas[int(r.Desc.RangeID)] = repl

	// We set the replica to be the leaseholder if one doesn't already
	// exist.
	if r.Leaseholder == nil {
		r.Leaseholder = repl
		repl.leaseHolder = true
	}
	return int(desc.ReplicaID)
}

// ApplyAllocatorAction updates the state with allocator ops such as
// moving/adding/removing replicas.
func (s *State) ApplyAllocatorAction(
	ctx context.Context, action allocatorimpl.AllocatorAction, priority float64,
) {
}

// ApplyLoad updates the state replicas with the LoadEvent info. These events
// are in the form of "key, read/write, size" and are incrementing counters such
// as QPS for replicas. Note that this means we don't store which keys were
// written and therefore reads never fail.
func (s *State) ApplyLoad(ctx context.Context, le LoadEvent) {
	r := s.Ranges.GetRange(fmt.Sprintf("%d", le.Key))
	// NB: Reads may occur in practice across follower replicas, however these
	// statistics are not currently considered in allocation decisions.
	// Initially we ignore workload on followers.
	// TODO(kvoli): Apply write/read load to followers.
	leaseHolder := r.Leaseholder
	leaseHolder.ReplicaLoad.ApplyLoad(le)
}

// RunAllocator runs the allocator code for some replicas as needed.
func RunAllocator(
	ctx context.Context,
	allocator allocatorimpl.Allocator,
	spanConf roachpb.SpanConfig,
	desc *roachpb.RangeDescriptor,
	tick time.Time,
) (action allocatorimpl.AllocatorAction, priority float64) {
	action, priority = allocator.ComputeAction(ctx, spanConf, desc)
	return action, priority
}

// Simulator simulates an entire cluster, and runs the allocators of each store
// in that cluster.
type Simulator struct {
	curr     time.Time
	end      time.Time
	interval time.Duration

	// The simulator can run multiple workload Generators in parallel.
	generators []WorkloadGenerator
	state      *State
}

// NewSimulator constructs a valid Simulator.
func NewSimulator(
	start, end time.Time, interval time.Duration, wgs []WorkloadGenerator, initialState *State,
) *Simulator {
	return &Simulator{
		curr:       start,
		end:        end,
		interval:   interval,
		generators: wgs,
		state:      initialState,
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

		for _, generator := range s.generators {
			for {
				done, event := generator.GetNext(tick)
				if done {
					break
				}
				s.state.ApplyLoad(ctx, event)
			}
		}

		// Done with config and load updates, the state is ready for the allocators.
		stateForAlloc := s.state

		for _, node := range stateForAlloc.Nodes {
			for _, store := range node.Stores {
				for {
					r := store.pacer.Next(tick)

					// No replicas to consider at this tick.
					if r == nil {
						break
					}

					// NB: Only the leaseholder replica for the range is
					// considered in the allocator.
					if !r.leaseHolder {
						continue
					}

					// Run the real allocator code. Note that the input is from the
					// "frozen" state which is not affected by rebalancing decisions.
					action, priority := RunAllocator(ctx, store.allocator, *r.spanConf, r.rangeDesc, tick)

					// The allocator ops are applied.
					s.state.ApplyAllocatorAction(ctx, action, priority)
				}
			}
		}
	}
}
