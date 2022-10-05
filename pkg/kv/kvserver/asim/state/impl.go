// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package state

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/google/btree"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/tracker"
)

type state struct {
	nodes       map[NodeID]*node
	stores      map[StoreID]*store
	load        map[RangeID]ReplicaLoad
	loadsplits  map[StoreID]LoadSplitter
	ranges      *rmap
	clusterinfo ClusterInfo
	usageInfo   *ClusterUsageInfo
	clock       *ManualSimClock
	settings    *config.SimulationSettings

	// Unique ID generators for Nodes and Stores. These are incremented
	// pre-assignment. So that IDs start from 1.
	nodeSeqGen  NodeID
	storeSeqGen StoreID
}

// NewState returns an implementation of the State interface.
func NewState(settings *config.SimulationSettings) State {
	return newState(settings)
}

func newState(settings *config.SimulationSettings) *state {
	s := &state{
		nodes:      make(map[NodeID]*node),
		stores:     make(map[StoreID]*store),
		loadsplits: make(map[StoreID]LoadSplitter),
		clock:      &ManualSimClock{nanos: settings.Start.UnixNano()},
		ranges:     newRMap(),
		usageInfo:  newClusterUsageInfo(),
		settings:   settings,
	}
	s.load = map[RangeID]ReplicaLoad{FirstRangeID: NewReplicaLoadCounter(s.clock)}
	return s
}

type rmap struct {
	// NB: Both rangeTree and rangeMap hold references to ranges. They must
	// both be updated on insertion and deletion to maintain consistent state.
	rangeTree *btree.BTree
	rangeMap  map[RangeID]*rng

	// Unique ID generator for Ranges.
	rangeSeqGen RangeID
}

func newRMap() *rmap {
	rmap := &rmap{
		rangeTree: btree.New(8),
		rangeMap:  make(map[RangeID]*rng),
	}

	rmap.initFirstRange()
	return rmap
}

// Less is part of the btree.Item interface.
func (r *rng) Less(than btree.Item) bool {
	return r.startKey < than.(*rng).startKey
}

// initFirstRange initializes the first range within the rangemap, with
// [MinKey, MaxKey) start and end key. All other ranges are split from this.
func (rm *rmap) initFirstRange() {
	rm.rangeSeqGen++
	rangeID := rm.rangeSeqGen
	desc := roachpb.RangeDescriptor{
		RangeID:       roachpb.RangeID(rangeID),
		StartKey:      MinKey.ToRKey(),
		EndKey:        MaxKey.ToRKey(),
		NextReplicaID: 1,
	}
	rng := &rng{
		rangeID:     rangeID,
		startKey:    MinKey,
		endKey:      MaxKey,
		desc:        desc,
		config:      defaultSpanConfig,
		replicas:    make(map[StoreID]*replica),
		leaseholder: 0,
	}

	rm.rangeTree.ReplaceOrInsert(rng)
	rm.rangeMap[rangeID] = rng
}

// String returns a string containing a compact representation of the state.
// TODO(kvoli,lidorcarmel): Add a unit test for this function.
func (s *state) String() string {
	builder := &strings.Builder{}

	orderedRanges := []*rng{}
	s.ranges.rangeTree.Ascend(func(i btree.Item) bool {
		r := i.(*rng)
		orderedRanges = append(orderedRanges, r)
		return !r.desc.EndKey.Equal(MaxKey.ToRKey())
	})

	nStores := len(s.stores)
	iterStores := 0
	builder.WriteString(fmt.Sprintf("stores(%d)=[", nStores))
	for _, store := range s.stores {
		builder.WriteString(store.String())
		if iterStores < nStores-1 {
			builder.WriteString(",")
		}
		iterStores++
	}
	builder.WriteString("] ")

	nRanges := len(orderedRanges)
	iterRanges := 0
	builder.WriteString(fmt.Sprintf("ranges(%d)=[", nRanges))
	for _, r := range orderedRanges {
		builder.WriteString(r.String())
		if iterRanges < nRanges-1 {
			builder.WriteString(",")
		}
		iterRanges++
	}
	builder.WriteString("]")

	return builder.String()
}

func (s *state) ClusterInfo() ClusterInfo {
	return s.clusterinfo
}

// Stores returns all stores that exist in this state.
func (s *state) Stores() []Store {
	stores := []Store{}
	for _, store := range s.stores {
		stores = append(stores, store)
	}
	sort.Slice(stores, func(i, j int) bool { return stores[i].StoreID() < stores[j].StoreID() })
	return stores
}

// StoreDescriptors returns the descriptors for all stores that exist in
// this state.
func (s *state) StoreDescriptors(storeIDs ...StoreID) []roachpb.StoreDescriptor {
	storeDescriptors := []roachpb.StoreDescriptor{}
	for _, storeID := range storeIDs {
		if store, ok := s.stores[storeID]; ok {
			s.updateStoreCapacity(storeID)
			storeDescriptors = append(storeDescriptors, store.desc)
		}
	}
	return storeDescriptors
}

// Store returns the Store with ID StoreID. This fails if no Store exists
// with ID StoreID.
func (s *state) Store(storeID StoreID) (Store, bool) {
	store, ok := s.stores[storeID]
	return store, ok
}

// Nodes returns all nodes that exist in this state.
func (s *state) Nodes() []Node {
	nodes := []Node{}
	for _, node := range s.nodes {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID() < nodes[j].NodeID() })
	return nodes
}

// RangeFor returns the range containing Key in [StartKey, EndKey). This
// cannot fail.
func (s *state) RangeFor(key Key) Range {
	return s.rangeFor(key)
}

// rangeFor is an internal method to find the range for key.
func (s *state) rangeFor(key Key) *rng {
	keyToFind := &rng{startKey: key}
	var r *rng
	// If keyToFind equals to MinKey of the range, we found the right range, if
	// the range is less than keyToFind then this is the right range also.
	s.ranges.rangeTree.DescendLessOrEqual(keyToFind, func(i btree.Item) bool {
		r = i.(*rng)
		return false
	})
	return r
}

// Range returns the range with ID RangeID. This fails if no Range exists
// with ID RangeID.
func (s *state) Range(rangeID RangeID) (Range, bool) {
	return s.rng(rangeID)
}

func (s *state) rng(rangeID RangeID) (*rng, bool) {
	r, ok := s.ranges.rangeMap[rangeID]
	return r, ok
}

// Ranges returns all ranges that exist in this state.
func (s *state) Ranges() []Range {
	ranges := []Range{}
	for _, r := range s.ranges.rangeMap {
		ranges = append(ranges, r)
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].RangeID() < ranges[j].RangeID() })
	return ranges
}

func (s *state) RangeCount() int64 {
	return int64(len(s.ranges.rangeMap))
}

type replicaList []Replica

func (r replicaList) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r replicaList) Len() int {
	return len(r)
}

func (r replicaList) Less(i, j int) bool {
	return r[i].Range() < r[j].Range()
}

// Replicas returns all replicas that exist on a store.
func (s *state) Replicas(storeID StoreID) []Replica {
	replicas := []Replica{}
	store, ok := s.stores[storeID]
	if !ok {
		return replicas
	}

	repls := replicaList{}
	for rangeID := range store.replicas {
		rng := s.ranges.rangeMap[rangeID]
		if replica := rng.replicas[storeID]; replica != nil {
			repls = append(repls, replica)
		}
	}
	sort.Sort(repls)

	return repls
}

// AddNode modifies the state to include one additional node. This cannot
// fail. The new Node is returned.
func (s *state) AddNode() Node {
	s.nodeSeqGen++
	nodeID := s.nodeSeqGen
	node := &node{
		nodeID: nodeID,
		desc:   roachpb.NodeDescriptor{NodeID: roachpb.NodeID(nodeID)},
		stores: []StoreID{},
	}
	s.nodes[nodeID] = node
	return node
}

// AddStore modifies the state to include one additional store on the Node
// with ID NodeID. This fails if no Node exists with ID NodeID.
func (s *state) AddStore(nodeID NodeID) (Store, bool) {
	if !s.CanAddStore(nodeID) {
		return nil, false
	}
	return s.addStore(nodeID), true
}

func (s *state) addStore(nodeID NodeID) Store {
	node := s.nodes[nodeID]
	s.storeSeqGen++
	storeID := s.storeSeqGen
	store := &store{
		storeID:   storeID,
		nodeID:    nodeID,
		desc:      roachpb.StoreDescriptor{StoreID: roachpb.StoreID(storeID), Node: node.Descriptor()},
		storepool: NewStorePool(s.NodeCountFn(), s.NodeLivenessFn(), hlc.NewClock(s.clock, 0)),
		replicas:  make(map[RangeID]ReplicaID),
	}

	// Commit the new store to state.
	node.stores = append(node.stores, storeID)
	s.stores[storeID] = store

	// Add a range load splitter for this store.
	s.loadsplits[storeID] = NewSplitDecider(
		s.settings.Seed,
		s.settings.SplitQPSThresholdFn(),
		s.settings.SplitQPSRetentionFn(),
	)

	// Add a usage info struct.
	_ = s.usageInfo.storeRef(storeID)

	return store
}

// CanAddStore returns true if it is possible to add a store to the node with
// ID NodeID, otherwise false.
func (s *state) CanAddStore(nodeID NodeID) bool {
	if _, ok := s.nodes[nodeID]; !ok {
		return false
	}
	return true
}

// AddReplica modifies the state to include one additional range for the
// Range with ID RangeID, placed on the Store with ID StoreID. This fails
// if a Replica for the Range already exists the Store.
func (s *state) AddReplica(rangeID RangeID, storeID StoreID) (Replica, bool) {
	// Check whether it is possible to add the replica.
	if !s.CanAddReplica(rangeID, storeID) {
		return nil, false
	}
	return s.addReplica(rangeID, storeID), true

}
func (s *state) addReplica(rangeID RangeID, storeID StoreID) *replica {
	store := s.stores[storeID]
	nodeID := store.nodeID
	rng, ok := s.rng(rangeID)
	if !ok {
		panic(
			fmt.Sprintf("programming error: attemtpted to add replica for a range=%d that doesn't exist",
				rangeID))
	}

	desc := rng.desc.AddReplica(roachpb.NodeID(nodeID), roachpb.StoreID(storeID), roachpb.VOTER_FULL)
	replica := &replica{
		replicaID: ReplicaID(desc.ReplicaID),
		storeID:   storeID,
		rangeID:   rangeID,
		desc:      desc,
	}

	store.replicas[rangeID] = replica.replicaID
	rng.replicas[storeID] = replica

	// Update the usage info.
	s.usageInfo.storeRef(storeID).Replicas++

	// This is the first replica to be added for this range. Make it the
	// leaseholder as a placeholder. The caller can update the lease, however
	// we want to ensure that for any range that has replicas, a leaseholder
	// exists at all times.
	if len(rng.replicas) == 1 {
		// NB: We don't call replaceLeaseHolder because that assumes a prior
		// leaseholder exists. This call cannot fail.
		s.setLeaseholder(rangeID, storeID)
	}

	return replica
}

// CanAddReplica returns whether adding a replica for the Range with ID RangeID
// to the Store with ID StoreID is valid.
func (s *state) CanAddReplica(rangeID RangeID, storeID StoreID) bool {
	// The range doesn't exist.
	if _, ok := s.rng(rangeID); !ok {
		return false
	}
	// The store doesn't exist.
	if _, ok := s.Store(storeID); !ok {
		return false
	}
	// If checking a valid add target, then a replica must not already exist on
	// the store. If checking a valid remove target, then a replica must exist
	// on the store.
	_, ok := s.stores[storeID].replicas[rangeID]
	return !ok
}

// CanRemoveReplica returns whether removing a replica for the Range with ID
// RangeID from the Store with ID StoreID is valid.
func (s *state) CanRemoveReplica(rangeID RangeID, storeID StoreID) bool {
	// The range doesn't exist.
	if _, ok := s.rng(rangeID); !ok {
		return false
	}
	// The store doesn't exist.
	if _, ok := s.Store(storeID); !ok {
		return false
	}
	// When checking a valid remove target, then a replica must exist on the
	// store.
	if replica, ok := s.stores[storeID].replicas[rangeID]; ok {
		// For remove targets, it cannot be the current leaseholder. A lease
		// transfer must occur before attempting to remove it.
		rng, _ := s.Range(rangeID)
		return rng.Leaseholder() != replica
	}
	return false
}

// RemoveReplica modifies the state to remove a Replica with the ID
// ReplicaID. It fails if this Replica does not exist.
func (s *state) RemoveReplica(rangeID RangeID, storeID StoreID) bool {
	// Check whether it is possible to remove the replica.
	if !s.CanRemoveReplica(rangeID, storeID) {
		return false
	}

	return s.removeReplica(rangeID, storeID)
}

func (s *state) removeReplica(rangeID RangeID, storeID StoreID) bool {
	store := s.stores[storeID]
	nodeID := store.nodeID
	rng, ok := s.rng(rangeID)
	if !ok {
		panic(fmt.Sprintf("programming error: attemtpted to remove replica that doesn't exist for range=%d", rangeID))
	}

	if _, ok := rng.desc.RemoveReplica(roachpb.NodeID(nodeID), roachpb.StoreID(storeID)); !ok {
		panic(fmt.Sprintf("programming error: attempted to replica, but it doesn't exist in descriptor for range=%d, desc=%s",
			rangeID, rng.desc),
		)
	}

	delete(store.replicas, rangeID)
	delete(rng.replicas, storeID)
	s.usageInfo.storeRef(storeID).Replicas--
	return true
}

// SetSpanConfig set the span config for the Range with ID RangeID.
func (s *state) SetSpanConfig(rangeID RangeID, spanConfig roachpb.SpanConfig) bool {
	if rng, ok := s.ranges.rangeMap[rangeID]; ok {
		rng.config = spanConfig
		return true
	}
	return false
}

// SplitRange splits the Range which contains Key in [StartKey, EndKey).
// The Range is partitioned into [StartKey, Key), [Key, EndKey) and
// returned. The right hand side of this split, is the new Range. If any
// replicas exist for the old Range [StartKey,EndKey), these become
// replicas of the left hand side [StartKey, Key) and are unmodified. For
// each of these replicas, new replicas are created for the right hand side
// [Key, EndKey), on identical stores to the un-split Range's replicas. This
// fails if the Key given already exists as a StartKey.
func (s *state) SplitRange(splitKey Key) (Range, Range, bool) {
	ranges := s.ranges
	ranges.rangeSeqGen++
	rangeID := s.ranges.rangeSeqGen

	// Create placeholder range that will be populated with any missing fields.
	r := &rng{
		rangeID:     rangeID,
		startKey:    splitKey,
		desc:        roachpb.RangeDescriptor{RangeID: roachpb.RangeID(rangeID), NextReplicaID: 1},
		config:      defaultSpanConfig,
		replicas:    make(map[StoreID]*replica),
		leaseholder: -1,
	}

	endKey := Key(math.MaxInt32)
	failed := false
	// Find the sucessor range in the range map, to determine the endkey.
	ranges.rangeTree.AscendGreaterOrEqual(r, func(i btree.Item) bool {
		// The min key already exists in the range map, we cannot return a new
		// range.
		if !r.Less(i) {
			failed = true
			return false
		}

		successorRange, _ := i.(*rng)
		endKey = successorRange.startKey
		return false
	})

	// A range with startKey=splitKey already exists in the rangeTree, we
	// cannot split at this key again.
	if failed {
		return nil, nil, false
	}

	var predecessorRange *rng
	// Find the predecessor range, to update it's endkey to the new range's min
	// key.
	ranges.rangeTree.DescendLessOrEqual(r, func(i btree.Item) bool {
		// The case where the min key already exists cannot occur here, as the
		// failed flag will have been set above.
		predecessorRange, _ = i.(*rng)
		return false
	})

	// There was no predecessor (LHS), meaning there was no initial range in
	// the rangeTree. In this case we cannot split the range into two.
	if predecessorRange == nil {
		panic(fmt.Sprintf("programming error: no predecessor range found for "+
			"split at %d, missing initial range",
			splitKey),
		)
	}

	// Set the predecessor (LHS) end key to the start key of the split (RHS).
	predecessorRange.endKey = r.startKey
	predecessorRange.desc.EndKey = r.startKey.ToRKey()

	// Set the new range keys.
	r.endKey = endKey
	r.desc.EndKey = endKey.ToRKey()
	r.desc.StartKey = r.startKey.ToRKey()

	// Update the range map state.
	ranges.rangeTree.ReplaceOrInsert(r)
	ranges.rangeMap[r.rangeID] = r

	// Update the range size to be split 50/50 between the lhs and rhs. Also
	// split the replica load that is recorded 50/50 between the lhs and rhs.
	// NB: This is a simplifying assumption.
	predecessorRange.size /= 2
	r.size = predecessorRange.size
	if predecessorLoad, ok := s.load[predecessorRange.rangeID]; ok {
		s.load[r.rangeID] = predecessorLoad.Split()
	}

	// Set the span config to be the same as the predecessor range.
	r.config = predecessorRange.config

	// If there are existing replicas for the LHS of the split, then also
	// create replicas on the same stores for the RHS.
	for _, replica := range predecessorRange.Replicas() {
		storeID := replica.StoreID()
		s.AddReplica(rangeID, storeID)
		if replica.HoldsLease() {
			// The successor range's leaseholder was on this store, copy the
			// leaseholder store over for the new split range.
			leaseholderStore, _ := s.LeaseholderStore(r.rangeID)
			// NB: This operation cannot fail.
			s.replaceLeaseHolder(r.rangeID, storeID, leaseholderStore.StoreID())
			// Reset the recorded load split statistics on the predecessor
			// range.
			s.loadsplits[storeID].ResetRange(predecessorRange.RangeID())
		}
	}

	return predecessorRange, r, true
}

func (s *state) RangeSpan(rangeID RangeID) (Key, Key, bool) {
	rng := s.ranges.rangeMap[rangeID]
	if rng == nil {
		return InvalidKey, InvalidKey, false
	}

	return rng.startKey, rng.endKey, true
}

// TransferLease transfers the lease for the Range with ID RangeID, to the
// Store with ID StoreID. This fails if there is no such Store; or there is
// no such Range; or if the Store doesn't hold a Replica for the Range; or
// if the Replica for the Range on the Store is already the leaseholder.
func (s *state) TransferLease(rangeID RangeID, storeID StoreID) bool {
	if !s.ValidTransfer(rangeID, storeID) {
		return false
	}
	oldStoreID, ok := s.LeaseholderStore(rangeID)
	if !ok {
		return false
	}

	// Reset the load stats on the old range, within the old
	// leaseholder store.
	s.loadsplits[oldStoreID.StoreID()].ResetRange(rangeID)
	s.loadsplits[storeID].ResetRange(rangeID)
	s.load[rangeID].ResetLoad()

	// Apply the lease transfer to state.
	s.replaceLeaseHolder(rangeID, storeID, oldStoreID.StoreID())
	return true
}

func (s *state) replaceLeaseHolder(rangeID RangeID, storeID, oldStoreID StoreID) {
	// Remove the old leaseholder.
	s.removeLeaseholder(rangeID, oldStoreID)
	// Update the range to reflect the new leaseholder.
	s.setLeaseholder(rangeID, storeID)
}

func (s *state) setLeaseholder(rangeID RangeID, storeID StoreID) {
	rng := s.ranges.rangeMap[rangeID]
	rng.replicas[storeID].holdsLease = true
	replicaID := s.stores[storeID].replicas[rangeID]
	rng.leaseholder = replicaID
	s.usageInfo.storeRef(storeID).Leases++
}

func (s *state) removeLeaseholder(rangeID RangeID, storeID StoreID) {
	rng := s.ranges.rangeMap[rangeID]
	if repl, ok := rng.replicas[storeID]; ok {
		if repl.holdsLease {
			repl.holdsLease = false
			s.usageInfo.storeRef(storeID).Leases--
			return
		}
	}
	panic(fmt.Sprintf(
		"programming error: attempted remove a leaseholder that doesn't exist or doesn't "+
			"hold lease for range=%d, store=%d",
		rangeID, storeID),
	)
}

// ValidTransfer returns whether transferring the lease for the Range with ID
// RangeID, to the Store with ID StoreID is valid.
func (s *state) ValidTransfer(rangeID RangeID, storeID StoreID) bool {
	// The store doesn't exist, not a valid transfer target.
	if _, ok := s.Store(storeID); !ok {
		return false
	}
	// The range doesn't exist, not a valid transfer target.
	if _, ok := s.Range(rangeID); !ok {
		return false
	}
	rng, _ := s.Range(rangeID)
	store, _ := s.Store(storeID)
	repl, ok := store.Replica(rangeID)
	// A replica for the range does not exist on the store, we cannot transfer
	// a lease to it.
	if !ok {
		return false
	}
	// The leaseholder replica for the range is already on the store, we can't
	// transfer it to ourselves.
	if repl == rng.Leaseholder() {
		return false
	}
	return true
}

// ApplyLoad modifies the state to reflect the impact of the LoadBatch.
// This modifies specifically the leaseholder replica's RangeUsageInfo for
// the targets of the LoadEvent.
func (s *state) ApplyLoad(lb workload.LoadBatch) {
	n := len(lb)
	if n < 1 {
		return
	}

	// Iterate in descending order over the ranges. LoadBatch keys are in
	// sorted in ascending order, we iterate backwards to also be in descending
	// order. It must be the case that at each range we visit, start key for
	// that range is not larger than the any key of the remaining load events.
	iter := n - 1
	max := &rng{startKey: Key(lb[iter].Key)}
	s.ranges.rangeTree.DescendLessOrEqual(max, func(i btree.Item) bool {
		next, _ := i.(*rng)
		for iter > -1 && lb[iter].Key >= int64(next.startKey) {
			s.applyLoad(next, lb[iter])
			iter--
		}
		return iter > -1
	})
}

func (s *state) applyLoad(rng *rng, le workload.LoadEvent) {
	s.load[rng.rangeID].ApplyLoad(le)
	s.usageInfo.ApplyLoad(rng, le)

	// Note that deletes are not supported currently, we are also assuming data
	// is not compacted.
	rng.size += le.WriteSize

	// Record the load against the splitter for the store which holds a lease
	// for this range, if one exists.
	store, ok := s.LeaseholderStore(rng.rangeID)
	if !ok {
		return
	}
	s.loadsplits[store.StoreID()].Record(s.clock.Now(), rng.rangeID, le)
}

func (s *state) updateStoreCapacity(storeID StoreID) {
	if store, ok := s.stores[storeID]; ok {
		store.desc.Capacity = Capacity(s, storeID)
	}
}

// ReplicaLoad returns the usage information for the Range with ID
// RangeID on the store with ID StoreID.
func (s *state) ReplicaLoad(rangeID RangeID, storeID StoreID) ReplicaLoad {
	// NB: we only return the actual replica load, if the range leaseholder is
	// currently on the store given. Otherwise, return an empty, zero counter
	// value.
	store, ok := s.LeaseholderStore(rangeID)
	if ok && store.StoreID() == storeID {
		return s.load[rangeID]
	}
	return &ReplicaLoadCounter{}
}

// ClusterUsageInfo returns the usage information for the Range with ID
// RangeID.
func (s *state) ClusterUsageInfo() *ClusterUsageInfo {
	return s.usageInfo
}

// TickClock modifies the state Clock time to Tick. The clock is used as the
// system time source for the store pools that are spawned from this state.
func (s *state) TickClock(tick time.Time) {
	s.clock.Set(tick.UnixNano())
}

// UpdateStorePool modifies the state of the StorePool for the Store with
// ID StoreID.
func (s *state) UpdateStorePool(
	storeID StoreID, storeDescriptors map[roachpb.StoreID]*storepool.StoreDetail,
) {
	for gossipStoreID, detail := range storeDescriptors {
		copiedDetail := *detail
		copiedDesc := *detail.Desc
		copiedDetail.Desc = &copiedDesc
		s.stores[storeID].storepool.DetailsMu.StoreDetails[gossipStoreID] = &copiedDetail
	}
}

// NextReplicasFn returns a function, that when called will return the current
// replicas that exist on the store.
func (s *state) NextReplicasFn(storeID StoreID) func() []Replica {
	nextReplFn := func() []Replica {
		return s.Replicas(storeID)
	}
	return nextReplFn
}

// NodeLivenessFn returns a function, that when called will return the
// liveness of the Node with ID NodeID.
// TODO(kvoli): Find a better home for this method, required by the storepool.
func (s *state) NodeLivenessFn() storepool.NodeLivenessFunc {
	nodeLivenessFn := func(nid roachpb.NodeID, now time.Time, timeUntilStoreDead time.Duration) livenesspb.NodeLivenessStatus {
		// TODO(kvoli): Implement liveness records for nodes, that signal they
		// are dead when simulating partitions, crashes etc.
		return livenesspb.NodeLivenessStatus_LIVE
	}
	return nodeLivenessFn
}

// NodeCountFn returns a function, that when called will return the current
// number of nodes that exist in this state.
// TODO(kvoli): Find a better home for this method, required by the storepool.
func (s *state) NodeCountFn() storepool.NodeCountFunc {
	nodeCountFn := func() int {
		return len(s.Nodes())
	}
	return nodeCountFn
}

// MakeAllocator returns an allocator for the Store with ID StoreID, it
// populates the storepool with the current state.
func (s *state) MakeAllocator(storeID StoreID) allocatorimpl.Allocator {
	return allocatorimpl.MakeAllocator(
		s.stores[storeID].storepool,
		func(addr string) (time.Duration, bool) { return 0, true },
		&allocator.TestingKnobs{
			AllowLeaseTransfersToReplicasNeedingSnapshots: true,
		},
	)
}

// LeaseHolderReplica returns the replica which holds a lease for the range
// with ID RangeID, if the range exists, otherwise returning false.
func (s *state) LeaseHolderReplica(rangeID RangeID) (Replica, bool) {
	rng, ok := s.ranges.rangeMap[rangeID]
	if !ok {
		return nil, false
	}

	for _, replica := range rng.replicas {
		if replica.holdsLease {
			return replica, true
		}
	}
	return nil, false
}

// LeaseholderStore returns the store which holds a lease for the range with ID
// RangeID, if the range and store exist, otherwise returning false.
func (s *state) LeaseholderStore(rangeID RangeID) (Store, bool) {
	replica, ok := s.LeaseHolderReplica(rangeID)
	if !ok {
		return nil, false
	}

	store, ok := s.stores[replica.StoreID()]
	if !ok {
		return nil, false
	}
	return store, true
}

// LoadSplitterFor returns the load splitter for the Store with ID StoreID.
func (s *state) LoadSplitterFor(storeID StoreID) LoadSplitter {
	return s.loadsplits[storeID]
}

// RaftStatus returns the current raft status for the replica of the Range
// with ID RangeID, on the store with ID StoreID.
func (s *state) RaftStatus(rangeID RangeID, storeID StoreID) *raft.Status {
	status := &raft.Status{
		Progress: make(map[uint64]tracker.Progress),
	}

	leader, ok := s.LeaseHolderReplica(rangeID)
	if !ok {
		return nil
	}
	rng, ok := s.rng(rangeID)
	if !ok {
		return nil
	}

	// TODO(kvoli): The raft leader will always be the current leaseholder
	// here. This should change to enable testing this scenario.
	status.Lead = uint64(leader.ReplicaID())
	status.RaftState = raft.StateLeader
	status.Commit = 2
	// TODO(kvoli): A replica is never behind on their raft log, this should
	// change to enable testing this scenario where replicas fall behind. e.g.
	// FirstIndex on all replicas will return 2.
	for _, replica := range rng.replicas {
		status.Progress[uint64(replica.ReplicaID())] = tracker.Progress{
			Match: 2,
			State: tracker.StateReplicate,
		}
	}

	return status
}

// node is an implementation of the Node interface.
type node struct {
	nodeID NodeID
	desc   roachpb.NodeDescriptor

	stores []StoreID
}

// NodeID returns the ID of this node.
func (n *node) NodeID() NodeID {
	return n.nodeID
}

// Stores returns all stores that are on this node.
func (n *node) Stores() []StoreID {
	return n.stores
}

// Descriptor returns the descriptor for this node.
func (n *node) Descriptor() roachpb.NodeDescriptor {
	return n.desc
}

// store is an implementation of the Store interface.
type store struct {
	storeID StoreID
	nodeID  NodeID
	desc    roachpb.StoreDescriptor

	storepool *storepool.StorePool
	replicas  map[RangeID]ReplicaID
}

// String returns a compact string representing the current state of the store.
func (s *store) String() string {
	builder := &strings.Builder{}
	builder.WriteString(fmt.Sprintf("s%dn%d=(", s.storeID, s.nodeID))

	nRepls := len(s.replicas)
	iterRepls := 0
	for rangeID, replicaID := range s.replicas {
		builder.WriteString(fmt.Sprintf("r%d:%d", rangeID, replicaID))
		if iterRepls < nRepls-1 {
			builder.WriteString(",")
		}
		iterRepls++
	}
	builder.WriteString(")")
	return builder.String()
}

// StoreID returns the ID of this store.
func (s *store) StoreID() StoreID {
	return s.storeID
}

// NodeID returns the ID of the node this store is on.
func (s *store) NodeID() NodeID {
	return s.nodeID
}

// Descriptor returns the Descriptor for this store.
func (s *store) Descriptor() roachpb.StoreDescriptor {
	return s.desc
}

// Replica returns a the Replica belonging to the Range with ID RangeID, if
// it exists, otherwise false.
func (s *store) Replica(rangeID RangeID) (ReplicaID, bool) {
	replicaID, ok := s.replicas[rangeID]
	return replicaID, ok
}

// rng is an implementation of the Range interface.
type rng struct {
	rangeID          RangeID
	startKey, endKey Key
	desc             roachpb.RangeDescriptor
	config           roachpb.SpanConfig
	replicas         map[StoreID]*replica
	leaseholder      ReplicaID
	size             int64
}

// RangeID returns the ID of this range.
func (r *rng) RangeID() RangeID {
	return r.rangeID
}

// Descriptor returns the descriptor for this range.
func (r *rng) Descriptor() *roachpb.RangeDescriptor {
	return &r.desc
}

// String returns a string representing the state of the range.
func (r *rng) String() string {
	builder := &strings.Builder{}
	builder.WriteString(fmt.Sprintf("r%d(%d)=(", r.rangeID, r.startKey))

	nRepls := len(r.replicas)
	iterRepls := 0
	for storeID, replica := range r.replicas {
		builder.WriteString(fmt.Sprintf("s%d:r%d", storeID, replica.replicaID))
		if r.leaseholder == replica.replicaID {
			builder.WriteString("*")
		}
		if iterRepls < nRepls-1 {
			builder.WriteString(",")
		}
		iterRepls++
	}
	builder.WriteString(")")

	return builder.String()
}

// SpanConfig returns the span config for this range.
func (r *rng) SpanConfig() roachpb.SpanConfig {
	return r.config
}

// Replicas returns all replicas which exist for this range.
func (r *rng) Replicas() []Replica {
	replicas := []Replica{}
	for _, replica := range r.replicas {
		replicas = append(replicas, replica)
	}
	sort.Slice(replicas, func(i, j int) bool { return replicas[i].ReplicaID() < replicas[j].ReplicaID() })
	return replicas
}

// Replica returns the replica that is on the store with ID StoreID if it
// exists, else false.
func (r *rng) Replica(storeID StoreID) (Replica, bool) {
	replica, ok := r.replicas[storeID]
	return replica, ok
}

// Leaseholder returns the ID of the leaseholder for this Range if there is
// one, otherwise it returns a ReplicaID -1.
func (r *rng) Leaseholder() ReplicaID {
	return r.leaseholder
}

func (r *rng) Size() int64 {
	return r.size
}

// SetSize sets the size of the range in bytes.
func (r *rng) SetSize(size int64) {
	r.size = size
}

// replica is an implementation of the Replica interface.
type replica struct {
	replicaID  ReplicaID
	storeID    StoreID
	rangeID    RangeID
	desc       roachpb.ReplicaDescriptor
	holdsLease bool
}

// ReplicaID returns the ID of this replica.
func (r *replica) ReplicaID() ReplicaID {
	return r.replicaID
}

// StoreID returns the ID of the store this replica is on.
func (r *replica) StoreID() StoreID {
	return r.storeID
}

// Descriptor returns the descriptor for this replica.
func (r *replica) Descriptor() roachpb.ReplicaDescriptor {
	return r.desc
}

// Range returns the RangeID which this is a replica for.
func (r *replica) Range() RangeID {
	return r.rangeID
}

// HoldsLease returns whether this replica holds the lease for the range.
func (r *replica) HoldsLease() bool {
	return r.holdsLease
}

// String returns a string representing the state of the replica.
func (r *replica) String() string {
	builder := &strings.Builder{}
	builder.WriteString(fmt.Sprintf("r%d,s%d/%d", r.rangeID, r.storeID, r.replicaID))
	return builder.String()
}
