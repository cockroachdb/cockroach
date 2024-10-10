// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigreporter"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/google/btree"
)

type state struct {
	nodes                   map[NodeID]*node
	stores                  map[StoreID]*store
	load                    map[RangeID]ReplicaLoad
	loadsplits              map[StoreID]LoadSplitter
	nodeLiveness            MockNodeLiveness
	capacityChangeListeners []CapacityChangeListener
	newCapacityListeners    []NewCapacityListener
	configChangeListeners   []ConfigChangeListener
	capacityOverrides       map[StoreID]CapacityOverride
	ranges                  *rmap
	clusterinfo             ClusterInfo
	usageInfo               *ClusterUsageInfo
	clock                   *ManualSimClock
	settings                *config.SimulationSettings

	// Unique ID generators for Nodes and Stores. These are incremented
	// pre-assignment. So that IDs start from 1.
	nodeSeqGen  NodeID
	storeSeqGen StoreID
}

var _ State = &state{}

// NewState returns an implementation of the State interface.
func NewState(settings *config.SimulationSettings) State {
	return newState(settings)
}

func newState(settings *config.SimulationSettings) *state {
	s := &state{
		nodes:             make(map[NodeID]*node),
		stores:            make(map[StoreID]*store),
		loadsplits:        make(map[StoreID]LoadSplitter),
		capacityOverrides: make(map[StoreID]CapacityOverride),
		clock:             &ManualSimClock{nanos: settings.StartTime.UnixNano()},
		ranges:            newRMap(),
		usageInfo:         newClusterUsageInfo(),
		settings:          settings,
	}
	s.nodeLiveness = MockNodeLiveness{
		clock:     hlc.NewClockForTesting(s.clock),
		statusMap: map[NodeID]livenesspb.NodeLivenessStatus{},
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
		config:      &defaultSpanConfig,
		replicas:    make(map[StoreID]*replica),
		leaseholder: -1,
	}

	rm.rangeTree.ReplaceOrInsert(rng)
	rm.rangeMap[rangeID] = rng
}

// PrettyPrint returns a pretty formatted string representation of the
// state (more concise than String()).
func (s *state) PrettyPrint() string {
	builder := &strings.Builder{}
	nStores := len(s.stores)
	builder.WriteString(fmt.Sprintf("stores(%d)=[", nStores))
	var storeIDs []StoreID
	for storeID := range s.stores {
		storeIDs = append(storeIDs, storeID)
	}
	slices.Sort(storeIDs)

	for i, storeID := range storeIDs {
		store := s.stores[storeID]
		builder.WriteString(store.PrettyPrint())
		if i < nStores-1 {
			builder.WriteString(",")
		}
	}
	builder.WriteString("]")
	return builder.String()
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
	builder.WriteString(fmt.Sprintf("stores(%d)=[", nStores))

	// Sort the unordered map storeIDs by its key to ensure deterministic
	// printing.
	var storeIDs []StoreID
	for storeID := range s.stores {
		storeIDs = append(storeIDs, storeID)
	}
	slices.Sort(storeIDs)

	for i, storeID := range storeIDs {
		store := s.stores[storeID]
		builder.WriteString(store.String())
		if i < nStores-1 {
			builder.WriteString(",")
		}
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
	stores := make([]Store, 0, len(s.stores))
	keys := make([]StoreID, 0, len(s.stores))
	for key := range s.stores {
		keys = append(keys, key)
	}

	for _, key := range keys {
		store := s.stores[key]
		stores = append(stores, store)
	}
	slices.SortFunc(stores, func(a, b Store) int {
		return cmp.Compare(a.StoreID(), b.StoreID())
	})
	return stores
}

// StoreDescriptors returns the descriptors for the StoreIDs given. If the
// first flag is false, then the capacity is generated from scratch,
// otherwise the last calculated capacity values are used for each store.
func (s *state) StoreDescriptors(cached bool, storeIDs ...StoreID) []roachpb.StoreDescriptor {
	storeDescriptors := []roachpb.StoreDescriptor{}
	for _, storeID := range storeIDs {
		if store, ok := s.Store(storeID); ok {
			if !cached {
				s.updateStoreCapacity(storeID)
			}
			storeDescriptors = append(storeDescriptors, store.Descriptor())
		}
	}
	return storeDescriptors
}

func (s *state) updateStoreCapacity(storeID StoreID) {
	if store, ok := s.stores[storeID]; ok {
		capacity := s.capacity(storeID)
		if override, ok := s.capacityOverrides[storeID]; ok {
			capacity = mergeOverride(capacity, override)
		}
		store.desc.Capacity = capacity
		s.publishNewCapacityEvent(capacity, storeID)
	}
}

func (s *state) capacity(storeID StoreID) roachpb.StoreCapacity {
	// TODO(kvoli,lidorcarmel): Store capacity will need to be populated with
	// the following missing fields: l0sublevels, bytesperreplica, writesperreplica.
	store, ok := s.stores[storeID]
	if !ok {
		panic(fmt.Sprintf("programming error: store (%d) doesn't exist", storeID))
	}

	// We re-use the existing store capacity and selectively zero out the fields
	// we intend to change.
	capacity := store.desc.Capacity
	capacity.QueriesPerSecond = 0
	capacity.WritesPerSecond = 0
	capacity.LogicalBytes = 0
	capacity.LeaseCount = 0
	capacity.RangeCount = 0
	capacity.Used = 0
	capacity.Available = 0

	for _, repl := range s.Replicas(storeID) {
		rangeID := repl.Range()
		replicaID := repl.ReplicaID()
		rng, _ := s.Range(rangeID)
		if rng.Leaseholder() == replicaID {
			// TODO(kvoli): We currently only consider load on the leaseholder
			// replica for a range. The other replicas have an estimate that is
			// calculated within the allocation algorithm. Adapt this to
			// support follower reads, when added to the workload generator.
			usage := s.RangeUsageInfo(rng.RangeID(), storeID)
			capacity.QueriesPerSecond += usage.QueriesPerSecond
			capacity.WritesPerSecond += usage.WritesPerSecond
			capacity.LogicalBytes += usage.LogicalBytes
			capacity.LeaseCount++
		}
		capacity.RangeCount++
	}

	// TODO(kvoli): parameterize the logical to actual used storage bytes. At the
	// moment we use 1.25 as a rough estimate.
	used := int64(float64(capacity.LogicalBytes) * 1.25)
	available := capacity.Capacity - used
	capacity.Used = used
	capacity.Available = available
	return capacity
}

// Store returns the Store with ID StoreID. This fails if no Store exists
// with ID StoreID.
func (s *state) Store(storeID StoreID) (Store, bool) {
	store, ok := s.stores[storeID]
	return store, ok
}

func (s *state) Nodes() []Node {
	nodes := make([]Node, 0, len(s.nodes))
	for _, node := range s.nodes {
		nodes = append(nodes, node)
	}
	slices.SortFunc(nodes, func(a, b Node) int {
		return cmp.Compare(a.NodeID(), b.NodeID())
	})
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
	ranges := make([]Range, 0, len(s.ranges.rangeMap))
	for _, r := range s.ranges.rangeMap {
		ranges = append(ranges, r)
	}
	slices.SortFunc(ranges, func(a, b Range) int {
		return cmp.Compare(a.RangeID(), b.RangeID())
	})
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
	if r[i].Range() == r[j].Range() {
		return r[i].ReplicaID() < r[j].ReplicaID()
	}
	return r[i].Range() < r[j].Range()
}

// Replicas returns all replicas that exist on a store.
func (s *state) Replicas(storeID StoreID) []Replica {
	var replicas []Replica
	store, ok := s.stores[storeID]
	if !ok {
		return replicas
	}

	repls := make(replicaList, 0, len(store.replicas))
	var rangeIDs []RangeID
	for rangeID := range store.replicas {
		rangeIDs = append(rangeIDs, rangeID)
	}
	slices.Sort(rangeIDs)
	for _, rangeID := range rangeIDs {
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
	s.SetNodeLiveness(nodeID, livenesspb.NodeLivenessStatus_LIVE)
	return node
}
func (s *state) SetNodeLocality(nodeID NodeID, locality roachpb.Locality) {
	node, ok := s.nodes[nodeID]
	if !ok {
		panic(fmt.Sprintf(
			"programming error: attempt to set locality for node which doesn't "+
				"exist (NodeID=%d, Nodes=%s)", nodeID, s))
	}
	node.desc.Locality = locality
	for _, storeID := range node.stores {
		s.stores[storeID].desc.Node = node.desc
	}
}

// Topology represents the locality hierarchy information for a cluster.
type Topology struct {
	children map[string]*Topology
	nodes    []int
}

// Topology returns the locality hierarchy information for a cluster.
func (s *state) Topology() Topology {
	nodes := s.Nodes()
	root := Topology{children: map[string]*Topology{}}
	for _, node := range nodes {
		current := &root
		for _, tier := range node.Descriptor().Locality.Tiers {
			_, ok := current.children[tier.Value]
			if !ok {
				current.children[tier.Value] = &Topology{children: map[string]*Topology{}}
			}
			current = current.children[tier.Value]
		}
		current.nodes = append(current.nodes, int(node.NodeID()))
	}
	return root
}

// String returns a compact string representing the locality hierarchy of the
// Topology.
func (t *Topology) String() string {
	var buf bytes.Buffer
	t.stringHelper(&buf, "", true)
	return buf.String()
}

func (t *Topology) stringHelper(buf *bytes.Buffer, prefix string, isLast bool) {
	if len(t.children) > 0 {
		childPrefix := prefix
		if isLast {
			childPrefix += "  "
		} else {
			childPrefix += "│ "
		}

		keys := make([]string, 0, len(t.children))
		for key := range t.children {
			keys = append(keys, key)
		}

		sort.Strings(keys)
		for i, key := range keys {
			buf.WriteString(fmt.Sprintf("%s%s\n", prefix, t.formatKey(key)))
			child := t.children[key]
			if i == len(keys)-1 {
				child.stringHelper(buf, childPrefix, true)
			} else {
				child.stringHelper(buf, childPrefix, false)
			}

		}
	}

	if len(t.nodes) > 0 {
		buf.WriteString(fmt.Sprintf("%s└── %v\n", prefix, t.nodes))
	}
}

func (t *Topology) formatKey(key string) string {
	if _, err := strconv.Atoi(key); err == nil {
		return fmt.Sprintf("[%s]", key)
	}

	return key
}

// AddStore modifies the state to include one additional store on the Node
// with ID NodeID. This fails if no Node exists with ID NodeID.
func (s *state) AddStore(nodeID NodeID) (Store, bool) {
	if _, ok := s.nodes[nodeID]; !ok {
		return nil, false
	}

	node := s.nodes[nodeID]
	s.storeSeqGen++
	storeID := s.storeSeqGen
	sp, st := NewStorePool(s.NodeCountFn(), s.NodeLivenessFn(), hlc.NewClockForTesting(s.clock))
	store := &store{
		storeID:   storeID,
		nodeID:    nodeID,
		desc:      roachpb.StoreDescriptor{StoreID: roachpb.StoreID(storeID), Node: node.Descriptor()},
		storepool: sp,
		settings:  st,
		replicas:  make(map[RangeID]ReplicaID),
	}

	// Commit the new store to state.
	node.stores = append(node.stores, storeID)
	s.stores[storeID] = store

	// Add a range load splitter for this store.
	s.loadsplits[storeID] = NewSplitDecider(s.settings)

	// Add a usage info struct.
	_ = s.usageInfo.storeRef(storeID)

	for _, listener := range s.configChangeListeners {
		listener.StoreAddNotify(storeID, s)
	}

	return store, true
}

func (s *state) SetStoreCapacity(storeID StoreID, capacity int64) {
	store, ok := s.stores[storeID]
	if !ok {
		panic(fmt.Sprintf("programming error: store with ID %d doesn't exist", storeID))
	}
	// TODO(kvoli): deal with overwriting this.
	store.desc.Capacity.Capacity = capacity
}

// AddReplica modifies the state to include one additional range for the
// Range with ID RangeID, placed on the Store with ID StoreID. This fails
// if a Replica for the Range already exists the Store.
func (s *state) AddReplica(
	rangeID RangeID, storeID StoreID, rtype roachpb.ReplicaType,
) (Replica, bool) {
	return s.addReplica(rangeID, storeID, rtype)
}

func (s *state) addReplica(
	rangeID RangeID, storeID StoreID, rtype roachpb.ReplicaType,
) (*replica, bool) {
	// Check whether it is possible to add the replica.
	if !s.CanAddReplica(rangeID, storeID) {
		return nil, false
	}

	store := s.stores[storeID]
	nodeID := store.nodeID
	rng, _ := s.rng(rangeID)

	desc := rng.desc.AddReplica(roachpb.NodeID(nodeID), roachpb.StoreID(storeID), rtype)
	replica := &replica{
		replicaID: ReplicaID(desc.ReplicaID),
		storeID:   storeID,
		rangeID:   rangeID,
		desc:      desc,
	}

	store.replicas[rangeID] = replica.replicaID
	rng.replicas[storeID] = replica
	s.publishCapacityChangeEvent(kvserver.RangeAddEvent, storeID)

	// This is the first replica to be added for this range. Make it the
	// leaseholder as a placeholder. The caller can update the lease, however
	// we want to ensure that for any range that has replicas, a leaseholder
	// exists at all times.
	if rng.leaseholder == -1 && rtype == roachpb.VOTER_FULL {
		s.setLeaseholder(rangeID, storeID)
	}

	return replica, true
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
	return s.removeReplica(rangeID, storeID)
}

func (s *state) removeReplica(rangeID RangeID, storeID StoreID) bool {
	// Check whether it is possible to remove the replica.
	if !s.CanRemoveReplica(rangeID, storeID) {
		return false
	}

	store := s.stores[storeID]
	nodeID := store.nodeID
	rng, _ := s.rng(rangeID)

	if _, ok := rng.desc.RemoveReplica(roachpb.NodeID(nodeID), roachpb.StoreID(storeID)); !ok {
		return false
	}

	delete(store.replicas, rangeID)
	delete(rng.replicas, storeID)
	s.publishCapacityChangeEvent(kvserver.RangeRemoveEvent, storeID)

	return true
}

// SetSpanConfigForRange set the span config for the Range with ID RangeID.
func (s *state) SetSpanConfigForRange(rangeID RangeID, spanConfig *roachpb.SpanConfig) bool {
	if rng, ok := s.ranges.rangeMap[rangeID]; ok {
		rng.config = spanConfig
		return true
	}
	return false
}

// SetSpanConfig sets the span config for all ranges represented by the span,
// splitting if necessary.
func (s *state) SetSpanConfig(span roachpb.Span, config *roachpb.SpanConfig) {
	startKey := ToKey(span.Key)
	endKey := ToKey(span.EndKey)

	// Decide whether we need to split due to the config intersecting an existing
	// range boundary. Split if necessary. Then apply the span config to all the
	// ranges contained within the span. e.g.
	//   ranges r1: [a, c) r2: [c, z)
	//   span: [b, f)
	// resulting ranges:
	//   [a, b)         - keeps old span config from [a,c)
	//   [b, c) [c, f)  - gets the new span config passed in
	//   [f, z)         - keeps old span config from [c,z)

	splitsRequired := []Key{}
	s.ranges.rangeTree.DescendLessOrEqual(&rng{startKey: startKey}, func(i btree.Item) bool {
		cur, _ := i.(*rng)
		rStart := cur.startKey
		// There are two cases we handle:
		// (1) rStart == startKey: We don't need to split.
		// (2) rStart < startKey:  We need to split into lhs [rStart, startKey) and
		//     rhs [startKey, ...). Where the lhs does not have the span config
		//     applied and the rhs does.
		if rStart < startKey {
			splitsRequired = append(splitsRequired, startKey)
		}
		return false
	})

	s.ranges.rangeTree.DescendLessOrEqual(&rng{startKey: endKey}, func(i btree.Item) bool {
		cur, _ := i.(*rng)
		rEnd := cur.endKey
		rStart := cur.startKey
		if rStart == endKey {
			return false
		}
		// There are two cases we handle:
		// (1) rEnd == endKey: We don't need to split.
		// (2) rEnd >  endKey: We need to split into lhs [..., endKey) and rhs
		//     [endKey, rEnd). Where the lhs has the span config applied and the rhs
		//     does not.
		// Split required if its the last range we will hit.
		if rEnd > endKey {
			splitsRequired = append(splitsRequired, endKey)
		}
		return false
	})

	for _, splitKey := range splitsRequired {
		// We panic here as we don't have any way to roll back the split if one
		// succeeds and another doesn't
		if _, _, ok := s.SplitRange(splitKey); !ok {
			panic(fmt.Sprintf(
				"programming error: unable to split range (key=%d) for set span "+
					"config=%s, state=%s", splitKey, config.String(), s))
		}
	}

	// Apply the span config to all the ranges affected.
	s.ranges.rangeTree.AscendGreaterOrEqual(&rng{startKey: startKey}, func(i btree.Item) bool {
		cur, _ := i.(*rng)
		if cur.startKey == endKey {
			return false
		}
		if cur.startKey > endKey {
			panic("programming error: unexpected range found with start key > end key")
		}
		if !s.SetSpanConfigForRange(cur.rangeID, config) {
			panic("programming error: unable to set span config for range")
		}
		return true
	})
}

// SetRangeBytes sets the size of the range with ID RangeID to be equal to
// the bytes given.
func (s *state) SetRangeBytes(rangeID RangeID, bytes int64) {
	rng, ok := s.ranges.rangeMap[rangeID]
	if !ok {
		panic(fmt.Sprintf("programming error: no range with with ID %d", rangeID))
	}
	rng.size = bytes
}

// SetCapacityOverride updates the capacity for the store with ID StoreID to
// always return the overriden value given for any set fields in
// CapacityOverride.
func (s *state) SetCapacityOverride(storeID StoreID, override CapacityOverride) {
	if _, ok := s.stores[storeID]; !ok {
		panic(fmt.Sprintf("programming error: no store exist with ID %d", storeID))
	}

	existing, ok := s.capacityOverrides[storeID]
	if !ok {
		s.capacityOverrides[storeID] = override
		return
	}

	s.capacityOverrides[storeID] = CapacityOverride(mergeOverride(roachpb.StoreCapacity(existing), override))
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
		config:      &defaultSpanConfig,
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
		if _, ok := s.AddReplica(rangeID, storeID, replica.Descriptor().Type); !ok {
			panic(
				fmt.Sprintf("programming error: unable to add replica for range=%d to store=%d",
					r.rangeID, storeID))
		}
		if replica.HoldsLease() {
			// The successor range's leaseholder was on this store, copy the
			// leaseholder store over for the new split range.
			leaseholderStore, ok := s.LeaseholderStore(r.rangeID)
			if !ok {
				panic(fmt.Sprintf("programming error: expected leaseholder store to "+
					"exist for RangeID %d", r.rangeID))
			}
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
	oldStore, ok := s.LeaseholderStore(rangeID)
	if !ok {
		return false
	}

	// Reset the load stats on the old range, within the old
	// leaseholder store.
	s.loadsplits[oldStore.StoreID()].ResetRange(rangeID)
	s.loadsplits[storeID].ResetRange(rangeID)
	s.load[rangeID].ResetLoad()

	// Apply the lease transfer to state.
	s.replaceLeaseHolder(rangeID, storeID, oldStore.StoreID())
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
	s.publishCapacityChangeEvent(kvserver.LeaseAddEvent, storeID)
}

func (s *state) removeLeaseholder(rangeID RangeID, storeID StoreID) {
	rng := s.ranges.rangeMap[rangeID]
	if repl, ok := rng.replicas[storeID]; ok {
		if repl.holdsLease {
			repl.holdsLease = false
			s.publishCapacityChangeEvent(kvserver.LeaseRemoveEvent, storeID)
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

// ReplicaLoad returns the usage information for the Range with ID
// RangeID on the store with ID StoreID.
func (s *state) RangeUsageInfo(rangeID RangeID, storeID StoreID) allocator.RangeUsageInfo {
	// NB: we only return the actual replica load, if the range leaseholder is
	// currently on the store given. Otherwise, return an empty, zero counter
	// value.
	store, ok := s.LeaseholderStore(rangeID)
	if !ok {
		panic(fmt.Sprintf("no leaseholder store found for range %d", storeID))
	}

	r, _ := s.Range(rangeID)
	// TODO(kvoli): The requested storeID is not the leaseholder. Non
	// leaseholder load tracking is not currently supported but is checked by
	// other components such as hot ranges. In this case, ignore it but we
	// should also track non leaseholder load. See load.go for more. Return an
	// empty initialized load counter here.
	if store.StoreID() != storeID {
		return allocator.RangeUsageInfo{LogicalBytes: r.Size()}
	}

	usage := s.load[rangeID].Load()
	usage.LogicalBytes = r.Size()
	return usage
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

func (s *state) Clock() timeutil.TimeSource {
	return s.clock
}

// UpdateStorePool modifies the state of the StorePool for the Store with
// ID StoreID.
func (s *state) UpdateStorePool(
	storeID StoreID, storeDescriptors map[roachpb.StoreID]*storepool.StoreDetail,
) {
	var storeIDs roachpb.StoreIDSlice
	for storeIDA := range storeDescriptors {
		storeIDs = append(storeIDs, storeIDA)
	}
	sort.Sort(storeIDs)
	for _, gossipStoreID := range storeIDs {
		detail := storeDescriptors[gossipStoreID]
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

// SetNodeLiveness sets the liveness status of the node with ID NodeID to be
// the status given.
func (s *state) SetNodeLiveness(nodeID NodeID, status livenesspb.NodeLivenessStatus) {
	s.nodeLiveness.statusMap[nodeID] = status
}

// NodeLivenessFn returns a function, that when called will return the
// liveness of the Node with ID NodeID.
// TODO(kvoli): Find a better home for this method, required by the storepool.
func (s *state) NodeLivenessFn() storepool.NodeLivenessFunc {
	return func(nid roachpb.NodeID) livenesspb.NodeLivenessStatus {
		return s.nodeLiveness.statusMap[NodeID(nid)]
	}
}

// NodeCountFn returns a function, that when called will return the current
// number of nodes that exist in this state.
// TODO(kvoli): Find a better home for this method, required by the storepool.
// TODO(wenyihu6): introduce the concept of membership separated from the
// liveness map.
func (s *state) NodeCountFn() storepool.NodeCountFunc {
	return func() int {
		count := 0
		for _, status := range s.nodeLiveness.statusMap {
			// Nodes with a liveness status other than decommissioned or
			// decommissioning are considered active members (see
			// liveness.MembershipStatus).
			if status != livenesspb.NodeLivenessStatus_DECOMMISSIONED && status != livenesspb.NodeLivenessStatus_DECOMMISSIONING {
				count++
			}
		}
		return count
	}
}

// MakeAllocator returns an allocator for the Store with ID StoreID, it
// populates the storepool with the current state.
func (s *state) MakeAllocator(storeID StoreID) allocatorimpl.Allocator {
	return allocatorimpl.MakeAllocator(
		s.stores[storeID].settings,
		s.stores[storeID].storepool.IsDeterministic(),
		func(id roachpb.NodeID) (time.Duration, bool) { return 0, true },
		&allocator.TestingKnobs{
			AllowLeaseTransfersToReplicasNeedingSnapshots: true,
		},
	)
}

// StorePool returns the store pool for the given storeID.
func (s *state) StorePool(storeID StoreID) storepool.AllocatorStorePool {
	return s.stores[storeID].storepool
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
		Progress: make(map[raftpb.PeerID]tracker.Progress),
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
	status.Lead = raftpb.PeerID(leader.ReplicaID())
	status.RaftState = raftpb.StateLeader
	status.Commit = 2
	// TODO(kvoli): A replica is never behind on their raft log, this should
	// change to enable testing this scenario where replicas fall behind. e.g.
	// FirstIndex on all replicas will return 2.
	for _, replica := range rng.replicas {
		status.Progress[raftpb.PeerID(replica.ReplicaID())] = tracker.Progress{
			Match: 2,
			State: tracker.StateReplicate,
		}
	}

	return status
}

func (s *state) GetStoreDescriptor(storeID roachpb.StoreID) (roachpb.StoreDescriptor, bool) {
	if descs := s.StoreDescriptors(false, StoreID(storeID)); len(descs) == 0 {
		return roachpb.StoreDescriptor{}, false
	} else {
		return descs[0], true
	}
}

// NeedsSplit is added for the spanconfig.StoreReader interface, required for
// SpanConfigConformanceReport.
func (s *state) NeedsSplit(ctx context.Context, start, end roachpb.RKey) (bool, error) {
	// We don't need to implement this method for conformance reports.
	panic("not implemented")
}

// ComputeSplitKey is added for the spanconfig.StoreReader interface, required for
// SpanConfigConformanceReport.
func (s *state) ComputeSplitKey(
	ctx context.Context, start, end roachpb.RKey,
) (roachpb.RKey, error) {
	// We don't need to implement this method for conformance reports.
	panic("not implemented")
}

// GetSpanConfigForKey is added for the spanconfig.StoreReader interface, required for
// SpanConfigConformanceReport.
func (s *state) GetSpanConfigForKey(
	ctx context.Context, key roachpb.RKey,
) (roachpb.SpanConfig, roachpb.Span, error) {
	rng := s.rangeFor(ToKey(key.AsRawKey()))
	if rng == nil {
		panic(fmt.Sprintf("programming error: range for key %s doesn't exist", key))
	}
	return *rng.config, roachpb.Span{
		Key:    rng.startKey.ToRKey().AsRawKey(),
		EndKey: rng.endKey.ToRKey().AsRawKey(),
	}, nil
}

// Scan is added for the rangedesc.Scanner interface, required for
// SpanConfigConformanceReport. We ignore the span passed in and return every
// descriptor available.
func (s *state) Scan(
	ctx context.Context,
	pageSize int,
	init func(),
	span roachpb.Span,
	fn func(descriptors ...roachpb.RangeDescriptor) error,
) error {
	// NB: we ignore the span passed in, we pass the fn every range descriptor
	// available.
	rngs := s.Ranges()
	descriptors := make([]roachpb.RangeDescriptor, len(rngs))
	for i, rng := range rngs {
		descriptors[i] = *rng.Descriptor()
	}
	return fn(descriptors...)
}

// Report returns the span config conformance report for every range in the
// simulated cluster. This may be used to assert on the current conformance
// state of ranges.
func (s *state) Report() roachpb.SpanConfigConformanceReport {
	reporter := spanconfigreporter.New(
		s.nodeLiveness, s, s, s,
		cluster.MakeClusterSettings(), &spanconfig.TestingKnobs{})
	report, err := reporter.SpanConfigConformance(context.Background(), []roachpb.Span{{}})
	if err != nil {
		panic(fmt.Sprintf("programming error: error getting span config report %s", err.Error()))
	}
	return report
}

// RegisterCapacityChangeListener registers a listener which will be called
// on events where there is a capacity change (lease or replica) in the
// cluster state.
func (s *state) RegisterCapacityChangeListener(listener CapacityChangeListener) {
	s.capacityChangeListeners = append(s.capacityChangeListeners, listener)
}

func (s *state) publishCapacityChangeEvent(cce kvserver.CapacityChangeEvent, storeID StoreID) {
	for _, listener := range s.capacityChangeListeners {
		listener.CapacityChangeNotify(cce, storeID)
	}
}

// RegisterCapacityListener registers a listener which will be called when
// a new store capacity has been generated from scratch, for a specific
// store.
func (s *state) RegisterCapacityListener(listener NewCapacityListener) {
	s.newCapacityListeners = append(s.newCapacityListeners, listener)
}

func (s *state) publishNewCapacityEvent(capacity roachpb.StoreCapacity, storeID StoreID) {
	for _, listener := range s.newCapacityListeners {
		listener.NewCapacityNotify(capacity, storeID)
	}
}

// RegisterCapacityListener registers a listener which will be called when
// a new store capacity has been generated from scratch, for a specific
// store.
func (s *state) RegisterConfigChangeListener(listener ConfigChangeListener) {
	s.configChangeListeners = append(s.configChangeListeners, listener)
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
	settings  *cluster.Settings
	replicas  map[RangeID]ReplicaID
}

// PrettyPrint returns pretty formatted string representation of the store.
func (s *store) PrettyPrint() string {
	builder := &strings.Builder{}
	builder.WriteString(fmt.Sprintf("s%dn%d=(replicas(%d))", s.storeID, s.nodeID, len(s.replicas)))
	return builder.String()
}

// String returns a compact string representing the current state of the store.
func (s *store) String() string {
	builder := &strings.Builder{}
	builder.WriteString(fmt.Sprintf("s%dn%d=(", s.storeID, s.nodeID))

	// Sort the unordered map rangeIDs by its key to ensure deterministic
	// printing.
	var rangeIDs []RangeID
	for rangeID := range s.replicas {
		rangeIDs = append(rangeIDs, rangeID)
	}
	slices.Sort(rangeIDs)

	for i, rangeID := range rangeIDs {
		replicaID := s.replicas[rangeID]
		builder.WriteString(fmt.Sprintf("r%d:%d", rangeID, replicaID))
		if i < len(s.replicas)-1 {
			builder.WriteString(",")
		}
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
	config           *roachpb.SpanConfig
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

	// Sort the unordered map storeIDs by its key to ensure deterministic
	// printing.
	var storeIDs []StoreID
	for storeID := range r.replicas {
		storeIDs = append(storeIDs, storeID)
	}
	slices.Sort(storeIDs)

	for i, storeID := range storeIDs {
		replica := r.replicas[storeID]
		builder.WriteString(fmt.Sprintf("s%d:r%d", storeID, replica.replicaID))
		if r.leaseholder == replica.replicaID {
			builder.WriteString("*")
		}
		if i < len(r.replicas)-1 {
			builder.WriteString(",")
		}
		i++
	}
	builder.WriteString(")")

	return builder.String()
}

// SpanConfig returns the span config for this range.
func (r *rng) SpanConfig() *roachpb.SpanConfig {
	return r.config
}

// Replicas returns all replicas which exist for this range.
func (r *rng) Replicas() []Replica {
	replicas := make(replicaList, 0, len(r.replicas))
	for _, replica := range r.replicas {
		replicas = append(replicas, replica)
	}
	sort.Sort(replicas)
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
