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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type (
	// NodeID is the unique identifier for a node.
	NodeID int32
	// StoreID is the unique identifier for a store.
	StoreID int32
	// ReplicaID is the unique identifier for a replica of a range.
	ReplicaID int32
	// RangeID is the unique identifier for a section of the keyspace.
	RangeID int32
)

// State encapsulates the current configuration and load of a simulation run.
// It provides methods for accessing and mutation simulation state of nodes,
// stores, ranges and replicas.
type State interface {
	// TODO(kvoli): Unit test this fn.
	// String returns string containing a compact representation of the state.
	String() string
	// ClusterInfo returns the info of the cluster represented in state.
	ClusterInfo() ClusterInfo
	// Store returns the Store with ID StoreID. This fails if no Store exists
	// with ID StoreID.
	Store(StoreID) (Store, bool)
	// Stores returns all stores that exist in this state.
	Stores() map[StoreID]Store
	// StoreDescriptors returns the descriptors for all stores that exist in
	// this state.
	StoreDescriptors() []roachpb.StoreDescriptor
	// Nodes returns all nodes that exist in this state.
	Nodes() map[NodeID]Node
	// RangeFor returns the range containing Key in [StartKey, EndKey). This
	// cannot fail.
	RangeFor(Key) Range
	// Range returns the range with ID RangeID. This fails if no Range exists
	// with ID RangeID.
	Range(RangeID) (Range, bool)
	// Ranges returns all ranges that exist in this state.
	Ranges() map[RangeID]Range
	// Replicas returns all replicas that exist on a store.
	Replicas(StoreID) []Replica
	// AddNode modifies the state to include one additional node. This cannot
	// fail. The new Node is returned.
	AddNode() Node
	// AddStore modifies the state to include one additional store on the Node
	// with ID NodeID. This fails if no Node exists with ID NodeID.
	AddStore(NodeID) (Store, bool)
	// CanAddReplica returns whether adding a replica for the Range with ID RangeID
	// to the Store with ID StoreID is valid.
	CanAddReplica(RangeID, StoreID) bool
	// CanRemoveReplica returns whether removing a replica for the Range with
	// ID RangeID from the Store with ID StoreID is valid.
	CanRemoveReplica(RangeID, StoreID) bool
	// AddReplica modifies the state to include one additional range for the
	// Range with ID RangeID, placed on the Store with ID StoreID. This fails
	// if a Replica for the Range already exists the Store.
	AddReplica(RangeID, StoreID) (Replica, bool)
	// RemoveReplica modifies the state to remove a Replica with the ID
	// ReplicaID. It fails if this Replica does not exist.
	RemoveReplica(RangeID, StoreID) bool
	// SplitRange splits the Range which contains Key in [StartKey, EndKey).
	// The Range is partitioned into [StartKey, Key), [Key, EndKey) and
	// returned. The right hand side of this split, is the new Range. If any
	// replicas exist for the old Range [StartKey,EndKey), these become
	// replicas of the left hand side [StartKey, Key) and are unmodified. For
	// each of these replicas, new replicas are created for the right hand side
	// [Key, EndKey), on identical stores to the un-split Range's replicas. This
	// fails if the Key given already exists as a StartKey.
	SplitRange(Key) (Range, Range, bool)
	// SetSpanConfig set the span config for the Range with ID RangeID.
	SetSpanConfig(RangeID, roachpb.SpanConfig) bool
	// Valid transfer returns whether transferring the lease for the Range with ID
	// RangeID, to the Store with ID StoreID is valid.
	ValidTransfer(RangeID, StoreID) bool
	// TransferLease transfers the lease for the Range with ID RangeID, to the
	// Store with ID StoreID. This fails if there is no such Store; or there is
	// no such Range; or if the Store doesn't hold a Replica for the Range; or
	// if the Replica for the Range on the Store is already the leaseholder.
	TransferLease(RangeID, StoreID) bool
	// ApplyLoad modifies the state to reflect the impact of the LoadEvent.
	// This modifies specifically the leaseholder replica's RangeUsageInfo for
	// the targets of the LoadEvent. The store which contains this replica is
	// likewise modified to reflect this in it's Capacity, held in the
	// StoreDescriptor.
	ApplyLoad(workload.LoadEvent)
	// UsageInfo returns the usage information for the Range with ID
	// RangeID.
	UsageInfo(RangeID) allocator.RangeUsageInfo
	// TickClock modifies the state Clock time to Tick.
	TickClock(time.Time)
	// UpdateStorePool modifies the state of the StorePool for the Store with
	// ID StoreID.
	UpdateStorePool(StoreID, map[roachpb.StoreID]*storepool.StoreDetail)
	// NextReplicasFn returns a function, that when called will return the current
	// replicas that exist on the store.
	NextReplicasFn(StoreID) func() []Replica
	// NodeLivenessFn returns a function, that when called will return the
	// liveness of the Node with ID NodeID.
	// TODO(kvoli): Find a better home for this method, required by the
	// storepool.
	NodeLivenessFn() storepool.NodeLivenessFunc
	// NodeCountFn returns a function, that when called will return the current
	// number of nodes that exist in this state.
	// TODO(kvoli): Find a better home for this method, required by the
	// storepool.
	NodeCountFn() storepool.NodeCountFunc
	// MakeAllocator returns an allocator for the Store with ID StoreID, it
	// populates the storepool with the current state.
	// TODO(kvoli): The storepool is part of the state at some tick, however
	// the allocator and storepool should both be separated out of this
	// interface, instead using it to populate themselves.
	MakeAllocator(StoreID) allocatorimpl.Allocator
}

// Node is a container for stores and is part of a cluster.
type Node interface {
	// NodeID returns the ID of this node.
	NodeID() NodeID
	// Stores returns the StoreIDs of all stores that are on this node.
	Stores() []StoreID
	// Descriptor returns the descriptor for this node.
	Descriptor() roachpb.NodeDescriptor
}

// Store is a container for replicas.
type Store interface {
	// StoreID returns the ID of this store.
	StoreID() StoreID
	// NodeID returns the ID of the node this store is on.
	NodeID() NodeID
	// Descriptor returns the Descriptor for this store.
	Descriptor() roachpb.StoreDescriptor
	// String returns a string representing the state of the store.
	String() string
	// Replicas returns all replicas that are on this store.
	Replicas() map[RangeID]ReplicaID
}

// Range is a slice of the keyspace, which may have replicas that exist on
// some store(s).
type Range interface {
	// RangeID returns the ID of this range.
	RangeID() RangeID
	// Descriptor returns the descriptor for this range.
	Descriptor() *roachpb.RangeDescriptor
	// String returns a string representing the state of the range.
	String() string
	// SpanConfig returns the span config for this range.
	SpanConfig() roachpb.SpanConfig
	// Replicas returns all replicas which exist for this range.
	Replicas() map[StoreID]Replica
	// Leaseholder returns the ID of the leaseholder for this Range if there is
	// one, otherwise it returns a ReplicaID -1.
	Leaseholder() ReplicaID
}

// Replica is a replica for a range that exists on a store. This is the
// smallest unit of distribution within a cluster.
type Replica interface {
	// ReplicaID returns the ID of this replica.
	ReplicaID() ReplicaID
	// StoreID returns the ID of the store this replica is on.
	StoreID() StoreID
	// Descriptor returns the descriptor for this replica.
	Descriptor() roachpb.ReplicaDescriptor
	// Range returns the range which this is a replica for.
	Range() RangeID
	// HoldsLease returns whether this replica holds the lease for the range.
	HoldsLease() bool
}

// Keys in the simulator are 64 bit integers. They are mapped to Keys in
// cockroach as the decimal representation, with 0 padding such that they are
// lexiographically ordered as strings. The simplification to limit keys to
// integers simplifies workload generation and testing.
//
// TODO(kvoli): This is a simplification. In order to replay workloads or use
// the workload tool, real keys, which may be arbitrary bytes will need to
// either be remapped or the key format extended to support them. Revisit this
// when we are ready.

// Key is a single slot in the keyspace.
type Key int64

// minKey is the minimum key in the keyspace.
const minKey Key = -1

// maxKey is the maximum key in the keyspace.
const maxKey Key = 9999999999

// keyFmt is the formatter for representing keys as lexicographically ordered
// strings.
const keyFmt = "%010d"

// ToRKey converts a key (int64) to a resolved Key, in decimal format.
func (k Key) ToRKey() roachpb.RKey {
	return roachpb.RKey(fmt.Sprintf(keyFmt, k))
}

// defaultSpanConfig is the span config applied by default to all ranges,
// unless overwritten.
var defaultSpanConfig roachpb.SpanConfig = roachpb.SpanConfig{
	RangeMinBytes: 128 << 20, // 128 MB
	RangeMaxBytes: 512 << 20, // 512 MB,
	NumReplicas:   3,
	NumVoters:     3,
}
