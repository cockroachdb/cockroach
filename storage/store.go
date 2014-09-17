// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"bytes"
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
)

const (
	// UserRoot is the username for the root user.
	UserRoot = "root"
	// GCResponseCacheExpiration is the expiration duration for response
	// cache entries.
	GCResponseCacheExpiration = 1 * time.Hour
	// rangeIDAllocCount is the number of range IDs to allocate per allocation.
	raftIDAllocCount = 10
)

// rangeMetadataKeyPrefix and hexadecimal-formatted range ID.
func makeRangeKey(rangeID int64) engine.Key {
	return engine.MakeKey(engine.KeyLocalRangeMetadataPrefix, engine.Key(strconv.FormatInt(rangeID, 10)))
}

// A RangeSlice is a slice of Range pointers used for replica lookups
// by key.
type RangeSlice []*Range

// Implementation of sort.Interface which sorts by StartKey from each
// range's metadata.
func (rs RangeSlice) Len() int {
	return len(rs)
}
func (rs RangeSlice) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}
func (rs RangeSlice) Less(i, j int) bool {
	return bytes.Compare(rs[i].Meta.StartKey, rs[j].Meta.StartKey) < 0
}

// A NotBootstrappedError indicates that an engine has not yet been
// bootstrapped due to a store identifier not being present.
type NotBootstrappedError struct{}

// Error formats error.
func (e *NotBootstrappedError) Error() string {
	return "store has not been bootstrapped"
}

// NodeDescriptor holds details on node physical/network topology.
type NodeDescriptor struct {
	NodeID  int32
	Address net.Addr
	Attrs   proto.Attributes // node specific attributes (e.g. datacenter, machine info)
}

// StoreDescriptor holds store information including store attributes,
// node descriptor and store capacity.
type StoreDescriptor struct {
	StoreID  int32
	Attrs    proto.Attributes // store specific attributes (e.g. ssd, hdd, mem)
	Node     NodeDescriptor
	Capacity engine.StoreCapacity
}

// CombinedAttrs returns the full list of attributes for the store,
// including both the node and store attributes.
func (s *StoreDescriptor) CombinedAttrs() *proto.Attributes {
	var a []string
	a = append(a, s.Node.Attrs.Attrs...)
	a = append(a, s.Attrs.Attrs...)
	return &proto.Attributes{Attrs: a}
}

// Less compares two StoreDescriptors based on percentage of disk available.
func (s StoreDescriptor) Less(b util.Ordered) bool {
	return s.Capacity.PercentAvail() < b.(StoreDescriptor).Capacity.PercentAvail()
}

// A Store maintains a map of ranges by start key. A Store corresponds
// to one physical device.
type Store struct {
	Ident     proto.StoreIdent
	clock     *hlc.Clock
	engine    engine.Engine  // The underlying key-value store
	db        DB             // Cockroach KV DB
	allocator *allocator     // Makes allocation decisions
	gossip    *gossip.Gossip // Passed to new ranges

	mu          sync.RWMutex     // Protects variables below...
	ranges      map[int64]*Range // Map of ranges by range ID
	rangesByKey RangeSlice       // Sorted slice of ranges by StartKey
	nextRaftID  int64            // Next available Raft ID
	lastRaftID  int64            // Last available Raft ID in pre-alloc'd block
}

// NewStore returns a new instance of a store.
func NewStore(clock *hlc.Clock, engine engine.Engine, db DB, gossip *gossip.Gossip) *Store {
	return &Store{
		clock:     clock,
		engine:    engine,
		db:        db,
		allocator: &allocator{},
		gossip:    gossip,
		ranges:    map[int64]*Range{},
	}
}

// Close calls Range.Stop() on all active ranges.
func (s *Store) Close() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, rng := range s.ranges {
		rng.Stop()
	}
	s.ranges = map[int64]*Range{}
	s.rangesByKey = nil
}

// String formats a store for debug output.
func (s *Store) String() string {
	return fmt.Sprintf("store=%d:%d (%s)", s.Ident.NodeID, s.Ident.StoreID, s.engine)
}

// Init starts the engine, sets the GC and reads the StoreIdent.
func (s *Store) Init() error {
	// Start engine and set garbage collector.
	if err := s.engine.Start(); err != nil {
		return err
	}
	// GCTimeouts method is called each time an engine compaction is
	// underway. It sets minimum timeouts for transaction records and
	// response cache entries.
	s.engine.SetGCTimeouts(func() (minTxnTS, minRCacheTS int64) {
		now := s.clock.Now()
		minTxnTS = 0 // disable GC of transactions until we know minimum write intent age
		minRCacheTS = now.WallTime - GCResponseCacheExpiration.Nanoseconds()
		return
	})

	// Read store ident and return a not-bootstrapped error if necessary.
	ok, err := engine.GetProto(s.engine, engine.KeyLocalIdent, &s.Ident)
	if err != nil {
		return err
	} else if !ok {
		return &NotBootstrappedError{}
	}

	// TODO(spencer): scan through all range metadata and instantiate
	// ranges. Right now we just get range ID hardcoded as 1.
	var meta proto.RangeMetadata
	if ok, err = engine.GetProto(s.engine, makeRangeKey(1), &meta); err != nil || !ok {
		return util.Errorf("unable to read range 1: %v", err)
	}

	rng := NewRange(&meta, s.clock, s.engine, s.allocator, s.gossip, s)
	rng.Start()

	s.mu.Lock()
	defer s.mu.Unlock()
	s.ranges[meta.RangeID] = rng
	s.rangesByKey = append(s.rangesByKey, rng)
	return nil
}

// Bootstrap writes a new store ident to the underlying engine. To
// ensure that no crufty data already exists in the engine, it scans
// the engine contents before writing the new store ident. The engine
// should be completely empty. It returns an error if called on a
// non-empty engine.
func (s *Store) Bootstrap(ident proto.StoreIdent) error {
	if err := s.engine.Start(); err != nil {
		return err
	}
	s.Ident = ident
	kvs, err := s.engine.Scan(engine.KeyMin, engine.KeyMax, 1 /* only need one entry to fail! */)
	if err != nil {
		return util.Errorf("unable to scan engine to verify empty: %v", err)
	} else if len(kvs) > 0 {
		return util.Errorf("bootstrap failed; non-empty map with first key %q", kvs[0].Key)
	}
	return engine.PutProto(s.engine, engine.KeyLocalIdent, &s.Ident)
}

// GetRange fetches a range by ID. Returns an error if no range is found.
func (s *Store) GetRange(rangeID int64) (*Range, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if rng, ok := s.ranges[rangeID]; ok {
		return rng, nil
	}
	return nil, proto.NewRangeNotFoundError(rangeID)
}

// LookupRange looks up a range via binary search over the sorted
// "rangesByKey" RangeSlice. Returns nil if no range is found for
// specified key range.
func (s *Store) LookupRange(start, end engine.Key) *Range {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := sort.Search(len(s.rangesByKey), func(i int) bool {
		return bytes.Compare(start, s.rangesByKey[i].Meta.EndKey) < 0
	})
	if n >= len(s.rangesByKey) || !s.rangesByKey[n].Meta.ContainsKeyRange(start, end) {
		return nil
	}
	return s.rangesByKey[n]
}

// CreateRange creates a new range by allocating a new range ID and
// storing range metadata. On success, returns the new range.
//
// TODO(spencer): this method is temporary and will need to be
// removed.  In its place, ranges themselves will initiate the
// creation of a new range. This is done during splits. The range
// leader calls store.SplitRange, which allocates the raft ID and all
// range IDs (range IDs will need to be changed from the local ID
// allocator used here to a global allocator). This way, the complete
// replica slice will be available for the Raft.InternalSplitRange
// command to be executed at each of the replicas in the original
// range, yielding identical results.
func (s *Store) CreateRange(startKey, endKey engine.Key, replicas []proto.Replica) (*Range, error) {
	if len(replicas) != 1 {
		panic("CreateRange can only handle a single replica currently")
	}
	raftID, err := s.allocateRaftID()
	if err != nil {
		return nil, err
	}
	rangeID, err := engine.Increment(s.engine, engine.KeyLocalRangeIDGenerator, 1)
	if err != nil {
		return nil, err
	}
	if ok, _ := engine.GetProto(s.engine, makeRangeKey(rangeID), nil); ok {
		return nil, util.Error("range ID already in use")
	}
	// RangeMetadata is stored local to this store only. It is neither
	// replicated via raft nor available via the global kv store.
	meta := &proto.RangeMetadata{
		ClusterID: s.Ident.ClusterID,
		RangeDescriptor: proto.RangeDescriptor{
			RaftID:   raftID,
			StartKey: startKey,
			EndKey:   endKey,
			Replicas: replicas,
		},
		RangeID: rangeID,
	}
	meta.Replicas[0].RangeID = rangeID
	err = engine.PutProto(s.engine, makeRangeKey(rangeID), meta)
	if err != nil {
		return nil, err
	}
	rng := NewRange(meta, s.clock, s.engine, s.allocator, s.gossip, s)
	rng.Start()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ranges[rangeID] = rng
	// Append new ranges to rangesByKey and keep sorted.
	s.rangesByKey = append(s.rangesByKey, rng)
	sort.Sort(s.rangesByKey)
	return rng, nil
}

// Attrs returns the attributes of the underlying store.
func (s *Store) Attrs() proto.Attributes {
	return s.engine.Attrs()
}

// Capacity returns the capacity of the underlying storage engine.
func (s *Store) Capacity() (engine.StoreCapacity, error) {
	return s.engine.Capacity()
}

// Descriptor returns a StoreDescriptor including current store
// capacity information.
func (s *Store) Descriptor(nodeDesc *NodeDescriptor) (*StoreDescriptor, error) {
	capacity, err := s.Capacity()
	if err != nil {
		return nil, err
	}
	// Initialize the store descriptor.
	return &StoreDescriptor{
		StoreID:  s.Ident.StoreID,
		Attrs:    s.Attrs(),
		Node:     *nodeDesc,
		Capacity: capacity,
	}, nil
}

// ExecuteCmd fetches a range based on the header's replica, assembles
// method, args & reply into a Raft Cmd struct and executes the
// command using the fetched range.
func (s *Store) ExecuteCmd(method string, args proto.Request, reply proto.Response) error {
	// If the request has a zero timestamp, initialize to this node's clock.
	header := args.Header()
	if header.Timestamp.WallTime == 0 && header.Timestamp.Logical == 0 {
		// Update the incoming timestamp.
		now := s.clock.Now()
		args.Header().Timestamp = now
	} else {
		// Otherwise, update our clock with the incoming request. This
		// advances the local node's clock to a high water mark from
		// amongst all nodes with which it has interacted. The update is
		// bounded by the max clock drift.
		_, err := s.clock.Update(header.Timestamp)
		if err != nil {
			return err
		}
	}

	// Verify specified range contains the command's implicated keys.
	rng, err := s.GetRange(header.Replica.RangeID)
	if err != nil {
		return err
	}
	if !rng.ContainsKeyRange(header.Key, header.EndKey) {
		return proto.NewRangeKeyMismatchError(header.Key, header.EndKey, rng.Meta)
	}
	if !rng.IsLeader() {
		// TODO(spencer): when we happen to know the leader, fill it in here via replica.
		return &proto.NotLeaderError{}
	}

	// Differentiate between read-only and read-write.
	if IsReadOnly(method) {
		return rng.ReadOnlyCmd(method, args, reply)
	}
	return rng.ReadWriteCmd(method, args, reply)
}

// allocateRaftID allocates a new Raft ID from the global KV DB.
// Each store allocates blocks of IDS in raftIDAllocCount increments
// for efficiency.
func (s *Store) allocateRaftID() (int64, error) {
	// Handle the bootstrapping case where db is nil; bootstrap raft ID is 1.
	if s.db == nil {
		return 1, nil
	}

	s.mu.Lock()
	// If we have pre-alloc'd Raft IDs available, return one.
	if s.nextRaftID < s.lastRaftID {
		id := s.nextRaftID
		s.nextRaftID++
		s.mu.Unlock()
		return id, nil
	}

	// Unlock mutex in anticipation of increment.
	s.mu.Unlock()
	ir := <-s.db.Increment(&proto.IncrementRequest{
		RequestHeader: proto.RequestHeader{
			Key:  engine.KeyRaftIDGenerator,
			User: UserRoot,
		},
		Increment: raftIDAllocCount,
	})
	if ir.Error != nil {
		return 0, util.Errorf("unable to allocate raft IDs: %v", ir.Error)
	}
	if ir.NewValue <= 1 {
		return 0, util.Errorf("raft ID allocation returned invalid allocation: %+v", ir)
	}

	// Re-lock mutex after call to increment.
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastRaftID = ir.NewValue + 1
	s.nextRaftID = ir.NewValue - raftIDAllocCount + 1
	// Raft ID 1 is reserved for the bootstrap raft group.
	if s.nextRaftID <= 1 {
		s.nextRaftID = 2
	}
	s.nextRaftID++
	return s.nextRaftID - 1, nil

}

// RangeManager is an interface satisfied by Store through which ranges
// contained in the store can access the methods required for rebalancing
// (i.e. splitting and merging) operations.
// TODO(Tobias): add necessary operations as we need them.
type RangeManager interface {
	CreateRange(startKey, endKey engine.Key, replicas []proto.Replica) (*Range, error)
}
