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

	gogoproto "code.google.com/p/gogoprotobuf/proto"
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
	// raftIDAllocCount is the number of Raft IDs to allocate per allocation.
	raftIDAllocCount = 10
	// rangeIDAllocCount is the number of range IDs to allocate per allocation.
	rangeIDAllocCount = 10
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
	Ident        proto.StoreIdent
	clock        *hlc.Clock
	engine       engine.Engine  // The underlying key-value store
	db           DB             // Cockroach KV DB
	allocator    *allocator     // Makes allocation decisions
	gossip       *gossip.Gossip // Passed to new ranges
	raftIDAlloc  *IDAllocator   // Raft ID allocator
	rangeIDAlloc *IDAllocator   // Range ID allocator

	mu          sync.RWMutex     // Protects variables below...
	ranges      map[int64]*Range // Map of ranges by range ID
	rangesByKey RangeSlice       // Sorted slice of ranges by StartKey
}

// NewStore returns a new instance of a store.
func NewStore(clock *hlc.Clock, eng engine.Engine, db DB, gossip *gossip.Gossip) *Store {
	return &Store{
		clock:     clock,
		engine:    eng,
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
	// Close store for idempotency.
	s.Close()

	// Start engine and set garbage collector.
	if err := s.engine.Start(); err != nil {
		return err
	}

	// Create ID allocators.
	s.raftIDAlloc = NewIDAllocator(engine.KeyRaftIDGenerator, s.db, 2, raftIDAllocCount)
	s.rangeIDAlloc = NewIDAllocator(engine.KeyRangeIDGenerator, s.db, 2, rangeIDAllocCount)

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

	s.mu.Lock()
	defer s.mu.Unlock()
	start := engine.KeyLocalRangeMetadataPrefix
	end := engine.PrefixEndKey(start)
	const rows = 64
	for {
		kvs, err := s.engine.Scan(start, end, rows)
		if err != nil {
			return err
		}
		for _, kv := range kvs {
			var meta proto.RangeMetadata
			if err := gogoproto.Unmarshal(kv.Value, &meta); err != nil {
				return err
			}
			rng := NewRange(&meta, s.clock, s.engine, s.allocator, s.gossip, s)
			rng.Start()
			s.ranges[meta.RangeID] = rng
			s.rangesByKey = append(s.rangesByKey, rng)
		}
		if len(kvs) < rows {
			break
		}
		start = engine.NextKey(kvs[rows-1].Key)
	}

	// Ensure that ranges are sorted.
	sort.Sort(s.rangesByKey)

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

// BootstrapRangeMetadata returns a range metadata for the very first range
// in a cluster.
func (s *Store) BootstrapRangeMetadata() *proto.RangeMetadata {
	return &proto.RangeMetadata{
		ClusterID: s.Ident.ClusterID,
		RangeDescriptor: proto.RangeDescriptor{
			RaftID:   1,
			StartKey: engine.KeyMin,
			EndKey:   engine.KeyMax,
			Replicas: []proto.Replica{
				proto.Replica{
					NodeID:  1,
					StoreID: 1,
					RangeID: 1,
				},
			},
		},
		RangeID: 1,
	}
}

// NewRangeMetadata creates a new RangeMetadata based on start and
// end keys and the supplied proto.Replicas slice. It allocates new
// Raft and range IDs to fill out the supplied RangeMetadata. Returns
// the new RangeMetadata.
func (s *Store) NewRangeMetadata(start, end engine.Key, replicas []proto.Replica) *proto.RangeMetadata {
	meta := &proto.RangeMetadata{
		ClusterID: s.Ident.ClusterID,
		RangeDescriptor: proto.RangeDescriptor{
			RaftID:   s.raftIDAlloc.Allocate(),
			StartKey: start,
			EndKey:   end,
			Replicas: replicas,
		},
		// Note that RangeID is specifically left blank, as it varies
		// for each replica which belongs to the range.
	}

	// Allocate a range ID for each replica.
	for i := range meta.Replicas {
		meta.Replicas[i].RangeID = s.rangeIDAlloc.Allocate()
	}

	return meta
}

// CreateRange creates a new Range using the provided RangeMetadata.
// It persists the metadata locally and adds the new range to the
// ranges map and sorted rangesByKey slice for doing range lookups
// by key.
func (s *Store) CreateRange(meta *proto.RangeMetadata) (*Range, error) {
	// Set the RangeID for meta based on the replica which matches this store.
	for _, repl := range meta.Replicas {
		if repl.StoreID == s.Ident.StoreID {
			meta.RangeID = repl.RangeID
			break
		}
	}
	if meta.RangeID == 0 {
		return nil, util.Errorf("unable to determine range ID for this range; no replicas match store %d: %s",
			s.Ident.StoreID, meta.Replicas)
	}

	err := engine.PutProto(s.engine, makeRangeKey(meta.RangeID), meta)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	rng := NewRange(meta, s.clock, s.engine, s.allocator, s.gossip, s)
	rng.Start()
	s.ranges[meta.RangeID] = rng
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

// RangeManager is an interface satisfied by Store through which ranges
// contained in the store can access the methods required for rebalancing
// (i.e. splitting and merging) operations.
// TODO(Tobias): add necessary operations as we need them.
type RangeManager interface {
	NewRangeMetadata(start, end engine.Key, replicas []proto.Replica) *proto.RangeMetadata
	CreateRange(meta *proto.RangeMetadata) (*Range, error)
}
