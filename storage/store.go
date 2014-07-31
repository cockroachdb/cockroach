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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/hlc"
	"github.com/cockroachdb/cockroach/util"
)

// rangeMetadataKeyPrefix and hexadecimal-formatted range ID.
func makeRangeKey(rangeID int64) Key {
	return MakeKey(KeyLocalRangeMetadataPrefix, Key(strconv.FormatInt(rangeID, 10)))
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

// A StoreIdent uniquely identifies a store in the cluster. The
// StoreIdent is written to the underlying storage engine at a
// store-reserved system key (KeyLocalIdent).
type StoreIdent struct {
	ClusterID string
	NodeID    int32
	StoreID   int32
}

// A Store maintains a map of ranges by start key. A Store corresponds
// to one physical device.
type Store struct {
	Ident     StoreIdent
	clock     *hlc.HLClock
	engine    Engine         // The underlying key-value store
	allocator *allocator     // Makes allocation decisions
	gossip    *gossip.Gossip // Passed to new ranges

	mu      sync.RWMutex        // Protects ranges, readQ & tsCache.
	ranges  map[int64]*Range    // Map of ranges by range ID
	readQ   *ReadQueue          // Reads queued behind pending writes
	tsCache *ReadTimestampCache // Most recent read timestamps for keys / key ranges
}

// NewStore returns a new instance of a store.
func NewStore(clock *hlc.HLClock, engine Engine, gossip *gossip.Gossip) *Store {
	return &Store{
		clock:     clock,
		engine:    engine,
		allocator: &allocator{},
		gossip:    gossip,
		ranges:    make(map[int64]*Range),
		readQ:     NewReadQueue(),
		tsCache:   NewReadTimestampCache(clock),
	}
}

// Close calls Range.Stop() on all active ranges.
func (s *Store) Close() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, rng := range s.ranges {
		rng.Stop()
	}
}

// String formats a store for debug output.
func (s *Store) String() string {
	return fmt.Sprintf("store=%d:%d (%s)", s.Ident.NodeID, s.Ident.StoreID, s.engine)
}

// IsBootstrapped returns true if the store has already been
// bootstrapped. If the store ident is corrupt, IsBootstrapped will
// return true; the exact error can be retrieved via a call to Init().
func (s *Store) IsBootstrapped() bool {
	ok, err := getI(s.engine, KeyLocalIdent, &s.Ident)
	if err != nil || ok {
		return true
	}
	return false
}

// Init reads the StoreIdent from the underlying engine.
func (s *Store) Init() error {
	ok, err := getI(s.engine, KeyLocalIdent, &s.Ident)
	if err != nil {
		return err
	} else if !ok {
		return util.Error("store has not been bootstrapped")
	}

	// TODO(spencer): scan through all range metadata and instantiate
	//   ranges. Right now we just get range ID hardcoded as 1.
	var meta RangeMetadata
	ok, err = getI(s.engine, makeRangeKey(1), &meta)
	if err != nil || !ok {
		return err
	}

	rng := NewRange(meta, s.engine, s.allocator, s.gossip)
	rng.Start()

	s.mu.Lock()
	defer s.mu.Unlock()
	s.ranges[meta.RangeID] = rng
	return nil
}

// Bootstrap writes a new store ident to the underlying engine. To
// ensure that no crufty data already exists in the engine, it scans
// the engine contents before writing the new store ident. The engine
// should be completely empty. It returns an error if called on a
// non-empty engine.
func (s *Store) Bootstrap(ident StoreIdent) error {
	s.Ident = ident
	kvs, err := s.engine.scan(KeyMin, KeyMax, 1 /* only need one entry to fail! */)
	if err != nil {
		return util.Errorf("unable to scan engine to verify empty: %v", err)
	} else if len(kvs) > 0 {
		return util.Errorf("bootstrap failed; non-empty map with first key %q", kvs[0].key)
	}
	return putI(s.engine, KeyLocalIdent, s.Ident)
}

// GetRange fetches a range by ID. Returns an error if no range is found.
func (s *Store) GetRange(rangeID int64) (*Range, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if rng, ok := s.ranges[rangeID]; ok {
		return rng, nil
	}
	return nil, util.Errorf("range %d not found on store", rangeID)
}

// GetRanges fetches all ranges.
func (s *Store) GetRanges() RangeSlice {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var ranges RangeSlice
	for _, rng := range s.ranges {
		ranges = append(ranges, rng)
	}
	return ranges
	// TODO(spencer): any changes to the ranges map will need to also
	// update the caller of this method. This can probably be done
	// using a listener for a store's range changes.
}

// CreateRange allocates a new range ID and stores range metadata.
// On success, returns the new range.
func (s *Store) CreateRange(startKey, endKey Key, replicas []Replica) (*Range, error) {
	rangeID, err := increment(s.engine, KeyLocalRangeIDGenerator, 1)
	if err != nil {
		return nil, err
	}
	if ok, _ := getI(s.engine, makeRangeKey(rangeID), nil); ok {
		return nil, util.Error("newly allocated range ID already in use")
	}
	// RangeMetadata is stored local to this store only. It is neither
	// replicated via raft nor available via the global kv store.
	meta := RangeMetadata{
		ClusterID: s.Ident.ClusterID,
		RangeID:   rangeID,
		StartKey:  startKey,
		EndKey:    endKey,
		Desc: RangeDescriptor{
			StartKey: startKey,
			Replicas: replicas,
		},
	}
	err = putI(s.engine, makeRangeKey(rangeID), meta)
	if err != nil {
		return nil, err
	}
	rng := NewRange(meta, s.engine, s.allocator, s.gossip)
	rng.Start()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ranges[rangeID] = rng
	return rng, nil
}

// Attrs returns the attributes of the underlying store.
func (s *Store) Attrs() Attributes {
	return s.engine.Attrs()
}

// Capacity returns the capacity of the underlying storage engine.
func (s *Store) Capacity() (StoreCapacity, error) {
	return s.engine.capacity()
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
func (s *Store) ExecuteCmd(method string, header *RequestHeader, args, reply interface{}) error {
	// If the request has a zero timestamp, initialize to this node's clock.
	if header.Timestamp.WallTime == 0 && header.Timestamp.Logical == 0 {
		// Update both incoming and outgoing timestamps.
		header.Timestamp = s.clock.Now()
		replyVal := reflect.ValueOf(reply)
		reflect.Indirect(replyVal).FieldByName("Timestamp").Set(reflect.ValueOf(header.Timestamp))
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
		return &NotInRangeError{start: rng.Meta.StartKey, end: rng.Meta.EndKey}
	}
	if !rng.IsLeader() {
		// TODO(spencer): when we happen to know the leader, fill it in here via replica.
		return &NotLeaderError{}
	}

	// If this is a read-only command, update the read timestamp cache
	// and wait for any overlapping writes ahead of us to clear via the
	// read queue.
	if !NeedWritePerm(method) {
		s.mu.Lock()
		s.tsCache.Add(header.Key, header.EndKey, header.Timestamp)
		var wg sync.WaitGroup
		s.readQ.AddRead(header.Key, header.EndKey, &wg)
		s.mu.Unlock()
		wg.Wait()

		// It's possible that arbitrary delays (e.g. major GC, VM
		// de-prioritization, etc.) could cause the execution of this read
		// to happen AFTER the range replica has lost leadership.
		//
		// There is a chance that we waited on writes, and although they
		// were committed to the log, they weren't successfully applied to
		// this replica's state machine. However, we avoid this being a
		// problem because Range.executeCmd verifies leadership again.
		//
		// Keep in mind that it's impossible for any writes to occur to
		// keys we're reading here with timestamps before this read's
		// timestamp. This is because the read-timestamp-cache prevents it
		// for the active leader and leadership changes force the
		// read-timestamp-cache to reset its high water mark.
		return rng.executeCmd(method, args, reply)
	}

	// Otherwise, we have a mutating (read-write/write-only)
	// command. One of the prime invariants of Cockroach is that a
	// mutating command cannot write a key with an earlier timestamp
	// than the most recent read of the same key. So first order of
	// business here is to check the read timestamp cache for reads
	// which are more recent than the timestamp of this write. If more
	// recent, we simply update the write's timestamp before enqueuing
	// it for execution. When the write returns, the updated timestamp
	// will inform the final commit timestamp.
	s.mu.Lock()
	if ts := s.tsCache.GetMax(header.Key, header.EndKey); header.Timestamp.Less(ts) {
		// Update both the incoming request and outgoing reply timestamps.
		ts.Logical += 1 // increment logical component by one to differentiate.
		header.Timestamp = ts
		replyVal := reflect.ValueOf(reply)
		reflect.Indirect(replyVal).FieldByName("Timestamp").Set(reflect.ValueOf(ts))
	}

	// The next step is to add the write to the read queue to inform
	// subsequent reads that there is a pending write. Reads which
	// overlap pending writes must wait for those writes to complete.
	wKey := s.readQ.AddWrite(header.Key, header.EndKey)
	s.mu.Unlock()

	// Create command and enqueue for Raft.
	cmd := &Cmd{
		Method:   method,
		Args:     args,
		Reply:    reply,
		ReadOnly: !NeedWritePerm(method),
		done:     make(chan error, 1),
	}
	// This waits for the command to complete.
	err = rng.EnqueueCmd(cmd)

	// Now that the command has completed, remove the pending write.
	s.mu.Lock()
	s.readQ.RemoveWrite(wKey)
	s.mu.Unlock()

	return err
}
