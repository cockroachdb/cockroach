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
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/util"
)

// Constants for store-reserved keys. These keys are prefixed with
// three null characters so that they precede all global keys in the
// store's map. Data at these keys is local to this store and is not
// replicated via raft nor is it available via access to the global
// key-value store.
var (
	// keyStoreIdent store immutable identifier for this store, created
	// when store is first bootstrapped.
	keyStoreIdent = Key("\x00\x00\x00store-ident")
	// keyRangeIDGenerator is a range ID generator sequence. Range IDs
	// must be unique per node ID.
	keyRangeIDGenerator = Key("\x00\x00\x00range-id-generator")
	// keyRangeMetadataPrefix is the prefix for keys storing range metadata.
	// The value is a struct of type RangeMetadata.
	keyRangeMetadataPrefix = Key("\x00\x00\x00range-")
)

// rangeKey creates a range key as the concatenation of the
// rangeMetadataKeyPrefix and hexadecimal-formatted range ID.
func rangeKey(rangeID int64) Key {
	return MakeKey(keyRangeMetadataPrefix, Key(strconv.FormatInt(rangeID, 16)))
}

// A StoreIdent uniquely identifies a store in the cluster. The
// StoreIdent is written to the underlying storage engine at a
// store-reserved system key (keyStoreIdent).
type StoreIdent struct {
	ClusterID string
	NodeID    int32
	StoreID   int32
}

// A Store maintains a map of ranges by start key. A Store corresponds
// to one physical device.
type Store struct {
	Ident     StoreIdent
	engine    Engine           // The underlying key-value store
	allocator *allocator       // Makes allocation decisions
	gossip    *gossip.Gossip   // Passed to new ranges
	mu        sync.Mutex       // Protects the ranges map
	ranges    map[int64]*Range // Map of ranges by range ID
}

// NewStore returns a new instance of a store.
func NewStore(engine Engine, gossip *gossip.Gossip) *Store {
	return &Store{
		engine:    engine,
		allocator: &allocator{},
		gossip:    gossip,
		ranges:    make(map[int64]*Range),
	}
}

// Close calls Range.Stop() on all active ranges.
func (s *Store) Close() {
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
	ok, _, err := getI(s.engine, keyStoreIdent, &s.Ident)
	if err != nil || ok {
		return true
	}
	return false
}

// Init reads the StoreIdent from the underlying engine.
func (s *Store) Init() error {
	ok, _, err := getI(s.engine, keyStoreIdent, &s.Ident)
	if err != nil {
		return err
	} else if !ok {
		return util.Error("store has not been bootstrapped")
	}

	// TODO(spencer): scan through all range metadata and instantiate
	//   ranges. Right now we just get range id hardcoded as 1.
	var meta RangeMetadata
	ok, _, err = getI(s.engine, rangeKey(1), &meta)
	if err != nil || !ok {
		return err
	}

	rng := NewRange(meta, s.engine, s.allocator, s.gossip)
	rng.Start()
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
		return util.Errorf("bootstrap failed; non-empty map with first key %q", kvs[0].Key)
	}
	return putI(s.engine, keyStoreIdent, s.Ident)
}

// GetRange fetches a range by ID. Returns an error if no range is found.
func (s *Store) GetRange(rangeID int64) (*Range, error) {
	if rng, ok := s.ranges[rangeID]; ok {
		return rng, nil
	}
	return nil, util.Errorf("range %d not found on store", rangeID)
}

// CreateRange allocates a new range ID and stores range metadata.
// On success, returns the new range.
func (s *Store) CreateRange(startKey, endKey Key, replicas []Replica) (*Range, error) {
	rangeID, err := increment(s.engine, keyRangeIDGenerator, 1, time.Now().UnixNano())
	if err != nil {
		return nil, err
	}
	if ok, _, _ := getI(s.engine, rangeKey(rangeID), nil); ok {
		return nil, util.Error("newly allocated range id already in use")
	}
	// RangeMetadata is stored local to this store only. It is neither
	// replicated via raft nor available via the global kv store.
	meta := RangeMetadata{
		ClusterID: s.Ident.ClusterID,
		RangeID:   rangeID,
		StartKey:  startKey,
		EndKey:    endKey,
		Replicas: RangeDescriptor{
			StartKey: startKey,
			Replicas: replicas,
		},
	}
	err = putI(s.engine, rangeKey(rangeID), meta)
	if err != nil {
		return nil, err
	}
	rng := NewRange(meta, s.engine, s.allocator, s.gossip)
	rng.Start()
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
