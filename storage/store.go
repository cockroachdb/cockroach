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
	"github.com/cockroachdb/cockroach/util/log"
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
	return engine.MakeLocalKey(engine.KeyLocalRangeMetadataPrefix, engine.Key(strconv.FormatInt(rangeID, 10)))
}

// verifyKeyLength verifies key length. Extra key length is allowed for
// keys prefixed with the meta1 or meta2 addressing prefixes.
func verifyKeyLength(key engine.Key) error {
	maxLength := engine.KeyMaxLength
	if bytes.HasPrefix(key, engine.KeyMeta1Prefix) || bytes.HasPrefix(key, engine.KeyMeta2Prefix) {
		maxLength += len(engine.KeyMeta1Prefix)
	}
	if len(key) > maxLength {
		return util.Errorf("maximum key length exceeded for %q: %d > %d", key, len(key), maxLength)
	}
	return nil
}

// verifyKeys verifies key length for start and end. If end is
// non-empty, it must be >= start.
func verifyKeys(start, end engine.Key) error {
	if err := verifyKeyLength(start); err != nil {
		return err
	}
	if len(end) > 0 {
		if err := verifyKeyLength(end); err != nil {
			return err
		}
		if end.Less(start) {
			return util.Errorf("end key cannot sort before start: %q < %q", end, start)
		}
	}
	return nil
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.raftIDAlloc = NewIDAllocator(engine.KeyRaftIDGenerator, s.db, 2 /* min ID */, raftIDAllocCount)
	s.rangeIDAlloc = NewIDAllocator(engine.KeyRangeIDGenerator, s.db, 2 /* min ID */, rangeIDAllocCount)

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
	startKey := engine.KeyLocalRangeMetadataPrefix
	endKey := startKey.PrefixEnd()
	start := startKey.Encode(nil)
	end := endKey.Encode(nil)
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
		start = engine.Key(kvs[rows-1].Key).Next()
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
		return util.Errorf("unable to scan engine to verify empty: %s", err)
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
// specified key range. Note that the specified keys are transformed
// using Key.Address() to ensure we lookup ranges correctly for local
// keys.
func (s *Store) LookupRange(start, end engine.Key) *Range {
	s.mu.RLock()
	defer s.mu.RUnlock()
	startAddr := start.Address()
	endAddr := end.Address()
	n := sort.Search(len(s.rangesByKey), func(i int) bool {
		return startAddr.Less(s.rangesByKey[i].Meta.EndKey)
	})
	if n >= len(s.rangesByKey) || !s.rangesByKey[n].Meta.ContainsKeyRange(startAddr, endAddr) {
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

// NewRangeMetadata creates a new RangeMetadata based on start and end keys and
// the supplied proto.Replicas slice, which is copied. It allocates new Raft
// and range IDs to fill out the supplied RangeMetadata.
// Returns the new RangeMetadata.
func (s *Store) NewRangeMetadata(start, end engine.Key, replicas []proto.Replica) *proto.RangeMetadata {
	meta := &proto.RangeMetadata{
		ClusterID: s.Ident.ClusterID,
		RangeDescriptor: proto.RangeDescriptor{
			RaftID:   s.raftIDAlloc.Allocate(),
			StartKey: start,
			EndKey:   end,
			Replicas: append([]proto.Replica(nil), replicas...),
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

// MatchRangeID finds and returns the range ID associated to this store's replica.
func (s *Store) MatchRangeID(meta *proto.RangeMetadata) (int64, error) {
	for _, repl := range meta.Replicas {
		if repl.StoreID == s.Ident.StoreID && meta.ClusterID == s.Ident.ClusterID {
			return repl.RangeID, nil
		}
	}
	return 0, util.Errorf("unable to determine range ID for this range; no replicas match store %d: %v",
		s.Ident.StoreID, meta.Replicas)
}

// CreateRange creates a new Range using the provided RangeMetadata.
// It persists the metadata locally and adds the new range to the
// ranges map and sorted rangesByKey slice for doing range lookups
// by key.
func (s *Store) CreateRange(meta *proto.RangeMetadata) (*Range, error) {
	// Set the RangeID for meta based on the replica which matches this store.
	rangeID, err := s.MatchRangeID(meta)
	if err != nil {
		return nil, err
	}
	meta.RangeID = rangeID

	s.mu.Lock()
	if _, ok := s.ranges[meta.RangeID]; ok {
		return nil, util.Errorf("RangeID %d already registered with the store", meta.RangeID)
	}
	rng := NewRange(meta, s.clock, s.engine, s.allocator, s.gossip, s)
	err := s.writeRangeToEngine(rng)
	if err != nil {
		return nil, err
	}
	s.ranges[meta.RangeID] = rng
	s.rangesByKey = append(s.rangesByKey, rng)
	sort.Sort(s.rangesByKey)
	s.mu.Unlock()

	return rng, nil
}

// UpdateRange writes the range metadata of a range contained in this store to
// the engine, overwriting previously stored metadata for the given rangeID.
func (s *Store) UpdateRange(rangeID int64) error {
	rng, err := s.GetRange(rangeID)
	if err != nil {
		return err
	}
	return s.writeRangeToEngine(rng)
}

// writeRangeToEngine does the actual work for UpdateRange().
func (s *Store) writeRangeToEngine(rng *Range) error {
	if rng == nil {
		return util.Error("no range given")
	}
	return engine.PutProto(s.engine, makeRangeKey(rng.Meta.RangeID).Encode(nil), rng.Meta)
}

// DropRange removes the specified range from the store's map, and also
// depersists the range so that the store will not know about it the next time
// it is initialized. DropRange does not affect any of the data stored inside
// the range and it will not Stop() it.
//
// For a rangeID that is not known to the store, DropRange is a noop.
func (s *Store) DropRange(rangeID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rng, ok := s.ranges[rangeID]; ok {
		rng.Stop()
	}
	delete(s.ranges, rangeID)
	if err := s.engine.Clear(makeRangeKey(rangeID).Encode(nil)); err != nil {
		return err
	}
	return nil
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
	if err := verifyKeys(header.Key, header.EndKey); err != nil {
		return err
	}
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

	// Get range and add command to the range for execution.
	rng, err := s.GetRange(header.Replica.RangeID)
	if err != nil {
		return err
	}
	if err := rng.AddCmd(method, args, reply, true); err == nil {
		return nil
	}
	// Maybe resolve a potential write intent error. We do this here
	// because this is the code path with the requesting client
	// waiting. We don't want every replica to attempt to resolve the
	// intent independently, so we can't do it in Range.executeCmd.
	return s.maybeResolveWriteIntentError(rng, method, args, reply)
}

// maybeResolveWriteIntentError checks the reply's error. If the error
// is a writeIntentError, it tries to push the conflicting
// transaction: either move its timestamp forward on a read/write
// conflict, or abort it on a write/write conflict. If the push
// succeeds, we immediately issue a resolve intent command and set the
// error's Resolved flag to true so the client retries the command
// immediately. If the push fails, we set the error's Resolved flag to
// false so that the client backs off before reissuing the command.
func (s *Store) maybeResolveWriteIntentError(rng *Range, method string, args proto.Request, reply proto.Response) error {
	err := reply.Header().GoError()
	wiErr, ok := err.(*proto.WriteIntentError)
	if !ok {
		return err
	}

	log.V(1).Infof("resolving write intent on %s %q: %s", method, args.Header().Key, wiErr)

	// Attempt to push the transaction which created the conflicting intent.
	pushArgs := &proto.InternalPushTxnRequest{
		RequestHeader: proto.RequestHeader{
			Timestamp:    args.Header().Timestamp,
			Key:          engine.MakeLocalKey(engine.KeyLocalTransactionPrefix, wiErr.Txn.ID), // Address to pushee's txn
			User:         args.Header().User,
			UserPriority: args.Header().UserPriority,
			Txn:          args.Header().Txn,
		},
		PusheeTxn: wiErr.Txn,
		Abort:     !IsReadOnly(method), // abort if cmd isn't read-only
	}
	pushReply := <-s.db.InternalPushTxn(pushArgs)
	if pushReply.Header().GoError() != nil {
		log.V(1).Infof("push %q failed: %s", pushArgs.Header().Key, pushReply.Header().GoError())

		// For write/write conflicts, propagate the push failure, not the
		// original write intent error. The push failure will instruct the
		// client to restart the transaction with a backoff.
		if !IsReadOnly(method) {
			reply.Header().Error = pushReply.Header().Error
			return reply.Header().GoError()
		}
		// For read/write conflicts, return the write intent error which
		// engages client's backoff/retry (with !Resolved). We don't need
		// to restart the txn, only resend the read with a backoff.
		return err
	}

	// Note that even though we're setting Resolved = true here, it'll
	// never be set in the response cache (the response containing this
	// write intent error was cached right after the command was
	// executed in Range.executeCmd). This means that a client which
	// retries the request will always backoff, even if backoff isn't
	// necessary. This doesn't affect correctness, only possibly adds
	// minor latency for an unusual case.
	wiErr.Resolved = true
	// Also, update the transaction record with result of push.
	wiErr.Txn = *pushReply.PusheeTxn

	resolveArgs := &proto.InternalResolveIntentRequest{
		RequestHeader: proto.RequestHeader{
			// Use the pushee's timestamp, which might be lower than the
			// pusher's request timestamp. No need to push the intent higher
			// than the pushee's txn!
			Timestamp: pushReply.PusheeTxn.Timestamp,
			Key:       wiErr.Key,
			User:      UserRoot,
			Txn:       pushReply.PusheeTxn,
		},
	}
	resolveReply := &proto.InternalResolveIntentResponse{}
	// Add resolve command with wait=false to add to Raft but not wait for completion.
	if resolveErr := rng.AddCmd(InternalResolveIntent, resolveArgs, resolveReply, false); resolveErr != nil {
		log.Warningf("resolve %+v failed: +v", resolveArgs, resolveErr)
	}

	// If the command is read-write, we must return the error to the
	// client so it can resubmit the command with a new ClientCmdID. For
	// read-only commands, we can resubmit immediately instead.
	if IsReadOnly(method) {
		return rng.AddCmd(method, args, reply, true)
	}
	return wiErr
}

// RunTransaction passes through to the DB contained in the store.
func (s *Store) RunTransaction(opts *TransactionOptions, retryable func(db DB) error) error {
	return s.db.RunTransaction(opts, retryable)
}

// RangeManager is an interface satisfied by Store through which ranges
// contained in the store can access the methods required for rebalancing
// (i.e. splitting and merging) operations.
// TODO(Tobias): add necessary operations as we need them.
type RangeManager interface {
	NewRangeMetadata(start, end engine.Key, replicas []proto.Replica) *proto.RangeMetadata
	MatchRangeID(meta *proto.RangeMetadata) (int64, error)
	GetRange(rangeID int64) (*Range, error)
	CreateRange(meta *proto.RangeMetadata) (*Range, error)
	UpdateRange(rangeID int64) error
	DropRange(rangeID int64) error
	RunTransaction(opts *TransactionOptions, retryable func(db DB) error) error
}
