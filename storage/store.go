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
	"flag"
	"fmt"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/coreos/etcd/raft/raftpb"
	gogoproto "github.com/gogo/protobuf/proto"
)

const (
	// UserRoot is the username for the root user.
	UserRoot = "root"
	// GCResponseCacheExpiration is the expiration duration for response
	// cache entries.
	GCResponseCacheExpiration = 1 * time.Hour
	// raftIDAllocCount is the number of Raft IDs to allocate per allocation.
	raftIDAllocCount = 10
	// defaultScanInterval is the default value for the scan interval
	// command line flag.
	defaultScanInterval = 10 * time.Minute
	// ttlCapacityGossip is time-to-live for capacity-related info.
	ttlCapacityGossip = 2 * time.Minute
)

var (
	// defaultRangeRetryOptions are default retry options for retrying commands
	// sent to the store's ranges, for WriteTooOld and WriteIntent errors.
	defaultRangeRetryOptions = util.RetryOptions{
		Backoff:     50 * time.Millisecond,
		MaxBackoff:  5 * time.Second,
		Constant:    2,
		MaxAttempts: 0, // retry indefinitely
	}

	scanInterval = flag.Duration("scan_interval", defaultScanInterval, "specify "+
		"--scan_interval to adjust the target for the duration of a single scan "+
		"through a store's ranges. The scan is slowed as necessary to approximately"+
		"achieve this duration.")
)

var (
	changeTypeRaftToInternal = map[raftpb.ConfChangeType]proto.ReplicaChangeType{
		raftpb.ConfChangeAddNode:    proto.ADD_REPLICA,
		raftpb.ConfChangeRemoveNode: proto.REMOVE_REPLICA,
	}
	changeTypeInternalToRaft = map[proto.ReplicaChangeType]raftpb.ConfChangeType{
		proto.ADD_REPLICA:    raftpb.ConfChangeAddNode,
		proto.REMOVE_REPLICA: raftpb.ConfChangeRemoveNode,
	}
)

// verifyKeyLength verifies key length. Extra key length is allowed for
// the local key prefix (for example, a transaction record), and also for
// keys prefixed with the meta1 or meta2 addressing prefixes. There is a
// special case for both key-local AND meta1 or meta2 addressing prefixes.
func verifyKeyLength(key proto.Key) error {
	maxLength := engine.KeyMaxLength
	if bytes.HasPrefix(key, engine.KeyLocalRangeKeyPrefix) {
		key = key[len(engine.KeyLocalRangeKeyPrefix):]
		_, key = encoding.DecodeBytes(key)
	}
	if bytes.HasPrefix(key, engine.KeyMetaPrefix) {
		key = key[len(engine.KeyMeta1Prefix):]
	}
	if len(key) > maxLength {
		return util.Errorf("maximum key length exceeded for %q", key)
	}
	return nil
}

// verifyKeys verifies key length for start and end. Also verifies
// that start key is less than KeyMax and end key is less than or
// equal to KeyMax. If end is non-empty, it must be >= start.
func verifyKeys(start, end proto.Key) error {
	if err := verifyKeyLength(start); err != nil {
		return err
	}
	if !start.Less(engine.KeyMax) {
		return util.Errorf("start key %q must be less than KeyMax", start)
	}
	if len(end) > 0 {
		if err := verifyKeyLength(end); err != nil {
			return err
		}
		if engine.KeyMax.Less(end) {
			return util.Errorf("end key %q must be less than or equal to KeyMax", end)
		}
		if end.Less(start) {
			return util.Errorf("end key cannot sort before start: %q < %q", end, start)
		}
	}
	return nil
}

// MakeRaftNodeID packs a NodeID and StoreID into a single uint64 for use in raft.
func MakeRaftNodeID(n proto.NodeID, s proto.StoreID) multiraft.NodeID {
	if n < 0 || s <= 0 {
		// Zeroes are likely the result of incomplete initialization.
		// TODO(bdarnell): should we disallow NodeID==0? It should never occur in
		// production but many tests use it.
		panic("NodeID must be >= 0 and StoreID must be > 0")
	}
	return multiraft.NodeID(n)<<32 | multiraft.NodeID(s)
}

// DecodeRaftNodeID converts a multiraft NodeID into its component NodeID and StoreID.
func DecodeRaftNodeID(n multiraft.NodeID) (proto.NodeID, proto.StoreID) {
	return proto.NodeID(n >> 32), proto.StoreID(n & 0xffffffff)
}

type rangeAlreadyExists struct {
	rng *Range
}

// Error implements the error interface.
func (e *rangeAlreadyExists) Error() string {
	return fmt.Sprintf("range for Raft ID %d already exists on store", e.rng.Desc().RaftID)
}

// A RangeSlice is a slice of Range pointers used for replica lookups
// by key.
type RangeSlice []*Range

// Implementation of sort.Interface which sorts by StartKey from each
// range's descriptor.
func (rs RangeSlice) Len() int {
	return len(rs)
}
func (rs RangeSlice) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}
func (rs RangeSlice) Less(i, j int) bool {
	return bytes.Compare(rs[i].Desc().StartKey, rs[j].Desc().StartKey) < 0
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
	NodeID  proto.NodeID
	Address net.Addr
	Attrs   proto.Attributes // node specific attributes (e.g. datacenter, machine info)
}

// NodeIDToAddress looks up the address of the node from the given Gossip instance.
func NodeIDToAddress(g *gossip.Gossip, nodeID proto.NodeID) (net.Addr, error) {
	nodeIDKey := gossip.MakeNodeIDKey(nodeID)
	info, err := g.GetInfo(nodeIDKey)
	if info == nil || err != nil {
		return nil, util.Errorf("Unable to lookup address for node: %d. Error: %s", nodeID, err)
	}
	return info.(*NodeDescriptor).Address, nil
}

// StoreDescriptor holds store information including store attributes,
// node descriptor and store capacity.
type StoreDescriptor struct {
	StoreID  proto.StoreID
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

// storeRangeIterator is an implementation of rangeIterator which
// cycles through a store's rangesByKey slice.
type storeRangeIterator struct {
	store     *Store
	remaining int
	index     int
}

func newStoreRangeIterator(store *Store) *storeRangeIterator {
	r := &storeRangeIterator{
		store: store,
	}
	r.Reset()
	return r
}

func (si *storeRangeIterator) Next() *Range {
	si.store.mu.Lock()
	defer si.store.mu.Unlock()
	if index, remaining := si.index, len(si.store.rangesByKey)-si.index; remaining > 0 {
		si.index++
		si.remaining = remaining - 1
		return si.store.rangesByKey[index]
	}
	return nil
}

func (si *storeRangeIterator) EstimatedCount() int {
	return si.remaining
}

func (si *storeRangeIterator) Reset() {
	si.store.mu.Lock()
	defer si.store.mu.Unlock()
	si.remaining = len(si.store.rangesByKey)
	si.index = 0
}

// StoreConfig contains various parameters of a Store.
type StoreConfig struct {
	// RaftTickInterval is the resolution of the Raft timer; other raft timeouts
	// are defined in terms of multiples of this value.
	RaftTickInterval time.Duration

	// RaftHeartbeatIntervalTicks is the number of ticks that pass between heartbeats.
	RaftHeartbeatIntervalTicks int

	// RaftElectionTimeoutTicks is the number of ticks that must pass before a follower
	// considers a leader to have failed and calls a new election. Should be significantly
	// higher than RaftHeartbeatIntervalTicks. The raft paper recommends a value of 150ms
	// for local networks.
	RaftElectionTimeoutTicks int
}

// setDefaults initializes unset fields in StoreConfig to values
// suitable for use on a local network.
func (c *StoreConfig) setDefaults() {
	if c.RaftTickInterval == 0 {
		c.RaftTickInterval = 10 * time.Millisecond
	}
	if c.RaftHeartbeatIntervalTicks == 0 {
		c.RaftHeartbeatIntervalTicks = 3
	}
	if c.RaftElectionTimeoutTicks == 0 {
		c.RaftElectionTimeoutTicks = 15
	}
}

// TestStoreConfig is a StoreConfig for use in tests which uses very short timeouts.
var TestStoreConfig = StoreConfig{
	RaftTickInterval:           time.Millisecond,
	RaftHeartbeatIntervalTicks: 1,
	RaftElectionTimeoutTicks:   5,
}

// A Store maintains a map of ranges by start key. A Store corresponds
// to one physical device.
type Store struct {
	*StoreFinder
	StoreConfig

	Ident          proto.StoreIdent
	RetryOpts      util.RetryOptions
	clock          *hlc.Clock
	engine         engine.Engine       // The underlying key-value store
	db             *client.KV          // Cockroach KV DB
	allocator      *allocator          // Makes allocation decisions
	gossip         *gossip.Gossip      // Configs and store capacities
	transport      multiraft.Transport // Log replication traffic
	raftIDAlloc    *IDAllocator        // Raft ID allocator
	gcQueue        *gcQueue            // Garbage collection queue
	splitQueue     *splitQueue         // Range splitting queue
	verifyQueue    *verifyQueue        // Checksum verification queue
	replicateQueue *replicateQueue     // Replication queue
	scanner        *rangeScanner       // Range scanner
	multiraft      *multiraft.MultiRaft
	started        int32
	stopper        *util.Stopper

	mu          sync.RWMutex     // Protects variables below...
	ranges      map[int64]*Range // Map of ranges by Raft ID
	rangesByKey RangeSlice       // Sorted slice of ranges by StartKey
}

var _ multiraft.Storage = &Store{}

// NewStore returns a new instance of a store.
func NewStore(clock *hlc.Clock, eng engine.Engine, db *client.KV, gossip *gossip.Gossip,
	transport multiraft.Transport, config StoreConfig) *Store {
	config.setDefaults()
	sf := newStoreFinder(gossip)
	s := &Store{
		StoreFinder: sf,
		StoreConfig: config,
		RetryOpts:   defaultRangeRetryOptions,
		clock:       clock,
		engine:      eng,
		db:          db,
		allocator:   newAllocator(sf.findStores),
		gossip:      gossip,
		transport:   transport,
		ranges:      map[int64]*Range{},
	}

	// Add range scanner and configure with queues.
	s.scanner = newRangeScanner(defaultScanInterval, newStoreRangeIterator(s))
	s.gcQueue = newGCQueue()
	s.splitQueue = newSplitQueue(db, gossip)
	s.verifyQueue = newVerifyQueue(s.scanner.Stats)
	s.replicateQueue = newReplicateQueue(gossip, s.allocator, clock)
	s.scanner.AddQueues(s.gcQueue, s.splitQueue, s.verifyQueue, s.replicateQueue)

	return s
}

// String formats a store for debug output.
func (s *Store) String() string {
	return fmt.Sprintf("store=%d:%d (%s)", s.Ident.NodeID, s.Ident.StoreID, s.engine)
}

// IsStarted returns true if the Store has been started.
func (s *Store) IsStarted() bool {
	return atomic.LoadInt32(&s.started) == 1
}

// Start the engine, set the GC and read the StoreIdent.
func (s *Store) Start(stopper *util.Stopper) error {
	s.stopper = stopper

	if s.Ident.NodeID == 0 {
		// Open engine (i.e. initialize RocksDB database). "NodeID != 0"
		// implies the engine has already been opened.
		if err := s.engine.Open(); err != nil {
			return err
		}
		s.stopper.AddCloser(s.engine)

		// Read store ident and return a not-bootstrapped error if necessary.
		ok, err := engine.MVCCGetProto(s.engine, engine.StoreIdentKey(), proto.ZeroTimestamp, true,
			nil, &s.Ident)
		if err != nil {
			return err
		} else if !ok {
			return &NotBootstrappedError{}
		}
	}

	// Create ID allocators.
	idAlloc, err := NewIDAllocator(engine.KeyRaftIDGenerator, s.db, 2 /* min ID */, raftIDAllocCount, s.stopper)
	if err != nil {
		return err
	}
	s.raftIDAlloc = idAlloc

	// GCTimeouts method is called each time an engine compaction is
	// underway. It sets minimum timeouts for transaction records and
	// response cache entries.
	now := s.clock.Now()
	minTxnTS := int64(0) // disable GC of transactions until we know minimum write intent age
	minRCacheTS := now.WallTime - GCResponseCacheExpiration.Nanoseconds()
	s.engine.SetGCTimeouts(minTxnTS, minRCacheTS)

	// Iterator over all range-local key-based data.
	start := engine.RangeDescriptorKey(engine.KeyMin)
	end := engine.RangeDescriptorKey(engine.KeyMax)

	if s.multiraft, err = multiraft.NewMultiRaft(s.RaftNodeID(), &multiraft.Config{
		Transport:              s.transport,
		Storage:                s,
		StateMachine:           s,
		TickInterval:           s.RaftTickInterval,
		ElectionTimeoutTicks:   s.RaftElectionTimeoutTicks,
		HeartbeatIntervalTicks: s.RaftHeartbeatIntervalTicks,
		EntryFormatter:         raftEntryFormatter,
	}); err != nil {
		return err
	}

	// Iterate over all range descriptors, ignoring uncommitted versions
	// (consistent=false). Uncommitted intents which have been abandoned
	// due to a split crashing halfway will simply be resolved on the
	// next split attempt. They can otherwise be ignored.
	if err := engine.MVCCIterate(s.engine, start, end, now, false, nil, func(kv proto.KeyValue) (bool, error) {
		// Only consider range metadata entries; ignore others.
		_, suffix, _ := engine.DecodeRangeKey(kv.Key)
		if !suffix.Equal(engine.KeyLocalRangeDescriptorSuffix) {
			return false, nil
		}
		var desc proto.RangeDescriptor
		if err := gogoproto.Unmarshal(kv.Value.Bytes, &desc); err != nil {
			return false, err
		}
		rng, err := NewRange(&desc, s)
		if err != nil {
			return false, err
		}
		s.mu.Lock()
		err = s.addRangeInternal(rng, false /* don't sort on each addition */)
		s.mu.Unlock()
		if err != nil {
			return false, err
		}
		// Note that we do not create raft groups at this time; they will be created
		// on-demand the first time they are needed. This helps reduce the amount of
		// election-related traffic in a cold start.
		// Raft initialization occurs when we propose a command on this range or
		// receive a raft message addressed to it.
		// TODO(bdarnell): Also initialize raft groups when read leases are needed.
		// TODO(bdarnell): Scan all ranges at startup for unapplied log entries
		// and initialize those groups.
		return false, nil
	}); err != nil {
		return err
	}
	// Sort the rangesByKey slice after they've all been added.
	sort.Sort(s.rangesByKey)

	// Start Raft processing goroutines.
	s.multiraft.Start(s.stopper)
	s.processRaft()

	// Start the scanner.
	s.scanner.Start(s.clock, s.stopper)

	// Register callbacks for any changes to accounting and zone
	// configurations; we split ranges along prefix boundaries.
	// Gossip is only ever nil for unittests.
	if s.gossip != nil {
		s.gossip.RegisterCallback(gossip.KeyConfigAccounting, s.configGossipUpdate)
		s.gossip.RegisterCallback(gossip.KeyConfigZone, s.configGossipUpdate)
		// Callback triggers on capacity gossip from all stores.
		capacityRegex := gossip.MakePrefixPattern(gossip.KeyMaxAvailCapacityPrefix)
		s.gossip.RegisterCallback(capacityRegex, s.capacityGossipUpdate)
	}

	// Set the started flag (for unittests).
	atomic.StoreInt32(&s.started, 1)

	return nil
}

// configGossipUpdate is a callback for gossip updates to
// configuration maps which affect range split boundaries.
func (s *Store) configGossipUpdate(key string, contentsChanged bool) {
	if !contentsChanged {
		return // Skip update if it's just a newer timestamp or fewer hops to info
	}
	info, err := s.gossip.GetInfo(key)
	if err != nil {
		log.Errorf("unable to fetch %s config from gossip: %s", key, err)
		return
	}
	configMap, ok := info.(PrefixConfigMap)
	if !ok {
		log.Errorf("gossiped info is not a prefix configuration map: %+v", info)
		return
	}
	s.maybeSplitRangesByConfigs(configMap)

	// If the zone configs changed, run through ranges and set max bytes.
	if key == gossip.KeyConfigZone {
		s.setRangesMaxBytes(configMap)
	}
}

// GossipCapacity broadcasts the node's capacity on the gossip network.
func (s *Store) GossipCapacity(n *NodeDescriptor) {
	storeDesc, err := s.Descriptor(n)
	if err != nil {
		log.Warningf("problem getting store descriptor for store %+v: %v", s.Ident, err)
		return
	}
	// Unique gossip key per store.
	keyMaxCapacity := gossip.MakeMaxAvailCapacityKey(storeDesc.Node.NodeID, storeDesc.StoreID)
	// Gossip store descriptor.
	s.gossip.AddInfo(keyMaxCapacity, *storeDesc, ttlCapacityGossip)
}

// maybeSplitRangesByConfigs determines ranges which should be
// split by the boundaries of the prefix config map, if any, and
// adds them to the split queue.
func (s *Store) maybeSplitRangesByConfigs(configMap PrefixConfigMap) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, config := range configMap {
		// Find the range which contains this config prefix, if any.
		n := sort.Search(len(s.rangesByKey), func(i int) bool {
			return config.Prefix.Less(s.rangesByKey[i].Desc().EndKey)
		})
		// If the config doesn't split the range, continue.
		if n >= len(s.rangesByKey) || !s.rangesByKey[n].Desc().ContainsKey(config.Prefix) {
			continue
		}
		s.splitQueue.MaybeAdd(s.rangesByKey[n], s.clock.Now())
	}
}

// ForceReplicationScan iterates over all ranges and enqueues any that need to be
// replicated. Exposed only for testing.
func (s *Store) ForceReplicationScan() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, r := range s.ranges {
		s.replicateQueue.MaybeAdd(r, s.clock.Now())
	}
}

// setRangesMaxBytes sets the max bytes for every range according
// to the zone configs.
//
// TODO(spencer): scanning all ranges with the lock held could cause
// perf issues if the number of ranges grows large enough.
func (s *Store) setRangesMaxBytes(zoneMap PrefixConfigMap) {
	s.mu.Lock()
	defer s.mu.Unlock()
	zone := zoneMap[0].Config.(*proto.ZoneConfig)
	idx := 0
	// Note that we must iterate through the ranges in lexicographic
	// order to match the ordering of the zoneMap.
	for _, rng := range s.rangesByKey {
		if idx < len(zoneMap)-1 && !rng.Desc().StartKey.Less(zoneMap[idx+1].Prefix) {
			idx++
			zone = zoneMap[idx].Config.(*proto.ZoneConfig)
		}
		rng.SetMaxBytes(zone.RangeMaxBytes)
	}
}

// Bootstrap writes a new store ident to the underlying engine. To
// ensure that no crufty data already exists in the engine, it scans
// the engine contents before writing the new store ident. The engine
// should be completely empty. It returns an error if called on a
// non-empty engine.
func (s *Store) Bootstrap(ident proto.StoreIdent, stopper *util.Stopper) error {
	if s.Ident.NodeID != 0 {
		return util.Errorf("engine already bootstrapped")
	}
	if err := s.engine.Open(); err != nil {
		return err
	}
	stopper.AddCloser(s.engine)
	s.Ident = ident
	kvs, err := engine.Scan(s.engine, proto.EncodedKey(engine.KeyMin), proto.EncodedKey(engine.KeyMax), 1)
	if err != nil {
		return util.Errorf("unable to scan engine to verify empty: %s", err)
	} else if len(kvs) > 0 {
		return util.Errorf("non-empty engine %s (first key: %q)", s.engine, kvs[0].Key)
	}
	err = engine.MVCCPutProto(s.engine, nil, engine.StoreIdentKey(), proto.ZeroTimestamp, nil, &s.Ident)
	return err
}

// GetRange fetches a range by Raft ID. Returns an error if no range is found.
func (s *Store) GetRange(raftID int64) (*Range, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if rng, ok := s.ranges[raftID]; ok {
		return rng, nil
	}
	log.Infof("couldn't find range %d in %s", raftID, s.ranges)
	return nil, proto.NewRangeNotFoundError(raftID)
}

// LookupRange looks up a range via binary search over the sorted
// "rangesByKey" RangeSlice. Returns nil if no range is found for
// specified key range. Note that the specified keys are transformed
// using Key.Address() to ensure we lookup ranges correctly for local
// keys.
func (s *Store) LookupRange(start, end proto.Key) *Range {
	s.mu.RLock()
	defer s.mu.RUnlock()
	startAddr := engine.KeyAddress(start)
	endAddr := engine.KeyAddress(end)
	n := sort.Search(len(s.rangesByKey), func(i int) bool {
		return startAddr.Less(s.rangesByKey[i].Desc().EndKey)
	})
	if n >= len(s.rangesByKey) || !s.rangesByKey[n].Desc().ContainsKeyRange(startAddr, endAddr) {
		return nil
	}
	return s.rangesByKey[n]
}

// BootstrapRange creates the first range in the cluster and manually
// writes it to the store. Default range addressing records are
// created for meta1 and meta2. Default configurations for accounting,
// permissions, and zones are created. All configs are specified for
// the empty key prefix, meaning they apply to the entire
// database. Permissions are granted to all users and the zone
// requires three replicas with no other specifications. It also adds
// the range tree and the root node, the first range, to it.
func (s *Store) BootstrapRange() error {
	desc := &proto.RangeDescriptor{
		RaftID:   1,
		StartKey: engine.KeyMin,
		EndKey:   engine.KeyMax,
		Replicas: []proto.Replica{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	batch := s.engine.NewBatch()
	ms := &engine.MVCCStats{}
	now := s.clock.Now()

	// Range descriptor.
	if err := engine.MVCCPutProto(batch, ms, engine.RangeDescriptorKey(desc.StartKey), now, nil, desc); err != nil {
		return err
	}
	// GC Metadata.
	gcMeta := proto.NewGCMetadata(now.WallTime)
	if err := engine.MVCCPutProto(batch, ms, engine.RangeGCMetadataKey(desc.RaftID), proto.ZeroTimestamp, nil, gcMeta); err != nil {
		return err
	}
	// Verification timestamp.
	if err := engine.MVCCPutProto(batch, ms, engine.RangeLastVerificationTimestampKey(desc.RaftID), proto.ZeroTimestamp, nil, &now); err != nil {
		return err
	}
	// Range addressing for meta2.
	meta2Key := engine.RangeMetaKey(engine.KeyMax)
	if err := engine.MVCCPutProto(batch, ms, meta2Key, now, nil, desc); err != nil {
		return err
	}
	// Range addressing for meta1.
	meta1Key := engine.RangeMetaKey(meta2Key)
	if err := engine.MVCCPutProto(batch, ms, meta1Key, now, nil, desc); err != nil {
		return err
	}
	// Accounting config.
	acctConfig := &proto.AcctConfig{}
	key := engine.MakeKey(engine.KeyConfigAccountingPrefix, engine.KeyMin)
	if err := engine.MVCCPutProto(batch, ms, key, now, nil, acctConfig); err != nil {
		return err
	}
	// Permission config.
	permConfig := &proto.PermConfig{
		Read:  []string{UserRoot}, // root user
		Write: []string{UserRoot}, // root user
	}
	key = engine.MakeKey(engine.KeyConfigPermissionPrefix, engine.KeyMin)
	if err := engine.MVCCPutProto(batch, ms, key, now, nil, permConfig); err != nil {
		return err
	}
	// Zone config.
	zoneConfig := &proto.ZoneConfig{
		ReplicaAttrs: []proto.Attributes{
			{},
			{},
			{},
		},
		RangeMinBytes: 1048576,
		RangeMaxBytes: 67108864,
		GC: &proto.GCPolicy{
			TTLSeconds: 24 * 60 * 60, // 1 day
		},
	}
	key = engine.MakeKey(engine.KeyConfigZonePrefix, engine.KeyMin)
	if err := engine.MVCCPutProto(batch, ms, key, now, nil, zoneConfig); err != nil {
		return err
	}
	// Range Tree setup.
	if err := SetupRangeTree(batch, ms, now, desc.StartKey); err != nil {
		return err
	}

	ms.MergeStats(batch, 1)
	if err := batch.Commit(); err != nil {
		return err
	}
	return nil
}

// The following methods implement the RangeManager interface.

// ClusterID accessor.
func (s *Store) ClusterID() string { return s.Ident.ClusterID }

// StoreID accessor.
func (s *Store) StoreID() proto.StoreID { return s.Ident.StoreID }

// RaftNodeID accessor.
func (s *Store) RaftNodeID() multiraft.NodeID {
	return MakeRaftNodeID(s.Ident.NodeID, s.Ident.StoreID)
}

// Clock accessor.
func (s *Store) Clock() *hlc.Clock { return s.clock }

// Engine accessor.
func (s *Store) Engine() engine.Engine { return s.engine }

// DB accessor.
func (s *Store) DB() *client.KV { return s.db }

// Allocator accessor.
func (s *Store) Allocator() *allocator { return s.allocator }

// Gossip accessor.
func (s *Store) Gossip() *gossip.Gossip { return s.gossip }

// SplitQueue accessor.
func (s *Store) SplitQueue() *splitQueue { return s.splitQueue }

// Stopper accessor.
func (s *Store) Stopper() *util.Stopper { return s.stopper }

// NewRangeDescriptor creates a new descriptor based on start and end
// keys and the supplied proto.Replicas slice. It allocates new Raft
// and range IDs to fill out the supplied replicas.
func (s *Store) NewRangeDescriptor(start, end proto.Key, replicas []proto.Replica) (*proto.RangeDescriptor, error) {
	desc := &proto.RangeDescriptor{
		RaftID:   s.raftIDAlloc.Allocate(),
		StartKey: start,
		EndKey:   end,
		Replicas: append([]proto.Replica(nil), replicas...),
	}
	return desc, nil
}

// SplitRange shortens the original range to accommodate the new
// range. The new range is added to the ranges map and the rangesByKey
// sorted slice.
func (s *Store) SplitRange(origRng, newRng *Range) error {
	if !bytes.Equal(origRng.Desc().EndKey, newRng.Desc().EndKey) ||
		bytes.Compare(origRng.Desc().StartKey, newRng.Desc().StartKey) >= 0 {
		return util.Errorf("orig range is not splittable by new range: %+v, %+v", origRng.Desc(), newRng.Desc())
	}
	// Replace the end key of the original range with the start key of
	// the new range.
	copy := *origRng.Desc()
	copy.EndKey = append([]byte(nil), newRng.Desc().StartKey...)
	origRng.SetDesc(&copy)
	s.mu.Lock()
	err := s.addRangeInternal(newRng, true)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	if err := s.multiraft.CreateGroup(uint64(newRng.Desc().RaftID)); err != nil {
		return err
	}
	return nil
}

// MergeRange expands the subsuming range to absorb the subsumed range.
// This merge operation will fail if the two ranges are not collocated
// on the same store.
func (s *Store) MergeRange(subsumingRng *Range, updatedEndKey proto.Key, subsumedRaftID int64) error {
	if !subsumingRng.Desc().EndKey.Less(updatedEndKey) {
		return util.Errorf("the new end key is not greater than the current one: %+v < %+v",
			updatedEndKey, subsumingRng.Desc().EndKey)
	}

	subsumedRng, err := s.GetRange(subsumedRaftID)
	if err != nil {
		return util.Errorf("Could not find the subsumed range: %d", subsumedRaftID)
	}

	if !ReplicaSetsEqual(subsumedRng.Desc().GetReplicas(), subsumingRng.Desc().GetReplicas()) {
		return util.Errorf("Ranges are not on the same replicas sets: %+v=%+v",
			subsumedRng.Desc().GetReplicas(), subsumingRng.Desc().GetReplicas())
	}

	// Remove and destroy the subsumed range.
	if err = s.RemoveRange(subsumedRng); err != nil {
		return util.Errorf("cannot remove range %s", err)
	}

	// TODO(bram): The removed range needs to have all of its metadata removed.

	// Update the end key of the subsuming range.
	copy := *subsumingRng.Desc()
	copy.EndKey = updatedEndKey
	subsumingRng.SetDesc(&copy)

	return nil
}

// AddRange adds the range to the store's range map and to the sorted
// rangesByKey slice.
func (s *Store) AddRange(rng *Range) error {
	s.mu.Lock()
	err := s.addRangeInternal(rng, true)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	if err := s.multiraft.CreateGroup(uint64(rng.Desc().RaftID)); err != nil {
		return err
	}
	return nil
}

// addRangeInternal starts the range and adds it to the ranges map and
// the rangesByKey slice. If resort is true, the rangesByKey slice is
// sorted; this is optional to allow many ranges to be added and the
// sort only invoked once. This method presupposes the store's lock
// is held. Returns a rangeAlreadyExists error if a range with the
// same Raft ID has already been added to this store.
func (s *Store) addRangeInternal(rng *Range, resort bool) error {
	rng.start(s.stopper)
	// TODO(spencer); will need to determine which range is
	// newer, and keep that one.
	if exRng, ok := s.ranges[rng.Desc().RaftID]; ok {
		return &rangeAlreadyExists{exRng}
	}
	s.ranges[rng.Desc().RaftID] = rng
	s.rangesByKey = append(s.rangesByKey, rng)
	if resort {
		sort.Sort(s.rangesByKey)
	}
	return nil
}

// RemoveRange removes the range from the store's range map and from
// the sorted rangesByKey slice.
func (s *Store) RemoveRange(rng *Range) error {
	// RemoveGroup needs to access the storage, which in turn needs the
	// lock. Some care is needed to avoid deadlocks.
	if err := s.multiraft.RemoveGroup(uint64(rng.Desc().RaftID)); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.ranges, rng.Desc().RaftID)
	// Find the range in rangesByKey slice and swap it to end of slice
	// and truncate.
	n := sort.Search(len(s.rangesByKey), func(i int) bool {
		return bytes.Compare(rng.Desc().StartKey, s.rangesByKey[i].Desc().EndKey) < 0
	})
	if n >= len(s.rangesByKey) {
		return util.Errorf("couldn't find range in rangesByKey slice")
	}
	s.rangesByKey = append(s.rangesByKey[:n], s.rangesByKey[n+1:]...)
	return nil
}

// NewSnapshot creates a new snapshot engine.
func (s *Store) NewSnapshot() engine.Engine {
	return s.engine.NewSnapshot()
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
	if header.Timestamp.Equal(proto.ZeroTimestamp) {
		// Update the incoming timestamp if unset.
		header.Timestamp = s.clock.Now()
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

	// Backoff and retry loop for handling errors.
	retryOpts := s.RetryOpts
	retryOpts.Tag = fmt.Sprintf("store: %s", method)
	err := util.RetryWithBackoff(retryOpts, func() (util.RetryStatus, error) {
		// Add the command to the range for execution; exit retry loop on success.
		reply.Reset()

		// Get range and add command to the range for execution.
		rng, err := s.GetRange(header.RaftID)
		if err != nil {
			reply.Header().SetGoError(err)
			return util.RetryBreak, err
		}

		if err = rng.AddCmd(method, args, reply, true); err == nil {
			return util.RetryBreak, nil
		}

		// Maybe resolve a potential write intent error. We do this here
		// because this is the code path with the requesting client
		// waiting. We don't want every replica to attempt to resolve the
		// intent independently, so we can't do it in Range.executeCmd.
		err = s.maybeResolveWriteIntentError(rng, method, args, reply)

		switch t := err.(type) {
		case *proto.WriteTooOldError:
			// Update request timestamp and retry immediately.
			header.Timestamp = t.ExistingTimestamp
			header.Timestamp.Logical++
			return util.RetryReset, nil
		case *proto.WriteIntentError:
			// If write intent error is resolved, exit retry/backoff loop to
			// immediately retry.
			if t.Resolved {
				return util.RetryReset, nil
			}
			// Otherwise, update timestamp on read/write and backoff / retry.
			if proto.IsReadWrite(method) && header.Timestamp.Less(t.Txn.Timestamp) {
				header.Timestamp = t.Txn.Timestamp
				header.Timestamp.Logical++
			}
			return util.RetryContinue, nil
		}
		return util.RetryBreak, nil
	})

	// By default, retries are indefinite. However, some unittests set a
	// maximum retry count; return txn retry error for transactional cases
	// and the original error otherwise.
	if _, ok := err.(*util.RetryMaxAttemptsError); ok && header.Txn != nil {
		reply.Header().SetGoError(proto.NewTransactionRetryError(header.Txn))
	}
	return reply.Header().GoError()
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
			Key:          wiErr.Txn.Key,
			User:         args.Header().User,
			UserPriority: args.Header().UserPriority,
			Txn:          args.Header().Txn,
		},
		PusheeTxn: wiErr.Txn,
		Abort:     proto.IsReadWrite(method), // abort if cmd is read/write
	}
	pushReply := &proto.InternalPushTxnResponse{}
	s.db.Call(proto.InternalPushTxn, pushArgs, pushReply)
	if pushErr := pushReply.GoError(); pushErr != nil {
		log.V(1).Infof("push %q failed: %s", pushArgs.Header().Key, pushErr)

		// For write/write conflicts within a transaction, propagate the
		// push failure, not the original write intent error. The push
		// failure will instruct the client to restart the transaction
		// with a backoff.
		if args.Header().Txn != nil && proto.IsReadWrite(method) {
			reply.Header().SetGoError(pushErr)
			return pushErr
		}
		// For read/write conflicts, return the write intent error which
		// engages backoff/retry (with !Resolved). We don't need to
		// restart the txn, only resend the read with a backoff.
		return err
	}
	wiErr.Resolved = true // success!

	// We pushed the transaction successfully, so resolve the intent.
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
	if resolveErr := rng.AddCmd(proto.InternalResolveIntent, resolveArgs, resolveReply, false); resolveErr != nil {
		log.Warningf("resolve of key %q failed: %s", wiErr.Key, resolveErr)
	}

	return wiErr
}

// ProposeRaftCommand submits a command to raft. The command is processed
// asynchronously and an error or nil will be written to the returned
// channel when it is committed or aborted (but note that committed does
// mean that it has been applied to the range yet).
func (s *Store) ProposeRaftCommand(idKey cmdIDKey, cmd proto.InternalRaftCommand) <-chan error {
	value := cmd.Cmd.GetValue()
	if value == nil {
		panic("proposed a nil command")
	}
	// Lazily create group. TODO(bdarnell): make this non-lazy
	err := s.multiraft.CreateGroup(uint64(cmd.RaftID))
	if err != nil {
		ch := make(chan error, 1)
		ch <- err
		return ch
	}

	data, err := gogoproto.Marshal(&cmd)
	if err != nil {
		log.Fatal(err)
	}
	etr, ok := value.(*proto.EndTransactionRequest)
	if ok && etr.InternalCommitTrigger != nil &&
		etr.InternalCommitTrigger.ChangeReplicasTrigger != nil {
		// EndTransactionRequest with a ChangeReplicasTrigger is special because raft
		// needs to understand it; it cannot simply be an opaque command.
		crt := etr.InternalCommitTrigger.ChangeReplicasTrigger
		return s.multiraft.ChangeGroupMembership(uint64(cmd.RaftID), string(idKey),
			changeTypeInternalToRaft[crt.ChangeType],
			MakeRaftNodeID(crt.NodeID, crt.StoreID),
			data)
	}
	return s.multiraft.SubmitCommand(uint64(cmd.RaftID), string(idKey), data)
}

// processRaft processes read/write commands that have been committed
// by the raft consensus algorithm, dispatching them to the
// appropriate range. This method starts a goroutine to process Raft
// commands indefinitely or until the stopper signals.
//
// TODO(bdarnell): when Raft elects this node as the leader for any
//   of its ranges, we need to be careful to do the following before
//   the range is allowed to believe it's the leader and begin to accept
//   writes and reads:
//     - Apply all committed log entries to the state machine.
//     - Signal the range to clear its read timestamp, response caches
//       and pending read queue.
//     - Signal the range that it's now the leader with the duration
//       of its leader lease.
//   If we don't do this, then a read which was previously gated on
//   the former leader waiting for overlapping writes to commit to
//   the underlying state machine, might transit to the new leader
//   and be able to access the new leader's state machine BEFORE
//   the overlapping writes are applied.
func (s *Store) processRaft() {
	s.stopper.RunWorker(func() {
		for {
			select {
			case e := <-s.multiraft.Events:
				var cmd proto.InternalRaftCommand
				var groupID int64
				var commandID string
				var index uint64
				var callback func(error)

				switch e := e.(type) {
				case *multiraft.EventCommandCommitted:
					groupID = int64(e.GroupID)
					commandID = e.CommandID
					index = e.Index
					err := gogoproto.Unmarshal(e.Command, &cmd)
					if err != nil {
						log.Fatal(err)
					}

				case *multiraft.EventMembershipChangeCommitted:
					groupID = int64(e.GroupID)
					commandID = e.CommandID
					index = e.Index
					callback = e.Callback
					err := gogoproto.Unmarshal(e.Payload, &cmd)
					if err != nil {
						log.Fatal(err)
					}

				default:
					continue
				}

				if groupID != cmd.RaftID {
					log.Fatalf("e.GroupID (%d) should == cmd.RaftID (%d)", groupID, cmd.RaftID)
				}

				s.mu.Lock()
				r, ok := s.ranges[groupID]
				s.mu.Unlock()
				var err error
				if !ok {
					err = util.Errorf("got committed raft command for %d but have no range with that ID: %+v",
						groupID, cmd)
					log.Error(err)
				} else {
					err = r.processRaftCommand(cmdIDKey(commandID), index, cmd)
				}
				if callback != nil {
					callback(err)
				}

			case <-s.stopper.ShouldStop():
				return
			}
		}
	})
}

// GroupStorage implements the multiraft.Storage interface.
func (s *Store) GroupStorage(groupID uint64) multiraft.WriteableGroupStorage {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.ranges[int64(groupID)]
	if !ok {
		var err error
		r, err = NewRange(&proto.RangeDescriptor{
			RaftID: int64(groupID),
			// TODO(bdarnell): other fields are unknown; need to populate them from
			// snapshot.
		}, s)
		if err != nil {
			panic(err) // TODO(bdarnell)
		}
		err = s.addRangeInternal(r, true)
		if err != nil {
			panic(err) // TODO(bdarnell)
		}
	}
	return r
}

// AppliedIndex implements the multiraft.StateMachine interface.
func (s *Store) AppliedIndex(groupID uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.ranges[int64(groupID)]
	if !ok {
		return 0, util.Errorf("range %d not found", groupID)
	}
	return r.appliedIndex, nil
}

func raftEntryFormatter(data []byte) string {
	if len(data) == 0 {
		return "[empty]"
	}
	var cmd proto.InternalRaftCommand
	if err := gogoproto.Unmarshal(data, &cmd); err != nil {
		return fmt.Sprintf("[error parsing entry: %s]", err)
	}
	s := cmd.String()
	maxLen := 300
	if len(s) > maxLen {
		s = s[:maxLen]
	}
	return s
}
