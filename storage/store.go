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
	"golang.org/x/net/context"
)

const (
	// UserRoot is the username for the root user.
	UserRoot = "root"
	// GCResponseCacheExpiration is the expiration duration for response
	// cache entries.
	GCResponseCacheExpiration = 1 * time.Hour
	// raftIDAllocCount is the number of Raft IDs to allocate per allocation.
	raftIDAllocCount                = 10
	defaultRaftTickInterval         = 10 * time.Millisecond
	defaultHeartbeatIntervalTicks   = 3
	defaultRaftElectionTimeoutTicks = 15
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

	// TestStoreContext has some fields initialized with values relevant
	// in tests.
	TestStoreContext = StoreContext{
		RaftTickInterval:           100 * time.Millisecond,
		RaftHeartbeatIntervalTicks: 1,
		RaftElectionTimeoutTicks:   2,
		ScanInterval:               10 * time.Minute,
	}
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
		_, key = encoding.DecodeBytes(key, nil)
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

// A Store maintains a map of ranges by start key. A Store corresponds
// to one physical device.
type Store struct {
	*StoreFinder

	Ident          proto.StoreIdent
	ctx            StoreContext
	kvDB           *client.DB
	engine         engine.Engine   // The underlying key-value store
	_allocator     *allocator      // Makes allocation decisions
	raftIDAlloc    *IDAllocator    // Raft ID allocator
	gcQueue        *gcQueue        // Garbage collection queue
	_splitQueue    *splitQueue     // Range splitting queue
	verifyQueue    *verifyQueue    // Checksum verification queue
	replicateQueue *replicateQueue // Replication queue
	rangeGCQueue   *rangeGCQueue   // Range GC queue
	scanner        *rangeScanner   // Range scanner
	feed           StoreEventFeed  // Event Feed
	multiraft      *multiraft.MultiRaft
	started        int32
	stopper        *util.Stopper
	startedAt      int64
	nodeDesc       *proto.NodeDescriptor
	initComplete   sync.WaitGroup // Signaled by async init tasks

	mu          sync.RWMutex     // Protects variables below...
	ranges      map[int64]*Range // Map of ranges by Raft ID
	rangesByKey RangeSlice       // Sorted slice of ranges by StartKey
}

var _ multiraft.Storage = &Store{}

// A StoreContext encompasses the auxiliary objects and configuration
// required to create a store.
// All fields holding a pointer or an interface are required to create
// a store; the rest will have sane defaults set if omitted.
type StoreContext struct {
	Clock     *hlc.Clock
	DB        *client.KV
	Gossip    *gossip.Gossip
	Transport multiraft.Transport

	// RangeRetryOptions are the retry options for ranges.
	// TODO(tschottdorf) improve comment once I figure out what this is.
	RangeRetryOptions util.RetryOptions

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

	// ScanInterval is the default value for the scan interval
	ScanInterval time.Duration

	// EventFeed is a feed to which this store will publish events.
	EventFeed *util.Feed
}

// Valid returns true if the StoreContext is populated correctly.
// We don't check for Gossip and DB since some of our tests pass
// that as nil.
func (sc *StoreContext) Valid() bool {
	return sc.Clock != nil && sc.Transport != nil &&
		sc.RaftTickInterval != 0 && sc.RaftHeartbeatIntervalTicks > 0 &&
		sc.RaftElectionTimeoutTicks > 0 && sc.ScanInterval > 0
}

// setDefaults initializes unset fields in StoreConfig to values
// suitable for use on a local network.
// TODO(tschottdorf) see if this ought to be configurable via flags.
func (sc *StoreContext) setDefaults() {
	if sc.RaftTickInterval == 0 {
		sc.RaftTickInterval = defaultRaftTickInterval
	}
	if sc.RaftHeartbeatIntervalTicks == 0 {
		sc.RaftHeartbeatIntervalTicks = defaultHeartbeatIntervalTicks
	}
	if sc.RaftElectionTimeoutTicks == 0 {
		sc.RaftElectionTimeoutTicks = defaultRaftElectionTimeoutTicks
	}
}

// NewStore returns a new instance of a store.
func NewStore(ctx StoreContext, eng engine.Engine, nodeDesc *proto.NodeDescriptor) *Store {
	// TODO(tschottdorf) find better place to set these defaults.
	ctx.setDefaults()

	if !ctx.Valid() {
		panic(fmt.Sprintf("invalid store configuration: %+v", &ctx))
	}

	sf := newStoreFinder(ctx.Gossip)
	s := &Store{
		ctx:         ctx,
		StoreFinder: sf,
		engine:      eng,
		_allocator:  newAllocator(sf.findStores),
		ranges:      map[int64]*Range{},
		nodeDesc:    nodeDesc,
	}

	// Add range scanner and configure with queues.
	s.scanner = newRangeScanner(ctx.ScanInterval, newStoreRangeIterator(s), s.updateStoreStatus)
	s.gcQueue = newGCQueue()
	s._splitQueue = newSplitQueue(s.ctx.DB, s.ctx.Gossip)
	s.verifyQueue = newVerifyQueue(s.scanner.Stats)
	s.replicateQueue = newReplicateQueue(s.ctx.Gossip, s.allocator(), s.ctx.Clock)
	s.rangeGCQueue = newRangeGCQueue(s.ctx.DB)
	s.scanner.AddQueues(s.gcQueue, s.splitQueue(), s.verifyQueue, s.replicateQueue, s.rangeGCQueue)

	return s
}

// String formats a store for debug output.
func (s *Store) String() string {
	return fmt.Sprintf("store=%d:%d (%s)", s.Ident.NodeID, s.Ident.StoreID, s.engine)
}

// Context returns a base context to pass along with commands being executed,
// derived from the supplied context (which is allowed to be nil).
func (s *Store) Context(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return log.Add(ctx,
		log.NodeID, s.Ident.NodeID,
		log.StoreID, s.Ident.StoreID)
}

// IsStarted returns true if the Store has been started.
func (s *Store) IsStarted() bool {
	return atomic.LoadInt32(&s.started) == 1
}

// Start the engine, set the GC and read the StoreIdent.
func (s *Store) Start(stopper *util.Stopper) error {
	s.stopper = stopper
	s.kvDB = s.ctx.DB.NewDB()

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

	// If the nodeID is 0, it has not be assigned yet.
	// TODO(bram): Figure out how to remove this special case.
	if s.nodeDesc.NodeID != 0 && s.Ident.NodeID != s.nodeDesc.NodeID {
		return util.Errorf("node id:%d does not equal the one in node descriptor:%d", s.Ident.NodeID, s.nodeDesc.NodeID)
	}

	// Start store event feed.
	s.feed = NewStoreEventFeed(s.Ident.StoreID, s.ctx.EventFeed)
	s.feed.startStore()

	// Create ID allocators.
	idAlloc, err := NewIDAllocator(engine.KeyRaftIDGenerator, s.ctx.DB, 2 /* min ID */, raftIDAllocCount, s.stopper)
	if err != nil {
		return err
	}
	s.raftIDAlloc = idAlloc

	now := s.ctx.Clock.Now()
	s.startedAt = now.WallTime

	// GCTimeouts method is called each time an engine compaction is
	// underway. It sets minimum timeouts for transaction records and
	// response cache entries.
	minTxnTS := int64(0) // disable GC of transactions until we know minimum write intent age
	minRCacheTS := now.WallTime - GCResponseCacheExpiration.Nanoseconds()
	s.engine.SetGCTimeouts(minTxnTS, minRCacheTS)

	// Iterator over all range-local key-based data.
	start := engine.RangeDescriptorKey(engine.KeyMin)
	end := engine.RangeDescriptorKey(engine.KeyMax)

	if s.multiraft, err = multiraft.NewMultiRaft(s.RaftNodeID(), &multiraft.Config{
		Transport:              s.ctx.Transport,
		Storage:                s,
		StateMachine:           s,
		TickInterval:           s.ctx.RaftTickInterval,
		ElectionTimeoutTicks:   s.ctx.RaftElectionTimeoutTicks,
		HeartbeatIntervalTicks: s.ctx.RaftHeartbeatIntervalTicks,
		EntryFormatter:         raftEntryFormatter,
	}, s.stopper); err != nil {
		return err
	}

	// Iterate over all range descriptors, ignoring uncommitted versions
	// (consistent=false). Uncommitted intents which have been abandoned
	// due to a split crashing halfway will simply be resolved on the
	// next split attempt. They can otherwise be ignored.
	s.feed.beginScanRanges()
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
		s.feed.addRange(rng)
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
	s.feed.endScanRanges()

	// Sort the rangesByKey slice after they've all been added.
	sort.Sort(s.rangesByKey)

	// Start Raft processing goroutines.
	if err = s.multiraft.Start(); err != nil {
		return err
	}
	s.processRaft()

	// Gossip is only ever nil while bootstrapping a cluster and
	// in unittests.
	if s.ctx.Gossip != nil {
		// Register callbacks for any changes to accounting and zone
		// configurations; we split ranges along prefix boundaries to
		// avoid having a range that has two different accounting/zone
		// configs. (We don't need a callback for permissions since
		// permissions don't have such a requirement.)
		s.ctx.Gossip.RegisterCallback(gossip.KeyConfigAccounting, s.configGossipUpdate)
		s.ctx.Gossip.RegisterCallback(gossip.KeyConfigZone, s.configGossipUpdate)
		// Callback triggers on capacity gossip from all stores.
		capacityRegex := gossip.MakePrefixPattern(gossip.KeyMaxAvailCapacityPrefix)
		s.ctx.Gossip.RegisterCallback(capacityRegex, s.capacityGossipUpdate)

		// Start a single goroutine in charge of periodically gossipping the
		// sentinel and first range metadata if we have a first range.
		// This may wake up ranges and requires everything to be set up and
		// running.
		if err := s.startGossip(); err != nil {
			return err
		}

		// Start the scanner. The construction here makes sure that the scanner
		// only starts after Gossip has connected, and that it does not block Start
		// from returning (as doing so might prevent Gossip from ever connecting).
		s.stopper.RunWorker(func() {
			select {
			case <-s.ctx.Gossip.Connected:
				s.scanner.Start(s.ctx.Clock, s.stopper)
			case <-s.stopper.ShouldStop():
				return
			}
		})

	}

	// Set the started flag (for unittests).
	atomic.StoreInt32(&s.started, 1)

	return nil
}

// WaitForInit waits for any asynchronous processes begun in Start()
// to complete their initialization. In particular, this includes
// gossiping. In some cases this may block until the range GC queue
// has completed its scan. Only for testing.
func (s *Store) WaitForInit() {
	s.initComplete.Wait()
}

// startGossip runs an infinite loop in a goroutine which regularly checks
// whether the store has a first range or config replica and asks those ranges
// to gossip accordingly.
func (s *Store) startGossip() error {
	// Periodic updates run in a goroutine and signal a WaitGroup upon completion
	// of their first iteration.
	s.initComplete.Add(1)
	s.stopper.RunWorker(func() {
		// Run the first time without waiting for the Ticker and signal the WaitGroup.
		if err := s.maybeGossipFirstRange(); err != nil {
			log.Warningf("error gossiping first range data: %s", err)
		}
		s.initComplete.Done()
		ticker := time.NewTicker(clusterIDGossipInterval)
		for {
			select {
			case <-ticker.C:
				if err := s.maybeGossipFirstRange(); err != nil {
					log.Warningf("error gossiping first range data: %s", err)
				}
			case <-s.stopper.ShouldStop():
				return
			}
		}
	})

	s.initComplete.Add(1)
	s.stopper.RunWorker(func() {
		if err := s.maybeGossipConfigs(); err != nil {
			log.Warningf("error gossiping configs: %s", err)
		}
		s.initComplete.Done()
		ticker := time.NewTicker(configGossipInterval)
		for {
			select {
			case <-ticker.C:
				if err := s.maybeGossipConfigs(); err != nil {
					log.Warningf("error gossiping configs: %s", err)
				}
			case <-s.stopper.ShouldStop():
				return
			}
		}
	})
	return nil
}

// maybeGossipFirstRange checks whether the store has a replia of the first
// range and if so, reminds it to gossip the first range descriptor and
// sentinel gossip.
func (s *Store) maybeGossipFirstRange() error {
	rng := s.LookupRange(engine.KeyMin, nil)
	if rng != nil {
		return rng.maybeGossipFirstRange()
	}
	return nil
}

// maybeGossipConfigs checks which of the store's ranges contain config
// descriptors and lets these ranges gossip them. Config gossip entries do not
// expire, so this is a rarely needed action in a working cluster - if values
// change, ranges will update gossip autonomously. However, the lease holder,
// who is normally in charge of that might crash after updates before gossiping
// and a new leader lease is only acquired if needed. To account for this rare
// scenario, we activate the very few ranges that hold config maps
// periodically.
func (s *Store) maybeGossipConfigs() error {
	for _, cd := range configDescriptors {
		rng := s.LookupRange(cd.keyPrefix, nil)
		if rng == nil {
			// This store has no range with this configuration.
			continue
		}
		// Wake up the replica. If it acquires a fresh lease, it will
		// gossip. If an unexpected error occurs (i.e. nobody else seems to
		// have an active lease but we still failed to obtain it), return
		// that error. If we ignored it we would run the risk of running a
		// cluster without configs gossiped.
		if _, err := rng.getLeaseForGossip(s.Context(nil)); err != nil {
			return err
		}
		// Always gossip the configs as there are leader lease acquisition
		// scenarios in which the config is not gossiped (e.g. leader executes
		// but crashes before gossiping; comes back up but group goes dormant).
		rng.maybeGossipConfigs(func(_ proto.Key) bool {
			return rng.ContainsKey(cd.keyPrefix)
		})
	}
	return nil
}

// configGossipUpdate is a callback for gossip updates to
// configuration maps which affect range split boundaries.
func (s *Store) configGossipUpdate(key string, contentsChanged bool) {
	if !contentsChanged {
		return // Skip update if it's just a newer timestamp or fewer hops to info
	}
	info, err := s.ctx.Gossip.GetInfo(key)
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
func (s *Store) GossipCapacity() {
	storeDesc, err := s.Descriptor()
	if err != nil {
		log.Warningf("problem getting store descriptor for store %+v: %v", s.Ident, err)
		return
	}
	// Unique gossip key per store.
	keyMaxCapacity := gossip.MakeMaxAvailCapacityKey(storeDesc.Node.NodeID, storeDesc.StoreID)
	// Gossip store descriptor.
	err = s.ctx.Gossip.AddInfo(keyMaxCapacity, *storeDesc, ttlCapacityGossip)
	if err != nil {
		log.Warning(err)
	}
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
		s.splitQueue().MaybeAdd(s.rangesByKey[n], s.ctx.Clock.Now())
	}
}

// ForceReplicationScan iterates over all ranges and enqueues any that
// need to be replicated. Exposed only for testing.
func (s *Store) ForceReplicationScan(t util.Tester) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, r := range s.ranges {
		s.replicateQueue.MaybeAdd(r, s.ctx.Clock.Now())
	}
}

// ForceRangeGCScan iterates over all ranges and enqueues any that
// may need to be GC'd. Exposed only for testing.
func (s *Store) ForceRangeGCScan(t util.Tester) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, r := range s.ranges {
		s.rangeGCQueue.MaybeAdd(r, s.ctx.Clock.Now())
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
		return util.Errorf("store %s: unable to access: %s", s.engine, err)
	} else if len(kvs) > 0 {
		// See if this is an already-bootstrapped store.
		ok, err := engine.MVCCGetProto(s.engine, engine.StoreIdentKey(), proto.ZeroTimestamp, true, nil, &s.Ident)
		if err != nil {
			return util.Errorf("store %s is non-empty but cluster ID could not be determined: %s", s.engine, err)
		}
		if ok {
			return util.Errorf("store %s already belongs to cockroach cluster %s", s.engine, s.Ident.ClusterID)
		}
		return util.Errorf("store %s is not-empty and has invalid contents (first key: %q)", s.engine, kvs[0].Key)
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
	return nil, proto.NewRangeNotFoundError(raftID)
}

// LookupRange looks up a range via binary search over the sorted
// "rangesByKey" RangeSlice. Returns nil if no range is found for
// specified key range. Note that the specified keys are transformed
// using Key.Address() to ensure we lookup ranges correctly for local
// keys. When end is nill, a range that contains start is looked up.
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
	ms := &proto.MVCCStats{}
	now := s.ctx.Clock.Now()

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

	if err := engine.MVCCSetRangeStats(batch, 1, ms); err != nil {
		return err
	}
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
func (s *Store) RaftNodeID() proto.RaftNodeID {
	return proto.MakeRaftNodeID(s.Ident.NodeID, s.Ident.StoreID)
}

// Clock accessor.
func (s *Store) Clock() *hlc.Clock { return s.ctx.Clock }

// Engine accessor.
func (s *Store) Engine() engine.Engine { return s.engine }

// DB accessor.
func (s *Store) DB() *client.KV { return s.ctx.DB }

// Allocator accessor.
func (s *Store) allocator() *allocator { return s._allocator }

// Gossip accessor.
func (s *Store) Gossip() *gossip.Gossip { return s.ctx.Gossip }

// SplitQueue accessor.
func (s *Store) splitQueue() *splitQueue { return s._splitQueue }

// Stopper accessor.
func (s *Store) Stopper() *util.Stopper { return s.stopper }

// EventFeed accessor.
func (s *Store) EventFeed() StoreEventFeed { return s.feed }

// NewRangeDescriptor creates a new descriptor based on start and end
// keys and the supplied proto.Replicas slice. It allocates new Raft
// and range IDs to fill out the supplied replicas.
func (s *Store) NewRangeDescriptor(start, end proto.Key, replicas []proto.Replica) (*proto.RangeDescriptor, error) {
	id, err := s.raftIDAlloc.Allocate()
	if err != nil {
		return nil, err
	}
	desc := &proto.RangeDescriptor{
		RaftID:   id,
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
	defer s.mu.Unlock()
	err := s.addRangeInternal(newRng, true)
	if err != nil {
		return err
	}
	s.feed.splitRange(origRng, newRng)
	return nil
}

// MergeRange expands the subsuming range to absorb the subsumed range.
// This merge operation will fail if the two ranges are not collocated
// on the same store.
func (s *Store) MergeRange(subsumingRng *Range, updatedEndKey proto.Key, subsumedRaftID int64) error {
	if !subsumingRng.Desc().EndKey.Less(updatedEndKey) {
		return util.Errorf("the new end key is not greater than the current one: %+v <= %+v",
			updatedEndKey, subsumingRng.Desc().EndKey)
	}

	subsumedRng, err := s.GetRange(subsumedRaftID)
	if err != nil {
		return util.Errorf("could not find the subsumed range: %d", subsumedRaftID)
	}

	if !replicaSetsEqual(subsumedRng.Desc().GetReplicas(), subsumingRng.Desc().GetReplicas()) {
		return util.Errorf("ranges are not on the same replicas sets: %+v != %+v",
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

	s.feed.mergeRange(subsumingRng, subsumedRng)
	return nil
}

// AddRange adds the range to the store's range map and to the sorted
// rangesByKey slice.
func (s *Store) AddRange(rng *Range) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.addRangeInternal(rng, true)
	if err != nil {
		return err
	}
	s.feed.addRange(rng)
	return nil
}

// addRangeInternal adds the range to the ranges map and the rangesByKey slice.
// If resort is true, the rangesByKey slice is sorted; this is optional to
// allow many ranges to be added and the sort only invoked once. This method
// presupposes the store's lock is held. Returns a rangeAlreadyExists error if
// a range with the same Raft ID has already been added to this store.
func (s *Store) addRangeInternal(rng *Range, resort bool) error {
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
	if err := <-s.multiraft.RemoveGroup(uint64(rng.Desc().RaftID)); err != nil {
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
	if n >= len(s.rangesByKey) || rng.Desc().RaftID != s.rangesByKey[n].Desc().RaftID {
		return util.Errorf("couldn't find range in rangesByKey slice")
	}
	s.rangesByKey = append(s.rangesByKey[:n], s.rangesByKey[n+1:]...)

	s.scanner.RemoveRange(rng)

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
func (s *Store) Capacity() (proto.StoreCapacity, error) {
	return s.engine.Capacity()
}

// Descriptor returns a StoreDescriptor including current store
// capacity information.
func (s *Store) Descriptor() (*proto.StoreDescriptor, error) {
	capacity, err := s.Capacity()
	if err != nil {
		return nil, err
	}
	// Initialize the store descriptor.
	return &proto.StoreDescriptor{
		StoreID:  s.Ident.StoreID,
		Attrs:    s.Attrs(),
		Node:     *s.nodeDesc,
		Capacity: capacity,
	}, nil
}

// ExecuteCmd fetches a range based on the header's replica, assembles
// method, args & reply into a Raft Cmd struct and executes the
// command using the fetched range.
func (s *Store) ExecuteCmd(ctx context.Context, call client.Call) error {
	args, reply := call.Args, call.Reply
	ctx = log.Add(s.Context(ctx), log.Method, args.Method())
	// If the request has a zero timestamp, initialize to this node's clock.
	header := args.Header()
	if err := verifyKeys(header.Key, header.EndKey); err != nil {
		reply.Header().SetGoError(err)
		return err
	}
	if !header.Timestamp.Equal(proto.ZeroTimestamp) {
		// Update our clock with the incoming request timestamp. This
		// advances the local node's clock to a high water mark from
		// amongst all nodes with which it has interacted. The update is
		// bounded by the max clock drift.
		_, err := s.ctx.Clock.Update(header.Timestamp)
		if err != nil {
			reply.Header().SetGoError(err)
			return err
		}
	}

	// Backoff and retry loop for handling errors.
	retryOpts := s.ctx.RangeRetryOptions
	retryOpts.Tag = fmt.Sprintf("store: %s", args.Method())
	err := util.RetryWithBackoff(retryOpts, func() (util.RetryStatus, error) {
		// Add the command to the range for execution; exit retry loop on success.
		reply.Reset()

		// Get range and add command to the range for execution.
		rng, err := s.GetRange(header.RaftID)
		if err != nil {
			reply.Header().SetGoError(err)
			return util.RetryBreak, err
		}
		ctx = log.Add(ctx, log.RaftID, header.RaftID)

		if err = rng.AddCmd(ctx, client.Call{Args: args, Reply: reply}, true); err == nil {
			return util.RetryBreak, nil
		}

		// Maybe resolve a potential write intent error. We do this here
		// because this is the code path with the requesting client
		// waiting. We don't want every replica to attempt to resolve the
		// intent independently, so we can't do it in Range.executeCmd.
		if wiErr, ok := err.(*proto.WriteIntentError); ok {
			// If inconsistent, return results and resolve asynchronously.
			if header.ReadConsistency == proto.INCONSISTENT && s.stopper.StartTask() {
				go func() {
					if err := s.resolveWriteIntentError(ctx, wiErr, rng, args, proto.CLEANUP_TXN, true); err != nil {
						log.Warningc(ctx, "failed to resolve on inconsistent read asynchronously", log.Err, err)
					}
					s.stopper.FinishTask()
				}()
				reply.Header().Error = nil
				return util.RetryBreak, nil
			}
			pushType := proto.PUSH_TIMESTAMP
			if proto.IsWrite(args) {
				pushType = proto.ABORT_TXN
			}
			err = s.resolveWriteIntentError(ctx, wiErr, rng, args, pushType, false)
			reply.Header().SetGoError(err)
		}

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
			for _, intent := range t.Intents {
				if proto.IsWrite(args) && header.Timestamp.Less(intent.Txn.Timestamp) {
					header.Timestamp = intent.Txn.Timestamp.Next()
				}
			}
			return util.RetryContinue, nil
		}
		return util.RetryBreak, err
	})

	// By default, retries are indefinite. However, some unittests set a
	// maximum retry count; return txn retry error for transactional cases
	// and the original error otherwise.
	if _, ok := err.(*util.RetryMaxAttemptsError); ok && header.Txn != nil {
		reply.Header().SetGoError(proto.NewTransactionRetryError(header.Txn))
	}

	return reply.Header().GoError()
}

// resolveWriteIntentError tries to push the conflicting transaction:
// either move its timestamp forward on a read/write conflict, or
// abort it on a write/write conflict. If the push succeeds, we
// immediately issue a resolve intent command and set the error's
// Resolved flag to true so the client retries the command
// immediately. If the push fails, we set the error's Resolved flag to
// false so that the client backs off before reissuing the command.
func (s *Store) resolveWriteIntentError(ctx context.Context, wiErr *proto.WriteIntentError, rng *Range, args proto.Request,
	pushType proto.PushTxnType, wait bool) error {
	// TODO set key upstream.
	ctx = log.Add(ctx, log.Err, wiErr, log.Key, args.Header().Key)
	if log.V(1) {
		log.Infoc(ctx, "resolving write intent")
	}

	// Attempt to push the transaction(s) which created the conflicting intent(s).
	bArgs := &proto.InternalBatchRequest{}
	bReply := &proto.InternalBatchResponse{}
	for _, intent := range wiErr.Intents {
		pushArgs := &proto.InternalPushTxnRequest{
			RequestHeader: proto.RequestHeader{
				Timestamp:    args.Header().Timestamp,
				Key:          intent.Txn.Key,
				User:         args.Header().User,
				UserPriority: args.Header().UserPriority,
				Txn:          args.Header().Txn,
			},
			PusheeTxn:   intent.Txn,
			PushType:    pushType,
			RangeLookup: args.Method() == proto.InternalRangeLookup,
		}
		bArgs.Add(pushArgs)
	}
	b := &client.Batch{}
	b.InternalAddCall(client.Call{Args: bArgs, Reply: bReply})

	// Run all pushes in parallel.
	if pushErr := s.kvDB.Run(b); pushErr != nil {
		if log.V(1) {
			log.Infoc(ctx, "pushes failed", log.Err, pushErr, log.Detail, wiErr.Intents)
		}

		// For write/write conflicts within a transaction, propagate the
		// push failure, not the original write intent error. The push
		// failure will instruct the client to restart the transaction
		// with a backoff.
		if args.Header().Txn != nil && proto.IsWrite(args) {
			return pushErr
		}
		// For read/write conflicts, return the write intent error which
		// engages backoff/retry (with !Resolved). We don't need to
		// restart the txn, only resend the read with a backoff.
		return wiErr
	}
	wiErr.Resolved = true // success!

	// We pushed the transaction(s) successfully, so resolve the intent(s).
	for i, intent := range wiErr.Intents {
		pushReply := bReply.Responses[i].GetValue().(*proto.InternalPushTxnResponse)
		resolveArgs := &proto.InternalResolveIntentRequest{
			RequestHeader: proto.RequestHeader{
				// Use the pushee's timestamp, which might be lower than the
				// pusher's request timestamp. No need to push the intent higher
				// than the pushee's txn!
				Timestamp: pushReply.PusheeTxn.Timestamp,
				Key:       intent.Key,
				User:      UserRoot,
				Txn:       pushReply.PusheeTxn,
			},
		}
		resolveReply := &proto.InternalResolveIntentResponse{}
		// Add resolve command with wait=false to add to Raft but not wait for completion.
		waitForResolve := wait && i == len(wiErr.Intents)-1
		if resolveErr := rng.AddCmd(ctx, client.Call{Args: resolveArgs, Reply: resolveReply}, waitForResolve); resolveErr != nil {
			return resolveErr
		}
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
	err := <-s.multiraft.CreateGroup(uint64(cmd.RaftID))
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
			proto.MakeRaftNodeID(crt.NodeID, crt.StoreID),
			data)
	}
	return s.multiraft.SubmitCommand(uint64(cmd.RaftID), string(idKey), data)
}

// processRaft processes read/write commands that have been committed
// by the raft consensus algorithm, dispatching them to the
// appropriate range. This method starts a goroutine to process Raft
// commands indefinitely or until the stopper signals.
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
					if log.V(6) {
						log.Infof("store %d: new committed %s command at index %d",
							cmd.Cmd.GetValue().(proto.Request).Method(), e.Index)
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

				s.mu.RLock()
				r, ok := s.ranges[groupID]
				s.mu.RUnlock()
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.ranges[int64(groupID)]
	if !ok {
		return 0, util.Errorf("range %d not found", groupID)
	}
	return atomic.LoadUint64(&r.appliedIndex), nil
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

// GetStatus fetches the latest store status from the stored value on the cluster.
// Returns nil if the scanner has not yet run.  The scanner runs once every
// ctx.ScanInterval.
func (s *Store) GetStatus() (*proto.StoreStatus, error) {
	if s.scanner.Count() == 0 {
		// The scanner hasn't completed a first run yet.
		return nil, nil
	}
	key := engine.StoreStatusKey(int32(s.Ident.StoreID))
	r, err := s.kvDB.Get(key)
	if err != nil {
		return nil, err
	}
	status := &proto.StoreStatus{}
	if err := r.Rows[0].ValueProto(status); err != nil {
		return nil, err
	}
	return status, nil
}

// WaitForRangeScanCompletion waits until the next range scan is complete and
// returns the total number of scans completed so far.  This is exposed for use
// in unit tests.
func (s *Store) WaitForRangeScanCompletion() int64 {
	return s.scanner.WaitForScanCompletion()
}

// updateStoreStatus updates the store's status proto.
func (s *Store) updateStoreStatus() {
	now := s.ctx.Clock.Now().WallTime
	scannerStats := s.scanner.Stats()

	desc, err := s.Descriptor()
	if err != nil {
		log.Error(err)
		return
	}
	status := &proto.StoreStatus{
		Desc:       *desc,
		NodeID:     s.Ident.NodeID,
		UpdatedAt:  now,
		StartedAt:  s.startedAt,
		RangeCount: int32(scannerStats.RangeCount),
		Stats:      proto.MVCCStats(scannerStats.MVCC),
	}
	key := engine.StoreStatusKey(int32(s.Ident.StoreID))
	if _, err := s.kvDB.Put(key, status); err != nil {
		log.Error(err)
	}
}

// SetRangeRetryOptions sets the retry options used for this store.
func (s *Store) SetRangeRetryOptions(ro util.RetryOptions) {
	s.ctx.RangeRetryOptions = ro
}
