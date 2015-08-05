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
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/tracer"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	gogoproto "github.com/gogo/protobuf/proto"
	"github.com/google/btree"
	"golang.org/x/net/context"
)

const (
	// GCResponseCacheExpiration is the expiration duration for response
	// cache entries.
	GCResponseCacheExpiration = 1 * time.Hour
	// raftIDAllocCount is the number of Raft IDs to allocate per allocation.
	raftIDAllocCount                = 10
	defaultRaftTickInterval         = 100 * time.Millisecond
	defaultHeartbeatIntervalTicks   = 3
	defaultRaftElectionTimeoutTicks = 15
	// ttlCapacityGossip is time-to-live for capacity-related info.
	ttlCapacityGossip = 2 * time.Minute
)

var (
	// defaultRangeRetryOptions are default retry options for retrying commands
	// sent to the store's ranges, for WriteTooOld and WriteIntent errors.
	defaultRangeRetryOptions = retry.Options{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		Multiplier:     2,
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
	maxLength := proto.KeyMaxLength
	if bytes.HasPrefix(key, keys.LocalRangePrefix) {
		key = key[len(keys.LocalRangePrefix):]
		_, key = encoding.DecodeBytes(key, nil)
	}
	if bytes.HasPrefix(key, keys.MetaPrefix) {
		key = key[len(keys.Meta1Prefix):]
	}
	if len(key) > maxLength {
		return util.Errorf("maximum key length exceeded for %q", key)
	}
	return nil
}

// verifyKeys verifies keys. If checkEndKey is true, then the end key
// is verified to be non-nil and greater than start key. If
// checkEndKey is false, end key is verified to be nil. Additionally,
// verifies that start key is less than KeyMax and end key is less
// than or equal to KeyMax.
func verifyKeys(start, end proto.Key, checkEndKey bool) error {
	if err := verifyKeyLength(start); err != nil {
		return err
	}
	if !start.Less(proto.KeyMax) {
		return util.Errorf("start key %q must be less than KeyMax", start)
	}
	if !checkEndKey {
		if len(end) != 0 {
			return util.Errorf("end key %q should not be specified for this operation", end)
		}
		return nil
	}
	if end == nil {
		return util.Errorf("end key must be specified")
	}
	if err := verifyKeyLength(end); err != nil {
		return err
	}
	if proto.KeyMax.Less(end) {
		return util.Errorf("end key %q must be less than or equal to KeyMax", end)
	}
	if !start.Less(end) {
		return util.Errorf("end key %q must be greater than start %q", end, start)
	}
	return nil
}

type rangeAlreadyExists struct {
	rng *Replica
}

// Error implements the error interface.
func (e *rangeAlreadyExists) Error() string {
	return fmt.Sprintf("range for Raft ID %d already exists on store", e.rng.Desc().RangeID)
}

// rangeKeyItem is a common interface for proto.Key and Range.
type rangeKeyItem interface {
	getKey() proto.Key
}

// rangeBTreeKey is a type alias of proto.Key that implements the
// rangeKeyItem interface and the btree.Item interface.
type rangeBTreeKey proto.Key

var _ rangeKeyItem = rangeBTreeKey{}

func (k rangeBTreeKey) getKey() proto.Key {
	return (proto.Key)(k)
}

var _ btree.Item = rangeBTreeKey{}

func (k rangeBTreeKey) Less(i btree.Item) bool {
	return k.getKey().Less(i.(rangeKeyItem).getKey())
}

var _ rangeKeyItem = &Replica{}

func (r *Replica) getKey() proto.Key {
	return r.Desc().EndKey
}

var _ btree.Item = &Replica{}

// Less returns true if the range's end key is less than the given item's key.
func (r *Replica) Less(i btree.Item) bool {
	return r.getKey().Less(i.(rangeKeyItem).getKey())
}

// A NotBootstrappedError indicates that an engine has not yet been
// bootstrapped due to a store identifier not being present.
type NotBootstrappedError struct{}

// Error formats error.
func (e *NotBootstrappedError) Error() string {
	return "store has not been bootstrapped"
}

// storeRangeSet is an implementation of rangeSet which
// cycles through a store's rangesByKey btree.
type storeRangeSet struct {
	store   *Store
	raftIDs []proto.RangeID // Raft IDs of ranges to be visited.
	visited int             // Number of visited ranges. -1 when Visit() is not being called.
}

func newStoreRangeSet(store *Store) *storeRangeSet {
	return &storeRangeSet{
		store:   store,
		visited: 0,
	}
}

func (rs *storeRangeSet) Visit(visitor func(*Replica) bool) {
	// Copy the raft IDs to a slice and iterate over the slice so
	// that we can safely (e.g., no race, no range skip) iterate
	// over ranges regardless of how BTree is implemented.
	rs.store.mu.RLock()
	rs.raftIDs = make([]proto.RangeID, rs.store.rangesByKey.Len())
	i := 0
	rs.store.rangesByKey.Ascend(func(item btree.Item) bool {
		rs.raftIDs[i] = item.(*Replica).Desc().RangeID
		i++
		return true
	})
	rs.store.mu.RUnlock()

	rs.visited = 0
	for _, raftID := range rs.raftIDs {
		rs.visited++
		rs.store.mu.RLock()
		rng, ok := rs.store.ranges[raftID]
		rs.store.mu.RUnlock()
		if ok {
			if !visitor(rng) {
				break
			}
		}
	}
	rs.visited = 0
}

func (rs *storeRangeSet) EstimatedCount() int {
	rs.store.mu.RLock()
	defer rs.store.mu.RUnlock()
	if rs.visited <= 0 {
		return rs.store.rangesByKey.Len()
	}
	return len(rs.raftIDs) - rs.visited
}

// A Store maintains a map of ranges by start key. A Store corresponds
// to one physical device.
type Store struct {
	Ident          proto.StoreIdent
	ctx            StoreContext
	db             *client.DB
	engine         engine.Engine   // The underlying key-value store
	_allocator     *allocator      // Makes allocation decisions
	raftIDAlloc    *idAllocator    // Raft ID allocator
	gcQueue        *gcQueue        // Garbage collection queue
	_splitQueue    *splitQueue     // Range splitting queue
	verifyQueue    *verifyQueue    // Checksum verification queue
	replicateQueue *replicateQueue // Replication queue
	_rangeGCQueue  *rangeGCQueue   // Range GC queue
	scanner        *rangeScanner   // Range scanner
	feed           StoreEventFeed  // Event Feed
	multiraft      *multiraft.MultiRaft
	started        int32
	stopper        *stop.Stopper
	startedAt      int64
	nodeDesc       *proto.NodeDescriptor
	initComplete   sync.WaitGroup // Signaled by async init tasks

	mu           sync.RWMutex               // Protects variables below...
	ranges       map[proto.RangeID]*Replica // Map of ranges by Raft ID
	rangesByKey  *btree.BTree               // btree keyed by ranges end keys.
	uninitRanges map[proto.RangeID]*Replica // Map of uninitialized ranges by Raft ID
}

var _ multiraft.Storage = &Store{}

// A StoreContext encompasses the auxiliary objects and configuration
// required to create a store.
// All fields holding a pointer or an interface are required to create
// a store; the rest will have sane defaults set if omitted.
type StoreContext struct {
	Clock     *hlc.Clock
	DB        *client.DB
	Gossip    *gossip.Gossip
	Transport multiraft.Transport

	// RangeRetryOptions are the retry options when retryable errors are
	// encountered sending commands to ranges.
	RangeRetryOptions retry.Options

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

	// ScanMaxIdleTime is the maximum time the scanner will be idle between ranges.
	// If enabled (> 0), the scanner may complete in less than ScanInterval for small
	// stores.
	ScanMaxIdleTime time.Duration

	// EventFeed is a feed to which this store will publish events.
	EventFeed *util.Feed

	// Tracer is a request tracer.
	Tracer *tracer.Tracer
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
	sc.RangeRetryOptions = defaultRangeRetryOptions

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

	s := &Store{
		ctx:          ctx,
		db:           ctx.DB, // TODO(tschottdorf) remove redundancy.
		engine:       eng,
		_allocator:   newAllocator(ctx.Gossip),
		ranges:       map[proto.RangeID]*Replica{},
		rangesByKey:  btree.New(64 /* degree */),
		uninitRanges: map[proto.RangeID]*Replica{},
		nodeDesc:     nodeDesc,
	}

	// Add range scanner and configure with queues.
	s.scanner = newRangeScanner(ctx.ScanInterval, ctx.ScanMaxIdleTime, newStoreRangeSet(s))
	s.gcQueue = newGCQueue()
	s._splitQueue = newSplitQueue(s.db, s.ctx.Gossip)
	s.verifyQueue = newVerifyQueue(s.RangeCount)
	s.replicateQueue = newReplicateQueue(s.ctx.Gossip, s.allocator(), s.ctx.Clock)
	s._rangeGCQueue = newRangeGCQueue(s.db)
	s.scanner.AddQueues(s.gcQueue, s._splitQueue, s.verifyQueue, s.replicateQueue, s._rangeGCQueue)

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

// StartedAt returns the timestamp at which the store was most recently started.
func (s *Store) StartedAt() int64 {
	return s.startedAt
}

// Start the engine, set the GC and read the StoreIdent.
func (s *Store) Start(stopper *stop.Stopper) error {
	s.stopper = stopper

	if s.Ident.NodeID == 0 {
		// Open engine (i.e. initialize RocksDB database). "NodeID != 0"
		// implies the engine has already been opened.
		if err := s.engine.Open(); err != nil {
			return err
		}
		s.stopper.AddCloser(s.engine)

		// Read store ident and return a not-bootstrapped error if necessary.
		ok, err := engine.MVCCGetProto(s.engine, keys.StoreIdentKey(), proto.ZeroTimestamp, true,
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

	// Create ID allocators.
	idAlloc, err := newIDAllocator(keys.RaftIDGenerator, s.db, 2 /* min ID */, raftIDAllocCount, s.stopper)
	if err != nil {
		return err
	}
	s.raftIDAlloc = idAlloc

	now := s.ctx.Clock.Now()
	s.startedAt = now.WallTime

	// Start store event feed.
	s.feed = NewStoreEventFeed(s.Ident.StoreID, s.ctx.EventFeed)
	s.feed.startStore(s.startedAt)

	// GCTimeouts method is called each time an engine compaction is
	// underway. It sets minimum timeouts for transaction records and
	// response cache entries.
	minTxnTS := int64(0) // disable GC of transactions until we know minimum write intent age
	minRCacheTS := now.WallTime - GCResponseCacheExpiration.Nanoseconds()
	s.engine.SetGCTimeouts(minTxnTS, minRCacheTS)

	// Iterator over all range-local key-based data.
	start := keys.RangeDescriptorKey(proto.KeyMin)
	end := keys.RangeDescriptorKey(proto.KeyMax)

	if s.multiraft, err = multiraft.NewMultiRaft(s.RaftNodeID(), &multiraft.Config{
		Transport:              s.ctx.Transport,
		Storage:                s,
		StateMachine:           s,
		TickInterval:           s.ctx.RaftTickInterval,
		ElectionTimeoutTicks:   s.ctx.RaftElectionTimeoutTicks,
		HeartbeatIntervalTicks: s.ctx.RaftHeartbeatIntervalTicks,
		EntryFormatter:         raftEntryFormatter,
		// TODO(bdarnell): Multiraft deadlocks if the Events channel is
		// unbuffered. Temporarily give it some breathing room until the underlying
		// deadlock is fixed. See #1185, #1193.
		EventBufferSize: 1000,
	}, s.stopper); err != nil {
		return err
	}

	// Iterate over all range descriptors, ignoring uncommitted versions
	// (consistent=false). Uncommitted intents which have been abandoned
	// due to a split crashing halfway will simply be resolved on the
	// next split attempt. They can otherwise be ignored.
	s.mu.Lock()
	s.feed.beginScanRanges()
	if _, err := engine.MVCCIterate(s.engine, start, end, now, false /* !consistent */, nil, func(kv proto.KeyValue) (bool, error) {
		// Only consider range metadata entries; ignore others.
		_, suffix, _ := keys.DecodeRangeKey(kv.Key)
		if !suffix.Equal(keys.LocalRangeDescriptorSuffix) {
			return false, nil
		}
		var desc proto.RangeDescriptor
		if err := gogoproto.Unmarshal(kv.Value.Bytes, &desc); err != nil {
			return false, err
		}
		rng, err := NewReplica(&desc, s)
		if err != nil {
			return false, err
		}
		if err = s.addRangeInternal(rng); err != nil {
			return false, err
		}
		s.feed.registerRange(rng, true /* scan */)
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

	s.mu.Unlock()

	// Start Raft processing goroutines.
	s.multiraft.Start()
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

		// Start a single goroutine in charge of periodically gossipping the
		// sentinel and first range metadata if we have a first range.
		// This may wake up ranges and requires everything to be set up and
		// running.
		s.startGossip()

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
func (s *Store) startGossip() {
	ctx := s.Context(nil)
	// Periodic updates run in a goroutine and signal a WaitGroup upon completion
	// of their first iteration.
	s.initComplete.Add(2)
	s.stopper.RunWorker(func() {
		// Run the first time without waiting for the Ticker and signal the WaitGroup.
		if err := s.maybeGossipFirstRange(); err != nil {
			log.Warningc(ctx, "error gossiping first range data: %s", err)
		}
		s.initComplete.Done()
		ticker := time.NewTicker(clusterIDGossipInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := s.maybeGossipFirstRange(); err != nil {
					log.Warningc(ctx, "error gossiping first range data: %s", err)
				}
			case <-s.stopper.ShouldStop():
				return
			}
		}
	})

	s.stopper.RunWorker(func() {
		if err := s.maybeGossipConfigs(); err != nil {
			log.Warningc(ctx, "error gossiping configs: %s", err)
		}
		s.initComplete.Done()
		ticker := time.NewTicker(configGossipInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := s.maybeGossipConfigs(); err != nil {
					log.Warningc(ctx, "error gossiping configs: %s", err)
				}
			case <-s.stopper.ShouldStop():
				return
			}
		}
	})
}

// maybeGossipFirstRange checks whether the store has a replia of the first
// range and if so, reminds it to gossip the first range descriptor and
// sentinel gossip.
func (s *Store) maybeGossipFirstRange() error {
	rng := s.LookupRange(proto.KeyMin, nil)
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
	}
	return nil
}

// configGossipUpdate is a callback for gossip updates to
// configuration maps which affect range split boundaries.
func (s *Store) configGossipUpdate(key string, contentsChanged bool) {
	if !contentsChanged {
		return // Skip update if it's just a newer timestamp or fewer hops to info
	}
	ctx := s.Context(nil)
	info, err := s.ctx.Gossip.GetInfo(key)
	if err != nil {
		log.Errorc(ctx, "unable to fetch %s config from gossip: %s", key, err)
		return
	}
	configMap, ok := info.(PrefixConfigMap)
	if !ok {
		log.Errorc(ctx, "gossiped info is not a prefix configuration map: %+v", info)
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
	ctx := s.Context(nil)
	if err != nil {
		log.Warningc(ctx, "problem getting store descriptor for store %+v: %v", s.Ident, err)
		return
	}
	// Unique gossip key per store.
	keyMaxCapacity := gossip.MakeCapacityKey(storeDesc.Node.NodeID, storeDesc.StoreID)
	// Gossip store descriptor.
	err = s.ctx.Gossip.AddInfo(keyMaxCapacity, *storeDesc, ttlCapacityGossip)
	if err != nil {
		log.Warningc(ctx, "%s", err)
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
		var rng *Replica
		s.rangesByKey.AscendGreaterOrEqual((rangeBTreeKey)(config.Prefix.Next()), func(i btree.Item) bool {
			rng = i.(*Replica)
			return false
		})
		// If the config doesn't split the range, continue.
		if rng == nil || !rng.Desc().ContainsKey(config.Prefix) {
			continue
		}
		s.splitQueue().MaybeAdd(rng, s.ctx.Clock.Now())
	}
}

// DisableRangeGCQueue disables or enables the range GC queue.
// Exposed only for testing.
func (s *Store) DisableRangeGCQueue(disabled bool) {
	s.rangeGCQueue().SetDisabled(disabled)
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
		s._rangeGCQueue.MaybeAdd(r, s.ctx.Clock.Now())
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
	s.rangesByKey.Ascend(func(i btree.Item) bool {
		rng := i.(*Replica)
		if idx < len(zoneMap)-1 && !rng.Desc().StartKey.Less(zoneMap[idx+1].Prefix) {
			idx++
			zone = zoneMap[idx].Config.(*proto.ZoneConfig)
		}
		rng.SetMaxBytes(zone.RangeMaxBytes)
		return true
	})
}

// Bootstrap writes a new store ident to the underlying engine. To
// ensure that no crufty data already exists in the engine, it scans
// the engine contents before writing the new store ident. The engine
// should be completely empty. It returns an error if called on a
// non-empty engine.
func (s *Store) Bootstrap(ident proto.StoreIdent, stopper *stop.Stopper) error {
	if s.Ident.NodeID != 0 {
		return util.Errorf("engine already bootstrapped")
	}
	if err := s.engine.Open(); err != nil {
		return err
	}
	stopper.AddCloser(s.engine)
	s.Ident = ident
	kvs, err := engine.Scan(s.engine, proto.EncodedKey(proto.KeyMin), proto.EncodedKey(proto.KeyMax), 1)
	if err != nil {
		return util.Errorf("store %s: unable to access: %s", s.engine, err)
	} else if len(kvs) > 0 {
		// See if this is an already-bootstrapped store.
		ok, err := engine.MVCCGetProto(s.engine, keys.StoreIdentKey(), proto.ZeroTimestamp, true, nil, &s.Ident)
		if err != nil {
			return util.Errorf("store %s is non-empty but cluster ID could not be determined: %s", s.engine, err)
		}
		if ok {
			return util.Errorf("store %s already belongs to cockroach cluster %s", s.engine, s.Ident.ClusterID)
		}
		return util.Errorf("store %s is not-empty and has invalid contents (first key: %q)", s.engine, kvs[0].Key)
	}
	err = engine.MVCCPutProto(s.engine, nil, keys.StoreIdentKey(), proto.ZeroTimestamp, nil, &s.Ident)
	return err
}

// GetRange fetches a range by Raft ID. Returns an error if no range is found.
func (s *Store) GetRange(raftID proto.RangeID) (*Replica, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if rng, ok := s.ranges[raftID]; ok {
		return rng, nil
	}
	return nil, proto.NewRangeNotFoundError(raftID)
}

// LookupRange looks up a range via binary search over the
// "rangesByKey" btree. Returns nil if no range is found for
// specified key range. Note that the specified keys are transformed
// using Key.Address() to ensure we lookup ranges correctly for local
// keys. When end is nil, a range that contains start is looked up.
func (s *Store) LookupRange(start, end proto.Key) *Replica {
	s.mu.RLock()
	defer s.mu.RUnlock()
	startAddr := keys.KeyAddress(start)
	endAddr := keys.KeyAddress(end)

	var rng *Replica
	s.rangesByKey.AscendGreaterOrEqual((rangeBTreeKey)(startAddr.Next()), func(i btree.Item) bool {
		rng = i.(*Replica)
		return false
	})
	if rng == nil || !rng.Desc().ContainsKeyRange(startAddr, endAddr) {
		return nil
	}
	return rng
}

// RaftStatus returns the current raft status of the given range.
func (s *Store) RaftStatus(raftID proto.RangeID) *raft.Status {
	return s.multiraft.Status(raftID)
}

// BootstrapRange creates the first range in the cluster and manually
// writes it to the store. Default range addressing records are
// created for meta1 and meta2. Default configurations for accounting,
// permissions, users, and zones are created. All configs are specified
// for the empty key prefix, meaning they apply to the entire
// database. Permissions are granted to all users and the zone
// requires three replicas with no other specifications. It also adds
// the range tree and the root node, the first range, to it.
func (s *Store) BootstrapRange() error {
	desc := &proto.RangeDescriptor{
		RangeID:  1,
		StartKey: proto.KeyMin,
		EndKey:   proto.KeyMax,
		Replicas: []proto.Replica{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	batch := s.engine.NewBatch()
	ms := &engine.MVCCStats{}
	now := s.ctx.Clock.Now()

	// Range descriptor.
	if err := engine.MVCCPutProto(batch, ms, keys.RangeDescriptorKey(desc.StartKey), now, nil, desc); err != nil {
		return err
	}
	// GC Metadata.
	gcMeta := proto.NewGCMetadata(now.WallTime)
	if err := engine.MVCCPutProto(batch, ms, keys.RangeGCMetadataKey(desc.RangeID), proto.ZeroTimestamp, nil, gcMeta); err != nil {
		return err
	}
	// Verification timestamp.
	if err := engine.MVCCPutProto(batch, ms, keys.RangeLastVerificationTimestampKey(desc.RangeID), proto.ZeroTimestamp, nil, &now); err != nil {
		return err
	}
	// Range addressing for meta2.
	meta2Key := keys.RangeMetaKey(proto.KeyMax)
	if err := engine.MVCCPutProto(batch, ms, meta2Key, now, nil, desc); err != nil {
		return err
	}
	// Range addressing for meta1.
	meta1Key := keys.RangeMetaKey(meta2Key)
	if err := engine.MVCCPutProto(batch, ms, meta1Key, now, nil, desc); err != nil {
		return err
	}
	// Accounting config.
	acctConfig := &proto.AcctConfig{}
	key := keys.MakeKey(keys.ConfigAccountingPrefix, proto.KeyMin)
	if err := engine.MVCCPutProto(batch, ms, key, now, nil, acctConfig); err != nil {
		return err
	}
	// Permission config.
	permConfig := &proto.PermConfig{
		Read:  []string{security.RootUser}, // root user
		Write: []string{security.RootUser}, // root user
	}
	key = keys.MakeKey(keys.ConfigPermissionPrefix, proto.KeyMin)
	if err := engine.MVCCPutProto(batch, ms, key, now, nil, permConfig); err != nil {
		return err
	}
	// User config.
	// TODO(marc): instead of a root entry, maybe we should have a default "node".
	userConfig := &proto.UserConfig{}
	key = keys.MakeKey(keys.ConfigUserPrefix, proto.KeyMin)
	if err := engine.MVCCPutProto(batch, ms, key, now, nil, userConfig); err != nil {
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
	key = keys.MakeKey(keys.ConfigZonePrefix, proto.KeyMin)
	if err := engine.MVCCPutProto(batch, ms, key, now, nil, zoneConfig); err != nil {
		return err
	}

	// We reserve the first 1000 descriptor IDs.
	key = keys.DescIDGenerator
	value := proto.Value{}
	value.SetInteger(structured.MaxReservedDescID + 1)
	value.InitChecksum(key)
	if err := engine.MVCCPut(batch, nil, key, now, value, nil); err != nil {
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
func (s *Store) DB() *client.DB { return s.ctx.DB }

// Allocator accessor.
func (s *Store) allocator() *allocator { return s._allocator }

// Gossip accessor.
func (s *Store) Gossip() *gossip.Gossip { return s.ctx.Gossip }

// splitQueue accessor.
func (s *Store) splitQueue() *splitQueue { return s._splitQueue }

// rangeGCQueue accessor.
func (s *Store) rangeGCQueue() *rangeGCQueue { return s._rangeGCQueue }

// Stopper accessor.
func (s *Store) Stopper() *stop.Stopper { return s.stopper }

// EventFeed accessor.
func (s *Store) EventFeed() StoreEventFeed { return s.feed }

// Tracer accessor.
func (s *Store) Tracer() *tracer.Tracer { return s.ctx.Tracer }

// NewRangeDescriptor creates a new descriptor based on start and end
// keys and the supplied proto.Replicas slice. It allocates new Raft
// and range IDs to fill out the supplied replicas.
func (s *Store) NewRangeDescriptor(start, end proto.Key, replicas []proto.Replica) (*proto.RangeDescriptor, error) {
	id, err := s.raftIDAlloc.Allocate()
	if err != nil {
		return nil, err
	}
	desc := &proto.RangeDescriptor{
		RangeID:  proto.RangeID(id),
		StartKey: start,
		EndKey:   end,
		Replicas: append([]proto.Replica(nil), replicas...),
	}
	return desc, nil
}

// SplitRange shortens the original range to accommodate the new
// range. The new range is added to the ranges map and the rangesByKey
// btree.
func (s *Store) SplitRange(origRng, newRng *Replica) error {
	if !bytes.Equal(origRng.Desc().EndKey, newRng.Desc().EndKey) ||
		bytes.Compare(origRng.Desc().StartKey, newRng.Desc().StartKey) >= 0 {
		return util.Errorf("orig range is not splittable by new range: %+v, %+v", origRng.Desc(), newRng.Desc())
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// Replace the end key of the original range with the start key of
	// the new range. Reinsert the range since the btree is keyed by range end keys.
	if s.rangesByKey.Delete(origRng) == nil {
		return util.Errorf("couldn't find range %s in rangesByKey btree", origRng)
	}

	copyDesc := *origRng.Desc()
	copyDesc.EndKey = append([]byte(nil), newRng.Desc().StartKey...)
	origRng.setDescWithoutProcessUpdate(&copyDesc)

	if s.rangesByKey.ReplaceOrInsert(origRng) != nil {
		return util.Errorf("couldn't insert range %v in rangesByKey btree", origRng)
	}
	if err := s.addRangeInternal(newRng); err != nil {
		return util.Errorf("couldn't insert range %v in rangesByKey btree: %s", newRng, err)
	}

	// Update the max bytes and other information of the new range.
	if err := newRng.updateRangeInfo(); err != nil {
		return err
	}

	s.feed.splitRange(origRng, newRng)
	return s.processRangeDescriptorUpdateLocked(origRng)
}

// MergeRange expands the subsuming range to absorb the subsumed range.
// This merge operation will fail if the two ranges are not collocated
// on the same store.
func (s *Store) MergeRange(subsumingRng *Replica, updatedEndKey proto.Key, subsumedRaftID proto.RangeID) error {
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
	if err := subsumingRng.setDesc(&copy); err != nil {
		return err
	}

	s.feed.mergeRange(subsumingRng, subsumedRng)
	return nil
}

// AddRangeTest adds the range to the store's range map and to the sorted
// rangesByKey slice. To be used only by unittests.
func (s *Store) AddRangeTest(rng *Replica) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.addRangeInternal(rng); err != nil {
		return err
	}
	s.feed.registerRange(rng, false /* scan */)
	return nil
}

// addRangeInternal adds the range to the ranges map and the rangesByKey btree.
// This method presupposes the store's lock is held. Returns a rangeAlreadyExists
// error if a range with the same Raft ID has already been added to this store.
func (s *Store) addRangeInternal(rng *Replica) error {
	if !rng.isInitialized() {
		return util.Errorf("attempted to add uninitialized range %s", rng)
	}

	// TODO(spencer); will need to determine which range is
	// newer, and keep that one.
	if err := s.addRangeToRangeMap(rng); err != nil {
		return err
	}

	if s.rangesByKey.Has(rng) {
		return &rangeAlreadyExists{rng}
	}
	if exRngItem := s.rangesByKey.ReplaceOrInsert(rng); exRngItem != nil {
		return util.Errorf("range for key %v already exists in rangesByKey btree",
			(exRngItem.(*Replica)).getKey())
	}
	return nil
}

// addRangeToRangeMap adds the range to the ranges map.
func (s *Store) addRangeToRangeMap(rng *Replica) error {
	if exRng, ok := s.ranges[rng.Desc().RangeID]; ok {
		return &rangeAlreadyExists{exRng}
	}
	s.ranges[rng.Desc().RangeID] = rng
	return nil
}

// RemoveRange removes the range from the store's range map and from
// the sorted rangesByKey btree.
func (s *Store) RemoveRange(rng *Replica) error {
	// RemoveGroup needs to access the storage, which in turn needs the
	// lock. Some care is needed to avoid deadlocks.
	if err := s.multiraft.RemoveGroup(rng.Desc().RangeID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.ranges, rng.Desc().RangeID)
	if s.rangesByKey.Delete(rng) == nil {
		return util.Errorf("couldn't find range in rangesByKey btree")
	}
	s.scanner.RemoveRange(rng)
	return nil
}

// processRangeDescriptorUpdate is called whenever a range's
// descriptor is updated.
func (s *Store) processRangeDescriptorUpdate(rng *Replica) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.processRangeDescriptorUpdateLocked(rng)
}

func (s *Store) processRangeDescriptorUpdateLocked(rng *Replica) error {
	if !rng.isInitialized() {
		return util.Errorf("attempted to process uninitialized range %s", rng)
	}

	if _, ok := s.uninitRanges[rng.Desc().RangeID]; !ok {
		// Do nothing if the range has already been initialized.
		return nil
	}
	delete(s.uninitRanges, rng.Desc().RangeID)
	s.feed.registerRange(rng, false /* scan */)

	if s.rangesByKey.Has(rng) {
		return &rangeAlreadyExists{rng}
	}
	if exRngItem := s.rangesByKey.ReplaceOrInsert(rng); exRngItem != nil {
		return util.Errorf("range for key %v already exists in rangesByKey btree",
			(exRngItem.(*Replica)).getKey())
	}
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
	capacity.RangeCount = int32(s.RangeCount())
	// Initialize the store descriptor.
	return &proto.StoreDescriptor{
		StoreID:  s.Ident.StoreID,
		Attrs:    s.Attrs(),
		Node:     *s.nodeDesc,
		Capacity: capacity,
	}, nil
}

// RangeCount returns the number of ranges contained by this store.
func (s *Store) RangeCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.ranges)
}

// ExecuteCmd fetches a range based on the header's replica, assembles
// method, args & reply into a Raft Cmd struct and executes the
// command using the fetched range.
func (s *Store) ExecuteCmd(ctx context.Context, args proto.Request) (proto.Response, error) {
	ctx = s.Context(ctx)
	trace := tracer.FromCtx(ctx)
	// If the request has a zero timestamp, initialize to this node's clock.
	header := args.Header()
	if err := verifyKeys(header.Key, header.EndKey, proto.IsRange(args)); err != nil {
		return nil, err
	}
	if !header.Timestamp.Equal(proto.ZeroTimestamp) {
		if s.Clock().MaxOffset() > 0 {
			// Once a command is submitted to raft, all replicas' logical
			// clocks will be ratcheted forward to match. If the command
			// appears to come from a node with a bad clock, reject it now
			// before we reach that point.
			offset := time.Duration(header.Timestamp.WallTime - s.Clock().PhysicalNow())
			if offset > s.Clock().MaxOffset() {
				return nil, util.Errorf("Rejecting command with timestamp in the future: %d (%s ahead)",
					header.Timestamp.WallTime, offset)
			}
		}
		// Update our clock with the incoming request timestamp. This
		// advances the local node's clock to a high water mark from
		// amongst all nodes with which it has interacted.
		s.ctx.Clock.Update(header.Timestamp)
	}

	defer trace.Epoch("executing " + args.Method().String())()
	// Backoff and retry loop for handling errors. Backoff times are measured
	// in the Trace.
	next := func(r *retry.Retry) bool {
		if r.CurrentAttempt() > 0 {
			defer trace.Epoch("backoff")()
		}
		return r.Next()
	}
	var rng *Replica
	var err error
	// Add the command to the range for execution; exit retry loop on success.
	for r := retry.Start(s.ctx.RangeRetryOptions); next(&r); {
		// Get range and add command to the range for execution.
		rng, err = s.GetRange(header.RangeID)
		if err != nil {
			return nil, err
		}

		var reply proto.Response
		reply, err = rng.AddCmd(ctx, args)
		if err == nil {
			return reply, nil
		} else if err == multiraft.ErrGroupDeleted {
			// This error needs to be converted appropriately so that
			// clients will retry.
			err = proto.NewRangeNotFoundError(rng.Desc().RangeID)
		}

		// Maybe resolve a potential write intent error. We do this here
		// because this is the code path with the requesting client
		// waiting. We don't want every replica to attempt to resolve the
		// intent independently, so we can't do it in Range.executeCmd.
		if wiErr, ok := err.(*proto.WriteIntentError); ok {
			var pushType proto.PushTxnType
			if proto.IsWrite(args) {
				pushType = proto.ABORT_TXN
			} else {
				pushType = proto.PUSH_TIMESTAMP
			}

			err = s.resolveWriteIntentError(ctx, wiErr, rng, args, pushType)
		}

		switch t := err.(type) {
		case *proto.WriteTooOldError:
			trace.Event(fmt.Sprintf("error: %T", err))
			// Update request timestamp and retry immediately.
			header.Timestamp = t.ExistingTimestamp.Next()
			r.Reset()
			if log.V(1) {
				log.Warning(err)
			}
			continue
		case *proto.WriteIntentError:
			trace.Event(fmt.Sprintf("error: %T", err))
			// If write intent error is resolved, exit retry/backoff loop to
			// immediately retry.
			if t.Resolved {
				r.Reset()
				if log.V(1) {
					log.Warning(err)
				}
				continue
			}

			// Otherwise, update timestamp on read/write and backoff / retry.
			for _, intent := range t.Intents {
				if proto.IsWrite(args) && header.Timestamp.Less(intent.Txn.Timestamp) {
					header.Timestamp = intent.Txn.Timestamp.Next()
				}
			}
			if log.V(1) {
				log.Warning(err)
			}
			continue
		}
		return reply, err
	}

	// By default, retries are indefinite. However, some unittests set a
	// maximum retry count; return txn retry error for transactional cases
	// and the original error otherwise.
	if header.Txn != nil {
		return nil, proto.NewTransactionRetryError(header.Txn)
	}
	return nil, err
}

// resolveWriteIntentError tries to push the conflicting transaction:
// either move its timestamp forward on a read/write conflict, or
// abort it on a write/write conflict. If the push succeeds, we
// immediately issue a resolve intent command and set the error's
// Resolved flag to true so the client retries the command
// immediately. If the push fails, we set the error's Resolved flag to
// false so that the client backs off before reissuing the command.
func (s *Store) resolveWriteIntentError(ctx context.Context, wiErr *proto.WriteIntentError, rng *Replica, args proto.Request, pushType proto.PushTxnType) error {
	if log.V(6) {
		log.Infoc(ctx, "resolving write intent %s", wiErr)
	}
	trace := tracer.FromCtx(ctx)
	defer trace.Epoch("intent resolution")()

	// Attempt to push the transaction(s) which created the conflicting intent(s).
	now := s.Clock().Now()
	header := args.Header()
	bArgs := &proto.BatchRequest{
		RequestHeader: proto.RequestHeader{
			Txn:          header.Txn,
			User:         header.User,
			UserPriority: header.UserPriority,
		},
	}
	bReply := &proto.BatchResponse{}
	for _, intent := range wiErr.Intents {
		pushArgs := &proto.PushTxnRequest{
			RequestHeader: proto.RequestHeader{
				Timestamp: header.Timestamp,
				Key:       intent.Txn.Key,
				// TODO(tschottdorf):
				// The following fields should not be supplied here, but store
				// tests (for example TestStoreResolveWriteIntent) which do not
				// go through TxnCoordSender rely on them being specified on
				// the individual calls (and TxnCoordSender is in charge of
				// filling them in here later).
				Txn: header.Txn,
				// This is here only for legacy reasons: testSender in the
				// storage tests duplicates batch processing and isn't as
				// smart as TxnCoordSender. A test that relies on this is
				// TestStoreResolveWriteIntentNoTxn.
				// This should disappear naturally when batches which address
				// the same Range get sent to that Range wholesale:
				// testSender then simply sends batches as any other call,
				// which should be enough for the few tests that need them.
				UserPriority: header.UserPriority,
			},
			PusheeTxn: intent.Txn,
			// The timestamp is used by PushTxn for figuring out whether the
			// transaction is abandoned. If we used the argument's timestamp
			// here, we would run into busy loops because that timestamp
			// usually stays fixed among retries, so it will never realize
			// that a transaction has timed out. See #877.
			Now:         now,
			PushType:    pushType,
			RangeLookup: args.Method() == proto.RangeLookup,
		}
		bArgs.Add(pushArgs)
	}
	b := &client.Batch{}
	b.InternalAddCall(proto.Call{Args: bArgs, Reply: bReply})

	// Run all pushes in parallel.
	if pushErr := s.db.Run(b); pushErr != nil {
		if log.V(1) {
			log.Infoc(ctx, "on %s: %s", args.Method(), pushErr)
		}

		// For write/write conflicts within a transaction, propagate the
		// push failure, not the original write intent error. The push
		// failure will instruct the client to restart the transaction
		// with a backoff.
		if header.Txn != nil && proto.IsWrite(args) {
			return pushErr
		}
		// For read/write conflicts, return the write intent error which
		// engages backoff/retry (with !Resolved). We don't need to
		// restart the txn, only resend the read with a backoff.
		return wiErr
	}
	wiErr.Resolved = true // success!

	var wg sync.WaitGroup

	// We pushed the transaction(s) successfully, so resolve the intent(s).
	trace.Event("resolving intents [async]")
	for i, intent := range wiErr.Intents {
		pushReply := bReply.Responses[i].PushTxn
		intentKey := intent.Key
		resolveArgs := &proto.ResolveIntentRequest{
			RequestHeader: proto.RequestHeader{
				// Use the pushee's timestamp, which might be lower than the
				// pusher's request timestamp. No need to push the intent higher
				// than the pushee's txn!
				Timestamp: pushReply.PusheeTxn.Timestamp,
				Key:       intentKey,
				User:      security.RootUser,
				Txn:       pushReply.PusheeTxn,
			},
		}

		wg.Add(1)
		ctx := tracer.ToCtx(ctx, trace.Fork())
		if !s.stopper.RunAsyncTask(func() {
			if _, resolveErr := rng.addWriteCmd(ctx, resolveArgs, &wg); resolveErr != nil && log.V(1) {
				log.Warningc(ctx, "resolve for key %s failed: %s", intentKey, resolveErr)
			}
		}) {
			// Couldn't run the task. We need to notify the WorkGroup.
			wg.Done()
		}
	}

	wg.Wait() // wait until all the `ResolveIntent`s have been submitted to raft.

	return wiErr
}

// ProposeRaftCommand submits a command to raft. The command is processed
// asynchronously and an error or nil will be written to the returned
// channel when it is committed or aborted (but note that committed does
// mean that it has been applied to the range yet).
func (s *Store) ProposeRaftCommand(idKey cmdIDKey, cmd proto.RaftCommand) <-chan error {
	value := cmd.Cmd.GetValue()
	if value == nil {
		panic("proposed a nil command")
	}
	// Lazily create group. TODO(bdarnell): make this non-lazy
	err := s.multiraft.CreateGroup(cmd.RangeID)
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
		return s.multiraft.ChangeGroupMembership(cmd.RangeID, string(idKey),
			changeTypeInternalToRaft[crt.ChangeType],
			proto.MakeRaftNodeID(crt.NodeID, crt.StoreID),
			data)
	}
	return s.multiraft.SubmitCommand(cmd.RangeID, string(idKey), data)
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
				var cmd proto.RaftCommand
				var groupID proto.RangeID
				var commandID string
				var index uint64
				var callback func(error)

				switch e := e.(type) {
				case *multiraft.EventCommandCommitted:
					groupID = e.GroupID
					commandID = e.CommandID
					index = e.Index
					err := gogoproto.Unmarshal(e.Command, &cmd)
					if err != nil {
						log.Fatal(err)
					}
					if log.V(6) {
						log.Infof("store %s: new committed %s command at index %d",
							s, cmd.Cmd.GetValue().(proto.Request).Method(), e.Index)
					}

				case *multiraft.EventMembershipChangeCommitted:
					groupID = e.GroupID
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

				if groupID != cmd.RangeID {
					log.Fatalf("e.GroupID (%d) should == cmd.RaftID (%d)", groupID, cmd.RangeID)
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
func (s *Store) GroupStorage(groupID proto.RangeID) multiraft.WriteableGroupStorage {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.ranges[groupID]
	if !ok {
		var err error
		r, err = NewReplica(&proto.RangeDescriptor{
			RangeID: groupID,
			// TODO(bdarnell): other fields are unknown; need to populate them from
			// snapshot.
		}, s)
		if err != nil {
			panic(err) // TODO(bdarnell)
		}
		// Add the range to range map, but not rangesByKey since
		// the range's start key is unknown. The range will be
		// added to rangesByKey later when a snapshot is applied.
		if err = s.addRangeToRangeMap(r); err != nil {
			panic(err) // TODO(bdarnell)
		}
		s.uninitRanges[r.Desc().RangeID] = r
	}
	return r
}

// AppliedIndex implements the multiraft.StateMachine interface.
func (s *Store) AppliedIndex(groupID proto.RangeID) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.ranges[groupID]
	if !ok {
		return 0, util.Errorf("range %d not found", groupID)
	}
	return atomic.LoadUint64(&r.appliedIndex), nil
}

func raftEntryFormatter(data []byte) string {
	if len(data) == 0 {
		return "[empty]"
	}
	var cmd proto.RaftCommand
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
// Returns nil if the scanner has not yet run. The scanner runs once every
// ctx.ScanInterval.
func (s *Store) GetStatus() (*StoreStatus, error) {
	if s.scanner.Count() == 0 {
		// The scanner hasn't completed a first run yet.
		return nil, nil
	}
	key := keys.StoreStatusKey(int32(s.Ident.StoreID))
	status := &StoreStatus{}
	if err := s.db.GetProto(key, status); err != nil {
		return nil, err
	}
	return status, nil
}

// computeReplicationStatus counts a number of simple replication statistics for
// the ranges in this store.
// TODO(bram): It may be appropriate to compute these statistics while scanning
// ranges. An ideal solution would be to create incremental events whenever
// availability changes.
func (s *Store) computeReplicationStatus(now int64) (
	leaderRangeCount, replicatedRangeCount, availableRangeCount int32) {
	// Get the zone configs, which are needed to determine if a range is
	// under-replicated.
	zoneMap, err := s.Gossip().GetInfo(gossip.KeyConfigZone)
	if err != nil || zoneMap == nil {
		log.Error("unable to get zone configs")
		return
	}

	timestamp := proto.Timestamp{WallTime: now}
	s.mu.Lock()
	defer s.mu.Unlock()
	for raftID, rng := range s.ranges {
		zoneConfig := zoneMap.(PrefixConfigMap).MatchByPrefix(rng.Desc().StartKey).Config.(*proto.ZoneConfig)
		raftStatus := s.RaftStatus(raftID)
		if raftStatus == nil {
			continue
		}
		if raftStatus.SoftState.RaftState == raft.StateLeader {
			leaderRangeCount++
			// TODO(bram): Compare attributes of the stores so we can track
			// ranges that have enough replicas but still need to be migrated
			// onto nodes with the desired attributes.
			if len(raftStatus.Progress) >= len(zoneConfig.ReplicaAttrs) {
				replicatedRangeCount++
			}

			// If any replica holds the leader lease, the range is available.
			if rng.getLease().Covers(timestamp) {
				availableRangeCount++
			} else {
				// If there is no leader lease, then as long as more than 50%
				// of the replicas are current then it is available.
				current := 0
				for _, progress := range raftStatus.Progress {
					if progress.Match == raftStatus.Applied {
						current++
					} else {
						current--
					}
				}
				if current > 0 {
					availableRangeCount++
				}
			}
		}
	}
	return
}

// PublishStatus publishes periodically computed status events to the store's
// events feed. This method itself should be periodically called by some
// external mechanism.
func (s *Store) PublishStatus() error {
	// broadcast store descriptor.
	desc, err := s.Descriptor()
	if err != nil {
		return err
	}
	s.feed.storeStatus(desc)

	// broadcast replication status.
	now := s.ctx.Clock.Now().WallTime
	leaderRangeCount, replicatedRangeCount, availableRangeCount :=
		s.computeReplicationStatus(now)
	s.feed.replicationStatus(leaderRangeCount, replicatedRangeCount, availableRangeCount)
	return nil
}

// SetRangeRetryOptions sets the retry options used for this store.
// For unittests only.
func (s *Store) SetRangeRetryOptions(ro retry.Options) {
	s.ctx.RangeRetryOptions = ro
}
