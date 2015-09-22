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

	"github.com/google/btree"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
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
)

const (
	// GCResponseCacheExpiration is the expiration duration for response
	// cache entries.
	GCResponseCacheExpiration = 1 * time.Hour
	// rangeIDAllocCount is the number of Range IDs to allocate per allocation.
	rangeIDAllocCount               = 10
	defaultRaftTickInterval         = 100 * time.Millisecond
	defaultHeartbeatIntervalTicks   = 3
	defaultRaftElectionTimeoutTicks = 15
	// ttlStoreGossip is time-to-live for store-related info.
	ttlStoreGossip = 2 * time.Minute
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

var changeTypeInternalToRaft = map[proto.ReplicaChangeType]raftpb.ConfChangeType{
	proto.ADD_REPLICA:    raftpb.ConfChangeAddNode,
	proto.REMOVE_REPLICA: raftpb.ConfChangeRemoveNode,
}

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
// than or equal to KeyMax. It also verifies that a key range that
// contains range-local keys is completely range-local.
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
	if bytes.HasPrefix(start, keys.LocalRangePrefix) && !bytes.HasPrefix(end, keys.LocalRangePrefix) {
		return util.Errorf("start key is range-local, but end key is not")
	}
	return nil
}

type errWithIndex struct {
	index int32
	err   error
}

func (ewi *errWithIndex) Error() string {
	return fmt.Sprintf("at %d: %s", ewi.index, ewi.err)
}

// unwrapIndexedError returns the wrapped error for an *errWithIndex, and
// the given error otherwise.
func unwrapIndexedError(err error) error {
	if iErr, ok := err.(*errWithIndex); ok {
		return iErr.err
	}
	return err
}

type rangeAlreadyExists struct {
	rng *Replica
}

// Error implements the error interface.
func (e *rangeAlreadyExists) Error() string {
	return fmt.Sprintf("range for Range ID %d already exists on store", e.rng.Desc().RangeID)
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
	store    *Store
	rangeIDs []proto.RangeID // Range IDs of ranges to be visited.
	visited  int             // Number of visited ranges. -1 when Visit() is not being called.
}

func newStoreRangeSet(store *Store) *storeRangeSet {
	return &storeRangeSet{
		store:   store,
		visited: 0,
	}
}

func (rs *storeRangeSet) Visit(visitor func(*Replica) bool) {
	// Copy the  range IDs to a slice and iterate over the slice so
	// that we can safely (e.g., no race, no range skip) iterate
	// over ranges regardless of how BTree is implemented.
	rs.store.mu.RLock()
	rs.rangeIDs = make([]proto.RangeID, rs.store.replicasByKey.Len())
	i := 0
	rs.store.replicasByKey.Ascend(func(item btree.Item) bool {
		rs.rangeIDs[i] = item.(*Replica).Desc().RangeID
		i++
		return true
	})
	rs.store.mu.RUnlock()

	rs.visited = 0
	for _, rangeID := range rs.rangeIDs {
		rs.visited++
		rs.store.mu.RLock()
		rng, ok := rs.store.replicas[rangeID]
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
		return rs.store.replicasByKey.Len()
	}
	return len(rs.rangeIDs) - rs.visited
}

// A Store maintains a map of ranges by start key. A Store corresponds
// to one physical device.
type Store struct {
	Ident             proto.StoreIdent
	ctx               StoreContext
	db                *client.DB
	engine            engine.Engine   // The underlying key-value store
	_allocator        Allocator       // Makes allocation decisions
	rangeIDAlloc      *idAllocator    // Range ID allocator
	gcQueue           *gcQueue        // Garbage collection queue
	_splitQueue       *splitQueue     // Range splitting queue
	verifyQueue       *verifyQueue    // Checksum verification queue
	replicateQueue    replicateQueue  // Replication queue
	_rangeGCQueue     *rangeGCQueue   // Range GC queue
	scanner           *replicaScanner // Range scanner
	feed              StoreEventFeed  // Event Feed
	removeReplicaChan chan removeReplicaOp
	proposeChan       chan proposeOp
	multiraft         *multiraft.MultiRaft
	started           int32
	stopper           *stop.Stopper
	startedAt         int64
	nodeDesc          *proto.NodeDescriptor
	initComplete      sync.WaitGroup             // Signaled by async init tasks
	mu                sync.RWMutex               // Protects variables below...
	replicas          map[proto.RangeID]*Replica // Map of replicas by Range ID
	replicasByKey     *btree.BTree               // btree keyed by ranges end keys.
	uninitReplicas    map[proto.RangeID]*Replica // Map of uninitialized replicas by Range ID
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
	StorePool *StorePool
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

	// TimeUntilStoreDead is the time after which if there is no new gossiped
	// information about a store, it can be considered dead.
	TimeUntilStoreDead time.Duration

	// RebalancingOptions configures how the store will attempt to rebalance its
	// replicas to other stores.
	RebalancingOptions RebalancingOptions

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
		ctx:               ctx,
		db:                ctx.DB, // TODO(tschottdorf) remove redundancy.
		engine:            eng,
		_allocator:        MakeAllocator(ctx.StorePool, ctx.RebalancingOptions),
		replicas:          map[proto.RangeID]*Replica{},
		replicasByKey:     btree.New(64 /* degree */),
		uninitReplicas:    map[proto.RangeID]*Replica{},
		nodeDesc:          nodeDesc,
		removeReplicaChan: make(chan removeReplicaOp),
		proposeChan:       make(chan proposeOp),
	}

	// Add range scanner and configure with queues.
	s.scanner = newReplicaScanner(ctx.ScanInterval, ctx.ScanMaxIdleTime, newStoreRangeSet(s))
	s.gcQueue = newGCQueue(s.ctx.Gossip)
	s._splitQueue = newSplitQueue(s.db, s.ctx.Gossip)
	s.verifyQueue = newVerifyQueue(s.ctx.Gossip, s.ReplicaCount)
	s.replicateQueue = makeReplicateQueue(s.ctx.Gossip, s.allocator(), s.ctx.Clock, s.ctx.RebalancingOptions)
	s._rangeGCQueue = newRangeGCQueue(s.db, s.ctx.Gossip)
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
	idAlloc, err := newIDAllocator(keys.RangeIDGenerator, s.db, 2 /* min ID */, rangeIDAllocCount, s.stopper)
	if err != nil {
		return err
	}
	s.rangeIDAlloc = idAlloc

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
	}, s.stopper); err != nil {
		return err
	}

	// Iterate over all range descriptors, ignoring uncommitted versions
	// (consistent=false). Uncommitted intents which have been abandoned
	// due to a split crashing halfway will simply be resolved on the
	// next split attempt. They can otherwise be ignored.
	s.mu.Lock()
	s.feed.beginScanRanges()
	if _, err := engine.MVCCIterate(s.engine, start, end, now, false /* !consistent */, nil, /* txn */
		false /* !reverse */, func(kv proto.KeyValue) (bool, error) {
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
			if err = s.addReplicaInternal(rng); err != nil {
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
		// Register callbacks for any changes to the system config.
		// This may trigger splits along structured boundaries,
		// and update max range bytes.
		s.ctx.Gossip.RegisterSystemConfigCallback(s.systemGossipUpdate)

		// Start a single goroutine in charge of periodically gossiping the
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
		if err := s.maybeGossipSystemConfig(); err != nil {
			log.Warningc(ctx, "error gossiping system config: %s", err)
		}
		s.initComplete.Done()
		ticker := time.NewTicker(configGossipInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := s.maybeGossipSystemConfig(); err != nil {
					log.Warningc(ctx, "error gossiping system config: %s", err)
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
	rng := s.LookupReplica(proto.KeyMin, nil)
	if rng != nil {
		return rng.maybeGossipFirstRange()
	}
	return nil
}

// maybeGossipSystemConfig looks for the range containing SystemDB keys and
// lets that range gossip them.
func (s *Store) maybeGossipSystemConfig() error {
	rng := s.LookupReplica(keys.SystemDBSpan.Start, nil)
	if rng == nil {
		// This store has no range with this configuration.
		return nil
	}
	// Wake up the replica. If it acquires a fresh lease, it will
	// gossip. If an unexpected error occurs (i.e. nobody else seems to
	// have an active lease but we still failed to obtain it), return
	// that error.
	_, err := rng.getLeaseForGossip(s.Context(nil))
	return err
}

// systemGossipUpdate is a callback for gossip updates to
// the system config which affect range split boundaries.
func (s *Store) systemGossipUpdate(cfg *config.SystemConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// For every range, update its MaxBytes and check if it needs to be split.
	for _, rng := range s.replicas {
		if zone, err := cfg.GetZoneConfigForKey(rng.Desc().StartKey); err == nil {
			rng.SetMaxBytes(zone.RangeMaxBytes)
		}
		s.splitQueue().MaybeAdd(rng, s.ctx.Clock.Now())
	}
}

// GossipStore broadcasts the store on the gossip network.
func (s *Store) GossipStore() {
	ctx := s.Context(nil)

	storeDesc, err := s.Descriptor()
	if err != nil {
		log.Warningc(ctx, "problem getting store descriptor for store %+v: %v", s.Ident, err)
		return
	}
	// Unique gossip key per store.
	gossipStoreKey := gossip.MakeStoreKey(storeDesc.StoreID)
	// Gossip store descriptor.
	if err := s.ctx.Gossip.AddInfoProto(gossipStoreKey, storeDesc, ttlStoreGossip); err != nil {
		log.Warningc(ctx, "%s", err)
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

	for _, r := range s.replicas {
		s.replicateQueue.MaybeAdd(r, s.ctx.Clock.Now())
	}
}

// ForceRangeGCScan iterates over all ranges and enqueues any that
// may need to be GC'd. Exposed only for testing.
func (s *Store) ForceRangeGCScan(t util.Tester) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, r := range s.replicas {
		s._rangeGCQueue.MaybeAdd(r, s.ctx.Clock.Now())
	}
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

// GetReplica fetches a replica by Range ID. Returns an error if no replica is found.
func (s *Store) GetReplica(rangeID proto.RangeID) (*Replica, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if rng, ok := s.replicas[rangeID]; ok {
		return rng, nil
	}
	return nil, proto.NewRangeNotFoundError(rangeID)
}

// LookupReplica looks up a replica via binary search over the
// "replicasByKey" btree. Returns nil if no replica is found for
// specified key range. Note that the specified keys are transformed
// using Key.Address() to ensure we lookup replicas correctly for local
// keys. When end is nil, a replica that contains start is looked up.
func (s *Store) LookupReplica(start, end proto.Key) *Replica {
	s.mu.RLock()
	defer s.mu.RUnlock()
	startAddr := keys.KeyAddress(start)
	endAddr := keys.KeyAddress(end)

	var rng *Replica
	s.replicasByKey.AscendGreaterOrEqual((rangeBTreeKey)(startAddr.Next()), func(i btree.Item) bool {
		rng = i.(*Replica)
		return false
	})
	if rng == nil || !rng.Desc().ContainsKeyRange(startAddr, endAddr) {
		return nil
	}
	return rng
}

// RaftStatus returns the current raft status of the given range.
func (s *Store) RaftStatus(rangeID proto.RangeID) *raft.Status {
	return s.multiraft.Status(rangeID)
}

// BootstrapRange creates the first range in the cluster and manually
// writes it to the store. Default range addressing records are
// created for meta1 and meta2. Default configurations for
// zones are created. All configs are specified
// for the empty key prefix, meaning they apply to the entire
// database. The zone requires three replicas with no other specifications.
// It also adds the range tree and the root node, the first range, to it.
// The 'initialValues' are written as well after each value's checksum
// is initalized.
func (s *Store) BootstrapRange(initialValues []proto.KeyValue) error {
	desc := &proto.RangeDescriptor{
		RangeID:       1,
		StartKey:      proto.KeyMin,
		EndKey:        proto.KeyMax,
		NextReplicaID: 2,
		Replicas: []proto.Replica{
			{
				NodeID:    1,
				StoreID:   1,
				ReplicaID: 1,
			},
		},
	}
	if err := desc.Validate(); err != nil {
		return err
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

	// Now add all passed-in default entries.
	for _, kv := range initialValues {
		// Initialize the checksums.
		kv.Value.InitChecksum(kv.Key)
		if err := engine.MVCCPut(batch, ms, kv.Key, now, kv.Value, nil); err != nil {
			return err
		}
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
func (s *Store) allocator() Allocator { return s._allocator }

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
// keys and the supplied proto.Replicas slice. It allocates new
// replica IDs to fill out the supplied replicas.
func (s *Store) NewRangeDescriptor(start, end proto.Key, replicas []proto.Replica) (*proto.RangeDescriptor, error) {
	id, err := s.rangeIDAlloc.Allocate()
	if err != nil {
		return nil, err
	}
	desc := &proto.RangeDescriptor{
		RangeID:       proto.RangeID(id),
		StartKey:      start,
		EndKey:        end,
		Replicas:      append([]proto.Replica(nil), replicas...),
		NextReplicaID: proto.ReplicaID(len(replicas) + 1),
	}
	for i := range desc.Replicas {
		desc.Replicas[i].ReplicaID = proto.ReplicaID(i + 1)
	}
	return desc, nil
}

// SplitRange shortens the original range to accommodate the new
// range. The new range is added to the ranges map and the rangesByKey
// btree.
func (s *Store) SplitRange(origRng, newRng *Replica) error {
	origDesc := origRng.Desc()
	newDesc := newRng.Desc()

	if !bytes.Equal(origDesc.EndKey, newDesc.EndKey) ||
		bytes.Compare(origDesc.StartKey, newDesc.StartKey) >= 0 {
		return util.Errorf("orig range is not splittable by new range: %+v, %+v", origDesc, newDesc)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// Replace the end key of the original range with the start key of
	// the new range. Reinsert the range since the btree is keyed by range end keys.
	if s.replicasByKey.Delete(origRng) == nil {
		return util.Errorf("couldn't find range %s in rangesByKey btree", origRng)
	}

	copyDesc := *origDesc
	copyDesc.EndKey = append([]byte(nil), newDesc.StartKey...)
	origRng.setDescWithoutProcessUpdate(&copyDesc)

	if s.replicasByKey.ReplaceOrInsert(origRng) != nil {
		return util.Errorf("couldn't insert range %v in rangesByKey btree", origRng)
	}
	if err := s.addReplicaInternal(newRng); err != nil {
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
// on the same store. Must be called from the processRaft goroutine.
func (s *Store) MergeRange(subsumingRng *Replica, updatedEndKey proto.Key, subsumedRangeID proto.RangeID) error {
	subsumingDesc := subsumingRng.Desc()

	if !subsumingDesc.EndKey.Less(updatedEndKey) {
		return util.Errorf("the new end key is not greater than the current one: %+v <= %+v",
			updatedEndKey, subsumingDesc.EndKey)
	}

	subsumedRng, err := s.GetReplica(subsumedRangeID)
	if err != nil {
		return util.Errorf("could not find the subsumed range: %d", subsumedRangeID)
	}
	subsumedDesc := subsumedRng.Desc()

	if !replicaSetsEqual(subsumedDesc.Replicas, subsumingDesc.Replicas) {
		return util.Errorf("ranges are not on the same replicas sets: %+v != %+v",
			subsumedDesc.Replicas, subsumingDesc.Replicas)
	}

	// Remove and destroy the subsumed range. Note that we are on the
	// processRaft goroutine so we can call removeReplicaImpl directly.
	if err := s.removeReplicaImpl(subsumedRng); err != nil {
		return util.Errorf("cannot remove range %s", err)
	}

	localRangeKeyPrefix := keys.MakeLocalRangeKeyPrefix(subsumedDesc.RangeID)
	if _, err := engine.MVCCDeleteRange(s.engine, nil, localRangeKeyPrefix, localRangeKeyPrefix.PrefixEnd(), 0, proto.ZeroTimestamp, nil); err != nil {
		return util.Errorf("cannot remove range metadata %s", err)
	}

	// Update the end key of the subsuming range.
	copy := *subsumingDesc
	copy.EndKey = updatedEndKey
	if err := subsumingRng.setDesc(&copy); err != nil {
		return err
	}

	s.feed.mergeRange(subsumingRng, subsumedRng)
	return nil
}

// AddReplicaTest adds the replica to the store's replica map and to the sorted
// replicasByKey slice. To be used only by unittests.
func (s *Store) AddReplicaTest(rng *Replica) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.addReplicaInternal(rng); err != nil {
		return err
	}
	s.feed.registerRange(rng, false /* scan */)
	return nil
}

// addReplicaInternal adds the replica to the replicas map and the replicasByKey btree.
// This method presupposes the store's lock is held. Returns a rangeAlreadyExists
// error if a replica with the same Range ID has already been added to this store.
func (s *Store) addReplicaInternal(rng *Replica) error {
	if !rng.isInitialized() {
		return util.Errorf("attempted to add uninitialized range %s", rng)
	}

	// TODO(spencer); will need to determine which range is
	// newer, and keep that one.
	if err := s.addReplicaToRangeMap(rng); err != nil {
		return err
	}

	if s.replicasByKey.Has(rng) {
		return &rangeAlreadyExists{rng}
	}
	if exRngItem := s.replicasByKey.ReplaceOrInsert(rng); exRngItem != nil {
		return util.Errorf("range for key %v already exists in rangesByKey btree",
			(exRngItem.(*Replica)).getKey())
	}
	return nil
}

// addReplicaToRangeMap adds the replica to the replicas map.
func (s *Store) addReplicaToRangeMap(rng *Replica) error {
	rangeID := rng.Desc().RangeID

	if exRng, ok := s.replicas[rangeID]; ok {
		return &rangeAlreadyExists{exRng}
	}
	s.replicas[rangeID] = rng
	return nil
}

type removeReplicaOp struct {
	rep *Replica
	ch  chan<- error
}

// RemoveReplica removes the replica from the store's replica map and from
// the sorted replicasByKey btree.
func (s *Store) RemoveReplica(rep *Replica) error {

	ch := make(chan error)
	s.removeReplicaChan <- removeReplicaOp{rep, ch}
	return <-ch
}

// removeReplicaImpl runs on the processRaft goroutine.
func (s *Store) removeReplicaImpl(rep *Replica) error {
	rangeID := rep.Desc().RangeID

	// RemoveGroup needs to access the storage, which in turn needs the
	// lock. Some care is needed to avoid deadlocks. We remove the group
	// from multiraft outside the scope of s.mu; this is effectively
	// synchronized by the fact that this method runs on the processRaft
	// goroutine.
	if err := s.multiraft.RemoveGroup(rangeID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.replicas, rangeID)
	if s.replicasByKey.Delete(rep) == nil {
		return util.Errorf("couldn't find range in replicasByKey btree")
	}
	s.scanner.RemoveReplica(rep)
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

	rangeID := rng.Desc().RangeID

	if _, ok := s.uninitReplicas[rangeID]; !ok {
		// Do nothing if the range has already been initialized.
		return nil
	}
	delete(s.uninitReplicas, rangeID)
	s.feed.registerRange(rng, false /* scan */)

	if s.replicasByKey.Has(rng) {
		return &rangeAlreadyExists{rng}
	}
	if exRngItem := s.replicasByKey.ReplaceOrInsert(rng); exRngItem != nil {
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
	capacity.RangeCount = int32(s.ReplicaCount())
	// Initialize the store descriptor.
	return &proto.StoreDescriptor{
		StoreID:  s.Ident.StoreID,
		Attrs:    s.Attrs(),
		Node:     *s.nodeDesc,
		Capacity: capacity,
	}, nil
}

// ReplicaCount returns the number of replicas contained by this store.
func (s *Store) ReplicaCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.replicas)
}

// ExecuteCmd fetches a range based on the header's replica, assembles
// method, args & reply into a Raft Cmd struct and executes the
// command using the fetched range.
func (s *Store) ExecuteCmd(ctx context.Context, args proto.Request) (proto.Response, *proto.Error) {
	ctx = s.Context(ctx)
	trace := tracer.FromCtx(ctx)
	// If the request has a zero timestamp, initialize to this node's clock.
	header := args.Header()
	// TODO(tschottdorf): remove this first branch when we only have batches here.
	if args.Method() != proto.Batch {
		if err := verifyKeys(header.Key, header.EndKey, proto.IsRange(args)); err != nil {
			return nil, proto.NewError(err)
		}
	} else {
		for _, union := range args.(*proto.BatchRequest).Requests {
			arg := union.GetInner()
			header := arg.Header()
			if err := verifyKeys(header.Key, header.EndKey, proto.IsRange(arg)); err != nil {
				return nil, proto.NewError(err)
			}
		}
	}
	if !header.Timestamp.Equal(proto.ZeroTimestamp) {
		if s.Clock().MaxOffset() > 0 {
			// Once a command is submitted to raft, all replicas' logical
			// clocks will be ratcheted forward to match. If the command
			// appears to come from a node with a bad clock, reject it now
			// before we reach that point.
			offset := time.Duration(header.Timestamp.WallTime - s.Clock().PhysicalNow())
			if offset > s.Clock().MaxOffset() {
				return nil, proto.NewError(util.Errorf("Rejecting command with timestamp in the future: %d (%s ahead)",
					header.Timestamp.WallTime, offset))
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
		rng, err = s.GetReplica(header.RangeID)
		if err != nil {
			return nil, proto.NewError(err)
		}

		var reply proto.Response
		var index *int32
		reply, err = rng.AddCmd(ctx, args)
		if err == nil {
			return reply, nil
		} else if err == multiraft.ErrGroupDeleted {
			// This error needs to be converted appropriately so that
			// clients will retry.
			err = proto.NewRangeNotFoundError(rng.Desc().RangeID)
		} else if iErr, ok := err.(*errWithIndex); ok {
			err, index = iErr.err, gogoproto.Int32(iErr.index)
		}

		// Maybe resolve a potential write intent error. We do this here
		// because this is the code path with the requesting client
		// waiting. We don't want every replica to attempt to resolve the
		// intent independently, so we can't do it in Replica.executeCmd.
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
		case *proto.ReadWithinUncertaintyIntervalError:
			t.NodeID = header.Replica.NodeID
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
		pErr := proto.NewError(err)
		if index != nil {
			pErr.Index = &proto.Error_Index{Index: *index}
		}
		return nil, pErr
	}

	// By default, retries are indefinite. However, some unittests set a
	// maximum retry count; return txn retry error for transactional cases
	// and the original error otherwise.
	trace.Event("store retry limit exceeded") // good to check for if tests fail
	if header.Txn != nil {
		return nil, proto.NewError(proto.NewTransactionRetryError(header.Txn))
	}
	return nil, proto.NewError(err)
}

// resolveWriteIntentError tries to push the conflicting transaction (if
// necessary, i.e. if the transaction is pending): either move its timestamp
// forward on a read/write conflict, or abort it on a write/write conflict. If
// the push succeeds (or if it wasn't necessary), we immediately issue a
// resolve intent command and set the error's Resolved flag to true so the
// client retries the command immediately. If the push fails, we set the
// error's Resolved flag to false so that the client backs off before reissuing
// the command.
// On write/write conflicts, a potential push error is returned; otherwise
// the updated WriteIntentError.
//
// Callers are involved with
// a) conflict resolution for commands being executed at the Store with the
//    client waiting,
// b) resolving intents encountered during inconsistent operations, and
// c) resolving intents upon EndTransaction which are not local to the given
//    range. This is the only path in which the transaction is going to be
//    in non-pending state and doesn't require a push.
func (s *Store) resolveWriteIntentError(ctx context.Context, wiErr *proto.WriteIntentError, rng *Replica, args proto.Request, pushType proto.PushTxnType) error {
	if log.V(6) {
		log.Infoc(ctx, "resolving write intent %s", wiErr)
	}
	trace := tracer.FromCtx(ctx)
	defer trace.Epoch("intent resolution")()

	// Split intents into those we need to push and those which are good to
	// resolve.
	// TODO(tschottdorf): can optimize this and use same underlying slice.
	var pushIntents, resolveIntents []proto.Intent
	for _, intent := range wiErr.Intents {
		// The current intent does not need conflict resolution.
		if intent.Txn.Status != proto.PENDING {
			resolveIntents = append(resolveIntents, intent)
		} else {
			pushIntents = append(pushIntents, intent)
		}
	}

	// Attempt to push the transaction(s) which created the conflicting intent(s).
	now := s.Clock().Now()
	header := args.Header()
	ba := &proto.BatchRequest{
		RequestHeader: proto.RequestHeader{
			Timestamp:    header.Timestamp,
			UserPriority: header.UserPriority,
		},
	}
	br := &proto.BatchResponse{}
	for _, intent := range pushIntents {
		pushArgs := &proto.PushTxnRequest{
			RequestHeader: proto.RequestHeader{
				Key: intent.Txn.Key,
			},
			PusherTxn: header.Txn,
			PusheeTxn: intent.Txn,
			// The timestamp is used by PushTxn for figuring out whether the
			// transaction is abandoned. If we used the argument's timestamp
			// here, we would run into busy loops because that timestamp
			// usually stays fixed among retries, so it will never realize
			// that a transaction has timed out. See #877.
			Now:      now,
			PushType: pushType,
		}
		ba.Add(pushArgs)
	}
	b := &client.Batch{}
	if len(ba.Requests) > 0 {
		b.InternalAddCall(proto.Call{Args: ba, Reply: br})
	}

	// Run all pushes in parallel.
	if pushErr := s.db.Run(b).GoError(); pushErr != nil {
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

	for i, intent := range pushIntents {
		intent.Txn = *(br.Responses[i].GetInner().(*proto.PushTxnResponse).PusheeTxn)
		resolveIntents = append(resolveIntents, intent)
	}

	rng.resolveIntents(ctx, resolveIntents)

	return wiErr
}

type proposeOp struct {
	idKey cmdIDKey
	cmd   proto.RaftCommand
	ch    chan<- <-chan error
}

// ProposeRaftCommand submits a command to raft. The command is processed
// asynchronously and an error or nil will be written to the returned
// channel when it is committed or aborted (but note that committed does
// mean that it has been applied to the range yet).
func (s *Store) ProposeRaftCommand(idKey cmdIDKey, cmd proto.RaftCommand) <-chan error {
	ch := make(chan (<-chan error))
	s.proposeChan <- proposeOp{idKey, cmd, ch}
	return <-ch
}

// proposeRaftCommandImpl runs on the processRaft goroutine.
func (s *Store) proposeRaftCommandImpl(idKey cmdIDKey, cmd proto.RaftCommand) <-chan error {
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
	for _, union := range cmd.Cmd.Requests {
		args := union.GetInner()
		etr, ok := args.(*proto.EndTransactionRequest)
		if ok && etr.InternalCommitTrigger != nil && etr.InternalCommitTrigger.ChangeReplicasTrigger != nil {
			// TODO(tschottdorf): the real check is that EndTransaction needs
			// to be the last element in the batch. Any caveats to solve before
			// changing this?
			if len(cmd.Cmd.Requests) != 1 {
				panic("EndTransaction should only ever occur by itself in a batch")
			}
			// EndTransactionRequest with a ChangeReplicasTrigger is special because raft
			// needs to understand it; it cannot simply be an opaque command.
			crt := etr.InternalCommitTrigger.ChangeReplicasTrigger
			return s.multiraft.ChangeGroupMembership(cmd.RangeID, string(idKey),
				changeTypeInternalToRaft[crt.ChangeType],
				proto.MakeRaftNodeID(crt.NodeID, crt.StoreID),
				data)
		}
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
			case events := <-s.multiraft.Events:
				for _, e := range events {
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
							log.Infof("store %s: new committed command at index %d", s, e.Index)
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
						log.Fatalf("e.GroupID (%d) should == cmd.RangeID (%d)", groupID, cmd.RangeID)
					}

					s.mu.RLock()
					r, ok := s.replicas[groupID]
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
				}

			case op := <-s.removeReplicaChan:
				op.ch <- s.removeReplicaImpl(op.rep)

			case op := <-s.proposeChan:
				op.ch <- s.proposeRaftCommandImpl(op.idKey, op.cmd)

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
	r, ok := s.replicas[groupID]
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
		if err = s.addReplicaToRangeMap(r); err != nil {
			panic(err) // TODO(bdarnell)
		}
		s.uninitReplicas[r.Desc().RangeID] = r
	}
	return r
}

// AppliedIndex implements the multiraft.StateMachine interface.
func (s *Store) AppliedIndex(groupID proto.RangeID) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.replicas[groupID]
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
	// Load the system config.
	cfg := s.Gossip().GetSystemConfig()
	if cfg == nil {
		log.Infof("system config not yet available")
		return
	}

	timestamp := proto.Timestamp{WallTime: now}
	s.mu.Lock()
	defer s.mu.Unlock()
	for rangeID, rng := range s.replicas {
		zoneConfig, err := cfg.GetZoneConfigForKey(rng.Desc().StartKey)
		if err != nil {
			log.Error(err)
			continue
		}
		raftStatus := s.RaftStatus(rangeID)
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
