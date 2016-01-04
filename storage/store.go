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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/btree"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/cache"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/tracer"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
)

const (
	// rangeIDAllocCount is the number of Range IDs to allocate per allocation.
	rangeIDAllocCount               = 10
	defaultRaftTickInterval         = 100 * time.Millisecond
	defaultHeartbeatIntervalTicks   = 3
	defaultRaftElectionTimeoutTicks = 15
	// ttlStoreGossip is time-to-live for store-related info.
	ttlStoreGossip = 2 * time.Minute

	// TODO(bdarnell): Determine the right size for this cache. Should
	// the cache be partitioned so that replica descriptors from the
	// range descriptors (which are the bulk of the data and can be
	// reloaded from disk as needed) don't crowd out the
	// message/snapshot descriptors (whose necessity is short-lived but
	// cannot be recovered through other means if evicted)?
	maxReplicaDescCacheSize = 1000

	raftReqBufferSize = 100
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

var changeTypeInternalToRaft = map[roachpb.ReplicaChangeType]raftpb.ConfChangeType{
	roachpb.ADD_REPLICA:    raftpb.ConfChangeAddNode,
	roachpb.REMOVE_REPLICA: raftpb.ConfChangeRemoveNode,
}

// errRaftGroupDeleted is returned for commands which are pending
// while their group is deleted.
var errRaftGroupDeleted = errors.New("raft group deleted")

// verifyKeys verifies keys. If checkEndKey is true, then the end key
// is verified to be non-nil and greater than start key. If
// checkEndKey is false, end key is verified to be nil. Additionally,
// verifies that start key is less than KeyMax and end key is less
// than or equal to KeyMax. It also verifies that a key range that
// contains range-local keys is completely range-local.
func verifyKeys(start, end roachpb.Key, checkEndKey bool) error {
	if bytes.Compare(start, roachpb.KeyMax) >= 0 {
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
	if bytes.Compare(roachpb.KeyMax, end) < 0 {
		return util.Errorf("end key %q must be less than or equal to KeyMax", end)
	}
	{
		sAddr, eAddr := keys.Addr(start), keys.Addr(end)
		if !sAddr.Less(eAddr) {
			return util.Errorf("end key %q must be greater than start %q", end, start)
		}
		if !bytes.Equal(sAddr, start) {
			if bytes.Equal(eAddr, end) {
				return util.Errorf("start key is range-local, but end key is not")
			}
		} else if bytes.Compare(start, keys.LocalMax) < 0 {
			// It's a range op, not local but somehow plows through local data -
			// not cool.
			return util.Errorf("start key in [%q,%q) must be greater than LocalMax", start, end)
		}
	}

	return nil
}

type rangeAlreadyExists struct {
	rng *Replica
}

// Error implements the error interface.
func (e rangeAlreadyExists) Error() string {
	return fmt.Sprintf("range for Range ID %d already exists on store", e.rng.Desc().RangeID)
}

// rangeKeyItem is a common interface for roachpb.Key and Range.
type rangeKeyItem interface {
	getKey() roachpb.RKey
}

// rangeBTreeKey is a type alias of roachpb.RKey that implements the
// rangeKeyItem interface and the btree.Item interface.
type rangeBTreeKey roachpb.RKey

var _ rangeKeyItem = rangeBTreeKey{}

func (k rangeBTreeKey) getKey() roachpb.RKey {
	return (roachpb.RKey)(k)
}

var _ btree.Item = rangeBTreeKey{}

func (k rangeBTreeKey) Less(i btree.Item) bool {
	return k.getKey().Less(i.(rangeKeyItem).getKey())
}

var _ rangeKeyItem = &Replica{}

func (r *Replica) getKey() roachpb.RKey {
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
	rangeIDs []roachpb.RangeID // Range IDs of ranges to be visited.
	visited  int               // Number of visited ranges. -1 when Visit() is not being called.
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
	rs.rangeIDs = make([]roachpb.RangeID, rs.store.replicasByKey.Len())
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

type replicaDescCacheKey struct {
	groupID   roachpb.RangeID
	replicaID roachpb.ReplicaID
}

// A Store maintains a map of ranges by start key. A Store corresponds
// to one physical device.
type Store struct {
	Ident             roachpb.StoreIdent
	ctx               StoreContext
	db                *client.DB
	engine            engine.Engine   // The underlying key-value store
	allocator         Allocator       // Makes allocation decisions
	rangeIDAlloc      *idAllocator    // Range ID allocator
	gcQueue           *gcQueue        // Garbage collection queue
	splitQueue        *splitQueue     // Range splitting queue
	verifyQueue       *verifyQueue    // Checksum verification queue
	replicateQueue    *replicateQueue // Replication queue
	replicaGCQueue    *replicaGCQueue // Replica GC queue
	raftLogQueue      *raftLogQueue   // Raft Log Truncation queue
	scanner           *replicaScanner // Replica scanner
	feed              StoreEventFeed  // Event Feed
	removeReplicaChan chan removeReplicaOp
	wakeRaftLoop      chan struct{}
	started           int32
	stopper           *stop.Stopper
	startedAt         int64
	nodeDesc          *roachpb.NodeDescriptor
	initComplete      sync.WaitGroup // Signaled by async init tasks

	// Lock ordering notes: The processRaft goroutine acts as a kind of
	// mutex. To avoid deadlocks, the following lock order must be
	// obeyed: processRaft goroutine < Store.mu < Replica.mu. (i.e.
	// methods like removeReplica which depend on the processRaft
	// goroutine must not be called while holding Store.mu).
	mu             sync.RWMutex                 // Protects variables below...
	replicas       map[roachpb.RangeID]*Replica // Map of replicas by Range ID
	replicasByKey  *btree.BTree                 // btree keyed by ranges end keys.
	uninitReplicas map[roachpb.RangeID]*Replica // Map of uninitialized replicas by Range ID

	replicaDescCache *cache.UnorderedCache
	// pendingRaftGroups contains the ranges that should be checked for
	// updates. After updating this map, write to wakeRaftLoop to
	// trigger the check.
	pendingRaftGroups map[roachpb.RangeID]struct{}
	raftRequestChan   chan *RaftMessageRequest
}

var _ client.Sender = &Store{}

// A StoreContext encompasses the auxiliary objects and configuration
// required to create a store.
// All fields holding a pointer or an interface are required to create
// a store; the rest will have sane defaults set if omitted.
type StoreContext struct {
	Clock     *hlc.Clock
	DB        *client.DB
	Gossip    *gossip.Gossip
	StorePool *StorePool
	Transport RaftTransport

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

	// AllocatorOptions configures how the store will attempt to rebalance its
	// replicas to other stores.
	AllocatorOptions AllocatorOptions

	// EventFeed is a feed to which this store will publish events.
	EventFeed *util.Feed

	// Tracer is a request tracer.
	Tracer *tracer.Tracer

	// ScannerStopper is used to shut down the background scanner (for tests).
	// If nil, defaults to the store's own stopper.
	ScannerStopper *stop.Stopper
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
func NewStore(ctx StoreContext, eng engine.Engine, nodeDesc *roachpb.NodeDescriptor) *Store {
	// TODO(tschottdorf) find better place to set these defaults.
	ctx.setDefaults()

	if !ctx.Valid() {
		panic(fmt.Sprintf("invalid store configuration: %+v", &ctx))
	}

	s := &Store{
		ctx:               ctx,
		db:                ctx.DB, // TODO(tschottdorf) remove redundancy.
		engine:            eng,
		allocator:         MakeAllocator(ctx.StorePool, ctx.AllocatorOptions),
		replicas:          map[roachpb.RangeID]*Replica{},
		replicasByKey:     btree.New(64 /* degree */),
		uninitReplicas:    map[roachpb.RangeID]*Replica{},
		nodeDesc:          nodeDesc,
		removeReplicaChan: make(chan removeReplicaOp),
		replicaDescCache: cache.NewUnorderedCache(cache.Config{
			Policy: cache.CacheLRU,
			ShouldEvict: func(size int, key, value interface{}) bool {
				return size > maxReplicaDescCacheSize
			},
		}),
		wakeRaftLoop:      make(chan struct{}, 1),
		pendingRaftGroups: map[roachpb.RangeID]struct{}{},
		raftRequestChan:   make(chan *RaftMessageRequest, raftReqBufferSize),
	}

	// Add range scanner and configure with queues.
	s.scanner = newReplicaScanner(ctx.ScanInterval, ctx.ScanMaxIdleTime, newStoreRangeSet(s))
	s.gcQueue = newGCQueue(s.ctx.Gossip)
	s.splitQueue = newSplitQueue(s.db, s.ctx.Gossip)
	s.verifyQueue = newVerifyQueue(s.ctx.Gossip, s.ReplicaCount)
	s.replicateQueue = newReplicateQueue(s.ctx.Gossip, s.allocator, s.ctx.Clock, s.ctx.AllocatorOptions)
	s.replicaGCQueue = newReplicaGCQueue(s.db, s.ctx.Gossip, &s.mu)
	s.raftLogQueue = newRaftLogQueue(s.db, s.ctx.Gossip)
	s.scanner.AddQueues(s.gcQueue, s.splitQueue, s.verifyQueue, s.replicateQueue, s.replicaGCQueue, s.raftLogQueue)

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
		ok, err := engine.MVCCGetProto(s.engine, keys.StoreIdentKey(), roachpb.ZeroTimestamp, true, nil, &s.Ident)
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
	// Always set gossip NodeID before gossiping any info.
	if s.ctx.Gossip != nil {
		s.ctx.Gossip.SetNodeID(s.Ident.NodeID)
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

	// Iterator over all range-local key-based data.
	start := keys.RangeDescriptorKey(roachpb.RKeyMin)
	end := keys.RangeDescriptorKey(roachpb.RKeyMax)

	// Iterate over all range descriptors, ignoring uncommitted versions
	// (consistent=false). Uncommitted intents which have been abandoned
	// due to a split crashing halfway will simply be resolved on the
	// next split attempt. They can otherwise be ignored.
	s.mu.Lock()
	s.feed.beginScanRanges()
	if _, err := engine.MVCCIterate(s.engine, start, end, now, false /* !consistent */, nil, /* txn */
		false /* !reverse */, func(kv roachpb.KeyValue) (bool, error) {
			// Only consider range metadata entries; ignore others.
			_, suffix, _, err := keys.DecodeRangeKey(kv.Key)
			if err != nil {
				return false, err
			}
			if !bytes.Equal(suffix, keys.LocalRangeDescriptorSuffix) {
				return false, nil
			}
			var desc roachpb.RangeDescriptor
			if err := kv.Value.GetProto(&desc); err != nil {
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
	if err := s.ctx.Transport.Listen(s.StoreID(), s.enqueueRaftMessage); err != nil {
		return err
	}
	s.processRaft()

	// Gossip is only ever nil while bootstrapping a cluster and
	// in unittests.
	if s.ctx.Gossip != nil {
		// Register update channel for any changes to the system config.
		// This may trigger splits along structured boundaries,
		// and update max range bytes.
		gossipUpdateC := s.ctx.Gossip.RegisterSystemConfigChannel()
		s.stopper.RunWorker(func() {
			for {
				select {
				case <-gossipUpdateC:
					cfg := s.ctx.Gossip.GetSystemConfig()
					s.systemGossipUpdate(cfg)
				case <-s.stopper.ShouldStop():
					return
				}
			}
		})

		// Start a single goroutine in charge of periodically gossiping the
		// sentinel and first range metadata if we have a first range.
		// This may wake up ranges and requires everything to be set up and
		// running.
		s.startGossip()

		// Start the scanner. The construction here makes sure that the scanner
		// only starts after Gossip has connected, and that it does not block Start
		// from returning (as doing so might prevent Gossip from ever connecting).
		scannerStopper := s.ctx.ScannerStopper
		if scannerStopper == nil {
			scannerStopper = s.stopper
		}
		scannerStopper.RunWorker(func() {
			select {
			case <-s.ctx.Gossip.Connected:
				s.scanner.Start(s.ctx.Clock, scannerStopper)
			case <-scannerStopper.ShouldStop():
				return
			}
		})

	}

	// Initialize txn entry GC - for now, it's pretty much disabled. See
	// #2062, #3005.
	s.engine.SetGCTimeouts(0)

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

// maybeGossipFirstRange checks whether the store has a replica of the
// first range and if so instructs it to gossip the cluster ID,
// sentinel gossip and first range descriptor. This is done in a retry
// loop in the event that the returned error indicates that the state
// of the leader lease is not known. This can happen on lease command
// timeouts. The retry loop makes sure we try hard to keep asking for
// the lease instead of waiting for the next clusterIDGossipInterval
// to transpire.
func (s *Store) maybeGossipFirstRange() error {
	retryOptions := retry.Options{
		InitialBackoff: 100 * time.Millisecond, // first backoff at 100ms
		MaxBackoff:     1 * time.Second,        // max backoff is 1s
		Multiplier:     2,                      // doubles
		Closer:         s.stopper.ShouldStop(), // stop no matter what on stopper
	}
	for loop := retry.Start(retryOptions); loop.Next(); {
		rng := s.LookupReplica(roachpb.RKeyMin, nil)
		if rng != nil {
			err := rng.maybeGossipFirstRange()
			if nlErr, ok := err.(*roachpb.NotLeaderError); !ok || nlErr.Leader != nil {
				return err
			}
		} else {
			return nil
		}
	}
	return nil
}

// maybeGossipSystemConfig looks for the range containing SystemDB keys and
// lets that range gossip them.
func (s *Store) maybeGossipSystemConfig() error {
	rng := s.LookupReplica(roachpb.RKey(keys.SystemDBSpan.Key), nil)
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
		s.splitQueue.MaybeAdd(rng, s.ctx.Clock.Now())
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

// DisableReplicaGCQueue disables or enables the replica GC queue.
// Exposed only for testing.
func (s *Store) DisableReplicaGCQueue(disabled bool) {
	s.replicaGCQueue.SetDisabled(disabled)
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

// ForceReplicaGCScanAndProcess iterates over all ranges and enqueues any that
// may need to be GC'd. Exposed only for testing.
func (s *Store) ForceReplicaGCScanAndProcess(t util.Tester) {
	s.mu.Lock()

	for _, r := range s.replicas {
		s.replicaGCQueue.MaybeAdd(r, s.ctx.Clock.Now())
	}
	s.mu.Unlock()

	s.replicaGCQueue.DrainQueue(s.ctx.Clock)
}

// DisableRaftLogQueue disables or enables the raft log queue.
// Exposed only for testing.
func (s *Store) DisableRaftLogQueue(disabled bool) {
	s.raftLogQueue.SetDisabled(disabled)
}

// ForceRaftLogScanAndProcess iterates over all ranges and enqueues any that
// need their raft logs truncated and then process each of them.
// Exposed only for testing.
func (s *Store) ForceRaftLogScanAndProcess(t util.Tester) {
	// Add each range to the queue.
	s.mu.RLock()
	for _, r := range s.replicas {
		s.raftLogQueue.MaybeAdd(r, s.ctx.Clock.Now())
	}
	s.mu.RUnlock()

	s.raftLogQueue.DrainQueue(s.ctx.Clock)
}

// Bootstrap writes a new store ident to the underlying engine. To
// ensure that no crufty data already exists in the engine, it scans
// the engine contents before writing the new store ident. The engine
// should be completely empty. It returns an error if called on a
// non-empty engine.
func (s *Store) Bootstrap(ident roachpb.StoreIdent, stopper *stop.Stopper) error {
	if s.Ident.NodeID != 0 {
		return util.Errorf("engine already bootstrapped")
	}
	if err := s.engine.Open(); err != nil {
		return err
	}
	s.Ident = ident
	kvs, err := engine.Scan(s.engine,
		engine.MakeMVCCMetadataKey(roachpb.Key(roachpb.RKeyMin)),
		engine.MakeMVCCMetadataKey(roachpb.Key(roachpb.RKeyMax)), 1)
	if err != nil {
		return util.Errorf("store %s: unable to access: %s", s.engine, err)
	} else if len(kvs) > 0 {
		// See if this is an already-bootstrapped store.
		ok, err := engine.MVCCGetProto(s.engine, keys.StoreIdentKey(), roachpb.ZeroTimestamp, true, nil, &s.Ident)
		if err != nil {
			return util.Errorf("store %s is non-empty but cluster ID could not be determined: %s", s.engine, err)
		}
		if ok {
			return util.Errorf("store %s already belongs to cockroach cluster %s", s.engine, s.Ident.ClusterID)
		}
		return util.Errorf("store %s is not-empty and has invalid contents (first key: %q)", s.engine, kvs[0].Key)
	}
	err = engine.MVCCPutProto(s.engine, nil, keys.StoreIdentKey(), roachpb.ZeroTimestamp, nil, &s.Ident)
	return err
}

// GetReplica fetches a replica by Range ID. Returns an error if no replica is found.
func (s *Store) GetReplica(rangeID roachpb.RangeID) (*Replica, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getReplicaLocked(rangeID)
}

// getReplicaLocked fetches a replica by RangeID. The store's lock must be held
// in read or read/write mode.
func (s *Store) getReplicaLocked(rangeID roachpb.RangeID) (*Replica, error) {
	if rng, ok := s.replicas[rangeID]; ok {
		return rng, nil
	}
	return nil, roachpb.NewRangeNotFoundError(rangeID)
}

// LookupReplica looks up a replica via binary search over the
// "replicasByKey" btree. Returns nil if no replica is found for
// specified key range. Note that the specified keys are transformed
// using Key.Address() to ensure we lookup replicas correctly for local
// keys. When end is nil, a replica that contains start is looked up.
func (s *Store) LookupReplica(start, end roachpb.RKey) *Replica {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var rng *Replica
	s.replicasByKey.AscendGreaterOrEqual((rangeBTreeKey)(start.Next()), func(i btree.Item) bool {
		rng = i.(*Replica)
		return false
	})
	if rng == nil || !rng.Desc().ContainsKeyRange(start, end) {
		return nil
	}
	return rng
}

// hasOverlappingReplicaLocked returns true if a Replica overlapping the given
// descriptor is present on the Store.
func (s *Store) hasOverlappingReplicaLocked(rngDesc *roachpb.RangeDescriptor) bool {
	var rng *Replica
	s.replicasByKey.AscendGreaterOrEqual((rangeBTreeKey)(rngDesc.StartKey.Next()), func(i btree.Item) bool {
		rng = i.(*Replica)
		return false
	})

	if rng != nil && rng.Desc().StartKey.Less(rngDesc.EndKey) {
		return true
	}

	return false
}

// RaftStatus returns the current raft status of the local replica of
// the given range.
func (s *Store) RaftStatus(rangeID roachpb.RangeID) *raft.Status {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if r, ok := s.replicas[rangeID]; ok {
		return r.RaftStatus()
	}
	return nil
}

// BootstrapRange creates the first range in the cluster and manually
// writes it to the store. Default range addressing records are
// created for meta1 and meta2. Default configurations for
// zones are created. All configs are specified
// for the empty key prefix, meaning they apply to the entire
// database. The zone requires three replicas with no other specifications.
// It also adds the range tree and the root node, the first range, to it.
// The 'initialValues' are written as well after each value's checksum
// is initialized.
func (s *Store) BootstrapRange(initialValues []roachpb.KeyValue) error {
	desc := &roachpb.RangeDescriptor{
		RangeID:       1,
		StartKey:      roachpb.RKeyMin,
		EndKey:        roachpb.RKeyMax,
		NextReplicaID: 2,
		Replicas: []roachpb.ReplicaDescriptor{
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
	gcMeta := roachpb.NewGCMetadata(now.WallTime)
	if err := engine.MVCCPutProto(batch, ms, keys.RangeGCMetadataKey(desc.RangeID), roachpb.ZeroTimestamp, nil, gcMeta); err != nil {
		return err
	}
	// Verification timestamp.
	if err := engine.MVCCPutProto(batch, ms, keys.RangeLastVerificationTimestampKey(desc.RangeID), roachpb.ZeroTimestamp, nil, &now); err != nil {
		return err
	}
	// Range addressing for meta2.
	meta2Key := keys.RangeMetaKey(roachpb.RKeyMax)
	if err := engine.MVCCPutProto(batch, ms, meta2Key, now, nil, desc); err != nil {
		return err
	}
	// Range addressing for meta1.
	meta1Key := keys.RangeMetaKey(keys.Addr(meta2Key))
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
func (s *Store) StoreID() roachpb.StoreID { return s.Ident.StoreID }

// Clock accessor.
func (s *Store) Clock() *hlc.Clock { return s.ctx.Clock }

// Engine accessor.
func (s *Store) Engine() engine.Engine { return s.engine }

// DB accessor.
func (s *Store) DB() *client.DB { return s.ctx.DB }

// Gossip accessor.
func (s *Store) Gossip() *gossip.Gossip { return s.ctx.Gossip }

// Stopper accessor.
func (s *Store) Stopper() *stop.Stopper { return s.stopper }

// EventFeed accessor.
func (s *Store) EventFeed() StoreEventFeed { return s.feed }

// Tracer accessor.
func (s *Store) Tracer() *tracer.Tracer { return s.ctx.Tracer }

// NewRangeDescriptor creates a new descriptor based on start and end
// keys and the supplied roachpb.Replicas slice. It allocates new
// replica IDs to fill out the supplied replicas.
func (s *Store) NewRangeDescriptor(start, end roachpb.RKey, replicas []roachpb.ReplicaDescriptor) (*roachpb.RangeDescriptor, error) {
	id, err := s.rangeIDAlloc.Allocate()
	if err != nil {
		return nil, err
	}
	desc := &roachpb.RangeDescriptor{
		RangeID:       roachpb.RangeID(id),
		StartKey:      start,
		EndKey:        end,
		Replicas:      append([]roachpb.ReplicaDescriptor(nil), replicas...),
		NextReplicaID: roachpb.ReplicaID(len(replicas) + 1),
	}
	for i := range desc.Replicas {
		desc.Replicas[i].ReplicaID = roachpb.ReplicaID(i + 1)
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
	if _, ok := s.uninitReplicas[newDesc.RangeID]; ok {
		// If we have an uninitialized replica of the new range, delete it
		// to make way for the complete one created by the split. A live
		// uninitialized raft group cannot be converted to an
		// initialized one (because of the tricks we do to change
		// FirstIndex from 0 to raftInitialLogIndex), so the group must be
		// removed before we install the new range into s.replicas.
		delete(s.uninitReplicas, newDesc.RangeID)
		delete(s.replicas, newDesc.RangeID)
	}

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
	// This may not happen if the system config has not yet been loaded.
	// Since this is done under the store lock, system config update will
	// properly set these fields.
	if err := newRng.updateRangeInfo(); err != nil {
		return err
	}

	s.feed.splitRange(origRng, newRng)
	return s.processRangeDescriptorUpdateLocked(origRng)
}

// MergeRange expands the subsuming range to absorb the subsumed range.
// This merge operation will fail if the two ranges are not collocated
// on the same store. Must be called from the processRaft goroutine.
func (s *Store) MergeRange(subsumingRng *Replica, updatedEndKey roachpb.RKey, subsumedRangeID roachpb.RangeID) error {
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
	if err := s.removeReplicaImpl(subsumedRng, *subsumedDesc); err != nil {
		return util.Errorf("cannot remove range %s", err)
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

	if s.hasOverlappingReplicaLocked(rng.Desc()) {
		return rangeAlreadyExists{rng}
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
		return rangeAlreadyExists{exRng}
	}
	s.replicas[rangeID] = rng
	return nil
}

type removeReplicaOp struct {
	rep      *Replica
	origDesc roachpb.RangeDescriptor
	ch       chan<- error
}

// RemoveReplica removes the replica from the store's replica map and from
// the sorted replicasByKey btree. The version of the replica descriptor that
// was used to make the removal decision is passed in, and the removal is
// aborted if the replica ID has changed since then.
func (s *Store) RemoveReplica(rep *Replica, origDesc roachpb.RangeDescriptor) error {

	ch := make(chan error)
	s.removeReplicaChan <- removeReplicaOp{rep, origDesc, ch}
	return <-ch
}

// removeReplicaImpl runs on the processRaft goroutine.
func (s *Store) removeReplicaImpl(rep *Replica, origDesc roachpb.RangeDescriptor) error {
	desc := rep.Desc()
	rangeID := desc.RangeID
	_, rd := desc.FindReplica(s.StoreID())
	if rd != nil && rd.ReplicaID >= origDesc.NextReplicaID {
		return util.Errorf("cannot remove replica %s; replica ID has changed (%s >= %s)",
			rep, rd.ReplicaID, origDesc.NextReplicaID)
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
		return rangeAlreadyExists{rng}
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
func (s *Store) Attrs() roachpb.Attributes {
	return s.engine.Attrs()
}

// Capacity returns the capacity of the underlying storage engine.
func (s *Store) Capacity() (roachpb.StoreCapacity, error) {
	return s.engine.Capacity()
}

// Descriptor returns a StoreDescriptor including current store
// capacity information.
func (s *Store) Descriptor() (*roachpb.StoreDescriptor, error) {
	capacity, err := s.Capacity()
	if err != nil {
		return nil, err
	}
	capacity.RangeCount = int32(s.ReplicaCount())
	// Initialize the store descriptor.
	return &roachpb.StoreDescriptor{
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

// Send fetches a range based on the header's replica, assembles
// method, args & reply into a Raft Cmd struct and executes the
// command using the fetched range.
func (s *Store) Send(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	ctx = s.Context(ctx)
	trace := tracer.FromCtx(ctx)

	for _, union := range ba.Requests {
		arg := union.GetInner()
		header := arg.Header()
		if err := verifyKeys(header.Key, header.EndKey, roachpb.IsRange(arg)); err != nil {
			return nil, roachpb.NewError(err)
		}
	}
	if !ba.Timestamp.Equal(roachpb.ZeroTimestamp) {
		if s.Clock().MaxOffset() > 0 {
			// Once a command is submitted to raft, all replicas' logical
			// clocks will be ratcheted forward to match. If the command
			// appears to come from a node with a bad clock, reject it now
			// before we reach that point.
			offset := time.Duration(ba.Timestamp.WallTime - s.Clock().PhysicalNow())
			if offset > s.Clock().MaxOffset() {
				return nil, roachpb.NewError(util.Errorf("Rejecting command with timestamp in the future: %d (%s ahead)",
					ba.Timestamp.WallTime, offset))
			}
		}
		// Update our clock with the incoming request timestamp. This
		// advances the local node's clock to a high water mark from
		// amongst all nodes with which it has interacted.
		// TODO(tschottdorf): see executeBatch for an explanation of the weird
		// logical ticks added here.
		ba.Timestamp.Add(0, int32(len(ba.Requests))-1)
	} else if ba.Txn == nil {
		// TODO(tschottdorf): possibly consolidate this with other locations
		// doing the same (but it's definitely required here).
		// TODO(tschottdorf): see executeBatch for an explanation of the weird
		// logical ticks added here.
		ba.Timestamp.Forward(s.Clock().Now().Add(0, int32(len(ba.Requests))-1))
	}
	s.ctx.Clock.Update(ba.Timestamp)

	defer trace.Epoch(fmt.Sprintf("executing %d requests", len(ba.Requests)))()
	// Backoff and retry loop for handling errors. Backoff times are measured
	// in the Trace.
	// Increase the sequence counter to avoid getting caught in replay
	// protection on retry.
	next := func(r *retry.Retry) bool {
		if r.CurrentAttempt() > 0 {
			ba.SetNewRequest()
			defer trace.Epoch("backoff")()
		}
		return r.Next()
	}
	var rng *Replica
	var err error

	// Add the command to the range for execution; exit retry loop on success.
	for r := retry.Start(s.ctx.RangeRetryOptions); next(&r); {
		// Get range and add command to the range for execution.
		rng, err = s.GetReplica(ba.RangeID)
		if err != nil {
			return nil, roachpb.NewError(err)
		}

		var br *roachpb.BatchResponse
		{
			var pErr *roachpb.Error
			br, pErr = rng.Send(ctx, ba)
			err = pErr.GoError()
		}

		if err == nil {
			return br, nil
		}

		// Maybe resolve a potential write intent error. We do this here
		// because this is the code path with the requesting client
		// waiting. We don't want every replica to attempt to resolve the
		// intent independently, so we can't do it there.
		if wiErr, ok := err.(*roachpb.WriteIntentError); ok {
			var pushType roachpb.PushTxnType
			if ba.IsWrite() {
				pushType = roachpb.PUSH_ABORT
			} else {
				pushType = roachpb.PUSH_TIMESTAMP
			}

			index, ok := wiErr.ErrorIndex()
			if ok {
				args := ba.Requests[index].GetInner()
				// TODO(tschottdorf): implications of using the batch ts here?
				var resolveIntents []roachpb.Intent
				resolveIntents, err = s.resolveWriteIntentError(ctx, wiErr, rng, args, ba.Header, pushType)
				if len(resolveIntents) > 0 {
					if resErr := rng.resolveIntents(ctx, resolveIntents, false /* !wait */, true /* poison */); resErr != nil {
						// When resolving asynchronously, should never get an error
						// back here.
						panic(resErr)
					}
				}
				// Make sure that if an index is carried in the error, it
				// remains the one corresponding to the batch here.
				if iErr, ok := err.(roachpb.IndexedError); ok {
					if _, ok := iErr.ErrorIndex(); ok {
						iErr.SetErrorIndex(index)
					}
				}
			}
		}

		switch t := err.(type) {
		case *roachpb.ReadWithinUncertaintyIntervalError:
			t.NodeID = ba.Replica.NodeID
		case *roachpb.WriteTooOldError:
			trace.Event(fmt.Sprintf("error: %T", err))
			// Update request timestamp and retry immediately.
			ba.Timestamp = t.ExistingTimestamp.Next()
			r.Reset()
			if log.V(1) {
				log.Warning(err)
			}
			continue
		case *roachpb.WriteIntentError:
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
				if ba.IsWrite() && ba.Timestamp.Less(intent.Txn.Timestamp) {
					ba.Timestamp = intent.Txn.Timestamp.Next()
				}
			}
			if log.V(1) {
				log.Warning(err)
			}
			continue
		}
		return nil, roachpb.NewError(err)
	}

	// By default, retries are indefinite. However, some unittests set a
	// maximum retry count; return txn retry error for transactional cases
	// and the original error otherwise.
	trace.Event("store retry limit exceeded") // good to check for if tests fail
	if ba.Txn != nil {
		return nil, roachpb.NewError(roachpb.NewTransactionRetryError(ba.Txn))
	}
	return nil, roachpb.NewError(err)
}

// resolveWriteIntentError tries to push the conflicting transaction (if
// necessary, i.e. if the transaction is pending): either move its timestamp
// forward on a read/write conflict, or abort it on a write/write conflict. If
// the push succeeds (or if it wasn't necessary), the error's Resolved flag is
// set to to true and the caller should call resolveIntents and retry its
// command immediately. If the push fails, the error's Resolved flag is set to
// false so that the client backs off before reissuing the command. On
// write/write conflicts, a potential push error is returned; otherwise the
// updated WriteIntentError is returned.
//
// Callers are involved with
// a) conflict resolution for commands being executed at the Store with the
//    client waiting,
// b) resolving intents encountered during inconsistent operations, and
// c) resolving intents upon EndTransaction which are not local to the given
//    range. This is the only path in which the transaction is going to be
//    in non-pending state and doesn't require a push.
func (s *Store) resolveWriteIntentError(ctx context.Context, wiErr *roachpb.WriteIntentError, rng *Replica, args roachpb.Request, h roachpb.Header, pushType roachpb.PushTxnType) ([]roachpb.Intent, error) {
	method := args.Method()
	pusherTxn := h.Txn
	readOnly := roachpb.IsReadOnly(args) // TODO(tschottdorf): pass as param
	args = nil

	if log.V(6) {
		log.Infoc(ctx, "resolving write intent %s", wiErr)
	}
	trace := tracer.FromCtx(ctx)
	defer trace.Epoch("intent resolution")()

	// Split intents into those we need to push and those which are good to
	// resolve.
	// TODO(tschottdorf): can optimize this and use same underlying slice.
	var pushIntents, resolveIntents []roachpb.Intent
	for _, intent := range wiErr.Intents {
		// The current intent does not need conflict resolution.
		if intent.Txn.Status != roachpb.PENDING {
			resolveIntents = append(resolveIntents, intent)
		} else {
			pushIntents = append(pushIntents, intent)
		}
	}

	// Attempt to push the transaction(s) which created the conflicting intent(s).
	now := s.Clock().Now()

	// TODO(tschottdorf): need deduplication here (many pushes for the same
	// txn are awkward but even worse, could ratchet up the priority).
	// If there's no pusher, we communicate a priority by sending an empty
	// txn with only the priority set.
	if pusherTxn == nil {
		pusherTxn = &roachpb.Transaction{
			Priority: roachpb.MakePriority(h.GetUserPriority()),
		}
	}
	var pushReqs []roachpb.Request
	for _, intent := range pushIntents {
		pushReqs = append(pushReqs, &roachpb.PushTxnRequest{
			Span: roachpb.Span{
				Key: intent.Txn.Key,
			},
			PusherTxn: *pusherTxn,
			PusheeTxn: intent.Txn,
			PushTo:    h.Timestamp,
			// The timestamp is used by PushTxn for figuring out whether the
			// transaction is abandoned. If we used the argument's timestamp
			// here, we would run into busy loops because that timestamp
			// usually stays fixed among retries, so it will never realize
			// that a transaction has timed out. See #877.
			Now:      now,
			PushType: pushType,
		})
	}
	b := &client.Batch{}
	b.InternalAddRequest(pushReqs...)
	br, pushErr := s.db.RunWithResponse(b)
	if pushErr != nil {
		if log.V(1) {
			log.Infoc(ctx, "on %s: %s", method, pushErr)
		}

		// For write/write conflicts within a transaction, propagate the
		// push failure, not the original write intent error. The push
		// failure will instruct the client to restart the transaction
		// with a backoff.
		if len(pusherTxn.ID) > 0 && !readOnly {
			return nil, pushErr
		}
		// For read/write conflicts, return the write intent error which
		// engages backoff/retry (with !Resolved). We don't need to
		// restart the txn, only resend the read with a backoff.
		return nil, wiErr
	}
	wiErr.Resolved = true // success!

	for i, intent := range pushIntents {
		intent.Txn = br.Responses[i].GetInner().(*roachpb.PushTxnResponse).PusheeTxn
		resolveIntents = append(resolveIntents, intent)
	}
	return resolveIntents, wiErr
}

// TODO(bdarnell): is this buffering necessary? sufficient?
func (s *Store) enqueueRaftMessage(req *RaftMessageRequest) error {
	s.raftRequestChan <- req
	return nil
}

func (s *Store) handleRaftMessage(req *RaftMessageRequest) error {
	switch req.Message.Type {
	case raftpb.MsgSnap:
		s.mu.Lock()
		canApply := s.canApplySnapshot(req.GroupID, req.Message.Snapshot)
		s.mu.Unlock()
		if !canApply {
			// If the storage cannot accept the snapshot, drop it before
			// passing it to RawNode.Step, since our error handling
			// options past that point are limited.
			return nil
		}

	// TODO(bdarnell): handle coalesced heartbeats
	case raftpb.MsgHeartbeat:
		// A subset of coalesced heartbeats: drop heartbeats (but not
		// other messages!) that come from a node that we don't believe to
		// be a current member of the group.
		s.mu.Lock()
		r, ok := s.replicas[req.GroupID]
		s.mu.Unlock()
		if ok {
			found := false
			for _, rep := range r.Desc().Replicas {
				if rep.ReplicaID == req.FromReplica.ReplicaID {
					found = true
					break
				}
			}
			if !found && req.FromReplica.ReplicaID < r.Desc().NextReplicaID {
				return nil
			}
		}
	}

	s.mu.Lock()
	s.cacheReplicaDescriptorLocked(req.GroupID, req.FromReplica)
	s.cacheReplicaDescriptorLocked(req.GroupID, req.ToReplica)
	// Lazily create the group.
	r, err := s.getOrCreateReplicaLocked(req.GroupID, req.ToReplica.ReplicaID)
	// TODO(bdarnell): is it safe to release the store lock here?
	// It deadlocks to hold s.mu while calling raftGroup.Step.
	s.mu.Unlock()
	if err != nil {
		return err
	}
	r.Lock()
	err = r.raftGroup.Step(req.Message)
	r.Unlock()
	if err != nil {
		return err
	}
	s.enqueueRaftUpdateCheck(req.GroupID)
	return nil
}

// enqueueRaftUpdateCheck asynchronously registers the given range ID to be
// checked for raft updates when the processRaft goroutine is idle.
// TODO(bdarnell): reconsider the goroutine relationships here.
func (s *Store) enqueueRaftUpdateCheck(rangeID roachpb.RangeID) {
	// checkRaftGroup may be called with Replica.RWMutex held. We cannot
	// simply acquire s.mu; this violates lock ordering and may
	// deadlock.
	go func() {
		s.mu.Lock()
		s.pendingRaftGroups[rangeID] = struct{}{}
		s.mu.Unlock()
		select {
		case s.wakeRaftLoop <- struct{}{}:
		default:
		}
	}()
}

// processRaft processes write commands that have been committed
// by the raft consensus algorithm, dispatching them to the
// appropriate range. This method starts a goroutine to process Raft
// commands indefinitely or until the stopper signals.
func (s *Store) processRaft() {
	s.stopper.RunWorker(func() {
		defer s.ctx.Transport.Stop(s.StoreID())
		ticker := time.NewTicker(s.ctx.RaftTickInterval)
		defer ticker.Stop()
		for {
			var replicas []*Replica
			s.mu.Lock()
			for rangeID := range s.pendingRaftGroups {
				r, ok := s.replicas[rangeID]
				if !ok {
					continue
				}
				replicas = append(replicas, r)

			}
			if len(s.pendingRaftGroups) > 0 {
				s.pendingRaftGroups = map[roachpb.RangeID]struct{}{}
			}
			s.mu.Unlock()
			for _, r := range replicas {
				if err := r.handleRaftReady(); err != nil {
					panic(err) // TODO(bdarnell)
				}
			}

			select {
			case <-s.wakeRaftLoop:

			case op := <-s.removeReplicaChan:
				op.ch <- s.removeReplicaImpl(op.rep, op.origDesc)

			case req := <-s.raftRequestChan:
				if err := s.handleRaftMessage(req); err != nil {
					log.Errorf("error handling raft message: %s", err)
				}

			case <-ticker.C:
				// TODO(bdarnell): rework raft ticker.
				s.mu.Lock()
				for rangeID, r := range s.replicas {
					r.Lock()
					r.raftGroup.Tick()
					r.Unlock()
					s.pendingRaftGroups[rangeID] = struct{}{}
				}
				s.mu.Unlock()

			case <-s.stopper.ShouldStop():
				return
			}
		}
	})
}

// getOrCreateReplicaLocked returns a replica for the given RangeID,
// creating an uninitialized replica if necessary. The caller must
// hold the store's lock.
func (s *Store) getOrCreateReplicaLocked(groupID roachpb.RangeID, replicaID roachpb.ReplicaID) (*Replica, error) {
	r, ok := s.replicas[groupID]
	if ok {
		if err := r.setReplicaID(replicaID); err != nil {
			return nil, err
		}
		return r, nil
	}

	// Before creating the group, see if there is a tombstone which
	// would indicate that this is a stale message.
	tombstoneKey := keys.RaftTombstoneKey(groupID)
	var tombstone roachpb.RaftTombstone
	if ok, err := engine.MVCCGetProto(s.Engine(), tombstoneKey, roachpb.ZeroTimestamp, true, nil, &tombstone); err != nil {
		return nil, err
	} else if ok {
		if replicaID != 0 && replicaID < tombstone.NextReplicaID {
			return nil, errRaftGroupDeleted
		}
	}

	var err error
	r, err = NewReplica(&roachpb.RangeDescriptor{
		RangeID: groupID,
		// TODO(bdarnell): other fields are unknown; need to populate them from
		// snapshot.
	}, s)
	if err != nil {
		return nil, err
	}
	if err := r.setReplicaID(replicaID); err != nil {
		return nil, err
	}
	// Add the range to range map, but not rangesByKey since
	// the range's start key is unknown. The range will be
	// added to rangesByKey later when a snapshot is applied.
	if err = s.addReplicaToRangeMap(r); err != nil {
		return nil, err
	}
	s.uninitReplicas[r.Desc().RangeID] = r

	return r, nil
}

// ReplicaDescriptor returns the replica descriptor for the given
// range and replica, if known.
func (s *Store) ReplicaDescriptor(groupID roachpb.RangeID, replicaID roachpb.ReplicaID) (roachpb.ReplicaDescriptor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.replicaDescriptorLocked(groupID, replicaID)
}

// replicaDescriptorLocked returns the replica descriptor for the given
// range and replica, if known.
func (s *Store) replicaDescriptorLocked(groupID roachpb.RangeID, replicaID roachpb.ReplicaID) (roachpb.ReplicaDescriptor, error) {
	if rep, ok := s.replicaDescCache.Get(replicaDescCacheKey{groupID, replicaID}); ok {
		return rep.(roachpb.ReplicaDescriptor), nil
	}
	rep, err := s.getReplicaLocked(groupID)
	if err != nil {
		return roachpb.ReplicaDescriptor{}, err
	}
	rd, err := rep.ReplicaDescriptor(replicaID)
	if err != nil {
		return roachpb.ReplicaDescriptor{}, err
	}
	s.cacheReplicaDescriptorLocked(groupID, rd)

	return rd, nil
}

// cacheReplicaDescriptorLocked adds the given replica descriptor to a cache
// to be used by replicaDescriptorLocked.
func (s *Store) cacheReplicaDescriptorLocked(groupID roachpb.RangeID, replica roachpb.ReplicaDescriptor) {
	if old, ok := s.replicaDescCache.Get(replicaDescCacheKey{groupID, replica.ReplicaID}); ok {
		if old != replica {
			log.Fatalf("%s replicaDescCache: clobbering %s with %s", s, old, replica)
		}
		return
	}
	s.replicaDescCache.Add(replicaDescCacheKey{groupID, replica.ReplicaID}, replica)
}

// canApplySnapshot returns true if the snapshot can be applied to
// this store's replica (i.e. it is not from an older incarnation of
// the replica). The caller must hold the store's lock.
func (s *Store) canApplySnapshot(rangeID roachpb.RangeID, snap raftpb.Snapshot) bool {
	if r, ok := s.replicas[rangeID]; ok && r.isInitialized() {
		// We have the range and it's initialized, so let the snapshot
		// through.
		return true
	}

	// We don't have the range (or we have an uninitialized
	// placeholder). Will we be able to create/initialize it?
	// TODO(bdarnell): can we avoid parsing this twice?
	var parsedSnap roachpb.RaftSnapshotData
	if err := parsedSnap.Unmarshal(snap.Data); err != nil {
		return false
	}

	if s.hasOverlappingReplicaLocked(&parsedSnap.RangeDescriptor) {
		// We have a conflicting range, so we must block the snapshot.
		// When such a conflict exists, it will be resolved by one range
		// either being split or garbage collected.
		return false
	}

	return true
}

func raftEntryFormatter(data []byte) string {
	if len(data) == 0 {
		return "[empty]"
	}
	_, encodedCmd := decodeRaftCommand(data)
	var cmd roachpb.RaftCommand
	if err := proto.Unmarshal(encodedCmd, &cmd); err != nil {
		return fmt.Sprintf("[error parsing entry: %s]", err)
	}
	s := cmd.Cmd.String()
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
	leaderRangeCount, replicatedRangeCount, availableRangeCount int64) {
	// Load the system config.
	cfg := s.Gossip().GetSystemConfig()
	if cfg == nil {
		log.Infof("system config not yet available")
		return
	}

	timestamp := roachpb.Timestamp{WallTime: now}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rng := range s.replicas {
		zoneConfig, err := cfg.GetZoneConfigForKey(rng.Desc().StartKey)
		if err != nil {
			log.Error(err)
			continue
		}
		raftStatus := rng.RaftStatus()
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
