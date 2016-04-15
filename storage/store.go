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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gogo/protobuf/proto"
	"github.com/google/btree"
	"github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils/storageutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/cache"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/tracing"
	"github.com/cockroachdb/cockroach/util/uuid"
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
	// sent to the store's ranges, for WriteIntent errors.
	defaultRangeRetryOptions = retry.Options{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		Multiplier:     2,
	}
)

var changeTypeInternalToRaft = map[roachpb.ReplicaChangeType]raftpb.ConfChangeType{
	roachpb.ADD_REPLICA:    raftpb.ConfChangeAddNode,
	roachpb.REMOVE_REPLICA: raftpb.ConfChangeRemoveNode,
}

// TestStoreContext has some fields initialized with values relevant
// in tests.
func TestStoreContext() StoreContext {
	return StoreContext{
		Tracer:                         tracing.NewTracer(),
		RaftTickInterval:               100 * time.Millisecond,
		RaftHeartbeatIntervalTicks:     1,
		RaftElectionTimeoutTicks:       3,
		ScanInterval:                   10 * time.Minute,
		ConsistencyCheckInterval:       10 * time.Minute,
		ConsistencyCheckPanicOnFailure: true,
	}
}

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
		sAddr, err := keys.Addr(start)
		if err != nil {
			return err
		}
		eAddr, err := keys.Addr(end)
		if err != nil {
			return err
		}
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
	return fmt.Sprintf("range for Range ID %d already exists on store", e.rng.RangeID)
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
	rs.store.mu.Lock()
	rs.rangeIDs = make([]roachpb.RangeID, rs.store.mu.replicasByKey.Len())
	i := 0
	rs.store.mu.replicasByKey.Ascend(func(item btree.Item) bool {
		rs.rangeIDs[i] = item.(*Replica).RangeID
		i++
		return true
	})
	rs.store.mu.Unlock()

	rs.visited = 0
	for _, rangeID := range rs.rangeIDs {
		rs.visited++
		rs.store.mu.Lock()
		rng, ok := rs.store.mu.replicas[rangeID]
		rs.store.mu.Unlock()
		if ok {
			if !visitor(rng) {
				break
			}
		}
	}
	rs.visited = 0
}

func (rs *storeRangeSet) EstimatedCount() int {
	rs.store.mu.Lock()
	defer rs.store.mu.Unlock()
	if rs.visited <= 0 {
		return rs.store.mu.replicasByKey.Len()
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
	Ident                   roachpb.StoreIdent
	ctx                     StoreContext
	db                      *client.DB
	engine                  engine.Engine            // The underlying key-value store
	allocator               Allocator                // Makes allocation decisions
	rangeIDAlloc            *idAllocator             // Range ID allocator
	gcQueue                 *gcQueue                 // Garbage collection queue
	splitQueue              *splitQueue              // Range splitting queue
	verifyQueue             *verifyQueue             // Checksum verification queue
	replicateQueue          *replicateQueue          // Replication queue
	replicaGCQueue          *replicaGCQueue          // Replica GC queue
	raftLogQueue            *raftLogQueue            // Raft Log Truncation queue
	scanner                 *replicaScanner          // Replica scanner
	replicaConsistencyQueue *replicaConsistencyQueue // Replica consistency check queue
	consistencyScanner      *replicaScanner          // Consistency checker scanner
	metrics                 *storeMetrics
	intentResolver          *intentResolver
	wakeRaftLoop            chan struct{}
	started                 int32
	stopper                 *stop.Stopper
	startedAt               int64
	nodeDesc                *roachpb.NodeDescriptor
	initComplete            sync.WaitGroup // Signaled by async init tasks
	raftRequestChan         chan *RaftMessageRequest

	// Locking notes: To avoid deadlocks, the following lock order
	// must be obeyed: processRaftMu < Store.mu.Mutex <
	// Replica.mu.Mutex < Store.pendingRaftGroups.Mutex.
	//
	// Methods of Store with a "Locked" suffix require that
	// Store.mu.Mutex be held. Other locking requirements are indicated
	// in comments.

	// processRaftMu is held while doing anything raft-related.
	// TODO(bdarnell): replace processRaftMu with a range-level lock.
	processRaftMu sync.Mutex
	mu            struct {
		sync.Mutex                                  // Protects all variables in the mu struct.
		replicas       map[roachpb.RangeID]*Replica // Map of replicas by Range ID
		replicasByKey  *btree.BTree                 // btree keyed by ranges end keys.
		uninitReplicas map[roachpb.RangeID]*Replica // Map of uninitialized replicas by Range ID

		replicaDescCache *cache.UnorderedCache
	}

	// pendingRaftGroups contains the ranges that should be checked for
	// updates. After updating this map, write to wakeRaftLoop to
	// trigger the check.
	pendingRaftGroups struct {
		sync.Mutex
		value map[roachpb.RangeID]struct{}
	}
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
	Transport *RaftTransport

	// SQLExecutor is used by the store to execute SQL statements in a way that
	// is more direct than using a sql.Executor.
	SQLExecutor sql.InternalExecutor

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

	// ConsistencyCheckInterval is the default time period in between consecutive
	// consistency checks on a range.
	ConsistencyCheckInterval time.Duration

	// ConsistencyCheckPanicOnFailure causes the node to panic when it detects a
	// replication consistency check failure.
	ConsistencyCheckPanicOnFailure bool

	// AllocatorOptions configures how the store will attempt to rebalance its
	// replicas to other stores.
	AllocatorOptions AllocatorOptions

	// Tracer is a request tracer.
	Tracer opentracing.Tracer

	// If LogRangeEvents is true, major changes to ranges will be logged into
	// the range event log.
	LogRangeEvents bool

	TestingKnobs StoreTestingKnobs
}

// StoreTestingKnobs is a part of the context used to control parts of the system.
type StoreTestingKnobs struct {
	// A callback to be called when executing every replica command.
	// If your filter is not idempotent, consider wrapping it in a
	// ReplayProtectionFilterWrapper.
	TestingCommandFilter storageutils.ReplicaCommandFilter
	// A callback to be called instead of panicking due to a
	// checksum mismatch in VerifyChecksum()
	BadChecksumPanic func([]ReplicaSnapshotDiff)
	// Disables the use of one phase commits.
	DisableOnePhaseCommits bool
}

type storeMetrics struct {
	registry *metric.Registry

	// Range data metrics.
	replicaCount         *metric.Counter
	leaderRangeCount     *metric.Gauge
	replicatedRangeCount *metric.Gauge
	availableRangeCount  *metric.Gauge

	// Storage metrics.
	liveBytes       *metric.Gauge
	keyBytes        *metric.Gauge
	valBytes        *metric.Gauge
	intentBytes     *metric.Gauge
	liveCount       *metric.Gauge
	keyCount        *metric.Gauge
	valCount        *metric.Gauge
	intentCount     *metric.Gauge
	intentAge       *metric.Gauge
	gcBytesAge      *metric.Gauge
	lastUpdateNanos *metric.Gauge
	capacity        *metric.Gauge
	available       *metric.Gauge
	sysBytes        *metric.Gauge
	sysCount        *metric.Gauge

	// RocksDB metrics.
	rdbBlockCacheHits           *metric.Gauge
	rdbBlockCacheMisses         *metric.Gauge
	rdbBlockCacheUsage          *metric.Gauge
	rdbBlockCachePinnedUsage    *metric.Gauge
	rdbBloomFilterPrefixChecked *metric.Gauge
	rdbBloomFilterPrefixUseful  *metric.Gauge
	rdbMemtableHits             *metric.Gauge
	rdbMemtableMisses           *metric.Gauge
	rdbMemtableTotalSize        *metric.Gauge
	rdbFlushes                  *metric.Gauge
	rdbCompactions              *metric.Gauge
	rdbTableReadersMemEstimate  *metric.Gauge

	// Range event metrics.
	rangeSplits  *metric.Counter
	rangeAdds    *metric.Counter
	rangeRemoves *metric.Counter

	// Stats for efficient merges.
	// TODO(mrtracy): This should be removed as part of #4465. This is only
	// maintained to keep the current structure of StatusSummaries; it would be
	// better to convert the Gauges above into counters which are adjusted
	// accordingly.
	mu    sync.Mutex
	stats engine.MVCCStats
}

func newStoreMetrics() *storeMetrics {
	storeRegistry := metric.NewRegistry()
	return &storeMetrics{
		registry:             storeRegistry,
		replicaCount:         storeRegistry.Counter("replicas"),
		leaderRangeCount:     storeRegistry.Gauge("ranges.leader"),
		replicatedRangeCount: storeRegistry.Gauge("ranges.replicated"),
		availableRangeCount:  storeRegistry.Gauge("ranges.available"),
		liveBytes:            storeRegistry.Gauge("livebytes"),
		keyBytes:             storeRegistry.Gauge("keybytes"),
		valBytes:             storeRegistry.Gauge("valbytes"),
		intentBytes:          storeRegistry.Gauge("intentbytes"),
		liveCount:            storeRegistry.Gauge("livecount"),
		keyCount:             storeRegistry.Gauge("keycount"),
		valCount:             storeRegistry.Gauge("valcount"),
		intentCount:          storeRegistry.Gauge("intentcount"),
		intentAge:            storeRegistry.Gauge("intentage"),
		gcBytesAge:           storeRegistry.Gauge("gcbytesage"),
		lastUpdateNanos:      storeRegistry.Gauge("lastupdatenanos"),
		capacity:             storeRegistry.Gauge("capacity"),
		available:            storeRegistry.Gauge("capacity.available"),
		sysBytes:             storeRegistry.Gauge("sysbytes"),
		sysCount:             storeRegistry.Gauge("syscount"),

		// RocksDB metrics.
		rdbBlockCacheHits:           storeRegistry.Gauge("rocksdb.block.cache.hits"),
		rdbBlockCacheMisses:         storeRegistry.Gauge("rocksdb.block.cache.misses"),
		rdbBlockCacheUsage:          storeRegistry.Gauge("rocksdb.block.cache.usage"),
		rdbBlockCachePinnedUsage:    storeRegistry.Gauge("rocksdb.block.cache.pinned-usage"),
		rdbBloomFilterPrefixChecked: storeRegistry.Gauge("rocksdb.bloom.filter.prefix.checked"),
		rdbBloomFilterPrefixUseful:  storeRegistry.Gauge("rocksdb.bloom.filter.prefix.useful"),
		rdbMemtableHits:             storeRegistry.Gauge("rocksdb.memtable.hits"),
		rdbMemtableMisses:           storeRegistry.Gauge("rocksdb.memtable.misses"),
		rdbMemtableTotalSize:        storeRegistry.Gauge("rocksdb.memtable.total-size"),
		rdbFlushes:                  storeRegistry.Gauge("rocksdb.flushes"),
		rdbCompactions:              storeRegistry.Gauge("rocksdb.compactions"),
		rdbTableReadersMemEstimate:  storeRegistry.Gauge("rocksdb.table-readers-mem-estimate"),

		// Range event metrics.
		rangeSplits:  storeRegistry.Counter("range.splits"),
		rangeAdds:    storeRegistry.Counter("range.adds"),
		rangeRemoves: storeRegistry.Counter("range.removes"),
	}
}

// updateGaugesLocked breaks out individual metrics from the MVCCStats object.
// This process should be locked with each stat application to ensure that all
// gauges increase/decrease in step with the application of updates. However,
// this locking is not exposed to the registry level, and therefore a single
// snapshot of these gauges in the registry might mix the values of two
// subsequent updates.
func (sm *storeMetrics) updateMVCCGaugesLocked() {
	sm.liveBytes.Update(sm.stats.LiveBytes)
	sm.keyBytes.Update(sm.stats.KeyBytes)
	sm.valBytes.Update(sm.stats.ValBytes)
	sm.intentBytes.Update(sm.stats.IntentBytes)
	sm.liveCount.Update(sm.stats.LiveCount)
	sm.keyCount.Update(sm.stats.KeyCount)
	sm.valCount.Update(sm.stats.ValCount)
	sm.intentCount.Update(sm.stats.IntentCount)
	sm.intentAge.Update(sm.stats.IntentAge)
	sm.gcBytesAge.Update(sm.stats.GCBytesAge)
	sm.lastUpdateNanos.Update(sm.stats.LastUpdateNanos)
	sm.sysBytes.Update(sm.stats.SysBytes)
	sm.sysCount.Update(sm.stats.SysCount)
}

func (sm *storeMetrics) updateCapacityGauges(capacity roachpb.StoreCapacity) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.capacity.Update(capacity.Capacity)
	sm.available.Update(capacity.Available)
}

func (sm *storeMetrics) updateReplicationGauges(leaders, replicated, available int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.leaderRangeCount.Update(leaders)
	sm.replicatedRangeCount.Update(replicated)
	sm.availableRangeCount.Update(available)
}

func (sm *storeMetrics) addMVCCStats(stats engine.MVCCStats) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.stats.Add(stats)
	sm.updateMVCCGaugesLocked()
}

func (sm *storeMetrics) subtractMVCCStats(stats engine.MVCCStats) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.stats.Subtract(stats)
	sm.updateMVCCGaugesLocked()
}

func (sm *storeMetrics) updateRocksDBStats(stats engine.Stats) {
	// We do not grab a lock here, because it's not possible to get a point-in-
	// time snapshot of RocksDB stats. Retrieving RocksDB stats doesn't grab any
	// locks, and there's no way to retrieve multiple stats in a single operation.
	sm.rdbBlockCacheHits.Update(int64(stats.BlockCacheHits))
	sm.rdbBlockCacheMisses.Update(int64(stats.BlockCacheMisses))
	sm.rdbBlockCacheUsage.Update(int64(stats.BlockCacheUsage))
	sm.rdbBlockCachePinnedUsage.Update(int64(stats.BlockCachePinnedUsage))
	sm.rdbBloomFilterPrefixUseful.Update(int64(stats.BloomFilterPrefixUseful))
	sm.rdbBloomFilterPrefixChecked.Update(int64(stats.BloomFilterPrefixChecked))
	sm.rdbMemtableHits.Update(int64(stats.MemtableHits))
	sm.rdbMemtableMisses.Update(int64(stats.MemtableMisses))
	sm.rdbMemtableTotalSize.Update(int64(stats.MemtableTotalSize))
	sm.rdbFlushes.Update(int64(stats.Flushes))
	sm.rdbCompactions.Update(int64(stats.Compactions))
	sm.rdbTableReadersMemEstimate.Update(int64(stats.TableReadersMemEstimate))
}

// Valid returns true if the StoreContext is populated correctly.
// We don't check for Gossip and DB since some of our tests pass
// that as nil.
func (sc *StoreContext) Valid() bool {
	return sc.Clock != nil && sc.Transport != nil &&
		sc.RaftTickInterval != 0 && sc.RaftHeartbeatIntervalTicks > 0 &&
		sc.RaftElectionTimeoutTicks > 0 && sc.ScanInterval > 0 &&
		sc.ConsistencyCheckInterval > 0 && sc.Tracer != nil
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
		ctx:             ctx,
		db:              ctx.DB, // TODO(tschottdorf) remove redundancy.
		engine:          eng,
		allocator:       MakeAllocator(ctx.StorePool, ctx.AllocatorOptions),
		nodeDesc:        nodeDesc,
		wakeRaftLoop:    make(chan struct{}, 1),
		raftRequestChan: make(chan *RaftMessageRequest, raftReqBufferSize),
		metrics:         newStoreMetrics(),
	}
	s.intentResolver = newIntentResolver(s)

	s.mu.Lock()
	s.mu.replicas = map[roachpb.RangeID]*Replica{}
	s.mu.replicasByKey = btree.New(64 /* degree */)
	s.mu.uninitReplicas = map[roachpb.RangeID]*Replica{}
	s.mu.replicaDescCache = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return size > maxReplicaDescCacheSize
		},
	})
	s.pendingRaftGroups.value = map[roachpb.RangeID]struct{}{}

	s.mu.Unlock()

	// Add range scanner and configure with queues.
	s.scanner = newReplicaScanner(ctx.ScanInterval, ctx.ScanMaxIdleTime, newStoreRangeSet(s))
	s.gcQueue = newGCQueue(s.ctx.Gossip)
	s.splitQueue = newSplitQueue(s.db, s.ctx.Gossip)
	s.verifyQueue = newVerifyQueue(s.ctx.Gossip, s.ReplicaCount)
	s.replicateQueue = newReplicateQueue(s.ctx.Gossip, s.allocator, s.ctx.Clock, s.ctx.AllocatorOptions)
	s.replicaGCQueue = newReplicaGCQueue(s.db, s.ctx.Gossip)
	s.raftLogQueue = newRaftLogQueue(s.db, s.ctx.Gossip)
	s.scanner.AddQueues(s.gcQueue, s.splitQueue, s.verifyQueue, s.replicateQueue, s.replicaGCQueue, s.raftLogQueue)

	// Add consistency check scanner.
	s.consistencyScanner = newReplicaScanner(ctx.ConsistencyCheckInterval, 0, newStoreRangeSet(s))
	s.replicaConsistencyQueue = newReplicaConsistencyQueue(s.ctx.Gossip)
	s.consistencyScanner.AddQueues(s.replicaConsistencyQueue)

	return s
}

// String formats a store for debug output.
func (s *Store) String() string {
	return fmt.Sprintf("store=%d:%d (%s)", s.Ident.NodeID, s.Ident.StoreID, s.engine)
}

// context returns a base context to pass along with commands being executed,
// derived from the supplied context (which is not allowed to be nil).
func (s *Store) context(ctx context.Context) context.Context {
	if ctx == nil {
		panic("ctx cannot be nil")
	}
	return log.Add(ctx,
		log.NodeID, s.Ident.NodeID,
		log.StoreID, s.Ident.StoreID)
}

// IsStarted returns true if the Store has been started.
func (s *Store) IsStarted() bool {
	return atomic.LoadInt32(&s.started) == 1
}

// IterateRangeDescriptors calls the provided function with each descriptor
// from the provided Engine. The return values of this method and fn have
// semantics similar to engine.MVCCIterate.
func IterateRangeDescriptors(eng engine.Engine, fn func(desc roachpb.RangeDescriptor) (bool, error)) error {
	// Iterator over all range-local key-based data.
	start := keys.RangeDescriptorKey(roachpb.RKeyMin)
	end := keys.RangeDescriptorKey(roachpb.RKeyMax)

	kvToDesc := func(kv roachpb.KeyValue) (bool, error) {
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
		return fn(desc)
	}

	_, err := engine.MVCCIterate(context.Background(), eng, start, end, roachpb.MaxTimestamp, false /* !consistent */, nil, /* txn */
		false /* !reverse */, kvToDesc)
	return err
}

// Start the engine, set the GC and read the StoreIdent.
func (s *Store) Start(stopper *stop.Stopper) error {
	s.stopper = stopper

	// Add a closer for the various scanner queues, needed to properly clean up
	// the event logs.
	s.stopper.AddCloser(stop.CloserFn(func() {
		s.gcQueue.Close()
		s.splitQueue.Close()
		s.verifyQueue.Close()
		s.replicateQueue.Close()
		s.replicaGCQueue.Close()
		s.raftLogQueue.Close()
		s.replicaConsistencyQueue.Close()
	}))

	if s.Ident.NodeID == 0 {
		// Open engine (i.e. initialize RocksDB database). "NodeID != 0"
		// implies the engine has already been opened.
		if err := s.engine.Open(); err != nil {
			return err
		}

		// Read store ident and return a not-bootstrapped error if necessary.
		ok, err := engine.MVCCGetProto(context.Background(), s.engine, keys.StoreIdentKey(), roachpb.ZeroTimestamp, true, nil, &s.Ident)
		if err != nil {
			return err
		} else if !ok {
			return &NotBootstrappedError{}
		}
	}

	// If the nodeID is 0, it has not be assigned yet.
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

	// Iterate over all range descriptors, ignoring uncommitted versions
	// (consistent=false). Uncommitted intents which have been abandoned
	// due to a split crashing halfway will simply be resolved on the
	// next split attempt. They can otherwise be ignored.
	s.mu.Lock()
	err = IterateRangeDescriptors(s.engine, func(desc roachpb.RangeDescriptor) (bool, error) {
		if _, repDesc := desc.FindReplica(s.StoreID()); repDesc == nil {
			// We are no longer a member of the range, but we didn't GC
			// the replica before shutting down. Destroy the replica now
			// to avoid creating a new replica without a valid replica ID
			// (which is necessary to have a non-nil raft group)
			return false, s.destroyReplicaData(&desc)
		}
		rng, err := NewReplica(&desc, s, 0)
		if err != nil {
			return false, err
		}
		if err = s.addReplicaInternalLocked(rng); err != nil {
			return false, err
		}
		// Add this range and its stats to our counter.
		s.metrics.replicaCount.Inc(1)
		s.metrics.addMVCCStats(rng.stats.GetMVCC())
		// TODO(bdarnell): lazily create raft groups to make the following comment true again.
		// Note that we do not create raft groups at this time; they will be created
		// on-demand the first time they are needed. This helps reduce the amount of
		// election-related traffic in a cold start.
		// Raft initialization occurs when we propose a command on this range or
		// receive a raft message addressed to it.
		// TODO(bdarnell): Also initialize raft groups when read leases are needed.
		// TODO(bdarnell): Scan all ranges at startup for unapplied log entries
		// and initialize those groups.
		return false, nil
	})
	s.mu.Unlock()
	if err != nil {
		return err
	}

	// Start Raft processing goroutines.
	s.ctx.Transport.Listen(s.StoreID(), s.enqueueRaftMessage)
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
					cfg, _ := s.ctx.Gossip.GetSystemConfig()
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
		s.stopper.RunWorker(func() {
			select {
			case <-s.ctx.Gossip.Connected:
				s.scanner.Start(s.ctx.Clock, s.stopper)
			case <-s.stopper.ShouldStop():
				return
			}
		})

		// Start the consistency scanner.
		s.stopper.RunWorker(func() {
			select {
			case <-s.ctx.Gossip.Connected:
				s.consistencyScanner.Start(s.ctx.Clock, s.stopper)
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
	ctx := s.context(context.TODO())
	// Periodic updates run in a goroutine and signal a WaitGroup upon completion
	// of their first iteration.
	s.initComplete.Add(2)
	s.stopper.RunWorker(func() {
		// Run the first time without waiting for the Ticker and signal the WaitGroup.
		if err := s.maybeGossipFirstRange(); err != nil {
			log.Warningc(ctx, "error gossiping first range data: %s", err)
		}
		s.initComplete.Done()
		ticker := time.NewTicker(sentinelGossipInterval)
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
// the lease instead of waiting for the next sentinelGossipInterval
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
			pErr := rng.maybeGossipFirstRange()
			if nlErr, ok := pErr.GetDetail().(*roachpb.NotLeaderError); !ok || nlErr.Leader != nil {
				return pErr.GoError()
			}
		} else {
			return nil
		}
	}
	return nil
}

// maybeGossipSystemConfig looks for the range containing SystemConfig keys and
// lets that range gossip them.
func (s *Store) maybeGossipSystemConfig() error {
	rng := s.LookupReplica(roachpb.RKey(keys.SystemConfigSpan.Key), nil)
	if rng == nil {
		// This store has no range with this configuration.
		return nil
	}
	// Wake up the replica. If it acquires a fresh lease, it will
	// gossip. If an unexpected error occurs (i.e. nobody else seems to
	// have an active lease but we still failed to obtain it), return
	// that error.
	_, pErr := rng.getLeaseForGossip(s.context(context.TODO()))
	return pErr.GoError()
}

// systemGossipUpdate is a callback for gossip updates to
// the system config which affect range split boundaries.
func (s *Store) systemGossipUpdate(cfg config.SystemConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// For every range, update its MaxBytes and check if it needs to be split.
	for _, rng := range s.mu.replicas {
		if zone, err := cfg.GetZoneConfigForKey(rng.Desc().StartKey); err == nil {
			rng.SetMaxBytes(zone.RangeMaxBytes)
		}
		s.splitQueue.MaybeAdd(rng, s.ctx.Clock.Now())
	}
}

// GossipStore broadcasts the store on the gossip network.
func (s *Store) GossipStore() {
	ctx := s.context(context.TODO())

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
		engine.MakeMVCCMetadataKey(roachpb.Key(roachpb.RKeyMax)), 10)
	if err != nil {
		return util.Errorf("store %s: unable to access: %s", s.engine, err)
	} else if len(kvs) > 0 {
		// See if this is an already-bootstrapped store.
		ok, err := engine.MVCCGetProto(context.Background(), s.engine, keys.StoreIdentKey(), roachpb.ZeroTimestamp, true, nil, &s.Ident)
		if err != nil {
			return util.Errorf("store %s is non-empty but cluster ID could not be determined: %s", s.engine, err)
		}
		if ok {
			return util.Errorf("store %s already belongs to cockroach cluster %s", s.engine, s.Ident.ClusterID)
		}
		keyVals := []string{}
		for _, kv := range kvs {
			keyVals = append(keyVals, fmt.Sprintf("%s: %q", kv.Key, kv.Value))
		}
		return util.Errorf("store %s is non-empty but does not contain store metadata (first %d key/values: %s)", s.engine, len(keyVals), keyVals)
	}
	err = engine.MVCCPutProto(context.Background(), s.engine, nil, keys.StoreIdentKey(), roachpb.ZeroTimestamp, nil, &s.Ident)
	return err
}

// GetReplica fetches a replica by Range ID. Returns an error if no replica is found.
func (s *Store) GetReplica(rangeID roachpb.RangeID) (*Replica, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getReplicaLocked(rangeID)
}

// getReplicaLocked fetches a replica by RangeID. The store's lock must be held
// in read or read/write mode.
func (s *Store) getReplicaLocked(rangeID roachpb.RangeID) (*Replica, error) {
	if rng, ok := s.mu.replicas[rangeID]; ok {
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
	s.mu.Lock()
	defer s.mu.Unlock()

	var rng *Replica
	s.mu.replicasByKey.AscendGreaterOrEqual((rangeBTreeKey)(start.Next()), func(i btree.Item) bool {
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
	s.mu.replicasByKey.AscendGreaterOrEqual((rangeBTreeKey)(rngDesc.StartKey.Next()), func(i btree.Item) bool {
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.mu.replicas[rangeID]; ok {
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
	ctx := context.Background()

	// Range descriptor.
	if err := engine.MVCCPutProto(ctx, batch, ms, keys.RangeDescriptorKey(desc.StartKey), now, nil, desc); err != nil {
		return err
	}
	// Replica GC & Verification timestamps.
	if err := engine.MVCCPutProto(ctx, batch, nil /* ms */, keys.RangeLastReplicaGCTimestampKey(desc.RangeID), roachpb.ZeroTimestamp, nil, &now); err != nil {
		return err
	}
	if err := engine.MVCCPutProto(ctx, batch, nil /* ms */, keys.RangeLastVerificationTimestampKey(desc.RangeID), roachpb.ZeroTimestamp, nil, &now); err != nil {
		return err
	}
	// Range addressing for meta2.
	meta2Key := keys.RangeMetaKey(roachpb.RKeyMax)
	if err := engine.MVCCPutProto(ctx, batch, ms, meta2Key, now, nil, desc); err != nil {
		return err
	}
	// Range addressing for meta1.
	meta2KeyAddr, err := keys.Addr(meta2Key)
	if err != nil {
		return err
	}
	meta1Key := keys.RangeMetaKey(meta2KeyAddr)
	if err := engine.MVCCPutProto(ctx, batch, ms, meta1Key, now, nil, desc); err != nil {
		return err
	}

	// Now add all passed-in default entries.
	for _, kv := range initialValues {
		// Initialize the checksums.
		kv.Value.InitChecksum(kv.Key)
		if err := engine.MVCCPut(ctx, batch, ms, kv.Key, now, kv.Value, nil); err != nil {
			return err
		}
	}

	// Range Tree setup.
	if err := SetupRangeTree(ctx, batch, ms, now, desc.StartKey); err != nil {
		return err
	}

	// Set range stats.
	if err := ms.AccountForSelf(desc.RangeID); err != nil {
		return err
	}
	if err := engine.MVCCSetRangeStats(ctx, batch, desc.RangeID, ms); err != nil {
		return err
	}

	if err := batch.Commit(); err != nil {
		return err
	}
	return nil
}

// ClusterID accessor.
func (s *Store) ClusterID() uuid.UUID { return s.Ident.ClusterID }

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

// Tracer accessor.
func (s *Store) Tracer() opentracing.Tracer { return s.ctx.Tracer }

// TestingKnobs accessor.
func (s *Store) TestingKnobs() *StoreTestingKnobs { return &s.ctx.TestingKnobs }

// NewRangeDescriptor creates a new descriptor based on start and end
// keys and the supplied roachpb.Replicas slice. It allocates new
// replica IDs to fill out the supplied replicas.
func (s *Store) NewRangeDescriptor(
	start, end roachpb.RKey, replicas []roachpb.ReplicaDescriptor,
) (*roachpb.RangeDescriptor, error) {
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
	if _, ok := s.mu.uninitReplicas[newDesc.RangeID]; ok {
		// If we have an uninitialized replica of the new range, delete it
		// to make way for the complete one created by the split. A live
		// uninitialized raft group cannot be converted to an
		// initialized one (because of the tricks we do to change
		// FirstIndex from 0 to raftInitialLogIndex), so the group must be
		// removed before we install the new range into s.replicas.
		delete(s.mu.uninitReplicas, newDesc.RangeID)
		delete(s.mu.replicas, newDesc.RangeID)
	}

	// Replace the end key of the original range with the start key of
	// the new range. Reinsert the range since the btree is keyed by range end keys.
	if s.mu.replicasByKey.Delete(origRng) == nil {
		return util.Errorf("couldn't find range %s in rangesByKey btree", origRng)
	}

	copyDesc := *origDesc
	copyDesc.EndKey = append([]byte(nil), newDesc.StartKey...)
	origRng.setDescWithoutProcessUpdate(&copyDesc)

	if s.mu.replicasByKey.ReplaceOrInsert(origRng) != nil {
		return util.Errorf("couldn't insert range %v in rangesByKey btree", origRng)
	}

	if err := s.addReplicaInternalLocked(newRng); err != nil {
		return util.Errorf("couldn't insert range %v in rangesByKey btree: %s", newRng, err)
	}

	// Update the max bytes and other information of the new range.
	// This may not happen if the system config has not yet been loaded.
	// Since this is done under the store lock, system config update will
	// properly set these fields.
	if err := newRng.updateRangeInfo(newRng.Desc()); err != nil {
		return err
	}

	s.metrics.replicaCount.Inc(1)
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
	if err := s.removeReplicaImpl(subsumedRng, *subsumedDesc, false); err != nil {
		return util.Errorf("cannot remove range %s", err)
	}

	// Update the end key of the subsuming range.
	copy := *subsumingDesc
	copy.EndKey = updatedEndKey
	if err := subsumingRng.setDesc(&copy); err != nil {
		return err
	}

	return nil
}

// AddReplicaTest adds the replica to the store's replica map and to the sorted
// replicasByKey slice. To be used only by unittests.
func (s *Store) AddReplicaTest(rng *Replica) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.addReplicaInternalLocked(rng); err != nil {
		return err
	}
	s.metrics.replicaCount.Inc(1)
	return nil
}

// addReplicaInternalLocked adds the replica to the replicas map and the
// replicasByKey btree. Returns a rangeAlreadyExists error if a replica with
// the same Range ID has already been added to this store.
// addReplicaInternalLocked requires that the store lock is held.
func (s *Store) addReplicaInternalLocked(rng *Replica) error {
	if !rng.IsInitialized() {
		return util.Errorf("attempted to add uninitialized range %s", rng)
	}

	// TODO(spencer); will need to determine which range is
	// newer, and keep that one.
	if err := s.addReplicaToRangeMapLocked(rng); err != nil {
		return err
	}

	if s.hasOverlappingReplicaLocked(rng.Desc()) {
		return rangeAlreadyExists{rng}
	}
	if exRngItem := s.mu.replicasByKey.ReplaceOrInsert(rng); exRngItem != nil {
		return util.Errorf("range for key %v already exists in rangesByKey btree",
			(exRngItem.(*Replica)).getKey())
	}
	return nil
}

// addReplicaToRangeMapLocked adds the replica to the replicas map.
// addReplicaToRangeMapLocked requires that the store lock is held.
func (s *Store) addReplicaToRangeMapLocked(rng *Replica) error {
	if exRng, ok := s.mu.replicas[rng.RangeID]; ok {
		return rangeAlreadyExists{exRng}
	}
	s.mu.replicas[rng.RangeID] = rng
	return nil
}

// RemoveReplica removes the replica from the store's replica map and
// from the sorted replicasByKey btree. The version of the replica
// descriptor that was used to make the removal decision is passed in,
// and the removal is aborted if the replica ID has changed since
// then. If `destroy` is true, all data beloing to the replica will be
// deleted. In either case a tombstone record will be written.
func (s *Store) RemoveReplica(rep *Replica, origDesc roachpb.RangeDescriptor, destroy bool) error {
	s.processRaftMu.Lock()
	defer s.processRaftMu.Unlock()
	return s.removeReplicaImpl(rep, origDesc, destroy)
}

// removeReplicaImpl is the implementation of RemoveReplica, which is
// sometimes called directly when the necessary lock is already held.
// It requires that s.processRaftMu is held and that s.mu is not held.
func (s *Store) removeReplicaImpl(rep *Replica, origDesc roachpb.RangeDescriptor, destroy bool) error {
	desc := rep.Desc()
	_, rd := desc.FindReplica(s.StoreID())
	if rd != nil && rd.ReplicaID >= origDesc.NextReplicaID {
		return util.Errorf("cannot remove replica %s; replica ID has changed (%s >= %s)",
			rep, rd.ReplicaID, origDesc.NextReplicaID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.mu.replicas, rep.RangeID)
	if s.mu.replicasByKey.Delete(rep) == nil {
		return util.Errorf("couldn't find range in replicasByKey btree")
	}
	s.scanner.RemoveReplica(rep)
	s.consistencyScanner.RemoveReplica(rep)

	// Adjust stats before calling Destroy. This can be called before or after
	// Destroy, but this configuration helps avoid races in stat verification
	// tests.
	s.metrics.subtractMVCCStats(rep.GetMVCCStats())
	s.metrics.replicaCount.Dec(1)

	// TODO(bdarnell): This is fairly expensive to do under store.Mutex, but
	// doing it outside the lock is tricky due to the risk that a replica gets
	// recreated by an incoming raft message.
	if destroy {
		if err := rep.Destroy(origDesc); err != nil {
			return err
		}
	}

	return nil
}

// destroyReplicaData deletes all data associated with a replica, leaving a tombstone.
// If a Replica object exists, use Replica.Destroy instead of this method.
func (s *Store) destroyReplicaData(desc *roachpb.RangeDescriptor) error {
	iter := newReplicaDataIterator(desc, s.Engine(), false /* !replicatedOnly */)
	defer iter.Close()
	batch := s.Engine().NewBatch()
	defer batch.Close()
	for ; iter.Valid(); iter.Next() {
		_ = batch.Clear(iter.Key())
	}

	// Save a tombstone. The range cannot be re-replicated onto this
	// node without having a replica ID of at least desc.NextReplicaID.
	tombstoneKey := keys.RaftTombstoneKey(desc.RangeID)
	tombstone := &roachpb.RaftTombstone{
		NextReplicaID: desc.NextReplicaID,
	}
	if err := engine.MVCCPutProto(context.Background(), batch, nil, tombstoneKey, roachpb.ZeroTimestamp, nil, tombstone); err != nil {
		return err
	}

	return batch.Commit()
}

// processRangeDescriptorUpdate is called whenever a range's
// descriptor is updated.
func (s *Store) processRangeDescriptorUpdate(rng *Replica) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.processRangeDescriptorUpdateLocked(rng)
}

// processRangeDescriptorUpdateLocked requires that the store lock is held.
func (s *Store) processRangeDescriptorUpdateLocked(rng *Replica) error {
	if !rng.IsInitialized() {
		return util.Errorf("attempted to process uninitialized range %s", rng)
	}

	rangeID := rng.RangeID

	if _, ok := s.mu.uninitReplicas[rangeID]; !ok {
		// Do nothing if the range has already been initialized.
		return nil
	}
	delete(s.mu.uninitReplicas, rangeID)

	// Add the range and its current stats into metrics.
	s.metrics.replicaCount.Inc(1)
	s.metrics.addMVCCStats(rng.stats.GetMVCC())

	if s.mu.replicasByKey.Has(rng) {
		return rangeAlreadyExists{rng}
	}
	if exRngItem := s.mu.replicasByKey.ReplaceOrInsert(rng); exRngItem != nil {
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

// Registry returns the metric registry used by this store.
func (s *Store) Registry() *metric.Registry {
	return s.metrics.registry
}

// MVCCStats returns the current MVCCStats accumulated for this store.
// TODO(mrtracy): This should be removed as part of #4465, this is only needed
// to support the current StatusSummary structures which will be changing.
func (s *Store) MVCCStats() engine.MVCCStats {
	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()
	return s.metrics.stats
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
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.mu.replicas)
}

// Send fetches a range based on the header's replica, assembles method, args &
// reply into a Raft Cmd struct and executes the command using the fetched
// range.
// An incoming request may be transactional or not. If it is not transactional,
// the timestamp at which it executes may be higher than that optionally
// specified through the incoming BatchRequest, and it is not guaranteed that
// all operations are written at the same timestamp. If it is transactional, a
// timestamp must not be set - it is deduced automatically from the
// transaction. Should a transactional operation be forced to a higher
// timestamp (for instance due to the timestamp cache), the response will have
// a transaction set which should be used to update the client transaction.
func (s *Store) Send(ctx context.Context, ba roachpb.BatchRequest) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	ctx = s.context(ctx)

	for _, union := range ba.Requests {
		arg := union.GetInner()
		header := arg.Header()
		if err := verifyKeys(header.Key, header.EndKey, roachpb.IsRange(arg)); err != nil {
			return nil, roachpb.NewError(err)
		}
	}

	if err := ba.SetActiveTimestamp(s.Clock().Now); err != nil {
		return nil, roachpb.NewError(err)
	}

	if s.Clock().MaxOffset() > 0 {
		// Once a command is submitted to raft, all replicas' logical
		// clocks will be ratcheted forward to match. If the command
		// appears to come from a node with a bad clock, reject it now
		// before we reach that point.
		offset := time.Duration(ba.Timestamp.WallTime - s.Clock().PhysicalNow())
		if offset > s.Clock().MaxOffset() {
			return nil, roachpb.NewErrorf("rejecting command with timestamp in the future: %d (%s ahead)",
				ba.Timestamp.WallTime, offset)
		}
	}
	// Update our clock with the incoming request timestamp. This advances the
	// local node's clock to a high water mark from all nodes with which it has
	// interacted. We hold on to the resulting timestamp - we know that any
	// write with a higher timestamp we run into later must have started after
	// this point in (absolute) time.
	now := s.ctx.Clock.Update(ba.Timestamp)

	defer func() {
		if r := recover(); r != nil {
			// On panic, don't run the defer. It's probably just going to panic
			// again due to undefined state.
			panic(r)
		}
		if ba.Txn != nil {
			// We're in a Txn, so we can reduce uncertainty restarts by attaching
			// the above timestamp to the returned response or error. The caller
			// can use it to shorten its uncertainty interval when it comes back to
			// this node.
			if pErr != nil {
				pErr.OriginNode = ba.Replica.NodeID
				txn := pErr.GetTxn()
				if txn == nil {
					txn = ba.Txn
				}
				txn.UpdateObservedTimestamp(ba.Replica.NodeID, now)
				pErr.SetTxn(txn)
			} else {
				if br.Txn == nil {
					br.Txn = ba.Txn
				}
				br.Txn.UpdateObservedTimestamp(ba.Replica.NodeID, now)
				// Update our clock with the outgoing response txn timestamp.
				s.ctx.Clock.Update(br.Txn.Timestamp)
			}
		} else {
			if pErr == nil {
				// Update our clock with the outgoing response timestamp.
				s.ctx.Clock.Update(br.Timestamp)
			}
		}

		if pErr != nil {
			pErr.Now = now
		} else {
			br.Now = now
		}
	}()

	if ba.Txn != nil {
		// We make our transaction aware that no other operation that causally
		// precedes it could have started after `now`. This is important: If we
		// wind up pushing a value, it will be in our immediate future, and not
		// updating the top end of our uncertainty timestamp would lead to a
		// restart (at least in the absence of a prior observed timestamp from
		// this node, in which case the following is a no-op).
		if now.Less(ba.Txn.MaxTimestamp) {
			shallowTxn := *ba.Txn
			shallowTxn.MaxTimestamp.Backward(now)
			ba.Txn = &shallowTxn
		}
	}

	if log.V(1) {
		log.Trace(ctx, fmt.Sprintf("executing %s", ba))
	} else {
		log.Trace(ctx, fmt.Sprintf("executing %d requests", len(ba.Requests)))
	}
	// Backoff and retry loop for handling errors. Backoff times are measured
	// in the Trace.
	// Increase the sequence counter to avoid getting caught in replay
	// protection on retry.
	next := func(r *retry.Retry) bool {
		if r.CurrentAttempt() > 0 {
			ba.SetNewRequest()
			log.Trace(ctx, "backoff")
		}
		return r.Next()
	}
	var rng *Replica

	// Add the command to the range for execution; exit retry loop on success.
	s.mu.Lock()
	retryOpts := s.ctx.RangeRetryOptions
	s.mu.Unlock()
	for r := retry.Start(retryOpts); next(&r); {
		// Get range and add command to the range for execution.
		var err error
		rng, err = s.GetReplica(ba.RangeID)
		if err != nil {
			pErr = roachpb.NewError(err)
			return nil, pErr
		}
		rng.assert5725(ba)
		br, pErr = rng.Send(ctx, ba)
		if pErr == nil {
			return br, nil
		}

		// Maybe resolve a potential write intent error. We do this here
		// because this is the code path with the requesting client
		// waiting. We don't want every replica to attempt to resolve the
		// intent independently, so we can't do it there.
		if wiErr, ok := pErr.GetDetail().(*roachpb.WriteIntentError); ok && pErr.Index != nil {
			var pushType roachpb.PushTxnType
			if ba.IsWrite() {
				pushType = roachpb.PUSH_ABORT
			} else {
				pushType = roachpb.PUSH_TIMESTAMP
			}

			index := pErr.Index
			args := ba.Requests[index.Index].GetInner()
			// Make a copy of the header for the upcoming push; we will update
			// the timestamp.
			h := ba.Header
			// We must push at least to h.Timestamp, but in fact we want to
			// go all the way up to a timestamp which was taken off the HLC
			// after our operation started. This allows us to not have to
			// restart for uncertainty as we come back and read.
			h.Timestamp.Forward(now)
			pErr = s.intentResolver.processWriteIntentError(ctx, *wiErr, rng, args, h, pushType)
			// Preserve the error index.
			pErr.Index = index
		}

		log.Trace(ctx, fmt.Sprintf("error: %T", pErr.GetDetail()))
		switch t := pErr.GetDetail().(type) {
		case *roachpb.WriteIntentError:
			// If write intent error is resolved, exit retry/backoff loop to
			// immediately retry.
			if t.Resolved {
				r.Reset()
				if log.V(1) {
					log.Warning(pErr)
				}
				continue
			}
			if log.V(1) {
				log.Warning(pErr)
			}
			// Update the batch transaction, if applicable, in case it has
			// been independently pushed and has more recent information.
			rng.assert5725(ba)
			if ba.Txn != nil {
				updatedTxn, pErr := s.maybeUpdateTransaction(ba.Txn, now)
				if pErr != nil {
					return nil, pErr
				}
				ba.Txn = updatedTxn
			}
			continue
		}
		return nil, pErr
	}

	// By default, retries are indefinite. However, some unittests set a
	// maximum retry count; return txn retry error for transactional cases
	// and the original error otherwise.
	log.Trace(ctx, "store retry limit exceeded") // good to check for if tests fail
	if ba.Txn != nil {
		pErr = roachpb.NewErrorWithTxn(roachpb.NewTransactionRetryError(), ba.Txn)
	}
	return nil, pErr
}

// maybeUpdateTransaction does a "touch" push on the specified
// transaction to glean possible changes, such as a higher timestamp
// and/or priority. It turns out this is necessary while a request
// is in a backoff/retry loop pushing write intents as two txns
// can have circular dependencies where both are unable to push
// because they have different information about their own txns.
//
// The supplied transaction is updated with the results of the
// "query" push if possible.
func (s *Store) maybeUpdateTransaction(txn *roachpb.Transaction, now roachpb.Timestamp) (*roachpb.Transaction, *roachpb.Error) {
	// Attempt to push the transaction which created the intent.
	b := client.Batch{}
	b.InternalAddRequest(&roachpb.PushTxnRequest{
		Span: roachpb.Span{
			Key: txn.Key,
		},
		Now:       now,
		PusheeTxn: txn.TxnMeta,
		PushType:  roachpb.PUSH_QUERY,
	})
	br, pErr := s.db.RunWithResponse(&b)
	if pErr != nil {
		return nil, pErr
	}
	// ID can be nil if no BeginTransaction has been sent yet.
	if updatedTxn := &br.Responses[0].GetInner().(*roachpb.PushTxnResponse).PusheeTxn; updatedTxn.ID != nil {
		switch updatedTxn.Status {
		case roachpb.COMMITTED:
			return nil, roachpb.NewErrorWithTxn(roachpb.NewTransactionStatusError("already committed"), updatedTxn)
		case roachpb.ABORTED:
			return nil, roachpb.NewErrorWithTxn(roachpb.NewTransactionAbortedError(), updatedTxn)
		}
		return updatedTxn, nil
	}
	return txn, nil
}

// enqueueRaftMessage enqueues a request for eventual processing. It
// returns an error to conform to the RPC interface but it always
// returns nil without waiting for the message to be processed.
//
// TODO(bdarnell): is this buffering necessary? sufficient? It's
// tempting to move handleRaftMessage off the processRaft goroutine
// and rely on processRaftMu instead. However, the interaction with
// the stopper is tricky. Run handleRaftMessage as a task and we see
// intent resolutions hang during test shutdown because they expect to
// be able to make progress during the draining phase. Run it without
// a task and it may run while the stopper is shutting down. It needs
// to be run from within a Worker and processRaft is a convenient one
// to use.
func (s *Store) enqueueRaftMessage(req *RaftMessageRequest) error {
	s.raftRequestChan <- req
	return nil
}

// handleRaftMessage dispatches a raft message to the appropriate
// Replica. It requires that processRaftMu is held and that s.mu is
// not held.
func (s *Store) handleRaftMessage(req *RaftMessageRequest) error {
	switch req.Message.Type {
	case raftpb.MsgSnap:
		if !s.canApplySnapshot(req.GroupID, req.Message.Snapshot) {
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
		r, ok := s.mu.replicas[req.GroupID]
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
	// It deadlocks to hold s.Mutex while calling raftGroup.Step.
	s.mu.Unlock()
	if err != nil {
		// If getOrCreateReplicaLocked returns an error, log it at V(1)
		// instead of returning it (where the caller will log it as
		// Error). Errors here generally mean that the sender is out of
		// date, and it may remain so until replicaGCQueue runs, so when
		// these messages occur they will repeat many times.
		// TODO(bdarnell): if we had a better way to report errors to the
		// sender then we could prompt the sender to GC this replica
		// immediately.
		if log.V(1) {
			log.Infof("refusing incoming Raft message %s for group %d from %s to %s: %s",
				req.Message.Type, req.GroupID, req.FromReplica, req.ToReplica, err)
		}
		return nil
	}
	r.mu.Lock()
	err = r.mu.raftGroup.Step(req.Message)
	r.mu.Unlock()
	if err != nil {
		return err
	}
	s.enqueueRaftUpdateCheck(req.GroupID)
	return nil
}

// enqueueRaftUpdateCheck asynchronously registers the given range ID to be
// checked for raft updates when the processRaft goroutine is idle.
func (s *Store) enqueueRaftUpdateCheck(rangeID roachpb.RangeID) {
	s.pendingRaftGroups.Lock()
	s.pendingRaftGroups.value[rangeID] = struct{}{}
	s.pendingRaftGroups.Unlock()
	select {
	case s.wakeRaftLoop <- struct{}{}:
	default:
	}
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
			s.processRaftMu.Lock()
			s.mu.Lock()
			s.pendingRaftGroups.Lock()
			if len(s.pendingRaftGroups.value) > 0 {
				for rangeID := range s.pendingRaftGroups.value {
					if r, ok := s.mu.replicas[rangeID]; ok {
						replicas = append(replicas, r)
					}
				}
				s.pendingRaftGroups.value = map[roachpb.RangeID]struct{}{}
			}
			s.pendingRaftGroups.Unlock()
			s.mu.Unlock()
			for _, r := range replicas {
				if err := r.handleRaftReady(); err != nil {
					panic(err) // TODO(bdarnell)
				}
			}
			s.processRaftMu.Unlock()

			select {
			case <-s.wakeRaftLoop:

			case req := <-s.raftRequestChan:
				s.processRaftMu.Lock()
				if err := s.handleRaftMessage(req); err != nil {
					log.Errorf("error handling raft message: %s", err)
				}
				s.processRaftMu.Unlock()

			case st := <-s.ctx.Transport.SnapshotStatusChan:
				s.processRaftMu.Lock()
				s.mu.Lock()
				if r, ok := s.mu.replicas[st.Req.GroupID]; ok {
					r.reportSnapshotStatus(st.Req.Message.To, st.Err)
				}
				s.mu.Unlock()
				s.processRaftMu.Unlock()

			case <-ticker.C:
				// TODO(bdarnell): rework raft ticker.
				s.processRaftMu.Lock()
				s.mu.Lock()
				for _, r := range s.mu.replicas {
					if err := r.tick(); err != nil {
						log.Error(err)
					}
				}
				// Enqueue all ranges for readiness checks. Note that we
				// could not hold the pendingRaftGroups lock during the
				// previous loop because of lock ordering constraints with
				// r.tick().
				s.pendingRaftGroups.Lock()
				for rangeID := range s.mu.replicas {
					s.pendingRaftGroups.value[rangeID] = struct{}{}
				}
				s.pendingRaftGroups.Unlock()
				s.mu.Unlock()
				s.processRaftMu.Unlock()

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
	r, ok := s.mu.replicas[groupID]
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
	if ok, err := engine.MVCCGetProto(context.Background(), s.Engine(), tombstoneKey, roachpb.ZeroTimestamp, true, nil, &tombstone); err != nil {
		return nil, err
	} else if ok {
		if replicaID != 0 && replicaID < tombstone.NextReplicaID {
			return nil, &roachpb.RaftGroupDeletedError{}
		}
	}

	var err error
	r, err = NewReplica(&roachpb.RangeDescriptor{
		RangeID: groupID,
		// TODO(bdarnell): other fields are unknown; need to populate them from
		// snapshot.
	}, s, replicaID)
	if err != nil {
		return nil, err
	}
	// Add the range to range map, but not rangesByKey since
	// the range's start key is unknown. The range will be
	// added to rangesByKey later when a snapshot is applied.
	if err = s.addReplicaToRangeMapLocked(r); err != nil {
		return nil, err
	}
	s.mu.uninitReplicas[r.RangeID] = r

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
	if rep, ok := s.mu.replicaDescCache.Get(replicaDescCacheKey{groupID, replicaID}); ok {
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
// cacheReplicaDescriptorLocked requires that the store lock is held.
func (s *Store) cacheReplicaDescriptorLocked(groupID roachpb.RangeID, replica roachpb.ReplicaDescriptor) {
	if old, ok := s.mu.replicaDescCache.Get(replicaDescCacheKey{groupID, replica.ReplicaID}); ok {
		if old != replica {
			log.Fatalf("%s replicaDescCache: clobbering %s with %s", s, old, replica)
		}
		return
	}
	s.mu.replicaDescCache.Add(replicaDescCacheKey{groupID, replica.ReplicaID}, replica)
}

// canApplySnapshot returns true if the snapshot can be applied to
// this store's replica (i.e. it is not from an older incarnation of
// the replica).
func (s *Store) canApplySnapshot(rangeID roachpb.RangeID, snap raftpb.Snapshot) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.mu.replicas[rangeID]; ok && r.IsInitialized() {
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
	_, encodedCmd := DecodeRaftCommand(data)
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

// computeReplicationStatus counts a number of simple replication statistics for
// the ranges in this store.
// TODO(bram): #4564 It may be appropriate to compute these statistics while
// scanning ranges. An ideal solution would be to create incremental events
// whenever availability changes.
func (s *Store) computeReplicationStatus(now int64) (
	leaderRangeCount, replicatedRangeCount, availableRangeCount int64) {
	// Load the system config.
	cfg, ok := s.Gossip().GetSystemConfig()
	if !ok {
		log.Infof("system config not yet available")
		return
	}

	timestamp := roachpb.Timestamp{WallTime: now}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rng := range s.mu.replicas {
		zoneConfig, err := cfg.GetZoneConfigForKey(rng.Desc().StartKey)
		if err != nil {
			log.Error(err)
			continue
		}
		raftStatus := rng.RaftStatus()
		if raftStatus.SoftState.RaftState == raft.StateLeader {
			leaderRangeCount++
			// TODO(bram): #4564 Compare attributes of the stores so we can
			// track ranges that have enough replicas but still need to be
			// migrated onto nodes with the desired attributes.
			if len(raftStatus.Progress) >= len(zoneConfig.ReplicaAttrs) {
				replicatedRangeCount++
			}

			// If any replica holds the leader lease, the range is available.
			if lease, _ := rng.getLeaderLease(); lease.Covers(timestamp) {
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

// ComputeMetrics immediately computes the current value of store metrics which
// cannot be computed incrementally. This method should be invoked periodically
// by a higher-level system which records store metrics.
func (s *Store) ComputeMetrics() error {
	// broadcast store descriptor.
	desc, err := s.Descriptor()
	if err != nil {
		return err
	}
	s.metrics.updateCapacityGauges(desc.Capacity)

	// broadcast replication status.
	now := s.ctx.Clock.Now().WallTime
	leaderRangeCount, replicatedRangeCount, availableRangeCount :=
		s.computeReplicationStatus(now)
	s.metrics.updateReplicationGauges(leaderRangeCount, replicatedRangeCount, availableRangeCount)

	// Get the latest RocksDB stats.
	stats, err := s.engine.GetStats()
	if err != nil {
		return err
	}
	s.metrics.updateRocksDBStats(*stats)

	return nil
}

// ComputeMVCCStatsTest immediately computes correct total MVCC usage statistics
// for the store, returning the computed values (but without modifying the
// store). This is intended for use only by unit tests.
func (s *Store) ComputeMVCCStatsTest() (engine.MVCCStats, error) {
	var totalStats engine.MVCCStats
	var err error

	visitor := newStoreRangeSet(s)
	now := s.Clock().PhysicalNow()
	visitor.Visit(func(r *Replica) bool {
		var stats engine.MVCCStats
		stats, err = ComputeStatsForRange(r.Desc(), s.Engine(), now)
		if err != nil {
			return false
		}
		totalStats.Add(stats)
		return true
	})
	return totalStats, err
}

// SetRangeRetryOptions sets the retry options used for this store.
// For unittests only.
// TODO(bdarnell): have the affected tests pass retry options in through
// the StoreContext.
func (s *Store) SetRangeRetryOptions(ro retry.Options) {
	s.mu.Lock()
	s.ctx.RangeRetryOptions = ro
	s.mu.Unlock()
}
