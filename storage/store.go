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
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gogo/protobuf/proto"
	"github.com/google/btree"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/build"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/sqlutil"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/storage/storagebase"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/envutil"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/cockroachdb/cockroach/util/tracing"
	"github.com/cockroachdb/cockroach/util/uuid"
)

const (
	// rangeIDAllocCount is the number of Range IDs to allocate per allocation.
	rangeIDAllocCount               = 10
	defaultHeartbeatIntervalTicks   = 3
	defaultRaftElectionTimeoutTicks = 15
	defaultAsyncSnapshotMaxAge      = time.Minute
	// ttlStoreGossip is time-to-live for store-related info.
	ttlStoreGossip = 2 * time.Minute

	// preemptiveSnapshotRaftGroupID is a bogus ID for which a Raft group is
	// temporarily created during the application of a preemptive snapshot.
	preemptiveSnapshotRaftGroupID = math.MaxUint64

	// rangeLeaseRaftElectionTimeoutMultiplier specifies what multiple the leader
	// lease active duration should be of the raft election timeout.
	rangeLeaseRaftElectionTimeoutMultiplier = 3

	// rangeLeaseRenewalDivisor specifies what quotient the range lease renewal
	// duration should be of the range lease active time.
	rangeLeaseRenewalDivisor = 5
)

var changeTypeInternalToRaft = map[roachpb.ReplicaChangeType]raftpb.ConfChangeType{
	roachpb.ADD_REPLICA:    raftpb.ConfChangeAddNode,
	roachpb.REMOVE_REPLICA: raftpb.ConfChangeRemoveNode,
}

var storeReplicaRaftReadyConcurrency = 2 * runtime.NumCPU()

// TestStoreContext has some fields initialized with values relevant in tests.
func TestStoreContext() StoreContext {
	return StoreContext{
		Tracer:                         tracing.NewTracer(),
		RaftTickInterval:               100 * time.Millisecond,
		RaftHeartbeatIntervalTicks:     1,
		RaftElectionTimeoutTicks:       3,
		ScanInterval:                   10 * time.Minute,
		ConsistencyCheckInterval:       10 * time.Minute,
		ConsistencyCheckPanicOnFailure: true,
		BlockingSnapshotDuration:       100 * time.Millisecond,
	}
}

func newRaftConfig(
	strg raft.Storage,
	id uint64,
	appliedIndex uint64,
	storeCtx StoreContext,
	logger raft.Logger,
) *raft.Config {
	return &raft.Config{
		ID:            id,
		Applied:       appliedIndex,
		ElectionTick:  storeCtx.RaftElectionTimeoutTicks,
		HeartbeatTick: storeCtx.RaftHeartbeatIntervalTicks,
		Storage:       strg,
		Logger:        logger,
		CheckQuorum:   true,
		// TODO(bdarnell): make these configurable; evaluate defaults.
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}
}

type semaphore chan struct{}

func makeSemaphore(n int) semaphore {
	return make(semaphore, n)
}

func (s semaphore) acquire() {
	s <- struct{}{}
}

func (s semaphore) release() {
	<-s
}

// verifyKeys verifies keys. If checkEndKey is true, then the end key
// is verified to be non-nil and greater than start key. If
// checkEndKey is false, end key is verified to be nil. Additionally,
// verifies that start key is less than KeyMax and end key is less
// than or equal to KeyMax. It also verifies that a key range that
// contains range-local keys is completely range-local.
func verifyKeys(start, end roachpb.Key, checkEndKey bool) error {
	if bytes.Compare(start, roachpb.KeyMax) >= 0 {
		return errors.Errorf("start key %q must be less than KeyMax", start)
	}
	if !checkEndKey {
		if len(end) != 0 {
			return errors.Errorf("end key %q should not be specified for this operation", end)
		}
		return nil
	}
	if end == nil {
		return errors.Errorf("end key must be specified")
	}
	if bytes.Compare(roachpb.KeyMax, end) < 0 {
		return errors.Errorf("end key %q must be less than or equal to KeyMax", end)
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
			return errors.Errorf("end key %q must be greater than start %q", end, start)
		}
		if !bytes.Equal(sAddr, start) {
			if bytes.Equal(eAddr, end) {
				return errors.Errorf("start key is range-local, but end key is not")
			}
		} else if bytes.Compare(start, keys.LocalMax) < 0 {
			// It's a range op, not local but somehow plows through local data -
			// not cool.
			return errors.Errorf("start key in [%q,%q) must be greater than LocalMax", start, end)
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
// cycles through a store's replicasByKey btree.
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

// Visit calls the visitor with each Replica until false is returned.
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
	// 1 if the store was started, 0 if it wasn't. To be accessed using atomic
	// ops.
	started int32
	stopper *stop.Stopper
	// The time when the store was Start()ed, in nanos.
	startedAt    int64
	nodeDesc     *roachpb.NodeDescriptor
	initComplete sync.WaitGroup // Signaled by async init tasks
	bookie       *bookie

	// This is 1 if there is an active raft snapshot. This field must be checked
	// and set atomically.
	// TODO(marc): This may be better inside of `mu`, but is not currently feasible.
	hasActiveRaftSnapshot int32

	// drainLeases holds a bool which indicates whether Replicas should be
	// allowed to acquire or extend range leases; see DrainLeases().
	//
	// TODO(bdarnell,tschottdorf): Would look better inside of `mu`. However,
	// deadlocks loom: For example, `systemGossipUpdate` holds `s.mu` while
	// iterating over `s.mu.replicas` and, still under `s.mu`, calls `r.Desc()`
	// but that needs the replica Mutex which may be held by a Replica while
	// calling out to r.store.IsDrainingLeases` (so it would deadlock if
	// that required `s.mu` as well).
	drainLeases atomic.Value

	// Locking notes: To avoid deadlocks, the following lock order must
	// be obeyed: Store.processRaftMu < Replica.readOnlyCmdMu <
	// Store.mu.Mutex < Replica.mu.Mutex <
	// Store.pendingRaftGroups.Mutex. (It is not required to acquire
	// every lock in sequence, but when multiple locks are held at the
	// same time, it is incorrect to acquire a lock with "lesser" value
	// in this sequence after one with "greater" value)
	//
	// Methods of Store with a "Locked" suffix require that
	// Store.mu.Mutex be held. Other locking requirements are indicated
	// in comments.
	//
	// The locking structure here is complex because A) Store is a
	// container of Replicas, so it must generally be consulted before
	// doing anything with any Replica, B) some Replica operations
	// (including splits) modify the Store. Therefore we generally lock
	// Store.mu to find a Replica, release it, then call a method on the
	// Replica. These short-lived locks of Store.mu and Replica.mu are
	// often surrounded by a long-lived lock of processRaftMu as
	// described below.
	//
	// There are two major entry points to this stack of locks:
	// Store.Send (which handles incoming RPCs) and raft-related message
	// processing (including handleRaftReady on the processRaft
	// goroutine and handleRaftMessage on GRPC goroutines). Reads are
	// processed solely through Store.Send; writes start out on
	// Store.Send until they propose their raft command and then they
	// finish on the raft goroutines.
	//
	// TODO(bdarnell): a Replica could be destroyed immediately after
	// Store.Send finds the Replica and releases the lock. We need
	// another RWMutex to be held by anything using a Replica to ensure
	// that everything is finished before releasing it. #7169
	//
	// Detailed description of the locks:
	//
	// * processRaftMu: Named after the processRaft goroutine; held
	//   while any raft messages are being processed (including
	//   handleRaftReady and handleRaftMessage) or while the set of
	//   Replicas in the Store is being changed (which may happen
	//   outside of raft via the replica GC queue). Multiple Replicas
	//   may be processed at a time.
	//
	// * Replica.readOnlyCmdMu (RWMutex): Held in read mode while any
	//   read-only command is in progress on the replica; held in write
	//   mode while executing a commit trigger. This is necessary
	//   because read-only commands mutate the Replica's timestamp cache
	//   (while holding Replica.mu in addition to readOnlyCmdMu). The
	//   RWMutex ensures that no reads are being executed during a split
	//   (which copies the timestamp cache) while still allowing
	//   multiple reads in parallel (#3148). TODO(bdarnell): this lock
	//   only needs to be held during splitTrigger, not all triggers.
	//
	// * Store.mu: Protects the Store's map of its Replicas. Acquired
	//   and released briefly at the start of each request; metadata
	//   operations like splits acquire it again to update the map. Even
	//   though these lock acquisitions do not make up a single critical
	//   section, it is safe thanks to processRaftMu which prevents any
	//   concurrent modifications.
	//
	// * Replica.mu: Protects the Replica's in-memory state. Acquired
	//   and released briefly as needed (note that while the lock is
	//   held "briefly" in that it is not held for an entire request, we
	//   do sometimes do I/O while holding the lock, as in
	//   Replica.Entries). This lock should be held when calling any
	//   methods on the raft group. Raft may call back into the Replica
	//   via the methods of the raft.Storage interface, which assume the
	//   lock is held even though they do not follow our convention of
	//   the "Locked" suffix.
	//
	// * Store.pendingRaftGroups.Mutex: Protects the set of Replicas
	//   that need to be checked for raft changes on the next iteration.
	//   It has its own lock because it is called from Replica while
	//   holding Replica.mu.
	//
	// Splits (and merges, but they're not finished and so will not be
	// discussed here) deserve special consideration: they operate on
	// two ranges. Naively, this is fine because the right-hand range is
	// brand new, but an uninitialized version may have been created by
	// a raft message before we process the split (see commentary on
	// Replica.splitTrigger). We currently make this safe by processing
	// all uninitialized replicas serially before starting any
	// initialized replicas which can run in parallel.
	//
	// Note that because we acquire and release Store.mu and Replica.mu
	// repeatedly rather than holding a lock for an entire request, we
	// are actually relying on higher-level locks to ensure that things
	// don't change out from under us. In particular, handleRaftReady
	// accesses the replicaID more than once, and we rely on
	// processRaftMu to ensure that this is not modified by a concurrent
	// handleRaftMessage. (#4476)

	processRaftMu syncutil.Mutex
	mu            struct {
		syncutil.Mutex                              // Protects all variables in the mu struct.
		replicas       map[roachpb.RangeID]*Replica // Map of replicas by Range ID
		replicasByKey  *btree.BTree                 // btree keyed by ranges end keys.
		uninitReplicas map[roachpb.RangeID]*Replica // Map of uninitialized replicas by Range ID
	}

	// pendingRaftGroups contains the ranges that should be checked for
	// updates. After updating this map, write to wakeRaftLoop to
	// trigger the check.
	pendingRaftGroups struct {
		syncutil.Mutex
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
	SQLExecutor sqlutil.InternalExecutor

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

	// BlockingSnapshotDuration is the amount of time Replica.Snapshot
	// will wait before switching to asynchronous mode. Zero is a good
	// choice for production but non-zero values can speed up tests.
	// (This only blocks on the first attempt; it will not block a
	// second time if the generation is still in progress).
	BlockingSnapshotDuration time.Duration

	// AsyncSnapshotMaxAge is the maximum amount of time that an
	// asynchronous snapshot will be held while waiting for raft to pick
	// it up (counted from when the snapshot generation is completed).
	AsyncSnapshotMaxAge time.Duration

	TestingKnobs StoreTestingKnobs

	// rangeLeaseActiveDuration is the duration of the active period of leader
	// leases requested.
	rangeLeaseActiveDuration time.Duration

	// rangeLeaseRenewalDuration specifies a time interval at the end of the
	// active lease interval (i.e. bounded to the right by the start of the stasis
	// period) during which operations will trigger an asynchronous renewal of the
	// lease.
	rangeLeaseRenewalDuration time.Duration
}

// StoreTestingKnobs is a part of the context used to control parts of the system.
type StoreTestingKnobs struct {
	// A callback to be called when executing every replica command.
	// If your filter is not idempotent, consider wrapping it in a
	// ReplayProtectionFilterWrapper.
	TestingCommandFilter storagebase.ReplicaCommandFilter
	// A callback to be called instead of panicking due to a
	// checksum mismatch in VerifyChecksum()
	BadChecksumPanic func([]ReplicaSnapshotDiff)
	// Disables the use of one phase commits.
	DisableOnePhaseCommits bool
	// A hack to manipulate the clock before sending a batch request to a replica.
	// TODO(kaneda): This hook is not encouraged to use. Get rid of it once
	// we make TestServer take a ManualClock.
	ClockBeforeSend func(*hlc.Clock, roachpb.BatchRequest)
	// LeaseTransferBlockedOnExtensionEvent, if set, is called when
	// replica.TransferLease() encounters an in-progress lease extension.
	// nextLeader is the replica that we're trying to transfer the lease to.
	LeaseTransferBlockedOnExtensionEvent func(nextLeader roachpb.ReplicaDescriptor)
	// DisableSplitQueue disables the split queue.
	DisableSplitQueue bool
	// DisableReplicateQueue disables the replication queue.
	DisableReplicateQueue bool
	// DisableScanner disables the replica scanner.
	DisableScanner bool
}

var _ base.ModuleTestingKnobs = &StoreTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*StoreTestingKnobs) ModuleTestingKnobs() {}

type storeMetrics struct {
	registry *metric.Registry

	// Range data metrics.
	replicaCount                 *metric.Counter // Does not include reserved replicas.
	reservedReplicaCount         *metric.Counter
	leaderRangeCount             *metric.Gauge
	replicatedRangeCount         *metric.Gauge
	replicationPendingRangeCount *metric.Gauge
	availableRangeCount          *metric.Gauge

	// Lease data metrics.
	leaseRequestSuccessCount *metric.Counter
	leaseRequestErrorCount   *metric.Counter

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
	reserved        *metric.Counter
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
	rdbReadAmplification        *metric.Gauge

	// Range event metrics.
	rangeSplits                     *metric.Counter
	rangeAdds                       *metric.Counter
	rangeRemoves                    *metric.Counter
	rangeSnapshotsGenerated         *metric.Counter
	rangeSnapshotsNormalApplied     *metric.Counter
	rangeSnapshotsPreemptiveApplied *metric.Counter

	// Stats for efficient merges.
	// TODO(mrtracy): This should be removed as part of #4465. This is only
	// maintained to keep the current structure of StatusSummaries; it would be
	// better to convert the Gauges above into counters which are adjusted
	// accordingly.
	mu    syncutil.Mutex
	stats enginepb.MVCCStats
}

func newStoreMetrics() *storeMetrics {
	storeRegistry := metric.NewRegistry()
	return &storeMetrics{
		registry:                     storeRegistry,
		replicaCount:                 storeRegistry.Counter("replicas"),
		reservedReplicaCount:         storeRegistry.Counter("replicas.reserved"),
		leaderRangeCount:             storeRegistry.Gauge("ranges.leader"),
		replicatedRangeCount:         storeRegistry.Gauge("ranges.replicated"),
		replicationPendingRangeCount: storeRegistry.Gauge("ranges.replication-pending"),
		availableRangeCount:          storeRegistry.Gauge("ranges.available"),
		leaseRequestSuccessCount:     storeRegistry.Counter("leases.success"),
		leaseRequestErrorCount:       storeRegistry.Counter("leases.error"),
		liveBytes:                    storeRegistry.Gauge("livebytes"),
		keyBytes:                     storeRegistry.Gauge("keybytes"),
		valBytes:                     storeRegistry.Gauge("valbytes"),
		intentBytes:                  storeRegistry.Gauge("intentbytes"),
		liveCount:                    storeRegistry.Gauge("livecount"),
		keyCount:                     storeRegistry.Gauge("keycount"),
		valCount:                     storeRegistry.Gauge("valcount"),
		intentCount:                  storeRegistry.Gauge("intentcount"),
		intentAge:                    storeRegistry.Gauge("intentage"),
		gcBytesAge:                   storeRegistry.Gauge("gcbytesage"),
		lastUpdateNanos:              storeRegistry.Gauge("lastupdatenanos"),
		capacity:                     storeRegistry.Gauge("capacity"),
		available:                    storeRegistry.Gauge("capacity.available"),
		reserved:                     storeRegistry.Counter("capacity.reserved"),
		sysBytes:                     storeRegistry.Gauge("sysbytes"),
		sysCount:                     storeRegistry.Gauge("syscount"),

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
		rdbReadAmplification:        storeRegistry.Gauge("rocksdb.read-amplification"),

		// Range event metrics.
		rangeSplits:                     storeRegistry.Counter("range.splits"),
		rangeAdds:                       storeRegistry.Counter("range.adds"),
		rangeRemoves:                    storeRegistry.Counter("range.removes"),
		rangeSnapshotsGenerated:         storeRegistry.Counter("range.snapshots.generated"),
		rangeSnapshotsNormalApplied:     storeRegistry.Counter("range.snapshots.normal-applied"),
		rangeSnapshotsPreemptiveApplied: storeRegistry.Counter("range.snapshots.preemptive-applied"),
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

func (sm *storeMetrics) updateReplicationGauges(leaders, replicated, pending, available int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.leaderRangeCount.Update(leaders)
	sm.replicatedRangeCount.Update(replicated)
	sm.replicationPendingRangeCount.Update(pending)
	sm.availableRangeCount.Update(available)
}

func (sm *storeMetrics) addMVCCStats(stats enginepb.MVCCStats) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.stats.Add(stats)
	sm.updateMVCCGaugesLocked()
}

func (sm *storeMetrics) subtractMVCCStats(stats enginepb.MVCCStats) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.stats.Subtract(stats)
	sm.updateMVCCGaugesLocked()
}

func (sm *storeMetrics) updateRocksDBStats(stats engine.Stats) {
	// We do not grab a lock here, because it's not possible to get a point-in-
	// time snapshot of RocksDB stats. Retrieving RocksDB stats doesn't grab any
	// locks, and there's no way to retrieve multiple stats in a single operation.
	sm.rdbBlockCacheHits.Update(stats.BlockCacheHits)
	sm.rdbBlockCacheMisses.Update(stats.BlockCacheMisses)
	sm.rdbBlockCacheUsage.Update(stats.BlockCacheUsage)
	sm.rdbBlockCachePinnedUsage.Update(stats.BlockCachePinnedUsage)
	sm.rdbBloomFilterPrefixUseful.Update(stats.BloomFilterPrefixUseful)
	sm.rdbBloomFilterPrefixChecked.Update(stats.BloomFilterPrefixChecked)
	sm.rdbMemtableHits.Update(stats.MemtableHits)
	sm.rdbMemtableMisses.Update(stats.MemtableMisses)
	sm.rdbMemtableTotalSize.Update(stats.MemtableTotalSize)
	sm.rdbFlushes.Update(stats.Flushes)
	sm.rdbCompactions.Update(stats.Compactions)
	sm.rdbTableReadersMemEstimate.Update(stats.TableReadersMemEstimate)
}

func (sm *storeMetrics) leaseRequestComplete(success bool) {
	if success {
		sm.leaseRequestSuccessCount.Inc(1)
	} else {
		sm.leaseRequestErrorCount.Inc(1)
	}
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
	if (sc.RangeRetryOptions == retry.Options{}) {
		sc.RangeRetryOptions = base.DefaultRetryOptions()
	}

	if sc.RaftTickInterval == 0 {
		sc.RaftTickInterval = base.DefaultRaftTickInterval
	}
	if sc.RaftHeartbeatIntervalTicks == 0 {
		sc.RaftHeartbeatIntervalTicks = defaultHeartbeatIntervalTicks
	}
	if sc.RaftElectionTimeoutTicks == 0 {
		sc.RaftElectionTimeoutTicks = defaultRaftElectionTimeoutTicks
	}
	if sc.AsyncSnapshotMaxAge == 0 {
		sc.AsyncSnapshotMaxAge = defaultAsyncSnapshotMaxAge
	}

	raftElectionTimeout := time.Duration(sc.RaftElectionTimeoutTicks) * sc.RaftTickInterval
	sc.rangeLeaseActiveDuration = rangeLeaseRaftElectionTimeoutMultiplier * raftElectionTimeout
	sc.rangeLeaseRenewalDuration = sc.rangeLeaseActiveDuration / rangeLeaseRenewalDivisor
}

// NewStore returns a new instance of a store.
func NewStore(ctx StoreContext, eng engine.Engine, nodeDesc *roachpb.NodeDescriptor) *Store {
	// TODO(tschottdorf) find better place to set these defaults.
	ctx.setDefaults()

	if !ctx.Valid() {
		panic(fmt.Sprintf("invalid store configuration: %+v", &ctx))
	}

	s := &Store{
		ctx:          ctx,
		db:           ctx.DB, // TODO(tschottdorf) remove redundancy.
		engine:       eng,
		allocator:    MakeAllocator(ctx.StorePool, ctx.AllocatorOptions),
		nodeDesc:     nodeDesc,
		wakeRaftLoop: make(chan struct{}, 1),
		metrics:      newStoreMetrics(),
	}
	s.intentResolver = newIntentResolver(s)
	s.drainLeases.Store(false)

	s.mu.Lock()
	s.mu.replicas = map[roachpb.RangeID]*Replica{}
	s.mu.replicasByKey = btree.New(64 /* degree */)
	s.mu.uninitReplicas = map[roachpb.RangeID]*Replica{}
	s.pendingRaftGroups.value = map[roachpb.RangeID]struct{}{}

	s.mu.Unlock()

	if s.ctx.Gossip != nil {
		// Add range scanner and configure with queues.
		s.scanner = newReplicaScanner(ctx.ScanInterval, ctx.ScanMaxIdleTime, newStoreRangeSet(s))
		s.gcQueue = newGCQueue(s, s.ctx.Gossip)
		s.splitQueue = newSplitQueue(s, s.db, s.ctx.Gossip)
		s.verifyQueue = newVerifyQueue(s, s.ctx.Gossip, s.ReplicaCount)
		s.replicateQueue = newReplicateQueue(s, s.ctx.Gossip, s.allocator, s.ctx.Clock, s.ctx.AllocatorOptions)
		s.replicaGCQueue = newReplicaGCQueue(s, s.db, s.ctx.Gossip)
		s.raftLogQueue = newRaftLogQueue(s, s.db, s.ctx.Gossip)
		s.scanner.AddQueues(s.gcQueue, s.splitQueue, s.verifyQueue, s.replicateQueue, s.replicaGCQueue, s.raftLogQueue)

		// Add consistency check scanner.
		s.consistencyScanner = newReplicaScanner(ctx.ConsistencyCheckInterval, 0, newStoreRangeSet(s))
		s.replicaConsistencyQueue = newReplicaConsistencyQueue(s, s.ctx.Gossip)
		s.consistencyScanner.AddQueues(s.replicaConsistencyQueue)
	}

	if ctx.TestingKnobs.DisableSplitQueue {
		s.setSplitQueueActive(false)
	}
	if ctx.TestingKnobs.DisableReplicateQueue {
		s.setReplicateQueueActive(false)
	}
	if ctx.TestingKnobs.DisableScanner {
		s.setScannerActive(false)
	}

	return s
}

// String formats a store for debug output.
func (s *Store) String() string {
	return fmt.Sprintf("store=%d:%d", s.Ident.NodeID, s.Ident.StoreID)
}

// DrainLeases (when called with 'true') prevents all of the Store's
// Replicas from acquiring or extending range leases and waits until all of
// them have expired. If an error is returned, the draining state is still
// active, but there may be active leases held by some of the Store's Replicas.
// When called with 'false', returns to the normal mode of operation.
func (s *Store) DrainLeases(drain bool) error {
	s.drainLeases.Store(drain)
	if !drain {
		return nil
	}

	return util.RetryForDuration(10*s.ctx.rangeLeaseActiveDuration, func() error {
		var err error
		now := s.Clock().Now()
		newStoreRangeSet(s).Visit(func(r *Replica) bool {
			lease, nextLease := r.getLease()
			// If we own an active lease or we're trying to obtain a lease
			// (and that request is fresh enough), wait.
			if (lease.OwnedBy(s.StoreID()) && lease.Covers(now)) ||
				(nextLease != nil && nextLease.Covers(now)) {

				err = fmt.Errorf("replica %s still has an active lease", r)
			}
			return err == nil // break on error
		})
		return err
	})
}

// context returns a base context to pass along with commands being executed,
// derived from the supplied context (which is not allowed to be nil).
func (s *Store) context(ctx context.Context) context.Context {
	if ctx == nil {
		panic("ctx cannot be nil")
	}
	return ctx // TODO(tschottdorf): see #1779
}

// IsStarted returns true if the Store has been started.
func (s *Store) IsStarted() bool {
	return atomic.LoadInt32(&s.started) == 1
}

// IterateRangeDescriptors calls the provided function with each descriptor
// from the provided Engine. The return values of this method and fn have
// semantics similar to engine.MVCCIterate.
func IterateRangeDescriptors(
	eng engine.Reader, fn func(desc roachpb.RangeDescriptor) (bool, error),
) error {
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

	_, err := engine.MVCCIterate(context.Background(), eng, start, end, hlc.MaxTimestamp, false /* !consistent */, nil, /* txn */
		false /* !reverse */, kvToDesc)
	return err
}

func (s *Store) migrate(ctx context.Context, desc roachpb.RangeDescriptor) {
	batch := s.engine.NewBatch()
	if err := migrate7310And6991(ctx, batch, desc); err != nil {
		log.Fatal(ctx, errors.Wrap(err, "during migration"))
	}
	if err := batch.Commit(); err != nil {
		log.Fatal(ctx, errors.Wrap(err, "could not migrate Raft state"))
	}
}

// Start the engine, set the GC and read the StoreIdent.
func (s *Store) Start(stopper *stop.Stopper) error {
	s.stopper = stopper
	ctx := s.context(context.TODO())

	// Add a closer for the various scanner queues, needed to properly clean up
	// the event logs.
	s.stopper.AddCloser(stop.CloserFn(func() {
		if q := s.gcQueue; q != nil {
			q.Close()
		}
		if q := s.splitQueue; q != nil {
			q.Close()
		}
		if q := s.verifyQueue; q != nil {
			q.Close()
		}
		if q := s.replicateQueue; q != nil {
			q.Close()
		}
		if q := s.replicaGCQueue; q != nil {
			q.Close()
		}
		if q := s.raftLogQueue; q != nil {
			q.Close()
		}
		if q := s.replicaConsistencyQueue; q != nil {
			q.Close()
		}
	}))

	// Add the bookie to the store.
	s.bookie = newBookie(
		s.ctx.Clock,
		s.stopper,
		s.metrics,
		envutil.EnvOrDefaultDuration("reservation_timeout", ttlStoreGossip),
	)

	if s.Ident.NodeID == 0 {
		// Open engine (i.e. initialize RocksDB database). "NodeID != 0"
		// implies the engine has already been opened.
		if err := s.engine.Open(); err != nil {
			return err
		}

		// Read store ident and return a not-bootstrapped error if necessary.
		ok, err := engine.MVCCGetProto(ctx, s.engine, keys.StoreIdentKey(), hlc.ZeroTimestamp, true, nil, &s.Ident)
		if err != nil {
			return err
		} else if !ok {
			return &NotBootstrappedError{}
		}
	}

	// If the nodeID is 0, it has not be assigned yet.
	if s.nodeDesc.NodeID != 0 && s.Ident.NodeID != s.nodeDesc.NodeID {
		return errors.Errorf("node id:%d does not equal the one in node descriptor:%d", s.Ident.NodeID, s.nodeDesc.NodeID)
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
		if _, ok := desc.GetReplicaDescriptor(s.StoreID()); !ok {
			// We are no longer a member of the range, but we didn't GC
			// the replica before shutting down. Destroy the replica now
			// to avoid creating a new replica without a valid replica ID
			// (which is necessary to have a non-nil raft group)
			return false, s.destroyReplicaData(&desc)
		}
		if !desc.IsInitialized() {
			return false, errors.Errorf("found uninitialized RangeDescriptor: %+v", desc)
		}
		s.migrate(ctx, desc)

		rng, err := NewReplica(&desc, s, 0)
		if err != nil {
			return false, err
		}
		if err = s.addReplicaInternalLocked(rng); err != nil {
			return false, err
		}
		// Add this range and its stats to our counter.
		s.metrics.replicaCount.Inc(1)
		s.metrics.addMVCCStats(rng.GetMVCCStats())
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
	s.ctx.Transport.Listen(s.StoreID(), s.handleRaftMessage)
	s.processRaft()

	doneUnfreezing := make(chan struct{})
	if s.stopper.RunAsyncTask(func() {
		defer close(doneUnfreezing)
		sem := make(chan struct{}, 512)
		var wg sync.WaitGroup // wait for unfreeze goroutines
		var unfrozen int64    // updated atomically
		newStoreRangeSet(s).Visit(func(r *Replica) bool {
			r.mu.Lock()
			frozen := r.mu.state.Frozen
			r.mu.Unlock()
			if !frozen {
				return true
			}
			wg.Add(1)
			if s.stopper.RunLimitedAsyncTask(sem, func() {
				defer wg.Done()
				desc := r.Desc()
				var ba roachpb.BatchRequest
				fReq := roachpb.ChangeFrozenRequest{
					Span: roachpb.Span{
						Key:    desc.StartKey.AsRawKey(),
						EndKey: desc.EndKey.AsRawKey(),
					},
					Frozen:      false,
					MustVersion: build.GetInfo().Tag,
				}
				ba.Add(&fReq)
				if _, pErr := r.Send(ctx, ba); pErr != nil {
					log.Errorf(ctx, "could not unfreeze Range %s on startup: %s", r, pErr)
				} else {
					// We don't use the returned RangesAffected (0 or 1) for
					// counting. One of the other Replicas may have beaten us
					// to it, but it is still fair to count this as "our"
					// success; otherwise, the logged count will be distributed
					// across various nodes' logs.
					atomic.AddInt64(&unfrozen, 1)
				}
			}) != nil {
				wg.Done()
			}
			return true
		})
		wg.Wait()
		if unfrozen > 0 {
			log.Infof(context.TODO(), "reactivated %d frozen Ranges", unfrozen)
		}
	}) != nil {
		close(doneUnfreezing)
	}
	// We don't want to jump into gossiping too early - if the first range is
	// frozen, that means waiting for the next attempt to happen. Instead,
	// wait for a little bit and if things take too long, let the Gossip
	// loop figure it out.
	select {
	case <-doneUnfreezing:
	case <-time.After(10 * time.Second):
	}

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
		s.startGossip(ctx)

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

		// Run metrics computation up front to populate initial statistics.
		if err := s.ComputeMetrics(); err != nil {
			return err
		}
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
func (s *Store) startGossip(ctx context.Context) {
	// Periodic updates run in a goroutine and signal a WaitGroup upon completion
	// of their first iteration.
	s.initComplete.Add(2)
	s.stopper.RunWorker(func() {
		// Run the first time without waiting for the Ticker and signal the WaitGroup.
		if err := s.maybeGossipFirstRange(); err != nil {
			log.Warningf(ctx, "error gossiping first range data: %s", err)
		}
		s.initComplete.Done()
		ticker := time.NewTicker(sentinelGossipInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := s.maybeGossipFirstRange(); err != nil {
					log.Warningf(ctx, "error gossiping first range data: %s", err)
				}
			case <-s.stopper.ShouldStop():
				return
			}
		}
	})

	s.stopper.RunWorker(func() {
		if err := s.maybeGossipSystemConfig(); err != nil {
			log.Warningf(ctx, "error gossiping system config: %s", err)
		}
		s.initComplete.Done()
		ticker := time.NewTicker(configGossipInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := s.maybeGossipSystemConfig(); err != nil {
					log.Warningf(ctx, "error gossiping system config: %s", err)
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
// of the range lease is not known. This can happen on lease command
// timeouts. The retry loop makes sure we try hard to keep asking for
// the lease instead of waiting for the next sentinelGossipInterval
// to transpire.
func (s *Store) maybeGossipFirstRange() error {
	retryOptions := base.DefaultRetryOptions()
	retryOptions.Closer = s.stopper.ShouldStop()
	for loop := retry.Start(retryOptions); loop.Next(); {
		rng := s.LookupReplica(roachpb.RKeyMin, nil)
		if rng != nil {
			pErr := rng.maybeGossipFirstRange()
			if nlErr, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); !ok || nlErr.LeaseHolder != nil {
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
func (s *Store) GossipStore(ctx context.Context) error {
	storeDesc, err := s.Descriptor()
	if err != nil {
		return errors.Wrapf(err, "problem getting store descriptor for store %+v", s.Ident)
	}
	// Unique gossip key per store.
	gossipStoreKey := gossip.MakeStoreKey(storeDesc.StoreID)
	// Gossip store descriptor.
	return s.ctx.Gossip.AddInfoProto(gossipStoreKey, storeDesc, ttlStoreGossip)
}

// GossipDeadReplicas broadcasts the stores dead replicas on the gossip network.
func (s *Store) GossipDeadReplicas(ctx context.Context) error {
	deadReplicas := s.deadReplicas()
	// Don't gossip if there's nothing to gossip.
	if len(deadReplicas.Replicas) == 0 {
		return nil
	}
	// Unique gossip key per store.
	key := gossip.MakeDeadReplicasKey(s.StoreID())
	// Gossip dead replicas.
	return s.ctx.Gossip.AddInfoProto(key, &deadReplicas, ttlStoreGossip)
}

// Bootstrap writes a new store ident to the underlying engine. To
// ensure that no crufty data already exists in the engine, it scans
// the engine contents before writing the new store ident. The engine
// should be completely empty. It returns an error if called on a
// non-empty engine.
func (s *Store) Bootstrap(ident roachpb.StoreIdent, stopper *stop.Stopper) error {
	if s.Ident.NodeID != 0 {
		return errors.Errorf("engine already bootstrapped")
	}
	if err := s.engine.Open(); err != nil {
		return err
	}
	s.Ident = ident
	kvs, err := engine.Scan(s.engine,
		engine.MakeMVCCMetadataKey(roachpb.Key(roachpb.RKeyMin)),
		engine.MakeMVCCMetadataKey(roachpb.Key(roachpb.RKeyMax)), 10)
	if err != nil {
		return errors.Errorf("store %s: unable to access: %s", s.engine, err)
	} else if len(kvs) > 0 {
		// See if this is an already-bootstrapped store.
		ok, err := engine.MVCCGetProto(context.Background(), s.engine, keys.StoreIdentKey(), hlc.ZeroTimestamp, true, nil, &s.Ident)
		if err != nil {
			return errors.Errorf("store %s is non-empty but cluster ID could not be determined: %s", s.engine, err)
		}
		if ok {
			return errors.Errorf("store %s already belongs to cockroach cluster %s", s.engine, s.Ident.ClusterID)
		}
		keyVals := []string{}
		for _, kv := range kvs {
			keyVals = append(keyVals, fmt.Sprintf("%s: %q", kv.Key, kv.Value))
		}
		return errors.Errorf("store %s is non-empty but does not contain store metadata (first %d key/values: %s)", s.engine, len(keyVals), keyVals)
	}
	err = engine.MVCCPutProto(context.Background(), s.engine, nil, keys.StoreIdentKey(), hlc.ZeroTimestamp, nil, &s.Ident)
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
	s.visitReplicasLocked(start, roachpb.RKeyMax, func(repl *Replica) bool {
		rng = repl
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
	s.visitReplicasLocked(rngDesc.StartKey, roachpb.RKeyMax, func(repl *Replica) bool {
		rng = repl
		return false
	})

	if rng != nil && rng.Desc().StartKey.Less(rngDesc.EndKey) {
		return true
	}

	return false
}

// visitReplicasLocked will call iterator for every replica on the store which
// contains any keys in the span between startKey and endKey. Iteration will be
// in ascending order. Iteration can be stopped early by returning false from
// iterator.
func (s *Store) visitReplicasLocked(startKey, endKey roachpb.RKey, iterator func(r *Replica) bool) {
	// Iterate over replicasByKey to visit all ranges containing keys in the
	// specified range. We use startKey.Next() because btree's Ascend methods
	// are inclusive of the start bound and exclusive of the end bound, but
	// ranges are stored in the BTree by EndKey; in cockroach, end keys have the
	// opposite behavior (a range's EndKey is contained by the subsequent
	// range). We want visitReplicasLocked to match cockroach's behavior; using
	// startKey.Next(), will ignore a range which has EndKey exactly equal to
	// the supplied startKey. Iteration ends when all ranges are exhausted, or
	// the next range contains no keys in the supplied span.
	s.mu.replicasByKey.AscendGreaterOrEqual(rangeBTreeKey(startKey.Next()),
		func(item btree.Item) bool {
			repl := item.(*Replica)
			if !repl.Desc().StartKey.Less(endKey) {
				// This properly checks if this range contains any keys in the supplied span.
				return false
			}
			return iterator(repl)
		})
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
	ms := &enginepb.MVCCStats{}
	now := s.ctx.Clock.Now()
	ctx := context.Background()

	// Range descriptor.
	if err := engine.MVCCPutProto(ctx, batch, ms, keys.RangeDescriptorKey(desc.StartKey), now, nil, desc); err != nil {
		return err
	}
	// Replica GC & Verification timestamps.
	if err := engine.MVCCPutProto(ctx, batch, nil /* ms */, keys.RangeLastReplicaGCTimestampKey(desc.RangeID), hlc.ZeroTimestamp, nil, &now); err != nil {
		return err
	}
	if err := engine.MVCCPutProto(ctx, batch, nil /* ms */, keys.RangeLastVerificationTimestampKey(desc.RangeID), hlc.ZeroTimestamp, nil, &now); err != nil {
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
	// Set range stats.
	if err := engine.AccountForSelf(ms, desc.RangeID); err != nil {
		return err
	}

	updatedMS, err := writeInitialState(ctx, batch, *ms, *desc)
	if err != nil {
		return err
	}
	*ms = updatedMS

	return batch.Commit()
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

// IsDrainingLeases accessor.
func (s *Store) IsDrainingLeases() bool {
	return s.drainLeases.Load().(bool)
}

// NewRangeDescriptor creates a new descriptor based on start and end
// keys and the supplied roachpb.Replicas slice. It allocates a new
// range ID and returns a RangeDescriptor whose Replicas are a copy
// of the supplied replicas slice, with appropriate ReplicaIDs assigned.
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

// splitTriggerPostCommit is the part of the split trigger which coordinates
// the actual split with the Store.
//
// TODO(tschottdorf): Want to merge this with SplitRange, but some legacy
// testing code calls SplitRange directly.
func splitTriggerPostCommit(
	ctx context.Context,
	deltaMS enginepb.MVCCStats,
	split *roachpb.SplitTrigger,
	r *Replica,
) {
	// Create the new Replica representing the right side of the split.
	// Our error handling options at this point are very limited, but
	// we need to do this after our batch has committed.
	rightRng, err := NewReplica(&split.RightDesc, r.store, 0)
	if err != nil {
		panic(err)
	}

	// Copy the timestamp cache into the RHS range.
	r.mu.Lock()
	rightRng.mu.Lock()
	r.mu.tsCache.MergeInto(rightRng.mu.tsCache, true /* clear */)
	rightRng.mu.Unlock()
	r.mu.Unlock()
	log.Trace(ctx, "copied timestamp cache")

	// Add the RHS replica to the store. This step atomically updates
	// the EndKey of the LHS replica and also adds the RHS replica
	// to the store's replica map.
	if err := r.store.SplitRange(r, rightRng); err != nil {
		// Our in-memory state has diverged from the on-disk state.
		log.Fatalf(ctx, "%s: failed to update Store after split: %s", r, err)
	}

	// Update store stats with difference in stats before and after split.
	r.store.metrics.addMVCCStats(deltaMS)

	// If the range was not properly replicated before the split, the
	// replicate queue may not have picked it up (due to the need for
	// a split). Enqueue both left and right halves to speed up a
	// potentially necessary replication. See #7022 and #7800.
	now := r.store.Clock().Now()
	r.store.replicateQueue.MaybeAdd(r, now)
	r.store.replicateQueue.MaybeAdd(rightRng, now)

	// To avoid leaving the RHS range unavailable as it waits to elect
	// its leader, one (and only one) of the nodes should start an
	// election as soon as the split is processed.
	//
	// If there is only one replica, raft instantly makes it the leader.
	// Otherwise, we must trigger a campaign here.
	//
	// TODO(bdarnell): The asynchronous campaign can cause a problem
	// with merges, by recreating a replica that should have been
	// destroyed. This will probably be addressed as a part of the
	// general work to be done for merges
	// (https://github.com/cockroachdb/cockroach/issues/2433), but for
	// now we're safe because we only perform the asynchronous
	// campaign when there are multiple replicas, and we only perform
	// merges in testing scenarios with a single replica.
	// Note that in multi-node scenarios the async campaign is safe
	// because it has exactly the same behavior as an incoming message
	// from another node; the problem here is only with merges.
	rightRng.mu.Lock()
	shouldCampaign := rightRng.mu.state.Lease.OwnedBy(r.store.StoreID())
	rightRng.mu.Unlock()
	if len(split.RightDesc.Replicas) > 1 && shouldCampaign {
		// Schedule the campaign a short time in the future. As
		// followers process the split, they destroy and recreate their
		// raft groups, which can cause messages to be dropped. In
		// general a shorter delay (perhaps all the way down to zero) is
		// better in production, because the race is rare and the worst
		// case scenario is that we simply wait for an election timeout.
		// However, the test for this feature disables election timeouts
		// and relies solely on this campaign trigger, so it is unacceptably
		// flaky without a bit of a delay.
		//
		// Note: you must not use the context inside of this task since it may
		// contain a finished trace by the time it runs.
		if err := r.store.stopper.RunAsyncTask(func() {
			time.Sleep(10 * time.Millisecond)
			// Make sure that rightRng hasn't been removed.
			replica, err := r.store.GetReplica(rightRng.RangeID)
			if err != nil {
				if _, ok := err.(*roachpb.RangeNotFoundError); ok {
					log.Infof(ctx, "%s: RHS replica %d removed before campaigning",
						r, r.mu.replicaID)
				} else {
					log.Infof(ctx, "%s: RHS replica %d unable to campaign: %s",
						r, r.mu.replicaID, err)
				}
				return
			}

			if err := replica.withRaftGroup(func(raftGroup *raft.RawNode) error {
				if err := raftGroup.Campaign(); err != nil {
					log.Warningf(ctx, "%s: error %v", r, err)
				}
				return nil
			}); err != nil {
				panic(err)
			}
		}); err != nil {
			log.Warningf(ctx, "%s: error %v", r, err)
			return
		}
	}
}

// SplitRange shortens the original range to accommodate the new
// range. The new range is added to the ranges map and the replicasByKey
// btree. processRaftMu must be held.
//
// This is only called from the split trigger in the context of the execution
// of a Raft command (so processRaftMu *is* held).
func (s *Store) SplitRange(origRng, newRng *Replica) error {
	origDesc := origRng.Desc()
	newDesc := newRng.Desc()

	if !bytes.Equal(origDesc.EndKey, newDesc.EndKey) ||
		bytes.Compare(origDesc.StartKey, newDesc.StartKey) >= 0 {
		return errors.Errorf("orig range is not splittable by new range: %+v, %+v", origDesc, newDesc)
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
		return errors.Errorf("couldn't find range %s in replicasByKey btree", origRng)
	}

	copyDesc := *origDesc
	copyDesc.EndKey = append([]byte(nil), newDesc.StartKey...)
	origRng.setDescWithoutProcessUpdate(&copyDesc)

	if s.mu.replicasByKey.ReplaceOrInsert(origRng) != nil {
		return errors.Errorf("couldn't insert range %v in replicasByKey btree", origRng)
	}

	if err := s.addReplicaInternalLocked(newRng); err != nil {
		return errors.Errorf("couldn't insert range %v in replicasByKey btree: %s", newRng, err)
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

// MergeRange expands the subsuming range to absorb the subsumed range. This
// merge operation will fail if the two ranges are not collocated on the same
// store. Must be called (perhaps indirectly) from the processRaft goroutine.
func (s *Store) MergeRange(subsumingRng *Replica, updatedEndKey roachpb.RKey, subsumedRangeID roachpb.RangeID) error {
	subsumingDesc := subsumingRng.Desc()

	if !subsumingDesc.EndKey.Less(updatedEndKey) {
		return errors.Errorf("the new end key is not greater than the current one: %+v <= %+v",
			updatedEndKey, subsumingDesc.EndKey)
	}

	subsumedRng, err := s.GetReplica(subsumedRangeID)
	if err != nil {
		return errors.Errorf("could not find the subsumed range: %d", subsumedRangeID)
	}
	subsumedDesc := subsumedRng.Desc()

	if !replicaSetsEqual(subsumedDesc.Replicas, subsumingDesc.Replicas) {
		return errors.Errorf("ranges are not on the same replicas sets: %+v != %+v",
			subsumedDesc.Replicas, subsumingDesc.Replicas)
	}

	// Remove and destroy the subsumed range. Note that we were called
	// (indirectly) from the processRaft goroutine so we must call
	// removeReplicaImpl directly to avoid deadlocking on processRaftMu.
	if err := s.removeReplicaImpl(subsumedRng, *subsumedDesc, false); err != nil {
		return errors.Errorf("cannot remove range %s", err)
	}

	// Update the end key of the subsuming range.
	copy := *subsumingDesc
	copy.EndKey = updatedEndKey
	return subsumingRng.setDesc(&copy)
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
		return errors.Errorf("attempted to add uninitialized range %s", rng)
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
		return errors.Errorf("range for key %v already exists in replicasByKey btree",
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
// then. If `destroy` is true, all data belonging to the replica will be
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
	if repDesc, ok := desc.GetReplicaDescriptor(s.StoreID()); ok && repDesc.ReplicaID >= origDesc.NextReplicaID {
		return errors.Errorf("cannot remove replica %s; replica ID has changed (%s >= %s)",
			rep, repDesc.ReplicaID, origDesc.NextReplicaID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.mu.replicas, rep.RangeID)
	if s.mu.replicasByKey.Delete(rep) == nil {
		return errors.Errorf("couldn't find range in replicasByKey btree")
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
	iter := NewReplicaDataIterator(desc, s.Engine(), false /* !replicatedOnly */)
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
	if err := engine.MVCCPutProto(context.Background(), batch, nil, tombstoneKey, hlc.ZeroTimestamp, nil, tombstone); err != nil {
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
		return errors.Errorf("attempted to process uninitialized range %s", rng)
	}

	rangeID := rng.RangeID

	if _, ok := s.mu.uninitReplicas[rangeID]; !ok {
		// Do nothing if the range has already been initialized.
		return nil
	}
	delete(s.mu.uninitReplicas, rangeID)

	if s.hasOverlappingReplicaLocked(rng.Desc()) {
		return rangeAlreadyExists{rng}
	}
	if exRngItem := s.mu.replicasByKey.ReplaceOrInsert(rng); exRngItem != nil {
		return errors.Errorf("range for key %v already exists in replicasByKey btree",
			(exRngItem.(*Replica)).getKey())
	}

	// Add the range and its current stats into metrics.
	s.metrics.replicaCount.Inc(1)

	return nil
}

// NewSnapshot creates a new snapshot engine.
func (s *Store) NewSnapshot() engine.Reader {
	return s.engine.NewSnapshot()
}

// AcquireRaftSnapshot returns true if a new raft snapshot can start.
// If true is returned, the caller MUST call ReleaseRaftSnapshot.
func (s *Store) AcquireRaftSnapshot() bool {
	return atomic.CompareAndSwapInt32(&s.hasActiveRaftSnapshot, 0, 1)
}

// ReleaseRaftSnapshot decrements the count of active snapshots.
func (s *Store) ReleaseRaftSnapshot() {
	atomic.SwapInt32(&s.hasActiveRaftSnapshot, 0)
}

// Attrs returns the attributes of the underlying store.
func (s *Store) Attrs() roachpb.Attributes {
	return s.engine.Attrs()
}

// Capacity returns the capacity of the underlying storage engine. Note that
// this does not include reservations.
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
func (s *Store) MVCCStats() enginepb.MVCCStats {
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

func (s *Store) deadReplicas() roachpb.StoreDeadReplicas {
	s.mu.Lock()
	defer s.mu.Unlock()

	var deadReplicas []roachpb.ReplicaIdent
	for rangeID, repl := range s.mu.replicas {
		repl.mu.Lock()
		destroyed := repl.mu.destroyed
		replicaID := repl.mu.replicaID
		repl.mu.Unlock()

		if destroyed != nil {
			deadReplicas = append(deadReplicas, roachpb.ReplicaIdent{
				RangeID: rangeID,
				Replica: roachpb.ReplicaDescriptor{
					NodeID:    s.Ident.NodeID,
					StoreID:   s.Ident.StoreID,
					ReplicaID: replicaID,
				},
			})
		}
	}

	return roachpb.StoreDeadReplicas{
		StoreID:  s.StoreID(),
		Replicas: deadReplicas,
	}
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
// transaction. In particular, the read (original) timestamp will be used for
// all reads _and writes_ (see the TxnMeta.OrigTimestamp for details).
//
// Should a transactional operation be forced to a higher timestamp (for
// instance due to the timestamp cache or finding a committed value in the path
// of one of its writes), the response will have a transaction set which should
// be used to update the client transaction.
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

	if s.ctx.TestingKnobs.ClockBeforeSend != nil {
		s.ctx.TestingKnobs.ClockBeforeSend(s.ctx.Clock, ba)
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
		log.Tracef(ctx, "executing %s", ba)
	} else {
		log.Tracef(ctx, "executing %d requests", len(ba.Requests))
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
		if !rng.IsInitialized() {
			// If we have an uninitialized copy of the range, then we are
			// probably a valid member of the range, we're just in the
			// process of getting our snapshot. If we returned
			// RangeNotFoundError, the client would invalidate its cache,
			// but we can be smarter: the replica that caused our
			// uninitialized replica to be created is most likely the
			// leader.
			return nil, roachpb.NewError(&roachpb.NotLeaseHolderError{
				RangeID:     ba.RangeID,
				LeaseHolder: rng.creatingReplica,
			})
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
		if _, ok := pErr.GetDetail().(*roachpb.WriteIntentError); ok && pErr.Index != nil {
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
			pErr = s.intentResolver.processWriteIntentError(ctx, pErr, rng, args, h, pushType)
			// Preserve the error index.
			pErr.Index = index
		}

		log.Tracef(ctx, "error: %T", pErr.GetDetail())
		switch t := pErr.GetDetail().(type) {
		case *roachpb.WriteIntentError:
			// If write intent error is resolved, exit retry/backoff loop to
			// immediately retry.
			if t.Resolved {
				r.Reset()
				if log.V(1) {
					log.Warning(ctx, pErr)
				}
				continue
			}
			if log.V(1) {
				log.Warning(ctx, pErr)
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

// maybeUpdateTransaction does a "query" push on the specified
// transaction to glean possible changes, such as a higher timestamp
// and/or priority. It turns out this is necessary while a request
// is in a backoff/retry loop pushing write intents as two txns
// can have circular dependencies where both are unable to push
// because they have different information about their own txns.
//
// The supplied transaction is updated with the results of the
// "query" push if possible.
func (s *Store) maybeUpdateTransaction(txn *roachpb.Transaction, now hlc.Timestamp) (*roachpb.Transaction, *roachpb.Error) {
	// Attempt to push the transaction which created the intent.
	b := client.Batch{}
	b.AddRawRequest(&roachpb.PushTxnRequest{
		Span: roachpb.Span{
			Key: txn.Key,
		},
		Now:       now,
		PusheeTxn: txn.TxnMeta,
		PushType:  roachpb.PUSH_QUERY,
	})
	if err := s.db.Run(&b); err != nil {
		// TODO(tschottdorf):
		// We shouldn't catch an error here (unless it's from the abort cache, in
		// which case we would not get the crucial information that we've been
		// aborted; instead we'll go around thinking we're still PENDING,
		// potentially caught in an infinite loop).  Same issue: we must not used
		// RunWithResponse on this level - we're trying to do internal kv stuff
		// through the public interface. Likely not exercised in tests, so I'd be
		// ok tackling this separately.
		//
		// Scenario:
		// - we're aborted and don't know if we have a read-write conflict
		// - the push above fails and we get a WriteIntentError
		// - we try to update our transaction (right here, and if we don't we might
		// be stuck in a race, that's why we do this - the txn proto we're using
		// might be outdated)
		// - query fails because our home range has the abort cache populated we catch
		// a TransactionAbortedError, but with a pending transaction (since we lose
		// the original txn, and you just use the txn we had...)
		//
		// so something is sketchy here, but it should all resolve nicely when we
		// don't use s.db for these internal requests any more.
		return nil, roachpb.NewError(err)
	}
	br := b.RawResponse()
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

// handleRaftMessage dispatches a raft message to the appropriate Replica. It
// requires that s.processRaftMu and s.mu are not held.
func (s *Store) handleRaftMessage(req *RaftMessageRequest) error {
	s.processRaftMu.Lock()
	defer s.processRaftMu.Unlock()

	ctx := s.context(context.Background())

	// Drop messages that come from a node that we believe was once
	// a member of the group but has been removed.
	s.mu.Lock()
	r, ok := s.mu.replicas[req.RangeID]
	s.mu.Unlock()
	if ok {
		found := false
		desc := r.Desc()
		for _, rep := range desc.Replicas {
			if rep.ReplicaID == req.FromReplica.ReplicaID {
				found = true
				break
			}
		}
		// It's not a current member of the group. Is it from the past?
		if !found && req.FromReplica.ReplicaID < desc.NextReplicaID {
			return errReplicaTooOld
		}
	}

	switch req.Message.Type {
	case raftpb.MsgSnap:
		if !s.canApplySnapshot(req.RangeID, req.Message.Snapshot) {
			// If the storage cannot accept the snapshot, drop it before
			// passing it to RawNode.Step, since our error handling
			// options past that point are limited.
			return nil
		}

	case raftpb.MsgHeartbeat:
		// TODO(bdarnell): handle coalesced heartbeats.
	}

	s.mu.Lock()
	// Lazily create the replica.
	r, err := s.getOrCreateReplicaLocked(req.RangeID, req.ToReplica.ReplicaID,
		req.FromReplica)
	// TODO(bdarnell): is it safe to release the store lock here?
	// It deadlocks to hold s.Mutex while calling raftGroup.Step.
	s.mu.Unlock()
	if err != nil {
		return err
	}
	r.setLastReplicaDescriptors(req)

	if req.ToReplica.ReplicaID == 0 {
		if req.Message.Type == raftpb.MsgSnap {
			// Allow snapshots to be applied to replicas before they are
			// members of the raft group (i.e. replicas with an ID of 0). This
			// is the only operation that can be performed before it is part of
			// the raft group.

			// Requiring that the Term is set in a message makes sure that we
			// get all of Raft's internal safety checks (it confuses messages
			// at term zero for internal messages). The sending side uses the
			// term from the snapshot itself, but we'll just check nonzero.
			if req.Message.Term == 0 {
				return errors.Errorf(
					"preemptive snapshot from term %d received with zero term",
					req.Message.Snapshot.Metadata.Term,
				)
			}
			// TODO(tschottdorf): A lot of locking of the individual Replica
			// going on below as well. I think that's more easily refactored
			// away; what really matters is that the Store doesn't do anything
			// else with that same Replica (or one that might conflict with us
			// while we still run). In effect, we'd want something like:
			//
			// 1. look up the snapshot's key range
			// 2. get an exclusive lock for operations on that key range from
			//    the store (or discard the snapshot)
			//    (at the time of writing, we have checked the key range in
			//    canApplySnapshot above, but there are concerns about two
			//    conflicting operations passing that check simultaneously,
			//    see #7830)
			// 3. do everything below (apply the snapshot through temp Raft group)
			// 4. release the exclusive lock on the snapshot's key range
			//
			// There are two future outcomes: Either we begin receiving
			// legitimate Raft traffic for this Range (hence learning the
			// ReplicaID and becoming a real Replica), or the Replica GC queue
			// decides that the ChangeReplicas as a part of which the
			// preemptive snapshot was sent has likely failed and removes both
			// in-memory and on-disk state.
			r.mu.Lock()
			groupExists := r.mu.internalRaftGroup != nil
			r.mu.Unlock()
			if groupExists {
				return errors.Errorf(
					"cannot apply preemptive snapshot, Raft group for %d already active",
					r.RangeID,
				)
			}
			appliedIndex, _, err := loadAppliedIndex(ctx, r.store.Engine(), r.RangeID)
			if err != nil {
				return err
			}
			raftGroup, err := raft.NewRawNode(
				newRaftConfig(
					raft.Storage(r),
					preemptiveSnapshotRaftGroupID,
					// We pass the "real" applied index here due to subtleties
					// arising in the case in which Raft discards the snapshot:
					// It would instruct us to apply entries, which would have
					// crashing potential for any choice of dummy value below.
					appliedIndex,
					r.store.ctx,
					&raftLogger{stringer: r},
				), nil)
			if err != nil {
				return err
			}
			// We have a Raft group; feed it the message.
			if err := raftGroup.Step(req.Message); err != nil {
				return errors.Wrap(err, "unable to process preemptive snapshot")
			}
			// In the normal case, the group should ask us to apply a snapshot.
			// If it doesn't, our snapshot was probably stale.
			var ready raft.Ready
			if raftGroup.HasReady() {
				ready = raftGroup.Ready()
			}
			if raft.IsEmptySnap(ready.Snapshot) {
				return errors.Errorf("preemptive snapshot discarded by Raft")
			}
			// Apply the snapshot, as Raft told us to.
			if err := r.applySnapshot(ctx, ready.Snapshot, ready.HardState); err != nil {
				return err
			}
			// At this point, the Replica has data but no ReplicaID. We hope
			// that it turns into a "real" Replica by means of receiving Raft
			// messages addressed to it with a ReplicaID, but if that doesn't
			// happen, at some point the Replica GC queue will have to grab it.
			return nil
		}
		// We disallow non-snapshot messages to replica ID 0. Note that
		// getOrCreateReplicaLocked disallows moving the replica ID backward, so
		// the only way we can get here is if the replica did not previously exist.
		if log.V(1) {
			log.Infof(context.TODO(), "refusing incoming Raft message %s for range %d from %+v to %+v",
				req.Message.Type, req.RangeID, req.FromReplica, req.ToReplica)
		}
		return errors.Errorf("cannot recreate replica that is not a member of its range (StoreID %s not found in range %d)",
			r.store.StoreID(), req.RangeID)
	}

	if err := r.withRaftGroup(func(raftGroup *raft.RawNode) error {
		return raftGroup.Step(req.Message)
	}); err != nil {
		return err
	}

	s.enqueueRaftUpdateCheck(req.RangeID)
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
	sem := makeSemaphore(storeReplicaRaftReadyConcurrency)

	s.stopper.RunWorker(func() {
		defer s.ctx.Transport.Stop(s.StoreID())
		ticker := time.NewTicker(s.ctx.RaftTickInterval)
		defer ticker.Stop()
		for {
			s.processRaftMu.Lock()

			// Copy the replicas tracked in pendingRaftGroups into local
			// variables that we can use without the lock. Divide them into
			// separate lists for initialized and uninitialized replicas
			// (explained below).
			var initReplicas []*Replica
			var uninitReplicas []*Replica
			s.mu.Lock()
			s.pendingRaftGroups.Lock()
			if len(s.pendingRaftGroups.value) > 0 {
				for rangeID := range s.pendingRaftGroups.value {
					if r, ok := s.mu.uninitReplicas[rangeID]; ok {
						uninitReplicas = append(uninitReplicas, r)
					} else if r, ok := s.mu.replicas[rangeID]; ok {
						initReplicas = append(initReplicas, r)
					}
				}
				s.pendingRaftGroups.value = map[roachpb.RangeID]struct{}{}
			}
			s.pendingRaftGroups.Unlock()
			s.mu.Unlock()

			// Handle raft updates for all replicas. Concurrency here is
			// subtle: we can process replicas in parallel if they are
			// initialized (because we know that they operate on disjoint
			// regions of the keyspace), but uninitialized replicas might
			// conflict with either initialized replicas (which can create
			// new replicas via splits) or other uninitialized replicas (by
			// applying snapshots). We therefore process all uninitialized
			// replicas serially, before starting initialized replicas in
			// parallel.
			for _, r := range uninitReplicas {
				if err := r.handleRaftReady(); err != nil {
					panic(err) // TODO(bdarnell)
				}
			}

			var wg sync.WaitGroup
			wg.Add(len(initReplicas))
			for _, r := range initReplicas {
				sem.acquire()
				go func(r *Replica) {
					defer sem.release()
					defer wg.Done()
					if err := r.handleRaftReady(); err != nil {
						panic(err) // TODO(bdarnell)
					}
				}(r)
			}
			wg.Wait()
			s.processRaftMu.Unlock()

			select {
			case <-s.wakeRaftLoop:

			case st := <-s.ctx.Transport.SnapshotStatusChan:
				s.processRaftMu.Lock()
				s.mu.Lock()
				if r, ok := s.mu.replicas[st.Req.RangeID]; ok {
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
						log.Error(context.TODO(), err)
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
func (s *Store) getOrCreateReplicaLocked(rangeID roachpb.RangeID, replicaID roachpb.ReplicaID, creatingReplica roachpb.ReplicaDescriptor) (*Replica, error) {
	r, ok := s.mu.replicas[rangeID]
	if ok {
		if err := r.setReplicaID(replicaID); err != nil {
			return nil, err
		}
		return r, nil
	}

	// Before creating the replica, see if there is a tombstone which
	// would indicate that this is a stale message.
	tombstoneKey := keys.RaftTombstoneKey(rangeID)
	var tombstone roachpb.RaftTombstone
	if ok, err := engine.MVCCGetProto(context.Background(), s.Engine(), tombstoneKey, hlc.ZeroTimestamp, true, nil, &tombstone); err != nil {
		return nil, err
	} else if ok {
		if replicaID != 0 && replicaID < tombstone.NextReplicaID {
			return nil, &roachpb.RaftGroupDeletedError{}
		}
	}

	var err error
	r, err = NewReplica(&roachpb.RangeDescriptor{
		RangeID: rangeID,
		// TODO(bdarnell): other fields are unknown; need to populate them from
		// snapshot.
	}, s, replicaID)
	if err != nil {
		return nil, err
	}
	r.creatingReplica = &creatingReplica
	// Add the range to range map, but not replicasByKey since
	// the range's start key is unknown. The range will be
	// added to replicasByKey later when a snapshot is applied.
	if err = s.addReplicaToRangeMapLocked(r); err != nil {
		return nil, err
	}
	s.mu.uninitReplicas[r.RangeID] = r

	return r, nil
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
	leaderRangeCount, replicatedRangeCount, replicationPendingRangeCount, availableRangeCount int64) {
	// Load the system config.
	cfg, ok := s.Gossip().GetSystemConfig()
	if !ok {
		log.Infof(context.TODO(), "system config not yet available")
		return
	}

	timestamp := hlc.Timestamp{WallTime: now}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rng := range s.mu.replicas {
		desc := rng.Desc()
		zoneConfig, err := cfg.GetZoneConfigForKey(desc.StartKey)
		if err != nil {
			log.Error(context.TODO(), err)
			continue
		}
		raftStatus := rng.RaftStatus()

		if raftStatus != nil && raftStatus.SoftState.RaftState == raft.StateLeader {
			leaderRangeCount++
			// TODO(bram): #4564 Compare attributes of the stores so we can
			// track ranges that have enough replicas but still need to be
			// migrated onto nodes with the desired attributes.
			if len(raftStatus.Progress) >= len(zoneConfig.ReplicaAttrs) {
				replicatedRangeCount++
			}

			// If any replica holds the range lease, the range is available.
			if lease, _ := rng.getLease(); lease.Covers(timestamp) {
				availableRangeCount++
			} else {
				// If there is no range lease, then as long as more than 50%
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

			if action, _ := s.allocator.ComputeAction(zoneConfig, desc); action != AllocatorNoop {
				replicationPendingRangeCount++
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
	leaderRangeCount, replicatedRangeCount, replicationPendingRangeCount, availableRangeCount :=
		s.computeReplicationStatus(now)
	s.metrics.updateReplicationGauges(
		leaderRangeCount, replicatedRangeCount, replicationPendingRangeCount, availableRangeCount)

	// Get the latest RocksDB stats.
	stats, err := s.engine.GetStats()
	if err != nil {
		return err
	}
	s.metrics.updateRocksDBStats(*stats)

	// If we're using RocksDB, log the sstable overview.
	if rocksdb, ok := s.engine.(*engine.RocksDB); ok {
		sstables := rocksdb.GetSSTables()
		readAmp := sstables.ReadAmplification()
		log.Infof(context.TODO(), "store %d sstables (read amplification = %d):\n%s", s.StoreID(), readAmp, sstables)
		s.metrics.rdbReadAmplification.Update(int64(readAmp))
	}
	return nil
}

// ComputeStatsForKeySpan computes the aggregated MVCCStats for all replicas on
// this store which contain any keys in the supplied range.
func (s *Store) ComputeStatsForKeySpan(startKey, endKey roachpb.RKey) (enginepb.MVCCStats, int) {
	var output enginepb.MVCCStats
	var count int

	s.mu.Lock()
	defer s.mu.Unlock()
	s.visitReplicasLocked(startKey, endKey, func(repl *Replica) bool {
		repl.mu.Lock()
		output.Add(repl.mu.state.Stats)
		repl.mu.Unlock()
		count++
		return true
	})

	return output, count
}

// FrozenStatus returns all of the Store's Replicas which are frozen (if the
// parameter is true) or unfrozen (otherwise). It makes no attempt to prevent
// new data being rebalanced to the Store, and thus does not guarantee that the
// Store remains in the reported state.
func (s *Store) FrozenStatus(collectFrozen bool) (repDescs []roachpb.ReplicaDescriptor) {
	newStoreRangeSet(s).Visit(func(r *Replica) bool {
		if !r.IsInitialized() {
			return true
		}
		repDesc, err := r.GetReplicaDescriptor()
		if err != nil {
			if _, ok := err.(*roachpb.RangeNotFoundError); ok {
				return true
			}
			log.Fatalf(context.TODO(), "unexpected error: %s", err)
		}
		r.mu.Lock()
		if r.mu.state.Frozen == collectFrozen {
			repDescs = append(repDescs, repDesc)
		}
		r.mu.Unlock()
		return true // want more
	})
	return
}

// Reserve requests a reservation from the store's bookie.
func (s *Store) Reserve(ctx context.Context, req roachpb.ReservationRequest) roachpb.ReservationResponse {
	return s.bookie.Reserve(ctx, req, s.deadReplicas().Replicas)
}

// The methods below can be used to control a store's queues. Stopping a queue
// is only meant to happen in tests.

func (s *Store) setRaftLogQueueActive(active bool) {
	s.raftLogQueue.SetDisabled(!active)
}
func (s *Store) setReplicaGCQueueActive(active bool) {
	s.replicaGCQueue.SetDisabled(!active)
}
func (s *Store) setReplicateQueueActive(active bool) {
	s.replicateQueue.SetDisabled(!active)
}
func (s *Store) setSplitQueueActive(active bool) {
	s.splitQueue.SetDisabled(!active)
}
func (s *Store) setScannerActive(active bool) {
	s.scanner.SetDisabled(!active)
}
