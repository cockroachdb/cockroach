// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/container"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/storage/compactor"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/idalloc"
	"github.com/cockroachdb/cockroach/pkg/storage/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/storage/raftentry"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/tscache"
	"github.com/cockroachdb/cockroach/pkg/storage/txnrecovery"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/shuffle"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	crdberrors "github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/google/btree"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"golang.org/x/time/rate"
)

const (
	// rangeIDAllocCount is the number of Range IDs to allocate per allocation.
	rangeIDAllocCount                 = 10
	defaultRaftHeartbeatIntervalTicks = 5

	// defaultRaftEntryCacheSize is the default size in bytes for a
	// store's Raft log entry cache.
	defaultRaftEntryCacheSize = 1 << 24 // 16M

	// replicaRequestQueueSize specifies the maximum number of requests to queue
	// for a replica.
	replicaRequestQueueSize = 100

	defaultGossipWhenCapacityDeltaExceedsFraction = 0.01

	// systemDataGossipInterval is the interval at which range lease
	// holders verify that the most recent system data is gossiped.
	// This ensures that system data is always eventually gossiped, even
	// if a range lease holder experiences a failure causing a missed
	// gossip update.
	systemDataGossipInterval = 1 * time.Minute
)

var changeTypeInternalToRaft = map[roachpb.ReplicaChangeType]raftpb.ConfChangeType{
	roachpb.ADD_REPLICA:    raftpb.ConfChangeAddNode,
	roachpb.REMOVE_REPLICA: raftpb.ConfChangeRemoveNode,
}

var storeSchedulerConcurrency = envutil.EnvOrDefaultInt(
	"COCKROACH_SCHEDULER_CONCURRENCY", 8*runtime.NumCPU())

var logSSTInfoTicks = envutil.EnvOrDefaultInt(
	"COCKROACH_LOG_SST_INFO_TICKS_INTERVAL", 60,
)

// bulkIOWriteLimit is defined here because it is used by BulkIOWriteLimiter.
var bulkIOWriteLimit = settings.RegisterByteSizeSetting(
	"kv.bulk_io_write.max_rate",
	"the rate limit (bytes/sec) to use for writes to disk on behalf of bulk io ops",
	1<<40,
)

// importRequestsLimit limits concurrent import requests.
var importRequestsLimit = settings.RegisterPositiveIntSetting(
	"kv.bulk_io_write.concurrent_import_requests",
	"number of import requests a store will handle concurrently before queuing",
	1,
)

// addSSTableRequestMaxRate is the maximum number of AddSSTable requests per second.
var addSSTableRequestMaxRate = settings.RegisterNonNegativeFloatSetting(
	"kv.bulk_io_write.addsstable_max_rate",
	"maximum number of AddSSTable requests per second for a single store",
	float64(rate.Inf),
)

const addSSTableRequestBurst = 32

// addSSTableRequestLimit limits concurrent AddSSTable requests.
var addSSTableRequestLimit = settings.RegisterPositiveIntSetting(
	"kv.bulk_io_write.concurrent_addsstable_requests",
	"number of AddSSTable requests a store will handle concurrently before queuing",
	1,
)

// concurrentRangefeedItersLimit limits concurrent rangefeed catchup iterators.
var concurrentRangefeedItersLimit = settings.RegisterPositiveIntSetting(
	"kv.rangefeed.concurrent_catchup_iterators",
	"number of rangefeeds catchup iterators a store will allow concurrently before queueing",
	64,
)

// ExportRequestsLimit is the number of Export requests that can run at once.
// Each extracts data from RocksDB to a temp file and then uploads it to cloud
// storage. In order to not exhaust the disk or memory, or saturate the network,
// limit the number of these that can be run in parallel. This number was chosen
// by a guessing - it could be improved by more measured heuristics. Exported
// here since we check it in in the caller to limit generated requests as well
// to prevent excessive queuing.
var ExportRequestsLimit = settings.RegisterPositiveIntSetting(
	"kv.bulk_io_write.concurrent_export_requests",
	"number of export requests a store will handle concurrently before queuing",
	3,
)

// TestStoreConfig has some fields initialized with values relevant in tests.
func TestStoreConfig(clock *hlc.Clock) StoreConfig {
	if clock == nil {
		clock = hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	}
	st := cluster.MakeTestingClusterSettings()
	sc := StoreConfig{
		DefaultZoneConfig:           config.DefaultZoneConfigRef(),
		DefaultSystemZoneConfig:     config.DefaultSystemZoneConfigRef(),
		Settings:                    st,
		AmbientCtx:                  log.AmbientContext{Tracer: st.Tracer},
		Clock:                       clock,
		CoalescedHeartbeatsInterval: 50 * time.Millisecond,
		RaftHeartbeatIntervalTicks:  1,
		ScanInterval:                10 * time.Minute,
		TimestampCachePageSize:      tscache.TestSklPageSize,
		HistogramWindowInterval:     metric.TestSampleInterval,
		EnableEpochRangeLeases:      true,
		ClosedTimestamp:             container.NoopContainer(),
	}

	// Use shorter Raft tick settings in order to minimize start up and failover
	// time in tests.
	sc.RaftElectionTimeoutTicks = 3
	sc.RaftTickInterval = 100 * time.Millisecond
	sc.SetDefaults()
	return sc
}

func newRaftConfig(
	strg raft.Storage, id uint64, appliedIndex uint64, storeCfg StoreConfig, logger raft.Logger,
) *raft.Config {
	return &raft.Config{
		ID:                        id,
		Applied:                   appliedIndex,
		ElectionTick:              storeCfg.RaftElectionTimeoutTicks,
		HeartbeatTick:             storeCfg.RaftHeartbeatIntervalTicks,
		MaxUncommittedEntriesSize: storeCfg.RaftMaxUncommittedEntriesSize,
		MaxCommittedSizePerReady:  storeCfg.RaftMaxCommittedSizePerReady,
		MaxSizePerMsg:             storeCfg.RaftMaxSizePerMsg,
		MaxInflightMsgs:           storeCfg.RaftMaxInflightMsgs,
		Storage:                   strg,
		Logger:                    logger,

		PreVote: true,
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

// rangeKeyItem is a common interface for roachpb.Key and Range.
type rangeKeyItem interface {
	startKey() roachpb.RKey
}

// rangeBTreeKey is a type alias of roachpb.RKey that implements the
// rangeKeyItem interface and the btree.Item interface.
type rangeBTreeKey roachpb.RKey

var _ rangeKeyItem = rangeBTreeKey{}

func (k rangeBTreeKey) startKey() roachpb.RKey {
	return (roachpb.RKey)(k)
}

var _ btree.Item = rangeBTreeKey{}

func (k rangeBTreeKey) Less(i btree.Item) bool {
	return k.startKey().Less(i.(rangeKeyItem).startKey())
}

// A NotBootstrappedError indicates that an engine has not yet been
// bootstrapped due to a store identifier not being present.
type NotBootstrappedError struct{}

// Error formats error.
func (e *NotBootstrappedError) Error() string {
	return "store has not been bootstrapped"
}

// A storeReplicaVisitor calls a visitor function for each of a store's
// initialized Replicas (in unspecified order). It provides an option
// to visit replicas in increasing RangeID order.
type storeReplicaVisitor struct {
	store   *Store
	repls   []*Replica // Replicas to be visited
	ordered bool       // Option to visit replicas in sorted order
	visited int        // Number of visited ranges, -1 before first call to Visit()
}

// Len implements sort.Interface.
func (rs storeReplicaVisitor) Len() int { return len(rs.repls) }

// Less implements sort.Interface.
func (rs storeReplicaVisitor) Less(i, j int) bool { return rs.repls[i].RangeID < rs.repls[j].RangeID }

// Swap implements sort.Interface.
func (rs storeReplicaVisitor) Swap(i, j int) { rs.repls[i], rs.repls[j] = rs.repls[j], rs.repls[i] }

// newStoreReplicaVisitor constructs a storeReplicaVisitor.
func newStoreReplicaVisitor(store *Store) *storeReplicaVisitor {
	return &storeReplicaVisitor{
		store:   store,
		visited: -1,
	}
}

// InOrder tells the visitor to visit replicas in increasing RangeID order.
func (rs *storeReplicaVisitor) InOrder() *storeReplicaVisitor {
	rs.ordered = true
	return rs
}

// Visit calls the visitor with each Replica until false is returned.
func (rs *storeReplicaVisitor) Visit(visitor func(*Replica) bool) {
	// Copy the range IDs to a slice so that we iterate over some (possibly
	// stale) view of all Replicas without holding the Store lock. In particular,
	// no locks are acquired during the copy process.
	rs.repls = nil
	rs.store.mu.replicas.Range(func(k int64, v unsafe.Pointer) bool {
		rs.repls = append(rs.repls, (*Replica)(v))
		return true
	})

	if rs.ordered {
		// If the replicas were requested in sorted order, perform the sort.
		sort.Sort(rs)
	} else {
		// The Replicas are already in "unspecified order" due to map iteration,
		// but we want to make sure it's completely random to prevent issues in
		// tests where stores are scanning replicas in lock-step and one store is
		// winning the race and getting a first crack at processing the replicas on
		// its queues.
		//
		// TODO(peter): Re-evaluate whether this is necessary after we allow
		// rebalancing away from the leaseholder. See TestRebalance_3To5Small.
		shuffle.Shuffle(rs)
	}

	rs.visited = 0
	for _, repl := range rs.repls {
		// TODO(tschottdorf): let the visitor figure out if something's been
		// destroyed once we return errors from mutexes (#9190). After all, it
		// can still happen with this code.
		rs.visited++
		repl.mu.RLock()
		destroyed := repl.mu.destroyStatus
		initialized := repl.isInitializedRLocked()
		repl.mu.RUnlock()
		if initialized && (destroyed.IsAlive() || destroyed.reason == destroyReasonRemovalPending) && !visitor(repl) {
			break
		}
	}
	rs.visited = 0
}

// EstimatedCount returns an estimated count of the underlying store's
// replicas.
//
// TODO(tschottdorf): this method has highly doubtful semantics.
func (rs *storeReplicaVisitor) EstimatedCount() int {
	if rs.visited <= 0 {
		return rs.store.ReplicaCount()
	}
	return len(rs.repls) - rs.visited
}

type raftRequestInfo struct {
	req        *RaftMessageRequest
	respStream RaftMessageResponseStream
}

type raftRequestQueue struct {
	syncutil.Mutex
	infos []raftRequestInfo
	// TODO(nvanbenschoten): consider recycling []raftRequestInfo slices. This
	// could be done without any new mutex locking by storing two slices here
	// and swapping them under lock in processRequestQueue.
}

// A Store maintains a map of ranges by start key. A Store corresponds
// to one physical device.
type Store struct {
	Ident              *roachpb.StoreIdent // pointer to catch access before Start() is called
	cfg                StoreConfig
	db                 *client.DB
	engine             engine.Engine        // The underlying key-value store
	compactor          *compactor.Compactor // Schedules compaction of the engine
	tsCache            tscache.Cache        // Most recent timestamps for keys / key ranges
	allocator          Allocator            // Makes allocation decisions
	replRankings       *replicaRankings
	storeRebalancer    *StoreRebalancer
	rangeIDAlloc       *idalloc.Allocator          // Range ID allocator
	gcQueue            *gcQueue                    // Garbage collection queue
	mergeQueue         *mergeQueue                 // Range merging queue
	splitQueue         *splitQueue                 // Range splitting queue
	replicateQueue     *replicateQueue             // Replication queue
	replicaGCQueue     *replicaGCQueue             // Replica GC queue
	raftLogQueue       *raftLogQueue               // Raft log truncation queue
	raftSnapshotQueue  *raftSnapshotQueue          // Raft repair queue
	tsMaintenanceQueue *timeSeriesMaintenanceQueue // Time series maintenance queue
	scanner            *replicaScanner             // Replica scanner
	consistencyQueue   *consistencyQueue           // Replica consistency check queue
	metrics            *StoreMetrics
	intentResolver     *intentresolver.IntentResolver
	recoveryMgr        txnrecovery.Manager
	raftEntryCache     *raftentry.Cache
	limiters           batcheval.Limiters
	txnWaitMetrics     *txnwait.Metrics

	// gossipRangeCountdown and leaseRangeCountdown are countdowns of
	// changes to range and leaseholder counts, after which the store
	// descriptor will be re-gossiped earlier than the normal periodic
	// gossip interval. Updated atomically.
	gossipRangeCountdown int32
	gossipLeaseCountdown int32
	// gossipQueriesPerSecondVal and gossipWritesPerSecond serve similar
	// purposes, but simply record the most recently gossiped value so that we
	// can tell if a newly measured value differs by enough to justify
	// re-gossiping the store.
	gossipQueriesPerSecondVal syncutil.AtomicFloat64
	gossipWritesPerSecondVal  syncutil.AtomicFloat64

	coalescedMu struct {
		syncutil.Mutex
		heartbeats         map[roachpb.StoreIdent][]RaftHeartbeat
		heartbeatResponses map[roachpb.StoreIdent][]RaftHeartbeat
	}
	// 1 if the store was started, 0 if it wasn't. To be accessed using atomic
	// ops.
	started int32
	stopper *stop.Stopper
	// The time when the store was Start()ed, in nanos.
	startedAt    int64
	nodeDesc     *roachpb.NodeDescriptor
	initComplete sync.WaitGroup // Signaled by async init tasks

	// Semaphore to limit concurrent non-empty snapshot application.
	snapshotApplySem chan struct{}

	// Track newly-acquired expiration-based leases that we want to proactively
	// renew. An object is sent on the signal whenever a new entry is added to
	// the map.
	renewableLeases       syncutil.IntMap // map[roachpb.RangeID]*Replica
	renewableLeasesSignal chan struct{}

	// draining holds a bool which indicates whether this store is draining. See
	// SetDraining() for a more detailed explanation of behavior changes.
	//
	// TODO(bdarnell,tschottdorf): Would look better inside of `mu`, which at
	// the time of its creation was riddled with deadlock (but that situation
	// has likely improved).
	draining atomic.Value

	// Locking notes: To avoid deadlocks, the following lock order must be
	// obeyed: baseQueue.mu < Replica.raftMu < Replica.readOnlyCmdMu < Store.mu
	// < Replica.mu < Replica.unreachablesMu < Store.coalescedMu < Store.scheduler.mu.
	// (It is not required to acquire every lock in sequence, but when multiple
	// locks are held at the same time, it is incorrect to acquire a lock with
	// "lesser" value in this sequence after one with "greater" value).
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
	// often surrounded by a long-lived lock of Replica.raftMu as
	// described below.
	//
	// There are two major entry points to this stack of locks:
	// Store.Send (which handles incoming RPCs) and raft-related message
	// processing (including handleRaftReady on the processRaft
	// goroutine and HandleRaftRequest on GRPC goroutines). Reads are
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
	// * Replica.raftMu: Held while any raft messages are being processed
	//   (including handleRaftReady and HandleRaftRequest) or while the set of
	//   Replicas in the Store is being changed (which may happen outside of raft
	//   via the replica GC queue).
	//
	//   If holding raftMus for multiple different replicas simultaneously,
	//   acquire the locks in the order that the replicas appear in replicasByKey.
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
	// * baseQueue.mu: The mutex contained in each of the store's queues (such
	//   as the replicate queue, replica GC queue, GC queue, ...). The mutex is
	//   typically acquired when deciding whether to add a replica to the respective
	//   queue.
	//
	// * Store.mu: Protects the Store's map of its Replicas. Acquired and
	//   released briefly at the start of each request; metadata operations like
	//   splits acquire it again to update the map. Even though these lock
	//   acquisitions do not make up a single critical section, it is safe thanks
	//   to Replica.raftMu which prevents any concurrent modifications.
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
	// * Store.scheduler.mu: Protects the Raft scheduler internal
	//   state. Callbacks from the scheduler are performed while not holding this
	//   mutex in order to observe the above ordering constraints.
	//
	// Splits and merges deserve special consideration: they operate on two
	// ranges. For splits, this might seem fine because the right-hand range is
	// brand new, but an uninitialized version may have been created by a raft
	// message before we process the split (see commentary on
	// Replica.splitTrigger). We make this safe, for both splits and merges, by
	// locking the right-hand range for the duration of the Raft command
	// containing the split/merge trigger.
	//
	// Note that because we acquire and release Store.mu and Replica.mu
	// repeatedly rather than holding a lock for an entire request, we are
	// actually relying on higher-level locks to ensure that things don't change
	// out from under us. In particular, handleRaftReady accesses the replicaID
	// more than once, and we rely on Replica.raftMu to ensure that this is not
	// modified by a concurrent HandleRaftRequest. (#4476)

	mu struct {
		syncutil.RWMutex
		// Map of replicas by Range ID (map[roachpb.RangeID]*Replica). This
		// includes `uninitReplicas`. May be read without holding Store.mu.
		replicas syncutil.IntMap
		// A btree key containing objects of type *Replica or *ReplicaPlaceholder.
		// Both types have an associated key range; the btree is keyed on their
		// start keys.
		replicasByKey  *btree.BTree
		uninitReplicas map[roachpb.RangeID]*Replica // Map of uninitialized replicas by Range ID
		// replicaPlaceholders is a map to access all placeholders, so they can
		// be directly accessed and cleared after stepping all raft groups. This
		// is always in sync with the placeholders in replicasByKey.
		replicaPlaceholders map[roachpb.RangeID]*ReplicaPlaceholder
	}

	// The unquiesced subset of replicas.
	unquiescedReplicas struct {
		syncutil.Mutex
		m map[roachpb.RangeID]struct{}
	}

	// The subset of replicas with active rangefeeds.
	rangefeedReplicas struct {
		syncutil.Mutex
		m map[roachpb.RangeID]struct{}
	}

	// replicaQueues is a map of per-Replica incoming request queues. These
	// queues might more naturally belong in Replica, but are kept separate to
	// avoid reworking the locking in getOrCreateReplica which requires
	// Replica.raftMu to be held while a replica is being inserted into
	// Store.mu.replicas.
	replicaQueues syncutil.IntMap // map[roachpb.RangeID]*raftRequestQueue

	scheduler *raftScheduler

	// livenessMap is a map from nodeID to a bool indicating
	// liveness. It is updated periodically in raftTickLoop().
	livenessMap atomic.Value

	// cachedCapacity caches information on store capacity to prevent
	// expensive recomputations in case leases or replicas are rapidly
	// rebalancing.
	cachedCapacity struct {
		syncutil.Mutex
		roachpb.StoreCapacity
	}

	counts struct {
		// Number of placeholders removed due to error.
		removedPlaceholders int32
		// Number of placeholders successfully filled by a snapshot.
		filledPlaceholders int32
		// Number of placeholders removed due to a snapshot that was dropped by
		// raft.
		droppedPlaceholders int32
	}

	computeInitialMetrics sync.Once
}

var _ client.Sender = &Store{}

// A StoreConfig encompasses the auxiliary objects and configuration
// required to create a store.
// All fields holding a pointer or an interface are required to create
// a store; the rest will have sane defaults set if omitted.
type StoreConfig struct {
	AmbientCtx log.AmbientContext
	base.RaftConfig

	DefaultZoneConfig       *config.ZoneConfig
	DefaultSystemZoneConfig *config.ZoneConfig
	Settings                *cluster.Settings
	Clock                   *hlc.Clock
	DB                      *client.DB
	Gossip                  *gossip.Gossip
	NodeLiveness            *NodeLiveness
	StorePool               *StorePool
	Transport               *RaftTransport
	NodeDialer              *nodedialer.Dialer
	RPCContext              *rpc.Context
	RangeDescriptorCache    kvbase.RangeDescriptorCache

	ClosedTimestamp *container.Container

	// SQLExecutor is used by the store to execute SQL statements.
	SQLExecutor sqlutil.InternalExecutor

	// TimeSeriesDataStore is an interface used by the store's time series
	// maintenance queue to dispatch individual maintenance tasks.
	TimeSeriesDataStore TimeSeriesDataStore

	// CoalescedHeartbeatsInterval is the interval for which heartbeat messages
	// are queued and then sent as a single coalesced heartbeat; it is a
	// fraction of the RaftTickInterval so that heartbeats don't get delayed by
	// an entire tick. Delaying coalescing heartbeat responses has a bad
	// interaction with quiescence because the coalesced (delayed) heartbeat
	// response can unquiesce the leader. Consider:
	//
	// T+0: leader queues MsgHeartbeat
	// T+1: leader sends MsgHeartbeat
	//                                        follower receives MsgHeartbeat
	//                                        follower queues MsgHeartbeatResp
	// T+2: leader queues quiesce message
	//                                        follower sends MsgHeartbeatResp
	//      leader receives MsgHeartbeatResp
	// T+3: leader sends quiesce message
	//
	// Thus we want to make sure that heartbeats are responded to faster than
	// the quiesce cadence.
	CoalescedHeartbeatsInterval time.Duration

	// RaftHeartbeatIntervalTicks is the number of ticks that pass between heartbeats.
	RaftHeartbeatIntervalTicks int

	// ScanInterval is the default value for the scan interval
	ScanInterval time.Duration

	// ScanMinIdleTime is the minimum time the scanner will be idle between ranges.
	// If enabled (> 0), the scanner may complete in more than ScanInterval for
	// stores with many ranges.
	ScanMinIdleTime time.Duration

	// ScanMaxIdleTime is the maximum time the scanner will be idle between ranges.
	// If enabled (> 0), the scanner may complete in less than ScanInterval for small
	// stores.
	ScanMaxIdleTime time.Duration

	// If LogRangeEvents is true, major changes to ranges will be logged into
	// the range event log.
	LogRangeEvents bool

	// RaftEntryCacheSize is the size in bytes of the Raft log entry cache
	// shared by all Raft groups managed by the store.
	RaftEntryCacheSize uint64

	// IntentResolverTaskLimit is the maximum number of asynchronous tasks that
	// may be started by the intent resolver. -1 indicates no asynchronous tasks
	// are allowed. 0 uses the default value (defaultIntentResolverTaskLimit)
	// which is non-zero.
	IntentResolverTaskLimit int

	TestingKnobs StoreTestingKnobs

	// concurrentSnapshotApplyLimit specifies the maximum number of empty
	// snapshots and the maximum number of non-empty snapshots that are permitted
	// to be applied concurrently.
	concurrentSnapshotApplyLimit int

	// TimestampCachePageSize is (server.Config).TimestampCachePageSize
	TimestampCachePageSize uint32

	// HistogramWindowInterval is (server.Config).HistogramWindowInterval
	HistogramWindowInterval time.Duration

	// EnableEpochRangeLeases controls whether epoch-based range leases are used.
	EnableEpochRangeLeases bool

	// GossipWhenCapacityDeltaExceedsFraction specifies the fraction from the last
	// gossiped store capacity values which need be exceeded before the store will
	// gossip immediately without waiting for the periodic gossip interval.
	GossipWhenCapacityDeltaExceedsFraction float64
}

// ConsistencyTestingKnobs is a BatchEvalTestingKnobs struct used to control the
// behavior of the consistency checker for tests.
type ConsistencyTestingKnobs struct {
	// If non-nil, BadChecksumPanic is called by CheckConsistency() instead of
	// panicking on a checksum mismatch.
	BadChecksumPanic func(roachpb.StoreIdent)
	// If non-nil, BadChecksumReportDiff is called by CheckConsistency() on a
	// checksum mismatch to report the diff between snapshots.
	BadChecksumReportDiff      func(roachpb.StoreIdent, ReplicaSnapshotDiffSlice)
	ConsistencyQueueResultHook func(response roachpb.CheckConsistencyResponse)
}

// Valid returns true if the StoreConfig is populated correctly.
// We don't check for Gossip and DB since some of our tests pass
// that as nil.
func (sc *StoreConfig) Valid() bool {
	return sc.Clock != nil && sc.Transport != nil &&
		sc.RaftTickInterval != 0 && sc.RaftHeartbeatIntervalTicks > 0 &&
		sc.RaftElectionTimeoutTicks > 0 && sc.ScanInterval >= 0 &&
		sc.AmbientCtx.Tracer != nil
}

// SetDefaults initializes unset fields in StoreConfig to values
// suitable for use on a local network.
// TODO(tschottdorf): see if this ought to be configurable via flags.
func (sc *StoreConfig) SetDefaults() {
	sc.RaftConfig.SetDefaults()

	if sc.CoalescedHeartbeatsInterval == 0 {
		sc.CoalescedHeartbeatsInterval = sc.RaftTickInterval / 2
	}
	if sc.RaftHeartbeatIntervalTicks == 0 {
		sc.RaftHeartbeatIntervalTicks = defaultRaftHeartbeatIntervalTicks
	}
	if sc.RaftEntryCacheSize == 0 {
		sc.RaftEntryCacheSize = defaultRaftEntryCacheSize
	}
	if sc.concurrentSnapshotApplyLimit == 0 {
		// NB: setting this value higher than 1 is likely to degrade client
		// throughput.
		sc.concurrentSnapshotApplyLimit =
			envutil.EnvOrDefaultInt("COCKROACH_CONCURRENT_SNAPSHOT_APPLY_LIMIT", 1)
	}

	if sc.GossipWhenCapacityDeltaExceedsFraction == 0 {
		sc.GossipWhenCapacityDeltaExceedsFraction = defaultGossipWhenCapacityDeltaExceedsFraction
	}
}

// LeaseExpiration returns an int64 to increment a manual clock with to
// make sure that all active range leases expire.
func (sc *StoreConfig) LeaseExpiration() int64 {
	// Due to lease extensions, the remaining interval can be longer than just
	// the sum of the offset (=length of stasis period) and the active
	// duration, but definitely not by 2x.
	maxOffset := sc.Clock.MaxOffset()
	if maxOffset == timeutil.ClocklessMaxOffset {
		// Don't do shady math on clockless reads.
		maxOffset = 0
	}
	return 2 * (sc.RangeLeaseActiveDuration() + maxOffset).Nanoseconds()
}

// NewStore returns a new instance of a store.
func NewStore(
	ctx context.Context, cfg StoreConfig, eng engine.Engine, nodeDesc *roachpb.NodeDescriptor,
) *Store {
	// TODO(tschottdorf): find better place to set these defaults.
	cfg.SetDefaults()

	if !cfg.Valid() {
		log.Fatalf(ctx, "invalid store configuration: %+v", &cfg)
	}
	s := &Store{
		cfg:      cfg,
		db:       cfg.DB, // TODO(tschottdorf): remove redundancy.
		engine:   eng,
		nodeDesc: nodeDesc,
		metrics:  newStoreMetrics(cfg.HistogramWindowInterval),
	}
	if cfg.RPCContext != nil {
		s.allocator = MakeAllocator(cfg.StorePool, cfg.RPCContext.RemoteClocks.Latency)
	} else {
		s.allocator = MakeAllocator(cfg.StorePool, func(string) (time.Duration, bool) {
			return 0, false
		})
	}
	s.replRankings = newReplicaRankings()

	s.draining.Store(false)
	s.scheduler = newRaftScheduler(s.metrics, s, storeSchedulerConcurrency)

	s.raftEntryCache = raftentry.NewCache(cfg.RaftEntryCacheSize)
	s.metrics.registry.AddMetricStruct(s.raftEntryCache.Metrics())

	s.coalescedMu.Lock()
	s.coalescedMu.heartbeats = map[roachpb.StoreIdent][]RaftHeartbeat{}
	s.coalescedMu.heartbeatResponses = map[roachpb.StoreIdent][]RaftHeartbeat{}
	s.coalescedMu.Unlock()

	s.mu.Lock()
	s.mu.replicaPlaceholders = map[roachpb.RangeID]*ReplicaPlaceholder{}
	s.mu.replicasByKey = btree.New(64 /* degree */)
	s.mu.uninitReplicas = map[roachpb.RangeID]*Replica{}
	s.mu.Unlock()

	s.unquiescedReplicas.Lock()
	s.unquiescedReplicas.m = map[roachpb.RangeID]struct{}{}
	s.unquiescedReplicas.Unlock()

	s.rangefeedReplicas.Lock()
	s.rangefeedReplicas.m = map[roachpb.RangeID]struct{}{}
	s.rangefeedReplicas.Unlock()

	s.tsCache = tscache.New(cfg.Clock, cfg.TimestampCachePageSize)
	s.metrics.registry.AddMetricStruct(s.tsCache.Metrics())

	s.txnWaitMetrics = txnwait.NewMetrics(cfg.HistogramWindowInterval)
	s.metrics.registry.AddMetricStruct(s.txnWaitMetrics)

	s.compactor = compactor.NewCompactor(
		s.cfg.Settings,
		s.engine.(engine.WithSSTables),
		func() (roachpb.StoreCapacity, error) {
			return s.Capacity(false /* useCached */)
		},
		func(ctx context.Context) {
			s.asyncGossipStore(ctx, "compactor-initiated rocksdb compaction", false /* useCached */)
		},
	)
	s.metrics.registry.AddMetricStruct(s.compactor.Metrics)

	s.snapshotApplySem = make(chan struct{}, cfg.concurrentSnapshotApplyLimit)

	s.renewableLeasesSignal = make(chan struct{})

	s.limiters.BulkIOWriteRate = rate.NewLimiter(rate.Limit(bulkIOWriteLimit.Get(&cfg.Settings.SV)), bulkIOWriteBurst)
	bulkIOWriteLimit.SetOnChange(&cfg.Settings.SV, func() {
		s.limiters.BulkIOWriteRate.SetLimit(rate.Limit(bulkIOWriteLimit.Get(&cfg.Settings.SV)))
	})
	s.limiters.ConcurrentImportRequests = limit.MakeConcurrentRequestLimiter(
		"importRequestLimiter", int(importRequestsLimit.Get(&cfg.Settings.SV)),
	)
	importRequestsLimit.SetOnChange(&cfg.Settings.SV, func() {
		s.limiters.ConcurrentImportRequests.SetLimit(int(importRequestsLimit.Get(&cfg.Settings.SV)))
	})
	s.limiters.ConcurrentExportRequests = limit.MakeConcurrentRequestLimiter(
		"exportRequestLimiter", int(ExportRequestsLimit.Get(&cfg.Settings.SV)),
	)
	// On low-CPU instances, a default limit value may still allow ExportRequests
	// to tie up all cores so cap limiter at cores-1 when setting value is higher.
	exportCores := runtime.NumCPU() - 1
	if exportCores < 1 {
		exportCores = 1
	}
	ExportRequestsLimit.SetOnChange(&cfg.Settings.SV, func() {
		limit := int(ExportRequestsLimit.Get(&cfg.Settings.SV))
		if limit > exportCores {
			limit = exportCores
		}
		s.limiters.ConcurrentExportRequests.SetLimit(limit)
	})
	s.limiters.AddSSTableRequestRate = rate.NewLimiter(
		rate.Limit(addSSTableRequestMaxRate.Get(&cfg.Settings.SV)), addSSTableRequestBurst)
	addSSTableRequestMaxRate.SetOnChange(&cfg.Settings.SV, func() {
		rateLimit := addSSTableRequestMaxRate.Get(&cfg.Settings.SV)
		if math.IsInf(rateLimit, 0) {
			// This value causes the burst limit to be ignored
			rateLimit = float64(rate.Inf)
		}
		s.limiters.AddSSTableRequestRate.SetLimit(rate.Limit(rateLimit))
	})
	s.limiters.ConcurrentAddSSTableRequests = limit.MakeConcurrentRequestLimiter(
		"addSSTableRequestLimiter", int(addSSTableRequestLimit.Get(&cfg.Settings.SV)),
	)
	importRequestsLimit.SetOnChange(&cfg.Settings.SV, func() {
		s.limiters.ConcurrentAddSSTableRequests.SetLimit(int(addSSTableRequestLimit.Get(&cfg.Settings.SV)))
	})
	s.limiters.ConcurrentRangefeedIters = limit.MakeConcurrentRequestLimiter(
		"rangefeedIterLimiter", int(concurrentRangefeedItersLimit.Get(&cfg.Settings.SV)),
	)
	concurrentRangefeedItersLimit.SetOnChange(&cfg.Settings.SV, func() {
		s.limiters.ConcurrentRangefeedIters.SetLimit(
			int(concurrentRangefeedItersLimit.Get(&cfg.Settings.SV)))
	})

	if s.cfg.Gossip != nil {
		// Add range scanner and configure with queues.
		s.scanner = newReplicaScanner(
			s.cfg.AmbientCtx, s.cfg.Clock, cfg.ScanInterval,
			cfg.ScanMinIdleTime, cfg.ScanMaxIdleTime, newStoreReplicaVisitor(s),
		)
		s.gcQueue = newGCQueue(s, s.cfg.Gossip)
		s.mergeQueue = newMergeQueue(s, s.db, s.cfg.Gossip)
		s.splitQueue = newSplitQueue(s, s.db, s.cfg.Gossip)
		s.replicateQueue = newReplicateQueue(s, s.cfg.Gossip, s.allocator)
		s.replicaGCQueue = newReplicaGCQueue(s, s.db, s.cfg.Gossip)
		s.raftLogQueue = newRaftLogQueue(s, s.db, s.cfg.Gossip)
		s.raftSnapshotQueue = newRaftSnapshotQueue(s, s.cfg.Gossip)
		s.consistencyQueue = newConsistencyQueue(s, s.cfg.Gossip)
		// NOTE: If more queue types are added, please also add them to the list of
		// queues on the EnqueueRange debug page as defined in
		// pkg/ui/src/views/reports/containers/enqueueRange/index.tsx
		s.scanner.AddQueues(
			s.gcQueue, s.mergeQueue, s.splitQueue, s.replicateQueue, s.replicaGCQueue,
			s.raftLogQueue, s.raftSnapshotQueue, s.consistencyQueue)

		if s.cfg.TimeSeriesDataStore != nil {
			s.tsMaintenanceQueue = newTimeSeriesMaintenanceQueue(
				s, s.db, s.cfg.Gossip, s.cfg.TimeSeriesDataStore,
			)
			s.scanner.AddQueues(s.tsMaintenanceQueue)
		}
	}

	if cfg.TestingKnobs.DisableGCQueue {
		s.setGCQueueActive(false)
	}
	if cfg.TestingKnobs.DisableMergeQueue {
		s.setMergeQueueActive(false)
	}
	if cfg.TestingKnobs.DisableRaftLogQueue {
		s.setRaftLogQueueActive(false)
	}
	if cfg.TestingKnobs.DisableReplicaGCQueue {
		s.setReplicaGCQueueActive(false)
	}
	if cfg.TestingKnobs.DisableReplicateQueue {
		s.SetReplicateQueueActive(false)
	}
	if cfg.TestingKnobs.DisableSplitQueue {
		s.setSplitQueueActive(false)
	}
	if cfg.TestingKnobs.DisableTimeSeriesMaintenanceQueue {
		s.setTimeSeriesMaintenanceQueueActive(false)
	}
	if cfg.TestingKnobs.DisableRaftSnapshotQueue {
		s.setRaftSnapshotQueueActive(false)
	}
	if cfg.TestingKnobs.DisableConsistencyQueue {
		s.setConsistencyQueueActive(false)
	}
	if cfg.TestingKnobs.DisableScanner {
		s.setScannerActive(false)
	}

	return s
}

// String formats a store for debug output.
func (s *Store) String() string {
	return fmt.Sprintf("[n%d,s%d]", s.Ident.NodeID, s.Ident.StoreID)
}

// ClusterSettings returns the node's ClusterSettings.
func (s *Store) ClusterSettings() *cluster.Settings {
	return s.cfg.Settings
}

// AnnotateCtx is a convenience wrapper; see AmbientContext.
func (s *Store) AnnotateCtx(ctx context.Context) context.Context {
	return s.cfg.AmbientCtx.AnnotateCtx(ctx)
}

// The maximum amount of time waited for leadership shedding before commencing
// to drain a store.
const raftLeadershipTransferWait = 5 * time.Second

// SetDraining (when called with 'true') causes incoming lease transfers to be
// rejected, prevents all of the Store's Replicas from acquiring or extending
// range leases, and attempts to transfer away any leases owned.
// When called with 'false', returns to the normal mode of operation.
func (s *Store) SetDraining(drain bool) {
	s.draining.Store(drain)
	if !drain {
		newStoreReplicaVisitor(s).Visit(func(r *Replica) bool {
			r.mu.Lock()
			r.mu.draining = false
			r.mu.Unlock()
			return true
		})
		return
	}

	var wg sync.WaitGroup

	ctx := logtags.AddTag(context.Background(), "drain", nil)
	transferAllAway := func() int {
		// Limit the number of concurrent lease transfers.
		sem := make(chan struct{}, 100)
		// Incremented for every lease or Raft leadership transfer attempted. We try
		// to send both the lease and the Raft leaders away, but this may not
		// reliably work. Instead, we run the surrounding retry loop until there are
		// no leaders/leases left (ignoring single-replica or uninitialized Raft
		// groups).
		var numTransfersAttempted int32
		newStoreReplicaVisitor(s).Visit(func(r *Replica) bool {
			wg.Add(1)
			if err := s.stopper.RunLimitedAsyncTask(
				r.AnnotateCtx(ctx), "storage.Store: draining replica", sem, true, /* wait */
				func(ctx context.Context) {
					defer wg.Done()

					r.mu.Lock()
					r.mu.draining = true
					status := r.raftStatusRLocked()
					// needsRaftTransfer is true when we can reasonably hope to transfer
					// this replica's lease and/or Raft leadership away.
					needsRaftTransfer := status != nil &&
						len(status.Progress) > 1 &&
						!(status.RaftState == raft.StateFollower && status.Lead != 0)
					r.mu.Unlock()

					var drainingLease roachpb.Lease
					for {
						var llHandle *leaseRequestHandle
						r.mu.Lock()
						lease, nextLease := r.getLeaseRLocked()
						if nextLease != (roachpb.Lease{}) && nextLease.OwnedBy(s.StoreID()) {
							llHandle = r.mu.pendingLeaseRequest.JoinRequest()
						}
						r.mu.Unlock()

						if llHandle != nil {
							<-llHandle.C()
							continue
						}
						drainingLease = lease
						break
					}

					// Learner replicas aren't allowed to become the leaseholder or raft
					// leader, so only consider the `Voters` replicas.
					needsLeaseTransfer := len(r.Desc().Replicas().Voters()) > 1 &&
						drainingLease.OwnedBy(s.StoreID()) &&
						r.IsLeaseValid(drainingLease, s.Clock().Now())

					if needsLeaseTransfer || needsRaftTransfer {
						atomic.AddInt32(&numTransfersAttempted, 1)
					}

					if needsLeaseTransfer {
						desc, zone := r.DescAndZone()
						leaseTransferred, err := s.replicateQueue.findTargetAndTransferLease(
							ctx,
							r,
							desc,
							zone,
							transferLeaseOptions{},
						)
						if log.V(1) && !leaseTransferred {
							// Note that a nil error means that there were no suitable
							// candidates.
							log.Errorf(
								ctx,
								"did not transfer lease %s for replica %s when draining: %v",
								drainingLease,
								desc,
								err,
							)
						}
						if err == nil && leaseTransferred {
							// If we just transferred the lease away, Raft leadership will
							// usually transfer with it. Invoking a separate Raft leadership
							// transfer would only obstruct this.
							needsRaftTransfer = false
						}
					}

					if needsRaftTransfer {
						r.raftMu.Lock()
						r.maybeTransferRaftLeadership(ctx)
						r.raftMu.Unlock()
					}
				}); err != nil {
				if log.V(1) {
					log.Errorf(ctx, "error running draining task: %+v", err)
				}
				wg.Done()
				return false
			}
			return true
		})
		wg.Wait()
		return int(numTransfersAttempted)
	}

	transferAllAway()

	if err := contextutil.RunWithTimeout(ctx, "wait for raft leadership transfer", raftLeadershipTransferWait,
		func(ctx context.Context) error {
			opts := retry.Options{
				InitialBackoff: 10 * time.Millisecond,
				MaxBackoff:     time.Second,
				Multiplier:     2,
			}
			// Avoid retry.ForDuration because of https://github.com/cockroachdb/cockroach/issues/25091.
			everySecond := log.Every(time.Second)
			return retry.WithMaxAttempts(ctx, opts, 10000, func() error {
				if numRemaining := transferAllAway(); numRemaining > 0 {
					err := errors.Errorf("waiting for %d replicas to transfer their lease away", numRemaining)
					if everySecond.ShouldLog() {
						log.Info(ctx, err)
					}
					return err
				}
				return nil
			})
		}); err != nil {
		// You expect this message when shutting down a server in an unhealthy
		// cluster. If we see it on healthy ones, there's likely something to fix.
		log.Warningf(ctx, "unable to drain cleanly within %s, service might briefly deteriorate: %+v", raftLeadershipTransferWait, err)
	}
}

// IsStarted returns true if the Store has been started.
func (s *Store) IsStarted() bool {
	return atomic.LoadInt32(&s.started) == 1
}

// IterateIDPrefixKeys helps visit system keys that use RangeID prefixing ( such as
// RaftHardStateKey, RaftTombstoneKey, and many others). Such keys could in principle exist at any
// RangeID, and this helper efficiently discovers all the keys of the desired type (as specified by
// the supplied `keyFn`) and, for each key-value pair discovered, unmarshals it into `msg` and then
// invokes `f`.
//
// Iteration stops on the first error (and will pass through that error).
func IterateIDPrefixKeys(
	ctx context.Context,
	eng engine.Reader,
	keyFn func(roachpb.RangeID) roachpb.Key,
	msg protoutil.Message,
	f func(_ roachpb.RangeID) (more bool, _ error),
) error {
	rangeID := roachpb.RangeID(1)
	iter := eng.NewIterator(engine.IterOptions{
		UpperBound: keys.LocalRangeIDPrefix.PrefixEnd().AsRawKey(),
	})
	defer iter.Close()

	for {
		bumped := false
		mvccKey := engine.MakeMVCCMetadataKey(keyFn(rangeID))
		iter.Seek(mvccKey)

		if ok, err := iter.Valid(); !ok {
			return err
		}

		unsafeKey := iter.UnsafeKey()

		if !bytes.HasPrefix(unsafeKey.Key, keys.LocalRangeIDPrefix) {
			// Left the local keyspace, so we're done.
			return nil
		}

		curRangeID, _, _, _, err := keys.DecodeRangeIDKey(unsafeKey.Key)
		if err != nil {
			return err
		}

		if curRangeID > rangeID {
			// `bumped` is always `false` here, but let's be explicit.
			if !bumped {
				rangeID = curRangeID
				bumped = true
			}
			mvccKey = engine.MakeMVCCMetadataKey(keyFn(rangeID))
		}

		if !unsafeKey.Key.Equal(mvccKey.Key) {
			if !bumped {
				// Don't increment the rangeID if it has already been incremented
				// above, or we could skip past a value we ought to see.
				rangeID++
				bumped = true // for completeness' sake; continuing below anyway
			}
			continue
		}

		ok, err := engine.MVCCGetProto(
			ctx, eng, unsafeKey.Key, hlc.Timestamp{}, msg, engine.MVCCGetOptions{})
		if err != nil {
			return err
		}
		if !ok {
			return errors.Errorf("unable to unmarshal %s into %T", unsafeKey.Key, msg)
		}

		more, err := f(rangeID)
		if !more || err != nil {
			return err
		}
		rangeID++
	}
}

// IterateRangeDescriptors calls the provided function with each descriptor
// from the provided Engine. The return values of this method and fn have
// semantics similar to engine.MVCCIterate.
func IterateRangeDescriptors(
	ctx context.Context, eng engine.Reader, fn func(desc roachpb.RangeDescriptor) (bool, error),
) error {
	log.Event(ctx, "beginning range descriptor iteration")
	// Iterator over all range-local key-based data.
	start := keys.RangeDescriptorKey(roachpb.RKeyMin)
	end := keys.RangeDescriptorKey(roachpb.RKeyMax)

	allCount := 0
	matchCount := 0
	bySuffix := make(map[string]int)
	kvToDesc := func(kv roachpb.KeyValue) (bool, error) {
		allCount++
		// Only consider range metadata entries; ignore others.
		_, suffix, _, err := keys.DecodeRangeKey(kv.Key)
		if err != nil {
			return false, err
		}
		bySuffix[string(suffix)]++
		if !bytes.Equal(suffix, keys.LocalRangeDescriptorSuffix) {
			return false, nil
		}
		var desc roachpb.RangeDescriptor
		if err := kv.Value.GetProto(&desc); err != nil {
			return false, err
		}
		matchCount++
		return fn(desc)
	}

	_, err := engine.MVCCIterate(ctx, eng, start, end, hlc.MaxTimestamp,
		engine.MVCCScanOptions{Inconsistent: true}, kvToDesc)
	log.Eventf(ctx, "iterated over %d keys to find %d range descriptors (by suffix: %v)",
		allCount, matchCount, bySuffix)
	return err
}

// ReadStoreIdent reads the StoreIdent from the store.
// It returns *NotBootstrappedError if the ident is missing (meaning that the
// store needs to be bootstrapped).
func ReadStoreIdent(ctx context.Context, eng engine.Engine) (roachpb.StoreIdent, error) {
	var ident roachpb.StoreIdent
	ok, err := engine.MVCCGetProto(
		ctx, eng, keys.StoreIdentKey(), hlc.Timestamp{}, &ident, engine.MVCCGetOptions{})
	if err != nil {
		return roachpb.StoreIdent{}, err
	} else if !ok {
		return roachpb.StoreIdent{}, &NotBootstrappedError{}
	}
	return ident, err
}

// Start the engine, set the GC and read the StoreIdent.
func (s *Store) Start(ctx context.Context, stopper *stop.Stopper) error {
	s.stopper = stopper

	// Populate the store ident. If not bootstrapped, ReadStoreIntent will
	// return an error.
	ident, err := ReadStoreIdent(ctx, s.engine)
	if err != nil {
		return err
	}
	s.Ident = &ident

	// Set the store ID for logging.
	s.cfg.AmbientCtx.AddLogTag("s", s.StoreID())
	ctx = s.AnnotateCtx(ctx)
	log.Event(ctx, "read store identity")

	// Add the store ID to the scanner's AmbientContext before starting it, since
	// the AmbientContext provided during construction did not include it.
	// Note that this is just a hacky way of getting around that without
	// refactoring the scanner/queue construction/start logic more broadly, and
	// depends on the scanner not having added its own log tag.
	if s.scanner != nil {
		s.scanner.AmbientContext.AddLogTag("s", s.StoreID())
	}

	// If the nodeID is 0, it has not be assigned yet.
	if s.nodeDesc.NodeID != 0 && s.Ident.NodeID != s.nodeDesc.NodeID {
		return errors.Errorf("node id:%d does not equal the one in node descriptor:%d", s.Ident.NodeID, s.nodeDesc.NodeID)
	}
	// Always set gossip NodeID before gossiping any info.
	if s.cfg.Gossip != nil {
		s.cfg.Gossip.NodeID.Set(ctx, s.Ident.NodeID)
	}

	// Create ID allocators.
	idAlloc, err := idalloc.NewAllocator(
		s.cfg.AmbientCtx, keys.RangeIDGenerator, s.db, rangeIDAllocCount, s.stopper,
	)
	if err != nil {
		return err
	}

	// Create the intent resolver.
	s.intentResolver = intentresolver.New(intentresolver.Config{
		Clock:                s.cfg.Clock,
		DB:                   s.db,
		Stopper:              stopper,
		TaskLimit:            s.cfg.IntentResolverTaskLimit,
		AmbientCtx:           s.cfg.AmbientCtx,
		TestingKnobs:         s.cfg.TestingKnobs.IntentResolverKnobs,
		RangeDescriptorCache: s.cfg.RangeDescriptorCache,
	})
	s.metrics.registry.AddMetricStruct(s.intentResolver.Metrics)

	// Create the recovery manager.
	s.recoveryMgr = txnrecovery.NewManager(
		s.cfg.AmbientCtx, s.cfg.Clock, s.db, stopper,
	)
	s.metrics.registry.AddMetricStruct(s.recoveryMgr.Metrics())

	s.rangeIDAlloc = idAlloc

	now := s.cfg.Clock.Now()
	s.startedAt = now.WallTime

	// Iterate over all range descriptors, ignoring uncommitted versions
	// (consistent=false). Uncommitted intents which have been abandoned
	// due to a split crashing halfway will simply be resolved on the
	// next split attempt. They can otherwise be ignored.

	// TODO(peter): While we have to iterate to find the replica descriptors
	// serially, we can perform the migrations and replica creation
	// concurrently. Note that while we can perform this initialization
	// concurrently, all of the initialization must be performed before we start
	// listening for Raft messages and starting the process Raft loop.
	err = IterateRangeDescriptors(ctx, s.engine,
		func(desc roachpb.RangeDescriptor) (bool, error) {
			if !desc.IsInitialized() {
				return false, errors.Errorf("found uninitialized RangeDescriptor: %+v", desc)
			}

			rep, err := NewReplica(&desc, s, 0)
			if err != nil {
				return false, err
			}

			// We can't lock s.mu across NewReplica due to the lock ordering
			// constraint (*Replica).raftMu < (*Store).mu. See the comment on
			// (Store).mu.
			s.mu.Lock()
			err = s.addReplicaInternalLocked(rep)
			s.mu.Unlock()
			if err != nil {
				return false, err
			}

			// Add this range and its stats to our counter.
			s.metrics.ReplicaCount.Inc(1)
			s.metrics.addMVCCStats(rep.GetMVCCStats())

			if _, ok := desc.GetReplicaDescriptor(s.StoreID()); !ok {
				// We are no longer a member of the range, but we didn't GC the replica
				// before shutting down. Add the replica to the GC queue.
				s.replicaGCQueue.AddAsync(ctx, rep, replicaGCPriorityRemoved)
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
		})
	if err != nil {
		return err
	}

	// Start Raft processing goroutines.
	s.cfg.Transport.Listen(s.StoreID(), s)
	s.processRaft(ctx)

	// Register a callback to unquiesce any ranges with replicas on a
	// node transitioning from non-live to live.
	if s.cfg.NodeLiveness != nil {
		s.cfg.NodeLiveness.RegisterCallback(s.nodeIsLiveCallback)
	}

	// Gossip is only ever nil while bootstrapping a cluster and
	// in unittests.
	if s.cfg.Gossip != nil {
		// Register update channel for any changes to the system config.
		// This may trigger splits along structured boundaries,
		// and update max range bytes.
		gossipUpdateC := s.cfg.Gossip.RegisterSystemConfigChannel()
		s.stopper.RunWorker(ctx, func(context.Context) {
			for {
				select {
				case <-gossipUpdateC:
					cfg := s.cfg.Gossip.GetSystemConfig()
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
		s.stopper.RunWorker(ctx, func(context.Context) {
			select {
			case <-s.cfg.Gossip.Connected:
				s.scanner.Start(s.stopper)
			case <-s.stopper.ShouldStop():
				return
			}
		})
	}

	if !s.cfg.TestingKnobs.DisableAutomaticLeaseRenewal {
		s.startLeaseRenewer(ctx)
	}

	// Connect rangefeeds to closed timestamp updates.
	s.startClosedTimestampRangefeedSubscriber(ctx)

	if s.replicateQueue != nil {
		s.storeRebalancer = NewStoreRebalancer(
			s.cfg.AmbientCtx, s.cfg.Settings, s.replicateQueue, s.replRankings)
		s.storeRebalancer.Start(ctx, s.stopper)
	}

	// Start the storage engine compactor.
	if envutil.EnvOrDefaultBool("COCKROACH_ENABLE_COMPACTOR", true) {
		s.compactor.Start(s.AnnotateCtx(context.Background()), s.stopper)
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

var errPeriodicGossipsDisabled = errors.New("periodic gossip is disabled")

// startGossip runs an infinite loop in a goroutine which regularly checks
// whether the store has a first range or config replica and asks those ranges
// to gossip accordingly.
func (s *Store) startGossip() {
	wakeReplica := func(ctx context.Context, repl *Replica) error {
		// Acquire the range lease, which in turn triggers system data gossip
		// functions (e.g. MaybeGossipSystemConfig or MaybeGossipNodeLiveness).
		_, pErr := repl.getLeaseForGossip(ctx)
		return pErr.GoError()
	}

	if s.cfg.TestingKnobs.DisablePeriodicGossips {
		wakeReplica = func(context.Context, *Replica) error {
			return errPeriodicGossipsDisabled
		}
	}

	gossipFns := []struct {
		key         roachpb.Key
		fn          func(context.Context, *Replica) error
		description string
		interval    time.Duration
	}{
		{
			key: roachpb.KeyMin,
			fn: func(ctx context.Context, repl *Replica) error {
				// The first range is gossiped by all replicas, not just the lease
				// holder, so wakeReplica is not used here.
				return repl.maybeGossipFirstRange(ctx).GoError()
			},
			description: "first range descriptor",
			interval:    s.cfg.SentinelGossipTTL() / 2,
		},
		{
			key:         keys.SystemConfigSpan.Key,
			fn:          wakeReplica,
			description: "system config",
			interval:    systemDataGossipInterval,
		},
		{
			key:         keys.NodeLivenessSpan.Key,
			fn:          wakeReplica,
			description: "node liveness",
			interval:    systemDataGossipInterval,
		},
	}

	// Periodic updates run in a goroutine and signal a WaitGroup upon completion
	// of their first iteration.
	s.initComplete.Add(len(gossipFns))
	for _, gossipFn := range gossipFns {
		gossipFn := gossipFn // per-iteration copy
		s.stopper.RunWorker(context.Background(), func(ctx context.Context) {
			ticker := time.NewTicker(gossipFn.interval)
			defer ticker.Stop()
			for first := true; ; {
				// Retry in a backoff loop until gossipFn succeeds. The gossipFn might
				// temporarily fail (e.g. because node liveness hasn't initialized yet
				// making it impossible to get an epoch-based range lease), in which
				// case we want to retry quickly.
				retryOptions := base.DefaultRetryOptions()
				retryOptions.Closer = s.stopper.ShouldStop()
				for r := retry.Start(retryOptions); r.Next(); {
					if repl := s.LookupReplica(roachpb.RKey(gossipFn.key)); repl != nil {
						annotatedCtx := repl.AnnotateCtx(ctx)
						if err := gossipFn.fn(annotatedCtx, repl); err != nil {
							log.Warningf(annotatedCtx, "could not gossip %s: %+v", gossipFn.description, err)
							if err != errPeriodicGossipsDisabled {
								continue
							}
						}
					}
					break
				}
				if first {
					first = false
					s.initComplete.Done()
				}
				select {
				case <-ticker.C:
				case <-s.stopper.ShouldStop():
					return
				}
			}
		})
	}
}

// startLeaseRenewer runs an infinite loop in a goroutine which regularly
// checks whether the store has any expiration-based leases that should be
// proactively renewed and attempts to continue renewing them.
//
// This reduces user-visible latency when range lookups are needed to serve a
// request and reduces ping-ponging of r1's lease to different replicas as
// maybeGossipFirstRange is called on each (e.g.  #24753).
func (s *Store) startLeaseRenewer(ctx context.Context) {
	// Start a goroutine that watches and proactively renews certain
	// expiration-based leases.
	s.stopper.RunWorker(ctx, func(ctx context.Context) {
		repls := make(map[*Replica]struct{})
		timer := timeutil.NewTimer()
		defer timer.Stop()

		// Determine how frequently to attempt to ensure that we have each lease.
		// The divisor used here is somewhat arbitrary, but needs to be large
		// enough to ensure we'll attempt to renew the lease reasonably early
		// within the RangeLeaseRenewalDuration time window. This means we'll wake
		// up more often that strictly necessary, but it's more maintainable than
		// attempting to accurately determine exactly when each iteration of a
		// lease expires and when we should attempt to renew it as a result.
		renewalDuration := s.cfg.RangeLeaseActiveDuration() / 5
		for {
			s.renewableLeases.Range(func(k int64, v unsafe.Pointer) bool {
				repl := (*Replica)(v)
				annotatedCtx := repl.AnnotateCtx(ctx)
				if _, pErr := repl.redirectOnOrAcquireLease(annotatedCtx); pErr != nil {
					if _, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); !ok {
						log.Warningf(annotatedCtx, "failed to proactively renew lease: %s", pErr)
					}
					s.renewableLeases.Delete(k)
				}
				return true
			})

			if len(repls) > 0 {
				timer.Reset(renewalDuration)
			}
			select {
			case <-s.renewableLeasesSignal:
			case <-timer.C:
				timer.Read = true
			case <-s.stopper.ShouldStop():
				return
			}
		}
	})
}

// startClosedTimestampRangefeedSubscriber establishes a new ClosedTimestamp
// subscription and runs an infinite loop to listen for closed timestamp updates
// and inform Replicas with active Rangefeeds about them.
func (s *Store) startClosedTimestampRangefeedSubscriber(ctx context.Context) {
	// NB: We can't use Stopper.RunWorker because doing so would race with
	// calling Stopper.Stop. We give the subscription channel a small capacity
	// to avoid blocking the closed timestamp goroutine.
	ch := make(chan ctpb.Entry, 8)
	const name = "closedts-rangefeed-subscriber"
	if err := s.stopper.RunAsyncTask(ctx, name, func(ctx context.Context) {
		s.cfg.ClosedTimestamp.Provider.Subscribe(ctx, ch)
	}); err != nil {
		return
	}

	s.stopper.RunWorker(ctx, func(ctx context.Context) {
		var replIDs []roachpb.RangeID
		for {
			select {
			case <-ch:
				// Drain all notifications from the channel.
			loop:
				for {
					select {
					case _, ok := <-ch:
						if !ok {
							break loop
						}
					default:
						break loop
					}
				}

				// Gather replicas to notify under lock.
				s.rangefeedReplicas.Lock()
				for replID := range s.rangefeedReplicas.m {
					replIDs = append(replIDs, replID)
				}
				s.rangefeedReplicas.Unlock()

				// Notify each replica with an active rangefeed to
				// check for an updated closed timestamp.
				for _, replID := range replIDs {
					repl, err := s.GetReplica(replID)
					if err != nil {
						continue
					}
					repl.handleClosedTimestampUpdate(ctx)
				}
				replIDs = replIDs[:0]
			case <-s.stopper.ShouldQuiesce():
				return
			}
		}
	})
}

func (s *Store) addReplicaWithRangefeed(rangeID roachpb.RangeID) {
	s.rangefeedReplicas.Lock()
	s.rangefeedReplicas.m[rangeID] = struct{}{}
	s.rangefeedReplicas.Unlock()
}

func (s *Store) removeReplicaWithRangefeed(rangeID roachpb.RangeID) {
	s.rangefeedReplicas.Lock()
	delete(s.rangefeedReplicas.m, rangeID)
	s.rangefeedReplicas.Unlock()
}

// systemGossipUpdate is a callback for gossip updates to
// the system config which affect range split boundaries.
func (s *Store) systemGossipUpdate(sysCfg *config.SystemConfig) {
	ctx := s.AnnotateCtx(context.Background())
	s.computeInitialMetrics.Do(func() {
		// Metrics depend in part on the system config. Compute them as soon as we
		// get the first system config, then periodically in the background
		// (managed by the Node).
		if err := s.ComputeMetrics(ctx, -1); err != nil {
			log.Infof(ctx, "%s: failed initial metrics computation: %s", s, err)
		}
		log.Event(ctx, "computed initial metrics")
	})

	// We'll want to offer all replicas to the split and merge queues. Be a little
	// careful about not spawning too many individual goroutines.

	// For every range, update its zone config and check if it needs to
	// be split or merged.
	now := s.cfg.Clock.Now()
	newStoreReplicaVisitor(s).Visit(func(repl *Replica) bool {
		key := repl.Desc().StartKey
		zone, err := sysCfg.GetZoneConfigForKey(key)
		if err != nil {
			if log.V(1) {
				log.Infof(context.TODO(), "failed to get zone config for key %s", key)
			}
			zone = s.cfg.DefaultZoneConfig
		}
		repl.SetZoneConfig(zone)
		s.splitQueue.Async(ctx, "gossip update", true /* wait */, func(ctx context.Context, h queueHelper) {
			h.MaybeAdd(ctx, repl, now)
		})
		s.mergeQueue.Async(ctx, "gossip update", true /* wait */, func(ctx context.Context, h queueHelper) {
			h.MaybeAdd(ctx, repl, now)
		})
		return true // more
	})
}

func (s *Store) asyncGossipStore(ctx context.Context, reason string, useCached bool) {
	if err := s.stopper.RunAsyncTask(
		ctx, fmt.Sprintf("storage.Store: gossip on %s", reason),
		func(ctx context.Context) {
			if err := s.GossipStore(ctx, useCached); err != nil {
				log.Warningf(ctx, "error gossiping on %s: %+v", reason, err)
			}
		}); err != nil {
		log.Warningf(ctx, "unable to gossip on %s: %+v", reason, err)
	}
}

// GossipStore broadcasts the store on the gossip network.
func (s *Store) GossipStore(ctx context.Context, useCached bool) error {
	select {
	case <-s.cfg.Gossip.Connected:
	default:
		// Nothing to do if gossip is not connected.
		return nil
	}

	// Temporarily indicate that we're gossiping the store capacity to avoid
	// recursively triggering a gossip of the store capacity.
	syncutil.StoreFloat64(&s.gossipQueriesPerSecondVal, -1)
	syncutil.StoreFloat64(&s.gossipWritesPerSecondVal, -1)

	storeDesc, err := s.Descriptor(useCached)
	if err != nil {
		return errors.Wrapf(err, "problem getting store descriptor for store %+v", s.Ident)
	}

	// Set countdown target for re-gossiping capacity earlier than
	// the usual periodic interval. Re-gossip more rapidly for RangeCount
	// changes because allocators with stale information are much more
	// likely to make bad decisions.
	rangeCountdown := float64(storeDesc.Capacity.RangeCount) * s.cfg.GossipWhenCapacityDeltaExceedsFraction
	atomic.StoreInt32(&s.gossipRangeCountdown, int32(math.Ceil(math.Min(rangeCountdown, 3))))
	leaseCountdown := float64(storeDesc.Capacity.LeaseCount) * s.cfg.GossipWhenCapacityDeltaExceedsFraction
	atomic.StoreInt32(&s.gossipLeaseCountdown, int32(math.Ceil(math.Max(leaseCountdown, 1))))
	syncutil.StoreFloat64(&s.gossipQueriesPerSecondVal, storeDesc.Capacity.QueriesPerSecond)
	syncutil.StoreFloat64(&s.gossipWritesPerSecondVal, storeDesc.Capacity.WritesPerSecond)

	// Unique gossip key per store.
	gossipStoreKey := gossip.MakeStoreKey(storeDesc.StoreID)
	// Gossip store descriptor.
	return s.cfg.Gossip.AddInfoProto(gossipStoreKey, storeDesc, gossip.StoreTTL)
}

type capacityChangeEvent int

const (
	rangeAddEvent capacityChangeEvent = iota
	rangeRemoveEvent
	leaseAddEvent
	leaseRemoveEvent
)

// maybeGossipOnCapacityChange decrements the countdown on range
// and leaseholder counts. If it reaches 0, then we trigger an
// immediate gossip of this store's descriptor, to include updated
// capacity information.
func (s *Store) maybeGossipOnCapacityChange(ctx context.Context, cce capacityChangeEvent) {
	if s.cfg.TestingKnobs.DisableLeaseCapacityGossip && (cce == leaseAddEvent || cce == leaseRemoveEvent) {
		return
	}

	// Incrementally adjust stats to keep them up to date even if the
	// capacity is gossiped, but isn't due yet to be recomputed from scratch.
	s.cachedCapacity.Lock()
	switch cce {
	case rangeAddEvent:
		s.cachedCapacity.RangeCount++
	case rangeRemoveEvent:
		s.cachedCapacity.RangeCount--
	case leaseAddEvent:
		s.cachedCapacity.LeaseCount++
	case leaseRemoveEvent:
		s.cachedCapacity.LeaseCount--
	}
	s.cachedCapacity.Unlock()

	if ((cce == rangeAddEvent || cce == rangeRemoveEvent) && atomic.AddInt32(&s.gossipRangeCountdown, -1) == 0) ||
		((cce == leaseAddEvent || cce == leaseRemoveEvent) && atomic.AddInt32(&s.gossipLeaseCountdown, -1) == 0) {
		// Reset countdowns to avoid unnecessary gossiping.
		atomic.StoreInt32(&s.gossipRangeCountdown, 0)
		atomic.StoreInt32(&s.gossipLeaseCountdown, 0)
		s.asyncGossipStore(ctx, "capacity change", true /* useCached */)
	}
}

// recordNewPerSecondStats takes recently calculated values for the number of
// queries and key writes the store is handling and decides whether either has
// changed enough to justify re-gossiping the store's capacity.
func (s *Store) recordNewPerSecondStats(newQPS, newWPS float64) {
	oldQPS := syncutil.LoadFloat64(&s.gossipQueriesPerSecondVal)
	oldWPS := syncutil.LoadFloat64(&s.gossipWritesPerSecondVal)
	if oldQPS == -1 || oldWPS == -1 {
		// Gossiping of store capacity is already ongoing.
		return
	}

	const minAbsoluteChange = 100
	updateForQPS := (newQPS < oldQPS*.5 || newQPS > oldQPS*1.5) && math.Abs(newQPS-oldQPS) > minAbsoluteChange
	updateForWPS := (newWPS < oldWPS*.5 || newWPS > oldWPS*1.5) && math.Abs(newWPS-oldWPS) > minAbsoluteChange

	if !updateForQPS && !updateForWPS {
		return
	}

	var message string
	if updateForQPS && updateForWPS {
		message = "queries-per-second and writes-per-second change"
	} else if updateForQPS {
		message = "queries-per-second change"
	} else {
		message = "writes-per-second change"
	}
	// TODO(a-robinson): Use the provided values to avoid having to recalculate
	// them in GossipStore.
	s.asyncGossipStore(context.TODO(), message, false /* useCached */)
}

// VisitReplicas invokes the visitor on the Store's Replicas until the visitor returns false.
// Replicas which are added to the Store after iteration begins may or may not be observed.
func (s *Store) VisitReplicas(visitor func(*Replica) bool) {
	v := newStoreReplicaVisitor(s)
	v.Visit(visitor)
}

// WriteLastUpTimestamp records the supplied timestamp into the "last up" key
// on this store. This value should be refreshed whenever this store's node
// updates its own liveness record; it is used by a restarting store to
// determine the approximate time that it stopped.
func (s *Store) WriteLastUpTimestamp(ctx context.Context, time hlc.Timestamp) error {
	ctx = s.AnnotateCtx(ctx)
	return engine.MVCCPutProto(
		ctx,
		s.engine,
		nil,
		keys.StoreLastUpKey(),
		hlc.Timestamp{},
		nil,
		&time,
	)
}

// ReadLastUpTimestamp returns the "last up" timestamp recorded in this store.
// This value can be used to approximate the last time the engine was was being
// served as a store by a running node. If the store does not contain a "last
// up" timestamp (for example, on a newly bootstrapped store), the zero
// timestamp is returned instead.
func (s *Store) ReadLastUpTimestamp(ctx context.Context) (hlc.Timestamp, error) {
	var timestamp hlc.Timestamp
	ok, err := engine.MVCCGetProto(ctx, s.Engine(), keys.StoreLastUpKey(), hlc.Timestamp{},
		&timestamp, engine.MVCCGetOptions{})
	if err != nil {
		return hlc.Timestamp{}, err
	} else if !ok {
		return hlc.Timestamp{}, nil
	}
	return timestamp, nil
}

// WriteHLCUpperBound records an upper bound to the wall time of the HLC
func (s *Store) WriteHLCUpperBound(ctx context.Context, time int64) error {
	ctx = s.AnnotateCtx(ctx)
	ts := hlc.Timestamp{WallTime: time}
	batch := s.Engine().NewBatch()
	// Write has to sync to disk to ensure HLC monotonicity across restarts
	defer batch.Close()
	if err := engine.MVCCPutProto(
		ctx,
		batch,
		nil,
		keys.StoreHLCUpperBoundKey(),
		hlc.Timestamp{},
		nil,
		&ts,
	); err != nil {
		return err
	}

	if err := batch.Commit(true /* sync */); err != nil {
		return err
	}
	return nil
}

// ReadHLCUpperBound returns the upper bound to the wall time of the HLC
// If this value does not exist 0 is returned
func ReadHLCUpperBound(ctx context.Context, e engine.Engine) (int64, error) {
	var timestamp hlc.Timestamp
	ok, err := engine.MVCCGetProto(ctx, e, keys.StoreHLCUpperBoundKey(), hlc.Timestamp{},
		&timestamp, engine.MVCCGetOptions{})
	if err != nil {
		return 0, err
	} else if !ok {
		return 0, nil
	}
	return timestamp.WallTime, nil
}

// ReadMaxHLCUpperBound returns the maximum of the stored hlc upper bounds
// among all the engines. This value is optionally persisted by the server and
// it is guaranteed to be higher than any wall time used by the HLC. If this
// value is persisted, HLC wall clock monotonicity is guaranteed across server
// restarts
func ReadMaxHLCUpperBound(ctx context.Context, engines []engine.Engine) (int64, error) {
	var hlcUpperBound int64
	for _, e := range engines {
		engineHLCUpperBound, err := ReadHLCUpperBound(ctx, e)
		if err != nil {
			return 0, err
		}
		if engineHLCUpperBound > hlcUpperBound {
			hlcUpperBound = engineHLCUpperBound
		}
	}
	return hlcUpperBound, nil
}

func checkEngineEmpty(ctx context.Context, eng engine.Engine) error {
	kvs, err := engine.Scan(
		eng,
		engine.MakeMVCCMetadataKey(roachpb.Key(roachpb.RKeyMin)),
		engine.MakeMVCCMetadataKey(roachpb.Key(roachpb.RKeyMax)),
		10,
	)
	if err != nil {
		return err
	}
	if len(kvs) > 0 {
		// See if this is an already-bootstrapped store.
		ident, err := ReadStoreIdent(ctx, eng)
		if err != nil {
			return errors.Wrap(err, "unable to read store ident")
		}
		keyVals := make([]string, len(kvs))
		for i, kv := range kvs {
			keyVals[i] = fmt.Sprintf("%s: %q", kv.Key, kv.Value)
		}
		return errors.Errorf("engine belongs to store %s, contains %s", ident.String(), keyVals)
	}
	return nil
}

// GetReplica fetches a replica by Range ID. Returns an error if no replica is found.
func (s *Store) GetReplica(rangeID roachpb.RangeID) (*Replica, error) {
	if value, ok := s.mu.replicas.Load(int64(rangeID)); ok {
		return (*Replica)(value), nil
	}
	return nil, roachpb.NewRangeNotFoundError(rangeID, s.StoreID())
}

// LookupReplica looks up the replica that contains the specified key. It
// returns nil if no such replica exists.
func (s *Store) LookupReplica(key roachpb.RKey) *Replica {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var repl *Replica
	s.mu.replicasByKey.DescendLessOrEqual(rangeBTreeKey(key), func(item btree.Item) bool {
		repl, _ = item.(*Replica)
		// Stop iterating immediately. The first item we see is the only one that
		// can possibly contain key.
		return false
	})
	if repl == nil || !repl.Desc().ContainsKey(key) {
		return nil
	}
	return repl
}

// lookupPrecedingReplica finds the replica in this store that immediately
// precedes the specified key without containing it. It returns nil if no such
// replica exists. It ignores replica placeholders.
//
// Concretely, when key represents a key within replica R,
// lookupPrecedingReplica returns the replica that immediately precedes R in
// replicasByKey.
func (s *Store) lookupPrecedingReplica(key roachpb.RKey) *Replica {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var repl *Replica
	s.mu.replicasByKey.DescendLessOrEqual(rangeBTreeKey(key), func(item btree.Item) bool {
		if r, ok := item.(*Replica); ok && !r.ContainsKey(key.AsRawKey()) {
			repl = r
			return false // stop iterating
		}
		return true // keep iterating
	})
	return repl
}

// getOverlappingKeyRangeLocked returns a KeyRange from the Store overlapping the given
// descriptor (or nil if no such KeyRange exists).
func (s *Store) getOverlappingKeyRangeLocked(rngDesc *roachpb.RangeDescriptor) KeyRange {
	var kr KeyRange
	s.mu.replicasByKey.DescendLessOrEqual(rangeBTreeKey(rngDesc.EndKey),
		func(item btree.Item) bool {
			if kr0 := item.(KeyRange); kr0.startKey().Less(rngDesc.EndKey) {
				kr = kr0
				return false // stop iterating
			}
			return true // keep iterating
		})
	if kr != nil && rngDesc.StartKey.Less(kr.Desc().EndKey) {
		return kr
	}
	return nil
}

// RaftStatus returns the current raft status of the local replica of
// the given range.
func (s *Store) RaftStatus(rangeID roachpb.RangeID) *raft.Status {
	if value, ok := s.mu.replicas.Load(int64(rangeID)); ok {
		return (*Replica)(value).RaftStatus()
	}
	return nil
}

// ClusterID accessor.
func (s *Store) ClusterID() uuid.UUID { return s.Ident.ClusterID }

// StoreID accessor.
func (s *Store) StoreID() roachpb.StoreID { return s.Ident.StoreID }

// Clock accessor.
func (s *Store) Clock() *hlc.Clock { return s.cfg.Clock }

// Engine accessor.
func (s *Store) Engine() engine.Engine { return s.engine }

// DB accessor.
func (s *Store) DB() *client.DB { return s.cfg.DB }

// Gossip accessor.
func (s *Store) Gossip() *gossip.Gossip { return s.cfg.Gossip }

// Compactor accessor.
func (s *Store) Compactor() *compactor.Compactor { return s.compactor }

// Stopper accessor.
func (s *Store) Stopper() *stop.Stopper { return s.stopper }

// TestingKnobs accessor.
func (s *Store) TestingKnobs() *StoreTestingKnobs { return &s.cfg.TestingKnobs }

// IsDraining accessor.
func (s *Store) IsDraining() bool {
	return s.draining.Load().(bool)
}

// NewRangeDescriptor creates a new descriptor based on start and end
// keys and the supplied roachpb.Replicas slice. It allocates a new
// range ID and returns a RangeDescriptor whose Replicas are a copy
// of the supplied replicas slice, with appropriate ReplicaIDs assigned.
func (s *Store) NewRangeDescriptor(
	ctx context.Context, start, end roachpb.RKey, replicas []roachpb.ReplicaDescriptor,
) (*roachpb.RangeDescriptor, error) {
	id, err := s.rangeIDAlloc.Allocate(ctx)
	if err != nil {
		return nil, err
	}
	replicas = append([]roachpb.ReplicaDescriptor(nil), replicas...)
	for i := range replicas {
		replicas[i].ReplicaID = roachpb.ReplicaID(i + 1)
	}
	desc := &roachpb.RangeDescriptor{
		RangeID:       roachpb.RangeID(id),
		StartKey:      start,
		EndKey:        end,
		NextReplicaID: roachpb.ReplicaID(len(replicas) + 1),
	}
	desc.SetReplicas(roachpb.MakeReplicaDescriptors(&replicas))
	return desc, nil
}

// splitPreApply is called when the raft command is applied. Any
// changes to the given ReadWriter will be written atomically with the
// split commit.
func splitPreApply(ctx context.Context, eng engine.ReadWriter, split roachpb.SplitTrigger) {
	// Update the raft HardState with the new Commit value now that the
	// replica is initialized (combining it with existing or default
	// Term and Vote).
	rsl := stateloader.Make(split.RightDesc.RangeID)
	if err := rsl.SynthesizeRaftState(ctx, eng); err != nil {
		log.Fatal(ctx, err)
	}
}

// splitPostApply is the part of the split trigger which coordinates the actual
// split with the Store. Requires that Replica.raftMu is held.
//
// TODO(tschottdorf): Want to merge this with SplitRange, but some legacy
// testing code calls SplitRange directly.
func splitPostApply(
	ctx context.Context, deltaMS enginepb.MVCCStats, split *roachpb.SplitTrigger, r *Replica,
) {
	// The right hand side of the split was already created (and its raftMu
	// acquired) in Replica.acquireSplitLock. It must be present here.
	rightRng, err := r.store.GetReplica(split.RightDesc.RangeID)
	if err != nil {
		log.Fatalf(ctx, "unable to find RHS replica: %+v", err)
	}
	{
		rightRng.mu.Lock()
		// Already holding raftMu, see above.
		err := rightRng.initRaftMuLockedReplicaMuLocked(&split.RightDesc, r.store.Clock(), 0)
		rightRng.mu.Unlock()
		if err != nil {
			log.Fatal(ctx, err)
		}
	}

	// Finish initialization of the RHS.

	// This initialMaxClosedValue is created here to ensure that follower reads
	// do not regress following the split. After the split occurs there will be no
	// information in the closedts subsystem about the newly minted RHS range from
	// its leaseholder's store. Furthermore, the RHS will have a lease start time
	// equal to that of the LHS which might be quite old. This means that
	// timestamps which follow the least StartTime for the LHS part are below the
	// current closed timestamp for the LHS would no longer be readable on the RHS
	// after the split. It is critical that this call to maxClosed happen during
	// the splitPostApply so that it refers to a LAI that is equal to the index at
	// which this lease was applied. If it were to refer to a LAI after the split
	// then the value of initialMaxClosed might be unsafe.
	initialMaxClosed := r.maxClosed(ctx)
	r.mu.Lock()
	rightRng.mu.Lock()
	// Copy the minLeaseProposedTS from the LHS.
	rightRng.mu.minLeaseProposedTS = r.mu.minLeaseProposedTS
	rightRng.mu.initialMaxClosed = initialMaxClosed
	rightLease := *rightRng.mu.state.Lease
	rightRng.mu.Unlock()
	r.mu.Unlock()

	// We need to explicitly wake up the Raft group on the right-hand range or
	// else the range could be underreplicated for an indefinite period of time.
	//
	// Specifically, suppose one of the replicas of the left-hand range never
	// applies this split trigger, e.g., because it catches up via a snapshot that
	// advances it past this split. That store won't create the right-hand replica
	// until it receives a Raft message addressed to the right-hand range. But
	// since new replicas start out quiesced, unless we explicitly awaken the
	// Raft group, there might not be any Raft traffic for quite a while.
	if err := rightRng.withRaftGroup(true, func(r *raft.RawNode) (unquiesceAndWakeLeader bool, _ error) {
		return true, nil
	}); err != nil {
		log.Fatalf(ctx, "unable to create raft group for right-hand range in split: %+v", err)
	}

	// Invoke the leasePostApply method to ensure we properly initialize
	// the replica according to whether it holds the lease. This enables
	// the txnWaitQueue.
	rightRng.leasePostApply(ctx, rightLease, false /* permitJump */)

	// Add the RHS replica to the store. This step atomically updates
	// the EndKey of the LHS replica and also adds the RHS replica
	// to the store's replica map.
	if err := r.store.SplitRange(ctx, r, rightRng, split.LeftDesc); err != nil {
		// Our in-memory state has diverged from the on-disk state.
		log.Fatalf(ctx, "%s: failed to update Store after split: %+v", r, err)
	}

	// Update store stats with difference in stats before and after split.
	r.store.metrics.addMVCCStats(deltaMS)

	now := r.store.Clock().Now()

	// While performing the split, zone config changes or a newly created table
	// might require the range to be split again. Enqueue both the left and right
	// ranges to speed up such splits. See #10160.
	r.store.splitQueue.MaybeAddAsync(ctx, r, now)
	r.store.splitQueue.MaybeAddAsync(ctx, rightRng, now)

	// If the range was not properly replicated before the split, the replicate
	// queue may not have picked it up (due to the need for a split). Enqueue
	// both the left and right ranges to speed up a potentially necessary
	// replication. See #7022 and #7800.
	r.store.replicateQueue.MaybeAddAsync(ctx, r, now)
	r.store.replicateQueue.MaybeAddAsync(ctx, rightRng, now)

	if len(split.RightDesc.Replicas().All()) == 1 {
		// TODO(peter): In single-node clusters, we enqueue the right-hand side of
		// the split (the new range) for Raft processing so that the corresponding
		// Raft group is created. This shouldn't be necessary for correctness, but
		// some tests rely on this (e.g. server.TestNodeStatusWritten).
		r.store.enqueueRaftUpdateCheck(rightRng.RangeID)
	}
}

// SplitRange shortens the original range to accommodate the new range. The new
// range is added to the ranges map and the replicasByKey btree. origRng.raftMu
// and newRng.raftMu must be held.
//
// This is only called from the split trigger in the context of the execution
// of a Raft command.
func (s *Store) SplitRange(
	ctx context.Context, leftRepl, rightRepl *Replica, newLeftDesc roachpb.RangeDescriptor,
) error {
	oldLeftDesc := leftRepl.Desc()
	rightDesc := rightRepl.Desc()

	if !bytes.Equal(oldLeftDesc.EndKey, rightDesc.EndKey) ||
		bytes.Compare(oldLeftDesc.StartKey, rightDesc.StartKey) >= 0 {
		return errors.Errorf("left range is not splittable by right range: %+v, %+v", oldLeftDesc, rightDesc)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if exRng, ok := s.mu.uninitReplicas[rightDesc.RangeID]; ok {
		// If we have an uninitialized replica of the new range we require pointer
		// equivalence with rightRepl. See Store.splitTriggerPostApply().
		if exRng != rightRepl {
			log.Fatalf(ctx, "found unexpected uninitialized replica: %s vs %s", exRng, rightRepl)
		}
		// NB: We only remove from uninitReplicas and the replicaQueues maps here
		// so that we don't leave open a window where a replica is temporarily not
		// present in Store.mu.replicas.
		delete(s.mu.uninitReplicas, rightDesc.RangeID)
		s.replicaQueues.Delete(int64(rightDesc.RangeID))
	}

	leftRepl.setDesc(ctx, &newLeftDesc)

	// Clear the LHS txn wait queue, to redirect to the RHS if
	// appropriate. We do this after setDescWithoutProcessUpdate
	// to ensure that no pre-split commands are inserted into the
	// txnWaitQueue after we clear it.
	leftRepl.txnWaitQueue.Clear(false /* disable */)

	// The rangefeed processor will no longer be provided logical ops for
	// its entire range, so it needs to be shut down and all registrations
	// need to retry.
	// TODO(nvanbenschoten): It should be possible to only reject registrations
	// that overlap with the new range of the split and keep registrations that
	// are only interested in keys that are still on the original range running.
	leftRepl.disconnectRangefeedWithReason(
		roachpb.RangeFeedRetryError_REASON_RANGE_SPLIT,
	)

	// Clear the original range's request stats, since they include requests for
	// spans that are now owned by the new range.
	leftRepl.leaseholderStats.resetRequestCounts()
	leftRepl.writeStats.splitRequestCounts(rightRepl.writeStats)

	if err := s.addReplicaInternalLocked(rightRepl); err != nil {
		return errors.Errorf("unable to add replica %v: %s", rightRepl, err)
	}

	// Update the replica's cached byte thresholds. This is a no-op if the system
	// config is not available, in which case we rely on the next gossip update
	// to perform the update.
	if err := rightRepl.updateRangeInfo(rightRepl.Desc()); err != nil {
		return err
	}

	// Add the range to metrics and maybe gossip on capacity change.
	s.metrics.ReplicaCount.Inc(1)
	s.maybeGossipOnCapacityChange(ctx, rangeAddEvent)
	return nil
}

// MergeRange expands the left-hand replica, leftRepl, to absorb the right-hand
// replica, identified by rightDesc. freezeStart specifies the time at which the
// right-hand replica promised to stop serving traffic and is used to initialize
// the timestamp cache's low water mark for the right-hand keyspace. The
// right-hand replica must exist on this store and the raftMus for both the
// left-hand and right-hand replicas must be held.
func (s *Store) MergeRange(
	ctx context.Context,
	leftRepl *Replica,
	newLeftDesc, rightDesc roachpb.RangeDescriptor,
	freezeStart hlc.Timestamp,
) error {
	if oldLeftDesc := leftRepl.Desc(); !oldLeftDesc.EndKey.Less(newLeftDesc.EndKey) {
		return errors.Errorf("the new end key is not greater than the current one: %+v <= %+v",
			newLeftDesc.EndKey, oldLeftDesc.EndKey)
	}

	rightRepl, err := s.GetReplica(rightDesc.RangeID)
	if err != nil {
		return err
	}

	leftRepl.raftMu.AssertHeld()
	rightRepl.raftMu.AssertHeld()

	// Shut down rangefeed processors on either side of the merge.
	//
	// It isn't strictly necessary to shut-down a rangefeed processor on the
	// surviving replica in a merge, but we choose to in order to avoid clients
	// who were monitoring both sides of the merge from establishing multiple
	// partial rangefeeds to the surviving range.
	// TODO(nvanbenschoten): does this make sense? We could just adjust the
	// bounds of the leftRepl.Processor.
	//
	// NB: removeReplicaImpl also disconnects any initialized rangefeeds with
	// REASON_REPLICA_REMOVED. That's ok because we will have already
	// disconnected the rangefeed here.
	leftRepl.disconnectRangefeedWithReason(
		roachpb.RangeFeedRetryError_REASON_RANGE_MERGED,
	)
	rightRepl.disconnectRangefeedWithReason(
		roachpb.RangeFeedRetryError_REASON_RANGE_MERGED,
	)

	if err := rightRepl.postDestroyRaftMuLocked(ctx, rightRepl.GetMVCCStats()); err != nil {
		return err
	}

	// Note that we were called (indirectly) from raft processing so we must
	// call removeReplicaImpl directly to avoid deadlocking on the right-hand
	// replica's raftMu.
	if err := s.removeReplicaImpl(ctx, rightRepl, rightDesc.NextReplicaID, RemoveOptions{
		DestroyData: false, // the replica was destroyed when the merge commit applied
	}); err != nil {
		return errors.Errorf("cannot remove range: %s", err)
	}

	if leftRepl.leaseholderStats != nil {
		leftRepl.leaseholderStats.resetRequestCounts()
	}
	if leftRepl.writeStats != nil {
		// Note: this could be drastically improved by adding a replicaStats method
		// that merges stats. Resetting stats is typically bad for the rebalancing
		// logic that depends on them.
		leftRepl.writeStats.resetRequestCounts()
	}

	// Clear the wait queue to redirect the queued transactions to the
	// left-hand replica, if necessary.
	rightRepl.txnWaitQueue.Clear(true /* disable */)

	leftLease, _ := leftRepl.GetLease()
	rightLease, _ := rightRepl.GetLease()
	if leftLease.OwnedBy(s.Ident.StoreID) && !rightLease.OwnedBy(s.Ident.StoreID) {
		// We hold the lease for the LHS, but do not hold the lease for the RHS.
		// That means we don't have up-to-date timestamp cache entries for the
		// keyspace previously owned by the RHS. Bump the low water mark for the RHS
		// keyspace to freezeStart, the time at which the RHS promised to stop
		// serving traffic, as freezeStart is guaranteed to be greater than any
		// entry in the RHS's timestamp cache.
		//
		// Note that we need to update our clock with freezeStart to preserve the
		// invariant that our clock is always greater than or equal to any
		// timestamps in the timestamp cache. For a full discussion, see the comment
		// on TestStoreRangeMergeTimestampCacheCausality.
		_ = s.Clock().Update(freezeStart)
		setTimestampCacheLowWaterMark(s.tsCache, &rightDesc, freezeStart)
	}

	// Update the subsuming range's descriptor.
	leftRepl.setDesc(ctx, &newLeftDesc)
	return nil
}

// addReplicaInternalLocked adds the replica to the replicas map and the
// replicasByKey btree. Returns an error if a replica with
// the same Range ID or a KeyRange that overlaps has already been added to
// this store. addReplicaInternalLocked requires that the store lock is held.
func (s *Store) addReplicaInternalLocked(repl *Replica) error {
	if !repl.IsInitialized() {
		return errors.Errorf("attempted to add uninitialized replica %s", repl)
	}

	if err := s.addReplicaToRangeMapLocked(repl); err != nil {
		return err
	}

	if exRange := s.getOverlappingKeyRangeLocked(repl.Desc()); exRange != nil {
		return errors.Errorf("%s: cannot addReplicaInternalLocked; range %s has overlapping range %s", s, repl, exRange.Desc())
	}

	if exRngItem := s.mu.replicasByKey.ReplaceOrInsert(repl); exRngItem != nil {
		return errors.Errorf("%s: cannot addReplicaInternalLocked; range for key %v already exists in replicasByKey btree", s,
			exRngItem.(KeyRange).startKey())
	}

	return nil
}

// addPlaceholderLocked adds the specified placeholder. Requires that the
// raftMu of the replica whose place is being held is locked.
func (s *Store) addPlaceholder(placeholder *ReplicaPlaceholder) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addPlaceholderLocked(placeholder)
}

// addPlaceholderLocked adds the specified placeholder. Requires that Store.mu
// and the raftMu of the replica whose place is being held are locked.
func (s *Store) addPlaceholderLocked(placeholder *ReplicaPlaceholder) error {
	rangeID := placeholder.Desc().RangeID
	if exRng := s.mu.replicasByKey.ReplaceOrInsert(placeholder); exRng != nil {
		return errors.Errorf("%s overlaps with existing KeyRange %s in replicasByKey btree", placeholder, exRng)
	}
	if exRng, ok := s.mu.replicaPlaceholders[rangeID]; ok {
		return errors.Errorf("%s has ID collision with existing KeyRange %s", placeholder, exRng)
	}
	s.mu.replicaPlaceholders[rangeID] = placeholder
	return nil
}

// removePlaceholder removes a placeholder for the specified range if it
// exists, returning true if a placeholder was present and removed and false
// otherwise. Requires that the raftMu of the replica whose place is being held
// is locked.
func (s *Store) removePlaceholder(ctx context.Context, rngID roachpb.RangeID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.removePlaceholderLocked(ctx, rngID)
}

// removePlaceholderLocked removes the specified placeholder. Requires that
// Store.mu and the raftMu of the replica whose place is being held are locked.
func (s *Store) removePlaceholderLocked(ctx context.Context, rngID roachpb.RangeID) bool {
	placeholder, ok := s.mu.replicaPlaceholders[rngID]
	if !ok {
		return false
	}
	switch exRng := s.mu.replicasByKey.Delete(placeholder).(type) {
	case *ReplicaPlaceholder:
		delete(s.mu.replicaPlaceholders, rngID)
		if exRng2 := s.getOverlappingKeyRangeLocked(&exRng.rangeDesc); exRng2 != nil {
			log.Fatalf(ctx, "corrupted replicasByKey map: %s and %s overlapped", exRng, exRng2)
		}
		return true
	case nil:
		log.Fatalf(ctx, "r%d: placeholder not found", rngID)
	default:
		log.Fatalf(ctx, "r%d: expected placeholder, got %T", rngID, exRng)
	}
	return false // appease the compiler
}

// addReplicaToRangeMapLocked adds the replica to the replicas map.
func (s *Store) addReplicaToRangeMapLocked(repl *Replica) error {
	// It's ok for the replica to exist in the replicas map as long as it is the
	// same replica object. This occurs during splits where the right-hand side
	// is added to the replicas map before it is initialized.
	if existing, loaded := s.mu.replicas.LoadOrStore(
		int64(repl.RangeID), unsafe.Pointer(repl)); loaded && (*Replica)(existing) != repl {
		return errors.Errorf("%s: replica already exists", repl)
	}
	// Check whether the replica is unquiesced but not in the map. This
	// can happen during splits and merges, where the uninitialized (but
	// also unquiesced) replica is removed from the unquiesced replica
	// map in advance of this method being called.
	s.unquiescedReplicas.Lock()
	if _, ok := s.unquiescedReplicas.m[repl.RangeID]; !repl.mu.quiescent && !ok {
		s.unquiescedReplicas.m[repl.RangeID] = struct{}{}
	}
	s.unquiescedReplicas.Unlock()
	return nil
}

// RemoveOptions bundles boolean parameters for Store.RemoveReplica.
type RemoveOptions struct {
	DestroyData bool
}

// RemoveReplica removes the replica from the store's replica map and from the
// sorted replicasByKey btree.
//
// The NextReplicaID from the replica descriptor that was used to make the
// removal decision is passed in. Removal is aborted if the replica ID has
// advanced to or beyond the NextReplicaID since the removal decision was made.
//
// If opts.DestroyReplica is false, replica.destroyRaftMuLocked is not called.
func (s *Store) RemoveReplica(
	ctx context.Context, rep *Replica, nextReplicaID roachpb.ReplicaID, opts RemoveOptions,
) error {
	rep.raftMu.Lock()
	defer rep.raftMu.Unlock()
	return s.removeReplicaImpl(ctx, rep, nextReplicaID, opts)
}

// removeReplicaImpl is the implementation of RemoveReplica, which is sometimes
// called directly when the necessary lock is already held. It requires that
// Replica.raftMu is held and that s.mu is not held.
func (s *Store) removeReplicaImpl(
	ctx context.Context, rep *Replica, nextReplicaID roachpb.ReplicaID, opts RemoveOptions,
) error {
	// We check both rep.mu.ReplicaID and rep.mu.state.Desc's replica ID because
	// they can differ in cases when a replica's ID is increased due to an
	// incoming raft message (see #14231 for background).
	rep.mu.Lock()
	replicaID := rep.mu.replicaID
	if rep.mu.replicaID >= nextReplicaID {
		rep.mu.Unlock()
		return errors.Errorf("cannot remove replica %s; replica ID has changed (%s >= %s)",
			rep, rep.mu.replicaID, nextReplicaID)
	}
	desc := rep.mu.state.Desc
	if repDesc, ok := desc.GetReplicaDescriptor(s.StoreID()); ok && repDesc.ReplicaID >= nextReplicaID {
		rep.mu.Unlock()
		return errors.Errorf("cannot remove replica %s; replica descriptor's ID has changed (%s >= %s)",
			rep, repDesc.ReplicaID, nextReplicaID)
	}
	rep.mu.Unlock()

	if _, err := s.GetReplica(rep.RangeID); err != nil {
		return err
	}

	// During merges, the context might have the subsuming range, so we explicitly
	// log the replica to be removed.
	log.Infof(ctx, "removing replica r%d/%d", rep.RangeID, replicaID)

	s.mu.Lock()
	if placeholder := s.getOverlappingKeyRangeLocked(desc); placeholder != rep {
		// This is a fatal error because uninitialized replicas shouldn't make it
		// this far. This method will need some changes when we introduce GC of
		// uninitialized replicas.
		s.mu.Unlock()
		log.Fatalf(ctx, "replica %+v unexpectedly overlapped by %+v", rep, placeholder)
	}
	// Adjust stats before calling Destroy. This can be called before or after
	// Destroy, but this configuration helps avoid races in stat verification
	// tests.
	s.metrics.subtractMVCCStats(rep.GetMVCCStats())
	s.metrics.ReplicaCount.Dec(1)
	s.mu.Unlock()

	// The replica will no longer exist, so cancel any rangefeed registrations.
	rep.disconnectRangefeedWithReason(
		roachpb.RangeFeedRetryError_REASON_REPLICA_REMOVED,
	)

	// Mark the replica as destroyed and (optionally) destroy the on-disk data
	// while not holding Store.mu. This is safe because we're holding
	// Replica.raftMu and the replica is present in Store.mu.replicasByKey
	// (preventing any concurrent access to the replica's key range).

	rep.readOnlyCmdMu.Lock()
	rep.mu.Lock()
	rep.cancelPendingCommandsLocked()
	rep.mu.internalRaftGroup = nil
	rep.mu.destroyStatus.Set(roachpb.NewRangeNotFoundError(rep.RangeID, rep.store.StoreID()), destroyReasonRemoved)
	rep.mu.Unlock()
	rep.readOnlyCmdMu.Unlock()

	if opts.DestroyData {
		if err := rep.destroyRaftMuLocked(ctx, nextReplicaID); err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.unlinkReplicaByRangeIDLocked(rep.RangeID)
	if placeholder := s.mu.replicasByKey.Delete(rep); placeholder != rep {
		// We already checked that our replica was present in replicasByKey
		// above. Nothing should have been able to change that.
		log.Fatalf(ctx, "replica %+v unexpectedly overlapped by %+v", rep, placeholder)
	}
	if rep2 := s.getOverlappingKeyRangeLocked(desc); rep2 != nil {
		log.Fatalf(ctx, "corrupted replicasByKey map: %s and %s overlapped", rep, rep2)
	}
	delete(s.mu.replicaPlaceholders, rep.RangeID)
	// TODO(peter): Could release s.mu.Lock() here.
	s.maybeGossipOnCapacityChange(ctx, rangeRemoveEvent)
	s.scanner.RemoveReplica(rep)

	return nil
}

// unlinkReplicaByRangeIDLocked removes all of the store's references to the
// provided replica that are keyed by its range ID. The replica may also need
// to be removed from the replicasByKey map.
//
// store.mu must be held.
func (s *Store) unlinkReplicaByRangeIDLocked(rangeID roachpb.RangeID) {
	s.mu.AssertHeld()
	s.unquiescedReplicas.Lock()
	delete(s.unquiescedReplicas.m, rangeID)
	s.unquiescedReplicas.Unlock()
	delete(s.mu.uninitReplicas, rangeID)
	s.replicaQueues.Delete(int64(rangeID))
	s.mu.replicas.Delete(int64(rangeID))
}

// maybeMarkReplicaInitializedLocked should be called whenever a previously
// unintialized replica has become initialized so that the store can update its
// internal bookkeeping. It requires that Store.mu and Replica.raftMu
// are locked.
func (s *Store) maybeMarkReplicaInitializedLocked(ctx context.Context, repl *Replica) error {
	if !repl.IsInitialized() {
		return errors.Errorf("attempted to process uninitialized range %s", repl)
	}

	rangeID := repl.RangeID

	if _, ok := s.mu.uninitReplicas[rangeID]; !ok {
		// Do nothing if the range has already been initialized.
		return nil
	}
	delete(s.mu.uninitReplicas, rangeID)

	if exRange := s.getOverlappingKeyRangeLocked(repl.Desc()); exRange != nil {
		return errors.Errorf("%s: cannot initialize replica; range %s has overlapping range %s",
			s, repl, exRange.Desc())
	}
	if exRngItem := s.mu.replicasByKey.ReplaceOrInsert(repl); exRngItem != nil {
		return errors.Errorf("range for key %v already exists in replicasByKey btree",
			(exRngItem.(*Replica)).startKey())
	}

	// Add the range to metrics and maybe gossip on capacity change.
	s.metrics.ReplicaCount.Inc(1)
	s.maybeGossipOnCapacityChange(ctx, rangeAddEvent)

	return nil
}

// Attrs returns the attributes of the underlying store.
func (s *Store) Attrs() roachpb.Attributes {
	return s.engine.Attrs()
}

// Capacity returns the capacity of the underlying storage engine. Note that
// this does not include reservations.
// Note that Capacity() has the side effect of updating some of the store's
// internal statistics about its replicas.
func (s *Store) Capacity(useCached bool) (roachpb.StoreCapacity, error) {
	if useCached {
		s.cachedCapacity.Lock()
		capacity := s.cachedCapacity.StoreCapacity
		s.cachedCapacity.Unlock()
		if capacity != (roachpb.StoreCapacity{}) {
			return capacity, nil
		}
	}

	capacity, err := s.engine.Capacity()
	if err != nil {
		return capacity, err
	}

	now := s.cfg.Clock.Now()
	var leaseCount int32
	var rangeCount int32
	var logicalBytes int64
	var totalQueriesPerSecond float64
	var totalWritesPerSecond float64
	replicaCount := s.metrics.ReplicaCount.Value()
	bytesPerReplica := make([]float64, 0, replicaCount)
	writesPerReplica := make([]float64, 0, replicaCount)
	rankingsAccumulator := s.replRankings.newAccumulator()
	newStoreReplicaVisitor(s).Visit(func(r *Replica) bool {
		rangeCount++
		if r.OwnsValidLease(now) {
			leaseCount++
		}
		mvccStats := r.GetMVCCStats()
		logicalBytes += mvccStats.Total()
		bytesPerReplica = append(bytesPerReplica, float64(mvccStats.Total()))
		// TODO(a-robinson): How dangerous is it that these numbers will be
		// incorrectly low the first time or two it gets gossiped when a store
		// starts? We can't easily have a countdown as its value changes like for
		// leases/replicas.
		var qps float64
		if avgQPS, dur := r.leaseholderStats.avgQPS(); dur >= MinStatsDuration {
			qps = avgQPS
			totalQueriesPerSecond += avgQPS
			// TODO(a-robinson): Calculate percentiles for qps? Get rid of other percentiles?
		}
		if wps, dur := r.writeStats.avgQPS(); dur >= MinStatsDuration {
			totalWritesPerSecond += wps
			writesPerReplica = append(writesPerReplica, wps)
		}
		rankingsAccumulator.addReplica(replicaWithStats{
			repl: r,
			qps:  qps,
		})
		return true
	})
	capacity.RangeCount = rangeCount
	capacity.LeaseCount = leaseCount
	capacity.LogicalBytes = logicalBytes
	capacity.QueriesPerSecond = totalQueriesPerSecond
	capacity.WritesPerSecond = totalWritesPerSecond
	capacity.BytesPerReplica = roachpb.PercentilesFromData(bytesPerReplica)
	capacity.WritesPerReplica = roachpb.PercentilesFromData(writesPerReplica)
	s.recordNewPerSecondStats(totalQueriesPerSecond, totalWritesPerSecond)
	s.replRankings.update(rankingsAccumulator)

	s.cachedCapacity.Lock()
	s.cachedCapacity.StoreCapacity = capacity
	s.cachedCapacity.Unlock()

	return capacity, nil
}

// ReplicaCount returns the number of replicas contained by this store. This
// method is O(n) in the number of replicas and should not be called from
// performance critical code.
func (s *Store) ReplicaCount() int {
	var count int
	s.mu.replicas.Range(func(_ int64, _ unsafe.Pointer) bool {
		count++
		return true
	})
	return count
}

// Registry returns the store registry.
func (s *Store) Registry() *metric.Registry {
	return s.metrics.registry
}

// Metrics returns the store's metric struct.
func (s *Store) Metrics() *StoreMetrics {
	return s.metrics
}

// MVCCStats returns the current MVCCStats accumulated for this store.
// TODO(mrtracy): This should be removed as part of #4465, this is only needed
// to support the current NodeStatus structures which will be changing.
func (s *Store) MVCCStats() enginepb.MVCCStats {
	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()
	return s.metrics.mu.stats
}

// Descriptor returns a StoreDescriptor including current store
// capacity information.
func (s *Store) Descriptor(useCached bool) (*roachpb.StoreDescriptor, error) {
	capacity, err := s.Capacity(useCached)
	if err != nil {
		return nil, err
	}

	// Initialize the store descriptor.
	return &roachpb.StoreDescriptor{
		StoreID:  s.Ident.StoreID,
		Attrs:    s.Attrs(),
		Node:     *s.nodeDesc,
		Capacity: capacity,
	}, nil
}

// Send fetches a range based on the header's replica, assembles method, args &
// reply into a Raft Cmd struct and executes the command using the fetched
// range.
//
// An incoming request may be transactional or not. If it is not transactional,
// the timestamp at which it executes may be higher than that optionally
// specified through the incoming BatchRequest, and it is not guaranteed that
// all operations are written at the same timestamp. If it is transactional, a
// timestamp must not be set - it is deduced automatically from the
// transaction. In particular, the read (original) timestamp will be used for
// all reads and the write (provisional commit) timestamp will be used for
// all writes. See the comments on txn.TxnMeta.Timestamp and txn.OrigTimestamp
// for more details.
//
// Should a transactional operation be forced to a higher timestamp (for
// instance due to the timestamp cache or finding a committed value in the path
// of one of its writes), the response will have a transaction set which should
// be used to update the client transaction object.
func (s *Store) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	// Attach any log tags from the store to the context (which normally
	// comes from gRPC).
	ctx = s.AnnotateCtx(ctx)
	for _, union := range ba.Requests {
		arg := union.GetInner()
		header := arg.Header()
		if err := verifyKeys(header.Key, header.EndKey, roachpb.IsRange(arg)); err != nil {
			return nil, roachpb.NewError(err)
		}
	}

	// Limit the number of concurrent AddSSTable requests, since they're expensive
	// and block all other writes to the same span.
	if ba.IsSingleAddSSTableRequest() {
		if err := s.limiters.ConcurrentAddSSTableRequests.Begin(ctx); err != nil {
			return nil, roachpb.NewError(err)
		}
		defer s.limiters.ConcurrentAddSSTableRequests.Finish()

		if err := s.limiters.AddSSTableRequestRate.Wait(ctx); err != nil {
			return nil, roachpb.NewError(err)
		}
		s.engine.PreIngestDelay(ctx)
	}

	if err := ba.SetActiveTimestamp(s.Clock().Now); err != nil {
		return nil, roachpb.NewError(err)
	}

	if s.cfg.TestingKnobs.ClockBeforeSend != nil {
		s.cfg.TestingKnobs.ClockBeforeSend(s.cfg.Clock, ba)
	}

	// Update our clock with the incoming request timestamp. This advances the
	// local node's clock to a high water mark from all nodes with which it has
	// interacted. We hold on to the resulting timestamp - we know that any
	// write with a higher timestamp we run into later must have started after
	// this point in (absolute) time.
	var now hlc.Timestamp
	if s.cfg.TestingKnobs.DisableMaxOffsetCheck {
		now = s.cfg.Clock.Update(ba.Timestamp)
	} else {
		// If the command appears to come from a node with a bad clock,
		// reject it now before we reach that point.
		var err error
		if now, err = s.cfg.Clock.UpdateAndCheckMaxOffset(ba.Timestamp); err != nil {
			return nil, roachpb.NewError(err)
		}
	}

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
				if txn := pErr.GetTxn(); txn == nil {
					pErr.SetTxn(ba.Txn)
				}
			} else {
				if br.Txn == nil {
					br.Txn = ba.Txn
				}
				// Update our clock with the outgoing response txn timestamp
				// (if timestamp has been forwarded).
				if ba.Timestamp.Less(br.Txn.Timestamp) {
					s.cfg.Clock.Update(br.Txn.Timestamp)
				}
			}
		} else {
			if pErr == nil {
				// Update our clock with the outgoing response timestamp.
				// (if timestamp has been forwarded).
				if ba.Timestamp.Less(br.Timestamp) {
					s.cfg.Clock.Update(br.Timestamp)
				}
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
		if _, ok := ba.Txn.GetObservedTimestamp(ba.Replica.NodeID); !ok {
			txnClone := ba.Txn.Clone()
			txnClone.UpdateObservedTimestamp(ba.Replica.NodeID, now)
			ba.Txn = txnClone
		}
	}

	if log.V(1) {
		log.Eventf(ctx, "executing %s", ba)
	} else if log.HasSpanOrEvent(ctx) {
		log.Eventf(ctx, "executing %d requests", len(ba.Requests))
	}

	var cleanupAfterWriteIntentError func(newWIErr *roachpb.WriteIntentError, newIntentTxn *enginepb.TxnMeta)
	defer func() {
		if cleanupAfterWriteIntentError != nil {
			// This request wrote an intent only if there was no error, the request
			// is transactional, the transaction is not yet finalized, and the request
			// wasn't read-only.
			if pErr == nil && ba.Txn != nil && !br.Txn.Status.IsFinalized() && !ba.IsReadOnly() {
				cleanupAfterWriteIntentError(nil, &br.Txn.TxnMeta)
			} else {
				cleanupAfterWriteIntentError(nil, nil)
			}
		}
	}()

	// Add the command to the range for execution; exit retry loop on success.
	for {
		// Exit loop if context has been canceled or timed out.
		if err := ctx.Err(); err != nil {
			return nil, roachpb.NewError(errors.Wrap(err, "aborted during Store.Send"))
		}

		// Get range and add command to the range for execution.
		repl, err := s.GetReplica(ba.RangeID)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		if !repl.IsInitialized() {
			repl.mu.RLock()
			replicaID := repl.mu.replicaID
			repl.mu.RUnlock()

			// If we have an uninitialized copy of the range, then we are
			// probably a valid member of the range, we're just in the
			// process of getting our snapshot. If we returned
			// RangeNotFoundError, the client would invalidate its cache,
			// but we can be smarter: the replica that caused our
			// uninitialized replica to be created is most likely the
			// leader.
			return nil, roachpb.NewError(&roachpb.NotLeaseHolderError{
				RangeID:     ba.RangeID,
				LeaseHolder: repl.creatingReplica,
				// The replica doesn't have a range descriptor yet, so we have to build
				// a ReplicaDescriptor manually.
				Replica: roachpb.ReplicaDescriptor{
					NodeID:    repl.store.nodeDesc.NodeID,
					StoreID:   repl.store.StoreID(),
					ReplicaID: replicaID,
				},
			})
		}

		// If necessary, the request may need to wait in the txn wait queue,
		// pending updates to the target transaction for either PushTxn or
		// QueryTxn requests.
		if br, pErr = s.maybeWaitForPushee(ctx, &ba, repl); br != nil || pErr != nil {
			return br, pErr
		}
		br, pErr = repl.Send(ctx, ba)
		if pErr == nil {
			return br, nil
		}

		// Handle push txn failures and write intent conflicts locally and
		// retry. Other errors are returned to caller.
		switch t := pErr.GetDetail().(type) {
		case *roachpb.TransactionPushError:
			// On a transaction push error, retry immediately if doing so will
			// enqueue into the txnWaitQueue in order to await further updates to
			// the unpushed txn's status. We check ShouldPushImmediately to avoid
			// retrying non-queueable PushTxnRequests (see #18191).
			dontRetry := s.cfg.TestingKnobs.DontRetryPushTxnFailures
			if !dontRetry && ba.IsSinglePushTxnRequest() {
				pushReq := ba.Requests[0].GetInner().(*roachpb.PushTxnRequest)
				dontRetry = txnwait.ShouldPushImmediately(pushReq)
			}
			if dontRetry {
				// If we're not retrying on push txn failures return a txn retry error
				// after the first failure to guarantee a retry.
				if ba.Txn != nil {
					err := roachpb.NewTransactionRetryError(
						roachpb.RETRY_REASON_UNKNOWN, "DontRetryPushTxnFailures testing knob")
					return nil, roachpb.NewErrorWithTxn(err, ba.Txn)
				}
				return nil, pErr
			}

			// Enqueue unsuccessfully pushed transaction on the txnWaitQueue and
			// retry the command.
			repl.txnWaitQueue.Enqueue(&t.PusheeTxn)
			pErr = nil

		case *roachpb.IndeterminateCommitError:
			if s.cfg.TestingKnobs.DontRecoverIndeterminateCommits {
				return nil, pErr
			}
			// On an indeterminate commit error, attempt to recover and finalize
			// the stuck transaction. Retry immediately if successful.
			if _, err := s.recoveryMgr.ResolveIndeterminateCommit(ctx, t); err != nil {
				// Do not propagate ambiguous results; assume success and retry original op.
				if _, ok := err.(*roachpb.AmbiguousResultError); !ok {
					// Preserve the error index.
					index := pErr.Index
					pErr = roachpb.NewError(err)
					pErr.Index = index
					return nil, pErr
				}
			}
			// We've recovered the transaction that blocked the push; retry command.
			pErr = nil

		case *roachpb.WriteIntentError:
			// Process and resolve write intent error. We do this here because
			// this is the code path with the requesting client waiting.
			if pErr.Index != nil {
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
				if h.Txn != nil {
					// We must push at least to h.Timestamp, but in fact we want to
					// go all the way up to a timestamp which was taken off the HLC
					// after our operation started. This allows us to not have to
					// restart for uncertainty as we come back and read.
					obsTS, ok := h.Txn.GetObservedTimestamp(ba.Replica.NodeID)
					if !ok {
						// This was set earlier in this method, so it's
						// completely unexpected to not be found now.
						log.Fatalf(ctx, "missing observed timestamp: %+v", h.Txn)
					}
					h.Timestamp.Forward(obsTS)
					// We are going to hand the header (and thus the transaction proto)
					// to the RPC framework, after which it must not be changed (since
					// that could race). Since the subsequent execution of the original
					// request might mutate the transaction, make a copy here.
					//
					// See #9130.
					h.Txn = h.Txn.Clone()
				}
				// Handle the case where we get more than one write intent error;
				// we need to cleanup the previous attempt to handle it to allow
				// any other pusher queued up behind this RPC to proceed.
				if cleanupAfterWriteIntentError != nil {
					cleanupAfterWriteIntentError(t, nil)
				}
				if cleanupAfterWriteIntentError, pErr =
					s.intentResolver.ProcessWriteIntentError(ctx, pErr, args, h, pushType); pErr != nil {
					// Do not propagate ambiguous results; assume success and retry original op.
					if _, ok := pErr.GetDetail().(*roachpb.AmbiguousResultError); !ok {
						// Preserve the error index.
						pErr.Index = index
						return nil, pErr
					}
					pErr = nil
				}
				// We've resolved the write intent; retry command.
			}

		case *roachpb.MergeInProgressError:
			// A merge was in progress. We need to retry the command after the merge
			// completes, as signaled by the closing of the replica's mergeComplete
			// channel. Note that the merge may have already completed, in which case
			// its mergeComplete channel will be nil.
			mergeCompleteCh := repl.getMergeCompleteCh()
			if mergeCompleteCh != nil {
				select {
				case <-mergeCompleteCh:
					// Merge complete. Retry the command.
				case <-ctx.Done():
					return nil, roachpb.NewError(errors.Wrap(ctx.Err(), "aborted during merge"))
				case <-s.stopper.ShouldQuiesce():
					return nil, roachpb.NewError(&roachpb.NodeUnavailableError{})
				}
			}
			pErr = nil
		}

		if pErr != nil {
			return nil, pErr
		}
	}
}

// RangeFeed registers a rangefeed over the specified span. It sends updates to
// the provided stream and returns with an optional error when the rangefeed is
// complete.
func (s *Store) RangeFeed(
	args *roachpb.RangeFeedRequest, stream roachpb.Internal_RangeFeedServer,
) *roachpb.Error {
	if err := verifyKeys(args.Span.Key, args.Span.EndKey, true); err != nil {
		return roachpb.NewError(err)
	}

	// Get range and add command to the range for execution.
	repl, err := s.GetReplica(args.RangeID)
	if err != nil {
		return roachpb.NewError(err)
	}
	if !repl.IsInitialized() {
		// (*Store).Send has an optimization for uninitialized replicas to send back
		// a NotLeaseHolderError with a hint of where an initialized replica might
		// be found. RangeFeeds can always be served from followers and so don't
		// otherwise return NotLeaseHolderError. For simplicity we also don't return
		// one here.
		return roachpb.NewError(roachpb.NewRangeNotFoundError(args.RangeID, s.StoreID()))
	}
	return repl.RangeFeed(args, stream)
}

// maybeWaitForPushee potentially diverts the incoming request to
// the txnwait.Queue, where it will wait for updates to the target
// transaction.
func (s *Store) maybeWaitForPushee(
	ctx context.Context, ba *roachpb.BatchRequest, repl *Replica,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// If this is a push txn request, check the push queue first, which
	// may cause this request to wait and either return a successful push
	// txn response or else allow this request to proceed.
	if ba.IsSinglePushTxnRequest() {
		pushReq := ba.Requests[0].GetInner().(*roachpb.PushTxnRequest)
		pushResp, pErr := repl.txnWaitQueue.MaybeWaitForPush(repl.AnnotateCtx(ctx), repl, pushReq)
		// Copy the request in anticipation of setting the force arg and
		// updating the Now timestamp (see below).
		pushReqCopy := *pushReq
		if pErr == txnwait.ErrDeadlock {
			// We've experienced a deadlock; set Force=true on push request,
			// and set the push type to ABORT.
			pushReqCopy.Force = true
			pushReqCopy.PushType = roachpb.PUSH_ABORT
		} else if pErr != nil {
			return nil, pErr
		} else if pushResp != nil {
			br := &roachpb.BatchResponse{}
			br.Add(pushResp)
			return br, nil
		}
		// Move the push timestamp forward to the current time, as this
		// request may have been waiting to push the txn. If we don't
		// move the timestamp forward to the current time, we may fail
		// to push a txn which has expired.
		now := s.Clock().Now()
		ba.Timestamp.Forward(now)
		ba.Requests = nil
		ba.Add(&pushReqCopy)
	} else if ba.IsSingleQueryTxnRequest() {
		// For query txn requests, wait in the txn wait queue either for
		// transaction update or for dependent transactions to change.
		queryReq := ba.Requests[0].GetInner().(*roachpb.QueryTxnRequest)
		pErr := repl.txnWaitQueue.MaybeWaitForQuery(repl.AnnotateCtx(ctx), repl, queryReq)
		if pErr != nil {
			return nil, pErr
		}
	}

	return nil, nil
}

// HandleSnapshot reads an incoming streaming snapshot and applies it if
// possible.
func (s *Store) HandleSnapshot(
	header *SnapshotRequest_Header, stream SnapshotResponseStream,
) error {
	s.metrics.raftRcvdMessages[raftpb.MsgSnap].Inc(1)

	if s.IsDraining() {
		return stream.Send(&SnapshotResponse{
			Status:  SnapshotResponse_DECLINED,
			Message: storeDrainingMsg,
		})
	}

	ctx := s.AnnotateCtx(stream.Context())
	return s.receiveSnapshot(ctx, header, stream)
}

func (s *Store) uncoalesceBeats(
	ctx context.Context,
	beats []RaftHeartbeat,
	fromReplica, toReplica roachpb.ReplicaDescriptor,
	msgT raftpb.MessageType,
	respStream RaftMessageResponseStream,
) {
	if len(beats) == 0 {
		return
	}
	if log.V(4) {
		log.Infof(ctx, "uncoalescing %d beats of type %v: %+v", len(beats), msgT, beats)
	}
	beatReqs := make([]RaftMessageRequest, len(beats))
	for i, beat := range beats {
		msg := raftpb.Message{
			Type:   msgT,
			From:   uint64(beat.FromReplicaID),
			To:     uint64(beat.ToReplicaID),
			Term:   beat.Term,
			Commit: beat.Commit,
		}
		beatReqs[i] = RaftMessageRequest{
			RangeID: beat.RangeID,
			FromReplica: roachpb.ReplicaDescriptor{
				NodeID:    fromReplica.NodeID,
				StoreID:   fromReplica.StoreID,
				ReplicaID: beat.FromReplicaID,
			},
			ToReplica: roachpb.ReplicaDescriptor{
				NodeID:    toReplica.NodeID,
				StoreID:   toReplica.StoreID,
				ReplicaID: beat.ToReplicaID,
			},
			Message: msg,
			Quiesce: beat.Quiesce,
		}
		if log.V(4) {
			log.Infof(ctx, "uncoalesced beat: %+v", beatReqs[i])
		}

		if err := s.HandleRaftUncoalescedRequest(ctx, &beatReqs[i], respStream); err != nil {
			log.Errorf(ctx, "could not handle uncoalesced heartbeat %s", err)
		}
	}
}

// HandleRaftRequest dispatches a raft message to the appropriate Replica. It
// requires that s.mu is not held.
func (s *Store) HandleRaftRequest(
	ctx context.Context, req *RaftMessageRequest, respStream RaftMessageResponseStream,
) *roachpb.Error {
	if len(req.Heartbeats)+len(req.HeartbeatResps) > 0 {
		if req.RangeID != 0 {
			log.Fatalf(ctx, "coalesced heartbeats must have rangeID == 0")
		}
		s.uncoalesceBeats(ctx, req.Heartbeats, req.FromReplica, req.ToReplica, raftpb.MsgHeartbeat, respStream)
		s.uncoalesceBeats(ctx, req.HeartbeatResps, req.FromReplica, req.ToReplica, raftpb.MsgHeartbeatResp, respStream)
		return nil
	}
	return s.HandleRaftUncoalescedRequest(ctx, req, respStream)
}

// HandleRaftUncoalescedRequest dispatches a raft message to the appropriate
// Replica. It requires that s.mu is not held.
func (s *Store) HandleRaftUncoalescedRequest(
	ctx context.Context, req *RaftMessageRequest, respStream RaftMessageResponseStream,
) *roachpb.Error {

	if len(req.Heartbeats)+len(req.HeartbeatResps) > 0 {
		log.Fatalf(ctx, "HandleRaftUncoalescedRequest cannot be given coalesced heartbeats or heartbeat responses, received %s", req)
	}
	// HandleRaftRequest is called on locally uncoalesced heartbeats (which are
	// not sent over the network if the environment variable is set) so do not
	// count them.
	s.metrics.raftRcvdMessages[req.Message.Type].Inc(1)

	value, ok := s.replicaQueues.Load(int64(req.RangeID))
	if !ok {
		value, _ = s.replicaQueues.LoadOrStore(int64(req.RangeID), unsafe.Pointer(&raftRequestQueue{}))
	}
	q := (*raftRequestQueue)(value)
	q.Lock()
	if len(q.infos) >= replicaRequestQueueSize {
		q.Unlock()
		// TODO(peter): Return an error indicating the request was dropped. Note
		// that dropping the request is safe. Raft will retry.
		s.metrics.RaftRcvdMsgDropped.Inc(1)
		return nil
	}
	q.infos = append(q.infos, raftRequestInfo{
		req:        req,
		respStream: respStream,
	})
	first := len(q.infos) == 1
	q.Unlock()

	// processRequestQueue will process all infos in the slice each time it
	// runs, so we only need to schedule a Raft request event if we added the
	// first info in the slice. Everyone else can rely on the request that added
	// the first info already having scheduled a Raft request event.
	if first {
		s.scheduler.EnqueueRaftRequest(req.RangeID)
	}
	return nil
}

// withReplicaForRequest calls the supplied function with the (lazily
// initialized) Replica specified in the request. The replica passed to
// the function will have its Replica.raftMu locked.
func (s *Store) withReplicaForRequest(
	ctx context.Context, req *RaftMessageRequest, f func(context.Context, *Replica) *roachpb.Error,
) *roachpb.Error {
	// Lazily create the replica.
	r, _, err := s.getOrCreateReplica(
		ctx,
		req.RangeID,
		req.ToReplica.ReplicaID,
		&req.FromReplica,
	)
	if err != nil {
		return roachpb.NewError(err)
	}
	defer r.raftMu.Unlock()
	ctx = r.AnnotateCtx(ctx)
	r.setLastReplicaDescriptors(req)
	return f(ctx, r)
}

// processRaftRequestWithReplica processes the (non-snapshot) Raft request on
// the specified replica. Notably, it does not handle updates to the Raft Ready
// state; callers will probably want to handle this themselves at some point.
func (s *Store) processRaftRequestWithReplica(
	ctx context.Context, r *Replica, req *RaftMessageRequest,
) *roachpb.Error {
	if verboseRaftLoggingEnabled() {
		log.Infof(ctx, "incoming raft message:\n%s", raftDescribeMessage(req.Message, raftEntryFormatter))
	}

	if req.Message.Type == raftpb.MsgSnap {
		log.Fatalf(ctx, "unexpected snapshot: %+v", req)
	}

	if req.Quiesce {
		if req.Message.Type != raftpb.MsgHeartbeat {
			log.Fatalf(ctx, "unexpected quiesce: %+v", req)
		}
		status := r.RaftStatus()
		if status != nil && status.Term == req.Message.Term && status.Commit == req.Message.Commit {
			if r.quiesce() {
				return nil
			}
		}
		if log.V(4) {
			log.Infof(ctx, "not quiescing: local raft status is %+v, incoming quiesce message is %+v", status, req.Message)
		}
	}

	if req.ToReplica.ReplicaID == 0 {
		log.VEventf(ctx, 1, "refusing incoming Raft message %s from %+v to %+v",
			req.Message.Type, req.FromReplica, req.ToReplica)
		return roachpb.NewErrorf(
			"cannot recreate replica that is not a member of its range (StoreID %s not found in r%d)",
			r.store.StoreID(), req.RangeID,
		)
	}

	drop := maybeDropMsgApp(r.AnnotateCtx(context.Background()), (*replicaMsgAppDropper)(r), &req.Message, req.RangeStartKey)
	if !drop {
		if err := r.stepRaftGroup(req); err != nil {
			return roachpb.NewError(err)
		}
	}
	return nil
}

// processRaftSnapshotRequest processes the incoming snapshot Raft request on
// the request's specified replica. This snapshot can be preemptive or not. If
// not, the function makes sure to handle any updated Raft Ready state. It also
// adds and later removes the (potentially) necessary placeholder to protect
// against concurrent access to the keyspace encompassed by the snapshot but not
// yet guarded by the replica.
func (s *Store) processRaftSnapshotRequest(
	ctx context.Context, snapHeader *SnapshotRequest_Header, inSnap IncomingSnapshot,
) *roachpb.Error {
	if snapHeader.IsPreemptive() {
		return roachpb.NewError(crdberrors.AssertionFailedf(`expected a raft or learner snapshot`))
	}

	return s.withReplicaForRequest(ctx, &snapHeader.RaftMessageRequest, func(
		ctx context.Context, r *Replica,
	) (pErr *roachpb.Error) {
		if snapHeader.RaftMessageRequest.Message.Type != raftpb.MsgSnap {
			log.Fatalf(ctx, "expected snapshot: %+v", snapHeader.RaftMessageRequest)
		}

		// Check to see if a snapshot can be applied. Snapshots can always be applied
		// to initialized replicas. Note that if we add a placeholder we need to
		// already be holding Replica.raftMu in order to prevent concurrent
		// raft-ready processing of uninitialized replicas.
		var addedPlaceholder bool
		var removePlaceholder bool
		if err := func() error {
			s.mu.Lock()
			defer s.mu.Unlock()
			placeholder, err := s.canApplySnapshotLocked(ctx, snapHeader)
			if err != nil {
				// If the storage cannot accept the snapshot, return an
				// error before passing it to RawNode.Step, since our
				// error handling options past that point are limited.
				log.Infof(ctx, "cannot apply snapshot: %s", err)
				return err
			}

			if placeholder != nil {
				// NB: The placeholder added here is either removed below after a
				// preemptive snapshot is applied or after the next call to
				// Replica.handleRaftReady. Note that we can only get here if the
				// replica doesn't exist or is uninitialized.
				if err := s.addPlaceholderLocked(placeholder); err != nil {
					log.Fatalf(ctx, "could not add vetted placeholder %s: %+v", placeholder, err)
				}
				addedPlaceholder = true
			}
			return nil
		}(); err != nil {
			return roachpb.NewError(err)
		}

		if addedPlaceholder {
			// If we added a placeholder remove it before we return unless some other
			// part of the code takes ownership of the removal (indicated by setting
			// removePlaceholder to false).
			removePlaceholder = true
			defer func() {
				if removePlaceholder {
					if s.removePlaceholder(ctx, snapHeader.RaftMessageRequest.RangeID) {
						atomic.AddInt32(&s.counts.removedPlaceholders, 1)
					}
				}
			}()
		}

		if err := r.stepRaftGroup(&snapHeader.RaftMessageRequest); err != nil {
			return roachpb.NewError(err)
		}

		if _, expl, err := r.handleRaftReadyRaftMuLocked(ctx, inSnap); err != nil {
			fatalOnRaftReadyErr(ctx, expl, err)
		}
		removePlaceholder = false
		return nil
	})
}

// HandleRaftResponse implements the RaftMessageHandler interface. Per the
// interface specification, an error is returned if and only if the underlying
// Raft connection should be closed.
// It requires that s.mu is not held.
func (s *Store) HandleRaftResponse(ctx context.Context, resp *RaftMessageResponse) error {
	ctx = s.AnnotateCtx(ctx)
	repl, replErr := s.GetReplica(resp.RangeID)
	if replErr == nil {
		// Best-effort context annotation of replica.
		ctx = repl.AnnotateCtx(ctx)
	}
	switch val := resp.Union.GetValue().(type) {
	case *roachpb.Error:
		switch tErr := val.GetDetail().(type) {
		case *roachpb.ReplicaTooOldError:
			if replErr != nil {
				// RangeNotFoundErrors are expected here; nothing else is.
				if _, ok := replErr.(*roachpb.RangeNotFoundError); !ok {
					log.Error(ctx, replErr)
				}
				return nil
			}

			repl.mu.Lock()
			// If the replica ID in the error does not match then we know
			// that the replica has been removed and re-added quickly. In
			// that case, we don't want to add it to the replicaGCQueue.
			if tErr.ReplicaID != repl.mu.replicaID {
				repl.mu.Unlock()
				log.Infof(ctx, "replica too old response with old replica ID: %s", tErr.ReplicaID)
				return nil
			}
			// If the replica ID in the error does match, we know the replica
			// will be removed and we can cancel any pending commands. This is
			// sometimes necessary to unblock PushTxn operations that are
			// necessary for the replica GC to succeed.
			repl.cancelPendingCommandsLocked()
			// The replica will be garbage collected soon (we are sure
			// since our replicaID is definitely too old), but in the meantime we
			// already want to bounce all traffic from it. Note that the replica
			// could be re-added with a higher replicaID, in which this error is
			// cleared in setReplicaIDRaftMuLockedMuLocked.
			if repl.mu.destroyStatus.IsAlive() {
				storeID := repl.store.StoreID()
				repl.mu.destroyStatus.Set(roachpb.NewRangeNotFoundError(repl.RangeID, storeID), destroyReasonRemovalPending)
			}
			repl.mu.Unlock()

			s.replicaGCQueue.AddAsync(ctx, repl, replicaGCPriorityRemoved)
		case *roachpb.RaftGroupDeletedError:
			if replErr != nil {
				// RangeNotFoundErrors are expected here; nothing else is.
				if _, ok := replErr.(*roachpb.RangeNotFoundError); !ok {
					log.Error(ctx, replErr)
				}
				return nil
			}

			// If the replica is talking to a replica that's been deleted, it must be
			// out of date. While this may just mean it's slightly behind, it can
			// also mean that it is so far behind it no longer knows where any of the
			// other replicas are (#23994). Add it to the replica GC queue to do a
			// proper check.
			s.replicaGCQueue.AddAsync(ctx, repl, replicaGCPriorityDefault)
		case *roachpb.StoreNotFoundError:
			log.Warningf(ctx, "raft error: node %d claims to not contain store %d for replica %s: %s",
				resp.FromReplica.NodeID, resp.FromReplica.StoreID, resp.FromReplica, val)
			return val.GetDetail() // close Raft connection
		default:
			log.Warningf(ctx, "got error from r%d, replica %s: %s",
				resp.RangeID, resp.FromReplica, val)
		}
	default:
		log.Warningf(ctx, "got unknown raft response type %T from replica %s: %s", val, resp.FromReplica, val)
	}
	return nil
}

// enqueueRaftUpdateCheck asynchronously registers the given range ID to be
// checked for raft updates when the processRaft goroutine is idle.
func (s *Store) enqueueRaftUpdateCheck(rangeID roachpb.RangeID) {
	s.scheduler.EnqueueRaftReady(rangeID)
}

func (s *Store) processRequestQueue(ctx context.Context, rangeID roachpb.RangeID) {
	value, ok := s.replicaQueues.Load(int64(rangeID))
	if !ok {
		return
	}
	q := (*raftRequestQueue)(value)
	q.Lock()
	infos := q.infos
	q.infos = nil
	q.Unlock()

	var lastRepl *Replica
	var hadError bool
	for i, info := range infos {
		last := i == len(infos)-1
		pErr := s.withReplicaForRequest(info.respStream.Context(), info.req,
			func(ctx context.Context, r *Replica) *roachpb.Error {
				// Save the last Replica we see, since we don't know in advance which
				// requests will fail during Replica retrieval. We want this later
				// so we can handle the Raft Ready state all at once.
				lastRepl = r
				pErr := s.processRaftRequestWithReplica(ctx, r, info.req)
				if last {
					// If this is the last request, we can handle raft.Ready without
					// giving up the lock. Set lastRepl to nil, so we don't handle it
					// down below as well.
					lastRepl = nil
					if _, expl, err := r.handleRaftReadyRaftMuLocked(ctx, noSnap); err != nil {
						fatalOnRaftReadyErr(ctx, expl, err)
					}
				}
				return pErr
			})
		if pErr != nil {
			hadError = true
			if err := info.respStream.Send(newRaftMessageResponse(info.req, pErr)); err != nil {
				// Seems excessive to log this on every occurrence as the other side
				// might have closed.
				log.VEventf(ctx, 1, "error sending error: %s", err)
			}
		}
	}

	if hadError {
		// If we're unable to process a request, consider dropping the request queue
		// to free up space in the map.
		// This is relevant if requests failed because the target replica could not
		// be created (for example due to the Raft tombstone). The particular code
		// here takes into account that we don't want to drop the queue if there
		// are other messages waiting on it, or if the target replica exists. Raft
		// tolerates the occasional dropped message, but our unit tests are less
		// forgiving.
		//
		// See https://github.com/cockroachdb/cockroach/issues/30951#issuecomment-428010411.
		if _, exists := s.mu.replicas.Load(int64(rangeID)); !exists {
			q.Lock()
			if len(q.infos) == 0 {
				s.replicaQueues.Delete(int64(rangeID))
			}
			q.Unlock()
		}
	}

	// If lastRepl is not nil, that means that some of the requests succeeded during
	// Replica retrieval (withReplicaForRequest) but that the last request did not,
	// otherwise we would have handled this above and set lastRepl to nil.
	if lastRepl != nil {
		// lastRepl will be unlocked when we exit withReplicaForRequest above.
		// It's fine to relock it here (by calling handleRaftReady instead of
		// handleRaftReadyRaftMuLocked) since racing to handle Raft Ready won't
		// have any undesirable results.
		ctx = lastRepl.AnnotateCtx(ctx)
		if _, expl, err := lastRepl.handleRaftReady(ctx, noSnap); err != nil {
			fatalOnRaftReadyErr(ctx, expl, err)
		}
	}
}

func (s *Store) processReady(ctx context.Context, rangeID roachpb.RangeID) {
	value, ok := s.mu.replicas.Load(int64(rangeID))
	if !ok {
		return
	}

	r := (*Replica)(value)
	ctx = r.AnnotateCtx(ctx)
	start := timeutil.Now()
	stats, expl, err := r.handleRaftReady(ctx, noSnap)
	if err != nil {
		log.Fatalf(ctx, "%s: %+v", log.Safe(expl), err) // TODO(bdarnell)
	}
	elapsed := timeutil.Since(start)
	s.metrics.RaftWorkingDurationNanos.Inc(elapsed.Nanoseconds())
	// Warn if Raft processing took too long. We use the same duration as we
	// use for warning about excessive raft mutex lock hold times. Long
	// processing time means we'll have starved local replicas of ticks and
	// remote replicas will likely start campaigning.
	if elapsed >= defaultReplicaRaftMuWarnThreshold {
		log.Warningf(ctx, "handle raft ready: %.1fs [applied=%d, batches=%d, state_assertions=%d]",
			elapsed.Seconds(), stats.entriesProcessed, stats.batchesProcessed, stats.stateAssertions)
	}
	if !r.IsInitialized() {
		// Only an uninitialized replica can have a placeholder since, by
		// definition, an initialized replica will be present in the
		// replicasByKey map. While the replica will usually consume the
		// placeholder itself, that isn't guaranteed and so this invocation
		// here is crucial (i.e. don't remove it).
		//
		// We need to hold raftMu here to prevent removing a placeholder that is
		// actively being used by Store.processRaftRequest.
		r.raftMu.Lock()
		if s.removePlaceholder(ctx, r.RangeID) {
			atomic.AddInt32(&s.counts.droppedPlaceholders, 1)
		}
		r.raftMu.Unlock()
	}
}

func (s *Store) processTick(ctx context.Context, rangeID roachpb.RangeID) bool {
	value, ok := s.mu.replicas.Load(int64(rangeID))
	if !ok {
		return false
	}
	livenessMap, _ := s.livenessMap.Load().(IsLiveMap)

	start := timeutil.Now()
	r := (*Replica)(value)
	exists, err := r.tick(livenessMap)
	if err != nil {
		log.Error(ctx, err)
	}
	s.metrics.RaftTickingDurationNanos.Inc(timeutil.Since(start).Nanoseconds())
	return exists // ready
}

// nodeIsLiveCallback is invoked when a node transitions from non-live
// to live. Iterate through all replicas and find any which belong to
// ranges containing the implicated node. Unquiesce if currently
// quiesced. Note that this mechanism can race with concurrent
// invocations of processTick, which may have a copy of the previous
// livenessMap where the now-live node is down. Those instances should
// be rare, however, and we expect the newly live node to eventually
// unquiesce the range.
func (s *Store) nodeIsLiveCallback(nodeID roachpb.NodeID) {
	// Update the liveness map.
	s.livenessMap.Store(s.cfg.NodeLiveness.GetIsLiveMap())

	s.mu.replicas.Range(func(k int64, v unsafe.Pointer) bool {
		r := (*Replica)(v)
		for _, rep := range r.Desc().Replicas().All() {
			if rep.NodeID == nodeID {
				r.unquiesce()
			}
		}
		return true
	})
}

func (s *Store) processRaft(ctx context.Context) {
	if s.cfg.TestingKnobs.DisableProcessRaft {
		return
	}

	s.scheduler.Start(ctx, s.stopper)
	// Wait for the scheduler worker goroutines to finish.
	s.stopper.RunWorker(ctx, s.scheduler.Wait)

	s.stopper.RunWorker(ctx, s.raftTickLoop)
	s.stopper.RunWorker(ctx, s.coalescedHeartbeatsLoop)
	s.stopper.AddCloser(stop.CloserFn(func() {
		s.cfg.Transport.Stop(s.StoreID())
	}))
}

func (s *Store) raftTickLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.RaftTickInterval)
	defer ticker.Stop()

	var rangeIDs []roachpb.RangeID

	for {
		select {
		case <-ticker.C:
			rangeIDs = rangeIDs[:0]
			// Update the liveness map.
			if s.cfg.NodeLiveness != nil {
				nextMap := s.cfg.NodeLiveness.GetIsLiveMap()
				for nodeID, entry := range nextMap {
					if entry.IsLive {
						// Make sure we ask all live nodes for closed timestamp updates.
						s.cfg.ClosedTimestamp.Clients.EnsureClient(nodeID)
						continue
					}
					// Liveness claims that this node is down, but ConnHealth gets the last say
					// because we'd rather quiesce a range too little than one too often.
					//
					// NB: This has false negatives. If a node doesn't have a conn open to it
					// when ConnHealth is called, then ConnHealth will return
					// rpc.ErrNotHeartbeated regardless of whether the node is up or not. That
					// said, for the nodes that matter, we're likely talking to them via the
					// Raft transport, so ConnHealth should usually indicate a real problem if
					// it gives us an error back. The check can also have false positives if the
					// node goes down after populating the map, but that matters even less.
					entry.IsLive = (s.cfg.NodeDialer.ConnHealth(nodeID) == nil)
					nextMap[nodeID] = entry
				}
				s.livenessMap.Store(nextMap)
			}

			s.unquiescedReplicas.Lock()
			// Why do we bother to ever queue a Replica on the Raft scheduler for
			// tick processing? Couldn't we just call Replica.tick() here? Yes, but
			// then a single bad/slow Replica can disrupt tick processing for every
			// Replica on the store which cascades into Raft elections and more
			// disruption.
			for rangeID := range s.unquiescedReplicas.m {
				rangeIDs = append(rangeIDs, rangeID)
			}
			s.unquiescedReplicas.Unlock()

			s.scheduler.EnqueueRaftTick(rangeIDs...)
			s.metrics.RaftTicks.Inc(1)

		case <-s.stopper.ShouldStop():
			return
		}
	}
}

// Since coalesced heartbeats adds latency to heartbeat messages, it is
// beneficial to have it run on a faster cycle than once per tick, so that
// the delay does not impact latency-sensitive features such as quiescence.
func (s *Store) coalescedHeartbeatsLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.CoalescedHeartbeatsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.sendQueuedHeartbeats(ctx)
		case <-s.stopper.ShouldStop():
			return
		}
	}
}

// sendQueuedHeartbeatsToNode requires that the s.coalescedMu lock is held. It
// returns the number of heartbeats that were sent.
func (s *Store) sendQueuedHeartbeatsToNode(
	ctx context.Context, beats, resps []RaftHeartbeat, to roachpb.StoreIdent,
) int {
	var msgType raftpb.MessageType

	if len(beats) == 0 && len(resps) == 0 {
		return 0
	} else if len(resps) == 0 {
		msgType = raftpb.MsgHeartbeat
	} else if len(beats) == 0 {
		msgType = raftpb.MsgHeartbeatResp
	} else {
		log.Fatal(ctx, "cannot coalesce both heartbeats and responses")
	}

	chReq := &RaftMessageRequest{
		RangeID: 0,
		ToReplica: roachpb.ReplicaDescriptor{
			NodeID:    to.NodeID,
			StoreID:   to.StoreID,
			ReplicaID: 0,
		},
		FromReplica: roachpb.ReplicaDescriptor{
			NodeID:  s.Ident.NodeID,
			StoreID: s.Ident.StoreID,
		},
		Message: raftpb.Message{
			Type: msgType,
		},
		Heartbeats:     beats,
		HeartbeatResps: resps,
	}

	if log.V(4) {
		log.Infof(ctx, "sending raft request (coalesced) %+v", chReq)
	}

	if !s.cfg.Transport.SendAsync(chReq) {
		for _, beat := range beats {
			if value, ok := s.mu.replicas.Load(int64(beat.RangeID)); ok {
				(*Replica)(value).addUnreachableRemoteReplica(beat.ToReplicaID)
			}
		}
		for _, resp := range resps {
			if value, ok := s.mu.replicas.Load(int64(resp.RangeID)); ok {
				(*Replica)(value).addUnreachableRemoteReplica(resp.ToReplicaID)
			}
		}
		return 0
	}
	return len(beats) + len(resps)
}

func (s *Store) sendQueuedHeartbeats(ctx context.Context) {
	s.coalescedMu.Lock()
	heartbeats := s.coalescedMu.heartbeats
	heartbeatResponses := s.coalescedMu.heartbeatResponses
	s.coalescedMu.heartbeats = map[roachpb.StoreIdent][]RaftHeartbeat{}
	s.coalescedMu.heartbeatResponses = map[roachpb.StoreIdent][]RaftHeartbeat{}
	s.coalescedMu.Unlock()

	var beatsSent int

	for to, beats := range heartbeats {
		beatsSent += s.sendQueuedHeartbeatsToNode(ctx, beats, nil, to)
	}
	for to, resps := range heartbeatResponses {
		beatsSent += s.sendQueuedHeartbeatsToNode(ctx, nil, resps, to)
	}
	s.metrics.RaftCoalescedHeartbeatsPending.Update(int64(beatsSent))
}

var errRetry = errors.New("retry: orphaned replica")

// getOrCreateReplica returns a replica for the given RangeID, creating an
// uninitialized replica if necessary. The caller must not hold the store's
// lock. The returned replica has Replica.raftMu locked and it is the caller's
// responsibility to unlock it.
func (s *Store) getOrCreateReplica(
	ctx context.Context,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	creatingReplica *roachpb.ReplicaDescriptor,
) (_ *Replica, created bool, _ error) {
	for {
		r, created, err := s.tryGetOrCreateReplica(
			ctx,
			rangeID,
			replicaID,
			creatingReplica,
		)
		if err == errRetry {
			continue
		}
		if err != nil {
			return nil, false, err
		}
		return r, created, err
	}
}

// tryGetOrCreateReplica performs a single attempt at trying to lookup or
// create a replica. It will fail with errRetry if it finds a Replica that has
// been destroyed (and is no longer in Store.mu.replicas) or if during creation
// another goroutine gets there first. In either case, a subsequent call to
// tryGetOrCreateReplica will likely succeed, hence the loop in
// getOrCreateReplica.
func (s *Store) tryGetOrCreateReplica(
	ctx context.Context,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	creatingReplica *roachpb.ReplicaDescriptor,
) (_ *Replica, created bool, _ error) {
	// The common case: look up an existing (initialized) replica.
	if value, ok := s.mu.replicas.Load(int64(rangeID)); ok {
		repl := (*Replica)(value)
		repl.raftMu.Lock() // not unlocked
		repl.mu.Lock()
		defer repl.mu.Unlock()

		var replTooOldErr error
		if creatingReplica != nil {
			// Drop messages that come from a node that we believe was once a member of
			// the group but has been removed.
			desc := repl.mu.state.Desc
			_, found := desc.GetReplicaDescriptorByID(creatingReplica.ReplicaID)
			// It's not a current member of the group. Is it from the past?
			if !found && creatingReplica.ReplicaID < desc.NextReplicaID {
				replTooOldErr = roachpb.NewReplicaTooOldError(creatingReplica.ReplicaID)
			}
		}

		var err error
		if replTooOldErr != nil {
			err = replTooOldErr
		} else if ds := repl.mu.destroyStatus; ds.reason == destroyReasonRemoved {
			err = errRetry
		} else {
			err = repl.setReplicaIDRaftMuLockedMuLocked(replicaID)
		}
		if err != nil {
			repl.raftMu.Unlock()
			return nil, false, err
		}
		return repl, false, nil
	}

	// No replica currently exists, so we'll try to create one. Before creating
	// the replica, see if there is a tombstone which would indicate that this is
	// a stale message.
	tombstoneKey := keys.RaftTombstoneKey(rangeID)
	var tombstone roachpb.RaftTombstone
	if ok, err := engine.MVCCGetProto(
		ctx, s.Engine(), tombstoneKey, hlc.Timestamp{}, &tombstone, engine.MVCCGetOptions{},
	); err != nil {
		return nil, false, err
	} else if ok {
		if replicaID != 0 && replicaID < tombstone.NextReplicaID {
			return nil, false, &roachpb.RaftGroupDeletedError{}
		}
	}

	// Create a new replica and lock it for raft processing.
	repl := newReplica(rangeID, s)
	repl.creatingReplica = creatingReplica
	repl.raftMu.Lock() // not unlocked

	// Install the replica in the store's replica map. The replica is in an
	// inconsistent state, but nobody will be accessing it while we hold its
	// locks.
	s.mu.Lock()
	// Grab the internal Replica state lock to ensure nobody mucks with our
	// replica even outside of raft processing. Have to do this after grabbing
	// Store.mu to maintain lock ordering invariant.
	repl.mu.Lock()
	repl.mu.minReplicaID = tombstone.NextReplicaID
	// Add the range to range map, but not replicasByKey since the range's start
	// key is unknown. The range will be added to replicasByKey later when a
	// snapshot is applied. After unlocking Store.mu above, another goroutine
	// might have snuck in and created the replica, so we retry on error.
	if err := s.addReplicaToRangeMapLocked(repl); err != nil {
		repl.mu.Unlock()
		s.mu.Unlock()
		repl.raftMu.Unlock()
		return nil, false, errRetry
	}
	s.mu.uninitReplicas[repl.RangeID] = repl
	s.mu.Unlock()

	desc := &roachpb.RangeDescriptor{
		RangeID: rangeID,
		// TODO(bdarnell): other fields are unknown; need to populate them from
		// snapshot.
	}
	if err := repl.initRaftMuLockedReplicaMuLocked(desc, s.Clock(), replicaID); err != nil {
		// Mark the replica as destroyed and remove it from the replicas maps to
		// ensure nobody tries to use it
		repl.mu.destroyStatus.Set(errors.Wrapf(err, "%s: failed to initialize", repl), destroyReasonRemoved)
		repl.mu.Unlock()
		s.mu.Lock()
		s.unlinkReplicaByRangeIDLocked(rangeID)
		s.mu.Unlock()
		repl.raftMu.Unlock()
		return nil, false, err
	}
	repl.mu.Unlock()
	return repl, true, nil
}

func (s *Store) updateCapacityGauges() error {
	desc, err := s.Descriptor(false /* useCached */)
	if err != nil {
		return err
	}
	s.metrics.Capacity.Update(desc.Capacity.Capacity)
	s.metrics.Available.Update(desc.Capacity.Available)
	s.metrics.Used.Update(desc.Capacity.Used)

	return nil
}

// updateReplicationGauges counts a number of simple replication statistics for
// the ranges in this store.
// TODO(bram): #4564 It may be appropriate to compute these statistics while
// scanning ranges. An ideal solution would be to create incremental events
// whenever availability changes.
func (s *Store) updateReplicationGauges(ctx context.Context) error {
	// Load the system config.
	cfg := s.Gossip().GetSystemConfig()
	if cfg == nil {
		return errors.Errorf("%s: system config not yet available", s)
	}

	var (
		raftLeaderCount               int64
		leaseHolderCount              int64
		leaseExpirationCount          int64
		leaseEpochCount               int64
		raftLeaderNotLeaseHolderCount int64
		quiescentCount                int64
		averageQueriesPerSecond       float64
		averageWritesPerSecond        float64

		rangeCount                int64
		unavailableRangeCount     int64
		underreplicatedRangeCount int64
		overreplicatedRangeCount  int64
		behindCount               int64
	)

	timestamp := s.cfg.Clock.Now()
	var livenessMap IsLiveMap
	if s.cfg.NodeLiveness != nil {
		livenessMap = s.cfg.NodeLiveness.GetIsLiveMap()
	}
	clusterNodes := s.ClusterNodeCount()

	var minMaxClosedTS hlc.Timestamp
	newStoreReplicaVisitor(s).Visit(func(rep *Replica) bool {
		metrics := rep.Metrics(ctx, timestamp, livenessMap, clusterNodes)
		if metrics.Leader {
			raftLeaderCount++
			if metrics.LeaseValid && !metrics.Leaseholder {
				raftLeaderNotLeaseHolderCount++
			}
		}
		if metrics.Leaseholder {
			leaseHolderCount++
			switch metrics.LeaseType {
			case roachpb.LeaseNone:
			case roachpb.LeaseExpiration:
				leaseExpirationCount++
			case roachpb.LeaseEpoch:
				leaseEpochCount++
			}
		}
		if metrics.Quiescent {
			quiescentCount++
		}
		if metrics.RangeCounter {
			rangeCount++
			if metrics.Unavailable {
				unavailableRangeCount++
			}
			if metrics.Underreplicated {
				underreplicatedRangeCount++
			}
			if metrics.Overreplicated {
				overreplicatedRangeCount++
			}
		}
		behindCount += metrics.BehindCount
		if qps, dur := rep.leaseholderStats.avgQPS(); dur >= MinStatsDuration {
			averageQueriesPerSecond += qps
		}
		if wps, dur := rep.writeStats.avgQPS(); dur >= MinStatsDuration {
			averageWritesPerSecond += wps
		}
		if mc := rep.maxClosed(ctx); minMaxClosedTS.IsEmpty() || mc.Less(minMaxClosedTS) {
			minMaxClosedTS = mc
		}
		return true // more
	})

	s.metrics.RaftLeaderCount.Update(raftLeaderCount)
	s.metrics.RaftLeaderNotLeaseHolderCount.Update(raftLeaderNotLeaseHolderCount)
	s.metrics.LeaseHolderCount.Update(leaseHolderCount)
	s.metrics.LeaseExpirationCount.Update(leaseExpirationCount)
	s.metrics.LeaseEpochCount.Update(leaseEpochCount)
	s.metrics.QuiescentCount.Update(quiescentCount)
	s.metrics.AverageQueriesPerSecond.Update(averageQueriesPerSecond)
	s.metrics.AverageWritesPerSecond.Update(averageWritesPerSecond)
	s.recordNewPerSecondStats(averageQueriesPerSecond, averageWritesPerSecond)

	s.metrics.RangeCount.Update(rangeCount)
	s.metrics.UnavailableRangeCount.Update(unavailableRangeCount)
	s.metrics.UnderReplicatedRangeCount.Update(underreplicatedRangeCount)
	s.metrics.OverReplicatedRangeCount.Update(overreplicatedRangeCount)
	s.metrics.RaftLogFollowerBehindCount.Update(behindCount)

	if !minMaxClosedTS.IsEmpty() {
		nanos := timeutil.Since(minMaxClosedTS.GoTime()).Nanoseconds()
		s.metrics.ClosedTimestampMaxBehindNanos.Update(nanos)
	}

	return nil
}

// ComputeMetrics immediately computes the current value of store metrics which
// cannot be computed incrementally. This method should be invoked periodically
// by a higher-level system which records store metrics.
func (s *Store) ComputeMetrics(ctx context.Context, tick int) error {
	ctx = s.AnnotateCtx(ctx)
	if err := s.updateCapacityGauges(); err != nil {
		return err
	}
	if err := s.updateReplicationGauges(ctx); err != nil {
		return err
	}

	// Get the latest RocksDB stats.
	stats, err := s.engine.GetStats()
	if err != nil {
		return err
	}
	s.metrics.updateRocksDBStats(*stats)

	// Get engine Env stats.
	envStats, err := s.engine.GetEnvStats()
	if err != nil {
		return err
	}
	s.metrics.updateEnvStats(*envStats)

	// If we're using RocksDB, log the sstable overview.
	if rocksdb, ok := s.engine.(*engine.RocksDB); ok {
		sstables := rocksdb.GetSSTables()
		s.metrics.RdbNumSSTables.Update(int64(sstables.Len()))
		readAmp := sstables.ReadAmplification()
		s.metrics.RdbReadAmplification.Update(int64(readAmp))
		// Log this metric infrequently.
		if tick%logSSTInfoTicks == 0 /* every 10m */ {
			log.Infof(ctx, "sstables (read amplification = %d):\n%s", readAmp, sstables)
			log.Infof(ctx, "%sestimated_pending_compaction_bytes: %s",
				rocksdb.GetCompactionStats(), humanizeutil.IBytes(stats.PendingCompactionBytesEstimate))
		}
	}
	return nil
}

// ClusterNodeCount returns this store's view of the number of nodes in the
// cluster. This is the metric used for adapative zone configs; ranges will not
// be reported as underreplicated if it is low. Tests that wait for full
// replication by tracking the underreplicated metric must also check for the
// expected ClusterNodeCount to avoid catching the cluster while the first node
// is initialized but the other nodes are not.
func (s *Store) ClusterNodeCount() int {
	return s.cfg.StorePool.ClusterNodeCount()
}

// HotReplicaInfo contains a range descriptor and its QPS.
type HotReplicaInfo struct {
	Desc *roachpb.RangeDescriptor
	QPS  float64
}

// HottestReplicas returns the hottest replicas on a store, sorted by their
// QPS. Only contains ranges for which this store is the leaseholder.
//
// Note that this uses cached information, so it's cheap but may be slightly
// out of date.
func (s *Store) HottestReplicas() []HotReplicaInfo {
	topQPS := s.replRankings.topQPS()
	hotRepls := make([]HotReplicaInfo, len(topQPS))
	for i := range topQPS {
		hotRepls[i].Desc = topQPS[i].repl.Desc()
		hotRepls[i].QPS = topQPS[i].qps
	}
	return hotRepls
}

// StoreKeySpanStats carries the result of a stats computation over a key range.
type StoreKeySpanStats struct {
	ReplicaCount         int
	MVCC                 enginepb.MVCCStats
	ApproximateDiskBytes uint64
}

// ComputeStatsForKeySpan computes the aggregated MVCCStats for all replicas on
// this store which contain any keys in the supplied range.
func (s *Store) ComputeStatsForKeySpan(startKey, endKey roachpb.RKey) (StoreKeySpanStats, error) {
	var result StoreKeySpanStats

	newStoreReplicaVisitor(s).Visit(func(repl *Replica) bool {
		desc := repl.Desc()
		if bytes.Compare(startKey, desc.EndKey) >= 0 || bytes.Compare(desc.StartKey, endKey) >= 0 {
			return true // continue
		}
		result.MVCC.Add(repl.GetMVCCStats())
		result.ReplicaCount++
		return true
	})

	var err error
	result.ApproximateDiskBytes, err = s.engine.ApproximateDiskBytes(startKey.AsRawKey(), endKey.AsRawKey())
	return result, err
}

// AllocatorDryRun runs the given replica through the allocator without actually
// carrying out any changes, returning all trace messages collected along the way.
// Intended to help power a debug endpoint.
func (s *Store) AllocatorDryRun(
	ctx context.Context, repl *Replica,
) ([]tracing.RecordedSpan, error) {
	ctx, collect, cancel := tracing.ContextWithRecordingSpan(ctx, "allocator dry run")
	defer cancel()
	canTransferLease := func() bool { return true }
	_, err := s.replicateQueue.processOneChange(
		ctx, repl, canTransferLease, true /* dryRun */)
	if err != nil {
		log.Eventf(ctx, "error simulating allocator on replica %s: %s", repl, err)
	}
	return collect(), nil
}

// ManuallyEnqueue runs the given replica through the requested queue,
// returning all trace events collected along the way as well as the error
// message returned from the queue's process method, if any.  Intended to help
// power an admin debug endpoint.
func (s *Store) ManuallyEnqueue(
	ctx context.Context, queueName string, repl *Replica, skipShouldQueue bool,
) ([]tracing.RecordedSpan, string, error) {
	ctx = repl.AnnotateCtx(ctx)

	var queue queueImpl
	var needsLease bool
	for _, replicaQueue := range s.scanner.queues {
		if strings.EqualFold(replicaQueue.Name(), queueName) {
			queue = replicaQueue.(queueImpl)
			needsLease = replicaQueue.NeedsLease()
		}
	}
	if queue == nil {
		return nil, "", errors.Errorf("unknown queue type %q", queueName)
	}

	sysCfg := s.cfg.Gossip.GetSystemConfig()
	if sysCfg == nil {
		return nil, "", errors.New("cannot run queue without a valid system config; make sure the cluster " +
			"has been initialized and all nodes connected to it")
	}

	// Many queues are only meant to be run on leaseholder replicas, so attempt to
	// take the lease here or bail out early if a different replica has it.
	if needsLease {
		hasLease, pErr := repl.getLeaseForGossip(ctx)
		if pErr != nil {
			return nil, "", pErr.GoError()
		}
		if !hasLease {
			return nil, fmt.Sprintf("replica %v does not have the range lease", repl), nil
		}
	}

	ctx, collect, cancel := tracing.ContextWithRecordingSpan(
		ctx, fmt.Sprintf("manual %s queue run", queueName))
	defer cancel()

	if !skipShouldQueue {
		log.Eventf(ctx, "running %s.shouldQueue", queueName)
		shouldQueue, priority := queue.shouldQueue(ctx, s.cfg.Clock.Now(), repl, sysCfg)
		log.Eventf(ctx, "shouldQueue=%v, priority=%f", shouldQueue, priority)
		if !shouldQueue {
			return collect(), "", nil
		}
	}

	log.Eventf(ctx, "running %s.process", queueName)
	err := queue.process(ctx, repl, sysCfg)
	if err != nil {
		return collect(), err.Error(), nil
	}
	return collect(), "", nil
}

// GetClusterVersion reads the the cluster version from the store-local version
// key. Returns an empty version if the key is not found.
func (s *Store) GetClusterVersion(ctx context.Context) (cluster.ClusterVersion, error) {
	return ReadClusterVersion(ctx, s.engine)
}

// WriteClusterVersion writes the given cluster version to the store-local cluster version key.
func WriteClusterVersion(
	ctx context.Context, writer engine.ReadWriter, cv cluster.ClusterVersion,
) error {
	return engine.MVCCPutProto(ctx, writer, nil, keys.StoreClusterVersionKey(), hlc.Timestamp{}, nil, &cv)
}

// ReadClusterVersion reads the the cluster version from the store-local version key.
func ReadClusterVersion(ctx context.Context, reader engine.Reader) (cluster.ClusterVersion, error) {
	var cv cluster.ClusterVersion
	_, err := engine.MVCCGetProto(ctx, reader, keys.StoreClusterVersionKey(), hlc.Timestamp{},
		&cv, engine.MVCCGetOptions{})
	return cv, err
}

// GetTxnWaitKnobs is part of txnwait.StoreInterface.
func (s *Store) GetTxnWaitKnobs() txnwait.TestingKnobs {
	return s.TestingKnobs().TxnWaitKnobs
}

// GetTxnWaitMetrics is called by txnwait.Queue instances to get a reference to
// the shared metrics instance.
func (s *Store) GetTxnWaitMetrics() *txnwait.Metrics {
	return s.txnWaitMetrics
}

func init() {
	tracing.RegisterTagRemapping("s", "store")
}
