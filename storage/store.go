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
	"github.com/cockroachdb/cockroach/util/bufalloc"
	"github.com/cockroachdb/cockroach/util/envutil"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"github.com/cockroachdb/cockroach/util/tracing"
	"github.com/cockroachdb/cockroach/util/uuid"
)

const (
	// rangeIDAllocCount is the number of Range IDs to allocate per allocation.
	rangeIDAllocCount               = 10
	defaultHeartbeatIntervalTicks   = 5
	defaultRaftElectionTimeoutTicks = 15
	defaultAsyncSnapshotMaxAge      = time.Minute
	// ttlStoreGossip is time-to-live for store-related info.
	ttlStoreGossip = 2 * time.Minute

	// preemptiveSnapshotRaftGroupID is a bogus ID for which a Raft group is
	// temporarily created during the application of a preemptive snapshot.
	preemptiveSnapshotRaftGroupID = math.MaxUint64

	// defaultRaftEntryCacheSize is the default size in bytes for a
	// store's Raft log entry cache.
	defaultRaftEntryCacheSize = 1 << 24 // 16M

	// rangeLeaseRaftElectionTimeoutMultiplier specifies what multiple the leader
	// lease active duration should be of the raft election timeout.
	rangeLeaseRaftElectionTimeoutMultiplier = 3

	// rangeLeaseRenewalDivisor specifies what quotient the range lease renewal
	// duration should be of the range lease active time.
	rangeLeaseRenewalDivisor = 5

	// replicaRequestQueueSize specifies the maximum number of requests to queue
	// for a replica.
	replicaRequestQueueSize = 100
)

var changeTypeInternalToRaft = map[roachpb.ReplicaChangeType]raftpb.ConfChangeType{
	roachpb.ADD_REPLICA:    raftpb.ConfChangeAddNode,
	roachpb.REMOVE_REPLICA: raftpb.ConfChangeRemoveNode,
}

var storeSchedulerConcurrency = envutil.EnvOrDefaultInt(
	"COCKROACH_SCHEDULER_CONCURRENCY", 2*runtime.NumCPU())

// TestStoreContext has some fields initialized with values relevant in tests.
func TestStoreContext() StoreContext {
	return StoreContext{
		Ctx:                            tracing.WithTracer(context.Background(), tracing.NewTracer()),
		RaftTickInterval:               100 * time.Millisecond,
		RaftHeartbeatIntervalTicks:     1,
		RaftElectionTimeoutTicks:       3,
		ScanInterval:                   10 * time.Minute,
		ConsistencyCheckInterval:       10 * time.Minute,
		ConsistencyCheckPanicOnFailure: true,
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
	endKey() roachpb.RKey
}

// rangeBTreeKey is a type alias of roachpb.RKey that implements the
// rangeKeyItem interface and the btree.Item interface.
type rangeBTreeKey roachpb.RKey

var _ rangeKeyItem = rangeBTreeKey{}

func (k rangeBTreeKey) endKey() roachpb.RKey {
	return (roachpb.RKey)(k)
}

var _ btree.Item = rangeBTreeKey{}

func (k rangeBTreeKey) Less(i btree.Item) bool {
	return k.endKey().Less(i.(rangeKeyItem).endKey())
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
	store   *Store
	refs    []ReplicaRef // (references of) Replicas to be visited.
	visited int          // Number of visited ranges. -1 when Visit() is not being called.
}

func newStoreRangeSet(store *Store) *storeRangeSet {
	return &storeRangeSet{
		store:   store,
		visited: 0,
	}
}

// Visit calls the visitor with each Replica until false is returned.
func (rs *storeRangeSet) Visit(visitor func(ref ReplicaRef) bool) {
	// Copy the  range IDs to a slice and iterate over the slice so
	// that we can safely (e.g., no race, no range skip) iterate
	// over ranges regardless of how BTree is implemented.
	rs.store.mu.Lock()
	rs.refs = make([]ReplicaRef, 0, rs.store.mu.replicasByKey.Len())
	rs.store.visitReplicasLocked(
		roachpb.RKeyMin, roachpb.RKeyMax, func(ref ReplicaRef) bool {
			rs.refs = append(rs.refs, ref)
			return true // more
		},
	)
	rs.store.mu.Unlock()

	rs.visited = 0
	for _, ref := range rs.refs {
		rs.visited++
		if !visitor(ref) {
			break
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
	return len(rs.refs) - rs.visited
}

type raftRequestInfo struct {
	req        *RaftMessageRequest
	respStream RaftMessageResponseStream
}

type raftRequestQueue []raftRequestInfo

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
	replicateQueue          *replicateQueue          // Replication queue
	replicaGCQueue          *replicaGCQueue          // Replica GC queue
	raftLogQueue            *raftLogQueue            // Raft Log Truncation queue
	scanner                 *replicaScanner          // Replica scanner
	replicaConsistencyQueue *replicaConsistencyQueue // Replica consistency check queue
	consistencyScanner      *replicaScanner          // Consistency checker scanner
	metrics                 *StoreMetrics
	intentResolver          *intentResolver
	raftEntryCache          *raftEntryCache
	// 1 if the store was started, 0 if it wasn't. To be accessed using atomic
	// ops.
	started int32
	stopper *stop.Stopper
	// The time when the store was Start()ed, in nanos.
	startedAt    int64
	nodeDesc     *roachpb.NodeDescriptor
	initComplete sync.WaitGroup // Signaled by async init tasks
	bookie       *bookie

	idleReplicaElectionTime struct {
		syncutil.Mutex
		at time.Time
	}

	// This is 1 if there is an active raft snapshot. This field must be checked
	// and set atomically.
	// TODO(marc): This may be better inside of `mu`, but is not currently feasible.
	hasActiveRaftSnapshot int32

	// drainLeases holds a bool which indicates whether Replicas should be
	// allowed to acquire or extend range leases; see DrainLeases().
	//
	// TODO(bdarnell,tschottdorf): Would look better inside of `mu`, which at
	// the time of its creation was riddled with deadlock (but that situation
	// has likely improved).
	drainLeases atomic.Value

	// Locking notes: To avoid deadlocks, the following lock order must be
	// obeyed:
	//
	//   Replica.refMu < Replica.raftMu < Store.uninitRaftMu
	//   < Replica.readOnlyCmdMu < Store.mu.Mutex < Replica.mu.Mutex
	//   < Store.scheduler.mu.
	//
	// It is not required to acquire every lock in sequence, but when multiple
	// locks are held at the same time, it is incorrect to acquire a lock with
	// "lesser" value in this sequence after one with "greater" value)
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
	// * Replica.refMu: Held in read-only mode while holding on to a *Replica.
	//   The fundamental raison d'être of this (enriched) RWMutex is to allow the
	//   ReplicaGCQueue to safely destroy a Replica by preventing later access to
	//   it. The lock is the gatekeeper behind ReplicaRef; since it must be
	//   held (in read mode) to even obtain a *Replica, it must order before
	//   raftMu. This means that while raftMu is held, the corresponding
	//   ReplicaRef must not be handed to any methods synchronously.
	//   Held in exclusive mode while the Replica is destroyed.
	//
	// * Replica.raftMu/Store.uninitRaftMu: Held while any raft messages are
	//   being processed (including handleRaftReady and HandleRaftRequest) or
	//   while the set of Replicas in the Store is being changed (which may
	//   happen outside of raft via the replica GC queue).
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
	// * Store.mu: Protects the Store's map of its Replicas. Acquired and
	//   released briefly at the start of each request; metadata operations like
	//   splits acquire it again to update the map. Even though these lock
	//   acquisitions do not make up a single critical section, it is safe thanks
	//   to Replica.raftMu/Store.uninitRaftMu which prevents any concurrent
	//   modifications.
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
	// repeatedly rather than holding a lock for an entire request, we are
	// actually relying on higher-level locks to ensure that things don't change
	// out from under us. In particular, handleRaftReady accesses the replicaID
	// more than once, and we rely on Replica.raftMu/Store.uninitRaftMu to ensure
	// that this is not modified by a concurrent HandleRaftRequest. (#4476)

	// uninitRaftMu protects Raft processing for uninitialized replicas. We need
	// to serialize processing of uninitialized replicas because we do not know
	// what range of keys such replicas cover. Uninitialized replicas might
	// conflict with either initialized replicas (which can create new replicas
	// via splits) or other uninitialized replicas (by applying snapshots).
	//
	// Locking notes: Replica.raftMu < Store.uninitRaftMu < Replica.mu
	uninitRaftMu syncutil.Mutex

	mu struct {
		syncutil.Mutex // Protects all variables in the mu struct.
		// Map of replicas by Range ID. This includes `uninitReplicas`.
		replicas map[roachpb.RangeID]*Replica
		// A btree key containing objects of type *Replica or
		// *ReplicaPlaceholder (both of which have an associated key range, on
		// the EndKey of which the btree is keyed)
		replicasByKey  *btree.BTree
		uninitReplicas map[roachpb.RangeID]*Replica // Map of uninitialized replicas by Range ID
		// replicaPlaceholders is a map to access all placeholders, so they can
		// be directly accessed and cleared after stepping all raft groups.
		replicaPlaceholders map[roachpb.RangeID]*ReplicaPlaceholder
		// replicaQueues is a map of per-Replica incoming request queues. These
		// queues might more naturally belong in Replica, but are kept separate to
		// avoid reworking the locking in getOrCreateReplica which requires
		// Replica.raftMu to be held while a replica is being inserted into
		// Store.mu.replicas.
		replicaQueues map[roachpb.RangeID]raftRequestQueue
	}

	scheduler *raftScheduler

	counts struct {
		// Number of placeholders removed due to error.
		removedPlaceholders int32
		// Number of placeholders successfully filled by a snapshot.
		filledPlaceholders int32
		// Number of placeholders removed due to a snapshot that was dropped by
		// raft.
		droppedPlaceholders int32
	}
}

var _ client.Sender = &Store{}

// A StoreContext encompasses the auxiliary objects and configuration
// required to create a store.
// All fields holding a pointer or an interface are required to create
// a store; the rest will have sane defaults set if omitted.
// TODO(radu): rename this to StoreConfig
type StoreContext struct {
	// Base context to be used for logs and traces inside the node or store; must
	// have a Tracer set.
	Ctx context.Context

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

	// If LogRangeEvents is true, major changes to ranges will be logged into
	// the range event log.
	LogRangeEvents bool

	// AsyncSnapshotMaxAge is the maximum amount of time that an
	// asynchronous snapshot will be held while waiting for raft to pick
	// it up (counted from when the snapshot generation is completed).
	AsyncSnapshotMaxAge time.Duration

	// RaftEntryCacheSize is the size in bytes of the Raft log entry cache
	// shared by all Raft groups managed by the store.
	RaftEntryCacheSize uint64

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
	// If non-nil, BadChecksumPanic is called by CheckConsistency() instead of
	// panicking on a checksum mismatch.
	BadChecksumPanic func(roachpb.StoreIdent)
	// If non-nil, BadChecksumReportDiff is called by CheckConsistency() on a
	// checksum mismatch to report the diff between snapshots.
	BadChecksumReportDiff func(roachpb.StoreIdent, []ReplicaSnapshotDiff)
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
	// DisableReplicaGCQueue disables the replication queue.
	DisableReplicaGCQueue bool
	// DisableReplicateQueue disables the replication queue.
	DisableReplicateQueue bool
	// DisableSplitQueue disables the split queue.
	DisableSplitQueue bool
	// DisableScanner disables the replica scanner.
	DisableScanner bool
	// DisableRefreshReasonTicks disables refreshing pending commands when a new
	// leader is discovered.
	DisableRefreshReasonNewLeader bool
	// DisableRefreshReasonTicks disables refreshing pending commands when a
	// snapshot is applied.
	DisableRefreshReasonSnapshotApplied bool
	// DisableRefreshReasonTicks disables refreshing pending commands
	// periodically.
	DisableRefreshReasonTicks bool
	// DisableProcessRaft disables the process raft loop.
	DisableProcessRaft bool
}

var _ base.ModuleTestingKnobs = &StoreTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*StoreTestingKnobs) ModuleTestingKnobs() {}

// Valid returns true if the StoreContext is populated correctly.
// We don't check for Gossip and DB since some of our tests pass
// that as nil.
func (sc *StoreContext) Valid() bool {
	return sc.Clock != nil && sc.Transport != nil &&
		sc.RaftTickInterval != 0 && sc.RaftHeartbeatIntervalTicks > 0 &&
		sc.RaftElectionTimeoutTicks > 0 && sc.ScanInterval >= 0 &&
		sc.ConsistencyCheckInterval >= 0 &&
		sc.Ctx != nil && sc.Ctx.Done() == nil && tracing.TracerFromCtx(sc.Ctx) != nil
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
	if sc.RaftEntryCacheSize == 0 {
		sc.RaftEntryCacheSize = defaultRaftEntryCacheSize
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
		ctx:       ctx,
		db:        ctx.DB, // TODO(tschottdorf) remove redundancy.
		engine:    eng,
		allocator: MakeAllocator(ctx.StorePool, ctx.AllocatorOptions),
		nodeDesc:  nodeDesc,
		metrics:   newStoreMetrics(),
	}
	s.intentResolver = newIntentResolver(s)
	s.raftEntryCache = newRaftEntryCache(ctx.RaftEntryCacheSize)
	s.drainLeases.Store(false)
	s.scheduler = newRaftScheduler(ctx.Ctx, s, storeSchedulerConcurrency)

	s.mu.Lock()
	s.mu.replicas = map[roachpb.RangeID]*Replica{}
	s.mu.replicaPlaceholders = map[roachpb.RangeID]*ReplicaPlaceholder{}
	s.mu.replicaQueues = map[roachpb.RangeID]raftRequestQueue{}
	s.mu.replicasByKey = btree.New(64 /* degree */)
	s.mu.uninitReplicas = map[roachpb.RangeID]*Replica{}
	s.mu.Unlock()

	if s.ctx.Gossip != nil {
		// Add range scanner and configure with queues.
		s.scanner = newReplicaScanner(ctx.ScanInterval, ctx.ScanMaxIdleTime, newStoreRangeSet(s))
		s.gcQueue = newGCQueue(s, s.ctx.Gossip)
		s.splitQueue = newSplitQueue(s, s.db, s.ctx.Gossip)
		s.replicateQueue = newReplicateQueue(s, s.ctx.Gossip, s.allocator, s.ctx.Clock, s.ctx.AllocatorOptions)
		s.replicaGCQueue = newReplicaGCQueue(s, s.db, s.ctx.Gossip)
		s.raftLogQueue = newRaftLogQueue(s, s.db, s.ctx.Gossip)
		s.scanner.AddQueues(s.gcQueue, s.splitQueue, s.replicateQueue, s.replicaGCQueue, s.raftLogQueue)

		// Add consistency check scanner.
		s.consistencyScanner = newReplicaScanner(ctx.ConsistencyCheckInterval, 0, newStoreRangeSet(s))
		s.replicaConsistencyQueue = newReplicaConsistencyQueue(s, s.ctx.Gossip)
		s.consistencyScanner.AddQueues(s.replicaConsistencyQueue)
	}

	if ctx.TestingKnobs.DisableReplicaGCQueue {
		s.setReplicaGCQueueActive(false)
	}
	if ctx.TestingKnobs.DisableReplicateQueue {
		s.setReplicateQueueActive(false)
	}
	if ctx.TestingKnobs.DisableSplitQueue {
		s.setSplitQueueActive(false)
	}
	if ctx.TestingKnobs.DisableScanner {
		s.setScannerActive(false)
	}

	return s
}

// String formats a store for debug output.
func (s *Store) String() string {
	return fmt.Sprintf("[n%d,s%d]", s.Ident.NodeID, s.Ident.StoreID)
}

// Ctx returns the base context for the store.
func (s *Store) Ctx() context.Context {
	return s.ctx.Ctx
}

// logContext adds the node and store log tags to a context. Used to
// personalize an operation context with this Store's identity.
func (s *Store) logContext(ctx context.Context) context.Context {
	// Copy the log tags from the base context. This allows us to opaquely set the
	// log tags that were passed by the upper layers.
	return log.WithLogTagsFromCtx(ctx, s.Ctx())
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
		var retryErr error
		now := s.Clock().Now()
		newStoreRangeSet(s).Visit(func(ref ReplicaRef) bool {
			r, release, err := ref.Acquire()
			defer release()
			if err != nil {
				return true // continue and do not set retryErr
			}

			lease, nextLease := r.getLease()
			// If we own an active lease or we're trying to obtain a lease
			// (and that request is fresh enough), wait.
			if (lease.OwnedBy(s.StoreID()) && lease.Covers(now)) ||
				(nextLease != nil && nextLease.Covers(now)) {

				retryErr = fmt.Errorf("replica %s still has an active lease", r)
				return true // continue
			}
			return retryErr == nil // break on error
		})
		return retryErr
	})
}

// IsStarted returns true if the Store has been started.
func (s *Store) IsStarted() bool {
	return atomic.LoadInt32(&s.started) == 1
}

// IterateRangeDescriptors calls the provided function with each descriptor
// from the provided Engine. The return values of this method and fn have
// semantics similar to engine.MVCCIterate.
func IterateRangeDescriptors(ctx context.Context,
	eng engine.Reader, fn func(desc roachpb.RangeDescriptor) (bool, error),
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

	_, err := engine.MVCCIterate(context.Background(), eng, start, end, hlc.MaxTimestamp, false /* !consistent */, nil, /* txn */
		false /* !reverse */, kvToDesc)
	log.Eventf(ctx, "iterated over %d keys to find %d range descriptors (by suffix: %v)",
		allCount, matchCount, bySuffix)
	return err
}

func (s *Store) migrate(ctx context.Context, desc roachpb.RangeDescriptor) {
	batch := s.engine.NewBatch()
	defer batch.Close()
	if err := migrate7310And6991(ctx, batch, desc); err != nil {
		log.Fatal(ctx, errors.Wrap(err, "during migration"))
	}
	if err := batch.Commit(); err != nil {
		log.Fatal(ctx, errors.Wrap(err, "could not migrate Raft state"))
	}
}

// Start the engine, set the GC and read the StoreIdent.
func (s *Store) Start(ctx context.Context, stopper *stop.Stopper) error {
	s.stopper = stopper

	// Add the bookie to the store.
	s.bookie = newBookie(
		s.ctx.Clock,
		s.stopper,
		s.metrics,
		envutil.EnvOrDefaultDuration("COCKROACH_RESERVATION_TIMEOUT", ttlStoreGossip),
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
	log.Event(ctx, "read store identity")

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

	// Set the store ID for logging.
	s.ctx.Ctx = log.WithLogTagInt(s.ctx.Ctx, "s", int(s.StoreID()))

	// Iterate over all range descriptors, ignoring uncommitted versions
	// (consistent=false). Uncommitted intents which have been abandoned
	// due to a split crashing halfway will simply be resolved on the
	// next split attempt. They can otherwise be ignored.
	s.mu.Lock()

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
			s.migrate(ctx, desc)

			// Create the Replica and insert it into the Store. Note that we
			// don't hold the reference outside of this small block; if we need
			// it later, we'll have to get a ReplicaRef from the Store.
			{
				rep, err := NewReplica(&desc, s, 0)
				if err != nil {
					return false, err
				}
				if err = s.addReplicaInternalLocked(rep); err != nil {
					return false, err
				}
				// Add this range and its stats to our counter.
				s.metrics.ReplicaCount.Inc(1)
				s.metrics.addMVCCStats(rep.GetMVCCStats())
			}

			if _, ok := desc.GetReplicaDescriptor(s.StoreID()); !ok {
				ref, err := s.getReplicaLocked(desc.RangeID)
				if err != nil {
					log.Fatalf(
						ctx, "cannot retrieve inserted Replica %s: %s", desc, err,
					)
				}

				// We are no longer a member of the range, but we didn't GC the
				// replica before shutting down. Add the replica to the GC
				// queue.
				if added, err := s.replicaGCQueue.Add(
					ref, replicaGCPriorityRemoved,
				); err != nil {
					log.Errorf(ctx, "%s: unable to add replica to GC queue: %s", ref, err)
				} else if added {
					log.Infof(ctx, "%s: added to replica GC queue", ref)
				}
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
	s.mu.Unlock()
	if err != nil {
		return err
	}

	// Start Raft processing goroutines.
	s.ctx.Transport.Listen(s.StoreID(), s)
	s.processRaft()

	doneUnfreezing := make(chan struct{})
	if s.stopper.RunAsyncTask(s.Ctx(), func(ctx context.Context) {
		defer close(doneUnfreezing)
		sem := make(chan struct{}, 512)
		var wg sync.WaitGroup // wait for unfreeze goroutines
		var unfrozen int64    // updated atomically
		newStoreRangeSet(s).Visit(func(ref ReplicaRef) bool {
			r, release, err := ref.Acquire()
			defer release()
			if err != nil {
				return true // continue
			}

			r.mu.Lock()
			frozen := r.mu.state.Frozen
			r.mu.Unlock()
			if !frozen {
				return true
			}
			wg.Add(1)
			if s.stopper.RunLimitedAsyncTask(ctx, sem, func(ctx context.Context) {
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
					log.Errorf(ctx, "%s: could not unfreeze Range %s on startup: %s", s, r, pErr)
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
			log.Infof(ctx, "reactivated %d frozen Ranges", unfrozen)
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
		log.Event(ctx, "finished unfreezing")
	case <-time.After(10 * time.Second):
		log.Event(ctx, "gave up waiting for unfreezing; continuing in background")
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

		// Run metrics computation up front to populate initial statistics.
		if err = s.ComputeMetrics(-1); err != nil {
			log.Infof(ctx, "%s: failed initial metrics computation: %s", s, err)
		}
		log.Event(ctx, "computed initial metrics")
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
	// Periodic updates run in a goroutine and signal a WaitGroup upon completion
	// of their first iteration.
	s.initComplete.Add(2)
	s.stopper.RunWorker(func() {
		// Run the first time without waiting for the Ticker and signal the WaitGroup.
		if err := s.maybeGossipFirstRange(); err != nil {
			log.Warningf(s.Ctx(), "error gossiping first range data: %s", err)
		}
		s.initComplete.Done()
		ticker := time.NewTicker(sentinelGossipInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := s.maybeGossipFirstRange(); err != nil {
					log.Warningf(s.Ctx(), "error gossiping first range data: %s", err)
				}
			case <-s.stopper.ShouldStop():
				return
			}
		}
	})

	s.stopper.RunWorker(func() {
		if err := s.maybeGossipSystemConfig(); err != nil {
			log.Warningf(s.Ctx(), "error gossiping system config: %s", err)
		}
		s.initComplete.Done()
		ticker := time.NewTicker(configGossipInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := s.maybeGossipSystemConfig(); err != nil {
					log.Warningf(s.Ctx(), "error gossiping system config: %s", err)
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
	ref := s.LookupReplica(roachpb.RKeyMin, nil)
	if ref == nil {
		return nil
	}
	rng, release, err := ref.Acquire()
	defer release()
	if err != nil {
		return err
	}
	for loop := retry.Start(retryOptions); loop.Next(); {
		pErr := rng.maybeGossipFirstRange()
		if nlErr, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); !ok || nlErr.LeaseHolder != nil {
			return pErr.GoError()
		}
	}
	return nil
}

// maybeGossipSystemConfig looks for the range containing SystemConfig keys and
// lets that range gossip them.
func (s *Store) maybeGossipSystemConfig() error {
	ref := s.LookupReplica(roachpb.RKey(keys.SystemConfigSpan.Key), nil)
	if ref == nil {
		// This store has no range with this configuration.
		return nil
	}
	rng, release, err := ref.Acquire()
	defer release()
	if err != nil {
		return err
	}

	// Wake up the replica. If it acquires a fresh lease, it will
	// gossip. If an unexpected error occurs (i.e. nobody else seems to
	// have an active lease but we still failed to obtain it), return
	// that error.
	_, pErr := rng.getLeaseForGossip(s.Ctx())
	return pErr.GoError()
}

// systemGossipUpdate is a callback for gossip updates to
// the system config which affect range split boundaries.
func (s *Store) systemGossipUpdate(cfg config.SystemConfig) {
	// For every range, update its MaxBytes and check if it needs to be split.
	newStoreRangeSet(s).Visit(func(ref ReplicaRef) bool {
		if zone, err := cfg.GetZoneConfigForKey(ref.Desc().StartKey); err == nil {
			// It's important that we don't hold a reference to the Replica
			// over the call to MaybeAdd below.
			func() {
				rng, release, err := ref.Acquire()
				defer release()
				if err == nil {
					rng.SetMaxBytes(zone.RangeMaxBytes)
				}
			}()
		}
		s.splitQueue.MaybeAdd(ref, s.ctx.Clock.Now())
		return true // more
	})
}

// GossipStore broadcasts the store on the gossip network.
func (s *Store) GossipStore(ctx context.Context) error {
	// This should always return immediately and acts as a sanity check that we
	// don't try to gossip before we're connected.
	select {
	case <-s.ctx.Gossip.Connected:
	default:
		panic(fmt.Sprintf("%s: not connected to gossip", s))
	}

	storeDesc, err := s.Descriptor()
	if err != nil {
		return errors.Wrapf(err, "problem getting store descriptor for store %+v", s.Ident)
	}
	// Unique gossip key per store.
	gossipStoreKey := gossip.MakeStoreKey(storeDesc.StoreID)
	// Gossip store descriptor.
	if err := s.ctx.Gossip.AddInfoProto(gossipStoreKey, storeDesc, ttlStoreGossip); err != nil {
		return err
	}
	// Once we have gossiped the store descriptor the first time, other nodes
	// will know that this node has restarted and will start sending Raft
	// heartbeats for active ranges. We compute the time in the future where a
	// replica on this store which receives a command for an idle range can
	// campaign the associated Raft group immediately instead of waiting for the
	// normal Raft election timeout.
	//
	// Note that computing this timestamp here is conservative. We really care
	// that the node descriptor has been gossipped as that is how remote nodes
	// locate this one to send Raft messages. The initialization sequence is:
	//   1. gossip node descriptor
	//   2. wait for gossip to be connected
	//   3. gossip store descriptors (where we're at here)
	s.idleReplicaElectionTime.Lock()
	if s.idleReplicaElectionTime.at == (time.Time{}) {
		// Raft uses a randomized election timeout in the range
		// [electionTimeout,2*electionTimeout]. Using the lower bound here means
		// that elections are somewhat more likely to be contested (assuming
		// traffic is distributed evenly across a cluster that is restarted
		// simultaneously). That's OK; it just adds a network round trip or two to
		// the process since a contested election just restarts the clock to where
		// it would have been anyway if we weren't doing idle replica campaigning.
		electionTimeout := s.ctx.RaftTickInterval * time.Duration(s.ctx.RaftElectionTimeoutTicks)
		s.idleReplicaElectionTime.at = s.Clock().PhysicalTime().Add(electionTimeout)
	}
	s.idleReplicaElectionTime.Unlock()
	return nil
}

func (s *Store) canCampaignIdleReplica() bool {
	s.idleReplicaElectionTime.Lock()
	defer s.idleReplicaElectionTime.Unlock()
	if s.idleReplicaElectionTime.at == (time.Time{}) {
		return false
	}
	return !s.Clock().PhysicalTime().Before(s.idleReplicaElectionTime.at)
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
func (s *Store) GetReplica(rangeID roachpb.RangeID) (ReplicaRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getReplicaLocked(rangeID)
}

// getReplicaLocked fetches a replica by RangeID. The store's lock must be held.
func (s *Store) getReplicaLocked(rangeID roachpb.RangeID) (ReplicaRef, error) {
	if rng, ok := s.mu.replicas[rangeID]; ok {
		return rng.AsRef(), nil
	}
	return nil, roachpb.NewRangeNotFoundError(rangeID)
}

// LookupReplica looks up a replica via binary search over the
// "replicasByKey" btree. Returns nil if no replica is found for
// specified key range. Note that the specified keys are transformed
// using Key.Address() to ensure we lookup replicas correctly for local
// keys. When end is nil, a replica that contains start is looked up.
func (s *Store) LookupReplica(start, end roachpb.RKey) ReplicaRef {
	s.mu.Lock()
	defer s.mu.Unlock()

	var ref ReplicaRef
	s.visitReplicasLocked(start, roachpb.RKeyMax, func(r ReplicaRef) bool {
		ref = r
		return false
	})
	if ref == nil || !ref.Desc().ContainsKeyRange(start, end) {
		return nil
	}
	return ref
}

// getOverlappingKeyRangeLocked returns a KeyRange from the Store overlapping the given
// descriptor (or nil if no such KeyRange exists).
func (s *Store) getOverlappingKeyRangeLocked(rngDesc *roachpb.RangeDescriptor) KeyRange {
	var kr KeyRange

	s.mu.replicasByKey.AscendGreaterOrEqual(rangeBTreeKey(rngDesc.StartKey.Next()),
		func(item btree.Item) bool {
			kr = item.(KeyRange)
			return false
		})

	if kr != nil && kr.Desc().StartKey.Less(rngDesc.EndKey) {
		return kr
	}

	return nil
}

// visitReplicasLocked will call iterator for every replica on the store which
// contains any keys in the span between startKey and endKey. Iteration will be
// in ascending order. Iteration can be stopped early by returning false from
// iterator.
func (s *Store) visitReplicasLocked(startKey, endKey roachpb.RKey, iterator func(r ReplicaRef) bool) {
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
			kr := item.(KeyRange)
			if !kr.Desc().StartKey.Less(endKey) {
				// This properly checks if this range contains any keys in the supplied span.
				return false
			}

			switch rep := item.(type) {
			case *Replica:
				return iterator(rep.AsRef())
			default:
				return true
			}
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
	defer batch.Close()
	ms := &enginepb.MVCCStats{}
	now := s.ctx.Clock.Now()
	ctx := context.Background()

	// Range descriptor.
	if err := engine.MVCCPutProto(ctx, batch, ms, keys.RangeDescriptorKey(desc.StartKey), now, nil, desc); err != nil {
		return err
	}
	// Replica GC timestamp.
	if err := engine.MVCCPutProto(ctx, batch, nil /* ms */, keys.RangeLastReplicaGCTimestampKey(desc.RangeID), hlc.ZeroTimestamp, nil, &now); err != nil {
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

	updatedMS, err := writeInitialState(ctx, batch, *ms, *desc, raftpb.HardState{})
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
func (s *Store) Tracer() opentracing.Tracer { return tracing.TracerFromCtx(s.Ctx()) }

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
// the actual split with the Store. Requires that Replica.raftMu is held.
//
// TODO(tschottdorf): Want to merge this with SplitRange, but some legacy
// testing code calls SplitRange directly.
func splitTriggerPostCommit(
	ctx context.Context,
	deltaMS enginepb.MVCCStats,
	split *roachpb.SplitTrigger,
	r *Replica,
) {
	// There might be an uninitialized replica for the right hand side of the
	// split. If there is, we want to use that *Replica because another goroutine
	// might already have a reference to it but is waiting for Store.uninitRaftMu
	// (which this goroutine holds).
	r.store.mu.Lock()
	rightRng, ok := r.store.mu.uninitReplicas[split.RightDesc.RangeID]
	r.store.mu.Unlock()

	// Create (or re-initialize) the new Replica representing the right side of
	// the split. Our error handling options at this point are very limited, but
	// we need to do this after our batch has committed.
	var err error
	if ok {
		// This is safe because any other goroutine which has a pointer to rightRng
		// cannot be performing raft processing on it since this goroutine holds
		// Store.uninitRaftMu.
		//
		// TODO(peter): We can't lock rightRng.raftMu here because another
		// goroutine may already hold that lock. But that goroutine will be waiting
		// on Store.uninitRaftMu which we hold, so we're safe to proceed. This
		// subtlety will go away when Store.uninitRaftMu goes away.
		err = rightRng.init(&split.RightDesc, r.store.Clock(), 0)
	} else {
		rightRng, err = NewReplica(&split.RightDesc, r.store, 0)
	}
	if err != nil {
		panic(err)
	}

	// We do not grab rightRng.raftMu here because some other goroutine might
	// already be holding it (if there was an existing uninitialized
	// replica). This is safe because that goroutine will be waiting for
	// Store.uninitRaftMu which we hold. Another goroutine cannot discover an
	// initialized version of rightRng until we install it with Store.SplitRange
	// which marks it initialized while holding Store.mu.

	// Copy the timestamp cache into the RHS range.
	r.mu.Lock()
	rightRng.mu.Lock()
	r.mu.tsCache.MergeInto(rightRng.mu.tsCache, true /* clear */)
	rightRng.mu.Unlock()
	r.mu.Unlock()
	log.Event(ctx, "copied timestamp cache")

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
	r.store.replicateQueue.MaybeAdd(r.AsRef(), now)
	r.store.replicateQueue.MaybeAdd(rightRng.AsRef(), now)

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
		if err := r.store.stopper.RunAsyncTask(ctx, func(ctx context.Context) {
			time.Sleep(10 * time.Millisecond)
			// Make sure that rightRng hasn't been removed.
			ref, err := r.store.GetReplica(rightRng.RangeID)
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

			replica, release, err := ref.Acquire()
			defer release()
			if err != nil {
				panic(err)
			}

			if err := replica.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
				if err := raftGroup.Campaign(); err != nil {
					log.Warningf(ctx, "%s: error %v", r, err)
				}
				return true, nil
			}); err != nil {
				panic(err)
			}
		}); err != nil {
			log.Warningf(ctx, "%s: error %v", r, err)
			return
		}
	} else if len(split.RightDesc.Replicas) == 1 {
		// TODO(peter): In single-node clusters, we enqueue the right-hand side of
		// the split (the new range) for Raft processing so that the corresponding
		// Raft group is created. This shouldn't be necessary for correctness, but
		// some tests rely on this (e.g. server.TestStatusSummaries).
		r.store.enqueueRaftUpdateCheck(rightRng.RangeID)
	}
}

// SplitRange shortens the original range to accommodate the new range. The new
// range is added to the ranges map and the replicasByKey
// btree. origRng.raftMu, newRng.raftMu and Store.uninitRaftMu must be held.
//
// This is only called from the split trigger in the context of the execution
// of a Raft command.
func (s *Store) SplitRange(origRng, newRng *Replica) error {
	origDesc := origRng.Desc()
	newDesc := newRng.Desc()

	if !bytes.Equal(origDesc.EndKey, newDesc.EndKey) ||
		bytes.Compare(origDesc.StartKey, newDesc.StartKey) >= 0 {
		return errors.Errorf("orig range is not splittable by new range: %+v, %+v", origDesc, newDesc)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if exRng, ok := s.mu.uninitReplicas[newDesc.RangeID]; ok {
		// If we have an uninitialized replica of the new range we require pointer
		// equivalence with newRng. See Store.splitTriggerPostCommit()
		if exRng != newRng {
			log.Fatalf(s.Ctx(), "found unexpected uninitialized replica: %s vs %s", exRng, newRng)
		}
		delete(s.mu.uninitReplicas, newDesc.RangeID)
		delete(s.mu.replicas, newDesc.RangeID)
		delete(s.mu.replicaQueues, newDesc.RangeID)
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

	s.metrics.ReplicaCount.Inc(1)
	return s.processRangeDescriptorUpdateLocked(origRng)
}

// MergeRange expands the subsuming range to absorb the subsumed range. This
// merge operation will fail if the two ranges are not collocated on the same
// store.
// The subsumed range's raftMu is assumed held. The caller must not hold a
// reference to the subsumed range (as it will be removed, which requires
// exclusive access).
func (s *Store) MergeRange(subsumingRng *Replica, updatedEndKey roachpb.RKey, subsumedRangeID roachpb.RangeID) error {
	subsumingDesc := subsumingRng.Desc()

	if !subsumingDesc.EndKey.Less(updatedEndKey) {
		return errors.Errorf("the new end key is not greater than the current one: %+v <= %+v",
			updatedEndKey, subsumingDesc.EndKey)
	}

	subsumedRef, err := s.GetReplica(subsumedRangeID)
	if err != nil {
		return errors.Errorf("could not find the subsumed range: %d", subsumedRangeID)
	}
	subsumedDesc := subsumedRef.Desc()

	if !replicaSetsEqual(subsumedDesc.Replicas, subsumingDesc.Replicas) {
		return errors.Errorf("ranges are not on the same replicas sets: %+v != %+v",
			subsumedDesc.Replicas, subsumingDesc.Replicas)
	}

	// Remove and destroy the subsumed range. Note that we were called
	// (indirectly) from raft processing so we pass the corresponding
	// parameter to RemoveReplica to avoid deadlocking on Replica.raftMu.
	if err := s.RemoveReplica(
		subsumedRef, *subsumedRef.Desc(), false /* !delete */, false, /* !raftMuHeld */
	); err != nil {
		return errors.Errorf("cannot remove range %s", err)
	}

	// Update the end key of the subsuming range.
	copy := *subsumingDesc
	copy.EndKey = updatedEndKey
	return subsumingRng.setDesc(&copy)
}

// AddReplicaTest adds the replica to the store's replica map and to the sorted
// replicasByKey slice. To be used only by unittests.
func (s *Store) AddReplicaTest(ref ReplicaRef) error {
	rng, release, err := ref.Acquire()
	defer release()
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.addReplicaInternalLocked(rng); err != nil {
		return err
	}
	s.metrics.ReplicaCount.Inc(1)
	return nil
}

// addReplicaInternalLocked adds the replica to the replicas map and the
// replicasByKey btree. Returns an error if a replica with
// the same Range ID or a KeyRange that overlaps has already been added to
// this store. addReplicaInternalLocked requires that the store lock is held.
func (s *Store) addReplicaInternalLocked(rng *Replica) error {
	if !rng.IsInitialized() {
		return errors.Errorf("attempted to add uninitialized range %s", rng)
	}

	// TODO(spencer); will need to determine which range is
	// newer, and keep that one.
	if err := s.addReplicaToRangeMapLocked(rng); err != nil {
		return err
	}

	if exRange := s.getOverlappingKeyRangeLocked(rng.Desc()); exRange != nil {
		return errors.Errorf("%s: cannot addReplicaInternalLocked; range %s has overlapping range %s", s, rng, exRange.Desc())
	}

	if exRngItem := s.mu.replicasByKey.ReplaceOrInsert(rng); exRngItem != nil {
		return errors.Errorf("%s: cannot addReplicaInternalLocked; range for key %v already exists in replicasByKey btree", s,
			exRngItem.(KeyRange).endKey())
	}

	return nil
}

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
// otherwise.
func (s *Store) removePlaceholder(rngID roachpb.RangeID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.removePlaceholderLocked(rngID)
}

func (s *Store) removePlaceholderLocked(rngID roachpb.RangeID) bool {
	rng, ok := s.mu.replicaPlaceholders[rngID]
	if !ok {
		return false
	}
	switch exRng := s.mu.replicasByKey.Delete(rng).(type) {
	case *ReplicaPlaceholder:
		delete(s.mu.replicaPlaceholders, rngID)
		return true
	case nil:
		log.Fatalf(context.TODO(), "%s range=%d: placeholder not found", s, rngID)
	default:
		log.Fatalf(context.TODO(), "%s range=%d: expected placeholder, got %T", s, rngID, exRng)
	}
	return false // appease the compiler
}

// addReplicaToRangeMapLocked adds the replica to the replicas map.
// addReplicaToRangeMapLocked requires that the store lock is held.
func (s *Store) addReplicaToRangeMapLocked(rng *Replica) error {
	if _, ok := s.mu.replicas[rng.RangeID]; ok {
		return errors.Errorf("%s: replica already exists", rng)
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
//
// TODO(tschottdorf): massage the awkward raftMuHeld parameter.
func (s *Store) RemoveReplica(
	ref ReplicaRef,
	origDesc roachpb.RangeDescriptor,
	destroy bool,
	raftMuHeld bool,
) error {
	// We want an exclusive reference to the Replica underlying ReplicaRef, but
	// the Replica may be in use (and in particular, may be in use by clients
	// stuck waiting on Raft). To circumvent deadlock, we first abort existing
	// proposals and disallow new ones (which means that all future references
	// to the Replica are going to be short-lived), and then acquire the
	// exclusive reference.
	//
	// TODO(tschottdorf): consider shelling this out as ReplicaRef.Drain() if
	// more call sites crop up.
	{
		rep := ref.(replicaRef).replica
		rep.mu.Lock()
		// Clear the pending command queue.
		for _, p := range rep.mu.pendingCmds {
			p.done <- roachpb.ResponseWithError{
				Reply: &roachpb.BatchResponse{},
				Err:   roachpb.NewError(roachpb.NewRangeNotFoundError(rep.RangeID)),
			}
		}
		// Clear the map.
		rep.mu.pendingCmds = map[storagebase.CmdIDKey]*pendingCmd{}
		rep.mu.internalRaftGroup = nil
		rep.mu.destroyed = roachpb.NewRangeNotFoundError(rep.RangeID)
		rep.mu.Unlock()
	}

	rep, release, err := ref.AcquireExclusive()
	defer release()
	if err != nil {
		return err
	}

	if !raftMuHeld {
		defer rep.raftUnlock(rep.raftLock())
	}

	if err := s.removeReplicaImpl(rep, origDesc, destroy); err != nil {
		return err // TODO(tschottdorf): this is Replica corruption or worse.
	}
	rep.mu.Lock()
	defer rep.mu.Unlock()
	// Let future calls to `Acquire{,Exclusive}` fail.
	rep.refMu.destroyed = rep.mu.destroyed
	return nil
}

// removeReplicaImpl is the implementation of RemoveReplica, which is sometimes
// called directly when the necessary lock is already held. It requires that
// Replica.raftMu is held and that s.mu is not held.
func (s *Store) removeReplicaImpl(rep *Replica, origDesc roachpb.RangeDescriptor, destroyData bool) error {
	desc := rep.Desc()
	if repDesc, ok := desc.GetReplicaDescriptor(s.StoreID()); ok && repDesc.ReplicaID >= origDesc.NextReplicaID {
		return errors.Errorf("cannot remove replica %s; replica ID has changed (%s >= %s)",
			rep, repDesc.ReplicaID, origDesc.NextReplicaID)
	}

	s.mu.Lock()
	if _, ok := s.mu.replicas[rep.RangeID]; !ok {
		s.mu.Unlock()
		return errors.New("replica not found")
	}
	if s.getOverlappingKeyRangeLocked(desc) != rep {
		// This is a fatal error because returning at this point will
		// leave the Store in an inconsistent state (we've already deleted
		// from s.mu.replicas), and uninitialized replicas shouldn't make
		// it this far anyway. This method will need some changes when we
		// introduce GC of uninitialized replicas.
		//
		// TODO(peter): The above comment is out of date: we could return an error
		// here but are not doing so yet in the name of conservatism.
		s.mu.Unlock()
		log.Fatalf(context.TODO(), "replica %s found by id but not by key", rep)
	}
	// Adjust stats before calling Destroy. This can be called before or after
	// Destroy, but this configuration helps avoid races in stat verification
	// tests.
	s.metrics.subtractMVCCStats(rep.GetMVCCStats())
	s.metrics.ReplicaCount.Dec(1)
	s.mu.Unlock()

	// Mark the replica as destroyed and (optionally) destroy the on-disk data
	// while not holding Store.mu. This is safe because we're holding
	// Replica.raftMu and the replica is present in Store.mu.replicasByKey
	// (preventing any concurrent access to the replica's key range).
	if err := rep.Destroy(origDesc, destroyData); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.mu.replicas, rep.RangeID)
	delete(s.mu.replicaPlaceholders, rep.RangeID)
	delete(s.mu.replicaQueues, rep.RangeID)
	delete(s.mu.uninitReplicas, rep.RangeID)
	if s.mu.replicasByKey.Delete(rep) == nil {
		// We already checked that our replica was present in replicasByKey
		// above. Nothing should have been able to change that.
		log.Fatalf(context.TODO(), "replica %s found by id but not by key", rep)
	}
	// Remove the replica from the queues asynchronously to avoid re-entrant
	// read lock.
	if err := s.stopper.RunAsyncTask(context.TODO(), func(ctx context.Context) {
		s.scanner.RemoveReplica(rep.AsRef())
		s.consistencyScanner.RemoveReplica(rep.AsRef())
	}); err != nil {
		log.Infof(context.TODO(), "unable to communicate removal to scanner: %s", err)
	}
	return nil
}

// processRangeDescriptorUpdate should be called whenever a replica's range
// descriptor is updated, to update the store's maps of its ranges to match
// the updated descriptor. Since the latter update requires acquiring the store
// lock (which cannot always safely be done by replicas), this function call
// should be deferred until it is safe to acquire the store lock.
func (s *Store) processRangeDescriptorUpdate(rng *Replica) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.processRangeDescriptorUpdateLocked(rng)
}

// processRangeDescriptorUpdateLocked requires that Store.mu and Replica.raftMu
// are locked.
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

	if exRange := s.getOverlappingKeyRangeLocked(rng.Desc()); exRange != nil {
		return errors.Errorf("%s: cannot processRangeDescriptorUpdate; range %s has overlapping range %s", s, rng, exRange.Desc())
	}
	if exRngItem := s.mu.replicasByKey.ReplaceOrInsert(rng); exRngItem != nil {
		return errors.Errorf("range for key %v already exists in replicasByKey btree",
			(exRngItem.(*Replica)).endKey())
	}

	// Add the range and its current stats into metrics.
	s.metrics.ReplicaCount.Inc(1)

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
// to support the current StatusSummary structures which will be changing.
func (s *Store) MVCCStats() enginepb.MVCCStats {
	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()
	return s.metrics.mu.stats
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
	// Attach any log tags from the store to the context (which normally
	// comes from gRPC).
	ctx = s.logContext(ctx)
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
		log.Eventf(ctx, "executing %s", ba)
	} else {
		log.Eventf(ctx, "executing %d requests", len(ba.Requests))
	}
	// Backoff and retry loop for handling errors. Backoff times are measured
	// in the Trace.
	// Increase the sequence counter to avoid getting caught in replay
	// protection on retry.
	next := func(r *retry.Retry) bool {
		if r.CurrentAttempt() > 0 {
			ba.SetNewRequest()
			log.Event(ctx, "backoff")
		}
		return r.Next()
	}

	// Add the command to the range for execution; exit retry loop on success.
	s.mu.Lock()
	retryOpts := s.ctx.RangeRetryOptions
	s.mu.Unlock()
	for r := retry.Start(retryOpts); next(&r); {
		// Get range and add command to the range for execution.
		ref, err := s.GetReplica(ba.RangeID)
		if err != nil {
			pErr = roachpb.NewError(err)
			return nil, pErr
		}
		br, pErr = func() (*roachpb.BatchResponse, *roachpb.Error) {
			rng, release, err := ref.Acquire()
			defer release()
			if err != nil {
				return nil, roachpb.NewError(roachpb.NewRangeNotFoundError(ref.Desc().RangeID))
			}

			if !ref.Desc().IsInitialized() {
				rng.mu.Lock()
				replicaID := rng.mu.replicaID
				rng.mu.Unlock()

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
					// The replica doesn't have a range descriptor yet, so we have to build
					// a ReplicaDescriptor manually.
					Replica: roachpb.ReplicaDescriptor{
						NodeID:    rng.store.nodeDesc.NodeID,
						StoreID:   rng.store.StoreID(),
						ReplicaID: replicaID,
					},
				})
			}
			return rng.Send(ctx, ba)
		}()
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
			// We are going to hand the header (and thus the transaction proto)
			// to the RPC framework, after which it must not be changed (since
			// that could race). Since the subsequent execution of the original
			// request might mutate the transaction, make a copy here.
			//
			// See #9130.
			if h.Txn != nil {
				clonedTxn := h.Txn.Clone()
				h.Txn = &clonedTxn
			}
			pErr = s.intentResolver.processWriteIntentError(ctx, pErr, args, h, pushType)
			// Preserve the error index.
			pErr.Index = index
		}

		log.Eventf(ctx, "error: %T", pErr.GetDetail())
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
	log.Event(ctx, "store retry limit exceeded") // good to check for if tests fail
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
	b := &client.Batch{}
	b.AddRawRequest(&roachpb.PushTxnRequest{
		Span: roachpb.Span{
			Key: txn.Key,
		},
		Now:       now,
		PusheeTxn: txn.TxnMeta,
		PushType:  roachpb.PUSH_QUERY,
	})
	if err := s.db.Run(context.TODO(), b); err != nil {
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

// HandleSnapshot reads an incoming streaming snapshot and applies it if
// possible.
func (s *Store) HandleSnapshot(
	header *SnapshotRequest_Header,
	stream MultiRaft_RaftSnapshotServer,
) error {
	s.metrics.raftRcvdMessages[raftpb.MsgSnap].Inc(1)

	sendSnapError := func(err error) error {
		return stream.Send(&SnapshotResponse{
			Status:  SnapshotResponse_ERROR,
			Message: err.Error(),
		})
	}

	ctx := s.logContext(stream.Context())

	// Check to see if the snapshot can be applied but don't attempt to add
	// a placeholder here, because we're not holding the replica's raftMu.
	// We'll perform this check again later after receiving the rest of the
	// snapshot data - this is purely an optimization to prevent downloading
	// a snapshot that we know we won't be able to apply.
	_, err := s.canApplySnapshot(&header.RangeDescriptor)
	if err != nil {
		return sendSnapError(
			errors.Wrapf(err, "%s,r%d: cannot apply snapshot", s, header.RangeDescriptor.RangeID),
		)
	}

	if err := stream.Send(&SnapshotResponse{Status: SnapshotResponse_ACCEPTED}); err != nil {
		return err
	}

	var batches [][]byte
	var logEntries [][]byte
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		if req.Header != nil {
			return sendSnapError(errors.New("client error: provided a header mid-stream"))
		}

		if req.KVBatch != nil {
			batches = append(batches, req.KVBatch)
		}
		if req.LogEntries != nil {
			logEntries = append(logEntries, req.LogEntries...)
		}
		if req.Final {
			snapUUID, err := uuid.FromBytes(header.RaftMessageRequest.Message.Snapshot.Data)
			if err != nil {
				return sendSnapError(errors.Wrap(err, "invalid snapshot"))
			}

			inSnap := IncomingSnapshot{
				SnapUUID:        *snapUUID,
				RangeDescriptor: header.RangeDescriptor,
				Batches:         batches,
				LogEntries:      logEntries,
			}

			if err := s.processRaftRequest(ctx, &header.RaftMessageRequest, inSnap); err != nil {
				return sendSnapError(errors.Wrap(err.GoError(), "failed to apply snapshot"))
			}
			return stream.Send(&SnapshotResponse{Status: SnapshotResponse_APPLIED})
		}
	}
}

// HandleRaftRequest dispatches a raft message to the appropriate Replica. It
// requires that s.mu is not held.
func (s *Store) HandleRaftRequest(
	ctx context.Context, req *RaftMessageRequest, respStream RaftMessageResponseStream) *roachpb.Error {
	s.metrics.raftRcvdMessages[req.Message.Type].Inc(1)

	if respStream == nil {
		return s.processRaftRequest(ctx, req, IncomingSnapshot{})
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.mu.replicaQueues[req.RangeID]
	if len(q) >= replicaRequestQueueSize {
		// TODO(peter): Return an error indicating the request was dropped. Note
		// that dropping the request is safe. Raft will retry.
		s.metrics.RaftRcvdMsgDropped.Inc(1)
		return nil
	}
	s.mu.replicaQueues[req.RangeID] = append(q, raftRequestInfo{
		req:        req,
		respStream: respStream,
	})

	s.scheduler.EnqueueRaftRequest(req.RangeID)
	return nil
}

func (s *Store) processRaftRequest(
	ctx context.Context,
	req *RaftMessageRequest,
	inSnap IncomingSnapshot,
) (pErr *roachpb.Error) {
	// Lazily create the replica.
	rp, uninitRaftLocked, err := s.getOrCreateReplica(
		req.RangeID, req.ToReplica.ReplicaID, req.FromReplica)
	if err != nil {
		return roachpb.NewError(err)
	}
	r, release, err := rp.Acquire()
	defer release()
	if err != nil {
		return roachpb.NewError(err)
	}

	defer r.raftUnlock(uninitRaftLocked)
	r.setLastReplicaDescriptors(req)

	if req.Quiesce {
		if req.Message.Type != raftpb.MsgHeartbeat {
			panic(fmt.Sprintf("unexpected quiesce: %+v", req))
		}
		status := r.RaftStatus()
		if status != nil && status.Term == req.Message.Term && status.Commit == req.Message.Commit {
			r.quiesce()
			return
		}
	}

	// Check to see if a snapshot can be applied. Snapshots can always be applied
	// to initialized replicas. Note that if we add a placeholder we need to
	// already be holding Store.uninitRaftMu in order to prevent concurrent
	// raft-ready processing of uninitialized replicas.
	var addedPlaceholder bool
	var removePlaceholder bool
	if req.Message.Type == raftpb.MsgSnap && !r.IsInitialized() {
		if earlyReturn := func() bool {
			s.mu.Lock()
			defer s.mu.Unlock()
			placeholder, err := s.canApplySnapshotLocked(&inSnap.RangeDescriptor)
			if err != nil {
				// If the storage cannot accept the snapshot, drop it before
				// passing it to RawNode.Step, since our error handling
				// options past that point are limited.
				// TODO(arjun): Now that we have better raft transport error
				// handling, consider if this error should be returned and
				// handled by the sending store.
				log.Info(ctx, errors.Wrapf(err, "%s: cannot apply snapshot", r))
				return true
			}

			if placeholder != nil {
				// NB: The placeholder added here is either removed below after a
				// preemptive snapshot is applied or after the next call to
				// Replica.handleRaftReady. Note that we can only get here if the
				// replica doesn't exist or is uninitialized.
				if err := s.addPlaceholderLocked(placeholder); err != nil {
					log.Fatal(ctx, errors.Wrapf(err, "%s: could not add vetted placeholder %s", s, placeholder))
				}
				addedPlaceholder = true
			}
			return false
		}(); earlyReturn {
			return nil
		}

		if addedPlaceholder {
			// If we added a placeholder remove it before we return unless some other
			// part of the code takes ownership of the removal (indicated by setting
			// removePlaceholder to false).
			removePlaceholder = true
			defer func() {
				if removePlaceholder {
					if s.removePlaceholder(req.RangeID) {
						atomic.AddInt32(&s.counts.removedPlaceholders, 1)
					}
				}
			}()
		}
	}

	if req.ToReplica.ReplicaID == 0 {
		if req.Message.Type == raftpb.MsgSnap {
			// Allow snapshots to be applied to replicas before they are
			// members of the raft group (i.e. replicas with an ID of 0). This
			// is the only operation that can be performed before it is part of
			// the raft group.

			defer func() {
				s.mu.Lock()
				defer s.mu.Unlock()

				// We need to remove the placeholder regardless of whether the snapshot
				// applied successfully or not.
				if addedPlaceholder {
					// Clear the replica placeholder; we are about to swap it with a real replica.
					if !s.removePlaceholderLocked(req.RangeID) {
						log.Fatalf(ctx, "%s: could not remove placeholder after preemptive snapshot", r)
					}
					if pErr == nil {
						atomic.AddInt32(&s.counts.filledPlaceholders, 1)
					} else {
						atomic.AddInt32(&s.counts.removedPlaceholders, 1)
					}
					removePlaceholder = false
				}

				if pErr == nil {
					// If the snapshot succeeded, process the range descriptor update.
					if err := s.processRangeDescriptorUpdateLocked(r); err != nil {
						pErr = roachpb.NewError(err)
					}
				}
			}()

			// Requiring that the Term is set in a message makes sure that we
			// get all of Raft's internal safety checks (it confuses messages
			// at term zero for internal messages). The sending side uses the
			// term from the snapshot itself, but we'll just check nonzero.
			if req.Message.Term == 0 {
				return roachpb.NewErrorf(
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
			//    canApplySnapshotLocked above, but there are concerns about two
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
			// We are paranoid about applying preemptive snapshots (which
			// were constructed via our code rather than raft) to the "real"
			// raft group. Instead, we destroy the "real" raft group if one
			// exists (this is rare in production, although it occurs in
			// tests), apply the preemptive snapshot to a temporary raft
			// group, then discard that one as well to be replaced by a real
			// raft group when we get a new replica ID.
			//
			// It might be OK instead to apply preemptive snapshots just
			// like normal ones (essentially switching between regular and
			// preemptive mode based on whether or not we have a raft group,
			// instead of based on the replica ID of the snapshot message).
			// However, this is a risk that we're not yet willing to take.
			// Additionally, without some additional plumbing work, doing so
			// would limit the effectiveness of RaftTransport.SendSync for
			// preemptive snapshots.
			r.mu.internalRaftGroup = nil
			r.mu.Unlock()

			appliedIndex, _, err := loadAppliedIndex(ctx, r.store.Engine(), r.RangeID)
			if err != nil {
				return roachpb.NewError(err)
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
					&raftLogger{ctx: r.ctx},
				), nil)
			if err != nil {
				return roachpb.NewError(err)
			}
			// We have a Raft group; feed it the message.
			if err := raftGroup.Step(req.Message); err != nil {
				return roachpb.NewError(errors.Wrap(err, "unable to process preemptive snapshot"))
			}
			// In the normal case, the group should ask us to apply a snapshot.
			// If it doesn't, our snapshot was probably stale.
			var ready raft.Ready
			if raftGroup.HasReady() {
				ready = raftGroup.Ready()
			}
			if raft.IsEmptySnap(ready.Snapshot) {
				// Raft discarded the snapshot, indicating that our local
				// state is already ahead of what the snapshot provides.
				return nil
			}

			// Apply the snapshot, as Raft told us to.
			if err := r.applySnapshot(ctx, inSnap, ready.Snapshot, ready.HardState); err != nil {
				return roachpb.NewError(err)
			}

			// NB: See the defer at the start of this block for the removal of the
			// placeholder and processing of the range descriptor update.
			return nil

			// At this point, the Replica has data but no ReplicaID. We hope
			// that it turns into a "real" Replica by means of receiving Raft
			// messages addressed to it with a ReplicaID, but if that doesn't
			// happen, at some point the Replica GC queue will have to grab it.
		}
		// We disallow non-snapshot messages to replica ID 0. Note that
		// getOrCreateReplica disallows moving the replica ID backward, so the only
		// way we can get here is if the replica did not previously exist.
		if log.V(1) {
			log.Infof(s.Ctx(), "refusing incoming Raft message %s for range %d from %+v to %+v",
				req.Message.Type, req.RangeID, req.FromReplica, req.ToReplica)
		}
		return roachpb.NewErrorf("cannot recreate replica that is not a member of its range (StoreID %s not found in range %d)",
			r.store.StoreID(), req.RangeID)
	}

	if err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
		// We're processing a message from another replica which means that the
		// other replica is not quiesced, so we don't need to wake the leader.
		r.unquiesceLocked()
		return false, /* !unquiesceAndWakeLeader */
			raftGroup.Step(req.Message)
	}); err != nil {
		return roachpb.NewError(err)
	}

	if (inSnap.SnapUUID == uuid.UUID{}) {
		// TODO(peter) investigate calling handleRaftReadyRaftMuLocked directly.
		s.enqueueRaftUpdateCheck(req.RangeID)
		removePlaceholder = false
	} else {
		// Force the replica to deal with this snapshot right now.
		if err := r.handleRaftReadyRaftMuLocked(&uninitRaftLocked, inSnap); err != nil {
			// mimic the behavior in processRaft.
			panic(err)
		}
		removePlaceholder = false
	}

	return nil
}

// HandleRaftResponse handles response messages from the raft transport. It
// requires that s.mu is not held.
func (s *Store) HandleRaftResponse(ctx context.Context, resp *RaftMessageResponse) {
	ctx = s.logContext(ctx)
	switch val := resp.Union.GetValue().(type) {
	case *roachpb.Error:
		switch val.GetDetail().(type) {
		case *roachpb.ReplicaTooOldError:
			ref, err := s.GetReplica(resp.RangeID)
			if err == nil {
				if added, err := s.replicaGCQueue.Add(ref, replicaGCPriorityRemoved); err != nil {
					log.Errorf(ctx, "%s: unable to add replica %d to GC queue: %s", ref, resp.ToReplica, err)
				} else if added {
					log.Infof(ctx, "%s: replica %s too old, added to replica GC queue", ref, resp.ToReplica)
				}
			}

		default:
			log.Warningf(ctx, "got error from range %d, replica %s: %s",
				resp.RangeID, resp.FromReplica, val)
		}

	default:
		log.Infof(ctx, "got unknown raft response type %T from replica %s: %s", val, resp.FromReplica, val)
	}
}

// sendSnapshot sends an outgoing snapshot via a pre-opened GRPC stream.
func sendSnapshot(
	stream MultiRaft_RaftSnapshotClient,
	header SnapshotRequest_Header,
	snap *OutgoingSnapshot,
	newBatch func() engine.Batch,
) error {
	if err := stream.Send(&SnapshotRequest{Header: &header}); err != nil {
		return err
	}
	// Wait until we get a response from the server.
	resp, err := stream.Recv()
	switch resp.Status {
	case SnapshotResponse_DECLINED:
		if header.CanDecline {
			return errors.Errorf("range=%s: remote declined snapshot: %s",
				header.RangeDescriptor.RangeID, resp.Message)
		}
		return errors.Errorf("range=%s: programming error: remote declined required snapshot: %s",
			header.RangeDescriptor.RangeID, resp.Message)
	case SnapshotResponse_ERROR:
		return errors.Errorf("range=%s: remote couldn't accept snapshot with error: %s",
			header.RangeDescriptor.RangeID, resp.Message)
	case SnapshotResponse_ACCEPTED:
		// This is the response we're expecting. Continue with snapshot sending.
	default:
		return errors.Errorf("range=%s: server sent an invalid status during negotiation: %s",
			header.RangeDescriptor.RangeID, resp.Status)
	}

	// Determine the unreplicated key prefix so we can drop any
	// unreplicated keys from the snapshot.
	unreplicatedPrefix := keys.MakeRangeIDUnreplicatedPrefix(header.RangeDescriptor.RangeID)
	var alloc bufalloc.ByteAllocator
	// TODO(jordan) make this configurable. For now, 1MB.
	const batchSize = 1 << 20
	n := 0
	var b engine.Batch
	for ; snap.Iter.Valid(); snap.Iter.Next() {
		var key engine.MVCCKey
		var value []byte
		alloc, key, value = snap.Iter.allocIterKeyValue(alloc)
		if bytes.HasPrefix(key.Key, unreplicatedPrefix) {
			continue
		}
		n++
		mvccKey := engine.MVCCKey{
			Key:       key.Key,
			Timestamp: key.Timestamp,
		}
		if b == nil {
			b = newBatch()
		}
		if err := b.Put(mvccKey, value); err != nil {
			b.Close()
			return err
		}

		if len(b.Repr()) >= batchSize {
			if err := sendBatch(stream, b); err != nil {
				return err
			}
			b = nil
			// We no longer need the keys and values in the batch we just sent,
			// so reset alloc and allow them to be garbage collected.
			alloc = bufalloc.ByteAllocator{}
		}
	}
	if b != nil {
		if err := sendBatch(stream, b); err != nil {
			return err
		}
	}

	rangeID := header.RangeDescriptor.RangeID

	truncState, err := loadTruncatedState(stream.Context(), snap.EngineSnap, rangeID)
	if err != nil {
		return err
	}
	firstIndex := truncState.Index + 1

	endIndex := snap.RaftSnap.Metadata.Index + 1
	logEntries := make([][]byte, 0, endIndex-firstIndex)

	scanFunc := func(kv roachpb.KeyValue) (bool, error) {
		bytes, err := kv.Value.GetBytes()
		if err == nil {
			logEntries = append(logEntries, bytes)
		}
		return false, err
	}

	if err := iterateEntries(stream.Context(), snap.EngineSnap, rangeID, firstIndex, endIndex, scanFunc); err != nil {
		return err
	}
	if err := stream.Send(&SnapshotRequest{LogEntries: logEntries, Final: true}); err != nil {
		return err
	}
	log.Infof(stream.Context(), "streamed snapshot: kv pairs: %d, log entries: %d",
		n, len(logEntries))

	resp, err = stream.Recv()
	if err != nil {
		return errors.Wrapf(err, "range=%s: remote failed to apply snapshot", header.RangeDescriptor.RangeID)
	}
	switch resp.Status {
	case SnapshotResponse_ERROR:
		return errors.Errorf("range=%s: remote failed to apply snapshot for reason %s",
			header.RangeDescriptor.RangeID, resp.Message)
	case SnapshotResponse_APPLIED:
		return nil
	default:
		return errors.Errorf("range=%s: server sent an invalid status during finalization: %s",
			header.RangeDescriptor.RangeID, resp.Status)
	}
}

func sendBatch(stream MultiRaft_RaftSnapshotClient, batch engine.Batch) error {
	repr := batch.Repr()
	batch.Close()
	return stream.Send(&SnapshotRequest{KVBatch: repr})
}

// enqueueRaftUpdateCheck asynchronously registers the given range ID to be
// checked for raft updates when the processRaft goroutine is idle.
func (s *Store) enqueueRaftUpdateCheck(rangeID roachpb.RangeID) {
	s.scheduler.EnqueueRaftReady(rangeID)
}

func (s *Store) processRequestQueue(rangeID roachpb.RangeID) {
	s.mu.Lock()
	q, ok := s.mu.replicaQueues[rangeID]
	if ok {
		delete(s.mu.replicaQueues, rangeID)
	}
	s.mu.Unlock()

	for _, info := range q {
		if pErr := s.processRaftRequest(info.respStream.Context(), info.req, IncomingSnapshot{}); pErr != nil {
			if err := info.respStream.Send(newRaftMessageResponse(info.req, pErr)); err != nil {
				// Seems excessive to log this on every occurrence as the other side
				// might have closed.
				if log.V(1) {
					log.Info(s.Ctx(), errors.Wrap(err, "error sending error"))
				}
			}
		}
	}
}

func (s *Store) processReady(rangeID roachpb.RangeID) {
	start := timeutil.Now()

	ref, err := s.GetReplica(rangeID)
	if err != nil {
		return
	}
	r, release, err := ref.Acquire()
	defer release()
	if err != nil {
		return
	}

	if err := r.handleRaftReady(IncomingSnapshot{}); err != nil {
		panic(err) // TODO(bdarnell)
	}
	elapsed := timeutil.Since(start)
	s.metrics.RaftWorkingDurationNanos.Inc(elapsed.Nanoseconds())
	// If Raft processing took longer than 10x the raft tick interval something
	// bad is going on. Such long processing time means we'll have starved
	// local replicas of ticks and remote replicas will likely start
	// campaigning.
	var warnDuration = 10 * s.ctx.RaftTickInterval
	if elapsed >= warnDuration {
		log.Warningf(s.Ctx(), "%s: handle raft ready: %.1fs", r, elapsed.Seconds())
	}
	if !r.IsInitialized() {
		// Only an uninitialized replica can have a placeholder since, by
		// definition, an initialized replica will be present in the
		// replicasByKey map. While the replica will usually consume the
		// placeholder itself, that isn't guaranteed and so this invocation
		// here is crucial (i.e. don't remove it).
		if s.removePlaceholder(r.RangeID) {
			atomic.AddInt32(&s.counts.droppedPlaceholders, 1)
		}
	}
}

func (s *Store) processTick(rangeID roachpb.RangeID) (ready bool) {
	start := timeutil.Now()

	ref, err := s.GetReplica(rangeID)
	if err != nil {
		return false
	}
	r, release, err := ref.Acquire()
	defer release()
	if err != nil {
		return false
	}

	exists, err := r.tick()
	if err != nil {
		log.Error(s.Ctx(), err)
	}
	s.metrics.RaftTickingDurationNanos.Inc(timeutil.Since(start).Nanoseconds())
	return exists
}

func (s *Store) processRaft() {
	if s.ctx.TestingKnobs.DisableProcessRaft {
		return
	}
	s.scheduler.Start(s.stopper)
	s.raftTickLoop()
	s.raftSnapshotStatusLoop()
}

func (s *Store) raftTickLoop() {
	s.stopper.RunWorker(func() {
		ticker := time.NewTicker(s.ctx.RaftTickInterval)
		defer func() {
			ticker.Stop()
			s.ctx.Transport.Stop(s.StoreID())
		}()

		var rangeIDs []roachpb.RangeID

		for {
			select {
			case <-ticker.C:
				rangeIDs = rangeIDs[:0]

				s.mu.Lock()
				for rangeID := range s.mu.replicas {
					rangeIDs = append(rangeIDs, rangeID)
				}
				s.mu.Unlock()

				s.scheduler.EnqueueRaftTick(rangeIDs...)

				s.metrics.RaftTicks.Inc(1)

			case <-s.stopper.ShouldStop():
				return
			}
		}
	})
}

func (s *Store) raftSnapshotStatusLoop() {
	s.stopper.RunWorker(func() {
		// We use a single goroutine for processing snapshot reporting. This
		// goroutine needs to grab Replica.raftMu which means that it can block
		// waiting for other raft processing on the range to complete. But note that
		// the replica we're reporting the snapshot status on is the one which
		// generated the snapshot (i.e. the leader). It is very unlikely that it will
		// be doing expensive raft processing elsewhere in the system.
		for {
			select {
			case st := <-s.ctx.Transport.SnapshotStatusChan:
				func() {
					ref, err := s.GetReplica(st.Req.RangeID)
					if err != nil {
						return
					}
					r, release, err := ref.Acquire()
					defer release()
					if err != nil {
						return
					}
					r.reportSnapshotStatus(st.Req.Message.To, st.Err)
				}()
			case <-s.stopper.ShouldStop():
				return
			}
		}
	})
}

var errRetry = errors.New("retry: orphaned replica")

// getOrCreateReplica returns a replica for the given RangeID, creating an
// uninitialized replica if necessary. The caller must not hold the store's
// lock. The returned replica has Replica.raftMu locked and it is the caller's
// responsibility to unlock it.
func (s *Store) getOrCreateReplica(
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	creatingReplica roachpb.ReplicaDescriptor,
) (_ ReplicaRef, uninitRaftLocked bool, _ error) {
	for {
		r, uninitRaftLocked, err := s.tryGetOrCreateReplica(
			rangeID, replicaID, creatingReplica)
		if err == errRetry {
			continue
		}
		if err != nil {
			return nil, false, err
		}
		return r, uninitRaftLocked, err
	}
}

// tryGetOrCreateReplica performs a single attempt at trying to lookup or
// create a replica. It will fail with errRetry if it finds a Replica that has
// been destroyed (and is no longer in Store.mu.replicas) or if during creation
// another goroutine gets there first. In either case, a subsequent call to
// tryGetOrCreateReplica will likely succeed, hence the loop in
// getOrCreateReplica.
func (s *Store) tryGetOrCreateReplica(
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	creatingReplica roachpb.ReplicaDescriptor,
) (_ ReplicaRef, uninitRaftLocked bool, _ error) {
	// The common case: look up an existing (initialized) replica.
	s.mu.Lock()
	r, ok := s.mu.replicas[rangeID]
	s.mu.Unlock()
	if ok {
		// Drop messages that come from a node that we believe was once a member of
		// the group but has been removed.
		desc := r.Desc()
		_, found := desc.GetReplicaDescriptorByID(creatingReplica.ReplicaID)
		// It's not a current member of the group. Is it from the past?
		if !found && creatingReplica.ReplicaID < desc.NextReplicaID {
			return nil, false, &roachpb.ReplicaTooOldError{}
		}

		uninitRaftUnlocked := r.raftLock()
		r.mu.Lock()
		destroyed, corrupted := r.mu.destroyed, r.mu.corrupted
		r.mu.Unlock()
		if destroyed != nil {
			r.raftUnlock(uninitRaftLocked)
			if corrupted {
				return nil, false, destroyed
			}
			return nil, false, errRetry
		}
		if err := r.setReplicaID(replicaID); err != nil {
			r.raftUnlock(uninitRaftLocked)
			return nil, false, err
		}
		return r.AsRef(), uninitRaftUnlocked, nil
	}

	// No replica currently exists, so we'll try to create one. Before creating
	// the replica, see if there is a tombstone which would indicate that this is
	// a stale message.
	tombstoneKey := keys.RaftTombstoneKey(rangeID)
	var tombstone roachpb.RaftTombstone
	if ok, err := engine.MVCCGetProto(context.Background(), s.Engine(), tombstoneKey, hlc.ZeroTimestamp, true, nil, &tombstone); err != nil {
		return nil, false, err
	} else if ok {
		if replicaID != 0 && replicaID < tombstone.NextReplicaID {
			return nil, false, &roachpb.RaftGroupDeletedError{}
		}
	}

	// Create a new replica and lock it for raft processing. This will lock
	// Replica.raftMu and Store.uninitRaftMu because the newly created replica is
	// uninitialized.
	r = newReplica(rangeID, s)
	r.creatingReplica = &creatingReplica
	if !r.raftLock() {
		log.Fatalf(s.Ctx(), "uninitRaftLocked must be true")
	}

	// Install the replica in the store's replica map. The replica is in an
	// inconsistent state, but nobody will be accessing it while we hold its
	// locks.
	s.mu.Lock()
	// Grab the internal Replica state lock to ensure nobody mucks with our
	// replica even outside of raft processing. Have to do this after grabbing
	// Store.mu to maintain lock ordering invariant.
	r.mu.Lock()
	// Add the range to range map, but not replicasByKey since the range's start
	// key is unknown. The range will be added to replicasByKey later when a
	// snapshot is applied. After unlocking Store.mu above, another goroutine
	// might have snuck in and created the replica, so we retry on error.
	if s.addReplicaToRangeMapLocked(r) != nil {
		r.mu.Unlock()
		s.mu.Unlock()
		r.raftUnlock(true)
		return nil, false, errRetry
	}
	s.mu.uninitReplicas[r.RangeID] = r
	s.mu.Unlock()

	desc := &roachpb.RangeDescriptor{
		RangeID: rangeID,
		// TODO(bdarnell): other fields are unknown; need to populate them from
		// snapshot.
	}
	if err := r.initLocked(desc, s.Clock(), replicaID); err != nil {
		// Mark the replica as destroyed and remove it from the replicas maps to
		// ensure nobody tries to use it
		r.mu.destroyed = errors.Wrapf(err, "%s: failed to initialize", r)
		r.mu.Unlock()
		s.mu.Lock()
		delete(s.mu.replicas, rangeID)
		delete(s.mu.replicaQueues, rangeID)
		delete(s.mu.uninitReplicas, rangeID)
		s.mu.Unlock()
		r.raftUnlock(true)
		return nil, false, err
	}
	r.mu.Unlock()
	return r.AsRef(), true, nil
}

// canApplySnapshot returns (_, nil) if the snapshot can be applied to
// this store's replica (i.e. the snapshot is not from an older incarnation of
// the replica) and a placeholder can be added to the replicasByKey map (if
// necessary). If a placeholder is required, it is returned as the first value.
func (s *Store) canApplySnapshot(rangeDescriptor *roachpb.RangeDescriptor) (*ReplicaPlaceholder, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.canApplySnapshotLocked(rangeDescriptor)
}

func (s *Store) canApplySnapshotLocked(rangeDescriptor *roachpb.RangeDescriptor) (*ReplicaPlaceholder, error) {
	if r, ok := s.mu.replicas[rangeDescriptor.RangeID]; ok && r.IsInitialized() {
		// We have the range and it's initialized, so let the snapshot through.
		return nil, nil
	}

	// We don't have the range (or we have an uninitialized
	// placeholder). Will we be able to create/initialize it?
	if exRng, ok := s.mu.replicaPlaceholders[rangeDescriptor.RangeID]; ok {
		return nil, errors.Errorf("%s: canApplySnapshotLocked: cannot add placeholder, have an existing placeholder %s", s, exRng)
	}

	if exRange := s.getOverlappingKeyRangeLocked(rangeDescriptor); exRange != nil {
		// We have a conflicting range, so we must block the snapshot.
		// When such a conflict exists, it will be resolved by one range
		// either being split or garbage collected.
		exReplica, err := s.getReplicaLocked(exRange.Desc().RangeID)
		if err != nil {
			log.Warning(s.Ctx(), errors.Wrapf(
				err, "unable to look up overlapping replica on %s", exReplica))
		}
		return nil, errors.Errorf("snapshot intersects existing range %s", exReplica)
	}

	placeholder := &ReplicaPlaceholder{
		rangeDesc: *rangeDescriptor,
	}
	return placeholder, nil
}

func (s *Store) updateCapacityGauges() error {
	desc, err := s.Descriptor()
	if err != nil {
		return err
	}
	s.metrics.Capacity.Update(desc.Capacity.Capacity)
	s.metrics.Available.Update(desc.Capacity.Available)

	return nil
}

// updateReplicationGauges counts a number of simple replication statistics for
// the ranges in this store.
// TODO(bram): #4564 It may be appropriate to compute these statistics while
// scanning ranges. An ideal solution would be to create incremental events
// whenever availability changes.
func (s *Store) updateReplicationGauges() error {
	// Load the system config.
	cfg, ok := s.Gossip().GetSystemConfig()
	if !ok {
		return errors.Errorf("%s: system config not yet available", s)
	}

	var (
		raftLeaderCount                 int64
		leaseHolderCount                int64
		raftLeaderNotLeaseHolderCount   int64
		quiescentCount                  int64
		availableRangeCount             int64
		replicaAllocatorNoopCount       int64
		replicaAllocatorAddCount        int64
		replicaAllocatorRemoveCount     int64
		replicaAllocatorRemoveDeadCount int64
	)

	timestamp := s.ctx.Clock.Now()

	newStoreRangeSet(s).Visit(func(ref ReplicaRef) bool {
		rep, release, err := ref.Acquire()
		defer release()
		if err != nil {
			return true
		}
		desc := rep.Desc()
		zoneConfig, err := cfg.GetZoneConfigForKey(desc.StartKey)
		if err != nil {
			log.Error(s.Ctx(), err)
			return true
		}

		rep.mu.Lock()
		raftStatus := rep.raftStatusLocked()
		lease := rep.mu.state.Lease
		if rep.mu.quiescent || rep.mu.internalRaftGroup == nil {
			quiescentCount++
		}
		rep.mu.Unlock()

		leaseCovers := lease.Covers(timestamp)
		leaseOwned := lease.OwnedBy(s.Ident.StoreID)

		// To avoid double counting, most stats are only counted on the raft
		// leader. The lease holder count is the exception, which is counted by
		// all replicas.
		if raftStatus != nil && raftStatus.SoftState.RaftState == raft.StateLeader {
			raftLeaderCount++

			if leaseCovers {
				// If any replica holds the range lease, the range is available.
				availableRangeCount++

				if leaseOwned {
					leaseHolderCount++
				} else {
					raftLeaderNotLeaseHolderCount++
				}
			} else {
				// If there is no range lease, then as long as a majority of
				// the replicas are current then it is available.
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

			switch action, _ := s.allocator.ComputeAction(zoneConfig, desc); action {
			case AllocatorNoop:
				replicaAllocatorNoopCount++
			case AllocatorAdd:
				replicaAllocatorAddCount++
			case AllocatorRemove:
				replicaAllocatorRemoveCount++
			case AllocatorRemoveDead:
				replicaAllocatorRemoveDeadCount++
			}
		} else if leaseCovers && leaseOwned {
			leaseHolderCount++
		}
		return true // more
	})

	s.metrics.RaftLeaderCount.Update(raftLeaderCount)
	s.metrics.RaftLeaderNotLeaseHolderCount.Update(raftLeaderNotLeaseHolderCount)
	s.metrics.LeaseHolderCount.Update(leaseHolderCount)
	s.metrics.QuiescentCount.Update(quiescentCount)

	s.metrics.AvailableRangeCount.Update(availableRangeCount)

	s.metrics.ReplicaAllocatorNoopCount.Update(replicaAllocatorNoopCount)
	s.metrics.ReplicaAllocatorRemoveCount.Update(replicaAllocatorRemoveCount)
	s.metrics.ReplicaAllocatorAddCount.Update(replicaAllocatorAddCount)
	s.metrics.ReplicaAllocatorRemoveDeadCount.Update(replicaAllocatorRemoveDeadCount)

	return nil
}

// ComputeMetrics immediately computes the current value of store metrics which
// cannot be computed incrementally. This method should be invoked periodically
// by a higher-level system which records store metrics.
func (s *Store) ComputeMetrics(tick int) error {
	if err := s.updateCapacityGauges(); err != nil {
		return err
	}

	if err := s.updateReplicationGauges(); err != nil {
		return err
	}

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
		s.metrics.RdbReadAmplification.Update(int64(readAmp))
		// Log this metric infrequently.
		if tick%100 == 0 {
			log.Infof(s.Ctx(), "sstables (read amplification = %d):\n%s", readAmp, sstables)
		}
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
	s.visitReplicasLocked(startKey, endKey, func(ref ReplicaRef) bool {
		repl, release, err := ref.Acquire()
		defer release()
		if err != nil {
			return true // continue
		}

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
	newStoreRangeSet(s).Visit(func(ref ReplicaRef) bool {
		r, release, err := ref.Acquire()
		defer release()
		if err != nil {
			return true // continue
		}

		if !r.IsInitialized() {
			return true
		}
		repDesc, err := r.GetReplicaDescriptor()
		if err != nil {
			if _, ok := err.(*roachpb.RangeNotFoundError); ok {
				return true
			}
			log.Fatalf(s.Ctx(), "unexpected error: %s", err)
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
func (s *Store) Reserve(ctx context.Context, req ReservationRequest) ReservationResponse {
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
