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
	"math/rand"
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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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

	defaultStoreMutexWarnThreshold = 100 * time.Millisecond
)

var changeTypeInternalToRaft = map[roachpb.ReplicaChangeType]raftpb.ConfChangeType{
	roachpb.ADD_REPLICA:    raftpb.ConfChangeAddNode,
	roachpb.REMOVE_REPLICA: raftpb.ConfChangeRemoveNode,
}

var storeSchedulerConcurrency = envutil.EnvOrDefaultInt(
	"COCKROACH_SCHEDULER_CONCURRENCY", 2*runtime.NumCPU())

// RaftElectionTimeout returns the raft election timeout, as computed
// from the specified tick interval and number of election timeout
// ticks. If raftElectionTimeoutTicks is 0, uses the value of
// defaultRaftElectionTimeoutTicks.
func RaftElectionTimeout(
	raftTickInterval time.Duration, raftElectionTimeoutTicks int,
) time.Duration {
	if raftTickInterval == 0 {
		raftTickInterval = base.DefaultRaftTickInterval
	}
	if raftElectionTimeoutTicks == 0 {
		raftElectionTimeoutTicks = defaultRaftElectionTimeoutTicks
	}
	return time.Duration(raftElectionTimeoutTicks) * raftTickInterval
}

// RangeLeaseDurations computes durations for range lease expiration
// and renewal based on a default multiple of Raft election timeout.
func RangeLeaseDurations(
	raftElectionTimeout time.Duration,
) (rangeLeaseActive time.Duration, rangeLeaseRenewal time.Duration) {
	rangeLeaseActive = rangeLeaseRaftElectionTimeoutMultiplier * raftElectionTimeout
	rangeLeaseRenewal = rangeLeaseActive / rangeLeaseRenewalDivisor
	return
}

// TestStoreConfig has some fields initialized with values relevant in tests.
func TestStoreConfig() StoreConfig {
	return StoreConfig{
		AmbientCtx:                     log.AmbientContext{Tracer: tracing.NewTracer()},
		RaftTickInterval:               100 * time.Millisecond,
		CoalescedHeartbeatsInterval:    50 * time.Millisecond,
		RaftHeartbeatIntervalTicks:     1,
		RaftElectionTimeoutTicks:       3,
		ScanInterval:                   10 * time.Minute,
		ConsistencyCheckInterval:       10 * time.Minute,
		ConsistencyCheckPanicOnFailure: true,
		MetricsSampleInterval:          time.Hour,
		EnableCoalescedHeartbeats:      true,
	}
}

func newRaftConfig(
	strg raft.Storage, id uint64, appliedIndex uint64, storeCfg StoreConfig, logger raft.Logger,
) *raft.Config {
	return &raft.Config{
		ID:            id,
		Applied:       appliedIndex,
		ElectionTick:  storeCfg.RaftElectionTimeoutTicks,
		HeartbeatTick: storeCfg.RaftHeartbeatIntervalTicks,
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

// A storeReplicaVisitor calls a visitor function for each of a store's
// initialized Replicas (in unspecified order).
type storeReplicaVisitor struct {
	store   *Store
	repls   []*Replica // Replicas to be visited.
	visited int        // Number of visited ranges, -1 before first call to Visit()
}

func newStoreReplicaVisitor(store *Store) *storeReplicaVisitor {
	return &storeReplicaVisitor{
		store:   store,
		visited: -1,
	}
}

// Visit calls the visitor with each Replica until false is returned.
func (rs *storeReplicaVisitor) Visit(visitor func(*Replica) bool) {
	// Copy the range IDs to a slice so that we iterate over some (possibly
	// stale) consistent view of all Replicas without holding the Store lock.
	// In particular, no locks are acquired during the copy process.
	rs.store.mu.Lock()
	rs.repls = make([]*Replica, 0, len(rs.store.mu.replicas))
	for _, repl := range rs.store.mu.replicas {
		rs.repls = append(rs.repls, repl)
	}
	rs.store.mu.Unlock()

	// The Replicas are already in "unspecified order" due to map iteration,
	// but we want to make sure it's completely random to prevent issues in
	// tests where stores are scanning replicas in lock-step and one store is
	// winning the race and getting a first crack at processing the replicas on
	// its queues.
	//
	// TODO(peter): Re-evaluate whether this is necessary after we allow
	// rebalancing away from the leaseholder. See TestRebalance_3To5Small.
	for i := 1; i < len(rs.repls); i++ {
		j := rand.Intn(i + 1)
		rs.repls[i], rs.repls[j] = rs.repls[j], rs.repls[i]
	}

	rs.visited = 0
	for _, repl := range rs.repls {
		// TODO(tschottdorf): let the visitor figure out if something's been
		// destroyed once we return errors from mutexes (#9190). After all, it
		// can still happen with this code.
		rs.visited++
		repl.mu.Lock()
		destroyed := repl.mu.destroyed
		initialized := repl.isInitializedLocked()
		repl.mu.Unlock()
		if initialized && destroyed == nil && !visitor(repl) {
			break
		}
	}
	rs.visited = 0
}

// TODO(tschottdorf): this method has highly doubtful semantics.
func (rs *storeReplicaVisitor) EstimatedCount() int {
	if rs.visited <= 0 {
		rs.store.mu.Lock()
		defer rs.store.mu.Unlock()
		return len(rs.store.mu.replicas)
	}
	return len(rs.repls) - rs.visited
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
	cfg                     StoreConfig
	db                      *client.DB
	engine                  engine.Engine               // The underlying key-value store
	allocator               Allocator                   // Makes allocation decisions
	rangeIDAlloc            *idAllocator                // Range ID allocator
	gcQueue                 *gcQueue                    // Garbage collection queue
	splitQueue              *splitQueue                 // Range splitting queue
	replicateQueue          *replicateQueue             // Replication queue
	replicaGCQueue          *replicaGCQueue             // Replica GC queue
	raftLogQueue            *raftLogQueue               // Raft Log Truncation queue
	tsMaintenanceQueue      *timeSeriesMaintenanceQueue // Time series maintenance queue
	scanner                 *replicaScanner             // Replica scanner
	replicaConsistencyQueue *replicaConsistencyQueue    // Replica consistency check queue
	consistencyScanner      *replicaScanner             // Consistency checker scanner
	metrics                 *StoreMetrics
	intentResolver          *intentResolver
	raftEntryCache          *raftEntryCache

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
	// obeyed: Replica.raftMu < Replica.readOnlyCmdMu < Store.mu < Replica.mu
	// < Replica.unreachablesMu < Store.coalescedMu < Store.scheduler.mu.
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
	// Splits (and merges, but they're not finished and so will not be discussed
	// here) deserve special consideration: they operate on two ranges. Naively,
	// this is fine because the right-hand range is brand new, but an
	// uninitialized version may have been created by a raft message before we
	// process the split (see commentary on Replica.splitTrigger). We make this
	// safe by locking the right-hand range for the duration of the Raft command
	// containing the split/merge trigger.
	//
	// Note that because we acquire and release Store.mu and Replica.mu
	// repeatedly rather than holding a lock for an entire request, we are
	// actually relying on higher-level locks to ensure that things don't change
	// out from under us. In particular, handleRaftReady accesses the replicaID
	// more than once, and we rely on Replica.raftMu to ensure that this is not
	// modified by a concurrent HandleRaftRequest. (#4476)

	mu struct {
		// TODO(peter): evaluate runtime overhead of the timed mutex.
		syncutil.TimedMutex // Protects all variables in the mu struct.
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

// A StoreConfig encompasses the auxiliary objects and configuration
// required to create a store.
// All fields holding a pointer or an interface are required to create
// a store; the rest will have sane defaults set if omitted.
type StoreConfig struct {
	AmbientCtx log.AmbientContext

	Clock        *hlc.Clock
	DB           *client.DB
	Gossip       *gossip.Gossip
	NodeLiveness *NodeLiveness
	StorePool    *StorePool
	Transport    *RaftTransport

	// SQLExecutor is used by the store to execute SQL statements in a way that
	// is more direct than using a sql.Executor.
	SQLExecutor sqlutil.InternalExecutor

	// TimeSeriesDataStore is an interface used by the store's time series
	// maintenance queue to dispatch individual maintenance tasks.
	TimeSeriesDataStore TimeSeriesDataStore

	// RangeRetryOptions are the retry options when retryable errors are
	// encountered sending commands to ranges.
	RangeRetryOptions retry.Options

	// RaftTickInterval is the resolution of the Raft timer; other raft timeouts
	// are defined in terms of multiples of this value.
	RaftTickInterval time.Duration

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

	// RangeLeaseActiveDuration is the duration of the active period of leader
	// leases requested.
	RangeLeaseActiveDuration time.Duration

	// RangeLeaseRenewalDuration specifies a time interval at the end of the
	// active lease interval (i.e. bounded to the right by the start of the stasis
	// period) during which operations will trigger an asynchronous renewal of the
	// lease.
	RangeLeaseRenewalDuration time.Duration

	// MetricsSampleInterval is (server.Context).MetricsSampleInterval
	MetricsSampleInterval time.Duration

	// EnableCoalescedHeartbeats controls whether heartbeats are coalesced.
	EnableCoalescedHeartbeats bool
}

// StoreTestingKnobs is a part of the context used to control parts of the system.
type StoreTestingKnobs struct {
	// A callback to be called when executing every replica command.
	// If your filter is not idempotent, consider wrapping it in a
	// ReplayProtectionFilterWrapper.
	TestingCommandFilter storagebase.ReplicaCommandFilter
	TestingApplyFilter   storagebase.ReplicaApplyFilter
	// TestingResponseFilter is called after the replica processes a
	// command in order for unittests to modify the batch response,
	// error returned to the client, or to simulate network failures.
	TestingResponseFilter storagebase.ReplicaResponseFilter
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
	// DisablePeriodicGossips disables periodic gossiping.
	DisablePeriodicGossips bool
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
	// ReplicateQueueAcceptsUnsplit allows the replication queue to
	// process ranges that need to be split, for use in tests that use
	// the replication queue but disable the split queue.
	ReplicateQueueAcceptsUnsplit bool
	// NumKeysEvaluatedForRangeIntentResolution is set by the stores to the
	// number of keys evaluated for range intent resolution.
	NumKeysEvaluatedForRangeIntentResolution *int64
	// SkipMinSizeCheck, if set, makes the store creation process skip the check
	// for a minimum size.
	SkipMinSizeCheck bool
}

var _ base.ModuleTestingKnobs = &StoreTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*StoreTestingKnobs) ModuleTestingKnobs() {}

// Valid returns true if the StoreConfig is populated correctly.
// We don't check for Gossip and DB since some of our tests pass
// that as nil.
func (sc *StoreConfig) Valid() bool {
	return sc.Clock != nil && sc.Transport != nil &&
		sc.RaftTickInterval != 0 && sc.RaftHeartbeatIntervalTicks > 0 &&
		sc.RaftElectionTimeoutTicks > 0 && sc.ScanInterval >= 0 &&
		sc.ConsistencyCheckInterval >= 0 &&
		sc.AmbientCtx.Tracer != nil
}

// SetDefaults initializes unset fields in StoreConfig to values
// suitable for use on a local network.
// TODO(tschottdorf) see if this ought to be configurable via flags.
func (sc *StoreConfig) SetDefaults() {
	if (sc.RangeRetryOptions == retry.Options{}) {
		sc.RangeRetryOptions = base.DefaultRetryOptions()
	}

	if sc.RaftTickInterval == 0 {
		sc.RaftTickInterval = base.DefaultRaftTickInterval
	}
	if sc.CoalescedHeartbeatsInterval == 0 {
		sc.CoalescedHeartbeatsInterval = sc.RaftTickInterval / 2
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

	rangeLeaseActiveDuration, rangeLeaseRenewalDuration :=
		RangeLeaseDurations(RaftElectionTimeout(sc.RaftTickInterval, sc.RaftElectionTimeoutTicks))
	if sc.RangeLeaseActiveDuration == 0 {
		sc.RangeLeaseActiveDuration = rangeLeaseActiveDuration
	}
	if sc.RangeLeaseRenewalDuration == 0 {
		sc.RangeLeaseRenewalDuration = rangeLeaseRenewalDuration
	}
}

// NewStore returns a new instance of a store.
func NewStore(cfg StoreConfig, eng engine.Engine, nodeDesc *roachpb.NodeDescriptor) *Store {
	// TODO(tschottdorf) find better place to set these defaults.
	cfg.SetDefaults()

	if !cfg.Valid() {
		panic(fmt.Sprintf("invalid store configuration: %+v", &cfg))
	}
	s := &Store{
		cfg:       cfg,
		db:        cfg.DB, // TODO(tschottdorf) remove redundancy.
		engine:    eng,
		allocator: MakeAllocator(cfg.StorePool, cfg.AllocatorOptions),
		nodeDesc:  nodeDesc,
		metrics:   newStoreMetrics(cfg.MetricsSampleInterval),
	}

	// EnableCoalescedHeartbeats is enabled by TestStoreConfig, so in that case
	// ignore the environment variable. Otherwise, use whatever the environment
	// variable says should be used.
	if !cfg.EnableCoalescedHeartbeats {
		s.cfg.EnableCoalescedHeartbeats = envutil.EnvOrDefaultBool(
			"COCKROACH_ENABLE_COALESCED_HEARTBEATS", false)
	}

	s.intentResolver = newIntentResolver(s)
	s.raftEntryCache = newRaftEntryCache(cfg.RaftEntryCacheSize)
	s.drainLeases.Store(false)
	s.scheduler = newRaftScheduler(s.cfg.AmbientCtx, s.metrics, s, storeSchedulerConcurrency)

	storeMuLogger := syncutil.ThresholdLogger(
		s.AnnotateCtx(context.Background()),
		defaultStoreMutexWarnThreshold,
		func(ctx context.Context, msg string, args ...interface{}) {
			log.Warningf(ctx, "storeMu: "+msg, args...)
		},
		func(t time.Duration) {
			s.metrics.MuStoreNanos.RecordValue(t.Nanoseconds())
		},
	)
	s.mu.TimedMutex = syncutil.MakeTimedMutex(storeMuLogger)

	s.coalescedMu.Lock()
	s.coalescedMu.heartbeats = map[roachpb.StoreIdent][]RaftHeartbeat{}
	s.coalescedMu.heartbeatResponses = map[roachpb.StoreIdent][]RaftHeartbeat{}
	s.coalescedMu.Unlock()

	s.mu.Lock()
	s.mu.replicas = map[roachpb.RangeID]*Replica{}
	s.mu.replicaPlaceholders = map[roachpb.RangeID]*ReplicaPlaceholder{}
	s.mu.replicaQueues = map[roachpb.RangeID]raftRequestQueue{}
	s.mu.replicasByKey = btree.New(64 /* degree */)
	s.mu.uninitReplicas = map[roachpb.RangeID]*Replica{}
	s.mu.Unlock()

	if s.cfg.Gossip != nil {
		// Add range scanner and configure with queues.
		s.scanner = newReplicaScanner(
			s.cfg.AmbientCtx, cfg.ScanInterval, cfg.ScanMaxIdleTime, newStoreReplicaVisitor(s),
		)
		s.gcQueue = newGCQueue(s, s.cfg.Gossip)
		s.splitQueue = newSplitQueue(s, s.db, s.cfg.Gossip)
		s.replicateQueue = newReplicateQueue(
			s, s.cfg.Gossip, s.allocator, s.cfg.Clock, s.cfg.AllocatorOptions,
		)
		s.replicaGCQueue = newReplicaGCQueue(s, s.db, s.cfg.Gossip)
		s.raftLogQueue = newRaftLogQueue(s, s.db, s.cfg.Gossip)
		s.scanner.AddQueues(s.gcQueue, s.splitQueue, s.replicateQueue, s.replicaGCQueue, s.raftLogQueue)

		// Add consistency check scanner.
		s.consistencyScanner = newReplicaScanner(
			s.cfg.AmbientCtx, cfg.ConsistencyCheckInterval, 0, newStoreReplicaVisitor(s),
		)
		s.replicaConsistencyQueue = newReplicaConsistencyQueue(s, s.cfg.Gossip)
		s.consistencyScanner.AddQueues(s.replicaConsistencyQueue)

		if s.cfg.TimeSeriesDataStore != nil {
			s.tsMaintenanceQueue = newTimeSeriesMaintenanceQueue(
				s, s.db, s.cfg.Gossip, s.cfg.TimeSeriesDataStore,
			)
			s.scanner.AddQueues(s.tsMaintenanceQueue)
		}
	}

	if cfg.TestingKnobs.DisableReplicaGCQueue {
		s.setReplicaGCQueueActive(false)
	}
	if cfg.TestingKnobs.DisableReplicateQueue {
		s.setReplicateQueueActive(false)
	}
	if cfg.TestingKnobs.DisableSplitQueue {
		s.setSplitQueueActive(false)
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

// AnnotateCtx is a convenience wrapper; see AmbientContext.
func (s *Store) AnnotateCtx(ctx context.Context) context.Context {
	return s.cfg.AmbientCtx.AnnotateCtx(ctx)
}

// AnnotateCtxWithSpan is a convenience wrapper; see AmbientContext.
func (s *Store) AnnotateCtxWithSpan(
	ctx context.Context, opName string,
) (context.Context, opentracing.Span) {
	return s.cfg.AmbientCtx.AnnotateCtxWithSpan(ctx, opName)
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

	return util.RetryForDuration(10*s.cfg.RangeLeaseActiveDuration, func() error {
		var drainingLease *roachpb.Lease
		now := s.Clock().Now()
		newStoreReplicaVisitor(s).Visit(func(r *Replica) bool {
			lease, nextLease := r.getLease()
			// If we own an active lease or we're trying to obtain a lease
			// (and that request is fresh enough), wait.
			switch {
			case lease.OwnedBy(s.StoreID()) && lease.Covers(now):
				drainingLease = lease
			case nextLease != nil && nextLease.OwnedBy(s.StoreID()) && nextLease.Covers(now):
				drainingLease = nextLease
			default:
				return true
			}
			return false // stop
		})
		if drainingLease != nil {
			return errors.Errorf("lease %s is still active", drainingLease)
		}
		return nil
	})
}

// IsStarted returns true if the Store has been started.
func (s *Store) IsStarted() bool {
	return atomic.LoadInt32(&s.started) == 1
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

	_, err := engine.MVCCIterate(ctx, eng, start, end, hlc.MaxTimestamp, false /* !consistent */, nil, /* txn */
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

// ReadStoreIdent reads the StoreIdent from the store.
// It returns *NotBootstrappedError if the ident is missing (meaning that the
// store needs to be bootstrapped).
func ReadStoreIdent(ctx context.Context, eng engine.Engine) (roachpb.StoreIdent, error) {
	var ident roachpb.StoreIdent
	ok, err := engine.MVCCGetProto(
		ctx, eng, keys.StoreIdentKey(), hlc.ZeroTimestamp, true, nil, &ident)
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

	// Add the bookie to the store.
	s.bookie = newBookie(
		s.cfg.Clock,
		s.stopper,
		s.metrics,
		envutil.EnvOrDefaultDuration("COCKROACH_RESERVATION_TIMEOUT", ttlStoreGossip),
	)

	// Read the store ident if not already initialized. "NodeID != 0" implies
	// the store has already been initialized.
	if s.Ident.NodeID == 0 {
		// Read store ident and return a not-bootstrapped error if necessary.
		ident, err := ReadStoreIdent(ctx, s.engine)
		if err != nil {
			return err
		}
		s.Ident = ident
	}
	log.Event(ctx, "read store identity")

	// If the nodeID is 0, it has not be assigned yet.
	if s.nodeDesc.NodeID != 0 && s.Ident.NodeID != s.nodeDesc.NodeID {
		return errors.Errorf("node id:%d does not equal the one in node descriptor:%d", s.Ident.NodeID, s.nodeDesc.NodeID)
	}
	// Always set gossip NodeID before gossiping any info.
	if s.cfg.Gossip != nil {
		s.cfg.Gossip.NodeID.Set(ctx, s.Ident.NodeID)
	}

	// Set the store ID for logging.
	s.cfg.AmbientCtx.AddLogTagInt("s", int(s.StoreID()))

	// Create ID allocators.
	idAlloc, err := newIDAllocator(
		s.cfg.AmbientCtx, keys.RangeIDGenerator, s.db, 2 /* min ID */, rangeIDAllocCount, s.stopper,
	)
	if err != nil {
		return err
	}
	s.rangeIDAlloc = idAlloc

	now := s.cfg.Clock.Now()
	s.startedAt = now.WallTime

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

			if _, ok := desc.GetReplicaDescriptor(s.StoreID()); !ok {
				// We are no longer a member of the range, but we didn't GC the replica
				// before shutting down. Add the replica to the GC queue.
				if added, err := s.replicaGCQueue.Add(rep, replicaGCPriorityRemoved); err != nil {
					log.Errorf(ctx, "%s: unable to add replica to GC queue: %s", rep, err)
				} else if added {
					log.Infof(ctx, "%s: added to replica GC queue", rep)
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
	s.cfg.Transport.Listen(s.StoreID(), s)
	s.processRaft()

	doneUnfreezing := make(chan struct{})
	if s.stopper.RunAsyncTask(ctx, func(ctx context.Context) {
		defer close(doneUnfreezing)
		sem := make(chan struct{}, 512)
		var wg sync.WaitGroup // wait for unfreeze goroutines
		var unfrozen int64    // updated atomically
		newStoreReplicaVisitor(s).Visit(func(r *Replica) bool {
			r.mu.Lock()
			frozen := r.mu.state.IsFrozen()
			r.mu.Unlock()
			if !frozen {
				return true
			}
			wg.Add(1)
			if s.stopper.RunLimitedAsyncTask(
				ctx, sem, true /* wait */, func(ctx context.Context) {
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
	if s.cfg.Gossip != nil {
		if !s.cfg.TestingKnobs.DisablePeriodicGossips {
			// Register update channel for any changes to the system config.
			// This may trigger splits along structured boundaries,
			// and update max range bytes.
			gossipUpdateC := s.cfg.Gossip.RegisterSystemConfigChannel()
			s.stopper.RunWorker(func() {
				for {
					select {
					case <-gossipUpdateC:
						cfg, _ := s.cfg.Gossip.GetSystemConfig()
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
		}

		// Start the scanner. The construction here makes sure that the scanner
		// only starts after Gossip has connected, and that it does not block Start
		// from returning (as doing so might prevent Gossip from ever connecting).
		s.stopper.RunWorker(func() {
			select {
			case <-s.cfg.Gossip.Connected:
				s.scanner.Start(s.cfg.Clock, s.stopper)
			case <-s.stopper.ShouldStop():
				return
			}
		})

		// Start the consistency scanner.
		s.stopper.RunWorker(func() {
			select {
			case <-s.cfg.Gossip.Connected:
				s.consistencyScanner.Start(s.cfg.Clock, s.stopper)
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
	gossipFns := []struct {
		fn          func(context.Context) error
		description string
		interval    time.Duration
	}{
		{
			fn:          s.maybeGossipFirstRange,
			description: "first range descriptor",
			interval:    sentinelGossipInterval,
		},
		{
			fn:          s.maybeGossipSystemData,
			description: "system config",
			interval:    configGossipInterval,
		},
	}

	// Periodic updates run in a goroutine and signal a WaitGroup upon completion
	// of their first iteration.
	s.initComplete.Add(len(gossipFns))
	for _, gossipFn := range gossipFns {
		gossipFn := gossipFn // per-iteration copy
		s.stopper.RunWorker(func() {
			ctx := s.AnnotateCtx(context.Background())
			// Run the first time without waiting for the Ticker and signal the WaitGroup.
			if err := gossipFn.fn(ctx); err != nil {
				log.Warningf(ctx, "error gossiping %s: %s", gossipFn.description, err)
			}
			s.initComplete.Done()
			ticker := time.NewTicker(gossipFn.interval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					if err := gossipFn.fn(ctx); err != nil {
						log.Warningf(ctx, "error gossiping %s: %s", gossipFn.description, err)
					}
				case <-s.stopper.ShouldStop():
					return
				}
			}
		})
	}
}

// maybeGossipFirstRange checks whether the store has a replica of the
// first range and if so instructs it to gossip the cluster ID,
// sentinel gossip and first range descriptor. This is done in a retry
// loop in the event that the returned error indicates that the state
// of the range lease is not known. This can happen on lease command
// timeouts. The retry loop makes sure we try hard to keep asking for
// the lease instead of waiting for the next sentinelGossipInterval
// to transpire.
func (s *Store) maybeGossipFirstRange(ctx context.Context) error {
	retryOptions := base.DefaultRetryOptions()
	retryOptions.Closer = s.stopper.ShouldStop()
	for loop := retry.Start(retryOptions); loop.Next(); {
		rng := s.LookupReplica(roachpb.RKeyMin, nil)
		if rng != nil {
			pErr := rng.maybeGossipFirstRange(ctx)
			if nlErr, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); !ok || nlErr.LeaseHolder != nil {
				return pErr.GoError()
			}
		} else {
			return nil
		}
	}
	return nil
}

// maybeGossipSystemData looks for the ranges containing system data
// and acquires the range lease for them which in turn will trigger
// Replica.maybeGossipSystemConfig and Replica.maybeGossipNodeLiveness.
func (s *Store) maybeGossipSystemData(ctx context.Context) error {
	for _, span := range keys.GossipedSystemSpans {
		rng := s.LookupReplica(roachpb.RKey(span.Key), nil)
		if rng == nil {
			// This store has no range with this configuration.
			continue
		}
		// Wake up the replica. If it acquires a fresh lease, it will
		// gossip. If an unexpected error occurs (i.e. nobody else seems to
		// have an active lease but we still failed to obtain it), return
		// that error.
		if _, pErr := rng.getLeaseForGossip(ctx); pErr != nil {
			return pErr.GoError()
		}
	}
	return nil
}

// systemGossipUpdate is a callback for gossip updates to
// the system config which affect range split boundaries.
func (s *Store) systemGossipUpdate(cfg config.SystemConfig) {
	// For every range, update its MaxBytes and check if it needs to be split.
	newStoreReplicaVisitor(s).Visit(func(repl *Replica) bool {
		if zone, err := cfg.GetZoneConfigForKey(repl.Desc().StartKey); err == nil {
			repl.SetMaxBytes(zone.RangeMaxBytes)
		}
		s.splitQueue.MaybeAdd(repl, s.cfg.Clock.Now())
		return true // more
	})
}

// GossipStore broadcasts the store on the gossip network.
func (s *Store) GossipStore(ctx context.Context) error {
	// This should always return immediately and acts as a sanity check that we
	// don't try to gossip before we're connected.
	select {
	case <-s.cfg.Gossip.Connected:
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
	if err := s.cfg.Gossip.AddInfoProto(gossipStoreKey, storeDesc, ttlStoreGossip); err != nil {
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
	// that the node descriptor has been gossiped as that is how remote nodes
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
		electionTimeout := s.cfg.RaftTickInterval * time.Duration(s.cfg.RaftElectionTimeoutTicks)
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
	return s.cfg.Gossip.AddInfoProto(key, &deadReplicas, ttlStoreGossip)
}

// Bootstrap writes a new store ident to the underlying engine. To
// ensure that no crufty data already exists in the engine, it scans
// the engine contents before writing the new store ident. The engine
// should be completely empty. It returns an error if called on a
// non-empty engine.
func (s *Store) Bootstrap(ident roachpb.StoreIdent) error {
	if (s.Ident != roachpb.StoreIdent{}) {
		return errors.Errorf("store %s is already bootstrapped", s)
	}
	ctx := s.AnnotateCtx(context.Background())
	if err := checkEngineEmpty(ctx, s.engine); err != nil {
		return errors.Wrap(err, "cannot verify empty engine for bootstrap")
	}
	s.Ident = ident
	if err := engine.MVCCPutProto(
		ctx,
		s.engine,
		nil,
		keys.StoreIdentKey(),
		hlc.ZeroTimestamp,
		nil,
		&s.Ident,
	); err != nil {
		return err
	}

	s.NotifyBootstrapped()
	return nil
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
		return errors.Errorf("engine belongs to store %s, contains %s", ident, keyVals)
	}
	return nil
}

// NotifyBootstrapped tells the store that it was bootstrapped and allows idle
// replicas to campaign immediately. This primarily affects tests.
func (s *Store) NotifyBootstrapped() {
	s.idleReplicaElectionTime.Lock()
	s.idleReplicaElectionTime.at = s.Clock().PhysicalTime()
	s.idleReplicaElectionTime.Unlock()
}

// GetReplica fetches a replica by Range ID. Returns an error if no replica is found.
func (s *Store) GetReplica(rangeID roachpb.RangeID) (*Replica, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getReplicaLocked(rangeID)
}

// getReplicaLocked fetches a replica by RangeID. The store's lock must be held.
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
			kr := item.(KeyRange)
			if !kr.Desc().StartKey.Less(endKey) {
				// This properly checks if this range contains any keys in the supplied span.
				return false
			}

			switch rep := item.(type) {
			case *Replica:
				return iterator(rep)
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
	now := s.cfg.Clock.Now()
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

	updatedMS, err := writeInitialState(ctx, batch, *ms, *desc, raftpb.HardState{}, &roachpb.Lease{})
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
func (s *Store) Clock() *hlc.Clock { return s.cfg.Clock }

// Engine accessor.
func (s *Store) Engine() engine.Engine { return s.engine }

// DB accessor.
func (s *Store) DB() *client.DB { return s.cfg.DB }

// Gossip accessor.
func (s *Store) Gossip() *gossip.Gossip { return s.cfg.Gossip }

// Stopper accessor.
func (s *Store) Stopper() *stop.Stopper { return s.stopper }

// Tracer accessor.
func (s *Store) Tracer() opentracing.Tracer { return s.cfg.AmbientCtx.Tracer }

// TestingKnobs accessor.
func (s *Store) TestingKnobs() *StoreTestingKnobs { return &s.cfg.TestingKnobs }

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
		log.Fatalf(ctx, "unable to find RHS replica: %s", err)
	}
	if err := rightRng.init(&split.RightDesc, r.store.Clock(), 0); err != nil {
		log.Fatal(ctx, err)
	}

	// Copy the timestamp cache into the RHS range.
	r.mu.Lock()
	rightRng.mu.Lock()
	// TODO(andrei): We should truncate the entries in both LHS and RHS' timestamp
	// caches to the respective spans of these new ranges.
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

	now := r.store.Clock().Now()

	// While performing the split, zone config changes or a newly created table
	// might require the range to be split again. Enqueue both the left and right
	// ranges to speed up such splits. See #10160.
	r.store.splitQueue.MaybeAdd(r, now)
	r.store.splitQueue.MaybeAdd(rightRng, now)

	// If the range was not properly replicated before the split, the replicate
	// queue may not have picked it up (due to the need for a split). Enqueue
	// both the left and right ranges to speed up a potentially necessary
	// replication. See #7022 and #7800.
	r.store.replicateQueue.MaybeAdd(r, now)
	r.store.replicateQueue.MaybeAdd(rightRng, now)

	if len(split.RightDesc.Replicas) == 1 {
		// TODO(peter): In single-node clusters, we enqueue the right-hand side of
		// the split (the new range) for Raft processing so that the corresponding
		// Raft group is created. This shouldn't be necessary for correctness, but
		// some tests rely on this (e.g. server.TestStatusSummaries).
		r.store.enqueueRaftUpdateCheck(rightRng.RangeID)
	}
}

// SplitRange shortens the original range to accommodate the new range. The new
// range is added to the ranges map and the replicasByKey btree. origRng.raftMu
// and newRng.raftMu must be held.
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
		// equivalence with newRng. See Store.splitTriggerPostApply().
		if exRng != newRng {
			ctx := s.AnnotateCtx(context.TODO())
			log.Fatalf(ctx, "found unexpected uninitialized replica: %s vs %s", exRng, newRng)
		}
		delete(s.mu.uninitReplicas, newDesc.RangeID)
		delete(s.mu.replicas, newDesc.RangeID)
		delete(s.mu.replicaQueues, newDesc.RangeID)
	}

	// Replace the end key of the original range with the start key of
	// the new range. Reinsert the range since the btree is keyed by range end keys.
	if kr := s.mu.replicasByKey.Delete(origRng); kr != origRng {
		return errors.Errorf("replicasByKey unexpectedly contains %v instead of replica %s", kr, origRng)
	}

	copyDesc := *origDesc
	copyDesc.EndKey = append([]byte(nil), newDesc.StartKey...)
	origRng.setDescWithoutProcessUpdate(&copyDesc)

	if kr := s.mu.replicasByKey.ReplaceOrInsert(origRng); kr != nil {
		return errors.Errorf("replicasByKey unexpectedly contains %s when inserting replica %s", kr, origRng)
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
// The subsumed range's raftMu is assumed held.
func (s *Store) MergeRange(
	ctx context.Context,
	subsumingRng *Replica,
	updatedEndKey roachpb.RKey,
	subsumedRangeID roachpb.RangeID,
) error {
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

	if err := s.maybeMergeTimestampCaches(ctx, subsumingRng, subsumedRng); err != nil {
		return err
	}

	// Remove and destroy the subsumed range. Note that we were called
	// (indirectly) from raft processing so we must call removeReplicaImpl
	// directly to avoid deadlocking on Replica.raftMu.
	if err := s.removeReplicaImpl(subsumedRng, *subsumedDesc, false); err != nil {
		return errors.Errorf("cannot remove range %s", err)
	}

	// Update the end key of the subsuming range.
	copy := *subsumingDesc
	copy.EndKey = updatedEndKey
	return subsumingRng.setDesc(&copy)
}

// If the subsuming replica has the range lease, we update its timestamp cache
// with the entries from the subsumed. Otherwise, then the timestamp cache
// doesn't matter (in fact it should be empty, to save memory).
func (s *Store) maybeMergeTimestampCaches(
	ctx context.Context, subsumingRep *Replica, subsumedRep *Replica,
) error {
	subsumingRep.mu.Lock()
	defer subsumingRep.mu.Unlock()
	subsumingLease := subsumingRep.mu.state.Lease

	subsumedRep.mu.Lock()
	defer subsumedRep.mu.Unlock()
	subsumedLease := subsumedRep.mu.state.Lease

	// Merge support is currently incomplete and incorrect. In particular, the
	// lease holders must be colocated and the subsumed range appropriately
	// quiesced. See also #2433.
	if subsumedLease.Covers(s.Clock().Now()) &&
		subsumingLease.Replica.StoreID != subsumedLease.Replica.StoreID {
		log.Fatalf(ctx, "cannot merge ranges with non-colocated leases. "+
			"Subsuming lease: %s. Subsumed lease: %s.", subsumingLease, subsumedLease)
	}

	if subsumingLease.Covers(s.Clock().Now()) &&
		subsumingLease.OwnedBy(s.StoreID()) {
		subsumedRep.mu.tsCache.MergeInto(subsumingRep.mu.tsCache, false /* clear */)
	}
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
		ctx := s.AnnotateCtx(context.TODO())
		log.Fatalf(ctx, "range=%d: placeholder not found", rngID)
	default:
		ctx := s.AnnotateCtx(context.TODO())
		log.Fatalf(ctx, "range=%d: expected placeholder, got %T", rngID, exRng)
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
func (s *Store) RemoveReplica(rep *Replica, origDesc roachpb.RangeDescriptor, destroy bool) error {
	rep.raftMu.Lock()
	defer rep.raftMu.Unlock()
	return s.removeReplicaImpl(rep, origDesc, destroy)
}

// removeReplicaImpl is the implementation of RemoveReplica, which is sometimes
// called directly when the necessary lock is already held. It requires that
// Replica.raftMu is held and that s.mu is not held.
func (s *Store) removeReplicaImpl(
	rep *Replica, origDesc roachpb.RangeDescriptor, destroyData bool,
) error {
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
	if kr := s.getOverlappingKeyRangeLocked(desc); kr != rep {
		// This is a fatal error because uninitialized replicas shouldn't make it
		// this far. This method will need some changes when we introduce GC of
		// uninitialized replicas.
		s.mu.Unlock()
		panic(fmt.Sprintf("replica %s unexpectedly overlapped by %v", rep, kr))
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

	rep.mu.Lock()
	// Clear the pending command queue.
	if len(rep.mu.proposals) > 0 {
		resp := proposalResult{
			Reply: &roachpb.BatchResponse{},
			Err:   roachpb.NewError(roachpb.NewRangeNotFoundError(rep.RangeID)),
		}
		for _, p := range rep.mu.proposals {
			p.done <- resp
		}
	}
	// Clear the map.
	rep.mu.proposals = map[storagebase.CmdIDKey]*ProposalData{}
	rep.mu.internalRaftGroup = nil
	rep.mu.destroyed = roachpb.NewRangeNotFoundError(rep.RangeID)
	rep.mu.Unlock()

	if destroyData {
		if err := rep.destroyDataRaftMuLocked(); err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.mu.replicas, rep.RangeID)
	delete(s.mu.replicaPlaceholders, rep.RangeID)
	delete(s.mu.replicaQueues, rep.RangeID)
	delete(s.mu.uninitReplicas, rep.RangeID)
	if kr := s.mu.replicasByKey.Delete(rep); kr != rep {
		// We already checked that our replica was present in replicasByKey
		// above. Nothing should have been able to change that.
		panic(fmt.Sprintf("replica %s unexpectedly overlapped by %v", rep, kr))
	}
	s.scanner.RemoveReplica(rep)
	s.consistencyScanner.RemoveReplica(rep)
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

// deadReplicas returns a list of all the corrupt replicas on the store.
func (s *Store) deadReplicas() roachpb.StoreDeadReplicas {
	// We can't use a storeReplicaVisitor here as it skips destroyed replicas.
	// Similar to in the storeReplicaVisitor, make a copy of the current
	// replicas to iterate over so we don't have to hold the store lock during
	// processing.
	// TODO(bram): does this need to visit all the stores? Could we just use the
	// store pool to locate any dead replicas on this store directly?
	s.mu.Lock()
	replicas := make([]*Replica, 0, len(s.mu.replicas))
	for _, repl := range s.mu.replicas {
		replicas = append(replicas, repl)
	}
	s.mu.Unlock()

	var deadReplicas []roachpb.ReplicaIdent
	for _, r := range replicas {
		r.mu.Lock()
		corrupted := r.mu.corrupted
		desc := r.mu.state.Desc
		r.mu.Unlock()
		replicaDesc, ok := desc.GetReplicaDescriptor(s.Ident.StoreID)
		if ok && corrupted {
			deadReplicas = append(deadReplicas, roachpb.ReplicaIdent{
				RangeID: desc.RangeID,
				Replica: replicaDesc,
			})
		}
	}
	return roachpb.StoreDeadReplicas{
		StoreID:  s.Ident.StoreID,
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

	if err := ba.SetActiveTimestamp(s.Clock().Now); err != nil {
		return nil, roachpb.NewError(err)
	}

	if s.cfg.TestingKnobs.ClockBeforeSend != nil {
		s.cfg.TestingKnobs.ClockBeforeSend(s.cfg.Clock, ba)
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
	now := s.cfg.Clock.Update(ba.Timestamp)

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
				s.cfg.Clock.Update(br.Txn.Timestamp)
			}
		} else {
			if pErr == nil {
				// Update our clock with the outgoing response timestamp.
				s.cfg.Clock.Update(br.Timestamp)
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
	var rng *Replica

	// Add the command to the range for execution; exit retry loop on success.
	s.mu.Lock()
	retryOpts := s.cfg.RangeRetryOptions
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
func (s *Store) maybeUpdateTransaction(
	txn *roachpb.Transaction, now hlc.Timestamp,
) (*roachpb.Transaction, *roachpb.Error) {
	ctx, span := s.AnnotateCtxWithSpan(context.Background(), "maybe-update-txn")
	defer span.Finish()
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
	if err := s.db.Run(ctx, b); err != nil {
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
	header *SnapshotRequest_Header, stream MultiRaft_RaftSnapshotServer,
) error {
	s.metrics.raftRcvdMessages[raftpb.MsgSnap].Inc(1)

	var capacity *roachpb.StoreCapacity

	sendSnapError := func(err error) error {
		return stream.Send(&SnapshotResponse{
			Status:        SnapshotResponse_ERROR,
			Message:       err.Error(),
			StoreCapacity: capacity,
		})
	}

	tmpCap, err := s.Capacity()
	if err != nil {
		return sendSnapError(err)
	}
	capacity = &tmpCap

	ctx := s.AnnotateCtx(stream.Context())

	if header.CanDecline {
		// Check the bookie to see if we can apply the snapshot.
		resp := s.Reserve(ctx, ReservationRequest{
			StoreRequestHeader: StoreRequestHeader{
				NodeID:  s.nodeDesc.NodeID,
				StoreID: s.StoreID(),
			},
			RangeSize: header.RangeSize,
			RangeID:   header.RangeDescriptor.RangeID,
		})
		if !resp.Reserved {
			return stream.Send(&SnapshotResponse{
				Status:        SnapshotResponse_DECLINED,
				StoreCapacity: capacity,
			})
		}
	}

	// Check to see if the snapshot can be applied but don't attempt to add
	// a placeholder here, because we're not holding the replica's raftMu.
	// We'll perform this check again later after receiving the rest of the
	// snapshot data - this is purely an optimization to prevent downloading
	// a snapshot that we know we won't be able to apply.
	_, err = s.canApplySnapshot(&header.RangeDescriptor)
	if err != nil {
		return sendSnapError(
			errors.Wrapf(err, "%s,r%d: cannot apply snapshot", s, header.RangeDescriptor.RangeID),
		)
	}

	if err := stream.Send(&SnapshotResponse{Status: SnapshotResponse_ACCEPTED, StoreCapacity: capacity}); err != nil {
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
			return stream.Send(&SnapshotResponse{Status: SnapshotResponse_APPLIED, StoreCapacity: capacity})
		}
	}
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
			panic("coalesced heartbeats must have rangeID == 0")
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

	if respStream == nil {
		return s.processRaftRequest(ctx, req, IncomingSnapshot{})
	}

	s.mu.Lock()
	q := s.mu.replicaQueues[req.RangeID]
	if len(q) >= replicaRequestQueueSize {
		s.mu.Unlock()
		// TODO(peter): Return an error indicating the request was dropped. Note
		// that dropping the request is safe. Raft will retry.
		s.metrics.RaftRcvdMsgDropped.Inc(1)
		return nil
	}
	s.mu.replicaQueues[req.RangeID] = append(q, raftRequestInfo{
		req:        req,
		respStream: respStream,
	})
	s.mu.Unlock()

	s.scheduler.EnqueueRaftRequest(req.RangeID)
	return nil
}

func (s *Store) processRaftRequest(
	ctx context.Context, req *RaftMessageRequest, inSnap IncomingSnapshot,
) (pErr *roachpb.Error) {
	// Lazily create the replica.
	r, _, err := s.getOrCreateReplica(
		req.RangeID, req.ToReplica.ReplicaID, &req.FromReplica)
	if err != nil {
		return roachpb.NewError(err)
	}
	defer r.raftMu.Unlock()
	r.setLastReplicaDescriptors(req)

	if req.Quiesce {
		if req.Message.Type != raftpb.MsgHeartbeat {
			panic(fmt.Sprintf("unexpected quiesce: %+v", req))
		}
		status := r.RaftStatus()
		if status != nil && status.Term == req.Message.Term && status.Commit == req.Message.Commit {
			if r.quiesce() {
				return
			}
		}
		if log.V(4) {
			log.Infof(ctx, "not quiescing: local raft status is %+v, incoming quiesce message is %+v", status, req.Message)
		}
	}

	// Check to see if a snapshot can be applied. Snapshots can always be applied
	// to initialized replicas. Note that if we add a placeholder we need to
	// already be holding Replica.raftMu in order to prevent concurrent
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
					r.store.cfg,
					&raftLogger{ctx: ctx},
				), nil)
			if err != nil {
				return roachpb.NewError(err)
			}
			// We have a Raft group; feed it the message.
			if err := raftGroup.Step(req.Message); err != nil {
				return roachpb.NewError(errors.Wrap(err, "unable to process preemptive snapshot"))
			}
			// In the normal case, the group should ask us to apply a snapshot.
			// If it doesn't, our snapshot was probably stale. In that case we
			// still go ahead and apply a noop because we want that case to be
			// counted by stats as a successful application.
			var ready raft.Ready
			if raftGroup.HasReady() {
				ready = raftGroup.Ready()
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
			log.Infof(ctx, "refusing incoming Raft message %s for range %d from %+v to %+v",
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
		if err := r.handleRaftReadyRaftMuLocked(inSnap); err != nil {
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
	ctx = s.AnnotateCtx(ctx)
	switch val := resp.Union.GetValue().(type) {
	case *roachpb.Error:
		switch val.GetDetail().(type) {
		case *roachpb.ReplicaTooOldError:
			repl, err := s.GetReplica(resp.RangeID)
			if err != nil {
				return // not unexpected
			}
			added, err := s.replicaGCQueue.Add(
				repl, replicaGCPriorityRemoved,
			)
			if err != nil {
				log.Errorf(ctx, "%s: unable to add to replica GC queue: %s", repl, err)
			} else if added {
				log.Infof(ctx, "%s: added to replica GC queue (peer suggestion)", repl)
			}
		default:
			log.Warningf(ctx, "got error from range %d, replica %s: %s",
				resp.RangeID, resp.FromReplica, val)
		}

	default:
		log.Infof(ctx, "got unknown raft response type %T from replica %s: %s", val, resp.FromReplica, val)
	}
}

// OutgoingSnapshotStream is the minimal interface on a GRPC stream required
// to send a snapshot over the network.
type OutgoingSnapshotStream interface {
	Send(*SnapshotRequest) error
	Recv() (*SnapshotResponse, error)
}

// SnapshotStorePool narrows StorePool to make sendSnapshot easier to test.
type SnapshotStorePool interface {
	throttle(reason throttleReason, toStoreID roachpb.StoreID)
	updateRemoteCapacityEstimate(toStoreID roachpb.StoreID, capacity roachpb.StoreCapacity)
}

// sendSnapshot sends an outgoing snapshot via a pre-opened GRPC stream.
func sendSnapshot(
	ctx context.Context,
	stream OutgoingSnapshotStream,
	storePool SnapshotStorePool,
	header SnapshotRequest_Header,
	snap *OutgoingSnapshot,
	newBatch func() engine.Batch,
) error {
	storeID := header.RaftMessageRequest.ToReplica.StoreID
	if err := stream.Send(&SnapshotRequest{Header: &header}); err != nil {
		return err
	}
	// Wait until we get a response from the server.
	resp, err := stream.Recv()
	if err != nil {
		storePool.throttle(throttleFailed, storeID)
		return err
	}
	if resp.StoreCapacity != nil {
		storePool.updateRemoteCapacityEstimate(storeID, *resp.StoreCapacity)
	}
	switch resp.Status {
	case SnapshotResponse_DECLINED:
		if header.CanDecline {
			storePool.throttle(throttleDeclined, storeID)
			return errors.Errorf("range=%s: remote declined snapshot: %s",
				header.RangeDescriptor.RangeID, resp.Message)
		}
		storePool.throttle(throttleFailed, storeID)
		return errors.Errorf("range=%s: programming error: remote declined required snapshot: %s",
			header.RangeDescriptor.RangeID, resp.Message)
	case SnapshotResponse_ERROR:
		storePool.throttle(throttleFailed, storeID)
		return errors.Errorf("range=%s: remote couldn't accept snapshot with error: %s",
			header.RangeDescriptor.RangeID, resp.Message)
	case SnapshotResponse_ACCEPTED:
	// This is the response we're expecting. Continue with snapshot sending.
	default:
		storePool.throttle(throttleFailed, storeID)
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

	truncState, err := loadTruncatedState(ctx, snap.EngineSnap, rangeID)
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

	if err := iterateEntries(ctx, snap.EngineSnap, rangeID, firstIndex, endIndex, scanFunc); err != nil {
		return err
	}
	if err := stream.Send(&SnapshotRequest{LogEntries: logEntries, Final: true}); err != nil {
		return err
	}
	log.Infof(ctx, "streamed snapshot: kv pairs: %d, log entries: %d",
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

func sendBatch(stream OutgoingSnapshotStream, batch engine.Batch) error {
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
					ctx := s.AnnotateCtx(context.TODO())
					log.Info(ctx, errors.Wrap(err, "error sending error"))
				}
			}
		}
	}
}

func (s *Store) processReady(rangeID roachpb.RangeID) {
	start := timeutil.Now()

	s.mu.Lock()
	r, ok := s.mu.replicas[rangeID]
	s.mu.Unlock()

	if ok {
		if err := r.handleRaftReady(IncomingSnapshot{}); err != nil {
			panic(err) // TODO(bdarnell)
		}
		elapsed := timeutil.Since(start)
		s.metrics.RaftWorkingDurationNanos.Inc(elapsed.Nanoseconds())
		// If Raft processing took longer than 10x the raft tick interval something
		// bad is going on. Such long processing time means we'll have starved
		// local replicas of ticks and remote replicas will likely start
		// campaigning.
		var warnDuration = 10 * s.cfg.RaftTickInterval
		if elapsed >= warnDuration {
			ctx := r.AnnotateCtx(context.TODO())
			log.Warningf(ctx, "handle raft ready: %.1fs", elapsed.Seconds())
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
}

func (s *Store) processTick(rangeID roachpb.RangeID) bool {
	start := timeutil.Now()

	s.mu.Lock()
	r, ok := s.mu.replicas[rangeID]
	s.mu.Unlock()

	var exists bool
	if ok {
		var err error
		if exists, err = r.tick(); err != nil {
			ctx := s.AnnotateCtx(context.TODO())
			log.Error(ctx, err)
		}
		s.metrics.RaftTickingDurationNanos.Inc(timeutil.Since(start).Nanoseconds())
	}
	return exists // ready
}

func (s *Store) processRaft() {
	if s.cfg.TestingKnobs.DisableProcessRaft {
		return
	}

	s.scheduler.Start(s.stopper)
	s.stopper.RunWorker(func() {
		// Wait for the scheduler worker goroutines to finish. Necessary because a
		// worker might be generating a snapshot.
		s.scheduler.Wait()

		// Abandon any unsent snapshots (releasing the associated RocksDB
		// resources).
		newStoreReplicaVisitor(s).Visit(func(r *Replica) bool {
			ctx := r.AnnotateCtx(context.TODO())
			r.maybeAbandonSnapshot(ctx)
			return true
		})
	})

	s.raftTickLoop()
	s.startCoalescedHeartbeatsLoop()
}

func (s *Store) raftTickLoop() {
	s.stopper.RunWorker(func() {
		ticker := time.NewTicker(s.cfg.RaftTickInterval)
		defer func() {
			ticker.Stop()
			s.cfg.Transport.Stop(s.StoreID())
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

// Since coalesced heartbeats adds latency to heartbeat messages, it is
// beneficial to have it run on a faster cycle than once per tick, so that
// the delay does not impact latency-sensitive features such as quiescence.
func (s *Store) startCoalescedHeartbeatsLoop() {
	s.stopper.RunWorker(func() {
		ticker := time.NewTicker(s.cfg.CoalescedHeartbeatsInterval)
		defer func() {
			ticker.Stop()
		}()

		for {
			select {
			case <-ticker.C:
				s.sendQueuedHeartbeats()
			case <-s.stopper.ShouldStop():
				return
			}
		}
	})
}

// sendQueuedHeartbeatsToNode requires that the s.coalescedMu lock is held. It
// returns the number of heartbeats that were sent.
func (s *Store) sendQueuedHeartbeatsToNode(beats, resps []RaftHeartbeat, to roachpb.StoreIdent) int {
	var msgType raftpb.MessageType

	if len(beats) == 0 && len(resps) == 0 {
		return 0
	} else if len(resps) == 0 {
		msgType = raftpb.MsgHeartbeat
	} else if len(beats) == 0 {
		msgType = raftpb.MsgHeartbeatResp
	} else {
		panic("cannot coalesce both heartbeats and responses")
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
		ctx := s.AnnotateCtx(context.TODO())
		log.Infof(ctx, "sending raft request (coalesced) %+v", chReq)
	}

	if !s.cfg.Transport.SendAsync(chReq) {
		s.mu.Lock()
		for _, beat := range beats {
			replica := s.mu.replicas[beat.RangeID]
			replica.addUnreachableRemoteReplica(beat.ToReplicaID)
		}
		for _, resp := range resps {
			replica := s.mu.replicas[resp.RangeID]
			replica.addUnreachableRemoteReplica(resp.ToReplicaID)
		}
		s.mu.Unlock()
		return 0
	}
	return len(beats) + len(resps)
}

func (s *Store) sendQueuedHeartbeats() {
	s.coalescedMu.Lock()
	heartbeats := s.coalescedMu.heartbeats
	heartbeatResponses := s.coalescedMu.heartbeatResponses
	s.coalescedMu.heartbeats = map[roachpb.StoreIdent][]RaftHeartbeat{}
	s.coalescedMu.heartbeatResponses = map[roachpb.StoreIdent][]RaftHeartbeat{}
	s.coalescedMu.Unlock()

	var beatsSent int

	for to, beats := range heartbeats {
		beatsSent += s.sendQueuedHeartbeatsToNode(beats, nil, to)
	}
	for to, resps := range heartbeatResponses {
		beatsSent += s.sendQueuedHeartbeatsToNode(nil, resps, to)
	}
	s.metrics.RaftCoalescedHeartbeatsPending.Update(int64(beatsSent))
}

var errRetry = errors.New("retry: orphaned replica")

// getOrCreateReplica returns a replica for the given RangeID, creating an
// uninitialized replica if necessary. The caller must not hold the store's
// lock. The returned replica has Replica.raftMu locked and it is the caller's
// responsibility to unlock it.
func (s *Store) getOrCreateReplica(
	rangeID roachpb.RangeID, replicaID roachpb.ReplicaID, creatingReplica *roachpb.ReplicaDescriptor,
) (_ *Replica, created bool, _ error) {
	for {
		r, created, err := s.tryGetOrCreateReplica(
			rangeID, replicaID, creatingReplica)
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
	rangeID roachpb.RangeID, replicaID roachpb.ReplicaID, creatingReplica *roachpb.ReplicaDescriptor,
) (_ *Replica, created bool, _ error) {
	// The common case: look up an existing (initialized) replica.
	s.mu.Lock()
	r, ok := s.mu.replicas[rangeID]
	s.mu.Unlock()
	if ok {
		if creatingReplica != nil {
			// Drop messages that come from a node that we believe was once a member of
			// the group but has been removed.
			desc := r.Desc()
			_, found := desc.GetReplicaDescriptorByID(creatingReplica.ReplicaID)
			// It's not a current member of the group. Is it from the past?
			if !found && creatingReplica.ReplicaID < desc.NextReplicaID {
				return nil, false, &roachpb.ReplicaTooOldError{}
			}
		}

		r.raftMu.Lock()
		r.mu.Lock()
		destroyed, corrupted := r.mu.destroyed, r.mu.corrupted
		r.mu.Unlock()
		if destroyed != nil {
			r.raftMu.Unlock()
			if corrupted {
				return nil, false, destroyed
			}
			return nil, false, errRetry
		}
		if err := r.setReplicaID(replicaID); err != nil {
			r.raftMu.Unlock()
			return nil, false, err
		}
		return r, false, nil
	}

	ctx := s.AnnotateCtx(context.TODO())

	// No replica currently exists, so we'll try to create one. Before creating
	// the replica, see if there is a tombstone which would indicate that this is
	// a stale message.
	tombstoneKey := keys.RaftTombstoneKey(rangeID)
	var tombstone roachpb.RaftTombstone
	if ok, err := engine.MVCCGetProto(
		ctx, s.Engine(), tombstoneKey, hlc.ZeroTimestamp, true, nil, &tombstone,
	); err != nil {
		return nil, false, err
	} else if ok {
		if replicaID != 0 && replicaID < tombstone.NextReplicaID {
			return nil, false, &roachpb.RaftGroupDeletedError{}
		}
	}

	// Create a new replica and lock it for raft processing.
	r = newReplica(rangeID, s)
	r.creatingReplica = creatingReplica
	r.raftMu.Lock()

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
		r.raftMu.Unlock()
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
		r.raftMu.Unlock()
		return nil, false, err
	}
	r.mu.Unlock()
	return r, true, nil
}

// canApplySnapshot returns (_, nil) if the snapshot can be applied to
// this store's replica (i.e. the snapshot is not from an older incarnation of
// the replica) and a placeholder can be added to the replicasByKey map (if
// necessary). If a placeholder is required, it is returned as the first value.
func (s *Store) canApplySnapshot(
	rangeDescriptor *roachpb.RangeDescriptor,
) (*ReplicaPlaceholder, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.canApplySnapshotLocked(rangeDescriptor)
}

func (s *Store) canApplySnapshotLocked(
	rangeDescriptor *roachpb.RangeDescriptor,
) (*ReplicaPlaceholder, error) {
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
			ctx := s.AnnotateCtx(context.TODO())
			log.Warning(ctx, errors.Wrapf(
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

	timestamp := s.cfg.Clock.Now()

	newStoreReplicaVisitor(s).Visit(func(rep *Replica) bool {
		desc := rep.Desc()
		zoneConfig, err := cfg.GetZoneConfigForKey(desc.StartKey)
		if err != nil {
			ctx := s.AnnotateCtx(context.TODO())
			log.Error(ctx, err)
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

// updateCommandQueueGauges updates a number of simple statistics for
// the CommandQueues of each replica in this store.
func (s *Store) updateCommandQueueGauges() error {
	var (
		maxCommandQueueSize       int64
		maxCommandQueueWriteCount int64
		maxCommandQueueReadCount  int64
		maxCommandQueueTreeSize   int64
		maxCommandQueueOverlaps   int64
		combinedCommandQueueSize  int64
		combinedCommandWriteCount int64
		combinedCommandReadCount  int64
	)
	newStoreReplicaVisitor(s).Visit(func(rep *Replica) bool {
		rep.cmdQMu.Lock()

		writes := rep.cmdQMu.global.localMetrics.writeCommands
		writes += rep.cmdQMu.local.localMetrics.writeCommands

		reads := rep.cmdQMu.global.localMetrics.readCommands
		reads += rep.cmdQMu.local.localMetrics.readCommands

		treeSize := int64(rep.cmdQMu.global.treeSize())
		treeSize += int64(rep.cmdQMu.local.treeSize())

		maxOverlaps := rep.cmdQMu.global.localMetrics.maxOverlapsSeen
		if locMax := rep.cmdQMu.local.localMetrics.maxOverlapsSeen; locMax > maxOverlaps {
			maxOverlaps = locMax
		}
		rep.cmdQMu.global.localMetrics.maxOverlapsSeen = 0
		rep.cmdQMu.local.localMetrics.maxOverlapsSeen = 0
		rep.cmdQMu.Unlock()

		cqSize := writes + reads
		if cqSize > maxCommandQueueSize {
			maxCommandQueueSize = cqSize
		}
		if writes > maxCommandQueueWriteCount {
			maxCommandQueueWriteCount = writes
		}
		if reads > maxCommandQueueReadCount {
			maxCommandQueueReadCount = reads
		}
		if treeSize > maxCommandQueueTreeSize {
			maxCommandQueueTreeSize = treeSize
		}
		if maxOverlaps > maxCommandQueueOverlaps {
			maxCommandQueueOverlaps = maxOverlaps
		}

		combinedCommandQueueSize += cqSize
		combinedCommandWriteCount += writes
		combinedCommandReadCount += reads
		return true // more
	})

	s.metrics.MaxCommandQueueSize.Update(maxCommandQueueSize)
	s.metrics.MaxCommandQueueWriteCount.Update(maxCommandQueueWriteCount)
	s.metrics.MaxCommandQueueReadCount.Update(maxCommandQueueReadCount)
	s.metrics.MaxCommandQueueTreeSize.Update(maxCommandQueueTreeSize)
	s.metrics.MaxCommandQueueOverlaps.Update(maxCommandQueueOverlaps)
	s.metrics.CombinedCommandQueueSize.Update(combinedCommandQueueSize)
	s.metrics.CombinedCommandWriteCount.Update(combinedCommandWriteCount)
	s.metrics.CombinedCommandReadCount.Update(combinedCommandReadCount)

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

	if err := s.updateCommandQueueGauges(); err != nil {
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
		s.metrics.RdbNumSSTables.Update(int64(sstables.Len()))
		readAmp := sstables.ReadAmplification()
		s.metrics.RdbReadAmplification.Update(int64(readAmp))
		// Log this metric infrequently.
		if tick%100 == 0 {
			ctx := s.AnnotateCtx(context.TODO())
			log.Infof(ctx, "sstables (read amplification = %d):\n%s", readAmp, sstables)
		}
	}
	return nil
}

// ComputeStatsForKeySpan computes the aggregated MVCCStats for all replicas on
// this store which contain any keys in the supplied range.
func (s *Store) ComputeStatsForKeySpan(startKey, endKey roachpb.RKey) (enginepb.MVCCStats, int) {
	var output enginepb.MVCCStats
	var count int

	newStoreReplicaVisitor(s).Visit(func(repl *Replica) bool {
		desc := repl.Desc()
		if bytes.Compare(startKey, desc.EndKey) >= 0 || bytes.Compare(desc.StartKey, endKey) >= 0 {
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
	newStoreReplicaVisitor(s).Visit(func(r *Replica) bool {
		if !r.IsInitialized() {
			return true
		}
		repDesc, err := r.GetReplicaDescriptor()
		if err != nil {
			if _, ok := err.(*roachpb.RangeNotFoundError); ok {
				return true
			}
			ctx := s.AnnotateCtx(context.TODO())
			log.Fatalf(ctx, "unexpected error: %s", err)
		}
		r.mu.Lock()
		if r.mu.state.IsFrozen() == collectFrozen {
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
