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

package storage

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/google/btree"
	"github.com/kr/pretty"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/abortspan"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	// sentinelGossipTTL is time-to-live for the gossip sentinel. The
	// sentinel informs a node whether or not it's connected to the
	// primary gossip network and not just a partition. As such it must
	// expire on a reasonable basis and be continually re-gossiped. The
	// replica which is the lease holder of the first range gossips it.
	sentinelGossipTTL = 2 * time.Minute
	// sentinelGossipInterval is the approximate interval at which the
	// sentinel info is gossiped.
	sentinelGossipInterval = sentinelGossipTTL / 2

	// configGossipTTL is the time-to-live for configuration maps.
	configGossipTTL = 0 // does not expire
	// optimizePutThreshold is the minimum length of a contiguous run
	// of batched puts or conditional puts, after which the constituent
	// put operations will possibly be optimized by determining whether
	// the key space being written is starting out empty.
	optimizePutThreshold = 10

	replicaChangeTxnName = "change-replica"
	splitTxnName         = "split"
	mergeTxnName         = "merge"

	// MaxQuotaReplicaLivenessDuration is the maximum duration that a replica
	// can remain inactive while still being counting against the range's
	// available proposal quota.
	MaxQuotaReplicaLivenessDuration = 10 * time.Second

	defaultReplicaRaftMuWarnThreshold = 500 * time.Millisecond
)

// TODO(irfansharif, peter): What's a good default? Too low and everything comes
// to a grinding halt, too high and we're not really throttling anything
// (we'll still generate snapshots). Should it be adjusted dynamically?
//
// We set the defaultProposalQuota to be less than raftLogMaxSize, in doing so
// we ensure all replicas have sufficiently up to date logs so that when the
// log gets truncated, the followers do not need non-preemptive snapshots.
var defaultProposalQuota = raftLogMaxSize / 4

// This flag controls whether Transaction entries are automatically gc'ed
// upon EndTransaction if they only have local intents (which can be
// resolved synchronously with EndTransaction). Certain tests become
// simpler with this being turned off.
var txnAutoGC = true

var syncRaftLog = settings.RegisterBoolSetting(
	"kv.raft_log.synchronize",
	"set to true to synchronize on Raft log writes to persistent storage",
	true,
)

// MaxCommandSizeFloor is the minimum allowed value for the MaxCommandSize
// cluster setting.
const MaxCommandSizeFloor = 4 << 20 // 4MB

// MaxCommandSize wraps "kv.raft.command.max_size".
var MaxCommandSize = settings.RegisterValidatedByteSizeSetting(
	"kv.raft.command.max_size",
	"maximum size of a raft command",
	64<<20,
	func(size int64) error {
		if size < MaxCommandSizeFloor {
			return fmt.Errorf("max_size must be greater than %s", humanizeutil.IBytes(MaxCommandSizeFloor))
		}
		return nil
	},
)

// raftInitialLog{Index,Term} are the starting points for the raft log. We
// bootstrap the raft membership by synthesizing a snapshot as if there were
// some discarded prefix to the log, so we must begin the log at an arbitrary
// index greater than 1.
const (
	raftInitialLogIndex = 10
	raftInitialLogTerm  = 5
)

type proposalRetryReason int

const (
	proposalNoRetry proposalRetryReason = iota
	// proposalIllegalLeaseIndex indicates the proposal failed to apply at
	// a Lease index it was not legal for. The command should be retried.
	proposalIllegalLeaseIndex
	// proposalAmbiguousShouldBeReevaluated indicates that it's ambiguous whether
	// the command was committed (and possibly even applied) or not. The command
	// should be retried. However, the original proposal may have succeeded, so if
	// the retry does not succeed, care must be taken to correctly inform the
	// caller via an AmbiguousResultError.
	proposalAmbiguousShouldBeReevaluated
	// proposalErrorReproposing indicates that re-proposal
	// failed. Because the original proposal may have succeeded, an
	// AmbiguousResultError must be returned. The command should not be
	// retried.
	proposalErrorReproposing
	// proposalRangeNoLongerExists indicates the proposal was for a
	// range that no longer exists. Because the original proposal may
	// have succeeded, an AmbiguousResultError must be returned. The
	// command should not be retried.
	proposalRangeNoLongerExists
)

// proposalResult indicates the result of a proposal. Exactly one of
// Reply, Err and ProposalRetry is set, and it represents the result of
// the proposal.
type proposalResult struct {
	Reply         *roachpb.BatchResponse
	Err           *roachpb.Error
	ProposalRetry proposalRetryReason
	Intents       []result.IntentsWithArg
	EndTxns       []result.EndTxnIntents
}

// ReplicaChecksum contains progress on a replica checksum computation.
type ReplicaChecksum struct {
	CollectChecksumResponse
	// started is true if the checksum computation has started.
	started bool
	// If gcTimestamp is nonzero, GC this checksum after gcTimestamp. gcTimestamp
	// is zero if and only if the checksum computation is in progress.
	gcTimestamp time.Time
	// This channel is closed after the checksum is computed, and is used
	// as a notification.
	notify chan struct{}
}

type atomicDescString struct {
	strPtr unsafe.Pointer
}

// store atomically updates d.strPtr with the string representation of desc.
func (d *atomicDescString) store(replicaID roachpb.ReplicaID, desc *roachpb.RangeDescriptor) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%d/", desc.RangeID)
	if replicaID == 0 {
		fmt.Fprintf(&buf, "?:")
	} else {
		fmt.Fprintf(&buf, "%d:", replicaID)
	}

	if !desc.IsInitialized() {
		buf.WriteString("{-}")
	} else {
		const maxRangeChars = 30
		rngStr := keys.PrettyPrintRange(roachpb.Key(desc.StartKey), roachpb.Key(desc.EndKey), maxRangeChars)
		buf.WriteString(rngStr)
	}

	str := buf.String()
	atomic.StorePointer(&d.strPtr, unsafe.Pointer(&str))
}

// String returns the string representation of the range; since we are not
// using a lock, the copy might be inconsistent.
func (d *atomicDescString) String() string {
	return *(*string)(atomic.LoadPointer(&d.strPtr))
}

// DestroyReason indicates if a replica is alive, destroyed, corrupted or pending destruction.
type DestroyReason int

const (
	// The replica is alive.
	destroyReasonAlive DestroyReason = iota
	// The replica has been corrupted.
	destroyReasonCorrupted
	// The replica has been marked for GC, but hasn't been GCed yet.
	destroyReasonRemovalPending
	// The replica has been GCed.
	destroyReasonRemoved
)

type destroyStatus struct {
	reason DestroyReason
	err    error
}

// IsAlive returns true when a replica is alive.
func (s destroyStatus) IsAlive() bool {
	return s.reason == destroyReasonAlive
}

// RemovedOrCorrupt returns true if a replica has either been removed or is corrupted.
func (s destroyStatus) RemovedOrCorrupt() bool {
	return (s.reason == destroyReasonCorrupted) || (s.reason == destroyReasonRemoved)
}

func (s *destroyStatus) Set(err error, reason DestroyReason) {
	s.err = err
	s.reason = reason
}

func (s *destroyStatus) Reset() {
	s.Set(nil, destroyReasonAlive)
}

// A Replica is a contiguous keyspace with writes managed via an
// instance of the Raft consensus algorithm. Many ranges may exist
// in a store and they are unlikely to be contiguous. Ranges are
// independent units and are responsible for maintaining their own
// integrity by replacing failed replicas, splitting and merging
// as appropriate.
type Replica struct {
	log.AmbientContext

	// TODO(tschottdorf): Duplicates r.mu.state.desc.RangeID; revisit that.
	RangeID roachpb.RangeID // Should only be set by the constructor.

	store        *Store
	abortSpan    *abortspan.AbortSpan // Avoids anomalous reads after abort
	txnWaitQueue *txnwait.Queue       // Queues push txn attempts by txn ID

	// leaseholderStats tracks all incoming BatchRequests to the replica and which
	// localities they come from in order to aid in lease rebalancing decisions.
	leaseholderStats *replicaStats
	// writeStats tracks the number of keys written by applied raft commands
	// in order to aid in replica rebalancing decisions.
	writeStats *replicaStats

	// creatingReplica is set when a replica is created as uninitialized
	// via a raft message.
	creatingReplica *roachpb.ReplicaDescriptor

	// Held in read mode during read-only commands. Held in exclusive mode to
	// prevent read-only commands from executing. Acquired before the embedded
	// RWMutex.
	readOnlyCmdMu syncutil.RWMutex

	// rangeStr is a string representation of a RangeDescriptor that can be
	// atomically read and updated without needing to acquire the replica.mu lock.
	// All updates to state.Desc should be duplicated here.
	rangeStr atomicDescString

	// raftMu protects Raft processing the replica.
	//
	// Locking notes: Replica.raftMu < Replica.mu
	//
	// TODO(peter): evaluate runtime overhead of the timed mutex.
	raftMu struct {
		syncutil.Mutex

		// Note that there are two StateLoaders, in raftMu and mu,
		// depending on which lock is being held.
		stateLoader stateloader.StateLoader
		// on-disk storage for sideloaded SSTables. nil when there's no ReplicaID.
		sideloaded sideloadStorage
	}

	// Contains the lease history when enabled.
	leaseHistory *leaseHistory

	cmdQMu struct {
		// Protects all fields in the cmdQMu struct.
		//
		// Locking notes: Replica.mu < Replica.cmdQMu
		syncutil.Mutex
		// Enforces at most one command is running per key(s) within each span
		// scope. The globally-scoped component tracks user writes (i.e. all
		// keys for which keys.Addr is the identity), the locally-scoped component
		// the rest (e.g. RangeDescriptor, transaction record, Lease, ...).
		// Commands with different accesses but the same scope are stored in the
		// same component.
		queues [spanset.NumSpanScope]*CommandQueue
	}

	mu struct {
		// Protects all fields in the mu struct.
		syncutil.RWMutex
		// The destroyed status of a replica indicating if it's alive, corrupt,
		// scheduled for destruction or has been GCed.
		destroyStatus
		// Is the range quiescent? Quiescent ranges are not Tick()'d and unquiesce
		// whenever a Raft operation is performed.
		quiescent bool
		// The state of the Raft state machine.
		state storagebase.ReplicaState
		// Counter used for assigning lease indexes for proposals.
		lastAssignedLeaseIndex uint64
		// Last index/term persisted to the raft log (not necessarily
		// committed). Note that lastTerm may be 0 (and thus invalid) even when
		// lastIndex is known, in which case the term will have to be retrieved
		// from the Raft log entry. Use the invalidLastTerm constant for this
		// case.
		lastIndex, lastTerm uint64
		// The raft log index of a pending preemptive snapshot. Used to prohibit
		// raft log truncation while a preemptive snapshot is in flight. A value of
		// 0 indicates that there is no pending snapshot.
		pendingSnapshotIndex uint64
		// raftLogSize is the approximate size in bytes of the persisted raft log.
		// On server restart, this value is assumed to be zero to avoid costly scans
		// of the raft log. This will be correct when all log entries predating this
		// process have been truncated.
		raftLogSize int64
		// raftLogLastCheckSize is the value of raftLogSize the last time the Raft
		// log was checked for truncation or at the time of the last Raft log
		// truncation.
		raftLogLastCheckSize int64
		// pendingLeaseRequest is used to coalesce RequestLease requests.
		pendingLeaseRequest pendingLeaseRequest
		// minLeaseProposedTS is the minimum acceptable lease.ProposedTS; only
		// leases proposed after this timestamp can be used for proposing commands.
		// This is used to protect against several hazards:
		// - leases held (or even proposed) before a restart cannot be used after a
		// restart. This is because:
		// 		a) the command queue is wiped during the restart; there might be
		// 		writes in flight that are not reflected in the new command queue. So,
		// 		we need to synchronize all new reads with those old in-flight writes.
		// 		Forcing acquisition of a new lease essentially flushes all the
		// 		previous raft commands.
		// 		b) a lease transfer might have been in progress at the time of the
		// 		restart. Using the existing lease after the restart would break the
		// 		transfer proposer's promise to not use the existing lease.
		// - a lease cannot be used after a transfer is initiated. Moreover, even
		// lease extension that were in flight at the time of the transfer cannot be
		// used, if they eventually apply.
		minLeaseProposedTS hlc.Timestamp
		// Max bytes before split.
		maxBytes int64
		// proposals stores the Raft in-flight commands which
		// originated at this Replica, i.e. all commands for which
		// propose has been called, but which have not yet
		// applied.
		//
		// The *ProposalData in the map are "owned" by it. Elements from the
		// map must only be referenced while Replica.mu is held, except if the
		// element is removed from the map first. The notable exception is the
		// contained RaftCommand, which we treat as immutable.
		proposals         map[storagebase.CmdIDKey]*ProposalData
		internalRaftGroup *raft.RawNode
		// The ID of the replica within the Raft group. May be 0 if the replica has
		// been created from a preemptive snapshot (i.e. before being added to the
		// Raft group). The replica ID will be non-zero whenever the replica is
		// part of a Raft group.
		replicaID roachpb.ReplicaID
		// The minimum allowed ID for this replica. Initialized from
		// RaftTombstone.NextReplicaID.
		minReplicaID roachpb.ReplicaID
		// The ID of the leader replica within the Raft group. Used to determine
		// when the leadership changes.
		leaderID roachpb.ReplicaID
		// The most recently added replica for the range and when it was added.
		// Used to determine whether a replica is new enough that we shouldn't
		// penalize it for being slightly behind. These field gets cleared out once
		// we know that the replica has caught up.
		lastReplicaAdded     roachpb.ReplicaID
		lastReplicaAddedTime time.Time

		// The most recently updated time for each follower of this range.
		lastUpdateTimes map[roachpb.ReplicaID]time.Time

		// The last seen replica descriptors from incoming Raft messages. These are
		// stored so that the replica still knows the replica descriptors for itself
		// and for its message recipients in the circumstances when its RangeDescriptor
		// is out of date.
		//
		// Normally, a replica knows about the other replica descriptors for a
		// range via the RangeDescriptor stored in Replica.mu.state.Desc. But that
		// descriptor is only updated during a Split or ChangeReplicas operation.
		// There are periods during a Replica's lifetime when that information is
		// out of date:
		//
		// 1. When a replica is being newly created as the result of an incoming
		// Raft message for it. This is the common case for ChangeReplicas and an
		// uncommon case for Splits. The leader will be sending the replica
		// messages and the replica needs to be able to respond before it can
		// receive an updated range descriptor (via a snapshot,
		// changeReplicasTrigger, or splitTrigger).
		//
		// 2. If the node containing a replica is partitioned or down while the
		// replicas for the range are updated. When the node comes back up, other
		// replicas may begin communicating with it and it needs to be able to
		// respond. Unlike 1 where there is no range descriptor, in this situation
		// the replica has a range descriptor but it is out of date. Note that a
		// replica being removed from a node and then quickly re-added before the
		// replica has been GC'd will also use the last seen descriptors. In
		// effect, this is another path for which the replica's local range
		// descriptor is out of date.
		//
		// The last seen replica descriptors are updated on receipt of every raft
		// message via Replica.setLastReplicaDescriptors (see
		// Store.HandleRaftRequest). These last seen descriptors are used when
		// the replica's RangeDescriptor contains missing or out of date descriptors
		// for a replica (see Replica.sendRaftMessage).
		//
		// Removing a replica from Store.mu.replicas is not a problem because
		// when a replica is completely removed, it won't be recreated until
		// there is another event that will repopulate the replicas map in the
		// range descriptor. When it is temporarily dropped and recreated, the
		// newly recreated replica will have a complete range descriptor.
		lastToReplica, lastFromReplica roachpb.ReplicaDescriptor

		// submitProposalFn can be set to mock out the propose operation.
		submitProposalFn func(*ProposalData) error
		// Computed checksum at a snapshot UUID.
		checksums map[uuid.UUID]ReplicaChecksum

		// proposalQuota is the quota pool maintained by the lease holder where
		// incoming writes acquire quota from a fixed quota pool before going
		// through. If there is no quota available, the write is throttled
		// until quota is made available to the pool.
		// Acquired quota for a given command is only released when all the
		// replicas have persisted the corresponding entry into their logs.
		proposalQuota *quotaPool

		proposalQuotaBaseIndex uint64

		// For command size based allocations we keep track of the sizes of all
		// in-flight commands.
		commandSizes map[storagebase.CmdIDKey]int

		// Once the leader observes a proposal come 'out of Raft', we consult
		// the 'commandSizes' map to determine the size of the associated
		// command and add it to a queue of quotas we have yet to release back
		// to the quota pool. We only do so when all replicas have persisted
		// the corresponding entry into their logs.
		quotaReleaseQueue []int

		// Counts calls to Replica.tick()
		ticks int

		// Counts Raft messages refused due to queue congestion.
		droppedMessages int

		// Note that there are two replicaStateLoaders, in raftMu and mu,
		// depending on which lock is being held.
		stateLoader stateloader.StateLoader

		// draining specifies whether this replica is draining. Raft leadership
		// transfers due to a lease change will be attempted even if the target does
		// not have all the log entries.
		draining bool
	}

	unreachablesMu struct {
		syncutil.Mutex
		remotes map[roachpb.ReplicaID]struct{}
	}
}

var _ batcheval.EvalContext = &Replica{}

// KeyRange is an interface type for the replicasByKey BTree, to compare
// Replica and ReplicaPlaceholder.
type KeyRange interface {
	Desc() *roachpb.RangeDescriptor
	rangeKeyItem
	btree.Item
	fmt.Stringer
}

var _ KeyRange = &Replica{}

// withRaftGroupLocked calls the supplied function with the (lazily
// initialized) Raft group. The supplied function should return true for the
// unquiesceAndWakeLeader argument if the replica should be unquiesced (and the
// leader awoken). See handleRaftReady for an instance of where this value
// varies. The shouldCampaignOnCreation argument indicates whether a new raft group
// should be campaigned upon creation and is used to eagerly campaign idle
// replicas.
//
// Requires that both Replica.mu and Replica.raftMu are held.
func (r *Replica) withRaftGroupLocked(
	shouldCampaignOnCreation bool, f func(r *raft.RawNode) (unquiesceAndWakeLeader bool, _ error),
) error {
	if r.mu.destroyStatus.RemovedOrCorrupt() {
		// Silently ignore all operations on destroyed replicas. We can't return an
		// error here as all errors returned from this method are considered fatal.
		return nil
	}

	if r.mu.replicaID == 0 {
		// The replica's raft group has not yet been configured (i.e. the replica
		// was created from a preemptive snapshot).
		return nil
	}

	if shouldCampaignOnCreation {
		// Special handling of idle replicas: we campaign their Raft group upon
		// creation if we gossiped our store descriptor more than the election
		// timeout in the past.
		shouldCampaignOnCreation = (r.mu.internalRaftGroup == nil) && r.store.canCampaignIdleReplica()
	}

	ctx := r.AnnotateCtx(context.TODO())

	if r.mu.internalRaftGroup == nil {
		raftGroup, err := raft.NewRawNode(newRaftConfig(
			raft.Storage((*replicaRaftStorage)(r)),
			uint64(r.mu.replicaID),
			r.mu.state.RaftAppliedIndex,
			r.store.cfg,
			&raftLogger{ctx: ctx},
		), nil)
		if err != nil {
			return err
		}
		r.mu.internalRaftGroup = raftGroup

		if !shouldCampaignOnCreation {
			// Automatically campaign and elect a leader for this group if there's
			// exactly one known node for this group.
			//
			// A grey area for this being correct happens in the case when we're
			// currently in the process of adding a second node to the group, with
			// the change committed but not applied.
			//
			// Upon restarting, the first node would immediately elect itself and
			// only then apply the config change, where really it should be applying
			// first and then waiting for the majority (which would now require two
			// votes, not only its own).
			//
			// However, in that special case, the second node has no chance to be
			// elected leader while the first node restarts (as it's aware of the
			// configuration and knows it needs two votes), so the worst that could
			// happen is both nodes ending up in candidate state, timing out and then
			// voting again. This is expected to be an extremely rare event.
			//
			// TODO(peter): It would be more natural for this campaigning to only be
			// done when proposing a command (see defaultProposeRaftCommandLocked).
			// Unfortunately, we enqueue the right hand side of a split for Raft
			// ready processing if the range only has a single replica (see
			// splitPostApply). Doing so implies we need to be campaigning
			// that right hand side range when raft ready processing is
			// performed. Perhaps we should move the logic for campaigning single
			// replica ranges there so that normally we only eagerly campaign when
			// proposing.
			shouldCampaignOnCreation = r.isSoloReplicaRLocked()
		}
		if shouldCampaignOnCreation {
			log.VEventf(ctx, 3, "campaigning")
			if err := raftGroup.Campaign(); err != nil {
				return err
			}
			if fn := r.store.cfg.TestingKnobs.OnCampaign; fn != nil {
				fn(r)
			}
		}
	}

	unquiesce, err := f(r.mu.internalRaftGroup)
	if unquiesce {
		r.unquiesceAndWakeLeaderLocked()
	}
	return err
}

// withRaftGroup calls the supplied function with the (lazily initialized)
// Raft group. It acquires and releases the Replica lock, so r.mu must not be
// held (or acquired by the supplied function).
//
// Requires that Replica.raftMu is held.
func (r *Replica) withRaftGroup(
	f func(r *raft.RawNode) (unquiesceAndWakeLeader bool, _ error),
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.withRaftGroupLocked(false, f)
}

var _ client.Sender = &Replica{}

func newReplica(rangeID roachpb.RangeID, store *Store) *Replica {
	r := &Replica{
		AmbientContext: store.cfg.AmbientCtx,
		RangeID:        rangeID,
		store:          store,
		abortSpan:      abortspan.New(rangeID),
		txnWaitQueue:   txnwait.NewQueue(store),
	}
	r.mu.pendingLeaseRequest = makePendingLeaseRequest(r)
	r.mu.stateLoader = stateloader.Make(r.store.cfg.Settings, rangeID)
	if leaseHistoryMaxEntries > 0 {
		r.leaseHistory = newLeaseHistory()
	}
	if store.cfg.StorePool != nil {
		r.leaseholderStats = newReplicaStats(store.Clock(), store.cfg.StorePool.getNodeLocalityString)
	}
	// Pass nil for the localityOracle because we intentionally don't track the
	// origin locality of write load.
	r.writeStats = newReplicaStats(store.Clock(), nil)

	// Init rangeStr with the range ID.
	r.rangeStr.store(0, &roachpb.RangeDescriptor{RangeID: rangeID})
	// Add replica log tag - the value is rangeStr.String().
	r.AmbientContext.AddLogTag("r", &r.rangeStr)
	// Add replica pointer value. NB: this was historically useful for debugging
	// replica GC issues, but is a distraction at the moment.
	// r.AmbientContext.AddLogTagStr("@", fmt.Sprintf("%x", unsafe.Pointer(r)))
	r.raftMu.stateLoader = stateloader.Make(r.store.cfg.Settings, rangeID)
	return r
}

// NewReplica initializes the replica using the given metadata. If the
// replica is initialized (i.e. desc contains more than a RangeID),
// replicaID should be 0 and the replicaID will be discovered from the
// descriptor.
func NewReplica(
	desc *roachpb.RangeDescriptor, store *Store, replicaID roachpb.ReplicaID,
) (*Replica, error) {
	r := newReplica(desc.RangeID, store)
	return r, r.init(desc, store.Clock(), replicaID)
}

func (r *Replica) init(
	desc *roachpb.RangeDescriptor, clock *hlc.Clock, replicaID roachpb.ReplicaID,
) error {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.initRaftMuLockedReplicaMuLocked(desc, clock, replicaID)
}

func (r *Replica) initRaftMuLockedReplicaMuLocked(
	desc *roachpb.RangeDescriptor, clock *hlc.Clock, replicaID roachpb.ReplicaID,
) error {
	ctx := r.AnnotateCtx(context.TODO())
	if r.mu.state.Desc != nil && r.isInitializedRLocked() {
		log.Fatalf(ctx, "r%d: cannot reinitialize an initialized replica", desc.RangeID)
	}
	if desc.IsInitialized() && replicaID != 0 {
		return errors.Errorf("replicaID must be 0 when creating an initialized replica")
	}

	r.cmdQMu.Lock()
	r.cmdQMu.queues[spanset.SpanGlobal] = NewCommandQueue(true /* optimizeOverlap */)
	r.cmdQMu.queues[spanset.SpanLocal] = NewCommandQueue(false /* optimizeOverlap */)
	r.cmdQMu.Unlock()

	r.mu.proposals = map[storagebase.CmdIDKey]*ProposalData{}
	r.mu.checksums = map[uuid.UUID]ReplicaChecksum{}
	// Clear the internal raft group in case we're being reset. Since we're
	// reloading the raft state below, it isn't safe to use the existing raft
	// group.
	r.mu.internalRaftGroup = nil

	var err error

	if r.mu.state, err = r.mu.stateLoader.Load(ctx, r.store.Engine(), desc); err != nil {
		return err
	}

	// Init the minLeaseProposedTS such that we won't use an existing lease (if
	// any). This is so that, after a restart, we don't propose under old leases.
	// If the replica is being created through a split, this value will be
	// overridden.
	if !r.store.cfg.TestingKnobs.DontPreventUseOfOldLeaseOnStart {
		// Only do this if there was a previous lease. This shouldn't be important
		// to do but consider that the first lease which is obtained is back-dated
		// to a zero start timestamp (and this de-flakes some tests). If we set the
		// min proposed TS here, this lease could not be renewed (by the semantics
		// of minLeaseProposedTS); and since minLeaseProposedTS is copied on splits,
		// this problem would multiply to a number of replicas at cluster bootstrap.
		// Instead, we make the first lease special (which is OK) and the problem
		// disappears.
		if r.mu.state.Lease.Sequence > 0 {
			r.mu.minLeaseProposedTS = clock.Now()
		}
	}

	r.rangeStr.store(0, r.mu.state.Desc)

	r.mu.lastIndex, err = r.mu.stateLoader.LoadLastIndex(ctx, r.store.Engine())
	if err != nil {
		return err
	}
	r.mu.lastTerm = invalidLastTerm

	pErr, err := r.mu.stateLoader.LoadReplicaDestroyedError(ctx, r.store.Engine())
	if err != nil {
		return err
	}
	if r.mu.destroyStatus.RemovedOrCorrupt() {
		if err := pErr.GetDetail(); err != nil {
			r.mu.destroyStatus.Set(err, destroyReasonRemoved)
		}
	}

	if replicaID == 0 {
		repDesc, ok := desc.GetReplicaDescriptor(r.store.StoreID())
		if !ok {
			// This is intentionally not an error and is the code path exercised
			// during preemptive snapshots. The replica ID will be sent when the
			// actual raft replica change occurs.
			return nil
		}
		replicaID = repDesc.ReplicaID
	}
	r.rangeStr.store(replicaID, r.mu.state.Desc)
	if err := r.setReplicaIDRaftMuLockedMuLocked(replicaID); err != nil {
		return err
	}

	r.assertStateLocked(ctx, r.store.Engine())
	return nil
}

// String returns the string representation of the replica using an
// inconsistent copy of the range descriptor. Therefore, String does not
// require a lock and its output may not be atomic with other ongoing work in
// the replica. This is done to prevent deadlocks in logging sites.
func (r *Replica) String() string {
	return fmt.Sprintf("[n%d,s%d,r%s]", r.store.Ident.NodeID, r.store.Ident.StoreID, &r.rangeStr)
}

// destroyData deletes all data associated with a replica, leaving a
// tombstone. Requires that Replica.raftMu is held.
func (r *Replica) destroyDataRaftMuLocked(
	ctx context.Context, consistentDesc roachpb.RangeDescriptor,
) error {
	startTime := timeutil.Now()

	// Use a more efficient write-only batch because we don't need to do any
	// reads from the batch.
	batch := r.store.Engine().NewWriteOnlyBatch()
	defer batch.Close()

	ms := r.GetMVCCStats()

	// NB: this uses the local descriptor instead of the consistent one to match
	// the data on disk.
	desc := r.Desc()
	if err := clearRangeData(ctx, desc, ms.KeyCount, r.store.Engine(), batch); err != nil {
		return err
	}
	clearTime := timeutil.Now()

	// Suggest the cleared range to the compactor queue.
	r.store.compactor.Suggest(ctx, storagebase.SuggestedCompaction{
		StartKey: roachpb.Key(desc.StartKey),
		EndKey:   roachpb.Key(desc.EndKey),
		Compaction: storagebase.Compaction{
			Bytes:            ms.Total(),
			SuggestedAtNanos: clearTime.UnixNano(),
		},
	})

	// Save a tombstone to ensure that replica IDs never get reused.
	//
	// NB: Legacy tombstones (which are in the replicated key space) are wiped
	// in clearRangeData, but that's OK since we're writing a new one in the same
	// batch (and in particular, sequenced *after* the wipe).
	if err := r.setTombstoneKey(ctx, batch, &consistentDesc); err != nil {
		return err
	}
	// We need to sync here because we are potentially deleting sideloaded
	// proposals from the file system next. We could write the tombstone only in
	// a synchronous batch first and then delete the data alternatively, but
	// then need to handle the case in which there is both the tombstone and
	// leftover replica data.
	if err := batch.Commit(true); err != nil {
		return err
	}
	commitTime := timeutil.Now()

	// NB: we need the nil check below because it's possible that we're
	// GC'ing a Replica without a replicaID, in which case it does not
	// have a sideloaded storage.
	//
	// TODO(tschottdorf): at node startup, we should remove all on-disk
	// directories belonging to replicas which aren't present. A crash
	// here will currently leave the files around forever.
	if r.raftMu.sideloaded != nil {
		if err := r.raftMu.sideloaded.Clear(ctx); err != nil {
			return err
		}
	}

	log.Infof(ctx, "removed %d (%d+%d) keys in %0.0fms [clear=%0.0fms commit=%0.0fms]",
		ms.KeyCount+ms.SysCount, ms.KeyCount, ms.SysCount,
		commitTime.Sub(startTime).Seconds()*1000,
		clearTime.Sub(startTime).Seconds()*1000,
		commitTime.Sub(clearTime).Seconds()*1000)
	return nil
}

func (r *Replica) cancelPendingCommandsLocked() {
	r.mu.AssertHeld()
	for _, p := range r.mu.proposals {
		resp := proposalResult{
			Reply:         &roachpb.BatchResponse{},
			Err:           roachpb.NewError(roachpb.NewAmbiguousResultError("removing replica")),
			ProposalRetry: proposalRangeNoLongerExists,
		}
		p.finishApplication(resp)
	}
	r.mu.proposals = map[storagebase.CmdIDKey]*ProposalData{}
}

// setTombstoneKey writes a tombstone to disk to ensure that replica IDs never
// get reused. It determines what the minimum next replica ID can be using
// the provided RangeDescriptor and the Replica's own ID.
//
// We have to be careful to set the right key, since a replica can be using an
// ID that it hasn't yet received a RangeDescriptor for if it receives raft
// requests for that replica ID (as seen in #14231).
func (r *Replica) setTombstoneKey(
	ctx context.Context, eng engine.ReadWriter, desc *roachpb.RangeDescriptor,
) error {
	r.mu.Lock()
	nextReplicaID := r.nextReplicaIDLocked(desc)
	r.mu.minReplicaID = nextReplicaID
	r.mu.Unlock()

	tombstoneKey := keys.RaftTombstoneKey(desc.RangeID)
	if !r.store.cfg.Settings.Version.IsMinSupported(cluster.VersionUnreplicatedTombstoneKey) {
		tombstoneKey = keys.RaftTombstoneIncorrectLegacyKey(desc.RangeID)
	}
	tombstone := &roachpb.RaftTombstone{
		NextReplicaID: nextReplicaID,
	}
	return engine.MVCCPutProto(ctx, eng, nil, tombstoneKey,
		hlc.Timestamp{}, nil, tombstone)
}

// nextReplicaIDLocked returns the minimum ID that a new replica can be created
// with for this replica's range. We have to be very careful to ensure that
// replica IDs never get re-used because that can cause panics.
//
// The externalDesc parameter is an optional way to provide an additional
// descriptor for the range that was looked up outside the replica code.
func (r *Replica) nextReplicaIDLocked(externalDesc *roachpb.RangeDescriptor) roachpb.ReplicaID {
	result := r.mu.state.Desc.NextReplicaID
	if externalDesc != nil && result < externalDesc.NextReplicaID {
		result = externalDesc.NextReplicaID
	}
	if result < r.mu.minReplicaID {
		result = r.mu.minReplicaID
	}
	return result
}

func (r *Replica) setReplicaID(replicaID roachpb.ReplicaID) error {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.setReplicaIDRaftMuLockedMuLocked(replicaID)
}

func (r *Replica) setReplicaIDRaftMuLockedMuLocked(replicaID roachpb.ReplicaID) error {
	if r.mu.replicaID == replicaID {
		// The common case: the replica ID is unchanged.
		return nil
	}
	if replicaID == 0 {
		// If the incoming message does not have a new replica ID it is a
		// preemptive snapshot. We'll update minReplicaID if the snapshot is
		// accepted.
		return nil
	}
	if replicaID < r.mu.minReplicaID {
		return &roachpb.RaftGroupDeletedError{}
	}
	if r.mu.replicaID > replicaID {
		return errors.Errorf("replicaID cannot move backwards from %d to %d", r.mu.replicaID, replicaID)
	}

	if r.mu.destroyStatus.reason == destroyReasonRemovalPending {
		// An earlier incarnation of this replica was removed, but apparently it has been re-added
		// now, so reset the status.
		r.mu.destroyStatus.Reset()
	}

	// if r.mu.replicaID != 0 {
	// 	// TODO(bdarnell): clean up previous raftGroup (update peers)
	// }

	// Initialize or update the sideloaded storage. If the sideloaded storage
	// already exists (which is iff the previous replicaID was non-zero), then
	// we have to move the contained files over (this corresponds to the case in
	// which our replica is removed and re-added to the range, without having
	// the replica GC'ed in the meantime).
	//
	// Note that we can't race with a concurrent replicaGC here because both that
	// and this is under raftMu.
	var prevSideloadedDir string
	if ss := r.raftMu.sideloaded; ss != nil {
		prevSideloadedDir = ss.Dir()
	}
	var err error
	if r.raftMu.sideloaded, err = newDiskSideloadStorage(
		r.store.cfg.Settings,
		r.mu.state.Desc.RangeID,
		replicaID,
		r.store.Engine().GetAuxiliaryDir(),
		r.store.limiters.BulkIOWriteRate,
	); err != nil {
		return errors.Wrap(err, "while initializing sideloaded storage")
	}
	if prevSideloadedDir != "" {
		if _, err := os.Stat(prevSideloadedDir); err != nil {
			if !os.IsNotExist(err) {
				return err
			}
			// Old directory not found.
		} else {
			// Old directory found, so we have something to move over to the new one.
			if err := os.Rename(prevSideloadedDir, r.raftMu.sideloaded.Dir()); err != nil {
				return errors.Wrap(err, "while moving sideloaded directory")
			}
		}
	}

	previousReplicaID := r.mu.replicaID
	r.mu.replicaID = replicaID

	if replicaID >= r.mu.minReplicaID {
		r.mu.minReplicaID = replicaID + 1
	}
	// Reset the raft group to force its recreation on next usage.
	r.mu.internalRaftGroup = nil

	// If there was a previous replica, repropose its pending commands under
	// this new incarnation.
	if previousReplicaID != 0 {
		if log.V(1) {
			log.Infof(r.AnnotateCtx(context.TODO()), "changed replica ID from %d to %d",
				previousReplicaID, replicaID)
		}
		// repropose all pending commands under new replicaID.
		r.refreshProposalsLocked(0, reasonReplicaIDChanged)
	}

	return nil
}

func (r *Replica) maybeAcquireProposalQuota(ctx context.Context, quota int64) error {
	r.mu.RLock()
	quotaPool := r.mu.proposalQuota
	desc := *r.mu.state.Desc
	r.mu.RUnlock()

	// Quota acquisition only takes place on the leader replica,
	// r.mu.proposalQuota is set to nil if a node is a follower (see
	// updateProposalQuotaRaftMuLocked). For the cases where the range lease
	// holder is not the same as the range leader, i.e. the lease holder is a
	// follower, r.mu.proposalQuota == nil. This means all quota acquisitions
	// go through without any throttling whatsoever but given how short lived
	// these scenarios are we don't try to remedy any further.
	//
	// NB: It is necessary to allow proposals with a nil quota pool to go
	// through, for otherwise a follower could never request the lease.

	if quotaPool == nil {
		return nil
	}

	if !quotaPoolEnabledForRange(desc) {
		return nil
	}

	// Trace if we're running low on available proposal quota; it might explain
	// why we're taking so long.
	if q := quotaPool.approximateQuota(); q < quotaPool.maxQuota()/10 && log.HasSpanOrEvent(ctx) {
		log.Eventf(ctx, "quota running low, currently available ~%d", q)
	}

	return quotaPool.acquire(ctx, quota)
}

func quotaPoolEnabledForRange(desc roachpb.RangeDescriptor) bool {
	// The NodeLiveness range does not use a quota pool. We don't want to
	// throttle updates to the NodeLiveness range even if a follower is falling
	// behind because this could result in cascading failures.
	return !bytes.HasPrefix(desc.StartKey, keys.NodeLivenessPrefix)
}

func (r *Replica) updateProposalQuotaRaftMuLocked(
	ctx context.Context, lastLeaderID roachpb.ReplicaID,
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.mu.replicaID == 0 {
		// The replica was created from preemptive snapshot and has not been
		// added to the Raft group.
		return
	}

	// We need to check if the replica is being destroyed and if so, unblock
	// all ongoing and subsequent quota acquisition goroutines (if any).
	//
	// TODO(irfansharif): There is still a potential problem here that leaves
	// clients hanging if the replica gets destroyed but this code path is
	// never taken. Moving quota pool draining to every point where a
	// replica can get destroyed is an option, alternatively we can clear
	// our leader status and close the proposalQuota whenever the replica is
	// destroyed.
	if r.mu.destroyStatus.RemovedOrCorrupt() {
		if r.mu.proposalQuota != nil {
			r.mu.proposalQuota.close()
		}
		r.mu.proposalQuota = nil
		r.mu.lastUpdateTimes = nil
		r.mu.quotaReleaseQueue = nil
		r.mu.commandSizes = nil
		return
	}

	if r.mu.leaderID != lastLeaderID {
		if r.mu.replicaID == r.mu.leaderID {
			// We're becoming the leader.
			r.mu.proposalQuotaBaseIndex = r.mu.lastIndex

			if r.mu.proposalQuota != nil {
				log.Fatal(ctx, "proposalQuota was not nil before becoming the leader")
			}
			if releaseQueueLen := len(r.mu.quotaReleaseQueue); releaseQueueLen != 0 {
				log.Fatalf(ctx, "len(r.mu.quotaReleaseQueue) = %d, expected 0", releaseQueueLen)
			}
			if commandSizesLen := len(r.mu.commandSizes); commandSizesLen != 0 {
				log.Fatalf(ctx, "len(r.mu.commandSizes) = %d, expected 0", commandSizesLen)
			}

			// Raft may propose commands itself (specifically the empty
			// commands when leadership changes), and these commands don't go
			// through the code paths where we acquire quota from the pool. To
			// offset this we reset the quota pool whenever leadership changes
			// hands.
			r.mu.proposalQuota = newQuotaPool(defaultProposalQuota)
			r.mu.lastUpdateTimes = make(map[roachpb.ReplicaID]time.Time)
			r.mu.commandSizes = make(map[storagebase.CmdIDKey]int)
		} else if r.mu.proposalQuota != nil {
			// We're becoming a follower.

			// We unblock all ongoing and subsequent quota acquisition
			// goroutines (if any).
			r.mu.proposalQuota.close()
			r.mu.proposalQuota = nil
			r.mu.lastUpdateTimes = nil
			r.mu.quotaReleaseQueue = nil
			r.mu.commandSizes = nil
		}
		return
	} else if r.mu.proposalQuota == nil {
		if r.mu.replicaID == r.mu.leaderID {
			log.Fatal(ctx, "leader has uninitialized proposalQuota pool")
		}
		// We're a follower.
		return
	}

	// We're still the leader.

	// TODO(peter): Can we avoid retrieving the Raft status on every invocation
	// in order to avoid the associated allocation? Tracking the progress
	// ourselves via looking at MsgAppResp messages would be overkill. Perhaps
	// another accessor on RawNode.
	status := r.raftStatusRLocked()
	if status == nil {
		log.Fatal(ctx, "leader with nil RaftStatus")
	}

	// Find the minimum index that active followers have acknowledged.
	minIndex := status.Commit
	for _, rep := range r.mu.state.Desc.Replicas {
		// Only consider followers that that have "healthy" RPC connections.

		addr, err := r.store.cfg.Transport.resolver(rep.NodeID)
		if err != nil {
			continue
		}
		if err := r.store.cfg.Transport.rpcContext.ConnHealth(addr.String()); err != nil {
			continue
		}

		// Only consider followers that are active.
		if !r.isFollowerActiveLocked(ctx, rep.ReplicaID) {
			continue
		}
		if progress, ok := status.Progress[uint64(rep.ReplicaID)]; ok {
			// Only consider followers who are in advance of the quota base
			// index. This prevents a follower from coming back online and
			// preventing throughput to the range until it has caught up.
			if progress.Match < r.mu.proposalQuotaBaseIndex {
				continue
			}
			if progress.Match > 0 && progress.Match < minIndex {
				minIndex = progress.Match
			}
			// If this is the most recently added replica and it has caught up, clear
			// our state that was tracking it. This is unrelated to managing proposal
			// quota, but this is a convenient place to do so.
			if rep.ReplicaID == r.mu.lastReplicaAdded && progress.Match >= status.Commit {
				r.mu.lastReplicaAdded = 0
				r.mu.lastReplicaAddedTime = time.Time{}
			}
		}
	}

	if r.mu.proposalQuotaBaseIndex < minIndex {
		// We've persisted minIndex - r.mu.proposalQuotaBaseIndex entries to
		// the raft log on all 'active' replicas since last we checked,
		// we 'should' be able to release the difference back to
		// the quota pool. But consider the scenario where we have a single
		// replica that we're writing to, we only construct the
		// quotaReleaseQueue when entries 'come out' of Raft via
		// raft.Ready.CommittedEntries. The minIndex computed above uses the
		// replica's commit index which is independent of whether or we've
		// iterated over the entirety of raft.Ready.CommittedEntries and
		// therefore may not have all minIndex - r.mu.proposalQuotaBaseIndex
		// command sizes in our quotaReleaseQueue.  Hence we only process
		// min(minIndex - r.mu.proposalQuotaBaseIndex, len(r.mu.quotaReleaseQueue))
		// quota releases.
		numReleases := minIndex - r.mu.proposalQuotaBaseIndex
		if qLen := uint64(len(r.mu.quotaReleaseQueue)); qLen < numReleases {
			numReleases = qLen
		}
		sum := 0
		for _, rel := range r.mu.quotaReleaseQueue[:numReleases] {
			sum += rel
		}
		r.mu.proposalQuotaBaseIndex += numReleases
		r.mu.quotaReleaseQueue = r.mu.quotaReleaseQueue[numReleases:]

		r.mu.proposalQuota.add(int64(sum))
	}
}

// GetMaxBytes gets the range maximum byte limit.
func (r *Replica) GetMaxBytes() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.maxBytes
}

// SetMaxBytes sets the maximum byte limit before split.
func (r *Replica) SetMaxBytes(maxBytes int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.maxBytes = maxBytes
}

// IsFirstRange returns true if this is the first range.
func (r *Replica) IsFirstRange() bool {
	return r.RangeID == 1
}

// IsDestroyed returns a non-nil error if the replica has been destroyed
// and the reason if it has.
func (r *Replica) IsDestroyed() (DestroyReason, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.destroyStatus.reason, r.mu.destroyStatus.err
}

// GetLease returns the lease and, if available, the proposed next lease.
func (r *Replica) GetLease() (roachpb.Lease, *roachpb.Lease) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getLeaseRLocked()
}

func (r *Replica) getLeaseRLocked() (roachpb.Lease, *roachpb.Lease) {
	if nextLease, ok := r.mu.pendingLeaseRequest.RequestPending(); ok {
		return *r.mu.state.Lease, &nextLease
	}
	return *r.mu.state.Lease, nil
}

// OwnsValidLease returns whether this replica is the current valid
// leaseholder. Note that this method does not check to see if a transfer is
// pending, but returns the status of the current lease and ownership at the
// specified point in time.
func (r *Replica) OwnsValidLease(ts hlc.Timestamp) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ownsValidLeaseRLocked(ts)
}

func (r *Replica) ownsValidLeaseRLocked(ts hlc.Timestamp) bool {
	return r.mu.state.Lease.OwnedBy(r.store.StoreID()) &&
		r.leaseStatus(*r.mu.state.Lease, ts, r.mu.minLeaseProposedTS).State == LeaseState_VALID
}

// IsLeaseValid returns true if the replica's lease is owned by this
// replica and is valid (not expired, not in stasis).
func (r *Replica) IsLeaseValid(lease roachpb.Lease, ts hlc.Timestamp) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isLeaseValidRLocked(lease, ts)
}

func (r *Replica) isLeaseValidRLocked(lease roachpb.Lease, ts hlc.Timestamp) bool {
	return r.leaseStatus(lease, ts, r.mu.minLeaseProposedTS).State == LeaseState_VALID
}

// newNotLeaseHolderError returns a NotLeaseHolderError initialized with the
// replica for the holder (if any) of the given lease.
//
// Note that this error can be generated on the Raft processing goroutine, so
// its output should be completely determined by its parameters.
func newNotLeaseHolderError(
	l *roachpb.Lease, proposerStoreID roachpb.StoreID, rangeDesc *roachpb.RangeDescriptor,
) *roachpb.NotLeaseHolderError {
	err := &roachpb.NotLeaseHolderError{
		RangeID: rangeDesc.RangeID,
	}
	err.Replica, _ = rangeDesc.GetReplicaDescriptor(proposerStoreID)
	if l != nil {
		// Normally, we return the lease-holding Replica here. However, in the
		// case in which a leader removes itself, we want the followers to
		// avoid handing out a misleading clue (which in itself shouldn't be
		// overly disruptive as the lease would expire and then this method
		// shouldn't be called for it any more, but at the very least it
		// could catch tests in a loop, presumably due to manual clocks).
		_, stillMember := rangeDesc.GetReplicaDescriptor(l.Replica.StoreID)
		if stillMember {
			err.LeaseHolder = &l.Replica
			err.Lease = l
		}
	}
	return err
}

// leaseGoodToGo is a fast-path for lease checks which verifies that an
// existing lease is valid and owned by the current store. This method should
// not be called directly. Use redirectOnOrAcquireLease instead.
func (r *Replica) leaseGoodToGo(ctx context.Context) (LeaseStatus, bool) {
	timestamp := r.store.Clock().Now()
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.requiresExpiringLeaseRLocked() {
		// Slow-path for expiration-based leases.
		return LeaseStatus{}, false
	}

	status := r.leaseStatus(*r.mu.state.Lease, timestamp, r.mu.minLeaseProposedTS)
	if status.State == LeaseState_VALID && status.Lease.OwnedBy(r.store.StoreID()) {
		// We own the lease...
		if repDesc, err := r.getReplicaDescriptorRLocked(); err == nil {
			if _, ok := r.mu.pendingLeaseRequest.TransferInProgress(repDesc.ReplicaID); !ok {
				// ...and there is no transfer pending.
				return status, true
			}
		}
	}
	return LeaseStatus{}, false
}

// redirectOnOrAcquireLease checks whether this replica has the lease
// at the current timestamp. If it does, returns success. If another
// replica currently holds the lease, redirects by returning
// NotLeaseHolderError. If the lease is expired, a renewal is
// synchronously requested. Leases are eagerly renewed when a request
// with a timestamp within rangeLeaseRenewalDuration of the lease
// expiration is served.
//
// TODO(spencer): for write commands, don't wait while requesting
//  the range lease. If the lease acquisition fails, the write cmd
//  will fail as well. If it succeeds, as is likely, then the write
//  will not incur latency waiting for the command to complete.
//  Reads, however, must wait.
func (r *Replica) redirectOnOrAcquireLease(ctx context.Context) (LeaseStatus, *roachpb.Error) {
	if status, ok := r.leaseGoodToGo(ctx); ok {
		return status, nil
	}

	// Loop until the lease is held or the replica ascertains the actual
	// lease holder. Returns also on context.Done() (timeout or cancellation).
	var status LeaseStatus
	for attempt := 1; ; attempt++ {
		timestamp := r.store.Clock().Now()
		llHandle, pErr := func() (*leaseRequestHandle, *roachpb.Error) {
			r.mu.Lock()
			defer r.mu.Unlock()

			status = r.leaseStatus(*r.mu.state.Lease, timestamp, r.mu.minLeaseProposedTS)
			switch status.State {
			case LeaseState_ERROR:
				// Lease state couldn't be determined.
				log.VEventf(ctx, 2, "lease state couldn't be determined")
				return nil, roachpb.NewError(
					newNotLeaseHolderError(nil, r.store.StoreID(), r.mu.state.Desc))

			case LeaseState_VALID, LeaseState_STASIS:
				if !status.Lease.OwnedBy(r.store.StoreID()) {
					_, stillMember := r.mu.state.Desc.GetReplicaDescriptor(status.Lease.Replica.StoreID)
					if !stillMember {
						// This would be the situation in which the lease holder gets removed when
						// holding the lease, or in which a lease request erroneously gets accepted
						// for a replica that is not in the replica set. Neither of the two can
						// happen since appropriate mechanisms have been added:
						//
						// 1. Only the lease holder (at the time) schedules removal of a replica,
						// but the lease can change hands and so the situation in which a follower
						// coordinates a replica removal of the (new) lease holder is possible (if
						// unlikely) in practice. In this situation, the new lease holder would at
						// some point be asked to propose the replica change's EndTransaction to
						// Raft. A check has been added that prevents proposals that amount to the
						// removal of the proposer's (and hence lease holder's) Replica, preventing
						// this scenario.
						//
						// 2. A lease is accepted for a Replica that has been removed. Without
						// precautions, this could happen because lease requests are special in
						// that they are the only command that is proposed on a follower (other
						// commands may be proposed from followers, but not successfully so). For
						// all proposals, processRaftCommand checks that their ProposalLease is
						// compatible with the active lease for the log position. For commands
						// proposed on the lease holder, the command queue then serializes
						// everything. But lease requests get created on followers based on their
						// local state and thus without being sequenced through the command queue.
						// Thus a recently removed follower (unaware of its own removal) could
						// submit a proposal for the lease (correctly using as a ProposerLease the
						// last active lease), and would receive it given the up-to-date
						// ProposerLease. Hence, an extra check is in order: processRaftCommand
						// makes sure that lease requests for a replica not in the descriptor are
						// bounced.
						log.Fatalf(ctx, "lease %s owned by replica %+v that no longer exists",
							status.Lease, status.Lease.Replica)
					}
					// Otherwise, if the lease is currently held by another replica, redirect
					// to the holder.
					return nil, roachpb.NewError(
						newNotLeaseHolderError(&status.Lease, r.store.StoreID(), r.mu.state.Desc))
				}
				// Check that we're not in the process of transferring the lease away.
				// If we are transferring the lease away, we can't serve reads or
				// propose Raft commands - see comments on TransferLease.
				// TODO(andrei): If the lease is being transferred, consider returning a
				// new error type so the client backs off until the transfer is
				// completed.
				repDesc, err := r.getReplicaDescriptorRLocked()
				if err != nil {
					return nil, roachpb.NewError(err)
				}
				if transferLease, ok := r.mu.pendingLeaseRequest.TransferInProgress(
					repDesc.ReplicaID); ok {
					return nil, roachpb.NewError(
						newNotLeaseHolderError(&transferLease, r.store.StoreID(), r.mu.state.Desc))
				}

				// If the lease is in stasis, we can't serve requests until we've
				// renewed the lease, so we return the handle to block on renewal.
				// Otherwise, we don't need to wait for the extension and simply
				// ignore the returned handle (whose channel is buffered) and continue.
				if status.State == LeaseState_STASIS {
					return r.requestLeaseLocked(ctx, status), nil
				}

				// Extend the lease if this range uses expiration-based
				// leases, the lease is in need of renewal, and there's not
				// already an extension pending.
				_, requestPending := r.mu.pendingLeaseRequest.RequestPending()
				if !requestPending && r.requiresExpiringLeaseRLocked() {
					renewal := status.Lease.Expiration.Add(-r.store.cfg.RangeLeaseRenewalDuration().Nanoseconds(), 0)
					if !timestamp.Less(renewal) {
						if log.V(2) {
							log.Infof(ctx, "extending lease %s at %s", status.Lease, timestamp)
						}
						// We had an active lease to begin with, but we want to trigger
						// a lease extension. We explicitly ignore the returned handle
						// as we won't block on it.
						_ = r.requestLeaseLocked(ctx, status)
					}
				}

			case LeaseState_EXPIRED:
				// No active lease: Request renewal if a renewal is not already pending.
				log.VEventf(ctx, 2, "request range lease (attempt #%d)", attempt)
				return r.requestLeaseLocked(ctx, status), nil

			case LeaseState_PROSCRIBED:
				// Lease proposed timestamp is earlier than the min proposed
				// timestamp limit this replica must observe. If this store
				// owns the lease, re-request. Otherwise, redirect.
				if status.Lease.OwnedBy(r.store.StoreID()) {
					log.VEventf(ctx, 2, "request range lease (attempt #%d)", attempt)
					return r.requestLeaseLocked(ctx, status), nil
				}
				// If lease is currently held by another, redirect to holder.
				return nil, roachpb.NewError(
					newNotLeaseHolderError(&status.Lease, r.store.StoreID(), r.mu.state.Desc))
			}

			// Return a nil handle to signal that we have a valid lease.
			return nil, nil
		}()
		if pErr != nil {
			return LeaseStatus{}, pErr
		}
		if llHandle == nil {
			// We own a valid lease.
			return status, nil
		}

		// Wait for the range lease to finish, or the context to expire.
		pErr = func() *roachpb.Error {
			slowTimer := timeutil.NewTimer()
			defer slowTimer.Stop()
			slowTimer.Reset(base.SlowRequestThreshold)
			for {
				select {
				case pErr = <-llHandle.C():
					if pErr != nil {
						switch tErr := pErr.GetDetail().(type) {
						case *roachpb.AmbiguousResultError:
							// This can happen if the RequestLease command we sent has been
							// applied locally through a snapshot: the RequestLeaseRequest
							// cannot be reproposed so we get this ambiguity.
							// We'll just loop around.
							return nil
						case *roachpb.LeaseRejectedError:
							if tErr.Existing.OwnedBy(r.store.StoreID()) {
								// The RequestLease command we sent was rejected because another
								// lease was applied in the meantime, but we own that other
								// lease. So, loop until the current node becomes aware that
								// it's the leaseholder.
								return nil
							}

							// Getting a LeaseRejectedError back means someone else got there
							// first, or the lease request was somehow invalid due to a concurrent
							// change. That concurrent change could have been that this replica was
							// removed (see processRaftCommand), so check for that case before
							// falling back to a NotLeaseHolderError.
							var err error
							if _, descErr := r.GetReplicaDescriptor(); descErr != nil {
								err = descErr
							} else if lease, _ := r.GetLease(); !r.IsLeaseValid(lease, r.store.Clock().Now()) {
								err = newNotLeaseHolderError(nil, r.store.StoreID(), r.Desc())
							} else {
								err = newNotLeaseHolderError(&lease, r.store.StoreID(), r.Desc())
							}
							pErr = roachpb.NewError(err)
						}
						return pErr
					}
					log.Eventf(ctx, "lease acquisition succeeded: %+v", status.Lease)
					return nil
				case <-slowTimer.C:
					slowTimer.Read = true
					log.Warningf(ctx, "have been waiting %s attempting to acquire lease",
						base.SlowRequestThreshold)
					r.store.metrics.SlowLeaseRequests.Inc(1)
					defer r.store.metrics.SlowLeaseRequests.Dec(1)
				case <-ctx.Done():
					llHandle.Cancel()
					log.VErrEventf(ctx, 2, "lease acquisition failed: %s", ctx.Err())
					return roachpb.NewError(newNotLeaseHolderError(nil, r.store.StoreID(), r.Desc()))
				case <-r.store.Stopper().ShouldStop():
					llHandle.Cancel()
					return roachpb.NewError(newNotLeaseHolderError(nil, r.store.StoreID(), r.Desc()))
				}
			}
		}()
		if pErr != nil {
			return LeaseStatus{}, pErr
		}
	}
}

// IsInitialized is true if we know the metadata of this range, either
// because we created it or we have received an initial snapshot from
// another node. It is false when a range has been created in response
// to an incoming message but we are waiting for our initial snapshot.
func (r *Replica) IsInitialized() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isInitializedRLocked()
}

// isInitializedRLocked is true if we know the metadata of this range, either
// because we created it or we have received an initial snapshot from
// another node. It is false when a range has been created in response
// to an incoming message but we are waiting for our initial snapshot.
// isInitializedLocked requires that the replica lock is held.
func (r *Replica) isInitializedRLocked() bool {
	return r.mu.state.Desc.IsInitialized()
}

// Desc returns the authoritative range descriptor, acquiring a replica lock in
// the process.
func (r *Replica) Desc() *roachpb.RangeDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.state.Desc
}

// NodeID returns the ID of the node this replica belongs to.
func (r *Replica) NodeID() roachpb.NodeID {
	return r.store.nodeDesc.NodeID
}

// setDesc atomically sets the range's descriptor. This method calls
// processRangeDescriptorUpdate() to make the Store handle the descriptor
// update. Requires raftMu to be locked.
func (r *Replica) setDesc(desc *roachpb.RangeDescriptor) error {
	r.setDescWithoutProcessUpdate(desc)
	if r.store == nil {
		// r.rm is null in some tests.
		return nil
	}
	return r.store.processRangeDescriptorUpdate(r.AnnotateCtx(context.TODO()), r)
}

// setDescWithoutProcessUpdate updates the range descriptor without calling
// processRangeDescriptorUpdate. Requires raftMu to be locked.
func (r *Replica) setDescWithoutProcessUpdate(desc *roachpb.RangeDescriptor) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if desc.RangeID != r.RangeID {
		ctx := r.AnnotateCtx(context.TODO())
		log.Fatalf(ctx, "range descriptor ID (%d) does not match replica's range ID (%d)",
			desc.RangeID, r.RangeID)
	}
	if r.mu.state.Desc != nil && r.mu.state.Desc.IsInitialized() &&
		(desc == nil || !desc.IsInitialized()) {
		ctx := r.AnnotateCtx(context.TODO())
		log.Fatalf(ctx, "cannot replace initialized descriptor with uninitialized one: %+v -> %+v",
			r.mu.state.Desc, desc)
	}

	newMaxID := maxReplicaID(desc)
	if newMaxID > r.mu.lastReplicaAdded {
		r.mu.lastReplicaAdded = newMaxID
		r.mu.lastReplicaAddedTime = timeutil.Now()
	}

	r.rangeStr.store(r.mu.replicaID, desc)
	r.mu.state.Desc = desc
}

func maxReplicaID(desc *roachpb.RangeDescriptor) roachpb.ReplicaID {
	if desc == nil || !desc.IsInitialized() {
		return 0
	}
	var maxID roachpb.ReplicaID
	for _, repl := range desc.Replicas {
		if repl.ReplicaID > maxID {
			maxID = repl.ReplicaID
		}
	}
	return maxID
}

// LastReplicaAdded returns the ID of the most recently added replica and the
// time at which it was added.
func (r *Replica) LastReplicaAdded() (roachpb.ReplicaID, time.Time) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.lastReplicaAdded, r.mu.lastReplicaAddedTime
}

// GetReplicaDescriptor returns the replica for this range from the range
// descriptor. Returns a *RangeNotFoundError if the replica is not found.
// No other errors are returned.
func (r *Replica) GetReplicaDescriptor() (roachpb.ReplicaDescriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getReplicaDescriptorRLocked()
}

// getReplicaDescriptorRLocked is like getReplicaDescriptor, but assumes that
// r.mu is held for either reading or writing.
func (r *Replica) getReplicaDescriptorRLocked() (roachpb.ReplicaDescriptor, error) {
	repDesc, ok := r.mu.state.Desc.GetReplicaDescriptor(r.store.StoreID())
	if ok {
		return repDesc, nil
	}
	return roachpb.ReplicaDescriptor{}, roachpb.NewRangeNotFoundError(r.RangeID)
}

// setLastReplicaDescriptors sets the the most recently seen replica
// descriptors to those contained in the *RaftMessageRequest, acquiring r.mu
// to do so.
func (r *Replica) setLastReplicaDescriptors(req *RaftMessageRequest) {
	r.mu.Lock()
	r.mu.lastFromReplica = req.FromReplica
	r.mu.lastToReplica = req.ToReplica
	r.mu.Unlock()
}

// GetMVCCStats returns a copy of the MVCC stats object for this range.
func (r *Replica) GetMVCCStats() enginepb.MVCCStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return *r.mu.state.Stats
}

// ContainsKey returns whether this range contains the specified key.
//
// TODO(bdarnell): This is not the same as RangeDescriptor.ContainsKey.
func (r *Replica) ContainsKey(key roachpb.Key) bool {
	return containsKey(*r.Desc(), key)
}

func containsKey(desc roachpb.RangeDescriptor, key roachpb.Key) bool {
	if bytes.HasPrefix(key, keys.LocalRangeIDPrefix) {
		return bytes.HasPrefix(key, keys.MakeRangeIDPrefix(desc.RangeID))
	}
	keyAddr, err := keys.Addr(key)
	if err != nil {
		return false
	}
	return desc.ContainsKey(keyAddr)
}

// ContainsKeyRange returns whether this range contains the specified
// key range from start to end.
func (r *Replica) ContainsKeyRange(start, end roachpb.Key) bool {
	return containsKeyRange(*r.Desc(), start, end)
}

func containsKeyRange(desc roachpb.RangeDescriptor, start, end roachpb.Key) bool {
	startKeyAddr, err := keys.Addr(start)
	if err != nil {
		return false
	}
	endKeyAddr, err := keys.Addr(end)
	if err != nil {
		return false
	}
	return desc.ContainsKeyRange(startKeyAddr, endKeyAddr)
}

// GetLastReplicaGCTimestamp reads the timestamp at which the replica was
// last checked for removal by the replica gc queue.
func (r *Replica) GetLastReplicaGCTimestamp(ctx context.Context) (hlc.Timestamp, error) {
	key := keys.RangeLastReplicaGCTimestampKey(r.RangeID)
	var timestamp hlc.Timestamp
	_, err := engine.MVCCGetProto(ctx, r.store.Engine(), key, hlc.Timestamp{}, true, nil, &timestamp)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	return timestamp, nil
}

func (r *Replica) setLastReplicaGCTimestamp(ctx context.Context, timestamp hlc.Timestamp) error {
	key := keys.RangeLastReplicaGCTimestampKey(r.RangeID)
	return engine.MVCCPutProto(ctx, r.store.Engine(), nil, key, hlc.Timestamp{}, nil, &timestamp)
}

// getQueueLastProcessed returns the last processed timestamp for the
// specified queue, or the zero timestamp if not available.
func (r *Replica) getQueueLastProcessed(ctx context.Context, queue string) (hlc.Timestamp, error) {
	key := keys.QueueLastProcessedKey(r.Desc().StartKey, queue)
	var timestamp hlc.Timestamp
	if r.store != nil {
		_, err := engine.MVCCGetProto(ctx, r.store.Engine(), key, hlc.Timestamp{}, true, nil, &timestamp)
		if err != nil {
			log.VErrEventf(ctx, 2, "last processed timestamp unavailable: %s", err)
			return hlc.Timestamp{}, err
		}
	}
	log.VEventf(ctx, 2, "last processed timestamp: %s", timestamp)
	return timestamp, nil
}

// setQueueLastProcessed writes the last processed timestamp for the
// specified queue.
func (r *Replica) setQueueLastProcessed(
	ctx context.Context, queue string, timestamp hlc.Timestamp,
) error {
	key := keys.QueueLastProcessedKey(r.Desc().StartKey, queue)
	return r.store.DB().PutInline(ctx, key, &timestamp)
}

func (r *Replica) refreshLastUpdateTimeForReplicaLocked(replicaID roachpb.ReplicaID) {
	if r.mu.lastUpdateTimes != nil {
		r.mu.lastUpdateTimes[replicaID] = r.store.Clock().PhysicalTime()
	}
}

func (r *Replica) refreshLastUpdateTimeForAllReplicasLocked() {
	if r.mu.lastUpdateTimes != nil {
		now := r.store.Clock().PhysicalTime()
		for _, rep := range r.mu.state.Desc.Replicas {
			r.mu.lastUpdateTimes[rep.ReplicaID] = now
		}
	}
}

// isFollowerActiveLocked returns whether the specified follower has made
// communication with the leader in the last MaxQuotaReplicaLivenessDuration.
func (r *Replica) isFollowerActiveLocked(ctx context.Context, followerID roachpb.ReplicaID) bool {
	if r.mu.lastUpdateTimes == nil {
		log.Fatalf(ctx, "replica %d has uninitialized lastUpdateTimes map", r.mu.replicaID)
	}
	lastUpdateTime, ok := r.mu.lastUpdateTimes[followerID]
	if !ok {
		// If the follower has no entry in lastUpdateTimes, it has not been
		// updated since r became the leader.
		return false
	}
	now := r.store.Clock().PhysicalTime()
	return now.Sub(lastUpdateTime) <= MaxQuotaReplicaLivenessDuration
}

// RaftStatus returns the current raft status of the replica. It returns nil
// if the Raft group has not been initialized yet.
func (r *Replica) RaftStatus() *raft.Status {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.raftStatusRLocked()
}

func (r *Replica) raftStatusRLocked() *raft.Status {
	if rg := r.mu.internalRaftGroup; rg != nil {
		return rg.Status()
	}
	return nil
}

// State returns a copy of the internal state of the Replica, along with some
// auxiliary information.
func (r *Replica) State() storagebase.RangeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var ri storagebase.RangeInfo
	ri.ReplicaState = *(protoutil.Clone(&r.mu.state)).(*storagebase.ReplicaState)
	ri.LastIndex = r.mu.lastIndex
	ri.NumPending = uint64(len(r.mu.proposals))
	ri.RaftLogSize = r.mu.raftLogSize
	ri.NumDropped = uint64(r.mu.droppedMessages)
	if r.mu.proposalQuota != nil {
		ri.ApproximateProposalQuota = r.mu.proposalQuota.approximateQuota()
	}
	return ri
}

// assertStateLocked can be called from the Raft goroutine to check that the
// in-memory and on-disk states of the Replica are congruent.
// Requires that both r.raftMu and r.mu are held.
//
// TODO(tschottdorf): Consider future removal (for example, when #7224 is resolved).
func (r *Replica) assertStateLocked(ctx context.Context, reader engine.Reader) {
	diskState, err := r.mu.stateLoader.Load(ctx, reader, r.mu.state.Desc)
	if err != nil {
		log.Fatal(ctx, err)
	}
	if !diskState.Equal(r.mu.state) {
		// The roundabout way of printing here is to expose this information in sentry.io.
		//
		// TODO(dt): expose properly once #15892 is addressed.
		log.Errorf(ctx, "on-disk and in-memory state diverged:\n%s", pretty.Diff(diskState, r.mu.state))
		r.mu.state.Desc, diskState.Desc = nil, nil
		log.Fatal(ctx, log.Safe(
			fmt.Sprintf("on-disk and in-memory state diverged: %s",
				pretty.Diff(diskState, r.mu.state)),
		))
	}
}

// maybeInitializeRaftGroup check whether the internal Raft group has
// not yet been initialized. If not, it is created and set to campaign
// if this replica is the most recent owner of the range lease.
func (r *Replica) maybeInitializeRaftGroup(ctx context.Context) {
	r.mu.RLock()
	// If this replica hasn't initialized the Raft group, create it and
	// unquiesce and wake the leader to ensure the replica comes up to date.
	initialized := r.mu.internalRaftGroup != nil
	r.mu.RUnlock()
	if initialized {
		return
	}

	// Acquire raftMu, but need to maintain lock ordering (raftMu then mu).
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	// Campaign if this replica is the current lease holder to avoid
	// an election storm after a recent split. If no replica is the
	// lease holder, all replicas must campaign to avoid waiting for
	// an election timeout to acquire the lease. In the latter case,
	// there's less chance of an election storm because this replica
	// will only campaign if it's been idle for >= election timeout,
	// so there's most likely been no traffic to the range.
	shouldCampaignOnCreation := r.mu.state.Lease.OwnedBy(r.store.StoreID()) ||
		r.leaseStatus(*r.mu.state.Lease, r.store.Clock().Now(), r.mu.minLeaseProposedTS).State !=
			LeaseState_VALID
	if err := r.withRaftGroupLocked(shouldCampaignOnCreation, func(raftGroup *raft.RawNode) (bool, error) {
		return true, nil
	}); err != nil {
		log.VErrEventf(ctx, 1, "unable to initialize raft group: %s", err)
	}
}

// Send executes a command on this range, dispatching it to the
// read-only, read-write, or admin execution path as appropriate.
// ctx should contain the log tags from the store (and up).
func (r *Replica) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	var br *roachpb.BatchResponse
	if r.leaseholderStats != nil && ba.Header.GatewayNodeID != 0 {
		r.leaseholderStats.record(ba.Header.GatewayNodeID)
	}

	// Add the range log tag.
	ctx = r.AnnotateCtx(ctx)
	ctx, cleanup := tracing.EnsureContext(ctx, r.AmbientContext.Tracer, "replica send")
	defer cleanup()

	// If the internal Raft group is not initialized, create it and wake the leader.
	r.maybeInitializeRaftGroup(ctx)

	isReadOnly := ba.IsReadOnly()
	useRaft := !isReadOnly && ba.IsWrite()

	if isReadOnly && r.store.Clock().MaxOffset() == timeutil.ClocklessMaxOffset {
		// Clockless reads mode: reads go through Raft.
		useRaft = true
	}

	if err := r.checkBatchRequest(ba, isReadOnly); err != nil {
		return nil, roachpb.NewError(err)
	}

	if filter := r.store.cfg.TestingKnobs.TestingRequestFilter; filter != nil {
		if pErr := filter(ba); pErr != nil {
			return nil, pErr
		}
	}

	// Differentiate between admin, read-only and write.
	var pErr *roachpb.Error
	if useRaft {
		log.Event(ctx, "read-write path")
		br, pErr = r.executeWriteBatch(ctx, ba)
	} else if isReadOnly {
		log.Event(ctx, "read-only path")
		br, pErr = r.executeReadOnlyBatch(ctx, ba)
	} else if ba.IsAdmin() {
		log.Event(ctx, "admin path")
		br, pErr = r.executeAdminBatch(ctx, ba)
	} else if len(ba.Requests) == 0 {
		// empty batch; shouldn't happen (we could handle it, but it hints
		// at someone doing weird things, and once we drop the key range
		// from the header it won't be clear how to route those requests).
		log.Fatalf(ctx, "empty batch")
	} else {
		log.Fatalf(ctx, "don't know how to handle command %s", ba)
	}
	if pErr != nil {
		if _, ok := pErr.GetDetail().(*roachpb.RaftGroupDeletedError); ok {
			// This error needs to be converted appropriately so that
			// clients will retry.
			pErr = roachpb.NewError(roachpb.NewRangeNotFoundError(r.RangeID))
		}
		log.Eventf(ctx, "replica.Send got error: %s", pErr)
	} else {
		if filter := r.store.cfg.TestingKnobs.TestingResponseFilter; filter != nil {
			pErr = filter(ba, br)
		}
	}
	return br, pErr
}

// requestCanProceed returns an error if a request (identified by its
// key span and timestamp) can proceed. It may be called multiple
// times during the processing of the request (i.e. during both
// proposal and application for write commands).
//
// This is called downstream of raft and therefore should be changed
// only with extreme care. It also accesses replica state that is not
// declared in the SpanSet; this is OK because it can never change the
// evaluation of a batch, only allow or disallow it.
func (r *Replica) requestCanProceed(rspan roachpb.RSpan, ts hlc.Timestamp) error {
	r.mu.Lock()
	desc := r.mu.state.Desc
	threshold := r.mu.state.GCThreshold
	r.mu.Unlock()
	if !threshold.Less(ts) {
		return &roachpb.BatchTimestampBeforeGCError{
			Timestamp: ts,
			Threshold: *threshold,
		}
	}

	if rspan.Key == nil && rspan.EndKey == nil {
		return nil
	}
	if desc.ContainsKeyRange(rspan.Key, rspan.EndKey) {
		return nil
	}

	mismatchErr := roachpb.NewRangeKeyMismatchError(
		rspan.Key.AsRawKey(), rspan.EndKey.AsRawKey(), desc)
	// Try to suggest the correct range on a key mismatch error where
	// even the start key of the request went to the wrong range.
	if !desc.ContainsKey(rspan.Key) {
		if repl := r.store.LookupReplica(rspan.Key, nil); repl != nil {
			// Only return the correct range descriptor as a hint
			// if we know the current lease holder for that range, which
			// indicates that our knowledge is not stale.
			if lease, _ := repl.GetLease(); repl.IsLeaseValid(lease, r.store.Clock().Now()) {
				mismatchErr.SuggestedRange = repl.Desc()
			}
		}
	}
	return mismatchErr
}

// checkBatchRequest verifies BatchRequest validity requirements. In
// particular, timestamp, user, user priority and transactions must
// all be set to identical values between the batch request header and
// all constituent batch requests. Also, either all requests must be
// read-only, or none.
// TODO(tschottdorf): should check that request is contained in range
// and that EndTransaction only occurs at the very end.
func (r *Replica) checkBatchRequest(ba roachpb.BatchRequest, isReadOnly bool) error {
	consistent := ba.ReadConsistency == roachpb.CONSISTENT
	if isReadOnly {
		if !consistent && ba.Txn != nil {
			// Disallow any inconsistent reads within txns.
			return errors.Errorf("cannot allow %v reads within a transaction", ba.ReadConsistency)
		}
	} else if !consistent {
		return errors.Errorf("%v mode is only available to reads", ba.ReadConsistency)
	}

	return nil
}

// batchCmdSet holds a *cmd for each permutation of SpanAccess and spanScope. The
// batch is divided into separate *cmds for access type (read-only or read/write)
// and key scope (local or global; used to facilitate use by the separate local
// and global command queues).
type batchCmdSet [spanset.NumSpanAccess][spanset.NumSpanScope]*cmd

// endCmds holds necessary information to end a batch after Raft
// command processing.
type endCmds struct {
	repl *Replica
	cmds batchCmdSet
	ba   roachpb.BatchRequest
}

// done removes pending commands from the command queue and updates
// the timestamp cache using the final timestamp of each command.
func (ec *endCmds) done(br *roachpb.BatchResponse, pErr *roachpb.Error, retry proposalRetryReason) {
	// Update the timestamp cache if the command is not being
	// retried. Each request is considered in turn; only those marked as
	// affecting the cache are processed. Inconsistent reads are
	// excluded.
	if retry == proposalNoRetry && ec.ba.ReadConsistency == roachpb.CONSISTENT {
		ec.repl.updateTimestampCache(&ec.ba, br, pErr)
	}

	if fn := ec.repl.store.cfg.TestingKnobs.OnCommandQueueAction; fn != nil {
		fn(&ec.ba, storagebase.CommandQueueFinishExecuting)
	}
	ec.repl.removeCmdsFromCommandQueue(ec.cmds)
}

// updateTimestampCache updates the timestamp cache in order to set a low water
// mark for the timestamp at which mutations to keys overlapping the provided
// request can write, such that they don't re-write history.
func (r *Replica) updateTimestampCache(
	ba *roachpb.BatchRequest, br *roachpb.BatchResponse, pErr *roachpb.Error,
) {
	readOnlyUseReadCache := true
	if r.store.Clock().MaxOffset() == timeutil.ClocklessMaxOffset {
		// Clockless mode: all reads count as writes.
		readOnlyUseReadCache = false
	}

	tc := r.store.tsCache
	// Update the timestamp cache using the timestamp at which the batch
	// was executed. Note this may have moved forward from ba.Timestamp,
	// as when the request is retried locally on WriteTooOldErrors.
	ts := ba.Timestamp
	if br != nil {
		ts = br.Timestamp
	}
	var txnID uuid.UUID
	if ba.Txn != nil {
		txnID = ba.Txn.ID
	}
	for i, union := range ba.Requests {
		args := union.GetInner()
		if roachpb.UpdatesTimestampCache(args) {
			// Skip update if there's an error and it's not for this index
			// or the request doesn't update the timestamp cache on errors.
			if pErr != nil {
				if index := pErr.Index; !roachpb.UpdatesTimestampCacheOnError(args) ||
					index == nil || int32(i) != index.Index {
					continue
				}
			}
			header := args.Header()
			start, end := header.Key, header.EndKey
			switch t := args.(type) {
			case *roachpb.EndTransactionRequest:
				// EndTransaction adds the transaction key to the write
				// timestamp cache to ensure replays create a transaction
				// record with WriteTooOld set.
				key := keys.TransactionKey(start, txnID)
				tc.Add(key, nil, ts, txnID, false /* readCache */)
			case *roachpb.ConditionalPutRequest:
				if pErr != nil {
					// ConditionalPut still updates on ConditionFailedErrors.
					if _, ok := pErr.GetDetail().(*roachpb.ConditionFailedError); !ok {
						continue
					}
				}
				tc.Add(start, end, ts, txnID, readOnlyUseReadCache)
			case *roachpb.ScanRequest:
				resp := br.Responses[i].GetInner().(*roachpb.ScanResponse)
				if resp.ResumeSpan != nil {
					// Note that for forward scan, the resume span will start at
					// the (last key read).Next(), which is actually the correct
					// end key for the span to update the timestamp cache.
					end = resp.ResumeSpan.Key
				}
				tc.Add(start, end, ts, txnID, readOnlyUseReadCache)
			case *roachpb.ReverseScanRequest:
				resp := br.Responses[i].GetInner().(*roachpb.ReverseScanResponse)
				if resp.ResumeSpan != nil {
					// Note that for reverse scans, the resume span's end key is
					// an open interval. That means it was read as part of this op
					// and won't be read on resume. It is the correct start key for
					// the span to update the timestamp cache.
					start = resp.ResumeSpan.EndKey
				}
				tc.Add(start, end, ts, txnID, readOnlyUseReadCache)
			case *roachpb.RefreshRequest:
				tc.Add(start, end, ts, txnID, !t.Write /* readCache */)
			case *roachpb.RefreshRangeRequest:
				tc.Add(start, end, ts, txnID, !t.Write /* readCache */)
			default:
				readCache := readOnlyUseReadCache
				if roachpb.UpdatesWriteTimestampCache(args) {
					readCache = false
				}
				tc.Add(start, end, ts, txnID, readCache)
			}
		}
	}
}

func collectSpans(
	desc roachpb.RangeDescriptor, ba *roachpb.BatchRequest,
) (*spanset.SpanSet, error) {
	spans := &spanset.SpanSet{}
	// TODO(bdarnell): need to make this less global when the local
	// command queue is used more heavily. For example, a split will
	// have a large read-only span but also a write (see #10084).
	// Currently local spans are the exception, so preallocate for the
	// common case in which all are global. We rarely mix read and
	// write commands, so preallocate for writes if there are any
	// writes present in the batch.
	//
	// TODO(bdarnell): revisit as the local portion gets its appropriate
	// use.
	if ba.IsReadOnly() {
		spans.Reserve(spanset.SpanReadOnly, spanset.SpanGlobal, len(ba.Requests))
	} else {
		spans.Reserve(spanset.SpanReadWrite, spanset.SpanGlobal, len(ba.Requests))
	}

	for _, union := range ba.Requests {
		inner := union.GetInner()
		if _, ok := inner.(*roachpb.NoopRequest); ok {
			continue
		}
		if cmd, ok := batcheval.LookupCommand(inner.Method()); ok {
			cmd.DeclareKeys(desc, ba.Header, inner, spans)
		} else {
			return nil, errors.Errorf("unrecognized command %s", inner.Method())
		}
	}

	// If any command gave us spans that are invalid, bail out early
	// (before passing them to the command queue, which may panic).
	if err := spans.Validate(); err != nil {
		return nil, err
	}
	return spans, nil
}

// beginCmds waits for any overlapping, already-executing commands via
// the command queue and adds itself to queues based on keys affected by the
// batched commands. This gates subsequent commands with overlapping keys or
// key ranges. This method will block if there are any overlapping commands
// already in the queue. Returns a cleanup function to be called when the
// commands are done and can be removed from the queue, and whose returned
// error is to be used in place of the supplied error.
func (r *Replica) beginCmds(
	ctx context.Context, ba *roachpb.BatchRequest, spans *spanset.SpanSet,
) (*endCmds, error) {
	var newCmds batchCmdSet
	clocklessReads := r.store.Clock().MaxOffset() == timeutil.ClocklessMaxOffset
	// Don't use the command queue for inconsistent reads.
	if ba.ReadConsistency == roachpb.CONSISTENT {

		// Check for context cancellation before inserting into the
		// command queue (and check again afterward). Once we're in the
		// command queue we'll need to transfer our prerequisites to all
		// dependent commands if we want to cancel, so it's good to bail
		// out early if we can.
		if err := ctx.Err(); err != nil {
			log.VEventf(ctx, 2, "%s before command queue: %s", err, ba.Summary())
			return nil, err
		}

		// Get the requested timestamp for a given scope. This is used for
		// non-interference of earlier reads with later writes, but only for
		// the global command queue. Reads and writes to local keys are specified
		// as having a zero timestamp which will cause them to always interfere.
		// This is done to avoid confusion with local keys declared as part of
		// proposer evaluated KV.
		scopeTS := func(scope spanset.SpanScope) hlc.Timestamp {
			switch scope {
			case spanset.SpanGlobal:
				// ba.Timestamp is always set appropriately, regardless of
				// whether the batch is transactional or not.
				return ba.Timestamp
			case spanset.SpanLocal:
				return hlc.Timestamp{}
			}
			panic(fmt.Sprintf("unexpected scope %d", scope))
		}

		r.cmdQMu.Lock()
		var prereqs [spanset.NumSpanAccess][spanset.NumSpanScope][]*cmd
		var prereqCount int
		// Collect all the channels to wait on before adding this batch to the
		// command queue.
		for i := spanset.SpanAccess(0); i < spanset.NumSpanAccess; i++ {
			// With clockless reads, everything is treated as writing.
			readOnly := i == spanset.SpanReadOnly && !clocklessReads
			for j := spanset.SpanScope(0); j < spanset.NumSpanScope; j++ {
				prereqs[i][j] = r.cmdQMu.queues[j].getPrereqs(readOnly, scopeTS(j), spans.GetSpans(i, j))
				prereqCount += len(prereqs[i][j])
			}
		}
		for i := spanset.SpanAccess(0); i < spanset.NumSpanAccess; i++ {
			readOnly := i == spanset.SpanReadOnly && !clocklessReads // ditto above
			for j := spanset.SpanScope(0); j < spanset.NumSpanScope; j++ {
				newCmds[i][j] = r.cmdQMu.queues[j].add(readOnly, scopeTS(j), prereqs[i][j], spans.GetSpans(i, j))
			}
		}
		r.cmdQMu.Unlock()

		ctxDone := ctx.Done()
		beforeWait := timeutil.Now()
		if prereqCount > 0 {
			log.Eventf(ctx, "waiting for %d overlapping requests", prereqCount)
		}
		if fn := r.store.cfg.TestingKnobs.OnCommandQueueAction; fn != nil {
			fn(ba, storagebase.CommandQueueWaitForPrereqs)
		}

		for _, accessCmds := range newCmds {
			for _, newCmd := range accessCmds {
				// If newCmd is nil it means that the BatchRequest contains no spans for this
				// SpanAccess/spanScope permutation.
				if newCmd == nil {
					continue
				}
				// Loop until the command has no more pending prerequisites. Resolving canceled
				// prerequisites can add new transitive dependencies to a command, so newCmd.prereqs
				// should not be accessed directly (see ResolvePendingPrereq).
				for {
					pre := newCmd.PendingPrereq()
					if pre == nil {
						break
					}
					select {
					case <-pre.pending:
						// The prerequisite command has finished so remove it from our prereq list.
						// If the prereq still has pending dependencies, migrate them.
						newCmd.ResolvePendingPrereq()
					case <-ctxDone:
						err := ctx.Err()
						log.VEventf(ctx, 2, "%s while in command queue: %s", err, ba)

						// Remove the command from the command queue immediately. Dependents will
						// transfer transitive dependencies when they try to block on this command,
						// because our prereqs slice is not empty. This migration of dependencies
						// will happen for each dependent in ResolvePendingPrereq, which will notice
						// that our prereqs slice was not empty when we stopped pending and will
						// adopt our prerequisites in turn. New commands that would have established
						// a dependency on this command will never see it, which is fine.
						if fn := r.store.cfg.TestingKnobs.OnCommandQueueAction; fn != nil {
							fn(ba, storagebase.CommandQueueCancellation)
						}
						r.removeCmdsFromCommandQueue(newCmds)
						return nil, err
					case <-r.store.stopper.ShouldQuiesce():
						// While shutting down, commands may have been added to the
						// command queue that will never finish.
						return nil, &roachpb.NodeUnavailableError{}
					}
				}
			}
		}

		if prereqCount > 0 {
			log.Eventf(ctx, "waited %s for overlapping requests", timeutil.Since(beforeWait))
		}
		if fn := r.store.cfg.TestingKnobs.OnCommandQueueAction; fn != nil {
			fn(ba, storagebase.CommandQueueBeginExecuting)
		}
	} else {
		log.Event(ctx, "operation accepts inconsistent results")
	}

	// Update the incoming timestamp if unset. Wait until after any
	// preceding command(s) for key range are complete so that the node
	// clock has been updated to the high water mark of any commands
	// which might overlap this one in effect.
	// TODO(spencer,tschottdorf): might remove this, but harder than it looks.
	//   This isn't just unittests (which would require revamping the test
	//   context sender), but also some of the scanner queues place batches
	//   directly into the local range they're servicing.
	if ba.Timestamp == (hlc.Timestamp{}) {
		if txn := ba.Txn; txn != nil {
			ba.Timestamp = txn.OrigTimestamp
		} else {
			ba.Timestamp = r.store.Clock().Now()
		}
	}

	ec := &endCmds{
		repl: r,
		cmds: newCmds,
		ba:   *ba,
	}
	return ec, nil
}

// removeCmdsFromCommandQueue removes a batch's set of commands for the
// replica's command queue.
func (r *Replica) removeCmdsFromCommandQueue(cmds batchCmdSet) {
	r.cmdQMu.Lock()
	for _, accessCmds := range cmds {
		for scope, cmd := range accessCmds {
			r.cmdQMu.queues[scope].remove(cmd)
		}
	}
	r.cmdQMu.Unlock()
}

// applyTimestampCache moves the batch timestamp forward depending on
// the presence of overlapping entries in the timestamp cache. If the
// batch is transactional, the txn timestamp and the txn.WriteTooOld
// bool are updated.
//
// Two important invariants of Cockroach: 1) encountering a more
// recently written value means transaction restart. 2) values must
// be written with a greater timestamp than the most recent read to
// the same key. Check the timestamp cache for reads/writes which
// are at least as recent as the timestamp of this write. The cmd must
// update its timestamp to be greater than more recent values in the
// timestamp cache. When the write returns, the updated timestamp
// will inform the batch response timestamp or batch response txn
// timestamp.
func (r *Replica) applyTimestampCache(
	ctx context.Context, ba *roachpb.BatchRequest,
) (bool, *roachpb.Error) {
	var bumped bool
	for _, union := range ba.Requests {
		args := union.GetInner()
		if roachpb.ConsultsTimestampCache(args) {
			header := args.Header()
			// BeginTransaction is a special case. We use the transaction
			// key to look for an entry which would indicate this transaction
			// has already been finalized, in which case this is a replay.
			if _, ok := args.(*roachpb.BeginTransactionRequest); ok {
				key := keys.TransactionKey(header.Key, ba.Txn.ID)
				wTS, wTxnID := r.store.tsCache.GetMaxWrite(key, nil)
				// GetMaxWrite will only find a timestamp interval with an
				// associated txnID on the TransactionKey if an EndTxnReq has
				// been processed. All other timestamp intervals will have no
				// associated txnID and will be due to the low-water mark.
				switch wTxnID {
				case ba.Txn.ID:
					return bumped, roachpb.NewError(roachpb.NewTransactionReplayError())
				case uuid.UUID{} /* noTxnID */ :
					if !wTS.Less(ba.Txn.Timestamp) {
						// This is a crucial bit of code. The timestamp cache is
						// reset with the current time as the low-water mark, so
						// if this replica recently obtained the lease, this
						// case will be true for new txns, even if they're not a
						// replay. We move the timestamp forward and return
						// retry. If it's really a replay, it won't retry.
						txn := ba.Txn.Clone()
						bumped = txn.Timestamp.Forward(wTS.Next()) || bumped
						err := roachpb.NewTransactionRetryError(roachpb.RETRY_POSSIBLE_REPLAY)
						return bumped, roachpb.NewErrorWithTxn(err, &txn)
					}
				default:
					log.Fatalf(ctx, "unexpected tscache interval (%s,%s) on TxnKey %s",
						wTS, wTxnID, key)
				}
				continue
			}

			// Forward the timestamp if there's been a more recent read (by someone else).
			rTS, rTxnID := r.store.tsCache.GetMaxRead(header.Key, header.EndKey)
			if ba.Txn != nil {
				if ba.Txn.ID != rTxnID {
					nextTS := rTS.Next()
					if ba.Txn.Timestamp.Less(nextTS) {
						txn := ba.Txn.Clone()
						bumped = txn.Timestamp.Forward(nextTS) || bumped
						ba.Txn = &txn
					}
				}
			} else {
				bumped = ba.Timestamp.Forward(rTS.Next()) || bumped
			}

			// On more recent writes, forward the timestamp and set the
			// write too old boolean for transactions. Note that currently
			// only EndTransaction and DeleteRange requests update the
			// write timestamp cache.
			wTS, wTxnID := r.store.tsCache.GetMaxWrite(header.Key, header.EndKey)
			if ba.Txn != nil {
				if ba.Txn.ID != wTxnID {
					if !wTS.Less(ba.Txn.Timestamp) {
						txn := ba.Txn.Clone()
						bumped = txn.Timestamp.Forward(wTS.Next()) || bumped
						txn.WriteTooOld = true
						ba.Txn = &txn
					}
				}
			} else {
				bumped = ba.Timestamp.Forward(wTS.Next()) || bumped
			}
		}
	}
	return bumped, nil
}

// executeAdminBatch executes the command directly. There is no interaction
// with the command queue or the timestamp cache, as admin commands
// are not meant to consistently access or modify the underlying data.
// Admin commands must run on the lease holder replica. Batch support here is
// limited to single-element batches; everything else catches an error.
func (r *Replica) executeAdminBatch(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	if len(ba.Requests) != 1 {
		return nil, roachpb.NewErrorf("only single-element admin batches allowed")
	}

	rSpan, err := keys.Range(ba)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	if err := r.requestCanProceed(rSpan, ba.Timestamp); err != nil {
		return nil, roachpb.NewError(err)
	}

	args := ba.Requests[0].GetInner()
	if sp := opentracing.SpanFromContext(ctx); sp != nil {
		sp.SetOperationName(reflect.TypeOf(args).String())
	}

	// Admin commands always require the range lease.
	_, pErr := r.redirectOnOrAcquireLease(ctx)
	if pErr != nil {
		return nil, pErr
	}
	// Note there is no need to limit transaction max timestamp on admin requests.

	var resp roachpb.Response
	switch tArgs := args.(type) {
	case *roachpb.AdminSplitRequest:
		var reply roachpb.AdminSplitResponse
		reply, pErr = r.AdminSplit(ctx, *tArgs)
		resp = &reply

	case *roachpb.AdminMergeRequest:
		var reply roachpb.AdminMergeResponse
		reply, pErr = r.AdminMerge(ctx, *tArgs)
		resp = &reply

	case *roachpb.AdminTransferLeaseRequest:
		pErr = roachpb.NewError(r.AdminTransferLease(ctx, tArgs.Target))
		resp = &roachpb.AdminTransferLeaseResponse{}

	case *roachpb.AdminChangeReplicasRequest:
		var err error
		for _, target := range tArgs.Targets {
			err = r.ChangeReplicas(
				ctx, tArgs.ChangeType, target, r.Desc(), ReasonAdminRequest, "")
			if err != nil {
				break
			}
		}
		pErr = roachpb.NewError(err)
		resp = &roachpb.AdminChangeReplicasResponse{}

	case *roachpb.CheckConsistencyRequest:
		var reply roachpb.CheckConsistencyResponse
		reply, pErr = r.CheckConsistency(ctx, *tArgs)
		resp = &reply

	case *roachpb.ImportRequest:
		cArgs := batcheval.CommandArgs{
			EvalCtx: NewReplicaEvalContext(r, &spanset.SpanSet{}),
			Header:  ba.Header,
			Args:    args,
		}
		var err error
		resp, err = importCmdFn(ctx, cArgs)
		pErr = roachpb.NewError(err)

	case *roachpb.AdminScatterRequest:
		reply, err := r.adminScatter(ctx, *tArgs)
		pErr = roachpb.NewError(err)
		resp = &reply

	default:
		return nil, roachpb.NewErrorf("unrecognized admin command: %T", args)
	}

	if pErr != nil {
		return nil, pErr
	}
	br := &roachpb.BatchResponse{}
	br.Add(resp)
	br.Txn = resp.Header().Txn
	return br, nil
}

// limitTxnMaxTimestamp limits the batch transaction's max timestamp
// so that it respects any timestamp already observed on this node.
// This prevents unnecessary uncertainty interval restarts caused by
// reading a value written at a timestamp between txn.Timestamp and
// txn.MaxTimestamp. The replica lease's start time is also taken into
// consideration to ensure that a lease transfer does not result in
// the observed timestamp for this node being inapplicable to data
// previously written by the former leaseholder. To wit:
//
// 1. put(k on leaseholder n1), gateway chooses t=1.0
// 2. begin; read(unrelated key on n2); gateway chooses t=0.98
// 3. pick up observed timestamp for n2 of t=0.99
// 4. n1 transfers lease for range with k to n2 @ t=1.1
// 5. read(k) on leaseholder n2 at OrigTimestamp=0.98 should get
//    ReadWithinUncertaintyInterval because of the write in step 1, so
//    even though we observed n2's timestamp in step 3 we must expand
//    the uncertainty interval to the lease's start time, which is
//    guaranteed to be greater than any write which occurred under
//    the previous leaseholder.
func (r *Replica) limitTxnMaxTimestamp(
	ctx context.Context, ba *roachpb.BatchRequest, status LeaseStatus,
) {
	if ba.Txn == nil {
		return
	}
	// For calls that read data within a txn, we keep track of timestamps
	// observed from the various participating nodes' HLC clocks. If we have
	// a timestamp on file for this Node which is smaller than MaxTimestamp,
	// we can lower MaxTimestamp accordingly. If MaxTimestamp drops below
	// OrigTimestamp, we effectively can't see uncertainty restarts anymore.
	obsTS, ok := ba.Txn.GetObservedTimestamp(ba.Replica.NodeID)
	if !ok {
		return
	}
	// If the lease is valid, we use the greater of the observed
	// timestamp and the lease start time, up to the max timestamp. This
	// ensures we avoid incorrect assumptions about when data was
	// written, in absolute time on a different node, which held the
	// lease before this replica acquired it.
	if status.State == LeaseState_VALID {
		obsTS.Forward(status.Lease.Start)
	}
	if obsTS.Less(ba.Txn.MaxTimestamp) {
		// Copy-on-write to protect others we might be sharing the Txn with.
		shallowTxn := *ba.Txn
		// The uncertainty window is [OrigTimestamp, maxTS), so if that window
		// is empty, there won't be any uncertainty restarts.
		if !ba.Txn.OrigTimestamp.Less(obsTS) {
			log.Event(ctx, "read has no clock uncertainty")
		}
		shallowTxn.MaxTimestamp.Backward(obsTS)
		ba.Txn = &shallowTxn
	}
}

// executeReadOnlyBatch updates the read timestamp cache and waits for any
// overlapping writes currently processing through Raft ahead of us to
// clear via the command queue.
func (r *Replica) executeReadOnlyBatch(
	ctx context.Context, ba roachpb.BatchRequest,
) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	// If the read is not inconsistent, the read requires the range lease.
	var status LeaseStatus
	if ba.ReadConsistency.RequiresReadLease() {
		if status, pErr = r.redirectOnOrAcquireLease(ctx); pErr != nil {
			return nil, pErr
		}
	}
	r.limitTxnMaxTimestamp(ctx, &ba, status)

	spans, err := collectSpans(*r.Desc(), &ba)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	// Add the read to the command queue to gate subsequent
	// overlapping commands until this command completes.
	log.Event(ctx, "command queue")
	endCmds, err := r.beginCmds(ctx, &ba, spans)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	log.Event(ctx, "waiting for read lock")
	r.readOnlyCmdMu.RLock()
	defer r.readOnlyCmdMu.RUnlock()

	// Guarantee we remove the commands from the command queue. It is
	// important that this is inside the readOnlyCmdMu lock so that the
	// timestamp cache update is synchronized. This is wrapped to delay
	// pErr evaluation to its value when returning.
	defer func() {
		endCmds.done(br, pErr, proposalNoRetry)
	}()

	if _, err := r.IsDestroyed(); err != nil {
		return nil, roachpb.NewError(err)
	}

	rSpan, err := keys.Range(ba)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	if err := r.requestCanProceed(rSpan, ba.Timestamp); err != nil {
		return nil, roachpb.NewError(err)
	}

	// Evaluate read-only batch command. It checks for matching key range; note
	// that holding readOnlyCmdMu throughout is important to avoid reads from the
	// "wrong" key range being served after the range has been split.
	var result result.Result
	rec := NewReplicaEvalContext(r, spans)
	readOnly := r.store.Engine().NewReadOnly()
	if util.RaceEnabled {
		readOnly = spanset.NewReadWriter(readOnly, spans)
	}
	defer readOnly.Close()
	br, result, pErr = evaluateBatch(ctx, storagebase.CmdIDKey(""), readOnly, rec, nil, ba)

	if intents := result.Local.DetachIntents(); len(intents) > 0 {
		log.Eventf(ctx, "submitting %d intents to asynchronous processing", len(intents))
		// We only allow synchronous intent resolution for consistent requests.
		// Intent resolution is async/best-effort for inconsistent requests.
		//
		// An important case where this logic is necessary is for RangeLookup
		// requests. In their case, synchronous intent resolution can deadlock
		// if the request originated from the local node which means the local
		// range descriptor cache has an in-flight RangeLookup request which
		// prohibits any concurrent requests for the same range. See #17760.
		allowSyncProcessing := ba.ReadConsistency == roachpb.CONSISTENT
		if err := r.store.intentResolver.processIntentsAsync(ctx, r, intents, allowSyncProcessing); err != nil {
			log.Warning(ctx, err)
		}
	}
	if pErr != nil {
		log.VErrEvent(ctx, 3, pErr.String())
	} else {
		log.Event(ctx, "read completed")
	}
	return br, pErr
}

// executeWriteBatch is the entry point for client requests which may mutate the
// range's replicated state. Requests taking this path are ultimately
// serialized through Raft, but pass through additional machinery whose goal is
// to allow commands which commute to be proposed in parallel. The naive
// alternative, submitting requests to Raft one after another, paying massive
// latency, is only taken for commands whose effects may overlap.
//
// Internally, multiple iterations of the above process may take place
// due to the Raft proposal failing retryably, possibly due to proposal
// reordering or re-proposals.
func (r *Replica) executeWriteBatch(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	var ambiguousResult bool
	for count := 0; ; count++ {
		br, pErr, retry := r.tryExecuteWriteBatch(ctx, ba)
		switch retry {
		case proposalIllegalLeaseIndex:
			continue // retry
		case proposalAmbiguousShouldBeReevaluated:
			ambiguousResult = true
			continue // retry
		case proposalRangeNoLongerExists, proposalErrorReproposing:
			ambiguousResult = true
		}
		if pErr != nil {
			// If this is a transactional request but doesn't include an
			// EndTransaction with commit=true, return error immediately.
			if etArg, ok := ba.GetArg(roachpb.EndTransaction); ba.Txn != nil &&
				(!ok || !etArg.(*roachpb.EndTransactionRequest).Commit) {
				return nil, pErr
			}
			// If we've gotten an indication of possible ambiguous result,
			// we must return an AmbiguousResultError to prevent callers
			// from retrying thinking this batch could not have succeeded.
			//
			// TODO(spencer): add metrics for how often the re-proposed
			// commands succeed and how often we return errors.
			if _, ok := pErr.GetDetail().(*roachpb.AmbiguousResultError); !ok && ambiguousResult {
				log.VEventf(ctx, 2, "received error after %d retries; returning ambiguous result: %s",
					count, pErr)
				are := roachpb.NewAmbiguousResultError(
					fmt.Sprintf("Raft re-proposal failed: %s", pErr))
				are.WrappedErr = pErr
				return nil, roachpb.NewError(are)
			}
		}
		return br, pErr
	}
}

// tryExecuteWriteBatch is invoked by executeWriteBatch, which will
// call this method until it returns a non-retryable result. Retries
// may happen if either the proposal was submitted to Raft but did not
// end up in a legal log position, or the proposal was submitted to
// Raft and then was re-proposed. On re-proposals, the proposal may
// have applied successfully and so the caller must be careful to
// indicate an ambiguous result to the caller in the event
// proposalReproposed is returned.
//
// Concretely,
//
// - The keys affected by the command are added to the command queue (i.e.
//   tracked as in-flight mutations).
// - Wait until the command queue promises that no overlapping mutations are
//   in flight.
// - The timestamp cache is checked to determine if the command's affected keys
//   were accessed with a timestamp exceeding that of the command; if so, the
//   command's timestamp is incremented accordingly.
// - A RaftCommand is constructed. If proposer-evaluated KV is active,
//   the request is evaluated and the Result is placed in the
//   RaftCommand. If not, the request itself is added to the command.
// - The proposal is inserted into the Replica's in-flight proposals map,
//   a lease index is assigned to it, and it is submitted to Raft, returning
//   a channel.
// - The result of the Raft proposal is read from the channel and the command
//   registered with the timestamp cache, removed from the command queue, and
//   its result (which could be an error) returned to the client.
//
// TODO(tschottdorf): take special care with "special" commands and their
// reorderings. For example, a batch of writes and a split could be in flight
// in parallel without overlap, but if the writes hit the RHS, something must
// prevent them from writing outside of the key range when they apply.
//
// NB: changing BatchRequest to a pointer here would have to be done cautiously
// as this method makes the assumption that it operates on a shallow copy (see
// call to applyTimestampCache).
func (r *Replica) tryExecuteWriteBatch(
	ctx context.Context, ba roachpb.BatchRequest,
) (br *roachpb.BatchResponse, pErr *roachpb.Error, retry proposalRetryReason) {
	startTime := timeutil.Now()

	if err := r.maybeBackpressureWriteBatch(ctx, ba); err != nil {
		return nil, roachpb.NewError(err), proposalNoRetry
	}

	spans, err := collectSpans(*r.Desc(), &ba)
	if err != nil {
		return nil, roachpb.NewError(err), proposalNoRetry
	}

	var endCmds *endCmds
	if !ba.IsLeaseRequest() {
		// Add the write to the command queue to gate subsequent overlapping
		// commands until this command completes. Note that this must be
		// done before getting the max timestamp for the key(s), as
		// timestamp cache is only updated after preceding commands have
		// been run to successful completion.
		log.Event(ctx, "command queue")
		var err error
		endCmds, err = r.beginCmds(ctx, &ba, spans)
		if err != nil {
			return nil, roachpb.NewError(err), proposalNoRetry
		}
	}

	// Guarantee we remove the commands from the command queue. This is
	// wrapped to delay pErr evaluation to its value when returning.
	defer func() {
		if endCmds != nil {
			endCmds.done(br, pErr, retry)
		}
	}()

	var lease roachpb.Lease
	var status LeaseStatus
	// For lease commands, use the provided previous lease for verification.
	if ba.IsSingleSkipLeaseCheckRequest() {
		lease = ba.GetPrevLeaseForLeaseRequest()
	} else {
		// Other write commands require that this replica has the range
		// lease.
		if status, pErr = r.redirectOnOrAcquireLease(ctx); pErr != nil {
			return nil, pErr, proposalNoRetry
		}
		lease = status.Lease
	}
	r.limitTxnMaxTimestamp(ctx, &ba, status)

	// Examine the read and write timestamp caches for preceding
	// commands which require this command to move its timestamp
	// forward. Or, in the case of a transactional write, the txn
	// timestamp and possible write-too-old bool.
	if bumped, pErr := r.applyTimestampCache(ctx, &ba); pErr != nil {
		return nil, pErr, proposalNoRetry
	} else if bumped {
		// If we bump the transaction's timestamp, we must absolutely
		// tell the client in a response transaction (for otherwise it
		// doesn't know about the incremented timestamp). Response
		// transactions are set far away from this code, but at the time
		// of writing, they always seem to be set. Since that is a
		// likely target of future micro-optimization, this assertion is
		// meant to protect against future correctness anomalies.
		defer func() {
			if br != nil && ba.Txn != nil && br.Txn == nil {
				log.Fatalf(ctx, "assertion failed: transaction updated by "+
					"timestamp cache, but transaction returned in response; "+
					"updated timestamp would have been lost (recovered): "+
					"%s in batch %s", ba.Txn, ba,
				)
			}
		}()
	}

	log.Event(ctx, "applied timestamp cache")

	ch, tryAbandon, undoQuotaAcquisition, pErr := r.propose(ctx, lease, ba, endCmds, spans)
	defer func() {
		// NB: We may be double free-ing here, consider the following cases:
		//  - The request was evaluated and the command resulted in an error, but a
		//    proposal is still sent.
		//  - Proposals get duplicated.
		// To counter this our quota pool is capped at the initial quota size.
		if pErr != nil {
			undoQuotaAcquisition()
		}
	}()
	if pErr != nil {
		return nil, pErr, proposalNoRetry
	}

	// After the command is proposed to Raft, invoking endCmds.done is now the
	// responsibility of processRaftCommand.
	endCmds = nil

	// If the command was accepted by raft, wait for the range to apply it.
	ctxDone := ctx.Done()
	shouldQuiesce := r.store.stopper.ShouldQuiesce()
	slowTimer := timeutil.NewTimer()
	defer slowTimer.Stop()
	slowTimer.Reset(base.SlowRequestThreshold)
	for {
		select {
		case propResult := <-ch:
			// Semi-synchronously process any intents that need resolving here in
			// order to apply back pressure on the client which generated them. The
			// resolution is semi-synchronous in that there is a limited number of
			// outstanding asynchronous resolution tasks allowed after which
			// further calls will block.
			if len(propResult.Intents) > 0 {
				// TODO(peter): Re-proposed and canceled (but executed) commands can
				// both leave intents to GC that don't hit this code path. No good
				// solution presents itself at the moment and such intents will be
				// resolved on reads.
				if err := r.store.intentResolver.processIntentsAsync(ctx, r, propResult.Intents, true /* allowSync */); err != nil {
					log.Warning(ctx, err)
				}
			}
			if len(propResult.EndTxns) > 0 {
				if err := r.store.intentResolver.cleanupTxnIntentsAsync(ctx, r, propResult.EndTxns, true /* allowSync */); err != nil {
					log.Warning(ctx, err)
				}
			}
			return propResult.Reply, propResult.Err, propResult.ProposalRetry
		case <-slowTimer.C:
			slowTimer.Read = true
			log.Warningf(ctx, "have been waiting %s for proposing command %s",
				base.SlowRequestThreshold, ba)
			r.store.metrics.SlowRaftRequests.Inc(1)
			defer r.store.metrics.SlowRaftRequests.Dec(1)

		case <-ctxDone:
			// If our context was canceled, return an AmbiguousResultError
			// if the command isn't already being executed and using our
			// context, in which case we expect it to finish soon. The
			// AmbiguousResultError indicates to caller that the command may
			// have executed.
			if tryAbandon() {
				log.VEventf(ctx, 2, "context cancellation after %0.1fs of attempting command %s",
					timeutil.Since(startTime).Seconds(), ba)
				return nil, roachpb.NewError(roachpb.NewAmbiguousResultError(ctx.Err().Error())), proposalNoRetry
			}
			ctxDone = nil
		case <-shouldQuiesce:
			// If shutting down, return an AmbiguousResultError if the
			// command isn't already being executed and using our context,
			// in which case we expect it to finish soon. AmbiguousResultError
			// indicates to caller that the command may have executed. If
			// tryAbandon fails, we iterate through the loop again to wait
			// for the command to finish.
			if tryAbandon() {
				log.VEventf(ctx, 2, "shutdown cancellation after %0.1fs of attempting command %s",
					timeutil.Since(startTime).Seconds(), ba)
				return nil, roachpb.NewError(roachpb.NewAmbiguousResultError("server shutdown")), proposalNoRetry
			}
			shouldQuiesce = nil
		}
	}
}

// requestToProposal converts a BatchRequest into a ProposalData, by
// evaluating it. The returned ProposalData is partially valid even
// on a non-nil *roachpb.Error and should be proposed through Raft
// if ProposalData.command is non-nil.
func (r *Replica) requestToProposal(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	ba roachpb.BatchRequest,
	endCmds *endCmds,
	spans *spanset.SpanSet,
) (*ProposalData, *roachpb.Error) {
	res, needConsensus, pErr := r.evaluateProposal(ctx, idKey, ba, spans)

	// Fill out the results even if pErr != nil; we'll return the error below.
	proposal := &ProposalData{
		ctx:     ctx,
		idKey:   idKey,
		endCmds: endCmds,
		doneCh:  make(chan proposalResult, 1),
		Local:   &res.Local,
		Request: &ba,
	}

	if needConsensus {
		proposal.command = &storagebase.RaftCommand{
			ReplicatedEvalResult: res.Replicated,
			WriteBatch:           res.WriteBatch,
		}
		if r.store.TestingKnobs().EvalKnobs.TestingEvalFilter != nil {
			// For backwards compatibility, tests that use TestingEvalFilter
			// need the original request to be preserved. See #10493
			proposal.command.TestingBatchRequest = &ba
		}
	}

	return proposal, pErr
}

// evaluateProposal generates a Result from the given request by
// evaluating it, returning both state which is held only on the
// proposer and that which is to be replicated through Raft. The
// return value is ready to be inserted into Replica's proposal map
// and subsequently passed to submitProposalLocked.
//
// The method also returns a flag indicating if the request needs to
// be proposed through Raft and replicated. This flag will be false
// either if the request was a no-op or if it hit an error. In this
// case, the result can be sent directly back to the client without
// going through Raft, but while still handling LocalEvalResult.
//
// Replica.mu must not be held.
func (r *Replica) evaluateProposal(
	ctx context.Context, idKey storagebase.CmdIDKey, ba roachpb.BatchRequest, spans *spanset.SpanSet,
) (*result.Result, bool, *roachpb.Error) {
	if ba.Timestamp == (hlc.Timestamp{}) {
		return nil, false, roachpb.NewErrorf("can't propose Raft command with zero timestamp")
	}

	// Evaluate the commands. If this returns without an error, the batch should
	// be committed. Note that we don't hold any locks at this point. This is
	// important since evaluating a proposal is expensive.
	// TODO(tschottdorf): absorb all returned values in `res` below this point
	// in the call stack as well.
	batch, ms, br, res, pErr := r.evaluateWriteBatch(ctx, idKey, ba, spans)

	// Note: reusing the proposer's batch when applying the command on the
	// proposer was explored as an optimization but resulted in no performance
	// benefit.
	defer batch.Close()

	if pErr != nil {
		pErr = r.maybeSetCorrupt(ctx, pErr)

		// Restore the original txn's Writing bool if the error specifies
		// a transaction.
		if txn := pErr.GetTxn(); txn != nil {
			if ba.Txn == nil {
				log.Fatalf(ctx, "error had a txn but batch is non-transactional. Err txn: %s", txn)
			}
			if txn.ID == ba.Txn.ID {
				txn.Writing = ba.Txn.Writing
			}
		}

		// Failed proposals can't have any Result except for what's
		// whitelisted here.
		intents := res.Local.DetachIntents()
		endTxns := res.Local.DetachEndTxns(true /* alwaysOnly */)
		res.Local = result.LocalResult{
			Intents:            &intents,
			EndTxns:            &endTxns,
			LeaseMetricsResult: res.Local.LeaseMetricsResult,
		}
		res.Replicated.Reset()
		return &res, false /* needConsensus */, pErr
	}

	// Set the local reply, which is held only on the proposing replica and is
	// returned to the client after the proposal completes, or immediately if
	// replication is not necessary.
	res.Local.Reply = br

	// needConsensus determines if the result needs to be replicated and
	// proposed through Raft. This is necessary if at least one of the
	// following conditions is true:
	// 1. the request created a non-empty write batch.
	// 2. the request had an impact on the MVCCStats. NB: this is possible
	//    even with an empty write batch when stats are recomputed.
	// 3. the request has replicated side-effects.
	// 4. the cluster is in "clockless" mode, in which case consensus is
	//    used to enforce a linearization of all reads and writes.
	needConsensus := !batch.Empty() ||
		ms != (enginepb.MVCCStats{}) ||
		!res.Replicated.Equal(storagebase.ReplicatedEvalResult{}) ||
		r.store.Clock().MaxOffset() == timeutil.ClocklessMaxOffset

	if needConsensus {
		// Set the proposal's WriteBatch, which is the serialized representation of
		// the proposals effect on RocksDB.
		res.WriteBatch = &storagebase.WriteBatch{
			Data: batch.Repr(),
		}

		// Set the proposal's replicated result, which contains metadata and
		// side-effects that are to be replicated to all replicas.
		res.Replicated.IsLeaseRequest = ba.IsLeaseRequest()
		res.Replicated.Timestamp = ba.Timestamp
		if r.store.cfg.Settings.Version.IsActive(cluster.VersionMVCCNetworkStats) {
			res.Replicated.Delta = ms.ToNetworkStats()
		} else {
			res.Replicated.DeprecatedDelta = &ms
		}
		// If the cluster version is and always will be VersionNoRaftProposalKeys or
		// greater, we don't need to send the key range through Raft. This decision
		// is based on the minimum supported version and not the active version
		// because Raft entries need to be usable even across allowable version
		// downgrades.
		if !r.ClusterSettings().Version.IsMinSupported(cluster.VersionNoRaftProposalKeys) {
			rSpan, err := keys.Range(ba)
			if err != nil {
				return &res, false /* needConsensus */, roachpb.NewError(err)
			}
			res.Replicated.DeprecatedStartKey = rSpan.Key
			res.Replicated.DeprecatedEndKey = rSpan.EndKey
		}
	}

	return &res, needConsensus, nil
}

// insertProposalLocked assigns a MaxLeaseIndex to a proposal and adds
// it to the pending map.
func (r *Replica) insertProposalLocked(
	proposal *ProposalData, proposerReplica roachpb.ReplicaDescriptor, proposerLease roachpb.Lease,
) {
	// Assign a lease index. Note that we do this as late as possible
	// to make sure (to the extent that we can) that we don't assign
	// (=predict) the index differently from the order in which commands are
	// proposed (and thus likely applied).
	if r.mu.lastAssignedLeaseIndex < r.mu.state.LeaseAppliedIndex {
		r.mu.lastAssignedLeaseIndex = r.mu.state.LeaseAppliedIndex
	}
	if !proposal.Request.IsLeaseRequest() {
		r.mu.lastAssignedLeaseIndex++
	}
	proposal.command.MaxLeaseIndex = r.mu.lastAssignedLeaseIndex
	proposal.command.ProposerReplica = proposerReplica

	// If the proposerLease has a sequence number then we can send this through
	// Raft instead of sending the entire Lease through Raft.
	//
	// NB: We can't check IsMinSupported(VersionLeaseSequence) here because its
	// value may not have been the same when a lease request was evaluated,
	// meaning that we have no guarantee that a new lease isn't being proposed
	// without a sequence number. If we started sending only lease sequence
	// numbers in this case, we wouldn't be able to distinguish the old and the
	// new lease.
	if proposerLease.Sequence != 0 {
		proposal.command.ProposerLeaseSequence = proposerLease.Sequence
	} else {
		proposal.command.DeprecatedProposerLease = &proposerLease
	}

	if log.V(4) {
		log.Infof(proposal.ctx, "submitting proposal %x: maxLeaseIndex=%d",
			proposal.idKey, proposal.command.MaxLeaseIndex)
	}

	if _, ok := r.mu.proposals[proposal.idKey]; ok {
		ctx := r.AnnotateCtx(context.TODO())
		log.Fatalf(ctx, "pending command already exists for %s", proposal.idKey)
	}
	r.mu.proposals[proposal.idKey] = proposal
}

func makeIDKey() storagebase.CmdIDKey {
	idKeyBuf := make([]byte, 0, raftCommandIDLen)
	idKeyBuf = encoding.EncodeUint64Ascending(idKeyBuf, uint64(rand.Int63()))
	return storagebase.CmdIDKey(idKeyBuf)
}

// propose prepares the necessary pending command struct and initializes a
// client command ID if one hasn't been. A verified lease is supplied
// as a parameter if the command requires a lease; nil otherwise. It
// then proposes the command to Raft if necessary and returns
// - a channel which receives a response or error upon application
// - a closure used to attempt to abandon the command. When called, it tries to
//   remove the pending command from the internal commands map. This is
//   possible until execution of the command at the local replica has already
//   begun, in which case false is returned and the client needs to continue
//   waiting for successful execution.
// - a callback to undo quota acquisition if the attempt to propose the batch
//   request to raft fails. This also cleans up the command sizes stored for
//   the corresponding proposal.
// - any error obtained during the creation or proposal of the command, in
//   which case the other returned values are zero.
func (r *Replica) propose(
	ctx context.Context,
	lease roachpb.Lease,
	ba roachpb.BatchRequest,
	endCmds *endCmds,
	spans *spanset.SpanSet,
) (chan proposalResult, func() bool, func(), *roachpb.Error) {
	noop := func() {}

	r.mu.Lock()
	if !r.mu.destroyStatus.IsAlive() {
		err := r.mu.destroyStatus.err
		r.mu.Unlock()
		return nil, nil, noop, roachpb.NewError(err)
	}
	r.mu.Unlock()

	rSpan, err := keys.Range(ba)
	if err != nil {
		return nil, nil, noop, roachpb.NewError(err)
	}

	// Checking the context just before proposing can help avoid ambiguous errors.
	if err := ctx.Err(); err != nil {
		errStr := fmt.Sprintf("%s before proposing: %s", err, ba.Summary())
		log.Warning(ctx, errStr)
		return nil, nil, noop, roachpb.NewError(err)
	}

	// Only need to check that the request is in bounds at proposal time,
	// not at application time, because the command queue will synchronize
	// all requests (notably EndTransaction with SplitTrigger) that may
	// cause this condition to change.
	if err := r.requestCanProceed(rSpan, ba.Timestamp); err != nil {
		return nil, nil, noop, roachpb.NewError(err)
	}

	idKey := makeIDKey()
	proposal, pErr := r.requestToProposal(ctx, idKey, ba, endCmds, spans)
	log.Event(proposal.ctx, "evaluated request")

	// There are two cases where request evaluation does not lead to a Raft
	// proposal:
	// 1. proposal.command == nil indicates that the evaluation was a no-op
	//    and that no Raft command needs to be proposed.
	// 2. pErr != nil corresponds to a failed proposal - the command resulted
	//    in an error.
	if proposal.command == nil {
		intents := proposal.Local.DetachIntents()
		endTxns := proposal.Local.DetachEndTxns(pErr != nil /* alwaysOnly */)
		r.handleLocalEvalResult(ctx, *proposal.Local)

		pr := proposalResult{
			Reply:   proposal.Local.Reply,
			Err:     pErr,
			Intents: intents,
			EndTxns: endTxns,
		}
		proposal.finishApplication(pr)
		return proposal.doneCh, func() bool { return false }, noop, nil
	}

	// TODO(irfansharif): This int cast indicates that if someone configures a
	// very large max proposal size, there is weird overflow behavior and it
	// will not work the way it should.
	proposalSize := proposal.command.Size()
	if proposalSize > int(MaxCommandSize.Get(&r.store.cfg.Settings.SV)) {
		// Once a command is written to the raft log, it must be loaded
		// into memory and replayed on all replicas. If a command is
		// too big, stop it here.
		return nil, nil, noop, roachpb.NewError(errors.Errorf(
			"command is too large: %d bytes (max: %d)",
			proposalSize, MaxCommandSize.Get(&r.store.cfg.Settings.SV),
		))
	}

	if err := r.maybeAcquireProposalQuota(ctx, int64(proposalSize)); err != nil {
		return nil, nil, noop, roachpb.NewError(err)
	}

	// submitProposalLocked calls withRaftGroupLocked which requires that
	// raftMu is held. In order to maintain our lock ordering we need to lock
	// Replica.raftMu here before locking Replica.mu.
	//
	// TODO(peter): It appears that we only need to hold Replica.raftMu when
	// calling raft.NewRawNode. We could get fancier with the locking here to
	// optimize for the common case where Replica.mu.internalRaftGroup is
	// non-nil, but that doesn't currently seem worth it. Always locking raftMu
	// has a tiny (~1%) performance hit for single-node block_writer testing.
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	log.Event(proposal.ctx, "acquired {raft,replica}mu")

	// Add size of proposal to commandSizes map.
	if r.mu.commandSizes != nil {
		r.mu.commandSizes[proposal.idKey] = proposalSize
	}
	undoQuotaAcquisition := func() {
		r.mu.Lock()
		if r.mu.commandSizes != nil && r.mu.proposalQuota != nil {
			delete(r.mu.commandSizes, proposal.idKey)
			r.mu.proposalQuota.add(int64(proposalSize))
		}
		r.mu.Unlock()
	}

	// NB: We need to check Replica.mu.destroyStatus again in case the Replica has
	// been destroyed between the initial check at the beginning of this method
	// and the acquisition of Replica.mu. Failure to do so will leave pending
	// proposals that never get cleared.
	if !r.mu.destroyStatus.IsAlive() {
		return nil, nil, undoQuotaAcquisition, roachpb.NewError(r.mu.destroyStatus.err)
	}

	repDesc, err := r.getReplicaDescriptorRLocked()
	if err != nil {
		return nil, nil, undoQuotaAcquisition, roachpb.NewError(err)
	}
	r.insertProposalLocked(proposal, repDesc, lease)

	if filter := r.store.TestingKnobs().TestingProposalFilter; filter != nil {
		filterArgs := storagebase.ProposalFilterArgs{
			Ctx:   ctx,
			Cmd:   *proposal.command,
			CmdID: idKey,
			Req:   ba,
		}
		if pErr := filter(filterArgs); pErr != nil {
			delete(r.mu.proposals, idKey)
			return nil, nil, undoQuotaAcquisition, pErr
		}
	}

	if err := r.submitProposalLocked(proposal); err == raft.ErrProposalDropped {
		// Silently ignore dropped proposals (they were always silently ignored
		// prior to the introduction of ErrProposalDropped).
		// TODO(bdarnell): Handle ErrProposalDropped better.
		// https://github.com/cockroachdb/cockroach/issues/21849
	} else if err != nil {
		delete(r.mu.proposals, proposal.idKey)
		return nil, nil, undoQuotaAcquisition, roachpb.NewError(err)
	}
	// Must not use `proposal` in the closure below as a proposal which is not
	// present in r.mu.proposals is no longer protected by the mutex. Abandoning
	// a command only abandons the associated context. As soon as we propose a
	// command to Raft, ownership passes to the "below Raft" machinery. In
	// particular, endCmds will be invoked when the command is applied. There are
	// a handful of cases where the command may not be applied (or even
	// processed): the process crashes or the local replica is removed from the
	// range.
	tryAbandon := func() bool {
		r.mu.Lock()
		p, ok := r.mu.proposals[idKey]
		if ok {
			// TODO(radu): Should this context be created via tracer.ForkCtxSpan?
			// We'd need to make sure the span is finished eventually.
			p.ctx = r.AnnotateCtx(context.TODO())
		}
		r.mu.Unlock()
		return ok
	}
	return proposal.doneCh, tryAbandon, undoQuotaAcquisition, nil
}

// submitProposalLocked proposes or re-proposes a command in r.mu.proposals.
// The replica lock must be held.
func (r *Replica) submitProposalLocked(p *ProposalData) error {
	p.proposedAtTicks = r.mu.ticks

	if r.mu.submitProposalFn != nil {
		return r.mu.submitProposalFn(p)
	}
	return defaultSubmitProposalLocked(r, p)
}

func (r *Replica) isSoloReplicaRLocked() bool {
	return len(r.mu.state.Desc.Replicas) == 1 &&
		r.mu.state.Desc.Replicas[0].ReplicaID == r.mu.replicaID
}

func defaultSubmitProposalLocked(r *Replica, p *ProposalData) error {
	data, err := protoutil.Marshal(p.command)
	if err != nil {
		return err
	}
	defer r.store.enqueueRaftUpdateCheck(r.RangeID)

	// Too verbose even for verbose logging, so manually enable if you want to
	// debug proposal sizes.
	if false {
		log.Infof(p.ctx, `%s: proposal: %d
  RaftCommand.ProposerReplica:               %d
  RaftCommand.ReplicatedEvalResult:          %d
  RaftCommand.ReplicatedEvalResult.Delta:    %d
  RaftCommand.WriteBatch:                    %d
`, p.Request.Summary(), len(data),
			p.command.ProposerReplica.Size(),
			p.command.ReplicatedEvalResult.Size(),
			p.command.ReplicatedEvalResult.Delta.Size(),
			p.command.WriteBatch.Size(),
		)
	}

	const largeProposalEventThresholdBytes = 2 << 19 // 512kb

	// Log an event if this is a large proposal. These are more likely to cause
	// blips or worse, and it's good to be able to pick them from traces.
	//
	// TODO(tschottdorf): can we mark them so lightstep can group them?
	if size := len(data); size > largeProposalEventThresholdBytes {
		log.Eventf(p.ctx, "proposal is large: %s", humanizeutil.IBytes(int64(size)))
	}

	if crt := p.command.ReplicatedEvalResult.ChangeReplicas; crt != nil {
		// EndTransactionRequest with a ChangeReplicasTrigger is special
		// because raft needs to understand it; it cannot simply be an
		// opaque command.
		log.Infof(p.ctx, "proposing %s", crt)

		// Ensure that we aren't trying to remove ourselves from the range without
		// having previously given up our lease, since the range won't be able
		// to make progress while the lease is owned by a removed replica (and
		// leases can stay in such a state for a very long time when using epoch-
		// based range leases). This shouldn't happen often, but has been seen
		// before (#12591).
		if crt.ChangeType == roachpb.REMOVE_REPLICA && crt.Replica.ReplicaID == r.mu.replicaID {
			log.Errorf(p.ctx, "received invalid ChangeReplicasTrigger %s to remove self (leaseholder)", crt)
			return errors.Errorf("%s: received invalid ChangeReplicasTrigger %s to remove self (leaseholder)", r, crt)
		}

		confChangeCtx := ConfChangeContext{
			CommandID: string(p.idKey),
			Payload:   data,
			Replica:   crt.Replica,
		}
		encodedCtx, err := protoutil.Marshal(&confChangeCtx)
		if err != nil {
			return err
		}

		return r.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
			// We're proposing a command here so there is no need to wake the
			// leader if we were quiesced.
			r.unquiesceLocked()
			return false, /* unquiesceAndWakeLeader */
				raftGroup.ProposeConfChange(raftpb.ConfChange{
					Type:    changeTypeInternalToRaft[crt.ChangeType],
					NodeID:  uint64(crt.Replica.ReplicaID),
					Context: encodedCtx,
				})
		})
	}

	return r.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
		encode := encodeRaftCommandV1
		if p.command.ReplicatedEvalResult.AddSSTable != nil {
			if p.command.ReplicatedEvalResult.AddSSTable.Data == nil {
				return false, errors.New("cannot sideload empty SSTable")
			}
			encode = encodeRaftCommandV2
			r.store.metrics.AddSSTableProposals.Inc(1)
			log.Event(p.ctx, "sideloadable proposal detected")
		}

		if log.V(4) {
			log.Infof(p.ctx, "proposing command %x: %s", p.idKey, p.Request.Summary())
		}
		// We're proposing a command so there is no need to wake the leader if we
		// were quiesced.
		r.unquiesceLocked()
		return false /* unquiesceAndWakeLeader */, raftGroup.Propose(encode(p.idKey, data))
	})
}

// mark the replica as quiesced. Returns true if the Replica is successfully
// quiesced and false otherwise.
func (r *Replica) quiesce() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.quiesceLocked()
}

func (r *Replica) quiesceLocked() bool {
	ctx := r.AnnotateCtx(context.TODO())
	if len(r.mu.proposals) != 0 {
		log.Infof(ctx, "not quiescing: %d pending commands", len(r.mu.proposals))
		return false
	}
	if !r.mu.quiescent {
		if log.V(3) {
			log.Infof(ctx, "quiescing")
		}
		r.mu.quiescent = true
	} else if log.V(4) {
		log.Infof(ctx, "already quiesced")
	}
	return true
}

func (r *Replica) unquiesceLocked() {
	if r.mu.quiescent {
		if log.V(3) {
			ctx := r.AnnotateCtx(context.TODO())
			log.Infof(ctx, "unquiescing")
		}
		r.mu.quiescent = false
		r.refreshLastUpdateTimeForAllReplicasLocked()
	}
}

func (r *Replica) unquiesceAndWakeLeaderLocked() {
	if r.mu.quiescent {
		if log.V(3) {
			ctx := r.AnnotateCtx(context.TODO())
			log.Infof(ctx, "unquiescing: waking leader")
		}
		r.mu.quiescent = false
		// Propose an empty command which will wake the leader.
		_ = r.mu.internalRaftGroup.Propose(encodeRaftCommandV1(makeIDKey(), nil))
	}
}

// stepRaftGroup calls Step on the replica's RawNode with the provided request's
// message. Before doing so, it assures that the replica is unquiesced and ready
// to handle the request.
func (r *Replica) stepRaftGroup(req *RaftMessageRequest) error {
	return r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
		// We're processing a message from another replica which means that the
		// other replica is not quiesced, so we don't need to wake the leader.
		r.unquiesceLocked()
		r.refreshLastUpdateTimeForReplicaLocked(req.FromReplica.ReplicaID)
		err := raftGroup.Step(req.Message)
		if err == raft.ErrProposalDropped {
			// A proposal was forwarded to this replica but we couldn't propose it.
			// Swallow the error since we don't have an effective way of signaling
			// this to the sender.
			// TODO(bdarnell): Handle ErrProposalDropped better.
			// https://github.com/cockroachdb/cockroach/issues/21849
			err = nil
		}
		return false /* unquiesceAndWakeLeader */, err
	})
}

type handleRaftReadyStats struct {
	processed int
}

// noSnap can be passed to handleRaftReady when no snapshot should be processed.
var noSnap IncomingSnapshot

// handleRaftReady processes a raft.Ready containing entries and messages that
// are ready to read, be saved to stable storage, committed or sent to other
// peers. It takes a non-empty IncomingSnapshot to indicate that it is
// about to process a snapshot.
//
// The returned string is nonzero whenever an error is returned to give a
// non-sensitive cue as to what happened.
func (r *Replica) handleRaftReady(inSnap IncomingSnapshot) (handleRaftReadyStats, string, error) {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	return r.handleRaftReadyRaftMuLocked(inSnap)
}

// handleRaftReadyLocked is the same as handleRaftReady but requires that the
// replica's raftMu be held.
//
// The returned string is nonzero whenever an error is returned to give a
// non-sensitive cue as to what happened.
func (r *Replica) handleRaftReadyRaftMuLocked(
	inSnap IncomingSnapshot,
) (handleRaftReadyStats, string, error) {
	var stats handleRaftReadyStats

	ctx := r.AnnotateCtx(context.TODO())
	var hasReady bool
	var rd raft.Ready
	r.mu.Lock()

	lastIndex := r.mu.lastIndex // used for append below
	lastTerm := r.mu.lastTerm
	raftLogSize := r.mu.raftLogSize
	leaderID := r.mu.leaderID
	lastLeaderID := leaderID

	// We defer the check to Replica.updateProposalQuotaRaftMuLocked because we need
	// to check it in both cases, if hasReady is false and otherwise.
	// If hasReady == false:
	//     Consider the case when our quota is of size 1 and two out of three
	//     replicas have committed one log entry while the third is lagging
	//     behind. When the third replica finally does catch up and sends
	//     along a MsgAppResp, since the entry is already committed on the
	//     leader replica, no Ready is emitted. But given that the third
	//     replica has caught up, we can release
	//     some quota back to the pool.
	// Otherwise:
	//     Consider the case where there are two replicas and we have a quota
	//     of size 1. We acquire the quota when the write gets proposed on the
	//     leader and expect it to be released when the follower commits it
	//     locally. In order to do so we need to have the entry 'come out of
	//     raft' and in the case of a two node raft group, this only happens if
	//     hasReady == true.
	//     If we don't release quota back at the end of
	//     handleRaftReadyRaftMuLocked, the next write will get blocked.
	defer r.updateProposalQuotaRaftMuLocked(ctx, lastLeaderID)

	err := r.withRaftGroupLocked(false, func(raftGroup *raft.RawNode) (bool, error) {
		if hasReady = raftGroup.HasReady(); hasReady {
			rd = raftGroup.Ready()
		}
		return hasReady /* unquiesceAndWakeLeader */, nil
	})
	r.mu.Unlock()
	if err != nil {
		const expl = "while checking raft group for Ready"
		return stats, expl, errors.Wrap(err, expl)
	}

	if !hasReady {
		return stats, "", nil
	}

	logRaftReady(ctx, rd)

	refreshReason := noReason
	if rd.SoftState != nil && leaderID != roachpb.ReplicaID(rd.SoftState.Lead) {
		// Refresh pending commands if the Raft leader has changed. This is usually
		// the first indication we have of a new leader on a restarted node.
		//
		// TODO(peter): Re-proposing commands when SoftState.Lead changes can lead
		// to wasteful multiple-reproposals when we later see an empty Raft command
		// indicating a newly elected leader or a conf change. Replay protection
		// prevents any corruption, so the waste is only a performance issue.
		if log.V(3) {
			log.Infof(ctx, "raft leader changed: %d -> %d", leaderID, rd.SoftState.Lead)
		}
		if !r.store.TestingKnobs().DisableRefreshReasonNewLeader {
			refreshReason = reasonNewLeader
		}
		leaderID = roachpb.ReplicaID(rd.SoftState.Lead)
	}

	if !raft.IsEmptySnap(rd.Snapshot) {
		snapUUID, err := uuid.FromBytes(rd.Snapshot.Data)
		if err != nil {
			const expl = "invalid snapshot id"
			return stats, expl, errors.Wrap(err, expl)
		}
		if inSnap.SnapUUID == (uuid.UUID{}) {
			log.Fatalf(ctx, "programming error: a snapshot application was attempted outside of the streaming snapshot codepath")
		}
		if snapUUID != inSnap.SnapUUID {
			log.Fatalf(ctx, "incoming snapshot id doesn't match raft snapshot id: %s != %s", snapUUID, inSnap.SnapUUID)
		}

		if err := r.applySnapshot(ctx, inSnap, rd.Snapshot, rd.HardState); err != nil {
			const expl = "while applying snapshot"
			return stats, expl, errors.Wrap(err, expl)
		}

		if err := func() error {
			r.store.mu.Lock()
			defer r.store.mu.Unlock()

			if r.store.removePlaceholderLocked(ctx, r.RangeID) {
				atomic.AddInt32(&r.store.counts.filledPlaceholders, 1)
			}
			return r.store.processRangeDescriptorUpdateLocked(ctx, r)
		}(); err != nil {
			const expl = "could not processRangeDescriptorUpdate after applySnapshot"
			return stats, expl, errors.Wrap(err, expl)
		}

		// r.mu.lastIndex, r.mu.lastTerm and r.mu.raftLogSize were updated in
		// applySnapshot, but we also want to make sure we reflect these changes in
		// the local variables we're tracking here.
		r.mu.RLock()
		lastIndex = r.mu.lastIndex
		lastTerm = r.mu.lastTerm
		raftLogSize = r.mu.raftLogSize
		r.mu.RUnlock()

		// We refresh pending commands after applying a snapshot because this
		// replica may have been temporarily partitioned from the Raft group and
		// missed leadership changes that occurred. Suppose node A is the leader,
		// and then node C gets partitioned away from the others. Leadership passes
		// back and forth between A and B during the partition, but when the
		// partition is healed node A is leader again.
		if !r.store.TestingKnobs().DisableRefreshReasonSnapshotApplied &&
			refreshReason == noReason {
			refreshReason = reasonSnapshotApplied
		}
	}

	// Separate the MsgApp messages from all other Raft message types so that we
	// can take advantage of the optimization discussed in the Raft thesis under
	// the section: `10.2.1 Writing to the leader’s disk in parallel`. The
	// optimization suggests that instead of a leader writing new log entries to
	// disk before replicating them to its followers, the leader can instead
	// write the entries to disk in parallel with replicating to its followers
	// and them writing to their disks.
	//
	// Here, we invoke this optimization by:
	// 1. sending all MsgApps.
	// 2. syncing all entries and Raft state to disk.
	// 3. sending all other messages.
	//
	// Since this is all handled in handleRaftReadyRaftMuLocked, we're assured
	// that even though we may sync new entries to disk after sending them in
	// MsgApps to followers, we'll always have them synced to disk before we
	// process followers' MsgAppResps for the corresponding entries because this
	// entire method requires RaftMu to be locked. This is a requirement because
	// etcd/raft does not support commit quorums that do not include the leader,
	// even though the Raft thesis states that this would technically be safe:
	// > The leader may even commit an entry before it has been written to its
	// > own disk, if a majority of followers have written it to their disks;
	// > this is still safe.
	//
	// However, MsgApps are also used to inform followers of committed entries
	// through the Commit index that they contains. Because the optimization
	// sends all MsgApps before syncing to disc, we may send out a commit index
	// in a MsgApp that we have not ourselves written in HardState.Commit. This
	// is ok, because the Commit index can be treated as volatile state, as is
	// supported by raft.MustSync. The Raft thesis corroborates this, stating in
	// section: `3.8 Persisted state and server restarts` that:
	// > Other state variables are safe to lose on a restart, as they can all be
	// > recreated. The most interesting example is the commit index, which can
	// > safely be reinitialized to zero on a restart.
	mgsApps, otherMsgs := splitMsgApps(rd.Messages)
	r.sendRaftMessages(ctx, mgsApps)

	// Use a more efficient write-only batch because we don't need to do any
	// reads from the batch. Any reads are performed via the "distinct" batch
	// which passes the reads through to the underlying DB.
	batch := r.store.Engine().NewWriteOnlyBatch()
	defer batch.Close()

	// We know that all of the writes from here forward will be to distinct keys.
	writer := batch.Distinct()
	prevLastIndex := lastIndex
	if len(rd.Entries) > 0 {
		// All of the entries are appended to distinct keys, returning a new
		// last index.
		thinEntries, sideLoadedEntriesSize, err := r.maybeSideloadEntriesRaftMuLocked(ctx, rd.Entries)
		if err != nil {
			const expl = "during sideloading"
			return stats, expl, errors.Wrap(err, expl)
		}
		raftLogSize += sideLoadedEntriesSize
		if lastIndex, lastTerm, raftLogSize, err = r.append(
			ctx, writer, lastIndex, lastTerm, raftLogSize, thinEntries,
		); err != nil {
			const expl = "during append"
			return stats, expl, errors.Wrap(err, expl)
		}
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := r.raftMu.stateLoader.SetHardState(ctx, writer, rd.HardState); err != nil {
			const expl = "during setHardState"
			return stats, expl, errors.Wrap(err, expl)
		}
	}
	writer.Close()
	// Synchronously commit the batch with the Raft log entries and Raft hard
	// state as we're promising not to lose this data.
	//
	// Note that the data is visible to other goroutines before it is synced to
	// disk. This is fine. The important constraints are that these syncs happen
	// before Raft messages are sent and before the call to RawNode.Advance. Our
	// regular locking is sufficient for this and if other goroutines can see the
	// data early, that's fine. In particular, snapshots are not a problem (I
	// think they're the only thing that might access log entries or HardState
	// from other goroutines). Snapshots do not include either the HardState or
	// uncommitted log entries, and even if they did include log entries that
	// were not persisted to disk, it wouldn't be a problem because raft does not
	// infer the that entries are persisted on the node that sends a snapshot.
	start := timeutil.Now()
	if err := batch.Commit(syncRaftLog.Get(&r.store.cfg.Settings.SV) && rd.MustSync); err != nil {
		const expl = "while committing batch"
		return stats, expl, errors.Wrap(err, expl)
	}
	elapsed := timeutil.Since(start)
	r.store.metrics.RaftLogCommitLatency.RecordValue(elapsed.Nanoseconds())

	if len(rd.Entries) > 0 {
		// We may have just overwritten parts of the log which contain
		// sideloaded SSTables from a previous term (and perhaps discarded some
		// entries that we didn't overwrite). Remove any such leftover on-disk
		// payloads (we can do that now because we've committed the deletion
		// just above).
		firstPurge := rd.Entries[0].Index // first new entry written
		purgeTerm := rd.Entries[0].Term - 1
		lastPurge := prevLastIndex // old end of the log, include in deletion
		for i := firstPurge; i <= lastPurge; i++ {
			err := r.raftMu.sideloaded.Purge(ctx, i, purgeTerm)
			if err != nil && errors.Cause(err) != errSideloadedFileNotFound {
				const expl = "while purging index %d"
				return stats, expl, errors.Wrapf(err, expl, i)
			}
		}
	}

	// Update protected state (last index, last term, raft log size and raft
	// leader ID) and set raft log entry cache. We clear any older, uncommitted
	// log entries and cache the latest ones.
	r.mu.Lock()
	r.store.raftEntryCache.addEntries(r.RangeID, rd.Entries)
	r.mu.lastIndex = lastIndex
	r.mu.lastTerm = lastTerm
	r.mu.raftLogSize = raftLogSize
	r.mu.leaderID = leaderID
	r.mu.Unlock()

	r.sendRaftMessages(ctx, otherMsgs)

	for _, e := range rd.CommittedEntries {
		switch e.Type {
		case raftpb.EntryNormal:
			// Committed entries come straight from the Raft log. Consequently,
			// sideloaded SSTables are not usually inlined.
			if newEnt, err := maybeInlineSideloadedRaftCommand(
				ctx, r.RangeID, e, r.raftMu.sideloaded, r.store.raftEntryCache,
			); err != nil {
				const expl = "maybeInlineSideloadedRaftCommand"
				return stats, expl, errors.Wrap(err, expl)
			} else if newEnt != nil {
				e = *newEnt
			}

			var commandID storagebase.CmdIDKey
			var command storagebase.RaftCommand

			// Process committed entries. etcd raft occasionally adds a nil entry
			// (our own commands are never empty). This happens in two situations:
			// When a new leader is elected, and when a config change is dropped due
			// to the "one at a time" rule. In both cases we may need to resubmit our
			// pending proposals (In the former case we resubmit everything because
			// we proposed them to a former leader that is no longer able to commit
			// them. In the latter case we only need to resubmit pending config
			// changes, but it's hard to distinguish so we resubmit everything
			// anyway). We delay resubmission until after we have processed the
			// entire batch of entries.
			if len(e.Data) == 0 {
				// Overwrite unconditionally since this is the most aggressive
				// reproposal mode.
				refreshReason = reasonNewLeaderOrConfigChange
				commandID = "" // special-cased value, command isn't used
			} else {
				var encodedCommand []byte
				commandID, encodedCommand = DecodeRaftCommand(e.Data)
				// An empty command is used to unquiesce a range and wake the
				// leader. Clear commandID so it's ignored for processing.
				if len(encodedCommand) == 0 {
					commandID = ""
				} else if err := protoutil.Unmarshal(encodedCommand, &command); err != nil {
					const expl = "while unmarshalling entry"
					return stats, expl, errors.Wrap(err, expl)
				}
			}

			if changedRepl := r.processRaftCommand(ctx, commandID, e.Term, e.Index, command); changedRepl {
				log.Fatalf(ctx, "unexpected replication change from command %s", &command)
			}
			r.store.metrics.RaftCommandsApplied.Inc(1)
			stats.processed++

			r.mu.Lock()
			if r.mu.replicaID == r.mu.leaderID {
				// At this point we're not guaranteed to have proposalQuota
				// initialized, the same is true for quotaReleaseQueue and
				// commandSizes. By checking if the specified commandID is
				// present in commandSizes, we'll only queue the cmdSize if
				// they're all initialized.
				if cmdSize, ok := r.mu.commandSizes[commandID]; ok {
					r.mu.quotaReleaseQueue = append(r.mu.quotaReleaseQueue, cmdSize)
					delete(r.mu.commandSizes, commandID)
				}
			}
			r.mu.Unlock()

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := protoutil.Unmarshal(e.Data, &cc); err != nil {
				const expl = "while unmarshaling ConfChange"
				return stats, expl, errors.Wrap(err, expl)
			}
			var ccCtx ConfChangeContext
			if err := protoutil.Unmarshal(cc.Context, &ccCtx); err != nil {
				const expl = "while unmarshaling ConfChangeContext"
				return stats, expl, errors.Wrap(err, expl)

			}
			var command storagebase.RaftCommand
			if err := protoutil.Unmarshal(ccCtx.Payload, &command); err != nil {
				const expl = "while unmarshaling RaftCommand"
				return stats, expl, errors.Wrap(err, expl)
			}
			commandID := storagebase.CmdIDKey(ccCtx.CommandID)
			if changedRepl := r.processRaftCommand(
				ctx, commandID, e.Term, e.Index, command,
			); !changedRepl {
				// If we did not apply the config change, tell raft that the config change was aborted.
				cc = raftpb.ConfChange{}
			}
			stats.processed++

			r.mu.Lock()
			if r.mu.replicaID == r.mu.leaderID {
				if cmdSize, ok := r.mu.commandSizes[commandID]; ok {
					r.mu.quotaReleaseQueue = append(r.mu.quotaReleaseQueue, cmdSize)
					delete(r.mu.commandSizes, commandID)
				}
			}
			r.mu.Unlock()

			if err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
				raftGroup.ApplyConfChange(cc)
				return true, nil
			}); err != nil {
				const expl = "during ApplyConfChange"
				return stats, expl, errors.Wrap(err, expl)
			}
		default:
			log.Fatalf(ctx, "unexpected Raft entry: %v", e)
		}
	}
	if refreshReason != noReason {
		r.mu.Lock()
		r.refreshProposalsLocked(0, refreshReason)
		r.mu.Unlock()
	}

	// TODO(bdarnell): need to check replica id and not Advance if it
	// has changed. Or do we need more locking to guarantee that replica
	// ID cannot change during handleRaftReady?
	const expl = "during advance"
	if err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
		raftGroup.Advance(rd)
		return true, nil
	}); err != nil {
		return stats, expl, errors.Wrap(err, expl)
	}
	return stats, "", nil
}

// splitMsgApps splits the Raft message slice into two slices, one containing
// MsgApps and one containing all other message types. Each slice retains the
// relative ordering between messages in the original slice.
func splitMsgApps(msgs []raftpb.Message) (mgsApps, otherMsgs []raftpb.Message) {
	splitIdx := 0
	for i, msg := range msgs {
		if msg.Type == raftpb.MsgApp {
			msgs[i], msgs[splitIdx] = msgs[splitIdx], msgs[i]
			splitIdx++
		}
	}
	return msgs[:splitIdx], msgs[splitIdx:]
}

func fatalOnRaftReadyErr(ctx context.Context, expl string, err error) {
	// Mimic the behavior in processRaft.
	log.Fatalf(ctx, "%s: %s", log.Safe(expl), err) // TODO(bdarnell)
}

// tick the Raft group, returning any error and true if the raft group exists
// and false otherwise.
func (r *Replica) tick() (bool, error) {
	r.unreachablesMu.Lock()
	remotes := r.unreachablesMu.remotes
	r.unreachablesMu.remotes = nil
	r.unreachablesMu.Unlock()

	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()

	// If the raft group is uninitialized, do not initialize raft groups on
	// tick.
	if r.mu.internalRaftGroup == nil {
		return false, nil
	}

	for remoteReplica := range remotes {
		r.mu.internalRaftGroup.ReportUnreachable(uint64(remoteReplica))
	}

	if r.mu.quiescent {
		// While a replica is quiesced we still advance its logical clock.
		//
		// When CheckQuorum is used (currently when !enablePreVote), this is
		// necessary to avoid a scenario where the leader quiesces and a follower
		// does not. The follower calls an election but the election fails because
		// the leader and other follower believe that no time in the current term
		// has passed. The Raft group is then in a state where one member has a
		// term that is advanced which will then cause subsequent heartbeats from
		// the existing leader to be rejected in a way that the leader will step
		// down. This situation is caused by an interaction between quiescence and
		// the Raft CheckQuorum feature which relies on the logical clock ticking
		// at roughly the same rate on all members of the group.
		//
		// By ticking the logical clock (incrementing an integer) we avoid this
		// situation. If one of the followers does not quiesce it will call an
		// election but the election will succeed. Note that while we expect such
		// elections from quiesced followers to be extremely rare, it is very
		// difficult to completely eliminate them so we want to minimize the
		// disruption when they do occur.
		//
		// Without CheckQuorum, the call to TickQuiesced is less critical,
		// but still improves our responsiveness to node failures by
		// priming the replica to start an election immediately upon
		// unquiescing, instead of waiting for a full election timeout.
		//
		// Enabling TickQuiesced slightly increases the rate of contested
		// elections. Deployments with high inter-node latency and skewed
		// range access patterns may benefit from disabling it.
		//
		// For more details, see #9372 and #16950.
		if enableTickQuiesced {
			r.mu.internalRaftGroup.TickQuiesced()
		}
		return false, nil
	}
	if r.maybeQuiesceLocked() {
		return false, nil
	}

	r.mu.ticks++
	r.mu.internalRaftGroup.Tick()
	if !r.store.TestingKnobs().DisableRefreshReasonTicks &&
		r.mu.ticks%r.store.cfg.RaftElectionTimeoutTicks == 0 {
		// RaftElectionTimeoutTicks is a reasonable approximation of how long we
		// should wait before deciding that our previous proposal didn't go
		// through. Note that the combination of the above condition and passing
		// RaftElectionTimeoutTicks to refreshProposalsLocked means that commands
		// will be refreshed when they have been pending for 1 to 2 election
		// cycles.
		r.refreshProposalsLocked(
			r.store.cfg.RaftElectionTimeoutTicks, reasonTicks,
		)
	}
	return true, nil
}

// maybeTickQuiesced attempts to tick a quiesced or dormant replica, returning
// true on success and false if the regular tick path must be taken
// (i.e. Replica.tick).
func (r *Replica) maybeTickQuiesced() bool {
	var done bool
	r.mu.Lock()
	if r.mu.internalRaftGroup == nil {
		done = true
	} else if r.mu.quiescent {
		done = true
		if enableTickQuiesced {
			// NB: It is safe to call TickQuiesced without holding Replica.raftMu
			// because that method simply increments a counter without performing any
			// other logic.
			r.mu.internalRaftGroup.TickQuiesced()
		}
	}
	r.mu.Unlock()
	return done
}

// maybeQuiesceLocked checks to see if the replica is quiescable and initiates
// quiescence if it is. Returns true if the replica has been quiesced and false
// otherwise.
//
// A quiesced range is not ticked and thus doesn't create MsgHeartbeat requests
// or cause elections. The Raft leader for a range checks various
// pre-conditions: no pending raft commands, no pending raft ready, all of the
// followers are up to date, etc. Quiescence is initiated by a special
// MsgHeartbeat that is tagged as Quiesce. Upon receipt (see
// Store.processRaftRequestWithReplica), the follower checks to see if the
// term/commit matches and marks the local replica as quiescent. If the
// term/commit do not match the MsgHeartbeat is passed through to Raft which
// will generate a MsgHeartbeatResp that will unquiesce the sender.
//
// Any Raft operation on the local replica will unquiesce the Replica. For
// example, a Raft operation initiated on a follower will unquiesce the
// follower which will send a MsgProp to the leader that will unquiesce the
// leader. If the leader of a quiesced range dies, followers will not notice,
// though any request directed to the range will eventually end up on a
// follower which will unquiesce the follower and lead to an election. When a
// follower unquiesces for a reason other than receiving a raft message or
// proposing a raft command (for example the concurrent enqueuing of a tick),
// it wakes the leader by sending an empty message proposal. This avoids
// unnecessary elections due to bugs in which a follower is left unquiesced
// while the leader is quiesced.
//
// Note that both the quiesce and wake-the-leader messages can be dropped or
// reordered by the transport. The wake-the-leader message is termless so it
// won't affect elections and, while it triggers reproprosals that won't cause
// problems on reordering. If the wake-the-leader message is dropped the leader
// won't be woken and the follower will eventually call an election.
//
// If the quiesce message is dropped the follower which missed it will not
// quiesce and will eventually cause an election. The quiesce message is tagged
// with the current term and commit index. If the quiesce message is reordered
// it will either still apply to the recipient or the recipient will have moved
// forward and the quiesce message will fall back to being a heartbeat.
//
// TODO(peter): There remains a scenario in which a follower is left unquiesced
// while the leader is quiesced: the follower's receive queue is full and the
// "quiesce" message is dropped. This seems very very unlikely because if the
// follower isn't keeping up with raft messages it is unlikely that the leader
// would quiesce. The fallout from this situation are undesirable raft
// elections which will cause throughput hiccups to the range, but not
// correctness issues.
//
// TODO(peter): When a node goes down, any range which has a replica on the
// down node will not quiesce. This could be a significant performance
// impact. Additionally, when the node comes back up we want to bring any
// replicas it contains back up to date. Right now this will be handled because
// those ranges never quiesce. One thought for handling both these scenarios is
// to hook into the StorePool and its notion of "down" nodes. But that might
// not be sensitive enough.
func (r *Replica) maybeQuiesceLocked() bool {
	ctx := r.AnnotateCtx(context.TODO())
	status, ok := shouldReplicaQuiesce(ctx, r, r.store.Clock().Now(), len(r.mu.proposals))
	if !ok {
		return false
	}
	return r.quiesceAndNotifyLocked(ctx, status)
}

type quiescer interface {
	raftStatusRLocked() *raft.Status
	raftLastIndexLocked() (uint64, error)
	hasRaftReadyRLocked() bool
	ownsValidLeaseRLocked(ts hlc.Timestamp) bool
	maybeTransferRaftLeader(ctx context.Context, status *raft.Status, ts hlc.Timestamp)
}

func (r *Replica) hasRaftReadyRLocked() bool {
	return r.mu.internalRaftGroup.HasReady()
}

func (r *Replica) maybeTransferRaftLeader(
	ctx context.Context, status *raft.Status, now hlc.Timestamp,
) {
	l := *r.mu.state.Lease
	if !r.isLeaseValidRLocked(l, now) {
		return
	}
	if pr, ok := status.Progress[uint64(l.Replica.ReplicaID)]; ok && pr.Match >= status.Commit {
		log.VEventf(ctx, 1, "transferring raft leadership to replica ID %v", l.Replica.ReplicaID)
		r.store.metrics.RangeRaftLeaderTransfers.Inc(1)
		r.mu.internalRaftGroup.TransferLeader(uint64(l.Replica.ReplicaID))
	}
}

// shouldReplicaQuiesce determines if a replica should be quiesced. All of the
// access to Replica internals are gated by the quiescer interface to
// facilitate testing. Returns the raft.Status and true on success, and (nil,
// false) on failure.
func shouldReplicaQuiesce(
	ctx context.Context, q quiescer, now hlc.Timestamp, numProposals int,
) (*raft.Status, bool) {
	if numProposals != 0 {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: %d pending commands", numProposals)
		}
		return nil, false
	}
	status := q.raftStatusRLocked()
	if status == nil {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: dormant Raft group")
		}
		return nil, false
	}
	if status.SoftState.RaftState != raft.StateLeader {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: not leader")
		}
		return nil, false
	}
	if status.LeadTransferee != 0 {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: leader transfer to %d in progress", status.LeadTransferee)
		}
		return nil, false
	}
	// Only quiesce if this replica is the leaseholder as well;
	// otherwise the replica which is the valid leaseholder may have
	// pending commands which it's waiting on this leader to propose.
	if !q.ownsValidLeaseRLocked(now) {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: not leaseholder")
		}
		// Try to correct leader-not-leaseholder condition, if encountered,
		// assuming the leaseholder is caught up to the commit index.
		//
		// TODO(peter): It is surprising that a method named shouldReplicaQuiesce
		// might initiate transfer of the Raft leadership.
		q.maybeTransferRaftLeader(ctx, status, now)
		return nil, false
	}
	// We need all of Applied, Commit, LastIndex and Progress.Match indexes to be
	// equal in order to quiesce.
	if status.Applied != status.Commit {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: applied (%d) != commit (%d)",
				status.Applied, status.Commit)
		}
		return nil, false
	}
	lastIndex, err := q.raftLastIndexLocked()
	if err != nil {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: %v", err)
		}
		return nil, false
	}
	if status.Commit != lastIndex {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: commit (%d) != lastIndex (%d)",
				status.Commit, lastIndex)
		}
		return nil, false
	}
	var foundSelf bool
	for id, progress := range status.Progress {
		if id == status.ID {
			foundSelf = true
		}
		if progress.Match != status.Applied {
			if log.V(4) {
				log.Infof(ctx, "not quiescing: replica %d match (%d) != applied (%d)",
					id, progress.Match, status.Applied)
			}
			return nil, false
		}
	}
	if !foundSelf {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: %d not found in progress: %+v",
				status.ID, status.Progress)
		}
		return nil, false
	}
	if q.hasRaftReadyRLocked() {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: raft ready")
		}
		return nil, false
	}
	return status, true
}

func (r *Replica) quiesceAndNotifyLocked(ctx context.Context, status *raft.Status) bool {
	fromReplica, fromErr := r.getReplicaDescriptorByIDRLocked(r.mu.replicaID, r.mu.lastToReplica)
	if fromErr != nil {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: cannot find from replica (%d)", r.mu.replicaID)
		}
		return false
	}

	if !r.quiesceLocked() {
		return false
	}
	for id := range status.Progress {
		if roachpb.ReplicaID(id) == r.mu.replicaID {
			continue
		}
		toReplica, toErr := r.getReplicaDescriptorByIDRLocked(
			roachpb.ReplicaID(id), r.mu.lastFromReplica)
		if toErr != nil {
			if log.V(4) {
				log.Infof(ctx, "failed to quiesce: cannot find to replica (%d)", id)
			}
			r.unquiesceLocked()
			return false
		}
		msg := raftpb.Message{
			From:   uint64(r.mu.replicaID),
			To:     id,
			Type:   raftpb.MsgHeartbeat,
			Term:   status.Term,
			Commit: status.Commit,
		}

		if r.maybeCoalesceHeartbeat(ctx, msg, toReplica, fromReplica, true) {
			continue
		}

		req := &RaftMessageRequest{
			RangeID:     r.RangeID,
			ToReplica:   toReplica,
			FromReplica: fromReplica,
			Message:     msg,
			Quiesce:     true,
		}
		if !r.sendRaftMessageRequest(ctx, req) {
			r.unquiesceLocked()
			r.mu.droppedMessages++
			r.mu.internalRaftGroup.ReportUnreachable(id)
			return false
		}
	}
	return true
}

// pendingCmdSlice sorts by increasing MaxLeaseIndex.
type pendingCmdSlice []*ProposalData

func (s pendingCmdSlice) Len() int      { return len(s) }
func (s pendingCmdSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s pendingCmdSlice) Less(i, j int) bool {
	return s[i].command.MaxLeaseIndex < s[j].command.MaxLeaseIndex
}

//go:generate stringer -type refreshRaftReason
type refreshRaftReason int

const (
	noReason refreshRaftReason = iota
	reasonNewLeader
	reasonNewLeaderOrConfigChange
	reasonSnapshotApplied
	reasonReplicaIDChanged
	reasonTicks
)

// refreshProposalsLocked goes through the pending proposals, notifying
// proposers whose proposals need to be retried, and resubmitting proposals
// which were likely dropped (but may still apply at a legal Lease index) -
// ensuring that the proposer will eventually get a reply on the channel it's
// waiting on.
// mu must be held.
//
// refreshAtDelta only applies for reasonTicks and specifies how old (in ticks)
// a command must be for it to be inspected; the usual value is the number of
// ticks of an election timeout (affect only proposals that have had ample time
// to apply but didn't).
func (r *Replica) refreshProposalsLocked(refreshAtDelta int, reason refreshRaftReason) {
	if refreshAtDelta != 0 && reason != reasonTicks {
		log.Fatalf(context.TODO(), "refreshAtDelta specified for reason %s != reasonTicks", reason)
	}

	numShouldRetry := 0
	var reproposals pendingCmdSlice
	for idKey, p := range r.mu.proposals {
		if p.command.MaxLeaseIndex == 0 {
			// Commands without a MaxLeaseIndex cannot be reproposed, as they might
			// apply twice. We also don't want to ask the proposer to retry these
			// special commands.
			delete(r.mu.proposals, idKey)
			log.VEventf(p.ctx, 2, "refresh (reason: %s) returning AmbiguousResultError for command "+
				"without MaxLeaseIndex: %v", reason, p.command)
			p.finishApplication(proposalResult{Err: roachpb.NewError(
				roachpb.NewAmbiguousResultError(
					fmt.Sprintf("unknown status for command without MaxLeaseIndex "+
						"at refreshProposalsLocked time (refresh reason: %s)", reason)))})
		} else if p.command.MaxLeaseIndex <= r.mu.state.LeaseAppliedIndex {
			// The command's designated lease index slot was filled up. We got to
			// LeaseAppliedIndex and p is still pending in r.mu.proposals; generally
			// this means that proposal p didn't commit, and it will be sent back to
			// the proposer for a retry - the request needs to be re-evaluated and the
			// command re-proposed with a new MaxLeaseIndex.
			//
			// An exception is the case when we're refreshing because of
			// reasonSnapshotApplied - in that case we don't know if p or some other
			// command filled the p.command.MaxLeaseIndex slot (i.e. p might have been
			// applied, but we're not watching for every proposal when applying a
			// snapshot, so nobody removed p from r.mu.proposals). In this ambiguous
			// case, we'll also send the command back to the proposer for a retry, but
			// the proposer needs to be aware that, if the retry fails, an
			// AmbiguousResultError needs to be returned to the higher layers.
			// We're relying on the fact that all commands are either idempotent
			// (generally achieved through the wonders of MVCC) or, if they aren't,
			// the 2nd application will somehow fail to apply (e.g. a command
			// resulting from a RequestLease is not idempotent, but the 2nd
			// application will be rejected by the ProposerLease verification).
			//
			// Note that we can't use the commit index here (which is typically a
			// little ahead), because a pending command is removed only as it
			// applies. Thus we'd risk reproposing a command that has been committed
			// but not yet applied.
			delete(r.mu.proposals, idKey)
			log.Eventf(p.ctx, "retry proposal %x: %s", p.idKey, reason)
			if reason == reasonSnapshotApplied {
				p.finishApplication(proposalResult{ProposalRetry: proposalAmbiguousShouldBeReevaluated})
			} else {
				p.finishApplication(proposalResult{ProposalRetry: proposalIllegalLeaseIndex})
			}
			numShouldRetry++
		} else if reason == reasonTicks && p.proposedAtTicks > r.mu.ticks-refreshAtDelta {
			// The command was proposed too recently, don't bother reproprosing it
			// yet.
		} else {
			// The proposal can still apply according to its MaxLeaseIndex. But it's
			// likely that the proposal was dropped, so we're going to repropose it
			// directly; it's cheaper than asking the proposer to re-evaluate and
			// re-propose. Note that we don't need to worry about receiving ambiguous
			// responses for this reproposal - we'll only be handling one response
			// across the original proposal and the reproposal because we're
			// reproposing with the same MaxLeaseIndex and the same idKey as the
			// original (a possible second response for the same idKey would be
			// ignored).
			//
			// This situation happens when we've proposed commands that never made it
			// to the leader - usually because we didn't know who the leader is. When
			// we do discover the leader we need to repropose the command. In local
			// testing, by far the most common reason for these reproposals is the
			// initial leader election after a split.  Specifically, when a range is
			// split when the next command is proposed to the RHS the leader is
			// elected. After the election completes the command is reproposed for
			// both reasonNewLeader and reasonNewLeaderOrConfigChange.
			reproposals = append(reproposals, p)
		}
	}
	if log.V(1) && (numShouldRetry > 0 || len(reproposals) > 0) {
		ctx := r.AnnotateCtx(context.TODO())
		log.Infof(ctx,
			"pending commands: sent %d back to client, reproposing %d (at %d.%d) %s",
			numShouldRetry, len(reproposals), r.mu.state.RaftAppliedIndex,
			r.mu.state.LeaseAppliedIndex, reason)
	}

	// Reproposals are those commands which we weren't able to send back to the
	// client (since we're not sure that another copy of them could apply at
	// the "correct" index). For reproposals, it's generally pretty unlikely
	// that they can make it in the right place. Reproposing in order is
	// definitely required, however.
	//
	// TODO(tschottdorf): evaluate whether `r.mu.proposals` should be a list/slice.
	sort.Sort(reproposals)
	for _, p := range reproposals {
		log.Eventf(p.ctx, "re-submitting command %x to Raft: %s", p.idKey, reason)
		if err := r.submitProposalLocked(p); err == raft.ErrProposalDropped {
			// TODO(bdarnell): Handle ErrProposalDropped better.
			// https://github.com/cockroachdb/cockroach/issues/21849
		} else if err != nil {
			delete(r.mu.proposals, p.idKey)
			p.finishApplication(proposalResult{Err: roachpb.NewError(err), ProposalRetry: proposalErrorReproposing})
		}
	}
}

func (r *Replica) getReplicaDescriptorByIDRLocked(
	replicaID roachpb.ReplicaID, fallback roachpb.ReplicaDescriptor,
) (roachpb.ReplicaDescriptor, error) {
	if repDesc, ok := r.mu.state.Desc.GetReplicaDescriptorByID(replicaID); ok {
		return repDesc, nil
	}
	if fallback.ReplicaID == replicaID {
		return fallback, nil
	}
	return roachpb.ReplicaDescriptor{},
		errors.Errorf("replica %d not present in %v, %v", replicaID, fallback, r.mu.state.Desc.Replicas)
}

// maybeCoalesceHeartbeat returns true if the heartbeat was coalesced and added
// to the appropriate queue.
func (r *Replica) maybeCoalesceHeartbeat(
	ctx context.Context,
	msg raftpb.Message,
	toReplica, fromReplica roachpb.ReplicaDescriptor,
	quiesce bool,
) bool {
	var hbMap map[roachpb.StoreIdent][]RaftHeartbeat
	switch msg.Type {
	case raftpb.MsgHeartbeat:
		r.store.coalescedMu.Lock()
		hbMap = r.store.coalescedMu.heartbeats
	case raftpb.MsgHeartbeatResp:
		r.store.coalescedMu.Lock()
		hbMap = r.store.coalescedMu.heartbeatResponses
	default:
		return false
	}
	beat := RaftHeartbeat{
		RangeID:       r.RangeID,
		ToReplicaID:   toReplica.ReplicaID,
		FromReplicaID: fromReplica.ReplicaID,
		Term:          msg.Term,
		Commit:        msg.Commit,
		Quiesce:       quiesce,
	}
	if log.V(4) {
		log.Infof(ctx, "coalescing beat: %+v", beat)
	}
	toStore := roachpb.StoreIdent{
		StoreID: toReplica.StoreID,
		NodeID:  toReplica.NodeID,
	}
	hbMap[toStore] = append(hbMap[toStore], beat)
	r.store.coalescedMu.Unlock()
	return true
}

func (r *Replica) sendRaftMessages(ctx context.Context, messages []raftpb.Message) {
	var lastAppResp raftpb.Message
	for _, message := range messages {
		drop := false
		switch message.Type {
		case raftpb.MsgApp:
			if util.RaceEnabled {
				// Iterate over the entries to assert that all sideloaded commands
				// are already inlined. replicaRaftStorage.Entries already performs
				// the sideload inlining for stable entries and raft.unstable always
				// contain fat entries. Since these are the only two sources that
				// raft.sendAppend gathers entries from to populate MsgApps, we
				// should never see thin entries here.
				for j := range message.Entries {
					assertSideloadedRaftCommandInlined(ctx, &message.Entries[j])
				}
			}

		case raftpb.MsgAppResp:
			// A successful (non-reject) MsgAppResp contains one piece of
			// information: the highest log index. Raft currently queues up
			// one MsgAppResp per incoming MsgApp, and we may process
			// multiple messages in one handleRaftReady call (because
			// multiple messages may arrive while we're blocked syncing to
			// disk). If we get redundant MsgAppResps, drop all but the
			// last (we've seen that too many MsgAppResps can overflow
			// message queues on the receiving side).
			//
			// Note that this reorders the chosen MsgAppResp relative to
			// other messages (including any MsgAppResps with the Reject flag),
			// but raft is fine with this reordering.
			//
			// TODO(bdarnell): Consider pushing this optimization into etcd/raft.
			// Similar optimizations may be possible for other message types,
			// although MsgAppResp is the only one that has been seen as a
			// problem in practice.
			if !message.Reject && message.Index > lastAppResp.Index {
				lastAppResp = message
				drop = true
			}
		}
		if !drop {
			r.sendRaftMessage(ctx, message)
		}
	}
	if lastAppResp.Index > 0 {
		r.sendRaftMessage(ctx, lastAppResp)
	}
}

// sendRaftMessage sends a Raft message.
func (r *Replica) sendRaftMessage(ctx context.Context, msg raftpb.Message) {
	r.mu.Lock()
	fromReplica, fromErr := r.getReplicaDescriptorByIDRLocked(roachpb.ReplicaID(msg.From), r.mu.lastToReplica)
	toReplica, toErr := r.getReplicaDescriptorByIDRLocked(roachpb.ReplicaID(msg.To), r.mu.lastFromReplica)
	r.mu.Unlock()

	if fromErr != nil {
		log.Warningf(ctx, "failed to look up sender replica %d in r%d while sending %s: %s",
			msg.From, r.RangeID, msg.Type, fromErr)
		return
	}
	if toErr != nil {
		log.Warningf(ctx, "failed to look up recipient replica %d in r%d while sending %s: %s",
			msg.To, r.RangeID, msg.Type, toErr)
		return
	}

	// Raft-initiated snapshots are handled by the Raft snapshot queue.
	if msg.Type == raftpb.MsgSnap {
		if _, err := r.store.raftSnapshotQueue.Add(r, raftSnapshotPriority); err != nil {
			log.Errorf(ctx, "unable to add replica to Raft repair queue: %s", err)
		}
		return
	}

	if r.maybeCoalesceHeartbeat(ctx, msg, toReplica, fromReplica, false) {
		return
	}

	if !r.sendRaftMessageRequest(ctx, &RaftMessageRequest{
		RangeID:     r.RangeID,
		ToReplica:   toReplica,
		FromReplica: fromReplica,
		Message:     msg,
	}) {
		if err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
			r.mu.droppedMessages++
			raftGroup.ReportUnreachable(msg.To)
			return true, nil
		}); err != nil {
			log.Fatal(ctx, err)
		}
	}
}

// addUnreachableRemoteReplica adds the given remote ReplicaID to be reported
// as unreachable on the next tick.
func (r *Replica) addUnreachableRemoteReplica(remoteReplica roachpb.ReplicaID) {
	r.unreachablesMu.Lock()
	if r.unreachablesMu.remotes == nil {
		r.unreachablesMu.remotes = make(map[roachpb.ReplicaID]struct{})
	}
	r.unreachablesMu.remotes[remoteReplica] = struct{}{}
	r.unreachablesMu.Unlock()
}

// sendRaftMessageRequest sends a raft message, returning false if the message
// was dropped. It is the caller's responsibility to call ReportUnreachable on
// the Raft group.
func (r *Replica) sendRaftMessageRequest(ctx context.Context, req *RaftMessageRequest) bool {
	if log.V(4) {
		log.Infof(ctx, "sending raft request %+v", req)
	}

	ok := r.store.cfg.Transport.SendAsync(req)
	// TODO(peter): Looping over all of the outgoing Raft message queues to
	// update this stat on every send is a bit expensive.
	r.store.metrics.RaftEnqueuedPending.Update(r.store.cfg.Transport.queuedMessageCount())
	return ok
}

func (r *Replica) reportSnapshotStatus(ctx context.Context, to roachpb.ReplicaID, snapErr error) {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()

	snapStatus := raft.SnapshotFinish
	if snapErr != nil {
		snapStatus = raft.SnapshotFailure
	}

	if err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
		raftGroup.ReportSnapshot(uint64(to), snapStatus)
		return true, nil
	}); err != nil {
		log.Fatal(ctx, err)
	}
}

func (r *Replica) checkForcedErrLocked(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	raftCmd storagebase.RaftCommand,
	proposal *ProposalData,
	proposedLocally bool,
) (uint64, proposalRetryReason, *roachpb.Error) {
	leaseIndex := r.mu.state.LeaseAppliedIndex

	isLeaseRequest := raftCmd.ReplicatedEvalResult.IsLeaseRequest
	var requestedLease roachpb.Lease
	if isLeaseRequest {
		requestedLease = *raftCmd.ReplicatedEvalResult.State.Lease
	}
	if idKey == "" {
		// This is an empty Raft command (which is sent by Raft after elections
		// to trigger reproposals or during concurrent configuration changes).
		// Nothing to do here except making sure that the corresponding batch
		// (which is bogus) doesn't get executed (for it is empty and so
		// properties like key range are undefined).
		return leaseIndex, proposalNoRetry, roachpb.NewErrorf("no-op on empty Raft entry")
	}

	// Verify the lease matches the proposer's expectation. We rely on
	// the proposer's determination of whether the existing lease is
	// held, and can be used, or is expired, and can be replaced.
	// Verify checks that the lease has not been modified since proposal
	// due to Raft delays / reorderings.
	// To understand why this lease verification is necessary, see comments on the
	// proposer_lease field in the proto.
	leaseMismatch := false
	if raftCmd.DeprecatedProposerLease != nil {
		// VersionLeaseSequence must not have been active when this was proposed.
		//
		// This does not prevent the lease race condition described below. The
		// reason we don't fix this here as well is because fixing the race
		// requires a new cluster version which implies that we'll already be
		// using lease sequence numbers and will fall into the case below.
		leaseMismatch = !raftCmd.DeprecatedProposerLease.Equivalent(*r.mu.state.Lease)
	} else {
		leaseMismatch = raftCmd.ProposerLeaseSequence != r.mu.state.Lease.Sequence
		if !leaseMismatch && isLeaseRequest {
			// Lease sequence numbers are a reflection of lease equivalency
			// between subsequent leases. However, Lease.Equivalent is not fully
			// symmetric, meaning that two leases may be Equivalent to a third
			// lease but not Equivalent to each other. If these leases are
			// proposed under that same third lease, neither will be able to
			// detect whether the other has applied just by looking at the
			// current lease sequence number because neither will will increment
			// the sequence number.
			//
			// This can lead to inversions in lease expiration timestamps if
			// we're not careful. To avoid this, if a lease request's proposer
			// lease sequence matches the current lease sequence and the current
			// lease sequence also matches the requested lease sequence, we make
			// sure the requested lease is Equivalent to current lease.
			if r.mu.state.Lease.Sequence == requestedLease.Sequence {
				// It is only possible for this to fail when expiration-based
				// lease extensions are proposed concurrently.
				leaseMismatch = !r.mu.state.Lease.Equivalent(requestedLease)
			}

			// This is a check to see if the lease we proposed this lease request against is the same
			// lease that we're trying to update. We need to check proposal timestamps because
			// extensions don't increment sequence numbers. Without this check a lease could
			// be extended and then another lease proposed against the original lease would
			// be applied over the extension.
			if raftCmd.ReplicatedEvalResult.PrevLeaseProposal != nil &&
				(*raftCmd.ReplicatedEvalResult.PrevLeaseProposal != *r.mu.state.Lease.ProposedTS) {
				leaseMismatch = true
			}
		}
	}
	if leaseMismatch {
		log.VEventf(
			ctx, 1,
			"command proposed from replica %+v with lease #%d incompatible to %v",
			raftCmd.ProposerReplica, raftCmd.ProposerLeaseSequence, *r.mu.state.Lease,
		)
		if isLeaseRequest {
			// For lease requests we return a special error that
			// redirectOnOrAcquireLease() understands. Note that these
			// requests don't go through the DistSender.
			return leaseIndex, proposalNoRetry, roachpb.NewError(&roachpb.LeaseRejectedError{
				Existing:  *r.mu.state.Lease,
				Requested: requestedLease,
				Message:   "proposed under invalid lease",
			})
		}
		// We return a NotLeaseHolderError so that the DistSender retries.
		nlhe := newNotLeaseHolderError(
			r.mu.state.Lease, raftCmd.ProposerReplica.StoreID, r.mu.state.Desc)
		nlhe.CustomMsg = fmt.Sprintf(
			"stale proposal: command was proposed under lease #%d but is being applied "+
				"under lease: %s", raftCmd.ProposerLeaseSequence, r.mu.state.Lease)
		return leaseIndex, proposalNoRetry, roachpb.NewError(nlhe)
	}

	if isLeaseRequest {
		// Lease commands are ignored by the counter (and their MaxLeaseIndex is ignored). This
		// makes sense since lease commands are proposed by anyone, so we can't expect a coherent
		// MaxLeaseIndex. Also, lease proposals are often replayed, so not making them update the
		// counter makes sense from a testing perspective.
		//
		// However, leases get special vetting to make sure we don't give one to a replica that was
		// since removed (see #15385 and a comment in redirectOnOrAcquireLease).
		if _, ok := r.mu.state.Desc.GetReplicaDescriptor(requestedLease.Replica.StoreID); !ok {
			return leaseIndex, proposalNoRetry, roachpb.NewError(&roachpb.LeaseRejectedError{
				Existing:  *r.mu.state.Lease,
				Requested: requestedLease,
				Message:   "replica not part of range",
			})
		}
	} else if r.mu.state.LeaseAppliedIndex < raftCmd.MaxLeaseIndex {
		// The happy case: the command is applying at or ahead of the minimal
		// permissible index. It's ok if it skips a few slots (as can happen
		// during rearrangement); this command will apply, but later ones which
		// were proposed at lower indexes may not. Overall though, this is more
		// stable and simpler than requiring commands to apply at their exact
		// lease index: Handling the case in which MaxLeaseIndex > oldIndex+1
		// is otherwise tricky since we can't tell the client to try again
		// (reproposals could exist and may apply at the right index, leading
		// to a replay), and assigning the required index would be tedious
		// seeing that it would have to rewind sometimes.
		leaseIndex = raftCmd.MaxLeaseIndex
	} else {
		// The command is trying to apply at a past log position. That's
		// unfortunate and hopefully rare; the client on the proposer will try
		// again. Note that in this situation, the leaseIndex does not advance.
		retry := proposalNoRetry
		if proposedLocally {
			log.VEventf(
				ctx, 1,
				"retry proposal %x: applied at lease index %d, required <= %d",
				proposal.idKey, leaseIndex, raftCmd.MaxLeaseIndex,
			)
			retry = proposalIllegalLeaseIndex
		}
		return leaseIndex, retry, roachpb.NewErrorf(
			"command observed at lease index %d, but required < %d", leaseIndex, raftCmd.MaxLeaseIndex,
		)
	}
	return leaseIndex, proposalNoRetry, nil
}

// processRaftCommand processes a raft command by unpacking the
// command struct to get args and reply and then applying the command
// to the state machine via applyRaftCommand(). The result is sent on
// the command's done channel, if available. As a special case, the
// zero idKey signifies an empty Raft command, which will apply as a
// no-op (without accessing raftCmd), updating only the applied index.
//
// This method returns true if the command successfully applied a
// replica change.
//
// TODO(tschottdorf): once we properly check leases and lease requests etc,
// make sure that the error returned from this method is always populated in
// those cases, as one of the callers uses it to abort replica changes.
func (r *Replica) processRaftCommand(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	term, raftIndex uint64,
	raftCmd storagebase.RaftCommand,
) bool {
	if raftIndex == 0 {
		log.Fatalf(ctx, "processRaftCommand requires a non-zero index")
	}

	if log.V(4) {
		log.Infof(ctx, "processing command %x: maxLeaseIndex=%d", idKey, raftCmd.MaxLeaseIndex)
	}

	var ts hlc.Timestamp
	if idKey != "" {
		ts = raftCmd.ReplicatedEvalResult.Timestamp
	}

	r.mu.Lock()
	proposal, proposedLocally := r.mu.proposals[idKey]

	// TODO(tschottdorf): consider the Trace situation here.
	if proposedLocally {
		// We initiated this command, so use the caller-supplied context.
		ctx = proposal.ctx
		proposal.ctx = nil // avoid confusion
		delete(r.mu.proposals, idKey)
	}

	leaseIndex, proposalRetry, forcedErr := r.checkForcedErrLocked(ctx, idKey, raftCmd, proposal, proposedLocally)

	r.mu.Unlock()

	if forcedErr == nil {
		// Verify that the batch timestamp is after the GC threshold. This is
		// necessary because not all commands declare read access on the GC
		// threshold key, even though they implicitly depend on it. This means
		// that access to this state will not be serialized by the command queue,
		// so we must perform this check upstream and downstream of raft.
		// See #14833.
		//
		// We provide an empty key span because we already know that the Raft
		// command is allowed to apply within its key range. This is guaranteed
		// by checks upstream of Raft, which perform the same validation, and by
		// the CommandQueue, which assures that any modifications to the range's
		// boundaries will be serialized with this command. Finally, the
		// leaseAppliedIndex check in checkForcedErrLocked ensures that replays
		// outside of the CommandQueue's control which break this serialization
		// ordering will already by caught and an error will be thrown.
		forcedErr = roachpb.NewError(r.requestCanProceed(roachpb.RSpan{}, ts))
	}

	// applyRaftCommand will return "expected" errors, but may also indicate
	// replica corruption (as of now, signaled by a replicaCorruptionError).
	// We feed its return through maybeSetCorrupt to act when that happens.
	if forcedErr != nil {
		log.VEventf(ctx, 1, "applying command with forced error: %s", forcedErr)
	} else {
		log.Event(ctx, "applying command")

		if splitMergeUnlock, err := r.maybeAcquireSplitMergeLock(ctx, raftCmd); err != nil {
			log.Eventf(ctx, "unable to acquire split lock: %s", err)
			// Send a crash report because a former bug in the error handling might have
			// been the root cause of #19172.
			_ = r.store.stopper.RunAsyncTask(ctx, "crash report", func(ctx context.Context) {
				log.SendCrashReport(
					ctx,
					&r.store.cfg.Settings.SV,
					0, // depth
					"while acquiring split lock: %s",
					[]interface{}{err},
				)
			})

			forcedErr = roachpb.NewError(err)
		} else if splitMergeUnlock != nil {
			// Close over raftCmd to capture its value at execution time; we clear
			// ReplicatedEvalResult on certain errors.
			defer func() {
				splitMergeUnlock(raftCmd.ReplicatedEvalResult)
			}()
		}
	}

	var response proposalResult
	var writeBatch *storagebase.WriteBatch
	{
		if filter := r.store.cfg.TestingKnobs.TestingApplyFilter; forcedErr == nil && filter != nil {
			forcedErr = filter(storagebase.ApplyFilterArgs{
				CmdID:                idKey,
				ReplicatedEvalResult: raftCmd.ReplicatedEvalResult,
				StoreID:              r.store.StoreID(),
				RangeID:              r.RangeID,
			})
		}

		if forcedErr != nil {
			// Apply an empty entry.
			raftCmd.ReplicatedEvalResult = storagebase.ReplicatedEvalResult{}
			raftCmd.WriteBatch = nil
		}

		// Update the node clock with the serviced request. This maintains
		// a high water mark for all ops serviced, so that received ops without
		// a timestamp specified are guaranteed one higher than any op already
		// executed for overlapping keys.
		r.store.Clock().Update(ts)

		var pErr *roachpb.Error
		if raftCmd.WriteBatch != nil {
			writeBatch = raftCmd.WriteBatch
		}

		if deprecatedDelta := raftCmd.ReplicatedEvalResult.DeprecatedDelta; deprecatedDelta != nil {
			raftCmd.ReplicatedEvalResult.Delta = deprecatedDelta.ToNetworkStats()
			raftCmd.ReplicatedEvalResult.DeprecatedDelta = nil
		}

		// AddSSTable ingestions run before the actual batch. This makes sure
		// that when the Raft command is applied, the ingestion has definitely
		// succeeded. Note that we have taken precautions during command
		// evaluation to avoid having mutations in the WriteBatch that affect
		// the SSTable. Not doing so could result in order reversal (and missing
		// values) here. If the key range we are ingesting into isn't empty,
		// we're not using AddSSTable but a plain WriteBatch.
		if raftCmd.ReplicatedEvalResult.AddSSTable != nil {
			copied := addSSTablePreApply(
				ctx,
				r.store.cfg.Settings,
				r.store.engine,
				r.raftMu.sideloaded,
				term,
				raftIndex,
				*raftCmd.ReplicatedEvalResult.AddSSTable,
				r.store.limiters.BulkIOWriteRate,
			)
			r.store.metrics.AddSSTableApplications.Inc(1)
			if copied {
				r.store.metrics.AddSSTableApplicationCopies.Inc(1)
			}
			raftCmd.ReplicatedEvalResult.AddSSTable = nil
		}

		if raftCmd.ReplicatedEvalResult.Split != nil {
			// Splits require a new HardState to be written to the new RHS
			// range (and this needs to be atomic with the main batch). This
			// cannot be constructed at evaluation time because it differs
			// on each replica (votes may have already been cast on the
			// uninitialized replica). Transform the write batch to add the
			// updated HardState.
			// See https://github.com/cockroachdb/cockroach/issues/20629
			//
			// This is not the most efficient, but it only happens on splits,
			// which are relatively infrequent and don't write much data.
			tmpBatch := r.store.engine.NewBatch()
			if err := tmpBatch.ApplyBatchRepr(writeBatch.Data, false); err != nil {
				log.Fatal(ctx, err)
			}
			splitPreApply(ctx, r.store.cfg.Settings, tmpBatch, raftCmd.ReplicatedEvalResult.Split.SplitTrigger)
			writeBatch.Data = tmpBatch.Repr()
			tmpBatch.Close()
		}

		var delta enginepb.MVCCStats
		{
			var err error
			delta, err = r.applyRaftCommand(
				ctx, idKey, raftCmd.ReplicatedEvalResult, raftIndex, leaseIndex, writeBatch)
			raftCmd.ReplicatedEvalResult.Delta = delta.ToNetworkStats()

			// applyRaftCommand returned an error, which usually indicates
			// either a serious logic bug in CockroachDB or a disk
			// corruption/out-of-space issue. Make sure that these fail with
			// descriptive message so that we can differentiate the root causes.
			if err != nil {
				log.Errorf(ctx, "unable to update the state machine: %s", err)
				// Report the fatal error separately and only with the error, as that
				// triggers an optimization for which we directly report the error to
				// sentry (which in turn allows sentry to distinguish different error
				// types).
				log.Fatal(ctx, err)
			}
		}

		if filter := r.store.cfg.TestingKnobs.TestingPostApplyFilter; pErr == nil && filter != nil {
			pErr = filter(storagebase.ApplyFilterArgs{
				CmdID:                idKey,
				ReplicatedEvalResult: raftCmd.ReplicatedEvalResult,
				StoreID:              r.store.StoreID(),
				RangeID:              r.RangeID,
			})
		}

		// calling maybeSetCorrupt here is mostly for tests and looks. The
		// interesting errors originate in applyRaftCommand, and they are
		// already handled above.
		pErr = r.maybeSetCorrupt(ctx, pErr)
		if pErr == nil {
			pErr = forcedErr
		}

		var lResult *result.LocalResult
		if proposedLocally {
			if proposalRetry != proposalNoRetry {
				response.ProposalRetry = proposalRetry
				if pErr == nil {
					log.Fatalf(ctx, "proposal with nontrivial retry behavior, but no error: %+v", proposal)
				}
			}
			if pErr != nil {
				// A forced error was set (i.e. we did not apply the proposal,
				// for instance due to its log position) or the Replica is now
				// corrupted.
				response.Err = pErr
			} else if proposal.Local.Reply != nil {
				response.Reply = proposal.Local.Reply
			} else {
				log.Fatalf(ctx, "proposal must return either a reply or an error: %+v", proposal)
			}
			response.Intents = proposal.Local.DetachIntents()
			response.EndTxns = proposal.Local.DetachEndTxns(response.Err != nil)
			lResult = proposal.Local
		}

		// Handle the Result, executing any side effects of the last
		// state machine transition.
		//
		// Note that this must happen after committing (the engine.Batch), but
		// before notifying a potentially waiting client.
		r.handleEvalResultRaftMuLocked(ctx, lResult,
			raftCmd.ReplicatedEvalResult, raftIndex, leaseIndex)
	}

	// When set to true, recomputes the stats for the LHS and RHS of splits and
	// makes sure that they agree with the state's range stats.
	const expensiveSplitAssertion = false

	if expensiveSplitAssertion && raftCmd.ReplicatedEvalResult.Split != nil {
		split := raftCmd.ReplicatedEvalResult.Split
		lhsStatsMS := r.GetMVCCStats()
		lhsComputedMS, err := rditer.ComputeStatsForRange(&split.LeftDesc, r.store.Engine(), lhsStatsMS.LastUpdateNanos)
		if err != nil {
			log.Fatal(ctx, err)
		}

		rightReplica, err := r.store.GetReplica(split.RightDesc.RangeID)
		if err != nil {
			log.Fatal(ctx, err)
		}

		rhsStatsMS := rightReplica.GetMVCCStats()
		rhsComputedMS, err := rditer.ComputeStatsForRange(&split.RightDesc, r.store.Engine(), rhsStatsMS.LastUpdateNanos)
		if err != nil {
			log.Fatal(ctx, err)
		}

		if diff := pretty.Diff(lhsStatsMS, lhsComputedMS); len(diff) > 0 {
			log.Fatalf(ctx, "LHS split stats divergence: diff(claimed, computed) = %s", pretty.Diff(lhsStatsMS, lhsComputedMS))
		}
		if diff := pretty.Diff(rhsStatsMS, rhsComputedMS); len(diff) > 0 {
			log.Fatalf(ctx, "RHS split stats divergence diff(claimed, computed) = %s", pretty.Diff(rhsStatsMS, rhsComputedMS))
		}
	}

	if proposedLocally {
		proposal.finishApplication(response)
	} else if response.Err != nil {
		log.VEventf(ctx, 1, "applying raft command resulted in error: %s", response.Err)
	}

	return raftCmd.ReplicatedEvalResult.ChangeReplicas != nil
}

// maybeAcquireSplitMergeLock examines the given raftCmd (which need
// not be evaluated yet) and acquires the split or merge lock if
// necessary (in addition to other preparation). It returns a function
// which will release any lock acquired (or nil) and use the result of
// applying the command to perform any necessary cleanup.
func (r *Replica) maybeAcquireSplitMergeLock(
	ctx context.Context, raftCmd storagebase.RaftCommand,
) (func(storagebase.ReplicatedEvalResult), error) {
	if split := raftCmd.ReplicatedEvalResult.Split; split != nil {
		return r.acquireSplitLock(ctx, &split.SplitTrigger)
	} else if merge := raftCmd.ReplicatedEvalResult.Merge; merge != nil {
		return r.acquireMergeLock(&merge.MergeTrigger)
	}
	return nil, nil
}

func (r *Replica) acquireSplitLock(
	ctx context.Context, split *roachpb.SplitTrigger,
) (func(storagebase.ReplicatedEvalResult), error) {
	rightRng, created, err := r.store.getOrCreateReplica(ctx, split.RightDesc.RangeID, 0, nil)
	if err != nil {
		return nil, err
	}

	// It would be nice to assert that rightRng is not initialized
	// here. Unfortunately, due to reproposals and retries we might be executing
	// a reproposal for a split trigger that was already executed via a
	// retry. The reproposed command will not succeed (the transaction has
	// already committed).
	//
	// TODO(peter): It might be okay to return an error here, but it is more
	// conservative to hit the exact same error paths that we would hit for other
	// commands that have reproposals interacting with retries (i.e. we don't
	// treat splits differently).

	return func(rResult storagebase.ReplicatedEvalResult) {
		if rResult.Split == nil && created && !rightRng.IsInitialized() {
			// An error occurred during processing of the split and the RHS is still
			// uninitialized. Mark the RHS destroyed and remove it from the replica's
			// map as it is likely detritus. One reason this can occur is when
			// concurrent splits on the same key are executed. Only one of the splits
			// will succeed while the other will allocate a range ID, but fail to
			// commit.
			//
			// We condition this removal on whether the RHS was newly created in
			// order to be conservative. If a Raft message had created the Replica
			// then presumably it was alive for some reason other than a concurrent
			// split and shouldn't be destroyed.
			rightRng.mu.Lock()
			rightRng.mu.destroyStatus.Set(errors.Errorf("%s: failed to initialize", rightRng), destroyReasonRemoved)
			rightRng.mu.Unlock()
			r.store.mu.Lock()
			r.store.mu.replicas.Delete(int64(rightRng.RangeID))
			delete(r.store.mu.uninitReplicas, rightRng.RangeID)
			r.store.replicaQueues.Delete(int64(rightRng.RangeID))
			r.store.mu.Unlock()
		}
		rightRng.raftMu.Unlock()
	}, nil
}

func (r *Replica) acquireMergeLock(
	merge *roachpb.MergeTrigger,
) (func(storagebase.ReplicatedEvalResult), error) {
	rightRng, err := r.store.GetReplica(merge.RightDesc.RangeID)
	if err != nil {
		return nil, err
	}

	// TODO(peter,tschottdorf): This is necessary but likely not sufficient. The
	// right hand side of the merge can still race on reads. See #8630.
	rightRng.raftMu.Lock()
	return func(storagebase.ReplicatedEvalResult) {
		rightRng.raftMu.Unlock()
	}, nil
}

// applyRaftCommand applies a raft command from the replicated log to the
// underlying state machine (i.e. the engine). When the state machine can not be
// updated, an error (which is likely fatal!) is returned and must be handled by
// the caller.
func (r *Replica) applyRaftCommand(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	rResult storagebase.ReplicatedEvalResult,
	raftAppliedIndex, leaseAppliedIndex uint64,
	writeBatch *storagebase.WriteBatch,
) (enginepb.MVCCStats, error) {
	if raftAppliedIndex <= 0 {
		return enginepb.MVCCStats{}, errors.New("raft command index is <= 0")
	}
	if writeBatch != nil && len(writeBatch.Data) > 0 {
		// Record the write activity, passing a 0 nodeID because replica.writeStats
		// intentionally doesn't track the origin of the writes.
		mutationCount, err := engine.RocksDBBatchCount(writeBatch.Data)
		if err != nil {
			log.Errorf(ctx, "unable to read header of committed WriteBatch: %s", err)
		} else {
			r.writeStats.recordCount(float64(mutationCount), 0 /* nodeID */)
		}
	}

	r.mu.Lock()
	oldRaftAppliedIndex := r.mu.state.RaftAppliedIndex
	oldLeaseAppliedIndex := r.mu.state.LeaseAppliedIndex

	// Exploit the fact that a split will result in a full stats
	// recomputation to reset the ContainsEstimates flag.
	//
	// TODO(tschottdorf): We want to let the usual MVCCStats-delta
	// machinery update our stats for the left-hand side. But there is no
	// way to pass up an MVCCStats object that will clear out the
	// ContainsEstimates flag. We should introduce one, but the migration
	// makes this worth a separate effort (ContainsEstimates would need to
	// have three possible values, 'UNCHANGED', 'NO', and 'YES').
	// Until then, we're left with this rather crude hack.
	if rResult.Split != nil {
		r.mu.state.Stats.ContainsEstimates = false
	}
	ms := *r.mu.state.Stats
	r.mu.Unlock()

	if raftAppliedIndex != oldRaftAppliedIndex+1 {
		// If we have an out of order index, there's corruption. No sense in
		// trying to update anything or running the command. Simply return
		// a corruption error.
		return enginepb.MVCCStats{}, errors.Errorf("applied index jumped from %d to %d",
			oldRaftAppliedIndex, raftAppliedIndex)
	}

	batch := r.store.Engine().NewWriteOnlyBatch()
	defer batch.Close()

	if writeBatch != nil {
		if err := batch.ApplyBatchRepr(writeBatch.Data, false); err != nil {
			return enginepb.MVCCStats{}, errors.Wrap(err, "unable to apply WriteBatch")
		}
	}

	// The only remaining use of the batch is for range-local keys which we know
	// have not been previously written within this batch. Currently the only
	// remaining writes are the raft applied index and the updated MVCC stats.
	writer := batch.Distinct()

	// Advance the last applied index. We use a blind write in order to avoid
	// reading the previous applied index keys on every write operation. This
	// requires a little additional work in order maintain the MVCC stats.
	var appliedIndexNewMS enginepb.MVCCStats
	if err := r.raftMu.stateLoader.SetAppliedIndexBlind(ctx, writer, &appliedIndexNewMS,
		raftAppliedIndex, leaseAppliedIndex); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "unable to set applied index")
	}
	rResult.Delta.SysBytes += appliedIndexNewMS.SysBytes -
		r.raftMu.stateLoader.CalcAppliedIndexSysBytes(oldRaftAppliedIndex, oldLeaseAppliedIndex)

	// Special-cased MVCC stats handling to exploit commutativity of stats
	// delta upgrades. Thanks to commutativity, the command queue does not
	// have to serialize on the stats key.
	deltaStats := rResult.Delta.ToStats()
	// Note that calling ms.Add will never result in ms.LastUpdateNanos
	// decreasing (and thus LastUpdateNanos tracks the maximum LastUpdateNanos
	// across all deltaStats).
	ms.Add(deltaStats)
	if err := r.raftMu.stateLoader.SetMVCCStats(ctx, writer, &ms); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "unable to update MVCCStats")
	}

	// TODO(peter): We did not close the writer in an earlier version of
	// the code, which went undetected even though we used the batch after
	// (though only to commit it). We should add an assertion to prevent that in
	// the future.
	writer.Close()

	start := timeutil.Now()

	var assertHS *raftpb.HardState
	if util.RaceEnabled && rResult.Split != nil && r.store.cfg.Settings.Version.IsActive(cluster.VersionSplitHardStateBelowRaft) {
		rsl := stateloader.Make(r.store.cfg.Settings, rResult.Split.RightDesc.RangeID)
		oldHS, err := rsl.LoadHardState(ctx, r.store.Engine())
		if err != nil {
			return enginepb.MVCCStats{}, errors.Wrap(err, "unable to load HardState")
		}
		assertHS = &oldHS
	}
	if err := batch.Commit(false); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "could not commit batch")
	}

	if assertHS != nil {
		// Load the HardState that was just committed (if any).
		rsl := stateloader.Make(r.store.cfg.Settings, rResult.Split.RightDesc.RangeID)
		newHS, err := rsl.LoadHardState(ctx, r.store.Engine())
		if err != nil {
			return enginepb.MVCCStats{}, errors.Wrap(err, "unable to load HardState")
		}
		// Assert that nothing moved "backwards".
		if newHS.Term < assertHS.Term || (newHS.Term == assertHS.Term && newHS.Commit < assertHS.Commit) {
			log.Fatalf(ctx, "clobbered HardState: %s\n\npreviously: %s\noverwritten with: %s",
				pretty.Diff(newHS, *assertHS), pretty.Sprint(*assertHS), pretty.Sprint(newHS))
		}
	}

	elapsed := timeutil.Since(start)
	r.store.metrics.RaftCommandCommitLatency.RecordValue(elapsed.Nanoseconds())
	return deltaStats, nil
}

// checkIfTxnAborted checks the txn AbortSpan for the given
// transaction. In case the transaction has been aborted, return a
// transaction abort error.
func checkIfTxnAborted(
	ctx context.Context, rec batcheval.EvalContext, b engine.Reader, txn roachpb.Transaction,
) *roachpb.Error {
	var entry roachpb.AbortSpanEntry
	aborted, err := rec.AbortSpan().Get(ctx, b, txn.ID, &entry)
	if err != nil {
		return roachpb.NewError(NewReplicaCorruptionError(errors.Wrap(err, "could not read from AbortSpan")))
	}
	if aborted {
		// We hit the cache, so let the transaction restart.
		if log.V(1) {
			log.Infof(ctx, "found AbortSpan entry for %s with priority %d",
				txn.ID.Short(), entry.Priority)
		}
		newTxn := txn.Clone()
		if entry.Priority > newTxn.Priority {
			newTxn.Priority = entry.Priority
		}
		return roachpb.NewErrorWithTxn(roachpb.NewTransactionAbortedError(), &newTxn)
	}
	return nil
}

// evaluateWriteBatch evaluates the supplied batch.
//
// If the batch is transactional and has all the hallmarks of a 1PC
// commit (i.e. includes BeginTransaction & EndTransaction, and
// there's nothing to suggest that the transaction will require retry
// or restart), the batch's txn is stripped and it's executed as an
// atomic batch write. If the writes cannot all be completed at the
// intended timestamp, the batch's txn is restored and it's
// re-executed in full. This allows it to lay down intents and return
// an appropriate retryable error.
func (r *Replica) evaluateWriteBatch(
	ctx context.Context, idKey storagebase.CmdIDKey, ba roachpb.BatchRequest, spans *spanset.SpanSet,
) (engine.Batch, enginepb.MVCCStats, *roachpb.BatchResponse, result.Result, *roachpb.Error) {
	ms := enginepb.MVCCStats{}
	// If not transactional or there are indications that the batch's txn will
	// require restart or retry, execute as normal.
	if isOnePhaseCommit(ba, r.store.TestingKnobs()) {
		arg, _ := ba.GetArg(roachpb.EndTransaction)
		etArg := arg.(*roachpb.EndTransactionRequest)

		// Try executing with transaction stripped. We use the transaction timestamp
		// to write any values as it may have been advanced by the timestamp cache.
		strippedBa := ba
		strippedBa.Timestamp = strippedBa.Txn.Timestamp
		strippedBa.Txn = nil
		strippedBa.Requests = ba.Requests[1 : len(ba.Requests)-1] // strip begin/end txn reqs

		// If there were no refreshable spans earlier in a serializable
		// txn (e.g. earlier gets or scans), then the batch can be retried
		// locally in the event of write too old errors.
		retryLocally := ba.Txn.IsSerializable() && etArg.NoRefreshSpans

		// If all writes occurred at the intended timestamp, we've succeeded on the fast path.
		rec := NewReplicaEvalContext(r, spans)
		batch, br, res, pErr := r.evaluateWriteBatchWithLocalRetries(
			ctx, idKey, rec, &ms, strippedBa, spans, retryLocally,
		)
		if pErr == nil && (ba.Timestamp == br.Timestamp ||
			(retryLocally && !isEndTransactionExceedingDeadline(br.Timestamp, *etArg))) {
			clonedTxn := ba.Txn.Clone()
			clonedTxn.Writing = true
			clonedTxn.Status = roachpb.COMMITTED
			// Make sure the returned txn has the actual commit
			// timestamp. This can be different if the stripped batch was
			// executed at the server's hlc now timestamp.
			clonedTxn.Timestamp = br.Timestamp

			// If the end transaction is not committed, clear the batch and mark the status aborted.
			if !etArg.Commit {
				clonedTxn.Status = roachpb.ABORTED
				batch.Close()
				batch = r.store.Engine().NewBatch()
				ms = enginepb.MVCCStats{}
			} else {
				// Run commit trigger manually.
				innerResult, err := runCommitTrigger(ctx, rec, batch, &ms, *etArg, &clonedTxn)
				if err != nil {
					return batch, ms, br, res, roachpb.NewErrorf("failed to run commit trigger: %s", err)
				}
				if err := res.MergeAndDestroy(innerResult); err != nil {
					return batch, ms, br, res, roachpb.NewError(err)
				}
			}

			br.Txn = &clonedTxn
			// Add placeholder responses for begin & end transaction requests.
			br.Responses = append([]roachpb.ResponseUnion{{BeginTransaction: &roachpb.BeginTransactionResponse{}}}, br.Responses...)
			br.Responses = append(br.Responses, roachpb.ResponseUnion{EndTransaction: &roachpb.EndTransactionResponse{OnePhaseCommit: true}})
			return batch, ms, br, res, nil
		}

		ms = enginepb.MVCCStats{}

		// Handle the case of a required one phase commit transaction.
		if etArg.Require1PC {
			if pErr != nil {
				return batch, ms, nil, result.Result{}, pErr
			} else if ba.Timestamp != br.Timestamp {
				err := roachpb.NewTransactionRetryError(roachpb.RETRY_REASON_UNKNOWN)
				return batch, ms, nil, result.Result{}, roachpb.NewError(err)
			}
			log.Fatal(ctx, "unreachable")
		}

		batch.Close()
		log.VEventf(ctx, 2, "1PC execution failed, reverting to regular execution for batch")
	}

	rec := NewReplicaEvalContext(r, spans)
	// We can retry locally if this is a non-transactional request.
	canRetry := ba.Txn == nil
	batch, br, res, pErr := r.evaluateWriteBatchWithLocalRetries(ctx, idKey, rec, &ms, ba, spans, canRetry)
	return batch, ms, br, res, pErr
}

// evaluateWriteBatchWithLocalRetries invokes evaluateBatch and
// retries in the event of a WriteTooOldError at a higher timestamp if
// canRetry is true.
func (r *Replica) evaluateWriteBatchWithLocalRetries(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	rec batcheval.EvalContext,
	ms *enginepb.MVCCStats,
	ba roachpb.BatchRequest,
	spans *spanset.SpanSet,
	canRetry bool,
) (batch engine.Batch, br *roachpb.BatchResponse, res result.Result, pErr *roachpb.Error) {
	for retries := 0; ; retries++ {
		if batch != nil {
			batch.Close()
		}
		batch = r.store.Engine().NewBatch()
		if util.RaceEnabled {
			batch = spanset.NewBatch(batch, spans)
		}
		br, res, pErr = evaluateBatch(ctx, idKey, batch, rec, ms, ba)
		// If we can retry, set a higher batch timestamp and continue.
		if wtoErr, ok := pErr.GetDetail().(*roachpb.WriteTooOldError); ok && canRetry {
			// Allow one retry only; a non-txn batch containing overlapping
			// spans will always experience WriteTooOldError.
			if retries == 1 {
				break
			}
			ba.Timestamp = wtoErr.ActualTimestamp
			continue
		}
		break
	}
	return
}

// isOnePhaseCommit returns true iff the BatchRequest contains all commands in
// the transaction, starting with BeginTransaction and ending with
// EndTransaction. One phase commits are disallowed if (1) the transaction has
// already been flagged with a write too old error, or (2) if isolation is
// serializable and the commit timestamp has been forwarded, or (3) the
// transaction exceeded its deadline, or (4) the testing knobs disallow optional
// one phase commits and the BatchRequest does not require one phase commit.
func isOnePhaseCommit(ba roachpb.BatchRequest, knobs *StoreTestingKnobs) bool {
	if ba.Txn == nil {
		return false
	}
	if _, hasBegin := ba.GetArg(roachpb.BeginTransaction); !hasBegin {
		return false
	}
	arg, hasEnd := ba.GetArg(roachpb.EndTransaction)
	if !hasEnd {
		return false
	}
	etArg := arg.(*roachpb.EndTransactionRequest)
	if isEndTransactionExceedingDeadline(ba.Txn.Timestamp, *etArg) {
		return false
	}
	if retry, _ := isEndTransactionTriggeringRetryError(ba.Txn, *etArg); retry {
		return false
	}
	if ba.Txn != nil && ba.Txn.OrigTimestamp != ba.Txn.Timestamp {
		// Transactions that have been pushed are never eligible for the
		// 1PC path. For serializable transactions this is covered by
		// isEndTransactionTriggeringRetryError, but even snapshot
		// transactions must go through the slow path when their
		// transaction has been pushed. See comments on
		// Transaction.orig_timestamp for the reasons why this is necessary
		// to prevent lost update anomalies.
		return false
	}
	return !knobs.DisableOptional1PC || etArg.Require1PC
}

// optimizePuts searches for contiguous runs of Put & CPut commands in
// the supplied request union. Any run which exceeds a minimum length
// threshold employs a full order iterator to determine whether the
// range of keys being written is empty. If so, then the run can be
// set to put "blindly", meaning no iterator need be used to read
// existing values during the MVCC write.
// The caller should use the returned slice (which is either equal to
// the input slice, or has been shallow-copied appropriately to avoid
// mutating the original requests).
func optimizePuts(
	batch engine.ReadWriter, origReqs []roachpb.RequestUnion, distinctSpans bool,
) []roachpb.RequestUnion {
	var minKey, maxKey roachpb.Key
	var unique map[string]struct{}
	if !distinctSpans {
		unique = make(map[string]struct{}, len(origReqs))
	}
	// Returns false on occurrence of a duplicate key.
	maybeAddPut := func(key roachpb.Key) bool {
		// Note that casting the byte slice key to a string does not allocate.
		if unique != nil {
			if _, ok := unique[string(key)]; ok {
				return false
			}
			unique[string(key)] = struct{}{}
		}
		if minKey == nil || bytes.Compare(key, minKey) < 0 {
			minKey = key
		}
		if maxKey == nil || bytes.Compare(key, maxKey) > 0 {
			maxKey = key
		}
		return true
	}

	firstUnoptimizedIndex := len(origReqs)
	for i, r := range origReqs {
		switch t := r.GetInner().(type) {
		case *roachpb.PutRequest:
			if maybeAddPut(t.Key) {
				continue
			}
		case *roachpb.ConditionalPutRequest:
			if maybeAddPut(t.Key) {
				continue
			}
		case *roachpb.InitPutRequest:
			if maybeAddPut(t.Key) {
				continue
			}
		}
		firstUnoptimizedIndex = i
		break
	}

	if firstUnoptimizedIndex < optimizePutThreshold { // don't bother if below this threshold
		return origReqs
	}
	iter := batch.NewIterator(false /* total order iterator */)
	defer iter.Close()

	// If there are enough puts in the run to justify calling seek,
	// we can determine whether any part of the range being written
	// is "virgin" and set the puts to write blindly.
	// Find the first non-empty key in the run.
	iter.Seek(engine.MakeMVCCMetadataKey(minKey))
	var iterKey roachpb.Key
	if ok, err := iter.Valid(); err != nil {
		// TODO(bdarnell): return an error here instead of silently
		// running without the optimization?
		log.Errorf(context.TODO(), "Seek returned error; disabling blind-put optimization: %s", err)
		return origReqs
	} else if ok && bytes.Compare(iter.Key().Key, maxKey) <= 0 {
		iterKey = iter.Key().Key
	}
	// Set the prefix of the run which is being written to virgin
	// keyspace to "blindly" put values.
	reqs := append([]roachpb.RequestUnion(nil), origReqs...)
	for i := range reqs[:firstUnoptimizedIndex] {
		inner := reqs[i].GetInner()
		if iterKey == nil || bytes.Compare(iterKey, inner.Header().Key) > 0 {
			switch t := inner.(type) {
			case *roachpb.PutRequest:
				shallow := *t
				shallow.Blind = true
				reqs[i].MustSetInner(&shallow)
			case *roachpb.ConditionalPutRequest:
				shallow := *t
				shallow.Blind = true
				reqs[i].MustSetInner(&shallow)
			case *roachpb.InitPutRequest:
				shallow := *t
				shallow.Blind = true
				reqs[i].MustSetInner(&shallow)
			default:
				log.Fatalf(context.TODO(), "unexpected non-put request: %s", t)
			}
		}
	}
	return reqs
}

// evaluateBatch evaluates a batch request by splitting it up into its
// individual commands, passing them to evaluateCommand, and combining
// the results.
func evaluateBatch(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	batch engine.ReadWriter,
	rec batcheval.EvalContext,
	ms *enginepb.MVCCStats,
	ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, result.Result, *roachpb.Error) {
	br := ba.CreateReply()

	maxKeys := int64(math.MaxInt64)
	if ba.Header.MaxSpanRequestKeys != 0 {
		// We have a batch of requests with a limit. We keep track of how many
		// remaining keys we can touch.
		maxKeys = ba.Header.MaxSpanRequestKeys
	}

	// Optimize any contiguous sequences of put and conditional put ops.
	if len(ba.Requests) >= optimizePutThreshold {
		ba.Requests = optimizePuts(batch, ba.Requests, ba.Header.DistinctSpans)
	}

	// Create a shallow clone of the transaction. We only modify a few
	// non-pointer fields (BatchIndex, WriteTooOld, Timestamp), so this saves
	// a few allocs.
	if ba.Txn != nil {
		txnShallow := *ba.Txn
		ba.Txn = &txnShallow

		// Check whether this transaction has been aborted, if applicable.
		// This applies to writes that leave intents (the use of the
		// IsTransactionWrite flag excludes operations like HeartbeatTxn),
		// and reads that occur in a transaction that has already written
		// (see #2231 for more about why we check for aborted transactions
		// on reads). Note that 1PC transactions have had their
		// transaction field cleared by this point so we do not execute
		// this check in that case.
		if ba.IsTransactionWrite() || ba.Txn.Writing {
			// If the request is asking to abort the transaction, then don't check the
			// AbortSpan; we don't want the request to be rejected if the transaction
			// has already been aborted.
			singleAbort := ba.IsSingleEndTransactionRequest() &&
				!ba.Requests[0].GetInner().(*roachpb.EndTransactionRequest).Commit
			if !singleAbort {
				if pErr := checkIfTxnAborted(ctx, rec, batch, *ba.Txn); pErr != nil {
					return nil, result.Result{}, pErr
				}
			}
		}
	}

	var result result.Result
	var writeTooOldErr *roachpb.Error
	returnWriteTooOldErr := true

	for index, union := range ba.Requests {
		// Execute the command.
		args := union.GetInner()
		if ba.Txn != nil {
			ba.Txn.BatchIndex = int32(index)
		}
		// Note that responses are populated even when an error is returned.
		// TODO(tschottdorf): Change that. IIRC there is nontrivial use of it currently.
		reply := br.Responses[index].GetInner()
		curResult, pErr := evaluateCommand(ctx, idKey, index, batch, rec, ms, ba.Header, maxKeys, args, reply)

		if err := result.MergeAndDestroy(curResult); err != nil {
			// TODO(tschottdorf): see whether we really need to pass nontrivial
			// Result up on error and if so, formalize that.
			log.Fatalf(
				ctx,
				"unable to absorb Result: %s\ndiff(new, old): %s",
				err, pretty.Diff(curResult, result),
			)
		}

		if pErr != nil {
			// Initialize the error index.
			pErr.SetErrorIndex(int32(index))

			switch tErr := pErr.GetDetail().(type) {
			case *roachpb.WriteTooOldError:
				// We got a WriteTooOldError. We continue on to run all
				// commands in the batch in order to determine the highest
				// timestamp for more efficient retries. If the batch is
				// transactional, we continue to lay down intents so that
				// other concurrent overlapping transactions are forced
				// through intent resolution and the chances of this batch
				// succeeding when it will be retried are increased.
				if writeTooOldErr != nil {
					writeTooOldErr.GetDetail().(*roachpb.WriteTooOldError).ActualTimestamp.Forward(tErr.ActualTimestamp)
				} else {
					writeTooOldErr = pErr
					// For transactions, we want to swallow the write too old error
					// and just move the transaction timestamp forward and set the
					// WriteTooOld flag. See below for exceptions.
					if ba.Txn != nil {
						returnWriteTooOldErr = false
					}
				}
				// Set the flag to return a WriteTooOldError with the max timestamp
				// encountered evaluating the entire batch on cput and inc requests,
				// with serializable isolation. Because both of these requests must
				// have their keys refreshed on commit with Transaction.WriteTooOld
				// is true, and that refresh will fail, we'd be otherwise guaranteed
				// to do a client-side retry. Returning an error allows a
				// txn-coord-side retry.
				if ba.Txn.IsSerializable() {
					switch args.(type) {
					case *roachpb.ConditionalPutRequest:
						// Conditional puts are an exception. Here, it makes less sense to
						// continue because it's likely that the cput will fail on retry (a
						// newer value is less likely to match the expected value). It's
						// better to return the WriteTooOldError directly, allowing the txn
						// coord sender to retry if it can refresh all other spans encountered
						// already during the transaction, and then, if the cput results in a
						// condition failed error, report that back to the client instead of a
						// retryable error.
						returnWriteTooOldErr = true
					case *roachpb.IncrementRequest:
						// Increments are an exception for similar reasons. If we wait until
						// commit, we'll need a client-side retry, so we return immediately
						// to see if we can do a txn coord sender retry instead.
						returnWriteTooOldErr = true
					case *roachpb.InitPutRequest:
						// Init puts are also an exception. There's no reason to believe they
						// will succeed on a retry, so better to short circuit and return the
						// write too old error.
						returnWriteTooOldErr = true
					}
				}
				if ba.Txn != nil {
					ba.Txn.Timestamp.Forward(tErr.ActualTimestamp)
					ba.Txn.WriteTooOld = true
				}
				// Clear pErr; we're done processing it by having moved the
				// batch or txn timestamps forward and set WriteTooOld if this
				// is a transactional write. The EndTransaction will detect
				// this pushed timestamp and return a TransactionRetryError.
				pErr = nil
			default:
				return nil, result, pErr
			}
		}

		if maxKeys != math.MaxInt64 {
			retResults := reply.Header().NumKeys
			if retResults > maxKeys {
				log.Fatalf(ctx, "received %d results, limit was %d", retResults, maxKeys)
			}
			maxKeys -= retResults
		}

		// If transactional, we use ba.Txn for each individual command and
		// accumulate updates to it.
		// TODO(spencer,tschottdorf): need copy-on-write behavior for the
		//   updated batch transaction / timestamp.
		if ba.Txn != nil {
			if txn := reply.Header().Txn; txn != nil {
				ba.Txn.Update(txn)
			}
		}
	}

	// If there's a write too old error, return now that we've found
	// the high water timestamp for retries.
	if writeTooOldErr != nil && returnWriteTooOldErr {
		return nil, result, writeTooOldErr
	}

	if ba.Txn != nil {
		// If transactional, send out the final transaction entry with the reply.
		br.Txn = ba.Txn
		// If a serializable transaction committed, forward the response
		// timestamp to the commit timestamp in case we were able to
		// optimize and commit at a higher timestamp without higher-level
		// retry (i.e. there were no refresh spans and the commit timestamp
		// wasn't leaked).
		if ba.Txn.IsSerializable() && ba.Txn.Status == roachpb.COMMITTED {
			br.Timestamp.Forward(ba.Txn.Timestamp)
		}
	}
	// Always update the batch response timestamp field to the timestamp at
	// which the batch executed.
	br.Timestamp.Forward(ba.Timestamp)

	return br, result, nil
}

// getLeaseForGossip tries to obtain a range lease. Only one of the replicas
// should gossip; the bool returned indicates whether it's us.
func (r *Replica) getLeaseForGossip(ctx context.Context) (bool, *roachpb.Error) {
	// If no Gossip available (some tests) or range too fresh, noop.
	if r.store.Gossip() == nil || !r.IsInitialized() {
		return false, roachpb.NewErrorf("no gossip or range not initialized")
	}
	var hasLease bool
	var pErr *roachpb.Error
	if err := r.store.Stopper().RunTask(
		ctx, "storage.Replica: acquiring lease to gossip",
		func(ctx context.Context) {
			// Check for or obtain the lease, if none active.
			_, pErr = r.redirectOnOrAcquireLease(ctx)
			hasLease = pErr == nil
			if pErr != nil {
				switch e := pErr.GetDetail().(type) {
				case *roachpb.NotLeaseHolderError:
					// NotLeaseHolderError means there is an active lease, but only if
					// the lease holder is set; otherwise, it's likely a timeout.
					if e.LeaseHolder != nil {
						pErr = nil
					}
				default:
					// Any other error is worth being logged visibly.
					log.Warningf(ctx, "could not acquire lease for range gossip: %s", e)
				}
			}
		}); err != nil {
		pErr = roachpb.NewError(err)
	}
	return hasLease, pErr
}

// maybeGossipFirstRange adds the sentinel and first range metadata to gossip
// if this is the first range and a range lease can be obtained. The Store
// calls this periodically on first range replicas.
func (r *Replica) maybeGossipFirstRange(ctx context.Context) *roachpb.Error {
	if !r.IsFirstRange() {
		return nil
	}

	// When multiple nodes are initialized with overlapping Gossip addresses, they all
	// will attempt to gossip their cluster ID. This is a fairly obvious misconfiguration,
	// so we error out below.
	if uuidBytes, err := r.store.Gossip().GetInfo(gossip.KeyClusterID); err == nil {
		if gossipClusterID, err := uuid.FromBytes(uuidBytes); err == nil {
			if gossipClusterID != r.store.ClusterID() {
				log.Fatalf(
					ctx, "store %d belongs to cluster %s, but attempted to join cluster %s via gossip",
					r.store.StoreID(), r.store.ClusterID(), gossipClusterID)
			}
		}
	}

	// Gossip the cluster ID from all replicas of the first range; there
	// is no expiration on the cluster ID.
	if log.V(1) {
		log.Infof(ctx, "gossiping cluster id %q from store %d, r%d", r.store.ClusterID(),
			r.store.StoreID(), r.RangeID)
	}
	if err := r.store.Gossip().AddInfo(
		gossip.KeyClusterID, r.store.ClusterID().GetBytes(), 0*time.Second,
	); err != nil {
		log.Errorf(ctx, "failed to gossip cluster ID: %s", err)
	}

	if r.store.cfg.TestingKnobs.DisablePeriodicGossips {
		return nil
	}

	hasLease, pErr := r.getLeaseForGossip(ctx)
	if pErr != nil {
		return pErr
	} else if !hasLease {
		return nil
	}
	r.gossipFirstRange(ctx)
	return nil
}

func (r *Replica) gossipFirstRange(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Gossip is not provided for the bootstrap store and for some tests.
	if r.store.Gossip() == nil {
		return
	}
	log.Event(ctx, "gossiping sentinel and first range")
	if log.V(1) {
		log.Infof(ctx, "gossiping sentinel from store %d, r%d", r.store.StoreID(), r.RangeID)
	}
	if err := r.store.Gossip().AddInfo(gossip.KeySentinel, r.store.ClusterID().GetBytes(), sentinelGossipTTL); err != nil {
		log.Errorf(ctx, "failed to gossip sentinel: %s", err)
	}
	if log.V(1) {
		log.Infof(ctx, "gossiping first range from store %d, r%d: %s",
			r.store.StoreID(), r.RangeID, r.mu.state.Desc.Replicas)
	}
	if err := r.store.Gossip().AddInfoProto(
		gossip.KeyFirstRangeDescriptor, r.mu.state.Desc, configGossipTTL); err != nil {
		log.Errorf(ctx, "failed to gossip first range metadata: %s", err)
	}
}

// shouldGossip returns true if this replica should be gossiping. Gossip is
// inherently inconsistent and asynchronous, we're using the lease as a way to
// ensure that only one node gossips at a time.
func (r *Replica) shouldGossip() bool {
	return r.OwnsValidLease(r.store.Clock().Now())
}

// MaybeGossipSystemConfig scans the entire SystemConfig span and gossips it.
// The first call is on NewReplica. Further calls come from the trigger on
// EndTransaction or range lease acquisition.
//
// Note that MaybeGossipSystemConfig gossips information only when the
// lease is actually held. The method does not request a range lease
// here since RequestLease and applyRaftCommand call the method and we
// need to avoid deadlocking in redirectOnOrAcquireLease.
//
// MaybeGossipSystemConfig must only be called from Raft commands
// (which provide the necessary serialization to avoid data races).
//
// TODO(nvanbenschoten,bdarnell): even though this is best effort, we
// should log louder when we continually fail to gossip system config.
func (r *Replica) MaybeGossipSystemConfig(ctx context.Context) error {
	if r.store.Gossip() == nil {
		log.VEventf(ctx, 2, "not gossiping system config because gossip isn't initialized")
		return nil
	}
	if !r.IsInitialized() {
		log.VEventf(ctx, 2, "not gossiping system config because the replica isn't initialized")
		return nil
	}
	if !r.ContainsKey(keys.SystemConfigSpan.Key) {
		log.VEventf(ctx, 2,
			"not gossiping system config because the replica doesn't contain the system config's start key")
		return nil
	}
	if !r.shouldGossip() {
		log.VEventf(ctx, 2, "not gossiping system config because the replica doesn't hold the lease")
		return nil
	}

	// TODO(marc): check for bad split in the middle of the SystemConfig span.
	loadedCfg, err := r.loadSystemConfig(ctx)
	if err != nil {
		if err == errSystemConfigIntent {
			log.VEventf(ctx, 2, "not gossiping system config because intents were found on SystemConfigSpan")
			return nil
		}
		return errors.Wrap(err, "could not load SystemConfig span")
	}

	if gossipedCfg, ok := r.store.Gossip().GetSystemConfig(); ok && gossipedCfg.Equal(loadedCfg) &&
		r.store.Gossip().InfoOriginatedHere(gossip.KeySystemConfig) {
		log.VEventf(ctx, 2, "not gossiping unchanged system config")
		return nil
	}

	log.VEventf(ctx, 2, "gossiping system config")
	if err := r.store.Gossip().AddInfoProto(gossip.KeySystemConfig, &loadedCfg, 0); err != nil {
		return errors.Wrap(err, "failed to gossip system config")
	}
	return nil
}

// MaybeGossipNodeLiveness gossips information for all node liveness
// records stored on this range. To scan and gossip, this replica
// must hold the lease to a range which contains some or all of the
// node liveness records. After scanning the records, it checks
// against what's already in gossip and only gossips records which
// are out of date.
func (r *Replica) MaybeGossipNodeLiveness(ctx context.Context, span roachpb.Span) error {
	if r.store.Gossip() == nil || !r.IsInitialized() {
		return nil
	}

	if !r.ContainsKeyRange(span.Key, span.EndKey) || !r.shouldGossip() {
		return nil
	}

	ba := roachpb.BatchRequest{}
	ba.Timestamp = r.store.Clock().Now()
	ba.Add(&roachpb.ScanRequest{Span: span})
	// Call evaluateBatch instead of Send to avoid command queue reentrance.
	rec := NewReplicaEvalContext(r, todoSpanSet)
	br, result, pErr :=
		evaluateBatch(ctx, storagebase.CmdIDKey(""), r.store.Engine(), rec, nil, ba)
	if pErr != nil {
		return errors.Wrapf(pErr.GoError(), "couldn't scan node liveness records in span %s", span)
	}
	if result.Local.Intents != nil && len(*result.Local.Intents) > 0 {
		return errors.Errorf("unexpected intents on node liveness span %s: %+v", span, *result.Local.Intents)
	}
	kvs := br.Responses[0].GetInner().(*roachpb.ScanResponse).Rows
	log.VEventf(ctx, 2, "gossiping %d node liveness record(s) from span %s", len(kvs), span)
	for _, kv := range kvs {
		var kvLiveness, gossipLiveness Liveness
		if err := kv.Value.GetProto(&kvLiveness); err != nil {
			return errors.Wrapf(err, "failed to unmarshal liveness value %s", kv.Key)
		}
		key := gossip.MakeNodeLivenessKey(kvLiveness.NodeID)
		// Look up liveness from gossip; skip gossiping anew if unchanged.
		if err := r.store.Gossip().GetInfoProto(key, &gossipLiveness); err == nil {
			if gossipLiveness == kvLiveness {
				continue
			}
		}
		if err := r.store.Gossip().AddInfoProto(key, &kvLiveness, 0); err != nil {
			return errors.Wrapf(err, "failed to gossip node liveness (%+v)", kvLiveness)
		}
	}
	return nil
}

// NewReplicaCorruptionError creates a new error indicating a corrupt replica,
// with the supplied list of errors given as history.
func NewReplicaCorruptionError(err error) *roachpb.ReplicaCorruptionError {
	return &roachpb.ReplicaCorruptionError{ErrorMsg: err.Error()}
}

// maybeSetCorrupt is a stand-in for proper handling of failing replicas. Such a
// failure is indicated by a call to maybeSetCorrupt with a ReplicaCorruptionError.
// Currently any error is passed through, but prospectively it should stop the
// range from participating in progress, trigger a rebalance operation and
// decide on an error-by-error basis whether the corruption is limited to the
// range, store, node or cluster with corresponding actions taken.
//
// TODO(d4l3k): when marking a Replica corrupt, must subtract its stats from
// r.store.metrics. Errors which happen between committing a batch and sending
// a stats delta from the store are going to be particularly tricky and the
// best bet is to not have any of those.
// @bdarnell remarks: Corruption errors should be rare so we may want the store
// to just recompute its stats in the background when one occurs.
func (r *Replica) maybeSetCorrupt(ctx context.Context, pErr *roachpb.Error) *roachpb.Error {
	if cErr, ok := pErr.GetDetail().(*roachpb.ReplicaCorruptionError); ok {
		r.mu.Lock()
		defer r.mu.Unlock()

		log.Errorf(ctx, "stalling replica due to: %s", cErr.ErrorMsg)
		cErr.Processed = true
		r.mu.destroyStatus.Set(cErr, destroyReasonCorrupted)
		pErr = roachpb.NewError(cErr)

		// Try to persist the destroyed error message. If the underlying store is
		// corrupted the error won't be processed and a panic will occur.
		if err := r.mu.stateLoader.SetReplicaDestroyedError(ctx, r.store.Engine(), pErr); err != nil {
			cErr.Processed = false
			return roachpb.NewError(cErr)
		}
	}
	return pErr
}

var errSystemConfigIntent = errors.New("must retry later due to intent on SystemConfigSpan")

// loadSystemConfig scans the system config span and returns the system
// config.
func (r *Replica) loadSystemConfig(ctx context.Context) (config.SystemConfig, error) {
	ba := roachpb.BatchRequest{}
	ba.ReadConsistency = roachpb.INCONSISTENT
	ba.Timestamp = r.store.Clock().Now()
	ba.Add(&roachpb.ScanRequest{Span: keys.SystemConfigSpan})
	// Call evaluateBatch instead of Send to avoid command queue reentrance.
	rec := NewReplicaEvalContext(r, todoSpanSet)
	br, result, pErr := evaluateBatch(
		ctx, storagebase.CmdIDKey(""), r.store.Engine(), rec, nil, ba,
	)
	if pErr != nil {
		return config.SystemConfig{}, pErr.GoError()
	}
	if intents := result.Local.DetachIntents(); len(intents) > 0 {
		// There were intents, so what we read may not be consistent. Attempt
		// to nudge the intents in case they're expired; next time around we'll
		// hopefully have more luck.
		// This is called from handleLocalEvalResult (with raftMu locked),
		// so disallow synchronous processing (which blocks that mutex for
		// too long and is a potential deadlock).
		if err := r.store.intentResolver.processIntentsAsync(ctx, r, intents, false /* allowSync */); err != nil {
			log.Warning(ctx, err)
		}
		return config.SystemConfig{}, errSystemConfigIntent
	}
	kvs := br.Responses[0].GetInner().(*roachpb.ScanResponse).Rows
	return config.SystemConfig{Values: kvs}, nil
}

// needsSplitBySize returns true if the size of the range requires it
// to be split.
func (r *Replica) needsSplitBySize() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.needsSplitBySizeRLocked()
}

func (r *Replica) needsSplitBySizeRLocked() bool {
	return r.exceedsMultipleOfSplitSizeRLocked(1)
}

func (r *Replica) exceedsMultipleOfSplitSizeRLocked(mult float64) bool {
	maxBytes := r.mu.maxBytes
	size := r.mu.state.Stats.Total()
	return maxBytes > 0 && float64(size) > float64(maxBytes)*mult
}

func (r *Replica) setPendingSnapshotIndex(index uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// We allow the pendingSnapshotIndex to change from 0 to 1 and then from 1 to
	// a value greater than 1. Any other change indicates 2 current preemptive
	// snapshots on the same replica which is disallowed.
	if (index == 1 && r.mu.pendingSnapshotIndex != 0) ||
		(index > 1 && r.mu.pendingSnapshotIndex != 1) {
		return errors.Errorf(
			"%s: can't set pending snapshot index to %d; pending snapshot already present: %d",
			r, index, r.mu.pendingSnapshotIndex)
	}
	r.mu.pendingSnapshotIndex = index
	return nil
}

func (r *Replica) clearPendingSnapshotIndex() {
	r.mu.Lock()
	r.mu.pendingSnapshotIndex = 0
	r.mu.Unlock()
}

func (r *Replica) endKey() roachpb.RKey {
	return r.Desc().EndKey
}

// Less implements the btree.Item interface.
func (r *Replica) Less(i btree.Item) bool {
	return r.endKey().Less(i.(rangeKeyItem).endKey())
}

// ReplicaMetrics contains details on the current status of the replica.
type ReplicaMetrics struct {
	Leader      bool
	LeaseValid  bool
	Leaseholder bool
	LeaseType   roachpb.LeaseType
	LeaseStatus LeaseStatus
	Quiescent   bool
	// Is this the replica which collects per-range metrics? This is done either
	// on the leader or, if there is no leader, on the largest live replica ID.
	RangeCounter      bool
	Unavailable       bool
	Underreplicated   bool
	BehindCount       int64
	CmdQMetricsLocal  CommandQueueMetrics
	CmdQMetricsGlobal CommandQueueMetrics
}

// Metrics returns the current metrics for the replica.
func (r *Replica) Metrics(
	ctx context.Context,
	now hlc.Timestamp,
	cfg config.SystemConfig,
	livenessMap map[roachpb.NodeID]bool,
) ReplicaMetrics {
	r.mu.RLock()
	raftStatus := r.raftStatusRLocked()
	leaseStatus := r.leaseStatus(*r.mu.state.Lease, now, r.mu.minLeaseProposedTS)
	quiescent := r.mu.quiescent || r.mu.internalRaftGroup == nil
	desc := r.mu.state.Desc
	r.cmdQMu.Lock()
	cmdQMetricsLocal := r.cmdQMu.queues[spanset.SpanLocal].metrics()
	cmdQMetricsGlobal := r.cmdQMu.queues[spanset.SpanGlobal].metrics()
	r.cmdQMu.Unlock()
	r.mu.RUnlock()

	return calcReplicaMetrics(
		ctx,
		now,
		cfg,
		livenessMap,
		desc,
		raftStatus,
		leaseStatus,
		r.store.StoreID(),
		quiescent,
		cmdQMetricsLocal,
		cmdQMetricsGlobal,
	)
}

func isRaftLeader(raftStatus *raft.Status) bool {
	return raftStatus != nil && raftStatus.SoftState.RaftState == raft.StateLeader
}

// HasRaftLeader returns true if the raft group has a raft leader currently.
func HasRaftLeader(raftStatus *raft.Status) bool {
	return raftStatus != nil && raftStatus.SoftState.Lead != 0
}

func calcReplicaMetrics(
	ctx context.Context,
	now hlc.Timestamp,
	cfg config.SystemConfig,
	livenessMap map[roachpb.NodeID]bool,
	desc *roachpb.RangeDescriptor,
	raftStatus *raft.Status,
	leaseStatus LeaseStatus,
	storeID roachpb.StoreID,
	quiescent bool,
	cmdQMetricsLocal CommandQueueMetrics,
	cmdQMetricsGlobal CommandQueueMetrics,
) ReplicaMetrics {
	var m ReplicaMetrics

	var leaseOwner bool
	m.LeaseStatus = leaseStatus
	if leaseStatus.State == LeaseState_VALID {
		m.LeaseValid = true
		leaseOwner = leaseStatus.Lease.OwnedBy(storeID)
		m.LeaseType = leaseStatus.Lease.Type()
	}
	m.Leaseholder = m.LeaseValid && leaseOwner
	m.Leader = isRaftLeader(raftStatus)
	m.Quiescent = quiescent

	// We compute an estimated range count across the cluster by counting the
	// first live replica in each descriptor. Note that the first live replica is
	// an arbitrary choice. We want to select one live replica to do the counting
	// that all replicas can agree on.
	//
	// Note that this heuristic can double count. If the first live replica is on
	// a node that is partitioned from the other replicas in the range, there may
	// be multiple nodes which believe they are the first live replica. This
	// scenario seems rare as it requires the partitioned node to be alive enough
	// to be performing liveness heartbeats.
	for _, rd := range desc.Replicas {
		if livenessMap[rd.NodeID] {
			m.RangeCounter = rd.StoreID == storeID
			break
		}
	}

	// We also compute an estimated per-range count of under-replicated and
	// unavailable ranges for each range based on the liveness table.
	if m.RangeCounter {
		liveReplicas := calcLiveReplicas(desc, livenessMap)
		if liveReplicas < computeQuorum(len(desc.Replicas)) {
			m.Unavailable = true
		}
		if zoneConfig, err := cfg.GetZoneConfigForKey(desc.StartKey); err != nil {
			log.Error(ctx, err)
		} else if int32(liveReplicas) < zoneConfig.NumReplicas {
			m.Underreplicated = true
		}
	}

	// The raft leader computes the number of raft entries that replicas are
	// behind.
	if m.Leader {
		m.BehindCount = calcBehindCount(raftStatus, desc, livenessMap)
	}

	m.CmdQMetricsLocal = cmdQMetricsLocal
	m.CmdQMetricsGlobal = cmdQMetricsGlobal

	return m
}

// calcLiveReplicas returns a count of the live replicas; a live replica is
// determined by checking its node in the provided liveness map.
func calcLiveReplicas(desc *roachpb.RangeDescriptor, livenessMap map[roachpb.NodeID]bool) int {
	var goodReplicas int
	for _, rd := range desc.Replicas {
		if livenessMap[rd.NodeID] {
			goodReplicas++
		}
	}
	return goodReplicas
}

// calcBehindCount returns a total count of log entries that follower replicas
// are behind. This can only be computed on the raft leader.
func calcBehindCount(
	raftStatus *raft.Status, desc *roachpb.RangeDescriptor, livenessMap map[roachpb.NodeID]bool,
) int64 {
	var behindCount int64
	for _, rd := range desc.Replicas {
		if progress, ok := raftStatus.Progress[uint64(rd.ReplicaID)]; ok {
			if progress.Match > 0 &&
				progress.Match < raftStatus.Commit {
				behindCount += int64(raftStatus.Commit) - int64(progress.Match)
			}
		}
	}

	return behindCount
}

// QueriesPerSecond returns the range's average QPS if it is the current
// leaseholder. If it isn't, this will return 0 because the replica does not
// know about the reads that the leaseholder is serving.
func (r *Replica) QueriesPerSecond() float64 {
	qps, _ := r.leaseholderStats.avgQPS()
	return qps
}

// WritesPerSecond returns the range's average keys written per second.
func (r *Replica) WritesPerSecond() float64 {
	wps, _ := r.writeStats.avgQPS()
	return wps
}

// GetLeaseHistory returns the lease history stored on this replica.
func (r *Replica) GetLeaseHistory() []roachpb.Lease {
	if r.leaseHistory == nil {
		return nil
	}
	return r.leaseHistory.get()
}

// EnableLeaseHistory turns on the lease history for testing purposes. Returns
// a function to return it to its original state that can be deferred.
func EnableLeaseHistory(maxEntries int) func() {
	originalValue := leaseHistoryMaxEntries
	leaseHistoryMaxEntries = maxEntries
	return func() {
		leaseHistoryMaxEntries = originalValue
	}
}

// GetCommandQueueSnapshot returns a snapshot of the command queue state for
// this replica.
func (r *Replica) GetCommandQueueSnapshot() storagebase.CommandQueuesSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.cmdQMu.Lock()
	defer r.cmdQMu.Unlock()
	return storagebase.CommandQueuesSnapshot{
		Timestamp:   r.store.Clock().Now(),
		LocalScope:  r.cmdQMu.queues[spanset.SpanLocal].GetSnapshot(),
		GlobalScope: r.cmdQMu.queues[spanset.SpanGlobal].GetSnapshot(),
	}
}
