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
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/abortspan"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/ctpb"
	ctstorage "github.com/cockroachdb/cockroach/pkg/storage/closedts/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/storage/spanlatch"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/split"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/google/btree"
	"github.com/kr/pretty"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

const (
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

var testingDisableQuiescence = envutil.EnvOrDefaultBool("COCKROACH_DISABLE_QUIESCENCE", false)

var disableSyncRaftLog = settings.RegisterBoolSetting(
	"kv.raft_log.disable_synchronization_unsafe",
	"set to true to disable synchronization on Raft log writes to persistent storage. "+
		"Setting to true risks data loss or data corruption on server crashes. "+
		"The setting is meant for internal testing only and SHOULD NOT be used in production.",
	false,
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

// FollowerReadsEnabled controls whether replicas attempt to serve follower
// reads. The closed timestamp machinery is unaffected by this, i.e. the same
// information is collected and passed around, regardless of the value of this
// setting.
var FollowerReadsEnabled = settings.RegisterBoolSetting(
	"kv.closed_timestamp.follower_reads_enabled",
	"allow (all) replicas to serve consistent historical reads based on closed timestamp information",
	false,
)

type proposalReevaluationReason int

const (
	proposalNoReevaluation proposalReevaluationReason = iota
	// proposalIllegalLeaseIndex indicates the proposal failed to apply at
	// a Lease index it was not legal for. The command should be re-evaluated.
	proposalIllegalLeaseIndex
)

// proposalResult indicates the result of a proposal. Exactly one of
// Reply, Err and ProposalRetry is set, and it represents the result of
// the proposal.
type proposalResult struct {
	Reply         *roachpb.BatchResponse
	Err           *roachpb.Error
	ProposalRetry proposalReevaluationReason
	Intents       []result.IntentsWithArg
	EndTxns       []result.EndTxnIntents
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
	// The replica has been merged into its left-hand neighbor, but its left-hand
	// neighbor hasn't yet subsumed it.
	destroyReasonMergePending
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

// a lastUpdateTimesMap is maintained on the Raft leader to keep track of the
// last communication received from followers, which in turn informs the quota
// pool and log truncations.
type lastUpdateTimesMap map[roachpb.ReplicaID]time.Time

func (m lastUpdateTimesMap) update(replicaID roachpb.ReplicaID, now time.Time) {
	if m == nil {
		return
	}
	m[replicaID] = now
}

// isFollowerActive returns whether the specified follower has made
// communication with the leader in the last MaxQuotaReplicaLivenessDuration.
func (m lastUpdateTimesMap) isFollowerActive(
	ctx context.Context, replicaID roachpb.ReplicaID, now time.Time,
) bool {
	lastUpdateTime, ok := m[replicaID]
	if !ok {
		// If the follower has no entry in lastUpdateTimes, it has not been
		// updated since r became the leader (at which point all then-existing
		// replicas were updated).
		return false
	}
	return now.Sub(lastUpdateTime) <= MaxQuotaReplicaLivenessDuration
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
	RangeID roachpb.RangeID // Only set by the constructor

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
	//
	// This mutex ensures proper interleaving of splits with concurrent reads.
	// Splits register an MVCC write span latch, but reads at lower timestamps
	// aren't held up by this latch, which could result in reads on the RHS
	// executed through the LHS after this is valid. For more detail, see:
	// https://github.com/cockroachdb/cockroach/issues/32583.
	readOnlyCmdMu syncutil.RWMutex

	// rangeStr is a string representation of a RangeDescriptor that can be
	// atomically read and updated without needing to acquire the replica.mu lock.
	// All updates to state.Desc should be duplicated here.
	rangeStr atomicDescString

	// raftMu protects Raft processing the replica.
	//
	// Locking notes: Replica.raftMu < Replica.mu
	raftMu struct {
		syncutil.Mutex

		// Note that there are two StateLoaders, in raftMu and mu,
		// depending on which lock is being held.
		stateLoader stateloader.StateLoader
		// on-disk storage for sideloaded SSTables. nil when there's no ReplicaID.
		sideloaded sideloadStorage

		// rangefeed is an instance of a rangefeed Processor that is capable of
		// routing rangefeed events to a set of subscribers. Will be nil if no
		// subscribers are registered.
		rangefeed *rangefeed.Processor
	}

	// Contains the lease history when enabled.
	leaseHistory *leaseHistory

	// Enforces at most one command is running per key(s) within each span
	// scope. The globally-scoped component tracks user writes (i.e. all
	// keys for which keys.Addr is the identity), the locally-scoped component
	// the rest (e.g. RangeDescriptor, transaction record, Lease, ...).
	latchMgr spanlatch.Manager

	mu struct {
		// Protects all fields in the mu struct.
		syncutil.RWMutex
		// The destroyed status of a replica indicating if it's alive, corrupt,
		// scheduled for destruction or has been GCed.
		destroyStatus
		// Is the range quiescent? Quiescent ranges are not Tick()'d and unquiesce
		// whenever a Raft operation is performed.
		quiescent bool
		// mergeComplete is non-nil if a merge is in-progress, in which case any
		// requests should be held until the completion of the merge is signaled by
		// the closing of the channel.
		mergeComplete chan struct{}
		// The state of the Raft state machine.
		state storagepb.ReplicaState
		// Counter used for assigning lease indexes for proposals.
		lastAssignedLeaseIndex uint64
		// Last index/term persisted to the raft log (not necessarily
		// committed). Note that lastTerm may be 0 (and thus invalid) even when
		// lastIndex is known, in which case the term will have to be retrieved
		// from the Raft log entry. Use the invalidLastTerm constant for this
		// case.
		lastIndex, lastTerm uint64
		// A map of raft log index of pending preemptive snapshots to deadlines.
		// Used to prohibit raft log truncations that would leave a gap between
		// the snapshot and the new first index. The map entry has a zero
		// deadline while the snapshot is being sent and turns nonzero when the
		// snapshot has completed, preventing truncation for a grace period
		// (since there is a race between the snapshot completing and its being
		// reflected in the raft status used to make truncation decisions).
		//
		// NB: If we kept only one value, we could end up in situations in which
		// we're either giving some snapshots no grace period, or keep an
		// already finished snapshot "pending" for extended periods of time
		// (preventing log truncation).
		snapshotLogTruncationConstraints map[uuid.UUID]snapTruncationInfo
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
		// 		a) the spanlatch manager is wiped during the restart; there might be
		// 		writes in flight that do not have the latches they held reflected. So,
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
		// A pointer to the zone config for this replica.
		zone *config.ZoneConfig
		// proposals stores the Raft in-flight commands which originated at
		// this Replica, i.e. all commands for which propose has been called,
		// but which have not yet applied.
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

		// The most recently updated time for each follower of this range. This is updated
		// every time a Raft message is received from a peer.
		// Note that superficially it seems that similar information is contained in the
		// Progress of a RaftStatus, which has a RecentActive field. However, that field
		// is always true unless CheckQuorum is active, which at the time of writing in
		// CockroachDB is not the case.
		//
		// TODO(tschottdorf): keeping a map on each replica seems to be
		// overdoing it. We should map the replicaID to a NodeID and then use
		// node liveness (or any sensible measure of the peer being around).
		// The danger in doing so is that a single stuck replica on an otherwise
		// functioning node could fill up the quota pool. We are already taking
		// this kind of risk though: a replica that gets stuck on an otherwise
		// live node will not lose leaseholdership.
		lastUpdateTimes lastUpdateTimesMap

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

	// Split keeps information for load-based splitting.
	splitMu struct {
		syncutil.Mutex
		lastReqTime time.Time // most recent time recorded by requests.
		count       int64     // reqs since last nanos
		qps         float64   // last reqs/s rate
		splitFinder *split.Finder
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
// varies.
//
// Requires that Replica.mu is held. Also requires that Replica.raftMu is held
// if either the caller can't guarantee that r.mu.internalRaftGroup != nil or
// the provided function requires Replica.raftMu.
func (r *Replica) withRaftGroupLocked(
	mayCampaignOnWake bool, f func(r *raft.RawNode) (unquiesceAndWakeLeader bool, _ error),
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

	if r.mu.internalRaftGroup == nil {
		r.raftMu.Mutex.AssertHeld()

		ctx := r.AnnotateCtx(context.TODO())
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

		if mayCampaignOnWake {
			r.maybeCampaignOnWakeLocked(ctx)
		}
	}

	// This wrapper function is a hack to add range IDs to stack traces
	// using the same pattern as Replica.sendWithRangeID.
	unquiesce, err := func(rangeID roachpb.RangeID, raftGroup *raft.RawNode) (bool, error) {
		return f(raftGroup)
	}(r.RangeID, r.mu.internalRaftGroup)
	if unquiesce {
		r.unquiesceAndWakeLeaderLocked()
	}
	return err
}

// withRaftGroup calls the supplied function with the (lazily initialized)
// Raft group. It acquires and releases the Replica lock, so r.mu must not be
// held (or acquired by the supplied function).
//
// If mayCampaignOnWake is true, the replica may initiate a raft
// election if it was previously in a dormant state. Most callers
// should set this to true, because the prevote feature minimizes the
// disruption from unnecessary elections. The exception is that we
// should not initiate an election while handling incoming raft
// messages (which may include MsgVotes from an election in progress,
// and this election would be disrupted if we started our own).
//
// Has the same requirement for Replica.raftMu as withRaftGroupLocked.
func (r *Replica) withRaftGroup(
	mayCampaignOnWake bool, f func(r *raft.RawNode) (unquiesceAndWakeLeader bool, _ error),
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.withRaftGroupLocked(mayCampaignOnWake, f)
}

func shouldCampaignOnWake(
	leaseStatus storagepb.LeaseStatus,
	lease roachpb.Lease,
	storeID roachpb.StoreID,
	raftStatus raft.Status,
) bool {
	// When waking up a range, campaign unless we know that another
	// node holds a valid lease (this is most important after a split,
	// when all replicas create their raft groups at about the same
	// time, with a lease pre-assigned to one of them). Note that
	// thanks to PreVote, unnecessary campaigns are not disruptive so
	// we should err on the side of campaigining here.
	anotherOwnsLease := leaseStatus.State == storagepb.LeaseState_VALID && !lease.OwnedBy(storeID)

	// If we're already campaigning or know who the leader is, don't
	// start a new term.
	noLeader := raftStatus.RaftState == raft.StateFollower && raftStatus.Lead == 0
	return !anotherOwnsLease && noLeader
}

// maybeCampaignOnWakeLocked is called when the range wakes from a
// dormant state (either the initial "raftGroup == nil" state or after
// being quiescent) and campaigns for raft leadership if appropriate.
func (r *Replica) maybeCampaignOnWakeLocked(ctx context.Context) {
	// Raft panics if a node that is not currently a member of the
	// group tries to campaign. That happens primarily when we apply
	// preemptive snapshots.
	if _, currentMember := r.mu.state.Desc.GetReplicaDescriptorByID(r.mu.replicaID); !currentMember {
		return
	}

	leaseStatus := r.leaseStatus(*r.mu.state.Lease, r.store.Clock().Now(), r.mu.minLeaseProposedTS)
	raftStatus := r.mu.internalRaftGroup.Status()
	if shouldCampaignOnWake(leaseStatus, *r.mu.state.Lease, r.store.StoreID(), *raftStatus) {
		log.VEventf(ctx, 3, "campaigning")
		if err := r.mu.internalRaftGroup.Campaign(); err != nil {
			log.VEventf(ctx, 1, "failed to campaign: %s", err)
		}
	}
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
	r.mu.stateLoader = stateloader.Make(rangeID)
	r.mu.quiescent = true
	r.mu.zone = config.DefaultZoneConfigRef()

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
	r.raftMu.stateLoader = stateloader.Make(rangeID)
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

	r.latchMgr = spanlatch.Make(r.store.stopper, r.store.metrics.SlowLatchRequests)
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

func (r *Replica) preDestroyRaftMuLocked(
	ctx context.Context,
	reader engine.Reader,
	batch engine.Batch,
	nextReplicaID roachpb.ReplicaID,
	destroyData bool,
) error {
	desc := r.Desc()
	err := clearRangeData(ctx, desc, reader, batch, destroyData)
	if err != nil {
		return err
	}

	// Save a tombstone to ensure that replica IDs never get reused.
	//
	// NB: Legacy tombstones (which are in the replicated key space) are wiped
	// in clearRangeData, but that's OK since we're writing a new one in the same
	// batch (and in particular, sequenced *after* the wipe).
	return r.setTombstoneKey(ctx, batch, nextReplicaID)
}

func (r *Replica) postDestroyRaftMuLocked(ctx context.Context, ms enginepb.MVCCStats) error {
	// Suggest the cleared range to the compactor queue.
	//
	// TODO(benesch): we would ideally atomically suggest the compaction with
	// the deletion of the data itself.
	desc := r.Desc()
	r.store.compactor.Suggest(ctx, storagepb.SuggestedCompaction{
		StartKey: roachpb.Key(desc.StartKey),
		EndKey:   roachpb.Key(desc.EndKey),
		Compaction: storagepb.Compaction{
			Bytes:            ms.Total(),
			SuggestedAtNanos: timeutil.Now().UnixNano(),
		},
	})

	// NB: we need the nil check below because it's possible that we're GC'ing a
	// Replica without a replicaID, in which case it does not have a sideloaded
	// storage.
	//
	// TODO(tschottdorf): at node startup, we should remove all on-disk
	// directories belonging to replicas which aren't present. A crash before a
	// call to postDestroyRaftMuLocked will currently leave the files around
	// forever.
	if r.raftMu.sideloaded != nil {
		return r.raftMu.sideloaded.Clear(ctx)
	}
	return nil
}

// destroyRaftMuLocked deletes data associated with a replica, leaving a
// tombstone. If `destroyData` is true, data in all of the range's keyspaces
// will be deleted. Otherwise, only data in the range-ID local keyspace will be
// deleted. Requires that Replica.raftMu is held.
func (r *Replica) destroyRaftMuLocked(ctx context.Context, nextReplicaID roachpb.ReplicaID) error {
	startTime := timeutil.Now()

	ms := r.GetMVCCStats()

	const destroyData = true
	batch := r.Engine().NewWriteOnlyBatch()
	defer batch.Close()
	if err := r.preDestroyRaftMuLocked(ctx, r.Engine(), batch, nextReplicaID, destroyData); err != nil {
		return err
	}
	preTime := timeutil.Now()

	// We need to sync here because we are potentially deleting sideloaded
	// proposals from the file system next. We could write the tombstone only in
	// a synchronous batch first and then delete the data alternatively, but
	// then need to handle the case in which there is both the tombstone and
	// leftover replica data.
	if err := batch.Commit(true); err != nil {
		return err
	}
	commitTime := timeutil.Now()

	if err := r.postDestroyRaftMuLocked(ctx, ms); err != nil {
		return err
	}

	log.Infof(ctx, "removed %d (%d+%d) keys in %0.0fms [clear=%0.0fms commit=%0.0fms]",
		ms.KeyCount+ms.SysCount, ms.KeyCount, ms.SysCount,
		commitTime.Sub(startTime).Seconds()*1000,
		preTime.Sub(startTime).Seconds()*1000,
		commitTime.Sub(preTime).Seconds()*1000)
	return nil
}

func (r *Replica) cancelPendingCommandsLocked() {
	r.mu.AssertHeld()
	for _, p := range r.mu.proposals {
		r.cleanupFailedProposalLocked(p)
		// NB: each proposal needs its own version of the error (i.e. don't try to
		// share the error across proposals).
		p.finishApplication(proposalResult{
			Err: roachpb.NewError(roachpb.NewAmbiguousResultError("removing replica")),
		})
	}
}

// cleanupFailedProposalLocked cleans up after a proposal that has failed. It
// clears any references to the proposal and releases associated quota.
func (r *Replica) cleanupFailedProposalLocked(p *ProposalData) {
	// Clear the proposal from the proposals map. May be a no-op if the
	// proposal has not yet been inserted into the map.
	delete(r.mu.proposals, p.idKey)
	// Release associated quota pool resources if we have been tracking
	// this command.
	//
	// NB: We may be double free-ing here in cases where proposals are
	// duplicated. To counter this our quota pool is capped at the initial
	// quota size.
	if cmdSize, ok := r.mu.commandSizes[p.idKey]; ok {
		r.mu.proposalQuota.add(int64(cmdSize))
		delete(r.mu.commandSizes, p.idKey)
	}
}

// setTombstoneKey writes a tombstone to disk to ensure that replica IDs never
// get reused. It determines what the minimum next replica ID can be using
// the provided nextReplicaID and the Replica's own ID.
//
// We have to be careful to set the right key, since a replica can be using an
// ID that it hasn't yet received a RangeDescriptor for if it receives raft
// requests for that replica ID (as seen in #14231).
func (r *Replica) setTombstoneKey(
	ctx context.Context, eng engine.ReadWriter, externalNextReplicaID roachpb.ReplicaID,
) error {
	r.mu.Lock()
	nextReplicaID := r.mu.state.Desc.NextReplicaID
	if nextReplicaID < externalNextReplicaID {
		nextReplicaID = externalNextReplicaID
	}
	if nextReplicaID > r.mu.minReplicaID {
		r.mu.minReplicaID = nextReplicaID
	}
	r.mu.Unlock()

	tombstoneKey := keys.RaftTombstoneKey(r.RangeID)
	tombstone := &roachpb.RaftTombstone{
		NextReplicaID: nextReplicaID,
	}
	return engine.MVCCPutProto(ctx, eng, nil, tombstoneKey,
		hlc.Timestamp{}, nil, tombstone)
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
		r.store.engine,
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
	if log.HasSpanOrEvent(ctx) {
		if q := quotaPool.approximateQuota(); q < quotaPool.maxQuota()/10 {
			log.Eventf(ctx, "quota running low, currently available ~%d", q)
		}
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
			r.mu.proposalQuota = newQuotaPool(r.store.cfg.RaftProposalQuota)
			r.mu.lastUpdateTimes = make(map[roachpb.ReplicaID]time.Time)
			now := timeutil.Now()
			for _, desc := range r.mu.state.Desc.Replicas {
				r.mu.lastUpdateTimes.update(desc.ReplicaID, now)
			}
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
	now := timeutil.Now()
	minIndex := status.Commit
	for _, rep := range r.mu.state.Desc.Replicas {
		// Only consider followers that that have "healthy" RPC connections.

		if err := r.store.cfg.NodeDialer.ConnHealth(rep.NodeID); err != nil {
			continue
		}

		// Only consider followers that are active.
		if !r.mu.lastUpdateTimes.isFollowerActive(ctx, rep.ReplicaID, now) {
			continue
		}
		if progress, ok := status.Progress[uint64(rep.ReplicaID)]; ok {
			// Note that the Match field has different semantics depending on
			// the State.
			//
			// In state ProgressStateReplicate, the Match index is optimistically
			// updated whenever a message is *sent* (not received). Due to Raft
			// flow control, only a reasonably small amount of data can be en
			// route to a given follower at any point in time.
			//
			// In state ProgressStateProbe, the Match index equals Next-1, and
			// it tells us the leader's optimistic best guess for the right log
			// index (and will try once per heartbeat interval to update its
			// estimate). In the usual case, the follower responds with a hint
			// when it rejects the first probe and the leader replicates or
			// sends a snapshot. In the case in which the follower does not
			// respond, the leader reduces Match by one each heartbeat interval.
			// But if the follower does not respond, we've already filtered it
			// out above. We use the Match index as is, even though the follower
			// likely isn't there yet because that index won't go up unless the
			// follower is actually catching up, so it won't cause it to fall
			// behind arbitrarily.
			//
			// Another interesting tidbit about this state is that the Paused
			// field is usually true as it is used to limit the number of probes
			// (i.e. appends) sent to this follower to one per heartbeat
			// interval.
			//
			// In state ProgressStateSnapshot, the Match index is the last known
			// (possibly optimistic, depending on previous state) index before
			// the snapshot went out. Once the snapshot applies, the follower
			// will enter ProgressStateReplicate again. So here the Match index
			// works as advertised too.

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

// GetMinBytes gets the replica's minimum byte threshold.
func (r *Replica) GetMinBytes() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return *r.mu.zone.RangeMinBytes
}

// GetMaxBytes gets the replica's maximum byte threshold.
func (r *Replica) GetMaxBytes() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return *r.mu.zone.RangeMaxBytes
}

// SetZoneConfig sets the replica's zone config.
func (r *Replica) SetZoneConfig(zone *config.ZoneConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.zone = zone
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
	return r.isDestroyedRLocked()
}

func (r *Replica) isDestroyedRLocked() (DestroyReason, error) {
	return r.mu.destroyStatus.reason, r.mu.destroyStatus.err
}

// GetLease returns the lease and, if available, the proposed next lease.
func (r *Replica) GetLease() (roachpb.Lease, roachpb.Lease) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getLeaseRLocked()
}

func (r *Replica) getLeaseRLocked() (roachpb.Lease, roachpb.Lease) {
	if nextLease, ok := r.mu.pendingLeaseRequest.RequestPending(); ok {
		return *r.mu.state.Lease, nextLease
	}
	return *r.mu.state.Lease, roachpb.Lease{}
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
		r.leaseStatus(*r.mu.state.Lease, ts, r.mu.minLeaseProposedTS).State == storagepb.LeaseState_VALID
}

// IsLeaseValid returns true if the replica's lease is owned by this
// replica and is valid (not expired, not in stasis).
func (r *Replica) IsLeaseValid(lease roachpb.Lease, ts hlc.Timestamp) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isLeaseValidRLocked(lease, ts)
}

func (r *Replica) isLeaseValidRLocked(lease roachpb.Lease, ts hlc.Timestamp) bool {
	return r.leaseStatus(lease, ts, r.mu.minLeaseProposedTS).State == storagepb.LeaseState_VALID
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
func (r *Replica) leaseGoodToGo(ctx context.Context) (storagepb.LeaseStatus, bool) {
	timestamp := r.store.Clock().Now()
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.requiresExpiringLeaseRLocked() {
		// Slow-path for expiration-based leases.
		return storagepb.LeaseStatus{}, false
	}

	status := r.leaseStatus(*r.mu.state.Lease, timestamp, r.mu.minLeaseProposedTS)
	if status.State == storagepb.LeaseState_VALID && status.Lease.OwnedBy(r.store.StoreID()) {
		// We own the lease...
		if repDesc, err := r.getReplicaDescriptorRLocked(); err == nil {
			if _, ok := r.mu.pendingLeaseRequest.TransferInProgress(repDesc.ReplicaID); !ok {
				// ...and there is no transfer pending.
				return status, true
			}
		}
	}
	return storagepb.LeaseStatus{}, false
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
func (r *Replica) redirectOnOrAcquireLease(
	ctx context.Context,
) (storagepb.LeaseStatus, *roachpb.Error) {
	if status, ok := r.leaseGoodToGo(ctx); ok {
		return status, nil
	}

	// Loop until the lease is held or the replica ascertains the actual
	// lease holder. Returns also on context.Done() (timeout or cancellation).
	var status storagepb.LeaseStatus
	for attempt := 1; ; attempt++ {
		timestamp := r.store.Clock().Now()
		llHandle, pErr := func() (*leaseRequestHandle, *roachpb.Error) {
			r.mu.Lock()
			defer r.mu.Unlock()

			status = r.leaseStatus(*r.mu.state.Lease, timestamp, r.mu.minLeaseProposedTS)
			switch status.State {
			case storagepb.LeaseState_ERROR:
				// Lease state couldn't be determined.
				log.VEventf(ctx, 2, "lease state couldn't be determined")
				return nil, roachpb.NewError(
					newNotLeaseHolderError(nil, r.store.StoreID(), r.mu.state.Desc))

			case storagepb.LeaseState_VALID, storagepb.LeaseState_STASIS:
				if !status.Lease.OwnedBy(r.store.StoreID()) {
					_, stillMember := r.mu.state.Desc.GetReplicaDescriptor(status.Lease.Replica.StoreID)
					if !stillMember {
						// This would be the situation in which the lease holder gets removed when
						// holding the lease, or in which a lease request erroneously gets accepted
						// for a replica that is not in the replica set. Neither of the two can
						// happen in normal usage since appropriate mechanisms have been added:
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
						// proposed on the lease holder, the spanlatch manager then serializes
						// everything. But lease requests get created on followers based on their
						// local state and thus without being sequenced through latching. Thus
						// a recently removed follower (unaware of its own removal) could submit
						// a proposal for the lease (correctly using as a ProposerLease the last
						// active lease), and would receive it given the up-to-date ProposerLease.
						// Hence, an extra check is in order: processRaftCommand makes sure that
						// lease requests for a replica not in the descriptor are bounced.
						//
						// However, this is possible if the `cockroach debug
						// unsafe-remove-dead-replicas` command has been used, so
						// this is just a logged error instead of a fatal
						// assertion.
						log.Errorf(ctx, "lease %s owned by replica %+v that no longer exists",
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
				if status.State == storagepb.LeaseState_STASIS {
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

			case storagepb.LeaseState_EXPIRED:
				// No active lease: Request renewal if a renewal is not already pending.
				log.VEventf(ctx, 2, "request range lease (attempt #%d)", attempt)
				return r.requestLeaseLocked(ctx, status), nil

			case storagepb.LeaseState_PROSCRIBED:
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
			return storagepb.LeaseStatus{}, pErr
		}
		if llHandle == nil {
			// We own a valid lease.
			return status, nil
		}

		// Wait for the range lease to finish, or the context to expire.
		pErr = func() (pErr *roachpb.Error) {
			slowTimer := timeutil.NewTimer()
			defer slowTimer.Stop()
			slowTimer.Reset(base.SlowRequestThreshold)
			tBegin := timeutil.Now()
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
					defer func() {
						r.store.metrics.SlowLeaseRequests.Dec(1)
						log.Infof(ctx, "slow lease acquisition finished after %s with error %v after %d attempts", timeutil.Since(tBegin), pErr, attempt)
					}()
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
			return storagepb.LeaseStatus{}, pErr
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

// DescAndZone returns the authoritative range descriptor as well
// as the zone config for the replica.
func (r *Replica) DescAndZone() (*roachpb.RangeDescriptor, *config.ZoneConfig) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.state.Desc, r.mu.zone
}

// Desc returns the authoritative range descriptor, acquiring a replica lock in
// the process.
func (r *Replica) Desc() *roachpb.RangeDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.state.Desc
}

func (r *Replica) descRLocked() *roachpb.RangeDescriptor {
	return r.mu.state.Desc
}

// NodeID returns the ID of the node this replica belongs to.
func (r *Replica) NodeID() roachpb.NodeID {
	return r.store.nodeDesc.NodeID
}

// ClusterSettings returns the node's ClusterSettings.
func (r *Replica) ClusterSettings() *cluster.Settings {
	return r.store.cfg.Settings
}

// StoreID returns the Replica's StoreID.
func (r *Replica) StoreID() roachpb.StoreID {
	return r.store.StoreID()
}

// EvalKnobs returns the EvalContext's Knobs.
func (r *Replica) EvalKnobs() storagebase.BatchEvalTestingKnobs {
	return r.store.cfg.TestingKnobs.EvalKnobs
}

// Clock returns the hlc clock shared by this replica.
func (r *Replica) Clock() *hlc.Clock {
	return r.store.Clock()
}

// DB returns the Replica's client DB.
func (r *Replica) DB() *client.DB {
	return r.store.DB()
}

// Engine returns the Replica's underlying Engine. In most cases the
// evaluation Batch should be used instead.
func (r *Replica) Engine() engine.Engine {
	return r.store.Engine()
}

// AbortSpan returns the Replica's AbortSpan.
func (r *Replica) AbortSpan() *abortspan.AbortSpan {
	// Despite its name, the AbortSpan doesn't hold on-disk data in
	// memory. It just provides methods that take a Batch, so SpanSet
	// declarations are enforced there.
	return r.abortSpan
}

// GetLimiters returns the Replica's limiters.
func (r *Replica) GetLimiters() *batcheval.Limiters {
	return &r.store.limiters
}

// GetTxnWaitQueue returns the Replica's txnwait.Queue.
func (r *Replica) GetTxnWaitQueue() *txnwait.Queue {
	return r.txnWaitQueue
}

// GetTerm returns the term of the given index in the raft log.
func (r *Replica) GetTerm(i uint64) (uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.raftTermRLocked(i)
}

// GetRangeID returns the Range ID.
func (r *Replica) GetRangeID() roachpb.RangeID {
	return r.RangeID
}

// GetGCThreshold returns the GC threshold.
func (r *Replica) GetGCThreshold() hlc.Timestamp {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return *r.mu.state.GCThreshold
}

// GetTxnSpanGCThreshold returns the time of the replica's last transaction span
// GC.
func (r *Replica) GetTxnSpanGCThreshold() hlc.Timestamp {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return *r.mu.state.TxnSpanGCThreshold
}

// setDesc atomically sets the replica's descriptor. It requires raftMu to be
// locked.
func (r *Replica) setDesc(ctx context.Context, desc *roachpb.RangeDescriptor) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if desc.RangeID != r.RangeID {
		log.Fatalf(ctx, "range descriptor ID (%d) does not match replica's range ID (%d)",
			desc.RangeID, r.RangeID)
	}
	if r.mu.state.Desc != nil && r.mu.state.Desc.IsInitialized() &&
		(desc == nil || !desc.IsInitialized()) {
		log.Fatalf(ctx, "cannot replace initialized descriptor with uninitialized one: %+v -> %+v",
			r.mu.state.Desc, desc)
	}
	if r.mu.state.Desc != nil && r.mu.state.Desc.IsInitialized() &&
		!r.mu.state.Desc.StartKey.Equal(desc.StartKey) {
		log.Fatalf(ctx, "attempted to change replica's start key from %s to %s",
			r.mu.state.Desc.StartKey, desc.StartKey)
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
	return roachpb.ReplicaDescriptor{}, roachpb.NewRangeNotFoundError(r.RangeID, r.store.StoreID())
}

func (r *Replica) getMergeCompleteCh() chan struct{} {
	r.mu.RLock()
	mergeCompleteCh := r.mu.mergeComplete
	r.mu.RUnlock()
	return mergeCompleteCh
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
// This accessor is thread-safe, but provides no guarantees about its
// synchronization with any concurrent writes.
func (r *Replica) GetMVCCStats() enginepb.MVCCStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return *r.mu.state.Stats
}

// GetSplitQPS returns the Replica's queries/s request rate.
// The value returned represents the QPS recorded at the time of the
// last request which can be found using GetLastRequestTime().
// NOTE: This should only be used for load based splitting, only
// works when the load based splitting cluster setting is enabled.
//
// Use QueriesPerSecond() for current QPS stats for all other purposes.
func (r *Replica) GetSplitQPS() float64 {
	r.splitMu.Lock()
	defer r.splitMu.Unlock()
	return r.splitMu.qps
}

// GetLastRequestTime returns the most recent time in nanos
// when the last rate was recorded.
// NOTE: This should only be used for load based splitting, only
// works when the load based splitting cluster setting is enabled.
func (r *Replica) GetLastRequestTime() time.Time {
	r.splitMu.Lock()
	defer r.splitMu.Unlock()
	return r.splitMu.lastReqTime
}

// ContainsKey returns whether this range contains the specified key.
//
// TODO(bdarnell): This is not the same as RangeDescriptor.ContainsKey.
func (r *Replica) ContainsKey(key roachpb.Key) bool {
	return storagebase.ContainsKey(*r.Desc(), key)
}

// ContainsKeyRange returns whether this range contains the specified
// key range from start to end.
func (r *Replica) ContainsKeyRange(start, end roachpb.Key) bool {
	return storagebase.ContainsKeyRange(*r.Desc(), start, end)
}

// GetLastReplicaGCTimestamp reads the timestamp at which the replica was
// last checked for removal by the replica gc queue.
func (r *Replica) GetLastReplicaGCTimestamp(ctx context.Context) (hlc.Timestamp, error) {
	key := keys.RangeLastReplicaGCTimestampKey(r.RangeID)
	var timestamp hlc.Timestamp
	_, err := engine.MVCCGetProto(ctx, r.store.Engine(), key, hlc.Timestamp{}, &timestamp,
		engine.MVCCGetOptions{})
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
		_, err := engine.MVCCGetProto(ctx, r.store.Engine(), key, hlc.Timestamp{}, &timestamp,
			engine.MVCCGetOptions{})
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
func (r *Replica) State() storagepb.RangeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var ri storagepb.RangeInfo
	ri.ReplicaState = *(protoutil.Clone(&r.mu.state)).(*storagepb.ReplicaState)
	ri.LastIndex = r.mu.lastIndex
	ri.NumPending = uint64(len(r.mu.proposals))
	ri.RaftLogSize = r.mu.raftLogSize
	ri.NumDropped = uint64(r.mu.droppedMessages)
	if r.mu.proposalQuota != nil {
		ri.ApproximateProposalQuota = r.mu.proposalQuota.approximateQuota()
	}
	ri.RangeMaxBytes = *r.mu.zone.RangeMaxBytes
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

	if err := r.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
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
	return r.sendWithRangeID(ctx, r.RangeID, ba)
}

// sendWithRangeID takes an unused rangeID argument so that the range
// ID will be accessible in stack traces (both in panics and when
// sampling goroutines from a live server). This line is subject to
// the whims of the compiler and it can be difficult to find the right
// value, but as of this writing the following example shows a stack
// while processing range 21 (0x15) (the first occurrence of that
// number is the rangeID argument, the second is within the encoded
// BatchRequest, although we don't want to rely on that occurring
// within the portion printed in the stack trace):
//
// github.com/cockroachdb/cockroach/pkg/storage.(*Replica).sendWithRangeID(0xc420d1a000, 0x64bfb80, 0xc421564b10, 0x15, 0x153fd4634aeb0193, 0x0, 0x100000001, 0x1, 0x15, 0x0, ...)
func (r *Replica) sendWithRangeID(
	ctx context.Context, rangeID roachpb.RangeID, ba roachpb.BatchRequest,
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
			pErr = roachpb.NewError(roachpb.NewRangeNotFoundError(r.RangeID, r.store.StoreID()))
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
	r.mu.RLock()
	desc := r.mu.state.Desc
	threshold := r.mu.state.GCThreshold
	r.mu.RUnlock()
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
		if repl := r.store.LookupReplica(rspan.Key); repl != nil {
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

// checkBatchRequest verifies BatchRequest validity requirements. In particular,
// the batch must have an assigned timestamp, and either all requests must be
// read-only, or none.
//
// TODO(tschottdorf): should check that request is contained in range
// and that EndTransaction only occurs at the very end.
func (r *Replica) checkBatchRequest(ba roachpb.BatchRequest, isReadOnly bool) error {
	if ba.Timestamp == (hlc.Timestamp{}) {
		// For transactional requests, Store.Send sets the timestamp. For non-
		// transactional requests, the client sets the timestamp. Either way, we
		// need to have a timestamp at this point.
		return errors.New("Replica.checkBatchRequest: batch does not have timestamp assigned")
	}
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

// endCmds holds necessary information to end a batch after Raft
// command processing.
type endCmds struct {
	repl *Replica
	lg   *spanlatch.Guard
	ba   roachpb.BatchRequest
}

// done releases the latches acquired by the command and updates
// the timestamp cache using the final timestamp of each command.
func (ec *endCmds) done(
	br *roachpb.BatchResponse, pErr *roachpb.Error, retry proposalReevaluationReason,
) {
	// Update the timestamp cache if the request is not being re-evaluated. Each
	// request is considered in turn; only those marked as affecting the cache are
	// processed. Inconsistent reads are excluded.
	if retry == proposalNoReevaluation && ec.ba.ReadConsistency == roachpb.CONSISTENT {
		ec.repl.updateTimestampCache(&ec.ba, br, pErr)
	}

	// Release the latches acquired by the request back to the spanlatch
	// manager. Must be done AFTER the timestamp cache is updated.
	if ec.lg != nil {
		ec.repl.latchMgr.Release(ec.lg)
	}
}

func (r *Replica) collectSpans(ba *roachpb.BatchRequest) (*spanset.SpanSet, error) {
	spans := &spanset.SpanSet{}
	// TODO(bdarnell): need to make this less global when local
	// latches are used more heavily. For example, a split will
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

	desc := r.Desc()
	for _, union := range ba.Requests {
		inner := union.GetInner()
		if cmd, ok := batcheval.LookupCommand(inner.Method()); ok {
			cmd.DeclareKeys(*desc, ba.Header, inner, spans)
		} else {
			return nil, errors.Errorf("unrecognized command %s", inner.Method())
		}
	}

	// Commands may create a large number of duplicate spans. De-duplicate
	// them to reduce the number of spans we pass to the spanlatch manager.
	spans.SortAndDedup()

	// If any command gave us spans that are invalid, bail out early
	// (before passing them to the spanlatch manager, which may panic).
	if err := spans.Validate(); err != nil {
		return nil, err
	}
	return spans, nil
}

// beginCmds waits for any in-flight, conflicting commands to complete. This
// includes merges in their critical phase or overlapping, already-executing
// commands.
//
// More specifically, after waiting for in-flight merges, beginCmds acquires
// latches for the request based on keys affected by the batched commands.
// This gates subsequent commands with overlapping keys or key ranges. It
// returns a cleanup function to be called when the commands are done and can be
// removed from the queue, and whose returned error is to be used in place of
// the supplied error.
func (r *Replica) beginCmds(
	ctx context.Context, ba *roachpb.BatchRequest, spans *spanset.SpanSet,
) (*endCmds, error) {
	// Only acquire latches for consistent operations.
	var lg *spanlatch.Guard
	if ba.ReadConsistency == roachpb.CONSISTENT {
		// Check for context cancellation before acquiring latches.
		if err := ctx.Err(); err != nil {
			log.VEventf(ctx, 2, "%s before acquiring latches: %s", err, ba.Summary())
			return nil, errors.Wrap(err, "aborted before acquiring latches")
		}

		var beforeLatch time.Time
		if log.ExpensiveLogEnabled(ctx, 2) {
			beforeLatch = timeutil.Now()
		}

		// Acquire latches for all the request's declared spans to ensure
		// protected access and to avoid interacting requests from operating at
		// the same time. The latches will be held for the duration of request.
		var err error
		lg, err = r.latchMgr.Acquire(ctx, spans, ba.Timestamp)
		if err != nil {
			return nil, err
		}

		if !beforeLatch.IsZero() {
			dur := timeutil.Since(beforeLatch)
			log.VEventf(ctx, 2, "waited %s to acquire latches", dur)
		}

		if filter := r.store.cfg.TestingKnobs.TestingLatchFilter; filter != nil {
			if pErr := filter(*ba); pErr != nil {
				r.latchMgr.Release(lg)
				return nil, pErr.GoError()
			}
		}

		if r.getMergeCompleteCh() != nil && !ba.IsSingleSubsumeRequest() {
			// The replica is being merged into its left-hand neighbor. This request
			// cannot proceed until the merge completes, signaled by the closing of
			// the channel.
			//
			// It is very important that this check occur after we have acquired latches
			// from the spanlatch manager. Only after we release these latches are we
			// guaranteed that we're not racing with a Subsume command. (Subsume
			// commands declare a conflict with all other commands.)
			//
			// Note that Subsume commands are exempt from waiting on the mergeComplete
			// channel. This is necessary to avoid deadlock. While normally a Subsume
			// request will trigger the installation of a mergeComplete channel after
			// it is executed, it may sometimes execute after the mergeComplete
			// channel has been installed. Consider the case where the RHS replica
			// acquires a new lease after the merge transaction deletes its local
			// range descriptor but before the Subsume command is sent. The lease
			// acquisition request will notice the intent on the local range
			// descriptor and install a mergeComplete channel. If the forthcoming
			// Subsume blocked on that channel, the merge transaction would deadlock.
			//
			// This exclusion admits a small race condition. If a Subsume request is
			// sent to the right-hand side of a merge, outside of a merge transaction,
			// after the merge has committed but before the RHS has noticed that the
			// merge has committed, the request may return stale data. Since the merge
			// has committed, the LHS may have processed writes to the keyspace
			// previously owned by the RHS that the RHS is unaware of. This window
			// closes quickly, as the RHS will soon notice the merge transaction has
			// committed and mark itself as destroyed, which prevents it from serving
			// all traffic, including Subsume requests.
			//
			// In our current, careful usage of Subsume, this race condition is
			// irrelevant. Subsume is only sent from within a merge transaction, and
			// merge transactions read the RHS descriptor at the beginning of the
			// transaction to verify that it has not already been merged away.
			//
			// We can't wait for the merge to complete here, though. The replica might
			// need to respond to a Subsume request in order for the merge to
			// complete, and blocking here would force that Subsume request to sit in
			// hold its latches forever, deadlocking the merge. Instead, we release
			// the latches we acquired above and return a MergeInProgressError.
			// The store will catch that error and resubmit the request after
			// mergeCompleteCh closes. See #27442 for the full context.
			log.Event(ctx, "waiting on in-progress merge")
			r.latchMgr.Release(lg)
			return nil, &roachpb.MergeInProgressError{}
		}
	} else {
		log.Event(ctx, "operation accepts inconsistent results")
	}

	// Handle load-based splitting.
	if r.SplitByLoadEnabled() {
		r.splitMu.Lock()
		r.splitMu.count += int64(len(ba.Requests))
		record, split := r.needsSplitByLoadLocked()
		if record {
			if boundarySpan := spans.BoundarySpan(spanset.SpanGlobal); boundarySpan != nil {
				r.splitMu.splitFinder.Record(*boundarySpan, rand.Intn)
			}
		}
		r.splitMu.Unlock()
		// Add to the split queue after releasing splitMu.
		if split {
			r.store.splitQueue.MaybeAdd(r, r.store.Clock().Now())
		}
	}

	ec := &endCmds{
		repl: r,
		lg:   lg,
		ba:   *ba,
	}
	return ec, nil
}

// executeAdminBatch executes the command directly. There is no interaction
// with the spanlatch manager or the timestamp cache, as admin commands
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
				ctx, tArgs.ChangeType, target, r.Desc(), storagepb.ReasonAdminRequest, "")
			if err != nil {
				break
			}
		}
		pErr = roachpb.NewError(err)
		resp = &roachpb.AdminChangeReplicasResponse{}

	case *roachpb.AdminRelocateRangeRequest:
		err := r.store.AdminRelocateRange(ctx, *r.Desc(), tArgs.Targets)
		pErr = roachpb.NewError(err)
		resp = &roachpb.AdminRelocateRangeResponse{}

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

	if ba.Header.ReturnRangeInfo {
		returnRangeInfo(resp, r)
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
	ctx context.Context, ba *roachpb.BatchRequest, status storagepb.LeaseStatus,
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
	if status.State == storagepb.LeaseState_VALID {
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

// maybeWatchForMerge checks whether a merge of this replica into its left
// neighbor is in its critical phase and, if so, arranges to block all requests
// until the merge completes.
func (r *Replica) maybeWatchForMerge(ctx context.Context) error {
	desc := r.Desc()
	descKey := keys.RangeDescriptorKey(desc.StartKey)
	_, intent, err := engine.MVCCGet(ctx, r.Engine(), descKey, r.Clock().Now(),
		engine.MVCCGetOptions{Inconsistent: true})
	if err != nil {
		return err
	} else if intent == nil {
		return nil
	}
	val, _, err := engine.MVCCGetAsTxn(
		ctx, r.Engine(), descKey, intent.Txn.Timestamp, intent.Txn)
	if err != nil {
		return err
	} else if val != nil {
		return nil
	}

	// At this point, we know we have a deletion intent on our range descriptor.
	// That means a merge is in progress. Block all commands until we can
	// retrieve an updated range descriptor from meta2, which will indicate
	// whether the merge succeeded or not.

	mergeCompleteCh := make(chan struct{})
	r.mu.Lock()
	if r.mu.mergeComplete != nil {
		// Another request already noticed the merge, installed a mergeComplete
		// channel, and launched a goroutine to watch for the merge's completion.
		// Nothing more to do.
		r.mu.Unlock()
		return nil
	}
	r.mu.mergeComplete = mergeCompleteCh
	// The RHS of a merge is not permitted to quiesce while a mergeComplete
	// channel is installed. (If the RHS is quiescent when the merge commits, any
	// orphaned followers would fail to queue themselves for GC.) Unquiesce the
	// range in case it managed to quiesce between when the Subsume request
	// arrived and now, which is rare but entirely legal.
	r.unquiesceLocked()
	r.mu.Unlock()

	taskCtx := r.AnnotateCtx(context.Background())
	err = r.store.stopper.RunAsyncTask(taskCtx, "wait-for-merge", func(ctx context.Context) {
		var pushTxnRes *roachpb.PushTxnResponse
		for retry := retry.Start(base.DefaultRetryOptions()); retry.Next(); {
			// Wait for the merge transaction to complete by attempting to push it. We
			// don't want to accidentally abort the merge transaction, so we use the
			// minimum transaction priority. Note that a push type of
			// roachpb.PUSH_TOUCH, though it might appear more semantically correct,
			// returns immediately and causes us to spin hot, whereas
			// roachpb.PUSH_ABORT efficiently blocks until the transaction completes.
			res, pErr := client.SendWrapped(ctx, r.DB().NonTransactionalSender(), &roachpb.PushTxnRequest{
				RequestHeader: roachpb.RequestHeader{Key: intent.Txn.Key},
				PusherTxn: roachpb.Transaction{
					TxnMeta: enginepb.TxnMeta{Priority: roachpb.MinTxnPriority},
				},
				PusheeTxn: intent.Txn,
				Now:       r.Clock().Now(),
				PushType:  roachpb.PUSH_ABORT,
			})
			if pErr != nil {
				select {
				case <-r.store.stopper.ShouldQuiesce():
					// The server is shutting down. The error while pushing the
					// transaction was probably caused by the shutdown, so ignore it.
					return
				default:
					log.Warningf(ctx, "error while watching for merge to complete: PushTxn: %s", pErr)
					// We can't safely unblock traffic until we can prove that the merge
					// transaction is committed or aborted. Nothing to do but try again.
					continue
				}
			}
			pushTxnRes = res.(*roachpb.PushTxnResponse)
			break
		}

		var mergeCommitted bool
		switch pushTxnRes.PusheeTxn.Status {
		case roachpb.PENDING:
			log.Fatalf(ctx, "PushTxn returned while merge transaction %s was still pending",
				intent.Txn.ID.Short())
		case roachpb.COMMITTED:
			// If PushTxn claims that the transaction committed, then the transaction
			// definitely committed.
			mergeCommitted = true
		case roachpb.ABORTED:
			// If PushTxn claims that the transaction aborted, it's not a guarantee
			// that the transaction actually aborted. It could also mean that the
			// transaction completed, resolved its intents, and GC'd its transaction
			// record before our PushTxn arrived. To figure out what happened, we
			// need to look in meta2.
			var getRes *roachpb.GetResponse
			for retry := retry.Start(base.DefaultRetryOptions()); retry.Next(); {
				metaKey := keys.RangeMetaKey(desc.EndKey)
				res, pErr := client.SendWrappedWith(ctx, r.DB().NonTransactionalSender(), roachpb.Header{
					// Use READ_UNCOMMITTED to avoid trying to resolve intents, since
					// resolving those intents might involve sending requests to this
					// range, and that could deadlock. See the comment on
					// TestStoreRangeMergeConcurrentSplit for details.
					ReadConsistency: roachpb.READ_UNCOMMITTED,
				}, &roachpb.GetRequest{
					RequestHeader: roachpb.RequestHeader{Key: metaKey.AsRawKey()},
				})
				if pErr != nil {
					select {
					case <-r.store.stopper.ShouldQuiesce():
						// The server is shutting down. The error while fetching the range
						// descriptor was probably caused by the shutdown, so ignore it.
						return
					default:
						log.Warningf(ctx, "error while watching for merge to complete: Get %s: %s", metaKey, pErr)
						// We can't safely unblock traffic until we can prove that the merge
						// transaction is committed or aborted. Nothing to do but try again.
						continue
					}
				}
				getRes = res.(*roachpb.GetResponse)
				break
			}
			if getRes.Value == nil {
				// A range descriptor with our end key is no longer present in meta2, so
				// the merge must have committed.
				mergeCommitted = true
			} else {
				// A range descriptor with our end key is still present in meta2. The
				// merge committed iff that range descriptor has a different range ID.
				var meta2Desc roachpb.RangeDescriptor
				if err := getRes.Value.GetProto(&meta2Desc); err != nil {
					log.Fatalf(ctx, "error while watching for merge to complete: "+
						"unmarshaling meta2 range descriptor: %s", err)
				}
				if meta2Desc.RangeID != r.RangeID {
					mergeCommitted = true
				}
			}
		}

		r.mu.Lock()
		if mergeCommitted && r.mu.destroyStatus.IsAlive() {
			// The merge committed but the left-hand replica on this store hasn't
			// subsumed this replica yet. Mark this replica as destroyed so it
			// doesn't serve requests when we close the mergeCompleteCh below.
			r.mu.destroyStatus.Set(roachpb.NewRangeNotFoundError(r.RangeID, r.store.StoreID()), destroyReasonMergePending)
		}
		// Unblock pending requests. If the merge committed, the requests will
		// notice that the replica has been destroyed and return an appropriate
		// error. If the merge aborted, the requests will be handled normally.
		r.mu.mergeComplete = nil
		close(mergeCompleteCh)
		r.mu.Unlock()
	})
	if err == stop.ErrUnavailable {
		// We weren't able to launch a goroutine to watch for the merge's completion
		// because the server is shutting down. Normally failing to launch the
		// watcher goroutine would wedge pending requests on the replica's
		// mergeComplete channel forever, but since we're shutting down those
		// requests will get dropped and retried on another node. Suppress the error.
		err = nil
	}
	return err
}

// executeReadOnlyBatch updates the read timestamp cache and waits for any
// overlapping writes currently processing through Raft ahead of us to
// clear via the latches.
func (r *Replica) executeReadOnlyBatch(
	ctx context.Context, ba roachpb.BatchRequest,
) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	// If the read is not inconsistent, the read requires the range lease or
	// permission to serve via follower reads.
	var status storagepb.LeaseStatus
	if ba.ReadConsistency.RequiresReadLease() {
		if status, pErr = r.redirectOnOrAcquireLease(ctx); pErr != nil {
			if lErr, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); ok &&
				FollowerReadsEnabled.Get(&r.store.cfg.Settings.SV) &&
				lErr.LeaseHolder != nil && lErr.Lease.Type() == roachpb.LeaseEpoch {

				r.mu.RLock()
				lai := r.mu.state.LeaseAppliedIndex
				r.mu.RUnlock()
				if !r.store.cfg.ClosedTimestamp.Provider.CanServe(
					lErr.LeaseHolder.NodeID, ba.Timestamp, r.RangeID, ctpb.Epoch(lErr.Lease.Epoch), ctpb.LAI(lai),
				) {
					r.store.cfg.ClosedTimestamp.Clients.Request(lErr.LeaseHolder.NodeID, r.RangeID)
					// NB: this can't go behind V(x) because the log message created by the
					// storage might be gigantic in real clusters, and we don't want to trip it
					// using logspy.
					if false {
						log.Warningf(ctx, "can't serve follower read for %s at epo %d lai %d, storage is %s",
							ba.Timestamp, lErr.Lease.Epoch, lai,
							r.store.cfg.ClosedTimestamp.Storage.(*ctstorage.MultiStorage).StringForNodes(lErr.LeaseHolder.NodeID),
						)
					}
					return nil, pErr
				}
				// This replica can serve this read!
				//
				// TODO(tschottdorf): once a read for a timestamp T has been served, the replica may
				// serve reads for that and smaller timestamps forever.
				log.Event(ctx, "serving via follower read")
			} else {
				return nil, pErr
			}
		}
	}
	r.limitTxnMaxTimestamp(ctx, &ba, status)

	spans, err := r.collectSpans(&ba)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	// Acquire latches to prevent overlapping commands from executing
	// until this command completes.
	log.Event(ctx, "acquire latches")
	endCmds, err := r.beginCmds(ctx, &ba, spans)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	log.Event(ctx, "waiting for read lock")
	r.readOnlyCmdMu.RLock()
	defer r.readOnlyCmdMu.RUnlock()

	// Guarantee we release the latches that we just acquired. It is
	// important that this is inside the readOnlyCmdMu lock so that the
	// timestamp cache update is synchronized. This is wrapped to delay
	// pErr evaluation to its value when returning.
	defer func() {
		endCmds.done(br, pErr, proposalNoReevaluation)
	}()

	// TODO(nvanbenschoten): Can this be moved into Replica.requestCanProceed?
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

	if result.Local.DetachMaybeWatchForMerge() {
		if err := r.maybeWatchForMerge(ctx); err != nil {
			return nil, roachpb.NewError(err)
		}
	}

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
		if err := r.store.intentResolver.cleanupIntentsAsync(ctx, r, intents, allowSyncProcessing); err != nil {
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
// range's replicated state. Requests taking this path are evaluated and ultimately
// serialized through Raft, but pass through additional machinery whose goal is
// to allow commands which commute to be proposed in parallel. The naive
// alternative, submitting requests to Raft one after another, paying massive
// latency, is only taken for commands whose effects may overlap.
//
// Internally, multiple iterations of the above process may take place
// due to the Raft proposal failing retryably, possibly due to proposal
// reordering or re-proposals. We call these retry "re-evaluations" since the
// request is evaluated again (against a fresh engine snapshot).
func (r *Replica) executeWriteBatch(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	for {
		// TODO(andrei): export some metric about re-evaluations.
		br, pErr, retry := r.tryExecuteWriteBatch(ctx, ba)
		if retry == proposalIllegalLeaseIndex {
			log.VEventf(ctx, 2, "retry: proposalIllegalLeaseIndex")
			if pErr != nil {
				log.Fatalf(ctx, "both error and retry returned: %s", pErr)
			}
			continue // retry
		}
		return br, pErr
	}
}

// tryExecuteWriteBatch is invoked by executeWriteBatch, which will
// call this method until it returns a non-retryable result (i.e. no
// proposalRetryReason is returned).
//
// Concretely,
//
// - Latches for the keys affected by the command are acquired (i.e.
//   tracked as in-flight mutations).
// - In doing so, we wait until no overlapping mutations are in flight.
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
//   registered with the timestamp cache, its latches are released, and
//   its result (which could be an error) is returned to the client.
//
// Returns exactly one of a response, an error or re-evaluation reason.
//
// NB: changing BatchRequest to a pointer here would have to be done cautiously
// as this method makes the assumption that it operates on a shallow copy (see
// call to applyTimestampCache).
func (r *Replica) tryExecuteWriteBatch(
	ctx context.Context, ba roachpb.BatchRequest,
) (br *roachpb.BatchResponse, pErr *roachpb.Error, retry proposalReevaluationReason) {
	startTime := timeutil.Now()

	if err := r.maybeBackpressureWriteBatch(ctx, ba); err != nil {
		return nil, roachpb.NewError(err), proposalNoReevaluation
	}

	spans, err := r.collectSpans(&ba)
	if err != nil {
		return nil, roachpb.NewError(err), proposalNoReevaluation
	}

	var endCmds *endCmds
	if !ba.IsLeaseRequest() {
		// Acquire latches to prevent overlapping commands from executing until
		// this command completes. Note that this must be done before getting
		// the max timestamp for the key(s), as timestamp cache is only updated
		// after preceding commands have been run to successful completion.
		log.Event(ctx, "acquire latches")
		var err error
		endCmds, err = r.beginCmds(ctx, &ba, spans)
		if err != nil {
			return nil, roachpb.NewError(err), proposalNoReevaluation
		}
	}

	// Guarantee we release the latches that we just acquired. This is
	// wrapped to delay pErr evaluation to its value when returning.
	defer func() {
		if endCmds != nil {
			endCmds.done(br, pErr, retry)
		}
	}()

	var lease roachpb.Lease
	var status storagepb.LeaseStatus
	// For lease commands, use the provided previous lease for verification.
	if ba.IsSingleSkipLeaseCheckRequest() {
		lease = ba.GetPrevLeaseForLeaseRequest()
	} else {
		// Other write commands require that this replica has the range
		// lease.
		if status, pErr = r.redirectOnOrAcquireLease(ctx); pErr != nil {
			return nil, pErr, proposalNoReevaluation
		}
		lease = status.Lease
	}
	r.limitTxnMaxTimestamp(ctx, &ba, status)

	minTS, untrack := r.store.cfg.ClosedTimestamp.Tracker.Track(ctx)
	defer untrack(ctx, 0, 0) // covers all error returns below

	// Examine the read and write timestamp caches for preceding
	// commands which require this command to move its timestamp
	// forward. Or, in the case of a transactional write, the txn
	// timestamp and possible write-too-old bool.
	if bumped, pErr := r.applyTimestampCache(ctx, &ba, minTS); pErr != nil {
		return nil, pErr, proposalNoReevaluation
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

	ch, tryAbandon, maxLeaseIndex, pErr := r.propose(ctx, lease, ba, endCmds, spans)
	if pErr != nil {
		if maxLeaseIndex != 0 {
			log.Fatalf(
				ctx, "unexpected max lease index %d assigned to failed proposal: %s, error %s",
				maxLeaseIndex, ba, pErr,
			)
		}
		return nil, pErr, proposalNoReevaluation
	}
	// A max lease index of zero is returned when no proposal was made or a lease was proposed.
	// In both cases, we don't need to communicate a MLAI.
	if maxLeaseIndex != 0 {
		untrack(ctx, r.RangeID, ctpb.LAI(maxLeaseIndex))
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
	tBegin := timeutil.Now()

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
				if err := r.store.intentResolver.cleanupIntentsAsync(ctx, r, propResult.Intents, true /* allowSync */); err != nil {
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
			defer func() {
				r.store.metrics.SlowRaftRequests.Dec(1)
				contextStr := ""
				if err := ctx.Err(); err != nil {
					contextStr = " with context cancellation"
				}
				log.Infof(ctx, "slow command %s finished after %s%s", ba, timeutil.Since(tBegin), contextStr)
			}()

		case <-ctxDone:
			// If our context was canceled, return an AmbiguousResultError
			// if the command isn't already being executed and using our
			// context, in which case we expect it to finish soon. The
			// AmbiguousResultError indicates to caller that the command may
			// have executed.
			if tryAbandon() {
				log.VEventf(ctx, 2, "context cancellation after %0.1fs of attempting command %s",
					timeutil.Since(startTime).Seconds(), ba)
				return nil, roachpb.NewError(roachpb.NewAmbiguousResultError(ctx.Err().Error())), proposalNoReevaluation
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
				return nil, roachpb.NewError(roachpb.NewAmbiguousResultError("server shutdown")), proposalNoReevaluation
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
		proposal.command = &storagepb.RaftCommand{
			ReplicatedEvalResult: res.Replicated,
			WriteBatch:           res.WriteBatch,
			LogicalOpLog:         res.LogicalOpLog,
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

		txn := pErr.GetTxn()
		if txn != nil && ba.Txn == nil {
			log.Fatalf(ctx, "error had a txn but batch is non-transactional. Err txn: %s", txn)
		}

		// Failed proposals can't have any Result except for what's
		// whitelisted here.
		intents := res.Local.DetachIntents()
		endTxns := res.Local.DetachEndTxns(true /* alwaysOnly */)
		res.Local = result.LocalResult{
			Intents: &intents,
			EndTxns: &endTxns,
			Metrics: res.Local.Metrics,
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
		!res.Replicated.Equal(storagepb.ReplicatedEvalResult{}) ||
		r.store.Clock().MaxOffset() == timeutil.ClocklessMaxOffset

	if needConsensus {
		// Set the proposal's WriteBatch, which is the serialized representation of
		// the proposals effect on RocksDB.
		res.WriteBatch = &storagepb.WriteBatch{
			Data: batch.Repr(),
		}

		// Set the proposal's replicated result, which contains metadata and
		// side-effects that are to be replicated to all replicas.
		res.Replicated.IsLeaseRequest = ba.IsLeaseRequest()
		res.Replicated.Timestamp = ba.Timestamp
		if r.store.cfg.Settings.Version.IsActive(cluster.VersionMVCCNetworkStats) {
			res.Replicated.Delta = ms.ToStatsDelta()
		} else {
			res.Replicated.DeprecatedDelta = &ms
		}
		// If the RangeAppliedState key is not being used and the cluster version is
		// high enough to guarantee that all current and future binaries will
		// understand the key, we send the migration flag through Raft. Because
		// there is a delay between command proposal and application, we may end up
		// setting this migration flag multiple times. This is ok, because the
		// migration is idempotent.
		// TODO(nvanbenschoten): This will be baked in to 2.1, so it can be removed
		// in the 2.2 release.
		r.mu.RLock()
		usingAppliedStateKey := r.mu.state.UsingAppliedStateKey
		r.mu.RUnlock()
		if !usingAppliedStateKey &&
			r.ClusterSettings().Version.IsMinSupported(cluster.VersionRangeAppliedStateKey) {
			if res.Replicated.State == nil {
				res.Replicated.State = &storagepb.ReplicaState{}
			}
			res.Replicated.State.UsingAppliedStateKey = true
		}
	}

	return &res, needConsensus, nil
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
		if log.V(3) {
			log.Infof(ctx, "not quiescing: %d pending commands", len(r.mu.proposals))
		}
		return false
	}
	if !r.mu.quiescent {
		if log.V(3) {
			log.Infof(ctx, "quiescing %d", r.RangeID)
		}
		r.mu.quiescent = true
		r.store.unquiescedReplicas.Lock()
		delete(r.store.unquiescedReplicas.m, r.RangeID)
		r.store.unquiescedReplicas.Unlock()
	} else if log.V(4) {
		log.Infof(ctx, "already quiesced")
	}
	return true
}

func (r *Replica) unquiesce() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unquiesceLocked()
}

func (r *Replica) unquiesceLocked() {
	r.unquiesceWithOptionsLocked(true /* campaignOnWake */)
}

func (r *Replica) unquiesceWithOptionsLocked(campaignOnWake bool) {
	if r.mu.quiescent && r.mu.internalRaftGroup != nil {
		ctx := r.AnnotateCtx(context.TODO())
		if log.V(3) {
			log.Infof(ctx, "unquiescing %d", r.RangeID)
		}
		r.mu.quiescent = false
		r.store.unquiescedReplicas.Lock()
		r.store.unquiescedReplicas.m[r.RangeID] = struct{}{}
		r.store.unquiescedReplicas.Unlock()
		if campaignOnWake {
			r.maybeCampaignOnWakeLocked(ctx)
		}
		now := timeutil.Now()
		for _, desc := range r.mu.state.Desc.Replicas {
			r.mu.lastUpdateTimes.update(desc.ReplicaID, now)
		}
	}
}

func (r *Replica) unquiesceAndWakeLeaderLocked() {
	if r.mu.quiescent && r.mu.internalRaftGroup != nil {
		ctx := r.AnnotateCtx(context.TODO())
		if log.V(3) {
			log.Infof(ctx, "unquiescing %d: waking leader", r.RangeID)
		}
		r.mu.quiescent = false
		r.store.unquiescedReplicas.Lock()
		r.store.unquiescedReplicas.m[r.RangeID] = struct{}{}
		r.store.unquiescedReplicas.Unlock()
		r.maybeCampaignOnWakeLocked(ctx)
		// Propose an empty command which will wake the leader.
		_ = r.mu.internalRaftGroup.Propose(encodeRaftCommandV1(makeIDKey(), nil))
	}
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
// The supplied livenessMap maps from node ID to a boolean indicating
// liveness. A range may be quiesced in the presence of non-live
// replicas if the remaining live replicas all meet the quiesce
// requirements. When a node considered non-live becomes live, the
// node liveness instance invokes a callback which causes all nodes to
// wakes up any ranges containing replicas owned by the newly-live
// node, allowing the out-of-date replicas to be brought back up to date.
// If livenessMap is nil, liveness data will not be used, meaning no range
// will quiesce if any replicas are behind, whether or not they are live.
// If any entry in the livenessMap is nil, then the missing node ID is
// treated as not live.
//
// TODO(peter): There remains a scenario in which a follower is left unquiesced
// while the leader is quiesced: the follower's receive queue is full and the
// "quiesce" message is dropped. This seems very very unlikely because if the
// follower isn't keeping up with raft messages it is unlikely that the leader
// would quiesce. The fallout from this situation are undesirable raft
// elections which will cause throughput hiccups to the range, but not
// correctness issues.
func (r *Replica) maybeQuiesceLocked(ctx context.Context, livenessMap IsLiveMap) bool {
	status, ok := shouldReplicaQuiesce(ctx, r, r.store.Clock().Now(), len(r.mu.proposals), livenessMap)
	if !ok {
		return false
	}
	return r.quiesceAndNotifyLocked(ctx, status)
}

type quiescer interface {
	descRLocked() *roachpb.RangeDescriptor
	raftStatusRLocked() *raft.Status
	raftLastIndexLocked() (uint64, error)
	hasRaftReadyRLocked() bool
	ownsValidLeaseRLocked(ts hlc.Timestamp) bool
	mergeInProgressRLocked() bool
	isDestroyedRLocked() (DestroyReason, error)
}

func (r *Replica) maybeTransferRaftLeadership(ctx context.Context) {
	r.mu.Lock()
	r.maybeTransferRaftLeadershipLocked(ctx)
	r.mu.Unlock()
}

// maybeTransferRaftLeadershipLocked attempts to transfer the leadership away
// from this node to the leaseholder, if this node is the current raft leader
// but not the leaseholder. We don't attempt to transfer leadership if the
// leaseholder is behind on applying the log.
//
// We like it when leases and raft leadership are collocated because that
// facilitates quick command application (requests generally need to make it to
// both the lease holder and the raft leader before being applied by other
// replicas).
func (r *Replica) maybeTransferRaftLeadershipLocked(ctx context.Context) {
	if r.store.TestingKnobs().DisableLeaderFollowsLeaseholder {
		return
	}
	lease := *r.mu.state.Lease
	if lease.OwnedBy(r.StoreID()) || !r.isLeaseValidRLocked(lease, r.Clock().Now()) {
		return
	}
	raftStatus := r.raftStatusRLocked()
	if raftStatus == nil || raftStatus.RaftState != raft.StateLeader {
		return
	}
	lhReplicaID := uint64(lease.Replica.ReplicaID)
	lhProgress, ok := raftStatus.Progress[lhReplicaID]
	if (ok && lhProgress.Match >= raftStatus.Commit) || r.mu.draining {
		log.VEventf(ctx, 1, "transferring raft leadership to replica ID %v", lhReplicaID)
		r.store.metrics.RangeRaftLeaderTransfers.Inc(1)
		r.mu.internalRaftGroup.TransferLeader(lhReplicaID)
	}
}

func (r *Replica) mergeInProgressRLocked() bool {
	return r.mu.mergeComplete != nil
}

// shouldReplicaQuiesce determines if a replica should be quiesced. All of the
// access to Replica internals are gated by the quiescer interface to
// facilitate testing. Returns the raft.Status and true on success, and (nil,
// false) on failure.
func shouldReplicaQuiesce(
	ctx context.Context, q quiescer, now hlc.Timestamp, numProposals int, livenessMap IsLiveMap,
) (*raft.Status, bool) {
	if testingDisableQuiescence {
		return nil, false
	}
	if numProposals != 0 {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: %d pending commands", numProposals)
		}
		return nil, false
	}
	if q.mergeInProgressRLocked() {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: merge in progress")
		}
		return nil, false
	}
	if _, err := q.isDestroyedRLocked(); err != nil {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: replica destroyed")
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
	for _, rep := range q.descRLocked().Replicas {
		if uint64(rep.ReplicaID) == status.ID {
			foundSelf = true
		}
		if progress, ok := status.Progress[uint64(rep.ReplicaID)]; !ok {
			if log.V(4) {
				log.Infof(ctx, "not quiescing: could not locate replica %d in progress: %+v",
					rep.ReplicaID, progress)
			}
			return nil, false
		} else if progress.Match != status.Applied {
			// Skip any node in the descriptor which is not live.
			if livenessMap != nil && !livenessMap[rep.NodeID].IsLive {
				if log.V(4) {
					log.Infof(ctx, "skipping node %d because not live. Progress=%+v",
						rep.NodeID, progress)
				}
				continue
			}
			if log.V(4) {
				log.Infof(ctx, "not quiescing: replica %d match (%d) != applied (%d)",
					rep.ReplicaID, progress.Match, status.Applied)
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

	for id, prog := range status.Progress {
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

		// Attach the commit as min(prog.Match, status.Commit). This is exactly
		// the same as what raft.sendHeartbeat does. See the comment there for
		// an explanation.
		//
		// If the follower is behind, we don't tell it that we're quiescing.
		// This ensures that if the follower receives the heartbeat then it will
		// unquiesce the Range and be caught up by the leader. Remember that we
		// only allow Ranges to quiesce with straggling Replicas if we believe
		// those Replicas are on dead nodes.
		commit := status.Commit
		quiesce := true
		if prog.Match < status.Commit {
			commit = prog.Match
			quiesce = false
		}
		msg := raftpb.Message{
			From:   uint64(r.mu.replicaID),
			To:     id,
			Type:   raftpb.MsgHeartbeat,
			Term:   status.Term,
			Commit: commit,
		}

		if !r.maybeCoalesceHeartbeat(ctx, msg, toReplica, fromReplica, quiesce) {
			log.Fatalf(ctx, "failed to coalesce known heartbeat: %v", msg)
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

// checkIfTxnAborted checks the txn AbortSpan for the given
// transaction. In case the transaction has been aborted, return a
// transaction abort error.
func checkIfTxnAborted(
	ctx context.Context, rec batcheval.EvalContext, b engine.Reader, txn roachpb.Transaction,
) *roachpb.Error {
	var entry roachpb.AbortSpanEntry
	aborted, err := rec.AbortSpan().Get(ctx, b, txn.ID, &entry)
	if err != nil {
		return roachpb.NewError(roachpb.NewReplicaCorruptionError(
			errors.Wrap(err, "could not read from AbortSpan")))
	}
	if aborted {
		// We hit the cache, so let the transaction restart.
		log.VEventf(ctx, 1, "found AbortSpan entry for %s with priority %d",
			txn.ID.Short(), entry.Priority)
		newTxn := txn.Clone()
		if entry.Priority > newTxn.Priority {
			newTxn.Priority = entry.Priority
		}
		newTxn.Status = roachpb.ABORTED
		return roachpb.NewErrorWithTxn(
			roachpb.NewTransactionAbortedError(roachpb.ABORT_REASON_ABORT_SPAN), &newTxn)
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
		_, hasBegin := ba.GetArg(roachpb.BeginTransaction)
		arg, _ := ba.GetArg(roachpb.EndTransaction)
		etArg := arg.(*roachpb.EndTransactionRequest)

		// Try executing with transaction stripped. We use the transaction timestamp
		// to write any values as it may have been advanced by the timestamp cache.
		strippedBa := ba
		strippedBa.Timestamp = strippedBa.Txn.Timestamp
		strippedBa.Txn = nil
		if hasBegin {
			strippedBa.Requests = ba.Requests[1 : len(ba.Requests)-1] // strip begin/end txn reqs
		} else {
			strippedBa.Requests = ba.Requests[:len(ba.Requests)-1] // strip end txn req
		}

		// If there were no refreshable spans earlier in the txn
		// (e.g. earlier gets or scans), then the batch can be retried
		// locally in the event of write too old errors.
		retryLocally := etArg.NoRefreshSpans && !ba.Txn.OrigTimestampWasObserved

		// If all writes occurred at the intended timestamp, we've succeeded on the fast path.
		rec := NewReplicaEvalContext(r, spans)
		batch, br, res, pErr := r.evaluateWriteBatchWithLocalRetries(
			ctx, idKey, rec, &ms, strippedBa, spans, retryLocally,
		)
		if pErr == nil && (ba.Timestamp == br.Timestamp ||
			(retryLocally && !batcheval.IsEndTransactionExceedingDeadline(br.Timestamp, *etArg))) {
			clonedTxn := ba.Txn.Clone()
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
				innerResult, err := batcheval.RunCommitTrigger(ctx, rec, batch, &ms, *etArg, &clonedTxn)
				if err != nil {
					return batch, ms, br, res, roachpb.NewErrorf("failed to run commit trigger: %s", err)
				}
				if err := res.MergeAndDestroy(innerResult); err != nil {
					return batch, ms, br, res, roachpb.NewError(err)
				}
			}

			br.Txn = &clonedTxn
			// Add placeholder responses for begin & end transaction requests.
			var resps []roachpb.ResponseUnion
			if hasBegin {
				resps = make([]roachpb.ResponseUnion, len(br.Responses)+2)
				resps[0].MustSetInner(&roachpb.BeginTransactionResponse{})
				copy(resps[1:], br.Responses)
			} else {
				resps = append(br.Responses, roachpb.ResponseUnion{})
			}
			resps[len(resps)-1].MustSetInner(&roachpb.EndTransactionResponse{OnePhaseCommit: true})
			br.Responses = resps
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
		var opLogger *engine.OpLoggerBatch
		if RangefeedEnabled.Get(&r.store.cfg.Settings.SV) {
			// TODO(nvanbenschoten): once we get rid of the RangefeedEnabled
			// cluster setting we'll need a way to turn this on when any
			// replica (not just the leaseholder) wants it and off when no
			// replicas want it. This turns out to be pretty involved.
			//
			// The current plan is to:
			// - create a range-id local key that stores all replicas that are
			//   subscribed to logical operations, along with their corresponding
			//   liveness epoch.
			// - create a new command that adds or subtracts replicas from this
			//   structure. The command will be a write across the entire replica
			//   span so that it is serialized with all writes.
			// - each replica will add itself to this set when it first needs
			//   logical ops. It will then wait until it sees the replicated command
			//   that added itself pop out through Raft so that it knows all
			//   commands that are missing logical ops are gone.
			// - It will then proceed as normal, relying on the logical ops to
			//   always be included on the raft commands. When its no longer
			//   needs logical ops, it will remove itself from the set.
			// - The leaseholder will have a new queue to detect registered
			//   replicas that are no longer live and remove them from the
			//   set to prevent "leaking" subscriptions.
			// - The condition here to add logical logging will be:
			//     if len(replicaState.logicalOpsSubs) > 0 { ... }
			//
			// An alternative to this is the reduce the cost of the including
			// the logical op log to a negligible amount such that it can be
			// included on all raft commands, regardless of whether any replica
			// has a rangefeed running or not.
			//
			// Another alternative is to make the setting table/zone-scoped
			// instead of a fine-grained per-replica state.
			opLogger = engine.NewOpLoggerBatch(batch)
			batch = opLogger
		}
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
		if opLogger != nil {
			res.LogicalOpLog = &storagepb.LogicalOpLog{
				Ops: opLogger.LogicalOps(),
			}
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
	if !ba.IsCompleteTransaction() {
		return false
	}
	arg, _ := ba.GetArg(roachpb.EndTransaction)
	etArg := arg.(*roachpb.EndTransactionRequest)
	if batcheval.IsEndTransactionExceedingDeadline(ba.Txn.Timestamp, *etArg) {
		return false
	}
	if retry, _ := batcheval.IsEndTransactionTriggeringRetryError(ba.Txn, *etArg); retry {
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
	iter := batch.NewIterator(engine.IterOptions{
		// We want to include maxKey in our scan. Since UpperBound is exclusive, we
		// need to set it to the key after maxKey.
		UpperBound: maxKey.Next(),
	})
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
	// non-pointer fields (Sequence, DeprecatedBatchIndex, WriteTooOld, Timestamp),
	// so this saves a few allocs.
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
			// We don't check the abort span for a couple of special requests:
			// - if the request is asking to abort the transaction, then don't check the
			// AbortSpan; we don't want the request to be rejected if the transaction
			// has already been aborted.
			// - heartbeats don't check the abort span. If the txn is aborted, they'll
			// return an aborted proto in their otherwise successful response.
			singleAbort := ba.IsSingleEndTransactionRequest() &&
				!ba.Requests[0].GetInner().(*roachpb.EndTransactionRequest).Commit
			if !singleAbort && !ba.IsSingleHeartbeatTxnRequest() {
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
			// Sequence numbers used to be set on each BatchRequest instead of
			// on each individual Request. This meant that all Requests in a
			// BatchRequest shared the same sequence number, so a BatchIndex was
			// augmented to provide an ordering between them. Individual
			// Requests were later given their own sequence numbers, so the
			// BatchIndex was no longer necessary.
			if seqNum := args.Header().Sequence; seqNum != 0 {
				// Set the Request's sequence number on the TxnMeta for this
				// request. Each request will set their own sequence number on
				// the TxnMeta, which is stored as part of an intent.
				ba.Txn.Sequence = seqNum
			}
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
				// encountered evaluating the entire batch on cput and inc requests.
				// Because both of these requests must have their keys refreshed on
				// commit with Transaction.WriteTooOld is true, and that refresh will
				// fail, we'd be otherwise guaranteed to do a client-side retry.
				// Returning an error allows a txn-coord-side retry.
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
		// If the transaction committed, forward the response
		// timestamp to the commit timestamp in case we were able to
		// optimize and commit at a higher timestamp without higher-level
		// retry (i.e. there were no refresh spans and the commit timestamp
		// wasn't leaked).
		if ba.Txn.Status == roachpb.COMMITTED {
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
	if gossipClusterID, err := r.store.Gossip().GetClusterID(); err == nil {
		if gossipClusterID != r.store.ClusterID() {
			log.Fatalf(
				ctx, "store %d belongs to cluster %s, but attempted to join cluster %s via gossip",
				r.store.StoreID(), r.store.ClusterID(), gossipClusterID)
		}
	}

	// Gossip the cluster ID from all replicas of the first range; there
	// is no expiration on the cluster ID.
	if log.V(1) {
		log.Infof(ctx, "gossiping cluster id %q from store %d, r%d", r.store.ClusterID(),
			r.store.StoreID(), r.RangeID)
	}
	if err := r.store.Gossip().AddClusterID(r.store.ClusterID()); err != nil {
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

		log.Fatalf(ctx, "replica is corrupted: %s", pErr)
	}
	return pErr
}

// SplitByLoadEnabled wraps "kv.range_split.by_load_enabled".
var SplitByLoadEnabled = settings.RegisterBoolSetting(
	"kv.range_split.by_load_enabled",
	"allow automatic splits of ranges based on where load is concentrated.",
	true,
)

// SplitByLoadQPSThreshold wraps "kv.range_split.load_qps_threshold".
var SplitByLoadQPSThreshold = settings.RegisterIntSetting(
	"kv.range_split.load_qps_threshold",
	"the QPS over which, the range becomes a candidate for load based splitting.",
	250, // 250 req/s
)

// SplitByLoadQPSThreshold returns the QPS request rate for a given replica.
func (r *Replica) SplitByLoadQPSThreshold() float64 {
	return float64(SplitByLoadQPSThreshold.Get(&r.store.cfg.Settings.SV))
}

// SplitByLoadEnabled returns whether load based splitting is enabled.
func (r *Replica) SplitByLoadEnabled() bool {
	return SplitByLoadEnabled.Get(&r.store.cfg.Settings.SV) &&
		r.store.ClusterSettings().Version.IsMinSupported(cluster.VersionLoadSplits) &&
		!r.store.TestingKnobs().DisableLoadBasedSplitting
}

// needsSplitByLoadLocked returns two bools indicating first, whether
// the range is over the threshold for splitting by load and second,
// whether the range is ready to be added to the split queue.
func (r *Replica) needsSplitByLoadLocked() (bool, bool) {
	// First compute requests per second since the last check.
	nowTime := r.store.Clock().PhysicalTime()
	duration := nowTime.Sub(r.splitMu.lastReqTime)
	if duration < time.Second {
		return r.splitMu.splitFinder != nil, false
	}

	// Update the QPS and reset the time and request counter.
	r.splitMu.qps = (float64(r.splitMu.count) / float64(duration)) * 1e9
	r.splitMu.lastReqTime = nowTime
	r.splitMu.count = 0

	// If the QPS for the range exceeds the threshold, start actively
	// tracking potential for splitting this range based on load.
	// This tracking will begin by initiating a splitFinder so it can
	// begin to Record requests so it can find a split point. If a
	// splitFinder already exists, we check if a split point is ready
	// to be used.
	if r.splitMu.qps >= r.SplitByLoadQPSThreshold() {
		if r.splitMu.splitFinder != nil {
			if r.splitMu.splitFinder.Ready(nowTime) {
				// We're ready to add this range to the split queue.
				return true, true
			}
		} else {
			r.splitMu.splitFinder = split.New(nowTime)
		}
		return true, false
	}

	r.splitMu.splitFinder = nil
	return false, false
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

func (r *Replica) needsMergeBySizeRLocked() bool {
	return r.mu.state.Stats.Total() < *r.mu.zone.RangeMinBytes
}

func (r *Replica) exceedsMultipleOfSplitSizeRLocked(mult float64) bool {
	maxBytes := *r.mu.zone.RangeMaxBytes
	size := r.mu.state.Stats.Total()
	return maxBytes > 0 && float64(size) > float64(maxBytes)*mult
}

func (r *Replica) startKey() roachpb.RKey {
	return r.Desc().StartKey
}

// Less implements the btree.Item interface.
func (r *Replica) Less(i btree.Item) bool {
	return r.startKey().Less(i.(rangeKeyItem).startKey())
}

func isRaftLeader(raftStatus *raft.Status) bool {
	return raftStatus != nil && raftStatus.SoftState.RaftState == raft.StateLeader
}

// HasRaftLeader returns true if the raft group has a raft leader currently.
func HasRaftLeader(raftStatus *raft.Status) bool {
	return raftStatus != nil && raftStatus.SoftState.Lead != 0
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

func init() {
	tracing.RegisterTagRemapping("r", "range")
}
