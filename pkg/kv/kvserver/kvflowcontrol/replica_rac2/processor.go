// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replica_rac2

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Replica abstracts kvserver.Replica. It exposes internal implementation
// details of Replica, specifically the locking behavior, since it is
// essential to reason about correctness.
type Replica interface {
	// RaftMuAssertHeld asserts that Replica.raftMu is held.
	RaftMuAssertHeld()
	// MuAssertHeld asserts that Replica.mu is held.
	MuAssertHeld()
	// MuRLock acquires Replica.mu for reads.
	MuRLock()
	// MuRUnlock releases the Replica.mu read lock.
	MuRUnlock()
	// LeaseholderMuRLocked returns the Replica's current knowledge of the
	// leaseholder, which can be stale. It is only called after Processor
	// knows the Replica is initialized.
	//
	// Replica mu is held for reads or writes. The caller does not make any claims
	// about whether it holds raftMu or not.
	LeaseholderMuRLocked() roachpb.ReplicaID
	// IsScratchRange returns true if this is range is a scratch range (i.e.
	// overlaps with the scratch span and has a start key <=
	// keys.ScratchRangeMin).
	IsScratchRange() bool
}

// RaftScheduler abstracts kvserver.raftScheduler.
type RaftScheduler interface {
	// EnqueueRaftReady schedules Ready processing, that will also ensure that
	// Processor.HandleRaftReadyRaftMuLocked is called.
	EnqueueRaftReady(id roachpb.RangeID)
}

// RaftNode abstracts raft.RawNode. All methods must be called while holding
// both Replica mu and raftMu.
//
// It should not be essential for read-only methods to hold Replica mu, since
// except for one case (flushing the proposal buffer), all methods that mutate
// state in raft.RawNode hold both mutexes. Consider the following information
// for a replica maintained by the leader: Match, Next, HighestUnstableIndex.
// (Match, Next) represent in-flight entries, that are not affected by
// flushing the proposal buffer. [Next, HighestUnstableIndex) are pending, and
// HighestUnstableIndex *is* affected by flushing the proposal buffer.
// Additionally, a replica (leader or follower) also has a NextUnstableIndex
// <= HighestUnstableIndex, which is the index of the next entry that will be
// sent to local storage (Match is equivalent to StableIndex at a replica), if
// there are any such entries. That is, NextUnstableIndex represents an
// exclusive upper bound on MsgStorageAppends that have already been retrieved
// from Ready. At the leader, the Next value for a replica is <=
// NextUnstableIndex for the leader. NextUnstableIndex on the leader is not
// affected by flushing the proposal buffer. RACv2 code limits its advancing
// knowledge of state on any replica (leader or follower) to
// NextUnstableIndex, since it is never concerned at any replica with indices
// that have not been seen in a MsgStorageAppend. This suggests read-only
// methods should not be affected by concurrent advancing of
// HighestUnstableIndex.
//
// Despite the above, there are implementation details of Raft, specifically
// maintenance of tracker.Progress, that result in false data races. Due to
// this, reads done by RACv2 ensure both mutexes are held. We mention this
// since RACv2 code may not be able to tolerate a true data race, in that it
// reads Raft state at various points while holding raftMu, and expects those
// various reads to be mutually consistent.
type RaftNode interface {
	rac2.RaftInterface
	// TermLocked returns the current term of this replica.
	TermLocked() uint64
	// LeaderLocked returns the current known leader. This state can advance
	// past the group membership state, so the leader returned here may not be
	// known as a current group member.
	LeaderLocked() roachpb.ReplicaID
	// LogMarkLocked returns the current log mark of the raft log. It is not
	// guaranteed to be stablestorage, unless this method is called right after
	// RawNode is initialized. Processor calls this only on initialization.
	LogMarkLocked() rac2.LogMark
	// NextUnstableIndexLocked returns the index of the next entry that will
	// be sent to local storage. All entries < this index are either stored,
	// or have been sent to storage.
	//
	// NB: NextUnstableIndex can regress when the node accepts appends or
	// snapshots from a newer leader.
	NextUnstableIndexLocked() uint64
	// ReplicasStateLocked returns the current status state of all replicas.
	// RACv2 uses the Match and Next indices only for replicas in StateReplicate.
	// All entries >= Next have not had MsgApps constructed during the lifetime
	// of this StateReplicate (they may have been constructed previously).
	//
	// When a follower transitions from {StateProbe,StateSnapshot} =>
	// StateReplicate, we start trying to send MsgApps. We should notice such
	// transitions both in rac2.HandleRaftEventRaftMuLocked and
	// rac2.SetReplicasRaftMuLocked.
	//
	// infoMap is an in-out parameter. It is expected to be empty, and is
	// populated with the ReplicaStateInfos for all replicas.
	ReplicasStateLocked(infoMap map[roachpb.ReplicaID]rac2.ReplicaStateInfo)
}

// AdmittedPiggybacker is used to enqueue admitted vector messages addressed to
// replicas on a particular node. For efficiency, these need to be piggybacked
// on other messages being sent to the given leader node. The store / range /
// replica IDs are provided so that the leader node can route the incoming
// message to the relevant range.
type AdmittedPiggybacker interface {
	Add(roachpb.NodeID, kvflowcontrolpb.PiggybackedAdmittedState)
}

// EntryForAdmission is the information provided to the admission control (AC)
// system, when requesting admission.
type EntryForAdmission struct {
	// Information needed by the AC system, for deciding when to admit, and for
	// maintaining its accounting of how much work has been requested/admitted.
	StoreID    roachpb.StoreID
	TenantID   roachpb.TenantID
	Priority   admissionpb.WorkPriority
	CreateTime int64
	// RequestedCount is the number of admission tokens requested (not to be
	// confused with replication AC flow tokens).
	RequestedCount int64
	// Ingested is true iff this request represents a sstable that will be
	// ingested into Pebble.
	Ingested bool

	// Routing info to get to the Processor, in addition to StoreID.
	RangeID   roachpb.RangeID
	ReplicaID roachpb.ReplicaID

	// CallbackState is information that is needed by the callback when the
	// entry is admitted.
	CallbackState EntryForAdmissionCallbackState
}

// EntryForAdmissionCallbackState is passed to the callback when the entry is
// admitted.
type EntryForAdmissionCallbackState struct {
	Mark     rac2.LogMark
	Priority raftpb.Priority
}

// ACWorkQueue abstracts the behavior needed from admission.WorkQueue.
type ACWorkQueue interface {
	// Admit returns false if the entry was not submitted for admission for
	// some reason.
	Admit(ctx context.Context, entry EntryForAdmission) bool
}

type rangeControllerInitState struct {
	replicaSet    rac2.ReplicaSet
	leaseholder   roachpb.ReplicaID
	nextRaftIndex uint64
	// These fields are required options for the RangeController specific to the
	// replica and range, rather than the store or node, so we pass them as part
	// of the range controller init state.
	rangeID        roachpb.RangeID
	tenantID       roachpb.TenantID
	localReplicaID roachpb.ReplicaID
	raftInterface  rac2.RaftInterface
	msgAppSender   rac2.MsgAppSender
}

// RangeControllerFactory abstracts RangeController creation for testing.
type RangeControllerFactory interface {
	// New creates a new RangeController.
	New(ctx context.Context, state rangeControllerInitState) rac2.RangeController
}

// ProcessorOptions are specified when creating a new Processor.
type ProcessorOptions struct {
	// Various constant fields that are duplicated from Replica, since we
	// have abstracted Replica for testing.
	//
	// TODO(sumeer): this is a premature optimization to avoid calling
	// Replica interface methods. Revisit.
	NodeID    roachpb.NodeID
	StoreID   roachpb.StoreID
	RangeID   roachpb.RangeID
	ReplicaID roachpb.ReplicaID

	Replica                Replica
	RaftScheduler          RaftScheduler
	AdmittedPiggybacker    AdmittedPiggybacker
	ACWorkQueue            ACWorkQueue
	MsgAppSender           rac2.MsgAppSender
	RangeControllerFactory RangeControllerFactory
	Settings               *cluster.Settings
	EvalWaitMetrics        *rac2.EvalWaitMetrics

	EnabledWhenLeaderLevel kvflowcontrol.V2EnabledWhenLeaderLevel
	Knobs                  *kvflowcontrol.TestingKnobs
}

// SideChannelInfoUsingRaftMessageRequest is used to provide a follower
// information about the leader's protocol, and if the leader is using the
// RACv2 protocol, additional information about entries.
type SideChannelInfoUsingRaftMessageRequest struct {
	UsingV2Protocol bool
	LeaderTerm      uint64
	// Following are only used if UsingV2Protocol is true.
	First, Last    uint64
	LowPriOverride bool
}

// Processor handles RACv2 processing for a Replica. It combines the
// functionality needed by any replica, and needed only at the leader, since
// there is common membership state needed in both roles. There are some
// methods that will only be called on the leader or a follower, and it must
// gracefully handle the case where those method calls are stale in their
// assumption of the role of this replica.
//
// Processor can be created on an uninitialized Replica, hence group
// membership may not be known. Group membership is learnt (and kept
// up-to-date) via OnDescChangedLocked. Knowledge of the leader can advance
// past the current group membership, and must be tolerated. Knowledge of the
// leaseholder can be stale, and must be tolerated.
//
// Transitions into and out of leadership, or knowledge of the current leader,
// is discovered in HandleRaftReadyRaftMuLocked. It is important that there is
// a low lag between losing leadership, which is discovered on calling
// RawNode.Step, and HandleRaftReadyRaftMuLocked. We rely on the current
// external behavior where Store.processRequestQueue (which calls Step using
// queued messages) will always return true if there were any messages that
// were stepped, even if there are errors. By returning true, the
// raftScheduler will call processReady during the same processing pass for
// the replica. Arguably, we could introduce a TryUpdateLeaderRaftMuLocked to
// be called from Replica.stepRaftGroup, but it does not capture all state
// transitions -- a raft group with a single member causes the replica to
// assume leadership without any messages being stepped. So we choose the
// first option to simplify the Processor interface.
//
// Locking:
//
// We *strongly* prefer methods to be called without holding Replica.mu, since
// then the callee (implementation of Processor) does not need to worry about
// (a) deadlocks, since it sometimes needs to lock Replica.mu itself, (b) the
// amount of work it is doing under this critical section. There are two
// exceptions to this, due to difficulty in changing the calling code:
// InitRaftLocked, OnDescChangedLocked.
type Processor interface {
	// InitRaftLocked is called when RaftNode is initialized for the Replica.
	// NB: can be called twice before the Replica is fully initialized.
	//
	// Both Replica.raftMu and Replica.mu are held.
	InitRaftLocked(context.Context, RaftNode)

	// OnDestroyRaftMuLocked is called when the Replica is being destroyed.
	//
	// We need to know when Replica.mu.destroyStatus is updated, so that we
	// can close, and return tokens. We do this call from
	// disconnectReplicationRaftMuLocked. Make sure this is not too late in
	// that these flow tokens may be needed by others.
	//
	// raftMu is held.
	OnDestroyRaftMuLocked(ctx context.Context)

	// SetEnabledWhenLeaderRaftMuLocked is the dynamic change corresponding to
	// ProcessorOptions.EnabledWhenLeaderLevel. The level must only be ratcheted
	// up. We call it in Replica.handleRaftReadyRaftMuLocked, before doing any
	// work (before Ready is called, since it may create a RangeController).
	// This may be a noop if the level has already been reached.
	//
	// raftMu is held.
	SetEnabledWhenLeaderRaftMuLocked(ctx context.Context, level kvflowcontrol.V2EnabledWhenLeaderLevel)
	// GetEnabledWhenLeader returns the current level. It may be used in
	// highly concurrent settings at the leaseholder, when waiting for eval,
	// and when encoding a proposal. Note that if the leaseholder is not the
	// leader and the leader has switched to a higher level, there is no harm
	// done, since the leaseholder can continue waiting for v1 tokens and use
	// the v1 entry encoding.
	GetEnabledWhenLeader() kvflowcontrol.V2EnabledWhenLeaderLevel

	// OnDescChangedLocked provides a possibly updated RangeDescriptor. The
	// tenantID passed in all calls must be the same.
	//
	// Both Replica.raftMu and Replica.mu are held.
	OnDescChangedLocked(
		ctx context.Context, desc *roachpb.RangeDescriptor, tenantID roachpb.TenantID)

	// HandleRaftReadyRaftMuLocked corresponds to processing that happens when
	// Replica.handleRaftReadyRaftMuLocked is called. It must be called even
	// if there was no Ready, since it can be used to advance Admitted, and do
	// other processing.
	//
	// The RaftEvent represents MsgStorageAppend on all replicas. To stay
	// consistent with the structure of Replica.handleRaftReadyRaftMuLocked, this
	// method only does leader specific processing of entries.
	// AdmitRaftEntriesFromMsgStorageAppendRaftMuLocked does the general replica
	// processing for MsgStorageAppend.
	//
	// raftMu is held.
	HandleRaftReadyRaftMuLocked(context.Context, rac2.RaftEvent)
	// AdmitRaftEntriesRaftMuLocked subjects entries to admission control on a
	// replica (leader or follower). Like HandleRaftReadyRaftMuLocked, this is
	// called from Replica.handleRaftReadyRaftMuLocked.
	//
	// It is split off from that function since it is natural to position the
	// admission control processing when we are writing to the store in
	// Replica.handleRaftReadyRaftMuLocked. This is mostly a noop if the leader is
	// not using the RACv2 protocol.
	//
	// Returns false if the leader is using RACv1 and the replica is not
	// destroyed, in which case the caller should follow the RACv1 admission
	// pathway.
	//
	// raftMu is held.
	AdmitRaftEntriesRaftMuLocked(
		ctx context.Context, event rac2.RaftEvent) bool

	// EnqueuePiggybackedAdmittedAtLeader is called at the leader when receiving a
	// piggybacked admitted vector that can advance the given follower's admitted
	// state. The caller is responsible for scheduling on the raft scheduler, such
	// that ProcessPiggybackedAdmittedAtLeaderRaftMuLocked gets called soon.
	EnqueuePiggybackedAdmittedAtLeader(roachpb.ReplicaID, kvflowcontrolpb.AdmittedState)
	// ProcessPiggybackedAdmittedAtLeaderRaftMuLocked is called to process
	// previously enqueued piggybacked admitted vectors.
	//
	// raftMu is held.
	ProcessPiggybackedAdmittedAtLeaderRaftMuLocked(ctx context.Context)

	// SideChannelForPriorityOverrideAtFollowerRaftMuLocked is called on a
	// follower to provide information about whether the leader is using the
	// RACv2 protocol, and if yes, the low-priority override, via a
	// side-channel, since we can't plumb this information directly through
	// Raft.
	//
	// raftMu is held.
	SideChannelForPriorityOverrideAtFollowerRaftMuLocked(
		info SideChannelInfoUsingRaftMessageRequest,
	)

	// SyncedLogStorage is called when the log storage is synced, after writing a
	// snapshot or log entries batch. It can be called synchronously from
	// OnLogSync or OnSnapSync handlers if the write batch is blocking, or
	// asynchronously from OnLogSync.
	SyncedLogStorage(ctx context.Context, mark rac2.LogMark)
	// AdmittedLogEntry is called when an entry is admitted. It can be called
	// synchronously from within ACWorkQueue.Admit if admission is immediate.
	AdmittedLogEntry(
		ctx context.Context, state EntryForAdmissionCallbackState,
	)
	// AdmittedState returns the vector of admitted log indices.
	AdmittedState() rac2.AdmittedVector

	// AdmitRaftMuLocked is called to notify RACv2 about the admitted vector
	// update on the given peer replica. This releases the associated flow tokens
	// if the replica is known and the admitted vector covers any.
	//
	// raftMu is held.
	AdmitRaftMuLocked(context.Context, roachpb.ReplicaID, rac2.AdmittedVector)

	// MaybeSendPingsRaftMuLocked sends a MsgApp ping to each raft peer whose
	// admitted vector is lagging, and there wasn't a recent MsgApp to this
	// peer. The messages are added to raft's message queue, and will be
	// extracted from raft and sent during the next Ready processing.
	//
	// If the replica is not the leader, this call does nothing.
	//
	// raftMu is held.
	MaybeSendPingsRaftMuLocked()

	// AdmitForEval is called to admit work that wants to evaluate at the
	// leaseholder.
	//
	// If the callee decided not to admit because replication admission
	// control is disabled, or for any other reason, admitted will be false
	// and error will be nil.
	AdmitForEval(
		ctx context.Context, pri admissionpb.WorkPriority, ct time.Time) (admitted bool, err error)

	// ProcessSchedulerEventRaftMuLocked is called to process events scheduled
	// by the RangeController.
	//
	// raftMu is held.
	ProcessSchedulerEventRaftMuLocked(ctx context.Context)

	// InspectRaftMuLocked returns a handle to inspect the state of the
	// underlying range controller. It is used to power /inspectz-style debugging
	// pages.
	InspectRaftMuLocked(ctx context.Context) (kvflowinspectpb.Handle, bool)

	// SendStreamStats returns the stats for the replica send streams It is only
	// populated on the leader. The stats may be used to inform placement
	// decisions pertaining to the range.
	SendStreamStats() rac2.RangeSendStreamStats
}

// processorImpl implements Processor.
//
// All the fields in it are used with Replica.raftMu held, with only a few
// exceptions commented explicitly.
type processorImpl struct {
	// opts is an immutable bag of constants and interfaces for interaction with
	// the Replica and surrounding components. Set once upon construction.
	opts ProcessorOptions

	// logTracker contains state for tracking and advancing the log's stable index
	// and admitted vector.
	//
	// Has its own mutex. Used without raftMu for a subset of operations, when
	// registering storage notifications or reporting the admitted vector.
	logTracker logTracker

	// destroyed transitions once from false => true when the Replica is being
	// destroyed.
	destroyed bool

	// term is the current raft term. Kept up-to-date with the latest Ready
	// cycle. It is used to notice transitions out of leadership and back, to
	// recreate leader.rc.
	term uint64
	// leaderID is the ID of the current term leader. Can be zero if unknown.
	leaderID roachpb.ReplicaID
	// leaderNodeID and leaderStoreID are a function of leaderID and replicas
	// fields. They are set when leaderID is non-zero and replicas contains
	// leaderID, else are 0.
	leaderNodeID  roachpb.NodeID
	leaderStoreID roachpb.StoreID
	// leaseholderID is the currently known leaseholder replica.
	leaseholderID roachpb.ReplicaID

	// scratchInfoMap is used as a pre-allocated in-out parameter for calling
	// ReplicasStateLocked when constructing a rac2.RaftEvent.
	scratchInfoMap map[roachpb.ReplicaID]rac2.ReplicaStateInfo

	// State at a follower.
	follower struct {
		// isLeaderUsingV2Protocol is true when the leaderID indicated that it's
		// using RACv2.
		isLeaderUsingV2Protocol bool
		// lowPriOverrideState records which raft log entries have their priority
		// overridden to be raftpb.LowPri.
		lowPriOverrideState lowPriOverrideState
	}

	// State when leader, i.e., when leaderID == opts.ReplicaID, and v2 protocol
	// is enabled.
	leader struct {
		// pendingAdmittedMu contains recently delivered admitted vectors. When the
		// updates map is not empty, the range is scheduled for applying these
		// vectors to the corresponding streams / token trackers. The map is cleared
		// when the admitted vectors are applied.
		//
		// Inserts into updates happen without holding raftMu, so there is a mutex
		// for synchronizing with the processor when it applies the updates.
		//
		// Invariant: len(updates) != 0 ==> the processing is scheduled.
		//
		// Invariant (under raftMu): updates == nil iff rc == nil. If raftMu is not
		// held, the invariant is eventually consistent since updates and rc are
		// updated under different mutexes.
		pendingAdmittedMu struct {
			syncutil.Mutex
			updates map[roachpb.ReplicaID]rac2.AdmittedVector
		}
		// scratch is used to as a pre-allocated swap-in replacement for
		// pendingAdmittedMu.updates when the queue is cleared. It is used while
		// holding raftMu, so doesn't need to be nested in pendingAdmittedMu.
		//
		// Invariant: len(scratch) == 0.
		//
		// TODO(pav-kv): factor out pendingAdmittedMu and scratch into a type.
		scratch map[roachpb.ReplicaID]rac2.AdmittedVector

		// rcReferenceUpdateMu is a narrow mutex held when rc reference is updated.
		// To access rc, the code must hold raftMu or rcReferenceUpdateMu.
		// Locking order: raftMu < rcReferenceUpdateMu.
		rcReferenceUpdateMu syncutil.RWMutex
		// rc is not nil iff this replica is a leader of the term, and uses RACv2.
		// rc is always updated while holding raftMu and rcReferenceUpdateMu. To
		// access rc, the code must hold at least one of these mutexes.
		rc rac2.RangeController
	}

	// replMu contains the fields that must be accessed while holding Replica.mu.
	replMu struct {
		// raftNode provides access to a subset of raft RawNode. The reference is
		// updated while holding both Replica.raftMu and Replica.mu, so can be read
		// with any of the two mutexes locked. When interacting with raftNode, the
		// Replica.mu must be held. See RaftNode comments.
		raftNode RaftNode
	}

	// desc contains the data derived from OnDescChangedLocked calls. It is always
	// updated with both Replica.raftMu and Replica.mu held, and is first set when
	// Replica is initialized. The fields are grouped for informational purposes,
	// processorImpl always accesses them under raftMu like other fields.
	desc struct {
		// replicas contains the current set of replicas.
		replicas rac2.ReplicaSet
		// replicasChanged is set to true when replicas has been updated. This is
		// used to lazily update all the state that depends on replicas.
		replicasChanged bool
		// tenantID is the tenant owning the replica. Set once, in the first call to
		// OnDescChanged.
		tenantID roachpb.TenantID
	}

	// enabledWhenLeader indicates the RACv2 mode of operation when this replica
	// is the leader. Atomic value, for serving GetEnabledWhenLeader. Updated only
	// while holding raftMu. Can be read non-atomically if raftMu is held.
	enabledWhenLeader kvflowcontrol.V2EnabledWhenLeaderLevel

	v1EncodingPriorityMismatch log.EveryN
}

var _ Processor = &processorImpl{}

func NewProcessor(opts ProcessorOptions) Processor {
	return &processorImpl{
		opts:                       opts,
		enabledWhenLeader:          opts.EnabledWhenLeaderLevel,
		v1EncodingPriorityMismatch: log.Every(time.Minute),
		scratchInfoMap:             make(map[roachpb.ReplicaID]rac2.ReplicaStateInfo),
	}
}

// isLeaderUsingV2RaftMuLocked returns true if the current leader uses the V2
// protocol.
func (p *processorImpl) isLeaderUsingV2ProcLocked() bool {
	// We are the leader using V2, or a follower who learned that the leader is
	// using the V2 protocol.
	return p.leader.rc != nil || (p.opts.ReplicaID != p.leaderID && p.follower.isLeaderUsingV2Protocol)
}

// InitRaftLocked implements Processor.
func (p *processorImpl) InitRaftLocked(ctx context.Context, rn RaftNode) {
	p.opts.Replica.RaftMuAssertHeld()
	p.opts.Replica.MuAssertHeld()
	if p.desc.replicas != nil {
		log.Fatalf(ctx, "initializing RaftNode after replica is initialized")
	}
	p.replMu.raftNode = rn
	p.logTracker.init(p.replMu.raftNode.LogMarkLocked())
}

// OnDestroyRaftMuLocked implements Processor.
func (p *processorImpl) OnDestroyRaftMuLocked(ctx context.Context) {
	p.opts.Replica.RaftMuAssertHeld()
	p.destroyed = true
	p.closeLeaderStateRaftMuLocked(ctx)
	// Release some memory.
	p.follower.lowPriOverrideState = lowPriOverrideState{}
}

// SetEnabledWhenLeaderRaftMuLocked implements Processor.
func (p *processorImpl) SetEnabledWhenLeaderRaftMuLocked(
	ctx context.Context, level kvflowcontrol.V2EnabledWhenLeaderLevel,
) {
	p.opts.Replica.RaftMuAssertHeld()
	if p.destroyed || p.enabledWhenLeader >= level {
		return
	}
	atomic.StoreUint32(&p.enabledWhenLeader, level)
	if level != kvflowcontrol.V2EnabledWhenLeaderV1Encoding ||
		p.desc.replicas == nil {
		return
	}
	log.VEventf(ctx, 1, "enabled v2 protocol using v1 priority encoding")
	// May need to create RangeController.
	var leaderID roachpb.ReplicaID
	var term uint64
	var nextUnstableIndex uint64
	func() {
		p.opts.Replica.MuRLock()
		defer p.opts.Replica.MuRUnlock()
		leaderID = p.replMu.raftNode.LeaderLocked()
		if leaderID == p.opts.ReplicaID {
			term = p.replMu.raftNode.TermLocked()
			nextUnstableIndex = p.replMu.raftNode.NextUnstableIndexLocked()
		}
	}()
	if leaderID == p.opts.ReplicaID {
		p.createLeaderStateRaftMuLocked(ctx, term, nextUnstableIndex)
	}
}

// GetEnabledWhenLeader implements Processor.
func (p *processorImpl) GetEnabledWhenLeader() kvflowcontrol.V2EnabledWhenLeaderLevel {
	return atomic.LoadUint32(&p.enabledWhenLeader)
}

func descToReplicaSet(desc *roachpb.RangeDescriptor) rac2.ReplicaSet {
	rs := rac2.ReplicaSet{}
	for _, r := range desc.InternalReplicas {
		rs[r.ReplicaID] = r
	}
	return rs
}

// OnDescChangedLocked implements Processor.
func (p *processorImpl) OnDescChangedLocked(
	ctx context.Context, desc *roachpb.RangeDescriptor, tenantID roachpb.TenantID,
) {
	p.opts.Replica.RaftMuAssertHeld()
	p.opts.Replica.MuAssertHeld()
	initialization := p.desc.replicas == nil
	if initialization {
		// Replica is initialized, in that we now have a descriptor.
		if p.replMu.raftNode == nil {
			panic(errors.AssertionFailedf("RaftNode is not initialized"))
		}
		p.desc.tenantID = tenantID
	} else if p.desc.tenantID != tenantID {
		panic(errors.AssertionFailedf("tenantId was changed from %s to %s",
			p.desc.tenantID, tenantID))
	}
	p.desc.replicas = descToReplicaSet(desc)
	p.desc.replicasChanged = true
	// We need to promptly:
	// - Return tokens if some replicas have been removed, since those tokens
	//   could be used by other ranges with replicas on the same store.
	// - Update (create) the RangeController's state used in WaitForEval, and
	//   for sending MsgApps to new replicas (by creating replicaSendStreams).
	//
	// We ensure that promptness by scheduling ready.
	//
	// TODO(sumeer): this is currently gated on !initialization due to kvserver
	// test failure for a quiescence test that ought to be rewritten. So if
	// processorImpl starts in pull mode, this is the leader, there are no new
	// entries being written, and other replicas have a send-queue, MsgApps can
	// be (arbitrarily?) delayed. Change this to unconditionally call
	// EnqueueRaftReady.
	if !initialization {
		p.opts.RaftScheduler.EnqueueRaftReady(p.opts.RangeID)
	}
}

// makeStateConsistentRaftMuLocked uses the union of the latest state retrieved
// from RaftNode and the p.replicas set to initialize or update the internal
// state of processorImpl.
//
// nextUnstableIndex is used to initialize the state of the send-queues if
// this replica is becoming the leader. This index must immediately precede
// the entries provided to RangeController.
func (p *processorImpl) makeStateConsistentRaftMuLocked(
	ctx context.Context,
	nextUnstableIndex uint64,
	leaderID roachpb.ReplicaID,
	leaseholderID roachpb.ReplicaID,
	term uint64,
) {
	if term < p.term {
		log.Fatalf(ctx, "term regressed from %d to %d", p.term, term)
	}
	termChanged := term > p.term
	if termChanged {
		p.term = term
	}
	replicasChanged := p.desc.replicasChanged
	if replicasChanged {
		p.desc.replicasChanged = false
	}
	if !replicasChanged && leaderID == p.leaderID && leaseholderID == p.leaseholderID &&
		(p.leader.rc == nil || !termChanged) {
		// Common case.
		return
	}
	// The leader or leaseholder or replicas or term changed. We set everything.
	p.leaderID = leaderID
	p.leaseholderID = leaseholderID
	// Set leaderNodeID, leaderStoreID.
	if p.leaderID == 0 {
		p.leaderNodeID = 0
		p.leaderStoreID = 0
	} else {
		rd, ok := p.desc.replicas[leaderID]
		if !ok {
			if leaderID == p.opts.ReplicaID {
				// Is leader, but not in the set of replicas. We expect this should not
				// be happening anymore, due to raft.Config.StepDownOnRemoval being set
				// to true. But we tolerate it.
				log.Errorf(ctx, "leader=%d is not in the set of replicas=%v",
					leaderID, p.desc.replicas)
				p.leaderNodeID = p.opts.NodeID
				p.leaderStoreID = p.opts.StoreID
			} else {
				// A follower learns about a leader before it learns about a config
				// change that includes the leader in the set of replicas. Ignore.
				p.leaderNodeID = 0
				p.leaderStoreID = 0
			}
		} else {
			p.leaderNodeID = rd.NodeID
			p.leaderStoreID = rd.StoreID
		}
	}
	if p.leaderID != p.opts.ReplicaID {
		if p.leader.rc != nil {
			// Transition from leader to follower.
			p.closeLeaderStateRaftMuLocked(ctx)
		}
		return
	}
	// Is the leader.
	if p.enabledWhenLeader == kvflowcontrol.V2NotEnabledWhenLeader {
		return
	}
	if p.leader.rc != nil && termChanged {
		// Need to recreate the RangeController.
		p.closeLeaderStateRaftMuLocked(ctx)
	}
	if p.leader.rc == nil {
		p.createLeaderStateRaftMuLocked(ctx, term, nextUnstableIndex)
		return
	}
	// Existing RangeController.
	if replicasChanged {
		if err := p.leader.rc.SetReplicasRaftMuLocked(ctx, p.desc.replicas); err != nil {
			log.Errorf(ctx, "error setting replicas: %v", err)
		}
	}
	p.leader.rc.SetLeaseholderRaftMuLocked(ctx, leaseholderID)
}

func (p *processorImpl) closeLeaderStateRaftMuLocked(ctx context.Context) {
	if p.leader.rc == nil {
		return
	}
	p.leader.rc.CloseRaftMuLocked(ctx)

	func() {
		p.leader.pendingAdmittedMu.Lock()
		defer p.leader.pendingAdmittedMu.Unlock()
		p.leader.pendingAdmittedMu.updates = nil
	}()
	p.leader.scratch = nil

	p.leader.rcReferenceUpdateMu.Lock()
	defer p.leader.rcReferenceUpdateMu.Unlock()
	p.leader.rc = nil
}

func (p *processorImpl) createLeaderStateRaftMuLocked(
	ctx context.Context, term uint64, nextUnstableIndex uint64,
) {
	if p.leader.rc != nil {
		panic("RangeController already exists")
	}
	p.term = term
	rc := p.opts.RangeControllerFactory.New(ctx, rangeControllerInitState{
		replicaSet:     p.desc.replicas,
		leaseholder:    p.leaseholderID,
		nextRaftIndex:  nextUnstableIndex,
		rangeID:        p.opts.RangeID,
		tenantID:       p.desc.tenantID,
		localReplicaID: p.opts.ReplicaID,
		raftInterface:  p.replMu.raftNode,
		msgAppSender:   p.opts.MsgAppSender,
	})

	func() {
		p.leader.pendingAdmittedMu.Lock()
		defer p.leader.pendingAdmittedMu.Unlock()
		p.leader.pendingAdmittedMu.updates = map[roachpb.ReplicaID]rac2.AdmittedVector{}
	}()
	p.leader.scratch = map[roachpb.ReplicaID]rac2.AdmittedVector{}

	p.leader.rcReferenceUpdateMu.Lock()
	defer p.leader.rcReferenceUpdateMu.Unlock()
	p.leader.rc = rc
}

// HandleRaftReadyRaftMuLocked implements Processor.
func (p *processorImpl) HandleRaftReadyRaftMuLocked(ctx context.Context, e rac2.RaftEvent) {
	p.opts.Replica.RaftMuAssertHeld()
	// Register all snapshots / log appends without exception. If the replica is
	// being destroyed, this should be a no-op, but there is no harm in
	// registering the write just in case.
	p.registerStorageAppendRaftMuLocked(ctx, e)

	// Skip if the replica is not initialized or already destroyed.
	if p.desc.replicas == nil || p.destroyed {
		return
	}
	if p.replMu.raftNode == nil {
		log.Fatal(ctx, "RaftNode is not initialized")
		return
	}

	// We will use the scratchInfoMap to get the latest state of the replicas and
	// construct the RaftEvent. Ensure that it is empty before we start.
	clear(p.scratchInfoMap)
	// NB: we need to call makeStateConsistentRaftMuLocked even if
	// NotEnabledWhenLeader, since this replica could be a follower and the leader
	// may switch to v2.

	// Grab the state we need in one shot after acquiring Replica mu.
	var nextUnstableIndex uint64
	var leaderID, leaseholderID roachpb.ReplicaID
	var term uint64
	func() {
		p.opts.Replica.MuRLock()
		defer p.opts.Replica.MuRUnlock()
		nextUnstableIndex = p.replMu.raftNode.NextUnstableIndexLocked()
		leaderID = p.replMu.raftNode.LeaderLocked()
		leaseholderID = p.opts.Replica.LeaseholderMuRLocked()
		term = p.replMu.raftNode.TermLocked()
		p.replMu.raftNode.ReplicasStateLocked(p.scratchInfoMap)
	}()
	if len(e.Entries) > 0 {
		nextUnstableIndex = e.Entries[0].Index
	}
	p.makeStateConsistentRaftMuLocked(
		ctx, nextUnstableIndex, leaderID, leaseholderID, term)

	if !p.isLeaderUsingV2ProcLocked() {
		return
	}
	// NB: since we've registered the latest log/snapshot write (if any) above,
	// our admitted vector is likely consistent with the latest leader term.
	p.maybeSendAdmittedRaftMuLocked(ctx)
	e.ReplicasStateInfo = p.scratchInfoMap
	if rc := p.leader.rc; rc != nil {
		if knobs := p.opts.Knobs; knobs == nil || !knobs.UseOnlyForScratchRanges ||
			p.opts.Replica.IsScratchRange() {
			if err := rc.HandleRaftEventRaftMuLocked(ctx, e); err != nil {
				log.Errorf(ctx, "error handling raft event: %v", err)
			}
		}
	}
}

// maybeSendAdmittedRaftMuLocked sends the admitted vector to the leader, if the
// vector was updated since the last send. If the replica is the leader, the
// sending is short-circuited to local processing.
func (p *processorImpl) maybeSendAdmittedRaftMuLocked(ctx context.Context) {
	// NB: this resets the scheduling bit in logTracker, which allows us to
	// schedule this call again when the admitted vector is updated next time.
	av, dirty := p.logTracker.admitted(true /* sched */)
	// Don't send the admitted vector if it hasn't been updated since the last
	// time it was sent.
	if !dirty {
		return
	}
	// If the admitted vector term is stale, don't send - the leader will drop it.
	if av.Term < p.term {
		return
	}
	// The admitted vector term can not outpace the raft term because raft would
	// never accept a write that bumps its log's leader term without bumping the
	// raft term first. We are holding raftMu between here and when the raft term
	// was last read, and the logTracker term was last updated (we are still in
	// the Ready handler), so this invariant could not be violated.
	//
	// However, we only check this in debug mode. There is no harm sending an
	// admitted vector with a future term. This informs the previous leader that
	// there is a new leader, so it has freedom to react to this information (e.g.
	// release all flow tokens, or step down from leadership).
	if buildutil.CrdbTestBuild && av.Term > p.term {
		panic(errors.AssertionFailedf(
			"admitted vector ahead of raft term: admitted=%+v, term=%d", av, p.term))
	}
	// If we are the leader, send the admitted vector directly to RangeController.
	if rc := p.leader.rc; rc != nil {
		rc.AdmitRaftMuLocked(ctx, p.opts.ReplicaID, av)
		return
	}
	// If the leader is unknown, don't send the admitted vector to anyone. This
	// should normally not happen here, since av.Term == p.term means we had at
	// least one append from the leader, so we know it. There are cases though
	// (see Replica.forgetLeaderLocked) when raft deliberately forgets the leader.
	if p.leaderNodeID == 0 {
		return
	}
	// Piggyback the new admitted vector to the message stream going to the node
	// containing the leader replica.
	p.opts.AdmittedPiggybacker.Add(p.leaderNodeID, kvflowcontrolpb.PiggybackedAdmittedState{
		RangeID:       p.opts.RangeID,
		ToStoreID:     p.leaderStoreID,
		FromReplicaID: p.opts.ReplicaID,
		ToReplicaID:   p.leaderID,
		Admitted: kvflowcontrolpb.AdmittedState{
			Term:     av.Term,
			Admitted: av.Admitted[:],
		}})
}

// registerStorageAppendRaftMuLocked registers the raft storage write with the
// logTracker. All raft writes must be seen by this function.
func (p *processorImpl) registerStorageAppendRaftMuLocked(ctx context.Context, e rac2.RaftEvent) {
	// NB: snapshot must be handled first. If Ready contains both snapshot and
	// entries, the entries are contiguous with the snapshot.
	if snap := e.Snap; snap != nil {
		mark := rac2.LogMark{Term: e.Term, Index: snap.Metadata.Index}
		p.logTracker.snap(ctx, mark)
	}
	if len(e.Entries) != 0 {
		after := e.Entries[0].Index - 1
		to := rac2.LogMark{Term: e.Term, Index: e.Entries[len(e.Entries)-1].Index}
		p.logTracker.append(ctx, after, to)
	}
}

// AdmitRaftEntriesRaftMuLocked implements Processor.
func (p *processorImpl) AdmitRaftEntriesRaftMuLocked(ctx context.Context, e rac2.RaftEvent) bool {
	// Return false only if we're not destroyed and not using V2.
	if p.destroyed || !p.isLeaderUsingV2ProcLocked() {
		return p.destroyed
	}

	for _, entry := range e.Entries {
		typ, priBits, err := raftlog.EncodingOf(entry)
		if err != nil {
			panic(errors.Wrap(err, "unable to determine raft command encoding"))
		}
		if !typ.UsesAdmissionControl() {
			continue // nothing to do
		}
		isV2Encoding := typ == raftlog.EntryEncodingStandardWithACAndPriority ||
			typ == raftlog.EntryEncodingSideloadedWithACAndPriority
		meta, err := raftlog.DecodeRaftAdmissionMeta(entry.Data)
		if err != nil {
			panic(errors.Wrap(err, "unable to decode raft command admission data: %v"))
		}

		if log.V(1) {
			if isV2Encoding {
				log.Infof(ctx,
					"decoded v2 raft admission meta below-raft: pri=%v create-time=%d "+
						"proposer=n%v receiver=[n%d,s%v] tenant=t%d tokens≈%d "+
						"sideloaded=%t raft-entry=%d/%d lead-v2=%v",
					raftpb.Priority(meta.AdmissionPriority),
					meta.AdmissionCreateTime,
					meta.AdmissionOriginNode,
					p.opts.NodeID,
					p.opts.StoreID,
					p.desc.tenantID.ToUint64(),
					kvflowcontrol.Tokens(len(entry.Data)),
					typ.IsSideloaded(),
					entry.Term,
					entry.Index,
					p.isLeaderUsingV2ProcLocked(),
				)
			} else {
				log.Infof(ctx,
					"decoded v1 raft admission meta below-raft: pri=%v create-time=%d "+
						"proposer=n%v receiver=[n%d,s%v] tenant=t%d tokens≈%d "+
						"sideloaded=%t raft-entry=%d/%d lead-v2=%v",
					admissionpb.WorkPriority(meta.AdmissionPriority),
					meta.AdmissionCreateTime,
					meta.AdmissionOriginNode,
					p.opts.NodeID,
					p.opts.StoreID,
					p.desc.tenantID.ToUint64(),
					kvflowcontrol.Tokens(len(entry.Data)),
					typ.IsSideloaded(),
					entry.Term,
					entry.Index,
					p.isLeaderUsingV2ProcLocked(),
				)
			}
		}

		mark := rac2.LogMark{Term: e.Term, Index: entry.Index}
		var raftPri raftpb.Priority
		if isV2Encoding {
			raftPri = raftpb.Priority(meta.AdmissionPriority)
			if raftPri != priBits {
				panic(errors.AssertionFailedf("inconsistent priorities %s, %s", raftPri, priBits))
			}
			raftPri = p.follower.lowPriOverrideState.getEffectivePriority(entry.Index, raftPri)
		} else {
			raftPri = raftpb.LowPri
			if admissionpb.WorkClassFromPri(admissionpb.WorkPriority(meta.AdmissionPriority)) ==
				admissionpb.RegularWorkClass && p.v1EncodingPriorityMismatch.ShouldLog() {
				log.Errorf(ctx,
					"do not use RACv1 for pri %s, which is regular work",
					admissionpb.WorkPriority(meta.AdmissionPriority))
			}
		}
		// Register all entries subject to AC with the log tracker.
		p.logTracker.register(ctx, mark, raftPri)

		// NB: cannot hold mu when calling Admit since the callback may execute from
		// inside Admit, when the entry is immediately admitted.
		submitted := p.opts.ACWorkQueue.Admit(ctx, EntryForAdmission{
			StoreID:        p.opts.StoreID,
			TenantID:       p.desc.tenantID,
			Priority:       rac2.RaftToAdmissionPriority(raftPri),
			CreateTime:     meta.AdmissionCreateTime,
			RequestedCount: int64(len(entry.Data)),
			Ingested:       typ.IsSideloaded(),
			RangeID:        p.opts.RangeID,
			ReplicaID:      p.opts.ReplicaID,
			CallbackState: EntryForAdmissionCallbackState{
				Mark:     mark,
				Priority: raftPri,
			},
		})
		// Failure is very rare and likely does not happen, e.g. store is not found.
		// TODO(pav-kv): audit failure scenarios and minimize/eliminate them.
		if !submitted {
			// NB: this also admits all previously registered entries.
			// TODO(pav-kv): consider not registering this entry in the first place,
			// instead of falsely admitting a prefix of the log. We don't want false
			// admissions to reach the leader.
			p.logTracker.logAdmitted(ctx, mark, raftPri)
		}
	}
	return true
}

// EnqueuePiggybackedAdmittedAtLeader implements Processor.
func (p *processorImpl) EnqueuePiggybackedAdmittedAtLeader(
	from roachpb.ReplicaID, state kvflowcontrolpb.AdmittedState,
) {
	var admitted [raftpb.NumPriorities]uint64
	copy(admitted[:], state.Admitted)

	p.leader.pendingAdmittedMu.Lock()
	defer p.leader.pendingAdmittedMu.Unlock()
	// Invariant: if updates == nil, we are not the leader or already stepping
	// down. Ignore the updates in this case.
	if p.leader.pendingAdmittedMu.updates == nil {
		return
	}
	// Merge in the received admitted vector. We are only interested in the
	// highest admitted marks. Zero value always merges in favour of the new one.
	p.leader.pendingAdmittedMu.updates[from] =
		p.leader.pendingAdmittedMu.updates[from].Merge(
			rac2.AdmittedVector{Term: state.Term, Admitted: admitted})
}

// ProcessPiggybackedAdmittedAtLeaderRaftMuLocked implements Processor.
func (p *processorImpl) ProcessPiggybackedAdmittedAtLeaderRaftMuLocked(ctx context.Context) {
	p.opts.Replica.RaftMuAssertHeld()
	if p.destroyed {
		return
	}
	// updates is left unset (so empty unallocated map) or refers to the map in
	// p.leader.scratch, so can be read and written without holding
	// pendingAdmittedMu.
	var updates map[roachpb.ReplicaID]rac2.AdmittedVector
	// Swap the pendingAdmittedMu.updates map with the empty scratch if
	// non-empty. This is an optimization to minimize the time we hold the
	// pendingAdmittedMu lock.
	if updatesEmpty := func() bool {
		p.leader.pendingAdmittedMu.Lock()
		defer p.leader.pendingAdmittedMu.Unlock()
		if len(p.leader.pendingAdmittedMu.updates) > 0 {
			updates = p.leader.pendingAdmittedMu.updates
			p.leader.pendingAdmittedMu.updates = p.leader.scratch
			p.leader.scratch = updates
			return false
		}
		return true
	}(); updatesEmpty {
		return
	}
	if p.leader.rc != nil {
		for replicaID, state := range updates {
			p.leader.rc.AdmitRaftMuLocked(ctx, replicaID, state)
		}
	}
	// Clear the scratch from the updates that we have just handled.
	clear(p.leader.scratch)
}

// SideChannelForPriorityOverrideAtFollowerRaftMuLocked implements Processor.
func (p *processorImpl) SideChannelForPriorityOverrideAtFollowerRaftMuLocked(
	info SideChannelInfoUsingRaftMessageRequest,
) {
	p.opts.Replica.RaftMuAssertHeld()
	if p.destroyed {
		return
	}
	if info.UsingV2Protocol {
		if p.follower.lowPriOverrideState.sideChannelForLowPriOverride(
			info.LeaderTerm, info.First, info.Last, info.LowPriOverride) &&
			!p.follower.isLeaderUsingV2Protocol {
			// Either term advanced, or stayed the same. In the latter case we know
			// that a leader does a one-way switch from v1 => v2. In the former case
			// we of course use v2 if the leader is claiming to use v2.
			p.follower.isLeaderUsingV2Protocol = true
		}
	} else {
		if p.follower.lowPriOverrideState.sideChannelForV1Leader(info.LeaderTerm) &&
			p.follower.isLeaderUsingV2Protocol {
			// Leader term advanced, so this is switching back to v1.
			p.follower.isLeaderUsingV2Protocol = false
		}
	}
}

// SyncedLogStorage implements Processor.
func (p *processorImpl) SyncedLogStorage(ctx context.Context, mark rac2.LogMark) {
	// NB: storage syncs will trigger raft Ready processing, so we don't need to
	// explicitly schedule it here like in AdmittedLogEntry.
	p.logTracker.logSynced(ctx, mark)
}

// AdmittedLogEntry implements Processor.
func (p *processorImpl) AdmittedLogEntry(
	ctx context.Context, state EntryForAdmissionCallbackState,
) {
	if p.logTracker.logAdmitted(ctx, state.Mark, state.Priority) {
		// Schedule a raft Ready cycle that will send the updated admitted vector to
		// the leader via AdmittedPiggybacker if it hasn't been sent by then.
		p.opts.RaftScheduler.EnqueueRaftReady(p.opts.RangeID)
	}
}

// AdmittedState implements Processor.
func (p *processorImpl) AdmittedState() rac2.AdmittedVector {
	admitted, _ := p.logTracker.admitted(false /* sched */)
	return admitted
}

// AdmitRaftMuLocked implements Processor.
func (p *processorImpl) AdmitRaftMuLocked(
	ctx context.Context, replicaID roachpb.ReplicaID, av rac2.AdmittedVector,
) {
	p.opts.Replica.RaftMuAssertHeld()
	// NB: rc is always updated while raftMu is held.
	if rc := p.leader.rc; rc != nil {
		rc.AdmitRaftMuLocked(ctx, replicaID, av)
	}
}

// MaybeSendPingsRaftMuLocked implements Processor.
func (p *processorImpl) MaybeSendPingsRaftMuLocked() {
	p.opts.Replica.RaftMuAssertHeld()
	if rc := p.leader.rc; rc != nil {
		rc.MaybeSendPingsRaftMuLocked()
	}
}

// AdmitForEval implements Processor.
func (p *processorImpl) AdmitForEval(
	ctx context.Context, pri admissionpb.WorkPriority, ct time.Time,
) (admitted bool, err error) {
	workClass := admissionpb.WorkClassFromPri(pri)
	mode := kvflowcontrol.Mode.Get(&p.opts.Settings.SV)
	bypass := mode == kvflowcontrol.ApplyToElastic && workClass == admissionpb.RegularWorkClass
	if bypass {
		p.opts.EvalWaitMetrics.OnWaiting(workClass)
		p.opts.EvalWaitMetrics.OnBypassed(workClass, 0 /* duration */)
		return false, nil
	}
	var rc rac2.RangeController
	func() {
		p.leader.rcReferenceUpdateMu.RLock()
		defer p.leader.rcReferenceUpdateMu.RUnlock()
		rc = p.leader.rc
	}()
	if rc == nil {
		p.opts.EvalWaitMetrics.OnWaiting(workClass)
		p.opts.EvalWaitMetrics.OnBypassed(workClass, 0 /* duration */)
		return false, nil
	}
	return rc.WaitForEval(ctx, pri)
}

// ProcessSchedulerEventRaftMuLocked implements Processor.
func (p *processorImpl) ProcessSchedulerEventRaftMuLocked(ctx context.Context) {
	p.opts.Replica.RaftMuAssertHeld()
	if p.destroyed {
		return
	}
	if rc := p.leader.rc; rc != nil {
		rc.HandleSchedulerEventRaftMuLocked(ctx)
	}
}

// InspectRaftMuLocked implements Processor.
func (p *processorImpl) InspectRaftMuLocked(ctx context.Context) (kvflowinspectpb.Handle, bool) {
	p.opts.Replica.RaftMuAssertHeld()
	p.leader.rcReferenceUpdateMu.RLock()
	defer p.leader.rcReferenceUpdateMu.RUnlock()
	if p.leader.rc == nil {
		return kvflowinspectpb.Handle{}, false
	}
	return p.leader.rc.InspectRaftMuLocked(ctx), true
}

// SendStreamStats implements Processor.
func (p *processorImpl) SendStreamStats() rac2.RangeSendStreamStats {
	p.leader.rcReferenceUpdateMu.RLock()
	defer p.leader.rcReferenceUpdateMu.RUnlock()
	if p.leader.rc == nil {
		return nil
	}
	return p.leader.rc.SendStreamStats()
}

// RangeControllerFactoryImpl implements the RangeControllerFactory interface.
var _ RangeControllerFactory = RangeControllerFactoryImpl{}

// RangeControllerFactoryImpl is a factory to create RangeControllers. There
// should be one per-store. When a new RangeController is created, the caller
// provides the range specific information as part of rangeControllerInitState.
type RangeControllerFactoryImpl struct {
	clock                      *hlc.Clock
	evalWaitMetrics            *rac2.EvalWaitMetrics
	streamTokenCounterProvider *rac2.StreamTokenCounterProvider
	closeTimerScheduler        rac2.ProbeToCloseTimerScheduler
	scheduler                  rac2.Scheduler
	sendTokenWatcher           *rac2.SendTokenWatcher
	knobs                      *kvflowcontrol.TestingKnobs
}

func NewRangeControllerFactoryImpl(
	clock *hlc.Clock,
	evalWaitMetrics *rac2.EvalWaitMetrics,
	streamTokenCounterProvider *rac2.StreamTokenCounterProvider,
	closeTimerScheduler rac2.ProbeToCloseTimerScheduler,
	scheduler rac2.Scheduler,
	sendTokenWatcher *rac2.SendTokenWatcher,
	knobs *kvflowcontrol.TestingKnobs,
) RangeControllerFactoryImpl {
	return RangeControllerFactoryImpl{
		clock:                      clock,
		evalWaitMetrics:            evalWaitMetrics,
		streamTokenCounterProvider: streamTokenCounterProvider,
		closeTimerScheduler:        closeTimerScheduler,
		scheduler:                  scheduler,
		sendTokenWatcher:           sendTokenWatcher,
		knobs:                      knobs,
	}
}

// New creates a new RangeController.
func (f RangeControllerFactoryImpl) New(
	ctx context.Context, state rangeControllerInitState,
) rac2.RangeController {
	return rac2.NewRangeController(
		ctx,
		rac2.RangeControllerOptions{
			RangeID:             state.rangeID,
			TenantID:            state.tenantID,
			LocalReplicaID:      state.localReplicaID,
			SSTokenCounter:      f.streamTokenCounterProvider,
			RaftInterface:       state.raftInterface,
			MsgAppSender:        state.msgAppSender,
			Clock:               f.clock,
			CloseTimerScheduler: f.closeTimerScheduler,
			Scheduler:           f.scheduler,
			SendTokenWatcher:    f.sendTokenWatcher,
			EvalWaitMetrics:     f.evalWaitMetrics,
			Knobs:               f.knobs,
		},
		rac2.RangeControllerInitState{
			ReplicaSet:    state.replicaSet,
			Leaseholder:   state.leaseholder,
			NextRaftIndex: state.nextRaftIndex,
		},
	)
}
