// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	// MuLock acquires Replica.mu.
	MuLock()
	// MuUnlock releases Replica.mu.
	MuUnlock()
	// LeaseholderMuLocked returns the Replica's current knowledge of the
	// leaseholder, which can be stale. It is only called after Processor
	// knows the Replica is initialized.
	//
	// At least Replica mu is held. The caller does not make any claims about
	// whether it holds raftMu or not.
	LeaseholderMuLocked() roachpb.ReplicaID
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
	// RaftInterface is an interface that abstracts the raft.RawNode for use in
	// the RangeController.
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
}

// RangeControllerFactory abstracts RangeController creation for testing.
type RangeControllerFactory interface {
	// New creates a new RangeController.
	New(ctx context.Context, state rangeControllerInitState) rac2.RangeController
}

// EnabledWhenLeaderLevel captures the level at which RACv2 is enabled when
// this replica is the leader.
//
// State transitions are NotEnabledWhenLeader => EnabledWhenLeaderV1Encoding
// => EnabledWhenLeaderV2Encoding, i.e., the level will never regress.
type EnabledWhenLeaderLevel uint8

const (
	NotEnabledWhenLeader EnabledWhenLeaderLevel = iota
	EnabledWhenLeaderV1Encoding
	EnabledWhenLeaderV2Encoding
)

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
	RangeControllerFactory RangeControllerFactory
	Settings               *cluster.Settings
	EvalWaitMetrics        *rac2.EvalWaitMetrics

	EnabledWhenLeaderLevel EnabledWhenLeaderLevel
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
// We *strongly* prefer methods to be called without holding
// Replica.mu, since then the callee (implementation of Processor) does not
// need to worry about (a) deadlocks, since processorImpl.mu is ordered before
// Replica.mu, (b) the amount of work it is doing under this critical section.
// The only exception is OnDescChangedLocked, where this was hard to achieve.
//
// TODO(sumeer):
// Integration notes reminders:
//
//   - Make Processor a direct member of Replica (not under raftMu), since
//     want to access it both before eval, on the eval wait path, and when the
//     proposal will be encoded. Processor becomes the definitive source of
//     the current EnabledWhenLeaderLevel.
//
//   - Keep a copy of EnabledWhenLeaderLevel under Replica.raftMu. This will
//     be initialized using the cluster version when Replica is created, and
//     the same value will be passed to ProcessorOptions. In
//     handleRaftReadyRaftMuLocked, which is called with raftMy held, cheaply
//     check whether already at the highest level and if not, read the cluster
//     version to see if ratcheting is needed. When ratcheting up from
//     NotEnabledWhenLeader, acquire Replica.mu and close
//     replicaFlowControlIntegrationImpl (RACv1).
type Processor interface {
	// InitRaftLocked is called when RaftNode is initialized for the Replica.
	// NB: can be called twice before the Replica is fully initialized.
	//
	// Both Replica mu and raftMu are held.
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
	SetEnabledWhenLeaderRaftMuLocked(ctx context.Context, level EnabledWhenLeaderLevel)
	// GetEnabledWhenLeader returns the current level. It may be used in
	// highly concurrent settings at the leaseholder, when waiting for eval,
	// and when encoding a proposal. Note that if the leaseholder is not the
	// leader and the leader has switched to a higher level, there is no harm
	// done, since the leaseholder can continue waiting for v1 tokens and use
	// the v1 entry encoding.
	GetEnabledWhenLeader() EnabledWhenLeaderLevel

	// OnDescChangedLocked provides a possibly updated RangeDescriptor. The
	// tenantID passed in all calls must be the same.
	//
	// Both Replica mu and raftMu are held.
	//
	// TODO(sumeer): we are currently delaying the processing caused by this
	// until HandleRaftReadyRaftMuLocked, including telling the
	// RangeController. However, RangeController.WaitForEval needs to have the
	// latest state. We need to either (a) change this
	// OnDescChangedRaftMuLocked, or (b) add a method in RangeController that
	// only updates the voting replicas used in WaitForEval, and call that
	// from OnDescChangedLocked, and do the rest of the updating later.
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
	// previously enqueued piggybacked admitted vectors. Returns true if
	// HandleRaftReadyRaftMuLocked should be called.
	//
	// raftMu is held.
	ProcessPiggybackedAdmittedAtLeaderRaftMuLocked(ctx context.Context) bool

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
	SyncedLogStorage(ctx context.Context, mark rac2.LogMark, snap bool)
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

	// AdmitForEval is called to admit work that wants to evaluate at the
	// leaseholder.
	//
	// If the callee decided not to admit because replication admission
	// control is disabled, or for any other reason, admitted will be false
	// and error will be nil.
	AdmitForEval(
		ctx context.Context, pri admissionpb.WorkPriority, ct time.Time) (admitted bool, err error)

	// InspectRaftMuLocked returns a handle to inspect the state of the
	// underlying range controller. It is used to power /inspectz-style debugging
	// pages.
	InspectRaftMuLocked(ctx context.Context) (kvflowinspectpb.Handle, bool)
}

type processorImpl struct {
	opts ProcessorOptions

	// State for tracking and advancing the log's admitted vector.
	logTracker logTracker

	// The fields below are accessed while holding the mutex. Lock ordering:
	// Replica.raftMu < this.mu < Replica.mu.
	mu struct {
		syncutil.Mutex

		// Transitions once from false => true when the Replica is destroyed.
		destroyed bool

		leaderID roachpb.ReplicaID
		// leaderNodeID, leaderStoreID are a function of leaderID and
		// raftMu.replicas. They are set when leaderID is non-zero and replicas
		// contains leaderID, else are 0.
		leaderNodeID  roachpb.NodeID
		leaderStoreID roachpb.StoreID
		leaseholderID roachpb.ReplicaID
		// State at a follower.
		follower struct {
			isLeaderUsingV2Protocol bool
			lowPriOverrideState     lowPriOverrideState
		}
		// State when leader, i.e., when leaderID == opts.ReplicaID, and v2
		// protocol is enabled.
		leader struct {
			// pendingAdmitted contains recently delivered admitted vectors. When this
			// map is not empty, the range is scheduled for applying these vectors to
			// the corresponding streams / token trackers. The map is cleared when the
			// admitted vectors are applied.
			//
			// Invariant: rc == nil ==> len(pendingAdmitted) == 0.
			// Invariant: len(pendingAdmitted) != 0 ==> the processing is scheduled.
			pendingAdmitted map[roachpb.ReplicaID]rac2.AdmittedVector
			// Updating the rc reference requires both the enclosing mu and
			// rcReferenceUpdateMu. Code paths that want to access this
			// reference only need one of these mutexes. rcReferenceUpdateMu
			// is ordered after the enclosing mu.
			rcReferenceUpdateMu syncutil.RWMutex
			rc                  rac2.RangeController
			// Term is used to notice transitions out of leadership and back,
			// to recreate rc. It is set when rc is created, and is not
			// up-to-date if there is no rc (which can happen when using the
			// v1 protocol).
			term uint64
		}
		// Is the RACv2 protocol enabled when this replica is the leader.
		enabledWhenLeader EnabledWhenLeaderLevel
	}
	// Fields below are accessed while holding Replica.raftMu. This
	// peculiarity is only to handle the fact that OnDescChanged is called
	// with Replica.mu held.
	raftMu struct {
		raftNode RaftNode
		// replicasChanged is set to true when replicas has been updated. This
		// is used to lazily update all the state under mu that needs to use
		// the state in replicas.
		replicas        rac2.ReplicaSet
		replicasChanged bool
		// Set once, in the first call to OnDescChanged.
		tenantID roachpb.TenantID
	}
	// Atomic value, for serving GetEnabledWhenLeader. Mirrors
	// mu.enabledWhenLeader.
	enabledWhenLeader atomic.Uint32

	v1EncodingPriorityMismatch log.EveryN
}

var _ Processor = &processorImpl{}

func NewProcessor(opts ProcessorOptions) Processor {
	p := &processorImpl{opts: opts}
	p.mu.enabledWhenLeader = opts.EnabledWhenLeaderLevel
	p.enabledWhenLeader.Store(uint32(opts.EnabledWhenLeaderLevel))
	p.v1EncodingPriorityMismatch = log.Every(time.Minute)
	return p
}

// isLeaderUsingV2ProcLocked returns true if the current leader uses the V2
// protocol.
//
// NB: the result of this method does not change while raftMu is held.
func (p *processorImpl) isLeaderUsingV2ProcLocked() bool {
	// We are the leader using V2, or a follower who learned that the leader is
	// using the V2 protocol.
	return p.mu.leader.rc != nil || p.mu.follower.isLeaderUsingV2Protocol
}

// InitRaftLocked implements Processor.
func (p *processorImpl) InitRaftLocked(ctx context.Context, rn RaftNode) {
	p.opts.Replica.RaftMuAssertHeld()
	p.opts.Replica.MuAssertHeld()
	if p.raftMu.replicas != nil {
		log.Fatalf(ctx, "initializing RaftNode after replica is initialized")
	}
	p.raftMu.raftNode = rn
	p.logTracker.init(p.raftMu.raftNode.LogMarkLocked())
}

// OnDestroyRaftMuLocked implements Processor.
func (p *processorImpl) OnDestroyRaftMuLocked(ctx context.Context) {
	p.opts.Replica.RaftMuAssertHeld()
	p.mu.Lock()
	defer p.mu.Unlock()

	p.mu.destroyed = true
	p.closeLeaderStateRaftMuLockedProcLocked(ctx)

	// Release some memory.
	p.mu.follower.lowPriOverrideState = lowPriOverrideState{}
}

// SetEnabledWhenLeaderRaftMuLocked implements Processor.
func (p *processorImpl) SetEnabledWhenLeaderRaftMuLocked(
	ctx context.Context, level EnabledWhenLeaderLevel,
) {
	p.opts.Replica.RaftMuAssertHeld()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mu.destroyed || p.mu.enabledWhenLeader >= level {
		return
	}
	p.mu.enabledWhenLeader = level
	p.enabledWhenLeader.Store(uint32(level))
	if level != EnabledWhenLeaderV1Encoding || p.raftMu.replicas == nil {
		return
	}
	// May need to create RangeController.
	var leaderID roachpb.ReplicaID
	var term uint64
	var nextUnstableIndex uint64
	func() {
		p.opts.Replica.MuLock()
		defer p.opts.Replica.MuUnlock()
		leaderID = p.raftMu.raftNode.LeaderLocked()
		if leaderID == p.opts.ReplicaID {
			term = p.raftMu.raftNode.TermLocked()
			nextUnstableIndex = p.raftMu.raftNode.NextUnstableIndexLocked()
		}
	}()
	if leaderID == p.opts.ReplicaID {
		p.createLeaderStateRaftMuLockedProcLocked(ctx, term, nextUnstableIndex)
	}
}

// GetEnabledWhenLeader implements Processor.
func (p *processorImpl) GetEnabledWhenLeader() EnabledWhenLeaderLevel {
	return EnabledWhenLeaderLevel(p.enabledWhenLeader.Load())
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
	initialization := p.raftMu.replicas == nil
	if initialization {
		// Replica is initialized, in that we now have a descriptor.
		if p.raftMu.raftNode == nil {
			panic(errors.AssertionFailedf("RaftNode is not initialized"))
		}
		p.raftMu.tenantID = tenantID
	} else if p.raftMu.tenantID != tenantID {
		panic(errors.AssertionFailedf("tenantId was changed from %s to %s",
			p.raftMu.tenantID, tenantID))
	}
	p.raftMu.replicas = descToReplicaSet(desc)
	p.raftMu.replicasChanged = true
	// We need to promptly return tokens if some replicas have been removed,
	// since those tokens could be used by other ranges with replicas on the
	// same store. Ensure that promptness by scheduling ready.
	if !initialization {
		p.opts.RaftScheduler.EnqueueRaftReady(p.opts.RangeID)
	}
}

// makeStateConsistentRaftMuLockedProcLocked, uses the union of the latest
// state retrieved from RaftNode, and the set of replica (in raftMu.replicas),
// to initialize or update the internal state of processorImpl.
//
// nextUnstableIndex is used to initialize the state of the send-queues if
// this replica is becoming the leader. This index must immediately precede
// the entries provided to RangeController.
func (p *processorImpl) makeStateConsistentRaftMuLockedProcLocked(
	ctx context.Context,
	nextUnstableIndex uint64,
	leaderID roachpb.ReplicaID,
	leaseholderID roachpb.ReplicaID,
	myLeaderTerm uint64,
) {
	replicasChanged := p.raftMu.replicasChanged
	if replicasChanged {
		p.raftMu.replicasChanged = false
	}
	if !replicasChanged && leaderID == p.mu.leaderID && leaseholderID == p.mu.leaseholderID &&
		(p.mu.leader.rc == nil || p.mu.leader.term == myLeaderTerm) {
		// Common case.
		return
	}
	// The leader or leaseholder or replicas or myLeaderTerm changed. We set
	// everything.
	p.mu.leaderID = leaderID
	p.mu.leaseholderID = leaseholderID
	// Set leaderNodeID, leaderStoreID.
	if p.mu.leaderID == 0 {
		p.mu.leaderNodeID = 0
		p.mu.leaderStoreID = 0
	} else {
		rd, ok := p.raftMu.replicas[leaderID]
		if !ok {
			if leaderID == p.opts.ReplicaID {
				// Is leader, but not in the set of replicas. We expect this
				// should not be happening anymore, due to
				// raft.Config.StepDownOnRemoval being set to true. But we
				// tolerate it.
				log.Errorf(ctx,
					"leader=%d is not in the set of replicas=%v",
					leaderID, p.raftMu.replicas)
				p.mu.leaderNodeID = p.opts.NodeID
				p.mu.leaderStoreID = p.opts.StoreID
			} else {
				// A follower, which can learn about a leader before it learns
				// about a config change that includes the leader in the set
				// of replicas, so ignore.
				p.mu.leaderNodeID = 0
				p.mu.leaderStoreID = 0
			}
		} else {
			p.mu.leaderNodeID = rd.NodeID
			p.mu.leaderStoreID = rd.StoreID
		}
	}
	if p.mu.leaderID != p.opts.ReplicaID {
		if p.mu.leader.rc != nil {
			// Transition from leader to follower.
			p.closeLeaderStateRaftMuLockedProcLocked(ctx)
		}
		return
	}
	// Is the leader.
	if p.mu.enabledWhenLeader == NotEnabledWhenLeader {
		return
	}
	if p.mu.leader.rc != nil && myLeaderTerm > p.mu.leader.term {
		// Need to recreate the RangeController.
		p.closeLeaderStateRaftMuLockedProcLocked(ctx)
	}
	if p.mu.leader.rc == nil {
		p.createLeaderStateRaftMuLockedProcLocked(ctx, myLeaderTerm, nextUnstableIndex)
		return
	}
	// Existing RangeController.
	if replicasChanged {
		if err := p.mu.leader.rc.SetReplicasRaftMuLocked(ctx, p.raftMu.replicas); err != nil {
			log.Errorf(ctx, "error setting replicas: %v", err)
		}
	}
	p.mu.leader.rc.SetLeaseholderRaftMuLocked(ctx, leaseholderID)
}

func (p *processorImpl) closeLeaderStateRaftMuLockedProcLocked(ctx context.Context) {
	if p.mu.leader.rc == nil {
		return
	}
	p.mu.leader.rc.CloseRaftMuLocked(ctx)
	func() {
		p.mu.leader.rcReferenceUpdateMu.Lock()
		defer p.mu.leader.rcReferenceUpdateMu.Unlock()
		p.mu.leader.rc = nil
	}()
	p.mu.leader.pendingAdmitted = nil
	p.mu.leader.term = 0
}

func (p *processorImpl) createLeaderStateRaftMuLockedProcLocked(
	ctx context.Context, term uint64, nextUnstableIndex uint64,
) {
	if p.mu.leader.rc != nil {
		panic("RangeController already exists")
	}
	func() {
		p.mu.leader.rcReferenceUpdateMu.Lock()
		defer p.mu.leader.rcReferenceUpdateMu.Unlock()
		p.mu.leader.rc = p.opts.RangeControllerFactory.New(ctx, rangeControllerInitState{
			replicaSet:     p.raftMu.replicas,
			leaseholder:    p.mu.leaseholderID,
			nextRaftIndex:  nextUnstableIndex,
			rangeID:        p.opts.RangeID,
			tenantID:       p.raftMu.tenantID,
			localReplicaID: p.opts.ReplicaID,
			raftInterface:  p.raftMu.raftNode,
		})
	}()
	p.mu.leader.term = term
	p.mu.leader.pendingAdmitted = map[roachpb.ReplicaID]rac2.AdmittedVector{}
}

// HandleRaftReadyRaftMuLocked implements Processor.
func (p *processorImpl) HandleRaftReadyRaftMuLocked(ctx context.Context, e rac2.RaftEvent) {
	p.opts.Replica.RaftMuAssertHeld()
	p.mu.Lock()
	defer p.mu.Unlock()
	// Skip if the replica is not initialized or already destroyed.
	if p.raftMu.replicas == nil || p.mu.destroyed {
		return
	}
	if p.raftMu.raftNode == nil {
		log.Fatal(ctx, "RaftNode is not initialized")
		return
	}
	// NB: we need to call makeStateConsistentRaftMuLockedProcLocked even if
	// NotEnabledWhenLeader, since this replica could be a follower and the
	// leader may switch to v2.

	// Grab the state we need in one shot after acquiring Replica mu.
	var nextUnstableIndex uint64
	var leaderID, leaseholderID roachpb.ReplicaID
	var myLeaderTerm uint64
	func() {
		p.opts.Replica.MuLock()
		defer p.opts.Replica.MuUnlock()
		nextUnstableIndex = p.raftMu.raftNode.NextUnstableIndexLocked()
		leaderID = p.raftMu.raftNode.LeaderLocked()
		leaseholderID = p.opts.Replica.LeaseholderMuLocked()
		if leaderID == p.opts.ReplicaID {
			myLeaderTerm = p.raftMu.raftNode.TermLocked()
		}
	}()
	if len(e.Entries) > 0 {
		nextUnstableIndex = e.Entries[0].Index
	}
	p.makeStateConsistentRaftMuLockedProcLocked(
		ctx, nextUnstableIndex, leaderID, leaseholderID, myLeaderTerm)

	if !p.isLeaderUsingV2ProcLocked() {
		return
	}

	if av, dirty := p.logTracker.admitted(true /* sched */); dirty {
		if rc := p.mu.leader.rc; rc != nil {
			// If we are the leader, notify the RangeController about our replica's
			// new admitted vector.
			rc.AdmitRaftMuLocked(ctx, p.opts.ReplicaID, av)
		} else if p.mu.leaderNodeID != 0 {
			// If the leader is known, piggyback the updated admitted vector to the
			// message stream going to the leader node.
			// TODO(pav-kv): must make sure the leader term is the same.
			p.opts.AdmittedPiggybacker.Add(p.mu.leaderNodeID, kvflowcontrolpb.PiggybackedAdmittedState{
				RangeID:       p.opts.RangeID,
				ToStoreID:     p.mu.leaderStoreID,
				FromReplicaID: p.opts.ReplicaID,
				ToReplicaID:   p.mu.leaderID,
				Admitted: kvflowcontrolpb.AdmittedState{
					Term:     av.Term,
					Admitted: av.Admitted[:],
				}})
		}
	}

	if p.mu.leader.rc != nil {
		if err := p.mu.leader.rc.HandleRaftEventRaftMuLocked(ctx, e); err != nil {
			log.Errorf(ctx, "error handling raft event: %v", err)
		}
	}
}

func (p *processorImpl) registerLogAppend(ctx context.Context, e rac2.RaftEvent) {
	if len(e.Entries) == 0 {
		return
	}
	after := e.Entries[0].Index - 1
	to := rac2.LogMark{Term: e.Term, Index: e.Entries[len(e.Entries)-1].Index}
	p.logTracker.append(ctx, after, to)
}

// AdmitRaftEntriesRaftMuLocked implements Processor.
func (p *processorImpl) AdmitRaftEntriesRaftMuLocked(ctx context.Context, e rac2.RaftEvent) bool {
	// Register all log appends without exception. If the replica is being
	// destroyed, this should be a no-op, but there is no harm in registering the
	// write just in case.
	p.registerLogAppend(ctx, e)

	// Return false only if we're not destroyed and not using V2.
	if destroyed, usingV2 := func() (bool, bool) {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.mu.destroyed, p.isLeaderUsingV2ProcLocked()
	}(); destroyed || !usingV2 {
		return destroyed
	}
	// NB: destroyed and "using v2" status does not change below since we are
	// holding raftMu.

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
		mark := rac2.LogMark{Term: e.Term, Index: entry.Index}
		var raftPri raftpb.Priority
		if isV2Encoding {
			raftPri = raftpb.Priority(meta.AdmissionPriority)
			if raftPri != priBits {
				panic(errors.AssertionFailedf("inconsistent priorities %s, %s", raftPri, priBits))
			}
			func() {
				p.mu.Lock()
				defer p.mu.Unlock()
				raftPri = p.mu.follower.lowPriOverrideState.getEffectivePriority(entry.Index, raftPri)
			}()
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
			TenantID:       p.raftMu.tenantID,
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
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mu.leader.rc == nil {
		return
	}
	var admitted [raftpb.NumPriorities]uint64
	copy(admitted[:], state.Admitted)
	// Merge in the received admitted vector. We are only interested in the
	// highest admitted marks. Zero value always merges in favour of the new one.
	p.mu.leader.pendingAdmitted[from] = p.mu.leader.pendingAdmitted[from].Merge(
		rac2.AdmittedVector{Term: state.Term, Admitted: admitted})
}

// ProcessPiggybackedAdmittedAtLeaderRaftMuLocked implements Processor.
func (p *processorImpl) ProcessPiggybackedAdmittedAtLeaderRaftMuLocked(ctx context.Context) bool {
	p.opts.Replica.RaftMuAssertHeld()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mu.destroyed || len(p.mu.leader.pendingAdmitted) == 0 {
		return false
	}
	for replicaID, state := range p.mu.leader.pendingAdmitted {
		p.mu.leader.rc.AdmitRaftMuLocked(ctx, replicaID, state)
	}
	clear(p.mu.leader.pendingAdmitted)
	return true
}

// SideChannelForPriorityOverrideAtFollowerRaftMuLocked implements Processor.
func (p *processorImpl) SideChannelForPriorityOverrideAtFollowerRaftMuLocked(
	info SideChannelInfoUsingRaftMessageRequest,
) {
	p.opts.Replica.RaftMuAssertHeld()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mu.destroyed {
		return
	}
	if info.UsingV2Protocol {
		if p.mu.follower.lowPriOverrideState.sideChannelForLowPriOverride(
			info.LeaderTerm, info.First, info.Last, info.LowPriOverride) &&
			!p.mu.follower.isLeaderUsingV2Protocol {
			// Either term advanced, or stayed the same. In the latter case we know
			// that a leader does a one-way switch from v1 => v2. In the former case
			// we of course use v2 if the leader is claiming to use v2.
			p.mu.follower.isLeaderUsingV2Protocol = true
		}
	} else {
		if p.mu.follower.lowPriOverrideState.sideChannelForV1Leader(info.LeaderTerm) &&
			p.mu.follower.isLeaderUsingV2Protocol {
			// Leader term advanced, so this is switching back to v1.
			p.mu.follower.isLeaderUsingV2Protocol = false
		}
	}
}

// SyncedLogStorage implements Processor.
func (p *processorImpl) SyncedLogStorage(ctx context.Context, mark rac2.LogMark, snap bool) {
	if snap {
		p.logTracker.snapSynced(ctx, mark)
	} else {
		p.logTracker.logSynced(ctx, mark)
	}
	// NB: storage syncs will trigger raft Ready processing, so we don't need to
	// explicitly schedule it here like in AdmittedLogEntry.
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
	if rc := p.mu.leader.rc; rc != nil {
		rc.AdmitRaftMuLocked(ctx, replicaID, av)
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
		p.mu.leader.rcReferenceUpdateMu.RLock()
		defer p.mu.leader.rcReferenceUpdateMu.RUnlock()
		rc = p.mu.leader.rc
	}()
	if rc == nil {
		p.opts.EvalWaitMetrics.OnWaiting(workClass)
		p.opts.EvalWaitMetrics.OnBypassed(workClass, 0 /* duration */)
		return false, nil
	}
	return rc.WaitForEval(ctx, pri)
}

// InspectRaftMuLocked implements Processor.
func (p *processorImpl) InspectRaftMuLocked(ctx context.Context) (kvflowinspectpb.Handle, bool) {
	p.opts.Replica.RaftMuAssertHeld()
	p.mu.leader.rcReferenceUpdateMu.RLock()
	defer p.mu.leader.rcReferenceUpdateMu.RUnlock()
	if p.mu.leader.rc == nil {
		return kvflowinspectpb.Handle{}, false
	}
	return p.mu.leader.rc.InspectRaftMuLocked(ctx), true
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
}

func NewRangeControllerFactoryImpl(
	clock *hlc.Clock,
	evalWaitMetrics *rac2.EvalWaitMetrics,
	streamTokenCounterProvider *rac2.StreamTokenCounterProvider,
	closeTimerScheduler rac2.ProbeToCloseTimerScheduler,
) RangeControllerFactoryImpl {
	return RangeControllerFactoryImpl{
		clock:                      clock,
		evalWaitMetrics:            evalWaitMetrics,
		streamTokenCounterProvider: streamTokenCounterProvider,
		closeTimerScheduler:        closeTimerScheduler,
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
			Clock:               f.clock,
			CloseTimerScheduler: f.closeTimerScheduler,
			EvalWaitMetrics:     f.evalWaitMetrics,
		},
		rac2.RangeControllerInitState{
			ReplicaSet:  state.replicaSet,
			Leaseholder: state.leaseholder,
		},
	)
}
