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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	// RaftNodeMuLocked returns a reference to the RaftNode. It is only called
	// after Processor knows the Replica is initialized.
	//
	// At least Replica mu is held. The caller does not make any claims about
	// whether it holds raftMu or not.
	RaftNodeMuLocked() RaftNode
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
	// Read-only methods.

	// LeaderLocked returns the current known leader. This state can advance
	// past the group membership state, so the leader returned here may not be
	// known as a current group member.
	LeaderLocked() roachpb.ReplicaID
	// StableIndexLocked is the (inclusive) highest index that is known to be
	// successfully persisted in local storage, and the leader term on which it
	// was received. The leader term must be at least the term of the entry
	// corresponding to the stable index (and could regress to that entry term
	// if the node crashed and recovered and this stable index predates the
	// crash).
	StableIndexLocked() StableIndexState
	// NextUnstableIndexLocked returns the index of the next entry that will
	// be sent to local storage. All entries < this index are either stored,
	// or have been sent to storage.
	//
	// NB: NextUnstableIndex can regress when the node accepts appends or
	// snapshots from a newer leader.
	NextUnstableIndexLocked() uint64
	// MyLeaderTermLocked returns the term, if this replica is the leader, else
	// 0.
	MyLeaderTermLocked() uint64
	// FollowerState returns the known follower state. If follower is unknown,
	// the empty struct is returned.
	FollowerState(r roachpb.ReplicaID) FollowerState
}

type StableIndexState struct {
	LeaderTerm uint64
	Index      uint64
}

type FollowerState struct {
	State tracker.StateType
	Match uint64
}

// AdmittedPiggybacker is used to enqueue PiggybackedAdmittedState messages
// whose purpose is to advance Admitted. For efficiency, these need to be
// piggybacked on other messages being sent to the given leader node. The
// ToStoreID and RangeID are provided so that the leader node can route the
// incoming message to the relevant range.
type AdmittedPiggybacker interface {
	AddPiggybackedAdmittedState(roachpb.NodeID, kvflowcontrolpb.PiggybackedAdmittedState)
}

// EntryForAdmission is the information provided to the admission control (AC)
// system, when requesting admission.
type EntryForAdmission struct {
	// Information needed by the AC system, for deciding when to admit, and
	// for maintaining its accounting of how much work has been
	// requested/admitted.
	TenantID   roachpb.TenantID
	Priority   admissionpb.WorkPriority
	CreateTime int64
	// RequestedCount is the number of admission tokens requested (not to be
	// confused with replication AC flow tokens).
	RequestedCount int64
	// Ingested is true iff this request represents a sstable that will be
	// ingested into Pebble.
	Ingested bool

	// CallbackState is information that is needed by the callback when the
	// entry is admitted.
	CallbackState EntryForAdmissionCallbackState
}

// EntryForAdmissionCallbackState is passed to the callback when the entry is
// admitted.
type EntryForAdmissionCallbackState struct {
	// Routing state to get to the Processor.
	StoreID roachpb.StoreID
	RangeID roachpb.RangeID

	// State needed by the Processor.
	ReplicaID  roachpb.ReplicaID
	LeaderTerm uint64
	Index      uint64
	Priority   raftpb.Priority
}

// ACWorkQueue abstracts the behavior needed from admission.WorkQueue.
type ACWorkQueue interface {
	// Admit returns false if the entry was not submitted for admission for
	// some reason.
	Admit(ctx context.Context, entry EntryForAdmission) bool
}

// TODO(sumeer): temporary placeholder, until RangeController is more fully
// fleshed out.
type rangeControllerInitState struct {
	replicaSet    rac2.ReplicaSet
	leaseholder   roachpb.ReplicaID
	nextRaftIndex uint64
}

// RangeControllerFactory abstracts RangeController creation for testing.
type RangeControllerFactory interface {
	// New creates a new RangeController.
	New(state rangeControllerInitState) rac2.RangeController
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
	TenantID  roachpb.TenantID
	ReplicaID roachpb.ReplicaID

	Replica                Replica
	RaftScheduler          RaftScheduler
	AdmittedPiggybacker    AdmittedPiggybacker
	ACWorkQueue            ACWorkQueue
	RangeControllerFactory RangeControllerFactory

	EnabledWhenLeaderLevel EnabledWhenLeaderLevel
	ProbeInterval          time.Duration
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
	SetEnabledWhenLeaderRaftMuLocked(level EnabledWhenLeaderLevel)
	// GetEnabledWhenLeader returns the current level. It may be used in
	// highly concurrent settings at the leaseholder, when waiting for eval,
	// and when encoding a proposal. Note that if the leaseholder is not the
	// leader and the leader has switched to a higher level, there is no harm
	// done, since the leaseholder can continue waiting for v1 tokens and use
	// the v1 entry encoding.
	GetEnabledWhenLeader() EnabledWhenLeaderLevel

	// OnDescChangedLocked provides a possibly updated RangeDescriptor.
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
	OnDescChangedLocked(ctx context.Context, desc *roachpb.RangeDescriptor)

	// HandleRaftReadyRaftMuLocked corresponds to processing that happens when
	// Replica.handleRaftReadyRaftMuLocked is called. It must be called even
	// if there was no Ready, since it can be used to advance Admitted, and do
	// other processing.
	//
	// The entries slice is Ready.Entries, i.e., represent MsgStorageAppend on
	// all replicas. To stay consistent with the structure of
	// Replica.handleRaftReadyRaftMuLocked, this method only does leader
	// specific processing of entries.
	// AdmitRaftEntriesFromMsgStorageAppendRaftMuLocked does the general
	// replica processing for MsgStorageAppend.
	//
	// raftMu is held.
	HandleRaftReadyRaftMuLocked(ctx context.Context, entries []raftpb.Entry)
	// AdmitRaftEntriesFromMsgStorageAppendRaftMuLocked subjects entries to
	// admission control on a replica (leader or follower). Like
	// HandleRaftReadyRaftMuLocked, this is called from
	// Replica.handleRaftReadyRaftMuLocked. It is split off from that function
	// since it is natural to position the admission control processing when we
	// are writing to the store in Replica.handleRaftReadyRaftMuLocked. This is
	// a noop if the leader is not using the RACv2 protocol. Returns false if
	// the leader is using RACv1, in which the caller should follow the RACv1
	// admission pathway.
	//
	// raftMu is held.
	AdmitRaftEntriesFromMsgStorageAppendRaftMuLocked(
		ctx context.Context, leaderTerm uint64, entries []raftpb.Entry) bool

	// EnqueueAdmittedStateAtLeader is called at the leader when receiving an
	// admitted vector from a follower, that may advance the leader's knowledge
	// of what the follower has admitted. The caller is responsible for ensuring
	// that ProcessPiggybackedAdmittedAtLeaderRaftMuLocked is called soon.
	//
	// TODO: callee code. Remember the time when this is called.
	EnqueueAdmittedStateAtLeader(msg kvflowcontrolpb.PiggybackedAdmittedState)
	// ProcessPiggybackedAdmittedAtLeaderRaftMuLocked is called to process
	// previous enqueued piggybacked MsgAppResp. Returns true if
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

	// AdmittedLogEntry is called when an entry is admitted. It can be called
	// synchronously from within ACWorkQueue.Admit if admission is immediate.
	AdmittedLogEntry(
		ctx context.Context, state EntryForAdmissionCallbackState,
	)

	// TryAdvanceStableIndex is called to get the latest admitted vector to
	// attach to a MsgAppResp. The caller may have a (leaderTerm, stableIndex)
	// pair that is beyond even the knowledge of Raft, since the
	// MsgStorageAppendResp may not have been stepped into Raft yet.
	TryAdvanceStableIndex(
		leaderTerm uint64, stableIndex uint64) (admitted AdmittedVector, admittedLeaderTerm uint64)

	// TickAndReturnFollowerAdmittedProbesRaftMuLocked returns some probes to be
	// sent to followers who have not recently called
	// EnqueueAdmittedStateAtLeader, have admitted state that is lagging match,
	// and are still in StateReplicate.
	TickAndReturnFollowerAdmittedProbesRaftMuLocked() []*kvserverpb.RaftMessageRequest

	// HandleProbeRaftMuLocked is called when the leader has sent an admitted
	// probe. Returns a RaftMessageRequest to send. Can return nil.
	HandleProbeRaftMuLocked(from roachpb.ReplicaDescriptor) *kvserverpb.RaftMessageRequest
}

type AdmittedVector [raftpb.NumPriorities]uint64

// admittedTracker tracks various state needed for advancing the admitted
// vector. It has its own mutex since it can be called without holding raftMu,
// or processorImpl.mu. Specifically, we don't want the caller to have to
// acquire either of those wide mutexes in (a) the AdmittedLogEntry callback,
// (b) TryAdvanceStableIndex.
type admittedTracker struct {
	replicaID roachpb.ReplicaID
	mu        struct {
		syncutil.Mutex
		// Monotonically non-decreasing.
		leaderTerm uint64
		// Can regress, due to a new leader.
		stableIndex uint64
		// Function of stableIndex and waitingForAdmissionState. Can regress, due
		// to a new leader. For instance admitted for a particular priority could
		// advance to 20 under leaderTerm 1, because both persistence and
		// admission has happened, and then leader term switches to 2 and stable
		// index regresses to 10, and the admitted value would become <= 10.
		admitted AdmittedVector
		// admittedChanged is set to true whenever admitted is updated. It is
		// reset to false in getAdmitted. It tells the caller of getAdmitted if it
		// has new information e.g. there is no need for the caller to call
		// AddPiggybackedAdmittedState if the information it is retrieving has
		// been retrieved and attached to a MsgAppResp.
		admittedChanged bool
		// The followers map only contains entries for the current set of
		// followers, based on the latest range descriptor. A new follower has an
		// admitted vector initialized to 0. Can regress if this node lost
		// leadership and regained it later.
		followers                   map[roachpb.ReplicaID]*admittedState
		waitingForAdmissionState    waitingForAdmissionState
		scheduledAdmittedProcessing bool
	}
}

type admittedState struct {
	admitted          AdmittedVector
	lastUpdateOrProbe time.Time
}

// Returns the previous value.
func (t *admittedTracker) setScheduledAdmittedProcessing(value bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if value == t.mu.scheduledAdmittedProcessing {
		return value
	}
	t.mu.scheduledAdmittedProcessing = value
	return !value
}

func (t *admittedTracker) close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Release some memory.
	t.mu.followers = nil
	t.mu.waitingForAdmissionState = waitingForAdmissionState{}
}

func (t *admittedTracker) addWaiting(leaderTerm uint64, index uint64, pri raftpb.Priority) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.waitingForAdmissionState.add(leaderTerm, index, pri)
}

func (t *admittedTracker) removeWaiting(
	leaderTerm uint64, index uint64, pri raftpb.Priority,
) (admittedAdvanced bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	mayAdvance := t.mu.waitingForAdmissionState.remove(leaderTerm, index, pri)
	if mayAdvance && index <= t.mu.stableIndex {
		t.mu.admitted = t.mu.waitingForAdmissionState.computeAdmitted(t.mu.stableIndex)
		t.mu.admittedChanged = true
		return true
	}
	return false
}

// tryAdvanceStableIndex will be called both using (a) MsgAppResp, when the
// storage write has completed, (b) when processing MsgStorageAppendResp. For
// (a) the "leader term" corresponds to that log write, and must be in the
// MsgAppResp. For (b), even though the MsgStorageAppendResp may be for an
// earlier "accepted term", we are simply using the latest stable index and
// leader term provided by RaftNode, so they are mutually consistent.
func (t *admittedTracker) tryAdvanceStableIndex(leaderTerm uint64, stableIndex uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if leaderTerm < t.mu.leaderTerm {
		return
	}
	if t.mu.leaderTerm < leaderTerm {
		t.mu.leaderTerm = leaderTerm
		t.mu.stableIndex = stableIndex
	} else if t.mu.stableIndex >= stableIndex {
		return
	} else {
		t.mu.stableIndex = stableIndex
	}
	t.mu.admitted = t.mu.waitingForAdmissionState.computeAdmitted(t.mu.stableIndex)
	t.mu.admittedChanged = true
}

func (t *admittedTracker) getAdmitted() (
	admitted [raftpb.NumPriorities]uint64,
	leaderTerm uint64,
	changed bool,
) {
	t.mu.Lock()
	defer t.mu.Unlock()
	admittedChanged := t.mu.admittedChanged
	if admittedChanged {
		t.mu.admittedChanged = false
	}
	return t.mu.admitted, t.mu.leaderTerm, admittedChanged
}

func (t *admittedTracker) setReplicas(replicas rac2.ReplicaSet) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mu.followers == nil {
		t.mu.followers = map[roachpb.ReplicaID]*admittedState{}
	}
	for r := range t.mu.followers {
		if _, ok := replicas[r]; !ok {
			// Not a replica anymore.
			delete(t.mu.followers, r)
		}
	}
	for r := range replicas {
		if r == t.replicaID {
			continue
		}
		if _, ok := t.mu.followers[r]; !ok {
			t.mu.followers[r] = &admittedState{}
		}
	}
}

func (t *admittedTracker) setAdmittedForFollower(
	now time.Time, replica roachpb.ReplicaID, v AdmittedVector,
) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.mu.followers[replica]
	if ok {
		entry.admitted = v
		entry.lastUpdateOrProbe = now
	}
}

func (t *admittedTracker) iterateFollowersForProbing(
	now time.Time,
	probeInterval time.Duration,
	f func(replica roachpb.ReplicaID, admitted AdmittedVector),
) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for r, entry := range t.mu.followers {
		if now.Sub(entry.lastUpdateOrProbe) > probeInterval {
			entry.lastUpdateOrProbe = now
			f(r, entry.admitted)
		}
	}
}

type processorImpl struct {
	opts ProcessorOptions

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
			enqueuedPiggybackedResponses map[roachpb.ReplicaID]AdmittedVector
			rc                           rac2.RangeController
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
	}
	// Atomic value, for serving GetEnabledWhenLeader. Mirrors
	// mu.enabledWhenLeader.
	enabledWhenLeader atomic.Uint32

	// admittedTracker.mu is ordered after processorImpl.mu.
	admittedTracker admittedTracker

	v1EncodingPriorityMismatch log.EveryN
}

var _ Processor = &processorImpl{}

func NewProcessor(opts ProcessorOptions) Processor {
	p := &processorImpl{opts: opts}
	p.mu.enabledWhenLeader = opts.EnabledWhenLeaderLevel
	p.enabledWhenLeader.Store(uint32(opts.EnabledWhenLeaderLevel))
	p.admittedTracker.replicaID = opts.ReplicaID
	p.v1EncodingPriorityMismatch = log.Every(time.Minute)
	return p
}

// OnDestroyRaftMuLocked implements Processor.
func (p *processorImpl) OnDestroyRaftMuLocked(ctx context.Context) {
	p.opts.Replica.RaftMuAssertHeld()
	p.mu.Lock()
	defer p.mu.Unlock()

	p.mu.destroyed = true
	p.closeLeaderStateRaftMuLockedProcLocked(ctx)

	// Release some memory.
	p.admittedTracker.close()
	p.mu.follower.lowPriOverrideState = lowPriOverrideState{}
}

// SetEnabledWhenLeaderRaftMuLocked implements Processor.
func (p *processorImpl) SetEnabledWhenLeaderRaftMuLocked(level EnabledWhenLeaderLevel) {
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
	var myLeaderTerm uint64
	var nextUnstableIndex uint64
	func() {
		p.opts.Replica.MuLock()
		defer p.opts.Replica.MuUnlock()
		leaderID = p.raftMu.raftNode.LeaderLocked()
		if leaderID == p.opts.ReplicaID {
			myLeaderTerm = p.raftMu.raftNode.MyLeaderTermLocked()
			nextUnstableIndex = p.raftMu.raftNode.NextUnstableIndexLocked()
		}
	}()
	if leaderID == p.opts.ReplicaID {
		p.createLeaderStateRaftMuLockedProcLocked(myLeaderTerm, nextUnstableIndex)
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
func (p *processorImpl) OnDescChangedLocked(ctx context.Context, desc *roachpb.RangeDescriptor) {
	p.opts.Replica.RaftMuAssertHeld()
	p.opts.Replica.MuAssertHeld()
	if p.raftMu.replicas == nil {
		// Replica is initialized, in that we have a descriptor. Get the
		// RaftNode.
		p.raftMu.raftNode = p.opts.Replica.RaftNodeMuLocked()
	}
	p.raftMu.replicas = descToReplicaSet(desc)
	p.raftMu.replicasChanged = true
	// Ensure admittedTracker is up-to-date since it may process piggybacked
	// admitted before the next ready.
	p.admittedTracker.setReplicas(p.raftMu.replicas)
	// We need to promptly return tokens if some replicas have been removed.
	// Ensure that promptness by scheduling ready.
	p.opts.RaftScheduler.EnqueueRaftReady(p.opts.RangeID)
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
		p.createLeaderStateRaftMuLockedProcLocked(myLeaderTerm, nextUnstableIndex)
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
	p.mu.leader.rc = nil
	p.mu.leader.enqueuedPiggybackedResponses = nil
	p.mu.leader.term = 0
}

func (p *processorImpl) createLeaderStateRaftMuLockedProcLocked(
	term uint64, nextUnstableIndex uint64,
) {
	if p.mu.leader.rc != nil {
		panic("RangeController already exists")
	}
	p.mu.leader.rc = p.opts.RangeControllerFactory.New(rangeControllerInitState{
		replicaSet:    p.raftMu.replicas,
		leaseholder:   p.mu.leaseholderID,
		nextRaftIndex: nextUnstableIndex,
	})
	p.mu.leader.term = term
	p.mu.leader.enqueuedPiggybackedResponses = map[roachpb.ReplicaID]AdmittedVector{}
}

// HandleRaftReadyRaftMuLocked implements Processor.
func (p *processorImpl) HandleRaftReadyRaftMuLocked(ctx context.Context, entries []raftpb.Entry) {
	p.opts.Replica.RaftMuAssertHeld()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mu.destroyed {
		return
	}
	if p.raftMu.raftNode == nil {
		if buildutil.CrdbTestBuild {
			if len(entries) > 0 {
				panic(errors.AssertionFailedf("entries provided without raft node"))
			}
		}
		return
	}
	// NB: we need to call makeStateConsistentRaftMuLockedProcLocked even if
	// NotEnabledWhenLeader, since this replica could be a follower and the
	// leader may switch to v2.

	// Grab the state we need in one shot after acquiring Replica mu.
	var nextUnstableIndex uint64
	var stableIndex StableIndexState
	var leaderID, leaseholderID roachpb.ReplicaID
	var admitted AdmittedVector
	var myLeaderTerm uint64
	func() {
		p.opts.Replica.MuLock()
		defer p.opts.Replica.MuUnlock()
		nextUnstableIndex = p.raftMu.raftNode.NextUnstableIndexLocked()
		stableIndex = p.raftMu.raftNode.StableIndexLocked()
		leaderID = p.raftMu.raftNode.LeaderLocked()
		leaseholderID = p.opts.Replica.LeaseholderMuLocked()
		if leaderID == p.opts.ReplicaID {
			myLeaderTerm = p.raftMu.raftNode.MyLeaderTermLocked()
		}
	}()
	if len(entries) > 0 {
		nextUnstableIndex = entries[0].Index
	}
	_ = p.admittedTracker.setScheduledAdmittedProcessing(false)
	p.makeStateConsistentRaftMuLockedProcLocked(
		ctx, nextUnstableIndex, leaderID, leaseholderID, myLeaderTerm)

	isLeaderUsingV2 := p.mu.leader.rc != nil || p.mu.follower.isLeaderUsingV2Protocol
	if !isLeaderUsingV2 {
		return
	}
	// If there was a recent MsgStoreAppendResp that triggered this Ready
	// processing, it has already been stepped, so the stable index would have
	// advanced. So this is an opportune place to do Admitted processing.
	p.admittedTracker.tryAdvanceStableIndex(stableIndex.LeaderTerm, stableIndex.Index)
	admitted, leaderTerm, changed := p.admittedTracker.getAdmitted()
	if changed {
		if p.mu.leader.rc == nil && p.mu.leaderNodeID != 0 {
			// Follower, and know leaderNodeID, leaderStoreID.
			p.opts.AdmittedPiggybacker.AddPiggybackedAdmittedState(
				p.mu.leaderNodeID, kvflowcontrolpb.PiggybackedAdmittedState{
					RangeID:       p.opts.RangeID,
					FromReplicaID: p.opts.ReplicaID,
					ToReplicaID:   p.mu.leaderID,
					ToStoreID:     p.mu.leaderStoreID,
					Admitted: kvflowcontrolpb.AdmittedState{
						LeaderTerm: leaderTerm,
						Admitted:   admitted[:],
					},
				})
		}
		// Else if the local replica is the leader, we have already told it about
		// the update by calling tryAdvanceStableIndex. If the leader is not
		// known, we simply drop the message.
	}
	if p.mu.leader.rc != nil {
		if err := p.mu.leader.rc.HandleRaftEventRaftMuLocked(ctx, rac2.RaftEvent{
			Entries: entries,
		}); err != nil {
			log.Errorf(ctx, "error handling raft event: %v", err)
		}
	}
}

// AdmitRaftEntriesFromMsgStorageAppendRaftMuLocked implements Processor.
func (p *processorImpl) AdmitRaftEntriesFromMsgStorageAppendRaftMuLocked(
	ctx context.Context, leaderTerm uint64, entries []raftpb.Entry,
) bool {
	// NB: the state being read here is only modified under raftMu, so it will
	// not become stale during this method.
	var isLeaderUsingV2Protocol bool
	func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		isLeaderUsingV2Protocol = !p.mu.destroyed &&
			(p.mu.leader.rc != nil || p.mu.follower.isLeaderUsingV2Protocol)
	}()
	if !isLeaderUsingV2Protocol {
		return false
	}
	for _, entry := range entries {
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
				p.admittedTracker.addWaiting(leaderTerm, entry.Index, raftPri)
			}()
		} else {
			raftPri = raftpb.LowPri
			if admissionpb.WorkClassFromPri(admissionpb.WorkPriority(meta.AdmissionPriority)) ==
				admissionpb.RegularWorkClass && p.v1EncodingPriorityMismatch.ShouldLog() {
				log.Errorf(ctx,
					"do not use RACv1 for pri %s, which is regular work",
					admissionpb.WorkPriority(meta.AdmissionPriority))
			}
			p.admittedTracker.addWaiting(leaderTerm, entry.Index, raftPri)
		}
		admissionPri := rac2.RaftToAdmissionPriority(raftPri)
		// NB: cannot hold mu when calling Admit since the callback may
		// execute from inside Admit, when the entry is immediately admitted.
		submitted := p.opts.ACWorkQueue.Admit(ctx, EntryForAdmission{
			TenantID:       p.opts.TenantID,
			Priority:       admissionPri,
			CreateTime:     meta.AdmissionCreateTime,
			RequestedCount: int64(len(entry.Data)),
			Ingested:       typ.IsSideloaded(),
			CallbackState: EntryForAdmissionCallbackState{
				StoreID:    p.opts.StoreID,
				RangeID:    p.opts.RangeID,
				ReplicaID:  p.opts.ReplicaID,
				LeaderTerm: leaderTerm,
				Index:      entry.Index,
				Priority:   raftPri,
			},
		})
		if !submitted {
			// Very rare. e.g. store was not found.
			p.admittedTracker.removeWaiting(leaderTerm, entry.Index, raftPri)
		}
	}
	return true
}

// EnqueueAdmittedStateAtLeader implements Processor.
func (p *processorImpl) EnqueueAdmittedStateAtLeader(msg kvflowcontrolpb.PiggybackedAdmittedState) {
	if msg.ToReplicaID != p.opts.ReplicaID {
		// Ignore message to a stale ReplicaID.
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mu.leader.rc == nil {
		return
	}
	// Only need to keep the latest message from a replica.
	v := toAdmittedVector(msg.Admitted)
	prevV := p.mu.leader.enqueuedPiggybackedResponses[msg.FromReplicaID]
	if admittedIncreased(prevV, v) {
		p.mu.leader.enqueuedPiggybackedResponses[msg.FromReplicaID] = v
	}
}

func toAdmittedVector(s kvflowcontrolpb.AdmittedState) AdmittedVector {
	var v AdmittedVector
	if len(s.Admitted) != len(v) {
		panic("")
	}
	for i := range v {
		v[i] = s.Admitted[i]
	}
	return v
}

// ProcessPiggybackedAdmittedAtLeaderRaftMuLocked implements Processor.
func (p *processorImpl) ProcessPiggybackedAdmittedAtLeaderRaftMuLocked(ctx context.Context) bool {
	p.opts.Replica.RaftMuAssertHeld()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mu.destroyed || len(p.mu.leader.enqueuedPiggybackedResponses) == 0 || p.raftMu.raftNode == nil {
		return false
	}
	now := timeutil.Now()
	for k, m := range p.mu.leader.enqueuedPiggybackedResponses {
		p.admittedTracker.setAdmittedForFollower(now, k, m)
		delete(p.mu.leader.enqueuedPiggybackedResponses, k)
	}
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

// AdmittedLogEntry implements Processor.
//
// Do *not* acquire any wide mutex in this callback from the AC subsystem.
func (p *processorImpl) AdmittedLogEntry(
	ctx context.Context, state EntryForAdmissionCallbackState,
) {
	if state.ReplicaID != p.opts.ReplicaID {
		return
	}
	admittedAdvanced :=
		p.admittedTracker.removeWaiting(state.LeaderTerm, state.Index, state.Priority)
	if !admittedAdvanced {
		return
	}
	// admittedAdvanced typically happens when admission happened after stability.
	// We need to schedule processing, if not already done.
	prevScheduled := p.admittedTracker.setScheduledAdmittedProcessing(true)
	if !prevScheduled {
		p.opts.RaftScheduler.EnqueueRaftReady(p.opts.RangeID)
	}
}

func admittedIncreased(prev, next AdmittedVector) bool {
	for i := range prev {
		if prev[i] < next[i] {
			return true
		}
	}
	return false
}

// TryAdvanceStableIndex implements Processor.
func (p *processorImpl) TryAdvanceStableIndex(
	leaderTerm uint64, stableIndex uint64,
) (admitted AdmittedVector, admittedLeaderTerm uint64) {
	p.admittedTracker.tryAdvanceStableIndex(leaderTerm, stableIndex)
	admitted, term, _ := p.admittedTracker.getAdmitted()
	return admitted, term
}

// TickAndReturnFollowerAdmittedProbesRaftMuLocked implements Processor.
func (p *processorImpl) TickAndReturnFollowerAdmittedProbesRaftMuLocked() []*kvserverpb.RaftMessageRequest {
	now := timeutil.Now()
	var buf [5]roachpb.ReplicaID
	replicas := buf[0:0:len(buf)]
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mu.destroyed || p.mu.leader.rc == nil {
		return nil
	}
	p.admittedTracker.iterateFollowersForProbing(now, p.opts.ProbeInterval,
		func(r roachpb.ReplicaID, admitted AdmittedVector) {
			p.opts.Replica.MuLock()
			defer p.opts.Replica.MuUnlock()
			state := p.raftMu.raftNode.FollowerState(r)
			if state.State != tracker.StateReplicate {
				return
			}
			for _, v := range admitted {
				if v < state.Match {
					replicas = append(replicas, r)
					return
				}
			}
		})
	var msgs []*kvserverpb.RaftMessageRequest
	for _, r := range replicas {
		desc, ok := p.raftMu.replicas[r]
		if !ok {
			continue
		}
		msg := &kvserverpb.RaftMessageRequest{
			RangeID:           p.opts.RangeID,
			FromReplica:       p.raftMu.replicas[p.opts.ReplicaID],
			ToReplica:         desc,
			Message:           raftpb.Message{},
			UsingRac2Protocol: true,
			IsAdmittedProbe:   true,
		}
		msgs = append(msgs, msg)
	}
	return msgs
}

func (p *processorImpl) HandleProbeRaftMuLocked(
	from roachpb.ReplicaDescriptor,
) *kvserverpb.RaftMessageRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mu.destroyed {
		return nil
	}
	admitted, leaderTerm, _ := p.admittedTracker.getAdmitted()
	msg := &kvserverpb.RaftMessageRequest{
		RangeID:     p.opts.RangeID,
		FromReplica: p.raftMu.replicas[p.opts.ReplicaID],
		ToReplica:   from,
		Message: raftpb.Message{
			// TODO: this will tickle the RaftMessageRequest handling path on the
			// leader, that will look at Admitted, which is what we want. But there
			// is nothing else here, and we will needlessly step it. As long as Raft
			// ignores it, we are ok. Confirm that Raft will ignore it.
			Type: raftpb.MsgAppResp,
		},
		Admitted: kvflowcontrolpb.AdmittedState{
			LeaderTerm: leaderTerm,
			Admitted:   admitted[:],
		},
	}
	return msg
}

// RangeControllerFactoryImpl implements RangeControllerFactory.
//
// TODO(sumeer): replace with real implementation once RangeController impl is
// ready.
type RangeControllerFactoryImpl struct {
}

func (f RangeControllerFactoryImpl) New(state rangeControllerInitState) rac2.RangeController {
	return nil
}
