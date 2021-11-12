// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// TODO(sumeer):
// Steps:
// - Finalize interface based on comments.
// - Implement interface.
// - Unit tests and randomized tests, including engine restarts that lose
//   state (using vfs.NewStrictMem).
// - Benchmarks comparing single and two engine implementations.
// - Integration (can be done incrementally).

// High-level overview:
//
// ReplicasStorage provides an interface to manage the persistent state that
// includes the lifecycle of a range replica, its raft log, and the state
// machine state. The implementation(s) are expected to be a stateless wrapper
// around persistent state in the underlying engine(s) (any state they
// maintain in-memory would be simply a performance optimization and always
// in-sync with the persistent state). Since this abstraction is mutating the
// same underlying engine state that was previously mutated via lower-level
// interfaces, and is not a data-structure in the usual sense, we can migrate
// callers incrementally to use this interface. That is, callers that use this
// interface, and those that use the lower-level engine interfaces can
// co-exist correctly.
//
// TODO(sumeer): this co-existence is not completely true since the following
// attempts to define an ideal interface where no sst or MutationBatch touches
// both raft engine state or state machine engine state. Which means transient
// inconsistencies can develop. We will either
// - alter this interface to include a more pragmatic once we have settled on
//   the ideal interface.
// - ensure that the minimal integration steo includes ReplicasStorage.Init,
//   which can eliminate any inconsistencies caused by an inopportune crash.
// Hopefully, the latter is sufficient.
//
// We consider the following distinct kinds of persistent state:
// - State machine state: It contains all replicated keys: replicated range-id
//   local keys, range local keys, range lock keys, lock table keys, global
//   keys. This includes the RangeAppliedState and the RangeDescriptor.
//
// - Raft and replica life-cycle state: This includes all the unreplicated
//   range-ID local key names prefixed by Raft, and the RangeTombstoneKey.
//   We will loosely refer to all of these as "raft state".
//   RangeLastReplicaGCTimestamp changes are ignored below, since it is
//   best-effort persistent state used to pace queues, and the caller is
//   allowed to mutate it out-of-band. However when deleting a range,
//   ReplicasStorage will clear that key too.
//
// The interface requires that any mutation (batch or sst) only touch one of
// these kinds of state. This discipline will allow us to eventually separate
// the engines containing these two kinds of state. This interface is not
// relevant for store local keys though they will be in the latter engine. The
// interface does not allow the caller to specify whether to sync a mutation
// to the raft log or state machine state -- that decision is left to the
// implementation of ReplicasStorage. So the hope is that even when we don't
// separate the state machine and raft engines, this abstraction will force us
// to reason more carefully about effects of crashes, and when to sync, and
// allow us to test more thoroughly.
//
// Note that the interface is not currently designed such that raft log writes
// avoid syncing to disk as discussed in
// https://github.com/cockroachdb/cockroach/issues/17500#issuecomment-727094672
// and followup comments on that issue. However, having a clean storage
// abstraction should be a reasonable step in that direction.
//
// ReplicasStorage does not interpret most of the data in the state machine.
// It expects mutations to that state to be provided as an opaque batch, or a
// set of files to be ingested. There are a few exceptions where it can read
// state machine state, mainly when recovering from a crash, so as to make
// changes to get to a consistent state.
// - RangeAppliedStateKey: needs to read this in order to truncate the log,
//   both as part of regular log truncation and on crash recovery.
// - RangeDescriptorKey: needs to read this to discover ranges whose state
//   machine state needs to be discarded on crash recovery.
//
// A corollary to this lack of interpretation is that reads of the state
// machine are not handled by this interface, though it does expose some
// metadata in case the reader want to be sure that the range it is trying to
// read actually exists in storage. ReplicasStorage also does not offer an
// interface to construct changes to the state machine state. It simply
// applies changes, and requires the caller to obey some simple invariants to
// not cause inconsistencies. It is aware of the keyspace occupied by a range
// and the difference between rangeID keys and range keys -- it needs this
// awareness to restore internal consistency when initializing (say after a
// crash), by clearing the state machine state for replicas that should no
// longer exist.
//
// ReplicasStorage does interpret the raft state (all the unreplicated
// range-ID local key names prefixed by Raft), and the RangeTombstoneKey. This
// is necessary for it to be able to maintain invariants spanning the raft log
// and the state machine (related to raft log truncation, replica lifetime
// etc.), including reapplying raft log entries on restart to the state
// machine. All accesses (read or write) to the raft log and RangeTombstoneKey
// must happen via ReplicasStorage. ReplicasStorage does not apply committed
// raft log entries to the state machine under normal operation -- this is
// because state machine application under normal operation has complex
// data-structure side-effects that are outside the scope of ReplicasStorage.
//
// Details:
//
// Since ReplicasStorage does not permit atomic updates spanning the state
// machine and raft state (even if they are a single engine), replica creation
// needs to be sequenced as:
//
// - [C1*] creation of RaftHardStateKey in raft state with {Term:0, Vote:0,
//   Commit:0}.
// - [C2*] creation of state machine state (via snapshot or some synthesized
//   state for rangeID keys etc. in the case of splits).
// - [C3] creation of RaftTruncatedStateKey in raft state and adjustment of
//   RaftHardStateKey (specifically HardState.Commit needs to be set to
//   RangeAppliedState.RaftAppliedIndex -- see below for details). Also
//   discard all raft log entries if any (see below).
//
// Every step above needs to be atomic. The *'s represent writes that need to
// be durable (need to sync) in that they can't be lost due to a crash. After
// step C1, the replica is considered uninitialized. It is initialized after
// step C3. An uninitialized replica will not have any log entries (invariant
// maintained outside ReplicaStorage). Note that we are doing 2 syncs, in
// steps C1 and C2, for the split case, where we currently do 1 sync -- splits
// are not common enough for this to matter.
//
// An initialized replica that receives a snapshot because it has lagged
// behind will execute C2 and C3. The C3 step throws away all the existing
// raft log entries. So a precondition for applying such a snapshot is:
// - The raft log does not have entries beyond the snapshot's
//   RangeAppliedState.RaftAppliedIndex. If it did, there would be no benefit
//   in applying this snapshot.
// - Corollary: since HardState.Commit cannot refer to log entries beyond the
//   locally persisted ones, the existing HardState.Commit <=
//   RangeAppliedState.RaftAppliedIndex, so step C3 will only need to increase
//   the value of HardState.Commit.
// The RaftTruncatedState.{Index,Term} is set to the values corresponding to
// this snapshot.
//
// Deletion is sequenced as:
//
// - [D1*] deletion of RaftHardStateKey, RaftTruncatedStateKey, log entries,
//   RangeLastReplicaGCTimestampKey, and update of RangeTombstoneKey. This is
//   an atomic step.
// - [D2*] deletion of state machine state. Deletion can be done using a
//   sequence of atomic operations, as long as the last one in the sequence is
//   deletion of the RangeDescriptorKey. D2 is a noop if there is no
//   RangeDescriptorKey since that indicates the range was never initialized
//   (this could be the RHS of a split where the split has not yet happened,
//   but we've created an uninitialized RHS, so we don't want to delete the
//   state machine state for the RHS since it doesn't own that state yet).
//   Note that we don't care in this case whether the RangeDescriptor is a
//   provisional one or not (though I believe a provisional RangeDescriptor
//   only exists if there is also a committed one). This step needs to sync
//   because of step D3 after it.
// - [D3] If D2 is not a noop, delete the RangeTombstoneKey.
//
// We now describe the reasoning behind this creation and deletion scheme, and
// go into more details.
//
// - The presence of a RaftHardStateKey (after step C1) implies that we have
//   an uninitialized or initialized replica. The absence of a
//   RaftHardStateKey but a present RangeDescriptorKey (which can happen due
//   to a crash after D1 and before D2) means we need to cleanup some state
//   machine state for this replica (we don't necessarily cleanup all the
//   state machine state since this replica may be dead because of a merge).
//
// - On crash recovery, we need to be self contained in the sense that the
//   ReplicasStorage must be able to execute state changes to reach a fully
//   consistent state without needing any external input, as part of its
//   initialization. We will consider both the creation and deletion cases,
//   and how to reach consistency despite an ill-timed crash.
//
// - Consistency at creation: we need to maintain the following invariants at
//   all times.
//   - HardState.Commit >= RangeAppliedState.RaftAppliedIndex
//     - if HardState.Commit > RangeAppliedState.RaftAppliedIndex, it points
//       to an entry in the log.
//   - RaftTruncatedState.{Index,Term} must be 0 for an uninitialized replica.
//     For an initialized replica RaftTruncatedState.{Index,Term} must be a
//     valid value, and after C3, since there is nothing in the raft log it
//     must reflect the {index,term} values corresponding to the state machine
//     state in C2.
//   If we performed step C3 before C2, there is a possibility that a crash
//   prevents C2. Now we would need to rollback the change made in C3 to reach
//   a fully consistent state on crash recovery. Rolling back HardState.Commit
//   is easy, since there is no raft log, we can set it to
//   RangeAppliedState.RaftAppliedIndex if it exists, else 0. Similarly, we
//   can rollback RaftTruncatedState by either:
//   - deleting it if the RangeAppliedState does not exist, which implies C3
//     did not happen.
//   - if RangeAppliedState exists, roll back RaftTruncatedState.Index to
//     RangeAppliedState.RaftAppliedIndex. However we don't know what to
//     rollback RaftTruncatedState.Term to. Note that this is a case where an
//     already initialized lagging replica has a snapshot being applied.
//   The need to fix RaftTruncatedState.Term on crash recovery requires us to
//   make a change in what we store in RangeAppliedState: RangeAppliedState
//   additionally contains the Term of the index corresponding to
//   RangeAppliedState.RaftAppliedIndex. We will see below that we need this
//   even if we perform C2 before C3.
//
//   An awkwardness with doing C3 before C2 is that we've thrown away the raft
//   log before creating the state machine (or applying the state machine
//   snapshot).
//   TODO*: can we say something stronger than "awkward" here. I would be
//   surprised if this didn't land us in trouble in some manner.
//
//   Therefore we choose to do C2 before C3. Since C3 may not happen due to a
//   crash, at recovery time the ReplicasStorage needs to roll forward to C3
//   when initializing itself.
//   This means doing the following on crash recovery:
//   - If HardState.Commit < RangeAppliedState.RaftAppliedIndex, update
//     HardState.Commit
//   - If RaftTruncatedState does not exist, or
//     RaftTruncatedState.Index < RangeAppliedState.RaftAppliedIndex and all
//     log entries are <= RangeAppliedState.RaftAppliedIndex
//     - Discard all raft log entries.
//     - Set RaftTruncatedState.{Index,Term} using
//       RangeAppliedState.{RaftAppliedIndex,RaftAppliedIndexTerm}
//
//   Since we now have RangeAppliesState.RaftAppliedIndexTerm, constructing an
//   outgoing snapshot only involves reading state machine state (this is a
//   tiny bit related to #72222, in that we are also assuming here that the
//   outgoing snapshot is constructed purely by reading state machine engine
//   state).
//
// - Consistency at deletion: After D1, there is no raft log state and
//   RangeTombstoneKey has been updated past the replicaID contained in the
//   corresponding RangeDescriptor. If there is a crash after D1 and before D2
//   is fully done, the crash recovery will do the following:
//   - Find all the RangeDescriptors and join them with the HardState to
//     figure out which of the initialized ranges are alive and which are
//     dead. Note that we don't need to look at the replicaID in the
//     RangeTombstoneKey, since the state machine state from a deletion is
//     durable (D2) before a subsequent (re)creation of the range.
//   - The dead ranges will have all their rangeID key spans in the state
//     machine removed.
//   - The union of the range (local, global, lock table) key spans of the
//     dead ranges will be computed and the corresponding union of these key
//     spans for the live ranges will be subtracted from the dead key spans,
//     and the resulting key spans will be removed.
//     TODO*: the key spans are in the RangeDescriptor, but RangeDescriptors
//     can be provisional in some cases. Lets assume we are using the committed
//     RangeDescriptors in this case.
//     - Split of R, into R and R2: R has a pre-split committed RangeDescriptor.
//       Since the split is not committed we should use that wider span. Though
//       even if we used the narrower span it should not cause any harm in the
//       span subtraction approach above since R2 will not have a RangeDescriptor yet.
//     - Merge of R and R2 into R: Both have pre-merge committed
//       RangeDescriptors and both have provisional RangeDescriptors (which is
//       empty for the RHS). Could R2 be rebalanced away and removed but R not
//       removed? If yes, and we use the committed RangeDescriptor for R, we
//       would delete R2's data. I suspect we're not changing raft membership
//       in the middle of a merge transaction, since the merge requires both
//       ranges to be on the same nodes, but need to confirm this.
//
// Normal operation of an initialized range is straightforward:
// - ReplicasStorage will be used to append/replace log entries and update HardState.
//   Currently these always sync to disk.
// - The caller keeps track of what prefix of the log is committed, since it
//   constructed HardState. The caller will apply entries to the state machine
//   as needed. These applications do not sync to disk. The caller may not
//   need to read a raft entry from ReplicasStorage in order to apply it, if
//   it happens to have stashed it somewhere in its in-memory data-structures.
// - Log truncation is advised by the caller, based on various signals
//   relevant to the proper functioning of the distributed raft group, except
//   that the caller is unaware of what is durable in the state machine. Hence
//   the advise provided by the caller serves as an upper bound of what can be
//   truncated. Log truncation does not need to be synced.

type RangeState int

const (
	UninitializedStateMachine RangeState = 0
	InitializedStateMachine
	DeletedRange
)

type RangeAndReplica struct {
	RangeID   roachpb.RangeID
	ReplicaID roachpb.ReplicaID
}

type RangeInfo struct {
	RangeAndReplica
	State RangeState
}

// MutationBatch only has a Commit method. We expect the caller to know which
// engine to construct a batch from, in order to update the state machine or
// the raft state. ReplicasStorage does not hide such details since we expect
// the caller to mostly do reads using the engine Reader interface.
type MutationBatch interface {
	Commit(sync bool) error
}

// RaftMutationBatch specifies mutations to the raft log entries and/or
// HardState.
type RaftMutationBatch struct {
	MutationBatch
	// [Lo, Hi) represents the raft log entries, if any in the MutationBatch.
	Lo, Hi uint64
	// HardState, if non-nil, specifies the HardState value being set by
	// MutationBatch.
	HardState *raftpb.HardState
}

// RangeStorage is a handle for a RangeAndReplica that provides the ability to
// write to the raft state and state machine state.
type RangeStorage interface {
	ReplicaID() roachpb.ReplicaID
	State() RangeState

	// CurrentRaftEntriesRange returns [lo, hi) representing the locally stored
	// raft entries. These are guaranteed to be locally durable.
	CurrentRaftEntriesRange() (lo uint64, hi uint64, err error)

	// CanTruncateRaftIfStateMachineIsDurable provides a new upper bound on what
	// can be truncated.
	CanTruncateRaftIfStateMachineIsDurable(index uint64)

	// DoRaftMutation changes the raft state. This will also purge sideloaded
	// files if any entries are being removed. The RaftMutationBatch is
	// committed with sync=true before returning.
	// REQUIRES: if rBatch.Lo < rBatch.Hi, the range is in state
	// InitializedStateMachine.
	DoRaftMutation(rBatch RaftMutationBatch) error

	// TODO(sumeer):
	// - add raft log read methods.
	// - what raft log stats do we need to maintain and expose (raftLogSize?)?

	// State machine commands.

	// IngestRangeSnapshot ingests a snapshot for the range.
	// - The committed RangeDescriptor describes the range as equal to span.
	// - The snapshot corresponds to (raftAppliedIndex,raftAppliedIndexTerm).
	// - sstPaths represent the ssts for this snapshot, and do not include anything
	//   other than state machine state and do not contain any keys outside span
	//   (after accounting for range local keys) and RangeID keys.
	// NB: the ssts contain RangeAppliedState, RangeDescriptor (including
	// possibly a provisional RangeDescriptor). Ingestion is the only way to
	// initialize a range except for the RHS of a split.
	//
	// Snapshot ingestion will fail if span overlaps with the committed span of
	// another range. The committed span can change only via
	// IngestRangeSnapshot, SplitRange, MergeRange so ReplicasStorage can keep
	// track of all committed spans without resorting to reading from the
	// engine(s). It will also fail if the raft log already has entries beyond
	// the snapshot.
	//
	// For reference, this will do steps C2 and C3, where the change
	// corresponding to C2 is being provided in the ssts.
	//
	// In handleRaftReadyRaftMuLocked, if there is a snapshot, it will first
	// call IngestRangeSnapshot, and then DoRaftMutation to change the
	// HardState.{Term,Vote}. We are doing not doing 2 syncs here, since C3 does
	// not need to sync and the subsequent DoRaftMutation will sync.
	// TODO*:
	// - IngestRangeSnapshot in step C3 adjusts HardState.Commit. Will
	//   DoRaftMutation with the HardState returned by RawNode.Ready potentially
	//   regress the HardState.Commit, or is it guaranteed to be consistent with
	//   what we've done when applying the snapshot?
	IngestRangeSnapshot(
		span roachpb.Span, raftAppliedIndex uint64, raftAppliedIndexTerm uint64,
		sstPaths []string) error

	// ApplyCommittedUsingIngest applies committed changes to the state machine
	// state by ingesting sstPaths. The ssts may not contain an update to
	// RangeAppliedState, in which case this call should be immediately followed
	// by a call to ApplyCommittedBatch that does update the RangeAppliedState.
	// It is possible for the node to crash prior to that call to
	// ApplyCommittedBatch -- this is ok since ReplicasStorage.Init will replay
	// this idempotent ingest and the following ApplyCommittedBatch.
	// REQUIRES: range is in state InitializedStateMachine.
	ApplyCommittedUsingIngest(sstPaths []string) error

	// ApplyCommittedBatch applies committed changes to the state machine state.
	// Does not sync.
	// REQUIRES: range is in state InitializedStateMachine.
	ApplyCommittedBatch(smBatch MutationBatch) error
}

type ReplicasStorage interface {
	// Init will block until all the raft and state machine states have been
	// made mutually consistent.
	//
	// It involves:
	// - Computing the live and initialized replicas:
	//   - Fixing the raft log state for live replicas that were part way
	//     through transitioning to initialized (step C3).
	//   - Applying committed entries from raft logs to the state machines when
	//     the state machine is behind (and possibly regressed from before the
	//     crash) because of not syncing.
	// - Cleaning up the state of dead replicas that did not finish cleanup
	//   before the crash (steps D2 and D3).
	Init()

	// Informational. Does not return any ranges with state DeletedRange, since
	// it has no knowledge of them.
	CurrentRanges() []RangeInfo

	// GetHandle returns a handle for a range listed in CurrentRanges().
	// ReplicasStorage will return the same handle object for a RangeAndReplica
	// during its lifetime. Once the RangeAndReplica transitions to DeletedRange
	// state, ReplicasStorage will forget the RangeStorage handle and it is up
	// to the caller to decide when to throw away a handle it may be holding
	// (the handle is not really usable for doing anything once the range is
	// deleted).
	GetHandle(rr RangeAndReplica) (RangeStorage, error)

	// CreateUninitializedRange is used when rebalancing is used to add a range
	// to this node, or a peer informs this node that it has a replica of a
	// range. This is the first step in creating a raft group for this
	// RangeAndReplica. It will return an error if:
	// - This ReplicaID is too old based on the RangeTombstoneKey.
	// - There already exists some state under any raft key for this range.
	//
	// The call will cause HardState to be initialized to {Term:0, Vote:0,
	// Commit:0}. This is step C1 listed in the earlier comment.
	//
	// Typically there will be no state machine state for this range. However it
	// is possible that a split is delayed and some other node has informed this
	// node about the RHS of the split, in which case part of the state machine
	// (except for the RangeID keys, RangeDescriptor) already exist. Note that
	// this locally lagging split case is one where the RHS does not transition
	// to initialized via anything other than a call to SplitRange (i.e., does
	// not apply a snapshot), except when the LHS moves past the split using a
	// snapshot, in which case the RHS can also then apply a snapshot.
	CreateUninitializedRange(rr RangeAndReplica) (RangeStorage, error)

	// SplitRange is called to split range r into a LHS and RHS, where the RHS
	// is represented by rhsRR. The smBatch specifies the state machine state to
	// modify for the LHS and RHS. rhsSpan is the committed span in the
	// RangeDescriptor for the RHS. The following cases can occur:
	//
	// - [A1] RangeTombstone for the RHS indicates rhsRR.ReplicaID has already
	//   been removed. Two subcases:
	//   - [A11] There exists a HardState for rhsRR.RangeID: the range has been
	//     added back with a new ReplicaID.
	//   - [A12] There exists no HardState, so rhsRR.RangeID should not exist on
	//     this node.
	// - [A2] RangeTombstone for the RHS indicates that rhsRR.ReplicaID has not
	//   been removed.
	// For A11 and A12, the smBatch must be clearing all state in the state
	// machine for the RHS. rhsRR.State specifies what state RHS will be in: A11
	// is UninitializedStateMachine, A12 is DeletedRange. For A2, the smBatch
	// must be constructing the appropriate rangeID local state and range local
	// state (including the RangeDescriptor) and the state is
	// InitializedStateMachine. We are relying on the caller properly
	// initializing smBatch and classifying what it has done so that the callee
	// can check that the classification is consistent with the state it
	// observes (so the caller needs to follow some of the code structure
	// currently in splitPreApply. We do not want to move that logic into
	// ReplicasStorage since in general ReplicasStorage is not concerned with
	// knowing the details of state machine state).
	//
	// From our earlier discussion of replica creation and deletion.
	// - For case A2, the callee will perform step C1 if needed, then commit
	//   smBatch (step C2), and then perform step C3.
	// - For case A11 there is no need to do step C1. Step C2 is performed by
	//   committing smBatch. Step C3 will not find any RangeAppliedState so
	//   HardState.Commit does not need adjusting, and RaftTruncatedState will
	//   be 0.
	// - For case A12, the callee is doing step D2 of deletion. Since the RHS
	//   range never transitioned to initialized (it never had a
	//   RangeDescriptor), the deletion was unable to executed D2 when the
	//   HardState etc. was being deleted.
	//
	// REQUIRES: The range being split is in state InitializedStateMachine, and
	// RHS either does not exist or is in state UninitializedStateMachine.
	//
	// Called below Raft -- this is being called when the split transaction commits.
	SplitRange(r RangeStorage, rhsRR RangeInfo, rhsSpan roachpb.Span, smBatch MutationBatch) (RangeStorage, error)

	// MergeRange is called to merge two ranges. smBatch contains the mutations
	// to delete the rangeID local keys for the RHS and the range local keys in
	// the RHS that are anchored to the RHS start key (RangeDescriptorKey and
	// QueueLastProcessedKey). It also includes any changes to the LHS to
	// incorporate the state of the RHS.
	//
	// TODO*: can we receive a post-merge snapshot for the LHS, and apply it
	// instead of applying the merge via raft (which is what calls MergeRange)?
	// I suspect this is disallowed since we cannot apply a snapshot that
	// overlaps with an existing different range (since it would overlap with
	// RHS). Is something preventing raft log truncation at the leaseholder that
	// throws away the merge log entry, so some raft group members would need to
	// catchup using a snapshot? Hmm, according to the comment in
	// https://github.com/cockroachdb/cockroach/pull/72745/files such a snapshot
	// can be applied -- understand this better.
	//
	// REQUIRES: LHS and RHS are in state InitializedStateMachine.
	//
	// Called below Raft -- this is being called when the merge transaction commits.
	MergeRange(lhsRS RangeStorage, rhsRS RangeStorage, smBatch MutationBatch) error

	// DiscardRange that has been rebalanced away. The range is not necessarily
	// initialized.
	DiscardRange(r RangeStorage) error
}

// MakeSingleEngineReplicasStorage constructs a ReplicasStorage where the same
// Engine contains the the raft log and the state machine.
func MakeSingleEngineReplicasStorage(eng Engine) ReplicasStorage {
	// TODO(sumeer): implement
	return nil
}
