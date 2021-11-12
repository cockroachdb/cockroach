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
// - The description of steps performed for creation/deletion/splits/merges
//   needs to be more precise and amenable to a proof sketch.
// - Terminology in comments needs tightening:
//   - commit is used for both raft commit and txn commit (for splits/merges).
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
// - ensure that the minimal integration step includes ReplicasStorage.Init,
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
//   ReplicasStorage will clear that key too. Note that
//   RangeLastReplicaGCTimestamp is placed in this category because it is not
//   replicated state machine state.
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
// metadata in case the reader want to be sure that the replica it is trying to
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
// machine (this reapplication includes committed splits and merges). All
// accesses (read or write) to the raft log and RangeTombstoneKey must happen
// via ReplicasStorage. ReplicasStorage does not apply committed raft log
// entries to the state machine under normal operation -- this is because
// state machine application under normal operation has complex data-structure
// side-effects that are outside the scope of ReplicasStorage.
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
//   discard all raft log entries if any (see below). This step will be a noop
//   if there is no RangeAppliedState after step C2 -- this can happen in some
//   split cases, in which case the real initialization (by executing C2 and
//   C3) will happen later using a snapshot (we explain this below).
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
//   - D2 will only delete the RangeID local keys, when this range deletion is
//     due to a merge.
// - [D3] If D2 is not a noop, delete the RangeTombstoneKey.
//
// TODO(sumeer): The real reason to sync after D2 here is not due to D3, but
// because we could later execute C1 when adding the range back to this node,
// and then crash. On crash recovery we'd find the raft HardState and old
// state machine state and incorrectly think this is an initialized replica.
// Fix the text.
//
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
//   The correctness problem with doing C3 before C2 is not that we can't make
//   the raft and state machine state consistent with each other, but that the
//   node violates raft promises it has made earlier. For example, if the
//   state machine had applied index 20 and the raft log contained [15, 25),
//   then this node is part of the quorum that causes [21, 25) to commit. If
//   after the crash this node has applied index 20 and has no raft state, it
//   is in effect an unavailable replica, since it no longer has [21, 25).
//
//   Therefore we do C2 before C3. Since C3 may not happen due to a crash, at
//   recovery time the ReplicasStorage needs to roll forward to C3 when
//   initializing itself.
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
//     figure out which of the initialized replicas are alive and which are
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
// Normal operation of an initialized replica is straightforward:
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

type ReplicaState int

const (
	UninitializedStateMachine ReplicaState = 0
	InitializedStateMachine
	DeletedRange
)

type RangeAndReplica struct {
	RangeID   roachpb.RangeID
	ReplicaID roachpb.ReplicaID
}

type RangeInfo struct {
	RangeAndReplica
	State ReplicaState
}

// MutationBatch can be committed to the underlying engine. Additionally it
// provides access to the underlying Batch. In some cases the implementation
// of ReplicasStorage will add additional mutations before committing. We
// expect the caller to know which engine to construct a batch from, in order
// to update the state machine or the raft state. ReplicasStorage does not
// hide such details since we expect the caller to mostly do reads using the
// engine Reader interface.
type MutationBatch interface {
	Commit(sync bool) error
	Batch() *Batch
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
// write to the raft state and state machine state. This could have been named
// ReplicaStorage, but that sounds too similar to ReplicasStorage. Note that,
// even though a caller can have two different RangeStorage handles for the
// same range, if it has been added and removed and so has different
// ReplicaIDs, at most one of them will be in state != DeletedRange.
type RangeStorage interface {
	ReplicaID() roachpb.ReplicaID
	State() ReplicaState

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
	// TODO*: I'd overlooked raft.Ready.MustSync. Is it false only when the
	// there is no change to HardState and log entries are being appended at the
	// leader, since it is allowed to send out new log entries before locally
	// syncing? We would need to capture that with a parameter here. Do we
	// additionally add a method to be able to sync the raft state, or can we
	// rely on the fact that when the leader has to mark these entries
	// committed, it will need to update HardState and that will set
	// MustSync=true.
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
	// Snapshot ingestion will not be accepted if:
	// - span overlaps with the committed span of another range, unless the
	//   range is listed in subsumedRangeIDs. The ranges listed in
	//   subsumedRangeIDs must have spans that lie wholly within span. The
	//   committed span of a range can change only via IngestRangeSnapshot,
	//   SplitRange, MergeRange, so ReplicasStorage can keep track of all
	//   committed spans without resorting to repeated reading from the
	//   engine.
	// - the raft log already has entries beyond the snapshot.
	//
	// For reference, this will do:
	// - Step D1 for all subsumed ranges. This syncs, but subsumed ranges will
	//   usually be empty.
	// - Step D2 for all subsumed ranges. This syncs, but subsumed ranges will
	//   usually be empty. Syncing here could be optimized away since the D2 for
	//   a range is performed using an atomic batch, since the ssts include
	//   RANGEDELs for the whole span and will cause the atomic batch to be
	//   flushed.
	// - Ingest the ssts, which is step C2 for this range.
	// - Step C3 for this range.
	// It is possible for the node to crash after step D1 or D2 above which
	// means in Init we will cleanup all the state machine state for the
	// subsumed replicas, before the merge that subsumed them. This is ok for
	// the following reason: since this snapshot was accepted, this store must
	// not have contained the raft log entries corresponding to any of the
	// merges that subsumed the replicas. So it did not contribute to the quorum
	// for those merges (in any of the raft groups that survived after each of
	// those merges). It did possibly contribute to the quorum for the raft
	// groups of the RHS of those merges, but those groups no longer exist.
	//
	// In handleRaftReadyRaftMuLocked, if there is a snapshot, it will first
	// call IngestRangeSnapshot, and then DoRaftMutation to change the
	// HardState.{Term,Vote,Commit}. We are doing not doing 2 syncs here, since
	// C3 does not need to sync and the subsequent DoRaftMutation will sync.
	// TODO(sumeer): adjust this "DoRaftMutation will sync" based on tbg's
	// answer to the other TODO*.
	// Note that the etcd/raft logic fast forwards the HardState.Commit to the
	// snapshot index, so the DoRaftMutation will not actually change the stored
	// value of HardState.Commit from what was already set in
	// IngestRangeSnapshot.
	IngestRangeSnapshot(
		span roachpb.Span, raftAppliedIndex uint64, raftAppliedIndexTerm uint64,
		sstPaths []string, subsumedRangeIDs []roachpb.RangeID) error

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
	// REQUIRES: replica is in state InitializedStateMachine (this is because we
	// create a new range with the first log entry at raftInitialLogIndex (10),
	// so a range always requires an initial state "snapshot" before it can
	// apply raft entries).
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
	//     crash) because of not syncing. There is a potential subtlety here for
	//     merges of R1 and R2: it cannot apply the merge in R1's raft log until
	//     R2's state machine is up-to-date, so this merge application in R1
	//     would need to pause and find R2 and bring it up to date, and only
	//     then continue (if possible). Since AdminMerge ensures *all* R2
	//     replicas are up-to-date before it creates the log entry in R1 that
	//     will do the merge (by calling waitForApplication), one can be sure
	//     that if this store can apply the merge, the R2 replica here has all
	//     the log entries in order to become up-to-date. So after it has
	//     applied R2 up to the committed index it can resume applying R1.
	//     TODO*:
	//     - confirm my understanding of this merge invariant.
	//     - waitForApplication cannot prevent regression of R2's state machine
	//       in a node that crashes and recovers. Since we currently don't have
	//       an Init() functionality, what if the raft group for R1 gets
	//       reactivated and starts applying the committed entries and applies
	//       the merge with regressed R2 state? Is changeRemovesReplica=true
	//       (which forces a sync on state machine application) for the first
	//       batch in a merge transaction?
	// - Cleaning up the state of dead replicas that did not finish cleanup
	//   before the crash (steps D2 and D3). This happens after the previous
	//   step since a merge may have deleted the HardState for the RHS and
	//   crashed before completing the merge -- the previous step will finish
	//   the merge which will prevent this step from cleaning up state that is
	//   now owned by the merged range. Finishing the merge in the previous
	//   includes intent resolution on the updated RangeDescriptor for the
	//   LHS.

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
	// For A11 and A12, the smBatch will be altered to clear all state in the
	// state machine for the RHS. The final state RHS will be in for A11 is
	// UninitializedStateMachine, for A12 is DeletedRange. For A2, the smBatch
	// must be constructing the appropriate rangeID local state and range local
	// state (including the RangeDescriptor) and the final RHS state is
	// InitializedStateMachine. If the final RHS state is
	// UninitializedStateMachine, a nil RangeStorage will be returned.
	//
	// From our earlier discussion of replica creation and deletion.
	// - For case A2, the callee will perform step C1 if needed, then commit
	//   smBatch (step C2), and then perform step C3.
	// - For case A11 there is no need to do step C1. Step C2 is performed by
	//   altering and committing smBatch. Step C3 will not find any RangeAppliedState so
	//   HardState.Commit does not need adjusting, and RaftTruncatedState will
	//   be 0. That is, step C3 is a noop and the RHS stays in
	//   UninitializedStateMachine state.
	// - For case A12, the callee is doing step D2 of deletion, by altering and
	//   committing smBatch. Since the RHS range never transitioned to
	//   initialized (it never had a RangeDescriptor), the deletion was unable
	//   to execute D2 when the HardState etc. was being deleted. The RHS will
	//   continue to be in DeletedRange state when the method returns.
	//
	// REQUIRES: The range being split is in state InitializedStateMachine, and
	// RHS either does not exist or is in state UninitializedStateMachine.
	//
	// Called below Raft -- this is being called when the split transaction commits.
	SplitRange(
		r RangeStorage, rhsRR RangeAndReplica, rhsSpan roachpb.Span, smBatch MutationBatch,
	) (RangeStorage, error)

	// TODO*: What happens to the transactional delete of the RangeDescriptor of
	// the RHS in a merge. It can't be getting applied here, since that is a different raft
	// group. I am guessing the following (confirm with tbg):
	// - The merge txns transaction record is on the LHS, so when it commits,
	//   we also delete the state of the RHS that is not relevant to the merged
	//   range (and do this deletion below raft). This deletion does not include
	//   the RangeDescriptor for the RHS since it is range local state that is
	//   expected to be relevant to the merged range (though the RHS
	//   RangeDescriptor actually isn't relevant).
	// - After the previous step, the LHR RangeDescriptor is resolved synchronously,
	//   but the RHS is not. Someone could observe it and do resolution, which will
	//   delete it.
	// - QueueLastProcessedKey is another range local key that is anchored to the
	//   start of a range an no longer relevant when the RHS is deleted. I don't
	//   see it being deleted in AdminMerge. How is that cleaned up?

	// MergeReplicas is called to merge two range replicas. smBatch contains
	// changes to the LHS to incorporate the state of the RHS.
	//
	// It will perform the following steps:
	// - Apply smBatch, which transforms the LHS into the merged range, and
	//   sync. This sync ensures that the RHS that is up-to-date is durable too.
	//   A crash after this step and before the next step will be rolled forward
	//   in Init. This step has also committed the RangeDescriptor for the
	//   merged range.
	// - D1 for the RHS. This will sync. At this point there is a
	//   RangeDescriptor for the RHS, but no raft state, so the range is
	//   considered deleted. But we need to delete the RangeID local keys for
	//   the RHS before someone resolves the deletion of the RangeDescriptor (if
	//   the RangeDescriptor is gone we won't know there is garbage leftover),
	//   which we do in the next step.
	// - D2 for the RHS. This does not need to sync. We could also do this by
	//   altering smBatch, in the first step.
	//
	// REQUIRES: LHS and RHS are in state InitializedStateMachine, and RHS has
	// applied all commands up to the merge.
	//
	// Called below Raft -- this is being called when the merge transaction commits.
	MergeReplicas(lhsRS RangeStorage, rhsRS RangeStorage, smBatch MutationBatch) error

	// DiscardReplica that has been rebalanced away. The replica is not
	// necessarily initialized.
	DiscardReplica(r RangeStorage) error
}

// MakeSingleEngineReplicasStorage constructs a ReplicasStorage where the same
// Engine contains the the raft log and the state machine.
func MakeSingleEngineReplicasStorage(eng Engine) ReplicasStorage {
	// TODO(sumeer): implement
	return nil
}
