// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
)

// TODO(sumeer):
// Interface and design:
// - Pick names for C1-C3 and D1-D2, to make it easier to remember what we are
//   referring to in various places.
// - Consider separating RecoveryInconsistentReplica into two different states
//   RecoveryRaftAndStateInconsistent, and RecoveryStateInconsistent.
// - Proof sketch.
// - Read cockroach debug check-store to see additional assertion ideas/code
//   we can lift.

// TODO(sumeer):
// See partial prototype in https://github.com/cockroachdb/cockroach/pull/88606.
//
// Implementation:
// - Implement interface.
// - Unit tests and randomized tests, including engine restarts that lose
//   state (using vfs.NewCrashableMem).
// - Benchmarks comparing single and two engine implementations.
// - Race-build dynamically asserts that SSTs or MutationBatches that are
//   passed through this interface only touch the keys they are allowed to
//   touch.
// - Integration (can be done incrementally).
// - Misc cleanup:
//   - Merges should cleanup QueueLastProcessedKey,

// High-level overview:
//
// ReplicasStorage provides an interface to manage the persistent state that
// includes the lifecycle of a range replica, its raft log, and the state
// machine state. The implementation(s) are expected to be a stateless wrapper
// around persistent state in the underlying engine(s) (any state they
// maintain in-memory is simply a performance optimization and always
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
// - alter this interface to be more pragmatic, i.e., be a step towards the
//   ideal interface, but not the end product, once we have settled on the
//   ideal interface.
// - ensure that the minimal integration step includes ReplicasStorage.Init,
//   which can eliminate any inconsistencies caused by an inopportune crash.
// Hopefully, the latter is sufficient.
//
// We consider the following distinct kinds of persistent state:
// - State machine state: It contains all replicated keys: replicated range-id
//   local keys, range local keys, range lock keys, global keys. NB: this
//   includes the RangeAppliedState and the RangeDescriptor.
//
// - Raft state: This includes all the unreplicated range-ID local key names
//   prefixed by Raft. We will loosely refer to all of these as "raft state".
//   RangeLastReplicaGCTimestamp changes are ignored below, since it is
//   best-effort persistent state used to pace queues, and the caller is
//   allowed to mutate it out-of-band. However when deleting a replica,
//   ReplicasStorage will clear that key too. RangeLastReplicaGCTimestamp is
//   placed in this state category because it is not replicated state machine
//   state.
//
// The interface requires that any mutation (batch or sst) only touch one of
// these kinds of state. This discipline will allow us to eventually separate
// the engines containing these two kinds of state. This interface is not
// relevant for store local keys though they will be in the latter engine. The
// interface does not allow the caller to specify whether to sync a mutation
// to the raft log or state machine state -- that decision is left to the
// implementation of ReplicasStorage (with a couple of small exceptions where
// a sync is explicitly requested, which are explained later). So even when we
// don't separate the state machine and raft engines, this abstraction forces
// us to reason more carefully about effects of crashes, and when to sync, and
// allow us to test more thoroughly.
//
// RangeTombstoneKey: This is an unreplicated key that is critical to the
// replica lifecycle. Since it is unreplicated, it is not part of the state
// machine. However, placing it in the category of "raft state" with the other
// unreplicated keys turns out to be complicated:
// (a) in various range merge situations (including replicas being subsumed
// during snapshot application) we need to atomically move the state machine
// forward for the surviving range, delete the state machine state for the
// subsumed range(s) and set the RangeTombstone.
// (b) when removing a replica due to rebalancing we need to atomically remove
// the state machine and set the RangeTombstone.
// For these reasons, we require that the RangeTombstone be in the same engine
// as the state machine state. However, it can only be mutated by
// ReplicasStorage.
//
// Note about terminology as pertaining to range-id keys: "range-id local
// keys" and "range-id keys" are the same thing, since all range-id keys are
// local. As documented in the keys package, range-id keys can be replicated
// or unreplicated. All the replicated range-id keys plus the
// RangeTombstoneKey (which is unreplicated) are referred to as "range-id
// state machine" keys. All the remaining unreplicated range-id keys belong to
// raft state and are referred to as "range-id raft" keys or simply "raft"
// keys (since all raft keys are also unreplicated range-id keys).
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
//   both as part of regular log truncation (see the comment section on
//   "Normal Replica Operation") and on crash recovery (see the comment
//   section on "Replica Initialization" and "Crash Recovery" for details).
// - RangeDescriptorKey: needs to read this to discover the spans of
//   initialized replicas (see the comment sections on "Invariants" and "Crash
//   Recovery").
//
// A corollary to this lack of interpretation is that reads of the state
// machine are not handled by this interface, though it does expose some
// metadata in case the reader want to be sure that the replica it is trying
// to read actually exists in storage. ReplicasStorage also does not offer an
// interface to construct changes to the state machine state. It applies
// changes, and requires the caller to obey some simple invariants to not
// cause inconsistencies. It is aware of the keyspace occupied by a replica
// and the difference between range-ID keys and range keys -- it needs this
// awareness to discard (parts of) replica state when replicas are moved or
// merged away.
//
// ReplicasStorage does interpret the raft state (all the unreplicated
// range-ID local key names prefixed by Raft), and the RangeTombstoneKey. This
// is necessary for it to be able to maintain invariants spanning the raft log
// and the state machine (related to raft log truncation, replica lifetime
// etc.), including reapplying raft log entries on restart, to the state
// machine. All accesses (read or write) to the raft log and RangeTombstoneKey
// must happen via ReplicasStorage. ReplicasStorage does not by itself apply
// committed raft log entries to the state machine in a running system -- this
// is because state machine application in a running system has complex
// data-structure side-effects that are outside the scope of ReplicasStorage.
// It is told by the caller to apply a committed entry, which also requires
// the caller to provide the state machine changes. ReplicasStorage does apply
// "simple" entries directly to the state machine during Init to fix any
// inconsistency of the state machine caused by durable sst ingestion and
// non-durable batch application (see ApplyCommitted* methods in the
// interface). Since these could be preceded by non-durable configuration
// changes, the notion of "simple" entries includes configuration changes,
// except for splits and merges (which we sync to ensure durability -- this is
// justified in the section below on "Implementation constraints on
// ReplicasStorage").
//
// TODO(sumeer):
// https://github.com/etcd-io/etcd/issues/7625#issuecomment-489232411 relies
// on a correctness argument based on bounded regression of conf changes.
// Consider strengthening that correctness argument by making the committed
// index durable for a conf change before applying it. We could introduce a
// `ApplyConfChange(MutationBatch, highestRaftIndex uint64)` method, like we
// have for ingestion, and first sync the Commit state if needed. That way we
// will not have any conf change regression.
//
// ============================================================================
// Invariants:
//
// INVARIANT (RaftAndStateConsistency): when there is any data in the state
// machine associated with a given RangeID, there is a corresponding
// internally consistent Raft state (according to etcd/raft) that is also
// consistent with the applied state on the state machine (i.e. the latter
// references a valid log position). Specifically,
// - HardState.Commit >= RangeAppliedState.RaftAppliedIndex
// - if HardState.Commit > RangeAppliedState.RaftAppliedIndex, it points to an
//   entry in the raft log.
// - RaftTruncatedState.{Index,Term} must be a valid value corresponding to
//   what was truncated. If there are no raft log entries,
//   RaftTruncatedState.{Index,Term} must equal
//   RangeAppliedState.{RaftAppliedIndex,RaftAppliedIndexTerm}.
//
// INVARIANT (StateConsistency): when there is any data in the state machine
// associated with a given RangeID, it will reflect the replicated state at
// the corresponding applied index (i.e., it materializes the replicated log
// at this index).
// Additionally, a range is first created with a RangeDescriptor present
// (external invariant) and neither the callers nor ReplicasStorage will ever
// selectively delete it. NOTE: subsumption as part of a range merge does
// delete the RangeDescriptor, but the Replica atomically ceases to exist in
// the process.
// Specifically,
// - The state machine state must be consistent with the value of
//   RaftAppliedIndex, i.e., it equals a state generated from the full history
//   of this range (for a range that has never been the LHS of a merge, this
//   is the initial snapshot when the range came into being, followed by all
//   subsequent raft log entries).
// - RaftAppliedIndex >= RaftInitialLogIndex
// - RaftAppliedIndexTerm >= RaftInitialLogTerm
// - Has at least 1 non-provisional RangeDescriptor.
// - Regression of the HardState.Commit and RaftAppliedIndex is permitted due
//   to a crash except for the following:
//   - Split that has progressed to applying a state machine change that
//     results in a non-provisional RangeDescriptor for the RHS must not
//     regress after the crash (i.e., must sync application of the split
//     trigger).
//   - Merge that has progressed to applying a state machine change that
//     deletes the RangeDescriptor for the RHS must not regress after the
//     crash (i.e., must sync application of the merge trigger).
//   One could possibly relax these split/merge invariants but the corner
//   cases are very subtle and make it hard to reason about correctness.
//   As an example, see the discussion about "not syncing for splits" in
//   https://github.com/cockroachdb/cockroach/pull/72745#pullrequestreview-807989047
//
// INVARIANT (InterReplicaStateConsistency): The latest non-provisional
// RangeDescriptors of replicas with state machine state have spans that do
// not overlap. We use the term replica-descriptor to refer to this latest
// non-provisional RangeDescriptor, in the text below.
//
// DEFINITION (InitializedStateMachine): a Replica with state
// InitializedStateMachine, has state machine state and obeys the invariants
// RaftAndStateConsistency, StateConsistency, InterReplicaStateConsistency.
//
// DEFINITION (DeletedReplica): it can be convenient to reference Replicas
// that once existed but no longer do, as evidenced by the presence of a
// RangeTombstone for a RangeID, but no state machine or raft state.
// RangeTombstone.NextReplicaID is populated with a value > the last ReplicaID
// seen by ReplicasStorage for this range. Note that RangeTombstone is
// populated even for ranges that no longer exist (RHS of a merge) -- in this
// case it is set to a constant (mergedTombstoneReplicaID, equal to MaxInt32).
//
// DEFINITION (UninitializedStateMachine): this is a Replica with no state
// machine, i.e., there is Raft state and possibly a RangeTombstone. In
// particular, there is no RangeDescriptor and so it has no key span
// associated with it yet. The RangeTombstone.NextReplicaID is <=
// RaftReplicaID.ReplicaID.
// The HardState{Term,Vote} can have arbitrary values since this replica can
// vote. However, it has a zero HardState.Commit and no log entries -- this
// Raft invariant is upheld externally by a combination of mostly external
// invariants:
// A new Range is initialized with all Replicas at truncated index equal to
// RaftInitialLogIndex (10) (so they are in InitializedStateMachine state),
// and any future Replicas will be initialized via a snapshot reflecting a
// nonzero applied index >= 10. In particular, prior to receiving the
// snapshot, no log entries can be sent to the Replica. And etcd/raft only
// communicates Commit entries for which the recipient has the log entry.
//
// Some of the above invariants may be violated when non-durable state is lost
// due to a crash, but ReplicasStorage.Init is required to fix the persistent
// state such that the above invariants are true. These are not exposed to the
// user of the interface. We list these below.
//
// DEFINITION (RecoveryDeletingReplica): a Replica whose Raft state requires a
// nonzero applied index in the state machine, but there is no state machine
// state. This is an intermediate state entered when transitioning from
// InitializedStateMachine to DeletedReplica, after the state machine state
// has been deleted and RangeTombstoneKey updated and before the raft state
// has been deleted. This is distinguishable from UninitializedStateMachine
// since RaftTruncatedState.{Index,Term} are guaranteed to exist and have
// values >= RaftInitialLogIndex, RaftInitialLogTerm. ReplicasStorage.Init
// will transition out of this state into DeletedReplica state.
// The RecoveryDeletingReplica can also occur when removing a replica in state
// UninitializedStateMachine. This is because the RangeTombstone is written
// first to the state machine, and subsequently the raft state is removed.
// This corresponds to the condition RangeTombstone.NextReplicaID >
// RaftReplicaID.ReplicaID.
//
// DEFINITION (RecoveryInconsistentReplica): This is a Replica that mostly
// looks like to be in state InitializedStateMachine, but has suffered
// regression in durable state such that the state machine has advanced past
// HardState.Commit, or a snapshot has been applied and all raft log entries
// are < RaftAppliedIndex, i.e., it violates RaftAndStateConsistency
// invariants. More severely, it can also violate StateConsistency invariants
// by having durably ingested SSTables but not yet updated the
// RaftAppliedIndex to reflect that state machine change. ReplicasStorage.Init
// restores all the invariants needed by an InitializedStateMachine replica,
// by fixing the raft log to be consistent with the state machine, and
// re-applying log entries up to HardState.Commit (except for log entries that
// indicate a split or merge -- see below).
//
// Replica state transitions:
// - Initial state: UninitializedStateMachine
// - Final state: DeletedReplica
// - UninitializedStateMachine => RecoveryDeletingReplica, InitializedStateMachine
// - InitializedStateMachine => RecoveryDeletingReplica, RecoveryInconsistentReplica
// - RecoveryDeletingReplica => DeletedReplica
// - RecoveryInconsistentReplica => InitializedStateMachine
//
// ============================================================================
// Implementation constraints on ReplicasStorage:
// - Splits and Merges typically happen by applying an entry in the raft log.
//   It is feasible for ReplicasStorage.Init to apply such committed entries.
//   However, the logic in such cases can add additional mutations to the
//   batch in the raft log, that have nothing to do with the normal scope of
//   what ReplicasStorage is concerned with. For example, splitPreApply has
//   logic to set RangeAppliedState.RaftClosedTimestamp. For this reason
//   ReplicasStorage ensures durability of split/merge application and does
//   not apply any Split/Merge log entries in ReplicasStorage.Init.
//
// ============================================================================
// Replica Initialization:
//
// Since ReplicasStorage does not permit atomic updates spanning the state
// machine and raft state (even if they are a single engine), replica creation
// is sequenced as (* indicates durable writes):
//
// - [C1*] creation of RaftHardStateKey in raft state with
//   {Term:0,Vote:0,Commit:0}. This is a replica in UninitializedStateMachine
//   state.
// - [C2*] creation of state machine state (via snapshot or some synthesized
//   state for range-ID and range local keys in the case of split).
// - [C3] set RaftTruncatedStateKey with RaftTruncatedState.{Index,Term} equal
//   to RangeAppliedState.{RaftAppliedIndex,RaftAppliedIndexTerm} and adjust
//   value of RaftHardStateKey (specifically HardState.Commit needs to be set
//   to RangeAppliedState.RaftAppliedIndex -- see below for details). Also
//   discard all raft log entries if any (see below). At this point the
//   replica is in InitializedStateMachine state.
//
// Every step above needs to be atomic. Note that we are doing 2 syncs, in
// steps C1 and C2, for the split case, where we currently do 1 sync -- splits
// are not common enough for this to matter. If we did not sync C2 we could
// start adding log entries after C3 but lose the state machine state in the
// case of a crash, which would violate the replica state invariants.
//
// An initialized replica that receives a snapshot because it has lagged
// behind will execute C2 and C3. The C3 step throws away all the existing
// raft log entries. So a precondition for applying such a snapshot is:
// - The raft log does not have entries beyond the snapshot's
//   RangeAppliedState.RaftAppliedIndex. If it did, there would be no benefit
//   in applying this snapshot.
//   The following etcd/raft code
//   https://github.com/etcd-io/etcd/blob/7572a61a39d4eaad596ab8d9364f7df9a84ff4a3/raft/raft.go#L1584-L1589
//   ensures this behavior -- if the raft log entry corresponding to the
//   snapshot is already present locally, it only advances the commit index to
//   the snapshot index, and does not actually apply the snapshot.
// - Corollary: since HardState.Commit cannot refer to log entries beyond the
//   locally persisted ones, the existing HardState.Commit <=
//   RangeAppliedState.RaftAppliedIndex, so the HardState manipulation done in
//   step C3 will only need to increase the value of HardState.Commit.
//
// Why C2 before C3?:
// If we performed step C3 before C2, there is a possibility that a crash
// prevents C2. Now we would need to rollback the change made in C3 to reach a
// fully consistent state on crash recovery. Rolling back HardState.Commit is
// easy, since there is no raft log, we can set it to
// RangeAppliedState.RaftAppliedIndex if it exists, else 0. Similarly, we can
// rollback RaftTruncatedState by either:
// - deleting it if the RangeAppliedState does not exist, which implies C3 did
//   not happen.
// - if RangeAppliedState exists, roll back RaftTruncatedState.{Index,Term} to
//   RangeAppliedState.{RaftAppliedIndex,RaftAppliedIndexTerm}. Note that this
//   is a case where an already initialized lagging replica has a snapshot
//   being applied.
// The correctness problem with doing C3 before C2 is that the store violates
// raft promises it has made earlier. For example, say the state machine had
// applied index 20 and the raft log contained [15, 25), then this store is
// part of the quorum that causes [21, 25) to commit. We receive a snapshot
// for 30, and crash after C3, and since C3 is before C2 in this scenario, we
// rollback to 20 and have no raft state. Therefore, this is in effect an
// unavailable replica, since it no longer has [21, 25).
//
// Rolling forward if crash after C2 and before C3:
// ReplicasStorage.Init will roll forward to C3 when initializing itself.
// - If HardState.Commit < RangeAppliedState.RaftAppliedIndex, update
//   HardState.Commit
// - If RaftTruncatedState does not exist, or RaftTruncatedState.Index <
//   RangeAppliedState.RaftAppliedIndex and all log entries are <=
//   RangeAppliedState.RaftAppliedIndex
//   - Discard all raft log entries.
//   - Set RaftTruncatedState.{Index,Term} using
//     RangeAppliedState.{RaftAppliedIndex,RaftAppliedIndexTerm}
//
// Aside:
// Since we now have RangeAppliesState.RaftAppliedIndexTerm, constructing an
// outgoing snapshot only involves reading state machine state (this is a tiny
// bit related to #72222, in that we are also assuming here that the outgoing
// snapshot is constructed purely by reading state machine engine state).
// TODO(sumeer): we can do this on master since RaftAppliedIndexTerm was
// introduced in 22.1.
//
// ============================================================================
// Replica Deletion:
//
// Replica deletion is sequenced as the following steps (* indicates durable
// writes):
//
// - [D1*] deletion of state machine state (iff the replica is in state
//   InitializedStateMachine) and write to the RangeTombstoneKey. If prior to
//   this step the range was in state InitializedStateMachine, it is now in
//   state RecoveryDeletingReplica. If it was in state UninitializedStateMachine,
//   again it is now in the state RecoveryDeletingReplica.
//   This latter case can occur for various reasons: one cause is this range
//   is the RHS of a split where the split has not yet happened, but we've
//   created an uninitialized RHS. So we can't delete the state machine state
//   for the RHS since it doesn't exist yet (there is some replicated state in
//   the state machine that could in the future belong to the RHS, but not
//   yet, and we don't know the span of that future RHS either). By updating
//   the RangeTombstone, when the split occurs, D1 will be repeated.
// - [D2] deletion of all Raft state for this RangeID, i.e., RaftHardStateKey,
//   RaftTruncatedStateKey, log entries, RangeLastReplicaGCTimestampKey.
//
// Every step above needs to be atomic. One of the reasons to sync after D1 it
// that we could later execute C1 when adding the range back to this store, and
// then crash. On crash recovery we'd find the raft HardState and old state
// machine state and incorrectly think this is an initialized replica.
//
// The merge operation alters step D1 to only write the RangeTombstoneKey and
// delete the range-id local keys in the RHS.
//
// Note that we don't delete the RangeTombstoneKey even when the range itself
// is being deleted (due to a merge). The replay protection offered by it is
// more important than the minuscule cost of leaking a RangeTombstoneKey per
// range. It is possible to have some cleanup of RangeTombstoneKeys for long
// dead ranges, but it is outside of the scope here.
//
// A crash after D1 will result in a replica in state RecoveryDeletingReplica.
// ReplicasStorage.Init will execute D2. See also
// https://github.com/cockroachdb/cockroach/issues/73424 which presumably
// deals with cleaning up UninitializedStateMachine replicas in the absence of
// a crash.
//
// ============================================================================
// Normal Replica Operation:
//
// - ReplicasStorage is used to append/replace log entries and update
//   HardState. This is done via a RaftMutationBatch. There is a
//   RaftMutationBatch.MustSync that the caller uses to specify the minimal
//   sync requirements imposed by Raft (ReplicasStorage is not in the business
//   of understanding Raft correctness requirements). Typically MustSync will
//   be true only if entries are appended, or a vote and/or term change has to
//   be recorded. In particular, a change solely to HardState.Commit would
//   have MustSync=false. See
//   https://github.com/etcd-io/etcd/blob/7572a61a39d4eaad596ab8d9364f7df9a84ff4a3/raft/node.go#L584-L593.
//   Note that this means that HardState.Commit can regress and become less
//   than RangeAppliedState.RaftAppliedIndex in case of a crash. We will fix
//   this in ReplicasStorage.Init, as discussed later.
//
// - The caller keeps track of HardState.Commit, since it constructed
//   HardState for the RaftMutationBatch. It applies committed entries to the
//   state machine using ApplyCommittedUsingIngest and ApplyCommittedBatch.
//   The ApplyCommitted* methods should not be used for log entries that are
//   performing splits or merges -- the caller should do those by calling
//   SplitReplica and MergeReplicas. ReplicasStorage decides when it is
//   necessary to sync -- ApplyCommitted* will not sync the state machine, and
//   SplitReplica/MergeReplicas will sync the state machine. Note that the
//   caller may not need to read a raft entry from ReplicasStorage in order to
//   apply it, if it happens to have stashed it somewhere in its in-memory
//   data-structures.
//
//   For log entries that are ingesting side-loaded files, the application of
//   a single entry is split into a pair, ApplyCommittedUsingIngest, that
//   usually does not update the RaftAppliedIndex and then ApplyCommittedBatch
//   which updates the RaftAppliedIndex. A crash after the first and before
//   the second leaves the state machine in an inconsistent state
//   (RecoveryInconsistentReplica) which needs to be fixed by
//   ReplicasStorage.Init. For this reason, ReplicasStorage keeps track of the
//   highest HardState.Commit known to be durable, and requires
//   ApplyCommittedUsingIngest to provide the highestRaftIndex of the changes
//   included in the files. ReplicasStorage will sync the raft state if
//   needed, to make the highestRaftIndex durable, before ingesting these
//   files. This prevents regression of HardState.Commit past an index that
//   contains side-loaded files. Note that this assumes that
//   ReplicasStorage.Init has the capability of applying all raft log entries
//   except for splits and merges (we've already mentioned that splits/merges
//   are made durable at application time).
//
// - Log truncation is done by the caller, based on various signals relevant
//   to the proper functioning of the distributed raft group. The truncation
//   is notified via RangeStorage.TruncatedRaftLog. This is breaking the
//   abstraction, in that the caller has changed the raft log and removed
//   sideloaded entries without going through RangeStorage, so we should see
//   if we can do better here. The current log truncation methods are:
//   - For strongly-coupled truncation (with a single engine): the truncation
//     happens when applying to the state machine (ApplyCommittedBatch) and we
//     don't want to leak this information via the MutationBatch that is
//     supposed to only touch the state machine. See more details below.
//   - For loosely-coupled truncation (single engine or separate engines): the
//     truncation happens in raftLogTruncator.tryEnactTruncations which is
//     mutating only the raft log. For this case we could keep all the
//     business logic related to deciding what to truncate (which requires
//     interaction with the Replica object) in raftLogTruncator, while giving
//     RangeStorage the actual batch (with additional information on what is
//     being truncated) to commit.
//   As of https://github.com/cockroachdb/cockroach/pull/80193 the
//   loosely-coupled raft log truncation is disabled due to a performance
//   regression in write-heavy workloads (see comment
//   https://github.com/cockroachdb/cockroach/issues/78412#issuecomment-1119922463
//   for conclusion of investigation).
//
//   TODO(sumeer): the following "revised plan" comment is from Sep 2022 and
//   probably stale.
//
//   The revised plan is to
//   - Do strongly-coupled truncation in
//     CanTruncateRaftIfStateMachineIsDurable for a ReplicasStorage
//     implementation that shares the same engine for the state machine and
//     raft state. This relies on external code structure for correctness: the
//     truncation proposal flows through raft, so we have already applied the
//     state machine changes for the preceding entries. A crash will cause a
//     suffix of the unsynced changes to be lost, so we cannot lose the state
//     machine changes while not losing the truncation.
//
//     This is the similar to the correctness argument that the code preceding
//     ReplicasStorage relies on. The separation of the RaftMutationBatch
//     provided to DoRaftMutation and the MutationBatch provided to
//     ApplyCommittedBatch is only more formalization of the separation that
//     already exists: handleRaftReadyRaftMuLocked makes raft changes with one
//     batch, and replicaAppBatch.ApplyToStateMachine is used to make changes
//     to the state machine with another batch.
//     replicaAppBatch.ApplyToStateMachine also does the raft log truncation,
//     and raft changes for splits and merges, which ReplicasStorage is doing
//     in a different way, but it does not change the correctness claim. Note
//     that #38566, a flaw in this correctness argument, has since been fixed.
//
//   - Do loosely-coupled truncation for a ReplicasStorage implementation that
//     has different engines for the state machine and raft state. The
//     experiments in
//     https://github.com/cockroachdb/cockroach/issues/16624#issuecomment-1137394935
//     have demonstrated that we do not have a performance regression. We
//     speculate that the absence of performance regression is due to:
//     - With multiple key-value pairs in a batch, the memtable for the raft
//       engine will be able to store more than the corresponding state
//       machine memtable where the key-value pairs get individual entries in
//       the memtable. This is because of the per-entry overhead. This means
//       there is a decent probability that the state machine memtable will
//       start getting flushed before the corresponding raft engine memtable
//       is flushed. If the flush is fast enough, we would be able to truncate
//       the raft log before the raft log entries are flushed.
//     - The smaller raft engine will have a higher likelihood that deletes
//       due to truncation get flushed to L0 while the log entry being deleted
//       is also in L0. This should reduce the likelihood of wasteful
//       compaction of raft log entries to lower levels.
//
// - Range merges impose an additional requirement: the merge protocol (at a
//   higher layer) needs the RHS replica of a merge to have applied all raft
//   entries up to a specified index and that this application is durable. To
//   ensure the durability we expose a SyncStateMachine method for the higher
//   layer.
//
// ============================================================================
// Crash Recovery:
// ReplicasStorage needs to be self contained in the sense that it must be
// able to execute state changes to reach a fully consistent state without
// needing any external input, as part of its initialization. Init will block
// until all the raft and state machine states have been made mutually
// consistent.
// - Iterate over RaftHardStateKeys and identify a set of replicas R_r. This
//   is efficiently done by seeking to the current RangeID+1.
// - Iterate over RangeDescriptorKeys and identify a set of replicas R_s. This
//   is efficiently done by using the latest non-provisional RangeDescriptor
//   (replica-descriptor) of the current range and then seeking to the end key
//   of the range's span.
//   - Note that this way of skipping spans will ensure that we will not find
//     RangeDescriptors that have overlapping spans, which is ideally an
//     invariant we should verify. Instead of verifying that invariant, which
//     is expensive, we additionally iterate over all the
//     RangeAppliedStateKeys, which are Range-ID local keys -- this iteration
//     can be accomplished by seeking using current RangeID+1. If we find
//     RangeAppliedStateKeys whose RangeID is not mentioned in a corresponding
//     RangeDescriptor we have an invariant violation.
// - The set R_s - R_r must be empty, i.e., R_s is a subset of R_r.
// - The set R_r - R_s are replicas are either in state
//   UninitializedStateMachine or RecoveryDeletingReplica.
// - Remove RecoveryDeletingReplica replicas by transitioning them to DeletedReplica
//   by executing D2.
// - The set R_s are replicas that ought to be in state
//   InitializedStateMachine, though may have been in the middle of that state
//   transition, or become inconsistent for other reasons mentioned earlier.
//   That is, they are potentially in RecoveryInconsistentReplica state.
//   - If RangeAppliedState.RaftAppliedIndex > HardState.Commit (NB: HardState
//     must exist), execute the following atomically:
//     - If there are no log entries or all log entries are <
//       RaftAppliedIndex: remove all log entries and set
//       RaftTruncatedState.{Index,Term} equal to
//       RangeAppliedState.{RaftAppliedIndex,RaftAppliedIndexTerm}.
//     - Set HardState.Commit to RaftAppliedIndex.
//     These steps handle (a) crash in the middle of replica creation (doing
//     step C3), and (b) regression of HardState.Commit under normal replica
//     operation. The RaftAndStateConsistency invariant now holds.
//   - The StateConsistency invariant may not hold. To ensure that it holds:
//     for ranges whose RangeAppliedState.RaftAppliedIndex < HardState.Commit,
//     apply log entries, including those that remove this replica, until one
//     encounters a log entry that is performing a split or merge.
// - InitializedStateMachine replicas:
//   - using the replica-descriptors, check that the spans do not overlap.
//   - This InterReplicaStateConsistency invariant must also hold before we
//     fixed the RecoveryInconsistentReplicas, so we could additionally check
//     it then.
// ============================================================================

// ReplicaState represents the current state of a range replica in this store.
type ReplicaState int

const (
	// UninitializedStateMachine is a replica with raft state but no state
	// machine.
	UninitializedStateMachine ReplicaState = iota
	// InitializedStateMachine is a replica with both raft state and state
	// machine.
	InitializedStateMachine
	// DeletedReplica is a replica with neither raft state or state machine.
	DeletedReplica
)

// FullReplicaID is a fully-qualified replica ID.
type FullReplicaID struct {
	// RangeID is the id of the range.
	RangeID roachpb.RangeID
	// ReplicaID is the id of the replica.
	ReplicaID roachpb.ReplicaID
}

// SafeFormat implements redact.SafeFormatter. It prints as
// r<rangeID>/<replicaID>.
func (id FullReplicaID) SafeFormat(s interfaces.SafePrinter, _ rune) {
	s.Printf("r%d/%d", id.RangeID, id.ReplicaID)
}

// String formats a store for debug output.
func (id FullReplicaID) String() string {
	return redact.StringWithoutMarkers(id)
}

// ReplicaInfo provides the replica ID and state pair.
type ReplicaInfo struct {
	FullReplicaID
	// State of the replica.
	State ReplicaState
}

// MutationBatch can be committed to the underlying engine. Additionally, it
// provides access to the underlying Batch. In some cases the implementation
// of ReplicasStorage will add additional mutations before committing. We
// expect the caller to know which engine to construct a batch from, in order
// to update the state machine or the raft state. ReplicasStorage does not
// hide such details since we expect the caller to mostly do reads using the
// engine Reader interface.
//
// TODO(sumeer): there are some operations that need to use the storage.Batch
// as a Reader. Be clear on when a storage.Batch can be read from, and whether
// it includes the changes in the batch or is reading from the underlying DB
// (see the code in pebbleBatch that selects between the two). Ideally, the
// supported semantics should be specified via an interface method on
// storage.Batch, and ReplicasStorage can check that the semantics are what it
// expects.
type MutationBatch interface {
	// Commit writes the mutation to the engine.
	Commit(sync bool) error
	// Batch returns the underlying storage.Batch.
	Batch() Batch
}

// RaftMutationBatch specifies mutations to the raft log entries and/or
// HardState.
type RaftMutationBatch struct {
	// MutationBatch should be created using NewUnindexedBatch(false) so it
	// can be read from, but the reads will read the underlying engine. It is
	// important for the reads to not see the state in the batch, since reading
	// the underlying engine is used to clear stale raft log entries that are
	// getting overwritten by this batch.
	MutationBatch
	// [Lo, Hi) represents the raft log entries, if any in the MutationBatch.
	// This is appending/overwriting entries in the raft log. That is, if the
	// log is [a,b,c,d], with a at index 12 and one appends e at index 13, the
	// result will be [a,e]. Note that the MutationBatch only contains the write
	// at index 13, and the removal of indices 14, 15 is done by the callee. The
	// callee assumes that the entries to remove are from Term-1. We assume the
	// caller is upholding Raft semantics (such as not overwriting raft log
	// entries that have been committed) -- ReplicasStorage is not in the
	// business of validating that such semantics are being upheld.
	Lo, Hi uint64
	// Term represents the term of those entries.
	Term uint64
	// HardState, if non-nil, specifies the HardState value being set by
	// MutationBatch.
	HardState *raftpb.HardState
	// MustSync is set to true if the mutation must be synced.
	MustSync bool
}

// RangeStorage is a handle for a FullReplicaID that provides the ability to
// write to the raft state and state machine state. This could have been named
// ReplicaStorage, but that sounds too similar to ReplicasStorage. Note that,
// even though a caller can have two different RangeStorage handles for the
// same range, if the range has been added and removed and so has different
// ReplicaIDs, at most one of them will be in state != DeletedReplica.
//
// Other than the FullReplicaID() method, the methods access mutable state,
// and so may not execute concurrently.
type RangeStorage interface {
	// FullReplicaID returns the FullReplicaID of this replica.
	FullReplicaID() FullReplicaID
	// State returns the ReplicaState of this replica.
	State() ReplicaState

	// CurrentRaftEntriesRange returns [lo, hi) representing the locally stored
	// raft entries. These are guaranteed to be locally durable.
	CurrentRaftEntriesRange(ctx context.Context) (lo uint64, hi uint64, err error)

	// GetHardState returns the current HardState. HardState.Commit is not
	// guaranteed to be durable.
	GetHardState(ctx context.Context) (raftpb.HardState, error)

	// TruncatedRaftLog provides the inclusive index up to which truncation has
	// been done.
	TruncatedRaftLog(index uint64)

	// DoRaftMutation changes the raft state. This will also purge sideloaded
	// files if any entries are being removed.
	// REQUIRES: if rBatch.Lo < rBatch.Hi, the range is in state
	// InitializedStateMachine.
	DoRaftMutation(ctx context.Context, rBatch RaftMutationBatch) error

	// TODO(sumeer):
	// - add raft log read methods.
	// - what raft log stats do we need to maintain and expose (raftLogSize?)?
	//   We could accept a callback with a truncated index parameter that
	//   RangeStorage invokes whenever it truncates entries, and let the caller
	//   maintain the size.

	// State machine commands.

	// IngestRangeSnapshot ingests a snapshot for the range.
	// - The replica-descriptor in the snapshot describes the range as equal to
	//   span.
	// - The snapshot corresponds to application of the log up to
	//   raftAppliedIndex.
	// - sstPaths represent the ssts for this snapshot, and do not include
	//   anything other than state machine state and do not contain any keys
	//   outside span (after accounting for range and replicated range-ID local
	//   keys), or Range-ID keys for other ranges.
	// - sstPaths include a RANGEDEL that will clear all the existing state
	//   machine state in the store for span (including range-id and range local
	//   keys) "before" adding the snapshot state (see below for additional
	//   RANGEDELs that may be added by ReplicasStorage if the previous span for
	//   this replica was wider).
	// NB: the ssts contain RangeAppliedState, RangeDescriptor (including
	// possibly a provisional RangeDescriptor). Ingestion is the only way to
	// initialize a range except for the RHS of a split.
	//
	// Snapshot ingestion will not be accepted in the following cases:
	// - span overlaps with the (span in the) replica-descriptor of another
	//   range, unless the range is listed in subsumedRangeIDs. The ranges
	//   listed in subsumedRangeIDs must have spans that at least partially
	//   overlap with span.
	//   TODO(sumeer): copy the partial overlap example documented in
	//   replica_raftstorage.go clearSubsumedReplicaDiskData.
	//   The span of a range can change only via IngestRangeSnapshot,
	//   SplitReplica, MergeRange, so ReplicasStorage can keep track of all
	//   spans without resorting to repeated reading from the engine.
	// - the raft log already has entries beyond the snapshot (this is an
	//   invariant that Raft is already expected to maintain, so it is not
	//   an expected error).
	//
	// For reference, ReplicasStorage will do:
	// - If this replica is already initialized compute
	//   rhsSpan = current span - span provided in this call.
	//   rhsSpan is non-empty if we are moving the LHS past a split using a
	//   snapshot. In this case any replica(s) corresponding to rhsSpan cannot
	//   possibly be in InitializedStateMachine state (since it would be a
	//   violation of spans being non-overlapping). That is, they may be
	//   - participating in the raft group(s) for the RHS, but will not have any
	//     log entries.
	//   - rebalanced away.
	//   In either case, it is safe to clear all range local and global keys for
	//   the rhsSpan. ssts will be added to clear this state. Note, that those
	//   uninitialized ranges cannot have any replicated range-ID local keys.
	//   They may have a RangeTombstoneKey, but it is not something this method
	//   needs to touch.
	// - Add additional ssts that clear the replicated Range-ID keys for the
	//   subsumed ranges, set the RangeTombstone to mergedTombstoneReplicaID,
	//   and the non-overlapping replicated range key spans for the subsumed
	//   range.
	// - Atomically ingest the ssts. This does step C2 for this range and D1 for
	//   all subsumed ranges. This is durable. A crash after this step and
	//   before the next step is rolled forward in ReplicasStorage.Init.
	// - Do steps C3 for this range and steps D2 for the subsumed ranges.
	//
	// In handleRaftReadyRaftMuLocked, if there is a snapshot, it will first
	// call IngestRangeSnapshot, and then DoRaftMutation to change the
	// HardState.{Term,Vote,Commit}. Note that the etcd/raft logic fast forwards
	// the HardState.Commit to the snapshot index, so the DoRaftMutation will
	// not actually change the stored value of HardState.Commit from what was
	// already set in IngestRangeSnapshot.
	IngestRangeSnapshot(
		ctx context.Context, span roachpb.RSpan, raftAppliedIndex uint64,
		sstPaths []string, subsumedRangeIDs []roachpb.RangeID,
		sstScratch struct{} /* TODO(sumeer): kvserver.SSTSnapshotStorageScratch */) error

	// ApplyCommittedUsingIngest applies committed changes to the state machine
	// state by ingesting sstPaths. highestRaftIndex is the highest index whose
	// changes are included in the sstPaths. This is due to "sideloaded sst"
	// raft log entries. These ssts do not contain an update to
	// RangeAppliedState, so this call must be immediately followed by a call to
	// ApplyCommittedBatch that does update the RangeAppliedState.
	// It is possible for the node containing this store to crash prior to that
	// call to ApplyCommittedBatch -- this is ok since ReplicasStorage.Init will
	// replay this idempotent ingest and the following ApplyCommittedBatch.
	// REQUIRES: replica is in state InitializedStateMachine.
	ApplyCommittedUsingIngest(
		ctx context.Context, sstPaths []string, highestRaftIndex uint64) error

	// ApplyCommittedBatch applies committed changes to the state machine state.
	// Does not sync. Do not use this for applying raft log entries that perform
	// split, merge, or remove this replica (due to rebalancing) -- see the
	// methods in ReplicasStorage that accomplish that.
	// REQUIRES: replica is in state InitializedStateMachine (this is because we
	// create a new range with the first log entry at RaftInitialLogIndex (10),
	// so a range always requires an initial state "snapshot" before it can
	// apply raft entries).
	ApplyCommittedBatch(smBatch MutationBatch) error

	// SyncStateMachine is for use by higher-level code that needs to ensure
	// durability of the RHS of a merge. It simply syncs the state machine state
	// to ensure all previous mutations are durable.
	// REQUIRES: replica is in state InitializedStateMachine.
	SyncStateMachine(ctx context.Context) error
}

// ReplicasStorage provides an interface to manage the persistent state of a
// store that includes the lifecycle of a range replica, its raft log, and the
// state machine state. See the comment at the top of the file.
type ReplicasStorage interface {
	// Init will block until all the raft and state machine states have been
	// made consistent.
	Init(ctx context.Context) error

	// CurrentRanges returns the replicas in the store. It does not return any
	// ranges with state DeletedReplica, since it has no knowledge of them.
	CurrentRanges() []ReplicaInfo

	// GetRangeTombstone returns the nextReplicaID in the range tombstone for
	// the range, if any.
	GetRangeTombstone(
		ctx context.Context, rangeID roachpb.RangeID) (nextReplicaID roachpb.ReplicaID, err error)

	// GetHandle returns a handle for a range listed in CurrentRanges().
	// ReplicasStorage will return the same handle object for a FullReplicaID
	// during its lifetime. Once the FullReplicaID transitions to DeletedReplica
	// state, ReplicasStorage will forget the RangeStorage handle and it is up
	// to the caller to decide when to throw away a handle it may be holding
	// (the handle is not really usable for doing anything once the range is
	// deleted).
	GetHandle(rr FullReplicaID) (RangeStorage, error)

	// CreateUninitializedRange is used when rebalancing is used to add a range
	// to this store, or a peer informs this store that it has a replica of a
	// range. This is the first step in creating a raft group for this
	// FullReplicaID. It will return an error if:
	// - This ReplicaID is too old based on the RangeTombstone.NextReplicaID
	// - There already exists some state under any raft key for this range.
	//
	// The call will cause HardState to be initialized to
	// {Term:0,Vote:0,Commit:0}.
	//
	// Typically there will be no state machine state for this range. However it
	// is possible that a split is delayed and some other store has informed this
	// store about the RHS of the split, in which case part of the state machine
	// (except for the Range-ID keys, RangeDescriptor) already exist. Note that
	// this locally lagging split case is one where the RHS does not transition
	// to initialized via anything other than a call to SplitReplica (i.e., does
	// not apply a snapshot), except when the LHS moves past the split using a
	// snapshot, in which case the RHS can also then apply a snapshot.
	CreateUninitializedRange(ctx context.Context, rr FullReplicaID) (RangeStorage, error)

	// SplitReplica is called to split range r into a LHS and RHS, where the RHS
	// is represented by rhsRR. The smBatch specifies the state machine state to
	// modify for the LHS and RHS. For the RHS, the smBatch must be constructing
	// the appropriate range-ID local state and range local state that doesn't
	// already exist in the store (including the RangeDescriptor). rhsSpan is
	// the span in the RangeDescriptor for the RHS. The following cases can
	// occur:
	//
	// - [A1] RangeTombstone for the RHS indicates rhsRR.ReplicaID has already
	//   been removed. Two subcases:
	//   - [A11] There exists a HardState for rhsRR.RangeID: the range has been
	//     added back with a new ReplicaID.
	//   - [A12] There exists no HardState, so rhsRR.RangeID should not exist on
	//     this store.
	// - [A2] RangeTombstone for the RHS indicates that rhsRR.ReplicaID has not
	//   been removed.
	//
	// For A11 and A12, the smBatch will be altered to clear all state in the
	// state machine for the RHS. The final state RHS will be in for A11 is
	// UninitializedStateMachine, for A12 is DeletedReplica. For A2, the smBatch
	// is not altered and the final RHS state is InitializedStateMachine. If the
	// final RHS state is DeletedReplica, a nil RangeStorage will be returned.
	// The application of smBatch is synced.
	//
	// From our earlier discussion of replica creation and deletion.
	// - For case A2, the callee will perform step C1 if needed, then commit
	//   smBatch (step C2), and then perform step C3.
	// - For case A11 there is no need to do step C1. Steps C2 and C3 cannot be
	//   performed since the RHS ReplicaID has changed and the state here is
	//   stale. All we are doing is cleaning up the state machine state for the
	//   RHS when committing smBatch. The callee is doing step D1 of deletion,
	//   of the RHS for the old replicaID.
	// - For case A12, the callee is doing step D1 of deletion, by altering and
	//   committing smBatch. Since the RHS range never transitioned to
	//   initialized (it never had a RangeDescriptor), the deletion was unable
	//   to execute D1 when the HardState etc. was being deleted (it only
	//   executed D2). The RHS will continue to be in DeletedReplica state when
	//   the method returns.
	//
	// REQUIRES: The range being split is in state InitializedStateMachine, and
	// RHS either does not exist or is in state UninitializedStateMachine.
	//
	// Called below Raft -- this is being called when the split transaction commits.
	SplitReplica(
		ctx context.Context, r RangeStorage, rhsRR FullReplicaID, rhsSpan roachpb.RSpan,
		smBatch MutationBatch,
	) (RangeStorage, error)

	// MergeReplicas is called to merge two range replicas. smBatch contains
	// changes to the LHS state machine to incorporate the state of the RHS, and
	// the intent resolution of the RHS RangeDescriptor.
	//
	// It will perform the following steps:
	// - Alter smBatch to remove all Range-ID local keys in the RHS and write the
	//   RangeTombstone to the RHS with value mergedTombstoneReplicaID.
	//
	// - Apply and sync smBatch, which transforms the LHS into the merged range,
	//   and performs step D1 for the RHS. The sync ensures that a crash after
	//   this step and before the next step will be rolled forward in Init.
	//
	// - Do step D2 for the RHS.
	//
	// REQUIRES: LHS and RHS are in state InitializedStateMachine, and RHS has
	// durably applied all commands up to the merge.
	//
	// Code above this layer ensures the above durability of application of all
	// commands in the RHS and additionally ensures that the RHS of a merge is
	// immovable once in the critical phase (i.e. past the SubsumeRequest is
	// handled), until the merge txn aborts (if it ever does). On the
	// leaseholder handling Subsume, this is done by the Subsume. But we also
	// prevent all future leaseholders from doing anything that would violate
	// the critical phase by observing the deletion intent on the range
	// descriptor. If a merge transaction commits, regardless of which replicas
	// know about this yet, the LHS and RHS will be fully colocated.
	//
	// Called below Raft -- this is being called when the merge transaction commits.
	MergeReplicas(
		ctx context.Context, lhsRS RangeStorage, rhsRS RangeStorage, smBatch MutationBatch) error

	// DiscardReplica is called to discard a replica that has been rebalanced
	// away. The replica is either in UninitializedStateMachine or
	// InitializedStateMachine state. There are multiple reasons for this to be
	// called, such as the raft log entry that removes the replica is being
	// applied, or ReplicaGCQueue notices that the replica is too old. Due to
	// these multiple callers, ReplicasStorage is not in a position to compute
	// what the nextReplicaID for the RangeTombstone should be. Therefore, it
	// expects the caller to provide that value as a parameter.
	DiscardReplica(
		ctx context.Context, r RangeStorage, nextReplicaID roachpb.ReplicaID) error
}

type sideloadedStorageConstructor func(
	rangeID roachpb.RangeID, replicaID roachpb.ReplicaID) struct{} /* TODO(sumeer): SideloadStorage */

// MakeSingleEngineReplicasStorage constructs a ReplicasStorage where the same
// Engine contains the raft log and the state machine.
func MakeSingleEngineReplicasStorage(
	nodeID roachpb.NodeID,
	storeID roachpb.StoreID,
	eng Engine,
	ssConstructor sideloadedStorageConstructor,
	st *cluster.Settings,
) ReplicasStorage {
	// TODO(sumeer): implement
	return nil
}
