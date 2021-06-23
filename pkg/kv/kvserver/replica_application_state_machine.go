// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/apply"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/kr/pretty"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// replica_application_*.go files provide concrete implementations of
// the interfaces defined in the storage/apply package:
//
// replica_application_state_machine.go  ->  apply.StateMachine
// replica_application_decoder.go        ->  apply.Decoder
// replica_application_cmd.go            ->  apply.Command         (and variants)
// replica_application_cmd_buf.go        ->  apply.CommandIterator (and variants)
// replica_application_cmd_buf.go        ->  apply.CommandList     (and variants)
//
// These allow Replica to interface with the storage/apply package.

// applyCommittedEntriesStats returns stats about what happened during the
// application of a set of raft entries.
//
// TODO(ajwerner): add metrics to go with these stats.
type applyCommittedEntriesStats struct {
	batchesProcessed     int
	entriesProcessed     int
	stateAssertions      int
	numEmptyEntries      int
	numConfChangeEntries int
}

// nonDeterministicFailure is an error type that indicates that a state machine
// transition failed due to an unexpected error. Failure to perform a state
// transition is a form of non-determinism, so it can't be permitted for any
// reason during the application phase of state machine replication. The only
// acceptable recourse is to signal that the replica has become corrupted.
//
// All errors returned by replicaDecoder and replicaStateMachine will be instances
// of this type.
type nonDeterministicFailure struct {
	wrapped  error
	safeExpl string
}

// The provided format string should be safe for reporting.
func makeNonDeterministicFailure(format string, args ...interface{}) error {
	err := errors.AssertionFailedWithDepthf(1, format, args...)
	return &nonDeterministicFailure{
		wrapped:  err,
		safeExpl: err.Error(),
	}
}

// The provided msg should be safe for reporting.
func wrapWithNonDeterministicFailure(err error, format string, args ...interface{}) error {
	return &nonDeterministicFailure{
		wrapped:  errors.Wrapf(err, format, args...),
		safeExpl: fmt.Sprintf(format, args...),
	}
}

// Error implements the error interface.
func (e *nonDeterministicFailure) Error() string {
	return fmt.Sprintf("non-deterministic failure: %s", e.wrapped.Error())
}

// Cause implements the github.com/pkg/errors.causer interface.
func (e *nonDeterministicFailure) Cause() error { return e.wrapped }

// Unwrap implements the github.com/golang/xerrors.Wrapper interface, which is
// planned to be moved to the stdlib in go 1.13.
func (e *nonDeterministicFailure) Unwrap() error { return e.wrapped }

// replicaStateMachine implements the apply.StateMachine interface.
//
// The structure coordinates state transitions within the Replica state machine
// due to the application of replicated commands decoded from committed raft
// entries. Commands are applied to the state machine in a multi-stage process
// whereby individual commands are prepared for application relative to the
// current view of ReplicaState and staged in a replicaAppBatch, the batch is
// committed to the Replica's storage engine atomically, and finally the
// side-effects of each command is applied to the Replica's in-memory state.
type replicaStateMachine struct {
	r *Replica
	// batch is returned from NewBatch(false /* ephemeral */).
	batch replicaAppBatch
	// ephemeralBatch is returned from NewBatch(true /* ephemeral */).
	ephemeralBatch ephemeralReplicaAppBatch
	// stats are updated during command application and reset by moveStats.
	stats applyCommittedEntriesStats
}

// getStateMachine returns the Replica's apply.StateMachine. The Replica's
// raftMu is held for the entire lifetime of the replicaStateMachine.
func (r *Replica) getStateMachine() *replicaStateMachine {
	sm := &r.raftMu.stateMachine
	sm.r = r
	return sm
}

// shouldApplyCommand determines whether or not a command should be applied to
// the replicated state machine after it has been committed to the Raft log. It
// then sets the provided command's leaseIndex, proposalRetry, and forcedErr
// fields and returns whether command should be applied or rejected.
func (r *Replica) shouldApplyCommand(
	ctx context.Context, cmd *replicatedCmd, replicaState *kvserverpb.ReplicaState,
) bool {
	cmd.leaseIndex, cmd.proposalRetry, cmd.forcedErr = checkForcedErr(
		ctx, cmd.idKey, &cmd.raftCmd, cmd.IsLocal(), replicaState,
	)
	if filter := r.store.cfg.TestingKnobs.TestingApplyFilter; cmd.forcedErr == nil && filter != nil {
		args := kvserverbase.ApplyFilterArgs{
			CmdID:                cmd.idKey,
			ReplicatedEvalResult: *cmd.replicatedResult(),
			StoreID:              r.store.StoreID(),
			RangeID:              r.RangeID,
		}
		if cmd.IsLocal() {
			args.Req = cmd.proposal.Request
		}
		var newPropRetry int
		newPropRetry, cmd.forcedErr = filter(args)
		if cmd.proposalRetry == 0 {
			cmd.proposalRetry = proposalReevaluationReason(newPropRetry)
		}
	}
	return cmd.forcedErr == nil
}

// noopOnEmptyRaftCommandErr is returned from checkForcedErr when an empty raft
// command is received. See the comment near its use.
var noopOnEmptyRaftCommandErr = roachpb.NewErrorf("no-op on empty Raft entry")

// checkForcedErr determines whether or not a command should be applied to the
// replicated state machine after it has been committed to the Raft log. This
// decision is deterministic on all replicas, such that a command that is
// rejected "beneath raft" on one replica will be rejected "beneath raft" on
// all replicas.
//
// The decision about whether or not to apply a command is a combination of
// three checks:
//  1. verify that the command was proposed under the current lease. This is
//     determined using the proposal's ProposerLeaseSequence.
//  2. verify that the command hasn't been re-ordered with other commands that
//     were proposed after it and which already applied. This is determined
//     using the proposal's MaxLeaseIndex.
//  3. verify that the command isn't in violation of the Range's current
//     garbage collection threshold. This is determined using the proposal's
//     Timestamp.
//
// TODO(nvanbenschoten): Unit test this function now that it is stateless.
func checkForcedErr(
	ctx context.Context,
	idKey kvserverbase.CmdIDKey,
	raftCmd *kvserverpb.RaftCommand,
	isLocal bool,
	replicaState *kvserverpb.ReplicaState,
) (uint64, proposalReevaluationReason, *roachpb.Error) {
	leaseIndex := replicaState.LeaseAppliedIndex
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
		return leaseIndex, proposalNoReevaluation, noopOnEmptyRaftCommandErr
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
		leaseMismatch = !raftCmd.DeprecatedProposerLease.Equivalent(*replicaState.Lease)
	} else {
		leaseMismatch = raftCmd.ProposerLeaseSequence != replicaState.Lease.Sequence
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
			if replicaState.Lease.Sequence == requestedLease.Sequence {
				// It is only possible for this to fail when expiration-based
				// lease extensions are proposed concurrently.
				leaseMismatch = !replicaState.Lease.Equivalent(requestedLease)
			}

			// This is a check to see if the lease we proposed this lease request against is the same
			// lease that we're trying to update. We need to check proposal timestamps because
			// extensions don't increment sequence numbers. Without this check a lease could
			// be extended and then another lease proposed against the original lease would
			// be applied over the extension.
			if raftCmd.ReplicatedEvalResult.PrevLeaseProposal != nil &&
				(*raftCmd.ReplicatedEvalResult.PrevLeaseProposal != *replicaState.Lease.ProposedTS) {
				leaseMismatch = true
			}
		}
	}
	if leaseMismatch {
		log.VEventf(
			ctx, 1,
			"command with lease #%d incompatible to %v",
			raftCmd.ProposerLeaseSequence, *replicaState.Lease,
		)
		if isLeaseRequest {
			// For lease requests we return a special error that
			// redirectOnOrAcquireLease() understands. Note that these
			// requests don't go through the DistSender.
			return leaseIndex, proposalNoReevaluation, roachpb.NewError(&roachpb.LeaseRejectedError{
				Existing:  *replicaState.Lease,
				Requested: requestedLease,
				Message:   "proposed under invalid lease",
			})
		}
		// We return a NotLeaseHolderError so that the DistSender retries.
		// NB: we set proposerStoreID to 0 because we don't know who proposed the
		// Raft command. This is ok, as this is only used for debug information.
		nlhe := newNotLeaseHolderError(
			*replicaState.Lease, 0 /* proposerStoreID */, replicaState.Desc,
			fmt.Sprintf(
				"stale proposal: command was proposed under lease #%d but is being applied "+
					"under lease: %s", raftCmd.ProposerLeaseSequence, replicaState.Lease))
		return leaseIndex, proposalNoReevaluation, roachpb.NewError(nlhe)
	}

	if isLeaseRequest {
		// Lease commands are ignored by the counter (and their MaxLeaseIndex is ignored). This
		// makes sense since lease commands are proposed by anyone, so we can't expect a coherent
		// MaxLeaseIndex. Also, lease proposals are often replayed, so not making them update the
		// counter makes sense from a testing perspective.
		//
		// However, leases get special vetting to make sure we don't give one to a replica that was
		// since removed (see #15385 and a comment in redirectOnOrAcquireLease).
		if _, ok := replicaState.Desc.GetReplicaDescriptor(requestedLease.Replica.StoreID); !ok {
			return leaseIndex, proposalNoReevaluation, roachpb.NewError(&roachpb.LeaseRejectedError{
				Existing:  *replicaState.Lease,
				Requested: requestedLease,
				Message:   "replica not part of range",
			})
		}
	} else if replicaState.LeaseAppliedIndex < raftCmd.MaxLeaseIndex {
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
		retry := proposalNoReevaluation
		if isLocal {
			log.VEventf(
				ctx, 1,
				"retry proposal %x: applied at lease index %d, required < %d",
				idKey, leaseIndex, raftCmd.MaxLeaseIndex,
			)
			retry = proposalIllegalLeaseIndex
		}
		return leaseIndex, retry, roachpb.NewErrorf(
			"command observed at lease index %d, but required < %d", leaseIndex, raftCmd.MaxLeaseIndex,
		)
	}

	// Verify that command is not trying to write below the GC threshold. This is
	// necessary because not all commands declare read access on the GC
	// threshold key, even though they implicitly depend on it. This means
	// that access to this state will not be serialized by latching,
	// so we must perform this check upstream and downstream of raft.
	// TODO(andrei,nvanbenschoten,bdarnell): Is this check below-Raft actually
	// necessary, given that we've check at evaluation time that the request
	// evaluates at a timestamp above the GC threshold? Does it actually matter if
	// the GC threshold has advanced since then?
	wts := raftCmd.ReplicatedEvalResult.WriteTimestamp
	if !wts.IsEmpty() && wts.LessEq(*replicaState.GCThreshold) {
		return leaseIndex, proposalNoReevaluation, roachpb.NewError(&roachpb.BatchTimestampBeforeGCError{
			Timestamp: wts,
			Threshold: *replicaState.GCThreshold,
		})
	}
	return leaseIndex, proposalNoReevaluation, nil
}

// NewBatch implements the apply.StateMachine interface.
func (sm *replicaStateMachine) NewBatch(ephemeral bool) apply.Batch {
	r := sm.r
	if ephemeral {
		mb := &sm.ephemeralBatch
		mb.r = r
		r.mu.RLock()
		mb.state = r.mu.state
		r.mu.RUnlock()
		return mb
	}
	b := &sm.batch
	b.r = r
	b.sm = sm
	b.batch = r.store.engine.NewBatch()
	r.mu.RLock()
	b.state = r.mu.state
	b.state.Stats = &b.stats
	*b.state.Stats = *r.mu.state.Stats
	b.closedTimestampSetter = r.mu.closedTimestampSetter
	r.mu.RUnlock()
	b.start = timeutil.Now()
	return b
}

// replicaAppBatch implements the apply.Batch interface.
//
// The structure accumulates state due to the application of raft commands.
// Committed raft commands are applied to the state machine in a multi-stage
// process whereby individual commands are prepared for application relative
// to the current view of ReplicaState and staged in the batch. The batch is
// committed to the state machine's storage engine atomically.
type replicaAppBatch struct {
	r  *Replica
	sm *replicaStateMachine

	// batch accumulates writes implied by the raft entries in this batch.
	batch storage.Batch
	// state is this batch's view of the replica's state. It is copied from
	// under the Replica.mu when the batch is initialized and is updated in
	// stageTrivialReplicatedEvalResult.
	state kvserverpb.ReplicaState
	// closedTimestampSetter maintains historical information about the
	// advancement of the closed timestamp.
	closedTimestampSetter closedTimestampSetterInfo
	// stats is stored on the application batch to avoid an allocation in
	// tracking the batch's view of replicaState. All pointer fields in
	// replicaState other than Stats are overwritten completely rather than
	// updated in-place.
	stats enginepb.MVCCStats
	// maxTS is the maximum clock timestamp that this command carries. Timestamps
	// come from the writes that are part of this command, and also from the
	// closed timestamp carried by this command. Synthetic timestamps are not
	// registered here.
	maxTS hlc.ClockTimestamp
	// migrateToAppliedStateKey tracks whether any command in the batch
	// triggered a migration to the replica applied state key. If so, this
	// migration will be performed when the application batch is committed.
	migrateToAppliedStateKey bool
	// changeRemovesReplica tracks whether the command in the batch (there must
	// be only one) removes this replica from the range.
	changeRemovesReplica bool

	// Statistics.
	entries      int
	emptyEntries int
	mutations    int
	start        time.Time
}

// Stage implements the apply.Batch interface. The method handles the first
// phase of applying a command to the replica state machine.
//
// The first thing the method does is determine whether the command should be
// applied at all or whether it should be rejected and replaced with an empty
// entry. The determination is based on the following rules: the command's
// MaxLeaseIndex must move the state machine's LeaseAppliedIndex forward, the
// proposer's lease (or rather its sequence number) must match that of the state
// machine, and lastly the GCThreshold must be below the timestamp that the
// command evaluated at. If any of the checks fail, the proposal's content is
// wiped and we apply an empty log entry instead. If a rejected command was
// proposed locally, the error will eventually be communicated to the waiting
// proposer. The two typical cases in which errors occur are lease mismatch (in
// which case the caller tries to send the command to the actual leaseholder)
// and violation of the LeaseAppliedIndex (in which case the proposal is retried
// if it was proposed locally).
//
// Assuming all checks were passed, the command's write batch is applied to the
// application batch. Its trivial ReplicatedState updates are then staged in
// the batch. This allows the batch to make an accurate determination about
// whether to accept or reject the next command that is staged without needing
// to actually update the replica state machine in between.
func (b *replicaAppBatch) Stage(cmdI apply.Command) (apply.CheckedCommand, error) {
	cmd := cmdI.(*replicatedCmd)
	ctx := cmd.ctx
	if cmd.ent.Index == 0 {
		return nil, makeNonDeterministicFailure("processRaftCommand requires a non-zero index")
	}
	if idx, applied := cmd.ent.Index, b.state.RaftAppliedIndex; idx != applied+1 {
		// If we have an out of order index, there's corruption. No sense in
		// trying to update anything or running the command. Simply return.
		return nil, makeNonDeterministicFailure("applied index jumped from %d to %d", applied, idx)
	}
	if log.V(4) {
		log.Infof(ctx, "processing command %x: raftIndex=%d maxLeaseIndex=%d closedts=%s",
			cmd.idKey, cmd.ent.Index, cmd.raftCmd.MaxLeaseIndex, cmd.raftCmd.ClosedTimestamp)
	}

	// Determine whether the command should be applied to the replicated state
	// machine or whether it should be rejected (and replaced by an empty command).
	// This check is deterministic on all replicas, so if one replica decides to
	// reject a command, all will.
	if !b.r.shouldApplyCommand(ctx, cmd, &b.state) {
		log.VEventf(ctx, 1, "applying command with forced error: %s", cmd.forcedErr)

		// Apply an empty command.
		cmd.raftCmd.ReplicatedEvalResult = kvserverpb.ReplicatedEvalResult{}
		cmd.raftCmd.WriteBatch = nil
		cmd.raftCmd.LogicalOpLog = nil
		cmd.raftCmd.ClosedTimestamp = nil
	} else {
		if err := b.assertNoCmdClosedTimestampRegression(cmd); err != nil {
			return nil, err
		}
		if err := b.assertNoWriteBelowClosedTimestamp(cmd); err != nil {
			return nil, err
		}
		log.Event(ctx, "applying command")
	}

	// Acquire the split or merge lock, if necessary. If a split or merge
	// command was rejected with a below-Raft forced error then its replicated
	// result was just cleared and this will be a no-op.
	if splitMergeUnlock, err := b.r.maybeAcquireSplitMergeLock(ctx, cmd.raftCmd); err != nil {
		var err error
		if cmd.raftCmd.ReplicatedEvalResult.Split != nil {
			err = wrapWithNonDeterministicFailure(err, "unable to acquire split lock")
		} else {
			err = wrapWithNonDeterministicFailure(err, "unable to acquire merge lock")
		}
		return nil, err
	} else if splitMergeUnlock != nil {
		// Set the splitMergeUnlock on the replicaAppBatch to be called
		// after the batch has been applied (see replicaAppBatch.commit).
		cmd.splitMergeUnlock = splitMergeUnlock
	}

	// Update the batch's max timestamp.
	if clockTS, ok := cmd.replicatedResult().WriteTimestamp.TryToClockTimestamp(); ok {
		b.maxTS.Forward(clockTS)
	}

	// Normalize the command, accounting for past migrations.
	b.migrateReplicatedResult(ctx, cmd)

	// Run any triggers that should occur before the batch is applied
	// and before the write batch is staged in the batch.
	if err := b.runPreApplyTriggersBeforeStagingWriteBatch(ctx, cmd); err != nil {
		return nil, err
	}

	// Stage the command's write batch in the application batch.
	if err := b.stageWriteBatch(ctx, cmd); err != nil {
		return nil, err
	}

	// Run any triggers that should occur before the batch is applied
	// but after the write batch is staged in the batch.
	if err := b.runPreApplyTriggersAfterStagingWriteBatch(ctx, cmd); err != nil {
		return nil, err
	}

	// Stage the command's trivial ReplicatedState updates in the batch. Any
	// non-trivial commands will be in their own batch, so delaying their
	// non-trivial ReplicatedState updates until later (without ever staging
	// them in the batch) is sufficient.
	b.stageTrivialReplicatedEvalResult(ctx, cmd)
	b.entries++
	if len(cmd.ent.Data) == 0 {
		b.emptyEntries++
	}

	// The command was checked by shouldApplyCommand, so it can be returned
	// as an apply.CheckedCommand.
	return cmd, nil
}

// migrateReplicatedResult performs any migrations necessary on the command to
// normalize it before applying it to the batch. This may modify the command.
func (b *replicaAppBatch) migrateReplicatedResult(ctx context.Context, cmd *replicatedCmd) {
	// If the command was using the deprecated version of the MVCCStats proto,
	// migrate it to the new version and clear out the field.
	res := cmd.replicatedResult()
	if deprecatedDelta := res.DeprecatedDelta; deprecatedDelta != nil {
		if res.Delta != (enginepb.MVCCStatsDelta{}) {
			log.Fatalf(ctx, "stats delta not empty but deprecated delta provided: %+v", cmd)
		}
		res.Delta = deprecatedDelta.ToStatsDelta()
		res.DeprecatedDelta = nil
	}
}

// stageWriteBatch applies the command's write batch to the application batch's
// RocksDB batch. This batch is committed to RocksDB in replicaAppBatch.commit.
func (b *replicaAppBatch) stageWriteBatch(ctx context.Context, cmd *replicatedCmd) error {
	wb := cmd.raftCmd.WriteBatch
	if wb == nil {
		return nil
	}
	if mutations, err := storage.RocksDBBatchCount(wb.Data); err != nil {
		log.Errorf(ctx, "unable to read header of committed WriteBatch: %+v", err)
	} else {
		b.mutations += mutations
	}
	if err := b.batch.ApplyBatchRepr(wb.Data, false); err != nil {
		return wrapWithNonDeterministicFailure(err, "unable to apply WriteBatch")
	}
	return nil
}

// changeRemovesStore returns true if any of the removals in this change have storeID.
func changeRemovesStore(
	desc *roachpb.RangeDescriptor, change *kvserverpb.ChangeReplicas, storeID roachpb.StoreID,
) (removesStore bool) {
	curReplica, existsInDesc := desc.GetReplicaDescriptor(storeID)
	// NB: if we're catching up from a preemptive snapshot then we won't
	// exist in the current descriptor and we can't be removed.
	if !existsInDesc {
		return false
	}

	// NB: We don't use change.Removed() because it will include replicas being
	// transitioned to VOTER_OUTGOING.

	// In 19.1 and before we used DeprecatedUpdatedReplicas instead of providing
	// a new range descriptor. Check first if this is 19.1 or earlier command which
	// uses DeprecatedChangeType and DeprecatedReplica
	if change.Desc == nil {
		return change.DeprecatedChangeType == roachpb.REMOVE_VOTER && change.DeprecatedReplica.ReplicaID == curReplica.ReplicaID
	}
	// In 19.2 and beyond we supply the new range descriptor in the change.
	// We know we're removed if we do not appear in the new descriptor.
	_, existsInChange := change.Desc.GetReplicaDescriptor(storeID)
	return !existsInChange
}

// runPreApplyTriggersBeforeStagingWriteBatch runs any triggers that must fire
// before a command is applied to the state machine but after the command is
// staged in the replicaAppBatch's write batch. It may modify the command.
func (b *replicaAppBatch) runPreApplyTriggersBeforeStagingWriteBatch(
	ctx context.Context, cmd *replicatedCmd,
) error {
	if ops := cmd.raftCmd.LogicalOpLog; ops != nil {
		b.r.populatePrevValsInLogicalOpLogRaftMuLocked(ctx, ops, b.batch)
	}
	return nil
}

// runPreApplyTriggersAfterStagingWriteBatch runs any triggers that must fire
// before a command is applied to the state machine but after the command is
// staged in the replicaAppBatch's write batch. It may modify the command.
func (b *replicaAppBatch) runPreApplyTriggersAfterStagingWriteBatch(
	ctx context.Context, cmd *replicatedCmd,
) error {
	res := cmd.replicatedResult()

	// AddSSTable ingestions run before the actual batch gets written to the
	// storage engine. This makes sure that when the Raft command is applied,
	// the ingestion has definitely succeeded. Note that we have taken
	// precautions during command evaluation to avoid having mutations in the
	// WriteBatch that affect the SSTable. Not doing so could result in order
	// reversal (and missing values) here.
	//
	// NB: any command which has an AddSSTable is non-trivial and will be
	// applied in its own batch so it's not possible that any other commands
	// which precede this command can shadow writes from this SSTable.
	if res.AddSSTable != nil {
		copied := addSSTablePreApply(
			ctx,
			b.r.store.cfg.Settings,
			b.r.store.engine,
			b.r.raftMu.sideloaded,
			cmd.ent.Term,
			cmd.ent.Index,
			*res.AddSSTable,
			b.r.store.limiters.BulkIOWriteRate,
		)
		b.r.store.metrics.AddSSTableApplications.Inc(1)
		if copied {
			b.r.store.metrics.AddSSTableApplicationCopies.Inc(1)
		}
		if added := res.Delta.KeyCount; added > 0 {
			b.r.writeStats.recordCount(float64(added), 0)
		}
		res.AddSSTable = nil
	}

	if res.Split != nil {
		// Splits require a new HardState to be written to the new RHS
		// range (and this needs to be atomic with the main batch). This
		// cannot be constructed at evaluation time because it differs
		// on each replica (votes may have already been cast on the
		// uninitialized replica). Write this new hardstate to the batch too.
		// See https://github.com/cockroachdb/cockroach/issues/20629.
		//
		// Alternatively if we discover that the RHS has already been removed
		// from this store, clean up its data.
		splitPreApply(ctx, b.r, b.batch, res.Split.SplitTrigger, cmd.raftCmd.ClosedTimestamp)

		// The rangefeed processor will no longer be provided logical ops for
		// its entire range, so it needs to be shut down and all registrations
		// need to retry.
		// TODO(nvanbenschoten): It should be possible to only reject registrations
		// that overlap with the new range of the split and keep registrations that
		// are only interested in keys that are still on the original range running.
		b.r.disconnectRangefeedWithReason(
			roachpb.RangeFeedRetryError_REASON_RANGE_SPLIT,
		)
	}

	if merge := res.Merge; merge != nil {
		// Merges require the subsumed range to be atomically deleted when the
		// merge transaction commits.

		// If our range currently has a non-zero replica ID then we know we're
		// safe to commit this merge because of the invariants provided to us
		// by the merge protocol. Namely if this committed we know that if the
		// command committed then all of the replicas in the range descriptor
		// are collocated when this command commits. If we do not have a non-zero
		// replica ID then the logic in Stage should detect that and destroy our
		// preemptive snapshot so we shouldn't ever get here.
		rhsRepl, err := b.r.store.GetReplica(merge.RightDesc.RangeID)
		if err != nil {
			return wrapWithNonDeterministicFailure(err, "unable to get replica for merge")
		}
		// We should already have acquired the raftMu for the rhsRepl and now hold
		// its unlock method in cmd.splitMergeUnlock.
		rhsRepl.raftMu.AssertHeld()

		// We mark the replica as destroyed so that new commands are not
		// accepted. This destroy status will be detected after the batch
		// commits by handleMergeResult() to finish the removal.
		rhsRepl.readOnlyCmdMu.Lock()
		rhsRepl.mu.Lock()
		rhsRepl.mu.destroyStatus.Set(
			roachpb.NewRangeNotFoundError(rhsRepl.RangeID, rhsRepl.store.StoreID()),
			destroyReasonRemoved)
		rhsRepl.mu.Unlock()
		rhsRepl.readOnlyCmdMu.Unlock()

		// Use math.MaxInt32 (mergedTombstoneReplicaID) as the nextReplicaID as an
		// extra safeguard against creating new replicas of the RHS. This isn't
		// required for correctness, since the merge protocol should guarantee that
		// no new replicas of the RHS can ever be created, but it doesn't hurt to
		// be careful.
		const clearRangeIDLocalOnly = true
		const mustClearRange = false
		if err := rhsRepl.preDestroyRaftMuLocked(
			ctx, b.batch, b.batch, mergedTombstoneReplicaID, clearRangeIDLocalOnly, mustClearRange,
		); err != nil {
			return wrapWithNonDeterministicFailure(err, "unable to destroy replica before merge")
		}

		// Shut down rangefeed processors on either side of the merge.
		//
		// NB: It is critical to shut-down a rangefeed processor on the surviving
		// replica primarily do deal with the possibility that there are logical ops
		// for the RHS to resolve intents written by the merge transaction. In
		// practice, the only such intents that exist are on the RangeEventTable,
		// but it's good to be consistent here and allow the merge transaction to
		// write to the RHS of a merge. See batcheval.resolveLocalLocks for details
		// on why we resolve RHS intents when committing a merge transaction.
		//
		// TODO(nvanbenschoten): Alternatively we could just adjust the bounds of
		// b.r.Processor to include the rhsRepl span.
		//
		// NB: removeInitializedReplicaRaftMuLocked also disconnects any initialized
		// rangefeeds with REASON_REPLICA_REMOVED. That's ok because we will have
		// already disconnected the rangefeed here.
		b.r.disconnectRangefeedWithReason(
			roachpb.RangeFeedRetryError_REASON_RANGE_MERGED,
		)
		rhsRepl.disconnectRangefeedWithReason(
			roachpb.RangeFeedRetryError_REASON_RANGE_MERGED,
		)
	}

	if res.State != nil && res.State.TruncatedState != nil {
		activeVersion := b.r.ClusterSettings().Version.ActiveVersion(ctx).Version
		migrationVersion := clusterversion.ByKey(clusterversion.TruncatedAndRangeAppliedStateMigration)
		// NB: We're being deliberate here in using the less-than operator (as
		// opposed to LessEq). TruncatedAndRangeAppliedStateMigration indicates
		// that the migration to move to the unreplicated truncated
		// state is currently underway. It's only when the active cluster
		// version has moved past it that we can assume that the migration has
		// completed.
		assertNoLegacy := migrationVersion.Less(activeVersion)

		if apply, err := handleTruncatedStateBelowRaft(
			ctx, b.state.TruncatedState, res.State.TruncatedState, b.r.raftMu.stateLoader, b.batch,
			assertNoLegacy,
		); err != nil {
			return wrapWithNonDeterministicFailure(err, "unable to handle truncated state")
		} else if !apply {
			// The truncated state was discarded, so make sure we don't apply
			// it to our in-memory state.
			res.State.TruncatedState = nil
			res.RaftLogDelta = 0
			// TODO(ajwerner): consider moving this code.
			// We received a truncation that doesn't apply to us, so we know that
			// there's a leaseholder out there with a log that has earlier entries
			// than ours. That leader also guided our log size computations by
			// giving us RaftLogDeltas for past truncations, and this was likely
			// off. Mark our Raft log size is not trustworthy so that, assuming
			// we step up as leader at some point in the future, we recompute
			// our numbers.
			b.r.mu.Lock()
			b.r.mu.raftLogSizeTrusted = false
			b.r.mu.Unlock()
		}
	}

	// Detect if this command will remove us from the range.
	// If so we stage the removal of all of our range data into this batch.
	// We'll complete the removal when it commits. Later logic detects the
	// removal by inspecting the destroy status.
	//
	// NB: This is the last step in the preApply which durably writes to the
	// replica state so that if it removes the replica it removes everything.
	if change := res.ChangeReplicas; change != nil &&
		changeRemovesStore(b.state.Desc, change, b.r.store.StoreID()) &&
		// Don't remove the data if the testing knobs ask us not to.
		!b.r.store.TestingKnobs().DisableEagerReplicaRemoval {

		// We mark the replica as destroyed so that new commands are not
		// accepted. This destroy status will be detected after the batch
		// commits by handleChangeReplicasResult() to finish the removal.
		//
		// NB: we must be holding the raftMu here because we're in the midst of
		// application.
		b.r.readOnlyCmdMu.Lock()
		b.r.mu.Lock()
		b.r.mu.destroyStatus.Set(
			roachpb.NewRangeNotFoundError(b.r.RangeID, b.r.store.StoreID()),
			destroyReasonRemoved)
		b.r.mu.Unlock()
		b.r.readOnlyCmdMu.Unlock()
		b.changeRemovesReplica = true

		// Delete all of the local data. We're going to delete the hard state too.
		// In order for this to be safe we need code above this to promise that we're
		// never going to write hard state in response to a message for a later
		// replica (with a different replica ID) to this range state.
		if err := b.r.preDestroyRaftMuLocked(
			ctx,
			b.batch,
			b.batch,
			change.NextReplicaID(),
			false, /* clearRangeIDLocalOnly */
			false, /* mustUseClearRange */
		); err != nil {
			return wrapWithNonDeterministicFailure(err, "unable to destroy replica before removal")
		}
	}

	// Provide the command's corresponding logical operations to the Replica's
	// rangefeed. Only do so if the WriteBatch is non-nil, in which case the
	// rangefeed requires there to be a corresponding logical operation log or
	// it will shut down with an error. If the WriteBatch is nil then we expect
	// the logical operation log to also be nil. We don't want to trigger a
	// shutdown of the rangefeed in that situation, so we don't pass anything to
	// the rangefeed. If no rangefeed is running at all, this call will be a noop.
	if ops := cmd.raftCmd.LogicalOpLog; cmd.raftCmd.WriteBatch != nil {
		b.r.handleLogicalOpLogRaftMuLocked(ctx, ops, b.batch)
	} else if ops != nil {
		log.Fatalf(ctx, "non-nil logical op log with nil write batch: %v", cmd.raftCmd)
	}

	return nil
}

// stageTrivialReplicatedEvalResult applies the trivial portions of the
// command's ReplicatedEvalResult to the batch's ReplicaState. This function
// modifies the receiver's ReplicaState but does not modify ReplicatedEvalResult
// in order to give the TestingPostApplyFilter testing knob an opportunity to
// inspect the command's ReplicatedEvalResult.
func (b *replicaAppBatch) stageTrivialReplicatedEvalResult(
	ctx context.Context, cmd *replicatedCmd,
) {
	if raftAppliedIndex := cmd.ent.Index; raftAppliedIndex != 0 {
		b.state.RaftAppliedIndex = raftAppliedIndex
	}
	if leaseAppliedIndex := cmd.leaseIndex; leaseAppliedIndex != 0 {
		b.state.LeaseAppliedIndex = leaseAppliedIndex
	}
	if cts := cmd.raftCmd.ClosedTimestamp; cts != nil && !cts.IsEmpty() {
		b.state.RaftClosedTimestamp = *cts
		b.closedTimestampSetter.record(cmd, b.state.Lease)
		if clockTS, ok := cts.TryToClockTimestamp(); ok {
			b.maxTS.Forward(clockTS)
		}
	}

	res := cmd.replicatedResult()

	// Special-cased MVCC stats handling to exploit commutativity of stats delta
	// upgrades. Thanks to commutativity, the spanlatch manager does not have to
	// serialize on the stats key.
	deltaStats := res.Delta.ToStats()
	b.state.Stats.Add(deltaStats)

	if res.State != nil && res.State.UsingAppliedStateKey && !b.state.UsingAppliedStateKey {
		b.migrateToAppliedStateKey = true
	}
}

// ApplyToStateMachine implements the apply.Batch interface. The method handles
// the second phase of applying a command to the replica state machine. It
// writes the application batch's accumulated RocksDB batch to the storage
// engine. This encompasses the persistent state transition portion of entry
// application.
func (b *replicaAppBatch) ApplyToStateMachine(ctx context.Context) error {
	if log.V(4) {
		log.Infof(ctx, "flushing batch %v of %d entries", b.state, b.entries)
	}

	// Update the node clock with the maximum timestamp of all commands in the
	// batch. This maintains a high water mark for all ops serviced, so that
	// received ops without a timestamp specified are guaranteed one higher than
	// any op already executed for overlapping keys.
	r := b.r
	r.store.Clock().Update(b.maxTS)

	// Add the replica applied state key to the write batch if this change
	// doesn't remove us.
	if !b.changeRemovesReplica {
		if err := b.addAppliedStateKeyToBatch(ctx); err != nil {
			return err
		}
	}

	// Apply the write batch to RockDB. Entry application is done without
	// syncing to disk. The atomicity guarantees of the batch and the fact that
	// the applied state is stored in this batch, ensure that if the batch ends
	// up not being durably committed then the entries in this batch will be
	// applied again upon startup. However, if we're removing the replica's data
	// then we sync this batch as it is not safe to call postDestroyRaftMuLocked
	// before ensuring that the replica's data has been synchronously removed.
	// See handleChangeReplicasResult().
	sync := b.changeRemovesReplica
	if err := b.batch.Commit(sync); err != nil {
		return wrapWithNonDeterministicFailure(err, "unable to commit Raft entry batch")
	}
	b.batch.Close()
	b.batch = nil

	// Update the replica's applied indexes, mvcc stats and closed timestamp.
	r.mu.Lock()
	r.mu.state.RaftAppliedIndex = b.state.RaftAppliedIndex
	r.mu.state.LeaseAppliedIndex = b.state.LeaseAppliedIndex

	// Sanity check that the RaftClosedTimestamp doesn't go backwards.
	existingClosed := r.mu.state.RaftClosedTimestamp
	newClosed := b.state.RaftClosedTimestamp
	if !newClosed.IsEmpty() && newClosed.Less(existingClosed) && raftClosedTimestampAssertionsEnabled {
		return errors.AssertionFailedf(
			"raft closed timestamp regression; replica has: %s, new batch has: %s.",
			existingClosed.String(), newClosed.String())
	}
	r.mu.closedTimestampSetter = b.closedTimestampSetter

	closedTimestampUpdated := r.mu.state.RaftClosedTimestamp.Forward(b.state.RaftClosedTimestamp)
	prevStats := *r.mu.state.Stats
	*r.mu.state.Stats = *b.state.Stats

	// If the range is now less than its RangeMaxBytes, clear the history of its
	// largest previous max bytes.
	if r.mu.largestPreviousMaxRangeSizeBytes > 0 && b.state.Stats.Total() < *r.mu.zone.RangeMaxBytes {
		r.mu.largestPreviousMaxRangeSizeBytes = 0
	}

	// Check the queuing conditions while holding the lock.
	needsSplitBySize := r.needsSplitBySizeRLocked()
	needsMergeBySize := r.needsMergeBySizeRLocked()
	needsTruncationByLogSize := r.needsRaftLogTruncationLocked()
	tenantID := r.mu.tenantID
	r.mu.Unlock()
	if closedTimestampUpdated {
		// TODO(andrei): Pass in the new closed timestamp to
		// r.handleClosedTimestampUpdateRaftMuLocked directly after the old closed
		// ts tracker goes away. Until then we can't do it; we have to let the
		// method consult r.maxClosed().
		r.handleClosedTimestampUpdateRaftMuLocked(ctx)
	}

	// Record the stats delta in the StoreMetrics.
	deltaStats := *b.state.Stats
	deltaStats.Subtract(prevStats)
	r.store.metrics.addMVCCStats(ctx, tenantID, deltaStats)

	// Record the write activity, passing a 0 nodeID because replica.writeStats
	// intentionally doesn't track the origin of the writes.
	b.r.writeStats.recordCount(float64(b.mutations), 0 /* nodeID */)

	now := timeutil.Now()
	if needsSplitBySize && r.splitQueueThrottle.ShouldProcess(now) {
		r.store.splitQueue.MaybeAddAsync(ctx, r, r.store.Clock().NowAsClockTimestamp())
	}
	if needsMergeBySize && r.mergeQueueThrottle.ShouldProcess(now) {
		r.store.mergeQueue.MaybeAddAsync(ctx, r, r.store.Clock().NowAsClockTimestamp())
	}
	if needsTruncationByLogSize {
		r.store.raftLogQueue.MaybeAddAsync(ctx, r, r.store.Clock().NowAsClockTimestamp())
	}

	b.recordStatsOnCommit()
	return nil
}

// addAppliedStateKeyToBatch adds the applied state key to the application
// batch's RocksDB batch. This records the highest raft and lease index that
// have been applied as of this batch. It also records the Range's mvcc stats.
func (b *replicaAppBatch) addAppliedStateKeyToBatch(ctx context.Context) error {
	loader := &b.r.raftMu.stateLoader
	if b.migrateToAppliedStateKey {
		// A Raft command wants us to begin using the RangeAppliedState key
		// and we haven't performed the migration yet. Delete the old keys
		// that this new key is replacing.
		//
		// NB: entering this branch indicates that the batch contains only a
		// single non-trivial command.
		err := loader.MigrateToRangeAppliedStateKey(ctx, b.batch, b.state.Stats)
		if err != nil {
			return wrapWithNonDeterministicFailure(err, "unable to migrate to range applied state")
		}
		b.state.UsingAppliedStateKey = true
	}
	if b.state.UsingAppliedStateKey {
		// Set the range applied state, which includes the last applied raft and
		// lease index along with the mvcc stats, all in one key.
		if err := loader.SetRangeAppliedState(
			ctx, b.batch, b.state.RaftAppliedIndex, b.state.LeaseAppliedIndex,
			b.state.Stats, &b.state.RaftClosedTimestamp,
		); err != nil {
			return wrapWithNonDeterministicFailure(err, "unable to set range applied state")
		}
	} else {
		// Advance the last applied index. We use a blind write in order to avoid
		// reading the previous applied index keys on every write operation. This
		// requires a little additional work in order maintain the MVCC stats.
		var appliedIndexNewMS enginepb.MVCCStats
		if err := loader.SetLegacyAppliedIndexBlind(
			ctx, b.batch, &appliedIndexNewMS, b.state.RaftAppliedIndex, b.state.LeaseAppliedIndex,
		); err != nil {
			return wrapWithNonDeterministicFailure(err, "unable to set applied index")
		}
		b.state.Stats.SysBytes += appliedIndexNewMS.SysBytes -
			loader.CalcAppliedIndexSysBytes(b.state.RaftAppliedIndex, b.state.LeaseAppliedIndex)

		// Set the legacy MVCC stats key.
		if err := loader.SetMVCCStats(ctx, b.batch, b.state.Stats); err != nil {
			return wrapWithNonDeterministicFailure(err, "unable to update MVCCStats")
		}
	}
	return nil
}

func (b *replicaAppBatch) recordStatsOnCommit() {
	b.sm.stats.entriesProcessed += b.entries
	b.sm.stats.numEmptyEntries += b.emptyEntries
	b.sm.stats.batchesProcessed++

	elapsed := timeutil.Since(b.start)
	b.r.store.metrics.RaftCommandCommitLatency.RecordValue(elapsed.Nanoseconds())
}

// Close implements the apply.Batch interface.
func (b *replicaAppBatch) Close() {
	if b.batch != nil {
		b.batch.Close()
	}
	*b = replicaAppBatch{}
}

// raftClosedTimestampAssertionsEnabled provides an emergency way of shutting
// down assertions.
var raftClosedTimestampAssertionsEnabled = envutil.EnvOrDefaultBool("COCKROACH_RAFT_CLOSEDTS_ASSERTIONS_ENABLED", true)

// Assert that the current command is not writing under the closed timestamp.
// This check only applies to IntentWrite commands, since others (for example,
// EndTxn) can operate below the closed timestamp.
//
// Note that we check that we're we're writing under b.state.RaftClosedTimestamp
// (i.e. below the timestamp closed by previous commands), not below
// cmd.raftCmd.ClosedTimestamp. A command is allowed to write below the closed
// timestamp carried by itself; in other words cmd.raftCmd.ClosedTimestamp is a
// promise about future commands, not the command carrying it.
func (b *replicaAppBatch) assertNoWriteBelowClosedTimestamp(cmd *replicatedCmd) error {
	if !cmd.IsLocal() || !cmd.proposal.Request.IsIntentWrite() {
		return nil
	}
	if !raftClosedTimestampAssertionsEnabled {
		return nil
	}
	wts := cmd.raftCmd.ReplicatedEvalResult.WriteTimestamp
	if !wts.IsEmpty() && wts.LessEq(b.state.RaftClosedTimestamp) {
		wts := wts // Make a shadow variable that escapes to the heap.
		var req redact.StringBuilder
		if cmd.proposal != nil {
			req.Print(cmd.proposal.Request)
		} else {
			req.SafeString("request unknown; not leaseholder")
		}
		return wrapWithNonDeterministicFailure(errors.AssertionFailedf(
			"command writing below closed timestamp; cmd: %x, write ts: %s, "+
				"batch state closed: %s, command closed: %s, request: %s, lease: %s",
			cmd.idKey, wts,
			b.state.RaftClosedTimestamp, cmd.raftCmd.ClosedTimestamp,
			req, b.state.Lease),
			"command writing below closed timestamp")
	}
	return nil
}

// Assert that the closed timestamp carried by the command is not below one from
// previous commands.
func (b *replicaAppBatch) assertNoCmdClosedTimestampRegression(cmd *replicatedCmd) error {
	if !raftClosedTimestampAssertionsEnabled {
		return nil
	}
	existingClosed := &b.state.RaftClosedTimestamp
	newClosed := cmd.raftCmd.ClosedTimestamp
	if newClosed != nil && !newClosed.IsEmpty() && newClosed.Less(*existingClosed) {
		var req redact.StringBuilder
		if cmd.IsLocal() {
			req.Print(cmd.proposal.Request)
		} else {
			req.SafeString("<unknown; not leaseholder>")
		}
		var prevReq redact.StringBuilder
		if req := b.closedTimestampSetter.leaseReq; req != nil {
			prevReq.Printf("lease acquisition: %s (prev: %s)", req.Lease, req.PrevLease)
		} else {
			prevReq.SafeString("<unknown; not leaseholder or not lease request>")
		}

		logTail, err := b.r.printRaftTail(cmd.ctx, 100 /* maxEntries */, 2000 /* maxCharsPerEntry */)
		if err != nil {
			if logTail != "" {
				logTail = logTail + "\n; error printing log: " + err.Error()
			} else {
				logTail = "error printing log: " + err.Error()
			}
		}

		return errors.AssertionFailedf(
			"raft closed timestamp regression in cmd: %x (term: %d, index: %d); batch state: %s, command: %s, lease: %s, req: %s, applying at LAI: %d.\n"+
				"Closed timestamp was set by req: %s under lease: %s; applied at LAI: %d. Batch idx: %d.\n"+
				"This assertion will fire again on restart; to ignore run with env var COCKROACH_RAFT_CLOSEDTS_ASSERTIONS_ENABLED=true"+
				"Raft log tail:\n%s",
			cmd.idKey, cmd.ent.Term, cmd.ent.Index, existingClosed, newClosed, b.state.Lease, req, cmd.leaseIndex,
			prevReq, b.closedTimestampSetter.lease, b.closedTimestampSetter.leaseIdx, b.entries,
			logTail)
	}
	return nil
}

// ephemeralReplicaAppBatch implements the apply.Batch interface.
//
// The batch performs the bare-minimum amount of work to be able to
// determine whether a replicated command should be rejected or applied.
type ephemeralReplicaAppBatch struct {
	r     *Replica
	state kvserverpb.ReplicaState
}

// Stage implements the apply.Batch interface.
func (mb *ephemeralReplicaAppBatch) Stage(cmdI apply.Command) (apply.CheckedCommand, error) {
	cmd := cmdI.(*replicatedCmd)
	ctx := cmd.ctx

	mb.r.shouldApplyCommand(ctx, cmd, &mb.state)
	mb.state.LeaseAppliedIndex = cmd.leaseIndex
	return cmd, nil
}

// ApplyToStateMachine implements the apply.Batch interface.
func (mb *ephemeralReplicaAppBatch) ApplyToStateMachine(ctx context.Context) error {
	panic("cannot apply ephemeralReplicaAppBatch to state machine")
}

// Close implements the apply.Batch interface.
func (mb *ephemeralReplicaAppBatch) Close() {
	*mb = ephemeralReplicaAppBatch{}
}

// ApplySideEffects implements the apply.StateMachine interface. The method
// handles the third phase of applying a command to the replica state machine.
//
// It is called with commands whose write batches have already been committed
// to the storage engine and whose trivial side-effects have been applied to
// the Replica's in-memory state. This method deals with applying non-trivial
// side effects of commands, such as finalizing splits/merges and informing
// raft about applied config changes.
func (sm *replicaStateMachine) ApplySideEffects(
	cmdI apply.CheckedCommand,
) (apply.AppliedCommand, error) {
	cmd := cmdI.(*replicatedCmd)
	ctx := cmd.ctx

	// Deal with locking during side-effect handling, which is sometimes
	// associated with complex commands such as splits and merged.
	if unlock := cmd.splitMergeUnlock; unlock != nil {
		defer unlock()
	}

	// Set up the local result prior to handling the ReplicatedEvalResult to
	// give testing knobs an opportunity to inspect it. An injected corruption
	// error will lead to replica removal.
	sm.r.prepareLocalResult(ctx, cmd)
	if log.ExpensiveLogEnabled(ctx, 2) {
		log.VEventf(ctx, 2, "%v", cmd.localResult.String())
	}

	// Handle the ReplicatedEvalResult, executing any side effects of the last
	// state machine transition.
	//
	// Note that this must happen after committing (the engine.Batch), but
	// before notifying a potentially waiting client.
	clearTrivialReplicatedEvalResultFields(cmd.replicatedResult())
	if !cmd.IsTrivial() {
		shouldAssert, isRemoved := sm.handleNonTrivialReplicatedEvalResult(ctx, cmd.replicatedResult())
		if isRemoved {
			// The proposal must not have been local, because we don't allow a
			// proposing replica to remove itself from the Range.
			cmd.FinishNonLocal(ctx)
			return nil, apply.ErrRemoved
		}
		// NB: Perform state assertion before acknowledging the client.
		// Some tests (TestRangeStatsInit) assumes that once the store has started
		// and the first range has a lease that there will not be a later hard-state.
		if shouldAssert {
			// Assert that the on-disk state doesn't diverge from the in-memory
			// state as a result of the side effects.
			sm.r.mu.RLock()
			sm.r.assertStateRaftMuLockedReplicaMuRLocked(ctx, sm.r.store.Engine())
			sm.r.mu.RUnlock()
			sm.stats.stateAssertions++
		}
	} else if res := cmd.replicatedResult(); !res.IsZero() {
		log.Fatalf(ctx, "failed to handle all side-effects of ReplicatedEvalResult: %v", res)
	}

	// On ConfChange entries, inform the raft.RawNode.
	if err := sm.maybeApplyConfChange(ctx, cmd); err != nil {
		return nil, wrapWithNonDeterministicFailure(err, "unable to apply conf change")
	}

	// Mark the command as applied and return it as an apply.AppliedCommand.
	// NB: Commands which were reproposed at a higher MaxLeaseIndex will not be
	// considered local at this point as their proposal will have been detached
	// in prepareLocalResult().
	if cmd.IsLocal() {
		// Handle the LocalResult.
		if cmd.localResult != nil {
			sm.r.handleReadWriteLocalEvalResult(ctx, *cmd.localResult)
		}

		rejected := cmd.Rejected()
		higherReproposalsExist := cmd.raftCmd.MaxLeaseIndex != cmd.proposal.command.MaxLeaseIndex
		if !rejected && higherReproposalsExist {
			log.Fatalf(ctx, "finishing proposal with outstanding reproposal at a higher max lease index")
		}
		if !rejected && cmd.proposal.applied {
			// If the command already applied then we shouldn't be "finishing" its
			// application again because it should only be able to apply successfully
			// once. We expect that when any reproposal for the same command attempts
			// to apply it will be rejected by the below raft lease sequence or lease
			// index check in checkForcedErr.
			log.Fatalf(ctx, "command already applied: %+v; unexpected successful result", cmd)
		}
		// If any reproposals at a higher MaxLeaseIndex exist we know that they will
		// never successfully apply, remove them from the map to avoid future
		// reproposals. If there is no command referencing this proposal at a higher
		// MaxLeaseIndex then it will already have been removed (see
		// shouldRemove in replicaDecoder.retrieveLocalProposals()). It is possible
		// that a later command in this batch referred to this proposal but it must
		// have failed because it carried the same MaxLeaseIndex.
		if higherReproposalsExist {
			sm.r.mu.Lock()
			delete(sm.r.mu.proposals, cmd.idKey)
			sm.r.mu.Unlock()
		}
		cmd.proposal.applied = true
	}
	return cmd, nil
}

// handleNonTrivialReplicatedEvalResult carries out the side-effects of
// non-trivial commands. It is run with the raftMu locked. It is illegal
// to pass a replicatedResult that does not imply any side-effects.
func (sm *replicaStateMachine) handleNonTrivialReplicatedEvalResult(
	ctx context.Context, rResult *kvserverpb.ReplicatedEvalResult,
) (shouldAssert, isRemoved bool) {
	// Assert that this replicatedResult implies at least one side-effect.
	if rResult.IsZero() {
		log.Fatalf(ctx, "zero-value ReplicatedEvalResult passed to handleNonTrivialReplicatedEvalResult")
	}

	if rResult.State != nil {
		if newLease := rResult.State.Lease; newLease != nil {
			sm.r.handleLeaseResult(ctx, newLease, rResult.PriorReadSummary)
			rResult.State.Lease = nil
			rResult.PriorReadSummary = nil
		}

		if newTruncState := rResult.State.TruncatedState; newTruncState != nil {
			rResult.RaftLogDelta += sm.r.handleTruncatedStateResult(ctx, newTruncState)
			rResult.State.TruncatedState = nil
		}

		if newThresh := rResult.State.GCThreshold; newThresh != nil {
			sm.r.handleGCThresholdResult(ctx, newThresh)
			rResult.State.GCThreshold = nil
		}

		if newVersion := rResult.State.Version; newVersion != nil {
			sm.r.handleVersionResult(ctx, newVersion)
			rResult.State.Version = nil
		}
		if (*rResult.State == kvserverpb.ReplicaState{}) {
			rResult.State = nil
		}
	}

	if rResult.RaftLogDelta != 0 {
		sm.r.handleRaftLogDeltaResult(ctx, rResult.RaftLogDelta)
		rResult.RaftLogDelta = 0
	}

	// The rest of the actions are "nontrivial" and may have large effects on the
	// in-memory and on-disk ReplicaStates. If any of these actions are present,
	// we want to assert that these two states do not diverge.
	shouldAssert = !rResult.IsZero()
	if !shouldAssert {
		return false, false
	}

	if rResult.Split != nil {
		sm.r.handleSplitResult(ctx, rResult.Split)
		rResult.Split = nil
	}

	if rResult.Merge != nil {
		sm.r.handleMergeResult(ctx, rResult.Merge)
		rResult.Merge = nil
	}

	if rResult.State != nil {
		if newDesc := rResult.State.Desc; newDesc != nil {
			sm.r.handleDescResult(ctx, newDesc)
			rResult.State.Desc = nil
		}

		if rResult.State.UsingAppliedStateKey {
			sm.r.handleUsingAppliedStateKeyResult(ctx)
			rResult.State.UsingAppliedStateKey = false
		}

		if (*rResult.State == kvserverpb.ReplicaState{}) {
			rResult.State = nil
		}
	}

	if rResult.ChangeReplicas != nil {
		isRemoved = sm.r.handleChangeReplicasResult(ctx, rResult.ChangeReplicas)
		rResult.ChangeReplicas = nil
	}

	if rResult.ComputeChecksum != nil {
		sm.r.handleComputeChecksumResult(ctx, rResult.ComputeChecksum)
		rResult.ComputeChecksum = nil
	}

	if !rResult.IsZero() {
		log.Fatalf(ctx, "unhandled field in ReplicatedEvalResult: %s", pretty.Diff(rResult, kvserverpb.ReplicatedEvalResult{}))
	}
	return true, isRemoved
}

func (sm *replicaStateMachine) maybeApplyConfChange(ctx context.Context, cmd *replicatedCmd) error {
	switch cmd.ent.Type {
	case raftpb.EntryNormal:
		return nil
	case raftpb.EntryConfChange, raftpb.EntryConfChangeV2:
		sm.stats.numConfChangeEntries++
		if cmd.Rejected() {
			// The command was rejected. There is no need to report a ConfChange
			// to raft.
			return nil
		}
		return sm.r.withRaftGroup(true, func(rn *raft.RawNode) (bool, error) {
			rn.ApplyConfChange(cmd.confChange.ConfChangeI)
			return true, nil
		})
	default:
		panic("unexpected")
	}
}

func (sm *replicaStateMachine) moveStats() applyCommittedEntriesStats {
	stats := sm.stats
	sm.stats = applyCommittedEntriesStats{}
	return stats
}

// closedTimestampSetterInfo contains information about the command that last
// bumped the closed timestamp.
type closedTimestampSetterInfo struct {
	// lease represents the lease under which the command is being applied.
	lease *roachpb.Lease
	// leaseIdx is the LAI of the command.
	leaseIdx ctpb.LAI
	// leaseReq is set if the request that generated this command was a
	// RequestLeaseRequest. This is only ever set on the leaseholder replica since
	// only the leaseholder has information about the request corresponding to a
	// command.
	// NOTE: We only keep track of lease requests because keeping track of all
	// requests would be too expensive: cloning the request is expensive and also
	// requests can be large in memory.
	leaseReq *roachpb.RequestLeaseRequest
	// split and merge are set if the request was an EndTxn with the respective
	// commit trigger set.
	split, merge bool
}

// record saves information about the command that update's the replica's closed
// timestamp.
func (s *closedTimestampSetterInfo) record(cmd *replicatedCmd, lease *roachpb.Lease) {
	*s = closedTimestampSetterInfo{}
	s.leaseIdx = ctpb.LAI(cmd.leaseIndex)
	s.lease = lease
	if !cmd.IsLocal() {
		return
	}
	req := cmd.proposal.Request
	et, ok := req.GetArg(roachpb.EndTxn)
	if ok {
		endTxn := et.(*roachpb.EndTxnRequest)
		if trig := endTxn.InternalCommitTrigger; trig != nil {
			if trig.SplitTrigger != nil {
				s.split = true
			} else if trig.MergeTrigger != nil {
				s.merge = true
			}
		}
	} else if req.IsLeaseRequest() {
		// Make a deep copy since we're not allowed to hold on to request
		// memory.
		lr, _ := req.GetArg(roachpb.RequestLease)
		s.leaseReq = protoutil.Clone(lr).(*roachpb.RequestLeaseRequest)
	}
}
