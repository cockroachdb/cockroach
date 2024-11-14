// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package kvserverbase

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
)

// ProposalRejectionType indicates how to handle a proposal that was
// rejected below raft (e.g. by the lease or lease applied index checks).
type ProposalRejectionType int

const (
	// ProposalRejectionPermanent indicates that the rejection is permanent.
	ProposalRejectionPermanent ProposalRejectionType = iota
	// ProposalRejectionIllegalLeaseIndex indicates the proposal failed to apply as the
	// assigned lease index had been consumed, and it is known that this proposal
	// had not applied previously. The command can be retried at a higher lease
	// index.
	ProposalRejectionIllegalLeaseIndex
)

// noopOnEmptyRaftCommandErr is returned from CheckForcedErr when an empty raft
// command is received. See the comment near its use.
var noopOnEmptyRaftCommandErr = kvpb.NewErrorf("no-op on empty Raft entry")

// NoopOnProbeCommandErr is returned from CheckForcedErr when a raft command
// corresponding to a ProbeRequest is handled.
var NoopOnProbeCommandErr = kvpb.NewErrorf("no-op on ProbeRequest")

// ForcedErrResult is the output from CheckForcedErr.
type ForcedErrResult struct {
	LeaseIndex  kvpb.LeaseAppliedIndex
	Rejection   ProposalRejectionType
	ForcedError *kvpb.Error
}

// CheckForcedErr determines whether or not a command should be applied to the
// replicated state machine after it has been committed to the Raft log. This
// decision is deterministic on all replicas, such that a command that is
// rejected "beneath raft" on one replica will be rejected "beneath raft" on
// all replicas.
//
// The decision about whether or not to apply a command is a combination of
// three checks:
//  1. verify that the command was proposed under the current lease. This is
//     determined using the proposal's ProposerLeaseSequence.
//     1.1. lease requests instead check for specifying the current lease
//     as the lease they follow.
//     1.2. ProbeRequest instead always fail this step with noopOnProbeCommandErr.
//  2. verify that the command hasn't been re-ordered with other commands that
//     were proposed after it and which already applied. This is determined
//     using the proposal's MaxLeaseIndex.
//  3. verify that the command isn't in violation of the Range's current
//     garbage collection threshold. This is determined using the proposal's
//     Timestamp.
//
// TODO(nvanbenschoten): Unit test this function now that it is stateless.
func CheckForcedErr(
	ctx context.Context,
	idKey CmdIDKey,
	raftCmd *kvserverpb.RaftCommand,
	isLocal bool,
	replicaState *kvserverpb.ReplicaState,
) (res ForcedErrResult) {
	isLeaseRequest := raftCmd.ReplicatedEvalResult.IsLeaseRequest

	defer func() {
		if res.ForcedError != nil &&
			raftCmd.ReplicatedEvalResult.State != nil &&
			raftCmd.ReplicatedEvalResult.State.Lease != nil {
			op := redact.SafeString("transfer")
			if isLeaseRequest {
				op = "request"
			}
			log.Infof(ctx, "rejected lease %s %s; current lease %s; err: %s",
				op, raftCmd.ReplicatedEvalResult.State.Lease, replicaState.Lease, res.ForcedError)
		}
	}()

	if raftCmd.ReplicatedEvalResult.IsProbe {
		// A Probe is handled by forcing an error during application (which
		// avoids a separate "success" code path for this type of request)
		// that we can special case as indicating success of the probe above
		// raft.
		return ForcedErrResult{
			Rejection:   ProposalRejectionPermanent,
			ForcedError: NoopOnProbeCommandErr,
		}
	}
	leaseIndex := replicaState.LeaseAppliedIndex
	var requestedLease roachpb.Lease
	if isLeaseRequest {
		requestedLease = *raftCmd.ReplicatedEvalResult.State.Lease
	}
	if idKey == "" {
		// This is an empty Raft command, which is sent by Raft after elections to
		// trigger reproposals, during concurrent configuration changes, or to
		// unquiesce the Raft group. Nothing to do here except making sure that the
		// corresponding batch (which is bogus) doesn't get executed (for it is
		// empty and so properties like key range are undefined).
		return ForcedErrResult{
			LeaseIndex:  leaseIndex,
			Rejection:   ProposalRejectionPermanent,
			ForcedError: noopOnEmptyRaftCommandErr,
		}
	}

	// Verify the lease matches the proposer's expectation. We rely on
	// the proposer's determination of whether the existing lease is
	// held, and can be used, or is expired, and can be replaced.
	// Verify checks that the lease has not been modified since proposal
	// due to Raft delays / reorderings.
	// To understand why this lease verification is necessary, see comments on the
	// proposer_lease field in the proto.
	leaseMismatch := raftCmd.ProposerLeaseSequence != replicaState.Lease.Sequence
	if !leaseMismatch && isLeaseRequest {
		// Lease sequence numbers are a reflection of lease equivalency
		// between subsequent leases. However, Lease.Equivalent is not fully
		// symmetric, meaning that two leases may be Equivalent to a third
		// lease but not Equivalent to each other. If these leases are
		// proposed under that same third lease, neither will be able to
		// detect whether the other has applied just by looking at the
		// current lease sequence number because neither will increment
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
			expToEpochEquiv := raftCmd.ReplicatedEvalResult.IsLeaseRequestWithExpirationToEpochEquivalent
			leaseMismatch = !replicaState.Lease.Equivalent(requestedLease, expToEpochEquiv)
		}

		// This is a check to see if the lease we proposed this lease request
		// against is the same lease that we're trying to update. We need to check
		// proposal timestamps because extensions don't increment sequence
		// numbers. Without this check a lease could be extended and then another
		// lease proposed against the original lease would be applied over the
		// extension.
		//
		// This check also confers replay protection when the sequence number
		// matches, as it ensures that only the first of duplicated proposal can
		// apply, and the second will be rejected (since its PrevLeaseProposal
		// refers to the original lease, and not itself).
		//
		// PrevLeaseProposal is always set. Its nullability dates back to the
		// migration that introduced it.
		if raftCmd.ReplicatedEvalResult.PrevLeaseProposal != nil &&
			(!raftCmd.ReplicatedEvalResult.PrevLeaseProposal.Equal(replicaState.Lease.ProposedTS)) {
			leaseMismatch = true
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
			return ForcedErrResult{
				LeaseIndex: leaseIndex,
				Rejection:  ProposalRejectionPermanent,
				ForcedError: kvpb.NewError(&kvpb.LeaseRejectedError{
					Existing:  *replicaState.Lease,
					Requested: requestedLease,
					Message:   "proposed under invalid lease",
				}),
			}
		}
		// We return a NotLeaseHolderError so that the DistSender retries.
		// NB: we set proposerStoreID to 0 because we don't know who proposed the
		// Raft command. This is ok, as this is only used for debug information.
		nlhe := kvpb.NewNotLeaseHolderError(
			*replicaState.Lease, 0 /* proposerStoreID */, replicaState.Desc,
			fmt.Sprintf(
				"stale proposal: command was proposed under lease #%d but is being applied "+
					"under lease: %s", raftCmd.ProposerLeaseSequence, replicaState.Lease))
		return ForcedErrResult{
			LeaseIndex:  leaseIndex,
			Rejection:   ProposalRejectionPermanent,
			ForcedError: kvpb.NewError(nlhe),
		}
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
			return ForcedErrResult{
				LeaseIndex: leaseIndex,
				Rejection:  ProposalRejectionPermanent,
				ForcedError: kvpb.NewError(&kvpb.LeaseRejectedError{
					Existing:  *replicaState.Lease,
					Requested: requestedLease,
					Message:   "replica not part of range",
				}),
			}
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
		retry := ProposalRejectionPermanent
		if isLocal {
			log.VEventf(
				ctx, 1,
				"retry proposal %x: applied at lease index %d, required < %d",
				idKey, leaseIndex, raftCmd.MaxLeaseIndex,
			)
			retry = ProposalRejectionIllegalLeaseIndex
		}
		return ForcedErrResult{
			LeaseIndex: leaseIndex,
			Rejection:  retry,
			ForcedError: kvpb.NewErrorf(
				"command observed at lease index %d, but required < %d", leaseIndex, raftCmd.MaxLeaseIndex,
			)}
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
		return ForcedErrResult{
			LeaseIndex: leaseIndex,
			Rejection:  ProposalRejectionPermanent,
			ForcedError: kvpb.NewError(&kvpb.BatchTimestampBeforeGCError{
				Timestamp: wts,
				Threshold: *replicaState.GCThreshold,
				RangeID:   replicaState.Desc.RangeID,
				StartKey:  replicaState.Desc.StartKey.AsRawKey(),
				EndKey:    replicaState.Desc.EndKey.AsRawKey(),
			}),
		}
	}
	return ForcedErrResult{
		LeaseIndex:  leaseIndex,
		Rejection:   ProposalRejectionPermanent,
		ForcedError: nil,
	}
}
