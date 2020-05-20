// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/errors"
)

func declareKeysRequestLease(
	desc *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
) {
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.RangeLeaseKey(header.RangeID)})
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)})
}

func newFailedLeaseTrigger(isTransfer bool) result.Result {
	var trigger result.Result
	trigger.Local.Metrics = new(result.Metrics)
	if isTransfer {
		trigger.Local.Metrics.LeaseTransferError = 1
	} else {
		trigger.Local.Metrics.LeaseRequestError = 1
	}
	return trigger
}

func checkCanReceiveLease(newLease *roachpb.Lease, rec EvalContext) error {
	repDesc, ok := rec.Desc().GetReplicaDescriptorByID(newLease.Replica.ReplicaID)
	if !ok {
		if newLease.Replica.StoreID == rec.StoreID() {
			return errors.AssertionFailedf(
				`could not find replica for store %s in %s`, rec.StoreID(), rec.Desc())
		}
		return errors.Errorf(`replica %s not found in %s`, newLease.Replica, rec.Desc())
	} else if t := repDesc.GetType(); t != roachpb.VOTER_FULL {
		// NB: there's no harm in transferring the lease to a VOTER_INCOMING,
		// but we disallow it anyway. On the other hand, transferring to
		// VOTER_OUTGOING would be a pretty bad idea since those voters are
		// dropped when transitioning out of the joint config, which then
		// amounts to removing the leaseholder without any safety precautions.
		// This would either wedge the range or allow illegal reads to be
		// served.
		//
		// Since the leaseholder can't remove itself and is a VOTER_FULL, we
		// also know that in any configuration there's at least one VOTER_FULL.
		//
		// TODO(tbg): if this code path is hit during a lease transfer (we check
		// upstream of raft, but this check has false negatives) then we are in
		// a situation where the leaseholder is a node that has set its
		// minProposedTS and won't be using its lease any more. Either the setting
		// of minProposedTS needs to be "reversible" (tricky) or we make the
		// lease evaluation succeed, though with a lease that's "invalid" so that
		// a new lease can be requested right after.
		return errors.Errorf(`replica %s of type %s cannot hold lease`, repDesc, t)
	}
	return nil
}

// evalNewLease checks that the lease contains a valid interval and that
// the new lease holder is still a member of the replica set, and then proceeds
// to write the new lease to the batch, emitting an appropriate trigger.
//
// The new lease might be a lease for a range that didn't previously have an
// active lease, might be an extension or a lease transfer.
//
// isExtension should be set if the lease holder does not change with this
// lease. If it doesn't change, we don't need the application of this lease to
// block reads.
//
// TODO(tschottdorf): refactoring what's returned from the trigger here makes
// sense to minimize the amount of code intolerant of rolling updates.
func evalNewLease(
	ctx context.Context,
	rec EvalContext,
	readWriter storage.ReadWriter,
	ms *enginepb.MVCCStats,
	lease roachpb.Lease,
	prevLease roachpb.Lease,
	isExtension bool,
	isTransfer bool,
) (result.Result, error) {
	// When returning an error from this method, must always return
	// a newFailedLeaseTrigger() to satisfy stats.

	// Ensure either an Epoch is set or Start < Expiration.
	if (lease.Type() == roachpb.LeaseExpiration && lease.GetExpiration().LessEq(lease.Start)) ||
		(lease.Type() == roachpb.LeaseEpoch && lease.Expiration != nil) {
		// This amounts to a bug.
		return newFailedLeaseTrigger(isTransfer),
			&roachpb.LeaseRejectedError{
				Existing:  prevLease,
				Requested: lease,
				Message: fmt.Sprintf("illegal lease: epoch=%d, interval=[%s, %s)",
					lease.Epoch, lease.Start, lease.Expiration),
			}
	}

	// Verify that requesting replica is part of the current replica set.
	desc := rec.Desc()
	if _, ok := desc.GetReplicaDescriptor(lease.Replica.StoreID); !ok {
		return newFailedLeaseTrigger(isTransfer),
			&roachpb.LeaseRejectedError{
				Existing:  prevLease,
				Requested: lease,
				Message:   "replica not found",
			}
	}

	// Requests should not set the sequence number themselves. Set the sequence
	// number here based on whether the lease is equivalent to the one it's
	// succeeding.
	if lease.Sequence != 0 {
		return newFailedLeaseTrigger(isTransfer),
			&roachpb.LeaseRejectedError{
				Existing:  prevLease,
				Requested: lease,
				Message:   "sequence number should not be set",
			}
	}
	if prevLease.Equivalent(lease) {
		// If the proposed lease is equivalent to the previous lease, it is
		// given the same sequence number. This is subtle, but is important
		// to ensure that leases which are meant to be considered the same
		// lease for the purpose of matching leases during command execution
		// (see Lease.Equivalent) will be considered so. For example, an
		// extension to an expiration-based lease will result in a new lease
		// with the same sequence number.
		lease.Sequence = prevLease.Sequence
	} else {
		// We set the new lease sequence to one more than the previous lease
		// sequence. This is safe and will never result in repeated lease
		// sequences because the sequence check beneath Raft acts as an atomic
		// compare-and-swap of sorts. If two lease requests are proposed in
		// parallel, both with the same previous lease, only one will be
		// accepted and the other will get a LeaseRejectedError and need to
		// retry with a different sequence number. This is actually exactly what
		// the sequence number is used to enforce!
		lease.Sequence = prevLease.Sequence + 1
	}

	// Store the lease to disk & in-memory.
	if err := MakeStateLoader(rec).SetLease(ctx, readWriter, ms, lease); err != nil {
		return newFailedLeaseTrigger(isTransfer), err
	}

	var pd result.Result
	pd.Replicated.State = &kvserverpb.ReplicaState{
		Lease: &lease,
	}
	pd.Replicated.PrevLeaseProposal = prevLease.ProposedTS

	pd.Local.Metrics = new(result.Metrics)
	if isTransfer {
		pd.Local.Metrics.LeaseTransferSuccess = 1
	} else {
		pd.Local.Metrics.LeaseRequestSuccess = 1
	}
	return pd, nil
}
