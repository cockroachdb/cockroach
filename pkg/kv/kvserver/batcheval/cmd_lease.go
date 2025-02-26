// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary/rspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

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
	isTransfer bool,
) (result.Result, error) {
	// When returning an error from this method, must always return
	// a newFailedLeaseTrigger() to satisfy stats.

	// Construct the prior read summary if the lease sequence is changing.
	var priorReadSum *rspb.ReadSummary
	var locksWritten int
	if prevLease.Sequence != lease.Sequence {
		// If the new lease is not equivalent to the old lease, construct a read
		// summary to instruct the new leaseholder on how to update its timestamp
		// cache to respect prior reads served on the range.
		if isTransfer {
			// Collect a read summary from the outgoing leaseholder to ship to the
			// incoming leaseholder. This is used to instruct the new leaseholder on
			// how to update its timestamp cache to ensure that no future writes are
			// allowed to invalidate prior reads.
			localReadSum := rec.GetCurrentReadSummary(ctx)
			priorReadSum = &localReadSum

			// If this is a lease transfer, we write out all unreplicated leases to
			// storage, so that any waiters will discover them on the new leaseholder.
			acquisitions := rec.GetConcurrencyManager().OnRangeLeaseTransferEval()
			log.VEventf(ctx, 2, "upgrading durability of %d locks", len(acquisitions))
			for _, acq := range acquisitions {
				if err := storage.MVCCAcquireLock(ctx, readWriter,
					&acq.Txn, acq.IgnoredSeqNums, acq.Strength, acq.Key, ms, 0, 0); err != nil {
					return newFailedLeaseTrigger(isTransfer), err
				}
			}
			locksWritten = len(acquisitions)
		} else {
			// If the new lease is not equivalent to the old lease (i.e. either the
			// lease is changing hands or the leaseholder restarted), construct a
			// read summary to instruct the new leaseholder on how to update its
			// timestamp cache. Since we are not the leaseholder ourselves, we must
			// pessimistically assume that prior leaseholders served reads all the
			// way up to the start of the new lease.
			//
			// NB: this is equivalent to the leaseChangingHands condition in
			// leasePostApplyLocked.
			worstCaseSum := rspb.FromTimestamp(lease.Start.ToTimestamp())
			priorReadSum = &worstCaseSum
		}
	}

	// Store the lease to disk & in-memory.
	if err := MakeStateLoader(rec).SetLeaseBlind(ctx, readWriter, ms, lease, prevLease); err != nil {
		return newFailedLeaseTrigger(isTransfer), err
	}

	var pd result.Result
	pd.Replicated.State = &kvserverpb.ReplicaState{
		Lease: &lease,
	}
	pd.Replicated.PrevLeaseProposal = &prevLease.ProposedTS

	// If we're setting a new prior read summary, store it to disk & in-memory.
	if priorReadSum != nil {
		pd.Replicated.PriorReadSummary = priorReadSum
		// Compress the persisted read summary, as it will likely never be needed.
		compressedSum := priorReadSum.Clone()
		compressedSum.Compress(0)
		if err := readsummary.Set(ctx, readWriter, rec.GetRangeID(), ms, compressedSum); err != nil {
			return newFailedLeaseTrigger(isTransfer), err
		}
	}

	pd.Local.Metrics = new(result.Metrics)
	pd.Local.Metrics.LeaseTransferLocksWritten = locksWritten
	if isTransfer {
		pd.Local.Metrics.LeaseTransferSuccess = 1
	} else {
		pd.Local.Metrics.LeaseRequestSuccess = 1
	}
	return pd, nil
}
