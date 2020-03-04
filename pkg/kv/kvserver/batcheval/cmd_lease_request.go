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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func init() {
	RegisterReadWriteCommand(roachpb.RequestLease, declareKeysRequestLease, RequestLease)
}

// RequestLease sets the range lease for this range. The command fails
// only if the desired start timestamp collides with a previous lease.
// Otherwise, the start timestamp is wound back to right after the expiration
// of the previous lease (or zero). If this range replica is already the lease
// holder, the expiration will be extended or shortened as indicated. For a new
// lease, all duties required of the range lease holder are commenced, including
// releasing all latches and clearing the timestamp cache.
func RequestLease(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	// When returning an error from this method, must always return a
	// newFailedLeaseTrigger() to satisfy stats.
	args := cArgs.Args.(*roachpb.RequestLeaseRequest)

	prevLease, _ := cArgs.EvalCtx.GetLease()
	rErr := &roachpb.LeaseRejectedError{
		Existing:  prevLease,
		Requested: args.Lease,
	}

	// For now, don't allow replicas of type LEARNER to be leaseholders. There's
	// no reason this wouldn't work in principle, but it seems inadvisable. In
	// particular, learners can't become raft leaders, so we wouldn't be able to
	// co-locate the leaseholder + raft leader, which is going to affect tail
	// latencies. Additionally, as of the time of writing, learner replicas are
	// only used for a short time in replica addition, so it's not worth working
	// out the edge cases. If we decide to start using long-lived learners at some
	// point, that math may change.
	//
	// If this check is removed at some point, the filtering of learners on the
	// sending side would have to be removed as well.
	if err := checkCanReceiveLease(&args.Lease, cArgs.EvalCtx); err != nil {
		rErr.Message = err.Error()
		return newFailedLeaseTrigger(false /* isTransfer */), rErr
	}

	// MIGRATION(tschottdorf): needed to apply Raft commands which got proposed
	// before the StartStasis field was introduced.
	newLease := args.Lease
	if newLease.DeprecatedStartStasis == nil {
		newLease.DeprecatedStartStasis = newLease.Expiration
	}

	isExtension := prevLease.Replica.StoreID == newLease.Replica.StoreID
	effectiveStart := newLease.Start

	// Wind the start timestamp back as far towards the previous lease as we
	// can. That'll make sure that when multiple leases are requested out of
	// order at the same replica (after all, they use the request timestamp,
	// which isn't straight out of our local clock), they all succeed unless
	// they have a "real" issue with a previous lease. Example: Assuming no
	// previous lease, one request for [5, 15) followed by one for [0, 15)
	// would fail without this optimization. With it, the first request
	// effectively gets the lease for [0, 15), which the second one can commit
	// again (even extending your own lease is possible; see below).
	//
	// If this is our lease (or no prior lease exists), we effectively absorb
	// the old lease. This allows multiple requests from the same replica to
	// merge without ticking away from the minimal common start timestamp. It
	// also has the positive side-effect of fixing #3561, which was caused by
	// the absence of replay protection.
	if prevLease.Replica.StoreID == 0 || isExtension {
		effectiveStart.Backward(prevLease.Start)
		// If the lease holder promised to not propose any commands below
		// MinProposedTS, it must also not be allowed to extend a lease before that
		// timestamp. We make sure that when a node restarts, its earlier in-flight
		// commands (which are not tracked by the spanlatch manager post restart)
		// receive an error under the new lease by making sure the sequence number
		// of that lease is higher. This in turn is achieved by forwarding its start
		// time here, which makes it not Equivalent() to the preceding lease for the
		// same store.
		//
		// Note also that leasePostApply makes sure to update the timestamp cache in
		// this case: even though the lease holder does not change, the the sequence
		// number does and this triggers a low water mark bump.
		//
		// The bug prevented with this is unlikely to occur in practice
		// since earlier commands usually apply before this lease will.
		if ts := args.MinProposedTS; isExtension && ts != nil {
			effectiveStart.Forward(*ts)
		}

	} else if prevLease.Type() == roachpb.LeaseExpiration {
		effectiveStart.Backward(prevLease.Expiration.Next())
	}

	if isExtension {
		if effectiveStart.Less(prevLease.Start) {
			rErr.Message = "extension moved start timestamp backwards"
			return newFailedLeaseTrigger(false /* isTransfer */), rErr
		}
		if newLease.Type() == roachpb.LeaseExpiration {
			// NB: Avoid mutating pointers in the argument which might be shared with
			// the caller.
			t := *newLease.Expiration
			newLease.Expiration = &t
			newLease.Expiration.Forward(prevLease.GetExpiration())
		}
	} else if prevLease.Type() == roachpb.LeaseExpiration && effectiveStart.Less(prevLease.GetExpiration()) {
		rErr.Message = "requested lease overlaps previous lease"
		return newFailedLeaseTrigger(false /* isTransfer */), rErr
	}
	newLease.Start = effectiveStart
	return evalNewLease(ctx, cArgs.EvalCtx, readWriter, cArgs.Stats,
		newLease, prevLease, isExtension, false /* isTransfer */)
}
