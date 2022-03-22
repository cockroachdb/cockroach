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
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary/rspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func init() {
	RegisterReadWriteCommand(roachpb.RequestLease, declareKeysRequestLease, RequestLease)
}

func declareKeysRequestLease(
	rs ImmutableRangeState,
	_ *roachpb.Header,
	_ roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
	_ time.Duration,
) {
	// NOTE: RequestLease is run on replicas that do not hold the lease, so
	// acquiring latches would not help synchronize with other requests. As
	// such, the request does not actually acquire latches over these spans
	// (see concurrency.shouldAcquireLatches). However, we continue to
	// declare the keys in order to appease SpanSet assertions under race.
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.RangeLeaseKey(rs.GetRangeID())})
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.RangePriorReadSummaryKey(rs.GetRangeID())})
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(rs.GetStartKey())})
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

	// NOTE: we use the range's current lease as prevLease instead of
	// args.PrevLease so that we can detect lease requests that will
	// inevitably fail early and reject them with a detailed
	// LeaseRejectedError before going through Raft.
	prevLease, _ := cArgs.EvalCtx.GetLease()
	rErr := &roachpb.LeaseRejectedError{
		Existing:  prevLease,
		Requested: args.Lease,
	}

	// If this check is removed at some point, the filtering of learners on the
	// sending side would have to be removed as well.
	if err := roachpb.CheckCanReceiveLease(args.Lease.Replica, cArgs.EvalCtx.Desc()); err != nil {
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
		// this case: even though the lease holder does not change, the sequence
		// number does and this triggers a low water mark bump.
		//
		// The bug prevented with this is unlikely to occur in practice
		// since earlier commands usually apply before this lease will.
		if ts := args.MinProposedTS; isExtension && ts != nil {
			effectiveStart.Forward(*ts)
		}

	} else if prevLease.Type() == roachpb.LeaseExpiration {
		effectiveStart.BackwardWithTimestamp(prevLease.Expiration.Next())
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
	} else if prevLease.Type() == roachpb.LeaseExpiration && effectiveStart.ToTimestamp().Less(prevLease.GetExpiration()) {
		rErr.Message = "requested lease overlaps previous lease"
		return newFailedLeaseTrigger(false /* isTransfer */), rErr
	}
	newLease.Start = effectiveStart

	var priorReadSum *rspb.ReadSummary
	if !prevLease.Equivalent(newLease) {
		// If the new lease is not equivalent to the old lease (i.e. either the
		// lease is changing hands or the leaseholder restarted), construct a
		// read summary to instruct the new leaseholder on how to update its
		// timestamp cache. Since we are not the leaseholder ourselves, we must
		// pessimistically assume that prior leaseholders served reads all the
		// way up to the start of the new lease.
		//
		// NB: this is equivalent to the leaseChangingHands condition in
		// leasePostApplyLocked.
		worstCaseSum := rspb.FromTimestamp(newLease.Start.ToTimestamp())
		priorReadSum = &worstCaseSum
	}

	return evalNewLease(ctx, cArgs.EvalCtx, readWriter, cArgs.Stats,
		newLease, prevLease, priorReadSum, isExtension, false /* isTransfer */)
}
