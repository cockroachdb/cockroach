// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func init() {
	RegisterReadWriteCommand(kvpb.RequestLease, declareKeysRequestLease, RequestLease)
}

func declareKeysRequestLease(
	rs ImmutableRangeState,
	_ *kvpb.Header,
	_ kvpb.Request,
	latchSpans *spanset.SpanSet,
	_ *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	// NOTE: RequestLease is run on replicas that do not hold the lease, so
	// acquiring latches would not help synchronize with other requests. As
	// such, the request does not declare latches. See also
	// concurrency.shouldIgnoreLatches().
	latchSpans.DisableUndeclaredAccessAssertions()
	return nil
}

// RequestLease sets the range lease for this range. The command fails
// only if the desired start timestamp collides with a previous lease.
// Otherwise, the start timestamp is wound back to right after the expiration
// of the previous lease (or zero). If this range replica is already the lease
// holder, the expiration will be extended or shortened as indicated. For a new
// lease, all duties required of the range lease holder are commenced, including
// releasing all latches and clearing the timestamp cache.
func RequestLease(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	// When returning an error from this method, must always return a
	// newFailedLeaseTrigger() to satisfy stats.
	args := cArgs.Args.(*kvpb.RequestLeaseRequest)
	prevLease := args.PrevLease
	newLease := args.Lease

	// If this check is removed at some point, the filtering of learners on the
	// sending side would have to be removed as well.
	// TODO(nvanbenschoten): move this into leases.Verify.
	wasLastLeaseholder := prevLease.Replica.StoreID == newLease.Replica.StoreID
	if err := roachpb.CheckCanReceiveLease(
		newLease.Replica, cArgs.EvalCtx.Desc().Replicas(), wasLastLeaseholder,
	); err != nil {
		rErr := &kvpb.LeaseRejectedError{
			Existing:  prevLease,
			Requested: newLease,
			Message:   err.Error(),
		}
		return newFailedLeaseTrigger(false /* isTransfer */), rErr
	}

	// Lease type switches need extra care to avoid expiration regressions.
	if args.RevokePrevAndForwardExpiration {
		// Stop using the current lease. All future calls to leaseStatus on this
		// node with the current lease will now return a PROSCRIBED status. This
		// stops the advancement of the expiration of the previous lease.
		cArgs.EvalCtx.RevokeLease(ctx, prevLease.Sequence)

		// After we revoke the previous lease, forward the new lease's minimum
		// expiration beyond the maximum expiration reached by the now-revoked
		// lease.
		minExp := cArgs.EvalCtx.Clock().Now().Add(int64(cArgs.EvalCtx.GetRangeLeaseDuration()), 0)
		if newLease.Type() == roachpb.LeaseExpiration {
			// NOTE: the MinExpiration field is not used by expiration-based leases.
			// They use the Expiration field to store the expiration instead.
			newLease.Expiration.Forward(minExp)
		} else {
			newLease.MinExpiration.Forward(minExp)
		}

		// Forwarding the lease's (minimum) expiration is safe because we know that
		// the lease's sequence number has been incremented. Assert this.
		if newLease.Sequence <= prevLease.Sequence {
			log.Fatalf(ctx, "lease sequence not incremented: prev=%s, new=%s", prevLease, newLease)
		}
	}

	log.VEventf(ctx, 2, "lease request: prev lease: %+v, new lease: %+v", prevLease, newLease)
	return evalNewLease(ctx, cArgs.EvalCtx, readWriter, cArgs.Stats,
		newLease, prevLease, false /* isTransfer */)
}
