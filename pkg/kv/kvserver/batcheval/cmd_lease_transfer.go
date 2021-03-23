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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary/rspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func init() {
	RegisterReadWriteCommand(roachpb.TransferLease, declareKeysTransferLease, TransferLease)
}

func declareKeysTransferLease(
	rs ImmutableRangeState, _ roachpb.Header, _ roachpb.Request, latchSpans, _ *spanset.SpanSet,
) {
	// TransferLease must not run concurrently with any other request so it uses
	// latches to synchronize with all other reads and writes on the outgoing
	// leaseholder. Additionally, it observes the state of the timestamp cache
	// and so it uses latches to wait for all in-flight requests to complete.
	//
	// Because of this, it declares a non-MVCC write over every addressable key
	// in the range, even though the only key the TransferLease actually writes
	// to is the RangeLeaseKey. This guarantees that it conflicts with any other
	// request because every request must declare at least one addressable key.
	//
	// We could, in principle, declare these latches as MVCC writes at the time
	// of the new lease. Doing so would block all concurrent writes but would
	// allow reads below the new lease timestamp through. However, doing so
	// would only be safe if we also accounted for clock uncertainty in all read
	// latches so that any read that may need to observe state on the new
	// leaseholder gets blocked. We actually already do this for transactional
	// reads (see DefaultDeclareIsolatedKeys), but not for non-transactional
	// reads. We'd need to be careful here, so we should only pull on this if we
	// decide that doing so is important.
	declareAllKeys(latchSpans)
}

// TransferLease sets the lease holder for the range.
// Unlike with RequestLease(), the new lease is allowed to overlap the old one,
// the contract being that the transfer must have been initiated by the (soon
// ex-) lease holder which must have dropped all of its lease holder powers
// before proposing.
func TransferLease(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	// When returning an error from this method, must always return
	// a newFailedLeaseTrigger() to satisfy stats.
	args := cArgs.Args.(*roachpb.TransferLeaseRequest)

	// NOTE: we use the range's current lease as prevLease instead of
	// args.PrevLease so that we can detect lease transfers that will
	// inevitably fail early and reject them with a detailed
	// LeaseRejectedError before going through Raft.
	prevLease, _ := cArgs.EvalCtx.GetLease()

	// Forward the lease's start time to a current clock reading. At this
	// point, we're holding latches across the entire range, we know that
	// this time is greater than the timestamps at which any request was
	// serviced by the leaseholder before it stopped serving requests (i.e.
	// before the TransferLease request acquired latches).
	newLease := args.Lease
	newLease.Start.Forward(cArgs.EvalCtx.Clock().NowAsClockTimestamp())
	args.Lease = roachpb.Lease{} // prevent accidental use below

	// If this check is removed at some point, the filtering of learners on the
	// sending side would have to be removed as well.
	if err := roachpb.CheckCanReceiveLease(newLease.Replica, cArgs.EvalCtx.Desc()); err != nil {
		return newFailedLeaseTrigger(true /* isTransfer */), err
	}

	// Stop using the current lease. All future calls to leaseStatus on this
	// node with the current lease will now return a PROSCRIBED status. This
	// includes calls to leaseStatus from the closed timestamp side-transport,
	// meaning that the following call to GetCurrentReadSummary is guaranteed to
	// observe the highest closed timestamp published under this lease.
	//
	// We perform this action during evaluation to ensure that the lease
	// revocation takes place regardless of whether the corresponding Raft
	// proposal succeeds, fails, or is ambiguous - in which case there's no
	// guarantee that the transfer will not still apply. This means that if the
	// proposal fails, we'll have relinquished the current lease but not managed
	// to give the lease to someone else, so we'll have to re-acquire the lease
	// again through a RequestLease request to recover. This situation is tested
	// in TestBehaviorDuringLeaseTransfer/transferSucceeds=false.
	//
	// NOTE: RevokeLease will be a no-op if the lease has already changed. In
	// such cases, we could detect that here and fail fast, but it's safe and
	// easier to just let the TransferLease be proposed under the wrong lease
	// and be rejected with the correct error below Raft.
	cArgs.EvalCtx.RevokeLease(ctx, args.PrevLease.Sequence)

	// Collect a read summary from the outgoing leaseholder to ship to the
	// incoming leaseholder. This is used to instruct the new leaseholder on how
	// to update its timestamp cache to ensure that no future writes are allowed
	// to invalidate prior reads.
	priorReadSum, _ := cArgs.EvalCtx.GetCurrentReadSummary(ctx)
	// For now, forward this summary to the proposed lease's start time. This
	// may appear to undermine the benefit of the read summary, but it doesn't
	// entirely. Until we ship higher-resolution read summaries, the read
	// summary doesn't provide much value in avoiding transaction retries, but
	// it is necessary for correctness if the outgoing leaseholder has served
	// reads at future times above the proposed lease start time.
	//
	// We can remove this in the future when we increase the resolution of read
	// summaries and have a per-range closed timestamp system that is easier to
	// think about.
	priorReadSum.Merge(rspb.FromTimestamp(newLease.Start.ToTimestamp()))

	log.VEventf(ctx, 2, "lease transfer: prev lease: %+v, new lease: %+v", prevLease, newLease)
	return evalNewLease(ctx, cArgs.EvalCtx, readWriter, cArgs.Stats,
		newLease, prevLease, &priorReadSum, false /* isExtension */, true /* isTransfer */)
}
