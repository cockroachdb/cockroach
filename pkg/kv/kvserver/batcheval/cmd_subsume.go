// Copyright 2018 The Cockroach Authors.
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
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary/rspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadWriteCommand(roachpb.Subsume, declareKeysSubsume, Subsume)
}

func declareKeysSubsume(
	_ ImmutableRangeState, header roachpb.Header, req roachpb.Request, latchSpans, _ *spanset.SpanSet,
) {
	// Subsume must not run concurrently with any other command. It declares a
	// non-MVCC write over every addressable key in the range; this guarantees
	// that it conflicts with any other command because every command must
	// declare at least one addressable key. It does not, in fact, write any
	// keys.
	declareAllKeys(latchSpans)
}

// Subsume freezes a range for merging with its left-hand neighbor. When called
// correctly, it provides important guarantees that ensure there is no moment in
// time where the ranges involved in the merge could both process commands for
// the same keys.
//
// Specifically, the receiving replica guarantees that:
//
//   1. it is the leaseholder at the time the request executes,
//   2. when it responds, there are no commands in flight with a timestamp
//      greater than the FreezeStart timestamp provided in the response,
//   3. the MVCC statistics in the response reflect the latest writes,
//   4. it, and all future leaseholders for the range, will not process another
//      command until they refresh their range descriptor with a consistent read
//      from meta2, and
//   5. if it or any future leaseholder for the range finds that its range
//      descriptor has been deleted, it self destructs.
//
// To achieve guarantees four and five, when issuing a Subsume request, the
// caller must have a merge transaction open that has already placed deletion
// intents on both the local and meta2 copy of the right-hand range descriptor.
// The intent on the meta2 allows the leaseholder to block until the merge
// transaction completes by performing a consistent read for its meta2
// descriptor. The intent on the local descriptor allows future leaseholders to
// efficiently check whether a merge is in progress by performing a read of its
// local descriptor after acquiring the lease.
//
// The period of time after intents have been placed but before the merge
// transaction is complete is called the merge's "critical phase".
func Subsume(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.SubsumeRequest)
	reply := resp.(*roachpb.SubsumeResponse)

	// Verify that the Subsume request was sent to the correct range and that
	// the range's bounds have not changed during the merge transaction.
	desc := cArgs.EvalCtx.Desc()
	if !bytes.Equal(desc.StartKey, args.RightDesc.StartKey) ||
		!bytes.Equal(desc.EndKey, args.RightDesc.EndKey) {
		return result.Result{}, errors.Errorf("RHS range bounds do not match: %s != %s",
			args.RightDesc, desc)
	}

	// Sanity check that the requesting range is our left neighbor. The ordering
	// of operations in the AdminMerge transaction should make it impossible for
	// these ranges to be nonadjacent, but double check.
	if !bytes.Equal(args.LeftDesc.EndKey, desc.StartKey) {
		return result.Result{}, errors.Errorf("ranges are not adjacent: %s != %s",
			args.LeftDesc.EndKey, desc.StartKey)
	}

	// Sanity check the caller has initiated a merge transaction by checking for
	// a deletion intent on the local range descriptor. Read inconsistently at
	// the maximum timestamp to ensure that we see an intent if one exists,
	// regardless of what timestamp it is written at.
	descKey := keys.RangeDescriptorKey(desc.StartKey)
	_, intent, err := storage.MVCCGet(ctx, readWriter, descKey, hlc.MaxTimestamp,
		storage.MVCCGetOptions{Inconsistent: true})
	if err != nil {
		return result.Result{}, errors.Errorf("fetching local range descriptor: %s", err)
	} else if intent == nil {
		return result.Result{}, errors.Errorf("range missing intent on its local descriptor")
	}
	val, _, err := storage.MVCCGetAsTxn(ctx, readWriter, descKey, intent.Txn.WriteTimestamp, intent.Txn)
	if err != nil {
		return result.Result{}, errors.Errorf("fetching local range descriptor as txn: %s", err)
	} else if val != nil {
		return result.Result{}, errors.Errorf("non-deletion intent on local range descriptor")
	}

	// NOTE: the deletion intent on the range's meta2 descriptor is just as
	// important to correctness as the deletion intent on the local descriptor,
	// but the check is too expensive as it would involve a network roundtrip on
	// most nodes.

	// Freeze the range. Do so by blocking all requests while a newly launched
	// async goroutine watches (pushes with low priority) the merge transaction.
	// This will also block the closed timestamp side-transport from closing new
	// timestamps, meaning that the following call to GetCurrentReadSummary is
	// guaranteed to observe the highest closed timestamp ever published by this
	// range (if the merge eventually completes).
	if err := cArgs.EvalCtx.WatchForMerge(ctx); err != nil {
		return result.Result{}, errors.Wrap(err, "watching for merge during subsume")
	}

	// We prevent followers of the RHS from being able to serve follower reads on
	// timestamps that fall in the timestamp window representing the range's
	// subsumed state (i.e. between the subsumption time (FreezeStart) and the
	// timestamp at which the merge transaction commits or aborts), by requiring
	// follower replicas to catch up to an MLAI that succeeds the range's current
	// LeaseAppliedIndex (note that we're tracking lai + 1 below instead of lai).
	// In case the merge successfully commits, this MLAI will never be caught up
	// to since the RHS will be destroyed. In case the merge aborts, this ensures
	// that the followers can only activate the newer closed timestamps once they
	// catch up to the LAI associated with the merge abort. We need to do this
	// because the closed timestamps that are broadcast by RHS in this subsumed
	// state are not going to be reflected in the timestamp cache of the LHS range
	// after the merge, which can cause a serializability violation.
	//
	// Note that we are essentially lying to the closed timestamp tracker here in
	// order to achieve the effect of unactionable closed timestamp updates until
	// the merge concludes. Tracking lai + 1 here ensures that the follower
	// replicas need to catch up to at least that index before they are able to
	// activate _any of the closed timestamps from this point onwards_. In other
	// words, we will never publish a closed timestamp update for this range below
	// this lai, regardless of whether a different proposal untracks a lower lai
	// at any point in the future.
	//
	// NB: The above statement relies on the invariant that the LAI that follows a
	// Subsume request will be applied only after the merge aborts. More
	// specifically, this means that no intervening request can bump the LAI of
	// range while it is subsumed. This invariant is upheld because the only Raft
	// proposals allowed after a range has been subsumed are lease requests, which
	// do not bump the LAI. In case there is lease transfer on this range while it
	// is subsumed, we ensure that the initial MLAI update broadcast by the new
	// leaseholder respects the invariant in question, in much the same way we do
	// here. Take a look at `EmitMLAI()` in replica_closedts.go for more details.
	//
	// TODO(nvanbenschoten): remove this in v21.2 when the rest of the v1 closed
	// timestamp system disappears.
	_, untrack := cArgs.EvalCtx.GetTracker().Track(ctx)
	lease, _ := cArgs.EvalCtx.GetLease()
	lai := cArgs.EvalCtx.GetLeaseAppliedIndex()
	untrack(ctx, ctpb.Epoch(lease.Epoch), desc.RangeID, ctpb.LAI(lai+1))

	// Now that the range is frozen, collect some information to ship to the LHS
	// leaseholder through the merge trigger.
	reply.MVCCStats = cArgs.EvalCtx.GetMVCCStats()
	reply.LeaseAppliedIndex = lai
	reply.FreezeStart = cArgs.EvalCtx.Clock().NowAsClockTimestamp()

	// Collect a read summary from the RHS leaseholder to ship to the LHS
	// leaseholder. This is used to instruct the LHS on how to update its
	// timestamp cache to ensure that no future writes are allowed to invalidate
	// prior reads performed to this point on the RHS range.
	priorReadSum, closedTS := cArgs.EvalCtx.GetCurrentReadSummary(ctx)
	// For now, forward this summary to the freeze time. This may appear to
	// undermine the benefit of the read summary, but it doesn't entirely. Until
	// we ship higher-resolution read summaries, the read summary doesn't
	// provide much value in avoiding transaction retries, but it is necessary
	// for correctness if the RHS has served reads at future times above the
	// freeze time.
	//
	// We can remove this in the future when we increase the resolution of read
	// summaries and have a per-range closed timestamp system that is easier to
	// think about.
	priorReadSum.Merge(rspb.FromTimestamp(reply.FreezeStart.ToTimestamp()))
	reply.ReadSummary = &priorReadSum
	// NOTE FOR v21.1: GetCurrentReadSummary might return an empty timestamp if
	// the Raft-based closed timestamp transport hasn't been enabled yet. That's
	// OK because, if the new transport is not enabled, then ranges with leading
	// closed timestamps can't exist yet, and so the closed timestamp must be
	// below the FreezeStart. The FreezeStart is used by Store.MergeRange to
	// bump the RHS' ts cache if LHS/RHS leases are not collocated. The case
	// when the leases are collocated also works out because then the closed
	// timestamp (according to the old mechanism) is the same for both ranges
	// being merged.
	reply.ClosedTimestamp = closedTS

	return result.Result{}, nil
}
