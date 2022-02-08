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
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
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
	_ ImmutableRangeState,
	_ *roachpb.Header,
	_ roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
	_ time.Duration,
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
		return result.Result{}, errors.Wrap(err, "fetching local range descriptor")
	} else if intent == nil {
		return result.Result{}, errors.Errorf("range missing intent on its local descriptor")
	}
	val, _, err := storage.MVCCGetAsTxn(ctx, readWriter, descKey, intent.Txn.WriteTimestamp, intent.Txn)
	if err != nil {
		return result.Result{}, errors.Wrap(err, "fetching local range descriptor as txn")
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

	// Now that the range is frozen, collect some information to ship to the LHS
	// leaseholder through the merge trigger.
	reply.MVCCStats = cArgs.EvalCtx.GetMVCCStats()
	reply.LeaseAppliedIndex = cArgs.EvalCtx.GetLeaseAppliedIndex()
	reply.FreezeStart = cArgs.EvalCtx.Clock().NowAsClockTimestamp()

	// Collect a read summary from the RHS leaseholder to ship to the LHS
	// leaseholder. This is used to instruct the LHS on how to update its
	// timestamp cache to ensure that no future writes are allowed to invalidate
	// prior reads performed to this point on the RHS range.
	priorReadSum := cArgs.EvalCtx.GetCurrentReadSummary(ctx)
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
	reply.ClosedTimestamp = cArgs.EvalCtx.GetClosedTimestamp(ctx)

	return result.Result{}, nil
}
