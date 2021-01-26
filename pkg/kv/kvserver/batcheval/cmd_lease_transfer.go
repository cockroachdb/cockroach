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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
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
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.RangeLeaseKey(rs.GetRangeID())})
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(rs.GetStartKey())})
	// Cover the entire addressable key space with a latch to prevent any writes
	// from overlapping with lease transfers. In principle we could just use the
	// current range descriptor (desc) but it could potentially change due to an
	// as of yet unapplied merge.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.LocalMax, EndKey: keys.MaxKey})
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

	// If this check is removed at some point, the filtering of learners on the
	// sending side would have to be removed as well.
	if err := CheckCanReceiveLease(args.Lease.Replica, cArgs.EvalCtx.Desc()); err != nil {
		return newFailedLeaseTrigger(true /* isTransfer */), err
	}

	prevLease, _ := cArgs.EvalCtx.GetLease()
	log.VEventf(ctx, 2, "lease transfer: prev lease: %+v, new lease: %+v", prevLease, args.Lease)
	return evalNewLease(ctx, cArgs.EvalCtx, readWriter, cArgs.Stats,
		args.Lease, prevLease, false /* isExtension */, true /* isTransfer */)
}
