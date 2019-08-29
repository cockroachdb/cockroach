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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func init() {
	RegisterCommand(roachpb.TransferLease, declareKeysRequestLease, TransferLease)
}

// TransferLease sets the lease holder for the range.
// Unlike with RequestLease(), the new lease is allowed to overlap the old one,
// the contract being that the transfer must have been initiated by the (soon
// ex-) lease holder which must have dropped all of its lease holder powers
// before proposing.
func TransferLease(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	// When returning an error from this method, must always return
	// a newFailedLeaseTrigger() to satisfy stats.
	args := cArgs.Args.(*roachpb.TransferLeaseRequest)

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
	if err := checkCanReceiveLease(cArgs.EvalCtx); err != nil {
		return newFailedLeaseTrigger(true /* isTransfer */), err
	}

	prevLease, _ := cArgs.EvalCtx.GetLease()
	if log.V(2) {
		log.Infof(ctx, "lease transfer: prev lease: %+v, new lease: %+v", prevLease, args.Lease)
	}
	return evalNewLease(ctx, cArgs.EvalCtx, batch, cArgs.Stats,
		args.Lease, prevLease, false /* isExtension */, true /* isTransfer */)
}
