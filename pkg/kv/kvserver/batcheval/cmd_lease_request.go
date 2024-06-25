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

	log.VEventf(ctx, 2, "lease request: prev lease: %+v, new lease: %+v", prevLease, newLease)
	return evalNewLease(ctx, cArgs.EvalCtx, readWriter, cArgs.Stats,
		newLease, prevLease, false /* isTransfer */)
}
