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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func init() {
	RegisterReadOnlyCommand(roachpb.LeaseInfo, declareKeysLeaseInfo, LeaseInfo)
}

func declareKeysLeaseInfo(
	rs ImmutableRangeState,
	_ *roachpb.Header,
	_ roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
	_ time.Duration,
) {
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeLeaseKey(rs.GetRangeID())})
}

// LeaseInfo returns information about the lease holder for the range.
func LeaseInfo(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	reply := resp.(*roachpb.LeaseInfoResponse)
	lease, nextLease := cArgs.EvalCtx.GetLease()
	if nextLease != (roachpb.Lease{}) {
		// If there's a lease request in progress, speculatively return that future
		// lease.
		reply.Lease = nextLease
		reply.CurrentLease = &lease
	} else {
		reply.Lease = lease
	}
	reply.EvaluatedBy = cArgs.EvalCtx.StoreID()
	return result.Result{}, nil
}
