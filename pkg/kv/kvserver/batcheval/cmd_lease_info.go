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
)

func init() {
	RegisterReadOnlyCommand(kvpb.LeaseInfo, declareKeysLeaseInfo, LeaseInfo)
}

func declareKeysLeaseInfo(
	_ ImmutableRangeState,
	_ *kvpb.Header,
	_ kvpb.Request,
	_ *spanset.SpanSet,
	_ *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	// We don't take out a latch on the range lease key, since lease requests
	// bypass latches anyway (they're evaluated on the proposer). The lease is
	// read from the replica's in-memory state.
	//
	// Being latchless allows using this as a simple replica health probe without
	// worrying about latch latency.
	return nil
}

// LeaseInfo returns information about the lease holder for the range.
func LeaseInfo(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	reply := resp.(*kvpb.LeaseInfoResponse)
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
